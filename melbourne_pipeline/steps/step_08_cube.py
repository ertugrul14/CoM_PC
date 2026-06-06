"""
Step 08 — Data cube assembly + dual graph construction.

Inputs (data/processed/):
  ped_complete.parquet       — N×14400 ped_flow + confidence + source
  parking_occupancy.parquet  — K×14400 occupancy_rate per parking street
  temporal_features.parquet  — 14400 cyclic time encodings + holiday flags
  weather.parquet            — 14400 weather observations
  static_features.parquet    — N static land-use features
  node_index.parquet         — street_id → node_idx (1397 arterial streets)
  spatial_edges.parquet      — (E_s) node_i, node_j, dist_m, weight
  semantic_edges.parquet     — (E_sem) node_i, node_j, similarity, weight

Outputs (data/processed/):
  cube.npy              — float32 [N × T × F] in node_idx order
  graph_spatial.pt      — torch sparse FloatTensor [N × N], D^-½ A D^-½ normalised
  graph_semantic.pt     — torch sparse FloatTensor [N × N], D^-½ A D^-½ normalised
  norm_stats.json       — mean / std for each of the F features
  cube_meta.json        — N, T, F, feature names, node order

Feature layout (F = 23):
  0   ped_flow              time-varying, continuous
  1   occupancy_rate        time-varying, continuous (0 if no parking sensor)
  2   ped_confidence        time-varying, ordinal  (1.0 / 0.8 / 0.5)
  3   hour_sin              cyclic
  4   hour_cos              cyclic
  5   dow_sin               cyclic
  6   dow_cos               cyclic
  7   is_weekend            binary
  8   is_public_holiday     binary
  9   is_school_holiday     binary
  10  temperature_2m        continuous
  11  relative_humidity_2m  continuous
  12  wind_speed_10m        continuous
  13  precipitation         continuous
  14  total_jobs            static continuous
  15  cafe_count            static continuous
  16  cafe_total_seats      static continuous
  17  bar_count             static continuous
  18  bar_patron_capacity   static continuous
  19  business_count        static continuous
  20  poi_total             static continuous
  21  dining_capacity       static continuous
  22  area_m2               static continuous
"""
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import torch

from config import PROCESSED_DIR, LOG_NORMALISE_FEATURES

log = logging.getLogger(__name__)

FEATURE_NAMES = [
    "ped_flow", "occupancy_rate", "ped_confidence",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "is_weekend", "is_public_holiday", "is_school_holiday",
    "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation",
    "total_jobs", "cafe_count", "cafe_total_seats",
    "bar_count", "bar_patron_capacity", "business_count",
    "poi_total", "dining_capacity", "area_m2",
]
F = len(FEATURE_NAMES)

# Features to normalise (indices into FEATURE_NAMES)
# Exclude cyclic [-1,1], binary, and ped_confidence (ordinal tier)
NORMALISE_IDX = [0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]

STATIC_COLS = [
    "total_jobs", "cafe_count", "cafe_total_seats",
    "bar_count", "bar_patron_capacity", "business_count",
    "poi_total", "dining_capacity", "area_m2",
]


def _build_normalised_adj(
    edges: pd.DataFrame,
    n: int,
    weight_col: str,
) -> torch.Tensor:
    """
    Build a symmetrically normalised sparse adjacency matrix.
    A_norm = D^{-1/2} (A + I) D^{-1/2}

    edges: DataFrame with columns node_i, node_j, <weight_col>
    n:     number of nodes
    """
    i_idx = torch.tensor(edges["node_i"].values, dtype=torch.long)
    j_idx = torch.tensor(edges["node_j"].values, dtype=torch.long)
    vals  = torch.tensor(edges[weight_col].values, dtype=torch.float32)

    # Self-loops
    self_idx  = torch.arange(n, dtype=torch.long)
    self_vals = torch.ones(n, dtype=torch.float32)

    all_i = torch.cat([i_idx, self_idx])
    all_j = torch.cat([j_idx, self_idx])
    all_v = torch.cat([vals,  self_vals])

    # Degree vector (row sums)
    deg = torch.zeros(n, dtype=torch.float32)
    deg.scatter_add_(0, all_i, all_v)
    d_inv_sqrt = torch.where(deg > 0, deg.pow(-0.5), torch.zeros_like(deg))

    # D^{-1/2} A D^{-1/2}
    norm_v = d_inv_sqrt[all_i] * all_v * d_inv_sqrt[all_j]

    adj = torch.sparse_coo_tensor(
        indices=torch.stack([all_i, all_j]),
        values=norm_v,
        size=(n, n),
    ).coalesce()
    return adj


def run() -> dict[str, Path]:
    log.info("=== Step 8: Data cube assembly ===")

    # ── Load inputs ───────────────────────────────────────────────────────────
    node_index    = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    spatial_edges = pd.read_parquet(PROCESSED_DIR / "spatial_edges.parquet")
    semantic_edges = pd.read_parquet(PROCESSED_DIR / "semantic_edges.parquet")
    static_df     = pd.read_parquet(PROCESSED_DIR / "static_features.parquet")
    temporal      = pd.read_parquet(PROCESSED_DIR / "temporal_features.parquet")
    weather       = pd.read_parquet(PROCESSED_DIR / "weather.parquet")
    ped_df        = pd.read_parquet(PROCESSED_DIR / "ped_complete.parquet")
    park_df       = pd.read_parquet(PROCESSED_DIR / "parking_occupancy.parquet")

    # ── Node order: sorted by node_idx ───────────────────────────────────────
    node_index = node_index.sort_values("node_idx").reset_index(drop=True)
    node_order  = node_index["street_id"].astype(str).tolist()   # length N
    sid_to_nidx = dict(zip(node_order, range(len(node_order))))
    N = len(node_order)
    log.info(f"  N={N} nodes in node_index order")

    # ── Pivot ped_flow and ped_confidence ─────────────────────────────────────
    log.info("  Pivoting ped_complete...")
    ped_df["street_id"] = ped_df["street_id"].astype(str)
    ped_df["time_bin"]  = pd.to_datetime(ped_df["time_bin"], utc=True)
    ped_pivot = ped_df.pivot(index="street_id", columns="time_bin", values="ped_flow")
    conf_pivot = ped_df.pivot(index="street_id", columns="time_bin", values="ped_confidence")
    # Fix #3 (D-013): ped validity mask — False on sensor-outage bins to exclude from loss
    valid_pivot = ped_df.pivot(index="street_id", columns="time_bin", values="ped_valid")

    # Reindex to node order; streets not in ped → 0
    ped_pivot   = ped_pivot.reindex(node_order).fillna(0.0)
    conf_pivot  = conf_pivot.reindex(node_order).fillna(0.5)
    valid_pivot = valid_pivot.reindex(node_order).fillna(False)
    time_index = ped_pivot.columns.tolist()
    T = len(time_index)
    log.info(f"  T={T} time bins")

    ped_arr  = ped_pivot.values.astype(np.float32)   # (N, T)
    conf_arr = conf_pivot.values.astype(np.float32)  # (N, T)
    # Save ped-validity mask for the training loss (D-013)
    ped_valid_mask = torch.tensor(valid_pivot.values.astype(bool))   # (N, T)
    torch.save(ped_valid_mask, PROCESSED_DIR / "ped_valid_mask.pt")
    log.info(f"  Saved ped_valid_mask.pt  ({int(ped_valid_mask.sum()):,}/{ped_valid_mask.numel():,} bins valid)")
    del ped_pivot, conf_pivot, valid_pivot

    # ── Pivot parking occupancy ───────────────────────────────────────────────
    log.info("  Pivoting parking_occupancy...")
    park_df["street_id"] = park_df["street_id"].astype(str)
    park_df["time_bin"]  = pd.to_datetime(park_df["time_bin"], utc=True)
    park_pivot = park_df.pivot(index="street_id", columns="time_bin", values="occupancy_rate")
    park_pivot = park_pivot.reindex(node_order)
    # Fix #5 (D-015): non-sensor streets have NO parking data — impute to the sensor
    # mean (≈ neutral/"unknown" after z-score) instead of 0 ("definitely empty").
    # Parking head loss + eval stay masked to the 143 real sensor streets.
    occ_sensor_mean = float(park_df["occupancy_rate"].mean())
    park_pivot = park_pivot.fillna(occ_sensor_mean)
    park_arr   = park_pivot.values.astype(np.float32)  # (N, T)
    log.info(f"  Non-sensor occupancy imputed to sensor mean {occ_sensor_mean:.4f} (D-015)")
    del park_pivot

    # ── Temporal features: broadcast to (N, T) ───────────────────────────────
    log.info("  Preparing temporal + weather features...")
    temporal["time_bin"] = pd.to_datetime(temporal["time_bin"], utc=True)
    weather["time_bin"]  = pd.to_datetime(weather["time_bin"],  utc=True)
    tw = temporal.merge(weather, on="time_bin").set_index("time_bin")
    tw = tw.reindex(time_index)

    tw_cols = ["hour_sin", "hour_cos", "dow_sin", "dow_cos",
               "is_weekend", "is_public_holiday", "is_school_holiday",
               "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation"]
    tw_arr = tw[tw_cols].values.astype(np.float32)  # (T, 11)
    # Broadcast to (N, T, 11)
    tw_broadcast = np.broadcast_to(tw_arr[np.newaxis, :, :], (N, T, len(tw_cols)))

    # ── Static features: broadcast to (N, T) ─────────────────────────────────
    static_df["street_id"] = static_df["street_id"].astype(str)
    static_lookup = static_df.set_index("street_id")[STATIC_COLS]
    static_arr = static_lookup.reindex(node_order).fillna(0.0).values.astype(np.float32)  # (N, 9)
    # Broadcast to (N, T, 9)
    static_broadcast = np.broadcast_to(static_arr[:, np.newaxis, :], (N, T, len(STATIC_COLS)))

    # ── Assemble cube [N, T, F] ───────────────────────────────────────────────
    log.info(f"  Allocating cube [{N} × {T} × {F}] float32 "
             f"({N * T * F * 4 / 1e9:.2f} GB)...")
    cube = np.empty((N, T, F), dtype=np.float32)

    cube[:, :, 0]     = ped_arr
    cube[:, :, 1]     = park_arr
    cube[:, :, 2]     = conf_arr
    cube[:, :, 3:14]  = tw_broadcast
    cube[:, :, 14:23] = static_broadcast

    log.info(f"  Cube assembled: shape={cube.shape}")

    del ped_arr, park_arr, conf_arr, tw_broadcast, static_broadcast

    # ── Normalisation stats (compute on train portion only: first 80% of T) ──
    log.info("  Computing normalisation stats (first 80% of timesteps)...")
    T_train = int(T * 0.8)
    norm_stats: dict[str, dict] = {}
    for fi in NORMALISE_IDX:
        name = FEATURE_NAMES[fi]
        vals = cube[:, :T_train, fi].ravel()
        # log-normalised features (ped_flow): stats computed in log1p space so the
        # saved scalar mean/std match the log transform applied by consumers (D-012)
        if name in LOG_NORMALISE_FEATURES:
            vals = np.log1p(np.maximum(vals, 0.0))
        mu  = float(np.mean(vals))
        std = float(np.std(vals))
        std = max(std, 1e-6)
        norm_stats[name] = {"mean": round(mu, 6), "std": round(std, 6)}

    # Save raw cube (training step applies normalisation on the fly)
    cube_path = PROCESSED_DIR / "cube.npy"
    np.save(cube_path, cube)
    log.info(f"  Saved cube.npy  ({cube_path.stat().st_size / 1e9:.2f} GB)")

    # ── Build normalised adjacency matrices ───────────────────────────────────
    log.info(f"  Building spatial adjacency (N={N}, E={len(spatial_edges)})...")
    adj_spatial = _build_normalised_adj(spatial_edges, N, "weight")
    torch.save(adj_spatial, PROCESSED_DIR / "graph_spatial.pt")
    log.info(f"  Saved graph_spatial.pt  (nnz={adj_spatial._nnz()})")

    log.info(f"  Building semantic adjacency (N={N}, E={len(semantic_edges)})...")
    adj_semantic = _build_normalised_adj(semantic_edges, N, "weight")
    torch.save(adj_semantic, PROCESSED_DIR / "graph_semantic.pt")
    log.info(f"  Saved graph_semantic.pt  (nnz={adj_semantic._nnz()})")

    # ── Save metadata ─────────────────────────────────────────────────────────
    meta = {
        "N": N, "T": T, "F": F,
        "feature_names": FEATURE_NAMES,
        "normalise_features": [FEATURE_NAMES[i] for i in NORMALISE_IDX],
        "node_order": node_order,          # street_id at each node index
        "T_train_end": T_train,
        "T_val_start": T_train,
    }
    meta_path = PROCESSED_DIR / "cube_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))

    norm_path = PROCESSED_DIR / "norm_stats.json"
    norm_path.write_text(json.dumps(norm_stats, indent=2))

    log.info(f"  Saved cube_meta.json, norm_stats.json")
    log.info(f"  Features normalised: {[FEATURE_NAMES[i] for i in NORMALISE_IDX]}")
    log.info("Step 8 complete.")

    return {
        "cube":         cube_path,
        "graph_spatial": PROCESSED_DIR / "graph_spatial.pt",
        "graph_semantic": PROCESSED_DIR / "graph_semantic.pt",
        "norm_stats":   norm_path,
        "cube_meta":    meta_path,
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
