"""
Step 08 (v2) — Sensor-union cube + adjacency assembly.

Slices the existing full cube (1,397 streets) down to the 189-street sensor union
defined by steps_v2/step_04_graph, builds the two fresh normalised adjacencies,
recomputes per-feature norm stats on the subcube, and emits the supervision masks.

No raw re-fetch and no re-imputation: the per-street rows in cube.npy are already
correct for these 189 real-sensor streets; we simply select and reorder them.

Inputs:
  data/processed/cube.npy, cube_meta.json, ped_valid_mask.pt   (full, 1,397 nodes)
  data/processed/node_index.parquet                            (full mapping)
  data/processed_v2/node_index.parquet, spatial_edges.parquet, semantic_edges.parquet

Outputs (data/processed_v2/):
  cube.npy            float32 [189 x T x F]
  graph_spatial.pt    sparse D^-1/2 A D^-1/2 [189 x 189]
  graph_semantic.pt   sparse D^-1/2 A D^-1/2 [189 x 189]
  norm_stats.json     per-feature mean/std (ped_flow in log1p space)
  cube_meta.json      N, T, F, feature_names, node_order, split fractions
  ped_valid_mask.pt   bool [189 x T]   (sliced from full)
  ped_sensor_mask.pt  bool [189]       True for the 74 ped-sensor streets
  parking_mask.pt     bool [189]       True for the 143 parking-sensor streets

Run:
  cd melbourne_pipeline && python -m steps_v2.step_08_cube   (after step_04 v2)
"""
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import torch

from config import PROCESSED_DIR, LOG_NORMALISE_FEATURES

log = logging.getLogger(__name__)

V2_DIR = PROCESSED_DIR.parent / "processed_v2"

FEATURE_NAMES = [
    "ped_flow", "occupancy_rate", "ped_confidence",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "is_weekend", "is_public_holiday", "is_school_holiday",
    "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation",
    "total_jobs", "cafe_count", "cafe_total_seats",
    "bar_count", "bar_patron_capacity", "business_count",
    "poi_total", "dining_capacity", "area_m2",
]
NORMALISE_IDX = [0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
TRAIN_FRAC, VAL_FRAC = 0.70, 0.15


def _build_normalised_adj(edges: pd.DataFrame, n: int, weight_col: str) -> torch.Tensor:
    """Symmetrically normalised sparse adjacency  D^-1/2 (A + I) D^-1/2."""
    i_idx = torch.tensor(edges["node_i"].values, dtype=torch.long)
    j_idx = torch.tensor(edges["node_j"].values, dtype=torch.long)
    vals  = torch.tensor(edges[weight_col].values, dtype=torch.float32)

    self_idx  = torch.arange(n, dtype=torch.long)
    self_vals = torch.ones(n, dtype=torch.float32)
    all_i = torch.cat([i_idx, self_idx])
    all_j = torch.cat([j_idx, self_idx])
    all_v = torch.cat([vals,  self_vals])

    deg = torch.zeros(n, dtype=torch.float32)
    deg.scatter_add_(0, all_i, all_v)
    d_inv_sqrt = torch.where(deg > 0, deg.pow(-0.5), torch.zeros_like(deg))
    norm_v = d_inv_sqrt[all_i] * all_v * d_inv_sqrt[all_j]

    return torch.sparse_coo_tensor(
        indices=torch.stack([all_i, all_j]), values=norm_v, size=(n, n)
    ).coalesce()


def _norm_stats(sub_cube: np.ndarray) -> dict:
    """Per-feature mean/std on the first TRAIN_FRAC of T; ped_flow in log1p space."""
    T = sub_cube.shape[1]
    t_stat = int(T * TRAIN_FRAC)
    stats = {}
    for fi in NORMALISE_IDX:
        name = FEATURE_NAMES[fi]
        vals = sub_cube[:, :t_stat, fi].astype(np.float64)
        if name in LOG_NORMALISE_FEATURES:
            vals = np.log1p(np.maximum(vals, 0.0))     # D-012 contract
        stats[name] = {"mean": float(vals.mean()), "std": float(max(vals.std(), 1e-8))}
    return stats


def run() -> dict[str, Path]:
    log.info("=== Step 08 (v2): sensor-union cube assembly ===")
    V2_DIR.mkdir(parents=True, exist_ok=True)

    # ── v2 node order + full mapping ──────────────────────────────────────────
    v2_ni = pd.read_parquet(V2_DIR / "node_index.parquet").sort_values("node_idx")
    v2_ni["street_id"] = v2_ni["street_id"].astype(str)
    v2_sids = v2_ni["street_id"].tolist()
    N = len(v2_sids)

    full_ni = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    full_ni["street_id"] = full_ni["street_id"].astype(str)
    sid_to_full = dict(zip(full_ni["street_id"], full_ni["node_idx"].astype(int)))
    full_idx = np.array([sid_to_full[s] for s in v2_sids], dtype=int)   # gather order

    # ── slice the full cube (mmap so we never hold 1.76 GB in RAM) ────────────
    full_cube = np.load(PROCESSED_DIR / "cube.npy", mmap_mode="r")      # [1397,T,F]
    sub_cube = np.ascontiguousarray(full_cube[full_idx]).astype(np.float32)  # [189,T,F]
    T, Fdim = sub_cube.shape[1], sub_cube.shape[2]
    log.info(f"  Sliced cube -> [{N}, {T}, {Fdim}]")
    np.save(V2_DIR / "cube.npy", sub_cube)

    # ── ped-validity mask sliced from full ────────────────────────────────────
    full_valid = torch.load(PROCESSED_DIR / "ped_valid_mask.pt", weights_only=True)  # [1397,T] bool
    ped_valid = full_valid[torch.tensor(full_idx)].contiguous()
    torch.save(ped_valid, V2_DIR / "ped_valid_mask.pt")
    log.info(f"  ped_valid_mask: {int(ped_valid.sum()):,}/{ped_valid.numel():,} bins valid")

    # ── supervision masks in v2 node order ────────────────────────────────────
    ped_sensor_mask = torch.tensor(v2_ni["is_ped_sensor"].values.astype(bool))
    parking_mask    = torch.tensor(v2_ni["is_park_sensor"].values.astype(bool))
    torch.save(ped_sensor_mask, V2_DIR / "ped_sensor_mask.pt")
    torch.save(parking_mask,    V2_DIR / "parking_mask.pt")
    log.info(f"  ped_sensor_mask: {int(ped_sensor_mask.sum())}/{N}; "
             f"parking_mask: {int(parking_mask.sum())}/{N}")

    # ── adjacencies on the v2 graphs ──────────────────────────────────────────
    spatial  = pd.read_parquet(V2_DIR / "spatial_edges.parquet")
    semantic = pd.read_parquet(V2_DIR / "semantic_edges.parquet")
    adj_s   = _build_normalised_adj(spatial,  N, "weight")
    adj_sem = _build_normalised_adj(semantic, N, "weight")
    torch.save(adj_s,   V2_DIR / "graph_spatial.pt")
    torch.save(adj_sem, V2_DIR / "graph_semantic.pt")
    log.info(f"  Adjacencies: spatial {adj_s._nnz()} nnz, semantic {adj_sem._nnz()} nnz")

    # ── norm stats + meta ─────────────────────────────────────────────────────
    norm_stats = _norm_stats(sub_cube)
    (V2_DIR / "norm_stats.json").write_text(json.dumps(norm_stats, indent=2))

    meta = {
        "N": N, "T": int(T), "F": int(Fdim),
        "feature_names": FEATURE_NAMES,
        "node_order": v2_sids,
        "train_frac": TRAIN_FRAC, "val_frac": VAL_FRAC,
        "T_train_end": int(T * TRAIN_FRAC),
        "T_val_end":   int(T * (TRAIN_FRAC + VAL_FRAC)),
        "experiment":  "v2_sensor_union_fresh_graph",
    }
    (V2_DIR / "cube_meta.json").write_text(json.dumps(meta, indent=2))
    log.info(f"  Wrote cube.npy, graphs, norm_stats, cube_meta, masks to {V2_DIR}")
    return {"cube": V2_DIR / "cube.npy", "meta": V2_DIR / "cube_meta.json"}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(name)s  %(levelname)s  %(message)s")
    run()
