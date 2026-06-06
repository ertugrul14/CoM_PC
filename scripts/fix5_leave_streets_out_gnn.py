"""
Fix 5 (decisive) — Leave-streets-out GNN test: is the GNN enough WITHOUT XGBoost?

Question
--------
The pipeline stacks two models:  XGBoost (step_05) imputes ped_flow on the 1,323
unsensored streets  ->  the MultiGCN (step_09) message-passes over all 1,397 nodes.
Does the GNN actually NEED the XGBoost pre-imputation, or can it reconstruct an
unsensored street's flow itself from the graph + static features + the OTHER sensors?

Design (leave-M-sensored-streets-out, K folds)
----------------------------------------------
For each fold we hold out M of the 74 sensor streets and treat them as if they were
unsensored.  The held-out streets are removed from the ped LOSS (never supervised) and
their REAL sensor values are kept aside as the honest answer key.  We then train the GNN
twice, differing ONLY in what the held-out nodes carry in the ped_flow INPUT channel:

  Arm A (GNN + XGBoost):  held-out nodes filled with a leakage-free XGBoost imputation
                          (XGBoost refit on the retained sensors only, lag recomputed
                          treating held-out streets as unsensored).
  Arm B (GNN alone):      held-out nodes filled with a dumb city-climatology rhythm
                          (mean flow of retained sensors per time bin — NO XGBoost).

Both arms are graded on the held-out streets' REAL flow.  We also report the standalone
XGBoost imputer MAE on the same held-out streets as a reference point.

Verdict
-------
  Arm A ~= Arm B  ->  the GNN reconstructs flow on its own; the XGBoost stage is
                      redundant -> drop it (simpler, more defensible pipeline).
  Arm A  < Arm B  ->  XGBoost inputs genuinely help the GNN -> keep the stage.

The cube's ped_flow channel is BOTH input and target, so we keep two cubes:
  cube_input  (held-out nodes filled)   feeds the 24h windows
  cube_target (untouched, real)         is the answer key
This prevents grading the GNN on copying XGBoost (which would be circular).

Cost
----
Each arm is a full-graph train (~ ExpB, ~1 h on a Lightning GPU).
Default 3 folds x 2 arms = 6 trains ~= 6 h.  Set SMOKE_TEST=1 for a fast local
shape/plumbing check first (few epochs, tiny eval).  Knobs are env-overridable.

Usage  (run from the project root)
-----
    SMOKE_TEST=1 python scripts/fix5_leave_streets_out_gnn.py   # fast plumbing check (CPU)
    python scripts/fix5_leave_streets_out_gnn.py               # full run (GPU, ~6 h)

Env overrides:  K_FOLDS (default 3), N_SEEDS (1), FOLD_EPOCHS (200), ARMS="A,B", SMOKE_TEST=1
Cost scales as K_FOLDS x len(ARMS) full-graph trains. K_FOLDS=5 is more realistic
(holds out ~15/74 at a time) but costs ~10 trains; K_FOLDS=3 holds out ~25 and costs 6.
Output: data/experiments/fix5_leave_streets_out_results.json
"""
import json
import logging
import os
import random
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import xgboost as xgb
from sklearn.metrics import r2_score
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau

# Make the package importable whether run as -m or as a file.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "melbourne_pipeline"))

from config import (
    PROCESSED_DIR,
    RAW_DIR,
    normalise_feature,
    denormalise_feature,
)
from steps.step_05_process import _compute_spatial_lag
from steps.step_09_train import (
    MultiGCN,
    _normalise_cube,
    WINDOW,
    HIDDEN,
    GRU_LAYERS,
    DROPOUT,
    LR,
    MAX_EPOCHS,
    PATIENCE,
    BATCHES_EPOCH,
    BATCH_SIZE,
    TRAIN_FRAC,
    VAL_FRAC,
    N_EVAL_FIXED,
)

log = logging.getLogger(__name__)
MODELS_DIR = PROCESSED_DIR.parent / "models"
RESULTS_DIR = PROCESSED_DIR.parent / "experiments"

# ── Config (env-overridable) ──────────────────────────────────────────────────
SMOKE_TEST = os.environ.get("SMOKE_TEST", "0") == "1"
K_FOLDS = int(os.environ.get("K_FOLDS", "3"))
N_SEEDS = int(os.environ.get("N_SEEDS", "1"))
FOLD_EPOCHS = int(os.environ.get("FOLD_EPOCHS", str(MAX_EPOCHS)))
ARMS = tuple(a.strip() for a in os.environ.get("ARMS", "A,B").split(","))
BASE_SEED = 42

if SMOKE_TEST:
    # Tiny everything — just proves the pipeline wires up and shapes are right.
    K_FOLDS = 2
    N_SEEDS = 1
    FOLD_EPOCHS = 2
    BATCHES_EPOCH_EFF = 16
    N_EVAL_FIXED_EFF = 8
    XGB_TREES = 40
else:
    BATCHES_EPOCH_EFF = BATCHES_EPOCH
    N_EVAL_FIXED_EFF = N_EVAL_FIXED
    XGB_TREES = 600  # matches step_05 final_model

PARK_WEIGHT = 0.5


# ══════════════════════════════════════════════════════════════════════════════
# Sensor identification + folds
# ══════════════════════════════════════════════════════════════════════════════

def _sensor_streets_and_nodes() -> tuple[list[str], np.ndarray, dict[str, int]]:
    """Return (sensor street_ids, their node indices, street_id->node_idx map)."""
    sm = pd.read_parquet(PROCESSED_DIR / "sensor_map.parquet")
    ni = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    ni["street_id"] = ni["street_id"].astype(str)

    ped_ids = set(
        sm[sm["sensor_type"] == "pedestrian"]["street_id"].dropna().astype(str).unique()
    )
    sub = ni[ni["street_id"].isin(ped_ids)].sort_values("node_idx")
    streets = sub["street_id"].tolist()
    nodes = sub["node_idx"].to_numpy()
    sid_to_node = dict(zip(sub["street_id"], sub["node_idx"].astype(int)))
    return streets, nodes, sid_to_node


def _make_folds(streets: list[str], k: int, seed: int) -> list[list[str]]:
    """Deterministic K-fold partition of sensor streets (held-out groups)."""
    rng = np.random.default_rng(seed)
    order = list(streets)
    rng.shuffle(order)
    return [order[i::k] for i in range(k)]


def _build_parking_mask(n_nodes: int, device: torch.device) -> torch.Tensor:
    """Full-graph parking mask (sensor-observed parking streets), as in step_09."""
    park_df = pd.read_parquet(PROCESSED_DIR / "parking_occupancy.parquet")
    valid = set(park_df[park_df["valid_parking"]]["street_id"].astype(str).unique())
    ni = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    ni["street_id"] = ni["street_id"].astype(str)
    nodes = ni[ni["street_id"].isin(valid)]["node_idx"].values
    mask = torch.zeros(n_nodes, dtype=torch.bool, device=device)
    mask[nodes] = True
    return mask


# ══════════════════════════════════════════════════════════════════════════════
# Faithful XGBoost imputer (Arm A) — leakage-free re-fit on retained sensors
# ══════════════════════════════════════════════════════════════════════════════

def _load_imputer_inputs():
    """Load + assemble the exact feature pieces step_05's imputer uses.

    Returns (static_lookup, tw_index_frame, static_cols, time_index, ped_sensored_long).
    Mirrors step_05._build_ped_complete feature assembly (arterial filter, parking
    stats merge, hour x weekend interactions) so the Arm-A imputation is faithful.
    """
    import geopandas as gpd
    from config import STREETS_GEOJSON, DATA_START, TIME_BIN_MINUTES
    from steps.step_05_process import ARTERIAL_TYPES, N_TIME_BINS

    sensor_map = pd.read_parquet(PROCESSED_DIR / "sensor_map.parquet")
    ped_raw = pd.read_parquet(RAW_DIR / "ped_raw.parquet")
    static = pd.read_parquet(PROCESSED_DIR / "static_features.parquet")
    temporal = pd.read_parquet(PROCESSED_DIR / "temporal_features.parquet")
    weather = pd.read_parquet(PROCESSED_DIR / "weather.parquet")
    parking_occ = pd.read_parquet(PROCESSED_DIR / "parking_occupancy.parquet")

    # Arterial-only filter (matches step_05)
    gdf = gpd.read_file(STREETS_GEOJSON)[["street_id", "str_type", "name"]]
    gdf["street_id"] = gdf["street_id"].astype(str)
    keep = (
        gdf["str_type"].isin(ARTERIAL_TYPES)
        & ~gdf["name"].str.contains("Intersection", case=False, na=False)
    )
    arterial_ids = set(gdf.loc[keep, "street_id"])
    static = static[static["street_id"].astype(str).isin(arterial_ids)].reset_index(drop=True)

    time_index = pd.date_range(
        start=DATA_START, periods=N_TIME_BINS, freq=f"{TIME_BIN_MINUTES}min", tz="UTC",
    )

    # Aggregate raw ped counts to 15-min bins per street
    ped_sensors = sensor_map[sensor_map["sensor_type"] == "pedestrian"][["sensor_id", "street_id"]].dropna(subset=["street_id"])
    loc_to_street = dict(zip(ped_sensors["sensor_id"].astype(str), ped_sensors["street_id"]))
    ped_raw["location_id"] = ped_raw["location_id"].astype(str)
    ped_raw["street_id"] = ped_raw["location_id"].map(loc_to_street)
    ped_raw = ped_raw.dropna(subset=["street_id"])
    ped_raw["local_datetime"] = pd.to_datetime(ped_raw["local_datetime"], utc=True)
    ped_raw["time_bin"] = ped_raw["local_datetime"].dt.floor(f"{TIME_BIN_MINUTES}min")
    ped_agg = (
        ped_raw.groupby(["street_id", "time_bin"])["total_of_directions"].sum().reset_index()
        .rename(columns={"total_of_directions": "ped_flow"})
    )
    ped_agg = ped_agg[ped_agg["time_bin"].isin(time_index)]

    # Parking stats merge (matches step_05)
    pstats = (
        parking_occ.groupby("street_id")["occupancy_rate"]
        .agg(parking_mean="mean", parking_std="std").fillna(0).reset_index()
    )
    pstats["has_parking"] = 1.0
    static = static.merge(pstats, on="street_id", how="left")
    for c in ["parking_mean", "parking_std", "has_parking"]:
        static[c] = static[c].fillna(0.0)

    # hour x weekend interactions (matches step_05)
    temporal["time_bin"] = pd.to_datetime(temporal["time_bin"], utc=True)
    weather["time_bin"] = pd.to_datetime(weather["time_bin"], utc=True)
    temporal["hour_sin_x_weekend"] = temporal["hour_sin"] * temporal["is_weekend"]
    temporal["hour_cos_x_weekend"] = temporal["hour_cos"] * temporal["is_weekend"]

    tw = temporal.merge(weather, on="time_bin", how="left").set_index("time_bin")
    static_cols = [c for c in static.columns if c != "street_id"]
    static_lookup = static.set_index("street_id")
    return static_lookup, tw, static_cols, time_index, ped_agg


def _make_X(streets_list, time_index, static_lookup, static_cols, tw, lag_pivot):
    """Replicate step_05._make_X feature matrix (street-major rows)."""
    n_times = len(time_index)
    tw_vals = tw.loc[time_index].values.astype(np.float32)
    n_tw = tw_vals.shape[1]
    n_static = len(static_cols)
    rows = np.empty((len(streets_list) * n_times, n_static + n_tw + 1), dtype=np.float32)
    for i, sid in enumerate(streets_list):
        sl = slice(i * n_times, (i + 1) * n_times)
        if sid in static_lookup.index:
            rows[sl, :n_static] = static_lookup.loc[sid, static_cols].values.astype(np.float32)
        else:
            rows[sl, :n_static] = 0.0
        rows[sl, n_static:n_static + n_tw] = tw_vals
        rows[sl, -1] = lag_pivot[sid].values.astype(np.float32) if sid in lag_pivot.columns else 0.0
    return rows


def _xgb_impute_heldout(
    held_out: list[str],
    retained: list[str],
    imputer_inputs,
) -> dict[str, np.ndarray]:
    """Leakage-free XGBoost imputation of held-out streets.

    Trains on `retained` sensors only; spatial lag for held-out streets is recomputed
    treating ONLY `retained` as sensored (held-out are pretend-unsensored).
    Returns {street_id -> raw ped_flow array of length T}.
    """
    static_lookup, tw, static_cols, time_index, ped_agg = imputer_inputs

    # ped_sensored long frame for the RETAINED streets only (training signal + lag source)
    retained_set = set(retained)
    ped_ret = ped_agg[ped_agg["street_id"].isin(retained_set)].copy()
    grid = pd.MultiIndex.from_product(
        [sorted(retained_set), time_index], names=["street_id", "time_bin"]
    ).to_frame(index=False)
    ped_sensored = grid.merge(ped_ret, on=["street_id", "time_bin"], how="left")
    ped_sensored["ped_flow"] = ped_sensored["ped_flow"].fillna(0.0).astype(np.float32)

    # Spatial lag: held-out + retained as targets, but only RETAINED count as sensored.
    all_streets = sorted(retained_set | set(held_out))
    lag_pivot = _compute_spatial_lag(ped_sensored, all_streets, sorted(retained_set))

    # Training matrix on retained streets
    X_train = _make_X(sorted(retained_set), time_index, static_lookup, static_cols, tw, lag_pivot)
    y_raw = ped_sensored.sort_values(["street_id", "time_bin"])["ped_flow"].values
    y_train = np.log1p(y_raw)

    model = xgb.XGBRegressor(
        n_estimators=XGB_TREES, max_depth=6, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
        tree_method="hist", random_state=42, verbosity=0,
    )
    model.fit(X_train, y_train)

    X_pred = _make_X(held_out, time_index, static_lookup, static_cols, tw, lag_pivot)
    pred = np.expm1(np.clip(model.predict(X_pred), 0, None)).astype(np.float32)
    n_t = len(time_index)
    return {sid: pred[i * n_t:(i + 1) * n_t] for i, sid in enumerate(held_out)}


# ══════════════════════════════════════════════════════════════════════════════
# Cube fills + dual-cube window sampling
# ══════════════════════════════════════════════════════════════════════════════

def _city_climatology(cube_target_norm, retained_nodes, ped_fi) -> np.ndarray:
    """Dumb 'no-XGBoost' baseline: mean (normalised) ped_flow of retained sensors per bin."""
    return cube_target_norm[retained_nodes, :, ped_fi].mean(axis=0)  # [T]


def _sample_windows_dual(
    cube_in, cube_tgt, t_start, t_end, n, window, ped_fi, park_fi, device,
    fixed_starts=None,
):
    """Sample windows: X from cube_in (filled), targets from cube_tgt (real). [+park]"""
    N, T, F = cube_in.shape
    max_start = t_end - window - 1
    if fixed_starts is not None:
        idx = fixed_starts[:n] % max(1, max_start - t_start)
        starts = np.clip(t_start + idx, t_start, max_start - 1)
    else:
        starts = np.random.randint(t_start, max_start, size=n)

    X, yp, yk = [], [], []
    for s in starts:
        X.append(cube_in[:, s:s + window, :].transpose(1, 0, 2))
        yp.append(cube_tgt[:, s + window, ped_fi][:, None])
        yk.append(cube_tgt[:, s + window, park_fi][:, None])
    return (
        torch.tensor(np.stack(X), dtype=torch.float32, device=device),
        torch.tensor(np.stack(yp), dtype=torch.float32, device=device),
        torch.tensor(np.stack(yk), dtype=torch.float32, device=device),
    )


def _eval_heldout(
    model, cube_in, cube_tgt, t_start, t_end, ped_fi, park_fi,
    norm_stats, feat_names, device, fixed_starts, held_mask_np,
):
    """MAE/R2 on held-out streets' REAL flow (raw counts, clipped at 0)."""
    ped_name = feat_names[ped_fi]
    preds, ys = [], []
    model.eval()
    with torch.no_grad():
        for i in range(0, len(fixed_starts), BATCH_SIZE):
            bs = fixed_starts[i:i + BATCH_SIZE]
            X, yp, _ = _sample_windows_dual(
                cube_in, cube_tgt, t_start, t_end, len(bs), WINDOW, ped_fi, park_fi,
                device, fixed_starts=bs - t_start,
            )
            pp, _ = model(X)
            preds.append(denormalise_feature(pp.cpu().numpy(), ped_name, norm_stats))
            ys.append(denormalise_feature(yp.cpu().numpy(), ped_name, norm_stats))
    p = np.clip(np.concatenate(preds, axis=0), 0, None)[:, held_mask_np, :]
    y = np.concatenate(ys, axis=0)[:, held_mask_np, :]
    mae = float(np.mean(np.abs(p - y)))
    ss_res = np.sum((y - p) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    return {"ped_mae": round(mae, 4), "ped_r2": round(float(1 - ss_res / (ss_tot + 1e-8)), 4)}


# ══════════════════════════════════════════════════════════════════════════════
# Train one arm
# ══════════════════════════════════════════════════════════════════════════════

def _train_arm(
    cube_in, cube_tgt, norm_stats, feat_names, parking_mask, device,
    T_train_end, T_val_end, T, val_starts, test_starts,
    ped_loss_mask, held_mask_np, label, seed,
):
    """Train GNN with ped loss on supervised streets; eval on held-out real flow."""
    torch.manual_seed(seed); np.random.seed(seed); random.seed(seed)
    ped_fi = feat_names.index("ped_flow")
    park_fi = feat_names.index("occupancy_rate")

    adj_s = torch.load(PROCESSED_DIR / "graph_spatial.pt", map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt", map_location=device, weights_only=False)
    model = MultiGCN(
        n_feat=cube_in.shape[2], hidden=HIDDEN, n_nodes=cube_in.shape[0],
        adj_s=adj_s, adj_sem=adj_sem, gru_layers=GRU_LAYERS, dropout=DROPOUT,
    ).to(device)

    opt = Adam(model.parameters(), lr=LR)
    sched = ReduceLROnPlateau(opt, mode="min", factor=0.5, patience=8)
    crit = nn.L1Loss()
    best_mae, best_state, no_imp = float("inf"), None, 0
    t0 = time.time()

    for epoch in range(1, FOLD_EPOCHS + 1):
        model.train()
        for _ in range(BATCHES_EPOCH_EFF // BATCH_SIZE):
            X, yp, yk = _sample_windows_dual(
                cube_in, cube_tgt, 0, T_train_end, BATCH_SIZE, WINDOW, ped_fi, park_fi, device,
            )
            opt.zero_grad()
            pp, pk = model(X)
            ped_loss = crit(pp[:, ped_loss_mask, :], yp[:, ped_loss_mask, :])
            park_loss = crit(pk[:, parking_mask, :], yk[:, parking_mask, :])
            loss = ped_loss + PARK_WEIGHT * park_loss
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()

        # Early stopping on held-out val MAE (the quantity of interest)
        vm = _eval_heldout(model, cube_in, cube_tgt, T_train_end, T_val_end,
                           ped_fi, park_fi, norm_stats, feat_names, device,
                           val_starts, held_mask_np)["ped_mae"]
        sched.step(vm)
        if vm < best_mae - 1e-4:
            best_mae, no_imp = vm, 0
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            no_imp += 1
            if no_imp >= PATIENCE:
                break

    model.load_state_dict({k: v.to(device) for k, v in best_state.items()})
    val = _eval_heldout(model, cube_in, cube_tgt, T_train_end, T_val_end, ped_fi, park_fi,
                       norm_stats, feat_names, device, val_starts, held_mask_np)
    test = _eval_heldout(model, cube_in, cube_tgt, T_val_end, T, ped_fi, park_fi,
                        norm_stats, feat_names, device, test_starts, held_mask_np)
    log.info(f"  [{label}] done {time.time()-t0:.0f}s  "
             f"val MAE={val['ped_mae']}  test MAE={test['ped_mae']}")
    return {"val": val, "test": test, "train_s": round(time.time() - t0, 1)}


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info("=" * 70)
    log.info(f"Fix 5 leave-streets-out GNN test  |  device={device}  smoke={SMOKE_TEST}")
    log.info(f"K_FOLDS={K_FOLDS}  N_SEEDS={N_SEEDS}  FOLD_EPOCHS={FOLD_EPOCHS}  arms={ARMS}")
    log.info("=" * 70)

    meta = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    norm_stats = json.loads((PROCESSED_DIR / "norm_stats.json").read_text())
    feat_names = meta["feature_names"]
    N, T = meta["N"], meta["T"]
    ped_fi = feat_names.index("ped_flow")
    T_train_end = int(T * TRAIN_FRAC)
    T_val_end = int(T * (TRAIN_FRAC + VAL_FRAC))

    def _starts(a, b):
        return np.linspace(a, max(a + 1, b - WINDOW - 1), N_EVAL_FIXED_EFF, dtype=int)
    val_starts, test_starts = _starts(T_train_end, T_val_end), _starts(T_val_end, T)

    sensor_streets, _, sid_to_node = _sensor_streets_and_nodes()
    log.info(f"Sensor streets: {len(sensor_streets)}")
    parking_mask = _build_parking_mask(N, device)

    # Real cube (answer key) — normalised once, reused across folds/arms.
    log.info("Loading + normalising cube (real target)...")
    cube_raw = np.load(PROCESSED_DIR / "cube.npy")
    cube_target = _normalise_cube(cube_raw, norm_stats, feat_names)
    del cube_raw

    imputer_inputs = _load_imputer_inputs() if "A" in ARMS else None

    results = {
        "config": {"k_folds": K_FOLDS, "n_seeds": N_SEEDS, "fold_epochs": FOLD_EPOCHS,
                   "arms": list(ARMS), "smoke_test": SMOKE_TEST, "n_sensor_streets": len(sensor_streets)},
        "folds": [],
    }

    for seed in range(BASE_SEED, BASE_SEED + N_SEEDS):
        folds = _make_folds(sensor_streets, K_FOLDS, seed)
        for fi, held_out in enumerate(folds):
            retained = [s for s in sensor_streets if s not in set(held_out)]
            held_nodes = np.array([sid_to_node[s] for s in held_out if s in sid_to_node])
            retained_nodes = np.array([sid_to_node[s] for s in retained if s in sid_to_node])
            held_mask_np = np.zeros(N, dtype=bool); held_mask_np[held_nodes] = True
            ped_loss_mask = torch.zeros(N, dtype=torch.bool, device=device)
            ped_loss_mask[retained_nodes] = True  # supervise retained sensors only

            log.info("-" * 70)
            log.info(f"seed={seed} fold={fi}: held_out={len(held_out)} retained={len(retained)}")
            fold_rec = {"seed": seed, "fold": fi, "n_held_out": len(held_out), "arms": {}}

            # Reference: standalone XGBoost imputer MAE on held-out (Arm A's fill, scored directly)
            xgb_fill = None
            if "A" in ARMS:
                t0 = time.time()
                xgb_fill = _xgb_impute_heldout(held_out, retained, imputer_inputs)
                # Score the imputer itself on real held-out flow. cube_target holds the
                # log1p z-scored real values; denormalise_feature inverts to raw counts.
                real = np.clip(np.array([
                    denormalise_feature(cube_target[sid_to_node[s], :, ped_fi], feat_names[ped_fi], norm_stats)
                    for s in held_out if s in sid_to_node
                ]), 0, None)
                pred = np.array([xgb_fill[s] for s in held_out if s in sid_to_node])
                xgb_mae = float(np.mean(np.abs(np.clip(pred, 0, None) - real)))
                fold_rec["xgb_only_mae"] = round(xgb_mae, 4)
                log.info(f"  [XGB-only] impute+score {time.time()-t0:.0f}s  MAE={xgb_mae:.4f}")

            for arm in ARMS:
                cube_in = cube_target.copy()
                if arm == "A":  # XGBoost fill at held-out nodes (normalised injection)
                    for s in held_out:
                        if s in sid_to_node:
                            cube_in[sid_to_node[s], :, ped_fi] = normalise_feature(
                                xgb_fill[s], feat_names[ped_fi], norm_stats)
                elif arm == "B":  # dumb city-climatology fill (no XGBoost)
                    clim = _city_climatology(cube_target, retained_nodes, ped_fi)
                    for n in held_nodes:
                        cube_in[n, :, ped_fi] = clim
                else:
                    raise ValueError(f"unknown arm {arm}")

                rec = _train_arm(
                    cube_in, cube_target, norm_stats, feat_names, parking_mask, device,
                    T_train_end, T_val_end, T, val_starts, test_starts,
                    ped_loss_mask, held_mask_np, label=f"Arm{arm}.f{fi}", seed=seed,
                )
                fold_rec["arms"][arm] = rec
                del cube_in

            results["folds"].append(fold_rec)

    # ── Aggregate verdict ─────────────────────────────────────────────────────
    def _mean(arm, split):
        vals = [f["arms"][arm][split]["ped_mae"] for f in results["folds"] if arm in f["arms"]]
        return round(float(np.mean(vals)), 4) if vals else None

    a_test, b_test = _mean("A", "test"), _mean("B", "test")
    xgb_only = [f.get("xgb_only_mae") for f in results["folds"] if f.get("xgb_only_mae") is not None]
    results["summary"] = {
        "armA_gnn_xgboost_test_mae": a_test,
        "armB_gnn_alone_test_mae": b_test,
        "xgb_only_test_mae": round(float(np.mean(xgb_only)), 4) if xgb_only else None,
        "gap_B_minus_A": round(b_test - a_test, 4) if (a_test and b_test) else None,
        "verdict_hint": (
            "A≈B → GNN reconstructs without XGBoost (drop stage); "
            "A≪B → XGBoost input helps (keep stage)."
        ),
    }
    log.info("=" * 70)
    log.info(f"SUMMARY  ArmA(GNN+XGB)={a_test}  ArmB(GNN alone)={b_test}  "
             f"XGB-only={results['summary']['xgb_only_test_mae']}  gap(B-A)={results['summary']['gap_B_minus_A']}")
    log.info("=" * 70)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / ("fix5_smoke_results.json" if SMOKE_TEST else "fix5_leave_streets_out_results.json")
    out.write_text(json.dumps(results, indent=2))
    log.info(f"Saved: {out}")


if __name__ == "__main__":
    main()
