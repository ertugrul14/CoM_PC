"""
Ablation experiment: does imputing 1,323 streets help the GCN?

Three conditions compared on the SAME 74 sensor streets:

  Baseline     — existing full model (best_model.pt), evaluated on sensor streets only
  Experiment A — 74-node subgraph, trained on real sensor data only (no imputation)
  Experiment B — full 1,397-node graph, ped loss masked to 74 sensor streets only

All use identical hyperparameters, seed, and chronological split.

Usage:
    cd melbourne_pipeline
    python -m experiments.ablation_sensor_only
"""
import json
import logging
import random
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import PROCESSED_DIR, denormalise_feature
from steps.step_08_cube import _build_normalised_adj, NORMALISE_IDX, FEATURE_NAMES
from steps.step_09_train import (
    MultiGCN,
    _normalise_cube,
    _sample_windows,
    WINDOW,
    HIDDEN,
    GRU_LAYERS,
    DROPOUT,
    LR,
    MAX_EPOCHS,
    PATIENCE,
    BATCHES_EPOCH,
    BATCH_SIZE,
    SEED,
    PARK_WEIGHT,
    TRAIN_FRAC,
    VAL_FRAC,
    N_EVAL_FIXED,
)

log = logging.getLogger(__name__)
MODELS_DIR = PROCESSED_DIR.parent / "models"
RESULTS_DIR = PROCESSED_DIR.parent / "experiments"


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

# Seeds for the multi-seed ablation. Reporting mean +/- std across these makes the
# "imputation does not help sensor-street accuracy" claim robust to seed noise
# rather than resting on a single lucky/unlucky run. 3 seeds keeps the Lightning
# GPU bill bounded (ExpB ~1h/seed); bump to (42,1,2,3,4) for tighter error bars.
SEEDS = (42, 1, 2)


def _seed_everything(seed: int = SEED):
    """Seed torch/numpy/random for a reproducible single ablation run."""
    torch.manual_seed(seed)
    np.random.seed(seed)
    random.seed(seed)


def _identify_sensor_nodes() -> tuple[np.ndarray, dict[int, int]]:
    """Return sorted sensor node indices and old→new re-indexing map."""
    sm = pd.read_parquet(PROCESSED_DIR / "sensor_map.parquet")
    ni = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    ni["street_id"] = ni["street_id"].astype(str)

    ped_street_ids = set(
        sm[sm["sensor_type"] == "pedestrian"]["street_id"]
        .dropna().astype(str).unique()
    )
    sensor_ni = ni[ni["street_id"].isin(ped_street_ids)]
    indices = np.sort(sensor_ni["node_idx"].values)
    old_to_new = {int(old): new for new, old in enumerate(indices)}
    return indices, old_to_new


def _build_parking_mask(
    node_indices: np.ndarray | None,
    n_nodes: int,
    device: torch.device,
    old_to_new: dict[int, int] | None = None,
) -> torch.Tensor:
    """Build bool parking mask for a given set of node indices."""
    park_df = pd.read_parquet(PROCESSED_DIR / "parking_occupancy.parquet")
    valid_streets = set(
        park_df[park_df["valid_parking"]]["street_id"].astype(str).unique()
    )
    ni = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    ni["street_id"] = ni["street_id"].astype(str)
    park_nodes_full = set(
        ni[ni["street_id"].isin(valid_streets)]["node_idx"].values
    )

    mask = torch.zeros(n_nodes, dtype=torch.bool, device=device)
    if old_to_new is not None:
        for old_idx in park_nodes_full:
            if old_idx in old_to_new:
                mask[old_to_new[old_idx]] = True
    else:
        for idx in park_nodes_full:
            if node_indices is None or idx in set(node_indices):
                mask[idx] = True
    return mask


def _compute_norm_stats(cube_sub: np.ndarray, feat_names: list) -> dict:
    """Recompute per-feature mean/std on the first 80% of T."""
    T = cube_sub.shape[1]
    T_stat = int(T * 0.8)
    stats = {}
    for fi, name in enumerate(feat_names):
        if fi in NORMALISE_IDX:
            col = cube_sub[:, :T_stat, fi]
            stats[name] = {
                "mean": float(np.mean(col)),
                "std": float(max(np.std(col), 1e-8)),
            }
    return stats


def _extract_subgraph(
    edges_path: Path,
    old_to_new: dict[int, int],
    n_new: int,
    weight_col: str,
    device: torch.device,
) -> tuple[torch.Tensor, int]:
    """Filter edges to subgraph nodes, remap indices, build normalised adj."""
    edges = pd.read_parquet(edges_path)
    valid = set(old_to_new.keys())
    sub = edges[
        edges["node_i"].isin(valid) & edges["node_j"].isin(valid)
    ].copy()
    sub["node_i"] = sub["node_i"].map(old_to_new)
    sub["node_j"] = sub["node_j"].map(old_to_new)
    n_edges = len(sub)
    adj = _build_normalised_adj(sub, n_new, weight_col).to(device)
    return adj, n_edges


def _fixed_eval_starts(t_start: int, t_end: int) -> np.ndarray:
    """Compute deterministic evaluation window start positions."""
    max_start = t_end - WINDOW - 1
    return np.linspace(
        t_start, max(t_start + 1, max_start), N_EVAL_FIXED, dtype=int
    )


def _evaluate_masked(
    model: MultiGCN,
    cube_norm: np.ndarray,
    t_start: int,
    t_end: int,
    target_fi: int,
    park_fi: int,
    norm_stats: dict,
    feat_names: list,
    device: torch.device,
    fixed_starts: np.ndarray,
    parking_mask: torch.Tensor,
    ped_eval_mask: np.ndarray | None = None,
) -> dict:
    """Evaluate and return metrics, optionally filtering ped to a node subset."""
    ped_name = feat_names[target_fi]   # "ped_flow" — log1p-normalised (D-012)
    mu_park = norm_stats[feat_names[park_fi]]["mean"]
    std_park = norm_stats[feat_names[park_fi]]["std"]
    park_idx = parking_mask.cpu().numpy()

    all_pred_ped, all_y_ped = [], []
    all_pred_park, all_y_park = [], []

    model.eval()
    with torch.no_grad():
        for i in range(0, len(fixed_starts), BATCH_SIZE):
            batch_starts = fixed_starts[i : i + BATCH_SIZE]
            X, y_ped, y_park = _sample_windows(
                cube_norm, t_start, t_end, len(batch_starts),
                WINDOW, target_fi, park_fi, device,
                fixed_starts=batch_starts - t_start,
            )
            pred_ped, pred_park = model(X)
            # D-012: ped_flow is log1p-normalised — invert with expm1(z*std+mu),
            # NOT linear z*std+mu, so MAE is reported in raw pedestrian counts.
            all_pred_ped.append(denormalise_feature(pred_ped.cpu().numpy(), ped_name, norm_stats))
            all_y_ped.append(denormalise_feature(y_ped.cpu().numpy(), ped_name, norm_stats))
            all_pred_park.append(pred_park.cpu().numpy() * std_park + mu_park)
            all_y_park.append(y_park.cpu().numpy() * std_park + mu_park)

    pred_ped_raw = np.clip(np.concatenate(all_pred_ped, axis=0), 0, None)
    y_ped_raw = np.concatenate(all_y_ped, axis=0)

    # Ped metrics — optionally filtered to sensor streets
    if ped_eval_mask is not None:
        p = pred_ped_raw[:, ped_eval_mask, :]
        y = y_ped_raw[:, ped_eval_mask, :]
    else:
        p, y = pred_ped_raw, y_ped_raw

    ped_mae = float(np.mean(np.abs(p - y)))
    ped_rmse = float(np.sqrt(np.mean((p - y) ** 2)))
    ss_res = np.sum((y - p) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    ped_r2 = float(1 - ss_res / (ss_tot + 1e-8))

    # Parking metrics — masked to parking sensor streets
    pred_park_raw = np.clip(np.concatenate(all_pred_park, axis=0), 0, 1)
    y_park_raw = np.concatenate(all_y_park, axis=0)
    pp = pred_park_raw[:, park_idx, :]
    yp = y_park_raw[:, park_idx, :]
    park_mae = float(np.mean(np.abs(pp - yp)))
    park_rmse = float(np.sqrt(np.mean((pp - yp) ** 2)))
    ss_res_p = np.sum((yp - pp) ** 2)
    ss_tot_p = np.sum((yp - yp.mean()) ** 2)
    park_r2 = float(1 - ss_res_p / (ss_tot_p + 1e-8))

    return {
        "ped_mae": round(ped_mae, 4),
        "ped_rmse": round(ped_rmse, 4),
        "ped_r2": round(ped_r2, 4),
        "park_mae": round(park_mae, 4),
        "park_rmse": round(park_rmse, 4),
        "park_r2": round(park_r2, 4),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Training loop (shared by both experiments)
# ══════════════════════════════════════════════════════════════════════════════

def _train_loop(
    model: MultiGCN,
    cube_norm: np.ndarray,
    norm_stats: dict,
    feat_names: list,
    parking_mask: torch.Tensor,
    device: torch.device,
    T_train_end: int,
    T_val_end: int,
    T: int,
    val_fixed_starts: np.ndarray,
    test_fixed_starts: np.ndarray,
    ped_loss_mask: torch.Tensor | None = None,
    ped_eval_mask: np.ndarray | None = None,
    label: str = "",
) -> dict:
    """Train model and return best val/test metrics."""
    target_fi = feat_names.index("ped_flow")
    park_fi = feat_names.index("occupancy_rate")
    criterion = nn.L1Loss()

    optimiser = Adam(model.parameters(), lr=LR)
    scheduler = ReduceLROnPlateau(optimiser, mode="min", factor=0.5, patience=8)

    best_val_mae = float("inf")
    best_state = None
    no_improve = 0
    best_epoch = 0

    n_params = sum(p.numel() for p in model.parameters())
    log.info(f"  [{label}] Parameters: {n_params:,}")
    t0 = time.time()

    for epoch in range(1, MAX_EPOCHS + 1):
        model.train()
        epoch_loss = 0.0
        n_steps = 0

        for _ in range(BATCHES_EPOCH // BATCH_SIZE):
            X, y_ped, y_park = _sample_windows(
                cube_norm, 0, T_train_end, BATCH_SIZE,
                WINDOW, target_fi, park_fi, device,
            )
            optimiser.zero_grad()
            pred_ped, pred_park = model(X)

            if ped_loss_mask is not None:
                ped_loss = criterion(
                    pred_ped[:, ped_loss_mask, :],
                    y_ped[:, ped_loss_mask, :],
                )
            else:
                ped_loss = criterion(pred_ped, y_ped)

            park_loss = criterion(
                pred_park[:, parking_mask, :],
                y_park[:, parking_mask, :],
            )
            loss = ped_loss + PARK_WEIGHT * park_loss

            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimiser.step()
            epoch_loss += loss.item()
            n_steps += 1

        train_loss = epoch_loss / max(n_steps, 1)

        val_metrics = _evaluate_masked(
            model, cube_norm, T_train_end, T_val_end,
            target_fi, park_fi, norm_stats, feat_names, device,
            val_fixed_starts, parking_mask, ped_eval_mask,
        )
        val_mae = val_metrics["ped_mae"]
        scheduler.step(val_mae)

        if epoch % 10 == 0 or epoch == 1:
            log.info(
                f"  [{label}] Epoch {epoch:3d}  loss={train_loss:.4f}  "
                f"ped_MAE={val_mae:.3f}  ped_R2={val_metrics['ped_r2']:.3f}"
            )

        if val_mae < best_val_mae - 1e-4:
            best_val_mae = val_mae
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            best_epoch = epoch
            no_improve = 0
        else:
            no_improve += 1
            if no_improve >= PATIENCE:
                log.info(f"  [{label}] Early stop at epoch {epoch}")
                break

    elapsed = time.time() - t0
    log.info(f"  [{label}] Training done in {elapsed:.0f}s, best epoch {best_epoch}")

    # Reload best weights
    model.load_state_dict({k: v.to(device) for k, v in best_state.items()})
    model.eval()

    val_final = _evaluate_masked(
        model, cube_norm, T_train_end, T_val_end,
        target_fi, park_fi, norm_stats, feat_names, device,
        val_fixed_starts, parking_mask, ped_eval_mask,
    )
    test_final = _evaluate_masked(
        model, cube_norm, T_val_end, T,
        target_fi, park_fi, norm_stats, feat_names, device,
        test_fixed_starts, parking_mask, ped_eval_mask,
    )

    return {
        "n_params": n_params,
        "best_epoch": best_epoch,
        "training_time_s": round(elapsed, 1),
        "val": val_final,
        "test": test_final,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Baseline: evaluate existing full model on sensor streets
# ══════════════════════════════════════════════════════════════════════════════

def evaluate_baseline(device: torch.device) -> dict:
    log.info("=== Baseline: existing full model on sensor streets ===")

    meta = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    norm_stats = json.loads((PROCESSED_DIR / "norm_stats.json").read_text())
    feat_names = meta["feature_names"]
    N, T, F = meta["N"], meta["T"], meta["F"]
    target_fi = feat_names.index("ped_flow")
    park_fi = feat_names.index("occupancy_rate")

    T_train_end = int(T * TRAIN_FRAC)
    T_val_end = int(T * (TRAIN_FRAC + VAL_FRAC))

    sensor_indices, _ = _identify_sensor_nodes()
    sensor_mask_np = np.zeros(N, dtype=bool)
    sensor_mask_np[sensor_indices] = True

    adj_s = torch.load(PROCESSED_DIR / "graph_spatial.pt",
                       map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt",
                         map_location=device, weights_only=False)

    model = MultiGCN(
        n_feat=F, hidden=HIDDEN, n_nodes=N,
        adj_s=adj_s, adj_sem=adj_sem,
        gru_layers=GRU_LAYERS, dropout=DROPOUT,
    ).to(device)
    state = torch.load(MODELS_DIR / "best_model.pt",
                       map_location=device, weights_only=True)
    model.load_state_dict(state)
    model.eval()

    cube_raw = np.load(PROCESSED_DIR / "cube.npy")
    cube_norm = _normalise_cube(cube_raw, norm_stats, feat_names)
    del cube_raw

    parking_mask = _build_parking_mask(None, N, device)
    val_starts = _fixed_eval_starts(T_train_end, T_val_end)
    test_starts = _fixed_eval_starts(T_val_end, T)

    # All-street metrics (reproduces model_eval.json)
    val_all = _evaluate_masked(
        model, cube_norm, T_train_end, T_val_end,
        target_fi, park_fi, norm_stats, feat_names, device,
        val_starts, parking_mask,
    )
    test_all = _evaluate_masked(
        model, cube_norm, T_val_end, T,
        target_fi, park_fi, norm_stats, feat_names, device,
        test_starts, parking_mask,
    )

    # Sensor-only metrics
    val_sensor = _evaluate_masked(
        model, cube_norm, T_train_end, T_val_end,
        target_fi, park_fi, norm_stats, feat_names, device,
        val_starts, parking_mask, ped_eval_mask=sensor_mask_np,
    )
    test_sensor = _evaluate_masked(
        model, cube_norm, T_val_end, T,
        target_fi, park_fi, norm_stats, feat_names, device,
        test_starts, parking_mask, ped_eval_mask=sensor_mask_np,
    )

    log.info(f"  All streets  — val MAE={val_all['ped_mae']:.3f}  "
             f"test MAE={test_all['ped_mae']:.3f}")
    log.info(f"  Sensor only  — val MAE={val_sensor['ped_mae']:.3f}  "
             f"test MAE={test_sensor['ped_mae']:.3f}")

    return {
        "all_streets": {"val": val_all, "test": test_all},
        "sensor_only": {"val": val_sensor, "test": test_sensor},
    }


# ══════════════════════════════════════════════════════════════════════════════
# Experiment A: 74-node sensor-only subgraph
# ══════════════════════════════════════════════════════════════════════════════

def run_experiment_a(device: torch.device, seed: int = SEED) -> dict:
    log.info(f"=== Experiment A: 74-node sensor subgraph (seed={seed}) ===")
    _seed_everything(seed)

    meta = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    feat_names = meta["feature_names"]
    T = meta["T"]
    F = meta["F"]

    T_train_end = int(T * TRAIN_FRAC)
    T_val_end = int(T * (TRAIN_FRAC + VAL_FRAC))

    sensor_indices, old_to_new = _identify_sensor_nodes()
    N_sub = len(sensor_indices)
    log.info(f"  Subgraph nodes: {N_sub}")

    # Slice cube to sensor streets only
    cube_raw = np.load(PROCESSED_DIR / "cube.npy")
    cube_sub = cube_raw[sensor_indices, :, :].copy()
    del cube_raw

    # Recompute norm_stats for the 74-street subcube
    sub_norm_stats = _compute_norm_stats(cube_sub, feat_names)
    cube_norm = _normalise_cube(cube_sub, sub_norm_stats, feat_names)
    del cube_sub

    # Extract subgraphs
    adj_s, n_spatial = _extract_subgraph(
        PROCESSED_DIR / "spatial_edges.parquet",
        old_to_new, N_sub, "weight", device,
    )
    adj_sem, n_semantic = _extract_subgraph(
        PROCESSED_DIR / "semantic_edges.parquet",
        old_to_new, N_sub, "weight", device,
    )

    # Count isolated nodes
    spatial_edges = pd.read_parquet(PROCESSED_DIR / "spatial_edges.parquet")
    valid = set(old_to_new.keys())
    sub_edges = spatial_edges[
        spatial_edges["node_i"].isin(valid) & spatial_edges["node_j"].isin(valid)
    ]
    connected = set(sub_edges["node_i"]) | set(sub_edges["node_j"])
    n_isolated = N_sub - len(connected & valid)
    log.info(f"  Spatial edges: {n_spatial}, semantic edges: {n_semantic}, "
             f"isolated nodes: {n_isolated}/{N_sub}")

    parking_mask = _build_parking_mask(
        sensor_indices, N_sub, device, old_to_new
    )
    log.info(f"  Parking mask: {int(parking_mask.sum())}/{N_sub}")

    model = MultiGCN(
        n_feat=F, hidden=HIDDEN, n_nodes=N_sub,
        adj_s=adj_s, adj_sem=adj_sem,
        gru_layers=GRU_LAYERS, dropout=DROPOUT,
    ).to(device)

    val_starts = _fixed_eval_starts(T_train_end, T_val_end)
    test_starts = _fixed_eval_starts(T_val_end, T)

    results = _train_loop(
        model, cube_norm, sub_norm_stats, feat_names,
        parking_mask, device, T_train_end, T_val_end, T,
        val_starts, test_starts,
        ped_loss_mask=None,
        ped_eval_mask=None,
        label="ExpA",
    )

    results["n_nodes"] = N_sub
    results["spatial_edges"] = n_spatial
    results["semantic_edges"] = n_semantic
    results["isolated_nodes"] = n_isolated
    results["parking_streets"] = int(parking_mask.sum())
    results["description"] = (
        f"{N_sub}-node subgraph, real sensor data only, "
        f"{n_isolated} isolated nodes"
    )
    return results


# ══════════════════════════════════════════════════════════════════════════════
# Experiment B: full graph, ped loss masked to sensor streets
# ══════════════════════════════════════════════════════════════════════════════

def run_experiment_b(device: torch.device, seed: int = SEED) -> dict:
    log.info(f"=== Experiment B: full graph, sensor-loss only (seed={seed}) ===")
    _seed_everything(seed)

    meta = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    norm_stats = json.loads((PROCESSED_DIR / "norm_stats.json").read_text())
    feat_names = meta["feature_names"]
    N, T, F = meta["N"], meta["T"], meta["F"]

    T_train_end = int(T * TRAIN_FRAC)
    T_val_end = int(T * (TRAIN_FRAC + VAL_FRAC))

    sensor_indices, _ = _identify_sensor_nodes()
    sensor_mask_bool = np.zeros(N, dtype=bool)
    sensor_mask_bool[sensor_indices] = True
    ped_loss_mask = torch.tensor(sensor_mask_bool, dtype=torch.bool, device=device)

    adj_s = torch.load(PROCESSED_DIR / "graph_spatial.pt",
                       map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt",
                         map_location=device, weights_only=False)

    cube_raw = np.load(PROCESSED_DIR / "cube.npy")
    cube_norm = _normalise_cube(cube_raw, norm_stats, feat_names)
    del cube_raw

    parking_mask = _build_parking_mask(None, N, device)
    log.info(f"  Ped loss mask: {int(ped_loss_mask.sum())}/{N} sensor streets")
    log.info(f"  Parking mask: {int(parking_mask.sum())}/{N}")

    model = MultiGCN(
        n_feat=F, hidden=HIDDEN, n_nodes=N,
        adj_s=adj_s, adj_sem=adj_sem,
        gru_layers=GRU_LAYERS, dropout=DROPOUT,
    ).to(device)

    val_starts = _fixed_eval_starts(T_train_end, T_val_end)
    test_starts = _fixed_eval_starts(T_val_end, T)

    # Train with ped loss masked to sensor streets, evaluate on sensor streets
    results = _train_loop(
        model, cube_norm, norm_stats, feat_names,
        parking_mask, device, T_train_end, T_val_end, T,
        val_starts, test_starts,
        ped_loss_mask=ped_loss_mask,
        ped_eval_mask=sensor_mask_bool,
        label="ExpB",
    )

    # Also evaluate on all streets for comparison
    target_fi = feat_names.index("ped_flow")
    park_fi = feat_names.index("occupancy_rate")
    val_all = _evaluate_masked(
        model, cube_norm, T_train_end, T_val_end,
        target_fi, park_fi, norm_stats, feat_names, device,
        val_starts, parking_mask,
    )
    test_all = _evaluate_masked(
        model, cube_norm, T_val_end, T,
        target_fi, park_fi, norm_stats, feat_names, device,
        test_starts, parking_mask,
    )
    results["val_all_streets"] = val_all
    results["test_all_streets"] = test_all
    results["description"] = (
        "Full 1397-node graph, ped loss masked to 74 sensor streets"
    )
    return results


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    log.info("=" * 60)
    log.info("Ablation: sensor-only vs full-graph training")
    log.info("=" * 60)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"Device: {device}")

    # Load original metrics for reference
    original_eval = json.loads((MODELS_DIR / "model_eval.json").read_text())

    # Baseline is a deterministic eval of the existing model — no seed loop needed.
    baseline = evaluate_baseline(device)

    # Experiments A and B are retrained once per seed so we can report mean +/- std
    # and show the A-vs-B gap is not a single-seed artifact.
    exp_a_runs, exp_b_runs = [], []
    for seed in SEEDS:
        exp_a_runs.append(run_experiment_a(device, seed))
        exp_b_runs.append(run_experiment_b(device, seed))

    def _agg(runs: list[dict], key_path: tuple[str, str]) -> dict:
        """Mean/std of a nested metric (e.g. ('test','ped_mae')) across seed runs."""
        vals = [r[key_path[0]][key_path[1]] for r in runs]
        return {
            "mean": round(float(np.mean(vals)), 4),
            "std":  round(float(np.std(vals)), 4),
            "per_seed": {str(s): v for s, v in zip(SEEDS, vals)},
        }

    exp_a_summary = {
        "seeds": list(SEEDS),
        "test_ped_mae": _agg(exp_a_runs, ("test", "ped_mae")),
        "test_ped_r2":  _agg(exp_a_runs, ("test", "ped_r2")),
        "val_ped_mae":  _agg(exp_a_runs, ("val", "ped_mae")),
        "runs": exp_a_runs,
    }
    exp_b_summary = {
        "seeds": list(SEEDS),
        "test_ped_mae": _agg(exp_b_runs, ("test", "ped_mae")),
        "test_ped_r2":  _agg(exp_b_runs, ("test", "ped_r2")),
        "val_ped_mae":  _agg(exp_b_runs, ("val", "ped_mae")),
        "runs": exp_b_runs,
    }

    results = {
        "config": {
            "seeds": list(SEEDS),
            "ped_denorm": "log1p (expm1 inverse) — D-012 contract",
            "note": (
                "ExpA recomputes norm_stats on the 74-street subcube; ExpB uses the "
                "global cube stats. Each model is normalised to its own training "
                "distribution, so the comparison is fair, but the stats differ by design."
            ),
        },
        "original_full_model": {
            "description": "Original model metrics from model_eval.json",
            "val_ped_mae": original_eval.get("val_ped_mae"),
            "val_ped_r2": original_eval.get("val_ped_r2"),
            "test_ped_mae": original_eval.get("test_ped_mae"),
            "test_ped_r2": original_eval.get("test_ped_r2"),
            "best_epoch": original_eval.get("best_epoch"),
            "n_params": original_eval.get("n_params"),
        },
        "baseline_full_model_sensor_eval": {
            "description": "Existing model, ped metrics on 74 sensor streets only",
            **baseline,
        },
        "experiment_a_sensor_subgraph": exp_a_summary,
        "experiment_b_full_graph_sensor_loss": exp_b_summary,
    }

    # Summary table
    a_mae = exp_a_summary["test_ped_mae"]
    b_mae = exp_b_summary["test_ped_mae"]
    log.info("")
    log.info("=" * 70)
    log.info(f"RESULTS SUMMARY (test ped MAE on sensor streets, {len(SEEDS)} seeds)")
    log.info("-" * 70)
    log.info(f"  Original (all streets):     "
             f"{original_eval.get('test_ped_mae', '?')}")
    log.info(f"  Baseline (sensor eval):     "
             f"{baseline['sensor_only']['test']['ped_mae']}")
    log.info(f"  Exp A (74-node subgraph):   "
             f"{a_mae['mean']} +/- {a_mae['std']}")
    log.info(f"  Exp B (sensor-loss only):   "
             f"{b_mae['mean']} +/- {b_mae['std']}")
    log.info(f"  A vs B gap (B-A):           "
             f"{round(b_mae['mean'] - a_mae['mean'], 4)} "
             f"(positive => imputation HURTS sensor accuracy)")
    log.info("=" * 70)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULTS_DIR / "ablation_results.json"
    out_path.write_text(json.dumps(results, indent=2))
    log.info(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
