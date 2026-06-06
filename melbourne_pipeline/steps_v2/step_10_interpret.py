"""
Step 10 — Permutation feature importance + GCN branch attention.

For each of the 23 features:
  1. Replace that feature column with Gaussian noise in the val set
  2. Measure MAE increase vs baseline → importance score
  3. Repeat 3 times, take mean

Also computes per-branch contribution by zeroing each GCN branch in turn.

Inputs:  data/processed/cube.npy, cube_meta.json, norm_stats.json
         data/processed/graph_spatial.pt, graph_semantic.pt
         data/models/best_model.pt, run_config.json
Outputs: data/models/feature_importance.json
"""
import json
import logging
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn

from config import PROCESSED_DIR, denormalise_feature
from steps.step_09_train import (
    MultiGCN, _normalise_cube, _sample_windows, WINDOW, HIDDEN,
    GRU_LAYERS, DROPOUT, BATCH_SIZE,
)

log = logging.getLogger(__name__)
MODELS_DIR = PROCESSED_DIR.parent / "models"
N_REPEATS  = 3
N_EVAL     = 64


def _baseline_mae(model, cube_norm, T_train, T, target_fi, park_fi,
                  norm_stats, feat_names, device, n_eval=N_EVAL):
    mu  = norm_stats[feat_names[target_fi]]["mean"]
    std = norm_stats[feat_names[target_fi]]["std"]
    all_pred, all_y = [], []
    model.eval()
    with torch.no_grad():
        for _ in range(0, n_eval, BATCH_SIZE):
            X, y_n, _ = _sample_windows(cube_norm, T_train, T, BATCH_SIZE,
                                        WINDOW, target_fi, park_fi, device)
            p_ped, _ = model(X)
            # D-012: expm1 de-norm for log-normalised ped_flow (via helper)
            p = denormalise_feature(p_ped.cpu().numpy(), feat_names[target_fi], norm_stats)
            all_pred.append(np.clip(p, 0, None))
            all_y.append(denormalise_feature(y_n.cpu().numpy(), feat_names[target_fi], norm_stats))
    pred = np.concatenate(all_pred); y = np.concatenate(all_y)
    return float(np.mean(np.abs(pred - y)))


def _perturbed_mae(model, cube_norm, T_train, T, target_fi, feat_idx,
                   park_fi, norm_stats, feat_names, device, n_eval=N_EVAL):
    """Replace feature feat_idx with N(0,1) noise, measure MAE."""
    mu  = norm_stats[feat_names[target_fi]]["mean"]
    std = norm_stats[feat_names[target_fi]]["std"]
    all_pred, all_y = [], []
    model.eval()
    with torch.no_grad():
        for _ in range(0, n_eval, BATCH_SIZE):
            X, y_n, _ = _sample_windows(cube_norm, T_train, T, BATCH_SIZE,
                                        WINDOW, target_fi, park_fi, device)
            X_p = X.clone()
            X_p[:, :, :, feat_idx] = torch.randn_like(X_p[:, :, :, feat_idx])
            p_ped, _ = model(X_p)
            # D-012: expm1 de-norm for log-normalised ped_flow (via helper)
            p = denormalise_feature(p_ped.cpu().numpy(), feat_names[target_fi], norm_stats)
            all_pred.append(np.clip(p, 0, None))
            all_y.append(denormalise_feature(y_n.cpu().numpy(), feat_names[target_fi], norm_stats))
    pred = np.concatenate(all_pred); y = np.concatenate(all_y)
    return float(np.mean(np.abs(pred - y)))


def run() -> dict[str, Path]:
    log.info("=== Step 10: Feature importance ===")

    meta       = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    norm_stats = json.loads((PROCESSED_DIR / "norm_stats.json").read_text())
    run_cfg    = json.loads((MODELS_DIR / "run_config.json").read_text())
    feat_names = meta["feature_names"]
    N, T, F    = meta["N"], meta["T"], meta["F"]
    T_train    = meta["T_train_end"]
    target_fi  = feat_names.index("ped_flow")
    park_fi    = feat_names.index("occupancy_rate")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"  Device: {device}")

    adj_s   = torch.load(PROCESSED_DIR / "graph_spatial.pt",
                         map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt",
                         map_location=device, weights_only=False)

    model = MultiGCN(n_feat=F, hidden=HIDDEN, n_nodes=N,
                     adj_s=adj_s, adj_sem=adj_sem,
                     gru_layers=GRU_LAYERS, dropout=DROPOUT).to(device)
    state = torch.load(MODELS_DIR / "best_model.pt",
                       map_location=device, weights_only=True)
    model.load_state_dict(state)
    model.eval()

    log.info("  Loading + normalising cube...")
    cube_raw  = np.load(PROCESSED_DIR / "cube.npy")
    cube_norm = _normalise_cube(cube_raw, norm_stats, feat_names)
    del cube_raw

    baseline = _baseline_mae(model, cube_norm, T_train, T,
                              target_fi, park_fi, norm_stats, feat_names, device)
    log.info(f"  Baseline val MAE: {baseline:.3f}")

    # ── Permutation importance ────────────────────────────────────────────────
    scores: dict[str, float] = {}
    for fi, name in enumerate(feat_names):
        deltas = []
        for _ in range(N_REPEATS):
            mae_p = _perturbed_mae(model, cube_norm, T_train, T, target_fi,
                                   fi, park_fi, norm_stats, feat_names, device)
            deltas.append(mae_p - baseline)
        importance = float(np.mean(deltas))
        scores[name] = round(importance, 4)
        log.info(f"  {name:<28} delta_MAE={importance:+.4f}")

    # Sort descending
    sorted_scores = dict(sorted(scores.items(), key=lambda x: -x[1]))

    # ── Branch contribution ───────────────────────────────────────────────────
    def _branch_mae(zero_spatial: bool) -> float:
        """Zero one GCN branch's projection weights, measure MAE."""
        orig_s   = {k: v.clone() for k, v in model.proj_s.named_parameters()}
        orig_sem = {k: v.clone() for k, v in model.proj_sem.named_parameters()}
        with torch.no_grad():
            if zero_spatial:
                for p in model.proj_s.parameters():   p.zero_()
            else:
                for p in model.proj_sem.parameters(): p.zero_()
        mae = _baseline_mae(model, cube_norm, T_train, T,
                            target_fi, park_fi, norm_stats, feat_names, device)
        with torch.no_grad():
            for name_p, p in model.proj_s.named_parameters():
                p.copy_(orig_s[name_p])
            for name_p, p in model.proj_sem.named_parameters():
                p.copy_(orig_sem[name_p])
        return mae

    mae_no_spatial  = _branch_mae(zero_spatial=True)
    mae_no_semantic = _branch_mae(zero_spatial=False)
    branch_contrib = {
        "spatial_branch_delta_mae":  round(mae_no_spatial  - baseline, 4),
        "semantic_branch_delta_mae": round(mae_no_semantic - baseline, 4),
    }
    log.info(f"  Branch contribution: {branch_contrib}")

    out = {
        "baseline_mae": round(baseline, 4),
        "feature_importance": sorted_scores,
        "branch_contribution": branch_contrib,
    }
    out_path = MODELS_DIR / "feature_importance.json"
    out_path.write_text(json.dumps(out, indent=2))
    log.info(f"  Saved feature_importance.json")
    log.info("Step 10 complete.")
    return {"feature_importance": out_path}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s")
    run()
