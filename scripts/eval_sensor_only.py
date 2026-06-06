"""
Disaggregated evaluation of the trained MultiGCN: pedestrian accuracy on the
74 real ped-sensor streets vs. all 1,397 streets, on the TEST split.

This produces the figure directly comparable to the ablation (Exp A/B, ~22-30
MAE on sensor streets) — which the all-street model_eval.json does NOT report.

Read-only. No training. Deterministic (seed 42, fixed eval windows).
Run from project root:  python scripts/eval_sensor_only.py
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import torch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "melbourne_pipeline"))

from config import PROCESSED_DIR, MODELS_DIR, denormalise_feature
from steps.step_09_train import (
    MultiGCN, _normalise_cube, _sample_windows,
    WINDOW, HIDDEN, GRU_LAYERS, DROPOUT,
)

SEED = 42
N_EVAL_FIXED = 128
EVAL_BATCH = 8


def _metrics(pred, y):
    pred = np.clip(pred, 0, None)
    mae = float(np.mean(np.abs(pred - y)))
    rmse = float(np.sqrt(np.mean((pred - y) ** 2)))
    ss_res = np.sum((y - pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = float(1 - ss_res / (ss_tot + 1e-8))
    return mae, rmse, r2


def main():
    np.random.seed(SEED)
    torch.manual_seed(SEED)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    meta = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    norm_stats = json.loads((PROCESSED_DIR / "norm_stats.json").read_text())
    feat_names = meta["feature_names"]
    node_order = [str(s) for s in meta["node_order"]]
    N, T, F = meta["N"], meta["T"], meta["F"]
    target_fi = feat_names.index("ped_flow")
    park_fi = feat_names.index("occupancy_rate")

    # Splits (same protocol as training)
    T_train_end = int(T * 0.70)
    T_val_end = int(T * 0.85)

    # ── Identify the 74 real ped-sensor streets ───────────────────────────────
    ped = pd.read_parquet(PROCESSED_DIR / "ped_complete.parquet")
    sensor_ids = set(ped.loc[ped["source"] == "sensor", "street_id"].astype(str).unique())
    sensor_nodes = np.array([i for i, s in enumerate(node_order) if s in sensor_ids])
    print(f"Sensor ped streets: {len(sensor_nodes)} / {N}")

    # ── Load model + cube + mask ──────────────────────────────────────────────
    cube_raw = np.load(PROCESSED_DIR / "cube.npy")
    cube_norm = _normalise_cube(cube_raw, norm_stats, feat_names)
    del cube_raw
    ped_valid = torch.load(PROCESSED_DIR / "ped_valid_mask.pt").numpy().astype(bool)  # [N,T]

    adj_s = torch.load(PROCESSED_DIR / "graph_spatial.pt", map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt", map_location=device, weights_only=False)
    model = MultiGCN(n_feat=F, hidden=HIDDEN, n_nodes=N, adj_s=adj_s, adj_sem=adj_sem,
                     gru_layers=GRU_LAYERS, dropout=DROPOUT).to(device)
    model.load_state_dict(torch.load(MODELS_DIR / "best_model.pt", map_location=device, weights_only=True))
    model.eval()

    def evaluate(t_start, t_end, label):
        max_start = t_end - WINDOW - 1
        starts = np.linspace(t_start, max(t_start + 1, max_start), N_EVAL_FIXED, dtype=int)
        preds, ys, masks = [], [], []
        with torch.no_grad():
            for i in range(0, len(starts), EVAL_BATCH):
                bs = starts[i:i + EVAL_BATCH]
                X, y_ped, _, m = _sample_windows(
                    cube_norm, t_start, t_end, len(bs), WINDOW, target_fi, park_fi, device,
                    fixed_starts=bs - t_start, ped_valid_mask=ped_valid, return_ped_mask=True,
                )
                p, _ = model(X)
                preds.append(denormalise_feature(p.cpu().numpy(), "ped_flow", norm_stats))  # [b,N,1]
                ys.append(denormalise_feature(y_ped.cpu().numpy(), "ped_flow", norm_stats))
                masks.append(m.cpu().numpy().astype(bool))
        pred = np.concatenate(preds, 0)   # [n,N,1]
        y = np.concatenate(ys, 0)
        msk = np.concatenate(masks, 0)

        # all-street (valid bins)
        all_mae, all_rmse, all_r2 = _metrics(pred[msk], y[msk])
        # sensor-only (valid bins, sensor nodes)
        sp, sy, sm = pred[:, sensor_nodes, :], y[:, sensor_nodes, :], msk[:, sensor_nodes, :]
        sen_mae, sen_rmse, sen_r2 = _metrics(sp[sm], sy[sm])
        print(f"\n[{label}]")
        print(f"  ALL 1397   ped  MAE={all_mae:6.3f}  RMSE={all_rmse:6.2f}  R2={all_r2:.4f}")
        print(f"  74 SENSOR  ped  MAE={sen_mae:6.3f}  RMSE={sen_rmse:6.2f}  R2={sen_r2:.4f}")
        return {"all": {"mae": all_mae, "rmse": all_rmse, "r2": all_r2},
                "sensor": {"mae": sen_mae, "rmse": sen_rmse, "r2": sen_r2}}

    out = {
        "n_sensor_streets": int(len(sensor_nodes)),
        "val": evaluate(T_train_end, T_val_end, "VAL"),
        "test": evaluate(T_val_end, T, "TEST"),
    }
    (PROCESSED_DIR / "sensor_only_eval.json").write_text(json.dumps(out, indent=2))
    print(f"\nSaved -> {PROCESSED_DIR / 'sensor_only_eval.json'}")


if __name__ == "__main__":
    main()
