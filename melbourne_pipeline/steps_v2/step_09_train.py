"""
Step 09 (v2) — Train MultiGCN on the sensor-union fresh graph.

EXPERIMENT (steps_v2): the 189-node real-sensor-union model.
  - Graph: fresh k-NN spatial + mutual-k-NN semantic (steps_v2/step_04).
  - Ped loss masked to the 74 real ped-sensor nodes (+ ped_valid outage mask).
  - Parking loss masked to the 143 real parking-sensor nodes.
  - 3 seeds (42, 1, 2) -> mean +/- std.

The MultiGCN architecture and all data primitives are imported UNCHANGED from the
frozen steps.step_09_train, so this differs from the production / ExpA / ExpB runs
only in (a) which graph + nodes are used and (b) the loss mask — a fair ablation.

Comparison anchors (test ped MAE on the 74 sensor streets, same cube generation):
  ExpA  (74-node induced subgraph)        54.71 +/- 1.32
  Deployed (full graph, unmasked loss)    28.02
  ExpB  (full graph, masked loss)         24.97 +/- 0.28
  V2    (this run)                        -> ?

Run (Lightning or local GPU):
  cd melbourne_pipeline && python -m steps_v2.step_09_train
"""
import json
import logging
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import torch
import torch.nn as nn
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau

from config import PROCESSED_DIR, denormalise_feature
# Architecture + data primitives imported UNCHANGED from the frozen trainer.
from steps.step_09_train import (
    MultiGCN, _normalise_cube, _sample_windows,
    WINDOW, HIDDEN, GRU_LAYERS, DROPOUT, LR,
    MAX_EPOCHS, PATIENCE, BATCHES_EPOCH, BATCH_SIZE, PARK_WEIGHT,
)

log = logging.getLogger(__name__)

V2_DIR        = PROCESSED_DIR.parent / "processed_v2"
MODELS_V2_DIR = PROCESSED_DIR.parent / "models_v2"

SEEDS        = (42, 1, 2)
N_EVAL_FIXED = 128
TARGET_FI    = 0   # ped_flow
PARK_FI      = 1   # occupancy_rate


def _seed(s: int):
    torch.manual_seed(s); np.random.seed(s); random.seed(s)


def _fixed_starts(t0: int, t1: int) -> np.ndarray:
    max_start = t1 - WINDOW - 1
    return np.linspace(t0, max(t0 + 1, max_start), N_EVAL_FIXED, dtype=int)


def _evaluate(model, cube_norm, t0, t1, norm_stats, feat_names, device,
              starts, ped_sensor_idx, parking_idx, ped_valid_np):
    """Ped MAE/R2 on the sensor nodes (valid bins) + all-node ped MAE + parking."""
    ped_name = feat_names[TARGET_FI]
    mu_p  = norm_stats[feat_names[PARK_FI]]["mean"]
    std_p = norm_stats[feat_names[PARK_FI]]["std"]

    P_ped, Y_ped, M_ped, P_pk, Y_pk = [], [], [], [], []
    model.eval()
    with torch.no_grad():
        for i in range(0, len(starts), BATCH_SIZE):
            bs = starts[i:i + BATCH_SIZE]
            X, y_ped, y_park, m_ped = _sample_windows(
                cube_norm, t0, t1, len(bs), WINDOW, TARGET_FI, PARK_FI, device,
                fixed_starts=bs - t0, ped_valid_mask=ped_valid_np, return_ped_mask=True,
            )
            pred_ped, pred_park = model(X)
            P_ped.append(denormalise_feature(pred_ped.cpu().numpy(), ped_name, norm_stats))
            Y_ped.append(denormalise_feature(y_ped.cpu().numpy(),    ped_name, norm_stats))
            M_ped.append(m_ped.cpu().numpy())
            P_pk.append(pred_park.cpu().numpy() * std_p + mu_p)
            Y_pk.append(y_park.cpu().numpy()    * std_p + mu_p)

    pred_ped = np.clip(np.concatenate(P_ped), 0, None)   # [n, N, 1]
    y_ped    = np.concatenate(Y_ped)
    m_ped    = np.concatenate(M_ped).astype(bool)

    def _masked_mae_r2(idx):
        p = pred_ped[:, idx, :]; y = y_ped[:, idx, :]; m = m_ped[:, idx, :]
        if m.sum() == 0:
            return float("nan"), float("nan")
        err = np.abs(p - y)[m]
        mae = float(err.mean())
        yy = y[m]; ss_res = float(np.sum((yy - p[m]) ** 2))
        ss_tot = float(np.sum((yy - yy.mean()) ** 2))
        return mae, float(1 - ss_res / (ss_tot + 1e-8))

    ped_mae_sensor, ped_r2_sensor = _masked_mae_r2(ped_sensor_idx)
    ped_mae_all, _ = _masked_mae_r2(np.arange(pred_ped.shape[1]))

    pred_pk = np.clip(np.concatenate(P_pk), 0, 1)[:, parking_idx, :]
    y_pk    = np.concatenate(Y_pk)[:, parking_idx, :]
    park_mae = float(np.mean(np.abs(pred_pk - y_pk)))
    ss_res = float(np.sum((y_pk - pred_pk) ** 2)); ss_tot = float(np.sum((y_pk - y_pk.mean()) ** 2))
    park_r2 = float(1 - ss_res / (ss_tot + 1e-8))

    return {
        "ped_mae_sensor": round(ped_mae_sensor, 4), "ped_r2_sensor": round(ped_r2_sensor, 4),
        "ped_mae_all": round(ped_mae_all, 4),
        "park_mae": round(park_mae, 4), "park_r2": round(park_r2, 4),
    }


def _train_one(seed, cube_norm, norm_stats, feat_names, device,
               adj_s, adj_sem, masks, splits):
    ped_sensor, parking, ped_valid_np = masks
    T_train, T_val, T = splits
    N = cube_norm.shape[0]
    _seed(seed)

    model = MultiGCN(n_feat=cube_norm.shape[2], hidden=HIDDEN, n_nodes=N,
                     adj_s=adj_s, adj_sem=adj_sem,
                     gru_layers=GRU_LAYERS, dropout=DROPOUT).to(device)
    opt = Adam(model.parameters(), lr=LR)
    sched = ReduceLROnPlateau(opt, mode="min", factor=0.5, patience=8)

    ped_w_node = torch.tensor(ped_sensor, dtype=torch.float32, device=device).view(1, N, 1)
    park_idx_t = torch.tensor(parking, dtype=torch.bool, device=device)
    ped_sensor_idx = np.where(ped_sensor)[0]
    parking_idx    = np.where(parking)[0]
    val_starts  = _fixed_starts(T_train, T_val)
    test_starts = _fixed_starts(T_val, T)

    best_val, best_state, best_ep, no_imp = float("inf"), None, 0, 0
    t0 = time.time()
    for ep in range(1, MAX_EPOCHS + 1):
        model.train()
        for _ in range(BATCHES_EPOCH // BATCH_SIZE):
            X, y_ped, y_park, m_ped = _sample_windows(
                cube_norm, 0, T_train, BATCH_SIZE, WINDOW, TARGET_FI, PARK_FI, device,
                ped_valid_mask=ped_valid_np, return_ped_mask=True,
            )
            opt.zero_grad()
            pred_ped, pred_park = model(X)
            # ped loss: masked to sensor nodes AND valid (non-outage) bins
            w = ped_w_node * m_ped                                   # [B, N, 1]
            ped_loss = (torch.abs(pred_ped - y_ped) * w).sum() / w.sum().clamp(min=1.0)
            park_loss = torch.abs(pred_park[:, park_idx_t, :] - y_park[:, park_idx_t, :]).mean()
            loss = ped_loss + PARK_WEIGHT * park_loss
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()

        val = _evaluate(model, cube_norm, T_train, T_val, norm_stats, feat_names, device,
                        val_starts, ped_sensor_idx, parking_idx, ped_valid_np)
        vmae = val["ped_mae_sensor"]
        sched.step(vmae)
        if ep % 10 == 0 or ep == 1:
            log.info(f"  [seed {seed}] ep {ep:3d}  val ped MAE(sensor)={vmae:.3f}  R2={val['ped_r2_sensor']:.3f}")
        if vmae < best_val - 1e-4:
            best_val, best_ep, no_imp = vmae, ep, 0
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            no_imp += 1
            if no_imp >= PATIENCE:
                log.info(f"  [seed {seed}] early stop at ep {ep}")
                break

    model.load_state_dict({k: v.to(device) for k, v in best_state.items()})
    test = _evaluate(model, cube_norm, T_val, T, norm_stats, feat_names, device,
                     test_starts, ped_sensor_idx, parking_idx, ped_valid_np)
    val_final = _evaluate(model, cube_norm, T_train, T_val, norm_stats, feat_names, device,
                          val_starts, ped_sensor_idx, parking_idx, ped_valid_np)
    elapsed = time.time() - t0
    log.info(f"  [seed {seed}] done {elapsed:.0f}s, best ep {best_ep}, "
             f"TEST ped MAE(sensor)={test['ped_mae_sensor']}")
    return {"seed": seed, "best_epoch": best_ep, "train_time_s": round(elapsed, 1),
            "val": val_final, "test": test}, best_state, best_val


def run() -> dict[str, Path]:
    log.info("=== Step 09 (v2): sensor-union fresh-graph training ===")
    MODELS_V2_DIR.mkdir(parents=True, exist_ok=True)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"  Device: {device}")

    meta       = json.loads((V2_DIR / "cube_meta.json").read_text())
    norm_stats = json.loads((V2_DIR / "norm_stats.json").read_text())
    feat_names = meta["feature_names"]
    T = meta["T"]; T_train = meta["T_train_end"]; T_val = meta["T_val_end"]

    cube_raw  = np.load(V2_DIR / "cube.npy")
    cube_norm = _normalise_cube(cube_raw, norm_stats, feat_names)
    del cube_raw

    adj_s   = torch.load(V2_DIR / "graph_spatial.pt",  map_location=device, weights_only=False)
    adj_sem = torch.load(V2_DIR / "graph_semantic.pt", map_location=device, weights_only=False)

    ped_sensor = torch.load(V2_DIR / "ped_sensor_mask.pt", weights_only=True).numpy()
    parking    = torch.load(V2_DIR / "parking_mask.pt",    weights_only=True).numpy()
    ped_valid  = torch.load(V2_DIR / "ped_valid_mask.pt",  weights_only=True).numpy()
    log.info(f"  N={meta['N']}  ped-sensor nodes={int(ped_sensor.sum())}  "
             f"parking nodes={int(parking.sum())}")

    runs, best_overall, best_overall_state = [], float("inf"), None
    for s in SEEDS:
        r, state, vbest = _train_one(
            s, cube_norm, norm_stats, feat_names, device,
            adj_s, adj_sem, (ped_sensor, parking, ped_valid), (T_train, T_val, T),
        )
        runs.append(r)
        if vbest < best_overall:
            best_overall, best_overall_state = vbest, state

    def _agg(key):
        vals = [r["test"][key] for r in runs]
        return {"mean": round(float(np.mean(vals)), 4), "std": round(float(np.std(vals)), 4),
                "per_seed": {str(r["seed"]): r["test"][key] for r in runs}}

    summary = {
        "experiment": "v2_sensor_union_fresh_graph",
        "n_nodes": meta["N"], "ped_sensor_nodes": int(ped_sensor.sum()),
        "parking_nodes": int(parking.sum()), "seeds": list(SEEDS),
        "test_ped_mae_sensor": _agg("ped_mae_sensor"),
        "test_ped_r2_sensor":  _agg("ped_r2_sensor"),
        "test_ped_mae_all":    _agg("ped_mae_all"),
        "test_park_mae":       _agg("park_mae"),
        "runs": runs,
    }
    (MODELS_V2_DIR / "model_eval_v2.json").write_text(json.dumps(summary, indent=2))
    if best_overall_state is not None:
        torch.save({k: v for k, v in best_overall_state.items()}, MODELS_V2_DIR / "best_model_v2.pt")

    m = summary["test_ped_mae_sensor"]
    log.info("=" * 60)
    log.info(f"  V2 TEST ped MAE (74 sensors): {m['mean']} +/- {m['std']}")
    log.info(f"  vs ExpA 54.71 | Deployed 28.02 | ExpB 24.97")
    log.info(f"  Saved -> {MODELS_V2_DIR / 'model_eval_v2.json'}")
    log.info("=" * 60)
    return {"eval": MODELS_V2_DIR / "model_eval_v2.json"}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(name)s  %(levelname)s  %(message)s")
    run()
