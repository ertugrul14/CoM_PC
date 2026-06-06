"""
Standalone Lightning AI training script — Step 09 MultiGCN (dual-head).

Usage (after extracting training_bundle.tar.gz):
    pip install -r requirements_training.txt
    python train_lightning.py

Outputs written to:  data/models/
  best_model.pt, parking_mask.pt, run_config.json, model_eval.json, cube_meta.json
"""
import json
import logging
import random
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau

# All paths relative to this script
BASE_DIR      = Path(__file__).resolve().parent
PROCESSED_DIR = BASE_DIR / "melbourne_pipeline" / "data" / "processed"
MODELS_DIR    = BASE_DIR / "melbourne_pipeline" / "data" / "models"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# ── Hyperparameters ────────────────────────────────────────────────────────────
WINDOW        = 96       # 24 h of 15-min bins
HIDDEN        = 64       # GCN hidden dim per branch (×2 after concat)
GRU_LAYERS    = 2
DROPOUT       = 0.1
LR            = 1e-3
MAX_EPOCHS    = 300      # raised from 200 — previous run did not early-stop
PATIENCE      = 25       # early-stop on val ped MAE
BATCHES_EPOCH = 256      # gradient steps per epoch
BATCH_SIZE    = 8        # windows per gradient step
SEED          = 42
PARK_WEIGHT   = 0.5      # weight of parking MAE in combined loss

# Features log1p-transformed before z-score (must match config.LOG_NORMALISE_FEATURES
# and step_08's stats computation). ped_flow only — heavy-tailed count. (D-012)
LOG_NORMALISE_FEATURES = ("ped_flow",)

# D-016 (Phase 3): scope of the pedestrian training loss.
#   "all"     — train ped head on all 1,397 streets (imputed targets included).
#   "sensors" — train ped head ONLY on the 74 real ped-sensor streets; the imputed
#               streets remain in the graph as context but are never targets.
# The ablation showed "sensors" gives far better accuracy on the verifiable streets
# (Exp B ~22.8 vs all-street model ~31 sensor MAE). Imputed streets stay connective
# tissue either way. Set to "sensors" for the honest run; "all" reproduces the
# previous production model.
PED_LOSS_SCOPE = "sensors"

TRAIN_FRAC = 0.70
VAL_FRAC   = 0.15
N_EVAL_FIXED = 128


# ==============================================================================
# Model
# ==============================================================================

def _sparse_batched(adj: torch.Tensor, x: torch.Tensor) -> torch.Tensor:
    """Apply sparse adj [N, N] to dense x [K, N, H] → [K, N, H]."""
    K, N, H = x.shape
    x_t   = x.permute(1, 0, 2).reshape(N, K * H)
    out_t = adj @ x_t
    return out_t.reshape(N, K, H).permute(1, 0, 2).contiguous()


class MultiGCN(nn.Module):
    """
    Dual-branch GCN + GRU spatio-temporal model with per-node bias.
    Joint prediction heads for pedestrian flow and parking occupancy.

    Forward:  x [B, W, N, F]  →  (pred_ped [B, N, 1], pred_park [B, N, 1])
    """
    def __init__(self, n_feat: int, hidden: int, n_nodes: int,
                 adj_s: torch.Tensor, adj_sem: torch.Tensor,
                 gru_layers: int = 2, dropout: float = 0.1):
        super().__init__()
        self.hidden  = hidden
        self.n_nodes = n_nodes
        self.register_buffer("adj_s",   adj_s)
        self.register_buffer("adj_sem", adj_sem)

        self.proj_s   = nn.Linear(n_feat, hidden, bias=False)
        self.proj_sem = nn.Linear(n_feat, hidden, bias=False)
        self.act      = nn.ReLU()

        self.gru = nn.GRU(
            input_size=hidden * 2,
            hidden_size=hidden,
            num_layers=gru_layers,
            batch_first=True,
            dropout=dropout if gru_layers > 1 else 0.0,
        )
        self.drop = nn.Dropout(dropout)

        self.head      = nn.Linear(hidden, 1)
        self.node_bias = nn.Parameter(torch.zeros(n_nodes, 1))

        self.head_park      = nn.Linear(hidden, 1)
        self.node_bias_park = nn.Parameter(torch.zeros(n_nodes, 1))

    def forward(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        B, W, N, F = x.shape
        xr = x.reshape(B * W, N, F)

        h_s   = self.act(_sparse_batched(self.adj_s,   self.proj_s(xr)))
        h_sem = self.act(_sparse_batched(self.adj_sem, self.proj_sem(xr)))

        h = torch.cat([h_s, h_sem], dim=-1).reshape(B, W, N, self.hidden * 2)
        h = self.drop(h)

        h_gru = h.permute(0, 2, 1, 3).reshape(B * N, W, self.hidden * 2)
        _, h_last = self.gru(h_gru)
        h_last = h_last[-1].reshape(B, N, self.hidden)

        pred_ped  = self.head(h_last)      + self.node_bias.unsqueeze(0)
        pred_park = self.head_park(h_last) + self.node_bias_park.unsqueeze(0)
        return pred_ped, pred_park


# ==============================================================================
# Data helpers
# ==============================================================================

def _normalise_cube(cube: np.ndarray, norm_stats: dict, feat_names: list) -> np.ndarray:
    cube = cube.copy()
    for fi, name in enumerate(feat_names):
        if name in norm_stats:
            mu  = norm_stats[name]["mean"]
            std = norm_stats[name]["std"]
            vals = cube[:, :, fi]
            if name in LOG_NORMALISE_FEATURES:
                vals = np.log1p(np.maximum(vals, 0.0))   # D-012: stats are in log space
            cube[:, :, fi] = (vals - mu) / std
    return cube


def _sample_windows(
    cube_norm: np.ndarray,
    t_start: int,
    t_end: int,
    n_samples: int,
    window: int,
    target_fi: int,
    park_fi: int,
    device: torch.device,
    fixed_starts: np.ndarray | None = None,
    ped_valid_mask: np.ndarray | None = None,
    return_ped_mask: bool = False,
) -> tuple[torch.Tensor, ...]:
    N, T, F   = cube_norm.shape
    max_start = t_end - window - 1
    if max_start <= t_start:
        raise ValueError(f"Not enough timesteps: t_end={t_end}, window={window}")

    if fixed_starts is not None:
        indices = fixed_starts[:n_samples] % max(1, max_start - t_start)
        starts  = np.clip(t_start + indices, t_start, max_start - 1)
    else:
        starts = np.random.randint(t_start, max_start, size=n_samples)

    X_list, y_ped_list, y_park_list, m_ped_list = [], [], [], []
    for s in starts:
        win = cube_norm[:, s : s + window, :]
        X_list.append(win.transpose(1, 0, 2))
        y_ped_list.append(cube_norm[:, s + window, target_fi][:, np.newaxis])
        y_park_list.append(cube_norm[:, s + window, park_fi][:, np.newaxis])
        if return_ped_mask:   # D-013: ped validity at the target bin
            m_ped_list.append(ped_valid_mask[:, s + window][:, np.newaxis])

    X      = torch.tensor(np.stack(X_list),      dtype=torch.float32, device=device)
    y_ped  = torch.tensor(np.stack(y_ped_list),  dtype=torch.float32, device=device)
    y_park = torch.tensor(np.stack(y_park_list), dtype=torch.float32, device=device)
    if return_ped_mask:
        m_ped = torch.tensor(np.stack(m_ped_list), dtype=torch.float32, device=device)
        return X, y_ped, y_park, m_ped
    return X, y_ped, y_park


def _evaluate(
    model, cube_norm, t_start, t_end, window, target_fi, park_fi,
    norm_stats, feat_names, device, fixed_starts: np.ndarray,
    parking_mask: torch.Tensor, eval_batch: int = 8,
    ped_valid_mask: np.ndarray | None = None,
) -> tuple[float, float, float, float, float, float]:
    mu_ped   = norm_stats[feat_names[target_fi]]["mean"]
    std_ped  = norm_stats[feat_names[target_fi]]["std"]
    mu_park  = norm_stats[feat_names[park_fi]]["mean"]
    std_park = norm_stats[feat_names[park_fi]]["std"]
    ped_is_log = feat_names[target_fi] in LOG_NORMALISE_FEATURES   # D-012
    _denorm_ped = (lambda z: np.expm1(z * std_ped + mu_ped)) if ped_is_log \
                  else (lambda z: z * std_ped + mu_ped)
    park_idx = parking_mask.cpu().numpy()
    n_eval   = len(fixed_starts)

    all_pred_ped, all_y_ped   = [], []
    all_pred_park, all_y_park = [], []
    all_m_ped = []
    want_mask = ped_valid_mask is not None

    model.eval()
    with torch.no_grad():
        for i in range(0, n_eval, eval_batch):
            batch_starts = fixed_starts[i : i + eval_batch]
            sampled = _sample_windows(
                cube_norm, t_start, t_end, len(batch_starts),
                window, target_fi, park_fi, device,
                fixed_starts=batch_starts - t_start,
                ped_valid_mask=ped_valid_mask, return_ped_mask=want_mask,
            )
            if want_mask:
                X, y_ped_norm, y_park_norm, m_ped = sampled
                all_m_ped.append(m_ped.cpu().numpy())
            else:
                X, y_ped_norm, y_park_norm = sampled
            pred_ped_norm, pred_park_norm = model(X)
            all_pred_ped.append(_denorm_ped(pred_ped_norm.cpu().numpy()))
            all_y_ped.append(_denorm_ped(y_ped_norm.cpu().numpy()))
            all_pred_park.append(pred_park_norm.cpu().numpy() * std_park + mu_park)
            all_y_park.append(y_park_norm.cpu().numpy()       * std_park + mu_park)

    pred_ped_raw = np.clip(np.concatenate(all_pred_ped, axis=0), 0, None)
    y_ped_raw    = np.concatenate(all_y_ped, axis=0)
    if want_mask:   # D-013: ped metrics over valid (non-outage) bins only
        m = np.concatenate(all_m_ped, axis=0).astype(bool)
        pred_ped_raw = pred_ped_raw[m]
        y_ped_raw    = y_ped_raw[m]
    ped_mae  = float(np.mean(np.abs(pred_ped_raw - y_ped_raw)))
    ped_rmse = float(np.sqrt(np.mean((pred_ped_raw - y_ped_raw) ** 2)))
    ss_res   = np.sum((y_ped_raw - pred_ped_raw) ** 2)
    ss_tot   = np.sum((y_ped_raw - y_ped_raw.mean()) ** 2)
    ped_r2   = float(1 - ss_res / (ss_tot + 1e-8))

    pred_park_raw    = np.clip(np.concatenate(all_pred_park, axis=0), 0, 1)
    y_park_raw       = np.concatenate(all_y_park, axis=0)
    pred_park_masked = pred_park_raw[:, park_idx, :]
    y_park_masked    = y_park_raw[:, park_idx, :]
    park_mae  = float(np.mean(np.abs(pred_park_masked - y_park_masked)))
    park_rmse = float(np.sqrt(np.mean((pred_park_masked - y_park_masked) ** 2)))
    ss_res_p  = np.sum((y_park_masked - pred_park_masked) ** 2)
    ss_tot_p  = np.sum((y_park_masked - y_park_masked.mean()) ** 2)
    park_r2   = float(1 - ss_res_p / (ss_tot_p + 1e-8))

    model.train()
    return ped_mae, ped_rmse, ped_r2, park_mae, park_rmse, park_r2


# ==============================================================================
# Main
# ==============================================================================

def main():
    log.info("=== MultiGCN training (joint ped + parking heads, D-009 time-aware cap) ===")
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    old_model_path = MODELS_DIR / "best_model.pt"
    if old_model_path.exists():
        shutil.copy2(old_model_path, MODELS_DIR / "best_model_ped_only.pt")
        log.info("  Backed up existing checkpoint to best_model_ped_only.pt")

    torch.manual_seed(SEED)
    np.random.seed(SEED)
    random.seed(SEED)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"  Device: {device}")

    meta       = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    norm_stats = json.loads((PROCESSED_DIR / "norm_stats.json").read_text())
    feat_names = meta["feature_names"]
    N, T, F    = meta["N"], meta["T"], meta["F"]
    target_fi  = feat_names.index("ped_flow")
    park_fi    = feat_names.index("occupancy_rate")

    T_train_end = int(T * TRAIN_FRAC)
    T_val_end   = int(T * (TRAIN_FRAC + VAL_FRAC))
    log.info(f"  Cube: N={N}, T={T}, F={F}")
    log.info(f"  Split  Train: 0..{T_train_end}  Val: {T_train_end}..{T_val_end}  Test: {T_val_end}..{T}")

    park_df = pd.read_parquet(PROCESSED_DIR / "parking_occupancy.parquet")
    valid_park_streets = set(
        park_df[park_df["valid_parking"]]["street_id"].astype(str).unique()
    )
    ni_df = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    ni_df["street_id"] = ni_df["street_id"].astype(str)
    park_nodes   = ni_df[ni_df["street_id"].isin(valid_park_streets)]["node_idx"].values
    parking_mask = torch.zeros(N, dtype=torch.bool, device=device)
    parking_mask[park_nodes] = True
    n_park = int(parking_mask.sum().item())
    log.info(f"  Parking mask: {n_park} / {N} streets have sensor data")

    log.info("  Loading cube.npy...")
    cube_raw  = np.load(PROCESSED_DIR / "cube.npy")
    log.info("  Normalising cube...")
    cube_norm = _normalise_cube(cube_raw, norm_stats, feat_names)
    del cube_raw

    # D-013: ped-validity mask — exclude sensor-outage bins from the ped loss/metrics
    ped_valid_mask_np = torch.load(PROCESSED_DIR / "ped_valid_mask.pt").numpy().astype(bool)  # [N,T]
    log.info(f"  Ped valid mask: {ped_valid_mask_np.mean()*100:.1f}% of bins valid")

    # D-016 (Phase 3): pedestrian-loss scope. ped_confidence (feat idx) is 1.0 only on
    # real sensor streets and is NOT normalised, so it survives in cube_norm unchanged.
    conf_idx = feat_names.index("ped_confidence")
    ped_sensor_np = (cube_norm[:, 0, conf_idx] == 1.0)                      # [N] bool
    ped_sensor_mask = torch.tensor(ped_sensor_np, device=device)           # [N] bool (for loss)
    if PED_LOSS_SCOPE == "sensors":
        ped_eval_mask = ped_valid_mask_np & ped_sensor_np[:, None]         # eval on sensor bins
    else:
        ped_eval_mask = ped_valid_mask_np
    log.info(f"  Ped loss scope: '{PED_LOSS_SCOPE}' ({int(ped_sensor_np.sum())} sensor streets); "
             f"metrics reported over {'sensor' if PED_LOSS_SCOPE=='sensors' else 'all'} streets")

    val_max_start  = T_val_end - WINDOW - 1
    test_max_start = T - WINDOW - 1
    val_fixed_starts  = np.linspace(T_train_end, max(T_train_end + 1, val_max_start),  N_EVAL_FIXED, dtype=int)
    test_fixed_starts = np.linspace(T_val_end,   max(T_val_end   + 1, test_max_start), N_EVAL_FIXED, dtype=int)

    adj_s   = torch.load(PROCESSED_DIR / "graph_spatial.pt",  map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt", map_location=device, weights_only=False)

    model = MultiGCN(
        n_feat=F, hidden=HIDDEN, n_nodes=N,
        adj_s=adj_s, adj_sem=adj_sem,
        gru_layers=GRU_LAYERS, dropout=DROPOUT,
    ).to(device)
    n_params = sum(p.numel() for p in model.parameters())
    log.info(f"  Model parameters: {n_params:,}")

    optimiser = Adam(model.parameters(), lr=LR)
    scheduler = ReduceLROnPlateau(optimiser, mode="min", factor=0.5, patience=8)
    criterion = nn.L1Loss()

    best_val_mae = float("inf")
    best_state   = None
    no_improve   = 0
    history      = []

    log.info(f"  Training: max {MAX_EPOCHS} epochs, {BATCHES_EPOCH} steps/epoch, "
             f"batch={BATCH_SIZE}, window={WINDOW}, hidden={HIDDEN}, park_weight={PARK_WEIGHT}")

    for epoch in range(1, MAX_EPOCHS + 1):
        model.train()
        epoch_loss = 0.0
        n_steps    = 0

        for _ in range(BATCHES_EPOCH // BATCH_SIZE):
            X, y_ped, y_park, m_ped = _sample_windows(
                cube_norm, 0, T_train_end, BATCH_SIZE,
                WINDOW, target_fi, park_fi, device,
                ped_valid_mask=ped_eval_mask, return_ped_mask=True,
            )
            optimiser.zero_grad()
            pred_ped, pred_park = model(X)

            # D-013/016: masked ped MAE — outage bins excluded; restricted to sensor
            # streets when PED_LOSS_SCOPE == "sensors" (imputed streets stay context only)
            ped_w = m_ped
            if PED_LOSS_SCOPE == "sensors":
                ped_w = ped_w * ped_sensor_mask[None, :, None].float()
            ped_loss  = (torch.abs(pred_ped - y_ped) * ped_w).sum() / ped_w.sum().clamp(min=1.0)
            park_loss = criterion(
                pred_park[:, parking_mask, :],
                y_park[:, parking_mask, :],
            )
            loss = ped_loss + PARK_WEIGHT * park_loss
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimiser.step()
            epoch_loss += loss.item()
            n_steps    += 1

        train_loss = epoch_loss / max(n_steps, 1)
        (val_ped_mae, val_ped_rmse, val_ped_r2,
         val_park_mae, val_park_rmse, val_park_r2) = _evaluate(
            model, cube_norm, T_train_end, T_val_end, WINDOW,
            target_fi, park_fi, norm_stats, feat_names, device,
            val_fixed_starts, parking_mask, ped_valid_mask=ped_eval_mask,
        )

        scheduler.step(val_ped_mae)
        history.append({
            "epoch":        epoch,
            "train_loss":   round(train_loss,   4),
            "val_ped_mae":  round(val_ped_mae,  4),
            "val_ped_rmse": round(val_ped_rmse, 4),
            "val_ped_r2":   round(val_ped_r2,   4),
            "val_park_mae": round(val_park_mae,  4),
            "val_park_r2":  round(val_park_r2,   4),
        })

        if epoch % 10 == 0 or epoch == 1:
            log.info(
                f"  Epoch {epoch:3d}  train={train_loss:.4f}  "
                f"ped_MAE={val_ped_mae:.3f}  ped_R2={val_ped_r2:.3f}  "
                f"park_MAE={val_park_mae:.3f}  park_R2={val_park_r2:.3f}"
            )

        if val_ped_mae < best_val_mae - 1e-4:
            best_val_mae = val_ped_mae
            best_state   = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            no_improve   = 0
        else:
            no_improve += 1
            if no_improve >= PATIENCE:
                log.info(f"  Early stopping at epoch {epoch} (no val improvement for {PATIENCE} epochs)")
                break

    # ── Final evaluation ───────────────────────────────────────────────────────
    model.load_state_dict({k: v.to(device) for k, v in best_state.items()})
    model.eval()

    (val_ped_mae_f, val_ped_rmse_f, val_ped_r2_f,
     val_park_mae_f, val_park_rmse_f, val_park_r2_f) = _evaluate(
        model, cube_norm, T_train_end, T_val_end, WINDOW,
        target_fi, park_fi, norm_stats, feat_names, device,
        val_fixed_starts, parking_mask, ped_valid_mask=ped_eval_mask,
    )
    (test_ped_mae, test_ped_rmse, test_ped_r2,
     test_park_mae, test_park_rmse, test_park_r2) = _evaluate(
        model, cube_norm, T_val_end, T, WINDOW,
        target_fi, park_fi, norm_stats, feat_names, device,
        test_fixed_starts, parking_mask, ped_valid_mask=ped_eval_mask,
    )

    log.info(f"  Best model -> val  ped_MAE={val_ped_mae_f:.3f}  ped_R2={val_ped_r2_f:.3f}  "
             f"park_MAE={val_park_mae_f:.3f}  park_R2={val_park_r2_f:.3f}")
    log.info(f"  Best model -> test ped_MAE={test_ped_mae:.3f}  ped_R2={test_ped_r2:.3f}  "
             f"park_MAE={test_park_mae:.3f}  park_R2={test_park_r2:.3f}")

    # ── Save outputs ───────────────────────────────────────────────────────────
    torch.save(best_state,          MODELS_DIR / "best_model.pt")
    torch.save(parking_mask.cpu(),  MODELS_DIR / "parking_mask.pt")

    best_row = min(history, key=lambda r: r["val_ped_mae"])
    eval_dict = {
        "best_epoch":             best_row["epoch"],
        "val_ped_mae":            round(val_ped_mae_f,   4),
        "val_ped_rmse":           round(val_ped_rmse_f,  4),
        "val_ped_r2":             round(val_ped_r2_f,    4),
        "test_ped_mae":           round(test_ped_mae,    4),
        "test_ped_rmse":          round(test_ped_rmse,   4),
        "test_ped_r2":            round(test_ped_r2,     4),
        "val_park_mae":           round(val_park_mae_f,  4),
        "val_park_rmse":          round(val_park_rmse_f, 4),
        "val_park_r2":            round(val_park_r2_f,   4),
        "test_park_mae":          round(test_park_mae,   4),
        "test_park_rmse":         round(test_park_rmse,  4),
        "test_park_r2":           round(test_park_r2,    4),
        "parking_mask_n_streets": n_park,
        "n_params":               n_params,
        "device":                 str(device),
        "split": {
            "T_train_end": T_train_end,
            "T_val_end":   T_val_end,
            "T_test_end":  T,
            "train_frac":  TRAIN_FRAC,
            "val_frac":    VAL_FRAC,
        },
        "history": history,
    }
    config_dict = {
        "window": WINDOW, "hidden": HIDDEN, "gru_layers": GRU_LAYERS,
        "dropout": DROPOUT, "lr": LR, "batch_size": BATCH_SIZE,
        "batches_epoch": BATCHES_EPOCH, "seed": SEED,
        "train_frac": TRAIN_FRAC, "val_frac": VAL_FRAC,
        "park_weight": PARK_WEIGHT, "max_epochs": MAX_EPOCHS,
    }

    (MODELS_DIR / "run_config.json").write_text(json.dumps(config_dict, indent=2))
    (MODELS_DIR / "model_eval.json").write_text(json.dumps(eval_dict,   indent=2))

    # Update cube_meta.json so steps 10/11 use the same split
    meta_path = PROCESSED_DIR / "cube_meta.json"
    meta_out  = json.loads(meta_path.read_text())
    meta_out["T_train_end"] = T_train_end
    meta_out["T_val_start"] = T_train_end
    meta_out["T_val_end"]   = T_val_end
    meta_path.write_text(json.dumps(meta_out))

    log.info("  Saved: best_model.pt, parking_mask.pt, run_config.json, model_eval.json, cube_meta.json")
    log.info("=== Training complete ===")


if __name__ == "__main__":
    main()
