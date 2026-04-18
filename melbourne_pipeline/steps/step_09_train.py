"""
Step 09 -- MultiGCN training: GRU + dual GCN branches, joint ped + parking heads.

Architecture:
  Input:  [batch, W, N, F]  -- W=96 timestep sliding window, N=1397 streets, F=23 features
  |-- GCN branch 1 (spatial):  F -> H  using graph_spatial.pt at each timestep
  |-- GCN branch 2 (semantic): F -> H  using graph_semantic.pt at each timestep
  |-- Concat -> [batch, W, N, 2H]
  |-- GRU:   [batch, W, N, 2H] -> [batch, N, H]  (temporal compression)
  |-- head_ped:  [batch, N, H] -> [batch, N, 1]   (predict next-step ped_flow)
  |-- head_park: [batch, N, H] -> [batch, N, 1]   (predict next-step occupancy_rate)
  +-- Per-node bias for each head [N, 1]

  Joint loss:  MAE_ped  +  PARK_WEIGHT * MAE_park
  Parking loss is masked to the 138 streets that have real sensor data.
  The remaining 1,259 streets have zero occupancy in the cube and contribute
  no parking supervision signal.

Training protocol:
  - Split (chronological, no leakage):
      Train  70%  bins 0 .. T_train_end-1
      Val    15%  bins T_train_end .. T_val_end-1     <- early stopping (on ped MAE)
      Test   15%  bins T_val_end  .. T-1              <- held-out, reported at end
  - Sliding windows of W=96 bins (24 h), stride=1 during training
  - Targets: ped_flow and occupancy_rate at t+1 (both z-score normalised)
  - Loss: MAE_ped + PARK_WEIGHT * masked_MAE_park
  - Early stopping criterion: val ped MAE (primary task)
  - Adam lr=1e-3, reduce on plateau, early stopping patience=25 epochs
  - BATCHES_EPOCH=256 gradient steps per epoch, BATCH_SIZE=8 windows per step
  - Val evaluated on the same 128 fixed windows every epoch (stable early stopping)

Inputs (data/processed/):
  cube.npy, cube_meta.json, norm_stats.json
  graph_spatial.pt, graph_semantic.pt
  parking_occupancy.parquet, node_index.parquet  (to build parking mask)

Outputs (data/models/):
  best_model.pt      -- model state_dict (dual-head)
  best_model_ped_only.pt  -- backup of any prior single-head checkpoint
  parking_mask.pt    -- bool tensor [N], True for streets with parking sensors
  run_config.json    -- hyperparameters
  model_eval.json    -- val + test MAE, RMSE, R2 for both heads + training history
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

from config import PROCESSED_DIR

log = logging.getLogger(__name__)

MODELS_DIR = PROCESSED_DIR.parent / "models"

# ---- Hyperparameters ---------------------------------------------------------
WINDOW        = 96        # 24 h of 15-min bins
HIDDEN        = 64        # GCN hidden dim per branch (x2 after concat)
GRU_LAYERS    = 2
DROPOUT       = 0.1
LR            = 1e-3
MAX_EPOCHS    = 200
PATIENCE      = 25        # early-stop on val MAE (fixed windows)
BATCHES_EPOCH = 256       # gradient steps per epoch
BATCH_SIZE    = 8         # windows per gradient step
SEED          = 42
PARK_WEIGHT   = 0.5   # weight of parking MAE in the combined loss

# Chronological data split fractions
TRAIN_FRAC = 0.70
VAL_FRAC   = 0.15
# Test = remaining 0.15 -- never seen during training or early stopping

# Number of fixed val windows used every epoch for evaluation
N_EVAL_FIXED = 128


# ==============================================================================
# Model
# ==============================================================================

def _sparse_batched(adj: torch.Tensor, x: torch.Tensor) -> torch.Tensor:
    """
    Apply sparse adj [N, N] to dense x [K, N, H] -> [K, N, H].
    Transposes to [N, K*H], does sparse mm, reshapes back.
    """
    K, N, H = x.shape
    x_t   = x.permute(1, 0, 2).reshape(N, K * H)
    out_t = adj @ x_t
    return out_t.reshape(N, K, H).permute(1, 0, 2).contiguous()


class MultiGCN(nn.Module):
    """
    Dual-branch GCN + GRU spatio-temporal model with per-node bias.
    Joint prediction heads for pedestrian flow and parking occupancy.

    Forward:
      x [B, W, N, F]  ->  (pred_ped [B, N, 1], pred_park [B, N, 1])

    node_bias [N, 1] is a learned per-street offset added to every prediction.
    This lets each street learn its own mean level, which is the single
    highest-leverage improvement for R2 on spatially heterogeneous data.

    The parking head (head_park / node_bias_park) shares all GCN + GRU weights
    with the ped head.  Only the final linear projection and bias differ.
    During training, parking loss is masked to streets with real sensor data.
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

        # Pedestrian flow head
        self.head      = nn.Linear(hidden, 1)
        self.node_bias = nn.Parameter(torch.zeros(n_nodes, 1))

        # Parking occupancy head (shared GCN+GRU backbone, separate projection)
        self.head_park      = nn.Linear(hidden, 1)
        self.node_bias_park = nn.Parameter(torch.zeros(n_nodes, 1))

    def forward(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """
        Parameters
        ----------
        x : [B, W, N, F]

        Returns
        -------
        pred_ped  : [B, N, 1]  normalised predicted ped_flow at t+1
        pred_park : [B, N, 1]  normalised predicted occupancy_rate at t+1
        """
        B, W, N, F = x.shape

        xr = x.reshape(B * W, N, F)                           # [K, N, F]

        h_s   = self.act(_sparse_batched(self.adj_s,   self.proj_s(xr)))   # [K, N, H]
        h_sem = self.act(_sparse_batched(self.adj_sem, self.proj_sem(xr))) # [K, N, H]

        h = torch.cat([h_s, h_sem], dim=-1).reshape(B, W, N, self.hidden * 2)
        h = self.drop(h)

        h_gru = h.permute(0, 2, 1, 3).reshape(B * N, W, self.hidden * 2)
        _, h_last = self.gru(h_gru)
        h_last = h_last[-1].reshape(B, N, self.hidden)         # [B, N, H]

        pred_ped  = self.head(h_last)      + self.node_bias.unsqueeze(0)       # [B, N, 1]
        pred_park = self.head_park(h_last) + self.node_bias_park.unsqueeze(0)  # [B, N, 1]
        return pred_ped, pred_park


# ==============================================================================
# Data helpers
# ==============================================================================

def _normalise_cube(cube: np.ndarray, norm_stats: dict, feat_names: list) -> np.ndarray:
    """Z-score normalise continuous features in-place on a copy."""
    cube = cube.copy()
    for fi, name in enumerate(feat_names):
        if name in norm_stats:
            mu  = norm_stats[name]["mean"]
            std = norm_stats[name]["std"]
            cube[:, :, fi] = (cube[:, :, fi] - mu) / std
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
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """
    Sample n_samples (window, next-timestep) pairs from [t_start, t_end).
    If fixed_starts is provided, cycle through it deterministically.

    Returns
    -------
    X       : [n_samples, W, N, F]
    y_ped   : [n_samples, N, 1]  target ped_flow at t+1
    y_park  : [n_samples, N, 1]  target occupancy_rate at t+1
    """
    N, T, F   = cube_norm.shape
    max_start = t_end - window - 1
    if max_start <= t_start:
        raise ValueError(f"Not enough timesteps: t_end={t_end}, window={window}")

    if fixed_starts is not None:
        # cycle through fixed_starts, clamped to valid range
        indices = fixed_starts[:n_samples] % max(1, max_start - t_start)
        starts  = np.clip(t_start + indices, t_start, max_start - 1)
    else:
        starts = np.random.randint(t_start, max_start, size=n_samples)

    X_list, y_ped_list, y_park_list = [], [], []
    for s in starts:
        win = cube_norm[:, s : s + window, :]
        X_list.append(win.transpose(1, 0, 2))
        y_ped_list.append(cube_norm[:, s + window, target_fi][:, np.newaxis])
        y_park_list.append(cube_norm[:, s + window, park_fi][:, np.newaxis])

    X      = torch.tensor(np.stack(X_list),      dtype=torch.float32, device=device)
    y_ped  = torch.tensor(np.stack(y_ped_list),  dtype=torch.float32, device=device)
    y_park = torch.tensor(np.stack(y_park_list), dtype=torch.float32, device=device)
    return X, y_ped, y_park


def _evaluate(
    model, cube_norm, t_start, t_end, window, target_fi, park_fi,
    norm_stats, feat_names, device, fixed_starts: np.ndarray,
    parking_mask: torch.Tensor, eval_batch: int = 8,
) -> tuple[float, float, float, float, float, float]:
    """
    Evaluate MAE / RMSE / R2 in raw units for both ped and parking heads.

    Parking metrics are computed only on streets in parking_mask (sensor-observed).

    Returns
    -------
    ped_mae, ped_rmse, ped_r2, park_mae, park_rmse, park_r2
    """
    mu_ped  = norm_stats[feat_names[target_fi]]["mean"]
    std_ped = norm_stats[feat_names[target_fi]]["std"]
    mu_park = norm_stats[feat_names[park_fi]]["mean"]
    std_park = norm_stats[feat_names[park_fi]]["std"]
    park_idx = parking_mask.cpu().numpy()   # bool [N]
    n_eval   = len(fixed_starts)

    all_pred_ped, all_y_ped   = [], []
    all_pred_park, all_y_park = [], []

    model.eval()
    with torch.no_grad():
        for i in range(0, n_eval, eval_batch):
            batch_starts = fixed_starts[i : i + eval_batch]
            X, y_ped_norm, y_park_norm = _sample_windows(
                cube_norm, t_start, t_end, len(batch_starts),
                window, target_fi, park_fi, device,
                fixed_starts=batch_starts - t_start,
            )
            pred_ped_norm, pred_park_norm = model(X)

            # Denormalise ped → raw counts
            all_pred_ped.append(pred_ped_norm.cpu().numpy() * std_ped + mu_ped)
            all_y_ped.append(y_ped_norm.cpu().numpy()       * std_ped + mu_ped)

            # Denormalise parking → raw occupancy rate
            all_pred_park.append(pred_park_norm.cpu().numpy() * std_park + mu_park)
            all_y_park.append(y_park_norm.cpu().numpy()       * std_park + mu_park)

    # Ped metrics: all streets, clipped at 0
    pred_ped_raw = np.clip(np.concatenate(all_pred_ped, axis=0), 0, None)  # [n, N, 1]
    y_ped_raw    = np.concatenate(all_y_ped, axis=0)
    ped_mae  = float(np.mean(np.abs(pred_ped_raw - y_ped_raw)))
    ped_rmse = float(np.sqrt(np.mean((pred_ped_raw - y_ped_raw) ** 2)))
    ss_res = np.sum((y_ped_raw - pred_ped_raw) ** 2)
    ss_tot = np.sum((y_ped_raw - y_ped_raw.mean()) ** 2)
    ped_r2   = float(1 - ss_res / (ss_tot + 1e-8))

    # Parking metrics: only sensor-observed streets (parking_mask), clipped [0, 1]
    pred_park_raw = np.clip(np.concatenate(all_pred_park, axis=0), 0, 1)   # [n, N, 1]
    y_park_raw    = np.concatenate(all_y_park, axis=0)
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

def run() -> dict[str, Path]:
    log.info("=== Step 9: MultiGCN training (joint ped + parking heads) ===")
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Backup any existing single-head checkpoint --------------------------
    old_model_path = MODELS_DIR / "best_model.pt"
    if old_model_path.exists():
        backup_path = MODELS_DIR / "best_model_ped_only.pt"
        shutil.copy2(old_model_path, backup_path)
        log.info(f"  Backed up existing checkpoint to {backup_path.name}")

    torch.manual_seed(SEED)
    np.random.seed(SEED)
    random.seed(SEED)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"  Device: {device}")

    # ---- Load cube + meta ----------------------------------------------------
    meta       = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    norm_stats = json.loads((PROCESSED_DIR / "norm_stats.json").read_text())
    feat_names = meta["feature_names"]
    N, T, F    = meta["N"], meta["T"], meta["F"]
    target_fi  = feat_names.index("ped_flow")
    park_fi    = feat_names.index("occupancy_rate")

    # Chronological 70 / 15 / 15 split
    T_train_end = int(T * TRAIN_FRAC)
    T_val_end   = int(T * (TRAIN_FRAC + VAL_FRAC))
    # T_test_end  = T

    log.info(f"  Cube: N={N}, T={T}, F={F}")
    log.info(f"  Split  Train: 0..{T_train_end}  "
             f"Val: {T_train_end}..{T_val_end}  "
             f"Test: {T_val_end}..{T}")

    # ---- Build parking mask --------------------------------------------------
    # Only streets with real sensor data contribute to the parking loss.
    park_df   = pd.read_parquet(PROCESSED_DIR / "parking_occupancy.parquet")
    valid_park_streets = set(
        park_df[park_df["valid_parking"]]["street_id"].astype(str).unique()
    )
    ni_df = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    ni_df["street_id"] = ni_df["street_id"].astype(str)
    park_nodes = ni_df[
        ni_df["street_id"].isin(valid_park_streets)
    ]["node_idx"].values
    parking_mask = torch.zeros(N, dtype=torch.bool, device=device)
    parking_mask[park_nodes] = True
    n_park = int(parking_mask.sum().item())
    log.info(f"  Parking mask: {n_park} / {N} streets have sensor data")

    log.info("  Loading cube.npy...")
    cube_raw  = np.load(PROCESSED_DIR / "cube.npy")
    log.info("  Normalising cube...")
    cube_norm = _normalise_cube(cube_raw, norm_stats, feat_names)
    del cube_raw

    # Precompute fixed val windows (same every epoch -> stable early stopping)
    val_max_start = T_val_end - WINDOW - 1
    val_fixed_starts = np.linspace(
        T_train_end, max(T_train_end + 1, val_max_start),
        N_EVAL_FIXED, dtype=int,
    )
    # Precompute fixed test windows
    test_max_start = T - WINDOW - 1
    test_fixed_starts = np.linspace(
        T_val_end, max(T_val_end + 1, test_max_start),
        N_EVAL_FIXED, dtype=int,
    )

    # ---- Load graphs ---------------------------------------------------------
    adj_s   = torch.load(PROCESSED_DIR / "graph_spatial.pt",
                         map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt",
                         map_location=device, weights_only=False)

    # ---- Build model ---------------------------------------------------------
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

    # ---- Training loop -------------------------------------------------------
    best_val_mae = float("inf")
    best_state   = None
    no_improve   = 0
    history      = []

    log.info(f"  Training: max {MAX_EPOCHS} epochs, {BATCHES_EPOCH} steps/epoch, "
             f"batch={BATCH_SIZE}, window={WINDOW}, hidden={HIDDEN}, "
             f"park_weight={PARK_WEIGHT}")

    for epoch in range(1, MAX_EPOCHS + 1):
        model.train()
        epoch_loss = 0.0
        n_steps    = 0

        for _ in range(BATCHES_EPOCH // BATCH_SIZE):
            X, y_ped, y_park = _sample_windows(
                cube_norm, 0, T_train_end, BATCH_SIZE,
                WINDOW, target_fi, park_fi, device,
            )
            optimiser.zero_grad()
            pred_ped, pred_park = model(X)

            ped_loss  = criterion(pred_ped, y_ped)
            # Parking loss only on streets with real sensor data
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
        val_ped_mae, val_ped_rmse, val_ped_r2, val_park_mae, val_park_rmse, val_park_r2 = _evaluate(
            model, cube_norm, T_train_end, T_val_end, WINDOW,
            target_fi, park_fi, norm_stats, feat_names, device,
            val_fixed_starts, parking_mask,
        )
        val_mae = val_ped_mae  # early stopping criterion is ped MAE

        scheduler.step(val_mae)
        history.append({
            "epoch":        epoch,
            "train_loss":   round(train_loss, 4),
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

        if val_mae < best_val_mae - 1e-4:
            best_val_mae = val_mae
            best_state   = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            no_improve   = 0
        else:
            no_improve += 1
            if no_improve >= PATIENCE:
                log.info(f"  Early stopping at epoch {epoch} "
                         f"(no val improvement for {PATIENCE} epochs)")
                break

    # ---- Reload best weights and evaluate on val + test ----------------------
    model.load_state_dict({k: v.to(device) for k, v in best_state.items()})
    model.eval()

    (val_ped_mae_f, val_ped_rmse_f, val_ped_r2_f,
     val_park_mae_f, val_park_rmse_f, val_park_r2_f) = _evaluate(
        model, cube_norm, T_train_end, T_val_end, WINDOW,
        target_fi, park_fi, norm_stats, feat_names, device,
        val_fixed_starts, parking_mask,
    )
    (test_ped_mae, test_ped_rmse, test_ped_r2,
     test_park_mae, test_park_rmse, test_park_r2) = _evaluate(
        model, cube_norm, T_val_end, T, WINDOW,
        target_fi, park_fi, norm_stats, feat_names, device,
        test_fixed_starts, parking_mask,
    )

    log.info(
        f"  Best model -> val  "
        f"ped_MAE={val_ped_mae_f:.3f}  ped_R2={val_ped_r2_f:.3f}  "
        f"park_MAE={val_park_mae_f:.3f}  park_R2={val_park_r2_f:.3f}"
    )
    log.info(
        f"  Best model -> test "
        f"ped_MAE={test_ped_mae:.3f}  ped_R2={test_ped_r2:.3f}  "
        f"park_MAE={test_park_mae:.3f}  park_R2={test_park_r2:.3f}"
    )

    # ---- Save ----------------------------------------------------------------
    model_path = MODELS_DIR / "best_model.pt"
    torch.save(best_state, model_path)

    # Save parking mask so step_11 can identify which streets have parking predictions
    torch.save(parking_mask.cpu(), MODELS_DIR / "parking_mask.pt")

    best_row = min(history, key=lambda r: r["val_ped_mae"])
    eval_dict = {
        "best_epoch":      best_row["epoch"],
        # Pedestrian flow metrics (primary task)
        "val_ped_mae":     round(val_ped_mae_f,  4),
        "val_ped_rmse":    round(val_ped_rmse_f, 4),
        "val_ped_r2":      round(val_ped_r2_f,   4),
        "test_ped_mae":    round(test_ped_mae,   4),
        "test_ped_rmse":   round(test_ped_rmse,  4),
        "test_ped_r2":     round(test_ped_r2,    4),
        # Parking occupancy metrics (sensor streets only)
        "val_park_mae":    round(val_park_mae_f,  4),
        "val_park_rmse":   round(val_park_rmse_f, 4),
        "val_park_r2":     round(val_park_r2_f,   4),
        "test_park_mae":   round(test_park_mae,   4),
        "test_park_rmse":  round(test_park_rmse,  4),
        "test_park_r2":    round(test_park_r2,    4),
        "parking_mask_n_streets": n_park,
        "n_params":        n_params,
        "device":          str(device),
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
        "park_weight": PARK_WEIGHT,
    }

    (MODELS_DIR / "run_config.json").write_text(json.dumps(config_dict, indent=2))
    (MODELS_DIR / "model_eval.json").write_text(json.dumps(eval_dict,   indent=2))

    # Update cube_meta.json so steps 10/11 use the same split
    meta_path = PROCESSED_DIR / "cube_meta.json"
    meta_out  = json.loads(meta_path.read_text())
    meta_out["T_train_end"]  = T_train_end
    meta_out["T_val_start"]  = T_train_end   # start of val period
    meta_out["T_val_end"]    = T_val_end      # start of test period
    meta_path.write_text(json.dumps(meta_out))

    log.info(
        "  Saved best_model.pt, parking_mask.pt, "
        "run_config.json, model_eval.json, cube_meta.json"
    )
    log.info("Step 9 complete.")

    return {
        "model":        model_path,
        "parking_mask": MODELS_DIR / "parking_mask.pt",
        "run_config":   MODELS_DIR / "run_config.json",
        "model_eval":   MODELS_DIR / "model_eval.json",
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
