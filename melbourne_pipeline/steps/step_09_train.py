"""
Step 09 -- MultiGCN training: GRU + dual GCN branches.

Architecture:
  Input:  [batch, W, N, F]  -- W=96 timestep sliding window, N=1397 streets, F=23 features
  |-- GCN branch 1 (spatial):  F -> H  using graph_spatial.pt at each timestep
  |-- GCN branch 2 (semantic): F -> H  using graph_semantic.pt at each timestep
  |-- Concat -> [batch, W, N, 2H]
  |-- GRU:   [batch, W, N, 2H] -> [batch, N, H]  (temporal compression)
  |-- Linear: [batch, N, H] -> [batch, N, 1]     (predict next-step ped_flow)
  +-- Per-node bias [N, 1]                        (learned street-level offset)

Training protocol:
  - Split (chronological, no leakage):
      Train  70%  bins 0 .. T_train_end-1
      Val    15%  bins T_train_end .. T_val_end-1     <- early stopping
      Test   15%  bins T_val_end  .. T-1              <- held-out, reported at end
  - Sliding windows of W=96 bins (24 h), stride=1 during training
  - Target: ped_flow at t+1 (z-score normalised)
  - Loss: MAE on normalised target
  - Adam lr=1e-3, reduce on plateau, early stopping patience=25 epochs
  - BATCHES_EPOCH=256 gradient steps per epoch, BATCH_SIZE=8 windows per step
  - Val evaluated on the same 128 fixed windows every epoch (stable early stopping)

Inputs (data/processed/):
  cube.npy, cube_meta.json, norm_stats.json
  graph_spatial.pt, graph_semantic.pt

Outputs (data/models/):
  best_model.pt      -- model state_dict
  run_config.json    -- hyperparameters
  model_eval.json    -- val + test MAE, RMSE, R2, history
"""
import json
import logging
import random
from pathlib import Path

import numpy as np
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

    Forward:
      x [B, W, N, F]  ->  pred [B, N, 1]

    node_bias [N, 1] is a learned per-street offset added to every prediction.
    This lets each street learn its own mean ped_flow level, which is the single
    highest-leverage improvement for R2 on spatially heterogeneous data.
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
        self.drop      = nn.Dropout(dropout)
        self.head      = nn.Linear(hidden, 1)
        # Per-node bias: one learned scalar offset per street
        self.node_bias = nn.Parameter(torch.zeros(n_nodes, 1))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        B, W, N, F = x.shape

        xr = x.reshape(B * W, N, F)                          # [K, N, F]

        h_s   = self.act(_sparse_batched(self.adj_s,   self.proj_s(xr)))   # [K, N, H]
        h_sem = self.act(_sparse_batched(self.adj_sem, self.proj_sem(xr))) # [K, N, H]

        h = torch.cat([h_s, h_sem], dim=-1).reshape(B, W, N, self.hidden * 2)
        h = self.drop(h)

        h_gru = h.permute(0, 2, 1, 3).reshape(B * N, W, self.hidden * 2)
        _, h_last = self.gru(h_gru)
        h_last = h_last[-1].reshape(B, N, self.hidden)        # [B, N, H]

        out = self.head(h_last)                                # [B, N, 1]
        return out + self.node_bias.unsqueeze(0)               # broadcast [1, N, 1]


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
    device: torch.device,
    fixed_starts: np.ndarray | None = None,
) -> tuple[torch.Tensor, torch.Tensor]:
    """
    Sample n_samples (window, next-timestep) pairs from [t_start, t_end).
    If fixed_starts is provided, cycle through it deterministically.
    Returns X [n_samples, W, N, F], y [n_samples, N, 1].
    """
    N, T, F  = cube_norm.shape
    max_start = t_end - window - 1
    if max_start <= t_start:
        raise ValueError(f"Not enough timesteps: t_end={t_end}, window={window}")

    if fixed_starts is not None:
        # cycle through fixed_starts, clamped to valid range
        indices = fixed_starts[:n_samples] % max(1, max_start - t_start)
        starts  = np.clip(t_start + indices, t_start, max_start - 1)
    else:
        starts = np.random.randint(t_start, max_start, size=n_samples)

    X_list, y_list = [], []
    for s in starts:
        win = cube_norm[:, s : s + window, :]
        nxt = cube_norm[:, s + window, target_fi]
        X_list.append(win.transpose(1, 0, 2))
        y_list.append(nxt[:, np.newaxis])

    X = torch.tensor(np.stack(X_list), dtype=torch.float32, device=device)
    y = torch.tensor(np.stack(y_list), dtype=torch.float32, device=device)
    return X, y


def _evaluate(model, cube_norm, t_start, t_end, window, target_fi, norm_stats,
              feat_names, device, fixed_starts: np.ndarray,
              eval_batch: int = 8) -> tuple[float, float, float]:
    """Evaluate MAE / RMSE / R2 in raw ped_flow units using fixed windows."""
    mu  = norm_stats[feat_names[target_fi]]["mean"]
    std = norm_stats[feat_names[target_fi]]["std"]
    n_eval = len(fixed_starts)

    all_pred, all_y = [], []
    model.eval()
    with torch.no_grad():
        for i in range(0, n_eval, eval_batch):
            batch_starts = fixed_starts[i : i + eval_batch]
            X, y_norm = _sample_windows(
                cube_norm, t_start, t_end, len(batch_starts),
                window, target_fi, device, fixed_starts=batch_starts - t_start,
            )
            pred_norm = model(X)
            all_pred.append(pred_norm.cpu().numpy() * std + mu)
            all_y.append(y_norm.cpu().numpy()       * std + mu)

    pred_raw = np.clip(np.concatenate(all_pred, axis=0), 0, None)
    y_raw    = np.concatenate(all_y, axis=0)
    mae  = float(np.mean(np.abs(pred_raw - y_raw)))
    rmse = float(np.sqrt(np.mean((pred_raw - y_raw) ** 2)))
    ss_res = np.sum((y_raw - pred_raw) ** 2)
    ss_tot = np.sum((y_raw - y_raw.mean()) ** 2)
    r2 = float(1 - ss_res / (ss_tot + 1e-8))
    model.train()
    return mae, rmse, r2


# ==============================================================================
# Main
# ==============================================================================

def run() -> dict[str, Path]:
    log.info("=== Step 9: MultiGCN training ===")
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

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

    # Chronological 70 / 15 / 15 split
    T_train_end = int(T * TRAIN_FRAC)
    T_val_end   = int(T * (TRAIN_FRAC + VAL_FRAC))
    # T_test_end  = T

    log.info(f"  Cube: N={N}, T={T}, F={F}")
    log.info(f"  Split  Train: 0..{T_train_end}  "
             f"Val: {T_train_end}..{T_val_end}  "
             f"Test: {T_val_end}..{T}")

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
             f"batch={BATCH_SIZE}, window={WINDOW}, hidden={HIDDEN}")

    for epoch in range(1, MAX_EPOCHS + 1):
        model.train()
        epoch_loss = 0.0
        n_steps    = 0

        for _ in range(BATCHES_EPOCH // BATCH_SIZE):
            X, y = _sample_windows(
                cube_norm, 0, T_train_end, BATCH_SIZE,
                WINDOW, target_fi, device,
            )
            optimiser.zero_grad()
            pred = model(X)
            loss = criterion(pred, y)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimiser.step()
            epoch_loss += loss.item()
            n_steps    += 1

        train_loss = epoch_loss / max(n_steps, 1)
        val_mae, val_rmse, val_r2 = _evaluate(
            model, cube_norm, T_train_end, T_val_end, WINDOW,
            target_fi, norm_stats, feat_names, device, val_fixed_starts,
        )

        scheduler.step(val_mae)
        history.append({
            "epoch": epoch, "train_loss": round(train_loss, 4),
            "val_mae": round(val_mae, 4), "val_rmse": round(val_rmse, 4),
            "val_r2": round(val_r2, 4),
        })

        if epoch % 10 == 0 or epoch == 1:
            log.info(f"  Epoch {epoch:3d}  train={train_loss:.4f}  "
                     f"val_MAE={val_mae:.3f}  val_R2={val_r2:.3f}  "
                     f"val_RMSE={val_rmse:.3f}")

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

    # ---- Reload best weights and evaluate on test set -----------------------
    model.load_state_dict({k: v.to(device) for k, v in best_state.items()})
    model.eval()

    val_mae_f,  val_rmse_f,  val_r2_f  = _evaluate(
        model, cube_norm, T_train_end, T_val_end, WINDOW,
        target_fi, norm_stats, feat_names, device, val_fixed_starts,
    )
    test_mae, test_rmse, test_r2 = _evaluate(
        model, cube_norm, T_val_end, T, WINDOW,
        target_fi, norm_stats, feat_names, device, test_fixed_starts,
    )

    log.info(f"  Best model -> val  MAE={val_mae_f:.3f}  RMSE={val_rmse_f:.3f}  R2={val_r2_f:.3f}")
    log.info(f"  Best model -> test MAE={test_mae:.3f}  RMSE={test_rmse:.3f}  R2={test_r2:.3f}")

    # ---- Save ----------------------------------------------------------------
    model_path = MODELS_DIR / "best_model.pt"
    torch.save(best_state, model_path)

    best_row = min(history, key=lambda r: r["val_mae"])
    eval_dict = {
        "best_epoch":  best_row["epoch"],
        "val_mae":     round(val_mae_f,  4),
        "val_rmse":    round(val_rmse_f, 4),
        "val_r2":      round(val_r2_f,   4),
        "test_mae":    round(test_mae,   4),
        "test_rmse":   round(test_rmse,  4),
        "test_r2":     round(test_r2,    4),
        "n_params":    n_params,
        "device":      str(device),
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

    log.info("  Saved best_model.pt, run_config.json, model_eval.json, cube_meta.json")
    log.info("Step 9 complete.")

    return {
        "model":      model_path,
        "run_config": MODELS_DIR / "run_config.json",
        "model_eval": MODELS_DIR / "model_eval.json",
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
