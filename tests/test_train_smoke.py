"""
Smoke test for MultiGCN training (Step 09).

Checks in ~30 seconds:
  1. Model instantiates correctly with real N, F, and graph adjacencies.
  2. Forward pass produces correct output shapes.
  3. Parking mask is non-empty and loss computation doesn't crash.
  4. Loss decreases over 3 gradient steps (gradients flow through all branches).
  5. No NaN / Inf in outputs or gradients after a backward pass.
  6. _sample_windows produces correct tensor shapes and target alignment.

Uses a thin time slice (T=300 bins) so it runs without GPU in under 30 s.
The real graphs (N=1397) are used because adj is N×N and cannot be sliced.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pytest
import torch
import torch.nn as nn

# Make melbourne_pipeline importable
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "melbourne_pipeline"))

from steps.step_09_train import (
    MultiGCN,
    _normalise_cube,
    _sample_windows,
)
from config import PROCESSED_DIR

MODELS_DIR = PROCESSED_DIR.parent / "models"

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def meta():
    return json.loads((PROCESSED_DIR / "cube_meta.json").read_text())


@pytest.fixture(scope="module")
def norm_stats():
    return json.loads((PROCESSED_DIR / "norm_stats.json").read_text())


@pytest.fixture(scope="module")
def cube_slice(meta, norm_stats):
    """Load cube.npy and return a thin normalised slice: shape (N, 300, F)."""
    T_SMOKE = 300
    cube_raw = np.load(PROCESSED_DIR / "cube.npy", mmap_mode="r")
    sliced = cube_raw[:, :T_SMOKE, :].copy()
    return _normalise_cube(sliced, norm_stats, meta["feature_names"])


@pytest.fixture(scope="module")
def graphs():
    device = torch.device("cpu")
    adj_s   = torch.load(PROCESSED_DIR / "graph_spatial.pt",
                         map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt",
                         map_location=device, weights_only=False)
    return adj_s, adj_sem


@pytest.fixture(scope="module")
def parking_mask(meta):
    import pandas as pd
    device = torch.device("cpu")
    park_df = pd.read_parquet(PROCESSED_DIR / "parking_occupancy.parquet")
    valid   = set(park_df[park_df["valid_parking"]]["street_id"].astype(str).unique())
    ni_df   = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    ni_df["street_id"] = ni_df["street_id"].astype(str)
    park_nodes = ni_df[ni_df["street_id"].isin(valid)]["node_idx"].values
    mask = torch.zeros(meta["N"], dtype=torch.bool, device=device)
    mask[park_nodes] = True
    return mask


@pytest.fixture(scope="module")
def model(meta, graphs):
    adj_s, adj_sem = graphs
    N, F = meta["N"], meta["F"]
    return MultiGCN(
        n_feat=F, hidden=32, n_nodes=N,
        adj_s=adj_s, adj_sem=adj_sem,
        gru_layers=2, dropout=0.0,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_cube_slice_shape(cube_slice, meta):
    N, F = meta["N"], meta["F"]
    assert cube_slice.shape == (N, 300, F), f"Unexpected cube slice shape: {cube_slice.shape}"
    assert not np.isnan(cube_slice).any(), "NaN in normalised cube slice"


def test_model_parameter_count(model, meta):
    n_params = sum(p.numel() for p in model.parameters())
    # With hidden=32, N=1397, F=23: should be in the tens of thousands
    assert n_params > 1_000, f"Too few parameters: {n_params}"
    print(f"\n  Model parameters: {n_params:,}")


def test_forward_output_shapes(model, cube_slice, meta):
    N, F = meta["N"], meta["F"]
    WINDOW, BATCH = 24, 2
    device = torch.device("cpu")
    model.eval()

    feat_names = meta["feature_names"]
    target_fi  = feat_names.index("ped_flow")
    park_fi    = feat_names.index("occupancy_rate")

    X, y_ped, y_park = _sample_windows(
        cube_slice, t_start=0, t_end=200,
        n_samples=BATCH, window=WINDOW,
        target_fi=target_fi, park_fi=park_fi,
        device=device,
    )

    assert X.shape      == (BATCH, WINDOW, N, F), f"X shape wrong: {X.shape}"
    assert y_ped.shape  == (BATCH, N, 1),          f"y_ped shape wrong: {y_ped.shape}"
    assert y_park.shape == (BATCH, N, 1),          f"y_park shape wrong: {y_park.shape}"

    with torch.no_grad():
        pred_ped, pred_park = model(X)

    assert pred_ped.shape  == (BATCH, N, 1), f"pred_ped shape wrong: {pred_ped.shape}"
    assert pred_park.shape == (BATCH, N, 1), f"pred_park shape wrong: {pred_park.shape}"


def test_no_nan_in_outputs(model, cube_slice, meta):
    N, F = meta["N"], meta["F"]
    WINDOW, BATCH = 24, 2
    device = torch.device("cpu")
    model.eval()

    feat_names = meta["feature_names"]
    target_fi  = feat_names.index("ped_flow")
    park_fi    = feat_names.index("occupancy_rate")

    X, _, _ = _sample_windows(
        cube_slice, 0, 200, BATCH, WINDOW,
        target_fi, park_fi, device,
    )
    with torch.no_grad():
        pred_ped, pred_park = model(X)

    assert not torch.isnan(pred_ped).any(),  "NaN in pred_ped"
    assert not torch.isinf(pred_ped).any(),  "Inf in pred_ped"
    assert not torch.isnan(pred_park).any(), "NaN in pred_park"
    assert not torch.isinf(pred_park).any(), "Inf in pred_park"


def test_parking_mask_nonempty(parking_mask):
    n_park = int(parking_mask.sum().item())
    assert n_park > 0, "Parking mask is all False — no parking streets found"
    print(f"\n  Parking mask: {n_park} streets")


def test_loss_decreases_over_steps(model, cube_slice, meta, parking_mask):
    """Three gradient steps on a tiny batch — loss must strictly decrease."""
    N, F = meta["N"], meta["F"]
    WINDOW, BATCH = 24, 2
    device = torch.device("cpu")
    model.train()

    feat_names = meta["feature_names"]
    target_fi  = feat_names.index("ped_flow")
    park_fi    = feat_names.index("occupancy_rate")

    # Fresh optimiser so this test is independent of model fixture state
    optimiser = torch.optim.Adam(model.parameters(), lr=1e-3)
    criterion = nn.L1Loss()
    losses = []

    # Fixed batch: same windows every step — guarantees monotonic decrease
    X, y_ped, y_park = _sample_windows(
        cube_slice, 0, 200, BATCH, WINDOW,
        target_fi, park_fi, device,
    )

    for _ in range(3):
        optimiser.zero_grad()
        pred_ped, pred_park = model(X)

        ped_loss  = criterion(pred_ped, y_ped)
        park_loss = criterion(
            pred_park[:, parking_mask, :],
            y_park[:,   parking_mask, :],
        )
        loss = ped_loss + 0.5 * park_loss
        loss.backward()
        nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimiser.step()
        losses.append(loss.item())

    print(f"\n  Losses over 3 steps: {[round(l, 4) for l in losses]}")
    assert losses[-1] < losses[0], (
        f"Loss did not decrease: {losses[0]:.4f} -> {losses[-1]:.4f}"
    )


def test_gradients_reach_all_branches(model, cube_slice, meta, parking_mask):
    """After one backward pass, all four leaf parameter groups must have gradients."""
    N, F = meta["N"], meta["F"]
    WINDOW, BATCH = 24, 2
    device = torch.device("cpu")
    model.train()

    feat_names = meta["feature_names"]
    target_fi  = feat_names.index("ped_flow")
    park_fi    = feat_names.index("occupancy_rate")

    X, y_ped, y_park = _sample_windows(
        cube_slice, 0, 200, BATCH, WINDOW,
        target_fi, park_fi, device,
    )
    model.zero_grad()
    pred_ped, pred_park = model(X)
    criterion = nn.L1Loss()
    loss = criterion(pred_ped, y_ped) + 0.5 * criterion(
        pred_park[:, parking_mask, :], y_park[:, parking_mask, :]
    )
    loss.backward()

    # Check each branch has gradients
    assert model.proj_s.weight.grad   is not None, "No grad: spatial GCN projection"
    assert model.proj_sem.weight.grad is not None, "No grad: semantic GCN projection"
    assert model.head.weight.grad     is not None, "No grad: ped head"
    assert model.head_park.weight.grad is not None, "No grad: parking head"
    assert model.node_bias.grad        is not None, "No grad: ped node_bias"
    assert model.node_bias_park.grad   is not None, "No grad: parking node_bias"
