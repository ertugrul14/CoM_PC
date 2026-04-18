"""
Step 11 — Scenario-based intervention simulation.

Replaces static opportunity scoring with a counterfactual rollout framework.

Given a target street, a time window, and an intervention type:
  1. Baseline rollout  — autoregressive forward pass using real observed features.
  2. Treated rollout   — same, but with intervention-modified features on the target
                         street for the duration of the treatment window.
  3. Delta             — treated minus baseline for every street at every rollout step.
  4. Network summary   — treated street, spatial neighbours, and top affected streets.

Intervention types
------------------
  pedestrianise   Set occupancy_rate = 0 on the target street (full kerb reallocation).
                  Models the removal of on-street parking for the intervention period.

  restrict_park   Set occupancy_rate = magnitude on the target street.
                  magnitude ∈ [0, 1]; e.g. 0.3 = allow only 30% occupancy.

  boost_ped       Add a constant raw uplift (in ped/15-min) to the target street's
                  ped_flow at each treated rollout step.  Models an exogenous demand
                  generator (activation event, temporary plaza).

Design assumptions
------------------
  A. All features except ped_flow are drawn from the observed cube at each rollout
     timestep.  Only ped_flow is rolled out autoregressively; weather, time encodings,
     land-use, and (except on the treated street) occupancy_rate are always real.
     This requires t_start + WINDOW + n_steps <= T.

  B. The intervention modifies node features only — graph edges are unchanged.
     In reality, pedestrianisation may alter network connectivity; this is not modelled.

  C. Removed parking is redistributed to spatial neighbours proportional to edge
     weight, capped at 100% occupancy.  This is a first-order displacement model;
     it does not cascade (neighbour overflow does not spill further).

  D. Autoregressive prediction error compounds over the rollout horizon.  Treat
     rollouts beyond ~4 hours (16 steps) as indicative, not precise.

  E. The model was trained on normalised ped_flow.  All comparisons are converted
     back to raw pedestrian counts before output.

Network analysis enhancements
-----------------------------
  1. Model-predicted parking spillover — the joint parking head (trained on 138
     sensor streets) predicts occupancy_rate at each rollout step.  For sensor
     streets the prediction is fed back into the sliding window, so subsequent
     GCN steps see the model's estimate of parking redistribution rather than
     fixed historical values.  This replaces the prior first-order displacement
     heuristic with learned behaviour.
  2. Graph diffusion — analytical A^k propagation of mean delta for k=1..3 hops
     through both spatial and semantic graphs (complements autoregressive spread).
  3. Semantic neighbours — reports delta on functionally similar streets (same
     land-use profile) that may be spatially distant.
  4. Confidence-weighted ranking — top_affected streets ranked by delta * ped_confidence
     so sensor-observed streets are prioritised over imputed ones.
  5. Rebound analysis — half-life and recovery fraction after intervention ends,
     characterising how quickly the network returns to baseline.

CLI
---
  python -m steps.step_11_scenario \\
      --street  STREET_ID     \\
      --start   BIN_INDEX     \\   # first rollout step; must be >= WINDOW
      --duration BINS         \\   # how long the intervention lasts  (default 16 = 4 h)
      --rollout  STEPS        \\   # total autoregressive steps        (default 32 = 8 h)
      --intervention TYPE     \\   # pedestrianise | restrict_park | boost_ped
      --magnitude VALUE       \\   # restrict_park: target occ [0,1]; boost_ped: raw uplift
      --out PATH                   # output JSON path (default: data/processed/scenario_results/<id>.json)

Inputs  (data/processed/)
  cube.npy, cube_meta.json, norm_stats.json
  graph_spatial.pt, graph_semantic.pt
  node_index.parquet

Outputs (data/processed/scenario_results/)
  <street_id>_<intervention>_<start>_<duration>.json
"""
import argparse
import json
import logging
import sys
from pathlib import Path

# Allow direct invocation: python steps/step_11_scenario.py ...
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import torch

from config import PROCESSED_DIR
from steps.step_09_train import (
    MultiGCN, _normalise_cube,
    WINDOW, HIDDEN, GRU_LAYERS, DROPOUT,
)

log = logging.getLogger(__name__)

MODELS_DIR    = PROCESSED_DIR.parent / "models"
SCENARIO_DIR  = PROCESSED_DIR / "scenario_results"

# Feature indices (must match cube_meta.json ordering)
FI_PED_FLOW      = 0
FI_OCC_RATE      = 1

# How many spatial neighbours to include in the network summary
N_NEIGHBOURS_REPORT = 8

VALID_INTERVENTIONS = {"pedestrianise", "restrict_park", "boost_ped"}


# ==============================================================================
# Loading helpers
# ==============================================================================

def _load_artifacts(device: torch.device):
    """
    Load and return all artefacts needed for scenario simulation:
      model, cube_norm, cube_raw, norm_stats, feat_names, node_order,
      adj_s (sparse), adj_sem (sparse), node_index DataFrame, meta dict,
      parking_mask (bool np.ndarray [N], None if not available).
    """
    meta        = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    norm_stats  = json.loads((PROCESSED_DIR / "norm_stats.json").read_text())
    feat_names  = meta["feature_names"]

    adj_s   = torch.load(PROCESSED_DIR / "graph_spatial.pt",
                         map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt",
                         map_location=device, weights_only=False)

    model = MultiGCN(
        n_feat     = meta["F"],
        hidden     = HIDDEN,
        n_nodes    = meta["N"],
        adj_s      = adj_s,
        adj_sem    = adj_sem,
        gru_layers = GRU_LAYERS,
        dropout    = DROPOUT,
    ).to(device)

    state = torch.load(MODELS_DIR / "best_model.pt",
                       map_location=device, weights_only=True)
    model.load_state_dict(state)
    model.eval()

    log.info("  Loading cube...")
    cube_raw  = np.load(PROCESSED_DIR / "cube.npy")          # [N, T, F]
    cube_norm = _normalise_cube(cube_raw, norm_stats, feat_names)

    node_index = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")

    # Load parking mask (available after retraining with joint heads)
    parking_mask_path = MODELS_DIR / "parking_mask.pt"
    if parking_mask_path.exists():
        parking_mask = torch.load(
            parking_mask_path, weights_only=True, map_location="cpu"
        ).numpy()   # bool [N]
        log.info(f"  Parking mask loaded: {parking_mask.sum()} / {meta['N']} streets")
    else:
        parking_mask = None
        log.warning("  parking_mask.pt not found — parking predictions disabled")

    return model, cube_norm, cube_raw, norm_stats, feat_names, meta, node_index, parking_mask


def _get_spatial_neighbours(adj_s: torch.Tensor, node_idx: int) -> list[int]:
    """
    Return the list of node indices that are spatial neighbours of node_idx.
    Works for both sparse COO and dense tensors.
    """
    if adj_s.is_sparse:
        idx = adj_s.coalesce().indices()          # [2, nnz]
        mask = (idx[0] == node_idx)
        neighbours = idx[1][mask].cpu().tolist()
    else:
        row = adj_s[node_idx]
        neighbours = (row > 0).nonzero(as_tuple=True)[0].cpu().tolist()

    # Exclude self
    return [n for n in neighbours if n != node_idx]


def _get_semantic_neighbours(adj_sem: torch.Tensor, node_idx: int) -> list[int]:
    """Return node indices that are semantic neighbours of node_idx."""
    if adj_sem.is_sparse:
        idx = adj_sem.coalesce().indices()
        mask = (idx[0] == node_idx)
        neighbours = idx[1][mask].cpu().tolist()
    else:
        row = adj_sem[node_idx]
        neighbours = (row > 0).nonzero(as_tuple=True)[0].cpu().tolist()
    return [n for n in neighbours if n != node_idx]


def _get_edge_weights(adj: torch.Tensor, node_idx: int, neighbours: list[int]) -> np.ndarray:
    """Extract edge weights from node_idx to each neighbour. Returns array aligned with neighbours list."""
    if adj.is_sparse:
        idx = adj.coalesce().indices()
        vals = adj.coalesce().values()
        mask = (idx[0] == node_idx)
        src_indices = idx[1][mask].cpu().tolist()
        src_values = vals[mask].cpu().numpy()
        weight_map = dict(zip(src_indices, src_values))
    else:
        row = adj[node_idx].cpu().numpy()
        weight_map = {n: row[n] for n in neighbours}
    weights = np.array([weight_map.get(n, 0.0) for n in neighbours], dtype=np.float32)
    total = weights.sum()
    if total > 0:
        weights /= total
    return weights



def _graph_diffusion(
    delta: np.ndarray,
    adj_s: torch.Tensor,
    adj_sem: torch.Tensor,
    max_hops: int = 3,
) -> dict:
    """
    Compute analytical graph diffusion of the mean delta through adjacency matrices.

    For each graph (spatial, semantic), computes A_norm^k @ mean_delta for k=1..max_hops
    where A_norm is the row-normalised adjacency matrix.

    Parameters
    ----------
    delta    : [n_steps, N] — raw delta (treated - baseline).
    adj_s    : Spatial adjacency (sparse or dense).
    adj_sem  : Semantic adjacency (sparse or dense).
    max_hops : Maximum diffusion hops to compute.

    Returns
    -------
    dict with keys 'spatial' and 'semantic', each containing a list of [N] arrays
    for hops 1..max_hops.
    """
    mean_delta = delta.mean(axis=0)  # [N]

    result = {}
    for name, adj in [("spatial", adj_s), ("semantic", adj_sem)]:
        # Row-normalise the adjacency matrix
        if adj.is_sparse:
            adj_dense = adj.to_dense().cpu().numpy()
        else:
            adj_dense = adj.cpu().numpy()

        row_sums = adj_dense.sum(axis=1, keepdims=True)
        row_sums[row_sums == 0] = 1.0
        A_norm = adj_dense / row_sums

        hop_deltas = []
        current = mean_delta.copy()
        for k in range(max_hops):
            current = A_norm @ current
            hop_deltas.append(current.copy())
        result[name] = hop_deltas

    return result


def _compute_rebound(
    delta: np.ndarray,
    intervention_end_step: int,
) -> dict:
    """
    Characterise how quickly the network returns to baseline after intervention ends.

    Computes per-street half-life: the number of steps after the intervention ends
    until |delta| drops to 50% of its value at intervention_end_step.

    Parameters
    ----------
    delta                : [n_steps, N] raw delta (treated - baseline).
    intervention_end_step: The rollout step index where the intervention stops.

    Returns
    -------
    dict with:
      - half_life_steps: [N] array, -1 if delta never drops to 50% within the rollout
      - peak_delta: [N] array, the delta value at intervention end
      - recovery_fraction: [N] array, fraction of peak delta remaining at final step
    """
    n_steps, N = delta.shape

    if intervention_end_step >= n_steps:
        return {
            "half_life_steps": [-1] * N,
            "peak_delta": [0.0] * N,
            "recovery_fraction": [1.0] * N,
        }

    peak_delta = delta[intervention_end_step]  # [N]
    abs_peak = np.abs(peak_delta)

    post_intervention = delta[intervention_end_step:]  # [remaining, N]
    abs_post = np.abs(post_intervention)

    half_life = np.full(N, -1, dtype=int)
    for ni in range(N):
        if abs_peak[ni] < 0.01:  # negligible effect
            half_life[ni] = 0
            continue
        threshold = abs_peak[ni] * 0.5
        below = np.where(abs_post[:, ni] <= threshold)[0]
        if len(below) > 0:
            half_life[ni] = int(below[0])

    # Recovery fraction at final step
    final_delta = delta[-1]
    safe_peak = np.where(abs_peak > 0.01, abs_peak, 1.0)  # avoid divide-by-zero
    recovery_fraction = np.where(
        abs_peak > 0.01,
        np.abs(final_delta) / safe_peak,
        0.0,
    )

    return {
        "half_life_steps": half_life.tolist(),
        "peak_delta": peak_delta.tolist(),
        "recovery_fraction": recovery_fraction.tolist(),
    }


# ==============================================================================
# Intervention encoding
# ==============================================================================

def _encode_intervention(
    next_row: np.ndarray,          # [N, F] — the next features row to be appended
    node_idx: int,
    itype: str,
    magnitude: float,
    norm_stats: dict,
    feat_names: list,
    ped_pred_norm: np.ndarray,     # [N] — model ped_flow prediction for this step (normalised)
) -> np.ndarray:
    """
    Apply the intervention to the target node's features in next_row.

    Returns a modified copy of next_row.

    Notes
    -----
    - next_row is already populated with real observed features and model-predicted
      ped_flow for all streets.
    - This function only modifies the target node (node_idx).
    - boost_ped adds to the model prediction in raw units, then re-normalises.
    """
    row = next_row.copy()

    if itype == "pedestrianise":
        # Set occupancy_rate on the target street to 0 (no parking)
        mu_occ  = norm_stats["occupancy_rate"]["mean"]
        std_occ = norm_stats["occupancy_rate"]["std"]
        row[node_idx, FI_OCC_RATE] = (0.0 - mu_occ) / std_occ

    elif itype == "restrict_park":
        # Set occupancy_rate to a specified target level (0–1)
        mu_occ  = norm_stats["occupancy_rate"]["mean"]
        std_occ = norm_stats["occupancy_rate"]["std"]
        row[node_idx, FI_OCC_RATE] = (magnitude - mu_occ) / std_occ

    elif itype == "boost_ped":
        # Add a raw pedestrian uplift to the predicted ped_flow on the target street.
        # magnitude is in raw pedestrians/15-min (before normalisation).
        mu_ped  = norm_stats["ped_flow"]["mean"]
        std_ped = norm_stats["ped_flow"]["std"]
        # Denormalise, add uplift, renormalise
        current_raw = ped_pred_norm[node_idx] * std_ped + mu_ped
        boosted_raw = current_raw + magnitude
        row[node_idx, FI_PED_FLOW] = (boosted_raw - mu_ped) / std_ped

    return row


# ==============================================================================
# Autoregressive rollout
# ==============================================================================

def _rollout(
    model: MultiGCN,
    cube_norm: np.ndarray,              # [N, T, F]
    t_start: int,                       # index of the first rollout timestep in cube
    n_steps: int,
    device: torch.device,
    intervention: dict | None = None,
    parking_mask: np.ndarray | None = None,  # bool [N], streets with parking sensors
) -> tuple[np.ndarray, np.ndarray]:
    """
    Autoregressive rollout for n_steps steps starting from t_start.

    Uses the dual-head MultiGCN to predict both ped_flow and occupancy_rate at
    each step.  The parking head output replaces cube values for sensor-observed
    streets (parking_mask), propagating model-predicted parking spillover through
    the context window fed back into subsequent steps.

    The input window covers [t_start - WINDOW, t_start).
    At each step k (0 … n_steps-1):
      1. Model predicts pred_ped [N] and pred_park [N].
      2. next_row = real observed features at (t_start + k).
      3. next_row[:, FI_PED_FLOW]  = pred_ped  (all streets, autoregressive).
      4. next_row[parking_mask, FI_OCC_RATE] = pred_park[parking_mask]
             (sensor streets: model-predicted occupancy replaces cube value,
              propagating parking spillover learned during training).
      5. Intervention encoding overwrites the target street's features if
         the treatment window is active.
      6. Slide the window forward.

    Parameters
    ----------
    intervention : dict or None
        Keys: node_idx, itype, magnitude, start_step, duration,
              _norm_stats, _feat_names
        If None, runs a clean baseline.
    parking_mask : bool np.ndarray [N] or None
        Streets whose parking predictions are fed back into the window.
        If None, cube occupancy values are used as-is (legacy behaviour).

    Returns
    -------
    ped_preds  : np.ndarray [n_steps, N]  normalised predicted ped_flow
    park_preds : np.ndarray [n_steps, N]  normalised predicted occupancy_rate
    """
    N, T, F = cube_norm.shape

    # Validate bounds
    if t_start < WINDOW:
        raise ValueError(
            f"t_start={t_start} must be >= WINDOW={WINDOW} to fill the initial window."
        )
    if t_start + n_steps > T:
        raise ValueError(
            f"t_start + n_steps = {t_start + n_steps} exceeds T={T}. "
            "Reduce rollout steps or choose a later t_start."
        )

    # Unpack intervention spec
    if intervention is not None:
        node_idx  = intervention["node_idx"]
        itype     = intervention["itype"]
        magnitude = intervention.get("magnitude", 0.0)
        iv_start  = intervention.get("start_step", 0)
        iv_end    = iv_start + intervention.get("duration", n_steps)
    else:
        node_idx = itype = magnitude = iv_start = iv_end = None

    # Initialise sliding window from real observed data: [W, N, F]
    window = cube_norm[:, t_start - WINDOW : t_start, :].transpose(1, 0, 2).copy()

    ped_preds  = np.zeros((n_steps, N), dtype=np.float32)
    park_preds = np.zeros((n_steps, N), dtype=np.float32)

    model.eval()
    with torch.no_grad():
        for step in range(n_steps):

            # ── Forward pass ──────────────────────────────────────────────────
            X = torch.tensor(
                window[np.newaxis],      # [1, W, N, F]
                dtype=torch.float32,
                device=device,
            )
            pred_ped_t, pred_park_t = model(X)            # each [1, N, 1]
            pred_ped_norm  = pred_ped_t[0, :, 0].cpu().numpy()    # [N]
            pred_park_norm = pred_park_t[0, :, 0].cpu().numpy()   # [N]

            ped_preds[step]  = pred_ped_norm
            park_preds[step] = pred_park_norm

            # ── Build next row ────────────────────────────────────────────────
            next_t   = t_start + step
            next_row = cube_norm[:, next_t, :].copy()    # [N, F]

            # Ped flow: always autoregressive for all streets
            next_row[:, FI_PED_FLOW] = pred_ped_norm

            # Parking: use model prediction for sensor-observed streets.
            # This feeds the learned parking spillover back into subsequent steps.
            if parking_mask is not None:
                next_row[parking_mask, FI_OCC_RATE] = pred_park_norm[parking_mask]

            # Apply intervention if within treatment window.
            # This overwrites the target street's features AFTER the parking
            # update above, so the intervention takes precedence on the target.
            if intervention is not None and iv_start <= step < iv_end:
                next_row = _encode_intervention(
                    next_row, node_idx, itype, magnitude,
                    intervention["_norm_stats"],
                    intervention["_feat_names"],
                    pred_ped_norm,
                )

            # ── Slide window ──────────────────────────────────────────────────
            window = np.concatenate(
                [window[1:], next_row[np.newaxis]], axis=0
            )   # still [W, N, F]

    return ped_preds, park_preds


# ==============================================================================
# Network summary helper
# ==============================================================================

def _build_network_summary(
    delta: np.ndarray,              # [n_steps, N], ped delta (normalised)
    park_delta: np.ndarray | None,  # [n_steps, N], parking delta (normalised), or None
    node_idx: int,
    adj_s: torch.Tensor,
    adj_sem: torch.Tensor,
    norm_stats: dict,
    node_order: list,
    ped_confidence: np.ndarray,     # [N] confidence tier per street (1.0/0.8/0.5)
    parking_mask: np.ndarray | None, # bool [N], streets with parking sensors
    intervention_end_step: int,
    top_k: int = 20,
) -> dict:
    """
    Summarise the network effect of the intervention on both ped and parking.

    Returns a dict with:
      treated_street      — cumulative and mean delta for the target street
      spatial_neighbours  — per-neighbour ped + parking delta summary
      semantic_neighbours — functionally similar streets affected
      top_affected        — top_k streets by |confidence-weighted mean ped delta|
      graph_diffusion     — analytical multi-hop spread estimate
      rebound             — post-intervention recovery characterisation
      all_deltas          — every street with non-trivial delta (for map painting)
    """
    mu_ped   = norm_stats["ped_flow"]["mean"]
    std_ped  = norm_stats["ped_flow"]["std"]
    mu_park  = norm_stats["occupancy_rate"]["mean"]
    std_park = norm_stats["occupancy_rate"]["std"]

    # Denormalise ped deltas to raw ped counts
    delta_raw = delta * std_ped

    mean_delta = delta_raw.mean(axis=0)       # [N]
    cum_delta  = delta_raw.sum(axis=0)        # [N]

    # Denormalise parking deltas to raw occupancy rate
    if park_delta is not None:
        park_delta_raw = park_delta * std_park  # [n_steps, N]
        mean_park_delta = park_delta_raw.mean(axis=0)   # [N]
    else:
        park_delta_raw  = None
        mean_park_delta = np.zeros(len(node_order))

    def _park_delta_for(n: int) -> float | None:
        """Return mean parking delta for street n, or None if no sensor."""
        if parking_mask is None or not parking_mask[n]:
            return None
        return float(mean_park_delta[n])

    # Confidence-weighted ranking on ped (primary task)
    weighted_mean = mean_delta * ped_confidence       # [N]
    abs_weighted  = np.abs(weighted_mean)

    # Treated street
    treated = {
        "node_idx":              node_idx,
        "street_id":             str(node_order[node_idx]),
        "mean_ped_delta":        float(mean_delta[node_idx]),
        "cumulative_ped_delta":  float(cum_delta[node_idx]),
        "ped_confidence":        float(ped_confidence[node_idx]),
        "mean_park_delta":       _park_delta_for(node_idx),
    }

    # Spatial neighbours
    neighbours_idx = _get_spatial_neighbours(adj_s, node_idx)
    spatial_neighbours = []
    for n in neighbours_idx[:N_NEIGHBOURS_REPORT]:
        spatial_neighbours.append({
            "node_idx":              n,
            "street_id":             str(node_order[n]),
            "mean_ped_delta":        float(mean_delta[n]),
            "cumulative_ped_delta":  float(cum_delta[n]),
            "confidence_weighted_delta": float(weighted_mean[n]),
            "ped_confidence":        float(ped_confidence[n]),
            "mean_park_delta":       _park_delta_for(n),
        })
    spatial_neighbours.sort(key=lambda x: abs(x["mean_ped_delta"]), reverse=True)

    # Semantic neighbours
    sem_neighbours_idx = _get_semantic_neighbours(adj_sem, node_idx)
    semantic_neighbours = []
    for n in sem_neighbours_idx[:N_NEIGHBOURS_REPORT]:
        semantic_neighbours.append({
            "node_idx":              n,
            "street_id":             str(node_order[n]),
            "mean_ped_delta":        float(mean_delta[n]),
            "cumulative_ped_delta":  float(cum_delta[n]),
            "confidence_weighted_delta": float(weighted_mean[n]),
            "ped_confidence":        float(ped_confidence[n]),
            "mean_park_delta":       _park_delta_for(n),
        })
    semantic_neighbours.sort(key=lambda x: abs(x["mean_ped_delta"]), reverse=True)

    # Top affected — ranked by confidence-weighted ped delta
    top_indices = np.argsort(abs_weighted)[::-1][:top_k]
    top_affected = []
    for n in top_indices:
        top_affected.append({
            "node_idx":              int(n),
            "street_id":             str(node_order[n]),
            "mean_ped_delta":        float(mean_delta[n]),
            "cumulative_ped_delta":  float(cum_delta[n]),
            "confidence_weighted_delta": float(weighted_mean[n]),
            "ped_confidence":        float(ped_confidence[n]),
            "mean_park_delta":       _park_delta_for(n),
            "is_treated":            bool(n == node_idx),
            "is_spatial_neighbour":  bool(n in neighbours_idx),
            "is_semantic_neighbour": bool(n in sem_neighbours_idx),
        })

    # Improvement #2: graph diffusion analysis
    diffusion = _graph_diffusion(delta_raw, adj_s, adj_sem, max_hops=3)
    # Summarise: for each hop, report the top-5 streets by diffused delta
    diffusion_summary = {}
    for graph_name, hop_deltas in diffusion.items():
        hops = []
        for k, hop_delta in enumerate(hop_deltas, start=1):
            top5 = np.argsort(np.abs(hop_delta))[::-1][:5]
            hops.append({
                "hop": k,
                "top_streets": [
                    {"street_id": str(node_order[i]),
                     "diffused_delta": float(hop_delta[i])}
                    for i in top5
                ],
            })
        diffusion_summary[graph_name] = hops

    # Improvement #5: rebound / recovery analysis
    rebound = _compute_rebound(delta_raw, intervention_end_step)
    # Summarise: treated street + top-5 slowest to recover
    half_lives = np.array(rebound["half_life_steps"])
    # Streets with actual effect that haven't recovered (-1 = never recovered)
    slow_recover_mask = half_lives != 0
    slow_indices = np.where(slow_recover_mask)[0]
    if len(slow_indices) > 0:
        # Sort by half-life descending (-1 = infinite, treat as largest)
        sorted_slow = sorted(
            slow_indices,
            key=lambda i: half_lives[i] if half_lives[i] >= 0 else 9999,
            reverse=True,
        )[:10]
    else:
        sorted_slow = []

    rebound_summary = {
        "treated_street": {
            "half_life_steps": int(half_lives[node_idx]),
            "half_life_minutes": int(half_lives[node_idx]) * 15 if half_lives[node_idx] >= 0 else -1,
            "peak_delta": float(rebound["peak_delta"][node_idx]),
            "recovery_fraction": float(rebound["recovery_fraction"][node_idx]),
        },
        "slowest_to_recover": [
            {
                "street_id": str(node_order[i]),
                "half_life_steps": int(half_lives[i]),
                "half_life_minutes": int(half_lives[i]) * 15 if half_lives[i] >= 0 else -1,
                "peak_delta": float(rebound["peak_delta"][i]),
                "recovery_fraction": float(rebound["recovery_fraction"][i]),
            }
            for i in sorted_slow
        ],
    }

    # All streets with non-trivial ped or parking delta (for map painting)
    DELTA_THRESHOLD = 0.01
    all_deltas = {}
    for n in range(len(node_order)):
        park_d = _park_delta_for(n)
        has_ped_effect  = abs(float(mean_delta[n])) > DELTA_THRESHOLD
        has_park_effect = (park_d is not None and abs(park_d) > DELTA_THRESHOLD)
        if has_ped_effect or has_park_effect:
            all_deltas[str(node_order[n])] = {
                "mean_ped_delta":            float(mean_delta[n]),
                "confidence_weighted_delta": float(weighted_mean[n]),
                "ped_confidence":            float(ped_confidence[n]),
                "mean_park_delta":           park_d,
            }

    return {
        "treated_street":      treated,
        "spatial_neighbours":  spatial_neighbours,
        "semantic_neighbours": semantic_neighbours,
        "top_affected":        top_affected,
        "graph_diffusion":     diffusion_summary,
        "rebound":             rebound_summary,
        "all_deltas":          all_deltas,
    }


# ==============================================================================
# Top-level entry point
# ==============================================================================

def run_scenario(
    street_id: str,
    t_start: int,
    duration: int,
    rollout_steps: int,
    intervention_type: str,
    magnitude: float | None = None,
    out_path: Path | None = None,
    save: bool = True,
) -> dict:
    """
    Run a single scenario simulation for one street and one intervention.

    Parameters
    ----------
    street_id        : City of Melbourne street identifier (string).
    t_start          : Bin index in the data cube at which the rollout starts.
                       The intervention begins immediately (start_step=0).
    duration         : Number of 15-min bins the intervention is active.
    rollout_steps    : Total autoregressive steps to simulate.  Must be >= duration.
                       Rollout continues after the intervention ends so that
                       rebound / decay effects are observable.
    intervention_type: One of 'pedestrianise', 'restrict_park', 'boost_ped'.
    magnitude        : For restrict_park: target occupancy [0, 1].
                       For boost_ped: raw ped uplift per 15-min interval.
                       Ignored for pedestrianise (implicitly 0).
    out_path         : Where to write the JSON result.  Auto-generated if None.

    Returns
    -------
    dict  — the full scenario result (also written to out_path).
    """
    if intervention_type not in VALID_INTERVENTIONS:
        raise ValueError(f"intervention_type must be one of {VALID_INTERVENTIONS}")

    if magnitude is None and intervention_type == "restrict_park":
        raise ValueError("magnitude is required for restrict_park (target occupancy in [0,1])")
    if magnitude is None and intervention_type == "boost_ped":
        raise ValueError("magnitude is required for boost_ped (raw ped uplift per 15-min)")
    if magnitude is None:
        magnitude = 0.0

    log.info(f"=== Step 11 Scenario: {intervention_type} on street {street_id} ===")
    log.info(f"  t_start={t_start}, duration={duration}, rollout={rollout_steps}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"  Device: {device}")

    (model, cube_norm, cube_raw, norm_stats,
     feat_names, meta, node_index, parking_mask) = _load_artifacts(device)

    N, T, F = meta["N"], meta["T"], meta["F"]

    # ── Resolve street_id → node_idx ─────────────────────────────────────────
    sid_to_nidx = dict(zip(
        node_index["street_id"].astype(str),
        node_index["node_idx"].astype(int),
    ))
    if str(street_id) not in sid_to_nidx:
        raise KeyError(
            f"street_id '{street_id}' not found in node_index. "
            f"Available IDs: {list(sid_to_nidx.keys())[:5]} ..."
        )
    node_idx  = sid_to_nidx[str(street_id)]
    node_order = meta["node_order"]   # list of street_ids in node order

    log.info(f"  Resolved street_id={street_id} → node_idx={node_idx}")

    # ── Bounds check ─────────────────────────────────────────────────────────
    if t_start < WINDOW:
        raise ValueError(f"t_start must be >= WINDOW ({WINDOW}). Got {t_start}.")
    if t_start + rollout_steps > T:
        raise ValueError(
            f"t_start ({t_start}) + rollout_steps ({rollout_steps}) = "
            f"{t_start + rollout_steps} exceeds T={T}."
        )
    if duration > rollout_steps:
        raise ValueError("duration cannot exceed rollout_steps.")

    # ── Intervention spec ─────────────────────────────────────────────────────
    intervention = {
        "node_idx":    node_idx,
        "itype":       intervention_type,
        "magnitude":   magnitude,
        "start_step":  0,
        "duration":    duration,
        # Private refs for _encode_intervention
        "_norm_stats": norm_stats,
        "_feat_names": feat_names,
    }

    adj_s   = torch.load(PROCESSED_DIR / "graph_spatial.pt",
                         map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt",
                         map_location=device, weights_only=False)

    # ── Baseline rollout (no intervention) ───────────────────────────────────
    log.info("  Running baseline rollout...")
    baseline_ped_norm, baseline_park_norm = _rollout(
        model, cube_norm, t_start, rollout_steps, device,
        intervention=None,
        parking_mask=parking_mask,
    )   # each [n_steps, N]

    # ── Treated rollout (with intervention; model handles parking spillover) ──
    log.info(f"  Running treated rollout ({intervention_type})...")
    treated_ped_norm, treated_park_norm = _rollout(
        model, cube_norm, t_start, rollout_steps, device,
        intervention=intervention,
        parking_mask=parking_mask,
    )   # each [n_steps, N]

    # ── Denormalise ped to raw counts ─────────────────────────────────────────
    mu_ped  = norm_stats["ped_flow"]["mean"]
    std_ped = norm_stats["ped_flow"]["std"]

    baseline_ped_raw = baseline_ped_norm * std_ped + mu_ped   # [n_steps, N]
    treated_ped_raw  = treated_ped_norm  * std_ped + mu_ped
    ped_delta_raw    = treated_ped_raw   - baseline_ped_raw   # [n_steps, N]

    # ── Denormalise parking to raw occupancy rate ─────────────────────────────
    mu_occ  = norm_stats["occupancy_rate"]["mean"]
    std_occ = norm_stats["occupancy_rate"]["std"]

    baseline_park_raw = np.clip(baseline_park_norm * std_occ + mu_occ, 0, 1)
    treated_park_raw  = np.clip(treated_park_norm  * std_occ + mu_occ, 0, 1)
    park_delta_raw    = treated_park_raw - baseline_park_raw  # [n_steps, N]

    # ── Real observed occupancy for the target street (reference) ─────────────
    obs_occ = np.clip(
        cube_norm[node_idx, t_start : t_start + rollout_steps, FI_OCC_RATE]
        * std_occ + mu_occ,
        0, 1,
    )   # [n_steps]

    # ── Per-node ped_confidence from the cube (raw, not normalised) ───────────
    if "ped_confidence" in feat_names:
        conf_fi = feat_names.index("ped_confidence")
        ped_confidence = cube_raw[:, t_start, conf_fi] if cube_raw is not None else np.full(N, 0.5)
    else:
        ped_confidence = np.full(N, 0.5)

    # ── Network summary ───────────────────────────────────────────────────────
    log.info("  Building network summary...")
    # Normalised deltas for diffusion / rebound (both computed in normalised space)
    ped_delta_norm  = treated_ped_norm  - baseline_ped_norm
    park_delta_norm = treated_park_norm - baseline_park_norm

    network_summary = _build_network_summary(
        delta                 = ped_delta_norm,
        park_delta            = park_delta_norm if parking_mask is not None else None,
        node_idx              = node_idx,
        adj_s                 = adj_s,
        adj_sem               = adj_sem,
        norm_stats            = norm_stats,
        node_order            = node_order,
        ped_confidence        = ped_confidence,
        parking_mask          = parking_mask,
        intervention_end_step = duration,
        top_k                 = 20,
    )

    # ── Assemble result ───────────────────────────────────────────────────────
    # Build parking delta series for sensor streets among top_affected
    top_affected_park_series = {}
    if parking_mask is not None:
        for entry in network_summary["top_affected"]:
            n = entry["node_idx"]
            if parking_mask[n]:
                top_affected_park_series[str(node_order[n])] = park_delta_raw[:, n].tolist()

    result = {
        "meta": {
            "street_id":          str(street_id),
            "node_idx":           int(node_idx),
            "t_start_bin":        int(t_start),
            "duration_bins":      int(duration),
            "rollout_steps":      int(rollout_steps),
            "step_minutes":       15,
            "intervention_type":  intervention_type,
            "magnitude":          float(magnitude),
            "parking_mask_active": parking_mask is not None,
            "assumptions": [
                "Non-ped features are real observed values from the data cube.",
                "Graph structure is unchanged by the intervention.",
                "Parking occupancy for sensor-observed streets is model-predicted "
                "(joint head), propagating learned spillover through the rollout.",
                "Autoregressive error compounds; interpret rollouts > 4h cautiously.",
                "Confidence tiers (1.0/0.8/0.5) weight the trustworthiness of per-street deltas.",
            ],
        },
        "baseline": {
            "ped_flow_treated_street":  baseline_ped_raw[:, node_idx].tolist(),
            "occ_rate_observed":        obs_occ.tolist(),
            "occ_rate_predicted":       baseline_park_raw[:, node_idx].tolist(),
        },
        "treated": {
            "ped_flow_treated_street":  treated_ped_raw[:, node_idx].tolist(),
            "occ_rate_predicted":       treated_park_raw[:, node_idx].tolist(),
        },
        "delta": {
            "ped_flow_treated_street":  ped_delta_raw[:, node_idx].tolist(),
            "occ_rate_treated_street":  park_delta_raw[:, node_idx].tolist(),
            # Full ped delta series for top_affected streets
            "top_affected_ped_series": {
                str(node_order[entry["node_idx"]]): ped_delta_raw[
                    :, entry["node_idx"]
                ].tolist()
                for entry in network_summary["top_affected"]
            },
            # Full parking delta series for top_affected streets that have sensors
            "top_affected_park_series": top_affected_park_series,
        },
        "network_summary": network_summary,
    }

    # ── Save ──────────────────────────────────────────────────────────────────
    if save:
        SCENARIO_DIR.mkdir(parents=True, exist_ok=True)
        if out_path is None:
            fname = f"{street_id}_{intervention_type}_t{t_start}_d{duration}.json"
            out_path = SCENARIO_DIR / fname
        out_path = Path(out_path)
        out_path.write_text(json.dumps(result, indent=2))
        log.info(f"  Saved: {out_path}")
    else:
        log.info("  save=False — result not written to disk")
    log.info("Step 11 Scenario complete.")

    return result


# ==============================================================================
# Pipeline run() shim — called by run_pipeline.py
# ==============================================================================

def run() -> dict[str, Path]:
    """
    When called from run_pipeline.py without arguments, logs a reminder that
    this step is interactive and requires explicit parameters.

    For automated pipeline runs, pass a scenario_config.json:
      {
        "street_id":         "...",
        "t_start":           10200,
        "duration":          16,
        "rollout_steps":     32,
        "intervention_type": "pedestrianise",
        "magnitude":         null
      }
    """
    log.info("=== Step 11: Scenario simulation ===")

    config_path = PROCESSED_DIR / "scenario_config.json"
    if config_path.exists():
        cfg = json.loads(config_path.read_text())
        log.info(f"  Found scenario_config.json: {cfg}")
        result = run_scenario(
            street_id         = cfg["street_id"],
            t_start           = cfg["t_start"],
            duration          = cfg["duration"],
            rollout_steps     = cfg["rollout_steps"],
            intervention_type = cfg["intervention_type"],
            magnitude         = cfg.get("magnitude"),
        )
        fname = (f"{cfg['street_id']}_{cfg['intervention_type']}"
                 f"_t{cfg['t_start']}_d{cfg['duration']}.json")
        return {"scenario_result": SCENARIO_DIR / fname}
    else:
        log.warning(
            "  No scenario_config.json found in data/processed/. "
            "Step 11 skipped. Run directly with CLI or create scenario_config.json."
        )
        return {}


# ==============================================================================
# CLI
# ==============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Scenario-based intervention simulation for Melbourne CBD curbside reallocation."
    )
    parser.add_argument("--street",       required=True,  type=str,
                        help="Street ID (string) to apply the intervention to.")
    parser.add_argument("--start",        required=True,  type=int,
                        help="Bin index in the data cube at which rollout begins (>= WINDOW=96).")
    parser.add_argument("--duration",     default=16,     type=int,
                        help="Number of 15-min bins the intervention is active (default 16 = 4h).")
    parser.add_argument("--rollout",      default=32,     type=int,
                        help="Total autoregressive rollout steps (default 32 = 8h).")
    parser.add_argument("--intervention", default="pedestrianise", type=str,
                        choices=list(VALID_INTERVENTIONS),
                        help="Intervention type.")
    parser.add_argument("--magnitude",    default=None,   type=float,
                        help="Type-specific magnitude parameter (see module docstring).")
    parser.add_argument("--out",          default=None,   type=str,
                        help="Output JSON path. Auto-generated if not specified.")

    args = parser.parse_args()

    result = run_scenario(
        street_id         = args.street,
        t_start           = args.start,
        duration          = args.duration,
        rollout_steps     = args.rollout,
        intervention_type = args.intervention,
        magnitude         = args.magnitude,
        out_path          = args.out,
    )

    # Print a brief console summary
    ns = result["network_summary"]
    ts = ns["treated_street"]
    rb = ns["rebound"]["treated_street"]
    print(f"\n{'='*60}")
    print(f"Intervention : {args.intervention} on street {args.street}")
    print(f"Duration     : {args.duration} steps × 15 min = {args.duration * 15} min")
    print(f"Rollout      : {args.rollout} steps × 15 min = {args.rollout * 15} min")
    print(f"\nTreated street effect:")
    print(f"  Mean dped_flow  : {ts['mean_ped_delta']:+.2f} ped / 15-min")
    print(f"  Cumulative dped : {ts['cumulative_ped_delta']:+.1f} person-intervals")
    print(f"  Confidence      : {ts['ped_confidence']:.1f}")
    if ts.get("mean_park_delta") is not None:
        print(f"  Mean dpark_occ  : {ts['mean_park_delta']:+.4f} (model-predicted)")
    print(f"\nRebound (treated street):")
    hl = rb['half_life_minutes']
    print(f"  Half-life       : {'never recovered' if hl < 0 else f'{hl} min'}")
    print(f"  Recovery frac.  : {rb['recovery_fraction']:.2f}")
    print(f"\nTop 5 affected streets (confidence-weighted):")
    for entry in ns["top_affected"][:5]:
        tag = " <- treated" if entry["is_treated"] else (
              " (spatial)" if entry["is_spatial_neighbour"] else (
              " (semantic)" if entry["is_semantic_neighbour"] else ""))
        print(f"  {entry['street_id']:>12}  dmean={entry['mean_ped_delta']:+.2f}"
              f"  conf={entry['ped_confidence']:.1f}"
              f"  weighted={entry['confidence_weighted_delta']:+.2f}{tag}")
    if ns["semantic_neighbours"]:
        print(f"\nSemantic neighbours ({len(ns['semantic_neighbours'])} functionally similar):")
        for entry in ns["semantic_neighbours"][:3]:
            print(f"  {entry['street_id']:>12}  dmean={entry['mean_ped_delta']:+.2f}"
                  f"  conf={entry['ped_confidence']:.1f}")
    print(f"\nFull result: {args.out or '(see SCENARIO_DIR)'}")
    print('='*60)
