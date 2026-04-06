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

  C. Parking occupancy on non-target streets is exogenous and always taken from real
     observations.  The model does not predict parking occupancy.

  D. Autoregressive prediction error compounds over the rollout horizon.  Treat
     rollouts beyond ~4 hours (16 steps) as indicative, not precise.

  E. The model was trained on normalised ped_flow.  All comparisons are converted
     back to raw pedestrian counts before output.

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
from pathlib import Path

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
      adj_s (sparse), adj_sem (sparse), node_index DataFrame, meta dict.
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

    return model, cube_norm, cube_raw, norm_stats, feat_names, meta, node_index


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
    cube_norm: np.ndarray,          # [N, T, F]
    t_start: int,                   # index of the first rollout timestep in cube
    n_steps: int,
    device: torch.device,
    intervention: dict | None = None,
) -> np.ndarray:
    """
    Autoregressive rollout for n_steps steps starting from t_start.

    The input window covers [t_start - WINDOW, t_start).
    At each step k (0 … n_steps-1):
      - The model predicts ped_flow at t_start + k for all streets.
      - The next row appended to the sliding window is:
            real observed features at (t_start + k) from cube_norm
            EXCEPT ped_flow (fi=0), which is replaced by the model prediction,
            AND (if treated) the intervention feature on the target node.

    Parameters
    ----------
    intervention : dict or None
        Keys:
          node_idx     : int         — target node
          itype        : str         — intervention type
          magnitude    : float       — type-specific parameter (or None)
          start_step   : int         — rollout step at which treatment begins (0-based)
          duration     : int         — number of rollout steps the treatment lasts
        If None, runs a clean baseline.

    Returns
    -------
    np.ndarray, shape [n_steps, N]
        Predicted ped_flow (normalised) for every street at every rollout step.
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
        node_idx    = intervention["node_idx"]
        itype       = intervention["itype"]
        magnitude   = intervention.get("magnitude", 0.0)
        iv_start    = intervention.get("start_step", 0)
        iv_end      = iv_start + intervention.get("duration", n_steps)
    else:
        node_idx = itype = magnitude = iv_start = iv_end = None

    # Initialise sliding window from real observed data: [W, N, F]
    window = cube_norm[:, t_start - WINDOW : t_start, :].transpose(1, 0, 2).copy()
    # window shape: [W, N, F]

    norm_stats_ref = None  # populated lazily if intervention uses it
    feat_names_ref = None

    predictions = np.zeros((n_steps, N), dtype=np.float32)

    model.eval()
    with torch.no_grad():
        for step in range(n_steps):

            # ── Forward pass ──────────────────────────────────────────────────
            X = torch.tensor(
                window[np.newaxis],          # [1, W, N, F]
                dtype=torch.float32,
                device=device,
            )
            pred = model(X)                  # [1, N, 1]
            pred_norm = pred[0, :, 0].cpu().numpy()   # [N]
            predictions[step] = pred_norm

            # ── Build next row ────────────────────────────────────────────────
            # Base: take real observed features at this rollout timestep
            next_t   = t_start + step
            next_row = cube_norm[:, next_t, :].copy()    # [N, F]

            # Override ped_flow with model prediction
            next_row[:, FI_PED_FLOW] = pred_norm

            # Apply intervention if within treatment window
            if (intervention is not None and
                    iv_start <= step < iv_end):
                next_row = _encode_intervention(
                    next_row, node_idx, itype, magnitude,
                    intervention["_norm_stats"],
                    intervention["_feat_names"],
                    pred_norm,
                )

            # ── Slide window ──────────────────────────────────────────────────
            window = np.concatenate(
                [window[1:], next_row[np.newaxis]], axis=0
            )   # still [W, N, F]

    return predictions   # [n_steps, N]


# ==============================================================================
# Network summary helper
# ==============================================================================

def _build_network_summary(
    delta: np.ndarray,            # [n_steps, N], treated - baseline (normalised)
    node_idx: int,
    adj_s: torch.Tensor,
    norm_stats: dict,
    node_order: list,
    top_k: int = 20,
) -> dict:
    """
    Summarise the network effect of the intervention.

    Returns a dict with:
      treated_street    — cumulative and mean delta for the target street
      spatial_neighbours — per-neighbour delta summary
      top_affected       — top_k streets by |mean delta| across the full network
    """
    mu_ped  = norm_stats["ped_flow"]["mean"]
    std_ped = norm_stats["ped_flow"]["std"]

    # Denormalise deltas to raw ped counts
    delta_raw = delta * std_ped   # mean is zero because both baseline and treated
                                   # are expressed relative to the same mean

    mean_delta = delta_raw.mean(axis=0)       # [N]
    cum_delta  = delta_raw.sum(axis=0)        # [N]
    abs_mean   = np.abs(mean_delta)

    # Treated street
    treated = {
        "node_idx":             node_idx,
        "street_id":            str(node_order[node_idx]),
        "mean_ped_delta":       float(mean_delta[node_idx]),
        "cumulative_ped_delta": float(cum_delta[node_idx]),
    }

    # Spatial neighbours
    neighbours_idx = _get_spatial_neighbours(adj_s, node_idx)
    spatial_neighbours = []
    for n in neighbours_idx[:N_NEIGHBOURS_REPORT]:
        spatial_neighbours.append({
            "node_idx":             n,
            "street_id":            str(node_order[n]),
            "mean_ped_delta":       float(mean_delta[n]),
            "cumulative_ped_delta": float(cum_delta[n]),
        })
    spatial_neighbours.sort(key=lambda x: abs(x["mean_ped_delta"]), reverse=True)

    # Top affected across entire network (top_k for detailed reporting)
    top_indices = np.argsort(abs_mean)[::-1][:top_k]
    top_affected = []
    for n in top_indices:
        top_affected.append({
            "node_idx":             int(n),
            "street_id":            str(node_order[n]),
            "mean_ped_delta":       float(mean_delta[n]),
            "cumulative_ped_delta": float(cum_delta[n]),
            "is_treated":           bool(n == node_idx),
            "is_spatial_neighbour": bool(n in neighbours_idx),
        })

    # All streets with non-trivial delta (for map painting)
    DELTA_THRESHOLD = 0.01
    all_deltas = {}
    for n in range(len(node_order)):
        if abs(float(mean_delta[n])) > DELTA_THRESHOLD:
            all_deltas[str(node_order[n])] = float(mean_delta[n])

    return {
        "treated_street":    treated,
        "spatial_neighbours": spatial_neighbours,
        "top_affected":       top_affected,
        "all_deltas":         all_deltas,
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
     feat_names, meta, node_index) = _load_artifacts(device)

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

    adj_s = torch.load(PROCESSED_DIR / "graph_spatial.pt",
                       map_location=device, weights_only=False)

    # ── Baseline rollout (no intervention) ───────────────────────────────────
    log.info("  Running baseline rollout...")
    baseline_norm = _rollout(
        model, cube_norm, t_start, rollout_steps, device,
        intervention=None,
    )   # [n_steps, N]

    # ── Treated rollout (with intervention) ──────────────────────────────────
    log.info(f"  Running treated rollout ({intervention_type})...")
    treated_norm = _rollout(
        model, cube_norm, t_start, rollout_steps, device,
        intervention=intervention,
    )   # [n_steps, N]

    # ── Denormalise to raw ped counts ─────────────────────────────────────────
    mu_ped  = norm_stats["ped_flow"]["mean"]
    std_ped = norm_stats["ped_flow"]["std"]

    baseline_raw = baseline_norm * std_ped + mu_ped   # [n_steps, N]
    treated_raw  = treated_norm  * std_ped + mu_ped
    delta_raw    = treated_raw  - baseline_raw         # [n_steps, N]

    # ── Also extract real observed occupancy for reference ────────────────────
    occ_fi   = FI_OCC_RATE
    mu_occ   = norm_stats["occupancy_rate"]["mean"]
    std_occ  = norm_stats["occupancy_rate"]["std"]

    obs_occ = (
        cube_norm[node_idx, t_start : t_start + rollout_steps, occ_fi]
        * std_occ + mu_occ
    )   # [n_steps]

    # ── Network summary ───────────────────────────────────────────────────────
    log.info("  Building network summary...")
    network_summary = _build_network_summary(
        delta       = treated_norm - baseline_norm,
        node_idx    = node_idx,
        adj_s       = adj_s,
        norm_stats  = norm_stats,
        node_order  = node_order,
        top_k       = 20,
    )

    # ── Assemble result ───────────────────────────────────────────────────────
    result = {
        "meta": {
            "street_id":         str(street_id),
            "node_idx":          int(node_idx),
            "t_start_bin":       int(t_start),
            "duration_bins":     int(duration),
            "rollout_steps":     int(rollout_steps),
            "step_minutes":      15,
            "intervention_type": intervention_type,
            "magnitude":         float(magnitude),
            # Assumptions embedded in this simulation
            "assumptions": [
                "Non-ped features are real observed values from the data cube.",
                "Graph structure is unchanged by the intervention.",
                "Parking occupancy on non-target streets is exogenous (real observed).",
                "Autoregressive error compounds; interpret rollouts > 4h cautiously.",
            ],
        },
        # Per-step, per-street arrays (only the treated street and its neighbours
        # are stored in full to keep the file manageable; full arrays kept for
        # the treated street and top-20 affected streets)
        "baseline": {
            "ped_flow_treated_street": baseline_raw[:, node_idx].tolist(),
            "occ_rate_observed":       np.clip(obs_occ, 0, 1).tolist(),
        },
        "treated": {
            "ped_flow_treated_street": treated_raw[:, node_idx].tolist(),
        },
        "delta": {
            "ped_flow_treated_street": delta_raw[:, node_idx].tolist(),
            # For all streets in top_affected — store their full delta series
            "top_affected_series": {
                str(node_order[entry["node_idx"]]): delta_raw[
                    :, entry["node_idx"]
                ].tolist()
                for entry in network_summary["top_affected"]
            },
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
    print(f"\n{'='*60}")
    print(f"Intervention : {args.intervention} on street {args.street}")
    print(f"Duration     : {args.duration} steps × 15 min = {args.duration * 15} min")
    print(f"Rollout      : {args.rollout} steps × 15 min = {args.rollout * 15} min")
    print(f"\nTreated street effect:")
    print(f"  Mean Δped_flow  : {ts['mean_ped_delta']:+.2f} ped / 15-min")
    print(f"  Cumulative Δped : {ts['cumulative_ped_delta']:+.1f} person-intervals")
    print(f"\nTop 5 affected streets:")
    for entry in ns["top_affected"][:5]:
        tag = " ← treated" if entry["is_treated"] else (
              " (neighbour)" if entry["is_spatial_neighbour"] else "")
        print(f"  {entry['street_id']:>12}  Δmean={entry['mean_ped_delta']:+.2f}{tag}")
    print(f"\nFull result: {args.out or '(see SCENARIO_DIR)'}")
    print('='*60)
