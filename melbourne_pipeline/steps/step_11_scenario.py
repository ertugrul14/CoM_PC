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

from config import PROCESSED_DIR, normalise_feature, denormalise_feature
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

VALID_INTERVENTIONS = {"pedestrianise", "restrict_park", "boost_ped", "curbside_dining",
                       "reallocate_kerb", "reallocate"}

# Interventions that perturb the parking signal (occupancy_rate). For these the
# counterfactual is only defensible on a street with a real parking sensor.
# curbside_dining reclaims parking bays (occupancy_rate ↓) AND converts them to
# outdoor dining (land-use features ↑), so it is a parking intervention too.
# reallocate (two-stage funnel, D-027) removes a fraction of the kerb's parking,
# so it perturbs occupancy_rate and is a parking intervention as well.
PARKING_INTERVENTIONS = {"pedestrianise", "restrict_park", "curbside_dining", "reallocate"}

# ── Two-stage kerb reallocation (D-027) ──────────────────────────────────────
# The frontend funnel splits a reallocation into two levers:
#   Stage A — remove a fraction `removal_frac` of the kerb's parking. occupancy_rate
#             scales to (1 - removal_frac) of its current level (1.0 = full clear),
#             and the GNN predicts the footfall response (model-driven, correlational).
#   Stage B — the reclaimed use. ONLY the pedestrian-plaza use injects an evidence-based
#             footfall uplift (the D-026 band, applied analytically with the same
#             mass-conserving redistribution as reallocate_kerb). Outdoor-dining and
#             greening/parklet inject NO uplift for now: they are pure occupancy-response
#             scenarios, so they currently produce identical numbers and differ only in
#             label (an explicit interim state, not a forecast that they behave the same).
REALLOCATE_USES = {"outdoor_dining", "greening_parklet", "pedestrian_plaza"}
UPLIFT_USES     = {"pedestrian_plaza"}   # only these inject a Stage-B footfall uplift

# ── Curbside-dining conversion assumptions (D-025) ───────────────────────────
# The curbside_dining intervention models "curbside intensification": reclaiming
# on-street parking bays and converting that kerb into outdoor dining (parklets).
# magnitude = number of parking bays reclaimed. The factors below translate bays
# into (a) an occupancy_rate reduction and (b) increases in the land-use features
# the GNN rewards. They are STATED THESIS ASSUMPTIONS, not learned quantities —
# the model only learned correlations, so the causal "kerb → dining → footfall"
# chain is asserted here and must be reported as such (see Known Limitations).
SEATS_PER_BAY        = 7.0    # NACTO / City of Melbourne parklet ≈ 6–8 seats / bay
CAFE_SEAT_SHARE      = 0.7    # share of reclaimed seats attributed to cafés
BAR_SEAT_SHARE       = 0.3    # remainder attributed to bars
BAYS_PER_NEW_CAFE    = 4.0    # ~4 reclaimed bays read to the model as +1 café frontage
BAYS_PER_NEW_BAR     = 8.0    # bars convert at half the café rate
DEFAULT_STREET_BAYS  = 20.0   # assumed on-street bay supply per block face;
                              # used only to map bays → an occupancy fraction

# ── reallocate_kerb conservation split (D-026; Aldred & Croft 2019, Hounslow) ─
# Their before/after counts found the treated street's GROSS footfall uplift is
# mostly DIVERTED from neighbouring streets, not created: ~30.8% genuinely new
# (mode shift), ~69.2% rerouted from other streets. reallocate_kerb conserves this:
# the diverted share is subtracted from spatial neighbours (edge-weighted); only the
# new share is net-new footfall. The GNN's diffusion would (wrongly) make neighbours
# GAIN — see docs/references/evidence/aldred2019.md + decisions.md D-026.
REALLOCATE_NEW_FRACTION      = 0.308
REALLOCATE_DIVERTED_FRACTION = 0.692

# A street counts as ped-sensor-backed when its confidence tier is exactly 1.0
# (real pedestrian sensor). Imputed streets carry 0.8 / 0.5 tiers (D-014).
PED_SENSOR_CONF = 1.0


def _data_backing(
    node_idx: int,
    intervention_type: str,
    parking_mask: np.ndarray | None,
    node_ped_conf: float,
) -> tuple[bool, str]:
    """
    Decide whether a scenario on `node_idx` is backed by real observed data for
    the signal the intervention actually perturbs (honesty gate).

    Parameters
    ----------
    node_idx          : Target street's node index.
    intervention_type : One of VALID_INTERVENTIONS.
    parking_mask      : bool [N] of streets with real parking sensors, or None.
    node_ped_conf     : The target street's ped_confidence tier (1.0/0.8/0.5).

    Returns
    -------
    (data_backed, reason)
        data_backed : True iff the manipulated signal is measured on this street.
        reason      : Human-readable justification (always populated).

    Assumptions
    -----------
    - Imputation provides graph connectivity for every node, but a *claim* about
      an intervention's effect is only made where the perturbed quantity is real.
    - pedestrianise / restrict_park perturb occupancy_rate  → need parking sensor.
    - boost_ped perturbs ped_flow                           → need ped sensor (conf 1.0).
    """
    if intervention_type in PARKING_INTERVENTIONS:
        has_park = bool(parking_mask is not None and parking_mask[node_idx])
        if has_park:
            return True, "parking-sensor street (occupancy_rate observed)"
        return False, (
            "no parking sensor on this street - occupancy_rate is imputed, so a "
            "kerb-reallocation counterfactual here is an extrapolation, not a measurement"
        )
    else:  # boost_ped
        is_ped_sensor = bool(node_ped_conf >= PED_SENSOR_CONF)
        if is_ped_sensor:
            return True, "pedestrian-sensor street (ped_flow observed, confidence 1.0)"
        return False, (
            f"ped_confidence={node_ped_conf:.2f} < 1.0 - baseline ped_flow is "
            "XGBoost-imputed here, so the demand-uplift baseline is an extrapolation"
        )


# ==============================================================================
# Loading helpers
# ==============================================================================

# Process-level artifact cache. The scenario server (api_server.py) is long-lived
# and handles many requests; loading the 1.72 GB cube + normalised copy + model +
# graphs on EVERY request churns disk and RAM and stalls the machine. We load once
# and reuse. Keyed by device string so a CPU and (hypothetical) CUDA caller don't
# share device-bound tensors. The CLI path also benefits (single-shot, harmless).
_ARTIFACT_CACHE: dict[str, dict] = {}


def _load_artifacts(device: torch.device):
    """
    Load and return all artefacts needed for scenario simulation:
      model, cube_norm, cube_raw, norm_stats, feat_names, meta,
      node_index DataFrame, parking_mask (bool np.ndarray [N], None if absent),
      adj_s (sparse), adj_sem (sparse).

    Results are cached at module level per device: the first call reads from disk;
    subsequent calls return the cached objects with no I/O or recomputation. The
    returned tensors/arrays are treated as read-only by the rollout (it copies the
    window slices it mutates), so sharing them across requests is safe.
    """
    key = str(device)
    cached = _ARTIFACT_CACHE.get(key)
    if cached is not None:
        c = cached
        return (c["model"], c["cube_norm"], c["cube_raw"], c["norm_stats"],
                c["feat_names"], c["meta"], c["node_index"], c["parking_mask"],
                c["adj_s"], c["adj_sem"])

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

    # ── Cubes: memory-map, never load resident ──────────────────────────────
    # The rollout only ever reads small time-slices, so a full in-RAM load (1.72 GB)
    # plus a normalised copy (1.85 GB) is pure waste — and recomputing the z-score
    # over 460M elements took ~150 s, which is what froze the machine. Instead we
    # mmap the raw cube and a PERSISTED normalised cube (cube_norm.npy), built once.
    cube_src   = PROCESSED_DIR / "cube.npy"
    norm_path  = PROCESSED_DIR / "cube_norm.npy"
    stats_path = PROCESSED_DIR / "norm_stats.json"

    cube_raw = np.load(cube_src, mmap_mode="r")             # [N, T, F] lazy view

    # cube_norm.npy is derived from cube.npy AND norm_stats.json, so it is stale if
    # either input is newer (e.g. after a retrain that rewrites the normalisation).
    _norm_inputs_mtime = max(cube_src.stat().st_mtime, stats_path.stat().st_mtime)
    if norm_path.exists() and norm_path.stat().st_mtime >= _norm_inputs_mtime:
        log.info("  Loading cached normalised cube (mmap)...")
        cube_norm = np.load(norm_path, mmap_mode="r")
    else:
        # One-time build: load the raw cube fully, normalise, persist, then mmap.
        # Subsequent process starts skip this entirely and just mmap the result.
        log.info("  Building normalised cube (one-time; cube_norm.npy missing/stale)...")
        cube_full = np.load(cube_src)
        cube_norm_arr = _normalise_cube(cube_full, norm_stats, feat_names)
        np.save(norm_path, cube_norm_arr)
        del cube_full, cube_norm_arr
        cube_norm = np.load(norm_path, mmap_mode="r")
        log.info(f"  Saved {norm_path.name} — future starts will mmap it instantly.")

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

    _ARTIFACT_CACHE[key] = {
        "model": model, "cube_norm": cube_norm, "cube_raw": cube_raw,
        "norm_stats": norm_stats, "feat_names": feat_names, "meta": meta,
        "node_index": node_index, "parking_mask": parking_mask,
        "adj_s": adj_s, "adj_sem": adj_sem,
    }
    log.info("  Artifacts cached for reuse across requests.")

    return (model, cube_norm, cube_raw, norm_stats, feat_names, meta,
            node_index, parking_mask, adj_s, adj_sem)


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
    step: int | None = None,       # rollout step index (reallocate_kerb pins to baseline)
    baseline_ped_norm: np.ndarray | None = None,  # [n_steps, N] baseline rollout ped (normalised)
    uplift: float = 0.0,           # reallocate (option 2, D-029): plaza footfall uplift injected here
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
    - reallocate_kerb (D-026) scales the BASELINE ped by an externally-estimated
      elasticity (1 + magnitude). Pinned to the baseline rollout (not the evolving
      treated prediction) so the imposed shock cannot compound across steps.
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
        # Denormalise (expm1 for log-normalised ped_flow), add uplift, renormalise. (D-012)
        current_raw = denormalise_feature(ped_pred_norm[node_idx], "ped_flow", norm_stats)
        boosted_raw = current_raw + magnitude
        row[node_idx, FI_PED_FLOW] = normalise_feature(boosted_raw, "ped_flow", norm_stats)

    elif itype == "reallocate_kerb":
        # Post-hoc elasticity composition (D-026). Rather than perturb model
        # features and hope the GNN infers a footfall response (it does not — the
        # learned land-use↔ped relationship is wrong-signed / OOD-fragile), we
        # IMPOSE an externally-estimated uplift on the treated street's baseline
        # ped, then let the GNN propagate that shock through the graph for spillover.
        # magnitude = uplift FRACTION (0.12 = +12%); the causal claim lives in this
        # number (literature), not in the model. Pin to the baseline rollout so the
        # target's delta equals exactly baseline × magnitude, with no compounding.
        if baseline_ped_norm is not None and step is not None:
            base_raw = denormalise_feature(baseline_ped_norm[step, node_idx], "ped_flow", norm_stats)
        else:
            base_raw = denormalise_feature(ped_pred_norm[node_idx], "ped_flow", norm_stats)
        boosted_raw = base_raw * (1.0 + magnitude)
        row[node_idx, FI_PED_FLOW] = normalise_feature(boosted_raw, "ped_flow", norm_stats)

    elif itype == "curbside_dining":
        # Curbside intensification (D-025): reclaim `magnitude` parking bays and
        # convert them to outdoor dining. Two opposing pushes on the target street:
        #   (1) parking ↓  — reclaimed bays leave the kerb, so occupancy_rate falls;
        #   (2) dining  ↑  — those bays become seats/frontage, raising the land-use
        #                    features the GNN positively associates with footfall.
        # The model nets the two effects; the graph carries spillover to neighbours.
        bays = max(0.0, float(magnitude))

        # (1) Parking side: occupancy scales down by the reclaimed fraction.
        occ_cur = denormalise_feature(row[node_idx, FI_OCC_RATE], "occupancy_rate", norm_stats)
        occ_new = occ_cur * max(0.0, 1.0 - bays / DEFAULT_STREET_BAYS)
        row[node_idx, FI_OCC_RATE] = normalise_feature(occ_new, "occupancy_rate", norm_stats)

        # (2) Dining side: bays → seats (capacity) and frontage (counts). Each
        # increment is added in RAW units then renormalised so the per-feature
        # transform/scale is applied consistently (config.normalise_feature).
        seats = bays * SEATS_PER_BAY
        increments = {
            "cafe_total_seats":    seats * CAFE_SEAT_SHARE,
            "dining_capacity":     seats,
            "bar_patron_capacity": seats * BAR_SEAT_SHARE,
            "cafe_count":          bays / BAYS_PER_NEW_CAFE,
            "bar_count":           bays / BAYS_PER_NEW_BAR,
        }
        for fname, inc in increments.items():
            if fname not in feat_names or fname not in norm_stats:
                continue
            fi = feat_names.index(fname)
            cur = denormalise_feature(row[node_idx, fi], fname, norm_stats)
            row[node_idx, fi] = normalise_feature(cur + inc, fname, norm_stats)

    elif itype == "reallocate":
        # Two-stage funnel (D-027 / D-029). Stage A reclaims a fraction `f` of the kerb's
        # parking: occupancy_rate scales to (1 - f) of its current level (f = 1.0 → full
        # clear / pedestrianisation). `magnitude` carries the removal fraction.
        f = min(1.0, max(0.0, float(magnitude)))
        occ_cur = denormalise_feature(row[node_idx, FI_OCC_RATE], "occupancy_rate", norm_stats)
        occ_new = occ_cur * (1.0 - f)
        row[node_idx, FI_OCC_RATE] = normalise_feature(occ_new, "occupancy_rate", norm_stats)
        # Stage B (option 2, D-029): inject the pedestrian-plaza footfall uplift INTO the
        # rollout so the GNN propagates it through the spatial/semantic graphs — neighbours
        # rise per the model's learned spillover instead of being docked a diverted share.
        # Pin to the baseline ped so the imposed uplift cannot compound across rollout steps.
        if uplift and uplift > 0.0:
            if baseline_ped_norm is not None and step is not None:
                base_raw = denormalise_feature(baseline_ped_norm[step, node_idx], "ped_flow", norm_stats)
            else:
                base_raw = denormalise_feature(ped_pred_norm[node_idx], "ped_flow", norm_stats)
            boosted_raw = base_raw * (1.0 + uplift)
            row[node_idx, FI_PED_FLOW] = normalise_feature(boosted_raw, "ped_flow", norm_stats)

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
                    step=step,
                    baseline_ped_norm=intervention.get("_baseline_ped_norm"),
                    uplift=intervention.get("uplift", 0.0),
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
    mu_park  = norm_stats["occupancy_rate"]["mean"]
    std_park = norm_stats["occupancy_rate"]["std"]

    # D-012: `delta` already arrives in raw ped counts (expm1 difference computed
    # by the caller) — log1p de-norm is nonlinear, so do NOT rescale by std here.
    delta_raw = delta

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
    removal_frac: float | None = None,
    use: str | None = None,
    uplift: float | None = None,
    out_path: Path | None = None,
    save: bool = True,
    allow_imputed: bool = False,
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
    intervention_type: One of 'pedestrianise', 'restrict_park', 'boost_ped',
                       'curbside_dining'.
    magnitude        : For restrict_park: target occupancy [0, 1].
                       For boost_ped: raw ped uplift per 15-min interval.
                       For curbside_dining: number of parking bays reclaimed and
                       converted to outdoor dining (drives both the occupancy
                       reduction and the land-use uplift; see D-025 constants).
                       Ignored for pedestrianise (implicitly 0).
    removal_frac     : For 'reallocate' (D-027): fraction of the kerb's parking removed,
                       in [0, 1]. 1.0 = full clear (pedestrianisation). Required for
                       'reallocate'; ignored otherwise.
    use              : For 'reallocate': the reclaimed use, one of REALLOCATE_USES
                       ('outdoor_dining' | 'greening_parklet' | 'pedestrian_plaza').
                       Only 'pedestrian_plaza' injects the Stage-B footfall uplift.
    uplift           : For 'reallocate' + 'pedestrian_plaza': the evidence-based
                       footfall uplift FRACTION (e.g. 0.30 = +30%, the D-026 band).
                       Forced to 0 for non-plaza uses.
    out_path         : Where to write the JSON result.  Auto-generated if None.
    allow_imputed    : If False (default), the scenario is rejected when the target
                       street lacks a real sensor for the signal the intervention
                       perturbs (honesty gate — see _data_backing). Set True to run
                       on imputed streets anyway (result is flagged data_backed=False).

    Returns
    -------
    dict  — the full scenario result (also written to out_path).

    Raises
    ------
    ValueError  — if the target street is not sensor-backed for this intervention
                  and allow_imputed is False.
    """
    if intervention_type not in VALID_INTERVENTIONS:
        raise ValueError(f"intervention_type must be one of {VALID_INTERVENTIONS}")

    if magnitude is None and intervention_type == "restrict_park":
        raise ValueError("magnitude is required for restrict_park (target occupancy in [0,1])")
    if magnitude is None and intervention_type == "boost_ped":
        raise ValueError("magnitude is required for boost_ped (raw ped uplift per 15-min)")
    if magnitude is None and intervention_type == "curbside_dining":
        raise ValueError("magnitude is required for curbside_dining (parking bays reclaimed for dining)")
    if magnitude is None and intervention_type == "reallocate_kerb":
        raise ValueError("magnitude is required for reallocate_kerb (footfall uplift fraction, e.g. 0.12 = +12%)")

    # ── Two-stage reallocate (D-027): resolve Stage A removal + Stage B use/uplift ─
    if intervention_type == "reallocate":
        if removal_frac is None:
            raise ValueError("removal_frac is required for reallocate (parking removed, fraction in [0,1])")
        removal_frac = min(1.0, max(0.0, float(removal_frac)))
        use = use or "pedestrian_plaza"
        if use not in REALLOCATE_USES:
            raise ValueError(f"use must be one of {REALLOCATE_USES}")
        # Only the pedestrian-plaza use injects a Stage-B footfall uplift; others = 0.
        uplift = float(uplift) if (uplift is not None and use in UPLIFT_USES) else 0.0
        # The rollout's occupancy scaling reads `magnitude` as the removal fraction.
        magnitude = removal_frac

    if magnitude is None:
        magnitude = 0.0

    log.info(f"=== Step 11 Scenario: {intervention_type} on street {street_id} ===")
    log.info(f"  t_start={t_start}, duration={duration}, rollout={rollout_steps}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"  Device: {device}")

    (model, cube_norm, cube_raw, norm_stats,
     feat_names, meta, node_index, parking_mask,
     adj_s, adj_sem) = _load_artifacts(device)

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

    # ── Honesty gate: only claim effects where the perturbed signal is observed ─
    # parking interventions need a real parking sensor; boost_ped needs a real ped
    # sensor (confidence 1.0). Imputed streets still carry graph messages, but a
    # quantitative claim on them is an extrapolation, so we reject by default.
    if "ped_confidence" in feat_names and cube_raw is not None:
        conf_fi_gate  = feat_names.index("ped_confidence")
        node_ped_conf = float(cube_raw[node_idx, t_start, conf_fi_gate])
    else:
        node_ped_conf = PED_SENSOR_CONF  # cannot verify → assume backed (legacy)

    data_backed, backing_reason = _data_backing(
        node_idx, intervention_type, parking_mask, node_ped_conf
    )
    if not data_backed:
        log.warning(f"  Honesty gate: street {street_id} NOT data-backed — {backing_reason}")
        if not allow_imputed:
            raise ValueError(
                f"Street {street_id} is not sensor-backed for '{intervention_type}': "
                f"{backing_reason}. Pass allow_imputed=True to run anyway (the result "
                f"will be flagged data_backed=False)."
            )
        log.warning("  allow_imputed=True — proceeding on an imputed street (flagged in output).")
    else:
        log.info(f"  Honesty gate: street {street_id} is data-backed — {backing_reason}")

    # ── Intervention spec ─────────────────────────────────────────────────────
    intervention = {
        "node_idx":    node_idx,
        "itype":       intervention_type,
        "magnitude":   magnitude,
        "start_step":  0,
        "duration":    duration,
        # reallocate option 2 (D-029): plaza uplift injected into the rollout so the GNN
        # diffuses it to neighbours. 0.0 for every other intervention / use.
        "uplift":      uplift if (uplift is not None) else 0.0,
        # Private refs for _encode_intervention
        "_norm_stats": norm_stats,
        "_feat_names": feat_names,
    }

    # adj_s / adj_sem come from the cached artifacts above (no re-load).

    # ── Baseline rollout (no intervention) ───────────────────────────────────
    log.info("  Running baseline rollout...")
    baseline_ped_norm, baseline_park_norm = _rollout(
        model, cube_norm, t_start, rollout_steps, device,
        intervention=None,
        parking_mask=parking_mask,
    )   # each [n_steps, N]

    # reallocate_kerb (D-026) imposes its uplift on the BASELINE ped at each step,
    # so the treated rollout needs the baseline series available inside the encoder.
    intervention["_baseline_ped_norm"] = baseline_ped_norm

    # ── Treated rollout (with intervention; model handles parking spillover) ──
    log.info(f"  Running treated rollout ({intervention_type})...")
    treated_ped_norm, treated_park_norm = _rollout(
        model, cube_norm, t_start, rollout_steps, device,
        intervention=intervention,
        parking_mask=parking_mask,
    )   # each [n_steps, N]

    # ── Denormalise ped to raw counts ─────────────────────────────────────────
    # D-012: ped_flow is log1p-normalised, so de-norm is expm1(z*std+mu) — nonlinear.
    # The raw delta MUST be the difference of the two expm1'd series, not delta*std.
    baseline_ped_raw = denormalise_feature(baseline_ped_norm, "ped_flow", norm_stats)  # [n_steps, N]
    treated_ped_raw  = denormalise_feature(treated_ped_norm,  "ped_flow", norm_stats)

    if intervention_type == "reallocate_kerb":
        # ── Conservation / redistribution (D-026; Aldred & Croft 2019) ──────────
        # The treated street's gross uplift is mostly DIVERTED from neighbours, not
        # created (~30.8% new, ~69.2% rerouted). The GNN does diffusion → neighbours
        # would GAIN, the wrong sign. We override the network propagation with a
        # mass-conserving redistribution:
        #   (1) treated street shows the imposed gross uplift  (baseline × (1+U));
        #   (2) the diverted share of its gain is SUBTRACTED from spatial neighbours,
        #       weighted by learned spatial edge weights (which neighbours, how much);
        #   (3) the new share (~31%) is the only net-new footfall (no neighbour cost).
        # Treated matches observation; neighbours lose realistically; city net = new share.
        # The imposed uplift is exogenous (literature), not model-predicted; the GNN
        # contributes only the edge weights that target which neighbours lose.
        treated_uplift = np.zeros_like(baseline_ped_raw[:, node_idx])          # [n_steps]
        treated_uplift[:duration] = baseline_ped_raw[:duration, node_idx] * magnitude
        diverted = treated_uplift * REALLOCATE_DIVERTED_FRACTION               # [n_steps]

        # Rebuild network delta from scratch: no GNN diffusion; treated keeps its
        # imposed delta; spatial neighbours absorb the diversion as edge-weighted losses.
        ped_delta_raw = np.zeros_like(baseline_ped_raw)
        ped_delta_raw[:, node_idx] = treated_uplift
        nbr_idx = _get_spatial_neighbours(adj_s, node_idx)
        if nbr_idx:
            w = np.asarray(_get_edge_weights(adj_s, node_idx, nbr_idx), dtype=np.float64)
            if w.sum() > 0:
                w = w / w.sum()
                for j, n in enumerate(nbr_idx):
                    ped_delta_raw[:, n] = -diverted * w[j]
        # Clip so no street loses more than it has, then resync treated for consistency.
        treated_ped_raw = np.maximum(baseline_ped_raw + ped_delta_raw, 0.0)
        ped_delta_raw   = treated_ped_raw - baseline_ped_raw

    elif intervention_type == "reallocate":
        # Option 2 (D-029). The pedestrian-plaza footfall uplift is injected INTO the treated
        # rollout (see _encode_intervention), so the GNN propagates it through the spatial and
        # semantic graphs. NEIGHBOURS therefore rise per the model's own learned spillover
        # rather than being docked a diverted share — no REALLOCATE_DIVERTED_FRACTION here.
        ped_delta_raw = treated_ped_raw - baseline_ped_raw   # neighbours: GNN diffusion
        # The TREATED street's own headline is the imposed evidence uplift. The recorded model
        # output for the treated node is muted by the occupancy-removal correlation (low
        # occupancy reads as "quieter"), so we pin the treated node to the exogenous +uplift
        # the scheme asserts. Neighbours keep their GNN-diffused gain; only the treated node
        # is overridden. Pinned to baseline (not compounding) and active for the duration only.
        if uplift > 0.0:
            imposed = np.zeros_like(baseline_ped_raw[:, node_idx])          # [n_steps]
            imposed[:duration] = baseline_ped_raw[:duration, node_idx] * uplift
            ped_delta_raw[:, node_idx]   = imposed
            treated_ped_raw[:, node_idx] = baseline_ped_raw[:, node_idx] + imposed

    else:
        ped_delta_raw = treated_ped_raw - baseline_ped_raw   # [n_steps, N] true raw delta

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
    # D-012: pass the true raw ped delta (diffusion/rebound/ranking then operate on
    # raw counts). Parking is linear, so its norm-space delta is rescaled inside.
    park_delta_norm = treated_park_norm - baseline_park_norm

    network_summary = _build_network_summary(
        delta                 = ped_delta_raw,
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

    # ── First spatial neighbour (highest edge weight from target node) ───────
    _nbr_candidates = _get_spatial_neighbours(adj_s, node_idx)
    first_spatial_neighbour = None
    if _nbr_candidates:
        _nbr_weights   = _get_edge_weights(adj_s, node_idx, _nbr_candidates)
        _top_pos       = int(np.argmax(_nbr_weights))
        _top_nbr_idx   = _nbr_candidates[_top_pos]
        first_spatial_neighbour = {
            "node_idx":          int(_top_nbr_idx),
            "street_id":         str(node_order[_top_nbr_idx]),
            "edge_weight":       float(_nbr_weights[_top_pos]),
            "ped_confidence":    float(ped_confidence[_top_nbr_idx]),
            "mean_ped_delta":    float(ped_delta_raw[:, _top_nbr_idx].mean()),
            "baseline_ped_flow": baseline_ped_raw[:, _top_nbr_idx].tolist(),
            "treated_ped_flow":  treated_ped_raw[:, _top_nbr_idx].tolist(),
            "ped_delta":         ped_delta_raw[:, _top_nbr_idx].tolist(),
        }
        if parking_mask is not None and parking_mask[_top_nbr_idx]:
            first_spatial_neighbour["baseline_occ_rate"] = baseline_park_raw[:, _top_nbr_idx].tolist()
            first_spatial_neighbour["treated_occ_rate"]  = treated_park_raw[:, _top_nbr_idx].tolist()

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
            "data_backed":         bool(data_backed),
            "data_backing_reason": backing_reason,
            "target_ped_confidence": float(node_ped_conf),
            "assumptions": [
                "Non-ped features are real observed values from the data cube.",
                "Graph structure is unchanged by the intervention.",
                "Parking occupancy for sensor-observed streets is model-predicted "
                "(joint head), propagating learned spillover through the rollout.",
                "Autoregressive error compounds; interpret rollouts > 4h cautiously.",
                "Confidence tiers (1.0/0.8/0.5) weight the trustworthiness of per-street deltas.",
            ] + ([
                f"curbside_dining: {magnitude:g} bays reclaimed → occupancy scaled by "
                f"(1 - bays/{DEFAULT_STREET_BAYS:g}) and {magnitude * SEATS_PER_BAY:g} dining seats "
                f"added (cafés {CAFE_SEAT_SHARE:g} / bars {BAR_SEAT_SHARE:g}), plus frontage "
                f"(+1 café / {BAYS_PER_NEW_CAFE:g} bays, +1 bar / {BAYS_PER_NEW_BAR:g} bays). "
                "These bay→dining conversions are ASSERTED assumptions (D-025), not learned by "
                "the model — the GNN encodes correlation, so the causal uplift is asserted here.",
            ] if intervention_type == "curbside_dining" else []),
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
        "first_spatial_neighbour": first_spatial_neighbour,
    }

    # ── Two-stage reallocate (D-027): expose the split + use/removal metadata ─────
    if intervention_type == "reallocate":
        result["meta"]["removal_frac"] = float(removal_frac)
        result["meta"]["use"]          = use
        result["meta"]["uplift"]       = float(uplift)
        result["meta"]["assumptions"].append(
            f"reallocate (D-029): removed {removal_frac * 100:.0f}% of the kerb's parking "
            f"(occupancy_rate scaled to {(1 - removal_frac) * 100:.0f}% of its level). "
            f"Reclaimed use = '{use}'."
            + (f" A +{uplift * 100:.0f}% pedestrian-plaza footfall uplift (Cambra & Moura 2020 / "
               "Aldred & Croft 2019) was injected into the treated street and PROPAGATED through the "
               "spatial/semantic graphs by the GNN (option 2), so neighbouring streets rise per the "
               "model's learned spillover. No diverted share is subtracted."
               if uplift > 0 else
               " No footfall uplift injected for this use — pure model occupancy response "
               "(dining and parklet are numerically identical for now; they differ only in label).")
        )

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
