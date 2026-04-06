"""
Step 11 — Opportunity scoring + counterfactual simulation.

For each arterial street:
  1. Opportunity score = weighted combination of:
       - predicted ped_flow (from model, averaged over best flexibility window)
       - parking occupancy (low = more room to reassign)
       - cluster archetype alignment (is the intervention type matched to peak time?)
       - ped_confidence (sensor > model estimate)
  2. Counterfactual: set occupancy_rate = 0 (full kerb reallocation) for that
     street, re-predict ped_flow → uplift = (counterfactual - baseline) / baseline

Outputs: data/processed/opportunities.json
         data/processed/street_scores.parquet
"""
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import torch

from config import PROCESSED_DIR
from steps.step_09_train import MultiGCN, _normalise_cube, WINDOW, HIDDEN, GRU_LAYERS, DROPOUT

log = logging.getLogger(__name__)
MODELS_DIR = PROCESSED_DIR.parent / "models"

# Weights for the composite opportunity score
W_PED      = 0.35   # high foot traffic → high opportunity
W_PARKING  = 0.25   # low parking → space available
W_ARCHETYPE = 0.20  # cluster archetype aligns with best window
W_CONF     = 0.10   # data confidence
W_UPLIFT   = 0.10   # counterfactual ped uplift


def _predict_street_mean(model, cube_norm, node_idx, feat_names, norm_stats,
                         T_val_start, T, device) -> float:
    """Mean predicted ped_flow for one street over the val period (batched windows)."""
    target_fi = feat_names.index("ped_flow")
    mu  = norm_stats[feat_names[target_fi]]["mean"]
    std = norm_stats[feat_names[target_fi]]["std"]

    # Sample 16 windows from val period for this street
    N_SAMP = 16
    N = cube_norm.shape[0]
    preds = []
    model.eval()
    with torch.no_grad():
        max_start = T - WINDOW - 1
        starts = np.random.randint(T_val_start, max(T_val_start + 1, max_start), N_SAMP)
        for s in starts:
            win = cube_norm[:, s: s + WINDOW, :]          # [N, W, F]
            X = torch.tensor(win.transpose(1, 0, 2)[np.newaxis],
                             dtype=torch.float32, device=device)  # [1, W, N, F]
            p = model(X)[0, node_idx, 0].item()           # scalar (normalised)
            preds.append(p * std + mu)
    return float(np.clip(np.mean(preds), 0, None))


def run() -> dict[str, Path]:
    log.info("=== Step 11: Opportunity scoring ===")

    meta        = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    norm_stats  = json.loads((PROCESSED_DIR / "norm_stats.json").read_text())
    feat_names  = meta["feature_names"]
    node_order  = meta["node_order"]
    N, T, F     = meta["N"], meta["T"], meta["F"]
    T_val_start = meta["T_val_start"]
    target_fi   = feat_names.index("ped_flow")
    occ_fi      = feat_names.index("occupancy_rate")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    adj_s   = torch.load(PROCESSED_DIR / "graph_spatial.pt",
                         map_location=device, weights_only=False)
    adj_sem = torch.load(PROCESSED_DIR / "graph_semantic.pt",
                         map_location=device, weights_only=False)
    model   = MultiGCN(n_feat=F, hidden=HIDDEN, n_nodes=N,
                       adj_s=adj_s, adj_sem=adj_sem,
                       gru_layers=GRU_LAYERS, dropout=DROPOUT).to(device)
    state   = torch.load(MODELS_DIR / "best_model.pt",
                         map_location=device, weights_only=True)
    model.load_state_dict(state)
    model.eval()

    log.info("  Loading + normalising cube...")
    cube_raw  = np.load(PROCESSED_DIR / "cube.npy")
    cube_norm = _normalise_cube(cube_raw, norm_stats, feat_names)
    del cube_raw

    # Load cluster summary for archetype / mean stats
    cluster_summ = json.loads((PROCESSED_DIR / "cluster_summary.json").read_text())
    node_index   = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    sid_to_nidx  = dict(zip(node_index["street_id"].astype(str),
                            node_index["node_idx"].astype(int)))

    # Normalised ped p95 (for relative scoring)
    ped_p95 = float(np.percentile(cube_norm[:, T_val_start:, target_fi], 95))

    mu_ped  = norm_stats["ped_flow"]["mean"]
    std_ped = norm_stats["ped_flow"]["std"]

    records = []
    log.info(f"  Scoring {N} streets...")

    # Use val-period mean of cube directly (fast, no per-street forward pass)
    ped_val_norm  = cube_norm[:, T_val_start:, target_fi]   # [N, T_val]
    occ_val_norm  = cube_norm[:, T_val_start:, occ_fi]

    # Denormalise ped
    mu_occ  = norm_stats["occupancy_rate"]["mean"]
    std_occ = norm_stats["occupancy_rate"]["std"]

    ped_mean_raw = ped_val_norm.mean(axis=1) * std_ped + mu_ped    # [N]
    occ_mean_raw = occ_val_norm.mean(axis=1) * std_occ + mu_occ    # [N]
    occ_mean_raw = np.clip(occ_mean_raw, 0, 1)

    # Counterfactual: zero the occupancy feature, get new ped prediction for sample windows
    log.info("  Running counterfactual (zero occupancy)...")
    cube_cf = cube_norm.copy()
    # Set occupancy_rate to its minimum (0 normalised)
    zero_occ_norm = (0.0 - mu_occ) / norm_stats["occupancy_rate"]["std"]
    cube_cf[:, :, occ_fi] = zero_occ_norm

    N_CF = 8
    np.random.seed(42)
    starts = np.random.randint(T_val_start, max(T_val_start + 1, T - WINDOW - 1), N_CF)

    # Predict baseline and counterfactual for ALL nodes simultaneously
    baseline_preds = []
    cf_preds       = []
    with torch.no_grad():
        for s in starts:
            # Baseline
            X_b = torch.tensor(
                cube_norm[:, s: s + WINDOW, :].transpose(1, 0, 2)[np.newaxis],
                dtype=torch.float32, device=device,
            )  # [1, W, N, F]
            baseline_preds.append(model(X_b)[0, :, 0].cpu().numpy())  # [N]

            # Counterfactual
            X_cf = torch.tensor(
                cube_cf[:, s: s + WINDOW, :].transpose(1, 0, 2)[np.newaxis],
                dtype=torch.float32, device=device,
            )
            cf_preds.append(model(X_cf)[0, :, 0].cpu().numpy())       # [N]

    baseline_mean = (np.stack(baseline_preds).mean(axis=0) * std_ped + mu_ped).clip(0)  # [N]
    cf_mean       = (np.stack(cf_preds).mean(axis=0)       * std_ped + mu_ped).clip(0)  # [N]
    uplift_frac   = np.where(baseline_mean > 1,
                             (cf_mean - baseline_mean) / baseline_mean, 0.0)  # [N]

    # Global ranges for min-max normalisation
    ped_max  = float(ped_mean_raw.max()) + 1e-6
    occ_max  = float(occ_mean_raw.max()) + 1e-6
    upl_max  = float(np.abs(uplift_frac).max()) + 1e-6

    for ni, sid in enumerate(node_order):
        cs = cluster_summ.get(sid, {})
        archetype      = cs.get("intervention_type", "")
        best_window    = cs.get("best_window", "")
        conf           = float(cs.get("cluster_confidence", 0.5))
        ped_conf       = float(cube_norm[ni, T_val_start, feat_names.index("ped_confidence")]
                               if "ped_confidence" in feat_names else 0.5)
        # Denorm ped_confidence (it was not in norm_stats, stored raw)
        ped_conf_raw   = ped_conf   # already 0.5/0.8/1.0 (not normalised)

        # Normalised sub-scores [0,1]
        score_ped      = min(float(ped_mean_raw[ni]) / ped_max, 1.0)
        score_park     = 1.0 - min(float(occ_mean_raw[ni]) / occ_max, 1.0)  # low occ = good
        score_archetype = conf                                                 # GMM confidence
        score_conf     = float(ped_conf_raw)                                   # 0.5/0.8/1.0
        score_uplift   = min(abs(float(uplift_frac[ni])) / upl_max, 1.0)

        composite = (
            W_PED      * score_ped +
            W_PARKING  * score_park +
            W_ARCHETYPE * score_archetype +
            W_CONF     * score_conf +
            W_UPLIFT   * score_uplift
        )

        records.append({
            "street_id":          sid,
            "opportunity_score":  round(float(composite),         4),
            "score_ped":          round(score_ped,                4),
            "score_parking":      round(score_park,               4),
            "score_archetype":    round(score_archetype,          4),
            "score_confidence":   round(score_conf,               4),
            "score_uplift":       round(score_uplift,             4),
            "pred_ped_mean":      round(float(baseline_mean[ni]), 2),
            "cf_ped_mean":        round(float(cf_mean[ni]),       2),
            "uplift_fraction":    round(float(uplift_frac[ni]),   4),
            "mean_parking":       round(float(occ_mean_raw[ni]),  4),
            "archetype":          archetype,
            "best_window":        best_window,
        })

    df = pd.DataFrame(records).sort_values("opportunity_score", ascending=False)
    log.info(f"  Top 5 streets by opportunity:\n{df[['street_id','opportunity_score','archetype']].head()}")

    scores_path = PROCESSED_DIR / "street_scores.parquet"
    df.to_parquet(scores_path, index=False)

    opps_dict = df.set_index("street_id").to_dict(orient="index")
    opps_path = PROCESSED_DIR / "opportunities.json"
    opps_path.write_text(json.dumps(opps_dict, indent=None))
    log.info(f"  Saved street_scores.parquet + opportunities.json ({len(df)} streets)")
    log.info("Step 11 complete.")
    return {"street_scores": scores_path, "opportunities": opps_path}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s")
    run()
