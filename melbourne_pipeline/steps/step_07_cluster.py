"""
Step 07 — GMM clustering on joint temporal street profiles.

Input:  data/processed/street_profiles.parquet  (3975 streets × 84 temporal features)

Cluster features:
  42 parking features  — mean occupancy_rate per (DoW × time-block)
  42 ped features      — mean ped_flow per (DoW × time-block), row-normalised

Pipeline:
  1. StandardScaler on 84 features
  2. PCA → 20 components (avoids curse of dimensionality; explains ~85%+ variance)
  3. GMM BIC search k=2..10, select true BIC minimum
  4. Bootstrap ARI stability (20 iterations)
  5. Archetype labelling by PARKING centroid peak (relative to global mean)
  6. Flexibility windows: time-blocks where ped demand is high AND parking is low

Intervention archetypes (high-activity):
  major_pedestrian_corridor  — dominant flow street
  morning_pedestrianisation  — morning / commute peak
  midday_retail_activation   — midday / lunch peak
  evening_outdoor_dining     — evening / nightlife peak
  parking_reallocation_priority — low parking flex, medium activity
  weekend_leisure            — weekend-dominant

Latent archetypes (low-activity, sub-differentiated by dominant ToD):
  latent_morning_potential   — low flow, morning-skewed
  latent_midday_potential    — low flow, midday-skewed
  latent_evening_potential   — low flow, evening-skewed

Outputs (data/processed/):
  clustered.parquet          — street_id, cluster, intervention_type, confidence, windows
  cluster_centroids.csv      — mean profile per cluster in original feature space
  cluster_report.json        — k, BIC, ARI, silhouette, cluster sizes, archetype map
"""
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler
from sklearn.utils import resample

from config import PROCESSED_DIR

log = logging.getLogger(__name__)

PCA_N_COMPONENTS     = 20
OCCUPANCY_THRESHOLD  = 0.30   # occupancy < 30% → potential flexibility window
CONFIDENCE_THRESHOLD = 0.70   # GMM max-prob < 0.70 → unreliable assignment
STABILITY_ITERS      = 20
SEED                 = 42

# Time-block → archetype mapping
_ARCHETYPE_MORNING = {"morning", "work_am"}
_ARCHETYPE_EVENING = {"evening"}
# Everything else → midday or weekend_leisure


SCORE_COLS = ["score_morning", "score_midday", "score_evening",
              "score_weekend_ratio", "score_parking_flex"]


def _assign_archetypes(gmm, scaler, pca, joint_cols: list, k: int) -> dict[int, str]:
    """
    Reconstruct GMM centroids in original feature space.
    Use the engineered score columns (score_morning, score_midday, score_evening)
    to determine which time-of-day each cluster peaks at.

    Each cluster is labeled by its RELATIVE score vs the global centroid mean,
    so clusters that are all-round moderate don't steal labels from true peaks.
    """
    means_orig = scaler.inverse_transform(pca.inverse_transform(gmm.means_))

    # Extract the three interpretable score columns
    score_indices = {col: joint_cols.index(col) for col in SCORE_COLS
                     if col in joint_cols}
    if not score_indices:
        # Fallback: sum ped columns by time block (old behaviour, but at least on ped not parking)
        block_names = ["night", "morning", "work_am", "midday", "work_pm", "evening"]
        scores = np.zeros((k, len(block_names)))
        for ci, col in enumerate(joint_cols):
            if not col.startswith("ped_"):
                continue
            for bi, block in enumerate(block_names):
                if col.endswith(f"_{block}"):
                    for cl in range(k):
                        scores[cl, bi] += float(means_orig[cl, ci])
        rel = scores - scores.mean(axis=0)
        mapping = {}
        for cl in range(k):
            top_block = block_names[int(np.argmax(rel[cl]))]
            if top_block in _ARCHETYPE_MORNING:
                mapping[cl] = "morning_pedestrianisation"
            elif top_block in _ARCHETYPE_EVENING:
                mapping[cl] = "evening_outdoor_dining"
            else:
                mapping[cl] = "midday_retail_activation"
        return mapping

    sm_i  = score_indices.get("score_morning")
    sd_i  = score_indices.get("score_midday")
    se_i  = score_indices.get("score_evening")
    sw_i  = score_indices.get("score_weekend_ratio")
    spf_i = score_indices.get("score_parking_flex")

    # Overall activity level per cluster — mean of morning + midday + evening centroids
    activity = np.zeros(k)
    for cl in range(k):
        vals = []
        if sm_i  is not None: vals.append(means_orig[cl, sm_i])
        if sd_i  is not None: vals.append(means_orig[cl, sd_i])
        if se_i  is not None: vals.append(means_orig[cl, se_i])
        activity[cl] = np.mean(vals) if vals else 0.0

    # Parking flexibility per cluster centroid
    park_flex = np.array([
        means_orig[cl, spf_i] if spf_i is not None else 1.0
        for cl in range(k)
    ])

    # Weekend ratio per cluster centroid
    weekend_ratio = np.array([
        means_orig[cl, sw_i] if sw_i is not None else 1.0
        for cl in range(k)
    ])

    # Rank clusters by activity level
    activity_rank = np.argsort(activity)[::-1]  # descending: [highest, ..., lowest]

    # Time-of-day z-scores for secondary tiebreaking within same tier
    ped_mat = np.zeros((k, 3))  # morning, midday, evening
    for cl in range(k):
        if sm_i is not None: ped_mat[cl, 0] = means_orig[cl, sm_i]
        if sd_i is not None: ped_mat[cl, 1] = means_orig[cl, sd_i]
        if se_i is not None: ped_mat[cl, 2] = means_orig[cl, se_i]
    col_std = ped_mat.std(axis=0)
    col_std = np.where(col_std < 1e-6, 1.0, col_std)
    z_tod   = (ped_mat - ped_mat.mean(axis=0)) / col_std

    # Activity thresholds: top cluster = highest tier
    act_mean = activity.mean()
    act_std  = activity.std() if activity.std() > 1e-6 else 1.0
    act_z    = (activity - act_mean) / act_std

    mapping: dict[int, str] = {}
    for cl in range(k):
        # Tier 1: cluster is the highest-activity cluster by a large margin
        if act_z[cl] == act_z.max() and act_z[cl] > 1.0:
            mapping[cl] = "major_pedestrian_corridor"
            continue

        # Tier 2: clearly low activity (z < -0.5) — sub-differentiate by dominant ToD
        if act_z[cl] < -0.5:
            tod_idx = int(np.argmax(z_tod[cl]))
            if tod_idx == 0:
                mapping[cl] = "latent_morning_potential"
            elif tod_idx == 2:
                mapping[cl] = "latent_evening_potential"
            else:
                mapping[cl] = "latent_midday_potential"
            continue

        # Tier 3: medium activity — distinguish by parking flexibility and time-of-day
        # Low parking flex = parking is heavily used = high reallocation opportunity
        if park_flex[cl] < 0.85:
            mapping[cl] = "parking_reallocation_priority"
            continue

        # Medium activity with good parking flexibility: time-of-day decides
        tod_idx = int(np.argmax(z_tod[cl]))
        if tod_idx == 0:
            mapping[cl] = "morning_pedestrianisation"
        elif tod_idx == 2:
            mapping[cl] = "evening_outdoor_dining"
        else:
            # Check weekend uplift
            if weekend_ratio[cl] == weekend_ratio.max() and weekend_ratio[cl] > 0.95:
                mapping[cl] = "weekend_leisure"
            else:
                mapping[cl] = "midday_retail_activation"

    # Post-hoc: guarantee unique labels.
    # If multiple clusters share the same label, rank by activity and append
    # _high / _medium / _low so each cluster has a distinct, interpretable name.
    from collections import Counter
    _tiers = ["_high", "_medium", "_low"]
    for dup_label, count in Counter(mapping.values()).items():
        if count < 2:
            continue
        dup_clusters = sorted(
            [cl for cl, lbl in mapping.items() if lbl == dup_label],
            key=lambda cl: float(activity[cl]),
            reverse=True,  # descending: first = most active
        )
        for rank, cl in enumerate(dup_clusters):
            suffix = _tiers[rank] if rank < len(_tiers) else f"_{rank + 1}"
            mapping[cl] = dup_label + suffix

    return mapping


def _flexibility_windows(
    park_profile: pd.DataFrame,
    ped_profile:  pd.DataFrame,
    labels: np.ndarray,
    best_k: int,
) -> tuple[list[str], list[str]]:
    """
    For each street: identify time-blocks where ped demand is above the
    cluster median AND parking occupancy is below OCCUPANCY_THRESHOLD.
    """
    park_arr = park_profile.values    # (N, 42)
    ped_arr  = ped_profile.values     # (N, 42)

    # Cluster-level ped medians (42-vector each)
    ped_cluster_med = np.array([
        np.median(ped_arr[labels == cl], axis=0) if (labels == cl).any()
        else np.zeros(ped_arr.shape[1])
        for cl in range(best_k)
    ])

    park_cols = list(park_profile.columns)
    ped_cols  = list(ped_profile.columns)

    # Map park col → ped col.
    # park col: park_occupancy_rate_Mon_morning
    # ped col:  ped_ped_flow_Mon_morning  (double prefix from step 6 add_prefix)
    def _park_to_ped(pcol: str) -> str | None:
        suffix = pcol.replace("park_occupancy_rate_", "")
        candidate = f"ped_ped_flow_{suffix}"
        return candidate if candidate in ped_cols else None

    flex_all, flex_best = [], []
    for i in range(len(labels)):
        cl  = int(labels[i])
        med = ped_cluster_med[cl]
        windows = []
        for pi, pcol in enumerate(park_cols):
            if park_arr[i, pi] >= OCCUPANCY_THRESHOLD:
                continue
            ped_col = _park_to_ped(pcol)
            if ped_col is None:
                continue
            qi = ped_cols.index(ped_col)
            if ped_arr[i, qi] > med[qi]:
                windows.append(pcol.replace("park_occupancy_rate_", ""))
        flex_all.append(json.dumps(windows))
        flex_best.append(windows[0] if windows else "")

    return flex_all, flex_best


def run() -> dict[str, Path]:
    log.info("=== Step 7: GMM clustering ===")

    profiles = pd.read_parquet(PROCESSED_DIR / "street_profiles.parquet")
    profiles["street_id"] = profiles["street_id"].astype(str)

    park_cols  = [c for c in profiles.columns if c.startswith("park_occupancy_rate_")]
    ped_cols   = [c for c in profiles.columns if c.startswith("ped_ped_flow_")]
    score_cols = [c for c in SCORE_COLS if c in profiles.columns]
    assert len(park_cols) == 42, f"Expected 42 park cols, got {len(park_cols)}"
    assert len(ped_cols)  == 42, f"Expected 42 ped cols, got {len(ped_cols)}"

    streets = profiles["street_id"].tolist()
    N = len(streets)
    log.info(f"  Clustering {N} streets on {len(park_cols) + len(ped_cols)} temporal + {len(score_cols)} score features")
    log.info(f"  Score columns: {score_cols}")

    joint_cols = park_cols + ped_cols + score_cols
    X_raw = profiles[joint_cols].values.astype(np.float64)

    # ── Normalise + PCA ───────────────────────────────────────────────────────
    np.random.seed(SEED)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)

    n_comp = min(PCA_N_COMPONENTS, N - 1, X_scaled.shape[1])
    pca    = PCA(n_components=n_comp, random_state=SEED)
    X_pca  = pca.fit_transform(X_scaled).astype(np.float64)
    expl   = pca.explained_variance_ratio_.sum()
    log.info(f"  PCA: {X_scaled.shape[1]} -> {n_comp} components, variance explained: {expl:.1%}")

    # ── BIC search ────────────────────────────────────────────────────────────
    bics: dict[int, float] = {}
    models: dict[int, GaussianMixture] = {}
    for k in range(2, 11):
        gmm = GaussianMixture(
            n_components=k, covariance_type="full",
            n_init=5, reg_covar=1e-3, random_state=SEED,
        )
        gmm.fit(X_pca)
        bics[k]   = gmm.bic(X_pca)
        models[k] = gmm

    best_k   = min(bics, key=bics.get)
    # No k=3 constraint — with raw (un-normalised) features the BIC landscape
    # reflects genuine data structure. Trust the BIC minimum directly.

    log.info(f"  BIC-selected k={best_k}  BIC={bics[best_k]:.1f}")
    for k, v in sorted(bics.items()):
        log.info(f"    k={k}: BIC={v:.1f}{'  <-- selected' if k == best_k else ''}")

    # ── Fit final GMM ─────────────────────────────────────────────────────────
    best_gmm = GaussianMixture(
        n_components=best_k, covariance_type="full",
        n_init=10, reg_covar=1e-3, random_state=SEED,
    )
    best_gmm.fit(X_pca)
    labels = best_gmm.predict(X_pca)
    probs  = best_gmm.predict_proba(X_pca)

    # Secondary (soft) assignment: second-highest probability cluster per street
    sorted_prob_idx  = np.argsort(probs, axis=1)          # ascending → last = primary
    secondary_idx    = sorted_prob_idx[:, -2]             # second-highest cluster index
    secondary_prob   = probs[np.arange(len(probs)), secondary_idx]

    sizes = {int(k): int((labels == k).sum()) for k in range(best_k)}
    log.info(f"  Cluster sizes: {sizes}")

    # ── Stability (bootstrap ARI) ─────────────────────────────────────────────
    aris = []
    for i in range(STABILITY_ITERS):
        Xs, ls = resample(X_pca, labels, random_state=i)
        g = GaussianMixture(
            n_components=best_k, covariance_type="full",
            n_init=3, reg_covar=1e-3, random_state=i,
        )
        g.fit(Xs)
        aris.append(adjusted_rand_score(ls, g.predict(Xs)))

    mean_ari = float(np.mean(aris))
    sil      = float(silhouette_score(X_pca, labels)) if best_k > 1 else 0.0
    log.info(f"  ARI={mean_ari:.3f}  silhouette={sil:.3f}")
    if mean_ari < 0.70:
        log.warning(f"  ARI={mean_ari:.3f} < 0.70 — clustering may be unstable")

    # ── Archetype labelling ───────────────────────────────────────────────────
    archetype_map = _assign_archetypes(best_gmm, scaler, pca, joint_cols, best_k)
    log.info(f"  Archetypes: {archetype_map}")

    # ── Flexibility windows ───────────────────────────────────────────────────
    park_df = profiles.set_index("street_id")[park_cols]
    ped_df  = profiles.set_index("street_id")[ped_cols]
    # Reorder to match labels array order
    park_df = park_df.loc[streets]
    ped_df  = ped_df.loc[streets]

    flex_all, flex_best = _flexibility_windows(park_df, ped_df, labels, best_k)

    # ── Confidence ────────────────────────────────────────────────────────────
    cluster_conf = probs.max(axis=1)
    n_uncertain  = int((cluster_conf < CONFIDENCE_THRESHOLD).sum())
    if n_uncertain:
        log.warning(f"  {n_uncertain}/{N} streets have confidence < {CONFIDENCE_THRESHOLD}")

    # ── Assemble output ───────────────────────────────────────────────────────
    prob_cols = {f"cluster_prob_{k}": probs[:, k] for k in range(best_k)}
    result = pd.DataFrame({
        "street_id":              streets,
        "cluster":                labels.tolist(),
        "intervention_type":      [archetype_map[int(l)] for l in labels],
        "cluster_confidence":     cluster_conf,
        "intervention_reliable":  cluster_conf >= CONFIDENCE_THRESHOLD,
        "secondary_cluster":      secondary_idx.tolist(),
        "secondary_archetype":    [archetype_map[int(i)] for i in secondary_idx],
        "secondary_prob":         secondary_prob.round(4).tolist(),
        "flexibility_windows":    flex_all,
        "best_window":            flex_best,
        "mean_ped":               profiles["mean_ped"].tolist(),
        "mean_parking":           profiles["mean_parking"].tolist(),
        "ped_confidence":         profiles["ped_confidence"].tolist(),
        **prob_cols,
    })

    # Centroids back-projected to original feature space
    means_orig = scaler.inverse_transform(pca.inverse_transform(best_gmm.means_))
    centroid_df = pd.DataFrame(
        means_orig,
        columns=joint_cols,
        index=[f"cluster_{k}" for k in range(best_k)],
    )

    # ── Save ──────────────────────────────────────────────────────────────────
    clustered_path  = PROCESSED_DIR / "clustered.parquet"
    centroids_path  = PROCESSED_DIR / "cluster_centroids.csv"
    report_path     = PROCESSED_DIR / "cluster_report.json"

    result.to_parquet(clustered_path, index=False)
    centroid_df.to_csv(centroids_path)

    report = {
        "k":                       best_k,
        "bic":                     {str(k): round(v, 1) for k, v in bics.items()},
        "ari":                     round(mean_ari, 4),
        "silhouette":              round(sil, 4),
        "pca_components":          n_comp,
        "pca_variance_explained":  round(float(expl), 4),
        "archetype_map":           {str(k): v for k, v in archetype_map.items()},
        "cluster_sizes":           {str(k): v for k, v in sizes.items()},
        "n_uncertain":             n_uncertain,
        "n_with_flex_windows":     int(sum(1 for w in flex_best if w)),
    }
    report_path.write_text(json.dumps(report, indent=2))

    log.info(f"  Streets with flexibility windows: {report['n_with_flex_windows']}")
    log.info(f"  Saved: {clustered_path}")

    # ── Cluster summary (for viz) ──────────────────────────────────────────────
    summ_cols = ["street_id", "cluster", "intervention_type", "cluster_confidence",
                 "intervention_reliable", "best_window", "mean_ped", "mean_parking",
                 "secondary_cluster", "secondary_archetype", "secondary_prob"]
    summ_df = result[summ_cols].copy()
    summ_df["cluster_confidence"] = summ_df["cluster_confidence"].round(4)
    summ_df["mean_ped"]           = summ_df["mean_ped"].round(2)
    summ_df["mean_parking"]       = summ_df["mean_parking"].round(4)
    summ_dict = summ_df.set_index("street_id").to_dict(orient="index")
    summ_path = PROCESSED_DIR / "cluster_summary.json"
    summ_path.write_text(json.dumps(summ_dict, indent=None))
    log.info(f"  Saved cluster_summary.json ({len(summ_dict)} streets)")

    log.info("Step 7 complete.")
    return {
        "clustered":         clustered_path,
        "cluster_centroids": centroids_path,
        "report":            report_path,
        "cluster_summary":   summ_path,
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
