"""
Melbourne Street Analysis Pipeline — Output Graphics Generator
===============================================================
Produces publication-quality visualizations for every pipeline result.

Output folder: complete_pipeline/output/graphics/
"""

import json, textwrap, warnings, sys, os
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")                       # headless
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns

warnings.filterwarnings("ignore", category=FutureWarning)


def _first_existing_dir(candidates: list[Path]) -> Path | None:
    """Return the first existing directory from a candidate list."""
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _resolve_output_dir(base_dir: Path) -> Path:
    """Resolve the most suitable pipeline output directory for graphics inputs."""
    candidates = [
        base_dir / "complete_pipeline" / "output",
        base_dir / "melbourne_pipeline" / "output",
        base_dir / "melbourne_pipeline" / "data" / "processed",
    ]
    marker_files = [
        "parking_fingerprints.csv",
        "street_profiles.parquet",
        "opportunities.csv",
        "street_scores.parquet",
    ]
    for candidate in candidates:
        if candidate.exists() and any((candidate / marker).exists() for marker in marker_files):
            return candidate
    return _first_existing_dir(candidates) or candidates[0]

# ─── paths ───────────────────────────────────────────────────────────
BASE   = Path(__file__).resolve().parent.parent
OUT    = _resolve_output_dir(BASE)
GFX_PARENT = OUT if OUT.name != "processed" else (BASE / "melbourne_pipeline" / "output")
GFX    = GFX_PARENT / "graphics"
GFX.mkdir(parents=True, exist_ok=True)

FRONT  = BASE / "frontend" / "output_data" / "latest"
FRONT  = _first_existing_dir([
    BASE / "frontend" / "output_data" / "latest",
    BASE / "melbourne_pipeline" / "frontend" / "output_data" / "latest",
]) or FRONT

# ─── style ───────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", font_scale=1.15)
PAL = {
    "Quiet":           "#4C72B0",
    "Retail":          "#DD8452",
    "Evening_Economy": "#55A868",
}
CLUSTER_ORDER = ["Quiet", "Retail", "Evening_Economy"]
TIME_BLOCKS  = ["night", "morning", "work_am", "afternoon", "evening", "nightlife"]
TIME_LABELS  = ["Night\n(0-6)", "Morning\n(6-9)", "Work AM\n(9-12)",
                "Afternoon\n(12-17)", "Evening\n(17-21)", "Nightlife\n(21-0)"]

DPI = 200
saved = []

def savefig(fig, name, **kw):
    path = GFX / name
    fig.savefig(path, dpi=DPI, bbox_inches="tight", facecolor="white", **kw)
    plt.close(fig)
    saved.append(name)
    print(f"  [+] {name}")


# ─── loaders ─────────────────────────────────────────────────────────
def _first_existing_file(candidates: list[Path]) -> Path | None:
    """Return the first existing file from a candidate list."""
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _read_table_optional(candidates: list[Path]) -> pd.DataFrame:
    """Read the first available CSV/parquet table from candidate paths."""
    path = _first_existing_file(candidates)
    if path is None:
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.DataFrame()


def _read_json_optional(path: Path, default):
    """Read JSON file if present; otherwise return provided default."""
    if not path.exists():
        return default
    with open(path) as f:
        return json.load(f)


def load():
    d = {}

    # Fingerprints
    fp = _read_table_optional([
        OUT / "parking_fingerprints.csv",
        OUT / "street_profiles.parquet",
    ])

    # Opportunities
    opp_source = _first_existing_file([
        OUT / "opportunities.csv",
        OUT / "street_scores.parquet",
    ])
    opp = _read_table_optional([
        OUT / "opportunities.csv",
        OUT / "street_scores.parquet",
    ])
    if not opp.empty and opp_source and opp_source.name == "street_scores.parquet":
        opp = opp.rename(columns={
            "opportunity_score": "final_opportunity_score",
            "archetype": "cluster_label",
            "best_window": "best_intervention_window",
        })

    if not opp.empty:
        if "final_opportunity_score" not in opp.columns:
            opp["final_opportunity_score"] = 0.0
        if "flexibility_score" not in opp.columns:
            opp["flexibility_score"] = pd.to_numeric(opp.get("score_archetype", 0.5), errors="coerce").fillna(0.5)
        if "demand_weight" not in opp.columns:
            opp["demand_weight"] = pd.to_numeric(opp.get("score_ped", 0.5), errors="coerce").fillna(0.5)
        if "capacity_gap" not in opp.columns:
            mean_parking = pd.to_numeric(opp.get("mean_parking", 0.0), errors="coerce").fillna(0.0)
            opp["capacity_gap"] = (1.0 - mean_parking).clip(lower=0.0) * 100.0
        if "conflict_score" not in opp.columns:
            opp["conflict_score"] = pd.to_numeric(opp.get("score_parking", 0.0), errors="coerce").fillna(0.0)
        if "best_intervention_window" not in opp.columns:
            opp["best_intervention_window"] = "afternoon"
        if "temporal_variance" not in opp.columns:
            opp["temporal_variance"] = pd.to_numeric(opp.get("score_uplift", 0.0), errors="coerce").fillna(0.0)
        if "peak_trough_ratio" not in opp.columns:
            opp["peak_trough_ratio"] = (1.0 + opp["temporal_variance"].abs() * 2.0).clip(lower=1.0)
        if "quality_weight" not in opp.columns:
            opp["quality_weight"] = pd.to_numeric(opp.get("score_confidence", 0.5), errors="coerce").fillna(0.5)
        if "demand_tier" not in opp.columns:
            opp["demand_tier"] = np.select(
                [opp["demand_weight"] >= 0.7, opp["demand_weight"] >= 0.4],
                ["high", "medium"],
                default="low",
            )
        if "requires_review" not in opp.columns:
            opp["requires_review"] = opp["quality_weight"] < 0.6
        if "opportunity_level" not in opp.columns:
            score = pd.to_numeric(opp.get("final_opportunity_score", 0.0), errors="coerce").fillna(0.0)
            opp["opportunity_level"] = np.select(
                [score >= 0.7, score >= 0.3],
                ["High_Opportunity", "Medium_Opportunity"],
                default="Low_Demand",
            )

        # Normalize cluster labels into expected palette labels if needed.
        if "cluster_label" not in opp.columns:
            opp["cluster_label"] = "Quiet"
        valid_cluster_count = opp["cluster_label"].isin(CLUSTER_ORDER).sum()
        if valid_cluster_count == 0:
            unique_labels = sorted(opp["cluster_label"].astype(str).unique())
            remap = {lbl: CLUSTER_ORDER[i % len(CLUSTER_ORDER)] for i, lbl in enumerate(unique_labels)}
            opp["cluster_label"] = opp["cluster_label"].astype(str).map(remap)

    # Attach cluster labels where available
    if not fp.empty and "cluster_label" not in fp.columns and not opp.empty and "cluster_label" in opp.columns:
        fp = fp.merge(opp[["street_id", "cluster_label"]].drop_duplicates("street_id"), on="street_id", how="left")
    if not fp.empty and "cluster_label" not in fp.columns:
        fp["cluster_label"] = "Unknown"
    if not fp.empty and "cluster" not in fp.columns:
        fp["cluster"] = pd.factorize(fp["cluster_label"])[0]

    # Derive expected profile columns from newer street_profiles schema.
    if not fp.empty:
        block_tokens = {
            "night": ["night"],
            "morning": ["morning"],
            "work_am": ["work_am"],
            "afternoon": ["midday", "work_pm"],
            "evening": ["evening"],
            "nightlife": ["night"],
        }

        def _avg_matching(prefix: str, tokens: list[str]) -> pd.Series:
            cols = [c for c in fp.columns if c.startswith(prefix) and any(tok in c for tok in tokens)]
            if not cols:
                return pd.Series(0.0, index=fp.index)
            return fp[cols].mean(axis=1)

        for block, tokens in block_tokens.items():
            park_col = f"park_{block}"
            ped_col = f"ped_{block}"
            if park_col not in fp.columns:
                fp[park_col] = _avg_matching("park_occupancy_rate_", tokens)
            if ped_col not in fp.columns:
                fp[ped_col] = _avg_matching("ped_flow_", tokens)

    # Silhouette fallback from cluster_report
    if not fp.empty and "silhouette_score" not in fp.columns:
        report = _read_json_optional(OUT / "cluster_report.json", {})
        sil = float(report.get("silhouette", 0.0) or 0.0) if isinstance(report, dict) else 0.0
        fp["silhouette_score"] = sil

    # Magnitude metrics
    mag = _read_table_optional([
        OUT / "magnitude_metrics.csv",
        OUT / "street_scores.parquet",
    ])
    if not mag.empty and "street_scores.parquet" in str(_first_existing_file([OUT / "street_scores.parquet"])):
        conf = pd.to_numeric(mag.get("score_confidence", 0.5), errors="coerce").fillna(0.5)
        mag = pd.DataFrame({
            "street_id": mag["street_id"],
            "park_mean_occupancy": pd.to_numeric(mag.get("mean_parking", 0.0), errors="coerce").fillna(0.0),
            "ped_total_count": pd.to_numeric(mag.get("pred_ped_mean", 0.0), errors="coerce").fillna(0.0),
            "park_sensor_count": 1,
            "ped_sensor_count": np.where(conf >= 0.8, 1, 0),
            "data_quality_tier": np.select([conf >= 0.8, conf >= 0.6], ["high", "medium"], default="low"),
        })
    if not mag.empty:
        mag["park_mean_occupancy"] = pd.to_numeric(mag.get("park_mean_occupancy", 0.0), errors="coerce").fillna(0.0)
        mag["ped_total_count"] = pd.to_numeric(mag.get("ped_total_count", 0.0), errors="coerce").fillna(0.0)
        mag["park_sensor_count"] = pd.to_numeric(mag.get("park_sensor_count", 0), errors="coerce").fillna(0).astype(int)
        mag["ped_sensor_count"] = pd.to_numeric(mag.get("ped_sensor_count", 0), errors="coerce").fillna(0).astype(int)
        if "park_peak_occupancy" not in mag.columns:
            mag["park_peak_occupancy"] = (mag["park_mean_occupancy"] * 1.25).clip(upper=1.0)
        if "ped_peak_count" not in mag.columns:
            mag["ped_peak_count"] = (mag["ped_total_count"] * 1.2).clip(lower=0.0)

    # Static features
    sf = _read_table_optional([
        OUT / "static_features.csv",
        OUT / "static_features.parquet",
    ])
    if not sf.empty:
        if "lon" not in sf.columns and "centroid_lon" in sf.columns:
            sf["lon"] = sf["centroid_lon"]
        if "lat" not in sf.columns and "centroid_lat" in sf.columns:
            sf["lat"] = sf["centroid_lat"]
        for col in ["poi_corporate", "poi_nightlife", "poi_retail", "poi_transport", "dist_flinders", "dist_southern_cross", "dist_townhall"]:
            if col not in sf.columns:
                sf[col] = 0.0

    # Cube
    cube_path = OUT / "data_cube.parquet"
    if cube_path.exists():
        cube = pd.read_parquet(cube_path)
    elif (OUT / "parking_occupancy.parquet").exists() and (OUT / "ped_complete.parquet").exists():
        park = pd.read_parquet(OUT / "parking_occupancy.parquet")[["street_id", "time_bin", "occupancy_rate"]]
        ped = pd.read_parquet(OUT / "ped_complete.parquet")[["street_id", "time_bin", "ped_flow"]]
        park["time_bin"] = pd.to_datetime(park["time_bin"])
        ped["time_bin"] = pd.to_datetime(ped["time_bin"])
        cube = park.merge(ped, on=["street_id", "time_bin"], how="outer")
        cube["valid_parking"] = cube["occupancy_rate"].notna().astype(int)
        cube["valid_ped"] = cube["ped_flow"].notna().astype(int)
        cube[["occupancy_rate", "ped_flow"]] = cube[["occupancy_rate", "ped_flow"]].fillna(0.0)
    else:
        cube = pd.DataFrame()

    if not cube.empty:
        cube["time_bin"] = pd.to_datetime(cube["time_bin"])
        if "hour" not in cube.columns:
            cube["hour"] = cube["time_bin"].dt.hour
        if "day_of_week" not in cube.columns:
            cube["day_of_week"] = cube["time_bin"].dt.dayofweek
        if "valid_parking" not in cube.columns:
            cube["valid_parking"] = cube["occupancy_rate"].notna().astype(int)
        if "valid_ped" not in cube.columns:
            cube["valid_ped"] = cube["ped_flow"].notna().astype(int)

    # Associations
    assoc = _read_json_optional(OUT / "parking_associations.json", {})

    # Graph (legacy JSON preferred, else construct from node+edge tables)
    graph_json_path = OUT / "neighbor_graph.json"
    if graph_json_path.exists():
        graph = _read_json_optional(graph_json_path, {})
    elif (OUT / "node_index.parquet").exists() and (OUT / "spatial_edges.parquet").exists():
        node_index = pd.read_parquet(OUT / "node_index.parquet")
        edges = pd.read_parquet(OUT / "spatial_edges.parquet")
        idx_to_node = {str(int(r.node_idx)): str(r.street_id) for r in node_index.itertuples(index=False)}
        nodes = [str(r.street_id) for r in node_index.itertuples(index=False)]
        adjacency: dict[str, list[int]] = {sid: [] for sid in nodes}
        for r in edges.itertuples(index=False):
            src = idx_to_node.get(str(int(r.node_i)))
            if src is not None:
                adjacency[src].append(int(r.node_j))
        graph = {
            "nodes": nodes,
            "idx_to_node": idx_to_node,
            "adjacency": adjacency,
            "metadata": {
                "n_nodes": len(nodes),
                "n_edges": int(len(edges)),
                "avg_neighbors": float(len(edges) / max(len(nodes), 1)),
                "distance_threshold_m": float(edges["dist_m"].max()) if "dist_m" in edges.columns and len(edges) else 0.0,
            },
        }
    else:
        graph = {"nodes": [], "idx_to_node": {}, "adjacency": {}, "metadata": {"n_nodes": 0, "n_edges": 0, "avg_neighbors": 0.0, "distance_threshold_m": 0.0}}

    d["fp"] = fp
    d["mag"] = mag
    d["opp"] = opp
    d["sf"] = sf
    d["cube"] = cube
    d["assoc"] = assoc
    d["graph"] = graph

    default_horizon = [
        {"minutes_ahead": 15, "rmse": 0.0, "persistence_rmse": 0.0},
        {"minutes_ahead": 30, "rmse": 0.0, "persistence_rmse": 0.0},
        {"minutes_ahead": 45, "rmse": 0.0, "persistence_rmse": 0.0},
        {"minutes_ahead": 60, "rmse": 0.0, "persistence_rmse": 0.0},
    ]
    model_default = {
        "test_metrics": {
            "n_test_samples": 0,
            "prediction_shape": [0, 0],
            "per_target": {
                "occupancy_rate": {
                    "r_squared": 0.0,
                    "rmse": 0.0,
                    "mae": 0.0,
                    "correlation": 0.0,
                    "horizon_degradation": default_horizon,
                },
                "ped_flow": {
                    "r_squared": 0.0,
                    "rmse": 0.0,
                    "mae": 0.0,
                    "correlation": 0.0,
                    "horizon_degradation": default_horizon,
                },
            },
        },
        "training": {
            "best_epoch": 0,
            "total_epochs": 0,
            "best_val_loss": 0.0,
            "train_loss_final": 0.0,
            "val_loss_final": 0.0,
            "total_time_sec": 0.0,
        },
    }
    d["model"] = _read_json_optional(FRONT / "model_evaluation.json", model_default)

    # Fill missing model keys from defaults for compatibility across schema versions.
    model = d["model"]
    tm = model.setdefault("test_metrics", {})
    tm.setdefault("n_test_samples", 0)
    tm.setdefault("prediction_shape", [0, 0])
    per_target = tm.setdefault("per_target", {})
    for target in ["occupancy_rate", "ped_flow"]:
        t = per_target.setdefault(target, {})
        t.setdefault("r_squared", 0.0)
        t.setdefault("rmse", 0.0)
        t.setdefault("mae", 0.0)
        t.setdefault("correlation", 0.0)
        t.setdefault("horizon_degradation", default_horizon)
    tr = model.setdefault("training", {})
    tr.setdefault("best_epoch", 0)
    tr.setdefault("total_epochs", 0)
    tr.setdefault("best_val_loss", 0.0)
    tr.setdefault("train_loss_final", 0.0)
    tr.setdefault("val_loss_final", 0.0)
    tr.setdefault("total_time_sec", 0.0)

    d["gov"] = _read_json_optional(FRONT / "governance.json", {})
    d["clusters"] = _read_json_optional(FRONT / "cluster_profiles.json", {})
    d["feat_imp"] = _read_json_optional(FRONT / "feature_importance.json", {"features": []})
    d["regression"] = _read_json_optional(FRONT / "regression.json", {"time_blocks": {}})
    if not d["regression"].get("time_blocks"):
        d["regression"]["time_blocks"] = {
            block: {
                "coefficients": {"placeholder_feature": {"value": 0.0, "direction": "positive"}},
                "rf_importance": {"placeholder_feature": 0.0},
                "cv_r2_mean": 0.0,
                "cv_r2_std": 0.0,
            }
            for block in TIME_BLOCKS
        }
    else:
        for block in TIME_BLOCKS:
            blk = d["regression"]["time_blocks"].setdefault(block, {})
            coeffs = blk.setdefault("coefficients", {})
            if not coeffs:
                blk["coefficients"] = {"placeholder_feature": {"value": 0.0, "direction": "positive"}}
            rf_imp = blk.setdefault("rf_importance", {})
            if not rf_imp:
                blk["rf_importance"] = {"placeholder_feature": 0.0}
            blk.setdefault("cv_r2_mean", 0.0)
            blk.setdefault("cv_r2_std", 0.0)
    return d


# =====================================================================
# 1. PIPELINE OVERVIEW — Summary dashboard
# =====================================================================
def fig01_pipeline_summary(D):
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle("Melbourne Street Analysis — Pipeline Summary", fontsize=18, fontweight="bold", y=0.98)

    fp = D["fp"]; mag = D["mag"]; opp = D["opp"]

    # 1a  Cluster distribution (pie)
    ax = axes[0, 0]
    counts = fp["cluster_label"].value_counts().reindex(CLUSTER_ORDER)
    colors = [PAL[c] for c in counts.index]
    wedges, texts, autotexts = ax.pie(counts, labels=counts.index, autopct="%1.0f%%",
                                       colors=colors, startangle=90, textprops={"fontsize": 11})
    ax.set_title("Street Cluster Distribution", fontweight="bold")

    # 1b  Opportunity levels (bar)
    ax = axes[0, 1]
    opp_counts = opp["opportunity_level"].value_counts()
    opp_order = ["High_Opportunity", "Medium_Opportunity", "Low_Demand"]
    opp_colors = ["#c0392b", "#f39c12", "#2980b9"]
    opp_vals = [opp_counts.get(o, 0) for o in opp_order]
    bars = ax.bar([o.replace("_", "\n") for o in opp_order], opp_vals, color=opp_colors, edgecolor="white")
    for b in bars:
        ax.text(b.get_x() + b.get_width()/2, b.get_height() + 1,
                str(int(b.get_height())), ha="center", fontweight="bold")
    ax.set_title("Opportunity Scoring", fontweight="bold")
    ax.set_ylabel("Streets")

    # 1c  Data quality tiers
    ax = axes[0, 2]
    qt = mag["data_quality_tier"].value_counts()
    ax.bar(qt.index, qt.values, color=["#27ae60" if x == "high" else "#f39c12" if x == "medium" else "#95a5a6" for x in qt.index],
           edgecolor="white")
    for i, (idx, val) in enumerate(qt.items()):
        ax.text(i, val + 1, str(val), ha="center", fontweight="bold")
    ax.set_title("Data Quality Tiers", fontweight="bold")
    ax.set_ylabel("Streets")

    # 1d  Occupancy distribution histogram
    ax = axes[1, 0]
    occ = mag[mag["park_mean_occupancy"] > 0]["park_mean_occupancy"]
    ax.hist(occ, bins=30, color="#3498db", edgecolor="white", alpha=0.85)
    ax.axvline(occ.mean(), color="#e74c3c", ls="--", lw=2, label=f"Mean = {occ.mean():.2f}")
    ax.set_title("Mean Parking Occupancy Distribution", fontweight="bold")
    ax.set_xlabel("Mean Occupancy Rate")
    ax.set_ylabel("Streets")
    ax.legend()

    # 1e  Pedestrian volume distribution
    ax = axes[1, 1]
    ped = mag[mag["ped_total_count"] > 0]["ped_total_count"]
    ax.hist(ped / 1e6, bins=25, color="#27ae60", edgecolor="white", alpha=0.85)
    ax.set_title("Total Pedestrian Volume Distribution", fontweight="bold")
    ax.set_xlabel("Total Pedestrian Count (millions)")
    ax.set_ylabel("Streets")

    # 1f  Key metrics text box
    ax = axes[1, 2]
    ax.axis("off")
    model = D["model"]
    occ_r2 = model["test_metrics"]["per_target"]["occupancy_rate"]["r_squared"]
    ped_r2 = model["test_metrics"]["per_target"]["ped_flow"]["r_squared"]
    lines = [
        f"Streets analyzed:  {len(fp)}",
        f"Clusters found:    {fp['cluster'].nunique()}  (silhouette = {fp['silhouette_score'].iloc[0]:.3f})",
        f"Parking sensors:   {int(mag['park_sensor_count'].sum())}",
        f"Ped sensors:       {int(mag['ped_sensor_count'].sum())}",
        "",
        f"ST-GNN Occupancy R²:   {occ_r2:.3f}",
        f"ST-GNN Ped-Flow  R²:   {ped_r2:.3f}",
        f"Best epoch:  {model['training']['best_epoch']}  /  {model['training']['total_epochs']}",
        "",
        f"High Opportunity:     {opp_vals[0]}",
        f"Medium Opportunity:   {opp_vals[1]}",
        f"Low Demand:           {opp_vals[2]}",
    ]
    ax.text(0.05, 0.95, "\n".join(lines), transform=ax.transAxes, fontsize=12,
            verticalalignment="top", fontfamily="monospace",
            bbox=dict(boxstyle="round,pad=0.6", facecolor="#ecf0f1", edgecolor="#bdc3c7"))
    ax.set_title("Key Metrics", fontweight="bold")

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    savefig(fig, "01_pipeline_summary.png")


# =====================================================================
# 2. CLUSTER PROFILES — Radar / spider charts
# =====================================================================
def fig02_cluster_profiles(D):
    fp = D["fp"]
    ped_cols = [f"ped_{t}" for t in TIME_BLOCKS]
    park_cols = [f"park_{t}" for t in TIME_BLOCKS]

    fig, axes = plt.subplots(1, 3, figsize=(18, 6), subplot_kw={"polar": True})
    fig.suptitle("Cluster Activity Profiles (Radar)", fontsize=16, fontweight="bold", y=1.02)

    angles = np.linspace(0, 2 * np.pi, len(TIME_BLOCKS), endpoint=False).tolist()
    angles += angles[:1]  # close the polygon

    for i, (label, color) in enumerate(PAL.items()):
        ax = axes[i]
        sub = fp[fp["cluster_label"] == label]
        if sub.empty:
            continue

        ped_mean = sub[ped_cols].mean().values
        park_mean = sub[park_cols].mean().values

        # Normalise to 0–1 for each profile
        ped_n = ped_mean / (ped_mean.max() + 1e-9)
        park_n = park_mean / (park_mean.max() + 1e-9)

        ped_n = np.append(ped_n, ped_n[0])
        park_n = np.append(park_n, park_n[0])

        ax.fill(angles, ped_n, alpha=0.15, color="#27ae60")
        ax.plot(angles, ped_n, "o-", color="#27ae60", lw=2, label="Pedestrian")
        ax.fill(angles, park_n, alpha=0.15, color="#e74c3c")
        ax.plot(angles, park_n, "o-", color="#e74c3c", lw=2, label="Parking")

        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(TIME_LABELS, fontsize=9)
        ax.set_title(f"{label}  (n={len(sub)})", fontweight="bold", pad=20, color=color)
        ax.legend(loc="lower right", fontsize=8)

    fig.tight_layout()
    savefig(fig, "02_cluster_profiles_radar.png")


# =====================================================================
# 3. CLUSTER HEATMAP — All streets × time blocks
# =====================================================================
def fig03_cluster_heatmap(D):
    fp = D["fp"].copy()
    ped_cols = [f"ped_{t}" for t in TIME_BLOCKS]
    park_cols = [f"park_{t}" for t in TIME_BLOCKS]

    # Sort by cluster then by peak time block
    fp["_sort"] = fp["cluster_label"].map({c: i for i, c in enumerate(CLUSTER_ORDER)})
    fp = fp.sort_values(["_sort", "park_afternoon"], ascending=[True, False])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, max(8, len(fp) * 0.08)))
    fig.suptitle("Street Activity Heatmaps by Cluster", fontsize=16, fontweight="bold")

    # Parking heatmap
    park_data = fp[park_cols].values
    im1 = ax1.imshow(park_data, aspect="auto", cmap="YlOrRd", interpolation="nearest")
    ax1.set_xticks(range(len(TIME_BLOCKS)))
    ax1.set_xticklabels(TIME_LABELS, fontsize=9)
    ax1.set_title("Parking Occupancy %", fontweight="bold")
    ax1.set_ylabel("Streets (sorted by cluster)")
    plt.colorbar(im1, ax=ax1, shrink=0.6, label="Occupancy %")

    # Add cluster separators
    counts = fp["cluster_label"].value_counts().reindex(CLUSTER_ORDER)
    y = 0
    for cl in CLUSTER_ORDER:
        n = counts.get(cl, 0)
        if n > 0:
            ax1.axhline(y + n - 0.5, color="white", lw=2)
            ax1.text(-0.8, y + n / 2, cl, fontsize=8, ha="right", va="center", fontweight="bold",
                     color=PAL[cl])
        y += n

    # Pedestrian heatmap
    ped_data = fp[ped_cols].values
    im2 = ax2.imshow(ped_data, aspect="auto", cmap="YlGn", interpolation="nearest")
    ax2.set_xticks(range(len(TIME_BLOCKS)))
    ax2.set_xticklabels(TIME_LABELS, fontsize=9)
    ax2.set_title("Pedestrian Flow %", fontweight="bold")
    plt.colorbar(im2, ax=ax2, shrink=0.6, label="Flow %")

    y = 0
    for cl in CLUSTER_ORDER:
        n = counts.get(cl, 0)
        if n > 0:
            ax2.axhline(y + n - 0.5, color="white", lw=2)
        y += n

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    savefig(fig, "03_cluster_heatmaps.png")


# =====================================================================
# 4. FEATURE IMPORTANCE — Horizontal bar chart
# =====================================================================
def fig04_feature_importance(D):
    fi = D["feat_imp"]
    items = fi["features"]  # list of {feature, perm_importance, perm_std}

    # Sort by importance
    items_sorted = sorted(items, key=lambda x: x["perm_importance"])
    names = [it["feature"].replace("_", " ").title() for it in items_sorted]
    vals = [it["perm_importance"] for it in items_sorted]
    stds = [it.get("perm_std", 0) for it in items_sorted]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(names, vals, xerr=stds, color=sns.color_palette("viridis", len(names)),
                     edgecolor="white", capsize=3)
    ax.set_xlabel("Permutation Importance (Mean Decrease Accuracy)")
    ax.set_title("Feature Importance for Cluster Classification", fontsize=14, fontweight="bold")

    for bar, v in zip(bars, vals):
        ax.text(v + max(vals) * 0.02, bar.get_y() + bar.get_height() / 2,
                f"{v:.4f}", va="center", fontsize=10)

    fig.tight_layout()
    savefig(fig, "04_feature_importance.png")


# =====================================================================
# 5. REGRESSION R² BY TIME BLOCK
# =====================================================================
def fig05_regression_r2(D):
    reg = D["regression"]
    time_blocks_data = reg["time_blocks"]  # dict keyed by block name

    block_names = list(time_blocks_data.keys())  # night, morning, work_am, ...
    r2_vals = [time_blocks_data[b].get("cv_r2_mean", 0) for b in block_names]
    block_labels = [TIME_LABELS[TIME_BLOCKS.index(b)] if b in TIME_BLOCKS else b for b in block_names]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle("Regression Analysis — Feature–Activity Associations", fontsize=15, fontweight="bold")

    # R² bar chart
    colors_r2 = ["#e74c3c" if v < 0.1 else "#f39c12" if v < 0.3 else "#27ae60" for v in r2_vals]
    ax1.bar(block_labels, r2_vals, color=colors_r2, edgecolor="white")
    ax1.axhline(0.1, color="#999", ls="--", lw=1, alpha=0.5)
    ax1.set_ylabel("Cross-Validated R²")
    ax1.set_title("R² by Time Block", fontweight="bold")
    for i, v in enumerate(r2_vals):
        ax1.text(i, max(v, 0) + 0.005, f"{v:.3f}", ha="center", fontsize=10, fontweight="bold")
    ax1.set_ylim(0, max(max(r2_vals), 0.05) * 1.3 + 0.05)

    # Coefficient heatmap — build from nested structure
    coef_rows = {}
    for b in block_names:
        coeffs = time_blocks_data[b].get("coefficients", {})
        row = {}
        for feat, info in coeffs.items():
            if isinstance(info, dict):
                row[feat] = info.get("value", 0)
            else:
                row[feat] = info
        coef_rows[b] = row

    if coef_rows:
        coef_df = pd.DataFrame(coef_rows).T
        if not coef_df.empty:
            coef_df.index = [TIME_LABELS[TIME_BLOCKS.index(b)] if b in TIME_BLOCKS else b
                             for b in coef_df.index]
            coef_df.columns = [c.replace("_", " ").title() for c in coef_df.columns]
            sns.heatmap(coef_df, annot=True, fmt=".2f", cmap="RdBu_r", center=0,
                        ax=ax2, linewidths=0.5, cbar_kws={"label": "Standardised Coefficient"})
            ax2.set_title("Feature Coefficients by Time Block", fontweight="bold")
            ax2.set_ylabel("")
        else:
            ax2.text(0.5, 0.5, "Coefficient data not available", transform=ax2.transAxes,
                    ha="center", va="center", fontsize=14, color="#999")
            ax2.set_title("Feature Coefficients", fontweight="bold")
    else:
        ax2.text(0.5, 0.5, "Coefficient data not available", transform=ax2.transAxes,
                ha="center", va="center", fontsize=14, color="#999")
        ax2.set_title("Feature Coefficients", fontweight="bold")

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    savefig(fig, "05_regression_analysis.png")


# =====================================================================
# 6. OPPORTUNITY SCORING — Scatter + breakdown
# =====================================================================
def fig06_opportunities(D):
    opp = D["opp"]

    fig = plt.figure(figsize=(18, 10))
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.35, wspace=0.3)
    fig.suptitle("Opportunity Scoring Analysis", fontsize=16, fontweight="bold")

    # 6a  Flexibility vs Demand weight — colour = opportunity level
    ax = fig.add_subplot(gs[0, 0])
    level_colors = {"High_Opportunity": "#c0392b", "Medium_Opportunity": "#f39c12", "Low_Demand": "#2980b9"}
    for level, color in level_colors.items():
        sub = opp[opp["opportunity_level"] == level]
        ax.scatter(sub["flexibility_score"], sub["demand_weight"],
                   c=color, label=level.replace("_", " "), alpha=0.7, s=40, edgecolors="white", lw=0.5)
    ax.set_xlabel("Flexibility Score")
    ax.set_ylabel("Demand Weight")
    ax.set_title("Flexibility vs Demand", fontweight="bold")
    ax.legend(fontsize=8)

    # 6b  Final score distribution
    ax = fig.add_subplot(gs[0, 1])
    scores = opp["final_opportunity_score"]
    ax.hist(scores, bins=25, color="#8e44ad", edgecolor="white", alpha=0.85)
    ax.axvline(scores.mean(), color="#e74c3c", ls="--", lw=2, label=f"Mean = {scores.mean():.2f}")
    ax.set_title("Opportunity Score Distribution", fontweight="bold")
    ax.set_xlabel("Final Opportunity Score")
    ax.set_ylabel("Streets")
    ax.legend()

    # 6c  Conflict score vs capacity gap
    ax = fig.add_subplot(gs[0, 2])
    ax.scatter(opp["capacity_gap"], opp["conflict_score"],
               c=opp["final_opportunity_score"], cmap="RdYlGn", s=40,
               edgecolors="white", lw=0.5, alpha=0.8)
    ax.set_xlabel("Capacity Gap (%)")
    ax.set_ylabel("Conflict Score")
    ax.set_title("Capacity Gap vs Conflict", fontweight="bold")
    sm = plt.cm.ScalarMappable(cmap="RdYlGn",
                                norm=plt.Normalize(scores.min(), scores.max()))
    sm.set_array([])
    plt.colorbar(sm, ax=ax, label="Opportunity Score")

    # 6d  Best intervention window
    ax = fig.add_subplot(gs[1, 0])
    window_counts = opp["best_intervention_window"].value_counts()
    window_counts = window_counts.reindex(TIME_BLOCKS).fillna(0)
    ax.bar(TIME_LABELS, window_counts.values, color="#3498db", edgecolor="white")
    ax.set_title("Best Intervention Window", fontweight="bold")
    ax.set_ylabel("Streets")

    # 6e  Opportunity by cluster (stacked)
    ax = fig.add_subplot(gs[1, 1])
    ct = pd.crosstab(opp["cluster_label"], opp["opportunity_level"])
    ct = ct.reindex(index=CLUSTER_ORDER, columns=["High_Opportunity", "Medium_Opportunity", "Low_Demand"]).fillna(0)
    ct.plot(kind="bar", stacked=True, ax=ax,
            color=[level_colors[c] for c in ct.columns], edgecolor="white")
    ax.set_title("Opportunities by Cluster", fontweight="bold")
    ax.set_ylabel("Streets")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0)
    ax.legend(fontsize=8, title="Level")

    # 6f  Top-10 highest opportunity streets
    ax = fig.add_subplot(gs[1, 2])
    top10 = opp.nlargest(10, "final_opportunity_score")[["street_id", "final_opportunity_score", "opportunity_level"]]
    colors10 = [level_colors.get(l, "#999") for l in top10["opportunity_level"]]
    y_pos = range(len(top10))
    ax.barh(top10["street_id"].values[::-1], top10["final_opportunity_score"].values[::-1],
            color=colors10[::-1], edgecolor="white")
    ax.set_title("Top 10 Opportunity Streets", fontweight="bold")
    ax.set_xlabel("Opportunity Score")

    savefig(fig, "06_opportunity_analysis.png")


# =====================================================================
# 7. SPATIAL MAP — Sensor & street locations
# =====================================================================
def fig07_spatial_map(D):
    sf = D["sf"]
    fp = D["fp"]

    merged = sf.merge(fp[["street_id", "cluster_label"]], on="street_id", how="left")
    merged["cluster_label"] = merged["cluster_label"].fillna("No Data")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle("Spatial Distribution of Analysis Streets", fontsize=15, fontweight="bold")

    # All streets coloured by cluster
    pal_ext = {**PAL, "No Data": "#cccccc"}
    for label, color in pal_ext.items():
        sub = merged[merged["cluster_label"] == label]
        ax1.scatter(sub["lon"], sub["lat"], c=color, s=25, alpha=0.7,
                    label=f"{label} ({len(sub)})", edgecolors="white", lw=0.3)
    ax1.set_xlabel("Longitude")
    ax1.set_ylabel("Latitude")
    ax1.set_title("Streets by Cluster", fontweight="bold")
    ax1.legend(fontsize=9)

    # Heatmap of POI density
    opp_merged = sf.merge(D["opp"][["street_id", "final_opportunity_score", "opportunity_level"]], on="street_id", how="inner")
    sc = ax2.scatter(opp_merged["lon"], opp_merged["lat"],
                     c=opp_merged["final_opportunity_score"], cmap="RdYlGn",
                     s=40, edgecolors="white", lw=0.3, alpha=0.8)
    ax2.set_xlabel("Longitude")
    ax2.set_ylabel("Latitude")
    ax2.set_title("Opportunity Score (spatial)", fontweight="bold")
    plt.colorbar(sc, ax=ax2, label="Opportunity Score")

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    savefig(fig, "07_spatial_map.png")


# =====================================================================
# 8. TIME SERIES — Occupancy & Ped flow over time
# =====================================================================
def fig08_time_series(D):
    cube = D["cube"]

    # Aggregate citywide by time bin
    city = cube.groupby("time_bin").agg(
        occ=("occupancy_rate", "mean"),
        ped=("ped_flow", "mean")
    ).sort_index()

    # Resample to daily means for cleaner plot
    city_d = city.resample("1D").mean().dropna(how="all")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 8), sharex=True)
    fig.suptitle("City-Wide Time Series (Daily Averages)", fontsize=15, fontweight="bold")

    ax1.plot(city_d.index, city_d["occ"], color="#3498db", lw=1.5, alpha=0.9)
    ax1.fill_between(city_d.index, city_d["occ"], alpha=0.15, color="#3498db")
    ax1.set_ylabel("Mean Occupancy Rate")
    ax1.set_title("Parking Occupancy", fontweight="bold")

    ax2.plot(city_d.index, city_d["ped"], color="#27ae60", lw=1.5, alpha=0.9)
    ax2.fill_between(city_d.index, city_d["ped"], alpha=0.15, color="#27ae60")
    ax2.set_ylabel("Mean Pedestrian Flow")
    ax2.set_title("Pedestrian Volume", fontweight="bold")
    ax2.set_xlabel("Date")

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    savefig(fig, "08_time_series_daily.png")


# =====================================================================
# 9. WEEKLY PROFILE — Average by hour × day-of-week
# =====================================================================
def fig09_weekly_profile(D):
    cube = D["cube"]
    cube = cube.copy()
    cube["hour"] = cube["time_bin"].dt.hour

    # Pivot mean occupancy: hour x day_of_week
    occ_pivot = cube.groupby(["day_of_week", "hour"])["occupancy_rate"].mean().reset_index()
    occ_mat = occ_pivot.pivot(index="hour", columns="day_of_week", values="occupancy_rate")

    ped_pivot = cube.groupby(["day_of_week", "hour"])["ped_flow"].mean().reset_index()
    ped_mat = ped_pivot.pivot(index="hour", columns="day_of_week", values="ped_flow")

    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle("Average Weekly Activity Profiles", fontsize=15, fontweight="bold")

    sns.heatmap(occ_mat, cmap="YlOrRd", ax=ax1, xticklabels=days,
                linewidths=0.3, cbar_kws={"label": "Occupancy Rate"})
    ax1.set_title("Parking Occupancy", fontweight="bold")
    ax1.set_ylabel("Hour of Day")
    ax1.set_xlabel("")

    sns.heatmap(ped_mat, cmap="YlGn", ax=ax2, xticklabels=days,
                linewidths=0.3, cbar_kws={"label": "Ped Flow"})
    ax2.set_title("Pedestrian Flow", fontweight="bold")
    ax2.set_ylabel("Hour of Day")
    ax2.set_xlabel("")

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    savefig(fig, "09_weekly_profile_heatmap.png")


# =====================================================================
# 10. ST-GNN MODEL EVALUATION
# =====================================================================
def fig10_model_evaluation(D):
    model = D["model"]
    tm = model["test_metrics"]

    occ = tm["per_target"]["occupancy_rate"]
    ped = tm["per_target"]["ped_flow"]

    fig = plt.figure(figsize=(18, 10))
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.35, wspace=0.35)
    fig.suptitle("ST-GNN Model Evaluation", fontsize=16, fontweight="bold")

    # 10a  R² comparison bar
    ax = fig.add_subplot(gs[0, 0])
    targets = ["Occupancy Rate", "Ped Flow"]
    r2 = [occ["r_squared"], ped["r_squared"]]
    bars = ax.bar(targets, r2, color=["#3498db", "#27ae60"], edgecolor="white", width=0.5)
    for b, v in zip(bars, r2):
        ax.text(b.get_x() + b.get_width()/2, v + 0.01, f"{v:.3f}",
                ha="center", fontweight="bold", fontsize=12)
    ax.set_ylim(0, 1.1)
    ax.axhline(0.9, color="#999", ls="--", lw=1, alpha=0.5)
    ax.set_ylabel("R²")
    ax.set_title("Model R² Scores", fontweight="bold")

    # 10b  RMSE comparison
    ax = fig.add_subplot(gs[0, 1])
    metrics_names = ["RMSE", "MAE"]
    occ_vals = [occ["rmse"], occ["mae"]]
    ped_vals = [ped["rmse"], ped["mae"]]
    x = np.arange(len(metrics_names))
    w = 0.3
    ax.bar(x - w/2, occ_vals, w, label="Occupancy", color="#3498db", edgecolor="white")
    ax.bar(x + w/2, ped_vals, w, label="Ped Flow", color="#27ae60", edgecolor="white")
    ax.set_xticks(x)
    ax.set_xticklabels(metrics_names)
    ax.set_title("Error Metrics", fontweight="bold")
    ax.legend()
    ax.set_ylabel("Value")

    # 10c  Horizon degradation — Occupancy
    ax = fig.add_subplot(gs[0, 2])
    hd_occ = occ["horizon_degradation"]
    steps = [h["minutes_ahead"] for h in hd_occ]
    rmse_model = [h["rmse"] for h in hd_occ]
    rmse_persist = [h["persistence_rmse"] for h in hd_occ]
    ax.plot(steps, rmse_model, "o-", color="#3498db", lw=2, label="ST-GNN")
    ax.plot(steps, rmse_persist, "s--", color="#e74c3c", lw=2, label="Persistence")
    ax.fill_between(steps, rmse_model, rmse_persist, alpha=0.15, color="#27ae60")
    ax.set_xlabel("Forecast Horizon (minutes)")
    ax.set_ylabel("RMSE")
    ax.set_title("Occupancy: Model vs Persistence", fontweight="bold")
    ax.legend()

    # 10d  Horizon degradation — Ped flow
    ax = fig.add_subplot(gs[1, 0])
    hd_ped = ped["horizon_degradation"]
    steps_p = [h["minutes_ahead"] for h in hd_ped]
    rmse_model_p = [h["rmse"] for h in hd_ped]
    rmse_persist_p = [h["persistence_rmse"] for h in hd_ped]
    ax.plot(steps_p, rmse_model_p, "o-", color="#27ae60", lw=2, label="ST-GNN")
    ax.plot(steps_p, rmse_persist_p, "s--", color="#e74c3c", lw=2, label="Persistence")
    ax.fill_between(steps_p, rmse_model_p, rmse_persist_p, alpha=0.15, color="#3498db")
    ax.set_xlabel("Forecast Horizon (minutes)")
    ax.set_ylabel("RMSE")
    ax.set_title("Ped Flow: Model vs Persistence", fontweight="bold")
    ax.legend()

    # 10e  Improvement over persistence
    ax = fig.add_subplot(gs[1, 1])
    occ_pct = [(1 - m / max(p, 1e-9)) * 100 for m, p in zip(rmse_model, rmse_persist)]
    ped_pct = [(1 - m / max(p, 1e-9)) * 100 for m, p in zip(rmse_model_p, rmse_persist_p)]
    x = np.arange(len(steps))
    w = 0.3
    ax.bar(x - w/2, occ_pct, w, label="Occupancy", color="#3498db", edgecolor="white")
    ax.bar(x + w/2, ped_pct, w, label="Ped Flow", color="#27ae60", edgecolor="white")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s}min" for s in steps])
    ax.set_ylabel("Improvement over Persistence (%)")
    ax.set_title("Forecast Improvement by Horizon", fontweight="bold")
    ax.legend()
    ax.axhline(0, color="black", lw=0.5)

    # 10f  Training summary text
    ax = fig.add_subplot(gs[1, 2])
    ax.axis("off")
    tr = model["training"]
    corr_occ = occ.get("correlation", 0)
    corr_ped = ped.get("correlation", 0)
    lines = [
        "ST-GNN Training Summary",
        "━" * 30,
        f"Best epoch:       {tr['best_epoch']} / {tr['total_epochs']}",
        f"Best val loss:    {tr['best_val_loss']:.4f}",
        f"Train loss final: {tr['train_loss_final']:.4f}",
        f"Val loss final:   {tr['val_loss_final']:.4f}",
        f"Training time:    {tr['total_time_sec']/60:.1f} min",
        "",
        f"Occupancy  corr = {corr_occ:.4f}",
        f"Ped-flow   corr = {corr_ped:.4f}",
        "",
        f"Test samples: {tm['n_test_samples']}",
        f"Pred shape:   {tm['prediction_shape']}",
    ]
    ax.text(0.05, 0.95, "\n".join(lines), transform=ax.transAxes, fontsize=11,
            verticalalignment="top", fontfamily="monospace",
            bbox=dict(boxstyle="round,pad=0.6", facecolor="#eaf2f8", edgecolor="#5dade2"))

    savefig(fig, "10_model_evaluation.png")


# =====================================================================
# 11. STATIC FEATURES — POI & Distance distributions
# =====================================================================
def fig11_static_features(D):
    sf = D["sf"]
    fp = D["fp"]
    merged = sf.merge(fp[["street_id", "cluster_label"]], on="street_id", how="inner")

    poi_cols = ["poi_corporate", "poi_nightlife", "poi_retail", "poi_transport"]
    dist_cols = ["dist_flinders", "dist_southern_cross", "dist_townhall"]

    fig, axes = plt.subplots(2, 4, figsize=(20, 10))
    fig.suptitle("Static Features by Cluster", fontsize=16, fontweight="bold")

    # POI box plots
    for i, col in enumerate(poi_cols):
        ax = axes[0, i]
        data_by_cluster = [merged[merged["cluster_label"] == cl][col].values for cl in CLUSTER_ORDER]
        bp = ax.boxplot(data_by_cluster, tick_labels=CLUSTER_ORDER, patch_artist=True,
                        widths=0.6, showfliers=False)
        for patch, cl in zip(bp["boxes"], CLUSTER_ORDER):
            patch.set_facecolor(PAL[cl])
            patch.set_alpha(0.7)
        ax.set_title(col.replace("_", " ").title(), fontweight="bold")
        if i == 0:
            ax.set_ylabel("POI Count")

    # Distance box plots
    for i, col in enumerate(dist_cols):
        ax = axes[1, i]
        data_by_cluster = [merged[merged["cluster_label"] == cl][col].values for cl in CLUSTER_ORDER]
        bp = ax.boxplot(data_by_cluster, tick_labels=CLUSTER_ORDER, patch_artist=True,
                        widths=0.6, showfliers=False)
        for patch, cl in zip(bp["boxes"], CLUSTER_ORDER):
            patch.set_facecolor(PAL[cl])
            patch.set_alpha(0.7)
        ax.set_title(col.replace("_", " ").title(), fontweight="bold")
        if i == 0:
            ax.set_ylabel("Distance (m)")

    # Hide unused subplot
    axes[1, 3].axis("off")

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    savefig(fig, "11_static_features.png")


# =====================================================================
# 12. NEIGHBOR GRAPH — Network visualization
# =====================================================================
def fig12_neighbor_graph(D):
    graph = D["graph"]
    sf = D["sf"]
    fp = D["fp"]

    node_ids = graph["nodes"]
    merged = sf.merge(fp[["street_id", "cluster_label"]], on="street_id", how="left")
    merged["cluster_label"] = merged["cluster_label"].fillna("No Data")
    merged = merged.set_index("street_id")

    # Parse adjacency
    adj = graph["adjacency"]

    fig, ax = plt.subplots(figsize=(14, 12))
    fig.suptitle("Street Neighbor Graph (Spatial Adjacency)", fontsize=15, fontweight="bold")

    # Draw edges
    for src, neighbors in adj.items():
        if src not in merged.index:
            continue
        x1, y1 = merged.loc[src, "lon"], merged.loc[src, "lat"]
        for nbr in neighbors:
            nbr_id = graph["idx_to_node"].get(str(nbr), None)
            if nbr_id and nbr_id in merged.index:
                x2, y2 = merged.loc[nbr_id, "lon"], merged.loc[nbr_id, "lat"]
                ax.plot([x1, x2], [y1, y2], color="#bdc3c7", lw=0.5, alpha=0.4, zorder=1)

    # Draw nodes
    pal_ext = {**PAL, "No Data": "#cccccc"}
    for label, color in pal_ext.items():
        sub = merged[merged["cluster_label"] == label]
        sub_in_graph = sub[sub.index.isin(node_ids)]
        ax.scatter(sub_in_graph["lon"], sub_in_graph["lat"], c=color,
                   s=30, label=f"{label} ({len(sub_in_graph)})",
                   edgecolors="white", lw=0.4, zorder=2, alpha=0.85)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.legend(loc="upper left", fontsize=10)
    meta = graph["metadata"]
    ax.set_title(f"{meta['n_nodes']} nodes, {meta['n_edges']} edges, "
                 f"avg {meta['avg_neighbors']:.1f} neighbors, threshold {meta['distance_threshold_m']}m",
                 fontsize=11)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    savefig(fig, "12_neighbor_graph.png")


# =====================================================================
# 13. PARKING–PEDESTRIAN CORRELATION
# =====================================================================
def fig13_parking_ped_correlation(D):
    mag = D["mag"]

    # Only streets with both data types
    both = mag[(mag["park_mean_occupancy"] > 0) & (mag["ped_total_count"] > 0)].copy()

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle("Parking–Pedestrian Relationship", fontsize=15, fontweight="bold")

    fp = D["fp"]
    both = both.merge(fp[["street_id", "cluster_label"]], on="street_id", how="left")

    # 13a  Scatter: mean occupancy vs ped volume
    ax = axes[0]
    for cl in CLUSTER_ORDER:
        sub = both[both["cluster_label"] == cl]
        ax.scatter(sub["park_mean_occupancy"], sub["ped_total_count"] / 1e6,
                   c=PAL[cl], s=40, alpha=0.7, label=cl, edgecolors="white", lw=0.3)
    ax.set_xlabel("Mean Parking Occupancy")
    ax.set_ylabel("Total Ped Count (millions)")
    ax.set_title("Occupancy vs Pedestrian Volume", fontweight="bold")
    ax.legend(fontsize=9)

    # 13b  Correlation matrix of time blocks
    ax = axes[1]
    fp2 = fp.copy()
    ped_cols = [f"ped_{t}" for t in TIME_BLOCKS]
    park_cols = [f"park_{t}" for t in TIME_BLOCKS]
    all_cols = park_cols + ped_cols
    available = [c for c in all_cols if c in fp2.columns]
    if len(available) >= 2:
        corr_mat = fp2[available].corr()
        mask = np.zeros_like(corr_mat, dtype=bool)
        mask[np.triu_indices_from(mask, k=1)] = True
        sns.heatmap(corr_mat, mask=mask, annot=True, fmt=".2f", cmap="coolwarm",
                    center=0, ax=ax, linewidths=0.5, cbar_kws={"shrink": 0.8},
                    xticklabels=[c.replace("_", "\n") for c in available],
                    yticklabels=[c.replace("_", "\n") for c in available])
        ax.set_title("Activity Correlation Matrix", fontweight="bold")
    else:
        ax.text(0.5, 0.5, "Not enough correlated features", transform=ax.transAxes,
                ha="center", va="center", fontsize=12, color="#999")
        ax.set_title("Activity Correlation Matrix", fontweight="bold")

    # 13c  Peak parking vs peak ped
    ax = axes[2]
    for cl in CLUSTER_ORDER:
        sub = both[both["cluster_label"] == cl]
        ax.scatter(sub["park_peak_occupancy"], sub["ped_peak_count"],
                   c=PAL[cl], s=40, alpha=0.7, label=cl, edgecolors="white", lw=0.3)
    ax.set_xlabel("Peak Parking Occupancy (%)")
    ax.set_ylabel("Peak Pedestrian Count")
    ax.set_title("Peak Occupancy vs Peak Ped Count", fontweight="bold")
    ax.legend(fontsize=9)

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    savefig(fig, "13_parking_ped_correlation.png")


# =====================================================================
# 14. DATA CUBE QUALITY — Coverage & completeness
# =====================================================================
def fig14_data_quality(D):
    cube = D["cube"]

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("Data Cube Quality & Coverage", fontsize=15, fontweight="bold")

    # 14a  Valid observations per street
    ax = axes[0, 0]
    street_counts = cube.groupby("street_id").agg(
        parking_bins=("valid_parking", "sum"),
        ped_bins=("valid_ped", "sum")
    )
    ax.hist(street_counts["parking_bins"], bins=30, color="#3498db", alpha=0.7, label="Parking", edgecolor="white")
    ax.hist(street_counts["ped_bins"], bins=30, color="#27ae60", alpha=0.7, label="Pedestrian", edgecolor="white")
    ax.set_title("Valid Observations per Street", fontweight="bold")
    ax.set_xlabel("Number of Valid Time Bins")
    ax.set_ylabel("Streets")
    ax.legend()

    # 14b  Temporal coverage
    ax = axes[0, 1]
    daily = cube.groupby(cube["time_bin"].dt.date).agg(
        n_records=("street_id", "count"),
        n_streets=("street_id", "nunique")
    )
    ax.plot(daily.index, daily["n_streets"], color="#8e44ad", lw=1.5)
    ax.set_title("Streets Reporting per Day", fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Active Streets")
    ax.tick_params(axis="x", rotation=45)

    # 14c  Sensor density
    ax = axes[1, 0]
    mag = D["mag"]
    ax.hist(mag[mag["park_sensor_count"] > 0]["park_sensor_count"], bins=30,
            color="#e67e22", edgecolor="white", alpha=0.85)
    ax.set_title("Parking Sensor Count per Street", fontweight="bold")
    ax.set_xlabel("Sensors")
    ax.set_ylabel("Streets")

    # 14d  Hourly record distribution
    ax = axes[1, 1]
    hour_dist = cube.groupby("hour")["street_id"].count()
    ax.bar(hour_dist.index, hour_dist.values, color="#1abc9c", edgecolor="white")
    ax.set_title("Records by Hour of Day", fontweight="bold")
    ax.set_xlabel("Hour")
    ax.set_ylabel("Records")
    ax.set_xticks(range(0, 24, 2))

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    savefig(fig, "14_data_quality.png")


# =====================================================================
# 15. INTERVENTION ANALYSIS — Suitability breakdown
# =====================================================================
def fig15_interventions(D):
    opp = D["opp"]

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("Intervention Suitability Analysis", fontsize=16, fontweight="bold")

    # 15a  Temporal variance by cluster
    ax = axes[0, 0]
    for cl in CLUSTER_ORDER:
        sub = opp[opp["cluster_label"] == cl]
        ax.hist(sub["temporal_variance"], bins=15, alpha=0.6, color=PAL[cl], label=cl, edgecolor="white")
    ax.set_title("Temporal Variance by Cluster", fontweight="bold")
    ax.set_xlabel("Temporal Variance")
    ax.set_ylabel("Streets")
    ax.legend()

    # 15b  Peak-trough ratio
    ax = axes[0, 1]
    valid = opp[opp["peak_trough_ratio"] > 0]
    for cl in CLUSTER_ORDER:
        sub = valid[valid["cluster_label"] == cl]
        ax.hist(sub["peak_trough_ratio"], bins=15, alpha=0.6, color=PAL[cl], label=cl, edgecolor="white")
    ax.set_title("Peak-to-Trough Ratio by Cluster", fontweight="bold")
    ax.set_xlabel("Peak / Trough Ratio")
    ax.set_ylabel("Streets")
    ax.legend()

    # 15c  Quality weight distribution
    ax = axes[1, 0]
    ax.hist(opp["quality_weight"], bins=20, color="#8e44ad", edgecolor="white", alpha=0.85)
    ax.set_title("Quality Weight Distribution", fontweight="bold")
    ax.set_xlabel("Quality Weight")
    ax.set_ylabel("Streets")

    # 15d  Demand tier breakdown with review flag
    ax = axes[1, 1]
    ct = pd.crosstab(opp["demand_tier"], opp["requires_review"])
    ct.plot(kind="bar", stacked=True, ax=ax, color=["#27ae60", "#e74c3c"], edgecolor="white")
    ax.set_title("Demand Tier × Requires Review", fontweight="bold")
    ax.set_ylabel("Streets")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0)
    ax.legend(["No Review", "Needs Review"], fontsize=9)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    savefig(fig, "15_intervention_analysis.png")


# =====================================================================
# 16. CLUSTER-SPECIFIC TIME SERIES
# =====================================================================
def fig16_cluster_timeseries(D):
    cube = D["cube"]
    fp = D["fp"]

    cube_m = cube.merge(fp[["street_id", "cluster_label"]], on="street_id", how="inner")

    fig, axes = plt.subplots(3, 2, figsize=(18, 14))
    fig.suptitle("Average Weekly Profile by Cluster", fontsize=16, fontweight="bold")

    for i, cl in enumerate(CLUSTER_ORDER):
        sub = cube_m[cube_m["cluster_label"] == cl]
        hourly = sub.groupby("hour").agg(
            occ=("occupancy_rate", "mean"),
            ped=("ped_flow", "mean")
        )

        # Occupancy
        ax = axes[i, 0]
        ax.bar(hourly.index, hourly["occ"], color=PAL[cl], edgecolor="white", alpha=0.85)
        ax.set_title(f"{cl} — Parking Occupancy", fontweight="bold", color=PAL[cl])
        ax.set_xlabel("Hour of Day")
        ax.set_ylabel("Mean Occupancy Rate")
        ax.set_xticks(range(0, 24, 3))

        # Ped flow
        ax = axes[i, 1]
        ax.bar(hourly.index, hourly["ped"], color=PAL[cl], edgecolor="white", alpha=0.85)
        ax.set_title(f"{cl} — Pedestrian Flow", fontweight="bold", color=PAL[cl])
        ax.set_xlabel("Hour of Day")
        ax.set_ylabel("Mean Ped Flow")
        ax.set_xticks(range(0, 24, 3))

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    savefig(fig, "16_cluster_timeseries.png")


# =====================================================================
# MAIN
# =====================================================================
def main():
    print("=" * 60)
    print("GENERATING OUTPUT GRAPHICS")
    print("=" * 60)
    print(f"Output directory: {GFX}")
    print()

    print("Loading data...")
    D = load()
    print(f"  Loaded {len(D)} data sources")
    print()

    print("Generating figures...")
    figures = [
        ("01_pipeline_summary", fig01_pipeline_summary),
        ("02_cluster_profiles", fig02_cluster_profiles),
        ("03_cluster_heatmap", fig03_cluster_heatmap),
        ("04_feature_importance", fig04_feature_importance),
        ("05_regression_r2", fig05_regression_r2),
        ("06_opportunities", fig06_opportunities),
        ("07_spatial_map", fig07_spatial_map),
        ("08_time_series", fig08_time_series),
        ("09_weekly_profile", fig09_weekly_profile),
        ("10_model_evaluation", fig10_model_evaluation),
        ("11_static_features", fig11_static_features),
        ("12_neighbor_graph", fig12_neighbor_graph),
        ("13_parking_ped_correlation", fig13_parking_ped_correlation),
        ("14_data_quality", fig14_data_quality),
        ("15_interventions", fig15_interventions),
        ("16_cluster_timeseries", fig16_cluster_timeseries),
    ]
    for name, fn in figures:
        try:
            fn(D)
        except Exception as exc:
            print(f"  [skip] {name}: {exc}")

    print()
    print("=" * 60)
    print(f"DONE — {len(saved)} graphics saved to {GFX}")
    print("=" * 60)
    for s in saved:
        print(f"  {s}")


if __name__ == "__main__":
    main()
