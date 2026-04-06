"""
Step 04 — Spatial + semantic graph construction.

Street eligibility (both graphs):
  - str_type in {Arterial, Council Major} only
  - Segments named "Intersection of …" are excluded

Spatial graph:  k-NN on centroid distance.  Weight = exp(−d / σ).

Semantic graph: per-feature ratio matching on activity features.

  Logic:
  1. A street needs ≥ MIN_ACTIVE_FEATURES (3) non-zero activity features.
  2. A pair qualifies only if:
       a. They share ≥ MIN_SHARED_FEATURES (2) non-zero features
       b. Shared features cover ≥ MIN_SHARED_RATIO (60%) of the smaller
          street's active profile
       c. For EVERY shared feature d:
            ratio_d = min(log1p(Xi_d), log1p(Xj_d))
                    / max(log1p(Xi_d), log1p(Xj_d))
            ratio_d ≥ FEATURE_SIM_THRESHOLD (0.70)
          — this is a hard per-feature gate; one mismatched feature
            blocks the connection entirely
  3. Edge weight = mean ratio across shared features.

  Rationale for log1p ratios:
  • log1p(10)/log1p(5)  = 0.83 → close enough (2:1 raw)
  • log1p(20)/log1p(5)  = 0.73 → borderline (4:1 raw)
  • log1p(50)/log1p(5)  = 0.60 → rejected   (10:1 raw)
  • All-or-nothing per feature: streets with 1 café and 10 cafés are
    different places, even if everything else matches.

Outputs (all in data/processed/):
  node_index.parquet     — street_id → node_idx (0-based)
  spatial_edges.parquet  — node_i, node_j, dist_m, weight
  semantic_edges.parquet — node_i, node_j, similarity, weight
"""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.neighbors import NearestNeighbors

from config import PROCESSED_DIR, STREETS_GEOJSON

log = logging.getLogger(__name__)

# ── Street eligibility ───────────────────────────────────────────────────────
KEEP_STR_TYPES = {"Arterial", "Council Major"}

# ── Spatial graph ────────────────────────────────────────────────────────────
K_SPATIAL = 8

# ── Semantic graph ───────────────────────────────────────────────────────────
SEMANTIC_FEATURE_COLS = [
    "total_jobs", "cafe_count", "cafe_total_seats",
    "bar_count", "bar_patron_capacity",
    "business_count", "poi_total",
]
MIN_ACTIVE_FEATURES   = 3    # street must have ≥ 3 non-zero features to be eligible
MIN_SHARED_FEATURES   = 2    # pair must share ≥ 2 non-zero features
MIN_SHARED_RATIO      = 0.60 # shared must cover ≥ 60% of smaller street's active profile
FEATURE_SIM_THRESHOLD = 0.70 # per-feature log1p ratio — hard gate, ALL features must pass

# ── Viz cap ──────────────────────────────────────────────────────────────────
VIZ_SEMANTIC_TOP_N = 4_000

# ── Projection reference (Melbourne CBD) ────────────────────────────────────
_REF_LAT = -37.814
_REF_LON  = 144.963
_LAT_M    = 111_320.0
_LON_M    = 111_320.0 * np.cos(np.radians(_REF_LAT))


# ═══════════════════════════════════════════════════════════════════════════════
# Street filtering
# ═══════════════════════════════════════════════════════════════════════════════

def _load_eligible_street_ids() -> set:
    """Return street_ids that are Arterial/Council Major and not intersections."""
    gdf = gpd.read_file(STREETS_GEOJSON)[["street_id", "str_type", "name"]]
    gdf["street_id"] = gdf["street_id"].astype(str)

    keep = (
        gdf["str_type"].isin(KEEP_STR_TYPES) &
        ~gdf["name"].str.contains("Intersection", case=False, na=False)
    )
    ids = set(gdf.loc[keep, "street_id"])
    n_inter = gdf["name"].str.contains("Intersection", case=False, na=False).sum()
    log.info(
        f"  Street filter: {len(ids)} eligible "
        f"({gdf['str_type'].isin(KEEP_STR_TYPES).sum()} type-qualified, "
        f"{n_inter} intersection segments removed)"
    )
    return ids


# ═══════════════════════════════════════════════════════════════════════════════
# Node index
# ═══════════════════════════════════════════════════════════════════════════════

def _build_node_index(static: pd.DataFrame) -> pd.DataFrame:
    streets = sorted(static["street_id"].unique())
    node_idx = pd.DataFrame({
        "street_id": streets,
        "node_idx":  np.arange(len(streets), dtype=np.int32),
    })
    log.info(f"  Node index: {len(node_idx)} streets")
    return node_idx


# ═══════════════════════════════════════════════════════════════════════════════
# Spatial graph
# ═══════════════════════════════════════════════════════════════════════════════

def _build_spatial_graph(static: pd.DataFrame, node_idx: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Part A: Spatial graph (k={K_SPATIAL})")

    coords = static[["street_id", "centroid_lat", "centroid_lon"]].copy()
    coords["x_m"] = (coords["centroid_lon"] - _REF_LON) * _LON_M
    coords["y_m"] = (coords["centroid_lat"] - _REF_LAT) * _LAT_M
    xy = coords[["x_m", "y_m"]].values

    nbrs = NearestNeighbors(n_neighbors=K_SPATIAL + 1, algorithm="ball_tree", metric="euclidean")
    nbrs.fit(xy)
    distances, indices = nbrs.kneighbors(xy)
    distances = distances[:, 1:]
    indices   = indices[:, 1:]

    sigma = float(np.median(distances))
    log.info(f"  σ={sigma:.1f} m  max={distances.max():.1f} m")

    sid_to_node = dict(zip(node_idx["street_id"], node_idx["node_idx"]))
    street_ids  = coords["street_id"].values

    rows = []
    for i in range(len(street_ids)):
        ni = sid_to_node[street_ids[i]]
        for k in range(K_SPATIAL):
            j  = int(indices[i, k])
            nj = sid_to_node[street_ids[j]]
            d  = float(distances[i, k])
            w  = float(np.exp(-d / sigma))
            rows.append((ni, nj, d, w))
            rows.append((nj, ni, d, w))

    edges = pd.DataFrame(rows, columns=["node_i", "node_j", "dist_m", "weight"])
    edges = edges.drop_duplicates(subset=["node_i", "node_j"])
    log.info(f"  Spatial edges: {len(edges):,}")
    return edges


# ═══════════════════════════════════════════════════════════════════════════════
# Semantic graph
# ═══════════════════════════════════════════════════════════════════════════════

def _build_semantic_graph(static: pd.DataFrame, node_idx: pd.DataFrame) -> pd.DataFrame:
    log.info("Part B: Semantic graph (per-feature ratio matching)")
    log.info(f"  Features: {SEMANTIC_FEATURE_COLS}")
    log.info(
        f"  Rules: ≥{MIN_ACTIVE_FEATURES} active features, "
        f"≥{MIN_SHARED_FEATURES} shared (≥{MIN_SHARED_RATIO:.0%} of smaller profile), "
        f"ALL shared features must have log1p ratio ≥ {FEATURE_SIM_THRESHOLD}"
    )

    df = (
        static.set_index("street_id")
        .reindex(node_idx["street_id"])[SEMANTIC_FEATURE_COLS]
        .fillna(0.0)
    )
    X_raw = df.values.astype(np.float64)   # (N, F)
    X_log = np.log1p(X_raw)                # log1p-transformed values

    # ── Eligibility ───────────────────────────────────────────────────────────
    active_mask  = X_raw > 0
    active_count = active_mask.sum(axis=1)
    elig_idx     = np.where(active_count >= MIN_ACTIVE_FEATURES)[0]
    log.info(f"  Eligible: {len(elig_idx)} / {len(node_idx)} streets")

    if len(elig_idx) == 0:
        return pd.DataFrame(columns=["node_i", "node_j", "similarity", "weight"])

    X_elig      = X_log[elig_idx]         # (E, F) log1p values
    act_elig    = active_mask[elig_idx]   # (E, F) boolean
    act_cnt_el  = active_count[elig_idx]  # (E,)

    # Approximate cosine for candidate pre-filtering (avoid full O(E²) detail pass)
    norms      = np.linalg.norm(X_elig, axis=1, keepdims=True)
    norms      = np.where(norms == 0, 1e-8, norms)
    X_norm     = X_elig / norms
    sim_approx = (X_norm @ X_norm.T).astype(np.float32)
    np.fill_diagonal(sim_approx, -1.0)

    sid_to_node = dict(zip(node_idx["street_id"], node_idx["node_idx"]))
    all_sids    = node_idx["street_id"].values

    rows  = []
    added = set()
    stats = {"cand": 0, "pass_shared": 0, "pass_ratio": 0}

    for i in range(len(elig_idx)):
        ni       = sid_to_node[all_sids[elig_idx[i]]]
        top_cand = np.argsort(sim_approx[i])[-20:][::-1]

        for j in top_cand:
            if i == j:
                continue
            stats["cand"] += 1

            # ── Gate 1: shared non-zero features ─────────────────────────────
            shared    = act_elig[i] & act_elig[j]        # features non-zero in BOTH
            n_shared  = int(shared.sum())
            if n_shared < MIN_SHARED_FEATURES:
                continue
            min_act = int(min(act_cnt_el[i], act_cnt_el[j]))
            if n_shared < MIN_SHARED_RATIO * min_act:
                continue
            stats["pass_shared"] += 1

            # ── Gate 2: per-feature log1p ratio — ALL shared dims must pass ──
            # For each shared feature d:
            #   ratio = min(log1p(a), log1p(b)) / max(log1p(a), log1p(b))
            # Both values are already log1p-transformed in X_elig.
            xi_sh = X_elig[i][shared]
            xj_sh = X_elig[j][shared]

            lo = np.minimum(xi_sh, xj_sh)
            hi = np.maximum(xi_sh, xj_sh)
            hi = np.where(hi < 1e-9, 1e-9, hi)
            ratios = lo / hi                  # per-feature similarity in [0, 1]

            if (ratios < FEATURE_SIM_THRESHOLD).any():
                continue                      # one bad feature → reject pair
            stats["pass_ratio"] += 1

            # Edge weight = mean ratio across shared features
            weight = float(ratios.mean())

            nj   = sid_to_node[all_sids[elig_idx[j]]]
            pair = (min(ni, nj), max(ni, nj))
            if pair not in added:
                added.add(pair)
                rows.append((ni, nj, weight, weight))
                rows.append((nj, ni, weight, weight))

    log.info(
        f"  Candidates: {stats['cand']:,} → "
        f"passed shared: {stats['pass_shared']:,} → "
        f"passed per-feature ratio: {stats['pass_ratio']:,}"
    )

    edges = pd.DataFrame(rows, columns=["node_i", "node_j", "similarity", "weight"])
    edges = edges.drop_duplicates(subset=["node_i", "node_j"])
    log.info(f"  Semantic edges: {len(edges):,} (bidirectional)")
    if len(edges):
        log.info(f"  Weight range: [{edges['weight'].min():.3f}, {edges['weight'].max():.3f}]")
    return edges


# ═══════════════════════════════════════════════════════════════════════════════
# Graph viz export
# ═══════════════════════════════════════════════════════════════════════════════

def _export_graph_viz(static, node_idx, spatial_edges, semantic_edges) -> Path:
    import json

    coords_lookup = dict(zip(
        node_idx["node_idx"],
        zip(static["centroid_lon"].values, static["centroid_lat"].values),
    ))

    # Track which nodes have semantic edges
    sem_nodes = set(semantic_edges["node_i"].unique()) | set(semantic_edges["node_j"].unique())

    # Build street_id lookup for node info
    node_to_sid = dict(zip(node_idx["node_idx"], node_idx["street_id"]))
    sid_to_static = static.set_index("street_id")

    features = []

    # ── Node points ───────────────────────────────────────────────────────────
    for nidx, coords in coords_lookup.items():
        sid = node_to_sid.get(nidx, "")
        semantic_eligible = nidx in sem_nodes
        props = {
            "feature_type":       "node",
            "node_idx":           int(nidx),
            "street_id":          str(sid),
            "semantic_eligible":  semantic_eligible,
        }
        # Attach street name if available
        if sid in sid_to_static.index and "name" in sid_to_static.columns:
            props["name"] = str(sid_to_static.at[sid, "name"])
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": list(coords)},
            "properties": props,
        })

    # ── Spatial edges ─────────────────────────────────────────────────────────
    sp_uniq = spatial_edges[spatial_edges["node_i"] < spatial_edges["node_j"]]
    for _, row in sp_uniq.iterrows():
        a = coords_lookup.get(int(row["node_i"]))
        b = coords_lookup.get(int(row["node_j"]))
        if a is None or b is None:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": [list(a), list(b)]},
            "properties": {
                "feature_type": "edge",
                "edge_type": "spatial",
                "weight":    round(float(row["weight"]), 4),
                "dist_m":    round(float(row["dist_m"]), 1),
            },
        })

    # ── Semantic edges ────────────────────────────────────────────────────────
    se_uniq = semantic_edges[semantic_edges["node_i"] < semantic_edges["node_j"]]
    se_uniq = se_uniq.nlargest(VIZ_SEMANTIC_TOP_N, "similarity")
    for _, row in se_uniq.iterrows():
        a = coords_lookup.get(int(row["node_i"]))
        b = coords_lookup.get(int(row["node_j"]))
        if a is None or b is None:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": [list(a), list(b)]},
            "properties": {
                "feature_type": "edge",
                "edge_type":  "semantic",
                "weight":     round(float(row["weight"]), 4),
                "similarity": round(float(row["similarity"]), 4),
            },
        })

    path = PROCESSED_DIR / "graph_viz.geojson"
    with open(path, "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)
    log.info(
        f"  graph_viz.geojson: {len(node_idx)} nodes + "
        f"{len(sp_uniq):,} spatial + {len(se_uniq):,} semantic edges"
    )
    return path


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════

def _validate(node_idx, spatial_edges, semantic_edges):
    N = len(node_idx)
    assert set(spatial_edges["node_i"].unique()) == set(range(N)), \
        "Spatial graph: some nodes have no outgoing edges"
    sem_isolated = N - len(semantic_edges["node_i"].unique())
    if sem_isolated:
        log.info(f"  {sem_isolated} nodes have no semantic edges "
                 "(low-activity or ineligible streets — expected)")
    assert (spatial_edges["weight"]  > 0).all(), "Zero/negative spatial weights"
    if len(semantic_edges):
        assert (semantic_edges["weight"] > 0).all(), "Zero/negative semantic weights"
    assert (spatial_edges["node_i"]  != spatial_edges["node_j"]).all(), "Spatial self-loops"
    if len(semantic_edges):
        assert (semantic_edges["node_i"] != semantic_edges["node_j"]).all(), "Semantic self-loops"
    log.info(f"  Validation OK — {N} nodes | "
             f"{len(spatial_edges):,} spatial | {len(semantic_edges):,} semantic edges")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def run() -> dict[str, Path]:
    static = pd.read_parquet(PROCESSED_DIR / "static_features.parquet")
    static["street_id"] = static["street_id"].astype(str)

    # Filter to Arterial + Council Major, non-intersection streets only
    eligible_ids = _load_eligible_street_ids()
    static = static[static["street_id"].isin(eligible_ids)].reset_index(drop=True)
    log.info(f"  static after filter: {len(static)} streets")

    node_idx       = _build_node_index(static)
    spatial_edges  = _build_spatial_graph(static, node_idx)
    semantic_edges = _build_semantic_graph(static, node_idx)

    _validate(node_idx, spatial_edges, semantic_edges)

    ni_path = PROCESSED_DIR / "node_index.parquet"
    sp_path = PROCESSED_DIR / "spatial_edges.parquet"
    se_path = PROCESSED_DIR / "semantic_edges.parquet"
    node_idx.to_parquet(ni_path, index=False)
    spatial_edges.to_parquet(sp_path, index=False)
    semantic_edges.to_parquet(se_path, index=False)

    gv_path = _export_graph_viz(static, node_idx, spatial_edges, semantic_edges)

    log.info(
        f"Step 4 complete: {len(node_idx)} nodes | "
        f"{len(spatial_edges):,} spatial | {len(semantic_edges):,} semantic edges"
    )
    return {
        "node_index": ni_path, "spatial_edges": sp_path,
        "semantic_edges": se_path, "graph_viz": gv_path,
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
