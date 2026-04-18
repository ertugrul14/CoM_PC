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
import json
import logging
from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.neighbors import NearestNeighbors

from config import PROCESSED_DIR, RAW_DIR, STREETS_GEOJSON

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
# Static feature enrichment (betweenness centrality + transit proximity)
# ═══════════════════════════════════════════════════════════════════════════════

# Melbourne LGA bounding box for tram stop filtering
_MELB_LON_MIN, _MELB_LON_MAX = 144.88, 145.00
_MELB_LAT_MIN, _MELB_LAT_MAX = -37.86, -37.77


def _haversine_batch(lat1: float, lon1: float,
                     lats2: np.ndarray, lons2: np.ndarray) -> np.ndarray:
    """Vectorised haversine distance (metres) from one point to an array of points."""
    R = 6_371_000.0
    φ1  = np.radians(lat1)
    φ2  = np.radians(lats2)
    dφ  = φ2 - φ1
    dλ  = np.radians(lons2 - lon1)
    a   = np.sin(dφ / 2) ** 2 + np.cos(φ1) * np.cos(φ2) * np.sin(dλ / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))


def _enrich_static_features(
    node_idx: pd.DataFrame,
    spatial_edges: pd.DataFrame,
) -> None:
    """
    Compute graph and transit features and append them to static_features.parquet.

    Called at the end of step_04's run() so all graph artefacts are available.

    New columns added
    -----------------
    betweenness_centrality : float  Normalised betweenness on the spatial graph.
                                    High values = topologically central streets
                                    that sit on many shortest paths — strong
                                    proxy for foot-traffic corridors.
    nearest_tram_stop_m    : float  Distance (m) to the nearest tram stop in
                                    the Melbourne area (MODE == "TRAM" in PTV data).
    tram_stops_200m        : int    Count of tram stops within a 200 m radius.
    nearest_bus_stop_m     : float  Distance (m) to the nearest Melbourne City
                                    bus stop sign.
    bus_stop_on_street     : int    1 if this street has a bus stop sign
                                    (roadseg_id direct match), else 0.
    """
    static = pd.read_parquet(PROCESSED_DIR / "static_features.parquet")
    static["street_id"] = static["street_id"].astype(str)

    # ── 1. Betweenness centrality ─────────────────────────────────────────────
    log.info("  Computing betweenness centrality on spatial graph...")
    G = nx.Graph()
    for _, row in spatial_edges.iterrows():
        G.add_edge(int(row["node_i"]), int(row["node_j"]),
                   weight=float(row["dist_m"]))

    bc = nx.betweenness_centrality(G, normalized=True, weight="weight")
    idx_to_sid = dict(zip(
        node_idx["node_idx"].astype(int),
        node_idx["street_id"].astype(str),
    ))
    bc_df = pd.DataFrame([
        {"street_id": idx_to_sid[ni], "betweenness_centrality": val}
        for ni, val in bc.items() if ni in idx_to_sid
    ])
    static = static.merge(bc_df, on="street_id", how="left")
    static["betweenness_centrality"] = static["betweenness_centrality"].fillna(0.0)
    log.info(f"  Betweenness centrality: max={static['betweenness_centrality'].max():.4f}")

    # Street centroid arrays for vectorised distance computation
    lats = static["centroid_lat"].values
    lons = static["centroid_lon"].values

    # ── 2. Tram stop proximity (PTV GeoJSON) ──────────────────────────────────
    ptv_path = RAW_DIR / "ptv_stops.geojson"
    if ptv_path.exists():
        log.info("  Loading PTV stops for tram proximity...")
        with open(ptv_path, encoding="utf-8") as f:
            ptv_data = json.load(f)

        # Filter to tram stops within Melbourne LGA bbox
        tram_coords = []
        for feat in ptv_data["features"]:
            if feat["properties"].get("MODE", "").upper() != "TRAM":
                continue
            lon, lat = feat["geometry"]["coordinates"]
            if (_MELB_LON_MIN <= lon <= _MELB_LON_MAX and
                    _MELB_LAT_MIN <= lat <= _MELB_LAT_MAX):
                tram_coords.append([lat, lon])

        log.info(f"  Tram stops in Melbourne bbox: {len(tram_coords)}")

        if tram_coords:
            tram_arr = np.array(tram_coords)   # [M, 2]  lat, lon
            nearest_tram = np.array([
                float(_haversine_batch(lat, lon, tram_arr[:, 0], tram_arr[:, 1]).min())
                for lat, lon in zip(lats, lons)
            ])
            tram_200m = np.array([
                int((_haversine_batch(lat, lon, tram_arr[:, 0], tram_arr[:, 1]) <= 200).sum())
                for lat, lon in zip(lats, lons)
            ])
        else:
            log.warning("  No tram stops found in bbox — defaulting to 9999 m")
            nearest_tram = np.full(len(static), 9999.0)
            tram_200m    = np.zeros(len(static), dtype=int)

        static["nearest_tram_stop_m"] = nearest_tram
        static["tram_stops_200m"]     = tram_200m
    else:
        log.warning("  ptv_stops.geojson not found — tram proximity set to 9999 m / 0")
        static["nearest_tram_stop_m"] = 9999.0
        static["tram_stops_200m"]     = 0

    # ── 3. Bus stop proximity (Melbourne City open data) ─────────────────────
    bus_path = RAW_DIR / "bus_stops.parquet"
    if bus_path.exists():
        log.info("  Loading Melbourne bus stops...")
        bus_df = pd.read_parquet(bus_path)
        bus_df["roadseg_id"] = bus_df["roadseg_id"].astype(str)

        # Direct match: count stops per street via roadseg_id
        on_street = set(bus_df["roadseg_id"].unique())
        static["bus_stop_on_street"] = static["street_id"].isin(on_street).astype(int)

        # Nearest bus stop (spatial) for all streets
        bus_arr = bus_df[["lat", "lon"]].dropna().values  # [K, 2]
        if len(bus_arr):
            nearest_bus = np.array([
                float(_haversine_batch(lat, lon, bus_arr[:, 0], bus_arr[:, 1]).min())
                for lat, lon in zip(lats, lons)
            ])
        else:
            nearest_bus = np.full(len(static), 9999.0)

        static["nearest_bus_stop_m"] = nearest_bus
        n_matched = static["bus_stop_on_street"].sum()
        log.info(f"  Bus stops: {n_matched} streets have a direct stop match")
    else:
        log.warning("  bus_stops.parquet not found — bus proximity set to 9999 m / 0")
        static["bus_stop_on_street"] = 0
        static["nearest_bus_stop_m"] = 9999.0

    # ── Save enriched static features ────────────────────────────────────────
    static.to_parquet(PROCESSED_DIR / "static_features.parquet", index=False)
    new_cols = [
        "betweenness_centrality", "nearest_tram_stop_m", "tram_stops_200m",
        "bus_stop_on_street", "nearest_bus_stop_m",
    ]
    log.info(
        f"  static_features enriched: {len(static)} streets × "
        f"{len(static.columns)} cols  (+{len(new_cols)} new: {new_cols})"
    )


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

    # Enrich static_features with betweenness centrality + transit proximity
    log.info("  Enriching static_features with graph + transit features...")
    _enrich_static_features(node_idx, spatial_edges)

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
