"""
Step 04 — Spatial + semantic graph construction.

Street eligibility (both graphs):
  - str_type in {Arterial, Council Major} only
  - Segments named "Intersection of …" are excluded

Spatial graph:  Intersection-topology edges.  Streets whose polygon
  boundaries physically touch (share at least a point) are connected —
  i.e. they meet at a real intersection.  Edge weight = exp(−d / σ) where
  d = centroid distance and σ = median centroid distance across all
  intersection edges (Gaussian kernel, topology-constrained).

  Fallback: 25 streets in the eligible set are geometrically isolated
  (no touching eligible neighbour).  Each isolate is connected to its
  single nearest centroid neighbour (k=1 NN) with weight 0.0 so the
  GCN receives a valid adjacency for every node.  These edges carry zero
  weight and are flagged with dist_m > 0 so downstream code can filter
  them if needed.

Semantic graph: cosine similarity on z-scored log1p features + mutual k-NN.

  Logic:
  1. Apply log1p to each feature, then z-score across streets (zero mean,
     unit variance). This normalises scale differences (jobs vs floorspace)
     so cosine similarity is not dominated by high-magnitude features.
  2. Compute pairwise cosine similarity between all 1,397 street vectors.
  3. For each street, keep the top-K (K_SEMANTIC) most similar neighbours
     by cosine score above COS_THRESHOLD.
  4. Mutual k-NN: only retain edges where BOTH streets are in each other's
     top-K list. This symmetry constraint prevents hub nodes from pulling
     in many dissimilar neighbours.
  5. Edge weight = cosine similarity value.

  Rationale vs. old log1p-ratio approach:
  • Old approach used a hard per-feature gate (ALL shared dims must pass
    ratio ≥ 0.70), which blocked any edge if one feature differed — causing
    77% isolates. Cosine is a holistic comparison; one noisy feature cannot
    veto the whole connection.
  • Mutual k-NN avoids the hub-and-spoke degeneration common in one-sided
    k-NN graphs, while keeping the graph sparse and interpretable.

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
from shapely.strtree import STRtree
from sklearn.neighbors import NearestNeighbors

from config import PROCESSED_DIR, RAW_DIR, STREETS_GEOJSON

log = logging.getLogger(__name__)

# ── Street eligibility ───────────────────────────────────────────────────────
KEEP_STR_TYPES = {"Arterial", "Council Major"}

# ── Spatial graph ────────────────────────────────────────────────────────────
# Intersection-topology: streets whose polygons touch get an edge.
# Isolates (no touching neighbour) fall back to k=1 nearest centroid.
K_SPATIAL_FALLBACK = 1

# ── Semantic graph ───────────────────────────────────────────────────────────
# Tier-A features: CV > 1, zero_pct < 60%, meaningful urban-activity signal.
# Excludes: centroid_lat/lon (spatial, not semantic), poi_total (derived sum),
#           cafe_*/bar_*/dining_capacity (sparse, low CV relative to zero_pct).
SEMANTIC_FEATURE_COLS = [
    "total_jobs", "jobs_office", "jobs_retail",
    "retail_floorspace", "office_floorspace",
    "dwelling_count", "dwellings_apartment",
    "business_count",
    "floorspace_residential_apt", "floorspace_entertainment",
    "offstreet_spaces",
    "floors_mean", "area_m2", "building_count",
    "tram_stops_300m", "cbd_distance_m",
]
K_SEMANTIC    = 10    # mutual k-NN neighbourhood size
COS_THRESHOLD = 0.85  # minimum cosine similarity to form an edge

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
    """
    Build intersection-topology spatial graph.

    Primary edges: street polygon pairs whose boundaries touch (share ≥ 1 point).
    Edge weight = exp(−d / σ) where d = centroid distance and σ = median distance
    across all intersection edges (Gaussian kernel, topology-constrained).

    Fallback edges: isolates (no touching neighbour) receive one k=1 NN edge
    with weight=0.0 so every node has at least one outgoing edge.

    Inputs:  static with centroid_lat/lon; node_idx mapping street_id → node_idx.
    Outputs: DataFrame [node_i, node_j, dist_m, weight] — bidirectional.
    Assumes: STREETS_GEOJSON contains MultiPolygon street surfaces in EPSG:4326.
    """
    log.info("Part A: Spatial graph (intersection-topology)")

    # ── Load street polygons for the eligible subset ──────────────────────────
    gdf = gpd.read_file(STREETS_GEOJSON)[["street_id", "str_type", "name", "geometry"]]
    gdf["street_id"] = gdf["street_id"].astype(str)
    keep_sids = set(node_idx["street_id"])
    gdf = gdf[gdf["street_id"].isin(keep_sids)].reset_index(drop=True)

    # Align gdf row order to node_idx order so position i == node_idx i
    sid_to_node = dict(zip(node_idx["street_id"], node_idx["node_idx"]))
    node_to_sid = dict(zip(node_idx["node_idx"], node_idx["street_id"]))
    gdf = gdf.set_index("street_id").reindex(node_idx["street_id"]).reset_index()
    N = len(gdf)

    # ── STRtree touch query ───────────────────────────────────────────────────
    tree = STRtree(gdf.geometry)
    geoms = gdf.geometry.values

    touch_pairs: set[tuple[int, int]] = set()

    for i in range(N):
        candidates = tree.query(geoms[i], predicate="touches")
        for j in candidates:
            if i == j:
                continue
            touch_pairs.add((min(i, j), max(i, j)))

    log.info(f"  Intersection edges (undirected): {len(touch_pairs):,}")

    # ── Centroid coords for dist_m + fallback ─────────────────────────────────
    coords = static[["street_id", "centroid_lat", "centroid_lon"]].copy()
    coords = coords.set_index("street_id").reindex(node_idx["street_id"]).reset_index()
    coords["x_m"] = (coords["centroid_lon"] - _REF_LON) * _LON_M
    coords["y_m"] = (coords["centroid_lat"] - _REF_LAT) * _LAT_M
    xy = coords[["x_m", "y_m"]].values

    # Centroid distance lookup (for dist_m column)
    def _cdist(i: int, j: int) -> float:
        dx = xy[i, 0] - xy[j, 0]
        dy = xy[i, 1] - xy[j, 1]
        return float(np.sqrt(dx * dx + dy * dy))

    # Gaussian kernel weights: σ = median centroid distance across intersection edges
    if touch_pairs:
        pair_dists = np.array([_cdist(i, j) for i, j in touch_pairs])
        sigma = float(np.median(pair_dists))
    else:
        sigma = 1.0
    log.info(f"  σ={sigma:.1f} m  (median intersection centroid distance)")

    rows: list[tuple] = []
    connected: set[int] = set()

    for (i, j) in touch_pairs:
        d = _cdist(i, j)
        w = float(np.exp(-d / sigma))
        rows.append((i, j, d, w))
        rows.append((j, i, d, w))
        connected.add(i)
        connected.add(j)

    # ── Fallback: nearest-centroid edge for isolates ──────────────────────────
    isolates = [i for i in range(N) if i not in connected]
    if isolates:
        log.info(f"  Isolates needing fallback: {len(isolates)} — adding k=1 NN edges")
        nbrs = NearestNeighbors(n_neighbors=2, algorithm="ball_tree", metric="euclidean")
        nbrs.fit(xy)
        for i in isolates:
            distances_i, indices_i = nbrs.kneighbors(xy[i:i+1])
            j = int(indices_i[0, 1])   # index 0 is self
            d = float(distances_i[0, 1])
            rows.append((i, j, d, 0.0))
            rows.append((j, i, d, 0.0))

    edges = pd.DataFrame(rows, columns=["node_i", "node_j", "dist_m", "weight"])
    edges = edges.drop_duplicates(subset=["node_i", "node_j"])
    log.info(f"  Spatial edges total (bidirectional): {len(edges):,}")
    return edges


# ═══════════════════════════════════════════════════════════════════════════════
# Semantic graph
# ═══════════════════════════════════════════════════════════════════════════════

def _build_semantic_graph(static: pd.DataFrame, node_idx: pd.DataFrame) -> pd.DataFrame:
    """
    Build semantic graph via cosine similarity on z-scored log1p features + mutual k-NN.

    Inputs:  static DataFrame aligned to node_idx row order.
    Outputs: DataFrame with columns [node_i, node_j, similarity, weight] (bidirectional).
    Assumes: SEMANTIC_FEATURE_COLS are all present in static; NaNs filled to 0.
    """
    log.info("Part B: Semantic graph (cosine + mutual k-NN)")
    log.info(f"  Features ({len(SEMANTIC_FEATURE_COLS)}): {SEMANTIC_FEATURE_COLS}")
    log.info(f"  K_SEMANTIC={K_SEMANTIC}  COS_THRESHOLD={COS_THRESHOLD}")

    df = (
        static.set_index("street_id")
        .reindex(node_idx["street_id"])[SEMANTIC_FEATURE_COLS]
        .fillna(0.0)
    )
    N = len(node_idx)
    X_raw = df.values.astype(np.float64)   # (N, F)

    # ── 1. log1p transform then z-score per feature ───────────────────────────
    X_log = np.log1p(X_raw)
    col_mean = X_log.mean(axis=0)
    col_std  = X_log.std(axis=0)
    col_std  = np.where(col_std < 1e-9, 1.0, col_std)  # avoid divide-by-zero on constant cols
    X_z = (X_log - col_mean) / col_std                  # (N, F) z-scored

    # ── 2. L2-normalise rows for cosine via dot product ───────────────────────
    norms = np.linalg.norm(X_z, axis=1, keepdims=True)
    norms = np.where(norms < 1e-9, 1.0, norms)
    X_norm = X_z / norms                                 # (N, F)

    # ── 3. Full cosine similarity matrix (float32 to save memory) ────────────
    # N=1,397 → 1,397² ≈ 1.95 M entries; float32 ≈ 7.8 MB — fine in memory.
    sim = (X_norm @ X_norm.T).astype(np.float32)
    np.fill_diagonal(sim, -2.0)                          # exclude self

    # ── 4. Build per-street top-K neighbour sets (one-sided k-NN) ────────────
    # argsort ascending → last K entries are highest similarity
    top_k = np.argsort(sim, axis=1)[:, -K_SEMANTIC:]    # (N, K)

    # ── 5. Mutual k-NN: edge exists iff i in top_k[j] AND j in top_k[i] ─────
    # Build adjacency as set for O(1) lookup
    neighbour_sets: list[set] = [set(top_k[i].tolist()) for i in range(N)]

    rows: list[tuple] = []
    added: set        = set()

    for i in range(N):
        for j in neighbour_sets[i]:
            if i not in neighbour_sets[j]:
                continue                                  # not mutual — skip
            cos = float(sim[i, j])
            if cos < COS_THRESHOLD:
                continue
            pair = (min(i, j), max(i, j))
            if pair in added:
                continue
            added.add(pair)
            rows.append((i, j, cos, cos))
            rows.append((j, i, cos, cos))

    edges = pd.DataFrame(rows, columns=["node_i", "node_j", "similarity", "weight"])

    # ── 6. Diagnostics ────────────────────────────────────────────────────────
    n_connected = len(set(edges["node_i"].unique()))
    n_isolated  = N - n_connected
    log.info(f"  Semantic edges: {len(edges):,} (bidirectional)")
    log.info(f"  Connected nodes: {n_connected} / {N} ({n_isolated} isolates)")
    if len(edges):
        log.info(
            f"  Cosine range: [{edges['similarity'].min():.3f}, "
            f"{edges['similarity'].max():.3f}]  mean={edges['similarity'].mean():.3f}"
        )
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

    # ── Spatial edges (exclude weight=0.0 fallback edges — viz only) ──────────
    sp_uniq = spatial_edges[
        (spatial_edges["node_i"] < spatial_edges["node_j"]) &
        (spatial_edges["weight"] > 0)
    ]
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
    # Fallback isolate edges carry weight=0.0 — check only for negative weights
    assert (spatial_edges["weight"]  >= 0).all(), "Negative spatial weights"
    n_fallback = (spatial_edges["weight"] == 0.0).sum() // 2
    if n_fallback:
        log.info(f"  {n_fallback} isolate fallback edge(s) with weight=0.0")
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
    bc_map = {idx_to_sid[ni]: val for ni, val in bc.items() if ni in idx_to_sid}
    static["betweenness_centrality"] = static["street_id"].map(bc_map).fillna(0.0)
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
