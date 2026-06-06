"""
Step 04 (v2) — Fresh sensor-union graphs.

EXPERIMENT VARIANT (steps_v2): instead of inducing a subgraph from the full
1,397-street intersection-topology graph (which leaves sparse sensors isolated),
this builds BRAND-NEW spatial + semantic graphs directly on the real-sensor
union (ped sensors u parking sensors = 189 streets). Every node is guaranteed a
connected neighbourhood, so the comparison against ExpA/ExpB is fair.

Node set
  189 streets = pedestrian-sensor streets (74) u parking-sensor streets (143),
  restricted to streets present in the modelled cube (node_index.parquet).
  Ordered by their original full-cube node_idx for determinism.

Spatial graph
  k-NN (K_SPATIAL) by centroid distance in EPSG:3111 metres, Gaussian-weighted
  exp(-d / sigma) with sigma = median 1-NN distance. Edges stored both directions.
  NOTE: these edges are LESS spatially faithful than the full-graph intersection
  topology - an edge may connect two sensors with non-sensor streets between them.

Semantic graph
  Mutual k-NN (K_SEMANTIC) on cosine similarity of z-scored log1p land-use vectors
  (same 9 static features as the production semantic graph). Mutual constraint
  prevents hub-and-spoke degeneration.

Outputs (data/processed_v2/):
  node_index.parquet     - street_id -> node_idx (0..188), + is_ped_sensor / is_park_sensor
  spatial_edges.parquet  - node_i, node_j, dist_m, weight
  semantic_edges.parquet - node_i, node_j, similarity, weight

Run:
  cd melbourne_pipeline && python -m steps_v2.step_04_graph
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.neighbors import NearestNeighbors

from config import PROCESSED_DIR

log = logging.getLogger(__name__)

V2_DIR = PROCESSED_DIR.parent / "processed_v2"

# Graph hyperparameters
K_SPATIAL  = 6
K_SEMANTIC = 6

# Same 9 land-use features as the production semantic graph (step_08 STATIC_COLS)
STATIC_COLS = [
    "total_jobs", "cafe_count", "cafe_total_seats",
    "bar_count", "bar_patron_capacity", "business_count",
    "poi_total", "dining_capacity", "area_m2",
]


def _sensor_union() -> tuple[pd.DataFrame, set, set]:
    """
    Return (ordered node_index df for the union, ped_sids, park_sids).

    Union = ped u park sensor streets present in the full cube, ordered by their
    original full-cube node_idx so the slice-and-map in step_08 (v2) is stable.
    """
    ni = pd.read_parquet(PROCESSED_DIR / "node_index.parquet")
    ni["street_id"] = ni["street_id"].astype(str)
    cube_ids = set(ni["street_id"])

    sm = pd.read_parquet(PROCESSED_DIR / "sensor_map.parquet")
    sm["street_id"] = sm["street_id"].astype(str)

    def ids(stype: str) -> set:
        s = sm[sm["sensor_type"] == stype]["street_id"].dropna().unique()
        return set(s) & cube_ids

    ped_sids  = ids("pedestrian")
    park_sids = ids("parking")
    union     = ped_sids | park_sids

    sub = ni[ni["street_id"].isin(union)].sort_values("node_idx").reset_index(drop=True)
    sub = sub[["street_id"]].copy()
    sub["node_idx"]       = np.arange(len(sub), dtype=int)   # v2 0-based index
    sub["is_ped_sensor"]  = sub["street_id"].isin(ped_sids)
    sub["is_park_sensor"] = sub["street_id"].isin(park_sids)
    log.info(f"  Sensor union: {len(sub)} streets "
             f"(ped {len(ped_sids)}, park {len(park_sids)}, both {len(ped_sids & park_sids)})")
    return sub, ped_sids, park_sids


def _centroids_3111(street_ids: list[str]) -> np.ndarray:
    """Project static-feature centroid lat/lon to EPSG:3111 metres. Returns [N,2]."""
    static = pd.read_parquet(PROCESSED_DIR / "static_features.parquet")
    static["street_id"] = static["street_id"].astype(str)
    static = static.set_index("street_id").reindex(street_ids).reset_index()
    pts = gpd.GeoSeries(
        gpd.points_from_xy(static["centroid_lon"], static["centroid_lat"]),
        crs="EPSG:4326",
    ).to_crs("EPSG:3111")
    return np.c_[pts.x.values, pts.y.values].astype(np.float64)


def _spatial_knn(xy: np.ndarray, k: int) -> pd.DataFrame:
    """k-NN spatial edges, Gaussian-weighted, stored both directions."""
    n = len(xy)
    k = min(k, n - 1)
    nn = NearestNeighbors(n_neighbors=k + 1).fit(xy)   # +1 for self
    dist, idx = nn.kneighbors(xy)
    sigma = float(np.median(dist[:, 1]))               # median 1-NN distance
    if sigma <= 0:
        sigma = 1.0

    rows = []
    for i in range(n):
        for col in range(1, k + 1):                    # skip self at col 0
            j = int(idx[i, col]); d = float(dist[i, col])
            w = float(np.exp(-d / sigma))
            rows.append((i, j, d, w))
            rows.append((j, i, d, w))                   # symmetric
    df = pd.DataFrame(rows, columns=["node_i", "node_j", "dist_m", "weight"])
    df = df.drop_duplicates(subset=["node_i", "node_j"]).reset_index(drop=True)
    log.info(f"  Spatial k-NN: {len(df)} directed edges (k={k}, sigma={sigma:.1f} m), 0 isolates")
    return df


def _semantic_mutual_knn(street_ids: list[str], k: int) -> pd.DataFrame:
    """Mutual k-NN cosine edges on z-scored log1p land-use vectors."""
    static = pd.read_parquet(PROCESSED_DIR / "static_features.parquet")
    static["street_id"] = static["street_id"].astype(str)
    static = static.set_index("street_id").reindex(street_ids)
    X = static[STATIC_COLS].fillna(0.0).values.astype(np.float64)

    Xl = np.log1p(np.clip(X, 0.0, None))
    mu = Xl.mean(axis=0); sd = Xl.std(axis=0); sd[sd == 0] = 1.0
    Z = (Xl - mu) / sd

    norms = np.linalg.norm(Z, axis=1, keepdims=True); norms[norms == 0] = 1e-8
    U = Z / norms
    cos = U @ U.T
    np.fill_diagonal(cos, -np.inf)

    n = len(street_ids)
    k = min(k, n - 1)
    topk = {i: set(np.argsort(cos[i])[::-1][:k].tolist()) for i in range(n)}

    rows = []
    for i in range(n):
        for j in topk[i]:
            if i in topk[j]:                            # mutual constraint
                rows.append((i, j, float(cos[i, j]), float(max(cos[i, j], 0.0))))
    df = pd.DataFrame(rows, columns=["node_i", "node_j", "similarity", "weight"])
    df = df.drop_duplicates(subset=["node_i", "node_j"]).reset_index(drop=True)
    connected = (set(df["node_i"]) | set(df["node_j"])) if len(df) else set()
    iso = n - len(connected)
    log.info(f"  Semantic mutual k-NN: {len(df)} directed edges (k={k}); {iso} isolates")
    return df


def run() -> dict[str, Path]:
    log.info("=== Step 04 (v2): fresh sensor-union graphs ===")
    V2_DIR.mkdir(parents=True, exist_ok=True)

    node_index, _, _ = _sensor_union()
    sids = node_index["street_id"].tolist()

    xy = _centroids_3111(sids)
    spatial = _spatial_knn(xy, K_SPATIAL)
    semantic = _semantic_mutual_knn(sids, K_SEMANTIC)

    node_index.to_parquet(V2_DIR / "node_index.parquet", index=False)
    spatial.to_parquet(V2_DIR / "spatial_edges.parquet", index=False)
    semantic.to_parquet(V2_DIR / "semantic_edges.parquet", index=False)
    log.info(f"  Wrote node_index ({len(node_index)}), spatial_edges ({len(spatial)}), "
             f"semantic_edges ({len(semantic)}) to {V2_DIR}")
    return {
        "node_index":     V2_DIR / "node_index.parquet",
        "spatial_edges":  V2_DIR / "spatial_edges.parquet",
        "semantic_edges": V2_DIR / "semantic_edges.parquet",
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(name)s  %(levelname)s  %(message)s")
    run()
