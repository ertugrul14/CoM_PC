"""
Step 02 — Sensor snapping + CLUE spatial aggregation.

Part A: Map every unique parking/ped sensor to its street polygon -> sensor_map.parquet
Part B: Aggregate CLUE datasets onto 362 street polygons -> static_features.parquet
"""
import json
import logging
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from shapely.geometry import Point, shape

from config import RAW_DIR, PROCESSED_DIR, STREETS_GEOJSON, MELBOURNE_OPEN_DATA_BASE, MELB_LAT, MELB_LON

log = logging.getLogger(__name__)

CRS_WGS84 = "EPSG:4326"
CRS_VIC = "EPSG:3111"  # Victorian GDA94

# ── CBD distance helper ───────────────────────────────────────────────────────

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine great-circle distance in metres between two WGS84 points."""
    from math import radians, sin, cos, sqrt, atan2
    R = 6_371_000
    φ1, φ2 = radians(lat1), radians(lat2)
    dφ = radians(lat2 - lat1)
    dλ = radians(lon2 - lon1)
    a = sin(dφ / 2) ** 2 + cos(φ1) * cos(φ2) * sin(dλ / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


# ── Helpers ──────────────────────────────────────────────────────────────────

def _load_streets() -> gpd.GeoDataFrame:
    """Load 362 street polygons, project to EPSG:3111.

    Normalises mixed geometry types (Polygon/MultiPolygon/LineString)
    by buffering LineStrings and converting everything to MultiPolygon.
    """
    streets = gpd.read_file(STREETS_GEOJSON)
    streets["street_id"] = streets["street_id"].astype(str)
    streets = streets.to_crs(CRS_VIC)

    # Buffer LineStrings into polygons (1m buffer), unify to Polygon
    from shapely.geometry import MultiPolygon, Polygon
    from shapely.ops import unary_union
    fixed = []
    for geom in streets.geometry:
        if geom is None:
            fixed.append(None)
        elif geom.geom_type == "LineString":
            fixed.append(geom.buffer(1.0))
        elif geom.geom_type == "MultiPolygon":
            fixed.append(geom)
        elif geom.geom_type == "Polygon":
            fixed.append(MultiPolygon([geom]))
        else:
            fixed.append(geom.buffer(0))  # force to polygon
    streets = streets.set_geometry(fixed)

    log.info(f"  Loaded {len(streets)} street polygons")
    return streets


def _points_to_gdf(df: pd.DataFrame, lon_col: str, lat_col: str) -> gpd.GeoDataFrame:
    """Convert DataFrame with lat/lon to projected GeoDataFrame."""
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs=CRS_WGS84,
    )
    return gdf.to_crs(CRS_VIC)


def _snap_points(
    points: gpd.GeoDataFrame,
    streets: gpd.GeoDataFrame,
    id_col: str,
    nn_max_dist: float = 25.0,
) -> pd.DataFrame:
    """Snap points to street polygons: point-in-polygon then nearest-neighbour fallback."""
    # Step 1: point-in-polygon
    pip = gpd.sjoin(points, streets[["street_id", "geometry"]], predicate="within", how="left")
    pip["method"] = np.where(pip["street_id"].notna(), "point_in_polygon", None)
    pip["dist_m"] = np.where(pip["street_id"].notna(), 0.0, np.nan)

    matched = pip[pip["street_id"].notna()]
    unmatched_ids = pip[pip["street_id"].isna()][id_col].unique()
    log.info(f"    PIP matched: {len(matched[id_col].unique())}, unmatched: {len(unmatched_ids)}")

    # Step 2: nearest-neighbour for unmatched
    if len(unmatched_ids) > 0:
        unmatched_pts = points[points[id_col].isin(unmatched_ids)].copy()
        nn = gpd.sjoin_nearest(
            unmatched_pts,
            streets[["street_id", "geometry"]],
            max_distance=nn_max_dist,
            distance_col="dist_m",
            how="left",
        )
        nn["method"] = np.where(nn["street_id"].notna(), "nearest_neighbour", "unmatched")
        nn.loc[nn["street_id"].isna(), "dist_m"] = np.nan

        # Combine
        result = pd.concat([
            matched[[id_col, "street_id", "method", "dist_m"]],
            nn[[id_col, "street_id", "method", "dist_m"]],
        ], ignore_index=True)
    else:
        result = matched[[id_col, "street_id", "method", "dist_m"]].copy()

    # Deduplicate (sjoin can produce dupes at polygon boundaries)
    result = result.drop_duplicates(subset=[id_col], keep="first")
    return result


def _filter_latest_year(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Filter to most recent census_year if column exists."""
    if "census_year" in df.columns:
        latest = df["census_year"].max()
        df = df[df["census_year"] == latest].copy()
        log.info(f"    {name}: filtered to census_year={latest} -> {len(df)} rows")
    return df


# ── Sensor location APIs ─────────────────────────────────────────────────────

def _fetch_ped_sensor_locations() -> pd.DataFrame:
    """Fetch pedestrian sensor locations from Melbourne Open Data API."""
    base = (f"{MELBOURNE_OPEN_DATA_BASE}/api/explore/v2.1/catalog/datasets"
            "/pedestrian-counting-system-sensor-locations/records")
    all_records = []
    offset = 0
    while True:
        resp = requests.get(base, params={"limit": 100, "offset": offset}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        all_records.extend(results)
        if offset + 100 >= data.get("total_count", len(all_records)):
            break
        offset += 100
        time.sleep(0.15)
    return pd.DataFrame(all_records)


# ═══════════════════════════════════════════════════════════════════════════════
# PART A — Sensor snapping
# ═══════════════════════════════════════════════════════════════════════════════

def _snap_sensors(streets: gpd.GeoDataFrame) -> pd.DataFrame:
    log.info("Part A: Sensor snapping")

    # ── Parking sensors ──────────────────────────────────────────────────────
    parking = pd.read_parquet(RAW_DIR / "parking_raw.parquet")
    parking_sensors = (
        parking.drop_duplicates(subset=["kerbsideid"], keep="first")
        [["kerbsideid", "latitude", "longitude"]]
        .copy()
    )
    parking_sensors["kerbsideid"] = parking_sensors["kerbsideid"].astype(str)
    log.info(f"  Parking: {len(parking_sensors)} unique sensors")

    parking_gdf = _points_to_gdf(parking_sensors, "longitude", "latitude")
    parking_snap = _snap_points(parking_gdf, streets, "kerbsideid")
    parking_snap = parking_snap.merge(
        parking_sensors[["kerbsideid", "latitude", "longitude"]],
        on="kerbsideid",
    )
    parking_snap = parking_snap.rename(columns={"kerbsideid": "sensor_id", "latitude": "lat", "longitude": "lon"})
    parking_snap["sensor_type"] = "parking"

    matched_pct = (parking_snap["street_id"].notna().sum() / len(parking_snap)) * 100
    log.info(f"  Parking match rate: {matched_pct:.1f}%")
    assert matched_pct >= 90, f"Parking match rate {matched_pct:.1f}% < 90%"

    # ── Pedestrian sensors ───────────────────────────────────────────────────
    ped = pd.read_parquet(RAW_DIR / "ped_raw.parquet")
    ped_ids = ped["location_id"].unique()
    log.info(f"  Pedestrian: {len(ped_ids)} unique location_ids in Supabase data")

    # Fetch sensor locations from Melbourne Open Data API
    ped_locs = _fetch_ped_sensor_locations()
    log.info(f"  Fetched {len(ped_locs)} ped sensor locations from API")

    # Deduplicate API data (some location_ids appear multiple times)
    ped_locs = ped_locs.drop_duplicates(subset=["location_id"], keep="first")

    # Match Supabase location_ids to API locations
    ped_sensors = ped_locs[ped_locs["location_id"].isin(ped_ids)].copy()
    ped_sensors["location_id"] = ped_sensors["location_id"].astype(str)
    log.info(f"  Matched {len(ped_sensors)}/{len(ped_ids)} ped sensors to API locations")

    unmatched_ped_ids = set(str(x) for x in ped_ids) - set(ped_sensors["location_id"])
    if unmatched_ped_ids:
        log.warning(f"  {len(unmatched_ped_ids)} ped location_ids not found in API: {sorted(unmatched_ped_ids)[:10]}")

    # Snap ped sensors to streets
    ped_gdf = _points_to_gdf(ped_sensors, "longitude", "latitude")
    ped_snap = _snap_points(ped_gdf, streets, "location_id")
    ped_snap = ped_snap.merge(
        ped_sensors[["location_id", "latitude", "longitude", "sensor_description"]],
        on="location_id",
    )
    ped_snap = ped_snap.rename(columns={
        "location_id": "sensor_id",
        "latitude": "lat",
        "longitude": "lon",
    })
    ped_snap["sensor_type"] = "pedestrian"
    # Keep sensor_description for viz export

    # Add unmatched ped sensors (no coords from API)
    if unmatched_ped_ids:
        unmatched_df = pd.DataFrame({
            "sensor_id": sorted(unmatched_ped_ids),
            "sensor_type": "pedestrian",
            "street_id": pd.Series([None] * len(unmatched_ped_ids), dtype="object"),
            "method": "unmatched",
            "dist_m": np.nan,
            "lat": np.nan,
            "lon": np.nan,
        })
        ped_snap = pd.concat([ped_snap, unmatched_df], ignore_index=True)

    ped_matched_pct = (ped_snap["street_id"].notna().sum() / len(ped_snap)) * 100
    log.info(f"  Pedestrian match rate: {ped_matched_pct:.1f}%")

    # ── Combine ──────────────────────────────────────────────────────────────
    sensor_map = pd.concat([
        parking_snap[["sensor_id", "sensor_type", "street_id", "method", "dist_m", "lat", "lon"]],
        ped_snap,
    ], ignore_index=True)

    # Validate: no duplicate sensor_id within same sensor_type
    dupes = sensor_map.groupby(["sensor_type", "sensor_id"]).size()
    assert (dupes == 1).all(), f"Duplicate sensor_ids found: {dupes[dupes > 1]}"

    return sensor_map


# ═══════════════════════════════════════════════════════════════════════════════
# PART B — CLUE spatial aggregation
# ═══════════════════════════════════════════════════════════════════════════════

def _agg_point_dataset(
    parquet_name: str,
    streets: gpd.GeoDataFrame,
    lat_col: str,
    lon_col: str,
    agg_dict: dict,
    dataset_label: str,
    pre_filter=None,
) -> pd.DataFrame:
    """Load a CLUE parquet, filter latest year, sjoin to streets, aggregate."""
    df = pd.read_parquet(RAW_DIR / parquet_name)
    log.info(f"  {dataset_label}: columns={df.columns.tolist()}")
    df = _filter_latest_year(df, dataset_label)

    if pre_filter is not None:
        before = len(df)
        df = pre_filter(df)
        log.info(f"    {dataset_label}: pre-filter {before} -> {len(df)} rows")

    # Drop rows without coordinates
    df = df.dropna(subset=[lat_col, lon_col])
    if df.empty:
        log.warning(f"    {dataset_label}: no rows with coordinates")
        return pd.DataFrame({"street_id": streets["street_id"]}).assign(
            **{col: 0 for col in agg_dict.values()}
        )

    gdf = _points_to_gdf(df, lon_col, lat_col)
    # Use nearest-neighbour join (max 50m) instead of point-in-polygon.
    # Street polygons are narrow curbside strips; CLUE points sit at
    # building addresses which are typically 10-30m away from the road.
    joined = gpd.sjoin_nearest(
        gdf, streets[["street_id", "geometry"]],
        max_distance=50, how="inner", distance_col="_dist",
    )
    # A point equidistant to two streets gets duplicated — keep only the closest
    joined = joined.loc[~joined.index.duplicated(keep="first")]
    joined = joined.drop(columns=["_dist"])
    log.info(f"    {dataset_label}: {len(joined)} points matched to streets (nearest, max 50m)")

    result = streets[["street_id"]].copy()
    for src_col, out_col in agg_dict.items():
        if src_col == "__count__":
            agged = joined.groupby("street_id").size().reset_index(name=out_col)
        else:
            agged = joined.groupby("street_id")[src_col].sum().reset_index(name=out_col)
        result = result.merge(agged, on="street_id", how="left")

    return result


def _agg_block_dataset(
    parquet_name: str,
    streets: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
    value_col: str,
    out_col: str,
    dataset_label: str,
    row_filter=None,
) -> pd.DataFrame:
    """Area-weighted block-to-street aggregation (Pattern 2)."""
    df = pd.read_parquet(RAW_DIR / parquet_name)
    log.info(f"  {dataset_label}: columns={df.columns.tolist()}")
    df = _filter_latest_year(df, dataset_label)

    if row_filter is not None:
        df = row_filter(df)

    # Merge block values onto block geometries
    blocks_with_val = blocks.merge(df[["block_id", value_col]], on="block_id", how="inner")
    blocks_with_val["block_area"] = blocks_with_val.geometry.area

    # Intersection
    intersection = gpd.overlay(streets[["street_id", "geometry"]], blocks_with_val, how="intersection")
    intersection["weight"] = intersection.geometry.area / intersection["block_area"]
    intersection["weighted_val"] = intersection[value_col].fillna(0) * intersection["weight"]

    agged = intersection.groupby("street_id")["weighted_val"].sum().reset_index(name=out_col)
    result = streets[["street_id"]].merge(agged, on="street_id", how="left")
    return result


def _build_static_features(streets: gpd.GeoDataFrame) -> pd.DataFrame:
    log.info("Part B: CLUE spatial aggregation")

    # Load CLUE blocks for area-weighted joins
    # The API stored geometry as a nested Feature object: geometry={type:Feature, geometry:{...}}
    # We need to extract the inner geometry manually.
    import json as _json
    from shapely.geometry import shape as _shape
    with open(RAW_DIR / "clue_blocks.geojson") as f:
        blk_data = _json.load(f)
    rows = []
    for feat in blk_data["features"]:
        props = feat["properties"]
        raw_geom = feat.get("geometry")
        geom = None
        if isinstance(raw_geom, dict):
            # Nested: {type: "Feature", geometry: {type: "Polygon", ...}}
            inner = raw_geom.get("geometry", raw_geom)
            if isinstance(inner, dict) and "coordinates" in inner:
                geom = _shape(inner)
        rows.append({**props, "geometry": geom})
    blocks_raw = gpd.GeoDataFrame(rows, crs=CRS_WGS84)
    blocks_raw = blocks_raw[blocks_raw.geometry.notna()].copy()
    blocks_raw = blocks_raw.to_crs(CRS_VIC)
    blocks_raw["block_id"] = blocks_raw["block_id"].astype(int)
    log.info(f"  Loaded {len(blocks_raw)} CLUE blocks")

    static = streets[["street_id"]].copy()

    # ── Point datasets ───────────────────────────────────────────────────────
    # Cafe — dataset has separate rows for indoor/outdoor seating per venue.
    # We need unique venue count (deduplicated) and total seats (summed).
    # First aggregate seats per venue, then sjoin unique venues to streets.
    def _dedup_cafes(df):
        """Collapse indoor/outdoor rows into one row per venue, summing seats."""
        grouped = df.groupby(
            ["trading_name", "latitude", "longitude"], as_index=False
        ).agg(number_of_seats=("number_of_seats", "sum"))
        log.info(f"    Cafes: {len(df)} seating rows -> {len(grouped)} unique venues")
        return grouped

    cafe = _agg_point_dataset(
        "clue_cafe.parquet", streets, "latitude", "longitude",
        {"__count__": "cafe_count", "number_of_seats": "cafe_total_seats"},
        "clue_cafe",
        pre_filter=_dedup_cafes,
    )
    static = static.merge(cafe, on="street_id", how="left")

    # Bar
    bar = _agg_point_dataset(
        "clue_bar.parquet", streets, "latitude", "longitude",
        {"__count__": "bar_count", "number_of_patrons": "bar_patron_capacity"},
        "clue_bar",
    )
    static = static.merge(bar, on="street_id", how="left")

    # Business (exclude hospitality: ANZSIC 4000-4599)
    def _exclude_hospitality(df):
        code = df["industry_anzsic4_code"].astype(str)
        hosp = code.str.match(r"^4[0-5]")
        return df[~hosp]

    biz = _agg_point_dataset(
        "clue_business.parquet", streets, "latitude", "longitude",
        {"__count__": "business_count"},
        "clue_business",
        pre_filter=_exclude_hospitality,
    )
    static = static.merge(biz, on="street_id", how="left")

    # Buildings
    bld = _agg_point_dataset(
        "clue_buildings.parquet", streets, "latitude", "longitude",
        {"__count__": "building_count"},
        "clue_buildings",
    )
    static = static.merge(bld, on="street_id", how="left")

    # Off-street parking
    ofs = _agg_point_dataset(
        "clue_offstreet.parquet", streets, "latitude", "longitude",
        {"parking_spaces": "offstreet_spaces"},
        "clue_offstreet",
    )
    static = static.merge(ofs, on="street_id", how="left")

    # Dwellings
    dwl = _agg_point_dataset(
        "clue_dwellings.parquet", streets, "latitude", "longitude",
        {"dwelling_number": "dwelling_count"},
        "clue_dwellings",
    )
    static = static.merge(dwl, on="street_id", how="left")

    # Landmarks (different column names)
    lmk = _agg_point_dataset(
        "clue_landmarks.parquet", streets, "co_ordinates.lat", "co_ordinates.lon",
        {"__count__": "poi_total"},
        "clue_landmarks",
    )
    static = static.merge(lmk, on="street_id", how="left")

    # ── Block datasets (area-weighted) ───────────────────────────────────────
    # Jobs
    jobs = _agg_block_dataset(
        "clue_jobs.parquet", streets, blocks_raw,
        "total_jobs_in_block", "total_jobs", "clue_jobs",
    )
    static = static.merge(jobs, on="street_id", how="left")

    # Floorspace — retail (sum of retail columns) and office
    fs = pd.read_parquet(RAW_DIR / "clue_floorspace_use.parquet")
    fs = _filter_latest_year(fs, "clue_floorspace_use")
    retail_cols = [c for c in fs.columns if c.startswith("retail_")]
    log.info(f"  clue_floorspace_use retail columns: {retail_cols}")
    fs["retail_total"] = fs[retail_cols].fillna(0).sum(axis=1)

    # Save modified floorspace temporarily for block agg
    fs.to_parquet(RAW_DIR / "_tmp_floorspace.parquet", index=False)

    retail_fs = _agg_block_dataset(
        "_tmp_floorspace.parquet", streets, blocks_raw,
        "retail_total", "retail_floorspace", "floorspace_retail",
    )
    static = static.merge(retail_fs, on="street_id", how="left")

    office_fs = _agg_block_dataset(
        "_tmp_floorspace.parquet", streets, blocks_raw,
        "office", "office_floorspace", "floorspace_office",
    )
    static = static.merge(office_fs, on="street_id", how="left")

    # Clean up temp file
    (RAW_DIR / "_tmp_floorspace.parquet").unlink(missing_ok=True)

    # ── Derived columns ──────────────────────────────────────────────────────
    static["dining_capacity"] = static["cafe_total_seats"].fillna(0) + static["bar_patron_capacity"].fillna(0)
    static["area_m2"] = streets.set_index("street_id").geometry.area.reindex(static["street_id"]).values

    # ── Street centroid coordinates (WGS84) ─────────────────────────────────
    # Critical for XGBoost imputation: location in CBD is the strongest predictor
    # of pedestrian activity magnitude (Flinders St area vs Docklands, etc.)
    centroids_wgs = streets[["street_id", "geometry"]].copy().to_crs("EPSG:4326")
    centroids_wgs["centroid_lon"] = centroids_wgs.geometry.centroid.x
    centroids_wgs["centroid_lat"] = centroids_wgs.geometry.centroid.y
    static = static.merge(
        centroids_wgs[["street_id", "centroid_lat", "centroid_lon"]],
        on="street_id", how="left",
    )

    static = static.fillna(0)

    # ── CBD distance: metres from street centroid to Hoddle Grid centre ──────
    # Uses MELB_LAT / MELB_LON from config (same point used for weather fetch).
    # Strongest single location predictor: Flinders St area >> Docklands.
    static["cbd_distance_m"] = [
        _haversine_m(lat, lon, MELB_LAT, MELB_LON)
        for lat, lon in zip(static["centroid_lat"], static["centroid_lon"])
    ]

    # ── Column order per spec ────────────────────────────────────────────────
    col_order = [
        "street_id", "area_m2", "centroid_lat", "centroid_lon",
        "cbd_distance_m",
        "total_jobs", "cafe_count", "cafe_total_seats",
        "bar_count", "bar_patron_capacity", "business_count", "building_count",
        "poi_total", "dining_capacity", "offstreet_spaces", "dwelling_count",
        "retail_floorspace", "office_floorspace",
    ]
    static = static[col_order]

    return static


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════

def _validate(sensor_map: pd.DataFrame, static: pd.DataFrame):
    # sensor_map
    parking_n = sensor_map[sensor_map["sensor_type"] == "parking"].shape[0]
    ped_n = sensor_map[sensor_map["sensor_type"] == "pedestrian"].shape[0]
    log.info(f"  sensor_map: {len(sensor_map)} rows (parking={parking_n}, ped={ped_n})")
    dupes = sensor_map.groupby(["sensor_type", "sensor_id"]).size()
    assert (dupes == 1).all(), "Duplicate sensor_id within sensor_type"

    # static_features
    n_streets = len(gpd.read_file(STREETS_GEOJSON))
    assert len(static) == n_streets, f"Expected {n_streets} streets, got {len(static)}"
    expected_cols = {
        "street_id", "area_m2", "centroid_lat", "centroid_lon",
        "cbd_distance_m",
        "total_jobs", "cafe_count", "cafe_total_seats",
        "bar_count", "bar_patron_capacity", "business_count", "building_count",
        "poi_total", "dining_capacity", "offstreet_spaces", "dwelling_count",
        "retail_floorspace", "office_floorspace",
    }
    assert set(static.columns) == expected_cols, f"Column mismatch: {set(static.columns) ^ expected_cols}"
    assert static.isna().sum().sum() == 0, f"NaN values found:\n{static.isna().sum()}"
    assert (static["total_jobs"].max() < 100_000), f"total_jobs max={static['total_jobs'].max()} >= 100k"
    assert (static["dining_capacity"] == static["cafe_total_seats"] + static["bar_patron_capacity"]).all()

    # Sanity check
    log.info("  Top 5 streets by total_jobs:")
    for _, r in static.nlargest(5, "total_jobs").iterrows():
        log.info(f"    street_id={r['street_id']}  total_jobs={r['total_jobs']:.0f}")
    log.info("  Top 5 streets by cafe_total_seats:")
    for _, r in static.nlargest(5, "cafe_total_seats").iterrows():
        log.info(f"    street_id={r['street_id']}  cafe_total_seats={r['cafe_total_seats']:.0f}")


# ═══════════════════════════════════════════════════════════════════════════════
# Visualization export
# ═══════════════════════════════════════════════════════════════════════════════

def _export_viz_geojson(sensor_map: pd.DataFrame, streets: gpd.GeoDataFrame):
    """Export GeoJSON files for the Mapbox visualization."""
    import json as _json

    # Sensors with coordinates
    has_coords = sensor_map[sensor_map["lat"].notna()].copy()
    features = []
    for _, r in has_coords.iterrows():
        props = {
            "sensor_id": r["sensor_id"],
            "sensor_type": r["sensor_type"],
            "street_id": r["street_id"],
            "method": r["method"],
            "dist_m": round(r["dist_m"], 1) if pd.notna(r["dist_m"]) else None,
        }
        # Add sensor_description if available
        if "sensor_description" in r.index and pd.notna(r.get("sensor_description")):
            props["sensor_description"] = r["sensor_description"]
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
        })
    sensors_geojson = {"type": "FeatureCollection", "features": features}

    # Streets in WGS84
    streets_wgs = streets.to_crs("EPSG:4326")
    streets_json = _json.loads(streets_wgs.to_json())

    # Write with NaN-safe serialization (replace NaN with None)
    import math

    def _nan_to_none(obj):
        if isinstance(obj, float) and math.isnan(obj):
            return None
        if isinstance(obj, dict):
            return {k: _nan_to_none(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_nan_to_none(v) for v in obj]
        return obj

    with open(PROCESSED_DIR / "sensors_viz.geojson", "w") as f:
        _json.dump(_nan_to_none(sensors_geojson), f)
    with open(PROCESSED_DIR / "streets_viz.geojson", "w") as f:
        _json.dump(_nan_to_none(streets_json), f)
    log.info(f"  Viz export: {len(features)} sensors, {len(streets_json['features'])} streets")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def run() -> dict[str, Path]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Verify all inputs exist
    required = [
        STREETS_GEOJSON,
        RAW_DIR / "parking_raw.parquet",
        RAW_DIR / "ped_raw.parquet",
        RAW_DIR / "clue_cafe.parquet",
        RAW_DIR / "clue_bar.parquet",
        RAW_DIR / "clue_business.parquet",
        RAW_DIR / "clue_jobs.parquet",
        RAW_DIR / "clue_buildings.parquet",
        RAW_DIR / "clue_blocks.geojson",
        RAW_DIR / "clue_offstreet.parquet",
        RAW_DIR / "clue_dwellings.parquet",
        RAW_DIR / "clue_floorspace_use.parquet",
        RAW_DIR / "clue_landmarks.parquet",
    ]
    for p in required:
        if not p.exists():
            raise FileNotFoundError(f"Required input missing: {p}")

    streets = _load_streets()

    sensor_map = _snap_sensors(streets)
    static = _build_static_features(streets)

    _validate(sensor_map, static)

    # Write outputs
    sm_path = PROCESSED_DIR / "sensor_map.parquet"
    sf_path = PROCESSED_DIR / "static_features.parquet"
    sensor_map.to_parquet(sm_path, index=False)
    static.to_parquet(sf_path, index=False)

    # Write visualization GeoJSON files
    _export_viz_geojson(sensor_map, streets)

    log.info(f"Step 2 complete: sensor_map={len(sensor_map)} sensors, "
             f"static_features={len(static)} streets x {len(static.columns)} cols")

    return {"sensor_map": sm_path, "static_features": sf_path}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
