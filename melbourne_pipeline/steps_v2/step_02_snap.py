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

# ── Arterial filter (must stay in sync with step_04_graph.KEEP_STR_TYPES) ────
# CLUE points + sensors are snapped only to these street types. Minor/private
# segments and intersection stubs are not carried through the pipeline, so
# snapping to them silently loses signal when step_04 filters them out later.
ARTERIAL_STR_TYPES = {"Arterial", "Council Major"}

# ── CLUE-point aggregation ───────────────────────────────────────────────────
# Each CLUE point is assigned to its single nearest arterial within
# CLUE_MAX_DIST_M and contributes an unweighted 1.0 (integer count). Points
# farther than this from any arterial fall outside the catchment and are
# dropped — they wouldn't realistically feed pedestrian flow on the nearest
# street. Integer counts keep the features directly interpretable: a value of
# 11 means "11 points assigned to this arterial's 250m catchment."
CLUE_MAX_DIST_M = 250.0

# Sensor snapping: NN fallback cap. Raised from 25m because the arterial-only
# street set is sparser than the all-streets set used previously.
SENSOR_NN_MAX_M = 50.0

# Buffer populated by _agg_point_dataset; flushed to clue_points_viz.geojson
# by _export_viz_geojson. Lets the browser viewer visualise the new snapping
# without any pipeline-side changes.
_CLUE_VIZ_BUFFER: list = []

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
    """Load street polygons filtered to arterial + Council Major (no intersections).

    Also normalises mixed geometry types (Polygon/MultiPolygon/LineString) by
    buffering LineStrings and converting everything to MultiPolygon.
    """
    streets = gpd.read_file(STREETS_GEOJSON)
    streets["street_id"] = streets["street_id"].astype(str)

    # Arterial-only filter — must match step_04_graph.KEEP_STR_TYPES so points
    # snapped here survive the downstream graph-eligibility gate.
    n_raw = len(streets)
    keep_type = streets["str_type"].isin(ARTERIAL_STR_TYPES) if "str_type" in streets.columns else pd.Series([True] * len(streets))
    keep_name = ~streets["name"].str.contains("Intersection", case=False, na=False) if "name" in streets.columns else pd.Series([True] * len(streets))
    streets = streets[keep_type & keep_name].reset_index(drop=True)
    log.info(f"  Arterial filter: {n_raw} raw → {len(streets)} kept "
             f"(dropped {n_raw - len(streets)} minor/private/intersection segments)")

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
    nn_max_dist: float = SENSOR_NN_MAX_M,
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
    viz_layer: str | None = None,
    viz_name_col: str | None = None,
    viz_value_col: str | None = None,
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
    # Nearest-neighbour join with a cap (CLUE_MAX_DIST_M). Each point is
    # attributed to its single closest arterial and contributes as an integer
    # count (weight = 1). Thesis-defensible: "how many cafes/bars/etc. are
    # associated with this street" is a clean, interpretable integer feature.
    joined = gpd.sjoin_nearest(
        gdf, streets[["street_id", "geometry"]],
        max_distance=CLUE_MAX_DIST_M, how="inner", distance_col="_dist",
    )
    joined = joined.loc[~joined.index.duplicated(keep="first")]
    joined["_w"] = 1.0
    log.info(
        f"    {dataset_label}: {len(joined)} points matched "
        f"(max {CLUE_MAX_DIST_M:.0f}m, integer counts)"
    )

    result = streets[["street_id"]].copy()
    int_cols: list[str] = []
    for src_col, out_col in agg_dict.items():
        if src_col == "__count__":
            agged = joined.groupby("street_id").size().reset_index(name=out_col)
            int_cols.append(out_col)
        else:
            vals = joined[src_col].fillna(0).astype(float)
            joined["_wv"] = vals
            agged = joined.groupby("street_id")["_wv"].sum().reset_index(name=out_col)
            joined = joined.drop(columns=["_wv"])
            # If every value is an integer after sum, cast down for tidiness.
            if (agged[out_col] % 1 == 0).all():
                agged[out_col] = agged[out_col].astype("int64")
                int_cols.append(out_col)
        result = result.merge(agged, on="street_id", how="left")

    # Fill missing streets with 0 and preserve int dtype where applicable.
    for col in agg_dict.values():
        result[col] = result[col].fillna(0)
        if col in int_cols:
            result[col] = result[col].astype("int64")

    # ── Viz buffer: one feature per matched point, in WGS84 ─────────────────
    # Uses the original lat/lon columns (which sjoin_nearest preserves on the
    # left side of the join) so no re-projection is needed.
    if viz_layer and len(joined):
        has_name = viz_name_col and viz_name_col in joined.columns
        has_val  = viz_value_col and viz_value_col in joined.columns
        for _, r in joined.iterrows():
            name = (str(r[viz_name_col]) if has_name and pd.notna(r[viz_name_col]) else None)
            if has_val and pd.notna(r[viz_value_col]):
                try:
                    val = float(r[viz_value_col])
                    val = int(val) if val.is_integer() else round(val, 2)
                except (ValueError, TypeError):
                    val = 1
            else:
                val = 1
            _CLUE_VIZ_BUFFER.append({
                "layer":     viz_layer,
                "street_id": str(r["street_id"]),
                "dist_m":    round(float(r["_dist"]), 1),
                "weight":    round(float(r["_w"]), 3),
                "name":      name,
                "value":     val,
                "lon":       float(r[lon_col]),
                "lat":       float(r[lat_col]),
            })

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


def _agg_block_multi(
    parquet_name: str,
    streets: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
    col_map: dict[str, str],
    dataset_label: str,
    pre_compute=None,
) -> pd.DataFrame:
    """Area-weighted block-to-street aggregation for multiple columns in one overlay pass.

    Args:
        parquet_name: filename in RAW_DIR
        streets: arterial street polygons in CRS_VIC
        blocks: CLUE block polygons in CRS_VIC
        col_map: {source_col: output_col} mapping
        dataset_label: for log messages
        pre_compute: optional callable(df) -> df to add derived columns before merge
    Returns:
        DataFrame with street_id + output columns, 0-filled for unmatched streets
    """
    df = pd.read_parquet(RAW_DIR / parquet_name)
    df = _filter_latest_year(df, dataset_label)

    if pre_compute is not None:
        df = pre_compute(df)

    src_cols = list(col_map.keys())
    available = [c for c in src_cols if c in df.columns]
    missing = [c for c in src_cols if c not in df.columns]
    if missing:
        log.warning(f"  {dataset_label}: source columns missing, will zero: {missing}")

    blocks_with_val = blocks.merge(df[["block_id"] + available], on="block_id", how="inner")
    blocks_with_val["block_area"] = blocks_with_val.geometry.area

    intersection = gpd.overlay(streets[["street_id", "geometry"]], blocks_with_val, how="intersection")
    intersection["weight"] = intersection.geometry.area / intersection["block_area"]
    log.info(f"  {dataset_label} multi: {len(available)} cols, {len(intersection)} intersection fragments")

    result = streets[["street_id"]].copy()
    for src_col, out_col in col_map.items():
        if src_col in available:
            intersection["_wv"] = intersection[src_col].fillna(0) * intersection["weight"]
            agged = intersection.groupby("street_id")["_wv"].sum().reset_index(name=out_col)
            result = result.merge(agged, on="street_id", how="left")
        if out_col not in result.columns:
            result[out_col] = 0.0
        result[out_col] = result[out_col].fillna(0.0)

    return result


def _agg_buildings_enriched(streets: gpd.GeoDataFrame) -> pd.DataFrame:
    """Snap CLUE buildings to streets, returning count, mean floors, and bicycle spaces.

    Single sjoin pass — avoids repeating the expensive nearest-neighbour join three times.
    Returns DataFrame with: building_count (int), floors_mean (float), bicycle_spaces (int).
    """
    df = pd.read_parquet(RAW_DIR / "clue_buildings.parquet")
    df = _filter_latest_year(df, "clue_buildings")
    df = df.dropna(subset=["latitude", "longitude"])
    df["number_of_floors_above_ground"] = (
        pd.to_numeric(df["number_of_floors_above_ground"], errors="coerce").fillna(0).clip(lower=0)
    )
    df["bicycle_spaces"] = (
        pd.to_numeric(df["bicycle_spaces"], errors="coerce").fillna(0).clip(lower=0)
    )

    gdf = _points_to_gdf(df, "longitude", "latitude")
    joined = gpd.sjoin_nearest(
        gdf, streets[["street_id", "geometry"]],
        max_distance=CLUE_MAX_DIST_M, how="inner", distance_col="_dist",
    )
    joined = joined.loc[~joined.index.duplicated(keep="first")]
    log.info(f"  clue_buildings enriched: {len(joined)} points matched (max {CLUE_MAX_DIST_M:.0f}m)")

    result = streets[["street_id"]].copy()

    cnt = joined.groupby("street_id").size().reset_index(name="building_count")
    result = result.merge(cnt, on="street_id", how="left")
    result["building_count"] = result["building_count"].fillna(0).astype("int64")

    # Mean floors over buildings that reported >0 floors (0 = not recorded)
    floors_data = joined[joined["number_of_floors_above_ground"] > 0]
    if len(floors_data):
        floors_agg = (
            floors_data.groupby("street_id")["number_of_floors_above_ground"]
            .mean().reset_index(name="floors_mean")
        )
        result = result.merge(floors_agg, on="street_id", how="left")
    else:
        result["floors_mean"] = 0.0
    result["floors_mean"] = result["floors_mean"].fillna(0.0).round(2)

    bic_agg = joined.groupby("street_id")["bicycle_spaces"].sum().reset_index(name="bicycle_spaces")
    result = result.merge(bic_agg, on="street_id", how="left")
    result["bicycle_spaces"] = result["bicycle_spaces"].fillna(0).astype("int64")

    for _, r in joined.iterrows():
        floors_val = int(r["number_of_floors_above_ground"]) if r["number_of_floors_above_ground"] > 0 else 1
        _CLUE_VIZ_BUFFER.append({
            "layer":     "building",
            "street_id": str(r["street_id"]),
            "dist_m":    round(float(r["_dist"]), 1),
            "weight":    1.0,
            "name":      str(r.get("property_id", "")),
            "value":     floors_val,
            "lon":       float(r["longitude"]),
            "lat":       float(r["latitude"]),
        })

    return result


def _agg_transit_stops(streets: gpd.GeoDataFrame) -> pd.DataFrame:
    """Aggregate bus and PTV transit stops onto streets.

    Bus stops use a direct roadseg_id → street_id join (no spatial operation).
    Tram/train stops count PTV points within 300 m / 500 m of each street centroid.

    Returns DataFrame with: bus_stops_count, tram_stops_300m, train_stations_500m.
    """
    result = streets[["street_id"]].copy()

    # ── Bus stops: direct roadseg_id join ────────────────────────────────────
    bus = pd.read_parquet(RAW_DIR / "bus_stops.parquet")
    bus["roadseg_id"] = bus["roadseg_id"].astype(str)
    bus_counts = (
        bus.groupby("roadseg_id").size()
        .reset_index(name="bus_stops_count")
        .rename(columns={"roadseg_id": "street_id"})
    )
    result = result.merge(bus_counts, on="street_id", how="left")
    result["bus_stops_count"] = result["bus_stops_count"].fillna(0).astype("int64")
    n_matched = (result["bus_stops_count"] > 0).sum()
    match_pct = n_matched / len(result) * 100
    log.info(f"  bus_stops: {len(bus)} stops -> {n_matched} streets ({match_pct:.1f}%) with stops")
    if match_pct < 5:
        log.warning("  bus_stops: very low match rate — roadseg_id may not align with street_id")

    # Viz buffer: bus stops with their directly-attributed street
    matched_bus = bus[bus["roadseg_id"].isin(result[result["bus_stops_count"] > 0]["street_id"])]
    for _, r in matched_bus.iterrows():
        _CLUE_VIZ_BUFFER.append({
            "layer":     "bus_stop",
            "street_id": str(r["roadseg_id"]),
            "dist_m":    0.0,
            "weight":    1.0,
            "name":      str(r.get("description", "")),
            "value":     1,
            "lon":       float(r["lon"]),
            "lat":       float(r["lat"]),
        })

    # ── PTV stops: centroid buffer spatial count + nearest-street viz ─────────
    ptv = gpd.read_file(RAW_DIR / "ptv_stops.geojson")
    ptv = ptv[ptv.geometry.notna()].to_crs(CRS_VIC)

    centroids_vic = streets[["street_id", "geometry"]].copy()
    centroids_vic = centroids_vic.set_geometry(centroids_vic.geometry.centroid)

    for mode, col, viz_layer, radius in [
        ("METRO TRAM",  "tram_stops_300m",    "tram_stop",      300),
        ("METRO TRAIN", "train_stations_500m", "train_station",  500),
    ]:
        mode_pts = ptv[ptv["MODE"] == mode].copy()
        if mode_pts.empty:
            result[col] = 0
            log.warning(f"  PTV {mode}: no stops found")
            continue

        # Count: stops within radius of each street centroid
        buffers = centroids_vic.copy()
        buffers = buffers.set_geometry(buffers.geometry.buffer(radius))
        joined_buf = gpd.sjoin(
            mode_pts[["geometry"]], buffers[["street_id", "geometry"]],
            predicate="within", how="inner",
        )
        counts = joined_buf.groupby("street_id").size().reset_index(name=col)
        result = result.merge(counts, on="street_id", how="left")
        result[col] = result[col].fillna(0).astype("int64")
        n_with = (result[col] > 0).sum()
        log.info(f"  PTV {mode}: {len(mode_pts)} stops -> {n_with} streets within {radius}m")

        # Viz: snap each stop to its single nearest street centroid.
        # Cap at radius so only stops near the CBD street network appear.
        nearest = gpd.sjoin_nearest(
            mode_pts[["STOP_NAME", "geometry"]].copy(),
            centroids_vic[["street_id", "geometry"]],
            how="inner", distance_col="_snap_dist",
            max_distance=radius,
        )
        nearest = nearest.loc[~nearest.index.duplicated(keep="first")]
        # geometry col is the left-side stop point (CRS_VIC); convert to WGS84 in bulk
        nearest_wgs = nearest.set_geometry("geometry").to_crs(CRS_WGS84)
        for idx, r in nearest_wgs.iterrows():
            _CLUE_VIZ_BUFFER.append({
                "layer":     viz_layer,
                "street_id": str(r["street_id"]),
                "dist_m":    round(float(nearest.at[idx, "_snap_dist"]), 1),
                "weight":    1.0,
                "name":      str(r.get("STOP_NAME", "")),
                "value":     1,
                "lon":       round(float(r.geometry.x), 6),
                "lat":       round(float(r.geometry.y), 6),
            })

    return result


def _agg_dwellings(streets: gpd.GeoDataFrame) -> pd.DataFrame:
    """Snap CLUE dwellings to streets, returning total count and type breakdown.

    Single sjoin pass with post-hoc type grouping.
    Returns: dwelling_count (total), dwellings_house, dwellings_apartment.
    Student Apartments are grouped into dwellings_apartment.
    """
    df = pd.read_parquet(RAW_DIR / "clue_dwellings.parquet")
    df = _filter_latest_year(df, "clue_dwellings")
    df = df.dropna(subset=["latitude", "longitude"])
    df["dwelling_number"] = pd.to_numeric(df["dwelling_number"], errors="coerce").fillna(0)
    df["_type"] = df["dwelling_type"].map({
        "House/Townhouse": "house",
        "Residential Apartments": "apartment",
        "Student Apartments": "apartment",
    }).fillna("other")

    gdf = _points_to_gdf(df, "longitude", "latitude")
    joined = gpd.sjoin_nearest(
        gdf, streets[["street_id", "geometry"]],
        max_distance=CLUE_MAX_DIST_M, how="inner", distance_col="_dist",
    )
    joined = joined.loc[~joined.index.duplicated(keep="first")]
    log.info(f"  clue_dwellings: {len(joined)} points matched (max {CLUE_MAX_DIST_M:.0f}m)")

    result = streets[["street_id"]].copy()

    # Total = house + apartment only; log if any unrecognised types are dropped
    n_other = (joined["_type"] == "other").sum()
    if n_other:
        log.warning(f"  clue_dwellings: {n_other} rows with unrecognised dwelling_type excluded from total")
    total = (
        joined[joined["_type"] != "other"]
        .groupby("street_id")["dwelling_number"].sum()
        .reset_index(name="dwelling_count")
    )
    result = result.merge(total, on="street_id", how="left")
    result["dwelling_count"] = result["dwelling_count"].fillna(0).astype("int64")

    for type_key, col in [("house", "dwellings_house"), ("apartment", "dwellings_apartment")]:
        agg = (
            joined[joined["_type"] == type_key]
            .groupby("street_id")["dwelling_number"].sum()
            .reset_index(name=col)
        )
        result = result.merge(agg, on="street_id", how="left")
        result[col] = result[col].fillna(0).astype("int64")

    for _, r in joined.iterrows():
        _CLUE_VIZ_BUFFER.append({
            "layer":     "dwelling",
            "street_id": str(r["street_id"]),
            "dist_m":    round(float(r["_dist"]), 1),
            "weight":    1.0,
            "name":      str(r.get("property_id", "")),
            "value":     int(r["dwelling_number"]) if r["dwelling_number"] > 0 else 1,
            "lon":       float(r["longitude"]),
            "lat":       float(r["latitude"]),
        })

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
        viz_layer="cafe", viz_name_col="trading_name", viz_value_col="number_of_seats",
    )
    static = static.merge(cafe, on="street_id", how="left")

    # Bar
    bar = _agg_point_dataset(
        "clue_bar.parquet", streets, "latitude", "longitude",
        {"__count__": "bar_count", "number_of_patrons": "bar_patron_capacity"},
        "clue_bar",
        viz_layer="bar", viz_name_col="trading_name", viz_value_col="number_of_patrons",
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
        viz_layer="business", viz_name_col="trading_name",
    )
    static = static.merge(biz, on="street_id", how="left")

    # Buildings — count + mean floors + bicycle spaces (single snap pass)
    bld = _agg_buildings_enriched(streets)
    static = static.merge(bld, on="street_id", how="left")

    # Off-street parking
    ofs = _agg_point_dataset(
        "clue_offstreet.parquet", streets, "latitude", "longitude",
        {"parking_spaces": "offstreet_spaces"},
        "clue_offstreet",
        viz_layer="offstreet", viz_name_col="property_id", viz_value_col="parking_spaces",
    )
    static = static.merge(ofs, on="street_id", how="left")

    # Dwellings — total + type split (house vs apartment, single snap pass)
    dwl = _agg_dwellings(streets)
    static = static.merge(dwl, on="street_id", how="left")

    # Landmarks (different column names)
    lmk = _agg_point_dataset(
        "clue_landmarks.parquet", streets, "co_ordinates.lat", "co_ordinates.lon",
        {"__count__": "poi_total"},
        "clue_landmarks",
        viz_layer="landmark", viz_name_col="feature_name",
    )
    static = static.merge(lmk, on="street_id", how="left")

    # ── Block datasets (area-weighted, single overlay pass each) ─────────────
    # Jobs — total + sector breakdown by floor-use type
    def _prep_jobs(df: pd.DataFrame) -> pd.DataFrame:
        retail_cols = [c for c in df.columns if c.startswith("retail_")]
        df["jobs_retail"] = df[retail_cols].fillna(0).sum(axis=1)
        return df

    jobs = _agg_block_multi(
        "clue_jobs.parquet", streets, blocks_raw,
        {
            "total_jobs_in_block": "total_jobs",
            "office":              "jobs_office",
            "jobs_retail":         "jobs_retail",
            "educational_research": "jobs_education",
            "hospital_clinic":     "jobs_health",
        },
        "clue_jobs",
        pre_compute=_prep_jobs,
    )
    static = static.merge(jobs, on="street_id", how="left")

    # Floorspace — office, retail, and activity-type breakdowns
    def _prep_floorspace(df: pd.DataFrame) -> pd.DataFrame:
        retail_cols = [c for c in df.columns if c.startswith("retail_")]
        log.info(f"  clue_floorspace_use retail columns: {retail_cols}")
        df["retail_total"] = df[retail_cols].fillna(0).sum(axis=1)
        return df

    floorspace = _agg_block_multi(
        "clue_floorspace_use.parquet", streets, blocks_raw,
        {
            "retail_total":                    "retail_floorspace",
            "office":                          "office_floorspace",
            "residential_apartment":           "floorspace_residential_apt",
            "educational_research":            "floorspace_education",
            "entertainment_recreation_indoor": "floorspace_entertainment",
        },
        "clue_floorspace_use",
        pre_compute=_prep_floorspace,
    )
    static = static.merge(floorspace, on="street_id", how="left")

    # Transit stops — bus (direct join) + tram/train (centroid buffer)
    transit = _agg_transit_stops(streets)
    static = static.merge(transit, on="street_id", how="left")

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
        "street_id",
        # Geometry / location
        "area_m2", "centroid_lat", "centroid_lon", "cbd_distance_m",
        # Employment
        "total_jobs", "jobs_office", "jobs_retail", "jobs_education", "jobs_health",
        # Buildings
        "building_count", "floors_mean", "bicycle_spaces",
        # Hospitality
        "cafe_count", "cafe_total_seats", "bar_count", "bar_patron_capacity", "dining_capacity",
        # Commercial activity
        "business_count",
        # Floorspace
        "retail_floorspace", "office_floorspace",
        "floorspace_residential_apt", "floorspace_education", "floorspace_entertainment",
        # Parking / residential
        "offstreet_spaces", "dwelling_count", "dwellings_house", "dwellings_apartment",
        # Transit
        "bus_stops_count", "tram_stops_300m", "train_stations_500m",
        # Landmarks
        "poi_total",
    ]
    static = static[col_order]

    return static


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════

def _validate(sensor_map: pd.DataFrame, static: pd.DataFrame, n_streets: int):
    # sensor_map
    parking_n = sensor_map[sensor_map["sensor_type"] == "parking"].shape[0]
    ped_n = sensor_map[sensor_map["sensor_type"] == "pedestrian"].shape[0]
    log.info(f"  sensor_map: {len(sensor_map)} rows (parking={parking_n}, ped={ped_n})")
    dupes = sensor_map.groupby(["sensor_type", "sensor_id"]).size()
    assert (dupes == 1).all(), "Duplicate sensor_id within sensor_type"

    # static_features — arterial-filtered street count passed in from run()
    assert len(static) == n_streets, f"Expected {n_streets} streets, got {len(static)}"
    expected_cols = {
        "street_id", "area_m2", "centroid_lat", "centroid_lon", "cbd_distance_m",
        "total_jobs", "jobs_office", "jobs_retail", "jobs_education", "jobs_health",
        "building_count", "floors_mean", "bicycle_spaces",
        "cafe_count", "cafe_total_seats", "bar_count", "bar_patron_capacity", "dining_capacity",
        "business_count",
        "retail_floorspace", "office_floorspace",
        "floorspace_residential_apt", "floorspace_education", "floorspace_entertainment",
        "offstreet_spaces", "dwelling_count", "dwellings_house", "dwellings_apartment",
        "bus_stops_count", "tram_stops_300m", "train_stations_500m",
        "poi_total",
    }
    assert set(static.columns) == expected_cols, f"Column mismatch: {set(static.columns) ^ expected_cols}"
    assert static.isna().sum().sum() == 0, f"NaN values found:\n{static.isna().sum()}"
    assert (static["total_jobs"].max() < 100_000), f"total_jobs max={static['total_jobs'].max()} >= 100k"
    assert (static["dining_capacity"] == static["cafe_total_seats"] + static["bar_patron_capacity"]).all()
    assert static[["bus_stops_count", "tram_stops_300m", "train_stations_500m", "floors_mean", "bicycle_spaces"]].min().min() >= 0
    assert (static["dwelling_count"] == static["dwellings_house"] + static["dwellings_apartment"]).all(), \
        "dwelling_count != dwellings_house + dwellings_apartment"

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

    # Streets in WGS84 — read ALL street segments (not just the arterial-filtered
    # subset used for snapping), so the browser viewer can still toggle the
    # "Arterial only" filter and visually confirm that CLUE points skip past
    # minor/private roads to their nearest arterial.
    all_streets_wgs = gpd.read_file(STREETS_GEOJSON)
    all_streets_wgs["street_id"] = all_streets_wgs["street_id"].astype(str)
    if all_streets_wgs.crs is None or str(all_streets_wgs.crs).upper() != "EPSG:4326":
        all_streets_wgs = all_streets_wgs.to_crs("EPSG:4326")
    # Repair the 92 self-intersecting polygons that cause bowtie/spike artefacts
    # in Mapbox. buffer(0) is the canonical shapely fix: it resolves ring
    # self-intersections while preserving polygon topology and type.
    invalid_mask = ~all_streets_wgs.geometry.is_valid
    if invalid_mask.any():
        all_streets_wgs.loc[invalid_mask, "geometry"] = (
            all_streets_wgs.loc[invalid_mask, "geometry"].buffer(0)
        )
        log.info(f"  Repaired {invalid_mask.sum()} invalid street geometries for viz export")
    streets_json = _json.loads(all_streets_wgs.to_json())

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

    # ── CLUE point viz — reflects the weighted nearest-arterial snapping ─
    # The browser viewer uses this to render per-CLUE-layer points and the
    # dashed snap-lines back to each point's assigned arterial.
    clue_features = []
    for p in _CLUE_VIZ_BUFFER:
        clue_features.append({
            "type": "Feature",
            "properties": {
                "layer":     p["layer"],
                "street_id": p["street_id"],
                "dist_m":    p["dist_m"],
                "weight":    p["weight"],
                "name":      p["name"],
                "value":     p["value"],
            },
            "geometry": {"type": "Point", "coordinates": [p["lon"], p["lat"]]},
        })
    clue_geojson = {"type": "FeatureCollection", "features": clue_features}
    with open(PROCESSED_DIR / "clue_points_viz.geojson", "w") as f:
        _json.dump(_nan_to_none(clue_geojson), f)
    from collections import Counter as _Counter
    by_layer = _Counter(p["layer"] for p in _CLUE_VIZ_BUFFER)
    log.info(f"  CLUE viz export: {len(clue_features)} points — {dict(by_layer)}")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def run() -> dict[str, Path]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    _CLUE_VIZ_BUFFER.clear()  # idempotent across re-runs within same process

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
        RAW_DIR / "bus_stops.parquet",
        RAW_DIR / "ptv_stops.geojson",
    ]
    for p in required:
        if not p.exists():
            raise FileNotFoundError(f"Required input missing: {p}")

    streets = _load_streets()

    sensor_map = _snap_sensors(streets)
    static = _build_static_features(streets)

    _validate(sensor_map, static, n_streets=len(streets))

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
