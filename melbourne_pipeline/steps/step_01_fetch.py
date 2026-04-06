"""
Step 01 — Fetch ALL raw data.

Downloads and saves to data/raw/:
  1. parking_raw.parquet        — Supabase parking_melbourne view
  2. ped_raw.parquet            — Supabase ped_melbourne view
  3. weather_raw.parquet        — Open-Meteo hourly archive
  4. clue_cafe.parquet          — Cafes & restaurants with seating capacity
  5. clue_bar.parquet           — Bars & pubs with patron capacity
  6. clue_business.parquet      — Business establishments
  7. clue_jobs.parquet          — Employment by block
  8. clue_buildings.parquet     — Buildings with attributes
  9. clue_blocks.geojson        — CLUE block polygons
 10. clue_offstreet.parquet     — Off-street car parks
 11. clue_dwellings.parquet     — Residential dwellings
 12. clue_floorspace_industry.parquet — Floor space by industry
 13. clue_floorspace_use.parquet     — Floor space by use
 14. clue_landmarks.parquet     — Landmarks & places of interest

Every fetch is idempotent — re-running overwrites data/raw/ cleanly.
"""
import json
import logging
import time
from pathlib import Path

import httpx
import pandas as pd
import requests

from config import (
    CLUE_DATASETS,
    DATA_END_STR,
    DATA_START_STR,
    MELB_LAT,
    MELB_LON,
    MELBOURNE_OPEN_DATA_BASE,
    OPEN_METEO_URL,
    RAW_DIR,
    SUPABASE_BATCH_SIZE,
    SUPABASE_KEY,
    SUPABASE_URL,
    WEATHER_PARAMS,
)

log = logging.getLogger(__name__)

# ── Supabase REST helpers ────────────────────────────────────────────────────

_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}


def _fetch_supabase_view(view_name: str) -> pd.DataFrame:
    """Keyset-paginate through a Supabase view using id ordering.

    Uses the PostgREST REST API directly (httpx) instead of the Supabase
    Python client so we can reliably fetch millions of rows by paging on
    the id column.
    """
    rest_url = f"{SUPABASE_URL}/rest/v1/{view_name}"
    all_rows: list[dict] = []
    last_id = 0

    with httpx.Client(timeout=60) as client:
        while True:
            params = {
                "select": "*",
                "local_datetime": [
                    f"gte.{DATA_START_STR}",
                    f"lte.{DATA_END_STR}",
                ],
                "id": f"gt.{last_id}",
                "order": "id.asc",
                "limit": str(SUPABASE_BATCH_SIZE),
            }
            log.info(f"  {view_name}: fetching after id={last_id} ...")
            resp = client.get(rest_url, params=params, headers=_HEADERS)
            resp.raise_for_status()
            batch = resp.json()

            if not batch:
                break
            all_rows.extend(batch)
            last_id = batch[-1]["id"]
            log.info(f"  {view_name}: got {len(batch):,} rows  (total so far: {len(all_rows):,})")
            if len(batch) < SUPABASE_BATCH_SIZE:
                break

    log.info(f"  {view_name}: {len(all_rows):,} total rows fetched")
    return pd.DataFrame(all_rows)


def fetch_parking() -> Path:
    """Download parking sensor events from Supabase."""
    log.info("Fetching parking data from Supabase ...")
    df = _fetch_supabase_view("parking_melbourne")
    df["local_datetime"] = pd.to_datetime(df["local_datetime"])
    out = RAW_DIR / "parking_raw.parquet"
    df.to_parquet(out, index=False)
    log.info(f"  -> {out}  ({len(df):,} rows)")
    return out


def fetch_pedestrian() -> Path:
    """Download pedestrian count events from Supabase."""
    log.info("Fetching pedestrian data from Supabase ...")
    df = _fetch_supabase_view("ped_melbourne")
    df["local_datetime"] = pd.to_datetime(df["local_datetime"])
    out = RAW_DIR / "ped_raw.parquet"
    df.to_parquet(out, index=False)
    log.info(f"  -> {out}  ({len(df):,} rows)")
    return out


# ── Open-Meteo weather ───────────────────────────────────────────────────────

def fetch_weather() -> Path:
    """Download hourly weather from Open-Meteo archive API."""
    log.info("Fetching weather from Open-Meteo ...")
    params = {
        "latitude": MELB_LAT,
        "longitude": MELB_LON,
        "start_date": DATA_START_STR,
        "end_date": DATA_END_STR,
        "hourly": WEATHER_PARAMS,
        "timezone": "UTC",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    hourly = data["hourly"]
    df = pd.DataFrame({
        "time": pd.to_datetime(hourly["time"]),
        "temperature_2m": hourly["temperature_2m"],
        "relative_humidity_2m": hourly["relative_humidity_2m"],
        "wind_speed_10m": hourly["wind_speed_10m"],
        "precipitation": hourly["precipitation"],
    })

    out = RAW_DIR / "weather_raw.parquet"
    df.to_parquet(out, index=False)
    log.info(f"  -> {out}  ({len(df):,} rows)")
    return out


# ── CLUE datasets from Melbourne Open Data ───────────────────────────────────

CLUE_MAX_OFFSET = 10_000  # Melbourne Open Data API hard limit


def _fetch_clue_page(base_url: str, limit: int, offset: int,
                     refine: str | None = None,
                     order_by: str | None = None,
                     where: str | None = None) -> dict:
    """Fetch a single page from the CLUE API."""
    params: dict = {"limit": limit, "offset": offset}
    if refine:
        params["refine"] = refine
    if order_by:
        params["order_by"] = order_by
    if where:
        params["where"] = where
    resp = requests.get(base_url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _paginate_clue(base_url: str, refine: str | None, where: str | None) -> list[dict]:
    """Fetch all records for a single (refine, where) combination, respecting 10K offset cap."""
    limit = 100
    probe = _fetch_clue_page(base_url, limit=1, offset=0, refine=refine, where=where)
    total = probe.get("total_count", 0)

    records: list[dict] = []
    offset = 0
    while offset < min(total, CLUE_MAX_OFFSET):
        payload = _fetch_clue_page(base_url, limit, offset, refine=refine, where=where)
        results = payload.get("results", [])
        if not results:
            break
        records.extend(results)
        offset += limit
        time.sleep(0.15)
    return records


# block_id ranges used to split datasets that exceed 10K rows even after
# filtering by census_year.  Each range must produce < 10K rows.
# Granular enough to handle business (19,672 rows in 2024).
_BLOCK_SPLITS = [
    "block_id < 50",
    "block_id >= 50 and block_id < 100",
    "block_id >= 100 and block_id < 300",
    "block_id >= 300 and block_id < 500",
    "block_id >= 500 and block_id < 700",
    "block_id >= 700",
]


def _fetch_clue_dataset(dataset_slug: str) -> list[dict]:
    """Paginate through a Melbourne Open Data v2.1 catalog dataset.

    The API caps offset at 10 000. Strategy:
      1. If total <= 10K -> fetch directly
      2. If total > 10K and has census_year -> filter to latest year
      3. If still > 10K -> split by block_id ranges
    """
    base = (f"{MELBOURNE_OPEN_DATA_BASE}/api/explore/v2.1/catalog/datasets"
            f"/{dataset_slug}/records")

    # First probe: get total_count
    probe = _fetch_clue_page(base, limit=1, offset=0)
    total = probe.get("total_count", 0)
    log.info(f"    total_count={total:,}")

    refine = None
    if total > CLUE_MAX_OFFSET:
        # Filter by latest census_year if available
        sample = probe.get("results", [{}])[0]
        if "census_year" in sample:
            probe_sorted = _fetch_clue_page(base, limit=1, offset=0,
                                            order_by="census_year desc")
            latest = probe_sorted.get("results", [{}])[0].get("census_year")
            if latest:
                refine = f"census_year:{latest}"
                log.info(f"    dataset > 10K rows, filtering to census_year={latest}")
                probe2 = _fetch_clue_page(base, limit=1, offset=0, refine=refine)
                total = probe2.get("total_count", total)
                log.info(f"    filtered total_count={total:,}")

    if total <= CLUE_MAX_OFFSET:
        # Simple case: fetch in one pass
        return _paginate_clue(base, refine=refine, where=None)

    # Still > 10K: split by block_id ranges
    log.info(f"    still > 10K after year filter, splitting by block_id ranges")
    all_records: list[dict] = []
    for where_clause in _BLOCK_SPLITS:
        chunk = _paginate_clue(base, refine=refine, where=where_clause)
        log.info(f"      {where_clause}: {len(chunk):,} rows")
        all_records.extend(chunk)

    log.info(f"    total after block splits: {len(all_records):,}")
    return all_records


def fetch_clue() -> dict[str, Path]:
    """Download all CLUE datasets and save to data/raw/."""
    outputs = {}

    for name, slug in CLUE_DATASETS.items():
        log.info(f"Fetching CLUE: {name} ({slug}) ...")
        records = _fetch_clue_dataset(slug)
        log.info(f"  {name}: {len(records):,} records")

        if not records:
            log.warning(f"  {name}: 0 records -- skipping")
            continue

        # clue_blocks contains geometry -> save as GeoJSON
        if name == "clue_blocks":
            out = RAW_DIR / "clue_blocks.geojson"
            features = []
            for rec in records:
                geo = rec.pop("geo_shape", rec.pop("geo_point_2d", None))
                feat = {
                    "type": "Feature",
                    "properties": rec,
                    "geometry": geo if isinstance(geo, dict) and "type" in geo else None,
                }
                features.append(feat)
            fc = {"type": "FeatureCollection", "features": features}
            with open(out, "w") as f:
                json.dump(fc, f)
        else:
            df = pd.json_normalize(records)
            out = RAW_DIR / f"{name}.parquet"
            df.to_parquet(out, index=False)

        log.info(f"  -> {out}")
        outputs[name] = out

    return outputs


# ── Main entry point ─────────────────────────────────────────────────────────

def run() -> dict[str, Path]:
    """Execute all Step 01 fetches. Returns dict of output paths."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    outputs = {}
    outputs["parking"] = fetch_parking()
    outputs["pedestrian"] = fetch_pedestrian()
    outputs["weather"] = fetch_weather()
    outputs.update(fetch_clue())

    log.info(f"Step 01 complete -- {len(outputs)} files written to {RAW_DIR}")
    return outputs


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
