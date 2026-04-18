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
 15. ptv_stops.geojson          — All Victoria public transport stops (PTV)
 16. bus_stops.parquet          — Melbourne City bus stop signs (City of Melbourne)

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

# ── Transit stop sources ─────────────────────────────────────────────────────
# Full Victoria public transport stops (tram, train, bus, coach, skybus).
# Filter to MODE == "TRAM" in step_04 when computing transit proximity.
PTV_STOPS_URL = (
    "https://opendata.transport.vic.gov.au/dataset/"
    "6d36dfd9-8693-4552-8a03-05eb29a391fd/resource/"
    "a2cba0b0-bddc-4b87-b495-2b6b7013af6e/download/"
    "public_transport_stops.geojson"
)
# Melbourne City bus stop signs (309 records, <10K → single paginate pass).
MELB_BUS_STOPS_SLUG = "bus-stops"

# ── Supabase REST helpers ────────────────────────────────────────────────────

_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}


def _fetch_supabase_view(view_name: str, max_retries: int = 3) -> pd.DataFrame:
    """Keyset-paginate through a Supabase view using id ordering.

    Uses the PostgREST REST API directly (httpx) instead of the Supabase
    Python client so we can reliably fetch millions of rows by paging on
    the id column.

    Resilience features:
    - Per-batch retry with exponential backoff (5 / 10 / 20 s) on 5xx errors.
    - Checkpoint/resume: writes a partial parquet + last_id cursor to RAW_DIR
      every CHECKPOINT_EVERY batches. On the next run, if both files exist the
      fetch resumes from the saved id rather than restarting from zero.
    """
    CHECKPOINT_EVERY = 50  # write checkpoint every 50 × SUPABASE_BATCH_SIZE rows

    checkpoint_path = RAW_DIR / f"{view_name}_checkpoint.txt"
    partial_path    = RAW_DIR / f"{view_name}_partial.parquet"

    rest_url  = f"{SUPABASE_URL}/rest/v1/{view_name}"
    all_rows: list[dict] = []
    last_id   = 0
    has_prior = False

    # Resume from a previous partial run if checkpoint files exist
    if checkpoint_path.exists() and partial_path.exists():
        last_id   = int(checkpoint_path.read_text().strip())
        prior_len = len(pd.read_parquet(partial_path, columns=[]))
        has_prior = True
        log.info(
            f"  {view_name}: resuming from checkpoint id={last_id} "
            f"({prior_len:,} rows already in partial file)"
        )

    batch_count = 0
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

            # Per-batch retry with exponential backoff on 5xx errors
            for attempt in range(1, max_retries + 1):
                resp = client.get(rest_url, params=params, headers=_HEADERS)
                if resp.status_code < 500 or attempt == max_retries:
                    resp.raise_for_status()
                    break
                wait = 5 * (2 ** (attempt - 1))  # 5 s, 10 s, 20 s
                log.warning(
                    f"  {view_name}: HTTP {resp.status_code} on attempt "
                    f"{attempt}/{max_retries}, retrying in {wait}s ..."
                )
                time.sleep(wait)

            batch = resp.json()
            if not batch:
                break
            all_rows.extend(batch)
            last_id = batch[-1]["id"]
            log.info(f"  {view_name}: got {len(batch):,} rows  (total so far: {len(all_rows):,})")

            # Checkpoint: write partial parquet + cursor so a crash is resumable.
            # During a resumed run we only overwrite the cursor; the partial
            # parquet from before the crash stays intact (avoids re-reading it).
            batch_count += 1
            if batch_count % CHECKPOINT_EVERY == 0 and not has_prior:
                pd.DataFrame(all_rows).to_parquet(partial_path, index=False)
                checkpoint_path.write_text(str(last_id))
                log.info(f"  {view_name}: checkpoint saved at id={last_id}")

            if len(batch) < SUPABASE_BATCH_SIZE:
                break

    # Combine partial (from prior run) with newly fetched rows
    new_df = pd.DataFrame(all_rows)
    if has_prior and partial_path.exists():
        prior_df = pd.read_parquet(partial_path)
        final_df = pd.concat([prior_df, new_df], ignore_index=True)
    else:
        final_df = new_df

    # Remove checkpoint files on clean completion
    for p in (checkpoint_path, partial_path):
        if p.exists():
            p.unlink()

    log.info(f"  {view_name}: {len(final_df):,} total rows fetched")
    return final_df


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


# ── Transit stops ────────────────────────────────────────────────────────────

def fetch_transit_stops() -> dict[str, Path]:
    """Download PTV public transport stops GeoJSON and Melbourne bus stop signs.

    Outputs
    -------
    ptv_stops.geojson   — All Victoria stops. Properties: STOP_ID, STOP_NAME, MODE.
                          MODE values include "TRAM", "TRAIN", "BUS", "SKYBUS", etc.
                          Step 04 filters to MODE == "TRAM" within the Melbourne bbox.
    bus_stops.parquet   — Melbourne City bus stop signs (309 records).
                          Fields: stop_id, roadseg_id, lat, lon, description.
                          roadseg_id links directly to street_id in node_index.
    """
    outputs: dict[str, Path] = {}

    # 1. PTV stops GeoJSON — all Victoria (~7.8 MB, single GET)
    log.info("Fetching PTV public transport stops GeoJSON (~7.8 MB)...")
    resp = requests.get(PTV_STOPS_URL, timeout=180)
    resp.raise_for_status()
    ptv_path = RAW_DIR / "ptv_stops.geojson"
    ptv_path.write_bytes(resp.content)
    log.info(f"  PTV stops: {len(resp.content) / 1024:.0f} KB -> {ptv_path}")
    outputs["ptv_stops"] = ptv_path

    # 2. Melbourne City bus stop signs — paginate all 309 records
    log.info("Fetching Melbourne City bus stops...")
    base = (
        f"{MELBOURNE_OPEN_DATA_BASE}/api/explore/v2.1/catalog/datasets"
        f"/{MELB_BUS_STOPS_SLUG}/records"
    )
    records = _paginate_clue(base, refine=None, where=None)
    bus_rows = []
    for r in records:
        geo = r.get("geo_point_2d") or {}
        bus_rows.append({
            "stop_id":    r.get("objectid"),
            "roadseg_id": r.get("roadseg_id"),
            "lat":        geo.get("lat"),
            "lon":        geo.get("lon"),
            "description": r.get("descriptio"),
        })
    bus_df = pd.DataFrame(bus_rows).dropna(subset=["lat", "lon"])
    bus_path = RAW_DIR / "bus_stops.parquet"
    bus_df.to_parquet(bus_path, index=False)
    log.info(f"  Melbourne bus stops: {len(bus_df)} records -> {bus_path}")
    outputs["bus_stops"] = bus_path

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
    outputs.update(fetch_transit_stops())

    log.info(f"Step 01 complete -- {len(outputs)} files written to {RAW_DIR}")
    return outputs


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
