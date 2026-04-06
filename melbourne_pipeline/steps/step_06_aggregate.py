"""
Step 06 — Aggregate ped + parking into weekly temporal profiles per street.

Produces one row per street with:
  - 42 parking features: mean occupancy_rate per (day-of-week × time-block)
    Streets without parking sensors get all zeros (valid_parking=0).
  - 42 ped features: mean ped_flow per (day-of-week × time-block),
    normalised by that street's overall mean flow so profiles are comparable
    across high- and low-footfall streets.
  - 4 summary features: mean_ped, peak_ped, mean_parking, ped_confidence
  - Static features passed through for GMM clustering context

Time blocks (6 per day, 15-min bins aggregated):
  night:   00:00–05:59
  morning: 06:00–09:59  (commute + cafes)
  work_am: 10:00–11:59
  midday:  12:00–13:59
  work_pm: 14:00–17:59
  evening: 18:00–23:59

7 days × 6 blocks = 42 features per signal.

Output:
  data/processed/street_profiles.parquet
  data/processed/street_profiles_summary.json
"""
import json
import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from config import PROCESSED_DIR, TIME_BIN_MINUTES, STREETS_GEOJSON

ARTERIAL_TYPES = {"Arterial", "Council Major"}

log = logging.getLogger(__name__)

# ── Time blocks ───────────────────────────────────────────────────────────────
TIME_BLOCKS = {
    "night":   (0,   5),   # hours 00–05
    "morning": (6,   9),   # hours 06–09
    "work_am": (10, 11),   # hours 10–11
    "midday":  (12, 13),   # hours 12–13
    "work_pm": (14, 17),   # hours 14–17
    "evening": (18, 23),   # hours 18–23
}
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _hour_to_block(hour: int) -> str:
    for name, (lo, hi) in TIME_BLOCKS.items():
        if lo <= hour <= hi:
            return name
    return "night"


def _build_profiles(signal_df: pd.DataFrame,
                    value_col: str,
                    all_streets: list,
                    normalise: bool = False) -> pd.DataFrame:
    """
    Build (7 × 6) = 42-feature weekly profile per street.

    signal_df must have columns: street_id, time_bin, <value_col>
    Returns DataFrame indexed by street_id with 42 feature columns.
    """
    df = signal_df.copy()
    df["dow"]   = df["time_bin"].dt.dayofweek          # 0=Mon … 6=Sun
    df["hour"]  = df["time_bin"].dt.hour
    df["block"] = df["hour"].map(_hour_to_block)

    # Mean value per (street_id, day, block)
    agg = (
        df.groupby(["street_id", "dow", "block"])[value_col]
        .mean()
        .reset_index()
    )

    # Pivot to wide: one column per (day, block)
    col_names = [f"{value_col}_{DAYS[d]}_{b}" for d in range(7) for b in TIME_BLOCKS]
    agg["col"] = agg.apply(
        lambda r: f"{value_col}_{DAYS[int(r['dow'])]}_{r['block']}", axis=1
    )
    wide = agg.pivot(index="street_id", columns="col", values=value_col)

    # Reindex to all streets (zeros for missing), ensure column order
    wide = wide.reindex(index=all_streets, columns=col_names, fill_value=0.0).fillna(0.0)

    # Optional row normalisation: divide by street's overall mean (skip zero rows)
    if normalise:
        row_means = wide.values.mean(axis=1, keepdims=True)
        row_means = np.where(row_means < 1e-6, 1.0, row_means)
        wide = pd.DataFrame(
            wide.values / row_means,
            index=wide.index,
            columns=wide.columns,
        )

    return wide


def run() -> dict[str, Path]:
    log.info("=== Step 6: Aggregate street profiles ===")

    ped   = pd.read_parquet(PROCESSED_DIR / "ped_complete.parquet")
    park  = pd.read_parquet(PROCESSED_DIR / "parking_occupancy.parquet")
    static = pd.read_parquet(PROCESSED_DIR / "static_features.parquet")

    ped["time_bin"]  = pd.to_datetime(ped["time_bin"],  utc=True)
    park["time_bin"] = pd.to_datetime(park["time_bin"], utc=True)

    # ── Arterial-only, no-intersection filter ─────────────────────────────────
    _gdf = gpd.read_file(STREETS_GEOJSON)[["street_id", "str_type", "name"]]
    _gdf["street_id"] = _gdf["street_id"].astype(str)
    _keep = (
        _gdf["str_type"].isin(ARTERIAL_TYPES) &
        ~_gdf["name"].str.contains("Intersection", case=False, na=False)
    )
    arterial_ids = set(_gdf.loc[_keep, "street_id"])
    static = static[static["street_id"].astype(str).isin(arterial_ids)].reset_index(drop=True)

    all_streets = sorted(static["street_id"].unique())
    log.info(f"  Streets (arterial, no intersections): {len(all_streets)}")

    # ── Parking profiles (42 features, raw occupancy 0–1) ────────────────────
    log.info("  Building parking weekly profiles (42 features)...")
    park_profile = _build_profiles(park, "occupancy_rate", all_streets, normalise=False)
    log.info(f"  parking profile shape: {park_profile.shape}")

    # ── Ped profiles (42 features, raw ped_flow — NOT row-normalised) ──────────
    # Row-normalisation was removed: dividing by each street's mean discards
    # absolute activity level, making a busy bar district identical in shape
    # to a quiet alley. StandardScaler in step_07 handles scale differences.
    log.info("  Building ped weekly profiles (42 features, raw scale)...")
    ped_profile = _build_profiles(ped, "ped_flow", all_streets, normalise=False)
    log.info(f"  ped profile shape: {ped_profile.shape}")

    # ── Summary features ──────────────────────────────────────────────────────
    log.info("  Computing summary features...")

    ped_summary = (
        ped.groupby("street_id")["ped_flow"]
        .agg(mean_ped="mean", peak_ped="max")
        .reindex(all_streets, fill_value=0.0)
    )
    # Dominant confidence per street
    ped_conf = (
        ped.groupby("street_id")["ped_confidence"]
        .first()
        .reindex(all_streets, fill_value=0.5)
    )
    park_summary = (
        park.groupby("street_id")["occupancy_rate"]
        .mean()
        .reindex(all_streets, fill_value=0.0)
        .rename("mean_parking")
    )

    # ── Combine ───────────────────────────────────────────────────────────────
    profiles = pd.concat([
        park_profile.add_prefix("park_"),
        ped_profile.add_prefix("ped_"),
        ped_summary,
        park_summary,
        ped_conf.rename("ped_confidence"),
    ], axis=1)

    # Join static pass-through (for cluster labelling / downstream use)
    static_pt = static.set_index("street_id")[[
        "total_jobs", "cafe_count", "cafe_total_seats",
        "bar_count", "bar_patron_capacity",
        "business_count", "poi_total", "dining_capacity",
        "area_m2",
    ]]
    profiles = profiles.join(static_pt, how="left")

    # Reset index so street_id is a column
    profiles = profiles.reset_index().rename(columns={"index": "street_id"})
    # street_id may already be a column from concat; ensure no duplicate
    if "street_id" not in profiles.columns:
        profiles.insert(0, "street_id", profiles.index)

    profiles["street_id"] = profiles["street_id"].astype(str)
    profiles = profiles.fillna(0.0)

    # ── Engineered temporal scores (added to clustering input) ───────────────
    # These give the GMM explicit axes: "how morning-active is this street?"
    # instead of forcing it to infer that from 84 noisy dimensions.
    _ped  = [c for c in profiles.columns if c.startswith("ped_ped_flow_")]
    _park = [c for c in profiles.columns if c.startswith("park_occupancy_rate_")]
    _wkday = ["Mon", "Tue", "Wed", "Thu", "Fri"]
    _wkend = ["Sat", "Sun"]

    _morn_cols  = [c for c in _ped if any(c.endswith(f"_{d}_{b}") for d in _wkday for b in ("morning", "work_am"))]
    _mid_cols   = [c for c in _ped if any(c.endswith(f"_{d}_{b}") for d in _wkday for b in ("midday", "work_pm"))]
    _eve_cols   = [c for c in _ped if c.endswith("_evening")]
    _wkend_cols = [c for c in _ped if any(f"_{d}_" in c for d in _wkend)]
    _wkday_cols = [c for c in _ped if any(f"_{d}_" in c for d in _wkday)]

    # Raw mean ped_flow per period (not normalised — carries both shape and level)
    profiles["score_morning"]  = profiles[_morn_cols].mean(axis=1)
    profiles["score_midday"]   = profiles[_mid_cols].mean(axis=1)
    profiles["score_evening"]  = profiles[_eve_cols].mean(axis=1)

    # Weekend ratio: still ratio-based so it captures relative weekend uplift
    # independent of overall activity level (a quiet weekend street vs busy one)
    _wkend_mean = profiles[_wkend_cols].mean(axis=1)
    _wkday_mean = profiles[_wkday_cols].mean(axis=1).replace(0, np.nan).fillna(1.0)
    profiles["score_weekend_ratio"] = (_wkend_mean / _wkday_mean).clip(0, 5)

    # Fraction of time-blocks where parking occupancy < 30% (space available)
    _park_arr = profiles[_park].values
    profiles["score_parking_flex"] = (_park_arr < 0.30).mean(axis=1)

    log.info(f"  score_morning  range: [{profiles['score_morning'].min():.3f}, {profiles['score_morning'].max():.3f}]")
    log.info(f"  score_midday   range: [{profiles['score_midday'].min():.3f}, {profiles['score_midday'].max():.3f}]")
    log.info(f"  score_evening  range: [{profiles['score_evening'].min():.3f}, {profiles['score_evening'].max():.3f}]")
    log.info(f"  score_weekend_ratio p50: {profiles['score_weekend_ratio'].median():.3f}")
    log.info(f"  score_parking_flex  p50: {profiles['score_parking_flex'].median():.3f}")

    log.info(f"  Final profile shape: {profiles.shape}")
    log.info(f"  Columns: {len(profiles.columns)} total "
             f"(42 park + 42 ped + 5 scores + 4 summary + 9 static + 1 street_id)")

    # ── Validate ──────────────────────────────────────────────────────────────
    assert len(profiles) == len(all_streets), \
        f"Expected {len(all_streets)} rows, got {len(profiles)}"
    assert profiles.isnull().sum().sum() == 0, "NaN values in profiles"
    n_active_ped  = (profiles["mean_ped"]  > 0).sum()
    n_active_park = (profiles["mean_parking"] > 0).sum()
    log.info(f"  Streets with mean_ped > 0: {n_active_ped}")
    log.info(f"  Streets with mean_parking > 0: {n_active_park}")
    log.info(f"  mean_ped range: [{profiles['mean_ped'].min():.1f}, {profiles['mean_ped'].max():.1f}]")
    log.info(f"  mean_parking range: [{profiles['mean_parking'].min():.3f}, {profiles['mean_parking'].max():.3f}]")

    # ── Save ──────────────────────────────────────────────────────────────────
    out_path     = PROCESSED_DIR / "street_profiles.parquet"
    summary_path = PROCESSED_DIR / "street_profiles_summary.json"

    profiles.to_parquet(out_path, index=False)

    summary = {
        "n_streets":       len(profiles),
        "n_features":      len(profiles.columns) - 1,
        "n_park_features": 42,
        "n_ped_features":  42,
        "n_active_ped":    int(n_active_ped),
        "n_active_park":   int(n_active_park),
        "mean_ped_p50":    float(profiles["mean_ped"].median()),
        "mean_ped_p95":    float(profiles["mean_ped"].quantile(0.95)),
        "mean_park_p50":   float(profiles["mean_parking"].median()),
    }
    summary_path.write_text(json.dumps(summary, indent=2))

    log.info(f"  Saved: {out_path}")
    log.info("Step 6 complete.")
    return {"street_profiles": out_path, "summary": summary_path}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
