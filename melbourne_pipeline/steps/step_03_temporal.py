"""
Step 03 — Temporal features + weather forward-fill.

Produces two 15-min-resolution parquets (14,400 rows each):
  1. weather.parquet          — hourly weather forward-filled to 15-min bins
  2. temporal_features.parquet — cyclic time encodings + holiday flags
"""
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from config import RAW_DIR, PROCESSED_DIR

log = logging.getLogger(__name__)

# ── Master 15-min time index ─────────────────────────────────────────────────

TIME_INDEX = pd.date_range(
    start="2025-11-01 09:00",
    periods=14_400,
    freq="15min",
    tz="UTC",
)
# Sanity: last bin should be 2026-03-31 08:45 UTC
assert len(TIME_INDEX) == 14_400

# ── Victorian public holidays within study window ────────────────────────────

PUBLIC_HOLIDAYS = pd.to_datetime([
    "2025-11-04",  # Melbourne Cup Day
    "2025-12-25",  # Christmas Day
    "2025-12-26",  # Boxing Day
    "2026-01-01",  # New Year's Day
    "2026-01-27",  # Australia Day (observed)
    "2026-03-09",  # Labour Day (Victoria)
]).date

# ── Victorian school holidays within study window ────────────────────────────

_school_ranges = [
    ("2025-11-01", "2025-11-02"),   # End of Term 3 break
    ("2025-12-20", "2026-01-27"),   # Summer holidays
    ("2026-03-21", "2026-03-30"),   # Term 1 break (partial, clipped to study window)
]
SCHOOL_HOLIDAY_DATES = set()
for start, end in _school_ranges:
    for d in pd.date_range(start, end).date:
        SCHOOL_HOLIDAY_DATES.add(d)


# ═══════════════════════════════════════════════════════════════════════════════

def _build_weather() -> pd.DataFrame:
    """Forward-fill hourly weather to 15-min resolution."""
    log.info("Building weather.parquet ...")
    raw = pd.read_parquet(RAW_DIR / "weather_raw.parquet")
    raw["time"] = pd.to_datetime(raw["time"], utc=True)
    raw = raw.set_index("time").sort_index()

    # Reindex to 15-min, forward-fill
    weather = raw.reindex(TIME_INDEX, method="ffill")
    weather.index.name = "time_bin"
    weather = weather.reset_index()

    # Cast to float32
    for col in ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation"]:
        weather[col] = weather[col].astype("float32")

    return weather


def _build_temporal_features() -> pd.DataFrame:
    """Build cyclic time encodings + holiday flags."""
    log.info("Building temporal_features.parquet ...")
    df = pd.DataFrame({"time_bin": TIME_INDEX})

    hour = df["time_bin"].dt.hour + df["time_bin"].dt.minute / 60.0
    dow = df["time_bin"].dt.dayofweek

    df["hour_sin"] = np.sin(2 * np.pi * hour / 24).astype("float32")
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24).astype("float32")
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7).astype("float32")
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7).astype("float32")
    df["is_weekend"] = (dow >= 5).astype("int8")

    dates = df["time_bin"].dt.date
    df["is_public_holiday"] = dates.isin(PUBLIC_HOLIDAYS).astype("int8")
    df["is_school_holiday"] = dates.isin(SCHOOL_HOLIDAY_DATES).astype("int8")

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════

def _validate_weather(df: pd.DataFrame):
    assert len(df) == 14_400, f"weather rows: {len(df)} != 14400"
    assert df.isna().sum().sum() == 0, f"weather NaN:\n{df.isna().sum()}"
    assert df["temperature_2m"].between(-5, 45).all(), \
        f"temperature out of range: [{df['temperature_2m'].min()}, {df['temperature_2m'].max()}]"
    assert (df["precipitation"] >= 0).all(), "negative precipitation"
    log.info(f"  weather: {len(df)} rows, temp range [{df['temperature_2m'].min():.1f}, {df['temperature_2m'].max():.1f}]C")


def _validate_temporal(df: pd.DataFrame):
    assert len(df) == 14_400, f"temporal rows: {len(df)} != 14400"
    assert df.isna().sum().sum() == 0, f"temporal NaN:\n{df.isna().sum()}"

    # Cyclic identity: sin^2 + cos^2 = 1
    hour_mag = df["hour_sin"] ** 2 + df["hour_cos"] ** 2
    assert np.allclose(hour_mag, 1.0, atol=1e-5), f"hour cyclic check failed, max deviation: {(hour_mag - 1).abs().max()}"
    dow_mag = df["dow_sin"] ** 2 + df["dow_cos"] ** 2
    assert np.allclose(dow_mag, 1.0, atol=1e-5), f"dow cyclic check failed"

    # Spot-check: 2025-11-08 is a Saturday
    sat_bins = df[df["time_bin"].dt.date == pd.Timestamp("2025-11-08").date()]
    assert (sat_bins["is_weekend"] == 1).all(), "Saturday not flagged as weekend"

    # Christmas Day
    xmas = df[df["time_bin"].dt.date == pd.Timestamp("2025-12-25").date()]
    assert (xmas["is_public_holiday"] == 1).all(), "Christmas not flagged"

    # Public holiday bin count = holidays * 96 bins/day
    expected_ph_bins = len(PUBLIC_HOLIDAYS) * 96
    actual_ph_bins = df["is_public_holiday"].sum()
    assert actual_ph_bins == expected_ph_bins, \
        f"public holiday bins: {actual_ph_bins} != {expected_ph_bins}"

    log.info(f"  temporal: {len(df)} rows, "
             f"public_holiday_bins={actual_ph_bins}, "
             f"school_holiday_bins={df['is_school_holiday'].sum()}")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def run() -> dict[str, Path]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    if not (RAW_DIR / "weather_raw.parquet").exists():
        raise FileNotFoundError(f"Required input missing: {RAW_DIR / 'weather_raw.parquet'}")

    weather = _build_weather()
    temporal = _build_temporal_features()

    _validate_weather(weather)
    _validate_temporal(temporal)

    wx_path = PROCESSED_DIR / "weather.parquet"
    tf_path = PROCESSED_DIR / "temporal_features.parquet"
    weather.to_parquet(wx_path, index=False)
    temporal.to_parquet(tf_path, index=False)

    log.info(
        f"Step 3 complete: "
        f"weather.parquet={len(weather)} rows, "
        f"temporal_features.parquet={len(temporal)} rows, "
        f"public_holiday_bins={temporal['is_public_holiday'].sum()}, "
        f"school_holiday_bins={temporal['is_school_holiday'].sum()}"
    )
    return {"weather": wx_path, "temporal_features": tf_path}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
