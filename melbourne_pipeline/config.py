"""
config.py — Single source of truth for ALL pipeline parameters.
"""
import os
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
MODELS_DIR = DATA_DIR / "models"
LOGS_DIR = BASE_DIR / "logs"
FRONTEND_DIR = BASE_DIR / "frontend"
FRONTEND_DATA_DIR = FRONTEND_DIR / "data"
STREETS_GEOJSON = PROCESSED_DIR / "streets.geojson"

# ── Supabase ─────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ── Study window ─────────────────────────────────────────────────────────────
DATA_START = datetime(2025, 11, 1, 9, 0, 0)   # UTC
DATA_END = datetime(2026, 3, 30, 9, 0, 0)     # UTC
DATA_START_STR = DATA_START.strftime("%Y-%m-%d")
DATA_END_STR = DATA_END.strftime("%Y-%m-%d")

# ── Temporal resolution ──────────────────────────────────────────────────────
TIME_BIN_MINUTES = 15

# ── Melbourne CBD centroid (for weather) ─────────────────────────────────────
MELB_LAT = -37.8136
MELB_LON = 144.9631

# ── Supabase batch settings ─────────────────────────────────────────────────
SUPABASE_BATCH_SIZE = 1_000    # rows per request (Supabase server-side max)

# ── CLUE datasets ────────────────────────────────────────────────────────────
MELBOURNE_OPEN_DATA_BASE = "https://data.melbourne.vic.gov.au"

CLUE_DATASETS = {
    "clue_cafe": "cafes-and-restaurants-with-seating-capacity",
    "clue_bar": "bars-and-pubs-with-patron-capacity",
    "clue_business": "business-establishments-with-address-and-industry-classification",
    "clue_jobs": "employment-by-block-by-space-use",
    "clue_buildings": "buildings-with-name-age-size-accessibility-and-bicycle-facilities",
    "clue_blocks": "blocks-for-census-of-land-use-and-employment-clue",
    "clue_offstreet": "off-street-car-parks-with-capacity-and-type",
    "clue_dwellings": "residential-dwellings",
    "clue_floorspace_industry": "floor-space-by-block-by-clue-industry",
    "clue_floorspace_use": "floor-space-by-use-by-block",
    "clue_landmarks": "landmarks-and-places-of-interest-including-schools-theatres-health-services-spor",
}

# ── Open-Meteo weather ──────────────────────────────────────────────────────
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_PARAMS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
