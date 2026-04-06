# Melbourne CBD Street Analysis Pipeline — Overview

Last updated: 2026-03-31

---

## Pipeline Architecture

12-step ST-GNN pipeline. Steps 1–3 complete. Step 4 is current frontier.

```
Step 01  fetch          Raw data ingestion (API)                            [done]
Step 02  snap           Sensor snapping + CLUE aggregation → static_features [done]
Step 03  temporal       Temporal encoding + weather forward-fill             [done]
Step 04  graph          Spatial + semantic graph construction                ← next
Step 05  process        Parking occupancy + XGBoost ped imputation (graph-informed)
Step 06  aggregate      Aggregate ped activity + parking → street profiles
Step 07  cluster        GMM clustering on full street character
Step 08  cube           Data cube + dual graph construction
Step 09  train          MultiGCN training
Step 10  interpret      Feature importance + attention
Step 11  opportunities  Opportunity scoring + counterfactual
Step 12  export         Frontend JSON export
```

> **Note — weather:** Role of weather features needs further discussion. Currently injected in Step 03 and consumed in Step 05 imputation. May belong as a dynamic covariate in the GCN (Step 09) rather than an imputation feature — to be decided.

---

## Street Geometry

| Version | Source | Count | Notes |
|---------|--------|-------|-------|
| v1 | Hand-crafted GeoJSON polygons | 361 | Contained 10 pairs with 100% geometric overlap |
| v2 | Melbourne Open Data road-corridors API | 3,975 | Official segments, 122 multi-part merged into MultiPolygon |

**Key decision:** Switched to official road-corridor segments (seg_id) as the pipeline's `street_id`.
- Parking sensor match rate: 96.3% → 100%
- CLUE dwelling match rate: 1,298 → 10,486 matched points

---

## Data Quality Fixes (Steps 01–02)

### CLUE API truncation
Melbourne Open Data v2.1 silently caps `offset` at 10,000.
Business, buildings, and dwellings datasets all exceeded this.
**Fix:** Split requests by `block_id` range (6 splits); each split < 10K rows.

### Cafe count inflation (~45%)
Indoor and outdoor seating were separate rows for the same venue.
**Fix:** `_dedup_cafes()` groups by `trading_name + lat + lon` before snapping.
Max cafe count per street: 56 → 42.

### Duplicate street polygons
10 files in `data/polygons/` were exact geometric duplicates (100% overlap).
Attributes were split across pairs, inflating feature values on one half.
**Fix:** Deleted duplicate polygon files; rebuilt `streets.geojson`.
Affected dwellings re-collapsed onto correct street (3,578 units on street 9).

---

## Static Features

Columns snapped to each of 3,975 streets:
- `poly_area` — street corridor area (m²)
- `business_count` — number of business premises
- `cafe_count` — unique cafes/restaurants (deduplicated)
- `dwelling_count` — residential dwellings
- `building_count` — buildings
- `parking_bays` — parking bay count
- `tram_stops`, `bike_share_stations`, `ev_chargers`
- `road_length_m`, `lane_count`
- Plus spatial context from CLUE block-level data

---

## Step 04 — Pedestrian Imputation Model

### Task
Train a model on 82 sensored streets to predict pedestrian flow
for ~3,893 unsensored streets across 14,400 time bins (150 days × 96 bins/day).

### Features (26 cols after v2)
- Static: ~15 cols (street geometry, CLUE land use, parking stats)
- **New v2:** `parking_mean`, `parking_std`, `has_parking` (street-level occupancy signal)
- Temporal: 7 cols (hour_sin/cos, day_sin/cos, is_weekday, etc.)
- **New v2:** `hour_sin_x_weekday`, `hour_cos_x_weekday` (interaction terms)
- Weather: 4 cols (temperature, rainfall, wind, humidity)

### Model History

| Version | CV Method | Target | Hyperparams | Overall R² | Median per-street R² | Confidence Tier |
|---------|-----------|--------|-------------|-----------|---------------------|-----------------|
| v1 | Per-street 3-fold, same model | raw count | n=300, lr=0.1, depth=6 | — | 0.570 | 0.5 |
| v2 | GroupKFold(5) over streets | log1p(count) | n=500, lr=0.05, depth=5, mcw=3 | 0.551 | 0.043 | 0.5 |
| v3 | GroupKFold(5) over streets | log1p(count) | same + centroid_lat/lon in static | 0.571 | 0.162 | 0.5 |

**v1 problems identified:**
1. CV methodology was testing temporal fit on a single street — not generalisation to new streets. GroupKFold over street groups is the correct test.
2. Raw ped counts are heavily right-skewed (median ~12, max ~800+). log1p transform helps XGBoost split quality.
3. Parking occupancy (a strong spatial signal) was not used as a feature in Part B despite being computed in Part A.
4. No interaction features between time-of-day and weekday/weekend.

**Target threshold:** Overall GroupKFold R² ≥ 0.6 → confidence tier 0.8 for imputed streets.

---

## Sensor Coverage

| Sensor Type | Count | Streets Mapped |
|-------------|-------|---------------|
| Pedestrian counters | ~82 | 82 streets (sensored) |
| Parking bays | ~3,000+ | ~171 streets (≥5 events) |
| CLUE land use | 3,975 blocks | 3,975 streets |

---

## Frontend Visualization

File: `sensor_map_viz.html` (Mapbox GL JS)

Layers:
- Street polygons (color-coded by `str_type`: arterial/blue, minor/green, private/yellow)
- Pedestrian sensor locations (marker pins)
- Parking sensor locations
- **Ped-sensored fill** — gradient green→red on streets with real sensor data
- **Ped-imputed fill** — same gradient, different outline, for XGBoost-predicted streets
- Hover: shows mean_ped_flow in top-right label
- Click: shows avg flow, source (sensor/xgboost), confidence tier

---

## Key Decisions & Rationale

| Decision | Rationale |
|----------|-----------|
| 15-min time bins (not hourly) | Captures lunch/commute spikes; aligns with parking sensor resolution |
| 150-day study window | Covers full seasonal variation without excessive memory |
| 2-hour parking event cap | Prevents all-day parked vehicles from dominating occupancy |
| log1p target transform | ped_flow is Poisson-like; log scale linearises the relationship for tree splits |
| GroupKFold over streets (not time) | The prediction task is spatial extrapolation, not temporal forecasting |
| Confidence tiers (1.0 / 0.8 / 0.5) | Downstream GCN attention weights; lower confidence = down-weighted in loss |
