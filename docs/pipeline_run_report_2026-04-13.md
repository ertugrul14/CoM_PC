# Melbourne CBD Street Analysis Pipeline — Run Report
**Date:** 2026-04-13  
**Run ID:** Steps 01–09 (staged execution)  
**Produced by:** Pipeline log analysis of `terminaloutput.txt`

---

## 1. High-Level Overview

This pipeline run ingested, processed, and modelled multi-source sensor and administrative data for 3,975 street segments in Melbourne's Central Business District. The computational work was structured as nine sequential steps, each executed separately across a single day (2026-04-13, approximately 09:09 to 15:48+). The pipeline did not run as a single monolithic job; it was invoked five times via `run_pipeline.py` with step arguments (`1`, `2 4`, `5`, `6`, `7`, `8`, `9`). Steps 10–12 (feature interpretation, scenario simulation, and export) were not run in this session.

The core question the pipeline addresses is: **how are Melbourne CBD streets actually used, when are they busy, and what does that tell us about the feasibility of reallocating curb space?** The inputs are parking sensor events (individual bay-level state changes), pedestrian counter readings (hourly footfall at fixed counting stations), meteorological data, and the City of Melbourne's CLUE census of land use and employment. From these, the pipeline constructs a street-level picture of activity at 15-minute resolution across a 150-day window (14,400 bins), groups streets into behavioural archetypes, and trains a spatio-temporal graph neural network (ST-GNN) to forecast short-term pedestrian demand. Parking demand forecasting via a joint prediction head was also attempted in this run; that head produced non-functional outputs (see Step 09).

For a non-technical reader: this system takes raw data from parking meters and pedestrian counters and, step by step, builds a detailed map of how each street in Melbourne's CBD is used across the day and week. It then groups streets into types (e.g., busy at lunchtime, busy late at night, quiet overall) and teaches a machine learning model to predict future pedestrian volumes. The outputs are designed to support decisions about where curb space could potentially be converted to other uses.

The run produced all core artifacts through the data cube (Step 08) and began model training (Step 09), which was still in progress when the log ended at epoch 50 of a maximum 200. No fatal errors occurred. Key limitations encountered include: a complete failure to locate tram stop proximity data (zero tram stops found in the bounding box), a poorly-fit XGBoost pedestrian imputation model (median per-street R² = 0.116, only 9 of 67 sensored streets exceeding R² = 0.6), and a parking prediction head in Step 09 that is numerically non-functional (R² oscillating between −8,434 and −98 across epochs, indicating the model is predicting near-constant zero rather than learning parking dynamics).

---

## 2. Step-by-Step Breakdown

---

### Step 01: Data Fetching

**Start time:** 09:09:45 | **End time:** ~09:26:38

#### Purpose (technical)
Retrieves all raw data from external sources — Supabase (pedestrian counters), Open-Meteo API (weather), and Melbourne Open Data (CLUE administrative datasets and transport infrastructure) — and writes them to local Parquet/GeoJSON files. For large CLUE datasets exceeding the API's 10,000-row response limit, the step applies two-stage filtering: first by `census_year=2024`, then by splitting on `block_id` ranges to stay within API limits.

#### Purpose (real-world meaning)
This step collects all the raw information the pipeline needs: foot traffic counts from the city's pedestrian sensors, hourly weather observations, and the City of Melbourne's official census of every business, building, cafe, bar, dwelling, off-street car park, job, and landmark in the CBD.

#### Inputs and assumptions
- Supabase instance (connection string in config): source for pedestrian sensor readings
- Open-Meteo API: hourly meteorological data
- Melbourne Open Data API: CLUE datasets, filtered to `census_year=2024`
- PTV GTFS API or similar: public transport stops
- Assumption: `census_year=2024` is the most current CLUE data available and is representative of the study period

#### Operations performed
- Fetches pedestrian raw data from Supabase: single bulk query
- Fetches weather from Open-Meteo: hourly time series over the study window
- Fetches 12 CLUE entity types from Melbourne Open Data API, applying year filter and block-range pagination where total count exceeds 10,000
- Fetches PTV stop GeoJSON (~7.8 MB) and Melbourne City bus stop records
- Writes all results to `data/raw/` as Parquet (tabular) or GeoJSON (spatial)

#### Concrete outputs from this run

| File | Format | Records |
|---|---|---|
| `ped_raw.parquet` | Parquet | 8,571,919 rows |
| `weather_raw.parquet` | Parquet | 3,600 rows |
| `clue_cafe.parquet` | Parquet | 3,249 records (filtered from 66,356 total) |
| `clue_bar.parquet` | Parquet | 5,304 records |
| `clue_business.parquet` | Parquet | 19,672 records (filtered from 413,550; block-split fetch) |
| `clue_buildings.parquet` | Parquet | 14,094 records (filtered from 305,557; block-split fetch) |
| `clue_jobs.parquet` | Parquet | 603 records |
| `clue_blocks.geojson` | GeoJSON | 603 records |
| `clue_offstreet.parquet` | Parquet | 7,185 records |
| `clue_dwellings.parquet` | Parquet | 10,516 records (block-split fetch) |
| `clue_floorspace_industry.parquet` | Parquet | 603 records |
| `clue_floorspace_use.parquet` | Parquet | 603 records |
| `clue_landmarks.parquet` | Parquet | 242 records |
| `ptv_stops.geojson` | GeoJSON | ~7,998 KB |
| `bus_stops.parquet` | Parquet | 309 records |

Total: 16 files written to `data/raw/`.

The `clue_business` dataset (413,550 total historical records) required block-range splitting into 6 batches after year-filtering to 19,672 rows still exceeded the API limit. This process took approximately 7 minutes due to API pagination.

#### Sample records / data snippets
`weather_raw.parquet` contains 3,600 rows at hourly resolution, implying a 150-day temporal window (3,600 ÷ 24 = 150 days). Temperature range logged in Step 03 as [6.4, 43.3]°C, consistent with a Melbourne calendar window spanning both winter and summer.

`ped_raw.parquet` at 8.57 million rows represents the raw event log from pedestrian counting sensors — each row is a timestamped count reading at a specific sensor location.

#### Terminology for a newcomer
- **CLUE:** Census of Land Use and Employment — City of Melbourne's biannual survey of all premises in the CBD by land use type, floor area, and employment.
- **census_year=2024:** CLUE data snapshot from 2024, used as a static proxy for current land use during the study period.
- **PTV:** Public Transport Victoria — state transport authority that publishes stop and route data.

---

### Step 02: Sensor Snapping and CLUE Spatial Aggregation

**Start time:** 10:59:20 | **End time:** ~10:59:27

#### Purpose (technical)
Assigns each sensor (parking bay, pedestrian counter) to its containing or nearest street polygon using point-in-polygon (PIP) matching, with nearest-neighbour fallback. Aggregates CLUE point records to street polygons using nearest-neighbour join with a 50-metre distance cap. Enriches the 3,975-street feature table (`static_features`) with sensor assignments and land-use counts.

#### Purpose (real-world meaning)
This step answers "which street does each parking sensor or footfall counter belong to?" and "how many cafes, bars, businesses, and jobs are near each street?" It translates raw point coordinates into street-level summaries.

#### Inputs and assumptions
- `data/raw/` files from Step 01
- Melbourne street polygon layer: 3,975 polygons (source: Melbourne Open Data streets dataset, EPSG:3111)
- Assumption: nearest street within 50 metres is the correct attribution for CLUE points
- Assumption: pedestrian sensor API provides the authoritative coordinates for the 145 sensor network locations (only 99 are present in Supabase data)

#### Operations performed
- PIP matching: each sensor coordinate tested against 3,975 street polygons
- Nearest-neighbour fallback for 35 parking sensors that failed PIP (matched to nearest street polygon)
- For pedestrians: API coordinate lookup (145 network locations), then PIP matching on the 99 that appear in Supabase data — 67 matched by PIP, 32 fell outside all polygons (street coverage gap)
- CLUE point records matched to streets via `nearest(max_dist=50m)` join per dataset type
- Block-level CLUE datasets (jobs, floorspace) joined via spatial block-to-street intersection
- Centroid computation for street polygons (performed in WGS84 — see warning below)

#### Concrete outputs from this run

Sensor mapping (`sensor_map`): 2,080 rows total (1,981 parking + 99 pedestrian)

| Sensor type | Total | PIP matched | Unmatched (fallback) | Final match rate |
|---|---|---|---|---|
| Parking | 1,981 | 1,946 | 35 (nearest fallback) | 100% |
| Pedestrian | 99 | 67 | 32 (outside polygons) | 98% |

Note: 32 pedestrian sensors had no containing street polygon. These are either in plazas, laneways, or at intersections not represented as polygon segments. Their data is retained but without spatial attribution; they contribute to the raw ped dataset but cannot be used for street-level training.

CLUE aggregation matched counts:

| Dataset | Input rows | Matched to streets |
|---|---|---|
| clue_cafe | 2,248 unique venues | 2,132 |
| clue_bar | 304 | 295 |
| clue_business | 14,798 (pre-filtered) | 13,907 |
| clue_buildings | 14,094 | 13,831 |
| clue_offstreet | 7,185 | 6,982 |
| clue_dwellings | 10,516 | 10,486 |
| clue_landmarks | 242 | 181 |

Output: `static_features.parquet` — 3,975 streets × 18 columns.

Top 5 streets by `total_jobs`: IDs 30081 (5,063), 22555 (2,939), 30082 (2,693), 20001 (2,209), 30077 (2,063).  
Top 5 streets by `cafe_total_seats`: IDs 22180 (5,116), 20040 (3,038), 22879 (2,765), 30743 (2,470), 30729 (2,180).

**Warning:** Centroid computation for street polygons was performed in a geographic CRS (WGS84 degrees) rather than a projected CRS (e.g. EPSG:3111). GeoPandas issued two `UserWarning` messages about this. In Melbourne at ~37.8°S, the angular-to-metric distortion is small (~0.1%) but not zero; centroid coordinates are slightly imprecise. This affects downstream proximity-based joins but is unlikely to produce material street mis-attributions.

#### Terminology for a newcomer
- **Point-in-polygon (PIP):** geometric test that checks whether a point coordinate lies inside a polygon boundary.
- **Street polygon:** each street segment is represented as a polygon (not a line), capturing the road surface area including footpaths.
- **static_features:** a table where each row is one street, and columns are time-invariant descriptors (land use counts, sensor assignments, geometry metadata).

---

### Step 03: Temporal Encoding and Weather Forward-Fill

**Start time:** 10:59:27 | **End time:** ~10:59:27 (< 1 second)

#### Purpose (technical)
Constructs two reference tables indexed by time bin (15-minute resolution) covering the full 150-day study window (14,400 bins): (1) a weather table by forward-filling hourly Open-Meteo observations into 15-minute bins, and (2) a temporal features table encoding cyclical calendar signals (hour of day, day of week), public holiday flags, and school holiday flags.

#### Purpose (real-world meaning)
This step creates the "calendar" and "weather" columns that the model will use to understand what time of day and year it is when predicting pedestrian volumes. For example, a public holiday weekend at 6pm with 35°C heat is a very different context for street activity than a Tuesday morning in winter.

#### Inputs and assumptions
- `weather_raw.parquet` (3,600 hourly rows)
- Hard-coded or config-driven public holiday and school holiday calendars for Victoria
- Assumption: Open-Meteo data has no gaps requiring interpolation (forward-fill is sufficient)
- Temporal window: 14,400 bins × 15 minutes = 150 days

#### Operations performed
- Hourly weather resampled to 15-minute bins via forward-fill
- Sinusoidal encoding of hour-of-day and day-of-week to avoid ordinal artifacts (`hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`)
- Binary flags: `is_weekend`, `is_public_holiday`, `is_school_holiday`

#### Concrete outputs from this run
- `weather.parquet`: 14,400 rows; temperature range [6.4, 43.3]°C across the study window
- `temporal_features.parquet`: 14,400 rows; 576 public-holiday bins (= ~6 days), 4,860 school-holiday bins (= ~50.6 days)

#### Terminology for a newcomer
- **Sinusoidal encoding:** representing cyclic values like hour-of-day (0–23) as sin/cos pairs so that hour 23 is numerically close to hour 0, avoiding a discontinuity that would confuse a linear model.
- **Forward-fill:** carrying the last observed hourly weather value forward into subsequent 15-minute bins until the next hourly observation.

---

### Step 04: Spatial and Semantic Graph Construction

**Start time:** 10:59:27 | **End time:** ~10:59:35

#### Purpose (technical)
Constructs two graphs over the 1,397 eligible street nodes: (1) a spatial graph using k=8 k-NN on Euclidean centroid distances with a Gaussian kernel for edge weights, and (2) a semantic graph connecting streets with similar land-use profiles using a per-feature log-ratio similarity rule. Also computes betweenness centrality and enriches `static_features` with transit proximity features.

#### Purpose (real-world meaning)
This step defines which streets are "neighbours" to which other streets. Physical neighbours are streets that are close together geographically. Semantic neighbours are streets that have a similar mix of land uses (e.g., two cafe-heavy streets far apart from each other). These graphs are what allow the ST-GNN in Step 09 to propagate information across the street network.

#### Inputs and assumptions
- `static_features.parquet` (3,975 streets × 23 cols after enrichment)
- Street type filter: only arterial-class streets retained; intersection segments excluded
  - Filter result: 2,111 type-qualified → 718 intersection segments removed → **1,397 eligible streets**
- k=8 for spatial k-NN (each street connected to its 8 nearest geographic neighbours)
- Semantic eligibility: ≥3 active land-use features; similarity rule: ≥2 shared features with log1p ratio ≥ 0.7 across all shared features
- `ptv_stops.geojson` and `bus_stops.parquet` for transit proximity
- CRS: EPSG:3111 for distance computations

#### Operations performed
- **Spatial graph**: k=8 k-NN computed over 1,397 street centroids in EPSG:3111; edge weight = Gaussian kernel exp(-d²/2σ²) with σ=131.2m; produces 13,160 directed edges (bidirectional = 6,580 undirected pairs)
- **Semantic graph**: 352 streets qualify (have ≥3 active features); 7,040 candidate pairs generated; after feature-ratio filter → 1,831 pairs survive → 2,230 directed edges (bidirectional = 1,115 undirected pairs); edge weights in [0.760, 0.997]
- **Betweenness centrality**: computed on spatial graph; max = 0.0982 (low, expected in a grid-like CBD network)
- **Transit enrichment**: tram stop proximity from `ptv_stops.geojson` — **0 tram stops found in bbox** (warning issued, all streets assigned `nearest_tram_stop_m = 9999`)
- Bus stop proximity from `bus_stops.parquet`: 197 streets matched to a direct bus stop

#### Concrete outputs from this run
- `spatial_edges.parquet`: 13,160 edges
- `semantic_edges.parquet`: 2,230 edges
- `graph_viz.geojson`: 1,397 nodes + 6,580 spatial + 1,115 semantic edges (undirected for visualisation)
- `static_features.parquet` enriched: 3,975 streets × 23 cols (+5: `betweenness_centrality`, `nearest_tram_stop_m`, `tram_stops_200m`, `bus_stop_on_street`, `nearest_bus_stop_m`)

**Critical warning — tram stop data missing:** Zero tram stops were found within the Melbourne CBD bounding box from `ptv_stops.geojson`. Melbourne's tram network is the largest in the Southern Hemisphere and is a primary mode of CBD access. This failure means `nearest_tram_stop_m` is set to 9,999m for all streets — effectively making this feature uninformative. The downstream model receives no signal from tram proximity. This is a data integrity issue, not a code error; it likely reflects a CRS mismatch, a bounding box definition error, or a change in the PTV GeoJSON format that the step did not detect. The feature is carried forward but should not be interpreted as meaningful.

**Note:** 1,075 of 1,397 streets (77%) have no semantic edges — meaning they are not connected to any other street by land-use similarity. These are low-activity or single-use streets. This is not a pipeline defect; the semantic graph is intentionally sparse.

#### Terminology for a newcomer
- **k-NN (k-nearest neighbours):** a method that connects each street to its k spatially closest streets; here k=8.
- **Gaussian kernel:** a weighting function that assigns higher weight to closer neighbours and decays smoothly with distance.
- **Betweenness centrality:** for each node in a graph, how often it lies on the shortest path between any two other nodes; a proxy for how structurally important a street is within the network.
- **Semantic graph:** an additional graph layer where edges connect streets with similar land use profiles, regardless of physical distance.
- **Bidirectional edges:** each connection is stored as two directed edges (A→B and B→A), so 13,160 directed edges = 6,580 street pairs.

---

### Step 05: Parking Occupancy Reconstruction and Pedestrian Imputation

**Start time:** 11:02:31 | **End time:** ~11:04:13

#### Purpose (technical)
**Part A:** Reconstructs per-bay, per-bin parking occupancy from raw state-change events using interval arithmetic, then aggregates to street-level occupancy rates at 15-minute resolution. Retains only streets with ≥5 parking events.

**Part B:** Trains an XGBoost regression model on the 67 streets with direct pedestrian sensor coverage, using temporal features, static features, weather, and a graph-informed spatial lag feature. Applies the trained model to impute pedestrian flow estimates for the remaining 1,330 unsensored streets. Assigns a data-quality confidence tier based on cross-validated R².

#### Purpose (real-world meaning)
Part A: For each street, this step calculates how full its parking bays were at each 15-minute interval throughout the study period — building a 150-day occupancy time series per street.

Part B: Only 67 of the 1,397 streets have actual pedestrian counters. For the other 1,330, this step estimates how busy they are at each time bin using a machine learning model trained on the streets that do have sensors. The model uses the time of day, day of week, weather, and land use characteristics of a street to make these estimates.

#### Inputs and assumptions
- Raw parking events with `street_id` (joined via sensor map from Step 02)
- `ped_raw.parquet` (8.57M rows)
- `static_features.parquet` (23 cols)
- `spatial_edges.parquet` (for spatial lag)
- `weather.parquet` and `temporal_features.parquet`
- Assumption: streets with fewer than 5 parking events are too sparse to reconstruct a reliable occupancy time series
- Assumption: XGBoost can generalise from 67 sensored streets to 1,330 unsensored streets — this is a strong assumption given the diversity of CBD street types

#### Operations performed
- **Part A:** Vectorised reconstruction of state-change events into per-interval occupancy; aggregation to 15-minute bins; capping of event durations to prevent unbounded intervals from bad state transitions
- **Part B:**
  - Feature matrix construction: temporal encodings (hour_sin/cos, `is_weekend`, weekend × hour interaction), static features, weather, spatial lag (mean ped flow of sensored graph-neighbours)
  - Training matrix shape: (964,800 rows × 39 features) — representing all 67 sensored streets × 14,400 time bins
  - Log1p transformation of target (raw pedestrian counts)
  - GroupKFold(5) cross-validation grouped by street (ensuring unseen streets in each fold)
  - Final model: XGBoost, 500 estimators
  - Batch prediction for 1,330 unsensored streets in batches of 300

#### Concrete outputs from this run
- `parking_occupancy.parquet`: 2,462,400 rows; 171 streets × 14,400 bins
- `ped_complete.parquet`: 20,116,800 rows; 1,397 streets × 14,400 bins; 67 at confidence=1.0

**Imputation quality — significant issue:**
| Metric | Value |
|---|---|
| GroupKFold R² (log scale, overall) | 0.585 |
| Per-street R² median | 0.116 |
| Per-street R² range | −27.033 to 0.843 |
| Streets with R² ≥ 0.6 | 9 / 67 (13%) |
| Confidence tier assigned to imputed streets | 0.5 |

The overall GroupKFold R² of 0.585 masks severe per-street heterogeneity. The median per-street R² is 0.116 — meaning for a typical street in the held-out fold, the model explains only 11.6% of temporal variance. Only 9 of 67 sensored streets exceeded the R²=0.6 quality threshold. This means the imputed pedestrian estimates for the 1,330 unsensored streets should be treated with significant caution: they capture broad time-of-day patterns but are not reliable for individual street-level magnitudes. The confidence tier of 0.5 (the lowest tier) is correctly assigned, but this degrades model supervision quality in Step 09.

A spatial lag feature was available for 337 of 1,397 streets (24%), meaning only that fraction of streets had at least one sensored spatial neighbour to borrow signal from.

**Note on parking vs. pedestrian street counts:** 171 streets have parking events (≥5 events threshold), but Step 06 reports only 138 streets with `mean_parking > 0` in the aggregated profiles. This discrepancy (171 vs 138) is not explained in the logs and may indicate that some of the 171 streets had events but zero computed occupancy (e.g. all events in a single state), or that occupancy was below the representation threshold after aggregation.

#### Terminology for a newcomer
- **Occupancy rate:** fraction of mapped parking bays that are occupied in a given 15-minute bin, as a value in [0, 1].
- **State-change event:** a parking sensor record indicating a transition from occupied to vacant or vice versa; used to infer how long each bay was occupied.
- **GroupKFold cross-validation:** a variant of k-fold CV that ensures all rows from the same street are always in the same fold, so the model is evaluated on streets it has never seen during training — a more honest estimate of generalisation to unsensored streets.
- **Spatial lag feature:** the average pedestrian count of a street's graph-connected sensored neighbours, used as an additional predictor.
- **Confidence tier:** a label (1.0 for direct sensor, 0.8 for R²≥0.6 imputation, 0.5 for R²<0.6 imputation) indicating how reliable the ped_complete time series is for a given street.

---

### Step 06: Aggregation to Street Profiles

**Start time:** 11:04:31 | **End time:** ~11:04:42

#### Purpose (technical)
Reduces the 14,400-bin time series for each street to a compact weekly temporal profile (42 features for parking, 42 for pedestrian) and computes 5 scalar summary scores (morning, midday, evening activity, weekend ratio, parking flexibility). Joins 9 static features. Output is a single 103-column profile table with one row per street.

#### Purpose (real-world meaning)
Instead of working with 150 days of 15-minute data, this step compresses each street's activity history into a typical week: a profile that shows the average pedestrian or parking level for each time slot across the 7 days of the week. This is the representation used for clustering in Step 07.

#### Inputs and assumptions
- `ped_complete.parquet` (1,397 streets × 14,400 bins)
- `parking_occupancy.parquet` (171 streets × 14,400 bins)
- `static_features.parquet`
- Assumption: a representative weekly average is sufficient to characterise a street's behavioural type; intra-week variability and seasonal trends are discarded
- Assumption: weekday and weekend patterns are sufficiently stable across the 150-day window to aggregate without temporal stratification

#### Operations performed
- Pivot of ped_complete to (streets × time bins) matrix; computation of average activity per day-of-week × time-block slot → 42 features
- Same aggregation for parking occupancy → 42 features
- Derived scores:
  - `score_morning`: mean ped activity in morning time block
  - `score_midday`: mean ped activity in midday block
  - `score_evening`: mean ped activity in evening block
  - `score_weekend_ratio`: ratio of weekend to weekday activity
  - `score_parking_flex`: a parking flexibility index (p50 = 1.000, meaning at least half of all streets have maximum flexibility score)
- Join of 9 static features from `static_features.parquet`

#### Concrete outputs from this run
- `street_profiles.parquet`: 1,397 streets × 103 columns
  - 42 parking profile features
  - 42 pedestrian profile features
  - 5 score columns
  - 4 summary columns
  - 9 static columns
  - 1 street_id column

Activity ranges:
| Metric | p50 or range |
|---|---|
| score_morning | [3.930, 409.463] |
| score_midday | [5.194, 1,059.815] |
| score_evening | [3.145, 764.749] |
| score_weekend_ratio | p50 = 0.901 |
| score_parking_flex | p50 = 1.000 |
| mean_ped | [3.4, 560.5] |
| mean_parking | [0.000, 0.001] |

The `mean_parking` range of [0.000, 0.001] is anomalous — this implies the parking occupancy signal is almost entirely zero in the aggregated profile, even for the 138 streets reported as having parking data. This likely reflects that the parking occupancy rate (bays occupied / total bays) is extremely low when the denominator includes many bays, or a unit normalisation issue introduced during reconstruction in Step 05. This is a red flag for any downstream analysis that relies on parking occupancy as a meaningful signal.

#### Terminology for a newcomer
- **Weekly profile:** a vector of 42 values representing the typical activity level at each of 6 time blocks × 7 days of the week, averaged over the full study period.
- **score_weekend_ratio:** weekend activity divided by weekday activity; a value near 1 means the street is equally busy on weekdays and weekends; higher means more weekend-oriented.
- **Temporal fingerprint:** another term for the weekly profile — captures *when* a street is busy, not absolute volume.

---

### Step 07: GMM Clustering on Street Character

**Start time:** 11:05:09 | **End time:** ~11:05:22

#### Purpose (technical)
Applies Gaussian Mixture Model (GMM) clustering — not K-Means — to the 1,397 street profiles. Dimensionality is first reduced from 89 input features to 20 PCA components (retaining 99.9% of variance). The number of clusters k is selected by minimising the Bayesian Information Criterion (BIC) over k ∈ {2, …, 10}. Cluster stability is assessed via bootstrapped Adjusted Rand Index (ARI). Each cluster is labelled with a named archetype.

#### Purpose (real-world meaning)
This step groups streets into types based on their usage patterns. The algorithm finds natural groupings by asking: which streets are most similar to each other in terms of when they are busy, how busy they are, and what their mix of land uses is?

#### Inputs and assumptions
- `street_profiles.parquet` (1,397 streets × 103 cols, subset of 84 temporal + 5 score features used)
- Assumption: GMM with diagonal or full covariance is appropriate for these data; the BIC score for the selected solution (k=7) is −52,669.8 vs −50,475.7 for k=8, a clear BIC improvement
- Assumption: the 20 PCA components adequately represent the clustering structure

#### Operations performed
- PCA: 89 features → 20 components (99.9% variance)
- GMM fit for k = 2 through 10; BIC computed for each
- BIC-selected solution: k=7 (BIC = −52,669.8)
- Bootstrap ARI for stability assessment
- Archetype labelling based on cluster centroid scores
- Computation of "flexibility windows" (time blocks where a street's parking or pedestrian demand is low enough to permit reallocation)

#### Concrete outputs from this run

BIC curve:
| k | BIC |
|---|---|
| 2 | −49,182.8 |
| 3 | −51,481.1 |
| 4 | −51,940.1 |
| 5 | −51,923.7 |
| 6 | −52,445.1 |
| **7** | **−52,669.8** ← selected |
| 8 | −50,475.7 |
| 9 | −50,143.0 |
| 10 | −50,190.7 |

Cluster assignment:
| Cluster | Archetype | Size | % of total |
|---|---|---|---|
| 0 | latent_evening_potential_medium | 113 | 8.1% |
| 1 | midday_retail_activation | 38 | 2.7% |
| 2 | major_pedestrian_corridor | 9 | 0.6% |
| 3 | morning_pedestrianisation_high | 34 | 2.4% |
| 4 | latent_evening_potential_high | 77 | 5.5% |
| 5 | morning_pedestrianisation_medium | 85 | 6.1% |
| 6 | latent_evening_potential_low | **1,041** | **74.5%** |

- ARI = 0.942 (bootstrap stability: very high; the k=7 solution is reproducible)
- Silhouette score = 0.448 (moderate; clusters have reasonable separation in PCA space)
- Warning: 3 streets have confidence < 0.7 (their cluster assignments are less reliable)
- Streets with flexibility windows: 1,114 of 1,397 (79.7%)

The dominant cluster (6, `latent_evening_potential_low`) contains 74.5% of all streets. This is not a sign of a poorly-behaved clustering — it reflects the actual composition of the CBD: the majority of streets are relatively low-activity and show some evening potential but not strongly enough to move into clusters 0 or 4. The 9-street `major_pedestrian_corridor` cluster (cluster 2) is the rarest and is likely capturing streets like Swanston, Bourke, or Collins with extreme footfall volumes.

- `clustered.parquet`: 1,397 streets × columns including `street_id`, `cluster`, `intervention_type`, `confidence`
- `cluster_summary.json`: 1,397-street summary

#### Terminology for a newcomer
- **GMM (Gaussian Mixture Model):** a probabilistic clustering algorithm that models the data as a mixture of Gaussian distributions; unlike K-Means, it assigns soft probabilities to cluster membership.
- **BIC (Bayesian Information Criterion):** a model selection criterion that penalises model complexity; lower BIC is better.
- **ARI (Adjusted Rand Index):** a measure of how similar two clustering solutions are to each other; 1.0 = identical, 0 = random; used here to check whether re-running the algorithm produces the same cluster assignments.
- **Silhouette score:** a value in [−1, 1] measuring how similar each point is to its own cluster vs. other clusters; 0.448 indicates moderate, interpretable separation.
- **Flexibility window:** a time block in which a street's measured or estimated activity is low enough that curb space could potentially be repurposed without displacement of existing users.

---

### Step 08: Data Cube Assembly and Graph Serialisation

**Start time:** 11:06:13 | **End time:** ~11:06:38

#### Purpose (technical)
Assembles the full spatio-temporal data cube `cube.npy` of shape (N=1,397 streets, T=14,400 time bins, F=23 features). Pivots `ped_complete` and `parking_occupancy` to (N×T) matrices, merges temporal/weather features, appends static features, and normalises using training-split statistics. Serialises graph adjacency matrices as PyTorch sparse tensors.

#### Purpose (real-world meaning)
This step packages all the data into the exact format the ST-GNN model expects: a 3D array where the three axes are "which street", "which 15-minute time slot", and "which feature". It also saves the two street graphs (physical neighbours, similar-use neighbours) as the network structure the GNN will use to propagate information.

#### Inputs and assumptions
- `ped_complete.parquet`, `parking_occupancy.parquet`
- `temporal_features.parquet`, `weather.parquet`
- `static_features.parquet`
- `spatial_edges.parquet`, `semantic_edges.parquet`
- `node_index.parquet` (canonical street ordering from Step 04)
- Assumption: normalisation statistics computed on the first 80% of timesteps only (11,520 of 14,400 bins, training split) to avoid data leakage from validation/test
- Note: cube_meta.json records `T_train_end=11520`, but Step 09 training log reports `Train: 0..10080` — a slight inconsistency in split boundary definitions between cube assembly and training (see Step 09 note)

#### Operations performed
- Pivot of ped_complete from long format (20.1M rows) to (1,397 × 14,400) matrix
- Pivot of parking_occupancy from long format (2.46M rows) to (1,397 × 14,400) matrix, zero-padded for non-parking streets
- Concatenation of 23 feature channels along axis 2
- Z-score normalisation of 15 continuous features using training-split mean/std
- Memory allocation: 1,397 × 14,400 × 23 × 4 bytes = 1.85 GB float32
- Spatial adjacency serialised to PyTorch sparse COO tensor (nnz=14,557)
- Semantic adjacency serialised to PyTorch sparse COO tensor (nnz=3,627)

#### Concrete outputs from this run
- `cube.npy`: 1.85 GB, shape (1,397 × 14,400 × 23), float32
- `graph_spatial.pt`: PyTorch sparse tensor, nnz=14,557
- `graph_semantic.pt`: PyTorch sparse tensor, nnz=3,627
- `cube_meta.json`: N, T, F, feature names, node order, normalisation split boundary
- `norm_stats.json`: per-feature mean and standard deviation from training split

The 23 features per node per time bin are:

| Feature | Type | Normalised? |
|---|---|---|
| ped_flow | dynamic (temporal) | Yes |
| occupancy_rate | dynamic (temporal) | Yes |
| ped_confidence | static-broadcast | No |
| hour_sin, hour_cos | temporal encoding | No |
| dow_sin, dow_cos | temporal encoding | No |
| is_weekend | flag | No |
| is_public_holiday | flag | No |
| is_school_holiday | flag | No |
| temperature_2m | dynamic weather | Yes |
| relative_humidity_2m | dynamic weather | Yes |
| wind_speed_10m | dynamic weather | Yes |
| precipitation | dynamic weather | Yes |
| total_jobs | static land use | Yes |
| cafe_count, cafe_total_seats | static land use | Yes |
| bar_count, bar_patron_capacity | static land use | Yes |
| business_count, poi_total | static land use | Yes |
| dining_capacity | static land use | Yes |
| area_m2 | static geometry | Yes |

#### Terminology for a newcomer
- **Data cube:** a 3-dimensional array indexed by [street, time bin, feature]; the fundamental input representation for the ST-GNN.
- **COO sparse tensor:** a format for storing graph adjacency matrices efficiently by recording only non-zero entries as (row, column, value) triples.
- **Data leakage:** using validation/test data statistics during normalisation would give the model indirect access to future information; computing normalisation on the training period only prevents this.

---

### Step 09: MultiGCN Training (GRU + Dual GCN, Joint Prediction Heads)

**Start time:** 11:07:28 | **Log truncates at epoch 50:** ~15:48:33 (training still in progress)

#### Purpose (technical)
Trains a spatio-temporal graph neural network (ST-GNN) with architecture: GRU encoder → dual GCNConv branches (one for spatial graph, one for semantic graph) → fusion → two prediction heads (pedestrian flow head, parking occupancy head). The model takes a 96-bin (24-hour) input window and predicts subsequent pedestrian and parking demand. Training is joint across both heads with a configurable weighting parameter.

The previous training run (ped-only, single head) checkpoint was backed up as `best_model_ped_only.pt` before this run commenced.

#### Purpose (real-world meaning)
This is the forecasting model. Given the last 24 hours of pedestrian counts, parking occupancy, weather, and time-of-day context for every street in the graph, the model tries to predict what pedestrian volumes will be next. It also attempts to predict parking occupancy, but that prediction head is not producing useful outputs in this run.

#### Inputs and assumptions
- `cube.npy` (1.85 GB)
- `graph_spatial.pt`, `graph_semantic.pt`
- `norm_stats.json`
- Time split: Train = bins 0–10,080 (70.0%), Val = 10,080–12,240 (15.0%), Test = 12,240–14,400 (15.0%)
- Key hyperparameters: `window=96` (24h look-back), `hidden=64`, `batch=8`, `max_epochs=200`, `steps_per_epoch=256`, `park_weight=0.5`
- Device: **CPU** (no GPU; each epoch takes approximately 4–5 minutes on this hardware)
- Parking mask: 138 / 1,397 streets (only these contribute to parking head loss)

**Note on split boundary discrepancy:** `cube_meta.json` records `T_train_end=11520`, but the training log reports `Train: 0..10080`. This implies the training script uses its own split boundaries independent of the cube assembly metadata. The practical difference is small (11,520 vs 10,080 = 960 bins = 10 days), but it means the "training data" seen by the model is slightly shorter than what `norm_stats.json` was computed on.

#### Operations performed
- PyTorch non-writable numpy array warning on parking mask construction (non-critical; suppressed after first occurrence)
- Model instantiation: 68,076 trainable parameters (a small model by GNN standards)
- For each epoch: sample 256 random (street-batch, window) pairs; compute GRU encoder over 96-step input; run dual GCNConv; fuse spatial and semantic branch embeddings; apply ped head and parking head; compute weighted loss
- Pedestrian loss: MSE on normalised ped_flow predictions
- Parking loss: MSE on normalised occupancy_rate predictions (only for 138 masked streets)
- Loss = ped_loss + park_weight × parking_loss (park_weight = 0.5)
- Best model checkpoint saved by validation loss

#### Concrete outputs from this run (partial — training ongoing)

Training progress logged at epochs 1, 10, 20, 30, 40, 50:

| Epoch | Train loss | Ped MAE | Ped R² | Park MAE | Park R² |
|---|---|---|---|---|---|
| 1 | 0.3235 | 16.038 | 0.296 | ≈0.000 | −8,434.9 |
| 10 | 0.1409 | 8.369 | 0.759 | ≈0.000 | −530.4 |
| 20 | 0.1193 | 7.636 | 0.811 | ≈0.000 | −98.5 |
| 30 | 0.1080 | 6.951 | 0.840 | ≈0.000 | −1,995.3 |
| 40 | 0.0957 | 6.463 | 0.854 | ≈0.000 | −292.6 |
| 50 | 0.0945 | 6.324 | 0.867 | ≈0.000 | −357.6 |

**Pedestrian head:** Training is proceeding normally. Ped MAE has decreased from 16.0 to 6.3 (pedestrians per 15-minute bin) and R² has improved from 0.296 to 0.867 at epoch 50. This is a good learning trajectory, though convergence has not been reached and the final performance is unknown.

**Parking head — critical failure:** The parking head is producing near-zero MAE (≈0.000) with wildly negative R² oscillating between −8,435 and −98. This is the signature of a model that is predicting a near-constant value close to the target mean (approximately zero, because `mean_parking` was already near 0.000–0.001 as observed in Step 06) and thus achieves low absolute error but zero explanatory power. The parking signal is almost entirely zero-valued for 1,259 of 1,397 streets, and for the 138 masked streets the occupancy values are near-zero (as flagged in Step 06). The parking head is not learning anything useful. Its loss contribution is real but it is not guiding the model toward parking prediction.

**Hardware note:** Training is running on CPU. At approximately 4–5 minutes per epoch, completing all 200 epochs will take roughly 13–16 hours of compute time from the 11:07 start, putting expected completion around 00:00–03:00 on 2026-04-14.

**Pre-training file:** `best_model_ped_only.pt` — backup of a previous ped-only trained checkpoint, retained before this joint-head run overwrote the primary checkpoint.

#### Terminology for a newcomer
- **GRU (Gated Recurrent Unit):** a recurrent neural network cell that encodes temporal sequences; here, encodes the 24-hour (96-bin) input window for each street.
- **GCNConv (Graph Convolutional Network convolution):** a graph neural network layer that aggregates features from neighbouring nodes weighted by edge weights.
- **Dual GCN:** two separate GCNConv layers, one per graph (spatial, semantic), whose outputs are fused before the prediction heads.
- **Prediction head:** a small feedforward layer attached to the fused embedding to predict a specific output (pedestrian flow, parking occupancy).
- **MAE (Mean Absolute Error):** average absolute difference between predicted and actual values; here in units of pedestrians per 15-minute bin.
- **R² (coefficient of determination):** fraction of variance explained by the model; R²=1.0 is perfect; R²=0 means the model does no better than predicting the mean; R²<0 means the model is worse than predicting the mean.

---

## 3. Cross-Cutting Behaviour and Limitations

### Key configuration values

| Parameter | Value | Practical meaning |
|---|---|---|
| Temporal resolution | 15 minutes | All time series at 96 bins/day; smallest observable event duration is 15 min |
| Study window | 14,400 bins = 150 days | ~5 months; captures seasonal variation but not a full year |
| Street count (total) | 3,975 polygons | Full Melbourne CBD streets layer |
| Street count (modelled) | 1,397 | After arterial filter + intersection removal |
| Ped sensor coverage | 67 / 1,397 (4.8%) | Only 4.8% of modelled streets have direct pedestrian sensor data |
| Parking sensor coverage | 171 / 1,397 (12.2%) | 12.2% of modelled streets have parking event data |
| Imputation R² threshold | 0.6 | Confidence=0.8 if met, else 0.5; threshold not met overall (R²=0.585) |
| GNN look-back window | 96 bins = 24 hours | Model only sees the past 24 hours of data when forecasting |
| Clustering algorithm | GMM | Soft assignment; BIC-selected k=7 |
| Spatial k-NN | k=8 | Each street connected to its 8 nearest geographic neighbours |
| Semantic similarity | log1p ratio ≥ 0.7 | Only land-use-similar streets are connected; 77% of streets have no semantic edges |
| Joint training weight | park_weight = 0.5 | Parking loss contributes equally to ped loss in the training objective (despite failing) |

### Limitations, stated plainly

1. **Pedestrian imputation is unreliable at the street level.** The XGBoost imputation model achieves a median per-street R² of 0.116 across sensored streets in held-out folds. Only 9 of 67 sensored streets exceed R²=0.6. The imputed pedestrian values for the 1,330 unsensored streets reflect broad temporal patterns (time of day, day of week) but not street-specific dynamics. Cluster assignments, model training targets, and opportunity scores for these streets are based on low-quality estimates.

2. **Parking occupancy signal is near-zero and uninformative.** The mean_parking range of [0.000, 0.001] in the street profiles, combined with the parking head's inability to learn (R² oscillating around extreme negative values), indicates that the parking occupancy reconstruction has produced a near-constant signal. This may be a unit normalisation issue (e.g., occupancy rate normalised by total bay count rather than per-street sensor count) or a genuine reflection of very low aggregate occupancy. Regardless, parking-based features are not contributing usefully to the model or profiles.

3. **No tram proximity data.** Zero tram stops were located within the Melbourne CBD bounding box. The `nearest_tram_stop_m` and `tram_stops_200m` features are set to 9,999 and 0 respectively for all streets. Tram stop proximity is a primary determinant of pedestrian activity in Melbourne's CBD and its absence is a material gap in the feature set.

4. **The ST-GNN training is incomplete.** The log ends at epoch 50 of 200. The model has not converged. Ped R²=0.867 at epoch 50 is on the training data; validation performance is not logged. No final checkpoint or test-set evaluation has been produced in this run.

5. **All imputation is non-causal and correlative.** The XGBoost model learns associations between land use and pedestrian patterns observed in sensored streets. It cannot account for unobserved factors (local events, construction, new businesses) that affect unsensored streets differently.

6. **GMM clustering is based partly on imputed data.** 95.2% of streets (1,330 / 1,397) have imputed rather than sensored pedestrian profiles. The cluster archetypes — including the assignment of 74.5% of streets to `latent_evening_potential_low` — are therefore substantially determined by the XGBoost model's outputs, not by direct observation.

7. **The semantic graph excludes 77% of streets.** Streets without ≥3 active land-use features have no semantic connections. These streets receive no benefit from the semantic GCN branch, and the spatial-only graph may not adequately capture their structural context.

8. **Centroid computation in geographic CRS.** Step 02 computed street polygon centroids in WGS84 (angular coordinates), not EPSG:3111 (metres). This introduces minor geometric imprecision in all subsequent proximity joins and distance calculations.

9. **Training hardware constraint.** CPU-only training at ~5 minutes per epoch makes full training (200 epochs) a ~16-hour wall-clock job. This constrains iteration speed and makes hyperparameter search impractical without a GPU.

10. **No real-time component.** The entire pipeline is batch-only. All data represents historical observations. There is no live data feed, no streaming inference, and no mechanism for updating the model as new sensor data arrives.

---

## 4. What This Pipeline Actually Tells You

**What a planning or policy stakeholder can credibly use from this run:**

- **Temporal activity profiles for 1,397 streets, at 15-minute resolution, over a 150-day window.** For the 67 streets with direct pedestrian sensors, these are reliable. For the remaining 1,330, the profiles capture broad time-of-day and day-of-week patterns but have poor street-level precision (R²=0.116 median).

- **Cluster archetype labels for 1,397 streets.** The 7-archetype GMM solution (ARI=0.942, silhouette=0.448) is stable and statistically well-supported. The labels (e.g., `major_pedestrian_corridor`, `midday_retail_activation`, `morning_pedestrianisation_high`) describe when a street is most active relative to other streets. These can inform where different types of interventions (e.g., morning delivery zones vs. evening activation spaces) might be feasible. However, because 95% of streets rely on imputed pedestrian data, archetype assignments for non-sensored streets are estimates.

- **A ranked list of 1,114 streets with identified flexibility windows.** These are time blocks where observed or estimated activity is low enough that curb reallocation would not immediately displace current users. This is a screening tool, not a design decision. Site-specific validation is required before any physical intervention.

- **Land use profiles for all 3,975 streets**, including jobs count, seating capacity, bar patronage, building count, and dwelling count, matched from the 2024 CLUE census. These are the highest-quality static features in the dataset; they are not model-derived.

- **An ST-GNN trained to 867 epochs on pedestrian flow.** At epoch 50, the model achieves Ped MAE ≈ 6.3 persons per 15-minute bin and R²=0.867 on training data. This is suitable for short-horizon operational forecasting (next 15–60 minutes) on the 67 sensored streets and potentially adjacent streets. Final test-set performance is not yet available.

**What cannot be credibly inferred from this run:**

- **Causal effects of specific policy interventions.** The model has learned historical patterns under current conditions. It cannot estimate what would happen to pedestrian flows if curb space were removed or reallocated, if a bike lane were added, or if a loading zone were relocated. Any such counterfactual requires an intervention study design that this pipeline does not implement.

- **Reliable street-specific pedestrian volumes for the 1,330 non-sensored streets.** The imputation model explains a median of 11.6% of street-level temporal variance. These estimates are systematically calibrated to the 67 sensored streets and should not be treated as individually accurate.

- **Parking demand as a meaningful signal.** The parking occupancy reconstruction produced near-zero values across the profile table. The parking prediction head in Step 09 is not learning. Parking-based opportunity scores, if computed, cannot be relied upon until this is diagnosed and corrected.

- **Tram-related accessibility effects.** The `nearest_tram_stop_m` feature is uniformly set to 9,999m for all streets due to a data retrieval failure. Any analysis depending on tram proximity (which is a primary driver of Melbourne CBD footfall) is missing this signal.

- **Seasonal or annual trends.** The 150-day window captures parts of multiple seasons but not a full annual cycle. Weekly patterns are well-represented. Phenomena that operate at monthly or seasonal scales (e.g., summer tourism peaks, winter drop-off) are partially captured but not fully modelled.

- **The final trained model's performance.** Training was ongoing at epoch 50 of 200 when the log ended. The test-set R² and MAE are not yet available. Until training completes and test-set evaluation is run, the model's generalisation quality is unknown.

---

*End of report. Run date: 2026-04-13. All figures sourced directly from `terminaloutput.txt`.*
