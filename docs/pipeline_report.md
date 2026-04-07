# Melbourne CBD Curbside Reallocation Pipeline
## Comprehensive Technical Report

**Project:** Master's thesis — spatio-temporal analysis of Melbourne CBD streets  
**Research question:** *How can we reallocate curbside functions based on streets' temporal behaviour with the assistance of data-driven methods?*  
**Pipeline location:** `melbourne_pipeline/`  
**Entry point:** `python melbourne_pipeline/run_pipeline.py [step_start] [step_end]`

---

## Architecture Overview

The pipeline is a 12-step sequence that transforms raw sensor data into trained model outputs and interactive frontend exports. Steps are strictly sequential; each step writes named parquet/JSON/numpy files that the next step reads.

```
Step 01  FETCH       Raw data download (Supabase, Open-Meteo, Melbourne Open Data)
Step 02  SNAP        Sensor geolocation + CLUE land-use aggregation
Step 03  TEMPORAL    Weather forward-fill + cyclic time encodings
Step 04  GRAPH       Spatial k-NN graph + semantic activity-profile graph
Step 05  PROCESS     Parking occupancy reconstruction + XGBoost pedestrian imputation
Step 06  AGGREGATE   Weekly temporal profiles per street (84 features)
Step 07  CLUSTER     GMM clustering → street archetypes + flexibility windows
Step 08  CUBE        [N × T × F] data cube + normalised adjacency matrices
Step 09  TRAIN       MultiGCN model training (GRU + dual GCNConv)
Step 10  INTERPRET   Permutation feature importance + branch contribution
Step 11  SCENARIO    Counterfactual intervention simulation
Step 12  EXPORT      Frontend GeoJSON/JSON export
```

**Study domain:** 1,397 Arterial and Council Major street segments in Melbourne CBD  
**Study period:** 2025-11-01 to 2026-03-30 (14,400 × 15-min bins)  
**CRS:** EPSG:3111 (Victorian GDA94) for spatial operations; WGS84 for storage/export

---

## Step 01 — Data Fetch (`step_01_fetch.py`)

### Purpose
Downloads all raw data from three external sources and writes it to `data/raw/`. All fetches are idempotent — re-running overwrites cleanly.

### Data Sources

#### A. Supabase (parking + pedestrian sensor events)
- **Method:** Keyset pagination via PostgREST REST API using httpx. Pages through rows ordered by `id`, taking 1,000 rows per request. Stops when a page returns fewer than the batch size.
- **Time filter:** `local_datetime` between `DATA_START_STR` and `DATA_END_STR` (from config).
- **parking_melbourne view** → `parking_raw.parquet` — individual bay-level parking events with `kerbsideid`, `latitude`, `longitude`, `local_datetime`, `status_description` (Present / Unoccupied).
- **ped_melbourne view** → `ped_raw.parquet` — pedestrian count events with `location_id`, `local_datetime`, `total_of_directions`.

#### B. Open-Meteo (`weather_raw.parquet`)
- Hourly archive API for Melbourne CBD centroid (lat=-37.8136, lon=144.9631).
- Parameters: `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`, `precipitation`.
- UTC timezone.

#### C. Melbourne Open Data CLUE datasets (14 files)
The Melbourne Open Data v2.1 catalog API has a hard 10,000-row offset cap. The fetch strategy handles this in three tiers:
1. If `total_count ≤ 10,000` → simple pagination.
2. If `total_count > 10,000` and dataset has `census_year` → filter to latest year, re-check count.
3. If still `> 10,000` → split by block_id ranges (6 bands covering the full range), fetch each band separately and concatenate.

| Dataset | Output file | Key columns |
|---|---|---|
| Cafes & restaurants | `clue_cafe.parquet` | trading_name, latitude, longitude, number_of_seats |
| Bars & pubs | `clue_bar.parquet` | latitude, longitude, number_of_patrons |
| Business establishments | `clue_business.parquet` | latitude, longitude, industry_anzsic4_code |
| Employment by block | `clue_jobs.parquet` | block_id, total_jobs_in_block |
| Buildings | `clue_buildings.parquet` | latitude, longitude |
| Block polygons | `clue_blocks.geojson` | block_id + geometry |
| Off-street car parks | `clue_offstreet.parquet` | latitude, longitude, parking_spaces |
| Residential dwellings | `clue_dwellings.parquet` | latitude, longitude, dwelling_number |
| Floorspace by industry | `clue_floorspace_industry.parquet` | block_id, industry values |
| Floorspace by use | `clue_floorspace_use.parquet` | block_id, retail_* columns, office |
| Landmarks | `clue_landmarks.parquet` | co_ordinates.lat/lon |

### Outputs
`data/raw/parking_raw.parquet`, `ped_raw.parquet`, `weather_raw.parquet`, 11 × CLUE parquets, `clue_blocks.geojson`

---

## Step 02 — Sensor Snapping + CLUE Aggregation (`step_02_snap.py`)

### Purpose
Maps every sensor to a street polygon (Part A) and aggregates CLUE land-use data onto streets (Part B).

### Part A — Sensor Snapping

**Street polygon loading:** Reads `streets.geojson` (362 polygons), projects to EPSG:3111. Normalises mixed geometry types: LineStrings are buffered 1m to become polygons; everything is standardised to MultiPolygon.

**Snapping algorithm (two-pass):**
1. **Point-in-polygon (PIP):** `geopandas.sjoin` with `predicate="within"`. Sensors that fall inside a street polygon are matched immediately with `dist_m = 0`.
2. **Nearest-neighbour fallback:** Unmatched sensors use `gpd.sjoin_nearest` with `max_distance=25m`. Sensors beyond 25m remain unmatched.

**Parking sensors:** Extracted from `parking_raw.parquet` by de-duplicating on `kerbsideid`. Must achieve ≥ 90% match rate (assertion enforced).

**Pedestrian sensors:** Location IDs from `ped_raw.parquet` are matched against sensor locations fetched from the Melbourne Open Data pedestrian sensor locations API. Unmatched location IDs are recorded with null coordinates but retained in the output.

**Output:** `sensor_map.parquet` — columns: `sensor_id`, `sensor_type`, `street_id`, `method` (point_in_polygon / nearest_neighbour / unmatched), `dist_m`, `lat`, `lon`.

### Part B — CLUE Spatial Aggregation

Produces `static_features.parquet` with 17 columns for all 362 streets.

**Two aggregation patterns:**

**Pattern 1 — Point datasets (nearest-neighbour join, max 50m):**
Each CLUE point is joined to its nearest street using `gpd.sjoin_nearest`. A point equidistant between two streets is deduplicated to keep only the closest match. Streets receive counts and sums of matched CLUE records.

| Feature | Source | Aggregation |
|---|---|---|
| `cafe_count` | clue_cafe | count of unique venues (deduplicated indoor/outdoor seating rows by trading_name+coords) |
| `cafe_total_seats` | clue_cafe | sum of seats after venue deduplication |
| `bar_count` | clue_bar | count of venues |
| `bar_patron_capacity` | clue_bar | sum of patron capacity |
| `business_count` | clue_business | count, excluding hospitality (ANZSIC codes 4000–4599) |
| `building_count` | clue_buildings | count |
| `offstreet_spaces` | clue_offstreet | sum of parking_spaces |
| `dwelling_count` | clue_dwellings | sum of dwelling_number |
| `poi_total` | clue_landmarks | count |

**Pattern 2 — Block datasets (area-weighted intersection):**
CLUE block values are area-weighted onto streets by computing the intersection of each street polygon with each CLUE block polygon. The weight for each (street, block) pair is `intersection_area / block_area`. The weighted sum gives a proportional allocation of block-level values to street segments.

| Feature | Source | Method |
|---|---|---|
| `total_jobs` | clue_jobs.total_jobs_in_block | area-weighted |
| `retail_floorspace` | clue_floorspace_use.retail_total | area-weighted |
| `office_floorspace` | clue_floorspace_use.office | area-weighted |

**Derived columns:**
- `dining_capacity = cafe_total_seats + bar_patron_capacity`
- `area_m2` = geometry area in EPSG:3111
- `centroid_lat`, `centroid_lon` = street centroid in WGS84 (critical for XGBoost imputation in Step 05)

**Validation:** Asserts 362 rows, 17 expected columns, zero NaN values, `total_jobs` max < 100,000, `dining_capacity` identity.

**Viz exports:** `sensors_viz.geojson` (sensor points) and `streets_viz.geojson` (street polygons in WGS84).

### Outputs
`sensor_map.parquet`, `static_features.parquet`, `sensors_viz.geojson`, `streets_viz.geojson`

---

## Step 03 — Temporal Features (`step_03_temporal.py`)

### Purpose
Generates two 14,400-row time-series files (one row per 15-min bin) covering the full study period.

### Master time index
`pd.date_range(start="2025-11-01 09:00", periods=14400, freq="15min", tz="UTC")`
Last bin: 2026-03-31 08:45 UTC. Validated with assertion.

### Weather forward-fill (`weather.parquet`)
Hourly `weather_raw.parquet` is reindexed to the 15-min time index using pandas `ffill` (forward-fill). Each hourly value is held constant for the four 15-min bins within that hour. Columns cast to float32: `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`, `precipitation`.

**Validation:** 14,400 rows, zero NaN, temperature in [-5, 45]°C, non-negative precipitation.

### Cyclic temporal encodings (`temporal_features.parquet`)
Rather than using raw hour integers (which treat 23:00 and 00:00 as maximally distant), the pipeline uses sin/cos cyclic encoding to preserve temporal continuity.

| Feature | Formula | Range |
|---|---|---|
| `hour_sin` | sin(2π × (hour + minute/60) / 24) | [-1, 1] |
| `hour_cos` | cos(2π × (hour + minute/60) / 24) | [-1, 1] |
| `dow_sin` | sin(2π × day_of_week / 7) | [-1, 1] |
| `dow_cos` | cos(2π × day_of_week / 7) | [-1, 1] |
| `is_weekend` | 1 if Saturday/Sunday | {0, 1} |
| `is_public_holiday` | 1 if Victorian public holiday | {0, 1} |
| `is_school_holiday` | 1 if Victorian school holiday | {0, 1} |

**Victorian public holidays encoded:** Melbourne Cup Day (2025-11-04), Christmas, Boxing Day, New Year's Day, Australia Day, Labour Day (2026-03-09).

**School holiday ranges:** End of Term 3 break (Nov 1–2), Summer holidays (Dec 20 – Jan 27), Term 1 break (Mar 21–30).

**Validation:** sin²+cos² = 1 check (cyclic identity); spot-check that 2025-11-08 (Saturday) has `is_weekend=1`; Christmas has `is_public_holiday=1`; total public holiday bins = `n_holidays × 96`.

### Outputs
`data/processed/weather.parquet`, `data/processed/temporal_features.parquet`

---

## Step 04 — Graph Construction (`step_04_graph.py`)

### Purpose
Constructs two graphs over the 1,397 eligible street nodes. Both graphs are used as fixed message-passing structures for the GCN in Step 09.

### Street eligibility filter
Streets must satisfy both conditions:
- `str_type` ∈ {Arterial, Council Major} — excludes minor and private streets
- Name does not contain "Intersection" — excludes intersection segments

### Spatial graph

**Method:** k-NN on Euclidean distance between street centroids in approximate metres (lat/lon converted with a Melbourne-centred reference projection).

**Parameters:** k = 8 neighbours per street. Bidirectional edges (both directions added for each k-NN pair), deduplicated.

**Edge weight:** Gaussian decay kernel — `w = exp(-d / σ)` where σ is the **median pairwise distance** across all k-NN pairs. This ensures ~50% of edges have weight ≥ 0.5 regardless of absolute scale.

**Result:** ~37,670 bidirectional edges. Every node has exactly 8 outgoing edges (validated: all nodes must have outgoing edges).

### Semantic graph

**Method:** Per-feature log1p ratio matching on 7 activity features: `total_jobs`, `cafe_count`, `cafe_total_seats`, `bar_count`, `bar_patron_capacity`, `business_count`, `poi_total`.

**Three-gate eligibility system:**

**Gate 0 — Street eligibility:** A street must have ≥ 3 non-zero activity features to participate.

**Gate 1 — Shared feature coverage:** A pair must share ≥ 2 non-zero features AND those shared features must cover ≥ 60% of the smaller street's active profile. Streets with very different activity footprints are rejected.

**Gate 2 — Per-feature ratio (hard gate):** For every shared feature d, compute:
```
ratio_d = min(log1p(X_i,d), log1p(X_j,d)) / max(log1p(X_i,d), log1p(X_j,d))
```
ALL shared features must have `ratio_d ≥ 0.70`. A single mismatched feature blocks the connection entirely. The log1p transform compresses scale: a 2:1 raw ratio gives 0.83, 4:1 gives ~0.73, 10:1 gives ~0.60 (rejected).

**Edge weight:** Mean ratio across all shared features (∈ [0.70, 1.0]).

**Candidate pre-filtering:** Approximate cosine similarity is computed first and only the top-20 candidates per street are evaluated through the full gate system, reducing O(N²) to manageable cost.

**Result:** ~9,852 bidirectional edges. Streets with no semantic matches (low-activity or ineligible streets) are isolated nodes — expected and not an error.

**Viz export:** `graph_viz.geojson` — node points + spatial LineStrings + top-4,000 semantic LineStrings by similarity.

### Outputs
`node_index.parquet` (street_id → node_idx), `spatial_edges.parquet`, `semantic_edges.parquet`, `graph_viz.geojson`

---

## Step 05 — Parking Reconstruction + Pedestrian Imputation (`step_05_process.py`)

### Purpose
Converts raw event logs into complete 14,400-bin time series for every street.

### Part A — Parking Occupancy Reconstruction

**Source:** `parking_raw.parquet` — individual sensor bay events with `status_description` ∈ {Present, Unoccupied}.

**Algorithm:**
1. For each bay (`kerbsideid`), iterate through events in chronological order.
2. When a "Present" event is found, record an occupancy interval: start = Present timestamp, end = next event timestamp (or start + 2h cap if no next event).
3. Duration is capped at 7,200 seconds (2 hours) to handle sensor dropouts and overnight parked vehicles.
4. Each interval is assigned to the 15-min bins it spans.

**Occupancy rate per bin:** For each (street, time_bin), count how many unique bay IDs were occupied during that bin, divide by total bays on that street. Capped at 1.0.

**Qualifying streets:** Only streets with ≥ 5 parking events are included. Result: 171 streets with sensor-observed parking.

**Output:** `parking_occupancy.parquet` — `street_id, time_bin, occupancy_rate, valid_parking` — 171 streets × 14,400 bins = 2,462,400 rows.

### Part B — XGBoost Pedestrian Imputation (v3 — graph-informed)

**Problem:** Pedestrian sensors cover ~82 arterial streets. The remaining ~1,315 streets have no pedestrian counts and must be imputed.

**Target variable:** `log1p(ped_flow)` — raw pedestrian counts are heavily right-skewed; log-transform stabilises variance.

**Feature matrix for each (street, time_bin):**

| Category | Features |
|---|---|
| Static (17) | area_m2, centroid_lat, centroid_lon, total_jobs, cafe_count, cafe_total_seats, bar_count, bar_patron_capacity, business_count, building_count, poi_total, dining_capacity, offstreet_spaces, dwelling_count, retail_floorspace, office_floorspace, parking_mean, parking_std, has_parking |
| Temporal (9) | hour_sin, hour_cos, dow_sin, dow_cos, is_weekend, is_public_holiday, is_school_holiday, hour_sin×is_weekend, hour_cos×is_weekend |
| Weather (4) | temperature_2m, relative_humidity_2m, wind_speed_10m, precipitation |
| Spatial lag (1) | Mean ped_flow of sensored spatial-graph neighbours at this time_bin |

**Spatial graph lag feature:** For each street, its sensored k-NN neighbours serve as a real-time reference signal — "what are nearby streets seeing right now?" This feature is unavailable in pure static+temporal designs and gives the XGBoost awareness of local network context. Implemented as a weight matrix `W` (n_all_streets × n_sensored_streets) times the sensored ped pivot matrix; computed once and stored as a pivot DataFrame indexed by time_bin.

**Cross-validation:** GroupKFold(5) over streets — each fold holds out entirely different streets to test generalisation to *unseen* streets, not just unseen time periods. This prevents a street's own historical patterns from leaking into its validation predictions.

**Model:** XGBoost with `n_estimators=200, max_depth=5, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8, min_child_weight=3, tree_method="hist", seed=42`.

**Final model:** Retrained on all sensored data with `n_estimators=500` before predicting unsensored streets. Prediction done in batches of 300 streets to avoid OOM.

**Confidence tiers:**
- `ped_confidence = 1.0` — sensor-observed streets
- `ped_confidence = 0.8` — imputed streets, model R² ≥ 0.6 (high-confidence imputation)
- `ped_confidence = 0.5` — imputed streets, model R² < 0.6 (low-confidence imputation)

**Output:** `ped_complete.parquet` — all streets × 14,400 bins with `ped_flow`, `ped_confidence`, `source`. Also `ped_street_summary.json` (mean/peak per street for the frontend).

### Outputs
`parking_occupancy.parquet`, `ped_complete.parquet`, `ped_street_summary.json`

---

## Step 06 — Aggregate Street Profiles (`step_06_aggregate.py`)

### Purpose
Compresses the 14,400-bin time series into a compact 84-feature weekly behavioural profile per street for clustering input.

### Time blocks
The 24-hour day is divided into 6 semantically meaningful periods:

| Block | Hours | Semantic meaning |
|---|---|---|
| night | 00:00–05:59 | Off-peak |
| morning | 06:00–09:59 | Commute, cafes |
| work_am | 10:00–11:59 | Late morning |
| midday | 12:00–13:59 | Lunch |
| work_pm | 14:00–17:59 | Afternoon work |
| evening | 18:00–23:59 | Dining, nightlife |

### Profile construction
For each signal (parking occupancy, pedestrian flow), for each street, the mean value is computed for each (day-of-week × time-block) combination: 7 days × 6 blocks = 42 features. Streets missing data in any bin default to 0.

**Note on normalisation:** Ped profiles are stored in raw scale (not row-normalised). Row-normalisation was explicitly rejected because it makes a quiet alley indistinguishable from a busy retail corridor — discarding the absolute activity level information needed for meaningful intervention ranking. StandardScaler in Step 07 handles scale differences across streets.

### Engineered temporal scores
Five interpretable scores are computed and added to the profile:

| Score | Formula | Purpose |
|---|---|---|
| `score_morning` | Mean ped_flow across Monday–Friday morning+work_am blocks | Raw morning activity level |
| `score_midday` | Mean ped_flow across Monday–Friday midday+work_pm blocks | Raw midday activity level |
| `score_evening` | Mean ped_flow across all days evening blocks | Raw evening activity level |
| `score_weekend_ratio` | Mean weekend ped / mean weekday ped (clipped 0–5) | Relative weekend uplift |
| `score_parking_flex` | Fraction of time-blocks with occupancy < 30% | Parking flexibility |

These give the GMM explicit interpretable axes rather than forcing it to infer temporal patterns from 84 noisy dimensions.

### Output shape
Per street: 42 parking features + 42 ped features + 5 scores + 4 summary stats (`mean_ped`, `peak_ped`, `mean_parking`, `ped_confidence`) + 9 static pass-throughs = 103 columns (plus `street_id`).

### Outputs
`street_profiles.parquet`, `street_profiles_summary.json`

---

## Step 07 — GMM Clustering (`step_07_cluster.py`)

### Purpose
Segments 1,397 streets into archetypes based on their joint parking and pedestrian temporal behaviour. Archetypes drive intervention type assignment and flexibility window identification.

### Clustering pipeline

1. **Input:** 84 temporal features (42 parking + 42 ped) + 5 engineered scores.

2. **StandardScaler:** Z-score normalisation across streets for each feature.

3. **PCA:** Dimensionality reduction to 20 components (min of 20, N-1, n_features). Typically explains ≥ 85% of variance. Avoids curse of dimensionality in the GMM covariance estimation.

4. **BIC search k=2..10:** Full Gaussian Mixture Model (full covariance, n_init=5, reg_covar=1e-3) fit for each k. BIC minimum selected directly — no k floor constraint, reflecting genuine data structure.

5. **Final GMM:** Re-fit with n_init=10 at best_k for stability.

6. **Bootstrap ARI stability (20 iterations):** Resample the PCA-projected data with replacement and refit; compute Adjusted Rand Index between bootstrap labels and original labels. Mean ARI reported; warning issued if < 0.70.

7. **Silhouette score:** Computed on PCA-projected data.

### Archetype labelling
GMM means are back-projected through PCA inverse_transform and StandardScaler inverse_transform to the original feature space. The engineered score columns determine archetype assignment via a priority-ranked decision tree:

- **Tier 1 (major_pedestrian_corridor):** Highest-activity cluster by z-score > 1.0
- **Tier 2 (latent_activation_potential):** Lowest-activity cluster, z-score < -0.5
- **Tier 3 (parking_reallocation_priority):** Medium activity, parking flex score < 0.85 (parking heavily used)
- **Tier 4 — time-of-day archetypes:** Medium activity, good parking flex:
  - `morning_pedestrianisation` — peak morning z-score
  - `evening_outdoor_dining` — peak evening z-score
  - `weekend_leisure` — highest weekend ratio cluster
  - `midday_retail_activation` — default for midday peak

### Flexibility windows
For each street, identifies time-blocks where **both** conditions hold:
- Parking occupancy < 30% (space available for reallocation)
- Pedestrian demand > cluster median for that time-block (opportunity exists)

`flexibility_windows` column contains a JSON array of all qualifying time-blocks. `best_window` contains the first (highest-priority) window.

### Confidence
`cluster_confidence` = GMM posterior probability (max over components). Streets with confidence < 0.70 are flagged as `intervention_reliable = False`.

### Outputs
`clustered.parquet` (street_id, cluster, intervention_type, cluster_confidence, intervention_reliable, flexibility_windows, best_window, mean_ped, mean_parking, ped_confidence + per-cluster probabilities), `cluster_centroids.csv`, `cluster_report.json`, `cluster_summary.json`

---

## Step 08 — Data Cube Assembly (`step_08_cube.py`)

### Purpose
Assembles the final [N × T × F] numpy array that serves as the unified input to the GCN model, and builds the normalised graph adjacency tensors.

### Cube dimensions
- **N = 1,397** streets (node_index order)
- **T = 14,400** time bins (15-min, Nov 2025 – Mar 2026)
- **F = 23** features

### Feature layout

| Index | Name | Type | Normalised |
|---|---|---|---|
| 0 | ped_flow | time-varying, continuous | Yes |
| 1 | occupancy_rate | time-varying, continuous | Yes |
| 2 | ped_confidence | time-varying, ordinal (1.0/0.8/0.5) | No |
| 3 | hour_sin | cyclic [-1,1] | No |
| 4 | hour_cos | cyclic [-1,1] | No |
| 5 | dow_sin | cyclic [-1,1] | No |
| 6 | dow_cos | cyclic [-1,1] | No |
| 7 | is_weekend | binary | No |
| 8 | is_public_holiday | binary | No |
| 9 | is_school_holiday | binary | No |
| 10 | temperature_2m | time-varying, continuous | Yes |
| 11 | relative_humidity_2m | time-varying, continuous | Yes |
| 12 | wind_speed_10m | time-varying, continuous | Yes |
| 13 | precipitation | time-varying, continuous | Yes |
| 14 | total_jobs | static, broadcast | Yes |
| 15 | cafe_count | static, broadcast | Yes |
| 16 | cafe_total_seats | static, broadcast | Yes |
| 17 | bar_count | static, broadcast | Yes |
| 18 | bar_patron_capacity | static, broadcast | Yes |
| 19 | business_count | static, broadcast | Yes |
| 20 | poi_total | static, broadcast | Yes |
| 21 | dining_capacity | static, broadcast | Yes |
| 22 | area_m2 | static, broadcast | Yes |

**Assembly:** ped_flow, occupancy_rate, and ped_confidence are pivoted from long format; temporal+weather features are broadcast across all N streets; static features are broadcast across all T time steps. The cube is stored as raw float32 (not normalised) — normalisation is applied on the fly during training.

**Normalisation statistics:** Computed on the first 80% of timesteps (training portion only) to avoid leakage. Mean and std stored in `norm_stats.json` for all 15 normalisable features.

### Graph adjacency matrices
Both adjacency matrices are built as symmetrically normalised sparse tensors:
```
A_norm = D^{-1/2} (A + I) D^{-1/2}
```
Self-loops (I) are added before normalisation. D is the degree matrix (row sums). Result is a sparse COO tensor saved as `.pt` via `torch.save`.

The self-loops ensure every node receives its own signal during message passing. The symmetric normalisation prevents gradient explosion as the graph grows.

### Outputs
`cube.npy` (float32, ~0.5–2 GB), `graph_spatial.pt`, `graph_semantic.pt`, `norm_stats.json`, `cube_meta.json`

---

## Step 09 — MultiGCN Training (`step_09_train.py`)

### Purpose
Trains the spatio-temporal graph neural network that learns to predict next-timestep pedestrian flow for all 1,397 streets simultaneously.

### Model Architecture — MultiGCN

```
Input: [B, W, N, F]   (batch, window=96, nodes=1397, features=23)
│
├── Spatial branch:   proj_s  (F → H=64)  →  ReLU(adj_s   @ proj_s(x))   [K, N, 64]
├── Semantic branch:  proj_sem (F → H=64)  →  ReLU(adj_sem @ proj_sem(x)) [K, N, 64]
│
└── Concat: [K, N, 128]
    └── Reshape: [B, W, N, 128]
        └── Dropout(0.1)
            └── GRU: (input=128, hidden=64, layers=2, batch_first=True)
                      applied per-node over W timesteps: [B×N, W, 128] → [B, N, 64]
                └── Linear: 64 → 1
                    └── + node_bias [N, 1]  (learned per-street offset)
Output: [B, N, 1]     (predicted ped_flow, z-score normalised)
```

**Key design choices:**
- **Dual GCN branches run at every timestep:** `_sparse_batched` reshapes [B×W, N, F] for efficient sparse matrix multiplication, applies adj to the projected features, then reshapes back. This gives the GRU temporally-aware neighbourhood context at each step.
- **Per-node bias:** Each street learns its own mean ped_flow level as a trainable scalar. This is the single highest-leverage improvement for R² on spatially heterogeneous data (a busy Flinders St corridor has a fundamentally different baseline than a quiet residential lane).
- **GRU over window:** After both GCN branches process all W=96 timesteps (24 hours), the GRU compresses the temporal sequence into a single hidden state per node. Only the final hidden state (from the last GRU layer) is used for prediction.
- **Parameter count:** ~98,000–120,000 parameters.

### Training Protocol

**Data split (chronological, no leakage):**
- Train: bins 0 – T×0.70 (~10,080 bins, Nov 2025 – ~early Feb 2026)
- Val: bins T×0.70 – T×0.85 (used for early stopping)
- Test: bins T×0.85 – T (held out, reported at end only)

**Training loop:**
- 256 gradient steps per epoch, batch size 8 windows per step
- Windows sampled randomly from the training period
- Target: `ped_flow` at timestep `t + W` (next-step prediction)
- Loss: MAE on z-score normalised target
- Optimiser: Adam, lr=1e-3
- Scheduler: ReduceLROnPlateau (factor=0.5, patience=8 epochs)
- Gradient clipping: max norm 1.0
- Early stopping: patience=25 epochs on val MAE

**Val evaluation:** 128 fixed windows (same every epoch, evenly spaced across the val period) — ensures stable early stopping that doesn't fluctuate due to random sampling.

**Seed:** 42 (torch, numpy, random — fully reproducible).

**Device:** CUDA if available, otherwise CPU.

### Outputs
`data/models/best_model.pt`, `run_config.json`, `model_eval.json` (val/test MAE, RMSE, R², training history). Updates `cube_meta.json` with `T_train_end`, `T_val_start`, `T_val_end`.

---

## Step 10 — Feature Interpretation (`step_10_interpret.py`)

### Purpose
Quantifies which features the trained model actually relies on, and how much each GCN branch contributes.

### Permutation importance
For each of the 23 features:
1. Load val-period windows.
2. Replace the feature with independent N(0,1) Gaussian noise (in the normalised input space).
3. Measure MAE on the perturbed data.
4. `importance = perturbed_MAE - baseline_MAE` — a positive value means the model relied on that feature (destroying it increases error).
5. Repeat 3 times, report mean delta_MAE.

Features are reported sorted descending by importance.

### Branch contribution
For each GCN branch (spatial, semantic):
1. Zero all weights of that branch's projection layer (`proj_s` or `proj_sem`).
2. Measure MAE with the branch disabled.
3. `branch_delta_MAE = disabled_MAE - baseline_MAE`.
4. Restore original weights.

This quantifies the marginal contribution of spatial vs. semantic neighbourhood information.

### Outputs
`data/models/feature_importance.json` — `baseline_mae`, `feature_importance` (dict sorted by importance), `branch_contribution` (spatial and semantic delta_MAE).

---

## Step 11 — Scenario Simulation (`step_11_scenario.py`)

### Purpose
Counterfactual intervention analysis: predict the pedestrian flow impact of a parking intervention on a specific street during a specific time window, including network-wide spillover effects.

### Intervention types
- `pedestrianise` — set target street `occupancy_rate = 0` (full removal of parking)
- `restrict_park` — set target street `occupancy_rate = magnitude` (e.g., 0.3 = allow only 30%)
- `boost_ped` — add a raw pedestrian uplift (ped/15-min) to the target street's predicted flow

### Autoregressive rollout
```
Initialise: sliding window from real observed cube data [WINDOW=96 steps before t_start]

For each rollout step k:
  1. Forward pass → model predicts ped_flow for all N streets
  2. Build next_row from real observed features (weather, time, occupancy, static)
  3. Override ped_flow with model prediction (autoregressive)
  4. If within intervention window: apply intervention encoding on target street
  5. (Improvement #1) If parking removed: redistribute displaced parking to spatial neighbours
  6. Append next_row to sliding window (pop oldest)

Repeat for rollout_steps total steps
```

Two rollouts are run: **baseline** (no intervention) and **treated** (with intervention). Delta = treated − baseline at every (step, street) pair.

### Five network analysis enhancements

1. **Parking displacement model:** Removed occupancy from the target street is redistributed to spatial neighbours proportional to edge weight, capped at 100% occupancy per neighbour. Addresses the core "where do displaced parkers go?" question.

2. **Graph diffusion analysis:** Analytically propagates `mean_delta` through row-normalised adjacency matrices for k=1,2,3 hops. Reports top-5 streets by diffused delta per hop for both spatial and semantic graphs. Complements the autoregressive spillover estimate.

3. **Semantic neighbour reporting:** Reports delta on functionally similar streets (semantic graph neighbours) that may be spatially distant. Captures non-local demand shifts in streets with the same land-use profile.

4. **Confidence-weighted ranking:** `top_affected` streets are ranked by `delta × ped_confidence` rather than raw delta. Sensor-observed streets (conf=1.0) are ranked above imputed streets (conf=0.5) for the same delta magnitude.

5. **Rebound / half-life analysis:** After the intervention ends, computes how many steps until `|delta|` drops to 50% of its value at the intervention end-step. Reports per-street half-life in steps and minutes, plus recovery fraction at the final rollout step.

### Output structure
```json
{
  "meta": { "street_id", "t_start_bin", "duration_bins", "rollout_steps",
            "intervention_type", "assumptions" },
  "baseline": { "ped_flow_treated_street", "occ_rate_observed" },
  "treated":  { "ped_flow_treated_street" },
  "delta":    { "ped_flow_treated_street", "top_affected_series" },
  "network_summary": {
    "treated_street":      { mean_delta, cumulative_delta, ped_confidence },
    "spatial_neighbours":  [ { street_id, mean_delta, confidence_weighted_delta } ],
    "semantic_neighbours": [ { street_id, mean_delta, confidence_weighted_delta } ],
    "top_affected":        [ { street_id, ..., is_treated, is_spatial_neighbour, is_semantic_neighbour } ],
    "graph_diffusion":     { "spatial": [...], "semantic": [...] },
    "rebound":             { "treated_street": { half_life_steps, peak_delta, recovery_fraction },
                             "slowest_to_recover": [...] },
    "all_deltas":          { "street_id": { mean_ped_delta, confidence_weighted_delta, ped_confidence } }
  }
}
```

### CLI / API usage
**CLI:**
```
python melbourne_pipeline/steps/step_11_scenario.py \
  --street STREET_ID --start BIN_INDEX --duration BINS --rollout STEPS \
  --intervention {pedestrianise|restrict_park|boost_ped} [--magnitude VALUE]
```
**API:** POST `http://127.0.0.1:5050/scenario` (via `api_server.py`, started separately).

### Outputs
`data/processed/scenario_results/<street_id>_<intervention>_t<start>_d<duration>.json` (when `save=True`)

---

## Step 11b — Opportunity Scoring (`step_11_opportunities.py`)

Separate module (also called Step 11 in `run_pipeline.py`) that scores all streets for intervention suitability.

**Composite opportunity score** (0–1):
```
score = 0.35 × score_ped        (predicted pedestrian flow, normalised by max)
      + 0.25 × score_parking    (1 - mean_occupancy: low parking = more room)
      + 0.20 × score_archetype  (GMM cluster confidence)
      + 0.10 × score_confidence (ped data source: sensor=1.0, R²≥0.6=0.8)
      + 0.10 × score_uplift     (counterfactual uplift fraction)
```

The counterfactual is computed by running 8 randomly sampled val-period windows with `occupancy_rate = 0` for all streets simultaneously, measuring the mean predicted ped uplift.

**Outputs:** `street_scores.parquet`, `opportunities.json`

---

## Step 12 — Frontend Export (`step_12_export.py`)

### Purpose
Enriches the GeoJSON and summary JSONs with pipeline outputs ready for consumption by `sensor_map_viz.html`.

### Operations
1. Loads `street_scores.parquet` and joins opportunity score fields (`opportunity_score`, `pred_ped_mean`, `uplift_fraction`, `cf_ped_mean`) onto each street feature in `streets_viz.geojson`.
2. Writes `opportunities_summary.json` — top-50 streets per archetype with key metrics.
3. Copies `model_eval.json` from `data/models/` to `data/processed/` for frontend access.

### Outputs
Updated `streets_viz.geojson`, `opportunities_summary.json`, `model_eval.json` (copy)

---

## API Server (`api_server.py`)

Flask + flask-cors server exposing the scenario simulation over HTTP.

**Endpoint:** `POST /scenario`  
**Body:** `{"street_id": "...", "intervention_type": "...", "duration": 16, "rollout_steps": 32, "magnitude": null, "t_start": <optional>}`  
**Default t_start:** Midpoint of the validation period (max(WINDOW, T_val_start + (T - T_val_start) // 2)).  
**Returns:** Full scenario result JSON (same structure as `run_scenario()`, `save=False` to avoid triggering Live Server reloads).

**Health check:** `GET /health` returns `{"status": "ok", "N": ..., "T": ...}`.

**Start:** `python melbourne_pipeline/api_server.py` (default port 5050, from inside `melbourne_pipeline/`)

---

## Configuration (`config.py`)

All pipeline parameters are centralised here.

| Parameter | Value | Purpose |
|---|---|---|
| BASE_DIR | `melbourne_pipeline/` | Root for all relative paths |
| DATA_START | 2025-11-01 09:00 UTC | Study period start |
| DATA_END | 2026-03-30 09:00 UTC | Study period end |
| TIME_BIN_MINUTES | 15 | Temporal resolution |
| MELB_LAT/LON | -37.8136 / 144.9631 | Weather API location |
| SUPABASE_BATCH_SIZE | 1,000 | Rows per Supabase REST page |

Supabase credentials (`SUPABASE_URL`, `SUPABASE_KEY`) are loaded from `.env` via python-dotenv.

---

## Data Flow Diagram

```
Supabase          Open-Meteo         Melbourne Open Data (CLUE)
    │                  │                         │
    ▼                  ▼                         ▼
parking_raw.parquet  weather_raw.parquet   clue_*.parquet + clue_blocks.geojson
         │                │                     │
         └──────────────┬─┘                     │
                        ▼                       ▼
                    [Step 02]  ←  streets.geojson
                    sensor_map.parquet
                    static_features.parquet
                        │
               ┌────────┴─────────┐
               ▼                  ▼
           [Step 03]          [Step 04]
           weather.parquet    node_index.parquet
           temporal.parquet   spatial_edges.parquet
                              semantic_edges.parquet
               │                  │
               └────────┬─────────┘
                        ▼
                    [Step 05]
                    parking_occupancy.parquet  (171 streets × 14,400)
                    ped_complete.parquet       (1,397 streets × 14,400)
                        │
                    [Step 06]
                    street_profiles.parquet   (1,397 × 103 features)
                        │
                    [Step 07]
                    clustered.parquet         (archetypes + flex windows)
                        │
                    [Step 08]
                    cube.npy                  [1,397 × 14,400 × 23]
                    graph_spatial.pt
                    graph_semantic.pt
                        │
                    [Step 09]
                    best_model.pt
                        │
              ┌─────────┼──────────┐
              ▼         ▼          ▼
         [Step 10]  [Step 11]  [Step 12]
         feature_   scenario_  streets_viz.geojson
         importance results/   opportunities_summary.json
         .json
```

---

## Known Limitations and Thesis Caveats

1. **Occupancy values in cube are low (max ~12.5%):** The cube assembly averages occupancy from sensor streets, but most of the 1,397 streets are imputed with occupancy = 0. The model trained on these values will show small scenario deltas for pedestrianisation interventions on low-occupancy streets.

2. **Parking displacement is first-order:** Displaced parking is redistributed to direct spatial neighbours only. Overflow does not cascade further. This is a conservative simplification.

3. **Autoregressive compounding error:** The rollout error compounds with each step. Rollouts beyond ~4 hours (16 steps) should be treated as indicative, not precise.

4. **Static semantic graph:** Semantic edges are based on static CLUE land-use data (annual census). Intra-year changes to a street's activity profile are not captured.

5. **Graph topology unchanged by intervention:** Pedestrianising a street may in reality alter network connectivity (closing it to vehicles). The model treats the graph as fixed.

6. **Confidence tiers on imputed streets:** ~1,315 of 1,397 streets have imputed (not sensor-observed) pedestrian data. Results for these streets carry the XGBoost model's uncertainty.
