# Melbourne CBD Street Analysis Pipeline

Spatio-temporal Graph Neural Network pipeline for analysing 3,975 Melbourne CBD street segments. Combines parking occupancy, pedestrian demand, static street context, and graph topology to support feature interpretation and counterfactual scenario simulation.

**Stack:** Python 3.11 · PyTorch Geometric · XGBoost · GeoPandas · Parquet  
**CRS:** EPSG:3111 (spatial ops) · WGS84 (storage)  
**Data:** Supabase (parking, pedestrian) · Melbourne Open Data (CLUE) · Open-Meteo (weather)

---

## Model Architecture

```
GRU encoder → dual GCNConv branches (spatial + semantic) → fusion → prediction head
```

- **Spatial graph:** k=8 k-NN, Gaussian kernel — 37,670 bidirectional edges
- **Semantic graph:** cosine similarity ≥ 0.99, mutual k-NN — 9,852 bidirectional edges
- **Confidence tiers:** 1.0 (sensor) / 0.8 (R² ≥ 0.6) / 0.5 (R² < 0.6)

---

## Pipeline

| Step | Module | Purpose |
|------|--------|---------|
| 01 | `step_01_fetch.py` | Fetch raw sources (parking, pedestrian, weather, CLUE) |
| 02 | `step_02_snap.py` | Snap sensors, aggregate static features |
| 03 | `step_03_temporal.py` | Temporal encoding, weather alignment |
| 04 | `step_04_graph.py` | Build spatial + semantic graph artifacts |
| 05 | `step_05_process.py` | Build occupancy, impute pedestrian activity |
| 06 | `step_06_aggregate.py` | Aggregate profile-level street metrics |
| 07 | `step_07_cluster.py` | Street archetype clustering |
| 08 | `step_08_cube.py` | Assemble model-ready cube + graph inputs |
| 09 | `step_09_train.py` | Train MultiGCN |
| 10 | `step_10_interpret.py` | Permutation importance, branch contribution |
| 11 | `step_11_scenario.py` | Intervention scenario simulation |
| 12 | `step_12_export.py` | Export GeoJSON / JSON for Mapbox GL JS |

Steps 01–09 are frozen post-training. Active work is Steps 10–12.

---

## Key Outputs

| Artifact | Description |
|----------|-------------|
| `static_features.parquet` | 17 cols × 3,975 streets |
| `ped_complete.parquet` | 3,975 streets × 14,400 time bins |
| `parking_occupancy.parquet` | 171 streets × 14,400 time bins |
| `clustered.parquet` | street_id, cluster, intervention_type, confidence |
| `spatial_edges.parquet` + `semantic_edges.parquet` | Graph topology |
| `*.pt` checkpoint | Trained MultiGCN weights |

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Create `melbourne_pipeline/.env`:

```env
SUPABASE_URL=...
SUPABASE_KEY=...
```

All other settings: `melbourne_pipeline/config.py`.

---

## Running

```bash
# Full pipeline
python melbourne_pipeline/run_pipeline.py

# Single step
python melbourne_pipeline/run_pipeline.py 10

# Step range
python melbourne_pipeline/run_pipeline.py 10 12
```

---

## Scenario API

```bash
python melbourne_pipeline/api_server.py
# Optional: --host 127.0.0.1 --port 8000
```

Endpoints: `GET /health` · `POST /scenario`

---

## Tests

```bash
python -m pytest tests/test_stress_fixes.py -v
```

---

## Repository Layout

```
melbourne_ingestor/
├── melbourne_pipeline/
│   ├── run_pipeline.py
│   ├── api_server.py
│   ├── config.py
│   ├── steps/           # step_01 … step_12
│   ├── data/
│   │   ├── raw/
│   │   ├── processed/
│   │   └── models/
│   ├── frontend/
│   │   ├── sensor_map_viz.html
│   │   └── output_data/
│   └── logs/
├── scripts/             # export, reporting, fetch utilities
├── tests/
├── docs/
│   ├── notes/
│   └── references/
└── requirements.txt
```
