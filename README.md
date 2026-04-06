# Melbourne CBD Curbside Reallocation — AI Pipeline

An end-to-end research pipeline for data-driven curbside policy analysis across 200 Melbourne CBD streets. Built for a Master's thesis on AI-driven urban mobility optimisation.

## What It Does

The system answers one question: **given the last 3 hours of parking and pedestrian data across every street in the CBD, what will happen in the next hour — and what changes if one street's policy changes?**

It does this in six sequential pipeline steps, culminating in a Spatio-Temporal Graph Neural Network (ST-GNN) that simultaneously models temporal memory (GRU), physical adjacency (Proximity GAT), and functional similarity (Semantic GAT).

---

## Pipeline Steps

| Step | Module | Description |
|------|--------|-------------|
| 1 | `s1_ingest` | Ingest parking sensor data + CLUE land-use surveys. 200 street polygons, 15-min intervals. |
| 2 | `s2_graph` | Build semantic similarity graph (cosine >= 0.3) and proximity graph (<= 50 m). |
| 3 | `s3_ped_impute` | XGBoost imputation for the 129 streets without pedestrian counters. Median CV R2 = 0.55. |
| 4 | `s4_cluster` | GMM temporal clustering (k=3) on 84-feature weekly profiles — three street archetypes. |
| 5 | `s5_train` | ST-GNN training: GRU (2L h=64) + Dual GAT (2L 4-head) + Fusion MLP (192->64). |
| 6 | `s6_intervene` | Counterfactual simulation: perturb one street's features, propagate via dual graphs, compute delta. |

---

## Model Performance

| Metric | Value |
|--------|-------|
| Occupancy R2 | **0.73** |
| Pedestrian R2 | **0.90** |
| Occupancy MAE | 0.088 |
| Pedestrian MAE | 21 peds/15 min |
| Parameters | 98,440 |
| Training epochs | 34 (early stop patience=10) |

---

## Repository Structure

```
melbourne_ingestor/
|
+-- pipeline/                    # Main production pipeline
|   +-- run.py                   # Entry point: python -m pipeline.run
|   +-- config.py                # Paths, hyperparameters
|   |
|   +-- steps/                   # One module per pipeline step
|   |   +-- s1_ingest/
|   |   +-- s2_graph/
|   |   +-- s3_ped_impute/
|   |   +-- s4_cluster/
|   |   +-- s5_train/
|   |   \-- s6_intervene/
|   |
|   +-- model/                   # ST-GNN architecture
|   |   +-- architecture.py      # GRU + Dual GAT + Fusion MLP
|   |   +-- dataset.py           # 3D tensor [N=200, T=12, F=8] data cube
|   |   \-- graphs.py            # Graph construction utilities
|   |
|   +-- utils/                   # Shared helpers
|   |
|   +-- transforms/              # Presentation HTML patch scripts
|   |   \-- transform*.py        # Each patches pipeline/output/presentation.html
|   |
|   +-- reference_docs/          # Academic papers (PDF)
|   |
|   \-- output/                  # All pipeline outputs
|       +-- presentation.html    # Interactive thesis presentation
|       +-- semantic_street_viz.html
|       +-- *.parquet            # Processed data at each step
|       +-- models/best_model.pt
|       +-- prox_graph.json / sem_graph.json
|       \-- viz_*.json           # Visualisation data
|
+-- data/                        # Raw inputs
|   +-- osm_pois.geojson
|   +-- polygons/
|   \-- bulk_download/
|
+-- scripts/                     # Utility and reporting scripts
|   +-- fetch_parking_data.py
|   +-- fetch_pedestrian_data.py
|   +-- export_to_frontend.py
|   \-- generate_pipeline_report.py
|
+-- frontend/                    # Standalone web dashboard
+-- requirements.txt
+-- CLAUDE.md
\-- .env                         # API keys (not committed)
```

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
python -m pipeline.run

# View the interactive presentation
open pipeline/output/presentation.html
```

### Running Individual Steps

```python
from pipeline.steps.s1_ingest import run as ingest
from pipeline.steps.s5_train import run as train

ingest()   # produces parking.parquet, static.parquet, temporal.parquet
train()    # produces models/best_model.pt, training_history.json, eval_metrics.json
```

---

## Interactive Presentation

`pipeline/output/presentation.html` is a self-contained interactive thesis presentation with:

- Scrollytelling graph-building walkthrough (9 scenes)
- Animated ST-GNN forward-pass diagram (5 phases: data cube, GRU, Prox GAT, Sem GAT, Fusion)
- Live counterfactual simulation canvas (6-step walkthrough)
- Click-to-pause on all animated diagrams

The presentation was built incrementally via patch scripts in `pipeline/transforms/`. Each `transform*.py` reads, patches, and rewrites `presentation.html` in a targeted way without touching unrelated sections.

---

## Key Design Decisions

**Why XGBoost for imputation?** The pedestrian sensor network covers only 71/200 streets. XGBoost with 5-fold CV gives a median R2 of 0.55; streets below 0.3 are assigned low confidence weights (0.3) rather than excluded, so the ST-GNN still trains on all streets.

**Why dual graphs?** Urban streets interact at two scales simultaneously: physical proximity (spillover, footfall diffusion) and functional similarity (correlated demand patterns across the day). A single graph loses one of these signals.

**Why GRU over Transformer?** With T=12 time steps and N=200 streets, the sequence length is short enough that GRU's sequential inductive bias outperforms self-attention on this dataset size. The inductive prior matches the data regime.

**Counterfactual via perturbation:** Rather than retraining for each policy scenario, the trained model acts as a differentiable simulator. Perturbing a street's static feature vector and running two forward passes (baseline vs. perturbed) gives a network-wide impact map in milliseconds. The dual GAT propagates the perturbation to both physical and functional neighbours.

---

## Data Sources

| Source | Description |
|--------|-------------|
| City of Melbourne Open Data | Parking sensor readings (15-min intervals, ~3.5M events) |
| CLUE Survey | Annual Census of Land Use & Employment (cafe seats, bar capacity, jobs, etc.) |
| OpenStreetMap | POI locations, street geometry |
| Bureau of Meteorology | Hourly weather (temperature, rainfall) |

---

## Requirements

- Python 3.10+
- PyTorch 2.x + torch-geometric
- pandas, geopandas, scikit-learn, xgboost
- flask (API server)
- See `requirements.txt` for pinned versions
