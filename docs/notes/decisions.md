# Key Decisions Log

Format per entry:
- Date / Decision ID
- Decision
- Options considered → Chosen approach + reason
- Known weakness (must acknowledge in thesis)
- Paper justification (if applicable)

---

### 2026-04-06 — Post-training phase lock
- Decision: Steps 01-09 frozen. Active work is Steps 10-12.
- Reason: Training preparation is sufficiently validated. Focus shifts to interpretation and scenario design.
- Impact: Claude will not modify pre-training code unless OVERRIDE is stated.

### 2026-04-06 — Semantic graph threshold
- Decision: min_sim = 0.99 (near-identical functional profiles only)
- Reason: Lower threshold created spurious connections between dissimilar streets
- Impact: 564 streets have no semantic edges (isolated)

### D01 — Spatial graph adjacency: Gaussian kernel vs hard threshold
- Rejected: hard distance threshold (Zhao & Zhang 2024)
- Chosen: Gaussian decay kernel with sigma ~72m
- Reason: smooth gradient of spatial influence; hard threshold is arbitrary at the boundary and loses near-boundary signal
- Weakness: sigma is a hyperparameter — somewhat arbitrary, sensitivity analysis needed
- Paper: Gong et al. 2023 (cosine similarity + distance weighting)

### D02 — Semantic graph: cosine similarity of activity profiles
- Rejected: POI category embedding (Hao et al. 2023 approach)
- Chosen: cosine similarity of normalised land-use activity vectors
- Reason: Melbourne CLUE data provides reliable annual land-use counts; POI embeddings require SafeGraph-style data not available here
- Weakness: land-use is static (annual CLUE update) — misses intra-year semantic shifts
- Paper: Gong et al. 2023 Section 3.2

### D03 — Temporal encoding: cyclic sin/cos vs raw hour integer
- Rejected: raw hour as integer feature
- Chosen: sin/cos cyclic encoding for hour-of-day and day-of-week
- Reason: raw integer treats 23:00 and 00:00 as maximally distant; cyclic encoding preserves continuity
- Weakness: loses distinction between e.g. 08:00 and 20:00 (same sin/cos values) — mitigated by including both sin and cos
- Paper: Asher et al. 2025 Section 3.2.1

### D04 — Study period: pre-COVID only (2015-2019)
- Rejected: full dataset including 2020-2024
- Chosen: pre-COVID only
- Reason: Asher et al. 2025 show post-COVID pedestrian dynamics in Melbourne have not returned to baseline (R² drops from 0.88 to 0.08 post-COVID). Including post-COVID data would contaminate the stable behavioural patterns the model is designed to learn.
- Weakness: model predictions may not reflect current (2024+) Melbourne conditions — must state this as a limitation
- Paper: Asher et al. 2025 Section 5.7, Table 5

### D05 — Imputation method: XGBoost GroupKFold vs spatial interpolation
- Rejected: spatial interpolation (inverse distance weighting)
- Chosen: XGBoost with GroupKFold cross-validation by sensor_id
- Reason: IDW assumes spatial stationarity — Melbourne CBD has high local variation (stadium vs office block 50m apart). XGBoost can learn non-linear relationships with built environment features.
- Weakness: GroupKFold still has spatial leakage risk for nearby sensors (acknowledged by Asher et al. 2025 Section 7)
- Current R²: 0.571 — lower than Asher et al. (0.88) but tasks differ (imputing unsensored streets vs predicting known sensors)
- Paper: Asher et al. 2025 Section 4.3

### D06 — Street archetypes: GMM vs K-means vs manual classification
- Rejected: K-means (hard cluster boundaries), manual classification
- Chosen: Gaussian Mixture Model (soft probabilistic assignment)
- Reason: streets can exhibit mixed temporal profiles (e.g. morning commuter + lunch retail) — soft assignment captures this better than hard boundaries
- Weakness: number of components is a hyperparameter; BIC/AIC selection must be reported
- Paper: Rhythm of Streets 2024

### 2026-04-07 — Five spillover prediction improvements in Step 11 scenario simulation
- Decision: Added parking displacement, graph diffusion, semantic neighbour reporting, confidence-weighted ranking, and rebound analysis to step_11_scenario.py
- Options considered: Retraining with multi-hop GCN layers (rejected — frozen), adding these as post-training analysis enhancements (chosen)
- Chosen approach: All 5 are post-training, no retraining needed. They modify rollout inputs (displacement) and enrich the output analysis (diffusion, semantics, confidence, rebound).
- Known weakness: Displacement model is first-order (no cascade overflow). Graph diffusion is analytical, not model-predicted. Rebound half-life assumes monotonic decay.
- Paper justification: Standard graph signal processing (A^k diffusion); displacement is domain logic, not paper-derived.

### 2026-04-10 — Joint prediction: MultiGCN retrained with parking occupancy head
- Decision: Add second output head (head_park) to MultiGCN sharing backbone with ped head
- Options considered: Post-training XGBoost layer predicting parking delta (Option 1, rejected),
  retrain with joint head (Option 2, chosen)
- Chosen approach: Shared GCN+GRU backbone, separate Linear projection for each target.
  Combined loss: ped_MAE + 0.5 * masked_park_MAE. Parking loss masked to 138 sensor streets.
  step_11 rollout now uses pred_park to update occupancy_rate in the sliding window,
  replacing the first-order displacement heuristic with model-learned spillover.
- Known weakness: Parking head has no supervision on 1,259 non-sensor streets — predictions
  there are extrapolations. Must state as thesis limitation.
- Paper justification: Multi-task learning with shared encoder (standard GNN literature).

### 2026-04-10 — Decided NOT to clip network to Melbourne CBD
- Decision: Keep all 1,397 streets; do not clip to strict CBD (188 streets) or LGA (1,202)
- Options: Clip to Hoddle Grid (188), clip to LGA (1,202), keep all 1,397 (chosen)
- Reasoning: 188 nodes too small for GNN; 95.2% of streets are confidence=0.5 regardless
  of geography (sensor density is the bottleneck, not geography); spillover analysis
  requires boundary streets — removing them truncates the propagation artificially.
- Thesis framing: "1,397 streets within and adjacent to City of Melbourne LGA, retaining
  boundary streets for valid spillover propagation."
- Step 12 will clip the Mapbox visualisation to LGA for clean presentation.

### 2026-04-10 — Imputation feature enrichment (Steps 01–05 OVERRIDE)
- Decision: Add 6 new features to static_features to improve XGBoost R² above 0.6 threshold
- Options: Network centrality only (cheaper), + transit proximity (chosen), + footpath width
  (road segments dataset, width often null, deferred)
- Chosen features:
    cbd_distance_m         — haversine to Hoddle Grid centre (step_02)
    betweenness_centrality — NetworkX on spatial_edges (step_04)
    nearest_tram_stop_m    — PTV GeoJSON filtered MODE=="TRAM", Melbourne bbox (step_04)
    tram_stops_200m        — count within 200m (step_04)
    bus_stop_on_street     — roadseg_id direct match (step_04)
    nearest_bus_stop_m     — spatial nearest bus stop (step_04)
- Data sources: PTV public_transport_stops.geojson (all Victoria, CC-BY 4.0) for trams;
  Melbourne Open Data bus-stops dataset (309 signs) for buses.
- City Circle tram dataset (28 stops) rejected — tourist loop only, misses Swanston/Collins.
- XGBoost capacity: CV model n_estimators 200→400, max_depth 5→6;
  final model 500→600, max_depth 5→6.
- Known weakness: R² improvement not guaranteed. Selection bias (sensor streets 5.5x busier
  than imputed streets) is the fundamental ceiling — no feature set fully bridges this.
  If R² stays below 0.6, all imputed streets remain at confidence=0.5.
- Paper justification: Betweenness centrality as pedestrian flow predictor — space syntax
  literature (Hillier & Hanson 1984; Penn 2003). Transit proximity — standard activity
  generation model (Ewing & Cervero 2010).

### [Add new decisions below this line]
