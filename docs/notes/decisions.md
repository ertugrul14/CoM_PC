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

### [Add new decisions below this line]
