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

### D-009 — 2026-04-30 — Time-aware parking event cap (Step 05 OVERRIDE)
- Decision: Replace global 4-hour `EVENT_CAP_SECONDS` with a time-aware per-event cap
- Problem: Global 4h cap truncates legitimate overnight stays on the 82% of CBD bays that become unrestricted after ~8:30pm. Evening scenarios (8pm–midnight) ran against an artificially deflated parking baseline, underestimating the cost of pedestrianisation.
- Options: (A) restrict scenarios to pre-8pm only; (B) time-aware cap [chosen]; (C) global cap + uncertainty flag; (D) no cap overnight; (E) raise global cap to 10h
- Chosen approach: events starting during restriction hours (~7:30am–7:30pm) keep 4h cap; events starting in unrestricted hours (~7:30pm–7:30am) are capped at next morning's restriction start. Derived from `on-street-car-park-bay-restrictions.csv` (4,263 bays).
- Known weakness: restriction cutoff times are global approximations (mode of per-bay distribution); per-bay matching not possible with current ID schemes; overnight sensor artifacts cannot be filtered
- Full detail: `docs/notes/decision_time_aware_event_cap.md`
- Steps affected: 05 → 12 cascade + model retraining

### [Add new decisions below this line]

### D-010 — 2026-06-02 — Ablation experiment: imputation does not improve sensor-street accuracy
- Decision: Empirically tested whether XGBoost-imputed streets improve GCN prediction on 74 real sensor streets
- Conditions tested:
  - Baseline: existing model evaluated on sensor streets only → test MAE 30.72, R² 0.815
  - Exp A: 74-node subgraph, real data only → test MAE 22.34, R² 0.929 (BEST)
  - Exp B: full 1,397-node graph, ped loss masked to 74 sensors → test MAE 22.84, R² 0.917
- Key findings: (1) published all-street MAE (5.5) is 5.5x better than sensor-street MAE (30.7); (2) loss masking yields 27% MAE improvement; (3) imputation does not help — sparse subgraph outperforms full graph; (4) pedestrian flow is primarily temporal
- Thesis framing: imputation enables city-wide coverage for scenario simulation, not prediction accuracy. Loss masking recommended as default when mixing real/imputed data.
- Known weakness: Exp A's advantage may partly reflect fewer parameters competing for the same signal, not just cleaner data. Single seed (42) — no confidence intervals.
- Full report: `docs/notes/ablation_experiment_report.md`

### 2026-04-18 — Step 12 frontend WebGL failure handling
- Decision: Add explicit WebGL preflight and map-constructor fallback handling in sensor_map_viz.html.
- Options considered: Keep current behavior (hard crash on map init error), add guarded init with in-page error message (chosen).
- Chosen approach: Check mapboxgl availability + mapboxgl.supported() before creating the map, wrap new mapboxgl.Map(...) in try/catch, and show a visible startup error inside the token prompt.
- Known weakness: This improves failure UX but does not resolve root causes such as disabled GPU acceleration, remote desktop rendering limits, or browser policy restrictions.
- Paper justification: Not applicable (frontend runtime robustness decision).


### 2026-06-03 — D-011: Weather timezone alignment (Category-A fix #1)
- **Decision:** Fetch Open-Meteo weather in Melbourne local time (`timezone=Australia/Melbourne`) instead of UTC, so the weather wall-clock axis matches the activity data (whose `local_datetime` is Melbourne wall-clock relabelled UTC).
- **Bug fixed:** weather was fetched as true UTC (step_01:200) and merged onto the Melbourne-relabelled time_bin axis (step_03/step_05) -> an 11h misalignment. Verified empirically: mean temperature peaked at time_bin label 04:00 and bottomed at 19:00 (physically inverted). After the fix it peaks at 14:00 and bottoms at 05:00 (correct summer diurnal cycle).
- **Why this matters:** prime suspect for the negative weather feature importance (permuting temp/wind/precip/humidity *improved* val MAE) flagged in the stress-test. Weather features were paired with the wrong time of day, so the model learned anti-correlations.
- **Scope:** OVERRIDE on frozen steps. Edited config.py (new MELBOURNE_TZ constant), step_01_fetch.py (use it), step_03_temporal.py (clarifying comment). Refetched weather_raw.parquet + rebuilt weather.parquet. Window Nov 2025-Mar 2026 is entirely AEDT (UTC+11), no DST transition inside it.
- **Falsifiable check after retrain:** weather feature importance should rise (become less negative / positive).
- **Options considered:** (A chosen) refetch in Melbourne tz - root-cause, DST-robust, no magic constant; (B) +11h shift in _build_weather - equivalent but hardcodes offset; (C) leave as-is and caveat - rejected, it is a real validity bug.
- Companion: docs/notes/thesis_defensibility_checklist.md (item A.1).


### 2026-06-03 — D-012: log1p normalisation for ped_flow (Category-A fix #2)
- **Decision:** Apply log1p to ped_flow BEFORE the global z-score; keep occupancy_rate on plain global z-score. Chosen over per-node z-scoring (the checklist's original proposal).
- **Problem fixed:** single global mean/std (28.2/61.2) put high-traffic sensor streets at z=4-10 in the tail (step_08 global norm). ped_flow is a right-skewed count; log1p compresses the tail while KEEPING cross-street magnitude (a per-node z-score would erase magnitude and compute noisy stats on the 1,323 imputed streets).
- **Why log1p over per-node:** (1) preserves magnitude signal the spatial GCN uses; (2) no JSON schema change (norm_stats.json stays scalar mean/std, computed in log space); (3) lower blast radius. occupancy_rate is bounded [0,1] (max 0.41) so it has no tail and needs no transform — leaving it linear avoided touching every parking de-norm site.
- **Contract:** config.LOG_NORMALISE_FEATURES + normalise_feature()/denormalise_feature() helpers are the single source of truth so the transform cannot be applied inconsistently. train_lightning.py keeps a matching local constant (it is standalone, no config import).
- **Files:** config.py (helpers+constant), step_08_cube.py (stats in log space), step_09_train.py + train_lightning.py (_normalise_cube + _evaluate expm1 de-norm), step_10_interpret.py (de-norm x2), step_11_scenario.py (boost inject, chart de-norm, true raw delta), step_11_opportunities.py (de-norm-then-average x3).
- **Nonlinearity footgun handled:** log1p de-norm is nonlinear, so (a) ped deltas must be expm1(treated)-expm1(baseline), NOT delta*std (step_11_scenario:542/824); (b) ped means must be de-norm-then-average, NOT average-then-de-norm (step_11_opportunities:116/150/151).
- **Verified:** helper round-trip err ~1e-12; all edited modules import clean. NOT yet retrained (pending cube rebuild + Lightning).
- **Falsifiable check after retrain:** ped MAE stays in raw units (expm1 in _evaluate) so comparable to the 5.535 baseline; tail-street errors should be better calibrated.


### 2026-06-03 — D-013/014/015: Category-A fixes #3, #4, #5 (sensor outages, per-street confidence, non-sensor parking)

**D-013 — Missing-vs-zero (fix #3).** step_05 no longer 0-fills every missing sensor bin. Short gaps (<= SHORT_GAP_BINS=4 bins = 1h) are linearly interpolated; longer runs (and edge runs) are treated as sensor OUTAGES — kept 0 in the cube but flagged ped_valid=False. step_08 emits ped_valid_mask.pt [N,T]; the GNN ped loss and eval metrics exclude masked bins in BOTH trainers (train_lightning.py + step_09_train.py). Rebuild result: observed=954,584, short-interp=44,053, masked-outage=66,963 (6.3% of sensor bins). A 0-filled outage otherwise mimics a quiet period and creates fake flexibility windows.
- _sample_windows gained opt-in (ped_valid_mask, return_ped_mask) returning a 4th mask tensor; default 3-tuple preserved so step_10's callers are unaffected.
- NOTE: reported ped MAE after retrain is now over OBSERVED bins only — slightly different denominator than the old 5.535, by design (more honest).

**D-014 — Per-street imputation confidence (fix #4).** Replaced the single global tier with similarity transfer: each imputed street borrows the mean CV R2 of its CONF_KNN=5 nearest sensored streets in z-scored static-feature space; mean R2 >= 0.6 -> 0.8 tier else 0.5. Rebuild result: only 2 streets reach 0.8, 1321 at 0.5. This is HONEST, not a bug — median per-street imputation R2 is 0.177 (near noise), so almost no imputed street earns high confidence. The 0.8 tier is now materialised but correctly tiny. Confidence is currently a reporting/ranking signal (not yet wired into message passing).

**D-015 — Non-sensor occupancy = unknown (fix #5).** step_08 imputes occupancy for the 1,254 non-sensor streets to the sensor MEAN (0.2897) instead of 0. After z-score this is ~neutral ("unknown"), not "definitely empty". Parking head loss + eval stay masked to the 143 real sensor streets.

**Verification:** cube (1397,14400,23); ped_flow norm_stats in log space (mean 2.46/std 1.42); cube kept raw (normalise on the fly); normalised ped z-range now [-1.73, 3.42] (heavy z=4-10 tail eliminated); round-trip err ~4e-4; ped_valid_mask 99.67% valid. All 10 edited modules byte-compile.

**Lightning retrain requirement:** the bundle MUST now include the NEW ped_valid_mask.pt (train_lightning.py loads it) alongside the rebuilt cube.npy, norm_stats.json, cube_meta.json, graph_*.pt, parking_occupancy.parquet, node_index.parquet.


### 2026-06-04 — D-016: Pedestrian-loss scope flag (Phase 3) + Phase-1 validation results

**Phase 1 (post-retrain validation, all local) — complete.** New Category-A model placed in models/.
- Weather flip CONFIRMED (D-011): temperature permutation delta_MAE -0.63 -> +0.30 (harmful -> useful). Secondary weather still weak (noise floor). Semantic branch contribution 2.12 -> 4.74; ped_confidence now #2 feature.
- **Sensor-only eval (scripts/eval_sensor_only.py): TEST all-street 4.45 but 74-SENSOR MAE = 31.4 (R2 0.841).** Category-A fixes improved the all-street COVERAGE metric, NOT sensor-street accuracy (~31 vs old ~30.7). Exp A/B reached 22-23 because they masked ped loss to sensors.
- Re-clustered (06/07): k=4 holds; archetypes shifted to EVENING (weather-fix downstream effect). Fixed pre-existing dual-head bug in step_11_opportunities (model(X)[0,:,0] -> [0][0,:,0]); re-ran opportunities + step12.

**D-016 — ped-loss scope.** Added PED_LOSS_SCOPE flag to train_lightning.py: 'all' (train ped on 1397, = current model) vs 'sensors' (train ped only on 74 real sensor streets; imputed streets stay graph context). Set to 'sensors' for the next run. Builds sensor mask from ped_confidence==1.0 (unnormalised). Loss + early-stop metrics restricted to sensor streets when scope='sensors'. Hypothesis: sensor MAE should drop from ~31 toward Exp B's ~22.8. training_bundle.zip rebuilt with this script.

**Next:** (Phase 3) user runs masked retrain on Lightning -> download model -> run eval_sensor_only.py to confirm sensor MAE drop. (Phase 2) hardened multi-seed ablation still pending. (Phase 4) final comprehensive report after masked result is in. Pending local: scenario-tool gating to data-streets.


### 2026-06-04 - D-017: Scenario honesty gate (Phase 3 local)

**What.** step_11_scenario.run_scenario() now refuses, by default, to run a counterfactual on a street that lacks a real sensor for the signal the intervention actually perturbs. The gate is intervention-type aware:
- pedestrianise / restrict_park perturb occupancy_rate -> require a real PARKING sensor (parking_mask[node] True).
- boost_ped perturbs ped_flow -> require a real PED sensor (cube ped_confidence == 1.0).

**How.** New helper `_data_backing(node_idx, intervention_type, parking_mask, node_ped_conf) -> (bool, reason)`. The gate runs after artifact load + bounds check, before the rollouts. If not data-backed and `allow_imputed=False` (default) it raises ValueError with the reason; `allow_imputed=True` proceeds but stamps the output. Result meta now carries `data_backed`, `data_backing_reason`, `target_ped_confidence`. api_server.py passes `allow_imputed` from the request body and returns HTTP 422 (not 500) on gate rejection so the frontend can show the reason / offer an override toggle.

**Why.** Imputation exists for network CONNECTIVITY (every node carries graph messages), but a quantitative claim about an intervention is only defensible where the manipulated quantity is measured. This operationalises the thesis honesty stance: scenarios are RUN on data-backed streets; imputed streets remain graph context only. Files changed: step_11_scenario.py, api_server.py (2 files). Verified: `_data_backing` unit cases pass (park-sensor/no-park/ped-sensor/imputed/no-mask); both files byte-compile. Not yet exercised end-to-end (would load the 1.85GB cube) - gate logic is pure and unit-tested.

### 2026-06-04 - D-018: Ablation script hardening (Phase 2 prep)

**What.** melbourne_pipeline/experiments/ablation_sensor_only.py updated for the post-Category-A cube. Two changes:
1. **log1p de-norm contract.** `_evaluate_masked` previously inverted ped predictions with linear `z*std+mu`. After D-012 the cube stores ped_flow in log1p space, so this would report MAE in log units. Now routes ped de-norm through config.denormalise_feature (expm1 inverse); parking stays linear. This is a correctness fix - the old ablation numbers (22-31) were computed under the linear contract and must not be cross-compared with re-run results.
2. **Multi-seed.** SEEDS=(42,1,2,3,4); experiments A and B retrain once per seed; main() reports test/val ped MAE and R2 as mean +/- std with per-seed breakdown, plus the B-A gap (positive => imputation hurts sensor accuracy). Baseline eval stays single (deterministic). Output JSON schema changed: experiment_*_summary now hold {test_ped_mae:{mean,std,per_seed}, runs:[...]}.

**Why.** The headline ablation claim ("imputation does not improve sensor-street accuracy; 74-node subgraph beats full graph") rested on single-seed runs (A 22.3 vs B 22.8, a 0.5 gap). Multi-seed mean +/- std makes that gap defensible against seed noise. Runs on Lightning GPU (user executes). Verified: byte-compiles (UTF-8). Norm-stats fairness caveat documented in output config block (ExpA recomputes stats on the 74-street subcube; ExpB uses global stats - each normalised to its own training distribution).

### 2026-06-04 - D-019: ExpV2 sensor-union fresh-graph ablation (steps_v2 branch)

**Context.** The 3-seed new-cube ablation showed ExpA (74-node INDUCED subgraph) = 54.71 vs ExpB (full graph, masked) = 24.97. ExpA's induced subgraph left 35/74 sensor nodes ISOLATED (sensors are spatially sparse; almost no pairs share an intersection), confounding "sensor-only training" with "degenerate graph". User question: is ExpA bad because of the data restriction or the broken topology?

**What.** New experiment V2 in a COPY of the steps folder (`melbourne_pipeline/steps_v2/`, frozen `steps/` untouched - no OVERRIDE needed since originals are not modified). Node set = ped+park sensor UNION = 189 streets (74 ped, 143 park, 28 both) - all REAL data, zero imputation. Build FRESH graphs directly on these 189 (not an induced subgraph): spatial k-NN(6) Gaussian on EPSG:3111 centroids (0 isolates); semantic mutual-k-NN(6) cosine on z-scored log1p land-use (6 isolates, covered by self-loops + spatial). Slice the existing full cube to the 189 nodes (rows are per-street, independent - valid slice, no re-impute). Train identical MultiGCN (imported from frozen steps.step_09_train), ped loss masked to the 74 ped sensors (+ ped_valid outage mask), parking masked to 143, 3 seeds.

**Files (steps_v2/):** step_04_graph.py (fresh graphs -> processed_v2/), step_08_cube.py (slice cube + adjacencies + masks -> processed_v2/), step_09_train.py (masked 3-seed trainer -> models_v2/model_eval_v2.json). Outputs isolated in data/processed_v2 + data/models_v2. Steps 04+08 RAN LOCALLY OK (189x14400x23 cube, spatial 1366 edges/0 isolates, semantic 758/6 isolates, 97.5% ped bins valid). step_09 CPU smoke-tested (2 epochs, path works). Bundled: ablation_v2_bundle.zip (27 MB, no .env) for Lightning.

**What it resolves.** If V2 ~30-40: ExpA's isolation artifact explained part of its badness BUT intermediate streets still add value (thesis survives, nuanced). If V2 ~25 (== ExpB): benefit was pure connectivity, not imputed-street info (weakens imputation-accuracy claim). CAVEAT: V2's k-NN spatial edges are LESS spatially faithful than ExpB's real intersection topology (edges can jump over non-sensor streets) - so a V2 win is partly fabricated-edge connectivity, not real topology. Pending: user runs ablation_v2_bundle.zip on Lightning -> models_v2/model_eval_v2.json.

### 2026-06-05 - D-020: Hard sensor gate on flexibility windows (RQ1 honesty, steps_v2)

**Context.** step_07 emits flexibility windows (high ped AND parking<0.30) for every street in the 1397 graph. But a window is a conjunction of two signals, and for ~90% of streets BOTH halves are unobserved: ped is imputed (median per-street R2 ~0.18, D-014) and parking is extrapolated off-sensor (no supervision, D-015). The old headline "1109 streets have flexibility windows" silently treats imputed conjunctions as findings. User chose the HARD GATE strategy.

**What.** melbourne_pipeline/steps_v2/step_07_cluster.py (frozen steps/ untouched). Added `_sensor_membership` (ped sensor iff ped_confidence==1.0; park sensor iff street_id in parking_occupancy.parquet) and `_evidence_tiers`. Each street gets an evidence_tier and a hard reallocation_candidate flag. **The gate keys on the PARKING sensor, not the ped sensor**: a reallocation physically consumes kerb slack, so the parking half must be observed, not imputed; the ped sensor only upgrades a candidate from provisional (B) to confirmed (A). New clustered.parquet columns: ped_sensor, park_sensor, evidence_tier, reallocation_candidate. Outputs isolated to data/processed_v2/ (clustered.parquet, cluster_report.json, cluster_summary.json) so frozen clustered.parquet is untouched. Windows still computed for all streets (map keeps them) but only A/B are actionable.

**Tiers (ran 2026-06-05):** A_confirmed (ped+park) = 28 | B_park_provisional (park only) = 115 | C_ped_only_indicative (ped only) = 46 | D_indicative (neither) = 1208. Sum=1397; A+B=143=parking sensors. **Reallocation candidates = 143; candidates WITH a flexibility window = 87.** So the honest RQ1 headline collapses from 1109 ungated windows to **87 parking-sensor-backed, actionable candidates** (28 of them double-confirmed by a ped sensor).

**Caveats.** (1) See D-021 - the assumed timezone window-name shift was tested and does NOT exist in the aggregation path. (2) "B_park_provisional" parking slack is real but ped demand is modelled - flagged provisional, not confirmed. (3) Gate is post-hoc on existing clustering; GMM still fits on all 1397 (archetype discovery is unchanged, only the actionable trigger is gated).

### 2026-06-05 - D-021: Timezone window-names NON-bug + best_window selection (C+B)

**Timezone finding (contradicts a CLAUDE.md limitation).** The "11-hour AEDT-as-UTC shift mislabels window time-of-day" limitation was tested empirically before any fix. Mean ped_flow by stored time_bin hour peaks at **17:00** and troughs at **04:00** (textbook CBD curve); parking occupancy peaks **19:00-20:00** (evening dining), troughs 08:00. Applying +11h would move the ped peak to 04:00 - absurd. Conclusion: the stored timestamps are NAIVE LOCAL wall-clock values mislabelled with a UTC tzinfo; step_06 derives blocks via `.dt.hour`/`.dt.dayofweek` which read wall-clock fields directly, so block/day assignment was NEVER shifted. The limitation note is a tzinfo-LABEL issue, not a values issue, and is misleading for the aggregation path. **No +11h fix applied** (it would corrupt correct data). The "all windows look like night" symptom was NOT timezone - see below.

**Real cause + fix (C+B).** The night-bias came from `_flexibility_windows` returning `windows[0]` with column order starting at the `night` block: overnight parking is empty everywhere, so `night` trivially qualified and was picked first. Fix in steps_v2/step_07_cluster.py: (C) exclude the `night` block from candidacy entirely (NIGHT_BLOCK const) - reallocating empty 3am kerb is not an intervention; (B) `best_window` is now the qualifying block with the HIGHEST pedestrian demand, and `flex_all` is ranked by the same demand so flex_all[0]==best_window.

**Result (ran 2026-06-05).** Actionable gated candidates: 87 -> **83** (4 streets only qualified via night). All-tier windows: 1109 -> 1041. best_window block mix now: morning 36, work_pm 20, midday 15, work_am 11, evening 1 - zero night. Gold-tier examples now read Thu_morning / Wed_midday / Thu_work_pm instead of Mon_night. Outputs in data/processed_v2/.

**Open item.** CLAUDE.md "Known Limitations" timezone bullet should be softened to "tzinfo label is UTC but values are naive local AEDT; block/day assignment unaffected" - frozen-doc edit, deferred pending user OK.

### 2026-06-06 - D-022: Pedestrian imputation stress test (diagnostics only, no code change to frozen step)

**What.** Audited step_05 `_build_ped_complete()` XGBoost ped imputation. Read-only diagnostics:
`scripts/stresstest_ped_imputation.py` + `scripts/fix5_neighbour_predictability.py`. Full write-up:
`docs/notes/ped_imputation_stresstest.md`.

**Findings.** (F1) Covariate shift: train on 74 busy sensored arterials, impute 1,323 quieter streets
(jobs 269→75, cafés 6.9→1.3; Cohen d 0.55-0.93) — extrapolation into a sparse low tail, so GroupKFold
R² is an optimistic upper bound. (F2) `spatial_lag_ped` (the "graph-informed" feature = mean ped of
sensored neighbours) is **0 for 1,170/1,323 (88%)** imputed streets — dead exactly where needed, and
lag=0 is OOD vs sensored training rows. (F3) Outputs plausible, not collapsed. (F4) Cheap Fix-5 proxy on
sensored streets: own time-of-day climatology R²=0.70 ≫ neighbour-mean 0.235 ≈ city-mean 0.239; corr 0.82
but low R² → neighbours give SHAPE not SCALE. (F5 reframe) Strongest predictor is a street's OWN history,
which unsensored streets lack by definition → imputation is information-capped (~R² 0.18-0.24), not
model-limited. Problem = shape×scale; SCALE must come from static features.

**Two roles (resolves metric confusion).** Role A (imputed as GNN INPUT): +2.2 MAE/~8% on sensors per
ExpB vs V2 ablation — modest, real. Role B (imputed as GNN TARGET): all-street MAE 4.45 is GNN copying
XGBoost (circular) → mask ped loss to 74 sensors.

**Fix plan.** F5 decisive test (leave-streets-out GNN, needs Lightning GPU) pending. Fix1 repair dead
spatial-lag (k-hop / distance-weighted / feature-space fallback) — needs OVERRIDE. Fix2 per-street SCALE
from static features + quantile uncertainty → continuous ped_confidence — needs OVERRIDE. Fix3 masked ped
loss (protocol). Fix4 distance-stratified R² reporting (thesis).

**Status.** No frozen code modified. Fix1/Fix2 gated on user OVERRIDE.

### 2026-06-06 - D-023: Drop XGBoost pre-imputation as GNN input — GNN reconstructs unsensored flow on its own

**Decision.** Eliminate the XGBoost stage in its role as the GNN's pedestrian INPUT fill. A trivial
per-bin city-climatology fill ties XGBoost downstream, so the second model is not needed to feed the GNN.
(XGBoost may still be retained ONLY to produce the published city-wide `ped_complete.parquet` series for
visualisation/scenario coverage, where per-street plausible values are wanted — that is a separate role.)

**Evidence — decisive leave-streets-out GNN test.** Bundle `scripts/fix5_leave_streets_out_gnn.py` run on
Lightning (T4, K_FOLDS=3, FOLD_EPOCHS=200, 1 seed). Per fold, hold out M of 74 sensors (removed from ped
LOSS, real flow kept as answer key), train the GNN twice differing ONLY in the held-out nodes' input fill:
Arm A = leakage-free XGBoost imputation; Arm B = dumb city-climatology (no XGBoost). Two-cube trick
(input filled / target real) prevents grading the GNN on copying XGBoost. Result
`data/experiments/fix5_leave_streets_out_results.json`:

| Fold | XGB-only MAE | Arm A (GNN+XGB) | Arm B (GNN alone) | gap B−A |
|------|--------------|-----------------|-------------------|---------|
| 0    | 61.46        | 44.14           | 45.79             | +1.65   |
| 1    | 86.93        | 89.92           | 91.34             | +1.42   |
| 2    | 73.21        | 59.33           | 56.62             | −2.72   |
| mean | 73.87        | **64.46**       | **64.58**         | **+0.12** |

Test R²: Arm A 0.446 vs Arm B 0.440. The per-fold gap changes SIGN (+1.65, +1.42, −2.72) → the +0.12 mean
is noise, not signal. Holding the graph fixed and swapping only the fill value, the GNN is the same model
with or without XGBoost.

**Reconciliation with D-010/D-022 Role A (+2.2 MAE).** Not a contradiction — different comparisons.
Ablation Role A measured the value of HAVING the 1,323 imputed nodes in the graph at all (ExpB vs sensor-only
V2) = graph structure/coverage. Fix 5 holds the full graph fixed and measures the value of XGBoost's specific
VALUES vs a trivial baseline = ~0. Refined story: a node with roughly-right magnitude helps; the
sophistication of the fill does not.

**Caveat.** Single seed; fold-to-fold swing (−2.72..+1.65) is large vs the +0.12 mean. Direction (no
consistent benefit) is clear; for a final thesis number, re-run N_SEEDS=3 to report mean±std straddling 0.
Held-out MAE 44–90 / R² 0.32–0.66 for BOTH methods → truly-unsensored streets are hard for everyone,
reinforcing "imputation = coverage, not accuracy" (and now: the imputation METHOD is downstream-irrelevant).

**Status.** Decision recorded. No frozen step_05 edited yet — removing/replacing the GNN-input fill is the
implementation follow-up (needs OVERRIDE when step_05/08 are touched).

### 2026-06-06 - D-024: Finalized pipeline — XGBoost removed (step_05 climatology) + evidence-gated clustering (step_07)

**OVERRIDE granted.** Implements D-023 in the live pipeline. Two-layer methodology made explicit:
- **Context layer (all 1,397 streets):** values exist everywhere so the GNN can message-pass and inform
  sensored streets from their surroundings. Accuracy here is scaffolding, not a finding.
- **Evidence layer (sensored streets):** the ONLY place we train the loss, cluster, and intervene —
  because that is where ground truth exists.

**step_05_process.py — XGBoost removed.** `_build_ped_complete()` XGBoost block (GroupKFold CV + 600-tree
final model + spatial-lag/_make_X features + kNN confidence) replaced by a **city-climatology fill**:
unsensored streets get the per-bin MEAN of the 74 sensored streets (outage bins excluded), confidence 0.5,
source "climatology". Sensored streets keep real counts (confidence 1.0). Output schema unchanged
(street_id, time_bin, ped_flow, ped_confidence, source, ped_valid) → steps 06/08 untouched. Justified by
Fix 5 (D-023): the GNN reconstructs unsensored flow equally well from this trivial fill.

**step_07_cluster.py — evidence gate.** Clustering now runs ONLY on the ped∪park sensor union
(ped_confidence==1.0 OR street in parking_occupancy.parquet). Imputed streets are written as cluster=-1 /
intervention_type="context_only" (kept for the 1,397-street viz contract). Fixes the dishonesty exposed when
the flat climatology made the old all-1,397 clustering collapse: 1208 identical imputed points formed an
artificial tight cluster (silhouette 0.875, k=6) that masked real structure.

**Tested end-to-end WITHOUT training (steps 5→6→7→8 all green):**
- step_05: ped_complete 20,116,800 rows = 1397×14400; 74 sensored; city rhythm mean 114 / peak 440 ped/bin.
- step_08: cube (1397,14400,23) rebuilt; ped_valid 20,049,837/20,116,800; graphs nnz 5635/8097.
- step_07 evidence-gated: 189 clustered (74 ped, 143 park, 28 both) + 1208 context_only. The prior all-1397
  clustering scored silhouette 0.875 / ARI 0.966 — INFLATED by 1208 identical imputed points forming an
  artificial tight blob. Honest sensored-only numbers are much lower (weak real structure).

**k selection changed from BIC-min to fixed k=3 (stability diagnostic).** BIC is erratic/non-monotonic on
189 streets with full-covariance GMM (k=6 dip to 4124 is a near-singular-covariance artifact, not structure)
— see scripts/diag_cluster_k.py, which sweeps k=2..10 reporting BIC + bootstrap ARI + silhouette. No k reaches
the 0.70 ARI bar (street behaviour is a continuum, not crisp clusters). k=3 maximises silhouette (0.363) with
ARI 0.567 and stays interpretable. step_07 now FIXES k=N_CLUSTERS=3 (silhouette/stability, BIC logged but
unused). Final archetypes: major_pedestrian_corridor (28), parking_reallocation_priority (131),
latent_morning_potential (30). Honest silhouette 0.363 / ARI 0.567 reported as-is (weak-moderate structure).

**Safety.** Pre-change snapshot at data/processed_backup_pre_xgb_removal/ (1.9 GB, local). Git checkpoint
705f7e11 pushed before edits.

**FINALIZATION (2026-06-06, retrain done).** Retrained on Lightning (T4) via train_lightning.py on the new
climatology cube with Exp B masked loss. Best epoch 137. HONEST sensor-street metrics: VAL ped MAE 27.85 /
R² 0.889, park MAE 0.058 / R² 0.885; TEST ped MAE 28.35 / R² 0.877, park MAE 0.052 / R² 0.880 (RMSE 68.5 →
error concentrated in peak bins). val≈test → generalises to held-out future, no overfit. Beats old all-street
model's true sensor MAE (~30.7); the circular ~5.5 all-street headline is gone.
- New model installed into melbourne_pipeline/data/models/ (best_model.pt + sidecars).
- cube_meta.json split patched to 70/15/15 (T_train_end=10080, T_val_end=12240) — local copy was stale (11520).
- **step_09_train.py finalized to Exp B**: added PED_LOSS_SCOPE="sensors" (default), ped_sensor mask from
  ped_confidence==1.0, ped loss+metrics masked to 74 sensors via ped_eval_mask; MAX_EPOCHS 200→300. Now matches
  train_lightning.py methodology. Steps 10→12 to be re-run by user against the new model + climatology cube.

### 2026-06-21 - D-026: Feature-perturbation scenarios cannot produce a positive kerb-reallocation effect — switch to post-hoc elasticity composition

**Context.** A `restrict_park`(→0.30) scenario on street 20009 (Lonsdale St) returned a network-wide ped
DECREASE (target mean Δped −17.6), the opposite of the "Curbside Intensification" hypothesis. Investigated
whether `curbside_dining` (D-025: occupancy↓ AND land-use↑) recovers the desired sign.

**Demo (post-training, step_11 only — no retraining, no frozen edits).** Street 20009, t_start=10476,
duration=rollout=16. 12 bays reclaimed ≈ same occupancy shock as restrict_park→0.30:
| Scenario | target mean Δped |
|---|---|
| restrict_park → 0.30 | −17.6 |
| curbside_dining 12 bays, default constants | −13.7 |
| curbside_dining 12 bays, 4× café / 2× seats per bay | **−60.4** |

**Finding — perturbing features cannot deliver the desired output.** Strengthening the land-use uplift made
footfall *more* negative, not positive. Two reasons:
1. **Permutation importance is sign-agnostic.** `cafe_count` (ΔMAE +6.67) / `bar_count` (+5.01) rank high but
   pushing them UP drives the ped prediction DOWN — the learned land-use↔footfall relationship is not the
   positive causal lever the policy story assumes.
2. **Static land-use features are OOD-fragile.** They are constant per street; the model never saw a block
   with 4× its café frontage, so large perturbations are unreliable extrapolation.
`occupancy_rate` is a confounder (vitality proxy), not a lever; forcing it down reads as "quieter street".
The "111/163 streets positive" network count is a mirage — tiny deltas on low-confidence imputed
(placeholder) streets, while the sensor-street mean stays negative (−0.32).

**Decision.** Do NOT use feature perturbation to claim a footfall response to kerb reallocation. Adopt
**post-hoc elasticity composition** (Route 1): apply an externally-estimated uplift to the treated street's
baseline (`ped_treated = ped_baseline × (1 + uplift)`), then use the GNN ONLY to propagate that imposed shock
through the spatial/semantic graph for network spillover. This separates the causal assumption (literature)
from the propagation (GNN) — consistent with the project frame "GNN = coverage/propagation, not causal
accuracy". restrict_park/curbside_dining demos retained as METHODS evidence for *why* perturbation is invalid.
Rigorous alternative (Route 2, out of scope): diff-in-diff on real bay-removal events in the parking history.

**Uplift band (signed off; sources verified 2026-06-21).** Multiplicative
`ped_treated = ped_baseline × (1 + uplift)` on the target street over the active window. Band now grounded in
VERIFIED primary studies (the earlier {5,12,25,40}% was unsourced and is retracted; the "Pedestrian Pound
2018 / NACTO / SF" citations were unverified-from-memory and removed):
- **0% placebo** (sanity floor) / **+18% conservative** / **~30% central** / **+39% optimistic**.
- +18% = Cambra & Moura (2020), Lisbon street improvement, statistically significant pedestrian *volumes*
  (throughput — same metric as our ped_flow). docs/references/evidence/cambra2020.md
- +39% = Aldred & Croft (2019), Hounslow modal filter, measured walking increase.
  docs/references/evidence/aldred2019.md
- Aggregator: Living Streets "The Pedestrian Pound" 3rd Ed (2024), primary-fetched & verified.
  docs/references/evidence/livingstreets2024.md
- **Carmona et al. (2018) +94% EXCLUDED** (per decision 2026-06-21): bundles stationary/lingering activity,
  not pure flow → not metric-comparable to ped_flow.
Report the band, not a point estimate; flag UK/Portugal→Melbourne external-validity transfer.

**Induced vs diverted — empirical support for the GNN-propagation design.** Aldred & Croft (2019) found only
**~30% of the treated street's walking gain was genuinely new; ~70% was diverted from other routes.** This
independently validates the two-part scenario engine: an imposed magnitude on the treated street PLUS spatial
redistribution to neighbours. The GNN propagation models exactly that diversion component — so a treated
street's footfall gain being largely redistribution (not pure creation) is an evidence-backed property, not an
artefact.

**Implemented (step_11_scenario.py, post-training, no frozen edits, no retraining).** New
`intervention_type="reallocate_kerb"`, magnitude = uplift fraction:
- `_encode_intervention`: scales the target's ped_flow INPUT to `baseline × (1+uplift)` each active step,
  PINNED to the baseline rollout (new `step` / `baseline_ped_norm` args) so the imposed shock cannot compound.
  The boosted input drives spillover through the GNN graph.
- `run_scenario`: the target's REPORTED series is overwritten to exactly `baseline × (1+uplift)` over the
  active window, so the correlational model cannot regress the exogenous causal claim back toward neighbours
  (without this the target diluted to +3% under a +12% assumption). Neighbours keep GNN-propagated values.
- Honesty gate unchanged: reallocate_kerb perturbs ped → falls in the ped-sensor branch (needs conf 1.0).

**Sweep result (street 20009, t_start=10476, dur=roll=16, baseline mean ped ~295/15min):**
| uplift | target Δped | target Δ% | sensor-net Δ | all-net Δ | #pos/N |
|---|---|---|---|---|---|
| 0% (placebo) | +0.0 | +0.0% | +0.000 | +0.000 | 0/0 |
| 5% | +14.7 | +5.0% | +0.97 | +0.50 | 71/71 |
| 12% | +35.4 | +12.0% | +2.04 | +0.88 | 95/95 |
| 25% | +73.7 | +25.0% | +3.75 | +1.54 | 109/110 |
| 40% | +117.9 | +40.0% | +5.35 | +2.13 | 122/123 |
Target Δ% tracks the imposed uplift exactly (sanity passed); spillover is positive, monotonic; placebo is
exactly zero (no leakage). Contrast D-026 head: the SAME street under restrict_park/curbside_dining went
NEGATIVE — this is the methods evidence for why Route 1 (impose + propagate) replaces feature perturbation.

**Status.** Implemented and verified via sweep. Frontend wired (2026-06-21): sensor_map_viz.html scenario
panel now has a "Kerb reallocation (footfall uplift)" card with a 0–40% magnitude (sent to API as a fraction)
and an honesty caveat note shown only for this intervention. No api_server.py change needed (passthrough; the
result already carries treated_street/spatial_neighbours). Full detail in
docs/notes/kerb_reallocation_scenario_findings.md. Live-browser smoke test still pending. Scripts in
melbourne_pipeline/scratch/ (run_curbside_demo.py, run_reallocate_sweep.py) — diagnostics, not pipeline.

**UPDATE 2026-06-21 — Aldred & Croft (2019) primary-verified (NotebookLM read the PDF) → conservation
redistribution added.** Two answers changed the design:
1. **The +39% is GROSS, not net.** Counts rose 859→1191/day (+39%); of the +332, only ~30.8% (≈+12% of
   baseline) were genuinely new, ~69.2% (≈+27%) were DIVERTED from other streets. Throughput metric
   (manual 24h counts of people walking *through*) → clean match to ped_flow. Modal filter, NOT parking
   removal (loose proxy); no control street; exploratory/low-sample; sustained ~2yr; historic-setting
   external-validity caveat. Full detail: docs/references/evidence/aldred2019.md.
2. **Sign error exposed.** The GNN propagation made neighbours GAIN (diffusion); the evidence says ~69%
   is diverted → neighbours should LOSE. Same class of bug as the parking-conservation issue.

**Decision (user-approved): conservation/redistribution for reallocate_kerb's network effect.**
Implemented in step_11_scenario.py: impose gross +U% on the treated street; subtract the diverted share
(REALLOCATE_DIVERTED_FRACTION=0.692) from its SPATIAL neighbours, weighted by learned spatial edge
weights; the new share (REALLOCATE_NEW_FRACTION=0.308) is the only net-new footfall. GNN diffusion is
overridden for this intervention (GNN now contributes edge weights = which/how-much, not the sign).
Verified (street 20009): target tracks imposed uplift exactly; neighbours lose; **city net / target
= 0.31 = the new-trip fraction**, and neighbour losses sum to 69.2% of the treated gain (exact mass
conservation). Limitation: diversion is concentrated on 1-hop spatial neighbours (here 4); multi-hop
spread is a possible refinement. Parking displacement (occupancy side) still NOT modelled — open item.
Frontend max raised handling unchanged (band {0,18,30,39}%, default 30%); the in-UI caveat already notes
~70% is diverted.

### 2026-06-22 - D-027: Two-stage kerb-reallocation scenario UI (parking-removal % + reclaimed use)

**Problem.** The frontend offered four "intervention" cards whose human labels were wired to the
WRONG backend mechanisms (e.g. a card labelled "Restrict parking" actually ran `pedestrianise`,
occupancy→0). The vocabulary fought the mechanism, so results were hard to explain.

**Decision.** Replace the four mislabelled cards with a two-stage funnel exposed as one new
intervention type `reallocate` (step_11_scenario.py):
- **Stage A — how much parking to remove?** A 0–100% slider. `removal_frac` ∈ [0,1] scales the
  target street's `occupancy_rate` to `(1 − removal_frac)` of its current level (1.0 = full clear,
  i.e. pedestrianisation). The GNN predicts the footfall response — model-driven, correlational.
- **Stage B — reclaim it for…** A `use` ∈ {`outdoor_dining`, `greening_parklet`, `pedestrian_plaza`}.
  **Only `pedestrian_plaza` injects a footfall uplift** (the D-026 band, default +30%), applied
  analytically post-rollout with the same mass-conserving redistribution as `reallocate_kerb`
  (diverted share 69.2% subtracted from spatial neighbours). Outdoor-dining and greening/parklet
  inject NO uplift for now → pure model occupancy response.

**Honest interim state (flagged in UI + assumptions).** With no uplift attached, `outdoor_dining`
and `greening_parklet` are **numerically identical** (both are just the occupancy removal); they
differ only in label until evidence-based bands are attached. The results panel splits the treated
delta into Stage A (parking removal, model) vs Stage B (plaza uplift, evidence) so the two
contributions are never conflated.

**Why uplift only for plaza.** Plaza is the only reclaimed use the available evidence (Cambra & Moura
2020 +18%; Aldred & Croft 2019 +39%) actually measured. Attaching an uplift to dining/parklet would
be an unsupported number — deferred until sourced (consistent with the evidence-gated principle).

**Separability.** Stage B is kept out of the GRU rollout (imposed on the baseline, non-compounding),
so Stage A (one model rollout) and Stage B (analytic) compose additively with no extra model pass —
the split reported to the frontend is exact, not an attribution estimate.

**Files.** step_11_scenario.py (new `reallocate` mode in `_encode_intervention` + run_scenario split,
`removal_frac`/`use`/`uplift` params, D-027 constants); api_server.py (root, live) + melbourne_pipeline/
api_server.py forward the three new fields; sensor_map_viz.html (Step 2 funnel markup, `wizPickUse` /
`wizRemovalChanged`, `ivApply` payload, use-aware verdict label, two-stage split panel, reallocate caveat).
Legacy `applyRecommendedIntervention` maps old archetype types → {use, removal%}. No model retrained
(post-training only). No new uplift numbers introduced beyond the existing D-026 band.

---

### 2026-06-22 — D-028: Scenario serving made non-freezing (mmap + cache + thread cap)
- Decision: Fix the scenario API freezing the machine on every `/scenario` run. Three
  serving-layer changes (no model retrain, no Steps 01-09 change):
  1. Persist a normalised cube to disk (`data/processed/cube_norm.npy`) and memory-map
     both `cube.npy` and `cube_norm.npy` (`mmap_mode="r"`) in `_load_artifacts`. The
     rollout only reads small time-slices, so nothing large is resident.
  2. Cache loaded artifacts (model/cubes/graphs/parking_mask) at module level per device;
     removed a duplicate `torch.load` of both graphs in `run_scenario`.
  3. Cap CPU threads in api_server.py: `torch.set_num_threads(min(8, cores-4))`
     (override via env `SCENARIO_TORCH_THREADS`); warm the cache at startup.
- Options considered → Chosen: (a) speed up `_normalise_cube` in place — rejected, the
  cost is I/O + a 1.85 GB strided rewrite, not arithmetic; (b) mmap a precomputed norm
  cube — CHOSEN, eliminates both the recompute and the resident copy.
- Root cause (measured): `_normalise_cube` ran on every request (no caching), doing
  `np.load` 1.72 GB → `.copy()` 1.85 GB → 23 strided z-score writes = ~150 s of saturated
  disk+memory bandwidth that locked the desktop. Rollout itself was only ~6 s.
- Result (measured on the dev laptop): cold load 154.7 s → 0.03 s (mmap); one scenario
  click ≈ 6 s using 8/20 cores, machine responsive. All three intervention types
  (pedestrianise / restrict_park / boost_ped) pass end-to-end, data_backed=True.
- Known weakness: `cube_norm.npy` is derived state. Staleness is guarded by an mtime check
  against BOTH `cube.npy` and `norm_stats.json` (auto-rebuilds, one-time ~40-150 s). The
  module cache lives for the process lifetime, so after a retrain the server must be
  restarted to pick up new weights. `cube_norm.npy` is gitignored (1.85 GB).

---

### 2026-06-22 — D-029: reallocate spillover = GNN diffusion (option 2), diversion override removed
- Decision: For the `reallocate` intervention with a pedestrian-plaza use, neighbouring
  streets now RISE with the treated street (positive spillover) instead of being docked a
  diverted share. Reverses the D-026 mass-conservation override for this path only.
- Mechanism (step_11_scenario.py):
  1. The Stage-B plaza uplift is INJECTED into the treated rollout (`_encode_intervention`,
     new `uplift` param, pinned to baseline, duration-bounded), so the GNN propagates it
     through the spatial + semantic graphs. Neighbour deltas are now the model's own learned
     spillover (`ped_delta_raw = treated - baseline`), no `REALLOCATE_DIVERTED_FRACTION`.
  2. The TREATED node's own delta is pinned to the imposed evidence uplift (baseline × uplift):
     the recorded model output for the treated node is muted by the occupancy-removal
     correlation, so without this pin the treated street would read ~flat/negative.
  - The analytic Stage A/B split + its JSON export (`ped_flow_stageA_parking` /
    `ped_flow_stageB_uplift`) are dropped; the frontend split panel was already removed.
- Options considered → Chosen: (1) lower diverted fraction for plazas; (2) **let the GNN
  diffusion stand [CHOSEN]**; (3) feeder-vs-competitor split. User picked (2): neighbours
  should reflect the model's spatial spread, not a forced redistribution.
- Verified (sensor street 20001, +30% plaza uplift): treated d=+1.99; spatial neighbours
  3 up / 1 down (feeders 20143 +0.26, 20144 +0.17; one competitor 20206 -0.10);
  top_affected 9/20 positive. Legacy pedestrianise/restrict_park/boost_ped unchanged.
- Production default (UI): pedestrian_plaza now applies +18% (Cambra & Moura 2020, Lisbon
  conservative band), set as a hardcoded constant in sensor_map_viz.html (the user-selectable
  18/30/39 footfall band was removed). The verification above used +30%; the spillover is
  linear in the injected uplift, so the spatial pattern is identical and magnitudes scale
  ~0.6×. The backend assumptions text is dynamic and reads the actual uplift sent.
- Known weakness / thesis note: this adopts the agglomeration view (intervention GROWS
  footfall area-wide) over the diversion view (Aldred & Croft 2019: ~69% rerouted). Defensible
  for destination-type placemaking (plazas), less so for a pure modal filter. The single
  neighbour that dips is the GNN's honest learned response, not a conservation rule. `reallocate_kerb`
  (legacy, not used by the UI) still uses the D-026 diversion override — left intact.
