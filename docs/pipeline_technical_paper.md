# A Spatio-Temporal Graph Neural Network for Curbside Intensification Analysis in the Melbourne CBD

**Technical report — methodology, model variants, and evaluation**
Study window: 2025-11-01 – 2026-03-31 · Modelled graph: N = 1,397 streets · T = 14,400 fifteen-minute bins · F = 23 features

---

## Abstract

We describe an end-to-end pipeline that learns the joint spatio-temporal behaviour of pedestrian flow and on-street parking occupancy across the Melbourne central business district, in order to support *curbside intensification* — the time-varying reallocation of kerb space between vehicle storage and pedestrian use. The core model is a dual-head graph neural network (two graph-convolutional branches over distinct adjacency structures, a recurrent temporal encoder, and separate pedestrian and parking prediction heads). We document the data foundation (23 datacube features), the two graph constructions (physical-adjacency and functional-similarity), the pedestrian imputation that extends sparse sensor coverage to the full network, and — the central contribution of this report — a controlled comparison of four training regimes that isolates *what actually drives predictive accuracy*. We evaluate honestly on the 74 streets with real pedestrian sensors rather than on the misleading all-street average, and we are explicit about a methodological correction: an earlier ablation that appeared to show imputation provides no accuracy benefit was confounded by a degenerate graph; a corrected experiment shows the benefit is real but modest (~2.2 MAE). We close with the recommended default training protocol and a candid threats-to-validity analysis.

---

## 1. Problem and motivation

### 1.1 The planning problem

Urban street design is effectively static: the allocation of the kerb — the contested 2–3 m strip at the road edge — is fixed by signage and regulation and changes on the timescale of years. Street *use*, by contrast, changes on the timescale of hours. A block dominated by parked cars at 03:00 may be saturated with pedestrians and outdoor dining at 19:00. **Curbside intensification** is the proposition that kerb function should be reallocated dynamically — that a street with high pedestrian demand and low parking utilisation during a given window is a candidate for temporary pedestrianisation or parking restriction.

The decision is hard for three reasons:

1. **It is spatio-temporal.** Whether a reallocation is justified depends jointly on *where* a street sits in the network and *when* it is observed. Neither a purely spatial (land-use) nor a purely temporal (time-series) view is sufficient.
2. **It is networked.** An intervention on one street displaces demand to its neighbours. Evaluating a single-street change requires a model of propagation, not just of the target street.
3. **It is data-sparse.** Pedestrian and parking sensors exist on a minority of streets. Any city-wide recommendation must reason about streets it cannot directly observe, and must be honest about the resulting uncertainty.

Current practice (e.g. NYC Dynamic Curb Management, Seattle Flex Zones) addresses this through manual observation and consultation, not algorithmic inference. The motivation for the present work is to test whether a data-driven spatio-temporal model can supply the missing quantitative layer.

### 1.2 Why a spatio-temporal GNN

A graph neural network is the natural representational match: streets are nodes, physical and functional relationships are edges, and a recurrent encoder captures the daily/weekly rhythm. The graph structure additionally provides the propagation mechanism required for scenario analysis (§5, §8) — a counterfactual perturbation on one node flows to its neighbours through the learned message-passing.

### 1.3 Research questions

- **RQ1.** Which streets exhibit temporal *flexibility windows* (high pedestrian demand coincident with low parking demand) that make curbside reallocation viable?
- **RQ2.** What spatio-temporal factors drive street-level pedestrian behaviour?
- **RQ3.** How does a single-street curbside intervention propagate across the surrounding network?

This report concentrates on the modelling foundation that underpins all three, and in particular on RQ2 — establishing, through controlled ablation, *what the model actually learns from* and therefore how much confidence each prediction warrants.

---

## 2. Data foundation

### 2.1 Sources and target

| Source | Contents | Role |
|---|---|---|
| Supabase (`parking_melbourne`, `ped_melbourne`) | Raw parking events and pedestrian counts | Targets + autoregressive inputs |
| City of Melbourne CLUE | Cafés, bars, businesses, employment, floorspace, landmarks | Static land-use features |
| Open-Meteo | Hourly weather archive (fetched in `Australia/Melbourne`) | Time-varying exogenous features |

The modelling targets are two per-street time series: **pedestrian flow** (`ped_flow`, counts per 15-minute bin) and **parking occupancy** (`occupancy_rate`, a fraction in [0, 1]). The model predicts both at the next time step.

### 2.2 The datacube

The assembled tensor (`cube.npy`, described by `cube_meta.json`) has shape **[N = 1,397 streets × T = 14,400 bins × F = 23 features]**, `float32`. The cube is stored in raw (un-normalised) units and normalised on the fly at training time, so that the normalisation contract lives in a single pair of helper functions (`config.normalise_feature` / `denormalise_feature`) and cannot drift between training and inference.

### 2.3 The 23 features

**Dynamic / signal features** (time-varying, per street):

| # | Feature | Unit | Meaning & rationale |
|---|---|---|---|
| 0 | `ped_flow` | persons / 15 min | The pedestrian target; also the dominant autoregressive predictor (§6). |
| 1 | `occupancy_rate` | fraction [0,1] | The parking target; couples the two heads (kerb trade-off). |
| 2 | `ped_confidence` | ordinal {0.5, 0.8, 1.0} | Per-street trust in the pedestrian signal (1.0 = real sensor). Carries the data-quality signal into the model (§4). |

**Temporal / cyclical features** (shared across streets, vary over time):

| # | Feature | Unit | Meaning & rationale |
|---|---|---|---|
| 3–4 | `hour_sin`, `hour_cos` | [-1,1] | Hour-of-day on a circle, so 23:45 and 00:00 are adjacent rather than maximally distant. |
| 5–6 | `dow_sin`, `dow_cos` | [-1,1] | Day-of-week on a circle (weekly rhythm). |
| 7 | `is_weekend` | binary | Coarse weekday/weekend regime switch. |
| 8 | `is_public_holiday` | binary | Victorian public holidays (atypical demand). |
| 9 | `is_school_holiday` | binary | Victorian school holidays (atypical demand). |

**Weather features** (shared across streets, vary over time):

| # | Feature | Unit | Meaning & rationale |
|---|---|---|---|
| 10 | `temperature_2m` | °C | Comfort driver of outdoor activity. |
| 11 | `relative_humidity_2m` | % | Secondary comfort driver. |
| 12 | `wind_speed_10m` | km/h | Secondary comfort driver. |
| 13 | `precipitation` | mm | Suppressor of pedestrian activity. |

**Land-use / POI features** (static per street; broadcast across time):

| # | Feature | Unit | Meaning & rationale |
|---|---|---|---|
| 14 | `total_jobs` | count | Employment in the street's catchment — commuter-flow proxy. |
| 15 | `cafe_count` | count | Café establishments — daytime footfall generator. |
| 16 | `cafe_total_seats` | seats | Café capacity — intensity of café-driven demand. |
| 17 | `bar_count` | count | Bar/pub establishments — evening footfall generator. |
| 18 | `bar_patron_capacity` | patrons | Bar capacity — intensity of nightlife demand. |
| 19 | `business_count` | count | General commercial density. |
| 20 | `poi_total` | count | Aggregate points-of-interest density. |
| 21 | `dining_capacity` | seats+patrons | Combined dining/drinking capacity. |

**Geometric feature** (static per street):

| # | Feature | Unit | Meaning & rationale |
|---|---|---|---|
| 22 | `area_m2` | m² | Street-surface area — normalises absolute counts to a physical footprint. |

### 2.4 Normalisation and splits

Continuous features (indices 0, 1, 10–22; constant `NORMALISE_IDX` in `step_08_cube`) are z-scored using per-feature mean/std computed on the **training portion only** (`norm_stats.json`). Cyclical features (already bounded in [-1, 1]), binary flags, and the ordinal `ped_confidence` are left unscaled.

One feature receives special treatment. `ped_flow` is heavily right-skewed (a few thoroughfares carry orders of magnitude more footfall than side streets); naïve z-scoring leaves a long z = 4–10 tail the model cannot fit. We therefore apply a `log1p` transform *before* z-scoring, with `expm1` as the inverse (decision D-012). This is a non-linear transform, so all downstream comparisons (deltas, means) are computed in raw count space after inversion, never in log space.

**Chronological split** (no temporal leakage):

| Split | Bins | Fraction | Use |
|---|---|---|---|
| Train | 0 – 10,079 | 70% | Parameter fitting + normalisation stats |
| Validation | 10,080 – 12,239 | 15% | Early stopping (on pedestrian MAE) |
| Test | 12,240 – 14,399 | 15% | Held-out; reported once |

---

## 3. Graph construction

Streets become nodes via `node_index.parquet` (street_id → 0-based index). Two adjacency structures are built (`step_04_graph`, materialised as `graph_spatial.pt` and `graph_semantic.pt`), each symmetrically normalised as A_norm = D^(-1/2)(A + I)D^(-1/2) at cube-assembly time.

### 3.1 Spatial graph — physical adjacency

**Construction.** Two streets are connected if their polygon boundaries physically touch (share at least a point), i.e. they meet at a real intersection. Edge weight is a Gaussian kernel of centroid distance, w = exp(-d / σ), with σ the median centroid distance across all intersection edges.

**Statistics.** ~5,635 directed edges. A small number of geometrically isolated eligible streets (≈25) have no touching neighbour; each is connected to its single nearest centroid neighbour (k = 1) with **weight 0**, purely so that every node has a well-defined adjacency row. These zero-weight fallbacks do not pass signal.

**Honest note on connectivity.** The nominal "0 isolates / 1 component" figure is partly cosmetic — it counts the zero-weight fallbacks. The true *touching* giant component covers ≈92.9% of nodes; ≈25 nodes are genuine geometric isolates carried only by the fallback. This matters for interpretation (those nodes receive no real spatial message-passing) and is revisited in §5.

**What it captures.** *Where a street is.* Physical influence: a change on one street can propagate to streets it physically connects to.

### 3.2 Semantic graph — functional similarity

**Construction.** Each street is represented by its z-scored `log1p` land-use vector (the static features of §2.3). Pairwise cosine similarity is computed, and edges are formed by **mutual k-nearest-neighbours**: an edge (i, j) exists only if j is in i's top-K most-similar list *and* i is in j's. Edge weight is the cosine similarity.

**Edge criterion (explicit).** Edge ⟺ mutual top-K cosine membership. The mutual constraint is deliberate: one-sided k-NN produces hub-and-spoke degeneration (a few generic streets attract many edges); mutuality keeps the graph sparse and symmetric.

**Statistics.** ~8,097 directed edges.

**What it captures.** *What a street is.* Functional influence: a café-and-bar dining strip is connected to other dining strips even when they are spatially distant. This is the channel through which the model can transfer behavioural patterns between functionally similar but physically separated streets.

### 3.3 Contrast

The two graphs are complementary. The spatial graph encodes proximity (relevant to displacement and physical spillover); the semantic graph encodes typology (relevant to "streets like this one behave like this"). Their relative contributions are quantified in §6 (branch-zeroing): the spatial branch dominates, the semantic branch is meaningful but secondary.

---

## 4. Pedestrian imputation and confidence

### 4.1 The coverage problem

Only **74** of the 1,397 modelled streets carry real pedestrian sensors (and 143 carry parking sensors). A city-wide recommendation requires a pedestrian value on every street, so the unsensored streets are imputed.

### 4.2 Imputation model

Imputation (`step_05_process`) uses **XGBoost** with:

- **`GroupKFold(5)` over streets** — the cross-validation folds are split by *street*, so reported R² measures generalisation to *unseen streets*, not unseen times. This is the honest estimator for an imputation whose job is exactly to predict streets it has never seen.
- **`log1p` target** (same skew rationale as §2.4).
- **A spatial-graph lag feature** — the mean `ped_flow` of a street's *sensored* spatial neighbours at each bin, giving the model a "what are nearby streets seeing now?" signal.

### 4.3 Confidence tiering

Imputation quality is not uniform, so each street carries a `ped_confidence` tier (decision D-014, via similarity transfer): a street inherits the mean cross-validated R² of its 5 nearest sensored streets in static-feature space; mean R² ≥ 0.6 → tier 0.8, else tier 0.5. Real sensors are tier 1.0.

**Candid quantitative statement.** The imputation is weak. The **median per-street cross-validated R² is ≈ 0.177** — close to noise. Consequently only **2 streets** earn the 0.8 tier; the overwhelming majority of imputed streets sit at 0.5. The confidence field is therefore not cosmetic: it correctly encodes that most imputed pedestrian values are low-trust estimates. We use `ped_confidence` as a feature and as a ranking weight in scenario analysis, not as a license to treat imputed streets as observed.

### 4.4 Does imputation help accuracy? (corrected)

This question was the subject of a methodological correction that is worth stating plainly, because the first answer was wrong.

- **Initial ablation (superseded).** A 74-node "sensor-only" subgraph (Exp A) was compared to the full imputed graph. Exp A appeared roughly competitive, supporting the conclusion *"imputation aids city-wide coverage but not sensor-street accuracy."*
- **The confound.** Exp A's graph was an **induced subgraph** of the intersection-topology graph: it kept only edges where *both* endpoints were sensors. Because sensors are spatially sparse, this left **35 of 74 nodes completely isolated**. Exp A therefore conflated "training on sensors only" with "training on a half-disconnected graph."
- **Corrected experiment (V2, §5).** Building a *fresh, fully-connected* graph on the 189-street sensor union (no isolates) yields a fair sensor-only baseline at **27.16 MAE**, versus the full imputed graph's **24.97 MAE** (both test, sensor streets, 3 seeds; `model_eval_v2.json`, `ablation_results04.06.26.json`).

**Honest conclusion.** Imputation *does* improve sensor-street accuracy, but by a **modest, quantified margin of ≈ 2.2 MAE (~8%)** — not the large gap the broken Exp A implied, and not zero. Its primary justification remains city-wide *coverage* (it is what makes scenario propagation possible on unobserved streets), with a secondary, real, but small accuracy benefit. A residual caveat (§7) is that this 2.2 MAE conflates the value of the imputed *nodes* with the value of the full graph's *real topology*, so 2.2 is an upper bound on the imputation-specific contribution.

---

## 5. Model variants and training regimes

### 5.1 Shared architecture — dual-head MultiGCN

All variants use the same architecture (`steps.step_09_train.MultiGCN`, 68,076 parameters):

```
Input  [B, W=96, N, F=23]
  ├─ GCN branch (spatial graph)   F → 64   ─┐ concat → [B, W, N, 128]
  ├─ GCN branch (semantic graph)  F → 64   ─┘
  └─ GRU (2 layers, dropout 0.1)  → [B, N, 64]
        ├─ head_ped   → ped_flow(t+1)        + per-node bias
        └─ head_park  → occupancy_rate(t+1)  + per-node bias
```

Two graph-convolutional branches (one per adjacency) are applied at each timestep and concatenated; a 2-layer GRU compresses the 96-step (24-hour) window into a per-node hidden state; two linear heads predict the next-step pedestrian flow and parking occupancy. Each head carries a **per-node bias**, which lets every street learn its own mean level — the single highest-leverage parameter for R² on spatially heterogeneous data.

**Loss.** L = MAE_ped + 0.5 · masked_MAE_park. The parking loss is **masked to the 143 streets with real parking sensors**; the remaining streets carry an "unknown" (sensor-mean) occupancy and contribute no parking gradient (decision D-015). The variants differ in whether the *pedestrian* loss is similarly masked.

**Training protocol (shared).** Chronological 70/15/15 split (§2.4); sliding window W = 96, stride 1; Adam, lr = 1e-3, ReduceLROnPlateau; 256 gradient steps/epoch, batch = 8 windows; early stopping on validation pedestrian MAE (patience 25); seed 42 for single-seed runs, seeds {42, 1, 2} for the multi-seed ablations.

### 5.2 The variants

#### V1 — Deployed full-graph dual-head model
- **Purpose / hypothesis.** The production model: predict every street well enough to drive city-wide scenario analysis.
- **Graphs.** Full 1,397-node spatial + semantic.
- **Loss.** Pedestrian loss on **all** streets (no mask); parking masked to 143.
- **Protocol.** Single seed (42), best epoch 179 (`model_eval.json`).
- **Results (test).** All-street ped MAE **4.45**, R² **0.911**; sensor-only ped MAE **28.02**; parking MAE **0.050**, R² **0.883**.

#### Exp A — 74-node sensor-only induced subgraph
- **Purpose / hypothesis.** Does training only on observed streets match or beat training on the full imputed graph?
- **Graphs.** Induced subgraph on the 74 ped-sensor nodes — **35 isolated** (see §4.4).
- **Loss.** Pedestrian loss on the 74 nodes.
- **Protocol.** 3 seeds; norm-stats recomputed on the 74-street subcube.
- **Results (test, sensor).** ped MAE **54.71 ± 1.32**, R² **0.566 ± 0.030** (`ablation_results04.06.26.json`).
- **Verdict.** Confounded by the disconnected graph; retained only to demonstrate the topology pitfall.

#### Exp B — full graph, pedestrian loss masked to sensors
- **Purpose / hypothesis.** Keep the full graph for connectivity, but learn the pedestrian signal *only* from real sensors (avoid training on noisy imputed targets).
- **Graphs.** Full 1,397-node spatial + semantic (real topology).
- **Loss.** Pedestrian loss **masked to the 74** ped-sensor nodes; parking masked to 143.
- **Protocol.** 3 seeds.
- **Results (test).** Sensor ped MAE **24.97 ± 0.28**, R² **0.884 ± 0.004**; all-street ped MAE **7.4–7.8**.

#### V2 — sensor-union fresh-graph model (the corrected sensor-only baseline)
- **Purpose / hypothesis.** A *fair* sensor-only model: all-real-data nodes, fully connected graph, isolating "sensor-only training" from "broken topology."
- **Graphs.** Fresh graphs (`steps_v2/`) on the **189-street ped∪park sensor union** (74 ped + 143 park − 28 overlap): spatial k-NN(6) Gaussian on EPSG:3111 centroids (**0 isolates**), semantic mutual-k-NN(6) cosine.
- **Loss.** Pedestrian loss masked to the 74 ped-sensor nodes; parking masked to 143.
- **Protocol.** 3 seeds; architecture imported unchanged from the frozen trainer (`model_eval_v2.json`).
- **Results (test).** Sensor ped MAE **27.16 ± 0.37**, R² **0.892 ± 0.003**; parking MAE **0.063**.

*(A pre-Category-A model, "V0", with all-street test MAE 5.535, exists but is on a different datacube generation and is therefore excluded from all comparisons below.)*

---

## 6. Ranking and evaluation

### 6.1 The honest metric

All-street pedestrian MAE (e.g. V1's 4.45) is **not** a valid cross-model comparator: it averages over 1,397 streets dominated by low-traffic, easy-to-predict side streets, and the variants do not all even predict the same set of streets. The defensible comparator is **pedestrian MAE on the 74 streets with real sensors** — the busiest, highest-variance, and only directly verifiable streets. All rankings below use it.

### 6.2 Ranking (test, 74 sensor streets, same datacube)

| Rank | Model | Graph / pedestrian loss | Sensor MAE | Sensor R² | All-street MAE |
|---|---|---|---|---|---|
| 1 | **Exp B** | full graph, masked to sensors | **24.97 ± 0.28** | 0.884 | 7.4–7.8 |
| 2 | **V2** | fresh 189-node union, masked | **27.16 ± 0.37** | 0.892 | — |
| 3 | **V1 (deployed)** | full graph, loss on all streets | **28.02** | 0.848 | **4.45** |
| 4 | **Exp A** | induced 74-node subgraph | **54.71 ± 1.32** | 0.566 | — |

### 6.3 Why the ranking comes out this way

**(a) The top three are tightly bunched (24.97 / 27.16 / 28.02); the fourth is a cliff.** The 28→55 jump from V1/V2 to Exp A is almost entirely the **isolated-node artifact**, not a property of sensor-only training. Repairing the graph (Exp A's induced subgraph → V2's fresh graph) recovers ≈ 28 MAE. The lesson is methodological: a degenerate graph, not the data restriction, produced the dramatic original gap.

**(b) Loss masking is the highest-leverage *training* choice.** Holding the graph fixed (full, real topology) and only changing where the pedestrian loss is computed moves sensor MAE from **28.02 (V1, unmasked)** to **24.97 (Exp B, masked)** — a ~3 MAE / ~11% gain. Masking works because ~1,323 of the streets V1 is graded on have *imputed* (median R² ≈ 0.18) pedestrian targets; training to match those noisy targets dilutes the real signal. This is the single most actionable result.

**(c) The marginal value of the full imputed network, fairly measured, is modest.** V2 (fair sensor-only graph) → Exp B (full graph) is **27.16 → 24.97 = ~2.2 MAE**, a ~5-σ gap (real, not noise) but small. The imputed intermediate streets and the real topology together add ~8% accuracy beyond a connected sensor-only graph.

**(d) Pedestrian flow is dominated by temporal autoregression.** Permutation importance (`feature_importance.json`, baseline MAE 5.03) is led overwhelmingly by the street's own recent flow (`ped_flow`, ΔMAE **+23.18**) and by `ped_confidence` (**+4.36**); weather and land-use features sit near or below the noise floor. The GRU's memory of recent flow does most of the work — consistent with (c), where adding network structure yields only modest marginal gains.

**(e) Spatial structure helps, but less than its branch weight first suggests.** Branch-zeroing gives spatial **ΔMAE +13.59** vs semantic **+4.74** — so within the trained model the spatial branch carries substantial structural signal, and physical adjacency clearly outweighs functional similarity. But (c) shows that *adding more connectivity* beyond a fair connected graph buys little. These are not contradictory: branch-zeroing measures how much the trained model *relies on* the spatial branch's conditioning; the V2-vs-ExpB ablation measures the *marginal* value of a larger, real-topology graph. Both point to the same synthesis — **the temporal signal dominates, spatial structure provides meaningful but bounded conditioning, and the semantic graph is a secondary refinement.**

---

## 7. Behavioural clustering and flexibility windows (RQ1)

The model of §5 supplies prediction and propagation; it does not by itself answer *which streets are reallocation candidates and when*. That question (RQ1) is addressed by an unsupervised analysis layer that runs on the temporal profiles, independent of the GNN.

### 7.1 Weekly temporal profiles (`step_06_aggregate`)

Each street's 14,400-bin series is collapsed into a compact weekly fingerprint: **42 pedestrian + 42 parking features = 7 days × 6 time-blocks**. The blocks are `night` (00–05), `morning` (06–09), `work_am` (10–11), `midday` (12–13), `work_pm` (14–17), `evening` (18–23). Pedestrian profiles are **row-normalised by the street's own mean flow**, so the downstream clustering compares the *shape* of demand (when a street is busy) rather than its *scale* (how busy) — a quiet lane and a major boulevard with the same daily silhouette cluster together. Output: `street_profiles.parquet`.

### 7.2 Clustering (`step_07_cluster`)

Pipeline: `StandardScaler` on the 84 profile features → **PCA to 20 components** (retaining **99.5%** of profile variance; `cluster_report.json`) → **Gaussian Mixture** with a BIC sweep over k = 2…10, selecting the true minimum at **k = 4** → bootstrap stability check (**ARI = 0.888**) and **silhouette = 0.526**. Each cluster is labelled by its parking-centroid peak relative to the global mean.

| Cluster | Archetype | Streets |
|---|---|---|
| 3 | `major_pedestrian_corridor` | 50 |
| 1 | `parking_reallocation_priority` | 139 |
| 2 | `latent_evening_potential_high` | 227 |
| 0 | `latent_evening_potential_medium` | 981 |

Three streets are flagged low-confidence (maximum GMM posterior < 0.70) and excluded from confident assignment.

### 7.3 Flexibility windows — the operational answer to RQ1

A **flexibility window** is a (day × time-block) cell where pedestrian demand is high *and* parking occupancy is low (below `OCCUPANCY_THRESHOLD = 0.30`). These are precisely the moments when the kerb is doing little for vehicles but much could be done for pedestrians — the candidate reallocation slots. **1,109 of 1,397 streets** exhibit at least one such window. RQ1 is therefore answered not as a binary street label but as a per-street, per-time-block shortlist of *when* reallocation is viable.

**Honest caveat.** The clustering ingests profiles for *all* streets, including the ~1,323 imputed (low-confidence, §4) pedestrian streets; for those, the archetype and windows inherit the imputation uncertainty, whereas the 74 sensor streets' windows are directly observed. Additionally, the archetype mix shifted toward evening-skewed types after the weather-timezone correction (D-011) — a reminder that these temporal profiles are sensitive to upstream data correctness, and that the windows should be read as hypotheses to verify on sensored streets, not as settled facts on imputed ones.

---

## 8. Limitations and threats to validity

- **Weather timezone (addressed, with residual caveat).** Open-Meteo data were originally fetched as UTC but treated as Melbourne local — an 11-hour shift. This was corrected (D-011, fetch in `Australia/Melbourne`) and the fix is falsifiably confirmed: `temperature_2m` permutation importance moved from −0.63 (harmful) to +0.30 (useful). The pedestrian and parking series are internally consistent in local time; the residual caveat is only that weather is the one stream that required realignment.
- **Parking-head extrapolation.** Parking loss is supervised on only 143 streets; occupancy predictions on the other ~1,254 streets are extrapolations with no ground truth and should not be treated as measurements.
- **Autoregressive drift.** Scenario rollouts are autoregressive; error compounds. Beyond ~4 hours (16 steps) the trajectories are indicative, not precise.
- **Imputation confidence is low.** Median per-street imputation R² ≈ 0.18 (§4.3). Imputed pedestrian values are low-trust; quantitative claims are restricted to sensor-backed streets (enforced operationally in the scenario tool's data-backing gate, D-017).
- **V2 edge faithfulness.** V2's k-NN spatial edges are *less* spatially faithful than the intersection-topology graph — an edge may connect two sensors with non-sensor streets between them. Consequently the V2-vs-ExpB gap (§6c) conflates imputed-node value with topology faithfulness; ~2.2 MAE is an upper bound on the imputation-specific contribution.
- **Normalisation confound in Exp A.** Exp A recomputed normalisation statistics on its 74-street subcube while Exp B used global statistics; each model is normalised to its own training distribution (fair in isolation) but the difference is a known confound, secondary to the dominant isolated-node artifact.
- **Study-period bias.** The window is 2025-11 – 2026-03 — southern-hemisphere summer/early-autumn, post-COVID. Seasonal generalisation (winter behaviour) is untested.
- **Deferred / sentinel features.** Tram-proximity features were all-sentinel and excluded; 33 of 171 parking streets fall outside the 1,397-street modelled graph.
- **Cross-generation comparability.** The pre-Category-A model (V0, 5.535 all-street) and the earlier single-seed ablation were computed on a different datacube and under a different (linear) de-normalisation; they are intentionally excluded from quantitative comparison.

---

## 9. Conclusion

**What the methodology establishes.**
1. A single dual-head spatio-temporal GNN predicts pedestrian flow and parking occupancy jointly, with the deployed model reaching test R² 0.911 (pedestrian, all-street) and 0.883 (parking) — sufficient to drive flexibility-window detection (RQ1) and network-propagation scenario analysis (RQ3).
2. On the honest metric (sensor-street MAE), the controlled comparison shows that **pedestrian flow is principally temporal**: the street's own recent flow dominates, network structure provides meaningful but bounded conditioning, and the highest-leverage *training* decision is **masking the pedestrian loss to real sensors** (~3 MAE / ~11%).
3. Imputation's primary value is **coverage**, with a **modest, real ~2.2 MAE secondary accuracy benefit** once measured against a fair (connected) sensor-only baseline — a correction to the earlier, confounded conclusion that imputation offered no accuracy benefit.

**What it does not establish.** It does not establish that the model predicts unobserved streets accurately (imputation R² ≈ 0.18), that parking predictions off-sensor are reliable, or that the patterns generalise outside the summer/autumn study window.

**Recommended default protocol.**
- **Graph:** full network with **real intersection topology** — never an induced sensor-only subgraph (the isolated-node failure mode of Exp A).
- **Pedestrian loss:** **masked to real sensors** when the objective is sensor-street accuracy; computed on all streets only when city-wide coverage is the objective (the deployed model's purpose).
- **Deployment split of duties:** ship the all-street model (V1) for coverage and scenario propagation; cite Exp B's masked-loss result as the verifiable accuracy ceiling; report **sensor-street** metrics, never all-street MAE, when comparing models.
- **Honesty:** restrict quantitative intervention claims to streets with real sensors for the perturbed signal; treat imputed streets as connectivity context, not as evidence.

---

### Cited artefacts

`cube.npy`, `cube_meta.json`, `norm_stats.json` (datacube); `graph_spatial.pt`, `graph_semantic.pt`, `node_index.parquet` (graphs); `steps/step_04_graph.py`, `steps/step_05_process.py`, `steps/step_08_cube.py`, `steps/step_09_train.py` (pipeline); `steps_v2/` (V2 variant); `model_eval.json` (deployed model), `feature_importance.json` (importance + branch attribution), `cluster_report.json` (archetypes), `ablation_results04.06.26.json` (Exp A / Exp B, 3-seed), `model_eval_v2.json` (V2, 3-seed); decisions D-011…D-019 (`docs/notes/decisions.md`).
