# Pedestrian Imputation — Stress Test & Improvement Plan

**Date:** 2026-06-06 · **Owner:** ertugrul · **Status:** diagnostics done; fixes pending
**Code under test:** `melbourne_pipeline/steps/step_05_process.py` → `_build_ped_complete()`
(XGBoost v3, "graph-informed"). FROZEN step — fixes need OVERRIDE.

**Diagnostic scripts (read-only, re-runnable locally):**
- `scripts/stresstest_ped_imputation.py` — covariate shift, spatial-lag coverage, output sanity
- `scripts/fix5_neighbour_predictability.py` — does the spatial signal carry real info?
  (writes `scripts/fix5_neighbour_predictability_result.csv`)

---

## 1. What the imputation does (plain words)

Goal: guess 15-min pedestrian flow for **1,323 streets with no sensor**, by
training XGBoost on the **74 streets that have sensors**, then predicting the rest.
Features: static (jobs, cafés, dining capacity, area…), temporal (hour/day sin/cos,
weekend), weather, parking stats, and **`spatial_lag_ped`**.

**`spatial_lag_ped` = the mean pedestrian flow of a street's SENSORED spatial-graph
neighbours at each time bin.** It is the "peek at what nearby measured streets are
doing right now" feature — the only feature that is both location-specific AND
time-varying. It's what makes v3 "graph-informed".

CV protocol: `GroupKFold(5)` over streets (holds out whole streets — correct, tests
generalisation to UNSEEN streets, no temporal leak). `log1p` target. Confidence tier
via kNN similarity transfer (0.8 if 5 nearest sensored streets have mean R²≥0.6 else 0.5).

---

## 2. Stress-test findings (all measured, 2026-06-06)

### F1 — Covariate shift: train on busy, deploy on quiet (moderate, directional)
Sensored vs unsensored street means: total_jobs 269 vs 75 (3.6×), cafe_count 6.9 vs 1.3
(5.3×), dining_capacity 857 vs 151 (5.7×). Cohen's d 0.55–0.93. BUT 99% of unsensored
streets fall inside the sensored 5–95 percentile band for most POI features → it's
extrapolation into a **sparse low-activity tail**, not out-of-range invention.
total_jobs is worst (47% overlap).
→ GroupKFold R² (over sensored streets) is an **optimistic upper bound** on true
unsensored performance.

### F2 — The graph-informed feature is DEAD for 88% of imputed streets (the big one)
`spatial_lag_ped` = 0 for **1,170 of 1,323** unsensored streets (no sensored neighbour
to peek at). Only **153 (12%)** get an informative lag. And lag=0 is a value the
sensored TRAINING streets almost never take (they're densely interconnected). So for
88% of streets, the "graph-informed" upgrade doesn't reach them AND it feeds an
out-of-distribution feature value.

### F3 — Outputs are plausible, not collapsed
Imputed median mean-flow 14.8 vs sensored 77.5 (correctly lower for quiet streets),
peaks scale, 0% flat-lined. XGBoost produces reasonable low-traffic profiles. The
problem is unverifiability + the dead feature, not garbage output.

### F4 — Fix-5 cheap proxy: the spatial signal is weak; own-history dominates
Predicting a SENSORED street's flow (median R²):
| predictor | R² |
|---|---|
| own time-of-day climatology | **0.703** |
| sensored-neighbours' mean | 0.235 |
| city-wide average rhythm | 0.239 |
| (correlation w/ neighbours) | 0.82 |

**Neighbours ≈ global mean (0.235 ≈ 0.239)** → the graph adds almost no unique signal
for the LEVEL of flow. High corr (0.82) + low R² → neighbours capture **shape** (daily
rhythm) but not **scale** (a laneway and a main road both peak at 5pm, at very different
magnitudes). Neighbour beats own-climatology on only 13% of streets (median uplift −0.48).

### F5 — Root cause / reframe (most important takeaway)
The strongest predictor is a street's OWN history (R² 0.70) — which **unsensored streets
do not have** (that's why they're unsensored). So imputation is capped by available
information, not by model tuning. Measured per-street imputation R²≈0.18 (D-014) sits
near the neighbour/global ceiling (~0.24). The problem decomposes as **shape × scale**:
- shape: transferable, cheap (neighbours / time-of-day)
- scale: per-street multiplier — the missing piece, can ONLY come from static features.
→ The real lever is **predicting per-street SCALE from static features**, not chasing a
better temporal/neighbour signal.

---

## 3. The two roles of imputation (don't conflate — this fixes the metric confusion)

- **Role A — imputed values as GNN INPUT (message-passing context).** The only place
  imputation can legitimately help. Already measured by ablation: ExpB (full imputed
  graph, masked loss) 24.97 vs V2 (fair sensor-only graph) 27.16 = **+2.2 MAE / ~8%**
  on sensor streets (~5σ). Modest, real, NOT 2×.
- **Role B — imputed values as GNN TARGET (supervision on 1,323 streets).** The deployed
  all-street MAE 4.45 is largely the GNN **learning to copy XGBoost** — circular, not
  evidence of accuracy. Fix: **mask the ped loss to the 74 sensored streets** (ablation
  already confirms masked is best).

---

## 3b. PROTOTYPE RESULTS (2026-06-06, off-to-the-side, no frozen edits)

Scripts: `scripts/exp_spatial_lag_v4.py` (Fix 1), `scripts/exp_scale_from_static.py` (Fix 2).
Natural experiment: 35 of 74 sensored streets ALSO have lag=0 today (only 39 have a
sensored immediate neighbour) → they're the ground-truthed twins of the 88% unsensored
dead-lag streets.

**Fix 1 — spatial-lag fallback, median R² on the 35 dead-lag sensored streets:**
| fallback | median R² | note |
|---|---|---|
| current (lag=0 → city-mean) | −2.53 | actively HARMFUL (these streets ≠ city avg) |
| **k2 (≤2 hops)** | **+0.02** | best; reaches a sensored street for 63% |
| k3 (≤3 hops) | −0.15 | over-reaches → mis-scales |
| featknn (static-similar) | −0.80 | worst; nearest sensored are still busy |
| own-clim (ceiling, unavail) | 0.52 | |
→ Build ONLY the 2-hop version. k3/featknn make it worse. Caveat: this scores neighbour-mean
as a STANDALONE predictor of LEVEL; inside XGBoost the lag is a SHAPE feature it can rescale,
so true marginal value still needs the Tier-2 retrain. Clean signal = the ranking k2>k3>featknn
and the ~0 ceiling.

**Fix 2 — can static features predict a street's LEVEL? (leave-one-street-out R²):**
| target | linear | xgboost |
|---|---|---|
| log mean flow (scale) | −0.10 | **0.06** |
| log peak flow | 0.14 | 0.24 |
→ Static features barely predict the scale. Fix 2's "predict scale from statics" is NOT a
strong lever (information ceiling). Caveat: measured within the 74 busy sensored streets
(range-restricted) so 0.06 is a lower bound, but direction is clear. KEEP only the cheap
sub-part: continuous ped_confidence from prediction uncertainty.

**SYNTHESIS — imputation is INFORMATION-limited, not MODEL-limited.** All levers cap low:
neighbour shape ~0.24, static scale ~0.06; the only strong signal (own history) is
structurally unavailable for unsensored streets. Explains per-street R²≈0.18 and the
collapsed 0.8 tier. Thesis framing: imputation = COVERAGE not ACCURACY (mechanistic reason now known).

## 4. Fix plan (ranked, REVISED after prototypes)

PRIORITY: Fix 1 (2-hop only, cheap) + Fix 3 (masking) + Fix 4 (honest reporting).
DE-PRIORITISED: Fix 2 heavy rework (information ceiling) — keep only continuous-confidence sub-part.


### Fix 5 — Is the XGBoost pre-imputation even pulling its weight? [BUNDLE WRITTEN + SMOKE-VALIDATED]
- Cheap proxy: DONE (F4) → spatial signal weak, own-history dominant.
- Decisive bundle: **`scripts/fix5_leave_streets_out_gnn.py`** — written 2026-06-06, smoke
  test passes (exit 0, CPU). Leave-M-sensored-streets-out, K folds. Per fold, hold out M of
  74 sensors (removed from ped LOSS), keep their REAL flow as answer key, train GNN twice:
    - Arm A (GNN+XGBoost): held-out input nodes filled with leakage-free XGBoost imputation
      (refit on retained sensors, lag recomputed treating held-out as unsensored).
    - Arm B (GNN alone): held-out input nodes filled with dumb city-climatology (NO XGBoost).
  Both graded on held-out REAL flow. Two-cube trick (cube_input filled / cube_target real)
  avoids grading the GNN on copying XGBoost. Also reports standalone XGBoost-imputer MAE.
  VERDICT: A≈B → GNN reconstructs without XGBoost → drop the stage; A≪B → keep it.
- RUN: `python scripts/fix5_leave_streets_out_gnn.py` from project root (GPU, ~6 h at K=3×2 arms).
  Smoke first: `SMOKE_TEST=1 python scripts/fix5_leave_streets_out_gnn.py`. Env knobs:
  K_FOLDS (3), N_SEEDS (1), FOLD_EPOCHS (200), ARMS. Output:
  `data/experiments/fix5_leave_streets_out_results.json`.
- NOTE (do NOT over-read): the 2-epoch smoke run already showed Arm A ≈ Arm B (gap ~0) and
  both ≪ XGB-only — directionally consistent with "GNN doesn't need XGBoost", but the real
  multi-epoch GPU run is required before drawing any conclusion.

### Fix 1 — Repair the dead spatial-lag (touches Step 05 → needs OVERRIDE)
88% of streets get lag=0. Options (simplest→best):
(a) k-hop expansion (reach sensored streets 2–3 hops out);
(b) distance-weighted lag over all sensored streets within radius R;
(c) feature-space fallback: when no sensored neighbour, borrow from functionally similar
    (kNN-in-static-features) sensored streets.
Caveat (per F4): even a perfect neighbour signal caps at R²≈0.24 for LEVEL — Fix 1 mainly
recovers SHAPE. Still worth it (currently dead for 88%), but not the headline lever.

### Fix 2 — Per-street SCALE from static features + uncertainty (touches Step 05 → OVERRIDE)
The real lever (F5). Make XGBoost output a SHAPE×SCALE decomposition, or add a
quantile/NGBoost head so each street gets a prediction INTERVAL. Feed interval width into
`ped_confidence` as a CONTINUOUS trust weight (current confidence is near-binary: only 2
streets at 0.8 — carries ~no info). Lets the GNN downweight shaky imputed nodes.

### Fix 3 — Mask ped loss to sensored streets (Role B fix; protocol, not frozen-step)
Already validated by ablation. Make masked-loss the deployed default so the headline
metric stops being circular.

### Fix 4 — Honest reporting (thesis, no code)
Report imputation R² STRATIFIED by feature-distance to the sensored centroid — show it
decays for unusual streets. Replaces a single rosy number with the truthful picture.

---

## 5. Open decisions for next session
- [ ] User OVERRIDE for Fix 1 / Fix 2 (both edit frozen step_05).
- [x] Fix 5 bundle written + smoke-validated: `scripts/fix5_leave_streets_out_gnn.py`.
- [ ] Run decisive Fix 5 on Lightning (GPU, ~6 h) → read `gap_B_minus_A` in the results JSON.
- [ ] Decide from Fix 5 result: keep XGBoost stage, or replace with cluster-mean init if A≈B.
