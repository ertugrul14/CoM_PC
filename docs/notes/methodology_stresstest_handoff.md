# Methodology Stress-Test — Session Handoff Report

**Date:** 2026-06-03
**Purpose:** Full context dump so a new chat can resume without re-deriving anything.
**Role taken in session:** Senior ML engineer stress-testing the thesis methodology, application, and scenario tool for defensibility.
**Companion files produced this session:**
- `docs/notes/thesis_defensibility_checklist.md` — prioritized fix list (categories A–E).
- `docs/_thesis_extract.txt` — extracted text of the thesis deck (32 pages).
- This file — narrative handoff + the final recommendation in detail.

---

## 0. Project in one paragraph
"Curbside Intensification" thesis. An AI framework that fuses pedestrian + parking + land-use + weather into a joint spatio-temporal graph over Melbourne CBD streets (N=1,397 modelled, T=14,400 × 15-min bins, Nov 2025–Mar 2026), to (a) find curbside reallocation *opportunities* and (b) simulate their network-wide impact. Dual-head MultiGCN (ped + parking heads), spatial + semantic graphs, GMM clustering into archetypes, XGBoost pedestrian imputation, Flask scenario API + Mapbox frontend. Main RQ: how to reallocate curbside functions based on streets' temporal behaviour using parking + pedestrian data. Sub-RQs: (1) which streets have flexibility windows; (2) what factors drive patterns; (3) how an intervention propagates across the network.

## 1. What was audited this session
Read in full or part: Steps 01 (fetch, grep), 03 (temporal), 04 (graph), 05 (process), 07 (cluster), 08 (cube), 11 (scenario core); the ablation report + results JSON + script. Ran read-only data/graph checks. **No pipeline code was changed.** Only the three docs above were created.

## 2. The three street tiers (backbone of every reliability claim)
- **Tier 1 — 28 streets**: real ped sensor AND real parking sensor (from ablation `experiment_a … "parking_streets": 28` within the 74-node sensor subgraph). Everything defensible here.
- **Tier 2 — ~189 streets**: real on one signal only (74 ped-sensor ∪ 143 parking-sensor minus 28 overlap). Half-reliable.
- **Tier 3 — ~1,180 streets**: both signals imputed/absent. Coverage only.

## 3. Key findings (evidence-cited)

### Confirmed bugs / issues
1. 🔴 **Weather 11h misaligned.** Open-Meteo fetched `timezone="UTC"` (`step_01_fetch.py:200`); ped/parking `local_datetime` is Melbourne local relabelled UTC (`step_05_process.py:280`). Weather merged against the wall-clock axis → 5pm peds paired with ~4am weather. Likely why weather feature importance is low. Fix: shift weather +11h (constant, no DST in window) or refetch Melbourne tz.
2. 🔴 **Global normalization.** Single mean/std across all 1,397 streets (`step_08_cube.py:209`). `norm_stats.json`: ped_flow mean 28.2 / std 61.2 → high-traffic sensor streets sit at z≈4–10 in the tail. Fix: per-node (per-street) z-scoring for ped_flow/occupancy_rate.
3. 🟠 **Missing sensor bins = 0.** `step_05_process.py:300` fills NaN ped with 0; 10.4% of sensor bins are exactly 0. Outages look like quiet periods → fake flexibility windows.
4. 🟠 **Confidence 0.8 tier never created.** `step_05_process.py:445` uses one global R² cutoff → tiers are only 0.5 (94.7%) / 1.0 (5.3%). Per-street R² is computed (`:399`) but unused. Fix: per-street confidence → materializes 0.8 tier → enables confidence-weighted message passing.
5. 🟠 **Non-sensor streets get occupancy=0** (`step_08_cube.py:163`) — conflates "no sensor" with "empty." Affects flexibility windows + scenarios.
6. 🟡 **`valid_parking` is a no-op** (always True, `step_05_process.py:160`).

### Things I suspected but TESTED and REFUTED (do not re-raise as bugs)
- **`touches` vs `intersects` predicate** in spatial graph: identical (2097 pairs each). No polygon-overlap bug.
- **"Fragmented graph":** false. Giant component = **1298/1397 = 92.9%**; 73/74 ped-sensor streets are in it; 0 sensor streets isolated. 25 fully isolated streets + tiny islands (27,26,7,6…) are the only gaps. The weight-0 fallback edges are cosmetic (give "1 component" appearance but carry no signal).
- This **resolves the ablation puzzle**: the ablation's "35/74 sensor nodes isolated" was an artifact of subsetting the graph to sensor-only nodes (their real neighbours are non-sensor streets that got deleted), NOT a graph flaw. → The ablation under-rates the full graph.

### Ablation reliability (from earlier in session)
- The ablation report claims "imputation does not justify itself" but rests on **n=1 seed** and a **normalization confound** (Exp A uses sensor-subset norm stats `ablation_sensor_only.py:462`; Exp B uses full-graph stats `:531` — not "identical conditions"). Exp B peaked at epoch 114 vs A's 195 / baseline 190 (likely undertrained); Exp B parking is worse than the baseline under the *same* parking protocol → optimization artifact, not "imputation poison." Sturdy findings that survive: disaggregated eval (5.54 all-street vs 30.7 sensor-only) and loss masking (~27% MAE gain).

### Structural limits (NOT fixable by the planned work)
- Imputation per-street R²=0.158 (near noise) → Tier 3 is coverage-only.
- Parking head unsupervised on 1,254 streets.
- Scenarios are correlational (model learned association, not causal effect of removing parking); `pedestrianise` sets occupancy=0 (`step_11_scenario.py:361`) = out-of-distribution extrapolation for busy streets.
- Autoregressive rollout error compounds beyond ~16 steps (4h).

## 4. Methods to improve the imputed model (asked early in session)
Ranked: (Tier 0, do first) per-node/sensor normalization for Exp B; multi-seed ≥5; epoch/optimization parity. (Tier 1) confidence-weighted edges / GATConv; confidence-weighted loss vs binary mask; pretrain-then-finetune on sensor streets. (Tier 2) stop feeding imputed ped_flow as raw input (use embedding + missing flag); graph-based imputation to attack R²=0.158 root cause.

## 5. What becomes answerable AFTER the fixes (the user's last question)
**Defensible:** flexibility windows for Tier 1 (~28) robustly; time-of-day/weather(after fix)/land-use as drivers for sensored streets; interpretable archetypes; sensor-street R²≈0.92; the methodological contributions (disaggregated eval, loss masking, coverage-vs-accuracy framing).
**Meaningful but bounded (scenario tool):** `boost_ped` most defensible; `restrict_park` ok on the 143 parking streets; `pedestrianise` weakest (extrapolation). Outputs: baseline forecast & target delta (≤4h) trustworthy on sensor streets; ped spillover exploratory in giant component; parking spillover only on 143 streets; diffusion/rebound descriptive.
**Off-limits even after fixes:** exact causal impact; precise per-street predictions on ~1,180 imputed streets; parking on non-sensor streets; >4h horizons; pedestrianise magnitude on busy streets; propagation for the 25 isolates.

## 6. LAST SUGGESTION (in detail) — what I proposed at end of session
**Recommendation:** Build a **confidence-badge scheme** into the scenario tool (Step 11 API + `melbourne_pipeline/frontend/sensor_map_viz.html`) so every scenario result visibly shows its reliability, derived from (street tier × intervention type × horizon):
- **Green** = Tier 1 street + `boost_ped` + ≤4h → trust shape and rough magnitude.
- **Amber** = real on one signal + ≤4h → trust direction/shape, not magnitude.
- **Grey** = imputed (Tier 3) street, OR >4h horizon, OR `pedestrianise` extrapolation → illustrative only.
This converts every hidden limitation into a visible label — exactly what an examiner wants — and is a **post-training change (no OVERRIDE needed)**.

I offered the user two next-step paths and asked them to choose:
- **(Option 1, recommended) OVERRIDE** → I implement Category-A foundation fixes (weather +11h, per-node norm, missing-vs-zero, per-street confidence), re-run Steps 1–8 locally, hand back a corrected cube for the user to retrain on Lightning AI. Then re-run the hardened ablation.
- **(Option 2) Stay post-training** → I draft the hardened ablation script (multi-seed + fair normalization + epoch parity) for the user to run on Lightning, and handle the rest as framing + the badge scheme.
- I also asked, if Option 1: start with the **weather fix** (highest validity impact, simplest) or the **normalization fix** (most likely to change the ablation conclusion).

**PENDING USER DECISION at end of session:** which option, and the build-the-badge-scheme offer. No code changes were authorized yet.

## 7. Hard constraints to respect next session
- Steps 01–09 are FROZEN. Any edit there requires the user to type an OVERRIDE (scope rule). The Category-A fixes all touch frozen steps → must get OVERRIDE first.
- User can run any pipeline step EXCEPT training (Step 09 / ablation training runs on Lightning AI; the `ablation_bundle/` is their Lightning bundle).
- Token discipline: don't read full PDFs/parquet/model files without asking; prefer targeted reads.
- User preferences: minimize cost (no polling, batch tool calls, suppress verbose output); explain in simpler language; keep the project purpose unchanged.
