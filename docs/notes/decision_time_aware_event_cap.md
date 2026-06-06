# Decision: Time-Aware Parking Event Cap

**Date:** 2026-04-30
**Decision ID:** D-009
**Status:** Approved — pending implementation (OVERRIDE required on Step 05)

---

## Background

The pipeline processes raw parking sensor events from Melbourne's on-street bay sensors. Each event records a `arrival_time` and `departure_time` for a vehicle. Due to sensor failures (missed exit-event triggers), some events have implausibly long durations — a vehicle appears "parked" for 12–24+ hours on a bay with a 2-hour restriction. These artifacts inflate occupancy rates and destabilise clustering.

A global duration cap was introduced in `step_05_process.py` (`EVENT_CAP_SECONDS`) to filter these artifacts. The cap was originally set to 7,200 seconds (2 hours), then raised to 14,400 seconds (4 hours) after analysis of the `on-street-car-park-bay-restrictions.csv` dataset showed that 13.5% of CBD bays allow 4-hour parking (4P bays), making 2 hours too aggressive.

---

## The Problem with a Global Cap

Analysis of `on-street-car-park-bay-restrictions.csv` (4,263 bays, 62 columns) reveals that Melbourne CBD parking restrictions are **time-windowed**, not 24-hour rules:

| Restriction end time | Bays | % of total |
|---|---|---|
| Before 7:30pm | ~2,877 | 67% |
| Before 8:30pm | ~3,495 | 82% |
| Up to 11pm or 24h | ~768 | 18% |

**82% of CBD bays become unrestricted after approximately 8:30pm.** Once the restriction window closes, a vehicle can legally park overnight until restrictions resume at ~7:30am the next morning — a window of approximately 10.5 hours.

A global 4-hour cap cannot distinguish between:
1. A **daytime sensor artifact** — a car "parked" for 8 hours at 10am on a 2P bay (impossible, should be filtered)
2. A **legitimate overnight stay** — a car arriving at 8pm parking until 7am on an unrestricted bay (real occupancy, should be preserved)

Under the global cap, scenario (2) is truncated at midnight (arrival + 4h), making the bay appear empty from midnight to 7am. For the study period (Nov 2025–Mar 2026), this affects all overnight stays across the 82% of bays with evening restriction endings.

### Impact on Scenario Simulations

This truncation is acceptable for daytime analysis — the policy-relevant window for most interventions. However, the thesis includes scenario simulations for **evening pedestrianisation** (e.g., cluster 3 `evening_outdoor_dining` streets, scenarios run at 8pm–midnight). For these scenarios:

- The model's baseline parking occupancy during evening hours is artificially deflated
- A `pedestrianise` intervention at 9pm compares against a baseline that may show 2–5% occupancy when true occupancy (overnight parkers) could be 20–30%
- The **cost of removing parking is systematically underestimated** for evening hours, making pedestrianisation appear cheaper than it is

This is a thesis-level methodological problem: a key cluster (`evening_outdoor_dining`, 244 streets) is assigned precisely because of its evening temporal signal, yet the parking baseline for evening scenarios is unreliable.

---

## Solution: Time-Aware Event Cap

Replace the global `EVENT_CAP_SECONDS` constant with a per-event cap that depends on the event's **start time**:

**Rule:**
- Events starting during restriction hours (approximately 7:30am–7:30pm): apply the 4-hour cap (preserves daytime artifact filtering)
- Events starting during unrestricted hours (approximately 7:30pm–7:30am): cap duration at `next_restriction_start - arrival_time`, i.e., allow the event to run until the next morning's restriction window opens

**Concrete example:**
- Car arrives 8pm → allowed until 7:30am = 11.5h cap (not 4h)
- Car arrives 11pm → allowed until 7:30am = 8.5h cap
- Car arrives 9am → 4-hour cap applies as before

The daytime sensor-artifact problem is solved by the 4-hour cap within restriction hours. The overnight legitimate-stay problem is solved by a longer dynamic cap outside restriction hours.

**Restriction window parameters (derived from bay restrictions dataset):**
- Restriction start: 07:30 (mode of StartTime distribution after correcting +53min UTC offset)
- Restriction end: 18:30–20:30 range; use 19:30 (7:30pm) as conservative cutoff

---

## Options Considered

| Option | Description | Verdict |
|---|---|---|
| A | Keep global 4h cap; restrict scenarios to before 8pm | Safe but excludes evening scenarios entirely — limits thesis scope |
| **B** | **Time-aware cap: 4h during restriction hours, dynamic overnight** | **Chosen — methodologically correct, preserves all scenario types** |
| C | Keep global 4h cap; flag evening scenarios with uncertainty warning | Honest but leaves a known bias in results |
| D | Remove cap entirely for overnight | Risky — no defense against sensor artifacts in evening hours |
| E | Raise global cap to 10h | Unjustifiable — no Melbourne CBD bay allows 10h daytime parking; reintroduces daytime artifacts |

---

## Implementation Scope

- **Step 05** (`step_05_process.py`): modify event duration clipping logic
- **Steps 06–12**: full cascade re-run required (profiles, clustering, cube, graphs, model training, interpretation, scenarios, frontend export)
- **Model retraining**: required — training targets (occupancy_rate) change for overnight bins

This requires `OVERRIDE: allow pre-training changes for this task`.

---

## Known Weaknesses (thesis acknowledgement)

1. **Restriction start time approximated globally.** The 7:30am restriction start is the mode across bays; individual bays vary (some start at 10am, some at 5pm). A per-bay cap would require joining the restrictions dataset to sensor bay IDs — not possible with current ID schemes.
2. **The +53-minute timestamp offset** in the restrictions CSV (UTC encoding artifact) was corrected manually when deriving the 7:30am / 7:30pm cutoffs. If this offset varies across rows, the cutoffs may be slightly imprecise.
3. **Overnight sensor artifacts still possible.** A sensor that fails to register an exit at 9pm will now produce a long event that runs to 7:30am. These cannot be distinguished from legitimate overnight stays without ground-truth validation data.

---

## Expected Impact

- Overnight bins (8pm–7:30am) will show higher occupancy for ~82% of bays
- Evening scenario baseline parking will be more realistic
- Clustering may shift slightly if overnight occupancy changes street profiles materially
- Model retraining is required; parking head R² expected to improve for evening/night bins
- Daytime results (7:30am–7:30pm) are unaffected


  ---                                                                                                                                                                                                                    
  Implementation To-Do List — Time-Aware Event Cap
                                                                                                                                                                                                                         
  Prerequisite: State OVERRIDE: allow pre-training changes for this task before starting Step 05 work.

  ---
  Step 05 — step_05_process.py

  - Define RESTRICTION_START = "07:30" and RESTRICTION_END = "19:30" as module-level constants (derived from restrictions dataset mode)
  - Replace the single EVENT_CAP_SECONDS = 14400 constant with a function get_event_cap(arrival_time) that returns 4h cap if arrival is within restriction hours, or seconds_until(next 07:30) if arrival is in the
  overnight window
  - Apply the new per-event cap in the event duration clipping logic
  - Verify: re-run Step 05, check that max occupancy stays below ~1.0 (no sensor artifacts) and overnight bins show higher occupancy than before

  Step 06 — step_06_aggregate.py

  - Re-run to regenerate street_profiles.parquet (42 parking temporal features will change for evening/night blocks)
  - Verify: check park_occupancy_rate_*_evening columns — values should be higher than before for sensor streets

  Step 07 — step_07_cluster.py

  - Re-run GMM clustering on updated profiles
  - Check: does k=4 still hold (BIC minimum)? Check cluster sizes and archetype assignments — evening_outdoor_dining cluster may gain streets or shift membership
  - Log any cluster composition changes in decisions.md

  Step 08 — step_08_cube.py

  - Re-run to regenerate cube.npy and norm_stats.json with updated occupancy values
  - Verify cube shape is still (1397, 14400, 23)

  - Re-run to regenerate cube.npy and norm_stats.json with updated occupancy values
  - Verify cube shape is still (1397, 14400, 23)

  Step 09 — Model training (Lightning AI / GPU)

  - Upload updated cube to Lightning AI
  - Retrain MultiGCN — run full 200+ epochs (previous run didn't early-stop at 200, consider MAX_EPOCHS=250)
  - Check: parking head R² for evening bins should improve; ped R² should be stable
  - Download best_model.pt, parking_mask.pt, run_config.json, model_eval.json

  Step 09 — Model training (Lightning AI / GPU)

  - Upload updated cube to Lightning AI
  - Retrain MultiGCN — run full 200+ epochs (previous run didn't early-stop at 200, consider MAX_EPOCHS=250)
  - Check: parking head R² for evening bins should improve; ped R² should be stable
  - Download best_model.pt, parking_mask.pt, run_config.json, model_eval.json

  Step 10 — step_10_interpret.py

  - Re-run permutation importance and branch contribution with new model
  - Update feature_importance.json
  - Check: occupancy_rate delta MAE — expect slight increase (feature is now more informative)
  - Re-run permutation importance and branch contribution with new model
  - Update feature_importance.json
  - Check: occupancy_rate delta MAE — expect slight increase (feature is now more informative)

  Step 11 — step_11_scenario.py

  - Re-run baseline scenario outputs for any pre-generated scenario JSONs in data/processed/scenario_results/
  - Verify: a pedestrianise scenario at 9pm now shows a higher baseline parking occupancy than before — this is the expected fix

  - Re-run baseline scenario outputs for any pre-generated scenario JSONs in data/processed/scenario_results/
  - Verify: a pedestrianise scenario at 9pm now shows a higher baseline parking occupancy than before — this is the expected fix

  - Update feature_importance.json
  - Check: occupancy_rate delta MAE — expect slight increase (feature is now more informative)

  Step 11 — step_11_scenario.py

  - Re-run baseline scenario outputs for any pre-generated scenario JSONs in data/processed/scenario_results/
  - Verify: a pedestrianise scenario at 9pm now shows a higher baseline parking occupancy than before — this is the expected fix

  Step 12 — step_12_export.py

  - Re-run frontend export to regenerate streets_viz.geojson with updated pred_ped_mean, uplift_fraction, cf_ped_mean
  - Verify map loads correctly in sensor_map_viz.html

  Thesis notes to add after completion

  - Update current_state.md with new training results
  - Update CLAUDE.md training results table
  - Add a note to Known Limitations: "Overnight restriction cutoff is a global approximation — per-bay precision not achievable with current sensor-to-bay ID mapping"
