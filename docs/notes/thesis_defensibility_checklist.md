# Thesis Defensibility Checklist — Curbside Intensification

Status legend: 🔴 must-fix (affects validity) · 🟠 important (affects claims/quality) · 🟡 cleanup (honesty/clarity)
Retrain = changes the cube, needs one Lightning retrain. OVERRIDE = touches frozen Steps 01–09.

Audit date: 2026-06-03. Steps audited: 01, 03, 04, 05, 07, 08, 11 + ablation.

---

## A. Data foundation correctness (do before the final retrain)

- [ ] 🔴 **Weather 11h misalignment.** Open-Meteo fetched as UTC (`step_01:200`) but merged against Melbourne-wall-clock activity axis. Fix: shift weather +11h (constant; no DST change in Nov–Mar) or refetch with `timezone=Australia/Melbourne`. *Retrain, OVERRIDE (Steps 01/03/08).*
- [ ] 🔴 **Global normalization.** Single mean/std across all 1,397 streets (`step_08:209`); ped_flow mean 28.2 / std 61.2 puts high-traffic sensor streets in the z≈4–10 tail. Fix: per-node (per-street) z-scoring for `ped_flow`/`occupancy_rate`. *Retrain, OVERRIDE (Step 08).*
- [ ] 🟠 **Missing sensor bins filled with 0.** 10.4% of sensor bins are exactly 0 (`step_05:300`); outages look like real quiet periods → fake flexibility windows. Fix: distinguish missing from zero (loss mask or short-gap fill). *Retrain, OVERRIDE (Step 05).*
- [ ] 🟠 **Confidence 0.8 tier never created.** Tiers are only 0.5 (94.7%) / 1.0 (5.3%); one global R² cutoff (`step_05:445`). Per-street R² already computed (`step_05:399`) but unused. Fix: assign per-street confidence → materializes 0.8 tier → enables confidence-weighted message passing. *Retrain, OVERRIDE (Step 05).*
- [ ] 🟠 **Non-sensor streets get occupancy = 0.** `step_08:163` fills 1,254 streets with 0, conflating "no sensor" with "empty." Fix: treat as unknown; keep parking outputs masked to the 143 real streets. *Retrain, OVERRIDE (Step 08).*
- [ ] 🟡 **`valid_parking` is a no-op** (always True, `step_05:160`). Remove the column + dependent filters (misleading in writeup). *No retrain.*
- [ ] 🟡 **First 9h of day 1 dropped** (grid starts 09:00 not 00:00). Document or start at 00:00. *Trivial retrain.*

## B. Evaluation rigor (the ablation is the methodological core — harden it)

- [ ] 🔴 **Multi-seed everything.** Current ablation is n=1 (`ablation_sensor_only.py` single SEED). Run ≥5 seeds, report mean ± std. The "imputation hurts" gap (0.5 MAE) is likely inside the noise band.
- [ ] 🔴 **Re-run Exp A vs B with controlled normalization.** A uses sensor-subset stats, B uses full-graph stats — not "identical conditions." Re-normalize B with sensor/per-node stats so the comparison is fair.
- [ ] 🟠 **Epoch/optimization parity for Exp B** (peaked at epoch 114 vs A's 195, baseline's 190 → likely undertrained). Parking head is even worse than baseline under the same protocol = optimization artifact, not imputation poison.
- [ ] 🟠 **Always report disaggregated metrics** (sensor vs imputed) — the 5.54 vs 30.7 gap is a genuine, sturdy contribution; lead with it.
- [ ] 🟡 **Drop or caveat the "all-streets 8.13 vs 5.54" row** — not like-for-like (Exp B never optimized imputed streets).

## C. Methodological framing / honesty (writeup, not code)

- [ ] 🔴 **Causal caveat on scenarios.** `pedestrianise` sets occupancy=0 and reads the ped head (`step_11:361`) — observational correlation extrapolated outside training support, not a causal effect. Frame as "model-implied associative exploration under stated assumptions."
- [ ] 🟠 **Coverage-vs-accuracy framing for imputation.** Imputation expands 74→1,397 streets for city-wide coverage; with confounds controlled it is accuracy-neutral on sensor streets, not accuracy-improving. (Per-street R²=0.158 → city-wide claims inherit this.)
- [ ] 🟠 **Flexibility windows / archetypes reliable only for sensored streets.** Non-sensor results rest on imputed ped + absent parking. State scope explicitly.
- [ ] 🟠 **Parking spillover valid only on the 143 sensor streets** (`step_11:485`); parking head unsupervised elsewhere.
- [ ] 🟡 **Correct the timezone note in CLAUDE.md/thesis.** Ped/parking/temporal ARE internally aligned to Melbourne local time (good news); only weather was shifted. The "cluster profiles shifted" worry was over-pessimistic.
- [ ] 🟡 **Branch-importance vs ablation tension.** "Spatial branch dominant (+14.76 MAE)" vs "flow is primarily temporal" — reconcile in writeup.

## D. Reproducibility / cleanup

- [ ] 🟡 **Spatial graph: 25 isolated streets + tiny islands** get only self-loops; weight-0 fallback is cosmetic. Optionally add short-distance edges (e.g., centroids < ~50m, or reconnect via removed intersection segments). State the giant component = 92.9%.
- [ ] 🟡 **Semantic feature name mismatch**: `tram_stops_300m` (list) vs `tram_stops_200m` (written); tram features all-sentinel → contribute nothing. Remove or fix.
- [ ] 🟡 **Log all of the above in `docs/notes/decisions.md`** as deliberate decisions, with rationale.

## E. Verification checks (confirm, then claim)

- [ ] Confirm holiday/school-holiday flags use Melbourne local dates (they appear correct since wall-clock is preserved).
- [ ] Confirm parking event-cap restriction window (07:30–19:30) is in Melbourne local time, consistent with bay-restriction data.
- [ ] After fixes: re-check weather feature importance (should rise if misalignment was the cause).
- [ ] Quantify how many "flexibility window" streets depend on absent parking data vs real low occupancy.

---

## Suggested execution order
1. Apply A-fixes in Steps 01/03/05/08 (run locally; needs OVERRIDE).
2. One Lightning retrain on the corrected cube.
3. Re-run the hardened ablation (B: multi-seed + controlled norm + epoch parity).
4. Update framing (C) and decisions log (D) from the new numbers.
