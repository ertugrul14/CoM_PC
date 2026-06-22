# Kerb-Reallocation Scenario — Findings & Implementation (D-026)

**Date:** 2026-06-21
**Scope:** `step_11_scenario.py` (post-training, not frozen). No retraining, no Steps 01–09 edits.
**Status:** Implemented + verified by sweep. Frontend NOT yet wired (see §7).
**Cross-ref:** `docs/notes/decisions.md` → D-026. Demo scripts: `melbourne_pipeline/scratch/`.

---

## 1. What was asked

Starting point: the saved scenario
`frontend/lonsdale_street_between_elizabeth_street_and_queen_street___restrict_park_240min.json`
showed that **restricting parking lowered both occupancy AND pedestrian flow across the
network** — the opposite of the "Curbside Intensification" thesis hypothesis (reallocating
kerb space to people should *raise* footfall).

The user asked, in sequence:
1. *Why* did restricting parking lower occupancy and ped flow network-wide?
2. *What has to be done* to get the desired output (kerb reallocation → footfall up)?
3. Pursue it: **(a)** run the chosen approach, **(b)** adjust the bounds.
4. Document everything in this file, and confirm whether the frontend scenario panel was updated.

---

## 2. Why restrict_park lowered footfall (root cause)

The MultiGCN is an **associational** model, not a causal one. Two mechanisms:

1. **`occupancy_rate` is a confounder, not a lever.** In training data, parking occupancy and
   pedestrian flow are *positively correlated* — both are proxies for "how alive a block is"
   (high at midday on busy retail streets, low at 4 a.m.). The joint ped/parking heads share a
   GRU representation that encodes this. Forcing occupancy down (`restrict_park` → 0.30) makes
   the model read the street as *quieter*, so the ped head predicts **less** footfall.
   `step_11_scenario.py:437-441` sets `occupancy_rate = magnitude` — that is the entire signal.

2. **Network propagation spreads the decline.** In `_rollout`:
   - `next_row[:, FI_PED_FLOW] = pred_ped_norm` (line ~585) feeds the lowered ped prediction back
     autoregressively for *all* streets; the spatial + semantic GCN branches diffuse the "quieter"
     signal to neighbours.
   - `next_row[parking_mask, FI_OCC_RATE] = pred_park_norm[parking_mask]` (line ~590) feeds
     model-predicted occupancy back, so the target's depressed occupancy pulls neighbouring
     sensor streets down too.

**Conclusion:** the model cannot represent "remove cars → free sidewalk → more people." It only
knows the statistical association, and the association runs the other way.

---

## 3. What we tried, and what it proved (the negative result)

`curbside_dining` (D-025) was designed to counter this: it lowers occupancy AND raises the
land-use features the model associates with footfall (`cafe_count`, `dining_capacity`, etc.),
netting the two pushes. We tested whether it recovers the desired sign.

**Demo: street 20009, t_start=10476, dur=roll=16. 12 bays reclaimed ≈ same occupancy shock as
restrict_park→0.30 (so the only new ingredient is the land-use uplift).**

| Scenario | target mean Δped |
|---|---|
| restrict_park → 0.30 | **−17.6** |
| curbside_dining 12 bays, default constants | **−13.7** |
| curbside_dining 12 bays, 4× café / 2× seats per bay | **−60.4** |

**Critical finding — strengthening the uplift made it WORSE, not better:**

1. **Permutation importance is sign-agnostic.** `cafe_count` (ΔMAE +6.67) / `bar_count` (+5.01)
   rank high in importance, but pushing them *up* drives the ped prediction *down*. The learned
   land-use↔footfall relationship is **not the positive causal lever the policy story assumes.**
2. **Static land-use features are OOD-fragile.** They are constant per street; the model never
   saw a block with 4× its café frontage, so large perturbations are unreliable extrapolation.
3. The "111/163 streets positive" network count under default curbside is a **mirage** — tiny
   deltas on low-confidence *imputed* (placeholder) streets, while the sensor-street mean stays
   negative (−0.32).

**Therefore: you cannot get the desired output by perturbing model features.** Not via
`occupancy_rate` (confounder, wrong sign) and not via land-use features (wrong sign / OOD-fragile).

Script: `melbourne_pipeline/scratch/run_curbside_demo.py`.

---

## 4. The fix: post-hoc elasticity composition ("Route 1")

Stop asking the GNN for the footfall *response*. Instead:

1. **Impose** an externally-estimated footfall uplift on the treated street's baseline:
   `ped_treated = ped_baseline × (1 + uplift)`. The causal claim lives in this number
   (urban-design literature), **not** in the model.
2. Use the GNN **only** for what it does well: propagating that imposed shock through the
   spatial/semantic graph to compute **network spillover**.

This cleanly separates the **causal assumption** (literature) from the **propagation** (GNN),
consistent with the project frame: *"GNN = coverage/propagation, not causal accuracy."*

Rigorous alternative (out of thesis scope, noted for completeness): **Route 2** =
difference-in-differences on streets that actually had bays removed during Nov 2025–Mar 2026,
if such events exist in the Supabase parking history. That is the only route where the positive
effect is a *finding* rather than an *assumption*.

---

## 5. Uplift band (signed off; sources VERIFIED 2026-06-21)

Multiplicative `ped_treated = ped_baseline × (1 + uplift)` on the target over the active window.
**Band: {0, 18, 30, 39}%** — grounded in verified primary studies.

| Band point | Value | Grounding |
|---|---|---|
| Placebo | 0% | Sanity floor; must yield exactly 0 on the target (isolates propagation/rounding noise) |
| Conservative | **+18%** | Cambra & Moura (2020), Lisbon street improvement — statistically significant pedestrian *volumes* (throughput; same metric as `ped_flow`) |
| Central | **~30%** | Midpoint of the throughput-comparable measured range |
| Optimistic | **+39%** | Aldred & Croft (2019), Hounslow modal filter — measured walking increase |

> **Provenance note.** The earlier `{5,12,25,40}%` band was **unsourced** and is retracted. The
> "*Pedestrian Pound* 2018 / NACTO / SF Pavement-to-Parks / NYC DOT" citations originally listed were
> **unverified from memory** and have been removed. The numbers above were verified on 2026-06-21:
> Living Streets *Pedestrian Pound* 3rd Ed. (2024) fetched directly; the two primaries confirmed to
> exist in *Journal of Transport & Health*. See `docs/references/evidence/{cambra2020,aldred2019,
> livingstreets2024}.md` and registry keys `cambra2020`, `aldred2019`, `livingstreets2024`.

**Carmona et al. (2018), +94% — EXCLUDED.** It bundles stationary/lingering activity, not pure
pedestrian flow, so it is not metric-comparable to `ped_flow`. (Decision 2026-06-21.)

**Induced vs diverted — empirical support for the propagation design.** Aldred & Croft (2019) found
only **~30% of the treated street's walking gain was genuinely new; ~70% was diverted from other
routes.** This independently validates the two-part engine: imposed magnitude on the treated street
PLUS spatial redistribution to neighbours. The GNN propagation *is* the diversion model — so a treated
street's gain being mostly redistribution is evidence-backed, not an artefact.

**Caveats that must travel with the numbers:** (1) UK/Portugal contexts, not Melbourne CBD →
external-validity transfer assumption; (2) the +18% (Lisbon) and +39% (Hounslow) are throughput-metric
matches; (3) these are public-realm/modal-filter schemes used as proxies for kerb reallocation.

**Report the band, not a point estimate.** Multiplicative (not additive) form is deliberate: a busy
lunchtime block gets a larger *absolute* gain than a 4 a.m. one, which is more realistic and avoids
the `boost_ped` problem of adding the same count to a dead street as to a packed one.

---

## 6. What changed in code

All edits in `melbourne_pipeline/steps/step_11_scenario.py` (post-training, not frozen):

| # | Location | Change |
|---|---|---|
| 1 | `VALID_INTERVENTIONS` (~line 112) | Added `"reallocate_kerb"` |
| 2 | `_encode_intervention` signature (~line 409) | Added `step` and `baseline_ped_norm` args |
| 3 | `_encode_intervention` body (after `boost_ped`) | New `reallocate_kerb` branch: scales target ped INPUT to `baseline × (1+uplift)`, **pinned to the baseline rollout** (not the evolving treated prediction) so the shock cannot compound |
| 4 | `_rollout` call site (~line 595) | Passes `step=step, baseline_ped_norm=intervention.get("_baseline_ped_norm")` |
| 5 | `run_scenario` validation (~line 862) | Requires magnitude for `reallocate_kerb` (uplift fraction) |
| 6 | `run_scenario`, after baseline rollout | Attaches `intervention["_baseline_ped_norm"] = baseline_ped_norm` |
| 7 | `run_scenario`, after denormalise (~line 967) | **Overwrites the target's REPORTED series** to exactly `baseline × (1+uplift)` over the active window, so the correlational model cannot regress the exogenous claim back toward neighbours |

**Why edit #7 matters:** without it, the reported target was the model's re-prediction, which
diluted a +12% assumption down to +3% (the GNN regressing the boosted input toward neighbours).
The boosted input still correctly drives spillover; we only override the *target's own reported
delta* so the figure shows the imposed elasticity. Neighbours keep their GNN-propagated values.

**Honesty gate:** unchanged. `reallocate_kerb` perturbs ped (not parking), so it is NOT in
`PARKING_INTERVENTIONS` and falls into the ped-sensor branch of `_data_backing` — it requires a
real ped sensor (confidence 1.0), exactly like `boost_ped`. Street 20009 qualifies.

---

## 7. What we got (verified result) — CONSERVATION MODEL

> **Design evolution.** The first build used GNN *diffusion* for the network effect, which made
> neighbours GAIN footfall. When Aldred & Croft (2019) was primary-verified (NotebookLM read the PDF),
> it showed the treated street's +39% is GROSS and **~69% is diverted FROM neighbours** (they LOSE) —
> the opposite sign. So the network propagation was replaced by a **mass-conserving redistribution**
> (user-approved, D-026 update 2026-06-21). The superseded positive-spillover numbers are dropped.

Mechanism: impose gross +U% on the treated street; subtract the diverted share
(`REALLOCATE_DIVERTED_FRACTION = 0.692`) from its spatial neighbours, weighted by learned spatial edge
weights; the new share (`REALLOCATE_NEW_FRACTION = 0.308`) is the only net-new footfall. GNN diffusion
is overridden — the GNN now supplies only the *edge weights* (which neighbours lose, how much).

Sweep: `melbourne_pipeline/scratch/run_reallocate_sweep.py`
(street 20009, t_start=10476, dur=roll=16, baseline mean ped ≈ 295/15min, 4 spatial neighbours):

| uplift | target Δped | target Δ% | nbr loss sum | city net | city/target | #neg/N |
|---|---|---|---|---|---|---|
| 0% (placebo) | +0.0 | +0.0% | +0.0 | +0.0 | 0.00 | 0/0 |
| 18% | +53.1 | +18.0% | −36.7 | +16.3 | 0.31 | 4/4 |
| 30% | +88.4 | +30.0% | −61.2 | +27.2 | 0.31 | 4/4 |
| 39% | +115.0 | +39.0% | −79.5 | +35.4 | 0.31 | 4/4 |

- **Target Δ% tracks the imposed uplift exactly** (39% → +39.0%).
- **Neighbours LOSE** (nbr loss sum < 0; all 4 neighbours negative) — matches the diversion evidence.
- **Mass conserved exactly:** neighbour losses sum to 69.2% of the treated gain; **city net / target =
  0.31 = the new-trip fraction**. So ~69% of the treated street's gain is redistribution, ~31% genuinely new.
- **Placebo (0%) is exactly zero.**

Known weakness: diversion is concentrated on **1-hop** spatial neighbours (here 4); multi-hop spread is a
possible refinement. Parking-side displacement (occupancy) is still NOT modelled — open item.

---

## 8. Frontend status — DONE (2026-06-21)

`melbourne_pipeline/frontend/sensor_map_viz.html` was updated to expose `reallocate_kerb`. Edits:
1. **Wizard card** (~line 2256): new "Kerb reallocation (footfall uplift)" card, green dot `#22c55e`,
   `data-iv="reallocate_kerb"`, with a desc stating the uplift is an assumption, not a prediction.
2. **Hidden select option** (~line 2270): `<option value="reallocate_kerb">Kerb reallocation</option>`.
3. **Assumption caveat note** (~line 2355): hidden `#iv-assumption-note` div, shown only for this
   intervention, explaining the imposed-elasticity framing in plain language.
4. **`ivTypeChanged`** (~line 4344): shows the magnitude row as **"Footfall uplift %"**, range 0–40,
   step 1, default 12 (the central band value).
5. **`ivApply`** (~line 4566): added `reallocate_kerb` to `needsMag`; the percent input is divided by
   100 before being sent (the API expects a fraction, 12 → 0.12).
6. **`IV_LABELS_SHORT`** (~line 4685): added `reallocate_kerb: 'Kerb reallocation'` for the verdict banner.
7. **Result render** (~line 4699): toggles the caveat note visible for `reallocate_kerb`, hidden otherwise.

**No API change needed.** `api_server.py` `POST /scenario` passes `intervention_type` straight through
to `run_scenario` and returns the result verbatim; `_build_network_summary` produces the
`treated_street` / `spatial_neighbours` keys the frontend reads for every intervention type, so the new
type renders through the identical path. (Note: `curbside_dining` is still NOT exposed in the UI — only
`reallocate_kerb` was added.)

**Full UI chain:** card → `iv-type-select=reallocate_kerb` → `ivApply` (percent→fraction) → API
passthrough → `run_scenario` → result with `treated_street`/`spatial_neighbours` → panel + caveat note.

**Not yet verified in a live browser** — changes are static HTML/JS and the backend path is already
verified (§7). A manual smoke test (start `python api_server.py`, open :5050, pick a ped-sensor street,
run Kerb reallocation at 12%) is the remaining confirmation step.

---

## 9. How to use it (CLI / programmatic)

```python
from melbourne_pipeline.steps import step_11_scenario as s
res = s.run_scenario(
    street_id="20009",
    t_start=10476, duration=16, rollout_steps=16,
    intervention_type="reallocate_kerb",
    magnitude=0.12,          # uplift fraction; 0.12 = +12%
    save=True,
)
```

Run the full sweep: `PYTHONPATH=<repo root> python melbourne_pipeline/scratch/run_reallocate_sweep.py`
(edit the `UPLIFTS` list to change bounds — bounds are a one-line list by design).

---

## 10. Assumptions that affect thesis results

- **The uplift band {5,12,25,40}% is a STATED ASSUMPTION, not a model output.** The causal claim
  lives entirely in that number, sourced from public-realm footfall literature. Must be reported
  as such; the GNN only does propagation.
- **The +40% row is a ceiling stress-probe**, not a recommended planning value.
- **Spillover magnitudes inherit the model's graph + autoregressive dynamics** — neighbour deltas
  beyond ~4h (16 steps) are indicative, not precise (existing rollout caveat).
- **No test in `tests/` yet** locks this in. Suggested: assert placebo→0 and +12%→+12.0% on target.
- The `restrict_park` / `curbside_dining` results are retained deliberately as **methods-section
  evidence** for *why* naïve feature perturbation is invalid — do not delete them as "failures."
