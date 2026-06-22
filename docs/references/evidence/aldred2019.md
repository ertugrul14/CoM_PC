# Aldred & Croft (2019) — Hounslow streetscape/modal-filter scheme → walking change

**Full citation:** Aldred, R., & Croft, J. (2019). *Evaluating active travel and health economic
impacts of small streetscape schemes: An exploratory study in London.* Journal of Transport & Health,
12, 86–96. https://www.sciencedirect.com/science/article/abs/pii/S2214140518304006
Open access full text (try in a browser — both hosts block automated bots but serve browsers fine):
- Author-hosted: http://rachelaldred.org/wp-content/uploads/2019/01/Hounslow_revising_final_updated.pdf
- Westminster repo: https://westminsterresearch.westminster.ac.uk/item/q9yv9/evaluating-active-travel-and-health-economic-impacts-of-small-streetscape-schemes-an-exploratory-study-in-london

**Verification status: VERIFIED AT PRIMARY (2026-06-21)** via NotebookLM reading the full PDF
(figures, design, and caveats below are direct from the paper text; cross-checked against Living
Streets 2024 [[livingstreets2024]], which agreed). Journal of Transport & Health, 2019, vol. 12,
86–96; Church Street, Hounslow modal filter.

## Verified figures (exact, from the primary)
- **+39% gross uplift in people walking** through the street (and +19% cycling). Quote: *"Count data
  had shown an uplift of 39% in people walking through the street, and 19% more people cycling."*
- Measurement: **manual pedestrian COUNTS, 24-hour basis across a week** → throughput (people walking
  *through*), NOT stationary/lingering activity. Clean metric match to `ped_flow`.
- Baseline **859 ped/day (Oct 2015) → 1191 (Nov 2016)**; +332 pedestrians.
- **Induced vs diverted split: 30.8% new / 69.2% diverted.** Of the +332, only ~102 (30.8%) were
  *truly new* trips (mode shift); ~230 (69.2%) were *diverted/rerouted* from other streets. Split
  estimated by applying an intercept-survey ratio (4 of 13 = 31% "different mode" vs 9 = "different
  route") to the count increase. → genuinely-new component ≈ **+12%** of baseline; diverted-in ≈ +27%.
- **The 39% is GROSS, not net of diverted-in trips.** Critical for network modelling (see [[..\..\notes\decisions]] D-026).
- Health-economic benefit **£530,171 over 20 yrs** vs scheme cost **~£10,000** (~50×).

## What they did
- Intercept survey + evaluation of a small streetscape scheme (modal filter / residential street
  closure to through traffic) on Church Street, Hounslow, London.
- Estimated change in walking and decomposed it into genuinely new trips vs. trips diverted from
  other routes.

## Key findings (the numbers we use)
- **+39% increase in people walking** following the scheme.
- **~30% of the additional walking trips were genuinely new**; the remaining **~70% were diverted**
  (rerouted from elsewhere rather than induced).

## How this supports the thesis (D-026)
- **Optimistic anchor** of the `reallocate_kerb` footfall-uplift band (+39%, street-level closure).
- More importantly, the **~30% new / ~70% diverted split independently validates the two-part design
  of our scenario engine**: an imposed magnitude on the treated street PLUS spatial redistribution to
  neighbours. The GNN propagation models exactly the diversion component — empirical support that a
  treated street's footfall gain is largely redistribution, not pure creation.

## Caveats (from the primary — important for thesis defensibility)
- **Modal filter, NOT parking removal.** Restricted through-traffic (cyclists still pass); the paper
  does NOT say on-street parking was removed (residents mostly use off-street). So it is a *loose*
  proxy for kerb/parking reallocation — it measures the footfall effect of removing *through-traffic*,
  not of reclaiming *parking bays*. State this when using it to calibrate `reallocate_kerb`.
- **No control/comparison street** — before/after only. Weak causal design.
- **Exploratory, low sample** (124 valid survey responses; authors' own words "low sample size"); no
  formal significance tests, though confidence intervals were generated.
- **Sustained, not novelty:** Oct-2017 follow-up counts matched Nov-2016 → effect persisted ~2 yrs.
- **External validity:** authors warn Church Street is an "inherently attractive" historic riverside
  setting and results "might not be replicated in less historic and beautiful settings" → Melbourne
  CBD is a different context; transfer with caution.
- **Single-street displacement:** authors note filtering one street may have pushed traffic onto other
  residential streets (a negative externality) — mirrors the diversion finding.
