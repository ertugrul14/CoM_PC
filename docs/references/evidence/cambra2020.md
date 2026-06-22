# Cambra & Moura (2020) — Street improvement → pedestrian volume change (Lisbon)

**Full citation:** Cambra, P., & Moura, F. (2020). *How does walkability change relate to walking
behavior change? Effects of a street improvement in pedestrian volumes and walking experience.*
Journal of Transport & Health, 16, 100797.
https://www.sciencedirect.com/science/article/abs/pii/S2214140519302129

**Verification status:** Primary study confirmed (Journal of Transport & Health, 2020). The headline
figure below is as reported in the paper and re-quoted by Living Streets (2024) — see
[[livingstreets2024]]. Abstract verified; full text not opened (paywalled). Confirm the +18% against
the primary before final thesis submission.

## What they did
- Before/after natural-experiment evaluation of a public-realm street improvement in Lisbon.
- Measured pedestrian volumes on two improved streets + a public square against comparison streets
  that received no improvement (difference-in-differences flavour).

## Key finding (the number we use)
- **+18% increase in pedestrian volumes**, statistically significant, on the improved streets/square
  relative to unimproved comparison streets (which showed no change).

## How this supports the thesis (D-026)
- This is the **conservative anchor** of the `reallocate_kerb` footfall-uplift band. It measures
  *pedestrian volume* (throughput), which is the same quantity our model predicts (`ped_flow`),
  making it a clean metric match — unlike studies that bundle stationary/lingering activity.
- It is a measured intervention effect, supplying the kind of external, evidence-based magnitude that
  the correlational GNN cannot itself produce (see [[livingstreets2024]], [[aldred2019]]).

## Caveats
- Lisbon context, not Melbourne CBD → external-validity assumption when transferring the elasticity.
- A public-realm *improvement*, not specifically parking removal; treat as a proxy for kerb upgrade.
