# Living Streets (2024) — The Pedestrian Pound, 3rd Edition

**Full citation:** Hopkinson, L., Hiblin, B., Wedderburn, M., Chatterjee, K., Cairns, S., & Frearson,
M. (2024). *The Pedestrian Pound: The business case for better streets and places, 3rd Edition.*
Transport for Quality of Life for Living Streets. November 2024.
https://reports.livingstreets.org.uk/pedestrianpound/MainReport/index.html

**Verification status:** Primary report fetched and verified directly (title, authors, publisher,
year, and the per-scheme figures below all confirmed from the report text).

## What it is
- Evidence review / business case aggregating measured before/after studies of public-realm and
  pedestrianisation schemes, with footfall, spend, and active-travel outcomes.
- **Secondary source** — used here as the entry point to the underlying primary studies, which are
  cited individually ([[cambra2020]], [[aldred2019]]).

## Per-scheme footfall figures it reports (with original citations)
- Lisbon: **+18% pedestrian volumes** (Cambra & Moura, 2020 → [[cambra2020]]). Throughput; our metric.
- Hounslow: **+39% walking, ~30% new / ~70% diverted** (Aldred & Croft, 2019 → [[aldred2019]]).
- 5 London high streets: +94% walking + static activity (Carmona et al., 2018). **EXCLUDED from our
  band (D-026):** bundles stationary/lingering activity, not pure pedestrian flow, so it is not metric-
  comparable to `ped_flow`.
- Mini-Holland, 3 boroughs: +40 min walking/week/person (Aldred et al., 2024). Person-level, not a
  street footfall %.

## How this supports the thesis (D-026)
- Supplies the externally-measured footfall elasticities injected into `reallocate_kerb`. The
  associational GNN cannot derive a causal intervention effect; these measured schemes provide it,
  while the GNN handles spatial propagation. Cite primaries ([[cambra2020]], [[aldred2019]]) for the
  numbers; cite this report for the synthesised business-case framing.

## Caveats
- UK/Portugal contexts, not Melbourne CBD → external-validity assumption on transfer.
- Advocacy-organisation report; rely on the underlying peer-reviewed primaries for the figures.
