# Su et al. (2022) — Rhythm of the Streets, Boston

**Full citation:** Su, T., Sun, M., Fan, Z., Noyman, A., Pentland, A., & Moro, E. (2022).
Rhythm of the streets: A street classification framework based on street activity patterns.
*EPJ Data Science*, 11, 43.

## What they did
- Activity-Based Street Type (AST) framework for 18,023 Boston street segments
- Data: anonymised GPS mobility data (Cuebiq), 12 weeks (Oct–Dec 2017), 82,620 unique users
- Two-step clustering: (1) FCM on activity volume → 4 types (Subdued/Calm/Moderate/Vibrant),
  (2) FCM on normalised 168-hour activity sequence → 3 patterns (Work/Hybrid/Leisure)
- Combined: 10 final AST types

## Key findings
- ASTs have NMI = 0.008 vs functional street categories (CFCC) → almost no overlap
- ASTs have NMI = 0.04 vs land-use categories → slightly more but still very low
- Key insight: land use alone cannot predict street activity patterns
  (commercial streets split ~50/50 between Vibrant and non-Vibrant types)
- Crime prediction: ASTs outperform both functional and contextual classifications (RMSE 4.14 vs 4.27)
- Work pattern: single daily peak on weekdays, low weekends
- Hybrid pattern: two daily peaks weekdays, one peak weekends
- Leisure pattern: peak Friday and Saturday evenings

## How this supports the thesis
- **Direct justification for GMM clustering combining land use AND activity patterns** (Step 07)
- Proves that static land use features alone are insufficient — temporal behaviour must be included
- Work/Hybrid/Leisure archetypes map directly to our morning/midday/evening cluster labelling
- Two-step approach (volume then pattern) mirrors our PCA → GMM procedure
- Boston finding that streets near same land use can have different activity patterns →
  justifies our semantic graph using mutual k-NN (not just spatial proximity)

## Key numbers to cite
- NMI(AST, land-use) = 0.04 → "land use explains only 4% of street activity variation"
- 10 AST types in Boston; we expect 3–5 clusters in Melbourne CBD (smaller, denser study area)
- Crime prediction improvement: cite as evidence that activity-based classification has
  downstream analytical value beyond description

## Limitations they note
- Data from single 12-week period (Oct–Dec, includes Christmas)
- GPS data biased toward certain user groups
- Boston only — cross-city generalisation not tested