# Asher et al. (2025) — Pedestrian Dynamics, Melbourne

**Full citation:** Asher, M., Oswald, Y., & Malleson, N. (2025). Understanding pedestrian dynamics
using machine learning with real-time urban sensors. *EPB: Urban Analytics and City Science*, 52(8), 1994–2017.

## What they did
- Random Forest model predicting pedestrian footfall across Melbourne CBD streets
- 18 pedestrian sensors (City of Melbourne open data), 2011–2020 (pre-COVID only)
- Features: land use (CLUE), time-of-day, day-of-week, weather, buffer distance from sensors
- Validated with leave-one-sensor-out cross-validation

## Key findings
- Random Forest outperformed linear regression and neural net baselines
- Buffer size analysis: 200m radius around sensors gave best spatial feature representation
- Top predictors: hour-of-day, day-of-week, employment density (CLUE jobs), café count
- Weekday vs weekend patterns differ substantially — separate models improve accuracy
- Model generalises well to unsensored locations when static features are rich

## How this supports the thesis
- Justifies XGBoost imputation for unsensored streets (Step 05) using CLUE static features
- Directly comparable study area (Melbourne CBD) and sensor infrastructure
- Validates feature set: 17 static features (jobs, café, bar, POI) as ped flow predictors
- GroupKFold over streets methodology adopted directly from their leave-one-sensor-out approach

## Key numbers to cite
- Our GroupKFold R² = 0.571 (log scale) vs their R² reported per sensor
- 82 sensored streets → predict 3,893 unsensored streets (much larger scale)

## Limitations they note
- Restricted to pre-COVID data; pandemic behaviour shifts excluded
- Sensor placement biased toward high-footfall areas (selection bias)