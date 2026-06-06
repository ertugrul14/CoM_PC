# Curbside Reallocation Candidates (hard sensor gate)

Generated from steps_v2 (D-020 hard parking-sensor gate, D-021 C+B window selection). Source: data/processed_v2/clustered.parquet. Every street below carries a REAL parking sensor, so the kerb-slack half of each flexibility window is observed, not imputed.

## Summary

| metric | value |
|---|---|
| Parking-sensor candidates (gate-passing) | 143 |
| Candidates with a daytime flexibility window | 83 |
| -- Tier A (ped+park both observed) | 22 |
| -- Tier B (park observed, ped modelled) | 61 |

Window = a (day x time-block) cell where parking occupancy < 0.30 AND ped flow exceeds the street cluster median. Overnight (night) blocks are excluded; best window = highest ped demand. mean ped / mean park are street DAILY means (a high daily mean can still hide a sub-0.30 trough).

## Tier A - confirmed (both halves observed) - 22 streets

| street_id | name | tier | archetype | best window | #wins | mean ped | mean park | conf |
|---|---|---|---|---|---|---|---|---|
| 20120 | Collins Street between King Street and Spencer Street | A (confirmed) | major_pedestrian_corridor | Thu_morning | 9 | 293.2 | 0.344 | 1.00 |
| 20127 | Flinders Lane between Swanston Street and Elizabeth Street | A (confirmed) | major_pedestrian_corridor | Wed_midday | 27 | 260.8 | 0.243 | 1.00 |
| 20167 | Elizabeth Street between Little Bourke Street and Lonsdale Street | A (confirmed) | parking_reallocation_priority | Thu_morning | 5 | 215.2 | 0.426 | 1.00 |
| 20009 | Lonsdale Street between Elizabeth Street and Queen Street | A (confirmed) | parking_reallocation_priority | Thu_morning | 7 | 156.9 | 0.461 | 1.00 |
| 20112 | Collins Street between Swanston Street and Elizabeth Street | A (confirmed) | parking_reallocation_priority | Thu_midday | 2 | 146.4 | 0.397 | 1.00 |
| 20106 | Collins Street between Spring Street and Exhibition Street | A (confirmed) | parking_reallocation_priority | Sun_morning | 1 | 144.9 | 0.424 | 1.00 |
| 20090 | Bourke Street between King Street and Spencer Street | A (confirmed) | parking_reallocation_priority | Thu_work_pm | 21 | 135.2 | 0.321 | 1.00 |
| 20080 | Bourke Street between Russell Street and Swanston Street | A (confirmed) | parking_reallocation_priority | Thu_midday | 18 | 122.4 | 0.380 | 1.00 |
| 20073 | Spencer Street between Little Collins Street and Bourke Street | A (confirmed) | parking_reallocation_priority | Wed_morning | 3 | 120.8 | 0.419 | 1.00 |
| 22819 | Collins Street between Spencer Street and Batmans Hill Drive | A (confirmed) | latent_evening_potential_high | Tue_work_pm | 27 | 116.3 | 0.043 | 1.00 |
| 20150 | Russell Street between Bourke Street and Little Bourke Street | A (confirmed) | parking_reallocation_priority | Sat_morning | 2 | 109.1 | 0.477 | 1.00 |
| 20005 | Lonsdale Street between Russell Street and Swanston Street | A (confirmed) | parking_reallocation_priority | Wed_work_am | 2 | 105.6 | 0.452 | 1.00 |
| 20212 | Flinders Street between Russell Street and Swanston Street | A (confirmed) | parking_reallocation_priority | Thu_work_pm | 11 | 77.2 | 0.350 | 1.00 |
| 20992 | Errol Street between Victoria Street and Queensberry Street | A (confirmed) | parking_reallocation_priority | Sun_work_pm | 10 | 63.8 | 0.272 | 1.00 |
| 20210 | Flinders Street between Exhibition Street and Russell Street | A (confirmed) | parking_reallocation_priority | Mon_work_pm | 8 | 62.1 | 0.319 | 1.00 |
| 20172 | Queen Street between Collins Street and Little Collins Street | A (confirmed) | parking_reallocation_priority | Sat_morning | 1 | 52.2 | 0.482 | 1.00 |
| 20206 | Spring Street between Little Bourke Street and Lonsdale Street | A (confirmed) | parking_reallocation_priority | Sun_midday | 9 | 49.1 | 0.321 | 1.00 |
| 20195 | King Street between Little Lonsdale Street and La Trobe Street | A (confirmed) | parking_reallocation_priority | Sun_work_pm | 10 | 44.1 | 0.310 | 1.00 |
| 20028 | La Trobe Street between William Street and King Street | A (confirmed) | parking_reallocation_priority | Wed_morning | 7 | 40.7 | 0.267 | 1.00 |
| 20030 | La Trobe Street between King Street and Spencer Street | A (confirmed) | parking_reallocation_priority | Sun_work_am | 11 | 39.2 | 0.313 | 1.00 |
| 20093 | Little Collins Street between Exhibition Street and Russell Street | A (confirmed) | parking_reallocation_priority | Thu_midday | 5 | 22.8 | 0.207 | 1.00 |
| 22801 | Bourke Street between Harbour Esplanade and Enterprize Way | A (confirmed) | parking_reallocation_priority | Wed_morning | 3 | 18.8 | 0.047 | 1.00 |

## Tier B - provisional (parking observed, ped demand modelled) - 61 streets

| street_id | name | tier | archetype | best window | #wins | mean ped | mean park | conf |
|---|---|---|---|---|---|---|---|---|
| 20110 | Collins Street between Russell Street and Swanston Street | B (provisional) | major_pedestrian_corridor | Thu_midday | 35 | 266.8 | 0.040 | 1.00 |
| 20055 | Little Bourke Street between Swanston Street and Elizabeth Street | B (provisional) | parking_reallocation_priority | Thu_morning | 5 | 219.3 | 0.439 | 1.00 |
| 20099 | Little Collins Street between Elizabeth Street and Queen Street | B (provisional) | parking_reallocation_priority | Thu_midday | 3 | 212.4 | 0.401 | 1.00 |
| 20095 | Little Collins Street between Russell Street and Swanston Street | B (provisional) | parking_reallocation_priority | Thu_morning | 4 | 166.9 | 0.399 | 1.00 |
| 20189 | King Street between Flinders Lane and Collins Street | B (provisional) | parking_reallocation_priority | Thu_work_pm | 25 | 160.5 | 0.270 | 1.00 |
| 20129 | Flinders Lane between Elizabeth Street and Queen Street | B (provisional) | parking_reallocation_priority | Thu_work_pm | 12 | 155.4 | 0.316 | 1.00 |
| 20152 | Russell Street between Lonsdale Street and Little Lonsdale Street | B (provisional) | parking_reallocation_priority | Thu_morning | 6 | 149.5 | 0.471 | 1.00 |
| 20007 | Lonsdale Street between Swanston Street and Elizabeth Street | B (provisional) | parking_reallocation_priority | Thu_morning | 4 | 143.5 | 0.459 | 1.00 |
| 20146 | Russell Street between Flinders Street and Flinders Lane | B (provisional) | parking_reallocation_priority | Wed_morning | 2 | 143.2 | 0.380 | 1.00 |
| 20216 | Flinders Street between Elizabeth Street and Queen Street | B (provisional) | parking_reallocation_priority | Sun_work_am | 8 | 140.5 | 0.359 | 1.00 |
| 20139 | Exhibition Street between Flinders Lane and Collins Street | B (provisional) | parking_reallocation_priority | Thu_midday | 35 | 135.8 | 0.063 | 1.00 |
| 20190 | King Street between Collins Street and Little Collins Street | B (provisional) | parking_reallocation_priority | Wed_work_pm | 33 | 132.1 | 0.160 | 1.00 |
| 20118 | Collins Street between William Street and King Street | B (provisional) | parking_reallocation_priority | Wed_morning | 5 | 130.7 | 0.397 | 1.00 |
| 20148 | Russell Street between Collins Street and Little Collins Street | B (provisional) | parking_reallocation_priority | Thu_morning | 5 | 119.0 | 0.373 | 1.00 |
| 20200 | Spring Street between Collins Street and Little Collins Street | B (provisional) | parking_reallocation_priority | Thu_morning | 6 | 111.3 | 0.377 | 1.00 |
| 20198 | Spring Street between Flinders Lane and Collins Street | B (provisional) | parking_reallocation_priority | Sun_morning | 1 | 107.8 | 0.441 | 1.00 |
| 20069 | Spencer Street between Flinders Lane and Collins Street | B (provisional) | parking_reallocation_priority | Wed_work_pm | 19 | 102.6 | 0.316 | 1.00 |
| 20175 | Queen Street between Little Bourke Street and Lonsdale Street | B (provisional) | parking_reallocation_priority | Sun_work_am | 8 | 100.6 | 0.353 | 1.00 |
| 20151 | Russell Street between Little Bourke Street and Lonsdale Street | B (provisional) | parking_reallocation_priority | Sat_work_am | 4 | 99.1 | 0.516 | 1.00 |
| 20149 | Russell Street between Little Collins Street and Bourke Street | B (provisional) | parking_reallocation_priority | Sun_morning | 1 | 97.4 | 0.491 | 1.00 |
| 20170 | Queen Street between Flinders Street and Flinders Lane | B (provisional) | parking_reallocation_priority | Sun_work_am | 6 | 89.9 | 0.415 | 1.00 |
| 20191 | King Street between Little Collins Street and Bourke Street | B (provisional) | parking_reallocation_priority | Thu_work_pm | 24 | 89.5 | 0.268 | 1.00 |
| 20176 | Queen Street between Lonsdale Street and Little Lonsdale Street | B (provisional) | parking_reallocation_priority | Sun_work_am | 8 | 87.5 | 0.375 | 1.00 |
| 20003 | Lonsdale Street between Exhibition Street and Russell Street | B (provisional) | parking_reallocation_priority | Sun_work_am | 5 | 85.7 | 0.429 | 1.00 |
| 20105 | Little Collins Street between King Street and Spencer Street | B (provisional) | parking_reallocation_priority | Sat_work_pm | 12 | 81.2 | 0.351 | 1.00 |
| 20088 | Bourke Street between William Street and King Street | B (provisional) | parking_reallocation_priority | Thu_midday | 21 | 80.4 | 0.275 | 1.00 |
| 22555 | Wurundjeri Way between Bourke Street and Flinders Street | B (provisional) | parking_reallocation_priority | Tue_work_pm | 25 | 79.7 | 0.040 | 1.00 |
| 20108 | Collins Street between Exhibition Street and Russell Street | B (provisional) | parking_reallocation_priority | Wed_morning | 4 | 77.0 | 0.357 | 1.00 |
| 20116 | Collins Street between Queen Street and Market Street | B (provisional) | parking_reallocation_priority | Thu_morning | 11 | 76.8 | 0.301 | 1.00 |
| 20434 | Godfrey Street between Bourke Street and Little Collins Street | B (provisional) | parking_reallocation_priority | Thu_morning | 5 | 71.7 | 0.446 | 1.00 |
| 22862 | McCrae Street between Wurundjeri Way and Batmans Hill Drive | B (provisional) | parking_reallocation_priority | Wed_work_pm | 35 | 66.1 | 0.015 | 1.00 |
| 20174 | Queen Street between Bourke Street and Little Bourke Street | B (provisional) | parking_reallocation_priority | Wed_morning | 3 | 63.1 | 0.417 | 1.00 |
| 20135 | Flinders Lane between William Street and King Street | B (provisional) | parking_reallocation_priority | Wed_midday | 35 | 62.0 | 0.105 | 1.00 |
| 20140 | Exhibition Street between Collins Street and Little Collins Street | B (provisional) | parking_reallocation_priority | Wed_morning | 2 | 58.8 | 0.447 | 1.00 |
| 20001 | Lonsdale Street between Spring Street and Exhibition Street | B (provisional) | parking_reallocation_priority | Wed_morning | 10 | 56.3 | 0.343 | 1.00 |
| 20044 | Little Lonsdale Street between Queen Street and William Street | B (provisional) | parking_reallocation_priority | Sun_midday | 10 | 55.4 | 0.355 | 1.00 |
| 20171 | Queen Street between Flinders Lane and Collins Street | B (provisional) | parking_reallocation_priority | Thu_morning | 8 | 52.8 | 0.398 | 1.00 |
| 20224 | Flinders Street between King Street and Spencer Street | B (provisional) | parking_reallocation_priority | Wed_morning | 7 | 51.9 | 0.363 | 1.00 |
| 21553 | Nicholson Street between Spring Street and Albert Street | B (provisional) | parking_reallocation_priority | Mon_midday | 17 | 50.8 | 0.281 | 1.00 |
| 20086 | Bourke Street between Queen Street and William Street | B (provisional) | parking_reallocation_priority | Wed_morning | 2 | 50.8 | 0.374 | 1.00 |
| 20137 | Flinders Lane between King Street and Spencer Street | B (provisional) | parking_reallocation_priority | Mon_work_pm | 3 | 49.8 | 0.364 | 1.00 |
| 20177 | Queen Street between Little Lonsdale Street and La Trobe Street | B (provisional) | parking_reallocation_priority | Sat_morning | 2 | 48.6 | 0.496 | 1.00 |
| 20026 | La Trobe Street between Queen Street and Wills Street | B (provisional) | parking_reallocation_priority | Thu_work_pm | 24 | 48.0 | 0.269 | 1.00 |
| 20141 | Exhibition Street between Little Collins Street and Bourke Street | B (provisional) | parking_reallocation_priority | Thu_midday | 17 | 46.6 | 0.298 | 1.00 |
| 20173 | Queen Street between Little Collins Street and Bourke Street | B (provisional) | parking_reallocation_priority | Thu_midday | 5 | 46.3 | 0.392 | 1.00 |
| 20147 | Russell Street between Flinders Lane and Collins Street | B (provisional) | parking_reallocation_priority | Sun_morning | 1 | 45.1 | 0.434 | 1.00 |
| 20194 | King Street between Lonsdale Street and Little Lonsdale Street | B (provisional) | parking_reallocation_priority | Wed_work_pm | 23 | 45.0 | 0.284 | 1.00 |
| 20103 | Little Collins Street between William Street and King Street | B (provisional) | parking_reallocation_priority | Sun_work_pm | 10 | 44.1 | 0.332 | 1.00 |
| 20046 | Little Lonsdale Street between William Street and King Street | B (provisional) | parking_reallocation_priority | Thu_work_am | 15 | 43.8 | 0.311 | 1.00 |
| 20208 | Flinders Street between Spring Street and Exhibition Street | B (provisional) | parking_reallocation_priority | Thu_work_am | 5 | 39.9 | 0.318 | 1.00 |
| 21521 | Wills Street between La Trobe Street and A'Beckett Street | B (provisional) | parking_reallocation_priority | Fri_work_pm | 7 | 38.2 | 0.386 | 1.00 |
| 20133 | Flinders Lane between Market Street and William Street | B (provisional) | parking_reallocation_priority | Thu_morning | 6 | 34.9 | 0.373 | 1.00 |
| 20059 | Little Bourke Street between Queen Street and William Street | B (provisional) | parking_reallocation_priority | Thu_morning | 6 | 34.0 | 0.382 | 1.00 |
| 20049 | Little Bourke Street between Spring Street and Exhibition Street | B (provisional) | parking_reallocation_priority | Mon_midday | 1 | 34.0 | 0.337 | 1.00 |
| 20121 | Flinders Lane between Spring Street and Exhibition Street | B (provisional) | parking_reallocation_priority | Sun_morning | 1 | 32.1 | 0.372 | 1.00 |
| 22453 | Footscray Road between Docklands Drive and Waterfront Way | B (provisional) | parking_reallocation_priority | Sat_work_pm | 2 | 30.0 | 0.110 | 1.00 |
| 20990 | Queensberry Street between Leveson Street and Errol Street | B (provisional) | parking_reallocation_priority | Mon_evening | 1 | 27.7 | 0.187 | 1.00 |
| 20889 | Albert Street between Nicholson Street and Gisborne Street | B (provisional) | parking_reallocation_priority | Thu_morning | 4 | 27.2 | 0.127 | 1.00 |
| 20983 | Victoria Street between Leveson Street and Errol Street | B (provisional) | parking_reallocation_priority | Sat_work_am | 1 | 25.3 | 0.088 | 1.00 |
| 20131 | Flinders Lane between Queen Street and Market Street | B (provisional) | parking_reallocation_priority | Thu_morning | 4 | 24.7 | 0.234 | 1.00 |
| 23314 | Collins Street between Bourke Street and Sailmaker Way | B (provisional) | parking_reallocation_priority | Sun_morning | 1 | 20.7 | 0.043 | 1.00 |
