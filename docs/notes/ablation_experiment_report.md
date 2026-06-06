# Ablation Experiment: Does Imputation Help the GCN?

## Motivation

The MultiGCN pipeline trains on 1,397 Melbourne CBD streets, but only 74 have real
pedestrian sensors. The remaining 1,323 streets use XGBoost-imputed pedestrian flow
(GroupKFold R² = 0.582, median per-street R² = 0.158). The published model reports
a test MAE of 5.54 — but this metric averages across all 1,397 streets, including
1,323 streets with smooth, low-traffic, easy-to-predict imputed values.

We designed an ablation experiment to answer a simple question:
**does training on the full imputed graph actually help predict the 74 streets that matter?**

---

## Experimental Design

Three conditions, all evaluated on the same 74 sensor streets, using identical
hyperparameters (seed=42, W=96, H=64, lr=1e-3, patience=25, batch=8), and the
same chronological train/val/test split (70/15/15%).

### Baseline — Existing Model, Honest Evaluation

- The published model (best_model.pt), trained on all 1,397 streets with ped loss
  computed across the full graph
- No retraining — just re-evaluate, but compute metrics on the 74 sensor streets only
- Purpose: establish the true performance of the model on streets with real ground truth

### Experiment A — Sensor-Only Subgraph

- Cut the graph down to 74 nodes (sensor streets only)
- Recompute normalisation statistics on this subset
- Extract the subgraph: 58 spatial edges, 84 semantic edges survive
- 35 of 74 nodes are completely isolated (zero neighbours)
- Train a fresh MultiGCN from scratch on real data only
- Purpose: can the GCN learn from real data alone, without any imputation?

### Experiment B — Full Graph, Masked Loss

- Keep the full 1,397-node graph intact — imputed streets remain as nodes,
  edges, and features
- Train a fresh MultiGCN, but mask the pedestrian loss to only the 74 sensor
  streets — the model sees imputed streets during the forward pass (GCN message
  passing) but receives no gradient signal from them
- Purpose: do imputed streets provide useful graph context even when we don't
  grade the model on them?

---

## Results

### Sensor-Street Metrics (74 streets — the only honest comparison)

| Condition | Test MAE | Test RMSE | Test R² | Params | Best Epoch | Training Time |
|-----------|----------|-----------|---------|--------|------------|---------------|
| Baseline  | 30.72    | 81.08     | 0.815   | 68,076 | 190        | —             |
| Exp A     | 22.34    | 50.37     | 0.929   | 65,430 | 195        | 213s          |
| Exp B     | 22.84    | 54.40     | 0.917   | 68,076 | 114        | 3,353s         |

### Validation-Street Metrics (74 streets)

| Condition | Val MAE | Val RMSE | Val R² |
|-----------|---------|----------|--------|
| Baseline  | 32.06   | 78.40    | 0.823  |
| Exp A     | 24.18   | 53.16    | 0.918  |
| Exp B     | 24.68   | 54.53    | 0.914  |

### Published All-Street Metrics (for reference)

| Condition | Test MAE (all) | Test R² (all) |
|-----------|----------------|---------------|
| Baseline  | 5.54           | 0.890         |
| Exp B     | 8.13           | 0.895         |

---

## Questions Answered

### Q1: How good is the published model, really?

**Bad — on the streets that matter.**

The published test MAE of 5.54 drops to 30.72 when evaluated only on the 74 sensor
streets. That is a 5.5x gap. The model achieves its low headline number by accurately
predicting 1,323 imputed streets that have smooth, low-traffic, XGBoost-generated
patterns. On real high-traffic streets (Bourke, Swanston, Flinders), it is 5.5 times
less accurate than the published figure suggests.

R² tells a less dramatic but consistent story: 0.890 across all streets vs 0.815
on sensor streets only. The model explains 81.5% of variance on real streets —
reasonable, but far below the headline 89%.

### Q2: Can a GCN learn from 74 sensor streets alone?

**Yes — surprisingly well.**

Despite a crippled graph (35 of 74 nodes fully isolated, only 58 spatial edges),
Experiment A achieves test MAE 22.34 and R² 0.929. This is a 27% MAE improvement
and a +0.114 R² improvement over the baseline.

The explanation: pedestrian flow is primarily a temporal signal. Each street has a
strong daily rhythm (morning commute, lunch peak, evening quiet) that the GRU can
learn from the street's own 96-step input window and time-of-day features. For the
35 isolated nodes, the GCN branches degenerate to linear transforms (no neighbours
to aggregate), but the GRU carries the model. The graph structure adds precision
on the 39 connected nodes, but the temporal backbone is sufficient for high R².

### Q3: Does imputation provide useful graph context?

**No. If anything, it slightly hurts.**

Experiment B (full graph, masked loss) achieves test MAE 22.84 vs Experiment A's
22.34. The difference is small (0.50 MAE) but consistent across all metrics — MAE,
RMSE, and R² are all slightly worse in Exp B.

The 1,323 imputed streets participate in GCN message passing, sending their features
to sensor-street neighbours through graph convolution. But those features are
XGBoost-generated approximations (R² = 0.158 per-street median) — they introduce
patterns that don't match real pedestrian dynamics. The GCN propagates this noise
into sensor-node representations, and the model cannot learn to ignore it.

### Q4: Is loss masking better than grading everything?

**Decisively yes.**

Both Exp A and Exp B outperform the baseline by 27% and 26% respectively. The
baseline's loss function grades the model on all 1,397 streets equally. Since 1,323
of those streets have low, smooth imputed values, the gradient signal is dominated
by easy predictions. The model learns to be good at predicting imputed streets at
the expense of the 74 streets with real, noisy, high-variance sensor data.

Masking the loss to sensor streets — whether in a subgraph (A) or the full graph
(B) — forces the model to focus its capacity on the streets that have real ground
truth.

---

## Key Takeaways

### 1. Disaggregated evaluation is essential

A model that reports MAE 5.54 but actually achieves MAE 30.72 on its ground-truth
streets is misleading. When mixing real and synthetic data, metrics must be reported
separately for each data source. This applies broadly to any spatio-temporal model
trained on partially imputed networks.

### 2. Loss masking is a simple, high-impact methodological fix

Restricting the loss function to streets with real sensor data yields a 27% MAE
improvement with zero architectural changes. This is the single most impactful
change in the experiment — more effective than adding 1,323 graph neighbours (Exp B)
or having 13x more spatial edges.

### 3. Imputation does not justify itself empirically

The 74-node subgraph (Exp A) matches or outperforms the full 1,397-node graph (Exp B)
on every metric. The imputed streets do not provide useful spatial context through
GCN message passing. Low imputation quality (per-street R² = 0.158) means the
messages are noise, not signal.

### 4. Pedestrian flow is primarily temporal, not spatial

The GRU captures daily rhythms from each street's own history. Even with 35 of 74
nodes isolated (GCN branches reduced to linear transforms), Exp A achieves R² 0.929.
Spatial graph structure helps but is not the primary driver of predictive performance.

### 5. Training efficiency favours the subgraph

Exp A trained in 213 seconds. Exp B trained in 3,353 seconds — 16x slower for worse
results. Fewer nodes means smaller matrix operations per GCN layer and fewer windows
to sample.

---

## Implications for the Thesis

These results support a nuanced thesis argument:

1. **The GCN architecture is sound** — it achieves strong R² on sensor streets
   under all conditions, and spatial/semantic branches contribute meaningfully
   when edges exist.

2. **The imputation strategy needs qualification** — while XGBoost imputation
   expands the modelled network from 74 to 1,397 streets (necessary for
   city-wide scenario simulation), it does not improve prediction accuracy on
   streets with real sensors. The thesis should present imputation as enabling
   city-wide coverage, not as improving model quality.

3. **Loss masking should be adopted** as the default training protocol when
   mixing real and imputed data. This is a transferable methodological
   recommendation for the urban computing community.

4. **Future work** should explore: denser sensor deployment, higher-quality
   imputation (e.g., graph-based imputation that respects spatial structure),
   or attention mechanisms that let the model learn which neighbours to trust
   vs ignore.

---

## Experimental Details

- Script: `melbourne_pipeline/experiments/ablation_sensor_only.py`
- Results: `melbourne_pipeline/data/experiments/ablation_results.json`
- Run environment: Lightning Studio (GPU), 2026-06-02
- All experiments: seed=42, window=96, hidden=64, GRU 2 layers, dropout=0.1,
  lr=1e-3, patience=25, batch=8, 256 gradient steps/epoch
- Chronological split: train 0-10079, val 10080-12239, test 12240-14399
