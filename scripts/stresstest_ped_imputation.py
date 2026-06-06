"""
Stress-test diagnostics for Step-05 XGBoost pedestrian imputation.

Read-only. Uses existing artefacts only (no retraining, no cube load).
Answers three senior-ML questions cheaply:
  1. Covariate shift: are the streets we IMPUTE (unsensored) drawn from the
     same distribution as the streets we TRAIN on (sensored)? If not, the
     GroupKFold CV R2 over sensored streets is an optimistic upper bound.
  2. Spatial-lag coverage: the model's strongest dynamic feature is the mean
     ped_flow of sensored spatial neighbours. How many unsensored streets
     actually have >=1 sensored neighbour? Streets with none get lag=0 — a
     feature value the model rarely saw in training.
  3. Output sanity: does the imputed ped_flow distribution (mean/peak per
     street) look plausible vs the sensored streets, or is it collapsed/clipped?
"""
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path

PROC = Path("melbourne_pipeline/data/processed")
STREETS_GEOJSON = None  # resolved below from config

import sys
sys.path.insert(0, "melbourne_pipeline")
from config import STREETS_GEOJSON as SG  # noqa
STREETS_GEOJSON = SG

ARTERIAL_TYPES = {"Arterial", "Council Major"}
CONF_FEATURES = ["total_jobs", "cafe_count", "business_count",
                 "poi_total", "dining_capacity", "area_m2"]

# ── Identify sensored vs unsensored (mirror step_05 logic) ──────────────────
sensor_map = pd.read_parquet(PROC / "sensor_map.parquet")
static     = pd.read_parquet(PROC / "static_features.parquet")
static["street_id"] = static["street_id"].astype(str)

gdf = gpd.read_file(STREETS_GEOJSON)[["street_id", "str_type", "name"]]
gdf["street_id"] = gdf["street_id"].astype(str)
keep = (gdf["str_type"].isin(ARTERIAL_TYPES) &
        ~gdf["name"].str.contains("Intersection", case=False, na=False))
arterial_ids = set(gdf.loc[keep, "street_id"])
static = static[static["street_id"].isin(arterial_ids)].reset_index(drop=True)

ped_sensors = sensor_map[sensor_map["sensor_type"] == "pedestrian"]
sensored = set(ped_sensors["street_id"].dropna().astype(str)) & arterial_ids
all_ids  = set(static["street_id"])
unsensored = sorted(all_ids - sensored)
sensored   = sorted(sensored & all_ids)

print("=" * 70)
print("STRESS TEST 1 — COVARIATE SHIFT (train=sensored vs deploy=unsensored)")
print("=" * 70)
print(f"sensored (train): {len(sensored)}   unsensored (impute): {len(unsensored)}")

S = static.set_index("street_id")
feats = [c for c in CONF_FEATURES if c in S.columns]
print(f"\n{'feature':<20} {'sens_mean':>12} {'unsens_mean':>12} {'std_diff':>10} {'overlap%':>9}")
print("-" * 70)
for f in feats:
    s_vals = S.loc[sensored, f].astype(float)
    u_vals = S.loc[unsensored, f].astype(float)
    pooled_sd = np.sqrt((s_vals.var() + u_vals.var()) / 2) or 1.0
    smd = (s_vals.mean() - u_vals.mean()) / pooled_sd          # standardized mean diff
    # overlap: fraction of unsensored within [p5, p95] of sensored
    lo, hi = s_vals.quantile(0.05), s_vals.quantile(0.95)
    overlap = ((u_vals >= lo) & (u_vals <= hi)).mean() * 100
    print(f"{f:<20} {s_vals.mean():>12.1f} {u_vals.mean():>12.1f} {smd:>10.2f} {overlap:>8.0f}%")
print("\n  |std_diff| > 0.8 = large shift (Cohen). overlap% = unsensored streets")
print("  falling inside the sensored 5-95 percentile band (in-distribution).")

# ── Spatial-lag coverage ────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("STRESS TEST 2 — SPATIAL-LAG COVERAGE (strongest dynamic feature)")
print("=" * 70)
ni = pd.read_parquet(PROC / "node_index.parquet")
se = pd.read_parquet(PROC / "spatial_edges.parquet")
sid_to_n = dict(zip(ni["street_id"].astype(str), ni["node_idx"]))
n_to_sid = dict(zip(ni["node_idx"], ni["street_id"].astype(str)))
sensored_set = set(sensored)
adj = {}
for _, r in se.iterrows():
    adj.setdefault(int(r["node_i"]), []).append(int(r["node_j"]))

def n_sensored_nbrs(sid):
    if sid not in sid_to_n:
        return None  # not in graph at all
    nbrs = adj.get(sid_to_n[sid], [])
    return sum(1 for x in nbrs if n_to_sid.get(x) in sensored_set)

cov = {s: n_sensored_nbrs(s) for s in unsensored}
not_in_graph = sum(1 for v in cov.values() if v is None)
zero_lag     = sum(1 for v in cov.values() if v == 0)
has_lag      = sum(1 for v in cov.values() if v and v > 0)
print(f"unsensored streets:           {len(unsensored)}")
print(f"  not in spatial graph:       {not_in_graph}  (lag forced to 0)")
print(f"  in graph, 0 sensored nbrs:  {zero_lag}  (lag = 0)")
print(f"  in graph, >=1 sensored nbr: {has_lag}  (lag is informative)")
print(f"  => {100*(not_in_graph+zero_lag)/len(unsensored):.0f}% of imputed streets get a"
      f" DEGENERATE lag=0 (a value rare among sensored training rows).")

# ── Imputed output sanity ───────────────────────────────────────────────────
print("\n" + "=" * 70)
print("STRESS TEST 3 — IMPUTED OUTPUT SANITY (ped_street_summary.json)")
print("=" * 70)
import json
summ = json.loads((PROC / "ped_street_summary.json").read_text())
df = pd.DataFrame(summ).T
df.index = df.index.astype(str)
df["mean_ped_flow"] = pd.to_numeric(df["mean_ped_flow"])
df["peak_ped_flow"] = pd.to_numeric(df["peak_ped_flow"])
for lab, ids in [("sensored", sensored), ("imputed", unsensored)]:
    sub = df.loc[df.index.intersection(ids)]
    print(f"\n{lab} ({len(sub)} streets):")
    print(f"  mean_ped_flow : median={sub.mean_ped_flow.median():.1f}  "
          f"p5={sub.mean_ped_flow.quantile(.05):.1f}  p95={sub.mean_ped_flow.quantile(.95):.1f}")
    print(f"  peak_ped_flow : median={sub.peak_ped_flow.median():.1f}  "
          f"max={sub.peak_ped_flow.max():.1f}")
    # collapse check: how many imputed streets are near-flat (peak ~ mean)?
    flat = ((sub.peak_ped_flow - sub.mean_ped_flow) < sub.mean_ped_flow * 0.5).mean() * 100
    print(f"  near-flat (peak < 1.5x mean): {flat:.0f}%")
print("\nDone.")
