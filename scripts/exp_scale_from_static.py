"""
Fix 2 PROTOTYPE (off to the side — does NOT touch frozen step_05).

The Fix-5 / Fix-1 results showed: neighbour/temporal signals give SHAPE but not
SCALE. The per-street SCALE (level of foot traffic) can only come from static
features. This script tests the premise directly:

    Can static features predict a street's traffic LEVEL?

Target options (per sensored street, leave-one-out):
    log mean ped_flow   (the per-street scale / intercept)
    log peak ped_flow   (busy-hour magnitude)
Predictors: static features only (jobs, cafes, dining capacity, area, ...).
Models: leave-one-street-out for both a linear baseline and XGBoost.

If R2 is high -> Fix 2 (predict scale from statics) is the real lever and worth
an OVERRIDE. If low -> imputation is fundamentally limited and we report that
honestly rather than over-engineering.
"""
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.model_selection import LeaveOneOut
import xgboost as xgb

PROC = Path("melbourne_pipeline/data/processed")

ped = pd.read_parquet(PROC / "ped_complete.parquet",
                      columns=["street_id", "ped_flow", "source", "ped_valid"])
ped["street_id"] = ped["street_id"].astype(str)
sens = ped[(ped["source"] == "sensor") & (ped["ped_valid"])]
agg = sens.groupby("street_id")["ped_flow"].agg(mean="mean", peak="max").reset_index()

static = pd.read_parquet(PROC / "static_features.parquet")
static["street_id"] = static["street_id"].astype(str)
FEATS = [c for c in ["total_jobs", "cafe_count", "cafe_total_seats", "bar_count",
                     "bar_patron_capacity", "business_count", "poi_total",
                     "dining_capacity", "area_m2"] if c in static.columns]

df = agg.merge(static[["street_id"] + FEATS], on="street_id", how="inner")
X = df[FEATS].astype(float).values
print(f"sensored streets with static features: {len(df)}  | features: {len(FEATS)}")

def loo_r2(X, y, model_fn):
    preds = np.zeros_like(y, dtype=float)
    loo = LeaveOneOut()
    for tr, te in loo.split(X):
        m = model_fn()
        m.fit(X[tr], y[tr])
        preds[te] = m.predict(X[te])
    return r2_score(y, preds), preds

def lin():
    return LinearRegression()

def xgbm():
    return xgb.XGBRegressor(n_estimators=200, max_depth=3, learning_rate=0.05,
                            subsample=0.9, colsample_bytree=0.9, min_child_weight=2,
                            tree_method="hist", random_state=42, verbosity=0)

print("\nLeave-one-street-out R2 (predicting a street's LEVEL from static features):")
print(f"{'target':<14}{'linear':>10}{'xgboost':>10}")
for tgt in ["mean", "peak"]:
    y = np.log1p(df[tgt].values)
    r2_lin, _ = loo_r2(X, y, lin)
    r2_xgb, _ = loo_r2(X, y, xgbm)
    print(f"log_{tgt:<10}{r2_lin:>10.3f}{r2_xgb:>10.3f}")

# Feature importance for the mean-level model (which statics drive scale?)
y = np.log1p(df["mean"].values)
m = xgbm(); m.fit(X, y)
imp = sorted(zip(FEATS, m.feature_importances_), key=lambda t: -t[1])
print("\nTop static drivers of per-street mean level (XGBoost gain):")
for f, v in imp[:6]:
    print(f"  {f:<22}{v:.3f}")

print("\nInterpretation:")
print("  High R2 => per-street scale IS learnable from statics => Fix 2 is the")
print("  real lever (predict scale from statics, combine with neighbour SHAPE).")
print("  Low R2  => imputation level is fundamentally hard; report honestly.")
