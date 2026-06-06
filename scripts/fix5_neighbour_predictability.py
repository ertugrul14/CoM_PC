"""
Fix 5 (cheap proxy) — Does the spatial-neighbour signal carry real predictive
information? Run LOCALLY, no GPU, no GNN retrain.

Logic: the whole premise of (a) the spatial_lag feature and (b) the GNN's
message passing is that a street's pedestrian flow can be predicted from its
neighbours' flow. We can test that premise directly on the 74 SENSORED
streets, because there we have BOTH the street's true flow AND its neighbours'
true flow.

For each sensored street with >=1 sensored neighbour:
  predictor = mean ped_flow of its sensored spatial neighbours at each time_bin
  target    = the street's own true ped_flow
  -> report R2 and correlation.

Compare against two trivial baselines per street:
  - global-mean   : predict the all-street mean flow at each time_bin
  - own-mean      : predict the street's own time-of-day climatology
If neighbour-R2 >> baselines, neighbour context is genuinely informative and
the imputation-as-context story (Role A) holds. If it ties the baselines,
the spatial story is weak and XGBoost/GNN are mostly learning climatology.
"""
import numpy as np
import pandas as pd
from pathlib import Path

PROC = Path("melbourne_pipeline/data/processed")

# ── sensored streets + their sensored spatial neighbours ────────────────────
ni = pd.read_parquet(PROC / "node_index.parquet")
se = pd.read_parquet(PROC / "spatial_edges.parquet")
sid_to_n = dict(zip(ni["street_id"].astype(str), ni["node_idx"]))
n_to_sid = dict(zip(ni["node_idx"], ni["street_id"].astype(str)))

# Read only sensored rows from ped_complete (source == 'sensor')
ped = pd.read_parquet(PROC / "ped_complete.parquet",
                      columns=["street_id", "time_bin", "ped_flow", "source", "ped_valid"])
ped["street_id"] = ped["street_id"].astype(str)
sens = ped[ped["source"] == "sensor"].copy()
sensored = sorted(sens["street_id"].unique())
print(f"sensored streets: {len(sensored)}")

# wide matrix: time_bin x street  (only sensored)
wide = sens.pivot_table(index="time_bin", columns="street_id", values="ped_flow")
valid = sens.pivot_table(index="time_bin", columns="street_id", values="ped_valid").astype(bool)

adj = {}
for _, r in se.iterrows():
    adj.setdefault(int(r["node_i"]), []).append(int(r["node_j"]))
sset = set(sensored)

def sensored_nbrs(sid):
    if sid not in sid_to_n:
        return []
    return [n_to_sid[x] for x in adj.get(sid_to_n[sid], [])
            if n_to_sid.get(x) in sset and n_to_sid.get(x) != sid]

def r2(y, yhat, m):
    y, yhat = y[m], yhat[m]
    ss_res = np.sum((y - yhat) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    return 1 - ss_res / ss_tot if ss_tot > 0 else np.nan

global_mean_profile = wide.mean(axis=1).to_numpy()  # mean across streets per bin

rows = []
for sid in sensored:
    nbrs = sensored_nbrs(sid)
    if not nbrs:
        continue
    y = wide[sid].to_numpy()
    m = valid[sid].to_numpy() & np.isfinite(y)
    nbr_mean = wide[nbrs].mean(axis=1).to_numpy()       # neighbour predictor
    # own time-of-day climatology (hour-of-day mean) as a baseline
    tb = pd.to_datetime(wide.index)
    hod = tb.hour * 4 + tb.minute // 15
    own_clim = pd.Series(y).groupby(hod).transform("mean").to_numpy()
    rows.append({
        "street_id": sid,
        "n_nbrs": len(nbrs),
        "r2_neighbour": r2(y, nbr_mean, m),
        "r2_globalmean": r2(y, global_mean_profile, m),
        "r2_ownclim": r2(y, own_clim, m),
        "corr_neighbour": np.corrcoef(y[m], nbr_mean[m])[0, 1],
    })

res = pd.DataFrame(rows)
print(f"\nsensored streets with >=1 sensored neighbour: {len(res)}")
print("\n              median     p25     p75")
for col in ["r2_neighbour", "r2_ownclim", "r2_globalmean", "corr_neighbour"]:
    print(f"{col:<16} {res[col].median():>7.3f} {res[col].quantile(.25):>7.3f} {res[col].quantile(.75):>7.3f}")

print("\nInterpretation:")
print("  r2_neighbour  = predict a street from its sensored neighbours' mean")
print("  r2_ownclim    = predict it from its own time-of-day average (no graph)")
print("  r2_globalmean = predict it from the city-wide average profile")
better = (res["r2_neighbour"] > res["r2_ownclim"]).mean() * 100
print(f"\n  neighbour beats own-climatology on {better:.0f}% of streets")
print(f"  median uplift (neighbour - ownclim R2): "
      f"{(res['r2_neighbour'] - res['r2_ownclim']).median():+.3f}")
res.to_csv("scripts/fix5_neighbour_predictability_result.csv", index=False)
print("\nsaved -> scripts/fix5_neighbour_predictability_result.csv")
