"""
Fix 1 PROTOTYPE (off to the side — does NOT touch frozen step_05).

Question: for streets where `spatial_lag_ped` is currently DEAD (0, no sensored
neighbour), can a smarter fallback give a usable signal?

Natural experiment: of 74 sensored streets, only 39 have a sensored immediate
neighbour — so 35 sensored streets have lag=0 TODAY, exactly like 88% of the
unsensored streets we impute. But these 35 have GROUND TRUTH. So we can score
fallback candidates on them with no GNN retrain.

Candidates for the dead-lag streets:
  current   : lag = 0                          (status quo)
  k2 / k3   : mean flow of sensored streets reachable in <=2 / <=3 graph hops
  featknn   : mean flow of the K nearest sensored streets in static-feature space
              (functionally similar streets, ignoring graph distance)
Reference levels:
  own_clim  : the street's own time-of-day climatology  (UPPER ceiling, needs history)
  glob_mean : city-wide average profile                 (what lag=0 ~ collapses toward)

A fallback is worth building into step_05 only if it beats glob_mean by a
meaningful margin on these dead-lag streets.
"""
import numpy as np
import pandas as pd
from pathlib import Path
from collections import deque

PROC = Path("melbourne_pipeline/data/processed")
K_FEAT = 5  # neighbours in feature space

# ── load ────────────────────────────────────────────────────────────────────
ni = pd.read_parquet(PROC / "node_index.parquet")
se = pd.read_parquet(PROC / "spatial_edges.parquet")
static = pd.read_parquet(PROC / "static_features.parquet")
static["street_id"] = static["street_id"].astype(str)
sid_to_n = dict(zip(ni["street_id"].astype(str), ni["node_idx"]))
n_to_sid = dict(zip(ni["node_idx"], ni["street_id"].astype(str)))

ped = pd.read_parquet(PROC / "ped_complete.parquet",
                      columns=["street_id", "time_bin", "ped_flow", "source", "ped_valid"])
ped["street_id"] = ped["street_id"].astype(str)
sens = ped[ped["source"] == "sensor"]
sensored = sorted(sens["street_id"].unique())
sset = set(sensored)

wide  = sens.pivot_table(index="time_bin", columns="street_id", values="ped_flow")
valid = sens.pivot_table(index="time_bin", columns="street_id", values="ped_valid").astype(bool)
tb = pd.to_datetime(wide.index)
hod = tb.hour * 4 + tb.minute // 15
glob = wide.mean(axis=1).to_numpy()

# ── undirected adjacency for multi-hop ───────────────────────────────────────
adj = {}
for _, r in se.iterrows():
    a, b = int(r["node_i"]), int(r["node_j"])
    adj.setdefault(a, set()).add(b)
    adj.setdefault(b, set()).add(a)

def sensored_within_hops(sid, max_hops):
    """sensored street_ids (excluding self) reachable within max_hops graph hops."""
    if sid not in sid_to_n:
        return []
    start = sid_to_n[sid]
    seen = {start}
    frontier = deque([(start, 0)])
    out = []
    while frontier:
        node, d = frontier.popleft()
        if d >= max_hops:
            continue
        for nb in adj.get(node, ()):
            if nb in seen:
                continue
            seen.add(nb)
            s = n_to_sid.get(nb)
            if s in sset and s != sid:
                out.append(s)
            frontier.append((nb, d + 1))
    return out

def immediate_sensored(sid):
    if sid not in sid_to_n:
        return []
    return [n_to_sid[x] for x in adj.get(sid_to_n[sid], set())
            if n_to_sid.get(x) in sset and n_to_sid.get(x) != sid]

# ── feature-space kNN among sensored streets ─────────────────────────────────
FEATS = ["total_jobs", "cafe_count", "business_count", "poi_total",
         "dining_capacity", "area_m2"]
S = static.set_index("street_id")
Z = S.loc[sensored, FEATS].astype(float)
mu, sd = Z.mean(), Z.std().replace(0, 1.0)
Zz = (Z - mu) / sd

def feat_knn(sid, k=K_FEAT):
    if sid not in Zz.index:
        return []
    d = np.linalg.norm(Zz.values - Zz.loc[sid].values, axis=1)
    order = np.argsort(d)
    out = [sensored[i] for i in order if sensored[i] != sid][:k]
    return out

def r2(y, yhat, m):
    y, yhat = y[m], yhat[m]
    ss = np.sum((y - y.mean()) ** 2)
    return 1 - np.sum((y - yhat) ** 2) / ss if ss > 0 else np.nan

def pred_from(streets, y_index_cols):
    if not streets:
        return None
    cols = [c for c in streets if c in y_index_cols]
    if not cols:
        return None
    return wide[cols].mean(axis=1).to_numpy()

# ── identify the dead-lag sensored streets (the natural experiment) ──────────
dead = [s for s in sensored if len(immediate_sensored(s)) == 0]
live = [s for s in sensored if len(immediate_sensored(s)) > 0]
print(f"sensored: {len(sensored)}  | live-lag: {len(live)}  | DEAD-lag: {len(dead)}")
print(f"(dead-lag sensored streets are the ground-truthed twins of the 88% "
      f"unsensored lag=0 streets)\n")

cols_set = set(wide.columns)
rows = []
for sid in dead:
    y = wide[sid].to_numpy()
    m = valid[sid].to_numpy() & np.isfinite(y)
    own = pd.Series(y).groupby(hod).transform("mean").to_numpy()
    rec = {"street_id": sid,
           "r2_glob": r2(y, glob, m),
           "r2_ownclim": r2(y, own, m)}
    for name, streets in [("k2", sensored_within_hops(sid, 2)),
                          ("k3", sensored_within_hops(sid, 3)),
                          ("featknn", feat_knn(sid))]:
        p = pred_from(streets, cols_set)
        rec[f"n_{name}"] = len(streets)
        rec[f"r2_{name}"] = r2(y, p, m) if p is not None else np.nan
    rows.append(rec)

res = pd.DataFrame(rows)
print("Median R2 on the DEAD-lag sensored streets (higher = better signal):")
print(f"  current (lag=0 ~ glob_mean) : {res.r2_glob.median():>7.3f}")
print(f"  k2  (<=2 hops)              : {res.r2_k2.median():>7.3f}   "
      f"(reaches a sensored street: {(res.n_k2>0).mean()*100:.0f}% of streets)")
print(f"  k3  (<=3 hops)              : {res.r2_k3.median():>7.3f}   "
      f"({(res.n_k3>0).mean()*100:.0f}%)")
print(f"  featknn (static-similar)    : {res.r2_featknn.median():>7.3f}   "
      f"({(res.n_featknn>0).mean()*100:.0f}%)")
print(f"  own_clim (ceiling, unavail) : {res.r2_ownclim.median():>7.3f}")

best = res[["r2_k2", "r2_k3", "r2_featknn"]].median().idxmax()
uplift = res[best].median() - res.r2_glob.median()
print(f"\nBest fallback: {best}  | median uplift over current: {uplift:+.3f}")
print("Decision rule: build Fix 1 into step_05 only if uplift is meaningfully > 0.")
res.to_csv("scripts/exp_spatial_lag_v4_result.csv", index=False)
print("saved -> scripts/exp_spatial_lag_v4_result.csv")
