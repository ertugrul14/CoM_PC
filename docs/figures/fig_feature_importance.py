"""
Figure: Step 10 feature interpretation — permutation importance (sensor-gated).

Source: melbourne_pipeline/data/models/feature_importance.json (D-024).
Each feature is shuffled over the held-out test windows (n_repeats=10) and the rise
in ped MAE on the 74 real ped-sensor streets is recorded. Baseline sensor MAE = 27.08.

ped_flow (+71.6) dwarfs everything, so it gets its own panel; the right panel zooms
into the remaining 22 features where the land-use / temporal structure is visible.
Colour = feature family. Bars left of zero are the noise floor (unpaired sampling),
not real negative signal.

    python docs/figures/fig_feature_importance.py  ->  docs/figures/fig_feature_importance.png
"""
from pathlib import Path
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

SRC = Path(__file__).resolve().parents[2] / "melbourne_pipeline" / "data" / "models" / "feature_importance.json"
with open(SRC) as f:
    data = json.load(f)

baseline = data["baseline_mae"]
imp = data["feature_importance"]

# Feature family -> colour
FAMILY = {
    "ped_flow": "autoregressive",
    "hour_sin": "temporal", "hour_cos": "temporal", "dow_sin": "temporal",
    "dow_cos": "temporal", "is_weekend": "temporal", "is_school_holiday": "temporal",
    "is_public_holiday": "temporal",
    "cafe_count": "land-use", "cafe_total_seats": "land-use", "bar_count": "land-use",
    "bar_patron_capacity": "land-use", "business_count": "land-use", "poi_total": "land-use",
    "dining_capacity": "land-use", "total_jobs": "land-use",
    "temperature_2m": "weather", "relative_humidity_2m": "weather",
    "wind_speed_10m": "weather", "precipitation": "weather",
    "area_m2": "structural", "occupancy_rate": "structural", "ped_confidence": "structural",
}
COLOR = {
    "autoregressive": "#dc2626", "temporal": "#2563eb", "land-use": "#16a34a",
    "weather": "#f59e0b", "structural": "#7c3aed",
}

# Sort descending by delta MAE
items = sorted(imp.items(), key=lambda kv: kv[1], reverse=True)
names = [k for k, _ in items]
vals = [v for _, v in items]
cols = [COLOR[FAMILY[k]] for k in names]

fig, (axL, axR) = plt.subplots(
    1, 2, figsize=(13, 7), dpi=150, gridspec_kw={"width_ratios": [1, 2.4]}
)

# ---- Left panel: the autoregressive giant vs the field --------------------
axL.bar([0], [vals[0]], 0.55, color=cols[0])
axL.bar([1], [vals[1]], 0.55, color=cols[1])
axL.annotate(f"+{vals[0]:.1f}", (0, vals[0]), ha="center", va="bottom",
             fontsize=12, fontweight="bold", color="#1e293b")
axL.annotate(f"+{vals[1]:.1f}", (1, vals[1]), ha="center", va="bottom",
             fontsize=11, color="#1e293b")
axL.set_xticks([0, 1])
axL.set_xticklabels([f"{names[0]}\n(autoregressive)", f"{names[1]}\n(next best)"], fontsize=10)
axL.set_ylabel("ΔMAE when feature shuffled  (pedestrians / 15-min)", fontsize=11)
axL.set_title("The autoregressive giant", fontsize=12, fontweight="bold")
axL.set_ylim(0, 80)
axL.spines[["top", "right"]].set_visible(False)
axL.annotate("≈11× the next\nstrongest feature", xy=(0.5, 40), ha="center",
             fontsize=9.5, color="#0f172a", style="italic")

# ---- Right panel: zoom into the remaining 22 features ---------------------
rest_names = names[1:]
rest_vals = vals[1:]
rest_cols = cols[1:]
y = np.arange(len(rest_names))[::-1]   # highest on top
axR.barh(y, rest_vals, 0.7, color=rest_cols)
axR.axvline(0, color="#475569", lw=1)
for yi, v in zip(y, rest_vals):
    off = 0.08 if v >= 0 else -0.08
    axR.annotate(f"{v:+.2f}", (v + off, yi), va="center",
                 ha="left" if v >= 0 else "right", fontsize=8, color="#334155")
axR.set_yticks(y)
axR.set_yticklabels(rest_names, fontsize=9)
axR.set_xlabel("ΔMAE when feature shuffled  (zoom — ped_flow excluded)", fontsize=11)
axR.set_title("Land-use sets the level, temporal drives the variation", fontsize=12, fontweight="bold")
axR.set_xlim(-4, 8)
axR.spines[["top", "right"]].set_visible(False)
axR.text(-3.8, len(rest_names) - 1.5, "noise floor\n(unpaired sampling)", fontsize=8,
         color="#94a3b8", style="italic")

# Shared family legend
handles = [mpatches.Patch(color=c, label=fam) for fam, c in COLOR.items()]
axR.legend(handles=handles, frameon=False, fontsize=9, loc="lower right",
           title="feature family", title_fontsize=9)

fig.suptitle(
    f"Step 10 — Permutation importance on the 74 real ped sensors  (baseline MAE = {baseline:.2f})",
    fontsize=13.5, fontweight="bold", y=0.99,
)
fig.text(0.5, -0.02,
    "ped_flow dominates (+71.6) → foot traffic is primarily autoregressive/temporal: the GRU predicts the next bin "
    "mostly from recent history. Land-use features (cafés, bars, POIs, jobs) add a few MAE each — they set each "
    "street's level, not its minute-to-minute shape. Bars left of zero (wind, is_weekend, is_school_holiday) are the "
    "sampling noise floor, not protective signal.",
    ha="center", fontsize=8.4, color="#475569", wrap=True)

out = Path(__file__).parent / "fig_feature_importance.png"
fig.tight_layout(rect=[0, 0, 1, 0.97])
fig.savefig(out, bbox_inches="tight", facecolor="white")
print(f"Saved: {out}")
