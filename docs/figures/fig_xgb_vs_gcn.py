"""
Figure: XGBoost vs GCN — why we kept only the GCN.

Source data: the Fix 5 leave-streets-out test (D-023). Each fold hides M of the 74
real ped sensors, then predicts them three ways and scores against the REAL flow:
  - XGBoost only       : the standalone imputer (cold prediction from features)
  - GCN + XGBoost fill : the GNN with held-out nodes filled by XGBoost
  - GCN alone          : the GNN with a trivial city-climatology fill (no XGBoost)
Metric: test MAE in pedestrians / 15-min (lower is better).

    python docs/figures/fig_xgb_vs_gcn.py   ->  docs/figures/fig_xgb_vs_gcn.png
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

groups   = ["Fold 0", "Fold 1", "Fold 2", "Mean"]
xgb_only = [61.46, 86.93, 73.21, 73.87]   # standalone XGBoost imputer
gcn_xgb  = [44.14, 89.92, 59.33, 64.46]   # GCN with XGBoost fill   (Arm A)
gcn_only = [45.79, 91.34, 56.62, 64.58]   # GCN alone, trivial fill (Arm B)

C_XGB, C_GX, C_GO = "#94a3b8", "#2563eb", "#16a34a"
x = np.arange(len(groups)); w = 0.26

fig, ax = plt.subplots(figsize=(10.5, 6), dpi=150)
b1 = ax.bar(x - w, xgb_only, w, label="XGBoost only", color=C_XGB)
b2 = ax.bar(x,      gcn_xgb,  w, label="GCN + XGBoost fill", color=C_GX)
b3 = ax.bar(x + w,  gcn_only, w, label="GCN alone (no XGBoost)", color=C_GO)

for bars in (b1, b2, b3):
    for r in bars:
        ax.annotate(f"{r.get_height():.1f}", (r.get_x() + r.get_width()/2, r.get_height()),
                    ha="center", va="bottom", fontsize=8, color="#334155")

# Separate the summary "Mean" group visually
ax.axvline(2.55, color="#cbd5e1", ls="--", lw=1)
ax.text(3.0, ax.get_ylim()[1]*0.02, "summary", ha="center", fontsize=8, color="#64748b")

# Headline annotation: the decisive tie at the Mean group
ax.annotate("GCN ± XGBoost: Δ 0.12 MAE\n(within fold noise → XGBoost adds nothing)",
            xy=(3.0, 64.5), xytext=(2.0, 30),
            fontsize=9.5, color="#0f172a", ha="center",
            bbox=dict(boxstyle="round,pad=0.4", fc="#fef9c3", ec="#eab308", lw=1),
            arrowprops=dict(arrowstyle="->", color="#64748b"))

ax.set_ylabel("Test MAE — pedestrians / 15-min   (lower is better)", fontsize=11)
ax.set_title("Does the GCN need XGBoost?  Leave-streets-out test on held-out real sensors",
             fontsize=12.5, fontweight="bold")
ax.set_xticks(x); ax.set_xticklabels(groups, fontsize=10)
ax.legend(frameon=False, fontsize=10, loc="upper left")
ax.spines[["top", "right"]].set_visible(False)
ax.set_ylim(0, 100)

fig.text(0.5, -0.02,
    "Verdict: the GCN with XGBoost (64.5) ties the GCN alone (64.6) — so XGBoost adds nothing the GCN can't do itself. "
    "The GCN alone also beats standalone XGBoost imputation (64.6 vs 73.9).  "
    "→ Drop XGBoost; keep one model.   (GCN R² 0.45 either way.)",
    ha="center", fontsize=8.5, color="#475569", wrap=True)

out = Path(__file__).parent / "fig_xgb_vs_gcn.png"
fig.tight_layout()
fig.savefig(out, bbox_inches="tight", facecolor="white")
print(f"Saved: {out}")
