"""
Figure: MultiGCN training curve — validation R² over epochs (R²-only view).

Reads the per-epoch history stored in data/models/model_eval.json (Exp B run).
Shows validation R² for the pedestrian and parking heads (higher better) and marks
the best (installed) epoch.

    python docs/figures/fig_training_curve.py  ->  docs/figures/fig_training_curve.png
"""
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
e = json.loads((ROOT / "melbourne_pipeline" / "data" / "models" / "model_eval.json").read_text())
h = e["history"]
ep  = [r["epoch"] for r in h]
pr2 = [r["val_ped_r2"] for r in h]
kr2 = [r["val_park_r2"] for r in h]
best = e["best_epoch"]
bi   = ep.index(best)

fig, ax = plt.subplots(figsize=(10.5, 6), dpi=150)

ax.plot(ep, pr2, color="#2563eb", lw=2.2, label="val pedestrian R²")
ax.plot(ep, kr2, color="#16a34a", lw=2.0, ls="--", label="val parking R²")

ax.set_xlabel("Epoch", fontsize=11)
ax.set_ylabel("Validation R²  (higher is better)", fontsize=11)
ax.set_ylim(0.5, 0.95)
ax.set_xlim(0, ep[-1] + 2)

# Best-epoch marker
ax.axvline(best, color="#64748b", ls=":", lw=1.4)
ax.annotate(f"best epoch {best}\nped R² {pr2[bi]:.3f} · park R² {kr2[bi]:.3f}",
            xy=(best, pr2[bi]), xytext=(best - 60, 0.66),
            fontsize=9.5, color="#0f172a",
            bbox=dict(boxstyle="round,pad=0.4", fc="#fef9c3", ec="#eab308", lw=1),
            arrowprops=dict(arrowstyle="->", color="#64748b"))

ax.set_title("MultiGCN training — validation R² over epochs (Exp B, sensor-masked loss)",
             fontsize=12.5, fontweight="bold")
ax.legend(loc="lower right", frameon=False, fontsize=10.5)
ax.spines[["top", "right"]].set_visible(False)
ax.grid(axis="y", color="#e2e8f0", lw=0.7)

fig.text(0.5, -0.01,
    f"Both heads converge to R² ~0.89; early-stopped at epoch {ep[-1]}. "
    "Metrics on the 74 real pedestrian sensors / 143 parking sensors.",
    ha="center", fontsize=8.5, color="#475569")

out = Path(__file__).parent / "fig_training_curve.png"
fig.tight_layout()
fig.savefig(out, bbox_inches="tight", facecolor="white")
print(f"Saved: {out}")
