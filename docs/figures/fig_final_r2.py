"""
Figure: final MultiGCN R² — validation vs test, both heads.

Reads data/models/model_eval.json (the deployed Exp B model). R² on the real sensor
streets (74 ped / 143 park). Shows val vs test side by side to make the generalisation
(val ~= test) visible.

    python docs/figures/fig_final_r2.py  ->  docs/figures/fig_final_r2.png
"""
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
e = json.loads((ROOT / "melbourne_pipeline" / "data" / "models" / "model_eval.json").read_text())

heads = ["Pedestrian\n(74 sensors)", "Parking\n(143 sensors)"]
val   = [e["val_ped_r2"],  e["val_park_r2"]]
test  = [e["test_ped_r2"], e["test_park_r2"]]
x = np.arange(len(heads)); w = 0.32

fig, ax = plt.subplots(figsize=(8.5, 6), dpi=150)
b1 = ax.bar(x - w/2, val,  w, label="Validation", color="#2563eb")
b2 = ax.bar(x + w/2, test, w, label="Test (held-out future)", color="#16a34a")

for bars in (b1, b2):
    for r in bars:
        ax.annotate(f"{r.get_height():.3f}", (r.get_x() + r.get_width()/2, r.get_height()),
                    ha="center", va="bottom", fontsize=11, fontweight="bold", color="#1e293b")

ax.set_ylabel("R²  (higher is better)", fontsize=11)
ax.set_title("Final MultiGCN — variance explained on real sensors (R²)",
             fontsize=12.5, fontweight="bold")
ax.set_xticks(x); ax.set_xticklabels(heads, fontsize=10.5)
ax.set_ylim(0, 1.0)
ax.axhline(1.0, color="#cbd5e1", lw=0.8, ls="--")
ax.legend(frameon=False, fontsize=10.5, loc="lower center", ncol=2)
ax.spines[["top", "right"]].set_visible(False)

fig.text(0.5, -0.02,
    "Both heads explain ~88% of variance on the streets where ground truth exists. "
    "Validation ~= Test -> the model generalises to held-out future time, no overfitting.",
    ha="center", fontsize=8.6, color="#475569")

out = Path(__file__).parent / "fig_final_r2.png"
fig.tight_layout()
fig.savefig(out, bbox_inches="tight", facecolor="white")
print(f"Saved: {out}")
