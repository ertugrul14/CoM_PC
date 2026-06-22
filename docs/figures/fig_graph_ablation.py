"""
Figure: graph-design ablation — sensor-only vs full-context graph.

The VALID comparison is the three left bars: same Category-A cube, identical masked
loss, only the graph differs. The final deployed model (right, separated) is on a
DIFFERENT cube (climatology) and is shown for reference only — not directly comparable.

Sources: decisions.md D-010/D-018/D-019/D-024 + ablation memory.
  ExpA (induced subgraph)   54.71   — degenerate (35/74 sensor nodes isolated)
  V2   (sensor-only graph)  27.16   — fresh graph on 189 real-sensor nodes, no imputation
  ExpB (full-context graph) 24.97   — 1,397 nodes, imputed streets as context  [kept]
  Final deployed model      28.35   — full-context design, climatology cube (diff. cube)

    python docs/figures/fig_graph_ablation.py  ->  docs/figures/fig_graph_ablation.png
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

labels = ["ExpA\ninduced subgraph", "V2\nsensor-only graph",
          "ExpB\nfull-context graph", "Final model\n(deployed)"]
mae    = [54.71, 27.16, 24.97, 28.35]
colors = ["#94a3b8", "#f59e0b", "#16a34a", "#2563eb"]
x = np.arange(len(labels))

fig, ax = plt.subplots(figsize=(10.5, 6), dpi=150)
bars = ax.bar(x, mae, 0.6, color=colors)
for r in bars:
    ax.annotate(f"{r.get_height():.2f}", (r.get_x() + r.get_width()/2, r.get_height()),
                ha="center", va="bottom", fontsize=10, fontweight="bold", color="#1e293b")

# Divider: the final model is on a different cube — keep it visually apart
ax.axvline(2.55, color="#94a3b8", ls="--", lw=1.3)
ax.text(1.0, 58, "Same cube (Category-A) — valid comparison",
        ha="center", fontsize=10, color="#334155", style="italic")
ax.text(3.0, 58, "different cube\n(reference only)",
        ha="center", fontsize=9, color="#94a3b8", style="italic")

# Annotate the key result: full-context beats sensor-only
ax.annotate("", xy=(2, 27.5), xytext=(1, 27.5),
            arrowprops=dict(arrowstyle="<->", color="#0f172a", lw=1.3))
ax.text(1.5, 31.5, "+2.2 MAE (~8%)\nunsensored streets as\ncontext HELP",
        ha="center", fontsize=9, color="#0f172a",
        bbox=dict(boxstyle="round,pad=0.35", fc="#dcfce7", ec="#16a34a", lw=1))

# Flag ExpA as degenerate
ax.text(0, 50, "degenerate\n(35/74 isolated)", ha="center", va="top",
        fontsize=8.5, color="#dc2626")

ax.set_ylabel("Sensor test MAE — pedestrians / 15-min  (lower is better)", fontsize=11)
ax.set_title("Graph design: sensor-only vs full-context (masked loss)",
             fontsize=12.5, fontweight="bold")
ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=9.5)
ax.set_ylim(0, 62)
ax.spines[["top", "right"]].set_visible(False)

fig.text(0.5, -0.03,
    "Valid comparison = the three left bars (same cube, same masked loss, only the graph differs): the full-context "
    "graph (ExpB 24.97) beats a fair sensor-only graph (V2 27.16); ExpA's 54.71 is a broken-graph artifact, not "
    "evidence.  The final deployed model (28.35) is on the climatology cube — a different run, shown for context only.",
    ha="center", fontsize=8.3, color="#475569", wrap=True)

out = Path(__file__).parent / "fig_graph_ablation.png"
fig.tight_layout()
fig.savefig(out, bbox_inches="tight", facecolor="white")
print(f"Saved: {out}")
