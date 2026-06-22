import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

old_css = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 6px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 9px; border-radius: 999px;
    background: rgba(255,255,255,0.04); border: 1px solid var(--border);
    font-size: 11px; color: var(--text-dim);
  }
  .sp-chip b { color: var(--text); font-weight: 600; }
  .sp-chip .dot { width: 12px; height: 12px; border-radius: 50%; background: currentColor; }"""

new_css = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 12px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 8px;
    padding: 8px 16px; border-radius: 999px;
    background: rgba(255,255,255,0.04); border: 1px solid var(--border);
    font-size: 22px; color: var(--text-dim);
  }
  .sp-chip b { color: var(--text); font-weight: 600; }
  .sp-chip .dot { width: 14px; height: 14px; border-radius: 50%; background: currentColor; }"""

content = content.replace(old_css, new_css)

old_span = r"""text += ` <span style="font-size:20px; opacity:0.7; font-weight:normal;">(${c.val} ${unit})</span>`;"""
new_span = r"""text += ` <span style="opacity:0.7; font-weight:normal;">(${c.val} ${unit})</span>`;"""
content = content.replace(old_span, new_span)

html_path.write_text(content, encoding="utf-8")
print("Fixed chip font size inconsistency.")
