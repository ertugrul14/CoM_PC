import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

old_css = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 12px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 8px;
    padding: 8px 16px; border-radius: 999px;
    background: rgba(255,255,255,0.04); border: 1px solid var(--border);
    font-size: 22px; color: var(--text-dim);
  }"""

new_css = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 8px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 5px 12px; border-radius: 999px;
    background: rgba(255,255,255,0.04); border: 1px solid var(--border);
    font-size: 14px; color: var(--text-dim);
  }"""

content = content.replace(old_css, new_css)
html_path.write_text(content, encoding="utf-8")
print("Changed chip font size to 14px.")
