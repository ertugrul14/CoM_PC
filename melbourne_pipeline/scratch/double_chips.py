import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Double the sp-chip CSS font size and padding
old_chip_css = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 12px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 8px;
    padding: 6px 14px; border: 1px solid var(--border); border-radius: 16px;
    font-size: 15px; color: var(--text-dim); background: var(--surface);
  }"""

new_chip_css = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 16px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 12px;
    padding: 12px 24px; border: 1px solid var(--border); border-radius: 30px;
    font-size: 30px; color: var(--text-dim); background: var(--surface);
  }"""

content = content.replace(old_chip_css, new_chip_css)

# 2. Double the dot size inside sp-chip
old_dot = r"""  .sp-chip .dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; }"""
new_dot = r"""  .sp-chip .dot { width: 12px; height: 12px; border-radius: 50%; background: currentColor; }"""
content = content.replace(old_dot, new_dot)

# 3. Double the inline font size for the seats/bays/units
old_inline = r"""text += ` <span style="font-size:10px; opacity:0.7; font-weight:normal;">(${c.val} ${unit})</span>`;"""
new_inline = r"""text += ` <span style="font-size:20px; opacity:0.7; font-weight:normal;">(${c.val} ${unit})</span>`;"""
content = content.replace(old_inline, new_inline)

html_path.write_text(content, encoding="utf-8")
print("Doubled the font size and padding of the CLUE features (On This Street chips).")
