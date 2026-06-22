import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Update sp-header, sp-close, sp-street-name, sp-street-meta
old_header = r"""  #sp-header {
    padding: 12px 14px 10px;
    border-bottom: 1px solid rgba(61,56,48,0.5);
    flex-shrink: 0; background: transparent;
    border-radius: 16px 16px 0 0;
  }
  #sp-close {
    float: right; background: none; border: none;
    color: var(--text-muted); font-size: 16px; cursor: pointer;
    padding: 0; line-height: 1; margin: 2px -2px 0 0;
  }
  #sp-close:hover { color: var(--text); }
  #sp-street-name {
    font-size: 17px; font-weight: 600; color: var(--text);
    margin-bottom: 4px; padding-right: 22px; line-height: 1.3;
  }
  #sp-street-meta { font-size: 11px; color: var(--text-muted); letter-spacing: 0.3px; }"""

new_header = r"""  #sp-header {
    display: flex; align-items: flex-start; gap: 12px;
    padding: 16px;
    border-bottom: 1px solid rgba(61,56,48,0.5);
    flex-shrink: 0; background: transparent;
    border-radius: 16px 16px 0 0;
  }
  #sp-close {
    background: none; border: none;
    color: var(--text-muted); font-size: 20px; cursor: pointer;
    padding: 0; line-height: 1; margin-top: 2px;
  }
  #sp-close:hover { color: var(--text); }
  #sp-street-name {
    font-size: 20px; font-weight: 600; color: var(--text);
    margin-bottom: 4px; padding-right: 0; line-height: 1.3;
  }
  #sp-street-meta { font-size: 13px; color: var(--text-muted); letter-spacing: 0.3px; }"""

content = content.replace(old_header, new_header)

# 2. Update sp-sec, sp-sec-title
old_sec = r"""  .sp-sec {
    padding: 16px; border-bottom: 1px solid var(--border);
  }
  .sp-sec-title {
    font-size: 10px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase;
    color: var(--text-muted); margin-bottom: 10px;
  }
  .sp-sec-title .meta { text-transform: none; font-weight: normal; font-size: 10px; letter-spacing: 0; float: right; margin-top: 1px; }"""

new_sec = r"""  .sp-sec {
    padding: 12px 16px; border-bottom: 1px solid var(--border);
  }
  .sp-sec-title {
    font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase;
    color: var(--text-muted); margin-bottom: 8px;
  }
  .sp-sec-title .meta { text-transform: none; font-weight: normal; font-size: 11px; letter-spacing: 0; float: right; margin-top: 1px; }"""

content = content.replace(old_sec, new_sec)

# 3. Update sp-arch-card
old_arch = r"""  .sp-arch-card .name { font-size: 13px; font-weight: 600; color: var(--text); margin-bottom: 2px; }"""
new_arch = r"""  .sp-arch-card .name { font-size: 15px; font-weight: 600; color: var(--text); margin-bottom: 2px; }"""
content = content.replace(old_arch, new_arch)

# 4. Update sp-chart-title, bar-row
old_chart = r"""  .sp-chart-title {
    font-size: 10px; font-weight: 700; color: var(--text-muted); margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px;
  }
  .sp-chart-title .meta { text-transform: none; letter-spacing: 0; margin-left: 4px; }
  .sp-bar-row { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
  .sp-bar-lbl { width: 66px; flex-shrink: 0; font-size: 10px; color: var(--text-dim); }
  .sp-bar-track {
    flex: 1; height: 6px; border-radius: 4px; background: rgba(255,255,255,0.06);
  }
  .sp-bar-fill { display: block; height: 100%; border-radius: 4px; }
  .sp-bar-val { width: 40px; flex-shrink: 0; text-align: right; font-size: 10px; color: var(--text); font-variant-numeric: tabular-nums; }"""

new_chart = r"""  .sp-chart-title {
    font-size: 12px; font-weight: 700; color: var(--text-muted); margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px;
  }
  .sp-chart-title .meta { text-transform: none; letter-spacing: 0; margin-left: 4px; }
  .sp-bar-row { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
  .sp-bar-lbl { width: 85px; flex-shrink: 0; font-size: 13px; color: var(--text-dim); }
  .sp-bar-track {
    flex: 1; height: 8px; border-radius: 4px; background: rgba(255,255,255,0.06);
  }
  .sp-bar-fill { display: block; height: 100%; border-radius: 4px; }
  .sp-bar-val { width: 45px; flex-shrink: 0; text-align: right; font-size: 13px; color: var(--text); font-variant-numeric: tabular-nums; }"""

content = content.replace(old_chart, new_chart)

# 5. Update sp-chips
old_chips = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 6px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 3px 8px; border: 1px solid var(--border); border-radius: 12px;
    font-size: 11px; color: var(--text-dim); background: var(--surface);
  }"""

new_chips = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 8px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 10px; border: 1px solid var(--border); border-radius: 12px;
    font-size: 13px; color: var(--text-dim); background: var(--surface);
  }"""

content = content.replace(old_chips, new_chips)

# 6. Update the inline Activity Profile title font size
old_act = r"""<div style="font-size:10px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Activity Profile (24h)</div>"""
new_act = r"""<div style="font-size:13px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Activity Profile (24h)</div>"""
content = content.replace(old_act, new_act)

html_path.write_text(content, encoding="utf-8")
print("CSS styles updated for panel text sizes and layout.")
