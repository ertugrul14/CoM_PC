import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Update `#sp-step-2-content` visibility & structure
content = content.replace(
    '<div id="sp-step-2-content" style="display:none;">',
    '<div id="sp-step-2-content" style="display:none; flex-direction: column; height: 100%;">'
)

content = content.replace(
    "document.getElementById('sp-step-2-content').style.display = (step === 2) ? 'block' : 'none';",
    "document.getElementById('sp-step-2-content').style.display = (step === 2) ? 'flex' : 'none';"
)

content = content.replace(
    '<div id="sp-activity-chart-container" style="height: 140px; position: relative;">',
    '<div id="sp-activity-chart-container" style="height: 220px; position: relative;">'
)

content = content.replace(
    '<div style="font-size:13px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Activity Profile (24h)</div>',
    '<div style="font-size:15px;color:var(--text-dim);margin-bottom:12px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Activity Profile (24h)</div>'
)

# 2. Update sp-header text sizes
content = content.replace(
    "font-size: 20px; font-weight: 600; color: var(--text);",
    "font-size: 24px; font-weight: 600; color: var(--text);"
)
content = content.replace(
    "#sp-street-meta { font-size: 13px; color: var(--text-muted); letter-spacing: 0.3px; }",
    "#sp-street-meta { font-size: 15px; color: var(--text-muted); letter-spacing: 0.3px; }"
)

# 3. Update sp-sec
content = content.replace(
    "padding: 12px 16px; border-bottom: 1px solid var(--border);",
    "padding: 18px 20px; border-bottom: 1px solid var(--border);"
)
content = content.replace(
    "font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase;",
    "font-size: 15px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase;"
)

# 4. Update sp-arch-card
content = content.replace(
    ".sp-arch-card .name { font-size: 15px; font-weight: 600; color: var(--text); margin-bottom: 2px; }",
    ".sp-arch-card .name { font-size: 17px; font-weight: 600; color: var(--text); margin-bottom: 4px; }"
)

# 5. Update sp-chart / bars
old_chart = r"""  .sp-chart-title {
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

new_chart = r"""  .sp-chart-title {
    font-size: 14px; font-weight: 700; color: var(--text-muted); margin-bottom: 12px; text-transform: uppercase; letter-spacing: 1px;
  }
  .sp-chart-title .meta { text-transform: none; letter-spacing: 0; margin-left: 4px; }
  .sp-bar-row { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; }
  .sp-bar-lbl { width: 105px; flex-shrink: 0; font-size: 15px; color: var(--text-dim); }
  .sp-bar-track {
    flex: 1; height: 12px; border-radius: 6px; background: rgba(255,255,255,0.06);
  }
  .sp-bar-fill { display: block; height: 100%; border-radius: 6px; }
  .sp-bar-val { width: 50px; flex-shrink: 0; text-align: right; font-size: 15px; color: var(--text); font-variant-numeric: tabular-nums; }"""

content = content.replace(old_chart, new_chart)

# 6. Update sp-chips
old_chips = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 8px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 10px; border: 1px solid var(--border); border-radius: 12px;
    font-size: 13px; color: var(--text-dim); background: var(--surface);
  }"""

new_chips = r"""  .sp-chips { display: flex; flex-wrap: wrap; gap: 12px; }
  .sp-chip {
    display: inline-flex; align-items: center; gap: 8px;
    padding: 6px 14px; border: 1px solid var(--border); border-radius: 16px;
    font-size: 15px; color: var(--text-dim); background: var(--surface);
  }"""

content = content.replace(old_chips, new_chips)

# 7. Update wiz-run button
old_run = r"""  .wiz-run {
    width: 100%; padding: 15px; border-radius: 12px;
    font-family: 'Space Grotesk', system-ui; font-weight: 700;
    font-size: 15.5px; letter-spacing: 0.2px; border: none;"""

new_run = r"""  .wiz-run {
    width: 100%; padding: 20px; border-radius: 14px;
    font-family: 'Space Grotesk', system-ui; font-weight: 700;
    font-size: 18px; letter-spacing: 0.2px; border: none;"""

content = content.replace(old_run, new_run)

# 8. Update chart area inline padding
content = content.replace(
    '<div id="sp-chart-area" style="padding: 0 16px 16px 16px;">',
    '<div id="sp-chart-area" style="padding: 16px 20px 20px 20px;">'
)

html_path.write_text(content, encoding="utf-8")
print("CSS and inline styles updated to scale up panel components.")
