import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Fix the chart container id in HTML
old_chart_html = r"""    <div id="sp-chart-area" style="padding: 0 16px 16px 16px;">
      <div style="font-size:10px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Activity Profile (24h)</div>
      <div style="height: 140px; position: relative;">
        <canvas id="activity-chart"></canvas>
      </div>
    </div>"""

new_chart_html = r"""    <div id="sp-chart-area" style="padding: 0 16px 16px 16px;">
      <div style="font-size:10px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Activity Profile (24h)</div>
      <div id="sp-activity-chart-container" style="height: 140px; position: relative;">
        <canvas id="activity-chart"></canvas>
      </div>
    </div>"""

content = content.replace(old_chart_html, new_chart_html)

# 2. Fix openStreetPanel JS
old_js_container = "const container = document.getElementById('bcp-chart-container');"
new_js_container = "const container = document.getElementById('sp-activity-chart-container');"
content = content.replace(old_js_container, new_js_container)

# 3. Fix map click handler
old_click_handler = r"""      if (feat) {
        sensorTip.style.display = 'none';
        openStreetPanel(feat.properties);
        // Show the scenario button for ANY clicked street (unless one is already running)
        if (!scenarioActive) {
          _scenarioProps = feat.properties;
          document.getElementById('scenario-btn').classList.add('visible');
        }
        return;
      }"""

new_click_handler = r"""      if (feat) {
        sensorTip.style.display = 'none';
        openStreetPanel(feat.properties);
        if (window.goToStep) window.goToStep(2);
        document.getElementById('sp-header').style.display = 'flex';
        // Show the scenario button for ANY clicked street (unless one is already running)
        if (!scenarioActive) {
          _scenarioProps = feat.properties;
          document.getElementById('scenario-btn').classList.add('visible');
        }
        return;
      }"""
content = content.replace(old_click_handler, new_click_handler)

# 4. Remove the leftover bcp-chart-container at the bottom
leftover_bcp = r"""  <div id="bcp-chart-container">
    <canvas id="activity-chart"></canvas>
  </div>"""
content = content.replace(leftover_bcp, "")

html_path.write_text(content, encoding="utf-8")
print("Fixes applied successfully.")
