import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Remove HTML
html_old = r"""  <div id="taskbar-right">
    <button type="button" id="scenario-btn" onclick="onScenarioBtn()">Run Scenario</button>
  </div>"""
html_new = r"""  <div id="taskbar-right">
  </div>"""
content = content.replace(html_old, html_new)

# 2. Remove JS startScenario
js1_old = r"""      // Update trigger button
      const btn = document.getElementById('scenario-btn');
      btn.textContent = 'End Scenario';
      btn.classList.add('ending');"""
content = content.replace(js1_old, "")

# 3. Remove JS endScenario
js2_old = r"""      // Reset trigger button
      const btn = document.getElementById('scenario-btn');
      btn.textContent = 'Start Scenario';
      btn.classList.remove('ending');"""
content = content.replace(js2_old, "")

# 4. Remove JS click handler show
js3_old = r"""        // Show the scenario button for ANY clicked street (unless one is already running)
        if (!scenarioActive) {
          _scenarioProps = feat.properties;
          document.getElementById('scenario-btn').classList.add('visible');
        }"""
js3_new = r"""        if (!scenarioActive) {
          _scenarioProps = feat.properties;
        }"""
content = content.replace(js3_old, js3_new)

# 5. Remove JS empty click hide
js4_old = r"""      if (!scenarioActive) {
        _scenarioProps = null;
        document.getElementById('scenario-btn').classList.remove('visible');
      }"""
js4_new = r"""      if (!scenarioActive) {
        _scenarioProps = null;
      }"""
content = content.replace(js4_old, js4_new)

html_path.write_text(content, encoding="utf-8")
print("Removed top right scenario button and its JS references")
