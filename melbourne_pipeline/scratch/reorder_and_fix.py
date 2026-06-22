import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Reorder the Step 2 HTML so sp-chart-area is above sp-body
old_step_2_html = r"""  <!-- STEP 2 CONTAINER: Analyze data -->
  <div id="sp-step-2-content" style="display:none;">
    <div id="sp-body"></div>
    <div id="sp-chart-area" style="padding: 0 16px 16px 16px;">
      <div style="font-size:10px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Activity Profile (24h)</div>
      <div id="sp-activity-chart-container" style="height: 140px; position: relative;">
        <canvas id="activity-chart"></canvas>
      </div>
    </div>
    <div style="padding: 16px;">
      <button type="button" class="wiz-run" id="btn-go-step-3" onclick="goToStep(3)">Configure Intervention</button>
    </div>
  </div>"""

new_step_2_html = r"""  <!-- STEP 2 CONTAINER: Analyze data -->
  <div id="sp-step-2-content" style="display:none;">
    <div id="sp-chart-area" style="padding: 0 16px 16px 16px;">
      <div style="font-size:10px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Activity Profile (24h)</div>
      <div id="sp-activity-chart-container" style="height: 140px; position: relative;">
        <canvas id="activity-chart"></canvas>
      </div>
    </div>
    <div id="sp-body"></div>
    <div style="padding: 16px;">
      <button type="button" class="wiz-run" id="btn-go-step-3" onclick="goToStep(3)">Configure Intervention</button>
    </div>
  </div>"""

content = content.replace(old_step_2_html, new_step_2_html)


# 2. Swap the order of generation inside openStreetPanel
split_1 = r"      // ── 3. Land use chips ────────────────────────────────────────"
replace_1 = r"""      let htmlCluster = html;
      html = '';
      // ── 3. Land use chips ────────────────────────────────────────"""

content = content.replace(split_1, replace_1)

split_2 = r"""      html += `</div>`;

      // ── 6. Scenario explanation"""

replace_2 = r"""      html += `</div>`;
      let htmlClue = html;
      html = htmlClue + htmlCluster;

      // ── 6. Scenario explanation"""

content = content.replace(split_2, replace_2)

# 3. Remove the inline styles that force the panel to stay open on load
old_init = r"""// Ensure panel is visible on load at step 1
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('street-panel').style.transform = 'translateX(0)';
    document.getElementById('street-panel').style.opacity = '1';
    document.getElementById('sp-header').style.display = 'none';
    goToStep(1);
});"""

new_init = r"""// Ensure panel is visible on load at step 1
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('sp-header').style.display = 'none';
    goToStep(1);
});"""

content = content.replace(old_init, new_init)

html_path.write_text(content, encoding="utf-8")
print("Reordered elements and fixed panel visibility logic.")
