import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Update progress bar
old_progress_bar = r"""<!-- ── Scenario Progress Bar ────────────────────────────────────────── -->
<div id="scenario-progress" class="scenario-progress">
  <div class="sp-step active" id="sp-step-1">
    <div class="sp-node"><div class="sp-icon">1</div></div>
    <div class="sp-label">Pick a street</div>
  </div>
  <div class="sp-line" id="sp-line-1"></div>
  <div class="sp-step" id="sp-step-2">
    <div class="sp-node"><div class="sp-icon">2</div></div>
    <div class="sp-label">Configure</div>
  </div>
  <div class="sp-line" id="sp-line-2"></div>
  <div class="sp-step" id="sp-step-3">
    <div class="sp-node"><div class="sp-icon">3</div></div>
    <div class="sp-label">Results</div>
  </div>
</div>"""

new_progress_bar = r"""<!-- ── Scenario Progress Bar ────────────────────────────────────────── -->
<div id="scenario-progress" class="scenario-progress">
  <div class="sp-step active" id="sp-step-1">
    <div class="sp-node"><div class="sp-icon">1</div></div>
    <div class="sp-label">Pick a street</div>
  </div>
  <div class="sp-line" id="sp-line-1"></div>
  <div class="sp-step" id="sp-step-2">
    <div class="sp-node"><div class="sp-icon">2</div></div>
    <div class="sp-label">Analyze data</div>
  </div>
  <div class="sp-line" id="sp-line-2"></div>
  <div class="sp-step" id="sp-step-3">
    <div class="sp-node"><div class="sp-icon">3</div></div>
    <div class="sp-label">Pick intervention</div>
  </div>
  <div class="sp-line" id="sp-line-3"></div>
  <div class="sp-step" id="sp-step-4">
    <div class="sp-node"><div class="sp-icon">4</div></div>
    <div class="sp-label">Scenario</div>
  </div>
</div>"""

content = content.replace(old_progress_bar, new_progress_bar)

# 2. Re-structure street-panel steps
old_step_1_2 = r"""  <!-- STEP 1 CONTAINER: Understand Street -->
  <div id="sp-step-1-content">
    <div id="sp-body"></div>
    <div style="padding: 16px;">
      <button type="button" class="wiz-run" id="btn-go-step-2" onclick="goToStep(2)">Build Scenario for this Street</button>
    </div>
  </div>

  <!-- STEP 2 CONTAINER: Configure Scenario -->
  <div id="sp-step-2-content" style="display:none; padding-bottom: 20px;">"""

new_step_1_2_3 = r"""  <!-- STEP 1 CONTAINER: Empty state -->
  <div id="sp-step-1-content" style="padding: 40px 16px; text-align: center; color: var(--text-muted); font-size: 13px;">
    Please select a street on the map to begin.
  </div>

  <!-- STEP 2 CONTAINER: Analyze data -->
  <div id="sp-step-2-content" style="display:none;">
    <div id="sp-body"></div>
    <div id="sp-chart-area" style="padding: 0 16px 16px 16px;">
      <div style="font-size:10px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Activity Profile (24h)</div>
      <div style="height: 140px; position: relative;">
        <canvas id="activity-chart"></canvas>
      </div>
    </div>
    <div style="padding: 16px;">
      <button type="button" class="wiz-run" id="btn-go-step-3" onclick="goToStep(3)">Configure Intervention</button>
    </div>
  </div>

  <!-- STEP 3 CONTAINER: Pick intervention -->
  <div id="sp-step-3-content" style="display:none; padding-bottom: 20px;">"""

content = content.replace(old_step_1_2, new_step_1_2_3)

# Replace 'id="sp-step-3-content"' to 'id="sp-step-4-content"' for the results container
old_step_3_content = r"""  <!-- STEP 3 CONTAINER: Results -->
  <div id="sp-step-3-content" style="display:none;">"""

new_step_4_content = r"""  <!-- STEP 4 CONTAINER: Results -->
  <div id="sp-step-4-content" style="display:none;">"""

content = content.replace(old_step_3_content, new_step_4_content)

# Update back button in Step 3
old_back_button = r"""    <div style="padding: 0 16px; margin-top: 10px;">
      <button type="button" class="dt-btn" onclick="goToStep(1)">Back</button>
    </div>
  </div>"""

new_back_button = r"""    <div style="padding: 0 16px; margin-top: 10px;">
      <button type="button" class="dt-btn" onclick="goToStep(2)">Back</button>
    </div>
  </div>"""

content = content.replace(old_back_button, new_back_button)

# 3. Remove bottom-chart-panel
bcp_pattern = r'<!-- Bottom Chart Panel -->.*?</div>\s*</div>'
content = re.sub(bcp_pattern, '', content, flags=re.DOTALL)

# 4. Update JavaScript
js_goToStep_old = r"""window.goToStep = function(step) {
  currentScenarioStep = step;
  
  // 1. Update Progress Bar
  const progBar = document.getElementById('scenario-progress');
  if (step === 1 && !selectedStreetId) {
    progBar.classList.remove('visible');
  } else {
    progBar.classList.add('visible');
  }

  for (let i = 1; i <= 3; i++) {
    const stepEl = document.getElementById('sp-step-' + i);
    const lineEl = document.getElementById('sp-line-' + i);
    if (!stepEl) continue;
    
    if (i < step) {
      stepEl.className = 'sp-step done';
      if (lineEl) lineEl.className = 'sp-line done';
      stepEl.querySelector('.sp-icon').innerHTML = '&#10003;'; // Checkmark
    } else if (i === step) {
      stepEl.className = 'sp-step active';
      if (lineEl) lineEl.className = 'sp-line';
      stepEl.querySelector('.sp-icon').innerHTML = i;
    } else {
      stepEl.className = 'sp-step';
      if (lineEl) lineEl.className = 'sp-line';
      stepEl.querySelector('.sp-icon').innerHTML = i;
    }
  }

  // 2. Update Panel Content Visibility
  document.getElementById('sp-step-1-content').style.display = (step === 1) ? 'block' : 'none';
  document.getElementById('sp-step-2-content').style.display = (step === 2) ? 'block' : 'none';
  document.getElementById('sp-step-3-content').style.display = (step === 3) ? 'block' : 'none';
  
  // Ensure the bottom chart panel is visible only in Step 1
  const bcp = document.getElementById('bottom-chart-panel');
  if (step === 1 && selectedStreetId) {
    bcp.classList.add('visible');
  } else {
    bcp.classList.remove('visible');
  }
}"""

js_goToStep_new = r"""window.goToStep = function(step) {
  currentScenarioStep = step;
  
  // 1. Update Progress Bar
  const progBar = document.getElementById('scenario-progress');
  progBar.classList.add('visible');

  for (let i = 1; i <= 4; i++) {
    const stepEl = document.getElementById('sp-step-' + i);
    const lineEl = document.getElementById('sp-line-' + i);
    if (!stepEl) continue;
    
    if (i < step) {
      stepEl.className = 'sp-step done';
      if (lineEl) lineEl.className = 'sp-line done';
      stepEl.querySelector('.sp-icon').innerHTML = '&#10003;'; // Checkmark
    } else if (i === step) {
      stepEl.className = 'sp-step active';
      if (lineEl) lineEl.className = 'sp-line';
      stepEl.querySelector('.sp-icon').innerHTML = i;
    } else {
      stepEl.className = 'sp-step';
      if (lineEl) lineEl.className = 'sp-line';
      stepEl.querySelector('.sp-icon').innerHTML = i;
    }
  }

  // 2. Update Panel Content Visibility
  document.getElementById('sp-step-1-content').style.display = (step === 1) ? 'block' : 'none';
  document.getElementById('sp-step-2-content').style.display = (step === 2) ? 'block' : 'none';
  document.getElementById('sp-step-3-content').style.display = (step === 3) ? 'block' : 'none';
  document.getElementById('sp-step-4-content').style.display = (step === 4) ? 'block' : 'none';
  
  // Resize chart if showing step 2
  if (step === 2 && window.activityChartObj) {
      setTimeout(() => window.activityChartObj.resize(), 50);
  }
}"""

content = content.replace(js_goToStep_old, js_goToStep_new)

js_startOver_old = r"""window.startOver = function() {
  stopScenarioAnimation();
  clearScenarioDeltas();
  document.getElementById('iv-chart-wrap').style.display = 'none';
  goToStep(1);
}"""

js_startOver_new = r"""window.startOver = function() {
  stopScenarioAnimation();
  clearScenarioDeltas();
  document.getElementById('iv-chart-wrap').style.display = 'none';
  document.getElementById('sp-header').style.display = 'none';
  goToStep(1);
}"""
content = content.replace(js_startOver_old, js_startOver_new)

js_openStreetPanel_old = r"""window.openStreetPanel = function(f) {
  if (originalOpenStreetPanel) originalOpenStreetPanel(f);
  document.getElementById('sp-header').style.display = 'flex';
  document.getElementById('iv-apply-btn').disabled = false;
  document.getElementById('iv-apply-btn').innerText = 'Run Model';
  goToStep(1);
}"""

js_openStreetPanel_new = r"""window.openStreetPanel = function(f) {
  if (originalOpenStreetPanel) originalOpenStreetPanel(f);
  document.getElementById('sp-header').style.display = 'flex';
  document.getElementById('iv-apply-btn').disabled = false;
  document.getElementById('iv-apply-btn').innerText = 'Run Model';
  goToStep(2);
}"""
content = content.replace(js_openStreetPanel_old, js_openStreetPanel_new)

js_ivApply_old = r"""window.ivApply = async function() {
  if (originalIvApply) await originalIvApply();
  goToStep(3);
}"""

js_ivApply_new = r"""window.ivApply = async function() {
  if (originalIvApply) await originalIvApply();
  goToStep(4);
}"""
content = content.replace(js_ivApply_old, js_ivApply_new)

# Force the panel to be open and on Step 1 when the page loads
js_init = r"""
// Ensure panel is visible on load at step 1
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('street-panel').style.transform = 'translateX(0)';
    document.getElementById('street-panel').style.opacity = '1';
    document.getElementById('sp-header').style.display = 'none';
    goToStep(1);
});
</script>
</body>"""

content = content.replace("</script>\n</body>", js_init)

# Remove .bcp CSS
content = re.sub(r'#bottom-chart-panel\s*\{.*?\n\}\n(?=\s*\.iv-verdict-banner|\s*</style>)', '', content, flags=re.DOTALL)
content = re.sub(r'\.bcp-header\s*\{.*?\}\s*\.bcp-title\s*\{.*?\}\s*\.bcp-title\s*\.meta\s*\{.*?\}\s*\.bcp-hero\s*\{.*?\}\s*\.bcp-hero-val\s*\{.*?\}\s*\.bcp-hero-lbl\s*\{.*?\}\s*#bcp-chart-container\s*\{.*?\}\s*', '', content, flags=re.DOTALL)

html_path.write_text(content, encoding="utf-8")
print("Refactored to 4-step UX successfully.")
