import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# Extract Step 2 HTML (Intervention)
step2_match = re.search(r'(<!-- STEP 2: Choose an intervention -->.*?<!-- STEP 3: Set the window -->)', content, re.DOTALL)
step2_html = step2_match.group(1).strip() if step2_match else ""

# Extract Step 3 HTML (Window)
step3_match = re.search(r'(<!-- STEP 3: Set the window -->.*?<!-- RUN BUTTON -->)', content, re.DOTALL)
step3_html = step3_match.group(1).strip() if step3_match else ""

# Extract RUN BUTTON HTML
run_match = re.search(r'(<!-- RUN BUTTON -->.*?</div>\s*</div><!-- /sp2-body -->)', content, re.DOTALL)
if run_match:
    run_html = run_match.group(1).strip()
    # Remove the </div><!-- /sp2-body --> from the end
    run_html = re.sub(r'</div><!-- /sp2-body -->$', '', run_html).strip()
else:
    run_html = ""

# Remove #scenario-panel entirely (from <!-- Scenario vertical panel --> to the end of #scenario-panel)
content = re.sub(r'<!-- Scenario vertical panel -->.*?</div>\s*<!-- Street detail panel -->', '<!-- Street detail panel -->', content, flags=re.DOTALL)

# Now rebuild #street-panel
# Find the start of #street-panel
street_panel_start = r"""<!-- Street detail panel -->
<div id="street-panel">
  <div id="sp-header">
    <button id="sp-close" onclick="closeStreetPanel()">&#x2715;</button>
    <div id="sp-street-name">—</div>
    <div id="sp-street-meta">—</div>
  </div>
  <div id="sp-body"></div>"""

new_street_panel = f"""<!-- Street detail panel -->
<div id="street-panel">
  <div id="sp-header">
    <button id="sp-close" onclick="closeStreetPanel()">&#x2715;</button>
    <div id="sp-street-name">—</div>
    <div id="sp-street-meta">—</div>
  </div>
  
  <!-- STEP 1 CONTAINER: Understand Street -->
  <div id="sp-step-1-content">
    <div id="sp-body"></div>
    <div style="padding: 16px;">
      <button type="button" class="wiz-run" id="btn-go-step-2" onclick="goToStep(2)">Build Scenario for this Street</button>
    </div>
  </div>

  <!-- STEP 2 CONTAINER: Configure Scenario -->
  <div id="sp-step-2-content" style="display:none; padding-bottom: 20px;">
    {step2_html}
    {step3_html}
    {run_html}
    <div style="padding: 0 16px; margin-top: 10px;">
      <button type="button" class="dt-btn" onclick="goToStep(1)">Back</button>
    </div>
  </div>

  <!-- STEP 3 CONTAINER: Results -->
  <div id="sp-step-3-content" style="display:none;">"""

# Replace the start of street-panel
content = content.replace(street_panel_start, new_street_panel)

# Finally, we need to wrap the sp2-results and sp2-save inside sp-step-3-content and add a back/start over button
save_scenario_block = r"""  <div id="sp2-save">
    <div id="sp2-save-label">Save Scenario</div>
    <input type="text" id="sp2-save-input" placeholder="e.g. Swanston St pedestrianise 4h">
    <button type="button" id="sp2-save-btn" onclick="saveScenario()">Download JSON</button>
  </div>
</div>"""

new_save_scenario = """  <div id="sp2-save">
    <div id="sp2-save-label">Save Scenario</div>
    <input type="text" id="sp2-save-input" placeholder="e.g. Swanston St pedestrianise 4h">
    <button type="button" id="sp2-save-btn" onclick="saveScenario()">Download JSON</button>
    <div style="margin-top: 16px; text-align: center;">
      <button type="button" class="dt-btn" onclick="startOver()">Start Over</button>
    </div>
  </div>
  </div><!-- /sp-step-3-content -->
</div><!-- /street-panel -->"""

content = content.replace(save_scenario_block, new_save_scenario)

html_path.write_text(content, encoding="utf-8")
print("Refactored panels successfully.")
