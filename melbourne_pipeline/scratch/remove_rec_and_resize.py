import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Update width
content = content.replace("width: 380px; background: rgba(17,16,9,0.88);", "width: 570px; background: rgba(17,16,9,0.88);")

# 2. Remove Recommended Scenario block
rec_block = r"""        const rec = (cluster != null && cluster >= 0 && itype) ? REC_INTERVENTION[itype] : null;
        if (rec) {
          const recColor = CLUSTER_COLORS[cluster] || '#93c5fd';
          html += `<div class="sp-rec" style="border-left-color:${recColor}">
            <div class="sp-rec-head">Recommended scenario</div>
            <div class="sp-rec-action">${rec.action}</div>
            <div class="sp-rec-brief">
              <div class="sp-rec-row"><span class="k">Does</span><span class="v">${rec.does}</span></div>
              <div class="sp-rec-row"><span class="k">Expect</span><span class="v">${rec.expect}</span></div>
            </div>
            ${!scenarioActive
              ? `<button type="button" class="sp-rec-btn" onclick="applyRecommendedIntervention('${rec.ivType}', ${rec.magnitude})">Run this scenario &#8594;</button>`
              : `<div class="sp-helper" style="margin-top:6px">End the current scenario to set this one up.</div>`}
          </div>`;
        } else if (cluster === -1) {
          html += `<div class="sp-helper">No intervention is recommended — this street has no sensor, so it stays in the graph only as spatial context.</div>`;
        }"""

content = content.replace(rec_block, "")

html_path.write_text(content, encoding="utf-8")
print("Removed recommended scenario block and updated panel width to 570px")
