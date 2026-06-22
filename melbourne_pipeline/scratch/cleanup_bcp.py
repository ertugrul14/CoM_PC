import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# Let's cleanly remove lines 4617 to 4625.
# Specifically, we want to remove the bcp and bcp-street-name lines.

bad_code = r"""      const bcp = document.getElementById('bottom-chart-panel');
      const container = document.getElementById('sp-activity-chart-container');
      
      // Setup BCP Header
      document.getElementById('bcp-street-name').innerHTML = (p.name || ('Street ' + sid)) + ` <span class="meta">24-hour typical profile</span>`;
      document.getElementById('bcp-hero-stats').innerHTML = `
        <div class="bcp-stat"><b>${pedFlow != null ? pedFlow.toFixed(0) : '—'}</b> avg peds / 15m</div>
        <div class="bcp-stat"><b>${parking != null && parking > 0 ? (parking * 100).toFixed(0) : '—'}%</b> avg parking occ</div>
      `;"""

good_code = r"""      const container = document.getElementById('sp-activity-chart-container');"""

content = content.replace(bad_code, good_code)

# We also need to remove 'if (bcp) bcp.classList.remove('open');'
bad_bcp_close = r"""      const bcp = document.getElementById('bottom-chart-panel');
      if (bcp) bcp.classList.remove('open');"""
content = content.replace(bad_bcp_close, "")

html_path.write_text(content, encoding="utf-8")
print("Cleaned up remaining BCP references!")
