import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

old_taskbar_left = r"""  <div id="taskbar-left">
    <button type="button" id="layers-toggle-btn" class="topbar-btn" onclick="toggleLayersPanel()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>
      Layers
    </button>
    <button type="button" id="buildings-toggle-btn" class="topbar-btn" onclick="toggle3DBuildings()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="20" rx="1"/><line x1="9" y1="6" x2="9" y2="6.01"/><line x1="15" y1="6" x2="15" y2="6.01"/><line x1="9" y1="10" x2="9" y2="10.01"/><line x1="15" y1="10" x2="15" y2="10.01"/><line x1="9" y1="14" x2="15" y2="14"/><line x1="9" y1="18" x2="15" y2="18"/></svg>
      3D
    </button>
  </div>"""

new_taskbar_left = r"""  <div id="taskbar-left">
  </div>"""

old_taskbar_right = r"""  <div id="taskbar-right">
  </div>"""

new_taskbar_right = r"""  <div id="taskbar-right">
    <button type="button" id="tour-btn-float" class="topbar-btn" onclick="tourStart()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
      How It Works
    </button>
    <button type="button" id="layers-toggle-btn" class="topbar-btn" onclick="toggleLayersPanel()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>
      Layers
    </button>
    <button type="button" id="buildings-toggle-btn" class="topbar-btn" onclick="toggle3DBuildings()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="20" rx="1"/><line x1="9" y1="6" x2="9" y2="6.01"/><line x1="15" y1="6" x2="15" y2="6.01"/><line x1="9" y1="10" x2="9" y2="10.01"/><line x1="15" y1="10" x2="15" y2="10.01"/><line x1="9" y1="14" x2="15" y2="14"/><line x1="9" y1="18" x2="15" y2="18"/></svg>
      3D
    </button>
  </div>"""

content = content.replace(old_taskbar_left, new_taskbar_left)
content = content.replace(old_taskbar_right, new_taskbar_right)

html_path.write_text(content, encoding="utf-8")
print("Buttons moved to right taskbar.")
