import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Move street-panel to the left
old_panel = r"""  #street-panel {
    position: absolute; top: 110px; right: 14px; bottom: 14px; z-index: 2;
    width: 570px; background: rgba(17,16,9,0.88);
    border: 1px solid var(--border-hi); border-radius: 16px;
    backdrop-filter: blur(12px);
    box-shadow: 0 8px 32px rgba(0,0,0,0.45);
    transform: translateX(calc(100% + 28px)); transition: transform 0.2s ease;"""

new_panel = r"""  #street-panel {
    position: absolute; top: 110px; left: 14px; bottom: 14px; z-index: 2;
    width: 570px; background: rgba(17,16,9,0.88);
    border: 1px solid var(--border-hi); border-radius: 16px;
    backdrop-filter: blur(12px);
    box-shadow: 0 8px 32px rgba(0,0,0,0.45);
    transform: translateX(calc(-100% - 28px)); transition: transform 0.2s ease;"""

content = content.replace(old_panel, new_panel)

# 2. Reorganize taskbar buttons in HTML
old_taskbar_html = r"""  <div id="taskbar-left">
    <button type="button" id="layers-toggle-btn" class="topbar-btn" onclick="toggleNetwork()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>
      Layers
    </button>
    <button type="button" id="buildings-toggle-btn" class="topbar-btn" onclick="toggle3DBuildings()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="20" rx="1"/><line x1="9" y1="6" x2="9" y2="6.01"/><line x1="15" y1="6" x2="15" y2="6.01"/><line x1="9" y1="10" x2="9" y2="10.01"/><line x1="15" y1="10" x2="15" y2="10.01"/><line x1="9" y1="14" x2="15" y2="14"/><line x1="9" y1="18" x2="15" y2="18"/></svg>
      3D
    </button>
  </div>
  <div id="taskbar-brand">
    <div class="diamond"></div>
    CURBSIDE INTENSIFICATION
  </div>
  <div id="taskbar-right">
  </div>"""

new_taskbar_html = r"""  <div id="taskbar-left">
  </div>
  <div id="taskbar-brand">
    <div class="diamond"></div>
    CURBSIDE INTENSIFICATION
  </div>
  <div id="taskbar-right">
    <button type="button" id="tour-btn-float" class="topbar-btn" onclick="tourStart()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
      How It Works
    </button>
    <button type="button" id="layers-toggle-btn" class="topbar-btn" onclick="toggleNetwork()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>
      Layers
    </button>
    <button type="button" id="buildings-toggle-btn" class="topbar-btn" onclick="toggle3DBuildings()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="20" rx="1"/><line x1="9" y1="6" x2="9" y2="6.01"/><line x1="15" y1="6" x2="15" y2="6.01"/><line x1="9" y1="10" x2="9" y2="10.01"/><line x1="15" y1="10" x2="15" y2="10.01"/><line x1="9" y1="14" x2="15" y2="14"/><line x1="9" y1="18" x2="15" y2="18"/></svg>
      3D
    </button>
  </div>"""

content = content.replace(old_taskbar_html, new_taskbar_html)

# 3. Remove old floating How it works button
old_float = r"""<button type="button" id="tour-btn-float" class="bottom-corner-btn bottom-left" onclick="tourStart()">
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
  How It Works
</button>"""
content = content.replace(old_float, "")

# 4. Make taskbar-right align to the bottom like taskbar-left
old_right_css = r"""  #taskbar-right {
    position: absolute; right: 16px; top: 50%; transform: translateY(-50%);
    display: flex; align-items: center; gap: 8px;
    opacity: 0; transition: opacity 0.5s ease;
  }"""

new_right_css = r"""  #taskbar-right {
    position: absolute; right: 16px; bottom: 16px;
    display: flex; align-items: center; gap: 8px;
    opacity: 0; transition: opacity 0.5s ease;
  }"""
content = content.replace(old_right_css, new_right_css)

html_path.write_text(content, encoding="utf-8")
print("Moved street panel left and topbar buttons right.")
