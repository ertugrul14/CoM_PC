import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Double taskbar height
old_taskbar = r"""  #top-taskbar {
    position: fixed; top: 0; left: 0; right: 0; z-index: 10000;
    width: 100%; height: 48px;"""

new_taskbar = r"""  #top-taskbar {
    position: fixed; top: 0; left: 0; right: 0; z-index: 10000;
    width: 100%; height: 96px;"""

content = content.replace(old_taskbar, new_taskbar)

# 2. Scale up brand text
old_brand = r"""  #taskbar-brand {
    display: flex; align-items: center; gap: 10px;
    font-family: 'Space Grotesk', system-ui; font-weight: 700;
    font-size: 15px; color: var(--text); letter-spacing: -0.2px;
  }
  #taskbar-brand .diamond {
    width: 10px; height: 10px; border-radius: 3px;
    background: var(--accent); transform: rotate(45deg); flex-shrink: 0;
  }"""

new_brand = r"""  #taskbar-brand {
    display: flex; align-items: center; gap: 14px;
    font-family: 'Space Grotesk', system-ui; font-weight: 700;
    font-size: 26px; color: var(--text); letter-spacing: -0.2px;
  }
  #taskbar-brand .diamond {
    width: 16px; height: 16px; border-radius: 4px;
    background: var(--accent); transform: rotate(45deg); flex-shrink: 0;
  }"""

content = content.replace(old_brand, new_brand)

# 3. Scale up topbar buttons (Layers, 3D, Tour)
old_topbtn = r"""  .topbar-btn {
    background: transparent; color: var(--text-dim);
    border: 1px solid var(--border-hi);
    padding: 7px 14px; border-radius: 18px;
    font-size: 12px; font-weight: 600; letter-spacing: 0.4px;
    cursor: pointer;
    display: flex; align-items: center; gap: 6px;
    transition: background 0.15s, color 0.15s, border-color 0.15s;
    white-space: nowrap;
  }
  .topbar-btn:hover { background: var(--surface-hi); color: var(--text); }
  .topbar-btn.active {
    background: rgba(75,116,136,0.25); color: var(--accent);
    border-color: var(--accent-dim);
  }
  .topbar-btn svg { width: 14px; height: 14px; opacity: 0.7; }"""

new_topbtn = r"""  .topbar-btn {
    background: transparent; color: var(--text-dim);
    border: 1px solid var(--border-hi);
    padding: 12px 20px; border-radius: 24px;
    font-size: 16px; font-weight: 600; letter-spacing: 0.4px;
    cursor: pointer;
    display: flex; align-items: center; gap: 8px;
    transition: background 0.15s, color 0.15s, border-color 0.15s;
    white-space: nowrap;
  }
  .topbar-btn:hover { background: var(--surface-hi); color: var(--text); }
  .topbar-btn.active {
    background: rgba(75,116,136,0.25); color: var(--accent);
    border-color: var(--accent-dim);
  }
  .topbar-btn svg { width: 18px; height: 18px; opacity: 0.7; }"""

content = content.replace(old_topbtn, new_topbtn)

# Make sure we also adjust the top padding of the street panel to account for the larger taskbar so it doesn't overlap the taskbar.
# The street panel had top: 14px; but wait, the topbar is absolute and overlays?
# Let's check street-panel top.
# Wait, `#street-panel { position: absolute; top: 14px;` - it's relative to its container.
# If the taskbar is fixed `height: 96px`, maybe we should push the street panel down to `top: 110px`.

old_panel = r"""  #street-panel {
    position: absolute; top: 14px; right: 14px; bottom: 14px; z-index: 2;"""

new_panel = r"""  #street-panel {
    position: absolute; top: 110px; right: 14px; bottom: 14px; z-index: 2;"""

content = content.replace(old_panel, new_panel)

# Also update the progress bar position
# .scenario-progress has bottom: 30px; Wait, the taskbar is at the TOP.
# So progress bar at the bottom is fine.

# What about the scenario panel?
old_scen_panel = r"""  #scenario-panel {
    position: absolute; top: 14px; left: 14px; bottom: 14px; z-index: 2;"""

new_scen_panel = r"""  #scenario-panel {
    position: absolute; top: 110px; left: 14px; bottom: 14px; z-index: 2;"""

content = content.replace(old_scen_panel, new_scen_panel)

# And the controls panel (bottom-chart-panel was deleted, map-controls etc)
# .map-controls { position: absolute; top: 14px; right: 14px; }
old_map_ctrl = r"""  .map-controls {
    position: absolute; top: 14px; right: 14px; z-index: 1;"""

new_map_ctrl = r"""  .map-controls {
    position: absolute; top: 110px; right: 14px; z-index: 1;"""

content = content.replace(old_map_ctrl, new_map_ctrl)

html_path.write_text(content, encoding="utf-8")
print("Doubled the height of the taskbar and adjusted panel offsets.")
