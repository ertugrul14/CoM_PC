import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Move progress bar down
old_prog = r"""  .scenario-progress {
    position: fixed; top: 64px; left: 50%; transform: translateX(-50%);"""

new_prog = r"""  .scenario-progress {
    position: fixed; top: 110px; left: 50%; transform: translateX(-50%);"""

content = content.replace(old_prog, new_prog)

# 2. Revert topbar-btn
old_topbtn = r"""  .topbar-btn {
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

new_topbtn = r"""  .topbar-btn {
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

content = content.replace(old_topbtn, new_topbtn)

# 3. Align taskbar-left down
old_left = r"""  #taskbar-left {
    position: absolute; left: 16px; top: 50%; transform: translateY(-50%);"""

new_left = r"""  #taskbar-left {
    position: absolute; left: 16px; bottom: 16px;"""

content = content.replace(old_left, new_left)

html_path.write_text(content, encoding="utf-8")
print("Reverted button size, aligned them down, and moved progress bar down.")
