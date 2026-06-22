import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

# 1. Move #controls down
old_controls = r"""  #controls {
    position: absolute; top: 14px; left: 14px; z-index: 6;"""

new_controls = r"""  #controls {
    position: absolute; top: 110px; left: 14px; z-index: 6;"""

content = content.replace(old_controls, new_controls)

# 2. Increase snap lines thickness
old_snap = r"""        paint: {
          'line-color': LAYER_COLORS[layerName],
          'line-width': 1,
          'line-opacity': 0.55,
          'line-dasharray': [2, 2],
          'line-emissive-strength': 1,
        },"""

new_snap = r"""        paint: {
          'line-color': LAYER_COLORS[layerName],
          'line-width': 2.5,
          'line-opacity': 0.55,
          'line-dasharray': [2, 2],
          'line-emissive-strength': 1,
        },"""

content = content.replace(old_snap, new_snap)

html_path.write_text(content, encoding="utf-8")
print("Moved layers panel down and increased snap line thickness.")
