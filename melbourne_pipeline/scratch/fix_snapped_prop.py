import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

content = content.replace(
    "['==', ['to-string', ['get', 'snapped_street_id']], sid], 1.0,",
    "['==', ['to-string', ['get', 'street_id']], sid], 1.0,"
)

content = content.replace(
    "['==', ['to-string', ['get', 'snapped_street_id']], sid], 0.8,",
    "['==', ['to-string', ['get', 'street_id']], sid], 0.8,"
)

html_path.write_text(content, encoding="utf-8")
print("Fixed snapped street property name.")
