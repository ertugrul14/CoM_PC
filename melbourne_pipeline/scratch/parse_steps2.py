import re

html_path = r'd:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html'
with open(html_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

in_step = False
step_text = []
for line in lines:
    if 'class="kb-step"' in line:
        print("--- NEW STEP ---")
        in_step = True
    if in_step:
        if '<h3>' in line or '<div class="label">' in line:
            print(line.strip())
