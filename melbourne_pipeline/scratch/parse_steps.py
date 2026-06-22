import re
import os

html_path = r'd:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html'
with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
    html = f.read()

# Find all kb-step elements
steps = re.findall(r'<div class="kb-step".*?>.*?</div>\s*(?=</?div)', html, re.DOTALL)
print("Found steps in HTML:")
for i, step in enumerate(steps):
    title_match = re.search(r'<h3[^>]*>(.*?)</h3>', step, re.DOTALL)
    title = title_match.group(1).strip() if title_match else "No Title"
    print(f"Step {i+1}: {title}")
    
print("\nPipeline Steps:")
steps_dir = r'd:\melbourne_ingestor\melbourne_pipeline\steps'
for filename in sorted(os.listdir(steps_dir)):
    if filename.endswith('.py') and filename.startswith('step_'):
        print(filename)
