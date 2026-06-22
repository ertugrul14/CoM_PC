import re

html_path = r'd:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Find all div elements that have 'kb-step' in their class
matches = re.finditer(r'<div[^>]*class="[^"]*kb-step[^"]*"[^>]*>(.*?)</div>\s*(?=<div|$)', html, re.DOTALL | re.IGNORECASE)
found = False
for i, match in enumerate(matches):
    found = True
    content = match.group(1)
    title_match = re.search(r'<h3[^>]*>(.*?)</h3>', content, re.DOTALL | re.IGNORECASE)
    label_match = re.search(r'<div[^>]*class="[^"]*label[^"]*"[^>]*>(.*?)</div>', content, re.DOTALL | re.IGNORECASE)
    title = title_match.group(1).strip() if title_match else "No Title"
    label = label_match.group(1).strip() if label_match else "No Label"
    print(f"Step {i+1}: {label} - {title}")

if not found:
    print("No kb-step elements found.")
