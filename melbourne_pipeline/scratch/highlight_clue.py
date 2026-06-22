import re
from pathlib import Path

html_path = Path(r"d:\melbourne_ingestor\melbourne_pipeline\frontend\sensor_map_viz.html")
content = html_path.read_text(encoding="utf-8")

old_click = r"""      if (feat) {
        sensorTip.style.display = 'none';
        openStreetPanel(feat.properties);
        if (window.goToStep) window.goToStep(2);
        document.getElementById('sp-header').style.display = 'flex';
        if (!scenarioActive) {
          _scenarioProps = feat.properties;
        }
        return;
      }"""

new_click = r"""      if (feat) {
        sensorTip.style.display = 'none';
        const sid = String(feat.properties.street_id);
        
        openStreetPanel(feat.properties);
        if (window.goToStep) window.goToStep(2);
        document.getElementById('sp-header').style.display = 'flex';
        if (!scenarioActive) {
          _scenarioProps = feat.properties;
        }
        
        // Highlight clicked street geometry
        if (typeof _highlightStreet === 'function') _highlightStreet(sid);
        
        // Turn on all CLUE layers and highlight snapped features
        if (typeof layerNames !== 'undefined') {
          layerNames.forEach(n => {
            const cLayer = 'clue-' + n;
            const sLayer = 'snap-' + n;
            if (map.getLayer(cLayer)) {
              map.setLayoutProperty(cLayer, 'visibility', 'visible');
              map.setPaintProperty(cLayer, 'circle-opacity', [
                'case',
                ['==', ['to-string', ['get', 'snapped_street_id']], sid], 1.0,
                0.15
              ]);
            }
            if (map.getLayer(sLayer)) {
              map.setLayoutProperty(sLayer, 'visibility', 'visible');
              map.setPaintProperty(sLayer, 'line-opacity', [
                'case',
                ['==', ['to-string', ['get', 'snapped_street_id']], sid], 0.8,
                0.05
              ]);
            }
          });
        }
        return;
      }"""

content = content.replace(old_click, new_click)

old_empty = r"""      // Empty click — close everything
      sensorTip.style.display = 'none';
      closeStreetPanel();
      if (!scenarioActive) {
        _scenarioProps = null;
      }"""

new_empty = r"""      // Empty click — close everything
      sensorTip.style.display = 'none';
      closeStreetPanel();
      if (!scenarioActive) {
        _scenarioProps = null;
      }
      
      // Clear street highlight
      if (typeof _clearHighlight === 'function') _clearHighlight();
      
      // Hide all CLUE layers
      if (typeof layerNames !== 'undefined') {
        layerNames.forEach(n => {
          if (map.getLayer('clue-' + n)) map.setLayoutProperty('clue-' + n, 'visibility', 'none');
          if (map.getLayer('snap-' + n)) map.setLayoutProperty('snap-' + n, 'visibility', 'none');
        });
      }"""

content = content.replace(old_empty, new_empty)

html_path.write_text(content, encoding="utf-8")
print("Added logic to highlight CLUE layers when a street is clicked.")
