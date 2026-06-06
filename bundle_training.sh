#!/bin/bash
# Bundle files needed for Step 09 training on Lightning AI.
# Run from project root: bash bundle_training.sh

set -e

BUNDLE="training_bundle.tar.gz"

echo "Bundling training files..."
tar -czf "$BUNDLE" \
  train_lightning.py \
  requirements_training.txt \
  melbourne_pipeline/data/processed/cube.npy \
  melbourne_pipeline/data/processed/cube_meta.json \
  melbourne_pipeline/data/processed/norm_stats.json \
  melbourne_pipeline/data/processed/graph_spatial.pt \
  melbourne_pipeline/data/processed/graph_semantic.pt \
  melbourne_pipeline/data/processed/parking_occupancy.parquet \
  melbourne_pipeline/data/processed/node_index.parquet \
  melbourne_pipeline/data/processed/ped_valid_mask.pt

SIZE=$(du -h "$BUNDLE" | cut -f1)
echo "Created $BUNDLE ($SIZE)"
echo ""
echo "=== On Lightning AI ==="
echo "1. Upload $BUNDLE to your Studio"
echo "2. In a terminal:"
echo "   tar -xzf training_bundle.tar.gz"
echo "   pip install -r requirements_training.txt"
echo "   python train_lightning.py"
echo ""
echo "3. Download results from:  melbourne_pipeline/data/models/"
echo "   Files to copy back:     best_model.pt  parking_mask.pt"
echo "                           run_config.json  model_eval.json  cube_meta.json"
