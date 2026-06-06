"""
Bundle only the files needed for the ablation experiment into a folder.
Upload the resulting folder to Lightning Studio.

Usage:
    python bundle_ablation.py
    # Then upload D:/melbourne_ingestor/ablation_bundle/ to Lightning Studio
"""
import shutil
from pathlib import Path

SRC = Path(__file__).parent / "melbourne_pipeline"
DST = Path(__file__).parent / "ablation_bundle" / "melbourne_pipeline"

FILES = {
    # Config
    "config.py": "config.py",
    ".env": ".env",

    # Steps (imported by the experiment)
    "steps/__init__.py": "steps/__init__.py",
    "steps/step_08_cube.py": "steps/step_08_cube.py",
    "steps/step_09_train.py": "steps/step_09_train.py",

    # Experiment
    "experiments/__init__.py": "experiments/__init__.py",
    "experiments/ablation_sensor_only.py": "experiments/ablation_sensor_only.py",

    # Data — processed
    "data/processed/cube.npy": "data/processed/cube.npy",
    "data/processed/cube_meta.json": "data/processed/cube_meta.json",
    "data/processed/norm_stats.json": "data/processed/norm_stats.json",
    "data/processed/graph_spatial.pt": "data/processed/graph_spatial.pt",
    "data/processed/graph_semantic.pt": "data/processed/graph_semantic.pt",
    "data/processed/node_index.parquet": "data/processed/node_index.parquet",
    "data/processed/sensor_map.parquet": "data/processed/sensor_map.parquet",
    "data/processed/parking_occupancy.parquet": "data/processed/parking_occupancy.parquet",
    "data/processed/spatial_edges.parquet": "data/processed/spatial_edges.parquet",
    "data/processed/semantic_edges.parquet": "data/processed/semantic_edges.parquet",

    # Data — models
    "data/models/best_model.pt": "data/models/best_model.pt",
    "data/models/model_eval.json": "data/models/model_eval.json",
}

def main():
    bundle_root = DST.parent
    if bundle_root.exists():
        shutil.rmtree(bundle_root)

    copied = 0
    skipped = []
    for src_rel, dst_rel in FILES.items():
        src_path = SRC / src_rel
        dst_path = DST / dst_rel
        if not src_path.exists():
            skipped.append(src_rel)
            continue
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_path)
        size_mb = src_path.stat().st_size / (1024 * 1024)
        print(f"  {src_rel:55s}  {size_mb:8.2f} MB")
        copied += 1

    # Write the setup script for Lightning Studio
    setup_script = DST.parent / "setup_and_run.sh"
    setup_script.write_text("""\
#!/bin/bash
# Run this in Lightning Studio terminal after uploading ablation_bundle/

set -e

cd /teamspace/studios/this_studio/ablation_bundle/melbourne_pipeline
# or wherever you uploaded it — adjust the path

# Install dependencies
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu121
pip install pandas pyarrow geopandas xgboost scikit-learn python-dotenv

# Verify GPU
python -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, Device: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"CPU\"}')"

# Run the ablation
python -m experiments.ablation_sensor_only

echo ""
echo "=== Done! Results at data/experiments/ablation_results.json ==="
cat data/experiments/ablation_results.json
""")

    # Write a requirements.txt
    req = DST.parent / "requirements.txt"
    req.write_text("""\
torch>=2.0
pandas>=2.0
pyarrow
geopandas
xgboost
scikit-learn
python-dotenv
""")

    total_mb = sum(
        (SRC / f).stat().st_size / (1024 * 1024)
        for f in FILES if (SRC / f).exists()
    )
    print(f"\nBundled {copied} files ({total_mb:.0f} MB) -> {bundle_root}")
    if skipped:
        print(f"Skipped (not found): {skipped}")
    print(f"\nNext steps:")
    print(f"  1. Upload '{bundle_root}' folder to Lightning Studio")
    print(f"  2. Open terminal in Studio")
    print(f"  3. Run: bash setup_and_run.sh")


if __name__ == "__main__":
    main()
