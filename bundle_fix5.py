"""
Bundle only the files needed for the Fix 5 leave-streets-out GNN test into a folder.
Upload the resulting folder to a NEW Lightning Studio.

Usage:
    python bundle_fix5.py
    # Then upload D:/melbourne_ingestor/fix5_bundle/ to Lightning Studio

Layout produced (so the script's sys.path logic resolves):
    fix5_bundle/
      scripts/fix5_leave_streets_out_gnn.py
      melbourne_pipeline/
        config.py, .env, steps/...
        data/processed/...  data/raw/ped_raw.parquet
      setup_and_run.sh
      requirements.txt
"""
import shutil
from pathlib import Path

ROOT = Path(__file__).parent
SRC_PKG = ROOT / "melbourne_pipeline"
BUNDLE = ROOT / "fix5_bundle"
DST_PKG = BUNDLE / "melbourne_pipeline"

# Files copied from the project root (the experiment script lives in scripts/).
ROOT_FILES = {
    "scripts/fix5_leave_streets_out_gnn.py": "scripts/fix5_leave_streets_out_gnn.py",
}

# Files copied from melbourne_pipeline/ (code + data the script reads).
PKG_FILES = {
    # ── Code ──────────────────────────────────────────────────────────────────
    "config.py": "config.py",
    ".env": ".env",
    "steps/__init__.py": "steps/__init__.py",
    "steps/step_05_process.py": "steps/step_05_process.py",   # _compute_spatial_lag, ARTERIAL_TYPES
    "steps/step_09_train.py": "steps/step_09_train.py",       # MultiGCN + hyperparameters

    # ── Data: GNN (model graph + cube) ─────────────────────────────────────────
    "data/processed/cube.npy": "data/processed/cube.npy",                 # 1.85 GB
    "data/processed/cube_meta.json": "data/processed/cube_meta.json",
    "data/processed/norm_stats.json": "data/processed/norm_stats.json",
    "data/processed/graph_spatial.pt": "data/processed/graph_spatial.pt",
    "data/processed/graph_semantic.pt": "data/processed/graph_semantic.pt",

    # ── Data: masks / mapping ──────────────────────────────────────────────────
    "data/processed/node_index.parquet": "data/processed/node_index.parquet",
    "data/processed/sensor_map.parquet": "data/processed/sensor_map.parquet",
    "data/processed/parking_occupancy.parquet": "data/processed/parking_occupancy.parquet",
    "data/processed/spatial_edges.parquet": "data/processed/spatial_edges.parquet",  # _compute_spatial_lag

    # ── Data: XGBoost refit inputs (Arm A) ─────────────────────────────────────
    "data/processed/static_features.parquet": "data/processed/static_features.parquet",
    "data/processed/temporal_features.parquet": "data/processed/temporal_features.parquet",
    "data/processed/weather.parquet": "data/processed/weather.parquet",
    "data/processed/streets.geojson": "data/processed/streets.geojson",
    "data/raw/ped_raw.parquet": "data/raw/ped_raw.parquet",
}

SETUP_SH = """\
#!/bin/bash
# Run this in the Lightning Studio terminal after uploading fix5_bundle/.
set -e

# Adjust if you uploaded elsewhere:
cd /teamspace/studios/this_studio/fix5_bundle

# Dependencies
pip install torch --index-url https://download.pytorch.org/whl/cu121
pip install pandas pyarrow geopandas xgboost scikit-learn python-dotenv

# Verify GPU (must print CUDA: True for the ~6 h run to be feasible)
python -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, Device: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \\"CPU\\"}')"

# 1) Plumbing check first (~2 min, tiny — proves all files are present)
SMOKE_TEST=1 python scripts/fix5_leave_streets_out_gnn.py

# 2) Full run (GPU, ~6 h at default K_FOLDS=3 x 2 arms).
#    For tighter evidence (holds out ~15/74 per fold, ~10 trains) use: K_FOLDS=5
python scripts/fix5_leave_streets_out_gnn.py

echo ""
echo "=== Done. Verdict in: ==="
echo "    melbourne_pipeline/data/experiments/fix5_leave_streets_out_results.json"
python -c "import json,glob; p=glob.glob('melbourne_pipeline/data/experiments/fix5_leave_streets_out_results.json'); print(json.dumps(json.load(open(p[0]))['summary'], indent=2)) if p else print('no results file')"
"""

REQUIREMENTS = """\
torch>=2.0
pandas>=2.0
pyarrow
geopandas
xgboost
scikit-learn
python-dotenv
"""


def _copy(mapping: dict, src_base: Path, dst_base: Path, copied: list, skipped: list):
    for src_rel, dst_rel in mapping.items():
        src_path = src_base / src_rel
        dst_path = dst_base / dst_rel
        if not src_path.exists():
            skipped.append(str(src_base.name + "/" + src_rel))
            continue
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_path)
        size_mb = src_path.stat().st_size / (1024 * 1024)
        print(f"  {src_rel:55s}  {size_mb:9.2f} MB")
        copied.append((src_path, size_mb))


def main():
    if BUNDLE.exists():
        shutil.rmtree(BUNDLE)

    copied, skipped = [], []
    print("Copying package code + data -> fix5_bundle/melbourne_pipeline/")
    _copy(PKG_FILES, SRC_PKG, DST_PKG, copied, skipped)
    print("Copying experiment script -> fix5_bundle/scripts/")
    _copy(ROOT_FILES, ROOT, BUNDLE, copied, skipped)

    # newline="\n": shell scripts MUST use LF, else bash on Linux/Studio sees a
    # stray \r on every line ("$'\r': command not found", "file.py\r: not found").
    (BUNDLE / "setup_and_run.sh").write_text(SETUP_SH, newline="\n")
    (BUNDLE / "requirements.txt").write_text(REQUIREMENTS, newline="\n")

    total_mb = sum(mb for _, mb in copied)
    print(f"\nBundled {len(copied)} files ({total_mb:.0f} MB) -> {BUNDLE}")
    if skipped:
        print(f"\n!! MISSING (run the pipeline first or check paths):")
        for s in skipped:
            print(f"     {s}")
    print("\nNext steps:")
    print(f"  1. Upload '{BUNDLE}' to a new Lightning Studio")
    print("  2. Open a terminal in the Studio")
    print("  3. Run: bash setup_and_run.sh")


if __name__ == "__main__":
    main()
