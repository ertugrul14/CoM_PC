"""
Bundle the finalized GNN retrain into an upload-ready folder for a new Lightning Studio.

Trains the dual-head MultiGCN on the NEW (XGBoost-removed, city-climatology) cube with
the Exp B protocol: ped loss masked to the 74 real ped sensors (train_lightning.py has
PED_LOSS_SCOPE = "sensors"). Imputed streets stay as graph context only.

    python bundle_retrain.py
    # then upload D:/melbourne_ingestor/retrain_bundle/ to a new Lightning Studio

Layout produced (paths match train_lightning.py's BASE_DIR logic):
    retrain_bundle/
      train_lightning.py
      requirements_training.txt
      setup_and_run.sh
      melbourne_pipeline/data/processed/{cube.npy, cube_meta.json, norm_stats.json,
                                         graph_spatial.pt, graph_semantic.pt,
                                         parking_occupancy.parquet, node_index.parquet,
                                         ped_valid_mask.pt}
"""
import shutil
from pathlib import Path

ROOT = Path(__file__).parent
BUNDLE = ROOT / "retrain_bundle"

# (src relative to ROOT) -> (dst relative to BUNDLE).  Same file list as bundle_training.sh.
FILES = {
    "train_lightning.py": "train_lightning.py",
    "requirements_training.txt": "requirements_training.txt",
    "melbourne_pipeline/data/processed/cube.npy":                 "melbourne_pipeline/data/processed/cube.npy",
    "melbourne_pipeline/data/processed/cube_meta.json":           "melbourne_pipeline/data/processed/cube_meta.json",
    "melbourne_pipeline/data/processed/norm_stats.json":          "melbourne_pipeline/data/processed/norm_stats.json",
    "melbourne_pipeline/data/processed/graph_spatial.pt":         "melbourne_pipeline/data/processed/graph_spatial.pt",
    "melbourne_pipeline/data/processed/graph_semantic.pt":        "melbourne_pipeline/data/processed/graph_semantic.pt",
    "melbourne_pipeline/data/processed/parking_occupancy.parquet":"melbourne_pipeline/data/processed/parking_occupancy.parquet",
    "melbourne_pipeline/data/processed/node_index.parquet":       "melbourne_pipeline/data/processed/node_index.parquet",
    "melbourne_pipeline/data/processed/ped_valid_mask.pt":        "melbourne_pipeline/data/processed/ped_valid_mask.pt",
}

SETUP_SH = """\
#!/bin/bash
# Run in the Lightning Studio terminal after uploading retrain_bundle/.
set -e
cd /teamspace/studios/this_studio/retrain_bundle   # adjust if uploaded elsewhere

pip install -r requirements_training.txt

# GPU check — must print CUDA: True for a sane runtime
python -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, Device: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \\"CPU\\"}')"

# Confirm the Exp B protocol is active before burning GPU hours
grep -n 'PED_LOSS_SCOPE' train_lightning.py | head -1

# Retrain (early-stops on val ped MAE; ~1-3 h on a T4)
python train_lightning.py

echo ""
echo "=== Done. Copy these back from melbourne_pipeline/data/models/ ==="
echo "    best_model.pt  parking_mask.pt  run_config.json  model_eval.json  cube_meta.json"
echo "--- headline metrics ---"
python -c "import json; e=json.load(open('melbourne_pipeline/data/models/model_eval.json')); print({k:e[k] for k in ['best_epoch','val_ped_mae','val_ped_r2','test_ped_mae','test_ped_r2','test_park_mae','test_park_r2']})"
"""


def main():
    if BUNDLE.exists():
        shutil.rmtree(BUNDLE)

    copied, skipped, total = 0, [], 0.0
    for src_rel, dst_rel in FILES.items():
        src = ROOT / src_rel
        if not src.exists():
            skipped.append(src_rel)
            continue
        dst = BUNDLE / dst_rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        mb = src.stat().st_size / (1024 * 1024)
        total += mb
        print(f"  {src_rel:60s} {mb:9.2f} MB")
        copied += 1

    # LF newlines — bash on Linux chokes on CRLF ("$'\\r': command not found").
    (BUNDLE / "setup_and_run.sh").write_text(SETUP_SH, newline="\n")

    print(f"\nBundled {copied} files ({total:.0f} MB) -> {BUNDLE}")
    if skipped:
        print("!! MISSING (run the pipeline first):")
        for s in skipped:
            print(f"     {s}")
    print("\nNext steps:")
    print(f"  1. Upload '{BUNDLE}' to a new Lightning Studio")
    print("  2. Terminal:  bash setup_and_run.sh")
    print("  3. Share back: melbourne_pipeline/data/models/model_eval.json")


if __name__ == "__main__":
    main()
