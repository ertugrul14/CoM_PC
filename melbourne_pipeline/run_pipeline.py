"""
run_pipeline.py — Orchestrator: runs steps 1–10 in sequence.

Usage:
    python run_pipeline.py              # run all steps
    python run_pipeline.py 1            # run only step 1
    python run_pipeline.py 1 3          # run steps 1 through 3
"""
import logging
import sys
from datetime import datetime
from pathlib import Path

from config import LOGS_DIR

# ── Logging setup ────────────────────────────────────────────────────────────
LOGS_DIR.mkdir(parents=True, exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOGS_DIR / f"pipeline_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("pipeline")


# ── Step registry ────────────────────────────────────────────────────────────
STEPS = {
    1:  ("step_01_fetch",       "Fetch all raw data"),
    2:  ("step_02_snap",        "Snap sensors + CLUE spatial aggregation → static_features"),
    3:  ("step_03_temporal",    "Temporal encoding + weather forward-fill"),
    4:  ("step_04_graph",       "Spatial + semantic graph construction"),
    5:  ("step_05_process",     "Parking occupancy + XGBoost ped imputation (graph-informed)"),
    6:  ("step_06_aggregate",   "Aggregate ped activity + parking occupancy → street profiles"),
    7:  ("step_07_cluster",     "GMM clustering on full street character"),
    8:  ("step_08_cube",       "Data cube assembly + dual graph construction"),
    9:  ("step_09_train",      "MultiGCN training (GRU + dual GCN)"),
    10: ("step_10_interpret",   "Permutation feature importance + branch contribution"),
    11: ("step_11_scenario",      "Scenario-based intervention simulation"),
    12: ("step_12_export",      "Export enriched frontend JSON"),
}


def run_step(step_num: int) -> None:
    module_name, description = STEPS[step_num]
    log.info(f"{'='*60}")
    log.info(f"Step {step_num}: {description}")
    log.info(f"{'='*60}")

    mod = __import__(f"steps.{module_name}", fromlist=["run"])
    mod.run()

    log.info(f"Step {step_num} done.\n")


def main():
    args = sys.argv[1:]
    if len(args) == 0:
        start, end = 1, max(STEPS)
    elif len(args) == 1:
        start = end = int(args[0])
    else:
        start, end = int(args[0]), int(args[1])

    log.info(f"Pipeline run: steps {start}-{end}  |  log -> {log_file}")
    for step in range(start, end + 1):
        if step in STEPS:
            run_step(step)
    log.info("Pipeline finished.")


if __name__ == "__main__":
    main()
