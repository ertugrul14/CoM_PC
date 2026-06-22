"""
Step 12 — Export frontend JSON files.

Enriches streets_viz.geojson with all pipeline outputs and writes
updated summary JSONs consumed by sensor_map_viz.html.

Updates:
  data/processed/streets_viz.geojson    — adds opportunity_score, pred_ped_mean,
                                          uplift_fraction, cf_ped_mean per street
  data/processed/ped_street_summary.json  — already up-to-date from step 05
  data/processed/cluster_summary.json    — already up-to-date from step 07
  data/processed/opportunities_summary.json — top-N streets per archetype
  data/models/model_eval.json            — already written by step 09
"""
import json
import logging
from pathlib import Path

import pandas as pd

from config import PROCESSED_DIR

log = logging.getLogger(__name__)
MODELS_DIR = PROCESSED_DIR.parent / "models"

TOP_N = 50   # top streets per archetype in the summary


def run() -> dict[str, Path]:
    log.info("=== Step 12: Export frontend JSON ===")

    # ── Load opportunity scores ───────────────────────────────────────────────
    scores_df = pd.read_parquet(PROCESSED_DIR / "street_scores.parquet")
    scores_df["street_id"] = scores_df["street_id"].astype(str)
    score_lookup = scores_df.set_index("street_id").to_dict(orient="index")
    log.info(f"  Opportunity scores: {len(scores_df)} streets")

    # ── Enrich streets_viz.geojson ────────────────────────────────────────────
    viz_path = PROCESSED_DIR / "streets_viz.geojson"
    with open(viz_path) as f:
        streets_gj = json.load(f)

    enriched = 0
    for feat in streets_gj["features"]:
        sid = str(feat["properties"].get("street_id", ""))
        sc  = score_lookup.get(sid)
        if sc:
            feat["properties"]["opportunity_score"] = sc["opportunity_score"]
            feat["properties"]["pred_ped_mean"]     = sc["pred_ped_mean"]
            feat["properties"]["uplift_fraction"]   = sc["uplift_fraction"]
            feat["properties"]["cf_ped_mean"]       = sc["cf_ped_mean"]
            enriched += 1

    with open(viz_path, "w") as f:
        json.dump(streets_gj, f)
    log.info(f"  Enriched {enriched}/{len(streets_gj['features'])} streets in streets_viz.geojson")

    # ── Opportunities summary: top-N per archetype ────────────────────────────
    summary: dict[str, list] = {}
    for archetype, grp in scores_df.groupby("archetype"):
        top = grp.nlargest(TOP_N, "opportunity_score")
        summary[archetype] = top[[
            "street_id", "opportunity_score", "pred_ped_mean",
            "uplift_fraction", "mean_parking", "best_window",
        ]].to_dict(orient="records")

    summ_path = PROCESSED_DIR / "opportunities_summary.json"
    summ_path.write_text(json.dumps(summary, indent=2))
    for arch, rows in summary.items():
        log.info(f"  {arch}: top score={rows[0]['opportunity_score']:.3f}  "
                 f"street={rows[0]['street_id']}")

    # ── Model eval summary ────────────────────────────────────────────────────
    eval_src = MODELS_DIR / "model_eval.json"
    eval_dst = PROCESSED_DIR / "model_eval.json"
    if eval_src.exists():
        eval_dst.write_text(eval_src.read_text())
        log.info(f"  Copied model_eval.json to processed/")

    # ── Time-Series Aggregation ───────────────────────────────────────────────
    log.info("  Aggregating time-series data...")
    try:
        # Load datasets
        ped_df = pd.read_parquet(PROCESSED_DIR / "ped_complete.parquet")
        prk_df = pd.read_parquet(PROCESSED_DIR / "parking_occupancy.parquet")

        # Filter ped data to only physical sensors
        ped_df = ped_df[ped_df["source"] == "sensor"]

        # Extract hour from UTC time_bin
        ped_df["hour"] = ped_df["time_bin"].dt.hour
        prk_df["hour"] = prk_df["time_bin"].dt.hour

        # Group by street and hour, calculating mean
        ped_agg = ped_df.groupby(["street_id", "hour"])["ped_flow"].mean().unstack(fill_value=0)
        prk_agg = prk_df.groupby(["street_id", "hour"])["occupancy_rate"].mean().unstack(fill_value=0)

        timeseries_dict = {}
        all_streets = set(ped_agg.index) | set(prk_agg.index)

        for sid in all_streets:
            ped_vals = ped_agg.loc[sid].tolist() if sid in ped_agg.index else [0.0] * 24
            prk_vals = prk_agg.loc[sid].tolist() if sid in prk_agg.index else [0.0] * 24

            # Format to 2 decimal places to save space
            ped_vals = [round(v, 2) for v in ped_vals]
            prk_vals = [round(v, 2) for v in prk_vals]

            # We only store the street if it actually has measured data for either ped or parking
            # (If it's in the index, it has data)
            timeseries_dict[str(sid)] = {"ped": ped_vals, "park": prk_vals}

        ts_path = PROCESSED_DIR / "street_timeseries.json"
        ts_path.write_text(json.dumps(timeseries_dict))
        log.info(f"  Saved street_timeseries.json ({len(timeseries_dict)} streets)")
    except Exception as e:
        log.error(f"  Failed to aggregate time-series data: {e}")

    log.info(f"  Saved opportunities_summary.json ({sum(len(v) for v in summary.values())} records)")
    log.info("Step 12 complete.")
    return {
        "streets_viz":    viz_path,
        "opps_summary":   summ_path,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s")
    run()
