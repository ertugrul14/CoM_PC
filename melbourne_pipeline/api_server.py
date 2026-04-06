"""
Scenario API server for the Melbourne CBD Curbside Reallocation Pipeline.

Exposes one endpoint:
  POST /scenario
  Body (JSON):
    {
      "street_id":         "12345",
      "t_start":           10200,        // optional — defaults to mid-val-period
      "duration":          16,           // 15-min bins (default 16 = 4 h)
      "rollout_steps":     32,           // total rollout (default 32 = 8 h)
      "intervention_type": "pedestrianise",  // pedestrianise | restrict_park | boost_ped
      "magnitude":         null          // required for restrict_park and boost_ped
    }

  Returns: scenario result JSON (same structure as step_11_scenario.run_scenario)

  GET /health  — liveness check

Usage:
  cd melbourne_pipeline
  python api_server.py            # default port 5050
  python api_server.py --port 8000
"""
import argparse
import json
import logging
import traceback
from pathlib import Path

from flask import Flask, jsonify, request
from flask_cors import CORS

from config import PROCESSED_DIR
from steps.step_11_scenario import run_scenario, VALID_INTERVENTIONS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
)
log = logging.getLogger("api_server")

app = Flask(__name__)
CORS(app)   # allow requests from the HTML file served from any origin

# ── Pre-load meta so we can validate t_start bounds without loading the cube ──
_meta = None

def _get_meta():
    global _meta
    if _meta is None:
        p = PROCESSED_DIR / "cube_meta.json"
        if p.exists():
            _meta = json.loads(p.read_text())
    return _meta


@app.route("/health", methods=["GET"])
def health():
    meta = _get_meta()
    return jsonify({
        "status": "ok",
        "N": meta["N"] if meta else None,
        "T": meta["T"] if meta else None,
    })


@app.route("/scenario", methods=["POST"])
def scenario():
    body = request.get_json(force=True, silent=True)
    if not body:
        return jsonify({"error": "Request body must be JSON"}), 400

    # ── Required ────────────────────────────────────────────────────────────
    street_id = body.get("street_id")
    if street_id is None:
        return jsonify({"error": "street_id is required"}), 400

    intervention_type = body.get("intervention_type", "pedestrianise")
    if intervention_type not in VALID_INTERVENTIONS:
        return jsonify({
            "error": f"intervention_type must be one of {list(VALID_INTERVENTIONS)}"
        }), 400

    # ── Optional with defaults ───────────────────────────────────────────────
    meta       = _get_meta()
    T          = meta["T"]     if meta else 14400
    T_val_start = meta.get("T_val_start", 10080) if meta else 10080
    WINDOW     = 96

    duration      = int(body.get("duration",      16))
    rollout_steps = int(body.get("rollout_steps",  max(duration, 32)))

    # Default t_start: midpoint of validation period, rounded to nearest hour
    default_t = T_val_start + ((T - T_val_start) // 2)
    default_t = max(WINDOW, default_t)
    t_start   = int(body.get("t_start", default_t))

    magnitude = body.get("magnitude", None)
    if magnitude is not None:
        magnitude = float(magnitude)

    log.info(
        f"Scenario request: street={street_id}, type={intervention_type}, "
        f"t_start={t_start}, duration={duration}, rollout={rollout_steps}, "
        f"magnitude={magnitude}"
    )

    try:
        result = run_scenario(
            street_id         = str(street_id),
            t_start           = t_start,
            duration          = duration,
            rollout_steps     = rollout_steps,
            intervention_type = intervention_type,
            magnitude         = magnitude,
            save              = False,   # don't write to disk — avoids Live Server reload
        )
        return jsonify(result)
    except KeyError as e:
        log.warning(f"KeyError: {e}")
        return jsonify({"error": str(e)}), 404
    except ValueError as e:
        log.warning(f"ValueError: {e}")
        return jsonify({"error": str(e)}), 400
    except Exception:
        log.error(traceback.format_exc())
        return jsonify({"error": "Internal server error — see server logs"}), 500


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5050)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    args = parser.parse_args()
    log.info(f"Starting API server on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False)
