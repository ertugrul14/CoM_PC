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
      "magnitude":         null,         // required for restrict_park and boost_ped
      "dow":               2,            // 0=Mon..6=Sun; used to find t_start if t_start omitted
      "hour":              9,            // 0-23; used with dow to pick the nearest val-period bin
      "minute":            0             // 0 or 15 or 30 or 45
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

# ── Temporal constants (from step_03_temporal.py TIME_INDEX) ─────────────────
# The cube's bin 0 starts at 2025-11-01 09:00 UTC, which is Saturday in
# Melbourne local wall-clock time (the ped/parking data are AEDT-labelled as UTC).
# Nov 1 2025 = Saturday = dayofweek 5 (0=Monday convention).
_BINS_PER_WEEK = 7 * 24 * 4          # 672 bins per week
_BIN0_OFFSET_MIN = 5 * 24 * 60 + 9 * 60  # Saturday 09:00 from Mon midnight = 7740 min


def _tstart_for_dow_hour(
    dow: int,
    hour: int,
    minute: int,
    val_start: int,
    val_end: int,
    window: int = 96,
) -> int:
    """
    Find the first cube bin in [val_start, val_end) that matches (dow, hour, minute).

    Parameters
    ----------
    dow    : 0=Monday … 6=Sunday
    hour   : 0-23
    minute : 0, 15, 30, or 45
    val_start, val_end : validation bin range from cube_meta.json
    window : model input window size; t_start must be >= window

    Returns
    -------
    Matching bin index, or midpoint of val period if none found (shouldn't happen).
    """
    target_min = dow * 24 * 60 + hour * 60 + minute
    week_min   = _BINS_PER_WEEK * 15  # 10080 minutes per week

    # Bin within the first week (mod 672) that matches the target time-of-week.
    # Python % returns non-negative, so this is safe for negative numerators.
    b0 = ((target_min - _BIN0_OFFSET_MIN) // 15) % _BINS_PER_WEEK

    # Advance by enough full weeks to reach val_start.
    n_weeks = max(0, (val_start - b0 + _BINS_PER_WEEK - 1) // _BINS_PER_WEEK)
    t = b0 + n_weeks * _BINS_PER_WEEK

    # Safety: ensure t >= window and is inside the val period.
    if t < window:
        t += _BINS_PER_WEEK
    if val_start <= t < val_end:
        return t

    # Fallback: midpoint of val period (should not be reached).
    return val_start + (val_end - val_start) // 2


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
    meta        = _get_meta()
    T           = meta["T"]           if meta else 14400
    T_val_start = meta.get("T_val_start", 10080) if meta else 10080
    T_val_end   = meta.get("T_val_end",   12240) if meta else 12240
    WINDOW      = 96

    duration      = int(body.get("duration",      16))
    rollout_steps = int(body.get("rollout_steps",  max(duration, 32)))

    # Resolve t_start: prefer explicit t_start, then dow/hour, then val midpoint.
    if "t_start" in body:
        t_start = int(body["t_start"])
    elif "dow" in body or "hour" in body:
        dow    = int(body.get("dow",    2))   # 0=Mon..6=Sun; default Wednesday
        hour   = int(body.get("hour",   9))   # 0-23; default 09:00
        minute = int(body.get("minute", 0))   # 0/15/30/45
        t_start = _tstart_for_dow_hour(dow, hour, minute, T_val_start, T_val_end, WINDOW)
    else:
        # Fallback: midpoint of validation period (not training/test boundary).
        t_start = T_val_start + (T_val_end - T_val_start) // 2
        t_start = max(WINDOW, t_start)

    magnitude = body.get("magnitude", None)
    if magnitude is not None:
        magnitude = float(magnitude)

    # Two-stage reallocate (D-027): Stage A removal fraction + Stage B use/uplift.
    removal_frac = body.get("removal_frac")
    use          = body.get("use")
    uplift       = body.get("uplift")

    allow_imputed = bool(body.get("allow_imputed", False))

    log.info(
        f"Scenario request: street={street_id}, type={intervention_type}, "
        f"t_start={t_start}, duration={duration}, rollout={rollout_steps}, "
        f"magnitude={magnitude}, allow_imputed={allow_imputed}"
    )

    try:
        result = run_scenario(
            street_id         = str(street_id),
            t_start           = t_start,
            duration          = duration,
            rollout_steps     = rollout_steps,
            intervention_type = intervention_type,
            magnitude         = magnitude,
            removal_frac      = removal_frac,
            use               = use,
            uplift            = uplift,
            save              = False,   # don't write to disk — avoids Live Server reload
            allow_imputed     = allow_imputed,
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
