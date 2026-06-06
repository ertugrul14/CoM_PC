"""
Scenario API server — serves the frontend and runs counterfactual simulations.

Usage:
    python api_server.py          # starts on http://127.0.0.1:5050

Endpoints:
    GET  /                  → serves sensor_map_viz.html
    POST /scenario          → runs step_11_scenario.run_scenario()

POST /scenario body (JSON):
    street_id         : str   — Melbourne street identifier
    intervention_type : str   — "pedestrianise" | "restrict_park" | "boost_ped"
    duration          : int   — number of 15-min bins the intervention is active
    rollout_steps     : int   — total autoregressive steps to simulate
    magnitude         : float | null — required for restrict_park / boost_ped
    dow               : int   — day of week (0=Monday … 6=Sunday), default 0
    hour              : int   — hour 0–23, default 9
    minute            : int   — minute 0–59, default 0

The server converts (dow, hour, minute) to the cube bin index that best matches
that time-of-day + day-of-week pattern using L2 distance on the circular encodings
stored in the cube (hour_sin/cos, dow_sin/cos).  The search is restricted to the
validation set so the model never sees its own training data during inference.
"""
import json
import logging
import math
import sys
from pathlib import Path

import numpy as np
from flask import Flask, Response, jsonify, request, send_file

# Allow import of melbourne_pipeline modules
sys.path.insert(0, str(Path(__file__).resolve().parent / "melbourne_pipeline"))

from config import PROCESSED_DIR
from steps.step_09_train import WINDOW
from steps.step_11_scenario import run_scenario

log = logging.getLogger(__name__)
app = Flask(__name__)

FRONTEND_HTML = Path(__file__).parent / "melbourne_pipeline" / "frontend" / "sensor_map_viz.html"

# ── Cube time-feature cache (loaded once) ────────────────────────────────────

_cache: dict | None = None


def _load_cache() -> dict:
    global _cache
    if _cache is not None:
        return _cache

    log.info("Loading cube time features…")
    meta       = json.loads((PROCESSED_DIR / "cube_meta.json").read_text())
    feat_names = meta["feature_names"]

    # Memory-map: only read the time features from node 0 (identical for all nodes)
    cube_raw = np.load(PROCESSED_DIR / "cube.npy", mmap_mode="r")

    fi = {k: feat_names.index(k) for k in ("hour_sin", "hour_cos", "dow_sin", "dow_cos")}
    t_min = max(WINDOW, meta.get("T_val_start", 10080))
    t_max = meta.get("T_val_end", 12240)

    _cache = {
        "hour_sin": cube_raw[0, :, fi["hour_sin"]].copy(),
        "hour_cos": cube_raw[0, :, fi["hour_cos"]].copy(),
        "dow_sin":  cube_raw[0, :, fi["dow_sin"]].copy(),
        "dow_cos":  cube_raw[0, :, fi["dow_cos"]].copy(),
        "t_min":    t_min,
        "t_max":    t_max,
    }
    log.info(f"Cache ready — search range t=[{t_min}, {t_max})")
    return _cache


def _dow_hour_to_bin(dow: int, hour: int, minute: int = 0) -> int:
    """Return the validation-set bin index whose (dow, hour) matches best."""
    c = _load_cache()
    frac       = hour + minute / 60.0
    target_hs  = math.sin(2 * math.pi * frac / 24)
    target_hc  = math.cos(2 * math.pi * frac / 24)
    target_ds  = math.sin(2 * math.pi * dow / 7)
    target_dc  = math.cos(2 * math.pi * dow / 7)

    lo, hi = c["t_min"], c["t_max"]
    dist = (
        (c["hour_sin"][lo:hi] - target_hs) ** 2
        + (c["hour_cos"][lo:hi] - target_hc) ** 2
        + (c["dow_sin"][lo:hi]  - target_ds) ** 2
        + (c["dow_cos"][lo:hi]  - target_dc) ** 2
    )
    return int(lo + int(np.argmin(dist)))


# ── CORS helper ───────────────────────────────────────────────────────────────

def _add_cors(resp: Response) -> Response:
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    return resp


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_file(FRONTEND_HTML)


@app.route("/scenario", methods=["POST", "OPTIONS"])
def scenario():
    if request.method == "OPTIONS":
        return _add_cors(app.make_response("")), 204

    body = request.get_json(force=True)
    try:
        street_id         = str(body["street_id"])
        intervention_type = body["intervention_type"]
        duration          = int(body.get("duration", 16))
        rollout_steps     = int(body.get("rollout_steps", 32))
        magnitude         = body.get("magnitude")
        dow               = int(body.get("dow",    0))
        hour              = int(body.get("hour",   9))
        minute            = int(body.get("minute", 0))
        allow_imputed     = bool(body.get("allow_imputed", False))

        t_start = _dow_hour_to_bin(dow, hour, minute)
        log.info(f"dow={dow} hour={hour:02d}:{minute:02d} → t_start={t_start}")

        # Ensure rollout fits within cube after t_start
        c = _load_cache()
        max_rollout = c["t_max"] - t_start
        if rollout_steps > max_rollout:
            rollout_steps = max(duration, max_rollout - 1)

        result = run_scenario(
            street_id         = street_id,
            t_start           = t_start,
            duration          = duration,
            rollout_steps     = rollout_steps,
            intervention_type = intervention_type,
            magnitude         = magnitude,
            save              = True,
            allow_imputed     = allow_imputed,
        )
        return _add_cors(jsonify(result))

    except ValueError as exc:
        # Honesty gate (or invalid params) — client-correctable, not a server fault.
        # 422 lets the frontend show the reason and offer an "override" toggle
        # (resend with allow_imputed=true) rather than a generic error.
        log.warning(f"Scenario rejected: {exc}")
        resp = jsonify({"error": str(exc), "data_backed": False})
        resp.status_code = 422
        return _add_cors(resp)

    except Exception as exc:
        log.exception("Scenario failed")
        resp = jsonify({"error": str(exc)})
        resp.status_code = 500
        return _add_cors(resp)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s  %(levelname)s  %(message)s",
    )
    _load_cache()       # warm up before first request
    app.run(host="127.0.0.1", port=5050, debug=False)
