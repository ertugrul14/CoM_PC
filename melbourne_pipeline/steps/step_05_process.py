"""
Step 05 — Parking occupancy reconstruction + pedestrian fill.

Outputs (all in data/processed/):
  parking_occupancy.parquet  — street_id, time_bin, occupancy_rate, valid_parking
  ped_complete.parquet       — street_id, time_bin, ped_flow, ped_confidence, source

Pedestrian fill (city-climatology — D-023, XGBoost removed):
  - Sensored streets (74) carry their REAL 15-min counts (confidence 1.0).
  - Unsensored streets (1,323) are filled with the per-bin MEAN of the sensored
    streets (a single city rhythm) at confidence 0.5, source "climatology".
  - Rationale: the leave-streets-out GNN test (Fix 5 / D-023) showed the GNN
    reconstructs unsensored flow as well from this trivial fill as from the former
    XGBoost imputation (gap within fold noise). Imputation = coverage, not
    accuracy; per-street scale is information-capped and downstream-irrelevant.
  - The output schema is unchanged from the XGBoost version, so steps 06/08 are
    untouched.
"""
import json
import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import r2_score
from sklearn.model_selection import GroupKFold, cross_val_predict

from config import PROCESSED_DIR, RAW_DIR, DATA_START, TIME_BIN_MINUTES, STREETS_GEOJSON

ARTERIAL_TYPES = {"Arterial", "Council Major"}

log = logging.getLogger(__name__)

N_TIME_BINS = 14_400
MIN_EVENTS_PER_STREET = 5   # minimum events to qualify as a parking street

# Time-aware event cap (D-009): 4h during restriction hours, dynamic overnight.
# Restriction window derived from bay restrictions dataset (UTC, +53min offset corrected).
DAYTIME_CAP_SECONDS = 14_400   # 4 hours — daytime artifact filter
RESTRICTION_START_H = 7        # 07:30 UTC — restrictions begin
RESTRICTION_START_M = 30
RESTRICTION_END_H = 19         # 19:30 UTC — restrictions end (conservative cutoff)
RESTRICTION_END_M = 30

# Fix #3 (D-013): sensor-outage handling. Interpolate ped gaps up to this many
# 15-min bins (4 = 1h, brief dropouts likely ≈ real low counts); longer runs are
# treated as sensor outages — kept 0 in the cube but flagged ped_valid=False so the
# GNN ped loss ignores them. A 0-filled outage otherwise mimics a genuine quiet
# period and manufactures fake "flexibility windows".
SHORT_GAP_BINS = 4

# Fix #4 (D-014): per-street imputation confidence via similarity transfer. Each
# unsensored street borrows the mean CV R² of its CONF_KNN nearest sensored streets
# in z-scored static-feature space; mean R² ≥ CONF_R2_HIGH → 0.8 tier else 0.5.
CONF_KNN = 5
CONF_R2_HIGH = 0.6
CONF_FEATURES = ["total_jobs", "cafe_count", "business_count",
                 "poi_total", "dining_capacity", "area_m2"]


def _fill_short_gaps(arr2d: np.ndarray, max_gap: int) -> tuple[np.ndarray, np.ndarray]:
    """Linearly interpolate interior NaN runs of length <= max_gap, per row.

    arr2d   : (n_streets, T) ped values; NaN where the sensor reported nothing.
    max_gap : maximum consecutive-NaN run length to interpolate.

    Returns (filled, interp_mask). Runs longer than max_gap, and edge runs that
    touch t=0 or t=T-1 (no two-sided anchor for interpolation), are left as NaN
    and marked False in interp_mask — i.e. treated as outages, not interpolated.
    Deterministic; no randomness.
    """
    filled = arr2d.copy()
    interp_mask = np.zeros(arr2d.shape, dtype=bool)
    n, T = arr2d.shape
    for r in range(n):
        isna = np.isnan(arr2d[r])
        if not isna.any():
            continue
        interp = pd.Series(arr2d[r]).interpolate(method="linear", limit_area="inside").to_numpy()
        idx = 0
        while idx < T:
            if not isna[idx]:
                idx += 1
                continue
            start = idx
            while idx < T and isna[idx]:
                idx += 1
            end = idx  # exclusive
            if start > 0 and end < T and (end - start) <= max_gap:   # interior, short
                filled[r, start:end] = interp[start:end]
                interp_mask[r, start:end] = True
    return filled, interp_mask


def _time_aware_cap(start_ts: "np.datetime64") -> int:
    """Return event duration cap in seconds based on arrival time.

    During restriction hours (07:30–19:30 UTC): apply 4h daytime cap.
    Outside restriction hours: extend to next 07:30 (legitimate overnight stay).
    """
    ts = pd.Timestamp(start_ts)
    minutes = ts.hour * 60 + ts.minute
    rest_start = RESTRICTION_START_H * 60 + RESTRICTION_START_M  # 450 min
    rest_end   = RESTRICTION_END_H   * 60 + RESTRICTION_END_M    # 1170 min

    if rest_start <= minutes < rest_end:
        return DAYTIME_CAP_SECONDS

    # Overnight: allow until next restriction-start (07:30 same or next day)
    next_rest = ts.replace(
        hour=RESTRICTION_START_H, minute=RESTRICTION_START_M, second=0, microsecond=0
    )
    if minutes >= rest_end:  # evening ≥19:30 — restriction starts tomorrow
        next_rest += pd.Timedelta(days=1)
    # early morning <07:30 — restriction starts today
    return max(int((next_rest - ts).total_seconds()), 0)


# ═══════════════════════════════════════════════════════════════════════════════
# PART A — Parking occupancy reconstruction
# ═══════════════════════════════════════════════════════════════════════════════

def _build_parking_occupancy() -> pd.DataFrame:
    log.info("Part A: Parking occupancy reconstruction")

    sensor_map  = pd.read_parquet(PROCESSED_DIR / "sensor_map.parquet")
    parking_raw = pd.read_parquet(RAW_DIR / "parking_raw.parquet")

    p_sensors   = sensor_map[sensor_map["sensor_type"] == "parking"][["sensor_id", "street_id"]]
    p_sensors   = p_sensors.dropna(subset=["street_id"])
    bay_to_street = dict(zip(p_sensors["sensor_id"], p_sensors["street_id"]))

    parking_raw["kerbsideid"] = parking_raw["kerbsideid"].astype(str)
    parking_raw["street_id"]  = parking_raw["kerbsideid"].map(bay_to_street)
    parking_raw = parking_raw.dropna(subset=["street_id"])
    log.info(f"  Parking events with street_id: {len(parking_raw):,}")

    parking_raw["local_datetime"] = pd.to_datetime(parking_raw["local_datetime"])
    parking_raw = parking_raw.sort_values(["kerbsideid", "local_datetime"])

    # Pair Present→Unoccupied events as occupied intervals (time-aware cap, D-009)
    events = []
    for bay_id, grp in parking_raw.groupby("kerbsideid"):
        street_id = grp["street_id"].iloc[0]
        statuses  = grp["status_description"].values
        times     = grp["local_datetime"].values
        for i in range(len(statuses)):
            if statuses[i] != "Present":
                continue
            start = times[i]
            cap   = _time_aware_cap(start)
            end   = times[i + 1] if i + 1 < len(statuses) \
                    else start + np.timedelta64(cap, "s")
            dur   = (end - start) / np.timedelta64(1, "s")
            if dur > cap:
                end = start + np.timedelta64(cap, "s")
            events.append((street_id, bay_id, start, end))

    log.info(f"  Occupancy intervals: {len(events):,}")

    time_index = pd.date_range(
        start=DATA_START, periods=N_TIME_BINS,
        freq=f"{TIME_BIN_MINUTES}min", tz="UTC",
    )
    bin_dur   = TIME_BIN_MINUTES * 60  # seconds
    _EPOCH    = pd.Timestamp("1970-01-01", tz="UTC")
    # Use total_seconds() — portable across pandas versions (ns vs us precision)
    first_bin = int((time_index[0] - _EPOCH).total_seconds())

    bays_per_street = (
        parking_raw.groupby("street_id")["kerbsideid"].nunique().to_dict()
    )

    ev_df = pd.DataFrame(events, columns=["street_id", "bay_id", "start", "end"])
    ev_df["start"] = pd.to_datetime(ev_df["start"], utc=True)
    ev_df["end"]   = pd.to_datetime(ev_df["end"],   utc=True)

    ev_start_ts = (ev_df["start"] - _EPOCH).dt.total_seconds().to_numpy(dtype=np.int64)
    ev_end_ts   = (ev_df["end"]   - _EPOCH).dt.total_seconds().to_numpy(dtype=np.int64)
    ev_bin_lo   = np.clip(((ev_start_ts - first_bin) // bin_dur).astype(int), 0, N_TIME_BINS - 1)
    ev_bin_hi   = np.clip(((ev_end_ts   - first_bin) // bin_dur).astype(int), 0, N_TIME_BINS - 1)

    from collections import defaultdict
    occupied = defaultdict(set)
    sids     = ev_df["street_id"].values
    bids     = ev_df["bay_id"].values
    for k in range(len(ev_df)):
        sid, bid = sids[k], bids[k]
        for b in range(ev_bin_lo[k], ev_bin_hi[k] + 1):
            occupied[(sid, b)].add(bid)

    log.info(f"  Bin-bay occupancy entries: {len(occupied):,}")

    qualifying = [
        s for s in bays_per_street
        if len(parking_raw[parking_raw["street_id"] == s]) >= MIN_EVENTS_PER_STREET
    ]
    log.info(f"  Streets with >= {MIN_EVENTS_PER_STREET} events: {len(qualifying)}")

    rows = []
    for sid in qualifying:
        total_bays = bays_per_street[sid]
        for b in range(N_TIME_BINS):
            n_occ = len(occupied.get((sid, b), set()))
            rows.append((sid, time_index[b], min(n_occ / total_bays, 1.0)))

    df = pd.DataFrame(rows, columns=["street_id", "time_bin", "occupancy_rate"])
    df["valid_parking"] = True
    log.info(f"  parking_occupancy: {len(df):,} rows, {df.street_id.nunique()} streets")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Spatial graph lag feature
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_spatial_lag(
    ped_sensored: pd.DataFrame,
    all_streets: list,
    sensored_streets: list,
) -> pd.DataFrame:
    """
    For each street, compute the mean ped_flow of its sensored spatial-graph
    neighbours at every time_bin.  Streets not in the graph get lag = 0.

    Returns a pivot DataFrame: index=time_bin, columns=street_id.
    This avoids creating a 57M-row long format table.
    """
    ni_path = PROCESSED_DIR / "node_index.parquet"
    se_path = PROCESSED_DIR / "spatial_edges.parquet"
    if not ni_path.exists() or not se_path.exists():
        log.warning("  Spatial graph files not found — spatial_lag_ped will be 0")
        pivot = ped_sensored.pivot(index="time_bin", columns="street_id", values="ped_flow")
        zero_pivot = pd.DataFrame(
            0.0, index=pivot.index, columns=sorted(all_streets), dtype=np.float32
        )
        return zero_pivot

    node_index    = pd.read_parquet(ni_path)
    spatial_edges = pd.read_parquet(se_path)

    sid_to_nidx = dict(zip(node_index["street_id"], node_index["node_idx"]))
    nidx_to_sid = dict(zip(node_index["node_idx"], node_index["street_id"]))
    sensored_set = set(sensored_streets)

    # Build adjacency: nidx → list of neighbour nidxs
    adj: dict[int, list[int]] = {}
    for _, row in spatial_edges.iterrows():
        ni, nj = int(row["node_i"]), int(row["node_j"])
        adj.setdefault(ni, []).append(nj)

    # For each street: list of sensored neighbour street_ids
    neighbor_lookup: dict[str, list[str]] = {}
    for sid in all_streets:
        if sid not in sid_to_nidx:
            neighbor_lookup[sid] = []
            continue
        nidx = sid_to_nidx[sid]
        nbrs = adj.get(nidx, [])
        neighbor_lookup[sid] = [
            nidx_to_sid[n] for n in nbrs
            if nidx_to_sid.get(n) in sensored_set
        ]

    n_with_lag = sum(1 for v in neighbor_lookup.values() if v)
    log.info(f"  Spatial lag: {n_with_lag}/{len(all_streets)} streets have >= 1 sensored neighbour")

    # Sensored pivot: (14400,) × (n_sensored) — small and fast
    s_pivot = ped_sensored.pivot(
        index="time_bin", columns="street_id", values="ped_flow"
    )
    sensored_cols = list(s_pivot.columns)
    sensored_idx  = {s: i for i, s in enumerate(sensored_cols)}
    n_sensored    = len(sensored_cols)

    # Weight matrix W: (n_all, n_sensored) — uniform over sensored neighbours
    all_sids = sorted(all_streets)
    W = np.zeros((len(all_sids), n_sensored), dtype=np.float32)
    for i, sid in enumerate(all_sids):
        nbrs = [n for n in neighbor_lookup.get(sid, []) if n in sensored_idx]
        if nbrs:
            for n in nbrs:
                W[i, sensored_idx[n]] = 1.0 / len(nbrs)

    # lag_mat: (14400, n_all) — vectorised neighbour-mean
    lag_mat   = s_pivot.values.astype(np.float32) @ W.T
    lag_pivot = pd.DataFrame(lag_mat, index=s_pivot.index, columns=all_sids)
    return lag_pivot   # index=time_bin, columns=street_id


# ═══════════════════════════════════════════════════════════════════════════════
# PART B — XGBoost pedestrian imputation (v3 — graph-informed)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_ped_complete(parking_occ: pd.DataFrame) -> tuple[pd.DataFrame, float, dict]:
    log.info("Part B: XGBoost ped imputation (v3 — graph-informed)")

    sensor_map = pd.read_parquet(PROCESSED_DIR / "sensor_map.parquet")
    ped_raw    = pd.read_parquet(RAW_DIR / "ped_raw.parquet")
    static     = pd.read_parquet(PROCESSED_DIR / "static_features.parquet")
    temporal   = pd.read_parquet(PROCESSED_DIR / "temporal_features.parquet")
    weather    = pd.read_parquet(PROCESSED_DIR / "weather.parquet")

    # ── Arterial-only, no-intersection filter ─────────────────────────────────
    _gdf = gpd.read_file(STREETS_GEOJSON)[["street_id", "str_type", "name"]]
    _gdf["street_id"] = _gdf["street_id"].astype(str)
    _keep = (
        _gdf["str_type"].isin(ARTERIAL_TYPES) &
        ~_gdf["name"].str.contains("Intersection", case=False, na=False)
    )
    arterial_ids = set(_gdf.loc[_keep, "street_id"])
    static = static[static["street_id"].astype(str).isin(arterial_ids)].reset_index(drop=True)
    log.info(f"  Arterial filter: {len(static)} streets kept (minor/private/intersections excluded)")

    ped_sensors   = sensor_map[sensor_map["sensor_type"] == "pedestrian"][["sensor_id", "street_id"]]
    ped_sensors   = ped_sensors.dropna(subset=["street_id"])
    loc_to_street = dict(zip(ped_sensors["sensor_id"].astype(str), ped_sensors["street_id"]))

    time_index = pd.date_range(
        start=DATA_START, periods=N_TIME_BINS,
        freq=f"{TIME_BIN_MINUTES}min", tz="UTC",
    )

    # ── Aggregate raw ped counts to 15-min bins per street ───────────────────
    ped_raw["location_id"] = ped_raw["location_id"].astype(str)
    ped_raw["street_id"]   = ped_raw["location_id"].map(loc_to_street)
    ped_raw = ped_raw.dropna(subset=["street_id"])
    ped_raw["local_datetime"] = pd.to_datetime(ped_raw["local_datetime"], utc=True)
    ped_raw["time_bin"]       = ped_raw["local_datetime"].dt.floor(f"{TIME_BIN_MINUTES}min")

    ped_agg = (
        ped_raw.groupby(["street_id", "time_bin"])["total_of_directions"]
        .sum().reset_index()
        .rename(columns={"total_of_directions": "ped_flow"})
    )
    ped_agg = ped_agg[ped_agg["time_bin"].isin(time_index)]

    sensored_streets  = sorted(s for s in ped_agg["street_id"].unique() if s in arterial_ids)
    all_streets       = sorted(static["street_id"].unique())
    unsensored_streets = sorted(set(all_streets) - set(sensored_streets))
    log.info(f"  Sensored: {len(sensored_streets)}, unsensored: {len(unsensored_streets)}, total: {len(all_streets)}")

    # Full grid for sensored streets — missing bins are sensor GAPS, not zeros.
    full_grid = pd.MultiIndex.from_product(
        [sensored_streets, time_index], names=["street_id", "time_bin"]
    ).to_frame(index=False)
    ped_sensored = full_grid.merge(ped_agg, on=["street_id", "time_bin"], how="left")

    # Fix #3 (D-013): interpolate short gaps, flag long gaps as outages.
    # After sort each street is N_TIME_BINS contiguous, time-ordered rows.
    ped_sensored = ped_sensored.sort_values(["street_id", "time_bin"]).reset_index(drop=True)
    n_sens   = len(sensored_streets)
    flow2d   = ped_sensored["ped_flow"].to_numpy(dtype=np.float64).reshape(n_sens, N_TIME_BINS)
    observed = ~np.isnan(flow2d)
    filled, interp = _fill_short_gaps(flow2d, SHORT_GAP_BINS)
    valid    = observed | interp                       # trustworthy bins for the loss
    flow_fin = np.nan_to_num(filled, nan=0.0)          # long-gap outages → 0 (masked out)
    ped_sensored["ped_flow"]  = flow_fin.reshape(-1).astype(np.float32)
    ped_sensored["ped_valid"] = valid.reshape(-1)
    n_masked = int((~valid).sum())
    log.info(f"  Ped bins: observed={int(observed.sum()):,} "
             f"interp(<={SHORT_GAP_BINS})={int(interp.sum()):,} "
             f"masked-outage={n_masked:,} ({100*n_masked/valid.size:.1f}%)")

    # ── City-climatology pedestrian fill (XGBoost removed — D-023) ────────────
    # Fix 5 (D-023, scripts/fix5_leave_streets_out_gnn.py) showed the GNN
    # reconstructs unsensored-street flow as well from a trivial city-average
    # rhythm as from XGBoost imputation (gap B-A = +0.12 MAE, within fold noise),
    # so the XGBoost stage is dropped. Every unsensored street is filled with the
    # per-bin MEAN of the sensored streets: same daily SHAPE, no per-street SCALE
    # (scale is information-capped and downstream-irrelevant per Fix 5). The
    # ped_complete output schema (street_id, time_bin, ped_flow, ped_confidence,
    # source, ped_valid) is unchanged, so steps 06/08 need no edit.
    ped_sensored["ped_confidence"] = 1.0          # observed sensor streets
    ped_sensored["source"]         = "sensor"

    # City rhythm: mean sensored ped_flow per time_bin, ignoring outage bins so a
    # masked-out sensor gap does not drag the average toward zero.
    sens_valid = ped_sensored.copy()
    sens_valid.loc[~sens_valid["ped_valid"].astype(bool), "ped_flow"] = np.nan
    city_rhythm = (
        sens_valid.groupby("time_bin")["ped_flow"].mean()
        .reindex(time_index).fillna(0.0).to_numpy(dtype=np.float32)
    )
    city_rhythm = np.clip(city_rhythm, 0.0, None)
    log.info(f"  City-climatology fill: mean={city_rhythm.mean():.1f} "
             f"peak={city_rhythm.max():.1f} ped/bin over {len(time_index)} bins")

    # Broadcast the city rhythm to every unsensored street (street-major order,
    # mirroring the sensored block so the downstream concat aligns cleanly).
    pred_sid_list, pred_time_list, pred_flow_list = [], [], []
    for sid in unsensored_streets:
        pred_sid_list.append(np.full(len(time_index), sid))
        pred_time_list.append(time_index)
        pred_flow_list.append(city_rhythm)

    unsensored_grid = pd.DataFrame({
        "street_id": np.concatenate(pred_sid_list),
        "time_bin":  np.concatenate(pred_time_list),
        "ped_flow":  np.concatenate(pred_flow_list).astype(np.float32),
    })
    unsensored_grid["ped_confidence"] = np.float32(0.5)   # flat low trust (imputed)
    unsensored_grid["source"]         = "climatology"
    unsensored_grid["ped_valid"]      = True              # value exists (not an outage)
    log.info(f"  Filled {len(unsensored_streets)} unsensored streets with city "
             f"climatology (confidence=0.5, source=climatology)")

    ped_complete = pd.concat([
        ped_sensored[["street_id", "time_bin", "ped_flow", "ped_confidence", "source", "ped_valid"]],
        unsensored_grid[["street_id", "time_bin", "ped_flow", "ped_confidence", "source", "ped_valid"]],
    ], ignore_index=True)

    log.info(f"  ped_complete: {len(ped_complete):,} rows, {ped_complete.street_id.nunique()} streets")
    return ped_complete


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════

def _validate(parking_occ: pd.DataFrame, ped_complete: pd.DataFrame):
    n_p = parking_occ["street_id"].nunique()
    assert len(parking_occ) == n_p * N_TIME_BINS, \
        f"parking_occupancy row count mismatch: {len(parking_occ)} != {n_p}*{N_TIME_BINS}"
    assert parking_occ["occupancy_rate"].between(0, 1).all(), "occupancy_rate outside [0,1]"
    log.info(f"  parking_occupancy OK: {n_p} streets x {N_TIME_BINS} bins")

    n_ped = ped_complete["street_id"].nunique()
    assert len(ped_complete) == n_ped * N_TIME_BINS, \
        f"ped_complete row count mismatch: {len(ped_complete)} != {n_ped}*{N_TIME_BINS}"
    assert (ped_complete["ped_flow"] >= 0).all(), "ped_flow has negative values"
    n_sensor = (ped_complete["ped_confidence"] == 1.0).sum() // N_TIME_BINS
    log.info(f"  ped_complete OK: {n_ped} streets, {n_sensor} sensored (confidence=1.0)")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def run() -> dict[str, Path]:
    parking_occ              = _build_parking_occupancy()
    ped_complete             = _build_ped_complete(parking_occ)

    _validate(parking_occ, ped_complete)

    p_path   = PROCESSED_DIR / "parking_occupancy.parquet"
    ped_path = PROCESSED_DIR / "ped_complete.parquet"
    parking_occ.to_parquet(p_path,   index=False)
    ped_complete.to_parquet(ped_path, index=False)

    # ── Pedestrian street summary (for viz) ───────────────────────────────────
    ped_summ = (
        ped_complete.groupby("street_id")
        .agg(
            mean_ped_flow=("ped_flow", "mean"),
            peak_ped_flow=("ped_flow", "max"),
            ped_source=("source", "first"),
            ped_confidence=("ped_confidence", "first"),
        )
        .reset_index()
    )
    ped_summ["mean_ped_flow"] = ped_summ["mean_ped_flow"].round(2)
    ped_summ["peak_ped_flow"] = ped_summ["peak_ped_flow"].round(2)
    summ_dict = ped_summ.set_index("street_id").to_dict(orient="index")
    summ_path = PROCESSED_DIR / "ped_street_summary.json"
    summ_path.write_text(json.dumps(summ_dict, indent=None))
    log.info(f"  Saved ped_street_summary.json ({len(summ_dict)} streets)")

    log.info(
        f"Step 5 complete: parking={parking_occ.street_id.nunique()} streets, "
        f"ped={ped_complete.street_id.nunique()} streets, "
        f"ped fill=city-climatology (XGBoost removed, D-023)"
    )
    return {"parking_occupancy": p_path, "ped_complete": ped_path, "ped_summary": summ_path}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
