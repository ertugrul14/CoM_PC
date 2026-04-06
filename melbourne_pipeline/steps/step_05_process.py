"""
Step 05 — Parking occupancy reconstruction + XGBoost pedestrian imputation.

Outputs (all in data/processed/):
  parking_occupancy.parquet  — street_id, time_bin, occupancy_rate, valid_parking
  ped_complete.parquet       — street_id, time_bin, ped_flow, ped_confidence, source

Model (v3 — graph-informed):
  - GroupKFold(5) over streets: tests generalisation to UNSEEN streets.
  - log1p target transform: ped_flow is heavily right-skewed.
  - Parking occupancy stats as street-level features.
  - Interaction features: hour_sin/cos × is_weekend.
  - Spatial graph lag: mean ped_flow of sensored spatial neighbours per time_bin.
    For each street in the graph, its sensored k-NN neighbours act as a local
    reference signal — giving the XGBoost a "what are nearby streets seeing now?"
    feature unavailable in pure static+temporal designs.
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
EVENT_CAP_SECONDS = 7_200   # 2-hour cap on parking intervals
MIN_EVENTS_PER_STREET = 5   # minimum events to qualify as a parking street


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

    # Pair Present→Unoccupied events as occupied intervals (capped at 2 h)
    events = []
    for bay_id, grp in parking_raw.groupby("kerbsideid"):
        street_id = grp["street_id"].iloc[0]
        statuses  = grp["status_description"].values
        times     = grp["local_datetime"].values
        for i in range(len(statuses)):
            if statuses[i] != "Present":
                continue
            start = times[i]
            end   = times[i + 1] if i + 1 < len(statuses) \
                    else start + np.timedelta64(EVENT_CAP_SECONDS, "s")
            dur   = (end - start) / np.timedelta64(1, "s")
            if dur > EVENT_CAP_SECONDS:
                end = start + np.timedelta64(EVENT_CAP_SECONDS, "s")
            events.append((street_id, bay_id, start, end))

    log.info(f"  Occupancy intervals: {len(events):,}")

    time_index = pd.date_range(
        start=DATA_START, periods=N_TIME_BINS,
        freq=f"{TIME_BIN_MINUTES}min", tz="UTC",
    )
    bin_starts  = time_index.values
    bin_dur     = TIME_BIN_MINUTES * 60
    first_bin   = bin_starts.astype("int64")[0] // 10**9

    bays_per_street = (
        parking_raw.groupby("street_id")["kerbsideid"].nunique().to_dict()
    )

    ev_df = pd.DataFrame(events, columns=["street_id", "bay_id", "start", "end"])
    ev_df["start"] = pd.to_datetime(ev_df["start"], utc=True)
    ev_df["end"]   = pd.to_datetime(ev_df["end"],   utc=True)

    ev_start_ts = ev_df["start"].values.astype("int64") // 10**9
    ev_end_ts   = ev_df["end"].values.astype("int64")   // 10**9
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

    # Full grid for sensored streets (missing bins → 0)
    full_grid = pd.MultiIndex.from_product(
        [sensored_streets, time_index], names=["street_id", "time_bin"]
    ).to_frame(index=False)
    ped_sensored = full_grid.merge(ped_agg, on=["street_id", "time_bin"], how="left")
    ped_sensored["ped_flow"] = ped_sensored["ped_flow"].fillna(0)

    # ── Street-level parking stats ────────────────────────────────────────────
    parking_stats = (
        parking_occ.groupby("street_id")["occupancy_rate"]
        .agg(parking_mean="mean", parking_std="std")
        .fillna(0).reset_index()
    )
    parking_stats["has_parking"] = 1.0
    static = static.merge(parking_stats, on="street_id", how="left")
    static["parking_mean"] = static["parking_mean"].fillna(0.0)
    static["parking_std"]  = static["parking_std"].fillna(0.0)
    static["has_parking"]  = static["has_parking"].fillna(0.0)
    log.info(f"  Streets with parking data: {parking_stats.street_id.nunique()}")

    # ── Temporal feature preparation ──────────────────────────────────────────
    temporal["time_bin"] = pd.to_datetime(temporal["time_bin"], utc=True)
    weather["time_bin"]  = pd.to_datetime(weather["time_bin"],  utc=True)

    # Interaction: hour shape × weekend/weekday distinction
    temporal["hour_sin_x_weekend"] = temporal["hour_sin"] * temporal["is_weekend"]
    temporal["hour_cos_x_weekend"] = temporal["hour_cos"] * temporal["is_weekend"]
    log.info("  Added hour_sin/cos × is_weekend interaction features")

    # ── Spatial graph lag feature ─────────────────────────────────────────────
    log.info("  Computing spatial graph lag feature...")
    lag_pivot = _compute_spatial_lag(ped_sensored, all_streets, sensored_streets)
    # lag_pivot: index=time_bin, columns=street_id — (14400, n_streets)

    # Pre-merge temporal + weather (same 14400 rows, used everywhere)
    tw = temporal.merge(weather, on="time_bin", how="left")
    tw.set_index("time_bin", inplace=True)

    static_cols   = [c for c in static.columns if c != "street_id"]
    temporal_cols = [c for c in temporal.columns if c != "time_bin"]
    weather_cols  = [c for c in weather.columns  if c != "time_bin"]
    feature_cols  = static_cols + temporal_cols + weather_cols + ["spatial_lag_ped"]

    static_lookup = static.set_index("street_id")

    def _make_X(streets_list: list, time_index_arr) -> np.ndarray:
        """Build feature matrix for given streets × full time_index.
        Processes one street at a time to keep memory O(14400 × n_features)."""
        n_times  = len(time_index_arr)
        tw_vals  = tw.loc[time_index_arr].values.astype(np.float32)   # (T, temporal+weather)
        n_tw     = tw_vals.shape[1]
        n_static = len(static_cols)
        n_feat   = n_static + n_tw + 1   # +1 for spatial_lag

        rows = np.empty((len(streets_list) * n_times, n_feat), dtype=np.float32)
        for i, sid in enumerate(streets_list):
            sl = slice(i * n_times, (i + 1) * n_times)
            # static features (broadcast over time)
            if sid in static_lookup.index:
                st_vals = static_lookup.loc[sid, static_cols].values.astype(np.float32)
            else:
                st_vals = np.zeros(n_static, dtype=np.float32)
            rows[sl, :n_static] = st_vals  # broadcast
            # temporal + weather
            rows[sl, n_static:n_static + n_tw] = tw_vals
            # spatial lag
            if sid in lag_pivot.columns:
                rows[sl, -1] = lag_pivot[sid].values.astype(np.float32)
            else:
                rows[sl, -1] = 0.0
        return rows

    # Training matrix (all sensored streets at once — 82×14400 = 1.18M rows, fine)
    X_train = _make_X(sensored_streets, time_index)

    # y in street-major order (matches X_train row order)
    y_raw   = ped_sensored.sort_values(["street_id", "time_bin"])["ped_flow"].values
    y_train = np.log1p(y_raw)

    log.info(f"  Training matrix: {X_train.shape}")
    log.info(f"  Raw ped range: [{y_raw.min():.0f}, {y_raw.max():.0f}]  "
             f"log1p: [{y_train.min():.3f}, {y_train.max():.3f}]")

    # ── GroupKFold(5) over streets ────────────────────────────────────────────
    log.info("  Running GroupKFold(5) CV over streets...")
    # groups: repeat each street_id 14400 times (street-major order)
    groups = np.repeat(sensored_streets, len(time_index))

    cv_model = xgb.XGBRegressor(
        n_estimators=200, max_depth=5, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
        tree_method="hist", random_state=42, verbosity=0,
    )
    gkf = GroupKFold(n_splits=5)
    y_pred_cv = cross_val_predict(cv_model, X_train, y_train, cv=gkf, groups=groups)

    overall_r2_log = r2_score(y_train, y_pred_cv)

    y_pred_cv_raw = np.expm1(np.clip(y_pred_cv, 0, None))
    # groups is street-major: each street occupies a contiguous block of N_TIME_BINS rows
    street_r2: dict[str, float] = {}
    for idx_s, sid in enumerate(sensored_streets):
        lo = idx_s * len(time_index)
        hi = lo + len(time_index)
        street_r2[sid] = float(r2_score(y_raw[lo:hi], y_pred_cv_raw[lo:hi]))

    r2_vals   = list(street_r2.values())
    median_r2 = float(np.median(r2_vals))
    log.info(f"  Overall GroupKFold R2 (log scale): {overall_r2_log:.3f}")
    log.info(f"  Per-street R2 -- median: {median_r2:.3f}  "
             f"range: [{min(r2_vals):.3f}, {max(r2_vals):.3f}]")
    log.info(f"  Streets with R2 >= 0.6: {sum(v >= 0.6 for v in r2_vals)}/{len(r2_vals)}")

    # ── Final model on all sensored data ──────────────────────────────────────
    log.info("  Training final model (n_estimators=500)...")
    final_model = xgb.XGBRegressor(
        n_estimators=500, max_depth=5, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
        tree_method="hist", random_state=42, verbosity=0,
    )
    final_model.fit(X_train, y_train)

    # ── Predict for unsensored streets — batched to avoid OOM ────────────────
    BATCH = 300
    log.info(f"  Predicting for {len(unsensored_streets)} unsensored streets "
             f"(batches of {BATCH})...")

    pred_sid_list, pred_time_list, pred_flow_list = [], [], []
    for b_start in range(0, len(unsensored_streets), BATCH):
        batch = unsensored_streets[b_start : b_start + BATCH]
        X_b   = _make_X(batch, time_index)
        p_b   = np.expm1(np.clip(final_model.predict(X_b), 0, None))
        for i, sid in enumerate(batch):
            pred_sid_list.append(np.full(len(time_index), sid))
            pred_time_list.append(time_index)
            pred_flow_list.append(p_b[i * len(time_index) : (i + 1) * len(time_index)])
        if (b_start // BATCH) % 5 == 0:
            log.info(f"    ... {min(b_start + BATCH, len(unsensored_streets))}"
                     f"/{len(unsensored_streets)} streets predicted")

    unsensored_grid = pd.DataFrame({
        "street_id": np.concatenate(pred_sid_list),
        "time_bin":  np.concatenate(pred_time_list),
        "ped_flow":  np.concatenate(pred_flow_list).astype(np.float32),
    })

    # ── Confidence tiers ──────────────────────────────────────────────────────
    ped_sensored["ped_confidence"] = 1.0
    ped_sensored["source"]         = "sensor"

    tier = 0.8 if overall_r2_log >= 0.6 else 0.5
    unsensored_grid["ped_confidence"] = tier
    unsensored_grid["source"]         = "xgboost"
    log.info(f"  Confidence tier for imputed streets: {tier} (R²={overall_r2_log:.3f})")

    ped_complete = pd.concat([
        ped_sensored[["street_id", "time_bin", "ped_flow", "ped_confidence", "source"]],
        unsensored_grid[["street_id", "time_bin", "ped_flow", "ped_confidence", "source"]],
    ], ignore_index=True)

    log.info(f"  ped_complete: {len(ped_complete):,} rows, {ped_complete.street_id.nunique()} streets")
    return ped_complete, overall_r2_log, street_r2


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
    ped_complete, r2, sr2    = _build_ped_complete(parking_occ)

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

    median_r2 = float(np.median(list(sr2.values())))
    log.info(
        f"Step 5 complete: parking={parking_occ.street_id.nunique()} streets, "
        f"ped={ped_complete.street_id.nunique()} streets, "
        f"GroupKFold_R2={r2:.3f}, median_per_street_R2={median_r2:.3f}"
    )
    return {"parking_occupancy": p_path, "ped_complete": ped_path, "ped_summary": summ_path}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    run()
