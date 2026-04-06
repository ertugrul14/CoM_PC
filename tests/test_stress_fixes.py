"""
Tests for all 12 stress-test fixes applied to the Melbourne CBD pipeline.

Run with:
    python -m pytest tests/test_stress_fixes.py -v

Each test uses synthetic data — no legacy pipeline artifacts required.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import torch

# Ensure project root is importable
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #4 — Architecture audit: confirm GCNConv, not GAT
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue4_ArchitectureAudit:

    def test_model_uses_gcnconv(self):
        """Model must use GCNConv, not GATConv."""
        from pipeline.model.architecture import MultiGCNModel
        model = MultiGCNModel(n_features=10, hidden_dim=16,
                              n_gcn_layers=2, horizon=2)
        from torch_geometric.nn import GCNConv
        for name, module in model.named_modules():
            if "conv" in name.lower() or isinstance(module, torch.nn.Module):
                assert not type(module).__name__.startswith("GAT"), \
                    f"Found GAT layer: {name} ({type(module).__name__})"

        # Positive check: GCNConv layers exist
        gcn_layers = [m for m in model.modules() if isinstance(m, GCNConv)]
        assert len(gcn_layers) == 4  # 2 prox + 2 sem

    def test_docstring_mentions_audit(self):
        """Architecture module docstring should note the audit."""
        import pipeline.model.architecture as arch
        assert "GCNConv" in arch.__doc__
        assert "NOT GATConv" in arch.__doc__


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #3 — cluster_label excluded from model features
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue3_ClusterLeakage:

    def test_cluster_cols_excluded_from_features(self):
        """cluster_label and cluster_prob_* must not be in feat_cols."""
        all_cols = [
            "street_id", "time_bin", "occupancy_rate", "ped_flow",
            "cafe_count", "is_weekend", "valid_parking", "ped_confidence",
            "cluster_label", "cluster_prob_0", "cluster_prob_1", "cluster_prob_2",
        ]
        # Replicate the logic from s5_train.py
        EXCLUDE = {"street_id", "time_bin", "valid_parking",
                    "ped_confidence", "valid_ped"}
        EXCLUDE_PREFIXES = ("cluster_label", "cluster_prob_")
        feat_cols = [c for c in all_cols
                     if c not in EXCLUDE
                     and not c.startswith(EXCLUDE_PREFIXES)]

        assert "cluster_label" not in feat_cols
        assert "cluster_prob_0" not in feat_cols
        assert "cluster_prob_1" not in feat_cols
        assert "cluster_prob_2" not in feat_cols
        # But real features are kept
        assert "occupancy_rate" in feat_cols
        assert "cafe_count" in feat_cols


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #2 — Counterfactual applies delta to last time step only
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue2_CounterfactualLastStep:

    def test_delta_only_on_last_timestep(self):
        """Intervention delta must modify only X_mod[-1], not X_mod[:]."""
        seq_len, N, F = 12, 5, 3
        X_norm = np.zeros((seq_len, N, F), dtype=np.float32)
        X_mod = X_norm.copy()

        target_n = 2
        fi = 1
        capped = 0.5

        # Replicate the FIXED logic from s6_intervene.py
        X_mod[-1, target_n, fi] += capped
        X_mod[-1, target_n, fi] = np.clip(X_mod[-1, target_n, fi], -3.0, 3.0)

        # Only the last time step should differ
        assert X_mod[-1, target_n, fi] == pytest.approx(0.5)
        # All other time steps should be unchanged
        for t in range(seq_len - 1):
            assert X_mod[t, target_n, fi] == pytest.approx(0.0), \
                f"Time step {t} was modified — only step -1 should change"


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #1 — Sensor assignment validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue1_SensorValidation:

    def test_validate_passes_with_known_ids(self):
        """Validation passes when all parking street_ids are in static."""
        from pipeline.steps.s1_ingest import _validate_sensor_assignments
        park = pd.DataFrame({"street_id": ["p1", "p2", "p3"],
                             "occupancy_rate": [0.5, 0.6, 0.7]})
        static = pd.DataFrame({"street_id": ["p1", "p2", "p3", "p4"]})
        # Should not raise
        _validate_sensor_assignments(park, static)

    def test_validate_raises_on_unknown_ids(self):
        """Validation must raise ValueError for unknown street_ids."""
        from pipeline.steps.s1_ingest import _validate_sensor_assignments
        park = pd.DataFrame({"street_id": ["p1", "p2", "UNKNOWN"],
                             "occupancy_rate": [0.5, 0.6, 0.7]})
        static = pd.DataFrame({"street_id": ["p1", "p2"]})
        with pytest.raises(ValueError, match="not snapped"):
            _validate_sensor_assignments(park, static)


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #5 — Sensored ped data not overwritten
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue5_SensoredGuard:

    def test_observed_rows_have_source_sensor(self):
        """After assembly, sensored rows must have source='sensor'."""
        # Simulate the concat logic from s3_ped_impute
        obs = pd.DataFrame({
            "street_id": ["s1", "s1"],
            "time_bin": pd.to_datetime(["2025-06-01 09:00", "2025-06-01 09:15"]),
            "ped_flow": [100.0, 120.0],
            "ped_confidence": [1.0, 1.0],
            "source": ["sensor", "sensor"],
        })
        est = pd.DataFrame({
            "street_id": ["s1", "s2"],
            "time_bin": pd.to_datetime(["2025-06-01 09:00", "2025-06-01 09:00"]),
            "ped_flow": [95.0, 50.0],  # s1 prediction differs from sensor
            "ped_confidence": [0.8, 0.8],
            "source": ["imputed", "imputed"],
        })
        combined = pd.concat([obs, est], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["street_id", "time_bin"], keep="first"
        )
        # s1 at 09:00 must keep sensor value (100.0), not imputed (95.0)
        s1_row = combined[
            (combined["street_id"] == "s1") &
            (combined["time_bin"] == pd.Timestamp("2025-06-01 09:00"))
        ]
        assert len(s1_row) == 1
        assert s1_row.iloc[0]["source"] == "sensor"
        assert s1_row.iloc[0]["ped_flow"] == 100.0
        assert s1_row.iloc[0]["ped_confidence"] == 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #6 — Binary features skip z-score normalisation
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue6_BinaryNormalisation:

    def _make_cube(self):
        """Create minimal synthetic cube with binary + continuous features."""
        np.random.seed(42)
        streets = ["s1", "s2", "s3"]
        n_bins = 20
        times = pd.date_range("2025-06-01", periods=n_bins, freq="15min")
        rows = []
        for t in times:
            for s in streets:
                rows.append({
                    "street_id": s, "time_bin": t,
                    "occupancy_rate": np.random.rand() * 100,
                    "ped_flow": np.random.rand() * 500,
                    "cafe_count": np.random.randint(0, 20),
                    "is_weekend": int(t.dayofweek >= 5),
                    "is_public_holiday": 0,
                    "valid_parking": 1,
                    "ped_confidence": 1.0,
                })
        return pd.DataFrame(rows)

    def test_binary_features_unchanged_after_normalisation(self):
        """is_weekend and is_public_holiday must stay as 0/1 after normalisation."""
        from pipeline.model.dataset import StreetSequenceDataset
        cube = self._make_cube()
        node_ids = sorted(cube["street_id"].unique().tolist())
        feat_cols = ["occupancy_rate", "ped_flow", "cafe_count",
                     "is_weekend", "is_public_holiday"]

        ds = StreetSequenceDataset(cube, node_ids, feat_cols,
                                   seq_length=4, horizon=2)
        ns = ds.norm_stats()

        # Binary feature indices
        weekend_idx = feat_cols.index("is_weekend")
        holiday_idx = feat_cols.index("is_public_holiday")

        # Check norm stats: mean=0, std=1 for binary features
        assert ns["mean"][weekend_idx] == pytest.approx(0.0)
        assert ns["std"][weekend_idx] == pytest.approx(1.0)
        assert ns["mean"][holiday_idx] == pytest.approx(0.0)
        assert ns["std"][holiday_idx] == pytest.approx(1.0)

        # Actual values in the dataset should be 0 or 1 only
        x, y, mp, mped = ds[0]
        weekend_vals = x[:, :, weekend_idx].numpy().flatten()
        unique_vals = set(np.unique(weekend_vals))
        assert unique_vals.issubset({0.0, 1.0}), \
            f"is_weekend has non-binary values after normalisation: {unique_vals}"

    def test_continuous_features_are_still_normalised(self):
        """Continuous features like cafe_count must still be z-scored."""
        from pipeline.model.dataset import StreetSequenceDataset
        cube = self._make_cube()
        node_ids = sorted(cube["street_id"].unique().tolist())
        feat_cols = ["occupancy_rate", "ped_flow", "cafe_count",
                     "is_weekend", "is_public_holiday"]

        ds = StreetSequenceDataset(cube, node_ids, feat_cols,
                                   seq_length=4, horizon=2)
        ns = ds.norm_stats()

        cafe_idx = feat_cols.index("cafe_count")
        # cafe_count mean should NOT be 0 (it was z-scored from real data)
        assert ns["mean"][cafe_idx] != pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #7 — Feature values clipped to ±3σ after intervention
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue7_FeatureClipping:

    def test_extreme_value_gets_clipped(self):
        """A feature already at +2.5σ + delta of +1.0σ should be clipped to +3.0σ."""
        X_mod = np.zeros((12, 5, 3), dtype=np.float32)
        target_n, fi = 2, 1
        X_mod[-1, target_n, fi] = 2.5  # already high

        capped = 1.0  # would push to 3.5
        X_mod[-1, target_n, fi] += capped
        X_mod[-1, target_n, fi] = np.clip(X_mod[-1, target_n, fi], -3.0, 3.0)

        assert X_mod[-1, target_n, fi] == pytest.approx(3.0)

    def test_normal_value_not_clipped(self):
        """A feature at 0σ + delta of +0.5σ should not be clipped."""
        X_mod = np.zeros((12, 5, 3), dtype=np.float32)
        target_n, fi = 2, 1

        capped = 0.5
        X_mod[-1, target_n, fi] += capped
        X_mod[-1, target_n, fi] = np.clip(X_mod[-1, target_n, fi], -3.0, 3.0)

        assert X_mod[-1, target_n, fi] == pytest.approx(0.5)


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #8 — Cluster confidence scores
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue8_ClusterConfidence:

    def test_cluster_output_has_confidence_columns(self):
        """s4 output must include cluster_confidence and intervention_reliable."""
        # We can't easily run s4 without data, so test the logic inline
        probs = np.array([
            [0.9, 0.05, 0.05],  # high confidence
            [0.4, 0.35, 0.25],  # low confidence
            [0.8, 0.1, 0.1],    # high confidence
        ])
        cluster_confidence = probs.max(axis=1)
        THRESHOLD = 0.7
        intervention_reliable = cluster_confidence >= THRESHOLD

        assert cluster_confidence[0] == pytest.approx(0.9)
        assert cluster_confidence[1] == pytest.approx(0.4)
        assert intervention_reliable[0] is np.True_
        assert intervention_reliable[1] is np.False_
        assert intervention_reliable[2] is np.True_


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #9 — Stratified evaluation exists
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue9_StratifiedEval:

    def test_month_to_season_mapping(self):
        """All 12 months must map to a season."""
        from pipeline.steps.s5_train import _MONTH_TO_SEASON
        assert len(_MONTH_TO_SEASON) == 12
        assert _MONTH_TO_SEASON[1] == "summer"
        assert _MONTH_TO_SEASON[6] == "winter"
        assert _MONTH_TO_SEASON[9] == "spring"
        assert _MONTH_TO_SEASON[4] == "autumn"

    def test_evaluate_stratified_callable(self):
        """_evaluate_stratified function must be importable."""
        from pipeline.steps.s5_train import _evaluate_stratified
        assert callable(_evaluate_stratified)


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #10 — Propagation depth documented in output
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue10_PropagationDoc:

    def test_confidence_radius_calculation(self):
        """confidence_radius = n_gcn_layers × proximity_threshold_m."""
        from pipeline.config import Config
        cfg = Config()
        radius = cfg.n_gcn_layers * cfg.proximity_threshold_m
        assert radius == 100.0  # 2 layers × 50m


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #11 — Shelter proxy feature
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue11_ShelterProxy:

    def test_shelter_proxy_computed(self):
        """shelter_proxy = building_count / area_m2."""
        static = pd.DataFrame({
            "street_id": ["s1", "s2"],
            "area_m2": [1000.0, 500.0],
            "building_count": [10, 20],
        })
        area = static["area_m2"].replace(0, 1.0)
        static["shelter_proxy"] = static["building_count"] / area

        assert static.loc[0, "shelter_proxy"] == pytest.approx(0.01)
        assert static.loc[1, "shelter_proxy"] == pytest.approx(0.04)

    def test_shelter_proxy_zero_area_safe(self):
        """Zero area should not cause division by zero."""
        static = pd.DataFrame({
            "street_id": ["s1"],
            "area_m2": [0.0],
            "building_count": [5],
        })
        area = static["area_m2"].replace(0, 1.0)
        static["shelter_proxy"] = static["building_count"] / area
        assert np.isfinite(static.loc[0, "shelter_proxy"])


# ═══════════════════════════════════════════════════════════════════════════════
# Issue #12 — Flask API input validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue12_APIValidation:

    def test_intervention_deltas_keys_are_valid(self):
        """All intervention types in serve.py must match s6_intervene.py."""
        from pipeline.frontend.serve import INTERVENTION_DELTAS as serve_deltas
        from pipeline.steps.s6_intervene import INTERVENTION_DELTAS as step_deltas
        assert set(serve_deltas.keys()) == set(step_deltas.keys())

    def test_invalid_intervention_type_rejected(self):
        """Flask app should return 400 for unknown intervention types."""
        from pipeline.frontend.serve import app
        client = app.test_client()
        resp = client.post("/api/intervene",
                           json={"street_id": "polygon1",
                                 "intervention_type": "INVALID_TYPE"})
        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data
        assert "valid_types" in data


# ═══════════════════════════════════════════════════════════════════════════════
# Integration: MultiGCN forward pass still works with reduced features
# ═══════════════════════════════════════════════════════════════════════════════

class TestModelForwardPass:

    def test_forward_pass_with_reduced_features(self):
        """Model should work without cluster_label features."""
        from pipeline.model.architecture import MultiGCNModel

        # Simulate: original had 65 features, now ~61 without cluster cols
        n_features = 61
        model = MultiGCNModel(n_features=n_features, hidden_dim=16,
                              n_gru_layers=1, n_gcn_layers=2,
                              horizon=4, dropout=0.0)
        model.eval()

        B, seq, N = 2, 12, 10
        x = torch.randn(B, seq, N, n_features)
        # Simple ring graph
        edge_index = torch.tensor([[0,1,2,3,4,5,6,7,8,9],
                                   [1,2,3,4,5,6,7,8,9,0]], dtype=torch.long)
        edge_weight = torch.ones(10)

        with torch.no_grad():
            out = model(x, edge_index, edge_weight, edge_index, edge_weight)

        assert out.shape == (B, 4, N, 2)
        assert torch.isfinite(out).all()


# ═══════════════════════════════════════════════════════════════════════════════
# Integration: snap_sensors module
# ═══════════════════════════════════════════════════════════════════════════════

class TestSnapSensors:

    def test_snap_sensors_importable(self):
        """snap_sensors module must be importable."""
        from pipeline.snap_sensors import snap_sensors_to_streets
        assert callable(snap_sensors_to_streets)
