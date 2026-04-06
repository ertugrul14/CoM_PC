"""
Generate a 10-page PDF technical report for the Melbourne CBD Street Analysis Pipeline.
Uses fpdf2 -no external system dependencies.
"""
import sys
from pathlib import Path

from fpdf import FPDF

OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)


class Report(FPDF):
    page_count_override = 10

    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(120)
        self.cell(0, 6, "Melbourne CBD Street Analysis Pipeline -Technical Report", align="L")
        self.cell(0, 6, f"Page {self.page_no()}", align="R", new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def footer(self):
        pass

    def section_title(self, text, level=1):
        if level == 1:
            self.set_font("Helvetica", "B", 14)
            self.set_text_color(25, 60, 120)
        elif level == 2:
            self.set_font("Helvetica", "B", 11)
            self.set_text_color(40, 80, 140)
        else:
            self.set_font("Helvetica", "B", 10)
            self.set_text_color(60, 60, 60)
        self.ln(2)
        self.cell(0, 7, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body(self, text):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 4.5, text)
        self.ln(1)

    def kv_table(self, rows, col_widths=(55, 125)):
        self.set_font("Helvetica", "", 8.5)
        for i, (k, v) in enumerate(rows):
            fill = i % 2 == 0
            if fill:
                self.set_fill_color(240, 243, 248)
            self.set_font("Helvetica", "B", 8.5)
            self.set_text_color(40, 40, 40)
            self.cell(col_widths[0], 5.5, str(k), border=0, fill=fill)
            self.set_font("Helvetica", "", 8.5)
            self.set_text_color(30, 30, 30)
            self.cell(col_widths[1], 5.5, str(v), border=0, fill=fill, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def bullet(self, text):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(30, 30, 30)
        x = self.get_x()
        self.cell(6, 4.5, "- ")
        self.multi_cell(0, 4.5, text)
        self.ln(0.5)


def build_report():
    pdf = Report(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.set_margins(15, 15, 15)

    # ======================================================================
    # PAGE 1 -Title
    # ======================================================================
    pdf.add_page()
    pdf.ln(50)
    pdf.set_font("Helvetica", "B", 24)
    pdf.set_text_color(20, 50, 100)
    pdf.cell(0, 12, "Melbourne CBD", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 12, "Street Analysis Pipeline", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)
    pdf.set_font("Helvetica", "", 13)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 8, "Technical Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(15)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100)
    pdf.cell(0, 6, "Spatio-Temporal Graph Neural Network Pipeline v4.0", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, "12-Step Architecture  |  3,975 Street Segments  |  150-Day Study Window", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(30)
    pdf.set_font("Helvetica", "I", 9)
    pdf.cell(0, 5, "Study period: 1 November 2025 - 30 March 2026", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, "Time resolution: 15-minute bins (14,400 total)", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, "Data sources: Supabase (parking, pedestrian), Melbourne Open Data (CLUE), Open-Meteo (weather)", align="C", new_x="LMARGIN", new_y="NEXT")

    # ======================================================================
    # PAGE 2 -Architecture Overview
    # ======================================================================
    pdf.add_page()
    pdf.section_title("1. Pipeline Architecture")
    pdf.body(
        "The pipeline processes raw sensor and land-use data through 12 sequential steps, "
        "producing a trained Spatio-Temporal GCN model with intervention recommendations per street. "
        "Steps 1-4 are implemented and validated. Steps 5-7 are in active development. Steps 8-12 are specified."
    )

    pdf.section_title("Step Registry", 2)
    steps = [
        ("Step 01 -Fetch", "Raw data ingestion from 3 APIs (14 datasets)"),
        ("Step 02 -Snap", "Sensor snapping + CLUE spatial aggregation (17-col static features)"),
        ("Step 03 -Temporal", "Temporal cyclic encoding + weather forward-fill"),
        ("Step 04 -Graph", "Spatial (k-NN) + semantic (mutual cosine) graph construction"),
        ("Step 05 -Process", "Parking occupancy reconstruction + XGBoost ped imputation"),
        ("Step 06 -Aggregate", "Aggregate ped + parking to street-level temporal profiles"),
        ("Step 07 -Cluster", "GMM clustering on full street character"),
        ("Step 08 -Cube", "Data cube assembly + dual graph refinement"),
        ("Step 09 -Train", "MultiGCN training (GRU encoder + dual GCN branches)"),
        ("Step 10 -Interpret", "Permutation feature importance + branch contribution"),
        ("Step 11 -Scenario", "Scenario-based intervention simulation"),
        ("Step 12 -Export", "Export enriched frontend JSON"),
    ]
    pdf.kv_table(steps, col_widths=(38, 142))

    pdf.section_title("Key Design Parameters", 2)
    pdf.kv_table([
        ("Study window", "2025-11-01 09:00 UTC to 2026-03-30 09:00 UTC (150 days)"),
        ("Time bins", "15-minute intervals, 14,400 total bins"),
        ("Street geometry", "3,975 official road-corridor segments (Melbourne Open Data)"),
        ("CRS", "EPSG:3111 (GDA94 / Victorian) for spatial operations, WGS84 for storage"),
        ("Confidence tiers", "1.0 (sensor) / 0.8 (R2 >= 0.6) / 0.5 (R2 < 0.6)"),
    ])

    # ======================================================================
    # PAGE 3 -Step 01: Fetch
    # ======================================================================
    pdf.add_page()
    pdf.section_title("2. Step 01 -Raw Data Ingestion")
    pdf.body(
        "Fetches 14 datasets from three sources: Supabase (parking events, pedestrian counts), "
        "Melbourne Open Data CLUE API (11 land-use datasets + block geometries), and Open-Meteo "
        "(hourly weather archive). All outputs saved to data/raw/ as Parquet or GeoJSON."
    )

    pdf.section_title("Data Sources", 2)
    pdf.kv_table([
        ("Supabase -parking", "~1.47M parking sensor events (Present/Unoccupied status changes)"),
        ("Supabase -pedestrian", "~1.18M pedestrian counter readings (directional counts)"),
        ("Open-Meteo -weather", "Hourly: temperature, humidity, wind speed, precipitation"),
        ("CLUE -cafe/restaurant", "Venues with seating counts (indoor + outdoor rows)"),
        ("CLUE -bars", "Licensed venues with patron capacity"),
        ("CLUE -business", "Business premises with ANZSIC industry codes"),
        ("CLUE -buildings", "Building footprints with addresses"),
        ("CLUE -dwellings", "Residential dwelling counts per address"),
        ("CLUE -landmarks", "Points of interest (POI)"),
        ("CLUE -off-street parking", "Off-street parking spaces per location"),
        ("CLUE -jobs", "Block-level employment totals"),
        ("CLUE -floorspace", "Block-level retail + office floorspace (m2)"),
        ("CLUE -blocks", "Block polygon geometries (GeoJSON)"),
    ])

    pdf.section_title("API Pagination Strategy", 2)
    pdf.body(
        "Melbourne Open Data v2.1 silently caps the offset parameter at 10,000. Datasets exceeding "
        "this limit (business, buildings, dwellings) are handled by a three-stage strategy: (1) direct "
        "fetch if total <= 10K; (2) filter by latest census_year; (3) if still > 10K, split by block_id "
        "into 6 ranges with < 10K rows each. Supabase uses keyset pagination (id > last_id) with a "
        "batch size of 1,000, avoiding the offset cap entirely."
    )

    # ======================================================================
    # PAGE 4 -Step 02: Snap
    # ======================================================================
    pdf.add_page()
    pdf.section_title("3. Step 02 -Sensor Snapping + Static Features")

    pdf.section_title("Part A: Sensor Snapping", 2)
    pdf.body(
        "Every unique parking bay and pedestrian counter is assigned to its street polygon via a "
        "two-pass spatial join: (1) point-in-polygon test; (2) nearest-neighbour fallback for "
        "unmatched sensors (max distance 25m). Pedestrian sensor coordinates are fetched live from "
        "the Melbourne Open Data sensor-locations API. Parking match rate: 100%. Pedestrian match "
        "rate: ~95% (unmatched sensors lack API coordinates)."
    )

    pdf.section_title("Part B: CLUE Spatial Aggregation", 2)
    pdf.body(
        "Seven point datasets are joined to streets using sjoin_nearest (max 50m) -the wider "
        "tolerance accounts for CLUE addresses sitting at building frontages rather than road centroids. "
        "Two block-level datasets (jobs, floorspace) use area-weighted polygon intersection. "
        "Cafe/restaurant rows are pre-deduplicated: indoor and outdoor seating entries for the same "
        "venue (matched by trading_name + lat + lon) are collapsed into a single row with summed seats."
    )

    pdf.section_title("Output: static_features.parquet (17 columns)", 2)
    pdf.kv_table([
        ("Geometry", "area_m2, centroid_lat, centroid_lon"),
        ("Employment", "total_jobs (area-weighted from blocks)"),
        ("Hospitality", "cafe_count, cafe_total_seats, bar_count, bar_patron_capacity, dining_capacity"),
        ("Commercial", "business_count (excl. ANZSIC 4000-4599 hospitality)"),
        ("Built environment", "building_count, dwelling_count, offstreet_spaces"),
        ("Floorspace", "retail_floorspace, office_floorspace (area-weighted)"),
        ("Points of interest", "poi_total (landmarks)"),
    ])

    # ======================================================================
    # PAGE 5 -Step 03: Temporal
    # ======================================================================
    pdf.add_page()
    pdf.section_title("4. Step 03 -Temporal Encoding + Weather")

    pdf.body(
        "Generates two flat parquets aligned to a master 15-minute UTC time index (14,400 bins). "
        "Temporal features use cyclic encoding to preserve continuity at boundaries (23:45 is close "
        "to 00:00; Sunday is close to Monday)."
    )

    pdf.section_title("Temporal Features (7 columns)", 2)
    pdf.kv_table([
        ("hour_sin / hour_cos", "Cyclic 24-hour encoding: sin(2pi * hour/24), cos(2pi * hour/24)"),
        ("dow_sin / dow_cos", "Cyclic 7-day encoding: sin(2pi * dow/7), cos(2pi * dow/7)"),
        ("is_weekend", "Binary: Saturday or Sunday"),
        ("is_public_holiday", "6 Victorian public holidays in study window"),
        ("is_school_holiday", "3 school holiday ranges (Dec-Jan, Easter, term breaks)"),
    ])

    pdf.section_title("Weather Features (4 columns)", 2)
    pdf.body(
        "Hourly Open-Meteo data is forward-filled to 15-minute bins: temperature_2m (float32, "
        "validated -5 to 45C), relative_humidity_2m, wind_speed_10m, and precipitation (non-negative). "
        "Forward-fill handles the 4x upsampling (1h -> 15min) without introducing interpolation artifacts."
    )

    pdf.section_title("Validation", 2)
    pdf.bullet("Cyclic identity: sin2 + cos2 = 1 for all rows (atol=1e-5)")
    pdf.bullet("Spot checks: specific dates verified against calendar")
    pdf.bullet("Weather bounds: temperature in [-5, 45], precipitation >= 0")
    pdf.bullet("No NaN values in either output parquet")

    # ======================================================================
    # PAGE 6 -Step 04: Graph (Part A)
    # ======================================================================
    pdf.add_page()
    pdf.section_title("5. Step 04 -Graph Construction")
    pdf.body(
        "Constructs two graph topologies over all 3,975 streets. Both graphs are stored as "
        "bidirectional edge lists for GCN message passing. A stable 0-based integer node index "
        "maps street_id to tensor indices."
    )

    pdf.section_title("Part A: Spatial Graph", 2)
    pdf.body(
        "Each street is connected to its k=8 nearest neighbours by Euclidean distance between "
        "centroids (WGS84 projected to approximate metres via equirectangular projection centred "
        "on Melbourne CBD). Edge weight uses a Gaussian kernel: w = exp(-d / sigma), where sigma "
        "is the median of all k-NN distances (adaptive scale). This ensures nearby streets have "
        "weight close to 1.0 and distant streets decay smoothly toward 0."
    )

    pdf.kv_table([
        ("k", "8 nearest neighbours"),
        ("sigma", "Median k-NN distance (~72m for Melbourne CBD)"),
        ("Edges", "37,670 bidirectional (18,835 unique pairs)"),
        ("Kernel", "Gaussian: exp(-dist_m / sigma)"),
    ])

    pdf.section_title("Part B: Semantic Graph", 2)
    pdf.body(
        "Connects streets with similar functional character based on 7 land-use activity features: "
        "total_jobs, cafe_count, cafe_total_seats, bar_count, bar_patron_capacity, business_count, "
        "poi_total. Structural features (area_m2, building_count) are excluded as they are non-zero "
        "for nearly all streets and would create spurious similarity between inactive streets."
    )

    pdf.section_title("Semantic Graph Methodology", 3)
    pdf.bullet("Streets with all-zero activity features are excluded (353 streets)")
    pdf.bullet("Features are z-scored (StandardScaler) then L2-normalized per row")
    pdf.bullet("Cosine similarity computed on the full active-street matrix")
    pdf.bullet("Top-k=5 neighbours selected per street")
    pdf.bullet("Mutual k-NN filter: edge kept only if BOTH streets chose each other as top-k")
    pdf.bullet("Minimum similarity threshold: 0.99 (near-identical functional profiles only)")

    # ======================================================================
    # PAGE 7 -Step 04: Graph (Part B) + Viz
    # ======================================================================
    pdf.add_page()
    pdf.section_title("5. Step 04 -Graph Construction (continued)")

    pdf.section_title("Semantic Graph Results", 2)
    pdf.kv_table([
        ("Active streets", "3,622 (out of 3,975 total)"),
        ("Feature set", "7 activity features (z-scored + L2-normalized)"),
        ("k", "5 (mutual k-NN)"),
        ("min_sim", "0.99"),
        ("Edges", "9,852 bidirectional (4,926 unique pairs)"),
        ("Similarity range", "[0.990, 1.000]"),
        ("Isolated streets", "564 (no semantic edges -low-activity or unique profile)"),
    ])

    pdf.section_title("Visualization Export", 2)
    pdf.body(
        "A GeoJSON file (graph_viz.geojson) is exported for the Mapbox GL JS interface with "
        "all spatial edges and the top 4,000 semantic edges by similarity weight. Line width and "
        "opacity are interpolated by edge weight. Both graph layers default to hidden and can be "
        "toggled independently in the control panel."
    )

    pdf.section_title("Graph Design Rationale", 2)
    pdf.bullet("Spatial graph captures local neighbourhood effects (nearby streets influence each other)")
    pdf.bullet("Semantic graph captures cross-distance functional similarity (similar land-use profiles)")
    pdf.bullet("Dual-graph architecture enables the GCN to learn both local and non-local patterns")
    pdf.bullet("Mutual k-NN prevents asymmetric edges where only one street benefits from the connection")
    pdf.bullet("High threshold (0.99) ensures only genuinely similar streets connect semantically")

    # ======================================================================
    # PAGE 8 -Step 05 + Step 06
    # ======================================================================
    pdf.add_page()
    pdf.section_title("6. Step 05 -Parking Occupancy + Pedestrian Imputation")

    pdf.section_title("Part A: Parking Occupancy Reconstruction", 2)
    pdf.body(
        "Raw parking sensor events (Present/Unoccupied status changes) are paired into occupied "
        "intervals per bay. Each interval is capped at 2 hours (7,200s). Intervals are expanded "
        "to 15-minute bins. Streets with fewer than 5 events are excluded."
    )
    pdf.kv_table([
        ("Input", "parking_raw.parquet (~1.47M events)"),
        ("Output", "parking_occupancy.parquet: 171 streets x 14,400 bins = 2,462,400 rows"),
    ])

    pdf.section_title("Part B: XGBoost Pedestrian Imputation (v3)", 2)
    pdf.body(
        "A global XGBoost model (n_estimators=500, max_depth=5, lr=0.05) trained on 82 sensored "
        "streets predicts pedestrian flow for 3,893 unsensored streets. Target: log1p(ped_flow). "
        "Feature matrix: 30 columns (17 static + 3 parking stats + 7 temporal + 4 weather). "
        "Validated via GroupKFold(5) over streets (holds out entire streets, not time slices)."
    )
    pdf.kv_table([
        ("GroupKFold R2 (v3)", "0.571 (log scale); per-street median: 0.162"),
        ("Confidence tiers", "1.0 (sensor), 0.8 (R2>=0.6), 0.5 (R2<0.6)"),
        ("Output", "ped_complete.parquet: 3,975 x 14,400 = 57,240,000 rows"),
    ])

    pdf.section_title("7. Step 06 -Aggregate Street Profiles")
    pdf.body(
        "Rolls ped_complete and parking_occupancy into per-street temporal profiles that "
        "characterise each street's activity rhythm. These profiles become the input for GMM "
        "clustering in Step 07, ensuring clusters reflect actual usage patterns."
    )
    pdf.kv_table([
        ("Inputs", "ped_complete.parquet, parking_occupancy.parquet"),
        ("Ped features", "Mean/peak flow by hour-of-day, weekday vs weekend, CoV"),
        ("Parking features", "Mean/peak occupancy by hour, turnover rate, weekend ratio"),
        ("Output", "street_profiles.parquet: 1 row per street, ~50 aggregated columns"),
    ])

    # ======================================================================
    # PAGE 9 -Steps 07, 08, 09
    # ======================================================================
    pdf.add_page()
    pdf.section_title("8. Step 07 -GMM Clustering")
    pdf.body(
        "Gaussian Mixture Model clustering on the full street character: static land-use "
        "features + pedestrian activity profiles + parking occupancy profiles. This is the "
        "richest possible feature set, combining what a street IS (land use) with how it "
        "BEHAVES (temporal activity patterns)."
    )
    pdf.kv_table([
        ("Input", "street_profiles.parquet + static_features.parquet"),
        ("Preprocessing", "StandardScaler + PCA (retaining ~95% variance)"),
        ("k search", "BIC over k=2..10, constrained to k=3 if within 5% of minimum"),
        ("Stability", "Bootstrap ARI (30 iterations), target >= 0.70"),
        ("Labelling", "Archetype assignment by centroid peak: morning/midday/evening"),
        ("Output", "clustered.parquet: street_id, cluster, intervention_type, confidence"),
    ])

    pdf.section_title("9. Step 08 -Data Cube Assembly")
    pdf.body(
        "Assembles the (N, T, F) tensor for GCN input where N=streets, T=time bins, "
        "F=feature channels. Refines the dual graph from Step 04 with cluster-informed "
        "edge reweighting: edges between same-cluster streets receive a boost, enabling "
        "the GCN to leverage functional groupings during message passing."
    )
    pdf.kv_table([
        ("Input", "ped_complete, parking_occupancy, static_features, graph edges, clusters"),
        ("Cube shape", "(N, 14400, F) where F includes ped_flow, occupancy, static, temporal"),
        ("Graph refinement", "Intra-cluster edge weight *= 1.2, inter-cluster unchanged"),
        ("Output", "data_cube.parquet, refined spatial_edges.parquet, semantic_edges.parquet"),
    ])

    pdf.section_title("10. Step 09 -MultiGCN Training")
    pdf.body(
        "Spatio-Temporal Graph Neural Network with dual GCN branches (spatial + semantic) "
        "and a GRU temporal encoder. The spatial branch propagates local neighbourhood signals; "
        "the semantic branch propagates functional similarity signals across non-adjacent streets. "
        "MSE loss is weighted by confidence tiers from Step 05."
    )
    pdf.kv_table([
        ("Architecture", "GRU encoder -> dual GCNConv branches -> fusion -> prediction head"),
        ("Spatial branch", "2-layer GCNConv on spatial_edges (Gaussian kernel weights)"),
        ("Semantic branch", "2-layer GCNConv on semantic_edges (cosine similarity weights)"),
        ("Loss", "Confidence-weighted MSE: higher weight for sensor streets (conf=1.0)"),
        ("Output", "Trained model checkpoint (.pt), training metrics log"),
    ])

    # ======================================================================
    # PAGE 10 -Steps 10, 11, 12
    # ======================================================================
    pdf.add_page()
    pdf.section_title("11. Step 10 -Feature Interpretation")
    pdf.body(
        "Quantifies the contribution of each feature and graph branch to predictions. "
        "Permutation importance measures how much prediction quality degrades when each "
        "feature is randomly shuffled. Branch contribution analysis compares spatial-only "
        "vs semantic-only vs combined predictions to assess each graph's value."
    )
    pdf.kv_table([
        ("Method", "Permutation importance (n_repeats=10) on held-out test set"),
        ("Branch analysis", "Ablation: spatial-only R2, semantic-only R2, combined R2"),
        ("Output", "feature_importance.json, branch_contribution.json"),
    ])

    pdf.section_title("12. Step 11 -Scenario Simulation")
    pdf.body(
        "Counterfactual intervention simulation per street cluster. For each cluster archetype "
        "(morning pedestrianisation, midday retail activation, evening outdoor dining), the model "
        "predicts what would happen if parking occupancy were reduced during flexibility windows "
        "identified in Step 07. Flexibility windows are time blocks where pedestrian demand "
        "exceeds the cluster median but parking occupancy is below 30%."
    )
    pdf.kv_table([
        ("Input", "Trained model, clustered.parquet, data_cube.parquet"),
        ("Scenarios", "Per-cluster: reduce parking occupancy by 25%/50%/75% in flex windows"),
        ("Metrics", "Predicted ped_flow change, economic impact estimate"),
        ("Output", "scenarios.parquet, scenario_report.json"),
    ])

    pdf.section_title("13. Step 12 -Frontend Export")
    pdf.body(
        "Exports all pipeline outputs as enriched GeoJSON and JSON files for the Mapbox GL JS "
        "frontend. Each street polygon carries its full attribute set: cluster label, intervention "
        "type, predicted ped flow, parking occupancy, scenario results, and confidence scores. "
        "The frontend provides interactive toggle layers for each data dimension."
    )
    pdf.kv_table([
        ("Input", "All pipeline outputs (parquet + model)"),
        ("Street GeoJSON", "3,975 features with ~30 properties each"),
        ("Scenario overlays", "Per-cluster intervention heatmaps"),
        ("Graph edges", "Spatial + semantic edges as LineString GeoJSON"),
        ("Output", "frontend/data/*.geojson, frontend/data/*.json"),
    ])

    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(120)
    pdf.cell(0, 5, "Generated by generate_pipeline_report.py", align="C")

    return pdf


if __name__ == "__main__":
    pdf = build_report()
    out_path = OUT_DIR / "pipeline_report.pdf"
    pdf.output(str(out_path))
    print(f"Report saved: {out_path}  ({pdf.page_no()} pages)")
