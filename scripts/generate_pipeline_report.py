"""
Generate PDF Report for Melbourne Street Analysis Pipeline.

Creates a comprehensive documentation of all pipeline modules.
"""
from fpdf import FPDF
from datetime import datetime
from pathlib import Path


class PipelineReportPDF(FPDF):
    """Custom PDF class with header/footer."""
    
    def header(self):
        self.set_font('Helvetica', 'B', 12)
        self.cell(0, 10, 'Melbourne Street Analysis Pipeline - Technical Documentation', 0, 1, 'C')
        self.ln(5)
    
    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', 0, 0, 'C')
    
    def chapter_title(self, title: str):
        self.set_font('Helvetica', 'B', 14)
        self.set_fill_color(230, 230, 230)
        self.cell(0, 10, title, 0, 1, 'L', fill=True)
        self.ln(4)
    
    def section_title(self, title: str):
        self.set_font('Helvetica', 'B', 12)
        self.set_text_color(50, 50, 150)
        self.cell(0, 8, title, 0, 1, 'L')
        self.set_text_color(0, 0, 0)
        self.ln(2)
    
    def subsection_title(self, title: str):
        self.set_font('Helvetica', 'BI', 11)
        self.cell(0, 7, title, 0, 1, 'L')
        self.ln(1)
    
    def body_text(self, text: str):
        self.set_font('Helvetica', '', 10)
        self.multi_cell(0, 5, text)
        self.ln(2)
    
    def code_block(self, text: str):
        self.set_font('Courier', '', 9)
        self.set_fill_color(245, 245, 245)
        self.multi_cell(0, 4, text, border=0, fill=True)
        self.set_font('Helvetica', '', 10)
        self.ln(3)
    
    def bullet_point(self, text: str, indent: int = 0):
        self.set_font('Helvetica', '', 10)
        self.set_x(self.l_margin + indent)
        self.cell(5, 5, chr(149), 0, 0)  # bullet character
        self.multi_cell(0, 5, text)


def generate_report():
    """Generate the complete pipeline documentation PDF."""
    
    pdf = PipelineReportPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # ============================================================
    # TITLE PAGE
    # ============================================================
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 24)
    pdf.ln(60)
    pdf.cell(0, 15, 'Melbourne Street Analysis Pipeline', 0, 1, 'C')
    pdf.set_font('Helvetica', '', 16)
    pdf.cell(0, 10, 'Technical Documentation', 0, 1, 'C')
    pdf.ln(20)
    pdf.set_font('Helvetica', '', 12)
    pdf.cell(0, 8, 'Complete Pipeline Architecture and Module Reference', 0, 1, 'C')
    pdf.ln(40)
    pdf.set_font('Helvetica', 'I', 10)
    pdf.cell(0, 6, f'Generated: {datetime.now().strftime("%B %d, %Y")}', 0, 1, 'C')
    pdf.cell(0, 6, 'Version 3.0.0', 0, 1, 'C')
    
    # ============================================================
    # TABLE OF CONTENTS
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('Table of Contents')
    pdf.set_font('Helvetica', '', 11)
    
    toc = [
        ('1. Executive Summary', 3),
        ('2. Pipeline Architecture Overview', 4),
        ('3. Core Module (complete_pipeline/core/)', 5),
        ('4. Data Module (complete_pipeline/data/)', 8),
        ('5. Processing Module (complete_pipeline/processing/)', 11),
        ('6. Analysis Module (complete_pipeline/analysis/)', 14),
        ('7. Forecasting Module (complete_pipeline/forecasting/)', 17),
        ('8. Simulation Module (complete_pipeline/simulation/)', 20),
        ('9. API Module (complete_pipeline/api/)', 23),
        ('10. Real-World Applications', 25),
        ('11. Future Improvements', 27),
    ]
    
    for item, page in toc:
        pdf.cell(0, 8, f'{item}{"." * (60 - len(item))} {page}', 0, 1)
    
    # ============================================================
    # EXECUTIVE SUMMARY
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('1. Executive Summary')
    
    pdf.body_text(
        'The Melbourne Street Analysis Pipeline is a comprehensive data processing system '
        'designed to analyze urban mobility patterns in Melbourne CBD. It processes millions '
        'of parking sensor events and pedestrian counts to identify temporal patterns, cluster '
        'streets by behavior, and simulate the effects of street space reallocations.'
    )
    
    pdf.section_title('Key Capabilities')
    pdf.bullet_point('Process 5.8+ million pedestrian records and 1.1+ million parking events')
    pdf.bullet_point('Reconstruct parking occupancy states from raw sensor events')
    pdf.bullet_point('Create street "fingerprints" showing temporal activity patterns')
    pdf.bullet_point('Cluster streets by behavioral similarity (Retail, Commuter, Quiet)')
    pdf.bullet_point('Score reallocation opportunities based on flexibility and demand')
    pdf.bullet_point('Simulate spillover effects using gravity-based models')
    pdf.bullet_point('Forecast future demand using Spatio-Temporal Graph Neural Networks')
    
    pdf.section_title('Real-World Purpose')
    pdf.body_text(
        'City planners face difficult decisions about reallocating street space - converting parking '
        'to bike lanes, creating pedestrian plazas, or adding loading zones. This pipeline provides '
        'data-driven insights by answering: "Which streets have temporal flexibility for change?" '
        'and "What are the likely spillover effects on neighboring streets?"'
    )
    
    pdf.section_title('Technical Stack')
    pdf.bullet_point('Python 3.10+ with pandas, numpy for data processing')
    pdf.bullet_point('scikit-learn for clustering and regression analysis')
    pdf.bullet_point('PyTorch + PyTorch Geometric for ST-GNN forecasting (optional)')
    pdf.bullet_point('Flask for REST API server')
    pdf.bullet_point('Supabase PostgreSQL for data storage')
    pdf.bullet_point('GeoJSON polygons for street geometries')
    
    # ============================================================
    # PIPELINE ARCHITECTURE
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('2. Pipeline Architecture Overview')
    
    pdf.section_title('Pipeline Steps')
    pdf.body_text(
        'The pipeline executes in 6 sequential steps, each building on the previous:'
    )
    
    pdf.subsection_title('Step 1: FETCH')
    pdf.body_text(
        'Loads raw sensor data from local Parquet files (bulk download) or fetches from '
        'Supabase PostgreSQL database. Handles 5.8M+ pedestrian records and 1.1M+ parking events. '
        'Uses ID-based pagination for reliable large data transfers.'
    )
    
    pdf.subsection_title('Step 2: PROCESS')
    pdf.body_text(
        'Transforms raw events into structured data cubes. Parking events (arrivals/departures) '
        'are reconstructed into occupancy time series. Data is aggregated to 15-minute bins per street. '
        '"Shape fingerprints" normalize patterns to 0-100 scale; "magnitude metrics" preserve absolute counts.'
    )
    
    pdf.subsection_title('Step 3: ANALYZE')
    pdf.body_text(
        'Performs statistical analysis including: K-Means clustering to group streets by temporal '
        'pattern, association analysis between POI density and activity levels, and opportunity '
        'scoring to identify streets with high flexibility and demand for reallocation.'
    )
    
    pdf.subsection_title('Step 4: FORECAST')
    pdf.body_text(
        'Builds a street adjacency graph from polygon geometries and constructs a data cube for '
        'the ST-GNN model. When training is enabled, trains a Spatio-Temporal GNN that models '
        'how activity propagates between neighboring streets over time.'
    )
    
    pdf.subsection_title('Step 5: SIMULATE')
    pdf.body_text(
        'Uses a gravity-based model to simulate what happens when street capacity is reduced. '
        'Calculates displaced demand, distributes it to neighbors based on distance and capacity, '
        'and provides confidence intervals via Monte Carlo simulation.'
    )
    
    pdf.subsection_title('Step 6: EXPORT')
    pdf.body_text(
        'Writes all results to the output directory and creates a manifest.json for consumption '
        'by the frontend dashboard or external systems.'
    )
    
    pdf.section_title('Directory Structure')
    pdf.code_block(
        'complete_pipeline/\n'
        '  run.py             # Main orchestrator\n'
        '  core/              # Configuration, logging, constants\n'
        '  data/              # Data fetching and sensor mapping\n'
        '  processing/        # Aggregation, clustering, state reconstruction\n'
        '  analysis/          # Associations, opportunities, static features\n'
        '  forecasting/       # Graph, data cube, ST-GNN model\n'
        '  simulation/        # Gravity model, reallocation, spillover\n'
        '  api/               # Flask REST API server'
    )
    
    # ============================================================
    # CORE MODULE
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('3. Core Module (complete_pipeline/core/)')
    
    pdf.body_text(
        'The core module provides foundational infrastructure used by all other modules: '
        'configuration management, logging, constants, and utility functions.'
    )
    
    pdf.section_title('3.1 config.py - Configuration Management')
    pdf.body_text(
        'Implements a dataclass-based configuration system that serves as the single source '
        'of truth for all pipeline settings. Uses environment variables for sensitive data '
        'like Supabase credentials.'
    )
    
    pdf.subsection_title('Key Configuration Classes:')
    pdf.bullet_point('SupabaseConfig: Database connection settings (URL, key, table names)')
    pdf.bullet_point('PathConfig: File and directory paths computed from base directory')
    pdf.bullet_point('TimeConfig: Temporal settings (bin size, time blocks, max gaps)')
    pdf.bullet_point('ClusteringConfig: K-Means parameters (n_clusters, stability thresholds)')
    pdf.bullet_point('ModelConfig: ST-GNN hyperparameters (hidden dim, attention heads)')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'Configuration centralization prevents bugs from inconsistent settings. For example, '
        'if one module used 15-minute bins and another used 30-minute, aggregations would fail. '
        'TimeConfig.bin_minutes=15 ensures all modules align.'
    )
    
    pdf.section_title('3.2 constants.py - Enumerations and Magic Numbers')
    pdf.body_text(
        'Defines enums for categorical values used throughout the pipeline. This prevents '
        'typos and enables IDE autocomplete.'
    )
    
    pdf.subsection_title('Key Enumerations:')
    pdf.bullet_point('TimeBlocks: NIGHT (0-6), MORNING (6-9), WORK_AM (9-12), AFTERNOON (12-17), EVENING (17-21), NIGHTLIFE (21-24)')
    pdf.bullet_point('ClusterLabels: COMMUTER, NIGHTLIFE, ALWAYS_BUSY, QUIET, MIXED')
    pdf.bullet_point('DataQualityTier: HIGH, MEDIUM, LOW, UNKNOWN')
    pdf.bullet_point('InterventionType: PEDESTRIANIZE, REDUCE_CAPACITY, TIME_RESTRICT, etc.')
    pdf.bullet_point('CausalEvidenceLevel: EXPERIMENTAL, QUASI_EXPERIMENTAL, CORRELATIONAL, NONE')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'Time blocks represent how urban planners think about the day. "Morning rush" means '
        'commuters arriving; "nightlife" means entertainment district activity. Cluster labels '
        'assign intuitive names to statistically-derived groups.'
    )
    
    pdf.section_title('3.3 logging.py - Structured Logging')
    pdf.body_text(
        'Provides structured logging with JSON output for machine parsing and colored console '
        'output for human readability. Includes performance timing decorators.'
    )
    
    pdf.subsection_title('Key Features:')
    pdf.bullet_point('@log_timing decorator: Automatically logs function execution time')
    pdf.bullet_point('RunTracker: Collects warnings and errors for governance reporting')
    pdf.bullet_point('Structured JSON logs for pipeline monitoring and debugging')
    
    # ============================================================
    # DATA MODULE
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('4. Data Module (complete_pipeline/data/)')
    
    pdf.body_text(
        'The data module handles all interactions with external data sources: fetching from '
        'Supabase, mapping sensors to streets, and validating data quality.'
    )
    
    pdf.section_title('4.1 fetcher.py - Data Fetching')
    pdf.body_text(
        'Implements SERVER-SIDE datetime filtering for reproducible data fetching. Uses ID-based '
        'pagination with cursor to handle millions of rows without timeouts.'
    )
    
    pdf.subsection_title('Key Classes:')
    pdf.bullet_point('DataFetcher: Base class with Supabase client and pagination logic')
    pdf.bullet_point('ParkingFetcher: Fetches from parking_melbourne table')
    pdf.bullet_point('PedestrianFetcher: Fetches from ped_melbourne table')
    
    pdf.subsection_title('Fetching Strategy:')
    pdf.code_block(
        '1. Query by datetime range (server-side filter)\n'
        '2. Order by ID ascending\n'
        '3. Fetch 10,000 rows per request\n'
        '4. Use last ID as cursor for next page\n'
        '5. Retry with exponential backoff on failure'
    )
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'Melbourne has 5.8 million pedestrian counts and 1.1 million parking events. Fetching all '
        'at once would timeout. Day-by-day pagination with ID cursors ensures reliable complete downloads. '
        'Server-side filtering prevents "data leakage" where future data accidentally enters training sets.'
    )
    
    pdf.section_title('4.2 sensor_mapping.py - Sensor to Street Mapping')
    pdf.body_text(
        'Maps individual sensors to street polygons. Parking sensors (kerbsideid) and pedestrian '
        'sensors (location_id) are matched to their containing street geometry.'
    )
    
    pdf.subsection_title('Key Methods:')
    pdf.bullet_point('get_parking_street(kerbsideid): Returns street_id for a parking sensor')
    pdf.bullet_point('get_pedestrian_street(location_id): Returns street_id for a ped sensor')
    pdf.bullet_point('get_sensors_for_street(street_id): Returns all sensors on a street')
    pdf.bullet_point('map_parking_data(df): Joins DataFrame with street mapping')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'Raw data comes with sensor IDs like "C1234" (parking bay) or "Bou292b" (pedestrian counter). '
        'To analyze at street level, we need to know which street each sensor belongs to. The mapping '
        'file was created by spatially joining sensor coordinates with street polygon geometries.'
    )
    
    pdf.section_title('4.3 schemas.py - Data Validation')
    pdf.body_text(
        'Defines expected column schemas for parking and pedestrian data. Used to validate '
        'data after fetching and before processing.'
    )
    
    pdf.section_title('4.4 validation.py - Quality Checks')
    pdf.body_text(
        'Implements data quality validation including: missing value detection, outlier flagging, '
        'coverage verification (are all days represented?), and schema conformance.'
    )
    
    # ============================================================
    # PROCESSING MODULE
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('5. Processing Module (complete_pipeline/processing/)')
    
    pdf.body_text(
        'The processing module transforms raw sensor events into structured analytics-ready data. '
        'This is where the heavy computation happens.'
    )
    
    pdf.section_title('5.1 state_reconstruction.py - Parking State Reconstruction')
    pdf.body_text(
        'Converts parking events (arrivals/departures) into occupancy time series. This is the '
        'most computationally intensive step, optimized from 6+ hours to ~84 seconds using vectorized pandas.'
    )
    
    pdf.subsection_title('The Challenge:')
    pdf.body_text(
        'Parking sensors only record state CHANGES - "car arrived" or "car left". To know occupancy '
        'at any given time, we must reconstruct the timeline: each "arrived" event creates occupancy '
        'until the next "departed" event.'
    )
    
    pdf.subsection_title('Algorithm:')
    pdf.code_block(
        '1. Sort events by (kerbsideid, timestamp)\n'
        '2. Calculate duration = next_event_time - current_time\n'
        '3. Cap durations at 24 hours (assume sensor offline)\n'
        '4. Assign events to 15-minute time bins\n'
        '5. Expand events spanning multiple bins\n'
        '6. Aggregate occupied_seconds per (street, time_bin)\n'
        '7. Calculate occupancy_rate = occupied / (bin_seconds * n_sensors)'
    )
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'If a parking sensor shows "Present" at 10:23 and "Unoccupied" at 11:45, that bay was occupied '
        'for 82 minutes. This affects the 10:15, 10:30, 10:45, 11:00, 11:15, 11:30, and 11:45 time bins. '
        'Aggregating across all sensors on a street gives the overall occupancy rate for that street.'
    )
    
    pdf.section_title('5.2 aggregator.py - Fingerprint Generation')
    pdf.body_text(
        'Creates two separate output types from processed data: shape fingerprints (normalized patterns) '
        'and magnitude metrics (absolute counts).'
    )
    
    pdf.subsection_title('Shape Fingerprints:')
    pdf.body_text(
        'Normalized to 0-100 based on each street\'s own maximum. This allows pattern comparison: '
        '"Street A peaks in morning, Street B peaks in evening" regardless of absolute traffic levels. '
        'Uses ped_night, ped_morning, ped_work_am, ped_afternoon, ped_evening, ped_nightlife columns.'
    )
    
    pdf.subsection_title('Magnitude Metrics:')
    pdf.body_text(
        'Preserves absolute counts: ped_total_count, ped_peak_count, ped_mean_count. Essential for '
        'demand assessment - a quiet street with flexibility is NOT an opportunity if no one uses it.'
    )
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'A street might have shape=[10, 80, 100, 60, 30, 5] meaning activity peaks mid-morning. '
        'But if magnitude is only 200 people/day vs 50,000/day on another street, the reallocation '
        'implications are vastly different. Shape tells "when", magnitude tells "how much".'
    )
    
    pdf.section_title('5.3 clustering.py - Street Clustering')
    pdf.body_text(
        'Groups streets by temporal behavior using K-Means clustering. Streets with similar '
        'shape fingerprints end up in the same cluster.'
    )
    
    pdf.subsection_title('Clustering Process:')
    pdf.bullet_point('Extract shape features (ped_night through ped_nightlife)')
    pdf.bullet_point('Standardize features (zero mean, unit variance)')
    pdf.bullet_point('Run K-Means with n_clusters=3')
    pdf.bullet_point('Compute silhouette score (target: > 0.5)')
    pdf.bullet_point('Run stability test with bootstrap (target: ARI > 0.7)')
    pdf.bullet_point('Assign interpretable labels (Retail, Commuter, Quiet)')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'Clusters represent archetypal street behaviors:\n'
        '- Retail: Busy afternoon/evening, shopping patterns\n'
        '- Commuter: Morning/evening peaks, office workers\n'
        '- Quiet: Low activity throughout, residential/service streets'
    )
    
    # ============================================================
    # ANALYSIS MODULE
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('6. Analysis Module (complete_pipeline/analysis/)')
    
    pdf.body_text(
        'The analysis module performs statistical analysis to understand relationships between '
        'street features and activity patterns, and to score reallocation opportunities.'
    )
    
    pdf.section_title('6.1 associations.py - Feature-Activity Associations')
    pdf.body_text(
        'CRITICAL: This module explicitly labels results as ASSOCIATIONS, not causal relationships. '
        'Cross-sectional variation (comparing different streets) cannot prove causation.'
    )
    
    pdf.subsection_title('Analysis Methods:')
    pdf.bullet_point('Ridge Regression: Linear associations with regularization')
    pdf.bullet_point('Random Forest: Non-linear relationships and feature importance')
    pdf.bullet_point('Cross-Validation: Honest R-squared scores (out-of-sample)')
    pdf.bullet_point('Permutation Importance: Robust feature ranking')
    
    pdf.subsection_title('Features Analyzed:')
    pdf.bullet_point('dist_flinders, dist_southern_cross, dist_townhall: Distance to stations')
    pdf.bullet_point('poi_corporate, poi_nightlife, poi_retail, poi_transport: POI densities')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'Finding "streets near Flinders Station have higher morning activity" is an ASSOCIATION. '
        'It does NOT mean adding a train station will increase activity - the causality could run '
        'the other direction (busy streets attracted transit investment). The code explicitly '
        'warns users about this limitation.'
    )
    
    pdf.section_title('6.2 opportunities.py - Opportunity Scoring')
    pdf.body_text(
        'Identifies streets with reallocation potential by combining flexibility (from shape) '
        'with demand (from magnitude). This prevents labeling "quiet streets" as opportunities.'
    )
    
    pdf.subsection_title('Scoring Formula:')
    pdf.code_block(
        'final_score = flexibility_score * demand_weight * quality_weight\n\n'
        'Where:\n'
        '  flexibility_score = 0.4*capacity_gap + 0.3*variance + 0.3*low_conflict\n'
        '  demand_weight = log(ped_total_count) / max_log_count\n'
        '  quality_weight = DataQualityTier.get_weight()'
    )
    
    pdf.subsection_title('Flexibility Metrics:')
    pdf.bullet_point('temporal_variance: How much activity varies across time blocks')
    pdf.bullet_point('capacity_gap: 100 - min_occupancy (room for intervention)')
    pdf.bullet_point('best_intervention_window: Time block with lowest activity')
    pdf.bullet_point('conflict_score: Average of two lowest time blocks')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'A street with 90% occupancy all day has NO flexibility for a lunchtime parklet. '
        'A street with morning peak (90%) but afternoon lull (20%) has HIGH flexibility - the '
        'afternoon could support temporary pedestrianization without displacing much demand.'
    )
    
    pdf.section_title('6.3 static_features.py - Static Feature Generation')
    pdf.body_text(
        'Builds time-invariant features for each street from external data sources.'
    )
    
    pdf.subsection_title('Feature Sources:')
    pdf.bullet_point('Street polygon geometries: Centroid coordinates')
    pdf.bullet_point('POI GeoJSON: Business/amenity counts within 50m buffer')
    pdf.bullet_point('Landmark coordinates: Distances to train stations, town hall')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'Static features help explain WHY streets have certain patterns. A street 50m from '
        'Southern Cross Station will likely have commuter patterns; a street with 20 restaurants '
        'will likely have evening activity. These features feed association analysis.'
    )
    
    # ============================================================
    # FORECASTING MODULE
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('7. Forecasting Module (complete_pipeline/forecasting/)')
    
    pdf.body_text(
        'The forecasting module implements spatio-temporal prediction using graph neural networks. '
        'This is an OPTIONAL advanced feature requiring PyTorch and PyTorch Geometric.'
    )
    
    pdf.section_title('7.1 graph.py - Neighbor Graph Construction')
    pdf.body_text(
        'Builds a street adjacency graph where edges connect neighboring streets. Uses polygon '
        'geometry (touching boundaries) rather than simple distance.'
    )
    
    pdf.subsection_title('Graph Properties:')
    pdf.bullet_point('Nodes: Streets (216 in Melbourne CBD)')
    pdf.bullet_point('Edges: Adjacency relationships (980 edges)')
    pdf.bullet_point('Edge weights: Centroid distance in meters')
    pdf.bullet_point('Format: JSON for storage, PyTorch Geometric for training')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'Streets don\'t exist in isolation. If Bourke Street is congested, drivers may divert to '
        'Collins Street. The neighbor graph captures which streets are adjacent and thus likely to '
        'experience spillover. Graph structure is essential for the ST-GNN to learn these patterns.'
    )
    
    pdf.section_title('7.2 cube.py - Data Cube Construction')
    pdf.body_text(
        'Creates a unified 3D tensor (time x streets x features) for ST-GNN input. Combines '
        'parking occupancy, pedestrian flow, temporal features, and static features.'
    )
    
    pdf.subsection_title('Data Cube Contents:')
    pdf.bullet_point('occupancy_rate: Parking occupancy (0-1)')
    pdf.bullet_point('ped_flow: Pedestrian count per time bin')
    pdf.bullet_point('hour_sin, hour_cos: Cyclical hour encoding')
    pdf.bullet_point('dow_sin, dow_cos: Cyclical day-of-week encoding')
    pdf.bullet_point('is_weekend: Binary weekend indicator')
    pdf.bullet_point('Static features: POI counts, distances')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'The data cube represents "what was happening on every street at every time". The model '
        'learns patterns like "occupancy on Street A at 10:00 predicts flow on Street B at 10:15". '
        'Cyclical encodings help the model understand that 11pm and 1am are closer than 11pm and 5pm.'
    )
    
    pdf.section_title('7.3 model.py - ST-GNN Architecture')
    pdf.body_text(
        'Defines a Spatio-Temporal Graph Neural Network combining temporal (GRU) and spatial (GAT) '
        'components. CRITICAL: This model learns historical PATTERNS, not intervention EFFECTS.'
    )
    
    pdf.subsection_title('Architecture:')
    pdf.code_block(
        '1. Temporal Encoder (GRU):\n'
        '   - Processes time series per node\n'
        '   - Captures weekly/daily patterns\n\n'
        '2. Spatial Encoder (GAT):\n'
        '   - Aggregates neighbor information\n'
        '   - Attention weights learn importance\n\n'
        '3. Fusion Layer:\n'
        '   - Combines temporal + spatial features\n\n'
        '4. Predictor:\n'
        '   - Outputs multi-step forecast'
    )
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'The model can predict "given activity patterns from 8am-11am, what will 12pm-1pm look like?" '
        'But it CANNOT predict "what happens if we remove parking" because it only learns from '
        'historical data where parking existed. That\'s why we need the simulation module.'
    )
    
    # ============================================================
    # SIMULATION MODULE
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('8. Simulation Module (complete_pipeline/simulation/)')
    
    pdf.body_text(
        'The simulation module provides counterfactual analysis: "What IF we change something?" '
        'It uses physics-inspired gravity models rather than ML predictions.'
    )
    
    pdf.section_title('8.1 gravity.py - Gravity Model')
    pdf.body_text(
        'Implements demand redistribution based on distance and capacity. When parking is removed, '
        'displaced demand flows to nearby streets with available capacity.'
    )
    
    pdf.subsection_title('Gravity Formula:')
    pdf.code_block(
        'attraction = (available_capacity) / (distance ^ alpha)\n\n'
        'Where:\n'
        '  available_capacity = capacity * (1 - occupancy)\n'
        '  distance = meters between street centroids\n'
        '  alpha = distance decay exponent (default: 0.5)'
    )
    
    pdf.subsection_title('Parameters:')
    pdf.bullet_point('alpha_distance_decay: 0.5 (higher = faster decay with distance)')
    pdf.bullet_point('max_reallocation_distance: 500m (beyond this, demand leaves system)')
    pdf.bullet_point('cruising_fraction: 0.7 (fraction willing to search for parking)')
    pdf.bullet_point('max_capacity_utilization: 0.95 (streets can\'t fill above 95%)')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'If you remove 10 parking spots at 80% occupancy, 8 cars need to go somewhere. '
        'Maybe 6 cruise to nearby streets (cruising_fraction=0.7), and 2 leave (destination '
        'becomes inaccessible). Of the 6, more will go to the closer Street A (50m away) than '
        'distant Street C (400m away) due to distance decay.'
    )
    
    pdf.section_title('8.2 reallocation.py - Reallocation Simulator')
    pdf.body_text(
        'Orchestrates simulation of street space changes with uncertainty quantification.'
    )
    
    pdf.subsection_title('Simulation Process:')
    pdf.bullet_point('1. Calculate displaced demand from capacity reduction')
    pdf.bullet_point('2. Identify neighbor streets within max_reallocation_distance')
    pdf.bullet_point('3. Calculate attraction scores using gravity model')
    pdf.bullet_point('4. Distribute displaced demand proportionally')
    pdf.bullet_point('5. Run Monte Carlo (100 iterations) varying parameters')
    pdf.bullet_point('6. Return point estimates + confidence intervals')
    
    pdf.subsection_title('Confidence Tiers:')
    pdf.bullet_point('HIGH: Prediction error <= 10%')
    pdf.bullet_point('MEDIUM: Prediction error 10-25%')
    pdf.bullet_point('LOW: Prediction error > 25%')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'Results come with explicit disclaimers: "These are MODEL-BASED SCENARIOS, not predictions." '
        'The confidence intervals acknowledge that actual behavior depends on factors we can\'t model - '
        'driver preferences, event days, weather, etc.'
    )
    
    pdf.section_title('8.3 spillover.py - Spillover Analysis')
    pdf.body_text(
        'Aggregates simulation results and calculates net impact metrics.'
    )
    
    pdf.subsection_title('Impact Metrics:')
    pdf.bullet_point('reach: Number of affected streets')
    pdf.bullet_point('concentration: Is effect spread evenly or concentrated?')
    pdf.bullet_point('intensity: Average magnitude of effects')
    pdf.bullet_point('absorption_rate: Fraction of displaced demand absorbed by neighbors')
    
    # ============================================================
    # API MODULE
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('9. API Module (complete_pipeline/api/)')
    
    pdf.body_text(
        'The API module provides a Flask-based REST API for frontend consumption. It exposes '
        'pipeline results without requiring clients to understand the internal data structures.'
    )
    
    pdf.section_title('9.1 server.py - Flask Application')
    pdf.body_text(
        'Creates a Flask app with CORS enabled for browser access. Serves JSON responses '
        'from pipeline output files.'
    )
    
    pdf.subsection_title('Endpoints:')
    pdf.code_block(
        'GET /api/health\n'
        '  -> {"status": "ok", "version": "3.0.0"}\n\n'
        'GET /api/opportunities?min_score=0.5&limit=50\n'
        '  -> {"opportunities": [...], "count": 28}\n\n'
        'GET /api/clusters/pedestrian\n'
        '  -> {"clusters": {"Retail": {...}, "Quiet": {...}}}\n\n'
        'POST /api/simulate {"target": "Bourke St", "reduction": 0.5}\n'
        '  -> {"spillover_effects": {...}, "confidence": "medium"}'
    )
    
    pdf.section_title('9.2 endpoints.py - Endpoint Functions')
    pdf.body_text(
        'Standalone functions that can be called from the server or imported directly. '
        'Handles data loading, filtering, and transformation.'
    )
    
    pdf.subsection_title('Key Functions:')
    pdf.bullet_point('get_opportunities(): Load and filter opportunity scores')
    pdf.bullet_point('get_street_clusters(): Get cluster assignments by data type')
    pdf.bullet_point('get_spillover_prediction(): Run reallocation simulation on demand')
    
    pdf.subsection_title('Real-World Meaning:')
    pdf.body_text(
        'The API decouples the heavy Python pipeline from the web frontend. A JavaScript dashboard '
        'can call "/api/opportunities" without needing pandas installed. Planners can explore '
        '"what-if" scenarios through the /simulate endpoint.'
    )
    
    # ============================================================
    # REAL-WORLD APPLICATIONS
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('10. Real-World Applications')
    
    pdf.section_title('10.1 Identifying Reallocation Candidates')
    pdf.body_text(
        'City planners can query: "Which streets have temporal flexibility AND sufficient demand '
        'to justify investment?" The opportunity scoring combines both criteria.'
    )
    
    pdf.subsection_title('Example Query:')
    pdf.code_block(
        '# Streets suitable for lunchtime parklet\n'
        'df[df["best_intervention_window"] == "afternoon"]\n'
        '  .sort_values("opportunity_score", ascending=False)\n'
        '  .head(10)'
    )
    
    pdf.section_title('10.2 Predicting Spillover Effects')
    pdf.body_text(
        'Before removing parking on Street A, planners can estimate impacts on Streets B, C, D. '
        'If spillover would push Street B above 95% occupancy, the plan needs modification.'
    )
    
    pdf.subsection_title('Example Query:')
    pdf.code_block(
        'result = simulate_reallocation(\n'
        '    target_street="Bourke Street",\n'
        '    intervention_type="parking_removal",\n'
        '    capacity_reduction_fraction=0.5\n'
        ')\n'
        'print(result.spillover_effects)  # {"Collins St": 12.3, ...}'
    )
    
    pdf.section_title('10.3 Understanding Street Types')
    pdf.body_text(
        'Clustering reveals which streets share behavioral patterns. All "Retail" streets might '
        'benefit from similar policies; all "Commuter" streets have similar peak-hour challenges.'
    )
    
    pdf.section_title('10.4 Data-Driven Policy Arguments')
    pdf.body_text(
        'Rather than intuition-based decisions, planners can present: "This street has 94% capacity '
        'gap during afternoon, 15,000 weekly pedestrians, and was classified as Retail-type based '
        'on objective clustering. Estimated spillover affects 3 streets within 200m."'
    )
    
    pdf.section_title('10.5 Limitations and Honest Uncertainty')
    pdf.body_text(
        'The pipeline explicitly labels correlational vs causal findings, provides confidence '
        'intervals, and includes disclaimers on simulation outputs. This supports honest '
        'communication with stakeholders about what we know vs what we\'re estimating.'
    )
    
    # ============================================================
    # FUTURE IMPROVEMENTS
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('11. Future Improvements')
    
    pdf.section_title('11.1 Model Calibration')
    pdf.body_text(
        'The gravity model uses default parameters (alpha=0.5) that should be calibrated with '
        'local before/after studies. Melbourne-specific data would improve accuracy.'
    )
    
    pdf.section_title('11.2 Event Detection')
    pdf.body_text(
        'Major events (sports, concerts) cause anomalies not captured by regular patterns. '
        'An event calendar integration would improve forecasting reliability.'
    )
    
    pdf.section_title('11.3 Real-Time Updates')
    pdf.body_text(
        'Currently the pipeline runs in batch mode. A streaming version could update fingerprints '
        'and forecasts as new sensor data arrives.'
    )
    
    pdf.section_title('11.4 Causal Inference')
    pdf.body_text(
        'When before/after data from actual interventions becomes available, causal inference '
        'methods (difference-in-differences) could replace or validate the gravity model.'
    )
    
    pdf.section_title('11.5 Multi-Modal Integration')
    pdf.body_text(
        'Adding public transit ridership, bike share data, and taxi/rideshare pickups would give '
        'a more complete picture of street activity.'
    )
    
    # ============================================================
    # APPENDIX
    # ============================================================
    pdf.add_page()
    pdf.chapter_title('Appendix: Running the Pipeline')
    
    pdf.section_title('Installation')
    pdf.code_block(
        'cd D:\\melbourne_ingestor\n'
        'python -m venv .venv\n'
        '.venv\\Scripts\\activate\n'
        'pip install -r complete_pipeline/requirements.txt'
    )
    
    pdf.section_title('Basic Usage')
    pdf.code_block(
        '# Run all steps\n'
        'python -m complete_pipeline.run --steps all\n\n'
        '# Run specific steps\n'
        'python -m complete_pipeline.run --steps fetch process analyze\n\n'
        '# Train ST-GNN model\n'
        'python -m complete_pipeline.run --steps forecast --train\n\n'
        '# Run simulation\n'
        'python -m complete_pipeline.run --steps simulate \\\n'
        '    --target-street "Bourke Street" \\\n'
        '    --intervention parking_removal \\\n'
        '    --reduction 0.5'
    )
    
    pdf.section_title('Output Files')
    pdf.code_block(
        'complete_pipeline/output/\n'
        '  parking_fingerprints.csv    # Shape patterns + clusters\n'
        '  magnitude_metrics.csv       # Absolute counts\n'
        '  opportunities.csv           # Scored opportunities\n'
        '  static_features.csv         # POI + distance features\n'
        '  neighbor_graph.json         # Street adjacency\n'
        '  data_cube.parquet           # ST-GNN training data\n'
        '  manifest.json               # Output file listing'
    )
    
    # ============================================================
    # SAVE PDF
    # ============================================================
    output_path = Path(__file__).resolve().parent.parent / 'complete_pipeline' / 'output' / 'Pipeline_Documentation.pdf'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(output_path))
    
    print(f"PDF report generated: {output_path}")
    return output_path


if __name__ == '__main__':
    generate_report()
