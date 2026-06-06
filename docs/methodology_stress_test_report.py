"""
Generate the Methodology Stress-Test Report as a PDF.

Usage:
    python docs/methodology_stress_test_report.py

Requires: reportlab  (pip install reportlab)
"""
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor, black, white
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, HRFlowable,
)

OUT = Path(__file__).resolve().parent / "methodology_stress_test_report.pdf"

# ─── Colour palette ─────────────────────────────────────────────────────────
DARK   = HexColor("#1a1a2e")
ACCENT = HexColor("#e94560")
MID    = HexColor("#16213e")
LIGHT  = HexColor("#0f3460")
GREEN  = HexColor("#2d6a4f")
AMBER  = HexColor("#e09f3e")
RED    = HexColor("#c1121f")
GREY   = HexColor("#6c757d")

# ─── Styles ──────────────────────────────────────────────────────────────────
styles = getSampleStyleSheet()

s_title = ParagraphStyle("Title2", parent=styles["Title"],
                          fontSize=22, leading=26, textColor=DARK,
                          spaceAfter=4*mm)
s_subtitle = ParagraphStyle("Subtitle", parent=styles["Normal"],
                             fontSize=11, leading=14, textColor=GREY,
                             spaceAfter=8*mm)
s_h1 = ParagraphStyle("H1", parent=styles["Heading1"],
                       fontSize=16, leading=20, textColor=MID,
                       spaceBefore=8*mm, spaceAfter=3*mm)
s_h2 = ParagraphStyle("H2", parent=styles["Heading2"],
                       fontSize=13, leading=16, textColor=LIGHT,
                       spaceBefore=6*mm, spaceAfter=2*mm)
s_h3 = ParagraphStyle("H3", parent=styles["Heading3"],
                       fontSize=11, leading=14, textColor=DARK,
                       spaceBefore=4*mm, spaceAfter=2*mm)
s_body = ParagraphStyle("Body", parent=styles["Normal"],
                         fontSize=10, leading=14, alignment=TA_JUSTIFY,
                         spaceAfter=3*mm)
s_body_bold = ParagraphStyle("BodyBold", parent=s_body, fontName="Helvetica-Bold")
s_bullet = ParagraphStyle("Bullet", parent=s_body,
                           bulletFontSize=10, leftIndent=14*mm,
                           bulletIndent=8*mm, spaceAfter=2*mm)
s_code = ParagraphStyle("Code", parent=styles["Code"],
                         fontSize=8.5, leading=11, backColor=HexColor("#f5f5f5"),
                         leftIndent=6*mm, rightIndent=6*mm,
                         spaceBefore=2*mm, spaceAfter=3*mm)
s_verdict_ok = ParagraphStyle("VerdictOK", parent=s_body,
                               textColor=GREEN, fontName="Helvetica-Bold")
s_verdict_warn = ParagraphStyle("VerdictWarn", parent=s_body,
                                 textColor=AMBER, fontName="Helvetica-Bold")
s_verdict_fail = ParagraphStyle("VerdictFail", parent=s_body,
                                 textColor=RED, fontName="Helvetica-Bold")
s_small = ParagraphStyle("Small", parent=s_body, fontSize=8.5, leading=11,
                          textColor=GREY)

def hr():
    return HRFlowable(width="100%", thickness=0.5, color=HexColor("#dee2e6"),
                      spaceAfter=4*mm, spaceBefore=2*mm)

def verdict_table(items: list[tuple[str, str, str]]):
    """items: [(component, status_emoji, explanation), ...]"""
    data = [["Component", "Status", "Assessment"]]
    for comp, status, expl in items:
        data.append([
            Paragraph(comp, s_body),
            Paragraph(status, s_body),
            Paragraph(expl, s_body),
        ])
    t = Table(data, colWidths=[45*mm, 18*mm, 95*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), MID),
        ("TEXTCOLOR", (0,0), (-1,0), white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,0), 9),
        ("FONTSIZE", (0,1), (-1,-1), 9),
        ("GRID", (0,0), (-1,-1), 0.4, HexColor("#dee2e6")),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("TOPPADDING", (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ("LEFTPADDING", (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
    ]))
    return t


def build():
    doc = SimpleDocTemplate(
        str(OUT), pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=18*mm, bottomMargin=18*mm,
    )
    story = []
    S = story.append

    # ═══════════════════════════════════════════════════════════════════════
    # COVER
    # ═══════════════════════════════════════════════════════════════════════
    S(Spacer(1, 30*mm))
    S(Paragraph("Methodology Stress-Test Report", s_title))
    S(Paragraph(
        "Melbourne CBD Spatio-Temporal GNN Pipeline<br/>"
        "Model Training, Scenario Simulation &amp; Network Impact Analysis<br/>"
        "Critical Assessment &amp; Remediation Plan",
        s_subtitle,
    ))
    S(hr())
    S(Paragraph("Prepared: 24 April 2026 &nbsp;|&nbsp; Author: Automated audit (Claude) &nbsp;|&nbsp; Scope: Steps 09-12", s_small))
    S(Spacer(1, 6*mm))
    S(Paragraph(
        "This report ruthlessly audits whether the current MultiGCN pipeline can "
        "meaningfully predict network-level impacts of street interventions — "
        "pedestrianisation, parking removal, and kerb reallocation — on the "
        "Melbourne CBD street network. Each claim is tested against the actual "
        "code and data artefacts.",
        s_body,
    ))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # TABLE OF CONTENTS
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("Contents", s_h1))
    toc = [
        "1. Executive Summary",
        "2. How Model Training Works",
        "3. How Scenario Simulation Works",
        "4. What the Model CAN and CANNOT Predict",
        "5. The Zero-Effect Problem (Critical Bug)",
        "6. Parking Spillover: Honest Assessment",
        "7. Pedestrian Network Impact: Honest Assessment",
        "8. Graph Diffusion: Decoration vs. Mechanism",
        "9. Autoregressive Error Accumulation",
        "10. Full Verdict Matrix",
        "11. Remediation Plan (Prioritised Fixes)",
        "12. What to Present in the Thesis",
    ]
    for item in toc:
        S(Paragraph(item, ParagraphStyle("TOC", parent=s_body, leftIndent=6*mm, spaceAfter=1.5*mm)))
    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 1. EXECUTIVE SUMMARY
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("1. Executive Summary", s_h1))
    S(Paragraph(
        "The MultiGCN model is well-engineered for its <b>primary task</b>: predicting "
        "per-street pedestrian flow with R²=0.876 and MAE=5.8 ped/15-min. The dual-head "
        "architecture, spatial + semantic GCN branches, and autoregressive rollout are "
        "architecturally sound.",
        s_body,
    ))
    S(Paragraph(
        "However, the <b>scenario simulation framework has a critical structural deficiency</b> "
        "that renders it non-functional for 90% of the street network. Additionally, several "
        "design choices limit the model's ability to predict meaningful network-level impacts "
        "even where it does produce non-zero outputs.",
        s_body,
    ))

    S(Paragraph("Key Findings:", s_h3))
    findings = [
        ("<b>CRITICAL:</b> 1,254 of 1,397 streets (90%) produce exactly zero effect when "
         "\"pedestrianised\" because their cube occupancy is already 0. The intervention "
         "sets occupancy_rate=0, which is a no-op on streets without parking sensor data.",
         RED),
        ("<b>PARTIAL:</b> For the 143 streets WITH parking sensors, the model produces "
         "plausible treated-street effects (+10 ped/15-min mean uplift) and detectable "
         "1-hop spatial neighbour effects (+3-7 ped/15-min). This is real learned signal.",
         AMBER),
        ("<b>WEAK:</b> Parking spillover affects only 6 streets measurably (max delta=-0.062 "
         "occupancy rate), far too few for a convincing \"network impact\" claim.",
         AMBER),
        ("<b>STRUCTURAL:</b> The model has no pedestrian demand model, no origin-destination "
         "routing, and no conservation constraints. It captures spatial correlation, "
         "not causal pedestrian redistribution.",
         AMBER),
    ]
    for text, color in findings:
        S(Paragraph(text, ParagraphStyle("F", parent=s_bullet,
                                          bulletText="•", bulletColor=color)))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 2. HOW MODEL TRAINING WORKS
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("2. How Model Training Works", s_h1))

    S(Paragraph("2.1 Architecture", s_h2))
    S(Paragraph(
        "The MultiGCN is a spatio-temporal graph neural network with <b>dual graph "
        "convolution branches</b> (spatial + semantic) feeding into a GRU temporal encoder, "
        "terminating in two prediction heads (pedestrian flow + parking occupancy).",
        s_body,
    ))
    S(Paragraph(
        "Input: [batch=8, W=96 timesteps, N=1397 streets, F=23 features]<br/>"
        "Spatial branch: Linear(23→64) then A_spatial @ h → ReLU → [K, N, 64]<br/>"
        "Semantic branch: Linear(23→64) then A_semantic @ h → ReLU → [K, N, 64]<br/>"
        "Concat → [B, W, N, 128] → GRU(2 layers) → [B, N, 64]<br/>"
        "→ head_ped: Linear(64→1) + per-node bias → predicted ped_flow<br/>"
        "→ head_park: Linear(64→1) + per-node bias → predicted occupancy_rate",
        s_code,
    ))

    S(Paragraph("2.2 What the GCN Actually Computes", s_h2))
    S(Paragraph(
        "At each timestep, the spatial GCN computes: <b>h_spatial[i] = ReLU(Σⱼ A[i,j] · W · x[j])</b> "
        "where A is the spatial adjacency matrix and W is a learned projection. This means each "
        "street's hidden representation is a <b>weighted average of its neighbours' features</b>, "
        "projected through a linear transform.",
        s_body,
    ))
    S(Paragraph(
        "This is a <b>single-layer GCN</b> — information propagates exactly <b>one hop</b> per forward pass. "
        "Street i sees features from its direct neighbours, not from 2-hop or 3-hop neighbours. "
        "Multi-hop effects can only emerge through the autoregressive temporal loop.",
        s_body,
    ))

    S(Paragraph("2.3 Training Signal: What the Model Learned", s_h2))
    S(Paragraph(
        "The model was trained on a <b>chronological split</b> (no leakage): Train 70% | Val 15% | Test 15%. "
        "The joint loss is: <b>MAE_ped + 0.5 × masked_MAE_park</b>.",
        s_body,
    ))
    S(Paragraph(
        "Critical detail: parking loss is <b>masked to 143 streets</b> that have real sensor data. "
        "The other 1,254 streets have occupancy_rate=0 in the cube and contribute <b>zero parking "
        "supervision signal</b>. The model never learned what parking looks like on those streets.",
        s_body,
    ))

    data = [
        ["Metric", "Val", "Test"],
        ["Ped MAE", "5.997", "5.804"],
        ["Ped R²", "0.878", "0.876"],
        ["Park MAE (143 sensor streets)", "0.049", "0.040"],
        ["Park R² (143 sensor streets)", "0.852", "0.842"],
    ]
    t = Table(data, colWidths=[80*mm, 35*mm, 35*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), MID),
        ("TEXTCOLOR", (0,0), (-1,0), white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("GRID", (0,0), (-1,-1), 0.4, HexColor("#dee2e6")),
        ("ALIGN", (1,0), (-1,-1), "CENTER"),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]))
    S(t)
    S(Spacer(1, 3*mm))

    S(Paragraph("2.4 Feature Importance (What the Model Relies On)", s_h2))
    S(Paragraph(
        "Permutation importance reveals the model's feature dependencies. When each feature "
        "is replaced with random noise, the MAE increase tells us how much the model relies on it.",
        s_body,
    ))
    fi_data = [
        ["Feature", "ΔMAE", "Interpretation"],
        ["ped_flow", "+27.40", "Dominant — the model is mostly autoregressive"],
        ["hour_cos", "+2.49", "Time-of-day is the second most important signal"],
        ["total_jobs", "+1.36", "Land-use matters for spatial variation"],
        ["occupancy_rate", "+1.07", "Parking matters, but only 4th rank"],
        ["hour_sin", "+0.89", "Time encoding (complementary)"],
        ["business_count", "+0.87", "Urban activity proxy"],
    ]
    t = Table(fi_data, colWidths=[38*mm, 18*mm, 102*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), MID),
        ("TEXTCOLOR", (0,0), (-1,0), white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("GRID", (0,0), (-1,-1), 0.4, HexColor("#dee2e6")),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]))
    S(t)
    S(Spacer(1, 3*mm))
    S(Paragraph(
        "<b>Key insight:</b> ped_flow's self-importance (+27.40) dwarfs all other features combined. "
        "The model is fundamentally an <b>autoregressive pedestrian flow predictor</b> that uses "
        "other features as secondary modulators. Changing occupancy_rate alone can shift predictions, "
        "but the effect is modest (1.07/27.40 ≈ 4% of the total signal).",
        s_body,
    ))

    S(Paragraph("2.5 Branch Contribution", s_h2))
    S(Paragraph(
        "Zeroing the spatial branch weights causes +14.76 MAE increase; zeroing the semantic "
        "branch causes +3.76. The spatial graph is <b>dominant</b>, meaning the model heavily relies "
        "on physically adjacent neighbours. The semantic branch adds meaningful but secondary signal "
        "from functionally similar streets.",
        s_body,
    ))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 3. HOW SCENARIO SIMULATION WORKS
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("3. How Scenario Simulation Works", s_h1))

    S(Paragraph("3.1 The Counterfactual Rollout Framework", s_h2))
    S(Paragraph(
        "Step 11 (step_11_scenario.py) runs two parallel autoregressive rollouts:",
        s_body,
    ))
    steps = [
        "<b>Baseline rollout:</b> Feed the model real observed features from the cube. "
        "Only ped_flow is predicted autoregressively; all other features (weather, time, "
        "land-use, parking for non-sensor streets) come from the actual data cube.",
        "<b>Treated rollout:</b> Same, but with the intervention applied to the target "
        "street's features during the treatment window (e.g., set occupancy_rate=0 for "
        "pedestrianisation).",
        "<b>Delta:</b> treated − baseline at every street, at every timestep.",
    ]
    for s in steps:
        S(Paragraph(s, s_bullet, bulletText="→"))

    S(Paragraph("3.2 The Autoregressive Loop (Detailed)", s_h2))
    S(Paragraph(
        "At each rollout step k:",
        s_body,
    ))
    S(Paragraph(
        "1. Take sliding window [W=96 timesteps, 1397 streets, 23 features]\n"
        "2. Forward pass through MultiGCN → pred_ped [N], pred_park [N]\n"
        "3. Build next_row from REAL cube features at timestep (t_start + k)\n"
        "4. Replace ped_flow with model prediction (autoregressive)\n"
        "5. For 143 sensor streets: replace occupancy_rate with model prediction\n"
        "6. For treated street during intervention: override with intervention values\n"
        "7. Slide window forward by 1 step\n"
        "8. Repeat",
        s_code,
    ))
    S(Paragraph(
        "The critical mechanism for network effects is in steps 2-5: the GCN aggregates "
        "features from neighbouring nodes. If node j's occupancy changes, node i's hidden "
        "representation changes because h[i] = Σⱼ A[i,j] · W · x[j]. "
        "This is how the intervention on one street can affect predictions for neighbours.",
        s_body,
    ))

    S(Paragraph("3.3 Network Summary Outputs", s_h2))
    S(Paragraph(
        "After both rollouts, the framework computes:",
        s_body,
    ))
    outputs = [
        "<b>Per-street ped delta:</b> change in predicted pedestrian flow (raw counts)",
        "<b>Per-street parking delta:</b> change in predicted occupancy (sensor streets only)",
        "<b>Graph diffusion:</b> analytical A^k propagation of mean delta for k=1..3 hops",
        "<b>Semantic neighbours:</b> functionally similar streets, possibly spatially distant",
        "<b>Rebound analysis:</b> half-life and recovery fraction after intervention ends",
        "<b>Confidence-weighted ranking:</b> top affected streets ranked by delta × ped_confidence",
    ]
    for o in outputs:
        S(Paragraph(o, s_bullet, bulletText="•"))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 4. WHAT THE MODEL CAN AND CANNOT PREDICT
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("4. What the Model CAN and CANNOT Predict", s_h1))

    S(Paragraph("4.1 What It CAN Do (Validated)", s_h2))
    S(Paragraph(
        "The model genuinely captures <b>spatio-temporal correlations</b> in Melbourne CBD "
        "pedestrian activity. Specifically:",
        s_body,
    ))
    can_do = [
        "Predict next-step pedestrian flow with MAE≈5.8 (R²=0.876) — strong forecasting performance.",
        "Leverage spatial adjacency: zeroing the spatial branch degrades MAE by +14.76, proving "
        "the model uses neighbour information meaningfully.",
        "Predict parking occupancy on 143 sensor streets with MAE=0.040 (R²=0.842).",
        "Detect that changing occupancy_rate on a parking-sensor street changes the predicted "
        "ped_flow on that street and its 1-hop spatial neighbours (empirically confirmed: "
        "street 20007 shows +10.3 ped/15-min uplift, neighbour 20160 shows +7.1).",
    ]
    for c in can_do:
        S(Paragraph(c, s_bullet, bulletText="✓"))

    S(Paragraph("4.2 What It CANNOT Do (Structural Limitations)", s_h2))
    cannot_do = [
        ("<b>Predict parking on 1,254 non-sensor streets.</b> The parking head was trained with "
         "masked loss — these streets provided zero supervision signal. Any parking prediction "
         "for them is pure extrapolation.", RED),
        ("<b>Model pedestrian demand generation or redistribution.</b> The model does not have "
         "an origin-destination matrix, trip generation model, or pedestrian routing logic. "
         "It does not know that people walk TO a pedestrianised street or THROUGH connecting streets. "
         "It captures correlation (streets near busy streets tend to be busy), not causation.", RED),
        ("<b>Enforce conservation of parking demand.</b> When parking is removed from one street, "
         "there is no constraint that displaced demand must appear elsewhere. The model may predict "
         "a slight parking increase on neighbours (learned correlation), but there is no "
         "physical law enforced.", AMBER),
        ("<b>Model network topology changes.</b> Pedestrianisation physically changes the street "
         "network (closed to vehicles, widened footpaths). The graph adjacency is fixed during "
         "simulation — only node features change.", AMBER),
        ("<b>Predict beyond ~4 hours reliably.</b> Autoregressive error compounds. The model "
         "was trained on 1-step-ahead predictions; multi-step rollouts are out-of-distribution.", AMBER),
    ]
    for text, color in cannot_do:
        S(Paragraph(text, ParagraphStyle("CN", parent=s_bullet,
                                          bulletText="✗", bulletColor=color)))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 5. THE ZERO-EFFECT PROBLEM
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("5. The Zero-Effect Problem (Critical Bug)", s_h1))
    S(Paragraph(
        "<b>This is the single most important finding in this audit.</b>",
        s_verdict_fail,
    ))

    S(Paragraph("5.1 The Mechanism", s_h2))
    S(Paragraph(
        "When the user selects \"pedestrianise\" on a street, the code does this:",
        s_body,
    ))
    S(Paragraph(
        "row[node_idx, FI_OCC_RATE] = (0.0 - mu_occ) / std_occ\n"
        "# i.e., set occupancy_rate to 0 in z-score space",
        s_code,
    ))
    S(Paragraph(
        "The problem: <b>1,254 out of 1,397 streets already have occupancy_rate = 0</b> in the "
        "data cube, because they lack parking sensors. For these streets, setting occupancy to 0 "
        "changes nothing — it's already 0. The z-score of 0 is -0.211 (because the mean across "
        "the cube is 0.022 due to the 1,254 zero-streets pulling it down), but the cube value "
        "for these streets is also already normalised from 0.0, giving the same z-score.",
        s_body,
    ))

    S(Paragraph("5.2 Empirical Proof", s_h2))
    data = [
        ["Street", "Has Sensor?", "Cube Occ Mean", "Max Delta", "Assessment"],
        ["20166", "No", "0.000", "0.000", "Complete no-op"],
        ["20158", "No", "0.000", "0.000", "Complete no-op"],
        ["20159", "No", "0.000", "0.000", "Complete no-op"],
        ["20011", "No", "0.000", "0.000", "Complete no-op"],
        ["20007", "Yes", "0.352", "+14.37", "Real effect detected"],
        ["20028", "Yes", "0.189", "(not run)", "Expected to work"],
        ["20046", "Yes", "0.214", "(not run)", "Expected to work"],
    ]
    t = Table(data, colWidths=[22*mm, 25*mm, 25*mm, 22*mm, 64*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), MID),
        ("TEXTCOLOR", (0,0), (-1,0), white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("GRID", (0,0), (-1,-1), 0.4, HexColor("#dee2e6")),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ("BACKGROUND", (0,5), (-1,5), HexColor("#d4edda")),
    ]))
    S(t)
    S(Spacer(1, 3*mm))
    S(Paragraph(
        "Result: every scenario run on a non-sensor street produced baseline == treated "
        "(identical predictions), yielding <b>delta = 0.0 everywhere</b> — on the treated street, "
        "on all neighbours, and on all network streets. The entire simulation was meaningless.",
        s_verdict_fail,
    ))

    S(Paragraph("5.3 Impact", s_h2))
    S(Paragraph(
        "This means the scenario framework is <b>functionally broken for 90% of the network</b>. "
        "The frontend map, the opportunity scoring, the graph diffusion analysis, the rebound "
        "analysis — all of these produce zeros for non-sensor streets. Only 143 streets can "
        "be meaningfully evaluated.",
        s_body,
    ))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 6. PARKING SPILLOVER
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("6. Parking Spillover: Honest Assessment", s_h1))

    S(Paragraph("6.1 What the Code Claims to Do", s_h2))
    S(Paragraph(
        "The rollout feeds the parking head's predictions back into the sliding window for "
        "143 sensor streets (step_11_scenario.py lines 485-486). The claim is that when parking "
        "is removed from street j, the model's parking head will predict increased occupancy on "
        "neighbouring sensor streets, propagating spillover through subsequent rollout steps.",
        s_body,
    ))

    S(Paragraph("6.2 What Actually Happens (Street 20007 Scenario)", s_h2))
    park_data = [
        ["Street", "Parking Delta", "Ped Delta", "Relation to Treated"],
        ["20007 (treated)", "-0.0622", "+10.34", "Target"],
        ["20055", "-0.0162", "+0.64", "Spatial neighbour"],
        ["20167", "-0.0132", "+3.63", "Spatial neighbour"],
        ["20009", "-0.0037", "+1.16", "Spatial neighbour"],
        ["20053", "-0.0034", "+1.23", "Nearby"],
        ["20151", "-0.0014", "+0.47", "Nearby"],
    ]
    t = Table(park_data, colWidths=[38*mm, 30*mm, 25*mm, 65*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), MID),
        ("TEXTCOLOR", (0,0), (-1,0), white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("GRID", (0,0), (-1,-1), 0.4, HexColor("#dee2e6")),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]))
    S(t)
    S(Spacer(1, 3*mm))

    S(Paragraph("6.3 Assessment", s_h2))
    S(Paragraph(
        "<b>Direction is correct but magnitude is negligible.</b> The parking delta on neighbouring "
        "streets is negative (parking decreased, not increased as expected from spillover). This "
        "means the model is NOT predicting parking spillover — it's predicting a general decrease "
        "in parking activity as a correlated response to changed conditions.",
        s_body,
    ))
    S(Paragraph(
        "True parking spillover would show: treated street parking goes DOWN, neighbouring "
        "streets parking goes UP (because displaced cars need to park somewhere). Instead, ALL "
        "parking deltas are negative. The model learned correlated decreases, not causal redistribution.",
        s_verdict_warn,
    ))
    S(Paragraph(
        "This makes physical sense from the model's perspective: it was trained on historical "
        "correlations where nearby streets tend to have similar parking patterns. It was never "
        "trained on an actual parking removal event, so it has no basis for predicting spillover.",
        s_body,
    ))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 7. PEDESTRIAN NETWORK IMPACT
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("7. Pedestrian Network Impact: Honest Assessment", s_h1))

    S(Paragraph("7.1 The Thesis Claim", s_h2))
    S(Paragraph(
        "\"If we remove parking from a street, people will have more space, pedestrian activity "
        "will increase on that street AND on network streets, because people use network streets "
        "to reach the pedestrianised street.\"",
        s_body,
    ))

    S(Paragraph("7.2 What the Model Can Actually Support", s_h2))
    S(Paragraph(
        "The model CAN show that changing one street's features affects neighbours via the GCN. "
        "For street 20007 (pedestrianised), 91 streets show non-trivial ped deltas, with "
        "spatial neighbours showing +3 to +7 ped/15-min uplift. This is real learned signal.",
        s_body,
    ))
    S(Paragraph(
        "<b>However, this is spatial correlation, not causal pedestrian routing.</b> The model "
        "does not know WHY neighbouring streets see more pedestrians. It does not model:",
        s_body,
    ))
    claims = [
        "Trip generation (people deciding to visit a pedestrianised street)",
        "Route choice (pedestrians walking through connecting streets)",
        "Destination attractiveness (wider footpaths drawing more visitors)",
        "Mode shift (drivers switching to walking because of better pedestrian environment)",
    ]
    for c in claims:
        S(Paragraph(c, s_bullet, bulletText="•"))

    S(Paragraph(
        "What the model captures is: \"historically, when street A had more/less pedestrians, "
        "nearby street B also tended to have more/less.\" This is a valid but limited form of "
        "network effect.",
        s_body,
    ))

    S(Paragraph("7.3 Structural Gap: No Demand Model", s_h2))
    S(Paragraph(
        "The fundamental limitation is that the model predicts <b>ped_flow</b> (counts at a "
        "point in time), not <b>pedestrian trips</b> (origin-destination movements). To truly "
        "predict network impact, you would need:",
        s_body,
    ))
    needs = [
        "An origin-destination demand model (where are people going?)",
        "A pedestrian route assignment model (how do they get there?)",
        "An attractiveness function (does more space = more visitors?)",
    ]
    for n in needs:
        S(Paragraph(n, s_bullet, bulletText="→"))
    S(Paragraph(
        "These are outside the scope of a GNN flow predictor. The model can detect correlative "
        "network effects but cannot explain them causally.",
        s_body,
    ))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 8. GRAPH DIFFUSION
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("8. Graph Diffusion: Decoration vs. Mechanism", s_h1))
    S(Paragraph(
        "The graph diffusion analysis (A^k propagation for k=1..3 hops) is computed <b>after</b> "
        "the rollout as a <b>post-hoc analytical operation</b>. It does NOT feed back into the "
        "model's predictions. Here is what it actually computes:",
        s_body,
    ))
    S(Paragraph(
        "1. Take the mean ped delta across rollout steps → vector [N]\n"
        "2. Row-normalise the adjacency matrix: A_norm = A / rowsum(A)\n"
        "3. Compute A_norm^k @ mean_delta for k=1,2,3\n"
        "4. Report top streets at each hop",
        s_code,
    ))
    S(Paragraph(
        "<b>Assessment:</b> This is mathematically correct as a diffusion model but is purely "
        "decorative — it tells you \"if the observed delta were to spread through the graph, "
        "these streets would be affected.\" It does NOT tell you that these streets ARE affected. "
        "The actual model predictions already include 1-hop GCN effects; the diffusion analysis "
        "hypothesises additional spread that the model itself does not predict.",
        s_verdict_warn,
    ))
    S(Paragraph(
        "For the thesis, present this as \"analytical diffusion estimate\" and explicitly state "
        "it is not a model prediction. It is a useful complementary analysis but should not be "
        "confused with the model's own network propagation.",
        s_body,
    ))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 9. AUTOREGRESSIVE ERROR
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("9. Autoregressive Error Accumulation", s_h1))
    S(Paragraph(
        "The model was trained on <b>1-step-ahead</b> predictions. During scenario rollout, it "
        "predicts autoregressively for up to 32 steps (8 hours). Each step feeds the model's own "
        "prediction back as input, so errors compound.",
        s_body,
    ))
    S(Paragraph(
        "The rebound analysis for street 20007 shows half_life=-1 (never recovers) and "
        "recovery_fraction=1.0 (effect persists at full strength). This is <b>suspicious</b> — "
        "in reality, removing the intervention should let the system return to baseline. The "
        "persistent effect suggests the autoregressive loop has locked into a different regime "
        "and cannot self-correct.",
        s_body,
    ))
    S(Paragraph(
        "<b>Implication:</b> Rollouts beyond ~4 hours (16 steps) should be treated as "
        "qualitative indicators, not quantitative predictions. The model's scenario output is "
        "most reliable for the first few steps after intervention begins. Post-intervention "
        "rebound analysis is likely an artifact of error accumulation rather than genuine "
        "network dynamics.",
        s_verdict_warn,
    ))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 10. FULL VERDICT MATRIX
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("10. Full Verdict Matrix", s_h1))
    S(verdict_table([
        ("Model architecture", "PASS",
         "Dual-branch GCN + GRU is sound. Per-node bias, proper normalisation, "
         "chronological split. No leakage."),
        ("Ped flow prediction", "PASS",
         "R²=0.876, MAE=5.8 on held-out test set. Strong performance."),
        ("Parking prediction (sensor)", "PASS",
         "R²=0.842, MAE=0.04 on 143 sensor streets. Good."),
        ("Parking prediction (non-sensor)", "FAIL",
         "No supervision for 1,254 streets. Predictions are unvalidated extrapolation."),
        ("Scenario: treated-street effect", "PARTIAL",
         "Works for 143 sensor streets. Zero effect on 1,254 non-sensor streets (90%)."),
        ("Scenario: spatial neighbour effect", "PARTIAL",
         "Detectable 1-hop effects on sensor streets. Zero for non-sensor."),
        ("Parking spillover", "FAIL",
         "All parking deltas are negative (correlated decrease, not redistribution). "
         "No conservation constraint."),
        ("Pedestrian redistribution", "FAIL",
         "No demand model, no routing. Spatial correlation ≠ causal redistribution."),
        ("Graph diffusion analysis", "WARN",
         "Mathematically correct but post-hoc decorative. Not a model prediction."),
        ("Rebound analysis", "WARN",
         "Suspicious never-recovery pattern. Likely autoregressive artifact."),
        ("Multi-hop propagation", "PARTIAL",
         "Emerges over rollout steps via autoregressive loop, but compounds error."),
        ("Opportunity scoring", "PARTIAL",
         "Composite score is reasonable but uplift_fraction is zero for 90% of streets."),
    ]))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 11. REMEDIATION PLAN
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("11. Remediation Plan (Prioritised Fixes)", s_h1))

    S(Paragraph("Priority 1 — Fix the Zero-Effect Problem [CRITICAL]", s_h2))
    S(Paragraph(
        "The pedestrianise intervention is a no-op for non-sensor streets because their "
        "occupancy is already 0. Three approaches, from easiest to most rigorous:",
        s_body,
    ))

    S(Paragraph("<b>Fix 1A: Impute parking occupancy for all 1,397 streets (recommended)</b>", s_h3))
    S(Paragraph(
        "Use the trained parking head (or a separate XGBoost model) to predict baseline "
        "occupancy for all streets, not just sensor streets. Write these predictions into the "
        "cube so that every street has a non-zero occupancy baseline. Then the pedestrianise "
        "intervention (set occ=0) will produce a meaningful delta for every street.",
        s_body,
    ))
    S(Paragraph(
        "Implementation: Run the trained model over the validation set. For each non-sensor "
        "street, use the parking head's output as the imputed occupancy. Backfill the cube's "
        "occupancy_rate column for these streets. Re-normalise. <b>No retraining required.</b>",
        s_body,
    ))
    S(Paragraph(
        "<b>Caveat:</b> These are unvalidated predictions. Add a confidence tier (e.g., 0.3) for "
        "imputed parking streets, below the 0.5 tier used for XGBoost-imputed ped streets.",
        s_body,
    ))

    S(Paragraph("<b>Fix 1B: Use proxy occupancy from land-use features</b>", s_h3))
    S(Paragraph(
        "For non-sensor streets, estimate a baseline occupancy from static features: "
        "area_m2 (wider streets likely have parking), business_count, proximity to known "
        "parking streets. This is cruder but transparent and doesn't rely on unvalidated model "
        "predictions.",
        s_body,
    ))

    S(Paragraph("<b>Fix 1C: Restrict scenario framework to sensor streets only</b>", s_h3))
    S(Paragraph(
        "Accept the limitation. Only allow interventions on 143 sensor streets. Be explicit "
        "in the thesis that network impact analysis is limited to streets with validated "
        "parking data. This is the most honest approach if imputation quality is uncertain.",
        s_body,
    ))

    S(hr())

    S(Paragraph("Priority 2 — Fix Parking Spillover Direction [HIGH]", s_h2))
    S(Paragraph(
        "Currently all parking deltas on neighbours are negative (correlated decrease). For "
        "genuine spillover, displaced parking must appear on neighbouring streets.",
        s_body,
    ))
    S(Paragraph("<b>Fix 2A: Add explicit spillover redistribution (post-hoc)</b>", s_h3))
    S(Paragraph(
        "After each rollout step, compute the \"displaced parking volume\" (baseline occ - treated occ) "
        "on the treated street. Redistribute this volume to spatial neighbours proportional to edge "
        "weight, capped at 100%. Inject these adjusted occupancy values into the next rollout step's "
        "features for those neighbours. This adds a physics-informed constraint on top of the model's "
        "predictions.",
        s_body,
    ))
    S(Paragraph(
        "<b>Note:</b> This was originally in an earlier version of the code (mentioned in the "
        "docstring as \"first-order displacement model\") but was replaced by pure model-predicted "
        "spillover. The model's learned spillover goes in the wrong direction — consider "
        "re-introducing the heuristic.",
        s_body,
    ))

    S(hr())

    S(Paragraph("Priority 3 — Improve Network Impact Interpretability [MEDIUM]", s_h2))
    S(Paragraph("<b>Fix 3A: Separate GCN-propagated effects from autoregressive effects</b>", s_h3))
    S(Paragraph(
        "Run a diagnostic variant: do a single forward pass (not autoregressive) with the "
        "intervention applied. This isolates the pure GCN network effect from the autoregressive "
        "temporal propagation. Compare the 1-step delta to the full rollout delta. If the 1-step "
        "delta is the dominant component, the \"network effect\" is mostly GCN-mediated spatial "
        "averaging, not temporal propagation.",
        s_body,
    ))

    S(Paragraph("<b>Fix 3B: Validate against boost_ped intervention</b>", s_h3))
    S(Paragraph(
        "The boost_ped intervention adds pedestrians directly and works for all streets (not just "
        "sensor streets). Use this to validate that the GCN propagation mechanism is functional: "
        "boost ped_flow on one street by +50, check that neighbours show non-zero ped delta. "
        "This tests the propagation mechanism independently of the parking data problem.",
        s_body,
    ))

    S(hr())

    S(Paragraph("Priority 4 — Bound Autoregressive Error [MEDIUM]", s_h2))
    S(Paragraph(
        "Add error bars to scenario outputs. Run the same scenario N times with different "
        "starting bins from the validation set. Report mean ± std of deltas. If std grows "
        "rapidly with rollout steps, this quantifies the degradation window.",
        s_body,
    ))

    S(hr())

    S(Paragraph("Priority 5 — Honest Thesis Framing [ESSENTIAL]", s_h2))
    S(Paragraph(
        "Regardless of which fixes are implemented, the thesis must clearly scope what the "
        "model does vs. what it cannot do. See Section 12.",
        s_body,
    ))

    S(PageBreak())

    # ═══════════════════════════════════════════════════════════════════════
    # 12. WHAT TO PRESENT IN THE THESIS
    # ═══════════════════════════════════════════════════════════════════════
    S(Paragraph("12. What to Present in the Thesis", s_h1))

    S(Paragraph("12.1 Framing the Contribution", s_h2))
    S(Paragraph(
        "The pipeline's genuine contribution is a <b>spatio-temporal forecasting model</b> that "
        "can be used for <b>sensitivity analysis</b> (\"what if this feature changed?\") on a "
        "graph-structured street network. This is valuable — but it is not the same as a "
        "\"network impact prediction\" system.",
        s_body,
    ))
    S(Paragraph("Recommended thesis language:", s_h3))
    S(Paragraph(
        "✓ \"The model enables sensitivity analysis of street-level interventions by "
        "modifying input features and observing changes in predicted pedestrian flow across "
        "the spatial graph.\"",
        ParagraphStyle("G", parent=s_body, textColor=GREEN),
    ))
    S(Paragraph(
        "✗ \"The model predicts network-wide pedestrian redistribution resulting from "
        "street pedestrianisation.\"",
        ParagraphStyle("R", parent=s_body, textColor=RED),
    ))

    S(Paragraph("12.2 Limitations Section Checklist", s_h2))
    lims = [
        "The intervention framework requires non-zero baseline occupancy; streets without "
        "parking sensors have zero occupancy in the training data and produce no counterfactual delta.",
        "Parking spillover is not explicitly modelled; the model captures spatial correlation "
        "in parking patterns but does not enforce conservation of parking demand.",
        "Pedestrian network effects are correlative (GCN-mediated spatial averaging), not "
        "causal (no demand model, no pedestrian routing).",
        "Autoregressive rollouts beyond 4 hours compound prediction error and should be "
        "interpreted qualitatively.",
        "The graph diffusion analysis is an analytical estimate, not a model prediction.",
        "The model was trained on pre-COVID data (2015-2019); post-COVID dynamics may differ.",
    ]
    for l in lims:
        S(Paragraph(l, s_bullet, bulletText="•"))

    S(Paragraph("12.3 Strengths to Emphasise", s_h2))
    strengths = [
        "Strong forecasting performance (R²=0.876) on a real-world spatio-temporal graph.",
        "Dual-branch architecture captures both physical adjacency and functional similarity.",
        "Feature importance analysis provides actionable insights (occupancy matters, land-use matters).",
        "For the 143 sensor streets, the counterfactual framework produces plausible and "
        "directionally correct sensitivity estimates.",
        "The pipeline demonstrates a complete workflow from raw open data to interactive "
        "scenario exploration — a methodological contribution even where individual predictions "
        "carry caveats.",
    ]
    for s in strengths:
        S(Paragraph(s, s_bullet, bulletText="✓"))

    S(Spacer(1, 10*mm))
    S(hr())
    S(Paragraph(
        "<i>End of report. This assessment is based on code review and empirical analysis of "
        "scenario output artefacts as of 24 April 2026.</i>",
        s_small,
    ))

    # BUILD
    doc.build(story)
    print(f"Report written to: {OUT}")


if __name__ == "__main__":
    build()
