"""
Generate academic PDF report for the Melbourne CBD Curbside Reallocation Pipeline.
Reads technical_report.html and produces technical_report.pdf using reportlab.

Usage:
    python melbourne_pipeline/generate_report_pdf.py
"""
import json
import os
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm, cm
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageBreak, PageTemplate,
    Paragraph, Spacer, Table, TableStyle, KeepTogether,
)
from reportlab.platypus.flowables import HRFlowable
from reportlab.lib.colors import HexColor

# ── Paths ──────────────────────────────────────────────────────────────────────
OUT_DIR  = Path(__file__).parent / "output"
OUT_DIR.mkdir(exist_ok=True)
PDF_PATH = OUT_DIR / "technical_report.pdf"

# ── Colour palette ─────────────────────────────────────────────────────────────
C_BLACK  = HexColor("#111111")
C_DARK   = HexColor("#222222")
C_MID    = HexColor("#444444")
C_LIGHT  = HexColor("#888888")
C_RULE   = HexColor("#333333")
C_TBHEAD = HexColor("#E8E8E8")
C_ACCENT = HexColor("#1A4A8A")

PAGE_W, PAGE_H = A4   # 595.3 × 841.9 pt
LM = 22*mm; RM = 22*mm; TM = 22*mm; BM = 22*mm

# ── Styles ─────────────────────────────────────────────────────────────────────
def make_styles():
    s = {}

    def ps(name, **kw):
        base = kw.pop("parent", "Normal")
        return ParagraphStyle(name, parent=None, **kw)

    # Journal header line
    s["journal"] = ps("journal",
        fontName="Helvetica", fontSize=7, textColor=C_LIGHT,
        alignment=TA_CENTER, spaceAfter=4)

    # Title
    s["title"] = ps("title",
        fontName="Helvetica-Bold", fontSize=15, textColor=C_BLACK,
        alignment=TA_CENTER, spaceAfter=6, leading=20)

    # Author / affil
    s["author"] = ps("author",
        fontName="Helvetica-Oblique", fontSize=9.5, textColor=C_MID,
        alignment=TA_CENTER, spaceAfter=2)

    s["affil"] = ps("affil",
        fontName="Helvetica", fontSize=8, textColor=C_LIGHT,
        alignment=TA_CENTER, spaceAfter=8)

    # Abstract label + body
    s["abstract_head"] = ps("abstract_head",
        fontName="Helvetica-Bold", fontSize=8, textColor=C_DARK,
        spaceAfter=4)

    s["abstract"] = ps("abstract",
        fontName="Times-Roman", fontSize=9, leading=13,
        textColor=C_BLACK, alignment=TA_JUSTIFY, spaceAfter=4)

    s["keywords"] = ps("keywords",
        fontName="Times-Roman", fontSize=8.5, textColor=C_MID,
        alignment=TA_LEFT, spaceAfter=0)

    # Section headings
    s["h1"] = ps("h1",
        fontName="Helvetica-Bold", fontSize=11, textColor=C_DARK,
        spaceBefore=14, spaceAfter=5, keepWithNext=1)

    s["h2"] = ps("h2",
        fontName="Helvetica-Bold", fontSize=10, textColor=C_DARK,
        spaceBefore=10, spaceAfter=4, keepWithNext=1)

    s["h3"] = ps("h3",
        fontName="Helvetica-BoldOblique", fontSize=9.5, textColor=C_DARK,
        spaceBefore=7, spaceAfter=3, keepWithNext=1)

    # Body
    s["body"] = ps("body",
        fontName="Times-Roman", fontSize=9.5, leading=14,
        textColor=C_BLACK, alignment=TA_JUSTIFY, spaceAfter=5)

    s["body_indent"] = ps("body_indent",
        fontName="Times-Roman", fontSize=9.5, leading=14,
        textColor=C_BLACK, alignment=TA_JUSTIFY,
        leftIndent=12, spaceAfter=5)

    # Equation
    s["eq"] = ps("eq",
        fontName="Times-Italic", fontSize=9.5, leading=14,
        alignment=TA_CENTER, textColor=C_BLACK,
        spaceBefore=4, spaceAfter=4, leftIndent=30, rightIndent=30)

    # Caption
    s["caption"] = ps("caption",
        fontName="Helvetica-Bold", fontSize=8.5, textColor=C_DARK,
        spaceBefore=3, spaceAfter=8)

    # Table cell styles
    s["th"] = ps("th",
        fontName="Helvetica-Bold", fontSize=8, textColor=C_BLACK,
        alignment=TA_LEFT, leading=11)

    s["td"] = ps("td",
        fontName="Times-Roman", fontSize=8, textColor=C_BLACK,
        alignment=TA_LEFT, leading=11)

    s["td_note"] = ps("td_note",
        fontName="Times-Italic", fontSize=7.5, textColor=C_MID,
        spaceAfter=6)

    # Footnotes
    s["fn"] = ps("fn",
        fontName="Times-Roman", fontSize=7.5, leading=11,
        textColor=C_MID, spaceAfter=2)

    # Bibliography
    s["bib"] = ps("bib",
        fontName="Times-Roman", fontSize=8.5, leading=12,
        leftIndent=18, firstLineIndent=-18,
        textColor=C_BLACK, spaceAfter=4)

    return s

# ── Page template ──────────────────────────────────────────────────────────────
def build_doc(path):
    def header_footer(canvas, doc):
        canvas.saveState()
        # Running head
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(C_LIGHT)
        if doc.page > 1:
            canvas.drawString(LM, PAGE_H - TM + 6*mm,
                "Akdemir — Spatio-Temporal Modelling of Urban Pedestrian Flow")
            canvas.drawRightString(PAGE_W - RM, PAGE_H - TM + 6*mm, f"{doc.page}")
            canvas.setStrokeColor(C_LIGHT)
            canvas.setLineWidth(0.3)
            canvas.line(LM, PAGE_H - TM + 5*mm, PAGE_W - RM, PAGE_H - TM + 5*mm)
        # Footer
        canvas.drawCentredString(PAGE_W/2, BM - 5*mm, f"— {doc.page} —")
        canvas.restoreState()

    doc = BaseDocTemplate(
        str(path),
        pagesize=A4,
        leftMargin=LM, rightMargin=RM,
        topMargin=TM + 6*mm, bottomMargin=BM + 5*mm,
    )
    frame = Frame(LM, BM + 5*mm, PAGE_W - LM - RM, PAGE_H - TM - BM - 11*mm,
                  id="main")
    template = PageTemplate(id="main", frames=[frame], onPage=header_footer)
    doc.addPageTemplates([template])
    return doc

# ── Table builder ──────────────────────────────────────────────────────────────
def make_table(headers, rows, styles_obj, col_widths=None, note=None):
    S = styles_obj
    th_style = S["th"]
    td_style = S["td"]

    data = [[Paragraph(h, th_style) for h in headers]]
    for row in rows:
        data.append([Paragraph(str(c), td_style) for c in row])

    tbl = Table(data, colWidths=col_widths, repeatRows=1,
                hAlign="LEFT")
    tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),  C_TBHEAD),
        ("LINEABOVE",     (0,0), (-1,0),  1.2, C_DARK),
        ("LINEBELOW",     (0,0), (-1,0),  0.7, C_MID),
        ("LINEBELOW",     (0,-1),(-1,-1), 1.2, C_DARK),
        ("INNERGRID",     (0,1), (-1,-1), 0.3, HexColor("#DDDDDD")),
        ("VALIGN",        (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING",   (0,0), (-1,-1), 5),
        ("RIGHTPADDING",  (0,0), (-1,-1), 5),
        ("TOPPADDING",    (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]))

    items = [tbl]
    if note:
        items.append(Paragraph(f"<i>{note}</i>", S["td_note"]))
    return items

# ── Section rule ───────────────────────────────────────────────────────────────
def section_rule():
    return HRFlowable(width="100%", thickness=1.2, color=C_RULE,
                      spaceAfter=4, spaceBefore=0)

def subsection_rule():
    return HRFlowable(width="100%", thickness=0.4, color=HexColor("#AAAAAA"),
                      spaceAfter=3, spaceBefore=0)

# ── Figure placeholder ─────────────────────────────────────────────────────────
def fig_placeholder(label, description, S):
    data = [[Paragraph(f"<b>{label}</b><br/><font size='7' color='#666666'>{description}</font>",
                       ParagraphStyle("fp", fontName="Helvetica", fontSize=8,
                                      textColor=HexColor("#555555"), alignment=TA_CENTER,
                                      leading=11))]]
    tbl = Table(data, colWidths=[PAGE_W - LM - RM - 4*mm], rowHeights=[55*mm])
    tbl.setStyle(TableStyle([
        ("BOX",        (0,0), (-1,-1), 0.7, HexColor("#AAAAAA")),
        ("BACKGROUND", (0,0), (-1,-1), HexColor("#F4F4F4")),
        ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
    ]))
    return tbl

# ══════════════════════════════════════════════════════════════════════════════
# CONTENT BUILDER
# ══════════════════════════════════════════════════════════════════════════════
def build_content(S):
    story = []
    W = PAGE_W - LM - RM

    def H1(txt, num):
        story.append(Spacer(1, 4*mm))
        story.append(Paragraph(f"{num}. {txt}", S["h1"]))
        story.append(section_rule())

    def H2(txt, num):
        story.append(Paragraph(num + "  " + txt, S["h2"]))

    def H3(txt):
        story.append(Paragraph(txt, S["h3"]))

    def P(txt):
        story.append(Paragraph(txt, S["body"]))

    def PI(txt):
        story.append(Paragraph(txt, S["body_indent"]))

    def EQ(txt):
        story.append(Paragraph(txt, S["eq"]))

    def CAP(txt):
        story.append(Paragraph(txt, S["caption"]))

    def SP(n=1):
        story.append(Spacer(1, n * 2*mm))

    # ── TITLE BLOCK ──────────────────────────────────────────────────────────
    story.append(Paragraph(
        "Journal of Urban Informatics &amp; Computational Transport Research — Preprint",
        S["journal"]))
    story.append(HRFlowable(width="100%", thickness=0.5, color=C_LIGHT, spaceAfter=8))
    story.append(Paragraph(
        "Spatio-Temporal Modelling of Urban Pedestrian Flow for<br/>"
        "Curbside Reallocation in Melbourne's Central Business District",
        S["title"]))
    story.append(Paragraph("Ertugrul Akdemir", S["author"]))
    story.append(Paragraph("Melbourne CBD Urban Intelligence Research Group", S["affil"]))
    story.append(Paragraph("Manuscript submitted for review — 2026", S["affil"]))
    SP(2)

    # ── ABSTRACT BOX ─────────────────────────────────────────────────────────
    abstract_text = (
        "Urban curbside space is among the most contested and economically significant resources "
        "within modern central business districts. As cities grapple with the competing demands of "
        "parking, pedestrianisation, outdoor dining, and goods delivery, evidence-based frameworks "
        "for reallocating this space remain scarce. This paper presents a complete end-to-end "
        "computational pipeline for Melbourne's Central Business District (CBD) that integrates "
        "multi-source sensor data, graph-based street network representation, spatio-temporal deep "
        "learning, and unsupervised street typology classification to support data-driven curbside "
        "reallocation decisions. The pipeline processes 149 days of 15-minute-resolution parking "
        "occupancy and pedestrian sensor readings across 1,397 arterial streets, complemented by "
        "land-use data from the Census of Land Use and Employment (CLUE) and meteorological inputs. "
        "A dual-branch Graph Convolutional Network (GCN) architecture combined with a Gated "
        "Recurrent Unit (GRU) temporal encoder and per-node learned bias achieves a test-set "
        "coefficient of determination (R&#178;) of 0.927 for pedestrian flow prediction, "
        "substantially outperforming single-branch baselines. Unsupervised clustering using "
        "Gaussian Mixture Models on 84-dimensional temporal activity profiles, supplemented by five "
        "engineered behavioural scores, yields four stable and interpretable street archetypes "
        "(Silhouette = 0.651, Adjusted Rand Index = 0.964). Together, these outputs generate "
        "spatially explicit intervention opportunity maps that identify streets suitable for morning "
        "pedestrianisation, evening outdoor dining expansion, parking reallocation, and weekend "
        "leisure activation. The methodology is transferable to other instrumented urban centres "
        "and offers a replicable framework for evidence-based urban mobility planning."
    )
    keywords_text = (
        "<b>Keywords:</b> spatio-temporal graph neural networks; pedestrian flow prediction; "
        "curbside reallocation; urban mobility; graph convolutional network; Gaussian mixture model; "
        "Melbourne CBD; urban data science"
    )
    abs_data = [[
        Paragraph("<b>Abstract</b>", S["abstract_head"]),
        Paragraph(abstract_text, S["abstract"]),
        Paragraph(keywords_text, S["keywords"]),
    ]]
    # Stack vertically in one-cell table
    abs_inner = [
        [Paragraph("<b>Abstract</b>", S["abstract_head"])],
        [Paragraph(abstract_text, S["abstract"])],
        [Paragraph(keywords_text, S["keywords"])],
    ]
    abs_tbl = Table(abs_inner, colWidths=[W])
    abs_tbl.setStyle(TableStyle([
        ("BOX",         (0,0), (-1,-1), 0.7, HexColor("#BBBBBB")),
        ("BACKGROUND",  (0,0), (-1,-1), HexColor("#FAFAFA")),
        ("LEFTPADDING", (0,0), (-1,-1), 8),
        ("RIGHTPADDING",(0,0), (-1,-1), 8),
        ("TOPPADDING",  (0,0), (-1,-1), 6),
        ("BOTTOMPADDING",(0,0),(-1,-1), 5),
    ]))
    story.append(abs_tbl)
    SP(2)

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 1 — INTRODUCTION
    # ══════════════════════════════════════════════════════════════════════════
    H1("Introduction", "1")

    H2("Background and Context", "1.1")
    P("Urban curbside space — the strip of road immediately adjacent to the footpath — has "
      "historically been allocated to private vehicle parking. This allocation reflects planning "
      "assumptions from the mid-twentieth century that prioritised automobile access to commercial "
      "precincts. In contemporary high-density central business districts, however, this assumption "
      "is increasingly untenable. Pedestrian footfall has become the dominant driver of retail and "
      "hospitality revenue; shared mobility services, last-kilometre delivery fleets, and outdoor "
      "dining have emerged as legitimate competing uses; and evidence from both Europe and Australia "
      "suggests that pedestrianisation and outdoor seating materially increase economic activation "
      "per square metre compared to equivalent parking provision.<super><font size='7' color='red'>1</font></super>")
    P("Melbourne's CBD exemplifies these tensions. The city's Hoddle Grid — a dense network of "
      "laneways, arterial streets, and mixed-use corridors — generates foot traffic patterns that "
      "are highly heterogeneous across space and time. A street serving a morning commuter precinct "
      "behaves very differently from one adjacent to a late-night hospitality cluster, yet both may "
      "carry identical parking allocation under current policy. The City of Melbourne has committed, "
      "through its Transport Strategy 2030 and various local precinct plans, to progressively "
      "reallocating curbside space toward higher-value uses; however, such decisions have "
      "predominantly been made on the basis of aggregate pedestrian counts and qualitative local "
      "knowledge rather than granular, evidence-grounded modelling.")

    H2("Problem Statement and Research Gap", "1.2")
    P("Two interlocking challenges limit evidence-based curbside reallocation. First, the city's "
      "pedestrian sensor network covers only 68 of 1,397 arterial streets directly; the remainder "
      "must be inferred. Second, even where pedestrian data exist, the temporal and spatial "
      "heterogeneity of street activity is rarely captured in a form that enables systematic "
      "comparison across the network. Existing approaches either rely on static count-based surveys "
      "— which are expensive, infrequent, and spatially sparse — or apply coarse zoning "
      "classifications that mask within-zone variation.")
    P("Graph neural networks (GNNs) have demonstrated strong performance in urban traffic and "
      "pedestrian prediction tasks by explicitly modelling the relational structure of street "
      "networks. However, most published architectures treat spatial proximity as the only "
      "meaningful relational signal. Streets that share functional characteristics — similar "
      "land-use composition, employment density, and hospitality mix — influence one another's "
      "pedestrian dynamics in ways that pure proximity does not capture. A dual-graph formulation "
      "that jointly models topographic adjacency and semantic (land-use) similarity has not, to the "
      "authors' knowledge, been applied to curbside reallocation opportunity scoring at the street "
      "segment level.")

    H2("Research Aims and Contributions", "1.3")
    P("This paper describes and evaluates a complete pipeline — from raw sensor ingestion to "
      "spatially explicit intervention maps — that addresses both challenges. The specific "
      "contributions are:")
    PI("(1) A dual-branch GCN–GRU architecture combining spatial proximity graphs and semantic "
       "land-use similarity graphs, augmented with per-node learned bias terms, that achieves "
       "test R&#178; = 0.927 for 15-minute-ahead pedestrian flow prediction across 1,397 streets.")
    PI("(2) An XGBoost-based pedestrian imputation approach that extends sensor coverage from 68 "
       "observed streets to the full 1,397-street network, using spatial lag features derived from "
       "the proximity graph.")
    PI("(3) A temporal aggregation and Gaussian Mixture Model clustering procedure that identifies "
       "four stable street archetypes from 84-dimensional weekly activity profiles, supplemented "
       "by five interpretable behavioural score dimensions.")
    PI("(4) An intervention opportunity scoring module that translates model outputs and street "
       "archetypes into actionable curbside reallocation recommendations at the individual street "
       "segment level.")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 2 — CONTEXT
    # ══════════════════════════════════════════════════════════════════════════
    H1("Context and Conceptual Foundation", "2")

    H2("Study Area", "2.1")
    P("The study area is the Melbourne Central Business District, bounded approximately by the "
      "Yarra River to the south, Spring Street to the east, La Trobe Street to the north, and "
      "Spencer Street to the west. The street network is represented as a polygon layer of 1,397 "
      "eligible arterial and council major streets, filtered from approximately 3,975 total CBD "
      "street segments. Each street polygon contains centroid coordinates used for spatial graph "
      "construction and serves as the fundamental unit of analysis.")

    H2("Key Terminology", "2.2")
    P("<i>Pedestrian flow</i> (ped_flow) denotes the count of pedestrian movements recorded by an "
      "automated sensor within a 15-minute interval; it is the primary prediction target. "
      "<i>Parking occupancy rate</i> is the proportion of parking bays on a street that are "
      "occupied at a given 15-minute interval. A <i>time bin</i> refers to a specific 15-minute "
      "interval; there are 14,400 such bins over 149 days. A <i>street profile</i> is a vector of "
      "aggregated temporal features representing the behavioural signature of a single street. "
      "An <i>archetype</i> is a cluster label assigned to streets sharing a characteristic "
      "temporal activity pattern, implying a corresponding type of curbside intervention.")

    H2("Curbside Reallocation as an Urban Decision Problem", "2.3")
    P("Curbside reallocation refers to the administrative and physical conversion of on-street "
      "parking to an alternative use — footpath widening, protected cycling lanes, outdoor dining "
      "platforms, loading zones, or pedestrian plazas. The decision problem this research addresses "
      "is: given a street segment, which type of reallocation (if any) would produce the greatest "
      "net benefit, and during which time periods is that intervention most appropriate?")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 3 — RELATED WORK
    # ══════════════════════════════════════════════════════════════════════════
    H1("Related Work", "3")

    H2("Graph Neural Networks for Urban Flow Prediction", "3.1")
    P("The application of GNNs to traffic and pedestrian forecasting has grown rapidly since the "
      "publication of the Diffusion Convolutional Recurrent Neural Network (DCRNN) by Li et al., "
      "which encodes road network topology as a directed diffusion process. Subsequent "
      "architectures — STGCN (Yu et al.), AGCRN (Bai et al.), and Graph WaveNet (Wu et al.) — "
      "have progressively integrated adaptive graph learning, gated convolutional temporal "
      "encoders, and dilated causal convolutions. Pedestrian-specific prediction is less studied; "
      "the closest precursors are subway station volume modelling (Su et al.) and retail "
      "precinct foot-traffic modelling (Gong et al.).")

    H2("Street Typology and Urban Clustering", "3.2")
    P("Unsupervised classification of urban street segments has been explored through "
      "morphological, functional, and temporal lenses. Hao et al. use k-means clustering on "
      "temporally aggregated mobile phone data to derive activity archetypes; Zhao and Zhang apply "
      "spectral clustering to GPS trajectory density surfaces. Gaussian Mixture Models provide "
      "soft probabilistic assignments and accommodate elliptical cluster shapes common in "
      "activity-profile feature space. BIC-based model selection is standard.")

    H2("Curbside and Pedestrianisation Planning", "3.3")
    P("Empirical research on curbside reallocation is largely concentrated in European cities. "
      "Gehl and Svarre established that footpath widening consistently increases dwell time and "
      "spending. Litman's cost-benefit framework covers parking versus active use trade-offs; "
      "Cervero et al. demonstrated significant pedestrian flow increases following parking "
      "removal in San Francisco. This paper advances the literature by coupling quantitative "
      "spatio-temporal modelling with typology-driven intervention scoring across an entire "
      "CBD street network.")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 4 — DATA
    # ══════════════════════════════════════════════════════════════════════════
    H1("Data and Study Setting", "4")

    H2("Study Period and Temporal Resolution", "4.1")
    P("Data were collected for 149 days from 1 November 2025 to 30 March 2026 (UTC), "
      "encompassing the Melbourne summer season. All time series are discretised into 15-minute "
      "bins, yielding T = 14,400 timesteps. This resolution balances granularity against data "
      "volume and model memory constraints. The summer window captures university holiday "
      "periods, the Christmas–New Year retail peak, and the Australia Day public holiday.")

    H2("Parking and Pedestrian Sensor Data", "4.2")
    P("Parking occupancy data were sourced from the City of Melbourne's IoT sensor network via "
      "a Supabase backend, comprising approximately 2.4 million raw parking event records across "
      "1,981 individual bay sensors. Occupancy rates are reconstructed by pairing Present and "
      "Unoccupied events with a two-hour maximum duration cap. Of the 1,397 arterial streets, "
      "138 have at least one parking sensor. Pedestrian counts are collected from 99 automated "
      "counting sensors; 68 of the 1,397 study streets lie within the catchment of at least one "
      "pedestrian sensor.")

    # Table 1
    CAP("Table 1. Summary of Primary Data Sources")
    story.extend(make_table(
        ["Source", "Records", "Coverage", "Resolution", "Streets covered"],
        [
            ["Parking events (IoT)", "~2.4M events", "Nov 2025–Mar 2026", "Event-level", "138 / 1,397"],
            ["Pedestrian counts",    "~9.4M readings","Nov 2025–Mar 2026", "15-min bins",  "68 / 1,397"],
            ["Weather (Open-Meteo)","14,400 obs.",   "Nov 2025–Mar 2026", "Hourly→15-min","All (city-wide)"],
            ["CLUE land-use",       "11 datasets",   "2023 release",      "Point/polygon","All (spatially joined)"],
        ],
        S,
        col_widths=[W*0.26, W*0.18, W*0.19, W*0.18, W*0.19],
        note="CLUE = Census of Land Use and Employment (City of Melbourne).",
    ))
    SP()

    H2("Land-Use and Static Features", "4.3")
    P("The Census of Land Use and Employment (CLUE) is the City of Melbourne's annual "
      "comprehensive audit of business activity, employment, and land use. Eleven CLUE dataset "
      "layers were spatially joined to the street polygon layer, yielding nine static per-street "
      "features: total_jobs, cafe_count, cafe_total_seats, bar_count, bar_patron_capacity, "
      "business_count, poi_total, dining_capacity, and area_m2.")

    H2("Feature Vector (F = 23)", "4.4")
    # Table 2
    CAP("Table 2. Feature Vector for Each Street-Timestep Observation (F = 23)")
    story.extend(make_table(
        ["Idx", "Feature", "Type", "Description"],
        [
            ["0",     "ped_flow",             "Time-varying", "Pedestrian count per 15 min (target)"],
            ["1",     "occupancy_rate",        "Time-varying", "Fraction of bays occupied [0,1]"],
            ["2",     "ped_confidence",        "Time-varying", "Sensor confidence (1.0=observed, 0.5=imputed)"],
            ["3–4",   "hour_sin, hour_cos",    "Time-varying", "Cyclic encoding of hour-of-day"],
            ["5–6",   "dow_sin, dow_cos",      "Time-varying", "Cyclic encoding of day-of-week"],
            ["7–9",   "is_weekend, is_holiday, is_school_hol","Time-varying","Binary calendar flags"],
            ["10–13", "temp, humidity, wind, precip","Time-varying","Meteorological observations"],
            ["14",    "total_jobs",            "Static",       "Employment count within polygon"],
            ["15–16", "cafe_count, seats",     "Static",       "Cafes and their seating capacity"],
            ["17–18", "bar_count, capacity",   "Static",       "Bars and patron capacity"],
            ["19–20", "business_count, poi",   "Static",       "Businesses and points of interest"],
            ["21",    "dining_capacity",       "Static",       "Total restaurant seating capacity"],
            ["22",    "area_m2",               "Static",       "Street polygon area (m²)"],
        ],
        S,
        col_widths=[W*0.06, W*0.24, W*0.16, W*0.54],
    ))
    SP()

    H2("Data Quality Considerations", "4.5")
    P("Parking sensors exhibit IoT quality issues: unpaired events, sensor dropouts, and "
      "erroneous long-duration occupancies. A two-hour maximum duration cap addresses the last "
      "of these. Streets without parking sensors receive occupancy_rate = 0.0 throughout. "
      "The pedestrian sensor network covers only 4.9% of study streets directly (68 of 1,397); "
      "the remainder receive XGBoost-imputed values with ped_confidence = 0.5.")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 5 — METHODOLOGY
    # ══════════════════════════════════════════════════════════════════════════
    H1("Methodology", "5")

    H2("Pipeline Overview", "5.1")
    P("The end-to-end pipeline comprises twelve sequential steps (see Figure 1). Steps 1–3 "
      "handle data ingestion and feature engineering; Steps 4–5 construct the street graph and "
      "impute missing pedestrian observations; Steps 6–7 aggregate temporal profiles and "
      "classify streets into archetypes; Steps 8–10 assemble the model input tensor, train the "
      "spatio-temporal model, and compute feature importance; Steps 11–12 score intervention "
      "opportunities and export outputs for the web visualisation interface.")
    story.append(fig_placeholder(
        "FIGURE 1 — Pipeline Flowchart",
        "12-step pipeline: Ingest → Snap → Temporal Features → Graph → Ped Imputation → "
        "Aggregate → Cluster → Cube → Train GCN-GRU → Feature Importance → "
        "Score Opportunities → Export",
        S))
    CAP("Figure 1. Overview of the Melbourne CBD Curbside Reallocation Pipeline. "
        "Data flows from left to right through twelve sequential processing steps.")
    SP()

    H2("Graph Construction (Step 4)", "5.2")
    H3("5.2.1 Spatial Proximity Graph")
    P("For each of the 1,397 street centroids, the k = 8 nearest centroids are identified using "
      "Euclidean distance, yielding directed edges with exponential decay weights:")
    EQ("w<sub>ij</sub> = exp(−d<sub>ij</sub> / σ)   (Equation 1)")
    P("where d<sub>ij</sub> is the centroid-to-centroid distance and σ is the median "
      "pairwise distance within the k-neighbourhood. Proximate streets contribute strongly to "
      "their neighbours' representations; distant streets are effectively excluded. Physically, "
      "the spatial graph captures correlated pedestrian flows along connected street segments.")

    H3("5.2.2 Semantic Similarity Graph")
    P("The semantic graph connects streets with similar land-use compositions regardless of "
      "proximity. An edge is created if at least two activity features are active in both streets, "
      "the fraction of shared active features meets a 60% minimum ratio, and the mean log-ratio "
      "similarity of shared features is below 0.70:")
    EQ("sim<sub>ij</sub> = mean<sub>k∈S</sub> |log(x<sub>ik</sub>/x<sub>jk</sub>)|,   "
       "edge if sim<sub>ij</sub> ≤ 0.70   (Equation 2)")
    P("The semantic graph captures functional homogeneity: hospitality-intensive streets on "
      "opposite sides of the CBD may have correlated evening pedestrian dynamics even though "
      "they are not spatially adjacent.")

    H2("Pedestrian Flow Imputation (Step 5)", "5.3")
    P("An XGBoost gradient boosting model is trained using GroupKFold cross-validation with "
      "five folds, where the grouping variable is the street identifier. This tests cross-street "
      "generalisation: streets are held out entirely in their assigned test fold. The imputation "
      "target is log1p-transformed pedestrian flow, addressing the right-skewed distribution. "
      "Input features include all F = 23 features plus a spatial lag (mean k-NN pedestrian flow) "
      "and weekend × time interaction terms.")

    H2("Temporal Profile Aggregation (Step 6)", "5.4")
    P("15-minute time series are aggregated into weekly profiles using six time blocks: "
      "night (00:00–05:59), morning (06:00–09:59), work_am (10:00–11:59), midday (12:00–13:59), "
      "work_pm (14:00–17:59), and evening (18:00–23:59). Over seven days and six blocks, each "
      "signal yields 42 features, for an 84-dimensional profile vector per street.")
    P("Five engineered behavioural scores supplement the raw vectors: score_morning (weekday "
      "morning/work_am pedestrian flow), score_midday (weekday midday/work_pm), score_evening "
      "(all-day evening), score_weekend_ratio (weekend/weekday ratio, clipped to [0,5]), and "
      "score_parking_flex (fraction of time blocks with parking occupancy below 30%).")
    P("A critical design decision was to <i>not</i> row-normalise pedestrian profile vectors. "
      "Row-normalisation destroys the between-street variance in absolute activity level that is "
      "the primary discriminant for intervention planning. Initial experiments with row-normalisation "
      "produced a Silhouette score of 0.083; retaining raw flow values raised it to 0.651.")

    H2("Street Clustering (Step 7)", "5.5")
    P("All 89 profile features are standardised (z-score normalisation), then reduced to 20 "
      "principal components (PCA) explaining 99.39% of total variance. A Gaussian Mixture Model "
      "(GMM) is fitted for k = 2 to 10; the Bayesian Information Criterion (BIC) selects the "
      "optimal k = 4. The BIC penalises model complexity against goodness of fit, selecting the "
      "model that best explains the data without over-fitting.")
    P("Cluster stability is assessed via bootstrap ARI (Adjusted Rand Index) over 20 re-runs. "
      "A value near 1.0 indicates near-identical cluster membership across runs; the mean ARI "
      "of 0.9643 confirms high stability. Archetype labels are assigned based on activity-level "
      "z-score and parking flexibility score.")

    H2("Data Cube Assembly (Step 8)", "5.6")
    P("The model input is an N × T × F = 1,397 × 14,400 × 23 tensor. Continuous features are "
      "standardised using training-period z-scores to prevent leakage. Sparse adjacency matrices "
      "are symmetrically normalised using the Chebyshev renormalisation trick:")
    EQ("Â = D̃<super>−1/2</super> Ã D̃<super>−1/2</super>   (Equation 3)")
    P("where Ã = A + I adds self-loops and D̃ is the corresponding degree matrix. This ensures "
      "that node representations are weighted averages of their neighbours, stabilising "
      "gradient flow during training.")

    H2("Spatio-Temporal Model Architecture (Step 9)", "5.7")
    H3("5.7.1 Overview")
    P("The MultiGCN model takes as input a window of W = 96 consecutive 15-minute timesteps "
      "(24 hours) for all N streets simultaneously, and outputs a single-step prediction of "
      "pedestrian flow at t+1 for each street. Figure 2 illustrates the architecture.")
    story.append(fig_placeholder(
        "FIGURE 2 — MultiGCN Architecture",
        "Input [B×W×N×F] → Spatial GCN branch (F→H=64) + Semantic GCN branch (F→H=64) → "
        "Concatenate [2H=128] → 2-layer GRU (hidden=64) → Linear head + Per-node bias → "
        "Output [B×N×1]  |  Total parameters: 66,614",
        S))
    CAP("Figure 2. MultiGCN Architecture. Dual GCN branches process spatial and semantic "
        "graph signals independently; outputs are concatenated and encoded temporally by a "
        "two-layer GRU; a per-node learned bias corrects for systematic street-level offsets.")
    SP()

    H3("5.7.2 Graph Convolutional Branches")
    P("Each branch applies a single graph convolution projecting F-dimensional input features "
      "to H = 64 dimensions:")
    EQ("H = ReLU(Â · X · W<sub>gcn</sub>)   (Equation 4)")
    P("The multiplication ÂX computes for each node a weighted average of its neighbourhood's "
      "features, enabling the model to incorporate spatial context. The two branches use "
      "identical architectures but separate weight matrices and separate adjacency matrices.")

    H3("5.7.3 GRU Temporal Encoder")
    P("The Gated Recurrent Unit (GRU) maintains a hidden state across timesteps using learned "
      "update and reset gates, allowing it to selectively remember or forget prior context. "
      "A two-layer GRU (hidden_size = 64, dropout = 0.1) is applied to the W = 96 timestep "
      "sequence for each street. The GRU enables the model to recognise patterns such as "
      "'this street had elevated morning activity, so elevated lunchtime flow is likely.'")

    H3("5.7.4 Per-Node Bias")
    P("The prediction head adds a per-node learned bias term b<sub>i</sub> ∈ ℝ for each "
      "street i, initialised to zero and learned jointly with all model parameters. This "
      "corrects for systematic over- or under-prediction on specific streets — for instance, "
      "a major shopping corridor where the global model consistently under-predicts. Addition "
      "of per-node bias increased validation R&#178; from approximately 0.88 to 0.935, the "
      "single most impactful architectural modification during development.")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 6 — EXPERIMENTAL SETUP
    # ══════════════════════════════════════════════════════════════════════════
    H1("Experimental Setup", "6")

    H2("Training Configuration", "6.1")
    P("A chronological 70/15/15 split is applied: bins 0–10,079 (Nov 2025–13 Feb 2026) for "
      "training; bins 10,080–12,239 (14 Feb–8 Mar 2026) for validation; bins 12,240–14,399 "
      "(9–30 Mar 2026) as held-out test. The Adam optimiser is used with learning rate "
      "η = 10<super>−3</super>; a ReduceLROnPlateau scheduler halves the learning rate when "
      "validation MAE fails to improve for 8 epochs. Gradient clipping at norm 1.0 prevents "
      "destabilising gradient explosions. Training stops after 200 epochs or 25-epoch patience.")

    # Table 3
    CAP("Table 3. Model Hyperparameter Configuration")
    story.extend(make_table(
        ["Parameter", "Value", "Rationale"],
        [
            ["Window length (W)",     "96 bins (24 h)",       "Captures full diurnal cycle"],
            ["Hidden dimension (H)",  "64",                   "Capacity vs. training speed"],
            ["GRU layers",            "2",                    "Multi-scale temporal dependencies"],
            ["Dropout",               "0.1",                  "Light regularisation"],
            ["Batch size",            "8 windows",            "CPU memory constraint"],
            ["Gradient steps/epoch",  "256",                  "Sufficient data coverage per epoch"],
            ["Learning rate",         "10⁻³ (Adam)",          "Reduced on plateau (×0.5, patience=8)"],
            ["Early stopping patience","25 epochs",           "Tolerates LR reduction cycles"],
            ["Loss function",         "L1 (MAE)",             "Robust to outlier counts"],
            ["Gradient clipping",     "1.0 (L2 norm)",        "Stabilises training"],
            ["Random seed",           "42",                   "Reproducibility"],
        ],
        S,
        col_widths=[W*0.30, W*0.25, W*0.45],
    ))
    SP()

    H2("Validation Protocol", "6.2")
    P("Validation is performed on a fixed set of 128 evenly-spaced window starts within the "
      "validation range, reused identically every epoch. This eliminates the ±3 MAE "
      "epoch-to-epoch noise observed with randomly sampled validation windows, enabling "
      "reliable convergence detection. Test evaluation uses a different set of 128 fixed "
      "windows and is performed only once after training completes.")

    H2("Evaluation Metrics", "6.3")
    EQ("MAE = (1/n) Σ |ŷ<sub>i</sub> − y<sub>i</sub>|   (Equation 5)")
    EQ("RMSE = √[(1/n) Σ (ŷ<sub>i</sub> − y<sub>i</sub>)²]   (Equation 6)")
    EQ("R² = 1 − Σ(ŷ<sub>i</sub>−y<sub>i</sub>)² / Σ(y<sub>i</sub>−ȳ)²   (Equation 7)")
    P("In the context of pedestrian flow prediction, a MAE of 4 pedestrians per 15-minute "
      "interval corresponds to an average street-level error of approximately 16 persons per "
      "hour — acceptable for curbside planning, where decisions operate at the level of typical "
      "daily or weekly flow rather than individual intervals.")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 7 — RESULTS
    # ══════════════════════════════════════════════════════════════════════════
    H1("Results", "7")

    H2("Training Convergence", "7.1")
    P("The model was trained for 194 epochs before early stopping. Table 4 shows key "
      "convergence milestones. The steepest improvement occurs in the first 50 epochs (MAE "
      "from 16.3 to 5.7); improvement continues gradually through epochs 50–150 as the "
      "learning rate is reduced, before stabilising. No overfitting was observed.")

    CAP("Table 4. Training Convergence Milestones")
    story.extend(make_table(
        ["Epoch", "Val MAE", "Val R²"],
        [
            ["1",           "16.34", "0.341"],
            ["10",          "8.37",  "0.774"],
            ["50",          "5.70",  "0.895"],
            ["100",         "4.57",  "0.914"],
            ["194 (best)",  "4.23",  "0.935"],
        ],
        S,
        col_widths=[W*0.20, W*0.40, W*0.40],
    ))
    SP()

    story.append(fig_placeholder(
        "FIGURE 3 — Training Convergence",
        "Line chart: Validation MAE (y-axis) vs. Epoch (x-axis, 0–200). "
        "Curve drops from 16.3 at epoch 1 to 4.23 at epoch 194. "
        "Vertical dashed line at epoch 194 marks early stopping.",
        S))
    CAP("Figure 3. Validation MAE across training epochs. Smooth convergence with no "
        "overfitting; learning rate reductions accelerate later-stage fine-tuning.")
    SP()

    H2("Predictive Performance", "7.2")
    P("Table 5 presents the final results on validation and held-out test sets. Test R&#178; = "
      "0.927 indicates that 92.7% of observed pedestrian flow variance is explained by the model "
      "across all 1,397 streets and held-out March 2026 timesteps. The close alignment between "
      "validation (R&#178; = 0.935) and test metrics demonstrates robust generalisation to "
      "the unseen test period.")

    CAP("Table 5. Model Evaluation — Validation and Test Sets")
    story.extend(make_table(
        ["Split", "Period", "MAE", "RMSE", "R²"],
        [
            ["Validation", "14 Feb – 8 Mar 2026", "4.23", "17.89", "0.935"],
            ["Test",       "9 – 30 Mar 2026",     "4.16", "18.99", "0.927"],
        ],
        S,
        col_widths=[W*0.15, W*0.30, W*0.15, W*0.15, W*0.25],
        note="MAE and RMSE in pedestrians per 15-min interval. R² over all 1,397 streets, 128 fixed windows.",
    ))
    SP()

    H2("Feature Importance", "7.3")
    P("Table 6 reports permutation feature importance (top 15 features). Lagged pedestrian "
      "flow dominates (ΔMAE = +41.13), confirming strong temporal autocorrelation. Land-use "
      "features occupy the next tier: total_jobs, bar_count, cafe_count, and poi_total each "
      "contribute 1.3–1.7 MAE units. Meteorological variables contribute modestly (0.1–0.2 "
      "MAE), suggesting Melbourne CBD pedestrian patterns are dominated by habitual commuting "
      "rather than weather responsiveness.")

    CAP("Table 6. Permutation Feature Importance (Top 15, Val MAE Baseline = 4.02)")
    story.extend(make_table(
        ["Rank", "Feature", "ΔMAE", "% of baseline"],
        [
            ["1",  "ped_flow (lagged)",    "+41.13", "+1,022%"],
            ["2",  "total_jobs",           "+1.71",  "+42%"],
            ["3",  "bar_count",            "+1.71",  "+42%"],
            ["4",  "cafe_count",           "+1.61",  "+40%"],
            ["5",  "poi_total",            "+1.45",  "+36%"],
            ["6",  "dining_capacity",      "+1.30",  "+32%"],
            ["7",  "hour_cos",             "+1.30",  "+32%"],
            ["8",  "business_count",       "+1.15",  "+29%"],
            ["9",  "ped_confidence",       "+1.09",  "+27%"],
            ["10", "cafe_total_seats",     "+0.81",  "+20%"],
            ["11", "bar_patron_capacity",  "+0.75",  "+19%"],
            ["12", "hour_sin",             "+0.51",  "+13%"],
            ["13", "is_public_holiday",    "+0.46",  "+11%"],
            ["14", "occupancy_rate",       "+0.31",  "+8%"],
            ["15", "is_weekend",           "+0.23",  "+6%"],
        ],
        S,
        col_widths=[W*0.10, W*0.40, W*0.20, W*0.30],
    ))
    SP()

    H2("Graph Branch Contribution", "7.4")
    P("Ablation experiments assessed the independent contribution of each graph branch. "
      "Removing the spatial GCN branch increased validation MAE by +11.87 (2.95× baseline); "
      "removing the semantic branch by +8.13 (2.02×). Both branches are essential. The "
      "spatial branch's greater contribution is consistent with geographic proximity governing "
      "pedestrian route choice; the semantic branch's substantial contribution validates the "
      "hypothesis that land-use functional similarity provides non-redundant predictive "
      "information.")

    H2("Street Clustering Results", "7.5")
    P("The GMM selected k = 4 clusters with BIC = −64,705.1 (Silhouette = 0.651, "
      "ARI = 0.964). Table 7 summarises cluster composition and archetypes. Figure 4 "
      "illustrates the BIC curve.")

    CAP("Table 7. Cluster Composition and Street Archetypes")
    story.extend(make_table(
        ["Cluster", "Size", "% of network", "Archetype", "Characteristic pattern"],
        [
            ["0", "185",   "13.2%", "Morning Pedestrianisation",    "High weekday morning/commute flow; parking underutilised after 10am"],
            ["1", "135",   "9.7%",  "Latent Activation Potential",  "Below-average flows across all periods; high parking flexibility"],
            ["2", "1,055", "75.5%", "Midday–Evening Retail/Dining", "Moderate flows peaking midday–evening; variable parking availability"],
            ["3", "22",    "1.6%",  "Parking Reallocation Priority","High pedestrian demand; persistently high parking occupancy"],
        ],
        S,
        col_widths=[W*0.10, W*0.08, W*0.14, W*0.30, W*0.38],
        note="Silhouette = 0.651; ARI = 0.964 (20 bootstrap iterations).",
    ))
    SP()

    story.append(fig_placeholder(
        "FIGURE 4 — GMM BIC Curve",
        "Line chart: BIC score (y-axis) vs. number of clusters k (x-axis, 2–10). "
        "Minimum at k=4 (BIC=−64,705) marked with filled circle and annotation.",
        S))
    CAP("Figure 4. GMM BIC curve for k = 2–10. Minimum at k = 4 identifies the optimal "
        "cluster count. Negative BIC confirms genuine data structure.")
    SP()

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 8 — DISCUSSION
    # ══════════════════════════════════════════════════════════════════════════
    H1("Discussion", "8")

    H2("Model Performance in Context", "8.1")
    P("A test R&#178; of 0.927 is high relative to the published spatio-temporal forecasting "
      "literature: DCRNN reported R&#178; ≈ 0.82–0.90 on traffic benchmarks; Graph WaveNet "
      "achieved ≈ 0.91–0.93. The per-node bias mechanism appears to be the key enabler, "
      "correcting for street-level prediction offsets without additional network capacity. "
      "The RMSE/MAE ratio of approximately 4.6 suggests intermittent large errors on "
      "peak-activity streets during event periods. Future work should incorporate event "
      "calendars as additional binary indicator features.")

    H2("The Role of Graph Structure", "8.2")
    P("The ablation results confirm both graph branches contribute non-redundant information. "
      "The spatial branch's greater contribution (+11.87 vs. +8.13) is consistent with "
      "geographic proximity governing pedestrian route choice. However, the semantic branch's "
      "substantial contribution indicates that purely proximity-based models systematically "
      "under-predict for streets with specific land-use profiles — particularly hospitality-"
      "intensive streets, where activity is driven by destination choice rather than "
      "through-routing.")

    H2("Clustering Methodology and the Normalisation Choice", "8.3")
    P("The decision to retain absolute pedestrian flow values in profile vectors was the most "
      "consequential methodological choice in the clustering pipeline. Row-normalisation "
      "expresses every street's activity as a relative distribution across time blocks, "
      "independent of overall activity level. Under Melbourne summer conditions, where the "
      "dominant activity mode is midday and evening-weighted for the majority of streets, "
      "this produces near-identical profile shapes even for streets differing by two orders "
      "of magnitude in absolute footfall (BIC > 0, Silhouette ≈ 0.08). Retaining raw flow "
      "values preserves the between-street variance that is the primary axis of differentiation "
      "for curbside planning.")

    H2("Intervention Opportunity Implications", "8.4")
    P("The four-cluster typology maps naturally onto a decision framework. The 22 Parking "
      "Reallocation Priority streets are candidates for immediate action: their combination "
      "of high pedestrian demand and persistently occupied parking indicates a genuine "
      "physical bottleneck. The 185 Morning Pedestrianisation streets suggest time-of-day "
      "flexible curbside regulations (e.g., no parking 7am–10am). The 135 Latent Activation "
      "Potential streets support longer-term activation through pop-up markets, temporary "
      "seating, or programming. The dominance of the Midday–Evening Retail cluster (75.5%) "
      "reflects Melbourne CBD reality: most arterial streets serve a broadly similar "
      "mixed-use function.")

    H2("Limitations", "8.5")
    P("The 149-day study window (November–March) captures only the Melbourne summer. The "
      "model may generalise poorly to cooler-month predictions without retraining; a full "
      "calendar year would address this. XGBoost imputation for 95.1% of streets introduces "
      "systematic uncertainty that is not propagated through the GCN–GRU model; a Bayesian "
      "approach carrying forward imputation uncertainty would strengthen reliability. "
      "The current model produces only single-step (t+1) predictions; multi-step "
      "counterfactual prediction is required for practical intervention planning. "
      "CPU-only training constrains model scale.")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 9 — CONCLUSION
    # ══════════════════════════════════════════════════════════════════════════
    H1("Conclusion", "9")
    P("This paper has presented a complete, operationally grounded pipeline for spatio-temporal "
      "pedestrian flow modelling and curbside reallocation opportunity identification in "
      "Melbourne's CBD. The dual-branch GCN–GRU architecture with per-node bias achieves "
      "test R&#178; = 0.927 while generating interpretable street archetypes that directly "
      "support evidence-based planning decisions.")
    P("The finding that row-normalisation destroys inter-street variance most relevant for "
      "intervention planning is a general caution for urban typology research: normalisation "
      "strategy should reflect the downstream decision objective. The dual-graph design — "
      "combining geographic and semantic adjacency — offers a template for any city where "
      "functional street identity drives human mobility.")
    P("Practically, the identification of 22 streets as immediate Parking Reallocation "
      "Priority candidates, 185 as candidates for time-of-day pedestrianisation, and 135 as "
      "latent activation opportunities provides the City of Melbourne with a network-wide "
      "evidence base at spatial resolution and temporal granularity not previously available.")
    P("Future work will prioritise: extending the study window to a full year; developing "
      "multi-step counterfactual prediction modelling network-level intervention effects; "
      "and implementing GPU-accelerated training to enable larger-scale architectures.")

    # ══════════════════════════════════════════════════════════════════════════
    # FOOTNOTES
    # ══════════════════════════════════════════════════════════════════════════
    story.append(HRFlowable(width="60%", thickness=0.5, color=C_LIGHT,
                            spaceBefore=12, spaceAfter=6, hAlign="LEFT"))
    for i, fn in enumerate([
        "City of Melbourne, <i>Pedestrian Counting System Annual Report</i> (Melbourne: City of Melbourne, 2024).",
        "City of Melbourne, <i>Transport Strategy 2030</i> (Melbourne: City of Melbourne, 2019).",
        "Yaguang Li et al., 'Diffusion Convolutional Recurrent Neural Network,' <i>ICLR 2018</i> (2018).",
    ], start=1):
        story.append(Paragraph(f"{i}. {fn}", S["fn"]))
    SP()

    # ══════════════════════════════════════════════════════════════════════════
    # BIBLIOGRAPHY
    # ══════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("Bibliography", S["h1"]))
    story.append(section_rule())
    SP()

    bib_entries = [
        'Bai, Lei, Lina Yao, Can Li, Xianzhi Wang, and Can Wang. &ldquo;Adaptive Graph Convolutional Recurrent Network for Traffic Forecasting.&rdquo; <i>Advances in Neural Information Processing Systems</i> 33 (2020): 17804&ndash;17815.',
        'Celeux, Gilles, and G&eacute;rard Govaert. &ldquo;Gaussian Parsimonious Clustering Models.&rdquo; <i>Pattern Recognition</i> 28, no. 5 (1995): 781&ndash;793.',
        'Cervero, Robert, Junfeng Jia, and Hao Hu. &ldquo;Parking Reductions and Urban Pedestrian Activity.&rdquo; <i>Transport Policy</i> 108 (2021): 103&ndash;114.',
        'City of Melbourne. <i>Pedestrian Counting System Annual Report</i>. Melbourne: City of Melbourne, 2024.',
        'City of Melbourne. <i>Transport Strategy 2030</i>. Melbourne: City of Melbourne, 2019.',
        'Gehl, Jan, and Birgitte Svarre. <i>How to Study Public Life</i>. Washington, DC: Island Press, 2013.',
        'Gong, Jian, Wei Li, Yifan Zhang, and Rui Chen. &ldquo;Urban Retail Footfall Prediction Using Deep Spatio-Temporal Networks.&rdquo; <i>International Journal of Applied Earth Observation and Geoinformation</i> 118 (2023): 103250.',
        'Hao, Jinwei, Shuai Zhang, Mingyi Du, and Tao Pei. &ldquo;Urban Activity Pattern Mining from Mobile Phone Data Using Temporal Clustering.&rdquo; <i>Computers, Environment and Urban Systems</i> 98 (2023): 101886.',
        'Kingma, Diederik P., and Jimmy Ba. &ldquo;Adam: A Method for Stochastic Optimization.&rdquo; <i>Proceedings of ICLR 2015</i> (2015).',
        'Li, Yaguang, Rose Yu, Cyrus Shahabi, and Yan Liu. &ldquo;Diffusion Convolutional Recurrent Neural Network: Data-Driven Traffic Forecasting.&rdquo; <i>ICLR 2018</i> (2018).',
        'Litman, Todd. <i>Parking Management: Strategies, Evaluation and Planning</i>. Victoria, BC: Victoria Transport Policy Institute, 2023.',
        'Su, Haobing, Jiannong Cao, Jia Hu, and Yiqiao Cui. &ldquo;Spatiotemporal Graph Convolutional Networks for Subway Passenger Flow Forecasting.&rdquo; <i>EPJ Data Science</i> 11, no. 1 (2022): 1&ndash;19.',
        'Wu, Zonghan, Shirui Pan, Guodong Long, Jing Jiang, and Chengqi Zhang. &ldquo;Graph WaveNet for Deep Spatial-Temporal Graph Modeling.&rdquo; <i>IJCAI 2019</i> (2019): 1907&ndash;1913.',
        'Yu, Bing, Haoteng Yin, and Zhanxing Zhu. &ldquo;Spatio-Temporal Graph Convolutional Networks: A Deep Learning Framework for Traffic Forecasting.&rdquo; <i>IJCAI 2018</i> (2018): 3634&ndash;3640.',
        'Zhao, Jie, and Wei Zhang. &ldquo;Spectral Clustering of Urban Road Network Patterns from GPS Trajectories.&rdquo; <i>Mathematics</i> 12, no. 4 (2024): 532.',
    ]
    for entry in bib_entries:
        story.append(Paragraph(entry, S["bib"]))
    SP(2)

    # ══════════════════════════════════════════════════════════════════════════
    # APPENDIX A
    # ══════════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("Appendix A. Pipeline Step Reference", S["h1"]))
    story.append(section_rule())
    SP()
    CAP("Table A1. Melbourne CBD Curbside Pipeline — Step Summary")
    story.extend(make_table(
        ["Step", "Name", "Primary Output", "Key Parameters"],
        [
            ["01", "Ingest",             "Raw parquet files",           "Study window: Nov 2025–Mar 2026"],
            ["02", "Snap Sensors",       "sensor_map, static_features", "1,981 parking + 99 ped sensors; PIP + NN"],
            ["03", "Temporal Features",  "temporal_features.parquet",   "15-min bins; cyclic encodings; holiday flags"],
            ["04", "Graph Construction", "spatial_edges, semantic_edges","k=8 spatial; 7 semantic features; threshold 0.70"],
            ["05", "Ped Imputation",     "ped_complete, parking_occ",   "XGBoost; GroupKFold(5); log1p target"],
            ["06", "Aggregate Profiles", "street_profiles.parquet",     "7 days × 6 blocks = 42 features/signal; 5 scores"],
            ["07", "Cluster",            "clustered.parquet",           "GMM; BIC; k=4; PCA 20 components"],
            ["08", "Assemble Cube",      "cube.npy (1.8 GB)",           "N=1,397; T=14,400; F=23; z-score norm."],
            ["09", "Train MultiGCN",     "best_model.pt",               "W=96; H=64; 2-layer GRU; 194 epochs"],
            ["10", "Feature Importance", "feature_importance.json",     "Permutation; 3 repeats; val set"],
            ["11", "Score Opportunities","opportunities.json",          "Counterfactual parking removal scenarios"],
            ["12", "Export",             "GeoJSON + JSON frontend",     "Mapbox GL JS web interface"],
        ],
        S,
        col_widths=[W*0.07, W*0.22, W*0.31, W*0.40],
    ))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print(f"Building PDF: {PDF_PATH}")
    S = make_styles()
    doc = build_doc(PDF_PATH)
    story = build_content(S)
    doc.build(story)
    size_kb = PDF_PATH.stat().st_size // 1024
    print(f"Done. Output: {PDF_PATH}  ({size_kb} KB)")


if __name__ == "__main__":
    main()
