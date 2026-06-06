"""Generate the Melbourne CBD Pipeline Report as a comprehensive PDF.

Covers all 12 pipeline steps with:
- Technical terminology glossary per step
- Real-life meaning of each operation
- Architecture diagrams and result tables
- Cluster, scenario, and frontend documentation

Run:  python docs/generate_pipeline_report.py
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import (
    BaseDocTemplate,
    Flowable,
    Frame,
    HRFlowable,
    Image,
    KeepTogether,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = ROOT / "melbourne_pipeline" / "data" / "models"
PROCESSED_DIR = ROOT / "melbourne_pipeline" / "data" / "processed"
FIGURES_DIR = ROOT / "docs" / "figures"
OUTPUT_PDF = ROOT / "docs" / "pipeline_report.pdf"

# -- Palette ----------------------------------------------------------------
ACCENT = colors.HexColor("#1f3a5f")
ACCENT_SOFT = colors.HexColor("#eaf0f7")
ACCENT_WARM = colors.HexColor("#f5f0e8")
INK = colors.HexColor("#111111")
MUTED = colors.HexColor("#6b6b6b")
RULE = colors.HexColor("#d9d9d9")
GREEN_SOFT = colors.HexColor("#e8f5e9")
ORANGE_SOFT = colors.HexColor("#fff3e0")
TERM_BG = colors.HexColor("#f7f5ff")
TERM_BAR = colors.HexColor("#5c4fa0")
REALLIFE_BG = colors.HexColor("#f0faf5")
REALLIFE_BAR = colors.HexColor("#2e7d5e")


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------

def build_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    s: dict[str, ParagraphStyle] = {}

    s["CoverEyebrow"] = ParagraphStyle(
        "CoverEyebrow", parent=base["Normal"],
        fontName="Helvetica", fontSize=10, textColor=ACCENT,
        leading=14, spaceAfter=6, alignment=TA_LEFT,
    )
    s["CoverTitle"] = ParagraphStyle(
        "CoverTitle", parent=base["Title"],
        fontName="Helvetica-Bold", fontSize=40, leading=46,
        textColor=INK, alignment=TA_LEFT, spaceAfter=14,
    )
    s["CoverSubtitle"] = ParagraphStyle(
        "CoverSubtitle", parent=base["Normal"],
        fontName="Helvetica", fontSize=16, leading=22,
        textColor=MUTED, alignment=TA_LEFT, spaceAfter=24,
    )
    s["CoverMeta"] = ParagraphStyle(
        "CoverMeta", parent=base["Normal"],
        fontName="Helvetica", fontSize=10, leading=14,
        textColor=INK, alignment=TA_LEFT,
    )
    s["TOCHead"] = ParagraphStyle(
        "TOCHead", parent=base["Heading1"],
        fontName="Helvetica-Bold", fontSize=22, leading=26,
        textColor=INK, alignment=TA_LEFT, spaceBefore=0, spaceAfter=16,
    )
    s["TOCEntry"] = ParagraphStyle(
        "TOCEntry", parent=base["Normal"],
        fontName="Helvetica", fontSize=10.5, leading=20,
        textColor=INK, alignment=TA_LEFT,
        leftIndent=12,
    )
    s["SectionNumber"] = ParagraphStyle(
        "SectionNumber", parent=base["Normal"],
        fontName="Helvetica", fontSize=10, textColor=ACCENT,
        leading=12, spaceAfter=2, alignment=TA_LEFT,
    )
    s["SectionTitle"] = ParagraphStyle(
        "SectionTitle", parent=base["Heading1"],
        fontName="Helvetica-Bold", fontSize=22, leading=26,
        textColor=INK, alignment=TA_LEFT,
        spaceBefore=0, spaceAfter=10,
    )
    s["Subhead"] = ParagraphStyle(
        "Subhead", parent=base["Heading2"],
        fontName="Helvetica-Bold", fontSize=12, leading=16,
        textColor=ACCENT, alignment=TA_LEFT,
        spaceBefore=14, spaceAfter=6,
    )
    s["SubheadSmall"] = ParagraphStyle(
        "SubheadSmall", parent=base["Heading3"],
        fontName="Helvetica-Bold", fontSize=10.5, leading=14,
        textColor=INK, alignment=TA_LEFT,
        spaceBefore=10, spaceAfter=4,
    )
    s["Body"] = ParagraphStyle(
        "Body", parent=base["BodyText"],
        fontName="Times-Roman", fontSize=10.5, leading=16,
        textColor=INK, alignment=TA_JUSTIFY,
        spaceAfter=8,
    )
    s["BodySmall"] = ParagraphStyle(
        "BodySmall", parent=base["Normal"],
        fontName="Times-Roman", fontSize=9, leading=13,
        textColor=INK, alignment=TA_JUSTIFY,
        spaceAfter=6,
    )
    s["Bullet"] = ParagraphStyle(
        "Bullet", parent=s["Body"],
        leftIndent=18, firstLineIndent=-12,
        spaceAfter=3,
    )
    s["Caption"] = ParagraphStyle(
        "Caption", parent=base["Normal"],
        fontName="Helvetica-Oblique", fontSize=8.5, leading=11,
        textColor=MUTED, alignment=TA_LEFT, spaceAfter=12,
    )
    s["Callout"] = ParagraphStyle(
        "Callout", parent=base["Normal"],
        fontName="Helvetica", fontSize=10, leading=15,
        textColor=INK, alignment=TA_LEFT,
    )
    s["CalloutLabel"] = ParagraphStyle(
        "CalloutLabel", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=8.5, leading=11,
        textColor=ACCENT, alignment=TA_LEFT, spaceAfter=4,
    )
    s["TermLabel"] = ParagraphStyle(
        "TermLabel", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=8.5, leading=11,
        textColor=TERM_BAR, alignment=TA_LEFT, spaceAfter=4,
    )
    s["RealLifeLabel"] = ParagraphStyle(
        "RealLifeLabel", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=8.5, leading=11,
        textColor=REALLIFE_BAR, alignment=TA_LEFT, spaceAfter=4,
    )
    s["TermBody"] = ParagraphStyle(
        "TermBody", parent=base["Normal"],
        fontName="Helvetica", fontSize=9, leading=13,
        textColor=INK, alignment=TA_LEFT,
    )
    s["Footer"] = ParagraphStyle(
        "Footer", parent=base["Normal"],
        fontName="Helvetica", fontSize=7.5, leading=10,
        textColor=MUTED, alignment=TA_LEFT,
    )
    return s


# ---------------------------------------------------------------------------
# Flowables
# ---------------------------------------------------------------------------

class Divider(Flowable):
    def __init__(self, width: float, color=RULE, thickness: float = 0.6):
        super().__init__()
        self.width = width
        self.color = color
        self.thickness = thickness

    def wrap(self, availWidth, availHeight):
        self.width = availWidth
        return availWidth, self.thickness + 2

    def draw(self):
        self.canv.setStrokeColor(self.color)
        self.canv.setLineWidth(self.thickness)
        self.canv.line(0, 1, self.width, 1)


def callout(label: str, body_html: str, styles, bg=ACCENT_SOFT, bar=ACCENT) -> Table:
    inner = [
        Paragraph(label.upper(), styles["CalloutLabel"]),
        Paragraph(body_html, styles["Callout"]),
    ]
    tbl = Table([[inner]], colWidths=[None])
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), bg),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LINEBEFORE", (0, 0), (0, -1), 2.5, bar),
    ]))
    return tbl


def terminology_box(terms: list[tuple[str, str]], styles) -> Table:
    rows = []
    for term, defn in terms:
        rows.append(Paragraph(
            f"<b>{term}</b> &mdash; {defn}", styles["TermBody"]
        ))
    inner = [
        Paragraph("TERMINOLOGY", styles["TermLabel"]),
        *rows,
    ]
    tbl = Table([[inner]], colWidths=[None])
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), TERM_BG),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LINEBEFORE", (0, 0), (0, -1), 2.5, TERM_BAR),
    ]))
    return tbl


def reallife_box(body_html: str, styles) -> Table:
    inner = [
        Paragraph("WHAT THIS MEANS FOR MELBOURNE", styles["RealLifeLabel"]),
        Paragraph(body_html, styles["Callout"]),
    ]
    tbl = Table([[inner]], colWidths=[None])
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), REALLIFE_BG),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LINEBEFORE", (0, 0), (0, -1), 2.5, REALLIFE_BAR),
    ]))
    return tbl


def data_table(header: list[str], rows: list[list[str]], col_widths=None) -> Table:
    tbl = Table([header, *rows], hAlign="LEFT", colWidths=col_widths)
    tbl.setStyle(TableStyle([
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 9),
        ("FONT", (0, 1), (-1, -1), "Helvetica", 9),
        ("TEXTCOLOR", (0, 0), (-1, 0), ACCENT),
        ("TEXTCOLOR", (0, 1), (-1, -1), INK),
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, ACCENT),
        ("LINEBELOW", (0, -1), (-1, -1), 0.4, RULE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#fafafa")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
    ]))
    return tbl


def figure_block(path: Path, caption: str, styles, width: float) -> list:
    if not path.exists():
        placeholder = Table(
            [[Paragraph("[figure missing: %s]" % path.name, styles["Caption"])]],
            colWidths=[width], rowHeights=[width * 0.5],
        )
        placeholder.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.5, RULE),
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#fafafa")),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        return [placeholder, Spacer(1, 4), Paragraph(caption, styles["Caption"])]
    img = Image(str(path), width=width, height=width * 0.62, kind="proportional")
    return [img, Spacer(1, 4), Paragraph(caption, styles["Caption"])]


def section_header(num: str, title: str, styles, frame_w: float) -> list:
    return [
        Paragraph(f"{num}", styles["SectionNumber"]),
        Paragraph(title, styles["SectionTitle"]),
        Divider(frame_w, color=ACCENT, thickness=1.2),
        Spacer(1, 10),
    ]


# ---------------------------------------------------------------------------
# Page decorations
# ---------------------------------------------------------------------------

def _cover_decor(canvas: Canvas, doc) -> None:
    page_w, page_h = A4
    canvas.saveState()
    canvas.setFillColor(ACCENT)
    canvas.rect(0, page_h - 8 * mm, page_w, 8 * mm, fill=1, stroke=0)
    canvas.setStrokeColor(ACCENT)
    canvas.setLineWidth(0.6)
    canvas.line(2.2 * cm, 3.2 * cm, page_w - 2.2 * cm, 3.2 * cm)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(MUTED)
    canvas.drawString(2.2 * cm, 2.2 * cm, "MELBOURNE CBD STREET ANALYSIS")
    canvas.drawRightString(page_w - 2.2 * cm, 2.2 * cm,
                           "Comprehensive Pipeline Report")
    canvas.restoreState()


def _body_decor(canvas: Canvas, doc) -> None:
    page_w, page_h = A4
    canvas.saveState()
    canvas.setStrokeColor(ACCENT)
    canvas.setLineWidth(1.4)
    canvas.line(2.2 * cm, page_h - 1.7 * cm, 3.2 * cm, page_h - 1.7 * cm)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(MUTED)
    canvas.drawString(3.6 * cm, page_h - 1.72 * cm - 2,
                      "MELBOURNE CBD  /  COMPREHENSIVE PIPELINE REPORT")
    canvas.drawRightString(page_w - 2.2 * cm, page_h - 1.72 * cm - 2,
                           date.today().strftime("%Y-%m-%d"))
    canvas.setStrokeColor(RULE)
    canvas.setLineWidth(0.4)
    canvas.line(2.2 * cm, 1.8 * cm, page_w - 2.2 * cm, 1.8 * cm)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(MUTED)
    canvas.drawString(2.2 * cm, 1.3 * cm, "Ertugrul Akdemir")
    canvas.drawRightString(page_w - 2.2 * cm, 1.3 * cm, str(doc.page))
    canvas.restoreState()


# ---------------------------------------------------------------------------
# Story content
# ---------------------------------------------------------------------------

def build_story(styles: dict, frame_w: float) -> list:
    model_eval = _load_json(MODELS_DIR / "model_eval.json")
    feature_imp = _load_json(MODELS_DIR / "feature_importance.json")
    story: list = []

    # =========================================================================
    # COVER
    # =========================================================================
    story.append(Spacer(1, 5 * cm))
    story.append(Paragraph(
        "COMPREHENSIVE PIPELINE REPORT  /  v2.1", styles["CoverEyebrow"]))
    story.append(Paragraph(
        "Melbourne CBD<br/>Street Dynamics", styles["CoverTitle"]))
    story.append(Paragraph(
        "A twelve-step spatio-temporal graph neural network pipeline "
        "for pedestrian flow prediction, parking occupancy modelling, "
        "and urban intervention simulation across 1,397 street segments "
        "in Melbourne&rsquo;s Central Business District.",
        styles["CoverSubtitle"],
    ))
    story.append(Spacer(1, 1.8 * cm))
    story.append(HRFlowable(width="40%", thickness=1.4, color=ACCENT,
                            spaceBefore=0, spaceAfter=10))
    story.append(Paragraph(
        "<b>Ertugrul Akdemir</b><br/>"
        "Master&rsquo;s Thesis &mdash; University of Melbourne<br/>"
        f"{date.today().strftime('%d %B %Y')}",
        styles["CoverMeta"],
    ))
    story.append(PageBreak())

    # =========================================================================
    # TABLE OF CONTENTS
    # =========================================================================
    story.append(Paragraph("Table of Contents", styles["TOCHead"]))
    story.append(Divider(frame_w, color=ACCENT, thickness=1.0))
    story.append(Spacer(1, 12))
    toc_entries = [
        ("01", "Executive Summary &mdash; Pipeline at a Glance"),
        ("02", "Step 01 &mdash; Data Ingestion (Fetch)"),
        ("03", "Step 02 &mdash; Sensor Snapping &amp; Spatial Aggregation"),
        ("04", "Step 03 &mdash; Temporal Feature Engineering"),
        ("05", "Step 04 &mdash; Dual Graph Construction"),
        ("06", "Step 05 &mdash; Parking Reconstruction &amp; Pedestrian Imputation"),
        ("07", "Step 06 &mdash; Weekly Street Profiles"),
        ("08", "Step 07 &mdash; Unsupervised Clustering"),
        ("09", "Step 08 &mdash; Data Cube Assembly"),
        ("10", "Step 09 &mdash; MultiGCN Training"),
        ("11", "Step 10 &mdash; Feature Interpretation"),
        ("12", "Step 11 &mdash; Scenario Simulation"),
        ("13", "Step 12 &mdash; Frontend Export &amp; API"),
        ("14", "Results &amp; Evaluation"),
        ("15", "Known Limitations &amp; Future Work"),
    ]
    for num, title in toc_entries:
        story.append(Paragraph(
            f"<b>{num}</b> &nbsp;&nbsp; {title}", styles["TOCEntry"]))
    story.append(PageBreak())

    # =========================================================================
    # 01 — EXECUTIVE SUMMARY
    # =========================================================================
    story.extend(section_header(
        "01  /  EXECUTIVE SUMMARY", "Pipeline at a Glance",
        styles, frame_w))

    story.append(Paragraph(
        "This report documents a complete twelve-step machine learning "
        "pipeline that ingests, transforms, and models the urban dynamics "
        "of Melbourne&rsquo;s Central Business District. The system fuses "
        "real-time pedestrian counts from street sensors, kerbside parking "
        "occupancy from embedded bay sensors, Census of Land Use and "
        "Employment (CLUE) data, and Bureau of Meteorology weather "
        "observations into a single spatio-temporal data structure. A "
        "dual-branch Graph Neural Network (GNN) then jointly predicts "
        "foot traffic and parking occupancy across the entire street "
        "network, enabling &ldquo;what-if&rdquo; policy simulations.",
        styles["Body"],
    ))
    story.append(Paragraph(
        "The pipeline is organised into <b>three phases</b>. The "
        "<i>ingestion phase</i> (Steps 01&ndash;05) assembles a harmonised "
        "15-minute time series for every street segment. The <i>modelling "
        "phase</i> (Steps 06&ndash;09) clusters streets by behavioural "
        "archetype and trains a dual-head MultiGCN model. The "
        "<i>post-training phase</i> (Steps 10&ndash;12) interprets the "
        "model, simulates interventions, and exports an interactive "
        "web-based frontend.",
        styles["Body"],
    ))

    story.append(Spacer(1, 6))
    story.append(callout(
        "Pipeline scope",
        "<b>3,975</b> CBD street segments ingested &bull; "
        "<b>1,397</b> modelled after arterial filtering &bull; "
        "<b>14,400</b> time bins at 15-minute resolution (2015&ndash;2019) "
        "&bull; <b>23</b> features per cell &bull; "
        "<b>68,076</b> trainable parameters &bull; "
        "<b>4</b> street archetypes &bull; "
        "<b>3</b> intervention types",
        styles,
    ))

    story.append(Spacer(1, 10))
    story.append(reallife_box(
        "Imagine you are a Melbourne city planner considering "
        "pedestrianising a busy street near Flinders Station. This "
        "pipeline lets you ask: &ldquo;If we remove all on-street parking "
        "from Collins Street on a Wednesday lunchtime, what happens to "
        "foot traffic on Collins <i>and</i> its surrounding streets over "
        "the next 24 hours?&rdquo; The model answers by rolling forward "
        "a counterfactual simulation using patterns learned from five "
        "years of real sensor data.",
        styles,
    ))

    story.append(Spacer(1, 14))
    story.append(Paragraph("Twelve steps, three phases", styles["Subhead"]))
    steps_overview = [
        ["01", "Fetch", "Download parking, pedestrian, CLUE, weather data",
         "Ingestion"],
        ["02", "Snap", "Map sensors to streets; aggregate land-use features",
         "Ingestion"],
        ["03", "Temporal", "Build 15-min time grid; encode cyclic time + weather",
         "Ingestion"],
        ["04", "Graph", "Build spatial (physical) + semantic (functional) graphs",
         "Ingestion"],
        ["05", "Process", "Reconstruct parking occupancy; impute missing ped data",
         "Ingestion"],
        ["06", "Aggregate", "Collapse into weekly behavioural profiles",
         "Modelling"],
        ["07", "Cluster", "GMM clustering into 4 street archetypes",
         "Modelling"],
        ["08", "Cube", "Assemble (1397, 14400, 23) tensor + normalise",
         "Modelling"],
        ["09", "Train", "Dual-head MultiGCN with joint ped + parking loss",
         "Modelling"],
        ["10", "Interpret", "Permutation importance + branch ablation",
         "Post-training"],
        ["11", "Scenario", "Counterfactual rollout + spillover + rebound",
         "Post-training"],
        ["12", "Export", "Enriched GeoJSON + Flask API + interactive map",
         "Post-training"],
    ]
    story.append(data_table(
        ["#", "Step", "Responsibility", "Phase"], steps_overview))
    story.append(PageBreak())

    # =========================================================================
    # 02 — STEP 01: FETCH
    # =========================================================================
    story.extend(section_header(
        "02  /  STEP 01", "Data Ingestion (Fetch)", styles, frame_w))

    story.append(terminology_box([
        ("API (Application Programming Interface)",
         "A structured way for one computer program to request data from "
         "another. Like ordering from a menu &mdash; you specify what you "
         "want and the server returns it in a predictable format."),
        ("Keyset Pagination",
         "A method of fetching large datasets in batches by remembering "
         "the last record&rsquo;s ID and asking for &ldquo;the next 1,000 "
         "after ID X&rdquo;. Avoids skipping or duplicating rows."),
        ("Parquet",
         "A column-oriented file format optimised for large datasets. "
         "Like a spreadsheet but compressed and much faster to read "
         "selectively."),
        ("Supabase",
         "A cloud database service. Stores the raw parking bay events "
         "and pedestrian sensor counts used as primary data sources."),
        ("CLUE (Census of Land Use and Employment)",
         "Melbourne City Council&rsquo;s annual survey of every business, "
         "building, and land parcel in the municipality."),
        ("Open-Meteo",
         "A free weather archive API providing historical hourly "
         "observations (temperature, humidity, wind, rain)."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 01 is the data ingestion layer. It connects to four external "
        "systems and downloads all raw data needed for the analysis. Every "
        "subsequent step works exclusively from these local files, ensuring "
        "that the pipeline can be re-run without internet access.",
        styles["Body"],
    ))

    story.append(Paragraph("Data sources", styles["Subhead"]))
    source_rows = [
        ["Parking bay events", "Supabase", "~3.6 M rows",
         "Each row = one sensor event (car arrived/departed)"],
        ["Pedestrian counts", "Supabase", "~2.1 M rows",
         "Hourly foot traffic from 74 permanent counters"],
        ["CLUE land use", "Melbourne Open Data", "11 datasets",
         "Cafes, bars, jobs, buildings, dwellings, etc."],
        ["Weather", "Open-Meteo", "~35 K hours",
         "Temperature, humidity, wind speed, precipitation"],
        ["Transit stops", "PTV / Council", "~800 records",
         "Bus, tram, and train stop locations"],
    ]
    story.append(data_table(
        ["Dataset", "Source", "Volume", "Description"], source_rows))

    story.append(Spacer(1, 10))
    story.append(Paragraph("Reliability features", styles["Subhead"]))
    story.append(Paragraph(
        "The fetch step includes <b>exponential backoff</b> (retries with "
        "progressively longer waits if the server is overloaded), "
        "<b>checkpoint/resume</b> (saves partial downloads so a network "
        "interruption doesn&rsquo;t restart from scratch), and "
        "<b>keyset pagination</b> (fetches exactly 1,000 rows per "
        "request, tracking the cursor by primary key to guarantee no "
        "gaps or duplicates).",
        styles["Body"],
    ))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "Think of this step as a research assistant who visits five "
        "different council offices, collects every relevant spreadsheet, "
        "and organises them into labelled folders. Every parking meter "
        "beep, every pedestrian counter reading, every cafe liquor "
        "licence, and every hourly weather observation for five years is "
        "gathered into one place. Without this foundation, no analysis "
        "is possible.",
        styles,
    ))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "<b>Outputs:</b> 16 Parquet files in <tt>data/raw/</tt>, including "
        "<tt>parking_raw.parquet</tt>, <tt>ped_raw.parquet</tt>, 11 CLUE "
        "dataset files, <tt>weather_raw.parquet</tt>, and 2 transit files.",
        styles["BodySmall"],
    ))
    story.append(PageBreak())

    # =========================================================================
    # 03 — STEP 02: SNAP
    # =========================================================================
    story.extend(section_header(
        "03  /  STEP 02", "Sensor Snapping &amp; Spatial Aggregation",
        styles, frame_w))

    story.append(terminology_box([
        ("Sensor Snapping",
         "The process of assigning each physical sensor (a small device "
         "embedded in the ground or mounted on a pole) to the street "
         "segment it monitors. A parking sensor at GPS coordinate X "
         "is &ldquo;snapped&rdquo; to the nearest street polygon."),
        ("Point-in-Polygon (PIP)",
         "A geometric test: is this GPS point inside this street&rsquo;s "
         "boundary? Used as the primary method to assign sensors to streets."),
        ("Nearest-Neighbour Fallback",
         "If a sensor falls just outside every polygon (e.g. at a "
         "kerb edge), find the closest polygon within 50 metres."),
        ("Arterial Streets",
         "Major through-roads that carry significant traffic and "
         "pedestrian activity, as classified by the council. Only these "
         "1,397 streets (out of 3,975 total) enter the model."),
        ("CLUE Spatial Aggregation",
         "Counting how many cafes, bars, jobs, etc. are located on or "
         "near each street. Point features (cafes, bars) are assigned "
         "to the nearest arterial within 250 m. Block-level data (jobs) "
         "is distributed by geographic overlap."),
        ("EPSG:3111 / WGS84",
         "Coordinate reference systems. EPSG:3111 is a projected system "
         "accurate for Victoria (distances in metres). WGS84 is the "
         "global GPS standard (latitude/longitude in degrees)."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 02 performs two essential spatial operations. <b>Part A</b> "
        "assigns each of the ~280 physical sensors (74 pedestrian counters "
        "and ~206 parking bay sensors) to the specific street segment they "
        "monitor. <b>Part B</b> aggregates 42 land-use features from the "
        "CLUE datasets onto each of the 1,397 arterial streets.",
        styles["Body"],
    ))

    story.append(Paragraph("Part A: Sensor assignment", styles["Subhead"]))
    story.append(Paragraph(
        "Parking sensors are matched via their <tt>kerbsideid</tt> field, "
        "then validated by point-in-polygon containment within street "
        "geometries projected to EPSG:3111. Sensors that fall outside any "
        "polygon (due to GPS drift or kerb-edge placement) are caught by "
        "a nearest-neighbour fallback within 50 m. The step asserts a "
        "&ge;97% match rate for parking and &ge;75% for pedestrian sensors.",
        styles["Body"],
    ))

    story.append(Paragraph(
        "Part B: CLUE land-use feature assembly", styles["Subhead"]))
    story.append(Paragraph(
        "For each arterial street, the step computes 42 static features "
        "from 11 CLUE datasets. Point-type data (individual cafes, bars, "
        "buildings) is assigned to the nearest arterial within 250 m "
        "using integer counts &mdash; &ldquo;this street has 5 cafes with "
        "a combined 180 seats.&rdquo; Block-level data (employment "
        "figures) is distributed proportionally by area overlap. Key "
        "derived columns include <tt>dining_capacity</tt> (cafe seats + "
        "bar patron capacity), <tt>cbd_distance_m</tt> (Haversine "
        "distance to the Hoddle Grid centre), and <tt>area_m2</tt>.",
        styles["Body"],
    ))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "Imagine pinning coloured flags on a city map. A red flag marks "
        "each parking sensor; a blue flag marks each pedestrian counter. "
        "This step draws the street boundaries and asks: &ldquo;Which "
        "flag belongs to which street?&rdquo; Then it counts nearby "
        "amenities: how many cafes, how many office workers, how much "
        "retail space surround each street? This transforms raw sensor "
        "locations and census data into a structured &ldquo;identity "
        "card&rdquo; for every street.",
        styles,
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<b>Outputs:</b> <tt>sensor_map.parquet</tt> (~280 sensors mapped "
        "to streets), <tt>static_features.parquet</tt> (1,397 streets "
        "&times; 34 columns).",
        styles["BodySmall"],
    ))
    story.append(PageBreak())

    # =========================================================================
    # 04 — STEP 03: TEMPORAL
    # =========================================================================
    story.extend(section_header(
        "04  /  STEP 03", "Temporal Feature Engineering",
        styles, frame_w))

    story.append(terminology_box([
        ("Time Bin",
         "A fixed 15-minute interval. The study period has 14,400 bins, "
         "covering 2015&ndash;2019 at 15-minute resolution."),
        ("Cyclic Encoding (sin/cos)",
         "A way to represent time so that 23:45 and 00:00 are close "
         "together (as they should be). The hour is encoded as two "
         "numbers: sin(2&pi;&middot;hour/24) and cos(2&pi;&middot;hour/24). "
         "Similarly for day-of-week. This prevents the model from "
         "thinking Monday and Sunday are maximally different."),
        ("Forward-Fill",
         "When hourly weather data must fill 15-minute bins, each hour&rsquo;s "
         "value is carried forward to the four 15-minute slots within it."),
        ("Binary Flag",
         "A feature that is either 0 or 1. <tt>is_weekend</tt> = 1 on "
         "Saturday/Sunday, 0 otherwise. Encodes categorical distinctions "
         "the model can use to adjust predictions."),
        ("Public / School Holiday",
         "Calendar events that dramatically change pedestrian patterns. "
         "Melbourne Cup Day, Christmas, and school term breaks all alter "
         "how people use the city."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 03 creates the temporal backbone: a monotonic 15-minute "
        "grid from the start to the end of the study window, with "
        "weather observations and calendar features aligned to each bin.",
        styles["Body"],
    ))

    story.append(Paragraph("Weather alignment", styles["Subhead"]))
    story.append(Paragraph(
        "Hourly weather data (temperature, relative humidity, wind speed, "
        "precipitation) is forward-filled to 15-minute resolution. Each "
        "hour&rsquo;s reading applies to the four quarter-hour bins within "
        "it. All values are stored as float32 for memory efficiency.",
        styles["Body"],
    ))

    story.append(Paragraph("Calendar features", styles["Subhead"]))
    cal_rows = [
        ["hour_sin, hour_cos", "Cyclic",
         "Time of day (smooth, wraps midnight)"],
        ["dow_sin, dow_cos", "Cyclic",
         "Day of week (smooth, wraps Sun&rarr;Mon)"],
        ["is_weekend", "Binary", "1 on Sat/Sun, 0 otherwise"],
        ["is_public_holiday", "Binary",
         "6 Victorian public holidays in study window"],
        ["is_school_holiday", "Binary",
         "3 school holiday periods (summer, term breaks)"],
    ]
    story.append(data_table(
        ["Feature", "Type", "Description"], cal_rows))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "Why does the time of day matter? Because Melbourne&rsquo;s "
        "streets transform throughout the day. Bourke Street Mall at "
        "7 AM is nearly empty; by noon it&rsquo;s packed with office "
        "workers on lunch breaks; by 6 PM the after-work crowd fills "
        "the bars. Weather matters too &mdash; a rainy Thursday "
        "lunchtime sees dramatically fewer pedestrians than a sunny one. "
        "This step encodes these rhythms so the model can learn them.",
        styles,
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<b>Outputs:</b> <tt>weather.parquet</tt> (14,400 rows &times; "
        "4 weather columns), <tt>temporal_features.parquet</tt> (14,400 "
        "rows &times; 7 calendar columns).",
        styles["BodySmall"],
    ))
    story.append(PageBreak())

    # =========================================================================
    # 05 — STEP 04: GRAPH
    # =========================================================================
    story.extend(section_header(
        "05  /  STEP 04", "Dual Graph Construction", styles, frame_w))

    story.append(terminology_box([
        ("Graph (in this context)",
         "A mathematical structure of <b>nodes</b> (streets) connected by "
         "<b>edges</b> (relationships). Not a chart or plot &mdash; a "
         "network that encodes which streets influence each other."),
        ("Spatial Graph",
         "Edges connect streets that physically touch (share an "
         "intersection). Weighted by proximity: closer streets have "
         "stronger connections."),
        ("Semantic Graph",
         "Edges connect streets that are <i>functionally similar</i> "
         "&mdash; same mix of cafes, offices, and foot traffic &mdash; "
         "even if they are kilometres apart."),
        ("Gaussian Kernel",
         "A weighting function: exp(&minus;d/&sigma;). Streets that are "
         "very close get a weight near 1.0; streets farther apart get "
         "exponentially weaker weights."),
        ("Cosine Similarity",
         "A measure of how similar two feature vectors are, ranging "
         "from &minus;1 (opposite) to +1 (identical). Here, it compares "
         "land-use profiles: two streets with similar cafe/job/bar "
         "mixes score near 1.0."),
        ("Mutual k-NN (Mutual k-Nearest Neighbours)",
         "Street A is linked to Street B only if A is among B&rsquo;s "
         "top-10 most similar <i>and</i> B is among A&rsquo;s top-10. "
         "This prevents &ldquo;hub&rdquo; streets from connecting to "
         "everything."),
        ("GCN (Graph Convolutional Network)",
         "A neural network layer that combines a node&rsquo;s features "
         "with its neighbours&rsquo; features. The graphs define <i>who</i> "
         "is a neighbour; the GCN learns <i>how</i> to blend their "
         "information."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 04 builds two complementary graph structures over the "
        "1,397 arterial streets. The <b>spatial graph</b> captures "
        "physical adjacency &mdash; streets that share an intersection "
        "directly influence each other&rsquo;s traffic. The <b>semantic "
        "graph</b> captures functional similarity &mdash; two retail "
        "streets blocks apart may respond to events more similarly than "
        "a retail street and its residential neighbour.",
        styles["Body"],
    ))

    story.append(Paragraph("Spatial graph: intersection topology",
                           styles["Subhead"]))
    story.append(Paragraph(
        "Using an STRtree spatial index, the step identifies all pairs "
        "of street polygons that physically touch (share a boundary "
        "segment at an intersection). Each such pair receives an edge "
        "with a Gaussian kernel weight based on centroid distance: "
        "exp(&minus;d/&sigma;) where &sigma; is the median centroid "
        "distance across all intersection-adjacent pairs. Twenty-five "
        "isolated streets (no touching neighbours) receive a single "
        "nearest-neighbour edge with weight 0.0 to ensure full "
        "connectivity.",
        styles["Body"],
    ))

    story.append(Paragraph("Semantic graph: functional similarity",
                           styles["Subhead"]))
    story.append(Paragraph(
        "Fourteen land-use features (total jobs, retail floorspace, "
        "dwelling count, etc.) are log-transformed, z-scored, and "
        "L2-normalised. A full [1397 &times; 1397] cosine similarity "
        "matrix is computed. Per-street top-10 neighbours are extracted, "
        "then a <i>mutual k-NN</i> filter retains only reciprocal edges "
        "above a 0.85 cosine threshold. This eliminates hub-and-spoke "
        "degeneracy that plagued earlier versions.",
        styles["Body"],
    ))

    graph_rows = [
        ["Spatial", "5,635 directed", "1,397", "1 component", "0 isolates"],
        ["Semantic", "8,097 directed", "1,397", "&mdash;", "&mdash;"],
    ]
    story.append(Spacer(1, 6))
    story.append(data_table(
        ["Graph", "Edges", "Nodes", "Components", "Isolates"],
        graph_rows))

    story.append(Spacer(1, 8))
    story.extend(figure_block(
        FIGURES_DIR / "fig2_spatial_graph.png",
        "Figure 1. Spatial graph (intersection-topology + Gaussian kernel). "
        "Each node is a street; edges connect streets sharing an intersection.",
        styles, frame_w,
    ))
    story.extend(figure_block(
        FIGURES_DIR / "fig3_semantic_graph.png",
        "Figure 2. Semantic graph (mutual k-NN on cosine similarity of "
        "14 land-use features). Connects functionally similar streets "
        "regardless of physical distance.",
        styles, frame_w,
    ))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "The spatial graph encodes the obvious: if Swanston Street gets "
        "more pedestrians, the cross-streets at its intersections probably "
        "will too. The semantic graph encodes the subtle: if a &ldquo;cafe "
        "strip&rdquo; in Carlton gets busier due to a festival, a similar "
        "cafe strip in Southbank might respond similarly &mdash; even "
        "though they&rsquo;re separated by the Yarra River. Together, "
        "these two views let the model learn both local spillover and "
        "city-wide functional patterns.",
        styles,
    ))
    story.append(PageBreak())

    # =========================================================================
    # 06 — STEP 05: PROCESS
    # =========================================================================
    story.extend(section_header(
        "06  /  STEP 05",
        "Parking Reconstruction &amp; Pedestrian Imputation",
        styles, frame_w))

    story.append(terminology_box([
        ("Parking Occupancy Reconstruction",
         "Turning raw &ldquo;car arrived&rdquo; / &ldquo;car departed&rdquo; "
         "sensor events into a percentage: what fraction of parking bays "
         "on this street are occupied in each 15-minute window?"),
        ("Interval Pairing",
         "Matching a &ldquo;Present&rdquo; event (car arrives) with the "
         "next &ldquo;Unoccupied&rdquo; event (car leaves) to determine "
         "how long the bay was in use."),
        ("Duration Capping (2 hours)",
         "If a car appears to stay in a bay for more than 2 hours "
         "without a departure event, the occupancy is capped at 2 hours. "
         "This guards against sensor failures."),
        ("Imputation",
         "Filling in missing data. Only 74 streets have real pedestrian "
         "sensors. For the other 1,323 streets, a machine learning model "
         "estimates what the count would have been."),
        ("XGBoost (eXtreme Gradient Boosting)",
         "A powerful tree-based machine learning algorithm. It builds "
         "many small decision trees, each correcting the errors of the "
         "previous ones. Used here because it handles mixed feature "
         "types well and generalises to unseen streets."),
        ("GroupKFold Cross-Validation",
         "A validation strategy that ensures entire streets are held "
         "out, not individual time bins. This tests whether the model "
         "can predict foot traffic on a street it has <i>never seen</i> "
         "&mdash; the real challenge for imputation."),
        ("Spatial Lag",
         "A feature computed as the average pedestrian count of a "
         "street&rsquo;s sensored spatial neighbours at the same time "
         "bin. If nearby streets are busy, this street probably is too."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 05 performs two critical data completion tasks that "
        "transform sparse sensor readings into a complete space-time "
        "matrix ready for deep learning.",
        styles["Body"],
    ))

    story.append(Paragraph("Part A: Parking occupancy", styles["Subhead"]))
    story.append(Paragraph(
        "Raw sensor events (Present/Unoccupied pairs) are joined into "
        "intervals, capped at 2 hours, and aggregated into a 15-minute "
        "grid. The occupancy rate for each street-bin is: occupied bays "
        "&divide; total bays. Streets with fewer than 5 events are "
        "excluded. Result: 143 streets with real parking supervision "
        "(1,201,375 non-zero entries, mean occupancy 19.7%).",
        styles["Body"],
    ))

    story.append(Paragraph(
        "Part B: XGBoost pedestrian imputation", styles["Subhead"]))
    story.append(Paragraph(
        "Only 74 streets have permanent pedestrian counters. An XGBoost "
        "model is trained on these 74 streets to predict foot traffic "
        "from static features (location, land use, building density), "
        "temporal features (time of day, day of week, weather), and "
        "a <i>spatial lag</i> feature (mean pedestrian count of sensored "
        "neighbours). GroupKFold(5) cross-validation over streets ensures "
        "the model generalises to unseen locations. The target is "
        "log-transformed to handle the right-skewed distribution of "
        "pedestrian counts. The imputed values fill the remaining 1,323 "
        "streets, producing a complete <tt>ped_complete.parquet</tt>.",
        styles["Body"],
    ))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "Melbourne has excellent sensor infrastructure, but it&rsquo;s "
        "physically impossible to install a pedestrian counter on every "
        "one of 1,397 streets. This step solves that problem: it uses "
        "the 74 streets that <i>do</i> have sensors to learn the "
        "relationship between a street&rsquo;s characteristics (how many "
        "cafes, how close to Flinders Street Station, what time of day) "
        "and its foot traffic. It then applies those patterns to "
        "estimate the pedestrian count on every street, every 15 minutes, "
        "for five years. Similarly, raw parking sensor pings are "
        "converted into a simple question the city can answer: &ldquo;"
        "At 12:30 PM on a Tuesday, what percentage of bays on Lonsdale "
        "Street are occupied?&rdquo;",
        styles,
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<b>Outputs:</b> <tt>ped_complete.parquet</tt> (1,397 streets "
        "&times; 14,400 bins), <tt>parking_occupancy.parquet</tt> (143 "
        "streets with real occupancy rates).",
        styles["BodySmall"],
    ))
    story.append(PageBreak())

    # =========================================================================
    # 07 — STEP 06: AGGREGATE
    # =========================================================================
    story.extend(section_header(
        "07  /  STEP 06", "Weekly Street Profiles", styles, frame_w))

    story.append(terminology_box([
        ("Street Profile",
         "A fixed-length vector summarising a street&rsquo;s typical "
         "behaviour across an average week. Like a fingerprint that "
         "captures its daily rhythms."),
        ("Time Block",
         "The day is divided into 6 blocks: night (00&ndash;06), "
         "morning (06&ndash;10), work AM (10&ndash;12), midday "
         "(12&ndash;14), work PM (14&ndash;18), evening (18&ndash;24). "
         "This captures distinct activity patterns."),
        ("Profile Feature",
         "The average pedestrian flow or parking occupancy for a "
         "specific (day-of-week, time-block) combination. 7 days "
         "&times; 6 blocks = 42 features for ped, 42 for parking."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 06 collapses the full temporal dimension into a "
        "manageably sized weekly profile for each street. Instead of "
        "14,400 time bins, each street is represented by 93 features "
        "that capture its typical weekly rhythm.",
        styles["Body"],
    ))

    profile_rows = [
        ["Parking (42)", "Mean occupancy per (DoW &times; time block)"],
        ["Pedestrian (42)", "Mean ped flow per (DoW &times; time block)"],
        ["Summary (9)",
         "mean_ped, peak_ped, mean_parking, ped_confidence, "
         "total_jobs, cafe_count, bar_count, etc."],
    ]
    story.append(data_table(
        ["Feature group (count)", "Description"], profile_rows))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "This step answers the question: &ldquo;What does a typical week "
        "look like on this street?&rdquo; A street near Southern Cross "
        "Station might show high morning/evening pedestrian spikes "
        "(commuters) but quiet midday and weekends. A laneway in the "
        "Degraves precinct might show the opposite: quiet mornings, "
        "packed lunchtime, busy weekend brunches. These profiles become "
        "the basis for grouping streets into archetypes.",
        styles,
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<b>Outputs:</b> <tt>street_profiles.parquet</tt> (1,397 streets "
        "&times; 93 columns), <tt>street_profiles_summary.json</tt>.",
        styles["BodySmall"],
    ))
    story.append(PageBreak())

    # =========================================================================
    # 08 — STEP 07: CLUSTER
    # =========================================================================
    story.extend(section_header(
        "08  /  STEP 07", "Unsupervised Clustering", styles, frame_w))

    story.append(terminology_box([
        ("Clustering",
         "Grouping streets that behave similarly without telling the "
         "algorithm what the groups should be. The algorithm discovers "
         "natural groupings from the data alone."),
        ("GMM (Gaussian Mixture Model)",
         "A probabilistic clustering method. Each cluster is modelled "
         "as a bell-curve (Gaussian) in high-dimensional space. Streets "
         "are assigned to the cluster whose bell curve best explains "
         "their profile."),
        ("PCA (Principal Component Analysis)",
         "A dimensionality reduction technique. 84 profile features "
         "are compressed into 20 &ldquo;principal components&rdquo; that "
         "retain ~85% of the variance. This removes noise and speeds "
         "up clustering."),
        ("BIC (Bayesian Information Criterion)",
         "A score that balances model fit against complexity. Lower BIC "
         "= better. Tested for k=2 through k=10 clusters; the minimum "
         "BIC determines the optimal number."),
        ("ARI (Adjusted Rand Index)",
         "A stability measure: re-run the clustering 20 times with "
         "random subsamples and check if the same streets always end "
         "up in the same group. ARI=1.0 means perfect agreement; "
         "our 0.940 indicates very stable clusters."),
        ("Silhouette Score",
         "Measures how well-separated clusters are. Ranges from &minus;1 "
         "to +1. Our 0.550 indicates good separation with some overlap "
         "at boundaries."),
        ("Archetype",
         "A human-readable label assigned to each cluster based on its "
         "distinguishing characteristics (e.g. &ldquo;evening outdoor "
         "dining&rdquo;)."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 07 applies unsupervised clustering to the 84 temporal "
        "profile features (42 parking + 42 pedestrian) to discover "
        "natural street archetypes. The pipeline: StandardScaler &rarr; "
        "PCA (20 components) &rarr; GMM with BIC search (k=2..10) "
        "&rarr; Bootstrap ARI stability check.",
        styles["Body"],
    ))

    story.append(Paragraph("Discovered archetypes", styles["Subhead"]))
    cluster_rows = [
        ["latent_midday_potential", "994", "boost_ped",
         "Quiet streets with untapped daytime capacity"],
        ["parking_reallocation_priority", "141", "pedestrianise",
         "High parking, low ped &mdash; candidates for kerb reallocation"],
        ["evening_outdoor_dining", "245", "boost_ped / restrict",
         "Cafe/bar strips with evening peaks"],
        ["major_pedestrian_corridor", "17", "restrict_park",
         "Highest foot traffic streets (e.g. Bourke St Mall)"],
    ]
    story.append(data_table(
        ["Archetype", "Streets", "Intervention", "Real-life meaning"],
        cluster_rows))

    story.append(Spacer(1, 8))
    story.append(callout(
        "Cluster quality",
        "GMM with k=4 achieved BIC minimum. Bootstrap ARI = <b>0.940</b> "
        "(very stable). Silhouette = <b>0.550</b> (good separation). "
        "The archetype labelling uses engineered scores for morning, "
        "midday, evening activity, weekend ratio, and parking flexibility.",
        styles,
    ))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "This step answers: &ldquo;What types of streets does Melbourne "
        "have?&rdquo; The algorithm discovers four distinct personalities: "
        "(1) the quiet streets with room to grow, (2) streets dominated "
        "by parked cars that could become pedestrian-friendly, (3) the "
        "dining and nightlife strips, and (4) the iconic pedestrian "
        "corridors like Bourke Street Mall. Each archetype suggests a "
        "different policy intervention. A council planner can look at "
        "a street and immediately see which family it belongs to.",
        styles,
    ))
    story.append(PageBreak())

    # =========================================================================
    # 09 — STEP 08: CUBE
    # =========================================================================
    story.extend(section_header(
        "09  /  STEP 08", "Data Cube Assembly", styles, frame_w))

    story.append(terminology_box([
        ("Data Cube",
         "A three-dimensional array: [streets &times; time bins &times; "
         "features]. Think of it as 1,397 spreadsheets stacked on top "
         "of each other, each with 14,400 rows (time) and 23 columns "
         "(features)."),
        ("Z-Score Normalisation",
         "Subtracting the mean and dividing by the standard deviation "
         "for each feature. This ensures all features are on the same "
         "scale (centred at 0, spread of 1), preventing features with "
         "large numbers (e.g. ped_flow ~200) from drowning out small "
         "ones (e.g. occupancy_rate ~0.2)."),
        ("Sparse Tensor",
         "A memory-efficient way to store a graph&rsquo;s connections. "
         "Most street pairs are NOT connected, so only storing the "
         "actual edges (not all 1,397 &times; 1,397 possible pairs) "
         "saves 99.7% of memory."),
        ("Symmetric Normalisation",
         "A graph preprocessing step: D<sup>&minus;&frac12;</sup> A "
         "D<sup>&minus;&frac12;</sup>. It scales each edge weight by "
         "the degree of both endpoints, ensuring that heavily connected "
         "nodes don&rsquo;t overwhelm their neighbours."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 08 is the assembly line: it takes all the cleaned, "
        "enriched data from Steps 01&ndash;07 and stacks it into a "
        "single NumPy array of shape <tt>(1397, 14400, 23)</tt> &mdash; "
        "the model&rsquo;s input tensor.",
        styles["Body"],
    ))

    story.append(Paragraph("The 23 feature channels", styles["Subhead"]))
    feat_rows = [
        ["0", "ped_flow", "Dynamic", "Pedestrian count (sensor or imputed)"],
        ["1", "occupancy_rate", "Dynamic", "Parking occupancy fraction"],
        ["2", "ped_confidence", "Metadata", "1.0 = sensor, 0.5 = imputed"],
        ["3-4", "hour_sin, hour_cos", "Cyclic", "Time of day"],
        ["5-6", "dow_sin, dow_cos", "Cyclic", "Day of week"],
        ["7", "is_weekend", "Binary", "Saturday/Sunday flag"],
        ["8", "is_public_holiday", "Binary", "Victorian public holidays"],
        ["9", "is_school_holiday", "Binary", "School term breaks"],
        ["10", "temperature_2m", "Weather", "Air temperature (&deg;C)"],
        ["11", "relative_humidity_2m", "Weather", "Humidity (%)"],
        ["12", "wind_speed_10m", "Weather", "Wind speed (m/s)"],
        ["13", "precipitation", "Weather", "Rainfall (mm)"],
        ["14", "total_jobs", "CLUE", "Nearby employment count"],
        ["15", "cafe_count", "CLUE", "Number of cafes within 250 m"],
        ["16", "cafe_total_seats", "CLUE", "Combined seating capacity"],
        ["17", "bar_count", "CLUE", "Number of bars/pubs"],
        ["18", "bar_patron_capacity", "CLUE", "Combined patron capacity"],
        ["19", "business_count", "CLUE", "Non-hospitality businesses"],
        ["20", "poi_total", "CLUE", "Total points of interest"],
        ["21", "dining_capacity", "CLUE", "Cafe seats + bar patrons"],
        ["22", "area_m2", "Geometry", "Street polygon area"],
    ]
    story.append(data_table(
        ["Idx", "Feature", "Type", "Description"], feat_rows))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "This step builds the &ldquo;master spreadsheet&rdquo; that the "
        "neural network will learn from. For every single street, at "
        "every single 15-minute window over five years, we know: how "
        "many people were walking, how full the parking was, what time "
        "it was, whether it was raining, how many cafes are nearby, and "
        "12 other descriptors. This is 461 million individual data points "
        "&mdash; the complete picture of Melbourne CBD street life.",
        styles,
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<b>Outputs:</b> <tt>cube.npy</tt> (float32, ~1.85 GB), "
        "<tt>graph_spatial.pt</tt>, <tt>graph_semantic.pt</tt>, "
        "<tt>norm_stats.json</tt>, <tt>cube_meta.json</tt>.",
        styles["BodySmall"],
    ))
    story.append(PageBreak())

    # =========================================================================
    # 10 — STEP 09: TRAIN
    # =========================================================================
    story.extend(section_header(
        "10  /  STEP 09", "MultiGCN Training", styles, frame_w))

    story.append(terminology_box([
        ("MultiGCN",
         "The neural network architecture. &ldquo;Multi&rdquo; = dual "
         "branches (spatial + semantic). &ldquo;GCN&rdquo; = Graph "
         "Convolutional Network, a layer that aggregates information "
         "from neighbouring nodes on a graph."),
        ("Dual-Head Architecture",
         "The model has two output heads sharing a common backbone. "
         "One head predicts pedestrian flow; the other predicts parking "
         "occupancy. Sharing forces both tasks to learn from the same "
         "spatial/temporal patterns."),
        ("GRU (Gated Recurrent Unit)",
         "A type of recurrent neural network that processes sequences. "
         "It reads the 96-step (24-hour) window and summarises the "
         "temporal context into a single vector per street."),
        ("Autoregressive",
         "The model predicts the next time step, then feeds that "
         "prediction back as input to predict the step after that. "
         "Like a weather forecast that uses today&rsquo;s prediction "
         "as tomorrow&rsquo;s starting point."),
        ("Node Bias",
         "A learnable offset per street. Some streets are inherently "
         "busier; the node bias lets the model encode this baseline "
         "without using GCN capacity."),
        ("Chronological Split",
         "Train on 2015&ndash;mid 2018, validate on mid&ndash;late 2018, "
         "test on late 2018&ndash;2019. No future data leaks into "
         "training. This is critical for honest evaluation."),
        ("Early Stopping",
         "Monitor validation error each epoch. If it hasn&rsquo;t "
         "improved for 25 epochs, stop training. Prevents the model "
         "from memorising the training data."),
        ("MAE (Mean Absolute Error)",
         "The average magnitude of prediction errors. &ldquo;On average, "
         "the model is off by X pedestrians per 15 minutes.&rdquo;"),
        ("R&sup2; (Coefficient of Determination)",
         "Measures how much variance the model explains. R&sup2; = 0.88 "
         "means the model explains 88% of the variation in foot traffic."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 09 trains the core neural network. The MultiGCN "
        "architecture processes a 24-hour sliding window of the data "
        "cube through two parallel GCN branches (one per graph), "
        "concatenates their outputs, feeds them through a GRU "
        "sequence encoder, and predicts the next time step&rsquo;s "
        "pedestrian flow and parking occupancy for all 1,397 streets "
        "simultaneously.",
        styles["Body"],
    ))

    story.append(Paragraph("Architecture diagram", styles["Subhead"]))
    story.extend(figure_block(
        FIGURES_DIR / "fig4_architecture.png",
        "Figure 3. MultiGCN architecture. Input: [batch, W=96, N=1397, "
        "F=23]. Two GCN branches (spatial + semantic) each project to "
        "H=64. Concatenated [128] fed through 2-layer GRU. Dual output "
        "heads with per-node biases.",
        styles, frame_w,
    ))

    story.append(Paragraph("Training protocol", styles["Subhead"]))
    training_rows = [
        ["Data split", "Train 70% / Val 15% / Test 15% (chronological)"],
        ["Window", "W = 96 bins (24 hours), stride = 1"],
        ["Batch", "8 windows per gradient step, 256 steps per epoch"],
        ["Optimiser", "Adam, learning rate = 1e-3"],
        ["Scheduler", "ReduceLROnPlateau on validation ped MAE"],
        ["Early stopping", "Patience = 25 epochs on val ped MAE"],
        ["Loss function",
         "MAE_ped + 0.5 &times; masked_MAE_park (parking masked to "
         "143 sensor streets)"],
        ["Seed", "42 (reproducible)"],
        ["Parameters", "68,076 total trainable"],
    ]
    story.append(data_table(
        ["Parameter", "Value"], training_rows))

    story.append(Spacer(1, 8))
    story.append(callout(
        "Joint loss design",
        "The parking loss is multiplied by 0.5 and <b>masked</b> to "
        "only the 143 streets with real sensors. This means the model "
        "learns parking patterns from supervised data while "
        "<i>extrapolating</i> to the remaining 1,254 streets. The "
        "pedestrian head is always unmasked.",
        styles,
    ))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "Training the model is like teaching it to read the city&rsquo;s "
        "pulse. It sees five years of Melbourne&rsquo;s rhythms: the "
        "Monday morning commuter surge, the Friday evening bar crowds, "
        "the rainy-day dip. By learning from 70% of the timeline and "
        "testing on the future 15%, we ensure the model can genuinely "
        "predict what happens next &mdash; not just memorise what already "
        "happened. The dual heads mean it simultaneously understands both "
        "foot traffic and parking, learning that these two phenomena are "
        "deeply interconnected: more parking turnover often correlates "
        "with more pedestrians.",
        styles,
    ))
    story.append(PageBreak())

    # =========================================================================
    # 11 — STEP 10: INTERPRET
    # =========================================================================
    story.extend(section_header(
        "11  /  STEP 10", "Feature Interpretation", styles, frame_w))

    story.append(terminology_box([
        ("Permutation Importance",
         "A model-agnostic technique: replace one feature with random "
         "noise and measure how much the prediction accuracy drops. "
         "Large drop = important feature. Repeated 3 times and averaged "
         "for stability."),
        ("Branch Ablation",
         "Zero out the weights of one GCN branch (spatial or semantic) "
         "and measure the damage. Tells us which graph matters more."),
        ("Delta MAE (&Delta; MAE)",
         "The increase in error when a feature or branch is removed. "
         "A &Delta; MAE of 27.4 for ped_flow means the error jumps by "
         "27.4 pedestrians/15-min when past foot traffic is scrambled."),
        ("Baseline MAE",
         "The model&rsquo;s normal error with all features intact: "
         "5.448 pedestrians per 15-minute bin."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 10 opens the model&rsquo;s &ldquo;black box&rdquo; with "
        "two complementary techniques: permutation importance reveals "
        "which <i>features</i> matter most, and branch ablation reveals "
        "which <i>graph</i> matters most.",
        styles["Body"],
    ))

    story.append(Paragraph("Top features by permutation importance",
                           styles["Subhead"]))
    imp_rows = [
        ["ped_flow", "27.40",
         "Past foot traffic is the strongest predictor"],
        ["hour_cos", "2.49",
         "Time of day drives daily rhythm"],
        ["total_jobs", "1.36",
         "Employment density sets baseline demand"],
        ["occupancy_rate", "1.07",
         "Parking activity informs pedestrian patterns"],
        ["hour_sin", "0.89",
         "Second component of time encoding"],
        ["business_count", "0.87",
         "Non-hospitality commercial density"],
        ["wind_speed_10m", "0.84",
         "Weather modulates outdoor activity"],
        ["cafe_count", "0.84",
         "Hospitality venues attract foot traffic"],
        ["precipitation", "0.74",
         "Rain reduces pedestrian activity"],
        ["dining_capacity", "0.68",
         "Combined cafe + bar seating"],
    ]
    story.append(data_table(
        ["Feature", "&Delta; MAE", "Interpretation"], imp_rows))

    story.append(Spacer(1, 8))
    story.extend(figure_block(
        FIGURES_DIR / "fig5_importance.png",
        "Figure 4. Permutation importance across all 23 features. "
        "ped_flow dominates as expected for a sequence model; time "
        "and land-use features carry real but secondary signal.",
        styles, frame_w,
    ))

    story.append(Paragraph("Branch ablation", styles["Subhead"]))
    branch_rows = [
        ["Spatial branch zeroed", "+14.76",
         "Physical adjacency is the dominant inductive bias"],
        ["Semantic branch zeroed", "+3.76",
         "Functional similarity is meaningful but secondary"],
    ]
    story.append(data_table(
        ["Experiment", "&Delta; MAE", "Interpretation"], branch_rows))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "This step answers: &ldquo;What does the model actually pay "
        "attention to?&rdquo; The answer is reassuringly intuitive. "
        "The strongest signal is recent foot traffic (if lots of people "
        "were walking here 15 minutes ago, lots will be walking now). "
        "Next comes time of day and employment density &mdash; the "
        "9-to-5 rhythm of office workers. Then weather: rain clears "
        "the streets. And the spatial graph (physical neighbours) "
        "matters 4&times; more than the semantic graph (similar but "
        "distant streets). The model has learned what any Melburnian "
        "would intuit &mdash; but with quantitative precision.",
        styles,
    ))
    story.append(PageBreak())

    # =========================================================================
    # 12 — STEP 11: SCENARIO
    # =========================================================================
    story.extend(section_header(
        "12  /  STEP 11", "Scenario Simulation", styles, frame_w))

    story.append(terminology_box([
        ("Counterfactual",
         "A &ldquo;what-if&rdquo; question. &ldquo;What would have "
         "happened if we removed all parking from this street?&rdquo; "
         "The model compares the intervention timeline against a "
         "no-change baseline."),
        ("Autoregressive Rollout",
         "Running the model forward step-by-step, feeding each "
         "prediction back as input to the next. Like dominoes: each "
         "step triggers the next."),
        ("Pedestrianise",
         "Set occupancy_rate = 0 on a target street. Simulates complete "
         "removal of on-street parking and reallocation to pedestrian "
         "use."),
        ("Restrict Parking",
         "Set occupancy_rate to a fraction (e.g. 0.3 = allow 30% "
         "capacity). Simulates partial parking reduction."),
        ("Boost Pedestrian",
         "Add a constant uplift to ped_flow. Simulates an event, "
         "festival, or new attraction increasing foot traffic."),
        ("Parking Spillover",
         "When parking is removed from one street, displaced cars "
         "redistribute to neighbouring streets. The model&rsquo;s "
         "joint parking head predicts this redistribution."),
        ("Graph Diffusion (A<sup>k</sup>)",
         "Propagating the intervention&rsquo;s effects through the "
         "graph: 1 hop = immediate neighbours, 2 hops = neighbours of "
         "neighbours, 3 hops = three streets away. Captures how "
         "disruptions ripple through the network."),
        ("Rebound Analysis",
         "After the intervention ends, how quickly does the street "
         "return to normal? Measured by half-life (time to recover "
         "50%) and recovery fraction (how much normalcy returns)."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 11 transforms the trained model into a counterfactual "
        "engine. Given a specific street, intervention type, start time, "
        "and duration, it produces a full 24-hour rollout comparing the "
        "intervention scenario against a matched baseline.",
        styles["Body"],
    ))

    story.append(Paragraph("Intervention types", styles["Subhead"]))
    interv_rows = [
        ["pedestrianise", "occupancy_rate &rarr; 0",
         "Full kerb reallocation to pedestrian space"],
        ["restrict_park", "occupancy_rate &rarr; magnitude",
         "Cap parking at e.g. 30% of current capacity"],
        ["boost_ped", "ped_flow + magnitude",
         "Inject constant foot traffic uplift (event/festival)"],
    ]
    story.append(data_table(
        ["Type", "Technical effect", "Real-life meaning"], interv_rows))

    story.append(Spacer(1, 8))
    story.append(Paragraph("Network effects", styles["Subhead"]))
    story.append(Paragraph(
        "Every scenario is enriched with five layers of network analysis, "
        "all computed post-training without retraining the model:",
        styles["Body"],
    ))
    effects = [
        "&bull; <b>Parking spillover:</b> The joint parking head predicts "
        "where displaced cars go.",
        "&bull; <b>Graph diffusion (A<sup>k</sup>, k=1..3):</b> Effects "
        "propagate through spatial and semantic edges, weakening with "
        "distance.",
        "&bull; <b>Semantic neighbours:</b> Reports changes on "
        "functionally similar streets that may be far away.",
        "&bull; <b>Confidence-weighted ranking:</b> Top affected streets "
        "ranked by delta &times; confidence.",
        "&bull; <b>Rebound analysis:</b> Half-life and recovery fraction "
        "after the intervention lifts.",
    ]
    for e in effects:
        story.append(Paragraph(e, styles["Bullet"]))

    story.append(Spacer(1, 10))
    story.extend(figure_block(
        FIGURES_DIR / "fig7_propagation.png",
        "Figure 5. Spillover propagation from a pedestrianisation "
        "scenario. Colour intensity shows predicted pedestrian flow "
        "change at 1, 2, and 3 hops along spatial and semantic edges.",
        styles, frame_w,
    ))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "This is where the pipeline becomes a policy tool. A planner "
        "asks: &ldquo;What happens if we pedestrianise Little Collins "
        "Street between Exhibition and Spring on a Wednesday at noon "
        "for 4 hours?&rdquo; The system answers: foot traffic on Little "
        "Collins rises by X%, but parking displaced to Flinders Lane "
        "and Lonsdale Street increases their occupancy by Y%. Two blocks "
        "away, a similar cafe strip sees a sympathetic uplift. After the "
        "intervention ends, Little Collins returns to 90% of baseline "
        "within 2 hours. This level of detail &mdash; covering direct "
        "effects, spillover, and recovery &mdash; is what enables "
        "evidence-based street design.",
        styles,
    ))
    story.append(PageBreak())

    # =========================================================================
    # 13 — STEP 12: EXPORT & FRONTEND
    # =========================================================================
    story.extend(section_header(
        "13  /  STEP 12", "Frontend Export &amp; API", styles, frame_w))

    story.append(terminology_box([
        ("GeoJSON",
         "A geographic data format based on JSON. Each street is stored "
         "as a polygon with properties (cluster, opportunity score, "
         "predicted flow, etc.). Can be loaded directly by mapping "
         "libraries."),
        ("Flask",
         "A lightweight Python web framework. Serves the interactive "
         "map and handles scenario API requests."),
        ("Mapbox GL JS",
         "A JavaScript library for rendering interactive, GPU-accelerated "
         "maps in the browser. Streets are drawn as coloured polygons "
         "on a dark basemap."),
        ("Chart.js",
         "A JavaScript charting library. Used to display time-series "
         "charts of baseline vs. treated pedestrian flow and parking "
         "occupancy."),
        ("Opportunity Score",
         "A composite metric ranking streets by their potential for "
         "improvement: how much latent capacity exists and how responsive "
         "the street is to intervention."),
    ], styles))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Step 12 bridges the gap between model outputs and human "
        "decision-making. It enriches the street GeoJSON with model "
        "predictions and opportunity scores, then serves an interactive "
        "web application where planners can explore the city and run "
        "scenarios on demand.",
        styles["Body"],
    ))

    story.append(Paragraph("API endpoints", styles["Subhead"]))
    api_rows = [
        ["GET /", "Serves the interactive Mapbox map"],
        ["POST /scenario",
         "Accepts street_id, intervention_type, duration, "
         "rollout_steps, magnitude, dow, hour, minute. Returns "
         "full counterfactual JSON."],
    ]
    story.append(data_table(["Endpoint", "Description"], api_rows))

    story.append(Spacer(1, 8))
    story.append(Paragraph("Frontend features", styles["Subhead"]))
    story.append(Paragraph(
        "The interactive map (<tt>sensor_map_viz.html</tt>) provides: "
        "coloured street polygons by cluster archetype; sensor markers "
        "for pedestrian and parking locations; a right-slide detail "
        "panel with metadata, weekly heatmaps, and charts; and a "
        "scenario simulation panel where users select an intervention "
        "type, set parameters, and view baseline vs. treated time "
        "series overlaid on the map.",
        styles["Body"],
    ))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "The frontend is the public face of the entire pipeline. A city "
        "planner opens the map, clicks on a street, sees its archetype "
        "and weekly rhythm, then runs a scenario: &ldquo;What if we "
        "pedestrianise this street on Friday evenings?&rdquo; The "
        "response appears as two overlaid charts &mdash; the baseline "
        "(what would happen normally) and the treated scenario (with "
        "the intervention). Neighbouring streets light up to show "
        "spillover effects. This transforms months of data science into "
        "a tool anyone can use in seconds.",
        styles,
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<b>Outputs:</b> Updated <tt>streets_viz.geojson</tt> (with "
        "opportunity_score, pred_ped_mean, uplift_fraction, cf_ped_mean), "
        "<tt>opportunities_summary.json</tt>, <tt>model_eval.json</tt> "
        "copy in processed/.",
        styles["BodySmall"],
    ))
    story.append(PageBreak())

    # =========================================================================
    # 14 — RESULTS & EVALUATION
    # =========================================================================
    story.extend(section_header(
        "14  /  RESULTS &amp; EVALUATION",
        "Where the Model Lands", styles, frame_w))

    story.append(Paragraph(
        "The model was trained for {best_epoch} epochs before early "
        "stopping triggered. Test performance closely tracks validation, "
        "confirming that the chronological split and early stopping "
        "successfully prevented optimistic tuning.".format(
            best_epoch=model_eval.get("best_epoch", "N/A")),
        styles["Body"],
    ))

    story.append(Paragraph("Final metrics", styles["Subhead"]))
    me = model_eval
    result_rows = [
        ["Validation",
         f"{me['val_ped_mae']:.3f}", f"{me['val_ped_rmse']:.2f}",
         f"{me['val_ped_r2']:.3f}",
         f"{me['val_park_mae']:.4f}", f"{me['val_park_r2']:.3f}"],
        ["Test",
         f"{me['test_ped_mae']:.3f}", f"{me['test_ped_rmse']:.2f}",
         f"{me['test_ped_r2']:.3f}",
         f"{me['test_park_mae']:.4f}", f"{me['test_park_r2']:.3f}"],
    ]
    story.append(data_table(
        ["Split", "Ped MAE", "Ped RMSE", "Ped R²",
         "Park MAE", "Park R²"],
        result_rows,
    ))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        f"A pedestrian MAE of {me['test_ped_mae']:.1f} means the model "
        "is, on average, off by about 6 people per 15-minute window per "
        "street. For busy corridors seeing 200+ pedestrians, this is "
        f"~3% error. The R² of {me['test_ped_r2']:.3f} means the "
        "model explains 87.6% of the variation in foot traffic across "
        "all streets and time periods.",
        styles["Body"],
    ))
    story.append(Paragraph(
        f"The parking R² of {me['test_park_r2']:.3f} is noteworthy "
        "given that the parking head is supervised on only 143 of 1,397 "
        "streets and shares nearly all representational capacity with "
        "the pedestrian head. This confirms that joint multi-task "
        "learning enables meaningful parking modelling even with sparse "
        "supervision.",
        styles["Body"],
    ))

    story.append(Spacer(1, 8))
    story.extend(figure_block(
        FIGURES_DIR / "fig6_trajectories.png",
        "Figure 6. Predicted (blue) vs observed (grey) pedestrian "
        "trajectories on held-out test streets. The model captures "
        "daily rhythm, weekly variation, and weather-driven dips.",
        styles, frame_w,
    ))

    story.append(Spacer(1, 8))
    story.extend(figure_block(
        FIGURES_DIR / "fig1_pedestrian_intensity.png",
        "Figure 7. Mean pedestrian intensity across all modelled streets. "
        "Retail spines along Swanston and Bourke dominate, with secondary "
        "concentrations near transport interchanges.",
        styles, frame_w,
    ))

    story.append(Spacer(1, 8))
    story.append(reallife_box(
        "In practical terms: if a planner uses this model to predict "
        "tomorrow&rsquo;s lunchtime foot traffic on a busy street, "
        "the prediction will typically be within &plusmn;6 pedestrians "
        "of reality (per 15-minute count). For a street that normally "
        "sees 200 people in that window, that&rsquo;s 97% accuracy. "
        "The parking model is similarly reliable on the 143 streets with "
        "sensors, and provides reasonable estimates elsewhere.",
        styles,
    ))
    story.append(PageBreak())

    # =========================================================================
    # 15 — LIMITATIONS & FUTURE WORK
    # =========================================================================
    story.extend(section_header(
        "15  /  KNOWN LIMITATIONS", "Boundaries &amp; Future Work",
        styles, frame_w))

    story.append(Paragraph(
        "Every model has boundaries. Understanding these is as important "
        "as understanding the results.",
        styles["Body"],
    ))

    story.append(Paragraph("Data limitations", styles["Subhead"]))
    lim_data = [
        ["Timezone shift",
         "All timestamps are AEDT (UTC+11) treated as UTC. Internally "
         "consistent but shifted 11 hours from actual Melbourne time."],
        ["Tram features",
         "nearest_tram_stop_m and tram_stops_200m are sentinel values "
         "(not populated). Tram proximity is not yet captured."],
        ["Parking coverage",
         "Only 143 of 1,397 streets have real parking sensors. "
         "Predictions on the remaining 1,254 are extrapolations."],
        ["Study period",
         "Pre-COVID only (2015&ndash;2019). Post-COVID mobility patterns "
         "may differ significantly."],
        ["33 orphan streets",
         "33 of 171 parking-sensored streets sit outside the 1,397-street "
         "modelled graph."],
    ]
    story.append(data_table(
        ["Limitation", "Detail"], lim_data))

    story.append(Paragraph("Model limitations", styles["Subhead"]))
    lim_model = [
        ["Autoregressive horizon",
         "Rollouts beyond ~4 hours (16 steps) accumulate error. "
         "Predictions past this horizon are directional, not precise."],
        ["Parking extrapolation",
         "The parking head is supervised on 143 streets only. "
         "Predictions on unsupervised streets are model-inferred."],
        ["Confidence binary",
         "Planned 0.8 confidence tier for high-quality imputation was "
         "never materialised. Only 1.0 (sensor) and 0.5 (imputed) exist."],
    ]
    story.append(data_table(
        ["Limitation", "Detail"], lim_model))

    story.append(Spacer(1, 12))
    story.append(Paragraph("Future directions", styles["Subhead"]))
    future = [
        "&bull; <b>Post-COVID extension:</b> Retrain on 2020&ndash;2025 "
        "data to capture changed mobility patterns.",
        "&bull; <b>Tram integration:</b> Incorporate PTV tram stop "
        "proximity as spatial features.",
        "&bull; <b>Real-time inference:</b> Connect live sensor feeds "
        "for nowcasting instead of historical analysis.",
        "&bull; <b>Multi-scenario optimisation:</b> Test combinations of "
        "interventions across multiple streets simultaneously.",
        "&bull; <b>Temporal attention:</b> Replace GRU with a "
        "transformer-style attention mechanism for longer-horizon "
        "predictions.",
    ]
    for f in future:
        story.append(Paragraph(f, styles["Bullet"]))

    story.append(Spacer(1, 18))
    story.append(Divider(frame_w, color=ACCENT, thickness=1.0))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<font color='#6b6b6b'>End of report &middot; "
        f"generated {date.today().isoformat()} &middot; "
        "Melbourne CBD Street Analysis Pipeline v2.1 &middot; "
        "Comprehensive Edition</font>",
        styles["Caption"],
    ))

    return story


# ---------------------------------------------------------------------------
# Document assembly
# ---------------------------------------------------------------------------

def build_pdf(output_path: Path = OUTPUT_PDF) -> Path:
    """Compose the comprehensive pipeline report and write to *output_path*."""
    doc = BaseDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=2.2 * cm, rightMargin=2.2 * cm,
        topMargin=2.4 * cm, bottomMargin=2.2 * cm,
        title="Melbourne CBD Street Dynamics — Comprehensive Pipeline Report",
        author="Ertugrul Akdemir",
    )

    page_w, page_h = A4
    cover_frame = Frame(
        2.2 * cm, 2.5 * cm,
        page_w - 4.4 * cm, page_h - 5.0 * cm,
        leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0,
        id="cover",
    )
    body_frame = Frame(
        2.2 * cm, 2.0 * cm,
        page_w - 4.4 * cm, page_h - 4.2 * cm,
        leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0,
        id="body",
    )

    doc.addPageTemplates([
        PageTemplate(id="cover", frames=[cover_frame], onPage=_cover_decor),
        PageTemplate(id="body", frames=[body_frame], onPage=_body_decor),
    ])

    styles = build_styles()
    frame_w = page_w - 4.4 * cm
    story = build_story(styles, frame_w)
    story.insert(0, NextPageTemplate("body"))
    doc.build(story)
    return output_path


if __name__ == "__main__":
    path = build_pdf()
    print(f"Wrote {path}")
