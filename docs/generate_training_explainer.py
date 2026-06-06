"""
Generate a kid-friendly PDF explaining how the MultiGCN model is trained.
Output: docs/training_explainer.pdf
Run:    python docs/generate_training_explainer.py
"""
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether
)
from reportlab.platypus.flowables import Flowable
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Circle
from reportlab.graphics import renderPDF
from reportlab.lib.colors import HexColor


# ── Colour palette ─────────────────────────────────────────────────────────────
BLUE       = HexColor("#2563EB")
LIGHT_BLUE = HexColor("#DBEAFE")
GREEN      = HexColor("#16A34A")
LIGHT_GREEN= HexColor("#DCFCE7")
ORANGE     = HexColor("#EA580C")
LIGHT_ORANGE=HexColor("#FFEDD5")
PURPLE     = HexColor("#7C3AED")
LIGHT_PURPLE=HexColor("#EDE9FE")
RED        = HexColor("#DC2626")
LIGHT_RED  = HexColor("#FEE2E2")
YELLOW     = HexColor("#CA8A04")
LIGHT_YELLOW=HexColor("#FEF9C3")
GREY       = HexColor("#6B7280")
LIGHT_GREY = HexColor("#F3F4F6")
DARK       = HexColor("#111827")
WHITE      = colors.white


# ── Custom Flowables ───────────────────────────────────────────────────────────

class ColorBox(Flowable):
    """A coloured callout box with an emoji label and body text."""
    def __init__(self, emoji, title, body, bg=LIGHT_BLUE, border=BLUE, width=None):
        Flowable.__init__(self)
        self.emoji  = emoji
        self.title  = title
        self.body   = body
        self.bg     = bg
        self.border = border
        self._width = width or (A4[0] - 4 * cm)
        self.height = 0  # computed in wrap

    def wrap(self, availWidth, availHeight):
        self._width = availWidth
        # Estimate height: title line + body lines
        lines = len(self.body) // 80 + self.body.count("\n") + 3
        self.height = max(lines * 14 + 30, 60)
        return self._width, self.height

    def draw(self):
        c = self.canv
        w, h = self._width, self.height
        # Background
        c.setFillColor(self.bg)
        c.setStrokeColor(self.border)
        c.setLineWidth(1.5)
        c.roundRect(0, 0, w, h, 8, fill=1, stroke=1)
        # Left accent bar
        c.setFillColor(self.border)
        c.roundRect(0, 0, 5, h, 4, fill=1, stroke=0)
        # Title
        c.setFillColor(self.border)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(14, h - 20, f"{self.emoji}  {self.title}")
        # Body
        c.setFillColor(DARK)
        c.setFont("Helvetica", 10)
        text = c.beginText(14, h - 36)
        text.setFont("Helvetica", 10)
        text.setFillColor(DARK)
        for line in self._wrap_text(self.body, int(w - 28)):
            text.textLine(line)
        c.drawText(text)

    def _wrap_text(self, text, max_width_pts):
        """Naive word-wrap by character estimate (10pt ≈ 5.5 pts/char)."""
        chars_per_line = max(1, int(max_width_pts / 5.5))
        lines = []
        for paragraph in text.split("\n"):
            words = paragraph.split()
            current = ""
            for word in words:
                if len(current) + len(word) + 1 <= chars_per_line:
                    current = current + " " + word if current else word
                else:
                    if current:
                        lines.append(current)
                    current = word
            if current:
                lines.append(current)
            if not words:
                lines.append("")
        return lines


class SectionHeader(Flowable):
    """Big coloured section header band."""
    def __init__(self, number, title, color=BLUE, width=None):
        Flowable.__init__(self)
        self.number = number
        self.title  = title
        self.color  = color
        self._width = width or (A4[0] - 4 * cm)
        self.height = 36

    def wrap(self, availWidth, availHeight):
        self._width = availWidth
        return self._width, self.height

    def draw(self):
        c = self.canv
        w = self._width
        c.setFillColor(self.color)
        c.roundRect(0, 0, w, self.height, 6, fill=1, stroke=0)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(14, 10, f"  {self.number}  {self.title}")


class PipelineBox(Flowable):
    """Draws a simple pipeline flow diagram for the forward pass."""
    def __init__(self, steps, width=None):
        Flowable.__init__(self)
        self.steps  = steps   # list of (label, color) tuples
        self._width = width or (A4[0] - 4 * cm)
        self.height = 70

    def wrap(self, availWidth, availHeight):
        self._width = availWidth
        return self._width, self.height

    def draw(self):
        c = self.canv
        n     = len(self.steps)
        total = self._width
        box_w = (total - (n - 1) * 18) / n
        box_h = 36
        y0    = (self.height - box_h) / 2

        for i, (label, col) in enumerate(self.steps):
            x = i * (box_w + 18)
            # Arrow before box (except first)
            if i > 0:
                ax = x - 18
                mid_y = y0 + box_h / 2
                c.setStrokeColor(GREY)
                c.setLineWidth(1.5)
                c.line(ax, mid_y, ax + 14, mid_y)
                c.setFillColor(GREY)
                c.setStrokeColor(GREY)
                p = c.beginPath()
                p.moveTo(ax + 14, mid_y)
                p.lineTo(ax + 10, mid_y + 4)
                p.lineTo(ax + 10, mid_y - 4)
                p.close()
                c.drawPath(p, fill=1, stroke=0)
            # Box
            c.setFillColor(col)
            c.setStrokeColor(colors.white)
            c.setLineWidth(0)
            c.roundRect(x, y0, box_w, box_h, 5, fill=1, stroke=0)
            # Label
            c.setFillColor(WHITE)
            c.setFont("Helvetica-Bold", 8)
            words = label.split("\n")
            for wi, w_text in enumerate(words):
                ty = y0 + box_h / 2 + (len(words) / 2 - wi - 0.5) * 10
                c.drawCentredString(x + box_w / 2, ty, w_text)


class TrainingLoopDiagram(Flowable):
    """Draws a circular training loop with labelled steps."""
    def __init__(self, width=None):
        Flowable.__init__(self)
        self._width = width or (A4[0] - 4 * cm)
        self.height = 200

    def wrap(self, availWidth, availHeight):
        self._width = availWidth
        return self._width, self.height

    def draw(self):
        c = self.canv
        cx = self._width / 2
        cy = self.height / 2
        r  = 70

        steps = [
            (0,   -r,  "1. Pick a\nwindow",         BLUE),
            (r,    0,  "2. Forward\nPass",           GREEN),
            (0,    r,  "3. Calculate\nLoss",         ORANGE),
            (-r,   0,  "4. Backprop &\nUpdate",      PURPLE),
        ]

        # Draw circle guide (dashed)
        c.setStrokeColor(LIGHT_GREY)
        c.setLineWidth(1)
        c.circle(cx, cy, r, fill=0, stroke=1)

        # Draw curved arrows between steps
        import math
        for i in range(len(steps)):
            x1, y1, _, _ = steps[i]
            x2, y2, _, _ = steps[(i + 1) % len(steps)]
            # midpoint offset for curve effect
            mx = (x1 + x2) / 2 * 0.7
            my = (y1 + y2) / 2 * 0.7
            c.setStrokeColor(GREY)
            c.setLineWidth(1.5)
            p = c.beginPath()
            p.moveTo(cx + x1, cy + y1)
            p.curveTo(cx + x1 * 0.5 + mx, cy + y1 * 0.5 + my,
                      cx + x2 * 0.5 + mx, cy + y2 * 0.5 + my,
                      cx + x2, cy + y2)
            c.drawPath(p, stroke=1, fill=0)

        # Draw boxes
        bw, bh = 80, 36
        for dx, dy, label, col in steps:
            bx = cx + dx - bw / 2
            by = cy + dy - bh / 2
            c.setFillColor(col)
            c.roundRect(bx, by, bw, bh, 5, fill=1, stroke=0)
            c.setFillColor(WHITE)
            c.setFont("Helvetica-Bold", 8)
            lines = label.split("\n")
            for wi, wt in enumerate(lines):
                ty = by + bh / 2 + (len(lines) / 2 - wi - 0.5) * 10
                c.drawCentredString(cx + dx, ty, wt)

        # Centre label
        c.setFillColor(LIGHT_GREY)
        c.circle(cx, cy, 28, fill=1, stroke=0)
        c.setFillColor(DARK)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(cx, cy + 4, "Training")
        c.drawCentredString(cx, cy - 6, "Loop")


# ── Styles ─────────────────────────────────────────────────────────────────────

def make_styles():
    base = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "BigTitle", parent=base["Title"],
        fontSize=28, leading=36, textColor=BLUE,
        spaceAfter=6, alignment=TA_CENTER, fontName="Helvetica-Bold"
    )
    subtitle_style = ParagraphStyle(
        "Subtitle", parent=base["Normal"],
        fontSize=13, leading=18, textColor=GREY,
        spaceAfter=4, alignment=TA_CENTER
    )
    body_style = ParagraphStyle(
        "Body", parent=base["Normal"],
        fontSize=11, leading=16, textColor=DARK,
        spaceAfter=8, alignment=TA_JUSTIFY
    )
    bold_style = ParagraphStyle(
        "Bold", parent=body_style,
        fontName="Helvetica-Bold", textColor=DARK
    )
    caption_style = ParagraphStyle(
        "Caption", parent=base["Normal"],
        fontSize=9, leading=12, textColor=GREY,
        spaceAfter=6, alignment=TA_CENTER, fontName="Helvetica-Oblique"
    )
    bullet_style = ParagraphStyle(
        "Bullet", parent=body_style,
        bulletIndent=10, leftIndent=20, spaceAfter=4
    )
    return {
        "title": title_style, "subtitle": subtitle_style,
        "body": body_style, "bold": bold_style,
        "caption": caption_style, "bullet": bullet_style,
    }


# ── Content builders ───────────────────────────────────────────────────────────

def build_cover(styles):
    s = styles
    return [
        Spacer(1, 2 * cm),
        Paragraph("How We Teach a Computer", s["title"]),
        Paragraph("to Understand Melbourne's Streets", s["title"]),
        Spacer(1, 0.5 * cm),
        Paragraph("A plain-English guide to training our AI model", s["subtitle"]),
        Paragraph("Melbourne CBD Street Analysis · Thesis Project · 2026", s["subtitle"]),
        Spacer(1, 1.5 * cm),
        HRFlowable(width="100%", thickness=2, color=BLUE),
        Spacer(1, 1 * cm),
        ColorBox("📖", "Who is this for?",
            "This guide explains exactly how our AI model learns to predict pedestrian "
            "foot traffic and parking occupancy on Melbourne's 1,397 streets. "
            "Every technical term is explained with a simple everyday analogy. "
            "No maths degree required!",
            bg=LIGHT_BLUE, border=BLUE),
        Spacer(1, 0.5 * cm),
        ColorBox("🗺️", "What does the model actually do?",
            "Given the last 24 hours of data across all 1,397 streets "
            "(how many pedestrians walked past, how full the parking was, what the weather was like, "
            "what time of day it is, etc.), the model predicts what will happen in the next 15 minutes "
            "on every street simultaneously.",
            bg=LIGHT_GREEN, border=GREEN),
    ]


def build_cube_section(styles):
    s = styles
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("01", "The Datacube — The Model's Memory Book", BLUE),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "Before we can train the model, we need to organise all our data into one neat package. "
            "We call this the <b>datacube</b>.",
            s["body"]
        ),
        ColorBox("📚", "Analogy: A School Report Book",
            "Imagine a giant school report book with a separate page for every street in Melbourne (1,397 pages). "
            "On each page there are 14,400 rows — one for every 15-minute slot across 5 months. "
            "And each row has 23 columns: pedestrian count, parking fullness, temperature, time of day, "
            "number of cafés nearby, and so on.\n\n"
            "That book — stored as a single file called cube.npy — is what we hand to the model to study from.",
            bg=LIGHT_YELLOW, border=YELLOW),
        Spacer(1, 0.3 * cm),
        Paragraph("The cube has a very specific shape:", s["body"]),
        Table(
            [
                ["Dimension", "Size", "Meaning"],
                ["Streets (N)", "1,397", "One row per Melbourne street segment"],
                ["Time bins (T)", "14,400", "One column per 15-min slot (Nov 2025 – Mar 2026)"],
                ["Features (F)", "23", "Pedestrian flow, parking, weather, café count, etc."],
            ],
            colWidths=[5 * cm, 3 * cm, 8 * cm],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), BLUE),
                ("TEXTCOLOR",  (0, 0), (-1, 0), WHITE),
                ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",   (0, 0), (-1, -1), 10),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [LIGHT_GREY, WHITE]),
                ("GRID",       (0, 0), (-1, -1), 0.5, GREY),
                ("LEFTPADDING",(0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING",(0,0),(-1,-1), 5),
            ])
        ),
        Spacer(1, 0.4 * cm),
        ColorBox("⚠️", "Why normalise?",
            "Pedestrian counts can be in the hundreds. Temperature is around 20. "
            "Parking occupancy is between 0 and 1. If we feed all these raw numbers into the model, "
            "the big numbers (pedestrians) would completely drown out the small ones (parking). "
            "So we rescale everything to roughly the same range — this is called z-score normalisation. "
            "Think of it like converting all measurements to the same units before comparing them.",
            bg=LIGHT_ORANGE, border=ORANGE),
    ]


def build_split_section(styles):
    s = styles
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("02", "The Three-Way Split — Train, Val, Test", GREEN),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "We split the 14,400 time slots into three groups. The model only ever studies from the "
            "first group. The other two are used to check how well it learned.",
            s["body"]
        ),
        ColorBox("🏫", "Analogy: Study, Practice Test, Final Exam",
            "Imagine studying for an exam:\n"
            "• TRAIN (70% = 10,080 slots): your textbook — the model reads this over and over.\n"
            "• VAL   (15% = 2,160 slots):  practice tests — used to check progress and stop early if needed.\n"
            "• TEST  (15% = 2,160 slots):  the sealed final exam — only opened once, at the very end.\n\n"
            "Crucially, we never shuffle time. Nov comes before Dec which comes before Jan. "
            "This stops the model from 'cheating' by seeing future data while studying the past.",
            bg=LIGHT_GREEN, border=GREEN),
    ]


def build_window_section(styles):
    s = styles
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("03", "Sliding Windows — How We Create One Training Sample", PURPLE),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "The model doesn't look at all 14,400 time slots at once. It looks at a <b>window</b> "
            "of 96 consecutive slots (exactly 24 hours) and tries to predict what comes next.",
            s["body"]
        ),
        ColorBox("🎬", "Analogy: Watching 24 Hours of CCTV",
            "Think of the datacube as a 5-month CCTV recording of every street in Melbourne. "
            "During training, we randomly pick a 24-hour clip and ask the model: "
            "'Having watched all 1,397 streets for the last 24 hours, what will happen in the next 15 minutes?'\n\n"
            "We do this 256 times per training epoch (round of study). "
            "Each time, we pick a different random 24-hour clip from the training period.",
            bg=LIGHT_PURPLE, border=PURPLE),
        Spacer(1, 0.3 * cm),
        Paragraph("One training sample looks like this:", s["body"]),
        Table(
            [
                ["What we give the model (INPUT)", "What we ask it to predict (TARGET)"],
                ["96 time slots × 1,397 streets × 23 features\n(the last 24 hours)", "1 time slot × 1,397 streets\n(the very next 15 minutes)"],
            ],
            colWidths=[8 * cm, 8 * cm],
            style=TableStyle([
                ("BACKGROUND",  (0, 0), (-1, 0), PURPLE),
                ("TEXTCOLOR",   (0, 0), (-1, 0), WHITE),
                ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",    (0, 0), (-1, -1), 10),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT_PURPLE, WHITE]),
                ("GRID",        (0, 0), (-1, -1), 0.5, GREY),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING",  (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING",(0,0),(-1,-1), 6),
                ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
            ])
        ),
    ]


def build_forward_pass_section(styles):
    s = styles
    steps = [
        ("Cube\nWindow\n[96×1397×23]", BLUE),
        ("GCN Branch 1\nSpatial\nNeighbours", GREEN),
        ("GCN Branch 2\nSemantic\nNeighbours", PURPLE),
        ("Combine\n& GRU\n(Memory)", ORANGE),
        ("Dual\nHeads\nOutput", RED),
    ]
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("04", "The Forward Pass — Making a Prediction", ORANGE),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "The <b>forward pass</b> is simply the journey your data takes through the model "
            "from input to output — going <i>forward</i> through each layer in sequence.",
            s["body"]
        ),
        ColorBox("🧠", "Analogy: A Team of Experts in a Room",
            "Imagine a room with 1,397 experts — one per Melbourne street. "
            "Each expert knows everything about their own street for the last 24 hours. "
            "But before making a prediction, each expert is allowed to:\n"
            "  1. Chat with their PHYSICAL neighbours (streets that are physically connected)\n"
            "  2. Chat with their SIMILAR neighbours (streets with similar café/bar vibes)\n"
            "After chatting, each expert summarises everything they've heard into a single prediction: "
            "how many pedestrians and how full the parking will be in the next 15 minutes.",
            bg=LIGHT_ORANGE, border=ORANGE),
        Spacer(1, 0.4 * cm),
        Paragraph("Here is the journey of data through the model:", s["body"]),
        Spacer(1, 0.2 * cm),
        PipelineBox(steps),
        Spacer(1, 0.15 * cm),
        Paragraph("Data flows left to right through each stage.", s["caption"]),
        Spacer(1, 0.4 * cm),
        Paragraph("<b>Stage 1 — GCN Branch 1 (Spatial Neighbours):</b>", s["bold"]),
        Paragraph(
            "GCN stands for Graph Convolutional Network. In the spatial branch, "
            "each street gathers information from streets it is physically connected to "
            "(streets that share an intersection). A street on Swanston St can learn from "
            "Bourke St because they physically meet.",
            s["body"]
        ),
        Paragraph("<b>Stage 2 — GCN Branch 2 (Semantic Neighbours):</b>", s["bold"]),
        Paragraph(
            "The semantic branch connects streets that are <i>functionally similar</i> "
            "— lots of cafés, same type of businesses — even if they are geographically far apart. "
            "Chapel Street and Lygon Street can share information even though they're kilometres apart.",
            s["body"]
        ),
        Paragraph("<b>Stage 3 — GRU (Memory across time):</b>", s["bold"]),
        Paragraph(
            "GRU stands for Gated Recurrent Unit. After the two GCN branches run at each of the 96 timesteps, "
            "the GRU reads through all 96 timesteps in order and builds a memory of how things evolved over the day. "
            "Think of it as the expert writing a diary entry for each hour and then reading the whole diary "
            "before making their final prediction.",
            s["body"]
        ),
        ColorBox("💡", "Why TWO GCN branches?",
            "Each branch captures a different type of relationship between streets. "
            "Physical connections tell you about foot traffic flow (people walk from one street to the next). "
            "Semantic connections tell you about activity patterns (a street full of bars behaves like other bar streets). "
            "Together they explain far more than either could alone — removing the spatial branch alone "
            "makes the model 14.76 MAE worse; removing the semantic branch makes it 3.76 MAE worse.",
            bg=LIGHT_BLUE, border=BLUE),
    ]


def build_dual_head_section(styles):
    s = styles
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("05", "Dual Heads — Two Questions, One Brain", RED),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "After all the GCN and GRU processing, the model produces a single rich summary "
            "for each of the 1,397 streets. Two separate <b>heads</b> then use that summary "
            "to answer two different questions at the same time.",
            s["body"]
        ),
        ColorBox("🎯", "Analogy: One Expert, Two Report Cards",
            "Imagine the same expert who just studied 24 hours of street data now has to "
            "fill in two separate report cards:\n\n"
            "  HEAD 1 (Pedestrian): 'How many people will walk past in the next 15 min?'\n"
            "  HEAD 2 (Parking):    'How full will the parking be in the next 15 min?'\n\n"
            "Both answers come from the same brain (same GCN + GRU). Only the final step — "
            "the translation from summary to number — is separate for each question.",
            bg=LIGHT_RED, border=RED),
        Spacer(1, 0.3 * cm),
        Table(
            [
                ["", "Pedestrian Head", "Parking Head"],
                ["Predicts", "Foot traffic (people/15 min)", "Parking fullness (0–100%)"],
                ["Supervised on", "All 1,397 streets", "Only 143 streets with real sensors"],
                ["Result (Test)", "MAE = 5.8 people", "MAE = 4% occupancy"],
            ],
            colWidths=[3.5 * cm, 6.5 * cm, 6 * cm],
            style=TableStyle([
                ("BACKGROUND",  (0, 0), (-1, 0), RED),
                ("TEXTCOLOR",   (0, 0), (-1, 0), WHITE),
                ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",    (0, 0), (-1, -1), 10),
                ("FONTNAME",    (0, 1), (0, -1), "Helvetica-Bold"),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT_RED, WHITE]),
                ("GRID",        (0, 0), (-1, -1), 0.5, GREY),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING",  (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING",(0,0),(-1,-1), 5),
            ])
        ),
        Spacer(1, 0.3 * cm),
        ColorBox("📍", "What is a per-node bias?",
            "Each street also has a tiny personal adjustment number (called a node bias). "
            "Swanston Street always has far more pedestrians than a quiet back lane. "
            "Rather than making the GRU memorise this every time, we give each street its own "
            "personal offset that gets added to the prediction. "
            "It is like each expert having a 'home ground advantage' adjustment.",
            bg=LIGHT_GREY, border=GREY),
    ]


def build_loss_section(styles):
    s = styles
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("06", "Loss — Measuring How Wrong We Are", ORANGE),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "After the model makes its prediction, we compare it to what <i>actually happened</i>. "
            "The <b>loss</b> is the score that tells us how far off the prediction was.",
            s["body"]
        ),
        ColorBox("🎯", "Analogy: Darts",
            "Imagine throwing darts at a bullseye. The loss is the average distance from the "
            "bullseye across all your throws. A loss of 0 means perfect aim every time. "
            "A high loss means you're consistently missing by a lot. "
            "The whole point of training is to reduce this number as much as possible.",
            bg=LIGHT_ORANGE, border=ORANGE),
        Spacer(1, 0.3 * cm),
        Paragraph("We use <b>MAE — Mean Absolute Error</b>:", s["bold"]),
        Paragraph(
            "For each street, at each timestep, we look at the absolute difference between "
            "the prediction and the real value, then average all those differences. "
            "If the model predicted 20 pedestrians but 27 walked past, the error for that "
            "street at that moment is 7.",
            s["body"]
        ),
        Paragraph("Our combined loss formula is:", s["body"]),
        Table(
            [["Total Loss  =  Pedestrian MAE  +  0.5 × Parking MAE"]],
            colWidths=[16 * cm],
            style=TableStyle([
                ("BACKGROUND",   (0, 0), (-1, -1), LIGHT_ORANGE),
                ("TEXTCOLOR",    (0, 0), (-1, -1), DARK),
                ("FONTNAME",     (0, 0), (-1, -1), "Helvetica-Bold"),
                ("FONTSIZE",     (0, 0), (-1, -1), 12),
                ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
                ("TOPPADDING",   (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING",(0, 0), (-1, -1), 10),
                ("BOX",          (0, 0), (-1, -1), 1.5, ORANGE),
                ("ROUNDEDCORNERS",(0,0),(-1,-1),  [5, 5, 5, 5]),
            ])
        ),
        Spacer(1, 0.3 * cm),
        Paragraph(
            "Parking is weighted at 0.5 (half) because it is a secondary task and because "
            "only 143 of 1,397 streets have real parking sensors. We do not want a noisy "
            "secondary signal to overwhelm the pedestrian prediction.",
            s["body"]
        ),
    ]


def build_backprop_section(styles):
    s = styles
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("07", "Backpropagation — Learning from Mistakes", PURPLE),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "Once we know how wrong the model was (the loss), we need to tell the model "
            "<i>which parts were responsible</i> and by how much they should change. "
            "This process is called <b>backpropagation</b> — or 'backprop' for short.",
            s["body"]
        ),
        ColorBox("🍳", "Analogy: A Recipe That Tasted Wrong",
            "Imagine you baked a cake and it came out too salty. "
            "You look back through the recipe and figure out: "
            "'The main problem was step 3 — I added twice as much salt as needed. "
            "Step 1 and Step 2 were fine.'\n\n"
            "Backprop does exactly this — but mathematically. "
            "It traces the error backwards through every layer of the model and calculates "
            "exactly how much each parameter (each number inside the model) contributed to the mistake.",
            bg=LIGHT_PURPLE, border=PURPLE),
        Spacer(1, 0.3 * cm),
        Paragraph("<b>Gradient Descent & the Optimiser</b>", s["bold"]),
        Paragraph(
            "Once backprop tells us which direction to adjust each parameter, the <b>optimiser</b> "
            "(we use Adam) actually makes the adjustments. Think of it like slowly turning a dial: "
            "if the model was guessing too high, turn the dial slightly down. "
            "The size of each turn is controlled by the <b>learning rate</b> (lr=0.001). "
            "Too big a turn and you overshoot; too small and you take forever to improve.",
            s["body"]
        ),
        ColorBox("✂️", "Gradient clipping — preventing wild swings",
            "Sometimes backprop calculates an enormous adjustment — like turning the dial "
            "from 1 all the way to 1000 in one step. This makes training unstable. "
            "We use gradient clipping (max norm = 1.0) to cap any single adjustment, "
            "so the model always takes small, safe steps.",
            bg=LIGHT_GREY, border=GREY),
    ]


def build_loop_section(styles):
    s = styles
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("08", "The Training Loop — Putting It All Together", BLUE),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "Training is simply repeating the same four steps thousands of times until the model "
            "stops improving. One full pass through all these steps is called an <b>epoch</b>.",
            s["body"]
        ),
        Spacer(1, 0.3 * cm),
        TrainingLoopDiagram(),
        Spacer(1, 0.2 * cm),
        Paragraph("The model loops through these four steps 256 times per epoch.", s["caption"]),
        Spacer(1, 0.4 * cm),
        Table(
            [
                ["Step", "What happens", "In our model"],
                ["1. Pick a window",   "Choose a random 24-hour clip from the training data",  "8 random windows at a time (batch size = 8)"],
                ["2. Forward Pass",    "Run the data through the model to get a prediction",    "GCN × 2 → GRU → dual heads"],
                ["3. Calculate Loss",  "Measure how wrong the prediction was",                  "MAE_ped + 0.5 × MAE_park"],
                ["4. Backprop & Update", "Adjust model weights to reduce the error",            "Adam optimiser, lr=0.001, grad clip=1.0"],
            ],
            colWidths=[3.5 * cm, 6.5 * cm, 6 * cm],
            style=TableStyle([
                ("BACKGROUND",  (0, 0), (-1, 0), BLUE),
                ("TEXTCOLOR",   (0, 0), (-1, 0), WHITE),
                ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",    (0, 0), (-1, -1), 9),
                ("FONTNAME",    (0, 1), (0, -1), "Helvetica-Bold"),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT_BLUE, WHITE]),
                ("GRID",        (0, 0), (-1, -1), 0.5, GREY),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING",  (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING",(0,0),(-1,-1), 5),
                ("VALIGN",      (0, 0), (-1, -1), "TOP"),
            ])
        ),
    ]


def build_early_stopping_section(styles):
    s = styles
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("09", "Early Stopping — Knowing When to Stop Studying", GREEN),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "We set a maximum of 200 epochs, but the model doesn't always need all of them. "
            "<b>Early stopping</b> is the rule that says: if the model hasn't improved in 25 "
            "epochs in a row, stop — it has learned everything it can.",
            s["body"]
        ),
        ColorBox("📖", "Analogy: Diminishing Returns on Revision",
            "Imagine you've been revising for an exam. At first every hour of study improves your score. "
            "But after a while, you start to memorise specific questions rather than understanding the topic. "
            "Your practice test scores stop improving and may even get worse.\n\n"
            "At that point, a wise teacher would say: 'Stop. You've learned as much as you can. "
            "More revision will just make you over-fit to the practice questions.'\n\n"
            "Our model stopped at epoch 137 — before the 200 limit — because val ped MAE "
            "hadn't improved for 25 epochs straight.",
            bg=LIGHT_GREEN, border=GREEN),
        Spacer(1, 0.3 * cm),
        ColorBox("🔍", "What is overfitting?",
            "Overfitting is when the model memorises the training data instead of learning general patterns. "
            "It gets perfect scores on training data but fails on new data it hasn't seen. "
            "Early stopping + monitoring the validation set prevents this — we stop as soon as the "
            "model stops improving on data it wasn't trained on.",
            bg=LIGHT_GREY, border=GREY),
    ]


def build_results_section(styles):
    s = styles
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("10", "Final Results — How Well Did It Learn?", RED),
        Spacer(1, 0.4 * cm),
        Paragraph(
            "After training finished at epoch 137, we loaded the best weights and evaluated on "
            "the <b>test set</b> — time slots the model had never seen during training or "
            "early-stopping checks.",
            s["body"]
        ),
        Table(
            [
                ["Metric", "Validation", "Test", "Plain English"],
                ["Ped MAE",   "5.997", "5.804", "Off by ~6 people per 15 min on average"],
                ["Ped R²",    "0.878", "0.876", "Explains 87.6% of foot traffic variation"],
                ["Park MAE",  "0.049", "0.040", "Off by ~4% occupancy on average"],
                ["Park R²",   "0.852", "0.842", "Explains 84.2% of parking variation"],
            ],
            colWidths=[3 * cm, 3 * cm, 3 * cm, 7 * cm],
            style=TableStyle([
                ("BACKGROUND",  (0, 0), (-1, 0), RED),
                ("TEXTCOLOR",   (0, 0), (-1, 0), WHITE),
                ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",    (0, 0), (-1, -1), 10),
                ("FONTNAME",    (0, 1), (0, -1), "Helvetica-Bold"),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT_RED, WHITE]),
                ("GRID",        (0, 0), (-1, -1), 0.5, GREY),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING",  (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING",(0,0),(-1,-1), 5),
            ])
        ),
        Spacer(1, 0.4 * cm),
        ColorBox("🏆", "What does R² = 0.876 mean?",
            "R² (R-squared) is a score between 0 and 1. An R² of 0 means the model is no better "
            "than just guessing the average every time. An R² of 1.0 means the model is perfect.\n\n"
            "Our R² of 0.876 means that 87.6% of the variation in pedestrian counts across all "
            "streets and all time slots is captured by the model. The remaining 12.4% is noise or "
            "patterns too complex or too rare to learn from 5 months of data.",
            bg=LIGHT_RED, border=RED),
    ]


def build_glossary(styles):
    s = styles
    terms = [
        ("Backpropagation", "The process of tracing an error backwards through the model to figure out which parts caused it, so they can be adjusted."),
        ("Batch",           "A small group of training samples processed together in one go (batch size = 8 windows)."),
        ("Cube / Datacube", "The pre-built file containing all 1,397 streets × 14,400 time bins × 23 features, stored as cube.npy."),
        ("Dual Head",       "Two separate output layers sharing the same backbone — one predicts pedestrian flow, one predicts parking occupancy."),
        ("Early Stopping",  "A rule that ends training if the validation score hasn't improved in 25 epochs, preventing overfitting."),
        ("Epoch",           "One full round of training: 256 batches of 8 windows each, followed by a validation check."),
        ("Forward Pass",    "The journey data takes from input through every layer to the output prediction."),
        ("GCN",             "Graph Convolutional Network — a layer that lets each street gather information from its neighbours in a graph."),
        ("GRU",             "Gated Recurrent Unit — a layer that reads a sequence (96 timesteps) and remembers patterns over time."),
        ("Gradient",        "A mathematical direction that tells us which way to nudge each parameter to reduce the loss."),
        ("Loss",            "A number measuring how wrong the model's predictions are. Lower is better."),
        ("MAE",             "Mean Absolute Error — the average size of prediction mistakes, ignoring direction."),
        ("Node Bias",       "A per-street offset learned during training, capturing each street's personal baseline level."),
        ("Normalisation",   "Rescaling all features to a similar range so no single feature dominates just by being numerically large."),
        ("Overfitting",     "When a model memorises training data instead of learning general patterns, causing it to fail on new data."),
        ("R²",              "A score from 0 to 1 measuring how much of the variation in the real data the model explains."),
        ("Semantic Graph",  "A graph connecting streets that are functionally similar (same types of businesses), regardless of physical distance."),
        ("Sliding Window",  "Selecting 96 consecutive time bins as a training sample and shifting along one step at a time."),
        ("Spatial Graph",   "A graph connecting streets that are physically adjacent (share an intersection)."),
        ("Z-score",         "A normalisation method: subtract the mean, divide by the standard deviation, giving values centred around 0."),
    ]

    table_data = [["Term", "Plain-English Definition"]] + [[t, d] for t, d in terms]
    return [
        Spacer(1, 0.8 * cm),
        SectionHeader("11", "Glossary — Every Technical Term Explained", GREY),
        Spacer(1, 0.4 * cm),
        Table(
            table_data,
            colWidths=[4.5 * cm, 11.5 * cm],
            style=TableStyle([
                ("BACKGROUND",  (0, 0), (-1, 0), GREY),
                ("TEXTCOLOR",   (0, 0), (-1, 0), WHITE),
                ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",    (0, 0), (-1, -1), 9),
                ("FONTNAME",    (0, 1), (0, -1), "Helvetica-Bold"),
                ("TEXTCOLOR",   (0, 1), (0, -1), DARK),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT_GREY, WHITE]),
                ("GRID",        (0, 0), (-1, -1), 0.5, GREY),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING",  (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING",(0,0),(-1,-1), 4),
                ("VALIGN",      (0, 0), (-1, -1), "TOP"),
            ])
        ),
    ]


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    out_path = Path(__file__).parent / "training_explainer.pdf"
    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=2 * cm, rightMargin=2 * cm,
        topMargin=2 * cm,  bottomMargin=2 * cm,
        title="How We Train the Model — Plain English Guide",
        author="Melbourne CBD Street Analysis",
    )

    styles  = make_styles()
    content = []

    content += build_cover(styles)
    content += build_cube_section(styles)
    content += build_split_section(styles)
    content += build_window_section(styles)
    content += build_forward_pass_section(styles)
    content += build_dual_head_section(styles)
    content += build_loss_section(styles)
    content += build_backprop_section(styles)
    content += build_loop_section(styles)
    content += build_early_stopping_section(styles)
    content += build_results_section(styles)
    content += build_glossary(styles)

    doc.build(content)
    print(f"PDF written to: {out_path}")


if __name__ == "__main__":
    main()
