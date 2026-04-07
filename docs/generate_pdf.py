"""
Convert docs/pipeline_report.md to a clean, professional PDF.
White pages, black text, clear typographic hierarchy.
Run from repo root: python docs/generate_pdf.py
"""
import re
from pathlib import Path
from fpdf import FPDF

MD_PATH  = Path(__file__).parent / "pipeline_report.md"
OUT_PATH = Path(__file__).parent / "pipeline_report.pdf"

# ── Colour palette (white-page, print-friendly) ─────────────────────────────
WHITE       = (255, 255, 255)
BLACK       = (20,  20,  20)    # near-black for body text
DARK_GREY   = (60,  60,  60)    # secondary text / captions
MID_GREY    = (120, 120, 120)   # muted labels, footer
LIGHT_GREY  = (220, 220, 220)   # divider lines, code border
CODE_BG     = (245, 245, 245)   # very light grey code background
CODE_FG     = (40,  40,  40)    # dark code text
H1_COLOR    = (15,  55, 115)    # deep navy — main title
H2_COLOR    = (15,  55, 115)    # same navy — section heading
H3_COLOR    = (40,  90, 160)    # medium blue — sub-section
H4_COLOR    = (70, 110, 180)    # lighter blue — sub-sub-section
ACCENT      = (40,  40,  40)    # bold inline text
RULE_COLOR  = (180, 180, 180)   # horizontal rule


# ── PDF class ────────────────────────────────────────────────────────────────
class ReportPDF(FPDF):
    def header(self):
        # Thin top rule on every page (except page 1)
        if self.page_no() > 1:
            self.set_draw_color(*RULE_COLOR)
            self.set_line_width(0.2)
            self.line(self.l_margin, 12, self.w - self.r_margin, 12)
            self.set_font("Helvetica", "I", 7.5)
            self.set_text_color(*MID_GREY)
            self.set_y(8)
            self.cell(0, 5, "Melbourne CBD Curbside Reallocation Pipeline -- Technical Report",
                      align="L")
            self.ln(8)

    def footer(self):
        self.set_y(-13)
        self.set_draw_color(*RULE_COLOR)
        self.set_line_width(0.2)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(2)
        self.set_font("Helvetica", "I", 7.5)
        self.set_text_color(*MID_GREY)
        self.cell(0, 5, f"Page {self.page_no()}", align="C")

    def section_rule(self):
        """Thin horizontal line used after ## headings."""
        self.set_draw_color(*H2_COLOR)
        self.set_line_width(0.5)
        y = self.get_y()
        self.line(self.l_margin, y, self.w - self.r_margin, y)
        self.ln(4)

    def light_rule(self):
        """Very light divider for --- markers."""
        self.set_draw_color(*RULE_COLOR)
        self.set_line_width(0.3)
        y = self.get_y() + 1
        self.line(self.l_margin, y, self.w - self.r_margin, y)
        self.ln(5)


# ── Text sanitizer ───────────────────────────────────────────────────────────
def sanitize(text: str) -> str:
    """Replace characters outside latin-1 with ASCII equivalents."""
    table = {
        '\u2014': '--',   '\u2013': '-',
        '\u2018': "'",    '\u2019': "'",
        '\u201c': '"',    '\u201d': '"',
        '\u2026': '...',  '\u00b7': '*',
        '\u0394': 'D',    '\u03b1': 'alpha',
        '\u03b2': 'beta', '\u03c3': 'sigma',
        '\u00b2': '^2',   '\u00b3': '^3',
        '\u00d7': 'x',    '\u2248': '~',
        '\u2265': '>=',   '\u2264': '<=',
        '\u00e9': 'e',    '\u00e0': 'a',
        '\u00e8': 'e',    '\u00fc': 'u',
        '\u2192': '->',   '\u2190': '<-',
        '\u00b0': 'deg',  '\u00b1': '+/-',
    }
    for ch, rep in table.items():
        text = text.replace(ch, rep)
    return text.encode('latin-1', errors='replace').decode('latin-1')


# ── Inline-markup splitter ───────────────────────────────────────────────────
INLINE_RE = re.compile(r'(\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`)')

def write_inline(pdf: ReportPDF, text: str, base_size: float = 10):
    """Write text with **bold**, *italic*, and `code` inline formatting."""
    parts = INLINE_RE.split(text)
    for part in parts:
        if part.startswith('**') and part.endswith('**'):
            pdf.set_font("Helvetica", "B", base_size)
            pdf.set_text_color(*ACCENT)
            pdf.write(5.5, part[2:-2])
        elif part.startswith('*') and part.endswith('*'):
            pdf.set_font("Helvetica", "I", base_size)
            pdf.set_text_color(*DARK_GREY)
            pdf.write(5.5, part[1:-1])
        elif part.startswith('`') and part.endswith('`'):
            # Inline code: monospace, slightly smaller, grey background hint
            pdf.set_font("Courier", "", base_size - 1)
            pdf.set_text_color(*CODE_FG)
            pdf.write(5.5, part[1:-1])
        else:
            pdf.set_font("Helvetica", "", base_size)
            pdf.set_text_color(*BLACK)
            pdf.write(5.5, part)
    pdf.ln(6)


# ── Code block renderer ──────────────────────────────────────────────────────
def render_code_block(pdf: ReportPDF, lines: list[str]):
    line_h  = 4.8
    pad_v   = 3.5
    pad_h   = 4
    block_h = len(lines) * line_h + pad_v * 2
    x       = pdf.l_margin
    w       = pdf.w - pdf.l_margin - pdf.r_margin

    if pdf.get_y() + block_h > pdf.h - pdf.b_margin - 15:
        pdf.add_page()

    y_top = pdf.get_y()
    pdf.set_fill_color(*CODE_BG)
    pdf.set_draw_color(*LIGHT_GREY)
    pdf.set_line_width(0.3)
    pdf.rect(x, y_top, w, block_h, style="FD")

    pdf.set_y(y_top + pad_v)
    for line in lines:
        display = (line[:120] + "...") if len(line) > 123 else line
        pdf.set_font("Courier", "", 8)
        pdf.set_text_color(*CODE_FG)
        pdf.set_x(x + pad_h)
        pdf.cell(w - pad_h * 2, line_h, display,
                 new_x="LMARGIN", new_y="NEXT")

    pdf.set_y(y_top + block_h + 3)


# ── Heading helpers ──────────────────────────────────────────────────────────
def strip_md(text: str) -> str:
    """Remove all markdown markers from a string."""
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'\*(.+?)\*',     r'\1', text)
    text = re.sub(r'`(.+?)`',       r'\1', text)
    return text.strip()


def render_h1(pdf: ReportPDF, text: str):
    pdf.ln(8)
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*H1_COLOR)
    pdf.multi_cell(0, 11, strip_md(text), align="L")
    pdf.ln(3)


def render_h2(pdf: ReportPDF, text: str):
    # Keep H2 together with following content — page break if near bottom
    if pdf.get_y() > pdf.h - pdf.b_margin - 45:
        pdf.add_page()
    pdf.ln(6)
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*H2_COLOR)
    pdf.multi_cell(0, 8, strip_md(text), align="L")
    pdf.section_rule()


def render_h3(pdf: ReportPDF, text: str):
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*H3_COLOR)
    pdf.multi_cell(0, 7, strip_md(text), align="L")
    pdf.ln(1)


def render_h4(pdf: ReportPDF, text: str):
    pdf.ln(3)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*H4_COLOR)
    pdf.multi_cell(0, 6, strip_md(text), align="L")
    pdf.ln(1)


# ── Bullet renderer ──────────────────────────────────────────────────────────
def render_bullet(pdf: ReportPDF, raw: str):
    depth       = len(raw) - len(raw.lstrip())
    indent_mm   = 5 + depth * 4
    bullet_text = re.sub(r'^\s*[-*+\d.]+\s+', '', raw)

    # Bullet symbol
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(*MID_GREY)
    pdf.set_x(pdf.l_margin + indent_mm - 4)
    symbol = "-" if depth == 0 else "o"
    pdf.cell(4, 5.5, symbol)

    # Text
    pdf.set_x(pdf.l_margin + indent_mm)
    write_inline(pdf, bullet_text, base_size=10)


# ── Main builder ─────────────────────────────────────────────────────────────
def build_pdf():
    pdf = ReportPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.set_margins(20, 20, 20)
    pdf.add_page()

    raw_lines = MD_PATH.read_text(encoding="utf-8").splitlines()
    lines = [sanitize(l) for l in raw_lines]

    in_code  = False
    code_buf: list[str] = []

    i = 0
    while i < len(lines):
        raw = lines[i]

        # ── Code fence ──────────────────────────────────────────────────────
        if raw.strip().startswith("```"):
            if in_code:
                render_code_block(pdf, code_buf)
                code_buf = []
                in_code  = False
            else:
                in_code = True
            i += 1
            continue

        if in_code:
            code_buf.append(raw)
            i += 1
            continue

        # ── Headings ────────────────────────────────────────────────────────
        if raw.startswith("# ") and not raw.startswith("## "):
            render_h1(pdf, raw[2:])
        elif raw.startswith("## ") and not raw.startswith("### "):
            render_h2(pdf, raw[3:])
        elif raw.startswith("### ") and not raw.startswith("#### "):
            render_h3(pdf, raw[4:])
        elif raw.startswith("#### "):
            render_h4(pdf, raw[5:])

        # ── Horizontal rule ─────────────────────────────────────────────────
        elif raw.strip() == "---":
            pdf.light_rule()

        # ── Bullet / numbered list ───────────────────────────────────────────
        elif re.match(r'^\s*[-*+]\s', raw) or re.match(r'^\s*\d+\.\s', raw):
            render_bullet(pdf, raw)

        # ── Blank line ───────────────────────────────────────────────────────
        elif raw.strip() == "":
            pdf.ln(2)

        # ── Normal paragraph ─────────────────────────────────────────────────
        else:
            pdf.set_x(pdf.l_margin)
            write_inline(pdf, raw, base_size=10)

        i += 1

    pdf.output(str(OUT_PATH))
    print(f"Done -> {OUT_PATH}")


if __name__ == "__main__":
    build_pdf()
