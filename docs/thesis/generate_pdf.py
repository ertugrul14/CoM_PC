"""Convert curbside_intensification_thesis.md to a styled PDF."""
import pathlib
import markdown2
from xhtml2pdf import pisa

SRC = pathlib.Path(__file__).parent / "curbside_intensification_thesis.md"
DST = pathlib.Path(__file__).parent / "curbside_intensification_thesis.pdf"

CSS = """
@page {
    size: A4;
    margin: 2.5cm 2.5cm 2.5cm 2.5cm;
    @frame footer {
        -pdf-frame-content: footerContent;
        bottom: 0.5cm;
        margin-left: 2.5cm;
        margin-right: 2.5cm;
        height: 1cm;
    }
}

body {
    font-family: Georgia, 'Times New Roman', serif;
    font-size: 11pt;
    line-height: 1.6;
    color: #1a1a1a;
    text-align: justify;
}

h1 {
    font-size: 24pt;
    font-weight: bold;
    text-align: center;
    margin-top: 40pt;
    margin-bottom: 8pt;
    color: #111;
    page-break-before: auto;
}

h2 {
    font-size: 16pt;
    font-weight: bold;
    margin-top: 28pt;
    margin-bottom: 12pt;
    color: #222;
    border-bottom: 1px solid #ccc;
    padding-bottom: 4pt;
    page-break-before: always;
}

/* Don't page-break before the very first h2 (subtitle) */
h2:first-of-type {
    page-break-before: avoid;
    font-size: 13pt;
    text-align: center;
    border-bottom: none;
    font-weight: normal;
    font-style: italic;
    color: #444;
}

h3 {
    font-size: 13pt;
    font-weight: bold;
    margin-top: 20pt;
    margin-bottom: 8pt;
    color: #333;
}

p {
    margin-top: 0;
    margin-bottom: 8pt;
    orphans: 3;
    widows: 3;
}

strong {
    font-weight: bold;
}

em {
    font-style: italic;
}

blockquote {
    margin-left: 20pt;
    margin-right: 20pt;
    font-style: italic;
    color: #555;
    border-left: 3pt solid #ccc;
    padding-left: 10pt;
}

table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 12pt;
    margin-bottom: 12pt;
    font-size: 10pt;
}

th {
    background-color: #f0f0f0;
    border: 1pt solid #999;
    padding: 6pt 8pt;
    text-align: left;
    font-weight: bold;
}

td {
    border: 1pt solid #bbb;
    padding: 5pt 8pt;
    text-align: left;
}

tr:nth-child(even) td {
    background-color: #fafafa;
}

hr {
    border: none;
    border-top: 1pt solid #999;
    margin-top: 20pt;
    margin-bottom: 20pt;
}

/* Figure placeholders styled via class in post-processing */
.figure-placeholder {
    font-style: italic;
    color: #555;
    text-align: center;
    margin-top: 12pt;
    margin-bottom: 12pt;
    padding: 16pt;
    background-color: #f9f9f9;
    border: 1pt dashed #bbb;
}

ul, ol {
    margin-top: 4pt;
    margin-bottom: 8pt;
    padding-left: 20pt;
}

li {
    margin-bottom: 4pt;
}

sup {
    font-size: 8pt;
    vertical-align: super;
}

/* Footnotes section */
.footnotes {
    font-size: 9pt;
    border-top: 1pt solid #999;
    margin-top: 20pt;
    padding-top: 8pt;
}

code {
    font-family: 'Courier New', monospace;
    font-size: 10pt;
    background-color: #f5f5f5;
    padding: 1pt 3pt;
}

pre {
    font-family: 'Courier New', monospace;
    font-size: 9pt;
    background-color: #f5f5f5;
    padding: 8pt;
    border: 1pt solid #ddd;
    page-break-inside: avoid;
}
"""


def convert_footnotes(html: str) -> str:
    """Convert ^N footnote markers to superscript HTML."""
    import re
    html = re.sub(r'\^(\d+)', r'<sup>\1</sup>', html)
    return html


def style_figure_placeholders(html: str) -> str:
    """Wrap [Figure N: ...] paragraphs in a styled div."""
    import re
    html = re.sub(
        r'<p>\[Figure (\d+):([^\]]+)\]</p>',
        r'<div class="figure-placeholder"><strong>Figure \1</strong>:\2</div>',
        html,
    )
    return html


def main():
    md_text = SRC.read_text(encoding="utf-8")
    html_body = markdown2.markdown(
        md_text,
        extras=["tables", "fenced-code-blocks", "cuddled-lists", "break-on-newline"],
    )
    html_body = convert_footnotes(html_body)
    html_body = style_figure_placeholders(html_body)

    full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>{CSS}</style>
</head>
<body>
{html_body}
<div id="footerContent" style="text-align: center; font-size: 9pt; color: #888;">
    <pdf:pagenumber />
</div>
</body>
</html>"""

    with open(DST, "wb") as f:
        status = pisa.CreatePDF(full_html, dest=f, encoding="utf-8")

    if status.err:
        print(f"ERROR: PDF generation failed with {status.err} errors")
    else:
        size_kb = DST.stat().st_size / 1024
        print(f"PDF generated: {DST} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
