"""Spike 001 — visual report: PDF page image | CURRENT text | MEIRI text.

The strongest oracle we have without OCR: render the actual page next to both
extractions so a human can see which reading order/spacing is right, across the
diverse PDF "sorts" the profiler found.

MEIRI side here is FAITHFUL to Meiri's real assembly (regroup lines ->
normalize span direction -> fix visual brackets, block-scope), unlike the
minimal wrapper in compare_extractors.py — so F-C (reversed parens) is judged
fairly.

Usage (from project root):
    python .planning/spikes/001-meiri-glyph-reorder-vs-current/visual_report.py
Writes out/VISUAL_REPORT.html
"""
from __future__ import annotations

import base64
import html
import importlib.util
import os
import sys

import fitz

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, HERE)

from shared.local_indexer import extract_pdf_pages  # noqa: E402
import compare_extractors as C  # noqa: E402  (reuse current_extract_sampled)

_MEIRI_PATH = os.path.join(PROJECT_ROOT, "ephraim_meiri_pdf_converter", "pdf_to_docx.py")
_spec = importlib.util.spec_from_file_location("meiri_pdf_to_docx", _MEIRI_PATH)
meiri = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(meiri)

ROOT = r"C:\Users\gersh\Dropbox\ספרים"
OUT = os.path.join(HERE, "out", "VISUAL_REPORT.html")

# (folder, page_fraction, why) — one representative PDF per "sort"
TARGETS = [
    ("ראשונים", 0.5, "letter-spacing 0.46 + 2col (Otzar ha-Geonim)"),
    ("רמבם", 0.5, "nikud 0.31 + letter-spacing 0.21 + 2col"),
    ("תלמודים", 0.5, "Talmud 2-column layout (Vilna)"),
    ("שפות", 0.5, "multi-column dictionary, mixed Heb/Latin"),
    ("תוספתא ליברמן", 0.5, "critical edition w/ apparatus (footnotes)"),
    ("תפילה", 0.5, "IMAGE-ONLY scan (siddur) — OCR gap demo"),
    ("מדרש", 0.5, "IMAGE-ONLY scan (Albeck) — OCR gap demo"),
]


def first_real_pdf(folder: str) -> str | None:
    for dp, _, fs in os.walk(folder):
        for f in sorted(fs):
            if f.lower().endswith(".pdf") and not f.startswith("._"):
                p = os.path.join(dp, f)
                try:
                    if os.path.getsize(p) > 20000:
                        return p
                except OSError:
                    continue
    return None


def meiri_full_page_text(page) -> str:
    d = page.get_text("rawdict")
    try:
        meiri._attach_nikud_page(d)
    except Exception:
        pass
    block_texts: list[str] = []
    for blk in d.get("blocks", []):
        if blk.get("type") != 0:
            continue
        # regroup visual lines, then normalize span direction, then fix brackets
        try:
            blk["lines"] = meiri._regroup_lines(blk)
        except Exception:
            pass
        for ln in blk.get("lines", []):
            for sp in ln.get("spans", []):
                try:
                    meiri._normalize_span_dir(sp)
                except Exception:
                    pass
        try:
            meiri._fix_visual_brackets(blk.get("lines", []))
        except Exception:
            pass
        lines = sorted(blk.get("lines", []), key=lambda ln: ln.get("bbox", (0, 0, 0, 0))[1])
        line_texts = []
        for ln in lines:
            spans = sorted(ln.get("spans", []),
                           key=lambda sp: -((sp.get("bbox", (0, 0, 0, 0))[0] + sp.get("bbox", (0, 0, 0, 0))[2]) / 2))
            txt = "".join(meiri._span_text(sp) for sp in spans).strip()
            if txt:
                line_texts.append(txt)
        bt = " ".join(line_texts).strip()
        if bt:
            block_texts.append(bt)
    return "\n\n".join(block_texts)


def page_img_datauri(page, dpi: int = 96) -> str:
    pix = page.get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72))
    b = pix.tobytes("png")
    return "data:image/png;base64," + base64.b64encode(b).decode()


def current_page_text(pdf: str, pg: int) -> str:
    return C.current_extract_sampled(pdf, {pg}).get(pg, "(no text layer — current extractor skips this page)")


def main() -> int:
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    parts = ["<!doctype html><meta charset=utf-8><title>Spike 001 visual report</title>",
             "<style>body{font-family:sans-serif;margin:0;background:#f4f4f4}"
             "h2{background:#222;color:#fff;padding:8px 12px;margin:0}"
             ".why{background:#fffae6;padding:4px 12px;font-size:13px;color:#555}"
             ".row{display:flex;gap:8px;padding:8px;align-items:flex-start}"
             ".col{flex:1;background:#fff;border:1px solid #ccc;border-radius:4px;padding:8px;"
             "max-height:80vh;overflow:auto}"
             ".col h3{margin:0 0 6px;font-size:13px;color:#888;position:sticky;top:0;background:#fff}"
             ".heb{direction:rtl;text-align:right;font-size:15px;line-height:1.7;white-space:pre-wrap}"
             "img{max-width:100%;height:auto}</style>"]
    for folder, frac, why in TARGETS:
        base = os.path.join(ROOT, folder)
        pdf = first_real_pdf(base)
        if not pdf:
            parts.append(f"<h2>{html.escape(folder)} — no pdf</h2>")
            continue
        try:
            doc = fitz.open(pdf)
            n = doc.page_count
            pg = max(1, min(n, int(n * frac)))
            page = doc[pg - 1]
            img = page_img_datauri(page)
            mei = meiri_full_page_text(page)
            doc.close()
            cur = current_page_text(pdf, pg)
        except Exception as e:
            parts.append(f"<h2>{html.escape(folder)} — ERROR {html.escape(type(e).__name__)}: {html.escape(str(e)[:120])}</h2>")
            continue
        parts.append(f"<h2>{html.escape(folder)} &mdash; {html.escape(os.path.basename(pdf))} &mdash; page {pg}/{n}</h2>")
        parts.append(f"<div class=why>{html.escape(why)}</div>")
        parts.append("<div class=row>")
        parts.append(f"<div class=col><h3>PAGE IMAGE</h3><img src='{img}'></div>")
        parts.append(f"<div class=col><h3>CURRENT</h3><div class=heb>{html.escape(cur)}</div></div>")
        parts.append(f"<div class=col><h3>MEIRI (full: regroup+reorder+brackets)</h3><div class=heb>{html.escape(mei)}</div></div>")
        parts.append("</div>")
        print(f"ok: {folder} p{pg}/{n}  {os.path.basename(pdf)[:40]}")
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))
    print(f"-> wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
