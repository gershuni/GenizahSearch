"""Spike 001 — Meiri glyph-level reorder vs. current LOCAL PDF extractor.

ONE question: does Ephraim Meiri's glyph-level span-reorder core produce BETTER
plain-text extraction than the current production stack on real problem PDFs?

- CURRENT  = shared.local_indexer.extract_pdf_pages  (the REAL production path:
             get_text("blocks") + _collapse_intra_block_newlines, with the
             sort=True + _fix_sort_true_rtl_line fallback for single-word pages)
- MEIRI     = a MINIMAL plain-text wrapper around Meiri's REAL functions
             (_normalize_span_dir + _span_text + _attach_nikud_page) operating on
             get_text("rawdict"). We deliberately do NOT replicate Meiri's DOCX
             pipeline (tables, header/footer stripping) — only the reorder core,
             which is the part the phase would adopt for Tantivy indexing.

Usage (from project root C:\\Genizahsearch):
    python .planning/spikes/001-meiri-glyph-reorder-vs-current/compare_extractors.py \
        "path\\to\\problem1.pdf" "path\\to\\problem2.pdf" ...

Writes per-PDF side-by-side text to ./out/<pdfname>.txt in the spike dir and
prints a quantitative summary table to stdout.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import unicodedata

import fitz  # PyMuPDF

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
OUT_DIR = os.path.join(HERE, "out")

# --- import the REAL current extractor ---------------------------------------
sys.path.insert(0, PROJECT_ROOT)
from shared.local_indexer import extract_pdf_pages  # noqa: E402

# --- import Meiri's REAL reorder functions by file path (avoid package setup) -
_MEIRI_PATH = os.path.join(PROJECT_ROOT, "ephraim_meiri_pdf_converter", "pdf_to_docx.py")
_spec = importlib.util.spec_from_file_location("meiri_pdf_to_docx", _MEIRI_PATH)
meiri = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(meiri)  # may pull python-docx etc.; both are desktop deps

_HEB_LO, _HEB_HI = 0x0590, 0x05FF  # Hebrew block (letters + nikud)


def _is_hebrew_letter(ch: str) -> bool:
    return bool(ch) and 0x05D0 <= ord(ch) <= 0x05EA


def meiri_extract_page_text(page) -> str:
    """Minimal plain-text emission using Meiri's reorder core.

    block -> lines (top-to-bottom) -> spans (normalized, rightmost-first) ->
    chars-as-source-of-truth text. Intra-block newlines collapsed to spaces to
    match the current extractor's _collapse_intra_block_newlines behaviour, so
    the comparison isolates *reorder quality*, not paragraph policy.
    """
    d = page.get_text("rawdict")
    # Re-attach standalone nikud spans (fitz often splits them onto their own line)
    try:
        meiri._attach_nikud_page(d)
    except Exception:
        pass
    block_texts: list[str] = []
    for blk in d.get("blocks", []):
        if blk.get("type") != 0:
            continue
        lines = sorted(blk.get("lines", []), key=lambda ln: ln.get("bbox", (0, 0, 0, 0))[1])
        line_texts: list[str] = []
        for ln in lines:
            spans = ln.get("spans", [])
            for sp in spans:
                try:
                    meiri._normalize_span_dir(sp)
                except Exception:
                    pass
            # RTL line assembly: rightmost span read first. Mixed LTR runs inside
            # a span are already handled internally by _normalize_span_dir.
            spans_sorted = sorted(
                spans, key=lambda sp: -((sp.get("bbox", (0, 0, 0, 0))[0] + sp.get("bbox", (0, 0, 0, 0))[2]) / 2)
            )
            txt = "".join(meiri._span_text(sp) for sp in spans_sorted).strip()
            if txt:
                line_texts.append(txt)
        block_text = " ".join(line_texts).strip()
        if block_text:
            block_texts.append(block_text)
    return "\n\n".join(block_texts)


def _sample_pages(path: str, max_n: int = 6) -> list[int]:
    """Spread sample across the doc (1-based), skipping the very first pages
    which are usually title/TOC. Returns sorted page numbers."""
    doc = fitz.open(path)
    try:
        n = doc.page_count
    finally:
        doc.close()
    if n <= max_n:
        return list(range(1, n + 1))
    lo = max(1, int(n * 0.08))
    hi = int(n * 0.92)
    step = (hi - lo) / (max_n - 1)
    return sorted({int(round(lo + i * step)) for i in range(max_n)})


def current_extract_sampled(path: str, targets: set[int]) -> dict[int, str]:
    """Real production output for sampled pages. Breaks once past the last target
    so we don't pay full-document iteration cost on 600-page books."""
    out: dict[int, str] = {}
    last = max(targets)
    for page_num, text, _title in extract_pdf_pages(path):
        if page_num in targets:
            out[page_num] = text
        if page_num >= last:
            break
    return out


def meiri_extract_sampled(path: str, targets: set[int]) -> dict[int, str]:
    out: dict[int, str] = {}
    doc = fitz.open(path)
    try:
        for i in sorted(targets):
            if i < 1 or i > doc.page_count:
                continue
            t = meiri_extract_page_text(doc[i - 1])
            out[i] = t
    finally:
        doc.close()
    return out


def _metrics(text: str) -> dict:
    """Lightweight signal. The killer bug (D-F13) fragments emphasized words into
    single Hebrew letters; bidi fragmentation also raises single-letter token
    counts. Fewer single-Hebrew-letter tokens + longer mean Hebrew word length =
    healthier extraction. Reading-order correctness still needs human eyes.
    """
    tokens = text.split()
    heb_tokens = [t for t in tokens if any(_is_hebrew_letter(c) for c in t)]
    single_heb = [t for t in heb_tokens if len([c for c in t if _is_hebrew_letter(c)]) == 1]
    heb_word_lens = [len([c for c in t if _is_hebrew_letter(c)]) for t in heb_tokens]
    return {
        "chars": len(text),
        "tokens": len(tokens),
        "heb_tokens": len(heb_tokens),
        "single_heb_letter_tokens": len(single_heb),
        "mean_heb_word_len": round(sum(heb_word_lens) / len(heb_word_lens), 2) if heb_word_lens else 0.0,
    }


def main(argv: list[str]) -> int:
    pdfs = argv[1:]
    if not pdfs:
        print("Usage: python compare_extractors.py <pdf1> [pdf2 ...]", file=sys.stderr)
        return 2
    os.makedirs(OUT_DIR, exist_ok=True)

    print(f"{'PDF':<34} {'page':>4} {'src':<7} {'chars':>7} {'heb_tok':>7} {'1-letter':>8} {'meanWL':>7}")
    print("-" * 84)

    for pdf in pdfs:
        name = os.path.basename(pdf)
        if not os.path.exists(pdf):
            print(f"!! MISSING: {pdf}", file=sys.stderr)
            continue
        targets = set(_sample_pages(pdf))
        cur = current_extract_sampled(pdf, targets)
        mei = meiri_extract_sampled(pdf, targets)
        all_pages = sorted(set(cur) | set(mei))

        lines_out: list[str] = [f"===== {name} =====\n"]
        for p in all_pages:
            c_txt, m_txt = cur.get(p, ""), mei.get(p, "")
            cm, mm = _metrics(c_txt), _metrics(m_txt)
            short = name[:33]
            print(f"{short:<34} {p:>4} {'CURRENT':<7} {cm['chars']:>7} {cm['heb_tokens']:>7} "
                  f"{cm['single_heb_letter_tokens']:>8} {cm['mean_heb_word_len']:>7}")
            print(f"{'':<34} {p:>4} {'MEIRI':<7} {mm['chars']:>7} {mm['heb_tokens']:>7} "
                  f"{mm['single_heb_letter_tokens']:>8} {mm['mean_heb_word_len']:>7}")
            lines_out.append(f"\n----- page {p} -----")
            lines_out.append(f"[metrics] CURRENT {cm}")
            lines_out.append(f"[metrics] MEIRI   {mm}")
            lines_out.append("\n--- CURRENT ---")
            lines_out.append(c_txt)
            lines_out.append("\n--- MEIRI ---")
            lines_out.append(m_txt)
        out_path = os.path.join(OUT_DIR, name + ".txt")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines_out))
        print(f"  -> wrote {out_path}")
    print("\nNote: lower 1-letter token count and higher meanWL => healthier Hebrew extraction.")
    print("Reading-order correctness still needs human review of the out/*.txt files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
