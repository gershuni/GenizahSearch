# Spike Manifest

## Idea

Phase 102 (PDF Extraction Reorder) rests on one bet: that Ephraim Meiri's glyph-level
reorder core (`ephraim_meiri_pdf_converter/pdf_to_docx.py::_normalize_span_dir`, operating
on `get_text("rawdict")` per-character bboxes) produces better LOCAL PDF plain-text
extraction than the current production stack (`get_text("blocks")` +
`_collapse_intra_block_newlines`, with a `sort=True` + `_fix_sort_true_rtl_line` fallback).
These spikes validate that bet empirically against Hillel's real problem PDFs before
committing to a 1–2 day phase. The result drives a three-way decision: adopt wholesale,
adopt for clean PDFs only (OCR out of scope), or abandon the phase and patch D-F13 narrowly.

## Spikes

| # | Name | Validates | Verdict | Tags |
|---|------|-----------|---------|------|
| 001 | meiri-glyph-reorder-vs-current | Meiri's rawdict reorder core beats the current blocks+S-1 stack on real problem PDFs | PARTIAL — Meiri's RTL reorder helps Hebrew (order/headers/brackets) but NOT letter-spacing; current wins on Latin (reorder must be RTL-gated). Catalogued 7 failure modes F-A..F-G (incl. corrupt encoding + image-only-scan prevalence). DECISION: Phase 102 = RTL-gated text-layer rewrite (reorder + adaptive de-spacing + brackets/punct + headers + F-G detect, no LTR regression); OCR deferred as optional extension (seeded). | pdf, rtl, hebrew, extraction, ocr, phase-102, d-f13, d-f14, d-f2 |
