# Phase 102 — Codex Pre-Planning Critique

> Cross-AI review (`codex exec`, gpt-5.5, xhigh reasoning) of the discuss-phase tentative
> locks, run BEFORE CONTEXT.md was finalized. Codex read the live codebase + PyMuPDF docs.
> Brief: `$CLAUDE_JOB_DIR/tmp/codex_brief_phase102.md`. Full transcript in session tool-results.

## HIGH

1. **Safety net too weak.** Falling back to `blocks` only when rawdict is *empty* misses the
   biggest regression class: rawdict returns plenty of text but our RTL reconstruction
   *damages clean LTR/modern PDFs*. Current `extract_pdf_pages` deliberately avoids fallback
   on clean PDFs; tests encode that. **Add an LTR/low-Hebrew page guard:** for low-RTL pages,
   compare rawdict output vs old `blocks` (token count / Jaccard); fall back to `blocks` if
   rawdict loses or scrambles too much. → *needs Hillel decision (extends safety-net lock).*

2. **Do NOT strip nikud at extraction if `cached_text` stays display/source text.**
   `_write_page_doc` writes the SAME `text` into both Tantivy `content` AND compressed
   `local_pages.cached_text` (`shared/local_indexer.py:2466` + `:2484`). cached_text drives
   result display, page browsing, AND index rebuild. Strip-at-extraction is irreversible →
   display becomes consonantal-only forever. **Better:** keep original (nikud-bearing) text
   as cached/display; index/search a *normalized* (nikud-stripped) form. The codebase ALREADY
   strips nikud at search time (`genizah_core.strip_nikud` / `strip_search_diacritics`, used
   query-side AND in the regex filter phase), so keeping nikud in extraction does not hurt
   recall. Likely needs an `extraction_format_version` bump. → *contradicts the strip-at-
   extraction lock; needs Hillel decision.*

3. **"De-space before reorder" is right, but do not implement it by mutating `chars` with
   synthetic zero-bbox space glyphs before Meiri reorder.** `_normalize_span_dir` segments by
   char-bbox x-direction jumps; zero-bbox spaces create bogus jumps. **Safe pipeline:**
   original glyphs → drop nikud for metrics → infer word units via bbox unions → reorder the
   units/segments → emit a plain string. Never feed synthetic-space chars back into the
   bbox-dependent segment code. → *implementation constraint, bake into spec.*

4. **New `corrupt_encoding` status is broader than it sounds.** Must be wired into
   `_ERROR_STATUSES_KEPT` (`:125`), scan success/error classification (`:1951`), folder
   counter aggregation (`:2868`), AND tree label/color mapping (`desktop/my_library_tab.py:333`)
   — else it displays/counts wrong. → *implementation completeness, bake into spec.*

## MED

5. **Per-line y-regrouping must be baseline/font-size based, not fixed `y_tol=2.5`.** Use only
   base glyphs for row grouping; ignore nikud combining marks and (maybe) superscript footnote
   refs when computing the line baseline. Fixed y-band splits vocalized Hebrew / merges
   superscripts into the wrong row. → *bake into spec.*

6. **1.8× median needs hysteresis.** "Ignore space glyphs for median, use as hints" is valid
   only if space glyphs are *soft* evidence. Some PDFs encode a literal space between every
   letter. Use two thresholds: hard break for very large gaps; mid-gap break only if
   corroborated by an explicit space cluster, punctuation boundary, font/span boundary, or
   abnormal-long-token guard. Add fixtures for BOTH under-split and over-merge. → *bake in.*

7. **Codepoint-garbage detection must be conservative.** Count U+FFFD, PUA, noncharacters,
   illegal controls, very-low wordlike ratio. Do NOT treat "not Hebrew/Latin" as bad — allow
   Arabic, Greek, common math, Hebrew presentation forms, bidi marks, broad punctuation. Flag
   file-level `corrupt_encoding` only with strong evidence (e.g. len≥100 AND
   FFFD/PUA/control garbage > 5–10%, OR wordlike chars < ~40% with high unknowns). → *bake in.*

## LOW / UAT traps

8. **Disable images in rawdict flags.** PyMuPDF: RAWDICT is ~4.5× baseline with default image
   behavior, ~1.68× with images excluded. Pass flags to drop image data — matters for a 12K
   re-index. → *bake in (addresses the perf concern).*
9. Normalize Hebrew presentation forms / Latin ligatures deliberately; keep display vs index
   text separate (reinforces #2).
10. Add a cheap "multi-column suspected" detector + log/status marker even though full column
    support is deferred — so UAT footnote/dictionary/Talmud failures are explainable. → *bake in.*
11. Use rawdict JSON / glyph-trace fixtures if real PDFs can't be committed — the logic is
    bbox-dependent; text-only `.expected.txt` files won't pin a bbox bug. → *bake into verification.*

**PyMuPDF confirmations:** rawdict replaces span text with per-char `chars` (`bbox`, `origin`,
`c`); `sort=True` on rawdict sorts blocks by coordinates; rawdict has significant
data-volume/perf cost unless image extraction is disabled.
