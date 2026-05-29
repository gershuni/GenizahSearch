---
phase: 102-pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13-
verified: 2026-05-29T12:00:00Z
status: passed
score: 18/18 must-haves verified
overrides_applied: 0
re_verification: null
---

# Phase 102: LOCAL PDF Extraction Rewrite Verification Report

**Phase Goal:** Rewrite the LOCAL PDF text-layer extractor in shared/local_indexer.py onto a page.get_text("rawdict") (per-glyph bbox) foundation that produces clean, searchable plain text for the Tantivy indexer, addressing the Spike 001 failure-mode catalog (F-A..F-G): RTL-gated segment reorder (adapted Meiri core), adaptive per-line letter-spacing de-collapse, punctuation normalization (F-B), corrupt-encoding detection (F-G), with NO LTR regression.
**Verified:** 2026-05-29
**Status:** PASSED
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | extract_pdf_pages drives every PDF page through page.get_text('rawdict') with image data disabled | VERIFIED | `_RAWDICT_FLAGS = fitz.TEXTFLAGS_RAWDICT & ~fitz.TEXT_PRESERVE_IMAGES` at local_indexer.py:104; `page.get_text("rawdict", flags=_RAWDICT_FLAGS)` at _extract_one_page_rawdict:1120 |
| 2 | Glyphs flattened preserving ORIGINAL rawdict reading order, annotated with span_id + original_order (no destructive x-sort) | VERIFIED | local_indexer.py:1155-1166: `span_id = sp_id`, `original_order = glyph_order` annotated while flattening, `glyph_order` monotonically incremented, no x-sort |
| 3 | Lines grouped PER ORIGINAL rawdict block; block rows join with \n; blocks join with \n\n (M2) | VERIFIED | local_indexer.py:1133-1194: `for blk in page_dict.get("blocks", []):` iterates per-block; `group_lines_by_baseline(block_lines)` per block; `block_texts.append("\n".join(row_texts))`; `"\n\n".join(...)` at 1194 |
| 4 | RTL lines de-spaced then reordered; LTR lines pass through rawdict's natural text untouched | VERIFIED | local_indexer.py:1172-1188: RTL branch calls `despace_line_to_word_units` → `reorder_word_units_rtl` → `fix_visual_brackets_rtl` → `normalize_punctuation_spacing`; LTR branch at 1176-1179 emits `line_text_raw.strip()` unchanged |
| 5 | extract_pdf_pages yields NIKUD-BEARING reconstructed text — no strip in extract_pdf_pages; strip happens once in _write_page_doc for ALL formats (D-06 FINAL) | VERIFIED | No `strip_nikud` call in `extract_pdf_pages` or `_extract_one_page_rawdict`; module-top grep shows 0 `from genizah_core import strip_nikud` at module level; lazy import only inside `_write_page_doc` at local_indexer.py:2811 |
| 6 | A page where rawdict loses/scrambles vs blocks output falls back to blocks for THAT page (no LTR regression) | VERIFIED | `_ltr_damage_guard` at local_indexer.py:941; called at 1198; checks empty output, token-count ratio < 0.70, and Jaccard < 0.50 on LTR pages |
| 7 | Per-page corrupt + multicolumn flags computed and surfaced via page_flags BEFORE any file-level write decision (detect-before-write) | VERIFIED | local_indexer.py:1055-1073: `page_flags[page_num] = {"corrupt": ..., "multicolumn": ...}` recorded for every yielded page when page_flags dict supplied; `_extract_and_write_pdf` at 2887 passes `page_flags={}` to buffer phase |
| 8 | extract_pdf_pages still yields the frozen (page_num, text, title) 3-tuple | VERIFIED | local_indexer.py:1079: `yield page_num, final_text, title` |
| 9 | Nikud stripped ONCE inside _write_page_doc for ALL LOCAL formats; stripped value written to BOTH content and cached_text (content == cached_text == stripped) | VERIFIED | local_indexer.py:2811-2847: function-local `from genizah_core import strip_nikud`; `stripped = strip_nikud(text)`; `content=[stripped]` at 2822; `compress_cached_text(stripped)` at 2842; `words = stripped.split()` at 2815 |
| 10 | strip_nikud is lazy-imported (function-local) inside _write_page_doc; shared/local_indexer.py stays free of a module-top genizah_core import (L1) | VERIFIED | grep confirms 0 module-top `from genizah_core import` lines; lazy import only at local_indexer.py:2811 inside `_write_page_doc` body |
| 11 | Pages written by the new pipeline carry extraction_format_version = 2 | VERIFIED | local_indexer.py:2847: `VALUES (?, ?, ?, ?, 'zstd', ?, 2, ?)` — literal `2` in INSERT for extraction_format_version |
| 12 | Corrupt pages NOT written to the index; file-level corrupt decision happens BEFORE any _write_page_doc call (buffer-then-decide) | VERIFIED | local_indexer.py:2886-2908: pages buffered first; corrupt decision at 2900-2908 returns `(0, "corrupt_encoding", ...)` before write loop begins |
| 13 | Cancellation DURING the BUFFER phase calls _rollback_partial(sys_id) and leaves no pre-inserted processed_files/local_files rows | VERIFIED | local_indexer.py:2888-2895: `if cancel_check(): self._rollback_partial(sys_id); return (0, "cancelled", display_title)` inside the buffer loop |
| 14 | Cancellation DURING the write loop triggers _rollback_partial and leaves no partial rows (M5) | VERIFIED | local_indexer.py:2913-2917: `if cancel_check(): self._rollback_partial(sys_id); return (pages_written, "cancelled", display_title)` inside write loop |
| 15 | A corrupt_encoding PDF is classified extraction_status = 'corrupt_encoding' (pages_written=0) and counted as an error in all 3 in-file surfaces | VERIFIED | Surface 1 _ERROR_STATUSES_KEPT at local_indexer.py:147-151: `"corrupt_encoding"` present; Surface 2 scan classification at 2298: `if status in ("ok", "no_text_layer", "encoding_error", "unsupported")` — corrupt_encoding is NOT in tuple so falls to errors; Surface 3 folder counter SQL at 3298-3301: `'corrupt_encoding'` in error subquery |
| 16 | The pre-existing 'encoding_error' status remains in the indexed bucket (no behavior change to legacy statuses) | VERIFIED | local_indexer.py:2298: `("ok", "no_text_layer", "encoding_error", "unsupported")` — `"encoding_error"` still present in the indexed-bucket tuple |
| 17 | corrupt_encoding renders as a red 'Corrupt encoding' label in the My Library tree at all 3 surface points | VERIFIED | desktop/my_library_tab.py:348-349: `_build_leaf_item_status` returns `(pages_str, tr("Corrupt encoding"), '#e74c3c')`; line 490-491: `update_file_status` label branch; line 523: color-paint set `('error', 'encoding_error', 'corrupt_encoding')` |
| 18 | DB migrates 2→3; _LATEST_VERSION = 3; init_sqlite stamps fresh DBs at user_version = 3; corrupt_encoding in _KEPT_STATUSES; no auto-flip (D-10) | VERIFIED | local_indexer_migrations.py:37: `_LATEST_VERSION = 3`; lines 51-63: `"corrupt_encoding"` in `_KEPT_STATUSES`; lines 154-162: `_migrate_2_to_3` no-op DDL; line 169: `2: _migrate_2_to_3` registered; local_indexer.py:889: `conn.execute("PRAGMA user_version = 3")` |

**Score:** 18/18 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/local_indexer_rtl.py` | Pure glyph-trace transform helpers (>200 lines), no fitz import | VERIFIED | 459 lines; exports rtl_ratio, group_lines_by_baseline, despace_line_to_word_units, reorder_word_units_rtl, fix_visual_brackets_rtl, normalize_punctuation_spacing, line_text_from_word_units; no `import fitz` or `import pymupdf` |
| `tests/test_local_pdf_rtl_helpers.py` | Unit tests over glyph-trace fixtures | VERIFIED | 27 test functions covering despace, reorder, bracket, punctuation, M3, LTR passthrough |
| `tests/fixtures/local_indexer/glyph_traces/*.json` | 7 fixture files with richer glyph records | VERIFIED | All 7 present: letter_spaced_line, letter_spaced_reversed_line, rtl_running_header, ltr_latin_line, undersplit_line, overmerge_line, intra_word_visual_ltr |
| `shared/local_indexer.py` | Rewritten rawdict extract_pdf_pages + _detect_corrupt_encoding + _ltr_damage_guard | VERIFIED | `extract_pdf_pages` calls rawdict primary; `_detect_corrupt_encoding` at :541; `_ltr_damage_guard` at :941; `_extract_one_page_rawdict` at :1113; `_write_page_doc` with lazy strip_nikud at :2811 |
| `shared/local_indexer_migrations.py` | _migrate_2_to_3 + _LATEST_VERSION=3 + corrupt_encoding in _KEPT_STATUSES | VERIFIED | _LATEST_VERSION=3 at :37; _migrate_2_to_3 at :154; registered at :169; corrupt_encoding in _KEPT_STATUSES at :55 |
| `desktop/my_library_tab.py` | corrupt_encoding branch in _build_leaf_item_status + update_file_status + color paint | VERIFIED | 3 surface points at lines 348, 490, 523 |
| `tests/test_local_pdf_nikud_strip.py` | Tests asserting all-format strip + version=2 + un-vocalized query match | VERIFIED | 5 test functions present |
| `tests/test_local_pdf_corrupt_status.py` | Tests for buffer-then-decide + cancel proofs + surface coverage | VERIFIED | 8 test functions including cancel-during-buffering row-cleanup proof |
| `tests/test_local_pdf_rawdict_extraction.py` | Rawdict extraction tests including page_flags contract | VERIFIED | Tests rawdict as primary path, 3-tuple contract, page_flags detection |
| `tests/test_local_indexer_migration_2_to_3.py` | Migration ladder + prune-protection tests | VERIFIED | 8 test functions covering fresh-stamp, ladder, prune-protection, idempotence |
| `tests/test_my_library_corrupt_status_label.py` | Desktop tree label tests | VERIFIED | 6 test functions for all 3 surface points |
| `tests/fixtures/local_indexer/letter_spaced_hebrew.pdf` | Letter-spaced Hebrew fixture | VERIFIED | File exists |
| `tests/fixtures/local_indexer/ltr_latin_noregress.pdf` | LTR no-regression fixture | VERIFIED | File exists |
| `tests/fixtures/local_indexer/corrupt_encoding_sample.pdf` | Corrupt-encoding fixture | VERIFIED | File exists |
| `tests/test_local_pdf_extraction_e2e.py` | End-to-end tests for all failure modes | VERIFIED | 7 test functions: test_letter_spaced_defragments, test_letter_spaced_reversed_reads_correctly, test_rtl_running_header, test_ltr_no_regression, test_corrupt_encoding_status, test_single_word_per_line_guard_still_passes, test_nikud_strip_e2e |
| `docs/OPEN_ISSUES.md` | D-F13/D-F14/D-F16 marked closed; SEED-004 added as deferred | VERIFIED | D-F13 "Fixed (2026-05-29, Phase 102)"; D-F14 "Addressed (2026-05-29, Phase 102...)"; D-F16 "Addressed (2026-05-29, Phase 102)"; SEED-004 "Deferred (2026-05-29, Phase 102)" |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `extract_pdf_pages` | `despace_line_to_word_units / reorder_word_units_rtl` | per-RTL-line de-space then reorder (D-05 ordering) | VERIFIED | local_indexer.py:1182-1183 |
| `extract_pdf_pages span flatten` | richer glyph records {c,bbox,font,size,span_id,original_order} | annotate span_id + original_order while flattening | VERIFIED | local_indexer.py:1155-1166 |
| `extract_pdf_pages per-block grouping` | block-boundary-preserving page text | `"\n\n"` join in _extract_one_page_rawdict | VERIFIED | local_indexer.py:1194 |
| `_write_page_doc` | content / cached_text (strip ALL formats) | lazy strip_nikud → stripped; content=[stripped] AND compress_cached_text(stripped) | VERIFIED | local_indexer.py:2811-2842 |
| `_extract_and_write_pdf` | extract_pdf_pages page_flags + cancel_check in BOTH buffer phase and write loop | `page_flags={}` passed; cancel_check in buffer (2888); cancel_check in write (2914) | VERIFIED | local_indexer.py:2886-2917 |
| `buffer-phase cancel` | `_rollback_partial(sys_id)` | rollback called before returning cancelled from buffer loop | VERIFIED | local_indexer.py:2894 |
| `extraction_status 'corrupt_encoding'` | _ERROR_STATUSES_KEPT + scan classification + folder counter SQL | 3 in-file surfaces updated; 4th surface = desktop tree (Plan 04) | VERIFIED | local_indexer.py:150, 2298, 3300 |
| `_build_leaf_item_status` | tree label/color | `corrupt_encoding` → `(pages_str, tr("Corrupt encoding"), '#e74c3c')` | VERIFIED | my_library_tab.py:348-349 |
| `_migrate_2_to_3` | _MIGRATIONS registry + _LATEST_VERSION + init_sqlite stamp | registered at key 2; _LATEST_VERSION=3; init_sqlite stamps fresh==3 | VERIFIED | local_indexer_migrations.py:37,169; local_indexer.py:889 |
| `normalize_punctuation_spacing` | Hebrew sof-pasuq (U+05C3 ׃) / maqaf (U+05BE ־) + ASCII punctuation | `_PUNCT_SPACING_RE = re.compile(r"\s+([.,;:!?)־׃])")` | VERIFIED | local_indexer_rtl.py:449 — regex includes Hebrew codepoints |
| `despace_line_to_word_units` | `reorder_word_units_rtl` | word-unit bbox-union list carrying original_order (de-space BEFORE reorder, D-05) | VERIFIED | local_indexer_rtl.py:250-352; units returned sorted by original_order for downstream reorder |
| `rtl_ratio` | every RTL transform helper | LTR pass-through guard `rtl_ratio(text) <= 0.4` at top of every helper | VERIFIED | local_indexer_rtl.py:273, 382, 432 |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|-------------------|--------|
| `_write_page_doc` → Tantivy | `stripped` | `strip_nikud(text)` where text comes from the real PDF/DOCX/TXT extractors | Yes — strip_nikud is a transform of the real extracted text | FLOWING |
| `extract_pdf_pages` yields | `final_text` | `_extract_one_page_rawdict(page, ...)` → real PyMuPDF rawdict parse | Yes — page.get_text("rawdict") from actual PDF bytes | FLOWING |
| `_detect_corrupt_encoding` | `garbage_ratio`, `wordlike_ratio` | codepoint scan over the real extracted text | Yes — real ratios from real text | FLOWING |
| `_ltr_damage_guard` | `rawdict_text`, `blocks_text` | both come from real per-page extraction calls | Yes — Jaccard/token-count over real tokens | FLOWING |

### Behavioral Spot-Checks

The orchestrator context confirms 317 tests passed / 3 skipped / 3 xfailed for Phase 102 + adjacent local-indexer regression suite. The full `pytest tests/` run cannot complete on Windows due to a pre-existing native access violation in genizah_core._build_fl_id_index daemon threads (unrelated to Phase 102; genizah_core untouched; logged in docs/OPEN_ISSUES.md).

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| RTL helper functions exported correctly | grep for all 7 exports in local_indexer_rtl.py | All 7 functions found: rtl_ratio, group_lines_by_baseline, despace_line_to_word_units, line_text_from_word_units, reorder_word_units_rtl, fix_visual_brackets_rtl, normalize_punctuation_spacing | PASS |
| No fitz import in helper module | grep import fitz in local_indexer_rtl.py | Not found — comment at line 5 confirms intent; module is pure | PASS |
| No module-top genizah_core import | grep module-top import in local_indexer.py | Count = 0; lazy import only inside _write_page_doc | PASS |
| extraction_format_version = 2 in INSERT | grep VALUES tuple in _write_page_doc | `'zstd', ?, 2, ?` at local_indexer.py:2847 | PASS |
| corrupt_encoding in all 4 D-08 surfaces | grep corrupt_encoding across 2 files | _ERROR_STATUSES_KEPT, scan classification (absence from indexed tuple), folder SQL, desktop tree — all 4 confirmed | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| D-F13 | 102-05 | Letter-spaced Hebrew de-collapse (dominant bug — 46% single-letter tokens in some books) | SATISFIED | rawdict + despace_line_to_word_units; test_letter_spaced_defragments; OPEN_ISSUES.md marked Fixed |
| D-F14 | 102-01, 102-02 | Adopt Meiri reorder core, RTL-gated (NOT wholesale) | SATISFIED | reorder_word_units_rtl adapts Meiri _normalize_span_dir; gated by rtl_ratio; OPEN_ISSUES.md marked Addressed |
| D-F16 | 102-02, 102-03, 102-04 | Corrupt-encoding detection — new corrupt_encoding status | SATISFIED | _detect_corrupt_encoding; buffer-then-decide; 4-surface D-08 wiring; OPEN_ISSUES.md marked Addressed |
| F-A (digit run handling) | 102-01 | Digit-only unit runs re-reversed to preserve LTR number order | SATISFIED | reorder_word_units_rtl digit-run reversal at local_indexer_rtl.py:405-420 |
| F-B (punctuation spacing) | 102-01 | Collapse spurious space before ASCII + Hebrew sof-pasuq/maqaf | SATISFIED | normalize_punctuation_spacing; regex includes ׃ and ־ codepoints at :449 |
| F-C (reversed brackets) | 102-01 | Mirror reversed paren pairs back to logical order | SATISFIED | fix_visual_brackets_rtl; _BRACKET_PAIRS copied verbatim from Meiri |
| F-D (letter-spacing de-collapse) | 102-01, 102-02 | 1.8x-median adaptive de-space with hysteresis | SATISFIED | despace_line_to_word_units; _HARD_GAP_MULT=1.8; hysteresis via span_id/font boundary |
| F-E (order-reversed + letter-spaced) | 102-01, 102-02 | De-space then reorder restores correct reading order | SATISFIED | D-05 sequencing enforced; letter_spaced_reversed fixture tests this |
| F-F (running header reversal) | 102-01 | RTL reorder corrects reversed running headers | SATISFIED | reorder_word_units_rtl; rtl_running_header fixture; test_rtl_running_header e2e |
| F-G (corrupt encoding) | 102-02, 102-03 | Detect garbage bytes, status corrupt_encoding, no garbage indexed | SATISFIED | _detect_corrupt_encoding; buffer-then-decide; pages_written=0 |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `shared/local_indexer.py` | 103-106 | `_RAWDICT_FLAGS` AttributeError fallback yields `0` (no flags), comment says TEXTFLAGS_DICT | Info | Cosmetic — comment misleads; actual behavior (flags=0) is acceptable; documented in 102-REVIEW.md IN-05 |
| `shared/local_indexer.py` | 2900-2908 | corrupt-decision denominator divides by `len(buffered)` but numerator only counts pages with flag entries | Warning | Low-probability edge: a page where flag collection raises silently counts as non-corrupt; documented in 102-REVIEW.md WR-02 |
| `shared/local_indexer.py` | 1043 vs 1058 | `page.get_text("rawdict")` called twice when page_flags is supplied (once in _extract_one_page_rawdict, once for multicolumn x-ranges) | Warning | Performance: every production PDF page parsed twice; documented in 102-REVIEW.md WR-03; correctness risk is low |
| `shared/local_indexer.py` | ~3071-3090 | `_rollback_partial` calls `writer.rollback()` discarding sibling files' uncommitted Tantivy docs (pre-existing behavior; Phase 102 made it more visible) | Warning | Bounded: sibling files get re-indexed on next scan; pre-existing pattern; documented in 102-REVIEW.md WR-01 |

None of the above are blockers for the phase goal. All three Warnings are documented in 102-REVIEW.md and are pre-existing patterns or low-probability edge cases.

### Human Verification Required

None. All critical behaviors are verifiable programmatically and were verified by the orchestrator-reported test run (317 passed / 3 skipped / 3 xfailed). The LTR no-regression contract is pinned by `test_ltr_no_regression` with a token-count-within-band check.

## Gaps Summary

No gaps found. All 18 must-haves are fully verified against the actual codebase:

1. The rawdict-primary extraction path is wired and substantive (not a stub).
2. The RTL helper module is fitz-free and independently unit-testable on committed glyph-trace fixtures.
3. The D-06 all-format nikud strip is applied once at _write_page_doc via a lazy import; content == cached_text == stripped; extraction_format_version = 2.
4. The buffer-then-decide corrupt-encoding flow is fully wired: per-page flags collected, ≥50% file-level decision before any _write_page_doc call, both buffer-phase cancel and write-loop cancel call _rollback_partial.
5. All 4 D-08 status surfaces are updated (_ERROR_STATUSES_KEPT, scan classification, folder counter SQL, desktop tree label/color).
6. The migration ladder reaches user_version = 3; init_sqlite stamps fresh DBs at 3; no auto-flip (D-10).
7. The OPEN_ISSUES.md tracker is updated: D-F13 fixed, D-F14/D-F16 addressed, SEED-004 deferred.
8. All Phase 102 fixture PDFs (letter_spaced_hebrew.pdf, ltr_latin_noregress.pdf, corrupt_encoding_sample.pdf) and glyph-trace JSON fixtures are committed.
9. The frozen (page_num, text, title) 3-tuple caller contract is preserved.
10. No duplicate function definitions in shared/local_indexer.py for extract_pdf_pages (single definition at line 1001).

The three Warnings in 102-REVIEW.md (WR-01 writer.rollback() sibling doc loss, WR-02 corrupt-decision denominator edge, WR-03 double-rawdict-parse) are non-blocking: they are documented deviations that do not prevent the phase goal (correct RTL de-space, searchable indexed text, corrupt detection, LTR no-regression).

---

_Verified: 2026-05-29T12:00:00Z_
_Verifier: Claude (gsd-verifier)_
