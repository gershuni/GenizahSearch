---
plan: 102-01
phase: 102-pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13-
status: complete
completed: 2026-05-29
tasks_total: 3
tasks_done: 3
---

# Plan 102-01 Summary — Pure RTL glyph-trace reconstruction helpers

## What was built

New fitz-free module `shared/local_indexer_rtl.py` — the algorithmic contracts the
Plan 02 `extract_pdf_pages` rewrite consumes. Pure dict transforms (no `fitz`/`pymupdf`
import), independently unit-testable on committed glyph-trace JSON fixtures.

### Task 1 — fixtures + classification + line grouping
- 7 hand-authored glyph-trace JSON fixtures under `tests/fixtures/local_indexer/glyph_traces/`
  carrying the richer `{c, bbox, font, size, span_id, original_order}` glyph contract
  (REVIEWS HIGH-4/HIGH-5), + `README.md` documenting the schema and the M3 center-x
  authoring rule (ם lowest center-x, ש highest → descending-x sort yields "שלום").
- `rtl_ratio`, `_is_nikud`, `_center_x` primitives.
- `group_lines_by_baseline` (D-02): baseline/font-size-derived tolerance from the global
  base (non-nikud) glyph size — a fitz-split nikud row merges back; a superscript
  footnote ref does not merge.

### Task 2 — adaptive de-space → word-unit bbox-unions (D-04/D-05/M3)
- `despace_line_to_word_units`: adaptive **1.8× median** hard break + **hysteresis** mid
  break corroborated by embedded space / punctuation / **span_id or font boundary**
  (HIGH-5) / abnormal-long-token. Nikud excluded from gap math. Word units carry
  bbox-unions + `original_order` (min of members); **no synthetic zero-bbox spaces**
  (HIGH-3/D-05). RTL intra-unit letters ordered by **descending center-x** with each
  nikud kept attached to its base consonant (**M3** — a visual-LTR-emitted "שלום"
  decodes correctly, not "םולש"). LTR lines pass through as whitespace tokens, never
  reordered. Returns units sorted by ascending `original_order`.
- `line_text_from_word_units`.

### Task 3 — reorder + bracket fix + punctuation normalize (F-C/F-B/F-E/F-F)
- `reorder_word_units_rtl`: adapts Meiri `_normalize_span_dir` to word-unit granularity —
  iterates units by `original_order` tracking center-x jumps (`MAX_BACKWARD_JUMP=15.0`),
  sorts segments right-to-left by max center-x, re-reverses embedded digit-only runs
  (F-A). RTL-gated; **no destructive global x-sort** (HIGH-4). Invariant to input-array
  shuffling when `original_order` is fixed.
- `fix_visual_brackets_rtl` (F-C) — string-granularity adaptation of Meiri
  `_fix_visual_brackets`; mirrors a reversed (closer-before-opener) pair.
- `normalize_punctuation_spacing` (F-B) — collapses a space before ASCII `.,;:!?)` AND
  Hebrew sof-pasuq (U+05C3) / maqaf (U+05BE); not ASCII-only.

## Verification
- `python -m pytest tests/test_local_pdf_rtl_helpers.py -q` → **37 passed**.
- `python -m ruff check shared/local_indexer_rtl.py tests/test_local_pdf_rtl_helpers.py` → clean.
- No `import fitz` / `import pymupdf` in the module (pure helpers).

## Key files
- created: `shared/local_indexer_rtl.py`
- created: `tests/test_local_pdf_rtl_helpers.py`
- created: 7× `tests/fixtures/local_indexer/glyph_traces/*.json` + `README.md`

## Deviations / notes
- Executed **inline by the execute-phase orchestrator** after the Wave 1 executor agent
  stalled re-deriving the RTL fixture geometry. Fixtures generated deterministically from
  the worked-out geometry (throwaway generator in `_tmp/`, not committed) to eliminate
  hand-counting error; the JSON files are the committed artifacts.
- Single atomic commit for the plan (cohesive new module) rather than per-task commits;
  `tdd_mode=false` in config so no RED/GREEN gate sequencing was required.

## Self-Check: PASSED
