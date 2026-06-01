---
phase: 102
reviewers: [codex]
reviewed_at: 2026-05-29T10:20:08Z
plans_reviewed: [102-01-PLAN.md, 102-02-PLAN.md, 102-03-PLAN.md, 102-04-PLAN.md, 102-05-PLAN.md]
verdict: HIGH risk — do not execute as-is; replan recommended
---

# Cross-AI Plan Review — Phase 102

> Reviewer: **Codex** (`codex exec`, default model, read the live codebase + installed PyMuPDF).
> Single-reviewer run (`--codex`). We run inside Claude Code, so the `claude` CLI was skipped for
> independence. Codex's sandbox could not run `pytest` (its venv launchers failed — environmental,
> not a plan defect; see Concern about broken Python env).

## Codex Review

**Summary**

The plan set is close on symbol names and most line references, but Codex would not execute it as-is.
The highest-risk drift is architectural: D-06 assumes `cached_text` drives display/browse, but live
LOCAL search and browse read the stored Tantivy `content` field. Also, the proposed `corrupt_encoding`
flow detects corruption only after pages have already been written to Tantivy, so it can still index
garbage.

**Plan↔Code Drift**

- **PyMuPDF rawdict flags: OK.** `.venv` has PyMuPDF `1.27.2.3`; source confirms `TEXT_PRESERVE_IMAGES`,
  `TEXTFLAGS_DICT`, and `TEXTFLAGS_RAWDICT` exist, and `Page.get_text(..., flags=...)` is accepted via
  `pymupdf/utils.py:459-553`. `TEXTFLAGS_RAWDICT = TEXTFLAGS_DICT` at `pymupdf/__init__.py:17631-17639`.
  Fix: prefer `fitz.TEXTFLAGS_RAWDICT & ~fitz.TEXT_PRESERVE_IMAGES` for clarity, though the plan's
  `TEXTFLAGS_DICT` expression is valid.

- **`_attach_nikud_page`: OK.** It exists with the assumed callable shape:
  `ephraim_meiri_pdf_converter/pdf_to_docx.py:791` defines
  `def _attach_nikud_page(page_dict: dict, x_tol: float = 4.0, y_tol: float = 6.0) -> None`.

- **D-06 display-text assumption is FALSE in live code.** Plan 03 says `cached_text` retains nikud for
  display/page browsing, but live LOCAL result/browse paths use Tantivy stored `content`:
  `genizah_core.py:7166` reads `content` for LOCAL results, and `genizah_core.py:9631` reads `content`
  for `get_local_browse_page`. If `_write_page_doc` changes `content=[strip_nikud(text)]`, display/browse
  will show stripped text unless more code changes are planned. Fix: either add a stored display field to
  the LOCAL Tantivy schema, or make LOCAL result/browse load `local_pages.cached_text` by UID.

- **Corrupt PDFs would still be indexed.** Live `_extract_and_write_pdf` writes each page inside the loop
  at `shared/local_indexer.py:2507`, calling `_write_page_doc` at lines `2515-2518`. Plan 03 only checks
  majority-corrupt after the loop, so garbage pages are already in Tantivy/local_pages. Fix: do not write
  corrupt pages, or buffer/first-pass pages and only write after file-level classification; if majority
  corrupt, rollback partial writes and return `pages_written=0, status='corrupt_encoding'`.

- **D-06 also misses atomic rebuild.** `rebuild_main_index_atomic` decompresses `cached_text` and writes
  it back to Tantivy as `content=[text]` at `shared/local_indexer.py:3052` and `:3072`. After a rebuild,
  vocalized cached text would be indexed with nikud again. Fix: apply `strip_nikud` there too, and derive
  `content_head/tail` from the stripped copy.

- **Fresh DB migration stamp drift.** Plan 04 bumps `_LATEST_VERSION` to 3, but `init_sqlite` still stamps
  fresh DBs as `PRAGMA user_version = 2` at `shared/local_indexer.py:736`. Existing migration tests expect
  fresh DBs to equal `_LATEST_VERSION`. Fix: update `init_sqlite` comments and stamp to 3, plus tests
  currently asserting version 2.

- **Existing fallback tests will break.** `tests/test_local_pdf_extraction_fallback.py` asserts the primary
  path calls `get_text("blocks")` and that sort fallback behavior is live. Phase 102 intentionally replaces
  that with rawdict primary. Fix: update this existing test module in Plan 02, not only add new tests.

- **D-08 status surfaces: mostly accurate, exact live locations are:**
  - `_ERROR_STATUSES_KEPT`: `shared/local_indexer.py:126`
  - scan classification tuple: `shared/local_indexer.py:1951`, currently includes `"encoding_error"`
  - folder `error_count` SQL: `shared/local_indexer.py:2876`, status list at `2879-2881`
  - tree static label: `desktop/my_library_tab.py:333`
  - **live-update label/color also need updates at `desktop/my_library_tab.py:486` and `:519`** (NOT in the plan)
  - migration `_KEPT_STATUSES`: `shared/local_indexer_migrations.py:47` (NOT in the plan)

- **`strip_nikud`: OK, but note behavior.** It exists at `genizah_core.py:199`; pattern is `[֑-׏]`
  at line `157`, which strips MORE than just U+05B0..U+05C7 vowel marks (includes cantillation/te'amim and
  more). Relevant to the D-04 "exclude nikud from gap math" range, which uses a narrower 0x05B0-0x05C7.

**Concerns**

- **HIGH:** Plan 01/02 glyph-order contract is underspecified. Plan 02 says flatten glyphs "in x order";
  Meiri's `_normalize_span_dir` relies on original rawdict char order plus x-direction jumps. Sorting
  glyphs up front can destroy the very signal needed for segment detection and can reverse RTL word letters.

- **HIGH:** `despace_line_to_word_units(line_glyphs: list[dict])` lacks span/font metadata, but the
  algorithm requires font/span-boundary hints. Either pass richer glyph records or annotate glyphs while
  flattening spans.

- **MEDIUM:** Plan 02 groups all page lines by baseline, which risks losing block/paragraph boundaries.
  Current extractor preserves block separation via `"\n\n"`.

- **MEDIUM:** Plan 04's proposed migration test inserts `corrupt_encoding` after running migrations, then
  re-runs no-op migration. That does not prove `_KEPT_STATUSES` protects rows during the 1→2 prune. Seed
  before 1→2 or assert `_KEPT_STATUSES` directly.

- **MEDIUM:** The local Python environments in Codex's sandbox are broken (`python`, `py`, `.venv`, and
  `venv` launchers fail), so the plan's `python -m pytest ...` verification was not executable in that
  sandbox. (Environmental, not a plan defect — our own toolchain runs pytest fine.)

**Suggestions**

1. Add a small design patch before execution for D-06 display: decide between `content_index + content_display`
   fields vs SQLite cached-text lookup for display/browse.
2. Change corrupt flow so detection happens before `_write_page_doc`, or rollback partial writes before
   returning `corrupt_encoding`.
3. Update `rebuild_main_index_atomic` with the same stripped-index/display-text split.
4. Make Plan 02 update `tests/test_local_pdf_extraction_fallback.py`.
5. Define a richer internal glyph unit: `{c, bbox, font, size, span_id, original_order}`.

**Risk Assessment**

**HIGH** until the D-06 display/rebuild path and corrupt-indexing flow are fixed. The low-level references
are mostly sound, but the current plans can silently strip display text and still index corrupt garbage,
which are direct violations of the phase goals.

---

## Consensus Summary

Single reviewer (Codex), so "consensus" = Codex's findings. The review is unusually high-value because
Codex read the live source and the installed PyMuPDF, catching drift the internal `gsd-plan-checker`
(which reasons from the plans + cited refs) structurally cannot.

### Confirmed sound (no action)
- PyMuPDF `TEXTFLAGS_*` / `TEXT_PRESERVE_IMAGES` flag names and `get_text(flags=...)` (D-11) — valid.
- `_attach_nikud_page` exists with the assumed signature.
- `strip_nikud` exists at `genizah_core.py:199` (note: its `[֑-׏]` range is broader than the
  D-04 nikud-gap-math range — intentional for index normalization, but worth a conscious note).

### Agreed Concerns (highest priority — must address before execution)
1. **[HIGH / blocker-class] D-06 display path is wrong.** LOCAL result + browse read Tantivy `content`
   (`genizah_core.py:7166`, `:9631`), not `cached_text`. Stripping `content` makes the user-visible LOCAL
   text consonantal-only — the exact regression D-06 was reversed (Codex HIGH-2) to avoid. The plan needs a
   display field or a cached_text-by-UID display lookup. **This invalidates the core premise of Plan 03.**
2. **[HIGH / blocker-class] Corrupt pages indexed before classification.** `_extract_and_write_pdf` writes
   per-page in the loop; majority-corrupt is checked only after. Garbage lands in the index. Needs
   detect-before-write or rollback-on-corrupt.
3. **[HIGH] D-06 atomic-rebuild gap.** `rebuild_main_index_atomic` (`:3052`, `:3072`) re-indexes cached_text
   as `content` — nikud reappears post-rebuild. Same strip must apply there. Plan 03 missed this site.
4. **[HIGH] Glyph-order contract conflict.** "Flatten in x order" (Plan 02) destroys the original-order +
   x-jump signal Meiri's reorder depends on, and can reverse RTL letters. The de-space/reorder glyph
   contract (Plan 01↔02) must preserve original order + carry span/font/order metadata.
5. **[HIGH] de-space lacks span/font metadata** that its own hysteresis (D-04 mid-gap corroboration)
   requires. Glyph records need `{c, bbox, font, size, span_id, original_order}`.
6. **[MEDIUM] Missing edit sites:** existing `tests/test_local_pdf_extraction_fallback.py` (asserts blocks
   primary) will break and must be updated in Plan 02; two more D-08 status surfaces (`my_library_tab.py:486`,
   `:519`) and the migration `_KEPT_STATUSES` (`local_indexer_migrations.py:47`) aren't covered.
7. **[MEDIUM] init_sqlite stamp** still writes `user_version = 2` (`:736`) — Plan 04's 3-bump leaves fresh
   DBs + version tests inconsistent.
8. **[MEDIUM] Plan 04 migration test** doesn't actually exercise the 1→2 prune-protection it claims.

### Divergent Views
None (single reviewer).

---

## Recommended Action

The two HIGH items #1 (D-06 display reads `content`) and #2 (corrupt pages written before classification)
are correctness defects that defeat stated phase goals — they should be fixed before execution, not during.
Items #4/#5 (glyph-order + metadata contract) sharpen Plan 01↔02 and reduce execution risk.

Incorporate into the plans:

```
/gsd-plan-phase 102 --reviews
```

This re-runs the planner in reviews mode against this REVIEWS.md, then re-verifies via gsd-plan-checker.
