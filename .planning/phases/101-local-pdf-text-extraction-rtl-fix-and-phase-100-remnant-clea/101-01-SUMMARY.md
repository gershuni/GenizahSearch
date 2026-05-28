---
phase: 101-local-pdf-text-extraction-rtl-fix-and-phase-100-remnant-clea
plan: 01
status: complete
completed: 2026-05-28
requirements:
  - D-01
  - D-03
  - D-04
  - D-05
  - D-06
  - D-09
---

## Plan 101-01 — LOCAL PDF RTL text-extraction fix + extractor-version self-heal + D-09 conftest fix

### What was built

**RTL fix (USER-DEC-1 / S-1 directional-run reversal, REVISED per REVIEWS round 2 Codex HIGH #5).** Two LIVE helpers in `shared/local_indexer.py`:

- `_fix_sort_true_rtl_line(line)` — groups consecutive tokens by predominant directionality (RTL vs non-RTL via per-token `_rtl_ratio > 0.4`), reverses the SEQUENCE of runs, AND reverses tokens INSIDE each RTL run, while preserving the internal order of non-RTL runs. Pure-Hebrew lines (single RTL run) flip word order; embedded Latin shelfmarks like `T-S 12.123`, folio numbers, and inline sigla stay adjacent in their natural left-to-right sub-order. Gated on `_rtl_ratio(line) <= 0.4` (no-op on LTR/numeric/empty/mixed-LTR). Letters within each word are NOT touched (we reverse TOKENS, not characters — `python-bidi`'s `get_display` would corrupt already-correct Hebrew letters).
- `_fix_sort_true_rtl_page(text)` — applies `_fix_sort_true_rtl_line` per line, joins with `'\n'` (canonical PyMuPDF line terminator on all platforms, REV-2f).

Wired into `extract_pdf_pages` ONLY in the `sort=True` fallback branch (line replaces `text = fallback_text` after `_detect_single_word_per_line(text)` fires). The primary `get_text("blocks")` path is UNTOUCHED, so correctly-ordered RTL PDFs are not re-reversed (verified by `test_extract_pdf_pages_blocks_path_untouched`).

**Extractor-version self-heal (USER-DEC-2 / Codex HIGH, REVISED per REVIEWS round 2 MEDIUM #3 + #7).** Added in `shared/local_indexer.py`:

- `_CURRENT_EXTRACTOR_VERSION = "2"` constant + `.extractor_version` marker file in index_dir + `_read_extractor_version` / `_write_extractor_version` helpers mirroring the `_read_schema_marker` / `_write_schema_marker` pattern.
- In `LocalIndexer.__init__`, AFTER the schema-marker block, a version-mismatch UPDATE runs inside `with self._conn: self._conn.execute("BEGIN IMMEDIATE")` and flips ONLY `status='committed'` PDF rows to `'pending'`:
  - `LOWER(COALESCE(file_extension, '')) = '.pdf'` defends against NULL extensions.
  - `AND status = 'committed'` ensures error/failed/skipped/already-pending rows are NOT revived (Codex HIGH consensus from round 1).
  - `cur.rowcount` (NOT a post-UPDATE `SELECT COUNT(*)`) drives the log count, so it reports rows actually flipped by THIS UPDATE rather than inflating when any PDF was already pending from an interrupted scan (round-2 MEDIUM #3).
  - The `_write_extractor_version` marker is written AFTER the `with self._conn:` block commits, so a crash between SQLite commit and FS write yields an idempotent repeat on the next launch — not a stale-marker / unmarked-rows split (round-2 MEDIUM #7).

**D-09 batch-order flake fix (USER-DEC-3 / REVIEWS round 2 HIGH #2).** Appended an autouse fixture `_refresh_local_indexer_for_local_indexer_tests` to `tests/conftest.py`. Scoped to `tests/test_local_indexer.py` via `request.node.fspath` check. Two steps — reload alone is NOT enough:

1. `importlib.reload(shared.local_indexer)` rebuilds the module's `__dict__` with new objects.
2. `setattr(request.module, name, ...)` for `LocalIndexer`, `EncodingError`, `extract_txt` rebinds the test module's stale imported aliases so `pytest.raises(EncodingError)` at the test site references the same class the reloaded `extract_txt` now raises.

`tests/test_mupdf_warnings_suppressed.py` is unchanged (its `importlib.reload` calls are intentional coverage). No local `from shared.local_indexer import …` was added inside `test_txt_undecodable_marked_encoding_error`.

### Wave 0 tests added (27 total)

- `tests/test_local_pdf_extraction_fallback.py` (+312 lines, 14 tests):
  - 4 S-1 directional-run cases (pure-RTL flip, shelfmark adjacency, digit interleave, LTR no-op).
  - 2 boundary-ratio cases (tuned to `0.35 < r < 0.40` and `0.40 < r < 0.50` so future `_rtl_ratio` tokenization drift catches the threshold gate — Gemini MEDIUM #9 tightening).
  - 2 branch-integration cases via `_FakePdfPage` / `_FakePdfDocument` proving the fallback branch CALLS the helper and the blocks path does NOT (Codex MEDIUM round-1 → round-2 HIGH #6).
  - 1 D-06 real-Hebrew-fixture test that skips when `hebrew_rtl_fixture.pdf` is absent and runs when committed.
  - 1 S-8 known-limitation guard (`@pytest.mark.xfail(strict=True)` — Codex LOW #10: strict so an XPASS forces re-review).
- `tests/test_local_indexer.py` (+116 lines, 2 extractor-version tests):
  - `test_extractor_version_bumps_only_committed_pdfs` — seeds 5 rows (committed PDF, error PDF, failed PDF, pending PDF, committed TXT), forces stale marker, re-inits, asserts ONLY the committed PDF flipped.
  - `test_extractor_version_fresh_install_writes_marker` — fresh-install marker write on empty processed_files.
- `tests/test_format_rtl_invariant.py` (+56 lines):
  - Extended `for forbidden in (...)` tuple to include `_fix_sort_true_rtl_line` / `_fix_sort_true_rtl_page` and narrowed docstring to "PDF sort=True RTL helpers must not be reused in structured extractors" (REV-2h).
  - New positive AST assertion `test_sort_true_rtl_helpers_only_called_from_extract_pdf_pages` requiring `extract_pdf_pages` to be among callers (proves wiring) AND no callers outside `{extract_pdf_pages, _fix_sort_true_rtl_line, _fix_sort_true_rtl_page}` (proves no rogue re-use) — Gemini LOW #12 strengthening.
- `tests/fixtures/local_indexer/README.md` — provenance/copyright note for `hebrew_rtl_fixture.pdf` (inbound asset, user supplies later; the real-fixture test skips until present).

### Verification

- `python -m pytest tests/test_local_pdf_extraction_fallback.py tests/test_local_indexer.py tests/test_format_rtl_invariant.py -x` → 26 passed, 1 xfailed (S-8 strict guard), 0 failed.
- `python -m pytest tests/test_mupdf_warnings_suppressed.py tests/test_local_indexer.py` → 15 passed (suppressed → indexer order).
- `python -m pytest tests/test_local_indexer.py tests/test_mupdf_warnings_suppressed.py` → 15 passed (indexer → suppressed reverse order).
- `python -m pytest tests/test_local_indexer.py::test_txt_undecodable_marked_encoding_error` → 1 passed (isolation).
- `python -m ruff check .` → All checks passed! (4 pre-existing F401s in unrelated tests were cleaned up as a separate hygiene commit per the elevated ruff acceptance criterion — `feedback_pre_release_must_run_ruff.md` / v7.12.0 CI failure precedent).

### Commits (worktree)

1. `c7ae7bf4` — test(101-01): Wave 0 RED tests (RTL + extractor-version)
2. `6b349928` — feat(101-01): S-1 directional-run RTL fix + committed-only extractor-version bump (GREEN)
3. `699ece45` — test(101-01): conftest autouse fixture closes D-09 batch-order flake
4. `f3e87c1e` — chore(101): pre-release ruff hygiene (4 pre-existing F401s)

### What this enables for Wave 2

Plan 101-02 Task 3 can safely flip the RTL row in `docs/OPEN_ISSUES.md` to `✅ Fixed (2026-05-28)` — the belt-and-suspenders gate (REVIEWS round 2 Codex LOW #11) requiring BOTH `101-01-SUMMARY.md` to exist AND `grep -q '_fix_sort_true_rtl_page' shared/local_indexer.py` to succeed is now satisfied: both this SUMMARY and the helper exist.

### Decisions honored

- D-01/D-02 VOIDED: NO `python-bidi` / `bidi` dependency added to `requirements.txt` or `GenizahSearchPro.spec`. Pure-Python fix.
- USER-DEC-1: S-1 directional-run reversal, REVISED with within-RTL-run token reversal so pure-RTL lines actually flip word order.
- USER-DEC-2: `AND status='committed'` + `BEGIN IMMEDIATE` + `COALESCE` + `cur.rowcount` + marker-after-commit.
- USER-DEC-3: conftest-level autouse fixture with `setattr(request.module, ...)` rebind (NOT per-test local import).

### Deferred

- D-F2 (PDF OCR for image-only PDFs) remains explicitly deferred to a future phase — `_detect_single_word_per_line` heuristic fallback covers most cases but image-only PDFs need real OCR.
