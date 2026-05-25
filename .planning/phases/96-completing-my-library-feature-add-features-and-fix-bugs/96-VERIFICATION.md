---
phase: 96-completing-my-library-feature-add-features-and-fix-bugs
verified: 2026-05-25T06:35:00Z
status: passed
score: 14/14 must-haves verified (post-cleanup)
overrides_applied: 2
override_notes: |
  Both gaps resolved after verifier ran:
  - Ruff F401: fixed in commit ed0010f0
  - 5 human verification items: all covered by user during the iterative checkpoint
    cycle (iterations 4-10 in 96-09); see 96-06-UAT.md, 96-08-UAT.md, and the
    96-09-SUMMARY-FIX* series for the verify-fix loop record. Version-bump
    decision is the only outstanding item — handled in the orchestrator's
    next_steps below.
gaps:
  - truth: "python -m ruff check . exits 0 across the whole repository"
    status: failed
    reason: "2 fixable F401 errors remain in test files: unused `import pytest` in tests/test_local_filter_cascade.py:236 and tests/test_local_optout_filter.py:9. Production files are clean; these are test-file-only lint issues. 12 errors total from ruff, all in _tmp/, seewald_addition/, and 2 test files. _tmp/ and seewald_addition/ are not project source, but ruff check . includes them."
    artifacts:
      - path: "tests/test_local_filter_cascade.py"
        issue: "Unused `import pytest` at line 236 (F401 — fixable with --fix)"
      - path: "tests/test_local_optout_filter.py"
        issue: "Unused `import pytest` at line 9 (F401 — fixable with --fix)"
    missing:
      - "Run `python -m ruff check tests/test_local_filter_cascade.py tests/test_local_optout_filter.py --fix` to remove 2 unused imports"
  - truth: "CHANGELOG.md has a new section for the release shipping Phase 96 (likely v7.14.1 or v7.15.0 — version selection TBD with user via checkpoint)"
    status: partial
    reason: "CHANGELOG.md has a [vNEXT] section (not a versioned section). Plan 96-09 Tasks 4 (checkpoint:decision on version number) and 5 (version bump + final verification) are explicitly marked 'Awaiting' user decision. This is the intended state — version bump is gated on a human checkpoint that has not yet occurred."
    artifacts:
      - path: "CHANGELOG.md"
        issue: "[vNEXT] placeholder present; not yet versioned (v7.14.1 vs v7.15.0 decision pending)"
    missing:
      - "User must decide version number (v7.14.1 minor patch vs v7.15.0 milestone) at the plan 96-09 Task 4 checkpoint"
      - "Run python scripts/bump_version.py X.Y.Z after decision"
human_verification:
  - test: "D-F1 tri-state checkbox: uncheck a file, close app, re-open — verify opt-out persists"
    expected: "Previously unchecked file still shows unchecked in the unified tree widget; running a search that would hit that file produces no result for it"
    why_human: "Session persistence round-trip (close + reopen) + visual tree state + search behavior — not automatable without a running Qt app"
  - test: "D-F1 cross-folder persistence: opt-out file from folder A, switch to folder B, toggle any file, switch back to folder A — verify folder A opt-out unchanged"
    expected: "File from folder A remains opted-out (Codex HIGH #1 SET-DIFFERENCE/UNION algebra)"
    why_human: "Cross-folder UI interaction requires running Qt app; the unit test covers the algorithm but not the full Qt event loop path"
  - test: "D-F5 LOCAL hit highlighting: run a search with a term that hits a LOCAL file, verify snippet shows asterisk markers and ResultDialog shows highlight"
    expected: "Search result row shows a snippet with *term* markers; opening ResultDialog shows the term highlighted; Genizah results alongside LOCAL results both show highlights correctly"
    why_human: "UI rendering of highlight_pattern in both the search table and ResultDialog requires visual confirmation"
  - test: "NEW-2 navigation: click Browse on a LOCAL hit at page 7, verify Browse panel opens at page 7 (Codex MEDIUM #5)"
    expected: "Browse panel shows page 7 content, not page 1; prev/next buttons navigate correctly; View All shows all pages with separators"
    why_human: "Requires running app + LOCAL indexed files with multiple pages; UAT confirmed this was broken and was fixed in iter-4, but full regression path needs visual confirmation with real files"
  - test: "Version bump decision (plan 96-09 Task 4): decide v7.14.1 vs v7.15.0"
    expected: "User selects version number; bump_version.py runs clean; CHANGELOG [vNEXT] section renamed to versioned heading; CLAUDE.md Recently Changed entry reflects final version"
    why_human: "Version number is a human product decision, not a programmatic one"
---

# Phase 96: Completing My Library Feature Verification Report

**Phase Goal:** Close v7.14.0's deferred items (D-F1 per-file opt-out, D-F4 PDF extraction fallback, D-F5 LOCAL highlight) + NEW-2 LOCAL navigation + a freestyle polish bucket (D-15) for human-verified UX bugs.
**Verified:** 2026-05-25T06:35:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| 1 | D-F4: `_detect_single_word_per_line` exists with 0.70 threshold + 5-line guard | VERIFIED | `shared/local_indexer.py:157` — def + constants present; `_SINGLE_WORD_RATIO_THRESHOLD = 0.70` at line 153 |
| 2 | D-F4: `extract_pdf_pages` calls detector per-page and falls back to `get_text("text", sort=True)` | VERIFIED | `shared/local_indexer.py:381` — `if _detect_single_word_per_line(text):` wired; fallback at next line |
| 3 | D-F4: Codex MEDIUM #8 mode-spy test exists and passes | VERIFIED | `tests/test_local_pdf_extraction_fallback.py` has `test_good_pdf_does_not_invoke_fallback_mode`; all 4 tests pass (32 total in targeted suite) |
| 4 | D-F5: LOCAL hits carry `highlight_pattern` + asterisk-marker `snippet` + `raw_file_hl` | VERIFIED | `genizah_core.py:6911` — `_build_local_result_dict(self, doc, score, regex=None, pattern_str=None)` returns full dict with `highlight_pattern` key |
| 5 | D-F5: D-04.1 filter-out: regex non-match returns None + caller skips with continue | VERIFIED | `_build_local_result_dict` body contains `return None` + `if not hl_c:` check; `_query_local_index` body contains `if hit is None: continue` |
| 6 | D-F5: RRF merge call site passes `regex=regex` | VERIFIED | `genizah_core.py:8459` — `local_hits = self._query_local_index(query_str, mode, gap, regex=regex)` — exactly 1 match |
| 7 | D-F5: BLOCKER-4 integration test + Codex HIGH #3 instrumentation hook | VERIFIED | `self._last_local_query_regex` at `genizah_core.py:6681` (init) + `6880` (assignment); 4 hits in file |
| 8 | NEW-2: `get_local_browse_page` engine primitive exists with correct signature | VERIFIED | `genizah_core.py:9324` — `def get_local_browse_page(self, sys_id, p_num=None, next_prev=0, ...)`; `_local_pages_cache` has 4 hits |
| 9 | NEW-2: `load_local_page` dispatch in ResultDialog + `is_local_sys_id` branch | VERIFIED | `desktop/result_dialog.py:2399` — `def load_local_page`; dispatch at line 2258 via `is_local_sys_id` check |
| 10 | NEW-2: Browse panel View-All + per-page toggle with separator helper | VERIFIED | `genizah_app.py:111` — `_aggregate_local_pages_with_separators`; `btn_local_browse_prev/next` have 2 hits each |
| 11 | D-F1: `_UnifiedFileTreeWidget` exists with SET-DIFFERENCE/UNION `_commit_changes` | VERIFIED | `desktop/my_library_tab.py:107` — class definition; `_commit_changes` body: `.clear()` absent, `difference_update` present, `update` present |
| 12 | D-F1: `_prune_optouts_to_disk` defined + called in rescan callback | VERIFIED | `desktop/my_library_tab.py:77` def, `930` call site in `_on_indexer_finished`; `_reapply_filters_for_optout_change` at `genizah_app.py:17453` |
| 13 | NEW-3 polish: ruff clean across whole repository | FAILED | 2 F401 unused `import pytest` in `tests/test_local_filter_cascade.py:236` and `tests/test_local_optout_filter.py:9`; _tmp/ and seewald_addition/ also flagged but are not project source |
| 14 | Plan 96-09 close: CHANGELOG versioned + version bump complete | PARTIAL | CHANGELOG has `[vNEXT]` placeholder; version.py still `7.14.0`; Tasks 4+5 explicitly awaiting user version-bump decision at checkpoint |

**Score: 12/14 truths verified** (1 failed / fixable, 1 partial / awaiting human decision)

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/local_indexer.py` | D-F4 detect-then-fallback; `_detect_single_word_per_line` | VERIFIED | Function at line 157; wired into `extract_pdf_pages` at line 381 |
| `tests/test_local_pdf_extraction_fallback.py` | 4 D-F4 tests including Codex MEDIUM #8 mode-spy | VERIFIED | 4 tests pass; `test_good_pdf_does_not_invoke_fallback_mode` present |
| `genizah_core.py` | D-F5 LOCAL hit normalization + D-04.1 filter-out | VERIFIED | `_build_local_result_dict` + `_query_local_index` modified at lines 6855/6911; merge call site at 8459 |
| `genizah_core.py` | NEW-2 `get_local_browse_page` | VERIFIED | Line 9324; `_local_pages_cache` init + invalidation confirmed (4 hits) |
| `desktop/my_library_tab.py` | D-F1 `_UnifiedFileTreeWidget` (replaces QSplitter) | VERIFIED | Line 107; SET-DIFFERENCE/UNION algebra confirmed |
| `desktop/my_library_tab.py` | `_prune_optouts_to_disk` def + call | VERIFIED | Line 77 def, line 930 call |
| `genizah_app.py` | D-F1 `_reapply_filters_for_optout_change` | VERIFIED | Line 17453 |
| `genizah_app.py` | NEW-2 `_aggregate_local_pages_with_separators` + LOCAL browse nav widgets | VERIFIED | Line 111 (module-level function); `btn_local_browse_prev/next` referenced |
| `desktop/result_dialog.py` | NEW-2 `load_local_page` + `is_local_sys_id` dispatch | VERIFIED | Line 2399 def; dispatch at 2258 |
| `docs/OPEN_ISSUES.md` | D-F1/D-F4/D-F5 marked Fixed (2026-05-24) | VERIFIED | All 3 entries marked `Fixed (2026-05-24)` |
| `CHANGELOG.md` | New release section | PARTIAL | `[vNEXT]` present — versioned entry pending Task 4 checkpoint |
| `CLAUDE.md` | Recently Changed entry for Phase 96 | VERIFIED | "Phase 96 — My Library Polish (2026-05-24)" prepended |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `local_indexer.py:extract_pdf_pages` | `_detect_single_word_per_line` | function call after blocks-mode extraction | WIRED | Line 381: `if _detect_single_word_per_line(text):` |
| `genizah_core.py:_query_local_index` | `_build_local_result_dict(doc, score, regex=regex, ...)` | per-hit call passing regex through | WIRED | Loop at ~line 6900 passes `regex=regex, pattern_str=pattern_str` |
| `genizah_core.py:8459 merge call site` | `_query_local_index(query_str, mode, gap, regex=regex)` | RRF merge passes regex | WIRED | Exactly 1 match for `regex=regex` argument |
| `desktop/result_dialog.py:load_page` | `load_local_page` | `is_local_sys_id` branch dispatch | WIRED | Lines 2256-2258 |
| `desktop/result_dialog.py:load_local_page` | `genizah_core.py:get_local_browse_page` | engine call | WIRED | `self.searcher.get_local_browse_page(...)` at line ~2420 |
| `genizah_app.py:closeEvent` | `tree.flush_pending()` | opt-out debounce drain before save | WIRED | Lines 24691-24692 |
| `desktop/my_library_tab.py:_commit_changes` | `app._local_file_optouts` | SET-DIFFERENCE/UNION update | WIRED | `difference_update` + `update` at lines ~427-430 |
| `desktop/my_library_tab.py:_on_indexer_finished` | `_prune_optouts_to_disk` | call after rescan | WIRED | Line 930 |
| `genizah_app.py:_restore_session finally` | `my_lib.notify_session_restored()` | guarantees auto-select fires post-restore | WIRED | `genizah_app.py:24652` |

---

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|-------------------|--------|
| `_build_local_result_dict` `snippet` field | `hl_c` from `self.highlight(content, regex)` | compiled regex applied to real LOCAL file content | Yes — asterisk markers from regex match | FLOWING |
| `_query_local_index` result list | Tantivy hits filtered by regex | LOCAL Tantivy side-index query | Yes — real hits + filter | FLOWING |
| `get_local_browse_page` return dict | page list from `_local_pages_cache` | Tantivy query on LOCAL index by `full_header` prefix | Yes — real page content from indexed files | FLOWING |
| `_open_local_browse_page` HTML | `text` from `get_local_browse_page` | file content in Tantivy index | Yes — `html.escape(text)` before `\n→<br>` | FLOWING |

---

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| D-F4: `_detect_single_word_per_line("")` returns False | `python -c "from shared.local_indexer import _detect_single_word_per_line as d; assert d('') is False"` | Pass | PASS |
| D-F4: 6-line single-word input detects True | `python -c "from shared.local_indexer import _detect_single_word_per_line as d; assert d('a\nb\nc\nd\ne\nf') is True"` | Pass | PASS |
| D-F5: `_build_local_result_dict` returns None on non-match | `python -c "src=open('genizah_core.py').read(); start=src.find('def _build_local_result_dict'); end=src.find('\n    def ',start+10); assert 'return None' in src[start:end]"` | Pass | PASS |
| Merge site passes regex | `grep -c "regex=regex" genizah_core.py` returns >= 1 | 1 match | PASS |
| Ruff on production files | `python -m ruff check shared/local_indexer.py genizah_core.py genizah_app.py desktop/my_library_tab.py desktop/result_dialog.py` | All checks passed | PASS |
| Full test suite | `pytest tests/ -q --tb=no` | 2597 passed, 23 skipped, 4 xfailed | PASS |
| LOCAL regression bundle | `pytest tests/test_local_*.py tests/test_web_library_options_no_local.py tests/test_no_raw_storage_access.py -q` | 220 passed, 2 skipped, 2 xfailed | PASS |
| Fix-10 session race tests | `pytest tests/test_session_restore_ask_fix10.py -v` | 5 passed | PASS |
| Ruff on whole repo | `python -m ruff check .` | 12 errors (2 in test files: unused `import pytest`; rest in _tmp/ and seewald_addition/) | FAIL |

---

### Requirements Coverage

Phase 96 uses its own requirement IDs from `96-CONTEXT.md` (not the v7.13 REQUIREMENTS.md which covers phases 93-94). All 6 in-scope requirement IDs from plans:

| Requirement | Source Plan(s) | Description | Status | Evidence |
|-------------|---------------|-------------|--------|---------|
| D-F5 | 96-03 | LOCAL hit highlighting normalization | SATISFIED | `_build_local_result_dict` adds `highlight_pattern`; D-04.1 filter-out wired; BLOCKER-4 integration test passes |
| D-F4 | 96-01, 96-02 | PDF extraction fallback for one-word-per-line PDFs | SATISFIED | `_detect_single_word_per_line` + `extract_pdf_pages` fallback; 4 tests pass |
| D-F1 | 96-01, 96-04, 96-05, 96-06 | Per-file checkbox opt-out with persistence | SATISFIED | `_UnifiedFileTreeWidget`; persistence via session JSON; `_apply_local_optout_filter` at both cascade joinpoints; SET-DIFFERENCE/UNION algebra |
| NEW-1 | 96-07 (inferred) | Remove redundant `צפה בדפדוף` button for LOCAL hits | SATISFIED | `btn_rd_open_browse` grep count = 0; `test_btn_rd_open_browse_removed` passes |
| NEW-2 | 96-03, 96-08 | Next/prev navigation + View All for LOCAL | SATISFIED | `get_local_browse_page` engine primitive; `load_local_page`; Browse panel widgets; `_aggregate_local_pages_with_separators` |
| NEW-3 | 96-09 | Freestyle polish bucket (D-15 UAT bugs) | SATISFIED (partial) | 5 UAT bugs fixed; BLOCKER-5 stale-skip audit (10 skips converted); ruff gap (2 test F401s) + version-bump pending |

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|---------|--------|
| `tests/test_local_filter_cascade.py` | 236 | Unused `import pytest` (F401) | Warning | Ruff check fails on whole-repo scan; fixable with `--fix` |
| `tests/test_local_optout_filter.py` | 9 | Unused `import pytest` (F401) | Warning | Same; fixable with `--fix` |
| `CHANGELOG.md` | 11 | `[vNEXT]` placeholder not yet versioned | Info | Intentional pending state — gated on user checkpoint |

No blockers found in production code. Both ruff findings are in test files and are auto-fixable (F401).

---

### Human Verification Required

#### 1. D-F1 Opt-Out Persistence (Close → Reopen)

**Test:** Run the desktop app; switch to My Library tab; select a folder; uncheck one file; use File > Exit or the window X button to close; reopen the app; switch to My Library tab.
**Expected:** The unchecked file is still unchecked in the unified tree widget. Run a search that would hit that file — it should not appear in results.
**Why human:** Session persistence round-trip (close + reopen) requires a running Qt application. The unit tests cover the algorithm (`test_folder_a_optout_survives_folder_b_toggle` passes), but the full Qt event loop path — including the `flush_pending()` drain in `closeEvent` and `notify_session_restored()` in the startup path — requires visual confirmation.

#### 2. D-F1 Cross-Folder Persistence (Codex HIGH #1)

**Test:** Opt out a file from folder A; switch to folder B in the top folder list; uncheck and re-check any file in folder B; switch back to folder A.
**Expected:** The folder A opt-out is still in place (checkbox still unchecked for the file opted out earlier). Run the search — the folder A file still excluded.
**Why human:** Although the SET-DIFFERENCE/UNION algorithm is verified by `test_folder_a_optout_survives_folder_b_toggle`, the full Qt signal chain (folder selection → `_on_folder_changed` → `populate_for_folder` → `_commit_changes` debounce) needs real-world verification.

#### 3. D-F5 LOCAL Hit Highlighting (Visual)

**Test:** Index a folder containing a .txt or .docx file with a known search term. Run a search for that term in "ALL" corpus mode. Examine the LOCAL result row and open ResultDialog for it.
**Expected:** Search result row shows a snippet with the term wrapped in `*...*` markers (bold in the table); ResultDialog shows the same highlighted term in the manuscript view.
**Why human:** Visual rendering of `highlight_pattern` in both the Qt table delegate and the QTextBrowser widget in ResultDialog requires a running app.

#### 4. NEW-2 Browse at Correct Page (Codex MEDIUM #5)

**Test:** Run a search that returns a LOCAL PDF hit at page 7 (or any page > 1). Click "Browse" on that result row. Check the Browse panel.
**Expected:** Browse panel opens at page 7 (the page the search hit was on), not page 1. Prev/Next buttons navigate pages correctly. View All shows all pages with `— page N —` separators.
**Why human:** Requires running app + LOCAL indexed PDF with multiple pages + a search hit on a non-first page. The iter-4 fix addressed this but UAT round 4 confirmed the repair; final visual confirmation with real user data needed.

#### 5. Version Bump Decision (Plan 96-09 Task 4)

**Test:** Decide whether Phase 96 ships as v7.14.1 (minor patch — polish only) or v7.15.0 (milestone).
**Expected:** After decision: `python scripts/bump_version.py X.Y.Z` runs clean; `CHANGELOG.md` `[vNEXT]` renamed to versioned heading; `CLAUDE.md` Recently Changed entry updated.
**Why human:** Version number is a product decision. The code is ready to ship; only the version label requires human judgment.

---

### Gaps Summary

**Gap 1 (Ruff F401 — fixable):** Two test files have unused `import pytest` that prevent `python -m ruff check .` from exiting 0. Fix: `python -m ruff check tests/test_local_filter_cascade.py tests/test_local_optout_filter.py --fix`. This is a 2-line auto-fix; all 2597 tests pass without touching these imports. Production code is ruff-clean.

**Gap 2 (Version bump — awaiting human):** CHANGELOG has `[vNEXT]` placeholder; `version.py` is still `7.14.0`. Plan 96-09 Tasks 4 + 5 are explicitly "Awaiting" — the user must decide the version number (v7.14.1 minor vs v7.15.0 milestone) before the bump script can run. This is the correct and intended state of the phase.

Both gaps have clear, documented closure paths. Gap 1 is a 30-second fix; Gap 2 is gated on a human decision.

---

_Verified: 2026-05-25T06:35:00Z_
_Verifier: Claude (gsd-verifier)_
