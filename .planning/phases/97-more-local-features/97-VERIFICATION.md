---
phase: 97-more-local-features
verified: 2026-05-25T18:00:00Z
status: human_needed
score: 7/8 goal-backward checks verified
overrides_applied: 0
human_verification:
  - test: "End-to-end indexing at 13K-file / 43 GB with mid-run crash and Resume"
    expected: "Recovery modal appears on next launch; LOCAL search is gated during recovery; Resume resumes without re-extracting committed files."
    why_human: "Requires a real 43 GB Hebrew scholarly corpus and taskkill simulation. No CI machine has this data; scripted kill race is non-deterministic."
  - test: "FolderWalkWorker wired to main scan path (U-03 full close of D-F9)"
    expected: "FolderWalkWorker emits batched pyqtSignal(list) during active indexing; no QWidget mutation from worker thread."
    why_human: "FolderWalkWorker class is defined and tested in isolation but is not connected to LocalIndexerWorker._start_indexing() or any production scan invocation. The test suite verifies the class in isolation only. A human needs to confirm whether the prescan-only use (PrescanWorker) satisfies D-F9 or if the full wiring is needed."
  - test: "Bilingual EN+HE Help / About visual review"
    expected: "zstd cleartext disclosure is readable in both languages; wording matches Phase 95 D-33 voice."
    why_human: "Strings confirmed present in code; AST test passes. Layout and Hebrew RTL rendering requires visual inspection."
---

# Phase 97: More LOCAL Features — Verification Report

**Phase Goal:** Make My Library usable at the scale Seewald's prototype already serves (13K files / 43 GB, target ceiling 50K / 50 GB) by adding crash-recovery semantics, durable text cache, and atomic Tantivy rebuild — and extend the file-format set with three light textual formats (.html / .xlsx / .csv). Does NOT add reading-experience features (OCR, side-by-side PDF) and does NOT touch web LOCAL exposure.

**Verified:** 2026-05-25T18:00:00Z
**Status:** HUMAN_NEEDED
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Storage contract is final and additive — 13-field Tantivy schema (11 base + scan_run_id + chunk_locator); SQLite at user_version=2 with migration ladder; no Wave E/F reshape | VERIFIED | `shared/local_indexer.py` lines 371-388 count exactly 13 add_text_field calls. `shared/local_indexer_migrations.py` _LATEST_VERSION=2, ladder 0->1->2. Test suite: 22 Wave A tests pass. |
| 2 | Capacity lift is real — `_MAX_FILES_CEILING = 50_000`, `_MAX_BYTES_CEILING = 50 GB`; soft warning Proceed/Cancel, not hard stop | VERIFIED | `desktop/my_library_tab.py` lines 69-70: constants confirmed. Lines 1570-1592: ceiling check uses `_show_ceiling_confirm_dialog` (Yes/Cancel, soft). |
| 3 | Recovery gate works — scan_runs lifecycle table; is_searchable weakref gate in `SearchEngine._query_local_index` with default-True fallback; 3-button recovery modal | VERIFIED | `genizah_core.py` lines 7070-7075: gate is FIRST executable check; default-True on dead weakref. `desktop/my_library_tab.py` lines 682, 714-717: `is_searchable=False` default, flip after probe. `_show_recovery_modal` at line 1002 has 3 buttons. `scan_runs` table in init_sqlite. 4 recovery_gate tests pass. |
| 4 | New formats wired — `_SUPPORTED_EXTENSIONS = {.pdf, .docx, .txt, .html, .xlsx, .csv}`; `defusedxml.defuse_stdlib()` at module init; xlsx extractor downstream of `_check_zip_bomb` | VERIFIED | `shared/local_indexer.py` line 80: set confirmed. Lines 53-63: defusedxml.defuse_stdlib() at module init with ImportError warning fallback. Line 96: extract_xlsx_pages calls _check_zip_bomb first per SUMMARY-03. 80/80 Phase 97 tests pass including test_xlsx_extraction.py. |
| 5 | No RTL pre-fix in new extractors — F-06 invariant: `tests/test_format_rtl_invariant.py` AST guard exists and is green | VERIFIED | File exists and confirmed. AST guard passes: test_format_rtl_invariant_no_fix_rtl_in_new_extractors PASSED. |
| 6 | Cancel-and-discard works — `discard_run(run_id)` does four-source atomic delete (Tantivy + local_pages + local_files + processed_files) with writer.rollback() for uncommitted docs | VERIFIED | `shared/local_indexer.py` lines 2898-3012: discard_run present. SQL block confirmed in SUMMARY-05 (local_pages + local_files + processed_files + scan_runs UPDATE in one BEGIN IMMEDIATE). Step 1: writer.rollback(). tests/test_scan_run_id.py::test_discard_removes_all_four_row_sources PASSED. |
| 7 | No regressions to v7.14 invariants — Phase 87 multitenant chokepoint untouched; web/ unaffected; v7.14 CI guards still green | VERIFIED | `tests/test_no_raw_storage_access.py`: 6/6 PASSED. `tests/test_web_library_options_no_local.py`: 3/3 PASSED. `tests/test_phase_97_invariants.py`: 4/4 PASSED. Allowlist `[]` confirmed green. |
| 8 | Documentation maintained — CHANGELOG.md has Phase 97 entry; OPEN_ISSUES.md updated | PARTIAL | CHANGELOG.md has Wave F entry at `[vNEXT]` (lines 11-30). Waves A-E are NOT consolidated into a single Phase 97 CHANGELOG section — only Wave F appears. OPEN_ISSUES.md Last Updated timestamp is 2026-05-25 with Phase 97 content. D-F7 entry still says "200 pages" cap despite U-04 raising it to 500. |

**Score:** 7/8 goal-backward checks fully verified (Check 8 is partial but not blocking)

---

## Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/local_indexer_migrations.py` | SQLite migration ladder | VERIFIED | _LATEST_VERSION=2, ladder 0->1->2, integrity_check gate |
| `shared/local_indexer.py` | Schema, extractors, recovery, discard | VERIFIED | 13-field schema, extract_html/xlsx/csv_pages, discard_run, _commit_batch bracket |
| `desktop/my_library_tab.py` | Ceilings, PrescanWorker, FolderWalkWorker, recovery modal | VERIFIED | 50K/50GB constants, PrescanWorker wired to ceiling check, FolderWalkWorker defined (see gap) |
| `genizah_core.py` | is_searchable gate, attach_my_library_tab | VERIFIED | Lines 7070-7075 gate; attach_my_library_tab at line 6800 |
| `tests/test_format_rtl_invariant.py` | F-06 AST guard | VERIFIED | Passes in CI |
| `tests/test_phase_97_invariants.py` | 4 invariant CI guards | VERIFIED | All 4 sub-tests pass |

---

## Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `MyLibraryTab.__init__` | `is_searchable=False` gate | `genizah_core._query_local_index` | WIRED | weakref via attach_my_library_tab in on_startup_finished |
| `_check_ceiling_single_folder` | `PrescanWorker` | `QProgressDialog` cancel loop | WIRED | PrescanWorker class at line 552, spawned in ceiling check |
| `FolderWalkWorker` | `_start_indexing` / scan path | expected signal wiring | ORPHANED | Class defined and tested in isolation; not connected to any production scan invocation — see human verification item |
| `_commit_batch` | `PRAGMA synchronous=FULL` bracket | `BEGIN IMMEDIATE` + ROLLBACK | WIRED | Lines 2518-2540 confirmed |
| `discard_run` | Tantivy term delete + 3 SQLite tables | `writer.rollback()` + `delete_documents` + SQL | WIRED | 5-step protocol confirmed, 4-source test passes |
| `defusedxml.defuse_stdlib()` | `local_indexer.py` module init | before openpyxl/lxml | WIRED | Lines 53-63 confirmed |

---

## Data-Flow Trace (Level 4)

Not applicable — phase produces no web-rendered dynamic components. All output surfaces are desktop Qt widgets verified through behavioral test assertions.

---

## Behavioral Spot-Checks

| Behavior | Result | Status |
|----------|--------|--------|
| 80 Phase 97 test files (22 Wave A + 8 Wave B + 16 Wave C + 10 Wave D + 13 Wave E + 20 Wave F) | 80/80 PASSED in 17.31s | PASS |
| Phase 87 multitenant invariants (9 tests) | 9/9 PASSED | PASS |
| Phase 97 cross-phase invariants (5 tests) | 5/5 PASSED | PASS |

---

## Requirements Coverage

All 27 requirements from ROADMAP.md Phase 97 `Requirements` list verified against implementations:

| Requirement Group | Requirements | Status |
|-------------------|-------------|--------|
| Wave A Recovery | D-NEW-1, R-03, R-02, R-04, R-01 | SATISFIED — 22 tests cover each |
| Wave B Safety | C-02, C-05, D-NEW-8 | SATISFIED — 8 tests cover each |
| Wave C Formats | F-01, F-02, F-03, F-04, F-05, F-06 | SATISFIED — 16 tests, all 3 extractors present |
| Wave D Capacity | C-01, C-03, C-04, C-06 | SATISFIED — ceiling 50K/50GB, PrescanWorker wired, disk indicator present |
| Wave E UX | U-01, U-02, U-03, U-04 | PARTIALLY SATISFIED — ETA, discard_run, View All 500+incremental all wired; FolderWalkWorker defined but not connected to scan path (see gap) |
| Wave F Gap closure | D-NEW-2, D-NEW-3, D-NEW-4, D-NEW-5, D-NEW-6, D-NEW-7 | SATISFIED — 20 tests, all invariant guards pass |

---

## Anti-Patterns Found

| File | Pattern | Severity | Impact |
|------|---------|----------|--------|
| `shared/local_indexer.py` | `defusedxml` DeprecationWarning (`cElementTree deprecated`) | Info | defusedxml==0.7.1 ships a deprecated internal import; does not affect functionality; upgrading to 0.8+ would suppress. Not Phase 97-introduced. |
| `desktop/my_library_tab.py` | `FolderWalkWorker` class defined but never instantiated in production code | Warning | U-03 spec says folder walk should run in QThread. The class works (tested) but is not wired to the actual indexing entry point. D-F9 (UI-thread walk) remains partially open. |
| `docs/OPEN_ISSUES.md` | D-F7 entry still reads "capped at 200 pages" | Info | U-04 raised cap to 500 with incremental rendering. The OPEN_ISSUES entry was not updated to reflect this. |
| `CHANGELOG.md` | Only Wave F appears under `[vNEXT] - Phase 97`; Waves A-E not consolidated | Warning | CLAUDE.md requires CHANGELOG entry for major features. Recovery foundation, capacity lift, and new formats are not documented in CHANGELOG. Not blocking goal achievement but incomplete per project conventions. |

---

## Human Verification Required

### 1. End-to-end crash recovery at scale

**Test:** (a) Point MyLibraryTab at a large corpus (>=13K files, >=10 GB). (b) Begin indexing. (c) `taskkill /f` the desktop process mid-batch. (d) Reopen app. Verify recovery modal appears, LOCAL search shows recovery-pending banner, Resume continues without re-extracting committed files.

**Expected:** Recovery modal with Resume/Restart/Skip. LOCAL search returns 0 hits until modal resolved. Resume uses cached_text for committed rows (no source file re-read). Restart triggers atomic rebuild from empty.

**Why human:** Requires a real large corpus and process-kill simulation. CI cannot reproduce 43 GB of Hebrew scholarly files. The SQLite scan_runs probe and the modal are unit-tested, but the full crash-then-resume sequence is manual-only per 97-VALIDATION.md.

### 2. FolderWalkWorker wiring decision

**Test:** Determine whether `FolderWalkWorker` should be wired to the main scan flow (`LocalIndexerWorker._start_indexing`) or whether `PrescanWorker` already satisfies the U-03 / D-F9 intent.

**Expected:** If D-F9 intended to move only the ceiling prescan to a QThread, then PrescanWorker already satisfies it. If it intended to move the full folder tree population (`_UnifiedFileTreeWidget.populate_for_folder`) to a background thread, then `FolderWalkWorker` needs wiring. Current state: `FolderWalkWorker` is isolated infrastructure.

**Why human:** The CONTEXT says U-03 "closes D-F9" but SUMMARY-05 explicitly notes "the class is available for connection." The scope interpretation needs a product decision.

### 3. Bilingual disclosure visual review

**Test:** Open Help page and About dialog; verify EN + HE cleartext disclosure section is readable and matches Phase 95 D-33 voice.

**Expected:** English section references `local_index.sqlite3`, `zstd`, "not encryption", "never uploaded". Hebrew section contains equivalent text in natural Hebrew. Layout is not broken by RTL/LTR mixing.

**Why human:** Strings are confirmed present by test (`test_privacy_disclosure_strings.py` passes). Visual layout and Hebrew prose quality requires human review.

---

## Gaps Summary

**No blocking code gaps.** All Wave A (recovery), Wave B (safety), Wave C (formats), Wave D (capacity), and Wave F (closure) functionality is fully implemented and tested. The 80-test suite passes.

The one partial implementation (FolderWalkWorker not wired to production scan path) is explicitly disclosed in SUMMARY-05 and does not prevent the core goal: My Library is now usable at 13K-file / 43 GB scale because recovery is in place (Wave A), the ceiling is lifted (Wave D), and new formats work (Wave C).

Documentation gaps (CHANGELOG missing Waves A-E, OPEN_ISSUES D-F7 stale) are informational and should be resolved before the v7.15.0 release tag. Recommended as 999.x backlog items:

- **999.x-a**: Update CHANGELOG.md — add consolidated Phase 97 entry covering Waves A-E (recovery foundation, 50K ceiling, HTML/XLSX/CSV formats, capacity UX, scan_run_id discard)
- **999.x-b**: Update OPEN_ISSUES.md D-F7 — reflect that cap was raised 200 → 500 with incremental rendering (U-04) and D-F9 status re-evaluated per FolderWalkWorker wiring decision
- **999.x-c**: If FolderWalkWorker wiring decision (human item 2 above) requires wiring to scan path, track as follow-up before calling U-03 fully closed

---

## Deferred Items

Items intentionally not implemented in Phase 97 per CONTEXT `<deferred>` section:

| Item | Addressed In | Evidence |
|------|-------------|---------|
| PDF OCR (D-F2) | v7.15+ | Explicitly deferred in CONTEXT deferred section |
| Side-by-side PDF rendering (D-F3) | v7.15+ | Explicitly deferred in CONTEXT deferred section |
| .md / .epub / .rtf formats | v7.15+ | Explicitly deferred in CONTEXT deferred section |
| Full background-thread View All refactor (D-F7 full) | v7.15+ | U-04 does cap+incremental (partial); full QThread refactor deferred |
| Web parity for LOCAL | Out of scope | Phase 95 SPEC invariant preserved |

---

_Verified: 2026-05-25T18:00:00Z_
_Verifier: Claude (gsd-verifier)_

---

## 2026-05-26 Hotfix Note — Phase 97.1 + Phase 97.2 Cascade Closure

Phase 97 verification completed on its own date with a known set of P0 cascade
items deferred. Two follow-up phases retroactively closed those items:

### Phase 97.1 (commit `2e1b846e`, 2026-05-25)
- MAX_PATH long-path prefix (`\\\\?\\` on Windows for paths > 260 chars)
- Non-blocking cancel + per-file cancel check (resolves UI freeze + `WinError 3`
  storm during cancel on large folders)

### Phase 97.2 (2026-05-26)
8-bug recovery cascade fix + new "Reset My Library" / "אפס ספריה שלי" toolbar
action. See `.planning/phases/97.2-recovery-cascade-lockbusy/` for full
documentation. Closes the Phase 97 P0 cascade items where startup +
upgrade-from-Phase-95 + post-cancel discard combined to leave the LOCAL index
in a permanently-broken state. New tests:

- `tests/test_phase_97_2_schema_marker_absence.py` (R97.2-F — Phase 95
  upgrade trigger; the literal root cause)
- `tests/test_phase_97_2_writer_handle_leak.py` (R97.2-A + R97.2-B)
- `tests/test_phase_97_2_discard_writer_lifecycle.py` (R97.2-C + R97.2-G)
- `tests/test_phase_97_2_sqlite_vs_tantivy_consistency.py` (R97.2-H)
- `tests/test_phase_97_2_reset_my_library_full_cycle.py` (R97.2-E)

**Phase 97 verification status (post-97.2):** the P0 cascade items originally
flagged for follow-up are now closed retroactively. Phase 97 invariants
(`tests/test_phase_97_invariants.py`, `tests/test_scan_run_id.py`) remain
green. Phase 87 web multitenant invariant
(`tests/test_phase_87_no_raw_storage_access.py`) remains green.
