---
phase: 95-my-library
verified: 2026-05-21T00:00:00Z
status: human_needed
score: 10/10 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Index 3+ local files via My Library tab; run a search returning both Genizah hits AND LOCAL hits; verify COL_SRC shows 'LOCAL' in blue (#3498db); verify LOCAL filter button appears and cycles All → Only Local → No Local → All; close and reopen app, verify filter state persists (REQ-6 D-39 full cycle)"
    expected: "Filter button visible only when LOCAL hits present; cycling correct; persistence across restart; independent state per surface (Search vs Composition vs Parallels)"
    why_human: "Qt UI state, QSettings persistence, and per-session JSON corpus_scope require live desktop app — cannot verify programmatically without a running Qt event loop"
  - test: "Double-click a LOCAL search result row; verify a ResultDialog opens (NOT Browse panel redirect); verify 'Open file' button is visible; click it — verify OS default app launches the file"
    expected: "ResultDialog with Open File button; os.startfile() launches Word/Acrobat/Notepad for the file's type"
    why_human: "os.startfile() integration and ResultDialog rendering require live desktop UI"
  - test: "With LOCAL filter state 'Only Local' set, run a search that returns ZERO LOCAL hits; verify filter renders as NO-OP (Genizah results shown, not filtered out); verify inline chip 'My Library filter inactive — no LOCAL hits in this query' appears"
    expected: "D-10 P1 NO-OP: all results shown; chip visible; persisted state preserved for next query that does have LOCAL hits"
    why_human: "Chip visibility and NO-OP behavior depend on Qt label/widget visibility state at runtime"
  - test: "Confirm above-ceiling dialog: register 3 folders whose aggregate file count exceeds 5,000 or total size exceeds 2 GB; click Refresh; verify modal dialog 'Indexing N files (X GB) — performance may degrade. Continue?' with Yes/Cancel"
    expected: "Dialog appears before scan begins; Cancel aborts; Yes proceeds (REQ-10 D-26 W8 aggregate ceiling)"
    why_human: "Requires a large file corpus and live Qt modal dialog interaction"
  - test: "Verify About dialog (desktop) contains 'My Library feature inspired by Yehuda Seewald's GenizahLocal prototype.' in EN and 'תכונת הספרייה שלי בהשראת אב-טיפוס GenizahLocal של יהודה זיוואלד.' in HE (D-32)"
    expected: "Both attribution lines present in the About dialog, bilingual"
    why_human: "About dialog content is rendered from ABOUT_HTML string in genizah_translations.py; correctness and display require visual inspection"
  - test: "Open Help page (web); verify 'My Library — Local Documents' section is present; verify cleartext-on-disk disclosure line is present in EN and HE (D-31 + D-33)"
    expected: "Section present with privacy guarantee, three-cloud-write-gates text, filter usage, hostname-rename caveat, and cleartext disclosure"
    why_human: "Web Help page visual presence and Hebrew rendering require browser check"
---

# Phase 95: My Library Verification Report

**Phase Goal:** Desktop users can point GenizahSearch at folders of `.docx` / `.pdf` / `.txt` files and have those documents indexed into a SEPARATE Tantivy side-index merged into normal search / Composition Search / Parallels results with a clear `LOCAL` badge and a three-state filter button. Personal corpora NEVER leak to the cloud — three regression tests pin the cloud-write boundaries. Productizes Yehuda Seewald's external prototype as a first-class in-app feature.

**Verified:** 2026-05-21
**Status:** human_needed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | LOCAL sys_ids use 18-digit 97-prefix namespace, disjoint from NLI/CUDL/real Alma | VERIFIED | `shared/local_sys_id.py` implements `is_local_sys_id()`, `generate_local_sys_id()` with `% 10**8` D-19 fix; `tests/test_local_sys_id_namespace.py` 17 tests pass |
| 2 | LOCAL documents live in a separate Tantivy side-index, never merged into shared corpus | VERIFIED | `shared/local_indexer.py` LocalIndexer class; `Config.LOCAL_INDEX_DIR` / `Config.LOCAL_LAB_INDEX_DIR` separate from main index; `genizah_core.py:6686+` `_open_local_searcher` opens side-index independently |
| 3 | Indexer ingests .docx, .pdf, .txt; yields unsupported_extension for others | VERIFIED | `shared/local_indexer.py` `_SUPPORTED_EXTENSIONS = {".docx", ".pdf", ".txt"}`; `tests/test_local_indexer.py` passes |
| 4 | RTL helpers ported as dead-code safety net (PyMuPDF is v1 primary extractor) | VERIFIED | `_fix_rtl_line`, `_fix_rtl_page`, `_join_fragmented_lines` in `shared/local_indexer.py` lines ~86-200 marked `# DEAD CODE per D-02`; PyMuPDF `fitz.get_text("blocks")` is the live path |
| 5 | Incremental re-index: second scan of unchanged folder is ≤ 5% of first scan wall time | VERIFIED | Two-phase commit protocol in `local_indexer.py`; SQLite `processed_files` mtime cache; `tests/test_local_indexer_incremental.py` 5 tests pass |
| 6 | Three-state LOCAL filter on Search / Composition Search / Parallels; cascade discipline enforced | VERIFIED | `_apply_local_filter` in `genizah_app.py:17323`; called from both `_apply_results_table_filters` (line 17474) and `_apply_comp_tree_filters` (line 17807); static AST test `tests/test_local_filter_cascade.py` passes; D-10 P1 NO-OP chip wired |
| 7 | LOCAL hits display blue LOCAL badge in COL_SRC column and "My Library" in library column | VERIFIED | `genizah_app.py:16681-16685` writes `QColor("#3498db")` for LOCAL; `comp_col_src = 8` added to comp_tree; `LIBRARY_CODES['LOCAL'] = 'My Library'` at `genizah_core.py:2016` |
| 8 | My Library tab is 7th desktop tab with folder picker, indexing, progress bar, worker thread, ceiling dialog | VERIFIED | `desktop/my_library_tab.py` MyLibraryTab; `genizah_app.py:3111` `addTab(self.my_library_tab, "My Library")`; 7th position after Search/Composition/Browse/Browse-by-ID/Personal Lists/Community; QMutex, QThread worker, ceiling checks wired |
| 9 | LOCAL sys_ids hard-rejected at all three cloud-write boundaries BEFORE any cloud I/O | VERIFIED | See REQ-9 detail below |
| 10 | Indexer handles up to 5,000 files / 2 GB with above-ceiling warning dialog; streaming per-file commit | VERIFIED | `_check_ceiling_single_folder` and `_check_ceiling_refresh_aggregate` in `desktop/my_library_tab.py`; D-21 batch-commit-every-25-files; `tests/test_local_ceiling_enforcement.py` passes |

**Score:** 10/10 truths verified (automated). 6 items require human visual/interactive verification.

---

### REQ-9 Cloud-Write Gates — Detailed Verification

**Gate 1: `lists_sync.sync_item_to_cloud`**

Code at `lists_sync.py:752-766`: LOCAL gate is the FIRST STATEMENT of the function body, before `is_sync_available()` at line 768 and `_get_client()` at line 772. The HIGH-2 fix is in place: `sys_id = item_data.get('sys_id', item_id) if item_data else item_id` runs OUTSIDE any `if item_data:` branch, so a LOCAL `item_id` with missing `item_data` is also gated. Confirmed by `tests/test_local_namespace_no_lists_leak.py::test_sync_item_to_cloud_local_item_id_missing_data` (load-bearing `_get_client.call_count == 0` assertion passes).

**Gate 2: `lists_sync.sync_list_to_cloud`**

Code at `lists_sync.py:699-713`: LOCAL gate is the FIRST STATEMENT, before `is_sync_available()` at line 715. Iterates `self.lists_manager.data.get('items', {})` flat dict, checks `item_data.get('lists')` membership, checks `is_local_sys_id(item_data.get('sys_id', iid))`. B2 field names correctly pinned. Confirmed by `test_sync_list_to_cloud_aborts_if_any_item_local`.

**Gate 3: `corrections_client.py:627-630`**

Parallel gate immediately after the SYNTH-06 synthetic gate. Distinct error code `local_corrections_disabled` (NOT merged into synthetic gate). Confirmed by `tests/test_local_namespace_no_corrections_leak.py`.

**Gate 4: `shared/search_serializer.py:582-585`**

`_is_local_item()` helper defined at line 106; filter `results = [r for r in results if not _is_local_item(r)]` at line 585, BEFORE the `_serialize_item` listcomp. Confirmed by `tests/test_local_namespace_no_api_leak.py`.

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/local_sys_id.py` | is_local_sys_id + generate_local_sys_id + D-19 modulo fix | VERIFIED | 124 lines; all public API present; D-42 canonical_filepath included |
| `shared/local_indexer.py` | LocalIndexer class with full pipeline | VERIFIED | 1595+ lines; per-thread SQLite; two-phase commit; RTL dead-code helpers; D-40 unavailability |
| `desktop/my_library_tab.py` | MyLibraryTab + LocalIndexerWorker QThread | VERIFIED | Exists; imports in genizah_app.py line 74; tab registered line 3111 |
| `genizah_app.py` | COL_SRC LOCAL badge; comp_col_src; 3-state filter; _apply_local_filter; cascade hooks | VERIFIED | Multiple verified grep hits; comp_col_src=8; #3498db; _apply_local_filter defined and called from both cascade joinpoints |
| `lists_sync.py` | LOCAL gate at TOP of sync_item_to_cloud + sync_list_to_cloud | VERIFIED | Lines 699-713 and 752-766; imports is_local_sys_id at line 15 |
| `corrections_client.py` | Parallel LOCAL gate with local_corrections_disabled code | VERIFIED | Lines 627-630; import at line 22 |
| `shared/search_serializer.py` | _is_local_item filter before _serialize_item | VERIFIED | Lines 53-54, 106-115, 582-585 |
| `genizah_core.py` | LIBRARY_CODES['LOCAL']='My Library'; parse_header_smart 97-prefix; LOCAL_LAB_INDEX_DIR | VERIFIED | Lines 2015-2016; 3845-3846; 2214 |
| `tests/test_local_namespace_no_api_leak.py` | REQ-9 API gate test (GREEN) | VERIFIED | 2 tests pass |
| `tests/test_local_namespace_no_lists_leak.py` | REQ-9 lists gate test (GREEN, HIGH-2) | VERIFIED | 6 tests pass including load-bearing item_data-absent path |
| `tests/test_local_namespace_no_corrections_leak.py` | REQ-9 corrections gate test (GREEN) | VERIFIED | 3 tests pass |
| `tests/test_local_filter_cascade.py` | Static AST cascade discipline guard | VERIFIED | Scans genizah_app.py; asserts _apply_local_filter called from both _apply_results_table_filters and _apply_comp_tree_filters; passes |
| `tests/test_web_library_options_no_local.py` | D-46 AST guard: web/pages never iterates LIBRARY_CODES without LOCAL guard | VERIFIED | 3 tests pass; NO-OP today (no current violators); CI pin in place |
| `web/pages/help.py` | My Library section bilingual EN+HE | VERIFIED | Lines 636-673; section present |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| `lists_sync.sync_item_to_cloud` | `shared.local_sys_id.is_local_sys_id` | First statement of function body, before `is_sync_available()` | WIRED | Line 764; import at line 15 |
| `lists_sync.sync_list_to_cloud` | `shared.local_sys_id.is_local_sys_id` | First statement of function body, before `is_sync_available()` | WIRED | Line 711; same import |
| `corrections_client.create_correction` | `is_local_sys_id` | Parallel gate after synthetic gate | WIRED | Line 627; import at line 22 |
| `shared/search_serializer.serialize_search_payload` | `_is_local_item` filter | Before `_serialize_item` listcomp | WIRED | Lines 582-585 |
| `genizah_app._apply_results_table_filters` | `_apply_local_filter` | Called within cascade function body | WIRED | Line 17474 |
| `genizah_app._apply_comp_tree_filters` | `_apply_local_filter` | Called within cascade function body | WIRED | Line 17807 |
| `genizah_core.LabEngine.__init__` | `reload_local_lab_index` | CR-02 fix: LOCAL LAB attrs initialized on LabEngine at startup | WIRED | Lines 698-714; `reload_local_lab_index()` at line 714 |
| `genizah_core.SearchEngine._current_lab_weights_hash` | `getattr(self, 'dynamic_rank_map', None)` | CR-01 fix: safe getattr defaults so SearchEngine without LAB attrs never raises | WIRED | Lines 6772-6773 |

---

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `genizah_app.py` result table | `self.last_results` / LOCAL rows | `LocalIndexer` side-index via `SearchEngine._open_local_searcher` + RRF merger at `genizah_core.py:6900+` | Yes — Tantivy query on side-index returns real indexed text | FLOWING |
| `genizah_app._apply_local_filter` | `results` list | Post-dedup merge result set (`genizah_core.py:8239-8255`) | Yes — LOCAL hits merged after `_deduplicate()` per D-08 Codex P0 fix | FLOWING |
| `shared/search_serializer._is_local_item` | Result row `display.library_code` | Search result row; web Tantivy never indexes LOCAL so filter is defense-in-depth (zero LOCAL in web index) | Defense-in-depth (no LOCAL in web Tantivy today) | FLOWING |

---

### Behavioral Spot-Checks

Step 7b: SKIPPED for UI surfaces (requires running Qt desktop app — no headless entry point). Server API tests covered by unit tests.

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Cloud-write gates: lists sync LOCAL item blocked | `pytest tests/test_local_namespace_no_lists_leak.py -q` | 6 passed | PASS |
| Cloud-write gates: API serializer drops LOCAL | `pytest tests/test_local_namespace_no_api_leak.py -q` | 2 passed | PASS |
| Cloud-write gates: corrections blocked | `pytest tests/test_local_namespace_no_corrections_leak.py -q` | 3 passed | PASS |
| Cascade discipline (static AST) | `pytest tests/test_local_filter_cascade.py -q` | passes | PASS |
| Web library options guard | `pytest tests/test_web_library_options_no_local.py -q` | 3 passed | PASS |
| Full Phase 95 test suite | `pytest tests/test_local_*.py + related -q` | 193 passed, 2 skipped, 2 xfailed | PASS |
| Full test suite (regression) | `pytest tests/ -q` | 2524 passed, 23 skipped, 4 xfailed, 0 failures | PASS |

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| REQ-1 LOCAL-NAMESPACE | 95-02 | 97-prefix 18-digit namespace, is_local_sys_id, generate_local_sys_id | SATISFIED | shared/local_sys_id.py; 17 tests in test_local_sys_id_namespace.py pass |
| REQ-2 SIDE-INDEX | 95-03, 95-05 | Separate Tantivy side-index; main index untouched; result-merger post-dedup (D-08 fix) | SATISFIED | LocalIndexer; Config.LOCAL_INDEX_DIR; RRF merger at genizah_core.py:8239-8255 |
| REQ-3 FILE-TYPES | 95-03 | .docx/.pdf/.txt; unsupported_extension for others | SATISFIED | _SUPPORTED_EXTENSIONS; tests pass |
| REQ-4 RTL-EXTRACTION | 95-03 | RTL helpers ported as dead-code; PyMuPDF primary; D-44 Hebrew fixture | SATISFIED | Dead-code helpers in local_indexer.py; fitz.get_text("blocks") live path; hebrew_sample.pdf fixture present |
| REQ-5 INCREMENTAL-REINDEX | 95-03 | SQLite mtime cache; second scan fast; D-21 two-phase commit | SATISFIED | processed_files table; two-phase commit; incremental tests pass |
| REQ-6 THREE-STATE-FILTER | 95-08 | Filter on Search/Composition/Parallels; cascade discipline; D-10 P1 NO-OP; QSettings persistence | SATISFIED (automated) / HUMAN for visual | AST cascade test passes; D-10 P1 chip code present; QSettings keys present — visual cycling requires human |
| REQ-7 RESULT-BADGE | 95-08 | LOCAL badge in COL_SRC blue; My Library in library column | SATISFIED (automated) / HUMAN for visual | #3498db code present; LIBRARY_CODES['LOCAL'] wired — visual color requires human |
| REQ-8 MY-LIBRARY-TAB | 95-07 | 7th tab; folder picker; worker; progress; ceiling dialog | SATISFIED (structural) / HUMAN for UX | Tab registered; MyLibraryTab class exists; ceiling entry points present — UX flow requires human |
| REQ-9 CLOUD-WRITE-GATES | 95-04 | Three gates: serializer + corrections + lists (BOTH sync_item AND sync_list) | SATISFIED | All three gates verified; position BEFORE cloud I/O confirmed; 11 regression tests pass |
| REQ-10 SCALE-CEILING | 95-07 | 5000 files / 2 GB ceiling dialog; streaming per-file commit; QThread | SATISFIED (structural) / HUMAN for ceiling dialog | Ceiling entry points; D-21 batch commit; QThread worker — ceiling dialog visual requires human with large corpus |

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `shared/local_indexer.py` | ~1041-1044 | WR-01 (from code review): `_write_page_doc` queries `local_files` for `file_id` before that row is inserted on first index — uses fallback `F0000` | Warning | Full_header F-suffix is `F0000` for newly-indexed files (corrected to real file_id on re-index). Non-crashing; D-34 uniqueness intent partially defeated on first index. Accepted: WR-01 deferred in 95-REVIEW.md as non-blocking warning. |
| No files | — | No TODO/FIXME/PLACEHOLDER strings found in phase 95 production files | — | Clean |

**Deferred review items (non-blocking per 95-REVIEW.md):** WR-02, WR-03, WR-04, WR-06, WR-07 (code quality / future robustness warnings), IN-01 through IN-07 (informational). None are blockers for phase goal achievement.

**Critical findings from 95-REVIEW.md — all fixed:**
- CR-01: `SearchEngine._current_lab_weights_hash` AttributeError — fixed with `getattr` safe defaults at `genizah_core.py:6772-6773` + defensive try/except at call site
- CR-02: LabEngine LOCAL LAB hook silently skipped — fixed by adding `reload_local_lab_index()` and `_check_local_lab_freshness()` to LabEngine (genizah_core.py:752-790) and calling at LabEngine.__init__ line 714
- WR-01, WR-05, WR-08: fixed per 95-REVIEW.md `fixed_items` list

---

### Human Verification Required

#### 1. LOCAL filter button full UX cycle (REQ-6 + D-39 + D-10 P1)

**Test:** Index 3+ local files. Run a search returning both Genizah and LOCAL hits. Verify filter button is visible. Click it — observe All → Only Local → No Local → All cycling. In Only Local: only LOCAL rows visible. In No Local: only Genizah rows visible. Close app, reopen, run same search — filter state persists per-surface independently.

**Expected:** Filter button visible when LOCAL hits present; hides when none; cycles correctly; QSettings persistence working; Composition Search and Parallels have independent state.

**Why human:** Qt UI state, QSettings read/write, and per-session JSON corpus_scope require a live Qt event loop.

#### 2. LOCAL double-click → ResultDialog + Open File (D-27 + D-28)

**Test:** Double-click a LOCAL search result row. Verify ResultDialog opens (not Browse panel). Verify "Open file" button present. Click it.

**Expected:** ResultDialog with Open File button; os.startfile launches OS default app for the file's extension.

**Why human:** os.startfile() requires live desktop OS integration; ResultDialog rendering is a live Qt UI concern.

#### 3. D-10 P1 NO-OP chip when no LOCAL hits in result

**Test:** Set filter state to "Only Local". Run a search that returns ZERO LOCAL hits.

**Expected:** All Genizah results shown (NO-OP); inline chip "My Library filter inactive — no LOCAL hits in this query" visible; persisted state preserved for next query.

**Why human:** Qt label visibility requires live event loop.

#### 4. Above-ceiling warning dialog (REQ-10 + D-26 + W8)

**Test:** Register folders whose aggregate file count exceeds 5,000 OR total size exceeds 2 GB. Click Refresh.

**Expected:** Modal dialog "Indexing N files (X GB) — performance may degrade. Continue?" with Yes/Cancel before scan begins.

**Why human:** Requires a large file corpus and live Qt modal dialog interaction.

#### 5. Seewald attribution in About dialog (D-32)

**Test:** Open About dialog (desktop); open Help page (web).

**Expected:** "My Library feature inspired by Yehuda Seewald's GenizahLocal prototype." in EN and Hebrew equivalent in both apps (both languages).

**Why human:** String content correctness and Hebrew rendering require visual inspection.

#### 6. Help page My Library section (D-31 + D-33)

**Test:** Open web Help page; navigate to "My Library — Local Documents" section.

**Expected:** Section present; privacy guarantee (three cloud-write gates described); cleartext-on-disk disclosure in EN and HE.

**Why human:** Web page visual rendering and Hebrew section accuracy require browser check.

---

## Gaps Summary

No automated gaps found. All 10 REQ truths pass at the code level. The code review critical findings (CR-01 + CR-02) were both fixed before this verification. The full pytest suite passes (2524 passed, 0 failures).

The 6 human verification items above are routine UI/UX checks for a new desktop feature — badge color, filter cycling, dialog interactions, and help page prose. The user has already smoke-tested Plans 95-07 and 95-08 (per the prompt: both approved), so items 1-3 above are largely re-confirmations of approved smoke tests. Items 4-6 are new checks not covered by the plan-level smoke.

---

_Verified: 2026-05-21_
_Verifier: Claude (gsd-verifier)_
