# Phase 95: My Library — Local Document Indexing — Specification

**Created:** 2026-05-21
**Ambiguity score:** 0.14 (gate: ≤ 0.20)
**Requirements:** 10 locked

## Goal

Desktop users can point GenizahSearch at a folder of `.docx` / `.pdf` / `.txt` files; those documents are indexed into a SEPARATE Tantivy side-index merged into the existing desktop search machinery at query time, so personal corpora surface inline in normal search / Composition Search / Parallels results with a clear `LOCAL` badge and a three-state filter button (`All` / `Only LOCAL` / `No LOCAL`) mirroring Phase 93's PGP-filter pattern. Personal documents NEVER leak to the cloud: hard-reject regression tests on three boundaries (`/api/search` response, `lists_sync.sync_item_to_cloud`, `corrections_client`) keep LOCAL items local-only.

## Background

Yehuda Seewald built an external prototype (`C:/Genizahsearch/seewald_addition/` — see `GenizahSearch_Local_Extension.md`) that indexes `.docx` / `.pdf` files into GenizahSearch-compatible files (`Transcriptions.txt` V0.8, 8-column `libraries.csv`, `browse_map.pkl`, `metadata_cache.pkl`) and runs them inside a SECOND parallel installation in `%USERPROFILE%\GenizahLocal\`. The prototype solved real problems (Hebrew RTL line-reversal for pdfplumber / pypdf, single-word-per-line PDF rejoining, incremental SQLite mtime cache) but the deployment model has structural issues:

- Personal sys_ids enter the SAME shared namespace as NLI / PGP / CUDL data via `99`-prefixed 12-digit IDs, indistinguishable from real Alma sys_ids that happen to start with `99`.
- Patching `Program Files\Genizah Search Pro\_internal\libraries.csv` requires UAC every refresh.
- Two installations duplicate the ~GB Genizah index and split user state.
- No isolation from `/api/search`, `lists_sync.py:763` (which currently does NOT gate synthetic sys_ids), or any other cloud-write boundary.

Codebase state today (from the codebase scout):

- Tantivy index at `genizah_core.py:5136-5189` ingests `Transcriptions.txt` V0.8 + V7 into a single shared index.
- `libraries.csv` loaded at `genizah_core.py:3386-3442` (8-column positional, `LIBRARY_CODES` at `:1723`).
- Synthetic sys_id helper `is_synthetic_sys_id()` at `shared/synthetic_sys_id.py:45-76` recognizes 18-digit `99` + 10-digit InventoryId + `"000000"` (CUDL Option-2 format).
- Desktop tabs constructed at `genizah_app.py:3079-3091` via `QTabWidget.addTab()`.
- Desktop result table column 9 is `COL_PGP` at `genizah_app.py:5910-5945` — the badge pattern to mirror.
- Composition Search / Parallels run against a separate LAB index at `Config.LAB_INDEX_DIR` rebuilt from `Transcriptions.txt` (`genizah_core.py:1292-1349`).
- Web API serializes via `shared.search_serializer.serialize_search_payload()` (`web/search_api.py:633-939`) — no LOCAL filter today (must add).
- Lists sync at `lists_sync.py:763-770` does NOT gate synthetic sys_ids (must add).
- Corrections submission at `corrections_client.py:619-623` DOES gate synthetic sys_ids today via `is_synthetic_sys_id()` — that's the model to extend.

This phase replaces Seewald's parallel-installation approach with a first-class in-app feature: new "My Library" desktop tab, side-index, isolated numeric sys_id namespace (`97`-prefixed 18-digit), three cloud-write boundary regression tests, and an opt-in three-state filter in search / Composition Search / Parallels.

## Requirements

1. **LOCAL-NAMESPACE**: LOCAL documents use a numeric sys_id namespace structurally separable from NLI / synthetic Genizah data.
   - Current: synthetic sys_ids follow `99 + 10-digit InventoryId + "000000"` (18 digits, CUDL). No namespace exists for user-private documents. Seewald's `99` + 10-digit hash collides with real Alma sys_ids.
   - Target: LOCAL sys_ids follow `97 + machine_id (8 digits) + content_hash (8 digits)` — 18 digits total, leading `97` reserved exclusively for LOCAL. `machine_id` = first 8 hex chars of `hashlib.sha256(socket.gethostname().encode()).hexdigest()` converted to decimal and zero-padded. `content_hash` = first 8 decimal digits of `hashlib.sha256(filepath.encode()).hexdigest()`. `library_code` = `"LOCAL"`.
   - Acceptance: New helper `is_local_sys_id(s) -> bool` returns True for any `97`-prefixed 18-digit string and False for every existing real or synthetic Genizah sys_id in `libraries.csv` (regression test scans the entire `libraries.csv` and asserts `is_local_sys_id` returns False for all rows). `is_synthetic_sys_id()` continues to return False for `97`-prefixed IDs (LOCAL is not "synthetic" in the existing CUDL sense — different category, distinct helper).

2. **SIDE-INDEX**: LOCAL documents live in a separate Tantivy index, NOT merged into the main `Transcriptions.txt`.
   - Current: main Tantivy index built from one `Transcriptions.txt`. Adding LOCAL via that file would force a multi-minute rebuild of the ~255K-doc shared index on every personal-library refresh.
   - Target: side-index at `%LOCALAPPDATA%\GenizahSearchPro\LocalIndex\` (or portable equivalent next to the EXE) with its own schema matching the main index (`unique_id`, `content`, `content_head`, `content_tail`, `line_starts`, `line_ends`, `source`, `full_header`, `shelfmark`, `scope`, `boundaries`). Main searcher queries both indexes and merges results before result-list rendering. The shared `Transcriptions.txt` is NEVER modified by this phase.
   - Acceptance: After indexing 10 local files and running a phrase search that matches both a NLI manuscript and a local file, the result list contains both items in the same `QTableWidget`. Force-corrupting the side-index file does NOT corrupt the main index (separate file path, separate `tantivy.Index.open()` call).

3. **FILE-TYPES**: Indexer ingests `.docx`, `.pdf`, `.txt` — and only these.
   - Current: no indexer for any file type exists in the desktop app (Seewald's lives outside the codebase).
   - Target: new module under `shared/` (e.g. `shared/local_indexer.py`) exposes `index_folder(folder_path, output_dir, progress_cb) -> IndexResult`. `.docx` via `python-docx`. `.pdf` via PyMuPDF (preferred for Hebrew) with pdfplumber / pypdf graceful fallbacks. `.txt` read with `utf-8-sig` encoding (BOM-tolerant). Other extensions ignored silently with a per-file `unsupported_extension` status row.
   - Acceptance: `tests/test_local_indexer.py` exercises each file type with a fixture (1 docx, 1 pdf, 1 txt, 1 unsupported `.html`), asserts the three supported types produce pages, the unsupported file yields a `status="unsupported_extension"` row, and no extraction exceptions propagate to the caller.

4. **RTL-EXTRACTION**: Hebrew text extracted from PDFs (when using pdfplumber / pypdf fallbacks) is in correct reading order.
   - Current: no Hebrew PDF extraction code in the desktop app. Seewald's `_fix_rtl_line` / `_fix_rtl_page` / `_join_fragmented_lines` in `seewald_addition/genizah_make_index.py:67-105` solved the visual-LTR-reversal and single-word-per-line problems.
   - Target: port `_fix_rtl_line`, `_fix_rtl_page`, `_join_fragmented_lines` verbatim into `shared/local_indexer.py` and apply ONLY when falling back to pdfplumber / pypdf. PyMuPDF (`fitz.get_text("blocks")`) is preferred and used unmodified when available.
   - Acceptance: `tests/test_local_indexer.py` includes a fixture pdfplumber-extracted page with known mirror-reversed Hebrew (≥40% RTL chars per line) and asserts the post-extraction page text matches a hand-corrected reference string. A second fixture with a single-word-per-line PDF (≥60% non-empty single-token lines) asserts the post-`_join_fragmented_lines` output joins words into paragraphs.

5. **INCREMENTAL-REINDEX**: Subsequent scans of the same folder skip unchanged files.
   - Current: no incremental cache in the codebase.
   - Target: SQLite cache `local_index.sqlite3` next to the side-index, table `processed_files(filepath TEXT PRIMARY KEY, mtime REAL, size INTEGER, sys_id TEXT)`. Scan diffs against cache: unchanged files reuse existing index rows, new / modified files re-extract, deleted files removed from the side-index. Mirrors Seewald's `processed_files` table at `seewald_addition/genizah_local_indexer.py` (and his `GenizahSearch_Local_Extension.md` lines 85-105).
   - Acceptance: Indexing a 100-file folder twice in succession completes the second scan in ≤ 5% of the first scan's wall time. Modifying one file's `mtime` (or its size) and re-scanning re-extracts only that file (asserted by a `progress_cb` mock that records extracted file paths). Deleting a file and re-scanning removes its rows from both the SQLite cache and the Tantivy side-index.

6. **THREE-STATE-FILTER**: A `LOCAL` toggle in desktop search / Composition Search / Parallels result toolbars mirrors the Phase 93 PGP-filter pattern.
   - Current: PGP filter button at `web/pages/search.py:1430-1434` and desktop sortable `COL_PGP` column at `genizah_app.py:5599-5634` set the precedent. No LOCAL filter exists.
   - Target: 3 buttons in 3 desktop surfaces (search results, Composition Search results, Parallels results) labeled `Filter LOCAL` / `Only LOCAL` / `No LOCAL`, cycling on click. Hidden until the current result set contains at least one LOCAL hit. Filter applied AFTER existing filters (printed, PGP, exclusions, refinement chain) — post-search only, no re-query. State persists across sessions via the existing session-persistence module (`shared/session_persistence`).
   - Acceptance: Manual smoke test: index 3 local files, run a search that matches one Genizah witness and one local file, click the LOCAL filter button — observe `All` → `Only LOCAL` → `No LOCAL` → `All` cycling and correct row visibility per state. Automated: `tests/test_local_filter_cascade.py` (mirrors `tests/test_pgp_filter_cascade.py`) asserts LOCAL filter is applied after PGP filter in the cascade order.

7. **RESULT-BADGE**: Each LOCAL hit in any desktop result list displays a `LOCAL` badge in the row.
   - Current: PGP badge at `genizah_app.py:5910-5945` (column 9, fixed 40px width) sets the badge pattern.
   - Target: new result-table column `COL_LOCAL` (or a single badge column that shows `LOCAL` / `PGP` / both) rendered immediately right of `COL_PGP`. Same fixed-width, same Qt resize mode. Library name column shows `LOCAL` (full name `"My Library"` via `LIBRARY_CODES`).
   - Acceptance: Manual smoke: a LOCAL hit in the result table shows a `LOCAL` badge in the badge column AND `"My Library"` in the library-name column. Automated: snapshot test of a 2-row mixed result fixture asserts row 1 (NLI) has no LOCAL badge, row 2 (LOCAL) has the badge.

8. **MY-LIBRARY-TAB**: New desktop tab `My Library` provides folder selection, indexing status, and a per-file failure panel.
   - Current: no such tab. `QTabWidget` at `genizah_app.py:3079-3091` has Search / Composition / Browse / Catalog Browse / Personal Lists.
   - Target: 6th tab added via `self.tabs.addTab(MyLibraryTab(self), "My Library")`. Tab contents: (a) folder picker (`QFileDialog.getExistingDirectory`), (b) "Index now" / "Refresh" button (manual trigger), (c) progress bar + cancellation, (d) per-file status `QTableWidget` columns `Filename | Pages | Status` populated from each scan with `OK` / `0 pages` / specific error message. Indexer runs in `QThread` so the UI stays responsive. On app start: if a folder was previously selected, auto-scan in background and update incrementally; UI shows "Last indexed: <timestamp>".
   - Acceptance: Manual smoke: select a folder containing 5 files (3 OK, 1 corrupt, 1 unsupported), click Refresh, observe progress bar advance and final status table with 3 `OK` rows, 1 specific-error row, 1 `unsupported_extension` row. Cancellation during scan leaves the side-index in a consistent state (cancellation tested with a folder containing 100 fixture files).

9. **CLOUD-WRITE-GATES**: LOCAL sys_ids are hard-rejected at all three cloud-write boundaries.
   - Current: corrections at `corrections_client.py:619-623` gates synthetic sys_ids only. `lists_sync.py:763` does NOT gate any synthetic / local sys_ids. `/api/search` serializer at `web/search_api.py:633-939` does NOT exclude LOCAL (but LOCAL won't exist in the web Tantivy index — the web has no side-index — so this is defense-in-depth).
   - Target: (a) `lists_sync.sync_item_to_cloud` short-circuits with a "local-only item, not synced" log entry and skip when `is_local_sys_id(item.sys_id)`. (b) `corrections_client` extends the existing `is_synthetic_sys_id` guard to ALSO reject `is_local_sys_id(document_id)` with code `local_corrections_disabled`. (c) `shared.search_serializer.serialize_search_payload` filters out any item whose `library_code == "LOCAL"` or whose sys_id matches `is_local_sys_id` (defense-in-depth; web Tantivy has no LOCAL but the helper is called from shared code).
   - Acceptance: Three new regression tests pin the invariant: `tests/test_local_namespace_no_api_leak.py` constructs a search payload with one LOCAL row injected and asserts the serializer drops it. `tests/test_local_namespace_no_lists_leak.py` calls `sync_item_to_cloud` with a LOCAL sys_id and asserts no Supabase call is made (mock the client) AND the function returns a skip status. `tests/test_local_namespace_no_corrections_leak.py` calls the corrections submit path with a LOCAL document_id and asserts a `local_corrections_disabled` error is returned without an HTTP call.

10. **SCALE-CEILING**: Indexer handles up to 5,000 files / 2 GB total text without UI freeze or memory blow-up.
    - Current: no scale guarantees.
    - Target: indexing uses a streaming approach — extracted text written to Tantivy + SQLite immediately per file, no full-corpus accumulation in memory. Progress bar updates per file. UI thread never blocks (all extraction in worker `QThread`). Above ceiling: a one-time warning dialog "Indexing more than 5,000 files — performance may degrade. Continue?" with `Yes` / `Cancel`. The indexer still attempts the scan if `Yes`.
    - Acceptance: Synthetic test fixture with 5,000 small `.txt` files (each 1 KB) completes indexing in ≤ 10 minutes on the developer's machine, with peak RSS under 500 MB. UI remains interactive (the test verifies the main thread accepts a `QApplication.processEvents()` round-trip within 100 ms during scan).

## Boundaries

**In scope:**

- New `shared/local_indexer.py` module with folder-scan + extraction (`.docx` / `.pdf` / `.txt`) + RTL fixes + single-word-per-line rejoin + SQLite mtime cache + side-index builder.
- New helper `is_local_sys_id(s) -> bool` (new file or extension of `shared/synthetic_sys_id.py`) recognizing 18-digit `97`-prefixed sys_ids.
- New `MyLibraryTab` widget in `desktop/` package, registered as the 6th tab in the desktop app's `QTabWidget`.
- Tantivy side-index opened alongside the main index; result-merger logic in `genizah_core.py` or `desktop/` that queries both indexes and combines hits.
- `COL_LOCAL` badge (or unified badge column) on desktop search / Composition Search / Parallels result tables.
- Three-state `LOCAL` filter button on desktop search / Composition Search / Parallels result toolbars, cycling `All` / `Only LOCAL` / `No LOCAL`, hidden until LOCAL hits exist in the current result set, persisted across sessions.
- Three hard-reject regression tests on cloud-write boundaries (`/api/search` serializer, `lists_sync.sync_item_to_cloud`, `corrections_client`).
- Per-file status panel showing OK / 0-pages / specific-error rows after each scan.
- Auto-detect at app start + manual `Refresh` button. Cancellation during scan.
- Scale ceiling: 5,000 files / 2 GB with above-ceiling warning dialog.

**Out of scope:**

- **Web app integration** — feature is desktop-only. The web Tantivy index never contains LOCAL data. — Reason: privacy (web users would have to upload files; that's a different feature with different privacy model).
- **OCR for image-only PDFs** — requires Tesseract dependency, ~1-5s per page, Hebrew OCR quality is mixed. — Reason: significant scope and binary-distribution complexity; ship without it, add as backlog later.
- **`.epub`, `.md`, `.html`, `.rtf`, `.doc` (legacy Word)** — unsupported file types are silently rejected with status rows. — Reason: keep dependency surface tight; add to a follow-up phase based on user demand.
- **Cloud-synced Lists containing LOCAL items** — Lists for LOCAL stays OUT of v1. — Reason: filename-as-title leaks personal library contents to anyone the user shares a list with; needs careful privacy design. Backlog item: "Local-only Lists (no Supabase) for LOCAL items".
- **Multi-machine sync of the local index** — index is per-machine, tied to a `machine_id` salt in the sys_id. — Reason: cross-machine sync of personal files is a separate problem; out of scope.
- **Watching the folder live with QFileSystemWatcher** — only auto-scan at app start + manual Refresh. — Reason: real-time watching has edge cases (network drives, locked files, transient mid-write states) that aren't worth the complexity for v1.
- **Modification of the shared `Transcriptions.txt` or `libraries.csv`** — side-index is fully separate. — Reason: keeps the shared corpus immutable from the user's perspective; rules out the Seewald-prototype UAC-patching path entirely.
- **Upgrade path for users who already ran Seewald's prototype** — their `GenizahLocal\` directory is left intact and ignored. — Reason: their data lives in a separate installation; nothing in v1 reads it. If they want their files in v1, they re-index via the new UI. (Backlog item: optional "Import from Seewald prototype" button if demand surfaces.)
- **Indexing per-user-account (multiple Windows users on the same machine)** — `machine_id` derives from `socket.gethostname()`, so two Windows accounts on the same machine share a `machine_id` but have separate `%LOCALAPPDATA%` paths and thus separate side-indexes. — Reason: keeps namespace logic simple; per-user-account separation comes from the OS user profile path.
- **Editing / annotating LOCAL documents from inside GenizahSearch** — view text + basic browse only. — Reason: GenizahSearch is a search/browse tool, not a document editor.
- **Browser extension support for LOCAL documents** — extension stays Genizah-only. — Reason: nothing to do with the local-indexing problem.

## Constraints

- Side-index Tantivy schema MUST match the main index schema (`unique_id`, `content`, `content_head`, `content_tail`, `line_starts`, `line_ends`, `source`, `full_header`, `shelfmark`, `scope`, `boundaries`) so the result-merger can treat both as homogeneous result lists. Source code reference: `genizah_core.py:5136-5189`.
- `python-docx` and `pymupdf` are NEW desktop dependencies (PyMuPDF is preferred for Hebrew; `pdfplumber` and `pypdf` are not required if PyMuPDF is the chosen extractor — pin choice in discuss-phase). `python-docx` ~2 MB, `pymupdf` ~25 MB — installer size impact must be acknowledged.
- Indexing MUST run in a `QThread` worker; the desktop UI thread MUST stay responsive (acceptance criterion in REQ-10 pins the 100 ms `processEvents` round-trip).
- Three-state filter MUST be applied AFTER existing filters (printed, PGP, exclusions, refinement chain) — post-search render-time only, no re-query of Tantivy.
- LOCAL `library_code` MUST be added to `LIBRARY_CODES` at `genizah_core.py:1723` with display name `"My Library"` so the existing library-display infrastructure works without special-casing.
- All sys_id namespace gates MUST use the new helper `is_local_sys_id(s)` rather than ad-hoc string checks — single source of truth for the namespace boundary.
- The shared `Transcriptions.txt` and `libraries.csv` files are READ-ONLY from this phase's perspective. No write path may modify them. (Defense against the Seewald-prototype UAC-patching pattern.)

## Acceptance Criteria

- [ ] `is_local_sys_id(s)` returns True for any 18-digit `97`-prefixed string and False for every row in the current `libraries.csv` (`tests/test_local_sys_id_namespace.py`).
- [ ] `is_synthetic_sys_id(s)` returns False for every `97`-prefixed string (regression test pinning the two namespaces are disjoint).
- [ ] After indexing 10 local files and running a phrase search hitting both a NLI manuscript and a local file, the result table contains both rows (manual smoke + `tests/test_side_index_merge.py`).
- [ ] Corrupting the side-index file does NOT corrupt the main Tantivy index (manual smoke).
- [ ] Indexer handles `.docx`, `.pdf`, `.txt` and yields `status="unsupported_extension"` for any other extension (`tests/test_local_indexer.py`).
- [ ] Mirror-reversed Hebrew PDF text (pdfplumber fallback) is rendered in correct reading order (`tests/test_local_indexer.py`).
- [ ] Single-word-per-line PDF (≥60% threshold) gets paragraphs rejoined (`tests/test_local_indexer.py`).
- [ ] Second scan of an unchanged 100-file folder completes in ≤ 5% of first-scan wall time (`tests/test_local_indexer_incremental.py`).
- [ ] Modifying one file's `mtime` and re-scanning re-extracts only that file (`tests/test_local_indexer_incremental.py`).
- [ ] Deleting a file and re-scanning removes its rows from both the SQLite cache and the Tantivy side-index (`tests/test_local_indexer_incremental.py`).
- [ ] LOCAL three-state filter button is hidden when no LOCAL hits in current result set; cycles `All` → `Only LOCAL` → `No LOCAL` → `All` on click; state persists across desktop sessions (manual smoke).
- [ ] LOCAL filter is applied AFTER PGP filter in the render cascade (`tests/test_local_filter_cascade.py`).
- [ ] Each LOCAL hit in any desktop result list shows a `LOCAL` badge and `"My Library"` in the library-name column (snapshot test).
- [ ] My Library tab is the 6th tab in the desktop `QTabWidget`; folder picker opens, Refresh triggers a scan, progress bar advances, per-file status table populates with OK / 0-pages / error rows (manual smoke).
- [ ] Cancellation during scan leaves the side-index in a consistent state — no orphaned rows, no corrupt SQLite (manual smoke with 100-file fixture).
- [ ] `tests/test_local_namespace_no_api_leak.py`: serializer drops any LOCAL row from the `/api/search` payload.
- [ ] `tests/test_local_namespace_no_lists_leak.py`: `sync_item_to_cloud` with a LOCAL sys_id makes no Supabase call and returns a skip status.
- [ ] `tests/test_local_namespace_no_corrections_leak.py`: corrections submit with a LOCAL `document_id` returns `local_corrections_disabled` without an HTTP call.
- [ ] Synthetic 5,000-file fixture completes indexing in ≤ 10 minutes on the developer's machine with peak RSS under 500 MB; UI thread accepts `QApplication.processEvents()` round-trips within 100 ms during scan (`tests/test_local_indexer_scale.py`, gated `@pytest.mark.slow`).
- [ ] Above-ceiling (>5,000 files) one-time warning dialog appears with `Yes` / `Cancel` choices (manual smoke).
- [ ] `LIBRARY_CODES` at `genizah_core.py:1723` includes `"LOCAL": "My Library"`.
- [ ] No write path under `web/` or `shared/` modifies `Transcriptions.txt` or `libraries.csv` as part of this phase (static grep guard or AST check).

## Ambiguity Report

| Dimension          | Score | Min  | Status | Notes                                                                            |
|--------------------|-------|------|--------|----------------------------------------------------------------------------------|
| Goal Clarity       | 0.88  | 0.75 | ✓      | File types, opt-in UX, namespace, scale ceiling all locked                       |
| Boundary Clarity   | 0.90  | 0.70 | ✓      | Lists OUT of v1 (backlog); web OUT; OCR OUT; upgrade path OUT; multi-machine OUT |
| Constraint Clarity | 0.85  | 0.65 | ✓      | Schema match, QThread, post-search filter, LIBRARY_CODES extension, read-only shared corpus |
| Acceptance Criteria| 0.80  | 0.70 | ✓      | 22 pass/fail checkboxes spanning indexing, UI, scale, three namespace gates       |
| **Ambiguity**      | 0.14  | ≤0.20| ✓      | Gate passed after 4 rounds                                                       |

Status: ✓ = met minimum, ⚠ = below minimum (planner treats as assumption)

## Interview Log

| Round | Perspective       | Question summary                                            | Decision locked                                                                       |
|-------|-------------------|-------------------------------------------------------------|---------------------------------------------------------------------------------------|
| 1     | Researcher        | File types for v1?                                          | `.docx` + `.pdf` + `.txt` — no OCR, no `.md`, no `.epub`                              |
| 1     | Researcher        | How does user opt LOCAL in for a search?                    | Three-state filter `All` / `Only LOCAL` / `No LOCAL` mirroring Phase 93 PGP pattern   |
| 2     | Researcher        | Index layout — shared file or side-index?                   | Side-index merged at query time (separate Tantivy index)                              |
| 2     | Simplifier        | sys_id namespace?                                           | `97` + 8-digit machine_id + 8-digit content_hash (18 digits, numeric, distinct from CUDL's 99-prefix) |
| 3     | Boundary Keeper   | Cloud-write gates (API / Lists / corrections)?              | User opened the Lists door — deferred clarification to round 4                        |
| 3     | Boundary Keeper   | Reindex model (auto / manual / live-watch)?                 | Auto-detect at app start + manual Refresh button (incremental via Seewald's SQLite mtime cache) |
| 3     | Boundary Keeper   | Failure visibility for corrupt PDFs?                        | Per-file status panel on My Library tab (Filename / Pages / Status)                   |
| 4     | Failure Analyst   | Lists for LOCAL — in or out of v1?                          | OUT of v1; backlog item for local-only Lists (no Supabase) later                      |
| 4     | Failure Analyst   | Scale ceiling for graceful handling?                        | 5,000 files / 2 GB with above-ceiling warning dialog                                  |

---

*Phase: 95-my-library*
*Spec created: 2026-05-21*
*Inspired by: Yehuda Seewald's external prototype at `seewald_addition/` — credit due in About / Help on ship*
*Next step: /gsd-discuss-phase 95 — implementation decisions (PDF extractor pin, exact schema field reuse, result-merger algorithm, QThread lifecycle, settings storage location)*
