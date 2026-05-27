# Phase 97: More LOCAL features - Context

**Gathered:** 2026-05-25
**Status:** Ready for planning
**Codex critique:** `97-CODEX-CRITIQUE.md` — folded into decisions below (all P0 + most P1).
**Trigger:** Yehuda Seewald email 2026-05-25 comparing v7.14.0 to his external prototype (13K files / 43 GB). His own recommendation to readers: use v7.14.0 only if < 5K files / 2 GB; otherwise his prototype. Phase 97 closes that gap.

<domain>
## Phase Boundary

Make My Library usable at the scale Seewald's prototype already serves (13K files / 43 GB, target ceiling 50K / 50 GB) by adding crash-recovery semantics, durable text cache, and atomic Tantivy rebuild — and extend the file-format set with three light textual formats (`.html` / `.xlsx` / `.csv`) commonly distributed as synopsis tables in Hebrew scholarly contexts. This phase does NOT add reading-experience features (OCR, side-by-side PDF, more formats) and does NOT touch web LOCAL exposure — those remain deferred to v7.15+.

</domain>

<decisions>
## Implementation Decisions

### Capacity Track

- **C-01 — Ceiling:** No hard cap. Soft warning at 50K files OR 50 GB, non-blocking ("Proceed / Cancel"). Replaces Phase 95's 5K/2GB hard-stop.
  - **[Codex sequencing — P0]** C-01 lands AFTER R-03 (text cache), R-02 (atomic recovery), and the SQLite migration plan (D-NEW-1). See "Sequencing" subsection below. Lifting the ceiling before recovery exists exposes the first 13K-file users to expensive or impossible recovery — exactly the opposite of the phase's intent.

- **C-02 — Commit policy:** Combined heap + count + bytes + time policy.
  - **[Codex revision — P0]** The actual tantivy-py API is `index.writer(heap_size=N)`, not `with_index_writer_heap_size`. More importantly, heap size is not a durability boundary. Commit policy: commit when **any** of these is reached:
    - Writer heap reaches 256 MB (sampled via `writer.get_memory_usage()` if available; else conservative file/byte fallback), OR
    - 100 files processed in this batch, OR
    - 200 MB of source bytes processed in this batch, OR
    - 60 seconds elapsed since last commit
  - Pin via integration test: assert a commit fires within the window on a synthetic mixed-size corpus. Replaces Phase 95's fixed 25-file batch.

- **C-03 — Pre-scan dialog:** Walk folder tree first (filesystem metadata only — no extraction). Compute file count + total bytes + ETA. Show non-blocking dialog ONLY when result exceeds 50K files OR 50 GB. Otherwise begin indexing immediately.
  - **[Codex revision — P1]** Folder walk runs in a worker thread; only filesystem `os.stat` / `os.walk` happens off-UI. Walk emits `pyqtSignal(list)` batches of file-metadata records; UI thread renders into the status panel via batched signals. **NO Qt widget mutation from the worker.**

- **C-04 — Status panel UI:** Aggregate-by-folder view default (Indexed / Errors / Pending / Oversized counts per folder). Click a folder row → drill-down detail with per-file rows for that folder only.
  - **[Codex revision — P2]** Folder-aggregate counters are **persisted** in SQLite (new columns on the `folders` table: `indexed_count`, `error_count`, `pending_count`, `oversized_count`, `last_aggregate_at`). Counters updated incrementally on each commit batch. Aggregate view reads counters in O(1) per folder — does NOT scan all file rows on every UI refresh.

- **C-05 — Per-file size cap + zip-bomb limits:** 100 MB hard skip with `status='oversized'` for raw file size.
  - **[Codex revision — P1]** Zip-container limits for `.docx` / `.xlsx`: max uncompressed bytes = 500 MB (sum across all parts), max cells/rows per sheet = 100K, max chars per chunk = 1 MB. Trigger `status='zip_bomb_suspected'` on breach + log warning. Per Phase 95 D-21 pattern: no Tantivy doc emitted for these.

- **C-06 — Disk-usage surface + merge headroom:** Live indicator in MyLibraryTab ("Index size: 4.2 GB / 1.1 TB free").
  - **[Codex revision — P1]** Warning threshold accounts for Tantivy segment-merge scratch space. Reserve = 2× current index size as merge headroom. Warning fires when `(free_space - estimated_growth - 2×index_size) < 1 GB`. Plain "80% utilized" check is misleading because mid-indexing merges can spike disk use sharply.

### Crash Recovery Track

- **R-01 — Recovery UX:** On next launch, if SQLite cache has pending-status rows OR the previous scan_run did not end with a clean shutdown marker, modal prompt at MyLibraryTab open: "Previous indexing interrupted — Resume / Restart / Skip?"
  - **[Codex revision — P0]** LOCAL search is **gated** until the prompt is resolved. Between app open and user picking Resume/Restart/Skip, `MyLibraryTab.is_searchable = False` — search queries on LOCAL return zero hits with a banner: "My Library recovery pending — resolve before searching." Prevents the inconsistency window where docs committed to Tantivy but not yet marked `committed` in SQLite are queryable while still "pending."

- **R-02 — Tantivy corruption recovery (atomic):** On startup, attempt `index.searcher()` on LOCAL Tantivy index. If it raises, trigger atomic rebuild.
  - **[Codex revision — P0]** Rebuild is **atomic via temp-dir swap**, not in-place:
    1. Build fresh LOCAL Tantivy index in `<LOCAL_INDEX_DIR>.rebuild-<scan_run_id>/`
    2. Walk SQLite `processed_files` WHERE `status='committed'`, re-index from `cached_text` (no source re-extraction)
    3. Validate via `index.searcher()`
    4. Close live readers/writers on the old index
    5. `os.rename(LOCAL_INDEX_DIR, LOCAL_INDEX_DIR + ".old-<timestamp>")`
    6. `os.rename(<LOCAL_INDEX_DIR>.rebuild-<id>, LOCAL_INDEX_DIR)`
    7. Delete `.old-` directory on next clean shutdown
  - Same protocol for `LOCAL_LAB_INDEX_DIR`. Pin via test that simulates Tantivy corruption + asserts rebuild fires + old index removed.

- **R-03 — Text cache:** Add cached extracted text per Tantivy doc (page-level) to SQLite, zstd-compressed.
  - **[Codex revision — P0]** SQLite schema additions need a real migration plan — see D-NEW-1. New columns on existing `local_pages` table (or a new sidecar table if locking concerns):
    - `cached_text BLOB` — zstd-compressed text
    - `cached_text_codec TEXT NOT NULL DEFAULT 'zstd'`
    - `cached_text_uncompressed_len INTEGER`
    - `extraction_format_version INTEGER NOT NULL DEFAULT 1`
  - Estimated footprint at 13K files: ~400 MB compressed (3-4x compression on Hebrew/English text). Also speeds View Page rendering.

- **R-04 — SQLite durability discipline:** Enable WAL mode.
  - **[Codex revision — P1]** Default `synchronous=NORMAL` for routine batch updates. For the critical `pending → committed` transition (the inner UPDATE that closes Phase 95's two-phase commit), explicitly call `PRAGMA synchronous=FULL` around the UPDATE OR use `PRAGMA wal_checkpoint(TRUNCATE)` to force a durable checkpoint before continuing to the next batch. WAL + NORMAL alone can lose the last transaction on power loss — unacceptable for the gate between "Tantivy committed" and "SQLite marks committed."

### Format Extraction Track

- **F-01 — HTML chunking:** Split at semantic boundaries (h1/h2 elements). Fallback to 20-paragraph chunks if h1/h2 sparse.
  - **["Sparse" defined]** Sparse = fewer than 3 `h1`/`h2` elements in document OR average inter-heading paragraph count below 5. Planner can adjust the heuristic based on smoke tests against a corpus of scholarly Hebrew HTML.
  - **[Codex revision — P1]** HTML encoding: read raw bytes, detect via `<meta charset>` declaration; fall back to chardet on the bytes; final fallback to cp1255. Use `BeautifulSoup` with the `lxml` parser (faster + more lenient than `html.parser` on malformed pages). Strip `<script>`/`<style>` content before chunking.

- **F-02 — XLSX chunking (per-sheet AND per-row-window):**
  - **[Codex revision — P1]** One Tantivy doc per `(sheet, row-window)` where row-window = 500 rows. Small sheet (< 500 rows) = single doc. Large sheet (10K rows) = 20 docs. Open via `openpyxl.load_workbook(read_only=True, data_only=True)` to avoid loading the entire workbook into memory.

- **F-03 — CSV chunking:** Per-200-rows windows.

- **F-04 — Header-row handling (uniform extraction):** No header-row assumption. Every row extracted uniformly as `cell1 | cell2 | cell3 | ...` joined text. Survives synopses, headerless data, pivoted tables, multi-row headers.

- **F-05 — CSV encoding chain:**
  - **[Codex revision — P1]** Try in order: utf-8-sig (BOM-tolerant) → cp1255 → utf-16-le (Excel default for non-ASCII). Delimiter detection via `csv.Sniffer().sniff(sample)` over first 4 KB; supports `,` / `;` (European/Excel) / `\t`. On total decode failure, mark `status='encoding_error'` + log warning.

- **F-06 — RTL: metadata-only, NO text reversal:**
  - **[Codex revision — P0]** Reverse direction from the original draft. BeautifulSoup, openpyxl, and Python `csv` all produce **logical-order strings already** — applying Phase 95's `_fix_rtl_line` / `_fix_rtl_page` (designed for PDF mirror-reversal) to HTML/XLSX/CSV will **corrupt already-correct Hebrew**. Honor `dir="rtl"` (HTML) / `sheetView.rightToLeft=True` (XLSX) as **display metadata only** — flag the chunk for RTL rendering in the Browse panel, but do NOT mutate the extracted string. Pin via test fixture: a logical-order Hebrew HTML doc must round-trip unchanged through extraction.

### Indexing UX at Scale

- **U-01 — ETA (hybrid + phase-aware):** Bytes-based ETA + file count both displayed.
  - **[Codex revision — P2]** Phase-aware ETA — separate sub-progress for: (a) folder walking, (b) extracting, (c) committing, (d) rebuilding LAB. Each phase has its own bytes/sec smoothing. The overall ETA composes the four. Avoids "12 minutes remaining" mis-estimating when the corpus has many cached skips (instant) interleaved with new PyMuPDF extractions (slow).

- **U-02 — Cancel semantics (with scan_run_id):** Cancel triggers confirmation prompt: "Discard everything indexed in this run, or keep partial library + stop?"
  - **[Codex revision — P0]** Run-level transaction semantics via `scan_run_id`:
    - Every Tantivy doc emitted in this run carries a `scan_run_id` field (UUID, indexed)
    - Every SQLite row touched in this run records the same `scan_run_id`
    - **Discard:** `writer.delete_documents(Term("scan_run_id", run_id))` + commit + `DELETE FROM processed_files WHERE scan_run_id = ?`
    - **Keep:** explicit `writer.commit()` of pending writer state + leave run_id intact for future audit/cleanup
  - Without this, "Discard" cannot reliably undo work spread across multiple committed batches.

- **U-03 — Folder walk threading:**
  - **[Codex revision — P1]** Folder walk in QThread (closes D-F9). Status panel updates throttled to once per 100 files OR 0.5 sec. **Critically:** widget item creation/update stays on the UI thread via batched `pyqtSignal(list)` — the worker thread emits signals carrying file-metadata batches; the slot on the UI thread materializes `QTreeWidgetItem`s. No widget mutation from the worker. At 100K files, prefer the persisted-counter aggregate view (C-04) over materializing 100K `QTreeWidgetItem`s.

- **U-04 — View All cap (raised + incremental):**
  - **[Codex revision — P2]** Raise cap from 200 to 500 pages. AVOID intentionally freezing the main thread for 30 sec — use the new `cached_text` (R-03) plus incremental rendering: render first 50 pages immediately, append remaining pages in batches via `QTimer.singleShot(0, append_next_batch)` so the event loop stays responsive. Full background-thread refactor (D-F7) still deferred.

### New decisions (closing Codex gaps)

- **D-NEW-1 — SQLite schema migration plan:** Use `PRAGMA user_version`. Phase 95 baseline = user_version 1. Phase 97 target = user_version 2.
  - Migration script in `shared/local_indexer_migrations.py`, runs at first MyLibraryTab open after upgrade
  - Migration 1→2 adds: `cached_text BLOB`, `cached_text_codec`, `cached_text_uncompressed_len`, `extraction_format_version`, `scan_run_id`, `mtime_ns INTEGER`, `chunk_locator TEXT`
  - Also adds: `folders.indexed_count` / `error_count` / `pending_count` / `oversized_count` / `last_aggregate_at` (C-04)
  - Test fixtures: (a) empty DB (fresh install), (b) v1 DB with Phase 95 data (Hillel's actual upgrade scenario), (c) v2 DB (no-op migration)
  - Backfill behavior: existing v1 rows get `cached_text=NULL`, `extraction_format_version=1`; rebuild path (R-02) accepts NULL by falling back to source re-extraction for those rows only
  - On `PRAGMA integrity_check` failure: surface error, do NOT auto-delete; require manual intervention via a dedicated "Reset My Library" button in advanced settings

- **D-NEW-2 — Network drive semantics:**
  - Unavailable folder (network path returns ENOENT/ETIMEDOUT at scan): `folders.status='unreachable'`; startup auto-rescan SKIPS unreachable folders (no blocking on slow shares)
  - Transient timeout (mid-scan): retry 3× with 2s backoff, then mark folder `status='timeout'`, finish other folders normally
  - User can manually trigger Refresh on an unreachable folder; if it succeeds, status flips back to `active`
  - Tests: mock `os.walk` raising `OSError(ETIMEDOUT)` and assert backoff + status transition

- **D-NEW-3 — File-change-during-index handling:**
  - `os.stat` BEFORE extraction; record `mtime_ns` + `size`
  - `os.stat` AFTER extraction; if `mtime_ns` OR `size` changed, mark `status='changed_during_index'` and re-queue
  - Bound retry: max 3 retries per file per scan_run; on 4th attempt, give up and log

- **D-NEW-4 — Supported-file scope (SQLite row policy):** SQLite rows only created for files matching `(extension IN supported OR status IN ('oversized', 'error', 'changed_during_index', 'zip_bomb_suspected'))`. Unsupported file types are NOT stored as rows. Prevents SQLite/status-panel bloat at 100K+ file trees where most files are images/binaries.
  - Existing Phase 95 behavior (record all walked files) is migrated: 1→2 migration deletes rows where `extension NOT IN supported AND status IS NULL`.

- **D-NEW-5 — Format-specific chunk locators for Browse:** Every Tantivy doc carries a `chunk_locator TEXT` field for human-readable display alongside the existing integer `p_num`:
  - PDF: `p. N` (e.g., `p. 42`)
  - DOCX: `¶ N-M` (e.g., `¶ 1-20`)
  - HTML: `§ <heading>` when chunked semantically, else `¶ N-M`
  - XLSX: `<sheet>!R<n>:R<m>` (e.g., `Synopsis!R1:R500`)
  - CSV: `rows N-M` (e.g., `rows 1-200`)
  - `p_num` integer still emitted for compatibility with existing nav widgets (Phase 96 96-08 NEW-2). `chunk_locator` shown in result snippet headers + Browse breadcrumb.

- **D-NEW-6 — Privacy disclosure (Help/About update):** Help page + About dialog updated bilingual EN+HE to disclose that LOCAL indexing now stores compressed cleartext of source files in `<INDEX_DIR>/local_index.sqlite3`. Discloses zstd compression (not encryption), location, and that the cache is per-machine (never uploaded). Mirrors Phase 95 D-33 cleartext disclosure language pattern.

- **D-NEW-7 — Phase 95/96 invariant regression tests:** New file `tests/test_phase_97_invariants.py` exercises:
  - (a) Three cloud-write gates still at TOP of `shared/search_serializer.py`, `corrections_client.py`, `lists_sync.{sync_item_to_cloud, sync_list_to_cloud}` — AST scanner
  - (b) Web LOCAL filtering: web LIBRARY_CODES allowlist still `[]` — AST scanner
  - (c) `is_local_sys_id()` recognizes 18-digit 97-prefixed sys_ids
  - (d) LOCAL RRF merge happens POST-`_deduplicate()` (Phase 95 D-08 P0): add LOCAL hit BEFORE `_deduplicate()` → asserts it gets dropped; add AFTER → asserts it survives
  - All four are CI guards, fail-fast.

- **D-NEW-8 — Incremental audit upgrade:** Move from float `mtime` to `st_mtime_ns` for change detection.
  - **[Codex revision — P1]** Store `mtime_ns INTEGER` alongside existing `size`. At scale / network drives the float tolerance can miss rapid edits. For same-size + same-mtime_ns rows where the file actually changed (rare, but possible if external tools clobber mtime), opt-in cheap hash of first + last 64 KB. Document as advanced setting.

### Sequencing (per Codex Overall Assessment)

Codex's strongest recommendation: ship recovery foundation BEFORE lifting the ceiling. Reordered plan waves:

- **Wave A — Recovery foundation:**
  - D-NEW-1 SQLite migration to user_version=2
  - R-03 cached_text + extraction_format_version
  - R-02 atomic Tantivy rebuild (temp-dir swap)
  - R-04 WAL + FULL on critical pending→committed transition
  - R-01 recovery UX + LOCAL-search-gated-during-recovery

- **Wave B — Commit policy + container safety:**
  - C-02 heap+count+bytes+time commit policy
  - C-05 100 MB cap + zip-bomb limits
  - D-NEW-8 mtime_ns incremental audit

- **Wave C — Format extraction:**
  - F-01 HTML chunking (semantic + fallback)
  - F-02 XLSX chunking (per-sheet + per-row-window)
  - F-03 CSV chunking
  - F-04 uniform row extraction
  - F-05 CSV encoding chain
  - F-06 RTL metadata-only (NO text reversal)

- **Wave D — Capacity UX:**
  - C-01 ceiling lift (50K/50GB soft warning)
  - C-03 pre-scan dialog with worker-thread walk
  - C-04 status panel + persisted counters
  - C-06 disk indicator + merge headroom

- **Wave E — Indexing UX at scale:**
  - U-01 phase-aware ETA
  - U-02 scan_run_id + Cancel discard/keep
  - U-03 folder-walk QThread + throttled batched signals
  - U-04 View All raised to 500 + incremental rendering

- **Wave F — Gap closure + privacy + tests:**
  - D-NEW-2 network drive semantics
  - D-NEW-3 file-change-during-index
  - D-NEW-4 supported-file scope (SQLite row policy)
  - D-NEW-5 chunk_locator per format
  - D-NEW-6 privacy disclosure (Help + About bilingual)
  - D-NEW-7 Phase 95/96 invariant regression tests

Waves A and B are foundation. Wave C parallelizable to Wave B. Wave D requires Wave A. Wave E builds on Wave D. Wave F closes gaps and can interleave with D/E.

### Claude's Discretion

- F-01 "sparse" threshold heuristic — planner can adjust based on smoke tests
- Wave-internal ordering of parallelizable items — planner can re-order
- Specific zstd compression level (1-22) for `cached_text` — planner picks based on benchmark (3-5 typical)
- Worker thread count for parallel extraction in Wave D — planner picks based on CPU detection
- Exact split between rebuild-from-cache vs rebuild-from-source in R-02 fallback — planner picks based on cache hit rate

### Folded Todos

None — todo matcher surfaced weak matches but none were LOCAL-relevant.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 95/96 invariants (binding for Phase 97)
- `.planning/phases/95-my-library/95-SPEC.md` — Phase 95 locked requirements still binding
- `.planning/phases/95-my-library/95-CONTEXT.md` — D-01..D-46, especially:
  - D-08 (RRF k=60 POST-`_deduplicate()`)
  - D-19 (sys_id derivation `% 10**8`, hostname-based machine_id)
  - D-21 (two-phase commit: SQLite pending → Tantivy commit → SQLite committed)
  - D-30 (cloud-write gates at TOP of 3 modules)
  - D-32 (cancellation pattern)
  - D-33 (cleartext disclosure language pattern — Phase 97 D-NEW-6 mirrors this)
- `.planning/phases/96-completing-my-library-feature-add-features-and-fix-bugs/96-CONTEXT.md` — Phase 96 decisions including D-F4 PyMuPDF `get_text("dict")` block-join
- `docs/OPEN_ISSUES.md:458-475` — Deferred items D-F1..D-F11 list; Phase 97 partially closes D-F7 (raises 200→500 cap, not full QThread) and D-F9 (folder walk to QThread)
- `docs/guides/MULTITENANT.md` — Web multitenant invariant (web LIBRARY_CODES `[]`, allowlist `[]`)

### Code surfaces touched
- `shared/local_indexer.py` — PRIMARY surface (extraction, SQLite cache, Tantivy writer, sys_id derivation)
- `genizah_app.py` — MyLibraryTab UI, status panel, View All (specifically `_VIEW_ALL_PAGE_CAP`, `_aggregate_local_pages_with_separators`, `_mark_blocks_for_pages`, `_open_local_browse_page`, `load_local_page`)
- `desktop/my_library_tab.py` — MyLibraryTab implementation, `_UnifiedFileTreeWidget`
- `desktop/result_dialog.py` — `load_local_page`, `_open_local_browse` LOCAL hit path
- `genizah_core.py:7390` — `_deduplicate()` call site (POST-RRF merge invariant)
- `genizah_core.py:7916-7921` — `_deduplicate()` whitelist behavior unchanged
- `genizah_core.py:1723` — `LIBRARY_CODES` (LOCAL entry from Phase 95)
- `genizah_core.py:742-790` — `rebuild_lab_index()` (LAB schema; LOCAL_LAB_INDEX_DIR shares this schema)
- `genizah_core.py:1292-1349` — `lab_composition_search()` (custom scoring path; LOCAL lab hits flow through)
- `shared/search_serializer.py` — cloud-write gate (TOP of file)
- `corrections_client.py` — cloud-write gate (TOP of file)
- `lists_sync.py` — `sync_item_to_cloud` + `sync_list_to_cloud` cloud-write gates (TOP of each)

### Phase 97 artifacts (this directory)
- `97-CODEX-BRIEF.md` — brief sent to Codex
- `97-CODEX-CRITIQUE.md` — Codex feedback (folded into decisions above)
- `97-CODEX-CRITIQUE-raw.txt` — raw transcript

### Email reference (off-repo)
- Yehuda Seewald email 2026-05-25 — primary trigger; comparing v7.14.0 to his 13K-file / 43-GB prototype. Not stored in repo.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared/local_indexer.py` Phase 95 infrastructure: folder scan, mtime cache, sys_id derivation, status panel signals, cancellation, two-phase commit
- `_UnifiedFileTreeWidget` (Phase 96 96-06): 3-column tree, ready for D-NEW-4 supported-file-scope filtering at row level
- Two-phase commit (Phase 95 D-21): SQLite `processed_files.status='pending'` → Tantivy commit → SQLite UPDATE `status='committed'` — extended by Phase 97 R-04 (FULL durability on the inner UPDATE) and U-02 (`scan_run_id`)
- RRF k=60 fusion (Phase 95 D-08): POST-`_deduplicate()`, tested via `tests/test_local_post_dedup_merge.py` — D-NEW-7 adds Phase-97-specific regression
- `is_local_sys_id()` helper: 18-digit `97`-prefixed sys_ids (Phase 95 D-19)
- Cloud-write gates (Phase 95 D-30): at TOP of 3 modules; D-NEW-7 reasserts as Phase 97 regression
- PyMuPDF `get_text("dict")` block-level join (Phase 96 D-F4 fix) — unchanged in Phase 97
- `_VIEW_ALL_PAGE_CAP` constant (Phase 96 D-F7): 200 → 500 in U-04 (with incremental rendering)
- Persisted UI state via `flush_pending()` in closeEvent (Phase 96 96-09 iter-8)

### Established Patterns
- QThread for indexing (Phase 95) — extended by U-03 to folder walk
- Per-file status panel via batched `pyqtSignal` (Phase 95) — U-03 throttles updates
- Cloud-write gates at TOP of modules (Phase 95 D-30) — D-NEW-7 invariant
- LOCAL test fixtures in `tests/fixtures/local_indexer/`
- AST-based pytest lint scanners as permanent CI guards (Phase 87 pattern) — D-NEW-7 follows
- Bilingual EN+HE Help / About text pattern (Phase 95 D-31..D-33)

### Integration Points
- `Config.LOCAL_INDEX_DIR` (Phase 95 D-14): `os.path.join(INDEX_DIR, "LocalIndex")` — R-02 atomic rebuild swaps this
- `Config.LOCAL_LAB_INDEX_DIR` (Phase 95 D-14): `os.path.join(INDEX_DIR, "LocalLabIndex")` — same atomic swap
- `<LOCAL_INDEX_DIR>/local_index.sqlite3` — cache DB (Phase 95); Phase 97 migrates via PRAGMA user_version=2
- `<LOCAL_LAB_INDEX_DIR>/.meta.json` — LAB invalidation contract (Phase 95 D-09) — preserved
- MyLibraryTab as 6th desktop tab (Phase 95 SPEC) — unchanged

</code_context>

<specifics>
## Specific Ideas

- **Trigger:** Yehuda Seewald email 2026-05-25 comparing v7.14.0 to his external prototype. He runs 13K files / 43 GB; v7.14.0 hard-caps at 5K files / 2 GB; his recommendation to readers: "use v7.14.0 if < 5K files, otherwise use my prototype." Phase 97's goal is to close that gap so power users (including Seewald himself, and Hillel) can adopt v7.14.
- **Capacity target:** 13K files / 43 GB comfortably; soft warning at 50K / 50 GB; effective ceiling = "whatever fits on disk + merge headroom."
- **Codex sequencing recommendation:** ship recovery (Wave A) BEFORE ceiling lift (Wave D). Encoded in the wave structure above.
- **Hebrew RTL respect:** F-06 reverses the original draft per Codex — do NOT apply text reversal to HTML/XLSX/CSV; the parsers already produce logical-order strings. Reversal is a PDF-mirror-reversal-only safety net.

</specifics>

<deferred>
## Deferred Ideas

### Deferred to v7.15+ (explicit during this discuss-phase)
- **D-F2 PDF OCR** (Tesseract for image-only PDFs) — discussed; large dependency; needs its own phase + language pack decisions (heb+eng+ara?)
- **D-F3 Side-by-side PDF rendering** — needs PyMuPDF page-image rendering + dual-pane layout; reading-experience feature; the new `cached_text` column from R-03 enables faster text-side rendering for when this lands
- **`.md` / `.epub` / `.rtf` formats** — not in this phase; each adds parser dep + Hebrew RTL validation
- **D-F7 full background-thread View All refactor** — partially addressed via U-04 (raise cap to 500 + incremental render); full QThread refactor deferred
- **D-F8 View All page-block matching (substring → sentinel)** — Phase 96 known limit; not blocking
- **D-F10 View All renderer path consolidation** — Phase 96 cleanup item

### Out of scope (Phase 95 SPEC invariants stay)
- Web parity for LOCAL — desktop-only rule preserved
- Cloud-synced Lists containing LOCAL items — preserved
- Live `QFileSystemWatcher` folder watching — manual Refresh only
- Browser extension changes — preserved
- Per-user-account separation beyond OS profile paths — preserved

### Codex P2 items consciously skipped this phase
- C-05 configurable-cap settings panel — keep 100 MB as default, no advanced settings surface this phase
- Per-folder source-rate-of-change telemetry — defer
- Format-extraction telemetry (which extractor failed how often) — defer

### Reviewed Todos (not folded)
- None — todo matcher surfaced weak matches (Reading Desk UX, server-side search, NLI MARC crawl) but none are LOCAL-related

</deferred>

---

*Phase: 97-more-local-features*
*Context gathered: 2026-05-25*
*Codex critique date: 2026-05-25*
