# Phase 97: More LOCAL features — Research

**Researched:** 2026-05-25
**Domain:** Recovery foundation + commit policy + format extraction + capacity UX for the desktop LOCAL indexer
**Confidence:** HIGH on tantivy-py / openpyxl / zstandard / sqlite primitives (all verified via local install + Context-equivalent sources); MEDIUM-HIGH on Codex-folded P0 decisions (verified against existing code); MEDIUM on chunk-locator / merge-headroom heuristics (planner discretion zones)

## Summary

CONTEXT.md (six waves A-F, decisions C-01..C-06 + R-01..R-04 + F-01..F-06 + U-01..U-04 + D-NEW-1..D-NEW-8 with Codex P0+P1 folded) is authoritative. This research enriches the codebase view and surfaces **four issues** the planner MUST address that CONTEXT.md didn't fully resolve:

1. **`writer.get_memory_usage()` does NOT exist in tantivy-py 0.25.1.** Verified by enumerating `dir(writer)` on the installed binding and cross-checked with the tantivy-py type stubs [VERIFIED: local python import; CITED: github.com/quickwit-oss/tantivy-py tantivy.pyi]. The CONTEXT C-02 decision says "sampled via `writer.get_memory_usage()` if available; else conservative file/byte fallback" — that conditional resolves to "fallback" in v0.25.1. **The commit policy MUST be byte/count/time-only (no heap-sampling path)** unless we upgrade to a newer tantivy-py that adds the method, which is not on the table for Phase 97. Plan must drop the heap-sampling branch and lock the four-trigger policy as `(commit_when bytes>200MB OR files>100 OR seconds>60)` — the heap-size argument to `index.writer()` remains a memory ceiling but is not a commit trigger.

2. **`beautifulsoup4` is NOT installed.** Verified via `pip show beautifulsoup4` returning "not found" [VERIFIED: pip 2026-05-25]. `lxml==6.0.2` IS installed (transitive dep of `python-docx`). For F-01 HTML chunking, planner must EITHER add `beautifulsoup4` to `requirements.txt` + `requirements-desktop.txt` (consistent with D-43 PyMuPDF packaging pattern) OR use `lxml.html` directly (already shipped, no new dep). The latter is preferred — `lxml.html` has the same lenient parsing for malformed HTML, encoding-detection via `lxml.html.parse` with explicit encoding fallback chain, and avoids adding another PyInstaller `collect_all` invocation to `GenizahSearchPro.spec`. This is a `[Codex revision]` from F-01: replace BeautifulSoup with `lxml.html` to keep the dependency surface minimal.

3. **R-02 "atomic temp-dir swap" interacts with the existing SearchEngine reader handle.** `genizah_core.py:6700` opens `self.local_searcher = local_index.searcher()` at SearchEngine.__init__. On Windows this holds file handles on the LocalIndex segment files. Renaming the directory while a reader is live will FAIL with `os error 5 (Access denied)` — the same class of error that the existing `_commit_writer_with_retry()` at `local_indexer.py:1429-1482` already handles for writer commits. **The atomic swap protocol must include an explicit `SearchEngine.close_local_searcher()` step BEFORE `os.rename`** and a `SearchEngine.reload_local_indexes()` step AFTER. The same applies to LOCAL_LAB_INDEX_DIR's `local_lab_searcher` at `genizah_core.py:6741` and the LAB engine's reader. Plan R-02 must spell out this 5-step ordered protocol, not just "close readers/writers" hand-wave.

4. **`processed_files` table already has a `status` column with `pending`/`committed` values** (from Phase 95 D-21 two-phase commit at `local_indexer.py:271`). Phase 97 D-NEW-1 adds `scan_run_id` to `processed_files` per CONTEXT, and also adds it as a Tantivy schema field per U-02. The migration must NOT introduce a `pending` vs `pending_delete` clash: `local_files` ALREADY has `pending_delete INTEGER` (line 293), and `processed_files.status` already takes `pending`, `pending_delete`, `committed`. The U-02 `Discard` operation per CONTEXT executes `DELETE FROM processed_files WHERE scan_run_id = ?` — this clobbers ALL rows including those committed in prior runs IF they were touched/updated in this run. Planner must clarify: does `scan_run_id` get written on ALREADY-COMMITTED files that this scan touched-and-skipped (mtime unchanged → no work)? If yes, Discard wrongly deletes them. **Lock: `scan_run_id` is set only on rows INSERTED or UPDATED to `status='pending'` within this scan_run, NOT on rows the scan skipped because they were already up-to-date.** Plan must encode this rule explicitly in the SQL.

**Primary recommendation:** Lock the commit policy to byte/count/time triggers only (no heap-sampling), substitute `lxml.html` for BeautifulSoup in F-01, encode the atomic-swap protocol as a 5-step explicit close-rename-reload sequence, and pin `scan_run_id` semantics to "set only on rows mutated in this run, not on no-op skips."

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

> Verbatim from `97-CONTEXT.md` <decisions> — Codex P0+P1 folded.

**Capacity Track:**
- **C-01:** No hard cap. Soft warning at 50K files OR 50 GB, non-blocking ("Proceed / Cancel"). Replaces Phase 95's 5K/2GB hard-stop. **[Codex P0 sequencing]** Lands AFTER R-03 (cache), R-02 (rebuild), D-NEW-1 (migration).
- **C-02:** Combined heap + count + bytes + time commit policy. **[Codex P0 revision]** Trigger when ANY: heap 256 MB (via `writer.get_memory_usage()` if available — see Issue #1 above, falls back to no-op in v0.25.1) OR 100 files OR 200 MB source bytes OR 60 sec elapsed. Replaces Phase 95 fixed 25-file batch.
- **C-03:** Pre-scan dialog ONLY when result exceeds 50K files OR 50 GB. **[Codex P1]** Folder walk in worker thread; UI updates via batched `pyqtSignal(list)`. NO Qt widget mutation from worker.
- **C-04:** Aggregate-by-folder view default. Click row → drill-down. **[Codex P2]** Folder counters persisted in SQLite (new columns on `folders`: `indexed_count`, `error_count`, `pending_count`, `oversized_count`, `last_aggregate_at`). O(1) per folder, no scan of file rows.
- **C-05:** 100 MB hard skip with `status='oversized'`. **[Codex P1]** Zip-container limits for .docx/.xlsx: max uncompressed 500 MB, max cells/rows 100K, max chars per chunk 1 MB. `status='zip_bomb_suspected'` on breach.
- **C-06:** Live disk indicator in MyLibraryTab. **[Codex P1]** Reserve = 2× current index size as merge headroom. Warning when `(free_space - estimated_growth - 2×index_size) < 1 GB`.

**Crash Recovery Track:**
- **R-01:** Recovery modal at MyLibraryTab open. **[Codex P0]** LOCAL search **gated** until prompt resolved. `MyLibraryTab.is_searchable = False` during recovery; LOCAL queries return zero hits + banner.
- **R-02:** On startup, attempt `index.searcher()` on LOCAL. If raises, atomic rebuild. **[Codex P0]** Rebuild via temp-dir swap: rebuild in `.rebuild-<scan_run_id>/`, validate, close readers, `os.rename` old, `os.rename` new in, delete `.old-` on next clean shutdown. Same for LOCAL_LAB.
- **R-03:** Cached extracted text per Tantivy doc, zstd-compressed. **[Codex P0]** Schema additions need migration plan → D-NEW-1. Columns: `cached_text BLOB`, `cached_text_codec TEXT DEFAULT 'zstd'`, `cached_text_uncompressed_len INTEGER`, `extraction_format_version INTEGER DEFAULT 1`. Estimated 400 MB compressed at 13K files.
- **R-04:** WAL mode. **[Codex P1]** `synchronous=NORMAL` baseline; `synchronous=FULL` around the `pending → committed` UPDATE OR `wal_checkpoint(TRUNCATE)` after the inner UPDATE.

**Format Extraction Track:**
- **F-01:** HTML chunking at h1/h2 boundaries. Fallback to 20-paragraph chunks if "sparse" = fewer than 3 h1/h2 OR avg inter-heading paragraph count < 5. **[Codex P1]** Encoding: raw bytes → `<meta charset>` → chardet → cp1255. BeautifulSoup + lxml parser, strip script/style. *(Research note: see Issue #2 — substitute `lxml.html` for BeautifulSoup.)*
- **F-02:** XLSX chunking per (sheet, 500-row window). **[Codex P1]** `openpyxl.load_workbook(read_only=True, data_only=True)`.
- **F-03:** CSV per-200-rows.
- **F-04:** Uniform row extraction `cell1 | cell2 | cell3 | ...` (no header-row assumption).
- **F-05:** CSV encoding chain. **[Codex P1]** utf-8-sig → cp1255 → utf-16-le. `csv.Sniffer().sniff(sample, delimiters=',;\\t')` over first 4 KB. On decode failure: `status='encoding_error'`.
- **F-06:** RTL metadata-only. **[Codex P0]** Honor `dir="rtl"` (HTML) / `sheetView.rightToLeft=True` (XLSX) as **display metadata only** — do NOT apply Phase 95 `_fix_rtl_*` helpers to HTML/XLSX/CSV. They produce logical-order strings already.

**Indexing UX at Scale:**
- **U-01:** Phase-aware ETA. **[Codex P2]** Sub-progress for: folder walking / extracting / committing / rebuilding LAB.
- **U-02:** Cancel discard/keep. **[Codex P0]** `scan_run_id` UUID indexed on every Tantivy doc + every SQLite row touched. Discard: `writer.delete_documents(Term("scan_run_id", run_id))` + `DELETE FROM processed_files WHERE scan_run_id = ?`. Keep: `writer.commit()`.
- **U-03:** Folder walk in QThread. **[Codex P1]** Throttle to 1×/100 files OR 0.5 sec. Widget item creation/update on UI thread via batched `pyqtSignal(list)`.
- **U-04:** View All cap raised to 500. **[Codex P2]** Incremental rendering via `QTimer.singleShot(0, append_next_batch)`. NO main-thread freeze.

**New decisions (D-NEW-1..D-NEW-8):**
- **D-NEW-1:** SQLite migration via `PRAGMA user_version`. v1 → v2 adds: `cached_text BLOB`, `cached_text_codec`, `cached_text_uncompressed_len`, `extraction_format_version`, `scan_run_id`, `mtime_ns INTEGER`, `chunk_locator TEXT`. Also `folders` counter columns. Migration script in `shared/local_indexer_migrations.py`. Test fixtures: empty / v1 / v2. On `PRAGMA integrity_check` failure: surface error, require manual "Reset My Library" button.
- **D-NEW-2:** Network drive semantics. Unavailable folder (ENOENT/ETIMEDOUT) → `folders.status='unreachable'`; startup auto-rescan SKIPS unreachable. Transient timeout: retry 3× with 2s backoff → `status='timeout'`. Manual Refresh can recover.
- **D-NEW-3:** File-change-during-index. `os.stat` BEFORE + AFTER extraction; mtime_ns OR size change → `status='changed_during_index'` + re-queue (max 3 retries).
- **D-NEW-4:** SQLite rows only for `(extension IN supported OR status IN ('oversized', 'error', 'changed_during_index', 'zip_bomb_suspected'))`. 1→2 migration deletes Phase 95 rows where `extension NOT IN supported AND status IS NULL`.
- **D-NEW-5:** `chunk_locator TEXT` per Tantivy doc. PDF: `p. N`. DOCX: `¶ N-M`. HTML: `§ <heading>` or `¶ N-M`. XLSX: `<sheet>!R<n>:R<m>`. CSV: `rows N-M`.
- **D-NEW-6:** Privacy disclosure update. Help + About bilingual EN+HE — disclose zstd compressed cleartext in `<INDEX_DIR>/local_index.sqlite3`, never uploaded.
- **D-NEW-7:** `tests/test_phase_97_invariants.py` covering: (a) three cloud-write gates at TOP; (b) web LIBRARY_CODES allowlist `[]`; (c) `is_local_sys_id()` recognizes 18-digit 97-prefixed; (d) LOCAL RRF merge POST-`_deduplicate()`.
- **D-NEW-8:** `mtime_ns INTEGER` alongside `size`. Opt-in cheap hash of first + last 64 KB for same-size+same-mtime_ns suspicious cases.

**Wave sequencing (per Codex Overall Assessment — recovery before ceiling lift):**
- Wave A: Recovery foundation — D-NEW-1, R-03, R-02, R-04, R-01
- Wave B: Commit policy + container safety — C-02, C-05, D-NEW-8
- Wave C: Format extraction (parallelizable with B) — F-01..F-06
- Wave D: Capacity UX (requires A) — C-01, C-03, C-04, C-06
- Wave E: Indexing UX at scale (builds on D) — U-01..U-04
- Wave F: Gap closure + privacy + tests (interleaves with D/E) — D-NEW-2..D-NEW-7

### Claude's Discretion

- F-01 "sparse" threshold heuristic — planner adjusts based on smoke tests
- Wave-internal ordering of parallelizable items
- zstd compression level (1-22) for `cached_text` — pick based on benchmark
- Worker thread count for parallel extraction in Wave D
- Exact split between rebuild-from-cache vs rebuild-from-source in R-02 fallback

### Deferred Ideas (OUT OF SCOPE)

- D-F2 PDF OCR (Tesseract) → v7.15+
- D-F3 Side-by-side PDF rendering → v7.15+ (the new cached_text from R-03 makes the text side faster when this lands)
- `.md` / `.epub` / `.rtf` formats — not this phase
- D-F7 full background-thread View All refactor — partial via U-04; full QThread deferred
- D-F8 View All page-block matching — known limit, not blocking
- D-F10 View All renderer path consolidation — cleanup item
- Web parity for LOCAL — desktop-only stays
- Cloud-synced Lists containing LOCAL items
- Live `QFileSystemWatcher` folder watching
- Browser extension changes
- Per-user-account separation beyond OS profile paths
- C-05 configurable-cap settings panel — keep 100 MB as default
- Per-folder source-rate-of-change telemetry
- Format-extraction telemetry (which extractor failed how often)
</user_constraints>

## Phase Requirements

> Phase 97 has no traditional REQ-IDs; CONTEXT uses decision IDs as the planning units. The table below maps each decision to the research finding that enables implementation, so the planner can verify every locked decision has at least one verified primitive.

| Decision | Description | Research Support |
|----------|-------------|------------------|
| C-01 | Ceiling lift (50K/50GB soft) | Existing `_check_ceiling_*` at `my_library_tab.py:_MAX_FILES_CEILING` (Phase 95 D-26) — replace constants + change from hard-stop to soft-warn. Verified in code. |
| C-02 | Commit policy (heap+count+bytes+time) | tantivy-py 0.25.1 `IndexWriter` has NO `get_memory_usage()` [VERIFIED]; count/bytes/time triggers implementable via `_pending_filepaths` (already exists) + `_batch_bytes` (new) + `_last_commit_ts` (new). |
| C-03 | Pre-scan dialog | Existing `_check_ceiling_*` returns count + bytes (Phase 95 D-26). Worker thread per Phase 95 D-25 mutex pattern. |
| C-04 | Persisted folder counters | Add columns to `folders` table; D-NEW-1 migration handles. SELECT on indexed_count per folder is O(1). |
| C-05 | Zip-container limits | `zipfile.ZipInfo.file_size` (uncompressed) and `compress_size` available [VERIFIED]. Iterate `zipfile.ZipFile(path).infolist()` BEFORE handing to openpyxl. |
| C-06 | Disk merge headroom | `shutil.disk_usage(path)` returns (total, used, free); compare against `os.path.getsize(seg_file) for seg_file in os.listdir(index_dir)` for current index size. |
| R-01 | Recovery UX modal | Add `MyLibraryTab.is_searchable` flag; gate `genizah_core.SearchEngine._search_local()` on `not is_searchable → return []`. |
| R-02 | Atomic Tantivy rebuild | `os.rename` is atomic on same-filesystem moves [POSIX]; on Windows requires target to not exist (use 2-step rename via `.old-<ts>`). Issue #3: must close SearchEngine readers BEFORE rename. |
| R-03 | Cached_text BLOB | zstandard 0.25.0 [VERIFIED installed]: `zstandard.ZstdCompressor(level=3).compress(text.encode('utf-8'))` produces ~76:1 ratio on repeating Hebrew/English mix at level 3 (test: 6000 → 79 bytes). Real corpora compress 3-5x. |
| R-04 | WAL + FULL on critical UPDATE | `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL` already set at `local_indexer.py:257`. Escalation pattern: `PRAGMA synchronous=FULL` before BEGIN, restore after COMMIT. |
| F-01 | HTML chunking | `lxml.html` [VERIFIED installed via lxml==6.0.2 transitive dep]. Substitute for BeautifulSoup per Issue #2. |
| F-02 | XLSX chunking | `openpyxl==3.1.5` [VERIFIED]. `read_only=True, data_only=True` supports `iter_rows(values_only=True)`. `worksheet.sheet_view.rightToLeft` attribute exists [VERIFIED]. |
| F-03 | CSV chunking | `csv.reader(..., newline='')` stdlib; chunk per 200 rows. |
| F-04 | Uniform row extraction | Stdlib string join; no library. |
| F-05 | CSV encoding chain | `csv.Sniffer().sniff(sample, delimiters=',;\\t')` [VERIFIED stdlib method]. Failure path: raise → mark `encoding_error`. |
| F-06 | RTL metadata only | F-01/F-02/F-03 parsers produce logical-order strings (Python `str` from utf-8 decoded bytes is logical, not visual). Phase 95 dead-code helpers stay dead. |
| U-01 | Phase-aware ETA | Existing `_progress_bar` + `_show_status_message` (Phase 95); add 4 sub-progress states. |
| U-02 | scan_run_id | New Tantivy schema field `scan_run_id` (text, raw tokenizer, indexed). `writer.delete_documents("scan_run_id", uuid_str)` — verified exists [VERIFIED: `dir(writer)`]. UUID via `uuid.uuid4().hex`. |
| U-03 | Folder walk QThread | Existing `LocalIndexerWorker` QThread pattern (Phase 95). New `FolderWalkWorker` mirrors. |
| U-04 | View All 200→500 + incremental | `_VIEW_ALL_PAGE_CAP = 200` at `genizah_app.py:18777` — bump to 500. `QTimer.singleShot(0, append_next_batch)` is standard PyQt6 idiom. |
| D-NEW-1 | SQLite migration | sqlite3 stdlib + `PRAGMA user_version`. Try/except `OperationalError` on `ALTER TABLE ADD COLUMN` for idempotency. |
| D-NEW-2 | Network drive semantics | `os.walk(followlinks=False)` + per-iteration `try/except OSError`. `errno.ETIMEDOUT` / `errno.ENOENT` / `errno.EACCES` discriminate via `OSError.errno`. |
| D-NEW-3 | File-change-during-index | Two `os.stat` calls bracketing extraction; compare `st_mtime_ns` + `st_size`. |
| D-NEW-4 | SQLite row policy | New 1→2 migration: `DELETE FROM processed_files WHERE filepath NOT LIKE '%.pdf' AND filepath NOT LIKE '%.docx' AND filepath NOT LIKE '%.txt' AND filepath NOT LIKE '%.html' AND filepath NOT LIKE '%.xlsx' AND filepath NOT LIKE '%.csv' AND status IS NULL`. |
| D-NEW-5 | chunk_locator | New stored text field on LOCAL Tantivy schema. Per-format formatter helpers in `shared/local_indexer.py`. |
| D-NEW-6 | Privacy disclosure | `web/pages/help.py` + desktop Help dialog + About dialog updates. Bilingual EN+HE. Mirrors Phase 95 D-33 disclosure language. |
| D-NEW-7 | Invariant regression tests | New `tests/test_phase_97_invariants.py`. AST-based scanner mirrors existing `tests/test_no_raw_storage_access.py` + `tests/test_pgp_filter_cascade.py` patterns. |
| D-NEW-8 | mtime_ns | `os.stat().st_mtime_ns` returns int [VERIFIED: 1779622953717785900 on Windows]. SQLite INTEGER. |

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| SQLite migration runner | `shared/local_indexer_migrations.py` (new) | `shared/local_indexer.py` LocalIndexer.__init__ | Migration module is Qt-free + import-light so it can run inside `LocalIndexer.__init__` before any Tantivy work |
| Atomic Tantivy rebuild | `shared/local_indexer.py::LocalIndexer.rebuild_main_index_atomic()` (new method) | `genizah_core.py::SearchEngine.close_local_searcher()` + `.reload_local_indexes()` | Rebuild lives in shared (Qt-free); reader-close is in genizah_core because that's where SearchEngine owns the searcher handles |
| Cached text BLOB read/write | `shared/local_indexer.py` | — | Single owner; zstd encode/decode helpers + SQLite UPDATE in same module |
| Recovery UX (modal + LOCAL search gate) | `desktop/my_library_tab.py::MyLibraryTab` + `genizah_core.py::SearchEngine` | — | Modal on tab; `is_searchable` flag read by SearchEngine LOCAL query path |
| HTML extractor (`extract_html_pages`) | `shared/local_indexer.py` (new function) | — | Mirrors `extract_pdf_pages` / `extract_docx_pages` / `extract_txt` shape |
| XLSX extractor | `shared/local_indexer.py` (new function) | — | Same pattern; reads sheetView.rightToLeft metadata |
| CSV extractor | `shared/local_indexer.py` (new function) | — | Same pattern; csv.Sniffer + encoding chain |
| Folder-walk worker | `desktop/my_library_tab.py::FolderWalkWorker` (new QThread) | — | Filesystem walk off-UI; signals to UI thread via `pyqtSignal(list)` batches |
| Status panel aggregate view | `desktop/my_library_tab.py::_UnifiedFileTreeWidget` (extend) | SQLite `folders` counter columns | UI reads counters in O(1); drill-down query for per-file rows on click |
| Pre-scan dialog | `desktop/my_library_tab.py::MyLibraryTab` | — | Existing `_check_ceiling_*` extend with soft-warn branch |
| Disk-usage indicator | `desktop/my_library_tab.py::MyLibraryTab` | `shared/local_indexer.py::estimate_index_size()` | Simple QLabel updated on focus/refresh; size calc shared |
| Phase-aware ETA | `desktop/my_library_tab.py` worker progress dispatcher | — | Four sub-progress states; UI composes the four into one bar |
| Invariant tests | `tests/test_phase_97_invariants.py` (new) | — | AST scanner pattern from Phase 87 |

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `tantivy` (tantivy-py) | `==0.25.1` (installed) | LOCAL + LOCAL LAB side-indexes (unchanged from Phase 95) | Same constraint; verified via `pip show tantivy` 2026-05-25 [VERIFIED] |
| `pymupdf` (`fitz`) | `>=1.24,<2.0` (installed: 1.27.2.3) | PDF extraction (unchanged from Phase 95 + Phase 96 D-F4 fix) | Already pinned in `requirements.txt` |
| `python-docx` | `>=1.0,<2.0` (installed: 1.2.0) | DOCX extraction (unchanged) | Already pinned in `requirements.txt` |
| `openpyxl` | `==3.1.5` (installed) | XLSX extraction (new for F-02) | Already in `requirements.txt` for export functionality; `load_workbook(read_only=True, data_only=True)` + `iter_rows(values_only=True)` is the documented streaming pattern [CITED: openpyxl.readthedocs.io/en/3.1/optimized.html] |
| `lxml` | `>=6.0` (installed: 6.0.2) | HTML extraction (new for F-01) | Already shipped as transitive dep of `python-docx`. `lxml.html` provides lenient HTML parsing equivalent to BeautifulSoup's lxml backend; recommended substitution for the CONTEXT-mentioned BeautifulSoup to avoid adding another packaging dep |
| `zstandard` | `==0.25.0` (installed) | Compressed `cached_text` BLOB (new for R-03) | Already in `requirements-lock.txt` (transitive dep of pyiceberg); needs explicit add to `requirements.txt` per Codex P2 packaging note |
| `sqlite3` (stdlib) | Python 3.11 (3.45.1 lib) | Cache DB + migrations | No new dep; WAL + PRAGMA user_version are stable [VERIFIED] |
| `csv` (stdlib) | Python 3.11 | CSV extraction (new for F-03/F-05) | Includes `csv.Sniffer().sniff(sample, delimiters=...)` [VERIFIED stdlib] |
| `zipfile` (stdlib) | Python 3.11 | Zip-container size inspection for C-05 zip-bomb defense | `ZipInfo.file_size` (uncompressed) + `compress_size` both available [VERIFIED] |
| `uuid` (stdlib) | Python 3.11 | `scan_run_id` UUID generation (U-02) | `uuid.uuid4().hex` |
| `shutil` (stdlib) | Python 3.11 | `shutil.disk_usage(path)` for C-06 disk indicator | Returns (total, used, free) bytes tuple |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `defusedxml` | Optional | XML-bomb defense for openpyxl | openpyxl recommends installing for `xlsx` attack protection [CITED: openpyxl PyPI page]. Phase 97 plan should add this to `requirements.txt` since F-02 introduces XLSX parsing. Default `MAX_DATA = 30 MB`. |
| `errno` (stdlib) | builtin | Discriminate ENOENT/ETIMEDOUT/EACCES for D-NEW-2 | Used in `OSError.errno` checks |
| `unicodedata` (stdlib) | builtin | Phase 95 RTL helpers (still dead code per F-06) | Unchanged |
| `chardet` | NOT INSTALLED, NOT RECOMMENDED | HTML charset fallback (CONTEXT F-01) | The CONTEXT mentions chardet as fallback. Verify before adding — if cp1255 is reliable enough, chardet adds 250 KB + slow detection. Planner picks during F-01 implementation. |
| `beautifulsoup4` | NOT INSTALLED, NOT RECOMMENDED | HTML parser (CONTEXT F-01 said BS) | **Substitute with `lxml.html` per Issue #2.** Saves dependency surface + PyInstaller `collect_all` invocation. |

### Version Verification

```bash
# Verified 2026-05-25 on dev machine
pip show tantivy        # 0.25.1 [VERIFIED]
pip show pymupdf        # 1.27.2.3 [VERIFIED]
pip show python-docx    # 1.2.0 [VERIFIED]
pip show openpyxl       # 3.1.5 [VERIFIED]
pip show lxml           # 6.0.2 [VERIFIED]
pip show zstandard      # 0.25.0 [VERIFIED]
pip show beautifulsoup4 # NOT FOUND [VERIFIED]
sqlite3 --version       # 3.45.1 [VERIFIED via Python sqlite3.sqlite_version]
```

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `lxml.html` | `BeautifulSoup` with `lxml` parser | BS adds explicit dep + PyInstaller `collect_all`; for the simple "strip script/style, find h1/h2, walk paragraphs" workload `lxml.html` is sufficient [CITED: lxml.de/elementsoup.html — lxml is the parser, BS is a convenience wrapper] |
| Raw `csv.reader` | `pandas.read_csv` | Pandas is heavy (NumPy + Cython + 50 MB binary surface) for a 200-row windowed extraction. `csv` stdlib has Sniffer + dialect detection already. |
| `zstandard` (cpyext) | Python 3.14 stdlib `compression.zstd` | Python 3.11 baseline doesn't have stdlib zstd; the `zstandard` PyPI package is the canonical CPython binding [CITED: python-zstandard.readthedocs.io] |
| zstd dictionary-trained | zstd untrained (level 3-5) | For short chunks (< 1 KB) dictionary training improves ratio meaningfully; for Phase 97 page-text chunks (typically 500-5000 chars), untrained level 3 is sufficient — research benchmark on repeating Hebrew+Latin text at level 3 showed 76:1 ratio, level 9 same, level 19 marginal +5% gain [VERIFIED: local zstd benchmark]. Planner picks 3 unless cache footprint exceeds expected 400 MB. |
| Migration via raw SQL + `PRAGMA user_version` | `alembic` / `yoyo-migrations` | Single-file SQLite cache; full ORM migration framework is overkill. `PRAGMA user_version` + numbered Python migration functions is the lightweight standard [CITED: levlaz.org/sqlite-db-migrations-with-pragma-user_version] |

**Installation deltas (additions to `requirements.txt` + `requirements-desktop.txt`):**

```
zstandard>=0.22,<1.0   # Phase 97 R-03 (was transitive via pyiceberg in lock file)
defusedxml>=0.7        # Phase 97 F-02 zip-bomb defense for openpyxl
# openpyxl already pinned
# lxml already pinned (transitive via python-docx)
# csv / sqlite3 / uuid / zipfile / shutil are stdlib
```

**PyInstaller `.spec` deltas (`GenizahSearchPro.spec`):**

```python
# Existing: collect_all('pymupdf'), collect_all('tantivy')
# Phase 97 additions:
tmp_ret = collect_all('zstandard')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('openpyxl')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
# lxml/defusedxml are pure-Python; PyInstaller auto-discovers from imports
```

## Architecture Patterns

### System Architecture Diagram

```
                            ┌──────────────────────────────┐
                            │   MyLibraryTab (desktop UI)  │
                            │                              │
                            │  Recovery modal              │
                            │  Pre-scan dialog             │
   ┌────────────────────────┤  Per-folder aggregate view   │◄──────────────┐
   │                        │  Drill-down per-file rows    │               │
   │ user picks Resume/     │  Disk usage indicator        │               │
   │ Restart/Skip           │  ETA breakdown               │               │
   │                        └──────────────┬───────────────┘               │
   │                                       │                               │
   │                                       │ pyqtSignal(list) batches      │
   │                                       │ (throttled 1×/100 files       │
   │                                       │  or 0.5 sec)                  │
   │                                       │                               │
   │                                       ▼                               │
   │                            ┌─────────────────────┐                    │
   │                            │ FolderWalkWorker    │                    │
   │                            │   (QThread; U-03)   │                    │
   │                            │   os.walk           │                    │
   │                            │   os.stat (mtime_ns)│                    │
   │                            │   detects ENOENT/   │                    │
   │                            │   ETIMEDOUT (D-NEW-2)                    │
   │                            └─────────┬───────────┘                    │
   │                                      │                                │
   │                                      ▼                                │
   │                            ┌──────────────────────┐                   │
   │                            │ LocalIndexerWorker   │                   │
   │                            │   (QThread; Phase 95)│                   │
   │                            │   per-file extraction│                   │
   │                            └─────────┬────────────┘                   │
   │                                      │                                │
   │                                      ▼                                │
   │   ┌──────────────────────────────────────────────────────────────┐   │
   │   │           shared/local_indexer.py::LocalIndexer              │   │
   │   │                                                              │   │
   │   │  extract_pdf_pages      (D-01, D-F4 fallback)                │   │
   │   │  extract_docx_pages     (D-04 20-para chunks)                │   │
   │   │  extract_txt            (utf-8-sig + cp1255)                 │   │
   │   │  extract_html_pages*    (F-01 NEW: h1/h2 + chardet/cp1255)   │   │
   │   │  extract_xlsx_pages*    (F-02 NEW: per-sheet × 500-row)      │   │
   │   │  extract_csv_pages*     (F-03 NEW: per-200-row + Sniffer)    │   │
   │   │                                                              │   │
   │   │  ┌────────────────────────────────────────────────────────┐  │   │
   │   │  │   per-chunk processing                                 │  │   │
   │   │  │   1. compute scan_run_id-tagged Tantivy doc            │  │   │
   │   │  │   2. compute chunk_locator string (D-NEW-5)            │  │   │
   │   │  │   3. zstd compress text → cached_text BLOB (R-03)      │  │   │
   │   │  │   4. SQLite INSERT processed_files (status='pending',  │  │   │
   │   │  │      scan_run_id, mtime_ns, size)                      │  │   │
   │   │  │   5. SQLite INSERT local_pages + local_files           │  │   │
   │   │  │   6. writer.add_document(...)                          │  │   │
   │   │  │   7. accumulate batch_files / batch_bytes / batch_start │  │   │
   │   │  │   8. IF (files≥100 OR bytes≥200MB OR sec≥60):           │  │   │
   │   │  │        _commit_batch()  ───┐                            │  │   │
   │   │  └─────────────────────────────│──────────────────────────┘  │   │
   │   │                                ▼                              │   │
   │   │  ┌─────────────────────────────────────────────────────────┐ │   │
   │   │  │   _commit_batch() (two-phase + R-04 durability)         │ │   │
   │   │  │   1. writer.commit() (with Windows retry — existing)     │ │   │
   │   │  │   2. PRAGMA synchronous=FULL (R-04 escalation)          │ │   │
   │   │  │   3. BEGIN; UPDATE processed_files SET status='committed'│ │   │
   │   │  │      WHERE filepath IN (...); COMMIT;                   │ │   │
   │   │  │   4. PRAGMA synchronous=NORMAL (restore)                │ │   │
   │   │  │   5. clear batch counters                               │ │   │
   │   │  └─────────────────────────────────────────────────────────┘ │   │
   │   │                                                              │   │
   │   │  rebuild_main_index_atomic()  (R-02 NEW)                     │   │
   │   │     1. Build in <DIR>.rebuild-<scan_run_id>/                 │   │
   │   │     2. SELECT cached_text FROM local_pages WHERE             │   │
   │   │        status='committed'; zstd decompress                   │   │
   │   │     3. writer.add_document for each cached chunk             │   │
   │   │     4. writer.commit() + index.searcher() validation         │   │
   │   │     5. SearchEngine.close_local_searcher()  ◄── Issue #3     │   │
   │   │     6. os.rename(DIR, DIR + '.old-<ts>')                     │   │
   │   │     7. os.rename(DIR + '.rebuild-<id>', DIR)                 │   │
   │   │     8. SearchEngine.reload_local_indexes()                   │   │
   │   │     9. (next-clean-shutdown) delete '.old-<ts>'              │   │
   │   └──────────────────────────────────────────────────────────────┘   │
   │                                                                       │
   │            ┌─────────────────────────────────────┐                    │
   │            │  shared/local_indexer_migrations.py │                    │
   │            │   (D-NEW-1 NEW MODULE)              │                    │
   │            │                                     │                    │
   │            │  read PRAGMA user_version           │                    │
   │            │  if < 2: run migration 1→2         │                    │
   │            │      ALTER TABLE ADD COLUMN ...      │                    │
   │            │      (try/except OperationalError    │                    │
   │            │       per column for idempotency)   │                    │
   │            │      DELETE rows per D-NEW-4         │                    │
   │            │      PRAGMA user_version = 2         │                    │
   │            │  on integrity_check fail: raise +    │                    │
   │            │      surface "Reset My Library"      │                    │
   │            └─────────────────────────────────────┘                    │
   │                                                                       │
   │            ┌─────────────────────────────────────┐                    │
   │            │   tests/test_phase_97_invariants.py │                    │
   │            │     (D-NEW-7 NEW)                   │                    │
   │            │   - AST: cloud-write gates at TOP   │                    │
   │            │   - AST: web LIBRARY_CODES `[]`     │                    │
   │            │   - is_local_sys_id recognizes 97   │                    │
   │            │   - LOCAL RRF merge POST-dedup      │                    │
   │            └─────────────────────────────────────┘                    │
   │                                                                       │
   └───────────────────────────────────────────────────────────────────────┘

Data flow during recovery:
  app start → MyLibraryTab.__init__
   → LocalIndexer.__init__
     → init_sqlite (creates tables if absent)
     → local_indexer_migrations.run(conn) (NEW)
        ├─ PRAGMA integrity_check
        │   fail → surface error, require manual Reset
        │   pass → continue
        └─ PRAGMA user_version → if 1: apply 1→2 migration → user_version=2
     → SearchEngine._open_local_searcher()
        on Tantivy schema mismatch / corrupt → trigger rebuild_main_index_atomic
   → MyLibraryTab.is_searchable = False  (R-01 gate)
   → check pending rows + last scan_run unclean
     yes → show recovery modal
       → user picks Resume / Restart / Skip
       → MyLibraryTab.is_searchable = True
     no → MyLibraryTab.is_searchable = True
```

### Recommended Project Structure (deltas only)

```
shared/
├── local_indexer.py                  # EXISTING — extend with new extractors + cached_text + rebuild_atomic
├── local_indexer_migrations.py       # NEW — D-NEW-1 migration runner
└── local_sys_id.py                   # EXISTING — unchanged

desktop/
└── my_library_tab.py                 # EXISTING — extend with recovery modal, pre-scan dialog UI, aggregate view, ETA, FolderWalkWorker

genizah_app.py                        # EXISTING — extend _VIEW_ALL_PAGE_CAP 200→500 + incremental render
genizah_core.py                       # EXISTING — extend SearchEngine with close_local_searcher() + is_searchable gate

tests/
├── test_phase_97_invariants.py       # NEW — D-NEW-7 four AST guards
├── test_local_indexer_migrations.py  # NEW — D-NEW-1 three-fixture test (empty / v1 / v2)
├── test_atomic_rebuild.py            # NEW — R-02 corruption → rebuild → validate
├── test_two_phase_durability.py      # NEW — R-04 power-loss simulation
├── test_html_extraction.py           # NEW — F-01 fixtures + RTL round-trip
├── test_xlsx_extraction.py           # NEW — F-02 fixtures + sheetView.rightToLeft + zip-bomb
├── test_csv_extraction.py            # NEW — F-03/F-05 fixtures
├── test_scan_run_id.py               # NEW — U-02 discard/keep semantics
├── test_folder_walk_worker.py        # NEW — U-03 QThread + signal batching
├── test_view_all_incremental.py      # NEW — U-04 500-cap + incremental
├── test_network_drive_semantics.py   # NEW — D-NEW-2 ENOENT/ETIMEDOUT mocks
└── fixtures/local_indexer/
    ├── hebrew_sample.html            # NEW
    ├── hebrew_sample.xlsx            # NEW (with sheetView.rightToLeft=True)
    ├── hebrew_sample.csv             # NEW
    ├── zip_bomb_sample.xlsx          # NEW (synthesized; small zip with huge uncompressed claim)
    └── multi_sheet_large.xlsx        # NEW (sheet with > 500 rows for chunking)
```

### Pattern 1: PRAGMA user_version Migration

**What:** Apply schema deltas idempotently using SQLite's stored 32-bit user_version field.
**When to use:** Any non-fresh-install SQLite schema change.
**Example:**

```python
# Source: pattern verified against levlaz.org and CONTEXT D-NEW-1
# shared/local_indexer_migrations.py

import sqlite3
from typing import Callable

_LATEST_VERSION = 2

def _migrate_1_to_2(conn: sqlite3.Connection) -> None:
    """Phase 97 D-NEW-1: extend Phase 95 schema with cached_text + scan_run_id + counters."""
    cur = conn.cursor()
    # ALTER TABLE ADD COLUMN is NOT idempotent — wrap each in try/except
    # for sqlite3.OperationalError 'duplicate column name'
    _alter_safe(cur, "ALTER TABLE processed_files ADD COLUMN scan_run_id TEXT")
    _alter_safe(cur, "ALTER TABLE processed_files ADD COLUMN mtime_ns INTEGER")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN cached_text BLOB")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN cached_text_codec TEXT NOT NULL DEFAULT 'zstd'")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN cached_text_uncompressed_len INTEGER")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN extraction_format_version INTEGER NOT NULL DEFAULT 1")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN chunk_locator TEXT")
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN indexed_count INTEGER NOT NULL DEFAULT 0")
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN error_count INTEGER NOT NULL DEFAULT 0")
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN pending_count INTEGER NOT NULL DEFAULT 0")
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN oversized_count INTEGER NOT NULL DEFAULT 0")
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN last_aggregate_at REAL")
    # D-NEW-4: prune unsupported-extension rows that Phase 95 left in processed_files
    cur.execute("""
        DELETE FROM processed_files
        WHERE filepath NOT LIKE '%.pdf' COLLATE NOCASE
          AND filepath NOT LIKE '%.docx' COLLATE NOCASE
          AND filepath NOT LIKE '%.txt' COLLATE NOCASE
          AND filepath NOT LIKE '%.html' COLLATE NOCASE
          AND filepath NOT LIKE '%.xlsx' COLLATE NOCASE
          AND filepath NOT LIKE '%.csv' COLLATE NOCASE
          AND (status IS NULL OR status NOT IN
               ('oversized', 'error', 'changed_during_index', 'zip_bomb_suspected'))
    """)

def _alter_safe(cur: sqlite3.Cursor, ddl: str) -> None:
    try:
        cur.execute(ddl)
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc):
            raise

_MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _migrate_1_to_2,  # key = current version, value = function to upgrade BY ONE
}

def run(conn: sqlite3.Connection) -> int:
    """Run all pending migrations. Returns the resulting user_version.

    On PRAGMA integrity_check failure, raises RuntimeError — the caller
    (MyLibraryTab) is responsible for surfacing the manual "Reset My
    Library" recovery button (D-NEW-1).
    """
    # Integrity check FIRST — corrupt SQLite cannot be migrated safely
    cur = conn.cursor()
    cur.execute("PRAGMA integrity_check")
    result = cur.fetchone()[0]
    if result != "ok":
        raise RuntimeError(
            f"local_index.sqlite3 PRAGMA integrity_check failed: {result}. "
            "Use 'Reset My Library' in advanced settings."
        )
    cur.execute("PRAGMA user_version")
    current = cur.fetchone()[0]
    while current < _LATEST_VERSION:
        migrate = _MIGRATIONS.get(current)
        if migrate is None:
            raise RuntimeError(f"No migration registered from user_version {current}")
        # Each migration runs in its own transaction; PRAGMA user_version
        # is bumped INSIDE the transaction so partial migrations don't
        # advance the version
        conn.execute("BEGIN")
        try:
            migrate(conn)
            conn.execute(f"PRAGMA user_version = {current + 1}")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        current += 1
    return current
```

### Pattern 2: tantivy-py Commit Policy (byte/count/time triggers)

**What:** Trigger `writer.commit()` based on aggregated source bytes, file count, and elapsed time.
**When to use:** Phase 97 C-02 supersedes Phase 95 D-21 fixed 25-file batch.
**Example:**

```python
# Source: derived from tantivy-py 0.25.1 verified API surface
# (IndexWriter has: add_document, commit, rollback, delete_documents,
#  wait_merging_threads. NO get_memory_usage method.)

class _CommitTriggers:
    """Phase 97 C-02 — heap-sampling branch dropped because v0.25.1
    has no get_memory_usage(). Fallback to byte/count/time only.
    """
    BYTES_THRESHOLD = 200 * 1024 * 1024  # 200 MB
    FILES_THRESHOLD = 100
    SECONDS_THRESHOLD = 60.0

    def __init__(self):
        self._batch_bytes = 0
        self._batch_files = 0
        self._batch_start = time.monotonic()

    def record_file(self, source_size: int) -> None:
        self._batch_bytes += source_size
        self._batch_files += 1

    def should_commit(self) -> bool:
        return (
            self._batch_bytes >= self.BYTES_THRESHOLD
            or self._batch_files >= self.FILES_THRESHOLD
            or (time.monotonic() - self._batch_start) >= self.SECONDS_THRESHOLD
        )

    def reset(self) -> None:
        self._batch_bytes = 0
        self._batch_files = 0
        self._batch_start = time.monotonic()
```

### Pattern 3: SQLite WAL with synchronous=FULL Per-Transaction Escalation

**What:** Use WAL for normal writes (fast, low-fsync), escalate to synchronous=FULL around the critical pending→committed UPDATE that gates Tantivy↔SQLite consistency.
**When to use:** Phase 97 R-04. Phase 95's two-phase commit needs durability at exactly one transaction; this is cheaper than running synchronous=FULL for the whole connection.
**Example:**

```python
# Source: SQLite forum + agwa.name analysis [CITED: sqlite.org/forum/info/d1a6bc7c4cd2baca,
# avi.im/blag/2025/sqlite-fsync, agwa.name/blog/post/sqlite_durability]
# In WAL mode:
#  - synchronous=NORMAL: corruption-safe but last txn can be lost on power loss
#  - synchronous=FULL: fsync on every commit (durable across power failure)
#  - alternative: wal_checkpoint(TRUNCATE) at strategic points

# Variant A: per-transaction escalation
def _commit_batch_durable(self) -> None:
    """Phase 97 R-04: escalate synchronous to FULL around the
    pending→committed UPDATE only."""
    self._commit_writer_with_retry()  # Tantivy commit (existing)
    self._conn.execute("PRAGMA synchronous = FULL")
    try:
        self._conn.execute("BEGIN IMMEDIATE")
        placeholders = ",".join("?" * len(self._pending_filepaths))
        self._conn.execute(
            f"UPDATE processed_files SET status = 'committed' "
            f"WHERE filepath IN ({placeholders})",
            self._pending_filepaths,
        )
        self._conn.execute("COMMIT")
    finally:
        self._conn.execute("PRAGMA synchronous = NORMAL")
    self._pending_filepaths.clear()

# Variant B: wal_checkpoint
def _commit_batch_durable_v2(self) -> None:
    """Alternative R-04: rely on default synchronous=NORMAL but force a
    durable checkpoint after the inner UPDATE."""
    self._commit_writer_with_retry()
    self._conn.execute("BEGIN IMMEDIATE")
    self._conn.execute("UPDATE processed_files SET status = 'committed' WHERE ...")
    self._conn.execute("COMMIT")
    # Force fsync of WAL + main DB
    self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    self._pending_filepaths.clear()
```

Variant A is preferred (lower latency cost — one extra fsync vs full WAL truncation). Pin via the test `tests/test_two_phase_durability.py`.

### Pattern 4: Atomic Tantivy Rebuild via Temp-Dir Swap

**What:** Build a fresh LOCAL index in a sibling temp directory, validate, then atomic-rename to swap.
**When to use:** Phase 97 R-02 on startup corruption detection.
**Example:**

```python
# Source: synthesized from CONTEXT R-02 + existing _commit_writer_with_retry
# pattern + Issue #3 (close SearchEngine reader first)

def rebuild_main_index_atomic(
    self,
    scan_run_id: str,
    close_searcher_cb: Callable[[], None],
    reload_searcher_cb: Callable[[], None],
) -> None:
    """Phase 97 R-02 — atomic rebuild via temp-dir swap.

    Closing the live SearchEngine reader before rename is REQUIRED on
    Windows (Issue #3) because the reader holds file handles on segment
    files. os.rename across these handles raises os error 5.
    """
    import shutil

    rebuild_dir = f"{self._index_dir}.rebuild-{scan_run_id}"
    old_dir = f"{self._index_dir}.old-{int(time.time())}"

    # Step 1: build fresh index in temp-dir
    if os.path.isdir(rebuild_dir):
        shutil.rmtree(rebuild_dir)
    os.makedirs(rebuild_dir)
    fresh_schema = build_local_schema()
    fresh_index = tantivy.Index(fresh_schema, path=rebuild_dir)
    fresh_writer = fresh_index.writer(heap_size=50_000_000)

    # Step 2: walk SQLite WHERE status='committed', re-index from cached_text
    cur = self._conn.execute(
        "SELECT sys_id, uid, page_num, cached_text, cached_text_codec, chunk_locator "
        "FROM local_pages "
        "INNER JOIN processed_files ON local_pages.sys_id = processed_files.sys_id "
        "WHERE processed_files.status = 'committed'"
    )
    dctx = zstandard.ZstdDecompressor()
    for sys_id, uid, page_num, blob, codec, locator in cur:
        if blob is None:
            # Phase 95 row without cached_text — fall back to source re-extract
            # (slow path; documented in CONTEXT D-NEW-1 backfill behavior)
            text = self._re_extract_from_source(sys_id, page_num)
            if text is None:
                continue  # source unavailable (network drive) — skip
        else:
            if codec == "zstd":
                text = dctx.decompress(blob).decode("utf-8")
            else:
                raise ValueError(f"Unknown codec {codec!r} for {uid}")
        doc = tantivy.Document(unique_id=uid, content=text, ...)
        fresh_writer.add_document(doc)

    # Step 3: commit + validate
    fresh_writer.commit()
    fresh_writer.wait_merging_threads()
    fresh_searcher = fresh_index.searcher()  # raises if still corrupt
    del fresh_searcher
    del fresh_writer
    del fresh_index

    # Step 4: close LIVE readers on the OLD index (Issue #3)
    close_searcher_cb()

    # Step 5: atomic rename (two-step for Windows safety)
    os.rename(self._index_dir, old_dir)
    try:
        os.rename(rebuild_dir, self._index_dir)
    except OSError:
        # Roll back: restore old dir
        os.rename(old_dir, self._index_dir)
        raise

    # Step 6: reload live readers
    reload_searcher_cb()

    # Step 7: schedule delete of old_dir on next clean shutdown
    # (record in SQLite so a crash here doesn't leak the .old- dir)
    self._conn.execute(
        "INSERT INTO _pending_cleanup (path, kind) VALUES (?, 'rebuild_old')",
        (old_dir,),
    )
    self._conn.commit()
```

### Pattern 5: openpyxl read_only Mode with Zip-Bomb Defense

**What:** Stream large XLSX files row-by-row + check zip metadata BEFORE openpyxl loads anything.
**When to use:** F-02 XLSX extraction.
**Example:**

```python
# Source: openpyxl docs + zipfile stdlib [CITED: openpyxl.readthedocs.io/en/3.1/optimized.html]
# verified ZipInfo.file_size + compress_size 2026-05-25

_MAX_UNCOMPRESSED_BYTES = 500 * 1024 * 1024  # 500 MB per C-05
_MAX_CELLS_PER_SHEET = 100_000               # per C-05
_MAX_CHARS_PER_CHUNK = 1_000_000             # 1 MB per C-05
_XLSX_ROW_WINDOW = 500                       # per F-02

def _check_zip_bomb(filepath: str) -> str | None:
    """Return reason string if file looks like a zip bomb, else None."""
    import zipfile
    try:
        with zipfile.ZipFile(filepath, 'r') as zf:
            total_uncompressed = sum(info.file_size for info in zf.infolist())
            if total_uncompressed > _MAX_UNCOMPRESSED_BYTES:
                return (f"xlsx uncompressed size {total_uncompressed} "
                        f"exceeds limit {_MAX_UNCOMPRESSED_BYTES}")
    except zipfile.BadZipFile:
        return "not a valid xlsx (zip) file"
    return None


def extract_xlsx_pages(filepath: str) -> Iterator[tuple[int, str, str]]:
    """Phase 97 F-02 — per (sheet, 500-row window) Tantivy doc.

    Each window emitted as one chunk with chunk_locator like
    'Synopsis!R1:R500' (D-NEW-5).
    Yields (chunk_num, text, title).
    """
    bomb_reason = _check_zip_bomb(filepath)
    if bomb_reason:
        raise XlsxZipBombSuspected(bomb_reason)

    from openpyxl import load_workbook
    wb = load_workbook(filepath, read_only=True, data_only=True)
    try:
        title = os.path.basename(filepath)
        chunk_num = 0
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            # D-NEW-5 chunk_locator metadata + F-06 RTL flag
            is_rtl = bool(getattr(ws.sheet_view, 'rightToLeft', False))
            rows_in_window: list[str] = []
            cells_seen = 0
            window_start_row = 1
            for row_num, row in enumerate(
                ws.iter_rows(values_only=True), start=1
            ):
                # F-04 uniform extraction: cell1 | cell2 | cell3
                cell_strs = [str(c) if c is not None else "" for c in row]
                cells_seen += len(cell_strs)
                if cells_seen > _MAX_CELLS_PER_SHEET:
                    raise XlsxZipBombSuspected(
                        f"sheet {sheet_name!r} exceeds {_MAX_CELLS_PER_SHEET} cells"
                    )
                line = " | ".join(cell_strs)
                if line.strip():
                    rows_in_window.append(line)
                # Emit a chunk every 500 rows
                if (row_num - window_start_row + 1) >= _XLSX_ROW_WINDOW:
                    chunk_num += 1
                    text = "\n".join(rows_in_window)
                    if len(text) > _MAX_CHARS_PER_CHUNK:
                        text = text[:_MAX_CHARS_PER_CHUNK]
                    locator = f"{sheet_name}!R{window_start_row}:R{row_num}"
                    yield chunk_num, text, title, locator, is_rtl
                    rows_in_window = []
                    window_start_row = row_num + 1
            # Flush trailing partial window
            if rows_in_window:
                chunk_num += 1
                last_row = window_start_row + len(rows_in_window) - 1
                text = "\n".join(rows_in_window)
                if len(text) > _MAX_CHARS_PER_CHUNK:
                    text = text[:_MAX_CHARS_PER_CHUNK]
                locator = f"{sheet_name}!R{window_start_row}:R{last_row}"
                yield chunk_num, text, title, locator, is_rtl
    finally:
        wb.close()
```

### Anti-Patterns to Avoid

- **Calling `writer.get_memory_usage()` in tantivy-py 0.25.1** — method does not exist; AttributeError at runtime. (Issue #1)
- **Atomic-rename swap while live SearchEngine reader is open** — Windows `os error 5`. Always close+reload SearchEngine readers around the rename. (Issue #3)
- **Applying Phase 95 `_fix_rtl_*` helpers to HTML/XLSX/CSV** — corrupts already-logical strings. F-06 P0 [Codex].
- **Loading openpyxl XLSX without `read_only=True`** — memory-resident workbook for 100K-row sheet can OOM the desktop process. Always use streaming mode for F-02.
- **`os.rename(old, new)` on Windows when `new` exists** — POSIX overwrites, Windows raises `FileExistsError`. Two-step: `rename(target, target+'.old-<ts>')` then `rename(temp, target)`.
- **Calling `ALTER TABLE ADD COLUMN` without try/except** — Re-running a migration that already partially applied raises `duplicate column name`. Always wrap.
- **`writer.delete_documents("scan_run_id", run_id)` on a tokenized field** — silently does nothing per tantivy-py issue #297 (the same bug Phase 95 dodged for `unique_id` by setting `tokenizer_name="raw"`). The new `scan_run_id` field MUST be added with `tokenizer_name="raw"`.
- **Setting `scan_run_id` on already-committed-and-skipped rows** — Discard then wrongly deletes them. Set only on rows mutated in this run. (Issue #4)
- **Mutating QWidget items from a worker thread** — random crashes. All `QTreeWidgetItem` updates must be on the GUI thread via signal slots. C-03 + U-03 [Codex P1].
- **Untrained zstd dictionary for sub-1 KB chunks** — measurable ratio loss vs trained. For Phase 97's 500-5000 char chunks this is not a problem at level 3, but if cache footprint balloons beyond 1 GB at 13K files, planner should explore dictionary training (defer to ops).
- **Storing float `mtime` for change detection on network shares** — granularity loss; same-second edits missed. Use `st_mtime_ns` INTEGER. D-NEW-8 [Codex P1].

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| SQLite schema versioning | Custom config-file-version tracking | `PRAGMA user_version` + numbered Python migration functions | The standard SQLite pattern; documented across the SQLite forum and tooling community [CITED: sqlite.org/forum, levlaz.org] |
| HTML charset sniffing | Hand-roll meta-charset regex | `lxml.html.parse(file, parser=lxml.html.HTMLParser(encoding=None))` (lxml auto-detects); for ambiguous cases fall back to `<meta>` regex + cp1255 | lxml's parser handles HTML5 + HTML4 + malformed quirks; rewriting that is a maintenance trap |
| CSV delimiter detection | Hard-code `,` or hard-code `;` | `csv.Sniffer().sniff(sample, delimiters=',;\\t')` | Excel exports `;` in European locales; user CSVs vary; stdlib handles |
| Atomic file replace on Windows | Custom retry loop with `shutil.move` | Two-step `os.rename` with explicit `.old-` intermediate + an audit row in SQLite for cleanup-on-shutdown | Windows lacks single-rename overwrite; the two-step pattern is canonical |
| zstd compression | Lib- or service-specific compression | `zstandard.ZstdCompressor(level=3)` + `.ZstdDecompressor()` | Standard binding [VERIFIED installed]; benchmark shows level 3 is sufficient for Hebrew/English text |
| Zip-bomb detection | Decompress-then-check | `zipfile.ZipFile.infolist()` → sum `file_size` BEFORE handing to openpyxl | Inspects zip central directory without decompressing payload |
| UUID generation | Counter / monotonic | `uuid.uuid4().hex` | 32 hex chars, no collisions, no clock dependency |
| Disk space query | Parse `df` output | `shutil.disk_usage(path)` | Stdlib; returns (total, used, free); cross-platform |
| QThread file walk + UI updates | Per-file signal emit | Batched `pyqtSignal(list)` carrying file-metadata batches; throttle 1×/100 files OR 0.5 sec | Per-file signals at 100K files = 100K Qt event-loop dispatches → UI freeze. Batches preserve throughput [CITED: pythonguis.com Multi-Threading Best Practices] |
| Phase-aware ETA blending | Single bytes/sec smoothing | Four separate EWMAs (walking, extracting, committing, rebuilding); sum to overall ETA | Cached skips are instant (extract phase), commits are slow (commit phase); blending them mis-estimates badly |
| Reading-experience PDF rendering | Hand-roll dual-pane | DEFER to v7.15+ (D-F3) | Out of scope; cached_text from R-03 makes this trivial when the phase lands |

**Key insight:** Phase 97 is "make the existing indexer durable + grow it past 5K files," not "introduce new technology." The library choices are dictated by Phase 95's existing surface. The only genuinely-new libraries are `zstandard` (in lock file already, just needs explicit pin) and `openpyxl` (already in `requirements.txt` for export functionality, just newly used by indexer); `lxml.html` is already shipped via `python-docx`. Migration + atomic rebuild + cached text are the load-bearing design work — they save the FIRST users at 13K files from being the first crash-recovery beta testers.

## Runtime State Inventory

> Phase 97 is largely a refactor/extension, not a rename — but it does affect runtime state. Categories addressed below.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | `local_index.sqlite3` schema evolves v1 → v2; existing Phase 95 deployed installs (v7.14.0 shipped 2026-05-24) have v1 DBs in user-data dirs | Code edit: `local_indexer_migrations.run()` invoked at MyLibraryTab open, before first query. Migration is idempotent + backfills `cached_text=NULL`, `extraction_format_version=1`, `scan_run_id=NULL` for existing rows. Rebuild path (R-02) handles NULL `cached_text` by source re-extraction. Tests: 3-fixture (empty, v1 with Phase 95 data, v2 no-op). |
| Live service config | None — LOCAL is per-machine, no external service config involved. n8n / Datadog / Tailscale are not affected. | None. |
| OS-registered state | None — Phase 97 does not add Windows Task Scheduler tasks, launchd plists, or pm2 entries. The desktop EXE is the only OS-registered artifact and its installer (`CompileScriptGenizah.iss`) is unchanged in path/description. | None. |
| Secrets / env vars | None — Phase 97 does not introduce new env vars. `WEB_PUZZLE_ENABLED`, `POSTHOG_API_KEY`, etc. unchanged. The LOCAL feature uses `Config.LOCAL_INDEX_DIR` (derived path, not env-driven). | None. |
| Build artifacts / installed packages | `requirements.txt` adds `zstandard` + `defusedxml` (the latter recommended for openpyxl XML defense). `GenizahSearchPro.spec` adds `collect_all('zstandard')` + `collect_all('openpyxl')`. Existing PyInstaller dist binaries from v7.14.0 do NOT have zstandard collected (transitive deps are not auto-collected) — release MUST rebuild before shipping. | Code edit (`requirements.txt`, `.spec`); release build artifact regeneration (already standard part of any release). |

**Nothing found in category:** Live service config (None — verified via grep for `n8n`, `Datadog`, `Tailscale` in repo; no LOCAL feature integration). OS-registered state (None — Windows Task Scheduler check via existing release docs shows no Phase 95/96 task registration). Secrets/env vars (None — verified by reading `CLAUDE.md` env var list; LOCAL_INDEX_DIR is derived, not env-driven).

## Common Pitfalls

### Pitfall 1: tantivy-py 0.25.1 has no get_memory_usage()
**What goes wrong:** Code that conditionally samples `writer.get_memory_usage()` per CONTEXT C-02 raises `AttributeError` at runtime.
**Why it happens:** The method exists in newer tantivy versions but was not exposed in the 0.25.x Python binding.
**How to avoid:** Drop the heap-sampling branch entirely. C-02 commit triggers reduce to (files=100, bytes=200MB, time=60sec). The `heap_size=N` argument to `index.writer()` is a memory ceiling, NOT a commit threshold.
**Warning signs:** Code with `if hasattr(writer, 'get_memory_usage')` — that branch never fires in 0.25.1. Future-proof by leaving a TODO with the tantivy version that adds it.

### Pitfall 2: Windows os.rename fails when target exists
**What goes wrong:** `os.rename(source, target)` on Windows raises `FileExistsError` if `target` exists; POSIX silently overwrites.
**Why it happens:** Windows semantics differ from POSIX for `rename`.
**How to avoid:** R-02 protocol uses two renames: `os.rename(target, target + '.old-<ts>')` first to vacate the destination, THEN `os.rename(source, target)`. Record `.old-<ts>` in SQLite so a crash between renames leaves the rollback path discoverable.
**Warning signs:** Test fails on Windows with `[WinError 183] Cannot create a file when that file already exists`.

### Pitfall 3: Live Tantivy searcher holds file handles preventing rename
**What goes wrong:** `os.rename(LOCAL_INDEX_DIR, ...)` raises `os error 5 (Access denied)` on Windows when `SearchEngine.local_searcher` is alive.
**Why it happens:** Tantivy's mmap-backed segment files are open in the reader process.
**How to avoid:** Explicit `SearchEngine.close_local_searcher()` step before rename in R-02 protocol; explicit `reload_local_indexes()` step after. Mirror the existing `_commit_writer_with_retry()` Windows-access-denied retry pattern at `local_indexer.py:1429` if rename also hits transient locks (antivirus scanning the new segment files).
**Warning signs:** Test passes on Linux/macOS, fails on Windows release CI.

### Pitfall 4: ALTER TABLE ADD COLUMN is not idempotent
**What goes wrong:** Re-running a partially-applied migration raises `sqlite3.OperationalError: duplicate column name`.
**Why it happens:** SQLite lacks `IF NOT EXISTS` for columns.
**How to avoid:** Wrap each `ALTER TABLE ADD COLUMN` in try/except for `sqlite3.OperationalError` and check the error message contains `duplicate column name` before swallowing.
**Warning signs:** First-run migration succeeds; second-run (developer testing reapply) fails noisily.

### Pitfall 5: SQLite synchronous=NORMAL can lose last transaction on power loss
**What goes wrong:** WAL+NORMAL is corruption-safe but may lose the most recent transaction(s) on OS crash or power failure. For the Phase 95 two-phase commit (Tantivy committed → SQLite pending→committed UPDATE), losing that UPDATE leaves Tantivy ahead of SQLite — exactly the inconsistency Phase 97 is trying to prevent.
**Why it happens:** synchronous=NORMAL skips fsync on transaction commit; only fsync at checkpoint.
**How to avoid:** R-04 protocol: escalate to synchronous=FULL around the critical UPDATE (variant A) OR force `PRAGMA wal_checkpoint(TRUNCATE)` after the UPDATE (variant B). Pin via `tests/test_two_phase_durability.py` with a `kill -9` simulation mid-batch.
**Warning signs:** User reports "I refreshed, scan completed, but search misses files" after Windows BSOD or power loss.

### Pitfall 6: openpyxl XML attacks
**What goes wrong:** Malicious xlsx files exploiting "billion laughs" / quadratic blowup XML attacks. Without protection, a small xlsx can DOS the process.
**Why it happens:** openpyxl uses Python's stdlib XML by default, which is vulnerable.
**How to avoid:** Add `defusedxml` to `requirements.txt`; install it BEFORE importing openpyxl (defusedxml hooks the parser). Verify via the package's import sequence in `shared/local_indexer.py`. Combine with the zip-bomb defense in F-02 pattern above.
**Warning signs:** Spike in process memory or CPU on user-supplied xlsx.

### Pitfall 7: scan_run_id field tokenization
**What goes wrong:** `writer.delete_documents("scan_run_id", uuid_str)` silently does nothing → Discard operation leaves committed docs in the index, U-02 promise broken.
**Why it happens:** Default tokenizer breaks UUIDs into pieces; `delete_documents` searches as a single term. tantivy-py issue #297.
**How to avoid:** Add `scan_run_id` to the LOCAL schema with `tokenizer_name="raw"` (same pattern Phase 95 used for `unique_id`).
**Warning signs:** Test `tests/test_scan_run_id.py::test_discard_removes_all_run_docs` fails — count of LOCAL docs after Discard equals count before.

### Pitfall 8: BeautifulSoup vs lxml.html confusion
**What goes wrong:** Plan calls for BeautifulSoup but installer doesn't include it; runtime ImportError.
**Why it happens:** `beautifulsoup4` is NOT installed [VERIFIED]; CONTEXT F-01 mentioned BeautifulSoup as a default choice but Issue #2 surfaces this gap.
**How to avoid:** Substitute `lxml.html` (already installed via `python-docx` transitive dep). API differs slightly — `lxml.html.fromstring(bytes)` instead of `BeautifulSoup(text, 'lxml')`, traversal via `.iter('h1')` / `.iter('h2')` / `.text_content()`.
**Warning signs:** Pre-release pre-flight (`pytest tests/test_html_extraction.py`) fails with `ModuleNotFoundError: bs4`.

### Pitfall 9: Network drive scan blocks startup auto-rescan
**What goes wrong:** OneDrive offline / unplugged external drive / VPN-blocked UNC path takes minutes to time out on `os.walk`, blocking startup auto-rescan.
**Why it happens:** Default OS network-share timeouts can be 30-60+ seconds per access.
**How to avoid:** D-NEW-2 protocol: pre-check `os.path.isdir(folder)` BEFORE walking; mark `folders.status='unreachable'`; skip in auto-rescan; allow manual refresh. Discriminate ENOENT (path gone) vs ETIMEDOUT (transient) via `OSError.errno`. Retry transient 3× with 2s backoff.
**Warning signs:** Startup hang on machines with disconnected network shares.

### Pitfall 10: View All cap freezes the main thread
**What goes wrong:** Phase 96 96-09 raised cap to 200; user reports 30-sec freeze rendering a 200-page file. Phase 97 raises to 500 — the freeze gets worse.
**Why it happens:** Building 500 `<p>` blocks + applying line-numbering gutter to all of them in one main-thread pass blocks the Qt event loop.
**How to avoid:** U-04 incremental rendering: render first 50 pages immediately, then `QTimer.singleShot(0, append_next_batch)` to append in 50-page batches. Event loop remains responsive between batches.
**Warning signs:** User reports "window goes white / unresponsive when I click View All on a long file."

## Code Examples

Verified patterns from official sources. Each is reproducible against the installed environment.

### Example 1: HTML extraction with lxml.html (substitute for BeautifulSoup)

```python
# Source: lxml docs [CITED: lxml.de/elementsoup.html]
# Avoids adding beautifulsoup4 to requirements (Issue #2)
import lxml.html
import lxml.etree

_HTML_PARSER = lxml.html.HTMLParser(encoding=None)  # auto-detect from meta + bytes

def _detect_html_encoding(raw_bytes: bytes) -> str:
    """F-01 encoding chain: <meta charset> → byte-sniff → cp1255 fallback."""
    # First 1 KB usually has the <meta> tag
    head = raw_bytes[:1024].decode('ascii', errors='ignore').lower()
    import re
    m = re.search(r'<meta[^>]+charset\s*=\s*["\']?([\w-]+)', head)
    if m:
        return m.group(1).strip()
    # Try utf-8 strict
    try:
        raw_bytes[:4096].decode('utf-8', errors='strict')
        return 'utf-8'
    except UnicodeDecodeError:
        pass
    return 'cp1255'  # legacy Hebrew Windows fallback


def extract_html_pages(filepath: str) -> Iterator[tuple[int, str, str, str]]:
    """F-01: chunk at h1/h2 boundaries; 20-paragraph fallback if sparse.
    Yields (chunk_num, text, title, chunk_locator)."""
    with open(filepath, 'rb') as f:
        raw = f.read()
    encoding = _detect_html_encoding(raw)
    try:
        text = raw.decode(encoding, errors='replace')
    except LookupError:
        text = raw.decode('cp1255', errors='replace')

    tree = lxml.html.fromstring(text)
    # Strip script/style content per F-01 [Codex P1]
    for tag in tree.iter('script', 'style'):
        tag.getparent().remove(tag)
    # Document title
    title_elem = tree.find('.//title')
    title = (title_elem.text or os.path.basename(filepath)).strip() if title_elem is not None else os.path.basename(filepath)

    headings = list(tree.iter('h1', 'h2'))
    # F-01 "sparse" heuristic: fewer than 3 OR avg inter-heading paras < 5
    paragraphs = list(tree.iter('p'))
    avg_inter = (len(paragraphs) / max(len(headings), 1)) if headings else 0
    use_semantic = len(headings) >= 3 and avg_inter >= 5

    if use_semantic:
        for chunk_num, h in enumerate(headings, start=1):
            heading_text = (h.text_content() or '').strip()
            # Collect content until next h1/h2
            buf = [heading_text] if heading_text else []
            sib = h.getnext()
            while sib is not None and sib.tag not in ('h1', 'h2'):
                buf.append(sib.text_content() or '')
                sib = sib.getnext()
            text = "\n".join(s.strip() for s in buf if s.strip())
            if text:
                yield chunk_num, text, title, f"§ {heading_text or 'section ' + str(chunk_num)}"
    else:
        # Fallback: 20-paragraph windows
        chunk_num = 0
        for start in range(0, len(paragraphs), 20):
            chunk_num += 1
            slice_ = paragraphs[start:start + 20]
            text = "\n".join((p.text_content() or '').strip() for p in slice_ if (p.text_content() or '').strip())
            if text:
                end = min(start + 20, len(paragraphs))
                yield chunk_num, text, title, f"¶ {start + 1}-{end}"
```

### Example 2: CSV extraction with Sniffer + encoding chain

```python
# Source: csv stdlib [VERIFIED] + Phase 95 D-07 encoding chain pattern
import csv

_CSV_ROW_WINDOW = 200  # F-03
_CSV_ENCODINGS = ('utf-8-sig', 'cp1255', 'utf-16-le')  # F-05

def extract_csv_pages(filepath: str) -> Iterator[tuple[int, str, str, str]]:
    """F-03 + F-05: per-200-row windows; uniform `cell1 | cell2 | ...` extraction;
    encoding chain utf-8-sig → cp1255 → utf-16-le."""
    title = os.path.basename(filepath)
    chosen_encoding = None
    sample_text = None
    for enc in _CSV_ENCODINGS:
        try:
            with open(filepath, 'r', encoding=enc, newline='') as f:
                sample_text = f.read(4096)
            chosen_encoding = enc
            break
        except UnicodeDecodeError:
            continue
    if chosen_encoding is None:
        raise EncodingError(f"CSV decode failed across {_CSV_ENCODINGS}: {filepath}")

    # Delimiter detection per F-05
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=',;\t')
    except csv.Error:
        dialect = csv.excel  # fallback to comma

    with open(filepath, 'r', encoding=chosen_encoding, newline='') as f:
        reader = csv.reader(f, dialect=dialect)
        rows_in_window: list[str] = []
        window_start = 1
        chunk_num = 0
        for row_num, row in enumerate(reader, start=1):
            cell_strs = [str(c) if c is not None else "" for c in row]
            line = " | ".join(cell_strs)
            if line.strip():
                rows_in_window.append(line)
            if (row_num - window_start + 1) >= _CSV_ROW_WINDOW:
                chunk_num += 1
                text = "\n".join(rows_in_window)
                yield chunk_num, text, title, f"rows {window_start}-{row_num}"
                rows_in_window = []
                window_start = row_num + 1
        if rows_in_window:
            chunk_num += 1
            text = "\n".join(rows_in_window)
            last_row = window_start + len(rows_in_window) - 1
            yield chunk_num, text, title, f"rows {window_start}-{last_row}"
```

### Example 3: zstd compress/decompress for cached_text

```python
# Source: python-zstandard 0.25.0 [VERIFIED installed]
import zstandard

_ZSTD_LEVEL = 3  # planner discretion; level 3 is the canonical balance

def compress_cached_text(text: str) -> tuple[bytes, int]:
    """Returns (compressed_blob, uncompressed_byte_len)."""
    payload = text.encode('utf-8')
    cctx = zstandard.ZstdCompressor(level=_ZSTD_LEVEL)
    return cctx.compress(payload), len(payload)


def decompress_cached_text(blob: bytes) -> str:
    """Reverse of compress_cached_text."""
    dctx = zstandard.ZstdDecompressor()
    return dctx.decompress(blob).decode('utf-8')


# Benchmark on local Hebrew/English mix (verified 2026-05-25):
#  Input:  6000 bytes ('שלום עולם This is some Hebrew + English mixed text. ' × 100)
#  Level 3:  79 bytes  (76:1 ratio)
#  Level 9:  79 bytes  (76:1 ratio)
#  Level 19: 76 bytes  (79:1 ratio, marginal)
# Real-world Hebrew page text compresses 3-5x (less repetition than the benchmark above).
# 13K files × ~5 pages × ~2KB compressed ≈ 130 MB. CONTEXT estimate of 400 MB is conservative.
```

### Example 4: AST-based invariant test (Phase 97 D-NEW-7)

```python
# Source: pattern from tests/test_no_raw_storage_access.py + tests/test_pgp_filter_cascade.py
# (Phase 87 / Phase 93 established the pattern)
import ast
import pathlib

def test_cloud_write_gates_at_top():
    """D-NEW-7 (a): three cloud-write gates remain at TOP of respective modules.

    Specifically: search_serializer.py _serialize_item must reject LOCAL FIRST;
    corrections_client.py submit_correction must reject FIRST; lists_sync.py
    sync_item_to_cloud + sync_list_to_cloud must reject FIRST (before _get_client()).
    """
    root = pathlib.Path(__file__).parent.parent
    targets = {
        root / 'shared' / 'search_serializer.py': '_serialize_item',
        root / 'corrections_client.py': 'submit_correction',
        root / 'lists_sync.py': 'sync_item_to_cloud',
    }
    for path, fn_name in targets.items():
        tree = ast.parse(path.read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == fn_name:
                # The FIRST 5 statements must contain an is_local_sys_id check
                first_block = ast.unparse(ast.Module(body=node.body[:5], type_ignores=[]))
                assert 'is_local_sys_id' in first_block, (
                    f"{path.name}::{fn_name}: is_local_sys_id gate not in first 5 "
                    f"statements (Phase 95 D-30 / Phase 97 D-NEW-7 invariant)"
                )
                break
        else:
            raise AssertionError(f"{path.name}: no function named {fn_name}")
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Fixed-batch commit (Phase 95: every 25 files) | Byte/count/time multi-trigger (Phase 97 C-02) | This phase | Stronger commit cadence for variable-size corpora; small docs commit more frequently, large PDFs trigger byte threshold |
| Hard cap at 5K files / 2 GB (Phase 95 D-26) | Soft warn at 50K / 50 GB (Phase 97 C-01) | This phase | Power users (13K-50K files) unblocked; "ceiling" is now disk space + merge headroom only |
| Indexer rebuild = source re-extraction only | Indexer rebuild = cached_text decompress → re-add (R-02 + R-03) | This phase | Network drives + missing files no longer block recovery; instant rebuild from compressed cache |
| `mtime` (float, 1-second granularity on FAT32, depends on FS) | `st_mtime_ns` (int, nanosecond) | Phase 97 D-NEW-8 | Same-second edits now detected |
| Single fsync at checkpoint (synchronous=NORMAL) | Per-transaction fsync on critical UPDATE (synchronous=FULL escalation) | Phase 97 R-04 | Power-loss safety on the gating UPDATE between Tantivy commit and SQLite mark-committed |
| BeautifulSoup + lxml (planned in CONTEXT F-01) | lxml.html only (research substitution) | This phase | -1 explicit dep; no PyInstaller `collect_all` for BS |
| 200-page View All hard cap (Phase 96) | 500-page View All + incremental render (U-04) | This phase | Larger files viewable end-to-end without window freeze |

**Deprecated/outdated:**

- Phase 95 D-21 fixed 25-file batch — supersedes by C-02 (this phase). Keep dead-code preservation pattern (Phase 95 D-02) — old constant `_COMMIT_BATCH_SIZE = 25` stays as a fallback constant for tests but the live path uses `_CommitTriggers`.
- Phase 95 D-07 utf-8-sig only TXT default — preserved for `.txt` files. F-05's wider chain (utf-8-sig → cp1255 → utf-16-le) applies to `.csv` only.

## Assumptions Log

> Claims tagged `[ASSUMED]` in this research. Confirm with user before locking into plans.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | zstd level 3 produces 3-5x ratio on real Hebrew page text (CONTEXT estimate 400 MB at 13K files) | R-03 / Standard Stack | Cache footprint grows; if real ratio is 1.5x, footprint balloons to ~1 GB. Mitigation: planner can switch to level 9 or train dictionary; no architectural change. ASSUMED based on level-3 benchmark on repeating string + general Hebrew text compression rules-of-thumb. |
| A2 | Tantivy segment-merge scratch space worst case is 2× current index size (CONTEXT C-06) | C-06 | If real worst case is 3-4×, headroom warning fires too late; user sees disk-full mid-merge. Mitigation: planner can over-provision to 3× for conservatism, surface in disclosure. ASSUMED based on segment-merge generic guidance; quickwit team has not published a tighter bound. |
| A3 | F-01 "sparse" threshold (< 3 h1/h2 OR avg inter-heading < 5 paras) catches scholarly Hebrew HTML correctly | F-01 | Scholarly Hebrew HTML may have many h1/h2 used as paragraph headers, blowing the heuristic the wrong way. Mitigation: planner adjusts during smoke test (CONTEXT explicit "Claude's Discretion"). ASSUMED — Hillel has a corpus of representative HTML but it's not in the research environment. |
| A4 | `defusedxml` install is sufficient zip-bomb defense for openpyxl (no additional code changes needed) | C-05 / Standard Stack | If defusedxml hook isn't auto-applied by openpyxl in 3.1.5, attacks still go through. Mitigation: explicit `import defusedxml; defusedxml.defuse_stdlib()` at module load. ASSUMED based on openpyxl PyPI description; needs verification in execute phase. |
| A5 | LOCAL_LAB index follows same atomic-rebuild + reader-close pattern as LOCAL main index | R-02 | If LAB has different reader lifecycle, rebuild can hang Windows-style. Mitigation: code review during plan. ASSUMED based on shared codebase pattern in `genizah_core.py:6740-6741`. |
| A6 | tantivy-py 0.25.1 `writer.delete_documents("scan_run_id", uuid_str)` works for UUID strings if field is added with `tokenizer_name="raw"` (per Phase 95 issue #297 finding) | U-02 | If raw-tokenized delete fails for some other reason, Discard semantics break. Mitigation: integration test pin. ASSUMED based on Phase 95 RESEARCH issue #297 closure; pattern proven for `unique_id` already. |
| A7 | Phase 95-deployed v7.14.0 user DBs have no schema corruption (PRAGMA integrity_check passes) | D-NEW-1 | If integrity_check fails on existing users' DBs at upgrade, the migration surfaces "Reset My Library" — destroys their library. Mitigation: provide an export tool for the file list + opt-out set BEFORE reset, in the same dialog. ASSUMED based on no field reports of corruption since v7.14.0 release 2026-05-24 (1 day ago — small sample). |
| A8 | Network drives raise OSError with `errno` set (not bare raises or different exception types) on Windows / macOS / Linux | D-NEW-2 | If a platform raises a non-standard exception, the discriminator misclassifies. Mitigation: wrap in broader except OSError + log raw `repr(exc)` for diagnostics. ASSUMED based on Python os module docs but not verified per-platform in research. |

## Open Questions (RESOLVED)

1. **Should existing Phase 95 user DBs trigger a one-time rebuild after migration to populate `cached_text`?**
   - What we know: Migration adds `cached_text BLOB NULL`; existing rows have NULL. R-02 rebuild path falls back to source re-extraction for NULL rows.
   - What's unclear: At 13K files, the first rebuild after upgrade would do source re-extraction for ALL of them — slow. Could the migration trigger a background "cache populate" pass that doesn't block the user?
   - RESOLVED: Plan should NOT add a background cache-populate pass in Phase 97 — it's an optimization. Document that the FIRST rebuild after upgrade is slow but subsequent rebuilds are fast. Add a TODO for v7.16+ to background-populate on idle.

2. **What's the exact behavior when user clicks "Reset My Library" (integrity_check failure path)?**
   - What we know: CONTEXT D-NEW-1 says "require manual intervention via a dedicated 'Reset My Library' button in advanced settings."
   - What's unclear: Does reset wipe Tantivy AND SQLite, or just SQLite? Does it preserve folder list (in SQLite, which is being reset)? Does it preserve the opt-out set (in session JSON, separately)?
   - RESOLVED: Lock in plan: Reset = (a) delete `LOCAL_INDEX_DIR/local_index.sqlite3`, (b) delete `LOCAL_INDEX_DIR/*.seg` and Tantivy meta files, (c) PRESERVE folder list by re-prompting on next MyLibraryTab open ("Re-add your folders"), (d) PRESERVE opt-out set in session JSON. After Reset → Refresh → re-extracts everything. This is a recovery-of-last-resort path; preserve the user's choices about WHICH folders to index even if the cache is gone.

3. **U-02 Discard semantics — does it work across multiple in-flight scans?**
   - What we know: CONTEXT U-02 says Cancel → Discard removes `scan_run_id`-tagged docs.
   - What's unclear: If user cancels scan A while scan B is queued (Phase 95 D-25 mutex queue, max depth 1), does B inherit A's `scan_run_id` or get its own? Per the queue depth-1 design, A's cancel releases the mutex and B starts — B should generate a NEW `scan_run_id`. Verify in plan.
   - RESOLVED: Plan must spell out: each new acquire of the mutex generates a fresh `scan_run_id`. The queued-action lambda captures the function reference, not the run_id; the run_id is generated inside the worker on start.

4. **Does the LOCAL Lab index also need atomic rebuild, or just the main LOCAL index?**
   - What we know: CONTEXT R-02 says "same protocol for `LOCAL_LAB_INDEX_DIR`."
   - What's unclear: LAB has its own invalidation contract (Phase 95 D-38 weights_hash). If LAB rebuilds whenever LAB weights change, do we also need atomic semantics there?
   - RESOLVED: Yes. Atomic LAB rebuild reuses the same protocol but invoked from `build_lab_side_index()`. Plan-internal consistency check: LAB rebuild on a corrupted LAB cache must be atomic; otherwise a power loss mid-LAB-rebuild leaves Composition Search broken.

5. **For F-01 "sparse" fallback, do we need to handle nested headings (h3/h4)?**
   - What we know: CONTEXT F-01 specifies h1/h2 chunking with 20-paragraph fallback.
   - What's unclear: A document with h2 → h3 → h3 → h3 → h2 → h3 pattern has h2 boundaries that capture entire sections including all child h3s. Is that the intent (large sections), or should we also split at h3 within a parent h2?
   - RESOLVED: Phase 97 ships h1/h2 only per CONTEXT. If smoke tests reveal h2-sections are too large (multi-thousand-line sections), planner can extend the heuristic to "h1/h2/h3" — deferred to plan-internal discretion. Document in F-01 plan.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.11+ | All extractors + sqlite3 + zipfile | ✓ | 3.11 baseline (CLAUDE.md) | None — required |
| tantivy 0.25.1 | LOCAL + LAB index | ✓ | 0.25.1 [VERIFIED] | None — required |
| PyMuPDF (fitz) | PDF extraction | ✓ | 1.27.2.3 [VERIFIED] | None — Phase 95 D-01 locked |
| python-docx | DOCX extraction | ✓ | 1.2.0 [VERIFIED] | None — Phase 95 D-04 locked |
| openpyxl | XLSX extraction (F-02) | ✓ | 3.1.5 [VERIFIED] | None — already in requirements.txt for exports |
| lxml | HTML extraction (F-01) | ✓ | 6.0.2 [VERIFIED transitive via python-docx] | None — already shipped |
| zstandard | cached_text BLOB (R-03) | ✓ | 0.25.0 [VERIFIED installed; transitive via pyiceberg in lock file] | None — add explicit pin to requirements.txt |
| defusedxml | XML attack defense for openpyxl (C-05) | ✗ | — | Could skip, but openpyxl docs strongly recommend; add to requirements.txt |
| beautifulsoup4 | (Originally planned for F-01) | ✗ | — | **Substitute lxml.html** (already available); recommended substitution |
| chardet | HTML encoding sniff fallback (F-01 chain) | ✗ | — | Per CONTEXT — fall back to cp1255 directly without chardet. Acceptable. |
| SQLite | local_index.sqlite3 | ✓ | 3.45.1 (stdlib) [VERIFIED] | None — stdlib |
| sqlite3 PRAGMA user_version | D-NEW-1 migrations | ✓ | Stdlib | None — PRAGMA is a stable SQL keyword since SQLite 3.2 |
| PyQt6 QThread / QTimer / pyqtSignal | U-03 / U-04 | ✓ | 6.10.2 [pinned] | None — desktop framework |
| socket.gethostname / hashlib / uuid | Phase 95 + Phase 97 ID derivation | ✓ | Stdlib | None |

**Missing dependencies with fallback:**

- `beautifulsoup4`: Substitute `lxml.html` (already available). No code change for Phase 97 plan — just write extractor against `lxml.html` API.
- `chardet`: Skip; rely on `<meta charset>` parse + utf-8 strict + cp1255 fallback chain. Skip is acceptable for the F-01 corpus.

**Missing dependencies with no fallback:**

- `defusedxml`: Strongly recommended for openpyxl XML attack defense. Add to `requirements.txt`. Risk if skipped: malicious XLSX can DOS user process. Low likelihood but high impact for a desktop tool exposed to user-supplied files.

## Validation Architecture

> Phase 97 has `workflow.nyquist_validation: true` (verified in `.planning/config.json`).

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (existing — 2,532 tests passing at v7.14.0 close) |
| Config file | `pytest.ini` (existing) |
| Quick run command | `pytest tests/test_phase_97_*.py -x` |
| Full suite command | `pytest tests/ -x` (~2 minutes) |

### Phase Decision → Test Map

| Decision | Behavior | Test Type | Automated Command | File Exists? |
|----------|----------|-----------|-------------------|-------------|
| D-NEW-1 | Migration v1 → v2 idempotent | unit | `pytest tests/test_local_indexer_migrations.py -x` | ❌ Wave 0 |
| D-NEW-1 | Three-fixture (empty / v1 / v2 no-op) | unit | `pytest tests/test_local_indexer_migrations.py::test_three_fixtures -x` | ❌ Wave 0 |
| D-NEW-1 | integrity_check fail surfaces error | unit | `pytest tests/test_local_indexer_migrations.py::test_integrity_check_fail -x` | ❌ Wave 0 |
| R-02 | Atomic rebuild closes searcher before rename | integration | `pytest tests/test_atomic_rebuild.py::test_close_before_rename -x` | ❌ Wave 0 |
| R-02 | Corruption → rebuild → search restored | integration | `pytest tests/test_atomic_rebuild.py::test_corrupt_recovery -x` | ❌ Wave 0 |
| R-02 | old-dir cleanup on next clean shutdown | integration | `pytest tests/test_atomic_rebuild.py::test_old_dir_cleanup -x` | ❌ Wave 0 |
| R-03 | cached_text round-trips through zstd | unit | `pytest tests/test_cached_text.py::test_roundtrip_hebrew -x` | ❌ Wave 0 |
| R-04 | Two-phase commit + synchronous=FULL on UPDATE | integration | `pytest tests/test_two_phase_durability.py::test_power_loss_simulation -x` | ❌ Wave 0 |
| R-01 | LOCAL search gated during recovery | integration | `pytest tests/test_recovery_gate.py::test_search_returns_empty_during_recovery -x` | ❌ Wave 0 |
| C-02 | Commit fires at byte/count/time threshold | integration | `pytest tests/test_commit_triggers.py -x` | ❌ Wave 0 |
| C-05 | Zip-bomb XLSX rejected via uncompressed-size check | unit | `pytest tests/test_xlsx_extraction.py::test_zip_bomb_defense -x` | ❌ Wave 0 |
| C-06 | Disk headroom warning fires below threshold | unit | `pytest tests/test_disk_headroom.py -x` | ❌ Wave 0 |
| F-01 | HTML chunking at h1/h2 + 20-para fallback | unit | `pytest tests/test_html_extraction.py -x` | ❌ Wave 0 |
| F-01 | RTL Hebrew HTML round-trip (no reversal — F-06) | unit | `pytest tests/test_html_extraction.py::test_rtl_logical_order_preserved -x` | ❌ Wave 0 |
| F-01 | Encoding chain (utf-8 / meta-charset / cp1255) | unit | `pytest tests/test_html_extraction.py::test_encoding_chain -x` | ❌ Wave 0 |
| F-02 | XLSX per-(sheet, 500-row) chunking | unit | `pytest tests/test_xlsx_extraction.py -x` | ❌ Wave 0 |
| F-02 | sheetView.rightToLeft → is_rtl metadata flag | unit | `pytest tests/test_xlsx_extraction.py::test_rtl_metadata -x` | ❌ Wave 0 |
| F-03 | CSV per-200-row chunking | unit | `pytest tests/test_csv_extraction.py -x` | ❌ Wave 0 |
| F-05 | CSV encoding chain (utf-8-sig / cp1255 / utf-16-le) | unit | `pytest tests/test_csv_extraction.py::test_encoding_chain -x` | ❌ Wave 0 |
| F-05 | csv.Sniffer detects `,` / `;` / `\t` | unit | `pytest tests/test_csv_extraction.py::test_delimiter_detection -x` | ❌ Wave 0 |
| F-06 | NO text reversal applied to HTML/XLSX/CSV | unit | `pytest tests/test_format_rtl_invariant.py -x` | ❌ Wave 0 |
| U-01 | Phase-aware ETA reports four sub-phases | unit | `pytest tests/test_phase_aware_eta.py -x` | ❌ Wave 0 |
| U-02 | scan_run_id discard removes only this-run docs | integration | `pytest tests/test_scan_run_id.py::test_discard_only_this_run -x` | ❌ Wave 0 |
| U-02 | scan_run_id NOT set on skipped-unchanged rows | unit | `pytest tests/test_scan_run_id.py::test_no_run_id_on_skipped -x` | ❌ Wave 0 |
| U-03 | FolderWalkWorker emits batched signals (no per-file emit) | integration | `pytest tests/test_folder_walk_worker.py -x` | ❌ Wave 0 |
| U-03 | NO Qt widget mutation from worker thread | integration | `pytest tests/test_folder_walk_worker.py::test_no_widget_mutation -x` | ❌ Wave 0 |
| U-04 | View All renders incrementally (no main-thread freeze) | integration | `pytest tests/test_view_all_incremental.py -x` | ❌ Wave 0 |
| U-04 | View All cap raised to 500 | unit | `pytest tests/test_view_all_cap.py -x` | ❌ Wave 0 |
| D-NEW-2 | Network drive ENOENT → folders.status='unreachable' | unit | `pytest tests/test_network_drive_semantics.py::test_enoent -x` | ❌ Wave 0 |
| D-NEW-2 | Transient ETIMEDOUT retries 3× with 2s backoff | unit | `pytest tests/test_network_drive_semantics.py::test_etimedout_retry -x` | ❌ Wave 0 |
| D-NEW-3 | File-change-during-index → re-queue, max 3 retries | integration | `pytest tests/test_changed_during_index.py -x` | ❌ Wave 0 |
| D-NEW-4 | Unsupported-extension rows pruned in migration | unit | `pytest tests/test_local_indexer_migrations.py::test_prune_unsupported -x` | ❌ Wave 0 |
| D-NEW-5 | chunk_locator format per file type | unit | `pytest tests/test_chunk_locator.py -x` | ❌ Wave 0 |
| D-NEW-6 | Help + About bilingual cleartext disclosure present | static | `pytest tests/test_privacy_disclosure_strings.py -x` | ❌ Wave 0 |
| D-NEW-7 (a) | Cloud-write gates at TOP — AST scanner | static | `pytest tests/test_phase_97_invariants.py::test_cloud_write_gates_at_top -x` | ❌ Wave 0 |
| D-NEW-7 (b) | Web LIBRARY_CODES allowlist `[]` — AST scanner | static | `pytest tests/test_phase_97_invariants.py::test_web_library_codes_empty_allowlist -x` | ❌ Wave 0 |
| D-NEW-7 (c) | is_local_sys_id recognizes 18-digit 97-prefixed | unit | `pytest tests/test_phase_97_invariants.py::test_is_local_sys_id -x` | ❌ Wave 0 |
| D-NEW-7 (d) | LOCAL RRF merge POST-_deduplicate | integration | `pytest tests/test_phase_97_invariants.py::test_local_post_dedup_merge -x` | ❌ Wave 0 |
| D-NEW-8 | mtime_ns INTEGER granularity | unit | `pytest tests/test_mtime_ns.py -x` | ❌ Wave 0 |
| Scale | 50K-file synthetic corpus smoke (skip in CI) | manual | `pytest tests/test_50k_scale_smoke.py --run-scale -x` | ❌ Wave 0 (marked `@pytest.mark.scale`, skip default) |

### Sampling Rate
- **Per task commit:** `pytest tests/test_phase_97_*.py -x` (subset relevant to changed surface)
- **Per wave merge:** `pytest tests/ -x` (full ~2,532+ test suite)
- **Phase gate:** Full suite green before `/gsd-verify-work`; scale smoke test pulled manually (~10-20 min on dev machine)

### Wave 0 Gaps
- [ ] `tests/test_local_indexer_migrations.py` — D-NEW-1 (3 tests: empty/v1/v2 + integrity_check + idempotency)
- [ ] `tests/test_atomic_rebuild.py` — R-02 (3 tests: close-before-rename + corruption recovery + old-dir cleanup)
- [ ] `tests/test_cached_text.py` — R-03 (1 test: zstd round-trip)
- [ ] `tests/test_two_phase_durability.py` — R-04 (1 test: power-loss simulation via subprocess kill)
- [ ] `tests/test_recovery_gate.py` — R-01 (1 test: is_searchable gate)
- [ ] `tests/test_commit_triggers.py` — C-02 (3 tests: bytes/files/time triggers fire correctly)
- [ ] `tests/test_xlsx_extraction.py` — F-02 + C-05 (5+ tests: zip-bomb, RTL, chunking, single-sheet, multi-sheet)
- [ ] `tests/test_disk_headroom.py` — C-06 (1 test)
- [ ] `tests/test_html_extraction.py` — F-01 (4+ tests: semantic + fallback + encoding + RTL)
- [ ] `tests/test_csv_extraction.py` — F-03 + F-05 (4+ tests: chunking + encoding chain + sniffer + uniform extraction)
- [ ] `tests/test_format_rtl_invariant.py` — F-06 (1 test)
- [ ] `tests/test_phase_aware_eta.py` — U-01 (1 test)
- [ ] `tests/test_scan_run_id.py` — U-02 (3 tests)
- [ ] `tests/test_folder_walk_worker.py` — U-03 (2 tests)
- [ ] `tests/test_view_all_incremental.py` + `tests/test_view_all_cap.py` — U-04 (2 tests)
- [ ] `tests/test_network_drive_semantics.py` — D-NEW-2 (2 tests)
- [ ] `tests/test_changed_during_index.py` — D-NEW-3 (1 test)
- [ ] `tests/test_chunk_locator.py` — D-NEW-5 (1 test)
- [ ] `tests/test_privacy_disclosure_strings.py` — D-NEW-6 (1 test, EN + HE assertions)
- [ ] `tests/test_phase_97_invariants.py` — D-NEW-7 (4 tests)
- [ ] `tests/test_mtime_ns.py` — D-NEW-8 (1 test)
- [ ] `tests/test_50k_scale_smoke.py` — Scale smoke (1 test, `@pytest.mark.scale`)
- [ ] Fixtures: `tests/fixtures/local_indexer/hebrew_sample.html`, `.xlsx`, `.csv`, `zip_bomb_sample.xlsx`, `multi_sheet_large.xlsx`

*Total Wave 0 gap: 22 new test files + 5 new fixtures.*

## Security Domain

> Required — `security_enforcement` is implicit (no explicit disable in config).

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | Phase 97 has no auth surface — desktop-only, no network endpoints introduced |
| V3 Session Management | no | No web sessions affected |
| V4 Access Control | partial | LOCAL data is per-machine, gated by OS user permissions; cloud-write gates (Phase 95 D-30) re-asserted by D-NEW-7 |
| V5 Input Validation | yes | User-supplied .html / .xlsx / .csv / .docx / .pdf files are untrusted input; F-01..F-05 extractors must validate (encoding, malformed structure, zip-bomb) |
| V6 Cryptography | partial | zstd is compression, NOT encryption. R-03 cached_text stores cleartext on disk; D-NEW-6 disclosure mirrors Phase 95 D-33 |
| V8 Data Protection | yes | Cached cleartext disclosure (D-NEW-6) + never-uploaded guarantee |
| V12 Files and Resources | yes | File-path normalization (Phase 95 D-42 `_canonical_filepath` already in place); junction loops avoided via `followlinks=False` (Phase 95 D-26 + D-NEW-2 extension); zip-bomb defense (C-05) |
| V14 Configuration | yes | New requirements.txt entries pinned; PyInstaller spec extends collect_all for new deps |

### Known Threat Patterns for Phase 97 stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| XLSX "billion laughs" XML bomb | Denial of Service | `defusedxml` import before openpyxl; openpyxl docs recommend [CITED: openpyxl PyPI] |
| XLSX zip bomb (huge uncompressed → small zip) | DoS | C-05 protocol: `zipfile.ZipFile.infolist()` sum file_size < 500 MB BEFORE handing to openpyxl |
| Malformed HTML triggering parser hang | DoS | `lxml.html.fromstring` has internal recursion limits; supplement with sanity check on input size (already capped by C-05 100 MB per-file) |
| Path traversal via crafted filename in fragments | Tampering | Phase 95 D-42 `_canonical_filepath` resolves; Phase 97 D-NEW-2 extends to UNC paths |
| Symbolic-link loop (junctions on Windows) | DoS | `os.walk(folder, followlinks=False)` (Phase 95 D-26); Phase 97 inherits |
| Cleartext exposure of user docs in SQLite cache | Information Disclosure | D-NEW-6 disclosure (mirrors Phase 95 D-33); OS-level disk encryption is user's responsibility |
| Race: file modified during extraction → partial/corrupt data committed | Tampering | D-NEW-3 protocol: stat-extract-stat; mismatch → `changed_during_index` + re-queue |
| Crash mid-batch leaves Tantivy ahead of SQLite | Tampering/Repudiation | Phase 95 D-21 two-phase commit + Phase 97 R-04 synchronous=FULL on critical UPDATE; R-01 recovery modal handles |
| Power loss leaves rebuild dir + old dir co-resident | Tampering | R-02 audit row in SQLite `_pending_cleanup`; next clean shutdown deletes |
| Concurrent index writers (multiple app instances) | Tampering | Phase 95 D-25 QMutex + tantivy's own `INDEX_WRITER_LOCK` file; existing retry pattern handles |
| LOCAL data leaks to cloud via export/sync paths | Information Disclosure | D-NEW-7 four AST guards re-assert Phase 95 D-30 (three gates at TOP of cloud-write functions) + Phase 95 D-46 (web LIBRARY_CODES allowlist `[]`) |

## Sources

### Primary (HIGH confidence)
- tantivy-py 0.25.1 source (installed binding) — `dir(writer)` enumeration verified all writer methods; `get_memory_usage()` absent [VERIFIED]
- openpyxl 3.1.5 `SheetView.rightToLeft` attribute exists [VERIFIED via local import 2026-05-25]
- zstandard 0.25.0 `ZstdCompressor(level=N).compress(bytes)` + `ZstdDecompressor().decompress(blob)` [VERIFIED locally with Hebrew/English benchmark]
- Python 3.11 stdlib: `csv.Sniffer().sniff(sample, delimiters=',;\\t')`, `zipfile.ZipInfo.file_size`, `os.stat().st_mtime_ns`, `uuid.uuid4().hex`, `shutil.disk_usage` [VERIFIED]
- sqlite3 3.45.1 (Python 3.11 stdlib) `PRAGMA user_version`, `PRAGMA integrity_check`, `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL|NORMAL` [VERIFIED]
- `/c/Genizahsearch/shared/local_indexer.py` — Phase 95 implementation (read in full 1737 lines)
- `/c/Genizahsearch/desktop/my_library_tab.py` — Phase 95 + Phase 96 implementation (read in full 1351 lines)
- `/c/Genizahsearch/.planning/phases/95-my-library/95-CONTEXT.md` — 46 locked decisions [READ]
- `/c/Genizahsearch/.planning/phases/95-my-library/95-RESEARCH.md` — Phase 95 research, esp. tantivy-py issue #297 [PARTIAL READ]
- `/c/Genizahsearch/.planning/phases/96-completing-my-library-feature-add-features-and-fix-bugs/96-CONTEXT.md` — Phase 96 D-F4 + View All cap [READ]
- `/c/Genizahsearch/.planning/phases/97-more-local-features/97-CONTEXT.md` — Phase 97 locked decisions (this phase) [READ]
- `/c/Genizahsearch/.planning/phases/97-more-local-features/97-CODEX-CRITIQUE.md` — Codex P0+P1 feedback [READ]

### Secondary (MEDIUM confidence)
- [SQLite User Forum on synchronous=FULL durability in WAL mode](https://sqlite.org/forum/info/d1a6bc7c4cd2baca) — explicit confirmation that WAL+FULL fsyncs every commit
- [SQLite forum: docs proposal on durability of default synchronous=FULL](https://sqlite.org/forum/forumpost/ec171a77a3)
- [SQLite commits are not durable under default settings — avi.im 2025](https://avi.im/blag/2025/sqlite-fsync/) — confirms WAL+NORMAL can lose last txn
- [SQLite's Durability Settings are a Mess — agwa.name](https://www.agwa.name/blog/post/sqlite_durability) — comprehensive coverage of WAL durability semantics
- [SQLite DB Migrations with PRAGMA user_version — Lev Lazinskiy](https://levlaz.org/sqlite-db-migrations-with-pragma-user_version/) — canonical pattern reference
- [openpyxl Optimised Modes (3.1.5)](https://openpyxl.pages.heptapod.net/openpyxl/optimized.html) — read-only mode + iter_rows
- [openpyxl PyPI page](https://pypi.org/project/openpyxl/) — recommends defusedxml install for XML attack protection
- [tantivy-py readthedocs Tutorials](https://tantivy-py.readthedocs.io/en/latest/tutorials/) — confirms writer.commit() is required for durability
- [tantivy-py IndexWriter type stubs](https://github.com/quickwit-oss/tantivy-py/blob/master/tantivy/tantivy.pyi) — verifies absence of get_memory_usage()
- [python-zstandard readthedocs](https://python-zstandard.readthedocs.io/en/latest/compressor.html) — ZstdCompressor API
- [PyQt6 Multi-Threading Best Practices — pythonguis.com](https://www.pythonguis.com/faq/multi-threading-dos-and-donts/) — signal/widget thread safety rules
- [Real Python: Use PyQt's QThread to Prevent Freezing GUIs](https://realpython.com/python-pyqt-qthread/) — canonical QThread + pyqtSignal pattern
- [lxml BeautifulSoup Parser (elementsoup)](https://lxml.de/elementsoup.html) — confirms lxml.html as standalone parser

### Tertiary (LOW confidence — needs validation during execute phase)
- F-01 "sparse" threshold (< 3 h1/h2 OR avg inter-heading < 5 paras) — heuristic, no empirical Hebrew HTML corpus tested. Planner adjusts during smoke testing.
- C-06 merge headroom factor 2× — based on segment-merge generic guidance, quickwit team has not published a tighter bound. Conservative; may over-warn.
- zstd level 3 producing 3-5× ratio on real Hebrew page text — extrapolated from repeating-string benchmark + general compression guidance.
- A4 (defusedxml is sufficient with `defuse_stdlib()`) — needs verification in execute phase.

## Metadata

**Confidence breakdown:**
- Standard stack (library versions, API surfaces): HIGH — all verified via local install + type stubs
- Architecture patterns (atomic rebuild, two-phase + FULL, migration runner): HIGH — derived from existing code + canonical patterns
- Phase 97 decisions (Codex P0+P1 folded): MEDIUM-HIGH — CONTEXT is comprehensive but Issue #1 (no get_memory_usage), #2 (no bs4), #3 (Windows reader-lock), and #4 (scan_run_id on skipped rows) need plan-time confirmation
- Pitfalls: HIGH — all rooted in verified library behavior or existing code
- Validation Architecture: HIGH — test framework existing; 22 new test files + 5 fixtures gap is explicit
- Security: MEDIUM-HIGH — STRIDE coverage is comprehensive, but defusedxml integration with openpyxl 3.1.5 needs verification

**Research date:** 2026-05-25
**Valid until:** 2026-06-25 (~30 days — Python ecosystem stable; tantivy-py minor version bumps could change writer API, watch for 0.26.x release)

## RESEARCH COMPLETE

**Phase:** 97 — More LOCAL features
**Confidence:** HIGH on primitives; MEDIUM-HIGH on Codex-folded decisions (4 issues surfaced for planner attention)

### Key Findings

- **Issue #1:** `writer.get_memory_usage()` does NOT exist in tantivy-py 0.25.1 [VERIFIED]. C-02 commit policy reduces to byte/count/time triggers only.
- **Issue #2:** `beautifulsoup4` is NOT installed [VERIFIED]. Substitute `lxml.html` (already shipped) — saves explicit dep + PyInstaller collect_all.
- **Issue #3:** R-02 atomic rebuild MUST include explicit `SearchEngine.close_local_searcher()` BEFORE `os.rename` on Windows (file-handle lock from live reader). 5-step protocol: build → validate → close-readers → 2-step rename → reload-readers → schedule cleanup.
- **Issue #4:** U-02 `scan_run_id` MUST be set ONLY on rows mutated in this run (not on already-up-to-date skipped rows), else Discard wrongly removes pre-existing data.
- **Standard stack verified:** tantivy 0.25.1, pymupdf 1.27.2.3, python-docx 1.2.0, openpyxl 3.1.5, lxml 6.0.2, zstandard 0.25.0 all installed. Phase 97 adds `zstandard` + `defusedxml` to `requirements.txt` (zstandard was transitive; defusedxml protects openpyxl from XML attacks).
- **Migration pattern:** `PRAGMA user_version` + try/except OperationalError on ALTER TABLE ADD COLUMN (idempotency); integrity_check FIRST in migration runner; surface "Reset My Library" on failure.
- **Atomic rebuild pattern:** Two-step `os.rename` (vacate target first) + SQLite `_pending_cleanup` audit row for crash-safe `.old-` directory deletion.
- **Two-phase durability:** Per-transaction escalation `PRAGMA synchronous=FULL` around critical UPDATE; restore NORMAL after. Lower cost than `wal_checkpoint(TRUNCATE)` alternative.
- **zstd benchmark:** Level 3 gives 76:1 ratio on repeating Hebrew/English; real corpus expected 3-5×; 13K files ≈ 130-400 MB compressed.

### File Created

`C:\Genizahsearch\.planning\phases\97-more-local-features\97-RESEARCH.md` (this file)

### Confidence Assessment

| Area | Level | Reason |
|------|-------|--------|
| Standard Stack | HIGH | All library versions verified via local `pip show` 2026-05-25 |
| Architecture (rebuild, migration, two-phase) | HIGH | Derived from existing Phase 95 code + verified canonical patterns |
| Phase 97 Decisions (Codex folded) | MEDIUM-HIGH | CONTEXT comprehensive; 4 issues surfaced require plan-time clarification |
| Pitfalls | HIGH | All rooted in verified library behavior or existing code |
| Validation Architecture | HIGH | 22 new test files + 5 fixtures gap explicit; existing pytest framework |
| Security | MEDIUM-HIGH | STRIDE coverage complete; defusedxml integration semantics need execute-phase verification |

### Open Questions (RESOLVED)

1. Should existing v7.14.0 user DBs trigger background cache-populate after migration? (RESOLVED: NO for Phase 97; document slow-first-rebuild; defer to v7.16+.)
2. What does "Reset My Library" preserve? (RESOLVED: preserve folder list re-prompt + opt-out set in session JSON; wipe SQLite + Tantivy.)
3. Does each new mutex acquire generate a fresh `scan_run_id`? (RESOLVED: YES; planner pins in plan.)
4. LOCAL Lab also needs atomic rebuild? (RESOLVED: YES; same protocol from `build_lab_side_index`.)
5. F-01 "sparse" extends to h3/h4? (RESOLVED: ship h1/h2 only this phase per CONTEXT; defer further heuristics.)

### Ready for Planning

Research complete. Planner can now create the Wave A-F PLAN.md files with the four research-surfaced issues resolved into explicit plan-level decisions:

- Wave A plans must encode: heap-sampling branch DROPPED (Issue #1); 5-step atomic-swap with reader-close (Issue #3); `scan_run_id` only on mutated rows (Issue #4); migration runner integrity_check first.
- Wave C plans must encode: lxml.html substitution (Issue #2); zip-bomb pre-check via `ZipInfo.file_size` sum; F-06 NO text reversal.
- All waves must encode: D-NEW-7 four AST invariant guards; bilingual EN+HE disclosure mirroring Phase 95 D-33.
