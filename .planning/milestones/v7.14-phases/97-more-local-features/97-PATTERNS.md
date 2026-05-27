# Phase 97: More LOCAL features — Pattern Map

**Mapped:** 2026-05-25
**Files analyzed:** 8 new files + ~5 modified files + 22 test files + 5 fixtures
**Analogs found:** 41 / 41 (100% match coverage — Phase 95/96 are extensive self-analogs)

> Consumed by `gsd-planner`. Each row gives the planner the exact analog file + line range, an excerpt of the load-bearing pattern, "mirror this" guidance, and explicit divergences. **Phase 97 is overwhelmingly an in-place extension** of Phase 95/96 infrastructure (`shared/local_indexer.py`, `desktop/my_library_tab.py`, `genizah_core.py::SearchEngine`). The four RESEARCH-surfaced plan-time issues (heap-sampling dropped, lxml.html substitution, atomic-swap 5-step protocol, scan_run_id mutated-rows-only) are encoded as explicit divergences in the relevant rows.
>
> **Sequencing per CONTEXT/RESEARCH:** Wave A (recovery foundation) → Wave B (commit + container safety) → Wave C (format extraction, parallelizable with B) → Wave D (capacity UX, requires A) → Wave E (indexing UX at scale) → Wave F (gap closure + privacy + tests).

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `shared/local_indexer_migrations.py` (NEW) | migration helper | SQLite DDL transform | `shared/local_indexer.py::init_sqlite` @ :247-297 (template for sqlite3 + PRAGMA usage); no migration analog in repo today | **role-match (template + new pattern)** |
| `shared/html_extractor.py` (NEW, F-01) — likely realized as `extract_html_pages` inside `shared/local_indexer.py` per RESEARCH §"Architectural Responsibility Map" | new extractor | file-I/O streaming → text chunks | `shared/local_indexer.py::extract_docx_pages` @ :398-420; `extract_pdf_pages` @ :346-395 | **exact (in-file extension)** |
| `shared/xlsx_extractor.py` (NEW, F-02 + C-05) — same realisation note as above | new extractor | file-I/O streaming + zip-bomb defense | `shared/local_indexer.py::extract_docx_pages` @ :398-420 (chunking shape); RESEARCH §"Pattern 5" verbatim template | **role-match (template + new)** |
| `shared/csv_extractor.py` (NEW, F-03 + F-05) — same realisation note | new extractor | file-I/O streaming + encoding chain | `shared/local_indexer.py::extract_txt` @ :423-453 (utf-8-sig + cp1255 chain); RESEARCH §"Example 2" verbatim template | **role-match (template + new)** |
| `shared/cached_text.py` (NEW, R-03) — likely realized as helpers inside `shared/local_indexer.py` | compression helper | bytes ↔ bytes | RESEARCH §"Example 3" verbatim template; no prior zstd use in repo | **NO IN-REPO ANALOG** |
| `shared/atomic_rebuild.py` (NEW, R-02) — likely realized as `LocalIndexer.rebuild_main_index_atomic()` method inside `shared/local_indexer.py` | recovery method | Tantivy + SQLite + filesystem | `shared/local_indexer.py::_commit_writer_with_retry` @ :1429-1482 (Windows retry pattern); `genizah_core.py::rebuild_lab_index` @ :742-790 (build-fresh-index template); RESEARCH §"Pattern 4" verbatim template | **role-match (extends retry pattern + new swap protocol)** |
| `shared/scan_run_id.py` (NEW, U-02) — likely realized as a constant + helpers inside `shared/local_indexer.py` | UUID plumbing | request-response | `shared/local_sys_id.py::_canonical_filepath` (style template — pure helpers in a sibling module); `uuid.uuid4().hex` is stdlib | **role-match (style template only)** |
| `shared/folder_walk_worker.py` (NEW, U-03) — likely realized as `FolderWalkWorker(QThread)` class inside `desktop/my_library_tab.py` | new QThread | event-driven (worker → UI) | `desktop/my_library_tab.py::LocalIndexerWorker` @ :473-519 (verbatim QThread template); `gui_threads.py::IndexerThread` (older template) | **exact (template)** |
| `shared/local_indexer.py` (MODIFY) | extension to existing | file-I/O + SQLite + Tantivy | self-analog (init_sqlite @ :247-297; build_local_schema @ :204-219; scan_all @ :753-886; _commit_batch @ :1484-1506; extract_*_pages @ :346-453) | **exact (in-file extension)** |
| `desktop/my_library_tab.py` (MODIFY) | UI extension | QWidget + persistence | self-analog (`_check_ceiling_*` @ :1174-1219; `_show_ceiling_confirm_dialog` @ :1221-1243; `LocalIndexerWorker` @ :473-519; `_UnifiedFileTreeWidget` @ :107-461) | **exact (in-file extension)** |
| `genizah_app.py` (MODIFY) | View All cap + incremental render | UI transform | self-analog (`_VIEW_ALL_PAGE_CAP = 200` @ :18777; `_aggregate_local_pages_with_separators` @ :111; `_get_local_pages_for_sys_id` Phase 96 helper) | **exact (in-file extension)** |
| `genizah_core.py` (MODIFY) | SearchEngine close/reload hooks | request-response | self-analog (`_open_local_searcher` @ :6686-6713; `reload_local_indexes` @ :6715-6727; `reload_local_lab_index` @ :6729-6758) | **exact (extend existing pair)** |
| `requirements.txt` / `requirements-desktop.txt` (MODIFY) | dep pin | config | self (existing `tantivy==0.25.1`, `pymupdf>=1.24`, `openpyxl==3.1.5`) | **exact** |
| `GenizahSearchPro.spec` (MODIFY) | PyInstaller config | build-time bundling | self (existing `collect_all('tantivy')`, `collect_all('pymupdf')` invocations) | **exact** |
| `docs/PRIVACY.md` + Help/About strings (MODIFY) | bilingual disclosure | static content | Phase 95 D-33 disclosure language pattern in `web/pages/help.py` + desktop Help | **role-match** |
| `tests/test_local_indexer_migrations.py` (NEW) | unit | static + I/O | `tests/test_local_indexer.py` @ :1-90 (fixture pattern); `tests/test_local_schema_evolution.py` (Phase 95 schema-introspection) | **role-match** |
| `tests/test_atomic_rebuild.py` (NEW) | integration | filesystem + Tantivy | `tests/test_local_commit_retry.py` (Windows retry pattern test) | **role-match** |
| `tests/test_cached_text.py` (NEW) | unit | round-trip | `tests/test_canonical_filepath.py` (pure-function round-trip pattern) | **role-match** |
| `tests/test_two_phase_durability.py` (NEW) | integration | fault-injection | `tests/test_local_two_phase_commit.py` (existing Phase 95 fault-inject) | **exact (template)** |
| `tests/test_recovery_gate.py` (NEW) | integration | UI gating | `tests/test_local_post_dedup_merge.py` :21-47 (engine stub pattern) | **role-match** |
| `tests/test_commit_triggers.py` (NEW) | integration | timing + counts | `tests/test_local_commit_retry.py` (commit-flow test) | **role-match** |
| `tests/test_html_extraction.py` (NEW) | unit + fixtures | I/O | `tests/test_local_indexer.py::test_pymupdf_hebrew_extraction_quality` @ :87-? (extraction fixture pattern) | **exact (template)** |
| `tests/test_xlsx_extraction.py` (NEW) | unit + fixtures | I/O | same as above | **exact (template)** |
| `tests/test_csv_extraction.py` (NEW) | unit + fixtures | I/O | same as above | **exact (template)** |
| `tests/test_format_rtl_invariant.py` (NEW) | static AST | static | `tests/test_local_filter_cascade.py` @ :39-72 (AST function walker — negated assertion form) | **exact (template, negated)** |
| `tests/test_phase_aware_eta.py` (NEW) | unit | timing | (no analog — synthesise from EWMA primitives) | **NO IN-REPO ANALOG** |
| `tests/test_scan_run_id.py` (NEW) | integration | Tantivy delete-by-term | `tests/test_local_delete_by_uid.py` (Phase 95 delete-by-term test — same tokenizer="raw" pattern) | **exact (template)** |
| `tests/test_folder_walk_worker.py` (NEW) | integration | QThread + signals | `tests/test_local_indexer_mutex.py` (Phase 95 QMutex test) | **role-match** |
| `tests/test_view_all_incremental.py` + `test_view_all_cap.py` (NEW) | unit | UI transform | (no direct analog — synthesise around `_VIEW_ALL_PAGE_CAP` constant scan) | **partial** |
| `tests/test_network_drive_semantics.py` (NEW) | unit | OS-error mocks | `tests/test_local_unavailable_folder.py` (Phase 95 unavailable-folder mock) | **exact (template)** |
| `tests/test_changed_during_index.py` (NEW) | integration | TOCTOU | (no direct analog — synthesise via `os.stat` mock + scan_all integration) | **partial** |
| `tests/test_chunk_locator.py` (NEW) | unit | static | `tests/test_local_indexer.py` (extraction-helper assertion style) | **role-match** |
| `tests/test_privacy_disclosure_strings.py` (NEW) | static | strings | (no analog — synthesise via file-content grep + EN/HE substring assertions) | **NO IN-REPO ANALOG** |
| `tests/test_phase_97_invariants.py` (NEW, D-NEW-7) | static AST | static | `tests/test_no_raw_storage_access.py` (Phase 87 AST scanner); `tests/test_pgp_filter_cascade.py` (verbatim AST template); `tests/test_local_filter_cascade.py` :39-72 (`_iter_function_defs` + `_function_contains_call`); `tests/test_local_post_dedup_merge.py` :21-47 (engine-stub pattern) | **exact (4 AST templates combined)** |
| `tests/test_mtime_ns.py` (NEW) | unit | I/O | `tests/test_local_indexer_incremental.py` (mtime-cache test pattern) | **role-match** |
| `tests/test_50k_scale_smoke.py` (NEW, `@pytest.mark.scale`) | scale (slow) | I/O | `tests/test_local_indexer_scale.py` (Phase 95 slow-marked scale test) | **exact (template)** |
| `tests/test_disk_headroom.py` (NEW, C-06) | unit | math + mocks | (no analog — synthesise via `shutil.disk_usage` mock) | **partial** |
| 5 NEW fixtures in `tests/fixtures/local_indexer/` | static data | n/a | Phase 95/96 fixtures alongside (hebrew_sample.pdf, single_word_per_line.pdf) | **n/a (new data)** |

---

## Pattern Assignments

Each section: read the analog at the cited file/line, copy that shape, apply the listed divergences.

---

### Wave A — D-NEW-1: SQLite Migration Module

**File:** `shared/local_indexer_migrations.py` (NEW)

**Primary analog (sqlite3 + PRAGMA setup style):** `shared/local_indexer.py:247-297` (`init_sqlite`).

**Excerpt** (lines 247-297):
```python
def init_sqlite(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")  # Pitfall #6 mitigation
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS folders (
            folder_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            path             TEXT    UNIQUE NOT NULL,
            added_at         REAL,
            last_scanned_at  REAL,
            status           TEXT    NOT NULL DEFAULT 'active'
        );
        CREATE TABLE IF NOT EXISTS processed_files (
            filepath  TEXT    PRIMARY KEY,
            mtime     REAL,
            size      INTEGER,
            sys_id    TEXT,
            status    TEXT    NOT NULL DEFAULT 'committed'
        );
        CREATE TABLE IF NOT EXISTS local_pages (
            sys_id    TEXT    NOT NULL,
            uid       TEXT    NOT NULL,
            page_num  INTEGER NOT NULL,
            PRIMARY KEY (sys_id, page_num)
        );
        ...
    """)
    conn.commit()
    return conn
```

**Secondary analog (RESEARCH §"Pattern 1" — verbatim template, lines 391-482 of 97-RESEARCH.md):**
```python
# shared/local_indexer_migrations.py
import sqlite3
from typing import Callable

_LATEST_VERSION = 2

def _migrate_1_to_2(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
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
    # D-NEW-4 prune
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

def run(conn: sqlite3.Connection) -> int:
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

**Mirror this:**
1. Module is **Qt-free + import-light** (only `sqlite3`, `typing.Callable`) so it can run inside `LocalIndexer.__init__` before any Tantivy work.
2. Wrap **every** `ALTER TABLE ADD COLUMN` with `_alter_safe()` — Pitfall #4 (ALTER TABLE not idempotent).
3. Run `PRAGMA integrity_check` **before** any migration; raise `RuntimeError` on failure (caller surfaces "Reset My Library" advanced-settings button per D-NEW-1).
4. Bump `PRAGMA user_version` **inside** the transaction so partial migrations don't advance the version.
5. Call site: `LocalIndexer.__init__` immediately after `init_sqlite(db_path)` and before any Tantivy operations. Add to `shared/local_indexer.py:519` (after `init_sqlite` returns).

**Divergences:**
- **No analog in repo for `PRAGMA user_version`** — first use. Pattern verified in RESEARCH (sqlite.org / levlaz.org citation).
- Migration script is module-level functions + a `_MIGRATIONS: dict[int, Callable]` registry — NOT a class. Keep it short (< 100 lines).

---

### Wave A — R-03: Cached Text Helpers (zstd)

**File:** `shared/cached_text.py` (NEW) — OR helpers inline in `shared/local_indexer.py`. Planner picks; module-level keeps test-isolation easier.

**No in-repo analog.** RESEARCH §"Example 3" is the verbatim template (97-RESEARCH.md:994-1022):
```python
import zstandard

_ZSTD_LEVEL = 3  # planner discretion; level 3 is the canonical balance

def compress_cached_text(text: str) -> tuple[bytes, int]:
    """Returns (compressed_blob, uncompressed_byte_len)."""
    payload = text.encode('utf-8')
    cctx = zstandard.ZstdCompressor(level=_ZSTD_LEVEL)
    return cctx.compress(payload), len(payload)

def decompress_cached_text(blob: bytes) -> str:
    dctx = zstandard.ZstdDecompressor()
    return dctx.decompress(blob).decode('utf-8')
```

**Mirror this:** Pure functions, no class. Use level 3 unless cache footprint exceeds 1 GB at 13K files (RESEARCH Open Q #1).

**Divergences:** First use of `zstandard` in the codebase. Add `zstandard>=0.22,<1.0` to `requirements.txt` + `requirements-desktop.txt`. Add `collect_all('zstandard')` to `GenizahSearchPro.spec`.

**Test:** `tests/test_cached_text.py::test_roundtrip_hebrew` — round-trip on Hebrew + English chunks; assert decompressed text equals input.

---

### Wave A — R-02: Atomic Tantivy Rebuild (5-step swap)

**File:** new method `LocalIndexer.rebuild_main_index_atomic()` in `shared/local_indexer.py`.

**Primary analog (Windows retry pattern):** `shared/local_indexer.py:1429-1482` (`_commit_writer_with_retry`).

**Excerpt** (lines 1429-1482):
```python
def _commit_writer_with_retry(self) -> None:
    """Commit the Tantivy writer with retry/backoff on Windows os error 5.
    ...Retry envelope: up to 3 attempts at 250 ms, 1 s, 2 s.
    Only retried on Windows-access-denied; all other exceptions propagate immediately.
    """
    import time as _time
    attempts = 0
    delays = (0.25, 1.0, 2.0)
    last_exc = None
    for delay in (0.0, *delays):
        if delay > 0:
            _time.sleep(delay)
        attempts += 1
        try:
            self._writer.commit()
            ...
            return
        except Exception as exc:
            last_exc = exc
            if not self._is_windows_access_denied(exc):
                raise
            logger.warning(...)
    raise ValueError(...)
```

**Secondary analog (build-fresh-index template):** `genizah_core.py:742-790` (`rebuild_lab_index`) — builds fresh index from scratch via SchemaBuilder + `tantivy.Index(schema, path=...)` + `writer = index.writer(heap_size=N)` + iterate-and-add-document.

**Tertiary analog (RESEARCH §"Pattern 4" verbatim template, 97-RESEARCH.md:573-661):**
```python
def rebuild_main_index_atomic(
    self,
    scan_run_id: str,
    close_searcher_cb: Callable[[], None],
    reload_searcher_cb: Callable[[], None],
) -> None:
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
            text = self._re_extract_from_source(sys_id, page_num)  # NULL fallback
            if text is None:
                continue
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
        os.rename(old_dir, self._index_dir)  # rollback
        raise

    # Step 6: reload live readers
    reload_searcher_cb()

    # Step 7: schedule delete of old_dir on next clean shutdown
    self._conn.execute(
        "INSERT INTO _pending_cleanup (path, kind) VALUES (?, 'rebuild_old')",
        (old_dir,),
    )
    self._conn.commit()
```

**Mirror this — exactly the 7-step protocol above.** RESEARCH Issue #3 is load-bearing: **close the SearchEngine reader BEFORE `os.rename`** or Windows `os error 5` strikes (the same class of error `_commit_writer_with_retry` handles for commits).

**Divergences from `rebuild_lab_index`:**
1. **Target dir is `Config.LOCAL_INDEX_DIR` (or `LOCAL_LAB_INDEX_DIR` for the LAB-side variant)** — NOT `Config.LAB_INDEX_DIR`.
2. **Two-step rename for Windows** — Pitfall #2 (POSIX overwrites; Windows raises FileExistsError).
3. **Source = cached_text (R-03)** — not source files. On NULL `cached_text` (Phase 95 legacy rows), fall back to source re-extraction (planner picks the exact policy split per CONTEXT "Claude's Discretion").
4. **Schedule `.old-<ts>` cleanup on next clean shutdown** via SQLite audit table — a crash between renames must leave the rollback path discoverable.

**Callback contract:** Accept `close_searcher_cb` + `reload_searcher_cb` so the rebuild method stays Qt-free; caller (`genizah_core.py::SearchEngine` or `desktop/my_library_tab.py`) supplies the bound methods.

**Tests:** `tests/test_atomic_rebuild.py::{test_close_before_rename, test_corrupt_recovery, test_old_dir_cleanup}` per VALIDATION.md.

---

### Wave A — R-04: WAL + synchronous=FULL Per-Transaction Escalation

**File:** modify `shared/local_indexer.py::_commit_batch` (`:1484-1506`).

**Primary analog (existing two-phase commit):** `shared/local_indexer.py:1484-1506`:
```python
def _commit_batch(self) -> None:
    if not self._pending_filepaths:
        return
    # Phase 1: Tantivy commit (with retry for Windows access-denied races).
    self._commit_writer_with_retry()
    # Phase 2: SQLite mark committed
    placeholders = ",".join("?" * len(self._pending_filepaths))
    self._conn.execute(
        f"UPDATE processed_files SET status = 'committed' "
        f"WHERE filepath IN ({placeholders})",
        self._pending_filepaths,
    )
    self._conn.commit()
    self._pending_filepaths.clear()
```

**RESEARCH §"Pattern 3" Variant A (preferred, 97-RESEARCH.md:539-557):**
```python
def _commit_batch_durable(self) -> None:
    self._commit_writer_with_retry()
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
```

**Mirror this — Variant A.** Lower latency than wal_checkpoint(TRUNCATE). The escalation is **per-transaction** so the rest of the indexing pass stays fast.

**Divergences from current `_commit_batch`:**
1. Bracket the SQLite UPDATE with `PRAGMA synchronous = FULL` → `BEGIN IMMEDIATE` → ... → `COMMIT` → `PRAGMA synchronous = NORMAL` (in `finally:` so escalation is always rolled back even on raise).
2. `BEGIN IMMEDIATE` (not bare `BEGIN`) so the lock escalation happens before any contention surface.

**Test:** `tests/test_two_phase_durability.py::test_power_loss_simulation` — kill the subprocess after Tantivy commit but before SQLite COMMIT; restart and assert recovery sees the pending row + R-01 modal fires.

---

### Wave A — R-01: Recovery UX Modal + LOCAL Search Gate

**File:** modify `desktop/my_library_tab.py::MyLibraryTab` + `genizah_core.py::SearchEngine`.

**Primary analog (existing gating pattern):** `genizah_core.py:6686-6713` (`_open_local_searcher`) — already returns `local_searcher = None` on failure, and the LOCAL query path at `genizah_core.py:6848-6874` (`_query_local_index`) already short-circuits when `local_searcher is None`.

**Excerpt** (lines 6692-6713):
```python
self.local_index = None
self.local_searcher = None
try:
    if os.path.isdir(Config.LOCAL_INDEX_DIR):
        from shared.local_indexer import build_local_schema
        schema = build_local_schema()
        local_index = tantivy.Index(schema, path=Config.LOCAL_INDEX_DIR)
        self.local_index = local_index
        self.local_searcher = local_index.searcher()
        ...
    else:
        LOGGER.info(...)
except Exception as e:
    LOGGER.warning(...)
    self.local_index = None
    self.local_searcher = None
```

**Mirror this:**
1. Add `MyLibraryTab.is_searchable: bool = False` instance attribute, initially `False` at `__init__`.
2. Set `is_searchable = True` when:
   - No pending rows AND no unclean shutdown marker → auto-set after `_init_indexer`.
   - User picks Resume / Restart / Skip in the recovery modal.
3. `SearchEngine._query_local_index` reads the flag via `self._my_library_tab.is_searchable` (or a callback set at startup). When `False`, return `[]` immediately + surface a banner via a new property `recovery_banner_message` consumed by `web/pages/help.py` analog or desktop status bar.

**Recovery modal pattern** (Phase 95 D-26 ceiling dialog @ `desktop/my_library_tab.py:1221-1243` provides the QMessageBox template):
```python
def _show_ceiling_confirm_dialog(
    self, file_count: int, total_bytes: int, title: str, body: str
) -> bool:
    formatted = "{}\n\n{}\n\n{}".format(body, ...)
    reply = QMessageBox.question(
        self, title, formatted,
        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
        QMessageBox.StandardButton.Cancel,
    )
    return reply == QMessageBox.StandardButton.Yes
```

Adapt: three buttons (Resume / Restart / Skip) via `QMessageBox.addButton(text, role)` pattern from `genizah_app.py:18787-18789` (existing View All large-file dialog).

**Divergences from Phase 95 ceiling dialog:**
- **Three buttons not two** — use `mb.addButton(tr("Resume"), QMessageBox.ButtonRole.AcceptRole)` × 3 with distinct roles + check `mb.clickedButton()`.
- Modal blocks until user chooses (no auto-dismiss).
- **Gate is read by `SearchEngine`** — cross-tab communication. Stash a weakref or callback on the engine at startup.

**Test:** `tests/test_recovery_gate.py::test_search_returns_empty_during_recovery`.

---

### Wave B — C-02: Commit Policy (byte/count/time, NO heap-sampling)

**File:** modify `shared/local_indexer.py::scan_all` + `_commit_batch`.

**Primary analog (existing batch-boundary check):** `shared/local_indexer.py:878-884`:
```python
# Batch commit boundary
if len(self._pending_filepaths) >= _COMMIT_BATCH_SIZE:
    self._commit_batch()

# Final commit
if not result["cancelled"] and self._pending_filepaths:
    self._commit_batch()
```

Constant at `local_indexer.py:71`:
```python
_COMMIT_BATCH_SIZE = 25                # D-21
```

**Secondary analog (RESEARCH §"Pattern 2" verbatim template, 97-RESEARCH.md:484-523):**
```python
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

**Mirror this:**
1. Add `_CommitTriggers` to `shared/local_indexer.py`.
2. Inside `scan_all`, after every file processed (around line 866, after `_file_finished_cb`):
   ```python
   self._commit_triggers.record_file(fsize)
   if self._commit_triggers.should_commit():
       self._commit_batch()  # uses Variant A durable commit (R-04)
       self._commit_triggers.reset()
   ```
3. Keep `_COMMIT_BATCH_SIZE = 25` as a **dead-code constant** (preserves Phase 95 D-02 dead-code preservation pattern) — comment it as "Phase 95 D-21 batch size, superseded by C-02 _CommitTriggers in Phase 97."

**Divergences from RESEARCH stub (Issue #1 in RESEARCH):**
- **DROP the heap-sampling conditional entirely.** tantivy-py 0.25.1 has no `writer.get_memory_usage()` method (RESEARCH §"Issue #1" verified via `dir(writer)` + tantivy.pyi). Do NOT write `if hasattr(writer, 'get_memory_usage'):` — that branch never fires and is misleading. Add a `# TODO(tantivy >= 0.26): add heap-size trigger when get_memory_usage() exists` comment.
- The `heap_size=N` argument to `index.writer()` remains a **memory ceiling**, NOT a commit trigger.

**Test:** `tests/test_commit_triggers.py` — assert commit fires within the expected window on a synthetic mixed-size corpus (CONTEXT C-02).

---

### Wave B — C-05: Per-File Size Cap + Zip-Bomb Limits

**File:** modify `shared/local_indexer.py::_iterate_supported_files` + new helper `_check_zip_bomb`.

**Primary analog (existing supported-extension filter):** `shared/local_indexer.py:_iterate_supported_files` (called from `scan_all` @ :790).

**Secondary analog (RESEARCH §"Pattern 5" excerpt, 97-RESEARCH.md:678-689):**
```python
_MAX_UNCOMPRESSED_BYTES = 500 * 1024 * 1024  # 500 MB per C-05
_MAX_CELLS_PER_SHEET = 100_000               # per C-05
_MAX_CHARS_PER_CHUNK = 1_000_000             # 1 MB per C-05
_MAX_FILE_SIZE = 100 * 1024 * 1024           # 100 MB hard skip per C-05

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
```

**Mirror this:**
1. Add the four constants and `_check_zip_bomb` to `shared/local_indexer.py`.
2. In `scan_all` (between `os.stat` and the cache-hit check), check `fsize > _MAX_FILE_SIZE` → mark `status='oversized'` + emit `file_finished_cb` + `continue`.
3. Before handing `.docx` or `.xlsx` to the extractor, call `_check_zip_bomb(filepath)`. On non-None return: mark `status='zip_bomb_suspected'` + emit file_finished_cb + `continue`. **No Tantivy doc emitted for these.**
4. The status enum gains two new values: `oversized` and `zip_bomb_suspected`. Update `update_file_status` in `desktop/my_library_tab.py:307` translation block (around line 320) to render the new states.

**Divergences:** Apply zip-bomb check only to zip-container formats (`.docx`, `.xlsx`). PDF/TXT/CSV/HTML get only the 100 MB raw-size check.

**Test:** `tests/test_xlsx_extraction.py::test_zip_bomb_defense` — fixture `zip_bomb_sample.xlsx` (synthesised; small zip with huge uncompressed claim).

---

### Wave B — D-NEW-8: mtime_ns Incremental Audit

**File:** modify `shared/local_indexer.py::scan_all` cache-hit check + `_index_one_file`.

**Primary analog (existing float-mtime cache check):** `shared/local_indexer.py:842-849`:
```python
# Check if unchanged
if (
    cached_row is not None
    and cached_row["status"] == "committed"
    and abs((cached_row["mtime"] or 0) - mtime) < 0.01
    and (cached_row["size"] or 0) == fsize
):
    result["skipped"] += 1
    continue
```

**Mirror this — replace float mtime with int mtime_ns:**
```python
# D-NEW-8: nanosecond-precision incremental audit
try:
    stat = os.stat(filepath)
    mtime_ns = stat.st_mtime_ns
    fsize = stat.st_size
except OSError as exc:
    ...

if (
    cached_row is not None
    and cached_row["status"] == "committed"
    and (cached_row["mtime_ns"] or 0) == mtime_ns
    and (cached_row["size"] or 0) == fsize
):
    result["skipped"] += 1
    continue
```

**Divergences:**
- Float `mtime` column kept for backward compat (Phase 95 D-02 dead-code preservation pattern). Read both during transition; write only `mtime_ns` going forward.
- Optional cheap-hash escape hatch on same-size+same-mtime_ns: first + last 64 KB SHA256. Advanced setting (opt-in), default OFF. Defer the UI surface — only ship the SQL column + helper function this phase.

**Test:** `tests/test_mtime_ns.py` — synthetic file with mtime changed at sub-second granularity; assert Phase 97 detects the change.

---

### Wave C — F-01: HTML Chunking via lxml.html (NOT BeautifulSoup)

**File:** new function `extract_html_pages` in `shared/local_indexer.py`.

**Primary analog (existing extractor shape):** `shared/local_indexer.py:398-420` (`extract_docx_pages`):
```python
def extract_docx_pages(filepath: str) -> Iterator[tuple[int, str, str]]:
    """Extract text in 20-paragraph chunks from a DOCX file (D-04).
    Yields (chunk_num, text, title).
    """
    doc = _DocxDoc(filepath)
    basename = os.path.basename(filepath)
    try:
        title = doc.core_properties.title or basename
    except Exception:
        title = basename

    paragraphs = [p.text for p in doc.paragraphs]
    chunk_num = 0
    for start in range(0, max(len(paragraphs), 1), _DOCX_CHUNK_PARAGRAPHS):
        chunk_num += 1
        chunk_paras = paragraphs[start : start + _DOCX_CHUNK_PARAGRAPHS]
        text = "\n".join(p for p in chunk_paras if p.strip())
        if text.strip():
            yield chunk_num, text, title
```

**Secondary analog (RESEARCH §"Example 1" verbatim template, 97-RESEARCH.md:865-937):**
```python
import lxml.html
import lxml.etree

_HTML_PARSER = lxml.html.HTMLParser(encoding=None)

def _detect_html_encoding(raw_bytes: bytes) -> str:
    """F-01 encoding chain: <meta charset> → byte-sniff → cp1255 fallback."""
    head = raw_bytes[:1024].decode('ascii', errors='ignore').lower()
    import re
    m = re.search(r'<meta[^>]+charset\s*=\s*["\']?([\w-]+)', head)
    if m:
        return m.group(1).strip()
    try:
        raw_bytes[:4096].decode('utf-8', errors='strict')
        return 'utf-8'
    except UnicodeDecodeError:
        pass
    return 'cp1255'

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
    for tag in tree.iter('script', 'style'):
        tag.getparent().remove(tag)
    title_elem = tree.find('.//title')
    title = (title_elem.text or os.path.basename(filepath)).strip() if title_elem is not None else os.path.basename(filepath)

    headings = list(tree.iter('h1', 'h2'))
    paragraphs = list(tree.iter('p'))
    avg_inter = (len(paragraphs) / max(len(headings), 1)) if headings else 0
    use_semantic = len(headings) >= 3 and avg_inter >= 5

    if use_semantic:
        for chunk_num, h in enumerate(headings, start=1):
            heading_text = (h.text_content() or '').strip()
            buf = [heading_text] if heading_text else []
            sib = h.getnext()
            while sib is not None and sib.tag not in ('h1', 'h2'):
                buf.append(sib.text_content() or '')
                sib = sib.getnext()
            text = "\n".join(s.strip() for s in buf if s.strip())
            if text:
                yield chunk_num, text, title, f"§ {heading_text or 'section ' + str(chunk_num)}"
    else:
        chunk_num = 0
        for start in range(0, len(paragraphs), 20):
            chunk_num += 1
            slice_ = paragraphs[start:start + 20]
            text = "\n".join((p.text_content() or '').strip() for p in slice_ if (p.text_content() or '').strip())
            if text:
                end = min(start + 20, len(paragraphs))
                yield chunk_num, text, title, f"¶ {start + 1}-{end}"
```

**Mirror this** verbatim.

**Divergences from RESEARCH Issue #2 (load-bearing substitution):**
- **DO NOT use BeautifulSoup.** `pip show beautifulsoup4` returns "not found"; `lxml==6.0.2` is already shipped via `python-docx` transitive dep. Using `lxml.html` directly avoids adding a new dep + a new `collect_all` invocation in `.spec`.
- **F-06 NO TEXT REVERSAL** — `lxml.html` returns logical-order Hebrew strings already. Honor `<html dir="rtl">` / `<body dir="rtl">` as a `is_rtl: bool` metadata flag fed to the chunk_locator/Browse panel — but DO NOT call Phase 95 `_fix_rtl_line` / `_fix_rtl_page`. Pin via `tests/test_html_extraction.py::test_rtl_logical_order_preserved`.

**Sparse heuristic (CONTEXT F-01 + Claude's Discretion):**
- Sparse = `len(headings) < 3 OR avg_inter < 5`.
- Planner may adjust during smoke test on representative corpus.

**Test:** `tests/test_html_extraction.py` — 4+ tests per VALIDATION.md.

---

### Wave C — F-02: XLSX Chunking + Zip-Bomb Defense

**File:** new function `extract_xlsx_pages` in `shared/local_indexer.py`.

**Primary analog (existing extractor shape):** `extract_docx_pages` @ :398-420 (same as F-01).

**Secondary analog (RESEARCH §"Pattern 5" verbatim template, 97-RESEARCH.md:692-748):**
```python
_MAX_UNCOMPRESSED_BYTES = 500 * 1024 * 1024
_MAX_CELLS_PER_SHEET = 100_000
_MAX_CHARS_PER_CHUNK = 1_000_000
_XLSX_ROW_WINDOW = 500

def extract_xlsx_pages(filepath: str) -> Iterator[tuple[int, str, str, str, bool]]:
    """Phase 97 F-02 — per (sheet, 500-row window) Tantivy doc.
    Each window emitted as one chunk with chunk_locator like 'Synopsis!R1:R500' (D-NEW-5).
    Yields (chunk_num, text, title, locator, is_rtl).
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
            is_rtl = bool(getattr(ws.sheet_view, 'rightToLeft', False))
            rows_in_window: list[str] = []
            cells_seen = 0
            window_start_row = 1
            for row_num, row in enumerate(
                ws.iter_rows(values_only=True), start=1
            ):
                cell_strs = [str(c) if c is not None else "" for c in row]
                cells_seen += len(cell_strs)
                if cells_seen > _MAX_CELLS_PER_SHEET:
                    raise XlsxZipBombSuspected(...)
                line = " | ".join(cell_strs)
                if line.strip():
                    rows_in_window.append(line)
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

**Mirror this** verbatim. F-04 uniform extraction (`cell1 | cell2 | cell3`) is built into the row loop. F-06 `is_rtl` flag honored from `sheet_view.rightToLeft` as metadata only.

**Divergences:**
- New exception class `XlsxZipBombSuspected(Exception)` added at module top.
- `openpyxl` is already in `requirements.txt` (was pinned for export functionality). Add `collect_all('openpyxl')` to `.spec` only if not present.
- Add `defusedxml>=0.7` to `requirements.txt` for XML-bomb defense (Pitfall #6) — `import defusedxml; defusedxml.defuse_stdlib()` at module load before `openpyxl`.

**Tests:** `tests/test_xlsx_extraction.py` — 5+ tests per VALIDATION.md including zip-bomb + RTL metadata + multi-sheet chunking.

---

### Wave C — F-03 + F-05: CSV Chunking + Encoding Chain

**File:** new function `extract_csv_pages` in `shared/local_indexer.py`.

**Primary analog (existing encoding chain):** `shared/local_indexer.py:423-453` (`extract_txt`):
```python
def extract_txt(filepath: str) -> Iterator[tuple[int, str, str]]:
    try:
        with open(filepath, "r", encoding="utf-8-sig", errors="strict") as f:
            text = f.read()
    except UnicodeDecodeError as utf8_err:
        try:
            with open(filepath, "r", encoding="cp1255", errors="strict") as f:
                text = f.read()
            logger.info(...)
        except UnicodeDecodeError as cp1255_err:
            raise EncodingError(
                f"Cannot decode {filepath} as utf-8-sig or cp1255: ..."
            ) from cp1255_err
    yield (1, text, os.path.basename(filepath))
```

**Secondary analog (RESEARCH §"Example 2" verbatim template, 97-RESEARCH.md:941-991):**
```python
import csv

_CSV_ROW_WINDOW = 200  # F-03
_CSV_ENCODINGS = ('utf-8-sig', 'cp1255', 'utf-16-le')  # F-05

def extract_csv_pages(filepath: str) -> Iterator[tuple[int, str, str, str]]:
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

**Mirror this** verbatim.

**Divergences:**
- Wider encoding chain than `extract_txt` (adds `utf-16-le` Excel default for non-ASCII).
- F-04 uniform extraction baked into the row loop.
- F-06: `csv` stdlib produces logical-order strings already — NO `_fix_rtl_*`.

**Tests:** `tests/test_csv_extraction.py` — 4+ tests including encoding chain, delimiter detection, RTL preservation.

---

### Wave C — F-06: Format-RTL Invariant (NEGATIVE AST guard)

**File:** new test `tests/test_format_rtl_invariant.py`.

**Primary analog (verbatim AST template, NEGATED assertion):** `tests/test_local_filter_cascade.py:39-72`:
```python
def test_local_filter_applied_within_results_cascade():
    source = GENIZAH_APP_PY.read_text(encoding='utf-8')
    tree = ast.parse(source)

    target_functions = {'_apply_results_table_filters', '_apply_comp_tree_filters'}
    found = {}
    for func in _iter_function_defs(tree):
        if func.name in target_functions:
            found[func.name] = func

    offenders = []
    for fname, func in found.items():
        if not _function_contains_call(func, '_apply_local_filter'):
            offenders.append((fname, func.lineno))

    assert not offenders, (...)
```

**Mirror this (negated):**
```python
def test_format_rtl_invariant_no_fix_rtl_in_new_extractors():
    """F-06: HTML/XLSX/CSV extractors MUST NOT call Phase 95 _fix_rtl_*."""
    source = LOCAL_INDEXER_PY.read_text(encoding='utf-8')
    tree = ast.parse(source)
    target_functions = {'extract_html_pages', 'extract_xlsx_pages', 'extract_csv_pages'}
    found = {f.name: f for f in _iter_function_defs(tree) if f.name in target_functions}
    offenders = []
    for fname, func in found.items():
        for forbidden in ('_fix_rtl_line', '_fix_rtl_page'):
            if _function_contains_call(func, forbidden):
                offenders.append((fname, forbidden, func.lineno))
    assert not offenders, (
        "F-06 violation: format extractor calls Phase 95 PDF mirror-reversal "
        "helper; HTML/XLSX/CSV strings are already in logical order: "
        + str(offenders)
    )
```

**Divergences:** Negative assertion — the forbidden calls must NOT appear.

---

### Wave D — C-01: Soft Ceiling Replacement (50K / 50 GB)

**File:** modify `desktop/my_library_tab.py` constants + `_check_ceiling_single_folder` + `_check_ceiling_refresh_aggregate`.

**Primary analog (existing hard-stop):** `desktop/my_library_tab.py:64-65, 1174-1219`:
```python
_MAX_FILES_CEILING = 5000
_MAX_BYTES_CEILING = 2 * 1024 ** 3   # 2 GB

def _check_ceiling_single_folder(self, folder_path: str) -> bool:
    if self._indexer is None:
        return True
    file_count, total_bytes = self._indexer.prescan_count(folder_path)
    if file_count > _MAX_FILES_CEILING or total_bytes > _MAX_BYTES_CEILING:
        return self._show_ceiling_confirm_dialog(
            file_count, total_bytes,
            tr("Add folder — pre-scan"),
            tr("Adding folder '{}' will index {:,} files ({}).").format(
                folder_path, file_count, self._human_bytes(total_bytes)
            ),
        )
    return True
```

**Mirror this — bump constants only:**
```python
# Phase 97 C-01 — soft warning thresholds (was Phase 95 hard-stop at 5K/2GB)
_MAX_FILES_CEILING = 50_000
_MAX_BYTES_CEILING = 50 * 1024 ** 3   # 50 GB
```

**Divergences:**
- **Same dialog shape** — Phase 95 already used `QMessageBox.Yes | Cancel` which is a soft prompt. The change is constant-only.
- **Sequencing per CONTEXT C-01 Codex P0:** C-01 lands AFTER Wave A (R-03, R-02, D-NEW-1). Wave-D ordering pins this.

**Test:** Phase 95 already has `tests/test_local_ceiling_enforcement.py`. Extend to assert the new thresholds; do NOT delete the old test.

---

### Wave D — C-04: Persisted Folder Counters + Aggregate View

**File:** modify `shared/local_indexer.py::scan_all` to update counters per commit batch; modify `desktop/my_library_tab.py::_UnifiedFileTreeWidget` to read counters from SQLite.

**Primary analog (existing folder list UI):** `desktop/my_library_tab.py:1249-1289` (`_refresh_folder_list_ui`).

**Excerpt** (lines 1270-1289):
```python
for folder in self._indexer.list_folders():
    path = folder["path"]
    status = folder.get("status", "active")
    item = QListWidgetItem(path)
    item.setData(Qt.ItemDataRole.UserRole, path)
    if status == "unavailable":
        item.setForeground(QColor("#f39c12"))
        item.setToolTip(...)
    self._folder_list.addItem(item)
```

**Mirror this:**
1. After D-NEW-1 migration, `folders` table has 5 new columns: `indexed_count`, `error_count`, `pending_count`, `oversized_count`, `last_aggregate_at`.
2. In `LocalIndexer.scan_all` (and `_commit_batch`), update counters in the same SQLite transaction as `status='committed'` UPDATE. Read-side queries are O(1) per folder.
3. UI default = aggregate view (4 counters per folder row); click drill-down → existing `_UnifiedFileTreeWidget` per-file rows for that folder only.

**Divergences:**
- New SQL UPDATE in `_commit_batch` (after the existing `processed_files` UPDATE, inside the same `synchronous=FULL` transaction from R-04).
- `_UnifiedFileTreeWidget.populate_for_folder` already exists (Phase 96) — extend it with a default "aggregate" mode that does NOT materialize per-file rows until the user expands.

**Test:** Add `tests/test_local_indexer_migrations.py::test_folder_counters_updated_in_scan_all`.

---

### Wave D — C-06: Disk Indicator + Merge Headroom

**File:** new method `MyLibraryTab._update_disk_indicator()` + new helper `LocalIndexer.estimate_index_size()`.

**No direct in-repo analog.** Primitives:
- `shutil.disk_usage(path)` returns `(total, used, free)` tuple.
- `os.path.getsize(seg_file) for seg_file in os.listdir(index_dir)` for current index size.

**Mirror this:**
```python
def _update_disk_indicator(self) -> None:
    """Phase 97 C-06: live disk indicator with merge headroom."""
    import shutil
    if self._indexer is None:
        return
    index_size = self._indexer.estimate_index_size()
    usage = shutil.disk_usage(Config.LOCAL_INDEX_DIR)
    headroom = usage.free - 2 * index_size  # 2× current index size for merge scratch
    label_text = tr("Index size: {} / {} free").format(
        self._human_bytes(index_size), self._human_bytes(usage.free)
    )
    if headroom < 1024 ** 3:  # < 1 GB headroom
        label_text += tr(" ⚠ low merge headroom")
    self._disk_label.setText(label_text)
```

**Divergences:** First use of `shutil.disk_usage` in repo. Add a QLabel widget to the existing MyLibraryTab top toolbar (between folder list and refresh row).

**Test:** `tests/test_disk_headroom.py` — `shutil.disk_usage` mock + assert warning fires when `(free - 2×index_size) < 1 GB`.

---

### Wave E — U-01: Phase-Aware ETA

**File:** modify `desktop/my_library_tab.py::LocalIndexerWorker` signals + `MyLibraryTab._on_progress`.

**Primary analog (existing single-phase progress):** `desktop/my_library_tab.py:487-489, 504-505`:
```python
progress_updated = pyqtSignal(int, int, str)
file_finished = pyqtSignal(str, str, int, str)
finished_signal = pyqtSignal(dict)
error_signal = pyqtSignal(str)
...
def _on_progress(current: int, total: int, filename: str) -> None:
    self.progress_updated.emit(current, total, filename)
```

**Mirror this:**
1. Add new signal: `phase_updated = pyqtSignal(str, int, int)  # phase_name, current, total`. Phases: `'walking'`, `'extracting'`, `'committing'`, `'rebuilding_lab'`.
2. Add helper class `_PhaseAwareETA` with four separate EWMA smoothers (one per phase) and a `compose_overall_eta() → float` method.
3. UI: stack 4 thin progress bars (one per phase) above the existing combined progress bar.

**Divergences:** No existing EWMA helper — synthesise. Use exponential weighting with `α = 0.2` (typical for I/O-bound smoothing).

**Test:** `tests/test_phase_aware_eta.py` — assert 4 phases tracked independently; combined ETA ≠ naive sum.

---

### Wave E — U-02: scan_run_id (UUID per Run)

**File:** modify `shared/local_indexer.py::build_local_schema` + `scan_all` + new `discard_run` / `keep_run` methods.

**Primary analog (existing `unique_id` raw-tokenizer pattern):** `shared/local_indexer.py:204-219`:
```python
def build_local_schema() -> tantivy.Schema:
    builder = tantivy.SchemaBuilder()
    # CRITICAL: tokenizer_name="raw" - main index at genizah_core.py:5125 omits this
    # and is rebuilt from scratch so the bug stays latent. For LOCAL where incremental
    # delete IS the central operation, raw is mandatory. tantivy-py issue #297.
    builder.add_text_field("unique_id", stored=True, tokenizer_name="raw")
    builder.add_text_field("content", stored=True, tokenizer_name="whitespace")
    ...
```

**Mirror this — add scan_run_id field:**
```python
builder.add_text_field("scan_run_id", stored=True, tokenizer_name="raw")  # U-02 + Pitfall #7
```

**Discard/Keep ops** (RESEARCH §"Issue #4 + Pitfall #7"):
```python
def discard_run(self, run_id: str) -> int:
    """Phase 97 U-02: remove all docs/rows from this run."""
    self._writer.delete_documents("scan_run_id", run_id)
    self._writer.commit()
    cur = self._conn.execute(
        "DELETE FROM processed_files WHERE scan_run_id = ?", (run_id,)
    )
    self._conn.commit()
    return cur.rowcount

def keep_run(self, run_id: str) -> None:
    """Phase 97 U-02: explicit final commit + leave run_id as audit trail."""
    self._writer.commit()
    # No SQL — scan_run_id stays in processed_files for future audit
```

**Divergences (load-bearing per RESEARCH Issue #4):**
- **`scan_run_id` is set ONLY on rows INSERTED or UPDATED to `status='pending'` within this scan_run.** NOT on rows the scan skipped because they were already up-to-date. The cache-hit branch in `scan_all` @ :842-849 must NOT write `scan_run_id`.
- Verify field tokenization with `tokenizer_name="raw"` or `writer.delete_documents` silently does nothing (Pitfall #7).

**Test:** `tests/test_scan_run_id.py::{test_discard_only_this_run, test_no_run_id_on_skipped}`.

---

### Wave E — U-03: FolderWalkWorker QThread

**File:** new class `FolderWalkWorker(QThread)` in `desktop/my_library_tab.py`.

**Primary analog (verbatim QThread template):** `desktop/my_library_tab.py:473-519` (`LocalIndexerWorker`):
```python
class LocalIndexerWorker(QThread):
    progress_updated = pyqtSignal(int, int, str)
    file_finished = pyqtSignal(str, str, int, str)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, indexer: LocalIndexer) -> None:
        super().__init__()
        self._indexer = indexer
        self._cancel_requested = False

    def cancel(self) -> None:
        self._cancel_requested = True

    def run(self) -> None:
        try:
            def _on_progress(current: int, total: int, filename: str) -> None:
                self.progress_updated.emit(current, total, filename)
            ...
            result = self._indexer.scan_all(
                cancel_check=lambda: self._cancel_requested
            )
            self.finished_signal.emit(result)
        except Exception as exc:
            logger.exception(...)
            self.error_signal.emit(str(exc))
```

**Mirror this:**
```python
class FolderWalkWorker(QThread):
    """Phase 97 U-03: filesystem walk off-UI; emits batched file-metadata."""
    # Batched signal: list of (filepath, mtime_ns, size) tuples
    batch_emitted = pyqtSignal(list)
    finished_signal = pyqtSignal(int, int)  # total_files, total_bytes
    error_signal = pyqtSignal(str)

    BATCH_SIZE = 100
    BATCH_TIMEOUT = 0.5

    def __init__(self, folder_paths: list[str]) -> None:
        super().__init__()
        self._folder_paths = folder_paths
        self._cancel_requested = False

    def cancel(self) -> None:
        self._cancel_requested = True

    def run(self) -> None:
        import os, time
        try:
            batch: list = []
            last_emit = time.monotonic()
            total_files = 0
            total_bytes = 0
            for folder in self._folder_paths:
                for root, dirs, files in os.walk(folder, followlinks=False):
                    if self._cancel_requested:
                        return
                    for name in files:
                        fp = os.path.join(root, name)
                        try:
                            stat = os.stat(fp)
                        except OSError:
                            continue
                        batch.append((fp, stat.st_mtime_ns, stat.st_size))
                        total_files += 1
                        total_bytes += stat.st_size
                        if (len(batch) >= self.BATCH_SIZE
                            or time.monotonic() - last_emit >= self.BATCH_TIMEOUT):
                            self.batch_emitted.emit(batch)
                            batch = []
                            last_emit = time.monotonic()
            if batch:
                self.batch_emitted.emit(batch)
            self.finished_signal.emit(total_files, total_bytes)
        except Exception as exc:
            logger.exception("FolderWalkWorker")
            self.error_signal.emit(str(exc))
```

**Divergences (load-bearing per RESEARCH "Don't Hand-Roll" table):**
- **NO QWidget mutation from the worker thread.** Worker emits `pyqtSignal(list)` carrying batches; a UI-thread slot on `MyLibraryTab` materializes `QTreeWidgetItem`s. Per-file signals at 100K files = 100K Qt event-loop dispatches → UI freeze; batches preserve throughput.
- Throttle: emit every 100 files OR 0.5 sec, whichever comes first.
- D-NEW-2 network drive semantics integrated: pre-check `os.path.isdir(folder)` before walking; on ENOENT/ETIMEDOUT mark `status='unreachable'` (delegate to `LocalIndexer.list_folders`).

**Tests:** `tests/test_folder_walk_worker.py::{test_batched_signal, test_no_widget_mutation}`.

---

### Wave E — U-04: View All 200 → 500 + Incremental Render

**File:** modify `genizah_app.py:18774-18804` View All path.

**Primary analog (existing 200-cap + warning dialog):** `genizah_app.py:18774-18804`:
```python
_VIEW_ALL_PAGE_CAP = 200
if len(_raw_pages) > _VIEW_ALL_PAGE_CAP:
    total_pages = len(_raw_pages)
    msg = tr("This file has {n} pages. Viewing all at once may freeze the window.").format(n=total_pages)
    detail = tr("Show first {cap} pages").format(cap=_VIEW_ALL_PAGE_CAP)
    from PyQt6.QtWidgets import QMessageBox
    mb = QMessageBox(self)
    mb.setWindowTitle(tr("Large file"))
    mb.setText(msg)
    ...
    if mb.clickedButton() == btn_cancel:
        ...
        return self._open_local_browse_page(sys_id, p_num=initial_p, hit_data=res)
    _raw_pages = _raw_pages[:_VIEW_ALL_PAGE_CAP]
```

**Aggregation helper:** `genizah_app.py:111` (`_aggregate_local_pages_with_separators`).

**Mirror this — bump cap + add incremental render:**
1. Change `_VIEW_ALL_PAGE_CAP = 200` → `500` (CONTEXT U-04 P2).
2. Replace the synchronous "build whole HTML, set text, return" path with incremental:
   ```python
   # Render first 50 pages immediately
   first_batch = _raw_pages[:50]
   html = self._build_pages_html(first_batch, is_pdf, lang)
   self.browse_text_edit.setHtml(html)
   # Append remaining batches via QTimer.singleShot(0, ...)
   remaining = _raw_pages[50:]
   if remaining:
       self._view_all_remaining = remaining
       QTimer.singleShot(0, self._append_next_view_all_batch)
   ```
3. New method `_append_next_view_all_batch(self)` that pops the next 50 pages, builds HTML, appends via `self.browse_text_edit.append(html)` (or moves cursor + insertHtml), and re-schedules itself if more remain.

**Divergences:**
- Cap bump is a one-line constant change.
- Incremental render is the load-bearing addition (CONTEXT U-04 P2 + Pitfall #10).
- **DO NOT** introduce a full QThread (D-F7 full refactor remains deferred per CONTEXT). The fix is `QTimer.singleShot(0, ...)` interleaving on the main thread.

**Tests:** `tests/test_view_all_cap.py` (assert constant is 500); `tests/test_view_all_incremental.py` (assert QTimer.singleShot pattern present in the View All path via AST scan).

---

### Wave F — D-NEW-2: Network Drive Semantics

**File:** modify `shared/local_indexer.py::scan_all` folder pre-check (existing logic @ :772-779).

**Primary analog (existing unavailable-folder pattern):** `shared/local_indexer.py:772-779`:
```python
# D-40: check folder availability
if not os.path.isdir(folder_path):
    self._conn.execute(
        "UPDATE folders SET status = 'unavailable' WHERE folder_id = ?",
        (folder_id,),
    )
    self._conn.commit()
    logger.info("Folder '%s' unavailable — preserving existing rows", folder_path)
    continue
```

**Mirror this — extend with retry + errno discrimination:**
```python
import errno

def _check_folder_reachable(folder_path: str, max_retries: int = 3) -> tuple[bool, str]:
    """Phase 97 D-NEW-2: returns (reachable, status_label).
    status_label ∈ {'active', 'unreachable', 'timeout'}.
    """
    delays = (2.0, 2.0, 2.0)  # 2s backoff × 3
    for attempt in range(max_retries):
        try:
            if os.path.isdir(folder_path):
                return True, 'active'
            return False, 'unreachable'  # ENOENT
        except OSError as exc:
            if exc.errno in (errno.ETIMEDOUT, errno.EAGAIN):
                if attempt < max_retries - 1:
                    time.sleep(delays[attempt])
                    continue
                return False, 'timeout'
            if exc.errno in (errno.ENOENT, errno.EACCES):
                return False, 'unreachable'
            raise
    return False, 'timeout'
```

**Divergences:**
- Existing logic is single-shot; add 3-retry × 2s backoff for ETIMEDOUT only.
- `OSError.errno` discrimination (RESEARCH Pitfall #9 + A8 assumption).

**Test:** `tests/test_network_drive_semantics.py::{test_enoent, test_etimedout_retry}`.

---

### Wave F — D-NEW-3: File-Change-During-Index

**File:** modify `shared/local_indexer.py::_index_one_file` (bracket extraction with `os.stat`).

**Primary analog (existing single-stat pre-extract):** `scan_all` @ :832-839:
```python
try:
    stat = os.stat(filepath)
    mtime = stat.st_mtime
    fsize = stat.st_size
except OSError as exc:
    logger.warning("scan_all: stat failed for %s: %s", filepath, exc)
    result["errors"] += 1
    continue
```

**Mirror this — bracket extraction:**
```python
# Pre-extraction stat
pre = os.stat(filepath)
status, pages = self._extract_to_chunks(filepath, folder_id, cancel_check)
# Post-extraction stat
try:
    post = os.stat(filepath)
    if post.st_mtime_ns != pre.st_mtime_ns or post.st_size != pre.st_size:
        status = 'changed_during_index'
        # Re-queue (max 3 retries per scan_run)
        retries = self._scan_run_retries.get(filepath, 0)
        if retries < 3:
            self._scan_run_retries[filepath] = retries + 1
            self._re_queue.append(filepath)
        else:
            logger.warning("File changed during index 3× — giving up: %s", filepath)
except OSError:
    pass
```

**Divergences:**
- Per-scan-run retry counter (`self._scan_run_retries: dict[str, int]`) — reset at scan start.
- New status value `'changed_during_index'` joins the enum.

**Test:** `tests/test_changed_during_index.py` — mock `os.stat` to return different mtime_ns on second call; assert re-queue fires.

---

### Wave F — D-NEW-4: Supported-File Scope Row Policy

**File:** part of `shared/local_indexer_migrations.py::_migrate_1_to_2` (the DELETE statement) + modify `scan_all` to skip SQLite INSERT for non-supported extensions unless they ended with error status.

**Migration pruning excerpt** (already in §"D-NEW-1" above):
```python
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
```

**Mirror this on the live scan path** — `_SUPPORTED_EXTENSIONS` constant update + skip-if-not-error gate:
```python
# Phase 97 D-NEW-4
_SUPPORTED_EXTENSIONS = {".docx", ".pdf", ".txt", ".html", ".xlsx", ".csv"}
_ERROR_STATUSES_KEPT = {"oversized", "error", "changed_during_index", "zip_bomb_suspected"}

# Inside scan_all, before INSERT:
ext = os.path.splitext(filepath)[1].lower()
if ext not in _SUPPORTED_EXTENSIONS and status not in _ERROR_STATUSES_KEPT:
    continue  # do not write SQLite row
```

**Test:** `tests/test_local_indexer_migrations.py::test_prune_unsupported`.

---

### Wave F — D-NEW-5: chunk_locator Per Format

**File:** modify `shared/local_indexer.py::build_local_schema` + each extractor + `_index_one_file`.

**Primary analog (existing schema field add pattern):** `shared/local_indexer.py:204-219` (every extractor yields the relevant locator string as part of the tuple).

**Mirror this:**
1. Add to schema: `builder.add_text_field("chunk_locator", stored=True)` (default tokenizer is fine — stored for display, not searched).
2. Extractors already yield tuples; extend the yielded tuple to include `chunk_locator` for the new format extractors (HTML/XLSX/CSV — already shown in their excerpts above). For PDF/DOCX/TXT, compute the locator in `_index_one_file` as:
   - PDF: `f"p. {page_num}"`
   - DOCX: `f"¶ {start+1}-{start+_DOCX_CHUNK_PARAGRAPHS}"`
   - TXT: `""` (single chunk; locator empty or `"full"`).
3. `chunk_locator` is also stored in SQLite `local_pages` (added by D-NEW-1 migration).

**Divergences:** New schema field — schema version bumps; existing LOCAL Tantivy indices must be rebuilt (R-02 atomic rebuild handles this — first launch on Phase 97 triggers schema-mismatch → rebuild from cached_text or source).

**Test:** `tests/test_chunk_locator.py` — assert each format produces the documented string format.

---

### Wave F — D-NEW-6: Bilingual Privacy Disclosure

**File:** modify `docs/PRIVACY.md` (or equivalent) + `web/pages/help.py` + desktop Help dialog + About dialog strings.

**Primary analog (Phase 95 D-33 cleartext disclosure pattern):** existing strings in `web/pages/help.py` `_create_english_content` / `_create_hebrew_content` (Phase 95 D-33), and desktop About dialog.

**Mirror this — add bilingual section:**
- EN: "My Library indexes the text of your local documents and stores compressed cleartext copies in `<INDEX_DIR>/local_index.sqlite3` on this machine. Compression is via zstd, **not encryption**. The cache is never uploaded; it stays on your computer for fast search and recovery."
- HE: equivalent translation per Phase 95 D-33 language conventions.

**Divergences:** First explicit zstd compression disclosure. Mirrors Phase 95 D-33 patterns.

**Test:** `tests/test_privacy_disclosure_strings.py` — assert substrings present in both EN and HE Help+About sources.

---

### Wave F — D-NEW-7: Phase 95/96/97 Invariant Regression Tests

**File:** new `tests/test_phase_97_invariants.py`.

**Primary analog combinations:**
- AST cloud-write-gate scanner: `tests/test_no_raw_storage_access.py` (Phase 87 pattern).
- AST cascade scanner: `tests/test_local_filter_cascade.py:39-72` (`_iter_function_defs` + `_function_contains_call`).
- Engine-stub pattern: `tests/test_local_post_dedup_merge.py:21-47`.
- PGP filter cascade template: `tests/test_pgp_filter_cascade.py`.

**RESEARCH §"Example 4" verbatim template (97-RESEARCH.md:1024-1057):**
```python
import ast
import pathlib

def test_cloud_write_gates_at_top():
    """D-NEW-7 (a): three cloud-write gates remain at TOP of respective modules."""
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
                first_block = ast.unparse(ast.Module(body=node.body[:5], type_ignores=[]))
                assert 'is_local_sys_id' in first_block, (
                    f"{path.name}::{fn_name}: is_local_sys_id gate not in first 5 "
                    f"statements (Phase 95 D-30 / Phase 97 D-NEW-7 invariant)"
                )
                break
        else:
            raise AssertionError(f"{path.name}: no function named {fn_name}")
```

**Mirror this for all four invariants:**
1. **(a)** Cloud-write gates at TOP of 3 modules (above template).
2. **(b)** Web LIBRARY_CODES allowlist `[]` — AST scan over `web/pages/*.py`. Mirror `tests/test_web_library_options_no_local.py` (Phase 95).
3. **(c)** `is_local_sys_id()` recognizes 18-digit 97-prefixed sys_ids. Call into `shared.local_sys_id` directly: `assert is_local_sys_id("97" + "0" * 16) is True`.
4. **(d)** LOCAL RRF merge POST-`_deduplicate()`. Build engine stub (`_make_engine` from `tests/test_local_post_dedup_merge.py:21-47`), call the merge path, assert ordering. The existing `tests/test_local_post_dedup_merge.py` already pins this; re-assert it inside `test_phase_97_invariants.py` for fail-fast CI.

**Divergences:** Four sub-tests in one file (not four separate files) per VALIDATION.md naming convention.

---

### Wave F — D-NEW-8: mtime_ns

(Already covered under Wave B above — same file edit, no additional pattern.)

---

## Shared Patterns

### Pattern A — Phase 95/96 Invariants Carry Forward (DO NOT BREAK)

**Sources:**
- `tests/test_local_filter_cascade.py` — cascade joinpoint AST guard (extend, do not weaken).
- `tests/test_local_post_dedup_merge.py` — RRF POST-`_deduplicate` ordering pinned.
- `tests/test_web_library_options_no_local.py` — web `LIBRARY_CODES` allowlist `[]`.
- `tests/test_no_raw_storage_access.py` — Phase 87 multitenant allowlist `[]`.
- `shared/search_serializer.py:582-585`, `corrections_client.py:627-630`, `lists_sync.py:699-713,752-766` — three cloud-write gates at TOP-of-function.
- `tests/test_local_optout_persistence.py` + `tests/test_result_dialog_local_button_removed.py` — Phase 96 invariants.

**Apply to:** Every Phase 97 modification. Especially:
- Schema field add (`scan_run_id`, `chunk_locator`) MUST keep `tokenizer_name="raw"` for delete-by-term fields (Pitfall #7).
- `_commit_batch` extension MUST preserve two-phase commit ordering (Tantivy first, SQLite second). R-04 adds durability around the SQLite UPDATE but does NOT reorder.
- Migration 1→2 MUST be idempotent (Pitfall #4 — duplicate column name try/except).

### Pattern B — `is_local_sys_id` Branch Dispatch

**Source:** `shared/local_sys_id.py::is_local_sys_id` (Phase 95).

**Used at three documented dispatch sites:**
- `genizah_app.py:18475-18483` (browse_to_result LOCAL dispatch).
- `genizah_app.py:18570-18572` (`_open_local_browse` guard).
- `desktop/result_dialog.py:1995` (Phase 96 NEW-1 removal — but ResultDialog `load_local_page` keeps the dispatch).

**Apply to:** Any new Phase 97 dispatch (e.g., disk indicator routing, recovery banner display). Reuse the import pattern: `from shared.local_sys_id import is_local_sys_id as _is_local`.

### Pattern C — Session JSON Save/Restore Symmetry

**Source:** `genizah_app.py:23532-23613` (save) + `:23623-23800` (restore).

**Apply to:** R-01 recovery state if any user choice persists across runs. Every key added to `_save_session` must have a corresponding restore line with a sensible default for backward-compat.

### Pattern D — Mock Tantivy Doc

**Source:** `tests/test_local_post_dedup_merge.py` + `tests/test_local_filter_cascade.py` (`_Stub` pattern).

**Apply to:** All new unit tests that need to feed a "Tantivy doc" through engine code. Use `MagicMock` with `get_first.side_effect = lambda field: {...}.get(field, "")`.

### Pattern E — Two-Phase Commit + Phase 97 Durability Bracket

**Source:** `shared/local_indexer.py::_commit_batch` @ :1484-1506 + R-04 escalation.

**Apply to:** Any new operation that mutates both Tantivy and SQLite (e.g., `discard_run`, atomic rebuild). The order is: Tantivy first, then SQLite, with `synchronous=FULL` bracketing the SQLite UPDATE for durability.

### Pattern F — Windows-Aware Atomic Rename (2-step)

**Source:** RESEARCH §"Pattern 4" + Pitfall #2.

**Apply to:** R-02 atomic rebuild + any future file-replacement on Windows. Always two renames: `target → target.old-<ts>` then `temp → target`. Audit row in SQLite for cleanup-on-shutdown if a crash strikes between.

### Pattern G — Bilingual Help/About String Pair

**Source:** Phase 95 D-33 disclosure pattern; existing EN/HE pairs in `web/pages/help.py` `_create_english_content` / `_create_hebrew_content`.

**Apply to:** D-NEW-6 privacy disclosure. Every EN string gets a HE counterpart.

---

## No Analog Found

| File | Role | Reason |
|------|------|--------|
| `shared/cached_text.py` (zstd helpers, if extracted as separate module) | compression helper | First use of `zstandard` in repo. RESEARCH §"Example 3" is the verbatim template. |
| `tests/test_phase_aware_eta.py` | timing test | No existing EWMA helper. Synthesise. |
| `tests/test_disk_headroom.py` | math + mocks | First use of `shutil.disk_usage` in tests. Synthesise. |
| `tests/test_privacy_disclosure_strings.py` | string presence | No prior bilingual-string assertion test pattern. Synthesise via grep + EN/HE substring assertions. |
| 5 new fixtures in `tests/fixtures/local_indexer/` | static data | New fixture files (`hebrew_sample.html`, `hebrew_sample.xlsx`, `hebrew_sample.csv`, `zip_bomb_sample.xlsx`, `multi_sheet_large.xlsx`). Wave 0 creates them. |

---

## Plan-Time Issues from RESEARCH (Load-Bearing Anti-Patterns)

These are NOT new analogs but **divergences from CONTEXT** that the planner MUST encode:

1. **Issue #1 (RESEARCH:11):** tantivy-py 0.25.1 has NO `writer.get_memory_usage()`. **DROP the heap-sampling branch from C-02 entirely** — commit policy is bytes/count/time only. Apply at Wave B / C-02 plan.

2. **Issue #2 (RESEARCH:13):** `beautifulsoup4` is NOT installed; `lxml==6.0.2` is. **Substitute `lxml.html` for BeautifulSoup in F-01** — avoids new dep + new `collect_all` invocation. Apply at Wave C / F-01 plan.

3. **Issue #3 (RESEARCH:15):** Atomic temp-dir swap will fail with `os error 5` on Windows if `SearchEngine.local_searcher` is alive. **R-02 protocol MUST include explicit close + reload callbacks** (5-step / 7-step ordered protocol). Apply at Wave A / R-02 plan.

4. **Issue #4 (RESEARCH:17):** `scan_run_id` must be set ONLY on rows INSERTED or UPDATED to `status='pending'` within this scan_run — NOT on rows the scan skipped because they were already up-to-date. Otherwise Discard wrongly deletes them. **Lock the SQL rule explicitly in the U-02 plan.** Apply at Wave E / U-02 plan.

---

## Metadata

**Analog search scope:** `shared/local_indexer.py`, `shared/local_sys_id.py`, `desktop/my_library_tab.py`, `desktop/result_dialog.py`, `genizah_core.py`, `genizah_app.py`, `web/pages/help.py`, `lists_sync.py`, `corrections_client.py`, `shared/search_serializer.py`, all `tests/test_local_*.py`, all `tests/test_pgp_filter_cascade.py` + `tests/test_no_raw_storage_access.py`.
**Files scanned:** 14 source files + 35 test files + 4 docs cross-checks.
**Pattern extraction date:** 2026-05-25.
**Phase 97 codebase baseline:** v7.14.0 (`f115bd87`) + Phase 96 closeout (`cb9dc677`).
**Early-stopping:** 41 strong analogs identified across 41 surfaces; no additional search performed.

---

## PATTERN MAPPING COMPLETE
