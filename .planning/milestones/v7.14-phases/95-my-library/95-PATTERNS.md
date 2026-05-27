# Phase 95: My Library — Pattern Map

**Mapped:** 2026-05-21
**Files analyzed:** 17 new files + 9 modified files + 26 test files (52 total)
**Analogs found:** 50 / 52 (96% match coverage)

> Consumed by `gsd-planner`. Each row gives the planner the exact analog file + line range to copy from, an excerpt of the load-bearing pattern, "mirror this" guidance, and explicit divergences (where the new code must NOT follow the analog). The five Codex P0/P1 fixes (D-08 dedup ordering, D-13 parser generalization, D-21 two-phase commit, D-25 mutex, D-30 lists_sync gate placement) each get their own row with precise insertion line numbers.
>
> **Path drift caveat (RESEARCH.md Pitfall #4):** SPEC REQ-8 and CONTEXT.md say "MyLibraryTab as 6th tab". The actual codebase has 6 tabs today (`genizah_app.py:3086-3091`: Search, Composition Search, Browse by Shelfmark, Browse by Identification, Personal Lists, Community). MyLibraryTab will be the **7th** tab; planner should not pin a tab index.

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `shared/local_sys_id.py` | new helper module | pure-function (no I/O) | `shared/synthetic_sys_id.py` | **exact** (template ported, prefix swap) |
| `shared/local_indexer.py` | new service module | file-I/O + Tantivy/SQLite writer + QThread-aware | `seewald_addition/genizah_local_indexer.py` (port source); `genizah_core.py:5118-5189` (Tantivy schema); `gui_threads.py:33-48` (IndexerThread shape) | **role-match + port** |
| `desktop/my_library_tab.py` | new desktop widget | QWidget + QThread orchestration | `genizah_app.py:11621+` `create_lists_tab` (QWidget composition + QSplitter); `gui_threads.py:33-48` `IndexerThread` | **role-match** |
| `genizah_core.py` (modify) | extension to existing | request-response (search dispatch); config; parsers | self-analog (`LIBRARY_CODES @ :1723`, `Config @ :2007`, `parse_header_smart @ :3640`, schema @ :5124, dedup @ :7390/:7916, lab @ :742/:1292) | **exact (in-file extension)** |
| `genizah_app.py` (modify) | extension to existing | tab registration; result-table render | self-analog (`addTab @ :3079-3091`, `COL_SRC @ :5909/:16534`, visibility @ :16741) | **exact (in-file extension)** |
| `lists_sync.py` (modify) | extension to existing | cloud-write gate (P0 — Codex placement fix) | `corrections_client.py:619-623` (existing synthetic-sys_id gate shape) | **role-match (different surface, same gate)** |
| `corrections_client.py` (modify) | extension to existing | cloud-write gate | self-analog at `:619-623` (existing `is_synthetic_sys_id` gate) | **exact (extend existing OR-clause)** |
| `shared/search_serializer.py` (modify) | extension to existing | result-list filter (defense-in-depth) | self-analog (`_serialize_item @ :282`; existing `is_synthetic_sys_id` import @ :52) | **exact (in-file extension)** |
| `web/pages/search.py` (modify) | extension to existing | UI dropdown sanitization (D-30 web filter) | self (PGP filter pattern @ :1430-1480) | **partial** (D-46 web-consumer guard) |
| `web/pages/browse.py` (modify) | extension to existing | UI dropdown sanitization | `web/pages/search.py` PGP cascade (template) | **partial** |
| `web/pages/help.py` (modify) | extension to existing | doc/help static content | self-analog (existing `_create_english_content` / `_create_hebrew_content`) | **exact** |
| desktop Help dialog (modify) | extension to existing | doc/help static content | desktop `Help.html` resource (`Config.HELP_FILE @ :2015`) | **partial** |
| `requirements.txt` (modify) | config | dep pin | self (existing pins: `python-docx==1.2.0`, `tantivy==0.25.1`) | **exact** |
| `GenizahSearchPro.spec` (modify) | config (PyInstaller) | build-time bundling | self (existing `collect_all('tantivy')` call site) | **exact (extend existing pattern)** |
| `CompileScriptGenizah.iss` (verify) | config (Inno Setup) | binary collection | self (existing setup file) | **likely no-op** (collect_all in .spec handles bundles) |
| `tests/test_local_sys_id_namespace.py` | new test | unit | `tests/test_synthetic_sys_id.py` (verbatim template) | **exact** |
| `tests/test_local_sys_id_parser_compat.py` | new test | unit | `tests/test_synthetic_sys_id.py::TestNoIntCoercion` (AST walker pattern) | **role-match** |
| `tests/test_local_indexer.py` | new test | unit + fixtures | `tests/test_synthetic_sys_id.py` (pytest class layout); `seewald_addition/genizah_make_index.py` (helpers under test) | **role-match** |
| `tests/test_local_indexer_incremental.py` | new test | integration | seewald `processed_files` cache pattern | **role-match** |
| `tests/test_local_indexer_scale.py` | new test (slow) | scale (slow-marked) | same as `test_local_indexer.py` | **role-match** |
| `tests/test_local_indexer_mutex.py` | new test | unit | (no existing QMutex test; mirror PyQt6 QMutex docs) | **no analog** |
| `tests/test_side_index_merge.py` | new test | integration | (no exact; mirror search invocation in `tests/test_*search*.py`) | **partial** |
| `tests/test_local_post_dedup_merge.py` | new test | unit | (no analog; mirror `_deduplicate` direct unit test) | **no analog** |
| `tests/test_local_filter_cascade.py` | new test | static AST | `tests/test_pgp_filter_cascade.py` (**verbatim** template) | **exact** |
| `tests/test_local_filter_persistence.py` | new test | unit | (none — QSettings test; mirror PyQt6 docs) | **partial** |
| `tests/test_local_delete_by_uid.py` | new test | unit | (no existing tantivy-delete test; mirror tantivy-py docs) | **no analog** |
| `tests/test_local_two_phase_commit.py` | new test | unit + fault-injection | (no analog; mirror crash-recovery test pattern) | **no analog** |
| `tests/test_local_namespace_no_api_leak.py` | new test | unit | `tests/test_synthetic_sys_id.py::TestRealAlmaCollisionNegative` (assertion shape) | **role-match** |
| `tests/test_local_namespace_no_lists_leak.py` | new test | unit (mock-based) | (no exact; mirror Supabase-mock test pattern) | **partial** |
| `tests/test_local_namespace_no_corrections_leak.py` | new test | unit | `tests/test_synthetic_sys_id.py::TestRealAlmaCollisionNegative` (assertion shape) | **role-match** |
| `tests/test_local_index_open_fallback.py` | new test | unit | (none; mirror tantivy-open mock) | **no analog** |
| `tests/test_local_lab_invalidation.py` | new test | unit | (none; mirror hash-based invalidation in `genizah_core.py`) | **partial** |
| `tests/test_local_unavailable_folder.py` | new test | unit | (none; OS-walk + missing-folder mock) | **no analog** |
| `tests/test_canonical_filepath.py` | new test | unit | (none; Windows-path normalization) | **no analog** |
| `tests/test_folder_overlap_detection.py` | new test | unit | (none; `commonpath` overlap detection) | **no analog** |
| `tests/test_export_dossier_local_handling.py` | new test | unit | `tests/test_export_xlsx_cross_parity.py` (cross-app parity precedent) | **role-match** |
| `tests/test_web_library_options_no_local.py` | new test | static AST | `tests/test_pgp_filter_cascade.py` (**verbatim** template, AST scan over `web/pages/`) | **exact** |
| `tests/test_local_schema_evolution.py` | new test | unit | (none; SQLite schema introspection) | **no analog** |
| `tests/fixtures/local_indexer/*` (8 fixtures) | new fixtures | static data | (D-44 picks: small Word-authored Hebrew PDF) | **n/a (new data)** |

---

## Pattern Assignments

Each section below tells the planner: **read the analog, copy that shape, and apply these divergences for LOCAL.**

---

### `shared/local_sys_id.py` (new helper, pure-function)

**Analog:** `shared/synthetic_sys_id.py` (entire file, 140 lines).

**Imports pattern** (lines 1-37):
```python
# -*- coding: utf-8 -*-
"""Phase 85 synthetic sys_id helpers (SYNTH-01).
..."""
from __future__ import annotations
from typing import Optional

_SYNTHETIC_PREFIX = "99"
_SYNTHETIC_SUFFIX = "000000"
_INVENTORY_PAD = 10
_TOTAL_LENGTH = 2 + _INVENTORY_PAD + 6  # 18
```

**Core detector pattern** (lines 45-76):
```python
def is_synthetic_sys_id(s: object) -> bool:
    if not s:
        return False
    s = str(s)
    if not s.isdigit():
        return False
    if len(s) != _TOTAL_LENGTH:
        return False
    return s.startswith(_SYNTHETIC_PREFIX) and s.endswith(_SYNTHETIC_SUFFIX)
```

**Mirror this:**
- Module docstring with explicit "string in, string out" contract (D-01b discipline carries to D-19).
- Module-level `_LOCAL_PREFIX = "97"`, `_MACHINE_PAD = 8`, `_HASH_PAD = 8`, `_TOTAL_LENGTH = 18` constants.
- `is_local_sys_id(s: object) -> bool` with the EXACT same control-flow shape: empty-guard → `str(s)` → `isdigit()` → length-check → `startswith(prefix)`.
- Docstring examples (`>>> ...`) so doctests pass without extra harness.
- The "real Alma sys_ids must NOT classify" invariant is `is_synthetic_sys_id` style — but here it's "real Alma + synthetic-99 sys_ids must NOT classify as LOCAL". Two disjoint negatives.

**Divergences (CRITICAL):**
1. **NO SUFFIX.** Synthetic uses `endswith("000000")` as discriminator; LOCAL has no suffix — discriminator is only the `97` prefix + 18-digit length.
2. **Add new helpers per RESEARCH.md Code Example lines 692-725:** `_canonical_filepath(p)` (D-42), `_machine_id()` (D-19 — explicit `% 10**8` per Codex revision), `_content_hash(canonical_filepath, slot=0)` (D-19 — slot argument supports collision retry), `generate_local_sys_id(filepath, slot=0)`.
3. **NO `decode_inventory_id` analog.** LOCAL sys_id is not reversible to a filepath (the SHA256 is one-way); skip the decode helper.
4. **`_machine_id` uses `socket.gethostname()`** — new stdlib import vs synthetic's no-stdlib helper.
5. The `% 10**8` arithmetic is the load-bearing Codex P0 fix (D-19) — RESEARCH "DON'T trust ... without modulo" anti-pattern. Hard-code `_MACHINE_PAD = 8` AND verify `f"{... % 10**8:08d}"` produces exactly 8 chars.

---

### `shared/local_indexer.py` (new service module, file-I/O + Tantivy + SQLite + Qt-free)

**Primary analog:** `seewald_addition/genizah_local_indexer.py` (port source, 1424 lines).

**Secondary analog:** `genizah_core.py:5118-5189` (Tantivy schema setup).

**Tertiary analog:** `gui_threads.py:33-48` `IndexerThread` (worker-shape template for the Qt-side wrapper that lives in `desktop/my_library_tab.py`).

**Extraction-helpers excerpt — port verbatim per D-02** (from `seewald_addition/genizah_make_index.py:60-105`):
```python
def _rtl_ratio(text):
    alpha = [c for c in text if c.isalpha()]
    if not alpha:
        return 0.0
    rtl = sum(1 for c in alpha if unicodedata.bidirectional(c) in ("R", "AL", "AN"))
    return rtl / len(alpha)

def _fix_rtl_line(line):
    """Reverse a pdfplumber mirror-reversed RTL line."""
    s = line.strip()
    if not s or _rtl_ratio(s) <= 0.4:
        return line
    lead = len(line) - len(line.lstrip())
    tail = len(line) - len(line.rstrip())
    core = s[::-1]
    return line[:lead] + core + (line[len(line) - tail:] if tail else "")

def _fix_rtl_page(text):
    if not text:
        return text
    lines = [_fix_rtl_line(ln) for ln in text.splitlines()]
    result = "\n".join(lines)
    result = re.sub(r"(\w)\s+([,.])", r"\1\2", result)
    result = re.sub(r"([,.])\s+(\w)", r"\1 \2", result)
    return result

def _join_fragmented_lines(text):
    """Join pages where each word is on its own line (common in Hebrew PDFs)."""
    lines = text.splitlines()
    non_empty = [l for l in lines if l.strip()]
    if len(non_empty) < 4:
        return text
    single = sum(1 for l in non_empty if len(l.split()) <= 1)
    if single / len(non_empty) < 0.60:
        return text
    paragraphs, current = [], []
    for line in lines:
        s = line.strip()
        if s:
            current.append(s)
        elif current:
            paragraphs.append(" ".join(current))
            current = []
    if current:
        paragraphs.append(" ".join(current))
    return "\n\n".join(paragraphs)
```

**Tantivy schema excerpt — main-index template** (`genizah_core.py:5124-5136`):
```python
builder = tantivy.SchemaBuilder()
builder.add_text_field("unique_id", stored=True)                          # ← LOCAL DIVERGES HERE
builder.add_text_field("content", stored=True, tokenizer_name="whitespace")
builder.add_text_field("content_head", stored=False, tokenizer_name="whitespace")
builder.add_text_field("content_tail", stored=False, tokenizer_name="whitespace")
builder.add_text_field("line_starts", stored=False, tokenizer_name="whitespace")
builder.add_text_field("line_ends", stored=False, tokenizer_name="whitespace")
builder.add_text_field("source", stored=True)
builder.add_text_field("full_header", stored=True)
builder.add_text_field("shelfmark", stored=True)
builder.add_text_field("scope", stored=True)
builder.add_text_field("boundaries", stored=True)
schema = builder.build()
```

**PyMuPDF per-page extraction — research-locked excerpt** (RESEARCH.md `extract_pdf_pages` lines 789-809):
```python
import fitz

def extract_pdf_pages(filepath):
    """Yield (page_num, text, title) — D-03 one-doc-per-page model."""
    doc = fitz.open(filepath)
    try:
        title = (doc.metadata or {}).get('title') or os.path.basename(filepath)
        for page_num, page in enumerate(doc, start=1):
            blocks = page.get_text("blocks")
            text_parts = [b[4].strip() for b in blocks if b[6] == 0 and b[4].strip()]
            text = "\n\n".join(text_parts)
            # D-02: RTL helpers are NOT invoked in v1 (dead code).
            yield page_num, text, title
    finally:
        doc.close()
```

**SQLite mtime-cache pattern — Seewald port** (`seewald_addition/genizah_local_indexer.py`):
```python
INDEX_DB_NAME = "local_index.sqlite3"
# CREATE TABLE processed_files(filepath PK, mtime, size, sys_id) — per SPEC REQ-5
```

**Mirror this:**
1. **Module structure:** dependency checks (`_check_dep` — Seewald lines 61-84), constants, text-extraction helpers (RTL block as DEAD CODE per D-02), per-format extractors (`extract_docx_pages`, `extract_pdf_pages`, `extract_txt`), Tantivy schema builders (`build_local_schema()`, `build_local_lab_schema()`), SQLite schema initializer, the two-phase commit body, file enumeration / pre-scan.
2. **Module is Qt-free** — only `desktop/my_library_tab.py` imports `PyQt6`.
3. **Use `genizah_core.py:5124-5136` schema as the field shape**, but apply the divergences below.
4. **DOCX chunking is NEW** — RESEARCH.md `extract_docx_pages` (lines 821-836): fixed 20-paragraph windows per D-04 (NOT Seewald's `contains_page_break` heuristic).
5. **Two-phase commit body** — copy verbatim from RESEARCH.md `commit_batch` + `startup_recovery` lines 486-525.
6. **`_canonical_filepath` and sys_id generation** are imported from `shared/local_sys_id.py`; don't duplicate.

**Divergences (CRITICAL — RESEARCH.md findings):**
1. **LOCAL schema `unique_id` uses `tokenizer_name="raw"`** — main index OMITS this kwarg (defaulting to the tokenized "default" tokenizer). This is the LOAD-BEARING DIVERGENCE per RESEARCH.md Pitfall #2 / tantivy-py issue #297. Without it, `writer.delete_documents("unique_id", uid)` SILENTLY does nothing on rescans and doubles the page-row count on every modify. Schema-divergence comment is REQUIRED inline:
   ```python
   # CRITICAL: tokenizer_name="raw" — main index at genizah_core.py:5125 omits this
   # and is rebuilt from scratch so the bug stays latent. For LOCAL where incremental
   # delete IS the central operation, raw is mandatory. tantivy-py issue #297.
   builder.add_text_field("unique_id", stored=True, tokenizer_name="raw")
   ```
2. **Per-page model (D-03), NOT per-system.** Seewald port has a "system" scope doc (line 528-536) — DROP it for LOCAL. One Tantivy doc per page.
3. **DOCX 20-paragraph chunking (D-04) replaces Seewald's `contains_page_break` heuristic** (Seewald lines 124-152).
4. **PyMuPDF is the ONLY PDF extractor in v1** — drop the `pdfplumber` / `pypdf` branches (Seewald lines 258-281). Helpers stay as DEAD CODE per D-02.
5. **Do NOT write `Transcriptions.txt`, `libraries.csv`, `browse_map.pkl`, or `metadata_cache.pkl`** — Seewald port writes these (lines 541-559) because his prototype patches the shared corpus. LOCAL writes ONLY the Tantivy side-index + SQLite. Shared corpus is READ-ONLY per SPEC Constraint #6.
6. **TXT extraction is NEW** (Seewald didn't ship TXT). Per D-07 / RESEARCH.md Code Examples: start with `utf-8-sig` only; planner records final policy in `95-NN-PLAN.md` after smoke test.
7. **D-34 `unique_id` / `full_header` format:** `unique_id = f"LOCAL_{sys_id}_P{page_num}"`; `full_header = f"{sys_id}_LOCAL_P{page_num}_F{file_id:04d}"`. NOT Seewald's `IE{n}_P{m}_FL{k}`. Pin via `tests/test_local_sys_id_parser_compat.py`.
8. **No CLI** — Seewald has a 280-line `main()` (lines 1265-1424). DROP all of it; `shared/local_indexer.py` is library-only.
9. **`browse_map[sys_id]` entry shape per D-34:** `{'p_num': page_num, 'uid': unique_id, 'full_header': full_header, 'ie_id': f"F{file_id:04d}", 'seq_index': page_num}`. The synthetic `ie_id` satisfies `get_volume_pages()` filter (Seewald's prototype trick).

---

### `shared/local_indexer.py` — LAB side-index builder (D-09)

**Analog:** `genizah_core.py:742-790` (`rebuild_lab_index` LAB schema).

**Excerpt** (lines 763-777):
```python
builder = tantivy.SchemaBuilder()
builder.add_text_field("unique_id", stored=True)                              # ← LOCAL LAB DIVERGES
builder.add_text_field("text_normalized", stored=True, tokenizer_name="simple")
builder.add_text_field("text_ngram", stored=False, tokenizer_name="whitespace")
builder.add_text_field(self.LAB_FINGERPRINT_FIELD, stored=False, tokenizer_name="simple")
builder.add_text_field("fingerprint_dyn", stored=False, tokenizer_name="simple")
builder.add_text_field("full_header", stored=True)
builder.add_text_field("shelfmark", stored=True)
builder.add_text_field("source", stored=True)
builder.add_text_field("content", stored=True, tokenizer_name="simple")
schema = builder.build()
index = tantivy.Index(schema, path=Config.LAB_INDEX_DIR)
self._ensure_lab_tokenizers(index)
writer = index.writer(heap_size=50_000_000)
```

**Mirror this:**
- Field set identical to main LAB.
- Call `self._ensure_lab_tokenizers(index)` (LAB-specific custom tokenizer setup) — needs `SearchEngine` instance access or factored helper. Planner picks: either inject `lab_engine` into `local_indexer.build_lab_side_index(...)` or factor `_ensure_lab_tokenizers` to a free function.
- Compute `fingerprint_dyn` using the current `dynamic_rank_map` for each LOCAL page.

**Divergences:**
1. **`unique_id` uses `tokenizer_name="raw"`** — same LOCAL Tantivy divergence as the main side-index (RESEARCH.md Pitfall #2).
2. **`Config.LOCAL_LAB_INDEX_DIR` target** (NOT `Config.LAB_INDEX_DIR`).
3. **Write `.meta.json` with `weights_hash` + `lab_schema_version` + `last_built_at`** per D-09 / D-38 Codex revision (RESEARCH.md Pitfall #7). Pin via `tests/test_local_lab_invalidation.py`.
4. **No `count_documents()` pre-pass** — for LOCAL the file count is already known from the indexer's pre-scan.

---

### `desktop/my_library_tab.py` (new QWidget)

**Primary analog:** `genizah_app.py:11621+` (`create_lists_tab` — QWidget composition with QSplitter / QVBoxLayout / QLabel / QPushButton).

**Secondary analog:** `gui_threads.py:33-48` (`IndexerThread` for the worker-thread shape).

**Tertiary analog:** `web/pages/search.py:1432-1480` (three-state filter button cycle, for the LOCAL filter on result toolbars — applied by `genizah_app.py` not this widget; cross-reference only).

**Tab construction excerpt** (`genizah_app.py:11621-11625`):
```python
def create_lists_tab(self):
    """Create the Personal Lists tab for managing starred manuscripts."""
    panel = QWidget()
    layout = QHBoxLayout(panel)
    layout.setContentsMargins(5, 5, 5, 5)
    main_splitter = QSplitter(Qt.Orientation.Horizontal)
    ...
```

**Tab registration excerpt** (`genizah_app.py:3079-3091`):
```python
self.tabs = QTabWidget()
self.search_tab = self.create_search_tab()
self.composition_tab = self.create_composition_tab()
self.browse_tab = self.create_browse_tab()
self.catalog_browse_tab = self.create_catalog_browse_tab()
self.lists_tab = self.create_lists_tab()
self.community_tab = self.create_community_tab()
self.tabs.addTab(self.search_tab, tr("Search"))
self.tabs.addTab(self.composition_tab, tr("Composition Search"))
self.tabs.addTab(self.browse_tab, tr("Browse by Shelfmark"))
self.tabs.addTab(self.catalog_browse_tab, tr("Browse by Identification"))
self.tabs.addTab(self.lists_tab, tr("Personal Lists"))
self.tabs.addTab(self.community_tab, tr("Community"))
# ADD HERE (7th tab — Pitfall #4):
# self.my_library_tab = MyLibraryTab(self)
# self.tabs.addTab(self.my_library_tab, tr("My Library"))
```

**Worker thread shape** (`gui_threads.py:33-48`):
```python
class IndexerThread(QThread):
    progress_signal = pyqtSignal(int, int)
    finished_signal = pyqtSignal(int)
    error_signal = pyqtSignal(str)

    def __init__(self, meta_mgr):
        super().__init__()
        self.indexer = Indexer(meta_mgr)

    def run(self):
        try:
            def callback(curr, total): self.progress_signal.emit(curr, total)
            total_docs = self.indexer.create_index(progress_callback=callback)
            self.finished_signal.emit(total_docs)
        except Exception as e: self.error_signal.emit(str(e))
```

**Mirror this:**
1. **Tab is a class (not a `create_my_library_tab` method)** so the file lives in `desktop/my_library_tab.py` per RESEARCH.md "Recommended Project Structure". Class shape: `class MyLibraryTab(QWidget)` with `__init__(self, parent)` taking the main window reference for cross-tab signaling.
2. **Layout** uses `QVBoxLayout` with:
   - Top: `QListWidget` of registered folders + `Add Folder…` / `Remove` buttons (D-16 multi-folder).
   - Middle: `Refresh` / `Cancel` toolbar + `QProgressBar`.
   - Bottom: per-file status `QTableWidget` (cols Filename | Pages | Status per REQ-8 acceptance).
3. **`QFileDialog.getExistingDirectory(self, tr("Select folder"))`** for the Add Folder action (search `_tmp/`, `genizah_app.py` for an existing usage if needed; otherwise standard PyQt6 API).
4. **Worker thread** (`LocalIndexerWorker(QThread)`) mirrors `IndexerThread`:
   - `progress_updated = pyqtSignal(int, int, str)` (D-23: current_index, total, current_filename).
   - `file_finished = pyqtSignal(str, str, int, str)` (D-23: filename, status, pages, error_msg).
   - `finished_signal = pyqtSignal(dict)` for the IndexResult summary.
   - `error_signal = pyqtSignal(str)`.
   - `self._cancel_requested = False` instance flag (D-24).
5. **Cancellation** mirrors `SearchThread.cancel_flag` pattern (`gui_threads.py:63, 69-70`): `cooperative flag, check between files AND between PDF pages / DOCX chunks` per D-24 Codex revision.
6. **`os.startfile(filepath)`** for the D-28 "Open file" button — Windows-native; no fallback needed for v1 (RESEARCH.md Environment Availability A8).

**Divergences:**
1. **NEW QMutex** per D-25 Codex revision: `self._indexer_mutex = QMutex()` (or `threading.Lock()`) gates all Refresh / Add Folder / Remove side-effects. Concurrent operations are FIFO-queued with max depth 1 (additional requests collapse). Pin via `tests/test_local_indexer_mutex.py`.
2. **Auto-rescan-at-startup** (D-25) runs in worker QThread; status bar shows non-modal toast. Use `QSystemTrayIcon` / `QStatusBar.showMessage` (planner picks — consistent with existing desktop styling).
3. **Pre-scan dialog** (D-26 / D-41) — modal `QMessageBox.question` BEFORE the worker starts. Display BOTH file_count + total_bytes formatted with `humanize` or manual `(N / 1024**3):.1f GB`. Trigger on `count > 5000 OR bytes > 2 * 1024**3`.
4. **Three-state LOCAL filter button on THIS tab is NOT applicable** — filter lives on the search / Composition / Parallels tabs, NOT in MyLibraryTab. Filter UI is added to existing tabs in `genizah_app.py`.

---

### `genizah_core.py` modifications (in-place extensions)

#### Modification 1: `LIBRARY_CODES` extension (D-13)

**Analog:** self at `:1723-1810` (entire `LIBRARY_CODES` dict).

**Excerpt** (lines 1723-1731):
```python
LIBRARY_CODES = {
    'CUL': 'Cambridge University Library',
    'JTS': 'The Jewish Theological Seminary of America',
    'RNL': 'The National Library of Russia',
    ...
}
```

**Mirror this:** add `'LOCAL': 'My Library'` to the dict (D-13). Hebrew display `'הספרייה שלי'` lives in a Hebrew translation dict (planner identifies the existing he-translations table).

**Divergences:** none (additive only).

#### Modification 2: `Config.LOCAL_INDEX_DIR` + `Config.LOCAL_LAB_INDEX_DIR` extension (D-14)

**Analog:** self at `:2000-2010`.

**Excerpt** (lines 2005-2010):
```python
# Lab Mode Paths
LAB_DIR = os.path.join(INDEX_DIR, "lab")
LAB_INDEX_DIR = os.path.join(INDEX_DIR, "lab_index")
LAB_CONFIG_FILE = os.path.join(LAB_DIR, "lab_config.json")
LAB_WEIGHTS_FILE = os.path.join(LAB_DIR, "lab_weights.json")
LAB_LOG_FILE = os.path.join(LAB_DIR, "lab_genizah.log")
```

**Mirror this:** add two new `os.path.join(INDEX_DIR, ...)` entries inside `class Config`:
```python
# Phase 95 — My Library side-indexes (D-14)
LOCAL_INDEX_DIR = os.path.join(INDEX_DIR, "LocalIndex")
LOCAL_LAB_INDEX_DIR = os.path.join(INDEX_DIR, "LocalLabIndex")
```

**Divergences:** none — inherits portable-mode `INDEX_DIR` resolution automatically.

#### Modification 3: `parse_header_smart` + `parse_full_id_components` generalization (D-13 Codex P0)

**Analog:** self at `:3640-3681`.

**Excerpt** (lines 3640-3681):
```python
def parse_header_smart(self, full_header):
    sys_match = re.search(r'(99\d{8,})', full_header)
    sys_id = sys_match.group(1) if sys_match else None
    p_num = "Unknown"
    p_match = re.search(r'_P(\d+)_', full_header)
    if p_match:
        p_num = str(int(p_match.group(1)))
    else:
        tif_match = re.search(r'[ -_](\d{3,4})\.tif', full_header, re.IGNORECASE)
        if tif_match: p_num = str(int(tif_match.group(1)))
    return sys_id, p_num

def parse_full_id_components(self, full_header):
    result = {'sys_id': None, 'ie_id': None, 'p_num': None, 'fl_id': None}
    sys_match = re.search(r'(99\d{8,})', full_header)
    if sys_match:
        result['sys_id'] = sys_match.group(1)
    ie_match = re.search(r'(IE\d+)', full_header)
    if ie_match:
        result['ie_id'] = ie_match.group(1)
    p_match = re.search(r'_?(P\d+)', full_header)
    if p_match:
        raw_p = p_match.group(1)
        result['p_num'] = str(int(raw_p[1:]))
    fl_match = re.search(r'(FL\d+)', full_header)
    if fl_match:
        result['fl_id'] = fl_match.group(1).replace("FL", "")
    return result
```

**Mirror this:** preserve function signatures.

**Divergences (Codex P0 fix per D-13):**
1. **`re.search(r'(99\d{8,})', ...)` → `re.search(r'((?:99|97)\d{8,})', ...)`** — broaden the prefix alternation. Apply to BOTH functions on line 3641 AND line 3660.
2. **Add `ie_id` LOCAL fallback** in `parse_full_id_components`: when D-34 `full_header` shape contains `_F\d{4}` (synthetic file_id), set `ie_id = f"F{file_id:04d}"` matching D-34's browse_map shape.
3. **Pin generalization via `tests/test_local_sys_id_parser_compat.py`** — asserts `parse_header_smart("970012345601234567_LOCAL_P3_F0042")` returns `("970012345601234567", "3")` and `parse_full_id_components(...)` returns the full dict with `ie_id="F0042"`.

> **Alternative considered + REJECTED:** Codex revision proposed a centralized `extract_sys_id(header)` helper routing to both `is_synthetic_sys_id` + `is_local_sys_id`. Per CONTEXT D-13 "broaden to `(99|97)\d{16}` OR (preferred) route through a centralized helper" — both options valid; planner picks the regex-broadening path because it's a smaller blast radius. The centralized helper can be added in a follow-up if a third prefix appears.

#### Modification 4: Tantivy main schema — LEAVE UNCHANGED

**Analog:** self at `:5124-5136`.

**Mirror this:** **do NOT change the main index schema.** LOCAL side-index uses its own schema with `tokenizer_name="raw"` on `unique_id`. Main index stays as-is because it's rebuilt from scratch (RESEARCH.md Pitfall #2 dormant case).

#### Modification 5: Main search merger — RRF k=60 POST-`_deduplicate` (D-08 Codex P0)

**Analog:** self at `:7390-7401` (post-dedup result-list assembly).

**Excerpt** (lines 7389-7401):
```python
LOGGER.debug(f"Line-break search: ...")
deduped = self._deduplicate(results)

if exclude_words and deduped:
    filtered = []
    for r in deduped:
        text_content = (r.get('snippet', '') + ' ' + r.get('full_text', '')).lower()
        should_exclude = any(w.lower() in text_content for w in exclude_words)
        if not should_exclude:
            filtered.append(r)
    deduped = filtered

return deduped
```

**Dedup body** (line 7916-7921 — DO NOT MODIFY):
```python
def _deduplicate(self, results):
    v8 = {r['uid']: r for r in results if r['display']['source'] == "V0.8"}
    final = list(v8.values())
    for r in results:
        if r['display']['source'] == "V0.7" and r['uid'] not in v8: final.append(r)
    return final
```

**Mirror this:** add LOCAL query + RRF merge AFTER `self._deduplicate(results)` at `:7390`. Critical insertion point per D-08 Codex P0:
```python
deduped = self._deduplicate(results)    # ← line 7390 (existing)

# Phase 95 D-08: LOCAL hits merge AFTER _deduplicate (LOCAL would otherwise
# be dropped — the dedup body whitelists V0.8/V0.7 only).
if hasattr(self, 'local_searcher') and self.local_searcher is not None:
    try:
        local_hits = self._query_local_index(query, mode, gap, limit=...)
    except Exception as e:
        LOGGER.warning("LOCAL side-index query failed; main results unaffected: %s", e)
        local_hits = []
    deduped = self._rrf_merge(deduped, local_hits, k=60)
```

**RRF body** (port verbatim from RESEARCH.md `_rrf_merge`, lines 381-416):
```python
def _rrf_merge(self, genizah_hits, local_hits, k=60, limit=None):
    """Reciprocal Rank Fusion (D-08 P0)."""
    rrf = {}
    for rank, hit in enumerate(genizah_hits, start=1):
        uid = hit['uid']
        rrf.setdefault(uid, {'hit': hit, 'score': 0.0, 'sources': set()})
        rrf[uid]['score'] += 1.0 / (k + rank)
        rrf[uid]['sources'].add('genizah')
    for rank, hit in enumerate(local_hits, start=1):
        uid = hit['uid']
        rrf.setdefault(uid, {'hit': hit, 'score': 0.0, 'sources': set()})
        rrf[uid]['score'] += 1.0 / (k + rank)
        rrf[uid]['sources'].add('local')
    fused = sorted(
        rrf.values(),
        key=lambda r: (r['score'], 'genizah' in r['sources']),
        reverse=True,
    )
    out = [r['hit'] for r in fused]
    return out[:limit] if limit else out
```

**Divergences:**
1. **DO NOT generalize `_deduplicate`** to whitelist LOCAL — Codex P0 explicitly says "merge after `_deduplicate()`. Smaller blast radius, leaves Genizah dedup behavior untouched." (CONTEXT D-08).
2. **Tie-break: Genizah first** — encoded in the sort key `(r['score'], 'genizah' in r['sources'])` (True > False).
3. **Pin via `tests/test_local_post_dedup_merge.py`**: insert a LOCAL hit BEFORE `_deduplicate()` → asserts it's dropped; insert AFTER → asserts it survives. AND `tests/test_side_index_merge.py` for the end-to-end RRF order.
4. **D-37 fallback** — wrap `self._query_local_index(...)` in try/except so main search returns Genizah-only results when LOCAL is unavailable (corrupt index, missing files, etc.).

#### Modification 6: `lab_composition_search` — query both LAB indexes (D-09)

**Analog:** self at `:1292-1349` (entry point) + the existing scoring loop further down.

**Excerpt** (lines 1292-1325):
```python
def lab_composition_search(self, full_text, mode='variants', progress_callback=None, chunk_size=None,
                            excluded_ids=None, filter_text=None, deep_scan=False, scan_limit=50000,
                            boundary_mode='full', boundary_delimiter='\n', boundary_boost=1.5,
                            min_boundary_matches=0, min_delimiter_distance=3):
    """
    Scans a composition using Lab Mode.
    UPGRADES:
    1. Filters common phrases.
    2. Boosts V0.8.
    ...
    """
    ...
    use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None
    target_field = "fingerprint_dyn" if use_dyn else self.LAB_FINGERPRINT_FIELD
    target_map = self.dynamic_rank_map if use_dyn else HEBREW_FREQ
```

**Mirror this:**
1. **At entry:** check `local_lab_index` availability + `weights_hash` per D-38; if stale, surface banner via signal (`self.local_lab_stale_signal.emit()` or equivalent — planner picks).
2. **Run scoring loop ALSO against `local_lab_index`** with same `target_field` (`fingerprint_dyn` or static) and same `target_map`.
3. **Merger:** concat scored lists from BOTH lab indexes, sort by EXISTING custom score (NOT BM25 / NOT RRF — D-09 explicit), Genizah first on tie.

**Divergences (per D-09 Codex revision):**
1. **NOT RRF, NOT raw BM25.** Use the EXISTING custom fingerprint scoring path (which lab_composition_search already runs). LOCAL lab hits flow through the same scoring with the same target_field and target_map. Simple concat + sort by `sort_score` desc + Genizah-first tie-break.
2. **`weights_hash` check** happens BEFORE the query — if stale, the local_lab_index query is skipped (returns []), banner surfaced, and main lab search proceeds normally. Pin via `tests/test_local_lab_invalidation.py`.
3. **Apply the same pattern in `search_composition_logic`** (the non-LAB Composition Search at `:7923+`) — REQ-6 covers BOTH Composition Search variants.

---

### `genizah_app.py` modifications

#### Modification 1: Register MyLibraryTab as 7th tab

**Analog:** self at `:3079-3091` (the 6-tab block).

**Excerpt** (lines 3086-3091):
```python
self.tabs.addTab(self.search_tab, tr("Search"))
self.tabs.addTab(self.composition_tab, tr("Composition Search"))
self.tabs.addTab(self.browse_tab, tr("Browse by Shelfmark"))
self.tabs.addTab(self.catalog_browse_tab, tr("Browse by Identification"))
self.tabs.addTab(self.lists_tab, tr("Personal Lists"))
self.tabs.addTab(self.community_tab, tr("Community"))
```

**Mirror this:** Insert AFTER line 3091 (or AFTER line 3085 for the construction):
```python
from desktop.my_library_tab import MyLibraryTab   # at module top
self.my_library_tab = MyLibraryTab(self)           # after line 3085
...
self.tabs.addTab(self.my_library_tab, tr("My Library"))   # after line 3091
```

**Divergences:** **The tab is the 7th, not the 6th** (RESEARCH.md Pitfall #4 / SPEC drift). Acceptance test MUST NOT pin `self.tabs.count() == 7` as a hard equality; use `findChild` / search by text instead.

#### Modification 2: COL_SRC LOCAL badge + visibility extension (D-11)

**Analog (write site):** self at `:16534-16545`.

**Excerpt** (lines 16533-16545):
```python
self.results_table.setItem(row_idx, self.COL_IMG, QTableWidgetItem(str(meta.get('img', ''))))
# Src
self.results_table.setItem(row_idx, self.COL_SRC, QTableWidgetItem(str(meta.get('source', ''))))
# PGP badge
if sid and sid in self._pgp_transcription_sys_ids:
    pgp_item = QTableWidgetItem("PGP")
    pgp_item.setForeground(QColor("#27ae60"))
    self.results_table.setItem(row_idx, self.COL_PGP, pgp_item)
else:
    self.results_table.setItem(row_idx, self.COL_PGP, QTableWidgetItem(""))
```

**Visibility rule analog:** self at `:16738-16741`:
```python
has_multiple_sources = os.path.exists(Config.FILE_V7) and os.path.getsize(Config.FILE_V7) > 0
self.results_table.setColumnHidden(self.COL_SRC, not has_multiple_sources)
```

**Mirror this (D-11):**
1. At `:16534`, **change** from the plain `meta.get('source', '')` write to a LOCAL-aware write:
```python
source_val = str(meta.get('source', ''))
if source_val == 'LOCAL':
    src_item = QTableWidgetItem('LOCAL')
    src_item.setForeground(QColor("#3498db"))   # blue — symmetric with PGP green
    self.results_table.setItem(row_idx, self.COL_SRC, src_item)
else:
    self.results_table.setItem(row_idx, self.COL_SRC, QTableWidgetItem(source_val))
```
2. At `:16741`, **broaden** the visibility rule to OR-in LOCAL presence:
```python
has_multiple_sources = os.path.exists(Config.FILE_V7) and os.path.getsize(Config.FILE_V7) > 0
has_local = any(r.get('display', {}).get('source') == 'LOCAL' for r in self.last_results)
self.results_table.setColumnHidden(self.COL_SRC, not (has_multiple_sources or has_local))
```

**Divergences:**
1. **`#3498db` blue** matches the PGP-green pattern symmetry. Planner picks an alternative shade if it clashes with refinement-chain highlighting.
2. **Audit downstream consumers of `display.source`** per D-45: `shared/export_dossier.py` row builders + `genizah_app.py:export_results('xlsx')` must understand `'LOCAL'` (not just `'V0.8'` / `'V0.7'`). Gated via new `skip_local: bool = False` kwarg on the row-builders.

#### Modification 3: Composition Search + Parallels result-table Source column (D-12)

**Analog:** unknown — D-12 is an **audit-during-plan** decision (CONTEXT D-12 follow-up + RESEARCH.md Open Question 2). Planner runs:
```
Grep("COL_", path="genizah_app.py") | head -50
```
…to find the Composition Search and Parallels result-table column definitions, then either:
- **If a Src-equivalent column exists:** mirror D-11 there (write `source='LOCAL'`, color blue, extend visibility).
- **If not:** add a new compact `COL_SRC` column to both tables with same shape as the search result table.

**Mirror this:** the COL_SRC pattern at `:5909-5945`.

**Divergences:** until the audit happens, no excerpt available.

---

### `lists_sync.py` modifications (D-30 Codex P0)

**Analog:** `corrections_client.py:619-623` (existing synthetic-sys_id gate shape).

**Excerpt** (corrections_client `:615-623`):
```python
# Phase 85 SYNTH-06 / D-10 — REVIEWS-MODE iteration 1 B1 gate.
# Reject synthetic sys_ids at the WRITE entry point.
if is_synthetic_sys_id(document_id):
    return (
        None,
        "synthetic_corrections_disabled: corrections cannot be added to synthetic sys_ids",
    )
```

**Target site:** `lists_sync.py:736-770` (`sync_item_to_cloud` body).

**Existing body excerpt** (`:736-762`):
```python
def sync_item_to_cloud(self, item_id: str, list_id: str) -> bool:
    """Push a specific item to cloud."""
    if not self.is_sync_available():                        # ← line 738 (existing)
        return False
    try:
        client = self._get_client()                          # ← line 742 — Codex P0: gate MUST run BEFORE this
        if not client:
            return False
        list_data = self.lists_manager.data.get('lists', {}).get(list_id)
        if not list_data:
            return False
        cloud_list_id = list_data.get('cloud_id')
        if not cloud_list_id:
            self.sync_list_to_cloud(list_id)                 # ← line 753 — Codex P0: AND BEFORE this
            cloud_list_id = list_data.get('cloud_id')
            if not cloud_list_id:
                return False
        item_data = self.lists_manager.data.get('items', {}).get(item_id)
        if not item_data:
            return False
        sys_id = item_data.get('sys_id', item_id)            # ← line 762 — "natural" lookup site (the WRONG gate placement)
        ...
```

**Mirror this (RESEARCH.md Pitfall #3 — Codex P0):**
**Insert the LOCAL gate at line 737 (FIRST statement of the function body, BEFORE `self.is_sync_available()` at line 738).** Lookup `item_data` from local `self.lists_manager.data` (purely in-memory, no network), extract sys_id, check `is_local_sys_id`, return False if positive — ALL before touching `self._get_client()` (line 742) or `self.sync_list_to_cloud(list_id)` (line 753).

**Concrete patch shape** (RESEARCH.md Code Examples lines 760-781):
```python
from shared.local_sys_id import is_local_sys_id   # at module top

def sync_item_to_cloud(self, item_id: str, list_id: str) -> bool:
    """Push a specific item to cloud."""
    # ===== Phase 95 LOCAL gate (D-30, REQ-9) =====
    # MUST run before _get_client() / sync_list_to_cloud() — those leak
    # cloud activity even before sys_id is normally read at line 762.
    item_data = self.lists_manager.data.get('items', {}).get(item_id)
    if item_data:
        sys_id = item_data.get('sys_id', item_id)
        if is_local_sys_id(sys_id):
            logger.info("[local-only item, not synced] %s", sys_id)
            return False
    # ============================================
    if not self.is_sync_available():                # existing line 738
        return False
    try:
        client = self._get_client()                  # existing line 742
        ...
```

**Apply same gate at TOP of `sync_list_to_cloud()`** (also in `lists_sync.py` — planner identifies exact line):
```python
def sync_list_to_cloud(self, list_id: str) -> bool:
    # ===== Phase 95 LOCAL gate (D-30, REQ-9) =====
    items = [self.lists_manager.data.get('items', {}).get(iid) for iid in ...]
    for item_data in items:
        if item_data and is_local_sys_id(item_data.get('sys_id', '')):
            logger.info("[list contains LOCAL items, not synced] %s", list_id)
            return False
    # ============================================
    ...
```

**Divergences from corrections_client:**
1. **Return shape is `bool`, NOT `(None, message)` tuple** — match `sync_item_to_cloud`'s existing `return False` convention.
2. **No payload-shape concern** — `lists_sync` doesn't build a correction payload; just short-circuits before cloud touch.
3. **`logger.info` not `logger.warning`** — LOCAL items being skipped is expected behavior, not an error.

**Pin via `tests/test_local_namespace_no_lists_leak.py`** — MUST mock `_get_client` AND `Supabase` and assert ZERO calls when LOCAL sys_id present. RESEARCH.md Pitfall #3 warning sign: "if the mock is called even once with a LOCAL sys_id, the gate is in the wrong place."

---

### `corrections_client.py` modifications (REQ-9)

**Analog:** self at `:619-623` (the existing gate).

**Excerpt** (lines 615-623):
```python
# Phase 85 SYNTH-06 / D-10 — REVIEWS-MODE iteration 1 B1 gate.
# Reject synthetic sys_ids at the WRITE entry point. Page_number
# semantics are undefined for image-less synthetic rows; Phase 87
# will define them. Match the existing return tuple shape.
if is_synthetic_sys_id(document_id):
    return (
        None,
        "synthetic_corrections_disabled: corrections cannot be added to synthetic sys_ids",
    )
```

**Mirror this:** **extend** the gate (don't replace) with a LOCAL OR-clause:
```python
from shared.local_sys_id import is_local_sys_id  # at module top, alongside existing
                                                   # `from shared.synthetic_sys_id import is_synthetic_sys_id`

# Phase 95 — extend the SYNTH-06 gate with LOCAL (REQ-9).
if is_synthetic_sys_id(document_id):
    return (
        None,
        "synthetic_corrections_disabled: corrections cannot be added to synthetic sys_ids",
    )
if is_local_sys_id(document_id):
    return (
        None,
        "local_corrections_disabled: corrections cannot be added to LOCAL sys_ids",
    )
```

**Divergences:**
1. **DO NOT merge into a single `if is_synthetic_sys_id(...) or is_local_sys_id(...):`** — the error code differs (`synthetic_corrections_disabled` vs `local_corrections_disabled` per SPEC REQ-9 acceptance), and tests pin the code via `tests/test_local_namespace_no_corrections_leak.py`.
2. **Two separate `if` clauses** preserves the exact existing message for synthetic + adds a parallel LOCAL message.

---

### `shared/search_serializer.py` modifications (REQ-9 defense-in-depth)

**Analog:** self at `:282+` (`_serialize_item`) + `:52` (existing `is_synthetic_sys_id` import).

**Excerpt** (lines 48-58):
```python
# Phase 85 D-14 (SYNTH-06): is_synthetic field is the single source of truth
# for synthetic-vs-real classification on /api/search, /api/browse, and
# /api/parallels response items.
from shared.synthetic_sys_id import is_synthetic_sys_id

logger = logging.getLogger(__name__)

# Schema version -- bump if envelope/item shape changes incompatibly.
SCHEMA_VERSION = 1
```

**Filter site analog:** `:568-580` (the `items = [_serialize_item(...) for r in results]` listcomp).

**Excerpt** (lines 568-580):
```python
items = [
    _serialize_item(
        r,
        meta_mgr=meta_mgr,
        domain_batch=domain_batch,
        catalog_batch=catalog_batch,
        transcription_sys_ids=transcription_sys_ids,
        printed_sys_ids=printed_sys_ids,
    )
    for r in results
]
```

**Mirror this:** add LOCAL filter as a `filter()` step BEFORE the listcomp:
```python
from shared.local_sys_id import is_local_sys_id   # at module top (alongside synthetic import)

# Phase 95 REQ-9 defense-in-depth — drop LOCAL items before serializing.
# Web Tantivy has no LOCAL data anyway, so this is a belt-and-suspenders gate.
def _is_local_item(result: dict) -> bool:
    display = result.get('display', {}) or {}
    sys_id = display.get('id', '') or result.get('sys_id', '')
    library_code = display.get('library_code', '') or ''
    return library_code == 'LOCAL' or is_local_sys_id(sys_id)

results = [r for r in results if not _is_local_item(r)]    # ← NEW filter
items = [
    _serialize_item(r, ...) for r in results
]
```

**Divergences:**
1. **Filter BEFORE serialize** so the LOCAL row never enters the payload. Apply to BOTH `serialize_search_payload` AND `serialize_parallels_payload` (same module, same filter helper).
2. **Library-code AND sys_id check** — defense in depth catches LOCAL even if a future LOCAL row somehow has a non-`97`-prefixed sys_id.
3. **Pin via `tests/test_local_namespace_no_api_leak.py`** — inject a LOCAL row + assert it's absent from `envelope['results']`.

---

### `web/pages/search.py` + `web/pages/browse.py` modifications (D-30 + D-46)

**Analog:** there's no existing library-filter dropdown that consumes `LIBRARY_CODES.items()` in `web/pages/`. RESEARCH.md `grep "LIBRARY_CODES" web/pages/` returned zero matches (confirmed). The library-filter UI in `web/pages/search.py` uses other channels (`get_library_display` per result row).

**Mirror this:** **the modification may be a NO-OP for `web/pages/search.py` + `web/pages/browse.py`**, since no LOCAL entry can flow through these surfaces from `LIBRARY_CODES.values()` today. BUT the planner MUST still:

1. **Audit `genizah_core.py:1857`** (`for name in LIBRARY_CODES.values():`) — find what consumer iterates the full dict and ensure LOCAL is filtered there if it's a web dropdown builder.
2. **Add `tests/test_web_library_options_no_local.py`** (D-46 / static AST guard) regardless — pre-emptively pin the invariant. Even if no consumer exists today, the test catches the day one is added.

**Test analog:** `tests/test_pgp_filter_cascade.py` (verbatim template for AST scanner).

**Excerpt — full file** (`tests/test_pgp_filter_cascade.py:42-66`):
```python
def test_every_printed_filter_caller_also_calls_pgp_filter():
    """MEDIUM-3 invariant: cascade coverage — printed_filter ⇒ pgp_filter."""
    source = SEARCH_PY.read_text(encoding='utf-8')
    tree = ast.parse(source)
    offenders = []
    for func in _iter_function_defs(tree):
        calls_printed = _function_contains_call(func, '_apply_printed_filter')
        if not calls_printed:
            continue
        if func.name in EXEMPT_FUNCTIONS:
            continue
        calls_pgp = _function_contains_call(func, '_apply_pgp_filter')
        if not calls_pgp:
            offenders.append((func.name, func.lineno))
    assert not offenders, (
        f"Phase 999.2 cascade-coverage drift detected. ..."
    )
```

**Mirror this for D-46:** `tests/test_web_library_options_no_local.py` walks every `.py` file under `web/pages/`, AST-parses each, and asserts: any function that iterates `LIBRARY_CODES` (via `for ... in LIBRARY_CODES.values()` / `.items()` / `.keys()` / dict expansion) must contain a sibling `if code == 'LOCAL'` / `code != 'LOCAL'` guard OR have its function name on an explicit EXEMPT list.

**Divergences:**
1. **The PGP cascade test scans ONE file (`web/pages/search.py`); the D-46 test scans ALL files under `web/pages/`** — broader scope, same AST shape.
2. **EXEMPT_FUNCTIONS may stay empty** (no current consumers).

---

### `web/pages/help.py` modifications (D-31 + D-33 disclosure)

**Analog:** self at `:1-200` (existing English / Hebrew content sections).

**Excerpt** (lines 35-64):
```python
def _create_english_content():
    """Create the English help content."""
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('list').classes('text-2xl text-primary')
            h2('Table of Contents', classes='text-xl font-bold', style='color: var(--text-primary);')
        with ui.column().classes('gap-2'):
            toc_items = [
                ('intro', 'Introduction: How it Works'),
                ('search', 'Search'),
                ...
            ]
```

**Mirror this:**
1. Add a new TOC entry `('my-library', 'My Library — Local Documents')` AND a new section card later in the file.
2. Section content covers per D-31: (a) what gets indexed, (b) where data lives, (c) privacy guarantee + three gates, (d) three-state filter usage, (e) hostname-rename caveat.
3. **D-33 disclosure line MUST appear**: `"Your indexed text is stored on disk in cleartext inside the local index — it is never uploaded to GenizahSearch's servers. Use OS-level disk encryption (BitLocker / FileVault) if you need at-rest encryption."`
4. **D-32 Seewald attribution** in About dialog AND Help: `"My Library feature inspired by Yehuda Seewald's GenizahLocal prototype"`.

**Divergences:** bilingual content — write both EN (`_create_english_content`) and HE (`_create_hebrew_content`) versions. Hebrew is the user's preferred language (UI context).

---

### Desktop Help dialog modifications (D-31 + D-33 disclosure)

**Analog:** `Config.HELP_FILE = os.path.join(INTERNAL_DIR, "Help.html")` at `genizah_core.py:2015`.

**Mirror this:** add an `<h2>My Library</h2>` section to `Help.html` with the same D-31 + D-33 + D-32 content as `web/pages/help.py`. Bilingual: planner adds both EN + HE blocks per the existing `Help.html` bilingual structure.

**Divergences:** static HTML vs NiceGUI components; otherwise identical text content.

---

### `requirements.txt` modifications (D-43)

**Analog:** self (entire file, 16 lines).

**Excerpt:**
```
PyQt6==6.10.2
tantivy==0.25.1
requests==2.32.5
tqdm==4.67.3
colorama==0.4.6
openpyxl==3.1.5
packaging==26.0
python-docx==1.2.0      # ← already present
nicegui==3.8.0
supabase==2.28.0
python-dotenv==1.2.2
keyring==25.7.0
Pillow==12.1.1
numpy==2.4.3
objgraph==3.6.2
```

**Mirror this:** add ONE line with a constraint range (NOT a pin, per D-43 contract `pymupdf>=1.24,<2.0`):
```
pymupdf>=1.24,<2.0
```

**Divergences:**
1. **Range, not pin** — the rest of `requirements.txt` uses `==X.Y.Z` pins; `pymupdf` uses a range per D-43 (RESEARCH.md confirms `1.27.2.3` installed today, but the range tolerates 1.24+ for future stability).
2. **`python-docx` is ALREADY at 1.2.0** — D-43 says to pin `python-docx>=1.0,<2.0` per RESEARCH. Planner picks: keep the existing `==1.2.0` exact pin (consistent with rest of file) OR loosen to `>=1.0,<2.0`. Recommendation: keep `==1.2.0` for reproducible builds.

**Pin via `tests/test_local_indexer.py::test_pymupdf_hebrew_extraction_quality`** (D-44 — REAL Hebrew PDF fixture, NOT just dead-code helpers).

---

### `GenizahSearchPro.spec` modifications (D-43)

**Analog:** self (entire file, 54 lines).

**Excerpt** (lines 1-9):
```python
# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

datas = [('icon.ico', '.'), ('Help.html', '.'), ('oxford_full_db.json', '.'), ...]
binaries = []
hiddenimports = ['tantivy', 'numpy', 'PIL']
tmp_ret = collect_all('tantivy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
```

**Mirror this:** add `fitz` + `pymupdf` to `hiddenimports` AND chain a second `collect_all('pymupdf')`:
```python
hiddenimports = ['tantivy', 'numpy', 'PIL', 'fitz', 'pymupdf']
tmp_ret = collect_all('tantivy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
# Phase 95 D-43 — PyMuPDF C-extension binaries must be explicitly collected.
# Without this, dist/GenizahSearch.exe raises ModuleNotFoundError: fitz._fitz
# at runtime (Pitfall #5).
tmp_ret = collect_all('pymupdf')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
```

**Additionally (RESEARCH.md Pitfall #5)** — for `--noconsole` Windows builds, add an stdout/stderr None-check shim at app startup. Planner identifies the existing entrypoint in `genizah_app.py`:
```python
import sys, os
if sys.stdout is None: sys.stdout = open(os.devnull, "w")
if sys.stderr is None: sys.stderr = open(os.devnull, "w")
```

**Divergences:** none — pattern is literally adding a sibling `collect_all` call. Pin via `@pytest.mark.packaging` smoke test (D-43) — runs against `dist/GenizahSearch.exe`, imports `fitz`, opens 1-page Hebrew PDF, asserts text returned.

---

### `CompileScriptGenizah.iss` modifications (verify)

**Analog:** self (entire file, 50+ lines).

**Excerpt** (lines 1-20):
```
#define MyAppName "Genizah Search Pro"
#define MyAppVersion "7.13.0"
#define MyAppPublisher "Hillel Gershuni / Dicta"
#define MyAppURL "https://www.GenizahSearch.com/"
#define MyAppExeName "GenizahSearchPro.exe"
```

**Mirror this:** **likely a no-op**. Inno Setup's `[Files]` section bundles whatever PyInstaller emits in `dist/GenizahSearchPro/`. If `collect_all('pymupdf')` in the .spec puts the binaries under `dist/`, Inno Setup picks them up automatically.

**Divergences:**
1. **Version bump** is REQUIRED if Phase 95 ships in a new release — bump `MyAppVersion "7.13.0"` to `MyAppVersion "7.14.0"` via `python scripts/bump_version.py 7.14.0` (per CLAUDE.md). This is a release-time concern, not a Phase 95 source-edit concern.
2. **MEMORY-relevant** (feedback_release_iss_hardcoded_paths.md): `OutputDir=C:\GenizahSearch\dist` and `SetupIconFile=C:\GenizahSearch\icon.ico` are hardcoded to the main checkout path. Worktree builds must junction `dist/` before running ISCC.exe — planner notes for release workflow.

---

## Shared Patterns (cross-cutting)

### Pattern: Helper-module template (`shared/` + `tests/`)

**Source:** `shared/synthetic_sys_id.py` (140 lines) + `tests/test_synthetic_sys_id.py` (237 lines) + `tests/fixtures/synthetic_fixtures.py`.

**Apply to:** `shared/local_sys_id.py` + `tests/test_local_sys_id_namespace.py` + `tests/fixtures/local_sys_id_fixtures.py` (new) + `tests/test_local_sys_id_parser_compat.py`.

**Shape:**
1. Module-level constants block (4 lines: `_LOCAL_PREFIX`, `_MACHINE_PAD`, `_HASH_PAD`, `_TOTAL_LENGTH`).
2. Single-purpose detector function with `>>> ` doctests (`is_local_sys_id`).
3. Generator function(s) (`generate_local_sys_id` + `_canonical_filepath` + `_machine_id` + `_content_hash`).
4. Sister-test file with TestXxx classes per helper.
5. Fixtures file: `LOCAL_GOLDEN_CASES`, `LOCAL_REAL_ALMA_NEGATIVE_CASES`, `LOCAL_SYNTHETIC_99_NEGATIVE_CASES`, `D_19_NORMALIZATION_NEGATIVES`.
6. **`TestNoIntCoercion` AST lint** is REPLICATED with allowlist `{"shared/local_sys_id.py", "tests/test_local_sys_id_namespace.py"}` — defends D-19 helper integrity the same way SYNTH-01 defends synthetic.

### Pattern: Three-state filter button (Phase 93 / PGP-FILTER)

**Source:** `web/pages/search.py:1432-1480` (printed_filter button + `_toggle_pgp_filter` + `_update_pgp_filter_btn`).

**Apply to:** desktop search, Composition Search, Parallels result toolbars per REQ-6 / D-10. Same cycle order, same `persist_value` per surface (D-39 keys).

**Cycle excerpt** (`web/pages/search.py:1441-1444`):
```python
states = ['all', 'only_pgp', 'hide_pgp']  # D-02 cycle order
current_idx = states.index(search_state.pgp_filter)
search_state.pgp_filter = states[(current_idx + 1) % 3]
persist_value('search_pgp_filter', search_state.pgp_filter)
```

**Mirror this for LOCAL:**
```python
# Desktop: read/write via QSettings (D-39), NOT web safe_storage.
states = ['all', 'only_local', 'no_local']
current_idx = states.index(self.local_filter)
self.local_filter = states[(current_idx + 1) % 3]
QSettings("Dicta", "GenizahSearchPro").setValue(
    "myLibrary/search_local_filter", self.local_filter
)
self._update_local_filter_btn()
self._apply_local_filter_and_render()
```

**Divergences:**
1. **Desktop persistence is `QSettings`** (per D-39 `myLibrary/search_local_filter`, `myLibrary/composition_local_filter`, `myLibrary/parallels_local_filter`), NOT the web `persist_value` helper.
2. **D-10 P1 fix — no-op when no LOCAL hits:** when state is `only_local`/`no_local` AND zero LOCAL hits in current results, filter is rendered as a no-op + inline chip surfaces `"My Library filter inactive — no LOCAL hits in this query"`. State preserved across query change. Pin via `tests/test_local_filter_cascade.py::test_no_op_when_no_local_hits`.
3. **Cascade discipline (per Phase 93)** — LOCAL filter applied AFTER PGP filter + printed filter + exclusions + refinement chain. Static AST test (`tests/test_local_filter_cascade.py`) pins ordering.

### Pattern: Static AST cascade-coverage test

**Source:** `tests/test_pgp_filter_cascade.py` (entire file, 121 lines).

**Apply to:**
- `tests/test_local_filter_cascade.py` — every desktop function that applies the PGP filter must ALSO apply the LOCAL filter (cascade-discipline rule).
- `tests/test_web_library_options_no_local.py` (D-46) — every web file under `web/pages/` that iterates `LIBRARY_CODES` must filter out `LOCAL`.

**AST scanner excerpt** (`tests/test_pgp_filter_cascade.py:23-40`):
```python
def _function_contains_call(func_node, name: str) -> bool:
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            callee = node.func
            if isinstance(callee, ast.Name) and callee.id == name:
                return True
            if isinstance(callee, ast.Attribute) and callee.attr == name:
                return True
    return False

def _iter_function_defs(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node
```

**Mirror this:** verbatim copy of `_function_contains_call` + `_iter_function_defs` + the assertion pattern.

**Divergences:**
- Test target path: `genizah_app.py` (desktop) instead of `web/pages/search.py`. Same scanner shape; different file.
- Function-name pairs: `_apply_pgp_filter` ⇒ `_apply_local_filter` (LOCAL cascade) and `for ... in LIBRARY_CODES.values()` ⇒ must contain `code == 'LOCAL'` / `code != 'LOCAL'` guard (web LOCAL exclusion).

### Pattern: QThread worker + cooperative cancellation

**Source:** `gui_threads.py:33-89` (`IndexerThread` + `SearchThread.cancel_flag` pattern).

**Apply to:** `LocalIndexerWorker` in `desktop/my_library_tab.py` (or factored into `gui_threads.py` — planner picks).

**Cancellation excerpt** (`gui_threads.py:63-90`):
```python
self.cancel_flag = False
...
def run(self):
    _prevent_sleep()
    try:
        def cb(curr, total):
            if self.cancel_flag:
                raise InterruptedError("Search cancelled by user")
            self.progress_signal.emit(curr, total)
        results = self.searcher.execute_search(...)
        self.results_signal.emit(results)
    except InterruptedError:
        self.results_signal.emit([])
    except Exception as e:
        self.error_signal.emit(str(e))
    finally:
        _allow_sleep()
```

**Mirror this:**
- `self._cancel_requested = False` instance flag.
- Cancellation check between files AND between PDF pages / DOCX 20-paragraph chunks (D-24 Codex revision).
- `try/except InterruptedError` arm: rollback partial Tantivy state via `writer.rollback()` then re-open (D-24 P1 fix).

**Divergences:**
- Two-tier cancellation (between files + within file) — `SearchThread` is single-tier.
- `QMutex` (D-25) wrapping ALL mutations — `SearchThread` doesn't need this because main index is single-writer at build time.

### Pattern: Cloud-write rejection (REQ-9 invariant)

**Source:** `corrections_client.py:619-623` (existing synthetic gate).

**Apply to:**
- `corrections_client.py` (extend existing gate with LOCAL OR clause).
- `lists_sync.py` (NEW gate at TOP of `sync_item_to_cloud` + `sync_list_to_cloud` per D-30 P0).
- `shared/search_serializer.py` (NEW filter before `_serialize_item`).

**Test pattern source:** `tests/test_synthetic_sys_id.py::TestRealAlmaCollisionNegative` (assertion shape).

**Mirror this:**
- Each gate logs at INFO level when triggered (no exception, no error).
- Each test mocks the cloud client (`_get_client`, `Supabase.table`) and asserts ZERO calls.
- Symmetric structure across all three gates: `if is_local_sys_id(...) → log → short-circuit return`.

### Pattern: Cross-app shared helper

**Source:** Phase 94 invariant — `shared/` helpers consumed by BOTH web AND desktop.

**Apply to:** `shared/local_sys_id.py` (consumed by `lists_sync.py` + `corrections_client.py` + `shared/search_serializer.py`), `shared/local_indexer.py` (consumed by desktop only — Qt-free body so future web wiring is trivial).

**Divergences:**
- `shared/local_indexer.py` is desktop-only DESPITE living in `shared/`. Justified because: (a) future-proofing for web demand, (b) test isolation, (c) consistent with `shared/document_service.py` (extracted in Phase 8, consumed mostly by desktop today).

---

## Per-Test Pattern Assignments

### Tests with EXACT analogs

| New test file | Analog | Notes |
|---------------|--------|-------|
| `tests/test_local_sys_id_namespace.py` | `tests/test_synthetic_sys_id.py` (verbatim — 237 lines) | Replace `is_synthetic_sys_id` with `is_local_sys_id`; replace `SYNTHETIC_*` fixtures with `LOCAL_*` fixtures; KEEP `TestNoIntCoercion` AST lint with allowlist `{"shared/local_sys_id.py", "tests/test_local_sys_id_namespace.py"}`. |
| `tests/test_local_filter_cascade.py` | `tests/test_pgp_filter_cascade.py` (verbatim AST scanner; 121 lines) | Scan `genizah_app.py` instead of `web/pages/search.py`; function names `_apply_pgp_filter` ⇒ `_apply_local_filter`. Add a `test_no_op_when_no_local_hits` per D-10 P1. |
| `tests/test_web_library_options_no_local.py` | `tests/test_pgp_filter_cascade.py` (AST scanner) | Scan ALL `web/pages/*.py` for `LIBRARY_CODES` iteration; assert each has a `code == 'LOCAL'` / `code != 'LOCAL'` guard. |
| `tests/test_local_namespace_no_corrections_leak.py` | `tests/test_synthetic_sys_id.py::TestRealAlmaCollisionNegative` (assertion pattern) | Mock corrections HTTP call; assert ZERO calls; assert error code `local_corrections_disabled`. |
| `tests/test_local_namespace_no_lists_leak.py` | (no exact analog) | Mock `_get_client` + `Supabase.table`; assert ZERO calls; mock placed BEFORE `is_sync_available()`. Critical: catches Codex P0 Pitfall #3 by asserting `_get_client.call_count == 0` after `sync_item_to_cloud("970012345601234567", "list-id")`. |
| `tests/test_local_namespace_no_api_leak.py` | (similar to corrections leak) | Build a results list with one LOCAL row injected; call `serialize_search_payload`; assert `len(envelope['results']) == 0` (or original count minus 1). |

### Tests with role-match analogs

| New test file | Analog | Notes |
|---------------|--------|-------|
| `tests/test_local_indexer.py` | `tests/test_synthetic_sys_id.py` (pytest class layout) + `seewald_addition/genizah_make_index.py:67-105` (helpers under test) | TestRtlHelpers class (port D-02 dead-code tests); TestSupportedFileTypes class (.docx/.pdf/.txt/.html); test_pymupdf_hebrew_extraction_quality (D-44 — uses fixture). |
| `tests/test_local_indexer_incremental.py` | Seewald `processed_files` cache pattern | Three tests: second-scan-fast (≤ 5% wall time), modify-reextract-only-modified, delete-removes-rows. |
| `tests/test_local_indexer_scale.py` (slow) | (none — gated `@pytest.mark.slow`) | 5,000 small .txt fixtures; assert ≤ 10 min wall time + RSS < 500 MB + `QApplication.processEvents()` round-trip < 100 ms. |
| `tests/test_export_dossier_local_handling.py` | `tests/test_export_xlsx_cross_parity.py` (cross-app parity precedent) | Desktop export INCLUDES LOCAL row; web export EXCLUDES LOCAL row via `skip_local=True` kwarg (D-45). |

### Tests with no analog (write from scratch using research patterns)

| New test file | Research source | Notes |
|---------------|-----------------|-------|
| `tests/test_local_sys_id_parser_compat.py` | CONTEXT D-13 P0 fix | Assert `parse_header_smart("970012345601234567_LOCAL_P3_F0042") == ("970012345601234567", "3")`. Assert `parse_full_id_components(...)['ie_id'] == "F0042"`. |
| `tests/test_local_post_dedup_merge.py` | CONTEXT D-08 P0 fix | Inject LOCAL hit BEFORE `_deduplicate(results)` call → assert dropped. Inject AFTER → assert survives. |
| `tests/test_side_index_merge.py` | SPEC REQ-2 acceptance | Index 10 LOCAL files; run phrase search hitting NLI + LOCAL; assert both in result list. Force-corrupt LOCAL index file; assert main still works. |
| `tests/test_local_delete_by_uid.py` | RESEARCH Pitfall #2 + tantivy-py issue #297 | Insert doc with `unique_id="LOCAL_..._P1"`; call `writer.delete_documents("unique_id", uid)`; commit; assert searcher returns 0 hits. Without `tokenizer_name="raw"` this WOULD silently fail. |
| `tests/test_local_two_phase_commit.py` | RESEARCH Code Examples lines 486-525 | Fault-injection: kill process between Tantivy commit + SQLite UPDATE; restart; assert recovery re-extracts. |
| `tests/test_local_indexer_mutex.py` | CONTEXT D-25 P1 fix | Spawn N concurrent Refresh/Remove requests; assert no interleaving in SQLite operation log. |
| `tests/test_local_index_open_fallback.py` | CONTEXT D-37 | Mock `tantivy.Index.open(Config.LOCAL_INDEX_DIR)` to raise; assert main search returns Genizah-only results without traceback. |
| `tests/test_local_lab_invalidation.py` | CONTEXT D-38 / RESEARCH Pitfall #7 | weights_hash mismatch → assert banner surfaced + Rebuild button hooked. |
| `tests/test_local_filter_persistence.py` | CONTEXT D-39 | Set each of 3 QSettings keys; restart app harness; assert values restored. |
| `tests/test_local_unavailable_folder.py` | CONTEXT D-40 | Register folder; delete folder on disk; trigger auto-rescan; assert `folders.status = 'unavailable'` and rows NOT purged. |
| `tests/test_canonical_filepath.py` | CONTEXT D-42 | Windows-specific fixtures: UNC path, junction-link, drive-letter casing, 8.3 short names. Assert `_canonical_filepath` produces same string for all equivalent inputs. |
| `tests/test_folder_overlap_detection.py` | CONTEXT D-17 P1 fix | Junction-link-to-folder, UNC mount, drive-letter-equivalent, mixed-case. Use `os.path.commonpath` overlap check. |
| `tests/test_local_schema_evolution.py` | CONTEXT D-35 | Introspect `local_files` / `local_pages` / `processed_files` / `folders` tables; assert column shape matches contract. |

### Fixtures

| Fixture | D-44 / planner picks | Notes |
|---------|----------------------|-------|
| `tests/fixtures/local_indexer/hebrew_sample.pdf` | Small Word-authored Hebrew PDF (RESEARCH Open Question 6 recommendation) | Multi-column or single-column Hebrew. Avoid scanned-Hebrew-from-Acrobat (highest RTL-corruption risk). |
| `tests/fixtures/local_indexer/hebrew_sample.expected.txt` | Hand-corrected reading-order reference | Used by `test_pymupdf_hebrew_extraction_quality`. |
| `tests/fixtures/local_indexer/mirror_reversed.pdf` | (existing Seewald fixture if any, else new) | For dead-code `_fix_rtl_line` test. |
| `tests/fixtures/local_indexer/single_word_per_line.pdf` | (new) | For dead-code `_join_fragmented_lines` test. |
| `tests/fixtures/local_indexer/sample.docx` | Word doc with > 20 paragraphs (D-04 chunking test) | |
| `tests/fixtures/local_indexer/sample.txt` | utf-8-sig encoded Hebrew text | |
| `tests/fixtures/local_indexer/unsupported.html` | Empty `.html` to trigger `unsupported_extension` status | |
| `tests/fixtures/local_sys_id_fixtures.py` | New fixtures module (mirrors `tests/fixtures/synthetic_fixtures.py`) | `LOCAL_GOLDEN_CASES`, `LOCAL_NEGATIVE_CASES`, `D_19_NORMALIZATION_NEGATIVES`. |

---

## No Analog Found (planner uses RESEARCH.md patterns)

| Component | Reason | Use Instead |
|-----------|--------|-------------|
| Reciprocal Rank Fusion (RRF) merger | No existing two-index fusion in the codebase | RESEARCH.md Pattern 1 (`_rrf_merge`, lines 381-416) — port verbatim. |
| Tantivy delete-by-UID with `raw` tokenizer | Main index never deletes; no existing pattern | RESEARCH.md Pattern 3 (`delete_file_from_local_index`, lines 460-477). |
| Two-phase commit (SQLite pending → Tantivy → SQLite committed) | Novel for this codebase | RESEARCH.md Pattern 4 (`commit_batch` + `startup_recovery`, lines 486-525). |
| QMutex-based indexer serialization | Novel | PyQt6 `QMutex` docs + RESEARCH Pitfall #6 + D-25 contract. |
| `weights_hash` invalidation contract for LAB | Novel | CONTEXT D-09 / D-38 + RESEARCH Pitfall #7. |
| LOCAL `unique_id` / `full_header` parseable format | Novel — main format is `IE\d+_P\d+_FL\d+`; LOCAL uses `LOCAL_\d{18}_P\d+` + `\d{18}_LOCAL_P\d+_F\d{4}` | CONTEXT D-34 (pinned strings). |

---

## Metadata

**Analog search scope:**
- `shared/` (all 16 modules including `synthetic_sys_id.py`, `search_serializer.py`, `document_service.py`, `fjms_service.py`, `export_dossier.py`)
- `genizah_core.py` (8,300-line file — anchors verified per RESEARCH.md primary sources)
- `genizah_app.py` (18,500-line file — anchors at `:3079`, `:5909`, `:11621`, `:16534`, `:16741`)
- `gui_threads.py` (28 worker classes; `IndexerThread` and `SearchThread` are the load-bearing analogs)
- `corrections_client.py`, `lists_sync.py` (cloud-write gates)
- `seewald_addition/` (4 prototype files — `genizah_local_indexer.py` is the port source; `genizah_make_index.py` has the RTL helpers)
- `tests/` (231 test files — `test_synthetic_sys_id.py` + `test_pgp_filter_cascade.py` are the template tests; `test_export_xlsx_cross_parity.py` is the cross-app parity precedent)
- `web/pages/` (40 page modules — `help.py`, `search.py`, `browse.py` are the modification targets)
- `desktop/` (9 modules — package exists but is light; new `my_library_tab.py` joins it)
- `GenizahSearchPro.spec`, `CompileScriptGenizah.iss`, `requirements.txt`

**Files read in pattern extraction:** 16 source files + 3 test files + 1 fixture file + 2 packaging files = 22 files.

**Pattern extraction date:** 2026-05-21.

**Cross-references for planner:**
- 5 Codex P0/P1 fixes have explicit pattern rows: D-08 (`genizah_core.py:7390` insert site + RESEARCH Pattern 1 RRF body), D-13 (`genizah_core.py:3640-3681` regex broadening), D-21 (RESEARCH Pattern 4 two-phase commit), D-25 (`desktop/my_library_tab.py` QMutex wrap), D-30 (`lists_sync.py:737` first-statement gate placement).
- 26 test files mapped to closest existing tests; 16 use exact / role-match analogs, 10 use research patterns or write from scratch.
- 1 path drift correction noted: MyLibraryTab is the **7th** tab (not 6th per SPEC) — RESEARCH Pitfall #4. Planner should not pin a tab index; use widget reference instead.
