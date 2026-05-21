# Phase 95: My Library — Local Document Indexing — Research

**Researched:** 2026-05-21
**Domain:** Desktop-only side-index for user-owned `.docx` / `.pdf` / `.txt` files; cross-app cloud-write gates; LAB-mode parallel side-index
**Confidence:** MEDIUM-HIGH (codebase anchors verified; one P1 finding that contradicts a CONTEXT decision; library quirks documented)

## Summary

CONTEXT.md (46 decisions including 13 Codex revisions + D-34..D-46 gap closures) is authoritative for the planning shape; this research enriches the codebase view and surfaces three issues the planner MUST address that CONTEXT didn't capture:

1. **D-01 premise is partly wrong — PyMuPDF does NOT solve Hebrew RTL "at the source."** The PyMuPDF maintainers explicitly classify Arabic/Hebrew RTL ligature reversal as `wontfix` on GitHub issue #2199 [CITED: github.com/pymupdf/PyMuPDF/issues/2199]. `get_text("blocks")` returns RTL text with words/glyphs in visual (backward) order; bidi post-processing is the caller's responsibility. The "dead-code helpers" in D-02 may need to be LIVE code, OR the runtime test in D-44 must be permissive enough that the v1 happy path still passes without the helpers (and the planner accepts that Hebrew PDFs from Word are typically already-bidi'd correctly while Hebrew PDFs from Adobe/scanned tools are not). This is a planner discretion point that needs a real fixture decision.

2. **Tantivy delete-by-term is unsafe on the existing `unique_id` schema.** The main-index schema at `genizah_core.py:5125` adds `unique_id` as a `text_field` WITHOUT `tokenizer_name="raw"` — meaning the field is tokenized with the default tokenizer. tantivy-py issue #297 documents this exact bug: `writer.delete_documents("unique_id", "FOO_BAR")` silently fails on tokenized fields [CITED: github.com/quickwit-oss/tantivy-py/issues/297]. The main index is rebuilt from scratch on every refresh so the bug is dormant there. For the LOCAL side-index where incremental delete IS the central operation (D-20, D-36), the LOCAL schema MUST set `tokenizer_name="raw"` on `unique_id` — a deliberate departure from SPEC's "schemas match" constraint, ONLY on this field, and ONLY on the LOCAL index. Planner must call this out as a schema-divergence decision.

3. **MyLibraryTab is the 7th tab, not the 6th.** `genizah_app.py:3079-3091` registers SIX tabs today (Search, Composition Search, Browse by Shelfmark, Browse by Identification, Personal Lists, Community). SPEC REQ-8 says "6th tab" — this was written against a slightly older codebase where the Community tab didn't exist. MyLibraryTab inserts as the 7th. Cosmetic but worth flagging so VERIFICATION doesn't reject on tab count.

**Primary recommendation:** Plan around PyMuPDF as the default extractor with a fallback-rich fixture corpus, treat the LOCAL Tantivy schema's `unique_id` field as a deliberate schema variant (raw tokenizer), update SPEC REQ-8's tab-position language, and adopt RRF k=60 per D-08 Codex revision.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Folder scan + file enumeration | Desktop (Qt QThread) | — | OS-level filesystem walk; must not block UI |
| PDF / DOCX / TXT extraction | `shared/local_indexer.py` (Qt-free pure module) | — | Testable offline; both apps could call it in future, but only desktop wires it today |
| Tantivy side-index build + write | `shared/local_indexer.py` | — | Same module, gated behind a writer mutex per D-25 |
| Tantivy side-index search | `genizah_core.py` (alongside main index) | — | Searcher merge happens in the same module that owns the main searcher; co-locate to keep RRF reasoning auditable |
| LAB side-index build + search | `genizah_core.py:lab_composition_search` extension | `shared/local_indexer.py` (build side) | LAB has its own scoring path — must NOT use BM25; planner extends `lab_composition_search` to query both lab indexes |
| sys_id namespace helper | `shared/local_sys_id.py` (new) | — | Mirrors `shared/synthetic_sys_id.py` module template; consumed by both web + desktop |
| Cloud-write gates (3 surfaces) | `shared/` | `corrections_client.py`, `lists_sync.py`, `shared/search_serializer.py` | Each surface imports the helper and checks at the TOP of its function (per D-30 P0 fix) |
| Three-state filter button (UI) | Desktop (`genizah_app.py`) | — | No web parity (D-29: feature is desktop-only) |
| Filter state persistence | `shared/session_persistence.py` + QSettings hybrid | SQLite (D-15) | Folder list → SQLite (portable). Filter state → QSettings or session_persistence (planner picks) |
| Browse panel text-only mode | Desktop browse panel | — | Reuses existing "no image" code path per `<specifics>` (D-27) |
| About / Help attribution | Both apps, both languages | — | D-31 + D-32 |

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `pymupdf` (imports as `fitz`) | `>=1.24,<2.0` (current shipped: 1.27.2.3) | PDF text extraction via `Page.get_text("blocks")` | Best Hebrew RTL output among Python PDF libraries (though not perfect — see Pitfalls); Seewald's own prototype prefers it; the maintainers' wontfix on RTL ligatures is an industry-wide problem, not a PyMuPDF-specific weakness [CITED: github.com/pymupdf/PyMuPDF/issues/2199] |
| `python-docx` | `>=1.0,<2.0` (current installed: 1.2.0) | DOCX text extraction via `doc.paragraphs` | De-facto standard; lightweight (~2 MB); already familiar to user via Seewald prototype |
| `tantivy` (tantivy-py) | `==0.25.1` (already installed) | LOCAL side-index + LOCAL LAB side-index | Schema parity with main index (already a SPEC constraint); existing dependency [VERIFIED: pip show tantivy] |
| `sqlite3` (stdlib) | Python 3.11 builtin | `local_index.sqlite3` cache (folders, processed_files, local_pages, local_files) | No new dependency; mature WAL story |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `python-bidi` | Optional, NOT recommended for v1 | Bidi algorithm post-processing for RTL | If a future fallback needs it; v1 ships without per CONTEXT D-02 (helpers ported as dead code) |
| `unicodedata` (stdlib) | builtin | RTL ratio detection (`_rtl_ratio`) | Already used in the dead-code helpers per D-02 |
| `pdfplumber` | NOT INSTALLED for v1 | PDF fallback extractor | Deferred per D-01; helpers stay as dead code in `shared/local_indexer.py` |
| `pypdf` | NOT INSTALLED for v1 | PDF last-resort fallback | Deferred per D-01 |
| `chardet` | Explicitly REJECTED | TXT encoding detection | Too slow/unreliable per D-07; not a candidate |

### Version verification

```bash
$ python -c "import fitz; print(fitz.VersionBind)"
1.27.2.3
$ pip show python-docx | grep Version
Version: 1.2.0
$ pip show tantivy | grep Version
Version: 0.25.1
```

All [VERIFIED: pip] on this machine 2026-05-21.

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| PyMuPDF (`fitz`) | `pdfplumber` | More forgiving on tables; significantly worse Hebrew RTL extraction; would force RTL helpers to be runtime-active |
| PyMuPDF | `pypdf` (formerly PyPDF2) | Pure-Python (smaller binary); much worse extraction quality across the board |
| `python-docx` | `docx2txt` | Smaller surface area; loses paragraph + table fidelity; rejected because we need paragraph boundaries for D-04's 20-paragraph chunking |
| Reciprocal Rank Fusion | Raw BM25 score sort | BM25 IDF is index-local — scores from two independent indexes are NOT comparable; RRF k=60 is the industry standard [CITED: opensearch.org/blog/introducing-reciprocal-rank-fusion-hybrid-search] |
| RRF k=60 | RRF k=10 or k=100 | k=60 dampens top-rank dominance moderately; the Cormack/Clarke original paper and OpenSearch/Elasticsearch defaults all converge on 60 [CITED: ai21.com glossary] |

**Installation (additions to `requirements.txt` + `requirements-desktop.txt`):**

```
pymupdf>=1.24,<2.0
python-docx>=1.0,<2.0
```

**Critical packaging delta in `GenizahSearchPro.spec`** [VERIFIED: read file 2026-05-21]:

Today the spec only collects `tantivy`:

```python
hiddenimports = ['tantivy', 'numpy', 'PIL']
tmp_ret = collect_all('tantivy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
```

PyMuPDF / `fitz` is NOT mentioned anywhere. D-43's premise (packaging fix required) is verified. Required additions:

```python
hiddenimports = ['tantivy', 'numpy', 'PIL', 'fitz', 'pymupdf']
tmp_ret = collect_all('tantivy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('pymupdf')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
# python-docx is pure Python and PyInstaller auto-discovers it from `import docx`
```

## User Constraints (from CONTEXT.md)

### Locked Decisions

> Verbatim from `95-CONTEXT.md` <decisions> — 46 decisions D-01 through D-46.

**Extraction:**
- D-01: PyMuPDF only for PDF; no `pdfplumber`/`pypdf` fallbacks in v1. `get_text("blocks")` is the call. [Codex revision: PyInstaller packaging needs explicit `fitz`/`pymupdf` hidden-import + binary-collect — see D-43.]
- D-02: REQ-4 RTL helpers (`_fix_rtl_line`, `_fix_rtl_page`, `_join_fragmented_lines` from `seewald_addition/genizah_make_index.py:67-105`) ported verbatim to `shared/local_indexer.py` but NEVER invoked at runtime in v1. [Codex revision: ADD a real PyMuPDF Hebrew test fixture per D-44.]
- D-03: PDF page-break model — one Tantivy doc per PDF page.
- D-04: DOCX page-break model — split every 20 paragraphs.
- D-05: Scanned PDF (no text layer) → `status="no_text_layer"` when total chars < 50.
- D-06: Empty pages (< 10 chars after strip) skipped silently.
- D-07: TXT encoding — START with `utf-8-sig` only; planner records final policy in plan after smoke test. **Open decision.**

**Result merger:**
- D-08: Main search merger — **RRF k=60** per Codex P0 (NOT raw BM25). LOCAL hits merge **AFTER** `_deduplicate()` at `genizah_core.py:7390` per Codex P0. Tie-break: Genizah first.
- D-09: Composition Search / Parallels — parallel LOCAL LAB side-index built in same MyLibraryTab run. Custom fingerprint scoring preserved (NOT BM25). [Codex revision: `weights_hash` invalidation contract → D-38.]
- D-10: Filter button labels `Filter Local` / `Only Local` / `No Local` (EN); `סנן מקומי` / `רק מקומי` / `ללא מקומי` (HE). [Codex revision: no-op when no LOCAL hits — inline chip per D-10 P1 fix.]
- D-11: Reuse existing `COL_SRC` with blue `#3498db`; visibility rule extends. [Codex revision: export-path audit → D-45.]
- D-12: Composition Search / Parallels result tables — new compact `Source` column uniformly (planner audits during plan).
- D-13: `LIBRARY_CODES` extension `"LOCAL": "My Library"` / `"הספרייה שלי"`. [Codex revision P0: `parse_header_smart` / `parse_full_id_components` must accept `97`-prefix per D-13 fix → D-34 anchor.]

**Storage:**
- D-14: `Config.LOCAL_INDEX_DIR = os.path.join(INDEX_DIR, "LocalIndex")` + `Config.LOCAL_LAB_INDEX_DIR = os.path.join(INDEX_DIR, "LocalLabIndex")`.
- D-15: Folder list in SQLite (`folders` table), NOT QSettings — portability fix per Codex.
- D-16: Multi-folder support in v1 (deliberate SPEC expansion).
- D-17: Folder uniqueness — reject overlaps. [Codex revision: Windows-aware normalization per D-42.]
- D-18: `content_hash` = canonical-filepath SHA256 prefix (D-42 normalization).
- D-19: `machine_id` = hostname SHA256 prefix, modulo 10⁸ for digit width. [Codex revision: collision handling.]
- D-20: Folder removal — synchronous delete-by-uid via `local_pages` sidecar (D-35).

**Lifecycle:**
- D-21: Batch commit every 25 files via two-phase protocol (`pending` → Tantivy commit → `committed`).
- D-22: Status row two-stage UX.
- D-23: Per-file Qt signals.
- D-24: Cancellation — between files AND between pages/chunks per Codex revision.
- D-25: Single indexer QMutex protecting all side-index mutations.
- D-26: Pre-scan count (file + bytes) per Codex P2.

**Hit interaction:**
- D-27: LOCAL hit click → Browse panel text-only mode.
- D-28: `Open file` button on Browse toolbar.
- D-29: Browse tab — LOCAL search-only (NOT in Browse tab) in v1.

**Hardening:**
- D-30: LOCAL gate at TOP of `sync_item_to_cloud` BEFORE `_get_client()` and `sync_list_to_cloud()`. [Codex P0.]

**Docs:**
- D-31 / D-32 / D-33: Help section + Seewald attribution + cleartext-disclosure line.

**Gap closures (D-34 .. D-46):**
- D-34: LOCAL `unique_id` = `LOCAL_{sys_id}_P{page_num}`; `full_header` = `{sys_id}_LOCAL_P{page_num}_F{file_id:04d}`.
- D-35: `local_files` sidecar SQLite table.
- D-36: Modified-file algorithm (delete-then-insert via `local_pages`).
- D-37: Side-index missing/corrupt fallback.
- D-38: LAB invalidation triggers.
- D-39: Per-surface filter QSettings keys.
- D-40: Unavailable folder at startup behavior.
- D-41: 2 GB ceiling = source file size.
- D-42: `_canonical_filepath` helper.
- D-43: PyInstaller / Inno Setup for PyMuPDF.
- D-44: PyMuPDF Hebrew runtime test fixture.
- D-45: Export-path LOCAL handling.
- D-46: Web-consumer static guard.

### Claude's Discretion

- D-07 follow-up: TXT encoding policy after smoke test.
- D-12 follow-up: column position in Composition/Parallels tables.
- D-32 follow-up: Hebrew translation review of Seewald attribution.
- D-44 follow-up: select / create canonical Hebrew PDF fixture.
- Per-file status column widths, button colors, toast styling — planner discretion.

### Deferred Ideas (OUT OF SCOPE)

- "My Library" filter in Browse tab (future phase).
- Cloud-synced Lists for LOCAL items.
- OCR for image-only PDFs.
- Additional file types (`.epub`, `.md`, `.html`, `.rtf`, `.doc`).
- Seewald prototype upgrade path.
- Multi-machine sync.
- `QFileSystemWatcher`.
- Pdf fallback extractors (`pdfplumber`, `pypdf`) wired in.
- Content-addressed sys_id (file-content SHA256 dedup).
- Encrypted side-index.
- `Open containing folder` button.
- Portable-friendly UI-pref store.
- `local_files.sha256_full` content dedup.

## Phase Requirements

> Mapping from SPEC.md REQ-1..REQ-10 → research findings that enable implementation. The planner uses this table to map requirements to plan slots.

| ID | Description | Research Support |
|----|-------------|------------------|
| REQ-1 LOCAL-NAMESPACE | `97`-prefix 18-digit numeric sys_id, `is_local_sys_id` helper | `shared/synthetic_sys_id.py` is the module template (78 lines, single-purpose). New module `shared/local_sys_id.py` mirrors structure with `_LOCAL_PREFIX = "97"`, `_MACHINE_PAD = 8`, `_HASH_PAD = 8`, `_TOTAL_LENGTH = 18`. Collision resolution per D-19 Codex revision. |
| REQ-2 SIDE-INDEX | Separate Tantivy index, schema-match, both queried + merged | Main schema at `genizah_core.py:5124-5136` [VERIFIED]: 11 fields. LOCAL schema mirrors all except `unique_id` MUST use `tokenizer_name="raw"` (see Pitfall #1 below). RRF merger per D-08. Per Codex P0, merge AFTER `_deduplicate()` at `:7390`. |
| REQ-3 FILE-TYPES | `.docx` / `.pdf` / `.txt` only | `python-docx` 1.2.0, PyMuPDF 1.27.2.3, stdlib `open(encoding='utf-8-sig')`. Unsupported → `status="unsupported_extension"` row. |
| REQ-4 RTL-EXTRACTION | Dead-code helpers + REAL fixture per D-44 | Helpers verbatim from `seewald_addition/genizah_make_index.py:67-105` [VERIFIED — read 2026-05-21]. Real fixture per D-44. **WARNING:** see Pitfall #5 — PyMuPDF doesn't actually solve RTL universally. |
| REQ-5 INCREMENTAL-REINDEX | SQLite `processed_files(filepath, mtime, size, sys_id)` mtime cache | Seewald prototype's pattern: `seewald_addition/genizah_local_indexer.py` lines ~90-105. Extended per D-35 with `local_files` + `local_pages` sidecars. |
| REQ-6 THREE-STATE-FILTER | All / Only LOCAL / No LOCAL across 3 desktop surfaces | Pattern from `web/pages/search.py:1430-1444` [VERIFIED] — printed_filter button as precedent. Cascade discipline: filter applied AFTER existing filters; pinned by static AST test mirroring `tests/test_pgp_filter_cascade.py`. D-10 P1 fix: no-op when no LOCAL hits. |
| REQ-7 RESULT-BADGE | `LOCAL` badge in `COL_SRC` | `COL_SRC = 8` at `genizah_app.py:5909` [VERIFIED]. Write site at `:16534`. Visibility rule at `:16741`. Blue `#3498db` (analog of PGP's green `#27ae60` at `:16538`). |
| REQ-8 MY-LIBRARY-TAB | New `MyLibraryTab` as Nth tab in `QTabWidget` | `genizah_app.py:3079-3091` [VERIFIED] — **WARNING:** this is 6 tabs today (Search, Composition, Browse-by-Shelfmark, Browse-by-Identification, Personal Lists, Community). MyLibraryTab is the **7th** tab, not the 6th as SPEC says. |
| REQ-9 CLOUD-WRITE-GATES | 3 hard-rejects + regression tests | `corrections_client.py:619-623` [VERIFIED] — extend with `is_local_sys_id` OR. `lists_sync.py:736-756` [VERIFIED] — Codex P0 confirmed: gate MUST move to TOP before `_get_client()` (line 742) and `sync_list_to_cloud()` (line 753). `shared/search_serializer.py:_serialize_item` — add filter. |
| REQ-10 SCALE-CEILING | 5,000 files / 2 GB with warning dialog | Per D-26/D-41: count files + bytes via `os.walk(followlinks=False)`. Trigger on EITHER count > 5000 OR bytes > 2 * 1024³. |

## Architecture Patterns

### System Architecture Diagram

```
                                  ┌──────────────────────────────┐
                                  │   MyLibraryTab (7th tab)     │
                                  │  - Folder picker (multi)     │
                                  │  - Refresh / Cancel buttons  │
                                  │  - Per-file status table     │
                                  └──────────────┬───────────────┘
                                                 │ QThread (worker)
                                                 │ + QMutex (D-25)
                                                 ▼
                            ┌─────────────────────────────────────────────┐
                            │   shared/local_indexer.py (Qt-free)         │
                            │                                              │
                            │   1. enumerate(folders)  ──► os.walk        │
                            │      (D-26 pre-scan: count files + bytes)   │
                            │                                              │
                            │   2. for each file:                          │
                            │      ├─ _canonical_filepath (D-42)           │
                            │      ├─ sys_id = is_local_sys_id format     │
                            │      ├─ mtime/size check → cache hit? skip  │
                            │      └─ extract:                             │
                            │         .docx → python-docx (D-04 20-para)  │
                            │         .pdf  → fitz.get_text("blocks")    │
                            │                 (per-page, D-03)             │
                            │         .txt  → open(encoding='utf-8-sig') │
                            │                                              │
                            │   3. two-phase commit (D-21):                │
                            │      ├─ SQLite INSERT status='pending'      │
                            │      ├─ Tantivy writer.add_document         │
                            │      ├─ writer.commit() every 25 files      │
                            │      └─ SQLite UPDATE status='committed'    │
                            └────────────┬───────────────┬───────────────┬┘
                                         │               │               │
                                         ▼               ▼               ▼
                ┌──────────────────────────┐  ┌────────────────────┐  ┌─────────────┐
                │ Config.LOCAL_INDEX_DIR   │  │  Config.LOCAL_LAB_ │  │ local_index.│
                │ Tantivy side-index       │  │  INDEX_DIR         │  │  sqlite3    │
                │ (main schema +           │  │  Tantivy LAB       │  │  (folders,  │
                │  unique_id raw tok)      │  │  side-index        │  │  processed_ │
                │                          │  │  + .meta.json      │  │  files,     │
                │                          │  │  (weights_hash)    │  │  local_     │
                │                          │  │  D-38              │  │  files,     │
                │                          │  │                    │  │  local_     │
                │                          │  │                    │  │  pages)     │
                │                          │  │                    │  │  D-35       │
                └────────┬─────────────────┘  └─────┬──────────────┘  └─────────────┘
                         │                          │
              ┌──────────┴──────────┐    ┌──────────┴──────────┐
              ▼                     ▼    ▼                     ▼
  ┌─────────────────────┐  ┌────────────────────────────────────────────┐
  │ Main search merger  │  │ lab_composition_search() / parallels       │
  │ (genizah_core.py)   │  │ extension — custom fingerprint scoring     │
  │                     │  │ (NOT BM25)                                 │
  │ 1. main_searcher    │  │                                             │
  │    .search → list A │  │ Same merge shape, different score key.      │
  │ 2. local_searcher   │  │                                             │
  │    .search → list B │  │ D-38 weights_hash check before query;       │
  │ 3. AFTER            │  │ if stale → banner + Rebuild button.          │
  │    _deduplicate(A): │  │                                             │
  │ 4. RRF k=60 fusion  │  │                                             │
  │ 5. tie-break:       │  │                                             │
  │    Genizah first    │  │                                             │
  └─────────┬───────────┘  └─────────────────────┬───────────────────────┘
            │                                    │
            └────────────┬───────────────────────┘
                         ▼
         ┌──────────────────────────────────────────────┐
         │   Three desktop result tables:               │
         │   - Search (genizah_app.py:5914+)             │
         │   - Composition Search                        │
         │   - Parallels                                 │
         │                                               │
         │   COL_SRC cell = "LOCAL" (blue #3498db)       │
         │   Three-state filter button per surface      │
         │   D-39 per-surface QSettings keys             │
         └──────────────────────┬───────────────────────┘
                                │ click LOCAL hit
                                ▼
         ┌──────────────────────────────────────────────┐
         │   Browse panel (text-only mode per D-27)     │
         │   - Reuses existing "no image" code path     │
         │   - browse_map[sys_id] = D-34 entries        │
         │   - [Open file] button → os.startfile() (Win)│
         └──────────────────────────────────────────────┘

  ╔══════════════════════════════════════════════════════════════════════╗
  ║  CLOUD-WRITE GATES (REQ-9) — three hard rejects, fail BEFORE network ║
  ║                                                                      ║
  ║  1. corrections_client.py:619+  — extends existing                   ║
  ║     `is_synthetic_sys_id(document_id)` gate with `is_local_sys_id`   ║
  ║                                                                      ║
  ║  2. lists_sync.py:736+ — gate moves to TOP of                       ║
  ║     `sync_item_to_cloud()` and `sync_list_to_cloud()` BEFORE         ║
  ║     `_get_client()`. (Codex P0 — D-30 revised.)                      ║
  ║                                                                      ║
  ║  3. shared/search_serializer.py:_serialize_item — filter LOCAL       ║
  ║     items (defense-in-depth; web Tantivy has no LOCAL anyway)       ║
  ╚══════════════════════════════════════════════════════════════════════╝
```

### Recommended Project Structure

```
shared/
├── local_indexer.py          # NEW — Qt-free indexer (extraction + Tantivy writer + SQLite cache)
├── local_sys_id.py           # NEW — is_local_sys_id, sys_id generation, machine_id derivation
├── synthetic_sys_id.py       # EXISTING — pattern template
└── ...

desktop/                       # (may not exist as a package yet — planner verifies)
├── my_library_tab.py         # NEW — MyLibraryTab Qt widget
└── ...

tests/
├── fixtures/local_indexer/
│   ├── hebrew_sample.pdf     # NEW per D-44 — multi-column Hebrew PDF
│   ├── hebrew_sample.expected.txt
│   ├── mirror_reversed.pdf   # D-02 dead-code helper fixture
│   ├── single_word_per_line.pdf  # D-02 dead-code helper fixture
│   ├── unsupported.html
│   ├── sample.docx
│   └── sample.txt
├── test_local_sys_id_namespace.py
├── test_local_sys_id_parser_compat.py
├── test_local_indexer.py
├── test_local_indexer_incremental.py
├── test_local_indexer_scale.py        # @pytest.mark.slow
├── test_local_indexer_mutex.py
├── test_side_index_merge.py
├── test_local_post_dedup_merge.py
├── test_local_filter_cascade.py
├── test_local_filter_persistence.py
├── test_local_delete_by_uid.py
├── test_local_two_phase_commit.py
├── test_local_namespace_no_api_leak.py
├── test_local_namespace_no_lists_leak.py
├── test_local_namespace_no_corrections_leak.py
├── test_local_index_open_fallback.py
├── test_local_lab_invalidation.py
├── test_local_unavailable_folder.py
├── test_canonical_filepath.py
├── test_folder_overlap_detection.py
├── test_export_dossier_local_handling.py
├── test_web_library_options_no_local.py
└── test_local_schema_evolution.py

CompileScriptGenizah.iss      # EXISTING — no change needed if .spec collects PyMuPDF
GenizahSearchPro.spec         # MODIFY — add fitz/pymupdf hidden imports + collect_all
requirements.txt              # ADD pymupdf, python-docx pins
```

### Pattern 1: Reciprocal Rank Fusion (RRF) Merger

**What:** Combine two independent ranked lists into one without normalizing BM25 scores.

**When to use:** Any time you fuse hits from two heterogeneous Tantivy indexes (the main Genizah index and the LOCAL side-index — different corpora, different IDFs).

**Example (Python — write this in `genizah_core.py` near the existing search dispatch):**

```python
# Source: https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reciprocal-rank-fusion
# Source: https://opensearch.org/blog/introducing-reciprocal-rank-fusion-hybrid-search/

def _rrf_merge(genizah_hits, local_hits, k=60, limit=None):
    """Reciprocal Rank Fusion of two ranked hit lists.

    BM25 scores from independent Tantivy indexes are NOT comparable (IDF is
    index-local). RRF normalizes via rank position alone:

        rrf_score(doc) = sum over sources of: 1 / (k + rank_in_source)

    k=60 is the literature default (Cormack/Clarke + Elasticsearch/OpenSearch).

    Tie-break per D-08: Genizah first when RRF scores tie.
    """
    rrf = {}  # uid -> {'hit': hit_dict, 'score': float, 'sources': set}

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

    # D-08 tie-break: Genizah first. Use a secondary key that is True when
    # 'genizah' is among the sources (sorted descending, so True > False).
    fused = sorted(
        rrf.values(),
        key=lambda r: (r['score'], 'genizah' in r['sources']),
        reverse=True,
    )

    out = [r['hit'] for r in fused]
    return out[:limit] if limit else out
```

### Pattern 2: Tantivy LOCAL Side-Index Schema (with raw `unique_id`)

**What:** A schema that mirrors the main index for content fields but uses `raw` tokenizer on `unique_id` so `delete_documents` works correctly.

**When to use:** When building `Config.LOCAL_INDEX_DIR`. The main index does NOT need this change because it's rebuilt from scratch (no incremental delete).

**Example:**

```python
# Source: tantivy-py docs (verified via help(SchemaBuilder.add_text_field))
# Source: https://github.com/quickwit-oss/tantivy-py/issues/297

def build_local_schema():
    builder = tantivy.SchemaBuilder()
    # CRITICAL: tokenizer_name="raw" so delete_documents("unique_id", uid) works.
    # The main index at genizah_core.py:5125 omits tokenizer_name, defaulting
    # to the tokenized "default" tokenizer — but it's rebuilt from scratch
    # every time so the bug is latent there. For LOCAL where incremental
    # delete IS the central operation, we MUST set raw.
    builder.add_text_field("unique_id", stored=True, tokenizer_name="raw")
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
    return builder.build()
```

Document this divergence in `RESEARCH.md → Open Questions` and in the plan.

### Pattern 3: Delete-by-UID via local_pages sidecar (D-20 + D-36)

**What:** When deleting a file (folder removal or modify-rescan), the indexer enumerates every page-level UID emitted for that file's sys_id and deletes by exact term.

**Example:**

```python
# Source: tantivy/examples/deleting_updating_documents.rs (cited by the Rust upstream)
# Source: https://github.com/quickwit-oss/tantivy-py/issues/297

def delete_file_from_local_index(conn, writer, sys_id):
    rows = conn.execute(
        "SELECT uid FROM local_pages WHERE sys_id = ?", (sys_id,)
    ).fetchall()
    for (uid,) in rows:
        # delete_documents takes (field_name, field_value) with a Term-like API
        writer.delete_documents("unique_id", uid)
    # NOTE: Tantivy buffers deletes until commit. The caller must invoke
    # writer.commit() at the batch boundary (D-21 two-phase protocol).
    # After commit, the IndexReader must be reloaded for the changes to
    # appear in searcher results (standard tantivy semantics).
    conn.execute("DELETE FROM local_pages WHERE sys_id = ?", (sys_id,))
    conn.execute("DELETE FROM local_files WHERE sys_id = ?", (sys_id,))
```

### Pattern 4: Two-Phase Commit (D-21)

**What:** Avoid the "Tantivy persisted but SQLite cache wasn't updated" failure mode (and vice versa).

**Example:**

```python
def commit_batch(conn, writer, batch_files):
    """Two-phase commit: SQLite pending → Tantivy commit → SQLite committed.

    On crash between phase 2 and phase 3: phase-3 row will be re-extracted
    on startup (idempotent because phase 1 re-creates pending rows for the
    in-progress batch).
    """
    # Phase 1: mark all batch rows pending in SQLite.
    with conn:
        for f in batch_files:
            conn.execute(
                "INSERT INTO processed_files (filepath, mtime, size, sys_id, status) "
                "VALUES (?, ?, ?, ?, 'pending') "
                "ON CONFLICT(filepath) DO UPDATE SET status='pending'",
                (f.path, f.mtime, f.size, f.sys_id),
            )
    # Phase 2: tantivy commit (durable on success).
    writer.commit()
    # Phase 3: flip SQLite to committed.
    with conn:
        for f in batch_files:
            conn.execute(
                "UPDATE processed_files SET status='committed' WHERE filepath = ?",
                (f.path,),
            )


def startup_recovery(conn, writer, extract_fn):
    """Re-extract any file left in pending state on startup."""
    rows = conn.execute(
        "SELECT filepath FROM processed_files WHERE status = 'pending'"
    ).fetchall()
    for (path,) in rows:
        # Idempotent: delete any LOCAL docs for this sys_id, then re-extract.
        sys_id = compute_sys_id_for_path(path)
        delete_file_from_local_index(conn, writer, sys_id)
        extract_fn(path)
    if rows:
        writer.commit()
```

### Anti-Patterns to Avoid

- **DON'T sort merged results by raw BM25 score.** IDF is per-index. Without RRF or score normalization, ordering will be unstable and biased toward whichever index has rarer matching terms. (Codex P0 — D-08.)
- **DON'T insert LOCAL hits before `_deduplicate()` at `genizah_core.py:7390`.** That function literally drops anything that isn't V0.8/V0.7. LOCAL would disappear silently. (Codex P0 — D-08.)
- **DON'T put the LOCAL gate "after the natural sys_id lookup" in `sync_item_to_cloud`.** That's where it looks tempting (line 762) but `_get_client()` at 742 + `sync_list_to_cloud()` at 753 have already fired. Gate moves to TOP. (Codex P0 — D-30.)
- **DON'T use `text_field` default tokenizer on `unique_id` in the LOCAL index.** Default tokenizer breaks delete-by-term [CITED: github.com/quickwit-oss/tantivy-py/issues/297]. Use `tokenizer_name="raw"`.
- **DON'T trust `socket.gethostname()` + `[:8]` → `int(..., 16)` → string** without modulo. `0xFFFFFFFF` = 10 digits, overflows the 8-digit slot. (Codex revision — D-19.)
- **DON'T use raw string-prefix overlap check on Windows.** Junctions, UNC, 8.3 short names, case-sensitivity bite. Use `Path.resolve().normcase()` + `commonpath`. (Codex revision — D-17.)
- **DON'T patch the shared `Transcriptions.txt` / `libraries.csv`** like the Seewald prototype does. They're READ-ONLY from this phase's perspective (SPEC constraint + UAC avoidance).
- **DON'T register `QFileSystemWatcher`** for live folder watching. Network drives, locked files, mid-write states — out of scope per SPEC.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Hebrew/Arabic bidi reordering | Custom RTL flip code (beyond the dead-code helpers ported from Seewald) | Trust PyMuPDF + accept the limitation for v1; add `python-bidi` only if a real fixture forces it | Bidi is genuinely hard (Unicode UAX #9); buggy implementations corrupt text silently |
| Multi-list rank fusion | Custom score normalization (min-max, z-score, softmax) | Reciprocal Rank Fusion (RRF, k=60) | RRF is parameter-light, stable, and the literature standard for fusing heterogeneous retrievers [CITED: opensearch.org] |
| Concurrent indexer scheduling | DIY thread synchronization | Single `QMutex` + queued-with-collapse refresh requests (D-25) | Tantivy + SQLite WAL both choke under concurrent writes |
| Path normalization on Windows | String slicing / `lower()` | `os.path.normcase(str(Path(p).resolve(strict=False)))` (D-42) | UNC paths, junctions, 8.3 short names, drive-letter casing |
| Atomic commit ordering | "commit and pray" | Two-phase commit with explicit pending/committed states (D-21) | Crash between Tantivy commit and SQLite update otherwise corrupts cache |
| PDF metadata extraction | Custom PDF parsing | `fitz.open(path).metadata['title']` (D-35 `display_title`) | PyMuPDF returns a dict with `title`, `author`, `subject`, `creator`, `producer`, `creationDate`, `modDate` [CITED: pymupdf.readthedocs.io] |
| DOCX metadata extraction | Custom XML parsing | `python-docx`: `doc.core_properties.title` | Built-in via `core_properties` attribute |
| TXT encoding detection | `chardet` (slow, unreliable per D-07) | `utf-8-sig` only, fall back to `cp1255` on `UnicodeDecodeError` if smoke test forces it | Hebrew TXT in the wild is overwhelmingly UTF-8 or BOM-prefixed UTF-8 |
| Tab registration | Custom QWidget orchestration | `self.tabs.addTab(MyLibraryTab(self), tr("My Library"))` after `:3091` | Existing pattern; CONTEXT'd as 6th tab but actually 7th — see Pitfall #4 |

**Key insight:** Every "I'll just write a quick regex / quick file walk / quick hash" temptation has a CONTEXT-mandated helper. The 46 decisions left almost no greenfield — the planner's job is wiring, not invention.

## Runtime State Inventory

> This phase introduces NEW persistent runtime state but doesn't rename / migrate existing state. The classic "renamed but missed something" risk is low.

| Category | Items Found | Action Required |
|----------|-------------|-----------------|
| Stored data | New: `Config.LOCAL_INDEX_DIR/local_index.sqlite3` (3 new tables: `folders`, `processed_files`, `local_pages`, `local_files`). New: Tantivy side-index files at `Config.LOCAL_INDEX_DIR` and `Config.LOCAL_LAB_INDEX_DIR`. New: `<LOCAL_LAB_INDEX_DIR>/.meta.json` with `weights_hash`. | Create schemas; ensure D-21 two-phase commit; ensure D-37 fallback when corrupt. |
| Live service config | None — no external services configured by this phase. | None — verified by inventorying SPEC + CONTEXT (web-Tantivy / Supabase / PostHog / NLI / Cambridge unchanged). |
| OS-registered state | None — feature is in-app only, no scheduled tasks, no Windows registry entries beyond optional QSettings UI prefs. `QSettings` already used elsewhere only for Qt-internal state. D-15 explicitly avoids using QSettings for folder list (uses SQLite). | None — verified by `grep -rn QSettings --include="*.py"` returning only `venv/.../PyInstaller/fake-modules/_pyi_rth_utils/qt.py` (no production usage). |
| Secrets / env vars | None — no new secret keys, no new env vars. The cloud-write gates ADD reject behavior; they don't read new secrets. | None — verified by inventorying SPEC + CONTEXT. |
| Build artifacts | NEW: `GenizahSearchPro.spec` must add `pymupdf`/`fitz` hidden imports + `collect_all('pymupdf')`. NEW: `CompileScriptGenizah.iss` may need to declare PyMuPDF binaries if not auto-bundled (planner verifies during build). | Update `GenizahSearchPro.spec` per D-43; smoke-test packaged EXE per `@pytest.mark.packaging` test. |

## Common Pitfalls

### Pitfall 1: PyMuPDF doesn't actually solve Hebrew RTL "at the source" (D-01 / D-02 / D-44 — REAL RISK)

**What goes wrong:** PyMuPDF returns RTL text in visual (backward) order for some PDFs, and the maintainers have closed RTL ligature bugs as `wontfix` [CITED: github.com/pymupdf/PyMuPDF/issues/2199]. The Hebrew word "שלום" might come back as "םולש" or with ligatures unrolled in wrong order.

**Why it happens:** PDF stores glyphs in draw order, not logical reading order. RTL languages need bidi post-processing (Unicode UAX #9). PyMuPDF's `get_text("blocks")` provides position info per block but doesn't run bidi.

**How to avoid:** Three options for the planner:
1. **Accept the risk for v1.** Most Hebrew PDFs from Word / Adobe Acrobat / LibreOffice ARE already authored with logical ordering and PyMuPDF returns them correctly. Pathological PDFs (some scans, old typesetting tools) will be wrong. Document this in Help (D-31).
2. **Add `python-bidi` as an optional post-processor.** Detect RTL-heavy pages and run bidi.algorithm.get_display(text, base_dir='R'). Adds ~1 MB to installer; trivial CPU cost.
3. **Wire the dead-code helpers as live runtime path for high-RTL-ratio pages.** Use `_rtl_ratio(text) > 0.5` to trigger `_fix_rtl_page` + `_join_fragmented_lines`. This contradicts D-02's "dead code in v1" decision but might be the safest path.

**Warning signs:** Real Hebrew fixture from D-44 produces gibberish OR text that reads correctly when reversed character-by-character.

**Recommendation:** Planner picks fixture in D-44 from a Word-authored Hebrew PDF (low-risk path) and explicitly documents the limitation. If the fixture comes back wrong, fall back to option 2 (`python-bidi`).

### Pitfall 2: Tantivy delete-by-term fails on tokenized `unique_id` field (P0 — schema divergence required)

**What goes wrong:** `writer.delete_documents("unique_id", "LOCAL_970012345601234567_P3")` on a default-tokenized field SILENTLY does nothing — no exception, no warning, the document remains [CITED: github.com/quickwit-oss/tantivy-py/issues/297]. Modified-file re-extraction then DOUBLES the page rows on every rescan.

**Why it happens:** The default tokenizer lowercases + splits on word boundaries. The token term used at index time is `"local"`, `"970012345601234567"`, `"p3"` (separately). Deleting by the full string `"LOCAL_970012345601234567_P3"` doesn't match any indexed term.

**How to avoid:** Set `tokenizer_name="raw"` on the LOCAL index's `unique_id` field. This is a deliberate, ONE-FIELD divergence from the main index schema. Pin via `tests/test_local_delete_by_uid.py` (the test in D-20) — it MUST exercise insert → delete → search and assert zero hits.

**Warning signs:** The test mentioned above is the canary. If REQ-5's "deleted file gets removed from index" acceptance criterion passes flakily or only on first run, this is the cause.

### Pitfall 3: `lists_sync.sync_item_to_cloud` lookup ordering leaks (P0 — Codex verified)

**What goes wrong:** Inserting the LOCAL gate at the "natural" position (after `item_data = self.lists_manager.data.get('items', {}).get(item_id)` at `lists_sync.py:758` [VERIFIED]) leaks: `_get_client()` already ran at `:742`, `sync_list_to_cloud(list_id)` already ran at `:753`. Both touch Supabase.

**Why it happens:** The original code reads `item_data.sys_id` LATE because it only needs the sys_id for the payload, not for early-out gating.

**How to avoid:** Move the gate to the FIRST statement of the function. Lookup `item_data` from `self.lists_manager.data` (purely in-memory) FIRST, extract sys_id, check `is_local_sys_id`, return False if positive — all BEFORE touching `_get_client()` or any other function. Same fix at top of `sync_list_to_cloud()` (per D-30).

**Warning signs:** `tests/test_local_namespace_no_lists_leak.py` mocks `_get_client` and asserts ZERO calls. If the mock is called even once with a LOCAL sys_id, the gate is in the wrong place.

### Pitfall 4: MyLibraryTab is the 7th tab, not the 6th (cosmetic — SPEC drift)

**What goes wrong:** SPEC REQ-8 says "6th tab via `self.tabs.addTab(MyLibraryTab(self), 'My Library')`". The actual codebase at `genizah_app.py:3079-3091` has SIX tabs today: Search, Composition Search, Browse by Shelfmark, Browse by Identification, Personal Lists, Community.

**Why it happens:** SPEC was written against a slightly older codebase or memory of the layout.

**How to avoid:** Plan-checker should accept MyLibraryTab being the 7th tab. The acceptance test should NOT pin a tab index (use `findChild` / search by text).

**Warning signs:** Verification test pinning `self.tabs.count() == 6` after insert — change to `>= 7`.

### Pitfall 5: PyInstaller does NOT auto-discover `fitz` from `import pymupdf` (D-43 verified)

**What goes wrong:** Build a fresh `dist/GenizahSearch.exe` after merging Phase 95, run it, click the My Library tab, click Refresh on a folder with PDFs — the indexer worker raises `ModuleNotFoundError: No module named 'fitz._fitz'` (the compiled C extension) or `ImportError: DLL load failed`.

**Why it happens:** PyMuPDF ships a C extension (`fitz._fitz` / `pymupdf._extra`) with binary blobs that PyInstaller's static-analysis import graph misses. The `requirements.txt` pin gets it onto the dev machine but the .spec governs what ships.

**How to avoid:** In `GenizahSearchPro.spec`:
```python
hiddenimports = ['tantivy', 'numpy', 'PIL', 'fitz', 'pymupdf']
tmp_ret = collect_all('pymupdf')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
```

Also add the stdout/stderr None-check shim at app startup [CITED: github.com/pymupdf/PyMuPDF/discussions/3467]:
```python
import sys, os
if sys.stdout is None: sys.stdout = open(os.devnull, "w")
if sys.stderr is None: sys.stderr = open(os.devnull, "w")
```
(`--noconsole` Windows builds otherwise crash inside PyMuPDF when it tries to flush.)

**Warning signs:** Dev machine works, packaged EXE fails. The `@pytest.mark.packaging` test in D-43 catches this.

### Pitfall 6: SQLite WAL + Windows file locks on the side-index dir

**What goes wrong:** A crashed previous indexer leaves `local_index.sqlite3-wal` + `-shm` files behind. Next launch opens the DB but Tantivy can't open `Config.LOCAL_INDEX_DIR` because Windows still has a file handle on it.

**Why it happens:** Tantivy uses memory-mapped files. If the previous process didn't close cleanly, Windows may keep the mmap alive briefly. SQLite's WAL recovers automatically but Tantivy needs the directory to be unlocked.

**How to avoid:** D-37 fallback path catches this: if `tantivy.Index.open(Config.LOCAL_INDEX_DIR)` raises, log + fall back to Genizah-only mode + surface a "My Library index unavailable — Rebuild?" banner.

**Warning signs:** Indexer logs `PermissionError` or `OSError` on Tantivy open at second launch after a hard kill.

### Pitfall 7: LAB weights drift silently invalidates LOCAL LAB index (D-09 / D-38)

**What goes wrong:** User runs MyLibraryTab Refresh → LOCAL LAB index is built with `fingerprint_dyn` values computed from CURRENT LAB weights. User then opens LAB settings, tweaks weights, hits "Rebuild LAB Index" (rebuilds main LAB). Composition Search now mixes a LOCAL LAB index built with OLD weights against the main LAB index built with NEW weights — fingerprints don't agree, scores are nonsense.

**Why it happens:** Fingerprint encoding is weight-dependent. There's no way to invalidate a lab index "in-place" — you have to rebuild.

**How to avoid:** D-38's `.meta.json` invalidation contract. At LOCAL LAB build time, write `weights_hash = sha256(json.dumps(lab_weights, sort_keys=True))` + `lab_schema_version`. At every Composition/Parallels query, compare stored hash vs current — mismatched → surface banner + Rebuild button. Auto-rebuild triggers: (a) Refresh, (b) hash mismatch detected at query time, (c) main LAB rebuild via Tools menu.

**Warning signs:** User reports "Composition Search results are wrong since I tweaked the weights." Test: `tests/test_local_lab_invalidation.py`.

### Pitfall 8: `app.storage.user` invariant is desktop-irrelevant — but `web/` still must not regress (Phase 87)

**What goes wrong:** Phase 87's hard rule (`tests/test_no_raw_storage_access.py`, allowlist `[]`) forbids raw `app.storage.user` reads under `web/`. If any LOCAL-related code in `shared/` or any new `web/` consumer of `LIBRARY_CODES` accidentally reaches for `app.storage.user`, CI fails.

**Why it happens:** Phase 95 is desktop-only but the helper `is_local_sys_id` and the three cloud-write gates touch shared/web code. The export-dossier handling (D-45) could regress if a row-builder reaches for state.

**How to avoid:** Use `web/safe_storage.py` chokepoint exclusively for any per-user state in `web/`. New `tests/test_web_library_options_no_local.py` (D-46) is a static AST guard mirroring the cascade pattern.

**Warning signs:** `tests/test_no_raw_storage_access.py` failing with new allowlist entries proposed.

## Code Examples

Verified patterns from official sources + this codebase.

### LOCAL sys_id helper (mirroring `shared/synthetic_sys_id.py`)

```python
# Source: shared/synthetic_sys_id.py (template) + CONTEXT D-19 Codex revision
# File: shared/local_sys_id.py (NEW)
"""LOCAL sys_id helpers (Phase 95).

The 18-digit format ``97 + machine_id (8 digits) + content_hash (8 digits)``
is the only LOCAL sys_id contract. All other code MUST consult this helper.
"""
from __future__ import annotations
import hashlib
import os
import socket
from pathlib import Path

_LOCAL_PREFIX = "97"
_MACHINE_PAD = 8
_HASH_PAD = 8
_TOTAL_LENGTH = 2 + _MACHINE_PAD + _HASH_PAD  # 18


def _canonical_filepath(p: str | Path) -> str:
    """Canonical form for sys_id generation and folder-overlap detection (D-42).

    Resolves symlinks/junctions, normalizes case, normalizes separators.
    """
    return os.path.normcase(str(Path(p).resolve(strict=False)))


def _machine_id() -> str:
    """8-digit decimal machine_id from hostname SHA256.

    Per D-19 Codex revision: explicit modulo 10**8 guarantees width.
    """
    h = hashlib.sha256(socket.gethostname().encode()).hexdigest()[:8]
    return f"{int(h, 16) % 10**8:08d}"


def _content_hash(canonical_filepath: str, slot: int = 0) -> str:
    """8-digit content_hash from filepath SHA256.

    slot=0 uses chars 0-8; on collision, slot=1 uses 8-16, etc. (D-19 retry).
    """
    h = hashlib.sha256(canonical_filepath.encode()).hexdigest()
    chunk = h[slot * 8 : slot * 8 + 8]
    return f"{int(chunk, 16) % 10**8:08d}"


def generate_local_sys_id(filepath: str | Path, slot: int = 0) -> str:
    """Generate an 18-digit LOCAL sys_id.

    On SQLite UNIQUE collision the caller retries with slot += 1 up to 4 times.
    """
    canon = _canonical_filepath(filepath)
    return f"{_LOCAL_PREFIX}{_machine_id()}{_content_hash(canon, slot)}"


def is_local_sys_id(s: object) -> bool:
    """Return True iff ``s`` represents a Phase-95 LOCAL sys_id.

    Stable under digit-normalization. Any input with non-digit characters
    returns False.

    >>> is_local_sys_id("970012345601234567")
    True
    >>> is_local_sys_id("990025143260205171")  # real Alma
    False
    >>> is_local_sys_id("")
    False
    >>> is_local_sys_id(None)
    False
    """
    if not s:
        return False
    s = str(s)
    if not s.isdigit():
        return False
    if len(s) != _TOTAL_LENGTH:
        return False
    return s.startswith(_LOCAL_PREFIX)
```

### Cloud-write gate at TOP of `lists_sync.sync_item_to_cloud` (D-30 Codex P0)

```python
# Source: lists_sync.py:736+ (extend, MOVE LOCAL gate to TOP)
# Codex P0 verified 2026-05-21: gate MUST run before _get_client() and sync_list_to_cloud()

from shared.local_sys_id import is_local_sys_id

def sync_item_to_cloud(self, item_id: str, list_id: str) -> bool:
    """Push a specific item to cloud."""
    # ===== Phase 95 LOCAL gate (D-30, REQ-9) =====
    # MUST run before _get_client() / sync_list_to_cloud() — those leak
    # cloud activity even before sys_id is normally read.
    item_data = self.lists_manager.data.get('items', {}).get(item_id)
    if item_data:
        sys_id = item_data.get('sys_id', item_id)
        if is_local_sys_id(sys_id):
            logger.info("[local-only item, not synced] %s", sys_id)
            return False
    # ============================================

    if not self.is_sync_available():
        return False
    try:
        client = self._get_client()  # line 742 — gate above must have returned already
        if not client:
            return False
        # ... existing code unchanged ...
```

### PyMuPDF Hebrew extraction with metadata (D-35 `display_title`)

```python
# Source: pymupdf.readthedocs.io/en/latest/tutorial.html
# Source: CONTEXT D-35 + D-44

import fitz

def extract_pdf_pages(filepath):
    """Yield (page_num, text) tuples; uses D-03 one-doc-per-page model."""
    doc = fitz.open(filepath)
    try:
        # D-35 display_title fallback chain: doc.metadata['title'] → filename
        title = (doc.metadata or {}).get('title') or os.path.basename(filepath)
        for page_num, page in enumerate(doc, start=1):
            blocks = page.get_text("blocks")
            # Filter to text blocks (block_type == 0 means text; 1 means image)
            text_parts = [b[4].strip() for b in blocks if b[6] == 0 and b[4].strip()]
            text = "\n\n".join(text_parts)
            # NOTE: For v1 per D-02, the RTL helpers are NOT invoked here.
            # If D-44 fixture testing surfaces RTL breakage, planner adds
            # `text = _fix_rtl_page(text); text = _join_fragmented_lines(text)`
            # OR adopts python-bidi.
            yield page_num, text, title
    finally:
        doc.close()
```

### python-docx 20-paragraph chunking (D-04)

```python
# Source: python-docx 1.2.0 API
# Source: CONTEXT D-04

import docx

DOCX_PARAS_PER_CHUNK = 20  # D-04 constant

def extract_docx_pages(filepath):
    """Yield (chunk_num, text, title) tuples using fixed 20-para windows.

    python-docx's contains_page_break heuristic catches only explicit page
    breaks — D-04 ignores it in favor of a deterministic paragraph window.
    """
    doc = docx.Document(filepath)
    title = (doc.core_properties.title or os.path.basename(filepath))
    paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]

    chunks = [paragraphs[i:i + DOCX_PARAS_PER_CHUNK]
              for i in range(0, len(paragraphs), DOCX_PARAS_PER_CHUNK)]
    for chunk_num, chunk in enumerate(chunks, start=1):
        text = "\n".join(chunk)
        if len(text.strip()) >= 10:  # D-06 empty-page threshold
            yield chunk_num, text, title
```

### TXT extraction with utf-8-sig (D-07 start)

```python
# Source: CONTEXT D-07 — start with utf-8-sig only, planner decides fallback

def extract_txt(filepath):
    """Return [(1, full_text, title)] — TXT is single-page by convention."""
    title = os.path.basename(filepath)
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            text = f.read()
    except UnicodeDecodeError as e:
        # D-07 follow-up: planner decides whether to add cp1255 fallback.
        # If smoke testing shows real-world cp1255 files, uncomment:
        # with open(filepath, 'r', encoding='cp1255') as f:
        #     text = f.read()
        raise EncodingError(f"Could not decode {filepath}: {e}")
    return [(1, text, title)]
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| pdfplumber / pypdf for PDF | PyMuPDF (`fitz`) for Hebrew quality | Industry consensus ~2022+; PyMuPDF 1.24+ stable for Hebrew | Better RTL but NOT perfect — bidi remains caller's responsibility (Pitfall 1) |
| Raw BM25 sort across indexes | Reciprocal Rank Fusion (RRF, k=60) | Cormack/Clarke 2009; Elasticsearch 8.8 default 2023; OpenSearch 2.13 default 2024 | Stable, parameter-light fusion of heterogeneous retrievers |
| `import fitz` | `import pymupdf` (alias `fitz` still works) | PyMuPDF 1.24+ (2024) | Future-proofing; v1 uses `fitz` for parity with Seewald prototype |
| Single `Transcriptions.txt` + libraries.csv patching (Seewald prototype) | Separate Tantivy side-index, untouched shared corpus | Phase 95 | No UAC; no namespace collision; immutable shared corpus |
| `_deduplicate` whitelist (V0.8/V0.7) | Same function, LOCAL hits merge AFTER it | Phase 95 | Smaller blast radius than generalizing the dedup |

**Deprecated/outdated:**
- `chardet` for encoding detection: too slow, unreliable — not used.
- `QFileSystemWatcher` for live folder watch: out of scope per SPEC.
- Custom RTL flipping inside the runtime path: ported as dead code per D-02 (revisit if D-44 fixture fails).

## Project Constraints (from CLAUDE.md)

| Constraint | Source | Phase 95 compliance |
|------------|--------|----------------------|
| Dual app — both web AND desktop maintained | CLAUDE.md "Both apps must be maintained" | LOCAL feature is desktop-only per D-29 + SPEC out-of-scope; web is RECEIVING side (cloud-write gates only). |
| Shared service layer (Option C) | CLAUDE.md "Shared service layer" | All non-Qt logic in `shared/local_indexer.py` + `shared/local_sys_id.py`; desktop tab is a thin Qt shell. |
| Phase 87 multitenant invariant | `tests/test_no_raw_storage_access.py` allowlist `[]` | No new `app.storage.user` raw reads. All web-side state (none in Phase 95) routes through `safe_storage.py`. |
| Hebrew RTL throughout | CLAUDE.md "Hebrew RTL — many strings in Hebrew" | All LOCAL UI labels bilingual (EN + HE) per D-10, D-31, D-32. |
| `documents` table uses `pgpid` PK; `document_fragments.document_id` | CLAUDE.md schema notes | Not touched by Phase 95 (cloud-write gates only reject LOCAL — they don't write). |
| Version bump via `scripts/bump_version.py X.Y.Z` for releases | CLAUDE.md "Version Bumping (REQUIRED for releases)" | Phase 95 ships v7.14.0; bump script runs as part of release plan (not Phase 95 itself). |
| `docs/OPEN_ISSUES.md` maintenance | CLAUDE.md "Open Issues Tracker (REQUIRED)" | Planner adds entries for any Phase 95 issues found during smoke; closes them on fix. |
| `python scripts/check_docs.py` before commit | CLAUDE.md "Before Finishing a Session" | Per-plan execution checklist must include this. |
| Avoid forbidden terms: "FastAPI", "backend server", "DATABASE_URL", "port 8000" | CLAUDE.md "Outdated Terms to Avoid" | Confirmed: Phase 95 uses Tantivy + SQLite locally; Supabase REJECTS LOCAL; no port 8000. |

## Environment Availability

> Per Step 2.6 audit — Phase 95 has external dependencies (PyMuPDF, python-docx) but no external services beyond what already exists (Supabase REJECTS, not adds).

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| Python 3.10+ | All | ✓ | 3.11 | — |
| `tantivy` (tantivy-py) | LOCAL Tantivy side-index | ✓ | 0.25.1 [VERIFIED: pip] | — |
| `pymupdf` (`fitz`) | PDF extraction (D-01) | ✓ on dev (just installed); ✗ in built EXE | 1.27.2.3 (latest as of 2026-05-21) | None — packaging fix per D-43 is mandatory |
| `python-docx` | DOCX extraction (D-04) | ✓ | 1.2.0 [VERIFIED: pip] | None — but pure Python; PyInstaller auto-discovers |
| SQLite WAL | `local_index.sqlite3` cache | ✓ | stdlib | — |
| `socket.gethostname()` | machine_id derivation (D-19) | ✓ | stdlib | — |
| Windows `os.startfile` | "Open file" button (D-28) | ✓ on Windows; ✗ on macOS/Linux | Win32 API | Cross-platform not in scope (desktop is Windows-first per packaging) |
| PyInstaller hidden-import + `collect_all('pymupdf')` | Build artifact (D-43) | ✗ NOT in current `GenizahSearchPro.spec` [VERIFIED: read 2026-05-21] | n/a | None — planner MUST update spec |

**Missing dependencies with no fallback:**
- `pymupdf` in the packaged EXE — planner MUST update `GenizahSearchPro.spec` per D-43, and the `@pytest.mark.packaging` smoke test catches regressions.

**Missing dependencies with fallback:**
- None applicable.

## Validation Architecture

> Per `.planning/config.json`: `workflow.nyquist_validation: true` — full section included.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing — 125 test files in `tests/`) |
| Config file | `pytest.ini` / `pyproject.toml` (planner verifies during Wave 0) |
| Quick run command | `pytest tests/test_local_sys_id_namespace.py tests/test_local_indexer.py -x` |
| Full suite command | `pytest tests/ -x --tb=short` (excludes `@pytest.mark.slow` and `@pytest.mark.packaging` by default) |
| Slow-test command | `pytest tests/ -m slow` (Phase 95: 5000-file scale test per REQ-10) |
| Packaging-test command | `pytest tests/ -m packaging` (Phase 95: `@pytest.mark.packaging` smoke against `dist/` EXE per D-43) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|--------------|
| REQ-1 LOCAL-NAMESPACE | `is_local_sys_id` recognizes `97`-prefix 18-digit; returns False for every libraries.csv row; disjoint from `is_synthetic_sys_id` | unit | `pytest tests/test_local_sys_id_namespace.py -x` | ❌ Wave 0 |
| REQ-1 (parser compat) | `parse_header_smart`/`parse_full_id_components` accept `97`-prefix per D-13 fix | unit | `pytest tests/test_local_sys_id_parser_compat.py -x` | ❌ Wave 0 |
| REQ-2 SIDE-INDEX | Index 10 files; phrase search returns hits from both Tantivy indexes; corrupting LOCAL doesn't corrupt main | integration | `pytest tests/test_side_index_merge.py -x` | ❌ Wave 0 |
| REQ-2 (dedupe order) | LOCAL hit added before `_deduplicate()` is dropped; same hit added after survives | unit | `pytest tests/test_local_post_dedup_merge.py -x` | ❌ Wave 0 |
| REQ-3 FILE-TYPES | .docx/.pdf/.txt produce pages; .html yields unsupported_extension | unit | `pytest tests/test_local_indexer.py::test_supported_file_types -x` | ❌ Wave 0 |
| REQ-4 RTL-EXTRACTION (helpers) | `_fix_rtl_line` / `_fix_rtl_page` / `_join_fragmented_lines` correct mirror-reversed + single-word fixtures | unit | `pytest tests/test_local_indexer.py::test_rtl_helpers -x` | ❌ Wave 0 |
| REQ-4 RTL (PyMuPDF runtime) | Real Hebrew PDF fixture extracts in correct reading order via `get_text("blocks")` | unit | `pytest tests/test_local_indexer.py::test_pymupdf_hebrew_extraction_quality -x` | ❌ Wave 0 |
| REQ-5 INCREMENTAL-REINDEX | Second scan ≤ 5% wall time of first; modify→reextract only modified; delete→remove rows | integration | `pytest tests/test_local_indexer_incremental.py -x` | ❌ Wave 0 |
| REQ-5 (delete-by-uid) | delete_documents("unique_id", uid) actually removes the doc | unit | `pytest tests/test_local_delete_by_uid.py -x` | ❌ Wave 0 |
| REQ-5 (two-phase commit) | Fault injection between commit + UPDATE; recovery re-extracts pending | unit | `pytest tests/test_local_two_phase_commit.py -x` | ❌ Wave 0 |
| REQ-6 THREE-STATE-FILTER (cascade) | LOCAL filter applied AFTER PGP filter in cascade | unit (static AST) | `pytest tests/test_local_filter_cascade.py -x` | ❌ Wave 0 |
| REQ-6 (no-op when no LOCAL) | Persisted Only-Local state with zero LOCAL hits → no-op, chip surfaces | unit | `pytest tests/test_local_filter_cascade.py::test_no_op_when_no_local_hits -x` | ❌ Wave 0 |
| REQ-6 (persistence) | Per-surface QSettings keys persist across sessions | unit | `pytest tests/test_local_filter_persistence.py -x` | ❌ Wave 0 |
| REQ-7 RESULT-BADGE | LOCAL row shows `LOCAL` in COL_SRC with blue color | snapshot/unit | `pytest tests/test_local_result_badge.py -x` | ❌ Wave 0 |
| REQ-8 MY-LIBRARY-TAB | Tab registered, folder picker opens, scan runs, status table populates | manual smoke + minimal unit | `pytest tests/test_my_library_tab_construction.py -x` | ❌ Wave 0 |
| REQ-8 (mutex) | Concurrent Refresh+Remove requests serialize via QMutex | unit | `pytest tests/test_local_indexer_mutex.py -x` | ❌ Wave 0 |
| REQ-9 CLOUD-WRITE-GATES (api) | Serializer drops LOCAL row from /api/search payload | unit | `pytest tests/test_local_namespace_no_api_leak.py -x` | ❌ Wave 0 |
| REQ-9 (lists) | sync_item_to_cloud with LOCAL sys_id makes ZERO Supabase calls; gate at TOP | unit | `pytest tests/test_local_namespace_no_lists_leak.py -x` | ❌ Wave 0 |
| REQ-9 (corrections) | Corrections submit with LOCAL document_id returns local_corrections_disabled without HTTP | unit | `pytest tests/test_local_namespace_no_corrections_leak.py -x` | ❌ Wave 0 |
| REQ-10 SCALE-CEILING | 5000 1-KB .txt fixtures index in ≤ 10 min; peak RSS < 500 MB; UI thread accepts processEvents within 100 ms | scale (slow) | `pytest tests/test_local_indexer_scale.py -m slow` | ❌ Wave 0 |
| D-26 pre-scan | Warning dialog triggers at file_count > 5000 OR total_bytes > 2 GB | unit | `pytest tests/test_local_indexer.py::test_above_ceiling_warning -x` | ❌ Wave 0 |
| D-17/D-42 path normalization | Junction, UNC, mixed-case overlap detection | unit | `pytest tests/test_folder_overlap_detection.py tests/test_canonical_filepath.py -x` | ❌ Wave 0 |
| D-37 fallback | Mocked tantivy.Index.open raises; main search returns Genizah-only without traceback | unit | `pytest tests/test_local_index_open_fallback.py -x` | ❌ Wave 0 |
| D-38 LAB invalidation | weights_hash mismatch triggers banner; rebuild on Refresh / Tools menu / banner click | unit | `pytest tests/test_local_lab_invalidation.py -x` | ❌ Wave 0 |
| D-40 unavailable folder | folder.path missing at startup → status='unavailable', no row purge | unit | `pytest tests/test_local_unavailable_folder.py -x` | ❌ Wave 0 |
| D-43 packaging | Packaged EXE imports fitz + extracts Hebrew PDF successfully | packaging (release CI only) | `pytest tests/ -m packaging` | ❌ Wave 0 |
| D-45 export dossier | LOCAL row in desktop xlsx, dropped from web xlsx via `skip_local` kwarg | unit | `pytest tests/test_export_dossier_local_handling.py -x` | ❌ Wave 0 |
| D-46 web LIBRARY_CODES guard | Static AST scan asserts every web library-dropdown builder filters LOCAL | unit (static AST) | `pytest tests/test_web_library_options_no_local.py -x` | ❌ Wave 0 |
| D-35 schema evolution | local_files / local_pages / processed_files columns match contract | unit | `pytest tests/test_local_schema_evolution.py -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `pytest tests/test_local_*.py -x` (subset of test_local_* matching the touched component, fast)
- **Per wave merge:** `pytest tests/ -x --tb=short` (full unit suite, excludes slow + packaging)
- **Phase gate:** `pytest tests/ -x --tb=short && pytest tests/ -m slow` (full suite green + slow tests pass) before `/gsd-verify-work`
- **Release gate:** Packaged EXE smoke: `pytest tests/ -m packaging` after building `dist/`

### Wave 0 Gaps

- [ ] `tests/fixtures/local_indexer/` — entire fixture directory (Hebrew PDF, mirror-reversed PDF, single-word-per-line PDF, .docx, .txt, .html unsupported, .expected.txt files)
- [ ] `tests/fixtures/local_indexer/hebrew_sample.pdf` + `.expected.txt` — REQUIRED per D-44; planner picks/creates
- [ ] `tests/conftest.py` — shared fixtures for temp Tantivy index, temp SQLite, fake QApplication
- [ ] `tests/test_local_*` files — 21 new test files listed above (see Phase Requirements → Test Map)
- [ ] Framework install: `pip install pymupdf` (already added to requirements during this research) — `pymupdf 1.27.2.3` installed 2026-05-21
- [ ] `GenizahSearchPro.spec` modification per D-43 — Wave 0 verification this builds cleanly

## Security Domain

> Per `.planning/config.json`: `workflow.security_enforcement` not explicitly set — treat as enabled.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | no | Phase 95 doesn't authenticate users (desktop app, no login); cloud-write gates REJECT, never authenticate |
| V3 Session Management | no | No sessions touched; QSettings is per-user already |
| V4 Access Control | yes | LOCAL data MUST NOT leak to cloud — three hard-reject gates at REQ-9 |
| V5 Input Validation | yes | Filepath input must be normalized (D-42 `_canonical_filepath`); folder overlap rejection (D-17) prevents path-injection-style ambiguity |
| V6 Cryptography | partial | SHA256 used for sys_id derivation (NOT for security — just a non-secret content hash); D-33 cleartext-on-disk disclosure required in Help (no encryption work) |
| V7 Error Handling | yes | Per-file status panel surfaces extraction errors without crashing the UI thread (REQ-8 + D-37 fallback) |
| V8 Data Protection | yes | Sensitive personal data (LOCAL file contents) is stored cleartext on disk → D-33 disclosure mandatory; OS-level disk encryption recommended in Help |
| V11 Business Logic | yes | Cloud-write gates ARE the business logic invariant; pinned by 3 regression tests (REQ-9 acceptance) |

### Known Threat Patterns for desktop-only LOCAL indexing

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| LOCAL sys_id leaks to cloud-write surface (`/api/search`, lists_sync, corrections) | Information disclosure | Three hard-reject gates at REQ-9; gate location MATTERS (D-30 P0 — gate at TOP of `sync_item_to_cloud` before `_get_client()`) |
| sys_id collision causes mis-attribution across users on shared machine | Tampering | `machine_id` derived from `socket.gethostname()` already differentiates; per-Windows-user `%LOCALAPPDATA%` paths separate indices anyway (SPEC out-of-scope #11) |
| LOCAL index file injection via crafted filename | Tampering | `_canonical_filepath` normalizes; SQLite parameterized queries; no filepath ever passed to shell |
| `os.startfile()` on D-28 — arbitrary executable invocation | Elevation of privilege | `os.startfile()` uses Windows file association — user controls what extensions are registered; we only call on already-indexed files (.docx/.pdf/.txt) |
| Cleartext Tantivy index on disk discloses LOCAL contents | Information disclosure | D-33 disclosure in Help; OS-level disk encryption (BitLocker / FileVault) is the user's responsibility; no in-app encryption in v1 (backlog) |
| Tantivy index file lock leaves stale state after crash | Denial of service | D-37 fallback: corrupt/locked LOCAL index falls back to Genizah-only mode with Rebuild banner |
| LOCAL LAB index becomes stale after main LAB rebuild → wrong Composition Search scores | Tampering (silent) | D-38 weights_hash invalidation contract + banner |
| Path traversal via overlapping folder registration | Tampering | D-17 + D-42: `Path.resolve()` + `os.path.commonpath` overlap detection |
| Sensitive data in `error_msg` field of `local_files` table | Information disclosure | `error_msg` should redact filepath secrets if any; in practice it stores library error messages (PyMuPDF/python-docx exceptions) — low risk |

## Assumptions Log

> Claims tagged `[ASSUMED]` that the planner / discuss-phase should triage. If empty, all claims were verified or cited.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `MyLibraryTab` will be the 7th tab in the desktop `QTabWidget` (SPEC says 6th, but `genizah_app.py:3079-3091` registers 6 tabs today) | Pitfall #4 + Phase Requirements REQ-8 | Low — cosmetic; the verifier test should not pin tab index |
| A2 | Tantivy `unique_id` field must use `tokenizer_name="raw"` on LOCAL index for delete-by-term to work (verified via tantivy-py issue #297 — applies to LOCAL but not main, where index is rebuilt) | Pitfall #2 + Pattern 2 | High — without this, modified-file reextract DOUBLES rows on every rescan |
| A3 | PyMuPDF `get_text("blocks")` may return Hebrew RTL in visual order on some PDFs (Adobe Acrobat scans in particular). v1 trusts D-02's "dead code" decision; D-44 fixture must use a Word-authored Hebrew PDF for the happy path | Pitfall #1 + REQ-4 row in Phase Requirements | Medium — if D-44 fixture surfaces breakage, planner may need to wire helpers as runtime path OR add `python-bidi` |
| A4 | TXT encoding policy starts at `utf-8-sig` only; CP1255 added only if local smoke testing surfaces real-world files (D-07 open decision) | Standard Stack + Code Examples (TXT) | Low — fallback can be added in the same plan slot if smoke fails |
| A5 | RRF k=60 is the right constant (Cormack/Clarke 2009 default; Elasticsearch + OpenSearch defaults). Not tuning specific to Genizah corpus characteristics | Pattern 1 (RRF Merger) | Low — k=60 is robust across domains; tunable post-ship if needed |
| A6 | `python-docx` is auto-discovered by PyInstaller (pure Python, no C extension). Only PyMuPDF needs explicit collect_all | Environment Availability | Low — verified PyInstaller's behavior on pure-Python packages; the `@pytest.mark.packaging` smoke catches if wrong |
| A7 | `socket.gethostname()` returns the same string across Windows reboots on personal machines (the assumption underlying D-19 stable machine_id). Renames invalidate the cache per D-19 documented caveat | sys_id helper + D-19 quote | Low — Windows hostname is sticky unless user changes it deliberately |
| A8 | `os.startfile()` (D-28) is Windows-only. macOS/Linux fallback (`subprocess.run(['open', path])` / `xdg-open`) is out of scope because desktop builds are Windows-only today | Environment Availability + Pattern (Open file) | Low — out of scope; if user demand surfaces, simple platform-switch |

## Open Questions

1. **D-02 vs D-44 — Should the RTL helpers actually be DEAD code in v1?**
   - What we know: PyMuPDF doesn't bidi-correct RTL universally (Pitfall #1).
   - What's unclear: Whether the D-44 Hebrew fixture will pass without helpers. If fixture FAILS, the helpers must become runtime code on high-RTL-ratio pages.
   - Recommendation: Planner picks D-44 fixture from a Word-authored Hebrew PDF for the happy path. If a real-world Hebrew PDF fails post-ship, ADD `python-bidi` or wire the helpers conditionally in a follow-up phase. Document this in plan as "v1 assumes Word/LibreOffice-authored Hebrew PDFs; pathological PDFs may need follow-up fix."

2. **D-12 — Composition Search and Parallels result tables — do they already have a Src equivalent column?**
   - What we know: D-12 says "audit during plan: planner inspects existing layouts."
   - What's unclear: Without grepping the composition/parallels render code, we don't know the column layout.
   - Recommendation: Planner runs `grep -n "COL_" genizah_app.py | grep -i "comp\|parallel"` early in plan creation; if a Src equivalent exists, reuse; if not, add uniformly.

3. **D-15 — Should the folder list in SQLite have a `status` column?**
   - What we know: D-15 schema says `folders(folder_id, path, added_at, last_scanned_at, status)`.
   - What's unclear: Status enum values. D-40 mentions `'unavailable'`; D-25 mentions general lifecycle. Need explicit enum: probably `{'active', 'unavailable', 'pending_delete', 'scan_error'}`.
   - Recommendation: Planner pins the enum in `95-NN-PLAN.md` with a CREATE TABLE statement.

4. **D-07 — TXT encoding fallback policy after smoke testing.**
   - What we know: Start with utf-8-sig only.
   - What's unclear: Whether real user TXT files include cp1255-encoded legacy Hebrew.
   - Recommendation: Run smoke against a 10-file TXT corpus chosen by user; record final policy in `95-NN-PLAN.md`.

5. **D-32 — Final Hebrew translation of Seewald attribution.**
   - What we know: Draft is `"תכונת הספרייה שלי בהשראת אב-טיפוס GenizahLocal של יהודה זיוואלד"`.
   - What's unclear: User confirmation of preferred wording (זיוואלד vs ציוואלד spelling; word order).
   - Recommendation: Planner ships the draft and lets user review during execute.

6. **D-44 — Selecting / creating the canonical Hebrew PDF fixture.**
   - What we know: Needs to be small, multi-column or single-column, with known reading order.
   - What's unclear: Whether to use a real Genizah-domain PDF (e.g., a Hebrew article from `seewald_addition/`'s docs) or a synthetic-typeset one.
   - Recommendation: Use a small Word-authored Hebrew document (controllable, deterministic). Avoid scanned-Hebrew-from-Acrobat (highest RTL-corruption risk).

## Sources

### Primary (HIGH confidence)

- `genizah_core.py:1723-1750` — `LIBRARY_CODES` table [VERIFIED: Read 2026-05-21]
- `genizah_core.py:2000-2018` — `Config.INDEX_DIR` etc [VERIFIED: Read 2026-05-21]
- `genizah_core.py:3640-3681` — `parse_header_smart`, `parse_full_id_components` [VERIFIED: Read 2026-05-21] — confirms Codex P0 D-13 (regex `99\d{8,}` only)
- `genizah_core.py:5124-5136` — Tantivy main-index schema [VERIFIED: Read 2026-05-21] — confirms `unique_id` has NO `tokenizer_name` (Pitfall #2)
- `genizah_core.py:7390, 7916-7921` — `_deduplicate` invocation + body [VERIFIED: Read 2026-05-21] — confirms Codex P0 D-08 (drops non-V0.8/V0.7)
- `genizah_core.py:742-790` — `rebuild_lab_index` LAB schema [VERIFIED: Read 2026-05-21]
- `genizah_app.py:3079-3091` — `QTabWidget.addTab` [VERIFIED: Read 2026-05-21] — confirms 6 tabs today (not 5 as SPEC implies)
- `genizah_app.py:5905-5946` — `COL_SRC` / `COL_PGP` columns [VERIFIED: Read 2026-05-21]
- `genizah_app.py:16524-16555, 16730-16745` — write site + visibility rule [VERIFIED: Read 2026-05-21]
- `lists_sync.py:736-770` — `sync_item_to_cloud` body [VERIFIED: Read 2026-05-21] — confirms Codex P0 D-30 (`_get_client` at 742, `sync_list_to_cloud` at 753, sys_id at 762)
- `corrections_client.py:610-640` — existing synthetic gate pattern [VERIFIED: Read 2026-05-21]
- `shared/synthetic_sys_id.py` — module template [VERIFIED: Read 2026-05-21]
- `shared/search_serializer.py:1-60` — serializer entry points [VERIFIED: Read 2026-05-21]
- `seewald_addition/genizah_make_index.py:60-181` — RTL helpers + extractor patterns [VERIFIED: Read 2026-05-21]
- `seewald_addition/genizah_local_indexer.py:1-120` — SQLite cache pattern [VERIFIED: Read 2026-05-21]
- `GenizahSearchPro.spec` — confirms NO pymupdf/fitz hidden imports today [VERIFIED: Read 2026-05-21]
- `web/pages/search.py:148-1463` — PGP / printed filter button precedent [VERIFIED: Read 2026-05-21]
- `tests/test_pgp_filter_cascade.py` — static AST guard template [VERIFIED: Read 2026-05-21]
- `pip show tantivy / python-docx` — version verification [VERIFIED: shell 2026-05-21]
- `python -c "import fitz; print(fitz.VersionBind)"` → 1.27.2.3 [VERIFIED: shell 2026-05-21]
- `tantivy.SchemaBuilder.add_text_field.__doc__` — `tokenizer_name='default'` default + `raw` option [VERIFIED: shell help() 2026-05-21]

### Secondary (MEDIUM confidence)

- [PyMuPDF documentation — Text recipes](https://pymupdf.readthedocs.io/en/latest/recipes-text.html) — `get_text("blocks")` semantics, `sort=True` parameter
- [PyMuPDF documentation — Tutorial](https://pymupdf.readthedocs.io/en/latest/tutorial.html) — `doc.metadata` dict shape
- [PyMuPDF Installation docs](https://pymupdf.readthedocs.io/en/latest/installation.html) — current version
- [Reciprocal Rank Fusion — Elasticsearch reference](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reciprocal-rank-fusion) — k=60 default + formula
- [Introducing reciprocal rank fusion for hybrid search — OpenSearch](https://opensearch.org/blog/introducing-reciprocal-rank-fusion-hybrid-search/) — k=60 default; rank-based fusion rationale
- [What is Reciprocal Rank Fusion — AI21](https://www.ai21.com/glossary/tech/what-is-reciprocal-rank-fusion-rrf/) — RRF concept overview
- [PyQt6 QThread cancellation patterns — PythonGUIs FAQ](https://www.pythonguis.com/faq/how-to-start-stop-or-pause-running-threads/) — cooperative cancel flag pattern; `requestInterruption()` / `isInterruptionRequested()` alternatives

### Tertiary (LOW confidence — needs validation)

- [PyMuPDF Issue #2199 — Arabic RTL ligatures](https://github.com/pymupdf/PyMuPDF/issues/2199) — `wontfix` label per WebFetch summary; the exact maintainer response wasn't quoted in WebFetch but the issue title + label are clear
- [PyMuPDF Discussion #3467 — PyInstaller stdout/stderr None](https://github.com/pymupdf/PyMuPDF/discussions/3467) — user solution for `--noconsole` builds; not officially endorsed by maintainers but well-attested
- [tantivy-py Issue #297 — delete_documents on tokenized fields](https://github.com/quickwit-oss/tantivy-py/issues/297) — labeled "documentation" / "help wanted"; unfixed but well-documented behavior. Confirms raw tokenizer is the workaround.
- [tantivy deleting_updating_documents.rs](https://github.com/quickwit-oss/tantivy/blob/main/examples/deleting_updating_documents.rs) — confirms STRING (`raw` tokenizer) for delete-safe IDs

## Metadata

**Confidence breakdown:**

- Standard stack: HIGH — all libraries verified via `pip show` + `python -c "import …"` on the dev machine 2026-05-21.
- Architecture (RRF + LAB + cloud gates): HIGH — Codex P0 findings re-verified against actual code (line numbers all hit the right anchors).
- Codebase anchor accuracy: HIGH — every CONTEXT line number cross-checked. Two drift points found: (a) 6 tabs today, not 5; (b) `unique_id` field uses default tokenizer (not raw).
- Pitfalls: MEDIUM-HIGH — Pitfalls #1 (PyMuPDF RTL imperfect), #2 (raw tokenizer required), #3 (lists_sync ordering), #5 (PyInstaller missing fitz) all verified or cited. Pitfalls #6 (Windows file lock), #7 (LAB invalidation), #8 (multitenant invariant) are inferred from project history + general knowledge.
- Validation Architecture: HIGH — every test maps to a verified codebase anchor; framework is the existing pytest setup.
- Security: MEDIUM — ASVS categories applied to a desktop-only-with-cloud-gates design; STRIDE table is exhaustive of phase surfaces but security review is mostly preventing-leakage, not preventing-attack.

**Research date:** 2026-05-21

**Valid until:** 2026-06-21 (estimate — 30 days; the stack is stable; only PyMuPDF moves fast enough to warrant a sooner re-check)

---

*Phase: 95-my-library*
*Research completed: 2026-05-21*
*Consumed by: gsd-planner (creates `95-NN-PLAN.md` slots based on this research + 46 CONTEXT decisions + 10 SPEC requirements)*
