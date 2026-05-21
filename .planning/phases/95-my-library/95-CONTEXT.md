# Phase 95: My Library — Local Document Indexing - Context

**Gathered:** 2026-05-21
**Status:** Ready for planning

<domain>
## Phase Boundary

A first-class desktop tab ("My Library") that indexes user-owned `.docx` / `.pdf` / `.txt` files into a SEPARATE Tantivy side-index (kept fully apart from the shared Genizah corpus), surfaces those documents inline in normal search / Composition Search / Parallels results with a `LOCAL` badge in the existing `COL_SRC` column and a three-state filter button, and hard-rejects personal sys_ids at three cloud-write boundaries (`/api/search` serializer, `lists_sync.sync_item_to_cloud`, `corrections_client`). Productizes Yehuda Seewald's `seewald_addition/` external prototype as an in-app feature — no second installation, no Program Files patching, no sys_id namespace collision with NLI / PGP / CUDL, no web/API/Supabase exposure.

</domain>

<spec_lock>
## Requirements (locked via SPEC.md)

**10 requirements are locked.** See `95-SPEC.md` for full requirements, boundaries, and acceptance criteria.

Downstream agents MUST read `95-SPEC.md` before planning or implementing. Requirements are not duplicated here.

**In scope (from SPEC.md):**
- New `shared/local_indexer.py` module: folder-scan + extraction (`.docx` / `.pdf` / `.txt`) + RTL/single-word helpers + SQLite mtime cache + Tantivy side-index builder.
- New helper `is_local_sys_id(s) -> bool` recognizing 18-digit `97`-prefixed sys_ids.
- New `MyLibraryTab` registered as 6th tab in the desktop `QTabWidget`.
- Tantivy side-index queried alongside main index; result-merger combines hits.
- `LOCAL` badge in `COL_SRC` column on desktop search / Composition Search / Parallels result tables.
- Three-state `LOCAL` filter button on the three desktop result surfaces, cycling `All` / `Only LOCAL` / `No LOCAL`, hidden until LOCAL hits exist, persisted across sessions.
- Three hard-reject regression tests on cloud-write boundaries.
- Per-file status panel; auto-detect at app start + manual Refresh; cancellation; 5,000-files/2 GB ceiling with warning dialog.

**Out of scope (from SPEC.md):**
- Web app integration (web Tantivy never contains LOCAL).
- OCR for image-only PDFs.
- `.epub`, `.md`, `.html`, `.rtf`, `.doc` (legacy Word).
- Cloud-synced Lists containing LOCAL items.
- Multi-machine sync of the local index.
- Live `QFileSystemWatcher` folder watching.
- Modification of shared `Transcriptions.txt` / `libraries.csv`.
- Upgrade path from Seewald's prototype installation.
- Per-user-account separation beyond OS profile paths.
- Editing / annotating LOCAL documents inside GenizahSearch.
- Browser extension changes.

</spec_lock>

<decisions>
## Implementation Decisions

### PDF / DOCX / TXT Extraction
- **D-01: PDF extractor pin — PyMuPDF only.** Single extractor; no `pdfplumber` / `pypdf` fallbacks. PyMuPDF's `get_text("blocks")` handles Hebrew RTL correctly at the source, eliminating the mirror-reversal problem the SPEC's RTL helpers address. Installer impact: ~+25 MB. Constraint: PyMuPDF `get_text("blocks")` is the per-page extraction call (NOT `get_text()` plain, NOT `get_text("dict")`).
- **D-02: REQ-4 RTL helpers ported as dead-code safety net.** `_fix_rtl_line`, `_fix_rtl_page`, `_join_fragmented_lines` from `seewald_addition/genizah_make_index.py:67-105` ported verbatim to `shared/local_indexer.py`. Acceptance tests (REQ-4 fixtures with mirror-reversed Hebrew + single-word-per-line PDFs) ship and pass. **The helpers are NEVER invoked at runtime in v1** — they're dead code preserved as ready-to-wire fallback if a future phase adds `pdfplumber`/`pypdf`. The tests serve as a regression-prevention contract for the heuristics.
- **D-03: PDF page-break model — one Tantivy doc per PDF page.** Mirrors the main index's per-page document model (`scope="page"`). Keeps snippet rendering + line-number gutter (Phase 92.2) uniform across LOCAL and Genizah hits.
- **D-04: DOCX page-break model — split every 20 paragraphs.** `python-docx` has no reliable page concept; the Seewald `contains_page_break` heuristic catches only explicit page breaks. Use a fixed paragraph window of 20 paragraphs per "page" instead — finer-grained snippet relevance than Seewald's heuristic, predictable Tantivy doc count per file. The page-break run detection is NOT used.
- **D-05: Scanned PDF (no text layer) handling.** If total extracted chars per file is `< 50`, file gets `status="no_text_layer"` in the per-file status panel. No Tantivy doc rows emitted. Threshold is a constant in `shared/local_indexer.py`; review post-ship if false-positives appear.
- **D-06: Empty-page detection.** Pages with `< 10` chars after stripping are skipped silently — no Tantivy doc, `Pages` count in the status row excludes them. Browse-map `p_num` is non-contiguous in this case (browse navigation skips empty pages).
- **D-07: TXT encoding policy — deferred to implementation, test-driven.** Starting point: `utf-8-sig` only (BOM-tolerant). Planner runs local smoke tests against a Hebrew TXT corpus before locking the final policy. Candidate fallbacks if smoke surfaces real-world breakage: `cp1255` (legacy Windows Hebrew). `chardet` is NOT a candidate — too slow, unreliable. **Open decision** — planner records the chosen policy in `95-NN-PLAN.md` after testing.

### Result Merger + LAB Integration
- **D-08: Main search result-merger — concat + BM25 sort.** Query both the main index and the LOCAL side-index; concatenate raw hits; sort descending by Tantivy BM25 `score`; truncate to result limit. Schemas match (SPEC constraint), so scores are comparable. **Tie-break: Genizah first** when LOCAL and Genizah hits have identical scores.
- **D-09: Composition Search / Parallels — parallel LOCAL lab side-index.** When the LOCAL indexer runs in MyLibraryTab, it ALSO writes a parallel lab side-index at `Config.LOCAL_LAB_INDEX_DIR` using the LAB schema (`fingerprint_dyn`, etc., per the schema built in `rebuild_lab_index()` at `genizah_core.py:742-790`). Composition Search and Parallels query both `lab_index` and `local_lab_index` and merge results. **Single MyLibraryTab indexing run produces BOTH side-indexes in sync.** This satisfies REQ-6's three-surface coverage.
- **D-10: Filter button labels.** `Filter Local` / `Only Local` / `No Local` (EN). Hebrew: `סנן מקומי` / `רק מקומי` / `ללא מקומי`. Mirrors the Phase 93 PGP-filter wording. Same `outline dense no-caps` styling.
- **D-11: Badge rendering — reuse existing `COL_SRC` column with color.** REQ-7 satisfied by writing `source='LOCAL'` on LOCAL search-result rows; existing `COL_SRC` at `genizah_app.py:5909` displays it. The Src cell for LOCAL rows is color-coded blue (`#3498db` foreground) — symmetric with the green-colored PGP cell pattern at `genizah_app.py:5910-5945`. **Visibility rule update**: the existing rule (`hide COL_SRC unless Config.FILE_V7 has data`) extends to `OR result set contains any LOCAL hit`. No new column added to the main search result table.
- **D-12: Composition Search / Parallels result tables — add a new compact 'Source' column uniformly.** Audit during plan: the planner inspects existing Composition / Parallels result-table column layouts. If they already have a Src equivalent, reuse it (mirror D-11). If they don't, the planner adds a new compact `COL_SRC` column to those tables with the same visibility + color-code rule. Goal: uniform LOCAL surface across all three result tables.
- **D-13: `LIBRARY_CODES` extension.** `genizah_core.py:1723` gains `"LOCAL": "My Library"` (EN) and Hebrew display `"הספרייה שלי"`. Existing `core_get_library_display(library_code, short=False, lang=...)` works without special-casing.

### Storage + Persistence
- **D-14: Side-index location — co-locate with `Config.INDEX_DIR`.** `Config.LOCAL_INDEX_DIR = os.path.join(INDEX_DIR, "LocalIndex")` and `Config.LOCAL_LAB_INDEX_DIR = os.path.join(INDEX_DIR, "LocalLabIndex")`. Inherits existing portable-mode rule at `genizah_core.py:2007` automatically — portable installations keep their LOCAL data with the install folder.
- **D-15: Folder-path persistence — `QSettings`.** Use the existing PyQt6 `QSettings` mechanism, scoped to `("GenizahSearchPro", "MyLibrary")`. Stores under `HKCU\Software\GenizahSearchPro` on Windows. Single source-of-truth for the folder list across sessions.
- **D-16: Multi-folder support in v1.** Users can register multiple source folders (not single-folder as SPEC default). UI: a `QListWidget` showing each indexed folder path with `Add Folder…` and `Remove` buttons. One global `Refresh` button rescans all folders. **This is a deliberate expansion of the SPEC's single-folder default** — recorded as an additive decision; SPEC requirements still apply unchanged per-folder.
- **D-17: Folder uniqueness — reject overlaps.** On `Add Folder…`: if path equals an existing entry OR is an ancestor/descendant of an existing entry, reject with error message (`"This folder is already covered by <existing>"`). Prevents duplicate indexing and ambiguous file ownership.
- **D-18: sys_id `content_hash` input — full absolute filepath.** Per SPEC REQ-1 literal: `hashlib.sha256(filepath.encode())` where `filepath` is the normalized absolute path. Moving a file to a different folder produces a new sys_id (re-indexed as new; old row becomes orphan until the file's old location is rescanned and detected as deleted). Content-addressed deduplication is OUT of scope for v1.
- **D-19: machine_id derivation — SPEC default (hostname).** `hashlib.sha256(socket.gethostname().encode()).hexdigest()[:8]` → decimal, zero-padded to 8 digits. Hostname renames invalidate the SQLite cache + force full re-extract on next scan — documented in Help; rare on personal Windows machines. NOT registry-derived; NOT file-pinned in v1.
- **D-20: Folder removal — synchronous delete.** Removing a folder triggers an immediate Tantivy delete-by-sys_id loop for all files under that folder + SQLite DELETE of matching cache rows + side-index commit. Blocks UI briefly for large folders (acceptable). No orphan state.

### Indexing Lifecycle (QThread + Cancellation)
- **D-21: Tantivy writer commit policy — batch commit every 25 files.** Indexer accumulates per-file extractions in the writer; `writer.commit()` + SQLite UPDATE the cache rows every 25 files. On cancellation, up to 25 trailing files re-extract on next scan (≤ 0.5% rework at the 5,000-file ceiling).
- **D-22: Status-row truthiness — two-stage UX.** A file's status transitions: extraction success → row shows `"Indexing…"` → batch commits → row updates to `"OK"`. Avoids the mismatch between per-file extraction success and not-yet-durable side-index state. Status panel reflects committed state once the user sees `"OK"`.
- **D-23: Qt signal cadence — per-file.** Worker emits `progress_updated(current_file_index, total_files, current_filename)` and `file_finished(filename, status, pages, error_msg)` per file. ~thousands of signals per scan — well within Qt throughput.
- **D-24: Cancellation — cooperative flag, between files.** Worker checks `self._cancel_requested` between files; the in-flight file completes its extraction so the next batch-commit boundary stays atomic. Cancel button on MyLibraryTab toolbar.
- **D-25: App-start auto-rescan — silent background + non-modal toast.** App startup is unblocked; rescan runs in a background QThread. Status bar shows a small `"Updating My Library…"` indicator. On completion, a non-modal toast: `"My Library updated: N new files indexed"`. Zero-friction.
- **D-26: Above-ceiling warning — pre-scan count.** Before scanning begins (either on `Add Folder…` or `Refresh`), the worker does an `os.walk` to count `.docx` + `.pdf` + `.txt` files. If total > 5,000, a modal dialog appears: `"Indexing N files — performance may degrade. Continue?"` with `Yes` / `Cancel`. Two-pass for very large folders is acceptable.

### LOCAL Hit Interaction
- **D-27: Click on LOCAL search result — Browse panel text-only view.** LOCAL hits use the existing manuscript browse machinery in a text-only mode: prev/next page navigation works within the same file (`browse_map[sys_id]` is the page list), text + snippet highlighting, **no image pane**. Reuses existing Browse panel code paths.
- **D-28: "Open file" button on LOCAL browse view.** Browse panel toolbar gains a single `Open file` button → `os.startfile(filepath)` (Windows native). Launches the OS default app (Word / Acrobat / Notepad). NOT also "Open containing folder" in v1.
- **D-29: Browse tab — LOCAL search-only in v1.** LOCAL manuscripts do NOT appear in the existing Browse tab. The Genizah Browse experience stays Genizah-only. Backlog item: `"My Library" filter in Browse tab` for a future phase.

### Web-app Surface Hardening
- **D-30: Filter LOCAL out of web-facing library lists.** `genizah_core.LIBRARY_CODES` gains `"LOCAL"` (per D-13), but web pages that render library-filter dropdowns (`web/pages/search.py`, `web/pages/browse.py`, any other consumer) skip any entry with `code == "LOCAL"`. Defense-in-depth alongside REQ-9 serializer filter. Web users never see "My Library" as a filter option.

### Documentation + Credit
- **D-31: Help page — new "My Library" section.** Both apps (web Help page at `web/pages/help.py` + desktop Help dialog). Bilingual (EN + HE). Covers: (a) what gets indexed, (b) where data lives (`%LOCALAPPDATA%\GenizahSearchPro\Index\LocalIndex\`), (c) **privacy guarantee** (never uploaded; three cloud-write gates prevent leak), (d) three-state filter usage, (e) hostname-rename caveat (full re-extract on next scan).
- **D-32: Seewald attribution.** About dialog + Help page line in BOTH apps (web + desktop), BOTH languages: `"My Library feature inspired by Yehuda Seewald's GenizahLocal prototype"`. (HE: `"תכונת הספרייה שלי בהשראת אב-טיפוס GenizahLocal של יהודה זיוואלד"` — translation pending user confirmation during plan/execute.)
- **D-33: Side-index security — no encryption, no mention in Help.** Tantivy stores cleartext on disk. OS-level disk encryption (BitLocker / FileVault) is the user's responsibility. No documentation note — trust OS-level encryption silently. (If a privacy concern surfaces post-ship, revisit.)

### Claude's Discretion
- **D-07 follow-up:** Exact TXT encoding fallback policy (utf-8-sig only vs utf-8-sig + cp1255 fallback) — planner picks after local smoke tests, records in `95-NN-PLAN.md`.
- **D-12 follow-up:** Exact column position for the new Composition / Parallels Source column — planner inspects existing column layouts and picks.
- **D-32 follow-up:** Final Hebrew translation of the Seewald attribution line — user-reviewed during execute (or planner picks if user is offline).
- Per-file status panel column widths, button colors, exact toast styling — planner discretion (consistent with existing desktop styling).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase Specification
- `.planning/phases/95-my-library/95-SPEC.md` — **Locked requirements — MUST read before planning.** 10 requirements, boundaries, constraints, 22 acceptance criteria checkboxes.

### Project + Milestone Context
- `.planning/PROJECT.md` — core value statement, current focus.
- `.planning/REQUIREMENTS.md` — milestone v7.13 requirements (Phase 95 is the v7.14 opener).
- `.planning/ROADMAP.md` §"v7.14 My Library" — phase positioning and rationale.
- `CLAUDE.md` §"Recently Changed" + §"Project Architecture" — dual-app architecture, Tantivy/shared-core constraints.

### Prior-Phase Patterns (Precedents to Mirror)
- `.planning/phases/85-synthetic-inventories/85-CONTEXT.md` (if present) — D-13 synthetic sys_id namespace precedent. `shared/synthetic_sys_id.py` is the helper-module template for `is_local_sys_id`.
- `.planning/phases/93-filtering-by-pgp/93-CONTEXT.md` — Three-state filter button pattern, `safe_storage` persistence pattern. Cascade discipline (`tests/test_pgp_filter_cascade.py` model).
- `.planning/phases/94-adding-pgp-to-downloaded-data/94-CONTEXT.md` — Cross-app parity via shared `shared/` helpers; CI-pin test pattern.
- `.planning/phases/87-90-multitenant/*-CONTEXT.md` (Phase 87 + 88) — `web/safe_storage.py` chokepoint invariant. Phase 95 is desktop-only but any `shared/` code MUST not regress the multitenant invariant.

### Seewald Prototype (Port Source)
- `seewald_addition/GenizahSearch_Local_Extension.md` — full prototype design doc (Hebrew). Reference for what's being productized + what's intentionally being replaced.
- `seewald_addition/genizah_make_index.py:67-105` — `_fix_rtl_line` / `_fix_rtl_page` / `_join_fragmented_lines` source (port target per D-02).
- `seewald_addition/genizah_local_indexer.py` — SQLite `processed_files` cache pattern (port target per SPEC REQ-5).

### Core Codebase Anchors
- `genizah_core.py:1723` — `LIBRARY_CODES` table (extension point per D-13).
- `genizah_core.py:2007` — `Config.INDEX_DIR` + portable-mode resolution (extension point per D-14).
- `genizah_core.py:5130-5189` — Tantivy main-index schema definition (LOCAL side-index MUST mirror per SPEC constraint).
- `genizah_core.py:742-790` — `rebuild_lab_index()` LAB schema + builder (LOCAL lab side-index MUST mirror per D-09).
- `genizah_core.py:1292-1349` — `lab_composition_search()` — extension point for querying `local_lab_index` per D-09.
- `genizah_app.py:3079-3091` — `QTabWidget` tab registration (MyLibraryTab inserts here as 6th tab per SPEC REQ-8).
- `genizah_app.py:5909-5945` — `COL_SRC` + `COL_PGP` column setup (`COL_SRC` reuse target per D-11).
- `genizah_app.py:16534, 16741` — `COL_SRC` write site + visibility rule (extend per D-11).
- `shared/synthetic_sys_id.py` — module template for new `is_local_sys_id` helper.
- `corrections_client.py:619-623` — existing `is_synthetic_sys_id` gate (extend with LOCAL check per SPEC REQ-9).
- `lists_sync.py:763-770` — currently ungated (add LOCAL gate per SPEC REQ-9).
- `shared/search_serializer.py:_serialize_item` + `web/search_api.py:633-939` — payload serializer (add LOCAL filter per SPEC REQ-9).

### Filter Pattern References
- `web/pages/search.py:1430-1434` — PGP filter button pattern (precedent for the LOCAL three-state filter per D-10, applied to desktop surfaces per SPEC REQ-6).
- `tests/test_pgp_filter_cascade.py` — static AST cascade-discipline guard (template for `tests/test_local_filter_cascade.py`).

### Help / Docs
- `web/pages/help.py` (or current Help page module) — extension point per D-31.
- Desktop Help dialog (planner identifies the exact file/widget) — extension point per D-31.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`shared/synthetic_sys_id.py`** — pattern template + module to mirror for `is_local_sys_id`. Includes the repo-grep lint test pattern (`tests/test_synthetic_sys_id.py::TestNoIntCoercion`) — replicate for LOCAL sys_ids.
- **`genizah_core.py:5130-5189` Tantivy schema builder** — same fields (`unique_id`, `content`, `content_head`, `content_tail`, `line_starts`, `line_ends`, `source`, `full_header`, `shelfmark`, `scope`, `boundaries`) used for both main and LOCAL side-index. Schema match is a SPEC constraint, not a choice.
- **`genizah_core.py:742-790` `rebuild_lab_index()`** — LAB schema + fingerprint computation template for the parallel LOCAL lab side-index (D-09).
- **`Config.INDEX_DIR` portable-mode resolution** — automatically gives LOCAL side-index the same portability story (D-14).
- **`QSettings` desktop persistence** — already used for other settings; extension target for folder-path persistence (D-15).
- **`COL_SRC` + `COL_PGP` column infrastructure** — sortable, fixed-width, color-coded foreground pattern (`#27ae60` for PGP). LOCAL reuses `COL_SRC` with `#3498db` blue (D-11).
- **`browse_map` per-sys_id page list** — extends to LOCAL files page-by-page (D-27 browse-panel reuse).
- **Phase 93 `safe_storage` persist_value pattern** — model for the LOCAL filter state persistence (cycling state across sessions).
- **`corrections_client.py:619-623` `is_synthetic_sys_id` gate** — direct extension point per SPEC REQ-9; adding LOCAL is an `OR` with the existing check.

### Established Patterns
- **Shared helpers in `shared/`** consumed by both apps (Phase 94 invariant). `shared/local_indexer.py` is desktop-only but the `is_local_sys_id` helper + cloud-write gates live in `shared/` so the web app correctly rejects LOCAL too.
- **Per-test CI pin** for invariants (`tests/test_no_raw_storage_access.py`, `tests/test_pgp_filter_cascade.py`, `tests/test_export_xlsx_cross_parity.py`). New tests follow this pattern: `tests/test_local_sys_id_namespace.py`, `tests/test_side_index_merge.py`, `tests/test_local_filter_cascade.py`, three `test_local_namespace_no_*_leak.py` files (SPEC REQ-9).
- **18-digit numeric sys_id namespacing**: `99`-prefix for synthetic Genizah (Phase 85), `97`-prefix reserved for LOCAL (this phase). Format is digits-only, length-locked.
- **Three-state filter cascade discipline**: filter applied AFTER existing filters (printed, PGP, exclusions, refinement). No re-query; post-search render-time only. Pinned by static AST tests.
- **Per-user state through chokepoint** (Phase 87): not directly applicable (desktop has no `app.storage.user`), but the principle (single source of truth, no scattered raw reads) extends to QSettings access — planner should consider whether a `desktop/settings.py` wrapper around `QSettings` is warranted.

### Integration Points
- **`genizah_app.py:3079-3091`** — `QTabWidget.addTab()` line registers MyLibraryTab as 6th tab.
- **Main search query path** (`genizah_core.py` Tantivy searcher) — extended to query both indexes and merge (D-08).
- **`lab_composition_search()` at `genizah_core.py:1292-1349`** — extended to query `local_lab_index` (D-09).
- **`genizah_app.py:16534`** (and Composition / Parallels result-render call sites) — extended to write `source='LOCAL'` and color the cell blue for LOCAL hits (D-11).
- **`genizah_app.py:16741`** — visibility-rule extension for `COL_SRC` when LOCAL hits present (D-11).
- **`corrections_client.py:619-623`** — extend the existing synthetic-sys_id gate with LOCAL check (REQ-9).
- **`lists_sync.py:763-770`** — add new LOCAL gate (REQ-9).
- **`shared/search_serializer.py:_serialize_item`** — add LOCAL filter (REQ-9 defense-in-depth).

</code_context>

<specifics>
## Specific Ideas

- **Browse-panel text-only mode (D-27)** — user explicitly wants LOCAL hits to work like Genizah hits (same browse navigation), just without the image pane. Planner should look for the existing "no image" branch in the Browse panel (e.g., when NLI image fetch fails) and reuse that rendering mode rather than adding a new branch.
- **`COL_SRC` reuse with blue color (D-11)** — user noted the existing Src column during discussion. The reuse approach is preferred over adding a new badge column. Blue (`#3498db`) chosen to match the existing PGP-green pattern (`#27ae60`); planner picks the exact shade if `#3498db` clashes with anything else.
- **Multi-folder expansion of SPEC default (D-16)** — user explicitly chose multi-folder despite SPEC's single-folder default. Recorded as additive: SPEC requirements still apply per-folder; `processed_files` cache adds a folder-grouping concept (folder_id or just inferred from path prefix).
- **PyMuPDF only (D-01)** — user prioritizes Hebrew-extraction quality over file-format coverage breadth. The dead-code helpers (D-02) preserve future optionality without invoking them in v1.
- **Seewald attribution in About + Help (D-32)** — user wants visible credit in both apps, both languages, on ship.

</specifics>

<deferred>
## Deferred Ideas

### Future Backlog (post-v7.14 phases)
- **"My Library" filter in Browse tab** — register `LOCAL` as a Browse-tab library filter option in a future phase so users can skim their indexed files outside of search. v1 is search-only per D-29.
- **Cloud-synced Lists for LOCAL items** — explicitly OUT of v1 per SPEC; backlog item with privacy-design caveat (filename-as-title leaks personal corpora to share recipients).
- **OCR for image-only PDFs** — Tesseract integration, ~1-5s/page, mixed Hebrew quality. Backlog.
- **Additional file types (`.epub`, `.md`, `.html`, `.rtf`, `.doc`)** — user-demand-driven follow-up phase. Backlog.
- **"Import from Seewald prototype"** — optional UI button to migrate users with existing `%USERPROFILE%\GenizahLocal\` data. Demand-driven backlog.
- **Multi-machine sync** — out of scope per SPEC; backlog if user demand.
- **Live folder watch via `QFileSystemWatcher`** — out of scope per SPEC; backlog if user demand.
- **PDF fallback extractors (`pdfplumber` / `pypdf`)** — REQ-4 helpers ported as dead code per D-02; future phase can wire them in without re-deriving the heuristics.
- **Content-addressed sys_id (file-content SHA256 dedup)** — D-18 picks filepath-based hashing for v1. Content-addressed dedup is a follow-up if users surface duplicate-file-across-folders pain.
- **Encrypted side-index** — D-33 trusts OS-level disk encryption. If a privacy concern surfaces post-ship, revisit (substantial Tantivy plumbing).
- **Reveal in Explorer button** — D-28 ships `Open file` only. Add `Open containing folder` if user demand.

### Reviewed Todos (not folded)
The `gsd-sdk query todo.match-phase 95` matcher surfaced 6 todos by keyword overlap, but **none** are actually scope-relevant to local document indexing:
- `2026-02-11-migrate-desktop-corrections-fetch-to-shared-corrections-service.md` — matched on "desktop, corrections, shared" but is about corrections-service refactor, not LOCAL.
- `2026-03-18-fill-missing-genizah-manuscripts-from-fist.md` — Genizah catalog fill, not LOCAL.
- `2026-04-16-reading-desk-ux-fixes.md` — Reading Desk UX, unrelated.
- `2026-03-07-server-side-search-with-email-notification-of-results.md` — server-side search, web-only direction.
- `2026-03-08-nli-marc-crawl-and-translate.md` — NLI MARC crawl, unrelated.
- `2026-03-09-unified-metadata-text-search-with-translations.md` — metadata search, unrelated.

All deferred (left in the todo backlog); none folded into Phase 95.

</deferred>

---

*Phase: 95-my-library*
*Context gathered: 2026-05-21*
