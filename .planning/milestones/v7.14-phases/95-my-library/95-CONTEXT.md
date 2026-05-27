# Phase 95: My Library — Local Document Indexing - Context

**Gathered:** 2026-05-21
**Revised:** 2026-05-21 (Codex critique folded in — see `95-CODEX-CRITIQUE.md`)
**Status:** Ready for planning

> **Revision note:** This document was revised after a Codex (gpt-5.5) critique surfaced
> two P0 issues + eight gaps. Amendments are marked **[Codex revision]** inline on the
> affected decisions (D-02, D-08, D-09, D-10, D-11, D-13, D-15, D-18, D-19, D-20, D-21,
> D-24, D-25, D-26, D-30, D-33). New decisions D-34 through D-46 close the gaps. Original
> decisions remain authoritative; amendments tighten/correct, never reverse.

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
  - **[Codex revision]** PyInstaller / Inno Setup packaging needs explicit `fitz`/`pymupdf` hidden-import + binary-collect handling — not just `requirements.txt`. See D-43 for the packaging spec.
- **D-02: REQ-4 RTL helpers ported as dead-code safety net.** `_fix_rtl_line`, `_fix_rtl_page`, `_join_fragmented_lines` from `seewald_addition/genizah_make_index.py:67-105` ported verbatim to `shared/local_indexer.py`. Acceptance tests (REQ-4 fixtures with mirror-reversed Hebrew + single-word-per-line PDFs) ship and pass. **The helpers are NEVER invoked at runtime in v1** — they're dead code preserved as ready-to-wire fallback if a future phase adds `pdfplumber`/`pypdf`. The tests serve as a regression-prevention contract for the heuristics.
  - **[Codex revision]** Dead-code tests alone don't prove v1 runtime extraction quality. ADD a real PyMuPDF Hebrew test fixture: `tests/test_local_indexer.py::test_pymupdf_hebrew_extraction_quality` with a known Hebrew PDF fixture (multi-column, RTL, mixed Hebrew/Latin), asserts expected paragraph text + correct reading order via `get_text("blocks")`. Confirms v1 happy path, not just the unused fallback.
- **D-03: PDF page-break model — one Tantivy doc per PDF page.** Mirrors the main index's per-page document model (`scope="page"`). Keeps snippet rendering + line-number gutter (Phase 92.2) uniform across LOCAL and Genizah hits.
- **D-04: DOCX page-break model — split every 20 paragraphs.** `python-docx` has no reliable page concept; the Seewald `contains_page_break` heuristic catches only explicit page breaks. Use a fixed paragraph window of 20 paragraphs per "page" instead — finer-grained snippet relevance than Seewald's heuristic, predictable Tantivy doc count per file. The page-break run detection is NOT used.
- **D-05: Scanned PDF (no text layer) handling.** If total extracted chars per file is `< 50`, file gets `status="no_text_layer"` in the per-file status panel. No Tantivy doc rows emitted. Threshold is a constant in `shared/local_indexer.py`; review post-ship if false-positives appear.
- **D-06: Empty-page detection.** Pages with `< 10` chars after stripping are skipped silently — no Tantivy doc, `Pages` count in the status row excludes them. Browse-map `p_num` is non-contiguous in this case (browse navigation skips empty pages).
- **D-07: TXT encoding policy — deferred to implementation, test-driven.** Starting point: `utf-8-sig` only (BOM-tolerant). Planner runs local smoke tests against a Hebrew TXT corpus before locking the final policy. Candidate fallbacks if smoke surfaces real-world breakage: `cp1255` (legacy Windows Hebrew). `chardet` is NOT a candidate — too slow, unreliable. **Open decision** — planner records the chosen policy in `95-NN-PLAN.md` after testing.

### Result Merger + LAB Integration
- **D-08: Main search result-merger — concat + BM25 sort.** Query both the main index and the LOCAL side-index; concatenate raw hits; sort descending by Tantivy BM25 `score`; truncate to result limit. Schemas match (SPEC constraint), so scores are comparable. **Tie-break: Genizah first** when LOCAL and Genizah hits have identical scores.
  - **[Codex revision — P0]** BM25 scores from two INDEPENDENT Tantivy indexes are NOT directly comparable (BM25 IDF is index-local). Replace raw `score` sort with **reciprocal-rank fusion (RRF, k=60)**: rank each source list independently, then sort by `Σ 1/(k + rank_in_source)`. RRF is stable, parameter-light, and the industry standard for fusing independent retriever outputs. Keep "Genizah first on tie" tie-break.
  - **[Codex revision — P0]** `_deduplicate()` at `genizah_core.py:7916-7921` literally drops anything that isn't `V0.8`/`V0.7` source. **LOCAL hits MUST merge AFTER `_deduplicate()` is called** at `genizah_core.py:7390`. Alternative: generalize `_deduplicate()` to whitelist V0.8/V0.7 dedup behavior and passthrough other sources. **Choose: merge after `_deduplicate()`** — smaller blast radius, leaves Genizah dedup behavior untouched. Pin via `tests/test_local_post_dedup_merge.py` (asserts a LOCAL hit added before `_deduplicate()` is dropped; same hit added after survives).
- **D-09: Composition Search / Parallels — parallel LOCAL lab side-index.** When the LOCAL indexer runs in MyLibraryTab, it ALSO writes a parallel lab side-index at `Config.LOCAL_LAB_INDEX_DIR` using the LAB schema (`fingerprint_dyn`, etc., per the schema built in `rebuild_lab_index()` at `genizah_core.py:742-790`). Composition Search and Parallels query both `lab_index` and `local_lab_index` and merge results. **Single MyLibraryTab indexing run produces BOTH side-indexes in sync.** This satisfies REQ-6's three-surface coverage.
  - **[Codex revision — P1]** `fingerprint_dyn` depends on current LAB dynamic weights and rank map. If LAB weights change (settings panel) or main LAB index rebuilds, the local lab side-index becomes silently stale (wrong fingerprints → wrong Composition matches). **Invalidation contract:** at LOCAL lab side-index build time, persist `weights_hash = sha256(json.dumps(current_lab_weights, sort_keys=True))` and `lab_schema_version` to `<LOCAL_LAB_INDEX_DIR>/.meta.json`. At Composition/Parallels query time, compare current LAB weights_hash to stored value; if mismatched, mark stale and surface a non-modal banner: "My Library LAB index out of date — Rebuild?" with a one-click rebuild action. Triggers for rebuild: (a) MyLibraryTab Refresh, (b) hash mismatch detected, (c) main LAB rebuild via Tools menu. See D-38 for the full invalidation algorithm.
  - **[Codex revision]** For LAB search merging (NOT main search), preserve the existing custom fingerprint scoring path — do NOT reuse raw Tantivy BM25 scores. The lab_composition_search at `genizah_core.py:1292-1349` runs its own scoring; LOCAL lab hits flow through the same path with the LOCAL fingerprint values. Merger = concat scored lists from both lab indexes, sort by custom score desc, Genizah first on tie.
- **D-10: Filter button labels.** `Filter Local` / `Only Local` / `No Local` (EN). Hebrew: `סנן מקומי` / `רק מקומי` / `ללא מקומי`. Mirrors the Phase 93 PGP-filter wording. Same `outline dense no-caps` styling.
  - **[Codex revision — P1]** Persisted `Only Local` state combined with REQ-6's "hide button when no LOCAL hits exist" creates an INVISIBLE FILTER: user filters to Only-Local on Tuesday, returns Wednesday with the button hidden, and sees zero results with no escape. **Fix:** when the current result set has zero LOCAL hits AND state is `Only Local` OR `No Local`, the filter is rendered as a NO-OP — i.e., it's not applied (all hits show), and a small inline chip appears: `"My Library filter inactive — no LOCAL hits in this query"`. The persisted state is preserved (next query with LOCAL hits re-activates the filter). User can clear the chip via the button (which becomes visible only when LOCAL hits exist). Pin via `tests/test_local_filter_cascade.py::test_no_op_when_no_local_hits`.
- **D-11: Badge rendering — reuse existing `COL_SRC` column with color.** REQ-7 satisfied by writing `source='LOCAL'` on LOCAL search-result rows; existing `COL_SRC` at `genizah_app.py:5909` displays it. The Src cell for LOCAL rows is color-coded blue (`#3498db` foreground) — symmetric with the green-colored PGP cell pattern at `genizah_app.py:5910-5945`. **Visibility rule update**: the existing rule (`hide COL_SRC unless Config.FILE_V7 has data`) extends to `OR result set contains any LOCAL hit`. No new column added to the main search result table.
  - **[Codex revision]** Reusing `COL_SRC` changes `display.source` semantic from "transcription version" to "result provenance" for LOCAL. Audit downstream consumers of `display.source` — specifically `shared/export_dossier.py` and `genizah_app.py:export_results('xlsx')`. See D-45 for the export-path handling decision.
- **D-12: Composition Search / Parallels result tables — add a new compact 'Source' column uniformly.** Audit during plan: the planner inspects existing Composition / Parallels result-table column layouts. If they already have a Src equivalent, reuse it (mirror D-11). If they don't, the planner adds a new compact `COL_SRC` column to those tables with the same visibility + color-code rule. Goal: uniform LOCAL surface across all three result tables.
- **D-13: `LIBRARY_CODES` extension.** `genizah_core.py:1723` gains `"LOCAL": "My Library"` (EN) and Hebrew display `"הספרייה שלי"`. Existing `core_get_library_display(library_code, short=False, lang=...)` works without special-casing.
  - **[Codex revision — P0]** Core parsers (`parse_header_smart`, `parse_full_id_components` at `genizah_core.py:3640-3680`) currently match `99\d…` patterns — they don't recognize `97`-prefixed LOCAL sys_ids. Without generalization, LOCAL display fields (library_code, shelfmark, sys_id) disappear from result rows. **Generalize:** audit every regex that matches `99\d{16}` or similar Phase-85 patterns and broaden to `(99|97)\d{16}` OR (preferred) route through a centralized helper `extract_sys_id(header) -> str | None` that internally calls `is_synthetic_sys_id` AND `is_local_sys_id`. Add `tests/test_local_sys_id_parser_compat.py` asserting `parse_header_smart` + `parse_full_id_components` correctly extract LOCAL sys_ids on synthetic LOCAL full_headers (per D-34 format).

### Storage + Persistence
- **D-14: Side-index location — co-locate with `Config.INDEX_DIR`.** `Config.LOCAL_INDEX_DIR = os.path.join(INDEX_DIR, "LocalIndex")` and `Config.LOCAL_LAB_INDEX_DIR = os.path.join(INDEX_DIR, "LocalLabIndex")`. Inherits existing portable-mode rule at `genizah_core.py:2007` automatically — portable installations keep their LOCAL data with the install folder.
- **D-15: Folder-path persistence — SQLite-mirrored.** Originally proposed: `QSettings` only. **[Codex revision]** `QSettings` stores under `HKCU\Software\GenizahSearchPro` — non-portable, disconnected from `Config.INDEX_DIR` (D-14). Portable users would keep the side-index data when moving the install folder but lose their folder-list configuration. **Fix:** store the folder list in the SQLite cache (`<LOCAL_INDEX_DIR>/local_index.sqlite3`, new table `folders(folder_id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, added_at REAL, last_scanned_at REAL, status TEXT)`). `QSettings` keeps non-portable per-user UI preferences only (last-selected folder in the list, column widths, etc.). On portable-mode launch, the folder list comes with the index automatically.
- **D-16: Multi-folder support in v1.** Users can register multiple source folders (not single-folder as SPEC default). UI: a `QListWidget` showing each indexed folder path with `Add Folder…` and `Remove` buttons. One global `Refresh` button rescans all folders. **This is a deliberate expansion of the SPEC's single-folder default** — recorded as an additive decision; SPEC requirements still apply unchanged per-folder.
- **D-17: Folder uniqueness — reject overlaps.** On `Add Folder…`: if path equals an existing entry OR is an ancestor/descendant of an existing entry, reject with error message (`"This folder is already covered by <existing>"`). Prevents duplicate indexing and ambiguous file ownership.
  - **[Codex revision — P1]** Naive string-prefix overlap check is fragile on Windows: junctions, UNC paths (`\\server\share`), 8.3 short names, case-insensitive vs case-sensitive segments. **Normalize:** for every candidate and existing path, apply `Path(p).resolve(strict=False)` → `os.path.normcase()` then test overlap via `os.path.commonpath([a, b]) in {a, b}`. Test fixtures: junction-link-to-folder, UNC mount, drive-letter-equivalent path, mixed-case path. Pin via `tests/test_folder_overlap_detection.py`.
- **D-18: sys_id `content_hash` input — full absolute filepath.** Per SPEC REQ-1 literal: `hashlib.sha256(filepath.encode())` where `filepath` is the normalized absolute path. Moving a file to a different folder produces a new sys_id (re-indexed as new; old row becomes orphan until the file's old location is rescanned and detected as deleted). Content-addressed deduplication is OUT of scope for v1.
  - **[Codex revision]** Filepath input MUST be normalized identically every time or the same file gets two sys_ids across rescans (rename casing, Windows path separators). **Canonical input:** `os.path.normcase(str(Path(filepath).resolve(strict=False)))`. Lock the normalization in a single helper `_canonical_filepath(p) -> str` in `shared/local_indexer.py`; all sys_id generation routes through it. See D-19 for the hash → 8-digit reduction.
- **D-19: machine_id derivation — SPEC default (hostname).** `hashlib.sha256(socket.gethostname().encode()).hexdigest()[:8]` → decimal, zero-padded to 8 digits. Hostname renames invalidate the SQLite cache + force full re-extract on next scan — documented in Help; rare on personal Windows machines. NOT registry-derived; NOT file-pinned in v1.
  - **[Codex revision]** "Hex `[:8]` converted to decimal" can produce up to 10 digits (max 0xFFFFFFFF = 4294967295), overflowing the 8-digit slot. **Canonical formula:** `f"{int(hashlib.sha256(socket.gethostname().encode()).hexdigest()[:8], 16) % 10**8:08d}"`. Same modulo applied to `content_hash`: `f"{int(hashlib.sha256(canonical_filepath.encode()).hexdigest()[:8], 16) % 10**8:08d}"`. The explicit `% 10**8` is the contract. Pin via `tests/test_local_sys_id_namespace.py::test_machine_id_always_8_digits` and `test_content_hash_always_8_digits`.
  - **[Codex revision]** At 5,000 files, an 8-digit content_hash space (10^8) has a non-trivial birthday-collision probability (~0.012%). Acceptable, but handle deterministically. **Collision-resolution:** SQLite `local_files` table has `UNIQUE(sys_id)` constraint; on insert collision, fall back to next 8 hex chars of the same SHA256 (`hexdigest()[8:16]` → modulo) and retry. Cap retries at 4; log warning if >0 fallbacks used. See D-35 schema.
- **D-20: Folder removal — synchronous delete.** Removing a folder triggers an immediate Tantivy delete-by-sys_id loop for all files under that folder + SQLite DELETE of matching cache rows + side-index commit. Blocks UI briefly for large folders (acceptable). No orphan state.
  - **[Codex revision — P1]** Tantivy's `unique_id` is a text field; deleting by sys_id alone is unsafe when a single file produces multiple page-level docs (one per PDF page / DOCX chunk per D-03/D-04). **Add SQLite sidecar `local_pages(sys_id TEXT, uid TEXT, page_num INTEGER, PRIMARY KEY(sys_id, page_num))`** to track every emitted page-level UID. Deletion = SELECT uids from `local_pages` WHERE sys_id IN (...) → `writer.delete_documents(Term("unique_id", uid))` per UID → commit → SQLite DELETE. Pin via `tests/test_local_delete_by_uid.py`.

### Indexing Lifecycle (QThread + Cancellation)
- **D-21: Tantivy writer commit policy — batch commit every 25 files.** Indexer accumulates per-file extractions in the writer; `writer.commit()` + SQLite UPDATE the cache rows every 25 files. On cancellation, up to 25 trailing files re-extract on next scan (≤ 0.5% rework at the 5,000-file ceiling).
  - **[Codex revision — P1]** Tantivy commit + SQLite update is NOT atomic. A crash between them leaves either (a) indexed Tantivy docs with no SQLite cache row (next scan re-extracts the file, doubling rows) or (b) SQLite cache rows marking files indexed when Tantivy never persisted them (next scan skips, search misses content). **Two-phase commit protocol:**
    1. SQLite INSERT into `processed_files` with `status='pending'` for each file in the batch.
    2. Tantivy `writer.commit()`.
    3. SQLite UPDATE all batch rows to `status='committed'`.
    On app startup, scan SQLite for `status='pending'` rows; re-extract those files (idempotent — delete-by-uid via `local_pages` then re-insert). Same protocol on folder removal: mark `status='pending_delete'` → commit Tantivy deletes → SQLite DELETE. Pin via `tests/test_local_two_phase_commit.py` with a fault-injection harness.
- **D-22: Status-row truthiness — two-stage UX.** A file's status transitions: extraction success → row shows `"Indexing…"` → batch commits → row updates to `"OK"`. Avoids the mismatch between per-file extraction success and not-yet-durable side-index state. Status panel reflects committed state once the user sees `"OK"`.
- **D-23: Qt signal cadence — per-file.** Worker emits `progress_updated(current_file_index, total_files, current_filename)` and `file_finished(filename, status, pages, error_msg)` per file. ~thousands of signals per scan — well within Qt throughput.
- **D-24: Cancellation — cooperative flag, between files.** Worker checks `self._cancel_requested` between files; the in-flight file completes its extraction so the next batch-commit boundary stays atomic. Cancel button on MyLibraryTab toolbar.
  - **[Codex revision]** Between-files-only feels broken on huge single files (e.g., 1,000-page PDF takes minutes to extract; Cancel button does nothing visible). **Add a second-tier cancellation check inside the extraction loop:** check `self._cancel_requested` between PDF pages (every page) and between DOCX paragraph chunks (every 20-paragraph chunk per D-04). If set mid-file, the in-flight file's partial pages are rolled back from the pending Tantivy writer (`writer.rollback()` then re-open) and the file's status row goes to `"Cancelled"`. The batch-commit boundary is still atomic because the partial file never reached commit.
- **D-25: App-start auto-rescan — silent background + non-modal toast.** App startup is unblocked; rescan runs in a background QThread. Status bar shows a small `"Updating My Library…"` indicator. On completion, a non-modal toast: `"My Library updated: N new files indexed"`. Zero-friction.
  - **[Codex revision — P1]** Auto-rescan, manual Refresh, and Remove Folder can race into the same side-index writer (Tantivy + SQLite). Concurrent writes corrupt the index. **Single indexer mutex:** `MyLibraryTab` owns a `QMutex` (or `threading.Lock`) gating ALL side-index mutations. While held: mutating UI controls (Refresh, Add Folder, Remove) are disabled. Concurrent operation requests are queued (FIFO, max queue depth 1 — additional requests collapse). Cancel button releases the mutex via worker shutdown. Pin via `tests/test_local_indexer_mutex.py` (spawn N concurrent Refresh/Remove requests, assert no interleaving in the SQLite log).
- **D-26: Above-ceiling warning — pre-scan count.** Before scanning begins (either on `Add Folder…` or `Refresh`), the worker does an `os.walk` to count `.docx` + `.pdf` + `.txt` files. If total > 5,000, a modal dialog appears: `"Indexing N files — performance may degrade. Continue?"` with `Yes` / `Cancel`. Two-pass for very large folders is acceptable.
  - **[Codex revision — P2]** `os.walk` ignores: (a) the 2 GB byte ceiling, (b) network/OneDrive `PermissionError`, (c) junction loops. **Hardened pre-scan:** use `os.walk(folder, followlinks=False)`, wrap per-directory iteration in `try/except OSError`, accumulate BOTH `file_count` and `total_bytes` (sum of `os.path.getsize`). Warning dialog shows both: `"Indexing 5,234 files (2.4 GB) — performance may degrade. Continue?"`. Trigger on EITHER `file_count > 5000` OR `total_bytes > 2 * 1024**3`. Per-directory `OSError` rows logged to per-file status panel as `status="scan_error"`. See D-41 for the ceiling-definition decision.

### LOCAL Hit Interaction
- **D-27: Click on LOCAL search result — Browse panel text-only view.** LOCAL hits use the existing manuscript browse machinery in a text-only mode: prev/next page navigation works within the same file (`browse_map[sys_id]` is the page list), text + snippet highlighting, **no image pane**. Reuses existing Browse panel code paths.
- **D-28: "Open file" button on LOCAL browse view.** Browse panel toolbar gains a single `Open file` button → `os.startfile(filepath)` (Windows native). Launches the OS default app (Word / Acrobat / Notepad). NOT also "Open containing folder" in v1.
- **D-29: Browse tab — LOCAL search-only in v1.** LOCAL manuscripts do NOT appear in the existing Browse tab. The Genizah Browse experience stays Genizah-only. Backlog item: `"My Library" filter in Browse tab` for a future phase.

### Web-app Surface Hardening
- **D-30: Filter LOCAL out of web-facing library lists.** `genizah_core.LIBRARY_CODES` gains `"LOCAL"` (per D-13), but web pages that render library-filter dropdowns (`web/pages/search.py`, `web/pages/browse.py`, any other consumer) skip any entry with `code == "LOCAL"`. Defense-in-depth alongside REQ-9 serializer filter. Web users never see "My Library" as a filter option.
  - **[Codex revision — P0]** `lists_sync.sync_item_to_cloud()` at `lists_sync.py:736-756` calls `self._get_client()` at line 742 AND `self.sync_list_to_cloud(list_id)` at line 753 BEFORE reading `item_data.sys_id` at line 762. Inserting the LOCAL gate "after the natural lookup" leaks the cloud client connection + a parent-list cloud sync before the gate fires. **CRITICAL: the gate MUST move to the TOP of `sync_item_to_cloud()`:**
    1. First statement: lookup `item_data = self.lists_manager.data.get('items', {}).get(item_id)` from local `self.lists_manager.data` (no network — `lists_manager.data` is the in-memory local copy).
    2. Extract `sys_id = item_data.get('sys_id', item_id)`.
    3. If `is_local_sys_id(sys_id)`: log `"[local-only item, not synced]"` + return False immediately.
    4. THEN proceed with `_get_client()` and any cloud operations.
    Apply the same gate AT THE TOP of `sync_list_to_cloud()`: if ANY item in the list has a LOCAL sys_id, abort the entire list sync with `"[list contains LOCAL items, not synced]"`. Acceptance test `tests/test_local_namespace_no_lists_leak.py` MUST mock `_get_client` and `Supabase` calls and assert ZERO calls when LOCAL sys_id present (per SPEC REQ-9 acceptance).

### Documentation + Credit
- **D-31: Help page — new "My Library" section.** Both apps (web Help page at `web/pages/help.py` + desktop Help dialog). Bilingual (EN + HE). Covers: (a) what gets indexed, (b) where data lives (`%LOCALAPPDATA%\GenizahSearchPro\Index\LocalIndex\`), (c) **privacy guarantee** (never uploaded; three cloud-write gates prevent leak), (d) three-state filter usage, (e) hostname-rename caveat (full re-extract on next scan).
- **D-32: Seewald attribution.** About dialog + Help page line in BOTH apps (web + desktop), BOTH languages: `"My Library feature inspired by Yehuda Seewald's GenizahLocal prototype"`. (HE: `"תכונת הספרייה שלי בהשראת אב-טיפוס GenizahLocal של יהודה זיוואלד"` — translation pending user confirmation during plan/execute.)
- **D-33: Side-index security — no encryption, no mention in Help.** Tantivy stores cleartext on disk. OS-level disk encryption (BitLocker / FileVault) is the user's responsibility. No documentation note — trust OS-level encryption silently. (If a privacy concern surfaces post-ship, revisit.)
  - **[Codex revision]** "No mention" leaves the privacy claim imprecise — users may assume in-app encryption. **Add a one-line disclosure** to the D-31 Help section: `"Your indexed text is stored on disk in cleartext inside the local index — it is never uploaded to GenizahSearch's servers. Use OS-level disk encryption (BitLocker / FileVault) if you need at-rest encryption."` Hebrew: `"הטקסט המאונדקס נשמר בקובץ אינדקס מקומי בטקסט גלוי — הוא לעולם לא מועלה לשרתי GenizahSearch. השתמש בהצפנת דיסק ברמת מערכת ההפעלה (BitLocker / FileVault) אם נדרשת הצפנה במנוחה."` Honest disclosure, no encryption work.

### Gaps Closed (added after Codex critique)

- **D-34: LOCAL `unique_id` + `full_header` format.** No existing format covers LOCAL; pin canonical strings so result row parsing, browse navigation, serializers, and exports all agree.
  - `unique_id` per page: `f"LOCAL_{sys_id}_P{page_num}"` — example: `"LOCAL_970012345601234567_P3"`. The `LOCAL_` prefix is the discriminator; `IE` / `P` patterns from the main index never start with `LOCAL_`. Length-bounded (≤ 40 chars).
  - `full_header` per page (written to Tantivy `full_header` stored field): `f"{sys_id}_LOCAL_P{page_num}_F{file_id:04d}"` where `file_id` is the `local_files.file_id` integer from D-35. Example: `"970012345601234567_LOCAL_P3_F0042"`. Format is parseable by `parse_full_id_components` after the D-13 generalization.
  - `browse_map[sys_id]` entries: `{'p_num': page_num, 'uid': unique_id, 'full_header': full_header, 'ie_id': f"F{file_id:04d}", 'seq_index': page_num}`. The synthetic `ie_id` satisfies `get_volume_pages()` filter — same trick Seewald's prototype used (see `seewald_addition/GenizahSearch_Local_Extension.md` "browse_map.pkl — מבנה כל עמוד").

- **D-35: `local_files` sidecar SQLite table.** Beyond `processed_files` (mtime cache) and `local_pages` (page-UID tracking), the Browse panel + per-file status panel + "Open file" button need richer metadata than the Tantivy schema can hold. New table:
  ```sql
  CREATE TABLE local_files (
    file_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sys_id           TEXT    NOT NULL UNIQUE,
    filepath         TEXT    NOT NULL,           -- canonical, per D-18
    folder_id        INTEGER NOT NULL REFERENCES folders(folder_id),
    display_title    TEXT,                       -- DOCX title / PDF metadata title / filename fallback
    original_filename TEXT   NOT NULL,           -- basename(filepath)
    file_extension   TEXT    NOT NULL,           -- '.pdf' / '.docx' / '.txt'
    page_count       INTEGER NOT NULL DEFAULT 0,
    file_size_bytes  INTEGER NOT NULL,
    extraction_status TEXT   NOT NULL,           -- 'ok' | 'no_text_layer' | 'encoding_error' | 'unsupported' | 'scan_error' | 'cancelled'
    last_indexed_at  REAL    NOT NULL,
    sha256_full      TEXT,                       -- nullable; reserved for future content-dedup
    error_msg        TEXT                        -- nullable; populated when status != 'ok'
  );
  ```
  `processed_files` keeps its narrow mtime-cache role. `local_pages` (D-20) handles per-page UID tracking. `local_files` is the metadata table consumed by Browse + status panel + Open-file. Schema pinned via `tests/test_local_schema_evolution.py`.

- **D-36: Modified-file update algorithm.** When `mtime` or `size` changes for a tracked file, the indexer:
  1. SELECT all `(uid, page_num)` from `local_pages` WHERE `sys_id = ?`.
  2. For each uid: `writer.delete_documents(Term("unique_id", uid))`.
  3. Re-extract pages from the file.
  4. Add new `(sys_id, content_*, full_header, …)` docs to the writer for each page.
  5. SQLite: DELETE FROM `local_pages` WHERE `sys_id = ?`; INSERT new `(sys_id, uid, page_num)` rows.
  6. `local_files`: UPDATE `page_count`, `last_indexed_at`, `extraction_status`.
  7. Tantivy commit (at batch boundary per D-21 two-phase protocol).
  No replacement-index-and-swap. Pure delete-then-insert keeps the writer's invariants simple and the SQLite delete-then-insert atomic within a single transaction. Pin via `tests/test_local_indexer_incremental.py::test_modified_file_reextract_only` (already in SPEC).

- **D-37: Side-index missing / locked / corrupt fallback.** If `tantivy.Index.open(Config.LOCAL_INDEX_DIR)` raises at search-init (missing files, file lock from a crashed previous instance, schema corruption):
  - The main-index search proceeds normally.
  - LOCAL hits are absent from results.
  - The LOCAL filter button stays HIDDEN regardless of result-set composition (button visibility gates on `local_searcher is not None AND result set contains LOCAL hits`).
  - Status bar shows: `"My Library index unavailable — Rebuild?"` with an inline `Rebuild` link that triggers a full MyLibraryTab Refresh.
  - The same fallback applies to `local_lab_index` for Composition/Parallels.
  - Logged at `WARNING` level with the raised exception's `repr()`.
  Pin via `tests/test_local_index_open_fallback.py` (mock Tantivy.open to raise; assert main search returns Genizah-only results without traceback).

- **D-38: Local LAB invalidation triggers.** Per D-09's `weights_hash` contract, rebuild the local lab side-index when ANY of:
  - User clicks Refresh in MyLibraryTab (always — explicit user intent).
  - Stored `weights_hash` ≠ current LAB weights hash (auto-detected at next Composition/Parallels query).
  - Stored `lab_schema_version` < current schema version (after a future schema bump).
  - User clicks Tools → "Rebuild LAB Index" in the main app (rebuilds main LAB; trigger local LAB rebuild too).
  - User clicks the inline "Rebuild" button on the stale-banner surfaced per D-09.
  Local LAB metadata file: `<LOCAL_LAB_INDEX_DIR>/.meta.json` with `{ "weights_hash": "<sha256>", "lab_schema_version": <int>, "last_built_at": "<iso8601>" }`. Test: `tests/test_local_lab_invalidation.py`.

- **D-39: Per-surface filter state keys.** Three independent QSettings keys (mirrored into the `folders` SQLite per D-15 if a portable-friendly UI-pref store materializes later):
  - `myLibrary/search_local_filter` (Search results)
  - `myLibrary/composition_local_filter` (Composition Search results)
  - `myLibrary/parallels_local_filter` (Parallels results)
  Default value: `"all"`. Cycle states: `"all"` → `"only_local"` → `"no_local"` → `"all"`. Each surface persists independently (consistent with the cascade-discipline separation in Phase 93 PGP filter, which is per-surface). Test: `tests/test_local_filter_persistence.py`.

- **D-40: Unavailable folder behavior at startup.** When app-start auto-rescan (D-25) iterates `folders` table, for each folder path: `os.path.isdir(folder.path)` check. If False (external drive unplugged, OneDrive offline, permissions revoked, share offline):
  - UPDATE `folders.status = 'unavailable'`.
  - Skip indexing for this folder.
  - Do NOT delete any existing `local_files` / `local_pages` / Tantivy rows (the data may become available again).
  - UI shows folder with a warning icon and tooltip `"Folder not found at <path> — files remain indexed from last scan."`
  - The folder's previously-indexed files REMAIN searchable (their Tantivy docs are intact).
  - User-initiated `Remove Folder` is the only path that purges rows.
  Pin via `tests/test_local_unavailable_folder.py`.

- **D-41: 2 GB ceiling — source file size.** Per Codex P2: the ceiling needs a measurable definition. **Lock: source file size** (sum of `os.path.getsize(filepath)` across all supported files). Most user-comprehensible — matches what File Explorer shows. NOT extracted-text byte count (computed mid-scan, can't pre-check). NOT Tantivy stored content size (post-commit). Pre-scan dialog displays both file_count + total_bytes per D-26. Ceiling: `file_count > 5000 OR total_bytes > 2 * 1024**3`.

- **D-42: Path normalization helper.** Single helper `_canonical_filepath(p) -> str` in `shared/local_indexer.py`:
  ```python
  def _canonical_filepath(p: str | Path) -> str:
      """Canonical form for sys_id generation and folder-overlap detection.
      Resolves symlinks/junctions, normalizes case, normalizes separators."""
      return os.path.normcase(str(Path(p).resolve(strict=False)))
  ```
  Every sys_id generation site, folder-overlap check, and `local_files.filepath` write routes through this helper. Pin via `tests/test_canonical_filepath.py` with Windows-specific fixtures (UNC, junction, drive-letter casing).

- **D-43: PyInstaller / Inno Setup packaging for PyMuPDF.** PyMuPDF / `fitz` ships C-extension binaries that PyInstaller does not auto-discover from `import fitz` alone. **Required additions:**
  - `CompileScriptGenizah.iss` (or the PyInstaller `.spec` file the build uses): add `--collect-all pymupdf` (or equivalent `hiddenimports = ['fitz']` + `datas` collection for PyMuPDF's binary blobs).
  - Add a smoke test that runs against the PACKAGED EXE in `dist/`: import `fitz`, open a 1-page Hebrew PDF fixture, assert `get_text("blocks")` returns the expected string. This test is gated `@pytest.mark.packaging` and runs in the release CI pipeline only (not on every commit).
  - `requirements.txt` (and any `pyproject.toml`/`requirements-desktop.txt`) pins `pymupdf>=1.24,<2.0`.
  Planner verifies the exact `.spec` location during research phase.

- **D-44: PyMuPDF Hebrew runtime test fixture.** Per D-02 Codex revision: REQ-4 dead-code tests pass without exercising v1's actual extraction path. Add `tests/fixtures/local_indexer/hebrew_sample.pdf` (small, multi-column or single-column Hebrew PDF — planner picks or creates) + `tests/test_local_indexer.py::test_pymupdf_hebrew_extraction_quality` asserting expected reading-order + paragraph segmentation via `get_text("blocks")`. Fixture committed alongside the test. Documented expected output stored next to the fixture as `.expected.txt`.

- **D-45: Export-path handling for LOCAL hits.** Per D-11 Codex revision: reusing `COL_SRC` propagates `display.source = 'LOCAL'` through to export paths. Decisions:
  - **Desktop xlsx export (`genizah_app.py:export_results('xlsx')`):** LOCAL rows ARE included in the local file (this is a desktop-side export the user explicitly initiated; it's NOT cloud-bound). The Source column shows `LOCAL`; the Library column shows `My Library`. Manuscripts sub-sheet may include LOCAL rows since they have a sys_id, but the PGP-URL / PGP-Description / NLI-Description / Library-Viewer-URL cells are empty (no upstream metadata for LOCAL).
  - **Web xlsx export (`web/export_service.py`):** Web Tantivy has no LOCAL data, so LOCAL rows never reach this path. But for defense-in-depth, `shared/export_dossier.py` row builders gain a `skip_local: bool = False` parameter; web sets `skip_local=True`, desktop sets `skip_local=False`. Behavior pinned via `tests/test_export_dossier_local_handling.py`.
  - **JSON `/api/search` export:** Per REQ-9, serializer drops LOCAL — `shared/search_serializer.py:_serialize_item` filters out items with `is_local_sys_id(sys_id)` regardless of any other flag.
  - **Lists export to cloud:** Per D-30, never reached — gate is at top of `sync_item_to_cloud` and `sync_list_to_cloud`.

- **D-46: `LIBRARY_CODES` web-consumer static guard.** Per Codex insight on D-30/D-31: adding `"LOCAL"` to shared `LIBRARY_CODES` requires every web library-option builder to opt out. Add `tests/test_web_library_options_no_local.py` (static AST scan, mirroring `tests/test_pgp_filter_cascade.py`) that scans every web module under `web/pages/` for code constructing library-filter dropdowns from `LIBRARY_CODES`; asserts each one filters out `code == "LOCAL"`. Prevents future regressions when new library-list consumers are added.

### Claude's Discretion
- **D-07 follow-up:** Exact TXT encoding fallback policy (utf-8-sig only vs utf-8-sig + cp1255 fallback) — planner picks after local smoke tests, records in `95-NN-PLAN.md`.
- **D-12 follow-up:** Exact column position for the new Composition / Parallels Source column — planner inspects existing column layouts and picks.
- **D-32 follow-up:** Final Hebrew translation of the Seewald attribution line — user-reviewed during execute (or planner picks if user is offline).
- **D-44 follow-up:** Selecting/creating the canonical Hebrew PDF fixture for the runtime extraction test — planner picks a small representative example.
- Per-file status panel column widths, button colors, exact toast styling — planner discretion (consistent with existing desktop styling).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase Specification
- `.planning/phases/95-my-library/95-SPEC.md` — **Locked requirements — MUST read before planning.** 10 requirements, boundaries, constraints, 22 acceptance criteria checkboxes.
- `.planning/phases/95-my-library/95-CODEX-CRITIQUE.md` — **MUST read.** Codex (gpt-5.5) critique that drove the D-NN amendments + D-34..D-46 additions in this CONTEXT.md. Two P0s + eight gaps; full rationale.

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
- `seewald_addition/GenizahSearch_Local_Extension.md` — full prototype design doc (Hebrew). Reference for what's being productized + what's intentionally being replaced. **`browse_map.pkl` structure** in this doc is the template for D-34's synthetic `ie_id`/`seq_index` keys.
- `seewald_addition/genizah_make_index.py:67-105` — `_fix_rtl_line` / `_fix_rtl_page` / `_join_fragmented_lines` source (port target per D-02).
- `seewald_addition/genizah_local_indexer.py` — SQLite `processed_files` cache pattern (port target per SPEC REQ-5).

### Core Codebase Anchors
- `genizah_core.py:1723` — `LIBRARY_CODES` table (extension point per D-13).
- `genizah_core.py:2007` — `Config.INDEX_DIR` + portable-mode resolution (extension point per D-14).
- `genizah_core.py:3640-3680` — `parse_header_smart` + `parse_full_id_components` (generalization point per D-13 Codex revision).
- `genizah_core.py:5130-5189` — Tantivy main-index schema definition (LOCAL side-index MUST mirror per SPEC constraint).
- `genizah_core.py:742-790` — `rebuild_lab_index()` LAB schema + builder (LOCAL lab side-index MUST mirror per D-09).
- `genizah_core.py:1292-1349` — `lab_composition_search()` — extension point for querying `local_lab_index` per D-09.
- `genizah_core.py:7390-7391` — main search invokes `_deduplicate` (LOCAL merge happens AFTER this line per D-08 Codex revision).
- `genizah_core.py:7916-7921` — `_deduplicate` body (only V0.8/V0.7 survive; LOCAL hits MUST NOT pass through this function — see D-08).
- `genizah_app.py:3079-3091` — `QTabWidget` tab registration (MyLibraryTab inserts here as 6th tab per SPEC REQ-8).
- `genizah_app.py:5909-5945` — `COL_SRC` + `COL_PGP` column setup (`COL_SRC` reuse target per D-11).
- `genizah_app.py:16534, 16741` — `COL_SRC` write site + visibility rule (extend per D-11).
- `shared/synthetic_sys_id.py` — module template for new `is_local_sys_id` helper.
- `corrections_client.py:619-623` — existing `is_synthetic_sys_id` gate (extend with LOCAL check per SPEC REQ-9).
- `lists_sync.py:736-770` — `sync_item_to_cloud` body; the LOCAL gate MUST insert at line 738+ (BEFORE `_get_client()` at line 742) per D-30 Codex revision.
- `shared/search_serializer.py:_serialize_item` + `web/search_api.py:633-939` — payload serializer (add LOCAL filter per SPEC REQ-9).

### Filter Pattern References
- `web/pages/search.py:1430-1434` — PGP filter button pattern (precedent for the LOCAL three-state filter per D-10, applied to desktop surfaces per SPEC REQ-6).
- `tests/test_pgp_filter_cascade.py` — static AST cascade-discipline guard (template for `tests/test_local_filter_cascade.py` AND `tests/test_web_library_options_no_local.py` per D-46).

### Help / Docs
- `web/pages/help.py` (or current Help page module) — extension point per D-31 + D-33 disclosure.
- Desktop Help dialog (planner identifies the exact file/widget) — extension point per D-31.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`shared/synthetic_sys_id.py`** — pattern template + module to mirror for `is_local_sys_id`. Includes the repo-grep lint test pattern (`tests/test_synthetic_sys_id.py::TestNoIntCoercion`) — replicate for LOCAL sys_ids.
- **`genizah_core.py:5130-5189` Tantivy schema builder** — same fields (`unique_id`, `content`, `content_head`, `content_tail`, `line_starts`, `line_ends`, `source`, `full_header`, `shelfmark`, `scope`, `boundaries`) used for both main and LOCAL side-index. Schema match is a SPEC constraint, not a choice.
- **`genizah_core.py:742-790` `rebuild_lab_index()`** — LAB schema + fingerprint computation template for the parallel LOCAL lab side-index (D-09).
- **`Config.INDEX_DIR` portable-mode resolution** — automatically gives LOCAL side-index the same portability story (D-14).
- **`QSettings` desktop persistence** — used for non-portable UI preferences only after Codex revision; folder-list moved to SQLite per D-15.
- **`COL_SRC` + `COL_PGP` column infrastructure** — sortable, fixed-width, color-coded foreground pattern (`#27ae60` for PGP). LOCAL reuses `COL_SRC` with `#3498db` blue (D-11).
- **`browse_map` per-sys_id page list** — extends to LOCAL files page-by-page (D-27 browse-panel reuse). D-34 specifies the LOCAL-specific browse_map entry shape including synthetic `ie_id` + `seq_index`.
- **Phase 93 `safe_storage` persist_value pattern** — model for the LOCAL filter state persistence (cycling state across sessions). D-39 specifies per-surface keys.
- **`corrections_client.py:619-623` `is_synthetic_sys_id` gate** — direct extension point per SPEC REQ-9; adding LOCAL is an `OR` with the existing check.
- **`_deduplicate()` at `genizah_core.py:7916`** — known to drop non-V0.8/V0.7 sources. LOCAL hits MUST merge AFTER its call site (D-08 Codex revision).

### Established Patterns
- **Shared helpers in `shared/`** consumed by both apps (Phase 94 invariant). `shared/local_indexer.py` is desktop-only but the `is_local_sys_id` helper + cloud-write gates live in `shared/` so the web app correctly rejects LOCAL too.
- **Per-test CI pin** for invariants (`tests/test_no_raw_storage_access.py`, `tests/test_pgp_filter_cascade.py`, `tests/test_export_xlsx_cross_parity.py`). New tests per this phase: `tests/test_local_sys_id_namespace.py`, `tests/test_side_index_merge.py`, `tests/test_local_filter_cascade.py`, three `test_local_namespace_no_*_leak.py` files (SPEC REQ-9), `tests/test_local_post_dedup_merge.py` (D-08), `tests/test_local_sys_id_parser_compat.py` (D-13), `tests/test_local_delete_by_uid.py` (D-20), `tests/test_local_two_phase_commit.py` (D-21), `tests/test_local_indexer_mutex.py` (D-25), `tests/test_local_index_open_fallback.py` (D-37), `tests/test_local_lab_invalidation.py` (D-38), `tests/test_local_filter_persistence.py` (D-39), `tests/test_local_unavailable_folder.py` (D-40), `tests/test_canonical_filepath.py` (D-42), `tests/test_folder_overlap_detection.py` (D-17), `tests/test_export_dossier_local_handling.py` (D-45), `tests/test_web_library_options_no_local.py` (D-46), `tests/test_local_indexer.py::test_pymupdf_hebrew_extraction_quality` (D-02/D-44).
- **18-digit numeric sys_id namespacing**: `99`-prefix for synthetic Genizah (Phase 85), `97`-prefix reserved for LOCAL (this phase). Format is digits-only, length-locked. `parse_header_smart` + `parse_full_id_components` MUST be generalized to accept both prefixes per D-13 Codex revision.
- **Three-state filter cascade discipline**: filter applied AFTER existing filters (printed, PGP, exclusions, refinement). No re-query; post-search render-time only. Pinned by static AST tests. D-10's no-op behavior when no LOCAL hits is an ADDITIONAL invariant on top of the cascade rules.
- **Per-user state through chokepoint** (Phase 87): not directly applicable (desktop has no `app.storage.user`), but the principle (single source of truth, no scattered raw reads) extends to QSettings access — planner should consider whether a `desktop/settings.py` wrapper around `QSettings` is warranted.

### Integration Points
- **`genizah_app.py:3079-3091`** — `QTabWidget.addTab()` line registers MyLibraryTab as 6th tab.
- **Main search query path** (`genizah_core.py` Tantivy searcher) — extended to query both indexes and merge POST-dedup via RRF per D-08.
- **`lab_composition_search()` at `genizah_core.py:1292-1349`** — extended to query `local_lab_index` per D-09 with weights-hash invalidation per D-38.
- **`_deduplicate` call at `genizah_core.py:7390`** — LOCAL hits merge AFTER this line (D-08 Codex P0 fix).
- **`parse_header_smart` + `parse_full_id_components` at `genizah_core.py:3640-3680`** — generalized to accept `97`-prefix LOCAL sys_ids per D-13 Codex P0 fix.
- **`genizah_app.py:16534`** (and Composition / Parallels result-render call sites) — extended to write `source='LOCAL'` and color the cell blue for LOCAL hits (D-11).
- **`genizah_app.py:16741`** — visibility-rule extension for `COL_SRC` when LOCAL hits present (D-11).
- **`corrections_client.py:619-623`** — extend the existing synthetic-sys_id gate with LOCAL check (REQ-9).
- **`lists_sync.py:736+`** — LOCAL gate inserted at TOP of `sync_item_to_cloud` BEFORE `_get_client()` per D-30 Codex P0 fix; same gate at top of `sync_list_to_cloud`.
- **`shared/search_serializer.py:_serialize_item`** — add LOCAL filter (REQ-9 defense-in-depth).

</code_context>

<specifics>
## Specific Ideas

- **Browse-panel text-only mode (D-27)** — user explicitly wants LOCAL hits to work like Genizah hits (same browse navigation), just without the image pane. Planner should look for the existing "no image" branch in the Browse panel (e.g., when NLI image fetch fails) and reuse that rendering mode rather than adding a new branch.
- **`COL_SRC` reuse with blue color (D-11)** — user noted the existing Src column during discussion. The reuse approach is preferred over adding a new badge column. Blue (`#3498db`) chosen to match the existing PGP-green pattern (`#27ae60`); planner picks the exact shade if `#3498db` clashes with anything else.
- **Multi-folder expansion of SPEC default (D-16)** — user explicitly chose multi-folder despite SPEC's single-folder default. Recorded as additive: SPEC requirements still apply per-folder; folder list lives in SQLite per D-15 Codex revision (not QSettings).
- **PyMuPDF only (D-01)** — user prioritizes Hebrew-extraction quality over file-format coverage breadth. The dead-code helpers (D-02) preserve future optionality without invoking them in v1. D-43 covers PyInstaller packaging; D-44 covers runtime quality testing.
- **Seewald attribution in About + Help (D-32)** — user wants visible credit in both apps, both languages, on ship.
- **Codex critique findings (post-discuss)** — Two P0 issues (D-08 dedup ordering + D-30 lists_sync gate placement) and eight gaps (D-34..D-46) folded back into CONTEXT.md before planning. The planner has explicit fix locations + acceptance tests for every issue.

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
- **Content-addressed sys_id (file-content SHA256 dedup)** — D-18 picks filepath-based hashing for v1. `local_files.sha256_full` column reserved per D-35. Content-addressed dedup is a follow-up if users surface duplicate-file-across-folders pain.
- **Encrypted side-index** — D-33 leaves cleartext-on-disk with explicit Help disclosure. If a privacy concern surfaces post-ship, revisit (substantial Tantivy plumbing).
- **Reveal in Explorer button** — D-28 ships `Open file` only. Add `Open containing folder` if user demand.
- **Portable-friendly UI-pref store** — D-39 notes that per-surface filter state lives in QSettings (non-portable); if portable-mode users surface pain, mirror the keys into a SQLite `ui_prefs` table.
- **`local_files.sha256_full` content dedup** — column reserved per D-35; future phase populates and enables cross-folder dedup.

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
*Revised post-Codex critique: 2026-05-21*
