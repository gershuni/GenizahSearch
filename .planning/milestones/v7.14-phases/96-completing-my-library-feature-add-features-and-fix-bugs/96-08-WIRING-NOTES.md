# Phase 96 Plan 96-08 + Plan 96-06 Wiring Notes — Pinned Attribute Names

**Revision date:** 2026-05-24 (Wave 0)
**Source-verified against:** `desktop/result_dialog.py`, `genizah_app.py`, `desktop/my_library_tab.py`, `shared/local_indexer.py`, `shared/local_sys_id.py` at commit `HEAD~` on 2026-05-24
**Purpose:** close checker BLOCKER 2 (plan 96-08) + Codex MEDIUM #10 (plan 96-06) from revision iteration 1 — plans 96-08 and 96-06 must NOT use `hasattr` fallback chains for the names below. Use the exact identifiers pinned here.

---

## ResultDialog (desktop/result_dialog.py)

### Text widget for rendering page content
- **Attribute name:** `self.text_ms`
- **Render call:** `apply_line_numbered_text(self.text_ms, self._htmlify(text), source_text=text, is_html=True)`
- **Why not setHtml(format_snippet(...)) :** v7.12.0 introduced the line-number gutter (Phase 999.4); the gutter helper preserves line numbering across the LOCAL render path. Direct `setHtml` BYPASSES the gutter.
- **Source:** desktop/result_dialog.py:2066, 2258 (Genizah path) — LOCAL path MUST mirror this.

### Per-page navigation buttons (existing — reuse for LOCAL)
- **Prev (compact bar):** `self.btn_compact_pg_prev` — desktop/result_dialog.py:118-121
- **Next (compact bar):** `self.btn_compact_pg_next` — desktop/result_dialog.py:128-131
- **Spin (page-number jump):** `self.spin_page` — desktop/result_dialog.py:237
- **Total-pages label:** `self.lbl_total` — desktop/result_dialog.py:241-245 (set via `self.lbl_total.setText(f"/ {total}")`)
- **NOTE:** the nav-row buttons at line 236, 238 (`btn_pg_prev`, `btn_pg_next`) are LOCAL VARIABLES, not stored on `self`. Use only the `btn_compact_pg_*` pair for state changes from `load_local_page`.

### Page-load state attributes
- **Current page number:** `self.current_p_num` (1-based int)
- **Current internal index:** `self.current_internal_idx` (0-based int)
- **Current sys_id:** `self.current_sys_id`
- **Source:** desktop/result_dialog.py:2207-2208 sets both after fetching page_data.

### Genizah-specific calls to OMIT in load_local_page
- **`self.cancel_image_thread()`** — desktop/result_dialog.py:2171. This cancels the IIIF image fetch thread used for Genizah folio images. LOCAL files have no IIIF images (CONTEXT D-27 text-only mode). Wrap in try/except and skip silently for LOCAL, OR explicitly check `is_local_sys_id` and don't call.
- **`self.btn_external_view.isChecked()` → `sync_external_view`** — Genizah-image-specific (line 2239-2240).

### Button being REMOVED in plan 96-07 (DO NOT TOUCH from 96-08)
- `self.btn_rd_open_browse` (declared at desktop/result_dialog.py:343-352) — plan 96-07 removes this entirely.

### NEW-1 confirms KEEPING this button (it is the `עיין` Browse button)
- `self.btn_view_transcription` (declared at desktop/result_dialog.py:248, connected to `self.open_full_transcription` at line 250)

---

## Main App / Browse Panel (genizah_app.py)

### Browse-panel text widget
- **Attribute name:** `self.browse_text` — genizah_app.py:7021 (declared `QTextEdit()`)
- **Render call (Genizah pages):** `apply_line_numbered_text(self.browse_text, full_html, pages=raw_text_parts, is_html=True)` — see genizah_app.py:9799, 10460
- **Render call (LOCAL view-all today):** `apply_line_numbered_text(self.browse_text, f"<div dir='rtl'>{browse_html}</div>", source_text=text, is_html=True)` — genizah_app.py:18615 (existing `_open_local_browse`)
- **Per-page LOCAL render MUST also use `apply_line_numbered_text` on `self.browse_text`** — this preserves the v7.12.0 gutter. See must_haves below.

### Manuscript-level Browse navigation (existing — reuse for LOCAL prev/next-file)
- **Prev manuscript:** `self.btn_prev_ms` — genizah_app.py:6507-6510 (`navigate_manuscript(-1)`)
- **Next manuscript:** `self.btn_next_ms` — genizah_app.py:6512-6515 (`navigate_manuscript(1)`)

### Per-page / per-chunk Browse navigation — DO NOT EXIST YET
**The Browse panel has NO per-page/per-chunk buttons today.** v7.14.0 uses the "View All" model (`self.btn_b_all` toggle at genizah_app.py:6546-6549 toggles between single-folio Genizah view and full-manuscript concatenation).

**Plan 96-08 MUST CREATE these widgets** (do NOT silently fall through with `hasattr`):
- New: `self.btn_local_browse_prev` (LOCAL per-page prev) — wire to `_open_local_browse_page(sys_id, p_num=current-1)`
- New: `self.btn_local_browse_next` (LOCAL per-page next) — wire to `_open_local_browse_page(sys_id, p_num=current+1)`
- New: `self.lbl_local_browse_page` (label e.g. "page 3 / 12" or "chunk 3 / 12")
- New: `self.btn_local_browse_view_toggle` (already in 96-08 plan)

**Insertion site for these widgets:** the existing Browse top-row controls layout in `_build_browse_tab` (around genizah_app.py:6498+). The simplest layout is a NEW horizontal row created lazily inside `_open_local_browse_page` and added to the Browse panel's top container — visible only when a LOCAL file is loaded, hidden otherwise (mirror the visibility pattern of `self.btn_rd_open_file` in result_dialog.py).

### Existing Browse-panel buttons (preserve)
- `self.btn_browse_by_list` — genizah_app.py:6503
- `self.btn_browse_go` — genizah_app.py:6522
- `self.btn_find_parallels` — genizah_app.py:6534
- `self.btn_b_save` — genizah_app.py:6543
- `self.btn_b_all` — genizah_app.py:6546 (Genizah View All toggle)
- `self.btn_b_add_to_view` — genizah_app.py:6561
- `self.btn_b_add_to_puzzle` — genizah_app.py:6568
- `self.btn_browse_add_to_list` — genizah_app.py:6574

### Browse-panel info label (for LOCAL filename display)
- `self.browse_info_lbl` — used at genizah_app.py:18632 (`self.browse_info_lbl.setText(f"<b>{basename}</b> ({tr('Local file')})")`)

### LOCAL Browse-panel state (existing — preserve)
- `self.current_browse_sid` — set at genizah_app.py:18592
- `self.current_browse_p` — set at genizah_app.py:18593
- `self.current_browse_internal_idx` — set at genizah_app.py:18594
- `self.current_browse_volume_ie` — set at genizah_app.py:18595
- `self._current_local_filepath` — set at genizah_app.py:18607
- `self.browse_open_file_btn` — used at genizah_app.py:18608-18610

### LOCAL data primitives (genizah_app.py)
- `self._lookup_local_filepath(sys_id)` — genizah_app.py:18507+ — returns canonical filepath or None
- `self._get_local_full_text_for_sys_id(sys_id)` — genizah_app.py:18507-18552 — aggregates all LOCAL pages into one string (existing — plan 96-08 modifies this to use `_aggregate_local_pages_with_separators`)

### Language detection (genizah_app.py)
- **CORRECT name:** `CURRENT_LANG` (module-level global imported from genizah_core)
- **Import line:** `from genizah_core import Config, MetadataManager, ..., CURRENT_LANG, ...` at genizah_app.py:34
- **Usage:** `if CURRENT_LANG == 'he': ...` — see genizah_app.py:252, 552, 652, etc. (many call sites)
- **DO NOT use `self.lang` or `self._current_ui_lang`** — these do NOT exist on the app.
- **For helpers in genizah_app.py module:** import `CURRENT_LANG` at the top and read it directly. Module-level helpers can take an explicit `lang` parameter (recommended for `_aggregate_local_pages_with_separators`).

---

## Cross-cutting

### Indexer access (shared/local_indexer.py)
- **LocalIndexer attribute on app:** `self.my_library_tab._indexer` — see genizah_app.py `_lookup_local_filepath`
- **Public method to enumerate on-disk filepaths:** **DOES NOT EXIST as of 2026-05-24.** Plan 96-06's rescan callback must EITHER:
  - (A) Add `LocalIndexer.list_all_filepaths(self) -> list[str]` method to `shared/local_indexer.py` (RECOMMENDED — public API, clean). Implementation: `return [r[0] for r in self._conn.execute("SELECT filepath FROM local_files").fetchall()]`
  - (B) Reach into `indexer._conn` and `SELECT filepath FROM local_files` directly (works but less clean).
- **96-06 plan currently picks (A) with (B) as fallback** — see plan 96-06 Task 1 Step 5. The fallback path is acceptable but the executor should PREFER (A).

### local_files SQLite table columns (verified 2026-05-24)
- `sys_id` (TEXT PK)
- `filepath` (TEXT) — canonical filepath
- `folder_id` (FK)
- `extraction_status` (TEXT)
- Others (mtime, size_bytes, etc.) — see shared/local_indexer.py:235 CREATE TABLE statement

### Session JSON nesting (genizah_app.py:_save_session)
- **Top-level keys** (cross-surface, e.g., `pre_search_filters`, `word_excluded_sys_ids`, `active_tab`, `browse_shelfmark`, `browse_catalog`, `was_interrupted`, `post_measurement_filters`): see genizah_app.py:23586-23607
- **Nested in `regular_search` dict:** `local_filter`, `printed_filter`, `excluded_sys_ids`, `printed_ids`, `excluded_shelfmarks`, `excluded_raw_entries`, `exclusion_sources`, `results_filters`, `filter_sources`, `filter_enabled_sources`, `refinement_chain`, etc. — see genizah_app.py:23544-23564
- **Nested in `composition_search` dict:** `local_filter_composition`, `local_filter_parallels`, `domain_exclusions`, `printed_filter` (composition variant), etc. — see genizah_app.py:23565-23583
- **CONCLUSION (W6 closure):** the Phase 95 LOCAL filter keys are **NESTED in their respective surface dict**, NOT top-level. Plan 96-04 places `local_file_optouts` at **top level** (cross-surface) because it is shared by Search/Composition/Parallels — that placement is correct, but it does NOT match the position of the existing `local_filter*` keys.

---

## Plan 96-06 wiring

REVISION 2026-05-24 — Codex MEDIUM #10 closure. These identifiers are consumed
by Plan 96-06 (file-opt-out tree widget). Source-verified against
`desktop/my_library_tab.py` and `genizah_app.py` on 2026-05-24.

### Folder-list QListWidget — selection plumbing
- **List widget attribute on MyLibraryTab:** `self._folder_list`
- **Signal for folder selection:** `self._folder_list.currentItemChanged`
  (alternative: `self._folder_list.itemSelectionChanged` — preserve whichever
  Phase 95 connected; do NOT introduce a NEW signal)
- **Folder path stashed in:** `Qt.ItemDataRole.UserRole` on each
  QListWidgetItem (Phase 95 sets this when populating the list)
- **Extraction in handler:** `selected_path = item.data(Qt.ItemDataRole.UserRole)`
- **DO NOT use:** `item.text()` (that returns the display label, not the
  filesystem path — they differ when the basename collides across folders)

### LocalIndexerWorker scan-complete plumbing (Phase 95 D-25)
- **Callback method on MyLibraryTab:** `_on_indexer_finished`
- **Signal connection:** `worker.finished.connect(self._on_indexer_finished)`
  is performed in `MyLibraryTab.__init__` (Phase 95) — do NOT re-connect
- **Indexer instance attribute on tab:** `self._indexer` (LocalIndexer object)
- **Indexer mutex (Phase 95 D-25):** `self._indexer_mutex` (QMutex) — used
  by Phase 95 to serialize scan invocations; Plan 96-06 should NOT acquire
  this mutex inside the scan-complete callback (we are already on the UI
  thread by the time `finished` fires; the worker has released).

### LocalIndexer public API (shared/local_indexer.py)
- **Per-sys_id lookup:** `indexer.get_filepath(sys_id) -> str | None`
- **Enumerate all on-disk filepaths:** **DOES NOT EXIST.** Plan 96-06 must
  EITHER:
  - (A) RECOMMENDED — add a new public method `LocalIndexer.list_all_filepaths(self) -> list[str]`:
    ```python
    def list_all_filepaths(self) -> list[str]:
        """Phase 96 D-F1: enumerate every on-disk filepath in the index.
        Used by MyLibraryTab._on_indexer_finished to prune stale opt-outs."""
        cur = self._conn.execute("SELECT filepath FROM local_files")
        return [row[0] for row in cur.fetchall()]
    ```
  - (B) FALLBACK — query `indexer._conn` directly with `SELECT filepath FROM local_files`.

### Canonical filepath
- **Helper:** `shared/local_sys_id.py::_canonical_filepath(p) -> str`
- **Apply at:** populate time (when stashing the leaf's UserRole value)
  AND at any external comparison (e.g., when looking up by filepath in
  `_local_file_optouts` set). DO NOT compare raw os.path.join output —
  Windows case + slash drift will make set membership unreliable.

### Re-filter trigger (consumed by Plan 96-06's _commit_changes)
- **App attribute:** `self._app` on MyLibraryTab (the main QMainWindow)
- **Re-filter method on app:** `_reapply_filters_for_optout_change()` (added
  by Plan 96-06 Task 2)
- **Session-save method on app:** `_save_session()` (Phase 95 + Plan 96-04)

### MyLibraryTab `_app` reference
- `MyLibraryTab.__init__(parent, app, ...)` stores the app instance on
  `self._app` (Phase 95). All cross-component calls go through this
  reference; never reach for `QApplication.instance()`.

---

## Resolution by checker issue

- **BLOCKER 2** → Pin all load-bearing attribute names above. Plan 96-08 must use these exact identifiers; no `hasattr` fallback chains for the pinned names.
- **W6** → `local_filter` is nested in `regular_search` (NOT top-level). `local_file_optouts` SHOULD be top-level (cross-surface). Documented above.
- **W8** → Public method approach (A) preferred over direct `_conn` access (B). Documented above.
- **W9** → LOCAL render path uses `apply_line_numbered_text` on `self.text_ms` (ResultDialog) / `self.browse_text` (Browse panel). v7.12.0 gutter preserved. `cancel_image_thread()` is Genizah-specific.
- **W11** → Plan 96-08 must_haves now include the line-number-gutter preservation invariant.
- **W12** → Browse panel per-page nav buttons do NOT exist; plan 96-08 creates them.
- **REVISION 2026-05-24 — Codex MEDIUM #10** → Plan 96-06 wiring identifiers (`_folder_list`, `currentItemChanged`, `Qt.ItemDataRole.UserRole`, `_on_indexer_finished`, `_indexer`, `_indexer_mutex`, `list_all_filepaths`, `_canonical_filepath`, `_app`, `_reapply_filters_for_optout_change`, `_save_session`) pinned in `### Plan 96-06 wiring` section above. No `hasattr` discovery in Wave 3.
