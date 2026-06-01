# Phase 100: LOCAL PDF Image in ResultDialog + Browse - Context

**Gathered:** 2026-05-27
**Status:** Ready for planning

<domain>
## Phase Boundary

Wire the Phase 99 `PdfRenderWorker` into the **two desktop surfaces** so a LOCAL **PDF** hit shows its rendered page image beside the extracted text, staying in sync as the user navigates:
- **`desktop/result_dialog.py` (`ResultDialog`)** — the (currently hidden-for-LOCAL) toggle-able external pane shows the rendered page; prev/next *result* re-renders for the newly shown hit (PDFIMG-03).
- **Browse panel (`genizah_app.py::create_browse_tab`, `self.browse_viewer`)** — the (currently hidden-for-LOCAL) image pane shows the rendered page; prev/next *page* updates the image in sync with the text (PDFIMG-04).

Both surfaces feed the `QImage` into the **existing `ManuscriptViewerWidget.display_image()`** (Phase 99 D-02 — no new pane, zoom/pan/rotate come free). This phase also builds the small **UI controller** that Phase 99 deliberately left out: the monotonic **token counter**, **latest-wins discard** of stale results, and the **~8s `QTimer` timeout watchdog**.

**NOT in this phase:** any change to the render core / worker / LRU (Phase 99 owns those), OCR (D-F2 deferred), disk caching, re-render-on-zoom, adaptive DPI, or rendering any non-PDF file type. Non-PDF LOCAL files (`.docx`/`.html`/`.xlsx`/`.csv`/`.txt`) stay text-only with the image pane hidden (PDFIMG-05).
</domain>

<decisions>
## Implementation Decisions

### Loading & failure placeholders (PDFIMG-03/04/06)
- **D-01 (loading state):** On navigation, immediately show the viewer's **existing status-message placeholder** ("Loading…" via `ZoomableScrollArea.set_status_message` / `set_image(null)` path, viewers.py ~156-205), then swap in the page when `render_succeeded` fires. Do **not** keep the previous page visible and do **not** blank to empty.
- **D-02 (failure granularity):** Map **each** `PdfRenderFailure` reason to a short, human placeholder message rather than one generic string. Reason → message (final wording is Claude's discretion, intent locked):
  - `MISSING_FILE` → "File not found"
  - `NOT_PDF` → (should not occur — extension-gated upstream; treat defensively as generic)
  - `ENCRYPTED` → "PDF is password-protected"
  - `CORRUPT` → "Could not open this PDF"
  - `PAGE_OUT_OF_RANGE` → "Page not found in file"
  - `RENDER_ERROR` → "Could not display this page"
  - `TIMEOUT` (emitted by this phase's watchdog, not the worker) → "Rendering timed out"
  - `CANCELLED`/stale → no placeholder (discarded silently by latest-wins; not user-facing)
  Every failure is already logged by the worker; the controller logs the watchdog `TIMEOUT`.
- **D-03 (placeholder language):** Localize each placeholder/failure string by the existing global **`CURRENT_LANG`** (`'he'` vs `'en'` — see `genizah_app.py` usages e.g. `is_hebrew = (CURRENT_LANG == 'he')`). Provide a Hebrew and English variant per message, select by `CURRENT_LANG`. If `CURRENT_LANG` is somehow unavailable in a surface's scope, fall back to bilingual "HE / EN" per the About/Help convention.

### Re-render timing on navigation (PDFIMG-03/04)
- **D-04 (debounce):** Apply a **~150ms debounce** before enqueuing a render, matching Browse's existing image debounce (`ManuscriptViewerWidget.set_page` 150ms timer, viewers.py ~1160). This skips wasted 200 DPI renders of pages blown past during key-repeat. Latest-wins token echo (Phase 99 D-03) still discards any stale *result* that does arrive. This is a **controller-side** debounce — it does NOT contradict Phase 99 D-03 ("no debounce **in the worker**").
- **D-05 (consistent policy):** Use the **same** timing/debounce policy identically in ResultDialog and Browse — predictable behavior, one code path.
- **D-06 (sync guarantee):** The displayed image always tracks the currently displayed page/hit: in ResultDialog the render uses `current_sys_id` + `current_p_num` of the shown result (re-fired on prev/next result AND on within-document `load_local_page`); in Browse it uses the current browsed `sys_id` + page `p_num` (re-fired on prev/next page + page combo change). Page is **1-based `p_num`** passed straight to `enqueue` (worker converts to `fitz` index `page_num − 1`).

### Worker scope & lifecycle (recommended default — locked, not deep-discussed)
- **D-07:** **One shared, app-level `PdfRenderWorker`** (single render thread + single 4-doc `DocLRU`) reused by BOTH ResultDialog and Browse, owned by the main `GenizahGUI`. The transient `ResultDialog` borrows the app's worker rather than creating its own. Rationale: single 4-doc LRU is more memory-efficient and honors Phase 99 D-09a single-owner-fitz discipline (one thread owns all `fitz` access); avoids spinning a render thread per dialog open/close.
- **D-07a (shutdown):** The shared worker is `stop()`'d on app close via the existing `GenizahGUI.closeEvent` (same place the Phase 97 `sweep_running_scan_runs()` cleanup lives). Cooperative `stop()` only — no `terminate()` (Phase 99 D-05).
- **D-07b (token counter):** The monotonic generation token counter lives on the **controller** (a thin helper, e.g. on `GenizahGUI` or a small `PdfImageController`), shared so each surface's latest-wins comparison works against the single shared worker. Each `enqueue` gets the next token; a surface discards a `render_succeeded`/`render_failed` whose token is not its most recent request.

### Image pane visibility & toggle (recommended default — locked, not deep-discussed)
- **D-08:** **Extension-gated auto-show.** On opening a LOCAL hit whose `file_extension == '.pdf'`: reveal the (previously hidden-for-LOCAL) viewer pane and render. For any non-PDF LOCAL file: keep the pane hidden and make **no** render attempt (PDFIMG-05). The existing image-toggle buttons (`ResultDialog` external-pane toggle; `btn_b_toggle_img` in Browse) remain functional for PDFs so the user can still hide the pane; for non-PDF LOCAL the toggle is hidden/disabled (no image to show). Genizah (non-LOCAL) behavior is unchanged.

### Claude's Discretion
- Exact placeholder wording (HE + EN) per reason; whether the controller is a standalone `PdfImageController` class vs methods on `GenizahGUI`; debounce timer ownership (per-surface `QTimer` vs shared); precise toggle-button hide-vs-disable choice for non-PDF; how `filepath` is resolved at enqueue time (`get_filepath(sys_id)` vs the already-cached `_lookup_local_filepath` / `_current_local_filepath` paths the surfaces already hold).
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap
- `.planning/REQUIREMENTS.md` — PDFIMG-03 (ResultDialog), PDFIMG-04 (Browse), PDFIMG-05 (non-PDF text-only), PDFIMG-06 (graceful failure, owned by Phase 99 but this phase surfaces the placeholders).
- `.planning/ROADMAP.md` §"Phase 100: LOCAL PDF Image in ResultDialog + Browse" — goal + 4 success criteria.

### Phase 99 (the renderer this phase wires in — READ FIRST)
- `.planning/phases/99-pdf-page-renderer/99-CONTEXT.md` — locked decisions that carry forward: D-02 (reuse `ManuscriptViewerWidget`), D-03 (latest-wins token), D-04 (failure enum → Phase 100 maps to placeholder text), D-05 (~8s UI-side `QTimer` watchdog is THIS phase's job), D-07 (signal shapes).
- `.planning/phases/99-pdf-page-renderer/99-02-SUMMARY.md` — final `PdfRenderWorker` API: `enqueue(token, sys_id, page_num, filepath) -> bool`, `stop(timeout_ms=5000)`, signals `render_succeeded(int token, str sys_id, int page_num, QImage)` / `render_failed(int token, str sys_id, int page_num, object reason, str detail)`; token counter + watchdog explicitly left to Phase 100.
- `.planning/phases/99-pdf-page-renderer/99-VERIFICATION.md` — confirms threading model (a): single dedicated render thread, single-owner fitz (D-09a).

### Implementation surfaces (desktop)
- `desktop/pdf_page_renderer.py` — `PdfRenderWorker` (signals ~378-379, `enqueue` ~391, `stop` ~474, `run` ~446), `DocLRU` (~191, maxsize=4), `render_page`/`render_via_lru` (~262/~318, 1-based page_num), `PdfRenderFailure` enum (~88-109, 8 reasons), `RENDER_DPI = 200` (~81).
- `desktop/result_dialog.py` — `ResultDialog.__init__` (line 49), `load_result_by_index` (~1993), LOCAL detection `is_local_sys_id` (~2011-2013), `load_page`/`load_local_page` (~2255/~2404), `self.ms_viewer` external pane (~544-561), filepath via `self._app._lookup_local_filepath(sys_id)` (~2018), `current_sys_id` / `current_p_num` state.
- `genizah_app.py` — `create_browse_tab` (~6563), `self.browse_viewer = ManuscriptViewerWidget()` (~7128), image toggle `btn_b_toggle_img` (~6825) + `toggle_browse_image` (~6830), `_browse_prev_next` (~6794), `on_browse_page_combo_changed` (~6799), LOCAL "Open file" + `_current_local_filepath` (~6845-6849), `CURRENT_LANG` global (e.g. ~11013), `GenizahGUI.closeEvent` (worker shutdown site).
- `desktop/viewers.py` — `ManuscriptViewerWidget.display_image(QImage)` (~1209), `ZoomableScrollArea.set_image(pixmap)` + null-pixmap "No Image" placeholder (~156-197), `set_status_message(text)` (~199-205) ← the loading/failure placeholder mechanism (D-01/D-02), `set_page` 150ms debounce (~1142-1207) ← D-04 reference.
- `shared/local_indexer.py` — `get_filepath(sys_id) -> Optional[str]` (~1343), `extract_pdf_pages()` 1-based page_num (~672-721), `_SUPPORTED_EXTENSIONS` set (line 110) ← extension gating (D-08), `local_files.file_extension` / `local_pages.page_num` schema (~574-600).

### Established conventions to mirror
- `desktop/image_loader.py::ImageLoaderThread` — cancel/cooperative pattern the worker already mirrors; controller's latest-wins is the equivalent of `ImageLoaderThread.cancel()`.
- Phase 97 `sweep_running_scan_runs()` wired into `GenizahGUI.closeEvent` — the precedent location for D-07a worker shutdown.
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`ManuscriptViewerWidget`** already instantiated in BOTH surfaces (`ResultDialog.ms_viewer`, `GenizahGUI.browse_viewer`) — Phase 100 only needs to feed it `display_image(QImage)` and toggle visibility; no new widget.
- **`ZoomableScrollArea.set_status_message()` / null-pixmap path** gives the loading + failure placeholder UI for free (D-01/D-02).
- **`get_filepath(sys_id)`** + the surfaces' already-held `_lookup_local_filepath` / `_current_local_filepath` resolve the PDF path needed for `enqueue`.
- **`_SUPPORTED_EXTENSIONS` / `local_files.file_extension`** — canonical "is this a PDF" check (`ext.lower() == '.pdf'`) for D-08 gating.
- **`CURRENT_LANG` global** — drives D-03 localized placeholder text.

### Established Patterns
- Both surfaces already distinguish LOCAL vs Genizah hits (`is_local_sys_id`) and already hide the image pane for LOCAL — Phase 100 flips that to "show for LOCAL **PDF**, keep hidden for non-PDF LOCAL".
- Browse's image load already debounces 150ms — D-04 reuses that interval for render enqueue.
- `fitz` already runs off the UI thread (indexing) without a separate process — Phase 99's single-thread worker (D-07) follows the same proven discipline.

### Integration Points
- `GenizahGUI` owns the single shared `PdfRenderWorker` (D-07), started lazily/at init, `stop()`'d in `closeEvent` (D-07a).
- `ResultDialog` borrows the app worker via its `self._app` reference (already used for `_lookup_local_filepath`).
- Controller (token counter + latest-wins + ~8s QTimer watchdog) is new code introduced by this phase — exact shape (class vs methods) is Claude's discretion.
</code_context>

<specifics>
## Specific Ideas

- The user explicitly chose **"Loading…" status text** over keeping the previous image, and **per-reason** failure messages over a generic one — the researcher should treat clear, specific feedback as a priority, consistent with the Phase 99 user driver ("reuse the zoom controls, make it genuinely useful").
- Language: **match `CURRENT_LANG`** rather than always-bilingual — desktop already keys display off this global, so placeholders should too.
- Timing: user accepted the **150ms debounce** to avoid wasted 200 DPI renders during key-repeat, applied **identically** in both surfaces.
</specifics>

<deferred>
## Deferred Ideas

- **Per-surface differing timing policy** (immediate in ResultDialog, debounced in Browse) — considered and rejected in favor of one consistent policy (D-05).
- **Re-render-on-zoom / adaptive DPI** — already a Phase 99 milestone-level deferral; unchanged.
- Reviewed todos (`migrate-desktop-corrections-fetch`, `fill-missing-genizah-manuscripts-from-fist`) surfaced only as generic keyword matches — **not folded**, unrelated to PDF image display.
</deferred>

---

*Phase: 100-local-pdf-image-in-resultdialog-browse*
*Context gathered: 2026-05-27*
