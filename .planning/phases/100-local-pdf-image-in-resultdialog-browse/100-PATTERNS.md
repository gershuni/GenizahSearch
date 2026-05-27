# Phase 100: LOCAL PDF Image in ResultDialog + Browse - Pattern Map

**Mapped:** 2026-05-27
**Files analyzed:** 3 modified (`genizah_app.py`, `desktop/result_dialog.py`, possibly a new small controller) + 1 read-only contract (`desktop/pdf_page_renderer.py`)
**Analogs found:** 6 / 6 integration points

> This phase has NO brand-new files except possibly one small UI controller (Claude's discretion: standalone `PdfImageController` class vs methods on `GenizahGUI`). Everything else is wiring into existing surfaces. Each integration point below names its closest in-repo analog with concrete excerpts the planner can copy.

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| UI controller (token counter + latest-wins + ~8s QTimer watchdog) — NEW, shape TBD | controller | event-driven (async render results) | `desktop/viewers.py::ManuscriptViewerWidget` `_load_generation` + `_nav_debounce_timer` (lines 1142-1204); `desktop/image_loader.py::ImageLoaderThread.cancel()` (42-43) | role + flow match (latest-wins generation pattern already in repo) |
| `desktop/result_dialog.py` (ResultDialog wiring) | component (Qt dialog) | event-driven / request-response | self — extend `load_result_by_index` (1993), `load_local_page` (2404), `toggle_external_viewer` (2866), `on_enriched_data_loaded` (2871-2928 — the "show pane + feed viewer" precedent) | exact (same file, Genizah image path is the analog for the LOCAL path) |
| `genizah_app.py::create_browse_tab` + Browse handlers | component (Qt panel) | event-driven / request-response | self — `_open_local_browse_page` (19099), `_on_local_browse_nav` (19248), `on_browse_page_combo_changed` (10181), `toggle_browse_image` (10329), `_on_volume_manifest_loaded` (10241 — stale-result guard precedent) | exact (LOCAL browse text path is the analog for the LOCAL image path) |
| viewer feed (no new code; call existing API) | utility | transform (QImage → display) | `desktop/viewers.py::ManuscriptViewerWidget.display_image` (1209), `ZoomableScrollArea.set_image` / `set_status_message` (156-205) | exact (Phase 99 D-02 reuse target) |
| extension gating (no new code; query) | utility | request-response (DB lookup) | `shared/local_indexer.py::get_filepath` (1343), `_SUPPORTED_EXTENSIONS` (110), `local_files.file_extension` schema (585-600) | exact |
| worker API surface (read-only contract) | service | event-driven (queue → signals) | `desktop/pdf_page_renderer.py::PdfRenderWorker` (signals 378-379, `enqueue` 391, `stop` 474) | exact (the thing being wired) |
| worker shutdown (in `closeEvent`) | lifecycle | n/a | `genizah_app.py::GenizahGUI.closeEvent` (24763) + Phase 97 `sweep_running_scan_runs()` call (24777-24782) + browse-viewer `stop_threads()` (24818-24819) | exact (D-07a precedent) |

---

## Pattern Assignments

### 1. UI Controller — token counter + latest-wins + ~8s QTimer watchdog (NEW)

**Analog A (latest-wins generation):** `desktop/viewers.py::ManuscriptViewerWidget` — the in-repo proof that a monotonic generation counter + lambda capture discards stale async results. This is exactly the shape D-07b/D-03 want.

Generation bump on navigate + immediate "Loading…" placeholder (lines 1152-1160):
```python
self.current_idx = index
self._load_generation += 1  # Invalidate any in-flight callbacks immediately
# Update status text immediately for responsiveness
self.scroll_area.set_status_message(tr("Loading..."))
# Store pending index and restart debounce timer (persistent, not recreated)
self._pending_page_idx = index
self._nav_debounce_timer.start(150)  # 150ms debounce  <-- D-04 reference interval
```

Fresh generation captured per request, stale results dropped by comparing the echoed generation (lines 1175-1203):
```python
self._load_generation += 1  # Fresh generation for actual load
gen = self._load_generation
...
self.loader_thread.image_loaded.connect(
    lambda img, g=gen: self.display_image(img) if g == self._load_generation and not self._closing else None
)
self.loader_thread.load_failed.connect(
    lambda g=gen: None if g != self._load_generation or self._closing else self.scroll_area.set_status_message(tr("No Image"))
)
```

**Map to Phase 100:** The controller's monotonic token counter = `_load_generation`. On enqueue, `token = self._next_token(); self._latest_token = token` then compare in the `render_succeeded`/`render_failed` slots: `if token != self._latest_token: return` (silent discard = D-02 CANCELLED no-placeholder). The worker echoes the token verbatim (verified by `test_token_echoed_in_signals`), so the controller compares the echoed token against its most-recent — NOT the viewer's internal `_load_generation`.

**Analog B (cooperative cancel flag):** `desktop/image_loader.py::ImageLoaderThread` (lines 29, 42-43):
```python
self._cancelled = False
...
def cancel(self):
    self._cancelled = True
```
The controller's latest-wins is the conceptual equivalent — but note the worker is shared/long-lived and does NOT support per-request cancel; the controller discards by token instead (no `cancel()` call into the worker).

**QTimer watchdog (~8s, D-05) — debounce timer precedent:** `desktop/viewers.py` uses a persistent (not recreated) `QTimer` restarted on each navigation. Mirror this for BOTH the 150ms debounce (D-04) and the ~8s watchdog:
```python
# persistent timer pattern (viewers.py _nav_debounce_timer, line 1160 / 1158 comment)
self._nav_debounce_timer.start(150)  # restart; do NOT recreate the QTimer each call
```
For the watchdog: on enqueue start an `~8000ms` single-shot (or restarted persistent) `QTimer`; on expiry, if the awaited token is still `_latest_token`, call `set_status_message(<TIMEOUT placeholder>)` and bump the token so the late result is discarded (D-05). On `render_succeeded`/`render_failed` for the current token, stop the watchdog. Log the watchdog `TIMEOUT` (D-02).

**Failure enum → placeholder mapping (D-02/D-03):** Read the enum from `desktop/pdf_page_renderer.py` (lines 102-109): `MISSING_FILE`, `NOT_PDF`, `ENCRYPTED`, `CORRUPT`, `PAGE_OUT_OF_RANGE`, `RENDER_ERROR`, `TIMEOUT`, `CANCELLED`. Build a `dict[PdfRenderFailure, tuple[str_he, str_en]]` and select by `CURRENT_LANG` (see Shared Patterns §Localization). `CANCELLED`/stale → no placeholder (discarded by token compare before reaching the placeholder code).

---

### 2. ResultDialog wiring (`desktop/result_dialog.py`)

**Analog:** the existing Genizah image path in the SAME file — `on_enriched_data_loaded` (2871-2928) is the precedent for "reveal the external pane + feed `self.ms_viewer`".

**Viewer pane is the reusable asset (lines 555-563):**
```python
# New: Reusable Viewer Widget
from desktop.viewers import ManuscriptViewerWidget
self.ms_viewer = ManuscriptViewerWidget()
ext_layout.addWidget(self.lbl_ext_attr)
ext_layout.addWidget(self.txt_ext_meta)
ext_layout.addWidget(self.ms_viewer, 1)
self.main_splitter.addWidget(self.external_pane)
```
`self.external_pane.setVisible(False)` (line 544) — the pane defaults hidden; Phase 100 flips it on for LOCAL PDF.

**Pane reveal precedent (Genizah path, 2900-2928):**
```python
self.btn_toggle_image.setVisible(has_images)
if has_images:
    self.external_pane.setVisible(True)
    self.btn_toggle_image.setChecked(True)
    ...
    self.ms_viewer.load_images(meta, initial_idx, target_folio=folio_num)
```
> Phase 100 differs: do NOT call `ms_viewer.load_images(...)` (that takes a Genizah meta dict + IIIF list). Instead feed the single rendered QImage straight to `self.ms_viewer.display_image(qimage)` (viewers.py 1209). Reveal the pane the same way (`external_pane.setVisible(True)` + sync `btn_toggle_image`).

**Toggle button + handler (293-296, 2866-2869):**
```python
self.btn_toggle_image = QPushButton("🖼️")
self.btn_toggle_image.clicked.connect(self.toggle_external_viewer)
...
def toggle_external_viewer(self, checked):
    self.external_pane.setVisible(checked)
    if checked:
        QTimer.singleShot(0, self.sync_external_view)
```
D-08: for LOCAL PDF keep this toggle functional; for non-PDF LOCAL hide/disable it.

**LOCAL detection + filepath resolution (load_result_by_index, 2009-2027):**
```python
from shared.local_sys_id import is_local_sys_id as _is_local
_src_id = (data.get('display', {}) or {}).get('id', '')
_is_local_hit = bool(_src_id and _is_local(_src_id) and self._app)
if _is_local_hit:
    _fp = None
    if hasattr(self._app, '_lookup_local_filepath'):
        _fp = self._app._lookup_local_filepath(_src_id)
    self._rd_local_filepath = _fp
    self.btn_rd_open_file.setVisible(bool(_fp))
```
> `self._rd_local_filepath` is already held — Phase 100 reuses it for the `enqueue(...filepath=self._rd_local_filepath)` call and for the `.pdf` extension gate. `self._app` is the `GenizahGUI` reference (D-07: borrow `self._app`'s shared worker + controller).

**Re-render on within-document navigation (load_local_page, 2404-2518):** This is the LOCAL prev/next-PAGE path. After the state-update block sets `self.current_p_num` (line 2461), fire the controller render for `(self.current_sys_id, self.current_p_num)` (D-06). The page is the 1-based `p_num` passed straight to `enqueue`.

**Re-render on prev/next RESULT:** `load_result_by_index` (1993) is the entry for a newly-shown hit — after `_is_local_hit` + `_rd_local_filepath` are computed, gate on `.pdf` and fire the render with `current_sys_id` + `current_p_num`.

---

### 3. Browse wiring (`genizah_app.py::create_browse_tab`)

**Analog:** the existing LOCAL browse TEXT path — `_open_local_browse_page` (19099) already does render + state-update + nav-button enable for LOCAL. Phase 100 adds the IMAGE render alongside the text render in this same method (and re-fires it from the nav/combo handlers).

**Viewer (line 7128):** `self.browse_viewer = ManuscriptViewerWidget()` — same widget class as ResultDialog; feed `display_image(qimage)`.

**Pane visibility helper (10329-10343):**
```python
def toggle_browse_image(self):
    visible = self.btn_b_toggle_img.isChecked()
    self.browse_viewer.setVisible(visible)

def _set_browse_image_pane_visible(self, visible: bool):
    """Phase 95 D-27 helper — programmatic equivalent of toggle_browse_image."""
    if hasattr(self, 'btn_b_toggle_img'):
        self.btn_b_toggle_img.setChecked(visible)
    if hasattr(self, 'browse_viewer'):
        self.browse_viewer.setVisible(visible)
```
> CRITICAL current behavior to flip: `_open_local_browse_page` line 19233 calls `self._set_browse_image_pane_visible(False)` for EVERY LOCAL hit (always text-only). Phase 100 changes this to: if the LOCAL file extension is `.pdf` → `_set_browse_image_pane_visible(True)` + render; else keep `False` (D-08, PDFIMG-05).

**Toggle button (6825-6831):**
```python
self.btn_b_toggle_img = QPushButton()
self.btn_b_toggle_img.setText("🖼️")
self.btn_b_toggle_img.setCheckable(True)
self.btn_b_toggle_img.setChecked(True)
self.btn_b_toggle_img.clicked.connect(self.toggle_browse_image)
self.btn_b_toggle_img.setEnabled(False)
```

**LOCAL prev/next-PAGE dispatch (19055-19064, 19248-19283):**
```python
def _browse_prev_next(self, offset: int) -> None:
    if self._is_browsing_local():
        self._on_local_browse_nav(offset=offset)
    else:
        self.browse_navigate(offset)
```
`_on_local_browse_nav` ends by calling `self._open_local_browse_page(sid, p_num=page_data.get('p_num'))` (line 19283) — so wiring the render INSIDE `_open_local_browse_page` covers prev/next-page automatically (D-06). The page combo change (`on_browse_page_combo_changed`, 10181) is the Genizah path; the LOCAL re-render point of truth is `_open_local_browse_page`.

**Filepath + extension already resolved in `_open_local_browse_page` (19136-19141, 19221-19234):**
```python
is_pdf = False
try:
    fp = self._lookup_local_filepath(sys_id) or ""
    is_pdf = fp.lower().endswith('.pdf')
except Exception:
    pass
...
filepath = self._lookup_local_filepath(sys_id)
...
self._set_browse_image_pane_visible(False)   # <-- Phase 100: gate on is_pdf
self._current_local_filepath = filepath
```
> `is_pdf` is ALREADY computed here (line 19138) for the page-label "Page" vs "Chunk" wording — reuse it directly for the D-08 gate. `self.current_browse_p` (set line 19223) is the 1-based page to render.

**Filepath lookup helper (18670-18683):**
```python
def _lookup_local_filepath(self, sys_id: str):
    my_lib_tab = getattr(self, 'my_library_tab', None)
    indexer = getattr(my_lib_tab, '_indexer', None) if my_lib_tab else None
    if indexer is None:
        return None
    try:
        return indexer.get_filepath(sys_id)
    except Exception:
        return None
```

**Stale-result guard precedent (Browse already does this) — `_on_volume_manifest_loaded` (10241-10249):**
```python
def _on_volume_manifest_loaded(self, sid, ie_id, data, gen, expected_vol_ie):
    # Guard: reject stale results
    if gen != self._browse_enrich_gen:
        return
    if sid != self.current_browse_sid:
        return
```
> Browse already uses a `_browse_enrich_gen` monotonic counter + sid recheck — the exact latest-wins discipline Phase 100's controller token formalizes. The render slot should likewise recheck `sys_id == self.current_browse_sid` as defense-in-depth on top of the token compare.

---

### 4. Viewer feed (`desktop/viewers.py`)

**Display entry (1209-1220):**
```python
def display_image(self, image):
    if self._closing:
        return
    pix = QPixmap.fromImage(image)
    self.scroll_area.set_image(pix)
    self._sync_fullscreen_image()
    self.slider_rotation.setValue(0)
    self.slider_brightness.setValue(0)
    ...
```
> Feed the rendered `QImage` here. This gives Ctrl+wheel zoom / pan / rotate / brightness for free (Phase 99 D-02). Do not build any new pane.

**Loading + failure placeholder mechanism (D-01/D-02) — `ZoomableScrollArea` (156-205):**
```python
def set_image(self, pixmap):
    ...
    if not pixmap or pixmap.isNull():
        self._pixmap_item.setVisible(False)
        self.set_status_message(tr("No Image"))
        return
    ...

def set_status_message(self, text):
    if sip.isdeleted(self._msg_item) or sip.isdeleted(self._pixmap_item):
        return
    self._pixmap_item.setVisible(False)
    self._msg_item.setText(text)
    self._msg_item.setVisible(True)
    self._update_text_pos()
```
> D-01 loading state: call `viewer.scroll_area.set_status_message("Loading…")` (localized) immediately on navigation, BEFORE the debounce/enqueue — exactly as `set_page` line 1156 does. D-02 failure: on `render_failed`, call `set_status_message(<reason placeholder>)`. The `ManuscriptViewerWidget` wraps `self.scroll_area` (a `ZoomableScrollArea`); reach the placeholder via `viewer.scroll_area.set_status_message(...)` (the `set_page` precedent at 1144-1156 accesses `self.scroll_area` directly).

---

### 5. Extension gating (`shared/local_indexer.py`)

**Supported extensions (line 110):**
```python
_SUPPORTED_EXTENSIONS = {".docx", ".pdf", ".txt", ".html", ".xlsx", ".csv"}
```

**Filepath lookup (1343-1352):**
```python
def get_filepath(self, sys_id: str) -> Optional[str]:
    row = self._conn.execute(
        "SELECT filepath FROM local_files WHERE sys_id = ?", (sys_id,)
    ).fetchone()
    return row["filepath"] if row else None
```

**Schema — `local_files.file_extension` + `local_pages.page_num` (574-600):**
```python
CREATE TABLE IF NOT EXISTS local_pages (
    sys_id   TEXT NOT NULL,
    uid      TEXT NOT NULL,
    page_num INTEGER NOT NULL,   -- 1-based; sparse for PDFs (blank pages skipped)
    ...
    PRIMARY KEY (sys_id, page_num)
);
CREATE TABLE IF NOT EXISTS local_files (
    ...
    file_extension TEXT NOT NULL,   -- canonical ".pdf" / ".docx" / ... for D-08 gate
    page_count     INTEGER NOT NULL DEFAULT 0,
    ...
);
```
> D-08 "is this a PDF" check: both surfaces already derive the path via `_lookup_local_filepath` / `_current_local_filepath` and test `fp.lower().endswith('.pdf')` (genizah_app.py 19138; result_dialog `_on_browse_open_file_clicked` ext-allowlist 19028-19029). Claude's discretion (CONTEXT line 49): use the already-cached path's `.lower().endswith('.pdf')` (simplest, matches existing code) rather than adding a new `get_file_extension(sys_id)` query. There is currently NO public `get_file_extension` accessor — `file_extension` is only written, not read back via a method — so the path-suffix check is the established idiom.

---

### 6. Worker API surface (`desktop/pdf_page_renderer.py`) — READ-ONLY contract

**Signals (378-379):**
```python
render_succeeded = pyqtSignal(int, str, int, QImage)        # token, sys_id, page_num, image
render_failed    = pyqtSignal(int, str, int, object, str)   # token, sys_id, page_num, PdfRenderFailure, detail
```
> The enum slot is `object` (PyMuPDF enums are not Qt metatypes). In the slot, the `reason` arg is a `PdfRenderFailure` instance — map it through the placeholder dict.

**enqueue (391-409):**
```python
def enqueue(self, token: int, sys_id: str, page_num: int, filepath: str) -> bool:
    if self._stopping:
        logger.debug("PdfRenderWorker.enqueue after stop ...")
        return False
    self._queue.put((token, sys_id, page_num, filepath))
    return True
```
> The controller owns the token counter (Phase 99 left it out by design). `page_num` is 1-based `p_num`; the worker converts to `fitz` index `page_num − 1` internally (D-06).

**stop (474-490):** cooperative only — `self._stopping = True`, push `_STOP`, `wait(timeout_ms)`. No `terminate()`. Do NOT call `_lru.close_all()` from the caller thread (single-owner rule); the worker closes docs in `run()`'s finally.

**Failure enum (102-109):** `MISSING_FILE / NOT_PDF / ENCRYPTED / CORRUPT / PAGE_OUT_OF_RANGE / RENDER_ERROR / TIMEOUT / CANCELLED`. `TIMEOUT` is NEVER emitted by the worker — the Phase 100 watchdog surfaces it.

**Worker ownership (D-07):** instantiate ONE `PdfRenderWorker(maxsize=4)` on `GenizahGUI`, `.start()` it, share it with both surfaces. `ResultDialog` borrows it via `self._app`.

---

## Shared Patterns

### Localization (D-03) — `CURRENT_LANG` global
**Source:** `genizah_app.py` module global, used pervasively (e.g. lines 291, 303, 2564):
```python
_printed_tag = 'דפוס' if CURRENT_LANG == 'he' else 'Printed'
```
And in `result_dialog.py` (line 2564): `... if CURRENT_LANG == 'he' else 'Printed'`.
**Apply to:** every placeholder/failure string in the controller. Build `{reason: (he, en)}` and pick by `CURRENT_LANG`. Fallback if unavailable in scope: bilingual `"HE / EN"` (About/Help convention, CONTEXT D-03). `tr(...)` is also available in both files for keys present in the translations dict.

### Latest-wins generation token
**Source:** `desktop/viewers.py` `_load_generation` (1153/1175/1199) + `genizah_app.py` `_browse_enrich_gen` (10224/10244).
**Apply to:** the controller's token counter; both surfaces' render slots compare the echoed token before touching the viewer (D-03/D-07b). Stale = silent discard (no placeholder).

### Persistent QTimer restart (debounce + watchdog)
**Source:** `desktop/viewers.py::ManuscriptViewerWidget` `_nav_debounce_timer.start(150)` (1160) — comment "persistent, not recreated" (1158).
**Apply to:** 150ms debounce (D-04, identical in both surfaces per D-05) AND the ~8s watchdog (D-05). Reuse a persistent timer; restart on each navigation rather than creating a new QTimer.

### Worker / thread shutdown in closeEvent (D-07a)
**Source:** `genizah_app.py::GenizahGUI.closeEvent` (24763) — the precedent stack: `sweep_running_scan_runs()` (24777-24782), `browse_viewer.stop_threads()` (24818-24819), per-thread `wait(2000)` guards.
**Apply to:** add `self._pdf_render_worker.stop()` here (cooperative, alongside the existing `try/except` worker-shutdown block). No `terminate()` (Phase 99 D-05).

### Defensive sid recheck on async result
**Source:** `genizah_app.py::_on_volume_manifest_loaded` (10244-10249) — `if sid != self.current_browse_sid: return`.
**Apply to:** Browse render slot — recheck `sys_id == self.current_browse_sid` on top of the token compare; ResultDialog — recheck against `self.current_sys_id` (cf. `on_enriched_data_loaded` line 2874 `if sid != self.current_sys_id: return`).

---

## No Analog Found

| Concern | Role | Reason |
|---------|------|--------|
| (none) | — | All six integration points have strong in-repo analogs. The only genuinely new code is the thin controller, whose latest-wins + persistent-timer shape is fully precedented by `ManuscriptViewerWidget` (viewers.py) and `_browse_enrich_gen` (genizah_app.py). |

---

## Metadata

**Analog search scope:** `desktop/` (image_loader, viewers, pdf_page_renderer, result_dialog), `genizah_app.py` (Browse tab + closeEvent), `shared/local_indexer.py`.
**Files scanned:** 5 source files + 3 planning docs.
**Pattern extraction date:** 2026-05-27
