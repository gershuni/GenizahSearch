# Phase 69: Image Viewer Extraction - Context

**Gathered:** 2026-04-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Move 3 image viewer classes out of `genizah_app.py` into a new `desktop/viewers.py` module. Zero user-visible behavior change. Third extraction in the v7.9 Decomposition milestone, following the pattern established by Phase 67 (ResultDialog) and Phase 68 (dialogs).

In scope:
- **`desktop/viewers.py`** (DESK-03): `ZoomableScrollArea`, `FullscreenImageWindow`, `ManuscriptViewerWidget`
- **`desktop/result_dialog.py`**: retarget 1 lazy import from `genizah_app` to `desktop.viewers`
- **`genizah_app.py`**: replace inline class defs with re-exports for back-compat

Out of scope:
- Any behavior change, styling tweak, or feature addition
- `HiddenScrollArea` (text snippet widget for search results, NOT an image viewer — stays in genizah_app.py)
- `DesktopVSCache`, `VSFetchThread`, `VSDownloadThread` (visual similarity feature, different domain)
- `desktop/image_utils.py` extraction (no shared image utilities exist — viewers and puzzle use separate pipelines)
- Reading desk ZoomableScrollArea site retargeting within GenizahGUI (Phase 71's domain)
- Protocol/ABC narrowing of parent surfaces (deferred to Phase 71)

</domain>

<decisions>
## Implementation Decisions

### Module Structure (Gray Area 1)
- **D-01:** All 3 classes go into a single **`desktop/viewers.py`** module. No split into `desktop/zoomable.py` or `desktop/image_utils.py`. ZoomableScrollArea is only used by the other two viewer classes and GenizahGUI's reading desk — no reason to isolate it. This mirrors the `desktop/dialogs_scholarly.py` single-module pattern from Phase 68.

### No Shared Image Utils (Roadmap Risk Resolution)
- **D-02:** The roadmap flagged potential shared image-loading helpers between viewers and puzzle. **Investigation found no sharing**: viewers use `ImageLoaderThread` (in `desktop/image_loader.py`), puzzle uses `PuzzleImageLoaderThread` (in `gui_threads.py`) + `shared/puzzle_image_service.py`. Completely separate pipelines. No `desktop/image_utils.py` needed.
- **D-03:** `ZoomableScrollArea._apply_adjustments_to_pixmap()` (brightness/contrast/gamma/invert) is instance-bound, not a standalone utility. It stays as a method on the class. If web ever needs the same pixel math, the right refactor is extracting to `shared/image_adjustments.py` — but that's not Phase 69 scope.

### Reading Desk Usage (Gray Area 2)
- **D-04:** GenizahGUI's reading desk creates `ZoomableScrollArea()` instances directly at 3 sites (lines 10657, 12856, 12872). After extraction, these use the **re-export** in `genizah_app.py` — same pattern as Phase 68 (D-07). The reading desk code stays unchanged. Phase 71 (GenizahGUI consolidation) can retarget these to import from `desktop.viewers` directly if desired.

### ResultDialog Lazy Import
- **D-05:** `desktop/result_dialog.py:489` has `from genizah_app import ManuscriptViewerWidget`. Retarget to `from desktop.viewers import ManuscriptViewerWidget`. Same pattern as Phase 68 D-04/D-05 — stays function-local, only source module changes. This eliminates the `desktop.result_dialog → genizah_app` back-edge for ManuscriptViewerWidget.
- **D-06:** After D-05, check if `desktop/result_dialog.py` has any remaining `from genizah_app import` lines. If `DesktopVSCache` (line 645) is the only one left, note it for Phase 71 but do NOT move it in Phase 69 (out of scope).

### Re-export Strategy
- **D-07:** `genizah_app.py` replaces the 3 moved class definitions with a top-of-file re-export: `from desktop.viewers import ZoomableScrollArea, FullscreenImageWindow, ManuscriptViewerWidget  # noqa: F401`. Same `# noqa: F401` pattern as Phase 68.
- **D-08:** `desktop/__init__.py` stays minimal (no barrel re-exports — same as Phase 68 D-08).

### Class Ordering in desktop/viewers.py
- **D-09:** Classes ordered by dependency: `ZoomableScrollArea` first (base), then `FullscreenImageWindow` (uses ZoomableScrollArea), then `ManuscriptViewerWidget` (uses both). This matches the source order in genizah_app.py and avoids forward references.

### Verification
- **D-10:** pytest baseline (1067 passed, 8 skipped as of Phase 68 close) must remain green.
- **D-11:** Import smoke after extraction: `python -c "from desktop.viewers import ZoomableScrollArea, FullscreenImageWindow, ManuscriptViewerWidget; from desktop.result_dialog import ResultDialog; from genizah_app import ZoomableScrollArea, FullscreenImageWindow, ManuscriptViewerWidget, GenizahGUI"` — all succeed.
- **D-12:** Desktop smoke test: launch app → browse a manuscript (exercises ManuscriptViewerWidget in browse tab) → click fullscreen (exercises FullscreenImageWindow) → open reading desk (exercises direct ZoomableScrollArea usage) → close. No crash, no visible regression.
- **D-13:** CI green (Ubuntu + Windows matrix). Same Windows case-sensitivity risk as Phase 68 — module name MUST be exactly `viewers` (lowercase).

### Parent Coupling — explicitly unchanged
- **D-14:** Like Phase 68, these viewer classes do NOT use `self.parent()` for app-level coupling. `ManuscriptViewerWidget` takes a standard `parent=None` Qt parent. `FullscreenImageWindow` takes `parent_viewer` (a ManuscriptViewerWidget reference, not GenizahGUI). No `self._app` rename needed.

### Claude's Discretion
- Commit granularity: single commit or fine-grained (skeleton → move classes → retarget imports → add re-exports), as long as each commit is pytest-green.
- Exact import set for `desktop/viewers.py` header — derived via ruff iteration.
- Module docstring wording.

### Folded Todos
None — the matched todos (corrections service migration, unified metadata search) are orthogonal to viewer extraction.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap & Requirements
- `.planning/ROADMAP.md` — Phase 69 entry; v7.9 milestone boundaries
- `.planning/REQUIREMENTS.md` — DESK-03
- `.planning/PROJECT.md` — v7.9 Active milestone; dual-app constraint; `desktop/` package

### Source — Subject of the Phase
- `genizah_app.py:1324-1644` — `ZoomableScrollArea` (320 lines)
- `genizah_app.py:1644-1879` — `FullscreenImageWindow` (235 lines)
- `genizah_app.py:1879-2484` — `ManuscriptViewerWidget` (605 lines)

### Call sites in GenizahGUI (must keep working via re-export)
- `genizah_app.py:10633` — `self.browse_viewer = ManuscriptViewerWidget()`
- `genizah_app.py:10657` — reading desk `ZoomableScrollArea` list
- `genizah_app.py:12856, 12872` — reading desk `ZoomableScrollArea()` construction

### Downstream consumer (retarget lazy import)
- `desktop/result_dialog.py:489` — `from genizah_app import ManuscriptViewerWidget`
- `desktop/result_dialog.py:645` — `from genizah_app import DesktopVSCache` (NOT in scope — note for Phase 71)

### Prior Phase Context (established pattern)
- `.planning/phases/68-desktop-dialog-extractions/68-CONTEXT.md` — re-export strategy, ruff F401, Windows case sensitivity
- `.planning/phases/67-resultdialog-extraction/67-CONTEXT.md` — one-directional imports, Qt lifecycle guards

### Existing Siblings
- `desktop/image_loader.py` — `ImageLoaderThread` (already extracted in Phase 67)
- `desktop/widgets.py` — shared UI helpers (do NOT add viewer classes here)
- `desktop/dialogs_scholarly.py`, `desktop/dialogs_filter.py` — Phase 68 extractions
- `gui_threads.py` — `PuzzleImageLoaderThread`, `FilterCountWorker` (separate from viewer pipeline)

### CI & Verification
- `.github/workflows/ci.yml` — Ubuntu + Windows matrix
- `tests/` — 1067 passed, 8 skipped baseline

</canonical_refs>

<code_context>
## Existing Code Insights

### Class Dependencies
- `ZoomableScrollArea` — standalone QGraphicsView. Uses PyQt6 graphics (QGraphicsScene, QGraphicsPixmapItem, etc.), QTimer for adjustment debounce, numpy for LUT-based pixel processing. Has context menu (copy/save image).
- `FullscreenImageWindow` — QMainWindow wrapping a ZoomableScrollArea. Takes `parent_viewer` (ManuscriptViewerWidget) for page sync. Emits `page_changed` signal.
- `ManuscriptViewerWidget` — QWidget composing a ZoomableScrollArea + combo source + thumbnails + folio nav. Uses `ImageLoaderThread` from `desktop/image_loader.py`. Has `_thumbnail_ready` signal.

### Import Implications
- `desktop/viewers.py` will need: PyQt6 (QMainWindow, QWidget, QGraphicsView, QGraphicsScene, QGraphicsPixmapItem, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QComboBox, QSlider, QCheckBox, QTimer, QPixmap, QColor, QFont, QPainter, Qt, pyqtSignal, etc.), numpy, genizah_core (tr, get_logger), desktop.image_loader (ImageLoaderThread)
- The `from desktop.image_loader import ImageLoaderThread` in viewers.py creates a `desktop.viewers → desktop.image_loader` edge — this is one-directional (correct).

### Integration Points
- `genizah_app.py` top imports: new re-export line joins the existing `from desktop.dialogs_scholarly import ...` and `from desktop.dialogs_filter import ...` block.
- `desktop/result_dialog.py:489`: single lazy import retarget.

</code_context>

<deferred>
## Deferred Ideas

### For Phase 71 (GenizahGUI Consolidation)
- Retarget GenizahGUI's direct `ZoomableScrollArea()` usage in reading desk to import from `desktop.viewers` instead of relying on re-export
- Move `DesktopVSCache` lazy import in `desktop/result_dialog.py:645` to its proper home
- Protocol/ABC narrowing of ManuscriptViewerWidget's constructor surface

### Potential Future Phase
- Extract `shared/image_adjustments.py` if web ever needs brightness/contrast/gamma/invert pixel processing (currently only desktop ZoomableScrollArea has this)

### For Phase 76 (Documentation Close)
- Record `desktop/viewers.py` in `docs/CODE_INDEX.md`
- Update path references pointing at `genizah_app.py:1324` (ZoomableScrollArea), `:1644` (FullscreenImageWindow), `:1879` (ManuscriptViewerWidget)

### Reviewed Todos (not folded)
- `2026-02-11-migrate-desktop-corrections-fetch-to-shared-corrections-service.md` — service-layer refactor, orthogonal
- `2026-03-09-unified-metadata-text-search-with-translations.md` — feature work, orthogonal

</deferred>

---

*Phase: 69-image-viewer-extraction*
*Context gathered: 2026-04-16*
