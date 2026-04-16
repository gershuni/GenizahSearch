# Phase 70: Puzzle Extraction - Context

**Gathered:** 2026-04-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Move 5 puzzle/join canvas classes out of `genizah_app.py` into a new `desktop/puzzle.py` module. Zero user-visible behavior change. Fourth extraction in the v7.9 Decomposition milestone.

In scope:
- **`desktop/puzzle.py`** (DESK-02): `PuzzleFragmentItem`, `PuzzleCanvasView`, `PuzzleExportThread`, `PuzzlePublishThread`, `PuzzleCanvasWindow`
- **`genizah_app.py`**: replace inline class defs with re-exports for back-compat

Out of scope:
- Any behavior change, styling tweak, or feature addition
- `ShelfmarkCompleter` extraction (used by puzzle but also by GenizahGUI — stays in genizah_app.py)
- Protocol/ABC narrowing of PuzzleCanvasWindow's `self.app` surface (Phase 71)
- `desktop/result_dialog.py` changes — result_dialog calls `parent.add_to_puzzle()` on GenizahGUI, not on puzzle classes directly; no import retargeting needed
- Moving PuzzleImageLoaderThread or PuzzleMetaLoaderThread out of gui_threads.py (already in correct home)

</domain>

<decisions>
## Implementation Decisions

### QThread Placement (Gray Area 1)
- **D-01:** `PuzzleExportThread` (50 lines) and `PuzzlePublishThread` (22 lines) stay **in `desktop/puzzle.py`** alongside the puzzle classes. They are puzzle-domain-specific (export PNG, publish/unpublish join), not shared utilities like FilterCountWorker. The "all QThreads in gui_threads.py" convention applies to shared threads. `PuzzleImageLoaderThread` and `PuzzleMetaLoaderThread` are already in gui_threads.py and stay there (they're used by puzzle but are general image/meta loaders).

### self.app Coupling (Gray Area 2)
- **D-02:** `PuzzleCanvasWindow.__init__(self, app)` and all 20+ `self.app.*` references move **verbatim** to `desktop/puzzle.py`. No Protocol/ABC narrowing, no typing changes. `self.app` remains an untyped GenizahGUI reference. Phase 71 is explicitly scoped for Protocol narrowing.
- **D-03:** The `self.app.*` attributes accessed include: `meta_mgr`, `shelf_model`, `valid_shelf_keys`, `_ensure_shelf_map`, `_shelf_to_sys`, `lists_mgr`, `joins_mgr`, `corrections_client`, `add_to_puzzle`, `_get_default_save_folder`, `windowIcon`. These are all accessed via `getattr()` or `hasattr()` guards — safe without typing.

### ShelfmarkCompleter Import (Gray Area 3)
- **D-04:** `PuzzleCanvasWindow` uses `ShelfmarkCompleter` (genizah_app.py:1014) for shelfmark autocomplete. After extraction, `desktop/puzzle.py` imports it via **`from genizah_app import ShelfmarkCompleter`** as a lazy function-local import inside `__init__`. This creates a `desktop.puzzle → genizah_app` edge, but it's the same re-export pattern. ShelfmarkCompleter is also used directly by GenizahGUI so extracting it is scope creep.
- **D-05:** Alternative: if ruff or circular import issues arise, the import can be made conditional (`if TYPE_CHECKING`) or the completer setup can be deferred to a method called by GenizahGUI after construction. Executor has discretion.

### Module Structure
- **D-06:** Single `desktop/puzzle.py` module with all 5 classes. Class ordering matches source order and dependency chain: PuzzleFragmentItem → PuzzleCanvasView → PuzzleExportThread → PuzzlePublishThread → PuzzleCanvasWindow.
- **D-07:** Module name MUST be exactly `puzzle` (lowercase) — same Windows/Ubuntu case-sensitivity rule as Phase 68/69.

### Re-export Strategy
- **D-08:** `genizah_app.py` replaces the 5 moved class definitions with a top-of-file re-export: `from desktop.puzzle import PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow  # noqa: F401`. Same pattern as Phase 68/69.
- **D-09:** `desktop/__init__.py` stays minimal (no barrel re-exports).

### No Viewer Dependency
- **D-10:** Confirmed: puzzle classes do NOT import from `desktop/viewers.py`. No circular import risk. The roadmap concern about "Puzzle classes import image helpers from desktop/viewers.py" is moot — puzzle uses `PuzzleImageLoaderThread` from gui_threads.py and `shared/puzzle_image_service.py`, which are completely separate from the viewer image pipeline.

### Verification
- **D-11:** pytest baseline (1067 passed, 8 skipped as of Phase 69 close) must remain green.
- **D-12:** Import smoke: `python -c "from desktop.puzzle import PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow; from genizah_app import PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow, GenizahGUI"` — all succeed.
- **D-13:** Desktop smoke test: launch app → open puzzle window (Add to Puzzle from browse or toolbar) → add a fragment by shelfmark → verify fragment loads on canvas → flip/rotate → close puzzle. No crash, no visible regression.
- **D-14:** CI green (Ubuntu + Windows matrix).

### Claude's Discretion
- Commit granularity within the plan.
- Exact import set for `desktop/puzzle.py` header — derived via ruff iteration.
- Module docstring wording.
- Whether ShelfmarkCompleter import is at module top or function-local (D-04/D-05).

### Folded Todos
None — matched todos are orthogonal to puzzle extraction.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap & Requirements
- `.planning/ROADMAP.md` — Phase 70 entry; v7.9 milestone boundaries
- `.planning/REQUIREMENTS.md` — DESK-02
- `.planning/PROJECT.md` — v7.9 Active milestone; dual-app constraint; `desktop/` package

### Source — Subject of the Phase
- `genizah_app.py:1479-1913` — `PuzzleFragmentItem` (435 lines)
- `genizah_app.py:1914-2073` — `PuzzleCanvasView` (160 lines)
- `genizah_app.py:2074-2123` — `PuzzleExportThread` (50 lines)
- `genizah_app.py:2124-2145` — `PuzzlePublishThread` (22 lines)
- `genizah_app.py:2146-4120` — `PuzzleCanvasWindow` (1975 lines)

### Call sites in GenizahGUI (must keep working via re-export)
- `genizah_app.py:16685, 16693` — `PuzzleCanvasWindow(self)` construction

### Cross-class dependency
- `genizah_app.py:1014` — `ShelfmarkCompleter` (used by PuzzleCanvasWindow, stays in genizah_app.py)

### Existing dependencies (already extracted, no change needed)
- `gui_threads.py` — `PuzzleImageLoaderThread`, `PuzzleMetaLoaderThread` (puzzle imports these)
- `shared/puzzle_service.py`, `shared/puzzle_model.py`, `shared/puzzle_export.py`, `shared/puzzle_image_service.py` (function-local imports inside puzzle classes)

### Prior Phase Context (established pattern)
- `.planning/phases/69-image-viewer-extraction/69-CONTEXT.md` — single-module extraction pattern
- `.planning/phases/68-desktop-dialog-extractions/68-CONTEXT.md` — re-export strategy, ruff F401

### CI & Verification
- `.github/workflows/ci.yml` — Ubuntu + Windows matrix
- `tests/` — 1067 passed, 8 skipped baseline

</canonical_refs>

<code_context>
## Existing Code Insights

### Class Dependencies
- `PuzzleFragmentItem` — QGraphicsPixmapItem with crop, flip, rotate, resize. No external class deps beyond PyQt6.
- `PuzzleCanvasView` — QGraphicsView with zoom/drag. References PuzzleFragmentItem for item interactions.
- `PuzzleExportThread` — QThread wrapping `shared.puzzle_export.compose_puzzle_export()`. Self-contained.
- `PuzzlePublishThread` — QThread wrapping Supabase publish/unpublish. Self-contained.
- `PuzzleCanvasWindow` — QMainWindow composing PuzzleCanvasView + all toolbar/UI. Heavy coupling to GenizahGUI via `self.app` (20+ attribute accesses). Uses ShelfmarkCompleter from genizah_app.py. Uses PuzzleImageLoaderThread and PuzzleMetaLoaderThread from gui_threads.py.

### Import Implications
- `desktop/puzzle.py` will need: PyQt6 (QMainWindow, QWidget, QGraphicsView, QGraphicsScene, QGraphicsPixmapItem, QGraphicsRectItem, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QLineEdit, QComboBox, QSlider, QCheckBox, QTimer, QToolBar, QAction, QMenu, QFileDialog, QMessageBox, QProgressDialog, QPixmap, QColor, QFont, QPen, QBrush, QPainter, QCursor, QTransform, Qt, pyqtSignal, QThread, etc.), genizah_core (tr, get_logger), gui_threads (PuzzleImageLoaderThread, PuzzleMetaLoaderThread), genizah_app (ShelfmarkCompleter — lazy import)
- Several shared/* imports are already function-local in the source code and stay that way

### Integration Points
- `genizah_app.py` top imports: new re-export line joins existing desktop.* re-exports
- No `desktop/result_dialog.py` changes needed — result_dialog's `_add_to_puzzle()` calls `parent.add_to_puzzle()` on GenizahGUI, not on puzzle classes

</code_context>

<deferred>
## Deferred Ideas

### For Phase 71 (GenizahGUI Consolidation)
- Protocol/ABC narrowing of PuzzleCanvasWindow's `self.app` surface — define what `app` actually needs as a typed contract
- Extract ShelfmarkCompleter to `desktop/widgets.py` if a natural refactor emerges
- Clean up GenizahGUI's re-exports once all extractions are complete

### For Phase 76 (Documentation Close)
- Record `desktop/puzzle.py` in `docs/CODE_INDEX.md`
- Update path references pointing at `genizah_app.py:1479` through `:4120`

### Reviewed Todos (not folded)
- `2026-02-11-migrate-desktop-corrections-fetch-to-shared-corrections-service.md` — service-layer refactor, orthogonal
- `2026-03-09-unified-metadata-text-search-with-translations.md` — feature work, orthogonal

</deferred>

---

*Phase: 70-puzzle-extraction*
*Context gathered: 2026-04-16*
