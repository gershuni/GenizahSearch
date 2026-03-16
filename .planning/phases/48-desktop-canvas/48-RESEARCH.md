# Phase 48: Desktop Canvas - Research

**Researched:** 2026-03-16
**Domain:** PyQt6 QGraphicsScene canvas with per-item manipulation
**Confidence:** HIGH

## Summary

Phase 48 builds a QGraphicsScene-based puzzle canvas in the desktop app where researchers can add manuscript fragment images by shelfmark (with autocomplete) or from browse/search/lists, then drag, rotate, flip, resize each fragment independently on a dark canvas. Background-removed images (RGBA PNGs from Phase 47) overlay as parchment shapes rather than rectangles.

The technical domain is well-understood: QGraphicsScene/QGraphicsView is the standard Qt approach for multi-item 2D canvases with per-item transformations. Phase 47 delivered all foundation services (puzzle_model.py, puzzle_image_service.py, background_removal.py). The main implementation work is: (1) a custom QGraphicsItem subclass for draggable/rotatable/flippable/resizable fragments, (2) a QMainWindow hosting the canvas with toolbar, (3) integration points in browse/ResultDialog/lists for "Add to Puzzle", and (4) a PuzzleImageLoaderThread for async image fetch + bg removal.

**Primary recommendation:** Build a `PuzzleFragmentItem(QGraphicsPixmapItem)` subclass with per-item transform state, then wrap it in a `PuzzleCanvasWindow(QMainWindow)` with a QGraphicsView center widget. Follow the existing singleton window pattern (like Reading Desk) for "Add to Puzzle" entry points.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Free movement by default, hold Shift to snap to grid
- Rotation via corner handle drag (like image editors), NOT slider/buttons
- Multi-select: Ctrl+click to select multiple. Move/rotate/flip applies to all selected.
- Flip: Both toolbar buttons AND right-click context menu
- Resize: Mouse scroll wheel on fragment + toolbar scale slider (10%-400%)
- Separate window (QMainWindow), not a tab. Like the Reading Desk pattern.
- Single instance: "Add to Puzzle" adds fragment to existing window (or opens if closed)
- Entry points for "Add to Puzzle" button: Browse page, ResultDialog, AND Personal Lists
- Puzzle window also has shelfmark input with autocomplete + ability to pick from known FJMS joins and personal lists
- Shelfmark autocomplete uses existing normalize_shelfmark() -> _shelf_to_sys lookup
- Minimal info in top toolbar: Shelfmark + folio label for selected fragment
- Controls: Flip H/V buttons, threshold slider (per-fragment), folio prev/next, delete fragment, scale slider
- Per-fragment threshold: each fragment stores its own bg_removal_threshold
- Scale slider: 10%-400% in toolbar for precise control (in addition to mouse wheel)
- Folio navigation: auto re-process with fragment's saved threshold. If cached, instant.
- Toggle between dark gray (#333) and checkerboard. Default dark gray.
- Full zoom/pan at view level: Ctrl+wheel to zoom canvas, hand-drag to pan. Fragments still independently movable on top.

### Claude's Discretion
- Exact corner handle visual design (small circles? squares? rotation cursor?)
- Selection visual (dashed border? handles? glow?)
- Toolbar button icons and layout
- Grid snap increment size
- How multi-select group transform center is calculated
- Async image loading thread design (can reuse ImageLoaderThread pattern)

### Deferred Ideas (OUT OF SCOPE)
- Save/load puzzle arrangements -- Phase 50 (Join Documents)
- Recto/verso toggle -- Phase 51
- Community publish -- Phase 52
- "Load known join" from FJMS join groups -- Phase 52
- Undo/redo -- deferred enhancement (CANV-09)
- Z-order layer panel -- deferred enhancement (CANV-10)
- Snap guides between fragments -- Phase 49 (CANV-08)
- Oxford image loading via parts JSON -- future enhancement
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| CANV-01 | User can add a fragment to the puzzle canvas by shelfmark | Shelfmark autocomplete via existing ShelfmarkCompleter + _shelf_to_sys -> fl_id resolution -> PuzzleImageService fetch |
| CANV-03 | User can drag fragments freely on the canvas | QGraphicsItem.ItemIsMovable flag + custom mouseMoveEvent for Shift-to-snap |
| CANV-04 | User can rotate a fragment to any angle | Corner handle rotation via custom mouse interaction on corner hotspots, setRotation() on item |
| CANV-05 | User can flip a fragment horizontally or vertically | QTransform scale(-1,1) for H flip, scale(1,-1) for V flip applied to item transform |
| CANV-06 | User can resize a fragment independently | Mouse wheel on item + toolbar scale slider; setScale() on QGraphicsItem |
| PLAT-02 | Puzzle works in the desktop app (PyQt6 + QGraphicsScene) | Full QGraphicsScene/View architecture with custom PuzzleFragmentItem |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| PyQt6 | 6.10.2 | Desktop GUI framework | Already used throughout genizah_app.py |
| QGraphicsScene/View | (part of PyQt6) | Multi-item 2D canvas with per-item transforms | Qt's purpose-built scene graph for exactly this use case |
| Pillow | (existing) | Image manipulation for bg removal | Already used in shared/background_removal.py |
| NumPy | (existing) | Array operations for bg removal | Already used in shared/background_removal.py |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| shared/puzzle_image_service.py | Phase 47 | IIIF fetch + bg removal + disk cache | Every fragment image load |
| shared/puzzle_model.py | Phase 47 | PuzzleFragment/PuzzleDocument dataclasses | Canvas state tracking |
| shared/background_removal.py | Phase 47 | HSV bg removal engine | Threshold adjustment re-processing |

### No Additional Dependencies
Everything needed is already in the project. No new packages required.

## Architecture Patterns

### Recommended Project Structure
```
genizah_app.py          # Add PuzzleCanvasWindow class + PuzzleFragmentItem class
                        # Add "Add to Puzzle" buttons in ResultDialog, browse, lists
gui_threads.py          # Add PuzzleImageLoaderThread (async IIIF + bg removal)
shared/puzzle_model.py  # Already exists (Phase 47) - no changes needed
shared/puzzle_image_service.py  # Already exists - no changes needed
```

### Pattern 1: PuzzleFragmentItem (Custom QGraphicsPixmapItem)
**What:** Subclass QGraphicsPixmapItem to hold per-fragment state and handle mouse interactions for drag/rotate/resize
**When to use:** Every fragment on the canvas
**Example:**
```python
class PuzzleFragmentItem(QGraphicsPixmapItem):
    """A single fragment on the puzzle canvas with drag/rotate/flip/resize."""

    HANDLE_SIZE = 12  # Corner handle radius in pixels

    def __init__(self, puzzle_frag: PuzzleFragment, pixmap: QPixmap, parent=None):
        super().__init__(pixmap, parent)
        self.puzzle_frag = puzzle_frag  # PuzzleFragment dataclass
        self._selected = False
        self._rotating = False
        self._rotation_start_angle = 0.0

        # Enable interactions
        self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIsMovable, True)
        self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIsSelectable, True)
        self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemSendsGeometryChanges, True)
        self.setTransformationMode(Qt.TransformationMode.SmoothTransformation)

        # Set transform origin to center for rotation/scale
        rect = self.boundingRect()
        self.setTransformOriginPoint(rect.center())

        # Apply initial state from PuzzleFragment
        self.setPos(puzzle_frag.x, puzzle_frag.y)
        self.setRotation(puzzle_frag.rotation)
        self.setScale(puzzle_frag.scale)
        self._apply_flip()

    def _apply_flip(self):
        """Apply flip transforms via QTransform."""
        t = QTransform()
        if self.puzzle_frag.flip_h:
            t.scale(-1, 1)
        if self.puzzle_frag.flip_v:
            t.scale(1, -1)
        # Must translate to keep centered after flip
        rect = self.boundingRect()
        if self.puzzle_frag.flip_h:
            t.translate(-rect.width(), 0)
        if self.puzzle_frag.flip_v:
            t.translate(0, -rect.height())
        self.setTransform(t)
```

### Pattern 2: Corner Handle Rotation
**What:** Detect mouse near corners to enter rotation mode; drag to rotate around item center
**When to use:** User grabs a corner handle on a selected fragment
**Example:**
```python
def mousePressEvent(self, event):
    if self._is_near_corner(event.pos()):
        self._rotating = True
        center = self.boundingRect().center()
        self._rotation_start_angle = math.degrees(
            math.atan2(event.pos().y() - center.y(),
                       event.pos().x() - center.x())
        ) - self.rotation()
        event.accept()
        return
    super().mousePressEvent(event)

def mouseMoveEvent(self, event):
    if self._rotating:
        center = self.boundingRect().center()
        angle = math.degrees(
            math.atan2(event.pos().y() - center.y(),
                       event.pos().x() - center.x())
        )
        new_rotation = angle - self._rotation_start_angle
        self.setRotation(new_rotation % 360)
        event.accept()
        return
    # Shift-to-snap grid
    if event.modifiers() & Qt.KeyboardModifier.ShiftModifier:
        pos = self.pos()
        grid = 20  # pixels
        snapped = QPointF(round(pos.x() / grid) * grid,
                          round(pos.y() / grid) * grid)
        self.setPos(snapped)
    super().mouseMoveEvent(event)
```

### Pattern 3: Singleton Window (like Reading Desk)
**What:** Single PuzzleCanvasWindow instance, re-raised on repeated "Add to Puzzle" calls
**When to use:** All "Add to Puzzle" entry points call the same method
**Example:**
```python
# In GenizahSearchApp:
self._puzzle_window = None  # Singleton

def add_to_puzzle(self, sys_id, shelfmark, folio_label, fl_id):
    """Add fragment to puzzle canvas. Opens window if needed."""
    if self._puzzle_window is None or not self._puzzle_window.isVisible():
        self._puzzle_window = PuzzleCanvasWindow(self)
    self._puzzle_window.add_fragment(sys_id, shelfmark, folio_label, fl_id)
    self._puzzle_window.show()
    self._puzzle_window.raise_()
    self._puzzle_window.activateWindow()
```

### Pattern 4: Async Image Loading with Background Removal
**What:** QThread that calls PuzzleImageService.resolve_fragment_image() off the main thread
**When to use:** Every fragment add/threshold change/folio navigation
**Example:**
```python
class PuzzleImageLoaderThread(QThread):
    """Load and process a fragment image in the background."""
    image_ready = pyqtSignal(str, bytes)  # fl_id, rgba_png_bytes
    load_failed = pyqtSignal(str, str)    # fl_id, error_message

    def __init__(self, fl_id, threshold=30.0, size=800):
        super().__init__()
        self.fl_id = fl_id
        self.threshold = threshold
        self.size = size

    def run(self):
        try:
            from shared.puzzle_image_service import resolve_fragment_image
            result = resolve_fragment_image(
                self.fl_id, size=self.size,
                threshold=self.threshold, processed=True
            )
            if result:
                self.image_ready.emit(self.fl_id, result)
            else:
                self.load_failed.emit(self.fl_id, "Image not available")
        except Exception as e:
            self.load_failed.emit(self.fl_id, str(e))
```

### Pattern 5: View-Level Zoom/Pan vs Item-Level Interaction
**What:** Ctrl+wheel zooms the entire view; plain wheel on a fragment resizes that fragment; hand-drag pans the view when not over a fragment
**When to use:** Canvas interaction routing
**Key insight:** QGraphicsView.DragMode.ScrollHandDrag enables view panning, but items with ItemIsMovable intercept mouse events first. The view only pans when clicking empty canvas areas.

### Anti-Patterns to Avoid
- **Single pixmap in scene:** ZoomableScrollArea uses one QGraphicsPixmapItem. The puzzle needs MULTIPLE independent items. Do NOT try to extend ZoomableScrollArea.
- **Slider-based rotation:** CONTEXT.md explicitly says corner handle drag, NOT slider/buttons. The existing ManuscriptViewerWidget rotation slider pattern must NOT be copied.
- **Blocking image loads:** Always use QThread for IIIF fetch + bg removal. A single image can take 1-3 seconds.
- **Global flip state:** Each fragment has independent flip_h/flip_v. Do NOT apply flip at view level.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Image fetch + bg removal | Custom HTTP + processing pipeline | `PuzzleImageService.resolve_fragment_image()` | Already handles IIIF fetch, bg removal, disk caching |
| Fragment data model | Ad-hoc dict/tuple state | `PuzzleFragment` dataclass from `shared/puzzle_model.py` | JSON-serializable, typed, shared with future web app |
| Shelfmark autocomplete | Custom prefix matching | Existing `ShelfmarkCompleter` + `shelf_model` | Already built, normalizes shelfmarks, ~217K entries |
| Shelfmark->sys_id resolution | Manual CSV lookup | `_shelf_to_sys` map + `normalize_shelfmark()` | Already loaded on app startup |
| Scene graph / transforms | Manual coordinate math | QGraphicsScene + QGraphicsItem transform system | Qt handles transform composition, hit testing, rendering |

**Key insight:** Phase 47 delivered all the services. This phase is pure GUI work -- no new service layer needed.

## Common Pitfalls

### Pitfall 1: RGBA PNG to QPixmap Transparency
**What goes wrong:** Loading RGBA PNG bytes into QPixmap loses alpha channel if not handled correctly
**Why it happens:** QPixmap.loadFromData() may not preserve alpha on all platforms; QImage intermediary needed
**How to avoid:** Load bytes into QImage with Format_ARGB32 first, then convert to QPixmap
**Warning signs:** Fragments appear with black/white backgrounds instead of transparent
```python
# CORRECT:
image = QImage()
image.loadFromData(rgba_bytes)  # Auto-detects PNG RGBA
pixmap = QPixmap.fromImage(image)
# QPixmap will preserve alpha from the QImage
```

### Pitfall 2: Transform Origin After Flip
**What goes wrong:** Flipping a QGraphicsPixmapItem via QTransform.scale(-1,1) shifts the item position
**Why it happens:** scale(-1,1) mirrors around x=0, so the item ends up at negative x coordinates
**How to avoid:** After flip, translate by the item width/height to compensate. Or use setTransformOriginPoint() at center and compose transforms carefully.
**Warning signs:** Fragments jump to unexpected positions after flip

### Pitfall 3: Mouse Event Routing (View vs Item)
**What goes wrong:** View-level pan (ScrollHandDrag) fights with item-level drag (ItemIsMovable)
**Why it happens:** QGraphicsView.DragMode.ScrollHandDrag consumes mouse events before items get them
**How to avoid:** Set DragMode.NoDrag by default. Implement custom view mousePressEvent: if click hits an item, let item handle it; if click hits empty canvas, enable hand-drag pan.
**Warning signs:** Cannot drag fragments because view eats the mouse event, or cannot pan because items eat it

### Pitfall 4: Wheel Event Routing (View Zoom vs Item Resize)
**What goes wrong:** Ctrl+wheel for view zoom AND plain wheel for fragment resize need to coexist
**Why it happens:** QGraphicsView.wheelEvent fires before item wheelEvent
**How to avoid:** Override view wheelEvent: if Ctrl pressed, zoom view; if mouse is over an item (use itemAt()), forward to item for resize; else ignore.
**Warning signs:** Scrolling over a fragment zooms the whole canvas instead of resizing the fragment

### Pitfall 5: Thread Safety for PuzzleImageService
**What goes wrong:** Multiple fragments loading simultaneously from different threads
**Why it happens:** PuzzleImageService uses `requests.get()` which is thread-safe, but file writes to cache could race
**How to avoid:** Each PuzzleImageLoaderThread creates its own service call. The service's file caching uses unique filenames per (fl_id, size, threshold), so no race conditions in practice.
**Warning signs:** Corrupted cache files if two threads write same path simultaneously (unlikely given unique keys)

### Pitfall 6: Scene Rect Growth
**What goes wrong:** QGraphicsScene sceneRect is fixed and fragments can't be dragged outside it
**Why it happens:** If sceneRect is set explicitly, items are confined
**How to avoid:** Call `scene.setSceneRect(QRectF())` to let it auto-grow, or set a very large rect (e.g., -10000,-10000 to 10000,10000)
**Warning signs:** Fragments "stuck" at canvas edges

### Pitfall 7: sip.isdeleted Check for Async Callbacks
**What goes wrong:** Async image load callback fires after the PuzzleCanvasWindow is closed
**Why it happens:** QThread finishes after window is destroyed; signal fires on deleted widget
**How to avoid:** Check `sip.isdeleted(self)` in slot handlers, and call `thread.wait()` in closeEvent
**Warning signs:** Crash with "wrapped C++ object has been deleted"

## Code Examples

### Loading RGBA PNG Into QGraphicsPixmapItem
```python
# Source: PyQt6 QImage/QPixmap documentation
def _bytes_to_pixmap(self, image_bytes: bytes) -> QPixmap:
    """Convert RGBA PNG bytes to QPixmap preserving transparency."""
    image = QImage()
    if not image.loadFromData(image_bytes):
        return QPixmap()
    return QPixmap.fromImage(image)
```

### Checkerboard Background Pattern
```python
# Source: Qt documentation for custom scene backgrounds
def drawBackground(self, painter, rect):
    """Draw checkerboard or solid background."""
    if self._checkerboard:
        tile_size = 20
        light = QColor(200, 200, 200)
        dark = QColor(150, 150, 150)
        left = int(rect.left()) - (int(rect.left()) % tile_size)
        top = int(rect.top()) - (int(rect.top()) % tile_size)
        for x in range(left, int(rect.right()), tile_size):
            for y in range(top, int(rect.bottom()), tile_size):
                if (x // tile_size + y // tile_size) % 2 == 0:
                    painter.fillRect(x, y, tile_size, tile_size, light)
                else:
                    painter.fillRect(x, y, tile_size, tile_size, dark)
    else:
        painter.fillRect(rect, QColor(0x33, 0x33, 0x33))
```

### Multi-Select Group Operations
```python
# Apply transform to all selected items
def _rotate_selected(self, angle_delta):
    """Rotate all selected fragments by angle_delta degrees."""
    selected = [item for item in self.scene.selectedItems()
                if isinstance(item, PuzzleFragmentItem)]
    if not selected:
        return
    # Calculate group center
    center = QPointF(0, 0)
    for item in selected:
        center += item.sceneBoundingRect().center()
    center /= len(selected)

    for item in selected:
        # Rotate item around its own center
        item.setRotation((item.rotation() + angle_delta) % 360)
        item.puzzle_frag.rotation = item.rotation()
```

### Shelfmark to FL_ID Resolution Chain
```python
# Reuse existing infrastructure:
# 1. ShelfmarkCompleter provides autocomplete on QLineEdit
# 2. normalize_shelfmark() -> _shelf_to_sys[norm] -> sys_id
# 3. sys_id -> MetadataManager.get_fl_ids(sys_id) -> [fl_id, ...]
# 4. fl_id -> PuzzleImageService.resolve_fragment_image(fl_id) -> RGBA bytes
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Single QGraphicsPixmapItem (ZoomableScrollArea) | Multiple independent items with per-item transforms | Phase 48 | New canvas widget, not extending existing |
| Slider-based rotation | Corner handle drag rotation | Phase 48 decision | More intuitive for image manipulation |
| View-level flip/rotate | Per-item flip/rotate/scale | Phase 48 | Each fragment independently transformable |

## Open Questions

1. **FL_ID Resolution for Non-NLI Libraries**
   - What we know: PuzzleImageService._fetch_iiif_image() constructs NLI IIIF URLs from fl_id digits
   - What's unclear: Cambridge, Manchester, JTS images use different URL schemes. How does "Add to Puzzle" work for non-NLI fragments?
   - Recommendation: For Phase 48, support NLI images only (vast majority of corpus). Non-NLI support can be added in a follow-up. The PuzzleImageService can be extended later.

2. **Multi-Select Group Rotation Center**
   - What we know: User wants multi-select with group transforms
   - What's unclear: Should group rotation rotate around the geometric center of selected items, or around the first-selected item?
   - Recommendation: Use geometric center (average of all selected item centers). This is the standard behavior in image editors.

3. **Grid Snap Increment**
   - What we know: Shift+drag snaps to grid
   - What's unclear: What grid size? 10px? 20px? Configurable?
   - Recommendation: 20px default (reasonable for 800px images). Not user-configurable for now.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | pytest.ini (if exists) or pyproject.toml |
| Quick run command | `pytest tests/test_puzzle_model.py tests/test_puzzle_image_service.py tests/test_puzzle_service.py -x -q` |
| Full suite command | `pytest tests/ -x -q` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CANV-01 | Add fragment by shelfmark | manual-only | N/A (requires GUI interaction) | N/A |
| CANV-03 | Drag fragments freely | manual-only | N/A (requires mouse events) | N/A |
| CANV-04 | Rotate fragment to any angle | manual-only | N/A (requires mouse events) | N/A |
| CANV-05 | Flip fragment H/V | manual-only | N/A (requires mouse events) | N/A |
| CANV-06 | Resize fragment independently | manual-only | N/A (requires mouse events) | N/A |
| PLAT-02 | Puzzle works in desktop app | manual-only | N/A (requires full app) | N/A |

**Note:** All Phase 48 requirements are GUI interaction requirements. Unit tests for the underlying data model and image service already exist from Phase 47. The canvas widget behavior requires manual testing with visual verification (drag, rotate, flip need a running QApplication). Automated GUI testing with QTest could be added but is not standard in this project.

### Sampling Rate
- **Per task commit:** `pytest tests/test_puzzle_model.py tests/test_puzzle_image_service.py tests/test_puzzle_service.py -x -q`
- **Per wave merge:** `pytest tests/ -x -q`
- **Phase gate:** Full suite green + manual visual verification of all 5 canvas interactions

### Wave 0 Gaps
None -- existing test infrastructure covers the service layer. GUI testing is manual-only for this phase.

## Sources

### Primary (HIGH confidence)
- `shared/puzzle_model.py` -- Phase 47 PuzzleFragment/PuzzleDocument dataclasses (read directly)
- `shared/puzzle_image_service.py` -- Phase 47 IIIF fetch + bg removal + cache (read directly)
- `shared/background_removal.py` -- Phase 47 HSV bg removal engine (read directly)
- `genizah_app.py` lines 1391-1573 -- ZoomableScrollArea pattern (read directly)
- `genizah_app.py` lines 2114+ -- ImageLoaderThread pattern (read directly)
- `genizah_app.py` lines 8590-8601 -- ShelfmarkCompleter setup (read directly)
- `gui_threads.py` -- All worker thread patterns (read directly)
- PyQt6 6.10.2 -- Confirmed installed version

### Secondary (MEDIUM confidence)
- Qt6 QGraphicsItem documentation for ItemIsMovable, setTransformOriginPoint, transform composition
- Qt6 QGraphicsView documentation for DragMode, wheelEvent routing

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - PyQt6 already used, all Phase 47 services exist and were read
- Architecture: HIGH - QGraphicsScene is the canonical Qt approach; patterns verified from existing code
- Pitfalls: HIGH - Based on direct analysis of existing codebase patterns and known Qt behaviors

**Research date:** 2026-03-16
**Valid until:** 2026-04-16 (stable -- PyQt6 and project patterns are well-established)
