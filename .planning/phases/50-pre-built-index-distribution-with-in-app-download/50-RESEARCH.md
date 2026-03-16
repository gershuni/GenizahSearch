# Phase 50: Join Documents - Research

**Researched:** 2026-03-16
**Domain:** Puzzle document persistence, composite image export, NiceGUI/PyQt6 UI panels
**Confidence:** HIGH

## Summary

Phase 50 wires the existing `PuzzleService` CRUD operations (already implemented in Phase 47) into both the web and desktop puzzle canvases, adding save/load UI, a document management side panel, auto-save behavior, and composite image export. The core persistence layer is already complete -- this phase is primarily UI integration work with one significant technical challenge: server-side image compositing at full IIIF resolution.

The scratch pad model (unnamed until explicitly saved, then auto-save) is well-defined by user decisions. The side panel uses standard NiceGUI `ui.left_drawer` (web) and `QDockWidget` (desktop). Composite export uses Pillow for server-side compositing with full-resolution IIIF images, producing transparent RGBA PNG auto-cropped to content bounds.

**Primary recommendation:** Structure as 4 plans: (1) schema cleanup + save/load core wiring, (2) side panel document management UI for both apps, (3) composite image export service + UI, (4) metadata editing + auto-save + scratch pad recovery.

<user_constraints>

## User Constraints (from CONTEXT.md)

### Locked Decisions
- Canvas starts as an **unnamed scratch pad** -- auto-recovered on crash but not saved to documents list
- User clicks **"Save Join"** to persist -- prompted for title and optional notes
- Once saved, all further changes **auto-save** to that document (every meaningful change)
- **"New Puzzle"** button clears canvas back to empty scratch pad
- **Loading** a saved document replaces the current canvas with save prompt if scratch pad has unsaved work
- Title **auto-suggested from shelfmarks** joined by ' + '
- **No join type field** -- every puzzle join is physical by definition. Drop `join_type` or hardcode to 'physical'.
- **Collapsible side panel** (left side) showing saved documents list with title, thumbnail, shelfmarks, last-edited date
- Click to load, double-click title to rename inline, delete button per item with confirmation
- Sorted by last-edited date (most recent first)
- **Full-resolution IIIF** images re-fetched for export (not canvas previews)
- **Transparent PNG** (RGBA) -- fragments on transparent background, auto-cropped to content bounds
- Progress indicator during export
- Title + notes only (no join type classification)
- Editable in a **details section** at bottom of side panel after saving
- Fragment list displayed read-only in details section

### Claude's Discretion
- Thumbnail generation approach (render from cached images or snapshot canvas)
- Thumbnail cache strategy and size
- Exact side panel width and collapse animation
- Scratch pad recovery mechanism (sessionStorage, temp row in joins.db, or app.storage.tab)
- Export compositing approach (Pillow on server for both apps, or canvas.toDataURL for web)
- Progress indicator style during export
- Exact auto-crop margin size

### Deferred Ideas (OUT OF SCOPE)
- Publish for community review -- Phase 52
- Recto/verso toggle -- Phase 51
- Undo/redo -- deferred (CANV-09)
- Z-order layer panel -- deferred (CANV-10)

</user_constraints>

<phase_requirements>

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| JDOC-01 | User can save a puzzle arrangement as a join document | PuzzleService.save_document() already exists; need UI wiring (save dialog, title auto-suggest, scratch pad to saved transition) |
| JDOC-02 | User can load a previously saved join document | PuzzleService.load_document() already exists; need side panel list UI + canvas state replacement logic |
| JDOC-03 | Join document stores fragment IDs, positions, rotations, scales, and flip state | PuzzleFragment dataclass already stores all fields; fragments_json column preserves full state |
| JDOC-04 | User can export a composite image of the assembled join | New: full-res IIIF fetch + Pillow RGBA compositing + auto-crop + download delivery |
| JDOC-05 | User can add metadata (join type, notes) to a join document | Schema already has title/notes/join_type; simplify to title+notes only per user decision |

</phase_requirements>

## Standard Stack

### Core (Already in Project)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Pillow (PIL) | 10.x | Image compositing, rotation, RGBA handling, PNG export | Already in project for bg removal; handles all compositing needs |
| NiceGUI | 2.x | Web UI -- left drawer, dialogs, cards, buttons | Already the web framework |
| PyQt6 | 6.x | Desktop UI -- QDockWidget, QListWidget, QGraphicsScene | Already the desktop framework |
| Fabric.js | 6.4.3 | Web canvas -- already loaded via CDN | Already used for puzzle canvas |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| NumPy | existing | Array operations for image compositing | Coordinate transforms during export |
| io.BytesIO | stdlib | In-memory image buffers | Thumbnail generation, export buffering |
| base64 | stdlib | Encode thumbnails for storage/display | Store thumbnail as base64 in joins.db or serve to web |
| math | stdlib | Rotation/transform calculations for compositing | sin/cos for rotated bounding box calculation |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Pillow server-side export | canvas.toDataURL (web only) | Canvas captures are low-res (800px); full-res requires server-side Pillow for both apps |
| Base64 thumbnails in DB | Thumbnail files on disk | DB storage is simpler, no file cleanup needed; ~5KB per 150px thumbnail is negligible |
| QDockWidget (desktop) | QSplitter with collapsible panel | QDockWidget is more standard for tool panels but can be undocked; QSplitter stays in-place -- either works |

## Architecture Patterns

### Recommended Project Structure
```
shared/
  puzzle_model.py        # PuzzleDocument, PuzzleFragment (MODIFY: default join_type='physical')
  puzzle_service.py      # CRUD operations (MODIFY: add thumbnail_b64 column, update list_documents)
  puzzle_image_service.py # IIIF fetch + bg removal (EXISTING: use for full-res export)
  puzzle_export.py       # NEW: composite image assembly logic
web/
  pages/puzzle.py        # MODIFY: add save/load/export UI, left drawer, auto-save
  api.py                 # MODIFY: add export endpoint
genizah_app.py           # MODIFY: add QDockWidget panel, save/load/export to PuzzleCanvasWindow
```

### Pattern 1: Scratch Pad State Machine
**What:** Canvas operates in two modes: "scratch" (unnamed, not in documents list) and "saved" (has doc_id, auto-saves)
**When to use:** Always -- this is the core save model
**Example:**
```python
class PuzzleCanvasState:
    """Track whether current canvas is scratch pad or saved document."""
    def __init__(self):
        self.current_doc_id: Optional[str] = None  # None = scratch pad
        self.has_unsaved_changes: bool = False      # For scratch pad save prompt

    @property
    def is_scratch(self) -> bool:
        return self.current_doc_id is None

    def mark_saved(self, doc_id: str):
        self.current_doc_id = doc_id
        self.has_unsaved_changes = False

    def mark_changed(self):
        self.has_unsaved_changes = True

    def reset_to_scratch(self):
        self.current_doc_id = None
        self.has_unsaved_changes = False
```

### Pattern 2: Server-Side Composite Export
**What:** Re-fetch full-resolution IIIF images, apply transforms (rotation, flip, scale), composite onto transparent RGBA canvas, auto-crop, save as PNG
**When to use:** Export button clicked
**Example:**
```python
from PIL import Image
import math

def compose_puzzle_export(fragments: list, image_service, margin: int = 20) -> Image.Image:
    """
    Compose full-resolution puzzle export.

    1. For each fragment: fetch full-res image, apply bg removal, rotate, flip, scale
    2. Calculate bounding box of all transformed fragments
    3. Create RGBA canvas sized to bounding box + margin
    4. Paste each transformed fragment at correct position
    5. Auto-crop to content bounds
    """
    transformed = []
    for frag in fragments:
        # Fetch full-res (no size limit)
        img_bytes = image_service.resolve_fragment_image(
            frag.fl_id, size=None, threshold=frag.bg_removal_threshold, processed=True
        )
        img = Image.open(io.BytesIO(img_bytes)).convert('RGBA')

        # Apply scale
        if frag.scale != 1.0:
            new_w = int(img.width * frag.scale)
            new_h = int(img.height * frag.scale)
            img = img.resize((new_w, new_h), Image.LANCZOS)

        # Apply flip
        if frag.flip_h:
            img = img.transpose(Image.FLIP_LEFT_RIGHT)
        if frag.flip_v:
            img = img.transpose(Image.FLIP_TOP_BOTTOM)

        # Apply rotation (expand=True to avoid clipping)
        if frag.rotation != 0:
            img = img.rotate(-frag.rotation, expand=True, resample=Image.BICUBIC)

        transformed.append((frag, img))

    # Calculate canvas bounds
    # ... (compute min/max x,y considering rotated dimensions)

    # Create output canvas and composite
    canvas = Image.new('RGBA', (width + 2*margin, height + 2*margin), (0, 0, 0, 0))
    for frag, img in transformed:
        # Paste at offset position
        canvas.paste(img, (offset_x, offset_y), img)  # img as mask for alpha

    # Auto-crop to non-transparent content
    bbox = canvas.getbbox()
    if bbox:
        canvas = canvas.crop((
            max(0, bbox[0] - margin),
            max(0, bbox[1] - margin),
            min(canvas.width, bbox[2] + margin),
            min(canvas.height, bbox[3] + margin)
        ))

    return canvas
```

### Pattern 3: Auto-Save Debounce
**What:** After explicit save, automatically persist changes but debounce to avoid excessive writes
**When to use:** Every fragment move/rotate/flip/folio change on a saved document
**Example:**
```python
# Web (NiceGUI): use asyncio timer
import asyncio

class AutoSaver:
    def __init__(self, save_fn, delay=1.0):
        self._save_fn = save_fn
        self._delay = delay
        self._task: Optional[asyncio.Task] = None

    def schedule(self):
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = asyncio.create_task(self._delayed_save())

    async def _delayed_save(self):
        await asyncio.sleep(self._delay)
        await self._save_fn()

# Desktop (PyQt6): use QTimer.singleShot
from PyQt6.QtCore import QTimer

class DesktopAutoSaver:
    def __init__(self, save_fn, delay_ms=1000):
        self._save_fn = save_fn
        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.setInterval(delay_ms)
        self._timer.timeout.connect(self._save_fn)

    def schedule(self):
        self._timer.start()  # restarts if already running
```

### Pattern 4: Thumbnail Generation
**What:** Generate small preview thumbnails from cached canvas images for the side panel
**When to use:** On save (initial) and on auto-save (update)
**Example:**
```python
def generate_thumbnail(fragments: list, image_service, thumb_size: int = 150) -> bytes:
    """Generate a small composite thumbnail for the documents list."""
    # Use cached 800px images (already in disk cache from canvas display)
    # Same compositing logic as export but at small scale
    # Return PNG bytes (base64-encode for storage in DB)
    ...
```

### Anti-Patterns to Avoid
- **Canvas-captured thumbnails (web):** `canvas.toDataURL()` captures the viewport, not the logical arrangement. Fragments outside viewport are clipped. Use server-side rendering from cached images instead.
- **Synchronous full-res fetch on export:** Full-resolution IIIF images can be 5-20MB each. Always fetch asynchronously with progress updates.
- **Auto-save on every mouse move:** Would flood SQLite with writes. Debounce to end-of-interaction events (mouseup/drag end, rotation end).
- **Storing full composite images in DB:** Only store small thumbnails (~5KB). Full exports are generated on demand.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Image rotation with alpha | Manual pixel math | `PIL.Image.rotate(expand=True)` | Handles bounding box expansion, alpha interpolation, resampling correctly |
| Image compositing with transparency | Manual alpha blending | `PIL.Image.paste(img, pos, mask=img)` | Pillow's alpha-aware paste handles premultiplied alpha correctly |
| Auto-crop to content | Scan pixels for non-transparent bounds | `PIL.Image.getbbox()` | Built-in, fast C implementation |
| Debounced timer (web) | Custom setTimeout wrapper | `asyncio.create_task` with cancel | Native Python async pattern |
| Debounced timer (desktop) | threading.Timer | `QTimer.singleShot` | Thread-safe, integrates with Qt event loop |
| File save dialog (desktop) | Custom path input | `QFileDialog.getSaveFileName` | Native OS file picker with proper filters |
| File download (web) | Custom download endpoint | `ui.download` or `app.add_media_file` | NiceGUI has built-in download mechanism |

**Key insight:** The compositing math (translating canvas coordinates to Pillow pixel coordinates, handling rotation pivot points, computing bounding boxes of rotated rectangles) is the only genuinely complex part. Everything else uses existing libraries.

## Common Pitfalls

### Pitfall 1: Rotation Pivot Point Mismatch
**What goes wrong:** Canvas rotates around fragment center; Pillow `rotate()` expands the image and the center shifts. Exported positions don't match canvas positions.
**Why it happens:** `Image.rotate(expand=True)` changes the image dimensions, so the fragment's anchor point (center) moves relative to the image origin.
**How to avoid:** After rotation, calculate the new center offset: `new_center = (rotated.width / 2, rotated.height / 2)`. Position the fragment so that its center matches the canvas (x, y) coordinate.
**Warning signs:** Exported image has fragments offset from their canvas positions.

### Pitfall 2: Scale Factor Coordinate Mapping
**What goes wrong:** Canvas images are 800px; full-res might be 4000px. If fragment positions are stored in canvas pixels, export coordinates are wrong.
**Why it happens:** Positions (x, y) are in canvas coordinate space where images are ~800px. Full-res images are larger.
**How to avoid:** Normalize positions relative to image dimensions, or apply a uniform scale factor to both image sizes and positions during export. The simplest approach: compute the ratio `full_res_width / canvas_width` per fragment and scale positions accordingly.
**Warning signs:** Fragments in export are bunched together or too spread out.

### Pitfall 3: IIIF Full-Resolution Size Parameter
**What goes wrong:** Requesting max resolution from IIIF servers may return enormous images (10000+ px) or get rate-limited.
**Why it happens:** NLI/Cambridge IIIF servers have different max size policies.
**How to avoid:** Cap full-resolution at a reasonable maximum (e.g., `!4000,4000` or `!3000,3000`). This gives much better quality than 800px while staying within server limits. The `resolve_fragment_image` service already accepts a `size` parameter.
**Warning signs:** Export takes extremely long, HTTP 429 errors, or enormous output files.

### Pitfall 4: Web File Download Timing
**What goes wrong:** NiceGUI's download mechanism fails if called before the file is fully generated.
**Why it happens:** Image compositing is CPU-bound and takes time; UI thread must not block.
**How to avoid:** Run compositing in `run.io_bound()` (or `run.cpu_bound()` for heavy Pillow work), then trigger download in the callback. Show progress indicator during generation.
**Warning signs:** Empty downloads, browser timeout, frozen UI.

### Pitfall 5: Scratch Pad Loss on Tab Close (Web)
**What goes wrong:** User closes browser tab, loses scratch pad work.
**Why it happens:** `app.storage.tab` is ephemeral (destroyed when tab closes).
**How to avoid:** For crash recovery, periodically save scratch pad state to `app.storage.tab` (already done in Phase 49 for canvas state). For explicit "I want to keep this", the Save Join button is the mechanism. This is acceptable -- scratch pad is explicitly not persistent.
**Warning signs:** User complaints about lost work (mitigated by the save prompt on load).

### Pitfall 6: Schema Migration for Existing joins.db
**What goes wrong:** Adding `thumbnail_b64` column to existing `join_documents` table fails if ALTER TABLE is not handled.
**Why it happens:** Phase 47 already created the schema. Adding a column requires migration.
**How to avoid:** Use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` (SQLite 3.35+) or check for column existence first. Alternatively, increment schema_version and handle migration in `_init_schema`.
**Warning signs:** Crash on startup for users who already have joins.db from Phase 48/49.

## Code Examples

### NiceGUI Left Drawer for Document List
```python
# In puzzle.py page setup
with ui.left_drawer(value=False).classes('w-80') as drawer:
    ui.label('Saved Joins').classes('text-h6 q-mb-sm')
    docs_container = ui.column().classes('w-full')

    async def refresh_documents_list():
        docs_container.clear()
        docs = await run.io_bound(puzzle_service.list_documents)
        with docs_container:
            for doc in docs:
                with ui.card().classes('w-full cursor-pointer q-mb-sm') as card:
                    card.on('click', lambda d=doc: load_document(d['id']))
                    with ui.row().classes('items-center w-full'):
                        # Thumbnail placeholder
                        ui.image(f'/api/puzzle/thumbnail/{doc["id"]}').classes('w-16 h-16')
                        with ui.column().classes('flex-grow'):
                            ui.label(doc['title']).classes('text-subtitle1')
                            ui.label(doc['updated_at']).classes('text-caption text-grey')
                    with ui.row().classes('justify-end'):
                        ui.button(icon='delete', on_click=lambda d=doc: confirm_delete(d['id']))
```

### PyQt6 QDockWidget for Document Panel
```python
# In PuzzleCanvasWindow.__init__
self.docs_dock = QDockWidget("Saved Joins", self)
self.docs_dock.setAllowedAreas(Qt.DockWidgetArea.LeftDockWidgetArea)
self.docs_list = QListWidget()
self.docs_list.itemClicked.connect(self._on_doc_clicked)
self.docs_dock.setWidget(self.docs_list)
self.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, self.docs_dock)
```

### Save Dialog with Auto-Suggested Title
```python
def auto_suggest_title(fragments: list) -> str:
    """Generate title from fragment shelfmarks joined by ' + '."""
    shelfmarks = []
    for f in fragments:
        if f.shelfmark and f.shelfmark not in shelfmarks:
            shelfmarks.append(f.shelfmark)
    return ' + '.join(shelfmarks) if shelfmarks else 'Untitled Join'
```

### Pillow Composite with Proper Rotation Handling
```python
def _transform_fragment_image(img: Image.Image, frag) -> tuple:
    """Apply scale, flip, rotation. Returns (transformed_img, center_offset)."""
    # Scale
    if frag.scale != 1.0:
        img = img.resize(
            (int(img.width * frag.scale), int(img.height * frag.scale)),
            Image.LANCZOS
        )

    # Flip
    if frag.flip_h:
        img = img.transpose(Image.FLIP_LEFT_RIGHT)
    if frag.flip_v:
        img = img.transpose(Image.FLIP_TOP_BOTTOM)

    # Pre-rotation center
    cx, cy = img.width / 2, img.height / 2

    # Rotate with expand
    if frag.rotation != 0:
        img = img.rotate(-frag.rotation, expand=True, resample=Image.BICUBIC)

    # Post-rotation center
    new_cx, new_cy = img.width / 2, img.height / 2

    return img, (new_cx - cx, new_cy - cy)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Canvas toDataURL export | Server-side Pillow compositing | Project decision (Phase 50) | Full-res output vs viewport-limited capture |
| Store composite in DB | Generate on demand, store only thumbnails | Project decision (Phase 50) | Avoids multi-MB blobs in SQLite |
| Client-side file save (web) | Server-side generation + NiceGUI download | NiceGUI pattern | Works cross-browser reliably |

## Open Questions

1. **Full-resolution IIIF cap size**
   - What we know: 800px is canvas preview, full-res can be very large
   - What's unclear: Optimal cap for export quality vs file size vs server limits
   - Recommendation: Use `!3000,3000` as default export resolution. Good enough for publication, reasonable file size.

2. **Thumbnail storage location**
   - What we know: Need small previews in document list
   - What's unclear: Store as base64 in joins.db column, or as separate files on disk, or generate on the fly
   - Recommendation: Store as base64 TEXT column in `join_documents` table (~5KB per 150px thumbnail). Simple, no file management. Generate from cached 800px images.

3. **Canvas-to-export coordinate mapping precision**
   - What we know: Canvas (x,y) are in 800px image coordinate space
   - What's unclear: Whether all fragment transforms are stored in canvas coords or normalized
   - Recommendation: Check Phase 49 web puzzle.py to verify coordinate system. Likely canvas pixels -- apply ratio scaling for export.

4. **Web export download mechanism**
   - What we know: NiceGUI has `ui.download()` and `app.add_media_file()`
   - What's unclear: Best pattern for large generated files
   - Recommendation: Generate in `run.cpu_bound()`, save to temp file, serve via media route, trigger download.

## Sources

### Primary (HIGH confidence)
- `shared/puzzle_model.py` -- read directly; PuzzleDocument/PuzzleFragment dataclass definitions confirmed
- `shared/puzzle_service.py` -- read directly; full CRUD implementation confirmed with schema
- `50-CONTEXT.md` -- user decisions and constraints
- `REQUIREMENTS.md` -- JDOC-01 through JDOC-05 confirmed

### Secondary (MEDIUM confidence)
- Pillow Image.rotate, Image.paste, Image.getbbox -- well-known stable API, verified from training data
- NiceGUI ui.left_drawer, ui.download -- standard NiceGUI components, used elsewhere in project
- PyQt6 QDockWidget -- standard Qt widget, used in many desktop apps

### Tertiary (LOW confidence)
- IIIF max resolution limits per server -- varies by institution; cap recommendation is conservative estimate
- NiceGUI run.cpu_bound for Pillow operations -- need to verify this is available vs run.io_bound

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already in project, no new dependencies
- Architecture: HIGH -- persistence layer already built, UI patterns well-established in project
- Pitfalls: HIGH -- rotation/coordinate mapping is the known hard problem, well-documented
- Export compositing: MEDIUM -- Pillow API is stable but coordinate mapping needs careful implementation

**Research date:** 2026-03-16
**Valid until:** 2026-04-16 (stable domain, no fast-moving dependencies)
