# Architecture Patterns: Fragment Puzzle / Jigsaw Join Tool

**Domain:** Visual fragment assembly tool for manuscript research platform
**Researched:** 2026-03-15
**Confidence:** HIGH (based on existing codebase analysis, verified framework capabilities)

## Recommended Architecture

The fragment puzzle tool adds a **canvas-based visual assembly layer** on top of the existing dual-app architecture. The key architectural decision: canvas implementations are entirely separate (JavaScript for web, QGraphicsScene for desktop) while sharing a common data model and image processing pipeline via `shared/puzzle_service.py`.

```
                        shared/puzzle_service.py
                        shared/background_removal.py
                        (data model, image processing,
                         IIIF metadata, serialization)
                              |
              +---------------+---------------+
              |                               |
    Web (NiceGUI)                    Desktop (PyQt6)
    Fabric.js canvas                 QGraphicsScene
    (custom JS component)           (PuzzleFragmentItem subclass)
    web/components/                  genizah_app.py or
      puzzle_canvas.py                puzzle_widget.py
      puzzle_canvas.js
              |                               |
              +---------------+---------------+
                              |
              +---------------+---------------+
              |                               |
    joins.db (local SQLite)        Supabase (published joins)
    (drafts, offline work)         + Storage (composite images)
```

### Component Boundaries

| Component | Responsibility | Communicates With |
|-----------|---------------|-------------------|
| `shared/puzzle_service.py` | Data model (PuzzleDocument, PuzzleFragment), DPI calibration, IIIF info fetch, composite image export, serialization | NliCrossrefService, FjmsService, joins.db, web+desktop canvases |
| `shared/background_removal.py` | OpenCV-based color segmentation and alpha mask generation | puzzle_service (called during fragment add) |
| `web/components/puzzle_canvas.py` + `.js` | NiceGUI custom component wrapping Fabric.js; drag/rotate/flip/scale, selection, toolbar | puzzle_service (data), web/api.py (image proxy) |
| Desktop `PuzzleWidget` (QWidget) | QGraphicsScene with movable PuzzleFragmentItem objects, toolbar, keyboard shortcuts | puzzle_service (data), ImageLoaderThread (image fetch) |
| `web/supabase_client.py` extensions | CRUD for published join_documents + Storage upload | Supabase cloud |
| `joins.db` (new SQLite sidecar) | Local persistence for join documents (drafts + published cache) | puzzle_service.py only |

### Data Flow

**Adding a fragment to the canvas:**

```
1. User selects manuscript (sys_id) from browse/search
2. puzzle_service.resolve_fragment_images(sys_id)
   -> NliCrossrefService.get_folio_images(sys_id) -> IIIF URLs + FL IDs
   -> fetch IIIF info.json for each -> {width, height}
3. Image loaded:
   - Web: browser loads via /api/nli_image proxy (existing)
   - Desktop: ImageLoaderThread with disk cache (existing)
4. Background removal (Python, server-side for both):
   -> background_removal.remove_background(image_bytes) -> RGBA PNG with alpha mask
   - Web: served via new /api/puzzle/process_image/{fl_id} endpoint
   - Desktop: called directly from puzzle_service, result -> QPixmap
5. Canvas creates interactive object:
   - Web: fabric.Image from data URL (supports alpha transparency)
   - Desktop: QGraphicsPixmapItem from QPixmap (supports alpha via ARGB32)
```

**Saving a join document:**

```
1. Canvas serializes fragment positions + transforms to JSON
   Each fragment: {sys_id, fl_id, x, y, rotation, scale, flip_h, flip_v}
2. Web: ui.run_javascript('JSON.stringify(canvas.toJSON())') -> Python
   Desktop: iterate scene.items(), extract transforms
3. puzzle_service.save_join_document(fragments_json, metadata)
   -> Always save to joins.db (local, immediate)
   -> If publishing: also upload to Supabase join_documents + Storage
4. Composite image rendered via Pillow (puzzle_service.export_composite())
   -> Stored locally and/or uploaded to Supabase Storage
```

**Recto/Verso toggle:**

```
1. User arranges recto -> positions saved as recto_layout
2. Toggle to verso -> auto-generate mirror layout:
   - Mirror all X positions around canvas center
   - Swap each fragment to its verso FL ID (NLI: S1=recto, S2=verso)
   - Load verso images (same sys_id, page+1 or S2 variant)
3. Verso layout independently editable
4. Join document stores both recto_layout and verso_layout
```

## Component Details

### 1. Web Canvas: Fabric.js via NiceGUI Custom Component

**Why Fabric.js:** Standard library for interactive canvas object manipulation. Built-in drag, rotate, scale, flip per object. Active maintenance, large community. The existing `advViewer` (search.py) is CSS transform-based -- it only handles a single image with zoom/pan/rotate. The puzzle needs true multi-object canvas manipulation.

**Integration with NiceGUI:** The project already uses `ui.run_javascript()` extensively (~20 call sites in search.py for advViewer). For the puzzle, create a proper custom component:

```
web/components/puzzle_canvas.py    -- Python NiceGUI Element subclass
web/components/puzzle_canvas.js    -- Fabric.js canvas logic (Vue component)
```

Python manages state and communicates via NiceGUI's `run_javascript()` / `emit()` bridge. JS handles all rendering and interaction. State of truth for visual positions lives in the JS canvas; Python requests it on save.

**Key Fabric.js features needed:**
- `fabric.Image` objects with per-object transforms
- `canvas.toJSON()` / `canvas.loadFromJSON()` for serialization
- `canvas.toDataURL()` for quick preview export
- Object controls (rotation handle, corner scale handles)
- Transparency (RGBA images with alpha from background removal)
- Z-order management (bring to front/send to back)

**Loading Fabric.js:** ~300KB minified from CDN via `ui.add_head_html('<script src="...">')`, same as other external JS.

**Example bridge pattern:**
```python
# Add fragment to canvas (Python -> JS)
await ui.run_javascript(f'''
    fabric.Image.fromURL("{processed_image_data_url}", function(img) {{
        img.set({{ left: 100, top: 100, angle: 0, fragmentId: "{fl_id}" }});
        canvas.add(img);
        canvas.setActiveObject(img);
        canvas.renderAll();
    }});
''')

# Get state for saving (Python <- JS)
state_json = await ui.run_javascript('JSON.stringify(canvas.toJSON())')
```

### 2. Desktop Canvas: QGraphicsScene with Custom Items

**Why QGraphicsScene:** Already in the codebase. `ZoomableScrollArea` (genizah_app.py:1391) demonstrates: QGraphicsView + QGraphicsScene + QGraphicsPixmapItem with pan/zoom, rotation, context menus. QGraphicsScene natively supports multiple items with independent transforms, z-ordering, and selection.

**Implementation:**

```python
class PuzzleFragmentItem(QGraphicsPixmapItem):
    """A single manuscript fragment on the puzzle canvas."""
    def __init__(self, pixmap: QPixmap, fragment_id: str, parent=None):
        super().__init__(pixmap, parent)
        self.fragment_id = fragment_id
        self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIsMovable)
        self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIsSelectable)
        self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemSendsGeometryChanges)
        self.setTransformOriginPoint(pixmap.width()/2, pixmap.height()/2)
        # Right-click context menu (existing pattern from ZoomableScrollArea)
        self.setAcceptHoverEvents(True)
```

**Reuse from ZoomableScrollArea:**
- Pan/zoom (Ctrl+wheel zoom at line 1527, drag mode at line 1400)
- Rotation (`setRotation()` at line 1520)
- Fit-to-viewport (`fitInView()` at line 1570)
- Context menu pattern (line 1427)

**Key extension:** Multiple movable items instead of a single pixmap. Add selection handles, rotation handle, and flip via QTransform with negative scale.

### 3. Shared Service: puzzle_service.py

Following `shared/document_service.py`, `shared/fjms_service.py` pattern:

```python
@dataclass
class PuzzleFragment:
    sys_id: str
    fl_id: str              # NLI FL ID for this side
    shelfmark: str
    side: str               # 'recto' or 'verso'
    paired_fl_id: str       # the other side's FL ID
    image_url: str          # IIIF URL
    width_px: int           # from info.json
    height_px: int
    dpi: float              # estimated or from IIIF physical dimensions
    x: float = 0.0          # canvas position (per-layout)
    y: float = 0.0
    rotation: float = 0.0
    scale: float = 1.0
    flip_h: bool = False
    flip_v: bool = False

@dataclass
class PuzzleDocument:
    id: str                  # UUID
    user_id: str
    title: str
    fragments: List[PuzzleFragment]
    recto_layout: dict
    verso_layout: dict
    join_type: str           # physical, content, uncertain
    notes: str
    status: str              # draft, proposed, confirmed
    created_at: str
    updated_at: str
    composite_recto_path: str
    composite_verso_path: str
```

### 4. Background Removal: shared/background_removal.py

**Approach: HSV color-based segmentation.** Manuscript photos from NLI, Cambridge, etc. have solid-color library backgrounds (dark blue, black, grey, green felt). Well-constrained problem for traditional CV.

**Why not ML (rembg/U-2-Net):** ~180MB model dependency. Overkill for solid-color backgrounds. Slower (seconds vs. milliseconds).

**Why not GrabCut:** Requires user-provided initial rectangle. Semi-interactive.

**Pipeline:**
```python
def remove_background(image_bytes: bytes, bg_color_hint: str = 'auto') -> bytes:
    """Returns RGBA PNG bytes with transparent background."""
    # 1. Decode to BGR numpy array
    # 2. Convert to HSV
    # 3. Sample 20x20 blocks at 4 corners -> median HSV = background color
    # 4. cv2.inRange() with tolerance (+/-15 H, +/-40 S, +/-40 V) -> mask
    # 5. Morphological cleanup (MORPH_CLOSE then MORPH_OPEN)
    # 6. Keep largest contour only
    # 7. GaussianBlur on mask edges for smooth alpha transition
    # 8. Composite RGBA = BGR + alpha mask
    # 9. Encode PNG, return bytes
```

**Performance:** ~100-300ms per 2000x3000px image. Parallelize with ThreadPoolExecutor for multi-fragment loading.

**Dependency:** `opencv-python-headless` (~40MB). Headless variant avoids GUI conflicts with PyQt6.

**Fallback:** If OpenCV unavailable or removal fails, show original image with opaque background. Users can still arrange fragments.

### 5. DPI Calibration

IIIF info.json provides `width` and `height` in pixels (required). Physical dimensions via optional `service` property with `physicalScale`/`physicalUnits` -- most Genizah servers do NOT provide this.

**Approach:**
1. Fetch info.json: `{iiif_base}/info.json` -> get native width/height
2. Check for physical dimensions service (bonus)
3. Fallback DPI by library: NLI ~400 PPI, Cambridge ~300-400 PPI, default 400 PPI
4. Allow user override per fragment
5. **Relative sizing is sufficient for Phase 1:** fragments from same library have consistent DPI, so pixel-ratio sizing is correct for visual alignment

### 6. Storage Architecture

**Local-first with optional cloud publish.** This follows the project's "offline-capable" principle.

**joins.db (new local SQLite sidecar):**
```sql
CREATE TABLE join_documents (
    id TEXT PRIMARY KEY,              -- UUID
    user_id TEXT,
    title TEXT,
    fragments TEXT NOT NULL,          -- JSON array of fragment descriptors
    recto_layout TEXT,                -- JSON {fl_id: {x, y, rotation, scale, flip_h, flip_v}}
    verso_layout TEXT,
    join_type TEXT DEFAULT 'uncertain',
    notes TEXT,
    status TEXT DEFAULT 'draft',
    composite_recto BLOB,             -- compressed PNG
    composite_verso BLOB,
    supabase_id TEXT,                 -- NULL until published
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
```

**Supabase (for published joins only):**
```sql
CREATE TABLE join_documents (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id UUID REFERENCES auth.users NOT NULL,
    title TEXT,
    fragments JSONB NOT NULL,
    recto_layout JSONB,
    verso_layout JSONB,
    join_type TEXT CHECK (join_type IN ('physical', 'content', 'uncertain')),
    notes TEXT,
    status TEXT DEFAULT 'proposed' CHECK (status IN ('proposed', 'confirmed')),
    composite_recto_url TEXT,
    composite_verso_url TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE join_documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Anyone reads published" ON join_documents
    FOR SELECT USING (true);
CREATE POLICY "Users insert own" ON join_documents
    FOR INSERT WITH CHECK (user_id = auth.uid());
CREATE POLICY "Users update own" ON join_documents
    FOR UPDATE USING (user_id = auth.uid());
```

**Supabase Storage bucket:** `puzzle-composites` with path `{user_id}/{doc_id}/recto.png`.

**Rationale for local-first:**
- Saves are instant (no network round-trip)
- Works offline (desktop users)
- Drafts never leave the user's machine
- Publishing is an explicit action (upload to Supabase)
- Same pattern as `session_persistence.py` for local + Supabase for community

**Relationship to existing fragment_joins table:**
- `fragment_joins`: pairwise A-B join claims (text metadata only, existing)
- `join_documents`: visual puzzle assemblies (images + positions, new)
- Coexist: puzzle tool creates visual evidence for textual join claims
- Optional link: join_document.notes can reference fragment_join IDs

## Patterns to Follow

### Pattern 1: Shared Service + App-Specific UI (Existing)
**What:** All data logic in `shared/`, all rendering in app-specific code.
**Applied here:** `shared/puzzle_service.py` + `shared/background_removal.py` handle data. Web (Fabric.js) and desktop (QGraphicsScene) are independent UIs consuming the same data model.

### Pattern 2: NiceGUI JavaScript Bridge (Existing)
**What:** Python manages state, JavaScript handles rendering via `ui.run_javascript()`.
**Applied here:** Canvas JS receives image data URLs from Python, emits state snapshots back on save. Same pattern as advViewer but formalized as a Vue component.

### Pattern 3: SQLite Sidecar for Local Data (Existing)
**What:** Structured data in SQLite, accessed via service module, auto-detect from project root.
**Applied here:** joins.db stores join documents locally. Same pattern as pgp.db, fjms_enrichment.db, nli_crossref.db.

### Pattern 4: Image Loading Through Existing Pipeline (Existing)
**What:** Reuse IIIF image loading (web: `/api/nli_image` proxy; desktop: `ImageLoaderThread` with disk cache).
**Applied here:** Fragment images load through the same proxy/thread as browse and search. Background removal is a post-processing step, not a parallel pipeline.

### Pattern 5: Graceful Degradation (Existing)
**What:** Feature works with reduced functionality when dependencies unavailable.
**Applied here:** No OpenCV -> show original images. No IIIF info.json -> use default DPI. No Supabase -> save locally only.

### Pattern 6: Async Image Loading (Existing)
**What:** Images loaded in background thread to avoid UI blocking.
**Applied here (desktop):** Reuse ImageLoaderThread. Each fragment triggers async load + background removal callback.
**Applied here (web):** Background removal via `run.io_bound()`. Processed image sent to browser as data URL.

## Anti-Patterns to Avoid

### Anti-Pattern 1: Shared Canvas Abstraction
**What:** Unified canvas API abstracting over Fabric.js and QGraphicsScene.
**Why bad:** Fundamentally different paradigms (DOM/JS events vs. Qt paint system). Leaky abstraction, limits both implementations.
**Instead:** Share data model only. Independent native canvas implementations.

### Anti-Pattern 2: Client-Side Background Removal for Web
**What:** Running OpenCV.js (~8MB) in the browser.
**Why bad:** Huge download. Slow without hardware acceleration. Inconsistent across browsers.
**Instead:** Background removal runs in NiceGUI Python process (same server, no network hop). Processed PNG served via image proxy.

### Anti-Pattern 3: Storing Full IIIF Images in SQLite
**What:** Storing original 2000px fragment images as BLOBs in joins.db.
**Why bad:** 500KB-2MB per image. 5-fragment join = 5-10MB per save. DB bloats.
**Instead:** Store only IIIF URLs + fragment metadata. Re-fetch from IIIF on load. Store ONLY the final composite as BLOB (single rendered output, compressed).

### Anti-Pattern 4: Bidirectional Real-Time State Sync
**What:** Mirroring every canvas state change to Python in real-time.
**Why bad:** High-frequency drag events create excessive WebSocket traffic. State goes out of sync.
**Instead:** Canvas is single source of truth for visual state. Python requests state only at save/export.

### Anti-Pattern 5: Server-Side Canvas Rendering
**What:** Rendering the interactive canvas on the server and streaming to browser.
**Why bad:** Latency on every interaction. Defeats visual puzzle purpose.
**Instead:** All canvas interaction is client-side. Server handles processing and persistence only.

## Integration Points with Existing Code

### Files to Modify

| File | Change | Reason |
|------|--------|--------|
| `web/supabase_client.py` | Add CRUD for published join_documents + Storage upload | Cloud persistence |
| `web/api.py` | Add `/api/puzzle/process_image/{fl_id}` endpoint | Serve background-removed RGBA images |
| `web/pages/browse.py` | Add "Open in Puzzle" button on manuscript view | Entry point from browse |
| `web/pages/search.py` | Add "Add to Puzzle" action in result menu | Entry point from search |
| `web/main.py` | Register `/puzzle` route | New page |
| `genizah_app.py` | Add puzzle tab/dialog launcher, toolbar action | Desktop entry point |
| `shared/nli_crossref_service.py` | Add `get_recto_verso_pairs(sys_id)` method | Map recto FL IDs to verso using S1/S2 |
| `supabase_corrections_client.py` | Add join_document publish/fetch (desktop) | Desktop Supabase access |

### New Files

| File | Purpose |
|------|---------|
| `shared/puzzle_service.py` | Data model, IIIF info, serialization, composite export |
| `shared/background_removal.py` | OpenCV HSV-based background removal |
| `web/components/puzzle_canvas.py` | NiceGUI Python component wrapper |
| `web/components/puzzle_canvas.js` | Fabric.js canvas (Vue component) |
| `web/pages/puzzle.py` | Puzzle workspace page |
| `tests/test_background_removal.py` | Background removal unit tests |
| `tests/test_puzzle_service.py` | Data model + serialization tests |

### Existing Infrastructure Reused Without Modification

| Component | How Reused |
|-----------|------------|
| `NliCrossrefService.get_folio_images()` | Get FL IDs and folio labels for manuscript |
| `NliCrossrefService.get_image_sources()` | Determine available image providers |
| `FjmsService.get_joins_for_fragment()` | Pre-populate with known FJMS join groups |
| `ImageLoaderThread` (desktop, line 2114) | Load fragment images with cache + fallback |
| `/api/nli_image/{fl_id}` proxy (web) | CORS-safe image fetch |
| `/api/cambridge_image/{sys_id}` proxy (web) | Cambridge IIIF images |
| `ZoomableScrollArea` (desktop, line 1391) | Reference for QGraphicsScene pan/zoom |
| Supabase auth + RLS patterns | User ownership, visibility |
| `session_persistence.py` pattern | Local session save/restore |
| `reading_desk_model.py` pattern | Multi-fragment data model reference |

## Scalability Considerations

| Concern | 2-3 fragments (typical) | 10+ fragments (large join) | Notes |
|---------|------------------------|---------------------------|-------|
| Canvas performance | Trivial | Both Fabric.js and QGraphicsScene handle dozens easily | Not a concern |
| Image memory (browser) | ~20MB (3 RGBA 2000px PNGs) | ~70MB | Within browser limits |
| Image memory (desktop) | ~20MB | ~70MB | Desktop has more headroom |
| Background removal | ~300ms x 3 = ~1s | ~300ms x 10 = ~3s (parallelize) | Show progress indicator |
| IIIF info.json | 3 HTTP GETs | 10 parallel GETs | Cache in memory |
| joins.db size | ~100KB per doc | Same | Composite BLOBs are main size factor |
| Supabase Storage | 2 composites ~1-2MB each | 2 composites ~3-5MB | Within free tier |

## Sources

- Existing codebase: `genizah_app.py` ZoomableScrollArea (line 1391), ImageLoaderThread (line 2114)
- Existing codebase: `web/api.py` image proxy endpoints
- Existing codebase: `shared/nli_crossref_service.py` (image metadata, FL IDs, folio parsing)
- Existing codebase: `web/supabase_client.py` fragment_joins CRUD pattern
- Existing codebase: `shared/reading_desk_model.py` multi-fragment data model
- [Fabric.js documentation](https://fabricjs.com/)
- [Qt QGraphicsScene documentation](https://doc.qt.io/qt-6/qgraphicsscene.html)
- [NiceGUI run_javascript](https://nicegui.io/documentation/run_javascript)
- [IIIF Image API 2.1 info.json specification](https://iiif.io/api/image/2.1/)
- [OpenCV background removal techniques](https://opencv.org/blog/remove-backgrounds-from-images-using-opencv/)
