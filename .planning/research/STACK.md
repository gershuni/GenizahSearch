# Technology Stack

**Project:** v7.0.0 Fragment Puzzle
**Researched:** 2026-03-15

## Scope

This document covers ONLY new stack additions for the fragment puzzle feature. The existing stack (NiceGUI, PyQt6, Tantivy, SQLite sidecars, Supabase, etc.) is validated and unchanged.

## Recommended Stack

### Web Canvas (NiceGUI) -- Fabric.js via embedded JavaScript

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Fabric.js | 6.x (CDN) | Canvas-based image manipulation: drag, rotate, flip, resize, z-order | Built-in interactive controls for object selection, rotation handles, scale handles. Mature library (10+ years), excellent documentation. v6 is ES-module based; v7 exists but v6 is more battle-tested. Use CDN -- no npm build step needed. |

**Integration pattern:** NiceGUI's `ui.add_head_html()` loads Fabric.js from CDN. A `<canvas>` element is injected via `ui.html()`. All canvas interaction is client-side JavaScript. Python communicates with the canvas via `ui.run_javascript()` (Python -> JS) and NiceGUI event handlers (JS -> Python). This is the same pattern the app already uses for zoomable images in browse.py and search.py (JavaScript image viewers with Python-driven URL loading).

**Why not NiceGUI's ui.interactive_image:** It provides click/hover events on a single image but has no multi-object canvas, no rotation handles, no z-ordering. The fragment puzzle requires manipulating multiple independent image objects simultaneously -- this is fundamentally a canvas application, not an image annotation tool.

**Why not ui.scene (3D):** NiceGUI's `ui.scene` is a Three.js-based 3D scene. Fragment assembly is a 2D problem. Using 3D adds unnecessary complexity with no benefit.

**Confidence:** HIGH -- Fabric.js is the standard answer for browser-based multi-object image manipulation. The NiceGUI integration pattern (HTML + JS + run_javascript) is already proven in this codebase.

### Desktop Canvas (PyQt6) -- QGraphicsScene

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| PyQt6 QGraphicsScene | (bundled) | Desktop canvas for fragment manipulation | Already in the codebase (ZoomableScrollArea). QGraphicsScene supports multiple items with independent transforms, drag, rotation, z-ordering. No new dependency needed. |
| QGraphicsPixmapItem (subclassed) | (bundled) | Individual fragment pieces on canvas | Each fragment becomes a custom QGraphicsPixmapItem with rotation handle, flip state, opacity. ItemIsMovable + ItemIsSelectable flags enable drag. Custom mouse events add rotation/resize handles. |

**Integration pattern:** Subclass QGraphicsPixmapItem into `FragmentPieceItem` with:
- `setFlag(ItemIsMovable)` and `setFlag(ItemIsSelectable)` for drag
- Custom paint override to draw rotation/resize handles when selected
- `setTransformOriginPoint()` for center-based rotation
- `mousePressEvent`/`mouseMoveEvent` overrides for handle interaction

The existing `ZoomableScrollArea` (line 1391 in genizah_app.py) already provides the QGraphicsView infrastructure with zoom, pan, and smooth rendering. The puzzle canvas extends this pattern.

**Confidence:** HIGH -- QGraphicsScene is Qt's designed solution for exactly this use case. Already partially implemented in the codebase.

### Image Processing -- Pillow + NumPy (background removal and compositing)

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Pillow | >=10.0 | Image loading, RGBA conversion, rotation, compositing, export | Already an indirect dependency (NiceGUI uses it). Provides Image.rotate(), Image.paste(), Image.alpha_composite(), and PNG export with alpha channel. |
| NumPy | >=1.24 | Pixel-level array operations for background removal | Fast vectorized color distance calculation. Convert PIL Image to numpy array, apply mask, convert back. Already an indirect dependency. |

**Why NOT OpenCV (cv2):** OpenCV is a 50+ MB dependency that adds BGR color space confusion and C++ build complexity. For solid-color background removal (the use case here -- library photograph backgrounds are uniform blue, green, gray, or white), simple HSV/RGB color distance thresholding with NumPy is sufficient and far simpler. OpenCV's GrabCut and contour detection are overkill when the background is a known solid color.

**Why NOT rembg / deep learning:** rembg uses U2-Net (175MB model download) for general-purpose background removal. Genizah fragment photos have solid-color library backgrounds -- a simple color threshold approach works better and is 100x faster. Deep learning models also sometimes clip manuscript edges, which is unacceptable for scholarly use.

**Background removal algorithm (recommended):**
1. Convert image to HSV color space (PIL -> NumPy array)
2. Sample corner pixels to detect dominant background color
3. Create binary mask: pixels within color_distance_threshold of background color = transparent
4. Apply morphological opening (small kernel) to clean mask edges -- this uses only NumPy, no OpenCV needed
5. Apply Gaussian-style edge feathering (3-5px) for natural edges -- scipy.ndimage or simple convolution
6. Set alpha channel from mask

This approach handles the four common Genizah photo backgrounds:
- NLI: medium gray (#808080-ish)
- Cambridge: dark background
- Oxford: cream/off-white
- Manchester: varies

**Confidence:** HIGH for Pillow+NumPy. MEDIUM for the specific algorithm -- will need tuning per library's background color. User-adjustable threshold is essential.

### DPI Calibration -- IIIF info.json + Physical Dimensions Service

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| IIIF Physical Dimensions Service | 1.0 | DPI/PPI calculation from image metadata | IIIF standard defines `physicalScale` + `physicalUnits` in info.json service blocks. Allows calculating real-world size: `physical_width = pixel_width * physicalScale`. |
| requests (existing) | (existing) | Fetch info.json from IIIF servers | Already in requirements.txt. |

**IIIF info.json endpoint:** `{base_url}/info.json` -- e.g., `https://iiif.nli.org.il/IIIFv21/FL7734473/info.json`

**Physical Dimensions Service JSON:**
```json
{
  "service": {
    "@context": "http://iiif.io/api/annex/services/physdim/1/context.json",
    "profile": "http://iiif.io/api/annex/services/physdim",
    "physicalScale": 0.0025,
    "physicalUnits": "in"
  }
}
```

**DPI calculation:** `DPI = 1.0 / physicalScale` (when units are inches).

**IMPORTANT caveat:** Most IIIF servers do NOT include the Physical Dimensions Service. NLI's IIIF server likely does not provide `physicalScale`. Cambridge CUDL may not either. This means DPI calibration will likely need a FALLBACK approach:

1. **Primary:** Check info.json for physicalScale service (ideal but rare)
2. **Fallback 1:** Use info.json `width`/`height` to get full-resolution pixel dimensions, then apply a per-library default DPI (e.g., NLI typically scans at 400 DPI, Cambridge at 300-400 DPI)
3. **Fallback 2:** Manual user calibration -- user sets a known measurement (e.g., "this edge is 15cm") and the tool calculates scale from that
4. **Fallback 3:** Relative sizing only -- fragments from the same library/scan batch are at the same DPI, so relative sizing is correct even without absolute DPI

**Confidence:** HIGH for the info.json fetch mechanism. LOW for physicalScale availability from NLI/Cambridge. The fallback chain is essential.

### Composite Image Export -- Pillow

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Pillow | >=10.0 | Render final composite PNG from assembled fragments | Create large canvas, paste each fragment with its transform (position, rotation, flip) using alpha compositing. Export as PNG (transparent background) or JPEG (white background). |

**Export pipeline (shared between web and desktop):**
1. Collect fragment states: `[{image_bytes, x, y, rotation, flip_h, flip_v, scale}]`
2. For each fragment: load as PIL Image, apply transforms (flip, rotate, scale)
3. Calculate bounding box of all transformed fragments
4. Create output canvas (RGBA) sized to bounding box
5. Paste each fragment using `Image.alpha_composite()` with position offset
6. Export as PNG (with alpha) or JPEG (flattened to white)

This runs server-side (web) or locally (desktop). No new dependencies needed.

**Confidence:** HIGH -- standard Pillow operations. Well-documented, well-tested.

### Join Document Persistence -- SQLite sidecar extension

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| SQLite (existing) | (existing) | Store join documents: metadata, fragment positions, composite images | Follows established sidecar pattern. Join documents are local-first (personal workspace), optionally publishable to Supabase for community review. |
| Supabase (existing) | (existing) | Published join documents for community features | Only used when user explicitly publishes. Existing auth + RLS pattern. |

**Schema sketch (joins.db or extend existing sidecar):**
```sql
CREATE TABLE join_documents (
    id TEXT PRIMARY KEY,          -- UUID
    title TEXT,
    notes TEXT,
    join_type TEXT,               -- 'physical', 'virtual', 'uncertain'
    created_at TEXT,
    updated_at TEXT,
    is_published INTEGER DEFAULT 0,
    composite_recto BLOB,         -- PNG bytes
    composite_verso BLOB,         -- PNG bytes (nullable)
    metadata_json TEXT            -- full state for reload
);

CREATE TABLE join_fragments (
    id INTEGER PRIMARY KEY,
    join_id TEXT REFERENCES join_documents(id),
    sys_id TEXT,                   -- manuscript system_number
    fl_id TEXT,                    -- fragment leaf ID
    shelfmark TEXT,
    side TEXT,                     -- 'recto' or 'verso'
    x REAL, y REAL,               -- position on canvas
    rotation REAL,                 -- degrees
    scale REAL,                    -- zoom factor
    flip_h INTEGER DEFAULT 0,
    flip_v INTEGER DEFAULT 0,
    z_order INTEGER,
    image_url TEXT                 -- source IIIF URL
);
```

**Confidence:** HIGH -- follows existing sidecar patterns exactly.

## Supporting Libraries (may need)

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| scipy.ndimage | >=1.10 | Gaussian blur for mask edge feathering | Only if NumPy-only convolution is too slow or produces artifacts. scipy is likely already installed as a transitive dependency. |

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Web canvas | Fabric.js (CDN) | Konva.js | Fabric.js has better documentation, more mature rotation/selection UX, larger community. Konva is good but Fabric specifically excels at object manipulation. |
| Web canvas | Fabric.js (CDN) | Raw HTML5 Canvas | Too much boilerplate for selection handles, hit testing, z-ordering. Fabric provides this out of the box. |
| Web canvas | Fabric.js (CDN) | Paper.js | Paper.js is vector-focused. Fragment puzzle is raster image manipulation. |
| Background removal | NumPy color threshold | OpenCV GrabCut | GrabCut is for complex backgrounds. Genizah photos have solid-color backgrounds. OpenCV adds 50MB+ dependency for no benefit. |
| Background removal | NumPy color threshold | rembg (U2-Net) | 175MB model, slower inference, sometimes clips manuscript edges. Overkill for solid backgrounds. |
| Desktop canvas | QGraphicsScene | Custom QWidget with QPainter | QGraphicsScene provides item management, hit testing, z-ordering, transform tracking for free. Custom painting reimplements all of this. |
| Composite export | Pillow | cairo/pycairo | Pillow already handles RGBA compositing. Cairo adds a system dependency for no benefit. |
| DPI calibration | IIIF info.json + fallbacks | Embedded EXIF DPI | IIIF images served via URL don't have EXIF. The IIIF API is the correct metadata source. |

## What NOT to Add

- **No npm/node build step.** Fabric.js loads from CDN via `<script>` tag. The project has no JavaScript build pipeline and should not add one.
- **No OpenCV.** Solid-color background removal does not need computer vision algorithms.
- **No deep learning models.** No rembg, no U2-Net, no ONNX runtime. Scholarly images need pixel-accurate edges, not ML approximations.
- **No new desktop dependencies.** PyQt6 already provides everything needed for the desktop canvas.
- **No WebSocket image streaming.** Fragment images load via existing IIIF URL patterns. The canvas manipulates them client-side (web) or locally (desktop).

## Installation

```bash
# No new pip packages required for core functionality.
# Pillow and NumPy are already indirect dependencies.

# To make them explicit in requirements.txt (recommended):
pip install Pillow numpy

# Optional (only if edge feathering needs Gaussian blur):
pip install scipy
```

**Web (Fabric.js):** Add to page head via NiceGUI:
```python
ui.add_head_html('<script src="https://cdn.jsdelivr.net/npm/fabric@6.4.3/dist/index.min.js"></script>')
```

**Desktop:** No new installation needed. PyQt6 QGraphicsScene is already available.

## Architecture Integration Points

### Web App (NiceGUI)
- **New page:** `web/pages/puzzle.py` -- fragment puzzle workspace
- **Canvas component:** Fabric.js canvas injected via `ui.html()`, controlled via `ui.run_javascript()`
- **Image loading:** Reuse existing IIIF URL resolution from `web/services.py` (`get_nli_image_url`, `get_oxford_image_url`)
- **Background removal:** Server-side Python (Pillow + NumPy), returns processed image as base64 data URL to canvas
- **Export:** Server-side Pillow compositing, served as downloadable file
- **State save/load:** JSON serialized canvas state via `ui.run_javascript('canvas.toJSON()')` -> Python -> SQLite

### Desktop App (PyQt6)
- **New widget:** `FragmentPuzzleWidget` extending QGraphicsView (similar pattern to ZoomableScrollArea at line 1391)
- **Fragment items:** `FragmentPieceItem(QGraphicsPixmapItem)` with rotation/resize handles
- **Image loading:** Reuse existing `ImageLoaderThread` (line 2114) for async IIIF fetch
- **Background removal:** Same Pillow + NumPy code as web (shared module)
- **Export:** Same Pillow compositing code as web (shared module)
- **State save/load:** Serialize fragment positions/transforms to JSON -> SQLite

### Shared Code (new module)
- **`shared/puzzle_service.py`** -- Fragment background removal, DPI calibration, composite export, join document persistence
- **`shared/background_removal.py`** -- Color threshold background removal (Pillow + NumPy)
- Follows existing service layer pattern (document_service.py, fjms_service.py, nli_crossref_service.py)

## Version Pinning Notes

| Library | Pin Strategy | Reason |
|---------|-------------|--------|
| Fabric.js | Pin to 6.x minor (e.g., 6.5.1) in CDN URL | v7 exists but v6 is stable. Pin CDN URL to avoid breaking changes. |
| Pillow | >=10.0 | Stable API, backward compatible. Any recent version works. |
| NumPy | >=1.24 | Stable API for array operations. |

## Sources

- [Fabric.js official site](https://fabricjs.com/) -- v6/v7 documentation, canvas API
- [Fabric.js npm](https://www.npmjs.com/package/fabric) -- v7.2.0 latest, v6.x still supported
- [Fabric.js releases](https://github.com/fabricjs/fabric.js/releases) -- version history
- [NiceGUI ui.run_javascript](https://nicegui.io/documentation/run_javascript) -- Python-JS communication
- [NiceGUI ui.html](https://nicegui.io/documentation/html) -- embedding custom HTML
- [Qt QGraphicsScene](https://doc.qt.io/qt-6/qgraphicsscene.html) -- scene management, transforms
- [Qt QGraphicsView](https://doc.qt.io/qt-6/qgraphicsview.html) -- viewport, zoom, pan
- [IIIF Image API 3.0](https://iiif.io/api/image/3.0/) -- info.json specification
- [IIIF Physical Dimensions Service](https://iiif.io/api/annex/services/) -- physicalScale, DPI calculation
- [Pillow Image module](https://pillow.readthedocs.io/en/stable/reference/Image.html) -- rotate, composite, paste, alpha
- [OpenCV background removal](https://opencv.org/blog/remove-backgrounds-from-images-using-opencv/) -- evaluated and rejected for this use case
