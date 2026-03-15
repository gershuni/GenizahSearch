# Domain Pitfalls: Fragment Puzzle / Visual Join Assembly

**Domain:** Visual fragment assembly tool for Cairo Genizah manuscript research platform
**Researched:** 2026-03-15
**Platform:** Dual-app (NiceGUI web + PyQt6 desktop)

## Critical Pitfalls

Mistakes that cause rewrites or major issues.

---

### Pitfall 1: NiceGUI Has No Canvas/Image Manipulation Framework

**What goes wrong:** Developers assume NiceGUI's `ui.interactive_image` can handle multi-image drag/rotate/flip assembly. It cannot. `ui.interactive_image` only supports SVG overlays on a single image -- it has no concept of multiple movable image items, rotation transforms, or layered composition. Building a puzzle canvas purely in NiceGUI's Python API leads to a dead end requiring a complete rewrite.

**Why it happens:** The component name "interactive_image" suggests interactivity. In reality it is an SVG-overlay viewer, not a canvas compositor. NiceGUI's GitHub discussions (#1339, #3427, #2513) show users repeatedly requesting drag/canvas features that do not exist natively.

**Consequences:** Weeks spent building a Python-side canvas abstraction that hits WebSocket round-trip latency on every mouse move. Unresponsive UX. Eventually must rewrite with a JavaScript canvas library anyway.

**Prevention:**
- For the web app: embed a JavaScript canvas library (Fabric.js or Konva.js) inside NiceGUI via `ui.html()` + `ui.run_javascript()`. All drag/rotate/flip logic runs client-side in JS. Python handles data persistence and image URL provision only.
- Communicate between Python and JS via `ui.run_javascript()` (Python-to-JS) and event handlers or exposed endpoints (JS-to-Python).
- Accept that the web puzzle canvas will be ~90% JavaScript, ~10% NiceGUI scaffolding.

**Detection:** If you find yourself writing Python code for mouse-move handlers that update image positions via WebSocket, stop. That path does not work for real-time manipulation.

**Confidence:** HIGH (verified via NiceGUI official docs and GitHub discussions)

---

### Pitfall 2: Divergent Canvas Architectures Between Web and Desktop

**What goes wrong:** The web canvas (JavaScript Fabric.js/Konva.js) and the desktop canvas (PyQt6 QGraphicsScene) have fundamentally different APIs, coordinate systems, and capabilities. Developers try to create a shared "canvas abstraction layer" and end up with a leaky abstraction that satisfies neither platform. Or worse, they build the desktop version first (QGraphicsScene is easier), then discover the web port requires a complete reimplementation.

**Why it happens:** The project's historical pattern is shared service layers (document_service.py, fjms_service.py, etc.) that work identically across both apps. But canvas manipulation is inherently platform-specific -- you cannot abstract over QGraphicsScene vs HTML5 Canvas in any meaningful way.

**Consequences:** Either (a) a brittle abstraction layer that breaks on edge cases, or (b) two completely separate implementations that drift apart in behavior.

**Prevention:**
- Accept two separate canvas implementations from the start. Share only the data model, not the rendering.
- Define a shared `PuzzleDocument` data model (fragment positions, rotations, scales, join metadata) in a shared module (e.g., `shared/puzzle_service.py`).
- Each platform renders from this model independently. The model is the contract, not the canvas.
- Build desktop first (QGraphicsScene is proven; existing `ZoomableScrollArea` code at line 1391 of genizah_app.py provides a starting point), then port the data model to the web JS canvas.
- Test roundtrip: Desktop saves PuzzleDocument -> Web loads it -> renders identically (and vice versa).
- Critical detail: set rotation center consistently -- Fabric.js defaults to top-left origin, Qt defaults to (0,0). Both must use center-origin:
  - Fabric.js: `img.set({ originX: 'center', originY: 'center' })`
  - Qt: `item.setTransformOriginPoint(width/2, height/2)`

**Detection:** If you are writing an `AbstractCanvas` class with `add_fragment()`, `rotate()`, `move()` methods that dispatch to platform-specific implementations, you are on the wrong path.

**Confidence:** HIGH (architectural analysis of existing codebase + NiceGUI limitations)

---

### Pitfall 3: IIIF Images Are Enormous and Kill Performance

**What goes wrong:** The existing codebase fetches IIIF images at `/full/2000,/0/default.jpg` (2000px wide). For a puzzle canvas with 3-8 fragments visible simultaneously, that is 3-8 images at 2000px+ each, all needing real-time transform operations. Memory explodes, rendering stutters, GPU compositing chokes.

**Why it happens:** Current image viewer shows one image at a time. The puzzle canvas shows many simultaneously. Linear memory scaling is the obvious consequence but easy to overlook during single-image prototyping.

**Consequences:** Desktop: QGraphicsScene with 8x 2000px QPixmaps = ~120MB of uncompressed pixel data per puzzle. Web: browser tab memory bloat, compositing lag on rotate/scale. 5-10 fragments at 2000x3000px RGBA = ~24MB each = 120-240MB in browser memory.

**Prevention:**
- Use a multi-resolution strategy: load reduced-res images (`/full/800,/0/default.jpg` or `/full/1000,/0/default.jpg`) for the interactive canvas, then load high-res (`/full/2000,/0/default.jpg`) only when exporting the final composite.
- On desktop (QGraphicsScene): use `QGraphicsPixmapItem` with LOD (Level of Detail) -- override `paint()` to swap between low-res and high-res based on zoom level.
- On web: use the JS canvas library's built-in image caching and implement progressive loading.
- Limit canvas to a maximum number of fragments (8-10 is typical for Genizah joins; enforce this).
- For the final composite export, fetch full-resolution images one at a time on the server side (Python Pillow), composite in memory, save, then release. Never send full-res to the browser.

**Detection:** If canvas becomes sluggish with 3+ fragments, or memory usage exceeds 500MB per puzzle, the resolution strategy is wrong.

**Confidence:** HIGH (existing IIIF URL patterns verified in codebase at genizah_app.py:1708,1858,1930 and web/api.py:145,208,285)

---

### Pitfall 4: Background Removal Fails on Parchment/Paper Edges

**What goes wrong:** HSV color segmentation works well for removing solid blue/green library backgrounds BUT fails at the fragment edges where the parchment color bleeds into the background color (shadow zones, translucent edges, frayed fibers). The result is either: (a) jagged edges with background color artifacts, or (b) over-aggressive removal that eats into the manuscript text near edges.

**Why it happens:** Manuscript fragments have irregular, fuzzy edges. Library backgrounds (NLI blue, Cambridge green/white, grid paper at other institutions) create gradients at the fragment boundary. Simple HSV thresholding produces binary masks with no edge feathering. The parchment itself varies in color from cream to dark brown, sometimes approaching the background hue in degraded areas.

**Consequences:** Researchers lose trust in the tool if background removal visibly damages the manuscript content. They will not use a tool that clips text at fragment edges. Scholarly credibility destroyed.

**Prevention:**
- Make background removal OPTIONAL and OFF by default -- show original image first.
- Use HSV segmentation as a first pass (more robust to lighting than RGB), then apply morphological operations (erode slightly to pull mask inward, then dilate back with Gaussian blur for soft edges).
- Add alpha feathering at mask boundaries (5-10px gradient from opaque to transparent) instead of hard cutoff.
- Provide a manual adjustment slider for threshold sensitivity per fragment (real-time preview).
- Provide a "show original" / "undo removal" toggle so researchers can verify nothing was lost.
- Consider GrabCut (OpenCV) as a refinement step after initial HSV segmentation -- it handles edge gradients much better than pure thresholding.
- Consider edge preservation: detect the manuscript boundary first (largest contour via findContours), then only remove outside it. This prevents interior parchment from being affected.
- Do NOT use deep learning models (rembg, U-Net) for this task -- they are trained on natural photos, not manuscripts, and will hallucinate edges on parchment.

**Detection:** Test with NLI blue backgrounds (the most common source) AND Cambridge images (different background colors) AND light-colored parchment on cream/white backgrounds. If edges look pixelated or text near edges is clipped, the approach needs refinement.

**Confidence:** MEDIUM (HSV segmentation well-documented for solid backgrounds; manuscript-specific edge behavior needs empirical testing with actual Genizah images)

---

### Pitfall 5: IIIF Sources Lack Physical Scale Metadata

**What goes wrong:** The DPI calibration feature assumes IIIF `info.json` includes a `physicalScale` service so fragments can be auto-sized to real-world proportions. In practice, most IIIF servers (including NLI, Cambridge CUDL) do NOT include the physical dimensions service. The `info.json` contains pixel dimensions only, with no scale information. Without this, DPI calibration silently falls back to pixel-based sizing, making fragments from different libraries display at wildly different relative scales.

**Why it happens:** The IIIF Physical Dimensions service (`http://iiif.io/api/annex/services/physdim`) is optional. It requires `physicalScale` (float ratio) and `physicalUnits` (mm/cm/in). Example: physicalScale=0.0025, physicalUnits="in" with a 4000px image means 400 DPI. Adoption is extremely low. Most digitization workflows store DPI in TIFF EXIF headers but strip it when serving via IIIF.

**Consequences:** A CUL fragment scanned at 400 DPI appears twice the size of a JTS fragment scanned at 200 DPI, even though they might be physically similar. The puzzle becomes useless for scale-sensitive assembly.

**Prevention:**
- Probe actual IIIF `info.json` endpoints for NLI, Cambridge, Manchester, JTS to determine what metadata is actually available. Do this in the research/prototyping phase, not during implementation.
- Build a fallback DPI lookup table per library/source: many institutions scan at consistent DPI within their collections (NLI typically 400 DPI, Cambridge CUDL typically 400 DPI).
- Normalize all fragments to a common pixels-per-cm before display.
- If no DPI data is available from any source, default all fragments to the same assumed DPI (e.g., 400) and let the user manually resize.
- Always provide manual scale adjustment (drag corner to resize), because auto-sizing will never be perfect.
- Display a visual indicator ("assumed scale" vs "calibrated scale") so researchers know when they are working with estimates.

**Detection:** If two fragments from the same physical manuscript but different digitization sources display at dramatically different sizes, the DPI fallback is not working.

**Confidence:** MEDIUM (IIIF physicalScale spec verified at iiif.io/api/annex/services; NLI/Cambridge actual support needs runtime probing)

---

### Pitfall 6: CORS Blocking IIIF Images on Web Canvas

**What goes wrong:** Fabric.js/Konva.js cannot manipulate pixels of images loaded from cross-origin URLs. The HTML5 canvas becomes "tainted" and `toDataURL()` / `toJSON()` throws security errors. Background removal requires pixel access which is blocked by CORS. Even if images display in an `<img>` tag, canvas pixel access requires explicit CORS headers.

**Why it happens:** IIIF servers (NLI, Cambridge, Oxford, Manchester) may not set `Access-Control-Allow-Origin` headers, or may restrict them. The puzzle tool needs pixel-level access for both background removal and composite export.

**Consequences:** Cannot apply background removal in-browser. Cannot export canvas to PNG/JPEG. Cannot serialize/deserialize fragment images. Tool is non-functional on web.

**Prevention:**
- Do NOT load IIIF images directly onto the JS canvas. Instead:
  1. Proxy images through the NiceGUI server (existing `web/api.py` already has proxy endpoints at lines 145, 208, 285, 345, 402)
  2. Apply background removal server-side (Python OpenCV) and send processed images as base64 data URLs
  3. Base64 data URLs are same-origin by definition -- no CORS issues
  4. Alternative: serve processed images via `/api/puzzle_image/{fl_id}` HTTP endpoint (avoids WebSocket payload issues too)
- On desktop: no CORS issue (Qt's `QNetworkAccessManager` is not subject to browser same-origin policy).

**Detection:** Test early with NLI and Cambridge IIIF URLs in Fabric.js. If `canvas.toDataURL()` throws, CORS is the problem.

**Confidence:** HIGH (standard web security constraint; existing proxy pattern verified in web/api.py)

---

## Moderate Pitfalls

Issues that cause incorrect behavior, poor performance, or significant rework.

---

### Pitfall 7: WebSocket Overhead for Real-Time Canvas State + Large Payloads

**What goes wrong:** Two related issues: (a) If canvas state (fragment positions) is managed in Python and pushed to the browser on every change, the WebSocket becomes a bottleneck during drag operations (60+ updates/second). (b) Sending large base64-encoded images (2000px RGBA = ~10MB base64) through NiceGUI's WebSocket causes timeouts, disconnections, or memory issues. The 200-result cap lesson from search (WebSocket safety) applies here.

**Prevention:**
- Canvas state during manipulation must live entirely in JavaScript (client-side). Only sync to Python on "drop" (mouseup) or explicit "save" events.
- Use debounced sync: batch position updates and send to Python at most once per 500ms.
- Serve processed images via HTTP endpoints (`/api/`), not WebSocket messages.
- Set processed images as server-served files and pass URLs to Fabric.js.
- Python is an event consumer, not the state authority during active editing.

**Detection:** Load 3+ fragments. If connection drops, UI freezes, or drag is laggy, the communication pattern is wrong.

**Confidence:** HIGH (known NiceGUI WebSocket limitation; existing lesson about parent_slot timer crash documented in project memory)

---

### Pitfall 8: Recto/Verso Auto-Generation Assumes Mirror Symmetry

**What goes wrong:** The recto/verso feature auto-generates a verso arrangement by mirroring the recto layout. But manuscript fragments are not perfectly flat -- they curl, have variable thickness, and were photographed from different angles for recto vs verso. Simple horizontal mirroring produces a verso where fragments do not align with each other, especially at join edges.

**Prevention:**
- Auto-generate verso as an initial approximation only (horizontal flip of positions + individual fragment horizontal flip). Clearly label it "auto-generated."
- Make verso independently editable -- researchers must be able to adjust each fragment's position on verso separately from recto.
- Store recto and verso arrangements as separate position arrays in the PuzzleDocument model, not as a derived transform of recto.
- Load actual verso images (different IIIF folio/canvas index) rather than flipping recto images programmatically.
- Show a toggle between recto/verso views, not a simultaneous split view (reduces confusion and memory usage).

**Confidence:** MEDIUM (informed by manuscript handling knowledge; specific fragment curl behavior is domain expertise)

---

### Pitfall 9: Join Document Schema Designed Too Narrowly

**What goes wrong:** The initial schema captures only "fragment A joins fragment B" with positions. But Genizah joins are more complex: fragments can overlap (palimpsests), join types vary (adjacent, overlapping, same-leaf), attribution matters (who proposed the join), and confidence levels exist. A narrow schema requires migration when these needs inevitably emerge.

**Prevention:**
- Design the PuzzleDocument schema to include from the start:
  - `fragments[]`: array of `{sys_id, fl_id, x, y, rotation, scale, flip_h, flip_v, z_order, opacity}`
  - `recto_arrangement` + `verso_arrangement`: separate position arrays
  - `join_type`: enum (adjacent, overlapping, uncertain)
  - `notes`: free text (Hebrew + English)
  - `confidence`: enum (certain, probable, possible)
  - `attribution`: who created this join document
  - `status`: draft / published / reviewed
  - `composite_image_url`: optional saved composite
  - `created_at`, `updated_at`
- Store in Supabase (community features pattern) with personal workspace default + publish workflow.
- Match existing FJMS join data fields where possible (48K joins in fjms_enrichment.db provide a reference model for required attributes).

**Confidence:** HIGH (existing join data structure in fjms_enrichment.db is well-understood)

---

### Pitfall 10: NiceGUI Page Navigation Destroys Canvas State

**What goes wrong:** User navigates away from puzzle page and back. Fabric.js canvas and all fragment state is destroyed because NiceGUI recreates the page DOM on navigation.

**Prevention:**
- Auto-save canvas state to database (Supabase or local SQLite) on every significant change (fragment added, moved, rotated).
- On page load, check for unsaved state and offer to restore.
- Use `app.on_disconnect` handler to save state before connection closes.
- Follow the session persistence pattern from v6.5.0 which successfully preserves search state, browse tabs, and composition summary across navigation.

**Confidence:** HIGH (existing session persistence pattern verified in v6.5.0)

---

### Pitfall 11: Background Removal Runs Server-Side, Blocks Event Loop

**What goes wrong:** OpenCV background removal (HSV conversion, morphological operations, GrabCut) is CPU-intensive. Running it in the NiceGUI async event loop blocks all other users. Running it in the desktop main thread freezes the UI.

**Prevention:**
- On web: run background removal via `run.cpu_bound()` to offload to a worker process (preferred for OpenCV since it is CPU-bound, not I/O-bound). `run.io_bound()` is acceptable but does not release the GIL as effectively.
- On desktop: run in a QThread (following existing `gui_threads.py` pattern with `SearchThread`).
- Show a progress indicator: "Removing background..." with cancel option.
- Consider pre-computing background removal when images are first loaded to the canvas (background task) rather than on-demand when the user toggles it.

**Confidence:** HIGH (matches existing patterns for CPU-intensive work in both apps)

---

### Pitfall 12: Image Caching Strategy Collision with Existing Viewer

**What goes wrong:** The existing image viewer has its own caching (`v2 cache: high resolution (2000px)` per genizah_app.py line 2155). The puzzle tool fetches the same images at different resolutions (800px for canvas, 2000px for export). Two caching systems for the same images create confusion, memory bloat, and cache invalidation bugs.

**Prevention:**
- Use the existing image cache infrastructure. Extend it to support multiple resolution tiers per image rather than building a separate cache.
- Key cache entries by `(fl_id, resolution)` tuple, not just fl_id.
- On desktop: share the `QNetworkAccessManager` and disk cache already in use.
- On web: browser HTTP cache handles IIIF URLs naturally (different URLs for different resolutions produce distinct cache entries automatically -- `/full/800,/` vs `/full/2000,/`).

**Confidence:** HIGH (existing cache code verified in genizah_app.py)

---

## Minor Pitfalls

Issues that cause minor bugs, UX friction, or developer confusion.

---

### Pitfall 13: Undo/Redo Missing from Initial Implementation

**What goes wrong:** Fragment assembly involves lots of trial and error. Without undo/redo, researchers are afraid to experiment. Retrofitting undo/redo after the canvas is built is far harder than building it in from the start because it requires wrapping every mutation in a command object.

**Prevention:**
- Implement undo/redo from the first version.
- Use a command pattern: each operation (move, rotate, flip, add, remove) is a reversible command.
- On desktop (QGraphicsScene): Qt has `QUndoStack` built in -- use it.
- On web (JS canvas): maintain a state history array (snapshot of all fragment positions per action). Both Fabric.js and Konva.js have undo examples in their ecosystems.

**Confidence:** HIGH (standard UX requirement; Qt's QUndoStack verified)

---

### Pitfall 14: Composite Image Export Loses Fragment Metadata

**What goes wrong:** The exported composite PNG/JPEG is just pixels -- it contains no information about which fragments are included, their positions, or the join metadata. Researchers share the image but recipients cannot reconstruct or verify the join.

**Prevention:**
- Always save both the composite image AND the PuzzleDocument JSON (structured metadata).
- Embed fragment IDs in PNG metadata (tEXt chunks) or use a sidecar JSON file alongside the image.
- When sharing/publishing, always link composite image to PuzzleDocument record in Supabase.
- Consider SVG export as an alternative -- it preserves individual fragment boundaries as separate elements and is zoomable.

**Confidence:** HIGH (data integrity requirement)

---

### Pitfall 15: Hebrew RTL Text and Right-to-Left Fragment Ordering

**What goes wrong:** Two related issues: (a) Fragment labels, join notes, and metadata fields contain Hebrew text. UI components default to LTR layout, causing garbled display. (b) Hebrew manuscripts read right-to-left. Default left-to-right fragment placement on the canvas confuses researchers -- the "first" fragment should be on the right.

**Prevention:**
- Apply the same RTL patterns used throughout the existing codebase (dir="rtl" on relevant elements, Hebrew font stacks).
- Notes/annotation text area must support bidirectional text (researchers write mixed Hebrew/English).
- Default canvas layout should place fragments right-to-left (first fragment on the right).
- Fragment labels derived from shelfmarks (e.g., "T-S 12.123") are LTR but may be embedded in Hebrew context.

**Confidence:** HIGH (existing pattern in codebase; known constraint)

---

### Pitfall 16: Fragment Z-Order Conflicts at Overlap Points

**What goes wrong:** When fragments overlap (common in Genizah joins where edges are placed adjacent with slight overlap), clicking in the overlap zone always selects the top fragment. Researchers cannot easily select or manipulate the bottom fragment without moving the top one first.

**Prevention:**
- Implement a fragment list panel (like Photoshop layers) showing z-order. Click to select any fragment regardless of overlap.
- Tab/cycle key to iterate through overlapping fragments at click point.
- Right-click context menu: "Select fragment underneath".

**Confidence:** MEDIUM (standard graphics editor UX)

---

### Pitfall 17: OpenCV Dependency Size for Desktop Distribution

**What goes wrong:** Adding `opencv-python` to the desktop build adds ~50-80MB to the installer package. The full OpenCV package includes GUI modules that conflict with PyQt6.

**Prevention:**
- Use `opencv-python-headless` instead of `opencv-python` (smaller, no GUI dependencies).
- Alternatively, for simple HSV segmentation only, Pillow + NumPy may suffice without OpenCV (convert to HSV via NumPy, threshold, apply mask via Pillow). Add OpenCV only if GrabCut or advanced morphological operations are needed.
- Profile the installer size delta before committing to the dependency.

**Confidence:** MEDIUM (opencv-python-headless is a known lighter alternative; Pillow-only approach needs validation)

---

### Pitfall 18: IIIF Image Orientation Metadata

**What goes wrong:** Some IIIF images are stored rotated (metadata says "rotate 90") but served un-rotated. Or the opposite -- served rotated but the tool applies rotation again, causing double rotation.

**Prevention:** Check IIIF info.json for rotation metadata. Most NLI images are 0 rotation, but verify. Ignore EXIF orientation (IIIF servers normalize this). If the image service's `profile` includes rotation support, the `/0/` in the URL path already handles it.

**Confidence:** LOW (needs runtime verification with actual IIIF endpoints)

---

### Pitfall 19: Fabric.js State Serialization Losing Image Data

**What goes wrong:** `canvas.toJSON()` serializes object properties but not the actual image pixel data. On reload, if the original image URL is no longer available (or was a one-time data URL from background removal), the fragment image is lost.

**Prevention:** Store fragment metadata (sys_id, fl_id, IIIF URL, position, rotation, scale, background_removal_params) separately from canvas state. On reload, re-fetch images from IIIF and re-apply background removal using saved parameters. Never depend on Fabric.js serialization for persistence -- the PuzzleDocument model is the source of truth.

**Confidence:** HIGH (Fabric.js serialization behavior is documented)

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Data model / schema | Pitfall 9: Too narrow join schema | Design with FJMS join fields as reference; include recto/verso positions from start |
| Background removal | Pitfall 4: Edge quality on parchment | Prototype with NLI blue + Cambridge images early; add alpha feathering; make optional |
| Background removal | Pitfall 11: CPU blocks event loop | Use `run.cpu_bound()` on web, QThread on desktop |
| DPI calibration | Pitfall 5: No physicalScale in IIIF info.json | Probe actual endpoints first; build per-library DPI fallback table |
| Desktop canvas | Memory risk (Pitfall 3) | Load at 800-1000px for interaction; full-res only for export |
| Web canvas | Pitfall 1 + 6 + 7: Must use JS library; CORS; WebSocket limits | Fabric.js/Konva.js in NiceGUI; proxy images; JS-authoritative state |
| Cross-platform parity | Pitfall 2: Two canvas implementations will diverge | Shared PuzzleDocument model is the contract; test roundtrip early |
| Recto/verso | Pitfall 8: Mirror symmetry is only approximate | Independent verso editing; load actual verso images |
| Image loading | Pitfall 3 + 12: Memory and cache conflicts | Multi-resolution strategy; extend existing cache by resolution tier |
| Composite export (web) | Pitfall 6: CORS taints canvas | Proxy through NiceGUI server or composite server-side via Pillow |
| State persistence | Pitfall 10: Navigation destroys canvas | Auto-save on changes; follow v6.5.0 session persistence pattern |
| UX polish | Pitfall 13: No undo = researchers afraid to experiment | Implement undo/redo from v1; QUndoStack on desktop, state history on web |
| Desktop packaging | Pitfall 17: OpenCV bloats installer | Use opencv-python-headless or evaluate Pillow-only approach |

---

## Sources

- [NiceGUI ui.interactive_image documentation](https://nicegui.io/documentation/interactive_image) -- verified capabilities and limitations (HIGH confidence)
- [NiceGUI canvas API discussion #2513](https://github.com/zauberzeug/nicegui/discussions/2513) -- community requests for canvas features
- [NiceGUI draggable image discussion #3427](https://github.com/zauberzeug/nicegui/discussions/3427) -- drag limitations
- [NiceGUI drag image discussion #1339](https://github.com/zauberzeug/nicegui/discussions/1339) -- no built-in drag support
- [Qt QGraphicsScene documentation](https://doc.qt.io/qt-6/qgraphicsscene.html) -- BSP tree indexing, item flags, performance
- [IIIF Physical Dimensions Service](https://iiif.io/api/annex/services/) -- physicalScale spec (physicalScale float + physicalUnits mm/cm/in)
- [IIIF Image API 2.1](https://iiif.io/api/image/2.1/) -- info.json structure, pixel dimensions
- [IIIF-discuss: DPI in image information](https://groups.google.com/g/iiif-discuss/c/uut9voeev_E) -- community discussion confirming low adoption
- [OpenCV HSV Color Segmentation](https://realpython.com/python-opencv-color-spaces/) -- technique for solid-background removal
- [Fragmentarium project (University of Fribourg)](https://www.unifr.ch/env/en/info/news/17667/next) -- existing DH manuscript fragment assembly platform
- Existing codebase: genizah_app.py `ZoomableScrollArea` (line 1391), IIIF URLs (lines 1708, 1858, 1930), image cache (line 2155), web/api.py proxy (lines 145, 208, 285, 345, 402)
