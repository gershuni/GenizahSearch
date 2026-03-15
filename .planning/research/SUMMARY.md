# Project Research Summary

**Project:** v7.0.0 Fragment Puzzle
**Domain:** Visual fragment assembly tool for Cairo Genizah manuscript research
**Researched:** 2026-03-15
**Confidence:** HIGH

## Executive Summary

The Fragment Puzzle is a canvas-based visual join assembly tool that lets researchers drag, rotate, flip, and overlay manuscript fragment images to test and document physical joins. This is fundamentally a dual-canvas problem: a JavaScript canvas (Fabric.js) embedded in NiceGUI for the web app, and a QGraphicsScene for the PyQt6 desktop app. The two canvas implementations share nothing at the rendering level -- the shared contract is a `PuzzleDocument` data model in `shared/puzzle_service.py` that serializes fragment positions, transforms, and join metadata. This "shared model, independent rendering" architecture is the only viable approach; attempts to abstract over both canvas APIs will fail.

The recommended build order is desktop-first. QGraphicsScene already provides multi-item drag, rotation, selection, and z-ordering out of the box, and the existing `ZoomableScrollArea` (genizah_app.py:1391) is a working starting point. The web canvas requires embedding Fabric.js via NiceGUI's JS bridge (`ui.run_javascript()`), which is more architecturally novel but follows proven patterns from the existing advViewer. The single biggest differentiator over the FJMS Jigsaw tool is automatic background removal -- isolating parchment from solid-color library scanning backgrounds using HSV color segmentation with Pillow and NumPy. This requires no new heavy dependencies (OpenCV is explicitly rejected in favor of Pillow+NumPy for simplicity and smaller installer size).

Key risks are: (1) IIIF images are large and showing 3-8 fragments simultaneously requires a multi-resolution loading strategy (800px for interaction, 2000px for export only); (2) background removal quality at manuscript edges needs empirical tuning with real Genizah images from multiple libraries; (3) IIIF servers almost certainly lack physical scale metadata, so DPI calibration must rely on per-library defaults with manual override; (4) CORS blocks pixel access to cross-origin images on the web canvas, requiring all image processing to go through the existing NiceGUI server proxy. All four risks have clear mitigation strategies documented in the research.

## Key Findings

### Recommended Stack

No new pip dependencies are required for core functionality. Pillow and NumPy are already indirect dependencies. The web canvas uses Fabric.js 6.x loaded from CDN (no npm build step). The desktop canvas uses PyQt6's built-in QGraphicsScene. Background removal uses Pillow + NumPy for HSV color segmentation. Persistence uses a new `joins.db` SQLite sidecar (local-first) with optional Supabase publish for community features.

**Core technologies:**
- **Fabric.js 6.x (CDN):** Web canvas for multi-image drag/rotate/flip/scale -- standard library for browser-based object manipulation, mature, well-documented
- **QGraphicsScene (PyQt6, bundled):** Desktop canvas -- already partially implemented via ZoomableScrollArea, native multi-item transforms
- **Pillow + NumPy:** Background removal via HSV color thresholding, composite image export -- no new dependencies, sufficient for solid-color library backgrounds
- **SQLite sidecar (joins.db):** Local persistence for join documents -- follows established pgp.db/fjms_enrichment.db pattern
- **IIIF info.json:** DPI calibration via pixel dimensions + per-library fallback table -- physicalScale service unlikely to be available

**What NOT to add:** OpenCV (50MB+ for no benefit over Pillow+NumPy for solid backgrounds), rembg/deep learning (175MB model, clips manuscript edges), npm build pipeline, WebSocket image streaming, 3D rendering.

### Expected Features

**Must have (table stakes):**
- Canvas with drag, rotate, flip, zoom for 2+ fragment images
- Load fragments by shelfmark/sys_id using existing image pipeline
- Pre-populate from FJMS/PGP join groups (48K known joins)
- Opacity/transparency control for edge-matching verification
- Save/load arrangements with full state persistence
- Undo/redo (QUndoStack on desktop, state history on web)

**Should have (differentiators over FJMS Jigsaw):**
- Automatic background removal (HSV segmentation for solid library backgrounds) -- the single biggest differentiator
- DPI-calibrated auto-sizing from IIIF metadata with manual fallback
- Recto/verso dual canvas with independent editing
- Composite image export (PNG) for publication/sharing
- Join document metadata (fragment IDs, type, notes, confidence)
- Personal workspace with multiple saved arrangements
- Per-fragment independent scaling

**Defer (v2+):**
- Community publish/review workflow (use existing corrections infrastructure as interim)
- Snap-to-edge alignment guides
- AI/ML-based join detection (explicitly out of scope per PROJECT.md)
- Image enhancement (contrast, levels) -- researchers have Photoshop/GIMP
- Real-time multi-user collaboration

### Architecture Approach

The architecture is "shared data model, independent canvas rendering." All data logic lives in `shared/puzzle_service.py` and `shared/background_removal.py`. Canvas implementations are entirely separate: Fabric.js (web) communicates with Python via `ui.run_javascript()` bridge; QGraphicsScene (desktop) operates natively. State of truth during active editing lives in the canvas (JS or Qt); Python requests state snapshots only on save/export. Images are proxied through existing infrastructure (web: `/api/nli_image` proxy; desktop: `ImageLoaderThread` with disk cache). Background removal runs server-side for both platforms.

**Major components:**
1. **shared/puzzle_service.py** -- PuzzleDocument/PuzzleFragment data model, IIIF info fetch, DPI calibration, composite export, serialization
2. **shared/background_removal.py** -- HSV color segmentation, alpha mask generation, edge feathering (Pillow + NumPy)
3. **web/components/puzzle_canvas.py + .js** -- Fabric.js canvas as NiceGUI custom component, toolbar, JS-Python bridge
4. **Desktop PuzzleWidget** -- QGraphicsScene with PuzzleFragmentItem subclass, rotation/resize handles, keyboard shortcuts
5. **joins.db** -- Local SQLite sidecar for join documents (drafts); Supabase for published joins
6. **web/pages/puzzle.py** -- Puzzle workspace page with fragment selection, canvas, metadata panel

### Critical Pitfalls

1. **NiceGUI has no canvas framework** -- `ui.interactive_image` cannot handle multi-image manipulation. Must embed Fabric.js via JS bridge. If you find yourself writing Python mouse-move handlers for canvas objects, stop immediately.
2. **IIIF images are enormous** -- 3-8 fragments at 2000px each = 120-240MB in browser memory. Load at 800px for interaction, full-res only for server-side composite export.
3. **Background removal fails at manuscript edges** -- HSV thresholding produces jagged edges on parchment. Must add alpha feathering (5-10px gradient), make removal optional and off by default, provide manual threshold slider.
4. **CORS blocks pixel access on web canvas** -- Must proxy all IIIF images through NiceGUI server and serve processed images as data URLs or via HTTP endpoint. Never load cross-origin images directly onto Fabric.js canvas.
5. **Two canvas implementations will diverge** -- Do not attempt a shared canvas abstraction. Share only the PuzzleDocument data model. Test cross-platform roundtrip (desktop save -> web load) early and continuously.
6. **IIIF servers lack physical scale metadata** -- Build per-library default DPI table from day one. Always provide manual scale override. Display "assumed scale" indicator.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Data Model + Shared Services
**Rationale:** Everything depends on the PuzzleDocument data model and the shared service layer. Defining this first prevents schema migration pain later and ensures both platforms build against the same contract.
**Delivers:** `shared/puzzle_service.py` with PuzzleDocument/PuzzleFragment dataclasses, joins.db schema, IIIF info.json fetch, DPI calibration with per-library fallback table, serialization/deserialization.
**Addresses:** Join document metadata (FEATURES), schema breadth (Pitfall 9), DPI fallback chain (Pitfall 5)
**Avoids:** Pitfall 9 (narrow schema) by designing with FJMS join fields as reference from the start

### Phase 2: Desktop Canvas (QGraphicsScene)
**Rationale:** Desktop is the natural prototype -- QGraphicsScene provides drag/rotate/flip/zoom/selection natively. Existing ZoomableScrollArea is the starting point. Building desktop first validates the data model before tackling the harder web canvas.
**Delivers:** PuzzleWidget with PuzzleFragmentItem, drag/rotate/flip/zoom, fragment loading by shelfmark/sys_id, pre-populate from FJMS join groups, opacity control, undo/redo (QUndoStack), save/load via joins.db.
**Addresses:** All table-stakes features (FEATURES Phase 1), desktop proof-of-concept
**Avoids:** Pitfall 3 (memory) via multi-resolution loading; Pitfall 13 (no undo) by using QUndoStack from day one

### Phase 3: Background Removal
**Rationale:** The biggest differentiator. Independent of canvas platform -- runs as a shared Python module. Needs empirical testing with real Genizah images from NLI, Cambridge, Manchester, Oxford before being wired into either canvas.
**Delivers:** `shared/background_removal.py` with HSV segmentation, alpha feathering, per-library color profiles, manual threshold adjustment. Test suite against sample images from multiple libraries.
**Addresses:** Automatic background removal (FEATURES top differentiator)
**Avoids:** Pitfall 4 (edge quality) by prototyping against real images early; Pitfall 17 (OpenCV bloat) by using Pillow+NumPy; Pitfall 11 (CPU blocking) by running in worker thread/process

### Phase 4: Web Canvas (Fabric.js + NiceGUI)
**Rationale:** Architecturally the hardest part -- JS canvas embedded in NiceGUI via custom component. Depends on validated data model (Phase 1) and proven interaction patterns from desktop (Phase 2). Background removal (Phase 3) feeds processed images to the canvas.
**Delivers:** `web/components/puzzle_canvas.py` + `.js`, `web/pages/puzzle.py`, Fabric.js canvas with all table-stakes features, image proxy for CORS, JS-authoritative state with Python persistence.
**Addresses:** Web parity with desktop canvas
**Avoids:** Pitfall 1 (no NiceGUI canvas) by using Fabric.js; Pitfall 6 (CORS) by proxying through server; Pitfall 7 (WebSocket overhead) by keeping state in JS

### Phase 5: Recto/Verso + Composite Export
**Rationale:** Completes the scholarly workflow. Depends on working canvas (either platform) and background removal. Recto/verso requires NLI S1/S2 mapping and independent verso editing.
**Delivers:** Recto/verso toggle with auto-generated (but independently editable) verso layout, composite PNG export via Pillow, metadata embedding.
**Addresses:** Recto/verso dual canvas, composite export, join metadata (FEATURES Phase 3)
**Avoids:** Pitfall 8 (mirror symmetry) by making verso independently editable; Pitfall 14 (metadata loss) by exporting PuzzleDocument JSON alongside image

### Phase 6: Integration + Polish
**Rationale:** Wire puzzle into existing app navigation (browse "Open in Puzzle", search "Add to Puzzle"), session persistence, state auto-save, and UX polish (fragment list panel, z-order controls, RTL layout, grid/ruler overlay).
**Delivers:** Entry points from browse/search, session persistence following v6.5.0 pattern, fragment layer panel, Hebrew RTL support in labels/notes.
**Addresses:** Personal workspace, snap-to-edge guides, grid/ruler (FEATURES Phase 4 items)
**Avoids:** Pitfall 10 (navigation destroys state) by auto-saving; Pitfall 15 (RTL) by applying existing patterns; Pitfall 16 (z-order) by adding layer panel

### Phase Ordering Rationale

- Data model first because both canvases and persistence depend on it. Changing the schema after canvas implementation is painful (Pitfall 9).
- Desktop before web because QGraphicsScene is easier and validates the interaction model. The web canvas (Fabric.js in NiceGUI) is architecturally novel and benefits from a working reference implementation.
- Background removal as a standalone phase because it needs empirical tuning with real images and is independent of canvas choice. Testing it in isolation reduces risk.
- Web canvas after desktop and background removal because it depends on both (validated data model + processed images).
- Recto/verso and export after both canvases work because they are workflow completions, not infrastructure.
- Integration last because it wires an already-working feature into the existing app.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 3 (Background Removal):** Needs empirical testing with actual Genizah images from 4+ libraries. HSV thresholds, edge feathering parameters, and failure modes are unknown until tested with real data. Probe IIIF info.json endpoints for NLI, Cambridge, Manchester, JTS during this phase.
- **Phase 4 (Web Canvas):** Fabric.js + NiceGUI custom component pattern is architecturally novel for this codebase. May need `/gsd:research-phase` to prototype the JS-Python bridge before full planning.
- **Phase 5 (Recto/Verso):** NLI S1/S2 folio mapping needs verification. How to reliably pair recto and verso FL IDs across different libraries is a data question.

Phases with standard patterns (skip research-phase):
- **Phase 1 (Data Model):** Standard dataclass + SQLite sidecar pattern, well-established in codebase.
- **Phase 2 (Desktop Canvas):** QGraphicsScene is Qt's designed solution; ZoomableScrollArea is the proven starting point.
- **Phase 6 (Integration):** Follows existing browse/search entry point and session persistence patterns exactly.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | No new dependencies needed. Fabric.js and QGraphicsScene are standard solutions for their respective platforms. Pillow+NumPy sufficient for background removal. |
| Features | MEDIUM | FJMS Jigsaw is the only real prior art; no open-source fragment assembly tools found. Feature set derived from domain analysis rather than competitive landscape. |
| Architecture | HIGH | Follows proven codebase patterns (shared service + app-specific UI). Canvas separation is the only viable approach. |
| Pitfalls | HIGH | Critical pitfalls (NiceGUI canvas limitations, CORS, image memory) are verified against official docs and codebase analysis. Edge-case pitfalls (background removal quality, IIIF metadata availability) need runtime validation. |

**Overall confidence:** HIGH -- the stack and architecture recommendations are well-grounded. The main uncertainty is background removal quality on real manuscript images, which is addressable through empirical prototyping in Phase 3.

### Gaps to Address

- **IIIF Physical Dimensions availability:** No one has actually probed NLI/Cambridge/Manchester info.json endpoints for physicalScale data. Must do this in Phase 1 or early Phase 3. If no library provides it, the entire DPI calibration feature reduces to a per-library lookup table + manual override.
- **Background removal quality:** HSV thresholding on solid backgrounds is well-documented, but manuscript-specific edge behavior (translucent parchment, frayed fibers, shadow zones) needs testing with 10+ real images per library. Phase 3 should start with a test suite before writing production code.
- **Pillow+NumPy vs OpenCV for morphological operations:** STACK.md recommends Pillow+NumPy; ARCHITECTURE.md references OpenCV. The recommendation is to start with Pillow+NumPy and add `opencv-python-headless` only if morphological cleanup proves insufficient. This decision should be made during Phase 3 prototyping.
- **Fabric.js version:** v6.x is recommended over v7.x (more battle-tested), but v7 may be necessary if v6 CDN availability decreases. Pin to specific minor version in CDN URL.
- **NLI recto/verso FL ID pairing:** The S1/S2 convention is assumed but not verified across all NLI collections. Must validate during Phase 5 planning.

## Sources

### Primary (HIGH confidence)
- [Fabric.js official documentation](https://fabricjs.com/) -- canvas API, serialization, object manipulation
- [Qt QGraphicsScene documentation](https://doc.qt.io/qt-6/qgraphicsscene.html) -- scene management, item transforms, z-ordering
- [IIIF Image API 2.1/3.0 specification](https://iiif.io/api/image/3.0/) -- info.json structure, image request syntax
- [NiceGUI documentation](https://nicegui.io/documentation/) -- run_javascript, ui.html, custom components
- Existing codebase: ZoomableScrollArea (genizah_app.py:1391), image proxy (web/api.py), service layer pattern (shared/)
- [Pillow Image module documentation](https://pillow.readthedocs.io/en/stable/reference/Image.html) -- rotate, composite, alpha operations

### Secondary (MEDIUM confidence)
- [IIIF Physical Dimensions Service](https://iiif.io/api/annex/services/) -- physicalScale specification (spec is clear; adoption is low)
- [NiceGUI GitHub discussions #1339, #2513, #3427](https://github.com/zauberzeug/nicegui/discussions/) -- canvas/drag limitations confirmed
- [Friedberg Genizah Project Research Platform](https://pr.genizah.org/) -- FJMS Jigsaw feature set (primary competitor)
- [IIIF-discuss mailing list](https://groups.google.com/g/iiif-discuss/) -- physical dimensions adoption discussion

### Tertiary (LOW confidence)
- IIIF info.json actual content from NLI/Cambridge/Manchester -- needs runtime probing, not yet verified
- Background removal edge quality on Genizah manuscript images -- needs empirical testing
- Fabric.js v6 vs v7 stability comparison -- based on community consensus, not direct testing

---
*Research completed: 2026-03-15*
*Ready for roadmap: yes*
