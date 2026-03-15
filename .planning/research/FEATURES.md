# Feature Landscape: Fragment Puzzle / Visual Join Assembly Tool

**Domain:** Manuscript fragment visual assembly, digital humanities jigsaw tools
**Researched:** 2026-03-15
**Overall Confidence:** MEDIUM (FJMS puzzle is the primary prior art; no open-source implementations found)

## Table Stakes

Features researchers expect from a visual fragment assembly tool. Missing = tool feels incomplete or unusable for real join work.

| Feature | Why Expected | Complexity | Dependencies | Notes |
|---------|--------------|------------|--------------|-------|
| **Canvas with multiple fragment images** | Core premise -- place 2+ fragment images on a shared workspace | Medium | Image fetching (nli_crossref_service, IIIF) | FJMS Jigsaw shows N images simultaneously on screen |
| **Drag to reposition** | Most basic manipulation -- slide fragments around to test alignment | Low | Canvas infrastructure | QGraphicsScene (desktop) already supports ItemIsMovable |
| **Free rotation (arbitrary angle)** | Fragments rarely align at 90-degree increments; edges must match | Medium | Canvas transform system | FJMS supports rotation. Must rotate around fragment center, not canvas origin |
| **Horizontal flip (mirror)** | Test whether a fragment is upside-down or the image was scanned reversed | Low | Image transform | Simple horizontal mirror of the pixmap/image |
| **Zoom (canvas-level)** | Researchers need both overview and pixel-level detail for edge matching | Low | Existing zoom infrastructure | Desktop ZoomableScrollArea already does this; web needs equivalent |
| **Load fragments by shelfmark/sys_id** | Researchers know which fragments to test; input by identifier, not file browse | Low | Existing search + image resolution pipeline | Leverage existing image URL resolution from nli_crossref_service |
| **Fragment selection from join groups** | Pre-populate canvas from known FJMS/PGP join partners | Low | fjms_service.get_join_group(), PGP fragment data | Critical UX shortcut -- researcher opens a join group and gets all fragments loaded |
| **Opacity/transparency control** | Overlay semi-transparent fragments to check text continuity across join edges | Medium | Canvas compositing | Essential for checking whether text lines continue across a physical join |
| **Undo/redo** | Manipulation errors happen constantly; must be reversible | Medium | Command pattern implementation | Without undo, a single wrong rotation ruins the arrangement |
| **Save arrangement** | Researchers spend minutes to hours arranging; losing work is unacceptable | Medium | Persistence layer (SQLite or JSON) | Save fragment positions, rotations, flips, opacity per arrangement |

## Differentiators

Features that set this tool apart from the FJMS Jigsaw. Not expected, but highly valued by researchers.

| Feature | Value Proposition | Complexity | Dependencies | Notes |
|---------|-------------------|------------|--------------|-------|
| **Automatic background removal** | Isolate parchment/paper from library scanning backgrounds (blue, green, gray, black cards) | High | OpenCV or similar image processing | FJMS Jigsaw does NOT do this -- fragments shown with full rectangular backgrounds. Color-based segmentation works well for uniform library backgrounds (HSV thresholding). This is the single biggest differentiator. |
| **DPI-calibrated auto-sizing** | Fragments from different libraries scanned at different resolutions appear at correct relative scale | High | IIIF physicalDimensions service, image info.json | IIIF Physical Dimensions service provides scale factor + units. Not all institutions provide this data -- need fallback to manual sizing. NLI images likely have this; Cambridge CUDL may not. |
| **Recto/verso dual canvas** | When assembling recto, auto-generate mirrored verso arrangement; both independently editable | High | Mirror transform + linked state model | Verso is horizontal mirror of recto arrangement. Each fragment's verso image loaded separately. Key insight: recto arrangement determines verso positions (mirrored), but verso images are independent (different page). |
| **Composite image export** | Export the assembled join as a single image (PNG/JPEG) for publication or sharing | Medium | Canvas rendering to image | Researchers need shareable output, not just an interactive view |
| **Join document metadata** | Structured record: fragment IDs, join type (physical/textual), researcher notes, confidence level | Medium | Data model + persistence | Goes beyond FJMS which tracks joins but not the visual evidence |
| **Personal workspace** | Save multiple join arrangements privately before publishing | Low | User session / Supabase user data | FJMS Jigsaw is session-only (no persistence found) |
| **Publish for community review** | Share a proposed join with other researchers for validation | Medium | Supabase community features | Leverages existing corrections/community infrastructure |
| **Per-fragment independent scaling** | Resize individual fragments when DPI metadata is unavailable or incorrect | Medium | Per-item transform handles | Needed as DPI fallback -- manual resize with visual ruler/grid |
| **Snap-to-edge alignment guides** | Visual guides when fragment edges approach alignment | Medium | Geometry calculations | Speeds up the tedious alignment process |
| **Grid/ruler overlay** | Physical measurement reference (cm/inches) on canvas | Low | Canvas overlay drawing | Useful when DPI is known; helps researchers estimate physical dimensions |

## Anti-Features

Features to explicitly NOT build. Each would add complexity without proportional value, or belongs in a different tool.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| **Automatic join detection / AI matching** | PROJECT.md explicitly scopes this out ("Build join detection AI -- import from NLI/PGP instead"). Enormous ML complexity for uncertain results. | Import known joins from FJMS (48K) and PGP. Let researchers manually test hypotheses. |
| **Image enhancement (contrast, levels, filters)** | Image processing tool, not join assembly tool. Scope creep. Researchers have Photoshop/GIMP. | Provide "open in external editor" link. At most, a simple brightness/contrast slider. |
| **Transcription overlay on fragments** | Mixing transcription editing with visual assembly conflates two tools. | Link to existing transcription viewer. Show transcription in a side panel if needed, not overlaid on the canvas. |
| **3D fragment modeling** | Massively complex (Dead Sea Scrolls project level). No existing infrastructure. | Stay 2D. Physical parchment curvature is a research-grade 3D scanning problem. |
| **Multi-user real-time collaboration** | WebSocket-based real-time editing is architecturally complex and rarely needed. | One user at a time. Share via publish/review workflow. |
| **Custom annotation drawing tools** | Turns the tool into a general-purpose image editor. | Keep annotations as text notes attached to the join document, not freehand drawing on the canvas. |
| **Automatic fragment boundary detection** | Edge detection on damaged parchment is unreliable. Researchers know their fragments. | Background removal (uniform backgrounds) is feasible; irregular edge detection is not worth the effort. |
| **Print-quality PDF export** | Publishing-grade output is a typesetting problem. | Export PNG at canvas resolution. Researchers use InDesign/LaTeX for publication layouts. |

## Feature Dependencies

```
Image fetching (existing) --> Canvas infrastructure --> Drag/rotate/flip
                                    |
                                    +--> Background removal (independent per fragment)
                                    |
                                    +--> DPI calibration (independent per fragment, needs IIIF info.json)
                                    |
                                    +--> Opacity control
                                    |
Canvas infrastructure --> Undo/redo (wraps all canvas operations)
                                    |
Canvas infrastructure --> Save arrangement --> Personal workspace --> Publish for review
                                    |
Drag/rotate/flip --> Recto/verso dual canvas (mirrors recto arrangement)
                                    |
Save arrangement --> Composite image export (renders saved state)
                                    |
Join group loading (existing fjms_service) --> Fragment selection UI
PGP fragment data (existing document_service) --> Fragment selection UI
```

## MVP Recommendation

**Phase 1 - Core Canvas (table stakes):**
1. Canvas with drag, rotate, flip, zoom for 2+ fragment images
2. Load fragments by shelfmark/sys_id (leveraging existing image pipeline)
3. Fragment selection from FJMS/PGP join groups (pre-populate)
4. Opacity/transparency control
5. Save/load arrangements (local persistence)
6. Undo/redo

**Phase 2 - Background Removal + DPI (key differentiators):**
7. Automatic background removal (color-based segmentation for uniform backgrounds)
8. DPI-calibrated auto-sizing from IIIF metadata (with manual fallback)
9. Per-fragment independent scaling
10. Grid/ruler overlay

**Phase 3 - Recto/Verso + Export (complete workflow):**
11. Recto/verso dual canvas with auto-mirrored arrangement
12. Composite image export (PNG)
13. Join document metadata (fragment IDs, type, notes)

**Phase 4 - Community (social layer):**
14. Personal workspace (multiple saved arrangements)
15. Publish for community review
16. Snap-to-edge alignment guides

**Rationale for ordering:**
- Phase 1 delivers a usable tool immediately -- researchers can start testing join hypotheses visually
- Phase 2 adds the biggest differentiator (background removal) that no existing tool provides accessibly
- Phase 3 completes the scholarly workflow (both sides of the page, exportable output)
- Phase 4 adds community features that build on existing Supabase infrastructure

**Defer:**
- Snap-to-edge guides: Nice polish but not essential for core workflow
- Community publishing: Can use existing corrections/discovery submission as interim

## Platform Considerations

| Concern | Web (NiceGUI) | Desktop (PyQt6) |
|---------|---------------|------------------|
| Canvas technology | HTML5 Canvas or SVG with JS interop; NiceGUI ui.interactive_image is limited for this | QGraphicsScene/QGraphicsView -- native, powerful, already used for image viewer |
| Image manipulation | Client-side JS (canvas transforms) or server-side Python | QGraphicsPixmapItem with QTransform -- well-supported |
| Background removal | Must run server-side (Python/OpenCV), send processed image to client | Can run locally (Python/OpenCV) on full-resolution image |
| Performance | Limited by WebSocket round-trips for server-side processing; client-side JS for real-time transforms | Native performance, direct memory access to images |
| Touch support | Browser-native touch events | Qt touch event handling (less tested) |

**Key insight:** The desktop app will be significantly easier to implement because QGraphicsScene already provides exactly the right abstraction (items with independent transforms, built-in drag, scene-level zoom). The web app will likely need a JavaScript canvas library (Fabric.js or Konva.js) with NiceGUI JS interop, which is architecturally more complex.

**Recommendation:** Build desktop first as proof-of-concept, then port interaction model to web. Both must ship, but desktop is the natural prototype.

## Prior Art Analysis

### FJMS Jigsaw Puzzle
- **What it does:** Load fragment images by number, drag/rotate/flip/calibrate on a large touch screen
- **What it lacks:** No background removal, no recto/verso linking, no save/export (session-only), no DPI auto-calibration, hardware-dependent (42" touch screen prototype)
- **Our advantage:** Software-only (runs on any screen), background removal, DPI calibration, persistent saves, integrated with join metadata
- **Source:** [Friedberg Genizah Project Research Platform](https://pr.genizah.org/TheResearchPlatform_New.aspx)

### Mirador IIIF Viewer
- **What it does:** Multi-window IIIF image comparison with annotations, deep zoom via OpenSeadragon
- **What it lacks:** Not designed for free-form fragment arrangement; annotations are rectangular regions, not free-positioned overlays
- **Relevance:** Demonstrates IIIF image loading patterns; its canvas/annotation model is too constrained for jigsaw-style assembly
- **Source:** [Project Mirador](https://projectmirador.org/)

### Dead Sea Scrolls Digital Projects
- **What they do:** AI-assisted fragment matching, 3D scanning, virtual unwrapping
- **What's relevant:** Research-grade fragment assembly is a known hard problem; our scope (manual visual assembly with good UX) is the practical sweet spot
- **Source:** [Times of Israel - Dead Sea Scrolls Ultimate Jigsaw](https://www.timesofisrael.com/new-dead-sea-scrolls-project-will-use-latest-technology-to-solve-ultimate-jigsaw/)

## Sources

- [Friedberg Genizah Project - Research Platform](https://pr.genizah.org/TheResearchPlatform_New.aspx) - FJMS Jigsaw tool description
- [FGP ViewONE User Manual](https://fgp.genizah.org/txtFiles/ViewerHelp.pdf) - Image viewer capabilities
- [Project Mirador](https://projectmirador.org/) - IIIF viewer reference
- [OpenSeadragon](https://openseadragon.github.io/) - Deep zoom image viewer
- [Qt QGraphicsScene](https://doc.qt.io/qt-6/qgraphicsscene.html) - Desktop canvas framework
- [IIIF Physical Dimensions Service](https://iiif.io/api/annex/services/) - DPI/resolution metadata
- [IIIF Image and Canvas with Differing Dimensions](https://iiif.io/api/cookbook/recipe/0004-canvas-size/) - Canvas sizing patterns
- [OpenCV Background Removal](https://opencv.org/blog/remove-backgrounds-from-images-using-opencv/) - Segmentation techniques
- [NiceGUI Interactive Image](https://nicegui.io/documentation/interactive_image) - Web canvas options
- [WCAG 2.5.7 Dragging Movements](https://github.com/zauberzeug/nicegui/discussions/932) - Accessibility requirement for web drag operations (Level AA as of June 2025)
