# Quick Task 260319-dt1: Image Controls (Brightness, Contrast, Gamma) - Context

**Gathered:** 2026-03-19
**Status:** Ready for planning

<domain>
## Task Boundary

Add image adjustment controls (brightness, contrast, gamma, invert) to all image viewers across web and desktop applications, enabling better reading of difficult manuscripts.

</domain>

<decisions>
## Implementation Decisions

### Which Controls
- **Brightness** slider (-100 to +100, default 0)
- **Contrast** slider (-100 to +100, default 0)
- **Gamma** slider (0.2 to 3.0, default 1.0)
- **Invert** toggle button (on/off)
- No grayscale or sharpen — deferred unless users request

### UI Layout
- **Inline sliders** in a second toolbar row below existing zoom/rotate controls
- Compact: `B[━●━] C[━●━] G[━●━] [Inv] [Reset]`
- Fullscreen mode must also show these controls
- Add fullscreen button where not already present

### Puzzle Canvas
- Ideal: per-fragment brightness/contrast/gamma via Fabric.js image filters
- If too complex: defer puzzle to a follow-up task
- Do NOT apply CSS filter to whole canvas (affects all fragments equally — not useful)

### Save/Copy Behavior
- Desktop context menu "Copy Image" and "Save Image As" must export the adjusted image (with filters applied), not the raw original
- This means filters need to be baked into the exported pixmap

### Claude's Discretion
- Web implementation approach: CSS `filter` property on `<img>` elements (brightness, contrast, invert are native; gamma via SVG feComponentTransfer filter)
- Desktop implementation: Apply adjustments at QPixmap/QImage level for both display and export
- Whether controls persist when navigating between folios (reasonable default: reset on new image)

</decisions>

<specifics>
## Specific Ideas

- User is a power user working with difficult-to-read manuscripts — readability is the priority
- All modules need coverage: browse, result dialog, advanced view, reading desk (web + desktop)
- Existing toolbar patterns: zoom +/- buttons, rotation slider, rotate left/right buttons
- Hebrew RTL UI — labels should be translated

</specifics>

<canonical_refs>
## Canonical References

### Image Viewer Locations (from codebase exploration)
- **Web browse**: `web/pages/browse.py` — standard view (L4134-4215), fullscreen (L4382-4399), reading desk (L3292-3367)
- **Web search/advanced**: `web/pages/search.py` — normal (L5255-5291), fullscreen (L4642-4661)
- **Web puzzle**: `web/pages/puzzle.py` — Fabric.js canvas (L3358), toolbar (L3266-3344)
- **Desktop**: `genizah_app.py` — `ZoomableScrollArea` (L1394-1576), `ManuscriptViewerWidget` (L1578-1988)
- **Desktop browse**: `genizah_app.py` L14251, reading desk L14275-14278

### CSS Filter Reference
- `filter: brightness(1.5) contrast(1.2) invert(1)` — native CSS
- Gamma requires SVG filter: `<feComponentTransfer><feFuncR type="gamma" amplitude="1" exponent="0.5"/></feComponentTransfer>`

</canonical_refs>
