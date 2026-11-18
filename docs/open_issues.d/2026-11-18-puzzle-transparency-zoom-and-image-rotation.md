## P3 web: Fragment Puzzle shows transparency faults around fragments
Where: `web/pages/puzzle.py` (canvas, background removal), `shared/puzzle_image_service.py`, `shared/puzzle_export.py::compose_puzzle_export`
Opened: 2026-11-18

Reported by the owner on 2026-11-18 while testing the Reading Desk's assembled picture: the
puzzle has "transparency issues". Not reproduced or narrowed down yet -- it is not recorded
whether the fault is in what background removal leaves around a fragment, in how transparent
areas of one fragment lie over another on the canvas, or in the exported picture. One thing was
seen in passing: in the saved join of ENA 2740.3 + ENA 2805.2 the shelfmark strip beside the
fragment survives background removal as a dark bar, in the Puzzle and in the picture the desk
shows.

Next step: a screenshot or a named join from the owner, then tell the three cases apart.

## P3 web: Zooming in is slow in the Fragment Puzzle
Where: `web/pages/puzzle.py` (canvas zoom)
Opened: 2026-11-18

Reported by the owner on 2026-11-18: zooming in on the puzzle canvas is slow. Not measured
yet -- neither the delay, nor the number or size of the fragments it needs, nor whether the
time goes to redrawing the canvas or to fetching larger images.

## P3 browse: The image controls turn an image only in quarter turns
Where: `web/pages/browse_desk_images.py::_toolbar` (`window.rdRotate`, +/-90), the single-page viewer's rotate buttons in `web/pages/browse.py`
Opened: 2026-11-18

The Reading Desk's image toolbar and the Browse viewer rotate by 90 degrees a click. A fragment
photographed at a slant cannot be set straight to read against its text; the Fragment Puzzle
turns a fragment freely. Asked for by the owner on 2026-11-18.

Fix sketch: a fine-rotation control beside the quarter-turn buttons (a small slider or +/-1
degree steps) writing the same `rotation` the viewer script already keeps per image, with
Reset View returning it to zero.
