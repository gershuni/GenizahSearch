## P3 web: Fragment Puzzle forgets that a mirrored fragment stands in for a missing side
Where: `web/pages/puzzle.py` (`flipAllPuzzle`, `_standIn`, `getState`), `shared/puzzle_model.py::PuzzleFragment`
Opened: 2026-11-18

Flip Puzzle shows a fragment mirrored when there is no picture of its other side (none exists,
it failed to load, or the list of sides had not arrived). The canvas remembers this in
`_standIn`, so the next flip shows the fragment as it is again. That mark is not part of what is
saved: a saved join and the tab's copy keep only `flip_h`. After a reload the fragment is an
ordinary flipped picture, so flipping back turns it to its other side (if that now loads) and
leaves it mirrored, where it should return to the original unmirrored side.

Found by an external review of the flip changes (2026-11-18); reproduced there with a probe, not
in a browser. It needs a failed or missing other side, a reload while flipped, and a flip back.

Fix sketch: a `stand_in` field on `PuzzleFragment`, carried through `getState`, the tab state,
`shared/puzzle_service.py`'s explicit serialization and the published `fragments_json`. The
desktop puzzle reads the same model, so the field needs a default that old documents load with.
