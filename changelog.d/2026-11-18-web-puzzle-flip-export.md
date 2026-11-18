## [Unreleased] -- Fragment Puzzle: flip, PNG export and full screen (web, branch `fix/web-puzzle-flip-export`)

### Fixed
- **Export PNG left fragments out.** The file was composed on the server, which had to find
  every picture again in its own cache; a picture that had reached the canvas another way (the
  browser extension, a library fallback) was skipped without a word, so the file held some of the
  fragments and an empty space. The browser now draws the file from the canvas itself
  (`puzzleCanvas.exportPng`): what is on screen is what is saved, with crops, flips and stacking
  order, on white with the shelfmark banner. The server composition remains as the fallback for a
  canvas the browser may not read (a picture shown straight from a library).
- **Flip Puzzle with slow pictures.** Each side's picture was drawn whenever it arrived, at the
  place recorded when it was asked for. Flipping again before it arrived left fragments on the
  wrong side or at the wrong place. Every picture request now carries a number
  (`_loadSeq`); only the newest one for a fragment draws, and it takes its place from the object
  on the canvas at that moment (`_swapFragmentImage`).
- **Flip Puzzle lost every crop**, which also moved the fragment. The crop is now kept, mirrored,
  and flipping back restores the puzzle exactly. Stacking order is kept too. A fragment with no
  picture of its other side is shown mirrored, so its outline still fits its neighbours.
- **A fragment appearing twice.** `?add=` and `?doc=` stayed in the address, so every reload of
  the page (a language switch, a lost connection) added the fragment again beside the saved one,
  recto next to verso once the saved one had been flipped. Both are taken out of the address once
  carried out. Adding the same fragment twice while its picture was loading, and a picture asked
  for by a document opened earlier, also drew a second copy; both are now dropped.
- **A half-flipped puzzle is not saved.** While other sides are still loading the fragments stand
  at their new places with their old pictures; auto-save and the tab's copy now wait until the
  canvas reports it has settled (`isBusy`, `_swapEnded`). A side that fails to load, or whose
  list of sides has not arrived, is shown mirrored and flips back.
- **Crop:** cropping a fragment cropped before is measured against the whole picture (it saved a
  negative crop, and could not be opened up again); Revert returns to the crop the session
  started from; deleting the fragment being cropped ends the crop; exporting no longer ends it.
- **Changing the threshold or background while another side was loading** left the fragment
  showing one side and recorded as the other. A page of an external library's manuscript is now
  saved with its own address after moving to another page.
- **The canvas never grew with the window**: the resize handler measured Fabric's own fixed-size
  wrapper. The toolbar now wraps instead of running off the side.

### Added
- **Full screen** button in the puzzle toolbar: the toolbars and canvas fill the window (and the
  screen, where the browser allows it). Escape leaves it.

- **Open in Puzzle** for a whole join: in the Connected Fragments panel (beside View All
  Fragments) and in the Reading Desk header. All the join's fragments open on one canvas
  (`web/joins_lab_storage.py::stage_fragments_for_puzzle`, the handoff the Joins Lab already
  uses). They open as a puzzle of their own, never inside a saved draft the tab had open; when
  the tab holds an unsaved puzzle, the visitor is asked whether to replace it or add to it.

### Tests
- `tests/test_puzzle_canvas_js.py` runs the real canvas script in Node against a stand-in for
  Fabric and replays the orders that went wrong. Five of its seven canvas cases fail on the
  script as it was.
