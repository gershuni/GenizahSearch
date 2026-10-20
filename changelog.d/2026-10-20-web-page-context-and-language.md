## [Unreleased] -- Background work stays in its page; each visitor's own language (web, branch `ccr-bf48c2a5-pj4z5d`)

NiceGUI keeps the current page per asyncio task, and a task the page starts
(`asyncio.ensure_future`, `create_task`) begins with none. Every site below did
page work from such a task and swallowed the error, so the work silently did
not happen. Found from the tracker's /catalog-browse row and a sweep of every
task-spawn site under `web/`.

### Fixed
- **/catalog-browse** shows its first page of results again without a filter
  being picked (it opened with no results; confirmed on the live site
  2026-10-07). A browser that does not answer the saved-filter read in time now
  gets the unfiltered page instead of none.
- **The reading view's "How to cite" chip** follows the version shown by
  default (a PGP or FGP edition, a user correction); it kept the citation of
  the automatic transcription it replaced, disagreeing with the printed sheet
  and the Word export.
- **The correction editor** keeps auto-saving the draft every 30 s; it saved
  once and stopped.
- **Fragment Puzzle** keeps the canvas positions in the tab, so a reload or a
  return to /puzzle puts fragments back where they were (they came back
  stacked at the default spot). A fragment not drawn yet keeps its last
  position.
- **Fragment Puzzle auto-save works.** An edit of an open saved join (moving,
  nudging or rotating a fragment, cropping, adding, removing, changing folio,
  title or notes) is saved a moment later; it never saved. Opening or
  reloading a join writes nothing. A fragment whose image did not load keeps
  its saved place; an empty canvas never empties a saved join. A join opened
  from a link replaces the canvas the tab held before, and a reload brings
  that join back rather than the older canvas.
- **Fragment Puzzle crops are kept.** Save wrote a crop the visitor had
  confirmed as uncropped, and a reload lost any crop on an unsaved canvas.
- **Joins Lab**: the address bar follows an anchor set from a card's pin, a
  Known Joins pin or a list (a reload or a copied address reopened the old
  anchor); Retry after a failed export downloads the file; adding candidates
  to a list confirms it, and picking a fragment from a list in the picker runs
  in the page.
- **Search results**: Retry after the full text failed to load runs in the page.
- **The UI language is each visitor's own.** It was one value per process,
  set by every page render, so text drawn after an await and the library
  names in the search Word export, the list Excel export and both Parallels
  exports could come out in the previous visitor's language. `get_language()`
  now answers from the page, the task the page started, or the request
  (saved choice, else the browser's language); see `web/translations.py`.

### Changed
- **Element cleanup no longer slows with the number of open tabs.** NiceGUI
  3.8's `binding.remove` (run by every `element.clear()`) walked a marker for
  every bindable property of every element on every open page; it now removes
  the binding links only, as NiceGUI 3.15 does (`web/framework_patches.py`,
  stands down on 3.15+). Synthetic benchmark, one 300-element clear: 0.5 /
  2.7 / 44 ms with 1,000 / 10,000 / 100,000 other elements live before,
  0.04-0.09 ms after. This was
  the `binding.remove` under `browse.py` `clear()` seen during the post-restart
  reload loops (2026-10-07); the reload loops themselves stay open.
