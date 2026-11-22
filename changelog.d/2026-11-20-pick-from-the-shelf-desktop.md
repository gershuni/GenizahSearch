## [Unreleased] -- Pick from the shelf, and previous / next on the shelf (desktop, branch `claude/shelf-picker-desktop-381357`)

### Added
- **Pick from the shelf** in the desktop app: beside Go on Browse, and in the Go to box (Ctrl+G), a
  button opens the library > collection > volume > shelfmark tree as the website has it, in
  columns, with counts, a filter on every column, the breadcrumb back to any level, Hebrew names in
  the Hebrew interface and the current manuscript marked. A click, Enter or Space on a shelfmark
  opens it in Browse (`desktop/widgets/shelfmark_tree_picker.py`, drawing the website's own columns
  from `shared/shelfmark_tree_columns.py`). Library names are never cut; the shelfmarks stand in
  three, two or one columns, as many as show the longest whole.
- **The place on the shelf** under the manuscript's title on Browse: its library, collection and
  volume, each opening the picker there, and its position ("7 of 279").

### Changed
- **Previous / next manuscript follow the shelf** (Ctrl+Shift+arrows, mirrored in Hebrew) wherever
  the manuscript has a place on it, across volumes and collections, stopping with a notice at the
  library's ends; their tooltips name the manuscript they lead to. Inside an Oxford Part they still
  walk its folios and Parts. A manuscript with no place, or an installation without the shelf tree
  file, keeps the transcription file's order.
- **Paging past a manuscript's last page** (the page buttons, Ctrl+PgDown / Ctrl+PgUp) goes on to
  the next manuscript in the same order: the Oxford Part's next folio or Part, else the next on the
  shelf, else the file order as before; going back lands on the previous manuscript's last page.

### Data and packaging
- The installer carries `shelfmark_tree_data/shelfmark_tree.db`: `GenizahSearchPro.spec` bundles it
  and runs `scripts/verify_shelfmark_tree_sidecar.py --leaves-from-meta` before bundling,
  `build_app.bat` stops early when it is missing, `scripts/checkpoint_sidecars.py` checkpoints it.
  `SHELFMARK_TREE_ENABLED` (on unless set off) AND the file opened and checked at start, off the UI
  thread: without it the button and the place on the shelf are absent.
