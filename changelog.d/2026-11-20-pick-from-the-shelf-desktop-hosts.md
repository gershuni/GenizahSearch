## [Unreleased] -- Pick from the shelf in the Reading Desk, Puzzle, Joins Lab, Joins and Share a Discovery (desktop, branch `claude/shelf-picker-desktop-hosts`)

### Added
- **Pick from the shelf** (📚) beside the shelfmark box of the Reading Desk, the Fragment Puzzle, the
  Joins Lab's anchor, the Fragment Joins dialog's fragment B and Share a Discovery's references
  (`desktop/widgets/shelf_pick_button.py`). The manuscript picked is added as a typed shelfmark is:
  to the desk, to the canvas, as the anchor, as fragment B (by its record), as a reference on the
  page given. Shown only where the shelf tree file is installed.

### Fixed
- The Fragment Joins dialog joins the record picked from a personal list for fragment B, not only
  its shelfmark: the record was forgotten as soon as its shelfmark was written into the box, so two
  records sharing one shelfmark could be confused.
- A fragment picked in Browse while the Reading Desk is open is added to the desk also when it is an
  Oxford Part's folio (it used to replace the desk's page with the Part's).
- A Puzzle fragment added again by its shelfmark keeps its external image (its address, provider
  and page were dropped when its folios were already known).
