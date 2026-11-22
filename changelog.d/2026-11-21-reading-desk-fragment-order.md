## [Unreleased] -- Reading Desk: move a fragment up or down (web, branch `claude/desk-fragment-order`)

### Added
- **The order of the fragments on the Reading Desk can be changed.** Up and down buttons ("העבר
  למעלה" / "העבר למטה") stand beside each fragment's name: over its images in "All images", beside
  the shown image in "One at a time", and on the text side's fragment headers. A fragment moves one
  place; its images, its text and the order the Puzzle takes the desk in follow, and the order is
  kept with the desk. The first cannot go up nor the last down; a desk of one has none
  (`web/pages/browse_desk_images.py::move_entry`, `move_buttons`; `browse.py::move_on_desk`).

### Tests
- `tests/render_smoke/test_reading_desk_document_render_smoke.py`: a fragment moved down from the
  images and back up from the text side, its images and text following and the order saved; the
  buttons at either end disabled; none on a desk of one; `move_entry`. Mutations of the move, the
  wiring on either side, the disabled end and the saving each fail a test.
