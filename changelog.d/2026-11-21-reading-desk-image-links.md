## [Unreleased] -- Reading Desk: the links of each image (web, branch `claude/desk-image-links`)

### Added
- **Beside every image on the Reading Desk, a menu of its links** (the "External link" icon), as
  the single page shows them: the image's own leaf at the Bodleian (recto, verso, or the image
  itself), then NLI Ktiv, the Bodleian part and the whole volume, Cambridge CUDL, the Katz Center's
  components, the holding library's own viewer and the PGP document (each side's, where the recto
  and verso are in two documents). The links are looked up the first time a fragment's menu is
  opened, off the event loop, and kept once every source has answered
  (`web/pages/browse_desk_links.py`). In "All images" every image has one; in "One at a time" the
  shown image.

### Tests
- `tests/test_reading_desk_image_links.py`: a Bodleian leaf's own links on its image and the
  fragment's after them; Cambridge's CUDL item once; the Katz Center's components; one source
  failing leaves the others; a catalogue-only record has the PGP link alone; one lookup per
  fragment. `tests/render_smoke/test_reading_desk_document_render_smoke.py`: a menu beside each
  image, nothing looked up until one is opened, its links in order, not looked up again. Mutations
  of the menu's place, the one lookup, the order, the single address and the image's page each fail
  a test.
