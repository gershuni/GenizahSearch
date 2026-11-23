## [Unreleased] -- Browse: a correction draft stays with the page it was written on (web, branch `claude/gs5-draft-stays-with-its-page`)

- Turning the page while editing a correction used to leave the editor open over the next page.
  Save Draft or Submit then filed the first page's text under the second page (GS-5).
- Now the turn closes the editor. A change not yet saved is kept as a draft of its own page, and
  the site says "Draft saved". Every correction write is filed under the page the editing began
  on, never under the page on screen.
- A draft save still running when the page turns is not written twice. The kept draft waits for
  that save, and updates its draft only if the text has changed since.
- The code is in `web/pages/browse_correction_edit.py` (new: `EditedPage`, `end_edit_for`,
  `keep_draft`, `write_correction`), wired into `web/pages/browse.py`. `create_browse_page` is 17
  lines shorter, and its ceiling in `tests/test_size_ratchet.py` is lowered to match. Tests are in
  `tests/test_browse_correction_off_loop.py` (a page turn while editing) and
  `tests/test_browse_correction_edit.py`.
