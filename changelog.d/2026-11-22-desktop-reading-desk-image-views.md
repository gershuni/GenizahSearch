## [Unreleased] -- Desktop Reading Desk: one image at a time, all images, puzzle images, and the fragments' order (desktop, branch `claude/desktop-reading-desk-parity-ca7173`)

### Added
- **The desktop Reading Desk's image side has the website's three views**, in one frame (GS-294):
  the image beside the text with its controls above it, and a narrow column on the outer edge
  with the view switcher above it.
  - **One at a time**: the column is a strip of thumbnails of every image of every fragment;
    the image beside the text is the one chosen. The text side then shows only that image's
    fragment, and only its page when the page records the image's NLI id (never by position:
    a library's own images may be in another order). The fragment's line above its text is gone
    (its name and remove are over the image) and the "Version" chooser above the text holds that
    fragment's texts. The default on a desk opened for a PGP document with an edition or a
    translation, as on the website.
  - **All images**: the stack the desk always had, the text scrolling in step with it; a
    thumbnail brings its image into view.
  - **Puzzle images** (two or more fragments on the desk): the saved puzzles of these fragments,
    the reader's own and the published ones, listed in the column with their pictures and a
    check box each (the first four shown by default), the ticked ones one under another, each
    with its controls and "Open in Puzzle"; "New Puzzle" opens the Fragment Puzzle on a new
    canvas with the desk's fragments, after its own question about the puzzle on the canvas. An
    own puzzle's picture is composed as the Puzzle exports it, and says when fragments' images
    could not be loaded.
- **A fragment moves up or down** (▲ ▼ beside its name, over the image in one at a time, on its
  header in all images): its images, its text and the order the Puzzle takes follow.
- **Every desk image has the adjustments** (brightness, contrast, gamma, invert) behind one
  button, beside zoom, rotate and a new "Reset View".

### Fixed
- The image side opens with half the room and keeps its width when the view changes or the
  window grows; before, the text took every spare pixel and the images sat at their minimum
  width. The divider comes back where it was when the desk is left.
- "[change version]" keeps the fragment's sources: choosing V0.8 used to delete them (no way
  back), and a translation chosen beside an edition was never shown.
- The desk's text lines are in the interface's language: "[Change version]" / "[שנה גרסה]",
  "[Remove]", "Current", the page separators, the title and the number of fragments were
  English in the Hebrew interface.
- Several fragments added to the Fragment Puzzle at once (New Puzzle, Add all) each get their
  own place; they were laid on top of each other when their folios arrived before any image.
  New Puzzle lays them out in the desk's order, whichever fragment's folios arrive first, and
  each "Loading..." stands where its fragment will be.
- A manuscript browsed while the desk is open no longer replaces the desk's text or its
  "Version" chooser, and the chooser is the browsed page's own again when the desk is left.
- Replacing the desk's sources worker, or leaving the desk, no longer drops a running thread;
  every image loader the desk starts is let go safely when it is redrawn or left.

### Not in this change
- A PGP document's edition shown once for the whole document (the website's document mode)
  is still to come on the desktop: the text side remains one text per fragment.
- Each image's "External link" menu (the website's, in review) is not on the desktop.

### Tests
- `tests/test_reading_desk_views.py`, `tests/test_reading_desk_puzzles_shared.py` (the shared
  rules), `tests/test_desktop_reading_desk_qt.py` (the frame, the views, the puzzle list, the
  order, the text following the image, the version chooser, the divider, the scroll sync),
  `tests/test_reading_desk_new_puzzle_qt.py` ("New Puzzle" on the real Puzzle window: Cancel
  changes nothing).
