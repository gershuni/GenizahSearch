## [Unreleased] -- Reading Desk: a document's edition, shown once (web, branch `feat/desk-document-edition`)

A PGP document that lies on several fragments is one text. The Reading Desk showed it
fragment by fragment: it asked for sources per fragment, which returns the sources of every
document the fragment belongs to. Measured on the sidecar's 1,722 documents on two or more
fragments: on 548 the same source was the one shown under two or more of the fragments; on
128 a fragment opened on an edition of a different document that shares it; and a text with
"recto"/"verso" headings was cut at them and dealt out to each fragment's first and later
pages, without the headings.

### Changed
- A desk opened for a document ("View All Fragments") shows that document's text once,
  beside the stack of images: one selector over the document's own editions, the chosen
  source whole, with its headings and its attribution. Translations are a separate choice,
  not entries in the edition list. "By fragment" shows one text per fragment, opening on
  what the fragment has of its own (not the PGP edition again under each), and "Edition"
  returns.
- The rule for "the desk is the document": every fragment on it belongs to the document it
  was opened for. Removing a fragment keeps the edition and says how many of the document's
  fragments are on the desk. Adding a fragment from elsewhere makes it a hand-built desk
  (one text per fragment, titled "Reading Desk") until that fragment is removed.
- The image side has two views, chosen above it. "One at a time": a strip of thumbnails of
  every page of every fragment and one large image with one toolbar -- the default beside a
  document's edition. "All images": the scrolling stack, one compact row of controls per
  image -- the default on any other desk. Brightness, contrast, gamma and invert are behind
  one button. A fragment of one leaf keeps Recto/Verso; a longer one is numbered.
- A third image view, "Assembled", on a desk of two or more fragments: the picture of a
  saved join of these fragments -- the visitor's own saved joins (each visitor sees only
  their own; the picture comes from the Puzzle's export route) and published joins (which
  carry their picture). A join counts when it holds at least two of the desk's fragments.
  "Open in Puzzle" opens it there; with no saved join, "Arrange in Puzzle" takes the desk's
  fragments there. The picture is read-only here: arranging stays in the Fragment Puzzle.
- With one image on show and the text one per fragment, the text side is that image's
  alone -- its fragment, and its side of the fragment, not recto and verso together -- with
  its version chooser, and follows the thumbnail chosen. Only the text side is redrawn, so
  the page stays where the reader was. Beside a
  document's edition it opens on the fragment's own text (FGP, else automatic); the PGP
  sources stay in the chooser. A document's edition is not cut to the fragment: the sidecar
  does not say which part belongs to which image (of the 607 editions of multi-fragment
  documents, 147 carry parts that can be found in their text, numbered by PGP's canvases,
  not by the images shown).
- A Bodleian fragment named as PGP names it ("Bodl. MS heb. ...") is told by its library in
  the catalogue and asked of the Bodleian; the desk asked NLI, which has no image of it.
- In document mode the two panes stack on a narrow screen, images above text.
- Every desk: the titles, fragment headers and image toolbars sit close together (they were
  1rem apart, and padded twice).
- A restored desk has the reader's choices before it is first drawn, and every choice on the
  desk (edition, translation, text view, image view, image shown, join, a fragment's
  version) is saved as it is made. A fragment's chosen version stays chosen when its text is
  redrawn for another image.
- A saved join of more fragments than the export route composes for an outside caller is
  said to be too large to show on the desk, with the way to the Puzzle, not reported as a
  failure.
- `/browse` with no address and a saved desk was a server error (the desk was drawn before
  any page was loaded); it now loads the desk's first fragment and the desk comes up with it.
  A desk restored as the page opens is filled without being drawn until its page arrives;
  before, the restore raised on drawing it, was logged as an error, and the page load that
  followed drew the desk.

### Unchanged
- A hand-built desk, a join without a PGP document, and a document with no edition or
  translation keep the per-fragment layout (more compact, as above). Automatic transcriptions are never joined into
  one text.

### Files
- `web/pages/browse_desk_document.py` (new): the rule, the sources, the panel.
- `web/pages/browse_desk_images.py` (new): the image side's views.
- `web/pages/browse_desk_puzzle.py` (new): the assembled picture of a saved join.
- `web/pages/browse.py`: the desk's text pane; `enter_joined_view(selected_sources=)`.
- `shared/genizah_translations.py`: two strings.
- `tests/test_reading_desk_document.py`, `tests/render_smoke/test_reading_desk_document_render_smoke.py`.
