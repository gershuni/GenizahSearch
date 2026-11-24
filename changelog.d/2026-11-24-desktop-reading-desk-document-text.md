## [Unreleased] -- Desktop Reading Desk: a document's own edition, shown once (desktop, branch `claude/gs307-reading-desk-edition`)

- A desk opened for a PGP document showed an edition under every fragment, the same text over and
  over, and sometimes another document's: a desk of PGP 1702 showed edition 5171, which belongs to
  PGP 1246, under all three fragments (GS-307). The desk asked for each fragment's sources, and
  those are the sources of every document the fragment belongs to.
- Now, while every fragment on the desk belongs to the document it was opened for, the text side
  shows that document's own edition once, as the website has done since 10.1.0. A switch above it
  moves between Edition, Translation and By fragment. With several editions, [Change version] or
  the Version chooser picks one, and the Cite bar credits the one on show. By fragment opens each
  fragment on a transcription of its own (its FGP edition, else the automatic one), with this
  document's PGP sources still on offer and another document's never. The choices are kept with
  the desk across a restart.
- On the dark theme the desk's text side no longer draws light rows (the edition's attribution, the
  fragments' names, the page breaks) with light text on them, and the line-number gutter is dark
  beside dark text, in Browse as on the desk. In Hebrew, the count of the document's fragments on
  the desk reads in its natural order, on the website too.
- The rules are shared by both apps in `shared/reading_desk_document.py` (moved from
  `web/pages/browse_desk_document.py`, which now draws the web's panel only). The desktop desk's
  text side moved out of `genizah_app.py` into `desktop/reading_desk_text.py`. `genizah_app.py` is
  125 lines shorter, and its ceilings in `tests/test_size_ratchet.py` are lowered to match. Tests are
  in `tests/test_desktop_reading_desk_qt.py`.
