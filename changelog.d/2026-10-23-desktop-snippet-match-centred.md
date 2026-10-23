## [Unreleased] -- The found words in the middle of the Snippet cell (desktop, branch `claude/inspiring-wozniak-vgf3et`)

- The Search tab's Snippet column shows the found words in the middle of the cell. The cell
  showed the end of the snippet: Qt centres a paragraph only while it fits its label, and a
  longer right-to-left line is laid out from the label's left edge, so a match cut 60
  characters in from either side stood at the cell's right edge, cut, or beyond it whenever
  the snippet was wider than twice the column -- in the app's font, most rows. The cell is
  now `desktop/ui_widgets.py::MatchCentredSnippet`: the label at its natural width, slid so
  the marked words are centred, never past either end of the text. Several marks are centred
  as one span; a match at the start or end of a page is shown with that end of the text at the
  cell's edge; a snippet that fits is shown whole, as before. Text selection in the cell is
  unchanged. Pinned by `tests/test_results_snippet_centred.py`, which reads where the red
  pixels land.
