## [Unreleased] -- The found words in the middle of the Snippet and Context cells (desktop, branch `claude/inspiring-wozniak-vgf3et`)

- The Search tab's Snippet column shows the found words in the middle of the cell. The cell
  showed the end of the snippet: Qt centres a paragraph only while it fits its label, and a
  longer right-to-left line is laid out from the label's left edge, so a match cut 60
  characters in from either side stood at the cell's right edge, cut, or beyond it whenever
  the snippet was wider than twice the column -- in the app's font, most rows. The cell is
  now `desktop/ui_widgets.py::MatchCentredSnippet`: the label at its natural width, slid so
  the marked words are centred, never past either end of the text. Several marks are centred
  as one span; a match at the start or end of a page is shown with that end of the text at the
  cell's edge; a snippet that fits is shown whole, as before. Text selection in the cell is
  unchanged.
- The Composition tab's Context and MS Context columns centre the matched chunk the same
  way (`ContextSnippet`). They were `HiddenScrollArea`, a scroll area whose scrollbar was set
  from the chunk's share of the text's characters, so the chunk stood near the middle only as
  the letters' widths allowed, after a 10 ms timer per cell and per column resize. Kept from
  it: the bold dark-red marks, the whole text as the tooltip, the 5 px gap between the two
  columns, the mouse wheel sliding the text sideways, and the MS Context cell centring on the
  chunk the Context cell shows. New: HTML in the text (`<`, `&`) is shown as text.
- Pinned by `tests/test_match_centred_cells.py`, which reads where the red pixels land.
