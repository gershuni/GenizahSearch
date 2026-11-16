## [Unreleased] -- The result filters as chips (web, branch `claude/results-filter-chips`)

The result filters beside the count are chips now (the owner's choice of three designs,
2026-11-16): **Domains**, **Prints**, **PGP**, **Scholarly transcriptions** and the library filter,
each a pill with a chevron, grey while off and filled once it hides rows. Printed, PGP and scholarly
transcription open a short menu (all / only / without) instead of cycling through their states on
each click, so the next state is never a guess; domains and library open their dialogs as before.
**Search within** is a plain action, not a chip. A chip that is not available yet takes no space
at any width, so the row closes up instead of leaving holes (`.tb-hidden` in `common.css`).

Under 1024px the count, **Reset filters** and the chips wrap instead of scrolling sideways, so
none is off-screen; on a phone the count has the first line beside the export icons (pinned to
the corner) and Reset filters with the chips start the next (`.results-chip-break`).

Selecting results shows "N selected" and the two bulk actions (add the selected results to a list,
copy their text) again: they had stayed hidden, because the page showed them with `.style('')`,
which in NiceGUI only adds styles. They show and hide by a class now (`tb-hidden`); on a phone the
count leaves room for the bulk actions in the corner (`.has-selection`) and "N selected" starts the
chips' line. A new search or a restored history entry starts with nothing selected (the selection
is row positions, so the new rows showed as selected and the bulk actions acted on them), and
unticking one row of a full selection keeps the others (the select-all box moving to match was
taken as the reader's "select none").

The printed and PGP menus re-render through the same path as the scholarly one, which keeps the
word-search exclusions and the Toggle Filters panel's fields in force. Help describes the chips in
both languages.
