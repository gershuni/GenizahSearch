## [Unreleased] -- Several from the shelf at once: the Reading Desk, the Puzzle and a list, on the desktop (desktop, branch `claude/shelf-picker-desktop-multi-hosts`)

### Added
- **Several to the Reading Desk**: from the desk toolbar's shelf button, or from Browse's own "Pick
  from the shelf" with its switch "Open more than one" on (the same window shows boxes to tick, where
  it is): tick shelfmarks or whole volumes, and "Open them in the Reading Desk" adds them to the desk
  that is open, or starts one with the manuscript shown, or, when none is shown, starting with the
  first one ticked (one alone just opens). The desk holds up to 30 manuscripts and 1,000 pages,
  counting what is on it: a tick past that is refused with the limit, a collection too big to take
  whole cannot be ticked, and what does not fit when it is added stays ticked for the next time. One
  already on the desk is not added twice; a window shows the pages loading.
- **The Fragment Puzzle's shelf button** takes several at once, up to 30 fragments with those on the
  canvas, placed side by side; a fragment leaves the choice only once its image is on the canvas, so
  one whose image fails stays chosen.
- **Add from the shelf on the Lists tab** (not on the recent list): the choice goes into the list
  shown in one add, up to 2,000 at a time (ticking stops there); a manuscript the list holds already
  is not added again.
- What is chosen for each (Reading Desk, Puzzle, lists) is kept in the session file until it is
  added or cleared. The chips name each choice with its library's code first ("CUL T-S 32").

### Fixed
- **Reading Desk sources lost on overlapping adds:** each add replaced the source loader before it
  and dropped its results (a manuscript's PGP sources could stay missing); every loader now keeps
  its results, and the desk's exit lets them finish without them.
