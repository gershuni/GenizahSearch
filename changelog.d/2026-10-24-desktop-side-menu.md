## [Unreleased] -- The desktop menu moves to the side of the window (desktop, branch `claude/sleepy-hawking-id0eg0`)

Owner, 2026-10-24: "move the menu to one of the app sides, remove the V tick boxes, and add also the
puzzle, joins lab and settings to the menu".

- **One side menu** (`desktop/widgets/side_nav.py`) on the window's start side, right in Hebrew and
  left in English, in place of the tab bar, the ☰ "All sections" menu and the buttons in the tab
  row's corner. It lists the seven sections, then **Fragment Puzzle** and **Joins Lab**, and at its
  foot **Settings**, the account (Login / Logout), the language switch, GenizahSearch.com and the
  version (click for the update check).
- **No tick boxes.** The current section is highlighted with an accent bar; the old menu drew an
  empty checkbox beside every other section.
- **☰ switches between labels and icons only**, and the choice is remembered (`side_nav_expanded`
  in config.pkl). On a narrow window (at 300% zoom a 1080p screen is 640 px wide) the menu stays
  icons-only and ☰ opens the full list instead, still without tick boxes. A short window scrolls
  the menu rather than being made taller by it.
- The pages gain the height of the old tab row; the menu takes about 175 px of width with labels
  (wider in English) and about 57 px as icons only.
- Help: the Joins Lab and Login passages say where the entries now are (English and Hebrew).
