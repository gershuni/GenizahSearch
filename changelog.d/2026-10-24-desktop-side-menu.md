## [Unreleased] -- A side menu as an option, text size, Ctrl+1..7, and an account menu (desktop, branch `claude/sleepy-hawking-id0eg0`)

Owner, 2026-10-24: "move the menu to one of the app sides, remove the V tick boxes, and add also the
puzzle, joins lab and settings to the menu", then "Keep the tabs. Offer an option to change them to
the side menu", and "option to change the size of the texts in search etc windows".

- **Two menu layouts.** Settings > Menu: *Tabs at the top* (the default, as before) or *Side menu*
  (`desktop/widgets/side_nav.py`), a column on the window's start side -- right in Hebrew, left in
  English -- with the seven sections, Fragment Puzzle and Joins Lab, and at its foot Settings, the
  account, the language switch, GenizahSearch.com and the version. The choice applies at once and is
  kept (`nav_layout` in config.pkl). The tab layout's ☰ menu offers *Move the menu to the side*; a
  right-click on the side menu offers *Back to tabs at the top*.
- **The ☰ menu without tick boxes**, now with Fragment Puzzle, Joins Lab and Settings after the
  sections. The current section is bold; the old menu drew an empty checkbox beside every other one.
- **The side menu:** the current section has an accent bar; ☰ shows or hides the labels (remembered).
  On a narrow window (a 1080p screen at 300% is 640 px wide) it shows icons only and ☰ opens the full
  list; a short window scrolls it rather than being made taller by it.
- **Ctrl+1 .. Ctrl+7** open the sections in their order, in either layout; each tab's tooltip and
  each menu entry shows its key.
- **An account menu.** Logged in, the Login button shows your name with ▾ and opens *Sync lists now*
  (enabled when the Lists tab's button is) and *Logout*. One click on your name used to log you out.
- **Text size.** Settings > *Text size in result lists*: Normal, Large, Larger or Largest, for the
  Search, Composition Search, Browse by Identification and Personal Lists results (the headings keep
  their size); Ctrl+= / Ctrl+- step it and Ctrl+0 resets it. The Browse tab gains the Manuscript
  Viewer's A- / A+ beside Find, and the two now share one transcription size (changing it in either
  changes both); on the Browse tab the same keys size the transcription.
- **Settings > Cancel** no longer undoes settings changed outside the dialog since the program
  started: the dialog is built once, and it kept the startup snapshot. It now takes one each time it
  opens.
- Help (English and Hebrew): a Menu section (layouts, the side menu, the shortcuts), and the Login
  and Joins Lab passages say where they are in each layout.
