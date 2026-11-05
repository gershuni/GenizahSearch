## [Unreleased] -- The dictionaries' look-up key and phone chip, a denser sidebar with an icon rail, the shortcuts card, one sign-in button (web, branch `claude/pre-release-ux`)

Owner, 2026-11-05, before 9.14.0. The sidebar order and the shortcuts work come from the readers'
own use (PostHog, 30 days to 2026-11-05).

- **Dictionaries without the switch.** On Browse, wherever a dictionary is served (the page marks
  the pane `data-blau-pane`; `web/static/js/blau_lookup.js`):
  - D or M tapped arms the next click, which looks up the word under it; held with a mouse, every
    click while it is down. The owner's first test held M on a laptop touchpad and it failed:
    Windows ignores the touchpad while a letter key is held. So a tap is enough, and Alt + a click
    does the same (the touchpad is not blocked for Alt). The pointer shows `help` over the text
    while armed.
  - Words selected and D or M looks up the selection (its first six words with Hebrew letters,
    line numbers and punctuation dropped).
  - On a phone, selected words show a "Look up" chip under them (`data-blau-chip`, translated).
  - Esc closes the panel, from its own box too.
  - D and M by name, by the Hebrew letter, or by the physical key; never while a box has the focus,
    a dialog or menu is open, or with Ctrl/Cmd. The switch's tooltip names the key under what
    pressing it does. `dictionary_lookup` counts `how` = `key` / `alt` / `selection` / `chip`
    beside `word` and `box`.
- **Sidebar.** No "NAVIGATION" / "TOOLS" titles; rows 34px apart (were about 64); 256px wide (was
  280) with no padding of its own. The order is the readers': Home, Search, Browse by Shelfmark,
  Find Parallels, Browse by Identification, Joins Lab, Computed Identifications, Atlas, Fragment
  Puzzle, Community, My Lists; then Start Here, About the Genizah, AI tools, Help Center, Download
  App, Settings, Keyboard shortcuts. A button at its top collapses it to a 64px icon rail (Quasar's
  `mini`; tooltips name the icons there, and every row has an `aria-label`), remembered per visitor
  (`drawer_mini`); its arrows point the same way in both languages (the sidebar is at the left in
  both). Below 1024px the overlay drawer is unchanged.
- **Keyboard shortcuts.** `?`, or "Keyboard shortcuts" in the sidebar (Enter or Space), opens a card
  listing them (`web/keyboard_shortcuts.py`, `web/static/js/shortcuts.js`, loaded with the layout);
  `/` jumps to the page's search box (/search, the homepage) or the header's go-to box. Browse's
  buttons name their keys in their tooltips. Fixed on Browse: F now works on a Hebrew layout;
  Ctrl+F and Ctrl++ no longer also toggle full screen and zoom the image; the arrows follow the page
  as the buttons do (in English, → is the next page; it was the previous); 0 resets the view. No
  key reaches the page while a dialog or menu is open.
- **Header.** Signed out, one "Sign in" button (its dialog has the Register tab). Signed in, the
  account icon alone: the name heads its menu and is the icon's tooltip, and the menu gains My
  Lists.
- **Help Center** (`web/pages/help.py`, both languages): a section after the Introduction on the
  keys, the icon rail and the one sign-in button (`help-shortcuts`), and in "Browse by Shelfmark"
  the dictionaries (the owner's approved release bullet) and their look-up key, shown only where a
  dictionary is served. The Hebrew reworded in 37 places the owner approved (among them "ניקוד"
  for a match's score, "חלופי אותיות", "ה"א הידיעה הערבית", "צפיין", the calques "בקרים",
  "ספינרים", "מגדירים", "קבוצות שורה", and one spelling each of שאילתה and מטא-נתונים); the
  same sentences in the desktop `Help.html` follow. The volume selector's bullet loses its
  release-note words in both languages, and the two old 🆕 marks go. The Hebrew introduction names
  the site אתר הגניזה של דיקטה (owner). Fixed: a list under a line of text with no blank line
  between rendered as one run-on paragraph, in some fifteen sections of both languages; every
  block now goes through `_md`, which adds markdown2's `cuddled-lists`.
- **Labels (owner):** the phone chip says "Look up in the dictionary" (חיפוש במילון); the
  Parallels button "Add witness text" is הוספת עד נוסח (was הוספת נוסח עד); the account menu's
  "Logout" is התנתקות (was התנתק), in both apps.
- New strings: 16, all approved by the owner.
