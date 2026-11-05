## [Unreleased] -- The dictionaries' M key, a denser sidebar with an icon rail, the shortcuts card, one sign-in button (web, branch `claude/pre-release-ux`)

Owner, 2026-11-05, before 9.14.0. The sidebar order and the shortcuts work come from the readers'
own use (PostHog, 30 days to 2026-11-05).

- **Dictionaries, M key.** On Browse, holding M (or the Hebrew letter on a Hebrew layout, or the
  physical M key) and clicking a word looks it up; selecting words and pressing M looks up the
  selection (its first six words with Hebrew letters, line numbers and punctuation dropped). Both
  work with the dictionaries switch off: the page marks the pane `data-blau-pane` wherever a
  dictionary is served (`web/static/js/blau_lookup.js`). Never while a box has the focus or with
  Ctrl/Alt/Cmd. The switch's tooltip says so under what pressing it does. `dictionary_lookup`
  counts `how` = `key` / `selection` beside `word` and `box`.
- **Sidebar.** No "NAVIGATION" / "TOOLS" titles; rows 34px apart (were about 64); 256px wide (was
  280) with no padding of its own. The order is the readers': Home, Search, Browse by Shelfmark,
  Find Parallels, Browse by Identification, Joins Lab, Computed Identifications, Atlas, Fragment
  Puzzle, Community, My Lists; then Start Here, About the Genizah, AI tools, Help Center, Download
  App, Settings, Keyboard shortcuts. A button at its top collapses it to a 64px icon rail (Quasar's
  `mini`; tooltips name the icons there), remembered per visitor (`drawer_mini`). Below 1024px the
  overlay drawer is unchanged.
- **Keyboard shortcuts.** `?` on any page, or "Keyboard shortcuts" in the sidebar, opens a card
  listing them (`web/keyboard_shortcuts.py`, `web/static/js/shortcuts.js`); `/` jumps to the
  page's search box (/search, the homepage) or the header's go-to box. Browse's buttons name
  their keys in their tooltips. Fixed on Browse: F now works on a Hebrew layout; Ctrl+F and Ctrl++
  no longer also toggle full screen and zoom the image; the arrows follow the page as the buttons
  do (in English, → is the next page; it was the previous); 0 resets the view.
- **Header.** Signed out, one "Sign in" button (its dialog has the Register tab). Signed in, the
  account icon alone: the name heads its menu and is the icon's tooltip, and the menu gains My
  Lists.
- New strings: 12, approved by the owner (the M-key hint is the owner's own wording).
