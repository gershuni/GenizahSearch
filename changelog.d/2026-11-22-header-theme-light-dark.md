## [Unreleased] -- The header's theme button switches between light and dark (web, branch `claude/header-theme-two-modes-c78a31`)

### Changed
- **The theme button in the website's header now switches between light and dark only.** It
  went light -> parchment -> dark, so dark took two clicks. Parchment counts as a light theme, so a
  click from it goes to dark. The icon and tooltip say what the next click does ("Switch to the
  dark theme" / "Switch to the light theme", in Hebrew too). Parchment is still chosen in Settings
  and in the sidebar's theme switcher, and a saved parchment theme keeps working
  (`web/header_theme.py`; the sidebar and Settings keep the button in step through
  `sync_header_theme_button`).

### Tests
- `tests/render_smoke/test_header_theme_button_render_smoke.py`: light -> dark -> light and
  parchment -> dark, with the icon, tooltip and aria-label, in English and Hebrew. All three fail
  with the old three-step cycle.
