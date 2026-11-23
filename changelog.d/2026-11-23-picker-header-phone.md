## [Unreleased] -- Picker, header and sidebar on narrow screens: tooltips on the icons, the switch remembered, no status dot, the theme button, the sidebar beside the page (web, branch `claude/web-picker-header-phone`)

- The picker's Clear, Add to List and Add to Puzzle, shown as their icons alone on a narrow screen, have their names as tooltips.
- A picker with "Open more than one" (the header's, the home page's, Browse's) opens in the mode the reader last left it in.
- The header's server status dot is hidden on a phone (up to 640 px).
- The header's theme button shows from 370 px (it was hidden up to 640 px, with room for it).
- The sidebar pushes the page aside from 900 px (it covered the page up to 1023 px); narrower, its overlay starts under the header, whose menu button stays in sight.
- `web/components/shelfmark_tree_picker.py` (`MODE_KEY`), `web/static/common.css`, `web/main.py` (the sidebar's breakpoint); tests in `tests/render_smoke/test_shelf_picker_phone_render_smoke.py`, `tests/test_phone_layout_rules.py`.
