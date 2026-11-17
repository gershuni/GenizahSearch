## [Unreleased] -- The on-screen Hebrew keyboard in every search box (web, branch `claude/hebrew-keyboard-home-and-header`)

The keyboard button that opens the Hebrew keys is now in the homepage's search box and in the
header's go-to box too, not only on the search page (owner, 2026-11-16). The homepage's keys open
under its box; the header's open over the page under its box (`.gs-hk-floating`). One helper adds
the button and the panel to a box (`web/hebrew_keyboard.py::attach`).

- Each box carries its own field (`main_search`, `home_search`, `header_search`) as the script's
  target, and its panel the same field: the script types a key into its panel's box, so on the
  search page, where the header's box comes first, the main box's keys still type in the main box.
  PostHog gets the field with both events.
- The script and styles come once per page, from the layout every page with a header uses, and the
  script refuses to run twice (a second copy would type each key twice).
