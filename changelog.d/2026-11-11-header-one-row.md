## [Unreleased] -- The header stays one row at every width, and its search box stays while it fits (web, branch `claude/header-one-row`)

From 641px until the header's groups fit (about 700px, in Hebrew and English; a browser zoomed to
200% on a 1360px window is 680px wide) the three header groups were wrap rows: each wrapped inside
itself, the menu over the logo and "?" on a second line, and the second lines hung over the page
under the 64px bar (the owner's screenshot, 2026-11-11). Only the phone block (640px and under)
made them no-wrap. And the go-to box gave way to the search button at a fixed 1023px, and on a
phone always, with room to spare (owner, same day: narrow the box while there is room).

- `web/main.py`: the header groups carry `header-left` / `header-right` (the logo link keeps its
  `header-logo-link`).
- `web/static/common.css` (`@media not all and (max-width: 640px)`, the phone block's exact
  complement, so a fractional width under browser zoom such as 640.5px is covered too): no header
  group wraps and the button groups keep their width; should the bar be short of room where the
  logo text shows, the logo link wraps the text onto a second line its 28px cap (the text's own
  height) hides, so the icon stays. The clip is the link's own overflow, which never cuts its own
  focus ring. Spacing where the text shows is unchanged.
- The go-to box: the search group takes the bar's free space and is a size container
  (`container: header-search / inline-size`); `@container header-search (min-width: 150px)` shows
  the field, at today's width (24vw, 220-320px) or the room if less, with an ellipsis on a cut
  placeholder. With less room it is the search button, whose field opens across the whole bar as
  before. Below 826px the logo text gives its room to the box (`@media (max-width: 825px)`; the
  widest header measured, Hebrew signed out with What's New, no longer fits both). Signed out, in
  both languages, the field now shows from 440px (it was 1024px), at today's widths from 1024px;
  below 440px it is the button. Open, the search group itself covers the bar and the field fills
  it (the field was the placed element before; in browsers that gave a size container layout
  containment it would have opened inside the narrow group -- Codex, round 3); a bar widened
  while open stays open until closed; "/" opens or focuses it. This also removes the 1023-1024px gap where a fractional width
  showed both the field and the button, and the 640.5px gap where Sign in showed its icon and its
  label (`.phone-icon-only`, now the phone block's exact complement too). No text changes.
- Measured in the browser at 320-1920px wide and 350-1000px high, both languages, on /browse, /,
  /search, /help and /joins-lab: one row, nothing outside the bar or overlapping; the logo still
  leads home.
- `tests/test_phone_layout_rules.py`: `test_the_header_is_one_row_above_640px` and
  `test_the_header_search_box_narrows_then_folds_behind_a_button` (replacing
  `..._folds_behind_a_button_below_1024px`) pin the hooks and the rules; both fail on the old
  sources.
