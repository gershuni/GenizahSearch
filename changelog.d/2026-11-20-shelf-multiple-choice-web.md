## [Unreleased] -- Several at once from the shelf, and Exclude from the shelf (web, branch `claude/shelf-picker-web-multi`)

### Added
- **Exclude manuscripts from the shelf** (`/search`, "Exclude known manuscripts" > "Pick from the
  shelf"): tick whole volumes, whole collections or single shelfmarks, in any library, and exclude
  them all at once. A whole volume is one choice ("T-S AS 39 · 120"); a shelfmark unticked inside
  it is left out of it. The choice becomes one exclusion, named after what was chosen, beside the
  pasted, listed and uploaded ones.
- **The shelf picker's multiple choice** (`web/components/shelfmark_tree_picker.py`,
  `shared/shelfmark_tree_selection.py`): a box on every shelfmark and every volume or collection,
  "Select all in ..." over the shelfmarks shown, the choices as removable chips above the columns
  ("+N more" when they do not fit), each named with its library's code ("CUL T-S AS 39"), the number
  of manuscripts ticked (each counted once), Clear, and "Save as a list". A choice not yet applied
  is kept for the reader and its purpose across closing the picker and reloading the page, until it
  is applied or cleared.
