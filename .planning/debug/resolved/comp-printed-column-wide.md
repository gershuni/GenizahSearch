---
status: diagnosed
trigger: "Desktop composition results Printed column is too wide. Should be narrow, resizable, filterable."
created: 2026-03-01T12:00:00Z
updated: 2026-03-01T12:00:00Z
---

## Current Focus

hypothesis: Printed column is last column (index 7) and stretchLastSection(True) overrides its width
test: Confirmed by reading code -- setStretchLastSection(True) at lines 9068 and 9090
expecting: Column stretches to fill remaining space
next_action: Return diagnosis

## Symptoms

expected: Printed column should be narrow (fit "Print"/"דפוס"), resizable by user, and filterable (like search results table)
actual: Printed column is very wide (stretches to fill all remaining space), not user-resizable, not filterable
errors: None (visual/UX issue)
reproduction: Open desktop app, run composition search, observe Printed column width
started: Since Printed column was added to composition tree (GAP-9)

## Eliminated

(none needed -- root cause identified on first hypothesis)

## Evidence

- timestamp: 2026-03-01T12:01
  checked: Column index assignment (line 6533)
  found: comp_col_printed = 7, which is the LAST of 8 columns (0-7)
  implication: stretchLastSection affects this column

- timestamp: 2026-03-01T12:02
  checked: Header configuration (lines 9046-9090)
  found: |
    1. Line 9057: header.setSectionResizeMode(comp_col_printed, ResizeToContents)
    2. Line 9067: comp_tree.setColumnWidth(comp_col_printed, 60)
    3. Line 9068: header.setStretchLastSection(True) -- OVERRIDES both above
    4. Line 9089: comp_header.setSectionResizeMode(comp_col_printed, ResizeToContents) -- set again after CheckBoxHeader
    5. Line 9090: comp_header.setStretchLastSection(True) -- OVERRIDES again
  implication: stretchLastSection(True) forces the last column to fill all remaining horizontal space

- timestamp: 2026-03-01T12:03
  checked: Search table comparison (lines 8697-8733)
  found: |
    Search table COL_PRINTED = 11 with:
    - setColumnWidth(COL_PRINTED, 50) -- narrow width
    - setSectionResizeMode(COL_PRINTED, Fixed) -- not resizable, not stretching
    - COL_PRINTED is NOT the last column (Snippet at col 7 gets Stretch)
    - desc_first_cols includes COL_PRINTED for sort-descending-first behavior
  implication: Search table pattern works because Printed is not last column and uses Fixed mode

- timestamp: 2026-03-01T12:04
  checked: CheckBoxHeader filter_columns for comp tree (line 9077)
  found: filter_columns=[comp_col_library, comp_col_shelfmark, comp_col_title, comp_col_context, comp_col_ms_context] -- comp_col_printed NOT included
  implication: No filter icon or filter dialog for Printed column

- timestamp: 2026-03-01T12:05
  checked: CheckBoxHeader desc_first_cols for comp tree (line 9075-9079)
  found: No desc_first_cols parameter passed to comp CheckBoxHeader constructor
  implication: Printed column not sortable desc-first like in search table

- timestamp: 2026-03-01T12:06
  checked: on_comp_header_clicked handler (line 19947-19948)
  found: Only handles sections 0,1,2,3 -- section 7 (Printed) is ignored for sorting
  implication: Clicking Printed header does nothing useful

## Resolution

root_cause: |
  Three compounding issues make the Printed column too wide and lacking features:

  1. **stretchLastSection(True) overrides width** (PRIMARY): Printed is column 7, the last column.
     `setStretchLastSection(True)` at lines 9068 and 9090 forces it to absorb all remaining
     horizontal space, overriding both the explicit `setColumnWidth(60)` and `ResizeToContents` mode.

  2. **ResizeToContents prevents user resize**: The resize mode is set to `ResizeToContents`
     (lines 9057, 9089) which does not allow manual column resizing by the user.

  3. **Not in filter_columns**: `comp_col_printed` is missing from the `filter_columns` list
     passed to `CheckBoxHeader` (line 9077), so no filter icon is drawn and no filter dialog opens.

fix: (not applied -- diagnosis only)
verification: (not applied)
files_changed: []
