---
status: diagnosed
trigger: "When adding manuscripts from a personal list while in reading desk mode, items are added to text pane but image pane completely disappears"
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:01:00Z
---

## Current Focus

hypothesis: CONFIRMED - browse_set_lists_panel_visible calls setSizes with 3-element list on 4-widget splitter, collapsing image scroll to 0
test: Traced full code path from Add from List through splitter size management
expecting: N/A - root cause found
next_action: Return diagnosis

## Symptoms

expected: Adding manuscripts from list in reading desk mode should add items to both text pane AND image pane
actual: Text pane gets items but image pane completely disappears
errors: None reported (visual bug)
reproduction: Enter reading desk mode, use "Add from List" to add manuscripts
started: After plan 11-04 (Add from List in desktop reading desk)

## Eliminated

- hypothesis: QScrollArea is being recreated (destroying splitter child) instead of repopulated
  evidence: Line 7376 shows guard `if self._browse_rd_image_scroll is None` -- scroll area IS created once and repopulated correctly via setWidget at line 7884
  timestamp: 2026-02-08

## Evidence

- timestamp: 2026-02-08
  checked: browse_splitter widget composition
  found: Normal mode has 3 widgets [lists_panel(0), text_widget(1), browse_viewer(2)]. Reading desk adds 4th widget _browse_rd_image_scroll at index 3 (line 7380).
  implication: Splitter has 4 children during reading desk mode

- timestamp: 2026-02-08
  checked: browse_set_lists_panel_visible (lines 6924-6947)
  found: When showing lists panel, computes sizes as 3-element list (line 6939) and calls setSizes on splitter. When browse_lists_panel_sizes is cached, it is also 3 elements (saved at line 6942 from normal 3-widget mode).
  implication: setSizes(3-element-list) on 4-widget splitter gives 4th widget (image scroll) size 0

- timestamp: 2026-02-08
  checked: _browse_rd_render_images (lines 7757-7888)
  found: Sets image scroll visible (line 7885) but never manages splitter sizes. setVisible(True) on a QSplitter child with 0 allocated width does NOT restore its space.
  implication: Even after re-render, image pane stays at 0 width

- timestamp: 2026-02-08
  checked: Full Add from List flow
  found: _browse_rd_add_from_list (line 7579) -> browse_set_lists_panel_visible(True) (line 7581) -> setSizes crushes widget 3. Then user clicks item -> browse_on_list_item_clicked -> _browse_rd_add_entry -> _browse_rd_render -> _browse_rd_render_images. Images rendered but still 0 width.
  implication: The act of opening the lists panel is what kills the image pane, not the add operation itself

## Resolution

root_cause: browse_set_lists_panel_visible (line 6939) computes splitter sizes as a hardcoded 3-element list [lists, text, viewer]. During reading desk mode, the splitter has 4 widgets (lists_panel, text_widget, browse_viewer, rd_image_scroll). Calling setSizes with 3 elements on a 4-widget splitter collapses the 4th widget (rd_image_scroll) to 0 width. Neither _browse_rd_render nor _browse_rd_render_images ever recalculates splitter sizes.
fix:
verification:
files_changed: []
