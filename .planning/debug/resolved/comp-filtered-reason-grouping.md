---
status: diagnosed
trigger: "Desktop composition excluded section shows grouped reason counts in header BUT still repeats reason label on each item. User wants items grouped UNDER their reason header."
created: 2026-03-01T00:00:00Z
updated: 2026-03-01T00:00:00Z
---

## Current Focus

hypothesis: Filtered section adds reason as per-item prefix AND header summary, but lacks sub-grouping by reason
test: Code review of filtered section tree construction
expecting: Flat list of items under ROOT_FILT with [reason] prefix on each
next_action: Return diagnosis with restructuring plan

## Symptoms

expected: Items grouped under reason sub-headers (e.g., "Found in source text (5)" as collapsible group, items beneath without per-item reason labels)
actual: Header says "Filtered (8) -- Found in source text (5), High frequency (3)" and each item also shows "[Found in source text]" or "[High frequency]" prefix in title column
errors: none (UX issue, not a bug)
reproduction: Run any composition search that produces filtered results
started: Current behavior since GAP-7 implementation

## Eliminated

(none)

## Evidence

- timestamp: 2026-03-01
  checked: genizah_app.py lines 20457-20510 (filtered section construction)
  found: Header aggregates reason_counts into summary string. Then items are added flat under root_filt with per-item [reason] prefix on title column (lines 20487-20500). Appendix groups are added as sub-nodes but grouped by shelfmark signature, not by reason.
  implication: The tree structure is ROOT_FILT -> flat items + appendix groups. No intermediate reason-grouping layer exists.

- timestamp: 2026-03-01
  checked: genizah_app.py lines 20955-20979 (_collect_checked_comp_items_struct for ROOT_FILT)
  found: Collection logic iterates ROOT_FILT children directly, checking if child has UserRole data (direct item -> sel_filt) or no data (group node -> sel_filt_appx). This code assumes a flat structure of items + appendix groups under ROOT_FILT.
  implication: Restructuring filtered section to add reason sub-group nodes will require updating _collect_checked_comp_items_struct to handle the new intermediate level.

- timestamp: 2026-03-01
  checked: genizah_app.py lines 20541-20564 (_get_filter_reason)
  found: Returns human-readable reason string from item's pages or direct filter_reason field. Possible values: "Found in source text", "High frequency", "Filtered", or comma-joined combinations.
  implication: This function already provides the grouping key needed for sub-headers.

## Resolution

root_cause: The filtered section (lines 20457-20510) constructs a flat list of manuscript items directly under ROOT_FILT, with each item receiving a `[reason]` prefix on its title column (lines 20494-20500). The header aggregates reason counts into a summary string (lines 20460-20480), but there are no intermediate QTreeWidgetItem nodes acting as reason sub-groups. The architecture is: ROOT_FILT -> [item1, item2, ..., appendix_group1, appendix_group2]. User wants: ROOT_FILT -> [reason_group1 -> [items], reason_group2 -> [items], appendix under respective groups].

fix: (not applied -- diagnosis only)
verification: (not applicable)
files_changed: []
