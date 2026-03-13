---
title: "Fix composition search ResultDialog parent_slot error and missing next/prev for filtered results"
mode: quick
tasks: 2
---

# Quick Task 18: Fix composition search ResultDialog + parent_slot error

## Task 1: Fix desktop ResultDialog traversal for filtered results

**Files:** `genizah_app.py`
**Action:** Fix `on_comp_item_double_clicked` (line ~25973) to traverse filtered result nodes properly. Currently the tree traversal goes only 2 levels (category → sub), but filtered results are nested 3 levels deep: `ROOT_FILT` → `reason_node` → `ms_node`. Reason nodes store data at `UserRole+100`, not `UserRole`, so `collect_node_data` returns immediately without descending into manuscript nodes.

**Fix:** Make the traversal recursive — add a helper that descends through all levels of the tree, calling `collect_node_data` on each node regardless of depth.

**Verify:** Review that flat_list correctly collects items from all tree sections (main, appendix, filtered, known).
**Done:** Filtered composition results open in ResultDialog with proper next/prev navigation across all result sections.

## Task 2: Fix web parallels page timer parent_slot error

**Files:** `web/pages/parallels.py`
**Action:** Replace `ui.timer(0.05, update_ui)` at line ~1997 with an asyncio-based loop that doesn't attach to a parent slot. This prevents the "parent slot of the element has been deleted" RuntimeError when users navigate away from the parallels page.

**Pattern:** Use `asyncio.get_event_loop().call_later()` or an `async` loop with `asyncio.sleep()` instead of `ui.timer()`. The timer should self-cancel when the client is deleted (detected by the existing RuntimeError check in `update_ui`).

**Verify:** No `ui.timer` calls remain for the progress update loop. One-shot init timers (lines 433, 3663, 3680) are lower risk but should also be converted if straightforward.
**Done:** No parent_slot RuntimeError when navigating away from parallels page during or after a search.
