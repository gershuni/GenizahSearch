# Quick Task 18: Summary

## What was fixed

### 1. Desktop: ResultDialog missing next/prev for filtered composition results
**File:** `genizah_app.py` — `on_comp_item_double_clicked`

**Root cause:** The tree traversal only descended 2 levels (category → child), but filtered results are nested 3 levels deep: `ROOT_FILT` → `reason_node` → `ms_node`. Reason nodes store data at `UserRole+100` (not `UserRole`), so `collect_node_data` returned immediately without descending.

**Fix:** Added recursive `traverse_tree()` helper that descends through all tree levels, calling `collect_node_data` on nodes with UserRole data and recursing into children of category/grouping nodes without it.

### 2. Web: "parent slot of the element has been deleted" RuntimeError
**File:** `web/pages/parallels.py`

**Root cause:** `ui.timer(0.05, update_ui)` creates a NiceGUI timer attached to the page's parent slot. When users navigate away (e.g., to /browse), the slot is deleted but the timer keeps firing, causing the RuntimeError in NiceGUI's timer infrastructure (before the callback's own error handling runs).

**Fix:** Replaced all `ui.timer()` calls with `asyncio` patterns:
- Repeating progress timer → `asyncio.ensure_future()` with `while True` + `asyncio.sleep(0.05)`
- Three one-shot init timers → `asyncio.ensure_future()` with `asyncio.sleep(delay)` + try/except

## Commit
`a0a8c9d2` — fix: composition search — ResultDialog nav for filtered results + parent_slot crash
