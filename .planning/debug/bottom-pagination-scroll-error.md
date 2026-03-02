---
status: resolved
trigger: "Bottom pagination's scroll-to-top throws RuntimeError when parent slot is deleted"
created: 2026-02-19T00:00:00Z
updated: 2026-02-19T00:00:00Z
---

## Current Focus

hypothesis: ui.run_javascript called after render_results destroys the bottom pagination's parent slot
test: compare top vs bottom pagination handlers
expecting: top handler has no scrollTo; bottom handler calls scrollTo AFTER render_results destroys its own DOM
next_action: confirm and recommend fix

## Symptoms

expected: Clicking bottom pagination changes page and scrolls to top
actual: RuntimeError - parent element slot deleted
errors: "RuntimeError: The parent element this slot belongs to has been deleted." at line 2268
reproduction: Click bottom pagination control on search results page
started: Unknown - likely since bottom pagination added

## Eliminated

(none needed - root cause found on first hypothesis)

## Evidence

- timestamp: 2026-02-19
  checked: render_results function (line 2211-2270)
  found: render_results() calls results_container.clear() at line 2212, which destroys ALL children including the bottom pagination row that contains on_page_change_bottom's closure context
  implication: After render_results returns, the ui.run_javascript call on line 2268 executes inside a handler whose parent NiceGUI element has been destroyed by results_container.clear()

- timestamp: 2026-02-19
  checked: top pagination handler (line 2250-2252) vs bottom pagination handler (line 2264-2268)
  found: Top handler calls render_results then RETURNS. Bottom handler calls render_results then tries ui.run_javascript AFTER. The top handler works because it returns immediately; the bottom handler fails because it continues executing after its own DOM is destroyed.
  implication: The fix is to move the scrollTo call BEFORE render_results, or wrap it in try/except, or use asyncio.call_later as noted in MEMORY.md

## Resolution

root_cause: Execution order bug. on_page_change_bottom calls render_results() on line 2266, which calls results_container.clear() on line 2212. This destroys ALL child elements of results_container, including the bottom pagination ui.row() that is the parent slot of the currently-executing handler. When execution returns to line 2268, ui.run_javascript('window.scrollTo(0, 0)') fails because the NiceGUI slot context for this handler's parent element no longer exists.
fix: Move ui.run_javascript before render_results, or use asyncio.call_later to schedule it outside the slot context
verification: pending
files_changed: [web/pages/search.py]
