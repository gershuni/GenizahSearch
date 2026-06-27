---
status: diagnosed
trigger: "web app crashes with Connection lost when Responsa search returns large result set (42K hits)"
created: 2026-02-10T00:00:00Z
updated: 2026-02-10T00:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - Multiple compounding issues cause WebSocket overload
test: Code analysis of full data flow
expecting: N/A - root cause confirmed
next_action: Report diagnosis

## Symptoms

expected: Responsa search for `#הולך` returns results and displays them normally
actual: Search processes correctly (42,213 Tantivy hits, 18,858 after dedup) but rendering causes "Connection lost". Console error: "The client this element belongs to has been deleted" on results_container.clear(). Search toolbar also disappears after some Responsa searches.
errors: "Connection lost", "The client this element belongs to has been deleted" on results_container.clear()
reproduction: Search for `#הולך` in Responsa mode on web app
started: Unknown - likely since Responsa mode was added

## Eliminated

## Evidence

- timestamp: 2026-02-10
  checked: Line 1719 of web/pages/search.py
  found: `app.storage.user['search_results'] = results` stores ALL 18,858 results (with full_text) into NiceGUI per-user storage. This triggers JSON serialization over the WebSocket of potentially 50-100+ MB of data.
  implication: PRIMARY CAUSE. NiceGUI app.storage.user is persisted to disk AND synced to browser via WebSocket. Serializing 18,858 results each with full manuscript text is catastrophic.

- timestamp: 2026-02-10
  checked: Lines 1698-1706 of web/pages/search.py
  found: `get_sys_ids_with_transcriptions(result_sys_ids)` is called with all 18,858 sys_ids BEFORE any limit is applied. This makes 94 Supabase API calls (18858/200 chunks) sequentially.
  implication: CONTRIBUTING FACTOR. Long blocking I/O before rendering begins, adding to total time before UI updates.

- timestamp: 2026-02-10
  checked: Lines 1761-1783 of web/pages/search.py (render_results + create_result_card)
  found: Each result card creates ~15-18 NiceGUI elements (card, rows, labels, buttons, checkbox, expansion). 200 results = ~3,000-3,600 DOM elements created in a single synchronous batch.
  implication: CONTRIBUTING FACTOR. Large batch DOM creation blocks the WebSocket event loop.

- timestamp: 2026-02-10
  checked: Lines 868, 879 of web/pages/search.py (apply_filters, clear_filters)
  found: `render_results(search_state.results)` and `render_results(search_state.results)` call render without the [:200] limit. If search_state.results has 18,858 items, filters/clear_filters would try to render ALL of them.
  implication: SECONDARY BUG. After a large Responsa search, using filters would crash because no limit is applied.

- timestamp: 2026-02-10
  checked: Lines 736-824 of web/pages/search.py (scroll auto-collapse)
  found: JavaScript scroll handler collapses expanded_panel by setting display:none on scroll down. This is CSS-only manipulation that survives WebSocket disconnect. If connection is lost while panel is collapsed, it stays hidden. The JS manipulates DOM directly - Python state (search_state.is_panel_collapsed) is NOT updated.
  implication: TOOLBAR DISAPPEARING is the scroll auto-collapse JS firing during/after the connection instability, OR from a previous page load where it was collapsed. Not a separate bug - it's a consequence of the connection loss.

## Resolution

root_cause: |
  PRIMARY: Line 1719 `app.storage.user['search_results'] = results` serializes ALL 18,858 results
  (each containing full_text with entire manuscript page content) into NiceGUI per-user storage.
  NiceGUI's app.storage.user is backed by server-side JSON files AND synced to the browser.
  With 18,858 results x ~2-5KB each = 37-94 MB of JSON serialization over WebSocket. This
  overwhelms the WebSocket connection, causing "Connection lost" and the subsequent
  "client this element belongs to has been deleted" errors.

  CONTRIBUTING: get_sys_ids_with_transcriptions() makes ~94 sequential Supabase API calls
  (18858 IDs / 200 chunk size) before rendering begins, adding seconds of delay.

  CONTRIBUTING: 200 result cards x 15-18 DOM elements each = ~3,600 elements created
  synchronously, further stressing the event loop.

  SECONDARY: apply_filters() and clear_filters() don't apply [:200] limit, so after
  any large search, using filters would also crash.
fix:
verification:
files_changed: []
