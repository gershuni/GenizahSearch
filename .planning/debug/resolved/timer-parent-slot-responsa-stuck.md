---
status: resolved
trigger: "NiceGUI ui.timer parent_slot RuntimeError spamming server logs. User cannot change search state from Responsa mode."
created: 2026-03-18T12:00:00Z
updated: 2026-03-18T13:00:00Z
resolved: 2026-03-18
---

## Current Focus

hypothesis: CONFIRMED — ui.timer elements in search.py become orphaned when user navigates between pages, causing RuntimeError spam from _get_context() accessing dead parent_slot weakrefs. The repeating progress timer and 13 one-shot timers all exhibited this issue.
test: Fix implemented and self-verified — all 14 ui.timer calls replaced with asyncio equivalents
expecting: No more parent_slot RuntimeErrors in server logs; search mode switching works normally
next_action: Awaiting human verification that the fix works in the user's real workflow

## Symptoms

expected: User should be able to switch search modes freely (e.g., from Responsa to regular search). No errors in server logs.
actual: Server logs show repeated "RuntimeError: The parent slot of the element has been deleted." from NiceGUI timer._get_context(). User reports they cannot change the search state from Responsa.
errors: RuntimeError at nicegui/timer.py -> nicegui/elements/timer.py -> nicegui/element.py - "The parent slot of the element has been deleted." Repeats multiple times per second.
reproduction: Go to web app, enter Responsa search mode, then try to change search state/mode.
started: Current server issue as of 2026-03-18

## Eliminated

- hypothesis: Mode change handler (on_mode_change) is broken for Responsa
  evidence: Handler at line 843-868 is straightforward — toggles set_visibility on UI elements. No complex logic or error handling that could swallow failures. No dependency on timer state.
  timestamp: 2026-03-18

- hypothesis: search_state.is_running gets stuck as True, preventing new searches
  evidence: is_running is set False at lines 3259, 3306, 3315, 3374 covering all exit paths (early return, error, cancel, success). But this only affects execute_search(), not mode_select changes.
  timestamp: 2026-03-18

## Evidence

- timestamp: 2026-03-18
  checked: All ui.timer usages in web/pages/search.py
  found: 14 ui.timer calls total. 1 repeating (line 2096: update_progress_ui, 0.5s). 13 once=True one-shot timers.
  implication: All become orphaned on page navigation, causing RuntimeError burst

- timestamp: 2026-03-18
  checked: NiceGUI timer source (venv timer.py + elements/timer.py)
  found: _run_in_loop line 90 enters parent_slot context once; _invoke_callback line 108 re-enters each tick. Both paths raise RuntimeError when parent_slot weakref is dead. Element-based _cleanup() also accesses parent_slot, producing second error per timer.
  implication: Each orphaned timer produces 2+ RuntimeErrors before dying

- timestamp: 2026-03-18
  checked: _puzzle_web_stderr.log for actual error traces
  found: Alternating errors from _run_once:76 and _run_in_loop:90 — confirms both one-shot and repeating timers are affected. Multiple occurrences in quick succession.
  implication: Errors occur during page navigation when old page's timers are orphaned

- timestamp: 2026-03-18
  checked: parallels.py lines 1998-2007 for existing fix pattern
  found: Already uses asyncio.ensure_future() with async loop instead of ui.timer, with explicit comment: "Use asyncio loop for progress updates instead of ui.timer to avoid parent slot RuntimeError on navigation"
  implication: This is a proven fix pattern already used elsewhere in the codebase

- timestamp: 2026-03-18
  checked: Whether repeating timer deactivation works properly
  found: search_state.update_timer.deactivate() at line 2095 only sets self.active=False, does NOT cancel asyncio task. Timer loop continues running, just skips callback. But the asyncio task still crashes when parent_slot is dead.
  implication: Even "deactivated" timers can cause parent_slot errors — need cancel() or asyncio pattern

- timestamp: 2026-03-18
  checked: NiceGUI version
  found: 3.8.0
  implication: Known issue in NiceGUI — timers are elements with parent_slot weakrefs that break on page navigation

- timestamp: 2026-03-18
  checked: Self-verification after fix
  found: Python syntax valid. Module imports OK. 868 tests pass (4 pre-existing failures unrelated to changes). All 14 ui.timer calls successfully replaced.
  implication: Fix is syntactically correct and introduces no regressions

## Resolution

root_cause: |
  All 14 ui.timer elements in search.py (1 repeating + 13 one-shot) become orphaned when the user
  navigates between pages. NiceGUI timers are UI elements with parent_slot weakrefs. When the page
  is rebuilt (navigation, language switch, reload), old page elements are destroyed but timer asyncio
  tasks survive. Each surviving timer tick calls _get_context() which accesses parent_slot — a weakref
  that now returns None — raising RuntimeError.

  The repeating progress timer (update_progress_ui, 0.5s interval) was particularly harmful because:
  (a) it ran continuously even when no search was active, and (b) deactivate() only sets active=False
  without canceling the asyncio task, so even "deactivated" timers still crashed on parent_slot access.

  The timer error spam could degrade server performance and WebSocket stability, potentially causing
  the "stuck in Responsa mode" symptom (mode restores from storage on every page reload).

fix: |
  Replaced all 14 ui.timer calls in search.py with asyncio equivalents:

  1. REPEATING TIMER (progress updates): Replaced ui.timer(0.5, update_progress_ui) with
     asyncio.ensure_future(_progress_update_loop()) — an async while loop that calls the
     update function and sleeps 0.5s, matching the proven pattern from parallels.py.
     The update function now returns True/False to signal loop continuation.
     Old timer stored in search_state.update_timer is properly canceled via .cancel().

  2. DEFERRED INIT TIMERS: Replaced ui.timer(delay, callback, once=True) with
     asyncio.ensure_future(_after_delay(delay, callback)). Added _after_delay() helper
     that does asyncio.sleep(delay) followed by await callback(), with exception handling
     for navigated-away pages.

  3. BUTTON CLICK HANDLERS: Replaced ui.timer(0, lambda: load_page(...), once=True) with
     asyncio.ensure_future(load_page(...)) — direct coroutine scheduling without creating
     a NiceGUI timer element.

verification: |
  Self-verified:
  - Python syntax check passes (ast.parse)
  - Module imports successfully (from web.pages.search import create_search_page)
  - 868 tests pass, 0 regressions (4 failures are pre-existing, unrelated)
  - Zero ui.timer calls remain in search.py (grep confirms)
  - asyncio already imported at line 32

files_changed: [web/pages/search.py]
