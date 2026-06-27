---
status: diagnosed
trigger: "RuntimeError: 'The parent slot of the element has been deleted' when changing per-fragment version selector in reading desk"
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:00:00Z
symptoms_prefilled: true
goal: find_root_cause_only
---

## Current Focus

hypothesis: CONFIRMED - ui.timer elements created inside content_container (by version_selector, notes_button, joins_button components) survive content_container.clear() as pending asyncio tasks, then crash when their parent_slot weakref is dead
test: Traced all ui.timer creation sites and content_container.clear() call sites; confirmed timers are not canceled on clear()
expecting: Expected NiceGUI element.clear() to cancel child timers; found it only marks them as _deleted, which is checked AFTER parent_slot access
next_action: Document findings and return diagnosis

## Symptoms

expected: Changing the per-fragment version selector dropdown in the reading desk should update the text display without errors
actual: RuntimeError "The parent slot of the element has been deleted" logged multiple times
errors: RuntimeError at nicegui/timer.py:76 -> nicegui/elements/timer.py:12 -> nicegui/element.py:148
reproduction: Change version selector dropdown in reading desk; also reproducible by navigating pages quickly in single-page view
started: Likely since ui.timer elements were introduced in components (version_selector.py, notes_display.py, joins_panel.py)

## Eliminated

- hypothesis: Reading desk on_change handler creates orphaned timers
  evidence: The on_change handler (browse.py:2693-2741) only calls container.clear() on text containers, creates no timers
  timestamp: 2026-02-08

- hypothesis: NiceGUI internal storage timers cause the error
  evidence: No timers found in NiceGUI storage module; app.storage.user uses file-based persistence without timers
  timestamp: 2026-02-08

- hypothesis: Header heartbeat timer (main.py:1543) is affected by content clearing
  evidence: Header timers are in a separate element tree (ui.header) from content_container; clear() only affects descendants
  timestamp: 2026-02-08

## Evidence

- timestamp: 2026-02-08
  checked: All ui.timer creation sites in web/ directory
  found: 3 timers created inside content_container during single-page view rendering: version_selector.py:185, notes_display.py:416, joins_panel.py:291
  implication: These timers become children of content_container's subtree

- timestamp: 2026-02-08
  checked: NiceGUI element.py clear() method (line 455-461)
  found: clear() calls client.remove_elements(self.descendants()) which sets _deleted=True but does NOT cancel asyncio tasks for timers
  implication: Timer asyncio tasks remain pending in event loop after parent deletion

- timestamp: 2026-02-08
  checked: NiceGUI timer.py _run_once() method (line 72-82)
  found: _run_once accesses self.parent_slot via _get_context() BEFORE checking self.is_deleted via _should_stop(). parent_slot uses weakref that returns None when parent is GC'd.
  implication: Race condition - timer task tries to access parent_slot before checking if it was deleted

- timestamp: 2026-02-08
  checked: NiceGUI element.py parent_slot property (line 142-149)
  found: Uses weakref.ref; raises RuntimeError when weakref returns None
  implication: The exact error source - dead weakref triggers the RuntimeError

- timestamp: 2026-02-08
  checked: browse.py update_content() (line 1647-1649)
  found: Calls content_container.clear() which destroys all child elements including timer parents
  implication: Any pending timer tasks from previous render cycle will fail

- timestamp: 2026-02-08
  checked: browse.py load_page() (line 838-963)
  found: Calls update_content() TWICE - once at line 848 (loading state) and once at line 963 (results). First call destroys previous render's timers.
  implication: If user navigates before timers complete (0.1s window), orphaned timers crash

- timestamp: 2026-02-08
  checked: NiceGUI GitHub issues
  found: Issue #1500 "Timers keep running when context they are declared in is deleted", Issue #1710 "Convert ui.timer into an element", Issue #3187 "container.clear silently deletes timers"
  implication: Known NiceGUI limitation - timers are not properly canceled when parent elements are deleted

- timestamp: 2026-02-08
  checked: NiceGUI elements/timer.py _cleanup() method (line 35-40)
  found: _cleanup also accesses self.parent_slot; called in finally block of _run_once, producing a SECOND error per timer
  implication: Each orphaned timer produces 2 RuntimeErrors, explaining "repeats multiple times"

## Resolution

root_cause: |
  The RuntimeError is caused by NiceGUI ui.timer elements whose parent containers are destroyed
  before their asyncio tasks complete. The mechanism is:

  1. When content_container renders the single-page view, three components create ui.timer(0.1s, once=True):
     - version_selector.py:185 (load_and_apply_latest)
     - notes_display.py:416 (check_comments)
     - joins_panel.py:291 (load_count)

  2. These timers become child elements of content_container's subtree.

  3. When content_container.clear() is called (by update_content(), which is called by
     load_page(), enter_joined_view(), exit_joined_view(), navigate_shelfmark(), etc.),
     it sets _deleted=True on timer elements but does NOT cancel their pending asyncio tasks.

  4. When the timer's asyncio task resumes, _run_once() calls _get_context() which accesses
     self.parent_slot. The parent_slot is a weakref that returns None (parent was GC'd).
     This raises RuntimeError at element.py:148.

  5. The _cleanup() method in the finally block ALSO accesses parent_slot, producing a second
     RuntimeError per timer. With 3 timers, this produces up to 6 error messages.

  The error is most easily triggered when:
  a) User navigates pages quickly (within 0.1s of page render)
  b) User enters/exits reading desk mode shortly after page load
  c) User switches language (causes full page re-render while timers pending)

  In the specific "reading desk" scenario: the user likely loaded a manuscript (creating timers),
  then quickly entered reading desk mode. Or the error messages from a slightly earlier
  event appear in the console during the reading desk interaction.

fix: (not applied - diagnosis only)
verification: (not verified - diagnosis only)
files_changed: []
