---
status: diagnosed
trigger: "Reading desk state lost on language switch despite 11-09 plan claiming fix via app.storage.user persistence"
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - update_content() early-returns when state.current_page is None, preventing reading desk rendering on restoration
test: Traced full code path from toggle_lang -> reload -> create_browse_page -> _restore_reading_desk_state -> enter_joined_view -> update_content
expecting: Found the exact guard condition that blocks rendering
next_action: Return diagnosis

## Symptoms

expected: Language switch in reading desk mode preserves reading desk state (open manuscripts, scroll positions)
actual: Reading desk state is completely lost on language switch - shows welcome prompt instead
errors: None reported (silent state loss)
reproduction: Open reading desk, add manuscripts, switch language
started: After 11-09 plan was supposedly implemented

## Eliminated

- hypothesis: Persistence code is missing
  evidence: _persist_reading_desk_state() and _restore_reading_desk_state() are fully implemented (lines 1143-1183)
  timestamp: 2026-02-08

- hypothesis: Language switch doesn't trigger reload
  evidence: toggle_lang() at main.py:1661 calls ui.navigate.reload() correctly
  timestamp: 2026-02-08

- hypothesis: Storage data is lost during reload
  evidence: app.storage.user persists across reloads; create_browse_page reads it at line 3700
  timestamp: 2026-02-08

- hypothesis: Restoration logic never fires
  evidence: Line 3724-3726 correctly attempts _restore_reading_desk_state() when no sys_id in URL
  timestamp: 2026-02-08

## Evidence

- timestamp: 2026-02-08
  checked: browse.py update_content() flow (line 1647+)
  found: Line 1665 checks "if not state.current_page:" and returns early with welcome prompt, BEFORE the "elif state.view_joined:" branch at line 2198
  implication: Reading desk rendering is unreachable when state.current_page is None

- timestamp: 2026-02-08
  checked: enter_joined_view() (line 1024-1058)
  found: Function sets state.view_joined, state.reading_desk_entries, etc. but NEVER sets state.current_page
  implication: After restoration, state.current_page remains None (from fresh BrowseState at line 708)

- timestamp: 2026-02-08
  checked: Normal reading desk entry flow via add_to_reading_desk() (line 1074)
  found: Requires state.current_page to exist (line 1076 check). User is already browsing, so state.current_page is set before entering joined view
  implication: In normal flow, state.current_page is pre-existing and truthy. Bug only manifests on restoration path.

- timestamp: 2026-02-08
  checked: Language switch flow: toggle_lang (main.py:1653) -> set_language -> ui.navigate.reload -> browse_page_route -> create_browse_page(initial_sys_id=None)
  found: Reload creates fresh BrowseState() at line 708 with all fields as None/False/empty
  implication: All in-memory state is lost; only app.storage.user data survives

## Resolution

root_cause: update_content() in browse.py has a guard at line 1665 that returns early when state.current_page is None, showing the welcome prompt. The reading desk rendering code (elif state.view_joined at line 2198) is unreachable in this case. During language-switch restoration, enter_joined_view() correctly restores state.view_joined=True and state.reading_desk_entries, but never sets state.current_page. Since BrowseState() is freshly created on reload (line 708), state.current_page is None, triggering the early return.

fix: The guard at line 1665 needs to also check state.view_joined. Change from "if not state.current_page: return" to "if not state.current_page and not state.view_joined: return". This allows the code to fall through to the "elif state.view_joined:" branch at line 2198 when the reading desk is being restored.

verification:
files_changed: []
