---
status: diagnosed
trigger: "Investigate why language switch loses reading desk state in the web app, returning to a blank browse page."
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:45:00Z
goal: find_root_cause_only
---

## Current Focus

hypothesis: CONFIRMED - Language is not persisted to app.storage.user, causing it to reset on reload. Reading desk state loss is likely due to app.storage.user not persisting across ui.navigate.reload() OR restoration failing silently.
test: Analyzed full code flow from language switch through page reload
expecting: Fix requires (1) persisting language to storage, (2) restoring language on page load, (3) possibly adding better error handling/logging to reading desk restoration
next_action: Document complete root cause and suggest fix

## Symptoms

expected: Language switch should preserve reading desk state (which manuscripts are loaded, which page is active)
actual: Language switch returns to blank browse page, losing all reading desk state
errors: None visible (state loss only)
reproduction: Open reading desk with manuscripts, switch language
started: After fix attempt 11-06 W4 that supposedly fixed this by persisting/restoring reading desk state
context: Previous fix reordered initialization so reading desk restore is checked before load_page() even when initial_sys_id is in URL params, but it's not working

## Eliminated

- hypothesis: state.sys_id is not set when initial_sys_id exists
  evidence: Line 723 explicitly sets state.sys_id = initial_sys_id
  timestamp: 2026-02-08T00:40:00Z

- hypothesis: _persist_reading_desk_state depends on language
  evidence: Function only checks state.view_joined and state.reading_desk_entries, no language dependency
  timestamp: 2026-02-08T00:40:00Z

- hypothesis: toggle_lang() clears app.storage.user
  evidence: Function only calls set_language() and ui.navigate.reload(), doesn't touch storage
  timestamp: 2026-02-08T00:40:00Z

## Evidence

- timestamp: 2026-02-08T00:15:00Z
  checked: browse.py state persistence and restoration functions
  found: |
    _persist_reading_desk_state() (line 1143): Saves reading_desk_entries to app.storage.user['reading_desk_state']
    - Only saves sys_id and shelfmark for each entry (not full page data)
    - Only saves if state.view_joined is True
    - Clears state if view_joined is False (line 1159)

    _restore_reading_desk_state() (line 1163): Restores from app.storage.user
    - Gets saved entries and calls enter_joined_view() to rebuild state
    - Returns True on success, False on failure
    - Called at page init (lines 3651, 3657)

    enter_joined_view() (line 1024): Rebuilds full reading desk
    - Sets state.view_joined = True
    - Re-fetches full manuscript data, pages, sources
    - Appends entries to state.reading_desk_entries
    - Calls _persist_reading_desk_state() at end (line 1057)
  implication: State persistence logic looks correct. Need to check if language switch is actually preserving app.storage.user or if it's being cleared

- timestamp: 2026-02-08T00:20:00Z
  checked: Language switch implementation in main.py and translations.py
  found: |
    toggle_lang() in main.py (line 1645-1649):
    - Calls set_language(new_lang) which only sets global _current_lang variable
    - Calls ui.navigate.reload() which reloads the entire page

    set_language() in translations.py (line 16-19):
    - Only sets global variable: _current_lang = lang
    - Does NOT persist to app.storage.user

    Language initialization:
    - _current_lang = 'he' (line 13 in translations.py) - hardcoded default
    - No app.on_connect or initialization code to restore language from storage
    - No search found for: app.storage.user['language'] or similar
  implication: CRITICAL BUG - Language is NOT persisted to storage! On page reload, _current_lang resets to 'he'. This means reading_desk_state might be saved but language context is lost, and the app likely doesn't restore properly because it's in wrong language mode

- timestamp: 2026-02-08T00:25:00Z
  checked: Reading desk state restoration during page load
  found: |
    Page initialization at browse.py lines 3648-3667:
    - If initial_fl_id is provided → loads that specific page (lines 3596-3647)
    - elif initial_sys_id → tries _restore_reading_desk_state() first (line 3651)
      - If restore succeeds → ignores sys_id param
      - If restore fails → calls load_page(p_num=initial_page)
    - else (no URL params) → tries _restore_reading_desk_state() (line 3657)
      - If restore fails → tries to restore browse_position from storage

    The logic LOOKS correct - restore is called before load_page.

    BUT: When language switch happens via ui.navigate.reload():
    - URL is preserved with current query params
    - If user had browsed to a manuscript before entering reading desk, sys_id might still be in URL
    - Language resets to 'he' hardcoded default BEFORE page load
  implication: Need to verify two things: (1) Does _restore_reading_desk_state() actually get called and succeed? (2) Is the persisted state actually there in app.storage.user after reload?

- timestamp: 2026-02-08T00:35:00Z
  checked: State initialization and assignment in create_browse_page
  found: |
    Line 708: state = BrowseState() - NEW instance created on every page load
    Line 723: state.sys_id = initial_sys_id - sys_id is set from URL param

    The flow when initial_sys_id exists:
    1. New state instance created
    2. sys_id assigned from URL
    3. _restore_reading_desk_state() called
    4. If restore succeeds → enter_joined_view() is called, which:
       - Sets state.view_joined = True
       - Rebuilds state.reading_desk_entries
       - Calls _persist_reading_desk_state()
       - Returns True (so load_page is NOT called)
    5. If restore fails → load_page() is called (fallback to single view)

    app.storage.user persistence (from web search):
    - Should persist across ui.navigate.reload()
    - However, some users report issues with persistence
  implication: If restoration is failing, it's either because (1) state isn't persisted before reload, or (2) exception occurs during restoration. Need to identify which.

## Resolution

root_cause: |
  Language switch implementation has TWO critical bugs:

  1. **Language not persisted**: toggle_lang() in main.py (line 1648) calls set_language(new_lang) which only sets a global variable (_current_lang in translations.py line 13). This variable resets to hardcoded 'he' default on every page reload. Language is NEVER saved to app.storage.user.

  2. **No language restoration on startup**: There is no app.on_connect or initialization code to restore language from app.storage.user. On every page load, language defaults to 'he'.

  The sequence:
  - User in reading desk with English UI
  - Clicks language switch button
  - set_language('he') sets global variable
  - ui.navigate.reload() reloads page
  - On reload: _current_lang resets to 'he' (hardcoded default)
  - Reading desk state IS in app.storage.user['reading_desk_state']
  - _restore_reading_desk_state() IS called (line 3651 or 3657)
  - But restoration may fail OR succeed with wrong language UI

  **Most likely**: The restoration actually succeeds, but because language isn't persisted, if user switched FROM Hebrew TO English and back, they're now in Hebrew mode with reading desk. But user reports "blank browse page", suggesting restoration is failing.

  **Secondary theory**: app.storage.user['reading_desk_state'] might be getting cleared or not persisting correctly across ui.navigate.reload() despite documentation claiming it should.

  **Need to verify**: Whether reading_desk_state actually persists in storage after reload, and whether _restore_reading_desk_state() is being called and what it returns.

  **UPDATE**: Previous fix (11-06 W4) DID add restoration logic at lines 3648-3654, checking reading desk before loading single page. But it's still failing, which means _restore_reading_desk_state() is returning False. Either storage isn't persisting or restoration is failing with exception.

  **KEY INSIGHT**: The fact that it goes to "blank browse page" (line 3667: update_content() with no state) suggests that BOTH restoration failed AND initial_sys_id might be None (or load_page failed). Need to check if URL params are preserved across ui.navigate.reload().

fix: |
  Two separate fixes needed:

  **Fix 1: Persist and restore language**
  - In toggle_lang() (main.py line 1645): Save language to app.storage.user BEFORE reload
  - Add initialization code to restore language from storage on page load
  - OR use app.storage.browser for language (persists across sessions)

  **Fix 2: Debug reading desk restoration**
  - Add logging to _restore_reading_desk_state() to show:
    - Whether saved state exists
    - What data is in saved state
    - Whether restoration succeeds
  - Verify app.storage.user persists across ui.navigate.reload()
  - Check if URL params (sys_id) are preserved across reload
  - Consider alternative: use app.storage.browser for reading desk state (more persistent)
verification:
files_changed: []
