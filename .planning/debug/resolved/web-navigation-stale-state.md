---
status: resolved
trigger: "Investigate why navigating from other pages to browse when reading desk was previously active shows blank page or stale reading desk."
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:50:00Z
---

## Current Focus

hypothesis: CONFIRMED - Reading desk state restoration takes priority over URL sys_id parameter during initialization
test: COMPLETE - Full understanding of root cause and fix strategy
expecting: Fix will distinguish language-switch from cross-page navigation by checking if initial_sys_id is in persisted reading desk entries
next_action: report findings to user

## Symptoms

expected: When navigating from another page (e.g., clicking a manuscript in a list) to /browse?sys_id=X, should show that specific manuscript in normal page view
actual: Shows blank browse page OR shows previous Reading Desk with old manuscripts instead of the new sys_id
errors: RuntimeError 'parent element this slot belongs to has been deleted' in toolbar_add_by_shelfmark at browse.py:2236 (stale UI element references)
reproduction:
1. View a manuscript in Reading Desk mode (multiple manuscripts)
2. Navigate to another page (e.g., Lists)
3. Click a manuscript in a list to navigate to /browse?sys_id=X
4. Result: blank page or stale reading desk
5. Workaround: Must click "Back to Page View" first
started: Unknown (recently reported)

## Eliminated

## Evidence

- timestamp: 2026-02-08T00:15:00Z
  checked: browse.py lines 3648-3668 (initialization flow)
  found: When initial_sys_id is provided via URL param, code FIRST checks if reading desk state exists in app.storage.user (line 3651), and if so, restores reading desk IGNORING the sys_id param
  implication: Navigation with ?sys_id=X is silently overridden by persisted reading desk state

- timestamp: 2026-02-08T00:20:00Z
  checked: browse.py lines 1163-1181 (_restore_reading_desk_state function)
  found: Function reads 'reading_desk_state' from app.storage.user and if present, calls enter_joined_view to restore reading desk
  implication: Reading desk state persists across ALL page navigations, not just language switches (despite comment saying "after language switch")

- timestamp: 2026-02-08T00:25:00Z
  checked: browse.py lines 1060-1072 (exit_joined_view function)
  found: exit_joined_view properly clears app.storage.user['reading_desk_state'], BUT this is only called when user explicitly clicks "Back to Page View" button
  implication: Navigating away from browse page does NOT call exit_joined_view, so state persists

- timestamp: 2026-02-08T00:30:00Z
  checked: browse.py lines 1143-1159 (_persist_reading_desk_state function)
  found: Function saves reading desk state whenever manuscripts are added. Comment says "for language-switch persistence" but there's no mechanism to distinguish language-switch navigation from other navigation
  implication: The persistence feature designed for language-switch accidentally persists across ALL navigations

- timestamp: 2026-02-08T00:35:00Z
  checked: browse.py line 2236 (RuntimeError location)
  found: _add_sys_id_to_reading_desk is called from toolbar_add_by_shelfmark, which then calls update_content()
  implication: If user navigates away mid-operation, UI elements are deleted but callbacks still reference them, causing RuntimeError

- timestamp: 2026-02-08T00:40:00Z
  checked: main.py lines 1645-1649 (language switch mechanism)
  found: Language switch calls set_language(new_lang) then ui.navigate.reload() - reloads current page with same URL
  implication: Language switch preserves URL parameters (sys_id), so both URL and persisted state should have same sys_ids. Cross-page navigation has DIFFERENT sys_id in URL

## Resolution

root_cause: Reading desk state persistence was designed for language-switch preservation (lines 3649-3652) but persists across ALL navigations. When user navigates from browse to another page and then back with ?sys_id=X, the initialization code prioritizes restoring persisted reading desk state over the URL parameter (line 3651-3652), causing the new sys_id to be ignored. The reading desk state is only cleared when user explicitly clicks "Back to Page View" (exit_joined_view), not when navigating away from browse page.

Language switch: ui.navigate.reload() - reloads with SAME URL (should restore reading desk)
Cross-page navigation: ui.navigate.to('/browse?sys_id=X') - NEW URL with different sys_id (should NOT restore reading desk)

fix: Modify lines 3651-3654 in browse.py initialization block. Before calling _restore_reading_desk_state(), check:
1. If initial_sys_id is provided (cross-page navigation with explicit sys_id)
2. Extract sys_ids from persisted reading_desk_state entries
3. If initial_sys_id is NOT in the persisted sys_ids list, clear the persisted state (user navigated to a different manuscript)
4. Only restore reading desk if: (a) no initial_sys_id provided, OR (b) initial_sys_id matches one of the persisted sys_ids (language switch case)

This preserves reading desk across language switch (same sys_ids) while honoring explicit navigation to new manuscripts.

verification:
1. Start reading desk with manuscripts A+B
2. Navigate to lists page
3. Click manuscript C from list -> should show C in normal view, not A+B reading desk
4. Test language switch while in reading desk -> should preserve reading desk
5. Test RuntimeError - may be secondary symptom that disappears when state management is fixed
files_changed: [web/pages/browse.py]
