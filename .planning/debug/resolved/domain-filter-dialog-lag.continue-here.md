---
phase: debug/domain-filter-dialog-lag
task: fix-sanitize-and-verify
status: in_progress
last_updated: 2026-02-18T15:13:39.374Z
---

<current_state>
Debugging two domain filter dialog bugs via /gsd:debug session.
The debugger agent rewrote the dialog from ~200 individual NiceGUI ui.checkbox elements to a single ui.html() container with client-side JS. However, the rewrite had a bug: missing `sanitize=False` parameter on ui.html() calls, causing TypeError at runtime.

The sanitize=False fix has been applied to BOTH files (search.py:1873, parallels.py:1569) but NOT YET TESTED in browser.
</current_state>

<completed_work>

- Root cause identified for BOTH bugs:
  - Bug 1 (lag): ~200 individual ui.checkbox() NiceGUI calls, each creating a Vue component + WebSocket message = 7-19s
  - Bug 2 ("Other" toggle): dict key collision — multiple "Other" children from different parents overwrote each other's checkbox references
- Dialog rewritten: replaced ui.checkbox approach with single ui.html() + client-side JS (zero Python round-trips during interaction)
- Page-level JS helpers added: domainFilterParentChanged(), domainFilterSelectAll(), domainFilterGetExcluded() — search.py:280-310, parallels.py:187-220
- Stale server mystery solved: two server process trees bound to port 8081, browser connecting to old one
- sanitize=False fix applied to both search.py:1873 and parallels.py:1569
- shared/fjms_service.py: duplicate children merge fix (lines 640-651) — verified correct
- Debug diagnostic prints added (module-level, function-level, dialog-level + timing)
- All tests pass: 671 passed, 5 skipped, 0 new failures

</completed_work>

<remaining_work>

- Verify the dialog actually opens after sanitize=False fix (user hasn't tested yet)
- Check timing output in console to confirm fast dialog open (<1s target)
- Test Select All/None toggles ALL checkboxes including "Other" entries
- Test Apply correctly filters results
- Test parallels page domain filter dialog (same pattern)
- Remove debug print statements after verification
- Commit the fix
- Update debug session file status to resolved

</remaining_work>

<decisions_made>

- Replaced ~200 NiceGUI ui.checkbox elements with single ui.html() container + raw HTML checkboxes
- All checkbox interaction (parent-child propagation, Select All/None) runs client-side via JS
- Data exchanged only on Apply click via async ui.run_javascript()
- Unique container IDs (uuid-based) to avoid stale DOM conflicts
- CSS.escape() used in JS querySelector for domain names with special characters
- sanitize=False required by this NiceGUI version for ui.html() (every other ui.html call in codebase uses it)

</decisions_made>

<blockers>
- Previous sessions 2-3 were blocked by stale server processes — now resolved (identified and killed)
- NiceGUI version in venv (Python 3.9) requires sanitize kwarg — debugger agent didn't know this
</blockers>

<context>
The debug session (agent ID: abbe36d) made the full rewrite and it's solid code. The only issue was the missing sanitize=False parameter, which is a NiceGUI API requirement this version enforces. The fix is a one-token change on two lines. The user needs to restart the server and test. If the dialog opens fast and Select All/None works, the debug is complete.

Key files changed:
- web/pages/search.py: lines 280-310 (JS helpers), 1719-1911 (dialog rewrite)
- web/pages/parallels.py: lines 187-220 (JS helpers), 1433-1605 (dialog rewrite)
- shared/fjms_service.py: lines 640-651 (merge fix from session 3)
- web/main.py: line 1 area (debug print)

Debug session file: .planning/debug/domain-filter-dialog-lag.md
</context>

<next_action>
Start with: restart web server (`venv\Scripts\python.exe -m web.main`), search for a broad Hebrew term, click "Filter by domains", verify dialog opens in <1s and Select All/None works. Then remove debug prints and commit.
</next_action>
