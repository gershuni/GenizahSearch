---
status: investigating
trigger: "Two bugs in the domain filter dialog on the web app: (1) Dialog opens very slowly (7-19 seconds), (2) Other domains don't toggle with Select All/None buttons"
created: 2026-02-18T00:00:00Z
updated: 2026-02-18T00:02:00Z
---

## Current Focus

hypothesis: CONFIRMED -- stale server process killed, fresh server started with our code. DEBUG marker visible in startup output.
test: User must navigate to search page and click Filter by domains in browser
expecting: Module-level marker prints on page load, timing markers print on dialog open, dialog opens fast
next_action: Wait for user to test in browser and report console output + dialog speed

## Symptoms

expected: Domain filter dialog opens quickly (<1s). Select All/None toggles ALL checkboxes including "Other" entries.
actual: Bug 1 - Dialog takes 7-19 seconds to appear. Bug 2 - Select All checks most boxes but "Other" entries remain unchecked.
errors: No error messages.
reproduction: Run web app locally, search for anything with FJMS domains, click "Filter by domains" button.
started: Bug 1 existed whenever enough domains were in results (~200 checkboxes). Bug 2 from Phase 27.

## Eliminated

- hypothesis: ui.html() with inline onchange handlers calling back to Python per-change (session 2)
  evidence: Made lag WORSE (19s vs 7-12s) -- each onchange triggered Python round-trip
- hypothesis: ui.tree with tick_strategy='leaf' + tree.tick() (session 2)
  evidence: "Other" doesn't toggle (Quasar bug)
- hypothesis: _batch_updating flags (session 2)
  evidence: didn't help
- hypothesis: _domain_hierarchy_cache as class variable (session 2)
  evidence: instance lifecycle issues
- hypothesis: Code changes between v5.9.0 and HEAD caused the lag (session 4)
  evidence: Dialog function IDENTICAL between v5.9.0 and HEAD committed code

## Evidence

- session 3: fjms_service.py duplicate children merge verified correct (13 unique Other entries, zero dupes)
- session 4: get_domain_hierarchy() takes 1.35s, returns 25 parents + 175 children = 200 items
- session 4: Dialog function identical v5.9.0 vs HEAD -- lag was always latent, depends on domain count
- session 4: NiceGUI ui.checkbox creates separate Vue component + WebSocket message per element
- session 4: Bug 2 caused by dict key collision -- duplicate "Other" domain names overwrite checkbox refs
- session 4: ui.html() does NOT execute embedded script tags -- JS must use ui.add_head_html()
- session 4: 671 tests pass, 0 regressions
- session 5: Confirmed on disk -- zero ui.checkbox in dialog function, HTML/JS rewrite present
- session 5: Only one search.py exists (web/pages/search.py), no stale .pyc files
- session 5: NiceGUI reload uses uvicorn ChangeReload watching CWD for *.py -- should detect changes
- session 5: Added 3 print markers to verify code loading: module level, create_search_page, dialog open
- session 5: FOUND ROOT CAUSE OF STALE CODE: TWO web server process trees on port 8081
  - OLD: PID 60124 (venv python) -> 79384 (Python 3.9) -> 80908 (fork) -- stale code, browser connects here
  - NEW: PID 33580 (Python 3.11) -> 31504 (fork) -- our changes, not used by browser
  - NikkudCorrections (PID 43316, 74132) is safe -- completely separate project

## Resolution

root_cause: |
  Bug 1 (Lag): Each of ~200 ui.checkbox calls creates a separate Vue component and WebSocket
  message. Combined with 1.35s synchronous DB call and calc_visible() triggered on every
  checkbox value change, total dialog creation took 7-19 seconds.

  Bug 2 (Other toggle): Python checkboxes dict used domain name as key. Multiple "Other"
  entries from different parents collided (last one wins), making earlier checkboxes
  unreachable by select_all/select_none.

fix: |
  Replaced ~200 individual NiceGUI ui.checkbox elements with single ui.html() container
  of raw HTML checkboxes. All interaction (parent-child propagation, Select All/None) runs
  as client-side JavaScript with zero Python round-trips. Data exchange only on Apply via
  async ui.run_javascript(). Pre-cache hierarchy during execute_search() to eliminate DB call.

  Key design decisions:
  - JS functions defined via ui.add_head_html() at page level (ui.html doesn't execute scripts)
  - Dynamic container ID (uuid) prevents stale dialog DOM conflicts
  - CSS.escape() for safe attribute selectors with special-character domain names
  - parentCb.closest('[id^="domain-filter-"]') for parent-child propagation without hardcoded IDs

verification: |
  Awaiting user verification with diagnostic markers.
  Three print markers added: module load, create_search_page, dialog open.
  Timing instrumentation added: hierarchy check, HTML build, dialog build, dialog.open, TOTAL.
  User must fully restart server (Ctrl+C, python -m web.main) and check console output.
files_changed:
  - web/pages/search.py (dialog rewrite + page-level JS)
  - web/pages/parallels.py (same pattern)
  - shared/fjms_service.py (duplicate children merge -- from session 3, kept)
