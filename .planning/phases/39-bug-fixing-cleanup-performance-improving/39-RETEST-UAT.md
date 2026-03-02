---
status: testing
phase: 39-bug-fixing-cleanup-performance-improving
source: 39-UAT.md (gap re-test)
started: 2026-02-20T14:00:00Z
updated: 2026-02-20T14:00:00Z
---

## Current Test
<!-- OVERWRITE each test - shows where we are -->

number: 1
name: Bottom Pagination Scroll-to-Top
expected: |
  In the web app, run a search returning many results. Scroll down and click a page number in the BOTTOM pagination. The page should scroll to the top and show the new page of results — no RuntimeError in the console/logs.
awaiting: user response

## Tests

### 1. Bottom Pagination Scroll-to-Top
expected: In the web app, run a search returning many results. Scroll down and click a page number in the BOTTOM pagination. The page should scroll to the top and show the new page of results — no RuntimeError in the console/logs.
result: pass

### 2. VRD Mouse Wheel Scroll vs Zoom
expected: In the desktop app Virtual Reading Desk, load an image. Plain mouse wheel should SCROLL the view. Ctrl+mouse wheel should ZOOM in/out. No crashes.
result: pass

### 3. E2E Tests Skip Gracefully Without Selenium
expected: Run `pytest tests/e2e/ -x -q` without selenium installed. Tests should skip gracefully with a message like "skipped" — no ModuleNotFoundError or import crash.
result: pass
note: "Selenium is installed — 15 passed, 1 skipped (340s). importorskip guards present but not triggered. No import crashes."

### 4. Page Navigation Speed
expected: In the web app, navigate between modules (search, browse, lists, etc.). Pages should load noticeably faster than before — CSS is now browser-cached and login dialog is lazy-built.
result: issue
reported: "Search: 6s, Browse: 12s, Community: 5s. Still not acceptable."
severity: major

## Summary

total: 4
passed: 3
issues: 1
pending: 0
skipped: 0

## Gaps

- truth: "Page navigation between modules is fast (under 3 seconds)"
  status: failed
  reason: "Search 6s, Browse 12s, Community 5s — serial blocking DB queries on each page load"
  severity: major
  test: 4
  root_cause: |
    Browse: 6+ serial DB queries (NLI manifest, PGP sources, FJMS 5-call metadata) block rendering.
    Search: PGP tags + transcription sys_id lookup + domain hierarchy fetched sequentially.
    Community: 3 Supabase stats queries + feed JOIN run sequentially.
    Fix: asyncio.gather() to parallelize independent queries on each page.
  artifacts:
    - path: "web/pages/browse.py"
      issue: "load_page() runs manifest fetch, PGP sources, FJMS metadata serially"
    - path: "web/pages/search.py"
      issue: "PGP tags, transcription lookup, domain hierarchy not parallelized"
    - path: "web/pages/discoveries.py"
      issue: "Stats + feed queries run sequentially"
