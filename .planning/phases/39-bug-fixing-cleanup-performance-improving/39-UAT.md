---
status: complete
phase: 39-bug-fixing-cleanup-performance-improving
source: 39-01-SUMMARY.md, 39-02-SUMMARY.md, 39-03-SUMMARY.md, 39-04-SUMMARY.md, 39-05-SUMMARY.md
started: 2026-02-19T23:00:00Z
updated: 2026-02-19T23:20:00Z
---

## Current Test
<!-- OVERWRITE each test - shows where we are -->

[testing complete]

## Tests

### 1. Search Results Pagination
expected: In the web app, run a search that returns many results. Results should display 50 per page with pagination controls at top and bottom.
result: issue
reported: "Bottom pagination scroll-to-top throws RuntimeError: The parent element this slot belongs to has been deleted. Traceback in on_page_change_bottom at search.py:2268 calling ui.run_javascript('window.scrollTo(0, 0)')"
severity: minor

### 2. Result Numbering Across Pages
expected: Navigate to page 2 of search results. The first result on page 2 should be numbered #51. Page 3 should start at #101.
result: pass

### 3. More Than 200 Results Accessible
expected: Run a search returning 200+ results. You should be able to page beyond result #200 (the old cap). Total result count shown should exceed 200.
result: pass

### 4. Filter Resets to Page 1
expected: Navigate to page 2+ of results, then apply a filter (library filter, domain exclusion, or text filter). The view should reset to page 1.
result: pass

### 5. Domain Filter Dialog Speed
expected: Open the domain filter dialog once (may take a few seconds on first load). Close it. Open it again -- second open should be nearly instant (was ~5 seconds before).
result: pass

### 6. Desktop Scroll Stability
expected: In the desktop app Virtual Reading Desk, rapidly scroll up and down while an image is loaded. The app should NOT crash with a RuntimeError. Scrolling should be smooth and stable.
result: pass
note: "No crash, but mouse wheel zooms images instead of scrolling. User wants mouse wheel to scroll in VRD (targeted fix only)."

### 7. PostHog Graceful Degradation
expected: With no POSTHOG_API_KEY environment variable set, the web app should load and function normally with no JavaScript console errors related to PostHog.
result: pass

### 7b. Web Page Navigation Speed
expected: Navigating between modules (search, browse, lists, etc.) should be responsive.
result: issue
reported: "Still slow to move from module to module in web"
severity: minor

### 8. E2E Test Suite Runs
expected: Running `pytest tests/e2e/ -x -q` should execute E2E tests. With ChromeDriver installed: 15+ tests pass. Without ChromeDriver: tests skip gracefully (no errors).
result: issue
reported: "ModuleNotFoundError: No module named 'selenium'. Tests error on import instead of skipping gracefully. Selenium import is at module level without try/except guard."
severity: major

## Summary

total: 9
passed: 6
issues: 4
pending: 0
skipped: 0

## Gaps

- truth: "Bottom pagination scroll-to-top works without errors"
  status: failed
  reason: "User reported: Bottom pagination scroll-to-top throws RuntimeError: The parent element this slot belongs to has been deleted. Traceback in on_page_change_bottom at search.py:2268 calling ui.run_javascript('window.scrollTo(0, 0)')"
  severity: minor
  test: 1
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""
- truth: "Web page navigation between modules is fast"
  status: failed
  reason: "User reported: Still slow to move from module to module in web"
  severity: minor
  test: 7b
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""
- truth: "Mouse wheel scrolls the Virtual Reading Desk view"
  status: failed
  reason: "User reported: mouse wheel zooms images instead of scrolling in VRD. Want targeted fix only, not wide change."
  severity: minor
  test: 6
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""
- truth: "E2E tests skip gracefully when selenium is not installed"
  status: failed
  reason: "User reported: ModuleNotFoundError: No module named 'selenium'. Tests error on import instead of skipping gracefully."
  severity: major
  test: 8
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""
