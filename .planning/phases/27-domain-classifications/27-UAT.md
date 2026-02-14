---
status: diagnosed
phase: 27-domain-classifications
source: 27-01-SUMMARY.md, 27-02-SUMMARY.md
started: 2026-02-13T12:00:00Z
updated: 2026-02-13T12:15:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web - Domains on Browse Page
expected: Open a manuscript browse page in the web app. You should see a "Subject Domains" section in the metadata panel showing clickable domain text links. Parent domains are deduplicated when child already shown.
result: pass

### 2. Web - Domain Link Navigation
expected: Click a domain link on the browse page. You should be navigated to the search page with that domain pre-selected in the domain filter.
result: pass

### 3. Desktop - Domains on Browse Page
expected: Open a manuscript in the desktop browse tab and view extended info. You should see domain classifications displayed with purple border styling before the PGP metadata section.
result: pass

### 4. Desktop - Domain Link Navigation
expected: Click a domain link in the desktop browse extended info. The app should switch to the search tab with that domain ready for filtering.
result: pass

### 5. Web - Domain Filter in Search
expected: Domain filter dropdown in search controls shows hierarchical list with parent/child structure and counts.
result: issue
reported: "Web dropdown doesn't show parent domains visually. Desktop button+dialog approach is better than the web dropdown."
severity: major

### 6. Web - Standalone Domain Browse
expected: Select domains without text query, see matching manuscripts.
result: pass

### 7. Web - Text Search + Domain Filter
expected: Text query + domain filter returns only matching manuscripts in selected domains.
result: issue
reported: "Filtering is post-search (searches all first then filters). If so, UI should make this clear and filtering should be dynamic after search. Also need ability to EXCLUDE domains (search everything except X Y Z)."
severity: major

### 8. Web - Domain Badges on Results
expected: Search results show domain indicator badges with purple styling and "+N more" pattern.
result: pass

### 9. Desktop - Domain Filter Dialog
expected: Domains button opens hierarchical tree dialog with checkboxes, type-ahead, parent propagation.
result: pass

### 10. Desktop - Standalone Domain Browse
expected: Select domains without text query, see matching manuscripts.
result: pass

### 11. Desktop - Text Search + Domain Filter
expected: Text query + domain filter returns only filtered results. Badge shows selected domain count.
result: issue
reported: "Same as web: filtering is post-search, should be visible. Need exclude mode to filter OUT domains."
severity: major

## Summary

total: 11
passed: 8
issues: 3
pending: 0
skipped: 0

## Gaps

- truth: "Domain filter UI should use button+dialog pattern (like desktop) instead of dropdown, and show parent hierarchy visually"
  status: failed
  reason: "User reported: Web dropdown doesn't show parent domains visually. Desktop button+dialog approach is better than the web dropdown."
  severity: major
  test: 5
  root_cause: "Web used NiceGUI ui.select dropdown which flattens hierarchy. Desktop already has the correct QTreeWidget dialog pattern. Web needs to be redesigned to use a modal dialog with checkbox tree matching desktop."
  artifacts:
    - path: "web/pages/search.py"
      issue: "Domain filter uses ui.select dropdown instead of modal dialog with tree"
  missing:
    - "Replace web domain dropdown with button + modal dialog containing checkbox tree"
    - "Show parent/child hierarchy visually in the dialog"
  debug_session: ""

- truth: "Domain filtering should be post-search dynamic filter with exclude capability"
  status: failed
  reason: "User reported: Filtering is post-search but invisible. Should be dynamic after search with exclude mode."
  severity: major
  test: 7
  root_cause: "UX design revision: domain filter should appear AFTER search completes, showing domains found in results. All checked by default (uncheck to exclude). Apply filters instantly without re-searching. Selections remembered for next search."
  artifacts:
    - path: "web/pages/search.py"
      issue: "Domain filter is pre-search selection, not post-search dynamic filter"
    - path: "genizah_app.py"
      issue: "Same: domain filter is pre-search, needs to become post-search dynamic"
  missing:
    - "Collect domain data for all search results after search completes"
    - "Show 'Domains' button only when results have domain data"
    - "Open dialog showing domains in current results with counts, all checked"
    - "Uncheck = exclude, apply filters results instantly"
    - "Remember selections for next search"
  debug_session: ""

- truth: "Desktop domain filter also needs dynamic post-search filtering with exclude mode"
  status: failed
  reason: "User reported: Same as web: need exclude mode to filter OUT domains."
  severity: major
  test: 11
  root_cause: "Same UX revision as web: desktop filter should become post-search dynamic filter. DomainFilterDialog already has correct tree UI but needs to show domains from results with all-checked-by-default exclude pattern."
  artifacts:
    - path: "genizah_app.py"
      issue: "DomainFilterDialog shows all domains, not just those in results. Pre-search, not post-search."
  missing:
    - "DomainFilterDialog accepts result domains instead of all domains"
    - "All checked by default, uncheck to exclude"
    - "Dynamic filtering without re-searching"
    - "Button appears after search with result count indicator"
  debug_session: ""
