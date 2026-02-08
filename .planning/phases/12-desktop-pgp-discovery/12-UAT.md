---
status: complete
phase: 12-desktop-pgp-discovery
source: 12-01-SUMMARY.md, 12-02-SUMMARY.md, 12-03-SUMMARY.md
started: 2026-02-08T19:00:00Z
updated: 2026-02-08T19:15:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Browse Tab PGP Extended Info
expected: Open a manuscript with PGP data in the Browse tab. A "Show Extended Info" button appears. Clicking it reveals a PGP section with green left border showing document type, tags, dates, description, and PGP link.
result: pass

### 2. ResultDialog PGP Extended Info
expected: Open a search result that has PGP data. The ResultDialog extended info section shows PGP metadata (type, tags, dates, description) alongside any existing KTI/Oxford/Cambridge info.
result: pass

### 3. PGP Tag Click Navigation
expected: In the extended info panel (Browse or ResultDialog), click a green PGP tag link. The app switches to the Search tab and initiates a search for that tag.
result: issue
reported: "not doing anything when clicking."
severity: major

### 4. PGP-Only Manuscript Extended Info
expected: Open a manuscript that has PGP data but no KTI/Oxford/Cambridge enrichment. The "Show Extended Info" button still appears and shows the PGP section.
result: pass

### 5. Desktop PGP Badge Column
expected: Run a search in the desktop app. Results table shows a "PGP" column (narrow, after SRC column). Manuscripts with PGP transcriptions show a green "PGP" badge in that column.
result: pass

### 6. Desktop PGP Only Filter
expected: In the desktop Search tab, a "PGP Only" checkbox appears (on a row below the main controls). Checking it and searching filters results to only show manuscripts that have PGP transcriptions.
result: pass

### 7. Desktop Tag Search Dropdown
expected: In the desktop Search tab, an editable dropdown with PGP tags appears alongside a "Search Tag" button. Selecting a tag and clicking "Search Tag" shows manuscripts with that tag.
result: pass

### 8. Web PGP Text Badge
expected: In the web app, search results for manuscripts with PGP transcriptions show a styled green text badge (like the library badge) indicating PGP availability.
result: pass

### 9. Web PGP Only Filter
expected: In the web app filters panel, a "PGP Only" checkbox is available. Enabling it and applying filters restricts results to manuscripts with PGP transcriptions.
result: issue
reported: "Toggle Filters not shown at all"
severity: major

### 10. Desktop PGP Joins in Related Fragments
expected: In the desktop app, open Related Fragments dialog for a manuscript that is part of a multi-fragment PGP document. PGP joins appear in the table with a green "PGP" source label alongside any user-created joins.
result: issue
reported: "It is shown in the dialog box but not in the menu that opens in the same icon with triangle"
severity: major

### 11. PGP Joins Deletion Protection
expected: In the Related Fragments dialog, select a PGP join row. The delete button should be disabled or have no effect -- PGP joins cannot be deleted (only user joins can).
result: pass

### 12. Tag Search Result Navigation (additional)
expected: After performing a tag search, clicking on a result should load that manuscript in Browse tab without getting stuck. Typing another shelfmark afterwards should navigate normally.
result: issue
reported: "Clicking on a search result from tag search gets the browse tab stuck on this result even if typing another ms"
severity: major

### 13. ResultDialog from Tag Search (additional)
expected: Double-clicking a tag search result should open the ResultDialog normally.
result: issue
reported: "ResultDialog not working after tag search"
severity: major

### 14. Tag Search Snippet Content (additional)
expected: Tag search result snippets should show transcription text (Hebrew), not English metadata.
result: issue
reported: "snippet in tag search should be of the text not the english metadata"
severity: minor

### 15. Bilingual UI Labels (additional)
expected: All Phase 12 UI elements (PGP Only checkbox, Search Tag button, tag dropdown, PGP badge, Extended Info labels) should have Hebrew translations and switch with the language setting.
result: issue
reported: "All features should be bilingual"
severity: major

## Summary

total: 15
passed: 8
issues: 7
pending: 0
skipped: 0

## Gaps

- truth: "Clicking a green PGP tag link in extended info switches to Search tab and initiates a search for that tag"
  status: failed
  reason: "User reported: not doing anything when clicking."
  severity: major
  test: 3
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""

- truth: "Web app filters panel shows a PGP Only checkbox that restricts results to PGP manuscripts"
  status: failed
  reason: "User reported: Toggle Filters not shown at all"
  severity: major
  test: 9
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""

- truth: "PGP joins appear in the dropdown menu triggered by the joins icon triangle, not just in the dialog"
  status: failed
  reason: "User reported: It is shown in the dialog box but not in the menu that opens in the same icon with triangle"
  severity: major
  test: 10
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""

- truth: "Tag search results navigate normally without getting Browse tab stuck"
  status: failed
  reason: "User reported: Clicking on a search result from tag search gets the browse tab stuck on this result even if typing another ms"
  severity: major
  test: 12
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""

- truth: "ResultDialog opens normally from tag search results"
  status: failed
  reason: "User reported: ResultDialog not working after tag search"
  severity: major
  test: 13
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""

- truth: "Tag search result snippets show transcription text not English metadata"
  status: failed
  reason: "User reported: snippet in tag search should be of the text not the english metadata"
  severity: minor
  test: 14
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""

- truth: "All Phase 12 UI elements have Hebrew translations and respect language setting"
  status: failed
  reason: "User reported: All features should be bilingual"
  severity: major
  test: 15
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""
