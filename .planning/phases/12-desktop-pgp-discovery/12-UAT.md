---
status: diagnosed
phase: 12-desktop-pgp-discovery
source: 12-01-SUMMARY.md, 12-02-SUMMARY.md, 12-03-SUMMARY.md
started: 2026-02-08T19:00:00Z
updated: 2026-02-08T19:20:00Z
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

### 16. Browse Tab Extended Info Fetches All Sources (additional)
expected: Browse tab extended info should fetch and display KTI/Oxford/Cambridge data alongside PGP data, like ResultDialog does.
result: issue
reported: "Browse tab extended info shows only PGP. It should fetch from sources too, like ResultDialog"
severity: major

## Summary

total: 16
passed: 8
issues: 8
pending: 0
skipped: 0

## Gaps

- truth: "Clicking a green PGP tag link in extended info switches to Search tab and initiates a search for that tag"
  status: failed
  reason: "User reported: not doing anything when clicking."
  severity: major
  test: 3
  root_cause: "Duplicate _search_by_pgp_tag method — override at line 12859 missing tab switch, dead first definition at line 7472 has it"
  artifacts:
    - path: "genizah_app.py:12859"
      issue: "Override missing self.tabs.setCurrentWidget(self.search_tab)"
    - path: "genizah_app.py:7472"
      issue: "Dead code — first definition overridden by second"
  missing:
    - "Add tab switch to line 12859 definition, remove dead line 7472 definition"
  debug_session: ".planning/debug/phase12-desktop-issues.md"

- truth: "Web app filters panel shows a PGP Only checkbox that restricts results to PGP manuscripts"
  status: failed
  reason: "User reported: Toggle Filters not shown at all"
  severity: major
  test: 9
  root_cause: "toggle_filters() reads .style as Style object not string — 'display: none' in check always fails silently"
  artifacts:
    - path: "web/pages/search.py:728-734"
      issue: "toggle_filters reads .style as object, string check fails"
    - path: "web/pages/search.py:538-539"
      issue: "Panel starts hidden with display:none, never gets shown"
  missing:
    - "Replace .style read-back with boolean state variable, matching toggle_search_panel pattern"
  debug_session: ".planning/debug/phase12-web-joins-issues.md"

- truth: "PGP joins appear in the dropdown menu triggered by the joins icon triangle, not just in the dialog"
  status: failed
  reason: "User reported: It is shown in the dialog box but not in the menu that opens in the same icon with triangle"
  severity: major
  test: 10
  root_cause: "Both dropdown menus (_update_joins_dropdown, _rd_update_joins_menu) only query local JoinsManager, never call _get_pgp_joins()"
  artifacts:
    - path: "genizah_app.py:5793-5798"
      issue: "Browse dropdown returns 'No joined fragments' without checking PGP"
    - path: "genizah_app.py:2765-2769"
      issue: "Reading Desk dropdown returns 'No joined fragments' without checking PGP"
    - path: "corrections_ui.py:3490-3568"
      issue: "_get_pgp_joins() only accessible from JoinsDialog, not main app"
  missing:
    - "Add PGP joins fallback to both dropdown functions using shared service functions"
  debug_session: ".planning/debug/phase12-web-joins-issues.md"

- truth: "Tag search results navigate normally without getting Browse tab stuck"
  status: failed
  reason: "User reported: Clicking on a search result from tag search gets the browse tab stuck on this result even if typing another ms"
  severity: major
  test: 12
  root_cause: "open_result_in_browse doesn't clear browse_shelf_input or set last_browse_field='sys' for tag results without FL ID"
  artifacts:
    - path: "genizah_app.py:13407-13447"
      issue: "Missing state reset in else branch when derived_fl_id is None"
    - path: "genizah_app.py:16414-16524"
      issue: "browse_load uses stale last_browse_field priority"
  missing:
    - "Add self.browse_shelf_input.clear() and self._set_last_browse_field('sys') in else branch at line 13443"
  debug_session: ".planning/debug/phase12-desktop-issues.md"

- truth: "ResultDialog opens normally from tag search results"
  status: failed
  reason: "User reported: ResultDialog not working after tag search"
  severity: major
  test: 13
  root_cause: "Tag search results missing uid, raw_header fields — load_result_by_index crashes on data['uid'] KeyError"
  artifacts:
    - path: "genizah_app.py:3552-3564"
      issue: "load_result_by_index crashes on data['uid'] bare dict key access"
    - path: "genizah_app.py:12816-12836"
      issue: "Tag result format missing uid, raw_header is empty string"
  missing:
    - "Guard load_result_by_index for missing keys or enrich tag search results with required fields"
  debug_session: ".planning/debug/phase12-desktop-issues.md"

- truth: "Tag search result snippets show transcription text not English metadata"
  status: failed
  reason: "User reported: snippet in tag search should be of the text not the english metadata"
  severity: minor
  test: 14
  root_cause: "get_fragments_by_tag() only selects pgpid, shelfmark_combined, document_type, description — never fetches transcription column"
  artifacts:
    - path: "shared/document_service.py:474"
      issue: "select() missing transcription column"
    - path: "genizah_app.py:12824"
      issue: "Snippet built from English description field"
  missing:
    - "Add transcription to select, use first ~120 chars as snippet (fallback to description)"
  debug_session: ".planning/debug/phase12-snippet-bilingual-issues.md"

- truth: "All Phase 12 UI elements have Hebrew translations and respect language setting"
  status: failed
  reason: "User reported: All features should be bilingual"
  severity: major
  test: 15
  root_cause: "8 translation keys missing from genizah_translations.py + key mismatches between tr() calls and dict entries"
  artifacts:
    - path: "genizah_translations.py"
      issue: "Missing: PGP Only, Show only manuscripts..., Search Tag, PGP Tag:, Search by PGP Tag..., Searching tag: {}..., No local results for tag: {}, Tag: {} - {} results"
    - path: "genizah_app.py:12796"
      issue: "Key mismatch: tr('No results for tag: {}') vs dict 'No results for tag'"
  missing:
    - "Add 8 Hebrew translations and fix key mismatches"
  debug_session: ".planning/debug/phase12-snippet-bilingual-issues.md"

- truth: "Browse tab extended info fetches and displays KTI/Oxford/Cambridge data alongside PGP, like ResultDialog"
  status: failed
  reason: "User reported: Browse tab extended info shows only PGP. It should fetch from sources too, like ResultDialog"
  severity: major
  test: 16
  root_cause: "Browse tab extended info only calls PGP worker, never fetches KTI/Oxford/Cambridge enrichment data like ResultDialog does"
  artifacts:
    - path: "genizah_app.py"
      issue: "Browse tab _build_pgp_extended_info_html only shows PGP section, no enrichment data fetch"
  missing:
    - "Add enrichment data worker call to Browse tab, merge KTI/Oxford/Cambridge info with PGP section"
  debug_session: ""
