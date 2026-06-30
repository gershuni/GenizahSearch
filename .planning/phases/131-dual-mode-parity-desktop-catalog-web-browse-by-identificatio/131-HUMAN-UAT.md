---
status: partial
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
source: [131-VERIFICATION.md]
started: 2026-06-30T14:30:00Z
updated: 2026-06-30T14:30:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Desktop catalog dialog — mode toggle + in-session persistence + button label
expected: Open Catalog tab → library filter button → toggle Hide vs Show-only, select a subset, Apply, reopen the dialog within the same session. Dialog reopens with the previously selected mode and codes; toggling mode clears all checkboxes (D-04 reset); button shows "Showing N/M libraries" (Show-only) or "Hiding N libraries" (Hide) with the real Phase-130 pluralized keys; total M matches the selectable universe (library_codes_with_manuscripts minus LOCAL, not the broader LIBRARY_CODES count).
result: [pending]

### 2. Desktop — Hide-mode suppression on "Search/Composition in these results"
expected: With a Hide library selection active, click "Search in these results" and "Composition in these results". The library restriction is SUPPRESSED (not silently inverted); status bar shows "Library Hide filter not applied to search/composition" (~5 s); the search/composition scope is NOT narrowed by the Hide selection.
result: [pending]

### 3. Web /catalog Browse-by-Identification — true-facet shortlist, toggles, reload persistence
expected: Open the library filter dialog with NO other filters active → the count-shortlist shows TRUE full-set per-library counts from get_browse_library_facets (off-page libraries appear, not just current PAGE_SIZE=50 page); toggle Show-only/Hide, type in text-search, click sort-by-count vs A-Z, Apply, reload the page → mode+set survive (dict-shape persist); SEED-023 PGP/Editions filters still work alongside the library filter.
result: [pending]

### 4. Web /catalog — Hide-mode handoff to /search preserves mode
expected: With a Hide library selection active, click "Search in these results" → the /search page opens with the filter in Hide mode (button shows "Hiding N libraries", not "Showing N/M"). The {mode,codes} handoff preserves the mode round-trip (not converted to Show-only).
result: [pending]

### 5. Web /parallels — library-only Show-only scope (ungated) + reload persistence
expected: Run a composition search → open the new library-filter button → select a subset in Show-only mode with NO advanced filters active → Apply. Results rescope to the chosen libraries (NOT a no-op — library-only Show-only scope works ungated by _has_active_filters); results outside the selected libraries disappear. Reload the page → mode+set survive (parallels_library_filter key persists).
result: [pending]

### 6. Web /parallels — Hide-mode export scoping
expected: With Hide mode active, export results (XLSX/JSON) → exported rows exclude rows from the hidden libraries (Show-only pre-query and Hide pre-export scoping both hold).
result: [pending]

### 7. Desktop catalog dialog — dynamic per-library counts (gap 131-06 fix)
expected: Open Catalog tab → library filter button → each library row shows a count, e.g. "CUL (1,234)" (localized thousands separator). Turn "PGP Only" on (or a Scholarly-Editions / domain filter) and reopen the dialog → each row's count DROPS to that library's PGP-only (resp. filtered) manuscript count — the counts are dynamic w.r.t. the active catalog filters, at parity with web /catalog. The dialog must open without freezing the UI (facets are computed on a background worker thread).
result: [pending]

### 8. Desktop catalog dialog — sort toggle + type-to-find (gap 131-07)
expected: Open Catalog tab -> library filter button. (a) A "Search libraries..." box filters the list as you type (case-insensitive); typing hides non-matching rows but keeps their checks (a checked-then-hidden library stays selected on Apply). (b) A sort toggle "A-Z" / "By count" reorders the list: "By count" shows highest-count libraries first; "A-Z" is alphabetical. Switching sort keeps your checks. "Select All" selects every library even while a search filter is active. No UI freeze.
result: [pending]

### 9. Hebrew UI — library rows show the English code, searchable by it (gap 131-08)
expected: Switch the app to Hebrew UI. Open the catalog library filter dialog (web /catalog AND desktop) -> each library row shows the English code in parentheses after the Hebrew name, e.g. "ספריית האוניברסיטה של קיימברידג' (CUL)". Type "CUL" (or "JTS", etc.) in the type-to-find box -> the matching library is found. A-Z sort order is unchanged (sorts by the Hebrew name, not the appended code). ALSO (gap 131-09): switch to English UI -> rows now show the code too, e.g. "Cambridge University Library (CUL)", searchable by typing "CUL".
result: [pending]

## Summary

total: 9
passed: 0
issues: 1
pending: 9
skipped: 0
blocked: 0

## Gaps

- truth: "Desktop catalog LibraryFilterDialog shows per-library manuscript COUNTS (e.g. 'CUL (1,234)'), and those counts are DYNAMIC — they honor the catalog's other active filters (PGP Only / Scholarly-Editions / domain), at parity with web /catalog Browse-by-Identification."
  status: fix_implemented_pending_uat
  resolution: "Closed by gap plan 131-06 (commits 08812bce dialog facets param + Name(count) render; c3c12d1a _CatalogFacetWorker off-UI-thread facet computation wired into _open_catalog_library_dialog; 8f156334 +5 tests). 27/27 test_libfilter_desktop.py pass headless. Live render-smoke is UAT test #7 (pending)."
  reason: "User feedback during UAT: desktop library dialog must include the count like web does; and the count should be dynamic — e.g. with 'PGP Only' on, each library row should show the number of that library's manuscripts that are ALSO PGP-only. (both apps — web /catalog already satisfies this)"
  severity: major
  test: 1
  root_cause: "Desktop LibraryFilterDialog (desktop/dialogs_filter.py:1677) renders each row as get_library_display(code) ONLY — no count; its __init__ takes only (mode, selected_codes), no facets param. The desktop catalog path (genizah_app.py) never calls the shared FjmsService.get_browse_library_facets. Web /catalog already does this dynamically: catalog_browse.py:335 _fetch_library_facets_blocking calls fjms.get_browse_library_facets(...) passing the active pgp_filter/editions_filter/pgp_sys_ids/edition_sys_ids (+ domain/author/work/date/text), and the shared method (shared/fjms_service.py:get_browse_library_facets) reuses _build_browse_conditions to count DISTINCT AlmaId under those conditions while intentionally excluding the library filter itself. So the shared engine ALREADY supports dynamic facets; only the desktop surface is unwired. DMF-12 was satisfied on web but never mirrored to desktop (DMF-07 'at parity with the web lead' parity gap)."
  artifacts:
    - path: "desktop/dialogs_filter.py"
      issue: "LibraryFilterDialog.__init__ (~1692) builds rows as get_library_display(code) with no count. Add an optional facets: dict[str,int] | None param; render 'Name (count)' (localized thousands sep) when a code's count is present; keep name-only fallback when facets is None/missing. Sort-by-count is optional polish; alpha sort stays the default."
    - path: "genizah_app.py"
      issue: "The catalog library-dialog open path (~10436-10450) constructs LibraryFilterDialog without facets. Compute facets via the shared fjms.get_browse_library_facets(...) — passing the SAME active PGP/Editions/domain/date/text filter values the _CatalogRefreshWorker already threads into get_browse_results (PGP/Editions sets via _get_catalog_filter_sets, added v8.2.2) — OFF the UI thread (reuse/extend the existing worker; never block the UI thread), then pass the {code:count} dict into the dialog. sys_id_to_library must be the full-corpus callable (MetadataManager.get_library_for_id bound method), NOT a page/result-local map, so off-page libraries count correctly (mirror catalog_browse.py:351 + WR-05 None-guard)."
  missing:
    - "Add facets param + 'Name (count)' row rendering to desktop LibraryFilterDialog (name-only fallback preserved)."
    - "Wire genizah_app catalog dialog-open to compute facets via shared get_browse_library_facets with the active PGP/Editions/domain filters, off-thread, full-corpus library resolver."
    - "Tests: extend tests/test_libfilter_desktop.py — dialog renders counts when facets provided; counts honor an active PGP-only filter set (dynamic); name-only fallback when facets absent; LOCAL excluded; off-thread/no-UI-block contract."
  debug_session: ""

- truth: "Desktop catalog LibraryFilterDialog offers (a) a sort toggle between A-Z and by-count (descending), and (b) a type-to-find search box that filters the visible library rows as the user types -- at parity with web /catalog (catLibFilterSort + catLibFilterSearch)."
  status: fix_implemented_pending_uat
  resolution: "Closed by gap plan 131-07 (commits 57344637 search box + A-Z/By-count sort toggle in LibraryFilterDialog; 0104900b +6 tests; genizah_translations.py Hebrew keys מיון:/לפי כמות/א–ת). 33/33 test_libfilter_desktop.py pass headless. Live render-smoke is UAT test #8 (pending)."
  reason: "User feedback during UAT: 'We should add sort by count/a-z and type to find.' The desktop library dialog has neither; web /catalog already has both."
  severity: minor
  test: 7
  root_cause: "desktop/dialogs_filter.py LibraryFilterDialog has a single alphabetical sort (sorted by get_library_display in __init__) and no search box. Web /catalog (web/pages/catalog_browse.py add_head_html) defines catLibFilterSearch (case-insensitive substring on the row label -> hide non-matching rows; checkbox state preserved; hidden rows still count as checked) and catLibFilterSort (key='count' -> data-count desc; key='az' -> label asc), plus a 'Search libraries...' input and two sort controls. Select-All operates on ALL rows regardless of the active search filter. The desktop dialog ALREADY has the facet counts (self._facets from gap 131-06) needed for by-count sort."
  artifacts:
    - path: "desktop/dialogs_filter.py"
      issue: "LibraryFilterDialog: add (1) a QLineEdit search box (placeholder tr('Search libraries...')) whose textChanged hides non-matching QListWidgetItems via setHidden, case-insensitive substring match on the display label; checked state preserved across hide/show; get_checked_codes() still returns ALL checked codes incl. hidden (mirror web). (2) A sort control (two radio buttons or small combo: tr('A-Z') and tr('By count')) that reorders the list: by-count uses self._facets (count desc, missing/0 last), A-Z uses get_library_display asc (current default). Re-sorting must preserve each row's checked state and re-apply the active search filter. Select All / Select None operate on ALL rows regardless of filter (web parity). When self._facets is empty (no counts available), by-count falls back to A-Z. Keep dual-mode init, D-04 mode-reset, and _update_ok_button (counts checked across all items incl. hidden) intact."
  missing:
    - "Add type-to-find QLineEdit + setHidden row-filter to desktop LibraryFilterDialog (label substring, case-insensitive, check-state preserved)."
    - "Add A-Z / by-count sort toggle reordering the list (by-count via self._facets desc; A-Z default), preserving check state + active search filter."
    - "Tests: extend tests/test_libfilter_desktop.py -- search hides non-matching rows and preserves checks/get_checked_codes; by-count sort orders by self._facets desc; A-Z sort orders by display name; by-count falls back to A-Z when facets empty; Select All ignores the filter. Headless: GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen, this file only."
  debug_session: ""
- truth: "In Hebrew UI, each library row in the catalog library-filter dialog (web /catalog AND desktop) shows the English library code in parentheses after the Hebrew name (e.g. 'ספריית האוניברסיטה של קיימברידג'' (CUL)'), and the type-to-find search matches that code (typing 'CUL' finds Cambridge). A-Z sort order is unaffected (keyed on the bare Hebrew name)."
  status: fix_implemented_pending_uat
  resolution: "Closed by gap plan 131-08 (commits a9d642bd shared get_library_display with_code default-OFF param; cfd15930 desktop appends code in Hebrew UI via live CURRENT_LANG, sort keys on bare name; 421d1c70 web label+data-label so catLibFilterSearch matches; ed8e47f0 SUMMARY). 62/62 desktop+catalog tests pass headless; default-off invariant verified (existing callers unchanged). Live render-smoke is UAT test #9 (pending)."
  reason: "User feedback during UAT: 'in Heb UI add also the English acronym, like ספריית האוניברסיטה של קיימברידג'' (CUL), so it can be searched also.' In Hebrew UI the row label is the Hebrew name only, so a user cannot find a library by typing its well-known English code (CUL/JTS/etc.) in the new type-to-find box."
  severity: minor
  test: 9
  root_cause: "Dialog row labels come from shared get_library_display(code, short=False, lang=...) (shared/browse_map_utils.py:259) which returns the name WITHOUT the code. The type-to-find search matches the label/data-label, so the code is neither shown nor searchable in Hebrew UI. Desktop: desktop/dialogs_filter.py LibraryFilterDialog (_populate_rows uses get_library_display(short=False)). Web: web/pages/catalog_browse.py dialog builds data-label + visible label via get_library_display(short=False, lang=_lang) (~lines 1163/1178/1189)."
  artifacts:
    - path: "shared/browse_map_utils.py"
      issue: "Add an opt-in way to append the code to the full name, e.g. a `with_code: bool=False` param on get_library_display: when True (and short=False), return f'{name} ({code})'. Default False so NO existing caller (search results column, browse, etc.) changes. Language-agnostic (the code is the same in both langs)."
    - path: "desktop/dialogs_filter.py"
      issue: "LibraryFilterDialog: when CURRENT_LANG == 'he', render row labels with the code appended (get_library_display(code, short=False, with_code=True) or equivalent). Keep the gap-131-06 'Name (count)' rendering: final Hebrew row reads 'שם (CODE)  —  count' style consistent with existing count rendering. CRITICAL: keep the A-Z sort key and the by-count tie-break on the BARE name (get_library_display(short=False), no code) so ordering is unchanged; only the DISPLAYED label and the search-match text include the code. The type-to-find search already matches the label, so appending the code makes it searchable automatically."
    - path: "web/pages/catalog_browse.py"
      issue: "Catalog library dialog: when _lang == 'he', append the code to the visible label AND to data-label (lowercased) so catLibFilterSearch matches it. Keep the A-Z sort key (data-label vs a separate sort attr) consistent so the appended code does not reorder A-Z — if sort uses data-label, ensure the code is appended in a way that does not change alphabetical order (append at END), matching desktop."
  missing:
    - "Add opt-in with_code (or equivalent) to get_library_display in shared/browse_map_utils.py; default OFF (no other caller changes)."
    - "Desktop LibraryFilterDialog: append (CODE) to Hebrew-UI row labels; preserve A-Z/by-count sort keys on the bare name; counts rendering intact; search now matches code."
    - "Web catalog dialog: append (CODE) to Hebrew-UI label + data-label so catLibFilterSearch matches; preserve A-Z order."
    - "Tests: desktop tests/test_libfilter_desktop.py (he-lang label contains '(CUL)'; search 'CUL' matches; en-lang label unchanged; A-Z order unchanged) + a web/shared test for the get_library_display with_code param + the he-lang data-label includes the code."
  debug_session: ""
- truth: "The English library code in parentheses ALSO appears in the catalog library-filter dialog rows in ENGLISH UI (both apps) — e.g. 'Cambridge University Library (CUL)' — searchable by code. Extends gap 131-08 (Hebrew-only) to both languages."
  status: failed
  reason: "User follow-up after 131-08 confirmed working: 'Works well, add library codes to the EN UI too.'"
  severity: minor
  test: 9
  root_cause: "131-08 gated with_code on the active language at the two call sites: desktop dialogs_filter.py sets self._with_code = (_gc.CURRENT_LANG == 'he'); web catalog_browse.py passes with_code=(_lang == 'he'). Flip BOTH to always-on (with_code=True). The shared get_library_display already supports with_code with lang='en' (returns 'EN name (CODE)') — no shared-helper change. The 131-08 tests that assert the en-lang label has NO code must flip to assert it HAS the code."
  artifacts:
    - path: "desktop/dialogs_filter.py"
      issue: "Set self._with_code = True unconditionally (drop the CURRENT_LANG=='he' gate). Sort keys stay on the bare name; counts/search/sort unaffected."
    - path: "web/pages/catalog_browse.py"
      issue: "Pass with_code=True at BOTH catalog row builders (drop the _lang=='he' gate). data-label still receives the code so catLibFilterSearch matches in English UI too. Expand-section sort key stays bare (no with_code)."
    - path: "tests/test_libfilter_desktop.py"
      issue: "Flip the en-lang assertions added by 131-08: the en-lang row label now CONTAINS '(CUL)' and is searchable by 'CUL'. Keep he-lang assertions. Keep the get_library_display default-OFF invariant test unchanged (other callers still unaffected)."
    - path: "tests/test_libfilter_catalog.py"
      issue: "Flip the web en-lang data-label assertion to expect the code present."
  missing:
    - "Flip with_code gate to always-on at both call sites (desktop + web)."
    - "Update en-lang tests to expect the code; keep he-lang + default-off-invariant tests."
  debug_session: ""
