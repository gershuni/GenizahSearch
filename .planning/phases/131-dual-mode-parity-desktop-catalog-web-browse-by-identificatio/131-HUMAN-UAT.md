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

## Summary

total: 7
passed: 0
issues: 0
pending: 7
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

