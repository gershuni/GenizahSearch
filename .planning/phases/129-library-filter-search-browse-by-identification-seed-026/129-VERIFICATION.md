---
phase: 129-library-filter-search-browse-by-identification-seed-026
verified: 2026-06-28T00:00:00Z
status: gaps_found
score: 5/5 must-haves verified (data layer) — 8 UX gaps found in human smoke test 2026-06-28
overrides_applied: 0
human_verification:
  - test: "Open the web app under Hebrew UI (/search). Run a search that returns results from multiple libraries. Click the 'סינון לפי ספרייה' button and select one library. Verify that the result count drops and that results from OTHER libraries are gone. Verify the chip appears reading 'ספרייה: <Hebrew library name>' and clicking × restores all results."
    expected: "Results filtered to selected library BEFORE the pagination slice (results beyond item 50 also filtered); chip visible; removing chip restores full set; no English text appears anywhere in the library filter UI."
    why_human: "Cannot verify NiceGUI async rendering, RTL/Hebrew chip text, or visual filter application headlessly."
  - test: "Open the web app under Hebrew UI (/browse or Browse-by-Identification). Select a library from the 'סינון לפי ספרייה' control. Verify the total count in the pagination area changes to reflect filtered records, and that the Editions/PGP filters still compose correctly when both are active."
    expected: "Total reflects the full filtered set before pagination (not just the visible page); composing library + PGP/Editions filters narrows correctly; no English text in the library filter UI."
    why_human: "Cannot verify SQL push-down result on real corpus data, or that total/pagination is correct without running the live web app."
  - test: "On the desktop app under Hebrew UI, open the catalog Browse-by-Identification tab. Click the 'כל הספריות' button, select CUL from the menu. Verify the catalog refreshes and shows only CUL records. Click the × on the chip to remove the filter."
    expected: "CUL filter applied via background worker; button label changes to 'ספריות (1)'; chip appears; removing restores all records; no English in the library filter UI; LOCAL ('My Library') is NOT present in the dropdown menu."
    why_human: "Cannot test PyQt6 widget rendering, QMenu interaction, or chip click behavior headlessly in this environment."
---

# Phase 129: Library Filter — Verification Report

**Phase Goal:** Add a library filter keyed on `library_code` to (1) web `/search` results as a multi-select applied over the FULL result set BEFORE the render cap, persisted via safe_storage, removable chips, i18n EN/HE; (2) Browse-by-Identification (catalog) as a `library_codes` arg pushed DOWN into `shared/fjms_service.get_browse_results` BEFORE COUNT/LIMIT so total/pagination stay correct; (3) desktop parity.

**Verified:** 2026-06-28
**Status:** human_needed (all automated checks VERIFIED; 3 items need human smoke-testing)
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| 1 | Web search library filter narrows full pre-render set (empty = all); persists via safe_storage; no English leak under Hebrew | VERIFIED | `_apply_library_filter` exists in `web/pages/search.py:3521`; iterates full `results_list` (never sliced first); loaded via `_safe_get('search_library_filter', [])` (line 187); saved via `persist_value('search_library_filter', ...)` (lines 1605, 1674); all 6 Hebrew keys confirmed in `genizah_translations.TRANSLATIONS` (fix commit `0b5bb13d`) |
| 2 | Browse-by-Identification `library_codes` arg narrows `total` over full filtered set; backward-compatible; composes with SEED-023 PGP/Editions | VERIFIED | `get_browse_results` extended with `library_codes`/`library_sys_ids` params (fjms_service.py:2060-2061); EXISTS clause inserted before `where =` at line 2261-2284; `_FILTER_TEMP_TABLES` includes `"_browse_filter_library"` (line 1997); 6 service tests pass (130 total, 0 failures); content-derived tuple token prevents stale reuse (WR-03 fix: `tuple(sorted(library_codes))` at line 2266) |
| 3 | Desktop catalog Browse-by-Identification gains same library filter; existing search-results filtering untouched | VERIFIED | `self._catalog_library_filter = []` at genizah_app.py:9601; `_CatalogRefreshWorker` accepts `library_filter` + `meta_mgr` params (line 502); resolution runs inside `run()` via `self._meta_mgr` (line 551); `LOCAL` excluded from menu (WR-02 fix, line 9854); `list(self._catalog_library_filter)` copy passed to worker (WR-01 fix, line 10190) |
| 4 | Codex-review-before-code gate satisfied | VERIFIED | `129-CODEX-CRUX-REVIEW.md` exists with verdict APPROVE WITH CHANGES; both required changes (content token, fail-open empty handling) implemented in the code |
| 5 | Tests green; ruff clean; existing PGP/printed + SEED-023 filters unbroken | VERIFIED | All targeted test suites pass: `test_libfilter_catalog.py` 6/6, `test_libfilter_web_search.py` 7/7, `test_libfilter_desktop.py` 3/3, `test_pgp_filter_cascade.py` 4/4, `test_no_raw_storage_access.py` 6/6, `test_catalog_availability_filter.py` 4/4, `test_seed023_catalog_filters.py` 11/11, `test_fjms_service.py` 109/109; ruff clean on all modified files |

**Score:** 5/5 truths verified

---

## Required Artifacts

| Artifact | Expected | Status | Details |
|----------|---------|--------|---------|
| `shared/fjms_service.py` | library push-down core + `resolve_library_sys_ids` | VERIFIED | `_browse_filter_library` in `_FILTER_TEMP_TABLES`; `library_codes`/`library_sys_ids` in `get_browse_results`; `resolve_library_sys_ids` at module level (line 3607); importable confirmed |
| `tests/test_libfilter_catalog.py` | 6 LIBFILTER-02 service tests | VERIFIED | File exists; 6 tests collected and green |
| `web/pages/search.py` | `_apply_library_filter`, routing predicate widening, persistence, chips | VERIFIED | `_apply_library_filter` defined (line 3521); `_compute_library_facets` defined (line 3533); 6 routing predicates widened with `or bool(search_state.library_filter)` (lines 3586, 3615, 3993, 4246, 4961, 5301); plus `has_any` widening for chips (line 1144) |
| `tests/test_libfilter_web_search.py` | 7 LIBFILTER-01 tests | VERIFIED | File exists; 7 tests collected and green |
| `web/pages/catalog_browse.py` | catalog filter state + dropdown + chips + `_fetch_results_blocking` wiring | VERIFIED | `catalog_library_filter` state persisted via `safe_user_get`/`safe_user_set`; `resolve_library_sys_ids` called inside `_fetch_results_blocking` (off event loop); `library_codes`/`library_sys_ids` passed to `get_browse_results`; `clear_library_code` + `clear_filter('library')` branch present; all toggle/clear handlers call `await refresh_results()` |
| `genizah_app.py` | desktop worker wiring + state + widget + chips | VERIFIED | `self._catalog_library_filter` state; `_CatalogRefreshWorker` with `library_filter` + `meta_mgr` params; `list(self._catalog_library_filter)` copy passed; `LOCAL` excluded from menu (WR-02); `resolve_library_sys_ids` called inside `run()` via `self._meta_mgr`; chips in `_catalog_update_chips`; `_catalog_remove_filter` has library branch |
| `tests/test_libfilter_desktop.py` | 3 LIBFILTER-03 gui-marked tests | VERIFIED | File exists; registered in `_GUI_TEST_FILES` (conftest.py:94); 3 tests collected and green |
| `genizah_translations.py` | 6 Hebrew translation keys for library-filter UI strings | VERIFIED | Keys confirmed present: "Filter by library", "Filter results by library", "Select libraries...", "Library filter", "All Libraries", "Libraries" (fix commit `0b5bb13d`) |

---

## Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `fjms_service.get_browse_results` | `_browse_filter_library` EXISTS clause before COUNT/LIMIT | `_ensure_filter_temp` + `conditions` list | WIRED | Confirmed: EXISTS block at lines 2261-2272, inserted before `where = " AND ".join(conditions)` |
| `fjms_service` library token | content-derived collision-free token | `tuple(sorted(library_codes))` at line 2266 | WIRED | WR-03 fix confirmed: no `hash()` — uses tuple comparison directly |
| `web/pages/search.py _apply_library_filter` | all post-search cascade paths | 3 call sites in filter helpers | WIRED | Called in `_apply_printed_filter_and_render` (line ~3556), `_apply_domain_exclusions` (line ~4061), `_apply_word_search_exclusions_and_render` both branches (lines ~3998, ~4006) |
| `web/pages/search.py library filter state` | safe_storage chokepoint | `persist_value('search_library_filter', ...)` / `_safe_get` | WIRED | Confirmed at lines 187, 1605, 1674; `test_no_raw_storage_access.py` confirms no raw `app.storage.user` access |
| `catalog_browse._fetch_results_blocking` | `get_browse_results(library_codes=, library_sys_ids=)` | `resolve_library_sys_ids` off event loop inside io_bound | WIRED | Lines 271-283 confirmed; runs inside `run.io_bound` context |
| `catalog_browse library toggle/chip clear` | `await refresh_results()` repaint path | all handlers use `refresh_results()` not `fetch_results()` | WIRED | Confirmed: `clear_library_code` (line 985) and `clear_filter('library')` both call `await refresh_results()` |
| `genizah_app._catalog_start_async_refresh` | `_CatalogRefreshWorker(library_filter=..., meta_mgr=...)` | constructor params | WIRED | Line 10190: `library_filter=list(self._catalog_library_filter), meta_mgr=self.meta_mgr` |
| `_CatalogRefreshWorker.run()` | `resolve_library_sys_ids(self._library_filter, self._meta_mgr)` | explicit `self._meta_mgr` ctor arg | WIRED | Lines 549-551 confirmed; uses `self._meta_mgr` not `self.parent().meta_mgr` |

---

## Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|--------------|--------|-------------------|--------|
| `shared/fjms_service.get_browse_results` | `total` (COUNT result) | SQL `COUNT(DISTINCT c.AlmaId)` with EXISTS filter applied before count | Yes — verified EXISTS clause appears before `where =` which feeds COUNT query | FLOWING |
| `web/pages/catalog_browse._fetch_results_blocking` | `library_sys_ids` | `resolve_library_sys_ids(library_codes, _state.meta_mgr)` reading `csv_bank` | Yes — O(255K) comprehension over in-memory csv_bank, returns real sys_id set | FLOWING |
| `web/pages/search._apply_library_filter` | filtered results | `r.get('display', {}).get('library_code', '')` on real search result dicts | Yes — reads actual library_code from MetadataManager-populated display data | FLOWING |
| `genizah_app._CatalogRefreshWorker.run()` | `library_sys_ids` | `resolve_library_sys_ids(self._library_filter, self._meta_mgr)` | Yes — explicit `self._meta_mgr` ctor arg; resolution on background thread | FLOWING |

---

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `resolve_library_sys_ids` importable | `python -c "from shared.fjms_service import resolve_library_sys_ids; print('OK')"` | OK | PASS |
| LIBFILTER-02 service tests (6) | `pytest tests/test_libfilter_catalog.py -q` | 6 passed | PASS |
| LIBFILTER-01 web search tests (7) | `pytest tests/test_libfilter_web_search.py -q` | 7 passed | PASS |
| LIBFILTER-03 desktop tests (3) | `pytest tests/test_libfilter_desktop.py -q` | 3 passed | PASS |
| GUARD-02: SEED-023 unchanged | `pytest tests/test_seed023_catalog_filters.py tests/test_pgp_filter_cascade.py -q` | 15 passed | PASS |
| Phase 87 safe_storage invariant | `pytest tests/test_no_raw_storage_access.py -q` | 6 passed | PASS |
| All 6 Hebrew translation keys present | `python -c "from genizah_translations import TRANSLATIONS; ..."` | NONE missing | PASS |
| Ruff on all modified files | `ruff check shared/fjms_service.py web/pages/search.py web/pages/catalog_browse.py genizah_app.py` | All checks passed | PASS |

---

## Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|---------|
| LIBFILTER-01 | 129-02 | Web search library multi-select, full pre-render filtering, safe_storage, i18n | SATISFIED | `_apply_library_filter` wired in all cascade paths; 6 routing predicates widened; persistence confirmed; Hebrew keys present; 7 tests green |
| LIBFILTER-02 | 129-01, 129-03 | Push-down into `get_browse_results` before COUNT/LIMIT; compose with PGP/Editions; web catalog UI | SATISFIED | EXISTS clause before `where =`; `resolve_library_sys_ids` shared helper; web catalog fully wired; 6 service tests + catalog tests green |
| LIBFILTER-03 | 129-04 | Desktop catalog library filter at parity | SATISFIED | `_CatalogRefreshWorker` extended; state + widget + chips; explicit meta_mgr; 3 gui tests green |
| GUARD-02 | all plans | Zero behavior change — existing suites pass at every phase boundary | SATISFIED | `test_seed023_catalog_filters.py` 11/11, `test_pgp_filter_cascade.py` 4/4, `test_fjms_service.py` 109/109, `test_catalog_availability_filter.py` 4/4 — all green |

---

## Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|---------|--------|
| `web/pages/catalog_browse.py` | ~977-985 | `clear_library_code` mutates list in-place (`lst.remove(code)`) while `_on_library_filter_change` reassigns a fresh list | Info (IN-02 from code review) | None functional — single-threaded async, value re-synced; inconsistent style only; deferred as non-blocking |
| `web/pages/search.py` | ~3540 | `from collections import Counter` inside `_compute_library_facets` body (runs on every facet recompute) | Info (IN-01 from code review) | Harmless; minor perf nit; deferred as non-blocking |
| `genizah_app.py` | ~9854 | Desktop library menu labels fixed at construction time (no live language switch) | Info (IN-03 from code review) | Consistent with desktop's existing "language change = restart" posture; not a regression |

No blockers found. All Critical (CR-01) and Warning (WR-01 through WR-04) findings from the execute-phase code review were fixed and committed before this verification. Commits: `0b5bb13d` (CR-01 i18n), `6fd016a2` (WR-01 list copy), `2e9e0e61` (WR-02 LOCAL exclusion), `3dbaea71` (WR-03 collision-free token), `aeaa095c` (WR-04 word-search count indicator).

---

## Human Verification Required

### 1. Web Search Library Filter — Visual + RTL smoke test

**Test:** Open the web app under Hebrew UI. Run any search returning results from multiple libraries (e.g. search for a common Hebrew word). Click the library filter button (should say "סינון לפי ספרייה"). Select a single library (e.g. CUL). Verify the result count drops and only CUL records appear. Check that a chip appears labeled "ספרייה: קיימברידג'" (or similar Hebrew library name). Click × on the chip to restore all results. Also verify that the results-count indicator shows the filter is active.

**Expected:** Filter applies over the full pre-paginated set (items beyond the first 50 are also filtered); chip displays in Hebrew with no English leakage; removing chip restores full count; button label changes to show number of selected libraries; the dropdown shows per-library facet counts from the pre-filter full set; 0-match libraries are hidden from the dropdown.

**Why human:** NiceGUI async rendering, RTL chip text, visual filter feedback, and per-library facet count display cannot be verified headlessly.

### 2. Web Browse-by-Identification — Push-down total correctness

**Test:** Open the web app's Browse-by-Identification (catalog) page. Select a library from the library filter dropdown. Note the total record count. Then add a PGP filter ("Has PGP"). Verify the total changes and reflects the intersection. Paginate to page 2 and verify the records shown are consistent with the filter.

**Expected:** Total reflects the full filtered set before pagination (not just the visible page); composing library + PGP/Editions filters correctly narrows via the SQL push-down; pagination is correct across all pages; no English text appears in the filter UI under Hebrew locale.

**Why human:** Cannot verify SQL push-down result on real FJMS corpus data, or check that total/pagination is correct without running the live web app.

### 3. Desktop Catalog Library Filter — Visual smoke test

**Test:** On the desktop app under Hebrew UI, open the catalog Browse-by-Identification tab. Click the "כל הספריות" button. Verify the dropdown menu shows library names in Hebrew (no "MY LIBRARY" / "LOCAL" option). Select CUL. Verify the catalog refreshes and the total/results reflect CUL records only. Verify the chip label says the library name in Hebrew. Click × to remove; verify results restore. Also confirm the filter composes correctly with the existing PGP/Editions availability filters.

**Expected:** Filter applied via background worker (no UI freeze); "My Library" / LOCAL option NOT present in the menu; button label changes to "ספריות (1)"; chip appears; removing chip restores all records; no English text anywhere in the library filter UI.

**Why human:** Cannot test PyQt6 widget rendering, QMenu interaction, or chip click behavior headlessly; cannot verify Hebrew/RTL label rendering on the desktop.

---

## Gaps Summary

The data layer is sound — all 5 must-have truths (push-down before COUNT/pagination, full-set filtering, persistence, desktop parity wiring, additive backward-compat) are VERIFIED by codebase evidence and passing tests; both Codex gates passed; the code-review BLOCKER + 4 Warnings were fixed.

**However, the human smoke test (2026-06-28) found 8 UX gaps — the filter CONTROL design is wrong.** The implemented dropdown/menu must be redesigned to a checkbox dialog mirroring the existing "Filter by Domains" feature, plus several behavior bugs. Status flipped to `gaps_found`. Remediation map (file:line + pattern to follow) below.

## Gaps (UAT 2026-06-28)

| ID | Surface | Issue | Root cause / fix location | Pattern to follow | status |
|----|---------|-------|---------------------------|-------------------|--------|
| GAP-A | web /search | Library filter button never renders | `web/pages/search.py:1689` inits via `.set_visibility(False)` (display:none + NiceGUI `_visible=False`) but reveal path uses `_set_btn_visible()` (CSS visibility) — mechanisms conflict. Use `_set_btn_visible` consistently. | siblings at `search.py:1491,1534` | failed |
| GAP-B | web /search | Clicking "Filter by Domains" opens BOTH menus | `ui.menu()` at `search.py:1691` is a bare sibling in the shared button row → Quasar anchors it to the whole row. Removed when switching to dialog (GAP-C). | history menu wrapped in own `ui.column()` `search.py:610-616` | failed |
| GAP-C | web /search | Control should be a checkbox DIALOG (users want to hide specific libraries), not a menu | Replace `ui.menu` + `_rebuild_library_menu()`/`_toggle_library_code()`/`_update_library_btn()` (`search.py:1599-1657`) with `_open_library_filter_dialog()` | `_open_domain_filter_dialog()` `search.py:3170-3410` (ui.dialog + raw-HTML checkboxes + JS readback) | failed |
| GAP-D | web /search | Chips render in the pre-search "search only in…" bar; should be in the post-search filter area (button shows only after a search across all libraries) | library chips appended in `_update_chip_bar()` `search.py:1292-1304` (the pre-search `chip_bar_container` at :1086). Move to a post-search chip row near `results_header`. | results_header region | failed |
| GAP-E | web catalog (Browse by Identification) | Should be a checkbox dialog, not the `ui.select` dropdown | `catalog_browse.py:1350-1369` (`ui.select multiple`) → button + `_open_library_filter_dialog()` | web search domain dialog | failed |
| GAP-F | web catalog | "Search in these results" broken / disabled when libraries selected | `_build_incoming_filters()` `catalog_browse.py:1142-1165` and `_has_active_filters()` `:1129-1140` omit `current_library_filter`; receiving `search.py` must consume `incoming.get('library_filter')` | existing domain/author handling in same fns | failed |
| GAP-G | desktop catalog tab | Should use checkboxes, not a QPushButton+QMenu | `genizah_app.py:9837-9861` (+ `_catalog_toggle_library` 10440, `_catalog_update_library_filter_btn` 10455) → checkbox widget/dialog | `DomainFilterDialog` `desktop/dialogs_filter.py:533-590` (flat checkable QListWidget) | failed |
| GAP-H | desktop catalog tab | "Search within these results" drops the library filter | `_catalog_build_browse_filters()` `genizah_app.py:10697-10710` omits `_catalog_library_filter`; also `_catalog_parallels_in_results()` `:10732+`; consume on the search-tab side | existing domain/author/date handling in same fn | failed |

**Design decisions (locked 2026-06-28 with user):**
1. **Execution:** gap-closure planning, Codex-gated (user choice).
2. **Web search + web catalog filter control:** a `ui.dialog` checkbox list mirroring "Filter by Domains" (`_open_domain_filter_dialog`) — exclusion semantics (all libraries shown by default, uncheck to hide), backed by the EXISTING inclusion push-down/post-filter (pass the still-checked set as `library_codes`; all-checked ⇒ no filter / None). Replaces the `ui.menu` (search) and `ui.select` (catalog).
3. **Desktop catalog widget:** DEFAULT = button on the catalog filter bar → checkbox `QDialog` (flat checkable `QListWidget`, mirroring `DomainFilterDialog`), consistent with web's dialog + the existing button-bar layout. (User did not select between this and an inline panel in the smoke-test question; confirm during gap-discuss — default stands if unaddressed.)
4. **Web search chip placement:** post-search filter chip area near `results_header`, NOT the pre-search "search only in…" `chip_bar_container`.
5. **"Search within these results":** MUST thread the library selection — web catalog (`_build_incoming_filters` + `_has_active_filters` + receiving `search.py` consume `incoming.get('library_filter')`) and desktop (`_catalog_build_browse_filters` + `_catalog_parallels_in_results`).

---

_Verified: 2026-06-28_
_Verifier: Claude (gsd-verifier); gaps appended by execute-phase orchestrator after human smoke test_
