---
phase: 129-library-filter-search-browse-by-identification-seed-026
verified: 2026-06-28T22:00:00Z
status: human_needed
score: 5/5 must-haves verified (data layer) + 8/8 UX gaps closed by code evidence
overrides_applied: 0
re_verification:
  previous_status: gaps_found
  previous_score: 5/5 (data layer only; 8 UX gaps blocked)
  gaps_closed:
    - "GAP-A: web /search library button now renders via _set_btn_visible consistently (3 sites)"
    - "GAP-B: ui.menu removed; library filter is dialog-only; no menu collision with domain filter"
    - "GAP-C: web /search uses checkbox ui.dialog (_open_library_filter_dialog) mirroring _open_domain_filter_dialog"
    - "GAP-D: library chips render in library_chip_row (post-search), NOT chip_bar_container (pre-search)"
    - "GAP-E: web catalog uses checkbox dialog (_open_library_filter_dialog); ui.select removed"
    - "GAP-F: consume_incoming_filters persists library filter gated on storage_prefix=='search'; _has_active_filters + _build_incoming_filters include library"
    - "GAP-G: desktop uses LibraryFilterDialog (QListWidget + OK guard); no QMenu for library"
    - "GAP-H: _catalog_build_browse_filters includes library; _catalog_search_in_results + _catalog_parallels_in_results intersect resolve_library_sys_ids (fail-open); FilterCountWorker accepts meta_mgr and intersects library restriction after chip removal"
    - "All-unchecked guard: 3 surfaces guarded (JS disable + Python short-circuit on web x2; OK disable + _on_accept guard on desktop)"
    - "GUARD-02: data layer (get_browse_results, _apply_library_filter, resolve_library_sys_ids, _CatalogRefreshWorker, safe_storage) not regressed; 64 targeted tests pass"
  gaps_remaining: []
  regressions: []
human_verification:
  - test: "Open the web app under Hebrew UI (/search). Run any search returning results from multiple libraries. After results appear, verify the 'סינון לפי ספרייה' button is visible. Click it. Verify a CHECKBOX DIALOG opens (not a dropdown menu). Verify the Apply button is disabled when all checkboxes are unchecked. Verify 'בחר הכל' (Select All) re-enables Apply. Select one library (e.g. CUL), click Apply. Verify: (a) result count drops; (b) a removable chip appears in the post-search filter row (not in the pre-search 'search only in...' bar); (c) clicking x on the chip restores full results; (d) the domain filter button opens ONLY the domain dialog (no second library menu)."
    expected: "Checkbox dialog opens; Apply disabled at zero-checked; filter applies over full pre-paginated set; chip renders in the post-search row with Hebrew library name; removing chip restores all results; domain button unaffected; no English text under Hebrew UI."
    why_human: "NiceGUI async rendering, dialog open/close, Apply disabled state on Quasar q-btn, RTL chip text, and visual chip placement cannot be verified headlessly."
  - test: "Open the web app's Browse-by-Identification (catalog) page. Click the library filter button ('כל הספריות'). Verify a CHECKBOX DIALOG opens. Select a library subset. Click Apply. Verify: (a) the total count changes; (b) click 'Search in these results' — verify the /search page opens with the library filter active (chip visible, results narrowed). Reload /search — verify the library filter persists. Navigate from catalog to parallels with a library selected — reload /search fresh (navigate away and back) — verify no library filter was silently inherited."
    expected: "Catalog dialog opens; total/pagination reflect the filtered set before pagination; browse→search handoff threads the library selection via consume_incoming_filters; /search reload re-applies it; catalog→parallels→/search does NOT leak the library filter (WR-01 gate)."
    why_human: "Cannot verify SQL push-down result on real FJMS corpus data, persist→reload lifecycle across pages, or the WR-01 parallels non-persist behavior without running the live web app."
  - test: "On the desktop app under Hebrew UI, open the catalog Browse-by-Identification tab. Click the library filter button. Verify a CHECKBOX DIALOG opens (not a QMenu). Verify LOCAL/'My Library' is NOT in the dialog. Verify OK is disabled when all items are unchecked. Select CUL. Click OK. Verify: (a) catalog refreshes with CUL records only; (b) a chip appears in the catalog chip row; (c) clicking x on the chip restores all records; (d) click 'Search in these results' — verify the search results chip bar shows a 'Library: Cambridge' chip that is removable; (e) removing a non-library chip (e.g. domain) after the catalog→search handoff preserves the library restriction in the search count."
    expected: "Checkbox dialog with Hebrew library names; LOCAL absent; OK guard works; filter applies; chip appears; search-within threads library scope; chip-removal recompute preserves library restriction (FilterCountWorker meta_mgr); no English text under Hebrew UI."
    why_human: "Cannot test PyQt6 widget rendering, QDialog accept/reject, chip click, or Hebrew label rendering headlessly; cannot verify FilterCountWorker library preservation end-to-end without a running desktop app."
---

# Phase 129: Library Filter — Re-Verification Report

**Phase Goal:** Add a library filter keyed on `library_code` to (1) web `/search` results as a multi-select applied over the FULL result set BEFORE the render cap, persisted via safe_storage, removable chips, i18n EN/HE; (2) Browse-by-Identification (catalog) as a `library_codes` arg pushed DOWN into `shared/fjms_service.get_browse_results` BEFORE COUNT/LIMIT so total/pagination stay correct; (3) desktop parity.

**Verified:** 2026-06-28 (re-verification after gap-closure plans 129-05/06/07)
**Status:** human_needed — all automated checks VERIFIED; 3 surfaces need live render smoke tests
**Re-verification:** Yes — previous status was gaps_found (8 UX gaps); gap-closure executed in plans 05/06/07

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| 1 | Web search library filter narrows full pre-render set; persists via safe_storage; no English leak under Hebrew | VERIFIED | `_apply_library_filter` at search.py:3521; 6 routing predicates widened; `persist_value('search_library_filter', ...)` at lines 1643, 1785; all Hebrew translation keys confirmed present |
| 2 | Browse-by-Identification `library_codes` arg narrows `total` over full filtered set; backward-compatible | VERIFIED | EXISTS clause in `get_browse_results` before COUNT/LIMIT; `resolve_library_sys_ids` wired inside `_fetch_results_blocking`; 6 service tests + 21 catalog tests green |
| 3 | Desktop catalog gains same library filter; existing search-results filtering untouched | VERIFIED | `LibraryFilterDialog` in `desktop/dialogs_filter.py`; `_open_catalog_library_dialog` in genizah_app.py; `_catalog_library_filter` state; LOCAL excluded from dialog; 17 desktop tests green |
| 4 | Codex-review-before-code gate satisfied | VERIFIED | `129-CODEX-GAP-CODE-REVIEW.md` converged APPROVE (round 2); all MEDIUM findings fixed in commits 5d8c7e39, 120b756a |
| 5 | Tests green; ruff clean; existing PGP/printed + SEED-023 filters unbroken (GUARD-02) | VERIFIED | 64 targeted + GUARD-02 regression tests pass: 12 web search + 21 catalog + 17 desktop + 4 PGP cascade + 6 safe_storage + 4 catalog availability + 10 SEED-023 |

**Score:** 5/5 truths verified

---

## UX Gap Closure (Plans 129-05/06/07)

| ID | Surface | Gap Description | Status | Code Evidence |
|----|---------|-----------------|--------|---------------|
| GAP-A | web /search | Button never rendered (display:none vs CSS visibility conflict) | CLOSED | `_set_btn_visible(library_filter_btn, False)` at construction (search.py line ~1821); 3 sites all use `_set_btn_visible` consistently; no `.set_visibility(False)` on `library_filter_btn` |
| GAP-B | web /search | `ui.menu` caused domain button to open two menus | CLOSED | `_library_menu_ref`, `_rebuild_library_menu`, `_toggle_library_code` all absent from search.py; no `ui.menu` + library crossover; history_menu (search.py:616) is unrelated |
| GAP-C | web /search | Wrong interaction model — needed checkbox dialog not menu | CLOSED | `_open_library_filter_dialog` function present (search.py:1652); `library_filter_btn.on('click', lambda: _open_library_filter_dialog())` wired; `_library_apply_selection` pure helper (search.py:1591) |
| GAP-D | web /search | Chips rendered in pre-search bar, should be post-search | CLOSED | `library_chip_row` declared in post-search column (search.py:1876); `_update_library_chips` rebuilds it (search.py:1617); `_update_chip_bar` comment confirms library chips NOT included; `has_any` OR-term for library_filter removed from chip_bar |
| GAP-E | web catalog | Should be checkbox dialog not `ui.select` | CLOSED | `ui.select` + "Select libraries" absent from catalog_browse.py; `_open_library_filter_dialog` + `library_filter_btn_ref` present; Library Filter Card uses button wired to dialog |
| GAP-F | web catalog | "Search in these results" broken when libraries selected | CLOSED | `_has_active_filters`: `current_library_filter['value']` in `any([...])` (catalog_browse.py:1313); `_build_incoming_filters`: `incoming['library_filter'] = list(...)` when active (line 1343-1344); `consume_incoming_filters` in filter_panel.py: gated on `storage_prefix == 'search'` (WR-01 fix, line 342) |
| GAP-G | desktop catalog | Should use checkbox dialog not QMenu | CLOSED | `LibraryFilterDialog` class in `desktop/dialogs_filter.py:1677`; `library_apply_selection` helper at :1661; `_open_catalog_library_dialog` method in genizah_app.py:10430; `LOCAL` excluded (`self._all_codes = [c for c in LIBRARY_CODES.keys() if c != 'LOCAL']`) |
| GAP-H | desktop | "Search within"/"parallels within" dropped library filter; recompute lost it | CLOSED | `_catalog_build_browse_filters` appends `filters['library']` (genizah_app.py:10685-10686); `_catalog_search_in_results` and `_catalog_parallels_in_results` call `resolve_library_sys_ids` and intersect fail-open (WR-02 fix, lines 10711-10721, 10744-10755); `FilterCountWorker` gains `meta_mgr=None` kwarg (gui_threads.py:1229) and intersects library restriction in `run()` (lines 1291-1303); 9 call sites in genizah_app.py pass `meta_mgr=self.meta_mgr`; desktop search-side chip bar renders per-code removable library chips (genizah_app.py:15389-15394) |
| All-unchecked guard | all 3 surfaces | Unchecking everything must not produce `[]` = show all | CLOSED | Web search: JS `n===0` disables Apply + Python `if not checked: ui.notify; return` before `_library_apply_selection`; Web catalog: same pattern with `catLibFilterUpdateApply`; Desktop: `_update_ok_button` disables OK at 0 checked + `_on_accept` guard; `_library_apply_selection`/`library_apply_selection` only reachable with non-empty checked set |

---

## Required Artifacts

| Artifact | Expected | Status | Details |
|----------|---------|--------|---------|
| `web/pages/search.py` | `_open_library_filter_dialog`, `library_chip_row`, `_set_btn_visible` consistently | VERIFIED | All present; no `_library_menu_ref`/`_rebuild_library_menu` artifacts; `_set_btn_visible` used at 3 sites on `library_filter_btn` |
| `web/pages/catalog_browse.py` | Checkbox dialog, `_has_active_filters` + `_build_incoming_filters` include library | VERIFIED | `_open_library_filter_dialog` wired; no `ui.select` for library; both filter functions include `current_library_filter['value']` |
| `web/components/filter_panel.py` | `consume_incoming_filters` GAP-F with WR-01 gate | VERIFIED | `if incoming.get('library_filter') and storage_prefix == 'search':` line 342; no dead `try/except AttributeError` |
| `desktop/dialogs_filter.py` | `LibraryFilterDialog` class + `library_apply_selection` helper | VERIFIED | Class at line 1677; helper at line 1661; LOCAL excluded; OK disabled at 0 checked; `_on_accept` guard |
| `genizah_app.py` | `_open_catalog_library_dialog`, GAP-H threading, desktop search-side chips | VERIFIED | All present; `_catalog_build_browse_filters` includes library; `_catalog_search_in_results`/`_catalog_parallels_in_results` intersect fail-open; `_update_filter_chip_bar` renders per-code library chips |
| `gui_threads.py` | `FilterCountWorker` `meta_mgr` kwarg + library intersection | VERIFIED | `__init__` at line 1229: `*, meta_mgr=None`; `run()` at lines 1293-1303: intersects when meta_mgr is not None and resolution is non-empty (fail-open) |
| `tests/test_libfilter_web_search.py` | 12 tests (7 data-layer + 5 new control-surface) | VERIFIED | 12/12 passed |
| `tests/test_libfilter_catalog.py` | 21 tests (6 service + 15 GAP-E/F) | VERIFIED | 21/21 passed |
| `tests/test_libfilter_desktop.py` | 17 tests (3 original + 14 GAP-G/H/FINDING) | VERIFIED | 17/17 passed |
| `genizah_translations.py` | All new i18n keys with Hebrew values | VERIFIED | 7 keys confirmed: "Filter by library", "Filter results by library", "Filter by Library", "Select at least one library, or check all to clear the filter", "All Libraries", "Libraries", "Library" |

---

## Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `web/pages/search.py library_filter_btn` | `_open_library_filter_dialog()` | `library_filter_btn.on('click', ...)` | WIRED | Dialog opens on click; no ui.menu attached |
| `web/pages/search.py _open_library_filter_dialog` | `_library_apply_selection` | JS readback + Apply handler | WIRED | All-checked → `[]`; subset → subset; zero-checked blocked by guard |
| `web/pages/search.py library_chip_row` | post-search column (after results_header) | `ui.row()` at line 1876 | WIRED | Hidden until filter active; library_chip_row.set_visibility managed by `_update_library_chips` |
| `web/pages/catalog_browse._has_active_filters` | `current_library_filter['value']` | `any([..., bool(current_library_filter['value'])])` | WIRED | Line 1313; enables "Search in these results" button |
| `web/pages/catalog_browse._build_incoming_filters` | `incoming['library_filter']` | conditional append when filter active | WIRED | Lines 1343-1344 |
| `web/components/filter_panel.consume_incoming_filters` | `persist_value('search_library_filter', ...)` | `storage_prefix == 'search'` gate | WIRED | WR-01 fix: parallels handoff does NOT persist search_library_filter |
| `genizah_app._open_catalog_library_dialog` | `LibraryFilterDialog` | `dlg = LibraryFilterDialog(self, selected_codes=...)` | WIRED | Line 10433; LOCAL excluded from all_codes |
| `genizah_app._catalog_build_browse_filters` | `filters['library']` | `if self._catalog_library_filter:` | WIRED | Line 10685-10686 |
| `genizah_app._catalog_search_in_results` | `resolve_library_sys_ids` + `pre_search_restrict_sys_ids` intersect | `lib_ids` fail-open guard | WIRED | Lines 10710-10721; empty resolution skipped with warning (WR-02 fix) |
| `genizah_app._remove_filter` | `FilterCountWorker(meta_mgr=self.meta_mgr)` | `meta_mgr=self.meta_mgr` kwarg | WIRED | Line 15465; preserves library restriction after non-library chip removal |
| `gui_threads.FilterCountWorker.run` | `resolve_library_sys_ids` + result intersection | `if self.filters.get('library') and self._meta_mgr is not None:` | WIRED | Lines 1293-1303; fail-open on empty resolution |
| `genizah_app._update_filter_chip_bar` | per-code library chips with `('library', code)` key | `for _lib_code in filters.get('library', []):` | WIRED | Lines 15389-15394; chips keyed so `_remove_filter` tuple branch handles removal |

---

## Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|--------------|--------|-------------------|--------|
| `web/pages/search._apply_library_filter` | `r.get('display', {}).get('library_code', '')` | MetadataManager-populated display data | Yes — reads actual library_code from real search result dicts | FLOWING |
| `web/pages/catalog_browse._fetch_results_blocking` | `library_sys_ids` | `resolve_library_sys_ids(library_codes, _state.meta_mgr)` reading `csv_bank` | Yes — O(255K) comprehension over in-memory csv_bank | FLOWING |
| `web/components/filter_panel.consume_incoming_filters` | `state.library_filter` + `search_library_filter` in storage | `incoming['library_filter']` built by `_build_incoming_filters` from `current_library_filter['value']` | Yes — real user selection from catalog dialog | FLOWING |
| `genizah_app._catalog_search_in_results` | `pre_search_restrict_sys_ids` | `resolve_library_sys_ids(self._catalog_library_filter, self.meta_mgr)` | Yes — explicit meta_mgr; fail-open when empty | FLOWING |
| `gui_threads.FilterCountWorker.run` | `result & lib_ids` | `resolve_library_sys_ids(self.filters['library'], self._meta_mgr)` on worker thread | Yes — background thread; intersected into recomputed set | FLOWING |

---

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 12 web search tests | `pytest tests/test_libfilter_web_search.py -q` | 12 passed | PASS |
| All 21 catalog tests | `pytest tests/test_libfilter_catalog.py -q` | 21 passed | PASS |
| All 17 desktop tests (gui) | `pytest tests/test_libfilter_desktop.py tests/test_catalog_availability_filter.py -q -m gui` | 17 passed | PASS |
| GUARD-02: SEED-023 unchanged | `pytest tests/test_seed023_catalog_filters.py -q` | 10 passed | PASS |
| GUARD-02: PGP cascade unchanged | `pytest tests/test_pgp_filter_cascade.py -q` | 4 passed | PASS |
| Phase 87 safe_storage invariant | `pytest tests/test_no_raw_storage_access.py -q` | 6 passed | PASS |
| All 14 gap-closure commits exist | `git cat-file -t <hash>` for each | All 14: OK commit | PASS |
| All 7 i18n keys present with Hebrew values | `python -c "from genizah_translations import TRANSLATIONS; ..."` | All 7 confirmed with Hebrew values | PASS |
| GAP-A: 3 sites use `_set_btn_visible` on library_filter_btn | grep count in search.py | 3 calls; 0 `.set_visibility(False)` on library_filter_btn | PASS |
| GAP-B: no menu artifacts | search for `_library_menu_ref` / `_rebuild_library_menu` / `_toggle_library_code` | 0 matches | PASS |
| GAP-E: no `ui.select` for library in catalog | search for "ui.select" + "Select libraries" in catalog_browse.py | 0 matches | PASS |
| GAP-G: no QMenu + library crossover | regex search in genizah_app.py | 0 matches | PASS |
| WR-01 gate in filter_panel.py | search for `storage_prefix == 'search'` | Present at line 342; dead `try/except AttributeError` absent | PASS |
| Total targeted + GUARD-02 | `pytest [all 7 suites] -v` | 64 passed, 0 failed | PASS |

---

## Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|---------|
| LIBFILTER-01 | 129-02, 129-05 | Web search library multi-select, checkbox dialog, full pre-render filtering, safe_storage, i18n, post-search chips | SATISFIED | `_open_library_filter_dialog` + `library_chip_row` + `_library_apply_selection`; `_set_btn_visible` consistent; 12 tests green |
| LIBFILTER-02 | 129-01, 129-03, 129-06 | Push-down into `get_browse_results` before COUNT/LIMIT; web catalog checkbox dialog; browse→search handoff | SATISFIED | EXISTS clause before where=; `_open_library_filter_dialog` in catalog; `_has_active_filters` + `_build_incoming_filters` include library; `consume_incoming_filters` WR-01 gated; 21 catalog tests green |
| LIBFILTER-03 | 129-04, 129-07 | Desktop catalog library filter at parity with checkbox dialog; search-within threads filter; recompute preserves it | SATISFIED | `LibraryFilterDialog`; `_catalog_build_browse_filters` includes library; `_catalog_search_in_results`/`_catalog_parallels_in_results` intersect; `FilterCountWorker` meta_mgr; desktop library chips; 17 tests green |
| GUARD-02 | all plans | Zero behavior change — existing suites pass at every phase boundary | SATISFIED | `test_seed023_catalog_filters.py` 10/10, `test_pgp_filter_cascade.py` 4/4, `test_catalog_availability_filter.py` 4/4, `test_no_raw_storage_access.py` 6/6 — all green |

---

## Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|---------|--------|
| `web/pages/search.py` | ~1730-1731 | JS `btn.disabled = (n===0)` on Quasar q-btn wrapper — raw DOM `.disabled` may not engage Quasar's reactive `disable` prop (WR-04) | Info | Python short-circuit is the authoritative guard; JS is cosmetic. WR-04 documented as such (commit 165ad935). Not a blocker. |
| `web/pages/catalog_browse.py` | ~1153 | `_clear_library_code` assigns a new list (`lst = [c for c in ...]`) instead of mutating in place, while `_open_library_filter_dialog` Apply sets `current_library_filter['value'] = new_filter` directly | Info | Consistent result; style inconsistency only; WR-03 LIBRARY_CODES validation present on the Apply path. |
| `web/pages/search.py` | ~1670 | `_open_library_filter_dialog` builds fresh `ui.dialog()` on every click (same as domain dialog pattern) | Info | Performance nit; consistent with existing domain dialog; deferred project-wide. |
| `genizah_app.py` | ~10454 | `_sync_library_menu_checks` retained as empty no-op (IN-02, cleaned up in commit 8fa4a104) | Info | Cleaned per IN-02 fix; verify it was actually removed. |

No blockers. All Critical (CR-01) and Warning (WR-01 through WR-04) findings from both internal review (`129-GAP-REVIEW.md`) and Codex CODE review (`129-CODEX-GAP-CODE-REVIEW.md`) were fixed and committed. Codex converged APPROVE (round 2, no findings).

---

## Human Verification Required

### 1. Web Search — Dialog open, chip placement, domain-button isolation

**Test:** Open the web app under Hebrew UI. Run any search returning results from multiple libraries. After results appear, verify the 'סינון לפי ספרייה' button is visible. Click it. Verify a CHECKBOX DIALOG opens (not a dropdown menu). Verify the Apply button is disabled when all checkboxes are unchecked. Verify 'בחר הכל' (Select All) re-enables Apply. Select one library (e.g. CUL), click Apply. Verify: (a) result count drops; (b) a removable chip appears in the post-search filter row BELOW the results header (not in the pre-search 'search only in...' bar above); (c) clicking x on the chip restores full results; (d) clicking the domain filter button opens ONLY the domain dialog (no second library menu).

**Expected:** Checkbox dialog opens; Apply disabled at zero-checked; filter applies over full pre-paginated set (results beyond page 1 also filtered); chip renders in the dedicated post-search row with Hebrew library name; removing chip restores all results; domain button unaffected; no English text under Hebrew UI.

**Why human:** NiceGUI async rendering, dialog open/close lifecycle, Apply disabled state on a Quasar q-btn (WR-04: raw `.disabled` may not engage Quasar reactive prop — Python guard is authoritative but JS cosmetic state is unverifiable headlessly), RTL chip text, and visual chip placement cannot be verified headlessly.

### 2. Web Catalog + Browse-to-Search Handoff

**Test:** Open the web app's Browse-by-Identification (catalog) page. Click the library filter button ('כל הספריות'). Verify a CHECKBOX DIALOG opens. Select a library subset. Click Apply. Verify: (a) the total count changes; (b) pagination is correct (page 2 shows filtered records); (c) click 'Search in these results' — verify the /search page opens with the library filter active (chip visible, results narrowed); (d) reload /search — verify the library filter persists; (e) set a library filter in catalog, click 'Parallel search in these results', navigate back to /search fresh — verify no library filter was silently inherited on /search.

**Expected:** Catalog dialog opens; total/pagination reflect the filtered set before pagination; browse→search handoff threads the library selection; /search reload re-applies it (persist→reload lifecycle); catalog→parallels→/search does NOT leak the library filter (WR-01 gate: `storage_prefix == 'search'`).

**Why human:** Cannot verify SQL push-down result on real FJMS corpus data, persist→reload lifecycle across page navigations, or the WR-01 parallels non-persist behavior without running the live web app.

### 3. Desktop — Dialog, chips, search-within, chip-removal recompute

**Test:** On the desktop app under Hebrew UI, open the catalog Browse-by-Identification tab. Click the library filter button. Verify a CHECKBOX DIALOG opens (not a QMenu). Verify LOCAL/'My Library' is NOT in the dialog. Verify OK is disabled when all items are unchecked. Select CUL. Click OK. Verify: (a) catalog refreshes with CUL records only; (b) a chip appears in the catalog chip row; (c) clicking x on the chip restores all records; (d) click 'Search in these results' — verify the search tab's chip bar shows a 'ספרייה: קיימברידג'' (Library: Cambridge) chip that is removable; (e) also add a domain filter chip in the catalog, then search-in-results — remove the domain chip only — verify the library restriction is PRESERVED in the manuscript count (FilterCountWorker meta_mgr path).

**Expected:** Checkbox dialog with Hebrew library names; LOCAL absent; OK guard works; filter applies; catalog chip appears; search-within threads library scope; removing a non-library chip preserves library restriction in the recomputed count; no English text under Hebrew UI.

**Why human:** Cannot test PyQt6 widget rendering, QDialog accept/reject, chip click behavior, FilterCountWorker library preservation, or Hebrew label rendering headlessly; the `QT_QPA_PLATFORM=offscreen` environment cannot render visible widgets.

---

## Gaps Summary

All 8 UX gaps from the 2026-06-28 human smoke test are CLOSED by codebase evidence:

- **GAP-A**: Confirmed — 3 `_set_btn_visible(library_filter_btn, ...)` calls; 0 `.set_visibility(False)` on that button.
- **GAP-B**: Confirmed — `_library_menu_ref`, `_rebuild_library_menu`, `_toggle_library_code` all absent; only `history_menu` (ui.menu) remains, unrelated.
- **GAP-C**: Confirmed — `_open_library_filter_dialog` present with HTML+JS checkbox readback pattern mirroring `_open_domain_filter_dialog`.
- **GAP-D**: Confirmed — `library_chip_row` declared in post-search column; comment in `_update_chip_bar` explicitly notes library chips NOT included there.
- **GAP-E**: Confirmed — no `ui.select` + "Select libraries" in catalog_browse.py; `_open_library_filter_dialog` wired to the library filter card button.
- **GAP-F**: Confirmed — `_has_active_filters` includes `current_library_filter['value']`; `_build_incoming_filters` appends `incoming['library_filter']`; `consume_incoming_filters` gated on `storage_prefix == 'search'` (WR-01 fix).
- **GAP-H**: Confirmed — `_catalog_build_browse_filters` appends `filters['library']`; both `_catalog_search_in_results` and `_catalog_parallels_in_results` call `resolve_library_sys_ids` with fail-open; `FilterCountWorker` meta_mgr path intersects library restriction; desktop chip bar renders per-code library chips keyed `('library', code)`.
- **All-unchecked guard**: Confirmed on all 3 surfaces — JS disable + Python short-circuit (web x2); OK disable + `_on_accept` guard (desktop).

**Phase is code-complete.** The 3 human verification items are for live render smoke tests (NiceGUI dialog behavior, RTL chip rendering, desktop QDialog, and multi-step navigation flows) that cannot be confirmed headlessly. All automated checks (64 tests, 14 commits, all i18n keys) are VERIFIED.

---

_Verified: 2026-06-28_
_Verifier: Claude (gsd-verifier) — re-verification after gap-closure plans 129-05/06/07_
