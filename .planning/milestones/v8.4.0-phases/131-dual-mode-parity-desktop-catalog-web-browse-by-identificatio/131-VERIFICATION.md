---
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
verified: 2026-06-30T14:00:00Z
status: passed
score: 6/6
overrides_applied: 0
human_verification:
  - test: "Desktop — open Catalog tab, click library filter button, toggle Hide vs Show-only in the dialog, select a subset of libraries, click Apply, reopen the dialog within the same session — confirm the mode and checked set are re-applied (D-04 reset fires when mode is toggled); confirm the button label uses the real pluralized keys and the total does not exceed the selectable count."
    expected: "Dialog reopens with the previously selected mode and codes; toggling mode clears all checkboxes; button shows 'Showing N/M libraries' (Show-only) or 'Hiding N libraries' (Hide) using the real Phase-130 translation keys; total M matches the selectable universe (library_codes_with_manuscripts minus LOCAL, not the broader LIBRARY_CODES count)."
    why_human: "PyQt6 dialog render and in-session state round-trip cannot be exercised headless — Qt is not available in CI without a display server."
  - test: "Desktop — with a Hide library selection active, click 'Search in these results' and 'Composition in these results' — confirm the library restriction is SUPPRESSED (not silently inverted) and a brief status-bar notice appears."
    expected: "Status bar shows 'Library Hide filter not applied to search/composition' (5 s); the search/composition scope is NOT narrowed by the Hide library selection."
    why_human: "Status-bar message display and search-scope correctness require a running Qt application."
  - test: "Web /catalog Browse-by-Identification — open the library filter dialog with NO other filters active, confirm the count-shortlist shows TRUE full-set per-library counts (including libraries whose manuscripts do not appear on the current PAGE_SIZE=50 page); toggle Show-only vs Hide, type in the text-search input, click sort-by-count vs A-Z, Apply a selection, reload the page — confirm mode+set survive the reload."
    expected: "Shortlist shows facet counts from get_browse_library_facets (not page-local counts); off-page libraries appear in the shortlist; toggles and text-search work; dict-shape persists across reload; SEED-023 PGP/Editions filters continue to work alongside the library filter."
    why_human: "NiceGUI async render path cannot be exercised headless; true-facet counts require a live fjms DB connection; page-reload persistence requires a running browser session."
  - test: "Web /catalog — with a Hide library selection active, click 'Search in these results' — confirm the /search page opens with the filter set in Hide mode (not converted to Show-only)."
    expected: "The /search library filter button shows 'Hiding N libraries' (or equivalent Hide state), not 'Showing N/M libraries'; the mode round-trip is preserved through the {mode,codes} handoff."
    why_human: "Page navigation and cross-page state transfer require a live browser session."
  - test: "Web /parallels — run a composition search, open the new library-filter button, select a subset of libraries in Show-only mode with NO advanced filters active, Apply — confirm that results are scoped to the selected libraries (library-only Show-only scope works ungated by _has_active_filters); reload the page — confirm mode+set survive the reload."
    expected: "With no domain/author/work filters, Show-only Apply rescopes the results to the chosen libraries (not a no-op); results outside the selected libraries disappear; the parallels_library_filter key in safe_storage persists across reload."
    why_human: "Library-only Show-only scope correctness and result-set changes require a running browser with a live Tantivy index and meta_mgr."
  - test: "Web /parallels — with Hide mode active, export the results (XLSX/JSON) and confirm exported rows are scoped (do not contain rows from hidden libraries)."
    expected: "Exported results exclude rows from the hidden libraries; Show-only pre-query and Hide post-pre-export scoping both hold."
    why_human: "Export file content requires a live browser session and file download."
---

# Phase 131: Dual-Mode Parity — Desktop Catalog / Web Browse / Web Parallels Verification Report

**Phase Goal:** The (mode + set) dual-mode library-filter model from Phase 130 reaches the three remaining filter surfaces at parity — the desktop catalog `LibraryFilterDialog`, the web Browse-by-Identification catalog filter, and a NEW library-filter control on the web `/parallels` page — each persisted for its surface.

**Verified:** 2026-06-30T14:00:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth (Requirement) | Status | Evidence |
|---|---------------------|--------|----------|
| 1 | DMF-07: Desktop catalog `LibraryFilterDialog` offers Show-only/Hide modes; mode+set persist and re-apply on reopen, at parity with the web lead. | VERIFIED | `desktop/dialogs_filter.py:1780` — `def get_mode()` present; `1784` — `_on_mode_changed(self, *args)` slot present; `1698` — `_all_codes` built from `library_codes_with_manuscripts()` minus `LOCAL`; `genizah_app.py:9604` — `_catalog_library_mode = 'hide'` init; `10440` — dialog constructed with `mode=self._catalog_library_mode`; `10450` — mode read back via `dlg.get_mode()`; `10485-10494` — real Phase-130 pluralized keys used; `10477` — total from `library_codes_with_manuscripts()`. 22/22 `test_libfilter_desktop.py` tests pass. |
| 2 | DMF-08: Web Browse-by-Identification catalog filter offers Show-only/Hide over the full canonical library list, persisted, composing with SEED-023 filters without regression. | VERIFIED | `web/pages/catalog_browse.py:118-137` — 3-branch D-06 migration restore (list/dict/else); `140` — `current_library_mode` cell; `308` — `library_mode=` kwarg passed to `fjms.get_browse_results`; `1066-1081` — 3-state button with real pluralized keys; `1071` — total from `library_codes_with_manuscripts()`; `1299-1311` — dict-shape persist via `safe_user_set('catalog_library_filter', {...})`; `1450` — clear site persists `{'mode':'hide','codes':[]}`; `1544` — `_build_incoming_filters` carries `{'mode':..., 'codes':[...]}` dict; `filter_panel.py:357-373` — `consume_incoming_filters` dict branch reads mode and preserves it. 15/15 `test_catalog_dual_mode_library_filter.py` tests pass; 4/4 `test_catalog_availability_filter.py` (SEED-023) pass; DMF-10 guards pass. |
| 3 | DMF-09: NEW web /parallels library-filter control scopes via restrict_sys_ids; button total uses the selectable-universe (library_codes_with_manuscripts), persisted. | VERIFIED | `web/pages/parallels.py:204-205` — `ParallelsState.library_filter/library_mode` fields; `213` — `_parallels_apply_selection` LOCAL helper defined; `226-268` — `_apply_parallels_library_filter` dual-mode post-fetch filter; `2655-2666` — Show-only pre-query intersect OUTSIDE `_has_active_filters()` block (Codex R3 F4), BEFORE per-ms exclusion subtraction; `2792-2800` — Hide post-fetch BEFORE `set_parallels_export`; `1459-1464` — button rendered; `1513` — total from `library_codes_with_manuscripts()`; `1722` — `safe_user_set('parallels_library_filter', {…})` dict-shape persist; `2487` — reset persists `{'mode':'hide','codes':[]}`. 16/16 `test_parallels_library_filter.py` tests pass. |
| 4 | DMF-10: 'LOCAL' absent from library-filter options on every new web path (sanitize against 'LOCAL', not just LIBRARY_CODES). | VERIFIED | `catalog_browse.py:1071,1142,1150,1296` — inline `c != 'LOCAL'` guards; `parallels.py:1583` + `_apply_parallels_library_filter` lines 262-266 — LOCAL excluded from Show-only pass-through (CR-02 fix `f2dd1cc0`); `filter_panel.py:361` — `sanitize_library_codes` on both dict and list branches; `dialogs_filter.py:1698` — `c != 'LOCAL'` guard at `_all_codes` build. 3/3 `test_web_library_options_no_local.py` + 4/4 `test_phase_97_invariants.py` pass. |
| 5 | DMF-12: Full-set facet counts (get_browse_library_facets) used for the count shortlist. | VERIFIED | `shared/fjms_service.py:2417` — `def get_browse_library_facets` method present with `SELECT DISTINCT c.AlmaId` counting; `catalog_browse.py:351` — called as `fjms.get_browse_library_facets(` instance method (NOT module-level import, per Codex N1); `catalog_browse.py:368` — `sys_id_to_library=(_state.meta_mgr.get_library_for_id if _state.meta_mgr else None)` callable (WR-05 fix `d92c42a7`); catalog dialog contains `Search libraries` text-search input, sort control (`catLibFilterSort`), count-shortlist with `data-count` rows, and expand A-Z section. No page-local Counter feeds the shortlist (Codex R3 F1 — no-count fallback only on `try/except` failure path). 18/18 `test_fjms_browse_library_mode.py` tests pass (incl. duplicate-AlmaId-once, off-page libraries, callable-mapper contract tests). |
| 6 | DMF-13: expand-all / universe uses library_codes_with_manuscripts() minus 'LOCAL'. | VERIFIED | `dialogs_filter.py:1695-1698` — desktop `_all_codes` from `library_codes_with_manuscripts()`; `genizah_app.py:10436-10437` — dialog `all_codes` from `library_codes_with_manuscripts()`; `catalog_browse.py:1148` — `_codes_with_mss = library_codes_with_manuscripts()`; `parallels.py:1583` — `_codes_with_mss = library_codes_with_manuscripts()`; all four surfaces also use the selectable-universe count for the Show-only button `total` (not LIBRARY_CODES count — Codex N4 / CR-01 fix `84f735d1`). |

**Score:** 6/6 truths verified

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `desktop/dialogs_filter.py` | LibraryFilterDialog mode toggle, get_mode(), _on_mode_changed, mode-aware OK, library_codes_with_manuscripts | VERIFIED | Lines 1680-1819: mode param, QButtonGroup with _rb_show_only/_rb_hide, get_mode (1780), _on_mode_changed(*args) (1784) with `not checked` early-return guard (WR-04 fix 3103d8a4), mode-aware _update_ok_button, library_codes_with_manuscripts universe |
| `genizah_app.py` | _catalog_library_mode field, dual-mode dialog, 3-state button, worker threading, Hide-suppress handoff | VERIFIED | 9604: init `'hide'`; 10436-10450: dialog dual-mode; 10462-10494: 3-state button real keys; 502/566: worker library_mode param; 10528/10545: clear-site resets; 10731: Show-only gate; 10773-10816: Hide-suppress with status bar notice |
| `shared/fjms_service.py` | library_mode param on get_browse_results, get_browse_library_facets, _build_browse_conditions | VERIFIED | 2293-2343: library_mode='show_only' default, `NOT EXISTS` branch on `library_mode == "hide"`; 2417-2507: get_browse_library_facets with DISTINCT AlmaId counting, CALLABLE sys_id_to_library, LOCAL exclusion |
| `web/components/filter_panel.py` | consume_incoming_filters accepts {mode,codes} dict, backward-compatible | VERIFIED | 357-373: isinstance dict branch reads mode+codes; bare list still maps to show_only; WR-02 comment fix (77429cac) |
| `web/pages/catalog_browse.py` | dual-mode dialog, 3-state button, true-facet shortlist, dict persist, migration restore, Hide server-side, {mode,codes} handoff | VERIFIED | 118-140: restore migration + mode cell; 308: library_mode= kwarg; 341-368: facet fetch via fjms instance method; 1055-1081: 3-state button; 1113-1315: dialog design; 1539-1546: {mode,codes} handoff |
| `web/pages/parallels.py` | ParallelsState fields, restore, LOCAL helpers, dual-mode button+dialog, HYBRID scoping | VERIFIED | 204-205: fields; 213-220: _parallels_apply_selection; 226-268: _apply_parallels_library_filter (CR-02 LOCAL fix f2dd1cc0); 2655-2666: Show-only UNGATED pre-query; 2792-2800: Hide post-fetch before export |
| `tests/test_libfilter_desktop.py` | DMF-07/DMF-13 desktop dialog tests including mode toggle, handoff | VERIFIED | 22 tests pass; revised in-place with mode='show_only' for legacy tests; Hide-handoff coverage added (Codex N3) |
| `tests/test_catalog_dual_mode_library_filter.py` | DMF-08/DMF-12/DMF-10 catalog tests | VERIFIED | 15 tests pass; pure-mirror + AST source-contract scans; fjms.get_browse_library_facets( call-form scan passes |
| `tests/test_parallels_library_filter.py` | DMF-09/DMF-10 parallels tests | VERIFIED | 16 tests pass; pure-mirror + AST scans; export-ordering (before set_parallels_export) and LOCAL-helper/no-search.py-import scans pass |
| `tests/test_fjms_browse_library_mode.py` | SQL-shape, facet contract, callable mapper | VERIFIED | 18 tests pass; EXISTS/NOT EXISTS dispatch, DISTINCT AlmaId counting, duplicate-once, off-page libraries, callable mapper contract |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `LibraryFilterDialog` | `genizah_app._catalog_library_mode` | `dlg.get_mode()` read back after `exec()` | VERIFIED | `genizah_app.py:10450` — `self._catalog_library_mode = mode` from `dlg.get_mode()` |
| `_CatalogRefreshWorker.run` | `get_browse_results` | `library_mode=` kwarg | VERIFIED | `genizah_app.py:566` — `library_mode=self._library_mode` |
| `_catalog_build_browse_filters` | Search/parallels handoff | Hide suppresses filters['library'] | VERIFIED | `genizah_app.py:10731` — `if ... and self._catalog_library_mode == 'show_only':` |
| `apply_catalog_library_filter` | `safe_user_set('catalog_library_filter', {...})` | dict-shape persist (never bare list) | VERIFIED | `catalog_browse.py:1299-1311` — `safe_user_set('catalog_library_filter', {'mode': ..., 'codes': ...})` |
| `_fetch_results_blocking` | `fjms.get_browse_results` | `library_mode=` kwarg | VERIFIED | `catalog_browse.py:308` — `library_mode=library_mode` |
| Facet fetch | `fjms.get_browse_library_facets` | instance-method call on page fjms handle | VERIFIED | `catalog_browse.py:351` — `fjms.get_browse_library_facets(` (NOT a module import) |
| `_build_incoming_filters` | `consume_incoming_filters` | `{mode,codes}` library_filter dict preserved | VERIFIED | `catalog_browse.py:1544-1546` — dict shape; `filter_panel.py:357-373` — dict branch reads mode |
| Show-only library codes (ungated by _has_active_filters) | `restrict_sys_ids` | `resolve_library_sys_ids` intersect before exclusion subtraction | VERIFIED | `parallels.py:2655-2666` — outside `if _has_active_filters():` block, before `restricted - excluded_manuscript_ids` at 2669 |
| Hide post-fetch filter | `set_parallels_export / safe_user_set('parallels_results')` | `_apply_parallels_library_filter` BEFORE export | VERIFIED | `parallels.py:2792-2800` — filter applied before `set_parallels_export(` call at 2800 |
| `_parallels_apply_selection` | parallels.py (LOCAL helper) | Show-only all-selected normalization — NOT search.py import | VERIFIED | `parallels.py:213` — defined locally; `test_parallels_library_filter.py` scans confirm no `from web.pages.search import _library_apply_selection` |

---

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|--------------|--------|--------------------|--------|
| `catalog_browse.py` library filter | `current_library_facets['value']` | `fjms.get_browse_library_facets(...)` via `io_bound` in `_fetch_library_facets_blocking` | Yes — `SELECT DISTINCT c.AlmaId` over real FJMS DB filtered set | FLOWING |
| `catalog_browse.py` library mode | `current_library_mode['value']` | `safe_user_get('catalog_library_filter', [])` restore + Apply handler | Yes — dict-shape `{'mode','codes'}` from persistent storage | FLOWING |
| `parallels.py` library filter | `p_state.library_filter`, `p_state.library_mode` | `safe_user_get('parallels_library_filter')` restore + Apply handler | Yes — dict-shape persisted per-user | FLOWING |
| `genizah_app.py` library mode | `self._catalog_library_mode` | In-memory init `'hide'` + dialog `get_mode()` | Yes — in-memory per-session (no QSettings/session JSON per Pitfall 1) | FLOWING |

---

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `get_browse_library_facets` exists and is callable | `python -c "from shared.fjms_service import get_fjms_service; import inspect; svc=get_fjms_service(); print(callable(svc.get_browse_library_facets))"` | (not run — FJMS DB not available in this session) | SKIP (DB dependency) |
| `library_mode=hide` routes to `NOT EXISTS` in SQL | `pytest tests/test_fjms_browse_library_mode.py -q` | 18/18 passed | PASS |
| `_apply_parallels_library_filter` LOCAL excludes Show-only LOCAL rows | `pytest tests/test_parallels_library_filter.py::test_apply_parallels_show_only -q` | 1/1 passed | PASS |
| DMF-10 guards across all web paths | `pytest tests/test_web_library_options_no_local.py -q` | 3/3 passed | PASS |
| SEED-023 regression | `pytest tests/test_catalog_availability_filter.py -q` | 4/4 passed | PASS |
| Cross-phase regression (libfilter_catalog) | `pytest tests/test_libfilter_catalog.py -q` | 17/17 passed | PASS |

---

### Probe Execution

Step 7c: SKIPPED — no probe-*.sh files declared in this phase's plans. The phase's primary verification is test-suite-based (270 tests per orchestrator).

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| DMF-07 | Plans 01, 03 | Desktop catalog LibraryFilterDialog dual-mode, persisted, at parity | SATISFIED | `dialogs_filter.py` mode toggle; `genizah_app.py` threading; 22 desktop tests pass |
| DMF-08 | Plans 01, 04 | Web Browse-by-Identification catalog filter dual-mode, persisted, composing with SEED-023 | SATISFIED | `catalog_browse.py` full redesign; `filter_panel.py` dict branch; 15 catalog + 4 SEED-023 tests pass |
| DMF-09 | Plans 01, 05 | Web /parallels NEW library-filter control, scopes via restrict_sys_ids, persisted | SATISFIED | `parallels.py` full implementation; HYBRID scoping; 16 parallels tests pass |
| DMF-10 | Plans 01-05 | LOCAL absent from all new web library-filter options | SATISFIED | Inline `c != 'LOCAL'` guards on all 3 web surfaces; sanitize_library_codes everywhere; guard tests pass |
| DMF-12 | Plans 01, 02, 04 | Web catalog dialog text-search, count-shortlist (TRUE full-set facets), sort | SATISFIED | `get_browse_library_facets` method with DISTINCT counting; instance-method call; 18 fjms tests + 15 catalog tests pass |
| DMF-13 | Plans 01-05 | expand-all / universe from library_codes_with_manuscripts() minus LOCAL | SATISFIED | All four surfaces (desktop dialog, desktop button, web catalog dialog/button, parallels dialog/button) use `library_codes_with_manuscripts()` |

No orphaned requirements — all 6 DMF requirements declared in plans are accounted for. REQUIREMENTS.md shows all 6 as Complete (DMF-13 marked `[~]` Partial for the overall milestone but the Phase 131 portion for catalog/parallels/desktop is done).

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `web/pages/parallels.py` | 2785 | `'boundary_options': None,  # Phase 77: ... placeholder for parity` | Info | Pre-existing comment, unrelated to Phase 131 |
| `web/components/filter_panel.py` | 335-337 | Planning task identifiers `T-130-02-06 / HIGH-1` in comment | Info | IN-03 from REVIEW.md — internal task IDs in production code comment; harmless but noisy |

No TBD/FIXME/XXX debt markers in Phase 131 modified files. No unreferenced blockers. No stub implementations (all data paths fully wired).

---

### Human Verification Required

#### 1. Desktop Catalog — Dialog Mode Toggle and Session Persistence

**Test:** Launch the desktop app, open the Catalog tab (Browse-by-Identification), click the library filter button, toggle between Hide and Show-only modes, check a subset of libraries, click Apply, reopen the dialog within the same session.
**Expected:** Dialog reopens with the previously selected mode and checkboxes; toggling mode unchecks all items (D-04 reset); button label uses Phase-130 pluralized keys; total in Show-only label does not exceed selectable count (no '18' when 15 are selectable).
**Why human:** PyQt6 dialog render and in-session state round-trip require a running Qt display; headless CI cannot exercise this path.

#### 2. Desktop Catalog — Hide-Mode Search/Composition Handoff

**Test:** With a Hide library selection active, click "Search in these results" and "Composition in these results."
**Expected:** Status bar shows the 5-second notice "Library Hide filter not applied to search/composition"; search/composition scope is NOT narrowed by the Hide library selection (no silent allowlist inversion).
**Why human:** Status-bar message and scope correctness require a running Qt application.

#### 3. Web Catalog — True Facet Counts and Persist

**Test:** Open `/catalog`, click Browse-by-Identification, open the library filter dialog with NO other filters active. Confirm shortlist counts include libraries not on the current page-50. Toggle modes, text-search, sort by count/A-Z, Apply, reload.
**Expected:** Shortlist shows full-set counts from `get_browse_library_facets` (not page-local); off-page libraries visible; dict-shape persists across reload; SEED-023 PGP/Editions filters still work.
**Why human:** NiceGUI async render, FJMS DB connection, and page-reload persistence require a live browser session.

#### 4. Web Catalog — Hide→Search Mode Handoff

**Test:** Apply a Hide library selection in Browse-by-Identification, click "Search in these results."
**Expected:** `/search` page opens with the library filter in Hide mode (not converted to Show-only); button shows 'Hiding N libraries' or equivalent.
**Why human:** Cross-page state transfer and /search filter-panel state require a live browser session.

#### 5. Web /parallels — Library-Only Show-only Scope

**Test:** Run a composition search on `/parallels`. Open the library-filter button, select a subset in Show-only mode with NO advanced (domain/author/work) filters active, Apply.
**Expected:** Results are scoped to the selected libraries (library-only Show-only is not a no-op); results from non-selected libraries disappear; page reload persists the selection.
**Why human:** Requires live Tantivy index and meta_mgr; result-set change observable only in browser.

#### 6. Web /parallels — Hide Post-Fetch Scoping in Exports

**Test:** With Hide library selection active on `/parallels`, export results (XLSX or JSON).
**Expected:** Export file contains only rows from non-hidden libraries (Hide post-fetch filter applied before export/storage).
**Why human:** File download and export content inspection require a live browser session.

---

### Gaps Summary

No gaps found. All 6 must-have truths are VERIFIED at code level. Code review findings (6 total from REVIEW.md) were addressed: CR-01 (wrong total denominator — fixed `84f735d1`), CR-02 (LOCAL asymmetry in Show-only — fixed `f2dd1cc0`), WR-01 (dead `facets` assignment — fixed in `84f735d1`), WR-02 (stale comment — fixed `77429cac`), WR-04 (`not checked` guard — fixed `3103d8a4`), WR-05 (meta_mgr None guard — fixed `d92c42a7`). WR-03 (parallels handoff from catalog browse) is deferred as out of scope for Phase 131; no browse→parallels button currently exists. 88 tests pass across the phase and guard suites. Ruff clean on all 6 production files.

Status is `human_needed` — not `passed` — because the project pattern for NiceGUI web pages and PyQt6 desktop dialogs mandates a live-render smoke test that headless pytest cannot exercise. Code-level verification is complete at 6/6.

---

_Verified: 2026-06-30T14:00:00Z_
_Verifier: Claude (gsd-verifier)_
