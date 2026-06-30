---
phase: 130-dual-mode-filter-core-web-search
verified: 2026-06-30T12:00:00Z
status: human_needed
score: 6/6 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Open web /search in a browser, apply Hide mode filtering (e.g. hide RNL), run a new search, confirm RNL results are still absent; navigate away and back, confirm the Hiding N button state persists."
    expected: "Button reads 'Hiding N' after reload; results exclude RNL across searches; mode toggle initialized to Hide on dialog reopen."
    why_human: "Persistence round-trip through safe_storage across full page reload and the live JS data-libmode attribute sync cannot be verified headlessly."
  - test: "Open the library filter dialog in Show-only mode, check 2-3 libraries, click Apply; reload the page; confirm the button shows 'Showing N/total' and only those libraries appear in results."
    expected: "Button reads e.g. 'Showing 2/18'; reloading restores the same Showing N/total state; results contain only the chosen libraries."
    why_human: "Live render smoke for the ui.toggle initialization, the mode toggle visual appearance, and the show-all normalization path (Show-only selecting all-in-results -> neutral button after Apply)."
  - test: "Load the page with a legacy v8.3.0 plain-list search_library_filter in safe_storage (e.g. inject {'search_library_filter': ['CUL','JTS']} via browser devtools), reload; confirm button reads 'Showing 2/N' and dialog opens in Show-only mode."
    expected: "Legacy list migrated silently to Show-only with ['CUL','JTS']; no error; dialog mode toggle initialized to 'Show only selected'."
    why_human: "Migration path requires live storage injection and browser-side verification."
  - test: "In the dialog, flip the mode toggle from Show-only to Hide; confirm the checkbox selection is immediately cleared (D-04); in Hide mode with zero boxes checked, confirm Apply is enabled (not greyed out)."
    expected: "Mode flip resets all checkboxes; Apply button is enabled in Hide mode with empty selection."
    why_human: "libFilterSetMode JS + mode-aware libFilterUpdateApply behavior requires a live browser rendering the dialog."
  - test: "Navigate from Browse-by-Identification (catalog) to /search with 2 libraries selected; confirm the library filter button shows 'Showing 2/N' in Show-only mode immediately."
    expected: "filter_panel.consume_incoming_filters writes Show-only mode + dict shape; button reflects it without a separate search run."
    why_human: "Browse-to-search handoff requires live navigation across two NiceGUI pages."
  - test: "In Hebrew UI, confirm button states read 'סינון לפי ספרייה' (neutral), 'מציג N מתוך total' (Show-only), 'מסתיר N' (Hide); confirm dialog toggle labels read 'הצג רק נבחרות' / 'הסתר נבחרות'."
    expected: "All four new HE translation keys render correctly; no English fallback under Hebrew UI."
    why_human: "Hebrew UI rendering requires a live browser with the HE locale active."
---

# Phase 130: Dual-Mode Filter Core — Web `/search` Verification Report

**Phase Goal:** The web `/search` library filter can express BOTH "show only these libraries" (allowlist) and "hide these libraries" (denylist) — chosen via a mode toggle in the filter dialog, persisted (mode + set) across searches and reloads, with the existing allowlist migrated cleanly and edge states handled predictably. This phase defines the shared (mode + set) state shape and the dialog UX that Phase 131 mirrors.
**Verified:** 2026-06-30T12:00:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `_apply_library_filter` branches on `search_state.library_mode`; Hide = `not in`; Show-only = `in`; empty set = pass-through in both modes | VERIFIED | `web/pages/search.py` lines 3800-3823; `mode = getattr(search_state, 'library_mode', 'hide')`; explicit `in codes` / `not in codes` branches; empty-codes returns `results_list` unchanged in both modes |
| 2 | A "hide RNL" intent persists across new searches (Hide filter never recomputed from results) | VERIFIED | The hide-set is stored in `search_state.library_filter` and persisted via `persist_value('search_library_filter', {mode, codes})`; `_apply_library_filter` reads the stored set, not a recomputed facet — any row whose `library_code` is not in the set passes through regardless of whether it appeared before |
| 3 | Mode + set persist through the `safe_storage` chokepoint; round-trips across reload; `tests/test_no_raw_storage_access.py` allowlist stays `[]` | VERIFIED | `persist_value('search_library_filter', {'mode':..., 'codes':...})` is the only writer in `search.py`; `filter_panel.py::consume_incoming_filters` also writes `{'mode':'show_only','codes':...}` (not a bare list); `_safe_get` is the reader; 6/6 `test_no_raw_storage_access` tests pass |
| 4 | 3-state button reads `library_mode`; bilingual EN+HE keys "Hiding" / "Showing N/total" / "Filter by library" exist | VERIFIED | `_update_library_btn` at lines 1692-1735 reads `mode = getattr(search_state, 'library_mode', 'hide')`; `tr('Hiding')`, `tr('Showing')`, `tr('Filter by library')` all present; `genizah_translations.py` contains `"Hiding": "מסתיר"`, `"Show only selected": "הצג רק נבחרות"`, `"Hide selected": "הסתר נבחרות"`, `"Search libraries...": "חיפוש ספריות..."` |
| 5 | Legacy plain-list `search_library_filter` migrates to Show-only without error; browse->search handoff loads as Show-only | VERIFIED | Restore path lines 190-208: `isinstance(_lib_raw, list)` branch loads non-empty list as `mode='show_only'`, empty as `mode='hide'`; `filter_panel.py` line 354 sets `state.library_mode = 'show_only'`; `test_legacy_list_migrates_to_show_only` passes |
| 6 | Empty Show-only = show all; full Hide = clean 0 results; `'LOCAL'` absent from both dialog zones AND count shortlist | VERIFIED | Empty Show-only: `if not codes: return results_list` (line 3816); full Hide: `not in codes` filter returns `[]` (3822-3823); shortlist: `[c for c in facets if c in LIBRARY_CODES and c != 'LOCAL']` (line 1779); expand: `[c for c in LIBRARY_CODES if c != 'LOCAL' and c not in shortlist_set]` (line 1787); 7/7 DMF-10 guard tests pass |

**Score:** 6/6 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `web/pages/search_state.py` | `library_mode` field + mode-aware `clear_search_snapshot` reset | VERIFIED | `self.library_mode: str = 'hide'` at line 62; `clear_search_snapshot` defaults dict sets `search_library_filter` to `{'mode': 'hide', 'codes': []}` (line 463) |
| `web/pages/search.py` | Mode-aware restore+migration, mode-branch filter, dict persist, redesigned dialog, 3-state button | VERIFIED | All 5 components present and substantive; see Key Link Verification below |
| `web/components/filter_panel.py` | Browse->search handoff writes Show-only + dict shape | VERIFIED | Lines 349-357: sanitizes codes, sets `state.library_mode = 'show_only'`, persists `{'mode': 'show_only', 'codes': _lib_codes}` |
| `genizah_translations.py` | New EN+HE keys: Hiding, Show only selected, Hide selected, Search libraries... | VERIFIED | Lines 2913-2916 in `genizah_translations.py`; all 4 keys present with HE values |
| `tests/test_dual_mode_library_filter.py` | 24 tests covering mode-branch, migration, edge-states, dual-writer, LOCAL-shortlist, AST scans | VERIFIED | File exists; 24 tests; all pass |
| `tests/test_libfilter_web_search.py` | Stale inclusion-only assertions revised; no whole test deleted | VERIFIED | 3 surgical changes: `test_chip_renders_when_library_only` active-label relaxed; `test_all_unchecked_guard` scoped to Show-only; `test_no_script_in_library_dialog_html` docstring filter extended |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `search.py` restore path (~188) | `search_state.library_mode + library_filter` | legacy-list / dict / fresh branch | VERIFIED | `isinstance(_lib_raw, list)` -> Show-only/hide branch; `isinstance(_lib_raw, dict)` -> dict read with sanitize; else -> fresh hide/[] |
| `search.py::_apply_library_filter` | `search_state.library_mode` | `show_only in` / `hide not-in` branch | VERIFIED | Lines 3811-3823; reads `library_mode` via `getattr`; correct semantics per mode |
| `search.py` Apply handler | `persist_value('search_library_filter', {mode, codes})` | dict persist with show-all normalization | VERIFIED | Lines 1935-1938; dict literal with `mode` + `codes` keys; show-all normalization at lines 1923-1925 |
| `filter_panel.py::consume_incoming_filters` | `state.library_mode = 'show_only'` + `persist_value('search_library_filter', {...})` | browse->search handoff | VERIFIED | Lines 351-357; imports `LIBRARY_CODES`, sanitizes, stamps `show_only`, persists dict |
| Dialog expand section + shortlist | `LIBRARY_CODES minus LOCAL` | `c != 'LOCAL'` comprehensions | VERIFIED | 5 occurrences of `c != 'LOCAL'` in the dialog function body; confirmed by AST scan test 18 + test 20 |
| `_update_library_btn` | `library_mode` + 3-state labels | reads mode; emits Neutral/Showing/Hiding text | VERIFIED | Lines 1709, 1724, 1728, 1733 |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|--------------|--------|--------------------|--------|
| `_apply_library_filter` | `search_state.library_filter` + `search_state.library_mode` | Loaded from `_safe_get('search_library_filter')` on page init; set by Apply handler; set by `consume_incoming_filters` | Yes — reads from per-user `safe_storage` (real persisted data, not hardcoded) | FLOWING |
| `_update_library_btn` | `search_state.library_filter` + `search_state.library_mode` + `search_state.results` facets | Same sources as above; facets derived from real search results | Yes — facets computed from live result set | FLOWING |
| Dialog shortlist | `_compute_library_facets(search_state.results)` | Result rows from live search engine | Yes — counts from full pre-`[:200]` result set | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `SearchUIState().library_mode` defaults to `'hide'` | `python -c "from web.pages.search_state import SearchUIState; s = SearchUIState(); assert s.library_mode == 'hide'"` | Pass | PASS |
| `clear_search_snapshot` resets to dict shape | AST check: `"'search_library_filter': {'mode': 'hide', 'codes': []}" in src` | Present in source | PASS |
| 52 named test files pass | `pytest tests/test_dual_mode_library_filter.py tests/test_libfilter_web_search.py tests/test_web_library_options_no_local.py tests/test_phase_97_invariants.py tests/test_no_raw_storage_access.py -q` | 52 passed in 4.35s | PASS |
| ruff clean on all modified files | `python -m ruff check web/pages/search.py web/pages/search_state.py web/components/filter_panel.py genizah_translations.py` | All checks passed | PASS |
| No `<script>` inside dialog `ui.html()` calls | Python AST scan of all `ui.html(...)` string arguments in dialog function | No `<script>` found | PASS |
| `c != 'LOCAL'` appears at least twice in dialog function body | Count occurrences | 5 occurrences | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| DMF-01 | 130-02 | Show-only/Hide mode toggle in dialog | SATISFIED | `ui.toggle` with `show_only`/`hide` options in `_open_library_filter_dialog`; `_apply_library_filter` enforces both modes |
| DMF-02 | 130-02 | Hide intent persists as new libraries appear | SATISFIED | `_apply_library_filter` reads stored `library_filter` set; never recomputes from results; test `test_hide_persists_over_new_libraries` passes |
| DMF-03 | 130-01, 130-02 | Mode + set persist via `safe_storage`; both writers use dict shape; `test_no_raw_storage_access` allowlist stays `[]` | SATISFIED | Both writers confirmed; 6 storage-guard tests pass |
| DMF-04 | 130-02 | 3-state button (Neutral / Showing N/total / Hiding N); bilingual | SATISFIED | `_update_library_btn` lines 1722-1735; 4 new HE keys in `genizah_translations.py` |
| DMF-05 | 130-02 | Legacy allowlist migrates to Show-only; browse->search handoff is Show-only | SATISFIED | Restore `isinstance(list)` branch; `filter_panel.py` stamps `show_only`; `test_legacy_list_migrates_to_show_only` passes |
| DMF-06 | 130-01, 130-02 | Empty Show-only = show all; full Hide = 0 results; show-all has one persisted representation | SATISFIED | Both edge-cases in `_apply_library_filter`; show-all normalization at Apply lines 1923-1925; `clear_search_snapshot` resets to `{'mode':'hide','codes':[]}`; tests pass |
| DMF-10 | 130-02, 130-03 | `'LOCAL'` absent from dialog options (both zones) AND count shortlist; guard tests green | SATISFIED | 5 `c != 'LOCAL'` literals in dialog function; `test_web_library_options_no_local.py` + `test_phase_97_invariants.py` 7/7 green |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None found | — | — | — | — |

No `TBD`, `FIXME`, `XXX` markers in any phase-130-modified files. No stub patterns. No hardcoded empty data flowing to user-visible output. No orphaned artifacts.

### Human Verification Required

6 items require a live browser. All automated code-verifiable checks passed.

#### 1. Persistence Round-Trip Across Reload (Hide Mode)

**Test:** Apply a Hide mode filter (e.g. hide RNL), run a search, fully reload the page.
**Expected:** Button reads "Hiding N" after reload; results still exclude RNL; dialog reopens in Hide mode.
**Why human:** `safe_storage` persistence across a real NiceGUI page reload requires a running server; headless pytest cannot exercise the WebSocket reconnect + state restore path.

#### 2. Persistence Round-Trip Across Reload (Show-only Mode) + Show-all Normalization

**Test:** Apply a Show-only filter for 2-3 libraries, run a search, reload; also test that selecting all in-result libraries and clicking Apply returns the button to neutral.
**Expected:** "Showing N/total" persists after reload; applying all-in-results in Show-only normalizes to neutral "Filter by library" button.
**Why human:** Live render of ui.toggle initialization + show-all normalization side effect (mode flip to hide/[] must render as neutral).

#### 3. Legacy Allowlist Migration (Live Storage Injection)

**Test:** Inject `{'search_library_filter': ['CUL','JTS']}` into browser storage, reload /search.
**Expected:** Button reads "Showing 2/N"; dialog mode toggle initialized to "Show only selected".
**Why human:** Requires direct storage manipulation and live browser render.

#### 4. Mode-Toggle Dialog Behavior (D-04 Reset + Apply Enable)

**Test:** Open dialog, check 2 libraries in Show-only; flip toggle to Hide; confirm checkboxes cleared; confirm Apply is enabled with zero boxes checked in Hide mode.
**Expected:** Mode flip clears selection immediately; Apply is not greyed out in empty-Hide state.
**Why human:** `libFilterSetMode` JS execution and `data-libmode`-based Apply-enable logic requires a live browser DOM.

#### 5. Browse-to-Search Handoff

**Test:** Select 2 libraries in Browse-by-Identification, navigate to /search.
**Expected:** Library filter button immediately shows "Showing 2/N" in Show-only mode without requiring a search run.
**Why human:** Cross-page NiceGUI navigation requires a running server and two live pages.

#### 6. Hebrew UI Label Rendering

**Test:** Switch web UI to Hebrew, open the library filter dialog.
**Expected:** Toggle shows "הצג רק נבחרות" / "הסתר נבחרות"; button neutral reads "סינון לפי ספרייה"; active states read "מציג N מתוך total" / "מסתיר N"; search placeholder reads "חיפוש ספריות...".
**Why human:** i18n rendering requires a live browser in Hebrew locale; `tr()` call resolution can only be confirmed visually.

### Gaps Summary

No gaps. All code-verifiable must-haves are VERIFIED. The 6 human verification items are exclusively live browser render checks (persistence across reload, dialog JS behavior, bilingual rendering, browse-to-search handoff) — the standard "UI hint: yes" residual for a NiceGUI phase. These are not blockers; they are the expected live smoke tests that code inspection cannot replace.

---

_Verified: 2026-06-30T12:00:00Z_
_Verifier: Claude (gsd-verifier)_
