---
phase: 129-library-filter-search-browse-by-identification-seed-026
plan: "02"
subsystem: web-search
tags: [library-filter, post-search-filter, safe-storage, nicegui, i18n, facets]
dependency_graph:
  requires: []
  provides: [LIBFILTER-01, library-multi-select-web-search]
  affects: [web/pages/search.py, web/pages/search_state.py]
tech_stack:
  added: [shared.browse_map_utils.get_library_display, shared.browse_map_utils.LIBRARY_CODES]
  patterns: [post-search-cascade-filter, safe_storage-chokepoint, routing-predicate-widening]
key_files:
  created:
    - tests/test_libfilter_web_search.py
  modified:
    - web/pages/search.py
    - web/pages/search_state.py
decisions:
  - "Library chips rendered inside _update_chip_bar (widened has_any) so they appear when library is the only active filter — no separate chip render function needed"
  - "_update_library_chips delegates to _update_chip_bar rather than duplicating chip creation"
  - "6 routing predicate widenings cover all code paths: manuscript_exclusions empty+swap, word-search, history-restore, _render_with_filters, enrichment-restore"
  - "search_library_filter reset added to clear_search_snapshot() defaults dict for central New-Search reset"
metrics:
  duration: "~9 minutes"
  completed: "2026-06-28"
  tasks: 2
  files: 3
---

# Phase 129 Plan 02: Web Search Library Filter (LIBFILTER-01) Summary

Library multi-select filter for the web `/search` page: a compact dropdown-with-checklist that narrows the full result set by `library_code` BEFORE the `PAGE_SIZE` (50) pagination slice, with EN/HE labels, facet counts, removable chips, and state persistence via the `safe_storage` chokepoint.

## What Was Built

### Filter Logic (web/pages/search.py)

**`_apply_library_filter(results_list)`** — pure Python over the FULL `search_state.results` list (never sliced first). Empty `library_filter` list returns input unchanged; otherwise filters to `set(library_filter)` membership on `r['display']['library_code']`.

**`_compute_library_facets(results_list)`** — `Counter` of `library_code` over the pre-filter full result set. Libraries with 0 matches are absent from the Counter (D-02).

**Cascade integration** — `_apply_library_filter` folded into ALL three post-search render paths:
- `_apply_printed_filter_and_render`: after pgp filter, before measurement (line ~3557)
- `_apply_domain_exclusions`: after pgp filter, before measurement (line ~4059)
- `_apply_word_search_exclusions_and_render`: in BOTH the printed/pgp branch AND the bare else branch (lines ~3999, ~4007)

**Routing predicate widening** — 6 predicates widened with `or bool(search_state.library_filter)`:
1. `_apply_manuscript_exclusions` empty-exclusion branch (was: only printed+pgp gated)
2. `_apply_manuscript_exclusions` swap-branch (same)
3. `_apply_word_search_exclusions_and_render` (line ~3860)
4. History-restore rerender (line ~4110, printed-ONLY form — no pgp clause)
5. `_render_with_filters` enrichment rerender (line ~4820)
6. `_deferred_transcription_restore` enrichment-completion restore (line ~5160)

Without these widenings, a library-only selection would fall through to the bare `render_results` / measurement-only fallback and silently do nothing.

### UI (web/pages/search.py)

**Library filter button** — `ui.button('Filter by library')` placed beside the PGP filter button, hidden until results arrive (same pattern as printed/pgp). On click, opens a `ui.menu` checklist.

**Dropdown checklist** — populated by `_rebuild_library_menu()` on each enrichment update. Shows only codes present in the current result set (D-02), sorted by count desc then label. Each row labeled `get_library_display(code, short=False, lang=get_language())` (D-01). Selected codes bold. Toggling calls `_toggle_library_code(code)` which updates state, persists, rebuilds menu, refreshes chips, and re-applies filters.

**Removable chips** — rendered inside `_update_chip_bar` (widened `has_any` to include `bool(search_state.library_filter)`). This ensures chips appear when library is the ONLY active filter — not gated on `_has_active_filters()` (pre-search only) or printed/pgp. Each chip shows `"Library: <full name>"` with `account_balance` icon; clicking × calls `_remove_library_code(code)`.

**New Search reset** — `_set_btn_visible(library_filter_btn, False)` + `search_state.library_filter = []` added alongside pgp reset in the New Search handler.

### State (web/pages/search_state.py)

- `SearchUIState.library_filter: list = []` field added
- `clear_search_snapshot()` defaults extended with `'search_library_filter': []`

### Persistence (safe_storage chokepoint)

- Loaded: `_safe_get('search_library_filter', [])`, normalized to `[]` if not list, codes validated against `LIBRARY_CODES` keys (unknown dropped — T-129-04 tamper mitigation)
- Saved: `persist_value('search_library_filter', search_state.library_filter)` on every toggle/remove

## Tests

**`tests/test_libfilter_web_search.py`** (7 tests, all green):
1. `test_library_filter_narrows_full_set` — 70-result list (>PAGE_SIZE), filters JTS; items beyond index 50 included; AST guard `_apply_library_filter` in `_apply_printed_filter_and_render` before `render_results`
2. `test_library_only_routes_through_filtering_helper` — counts 6 `or bool(search_state.library_filter)` widenings; checks `_apply_manuscript_exclusions` and `_apply_word_search_exclusions_and_render` contain `library_filter`
3. `test_empty_selection_is_noop` — empty list returns input unchanged
4. `test_facets_from_prefilter_full_set` — Counter counts correct; 0-count codes absent; `_compute_library_facets` in AST
5. `test_persistence_uses_safe_storage_chokepoint` — `persist_value('search_library_filter', ...)` and `_safe_get('search_library_filter', ...)` present; no `app.storage.user` near library_filter
6. `test_label_uses_get_library_display` — `get_library_display` with `short=False` found in source
7. `test_chip_renders_when_library_only` — chip-related lines referencing `library_filter` present; `search_library_filter` key present

**GUARD-02:**
- `tests/test_pgp_filter_cascade.py` — 4 tests, all green (cascade coverage unbroken)
- `tests/test_no_raw_storage_access.py` — 6 tests, all green (allowlist still [])

## Deviations from Plan

**None** — plan executed exactly as specified. All 6 routing predicate sites (3418, 3446, 3823, 4066, 4775, 5114 in original line numbers) were widened. The `_update_chip_bar_with_library` helper was simplified to just delegate to `_update_chip_bar` after integrating library chip rendering directly into `_update_chip_bar` itself (cleaner than duplicating chip creation logic).

## Threat Surface Scan

No new network endpoints, auth paths, or file-system access patterns introduced. Filter state is non-sensitive UI state (T-129-05 accept). Tamper mitigation for persisted codes applied (T-129-04: unknown codes dropped on load).

## Self-Check: PASSED

- FOUND: tests/test_libfilter_web_search.py
- FOUND: web/pages/search.py
- FOUND: web/pages/search_state.py
- FOUND: commit 3ee11590 (Task 1 — test scaffold)
- FOUND: commit f1afd09f (Task 2 — implementation)
