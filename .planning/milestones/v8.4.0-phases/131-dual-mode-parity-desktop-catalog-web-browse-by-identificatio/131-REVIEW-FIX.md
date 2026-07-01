---
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identification
fixed_at: 2026-06-30T12:30:00Z
review_path: .planning/phases/131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio/131-REVIEW.md
iteration: 1
findings_in_scope: 6
fixed: 5
skipped: 1
status: partial
---

# Phase 131: Code Review Fix Report

**Fixed at:** 2026-06-30T12:30:00Z
**Source review:** `.planning/phases/131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio/131-REVIEW.md`
**Iteration:** 1

**Summary:**
- Findings in scope: 6 (CR-01, CR-02, WR-01, WR-02, WR-04, WR-05)
- Fixed: 5
- Skipped: 1 (WR-03 — out of Phase-131 scope per objective)

**Test result:** 71/71 passed (`tests/test_libfilter_desktop.py`, `tests/test_catalog_dual_mode_library_filter.py`, `tests/test_parallels_library_filter.py`, `tests/test_fjms_browse_library_mode.py`) — 1 pre-existing deprecation warning (defusedxml.cElementTree), not introduced by these fixes.

**Ruff:** All checks passed on all 4 touched files.

---

## Fixed Issues

### CR-01 + WR-01: Fix Show-only total denominator + remove dead `facets` assignment

**Files modified:** `web/pages/catalog_browse.py`
**Commit:** `84f735d1`
**Applied fix:** In `_update_library_filter_btn`, replaced `len([c for c in LIBRARY_CODES if c != 'LOCAL'])` with `len([c for c in library_codes_with_manuscripts() if c != 'LOCAL'])` to use the selectable universe (Codex R5 mandate). Simultaneously removed the dead `facets = current_library_facets['value']` assignment in the Show-only branch (WR-01) — `facets` was assigned but never used there, and the comment claiming it counted facet-filtered libraries was wrong.

---

### CR-02: Add LOCAL exclusion to Show-only in `_apply_parallels_library_filter`

**Files modified:** `web/pages/parallels.py`
**Commit:** `f2dd1cc0`
**Applied fix:** Added `and _get_lib_code(r) != 'LOCAL'` to the Show-only list comprehension. The original code kept LOCAL-code rows in Hide mode (correct — LOCAL not in codes, not hidden) but silently dropped them in Show-only mode because LOCAL is never in the user-selectable codes set. This asymmetry violated DMF-10. The fix makes both branches consistent: LOCAL rows pass through Hide unchanged and are explicitly excluded in Show-only, matching the invariant that LOCAL is never part of the filter universe on any web surface. (Note: this is a logic change — requires human verification that the production behavior is correct per DMF-10.)

**Status: fixed: requires human verification** (logic change per DMF-10 invariant).

---

### WR-02: Update stale comment in `consume_incoming_filters`

**Files modified:** `web/components/filter_panel.py`
**Commit:** `77429cac`
**Applied fix:** Replaced the three-line comment block that incorrectly stated "parallels does not implement a library post-filter" with an accurate description: "Only persist 'search_library_filter' when storage_prefix == 'search'. The parallels page has its own 'parallels_library_filter' key (Phase 131); writing 'search_library_filter' during a parallels handoff would silently infect a subsequent fresh /search render."

---

### WR-04: Add `not checked` early-return guard to `_on_mode_changed`

**Files modified:** `desktop/dialogs_filter.py`
**Commit:** `3103d8a4`
**Applied fix:** `QButtonGroup.buttonToggled` emits `(button, checked)` for BOTH the newly-activated button (`checked=True`) and the previously-active button (`checked=False`). Without the guard, every mode toggle fired the full checkbox-reset twice, and clicking the already-selected radio caused an unexpected reset. Added `if len(args) >= 2 and not args[1]: return` at the top of the method body. The `*args` signature is preserved to mirror `PreSearchFilterDialog._on_filter_changed` as the scope mandates.

---

### WR-05: Guard `meta_mgr` None in `_fetch_library_facets_blocking`

**Files modified:** `web/pages/catalog_browse.py`
**Commit:** `d92c42a7`
**Applied fix:** Changed `sys_id_to_library=_state.meta_mgr.get_library_for_id` to `sys_id_to_library=(_state.meta_mgr.get_library_for_id if _state.meta_mgr else None)`. When `MetadataManager` is not yet initialized (startup race), the old code raised `AttributeError` which was silently swallowed by the bare `except`, producing empty facets with no actionable diagnostic. The `get_browse_library_facets` method already handles `None` mapper by returning `{}` gracefully (covered by `test_facets_returns_empty_when_no_mapper`).

---

## Skipped Issues

### WR-03: Parallels library handoff from catalog browse not wired

**File:** `web/components/filter_panel.py:334-378`
**Reason:** Out of Phase-131 scope per fix objective. Phase 131 delivers the parallels page's own filter control, NOT a browse→parallels handoff. WR-02's comment fix already removes the misleading claim that parallels has no library filter. The handoff wiring is a future task.
**Original issue:** `consume_incoming_filters` drops `library_filter` for `storage_prefix == 'parallels'`; no browse→parallels handoff button currently exists with a library key (latent, not currently broken).

---

_Fixed: 2026-06-30T12:30:00Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
