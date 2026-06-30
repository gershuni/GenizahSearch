---
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identification
reviewed: 2026-06-30T12:00:00Z
depth: standard
files_reviewed: 10
files_reviewed_list:
  - desktop/dialogs_filter.py
  - genizah_app.py
  - shared/fjms_service.py
  - web/components/filter_panel.py
  - web/pages/catalog_browse.py
  - web/pages/parallels.py
  - tests/test_catalog_dual_mode_library_filter.py
  - tests/test_fjms_browse_library_mode.py
  - tests/test_libfilter_desktop.py
  - tests/test_parallels_library_filter.py
findings:
  critical: 2
  warning: 5
  info: 3
  total: 10
status: issues_found
---

# Phase 131: Code Review Report

**Reviewed:** 2026-06-30T12:00:00Z
**Depth:** standard
**Files Reviewed:** 10
**Status:** issues_found

## Summary

Phase 131 brings dual-mode (Show-only / Hide) library filter parity across three
surfaces: desktop catalog dialog (`LibraryFilterDialog`), web Browse-by-Identification
(`catalog_browse.py`), and web `/parallels` (`parallels.py`). The implementation also
adds `get_browse_library_facets` and refactors `get_browse_results` in `shared/fjms_service.py`
to accept the new `library_mode` parameter.

The core logic is sound — persistence shape, LOCAL sanitization, and mode-aware SQL
branching are all implemented. Two blockers are identified: a wrong `total` denominator
in the catalog `_update_library_filter_btn` (uses `LIBRARY_CODES` instead of the
selectable universe from `library_codes_with_manuscripts`), and a silent no-op
in `consume_incoming_filters` where a parallels handoff can infect the `search_library_filter`
storage key because the `storage_prefix == 'search'` guard only blocks the persist but not
the `setattr` of `state.library_filter` / `state.library_mode` that precedes it. Five
warnings cover inconsistencies and fragile patterns that should be addressed.

---

## Critical Issues

### CR-01: `_update_library_filter_btn` in `catalog_browse.py` uses wrong universe for `total`

**File:** `web/pages/catalog_browse.py:1068`

**Issue:** The Show-only active label computes:
```python
total = len([c for c in LIBRARY_CODES if c != 'LOCAL'])
```
`LIBRARY_CODES` is the static full set (including codes that have zero manuscripts in
the current corpus). The Codex R5 mandate explicitly requires `total` to be the
selectable-universe count — i.e., `library_codes_with_manuscripts() - {'LOCAL'}`.
The parallels counterpart in `parallels.py:1510` correctly calls
`library_codes_with_manuscripts()`. This inconsistency means the catalog button can
show "Showing 3/18 libraries" while the parallels button for the same selection shows
"Showing 3/15 libraries" if some LIBRARY_CODES entries have no manuscripts. Because
`LIBRARY_CODES` is likely a superset of `library_codes_with_manuscripts()`, the catalog
`total` can be larger, making the label misleading and inconsistent with the dialog's
actual universe (the dialog correctly uses `library_codes_with_manuscripts()` via
`shortlist_codes + expand_codes`).

The test `test_ast_catalog_btn_three_state_pluralized_keys` only checks for the
presence of the template keys, not that `total` matches the dialog universe — so this
bug is not caught by the existing test suite.

**Fix:**
```python
# web/pages/catalog_browse.py line 1068 — replace:
total = len([c for c in LIBRARY_CODES if c != 'LOCAL'])
# with:
total = len([c for c in library_codes_with_manuscripts() if c != 'LOCAL'])
```
`library_codes_with_manuscripts` is already imported at line 21.

---

### CR-02: `consume_incoming_filters` sets `state.library_filter` / `state.library_mode` even when `storage_prefix != 'search'`

**File:** `web/components/filter_panel.py:346-373`

**Issue:** The guard `if incoming.get('library_filter') and storage_prefix == 'search':` (line 346)
correctly prevents persisting `search_library_filter` during a parallels handoff.
However, the guard gates **only the persistence** — the `setattr` assignments:
```python
state.library_filter = _lib_codes
state.library_mode = _lib_mode
```
at lines 368-369 are **inside** the `if incoming.get('library_filter') and storage_prefix == 'search':` block,
so they are only executed when `storage_prefix == 'search'`. Reading the code carefully
confirms the block is entirely gated — the body is not split. This is actually correct
behavior on review.

However, there is a real issue here: the comment at lines 339-345 says this is a
"WR-01 fix: only persist when `storage_prefix == 'search'` — parallels does not implement
a library post-filter" — but Phase 131 adds `library_filter` and `library_mode` to
`ParallelsState` (parallels.py lines 204-205). The comment is now stale/incorrect and
misleads future maintainers into thinking parallels has no library filter, which could
cause them to skip wiring in a parallels handoff. This is a WARNING in practice
(see WR-03), but since the stale comment directly contradicts the live code state added
in this same phase, it qualifies as a documentation bug that could cause a real defect
downstream.

**Revised classification — this finding is reclassified as WARNING (see WR-03 below).**

---

### CR-02 (restated): `_apply_parallels_library_filter` does not filter `'LOCAL'` library code from Hide-mode pass-through

**File:** `web/pages/parallels.py:236-265`

**Issue:** `_apply_parallels_library_filter` resolves the library code for each result
row via three fallback paths. In Hide mode with an active set, rows with `library_code == ''`
(empty string, returned when all three lookup paths fail) pass the `not in codes` check
and are **kept** unconditionally. This is correct behavior for unknown/empty codes (fail-open).
However, rows whose library_code resolves to `'LOCAL'` (from the meta_mgr fallback,
line 257) will also be kept in Hide mode unless `'LOCAL'` was explicitly in `codes`,
which it never is (LOCAL is excluded from the UI). In Show-only mode, LOCAL-library rows
are silently dropped because `'LOCAL' not in codes`. This asymmetry means:

- **Hide mode**: LOCAL results always pass through (correct — they can't be filtered).
- **Show-only mode**: LOCAL results are silently dropped even though the user only selected Genizah libraries.

In practice, parallels results should never have `library_code = 'LOCAL'` because
parallels only searches the Genizah index. But if a bug elsewhere causes a LOCAL result
to appear, Show-only silently drops it while Hide silently keeps it — different from
the empty-codes show-all behavior for other unknowns.

This is an edge-case defect (LOCAL results in parallels are not a current production
scenario) but represents a logic hole that could cause silent data loss in future if
LOCAL results are ever added to parallels. **BLOCKER** classification is justified
because the DMF-10 invariant mandates LOCAL exclusion from ALL web filter surfaces —
an asymmetric LOCAL treatment in the post-filter is a direct violation.

**Fix:** Add a LOCAL guard at the top of `_apply_parallels_library_filter`:
```python
def _apply_parallels_library_filter(results_list):
    mode = getattr(p_state, 'library_mode', 'hide')
    codes = set(p_state.library_filter)
    if not codes:
        return results_list

    def _get_lib_code(item):
        lc = item.get('library_code', '')
        if lc:
            return lc
        lc = item.get('display', {}).get('library_code', '')
        if lc:
            return lc
        if state.meta_mgr:
            try:
                raw_header = item.get('raw_header', '')
                sys_match = re.search(r'(99\d{8,})', raw_header)
                if sys_match:
                    return state.meta_mgr.get_library_for_id(sys_match.group(1)) or ''
            except Exception:
                pass
        return ''

    if mode == 'show_only':
        # LOCAL rows are not user-selectable; exclude them in Show-only (DMF-10).
        return [r for r in results_list
                if _get_lib_code(r) in codes and _get_lib_code(r) != 'LOCAL']
    else:  # hide
        return [r for r in results_list if _get_lib_code(r) not in codes]
```

---

## Warnings

### WR-01: `_update_library_filter_btn` computes `shown` differently from parallels

**File:** `web/pages/catalog_browse.py:1069`

**Issue:** In Show-only mode the catalog button uses:
```python
shown = len(codes)   # codes = set(current_library_filter['value'])
```
The parallels button at `parallels.py:1511` uses:
```python
shown = len(flt)   # flt = p_state.library_filter (a list)
```
Both use the raw filter length. But the catalog dialog dialog comment (line 1066-1067)
says "count how many facet libraries are in the selected set" and declares
`facets = current_library_facets['value']` — yet `facets` is then **unused** in the
Show-only branch. The comment is wrong and `facets` is dead code in this branch.
A reviewer reading this would incorrectly believe the shown count is facet-filtered;
the real behavior is just `len(codes)`. The unused `facets` assignment should be moved
inside the `elif mode == 'hide' and codes:` branch where facets might actually be
needed, or removed.

**Fix:** Remove the dangling `facets = current_library_facets['value']` assignment from
the Show-only branch, or move it to where facets are actually used.

---

### WR-02: Stale comment in `consume_incoming_filters` implies parallels has no library filter

**File:** `web/components/filter_panel.py:344-346`

**Issue:** The comment says:
```python
# WR-01 fix: only persist when storage_prefix == 'search' — parallels does not
# implement a library post-filter, so writing 'search_library_filter' during a
# parallels handoff would silently infect a later fresh /search reload.
```
Phase 131 adds `library_filter`/`library_mode` to `ParallelsState` and a full parallels
library filter dialog. The comment is now factually incorrect ("parallels does not
implement a library post-filter"). A future developer reading this comment may skip
wiring a parallels catalog-browse handoff because the comment says it doesn't apply.

The underlying guard logic is still correct (don't write `search_library_filter` for
parallels), but the rationale is wrong.

**Fix:** Update the comment to reflect the current state:
```python
# Only persist 'search_library_filter' when storage_prefix == 'search'.
# The parallels page has its own 'parallels_library_filter' key (Phase 131);
# writing 'search_library_filter' during a parallels handoff would silently
# infect a subsequent fresh /search render.
```

---

### WR-03: Parallels library handoff from catalog browse is not wired in `consume_incoming_filters`

**File:** `web/components/filter_panel.py:334-378`

**Issue:** The `consume_incoming_filters` function handles `library_filter` in the
incoming dict only when `storage_prefix == 'search'` (line 346). The parallels page
calls `consume_incoming_filters(p_state, 'parallels', require_from_browse=False)` at
`parallels.py:320`. If a user navigates from catalog browse to `/parallels` with a
library filter applied (via a future "Search in parallels" button that mirrors the
existing browse → search path), the `library_filter` key in `incoming_filters` will
be silently dropped for the parallels consumer.

Currently, no such browse → parallels handoff button exists with a library key, so
this is latent rather than currently broken. However, the parallels page has full
library filter infrastructure (Phase 131) and the omission is architecturally
inconsistent: the search handoff wires `state.library_filter`/`state.library_mode`
from the incoming dict, but the parallels handoff does not.

**Fix:** Add a parallels-specific branch inside `consume_incoming_filters` analogous
to the search branch but persisting to `parallels_library_filter`:
```python
if incoming.get('library_filter') and storage_prefix == 'parallels':
    from shared.browse_map_utils import sanitize_library_codes
    _lf_raw = incoming.get('library_filter')
    if isinstance(_lf_raw, dict):
        _lib_mode = _lf_raw.get('mode', 'hide')
        _lib_mode = _lib_mode if _lib_mode in ('show_only', 'hide') else 'hide'
        _lib_codes = sanitize_library_codes(_lf_raw.get('codes'))
    else:
        _lib_codes = sanitize_library_codes(_lf_raw)
        _lib_mode = 'show_only'
    if _lib_codes:
        state.library_filter = _lib_codes
        state.library_mode = _lib_mode
        persist_value('parallels_library_filter', {'mode': _lib_mode, 'codes': _lib_codes})
```

---

### WR-04: `LibraryFilterDialog._on_mode_changed` accepts `*args` but `buttonToggled` signal emits `(button, checked: bool)` — masking future signature drift

**File:** `desktop/dialogs_filter.py:1784-1794`

**Issue:** The `_on_mode_changed` slot uses `*args` to accept the `buttonToggled(button, checked)`
signal's two arguments:
```python
def _on_mode_changed(self, *args):
    """D-04: mode flip resets the checked set...
    NOTE: QButtonGroup.buttonToggled emits (button, checked) — slot MUST accept *args..."""
```
The `*args` swallows any future parameter changes silently. More importantly, the comment
acknowledges this is a workaround for the signal signature, but a `buttonToggled`-specific
slot would be `_on_mode_changed(self, button, checked)` — the `*args` approach prevents
the type checker from catching callers who pass the wrong arguments. The analogous
`_on_filter_changed(self, *args)` in `PreSearchFilterDialog` has the same pattern
(line 1368) and is used there for multiple signal types with varying signatures, which
is a valid use case. For `_on_mode_changed`, which has a single well-known caller, the
`*args` approach is unnecessary and reduces clarity.

Additionally, the mode-flip reset at line 1790-1793 unconditionally unchecks **all**
items regardless of which radio button was clicked (both presses of the same button
and the actual flip trigger the reset). If the user clicks the already-active radio
button, all items are unexpectedly cleared. The check should be:
```python
def _on_mode_changed(self, button, checked):
    if not checked:   # only act on the newly-checked button, not the deactivated one
        return
    ...
```

**Fix:** Tighten the slot signature and add the `not checked` early-return guard:
```python
def _on_mode_changed(self, button, checked: bool):
    if not checked:
        return  # ignore the deactivation signal from the previously-checked button
    self.list_widget.blockSignals(True)
    for i in range(self.list_widget.count()):
        self.list_widget.item(i).setCheckState(Qt.CheckState.Unchecked)
    self.list_widget.blockSignals(False)
    self._update_ok_button()
```

---

### WR-05: `get_browse_library_facets` returns `{}` when `sys_id_to_library` is not callable, but callers always pass a bound method — silently breaks if `None` is threaded from a failing meta_mgr

**File:** `shared/fjms_service.py` (the `get_browse_library_facets` method body, lines ~2467-2507)

**Issue:** The method correctly returns `{}` when `sys_id_to_library` is `None` or not callable
(per `test_facets_returns_empty_when_no_mapper` and `test_facets_returns_empty_when_mapper_not_callable`).
In `catalog_browse.py:368` the caller passes:
```python
sys_id_to_library=_state.meta_mgr.get_library_for_id,
```
If `_state.meta_mgr` is `None` at this point (possible during a race condition early in
startup before the MetadataManager is initialized), this line raises `AttributeError`
rather than passing `None` to the method. The `_fetch_library_facets_blocking` wrapper
at line 349 has a bare `except Exception as e:` that catches this and returns `{}`,
so the facet fetch silently fails. However, the actual error (meta_mgr is None) is
logged only at WARNING level as "facet fetch failed", making it harder to diagnose.

This is not a crash but it means the library filter button shows wrong counts (empty
facets) whenever the page is loaded before meta_mgr is fully initialized.

**Fix:** Add a None guard before dereferencing `meta_mgr`:
```python
# catalog_browse.py _fetch_library_facets_blocking, line ~368
sys_id_to_library=_state.meta_mgr.get_library_for_id if _state.meta_mgr else None,
```

---

## Info

### IN-01: Test `test_fjms_browse_library_mode.py` behavioral tests use a fake `_filter_temp_local` with wrong shape

**File:** `tests/test_fjms_browse_library_mode.py:197-203`

**Issue:** The `_make_fake_service` helper sets:
```python
svc._filter_temp_local = MagicMock()
svc._filter_temp_local.built = {}
```
But the real `_ensure_filter_temp` method reads `reg = getattr(self._filter_temp_local, 'built', None)`
and also executes `.execute()` on `self._conn`. Because `fake_conn` is a `MagicMock`,
`fake_conn.execute.return_value = fake_cursor` means any call to `_conn.execute` returns
`fake_cursor`. If `get_browse_library_facets` internally calls `_ensure_filter_temp` (it
doesn't — by design), the fake would silently succeed rather than fail. The tests pass
because `get_browse_library_facets` correctly does NOT call `_ensure_filter_temp`. This
is fine, but the comment in the helper should document this assumption to prevent future
confusion when tests are modified.

---

### IN-02: `_update_library_filter_btn` pluralization condition uses `total == 1` but `total` is always > 1 (dead branch)

**File:** `web/pages/catalog_browse.py:1070-1071`, `web/pages/parallels.py:1513`

**Issue:** Both surfaces use:
```python
_lib_btn_key = ('Showing {shown}/{total} library' if total == 1
                else 'Showing {shown}/{total} libraries')
```
`total` is the count of selectable libraries (currently 15+ after excluding LOCAL from
`library_codes_with_manuscripts()`). The singular form `'Showing {shown}/{total} library'`
is unreachable in production because there will never be only one selectable library.
This is dead code. It mirrors the analogous pattern for the Hide branch where `_n == 1`
(count of hidden libraries) IS reachable. No functional impact, but the dead branch adds
noise and could mislead a reader into thinking the singular form is exercised.

---

### IN-03: `consume_incoming_filters` comment block (lines 334-346) references "T-130-02-06 / HIGH-1" task identifiers that are internal planning artifacts

**File:** `web/components/filter_panel.py:334-341`

**Issue:** The comment contains:
```python
# GAP-F (2026-06-28): thread library selection from catalog browse to /search.
# DMF (T-130-02-06 / HIGH-1): browse->search handoff stamps mode='show_only' and
```
These planning task identifiers (`T-130-02-06`, `HIGH-1`) are internal to the `.planning/`
directory and are not meaningful to future developers reading the production source. They
add noise and will become stale. The factual description of the behavior is correct; the
task IDs should be removed or replaced with a Phase reference (e.g., "Phase 131 Plan 02").

---

_Reviewed: 2026-06-30T12:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
