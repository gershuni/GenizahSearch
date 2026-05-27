---
phase: 93-filtering-by-pgp
reviewed: 2026-05-19T16:00:51Z
depth: standard
files_reviewed: 4
files_reviewed_list:
  - web/pages/search.py
  - web/pages/search_state.py
  - tests/test_pgp_filter_cascade.py
  - genizah_translations.py
findings:
  critical: 0
  warning: 1
  info: 5
  total: 6
status: issues_found
---

# Phase 93: Code Review Report

**Reviewed:** 2026-05-19T16:00:51Z
**Depth:** standard
**Files Reviewed:** 4
**Status:** issues_found

## Summary

Phase 93 cleanly mirrors the existing `printed_filter` pattern to add a new
post-search PGP-presence toggle. The cascade-coverage invariant is well
protected by a new static AST guard (`tests/test_pgp_filter_cascade.py`),
the chip removal (`6437457d`) was applied surgically and `_clear_pgp_filter`
has zero remaining callers, and the Phase 87 storage chokepoint is preserved
(no raw `app.storage.user` reads/writes anywhere in the diff).

Code quality is good. No critical bugs and no security issues. One Warning
covers an asymmetry between `pgp_filter` and `printed_filter` in
`web/pages/search_state.py` (tab-snapshot helpers omit `pgp_filter`), which
"works by accident" today but is a maintenance hazard. The remaining items
are Info-level: a pre-existing logic edge case mirrored from
`printed_filter`, label drift in the same edge case, a small asymmetry in
the New Search reset block, an unverified count-label gap inherited from
`printed_filter`, and a minor inconsistency between bootstrap-restore and
New-Search-reset (the latter syncs the chip stub, the former does not — but
the chip is a no-op so neither path matters at runtime).

## Warnings

### WR-01: `pgp_filter` not persisted/restored via tab-snapshot helpers (asymmetric with `printed_filter`)

**File:** `web/pages/search_state.py:295-329, 374-397`
**Issue:** `printed_filter` is restored by `restore_search_active_snapshot`
(line 301), persisted by `persist_search_active_snapshot` (line 322-323),
and restored by `restore_search_snapshot` (line 379). The new
`pgp_filter` field is **not** handled in any of these three helpers — it
is only restored via the separate bootstrap-input path at
`web/pages/search.py:150`. The page works today only because:
1. The bootstrap restore at search.py:150 sets `state.pgp_filter` *before*
   `restore_search_snapshot()` is called at search.py:266, and
2. Neither helper overwrites `state.pgp_filter` (they only mutate
   `results`, `printed_filter`, `domain_exclusions`, `refinement_chain`,
   `exclusion_sources`).

Risk: a future refactor that resets `state.pgp_filter` inside
`restore_search_active_snapshot` (e.g., to clear stale state on tab
takeover), OR a reordering of search.py's restore block, OR a tab-snapshot
read path that runs first, would silently break PGP filter session
restore — and there is no test asserting this invariant. The asymmetry
also makes the helpers harder to reason about: a reader of
`search_state.py` would not know that `pgp_filter` is a restorable field
unless they cross-reference `search.py`.

**Fix:** Add `pgp_filter` to the three helpers symmetrically with
`printed_filter`:

```python
# search_state.py:295 — restore_search_active_snapshot
def restore_search_active_snapshot(state: 'SearchUIState') -> bool:
    raw = get_search_active_snapshot()
    if not raw:
        return False
    state.results = raw.get('results', []) or []
    state.printed_filter = raw.get('printed_filter', 'all')
    state.pgp_filter = raw.get('pgp_filter', 'all')   # add
    # ...

# search_state.py:314 — persist_search_active_snapshot
def persist_search_active_snapshot(state: 'SearchUIState') -> None:
    # ...
    tab[_SEARCH_ACTIVE_TAB_KEY] = {
        'version': _SEARCH_ACTIVE_TAB_VERSION,
        'results': _compact_result_rows((state.results or [])[:1000]),
        'printed_filter': state.printed_filter,
        'pgp_filter': state.pgp_filter,   # add
        # ...
    }

# search_state.py:374 — restore_search_snapshot
        state.printed_filter = safe_user_get('search_printed_filter', 'all')
        state.pgp_filter = safe_user_get('search_pgp_filter', 'all')   # add
```

And, for `persist_search_snapshot` at line 417, add:
```python
safe_user_set('search_pgp_filter', state.pgp_filter)
```

Alternative: leave the helpers alone and add a comment block above the
bootstrap read at search.py:150 documenting that `pgp_filter` is
intentionally NOT in the snapshot helpers (and why). If you choose the
alternative, add a unit test asserting that `restore_search_active_snapshot`
does not mutate `state.pgp_filter`, so the invariant is locked in.

## Info

### IN-01: `_apply_pgp_filter` short-circuit returns unfiltered results when `transcription_sys_ids` is empty AND filter is `'only_pgp'`

**File:** `web/pages/search.py:3238`
**Issue:** Mirrors the same edge case in `_apply_printed_filter`
(line 3218): when the relevant id-set is empty, the function short-circuits
and returns the full list — regardless of whether the active mode is
`only_pgp` (which should return `[]`) or `hide_pgp` (which should return
the full list, which is correct). With `pgp_filter='only_pgp'` and
`transcription_sys_ids=set()`, the user sees all results despite the UI
claiming "Only PGP" is active. The button visibility flip at line 4555
prevents this state via interactive UI, but it CAN occur on session
restore if the stored filter is `'only_pgp'` and the deferred transcription
fetch at line 4830-4848 returns an empty set (no result has PGP).

**Fix:** Split the short-circuit so `only_pgp` always returns the
filtered (potentially empty) list:

```python
def _apply_pgp_filter(results_list):
    if search_state.pgp_filter == 'all':
        return results_list
    if not search_state.transcription_sys_ids:
        # Only-PGP with no PGP data => no matches; Hide-PGP with no PGP
        # data => no change.
        return [] if search_state.pgp_filter == 'only_pgp' else results_list
    filtered = []
    for r in results_list:
        ...
```

Same logic-bug class affects `_apply_printed_filter` and pre-dates
Phase 93 — fixing only one would create new asymmetry, so consider patching
both in a follow-up.

### IN-02: Count-label drift when `pgp_filter='only_pgp'` AND `transcription_sys_ids` is empty

**File:** `web/pages/search.py:3266-3267, 3761-3762`
**Issue:** `count_parts.append(tr('Only PGP') ...)` always fires when
`pgp_filter != 'all'`, regardless of whether
`transcription_sys_ids` is empty. Combined with IN-01, this means the
results count label can read "X of X Results (Only PGP)" while every
result is shown. The user will see a misleading state until the next
toggle. Same drift exists for `printed_filter`.

**Fix:** Gate the count_parts append on the same condition as the
predicate:
```python
if search_state.pgp_filter != 'all' and search_state.transcription_sys_ids:
    count_parts.append(...)
```

### IN-03: Word-search count label omits PGP filter status (inherited from `printed_filter`)

**File:** `web/pages/search.py:3697`
**Issue:** In the word-search branch widened by Phase 93 (line 3689), the
count label is set to:

```python
results_count.text = f"{showing} {tr('Results')} ({n_excl} {tr('excluded')})"
```

This drops both "of total" prefix AND the "Only PGP / Hiding PGP" status
that the `_apply_printed_filter_and_render` and `_apply_domain_exclusions`
sites correctly include via `count_parts`. So when the user has
word-search active + `pgp_filter='only_pgp'`, the count label says
"N Results (M excluded)" — no indication that PGP filter is also active.

This is pre-existing for `printed_filter`; Phase 93 inherits it. Not a
regression. Fix would normalize this branch to use the same `count_parts`
pattern as the other three sites.

### IN-04: `_update_printed_filter_btn` not called in New Search reset (asymmetric with PGP)

**File:** `web/pages/search.py:2087-2089` vs `:2107-2110`
**Issue:** PGP reset block correctly calls `_update_pgp_filter_btn()` after
setting `pgp_filter='all'` (line 2109). The printed_filter reset block
(line 2087) does NOT call `_update_printed_filter_btn()`. Since both
buttons are immediately hidden via `_set_btn_visible(..., False)`, neither
is visible after reset, so no functional difference. But on the next
search that has printed data, the button reappears with its OLD label
("Hiding printed" / "Only printed") for one frame until `_apply_enrichment_to_ui`
fires and calls `_update_printed_filter_btn()` at line ~ (no explicit call
— relies on `_render_with_filters` to re-render with fresh count).

Phase 93's PGP path is more thorough than the printed_filter precedent.
Not introduced by Phase 93. No fix needed for Phase 93; consider a
follow-up to normalize `printed_filter`.

### IN-05: Hebrew translations alias different English keys to identical strings

**File:** `genizah_translations.py:2681-2683`
**Issue:** The new translations:

```python
"Has PGP": "PGP בלבד",
"No PGP": "ללא PGP",
"Only PGP": "PGP בלבד",
"Hiding PGP": "ללא PGP",
```

`Has PGP` (button label) and `Only PGP` (count label) translate to the
same Hebrew string. Same for `No PGP` / `Hiding PGP`. This appears to be
**intentional aliasing** for natural Hebrew naming consistency (the same
concept is expressed identically in Hebrew regardless of English UI
context), and the Hebrew strings are semantically correct. Documenting
here for visibility: any future English-side rename of one without the
other will create asymmetric Hebrew text. Consider consolidating to a
single translation key per concept and using it from both call sites, or
adding an inline comment in `genizah_translations.py` clarifying the
deliberate aliasing.

---

_Reviewed: 2026-05-19T16:00:51Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
