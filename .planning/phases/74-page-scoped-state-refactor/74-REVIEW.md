---
phase: 74-page-scoped-state-refactor
reviewed: 2026-04-17T00:00:00Z
depth: standard
files_reviewed: 10
files_reviewed_list:
  - tests/e2e/test_browse_flow.py
  - tests/test_browse_bootstrap.py
  - tests/test_search_state.py
  - web/browse_bootstrap.py
  - web/components/filter_panel.py
  - web/pages/browse.py
  - web/pages/browse_state.py
  - web/pages/search.py
  - web/pages/search_results.py
  - web/pages/search_state.py
findings:
  critical: 0
  warning: 3
  info: 5
  total: 8
status: issues_found
---

# Phase 74: Code Review Report

**Reviewed:** 2026-04-17
**Depth:** standard
**Files Reviewed:** 10
**Status:** issues_found

## Summary

Phase 74 extracts persistence helpers (`persist_/restore_/clear_search_snapshot`,
`persist_/restore_/clear_browse_snapshot`), pulls the browse bootstrap
precedence into a pure `resolve_browse_bootstrap()`, and sweeps Cat-1
`on_click=lambda: asyncio.ensure_future(...)` wrappers from the shelfmark
navigation buttons. The extraction is well-scoped, the pure resolver is
nicely unit-tested, and no new security issues were introduced.

The highlighted concerns are all about the *partial* integration of
`restore_search_snapshot` and internal consistency of
`_SEARCH_SNAPSHOT_KEYS` — the helper advertises ownership of keys it never
reads or persists. These are correctness / maintainability issues, not
bugs the user will hit today, but they will bite the next person who tries
to complete the migration.

No critical security, injection, or auth issues found. The Cat-1 sweep at
`browse.py:1610` / `1803` is correctly implemented and the new e2e
regression test (`test_shelfmark_navigation_updates_url`) asserts the
behavior with a strong selector + strong assertion that the `sys_id`
actually changes.

## Warnings

### WR-01: `restore_search_snapshot` omits keys declared in `_SEARCH_SNAPSHOT_KEYS`

**File:** `web/pages/search_state.py:217-281`
**Issue:** `_SEARCH_SNAPSHOT_KEYS` lists `word_search_excluded_ids` and
`search_all_terms_filter` as owned by the helper (lines 219, 221), but
`restore_search_snapshot` (lines 241-281) never reads either key back.
It restores only `search_results`, `search_printed_filter`,
`domain_exclusions`, `search_refinement_chain`, and
`search_exclusion_sources`.

Meanwhile, `persist_search_snapshot` (lines 284-321) *also* does not
write `word_search_excluded_ids` or `search_all_terms_filter` — those are
still persisted via `persist_value(...)` from `filter_panel`/`search.py`
(e.g. `search.py:1688`, `1704`, `3888`, `search_results.py:270`, `294`).
The only place the helper actually touches these keys is inside
`clear_search_snapshot` (lines 347, 357).

Net effect: the helper *claims* to own these keys per the module
docstring ("SOLE owners of the restorable_page_snapshot storage keys",
line 202) but in practice it only owns the clear half. A reader
trying to audit key lifecycle will be misled.

This does not currently break user-visible behavior because
`restore_search_snapshot` is not yet invoked from `search.py` — the live
restore block at `search.py:142-162` still reads these keys inline
(`_wse = app.storage.user.get('word_search_excluded_ids')`,
`_saved_refinement_chain = app.storage.user.get('search_refinement_chain', [])`).
So today the inline path compensates. But the moment someone replaces
that block with `restore_search_snapshot(search_state)` (which is the
stated end state of this refactor), `word_search_excluded_ids` and
`search_all_terms_filter` will silently stop restoring across page
reloads.

**Fix:** Either (a) add the missing reads/writes to `restore_search_snapshot`
/ `persist_search_snapshot`:

```python
# in persist_search_snapshot
app.storage.user['word_search_excluded_ids'] = list(state.word_search_excluded_ids or [])
app.storage.user['search_all_terms_filter'] = bool(state._all_terms_filter)

# in restore_search_snapshot
_wse = app.storage.user.get('word_search_excluded_ids')
state.word_search_excluded_ids = set(_wse) if _wse else set()
state._all_terms_filter = bool(app.storage.user.get('search_all_terms_filter', False))
```

Or (b) remove `word_search_excluded_ids` and `search_all_terms_filter`
from `_SEARCH_SNAPSHOT_KEYS` and add a comment making clear they are still
`persist_value`-owned by callers. The current middle state is what's
misleading.

---

### WR-02: `clear_search_snapshot` resets `word_search_excluded_ids` but loses user intent on "New Search"

**File:** `web/pages/search_state.py:343-361` and `web/pages/search.py:1983`
**Issue:** `clear_search_snapshot` resets
`word_search_excluded_ids` to `[]` in storage (line 347) but the
corresponding `SearchUIState.word_search_excluded_ids` is not touched —
only the storage key. Callers such as `search.py:1983` already reset
`search_state.word_search_excluded_ids = set()` on New Search, so the
two paths agree there. However, the helper is also called from
`search.py:820` (`_clear_all_adv_filters`) where there is no corresponding
in-memory reset of `search_state.word_search_excluded_ids`. After
`_clear_all_adv_filters`, storage says `[]` while the live
`SearchUIState` still holds the old exclusion set, so the next
`persist_search_snapshot` call will (via the missing write path in
WR-01 today, or a future fix) re-write them back or leave storage and
state divergent.

**Fix:** Either reset `state.word_search_excluded_ids` inside
`clear_search_snapshot` (but the helper currently takes no `state`
argument — a signature change), or have `_clear_all_adv_filters` at
`search.py:781-821` explicitly reset the in-memory set:

```python
# at search.py ~820, alongside other state resets
search_state.word_search_excluded_ids = set()
search_state._all_terms_filter = False
clear_search_snapshot()
```

The cleanest option is to make `clear_search_snapshot(state=None)`
optionally reset state fields when a `state` is passed, so both
call sites stay symmetric.

---

### WR-03: `restore_browse_snapshot` version-stamp mismatch silently wipes live snapshot

**File:** `web/pages/browse_state.py:122-125`
**Issue:** When `stored_version != _BROWSE_SNAPSHOT_VERSION`,
`restore_browse_snapshot` calls `clear_browse_snapshot()` *before*
returning `(None, None)`. That is correct on schema upgrade, but it also
fires on the **very first visit** of any browser that has pre-existing
`browse_position` / `reading_desk_state` keys written by a pre-Phase-74
build — because those older keys have no `browse_snapshot_schema_version`
stamp, so `stored_version` is `0` and doesn't equal `1`.

In other words, every returning user who had a saved reading desk or
browse position from the prior release will see it silently cleared on
their first load after the upgrade. This is a one-time regression, not a
loop, but it's unannounced data loss.

The equivalent search helper (`restore_search_snapshot`,
`search_state.py:255-258`) has the same behavior, but in practice it's
masked because `restore_search_snapshot` is not wired in (see WR-01).
For browse, the wiring *is* live at `browse.py:4452`.

**Fix:** On first version mismatch (version == 0 meaning "no stamp
written yet"), attempt to read the legacy keys into the return tuple
once, then stamp the new version, rather than clearing. Sketch:

```python
stored_version = app.storage.user.get('browse_snapshot_schema_version', 0)
if stored_version != _BROWSE_SNAPSHOT_VERSION:
    if stored_version == 0:
        # First upgrade: adopt legacy payload, stamp new version.
        pos = app.storage.user.get('browse_position')
        desk = app.storage.user.get('reading_desk_state')
        app.storage.user['browse_snapshot_schema_version'] = _BROWSE_SNAPSHOT_VERSION
        saved_position = pos if pos and pos.get('sys_id') else None
        saved_desk = desk if desk and desk.get('entries') else None
        return (saved_position, saved_desk)
    clear_browse_snapshot()
    return (None, None)
```

Apply the same to `restore_search_snapshot` before wiring it in.

## Info

### IN-01: `restore_browse_snapshot` `state` parameter is unused

**File:** `web/pages/browse_state.py:95`
**Issue:** `restore_browse_snapshot(state: 'BrowseState')` accepts a
`state` argument but never reads or writes it — the function only
returns `(saved_position, saved_reading_desk)` for the caller to use.
The docstring even says "return the raw dicts" as its primary role.
Keeping the parameter is fine for future extensibility, but it's
currently dead weight that implies mutation that doesn't happen.

**Fix:** Either drop the parameter, or add a comment noting it's
reserved for future use (e.g. partial-restore mode).

---

### IN-02: `persist_browse_snapshot(state, page=None)` silently skips position write when `page is None`

**File:** `web/pages/browse_state.py:145-184`
**Issue:** `_persist_reading_desk_state` at `browse.py:1049-1055` calls
`persist_browse_snapshot(state, state.current_page)`. If the current
page hasn't loaded yet (e.g. error path, or a fresh desk add before the
active page mounts), `state.current_page` is `None` and the position
block at lines 162-168 is skipped — intentionally — but the reading-desk
block still runs. That's correct behavior, but the comment "if None,
only reading-desk half is persisted" (line 154) is easy to miss. A test
would help.

**Fix:** Add a unit test asserting that `persist_browse_snapshot(state,
page=None)` writes `reading_desk_state` but leaves `browse_position`
untouched (both when there was a prior position and when there wasn't).

---

### IN-03: `exit_joined_view` wipes `browse_position` along with reading desk

**File:** `web/pages/browse.py:967-978`
**Issue:** `exit_joined_view` calls `clear_browse_snapshot()`, which
also drops `browse_position` and `browse_snapshot_schema_version`. The
inline comment at line 975 acknowledges this as "intended behavior".
However, the user is exiting a *reading-desk-specific* mode, not doing a
full page reset — their last browse position on the single-page view is
arguably not stale. Next visit to `/browse` will skip restore and show
the blank landing.

This is a behavior change vs pre-Phase-74: the pre-refactor code at
browse.py:~1056-1074 only removed `reading_desk_state`, not
`browse_position`.

**Fix:** Split the helper or add a scope flag:

```python
def clear_browse_snapshot(keep_position: bool = False) -> None:
    keys = ['reading_desk_state', 'browse_snapshot_schema_version']
    if not keep_position:
        keys.append('browse_position')
    for key in keys:
        app.storage.user.pop(key, None)
```

Call `clear_browse_snapshot(keep_position=True)` from `exit_joined_view`.

---

### IN-04: `persist_search_snapshot` has bare `except: pass` that swallows serialization errors

**File:** `web/pages/search_state.py:320-321`
**Issue:** The outer `try/except Exception` at line 292/320 swallows any
failure silently — including programming errors like a non-JSON-able
value on `state.results`. The inline comment "Browser storage operation
failed; snapshot not persisted" is accurate for the intended case, but
it will also hide real bugs. The equivalent pattern in
`persist_browse_snapshot` (line 183) logs via `logger.error`, which is
better.

**Fix:** Mirror the browse helper:

```python
except Exception as e:
    logger.error(f"[SearchSnapshot] Error persisting state: {e}")
```

(`search_state.py` currently has no `logger` — add
`import logging; logger = logging.getLogger(__name__)` at the top.)

---

### IN-05: E2E test selector fallback logic is defensive but skips on missing button

**File:** `tests/e2e/test_browse_flow.py:125-134`
**Issue:** If the stable selector doesn't match, the test uses
`pytest.skip(...)` rather than `pytest.fail(...)`. The docstring
explains the rationale (Codex HIGH #10 — avoid matching page-nav
chevrons by accident), but `skip` means a regression that removes the
`aria-label`/`data-action` attrs from `browse.py:1611` / `1804` will
result in the regression test silently being skipped in CI, not failing.

**Fix:** Since the Task 0 contract is that these selectors are stable,
assert their presence up-front and fail (not skip) if missing. Skip
only belongs when the underlying prerequisite (Tantivy index, browser)
is absent — which is already handled at the class level via
`@pytest.mark.skipif(not _has_tantivy_index())`.

```python
assert next_btns, (
    "Next Shelfmark button missing stable selector. "
    "Task 0 must set aria-label='Next manuscript' and "
    "data-action='next-manuscript' on browse.py:1804."
)
```

---

_Reviewed: 2026-04-17_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
