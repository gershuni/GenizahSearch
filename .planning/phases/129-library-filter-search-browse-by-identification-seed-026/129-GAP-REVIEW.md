---
phase: 129-library-filter-search-browse-by-identification-seed-026
reviewed: 2026-06-28T00:00:00Z
depth: standard
files_reviewed: 7
files_reviewed_list:
  - web/pages/search.py
  - web/pages/catalog_browse.py
  - web/components/filter_panel.py
  - genizah_app.py
  - gui_threads.py
  - desktop/dialogs_filter.py
  - genizah_translations.py
findings:
  critical: 0
  warning: 4
  info: 3
  total: 7
status: issues_found
---

# Phase 129: Code Review Report (GAP diff vs 795ccda1)

**Reviewed:** 2026-06-28
**Depth:** standard
**Files Reviewed:** 7 (gap-diff hunks only for the 3 large files)
**Status:** issues_found

## Summary

Reviewed the SEED-026 GAP-closure diff: the library filter was redesigned from
a menu/dropdown (web search), a `ui.select(multiple=True)` (web catalog), and a
checkable `QMenu` (desktop) into a unified **checkbox-dialog** with an inclusion
model, plus "search within results" wiring (GAP-F/GAP-H) and a desktop recompute
fix (FINDING-2).

The core safety invariant the phase set out to protect — **all-unchecked must not
collapse to `[]` (= "show all")** — holds on all three surfaces via a layered
guard: a JS-disabled Apply button + a Python short-circuit (web ×2) and a
disabled OK button + `_on_accept` short-circuit (desktop). The `[] = show all`
mapping is centralized in three identical `_library_apply_selection` /
`library_apply_selection` helpers, each only reachable with a non-empty checked
set. i18n keys for every new string are present (no English leak under Hebrew).
Phase 87 safe_storage chokepoint is respected (`persist_value` → safe_storage;
catalog uses `safe_user_set`). The data layer (GUARD-02) was not touched by this
diff and still fail-OPENs on empty library resolution in `get_browse_results`.

Four WARNING-level issues were found, the most material being a **cross-page
state leak**: a catalog→parallels navigation with an active library filter
persists `search_library_filter` into `/search`'s durable storage (GAP-F's persist
key is the flat literal, and parallels' `consume_incoming_filters` runs the GAP-F
block), so a later fresh `/search` silently inherits a library filter the user set
on a parallels handoff. The remaining WARNINGs concern fail-open/fail-closed
inconsistency between the browse data layer and the desktop search-within recompute,
a dropped validation step on the catalog dialog's checked codes, and the reliability
of the JS `disabled`-button guard on a Quasar `<q-btn>`.

## Warnings

### WR-01: Catalog→parallels handoff leaks the library filter into `/search`'s persisted state

**File:** `web/components/filter_panel.py:344-351`, `web/pages/catalog_browse.py:1331-1334`, `web/pages/parallels.py:252`

**Issue:** The GAP-F block in `consume_incoming_filters` persists under the FLAT
literal key `'search_library_filter'` regardless of `storage_prefix`:

```python
if incoming.get('library_filter'):
    _lib_codes = [str(c) for c in incoming['library_filter']]
    try:
        setattr(state, 'library_filter', _lib_codes)
    except (AttributeError, TypeError):
        pass
    persist_value('search_library_filter', getattr(state, 'library_filter', _lib_codes))
```

`consume_incoming_filters` runs for BOTH `/search` (`'search'`) and `/parallels`
(`'parallels'`, parallels.py:252). The catalog's `_build_incoming_filters`
(catalog_browse.py:1331-1334) puts `library_filter` into `incoming` for BOTH
`_search_in_results` AND `_parallels_in_results` (catalog_browse.py:1351-1367).
So a catalog→**parallels** navigation with an active library filter executes the
GAP-F block and **persists `search_library_filter` into durable user storage**.
parallels never reads/applies it (out of scope, acknowledged), but the next time
the user opens `/search` on a restore path (`restore_saved_exclusions=True`),
search.py:187-189 loads `search_library_filter` and silently filters the results
by a library selection the user made during a *parallels* handoff.

Note the `try/except AttributeError` is effectively dead: `setattr` on a plain
`ParallelsState` instance (no `__slots__`) never raises — it just adds an unused
`library_filter` attribute — so the `getattr(..., _lib_codes)` fallback always
returns the catalog codes and the leak persists.

**Fix:** Only persist when the target page actually consumes a library filter.
Gate on `storage_prefix == 'search'` (parallels does not implement it), e.g.:

```python
if incoming.get('library_filter') and storage_prefix == 'search':
    _lib_codes = [str(c) for c in incoming['library_filter'] if c]
    state.library_filter = _lib_codes
    persist_value('search_library_filter', _lib_codes)
```

Alternatively, stop emitting `library_filter` from `_parallels_in_results`'s
`incoming` dict.

### WR-02: Desktop search-within / FilterCountWorker fail-CLOSED on empty library resolution, contradicting the data layer's fail-OPEN

**File:** `genizah_app.py:10715-10721`, `genizah_app.py:10742-10748`, `gui_threads.py:1291-1294`

**Issue:** `shared/fjms_service.get_browse_results` deliberately fail-OPENs when a
library is selected but resolves to an empty sys_id set (fjms_service.py:2274-2285
— "Selected-but-resolved-to-empty: fail open rather than returning 0 results").
The new desktop search-within and recompute paths do the opposite:

```python
# genizah_app.py _catalog_search_in_results / _catalog_search_in_parallels
if self._catalog_library_filter:
    lib_ids = resolve_library_sys_ids(self._catalog_library_filter, self.meta_mgr)
    if self.pre_search_restrict_sys_ids is None:
        self.pre_search_restrict_sys_ids = lib_ids      # empty set -> zero results
    else:
        self.pre_search_restrict_sys_ids &= lib_ids     # & {} -> zero results
```

`resolve_library_sys_ids` returns `set()` for THREE distinct conditions
(fjms_service.py:3632-3651): empty codes, `meta_mgr is None`, and
LIBRARY_CODES-import failure. `restrict_sys_ids = set()` is interpreted by the
search engine as "restrict to nothing" → **zero results, silently, with no user
notice** (search_engine.py:2036 treats a non-None empty set as an empty
`restrict_uids`). During normal operation (`meta_mgr` loaded, valid codes) this is
correct; but on an infra/timing failure (e.g. `self.meta_mgr` still `None` early in
startup — it is initialized to `None` at genizah_app.py:1185 and only assigned at
:1345) the user sees an unexplained empty result set on "Search in these results",
whereas the equivalent browse on the same selection would have shown all results.

The `FilterCountWorker` has the inverse inconsistency: when `filters['library']`
is set but `self._meta_mgr is None`, the intersect is skipped entirely
(gui_threads.py:1291), silently DROPPING the library restriction (fail-open there).
So the same "meta_mgr unavailable" condition is fail-closed in one site and
fail-open in another.

**Fix:** Mirror the data layer's fail-open guard at the two search-within sites —
only intersect when resolution is non-empty, and log otherwise:

```python
if self._catalog_library_filter:
    lib_ids = resolve_library_sys_ids(self._catalog_library_filter, self.meta_mgr)
    if not lib_ids:
        logger.warning("library filter %s resolved to empty — skipping (fail-open)",
                       self._catalog_library_filter)
    elif self.pre_search_restrict_sys_ids is None:
        self.pre_search_restrict_sys_ids = lib_ids
    else:
        self.pre_search_restrict_sys_ids &= lib_ids
```

Apply the same `if not lib_ids: skip` guard in `FilterCountWorker.run`. (If
fail-closed IS the intended desktop behavior for a legitimately-empty selection,
distinguish it from the meta_mgr-unavailable case so an infra failure does not
masquerade as "no manuscripts in those libraries.")

### WR-03: Catalog dialog Apply drops the LIBRARY_CODES validation the old `ui.select` path enforced

**File:** `web/pages/catalog_browse.py:1115-1123`

**Issue:** The removed `_on_library_filter_change` validated checked codes against
the canonical set before persisting:

```python
# OLD:
current_library_filter['value'] = [c for c in selected if c in LIBRARY_CODES]
```

The new `apply_catalog_library_filter` handler stores the JS readback directly
with no such validation:

```python
new_filter = _library_apply_selection(checked, _all)
current_library_filter['value'] = new_filter
safe_user_set('catalog_library_filter', new_filter)
```

`checked` comes from `catLibFilterGetChecked` reading `cb.dataset.code` off
rendered checkboxes, so in the normal flow every value is a valid code. But a
tampered client could push arbitrary `data-code` values into durable storage.
The downstream `resolve_library_sys_ids` validates and drops unknowns, so this is
not a correctness/security BLOCKER — but it is a silent regression of an existing
defensive step and lets junk accumulate in `catalog_library_filter` storage.

**Fix:** Re-add the validation before persisting:

```python
new_filter = [c for c in _library_apply_selection(checked, _all) if c in LIBRARY_CODES]
current_library_filter['value'] = new_filter
safe_user_set('catalog_library_filter', new_filter)
```

(The web-search dialog reads codes from a facet map keyed by real codes, so it is
less exposed, but the same hardening there would be consistent.)

### WR-04: Apply-disabled guard relies on raw `.disabled` on a Quasar `<q-btn>`; only the Python short-circuit is load-bearing

**File:** `web/pages/search.py:1730-1731, 1803`, `web/pages/catalog_browse.py` (`catLibFilterUpdateApply` + `apply_btn.props(f'id=...')`)

**Issue:** The client-side all-unchecked guard sets `btn.disabled = (n===0)` on the
element found by `document.getElementById("libApplyBtn_"+cid)`, where that id is
applied to a NiceGUI button via `apply_btn.props('id="libApplyBtn_..."')`. NiceGUI
buttons render as Quasar `<q-btn>` Vue components; a raw DOM `.disabled` assignment
on the wrapper is not Quasar's reactive `disable` prop and is not a documented or
pre-existing pattern in this codebase (the sibling domain-filter dialog,
search.py:330-345, has no such disabled-Apply mechanism — it is an exclusion model
with no all-unchecked hazard). If `.disabled` does not take effect on the rendered
button, the client guard is a no-op.

This is NOT a BLOCKER because the Python-side guard is present and correct on all
three surfaces (search.py:1777-1782, catalog_browse.py:`if not checked: ui.notify;
return`, desktop `_on_accept`), so an all-unchecked Apply can never commit `[]`.
But the design comments present the JS-disable as a first line of defense; if it
silently fails, the only feedback is a transient `ui.notify` warning after the user
clicks a button that looked enabled.

**Fix:** Verify the disable actually applies (render-smoke test per the project's
NiceGUI render-smoke convention), or drive disabled state through Quasar's reactive
prop instead of raw `.disabled` (e.g. bind a NiceGUI-managed `disable` prop, or
toggle `btn.props('disable')` / `btn.props(remove='disable')` from a server-side
count rather than client JS). At minimum, document that the Python short-circuit is
the authoritative guard and the JS is cosmetic.

## Info

### IN-01: Orphaned i18n key `"Select libraries..."` after the `ui.select` removal

**File:** `genizah_translations.py:2903`

**Issue:** The catalog `ui.select(... label=tr('Select libraries...'))` was removed
(replaced by the checkbox dialog), and `tests/test_libfilter_catalog.py:453`
asserts the literal is gone from `catalog_browse.py`. The translation entry
`"Select libraries...": "בחר ספריות..."` is now unreferenced dead data.

**Fix:** Remove the orphaned key (low priority; harmless but adds noise).

### IN-02: `_sync_library_menu_checks` retained as an empty no-op

**File:** `genizah_app.py:10454-10459`

**Issue:** After the QMenu→dialog migration, `_sync_library_menu_checks` is now a
documented empty method kept only so `_catalog_remove_filter` (genizah_app.py:10489,
10505) does not `AttributeError`. This is a deliberate, well-commented shim, but it
leaves two dead call sites that could simply be deleted.

**Fix:** Optionally remove the no-op method and its two call sites in
`_catalog_remove_filter` for clarity. Non-blocking.

### IN-03: Repeated `ui.dialog()` construction on every filter-button click

**File:** `web/pages/search.py:1652+` (`_open_library_filter_dialog`), `web/pages/catalog_browse.py:976+` (`_open_library_filter_dialog`)

**Issue:** Each click builds a fresh `ui.dialog()` + card + checkbox HTML without
disposing prior instances, accumulating DOM elements over repeated opens. This
mirrors the existing domain-filter dialog pattern, so it is consistent with the
codebase, and it is a performance concern (explicitly out of v1 review scope). Noted
for awareness only.

**Fix:** None required for this phase; consider reusing a single dialog instance if
the pattern is ever revisited project-wide.

---

## Verification notes (concerns from the review brief)

1. **All-unchecked guard (3 surfaces):** HOLDS. Web search + web catalog: JS
   `libFilterUpdateApply`/`catLibFilterUpdateApply` disable Apply at 0 checked
   PLUS Python `if not checked: notify; return` short-circuit before
   `_library_apply_selection`. Desktop: `_update_ok_button` disables OK at 0 checked
   PLUS `_on_accept` short-circuit. `_library_apply_selection`/`library_apply_selection`
   are only reachable with a non-empty set, so `set(checked)==set(all)` → `[]` is the
   only `[]`-producing path (= legitimate "show all"). See WR-04 re: JS-disable
   reliability — Python guard is the authoritative layer.

2. **GAP-F (catalog→search persist→reload):** WORKS for the documented lifecycle.
   On initial from_browse navigation, `restore_saved_exclusions=False`
   (search_bootstrap.py:107 with from_browse truthy), so search.py:186-189 is
   skipped and `consume_incoming_filters` (search.py:199) sets `state.library_filter`
   directly. On a later bare reload, `restore_saved_exclusions=True` and
   search.py:187-189 reloads the persisted `search_library_filter`. `_apply_library_filter`
   (search.py:3655-3665, called in the `_apply_printed_filter_and_render` cascade at
   :3690) then filters by `display.library_code`. NOTE the filter_panel.py comment
   ("search.py loads it at :187-189") is only true for the reload path, not the
   initial navigation — minor doc imprecision, not a defect. See WR-01 for the
   parallels persist leak.

3. **GAP-H + desktop recompute (FilterCountWorker.meta_mgr):** Resolution +
   intersection run in `run()` off the UI thread (gui_threads.py:1291-1294). All 4
   sites pass `meta_mgr=self.meta_mgr` (genizah_app.py:15451, 24555, 24623, 25260).
   `get_filter_sys_ids` is NOT given a `library` kwarg, so there is no double-handling.
   Empty-resolution None-safety is the WR-02 concern (fail-closed at search-within,
   fail-open at the worker — inconsistent).

4. **Chip relocation (search.py):** CORRECT. Library chips moved to the dedicated
   post-search `library_chip_row` (search.py:1871, populated by `_update_library_chips`
   at :1617-1639). The `has_any` OR-term for `library_filter` was removed from
   `_update_chip_bar` (search.py:1145) so the pre-search bar still renders
   domain/text-position/material chips. Definition-before-use ordering is safe: the
   only render-time `_update_library_chips()` call (search.py:1877) runs AFTER
   `library_chip_row` is created (:1871); the session-restore branch at :1820-1821
   calls only `_update_library_btn()`, not the chip rebuild. No orphaned references to
   the removed `_rebuild_library_menu`/`_toggle_library_code`/`_library_menu_ref`.

5. **Phase 87 / i18n / GUARD-02:** safe_storage chokepoint respected (`persist_value`
   → safe_storage; catalog → `safe_user_set`; no raw `app.storage` for new state). All
   new strings ("Filter by Library", "Select at least one library, or check all to
   clear the filter", plus reused "All Libraries"/"Libraries"/"Select All"/"Apply"/
   "Cancel"/"Showing"/"results"/"Library") are present in TRANSLATIONS; desktop
   `get_library_display(code, short=False)` auto-detects CURRENT_LANG (no English leak
   under Hebrew). GUARD-02: the data layer (`get_browse_results`/`resolve_library_sys_ids`/
   `get_filter_sys_ids`) is NOT in the diff and was not regressed; it still fail-OPENs
   on empty resolution (fjms_service.py:2274-2285) — which is the basis of the WR-02
   inconsistency.

---

_Reviewed: 2026-06-28_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
