# Phase 131: Dual-Mode Parity — Desktop Catalog + Web Browse-by-Identification + Web `/parallels` - Research

**Researched:** 2026-06-30
**Domain:** Library-filter dual-mode parity across three surfaces
**Confidence:** HIGH

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| DMF-07 | Desktop catalog `LibraryFilterDialog` gains Show-only / Hide modes; mode + set persist and re-apply on reopen | Existing dialog is in `desktop/dialogs_filter.py:1677`. Mode toggle = `QButtonGroup` + two `QRadioButton`s (already used in `PreSearchFilterDialog` at line 847). Desktop state is in-memory only — no QSettings, no session JSON for catalog filters; add `self._catalog_library_mode = 'hide'` on GenizahGUI. |
| DMF-08 | Web Browse-by-Identification catalog filter gains Show-only / Hide modes, persisted, composing with existing SEED-023 filters | `web/pages/catalog_browse.py` — modify `_open_library_filter_dialog`, `apply_catalog_library_filter`, `_update_library_filter_btn`, restore at line 116-119, clear sites at lines 1238-1239 and 1260-1263. New persistence key `catalog_library_filter` changes from a plain `list` to `{'mode': ..., 'codes': [...]}`. |
| DMF-09 | Web `/parallels` gains a library-filter control; Show-only/Hide scopes via existing `restrict_sys_ids` path; selection persists for the page | `web/pages/parallels.py` — add `library_mode`/`library_filter` to `ParallelsState`, new `parallels_library_filter` safe_storage key, new filter button + dialog mirroring the `/search` approach, intersect into `restrict_sys_ids` at lines 2177-2224. |
| DMF-10 | `'LOCAL'` absent from web filter options in BOTH modes; existing guards stay green | Every new function iterating `LIBRARY_CODES` must contain the literal `'LOCAL'` comparison. Checked by `tests/test_web_library_options_no_local.py` AST scan + `tests/test_phase_97_invariants.py`. Desktop dialog already excludes LOCAL by construction at `dialogs_filter.py:1698`. |
| DMF-12 | Web Browse-by-Identification library filter gains: client-side text-search, per-library fragment count on shortlist, sort-by-count / sort-A-Z | `web/pages/catalog_browse.py` — the existing `_open_library_filter_dialog` shows a flat list with no counts. Must be rebuilt to match the Phase 130 shortlist-by-count + expand-all-A-Z + text-search pattern from `web/pages/search.py`. Count source: `get_browse_results` total field is per-page not per-library — requires a new facet query OR a per-library count injected from `fjms_service`. |
| DMF-13 | Libraries with zero manuscripts excluded from filter universe on every surface via `library_codes_with_manuscripts()` | `shared/browse_map_utils.library_codes_with_manuscripts()` is fully built (Phase 130, fail-open). Currently used on web `/search`. Must also be applied to catalog/parallels dialog build and to `LibraryFilterDialog._all_codes` on desktop. |
</phase_requirements>

---

## Summary

Phase 131 is a pure parity phase — it mirrors the dual-mode (mode + set) library filter model settled in Phase 130 onto three remaining surfaces without redesigning anything. The core algorithm and UX shape are locked: two-mode toggle, shortlist-by-count + expand-all + text-search dialog, persist `{'mode': ..., 'codes': [...]}`, button labels "Filter by library" / "Showing N/total" / "Hiding N", LOCAL excluded by construction.

The three surfaces are structurally similar but differ in their persistence mechanisms and result-count sources. Web catalog (`catalog_browse.py`) already has the full dialog infrastructure from Phase 129 — it only needs the mode dimension added to the existing dialog and persist key. Web `/parallels` has no library filter at all today — it needs a new button + dialog attached to the existing `restrict_sys_ids` computation path. The desktop `LibraryFilterDialog` is a standalone QDialog that currently knows nothing about mode — it needs a `QButtonGroup` mode toggle and mode-aware `get_checked_codes()`.

**Primary recommendation:** Implement as three independent plans in one wave (they share no code), each following the Phase 130 analog pattern exactly. Desktop can be parallelized from the two web surfaces.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Library filter mode + code set (show/hide) | Browser / Client (web) + Desktop App | — | Post-search client-side filter applied over already-fetched results (web search); pre-query scope restriction (web parallels + catalog, desktop) |
| Persisting filter mode+set | Frontend Server (SSR) via safe_storage (web) / In-memory instance var (desktop) | — | Web uses safe_storage chokepoint; desktop has no cross-session persistence for catalog filters |
| Resolving library codes → sys_ids for restrict_sys_ids | API / Backend (shared/fjms_service) | — | `resolve_library_sys_ids` runs in io_bound off-event-loop; must stay off the UI thread |
| Per-library manuscript count (DMF-12) | Database / Storage (shared/fjms_service) | — | `get_browse_results` returns total, not per-library breakdown; count source is the current result-derived facets on web `/search` (already built); for catalog, requires a new per-library count query or reuse of the result total from the existing browse query |
| DMF-10 LOCAL exclusion guard | Browser / Client (web page functions) + Desktop dialog constructor | — | AST guard ensures `'LOCAL'` literal appears in every web page function that iterates `LIBRARY_CODES` |

---

## Standard Stack

No new packages. Phase 131 uses:

- **PyQt6** (desktop) — `QButtonGroup`, `QRadioButton` for mode toggle (already used in `PreSearchFilterDialog`)
- **NiceGUI** (web) — `ui.toggle` for mode control (already used in Phase 130 `/search` dialog)
- `shared/browse_map_utils.py` — `LIBRARY_CODES`, `get_library_display`, `library_codes_with_manuscripts`, `sanitize_library_codes` (all already used)
- `web/safe_storage.py` — `safe_user_get` / `safe_user_set` chokepoint

[VERIFIED: codebase grep] All referenced symbols exist at the locations noted below.

## Package Legitimacy Audit

No new external packages are installed in this phase.

---

## Architecture Patterns

### System Architecture Diagram

```
Surface A: Desktop catalog LibraryFilterDialog
  GenizahGUI._catalog_library_filter (list)   ←─── persists in-memory only (session lifetime)
  GenizahGUI._catalog_library_mode (str)      ←─── NEW: 'show_only'|'hide', in-memory only
        │
        ▼
  LibraryFilterDialog(mode=..., selected_codes=...)
    ┌─ QButtonGroup (Show only | Hide) ─── sets mode (D-03/D-04)
    └─ QListWidget (checkboxes)
        │ OK accepted
        ▼
  library_apply_selection_dual(checked, all_codes, mode) → (mode, new_filter_list)
        │
        ▼
  _catalog_update_library_filter_btn()   ─── 3-state label
  _catalog_start_async_refresh()         ─── passes mode+codes to _CatalogRefreshWorker
        │ (mode=show_only → include filter, mode=hide → exclude filter)
        ▼
  fjms_service.get_browse_results(library_codes=..., library_sys_ids=...)


Surface B: Web Browse-by-Identification (catalog_browse.py)
  safe_user_get('catalog_library_filter')
  → {'mode': ..., 'codes': [...]}          ←─── NEW dict shape (was plain list)
        │
        ▼
  _open_library_filter_dialog()
    ┌─ ui.toggle (Show only | Hide) ─── current_mode[0] closure (D-03/D-04)
    ├─ ui.input (text search) ─── libFilterSearch JS
    ├─ count-shortlist section ─── per-library fragment counts from catalog data
    └─ expand-all section ─── A-Z, no counts
        │ apply_catalog_library_filter()
        ▼
  current_library_filter['value'] = new_filter
  current_library_mode['value'] = mode     ←─── NEW state var
  safe_user_set('catalog_library_filter', {'mode':..., 'codes':[...]})
        │
        ▼
  _fetch_results_blocking(library_codes=..., library_mode=...) ─── mode-branch inside
        │
        ▼
  fjms_service.get_browse_results(library_codes=..., library_sys_ids=...)


Surface C: Web /parallels (parallels.py)
  safe_user_get('parallels_library_filter')
  → {'mode': ..., 'codes': [...]}          ←─── NEW: no prior library filter existed
        │
        ▼
  new library filter button + dialog (same pattern as /search)
        │ apply
        ▼
  p_state.library_mode = mode              ←─── NEW ParallelsState fields
  p_state.library_filter = codes
  safe_user_set('parallels_library_filter', {'mode':..., 'codes':[...]})
        │
  start_search()
        │
        ▼
  restrict_sys_ids = None
  if _has_active_filters() OR library filter is active:
    if library filter active:
      lib_ids = await run.io_bound(resolve_library_sys_ids, codes, state.meta_mgr)
      restrict_sys_ids = lib_ids (if show_only) OR complement (if hide)
    ... merge with other filter restrict_sys_ids ...
        │
        ▼
  state.searcher.search_composition_logic(..., restrict_sys_ids=...)
```

### Recommended Project Structure

No new files needed. All changes are in-place modifications to:
- `desktop/dialogs_filter.py` — `LibraryFilterDialog` class
- `genizah_app.py` — `_catalog_library_mode` field, `_open_catalog_library_dialog`, `_catalog_update_library_filter_btn`, `_CatalogRefreshWorker` ctor
- `web/pages/catalog_browse.py` — dialog, restore, button, apply, clear sites
- `web/pages/parallels.py` — `ParallelsState`, restore, new button/dialog, filter logic

### Pattern 1: Desktop Mode Toggle (QButtonGroup + QRadioButton)

Analog: `PreSearchFilterDialog` at `desktop/dialogs_filter.py` lines 843–855.

```python
# Source: desktop/dialogs_filter.py:843-855 (PreSearchFilterDialog __init__)
mode_layout = QHBoxLayout()
self._mode_group = QButtonGroup(self)
self._rb_include = QRadioButton(tr("Include"))
self._rb_exclude = QRadioButton(tr("Exclude"))
self._mode_group.addButton(self._rb_include, 1)
self._mode_group.addButton(self._rb_exclude, 2)
include_mode = self._current_filters.get('include_mode', True)
self._rb_include.setChecked(include_mode)
self._rb_exclude.setChecked(not include_mode)
self._mode_group.buttonToggled.connect(self._on_filter_changed)
```

For `LibraryFilterDialog`, analogously:

```python
# In LibraryFilterDialog.__init__, after setWindowTitle:
mode_layout = QHBoxLayout()
self._mode_group = QButtonGroup(self)
self._rb_show_only = QRadioButton(tr("Show only selected"))
self._rb_hide = QRadioButton(tr("Hide selected"))
self._mode_group.addButton(self._rb_show_only, 1)
self._mode_group.addButton(self._rb_hide, 2)
if current_mode == 'show_only':
    self._rb_show_only.setChecked(True)
else:
    self._rb_hide.setChecked(True)
self._mode_group.buttonToggled.connect(self._on_mode_changed)
layout.addLayout(mode_layout)
```

D-04 reset on mode flip:

```python
def _on_mode_changed(self):
    """D-04: mode flip resets the checked set (starts fresh/empty)."""
    self.list_widget.blockSignals(True)
    for i in range(self.list_widget.count()):
        self.list_widget.item(i).setCheckState(Qt.CheckState.Unchecked)
    self.list_widget.blockSignals(False)
    self._update_ok_button()
```

OK guard update for dual mode: in Hide mode, zero items checked IS valid (D-05 default). Disable OK only when mode is Show-only AND zero items are checked.

```python
def _update_ok_button(self):
    mode = self.get_mode()
    checked_count = sum(
        1 for i in range(self.list_widget.count())
        if self.list_widget.item(i).checkState() == Qt.CheckState.Checked
    )
    if mode == 'hide':
        self.ok_button.setEnabled(True)      # empty hide = show all (D-05/D-08)
        self._hint_label.setVisible(False)
    else:  # show_only
        self.ok_button.setEnabled(checked_count > 0)
        self._hint_label.setVisible(checked_count == 0)

def get_mode(self) -> str:
    return 'show_only' if self._rb_show_only.isChecked() else 'hide'
```

[VERIFIED: codebase] Pattern derived from existing `PreSearchFilterDialog` and `LibraryFilterDialog` in `desktop/dialogs_filter.py`.

### Pattern 2: Web Catalog Restore — dict shape with legacy migration

[VERIFIED: codebase] Phase 130 established the migration pattern in `web/pages/search.py` lines 188–210.

For catalog, at `web/pages/catalog_browse.py` lines 116–119, replace the current plain-list restore:

```python
# CURRENT (Phase 129):
_lib0 = safe_user_get('catalog_library_filter', [])
current_library_filter = {
    'value': [c for c in _lib0 if c in LIBRARY_CODES] if isinstance(_lib0, list) else []
}

# NEW (Phase 131) — mirrors search.py D-06 migration pattern:
_lib_raw = safe_user_get('catalog_library_filter', None)
if isinstance(_lib_raw, list):
    # Legacy plain-list (Phase 129 shape) → migrate to Show-only
    _lib_codes = [c for c in _lib_raw if c in LIBRARY_CODES and c != 'LOCAL']
    if _lib_codes:
        _cat_library_mode = 'show_only'
        _cat_library_codes = _lib_codes
    else:
        _cat_library_mode = 'hide'
        _cat_library_codes = []
elif isinstance(_lib_raw, dict):
    _m = _lib_raw.get('mode', 'hide')
    _cat_library_mode = _m if _m in ('show_only', 'hide') else 'hide'
    _cat_library_codes = [c for c in (_lib_raw.get('codes') or [])
                          if c in LIBRARY_CODES and c != 'LOCAL']
else:
    _cat_library_mode = 'hide'
    _cat_library_codes = []
current_library_mode = {'value': _cat_library_mode}
current_library_filter = {'value': _cat_library_codes}
```

### Pattern 3: Web `/parallels` library filter as a new `restrict_sys_ids` contributor

[VERIFIED: codebase] `web/pages/parallels.py` lines 2177–2224. The parallels library filter must intersect into `restrict_sys_ids`. Two cases:

- **Show-only mode:** `restrict_sys_ids = await run.io_bound(resolve_library_sys_ids, codes, state.meta_mgr)` → intersect into existing restrict
- **Hide mode:** Resolve ALL-library sys_ids, subtract the hidden-library sys_ids. Simpler alternative: resolve the hidden library sys_ids, then intersect `restrict_sys_ids` with `all_genizah_ids - hide_ids`. But the cleanest path is: resolve hide-set codes to sys_ids, then if `restrict_sys_ids is None`, set to `all_ids - hide_ids`; if `restrict_sys_ids is not None`, apply `restrict_sys_ids -= hide_ids`. `resolve_library_sys_ids` accepts a list of codes and returns their sys_id set — for Hide mode call with the hidden codes, then subtract.

Concrete integration at the search-start path:

```python
# After line 2208 (restrict_sys_ids computation for advanced filters):
if p_state.library_filter:   # non-empty codes
    lib_codes = p_state.library_filter
    lib_mode = getattr(p_state, 'library_mode', 'hide')
    lib_ids = await run.io_bound(resolve_library_sys_ids, lib_codes, state.meta_mgr)
    if lib_ids:
        if lib_mode == 'show_only':
            if restrict_sys_ids is None:
                restrict_sys_ids = lib_ids
            else:
                restrict_sys_ids &= lib_ids
        else:  # hide
            if restrict_sys_ids is None:
                pass  # Can't compute complement without full corpus — use lib_ids as exclusion
                # Practical approach: intersect with (resolve_library_sys_ids(all_non_hidden))
                # OR use the same pattern as search: filter results post-fetch
            else:
                restrict_sys_ids -= lib_ids
```

**Note on Hide mode for parallels:** `restrict_sys_ids` is a set of IDs to INCLUDE. For Hide mode the complement approach requires knowing all valid IDs, which is expensive. The cleanest equivalent to the web `/search` filter cascade is: compute restrict from advanced filters as normal, then subtract the hide-set IDs. This works when restrict is not None (narrowing). When restrict is None (no other filters), hide-mode IDs are subtracted from a "full corpus" placeholder — or the page can compute `resolve_library_sys_ids(hidden_codes)` and set `p_state.hidden_library_ids` for use in a post-fetch filter applied to results (same as how web `/search` does it: `_apply_library_filter` runs over the full result list, not pre-query). **Recommendation for the planner: use the same post-fetch filter pattern as web `/search` for consistency.** Add `p_state.library_mode` and `p_state.library_filter` to `ParallelsState`, and filter the results list before display using the same mode-branch logic as `_apply_library_filter` in `search.py`.

### Pattern 4: DMF-12 per-library count source for catalog dialog

[ASSUMED] The web Browse-by-Identification catalog dialog currently shows no counts. The catalog page queries `fjms_service.get_browse_results` which returns a `{'results': [...], 'total': N}` dict — it does NOT return per-library breakdown. Three options:

1. **Use the existing result data from the latest `refresh_results()` call** — iterate `results_container` rows and count by library_code. This mirrors how `_compute_library_facets` works in `search.py`. The count reflects only the current filtered results page, not the full corpus.
2. **Add a new `fjms_service.get_library_facets(domain, author, work, ...)` call** — queries COUNT GROUP BY library_code. More accurate but a new DB call.
3. **Call `get_browse_results` with no offset/limit just for facets** — heavy, not recommended.

**Planner recommendation:** Use option 1 (result-derived facets from `current_results`) for the shortlist, mirroring the web `/search` pattern. The expand section shows libraries not in the current result set with no count. This avoids a new DB query and is consistent with the Phase 130 model.

The result data is available via a `current_results` dict that the catalog page builds from `refresh_results`. Read from it to compute `{library_code: count}` facets.

### Anti-Patterns to Avoid

- **Don't use `QSettings` for the desktop catalog library mode** — existing catalog filters (pgp, editions, library) are in-memory instance variables only. No cross-session persistence exists or is needed for the desktop catalog tab. The mode should follow the same in-memory pattern.
- **Don't iterate `LIBRARY_CODES` in a web page function without the `'LOCAL'` guard** — the AST guard in `tests/test_web_library_options_no_local.py` fails CI. Every new function in `web/pages/*.py` that references `LIBRARY_CODES` must also contain the string literal `'LOCAL'` somewhere in the function body.
- **Don't pass a raw empty set to `get_browse_results`** — existing code has the "Pitfall 5" fail-open: `lib_sys_ids = resolved if resolved else None` (never `set()`). Preserve this everywhere.
- **Don't put `<script>` inside `ui.html()` blocks** — BUG-B pattern from Phase 129/130; JS must be at page level via `ui.add_head_html`.
- **Don't use per-item `ui.checkbox` in the dialog** — causes 7–19s open time; always use a single `ui.html()` block.
- **Don't apply `library_codes_with_manuscripts()` synchronously in a web event handler** — it reads `libraries.csv` on first call (then caches); call it lazily on first dialog open or at page init in an io_bound context.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Mode toggle (web) | Custom HTML/CSS tabs | `ui.toggle({'show_only': ..., 'hide': ...})` | Already used in Phase 130; renders as Quasar segmented control |
| Mode toggle (desktop) | Custom stylesheet faked buttons | `QButtonGroup` + two `QRadioButton`s | Already used in `PreSearchFilterDialog` at line 843; standard Qt pattern |
| Library → sys_id resolution | Custom csv lookup | `resolve_library_sys_ids(codes, meta_mgr)` from `shared.fjms_service` | Already handles all resolution logic, caching, and fail-open |
| Non-zero-manuscript filtering | Re-implementing the CSV scan | `library_codes_with_manuscripts()` from `shared/browse_map_utils.py` | Already built in Phase 130; fail-open; cached after first call |
| Code sanitization | Ad-hoc isinstance checks | `sanitize_library_codes(raw)` from `shared/browse_map_utils.py` | Already handles all edge cases; contains the `'LOCAL'` guard literal |
| JS text search in dialog | Custom Python filter loop | `libFilterSearch(cid, query)` JS helper (already in page-level JS from Phase 130) | Already defined in `web/pages/search.py`; reuse or duplicate at page level for catalog/parallels |

---

## Runtime State Inventory

Phase 131 is not a rename/refactor/migration phase. No runtime state inventory required.

---

## Common Pitfalls

### Pitfall 1: Desktop mode NOT persisted to session JSON — and that is correct

**What goes wrong:** Assuming the desktop `_catalog_library_mode` needs to be saved to `session_persistence.py` like My Library state.
**Why it happens:** Other desktop state (search results, LAB state) uses session JSON persistence.
**How to avoid:** Check the existing catalog filter state: `_catalog_pgp_filter`, `_catalog_editions_filter`, and `_catalog_library_filter` are ALL in-memory only (never read from or written to session). The new `_catalog_library_mode` must follow the same pattern — a fresh `'hide'` default on each app launch. This matches D-05 and is intentional.
**Warning signs:** If you grep `session_persistence` near `_catalog_pgp_filter` you'll find nothing — that's the pattern.

### Pitfall 2: Web catalog persist key shape change requires migration at restore

**What goes wrong:** The existing `catalog_library_filter` key stores a plain `list`. Phase 131 changes it to `{'mode': ..., 'codes': [...]}`. A user who has saved a Phase 129 plain list will have `isinstance(_lib_raw, list)` = True — that must be handled gracefully (migrate to Show-only, same as `search_library_filter` in Phase 130).
**Why it happens:** The D-06 migration pattern was built for `search_library_filter` in Phase 130 and must be replicated for `catalog_library_filter`.
**How to avoid:** Copy the 3-branch restore from `search.py` lines 188–210 (see Pattern 2 above). Test with a mock value `['CUL', 'JTS']` → should produce `mode='show_only', codes=['CUL','JTS']`.
**Warning signs:** If the restore for `catalog_library_filter` only has `isinstance(_lib_raw, list)` without the `dict` branch, the Phase 131 shape won't round-trip.

### Pitfall 3: LOCAL exclusion in the new web dialog functions fails the AST guard

**What goes wrong:** `test_web_library_options_no_local.py` scans ALL functions in `web/pages/*.py` that reference `LIBRARY_CODES`. If the new `_open_library_filter_dialog` redesign (or the restore block, which is NOT in a function) doesn't contain the literal string `'LOCAL'`, the AST guard fails.
**Why it happens:** The guard checks `ast.Constant` with `value == "LOCAL"` — it looks for the string literal `'LOCAL'` anywhere in the function body. `sanitize_library_codes()` is in `shared/`, not the web page, so calling it alone doesn't satisfy the guard.
**How to avoid:** The dialog build and the Apply handler must both contain an inline `c != 'LOCAL'` guard when iterating `LIBRARY_CODES` — even if `sanitize_library_codes` is also called. Example: `all_codes = [c for c in LIBRARY_CODES if c != 'LOCAL']`. The guard looks for the literal inside the function, not a transitive call.
**Warning signs:** Run `pytest tests/test_web_library_options_no_local.py` after adding the new function.

### Pitfall 4: Parallels page library filter in Hide mode without a pre-existing restrict set

**What goes wrong:** When there are no Advanced Filters active, `restrict_sys_ids` is None. For Hide mode, subtracting hide-set IDs from None is a TypeError.
**Why it happens:** `restrict_sys_ids` starts as `None` (meaning "no restriction — full corpus"). Hide mode can't be applied as a set subtraction without a concrete set.
**How to avoid:** For the parallels page, treat the library filter as a POST-FETCH result filter (same model as web `/search`) rather than a pre-query `restrict_sys_ids` constraint. This means applying `_apply_library_filter_dual(results, mode, codes)` to `p_state.results` after the search completes, rather than intersecting into `restrict_sys_ids`. For Show-only mode, `restrict_sys_ids` intersection is also viable and more efficient for large corpora.
**Warning signs:** A `TypeError: unsupported operand type(s) for -=` or an `AttributeError` on `None` in the search path.

### Pitfall 5: `library_codes_with_manuscripts()` called on the event loop

**What goes wrong:** First call reads `libraries.csv` (~255K rows). If called synchronously in a page render or button click handler, it blocks the event loop for 0.5–2s.
**Why it happens:** The function is cached after first call but that first call is expensive.
**How to avoid:** Call it inside an `io_bound` worker (e.g., wrap in `_fetch_results_blocking`) or ensure it's populated at page-init time before any user interaction triggers the dialog.

### Pitfall 6: Desktop `LibraryFilterDialog` mode resets on each open

**What goes wrong:** The dialog is reconstructed on each button click. If it opens fresh with `mode='hide'` regardless of the persisted mode, the user's previous choice is lost.
**How to avoid:** Pass the current in-memory `_catalog_library_mode` to the dialog constructor: `dlg = LibraryFilterDialog(self, mode=self._catalog_library_mode, selected_codes=list(self._catalog_library_filter))`. Read it back after `exec()`: `self._catalog_library_mode, self._catalog_library_filter = dlg.get_mode(), ...`.

---

## Code Examples

### LibraryFilterDialog — current constructor signature

```python
# Source: desktop/dialogs_filter.py:1693
def __init__(self, parent=None, *, selected_codes: list | None = None):
```

New signature needed:

```python
def __init__(self, parent=None, *, mode: str = 'hide', selected_codes: list | None = None):
```

### `_open_catalog_library_dialog` in genizah_app.py — current form

```python
# Source: genizah_app.py:10430-10439
def _open_catalog_library_dialog(self):
    all_codes = [c for c in LIBRARY_CODES.keys() if c != 'LOCAL']
    dlg = LibraryFilterDialog(self, selected_codes=list(self._catalog_library_filter))
    if dlg.exec() == QDialog.DialogCode.Accepted:
        self._catalog_library_filter = library_apply_selection(
            dlg.get_checked_codes(), all_codes
        )
        self._catalog_update_library_filter_btn()
        self._catalog_current_page = 0
        self._catalog_start_async_refresh(refresh_authors=False, refresh_works=False)
```

New form:

```python
def _open_catalog_library_dialog(self):
    all_codes = [c for c in library_codes_with_manuscripts() if c != 'LOCAL']
    dlg = LibraryFilterDialog(self, mode=self._catalog_library_mode,
                              selected_codes=list(self._catalog_library_filter))
    if dlg.exec() == QDialog.DialogCode.Accepted:
        mode = dlg.get_mode()
        checked = dlg.get_checked_codes()
        if mode == 'show_only':
            new_filter = library_apply_selection(checked, all_codes)  # all-checked -> []
        else:  # hide
            new_filter = list(checked)  # empty hide-set is valid (D-05)
        self._catalog_library_mode = mode
        self._catalog_library_filter = new_filter
        self._catalog_update_library_filter_btn()
        self._catalog_current_page = 0
        self._catalog_start_async_refresh(refresh_authors=False, refresh_works=False)
```

### ParallelsState new fields

```python
# Source: web/pages/parallels.py:156 (class ParallelsState)
# NEW FIELDS to add:
self.library_filter: list = []   # parallels library-filter codes (active set)
self.library_mode: str = 'hide'  # 'show_only' | 'hide' (D-05 default)
```

### Desktop 3-state button label update

```python
# In _catalog_update_library_filter_btn — new 3-state logic:
def _catalog_update_library_filter_btn(self):
    if not hasattr(self, '_catalog_library_filter_btn'):
        return
    btn = self._catalog_library_filter_btn
    mode = getattr(self, '_catalog_library_mode', 'hide')
    flt = self._catalog_library_filter
    total = len([c for c in LIBRARY_CODES.keys() if c != 'LOCAL'])
    if not flt:
        btn.setText(tr("Filter by library"))
        btn.setStyleSheet("QPushButton { text-align: left; padding: 2px 8px; font-size: 11px; }")
    elif mode == 'show_only':
        shown = len(flt)
        btn.setText(tr("Filter by library") + f" ({shown}/{total})")
        btn.setStyleSheet(
            "QPushButton { text-align: left; padding: 2px 8px; font-size: 11px; "
            "background-color: #d32f2f; color: white; border: none; border-radius: 3px; }"
            "QPushButton:hover { background-color: #b71c1c; }"
        )
    else:  # hide mode, non-empty set
        n = len(flt)
        btn.setText(tr("Hiding") + f" {n}")
        btn.setStyleSheet(
            "QPushButton { text-align: left; padding: 2px 8px; font-size: 11px; "
            "background-color: #e65100; color: white; border: none; border-radius: 3px; }"
            "QPushButton:hover { background-color: #bf360c; }"
        )
```

### `library_codes_with_manuscripts()` — confirmed signature

```python
# Source: shared/browse_map_utils.py:123
def library_codes_with_manuscripts() -> frozenset:
    """Fail-open: returns frozenset(LIBRARY_CODES) when libraries.csv is unavailable."""
```

Usage pattern for dialog builds (DMF-13):

```python
# For web pages — wrap in a dict-key check:
all_codes = [c for c in library_codes_with_manuscripts() if c != 'LOCAL']
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|-----------------|--------------|--------|
| Desktop catalog library button = QMenu of checkable QActions | `LibraryFilterDialog` (QDialog + QListWidget) | Phase 129 (v8.3.0) | Dialog allows OK-guard and type-ahead; QMenu didn't |
| Web catalog `ui.select(multiple=True)` | `ui.dialog` + `ui.html` checkboxes | Phase 129 (v8.3.0) | Consistent with search; avoids 7-19s open time |
| Inclusion-only allowlist (coded-list = show-only) | Dual-mode (mode + set): Show-only \| Hide | Phase 130 (v8.4.0, web /search) | Phase 131 mirrors this to remaining surfaces |
| `library_codes_with_manuscripts()` absent | Built in Phase 130 in `shared/browse_map_utils.py` | Phase 130 (2026-06-30) | Phase 131 reuses it on catalog/parallels/desktop |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Desktop catalog filter state is in-memory only (no QSettings, no session JSON for pgp/editions/library). Derived from absence of any session-persistence write calls near these fields in genizah_app.py. | Pitfall 1, Standard Stack | If wrong: mode would need to be read from session JSON on launch — a 2-line addition, low risk |
| A2 | DMF-12 per-library count source for catalog is result-derived (from `current_results` data), not a new DB query | Pattern 4, Common Pitfalls | If a new fjms DB query is required: adds a new `get_library_facets()` method to fjms_service; medium complexity |
| A3 | Parallels library filter should be implemented as a post-fetch result filter (same as web /search) rather than as a pre-query restrict_sys_ids contributor for Hide mode | Architecture Patterns, Pitfall 4 | If restrict-based Hide is required: needs full-corpus sys_id resolution; significantly more complex |

---

## Open Questions

1. **DMF-12 count source for catalog browse dialog**
   - What we know: `get_browse_results` returns `{'results': [], 'total': N}` but total is the count for all active filters, not per-library. Current results data includes `library_code` per row.
   - What's unclear: Should counts be derived from current page results (partial), from a separate COUNT query, or from a cached full-result scan?
   - Recommendation: Use result-derived facets from the current result rows (same pattern as `/search`). This is consistent, no new DB calls, and the expand-section handles libraries not in current results with no count.

2. **Parallels library filter: post-fetch vs pre-query for Hide mode**
   - What we know: `/search` uses a post-fetch result filter; `/parallels` uses `restrict_sys_ids` as a pre-query scope restrictor.
   - What's unclear: Should Hide mode on parallels be pre-query (requires complement resolution) or post-fetch (mirrors /search)?
   - Recommendation: Post-fetch filter for parallels library filter (both modes). This avoids full-corpus ID resolution for Hide mode and is consistent with the web `/search` pattern. The planner should confirm this.

3. **New translation keys needed**
   - `"Show only selected"` — added in Phase 130 (VERIFIED in 130-02-SUMMARY.md)
   - `"Hide selected"` — added in Phase 130
   - `"Hiding"` — added in Phase 130
   - `"Search libraries..."` — added in Phase 130
   - No new translation keys required for Phase 131 (all needed keys are already in `genizah_translations.py`).

---

## Environment Availability

Phase 131 is a code-only change with no external dependencies.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (no GUI for web; `gui-tests` marker for desktop Qt) |
| Config file | `tests/conftest.py` (existing) |
| Quick run command | `pytest tests/test_libfilter_desktop.py tests/test_web_library_options_no_local.py tests/test_phase_97_invariants.py tests/test_no_raw_storage_access.py tests/test_dual_mode_library_filter.py -x -q` |
| Full suite command | `pytest tests/ -q --ignore=tests/test_libfilter_desktop.py` then `pytest tests/test_libfilter_desktop.py tests/test_catalog_availability_filter.py -q` (gui-tests split per project conventions) |
| GUI test marker | `@pytest.mark.gui` or explicit inclusion in `_GUI_TEST_FILES` in `conftest.py` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| DMF-07 | Desktop `LibraryFilterDialog` offers Show-only/Hide mode toggle; mode+set returned correctly; LOCAL absent in both modes | unit (gui-marked) | `pytest tests/test_libfilter_desktop.py -x -q` | Extend existing ❌ Wave 0 |
| DMF-07 | Desktop `_catalog_library_mode` initialized to 'hide'; mode passed through to `_CatalogRefreshWorker` | unit (gui-marked) | `pytest tests/test_libfilter_desktop.py -x -q` | Extend existing ❌ Wave 0 |
| DMF-08 | Web catalog restore migrates plain list → Show-only; dict shape round-trips | unit (pure mirror) | `pytest tests/test_catalog_dual_mode_library_filter.py -x -q` | ❌ Wave 0 new file |
| DMF-08 | `apply_catalog_library_filter` persists dict shape; `c != 'LOCAL'` guard present | AST scan | `pytest tests/test_catalog_dual_mode_library_filter.py -x -q` | ❌ Wave 0 |
| DMF-08 | Web catalog SEED-023 filters (pgp/editions) unaffected by library-mode change | unit | `pytest tests/test_catalog_availability_filter.py -x -q` | ✅ existing — regression check only |
| DMF-09 | `ParallelsState` has `library_mode`/`library_filter` fields with correct defaults | unit | `pytest tests/test_parallels_library_filter.py -x -q` | ❌ Wave 0 new file |
| DMF-09 | Parallels page restores from `parallels_library_filter` key; migrates legacy list | unit | `pytest tests/test_parallels_library_filter.py -x -q` | ❌ Wave 0 |
| DMF-09 | Parallels library filter applies as post-fetch filter (Show-only IN set, Hide NOT-IN set) | unit (pure mirror) | `pytest tests/test_parallels_library_filter.py -x -q` | ❌ Wave 0 |
| DMF-10 | `'LOCAL'` absent from all new web page functions that reference `LIBRARY_CODES` | AST guard (existing) | `pytest tests/test_web_library_options_no_local.py tests/test_phase_97_invariants.py -x -q` | ✅ existing — must stay green |
| DMF-10 | `tests/test_no_raw_storage_access.py` allowlist stays `[]` | guard (existing) | `pytest tests/test_no_raw_storage_access.py -x -q` | ✅ existing |
| DMF-12 | Web catalog dialog has count-shortlist + expand-all + text-search | AST source scan | `pytest tests/test_catalog_dual_mode_library_filter.py -x -q` | ❌ Wave 0 |
| DMF-13 | `library_codes_with_manuscripts()` used in desktop dialog + web catalog dialog + web parallels dialog | AST source scan | `pytest tests/test_catalog_dual_mode_library_filter.py tests/test_parallels_library_filter.py tests/test_libfilter_desktop.py -x -q` | ❌ Wave 0 new |

### Sampling Rate

- **Per task commit:** `pytest tests/test_web_library_options_no_local.py tests/test_phase_97_invariants.py tests/test_no_raw_storage_access.py -x -q`
- **Per wave merge:** full quick run command above
- **Phase gate:** All tests green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/test_catalog_dual_mode_library_filter.py` — new file; covers DMF-08, DMF-12, DMF-10 (catalog surface); AST source contracts + pure-mirror behavior tests
- [ ] `tests/test_parallels_library_filter.py` — new file; covers DMF-09, DMF-10 (parallels surface); ParallelsState defaults, restore migration, filter behavior
- [ ] `tests/test_libfilter_desktop.py` — extend existing file; add tests for: `LibraryFilterDialog` mode parameter, `get_mode()` method, D-04 reset behavior, OK guard mode-awareness, `_catalog_library_mode` field on GenizahGUI

*(Existing infrastructure covers all other needs; no new framework installs needed.)*

---

## Security Domain

Phase 131 adds no network endpoints, auth paths, or schema changes. The existing security controls apply:

- **V5 Input Validation:** All new library-code inputs pass through `sanitize_library_codes()` and inline `c != 'LOCAL'` guards before being persisted or used as filters.
- **V3 Session Management:** Web persistence uses the `safe_storage` chokepoint exclusively (no raw `app.storage.user`). Desktop is in-memory only.
- No new threat surface beyond what was analyzed for Phase 130.

---

## Sources

### Primary (HIGH confidence)
- `desktop/dialogs_filter.py:1677–1820` — `LibraryFilterDialog` current implementation (VERIFIED: codebase read)
- `desktop/dialogs_filter.py:840–860` — `PreSearchFilterDialog` QButtonGroup pattern (VERIFIED: codebase read)
- `web/pages/catalog_browse.py:100–120, 956–1145` — catalog library filter current state + dialog + button (VERIFIED: codebase read)
- `web/pages/parallels.py:155–268, 2150–2270` — `ParallelsState`, restore, `restrict_sys_ids` path (VERIFIED: codebase read)
- `shared/browse_map_utils.py:123–201` — `library_codes_with_manuscripts()` + `sanitize_library_codes()` (VERIFIED: codebase read)
- `genizah_app.py:9597–9604, 10430–10439, 10444–10464` — desktop catalog filter state + dialog invocation + button update (VERIFIED: codebase read)
- `.planning/phases/130-dual-mode-filter-core-web-search/130-CONTEXT.md` — locked decisions D-01..D-10 (VERIFIED: read)
- `.planning/phases/130-dual-mode-filter-core-web-search/130-PATTERNS.md` — Phase 130 analog map (VERIFIED: read)
- `.planning/phases/130-dual-mode-filter-core-web-search/130-02-SUMMARY.md` — Phase 130 dialog implementation details (VERIFIED: read)
- `.planning/phases/130-dual-mode-filter-core-web-search/130-03-SUMMARY.md` — Phase 130 test patterns (VERIFIED: read)
- `tests/test_web_library_options_no_local.py` — AST guard logic (VERIFIED: codebase read)
- `tests/test_libfilter_desktop.py` — existing desktop filter tests (VERIFIED: codebase read)
- `tests/test_catalog_availability_filter.py` — SEED-023 test structure (VERIFIED: codebase read)

### Secondary (MEDIUM confidence)
- `.planning/REQUIREMENTS.md` — DMF-07..13 requirement text (VERIFIED: read)
- `.planning/ROADMAP.md` — Phase 131 success criteria (VERIFIED: read)

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all symbols verified in codebase
- Architecture: HIGH — code flow traced from actual source; one assumption (A3) about post-fetch vs pre-query for parallels Hide mode
- Pitfalls: HIGH — derived from Phase 129/130 findings documented in SUMMARY.md files
- DMF-12 count source: MEDIUM — A2 is an assumption; planner must decide between result-derived and new DB query

**Research date:** 2026-06-30
**Valid until:** 2026-07-30 (stable codebase; fast-moving only around phase execution)
