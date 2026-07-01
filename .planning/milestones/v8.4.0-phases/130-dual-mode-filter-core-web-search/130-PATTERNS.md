# Phase 130: Dual-Mode Filter Core — Web `/search` - Pattern Map

**Mapped:** 2026-06-30
**Files analyzed:** 5 files to be modified (no new files)
**Analogs found:** 5 / 5 (all in-place modifications with clear internal analogs)

## File Classification

| Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---------------|------|-----------|----------------|---------------|
| `web/pages/search.py` — `_open_library_filter_dialog` | component | request-response | `web/pages/search.py::_open_domain_filter_dialog` | exact (same file, structural mirror) |
| `web/pages/search.py` — `_update_library_btn` | utility | request-response | `web/pages/search.py::_update_printed_filter_btn` / `_update_pgp_filter_btn` | exact |
| `web/pages/search.py` — `apply_library_filter` + `_library_apply_selection` | utility | request-response | `web/pages/catalog_browse.py::apply_catalog_library_filter` | exact |
| `web/pages/search.py` — `_apply_library_filter` | utility | transform | `web/pages/search.py::_apply_pgp_filter` | role-match |
| `web/pages/search.py` — restore/sanitize path (line 188-190) + `clear_search_snapshot` in `search_state.py` | utility | request-response | `web/pages/search.py` printed/pgp restore + `search_state.py::clear_search_snapshot` | exact |
| `web/pages/search_state.py` — `SearchUIState.library_filter` + `clear_search_snapshot` | model | CRUD | existing `printed_filter`/`pgp_filter` fields | exact |

## Pattern Assignments

---

### `_open_library_filter_dialog` — Redesign to dual-mode (search.py ~line 1681)

**Analog:** `_open_domain_filter_dialog` in `web/pages/search.py` (lines 3326–3566) — the structural mirror the v8.3.0 library dialog already followed. Read it in full for exact dialog scaffold.

**Dialog scaffold pattern** (lines 3520–3566):
```python
with ui.dialog() as dialog, ui.card().classes('w-[600px] max-h-[80vh]'):
    with ui.column().classes('w-full gap-2'):
        ui.label(tr('Filter by Domain')).classes('text-lg font-bold')
        ui.label(f"{tr('Showing')} {total_results} ...").classes('text-sm text-gray-500')

        with ui.scroll_area().classes('w-full').style('max-height: 50vh;'):
            ui.html(f'<div id="{container_id}">{checkbox_html}</div>', sanitize=False)

        with ui.row().classes('w-full justify-between'):
            _cid = container_id  # capture for closures

            with ui.row().classes('gap-2'):
                ui.button(tr('Select All'), on_click=...).props('flat dense no-caps')
                ui.button(tr('Select None'), on_click=...).props('flat dense no-caps')

            with ui.row().classes('gap-2'):
                async def apply_filter():
                    excluded_list = await ui.run_javascript(
                        f'domainFilterGetExcluded("{_cid}")', timeout=5.0
                    )
                    # ... update state, persist, re-render, dialog.close()

                ui.button(tr('Apply'), on_click=apply_filter).props('dense no-caps color=primary')
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat dense no-caps')

dialog.open()
```

**The NEW element** — mode segmented control: place at top of dialog (above the scroll area), before checkboxes. NiceGUI analog for segmented/toggle: `ui.toggle` or a `ui.radio` styled inline. Mode flip must reset checked set (D-04).

**Full-universe list pattern** from `catalog_browse.py::apply_catalog_library_filter` (lines 1024–1029):
```python
# Build full canonical list (minus LOCAL) — used for the expand-all-A–Z section
all_codes = [c for c in LIBRARY_CODES if c != 'LOCAL']
all_codes_sorted = sorted(
    all_codes,
    key=lambda c: get_library_display(c, short=False, lang=_lang),
)
```

**Shortlist (result-derived facets) pattern** from current `_open_library_filter_dialog` (lines 1700–1711):
```python
facets = _compute_library_facets(search_state.results)
lang = get_language()
# Shortlist = sorted by count descending
all_codes = sorted(
    facets.keys(),
    key=lambda c: get_library_display(c, short=False, lang=lang)
)
```
For Phase 130, shortlist = `sorted(facets.keys(), key=lambda c: -facets[c])` (count desc). Expand section = `[c for c in LIBRARY_CODES if c != 'LOCAL' and c not in facets]` sorted A–Z.

**Checkbox HTML pattern** (lines 1720–1739, current form):
```python
import html as _html
import uuid as _uuid
container_id = f'lib-filter-{_uuid.uuid4().hex[:8]}'
current_filter = set(search_state.library_filter)

for code in all_codes:
    count = facets[code]
    label = get_library_display(code, short=False, lang=lang)
    is_checked = (not current_filter) or (code in current_filter)
    checked_attr = 'checked' if is_checked else ''
    code_attr = _html.escape(code, quote=True)
    label_html = _html.escape(f"{label} ({count})")
    checkbox_html_parts.append(
        f'<label style="display:flex;align-items:center;gap:8px;'
        f'padding:5px 0;cursor:pointer;font-size:0.9rem">'
        f'<input type="checkbox" class="lib-cb" data-code="{code_attr}" '
        f'{checked_attr} '
        f'style="width:16px;height:16px;accent-color:#1976d2;cursor:pointer" '
        f'onchange="libFilterUpdateApply(\'{container_id}\')">'
        f'<span>{label_html}</span></label>'
    )
```
For the expand section: same structure but no count; use an HTML `<details>`/`<summary>` or `ui.expansion` wrapper outside the scroll area.

**Text-search filter:** add a `ui.input` above the scroll area that runs JS to show/hide checkboxes matching the typed string (client-side filter — no Python round-trip needed).

**Apply handler pattern** (lines 1783–1810):
```python
async def apply_library_filter():
    checked_list = await ui.run_javascript(
        f'libFilterGetChecked("{_cid}")', timeout=5.0
    )
    checked = list(checked_list) if checked_list else []
    if not checked:  # Python-side guard (FINDING 1)
        ui.notify(tr('Select at least one library, or check all to clear the filter'), type='warning')
        return
    new_filter = _library_apply_selection(checked, _all)
    search_state.library_filter = new_filter
    persist_value('search_library_filter', search_state.library_filter)
    _update_library_btn()
    if search_state.exclusion_sources:
        _apply_manuscript_exclusions()
    elif search_state.domain_exclusions and search_state.has_domain_data:
        _apply_domain_exclusions()
    elif search_state.results:
        _apply_printed_filter_and_render(search_state.results)
    dialog.close()
```
For Phase 130: also read back the mode from the segmented control before persisting; store as `{'mode': mode, 'codes': new_filter}` dict (or two separate keys — planner's call per D-09).

---

### `_library_apply_selection` — existing pure helper (search.py lines 1618–1631)

**Current implementation** (lines 1618–1631):
```python
def _library_apply_selection(checked_codes, all_codes):
    """Returns [] when all codes are checked (= show all sentinel), else the inclusion list."""
    if set(checked_codes) == set(all_codes):
        return []
    return list(checked_codes)
```
This helper maps the "all-checked = clear filter" convention. In the dual-mode world, this `[]`-means-all-for-Show-only contract is reused ONLY for Show-only mode. In Hide mode, an empty set is a valid "hide nothing" state (D-08), so the Python-side guard (`if not checked: notify; return`) no longer applies for Hide mode — an empty Hide set is allowed and is the DEFAULT (D-05).

---

### `_update_library_btn` — extend to 3-state (search.py lines 1648–1679)

**Current implementation** (lines 1648–1679):
```python
def _update_library_btn():
    facets = _compute_library_facets(search_state.results) if search_state.results else {}
    total = len(facets)
    shown = total
    active = False
    if search_state.library_filter:
        sel = set(search_state.library_filter)
        shown = sum(1 for code in facets if code in sel)
        active = bool(total) and shown != total
    if not active:
        library_filter_btn.text = tr('Filter by library')
        library_filter_btn.props(remove='color')
        library_filter_btn.props('outline dense no-caps color=primary')
    else:
        library_filter_btn.text = f"{tr('Filter by library')} ({shown}/{total})"
        library_filter_btn.props(remove='color outline')
        library_filter_btn.props('dense no-caps color=negative')
```
**Pattern to mirror for new 3-state logic** — `_update_printed_filter_btn` (lines 1533–1546):
```python
def _update_printed_filter_btn():
    if search_state.printed_filter == 'all':
        printed_filter_btn.text = tr('Filter Printed')
        printed_filter_btn.props('outline dense no-caps color=primary')
    elif search_state.printed_filter == 'hide_printed':
        printed_filter_btn.text = tr('Hiding printed')
        printed_filter_btn.props(remove='color')
        printed_filter_btn.props('outline dense no-caps color=red')
    elif search_state.printed_filter == 'only_printed':
        printed_filter_btn.text = tr('Only printed')
        printed_filter_btn.props(remove='color')
        printed_filter_btn.props('outline dense no-caps color=deep-orange')
```
**New 3-state mapping** (D-07):
- Neutral: `tr('Filter by library')` — outline primary (current neutral behavior preserved)
- Show-only active: `f"{tr('Showing')} {N}/{total}"` — filled negative/red
- Hide active: `f"{tr('Hiding')} {N}"` — filled red (or different color to distinguish from Show-only)

"Active" definition per mode:
- Show-only: active when `library_filter` (the code set) is non-empty AND it doesn't include all in-result libraries
- Hide: active when `library_mode == 'hide'` AND `library_filter` (the hide set) is non-empty

---

### `_apply_library_filter` — add mode branch (search.py line 3677)

**Current implementation** (lines 3677–3687):
```python
def _apply_library_filter(results_list):
    """SEED-026 (LIBFILTER-01): filter results by selected library codes.

    Returns results_list unchanged when search_state.library_filter is empty.
    Otherwise returns only results whose r['display']['library_code'] is in the
    selected set. Iterates the FULL results list — never sliced first.
    """
    if not search_state.library_filter:
        return results_list
    selected = set(search_state.library_filter)
    return [r for r in results_list if r.get('display', {}).get('library_code', '') in selected]
```
**Pattern to mirror for mode branch** — `_apply_pgp_filter` (lines 3660–3675):
```python
def _apply_pgp_filter(results_list):
    if search_state.pgp_filter == 'all':
        return results_list
    filtered = []
    for r in results_list:
        has_pgp = bool(r.get('display', {}).get('has_pgp'))
        if search_state.pgp_filter == 'only_pgp' and not has_pgp:
            continue
        elif search_state.pgp_filter == 'hide_pgp' and has_pgp:
            continue
        filtered.append(r)
    return filtered
```
**New dual-mode branch** (D-01, Show-only = ∈, Hide = ∉):
```python
def _apply_library_filter(results_list):
    mode = getattr(search_state, 'library_mode', 'hide')   # 'show_only' | 'hide'
    codes = set(search_state.library_filter)

    if mode == 'show_only':
        if not codes:
            return results_list  # empty Show-only = show all (D-08)
        return [r for r in results_list
                if r.get('display', {}).get('library_code', '') in codes]
    else:  # hide
        if not codes:
            return results_list  # empty Hide = show all (D-05 default)
        return [r for r in results_list
                if r.get('display', {}).get('library_code', '') not in codes]
```
Note: `library_mode` must be a new `SearchUIState` attribute (see below).

---

### Restore/sanitize path — make mode-aware (search.py lines 186–190)

**Current implementation** (lines 186–190):
```python
# SEED-026 (LIBFILTER-01): library multi-select filter
# D-46/D-NEW-7: exclude 'LOCAL'
_lib0 = _safe_get('search_library_filter', [])
_lib0 = _lib0 if isinstance(_lib0, list) else []
search_state.library_filter = [c for c in _lib0 if c in LIBRARY_CODES and c != 'LOCAL']
```
**Pattern to mirror** — printed/pgp restore (lines 183–185):
```python
search_state.domain_exclusions = set(_de) if _de is not None else set()
search_state.printed_filter = _safe_get('search_printed_filter', 'all')
search_state.pgp_filter = _safe_get('search_pgp_filter', 'all')
```
**New mode-aware restore with D-06 legacy migration:**
```python
_lib_raw = _safe_get('search_library_filter', None)

# D-06 legacy migration: plain list → Show-only
if isinstance(_lib_raw, list):
    # v8.3.0 persisted value: plain list of codes → migrate to Show-only
    _lib_codes = [c for c in _lib_raw if c in LIBRARY_CODES and c != 'LOCAL']
    if _lib_codes:
        search_state.library_mode = 'show_only'
        search_state.library_filter = _lib_codes
    else:
        search_state.library_mode = 'hide'
        search_state.library_filter = []
elif isinstance(_lib_raw, dict):
    # New shape: {'mode': 'show_only'|'hide', 'codes': [...]}
    _mode = _lib_raw.get('mode', 'hide')
    _codes = [c for c in (_lib_raw.get('codes') or []) if c in LIBRARY_CODES and c != 'LOCAL']
    search_state.library_mode = _mode if _mode in ('show_only', 'hide') else 'hide'
    search_state.library_filter = _codes
else:
    # Fresh/absent: default Hide mode, empty set (D-05)
    search_state.library_mode = 'hide'
    search_state.library_filter = []
```
**Persist shape** (D-09 — both mode and codes, single key):
```python
persist_value('search_library_filter', {
    'mode': search_state.library_mode,
    'codes': search_state.library_filter,
})
```

---

### `clear_search_snapshot` — reset mode too (search_state.py lines 438–473)

**Current reset default** (line 462):
```python
'search_library_filter': [],  # SEED-026 (LIBFILTER-01)
```
**New reset default** (D-05 — fresh = Hide/empty):
```python
'search_library_filter': {'mode': 'hide', 'codes': []},
```

**In-memory reset** (search.py lines 2528–2532):
```python
# SEED-026 (LIBFILTER-01): hide library button + reset in-memory state on New Search.
_set_btn_visible(library_filter_btn, False)
search_state.library_filter = []
_update_library_btn()
```
Must also reset `search_state.library_mode = 'hide'` here.

---

### `SearchUIState` — new `library_mode` field (search_state.py line 61)

**Current declaration** (line 61):
```python
self.library_filter: list = []  # SEED-026 (LIBFILTER-01): selected library codes; empty = all
```
**New fields:**
```python
self.library_filter: list = []  # selected library codes (set for active filter)
self.library_mode: str = 'hide'  # 'show_only' | 'hide' (D-05 default: Hide)
```

---

## Shared Patterns

### Persistence chokepoint
**Source:** `web/components/filter_panel.py` lines 220–231
**Apply to:** every `persist_value('search_library_filter', ...)` call in `search.py`
```python
def persist_value(key, value):
    from web.safe_storage import safe_user_get, safe_user_set
    if safe_user_get('session_persistence_enabled', True):
        safe_user_set(key, value)
```
All reads use `_safe_get` (aliased from `safe_user_get`), all writes use `persist_value`. Never call `app.storage.user` directly.

### Dialog HTML scaffold
**Source:** `web/pages/search.py` lines 3520–3566 (`_open_domain_filter_dialog`)
**Apply to:** `_open_library_filter_dialog` redesign
Key invariants:
- Single `ui.html()` with all checkboxes (never individual `ui.checkbox` per item — causes 7–19s open times)
- JS functions defined once at page-level via `ui.add_head_html` (not inside the dialog)
- `container_id = f'lib-filter-{_uuid.uuid4().hex[:8]}'` — unique per open to avoid stale DOM conflicts
- `dialog.open()` at end; Apply calls `dialog.close()`

### LOCAL exclusion guard
**Source:** `tests/test_web_library_options_no_local.py` and `tests/test_phase_97_invariants.py`
**Apply to:** any function in `web/pages/*.py` that iterates `LIBRARY_CODES`
The AST guard checks that every such function contains a string comparison against `'LOCAL'`. The pattern must appear in every new function that iterates `LIBRARY_CODES`:
```python
all_codes = [c for c in LIBRARY_CODES if c != 'LOCAL']
```
The guard scans for the `'LOCAL'` constant as an AST `ast.Constant`. Both `c != 'LOCAL'` and `c in {'LOCAL', ...}` pass. A function that constructs the expand-section list MUST include this guard by construction.

### Filter cascade position
**Source:** `web/pages/search.py` lines 3689–3714 (`_apply_printed_filter_and_render`)
**Apply to:** `_apply_library_filter` position is unchanged — it stacks AFTER pgp filter, BEFORE measurement post-filters:
```python
filtered = _apply_printed_filter(results_list)
filtered = _apply_pgp_filter(filtered)
filtered = _apply_library_filter(filtered)   # mode branch added here
filtered = _apply_measurement_post_filters(filtered, search_state)
```

### Button visibility pattern
**Source:** `web/pages/search.py` line 1501–1502
**Apply to:** library filter button on New Search reset
```python
def _set_btn_visible(btn, visible):
    btn.style(f'visibility: {"visible" if visible else "hidden"};')
```
Use `_set_btn_visible`, NOT `set_visibility()` (which uses `display:none` and conflicts with the results-arrive reveal path).

---

## Persistence Shape Summary (D-09)

**Current stored shape** (v8.3.0): `search_library_filter` = a plain `list[str]` of library codes (or `[]` for no filter).

**New stored shape** (Phase 130): `search_library_filter` = `{'mode': 'show_only'|'hide', 'codes': list[str]}`.

**Migration rule** (D-06, line 188-190 path): on restore, if `_safe_get('search_library_filter', [])` returns a `list`, treat it as a legacy Show-only value — non-empty → `mode='show_only', codes=that-list`; empty → `mode='hide', codes=[]` (i.e., already neutral). If it returns a `dict`, read `mode` and `codes` directly. If it returns `None` or any other type → fresh Hide/empty default (D-05).

**Reset value** in `clear_search_snapshot`: `{'mode': 'hide', 'codes': []}` (not `[]`).

---

## No Analog Found

None. All Phase 130 modifications have close in-codebase analogs.

---

## Metadata

**Analog search scope:** `web/pages/search.py`, `web/pages/catalog_browse.py`, `web/pages/search_state.py`, `web/components/filter_panel.py`, `web/safe_storage.py`, `shared/browse_map_utils.py`, `tests/test_web_library_options_no_local.py`, `tests/test_phase_97_invariants.py`
**Files scanned:** 8
**Pattern extraction date:** 2026-06-30
