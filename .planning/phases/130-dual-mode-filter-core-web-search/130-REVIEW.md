---
phase: 130-dual-mode-filter-core-web-search
reviewed: 2026-06-30T07:26:36Z
depth: deep
files_reviewed: 6
files_reviewed_list:
  - web/pages/search_state.py
  - web/pages/search.py
  - web/components/filter_panel.py
  - genizah_translations.py
  - tests/test_dual_mode_library_filter.py
  - tests/test_libfilter_web_search.py
findings:
  critical: 1
  warning: 5
  info: 3
  total: 9
status: issues_found
---

# Phase 130: Dual-Mode Library Filter — Code Review

**Reviewed:** 2026-06-30T07:26:36Z
**Depth:** deep
**Files Reviewed:** 6
**Status:** issues_found

## Summary

Phase 130 adds a dual-mode (Show-only / Hide) library filter to the web `/search` page. The
architecture is sound: the dict persist shape, legacy migration, sanitization against `LIBRARY_CODES`,
LOCAL exclusion, safe_storage chokepoint, and the `_apply_library_filter` mode branch are all
correctly implemented and well-tested. One blocker was found: the mode toggle event handler reads
`e.args` (a raw integer index) instead of the mapped string key, which means Show-only mode is
structurally unreachable at runtime despite appearing wired up. Five additional warnings cover
a normalization gap with the expand section, a wrong JS fallback default, a filter_panel empty-
sanitization edge case, a non-idiomatic JS string injection, and a duplicated `shown` computation.

---

## Critical Issues

### CR-01: Mode toggle event reads integer index — Show-only mode is completely non-functional

**File:** `web/pages/search.py:1871`
**Issue:** `mode_toggle.on('update:modelValue', lambda e: _on_mode_change(e.args))` passes
`e.args` to `_on_mode_change`. For NiceGUI's `ui.toggle` (backed by `q-btn-toggle`), the
internal `_props['options']` is built as `[{'value': 0, ...}, {'value': 1, ...}]` — integer
indices (see `choice_element.py:30`). Quasar emits `update:modelValue` with this integer
index, so `e.args` is `0` or `1`, not `'show_only'` or `'hide'`.

Downstream consequences:

1. `current_mode[0] = 0` or `1` — never the string key.
2. `committed_mode = 0` — `if committed_mode == 'show_only':` is always `False`, so Apply
   always takes the Hide branch regardless of the toggle position.
3. `ui.run_javascript(f'libFilterSetMode("{container_id}", "0")')` sets `data-libmode="0"`.
   The JS `libFilterUpdateApply` default: `var mode = cont.getAttribute('data-libmode') || 'show_only'`
   — `"0"` is truthy, so `mode = "0"`, which is never `=== 'show_only'` → Apply button is never
   disabled even at zero checked in "Show-only" position.
4. The Python Show-only guard on line 1912 (`if committed_mode == 'show_only':`) never fires.

Show-only is effectively dead code.

The `on_change` high-level API (used by every other toggle in the codebase, e.g., line 3276)
passes the properly-mapped value via `ValueChangeEventArguments.value`. The raw `.on()` API
passes the unconverted Vue arg.

**Fix:**
```python
# Replace:
mode_toggle.on('update:modelValue', lambda e: _on_mode_change(e.args))

# With either of:
mode_toggle.on_value_change(lambda e: _on_mode_change(e.value))
# OR (constructor form, consistent with line 3276 pattern):
mode_toggle = ui.toggle(
    options=mode_options,
    value=current_mode[0],
    on_change=lambda e: _on_mode_change(e.value),
).props('dense no-caps')
```

The `e.value` in `ValueChangeEventArguments` is the properly-mapped string key
(`'show_only'` or `'hide'`), via `toggle._event_args_to_value(e)`.

---

## Warnings

### WR-01: "Select All" in Show-only mode does not normalize to neutral when expand section is present

**File:** `web/pages/search.py:1888,1921`
**Issue:** The show-all normalization logic at line 1921 calls
`_library_apply_selection(checked, _all_shortlist)` where `_all_shortlist = shortlist_codes`
(only codes present in the current result set). However, "Select All" (`libFilterSelectAll`)
selects ALL `.lib-cb` checkboxes — including codes in the expand section. When the user
clicks Select All and applies in Show-only mode, `set(checked) != set(_all_shortlist)` because
`checked` contains extra expand-section codes. `_library_apply_selection` returns `list(checked)`
rather than `[]`, so the normalization to neutral `hide/[]` is skipped.

The persisted state becomes `{'mode': 'show_only', 'codes': [all 100+ codes]}`. Filtering
is functionally equivalent to show-all, BUT: on a subsequent search that returns a library code
not in the stored list (a future addition to `LIBRARY_CODES`), that library would be silently
hidden because the filter believes it is an active Show-only restriction, not a neutral state.

**Fix:** Pass the combined list (shortlist + expand) to the normalization check:
```python
# Replace at line 1888:
_all_shortlist = shortlist_codes  # for all-checked -> [] mapping

# With:
_all_for_norm = shortlist_codes + expand_codes  # full dialog list for normalization

# Replace at line 1921:
new_filter = _library_apply_selection(checked, _all_shortlist)
# With:
new_filter = _library_apply_selection(checked, _all_for_norm)
```

### WR-02: JS `libFilterUpdateApply` fallback defaults to wrong mode

**File:** `web/pages/search.py:348`
**Issue:**
```javascript
var mode = cont.getAttribute('data-libmode') || 'show_only';
```
The fallback when `data-libmode` is absent or empty is `'show_only'`, but the system default
mode is `'hide'` (D-05). If `data-libmode` is absent for any reason, Apply is incorrectly
disabled at zero checked (instead of being enabled as it should be for a fresh Hide-mode dialog).

This is normally harmless because `init_mode` is always written, but it creates a wrong
default that could also mask CR-01's symptom: when the bug causes `data-libmode` to be set
to `"0"` or `"1"` (truthy), neither triggers the disable. If someone corrects CR-01 but
leaves the fallback as `'show_only'`, a missing-attribute edge case could incorrectly
restrict the Hide-mode dialog.

**Fix:**
```javascript
var mode = cont.getAttribute('data-libmode') || 'hide';
```

### WR-03: `filter_panel.consume_incoming_filters` can persist `show_only` with empty codes

**File:** `web/components/filter_panel.py:346-357`
**Issue:** The guard on line 346 is:
```python
if incoming.get('library_filter') and storage_prefix == 'search':
```
This passes when `incoming['library_filter']` is a non-empty list. After sanitization on
line 350, `_lib_codes` can become `[]` if every incoming code is `LOCAL` or invalid. The
code then unconditionally sets `state.library_mode = 'show_only'` and persists
`{'mode': 'show_only', 'codes': []}`.

On the next reload, the restore path reads this as `mode='show_only'`, `codes=[]`.
`_apply_library_filter` correctly handles this (empty Show-only → show all per D-08), and
`_update_library_btn` shows neutral. Functionally benign, but the persisted state is
logically invalid and contradicts the principle that `show_only/[]` is not a valid persisted
state (the show-all normalization in Apply normalizes it to `hide/[]`).

**Fix:** Guard the `show_only` stamp on the sanitized result:
```python
_lib_codes = [str(c) for c in incoming['library_filter']
              if c and str(c) in _LIBRARY_CODES and str(c) != 'LOCAL']
if _lib_codes:
    state.library_filter = _lib_codes
    state.library_mode = 'show_only'
    persist_value('search_library_filter', {'mode': 'show_only', 'codes': _lib_codes})
# else: all codes sanitized away — leave state as-is (neutral)
```

### WR-04: `repr(e.value)` used for JavaScript string injection — non-idiomatic, brittle

**File:** `web/pages/search.py:1877`
**Issue:**
```python
on_change=lambda e: ui.run_javascript(
    f'libFilterSearch("{container_id}", {repr(e.value)})'
),
```
`repr(e.value)` produces a Python string literal (e.g., `'cambridge'`, `"it's"`,
`'it\'s "quoted"'`). While Python `\'` is also a valid JavaScript escape, `repr()` is
designed for Python debugging output, not JS string generation. The correct idiom for
emitting a JS-safe string literal is `json.dumps(e.value)`:

- `repr()` uses Python escape sequences that may not always be valid JS
  (e.g., `repr('\a')` → `'\\a'` which in JS is `\a` = `a`, silently wrong).
- `json.dumps()` always emits double-quoted strings using only standard JSON/JS escapes.
- This is the only `repr(e.value)` call in the entire `web/pages/` directory; every other
  case uses `e.value` directly (numeric sliders) or `json.dumps`.

**Fix:**
```python
import json as _json
on_change=lambda e: ui.run_javascript(
    f'libFilterSearch("{container_id}", {_json.dumps(e.value)})'
),
```

### WR-05: `shown` computed twice identically in `_update_library_btn`

**File:** `web/pages/search.py:1714,1723`
**Issue:** `shown = sum(1 for c in facets if c in codes)` is computed at line 1714 (to
evaluate `active`) and then recomputed identically at line 1723 (to format the button label).
The first value is immediately discarded when `show_only_active` is re-checked. This is dead
code duplication; in a large result set with many facet codes, it iterates the facets twice.

```python
# Lines 1712-1716 — shown computed, then discarded:
if mode == 'show_only' and codes:
    shown = sum(1 for c in facets if c in codes)   # ← first computation
    active = bool(total) and shown != total
    show_only_active = active

# Lines 1722-1724 — shown recomputed identically:
if show_only_active:
    shown = sum(1 for c in facets if c in codes)   # ← duplicate
    library_filter_btn.text = f"{tr('Showing')} {shown}/{total}"
```

**Fix:** Retain `shown` from the first computation:
```python
if mode == 'show_only' and codes:
    shown = sum(1 for c in facets if c in codes)
    active = bool(total) and shown != total
    show_only_active = active
else:
    shown = 0
    show_only_active = False

hide_active = (mode == 'hide' and bool(codes))

if show_only_active:
    library_filter_btn.text = f"{tr('Showing')} {shown}/{total}"  # reuses computed shown
    ...
```

---

## Info

### IN-01: Duplicate `"Showing"` key in `genizah_translations.py`

**File:** `genizah_translations.py:1704,2151`
**Issue:** The key `"Showing"` appears twice with the same Hebrew value `"מציג"`. Python dict
literals accept duplicate keys (last value wins), so this is harmless but wasteful and could
confuse tooling that validates translation completeness.

**Fix:** Remove the earlier occurrence at line 1704 (or the later at line 2151). Both have
the same HE value so the result is identical.

### IN-02: `_html.escape(label_text)` without `quote=True` in `data-label` HTML attribute

**File:** `web/pages/search.py:1794,1797`
**Issue:**
```python
label_esc = _html.escape(label_text)  # quote=False (default)
# ...
f'<label ... data-label="{label_esc.lower()}" ...'
```
`html.escape` without `quote=True` does not escape `"`. Since `data-label` is enclosed in
double quotes in the generated HTML, a library display name containing `"` would break the
attribute. No current library names contain `"`, so this is latent. The `code_attr` line on
1793 correctly uses `quote=True`.

**Fix:**
```python
label_esc = _html.escape(label_text, quote=True)
```

### IN-03: Text-search `libFilterSearch` does not auto-expand `<details>` on match

**File:** `web/pages/search.py:375-383`
**Issue:** `libFilterSearch` sets `row.style.display` on `.lib-cb-row` elements inside the
`<details>` expand section. When a text-search query matches only a row inside the collapsed
`<details>`, the row is technically visible (`display=''`) but the user cannot see it because
the `<details>` element is still closed. The expand-section libraries effectively become
unsearchable via the text-search box.

**Fix:** Auto-open `<details>` when any child row matches; close it when no child rows match:
```javascript
function libFilterSearch(cid, query) {
    var cont = document.getElementById(cid);
    if (!cont) return;
    var q = query.toLowerCase().trim();
    cont.querySelectorAll('.lib-cb-row').forEach(function(row) {
        if (!q) { row.style.display = ''; return; }
        var label = (row.getAttribute('data-label') || '').toLowerCase();
        row.style.display = (label.indexOf(q) >= 0) ? '' : 'none';
    });
    // Auto-expand <details> if any child row matches.
    cont.querySelectorAll('details').forEach(function(det) {
        if (!q) { det.removeAttribute('open'); return; }
        var hasVisible = false;
        det.querySelectorAll('.lib-cb-row').forEach(function(r) {
            if (r.style.display !== 'none') hasVisible = true;
        });
        if (hasVisible) det.setAttribute('open', ''); else det.removeAttribute('open');
    });
}
```

---

_Reviewed: 2026-06-30T07:26:36Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: deep_
