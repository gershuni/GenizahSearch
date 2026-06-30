# Phase 131: Dual-Mode Parity — Desktop Catalog + Web Browse-by-Identification + Web `/parallels` - Pattern Map

**Mapped:** 2026-06-30
**Files analyzed:** 8 modified files (no new production files; 3 new test files)
**Analogs found:** 8 / 8 (all in-place modifications with direct analogs)

---

## File Classification

| Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---------------|------|-----------|----------------|---------------|
| `desktop/dialogs_filter.py` — `LibraryFilterDialog` | component | request-response | `desktop/dialogs_filter.py::PreSearchFilterDialog` (QButtonGroup toggle, lines 841–855) | exact |
| `genizah_app.py` — `_catalog_library_mode` init field | model | CRUD | `genizah_app.py` line 9598–9601 (`_catalog_pgp_filter`/`_catalog_editions_filter`/`_catalog_library_filter` init) | exact |
| `genizah_app.py` — `_open_catalog_library_dialog` | controller | request-response | `genizah_app.py` lines 10430–10441 (current function — modify in-place) | exact (self-analog) |
| `genizah_app.py` — `_catalog_update_library_filter_btn` | utility | request-response | `genizah_app.py` lines 10443–10470 (current function — extend to 3-state) | exact (self-analog) |
| `genizah_app.py` — `_CatalogRefreshWorker.__init__` + `run` | service | CRUD | `genizah_app.py` lines 488–566 (current worker — add `library_mode` param) | exact (self-analog) |
| `web/pages/catalog_browse.py` — restore block, `current_library_mode`, `_update_library_filter_btn`, `_open_library_filter_dialog`, `apply_catalog_library_filter`, clear sites | component | request-response | `web/pages/search.py` lines 186–216 (restore), 1700–1986 (dialog + btn) | exact |
| `web/pages/parallels.py` — `ParallelsState` fields, restore block, new filter button + dialog, post-fetch filter | component | request-response | `web/pages/search.py` restore (186–216) + `_apply_library_filter` (3830–3853) + `_open_library_filter_dialog` (1752–1984) | exact |
| `tests/test_libfilter_desktop.py` — extend | test | request-response | `tests/test_libfilter_desktop.py` lines 1–23 (existing file structure) | exact (self-analog) |

---

## Pattern Assignments

---

### `desktop/dialogs_filter.py` — `LibraryFilterDialog` (add mode toggle)

**Analog:** `desktop/dialogs_filter.py::PreSearchFilterDialog.__init__` — QButtonGroup + two QRadioButton pattern (lines 841–855).

**Current `LibraryFilterDialog.__init__` signature** (line 1693):
```python
def __init__(self, parent=None, *, selected_codes: list | None = None):
```
**New signature (add `mode` param):**
```python
def __init__(self, parent=None, *, mode: str = 'hide', selected_codes: list | None = None):
```

**Mode toggle pattern** (copy from `desktop/dialogs_filter.py:841–855`, adapt labels):
```python
# Insert after setWindowTitle, before self._all_codes = ...
mode_layout = QHBoxLayout()
self._mode_group = QButtonGroup(self)
self._rb_show_only = QRadioButton(tr("Show only selected"))
self._rb_hide = QRadioButton(tr("Hide selected"))
self._mode_group.addButton(self._rb_show_only, 1)
self._mode_group.addButton(self._rb_hide, 2)
if mode == 'show_only':
    self._rb_show_only.setChecked(True)
else:
    self._rb_hide.setChecked(True)
self._mode_group.buttonToggled.connect(self._on_mode_changed)
mode_layout.addWidget(self._rb_show_only)
mode_layout.addWidget(self._rb_hide)
mode_layout.addStretch()
layout.addLayout(mode_layout)
```

**D-04 reset on mode flip** — add new method:
```python
def _on_mode_changed(self):
    """D-04: mode flip resets the checked set (prevents silent inversion of intent)."""
    self.list_widget.blockSignals(True)
    for i in range(self.list_widget.count()):
        self.list_widget.item(i).setCheckState(Qt.CheckState.Unchecked)
    self.list_widget.blockSignals(False)
    self._update_ok_button()
```

**Mode-aware OK guard** — replace `_update_ok_button` (current: lines 1762–1775, always requires >0 checked):
```python
def _update_ok_button(self):
    mode = self.get_mode()
    checked_count = sum(
        1 for i in range(self.list_widget.count())
        if self.list_widget.item(i).checkState() == Qt.CheckState.Checked
    )
    if mode == 'hide':
        self.ok_button.setEnabled(True)       # empty hide-set = show all (D-05/D-08)
        self._hint_label.setVisible(False)
    else:  # show_only
        self.ok_button.setEnabled(checked_count > 0)
        self._hint_label.setVisible(checked_count == 0)
```

**New `get_mode()` method:**
```python
def get_mode(self) -> str:
    return 'show_only' if self._rb_show_only.isChecked() else 'hide'
```

**`_all_codes` source change** (DMF-13 — swap `LIBRARY_CODES.keys()` for `library_codes_with_manuscripts()`):
```python
# Current (line 1698):
self._all_codes = [c for c in LIBRARY_CODES.keys() if c != 'LOCAL']
# New:
from shared.browse_map_utils import library_codes_with_manuscripts
self._all_codes = [c for c in library_codes_with_manuscripts() if c != 'LOCAL']
```

**Initial checkbox state** — add mode-awareness to `all_checked` logic (current: lines 1715–1725):
```python
# Current: all_checked = len(active_set) == 0  (always start all-checked)
# New: depends on mode
if mode == 'show_only':
    all_checked = len(active_set) == 0   # empty selected = show all (show-only semantics)
else:  # hide
    all_checked = False                   # hide mode: start empty (checked = in hide-set)
```

**`_on_accept` guard** — update for mode-awareness (current: lines 1799–1808):
```python
def _on_accept(self):
    mode = self.get_mode()
    checked = self.get_checked_codes()
    if mode == 'show_only' and not checked:
        self._hint_label.setText(
            tr("Select at least one library, or check all to clear the filter")
        )
        self._hint_label.setVisible(True)
        return
    # Hide mode: empty checked is valid (= hide nothing = show all)
    self.accept()
```

---

### `genizah_app.py` — `_catalog_library_mode` init field (new)

**Analog:** `genizah_app.py:9598–9601` — existing `_catalog_pgp_filter` / `_catalog_editions_filter` / `_catalog_library_filter` init pattern.

**Current init block** (lines 9597–9601):
```python
self._catalog_pgp_filter = 'all'        # 'all' | 'has_pgp' | 'no_pgp'
self._catalog_editions_filter = 'all'   # 'all' | 'has_edition' | 'no_edition'
# SEED-026 desktop parity — library filter (LIBFILTER-03).
self._catalog_library_filter = []       # [] = all; list of library codes when active
```
**Add immediately after line 9601:**
```python
self._catalog_library_mode = 'hide'     # 'show_only' | 'hide' (DMF D-05 default)
```
Note: in-memory only — no QSettings, no session JSON (matches `_catalog_pgp_filter` / `_catalog_editions_filter` — neither is persisted; see Pitfall 1).

---

### `genizah_app.py` — `_open_catalog_library_dialog` (modify in-place, lines 10430–10441)

**Analog:** `genizah_app.py:10430–10441` (current form — direct self-analog).

**Current implementation:**
```python
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
        self._catalog_update_chips()
```
**New form (dual-mode, DMF-07):**
```python
def _open_catalog_library_dialog(self):
    from shared.browse_map_utils import library_codes_with_manuscripts
    all_codes = [c for c in library_codes_with_manuscripts() if c != 'LOCAL']
    dlg = LibraryFilterDialog(
        self,
        mode=self._catalog_library_mode,
        selected_codes=list(self._catalog_library_filter),
    )
    if dlg.exec() == QDialog.DialogCode.Accepted:
        mode = dlg.get_mode()
        checked = dlg.get_checked_codes()
        if mode == 'show_only':
            new_filter = library_apply_selection(checked, all_codes)  # all-checked → []
        else:  # hide
            new_filter = list(checked)   # empty hide-set is valid (D-05/D-08)
        self._catalog_library_mode = mode
        self._catalog_library_filter = new_filter
        self._catalog_update_library_filter_btn()
        self._catalog_current_page = 0
        self._catalog_start_async_refresh(refresh_authors=False, refresh_works=False)
        self._catalog_update_chips()
```

---

### `genizah_app.py` — `_catalog_update_library_filter_btn` (extend to 3-state, lines 10443–10470)

**Analog:** `genizah_app.py:10443–10470` (current 2-state function — extend to 3-state).
Mirror the 3-state web button logic from `web/pages/search.py:1700–1750` (`_update_library_btn`).

**Current implementation** (lines 10443–10470 — 2-state: neutral or red show-count):
```python
def _catalog_update_library_filter_btn(self):
    if not hasattr(self, '_catalog_library_filter_btn'):
        return
    btn = self._catalog_library_filter_btn
    if self._catalog_library_filter:
        total = len([c for c in LIBRARY_CODES.keys() if c != 'LOCAL'])
        shown = len(self._catalog_library_filter)
        btn.setText(tr("Filter by library") + f" ({shown}/{total})")
        btn.setStyleSheet(
            "QPushButton { text-align: left; padding: 2px 8px; font-size: 11px; "
            "background-color: #d32f2f; color: white; border: none; border-radius: 3px; }"
            "QPushButton:hover { background-color: #b71c1c; }"
        )
    else:
        btn.setText(tr("Filter by library"))
        btn.setStyleSheet(
            "QPushButton { text-align: left; padding: 2px 8px; font-size: 11px; }"
        )
```
**New 3-state form (D-07: Neutral / Show-only / Hide):**
```python
def _catalog_update_library_filter_btn(self):
    if not hasattr(self, '_catalog_library_filter_btn'):
        return
    btn = self._catalog_library_filter_btn
    mode = getattr(self, '_catalog_library_mode', 'hide')
    flt = self._catalog_library_filter
    # Codex F2/N4: total comes from the SAME universe the dialog offers
    # (library_codes_with_manuscripts), NOT LIBRARY_CODES.keys() — otherwise the
    # count can exceed the selectable libraries.
    total = len([c for c in library_codes_with_manuscripts() if c != 'LOCAL'])
    if not flt:
        # Neutral: no active restriction
        btn.setText(tr("Filter by library"))
        btn.setStyleSheet("QPushButton { text-align: left; padding: 2px 8px; font-size: 11px; }")
    elif mode == 'show_only':
        shown = len(flt)
        # Codex F2: REAL Phase-130 pluralized keys (genizah_translations.py:2918-2921), not an invented label
        btn.setText(tr('Showing {shown}/{total} library' if total == 1
                       else 'Showing {shown}/{total} libraries').format(shown=shown, total=total))
        btn.setStyleSheet(
            "QPushButton { text-align: left; padding: 2px 8px; font-size: 11px; "
            "background-color: #d32f2f; color: white; border: none; border-radius: 3px; }"
            "QPushButton:hover { background-color: #b71c1c; }"
        )
    else:  # hide mode, non-empty set
        n = len(flt)
        btn.setText(tr('Hiding {n} library' if n == 1 else 'Hiding {n} libraries').format(n=n))
        btn.setStyleSheet(
            "QPushButton { text-align: left; padding: 2px 8px; font-size: 11px; "
            "background-color: #e65100; color: white; border: none; border-radius: 3px; }"
            "QPushButton:hover { background-color: #bf360c; }"
        )
```

---

### `genizah_app.py` — `_CatalogRefreshWorker` (add `library_mode`, lines 488–566)

**Analog:** `genizah_app.py:488–566` (current worker — add `library_mode` param, wire into `run`).

**Current `__init__` signature** (lines 497–502):
```python
def __init__(self, parent, domain, author, work, offset, limit,
             date_from=None, date_to=None, include_undated=False,
             text_all=None, text_any=None, text_not=None,
             refresh_authors=True, refresh_works=True,
             pgp_filter='all', editions_filter='all',
             library_filter=None, meta_mgr=None):
```
**Add `library_mode='hide'` parameter:**
```python
def __init__(self, ..., library_filter=None, library_mode='hide', meta_mgr=None):
    ...
    self._library_filter = library_filter or []
    self._library_mode = library_mode          # NEW
    self._meta_mgr = meta_mgr
```

**Call site** (`_catalog_start_async_refresh`, line 10163–10182) — add `library_mode`:
```python
self._catalog_refresh_worker = _CatalogRefreshWorker(
    self,
    ...
    library_filter=list(self._catalog_library_filter),
    library_mode=self._catalog_library_mode,    # NEW
    meta_mgr=self.meta_mgr,
)
```

**`run()` library filter path** (lines 548–564) — add mode-aware pass through:
The current `run()` resolves `library_sys_ids` from `self._library_filter` and passes both `library_codes` and `library_sys_ids` to `get_browse_results`. Phase 131 must also pass `library_mode` to `get_browse_results` so the service can apply Include vs Exclude semantics.

Check whether `shared/fjms_service.get_browse_results` already accepts a `library_mode` param:
```python
# Current run() (lines 548–564):
library_sys_ids = None
if self._library_filter:
    from shared.fjms_service import resolve_library_sys_ids
    library_sys_ids = resolve_library_sys_ids(self._library_filter, self._meta_mgr)
result['data'] = fjms.get_browse_results(
    ...
    library_codes=(self._library_filter or None),
    library_sys_ids=(library_sys_ids or None),
)
```
Add `library_mode` to the `get_browse_results` call if the service supports it; otherwise apply Hide-mode as a post-fetch filter over the returned results (same pattern as web `/search` uses `_apply_library_filter` post-fetch). The planner must verify `get_browse_results` signature in `shared/fjms_service.py` before deciding.

---

### `web/pages/catalog_browse.py` — restore block + `current_library_mode` + dialog redesign (6 sites)

**Lead analog:** `web/pages/search.py:186–216` (restore with D-06 migration), `1700–1986` (3-state button + dual-mode dialog). All patterns extracted verbatim from the shipped Phase 130 implementation.

#### Site 1: Restore block (lines 114–119 — replace plain-list restore)

**Current** (lines 114–119):
```python
_lib0 = safe_user_get('catalog_library_filter', [])
current_library_filter = {
    'value': [c for c in _lib0 if c in LIBRARY_CODES] if isinstance(_lib0, list) else []
}
```
**New (D-06 migration, mirrors `search.py:189–216`):**
```python
_lib_raw = safe_user_get('catalog_library_filter', None)
if isinstance(_lib_raw, list):
    # Legacy plain-list (Phase 129 shape) → migrate to Show-only
    _lib_codes = sanitize_library_codes(_lib_raw)
    if _lib_codes:
        _cat_library_mode = 'show_only'
        _cat_library_codes = _lib_codes
    else:
        _cat_library_mode = 'hide'
        _cat_library_codes = []
elif isinstance(_lib_raw, dict):
    _m = _lib_raw.get('mode', 'hide')
    _cat_library_mode = _m if _m in ('show_only', 'hide') else 'hide'
    _cat_library_codes = sanitize_library_codes(_lib_raw.get('codes'))
    # Normalize invalid show_only+empty to neutral (Codex HIGH fix, mirrors search.py:206–210)
    if _cat_library_mode == 'show_only' and not _cat_library_codes:
        _cat_library_mode = 'hide'
else:
    _cat_library_mode = 'hide'
    _cat_library_codes = []
current_library_mode = {'value': _cat_library_mode}     # NEW mutable cell
current_library_filter = {'value': _cat_library_codes}
```
Import `sanitize_library_codes` from `shared.browse_map_utils` (already imported in `search.py:43` — add to `catalog_browse.py` import block).

#### Site 2: `_update_library_filter_btn` (lines 962–988 — extend to 3-state)

**Current** (lines 962–988 — 2-state):
```python
def _update_library_filter_btn():
    btn = library_filter_btn_ref['ref']
    if not btn:
        return
    sel = current_library_filter['value']
    if not sel:
        btn.text = tr('Filter by library')
        btn.props(remove='color')
        btn.props('outline dense no-caps color=primary')
    else:
        total = len([c for c in LIBRARY_CODES if c != 'LOCAL'])
        shown = len(sel)
        btn.text = f"{tr('Filter by library')} ({shown}/{total})"
        btn.props(remove='color outline')
        btn.props('dense no-caps color=negative')
```
**New (3-state — mirrors `search.py:1700–1750`):**
```python
def _update_library_filter_btn():
    btn = library_filter_btn_ref['ref']
    if not btn:
        return
    sel = current_library_filter['value']
    mode = current_library_mode['value']
    if not sel:
        btn.text = tr('Filter by library')
        btn.props(remove='color')
        btn.props('outline dense no-caps color=primary')
    elif mode == 'show_only':
        total = len([c for c in LIBRARY_CODES if c != 'LOCAL'])
        shown = len(sel)
        btn.text = f"{tr('Filter by library')} ({shown}/{total})"
        btn.props(remove='color outline')
        btn.props('dense no-caps color=negative')
    else:  # hide, non-empty
        n = len(sel)
        btn.text = f"{tr('Hiding')} {n}"
        btn.props(remove='color outline')
        btn.props('dense no-caps color=deep-orange')
```

#### Site 3: `_open_library_filter_dialog` (lines 1006–1142 — full redesign)

**Analog:** `web/pages/search.py:1752–1986` (the Phase 130 shipped dialog — copy structure verbatim, adapting container_id prefix and callback names).

Key differences from current catalog dialog:
- Add `ui.toggle({'show_only': tr('Show only selected'), 'hide': tr('Hide selected')})` at top (D-03)
- Add `ui.input(placeholder=tr('Search libraries...'), ...)` client-side text filter
- Split checkboxes into shortlist (TRUE full-set facets via `fjms.get_browse_library_facets(...)` — see Pattern below; NO page-local/result-derived fallback, Codex R3 F1) + expand section
- JS functions: reuse `libFilterSearch` / `libFilterSetMode` / `libFilterGetChecked` / `libFilterUpdateApply` / `libFilterSelectAll` (already defined at page level in `search.py`; must be added to `catalog_browse.py` page-level `ui.add_head_html` if not already present)
- `container_id = f'cat-lib-filter-{_uuid.uuid4().hex[:8]}'` (unique per open)

**Per-library facet count source for catalog (DMF-12) — TRUE full-set facets, ALWAYS (Codex R3 F1/F2):**
Do NOT build facets from the current result rows. The catalog is ALWAYS paginated (PAGE_SIZE=50), so a current-page `Counter` misses off-page libraries even with no filters active. The shortlist counts ALWAYS come from Plan 02's `fjms.get_browse_library_facets(...)` (true full-set facets) — called as an INSTANCE METHOD on the page's `fjms` handle (`fjms = get_fjms_service(thread_safe=True)` at line 89, the SAME handle `fjms.get_browse_results(...)` uses — Codex N1), inside the io_bound path (it is a DB call). The `sys_id_to_library` argument is a CALLABLE: pass the bound full-corpus method `state.meta_mgr.get_library_for_id` directly (Codex R3 F3) — NOT a dict, NOT the page-local `_resolve_all`, NOT a current-page Counter.
```python
# Inside the io_bound fetch path (NOT on the event loop), refreshed alongside refresh_results:
try:
    facets = fjms.get_browse_library_facets(
        domain=..., author=..., work=...,            # active non-library filters
        date_from=..., date_to=..., include_undated=...,
        text_all=..., text_any=..., text_not=...,
        pgp_filter=..., pgp_sys_ids=...,
        editions_filter=..., edition_sys_ids=...,
        sys_id_to_library=state.meta_mgr.get_library_for_id,   # CALLABLE full-corpus mapper (Codex F3)
    )                                                          # -> {library_code: count}, '' / 'LOCAL' skipped
except Exception:
    facets = {}   # ONLY fallback: render the shortlist WITHOUT counts on facet-query failure (Codex F1)
current_library_facets['value'] = facets
```
There is NO page-local/result-derived fallback (Codex R3 F1). When `current_library_facets['value']` is empty (facet-query failure), the dialog renders the shortlist codes WITHOUT counts — never a page-local count substitute. Expose the facet cell to `_open_library_filter_dialog` via a `current_library_facets = {'value': {}}` closure cell refreshed in the io_bound `refresh_results` path.

**Apply handler** (mirrors `search.py:1930–1976`):
```python
async def apply_catalog_library_filter():
    checked_list = await ui.run_javascript(
        f'libFilterGetChecked("{_cid}")', timeout=5.0
    )
    checked = sanitize_library_codes(list(checked_list) if checked_list else [])
    committed_mode = current_mode_cell[0]  # local mutable cell
    if committed_mode == 'show_only':
        if not checked:
            ui.notify(tr('Select at least one library, or check all to clear the filter'), type='warning')
            return
        new_filter = _library_apply_selection(checked, _all_for_norm)
        if not new_filter:
            current_library_mode['value'] = 'hide'
            current_library_filter['value'] = []
        else:
            current_library_mode['value'] = 'show_only'
            current_library_filter['value'] = new_filter
    else:  # hide
        current_library_mode['value'] = 'hide'
        current_library_filter['value'] = checked
    safe_user_set('catalog_library_filter', {
        'mode': current_library_mode['value'],
        'codes': current_library_filter['value'],
    })
    _update_library_filter_btn()
    current_page['value'] = 1
    await refresh_results()
    render_chips()
    _update_search_buttons()
    dialog.close()
```

#### Site 4: `apply_catalog_library_filter` clear site (line 1238–1239)

**Current** (lines 1236–1240):
```python
elif filter_name == 'library':
    current_library_filter['value'] = []
    safe_user_set('catalog_library_filter', [])
    _update_library_filter_btn()
```
**New:**
```python
elif filter_name == 'library':
    current_library_filter['value'] = []
    current_library_mode['value'] = 'hide'
    safe_user_set('catalog_library_filter', {'mode': 'hide', 'codes': []})
    _update_library_filter_btn()
```

#### Site 5: `clear_all_filters` clear site (lines 1260–1263)

**Current** (lines 1260–1266):
```python
current_library_filter['value'] = []
safe_user_set('catalog_pgp_filter', 'all')
safe_user_set('catalog_editions_filter', 'all')
safe_user_set('catalog_library_filter', [])
...
_update_library_filter_btn()
```
**New:**
```python
current_library_filter['value'] = []
current_library_mode['value'] = 'hide'
safe_user_set('catalog_pgp_filter', 'all')
safe_user_set('catalog_editions_filter', 'all')
safe_user_set('catalog_library_filter', {'mode': 'hide', 'codes': []})
...
_update_library_filter_btn()
```

#### DMF-10 LOCAL guard in new dialog function

Every new function in `catalog_browse.py` that iterates `LIBRARY_CODES` must contain the literal `'LOCAL'` (AST guard in `tests/test_web_library_options_no_local.py`). The expand-section list comprehension satisfies this:
```python
expand_codes = [c for c in LIBRARY_CODES if c != 'LOCAL' and c not in shortlist_set
                and c in _codes_with_mss]
```

---

### `web/pages/parallels.py` — `ParallelsState` + restore + new filter button/dialog + post-fetch filter

**Lead analog:** `web/pages/search.py` — restore (lines 186–216), `_open_library_filter_dialog` (1752–1984), `_apply_library_filter` (3830–3853).

#### Site 1: `ParallelsState.__init__` — add new fields (line 156–196)

**Analog:** `web/pages/search_state.py::SearchUIState` fields `library_filter` / `library_mode`.

**Current last lines of `ParallelsState.__init__`** (lines 192–196):
```python
self.restrict_sys_ids: set = None
self.excluded_manuscript_ids: set = set()
self.auto_excluded_source_id: str = None
self.title_translations: dict = {}
self.translation_data: dict = {}
```
**Add after line 196:**
```python
# DMF-09: library filter for parallels page
self.library_filter: list = []   # active library codes (for filter)
self.library_mode: str = 'hide'  # 'show_only' | 'hide' (D-05 default)
```

#### Site 2: Restore block (after line 261, in the `_safe_get` block)

**Analog:** `web/pages/search.py:189–216` (3-branch migration pattern — copy verbatim, changing key name).

**Current block** (lines 261–271):
```python
from web.safe_storage import safe_user_get as _safe_get
_emi = _safe_get('parallels_excluded_manuscript_ids')
p_state.excluded_manuscript_ids = set(_emi) if _emi is not None else set()
...
_pde = _safe_get('parallels_domain_exclusions')
p_state.domain_exclusions = set(_pde) if _pde is not None else set()
```
**Add after `_pde` block:**
```python
# DMF-09: restore library filter mode + codes (key 'parallels_library_filter')
# Mirrors search.py:189–216 D-06 migration pattern.
from shared.browse_map_utils import sanitize_library_codes as _san_lib
_plib_raw = _safe_get('parallels_library_filter', None)
if isinstance(_plib_raw, list):
    _plib_codes = _san_lib(_plib_raw)
    if _plib_codes:
        p_state.library_mode = 'show_only'
        p_state.library_filter = _plib_codes
    else:
        p_state.library_mode = 'hide'
        p_state.library_filter = []
elif isinstance(_plib_raw, dict):
    _pm = _plib_raw.get('mode', 'hide')
    _plib_codes = _san_lib(_plib_raw.get('codes'))
    _pm = _pm if _pm in ('show_only', 'hide') else 'hide'
    if _pm == 'show_only' and not _plib_codes:
        _pm = 'hide'
    p_state.library_mode = _pm
    p_state.library_filter = _plib_codes
else:
    p_state.library_mode = 'hide'
    p_state.library_filter = []
```

#### Site 3: New filter button + dialog

**Analog:** `web/pages/search.py:1752–1996` (`_open_library_filter_dialog` + `library_filter_btn`).

The dialog is structurally identical to the search.py version. Key differences:
- `container_id = f'par-lib-filter-{_uuid.uuid4().hex[:8]}'`
- No `facets` (no current results to derive counts from at page load — start directly with expand-all; or derive from `p_state.results` if results exist)
- Apply handler: persist to `'parallels_library_filter'` key; call `_apply_parallels_library_filter()` post-apply
- `current_mode` cell: read from `p_state.library_mode`

Apply handler persist pattern (mirrors `search.py:1964–1968`):
```python
from web.safe_storage import safe_user_set as _set
_set('parallels_library_filter', {
    'mode': p_state.library_mode,
    'codes': p_state.library_filter,
})
```

#### Site 4: Post-fetch filter function (new)

**Analog:** `web/pages/search.py:3830–3853` (`_apply_library_filter` — copy verbatim, referencing `p_state` instead of `search_state`).

```python
def _apply_parallels_library_filter(results_list):
    """DMF-09: dual-mode filter parallels results by selected library codes.

    Mirrors web/pages/search.py::_apply_library_filter (3830-3853).
    Show-only: keep rows where library_code IN codes.
    Hide: keep rows where library_code NOT IN codes.
    Empty codes in either mode = show all (D-05/D-08).
    """
    mode = getattr(p_state, 'library_mode', 'hide')
    codes = set(p_state.library_filter)
    if mode == 'show_only':
        if not codes:
            return results_list
        return [r for r in results_list
                if r.get('library_code', '') in codes
                or r.get('display', {}).get('library_code', '') in codes]
    else:  # hide
        if not codes:
            return results_list
        return [r for r in results_list
                if r.get('library_code', '') not in codes
                and r.get('display', {}).get('library_code', '') not in codes]
```

`_apply_parallels_library_filter` is used ONLY for **Hide** (Show-only is scoped pre-query — see below). Apply it to `main_results`/`filtered_results` BEFORE `set_parallels_export(...)` (line 2343) and the `safe_user_set('parallels_results', ...)` write (line 2353) so exports + stored payloads are scoped (Codex R1 MED #6), plus a defensive idempotent re-apply at the top of `render_results`.

**HYBRID scoping — `restrict_sys_ids` IS modified for Show-only (Codex R3 F2/F4 — correcting the earlier "not modified" statement):**
The earlier note that "`restrict_sys_ids` is NOT modified for the library filter" is WRONG and is corrected here. The parallels library filter is HYBRID:
- **Show-only (pre-query):** resolve the selected-library sys_ids and INTERSECT them INTO `restrict_sys_ids`. This block MUST live OUTSIDE the `if _has_active_filters():` body (that gate is `False` when ONLY a library filter is set — gating the resolve inside it would make a library-only Show-only never scope; Codex R3 F4) and MUST run AFTER the advanced-filter restrict block (after the empty-match early-return ~2217) and BEFORE the per-manuscript exclusion subtraction at 2219, so library-only AND advanced+library both compose:
  ```python
  # After the `if _has_active_filters():` block (after the 2211-2217 early-return),
  # BEFORE `if p_state.excluded_manuscript_ids and restrict_sys_ids is not None:` (2219):
  if p_state.library_mode == 'show_only' and p_state.library_filter:   # UNGATED by _has_active_filters (Codex F4)
      lib_ids = await run.io_bound(resolve_library_sys_ids, list(p_state.library_filter), state.meta_mgr)
      if lib_ids:  # fail-open: skip if empty
          restrict_sys_ids = lib_ids if restrict_sys_ids is None else (restrict_sys_ids & lib_ids)
  # ...then the existing exclusion subtraction at 2219-2221 subtracts per-ms exclusions from the
  #    now-concrete library set too (library-only case composes), and 2224 captures restrict_sys_ids.
  ```
  This folds the Show-only scope into the Tantivy query (`search_engine.py:2929`) so selected-library hits beyond the unscoped top-50/chunk are retained.
- **Hide (post-fetch):** the full-corpus complement is NOT computable from `restrict_sys_ids` alone (Pitfall 4), so Hide stays a post-fetch filter over the result rows, applied BEFORE export/storage (above). Do NOT touch the `restrict_sys_ids` path for Hide.

The advanced-filter `restrict_sys_ids` semantics + the empty-match early-return (2211-2217) are otherwise unchanged.

#### Site 5: DMF-10 LOCAL guard in new parallels dialog function

New function in `parallels.py` that iterates `LIBRARY_CODES` must contain `'LOCAL'` literal:
```python
expand_codes = [c for c in LIBRARY_CODES if c != 'LOCAL' and c not in shortlist_set
                and c in _codes_with_mss]
```

---

### `tests/test_libfilter_desktop.py` — extend with DMF-07 tests

**Analog:** `tests/test_libfilter_desktop.py:1–23` (existing file header + test pattern — extend, do not create new file).

Existing test pattern for `_CatalogRefreshWorker` (uses `_run_worker_with_library_filter` helper at line 44). New tests must add:
1. `LibraryFilterDialog(mode='show_only', ...)` — `get_mode()` returns `'show_only'`
2. `LibraryFilterDialog(mode='hide', ...)` — `get_mode()` returns `'hide'`
3. D-04: mode flip → checked set reset
4. OK guard: Show-only + zero checked → disabled; Hide + zero checked → enabled
5. `_catalog_library_mode = 'hide'` on `GenizahGUI` init (requires `GenizahGUI` stub or attribute check)

---

### `tests/test_catalog_dual_mode_library_filter.py` (new file)

**Analog:** `tests/test_catalog_availability_filter.py:1–47` (test structure: monkeypatch fjms service, test filter pass-through). Also mirrors `tests/test_dual_mode_library_filter.py` (Phase 130 tests).

Tests to include:
1. Restore migration: plain-list `['CUL','JTS']` → `mode='show_only', codes=['CUL','JTS']`
2. Restore migration: empty plain-list `[]` → `mode='hide', codes=[]`
3. Restore migration: dict shape round-trips
4. Restore migration: invalid dict `mode='show_only', codes=[]` → normalized to `mode='hide'`
5. Apply persists dict shape `{'mode': ..., 'codes': [...]}`
6. `_update_library_filter_btn` shows correct 3-state labels
7. `_open_library_filter_dialog` function body contains `'LOCAL'` literal (AST check)
8. `library_codes_with_manuscripts()` referenced in the dialog build (AST/source scan)

---

### `tests/test_parallels_library_filter.py` (new file)

**Analog:** `tests/test_dual_mode_library_filter.py` (Phase 130 — mirrors pure-behavior tests for the search page).

Tests to include:
1. `ParallelsState` defaults: `library_filter=[]`, `library_mode='hide'`
2. Restore migration: same 4 cases as catalog (plain-list, empty-list, dict, invalid)
3. `_apply_parallels_library_filter` Show-only: keeps only IN-set results
4. `_apply_parallels_library_filter` Hide: keeps only NOT-IN-set results
5. Both modes with empty codes: returns full list (D-05/D-08)
6. `'LOCAL'` absent from parallels filter options (pure function test with a mock LIBRARY_CODES that includes LOCAL)

---

## Shared Patterns

### Persistence chokepoint
**Source:** `web/safe_storage.py` via `web/pages/search.py` — `persist_value` wrapper and `_safe_get` alias.
**Apply to:** all `safe_user_set('catalog_library_filter', ...)` and `safe_user_set('parallels_library_filter', ...)` calls.

New persist shape (both surfaces):
```python
safe_user_set('<surface>_library_filter', {
    'mode': current_library_mode['value'],   # 'show_only' | 'hide'
    'codes': current_library_filter['value'],  # list[str]
})
```
Never write a plain `list` — that is the legacy v8.3.0 shape that the migration code reads and upgrades.

### D-06 Legacy migration (3-branch restore)
**Source:** `web/pages/search.py:189–216` — the canonical implementation, shipped in Phase 130.
**Apply to:** restore blocks in `catalog_browse.py` and `parallels.py`.

Pattern rule: `isinstance(raw, list)` → Show-only migration; `isinstance(raw, dict)` → read mode+codes; else → fresh Hide/empty default. Always call `sanitize_library_codes()` on codes.

### `sanitize_library_codes` import
**Source:** `web/pages/search.py:43` — `from shared.browse_map_utils import ..., sanitize_library_codes`
**Apply to:** both `catalog_browse.py` and `parallels.py` import blocks — add `sanitize_library_codes` to the existing `browse_map_utils` import.

### `library_codes_with_manuscripts()` usage (DMF-13)
**Source:** `shared/browse_map_utils.py:123` — `def library_codes_with_manuscripts() -> frozenset`
**Apply to:** every dialog build that constructs the full library code list (desktop `LibraryFilterDialog.__init__`, web catalog dialog, web parallels dialog). Pattern:
```python
_codes_with_mss = library_codes_with_manuscripts()
expand_codes = [c for c in LIBRARY_CODES if c != 'LOCAL' and c not in shortlist_set
                and c in _codes_with_mss]
```
Called inside a dialog-build function (not at module level) to defer the first-call CSV read.

### LOCAL exclusion AST guard
**Source:** `tests/test_web_library_options_no_local.py` — scans `web/pages/*.py` for functions referencing `LIBRARY_CODES` without the `'LOCAL'` string literal.
**Apply to:** every new function in `catalog_browse.py` and `parallels.py` that iterates `LIBRARY_CODES`. Must contain the inline guard:
```python
[c for c in LIBRARY_CODES if c != 'LOCAL' ...]
```

### 3-state button logic
**Source:** `web/pages/search.py:1700–1750` (`_update_library_btn`) and `genizah_app.py:10443–10470` (`_catalog_update_library_filter_btn`).
**Apply to:** both `_update_library_filter_btn` in `catalog_browse.py` and new button updater in `parallels.py`.

Three states (D-07):
- `not flt` → neutral: `tr('Filter by library')`, outline primary
- `mode == 'show_only' and flt` → `f"{tr('Filter by library')} ({shown}/{total})"`, filled negative/red
- `mode == 'hide' and flt` → `f"{tr('Hiding')} {n}"`, filled deep-orange

Translation keys `'Hiding N library'` / `'Hiding N libraries'` / `'Showing {shown}/{total} library'` / `'Showing {shown}/{total} libraries'` are already in `genizah_translations.py` (added in Phase 130).

### `_apply_library_filter` dual-mode logic
**Source:** `web/pages/search.py:3830–3853` — the canonical shipped implementation.
**Apply to:** new `_apply_parallels_library_filter` in `parallels.py` (copy verbatim, substitute `p_state` for `search_state`). The catalog surface uses `_CatalogRefreshWorker.run()` (pre-query, server-side) rather than a post-fetch Python filter — its mode handling goes into the `get_browse_results` call.

---

## No Analog Found

None. All Phase 131 modifications have direct in-codebase analogs (either exact self-analogs or the Phase 130 shipped code).

---

## Metadata

**Analog search scope:**
- `desktop/dialogs_filter.py` (1,820+ lines) — `LibraryFilterDialog`, `PreSearchFilterDialog`
- `genizah_app.py` (25,000+ lines) — `_CatalogRefreshWorker`, `_open_catalog_library_dialog`, `_catalog_update_library_filter_btn`, `_catalog_start_async_refresh`, `_catalog_library_filter` init
- `web/pages/catalog_browse.py` (~1,600 lines) — restore, dialog, button, apply, clear sites
- `web/pages/parallels.py` (~2,300+ lines) — `ParallelsState`, restore, `restrict_sys_ids` path
- `web/pages/search.py` (~4,300+ lines) — shipped Phase 130 implementation (all 6 modification sites)
- `shared/browse_map_utils.py` — `library_codes_with_manuscripts`, `sanitize_library_codes`, `LIBRARY_CODES`
- `tests/test_libfilter_desktop.py`, `tests/test_catalog_availability_filter.py`

**Files scanned:** 8 production + 2 test
**Pattern extraction date:** 2026-06-30
