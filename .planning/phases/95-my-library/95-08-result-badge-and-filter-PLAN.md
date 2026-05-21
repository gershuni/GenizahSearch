---
phase: 95
plan: 08
type: execute
wave: 4
depends_on: [02, 05, 06, 07]
files_modified:
  - genizah_app.py
  - tests/test_local_filter_cascade.py
  - tests/test_local_filter_persistence.py
autonomous: false
requirements: [REQ-6, REQ-7]
must_haves:
  truths:
    - "LOCAL hits write source='LOCAL' on result rows; existing COL_SRC column renders 'LOCAL' in blue #3498db (D-11)"
    - "COL_SRC visibility rule extends to OR result set contains any LOCAL hit (D-11)"
    - "Three-state LOCAL filter button appears on Search / Composition Search / Parallels result toolbars (REQ-6)"
    - "Filter cycles all → only_local → no_local → all per click; mirrors Phase 93 PGP-filter pattern"
    - "Filter is hidden when no LOCAL hits present in current result set; persisted per-surface via QSettings (D-39)"
    - "When state is only_local/no_local AND zero LOCAL hits exist, filter renders as NO-OP + inline chip 'My Library filter inactive — no LOCAL hits in this query' (D-10 P1 fix)"
    - "Filter cascade discipline: LOCAL filter applied AFTER printed filter + exclusions + refinement chain within _apply_results_table_filters (search) and _apply_comp_tree_filters (composition)"
    - "LOCAL hit click → Browse panel text-only mode (no image pane) with prev/next page nav + 'Open file' button (D-27, D-28)"
    - "Composition Search QTreeWidget (8 cols today) gets a new COL_SRC_COMP column inserted at index 8 (after Printed) — Parallels uses the same tree (D-12 audit resolved)"
  artifacts:
    - path: "genizah_app.py"
      provides: "COL_SRC LOCAL badge + visibility extension; new comp_col_src constant + tree column; three-state LOCAL filter button + persistence + cascade hooks; Browse panel text-only mode for LOCAL hits; Open File button"
      contains: "_apply_local_filter"
  key_links:
    - from: "tests/test_local_filter_cascade.py"
      to: "genizah_app.py"
      via: "static AST scanner asserting _apply_results_table_filters AND _apply_comp_tree_filters both call _apply_local_filter"
      pattern: "_apply_local_filter"
    - from: "myLibrary/search_local_filter QSettings key (+ composition + parallels)"
      to: "filter state restoration on app launch"
      via: "self._settings.value(...) at init"
      pattern: "myLibrary/.*_local_filter"
---

<objective>
Wire LOCAL hits into the desktop result-rendering surfaces. Three cross-cutting concerns:

**(A) COL_SRC LOCAL badge + visibility extension (REQ-7 / D-11):**
- Write `source='LOCAL'` on LOCAL result-rows so the existing `COL_SRC` at `genizah_app.py:16534` renders it.
- Color LOCAL cells `#3498db` blue (symmetric with PGP's `#27ae60` green at `:16538`).
- Extend `COL_SRC` visibility rule at `:16741` to OR-in `has any LOCAL hit in result set`.

**(B) D-12 Composition / Parallels Src column — AUDIT RESOLVED:**

The planner has inspected `genizah_app.py` and confirms:

1. **Main search table** — `self.results_table` (QTableWidget) at `genizah_app.py:5914-5915`. Has a `Src` column at `self.COL_SRC` (column index 8). **Reuse** per D-11. No new column.

2. **Composition Search tree** — `self.comp_tree` (QTreeWidget) at `genizah_app.py:6321`. Columns: `["Score", "Library", "Shelfmark", "Title", "System ID", "Context", "MS Context", "Printed"]` (8 columns, indexes 0..7). Column constants are at `genizah_app.py:2790-2796`. **There is NO Src column today.** Per D-12, **add a new compact `Src` column at index 8** (after Printed). New constant: `self.comp_col_src = 8` (placed after `self.comp_col_printed = 7` at `genizah_app.py:2796`). The header label list at `:6321` is extended with `tr("Src")` as the 9th entry.

3. **Parallels** — `browse_search_parallels` at `genizah_app.py:10159` does NOT have a separate result table. It calls `self.send_result_to_composition(...)` which routes results into the SAME Composition Search tree (`self.comp_tree`). **Parallels therefore inherits the Composition column extension automatically — no separate column work needed.** The "three surfaces" of REQ-6 collapse to TWO tables in practice: `results_table` (search) and `comp_tree` (composition + parallels).

**(C) Three-state LOCAL filter button (REQ-6):**
- Add filter buttons to Search / Composition Search / Parallels result toolbars labeled `Filter Local` / `Only Local` / `No Local` (EN) and `סנן מקומי` / `רק מקומי` / `ללא מקומי` (HE) per D-10.
- Even though Composition and Parallels share `comp_tree`, the filter buttons are still THREE INDEPENDENT toolbar buttons with three QSettings keys per D-39 — Parallels surfaces filter state from `myLibrary/parallels_local_filter` and applies it to the Parallels-originated result subset.
- Same `outline dense no-caps` styling as PGP filter (Phase 93).
- Cycle states `all → only_local → no_local → all` per click.
- Hidden until LOCAL hits exist in current result set.
- **D-10 P1 fix:** when state is `only_local` or `no_local` AND zero LOCAL hits exist, filter renders as NO-OP + inline chip surfaces.
- Cascade discipline: applied INSIDE the existing master cascade functions on desktop. **W4 RESOLVED:** desktop has NO `_apply_pgp_filter` function (that's a web-only function). The desktop cascade joinpoints are `_apply_results_table_filters` (at `genizah_app.py:17158`, called from `_open_results_filter_dialog`, `display_results`, etc.) and `_apply_comp_tree_filters` (the corresponding function for `comp_tree`). The AST scanner test asserts both of those functions invoke `_apply_local_filter`.

**(D) LOCAL hit click → Browse panel text-only (D-27 + D-28):**
- LOCAL hits use existing Browse panel machinery with NO image pane. **I15 RESOLVED:** the existing "no image" path on desktop is `self.browse_viewer.setVisible(False)` (the `browse_viewer` QWidget is toggled by `toggle_browse_image` at `genizah_app.py:10155-10157` via the `btn_b_toggle_img` toolbar button). Plan 08 introduces a tiny helper `_set_browse_image_pane_visible(visible: bool)` that wraps `self.browse_viewer.setVisible(visible)` AND `self.btn_b_toggle_img.setChecked(visible)` so the toolbar toggle state stays in sync.
- Add an `Open file` button to the Browse toolbar that calls `os.startfile(filepath)` (Windows native).

Output: Modified `genizah_app.py` (multiple sites) + 2 GREEN test files. One human checkpoint for visual verification of badge color + filter button cycling.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/95-my-library/95-CONTEXT.md
@.planning/phases/95-my-library/95-PATTERNS.md
@genizah_app.py
@web/pages/search.py
@tests/test_pgp_filter_cascade.py

<interfaces>
COL_SRC write site (genizah_app.py:16533-16545):
```python
self.results_table.setItem(row_idx, self.COL_IMG, QTableWidgetItem(str(meta.get('img', ''))))
# Src
self.results_table.setItem(row_idx, self.COL_SRC, QTableWidgetItem(str(meta.get('source', ''))))
# PGP badge
if sid and sid in self._pgp_transcription_sys_ids:
    pgp_item = QTableWidgetItem("PGP")
    pgp_item.setForeground(QColor("#27ae60"))
    self.results_table.setItem(row_idx, self.COL_PGP, pgp_item)
else:
    self.results_table.setItem(row_idx, self.COL_PGP, QTableWidgetItem(""))
```

Visibility rule (genizah_app.py:16738-16741):
```python
has_multiple_sources = os.path.exists(Config.FILE_V7) and os.path.getsize(Config.FILE_V7) > 0
self.results_table.setColumnHidden(self.COL_SRC, not has_multiple_sources)
```

Composition tree header (genizah_app.py:6321 — VERBATIM current state, 8 columns):
```python
self.comp_tree = QTreeWidget(); self.comp_tree.setHeaderLabels([tr("Score"), tr("Library"), tr("Shelfmark"), tr("Title"), tr("System ID"), tr("Context"), tr("MS Context"), tr("Printed")])
```

Composition column constants (genizah_app.py:2790-2796 — VERBATIM current state, NO src):
```python
self.comp_col_library = 1
self.comp_col_shelfmark = 2
self.comp_col_title = 3
self.comp_col_sysid = 4
self.comp_col_context = 5
self.comp_col_ms_context = 6
self.comp_col_printed = 7
```

Browse image toggle (genizah_app.py:10155-10157 — the "no image" pattern to wrap):
```python
def toggle_browse_image(self):
    visible = self.btn_b_toggle_img.isChecked()
    self.browse_viewer.setVisible(visible)
```

Desktop master cascade joinpoints (NOT `_apply_pgp_filter` — that's web only):
- `_apply_results_table_filters` at `genizah_app.py:17158` — invoked from `display_results`, `_open_results_filter_dialog` (called after every printed-filter cycle at `:17119`), `_apply_results_table_filters` at `:16565`, etc.
- `_apply_comp_tree_filters` — the corresponding cascade function for `comp_tree` (planner verifies the exact name via `grep -n "def _apply_comp" genizah_app.py` during execution).

PGP filter cycle pattern (web/pages/search.py:1441-1444 — mirror this for LOCAL):
```python
states = ['all', 'only_pgp', 'hide_pgp']
current_idx = states.index(search_state.pgp_filter)
search_state.pgp_filter = states[(current_idx + 1) % 3]
persist_value('search_pgp_filter', search_state.pgp_filter)
```

Static AST cascade scanner (tests/test_pgp_filter_cascade.py — verbatim template):
```python
def _function_contains_call(func_node, name: str) -> bool:
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            callee = node.func
            if isinstance(callee, ast.Name) and callee.id == name:
                return True
            if isinstance(callee, ast.Attribute) and callee.attr == name:
                return True
    return False
```

D-39 per-surface QSettings keys:
- `myLibrary/search_local_filter`
- `myLibrary/composition_local_filter`
- `myLibrary/parallels_local_filter`
Default value `"all"`; cycle states `"all"` → `"only_local"` → `"no_local"` → `"all"`.

D-10 P1 fix:
- When state is `only_local` or `no_local` AND no LOCAL hits exist in current results: filter renders as NO-OP (all hits shown). Inline chip surfaces: `"My Library filter inactive — no LOCAL hits in this query"`. Persisted state preserved.
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: COL_SRC LOCAL badge write + visibility extension + new comp_col_src column (D-11 + D-12 audit resolution)</name>
  <read_first>
    - genizah_app.py:16533-16545 (COL_SRC + COL_PGP write site)
    - genizah_app.py:16738-16741 (visibility rule)
    - genizah_app.py:2790-2796 (composition column constants — NO Src today, verified)
    - genizah_app.py:6321 (composition tree header — 8 columns today, verified)
    - genizah_app.py:6340-6376 (composition tree header resize modes + column widths — needs to be extended for the new comp_col_src)
    - genizah_app.py:20494-20540 (add_manuscript_node — the comp_tree row writer; this is where we write the Src cell for composition rows)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Modification 2: COL_SRC LOCAL badge + visibility extension (D-11)")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-11 — `#3498db` blue + visibility OR LOCAL; D-12 — uniform Src across surfaces)
  </read_first>
  <behavior>
    Snapshot test (manual via checkpoint Task 4): a 2-row mixed result fixture — row 1 (NLI, source='V0.8') has no special color; row 2 (LOCAL, source='LOCAL') has blue text in COL_SRC.

    Visibility test: when result set has no V7 data AND no LOCAL hits, COL_SRC is hidden. When it has either, COL_SRC is visible.

    Composition tree test: after Task 1 ships, `self.comp_tree.columnCount() == 9` AND `self.comp_col_src == 8`.
  </behavior>
  <action>
    **Subtask 1.1 — Main search table (results_table) — REUSE existing COL_SRC per D-11:**

    At `genizah_app.py:16534`, REPLACE the simple write:
    ```python
    self.results_table.setItem(row_idx, self.COL_SRC, QTableWidgetItem(str(meta.get('source', ''))))
    ```
    with the LOCAL-aware version:
    ```python
    # Phase 95 D-11 — LOCAL source rendered in blue #3498db (symmetric with PGP green).
    source_val = str(meta.get('source', ''))
    if source_val == 'LOCAL':
        src_item = QTableWidgetItem('LOCAL')
        src_item.setForeground(QColor("#3498db"))
        self.results_table.setItem(row_idx, self.COL_SRC, src_item)
    else:
        self.results_table.setItem(row_idx, self.COL_SRC, QTableWidgetItem(source_val))
    ```

    At `genizah_app.py:16741`, EXTEND the visibility rule:
    ```python
    has_multiple_sources = os.path.exists(Config.FILE_V7) and os.path.getsize(Config.FILE_V7) > 0
    # Phase 95 D-11 — show COL_SRC also when LOCAL hits present.
    has_local = any(
        (r.get('display', {}) or {}).get('source') == 'LOCAL'
        for r in (self.last_results or [])
    )
    self.results_table.setColumnHidden(self.COL_SRC, not (has_multiple_sources or has_local))
    ```

    The exact attribute name on the SearchEngine result-cache is `self.last_results` per the existing visibility rule context. If the executor finds a different variable in scope (verify via `grep -n "self\\.last_results" genizah_app.py | head`), use the one already in scope at line 16741.

    **Subtask 1.2 — Composition tree (comp_tree) — ADD new comp_col_src column at index 8 per D-12 audit:**

    The planner has verified `comp_tree` has 8 columns today (no Src). Per D-12, ADD a new Src column at index 8 (after Printed). Concrete edits:

    (a) At `genizah_app.py:2796` (immediately after `self.comp_col_printed = 7`), ADD:
    ```python
    # Phase 95 D-12 — uniform Src column for LOCAL badge on Composition / Parallels.
    self.comp_col_src = 8  # New compact column appended after Printed
    ```

    (b) At `genizah_app.py:6321`, EXTEND the header labels list (currently 8 entries):
    ```python
    self.comp_tree = QTreeWidget(); self.comp_tree.setHeaderLabels([tr("Score"), tr("Library"), tr("Shelfmark"), tr("Title"), tr("System ID"), tr("Context"), tr("MS Context"), tr("Printed"), tr("Src")])
    ```

    (c) After the existing column-width setup block (around `:6344-6354`), ADD resize/width config for the new column (mirror the `comp_col_printed` Fixed-mode pattern):
    ```python
    # Phase 95 D-12 — Src column setup
    header.setSectionResizeMode(self.comp_col_src, QHeaderView.ResizeMode.Fixed)
    self.comp_tree.setColumnWidth(self.comp_col_src, 60)  # narrow fixed, mirrors Printed width
    ```

    (d) Mirror the same Fixed resize call inside the SECOND header setup block (around `:6371-6376`) where `comp_col_printed` already has its `Fixed` mode call.

    (e) Update the `filter_columns=` list at `:6364` to INCLUDE `self.comp_col_src` so the column header filter chip works for it:
    ```python
    filter_columns=[self.comp_col_library, self.comp_col_shelfmark, self.comp_col_title, self.comp_col_context, self.comp_col_ms_context, self.comp_col_printed, self.comp_col_src],
    ```

    (f) Inside `add_manuscript_node` (called by `display_comp_results` at `:20494+`), write the Src cell after the existing Printed cell logic. Locate where `apply_printed_badge(node, sid)` is invoked (it sets the Printed column). IMMEDIATELY AFTER that call, ADD:
    ```python
    # Phase 95 D-11 — Src cell with LOCAL color (composition tree)
    source_val = str(ms_item.get('source', '') or (ms_item.get('display', {}) or {}).get('source', ''))
    if source_val == 'LOCAL':
        node.setText(self.comp_col_src, 'LOCAL')
        node.setForeground(self.comp_col_src, QColor("#3498db"))
    else:
        node.setText(self.comp_col_src, source_val)
    ```

    (g) Add a visibility/column-hide rule for `comp_col_src`. Find the existing `setColumnHidden` calls for `comp_tree` (grep `comp_tree.setColumnHidden` or `setColumnHidden.*comp_col`). If none exist for `comp_col_printed`, that column is always visible — match that behavior. If a `setColumnHidden(comp_col_printed, ...)` pattern exists, mirror it for `comp_col_src` with the rule `not has_local_in_comp_results`.

    **Subtask 1.3 — Parallels (no separate table — automatic):**

    `browse_search_parallels` at `:10159` routes via `send_result_to_composition`, which populates `self.comp_tree`. The work in Subtask 1.2 ALREADY covers Parallels — no separate column edit needed. Verify by running a Parallels search after Subtask 1.2: the Src column should appear in the Composition tab's tree.

    Document this finding in `95-08-SUMMARY.md` as the resolution of D-12.

    **Subtask 1.4 — DO NOT modify:**
    - `catalog_results_table` (`:10735-10737`) — that's the Catalog Browse table, not a search result surface.
    - `lists_items_table` (`:11777-11778`) — Personal Lists table, not search results.
  </action>
  <verify>
    <automated>python -c "import re; src=open('genizah_app.py',encoding='utf-8').read(); assert '#3498db' in src; assert 'source_val == \\'LOCAL\\'' in src or '\"LOCAL\"' in src; assert 'has_local' in src; assert 'self.comp_col_src' in src; assert 'comp_col_src = 8' in src; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "#3498db" genizah_app.py` returns ≥ 2 (one for results_table, one for comp_tree).
    - `grep -c "source_val == 'LOCAL'" genizah_app.py` returns ≥ 2.
    - `grep -c "has_local" genizah_app.py` returns ≥ 1.
    - `grep -c "self.comp_col_src" genizah_app.py` returns ≥ 4 (constant declaration + filter_columns + write + visibility).
    - `grep -c "comp_col_src = 8" genizah_app.py` returns 1.
    - Composition tree header label list has 9 entries: `grep -nE "setHeaderLabels.*Score.*Src" genizah_app.py` returns 1 match.
    - `python -c "import ast; ast.parse(open('genizah_app.py',encoding='utf-8').read())"` exits 0.
    - REGRESSION: `python -m pytest tests/ -q` exits 0.
    - `python -m ruff check genizah_app.py` exits 0 (or no NEW errors compared to baseline).
  </acceptance_criteria>
  <done>COL_SRC writes blue 'LOCAL' for LOCAL rows in main search table; new comp_col_src column added to comp_tree per D-12 audit; Parallels inherits automatically.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Three-state LOCAL filter button + cascade hook (REQ-6 / D-10 / D-39) — W4 RESOLVED</name>
  <read_first>
    - web/pages/search.py:1432-1480 (PGP filter button + _toggle_pgp_filter + _update_pgp_filter_btn — mirror this pattern)
    - genizah_app.py:17097-17121 (the desktop printed-filter cycle pattern — closest existing analog, since desktop has no _apply_pgp_filter)
    - genizah_app.py:17158 (`_apply_results_table_filters` — master cascade joinpoint for SEARCH surface)
    - genizah_app.py — find `def _apply_comp_tree_filters` (master cascade joinpoint for COMPOSITION + PARALLELS surfaces) via `grep -n "def _apply_comp" genizah_app.py`
    - .planning/phases/95-my-library/95-PATTERNS.md ("Pattern: Three-state filter button (Phase 93 / PGP-FILTER)" + "Pattern: Static AST cascade-coverage test")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-10, D-10 P1 fix, D-39)
    - tests/test_pgp_filter_cascade.py (verbatim AST template — 121 lines)
  </read_first>
  <behavior>
    Test `test_local_filter_applied_within_results_cascade` (in tests/test_local_filter_cascade.py):
    - Static AST scan of `genizah_app.py`. Use the same `_function_contains_call` + `_iter_function_defs` scanner from `tests/test_pgp_filter_cascade.py`.
    - **W4 RESOLVED:** desktop has NO `_apply_pgp_filter`. The scanner targets TWO functions instead:
      1. `_apply_results_table_filters` (master cascade for main search table) — MUST call `_apply_local_filter`.
      2. `_apply_comp_tree_filters` (master cascade for comp_tree; covers Composition + Parallels) — MUST call `_apply_local_filter`.
    - For each target function, assert it contains a call to `_apply_local_filter` (the scanner walks the AST of the function body looking for `Call(func=Attribute(attr='_apply_local_filter'))` or `Call(func=Name(id='_apply_local_filter'))`).
    - If the exact composition cascade function is named differently (executor verifies via grep — the body shape "filter rows in comp_tree by self.comp_filters + self._comp_printed_filter_state" — see `:17475-17560` for hints), use the name discovered. Document the discovered name in `95-08-SUMMARY.md`.
    - Allowlist explicit exemptions (none expected initially).

    Test `test_no_op_when_no_local_hits` (in tests/test_local_filter_cascade.py):
    - Set `self._local_filter_state_search = 'only_local'`. Set `self.last_results = [genizah_only_row]` (no LOCAL).
    - Call `_apply_local_filter(self.last_results, 'only_local')`.
    - Assert filtered result == `[genizah_only_row]` (filter is NO-OP per D-10 P1 fix).
    - Assert an inline chip is surfaced (verify via mock spy on the chip-creation method OR check a flag like `self._local_filter_inactive_chip_visible == True`).

    Test `test_3_qsettings_keys_persist` (in tests/test_local_filter_persistence.py):
    - Construct a tiny mock subject (a `QObject` with a `QSettings` member) representing the filter-state owner.
    - Set each of `myLibrary/search_local_filter`, `myLibrary/composition_local_filter`, `myLibrary/parallels_local_filter` to different values.
    - Tear down and re-instantiate (simulating app restart).
    - Assert each value is restored from QSettings.

    Test `test_filter_cycle_all_only_no` (in tests/test_local_filter_cascade.py):
    - Initial state `"all"`. Call `_toggle_local_filter_search()`. Assert state == `"only_local"`.
    - Call again. Assert state == `"no_local"`.
    - Call again. Assert state == `"all"` (cycle).
  </behavior>
  <action>
    1. Add filter UI to each of the three result toolbars (Search, Composition Search, Parallels) in `genizah_app.py`. The closest existing analog on desktop is the printed-filter cycle at `:17097-17121` (not `_apply_pgp_filter` — that's web only). Identify the toolbar containers (the search result toolbar is constructed around `:5914+`; the composition toolbar around `:6321+`):

    ```bash
    grep -nE "(addWidget|search_toolbar|comp_toolbar|_printed_filter_state)" genizah_app.py | head -30
    ```

    2. For each surface, create a tri-state filter button. Pattern (for the Search surface):
    ```python
    # Phase 95 REQ-6 D-10 — three-state LOCAL filter button (search surface).
    self.local_filter_btn_search = QPushButton(self)
    self.local_filter_btn_search.setProperty("flat", True)
    self.local_filter_btn_search.setStyleSheet(
        "QPushButton { padding: 2px 6px; }"  # outline dense no-caps per D-10
    )
    self.local_filter_btn_search.clicked.connect(self._toggle_local_filter_search)
    self.local_filter_btn_search.setVisible(False)  # hidden until LOCAL hits exist
    # Restore persisted state per D-39
    self._local_filter_state_search = self._settings.value(
        "myLibrary/search_local_filter", "all", type=str
    )
    self._update_local_filter_btn_search()
    # Add to the search toolbar (placed beside the existing printed-filter cycle setup)
    self.search_toolbar.addWidget(self.local_filter_btn_search)
    ```

    Mirror for Composition Search (`self.local_filter_btn_composition` + `_toggle_local_filter_composition` + `myLibrary/composition_local_filter`) and Parallels (`self.local_filter_btn_parallels` + `_toggle_local_filter_parallels` + `myLibrary/parallels_local_filter`).

    Parallels button placement note: since Parallels routes results into `comp_tree`, the Parallels filter button STILL lives on its own toolbar (the Browse-tab toolbar where `btn_find_parallels` lives at `:6462-6464`). It controls the third independent QSettings key but applies on `comp_tree` results when those came from a parallels run. The executor wires this by tracking the source surface in a per-result tag (e.g., `self._comp_results_from_parallels: bool`) and selecting the appropriate filter state at apply time.

    3. Implement the cycle + update methods:
    ```python
    def _toggle_local_filter_search(self):
        states = ['all', 'only_local', 'no_local']
        cur = states.index(self._local_filter_state_search)
        new = states[(cur + 1) % 3]
        self._local_filter_state_search = new
        self._settings.setValue("myLibrary/search_local_filter", new)
        self._update_local_filter_btn_search()
        self._apply_results_table_filters()  # existing master cascade (line 17158)

    def _update_local_filter_btn_search(self):
        labels = {
            'all': (self.tr("Filter Local"), self.tr("סנן מקומי")),
            'only_local': (self.tr("Only Local"), self.tr("רק מקומי")),
            'no_local': (self.tr("No Local"), self.tr("ללא מקומי")),
        }
        text_en, text_he = labels[self._local_filter_state_search]
        # Locale-aware label per existing tr() locale routing (CURRENT_LANG check).
        self.local_filter_btn_search.setText(text_he if CURRENT_LANG == 'he' else text_en)
    ```

    Mirror for Composition + Parallels (the composition variant calls `_apply_comp_tree_filters` instead of `_apply_results_table_filters`).

    4. Visibility gate — call this after every search re-render:
    ```python
    def _update_local_filter_visibility(self, surface: str):
        """Hide filter button when no LOCAL hits in current result set."""
        results = self._get_results_for_surface(surface)  # planner picks
        has_local = any(
            (r.get('display', {}) or {}).get('source') == 'LOCAL'
            for r in results
        )
        btn = {
            'search': self.local_filter_btn_search,
            'composition': self.local_filter_btn_composition,
            'parallels': self.local_filter_btn_parallels,
        }[surface]
        btn.setVisible(has_local)
    ```

    5. Implement `_apply_local_filter`. Place near `_apply_results_table_filters`:
    ```python
    def _apply_local_filter(self, results, state):
        """Apply LOCAL three-state filter per D-10 / D-10 P1.

        state: 'all' | 'only_local' | 'no_local'.
        D-10 P1: when state is only_local/no_local AND zero LOCAL hits exist,
        return unfiltered results (NO-OP) and set the inactive-chip flag.
        """
        if state == 'all':
            self._local_filter_inactive_chip_visible = False
            return results
        has_local = any(
            (r.get('display', {}) or {}).get('source') == 'LOCAL'
            for r in results
        )
        if not has_local:
            # D-10 P1 NO-OP — preserve state but show inline chip.
            self._local_filter_inactive_chip_visible = True
            return results
        self._local_filter_inactive_chip_visible = False
        if state == 'only_local':
            return [r for r in results if (r.get('display', {}) or {}).get('source') == 'LOCAL']
        if state == 'no_local':
            return [r for r in results if (r.get('display', {}) or {}).get('source') != 'LOCAL']
        return results
    ```

    6. **Cascade discipline — W4 RESOLVED:** The desktop master cascade functions are `_apply_results_table_filters` (at `:17158`, for main search) and `_apply_comp_tree_filters` (the analogous function for `comp_tree`; executor verifies exact name via `grep -n "def _apply_comp" genizah_app.py`). Both functions MUST be modified to call `_apply_local_filter` after the existing printed-filter / column-filter logic but before final rendering.

    Concrete edit inside `_apply_results_table_filters` (line 17158): after the existing `self._printed_filter_state` filter clause (around `:17221-17227`), ADD:
    ```python
    # Phase 95 REQ-6 — LOCAL filter cascade joinpoint.
    if hasattr(self, '_local_filter_state_search'):
        # _apply_local_filter mutates self._local_filter_inactive_chip_visible.
        # The function works on row dicts; for the QTableWidget we hide rows
        # whose underlying source dict matches the filter exclusion.
        # Apply per-row: if state is only_local/no_local and the row's source
        # doesn't match the filter (and there ARE LOCAL hits to make it active),
        # call self.results_table.setRowHidden(row_idx, True).
        ...
    ```

    Inside the corresponding composition cascade function, do the same with `_local_filter_state_composition` (or `_local_filter_state_parallels` when `self._comp_results_from_parallels` is set).

    7. Implement an inline "filter inactive" chip — a `QLabel` near the filter button, visible only when `self._local_filter_inactive_chip_visible == True`. Text: `tr("My Library filter inactive — no LOCAL hits in this query")`.

    8. Implement the GREEN test bodies in `tests/test_local_filter_cascade.py` and `tests/test_local_filter_persistence.py` per the behavior block.
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_filter_cascade.py tests/test_local_filter_persistence.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "_toggle_local_filter_search\\|_toggle_local_filter_composition\\|_toggle_local_filter_parallels" genizah_app.py` returns ≥ 3.
    - `grep -c "_apply_local_filter" genizah_app.py` returns ≥ 3 (definition + call from search cascade + call from comp cascade).
    - `grep -c "myLibrary/search_local_filter\\|myLibrary/composition_local_filter\\|myLibrary/parallels_local_filter" genizah_app.py` returns ≥ 3.
    - `grep -c "סנן מקומי\\|רק מקומי\\|ללא מקומי" genizah_app.py` returns ≥ 3 (Hebrew labels per D-10).
    - `grep -c "_local_filter_inactive_chip_visible\\|filter inactive" genizah_app.py` returns ≥ 2 (D-10 P1 chip).
    - `python -m pytest tests/test_local_filter_cascade.py tests/test_local_filter_persistence.py -x -q` exits 0 with all tests PASSED.
    - REGRESSION: `python -m pytest tests/test_pgp_filter_cascade.py -x -q` exits 0 (PGP cascade test still passes; it only scans web code).
    - `python -m ruff check genizah_app.py tests/test_local_filter_cascade.py tests/test_local_filter_persistence.py` exits 0.
  </acceptance_criteria>
  <done>3 filter buttons + cycle + persistence + cascade hooks injected INTO _apply_results_table_filters AND _apply_comp_tree_filters + chip + green tests.</done>
</task>

<task type="auto">
  <name>Task 3: LOCAL hit click → Browse panel text-only mode + Open File button (D-27 + D-28) — I15 RESOLVED</name>
  <read_first>
    - genizah_app.py:10155-10157 (`toggle_browse_image` — confirmed "no image" pattern: `self.browse_viewer.setVisible(visible)` driven by `btn_b_toggle_img`)
    - genizah_app.py — locate the Browse panel implementation:
      ```bash
      grep -n "def.*browse_panel\\|browse_map\\|self\\.browse_viewer\\|create_browse_tab" genizah_app.py | head -20
      ```
    - genizah_app.py — find the result-row double-click handler:
      ```bash
      grep -nE "cellClicked|cellDoubleClicked|itemDoubleClicked" genizah_app.py | head -10
      ```
    - .planning/phases/95-my-library/95-CONTEXT.md (D-27, D-28)
    - .planning/phases/95-my-library/95-PATTERNS.md (specifics)
  </read_first>
  <action>
    1. **I15 RESOLVED:** The desktop "no image" pattern is `self.browse_viewer.setVisible(False)` (driven by `self.btn_b_toggle_img.isChecked()` at `:10155-10157`). Plan 08 wraps this in a helper:

    Add a small helper method near `toggle_browse_image`:
    ```python
    def _set_browse_image_pane_visible(self, visible: bool):
        """Phase 95 D-27 helper — programmatic equivalent of toggle_browse_image.
        Keeps the toolbar toggle button state in sync with the pane visibility.
        Used by LOCAL hits which always render text-only (no image)."""
        if hasattr(self, 'btn_b_toggle_img'):
            self.btn_b_toggle_img.setChecked(visible)
        if hasattr(self, 'browse_viewer'):
            self.browse_viewer.setVisible(visible)
    ```

    2. Locate the result-row double-click handler. In the click handler, after determining the sys_id of the clicked row, check if it's LOCAL:
    ```python
    from shared.local_sys_id import is_local_sys_id
    ...
    if is_local_sys_id(sys_id):
        self._open_local_browse(sys_id, fl_id_or_full_header)
    else:
        # existing Genizah browse path
        ...
    ```

    3. Implement `_open_local_browse`:
    ```python
    def _open_local_browse(self, sys_id: str, full_header_or_uid: str):
        """D-27 — LOCAL hits use Browse panel text-only mode (no image pane).
        Reuses the existing 'no image' branch of the Browse panel via the
        _set_browse_image_pane_visible(False) helper (I15)."""
        # Look up the filepath from local_files for the Open File button (D-28).
        filepath = self._lookup_local_filepath(sys_id)
        # browse_map[sys_id] entries already conform to D-34 shape:
        # {'p_num', 'uid', 'full_header', 'ie_id', 'seq_index'} per Plan 03 indexer
        self._switch_to_browse_panel(sys_id)
        self._set_browse_image_pane_visible(False)  # I15 — uses btn_b_toggle_img + browse_viewer
        self._populate_browse_open_file_button(filepath)
    ```

    4. Add an `Open file` button to the Browse panel toolbar. The button is only ENABLED when the current sys_id is LOCAL:
    ```python
    def _populate_browse_open_file_button(self, filepath: str | None):
        if not hasattr(self, 'browse_open_file_btn'):
            self.browse_open_file_btn = QPushButton(self.tr("Open file"))
            self.browse_open_file_btn.clicked.connect(self._on_browse_open_file_clicked)
            # Add next to btn_b_toggle_img on the browse toolbar
            self.browse_open_file_btn.setVisible(False)
            # Locate the layout container holding btn_b_toggle_img (the parent of that button)
            # and add the new button there. Pseudocode:
            parent_layout = self.btn_b_toggle_img.parent().layout()
            if parent_layout is not None:
                parent_layout.addWidget(self.browse_open_file_btn)
        self._current_local_filepath = filepath
        self.browse_open_file_btn.setVisible(bool(filepath))
        self.browse_open_file_btn.setEnabled(bool(filepath))

    def _on_browse_open_file_clicked(self):
        if self._current_local_filepath and os.path.exists(self._current_local_filepath):
            os.startfile(self._current_local_filepath)  # Windows-native (D-28)
    ```

    5. Implement `_lookup_local_filepath(sys_id)` — read from `LocalIndexer.list_files()` or directly query `local_files` SQLite table via the indexer (`get_filepath` helper):
    ```python
    def _lookup_local_filepath(self, sys_id: str) -> Optional[str]:
        indexer = getattr(self.my_library_tab, '_indexer', None) if hasattr(self, 'my_library_tab') else None
        if indexer is None:
            return None
        return indexer.get_filepath(sys_id)  # helper added to LocalIndexer in Plan 03
    ```

    Add `LocalIndexer.get_filepath(sys_id)` to `shared/local_indexer.py` if not yet present (this is also referenced in Plan 03's task list — verify and add here if not):
    ```python
    def get_filepath(self, sys_id: str) -> Optional[str]:
        row = self._sqlite_conn.execute(
            "SELECT filepath FROM local_files WHERE sys_id = ?", (sys_id,)
        ).fetchone()
        return row[0] if row else None
    ```

    6. When the user navigates AWAY from a LOCAL hit (back to a Genizah hit), restore the image pane visibility based on the user's previous preference. The simplest contract: at click time, call `self._set_browse_image_pane_visible(True)` for Genizah hits unless the user had explicitly toggled it off. The executor decides exactly how to track that preference (acceptable: always restore to True on Genizah click; the user can re-toggle).

    7. **Browse-tab Behavior (D-29):** LOCAL manuscripts do NOT appear in the existing Browse tab's primary listing. Genizah Browse stays Genizah-only. This is a deliberate scope decision — backlog item. Verify no code change needed here; the existing Browse tab queries the main Tantivy/libraries.csv only, which excludes LOCAL by design.
  </action>
  <verify>
    <automated>python -c "import re; src=open('genizah_app.py',encoding='utf-8').read(); assert '_open_local_browse' in src; assert 'os.startfile' in src; assert 'browse_open_file_btn' in src or 'Open file' in src; assert '_set_browse_image_pane_visible' in src; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "_open_local_browse" genizah_app.py` returns ≥ 1.
    - `grep -c "_set_browse_image_pane_visible" genizah_app.py` returns ≥ 2 (definition + use).
    - `grep -c "os.startfile" genizah_app.py` returns ≥ 1.
    - `grep -c "browse_open_file_btn\\|Open file" genizah_app.py` returns ≥ 1.
    - `grep -c "is_local_sys_id" genizah_app.py` returns ≥ 1.
    - REGRESSION: `python -m pytest tests/ -q` exits 0 (no Browse-panel test regressions).
    - `python -m ruff check genizah_app.py` exits 0.
  </acceptance_criteria>
  <done>LOCAL clicks open text-only browse via the I15-resolved helper; Open file button visible + functional via os.startfile.</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task 4: Manual smoke test — badge color, filter cycling, browse + Open file</name>
  <what-built>
    - COL_SRC LOCAL badge in blue (D-11).
    - New comp_col_src column in comp_tree per D-12 audit resolution.
    - Three-state LOCAL filter button on Search / Composition Search / Parallels (REQ-6 + D-10 + D-39).
    - D-10 P1 no-op + inline chip when no LOCAL hits.
    - LOCAL hit click → Browse panel text-only + Open file button (D-27 + D-28).
  </what-built>
  <how-to-verify>
    Pre-requisite: Plan 07 manual smoke complete; MyLibraryTab has indexed some files.

    Launch the desktop app: `python genizah_app.py`.

    **A) COL_SRC badge — main search table (D-11):**
    1. Run a search that returns BOTH Genizah hits AND LOCAL hits.
    2. The Src column should be visible (extended visibility rule per D-11).
    3. LOCAL row's Src cell shows "LOCAL" in BLUE (`#3498db`). Genizah rows show their normal source value uncolored.

    **A2) comp_col_src — composition tree (D-12 audit):**
    3a. Send a chunk of text to Composition Search that includes content present in BOTH Genizah corpus AND a LOCAL file.
    3b. Run the composition analysis.
    3c. The Composition tree should now show a 9th column "Src" at the right end (after Printed).
    3d. LOCAL rows in the comp_tree show "LOCAL" in blue in the new Src column.

    **B) Filter button (REQ-6 + D-10):**
    4. Verify the LOCAL filter button is VISIBLE on the search result toolbar (because LOCAL hits exist).
    5. Click the filter button. Label cycles: `Filter Local` → `Only Local` → `No Local` → `Filter Local`.
    6. In `Only Local` state: only LOCAL rows visible.
    7. In `No Local` state: only Genizah rows visible.
    8. In `Filter Local` (all) state: both visible.
    9. Repeat on Composition Search + Parallels surfaces — same cycling.

    **C) Filter persistence (D-39):**
    10. Set the filter to "Only Local" on Search. Close the app. Reopen.
    11. Run the same search. The filter button should still read "Only Local" (state restored from QSettings).
    12. The three surfaces have INDEPENDENT state — verify Composition / Parallels did not inherit Search's state unless you set them too.

    **D) D-10 P1 no-op (CRITICAL):**
    13. With filter state "Only Local", run a search that returns ZERO LOCAL hits.
    14. **Critical:** Filter should render as NO-OP (Genizah results SHOWN, not hidden).
    15. An inline chip should appear with text: "My Library filter inactive — no LOCAL hits in this query" (EN) or Hebrew equivalent.
    16. Run a different search that DOES return LOCAL hits — the filter button reactivates, chip disappears, "Only Local" is applied normally.

    **E) Browse + Open file (D-27 + D-28):**
    17. Double-click a LOCAL search result row.
    18. Browse panel opens. Prev/Next page navigation works within the file.
    19. NO image pane is shown (text-only mode per D-27 — `browse_viewer.setVisible(False)` and `btn_b_toggle_img.setChecked(False)`).
    20. An "Open file" button is visible on the Browse toolbar.
    21. Click "Open file" — the OS default app (Word / Acrobat / Notepad) launches with the file open.
    22. Click on a Genizah hit afterward — the image pane should reappear (toggle restored).
  </how-to-verify>
  <resume-signal>Reply "approved" if all 5 sections pass. Describe any failures so the executor can patch.</resume-signal>
  <acceptance_criteria>
    - All 5 sections pass.
    - User confirms with "approved".
  </acceptance_criteria>
  <done>Human smoke approved.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Result-row click → `os.startfile(filepath)` | OS shell integration; filepath sourced from indexer's `local_files.filepath` (already canonical) |
| QSettings filter state → cross-session restoration | Per-Windows-user state; no cross-user leak |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-95-30 | Elevation of privilege | `os.startfile()` invokes OS default app for arbitrary extensions | accept | We only call on indexed files of types {.docx, .pdf, .txt} — already filtered by indexer; user controls which apps are registered to handle those extensions |
| T-95-31 | Information disclosure | Inline chip text "no LOCAL hits in this query" leaks state to bystanders | accept | Same as MyLibraryTab T-95-29 — personal-machine context |
| T-95-32 | Tampering | Future contributor removes `_apply_local_filter` call from a cascade callsite | mitigate | Static AST guard `tests/test_local_filter_cascade.py` (mirror of `tests/test_pgp_filter_cascade.py`) enforces the cascade on `_apply_results_table_filters` AND `_apply_comp_tree_filters` (W4 — desktop has no `_apply_pgp_filter`) — CI fails on regression |
| T-95-33 | Repudiation | User filter state silently lost on QSettings corruption | accept | `self._settings.value(..., "all", type=str)` defaults to "all" on missing/corrupt; user re-clicks once |
</threat_model>

<verification>
- `python -m pytest tests/test_local_filter_cascade.py tests/test_local_filter_persistence.py -x -q` exits 0.
- `python -m pytest tests/ -q` exits 0 (no regressions).
- `python -m ruff check genizah_app.py tests/test_local_filter_cascade.py tests/test_local_filter_persistence.py` exits 0.
- Manual smoke (Task 4) approved.
- AST cascade scanner: `tests/test_local_filter_cascade.py::test_local_filter_applied_within_results_cascade` passes — both `_apply_results_table_filters` and `_apply_comp_tree_filters` invoke `_apply_local_filter`.
</verification>

<success_criteria>
- COL_SRC LOCAL badge renders blue `#3498db`; visibility extended to include LOCAL presence.
- New `comp_col_src = 8` column added to comp_tree per D-12 audit; header label includes "Src" as 9th column.
- Three-state LOCAL filter button on all 3 result surfaces (Search / Composition / Parallels).
- Per-surface QSettings persistence working (D-39).
- D-10 P1 no-op + inline chip when zero LOCAL hits.
- Cascade discipline pinned via static AST test scanning `_apply_results_table_filters` AND `_apply_comp_tree_filters` (W4 resolved — desktop has no `_apply_pgp_filter`).
- LOCAL hit click → Browse panel text-only via `_set_browse_image_pane_visible(False)` wrapping the existing `browse_viewer.setVisible` + `btn_b_toggle_img` pattern (I15 resolved) + Open file button.
- 2 Wave-0 stub files green.
- Manual smoke approved.
- No regressions in existing PGP filter / Browse panel tests.
</success_criteria>

<output>
After completion, create `.planning/phases/95-my-library/95-08-SUMMARY.md` documenting:
- Confirmation that the new comp_col_src column was added at index 8 (D-12 audit resolution)
- The EXACT function name discovered for the composition cascade joinpoint (`_apply_comp_tree_filters` or as discovered)
- Confirmation that the cascade scanner targets `_apply_results_table_filters` AND the composition cascade fn (W4 resolution)
- Whether the "no image" Browse branch was reused via `_set_browse_image_pane_visible` wrapper (I15 resolution)
- Manual smoke verdict
</output>
