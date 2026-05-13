---
phase: 88-state-separation-by-deletion
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - web/pages/search.py
  - web/pages/search_results.py
  - web/pages/parallels.py
autonomous: true
requirements: [STATE-02]
must_haves:
  truths:
    - "Every `state.X = value` line targeting one of the 10 deleted AppState fields is replaced by a local variable assignment in each writer site across search.py / search_results.py / parallels.py."
    - "Every existing `set_search_export(...) / set_parallels_export(...) / update_*(...) / clear_*(...)` call continues to receive identical values (sourced from locals instead of state.X)."
    - "AppState fields still exist on the class after this plan — they are now write-orphaned (no writer references them) but readers (none remain; api.py already migrated to per-session payload in v7.11.1) do not break."
    - "All existing tests pass byte-unchanged. Test fixtures that do `state.last_results = [...]` etc. continue to work because the AppState class still owns those attributes."
    - "Every `set_parallels_export(` call in `web/pages/parallels.py` after this plan is either (a) a clear-export path with empty results, (b) a positive export path with `source_text` in meta per D-13, or (c) carries an inline comment justifying intentional empty source_text — no unannotated `meta=None` with non-empty results ships (Refinement 3 per cross-AI review)."
    - "Plan-boundary green: `pytest` and `ruff check` and `python scripts/check_docs.py` all exit 0."
  artifacts:
    - path: "web/pages/search.py"
      provides: "Writer sites at 2067-2076, 2101-2106, 3801-3820, 4112-4140, 4197-4231 migrated to locals"
      contains: "set_search_export("
    - path: "web/pages/search_results.py"
      provides: "Writer sites at 126, 377-380 migrated to locals"
      contains: "update_search_export_results("
    - path: "web/pages/parallels.py"
      provides: "Writer sites at 281-302, 1981-2002, 2300-2338 migrated to locals; source_text fold-in audited per Refinement 3"
      contains: "set_parallels_export("
  key_links:
    - from: "web/pages/search.py writer sites"
      to: "web/export_state.set_search_export"
      via: "kwargs sourced from local variables (was state.* attribute reads)"
      pattern: "set_search_export\\("
    - from: "web/pages/parallels.py writer sites"
      to: "web/export_state.set_parallels_export"
      via: "kwargs sourced from local variables (was state.* attribute reads)"
      pattern: "set_parallels_export\\("
---

<objective>
Migrate every `state.X = value` writer site for the 10 deleted-in-Plan-88-03 AppState fields to local variables, threading those locals through the existing `web.export_state` setter/updater/clearer calls without changing the export_state ABI or AppState class shape.

Purpose: Eliminate the cross-user data-leak vector at its source. The 10 singleton fields on `AppState` are written by these 13 writer sites; once writers stop touching them, the singleton mirrors are dead code that Plan 88-03 can safely delete. Doing the migration in this order — locals first, fields later — avoids the data-loss window Codex caught (CONTEXT.md "Why Codex caught the original plan ordering"): `set_search_export(...)` calls at `search.py:4112-4140` pass `state.current_search_gap` / `state.last_filters_applied` / `state.last_search_warnings` as keyword arguments two lines BELOW their `state.X = value` assignments. Deleting the assignments first would feed default/stale values into the export call.

Output: 3 modified source files. AppState fields remain physically present on the class but are now write-orphaned. No test files modified. Full pytest + ruff + check_docs green.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@./CLAUDE.md
@.planning/STATE.md
@.planning/ROADMAP.md
@.planning/phases/88-state-separation-by-deletion/88-CONTEXT.md
@web/state.py
@web/export_state.py
@web/safe_storage.py

<interfaces>
<!-- web/export_state.py setter/updater/clearer signatures (current, MUST NOT change in this plan) -->
<!-- These functions ALREADY exist and ALREADY route through app.storage.user via _backend(). -->
<!-- This plan only changes the values being passed in by the writers; the ABI is unchanged. -->

```python
# web/export_state.py — current signatures (CONFIRMED at lines 55-77, 92-105, 108-121, 124-129, 136-149, 160-169, 172-177)

def set_search_export(
    results: List[Dict[str, Any]],
    query: str,
    mode: str = 'text',
    gap: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    warnings: Optional[List[str]] = None,
    selected_uids: Optional[List[str]] = None,
) -> None: ...

def update_search_export_results(results: List[Dict[str, Any]]) -> None: ...
def update_search_export_selection(selected_uids: Optional[List[str]]) -> None: ...
def clear_search_export() -> None: ...

def set_parallels_export(
    results: List[Dict[str, Any]],
    filtered: List[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
) -> None: ...

def update_parallels_export_filtered(filtered: List[Dict[str, Any]]) -> None: ...
def clear_parallels_export() -> None: ...
```

<!-- The 10 AppState fields being orphaned (web/state.py lines 26-50, NOT deleted in this plan) -->
last_results, current_search_query, current_search_mode, current_search_gap,
last_filters_applied, last_search_warnings, last_selected_uids,
parallels_results, parallels_filtered, parallels_search_meta
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Migrate writer sites in web/pages/search.py (5 sites)</name>
  <files>web/pages/search.py</files>
  <read_first>
    - web/pages/search.py (full file — at minimum read lines 2055-2110, 3790-3830, 4095-4240 to understand local scope at each writer site)
    - web/export_state.py (function signatures — already in plan context)
    - web/state.py (AppState field shape — already in plan context)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-04, D-05 ordering rationale)
  </read_first>
  <action>
**Tooling note (per Codex review, Refinement 7):** Acceptance-criterion shell
commands below assume Bash + GNU grep are available via the `Bash` tool in
the execute-phase runtime. If running on a pure-PowerShell shell, the
equivalent commands are:
  - `grep -nE "pat" file` → `rg -nN "pat" file`
  - `grep -c "pat" file`  → `(rg -c "pat" file)` (rg returns count)
  - `grep -rn "pat" dir`  → `rg -n "pat" dir`
  - `test -f path && echo OK` → `python -c "import os; assert os.path.isfile('path'); print('OK')"`
The executor agent SHOULD run the Bash-style commands first via the Bash
tool; fallback to PowerShell equivalents only if the Bash invocation fails
or is unavailable.

Migrate 5 writer site clusters in `web/pages/search.py`. At each site, replace `state.X = value` lines with local-variable assignments and thread those locals as keyword arguments into the existing `set_search_export(...)` / `update_search_export_selection(...)` / `clear_search_export()` calls.

**Naming convention (per CONTEXT.md "Claude's Discretion"):** Match the kwarg name on the export_state call site. Use `_results`, `_query`, `_mode`, `_gap`, `_filters_applied`, `_warnings`, `_selected_uids` as the literal local names — leading underscore signals "scratch variable for export payload, no consumer beyond this scope." Per D-13/D-14 these locals do NOT need to outlive the immediate `set_search_export(...)` call.

**Site 1: `_reset_search` block at lines 2067-2079** (search reset path).
- BEFORE: 7 lines `state.last_results = []` / `state.current_search_query = ''` / `state.current_search_mode = 'exact'` / `state.current_search_gap = None` / `state.last_filters_applied = None` / `state.last_search_warnings = []` / `state.last_selected_uids = None`, followed by `clear_search_export()` call.
- AFTER: Delete the 7 `state.X = ...` lines entirely. The `clear_search_export()` call at line 2079 takes no arguments and already does the right thing for per-session storage. Keep the `from web.export_state import clear_search_export` and the call. Keep the comment block but trim it to reflect that the per-session export is now the only state being cleared (remove the Phase 77 gap-closure preamble; replace with a one-line comment: `# Clear per-session export payload — Phase 88 deleted the singleton mirror.`).

**Site 2: `toggle_select_all` block at lines 2098-2106** (bulk select-all).
- BEFORE: `state.last_selected_uids = compute_selected_uids(search_state)` followed by `update_search_export_selection(state.last_selected_uids)`.
- AFTER: `_selected_uids = compute_selected_uids(search_state)` followed by `update_search_export_selection(_selected_uids)`. Delete the `state.last_selected_uids = ...` assignment. Trim the Phase 77 gap-closure comment block to: `# Mirror selection to per-session export payload (Phase 88: singleton mirror removed).`

**Site 3: history-restore block at lines 3801-3822** (`state.last_results = state_snapshot[...]` through `set_search_export(...)` call).
- BEFORE: 6 lines assigning to `state.last_results`, `state.current_search_query`, `state.current_search_mode`, `state.current_search_gap`, `state.last_filters_applied`, `state.last_search_warnings`, then `set_search_export(results=state_snapshot['results'], query=entry.get('query', '') or '', mode=state.current_search_mode, gap=state.current_search_gap, filters=state.last_filters_applied, warnings=state.last_search_warnings, selected_uids=None)`.
- AFTER: Replace the 6 `state.X =` lines with 6 local-variable assignments using the same right-hand-side expressions (incl. the try/except on `int(params['gap'])`). Then the `set_search_export(...)` call uses `mode=_mode`, `gap=_gap`, `filters=_filters_applied`, `warnings=_warnings`. Use literal names `_results`, `_query`, `_mode`, `_gap`, `_filters_applied`, `_warnings`.
- KEEP the try/except ValueError/TypeError block for gap parsing — just retarget it from `state.current_search_gap` to local `_gap`.

**Site 4: partial-results cancel path at lines 4112-4143** (`if search_state.is_cancelled:` branch).
- BEFORE: 6 lines assigning to `state.current_search_query`, `state.current_search_mode`, `state.current_search_gap`, `state.last_filters_applied`, `state.last_search_warnings`, `state.last_results` (the gap parse is inside try/except), then `set_search_export(results=results, query=clean_query, mode=mode, gap=state.current_search_gap, filters=state.last_filters_applied, warnings=['partial-results'], selected_uids=None)`.
- AFTER: Replace 6 `state.X =` lines with locals `_current_search_query`, `_current_search_mode`, `_current_search_gap`, `_last_filters_applied`, `_last_search_warnings`, `_last_results`. Update the `set_search_export(...)` call to `gap=_current_search_gap, filters=_last_filters_applied`. Note: `query=clean_query`, `mode=mode`, `results=results`, `warnings=['partial-results']` already use locals/parameters from enclosing scope — leave unchanged.
- KEEP the try/except for gap parsing — retarget to local `_current_search_gap`.

**Site 5: happy-path enrichment at lines 4197-4234** (post-`# Phase 77: populate state for JSON export envelope`).
- BEFORE: 6 lines assigning to `state.current_search_query`, `state.current_search_mode`, `state.current_search_gap` (with try/except), `state.last_filters_applied` (10-key dict), `state.last_search_warnings`, `state.last_results`, then `set_search_export(results=results, query=clean_query, mode=mode, gap=state.current_search_gap, filters=state.last_filters_applied, warnings=[], selected_uids=None)`.
- AFTER: Replace 6 `state.X =` lines with locals `_current_search_query`, `_current_search_mode`, `_current_search_gap`, `_last_filters_applied`, `_last_search_warnings`, `_last_results`. Update the `set_search_export(...)` call to `gap=_current_search_gap, filters=_last_filters_applied`.
- KEEP the try/except for gap parsing — retarget to local `_current_search_gap`.
- The 10-key filters dict construction at lines 4208-4219 is verbatim from Phase 77; it now assigns to `_last_filters_applied = { ... }` instead of `state.last_filters_applied = { ... }`.

**Comment hygiene at each site:** Update inline comments that reference "global state singleton" or "state.last_results" to say "per-session export payload (Phase 88: singleton mirror removed)". Do NOT delete the Phase 77 / 2026-05-12 historical context comments — they remain valid as historical pointers.

**Verification grep targets (use these to confirm post-edit):**
- `grep -nE '^\s*state\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids)\s*=' web/pages/search.py` MUST return 0 matches.
- `grep -nE '(set|update|clear)_search_export\(' web/pages/search.py` MUST return the same number of matches as before the edit (5 sites still wired through export_state).
  </action>
  <verify>
    <automated>cd C:/Genizahsearch && python -m pytest tests/test_export_cross_user_isolation.py tests/test_export_state_selection.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -x --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `grep -nE "^\s*state\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids)\s*=" web/pages/search.py` returns 0 matches.
    - `grep -cE "(set|update|clear)_search_export\(" web/pages/search.py` returns 5 (5 wired call sites preserved).
    - `python -c "import ast; ast.parse(open('web/pages/search.py', encoding='utf-8').read())"` exits 0 (syntax-clean).
    - `python -m pytest tests/test_export_cross_user_isolation.py tests/test_export_state_selection.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -x` exits 0 (tests still pass; they read `state.X` directly in fixtures and that's fine because the fields still exist on AppState).
    - `python -m ruff check web/pages/search.py` exits 0.
  </acceptance_criteria>
  <done>5 writer-site clusters in web/pages/search.py migrated to local variables; AppState fields are now write-orphaned for these sites; export_state calls receive identical values via locals; all existing tests pass.</done>
</task>

<task type="auto">
  <name>Task 2: Migrate writer sites in web/pages/search_results.py (2 sites)</name>
  <files>web/pages/search_results.py</files>
  <read_first>
    - web/pages/search_results.py (lines 115-135 for site 1, lines 365-385 for site 2)
    - web/export_state.py (update_search_export_results, update_search_export_selection signatures already in plan context)
  </read_first>
  <action>
Migrate 2 writer sites in `web/pages/search_results.py`.

**Site 1: post-display-filter sync at lines 125-129.**
- BEFORE: `state.last_results = results  # Keep export in sync with displayed (post-filter) results`, then `update_search_export_results(results)`.
- AFTER: Delete the `state.last_results = results` line entirely. Keep `update_search_export_results(results)`. The `results` local already holds the right value — no new local needed.
- Trim the comment to: `# Sync per-session export payload with displayed (post-filter) results (Phase 88: singleton mirror removed).`

**Site 2: per-row selection toggle at lines 372-380.**
- BEFORE: `state.last_selected_uids = compute_selected_uids(search_state)` followed by `update_search_export_selection(state.last_selected_uids)`.
- AFTER: `_selected_uids = compute_selected_uids(search_state)` followed by `update_search_export_selection(_selected_uids)`. Delete the `state.last_selected_uids = ...` line.
- Trim the comment block to: `# Mirror selection to per-session export payload (Phase 88: singleton mirror removed).`

**Imports:** If `state` is imported but no longer used after this edit (because the only references to it were the 2 writer-site assignments), do NOT delete the import — `state` may still be used elsewhere in the file (e.g., `state.meta_mgr`, `state.searcher`). Use grep to confirm before deciding. Almost certainly the import stays.

**Verification grep targets:**
- `grep -nE '^\s*state\.(last_results|last_selected_uids)\s*=' web/pages/search_results.py` MUST return 0 matches.
- `grep -nE 'update_search_export_(results|selection)\(' web/pages/search_results.py` MUST return 2 matches.
  </action>
  <verify>
    <automated>cd C:/Genizahsearch && python -m pytest tests/test_export_cross_user_isolation.py tests/test_export_state_selection.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -x --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `grep -nE "^\s*state\.(last_results|last_selected_uids)\s*=" web/pages/search_results.py` returns 0 matches.
    - `grep -cE "update_search_export_(results|selection)\(" web/pages/search_results.py` returns 2.
    - `python -c "import ast; ast.parse(open('web/pages/search_results.py', encoding='utf-8').read())"` exits 0.
    - `python -m pytest tests/test_export_cross_user_isolation.py tests/test_export_state_selection.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -x` exits 0.
    - `python -m ruff check web/pages/search_results.py` exits 0.
  </acceptance_criteria>
  <done>2 writer sites in search_results.py migrated to locals; AppState fields write-orphaned for these sites; update_search_export_* calls still functional.</done>
</task>

<task type="auto">
  <name>Task 3: Migrate writer sites in web/pages/parallels.py (3 sites + source_text fold-in + audit per Refinement 3)</name>
  <files>web/pages/parallels.py</files>
  <read_first>
    - web/pages/parallels.py (lines 275-310 for site 1, lines 450-470 for source_text writer at line 457, lines 1970-2010 for site 2, lines 2017-2065 for site 3 + source_text resets at 2049-2061, lines 2295-2350 for site 4 + source_text persistence at 2341-2344)
    - web/export_state.py (set_parallels_export signature; meta parameter shape)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-13: source_text fold-in)
  </read_first>
  <action>
Migrate 3 writer-site clusters in `web/pages/parallels.py` AND fold `parallels_source_text` into the `meta` dict per D-13. Audit every remaining `set_parallels_export(` call per Refinement 3 (cross-AI review).

**Naming convention:** Use locals `_results`, `_filtered`, `_meta`, `_parallels_search_meta`, `_parallels_results`, `_parallels_filtered` (match the kwarg names on `set_parallels_export(results=, filtered=, meta=)`).

**Site 1: bootstrap snapshot-restore at lines 281-289** (`if _active_snapshot:` branch).
- BEFORE: `state.parallels_results = p_state.results` and `state.parallels_filtered = p_state.filtered_results`, then `set_parallels_export(results=p_state.results, filtered=p_state.filtered_results, meta=None)`.
- AFTER: Delete both `state.X =` assignment lines. The `set_parallels_export(...)` call already uses `p_state.results` and `p_state.filtered_results` directly — no new locals needed; just remove the singleton mirror.
- Trim comments: change "2026-05-12 cross-user fix: mirror to per-session export payload" to "Phase 88: per-session export payload is the sole writer path (singleton mirror removed)".
- **Per Refinement 3 (cross-AI review):** This site passes `meta=None` even when `p_state.results` may be non-empty. Determine if the snapshot object (`p_state` — actual class `ParallelsState`) carries a `source_text` attribute (read `web/pages/parallels.py` for the `ParallelsState` class definition; check the snapshot-restore mechanism for a `source_text` field). 
  - **If `p_state.source_text` exists:** thread it through by changing `meta=None` to `meta={'source_text': getattr(p_state, 'source_text', None) or ''}` — this is the cleanest path, populating meta with the snapshot's source_text.
  - **If `p_state.source_text` does NOT exist:** keep `meta=None` AND add an inline comment above the call: `# Phase 88 D-13: snapshot-restore path — source_text intentionally empty (ParallelsState carries no source_text field; legacy bootstrap path Site 1b handles fallback for non-snapshot reloads).`
  - This explicit classification is mandatory per Refinement 3; do not leave the call unannotated.

**Site 1b: bootstrap legacy-storage fallback at lines 293-308** (`else:` branch — reads `_safe_get('parallels_results')`).
- BEFORE: `state.parallels_results = p_state.results` and `state.parallels_filtered = _legacy_filtered`, then `set_parallels_export(results=p_state.results, filtered=_legacy_filtered, meta=None)`.
- AFTER: Delete both `state.X =` assignment lines. Keep the `set_parallels_export(...)` call. **Additionally per D-13:** if the legacy storage has a `parallels_source_text`, fold it into the meta dict. Concretely: change `meta=None` to `meta={'source_text': _safe_get('parallels_source_text', '') or ''}` ONLY if a non-empty source_text exists, otherwise keep `meta=None`. Implementation:
  ```python
  _legacy_source_text = _safe_get('parallels_source_text', '') or ''
  _bootstrap_meta = {'source_text': _legacy_source_text} if _legacy_source_text else None
  set_parallels_export(
      results=p_state.results,
      filtered=_legacy_filtered,
      meta=_bootstrap_meta,
  )
  ```

**Site 2: history-restore at lines 1981-2003** (composition history reload).
- BEFORE: 2 lines `state.parallels_results = p_state.results` and `state.parallels_filtered = p_state.filtered_results`, then a `state.parallels_search_meta = { ... 7-key dict ... }` block at lines 1988-1996, then `set_parallels_export(results=p_state.results, filtered=p_state.filtered_results, meta=state.parallels_search_meta)`.
- AFTER: Delete the 2 `state.parallels_results = ...` and `state.parallels_filtered = ...` lines. Rename `state.parallels_search_meta = { ... }` to `_parallels_search_meta = { ... }`. Update the `set_parallels_export(...)` call to `meta=_parallels_search_meta`. Keep the 7-key dict structure (source_text, chunk_size, mode, max_freq, filters, boundary_options, warnings) verbatim — D-13 source_text fold-in is already happening here since the dict already has a `source_text` key from `state_snapshot.get('source_text', '')`.

**Site 3: `_reset_parallels` at lines 2017-2062** (compositions reset path).
- BEFORE: At lines 2049-2053 there are 5 `safe_user_set(...)` calls including `safe_user_set('parallels_source_text', '')` at line 2051. At lines 2056-2058 there are 3 `state.X = ...` assignments (`state.parallels_results = []`, `state.parallels_filtered = []`, `state.parallels_search_meta = None`).
- AFTER: Delete the 3 `state.X = ...` assignments at lines 2056-2058. **Per D-14:** the `safe_user_set('parallels_source_text', '')` line at 2051 — keep it for now (writer side). The READER-side fallback in `web/api.py` reading `safe_user_get('parallels_source_text', '')` is deleted in Plan 88-02 (D-14). For consistency with the fold-in, the writer here at 2051 will become dead code after Plan 88-02 lands, but we leave the cleanup of writer-side `safe_user_set('parallels_source_text', ...)` to Plan 88-02 (it goes together with the reader-side fallback removal).
- Note: lines 2049 (`safe_user_set('parallels_results', [])`) and 2050 (`safe_user_set('parallels_filtered', [])`) are SEPARATE from the export_state path — they write to the legacy storage keys for UI persistence across page reloads (see lines 2341-2344 for the writes that load these). Do NOT touch them in this plan; they are not export_state writes.
- Keep the `clear_parallels_export()` call at 2061. Trim comment to `# Clear per-session export payload (Phase 88: singleton mirror removed).`
- **Per Refinement 3:** `clear_parallels_export()` is bucket (a) (clear-export path with empty results) — no annotation needed since it does not call `set_parallels_export(..., meta=None)`. However if the audit reveals that the cluster ALSO calls `set_parallels_export(results=[], filtered=[], meta=None)` somewhere (it shouldn't — `clear_parallels_export()` does the dict pop), classify as (a) explicitly.

**Site 4: search-completion at lines 2300-2339** (`execute_parallels` completion block).
- BEFORE: 2 lines `state.parallels_results = main_results` and `state.parallels_filtered = filtered_results` at 2300-2301, then `state.parallels_search_meta = { ... 7-key dict ... }` at lines 2323-2331, then `set_parallels_export(results=main_results, filtered=filtered_results, meta=state.parallels_search_meta)` at lines 2335-2338.
- AFTER: Delete the 2 `state.parallels_results = ...` and `state.parallels_filtered = ...` lines at 2300-2301. Rename `state.parallels_search_meta = { ... }` to `_parallels_search_meta = { ... }`. Update `set_parallels_export(...)` call to `meta=_parallels_search_meta`. The 7-key dict structure is preserved — `source_text` field already populated from `text_input.value or ''`.
- The `_parallels_filters` local at lines 2311-2322 already exists and is used as `'filters': _parallels_filters` inside the 7-key dict — no change.

**Source-text writer at line 457 (`update_word_count`):**
- BEFORE: `safe_user_set('parallels_source_text', text)` is called every time the textarea changes (for persistence across page reloads).
- AFTER: Keep this call AS-IS in Plan 88-01. Per D-14, the READER-side fallback in api.py is deleted in Plan 88-02. The writer at line 457 stops being load-bearing after Plan 88-02 (because the export path reads from meta['source_text'], not from safe_user_get). For Plan 88-01 we are NOT touching this writer — it stays.

**Source-text persistence at lines 2341-2344:**
- BEFORE: After the export_state set, there are `safe_user_set('parallels_results', _compact_result_rows(...))` and `safe_user_set('parallels_filtered', _compact_result_rows(...))` calls for legacy page-reload persistence.
- AFTER: NOT TOUCHED in Plan 88-01. These are not export_state writes; they are UI persistence. They are handled in Plan 88-02 only if they collide with source_text fold-in — they don't, so leave alone.

**Post-edit audit (per Codex review, Refinement 3):** Walk every `set_parallels_export(` call in the edited file. For each, classify into (a) clear path with empty results, (b) positive export with source_text in meta, or (c) positive export with intentionally empty source_text. Every call must fall into one of these three buckets. The Site 1 snapshot-restore path (`if _active_snapshot:` branch) is a known (c) candidate — it passes `meta=None` even when `p_state.results` may be non-empty. If `p_state.results` can be non-empty at that path, fold in the snapshot's source_text if available from `p_state` (e.g., via a `_snapshot_meta = {'source_text': getattr(p_state, 'source_text', None) or ''}` local) OR add an inline comment `# Phase 88 D-13: snapshot path — source_text intentionally empty (no snapshot.source_text field)` justifying the omission.

After applying the audit, the Sites 1, 1b, 2, 4 calls produce non-empty meta or carry the bucket-(c) annotation; Site 3 calls only `clear_parallels_export()` which is bucket (a) — does not call `set_parallels_export(...)`.

**Verification grep targets:**
- `grep -nE '^\s*state\.(parallels_results|parallels_filtered|parallels_search_meta)\s*=' web/pages/parallels.py` MUST return 0 matches.
- `grep -nE 'set_parallels_export\(' web/pages/parallels.py` MUST return at least 4 matches (4 writer-site clusters wired).
  </action>
  <verify>
    <automated>cd C:/Genizahsearch && python -m pytest tests/test_export_cross_user_isolation.py tests/test_export_state_selection.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -x --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `grep -nE "^\s*state\.(parallels_results|parallels_filtered|parallels_search_meta)\s*=" web/pages/parallels.py` returns 0 matches.
    - `grep -cE "set_parallels_export\(" web/pages/parallels.py` returns at least 4 (4 wired call sites preserved).
    - `python -c "import ast; ast.parse(open('web/pages/parallels.py', encoding='utf-8').read())"` exits 0.
    - `python -m pytest tests/test_export_cross_user_isolation.py tests/test_export_state_selection.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -x` exits 0.
    - `python -m ruff check web/pages/parallels.py` exits 0.
    - Every `set_parallels_export(` call in `web/pages/parallels.py` after this task is one of:
        (a) `meta=None` AND `results=[]` AND `filtered=[]` (the legitimate clear-export path; OR called from `_reset_parallels` cluster); OR
        (b) `meta=<dict>` that contains a `'source_text'` key (positive export path with source_text fold-in per D-13); OR
        (c) `meta=None` with non-empty results AND has an inline comment `# Phase 88 D-13: source_text empty/unknown for this path because <reason>` justifying the omission.
      No `set_parallels_export(...)` call with non-empty results ships an unannotated `meta=None`.
    - `python -c "import re; src=open('web/pages/parallels.py', encoding='utf-8').read(); calls=re.findall(r'set_parallels_export\s*\([^)]+\)', src, re.DOTALL); print(len(calls), 'set_parallels_export calls'); [print(repr(c[:200])) for c in calls]"` — manual visual audit; every call printed must match (a), (b), or (c) above.
  </acceptance_criteria>
  <done>3 writer-site clusters in parallels.py migrated to locals; AppState parallels_* fields write-orphaned across all writer sites; set_parallels_export still receives all values via locals; D-13 source_text bootstrap fold-in landed for the legacy bootstrap path; every set_parallels_export call audited per Refinement 3 and classified as (a), (b), or (c).</done>
</task>

<task type="auto">
  <name>Task 4: Plan-boundary green verification (pytest + ruff + check_docs)</name>
  <files></files>
  <read_first>
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-05: plan boundaries MUST stay green)
  </read_first>
  <action>
Run the full test/lint/docs verification trio. Fix any regressions surfaced before the plan can be considered complete.

Concrete commands (each must exit 0):
1. `python -m pytest` (full suite — 1879 tests target; Plan 88-01 must leave count identical to Phase 87 close at 1879 passed / 20 skipped, give or take).
2. `python -m ruff check .` (no new lint violations in the 3 modified files).
3. `python scripts/check_docs.py` (docs health check; should be unchanged from Phase 87 close — Plan 88-01 doesn't modify any docs).

If pytest fails on a test outside the 4 export-specific tests, investigate — the migration MAY have broken a test that touches `state.X` reads (although there should be none since api.py was already migrated in v7.11.1). If any failure is surfaced, the most likely cause is a missed `state.X =` line that should have been migrated, OR a kwarg name mismatch on the export_state call. Use the verification greps in Tasks 1-3 to locate.

**Specifically MUST verify (scoped to web/ per Refinement 1 — cross-AI review):**
- `rg -n "^\s*state\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\s*=" web` returns 0 matches across ALL `web/` (not just the 3 modified files — sanity check there are no other writer sites we missed). Equivalent Bash form: `grep -rnE "^\s*state\.(<10 fields>)\s*=" web/`. Both are scoped to `web/` to avoid false-positives in `.planning/`, `_tmp/`, or CLAUDE.md historical context.
  </action>
  <verify>
    <automated>cd C:/Genizahsearch && python -m pytest -q 2>&1 | tail -5 && python -m ruff check . 2>&1 | tail -3 && python scripts/check_docs.py 2>&1 | tail -3</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest -q` exits 0 with at least 1879 tests passing (Phase 87 close baseline).
    - `python -m ruff check .` exits 0.
    - `python scripts/check_docs.py` exits 0.
    - `rg -n "^\s*state\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\s*=" web` returns 0 matches (no writer site missed; scoped to web/ per Refinement 1).
  </acceptance_criteria>
  <done>Plan 88-01 leaves the tree green: pytest at Phase 87 baseline, ruff clean, check_docs clean. AppState fields physically still exist on the class but have zero writers in the web/ tree.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Writer site → local variable → export_state call | Internal — Python lexical scope; risk is that a `state.X = value` line is missed and continues to feed a stale singleton mirror after the AppState fields are physically deleted in Plan 88-03. |
| AppState class (singleton) → 13 writer sites | Process-wide; risk is that a NEW writer site is added between Plan 88-01 and Plan 88-03 (e.g., another developer's PR) writing to one of the 10 fields, and the migration misses it. Static guard in Plan 88-03 D-07 catches this at CI time. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-88-01-01 | Information Disclosure | Writer migration (search.py site 4 or 5) | mitigate | If a `state.current_search_gap = ...` line is missed at lines 4117-4117 or 4202-4202 (inside try/except), the assignment continues to write to the singleton. Plan 88-03's static AST guard (D-07) catches the residual access. Plan 88-01 mitigates with grep verification in Task 4: `rg -n "^\s*state\.(<10 fields>)\s*=" web` must return 0 matches. |
| T-88-01-02 | Tampering | export_state.set_search_export kwarg ordering | mitigate | If a local variable rename (e.g., `_gap` vs `_current_search_gap`) is inconsistent between the assignment site and the `set_search_export(...)` call, Python raises `NameError` immediately. Mitigation: per-site verification via `python -c "import ast; ast.parse(...)"` in each Task's acceptance criteria + pytest test run. No silent failure mode — Python's lexical scoping is the safety net. |
| T-88-01-03 | Denial of Service | Plan-boundary regression | accept | If a pytest test fails because Plan 88-01 missed a writer site, the migration regression is bisectable (3 files, ~13 sites). Acceptance criterion: full pytest in Task 4. Rollback strategy: `git revert HEAD` on the offending commit. Mitigation already adequate via test gate. |

**No HIGH-severity threats.** Plan 88-01 is a mechanical refactor that REDUCES the cross-user data-leak attack surface (singleton mirrors getting overwritten by concurrent users) by stopping the writes. The leak persists if a writer is missed (T-88-01-01), but the static guard in Plan 88-03 will catch it. Plan 88-01 alone does not eliminate the leak — Plan 88-03's field deletion does.

**Defense-in-depth note:** This plan does NOT delete the AppState fields. Until Plan 88-03 lands, the singleton mirrors remain physically present and continue to leak between users IF any code still reads them. The v7.11.1 hotfix already migrated all reader sites (api.py export handlers) to per-session payload, so by inspection there should be no remaining cross-user data leak after Plan 88-01 + Plan 88-02 land — Plan 88-03 then removes the latent attack surface entirely.
</threat_model>

<verification>
1. **All 4 tasks pass acceptance criteria** (greps + pytest + ruff per-task).
2. **Plan-boundary green** (Task 4): full pytest at Phase 87 baseline (~1879 passed), ruff clean, check_docs clean.
3. **No new writer site outside the 3 modified files:** `rg -n "^\s*state\.(<10 fields>)\s*=" web` returns 0 matches across all of `web/` (scoped per Refinement 1).
4. **export_state ABI unchanged:** `grep -cE "def (set|update|clear)_(search|parallels)_export\b" web/export_state.py` returns 7 (7 public functions still defined with identical signatures).
5. **AppState class shape unchanged:** `grep -cE "^\s+self\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\s*[:=]" web/state.py` returns 10 (all 10 fields still declared in `init()`).
</verification>

<success_criteria>
- STATE-02 partially advanced: all writer sites now write through local variables to the existing `web/export_state` setter/updater/clearer calls; the singleton mirror writes are gone. (Full STATE-02 satisfaction comes after Plan 88-02 + 88-03.)
- Phase 87 invariants intact: no new raw `app.storage.user` access introduced (still routed through `_backend()` in export_state.py — Plan 88-02 will route through safe_storage chokepoint).
- Zero user-visible behavior change: the export payload received by `web/api.py` handlers is identical pre- and post-this plan; the AppState fields are write-orphaned but still present (so they read as their last-written value, which was the same value passed to export_state — no semantic change).
</success_criteria>

<output>
After completion, create `.planning/phases/88-state-separation-by-deletion/88-01-writer-migration-SUMMARY.md` per @$HOME/.claude/get-shit-done/templates/summary.md.
</output>
</content>
</invoke>