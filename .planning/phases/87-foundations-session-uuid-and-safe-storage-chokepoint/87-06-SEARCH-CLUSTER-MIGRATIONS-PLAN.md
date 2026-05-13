---
phase: 87
plan: 06
type: execute
wave: 2
depends_on: [87-02]
files_modified:
  - web/pages/parallels.py
  - web/pages/search.py
  - web/pages/search_state.py
autonomous: true
requirements:
  - FOUND-02
tags:
  - phase87
  - migration
  - safe-storage
  - search
  - parallels
  - search-state
  - codex-deferred-sites
must_haves:
  truths:
    - "web/pages/parallels.py raw accesses reduced from 35 to 0 (including the Codex round 4 deferred-callback site at line 3520)"
    - "web/pages/search.py raw accesses reduced from 14 to 0"
    - "web/pages/search_state.py raw accesses reduced from 31 to 0"
    - "All non-FOUND-04 tests pass"
  artifacts:
    - path: "web/pages/parallels.py"
      provides: "All 35 sites migrated including bootstrap reads, history persistence, filter writes, and the deferred-restore callback at 3520"
      contains: "from web.safe_storage import"
    - path: "web/pages/search.py"
      provides: "All 14 sites migrated: query/preset/gap/mode/text_position writes + 2 show_translations reads (4420, 4630)"
      contains: "safe_user_set"
    - path: "web/pages/search_state.py"
      provides: "All 31 sites migrated in persist_search_snapshot, restore_search_snapshot, _reset_filter_storage_keys"
      contains: "safe_user_pop"
  key_links:
    - from: "web/pages/parallels.py:3520 (deferred-restore callback)"
      to: "safe_user_get/set/pop"
      via: "Codex round 4 MEDIUM-2 — deferred async callback safety"
      pattern: "parallels"
    - from: "web/pages/search_state.py persist_search_snapshot"
      to: "safe_user_set for ~6 snapshot keys + safe_user_pop for filter reset"
      via: "wholesale function-body migration"
      pattern: "safe_user_set\\('search_snapshot_schema_version'"
---

<objective>
Migrate the three search-cluster files (`parallels.py`, `search.py`, `search_state.py`) — the largest migration in Phase 87, totaling 80 raw access sites. Includes the explicit Codex round 4 MEDIUM-2 deferred-callback site at `parallels.py:3520`.

Purpose: The search cluster is the most heavily-trafficked area of the web app and the place where prune-mid-flight bugs are most likely to surface (long-running searches >10s can have their storage pruned by the NiceGUI scheduler). The safe_storage migration here is the single largest reduction in 500-error surface from Phase 87.

Output: 3 files migrated; 80 raw access sites converted; `tests/test_safe_storage.py` + `tests/test_session_uuid.py` + (any search-related tests) all green.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md
@web/safe_storage.py
@web/pages/parallels.py
@web/pages/search.py
@web/pages/search_state.py

<interfaces>
<!-- search.py storage keys (per research line 4): -->

```
'search_query'             # str
'search_preset'            # str
'search_gap'               # int
'search_text_position'     # str
'search_max_changes'       # int
'search_mode'              # str
'show_translations'        # bool (read at 4420, 4630)
```

<!-- search_state.py snapshot keys (per research line 4): -->

```
'search_snapshot_schema_version'  # int
'search_results'                  # list[dict] — compacted result rows
'search_printed_filter'           # str
'domain_exclusions'               # list[str]
'search_refinement_chain'         # list[dict]
'search_exclusion_sources'        # list[str]
'session_persistence_enabled'     # bool (read)
```

<!-- parallels.py storage keys (per research and grep): -->

```
'composition_history'             # list[dict] — search history
'session_persistence_enabled'     # bool (read)
'search_history_limit'            # int (read)
'parallels_source_text'           # str (write at 457)
'word_search_excluded_ids'        # list[str] (read at 883)
'parallels_filter_domains'        # list[str] (write at 929)
... plus 30+ more (this file has 36 raw accesses per grep)
```

<!-- The deferred-restore at parallels.py:3520 is inside an `async def _deferred_restore()` callback that runs AFTER the page handler returns. Storage state may be different from when the callback was scheduled. safe_storage helpers handle this gracefully. -->
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Migrate web/pages/parallels.py (35 sites including Codex deferred-callback at 3520)</name>
  <read_first>
    - web/pages/parallels.py — read selectively (file is large, ~3800 lines):
      - Top of file (find imports; check for existing safe_storage imports)
      - Run `grep -n "app\.storage\.user" web/pages/parallels.py` first to get the EXACT line numbers of all 35 sites (the research's line numbers are approximate — verify against current state)
      - For each line returned by grep, read 5 lines of context around it
      - Lines 3515-3530 SPECIFICALLY — the Codex round 4 MEDIUM-2 deferred-restore site (this is critical; understand the async callback flow before migrating)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (read R-02 for parallels.py site classifications and R-10 for Codex round 4 landmines)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (if "web/pages/parallels.py" section exists, follow it)
    - web/safe_storage.py (helper signatures reference)
  </read_first>
  <files>web/pages/parallels.py</files>
  <action>
**File: `web/pages/parallels.py`** — 35 raw access sites including the Codex round 4 deferred-callback site at line 3520.

This is a large migration. Approach systematically:

**Step 1: Comprehensive enumeration.**

Before editing, build a full inventory:
```bash
grep -n "app\.storage\.user" web/pages/parallels.py > /tmp/parallels_sites.txt
cat /tmp/parallels_sites.txt
wc -l /tmp/parallels_sites.txt
```

Expected count: 35 (verify against grep output).

**Step 2: Add safe_storage import at top of file.**

Find the existing imports. Add:
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

(If a partial import already exists, extend it.)

**Step 3: Classify and migrate each site.**

For EACH line from `/tmp/parallels_sites.txt`, identify which pattern applies:

| Pattern | Detection | Migration |
|---------|-----------|-----------|
| Raw read | `... = app.storage.user.get(KEY, DEFAULT)` | `... = safe_user_get(KEY, DEFAULT)` |
| Raw read in condition | `if app.storage.user.get(KEY, DEFAULT):` | `if safe_user_get(KEY, DEFAULT):` |
| Raw write | `app.storage.user[KEY] = VALUE` | `safe_user_set(KEY, VALUE)` |
| Raw pop | `app.storage.user.pop(KEY, DEFAULT)` | `safe_user_pop(KEY, DEFAULT)` |
| Inline try/except wrapper around any of the above | Whole try/except block | Single helper call; drop the wrapper |

Known specific sites from research grep (verify line numbers against your inventory — research's numbers may have drifted):

| Line | Operation | Key | Notes |
|------|-----------|-----|-------|
| 340 | read | composition_history | `return app.storage.user.get('composition_history', [])` → `return safe_user_get('composition_history', [])` |
| 344 | read in condition | session_persistence_enabled | `if not app.storage.user.get('session_persistence_enabled', True):` → `if not safe_user_get('session_persistence_enabled', True):` |
| 346 | read | search_history_limit | `limit = app.storage.user.get('search_history_limit', 20)` → `limit = safe_user_get('search_history_limit', 20)` |
| 376 | write | composition_history | `app.storage.user['composition_history'] = history` → `safe_user_set('composition_history', history)` |
| 383 | write | composition_history | same pattern as 376 |
| 387 | write | composition_history | same pattern as 376 (likely a `= []` reset) |
| 457 | write | parallels_source_text | `app.storage.user['parallels_source_text'] = text` → `safe_user_set('parallels_source_text', text)` |
| 883 | read | word_search_excluded_ids | `ws_excluded = app.storage.user.get('word_search_excluded_ids', [])` → `ws_excluded = safe_user_get('word_search_excluded_ids', [])` |
| 929 | write | parallels_filter_domains | `app.storage.user['parallels_filter_domains'] = []` → `safe_user_set('parallels_filter_domains', [])` |
| 938 | write | parallels_filter_domains (?) | likely similar to 929 |
| 1419, 1424 | filter writes | (verify by reading context) | |
| 2051, 2055 | filter reads/writes | (verify) | |
| 2343, 2346 | (verify) | (verify) | |
| 2409 | (verify) | (verify) | |
| 2729 | (verify) | (verify) | |
| 3520, 3523 | **CRITICAL — Codex MEDIUM-2** | deferred-restore callback | |

For the remaining sites not enumerated above, use the grep output to identify them and apply the pattern.

**Step 4: SPECIAL HANDLING for line 3520 (Codex round 4 MEDIUM-2 deferred callback).**

Read lines 3500-3540 carefully. The site is described in research as:

> parallels.py:3520 (inside `async def _deferred_restore()` — known fragility per Codex round 4 MEDIUM-2)

This is an async callback scheduled via `asyncio.ensure_future` or `ui.timer`. It runs AFTER the page handler returns. Storage may be in a different state than when the callback was scheduled.

Migration approach:
- The inline access on line 3520 (whatever its exact shape — `get`/`set`/`pop`) becomes a safe_storage helper call.
- The helper absorbs AssertionError (the prune-race case that motivated Phase 87 entirely).
- The new behavior: on prune-race, the deferred callback silently no-ops (returns default for reads; logs at debug; no exception bubbles to the asyncio event loop).
- This is INTENTIONAL per Codex round 4 review — silent loss of state in deferred callbacks is the documented tradeoff vs. crashing the asyncio loop.

Add a code comment at the migration site documenting this:
```python
# Phase 87 migration: deferred callbacks may silently lose state on session prune
# (the safe_storage helpers absorb AssertionError). This is intentional — the
# alternative would crash the asyncio event loop. See Codex round 4 MEDIUM-2.
```

**Step 5: Verify.**

After all 35 sites migrated:
```bash
grep -c "app\.storage\.user" web/pages/parallels.py     # expect 0
grep -c "safe_user_get\|safe_user_set\|safe_user_pop" web/pages/parallels.py  # expect at least 36 (1 import + 35 sites)
grep -c "'composition_history'" web/pages/parallels.py  # expect at least 4 (key preserved across all 4 sites)
grep -c "'parallels_source_text'" web/pages/parallels.py  # expect at least 1 (key preserved)
python -c "import ast; ast.parse(open('web/pages/parallels.py').read())"
ruff check web/pages/parallels.py
pytest tests/ -k "parallels" --tb=short -q
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/pages/parallels.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/pages/parallels.py` returns 0
    - `grep -c "from web.safe_storage import" web/pages/parallels.py` returns 1
    - `grep -c "safe_user_get\|safe_user_set\|safe_user_pop" web/pages/parallels.py` returns at least 36 (1 import + 35 sites)
    - Codex MEDIUM-2 comment added near line 3520 (verify with `grep -c "Codex round 4 MEDIUM-2\|deferred callbacks may silently lose" web/pages/parallels.py` returns at least 1)
    - All 35 storage keys still referenced in the migrated calls (no key accidentally renamed). Sanity check the most important keys:
      - `grep -c "'composition_history'" web/pages/parallels.py` returns at least 4
      - `grep -c "'parallels_source_text'" web/pages/parallels.py` returns at least 1
      - `grep -c "'parallels_filter_domains'" web/pages/parallels.py` returns at least 2
      - `grep -c "'word_search_excluded_ids'" web/pages/parallels.py` returns at least 1
      - `grep -c "'session_persistence_enabled'" web/pages/parallels.py` returns at least 1
    - File parses: `python -c "import ast; ast.parse(open('web/pages/parallels.py').read())"` exits 0
    - `ruff check web/pages/parallels.py` exits 0
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0
    - `pytest tests/ -k parallels --tb=short` exits 0 if any tests exist
  </acceptance_criteria>
  <done>parallels.py: 35 → 0 raw accesses; Codex deferred-callback site at 3520 migrated with documenting comment.</done>
</task>

<task type="auto">
  <name>Task 2: Migrate web/pages/search.py (14 sites)</name>
  <read_first>
    - web/pages/search.py — read selectively (file is ~4700 lines):
      - Top of file (find imports; per research line 101 already has `_safe_get` aliased)
      - Run `grep -n "app\.storage\.user" web/pages/search.py` first to confirm the 14 sites
      - For each site, read 5 lines of context
      - Lines 4360-4365 (verify line 4362 — search_query write inside a search-execution path)
      - Lines 4415-4425 (verify line 4420 — show_translations read in result-enrichment path)
      - Lines 4625-4635 (verify line 4630 — show_translations read in tag display)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (if section exists)
  </read_first>
  <files>web/pages/search.py</files>
  <action>
**File: `web/pages/search.py`** — 14 raw access sites.

**Step 1: Confirm/extend safe_storage import.**

Per research line 101, the file already imports `_safe_get` (likely as `from web.safe_storage import safe_user_get as _safe_get`). Verify with grep, then extend to include `safe_user_set`:
```python
from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
```

**Step 2: Migrate 14 sites.**

Per research, the 14 sites are:

| Line | Operation | Key | Migration |
|------|-----------|-----|-----------|
| 422 | write | search_query | `app.storage.user['search_query'] = query_input.value or ''` → `_safe_set('search_query', query_input.value or '')` |
| 532 | write | search_preset | `app.storage.user['search_preset'] = level_value` → `_safe_set('search_preset', level_value)` |
| 545 | write | search_gap | `app.storage.user['search_gap'] = int(gap_input.value or 0)` → `_safe_set('search_gap', int(gap_input.value or 0))` |
| 657 | write | search_text_position | `app.storage.user['search_text_position'] = text_position_select.value` → `_safe_set('search_text_position', text_position_select.value)` |
| 681 | write | search_preset | `app.storage.user['search_preset'] = val` → `_safe_set('search_preset', val)` |
| 689 | write | search_max_changes | `app.storage.user['search_max_changes'] = int(max_changes_select.value)` → `_safe_set('search_max_changes', int(max_changes_select.value))` |
| 718 | write | search_mode | `app.storage.user['search_mode'] = mode` → `_safe_set('search_mode', mode)` |
| 1086 | write | search_text_position | `app.storage.user['search_text_position'] = 'anywhere'` → `_safe_set('search_text_position', 'anywhere')` |
| 2055 | write | search_query | `app.storage.user['search_query'] = ''` → `_safe_set('search_query', '')` |
| 2056 | write | search_mode | `app.storage.user['search_mode'] = 'exact'` → `_safe_set('search_mode', 'exact')` |
| 2061 | write | search_text_position | `app.storage.user['search_text_position'] = 'anywhere'` → `_safe_set('search_text_position', 'anywhere')` |
| 4362 | write | search_query | `app.storage.user['search_query'] = clean_query` → `_safe_set('search_query', clean_query)` |
| 4420 | read | show_translations | `_show_trans_for_enrich = app.storage.user.get('show_translations', False)` → `_show_trans_for_enrich = _safe_get('show_translations', False)` |
| 4630 | read | show_translations | `_tag_show = app.storage.user.get('show_translations', False)` → `_tag_show = _safe_get('show_translations', False)` |

NOTE: Lines 2055-2061 are likely a "reset/clear search state" code block — verify they execute together. Migrate them as 3 separate calls.

**Step 3: Verify.**

```bash
grep -c "app\.storage\.user" web/pages/search.py     # expect 0
grep -c "_safe_set\|_safe_get\|safe_user_get\|safe_user_set" web/pages/search.py  # expect at least 15 (1 import line counted as import + 14 sites = 15)
grep -c "'search_query'" web/pages/search.py         # at least 3 references preserved (422, 2055, 4362)
grep -c "'search_mode'" web/pages/search.py          # at least 2 preserved (718, 2056)
python -c "import ast; ast.parse(open('web/pages/search.py').read())"
ruff check web/pages/search.py
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/pages/search.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/pages/search.py` returns 0
    - `grep -c "from web.safe_storage import" web/pages/search.py` returns 1
    - `grep -c "_safe_set\|safe_user_set" web/pages/search.py` returns at least 12 (12 write sites)
    - `grep -c "_safe_get\|safe_user_get" web/pages/search.py` returns at least 3 (existing reads + 2 new show_translations reads)
    - All 7 storage keys still referenced:
      - `grep -c "'search_query'" web/pages/search.py` returns at least 3
      - `grep -c "'search_preset'" web/pages/search.py` returns at least 2
      - `grep -c "'search_gap'" web/pages/search.py` returns at least 1
      - `grep -c "'search_text_position'" web/pages/search.py` returns at least 3
      - `grep -c "'search_max_changes'" web/pages/search.py` returns at least 1
      - `grep -c "'search_mode'" web/pages/search.py` returns at least 2
      - `grep -c "'show_translations'" web/pages/search.py` returns at least 2
    - File parses: `python -c "import ast; ast.parse(open('web/pages/search.py').read())"` exits 0
    - `ruff check web/pages/search.py` exits 0
    - `pytest tests/ -k "search and not session_uuid" --tb=short` exits 0 if any tests exist (excluding test_session_uuid.py which is Plan 02's)
  </acceptance_criteria>
  <done>search.py: 14 → 0 raw accesses; all 7 keys preserved.</done>
</task>

<task type="auto">
  <name>Task 3: Migrate web/pages/search_state.py (31 sites — bulk writes in persist + bulk reads in restore + filter pops)</name>
  <read_first>
    - web/pages/search_state.py FULL FILE (this is a state-management module; understanding the full structure is critical — it has 3 main functions: `restore_search_snapshot`, `persist_search_snapshot`, and `_reset_filter_storage_keys`)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/pages/search_state.py" section — has explicit before/after for the persist function)
    - web/pages/browse_state.py (REFERENCE — Plan 05 migrated this file with the same pattern; use it as the analog)
    - tests/ — check if `tests/test_search_state.py` or similar exists; if so, must remain green
  </read_first>
  <files>web/pages/search_state.py</files>
  <action>
**File: `web/pages/search_state.py`** — 31 raw access sites. The file has a clear structure (persist + restore + filter-reset), so migration is mostly mechanical.

**Step 1: Inventory and grouping.**

Run:
```bash
grep -n "app\.storage\.user" web/pages/search_state.py > /tmp/search_state_sites.txt
cat /tmp/search_state_sites.txt
wc -l /tmp/search_state_sites.txt
```

Per research the sites are roughly:
- Lines 343, 390: ALREADY use `safe_user_get` (existing reads at top of file — preserve)
- Lines 351: write (schema_version migration upgrade — similar to browse_state pattern)
- Lines 362-374: reads inside `restore_search_snapshot` (search_results, search_printed_filter, domain_exclusions, search_refinement_chain, search_exclusion_sources)
- Lines 394-471: writes inside `persist_search_snapshot` (bulk write of 6+ snapshot keys)
- Lines 441-502, 513-563: writes/pops inside `_reset_filter_storage_keys`

Verify via grep output that these line ranges still match. The grep output is authoritative — research line numbers may have drifted slightly.

**Step 2: Add or confirm safe_storage import.**

Per research, the file already partially uses `safe_user_get` (lines 343, 390). Verify and extend to include all three helpers:
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

**Step 3: Migrate the `restore_search_snapshot` function reads.**

For each of these BEFORE/AFTER pairs (verify exact text by reading the function body first):

| Line | Before | After |
|------|--------|-------|
| 362 | `state.results = app.storage.user.get('search_results', []) or []` | `state.results = safe_user_get('search_results', []) or []` |
| 363 | `state.printed_filter = app.storage.user.get('search_printed_filter', 'all')` | `state.printed_filter = safe_user_get('search_printed_filter', 'all')` |
| 364 | `_de = app.storage.user.get('domain_exclusions')` | `_de = safe_user_get('domain_exclusions')` |
| 368 | `raw_chain = app.storage.user.get('search_refinement_chain', []) or []` | `raw_chain = safe_user_get('search_refinement_chain', []) or []` |
| 374 | `state.exclusion_sources = app.storage.user.get('search_exclusion_sources', []) or []` | `state.exclusion_sources = safe_user_get('search_exclusion_sources', []) or []` |

If the function has an outer try/except wrapping ALL of these reads, drop the outer try/except (each helper absorbs its own exception). If it has a check like `if pos is None: return None` based on a particular missing key, preserve that logic.

**Step 4: Migrate the `persist_search_snapshot` function writes.**

Per PATTERNS.md:

BEFORE:
```python
app.storage.user['search_snapshot_schema_version'] = _SEARCH_SNAPSHOT_VERSION
app.storage.user['search_results'] = _compact_result_rows(...)
app.storage.user['search_printed_filter'] = state.printed_filter
app.storage.user['domain_exclusions'] = list(state.domain_exclusions or [])
app.storage.user['search_refinement_chain'] = [...]
app.storage.user['search_exclusion_sources'] = list(state.exclusion_sources or [])
```

AFTER:
```python
safe_user_set('search_snapshot_schema_version', _SEARCH_SNAPSHOT_VERSION)
safe_user_set('search_results', _compact_result_rows(...))
safe_user_set('search_printed_filter', state.printed_filter)
safe_user_set('domain_exclusions', list(state.domain_exclusions or []))
safe_user_set('search_refinement_chain', [...])
safe_user_set('search_exclusion_sources', list(state.exclusion_sources or []))
```

Drop any outer try/except around the block.

**Step 5: Migrate `_reset_filter_storage_keys` function.**

Per research, this function iterates over filter keys and either sets or pops them. The pattern is:
```python
app.storage.user[key] = value          # → safe_user_set(key, value)
# or
app.storage.user.pop(key, None)        # → safe_user_pop(key, None)
```

Apply both substitutions throughout the function body.

**Step 6: Final verification.**

```bash
grep -c "app\.storage\.user" web/pages/search_state.py     # expect 0
grep -c "safe_user_get\|safe_user_set\|safe_user_pop" web/pages/search_state.py  # expect at least 32 (1 import + 31 sites)
# Verify all snapshot keys preserved
for key in search_snapshot_schema_version search_results search_printed_filter domain_exclusions search_refinement_chain search_exclusion_sources session_persistence_enabled; do
  count=$(grep -c "'$key'" web/pages/search_state.py)
  echo "$key: $count refs"
done
python -c "import ast; ast.parse(open('web/pages/search_state.py').read())"
ruff check web/pages/search_state.py
pytest tests/ -k "search_state or search_snapshot" --tb=short
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/pages/search_state.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/pages/search_state.py` returns 0
    - `grep -c "from web.safe_storage import" web/pages/search_state.py` returns 1
    - `grep -c "safe_user_get\|safe_user_set\|safe_user_pop" web/pages/search_state.py` returns at least 32
    - All 6 snapshot keys preserved:
      - `grep -c "'search_snapshot_schema_version'" web/pages/search_state.py` returns at least 2 (read + write)
      - `grep -c "'search_results'" web/pages/search_state.py` returns at least 2
      - `grep -c "'search_printed_filter'" web/pages/search_state.py` returns at least 2
      - `grep -c "'domain_exclusions'" web/pages/search_state.py` returns at least 2
      - `grep -c "'search_refinement_chain'" web/pages/search_state.py` returns at least 2
      - `grep -c "'search_exclusion_sources'" web/pages/search_state.py` returns at least 2
    - File parses: `python -c "import ast; ast.parse(open('web/pages/search_state.py').read())"` exits 0
    - `ruff check web/pages/search_state.py` exits 0
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariant)
    - Any test_search_state.py or test_search_snapshot tests pass: `pytest tests/ -k "search_state or search_snapshot" --tb=short` exits 0
  </acceptance_criteria>
  <done>search_state.py: 31 → 0 raw accesses; persist/restore/reset functions migrated; all keys preserved.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Long-running search (>10s) → app.storage.user (write of partial results) | Prune-mid-flight risk; safe_user_set absorbs the AssertionError |
| Deferred-restore async callback (parallels.py:3520) → app.storage.user | Codex round 4 MEDIUM-2; safe_user_get returns default on prune (intentional silent loss vs. asyncio crash) |
| Filter chip handoff (search_state._reset_filter_storage_keys) → safe_user_pop | Cross-page handoff with safe semantics |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| — | Denial of Service | Search-page 500 on prune-mid-flight | mitigate | 80 sites in this plan now route through safe_storage; the entire search cluster is closed against the prune-race DoS class |
| — | Information disclosure | Last-writer-wins on `search_results` across users (singleton risk) | accept (Phase 88 handles) | search_state.py reads/writes are per-session via app.storage.user; cross-user leak risk is in `web/state.py:AppState` mirror fields, not search_state.py. Phase 88 STATE-01..06 explicitly addresses the singleton mirror. |
| T-87-04 | Tampering | Lint scanner — no allowlist entries for these 3 files | accept | All 3 files fully migrated; lint reports zero violations |
| — | Repudiation | Codex round 4 deferred-callback silent loss | accept | Documented in code comment near parallels.py:3520; silent loss in deferred callback is the intentional tradeoff vs. crashing the asyncio loop |

This plan does not directly mitigate T-87-01/02/03 (Plan 02's concern). Primary value: closing the prune-race DoS class at the 80 highest-traffic sites in the web app + addressing the Codex round 4 MEDIUM-2 deferred-callback finding.
</threat_model>

<verification>
After all 3 tasks:

```bash
# Verify zero raw access in all 3 files
for f in web/pages/parallels.py web/pages/search.py web/pages/search_state.py; do
  count=$(grep -c "app\.storage\.user" "$f")
  echo "$f: $count raw access (expect 0)"
done

# Total Phase 87 migration site count so far (Plans 03, 04, 05, 06)
echo "Total migrated:"
echo "  Plan 03 leaf: 16"
echo "  Plan 04 main+aliases: 18"
echo "  Plan 05 browse cluster: 18"
echo "  Plan 06 search cluster: 80"
echo "  Total: 132 sites"

# Verify all 3 files import safe_storage
for f in web/pages/parallels.py web/pages/search.py web/pages/search_state.py; do
  count=$(grep -c "from web.safe_storage import" "$f")
  echo "$f: $count safe_storage imports (expect 1)"
done

# Verify files parse and pass ruff
python -c "
import ast
for f in ['web/pages/parallels.py', 'web/pages/search.py', 'web/pages/search_state.py']:
    ast.parse(open(f).read(), filename=f)
print('All 3 files parse OK')
"
ruff check web/pages/parallels.py web/pages/search.py web/pages/search_state.py

# Codex MEDIUM-2 documenting comment present in parallels.py
grep -c "Codex round 4 MEDIUM-2\|deferred callbacks may silently lose" web/pages/parallels.py

# Plan 02 invariants
pytest tests/test_safe_storage.py tests/test_session_uuid.py -x

# Search-cluster tests (if any exist)
pytest tests/ -k "search or parallels" --tb=short -q

# Lint scanner: verify no violations in these 3 files
pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist 2>&1 | grep -E "parallels\.py|search\.py|search_state\.py" || echo "No violations in search cluster"
```
</verification>

<success_criteria>
1. `web/pages/parallels.py`: 0 raw access (was 35; includes the Codex round 4 MEDIUM-2 site at 3520)
2. `web/pages/search.py`: 0 raw access (was 14)
3. `web/pages/search_state.py`: 0 raw access (was 31)
4. Codex round 4 MEDIUM-2 documenting comment present near parallels.py:3520 (sweep verifiable via `grep -c "Codex round 4 MEDIUM-2"`)
5. All 3 files parse and pass ruff
6. Plan 02 invariants preserved (`pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0)
7. Lint scanner reports zero violations in any of the 3 files
8. Cumulative Phase 87 migration count: 132 sites (Plans 03-06)
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-06-SUMMARY.md` summarizing:
- 3 files migrated: parallels.py (35), search.py (14), search_state.py (31) = 80 total
- Codex round 4 MEDIUM-2 site at parallels.py:3520 migrated with documenting comment
- Cumulative Phase 87 migrated sites: 132 (16 + 18 + 18 + 80)
- All search-related tests still green
- Next step: Plan 07 (lint finalization) — the lint scanner should now report 0 violations
</output>
