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
  - tests/test_search_state.py
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
    - "tests/test_search_state.py monkeypatch targets updated to include web.safe_storage.app (B3 fix); all 8 existing tests still pass"
    - "Defensive wrappers catching JSON/parse errors PRESERVED (M3); only AssertionError-only wrappers collapsed"
    - "Plan 02 + Plan 03 + Plan 05 invariants preserved"
  artifacts:
    - path: "web/pages/parallels.py"
      provides: "All 35 sites migrated including bootstrap reads, history persistence, filter writes, and the deferred-restore callback at 3520"
      contains: "from web.safe_storage import"
    - path: "web/pages/search.py"
      provides: "All 14 sites migrated: query/preset/gap/mode/text_position writes + 2 show_translations reads (4420, 4630)"
      contains: "safe_user_set"
    - path: "web/pages/search_state.py"
      provides: "All 31 sites migrated in persist_search_snapshot, restore_search_snapshot, _reset_filter_storage_keys; M3 defensive wrappers preserved"
      contains: "safe_user_pop"
    - path: "tests/test_search_state.py"
      provides: "Monkeypatches updated to patch web.safe_storage.app — required after migration (B3 fix)"
      contains: "patch('web.safe_storage.app')"
  key_links:
    - from: "web/pages/parallels.py:3520 (deferred-restore callback)"
      to: "safe_user_get/set/pop"
      via: "Codex round 4 MEDIUM-2 — deferred async callback safety"
      pattern: "parallels"
    - from: "web/pages/search_state.py persist_search_snapshot"
      to: "safe_user_set for ~6 snapshot keys + safe_user_pop for filter reset"
      via: "wholesale function-body migration"
      pattern: "safe_user_set\\('search_snapshot_schema_version'"
    - from: "tests/test_search_state.py"
      to: "web.safe_storage.app"
      via: "B3 monkeypatch target update"
      pattern: "patch\\('web\\.safe_storage\\.app'\\)"
---

<objective>
Migrate the three search-cluster files (`parallels.py`, `search.py`, `search_state.py`) — the largest migration in Phase 87, totaling 80 raw access sites. Includes the explicit Codex round 4 MEDIUM-2 deferred-callback site at `parallels.py:3520`. ALSO update `tests/test_search_state.py` so its monkeypatches continue to work after migration (B3 fix mirrored from Plan 05).

**REVISION (B3, M1, M3, M4 from 87-REVIEWS.md):**
- **B3 (BLOCKER):** `tests/test_search_state.py` currently patches `web.pages.search_state.app`. After migration the storage reads/writes go through `web.safe_storage`. Tests must be updated to patch `web.safe_storage.app`. Note: one test (`test_stale_version_discards_snapshot`) already patches both — that test stays as a model for the others.
- **M1:** All acceptance criteria use `pytest tests/test_no_raw_storage_access.py` (and `pytest tests/test_search_state.py`), not grep.
- **M3:** Defensive try/except blocks catching JSON or type errors are preserved; AssertionError-only wrappers are collapsed.
- **M4:** Windows-safe Python one-liners throughout. No `/tmp` or POSIX-only.

Purpose: The search cluster is the most heavily-trafficked area of the web app and the place where prune-mid-flight bugs are most likely to surface (long-running searches >10s can have their storage pruned by the NiceGUI scheduler). The safe_storage migration here is the single largest reduction in 500-error surface from Phase 87.

Output: 3 production files migrated; 80 raw access sites converted; 1 test file updated for B3 fix; all existing search-related tests stay green.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md
@web/safe_storage.py
@web/pages/parallels.py
@web/pages/search.py
@web/pages/search_state.py
@tests/test_search_state.py

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

<!-- parallels.py storage keys: 36 raw accesses across ~30 distinct keys -->

```
'composition_history'             # list[dict] — search history
'session_persistence_enabled'     # bool (read)
'search_history_limit'            # int (read)
'parallels_source_text'           # str (write at 457)
'word_search_excluded_ids'        # list[str] (read at 883)
'parallels_filter_domains'        # list[str] (write at 929)
... plus ~25 more — enumerate via grep at the start of Task 1
```

<!-- B3 monkeypatch fix template (same as Plan 05): -->

For tests/test_search_state.py: each `patch('web.pages.search_state.app')`
must be replaced with (or augmented with) `patch('web.safe_storage.app')`.
The variable name `mock_app` and the inner `mock_app.storage.user = storage`
assignment do NOT change. One existing test (test_stale_version_discards_snapshot)
already uses the dual-patch idiom and serves as a model.

After Plan 06 lands, the simpler patch target is `web.safe_storage.app` alone,
because all storage reads/writes in search_state.py go through safe_storage.
Some tests also need `mock_app.storage.tab` — those still need a way to mock
tab storage. For tab storage, patch `web.pages.search_state.app` ADDITIONALLY
because `app.storage.tab` is still accessed directly in search_state.py (it's
not part of Phase 87's user-storage chokepoint).

Recommended pattern (verified by reading current search_state.py and the test):
```python
with patch('web.safe_storage.app') as mock_safe_app, \
     patch('web.pages.search_state.app') as mock_tab_app:
    mock_safe_app.storage.user = storage
    mock_tab_app.storage.tab = tab_storage
```

For tests that only exercise user storage and don't touch tab storage, the
single-patch form suffices:
```python
with patch('web.safe_storage.app') as mock_app:
    mock_app.storage.user = storage
```
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Migrate web/pages/parallels.py (35 sites including Codex deferred-callback at 3520) — DESCENDING line-number order</name>
  <read_first>
    - web/pages/parallels.py — read selectively (file is large, ~3800 lines):
      - Top of file (find imports; check for existing safe_storage imports)
      - Run the enumeration step below first to get the EXACT line numbers of all 35 sites
      - For each line returned by enumeration, read 5 lines of context
      - Lines 3515-3530 SPECIFICALLY — the Codex round 4 MEDIUM-2 deferred-restore site (this is critical; understand the async callback flow before migrating)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (read R-02 for parallels.py site classifications and R-10 for Codex round 4 landmines)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (if "web/pages/parallels.py" section exists, follow it)
    - web/safe_storage.py (helper signatures reference)
    - The `<defensive_wrapper_preservation>` rule from Plan 03 (applies here too)
  </read_first>
  <files>web/pages/parallels.py</files>
  <action>
**File: `web/pages/parallels.py`** — 35 raw access sites including the Codex round 4 deferred-callback site at line 3520.

This is the largest migration in Phase 87. Approach systematically.

**Step 1: Comprehensive enumeration (Windows-safe).**

Before editing, build the complete inventory:
```
python -c "import re, pathlib; src = pathlib.Path('web/pages/parallels.py').read_text(encoding='utf-8'); hits = [(i, line.strip()) for i, line in enumerate(src.splitlines(), start=1) if 'app.storage.user' in line]; print(f'total: {len(hits)} sites'); [print(f'{i}: {line}') for i, line in hits]"
```

Expected: 35 lines. Save the output (mentally or in a working note); you will work through them in DESCENDING line-number order.

**Step 2: Add safe_storage import.**

```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

If a partial import already exists (per research line 101, search.py has `_safe_get` aliased — verify the same isn't true here), extend it.

**Step 3: Classify each site BEFORE migrating.**

For each line in your inventory, run a context read:
```
python -c "import pathlib; src = pathlib.Path('web/pages/parallels.py').read_text(encoding='utf-8').splitlines(); LINE = 3520  # change for each site; print(chr(10).join(src[max(0,LINE-6):LINE+5]))"
```

(Replace `LINE = N` with each line number.)

Classify each site:
- **Type R (read):** `... = app.storage.user.get(KEY, DEFAULT)` or in a condition. Migration: `safe_user_get(KEY, DEFAULT)`.
- **Type W (write):** `app.storage.user[KEY] = VALUE`. Migration: `safe_user_set(KEY, VALUE)`.
- **Type P (pop):** `app.storage.user.pop(KEY, DEFAULT)`. Migration: `safe_user_pop(KEY, DEFAULT)`.
- **Wrapper Class A (collapse):** Surrounding try/except catches only AssertionError or generic Exception with a default-fallback body.
- **Wrapper Class B (preserve):** Surrounding try/except catches json.JSONDecodeError, ValueError, TypeError, KeyError, or any non-storage-specific error.

**Step 4: Migrate sites in DESCENDING line-number order.**

Apply the migration table from research as a starting point, BUT use your actual enumeration output as the authoritative list (research's line numbers may have drifted):

| Line | Type | Migration |
|------|------|-----------|
| 3520, 3523 (verify) | Deferred-restore — **Codex MEDIUM-2** | See Step 5 |
| 2729 | (verify) | Per type |
| 2409 | (verify) | Per type |
| 2343, 2346 | (verify) | Per type |
| 2051, 2055 | (verify) | Per type |
| 1419, 1424 | (verify) | Per type |
| 938 | W | `safe_user_set` |
| 929 | W | `safe_user_set('parallels_filter_domains', [])` |
| 883 | R | `safe_user_get('word_search_excluded_ids', [])` |
| 457 | W | `safe_user_set('parallels_source_text', text)` |
| 387 | W | `safe_user_set('composition_history', history)` |
| 383 | W | `safe_user_set('composition_history', history)` |
| 376 | W | `safe_user_set('composition_history', history)` |
| 346 | R | `safe_user_get('search_history_limit', 20)` |
| 344 | R in condition | `if not safe_user_get('session_persistence_enabled', True):` |
| 340 | R | `safe_user_get('composition_history', [])` |

(For any line not enumerated above, classify via Step 3 and apply the appropriate substitution.)

**Step 5: SPECIAL HANDLING for line 3520 (Codex round 4 MEDIUM-2 deferred callback).**

Read lines 3500-3540 carefully. The site is described in research as:

> parallels.py:3520 (inside `async def _deferred_restore()` — known fragility per Codex round 4 MEDIUM-2)

This is an async callback scheduled via `asyncio.ensure_future` or `ui.timer`. It runs AFTER the page handler returns. Storage may be in a different state than when the callback was scheduled.

Migration approach:
- The inline access on line 3520 (whatever its exact shape — get/set/pop) becomes a safe_storage helper call.
- The helper absorbs AssertionError (the prune-race case that motivated Phase 87).
- The new behavior: on prune-race, the deferred callback silently no-ops (returns default for reads; logs at debug; no exception bubbles to the asyncio event loop).
- This is INTENTIONAL per Codex round 4 review — silent loss of state in deferred callbacks is the documented tradeoff vs. crashing the asyncio loop.

Add a code comment at the migration site documenting this:
```python
# Phase 87 migration (87-REVIEWS.md MEDIUM-2 from Codex round 4): deferred
# callbacks may silently lose state on session prune (safe_storage helpers
# absorb AssertionError). This is intentional — the alternative would crash
# the asyncio event loop.
```

**Step 6: Verify (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/pages/parallels.py').read()); print('parses OK')"
ruff check web/pages/parallels.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/parallels.py'), pathlib.Path('web/pages/parallels.py').read_text(encoding='utf-8')); assert len(v) == 0, v[:5]; print('OK')"
python -c "import re; src = open('web/pages/parallels.py').read(); print('Codex MEDIUM-2 comment present:', bool(re.search(r'MEDIUM-2|deferred callbacks may silently lose', src)))"
```
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/parallels.py'), pathlib.Path('web/pages/parallels.py').read_text(encoding='utf-8')); assert len(v) == 0, v[:5]; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File parses: `python -c "import ast; ast.parse(open('web/pages/parallels.py').read())"` exits 0
    - `ruff check web/pages/parallels.py` exits 0
    - AST scanner reports 0 violations (verified by `<verify>`)
    - safe_storage import present
    - Codex MEDIUM-2 documenting comment present near the deferred-restore site: `python -c "import re; src = open('web/pages/parallels.py').read(); assert re.search(r'MEDIUM-2|deferred callbacks may silently', src); print('OK')"` prints `OK`
    - Critical keys preserved (sanity check):
      - `'composition_history'` count ≥ 4
      - `'parallels_source_text'` count ≥ 1
      - `'parallels_filter_domains'` count ≥ 2
      - `'word_search_excluded_ids'` count ≥ 1
      - `'session_persistence_enabled'` count ≥ 1
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0
    - `pytest tests/ -k parallels --tb=short` exits 0 if any parallels tests exist
    - M3 audit recorded in SUMMARY
  </acceptance_criteria>
  <done>parallels.py: 35 → 0 raw accesses; Codex deferred-callback site at 3520 migrated with documenting comment.</done>
</task>

<task type="auto">
  <name>Task 2: Migrate web/pages/search.py (14 sites) — DESCENDING line-number order</name>
  <read_first>
    - web/pages/search.py — read selectively (file is ~4700 lines):
      - Top of file (find imports; per research line 101 already has `_safe_get` aliased)
      - Run enumeration step below to confirm the 14 sites
      - For each site, read 5 lines of context
      - Lines 4360-4365, 4415-4425, 4625-4635 SPECIFICALLY for the 3 highest-numbered sites
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (if section exists)
  </read_first>
  <files>web/pages/search.py</files>
  <action>
**File: `web/pages/search.py`** — 14 raw access sites.

**Step 1: Enumerate (Windows-safe).**

```
python -c "import pathlib; src = pathlib.Path('web/pages/search.py').read_text(encoding='utf-8'); hits = [(i, line.strip()) for i, line in enumerate(src.splitlines(), start=1) if 'app.storage.user' in line]; print(f'total: {len(hits)}'); [print(f'{i}: {line}') for i, line in hits]"
```

Expected: 14 lines.

**Step 2: Confirm/extend safe_storage import.**

Per research line 101, the file already imports `_safe_get` (likely as `from web.safe_storage import safe_user_get as _safe_get`). Verify (Windows-safe):
```
python -c "import re; print(re.search(r'from web\\.safe_storage import', open('web/pages/search.py').read()))"
```

Extend to include `safe_user_set`:
```python
from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
```

**Step 3: Migrate 14 sites in DESCENDING line-number order.**

| Order | Line | Type | Migration |
|-------|------|------|-----------|
| 1 | 4630 | R | `_tag_show = _safe_get('show_translations', False)` |
| 2 | 4420 | R | `_show_trans_for_enrich = _safe_get('show_translations', False)` |
| 3 | 4362 | W | `_safe_set('search_query', clean_query)` |
| 4 | 2061 | W | `_safe_set('search_text_position', 'anywhere')` |
| 5 | 2056 | W | `_safe_set('search_mode', 'exact')` |
| 6 | 2055 | W | `_safe_set('search_query', '')` |
| 7 | 1086 | W | `_safe_set('search_text_position', 'anywhere')` |
| 8 | 718 | W | `_safe_set('search_mode', mode)` |
| 9 | 689 | W | `_safe_set('search_max_changes', int(max_changes_select.value))` |
| 10 | 681 | W | `_safe_set('search_preset', val)` |
| 11 | 657 | W | `_safe_set('search_text_position', text_position_select.value)` |
| 12 | 545 | W | `_safe_set('search_gap', int(gap_input.value or 0))` |
| 13 | 532 | W | `_safe_set('search_preset', level_value)` |
| 14 | 422 | W | `_safe_set('search_query', query_input.value or '')` |

**Apply M3 audit:** lines 545 and 689 use `int(...)` conversion which can raise ValueError. If wrapped in try/except ValueError, preserve the wrapper. Otherwise straight substitution.

**Step 4: Verify (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/pages/search.py').read()); print('parses OK')"
ruff check web/pages/search.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/search.py'), pathlib.Path('web/pages/search.py').read_text(encoding='utf-8')); assert len(v) == 0, v[:5]; print('OK')"
```
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/search.py'), pathlib.Path('web/pages/search.py').read_text(encoding='utf-8')); assert len(v) == 0, v[:5]; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File parses
    - `ruff check web/pages/search.py` exits 0
    - AST scanner reports 0 violations (verified by `<verify>`)
    - safe_storage import present
    - All 7 storage keys still referenced (Python regex count, each ≥1):
      - `search_query`, `search_preset`, `search_gap`, `search_text_position`, `search_max_changes`, `search_mode`, `show_translations`
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0
    - M3 audit recorded
  </acceptance_criteria>
  <done>search.py: 14 → 0 raw accesses; all 7 keys preserved.</done>
</task>

<task type="auto">
  <name>Task 3: Migrate web/pages/search_state.py (31 sites — bulk writes in persist + bulk reads in restore + filter pops) — M3 audit + M2 independent reads</name>
  <read_first>
    - web/pages/search_state.py FULL FILE (this is a state-management module; understanding the full structure is critical — it has 3 main functions: `restore_search_snapshot`, `persist_search_snapshot`, `_reset_filter_storage_keys` plus several helper paths)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/pages/search_state.py" section)
    - web/pages/browse_state.py (REFERENCE — Plan 05 migrated this file with the same pattern; use it as the analog)
    - tests/test_search_state.py FULL FILE (the test contract — 8 tests; understand what each asserts so the migration preserves semantics)
    - The `<defensive_wrapper_preservation>` rule and M2 rule from prior plans
  </read_first>
  <files>web/pages/search_state.py</files>
  <action>
**File: `web/pages/search_state.py`** — 31 raw access sites. The file has a clear structure (persist + restore + filter-reset).

**Step 1: Inventory and grouping (Windows-safe).**

```
python -c "import pathlib; src = pathlib.Path('web/pages/search_state.py').read_text(encoding='utf-8'); hits = [(i, line.strip()) for i, line in enumerate(src.splitlines(), start=1) if 'app.storage.user' in line]; print(f'total: {len(hits)}'); [print(f'{i}: {line}') for i, line in hits]"
```

Expected: 31 lines. Cross-reference with research line numbers (343, 351, 362-374, 390, 394-471, 441-502, 513-563).

**Step 2: Check existing safe_storage import.**

Per research, the file already partially uses `safe_user_get` (lines 343, 390 — those ALREADY went through the helper). Verify:
```
python -c "import re; print(re.search(r'from web\\.safe_storage import', open('web/pages/search_state.py').read()))"
```

Extend to include all three helpers:
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

**Step 3: Migrate `restore_search_snapshot` reads (M2 independent-read semantics).**

Per the test `test_persist_and_restore_round_trip`, the function reads multiple keys independently and assembles them into the SearchUIState. M2 applies: a missing `search_results` must not short-circuit the `domain_exclusions` read.

For each read site (lines 362-374 approximately), apply:

| Line | Before | After |
|------|--------|-------|
| 362 | `state.results = app.storage.user.get('search_results', []) or []` | `state.results = safe_user_get('search_results', []) or []` |
| 363 | `state.printed_filter = app.storage.user.get('search_printed_filter', 'all')` | `state.printed_filter = safe_user_get('search_printed_filter', 'all')` |
| 364 | `_de = app.storage.user.get('domain_exclusions')` | `_de = safe_user_get('domain_exclusions')` |
| 368 | `raw_chain = app.storage.user.get('search_refinement_chain', []) or []` | `raw_chain = safe_user_get('search_refinement_chain', []) or []` |
| 374 | `state.exclusion_sources = app.storage.user.get('search_exclusion_sources', []) or []` | `state.exclusion_sources = safe_user_get('search_exclusion_sources', []) or []` |

**M3 audit:** if the function wraps these reads in a single outer try/except json.JSONDecodeError or similar, KEEP the wrapper (preserve M3). If the wrapper catches only AssertionError, collapse it.

**Step 4: Migrate `persist_search_snapshot` writes.**

Per PATTERNS.md, the persist function does a bulk write of 6 keys.

**Specific Class B preservation site (Fix 4 in 87-REVIEWS.md iteration 3 — Codex MEDIUM M3 residual):** `persist_search_snapshot()` at lines 384-410 (post-iter2 line numbers). The try-except at lines 393-410 wraps `app.storage.user['search_snapshot_schema_version'] = ...`, `persist_search_active_snapshot(state)`, `_compact_result_rows(...)` call, `state.refinement_chain` `to_dict()` iteration, AND 4 more `app.storage.user[...]` assignments. This is **Class B** because the block covers non-storage transformations (list slicing, `_compact_result_rows`, `to_dict()` over the refinement chain, list-construction expressions `list(state.domain_exclusions or [])`, etc.). **Do NOT collapse the outer try-except**; only replace raw `app.storage.user[...]` calls inside it with `safe_user_set`. Note: there is also a NESTED try-except at lines 402-407 around the refinement_chain `to_dict()` loop — that one is also Class B (catches transformation failures) and stays.

BEFORE (showing the wrapping try-except — lines 393-410):
```python
    try:
        app.storage.user['search_snapshot_schema_version'] = _SEARCH_SNAPSHOT_VERSION
        persist_search_active_snapshot(state)
        app.storage.user['search_results'] = _compact_result_rows(
            (state.results or [])[:_SEARCH_ACTIVE_USER_FALLBACK_LIMIT]
        )
        app.storage.user['search_printed_filter'] = state.printed_filter
        app.storage.user['domain_exclusions'] = list(state.domain_exclusions or [])
        # refinement_chain (list[RefinementStep] -> list[dict])
        try:
            app.storage.user['search_refinement_chain'] = [
                s.to_dict() for s in (state.refinement_chain or [])
            ]
        except Exception:
            app.storage.user['search_refinement_chain'] = []
        app.storage.user['search_exclusion_sources'] = list(state.exclusion_sources or [])
    except Exception:
        pass  # Browser storage operation failed; snapshot not persisted (D-08)
```

AFTER (BOTH try-except wrappers PRESERVED per Fix 4 — only raw storage calls swapped):
```python
    # Class B OUTER try-except PRESERVED — covers transformations.
    try:
        safe_user_set('search_snapshot_schema_version', _SEARCH_SNAPSHOT_VERSION)
        persist_search_active_snapshot(state)
        safe_user_set('search_results', _compact_result_rows(
            (state.results or [])[:_SEARCH_ACTIVE_USER_FALLBACK_LIMIT]
        ))
        safe_user_set('search_printed_filter', state.printed_filter)
        safe_user_set('domain_exclusions', list(state.domain_exclusions or []))
        # Class B INNER try-except also PRESERVED — wraps to_dict() iteration:
        try:
            safe_user_set('search_refinement_chain', [
                s.to_dict() for s in (state.refinement_chain or [])
            ])
        except Exception:
            safe_user_set('search_refinement_chain', [])
        safe_user_set('search_exclusion_sources', list(state.exclusion_sources or []))
    except Exception:
        pass  # Browser storage operation failed; snapshot not persisted (D-08)
```

This task does NOT drop either wrapper. The Class A vs Class B rule says: if the except clause covers ONLY storage prune (AssertionError), collapse. If it covers transformations (list/dict construction, to_dict, _compact_result_rows), preserve. Both wrappers in persist_search_snapshot fall into the preserve bucket.

**Step 5: Migrate `_reset_filter_storage_keys` function (pops + writes).**

Per research, this function iterates over filter keys and either sets or pops them. The pattern is mechanical:

```python
app.storage.user[key] = value          # → safe_user_set(key, value)
app.storage.user.pop(key, None)        # → safe_user_pop(key, None)
```

Apply both substitutions throughout the function body.

**Step 6: Run the test file BEFORE updating tests (intentional — Task 4 updates monkeypatches).**

```
python -m pytest tests/test_search_state.py -x -v
```

Some tests may pass (e.g., `test_stale_version_discards_snapshot` because it ALREADY patches `web.safe_storage.app`), some may fail (the ones that only patch `web.pages.search_state.app`). The failures are B3 evidence — Task 4 fixes them.

**Step 7: Verify (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/pages/search_state.py').read()); print('parses OK')"
ruff check web/pages/search_state.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/search_state.py'), pathlib.Path('web/pages/search_state.py').read_text(encoding='utf-8')); assert len(v) == 0, v[:5]; print('OK')"
```
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/search_state.py'), pathlib.Path('web/pages/search_state.py').read_text(encoding='utf-8')); assert len(v) == 0, v[:5]; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File parses
    - `ruff check web/pages/search_state.py` exits 0
    - AST scanner reports 0 violations (verified by `<verify>`)
    - safe_storage import present including all 3 helpers
    - All 6 snapshot keys preserved (each ≥2 references — read + write):
      - search_snapshot_schema_version, search_results, search_printed_filter, domain_exclusions, search_refinement_chain, search_exclusion_sources
    - **M2 preserved:** `restore_search_snapshot` reads each snapshot key via INDEPENDENT `safe_user_get` calls; one absent value does not short-circuit others
    - **M3 audit recorded** for each try/except in the file
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0
    - Test file failures from this task are documented (Task 4 fixes them)
  </acceptance_criteria>
  <done>search_state.py: 31 → 0 raw accesses; persist/restore/reset functions migrated; M2 and M3 preserved.</done>
</task>

<task type="auto">
  <name>Task 4: Update tests/test_search_state.py monkeypatches to web.safe_storage.app (B3 BLOCKER fix mirrored from Plan 05)</name>
  <read_first>
    - tests/test_search_state.py (FULL FILE — read all 8 tests; identify every `patch('web.pages.search_state.app')` site)
    - tests/test_search_state.py lines 159-170 (`test_stale_version_discards_snapshot`) — this test already uses the dual-patch idiom; use it as a model
    - web/pages/search_state.py (AFTER Task 3 — verify it no longer has any `app.storage.user` text)
    - tests/test_browse_state.py (Plan 05's B3 fix is the analog; same pattern applies)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (B3 description)
  </read_first>
  <files>tests/test_search_state.py</files>
  <action>
**B3 BLOCKER FIX:** Update `tests/test_search_state.py` so its monkeypatches work against the migrated production code.

After Task 3, `web/pages/search_state.py` reads from / writes to `app.storage.user` via `web.safe_storage` helpers. But the file ALSO uses `app.storage.tab` directly (for tab-scoped active-snapshot caching) — that is NOT routed through safe_storage in Phase 87 (it's per-tab, not per-session, and is out of scope per research).

So tests need to patch:
- `web.safe_storage.app` for user storage (the new chokepoint)
- `web.pages.search_state.app` for tab storage (still direct)

The pattern (verified against the existing `test_stale_version_discards_snapshot` model at lines 159-170):

```python
with patch('web.pages.search_state.app') as mock_app, \
     patch('web.safe_storage.app') as mock_safe_app:
    mock_app.storage.user = storage
    mock_app.storage.tab = tab_storage
    mock_safe_app.storage.user = storage
    # ... test body
```

**Why patch both `mock_app.storage.user` AND `mock_safe_app.storage.user`:**

Tests may import functions that ALSO read user storage directly (legacy paths) OR via safe_storage. Patching both targets to the SAME `storage` dict means any code path — direct or chokepointed — sees the same data.

**Step 1: Survey existing patches.**

```
python -c "import re; src = open('tests/test_search_state.py').read(); [print(i, line.strip()) for i, line in enumerate(src.splitlines(), start=1) if 'patch(' in line]"
```

Expected: 8 occurrences of `patch('web.pages.search_state.app')` (one per test, with `test_stale_version_discards_snapshot` ALSO having a `patch('web.safe_storage.app')` so total `patch(` calls = 9). One test (`test_stale_version_discards_snapshot`) already has the dual-patch idiom.

**Step 2: Update each test's patch block.**

For tests that currently use a SINGLE `with patch('web.pages.search_state.app') as mock_app:` block: convert to the DUAL-patch idiom shown above.

For `test_stale_version_discards_snapshot`: it already uses dual-patch — verify the pattern matches the template (mock_safe_app.storage.user = storage). No change needed if it does.

CONCRETE EXAMPLE for `test_persist_and_restore_round_trip` (lines 10-43):

BEFORE:
```python
def test_persist_and_restore_round_trip():
    storage = _make_storage()
    tab_storage = {}
    with patch('web.pages.search_state.app') as mock_app:
        mock_app.storage.user = storage
        mock_app.storage.tab = tab_storage

        from web.pages.search_state import (
            SearchUIState,
            persist_search_snapshot,
            restore_search_snapshot,
        )

        state = SearchUIState()
        state.results = [{'display': {'id': 'abc'}}]
        # ...
```

AFTER:
```python
def test_persist_and_restore_round_trip():
    storage = _make_storage()
    tab_storage = {}
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app:
        mock_app.storage.user = storage
        mock_app.storage.tab = tab_storage
        mock_safe_app.storage.user = storage

        from web.pages.search_state import (
            SearchUIState,
            persist_search_snapshot,
            restore_search_snapshot,
        )

        state = SearchUIState()
        state.results = [{'display': {'id': 'abc'}}]
        # ...
```

Apply the same transformation to all 8 tests (or to whichever tests don't already have the dual-patch — `test_stale_version_discards_snapshot` already does and needs no edit).

**Step 3: Run the test file.**

```
python -m pytest tests/test_search_state.py -x -v
```

Expected: all 8 tests pass.

Common failure modes:
- If a test fails on `mock_app.storage.tab` access: verify the test still has the `mock_app.storage.tab = tab_storage` line.
- If a test fails on `storage[key] = value` assignments NOT being visible to the production code: the production code is reading via `mock_safe_app.storage.user` — confirm both `mock_app.storage.user` AND `mock_safe_app.storage.user` point to the SAME `storage` dict (not different copies).

**Step 4: Verify (Windows-safe).**

```
python -c "import re; src = open('tests/test_search_state.py').read(); print('test count:', len(re.findall(r'^def test_', src, re.MULTILINE))); print('patch(safe_storage.app):', len(re.findall(r\"patch\\('web\\.safe_storage\\.app'\\)\", src))); print('patch(search_state.app):', len(re.findall(r\"patch\\('web\\.pages\\.search_state\\.app'\\)\", src)))"
```

Expected: 8 test functions; `patch('web.safe_storage.app')` count ≥8 (every test that touches user storage); `patch('web.pages.search_state.app')` count ≥6 (tests that use tab storage — likely all of them).
  </action>
  <verify>
    <automated>python -m pytest tests/test_search_state.py -x</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest tests/test_search_state.py -x` exits 0 (all 8 tests pass)
    - File contains 8 `def test_*` functions
    - At least 8 `patch('web.safe_storage.app')` occurrences (one per user-storage-touching test): verified via Python regex
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_browse_state.py -x` exits 0 (Plan 02 + Plan 05 invariants preserved)
  </acceptance_criteria>
  <done>test_search_state.py: 8 tests pass after monkeypatch update; B3 BLOCKER closed.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Long-running search (>10s) -> app.storage.user (write of partial results) | Prune-mid-flight risk; safe_user_set absorbs the AssertionError |
| Deferred-restore async callback (parallels.py:3520) -> app.storage.user | Codex round 4 MEDIUM-2; safe_user_get returns default on prune (intentional silent loss vs. asyncio crash) |
| Filter chip handoff (search_state._reset_filter_storage_keys) -> safe_user_pop | Cross-page handoff with safe semantics |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| -- | Denial of Service | Search-page 500 on prune-mid-flight | mitigate | 80 sites in this plan now route through safe_storage; the entire search cluster is closed against the prune-race DoS class |
| -- | Information disclosure | Last-writer-wins on `search_results` across users (singleton risk) | accept (Phase 88 handles) | search_state.py reads/writes are per-session via app.storage.user; cross-user leak risk is in `web/state.py:AppState` mirror fields, not search_state.py. Phase 88 STATE-01..06 explicitly addresses the singleton mirror. |
| T-87-04 | Tampering | Lint scanner — no allowlist entries for these 3 files | accept | All 3 files fully migrated; lint reports zero violations |
| -- | Repudiation | Codex round 4 deferred-callback silent loss | accept | Documented in code comment near parallels.py:3520; silent loss in deferred callback is the intentional tradeoff vs. crashing the asyncio loop |
| -- | Test integrity (B3) | Monkeypatch target drift after migration | mitigate | Task 4 explicitly updates test_search_state.py to dual-patch (web.safe_storage.app + web.pages.search_state.app) |

This plan does not directly mitigate T-87-01/02/03 (Plan 02's concern). Primary value: closing the prune-race DoS class at the 80 highest-traffic sites in the web app + addressing the Codex round 4 MEDIUM-2 deferred-callback finding + preserving test fidelity.
</threat_model>

<verification>
After all 4 tasks (Windows-safe):

```
# Verify zero violations in all 3 production files
python -c "
import sys, pathlib
sys.path.insert(0, '.')
from tests.test_no_raw_storage_access import _scan_file
for f in ['web/pages/parallels.py', 'web/pages/search.py', 'web/pages/search_state.py']:
    v = _scan_file(pathlib.Path(f), pathlib.Path(f).read_text(encoding='utf-8'))
    print(f, 'violations:', len(v))
    assert len(v) == 0, v[:3]
print('OK')
"

# Cumulative Phase 87 site count (after Plans 03-06)
python -c "print('Plan 03 leaf: 16'); print('Plan 04 main+aliases: 18'); print('Plan 05 browse cluster: 18'); print('Plan 06 search cluster: 80'); print('Total: 132 sites')"

# Verify safe_storage imports
python -c "
import re
for f in ['web/pages/parallels.py', 'web/pages/search.py', 'web/pages/search_state.py']:
    src = open(f).read()
    has = bool(re.search(r'from web\\.safe_storage import', src))
    print(f, has)
    assert has, f
"

# Verify files parse and pass ruff
python -c "
import ast
for f in ['web/pages/parallels.py', 'web/pages/search.py', 'web/pages/search_state.py']:
    ast.parse(open(f).read(), filename=f)
print('All 3 production files parse OK')
"
ruff check web/pages/parallels.py web/pages/search.py web/pages/search_state.py

# Codex MEDIUM-2 documenting comment present in parallels.py
python -c "import re; src = open('web/pages/parallels.py').read(); assert re.search(r'MEDIUM-2|deferred callbacks may silently', src), 'comment missing'; print('OK: Codex MEDIUM-2 comment present')"

# B3 (test integrity)
python -m pytest tests/test_search_state.py tests/test_browse_state.py -x

# Plan 02 invariants
python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x

# Plan 01 standalone tests
python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x

# Search-cluster tests (any)
python -m pytest tests/ -k "search or parallels" --tb=short -q
```
</verification>

<success_criteria>
1. `web/pages/parallels.py`: 0 AST violations (was 35; includes the Codex round 4 MEDIUM-2 site at 3520)
2. `web/pages/search.py`: 0 AST violations (was 14)
3. `web/pages/search_state.py`: 0 AST violations (was 31)
4. **B3:** `tests/test_search_state.py` monkeypatches updated; all 8 tests pass
5. Codex round 4 MEDIUM-2 documenting comment present near parallels.py:3520
6. All 3 production files parse and pass ruff
7. Plan 02 + Plan 05 invariants preserved
8. M3 audit recorded per file
9. Cumulative Phase 87 migration count: 132 sites (Plans 03-06)
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-06-SUMMARY.md` summarizing:
- 3 production files migrated: parallels.py (35), search.py (14), search_state.py (31) = 80 total
- 1 test file updated: tests/test_search_state.py — 8 tests' monkeypatches updated to dual-patch idiom (B3 fix)
- test_search_state.py: 8/8 tests pass after update
- Codex round 4 MEDIUM-2 site at parallels.py:3520 migrated with documenting comment
- **M2 verification:** search_state.py restore reads keys independently
- **M3 audit per file:** list defensive wrappers preserved
- **B3 verification:** test patches now route through safe_storage chokepoint
- Cumulative Phase 87 migrated sites: 132 (16 + 18 + 18 + 80)
- All search-related tests still green
- Next step: Plan 07 (lint finalization) — the lint scanner should now report 0 unallowlisted violations
</output>
