---
phase: 87
plan: 05
type: execute
wave: 2
depends_on: [87-02]
files_modified:
  - web/pages/browse.py
  - web/pages/browse_state.py
  - web/pages/catalog_browse.py
autonomous: true
requirements:
  - FOUND-02
tags:
  - phase87
  - migration
  - safe-storage
  - browse
  - browse-state
  - catalog
must_haves:
  truths:
    - "web/pages/browse.py raw accesses reduced from 4 to 0 (lines 1122, 1214, 2080, 2115)"
    - "web/pages/browse_state.py raw accesses reduced from 11 to 0 (lines 127, 137, 147, 153, 174, 180, 184, 197, 203, 224 — plus the inline guard wrappers removed)"
    - "web/pages/catalog_browse.py raw accesses reduced from 3 to 0 (lines 339, 954, 962)"
    - "tests/test_browse_state.py passes (existing 7+ tests preserved)"
    - "Plan 02 + Plan 03 invariants preserved"
  artifacts:
    - path: "web/pages/browse.py"
      provides: "Migrated 4 sites: reading_desk_state read (1122), browse_export_data write (1214), 2 show_translations reads (2080, 2115)"
      contains: "from web.safe_storage import"
    - path: "web/pages/browse_state.py"
      provides: "Migrated 11 sites in restore_browse_snapshot + persist_browse_snapshot + clear_browse_snapshot; inline try/except guards removed"
      contains: "safe_user_get, safe_user_set, safe_user_pop"
    - path: "web/pages/catalog_browse.py"
      provides: "Migrated 3 sites: show_translations read (339), incoming_filters writes (954, 962)"
      contains: "safe_user_get"
  key_links:
    - from: "web/pages/browse_state.py restore_browse_snapshot"
      to: "safe_user_get for schema_version + browse_position + reading_desk_state + session_persistence_enabled"
      via: "single-call replacement of inline try/except guards"
      pattern: "safe_user_get\\('browse_position'\\)"
    - from: "web/pages/browse.py reading desk restore"
      to: "safe_user_get('reading_desk_state')"
      via: "drop-in replacement at line 1122"
      pattern: "safe_user_get\\('reading_desk_state'\\)"
---

<objective>
Migrate the three browse-related files (`browse.py`, `browse_state.py`, `catalog_browse.py`) from raw `app.storage.user.*` access to `web.safe_storage` helpers. Total: 18 raw access sites migrated; `browse_state.py`'s inline try/except guards (which predate the safe_storage module) are replaced with single-line helper calls.

Purpose: The browse cluster is the page where the original v7.11.0 `/browse 500` bug surfaced (`AssertionError` on pruned session storage during reading-desk restore). The safe_storage module was created specifically to fix this class of bug; this plan completes the migration that `cca23db3` started.

Output: 3 files migrated; 18 sites converted; inline try/except guards collapsed to helper calls; existing `tests/test_browse_state.py` (7 tests per HANDOFF) passes.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@web/safe_storage.py
@web/pages/browse.py
@web/pages/browse_state.py
@web/pages/catalog_browse.py
@tests/test_browse_state.py

<interfaces>
<!-- Browse state contract — these are the storage keys that browse_state.py owns: -->

```
'browse_snapshot_schema_version'  # int, version of the snapshot schema
'browse_position'                 # dict — {sys_id, fl_id, page_number, ...}
'reading_desk_state'              # dict — {selected_uids: [...], notes: ..., ...}
'session_persistence_enabled'     # bool (read but written by settings.py)
'browse_export_data'              # dict — export payload (browse.py:1214)
'show_translations'               # bool (read in browse.py:2080,2115 and catalog_browse.py:339)
'incoming_filters'                # dict — cross-page filter handoff (catalog_browse.py:954,962)
```

<!-- Migration pattern for inline try/except (used heavily in browse_state.py): -->

Before:
```python
try:
    value = app.storage.user.get(key, default)
except (AssertionError, Exception) as e:
    logger.debug(f"...: {e}")
    return default_result
```

After:
```python
value = safe_user_get(key, default)
# (the outer "return default_result" path is collapsed; safe_user_get already returns default on failure)
```
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Migrate web/pages/browse.py (4 sites)</name>
  <read_first>
    - web/pages/browse.py — read at minimum:
      - Top of file (find existing imports; check for any existing `from web.safe_storage import`)
      - Lines 1115-1135 (verify line 1122 reads `reading_desk_state` inside a "Restore reading desk state from app.storage.user after language switch" function)
      - Lines 1205-1225 (verify line 1214 writes `browse_export_data`)
      - Lines 2075-2090 (verify line 2080 reads `show_translations`)
      - Lines 2110-2125 (verify line 2115 reads `show_translations` — different code path)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/pages/browse.py" section if present, otherwise refer to general migration pattern)
    - web/safe_storage.py (so you know the helper signatures)
  </read_first>
  <files>web/pages/browse.py</files>
  <action>
**File: `web/pages/browse.py`** — 4 raw access sites.

**Step 1: Add safe_storage import.**

Find the existing imports section near the top of the file. Add or extend:
```python
from web.safe_storage import safe_user_get, safe_user_set
```

(If a partial import already exists, e.g., `from web.safe_storage import safe_user_get`, extend it to include `safe_user_set`.)

**Step 2: Migrate 4 sites.**

| Line | Current | Replace With |
|------|---------|--------------|
| 1122 | `saved = app.storage.user.get('reading_desk_state')` | `saved = safe_user_get('reading_desk_state')` |
| 1214 | `app.storage.user['browse_export_data'] = export_data` | `safe_user_set('browse_export_data', export_data)` |
| 2080 | `_browse_show_trans = app.storage.user.get('show_translations', False)` | `_browse_show_trans = safe_user_get('show_translations', False)` |
| 2115 | `_show_trans_browse = app.storage.user.get('show_translations', False)` | `_show_trans_browse = safe_user_get('show_translations', False)` |

NOTE: At line 1122, the surrounding code may have an outer try/except guarding `app.storage.user.get('reading_desk_state')` against AssertionError. Read 5 lines of context before/after to confirm. If the try/except wraps ONLY the storage read, simplify by removing it. If it wraps additional code (e.g., JSON parsing of the returned value), keep the outer wrapper but replace the inner storage call with `safe_user_get`.

**Step 3: Verify.**

```bash
grep -c "app\.storage\.user" web/pages/browse.py     # expect 0
grep -c "safe_user_get\|safe_user_set" web/pages/browse.py  # expect at least 4 (1 import + 3 reads + 1 write = 5)
python -c "import ast; ast.parse(open('web/pages/browse.py').read())"
ruff check web/pages/browse.py
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/pages/browse.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/pages/browse.py` returns 0
    - `grep -c "from web.safe_storage import" web/pages/browse.py` returns 1
    - `grep -c "safe_user_get\|safe_user_set" web/pages/browse.py` returns at least 4
    - `grep -c "'reading_desk_state'" web/pages/browse.py` returns at least 1 (key preserved)
    - `grep -c "'browse_export_data'" web/pages/browse.py` returns at least 1 (key preserved)
    - `grep -c "'show_translations'" web/pages/browse.py` returns at least 2 (both reads preserved)
    - File parses: `python -c "import ast; ast.parse(open('web/pages/browse.py').read())"` exits 0
    - `ruff check web/pages/browse.py` exits 0
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 preserved)
  </acceptance_criteria>
  <done>browse.py: 4 → 0 raw accesses; ruff happy; tests preserved.</done>
</task>

<task type="auto">
  <name>Task 2: Migrate web/pages/browse_state.py (11 sites + collapse inline try/except guards)</name>
  <read_first>
    - web/pages/browse_state.py FULL FILE (this is a small focused file — read it entirely; understand the 3 main functions: `restore_browse_snapshot`, `persist_browse_snapshot`, and the `_BROWSE_SNAPSHOT_VERSION` constant; per research and PATTERNS.md lines 126-130, 147-156, 174-203, 224 are the migration targets)
    - tests/test_browse_state.py (FULL FILE — this is the regression-test contract; 7+ tests per HANDOFF including pruned-session AssertionError handling; understand what the tests assert so your migration preserves the contract)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/pages/browse_state.py" section — has explicit before/after for the inline-guard pattern collapse)
  </read_first>
  <files>web/pages/browse_state.py</files>
  <action>
**File: `web/pages/browse_state.py`** — 11 raw access sites, all wrapped in inline `try: ... except (AssertionError, Exception): ...` guards that PREDATE the safe_storage module. This task replaces those inline guards with single-line helper calls.

**Step 1: Add safe_storage import.**

At the top of the file (after existing imports — check first with grep):
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

**Step 2: Read the file end-to-end and understand the structure.**

The file has three main functions per research:
- `restore_browse_snapshot(state)` — lines ~120-160 (reads from storage; returns the restored state or None)
- `persist_browse_snapshot(state, page)` — lines ~170-210 (writes to storage)
- `clear_browse_snapshot(state)` or similar cleanup at line ~224 (pops keys)

Map every raw access to its containing function before editing. Per research the line numbers are:
- 127: read `'browse_snapshot_schema_version'` (in restore)
- 137: write `'browse_snapshot_schema_version'` (somewhere — maybe a migration upgrade path)
- 147: read `'browse_position'` (in restore)
- 153: read `'reading_desk_state'` (in restore)
- 174: read `'session_persistence_enabled'` (in persist — gates whether to write)
- 180: write `'browse_snapshot_schema_version'` (in persist)
- 184: write `'browse_position'` (in persist)
- 197: write `'reading_desk_state'` (in persist, conditional on having reading-desk data)
- 203: pop `'reading_desk_state'` (in persist, when no reading-desk data)
- 224: pop arbitrary `key` (in clear or similar)

**Step 3: Replace each site.**

Approach: for EACH line listed, read the surrounding 5-10 lines of context and apply the pattern below.

| Site | Pattern to Apply |
|------|------------------|
| Read sites (127, 147, 153, 174) | Replace `try: x = app.storage.user.get(KEY, DEFAULT); except (AssertionError, Exception) as e: logger.debug(...); return ...` with `x = safe_user_get(KEY, DEFAULT)` and remove the surrounding try/except. If the outer return-on-failure is needed, retain it but key it off the value (e.g., `if x is None: return None`). |
| Write sites (137, 180, 184, 197) | Replace `try: app.storage.user[KEY] = VALUE; except Exception as e: logger.error(...)` with `safe_user_set(KEY, VALUE)`. Drop the outer try/except. |
| Pop sites (203, 224) | Replace `try: app.storage.user.pop(KEY, None); except Exception: pass` with `safe_user_pop(KEY, None)`. Drop the outer try/except. |

CONCRETE EXAMPLE for `restore_browse_snapshot` (lines 126-156 per research):

BEFORE:
```python
    try:
        stored_version = app.storage.user.get('browse_snapshot_schema_version', 0)
    except (AssertionError, Exception) as e:
        logger.debug(f"[BrowseSnapshot] user storage unavailable on restore: {e}")
        return (None, None)

    if stored_version != _BROWSE_SNAPSHOT_VERSION:
        # Migration upgrade path
        try:
            app.storage.user['browse_snapshot_schema_version'] = _BROWSE_SNAPSHOT_VERSION
        except (AssertionError, Exception):
            pass
        return (None, None)

    try:
        pos = app.storage.user.get('browse_position')
    except (AssertionError, Exception):
        return (None, None)

    try:
        desk = app.storage.user.get('reading_desk_state')
    except (AssertionError, Exception):
        desk = None
```

AFTER:
```python
    stored_version = safe_user_get('browse_snapshot_schema_version', 0)

    if stored_version != _BROWSE_SNAPSHOT_VERSION:
        safe_user_set('browse_snapshot_schema_version', _BROWSE_SNAPSHOT_VERSION)
        return (None, None)

    pos = safe_user_get('browse_position')
    if pos is None:
        return (None, None)

    desk = safe_user_get('reading_desk_state')
```

NOTE 1: The original returned `(None, None)` on AssertionError for the `pos` read. With `safe_user_get`, the helper returns `None` (the default) on AssertionError. So the explicit `if pos is None: return (None, None)` preserves the original semantics IF the caller distinguishes "key absent" from "storage unavailable" — and the existing code DID NOT distinguish (both branches returned `(None, None)`). So the migration is semantically equivalent.

NOTE 2: For the `desk` read, the original set `desk = None` on AssertionError; `safe_user_get(key)` returns `None` by default if key is missing OR storage is unavailable. Equivalent.

CONCRETE EXAMPLE for `persist_browse_snapshot` (lines 174-203 per research):

BEFORE:
```python
        if not app.storage.user.get('session_persistence_enabled', True):
            return
        try:
            app.storage.user['browse_snapshot_schema_version'] = _BROWSE_SNAPSHOT_VERSION
            if page is not None and state.sys_id:
                app.storage.user['browse_position'] = {...}
            if state.reading_desk_data:
                app.storage.user['reading_desk_state'] = {...}
            else:
                app.storage.user.pop('reading_desk_state', None)
        except Exception as e:
            logger.error(f"[BrowseSnapshot] Error persisting state: {e}")
```

AFTER:
```python
        if not safe_user_get('session_persistence_enabled', True):
            return
        safe_user_set('browse_snapshot_schema_version', _BROWSE_SNAPSHOT_VERSION)
        if page is not None and state.sys_id:
            safe_user_set('browse_position', {...})
        if state.reading_desk_data:
            safe_user_set('reading_desk_state', {...})
        else:
            safe_user_pop('reading_desk_state', None)
```

NOTE: Drop the outer try/except — each helper absorbs its own exception. The previous `logger.error` was a single combined error log; the helpers log individually at warning level for unexpected exceptions, which is equivalent observability.

**Step 4: Verify the tests still pass.**

The file `tests/test_browse_state.py` exists with 7 tests including pruned-session AssertionError handling. The migration MUST NOT break these tests. Run:
```bash
pytest tests/test_browse_state.py -x -v
```

Expected: all 7+ tests pass.

If a test fails, the migration changed semantics. Common pitfall: the original code had a path that wrote `app.storage.user.pop(key, None)` WITHOUT a default — `safe_user_pop(key, None)` preserves the default-None semantics. Verify by reading the test that fails to understand the assertion.

**Step 5: Final verification.**

```bash
grep -c "app\.storage\.user" web/pages/browse_state.py     # expect 0
grep -c "safe_user_get\|safe_user_set\|safe_user_pop" web/pages/browse_state.py  # expect at least 11 (1 import + 10 calls)
python -c "import ast; ast.parse(open('web/pages/browse_state.py').read())"
ruff check web/pages/browse_state.py
pytest tests/test_browse_state.py -x
```
  </action>
  <verify>
    <automated>pytest tests/test_browse_state.py -x</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/pages/browse_state.py` returns 0
    - `grep -c "from web.safe_storage import" web/pages/browse_state.py` returns 1
    - `grep -c "safe_user_get\|safe_user_set\|safe_user_pop" web/pages/browse_state.py` returns at least 11
    - `grep -c "'browse_snapshot_schema_version'" web/pages/browse_state.py` returns at least 2 (read + write preserved)
    - `grep -c "'browse_position'" web/pages/browse_state.py` returns at least 2 (read + write preserved)
    - `grep -c "'reading_desk_state'" web/pages/browse_state.py` returns at least 3 (read + write + pop preserved)
    - `grep -c "'session_persistence_enabled'" web/pages/browse_state.py` returns at least 1 (read preserved)
    - File parses: `python -c "import ast; ast.parse(open('web/pages/browse_state.py').read())"` exits 0
    - `ruff check web/pages/browse_state.py` exits 0
    - `pytest tests/test_browse_state.py -x` exits 0 (7+ tests pass)
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 preserved)
  </acceptance_criteria>
  <done>browse_state.py: 11 → 0 raw accesses; inline try/except guards collapsed; test_browse_state.py still green.</done>
</task>

<task type="auto">
  <name>Task 3: Migrate web/pages/catalog_browse.py (3 sites at lines 339, 954, 962)</name>
  <read_first>
    - web/pages/catalog_browse.py — read lines 335-345 (verify line 339 reads `'show_translations'`), lines 945-970 (verify lines 954, 962 write `'incoming_filters'` — likely 2 different conditional branches)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (if "web/pages/catalog_browse.py" section exists, follow it; otherwise use the generic settings.py pattern)
  </read_first>
  <files>web/pages/catalog_browse.py</files>
  <action>
**File: `web/pages/catalog_browse.py`** — 3 raw access sites.

**Step 1: Add safe_storage import.**

```python
from web.safe_storage import safe_user_get, safe_user_set
```

**Step 2: Migrate 3 sites.**

| Line | Current | Replace With |
|------|---------|--------------|
| 339 | `_show_cat_trans = app.storage.user.get('show_translations', False)` | `_show_cat_trans = safe_user_get('show_translations', False)` |
| 954 | `app.storage.user['incoming_filters'] = incoming` | `safe_user_set('incoming_filters', incoming)` |
| 962 | `app.storage.user['incoming_filters'] = incoming` | `safe_user_set('incoming_filters', incoming)` |

NOTE 1: Lines 954 and 962 write the same key from different branches. Don't try to dedupe them — preserve the branching structure. Just substitute the single line in each branch.

NOTE 2: Per Codex round 4 MEDIUM-2, the `incoming_filters` writes are cross-page handoff state — when a user clicks a catalog row, filters are written here so the destination page can read them. The `safe_user_set` wrapper is the correct migration target; behavior is equivalent.

**Step 3: Verify.**

```bash
grep -c "app\.storage\.user" web/pages/catalog_browse.py     # expect 0
grep -c "safe_user_get\|safe_user_set" web/pages/catalog_browse.py  # expect at least 3 (1 import + 3 sites = 4)
grep -c "'show_translations'" web/pages/catalog_browse.py    # expect at least 1
grep -c "'incoming_filters'" web/pages/catalog_browse.py     # expect at least 2 (both writes preserved)
python -c "import ast; ast.parse(open('web/pages/catalog_browse.py').read())"
ruff check web/pages/catalog_browse.py
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/pages/catalog_browse.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/pages/catalog_browse.py` returns 0
    - `grep -c "from web.safe_storage import" web/pages/catalog_browse.py` returns 1
    - `grep -c "safe_user_get\|safe_user_set" web/pages/catalog_browse.py` returns at least 3
    - `grep -c "'show_translations'" web/pages/catalog_browse.py` returns at least 1
    - `grep -c "'incoming_filters'" web/pages/catalog_browse.py` returns at least 2
    - File parses: `python -c "import ast; ast.parse(open('web/pages/catalog_browse.py').read())"` exits 0
    - `ruff check web/pages/catalog_browse.py` exits 0
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_browse_state.py -x` exits 0
  </acceptance_criteria>
  <done>catalog_browse.py: 3 → 0 raw accesses; ruff happy.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Page navigation → app.storage.user (browse_state, catalog_browse incoming_filters) | Cross-page handoff state; safe_user_set absorbs prune-race |
| Reading-desk restore on language switch (browse.py:1122) | Deferred callback path — exactly the v7.11.0 /browse 500 bug site; safe_user_get is the production fix |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| — | Denial of Service | /browse 500 on pruned session (the original v7.11.0 bug) | mitigate | All 18 raw sites in this plan now route through safe_storage helpers, which absorb AssertionError. The /browse 500 class of bug is closed at these specific sites. |
| T-87-04 | Tampering | Lint scanner pattern matching — N/A (no allowlist entries for these 3 files) | accept | All 3 files fully migrated; lint scanner finds zero violations |
| — | Tampering | browse_state schema_version migration path | accept | Schema-version write inside the migration upgrade branch is wrapped by safe_user_set; if the write fails (storage pruned), the next request will retry and converge. No data corruption possible. |
| — | Information disclosure | Cross-page incoming_filters carrying user-specific data | accept | Filters are per-session; storage is per-session; no cross-user leak introduced |

This plan does not directly mitigate T-87-01/02/03 (those are Plan 02's concern). The primary value is closing the prune-race DoS class at 18 specific code paths.
</threat_model>

<verification>
After all 3 tasks:

```bash
# Verify zero raw access in all 3 files
for f in web/pages/browse.py web/pages/browse_state.py web/pages/catalog_browse.py; do
  count=$(grep -c "app\.storage\.user" "$f")
  echo "$f: $count raw access (expect 0)"
done

# Verify all 3 files import safe_storage
for f in web/pages/browse.py web/pages/browse_state.py web/pages/catalog_browse.py; do
  count=$(grep -c "from web.safe_storage import" "$f")
  echo "$f: $count safe_storage imports (expect 1)"
done

# Verify files parse and pass ruff
python -c "
import ast
for f in ['web/pages/browse.py', 'web/pages/browse_state.py', 'web/pages/catalog_browse.py']:
    ast.parse(open(f).read(), filename=f)
print('All 3 files parse OK')
"
ruff check web/pages/browse.py web/pages/browse_state.py web/pages/catalog_browse.py

# Critical: tests/test_browse_state.py must pass (browse_state migration changes function bodies)
pytest tests/test_browse_state.py -x -v

# Plan 02 + Plan 03 + Plan 04 invariants preserved
pytest tests/test_safe_storage.py tests/test_session_uuid.py -x

# Lint scanner: verify no violations in these 3 files
pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist 2>&1 | grep -E "browse\.py|browse_state\.py|catalog_browse\.py" || echo "No violations in browse cluster"
```
</verification>

<success_criteria>
1. `web/pages/browse.py`: 0 raw access (was 4)
2. `web/pages/browse_state.py`: 0 raw access (was 11)
3. `web/pages/catalog_browse.py`: 0 raw access (was 3)
4. All 3 files import `safe_user_get` (and set/pop where needed) from `web.safe_storage`
5. `pytest tests/test_browse_state.py -x` passes (7+ tests)
6. `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` passes (Plan 02 invariant)
7. `ruff check` clean on all 3 files
8. Lint scanner reports zero violations in any of the 3 files
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-05-SUMMARY.md` summarizing:
- 3 files migrated: browse.py (4 sites), browse_state.py (11 sites + inline-guard collapse), catalog_browse.py (3 sites) = 18 total
- test_browse_state.py still green (7+ tests)
- Inline try/except guards removed from browse_state.py functions (count of removed guards)
- Phase 87 progress: 34 (plans 03-04) + 18 (this plan) = 52 sites migrated cumulatively
</output>
