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
  - tests/test_browse_state.py
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
    - "web/pages/browse_state.py raw accesses reduced from 11 to 0 — but only after defensive wrapper audit per M3"
    - "web/pages/catalog_browse.py raw accesses reduced from 3 to 0 (lines 339, 954, 962)"
    - "tests/test_browse_state.py monkeypatch targets updated to include web.safe_storage.app (B3 fix); all 7 existing tests still pass"
    - "restore_browse_snapshot preserves independent read semantics for browse_position and reading_desk_state per M2 — one being absent must NOT short-circuit the other"
    - "Plan 02 + Plan 03 invariants preserved"
  artifacts:
    - path: "web/pages/browse.py"
      provides: "Migrated 4 sites: reading_desk_state read (1122), browse_export_data write (1214), 2 show_translations reads (2080, 2115)"
      contains: "from web.safe_storage import"
    - path: "web/pages/browse_state.py"
      provides: "Migrated 11 sites in restore_browse_snapshot + persist_browse_snapshot + clear_browse_snapshot; defensive wrappers preserved per M3; independent read semantics preserved per M2"
      contains: "safe_user_get, safe_user_set, safe_user_pop"
    - path: "web/pages/catalog_browse.py"
      provides: "Migrated 3 sites: show_translations read (339), incoming_filters writes (954, 962)"
      contains: "safe_user_get"
    - path: "tests/test_browse_state.py"
      provides: "Monkeypatch updated to also patch web.safe_storage.app — required because browse_state.py reads/writes now go through safe_storage helpers (B3 fix)"
      contains: "patch('web.safe_storage.app')"
  key_links:
    - from: "web/pages/browse_state.py restore_browse_snapshot"
      to: "safe_user_get for schema_version + browse_position + reading_desk_state + session_persistence_enabled"
      via: "independent calls; one absent value does NOT short-circuit the other (M2)"
      pattern: "safe_user_get\\('browse_position'\\)"
    - from: "web/pages/browse.py reading desk restore"
      to: "safe_user_get('reading_desk_state')"
      via: "drop-in replacement at line 1122"
      pattern: "safe_user_get\\('reading_desk_state'\\)"
    - from: "tests/test_browse_state.py"
      to: "web.safe_storage.app"
      via: "B3 monkeypatch target update"
      pattern: "patch\\('web\\.safe_storage\\.app'\\)"
---

<objective>
Migrate the three browse-related files (`browse.py`, `browse_state.py`, `catalog_browse.py`) from raw `app.storage.user.*` access to `web.safe_storage` helpers. Total: 18 raw access sites migrated. ALSO update `tests/test_browse_state.py` to keep the existing 7 test cases working after the production code moves storage access through `web.safe_storage` (B3 fix).

**REVISION (B3, M1, M2, M3, M4 from 87-REVIEWS.md):**
- **B3 (BLOCKER):** `tests/test_browse_state.py` currently patches `web.pages.browse_state.app` (the module-level `from nicegui import app` import in browse_state.py). After this plan, browse_state.py's reads go through `safe_user_get` which itself reads from `web.safe_storage.app`. The existing monkeypatch would become a no-op and the tests would fail despite production code being correct. Fix: update the test file to ALSO patch `web.safe_storage.app` (or to patch ONLY `web.safe_storage.app` since browse_state.py no longer touches `app.storage.user` directly).
- **M2 (MEDIUM — snapshot semantics):** Plan 05 reads `browse_position` and `reading_desk_state` INDEPENDENTLY in `restore_browse_snapshot`. A missing `browse_position` MUST NOT short-circuit the `reading_desk_state` read. Before/after code example included in Task 2.
- **M3 (MEDIUM — defensive wrappers):** Some try/except blocks in browse_state.py catch JSON parsing or value-shape errors in addition to storage prune. Those wrappers are PRESERVED; only AssertionError-only wrappers are collapsed.
- **M1:** All acceptance criteria use `pytest tests/test_no_raw_storage_access.py` invocations, not grep.
- **M4:** Windows-safe Python one-liners throughout.

Purpose: The browse cluster is the page where the original v7.11.0 `/browse 500` bug surfaced (`AssertionError` on pruned session storage during reading-desk restore). The safe_storage module was created specifically to fix this class of bug; this plan completes the migration that `cca23db3` started.

Output: 3 production files migrated; 18 sites converted; existing `tests/test_browse_state.py` (7 tests) passes with updated monkeypatch.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md
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

<!-- Migration pattern for AssertionError-only try/except (Class A collapse): -->

Before:
```python
try:
    value = app.storage.user.get(key, default)
except AssertionError as e:
    logger.debug(f"...: {e}")
    return default_result
```

After:
```python
value = safe_user_get(key, default)
# (the outer "return default_result" path may need to remain as a check-on-value, NOT as except handler)
```

<!-- Migration pattern for parsing-error try/except (Class B preserve): -->

Before:
```python
try:
    raw = app.storage.user.get(key)
    parsed = json.loads(raw) if raw else {}
except (AssertionError, json.JSONDecodeError) as e:
    parsed = {}
```

After:
```python
try:
    raw = safe_user_get(key)
    parsed = json.loads(raw) if raw else {}
except json.JSONDecodeError as e:
    # AssertionError now absorbed by safe_user_get; only JSON failure remains
    parsed = {}
```

<!-- B3 monkeypatch fix template for tests/test_browse_state.py: -->

When browse_state.py uses safe_user_get/set/pop, the storage access happens
inside web.safe_storage. Tests must patch BOTH module-level `app` imports
(browse_state.py's own `app` for any remaining non-storage references, and
web.safe_storage.app for the storage calls). After Plan 05 migration,
browse_state.py has zero raw `app.storage.user` accesses — so patching
web.safe_storage.app is SUFFICIENT (the browse_state.app patch becomes
unnecessary but harmless).

```python
# Old (Plan-05-incompatible):
with patch('web.pages.browse_state.app') as mock_app:
    mock_app.storage.user = storage
    # ... call browse_state functions

# New (Plan 05 / B3 compatible):
with patch('web.safe_storage.app') as mock_safe_app:
    mock_safe_app.storage.user = storage
    # ... call browse_state functions
```

Note: tests that DON'T call browse_state functions (e.g., direct calls to
safe_storage helpers) should already use `web.safe_storage.app`. Tests in
tests/test_browse_state.py call browse_state functions which now route
through safe_storage — patch web.safe_storage.app.

Alternative (option B in the review): introduce a shared conftest fixture
that patches web.safe_storage.app. Chosen approach for this plan: direct
patch replacement in the 7 existing tests because the test file is small
and self-contained.
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Migrate web/pages/browse.py (4 sites)</name>
  <read_first>
    - web/pages/browse.py — read at minimum:
      - Top of file (find existing imports; check for any existing `from web.safe_storage import`)
      - Lines 1115-1135 (verify line 1122 reads `reading_desk_state`; apply M3 audit)
      - Lines 1205-1225 (verify line 1214 writes `browse_export_data`)
      - Lines 2075-2090 (verify line 2080 reads `show_translations`)
      - Lines 2110-2125 (verify line 2115 reads `show_translations` — different code path)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/pages/browse.py" section if present)
    - web/safe_storage.py (helper signatures reference)
  </read_first>
  <files>web/pages/browse.py</files>
  <action>
**File: `web/pages/browse.py`** — 4 raw access sites.

**Step 1: Add safe_storage import.**

Check for existing import (Windows-safe):
```
python -c "import re; print(re.search(r'from web\\.safe_storage import', open('web/pages/browse.py').read()))"
```

Add or extend:
```python
from web.safe_storage import safe_user_get, safe_user_set
```

**Step 2: Migrate 4 sites in DESCENDING line-number order (per L1 ordering principle).**

Order: 2115, 2080, 1214, 1122.

| Order | Line | Before | After |
|-------|------|--------|-------|
| 1 | 2115 | `_show_trans_browse = app.storage.user.get('show_translations', False)` | `_show_trans_browse = safe_user_get('show_translations', False)` |
| 2 | 2080 | `_browse_show_trans = app.storage.user.get('show_translations', False)` | `_browse_show_trans = safe_user_get('show_translations', False)` |
| 3 | 1214 | `app.storage.user['browse_export_data'] = export_data` | `safe_user_set('browse_export_data', export_data)` |
| 4 | 1122 | `saved = app.storage.user.get('reading_desk_state')` | `saved = safe_user_get('reading_desk_state')` |

**Apply M3 audit at line 1122:** Read 10 lines around line 1122. The current code may have a try/except wrapper that catches AssertionError. Likely BUT NOT GUARANTEED to also catch JSON-shape errors when iterating over `saved.entries`. Classify:

- Class A (collapse): if the except clause is `except AssertionError:` or `except Exception:` with body that just returns default. Replace and collapse.
- Class B (preserve): if there is additional parsing after the get (e.g., `saved = json.loads(...)` or `saved.get('entries', [])`) inside the same try. Keep the outer try but replace ONLY the storage call.

When in doubt, preserve the wrapper.

**Step 3: Verify (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/pages/browse.py').read()); print('parses OK')"
ruff check web/pages/browse.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/browse.py'), pathlib.Path('web/pages/browse.py').read_text(encoding='utf-8')); assert len(v) == 0, v; print('OK')"
```
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/browse.py'), pathlib.Path('web/pages/browse.py').read_text(encoding='utf-8')); assert len(v) == 0, v; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File parses: `python -c "import ast; ast.parse(open('web/pages/browse.py').read())"` exits 0
    - `ruff check web/pages/browse.py` exits 0
    - AST scanner reports 0 violations for browse.py (verified by `<verify>`)
    - safe_storage import present: verified via Python regex
    - Keys preserved (no accidental rename): `python -c "import re; src = open('web/pages/browse.py').read(); [print(k, 'count:', len(re.findall(rf\"'{k}'\", src))) for k in ['reading_desk_state', 'browse_export_data', 'show_translations']]"` — each ≥1
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0
  </acceptance_criteria>
  <done>browse.py: 4 sites migrated; 0 AST violations; M3 wrappers handled per audit.</done>
</task>

<task type="auto">
  <name>Task 2: Migrate web/pages/browse_state.py (11 sites) — PRESERVE M2 independent-read semantics + M3 defensive wrappers</name>
  <read_first>
    - web/pages/browse_state.py FULL FILE (this is a small focused file — read it entirely; understand the 3 main functions: `restore_browse_snapshot`, `persist_browse_snapshot`, `clear_browse_snapshot`)
    - tests/test_browse_state.py (FULL FILE — this is the regression-test contract; 7 tests; understand what each test asserts so your migration preserves the contract)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md ("web/pages/browse_state.py" section)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (M2 independent-read semantics; M3 defensive wrappers)
  </read_first>
  <files>web/pages/browse_state.py</files>
  <action>
**File: `web/pages/browse_state.py`** — 11 raw access sites. The original implementation uses inline `try: ... except (AssertionError, Exception): ...` guards that PREDATE the safe_storage module. This task replaces those guards but PRESERVES anything catching non-storage errors (M3) AND preserves independent-read semantics for `browse_position` vs `reading_desk_state` (M2).

**Step 1: Add safe_storage import.**

```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

**Step 2: Read the file end-to-end and build a site-to-pattern map.**

Run:
```
python -c "import re; src = open('web/pages/browse_state.py').read(); [print(i, line.strip()) for i, line in enumerate(src.splitlines(), start=1) if 'app.storage.user' in line]"
```

This gives you the 11 lines. Cross-reference with the line numbers in the research (127, 137, 147, 153, 174, 180, 184, 197, 203, 224) — line numbers may have drifted; trust the grep output.

**Step 3: M2 — Preserve independent-read semantics in `restore_browse_snapshot`.**

This is the MEDIUM finding from the cross-AI review. The original code reads `browse_position` and `reading_desk_state` as TWO SEPARATE reads, and the function returns a 2-tuple `(pos, desk)` where each can independently be None.

**The CORRECT migration (preserves M2 semantics):**

```python
def restore_browse_snapshot(state):
    """Restore browse position and reading desk from storage. Returns (pos, desk)."""
    stored_version = safe_user_get('browse_snapshot_schema_version', 0)

    if stored_version != _BROWSE_SNAPSHOT_VERSION and stored_version != 0:
        # Stale version — wipe both
        safe_user_pop('browse_position', None)
        safe_user_pop('reading_desk_state', None)
        safe_user_pop('browse_snapshot_schema_version', None)
        return (None, None)

    # M2: read browse_position and reading_desk_state INDEPENDENTLY.
    # Either can be present without the other. A missing browse_position must
    # NOT cause reading_desk_state to be returned as None — the test
    # `test_clear_snapshot_keep_position_preserves_position` exercises the
    # inverse case (position present, desk absent).
    pos = safe_user_get('browse_position')
    desk = safe_user_get('reading_desk_state')

    # Version stamp adoption: if stored_version == 0 (no stamp present) and we
    # have either pos or desk, adopt the current schema version (per the test
    # `test_missing_stamp_adopts_legacy_payload`).
    if stored_version == 0 and (pos is not None or desk is not None):
        safe_user_set('browse_snapshot_schema_version', _BROWSE_SNAPSHOT_VERSION)

    return (pos, desk)
```

**INCORRECT pattern (collapsed and broken — DO NOT do this):**

```python
def restore_browse_snapshot(state):
    pos = safe_user_get('browse_position')
    if pos is None:
        return (None, None)  # ← BUG: this short-circuits the desk read!
    desk = safe_user_get('reading_desk_state')
    return (pos, desk)
```

The bug above would cause `test_clear_snapshot_keep_position_preserves_position` to fail because it asserts that when position is present (and desk was just cleared), `pos != None` and `desk == None` — but the buggy code would short-circuit and return `(None, None)` if position were absent in a different test scenario. **Read both independently; let each return its own value.**

Read the CURRENT body of `restore_browse_snapshot` carefully. The original code at lines 127, 147, 153 likely already reads each key independently (just wrapped in separate try/except blocks). The migration substitutes the storage calls but preserves the independent-read structure.

**Step 4: M3 — Audit each try/except wrapper.**

For each of the 11 sites, classify the surrounding try/except:

- **Class A (collapse):** `except AssertionError:` or `except (AssertionError, Exception) as e: logger.debug(...); return default`. The wrapper only absorbs storage prune. Collapse to `safe_user_get(key, default)`.
- **Class B (preserve):** Any except clause catching json.JSONDecodeError, KeyError, ValueError, TypeError, AttributeError, or any combination not exclusively storage-related. Keep the wrapper but replace the inner storage call with the helper.

For `browse_state.py` specifically, the predominant pattern per research is Class A — the wrappers exist solely to absorb the prune AssertionError. Most will collapse cleanly.

**Step 5: Migrate each site.**

Approach: edit the file ONE FUNCTION AT A TIME, applying the M2/M3 rules.

**Specific Class B preservation site (Fix 4 in 87-REVIEWS.md iteration 3 — Codex MEDIUM M3 residual):** `persist_browse_snapshot()` at lines 162-205 (post-iter2 line numbers). The OUTER try-except at lines 173-178 wraps `app.storage.user.get('session_persistence_enabled', True)` — this is **Class A** (storage gate; will be replaced by `safe_user_get` and the wrapper collapses cleanly). The INNER try-except at lines 179-205 wraps the schema-version write, position write, reading-desk write, AND the pop fallback — these are **Class B** because the block also covers dict construction (the `{'sys_id': state.sys_id, ...}` and `[{'sys_id': e.get('sys_id', ''), ...} for e in state.reading_desk_entries]` expressions) and the conditional logic (`if page is not None and state.sys_id:` / `if state.view_joined and state.reading_desk_entries:`). **Do NOT collapse the inner try-except**; only replace the raw `app.storage.user[...]` calls inside it with `safe_user_set` / `safe_user_pop`. The `except Exception as e: logger.error(...)` remains as a safety net for dict-construction failures unrelated to storage prune.

CONCRETE EXAMPLE for `persist_browse_snapshot`:

BEFORE (per research, lines 174-203):
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

Analyze (revised per Fix 4): the OUTER try-except (lines 173-178, wrapping `session_persistence_enabled` get) is Class A — collapses to `safe_user_get(...)`. The INNER try-except (lines 179-205) is Class B — it wraps multi-key writes PLUS dict construction PLUS conditional logic. Preserve the inner wrapper:

AFTER (Class B inner try-except PRESERVED per Fix 4):
```python
        # Class A outer storage gate (collapses):
        if not safe_user_get('session_persistence_enabled', True):
            return
        # Class B inner try-except PRESERVED — it covers dict construction
        # and conditional logic, not just storage prune. Per Fix 4 in
        # 87-REVIEWS.md iteration 3 (Codex MEDIUM M3 residual).
        try:
            safe_user_set('browse_snapshot_schema_version', _BROWSE_SNAPSHOT_VERSION)
            if page is not None and state.sys_id:
                safe_user_set('browse_position', {
                    'sys_id': state.sys_id,
                    'p_num': getattr(page, 'p_num', 1),
                    'shelfmark': getattr(page, 'shelfmark', ''),
                    'volume_ie': state.volume_ie,
                })
            if state.view_joined and state.reading_desk_entries:
                rd_data = [
                    {'sys_id': e.get('sys_id', ''), 'shelfmark': e.get('shelfmark', '')}
                    for e in state.reading_desk_entries
                ]
                safe_user_set('reading_desk_state', {
                    'entries': rd_data,
                    'pgpid': state.joined_pgpid,
                    'selected_sources': state.reading_desk_selected_sources or {},
                })
            else:
                safe_user_pop('reading_desk_state', None)
        except Exception as e:
            logger.error(f"[BrowseSnapshot] Error persisting state: {e}")
```

**Step 6: Run the test file BEFORE updating it (intentional — Task 3 updates the monkeypatches).**

```
python -m pytest tests/test_browse_state.py -x -v
```

Expected: **some or all tests FAIL**. This is the B3 evidence — the existing tests patch `web.pages.browse_state.app`, which after migration is no longer the storage access point. Task 3 fixes the tests.

**Step 7: Verify (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/pages/browse_state.py').read()); print('parses OK')"
ruff check web/pages/browse_state.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/browse_state.py'), pathlib.Path('web/pages/browse_state.py').read_text(encoding='utf-8')); assert len(v) == 0, v; print('OK')"
```
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/browse_state.py'), pathlib.Path('web/pages/browse_state.py').read_text(encoding='utf-8')); assert len(v) == 0, v; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File parses: `python -c "import ast; ast.parse(open('web/pages/browse_state.py').read())"` exits 0
    - `ruff check web/pages/browse_state.py` exits 0
    - AST scanner reports 0 violations for browse_state.py (verified by `<verify>`)
    - safe_storage import present
    - **M2 preserved:** `restore_browse_snapshot` reads `browse_position` and `reading_desk_state` via SEPARATE `safe_user_get` calls (not nested or short-circuited). Manual verification by reading the function body: `python -c "import re; src = open('web/pages/browse_state.py').read(); m = re.search(r'def restore_browse_snapshot.*?(?=\\ndef )', src, re.DOTALL); body = m.group(0); assert \"safe_user_get('browse_position')\" in body and \"safe_user_get('reading_desk_state')\" in body; print('OK')"` prints `OK`
    - Schema keys preserved: `python -c "import re; src = open('web/pages/browse_state.py').read(); keys = ['browse_snapshot_schema_version', 'browse_position', 'reading_desk_state', 'session_persistence_enabled']; missing = [k for k in keys if f\"'{k}'\" not in src]; assert not missing, missing; print('OK')"` prints `OK`
    - **M3 audit recorded:** SUMMARY notes which try/except blocks were collapsed (Class A) vs preserved (Class B), counted per function
  </acceptance_criteria>
  <done>browse_state.py: 0 AST violations; M2 independent-read semantics preserved; M3 defensive wrappers audited and handled.</done>
</task>

<task type="auto">
  <name>Task 3: Update tests/test_browse_state.py monkeypatches to web.safe_storage.app (B3 BLOCKER fix)</name>
  <read_first>
    - tests/test_browse_state.py (FULL FILE — read all 7 tests; identify every `patch('web.pages.browse_state.app')` site)
    - web/pages/browse_state.py (AFTER Task 2 — verify it no longer has any `app.storage.user` text and now uses `safe_user_get/set/pop`)
    - tests/test_search_state.py lines 159-170 (REFERENCE — `test_stale_version_discards_snapshot` already patches BOTH `web.pages.search_state.app` AND `web.safe_storage.app`. Same pattern applies here.)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (B3 description)
  </read_first>
  <files>tests/test_browse_state.py</files>
  <action>
**B3 BLOCKER FIX:** Update `tests/test_browse_state.py` so its monkeypatches work against the migrated production code.

After Task 2, `web/pages/browse_state.py` reads from / writes to `app.storage.user` ONLY via `web.safe_storage.safe_user_*` helpers. The helpers in `web/safe_storage.py` use `app.storage.user.get(key)` etc., where `app` is the module-level import in `web.safe_storage`. So tests must patch `web.safe_storage.app`.

The current test file patches `web.pages.browse_state.app` — after Task 2, browse_state.py still has `from nicegui import app` (likely) but doesn't access `.storage.user` through it directly. Patching `browse_state.app` has no effect on storage reads anymore.

**Step 1: Survey existing patches.**

Windows-safe:
```
python -c "import re; src = open('tests/test_browse_state.py').read(); [print(i, line.strip()) for i, line in enumerate(src.splitlines(), start=1) if 'patch(' in line]"
```

Expected: 7 occurrences of `patch('web.pages.browse_state.app')`.

**Step 2: Replace each patch target.**

For EACH `patch('web.pages.browse_state.app')` occurrence, replace it with `patch('web.safe_storage.app')`. The variable name `mock_app` and the inner `mock_app.storage.user = storage` line do NOT change — only the patch target string changes.

CONCRETE EXAMPLE:

BEFORE:
```python
def test_missing_stamp_adopts_legacy_payload():
    storage = {
        'browse_position': {...},
        'reading_desk_state': {...},
    }
    with patch('web.pages.browse_state.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.browse_state import BrowseState, restore_browse_snapshot
        pos, desk = restore_browse_snapshot(BrowseState())
        ...
```

AFTER:
```python
def test_missing_stamp_adopts_legacy_payload():
    storage = {
        'browse_position': {...},
        'reading_desk_state': {...},
    }
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.browse_state import BrowseState, restore_browse_snapshot
        pos, desk = restore_browse_snapshot(BrowseState())
        ...
```

Apply this substitution to all 7 occurrences.

**Step 3 (defensive — handle dual-patch test if any).**

Some tests may patch BOTH `browse_state.app` AND `safe_storage.app` (as `test_search_state.py:test_stale_version_discards_snapshot` already does). If so, keep both patches or simplify to just `safe_storage.app`. Either works; pick the simpler form.

**Step 4: Run the test file.**

```
python -m pytest tests/test_browse_state.py -x -v
```

Expected: **all 7 tests pass**.

Common failure modes:
- If tests still fail with AssertionError about storage not being available: confirm browse_state.py's migration removed ALL `app.storage.user` references.
- If a test fails on a specific assertion about returned values: check whether M2 semantics were preserved in Task 2 (the test you'd expect to break first is `test_clear_snapshot_keep_position_preserves_position`).
- If a test fails on `mock_app.storage.tab` not being set: browse_state.py might also use `app.storage.tab` (verify; if so, this plan needs a small extension to also wrap tab storage, OR keep `web.pages.browse_state.app` patched in tests that use tab storage).

**Step 5: Verify test count and patch target consistency.**

```
python -c "import re; src = open('tests/test_browse_state.py').read(); print('test functions:', len(re.findall(r'^def test_', src, re.MULTILINE))); print('patch(safe_storage.app):', len(re.findall(r\"patch\\('web\\.safe_storage\\.app'\\)\", src))); print('patch(browse_state.app):', len(re.findall(r\"patch\\('web\\.pages\\.browse_state\\.app'\\)\", src)))"
```

Expected: 7 test functions, 7 patches to web.safe_storage.app, 0 patches to web.pages.browse_state.app.

(If any test patches both — for tab storage reasons — the count of `web.pages.browse_state.app` patches might be >0. Document in SUMMARY.)
  </action>
  <verify>
    <automated>python -m pytest tests/test_browse_state.py -x</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest tests/test_browse_state.py -x` exits 0 (all 7 tests pass)
    - The test file has 7 `def test_*` functions: verified via Python regex
    - All `patch('web.pages.browse_state.app')` occurrences are EITHER replaced with `patch('web.safe_storage.app')` OR retained alongside an additional `patch('web.safe_storage.app')` patch (for tests using tab storage). Verified by ensuring at least 7 `patch('web.safe_storage.app')` occurrences exist OR the test passes (functional gate)
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariants preserved)
    - `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x` exits 0 (Plan 01 standalone tests still pass)
  </acceptance_criteria>
  <done>test_browse_state.py: 7 tests pass after monkeypatch update; B3 BLOCKER closed.</done>
</task>

<task type="auto">
  <name>Task 4: Migrate web/pages/catalog_browse.py (3 sites at lines 339, 954, 962)</name>
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

**Step 2: Migrate 3 sites in DESCENDING line-number order.**

| Order | Line | Before | After |
|-------|------|--------|-------|
| 1 | 962 | `app.storage.user['incoming_filters'] = incoming` | `safe_user_set('incoming_filters', incoming)` |
| 2 | 954 | `app.storage.user['incoming_filters'] = incoming` | `safe_user_set('incoming_filters', incoming)` |
| 3 | 339 | `_show_cat_trans = app.storage.user.get('show_translations', False)` | `_show_cat_trans = safe_user_get('show_translations', False)` |

NOTE 1: Lines 954 and 962 write the same key from different branches. Don't dedupe — preserve the branching structure.

NOTE 2: Per Codex round 4 MEDIUM-2, the `incoming_filters` writes are cross-page handoff state — when a user clicks a catalog row, filters are written here so the destination page can read them. The `safe_user_set` wrapper is the correct migration target; behavior is equivalent.

**M3 audit:** Reading 5 lines around each site to check for wrappers; collapse Class A, preserve Class B.

**Step 3: Verify (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/pages/catalog_browse.py').read()); print('parses OK')"
ruff check web/pages/catalog_browse.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/catalog_browse.py'), pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')); assert len(v) == 0, v; print('OK')"
```
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/catalog_browse.py'), pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')); assert len(v) == 0, v; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File parses
    - `ruff check web/pages/catalog_browse.py` exits 0
    - AST scanner reports 0 violations (verified by `<verify>`)
    - safe_storage import present
    - Keys preserved: `'show_translations'` ≥1, `'incoming_filters'` ≥2
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_browse_state.py -x` exits 0
  </acceptance_criteria>
  <done>catalog_browse.py: 3 sites migrated; 0 AST violations.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Page navigation -> app.storage.user (browse_state, catalog_browse incoming_filters) | Cross-page handoff state; safe_user_set absorbs prune-race |
| Reading-desk restore on language switch (browse.py:1122) | Deferred callback path — exactly the v7.11.0 /browse 500 bug site; safe_user_get is the production fix |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| -- | Denial of Service | /browse 500 on pruned session (the original v7.11.0 bug) | mitigate | All 18 raw sites in this plan now route through safe_storage helpers, which absorb AssertionError. The /browse 500 class of bug is closed at these specific sites. |
| T-87-04 | Tampering | Lint scanner pattern matching — N/A (no allowlist entries for these 3 files) | accept | All 3 files fully migrated; lint scanner finds zero violations |
| -- | Tampering | browse_state schema_version migration path | accept | Schema-version write inside the migration upgrade branch is wrapped by safe_user_set; if the write fails (storage pruned), the next request will retry and converge. No data corruption possible. |
| -- | Information disclosure | Cross-page incoming_filters carrying user-specific data | accept | Filters are per-session; storage is per-session; no cross-user leak introduced |
| -- | Test integrity (B3) | Monkeypatch target drift after migration | mitigate | Task 3 explicitly updates test_browse_state.py to patch the new storage access point. Without this, tests would pass-trivially or fail noisily — both are bad signals. |
| -- | Data semantics (M2) | Snapshot restore short-circuit bug | mitigate | Task 2 includes explicit before/after code showing the M2-correct pattern; acceptance criteria verify both keys are read independently |

This plan does not directly mitigate T-87-01/02/03 (Plan 02's concern). Primary value: closing the prune-race DoS class at 18 specific code paths AND preserving test fidelity.
</threat_model>

<verification>
After all 4 tasks (Windows-safe):

```
# Verify zero violations in all 3 production files
python -c "
import sys, pathlib
sys.path.insert(0, '.')
from tests.test_no_raw_storage_access import _scan_file
for f in ['web/pages/browse.py', 'web/pages/browse_state.py', 'web/pages/catalog_browse.py']:
    v = _scan_file(pathlib.Path(f), pathlib.Path(f).read_text(encoding='utf-8'))
    print(f, 'violations:', len(v))
    assert len(v) == 0, v
print('OK')
"

# Verify all 3 files import safe_storage
python -c "
import re
for f in ['web/pages/browse.py', 'web/pages/browse_state.py', 'web/pages/catalog_browse.py']:
    has = bool(re.search(r'from web\\.safe_storage import', open(f).read()))
    print(f, has)
    assert has, f
"

# Verify files parse and pass ruff
python -c "
import ast
for f in ['web/pages/browse.py', 'web/pages/browse_state.py', 'web/pages/catalog_browse.py']:
    ast.parse(open(f).read(), filename=f)
print('All 3 production files parse OK')
"
ruff check web/pages/browse.py web/pages/browse_state.py web/pages/catalog_browse.py

# Critical (B3): tests/test_browse_state.py must pass after monkeypatch update
python -m pytest tests/test_browse_state.py -x -v

# Plan 01 + 02 invariants preserved
python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x
python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x
```
</verification>

<success_criteria>
1. `web/pages/browse.py`: 0 AST violations (was 4)
2. `web/pages/browse_state.py`: 0 AST violations (was 11); M2 independent-read semantics preserved
3. `web/pages/catalog_browse.py`: 0 AST violations (was 3)
4. **B3:** `tests/test_browse_state.py` monkeypatches updated; all 7 tests pass against migrated production code
5. All 3 production files import safe_user_get (and set/pop where needed)
6. `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` passes (Plan 02 invariant)
7. `ruff check` clean on all 3 production files
8. M3 audit recorded: per-file note of which try/except blocks were collapsed vs preserved
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-05-SUMMARY.md` summarizing:
- 3 production files migrated: browse.py (4 sites), browse_state.py (11 sites), catalog_browse.py (3 sites) = 18 total
- 1 test file updated: tests/test_browse_state.py — 7 monkeypatch sites swapped from `web.pages.browse_state.app` to `web.safe_storage.app` (B3 fix)
- test_browse_state.py: 7/7 tests pass after update
- **M2 verification:** restore_browse_snapshot reads browse_position and reading_desk_state independently — confirmed by reading function body and by the persisting test_clear_snapshot_keep_position_preserves_position
- **M3 audit per file:** list defensive wrappers preserved (with brief why-it-catches-non-storage rationale)
- **B3 verification:** new monkeypatch target works; production code's path through safe_storage is exercised by tests
- Phase 87 progress: 34 (plans 03-04) + 18 (this plan) = 52 sites migrated cumulatively
</output>
