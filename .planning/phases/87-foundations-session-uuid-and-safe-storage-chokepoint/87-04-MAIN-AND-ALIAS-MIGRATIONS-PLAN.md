---
phase: 87
plan: 04
type: execute
wave: 2
depends_on: [87-02, 87-03]
files_modified:
  - web/main.py
  - web/api.py
  - web/supabase_client.py
autonomous: true
requirements:
  - FOUND-02
tags:
  - phase87
  - migration
  - safe-storage
  - main-app
  - aliases
  - oauth-allowlist
must_haves:
  truths:
    - "web/main.py raw accesses reduced from 17 to exactly 3 (the 3 OAuth callback writes at lines 1458, 1460, 1463 which are allowlisted in Phase 87)"
    - "web/main.py local helpers _safe_user_storage_get and set_current_page DELETED — but ONLY after all 14 call-site migrations land first (L1 ordering)"
    - "web/api.py raw accesses reduced from 3 (nicegui_app aliased) to 0"
    - "web/supabase_client.py raw accesses reduced from 1 (the line 263 _app aliased sign_out site) to 0; line 111 still raw (allowlisted as Phase 90 deletes the entire get_user_client function)"
    - "AST scanner reports zero non-allowlisted violations across these 3 files (verified via pytest, not grep — per M1)"
    - "Plan 02's bootstrap call ensure_session_uuid() in create_layout() is PRESERVED — Plan 04 must not accidentally remove it"
  artifacts:
    - path: "web/main.py"
      provides: "Migrated 14 sites (lines 327, 493, 567, 587, 598, 657, 663, 664, 691, 820, 952-in-helper-body, 960-in-helper-body, 968, 1283); deleted 2 local helpers AFTER migrations; OAuth 3-key write at 1458-1463 left raw (allowlisted)"
      contains: "from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop"
    - path: "web/api.py"
      provides: "Migrated 3 sites at lines 1932, 1968, 2073 (all using nicegui_app alias)"
      contains: "safe_user_get"
    - path: "web/supabase_client.py"
      provides: "Migrated 1 site at line 263 (sign_out using _app alias); line 111 retained for Phase 90 deletion"
      contains: "safe_user_get"
  key_links:
    - from: "web/main.py callers of _safe_user_storage_get"
      to: "web.safe_storage.safe_user_get"
      via: "import statement at module top"
      pattern: "from web\\.safe_storage import"
    - from: "web/api.py:1932,1968,2073"
      to: "safe_user_get"
      via: "alias resolution — nicegui_app removed from these read sites"
      pattern: "safe_user_get\\('parallels_source_text'"
---

<objective>
Migrate `web/main.py` (14 of 17 raw sites; 3 OAuth sites stay allowlisted), `web/api.py` (all 3 alias-bearing sites), and `web/supabase_client.py` (1 of 2 sites; line 111 stays allowlisted) to use `web.safe_storage` helpers. Also consolidate `web/main.py`'s 2 local duplicate helpers (`_safe_user_storage_get` + `set_current_page`) by deleting them and routing all callers through the chokepoint module.

**REVISION (L1, M1, M4 from 87-REVIEWS.md):**
- **L1 (LOW — ordering):** Plan 04 now ENFORCES migrate-before-delete order. All 14 raw-site migrations in `web/main.py` happen FIRST; the deletion of `_safe_user_storage_get` and `set_current_page` is the FINAL step of Task 1. This reduces off-by-one risk because deleting functions mid-way through line-numbered edits shifts every subsequent line number.
- **L1 additional:** When applying line-numbered migrations within a single file, work in DESCENDING line-number order (line 1283 first, then 968, 820, ..., 327 last). This way prior edits don't shift the line numbers of later edits.
- **M1:** All acceptance criteria use `pytest tests/test_no_raw_storage_access.py` invocations instead of `grep -c` gates.
- **M4:** All shell snippets use Windows-safe Python one-liners. No `/tmp`, POSIX-only.
- **Plan 02 B1 wiring preservation:** Plan 02 added `ensure_session_uuid()` to `create_layout()`. Plan 04 must NOT accidentally delete this line during its own edits to main.py.

Purpose: These are the "central" files of the web app — main entry, API endpoints, and Supabase glue. Two of them (`api.py`, `supabase_client.py`) use non-standard aliases (`nicegui_app`, `_app`) which research R-02 and Pitfall 1 flagged as the grep-misses risk. After this plan, only the OAuth callback (Phase 91) and the get_user_client cache (Phase 90) retain raw access — both deliberately allowlisted.

Output: 3 files modified; 18 raw access sites migrated (14 + 3 + 1); 2 local helpers deleted (AFTER migrations); allowlist invariant preserved; Plan 02's bootstrap call preserved.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md
@.planning/phase87_storage_allowlist.yaml
@web/safe_storage.py
@web/main.py
@web/api.py
@web/supabase_client.py

<interfaces>
<!-- Already-available safe_storage API (Plan 02 landed): -->

```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop, get_session_uuid, ensure_session_uuid
```

<!-- Current main.py local helpers (TO BE DELETED AS THE LAST STEP OF TASK 1 — per L1): -->

Lines 949-957 (`_safe_user_storage_get`):
```python
def _safe_user_storage_get(key: str, default=None):
    """Safely read from app.storage.user, returning default if session not ready."""
    try:
        return app.storage.user.get(key, default)
    except (AssertionError, KeyError, Exception):
        return default
```

Lines 959-963 (`set_current_page`):
```python
def set_current_page(page_path: str):
    """Safely set the current page in user storage."""
    try:
        app.storage.user['current_page'] = page_path
    except (AssertionError, KeyError, Exception):
        pass
```

<!-- Plan 02 B1 wiring — DO NOT REMOVE: -->

In `create_layout()` (line ~342):
```python
def create_layout():
    """Create the main application layout with modern Header and Sidebar."""
    # Phase 87 FOUND-01 (B1 in 87-REVIEWS.md): mint _session_uuid on first
    # page render of every session. ensure_session_uuid() is idempotent and
    # returns False harmlessly on prune-race. Downstream code (Phases 88+)
    # can rely on _session_uuid being present in storage after this point.
    ensure_session_uuid()
    ...
```

The `from web.safe_storage import ensure_session_uuid` line added by Plan 02 must remain. Plan 04 EXTENDS that import to also include safe_user_get/set/pop.

<!-- Allowlisted (DO NOT migrate in this plan): -->

`web/main.py:1458-1463` — OAuth callback's 3-key atomic write (already in allowlist YAML; Phase 91 AUTHW-02 will migrate)
`web/supabase_client.py:111` — get_user_client captured-handle (already in allowlist YAML; Phase 90 AUTHC-01 deletes the function)
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Migrate web/main.py — 14 raw sites in DESCENDING line-number order, THEN delete 2 local helpers, preserving OAuth allowlist AND Plan 02 bootstrap wiring (L1 ordering)</name>
  <read_first>
    - web/main.py lines 320-340 (verify line 327 read of 'ui_language')
    - web/main.py lines 485-505 (verify line 493 write of 'ui_language')
    - web/main.py lines 560-605 (verify lines 567, 587, 598 — whats_new_dismissed writes and drawer_open read+write combo)
    - web/main.py lines 650-700 (verify lines 657, 663, 664, 691 — show_translations + theme)
    - web/main.py lines 815-830 (verify line 820 — current_theme read)
    - web/main.py lines 945-980 (verify the LOCAL HELPERS at 949-962 + the line 968 raw read inside set_current_page or nearby)
    - web/main.py lines 1275-1295 (verify line 1283 pop)
    - web/main.py lines 1450-1470 (CRITICAL — verify the OAuth callback at 1458, 1460, 1463 to KEEP RAW; allowlisted)
    - web/main.py lines 340-360 (CRITICAL — verify Plan 02's `ensure_session_uuid()` call exists in create_layout() and DO NOT remove it)
    - .planning/phase87_storage_allowlist.yaml (confirm the web/main.py entry's 3 patterns match the OAuth callback strings)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/main.py" section for the migration table)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (L1 ordering rationale)
  </read_first>
  <files>web/main.py</files>
  <action>
**File: `web/main.py`** — 17 raw sites; migrate 14 (delete 2 local helpers + 12 inline sites); keep 3 OAuth sites raw (allowlisted).

**L1 ORDERING DIRECTIVE (87-REVIEWS.md L1):**

The natural temptation is to delete the local helpers first (since they conceptually go away). Resist it. Instead, follow this order:

1. **Step 1:** Extend the safe_storage import (Plan 02 already added `ensure_session_uuid`; this step adds `safe_user_get/set/pop`).
2. **Step 2:** Migrate all 14 INLINE raw sites in DESCENDING line-number order (1283 first, then 968, 820, 691, 664, 663, 657, 598, 587, 567, 493, 327; the 952 and 960 sites are INSIDE the local helpers and will disappear in Step 4).
3. **Step 3:** Replace ALL CALL SITES of `_safe_user_storage_get(...)` and `set_current_page(...)` with the corresponding `safe_user_get/set` calls.
4. **Step 4 (LAST):** Delete the two helper function definitions at lines 949-962.

Reason: line numbers shift when functions are deleted. If we deleted at Step 1, every subsequent line number ≥963 would shift up by ~14 lines, breaking the line-by-line migration map. Descending order in Step 2 also prevents shift-on-edit issues.

**Step 1: Extend the safe_storage import.**

Plan 02 already added:
```python
from web.safe_storage import ensure_session_uuid
```

Extend this to include the three helpers used in migration:
```python
from web.safe_storage import (
    ensure_session_uuid,
    safe_user_get,
    safe_user_set,
    safe_user_pop,
)
```

(Or keep it on one line if it fits; the parenthesized form is just easier to extend later. Either is acceptable.)

**Step 2: Migrate the 12 INLINE raw sites in DESCENDING line-number order.**

DO each in this order; verify each edit by reading the resulting file region before moving to the next. The line numbers are PRE-migration (current state of web/main.py); they will NOT shift if we work in descending order.

| Order | Line | Before | After |
|-------|------|--------|-------|
| 1 | 1283 | `app.storage.user.pop(key, None)` | `safe_user_pop(key, None)` |
| 2 | 968 | `current_theme = app.storage.user.get('theme', 'light')` | `current_theme = safe_user_get('theme', 'light')` |
| 3 | 820 | `current_theme = app.storage.user.get('theme', 'light')` | `current_theme = safe_user_get('theme', 'light')` |
| 4 | 691 | `app.storage.user['theme'] = theme_name` | `safe_user_set('theme', theme_name)` |
| 5 | 664 | `app.storage.user['show_translations'] = not current` | `safe_user_set('show_translations', not current)` |
| 6 | 663 | `current = app.storage.user.get('show_translations', False)` | `current = safe_user_get('show_translations', False)` |
| 7 | 657 | `show_translations = app.storage.user.get('show_translations', False)` | `show_translations = safe_user_get('show_translations', False)` |
| 8 | 598 | `app.storage.user['drawer_open'] = not app.storage.user.get('drawer_open', True)` | `safe_user_set('drawer_open', not safe_user_get('drawer_open', True))` |
| 9 | 587 | `app.storage.user['whats_new_dismissed'] = WHATS_NEW_VERSION` | `safe_user_set('whats_new_dismissed', WHATS_NEW_VERSION)` |
| 10 | 567 | `app.storage.user['whats_new_dismissed'] = WHATS_NEW_VERSION` | `safe_user_set('whats_new_dismissed', WHATS_NEW_VERSION)` |
| 11 | 493 | `app.storage.user['ui_language'] = new_lang` | `safe_user_set('ui_language', new_lang)` |
| 12 | 327 | `saved_lang = app.storage.user.get('ui_language')` | `saved_lang = safe_user_get('ui_language')` |

After EACH edit, re-read 5 lines of context to confirm the edit applied cleanly and no unintended text was changed.

**Step 3: Replace callers of `_safe_user_storage_get` and `set_current_page`.**

Find all call sites (Windows-safe):
```
python -c "import re; src = open('web/main.py').read(); [print(i, line) for i, line in enumerate(src.splitlines(), start=1) if '_safe_user_storage_get(' in line or 'set_current_page(' in line]"
```

Expected callers (from research):
- `_safe_user_storage_get('current_page', '/')` → replace with `safe_user_get('current_page', '/')` (likely at line 348 inside create_layout())
- `set_current_page(some_path)` → replace with `safe_user_set('current_page', some_path)`

Replace every caller. The new `safe_user_set('current_page', ...)` is equivalent in behavior — both swallow AssertionError, both return cleanly.

**CRITICAL preservation check:** After replacing the caller at line 348 (inside `create_layout()`), the function should now look like:
```python
def create_layout():
    """Create the main application layout with modern Header and Sidebar."""
    # Phase 87 FOUND-01 (B1 in 87-REVIEWS.md): mint _session_uuid on first
    # page render of every session. ensure_session_uuid() is idempotent and
    # returns False harmlessly on prune-race. Downstream code (Phases 88+)
    # can rely on _session_uuid being present in storage after this point.
    ensure_session_uuid()

    resolved_lang = _resolve_ui_language()
    set_language(resolved_lang)

    current_page = safe_user_get('current_page', '/')    # ← migrated from _safe_user_storage_get
    rtl_mode = resolved_lang == 'he'
    ...
```

Verify the `ensure_session_uuid()` line is still present.

**Step 4 (LAST): Delete the two local helper function definitions.**

Now that all callers have been redirected, delete the function definitions at lines 949-962:
- `def _safe_user_storage_get(...)` and its body (lines 949-957)
- `def set_current_page(...)` and its body (lines 959-963 or similar — verify exact boundaries by reading)

After deletion, preserve the surrounding blank-line spacing. Don't accidentally merge unrelated functions.

**Step 5: DO NOT touch lines 1458, 1460, 1463 (OAuth callback).**

Verify these 3 lines remain unchanged after all your edits:
```python
app.storage.user[GlobalAuthState.USER_KEY] = user
app.storage.user[GlobalAuthState.PROFILE_KEY] = profile
app.storage.user['auth_session'] = {
```

These are allowlisted per `.planning/phase87_storage_allowlist.yaml` for atomic OAuth-callback semantics. Phase 91 AUTHW-02 migrates them.

**Step 6: Verify (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/main.py').read()); print('parses OK')"
ruff check web/main.py
python -c "import re; src = open('web/main.py').read(); print('helpers deleted:', not re.search(r'^def _safe_user_storage_get', src, re.MULTILINE) and not re.search(r'^def set_current_page', src, re.MULTILINE))"
python -c "import re; src = open('web/main.py').read(); print('callers removed:', not re.search(r'_safe_user_storage_get\\(', src) and not re.search(r'set_current_page\\(', src))"
python -c "import re; src = open('web/main.py').read(); body = src[src.index('def create_layout()'):src.index('def create_layout()')+800]; assert 'ensure_session_uuid()' in body, 'B1 wiring lost!'; print('B1 wiring preserved')"
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file, _is_allowlisted, _load_allowlist; src = pathlib.Path('web/main.py').read_text(encoding='utf-8'); v = _scan_file(pathlib.Path('web/main.py'), src); allowed = {e['file']: e for e in _load_allowlist().get('allowed_raw_access', [])}; unallowed = [f'{lno}: {seg}' for lno, seg in v if not _is_allowlisted('web/main.py', seg, allowed)]; assert not unallowed, unallowed; print(f'OK: {len(v)} raw accesses, all allowlisted (the 3 OAuth sites)')"
```
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file, _is_allowlisted, _load_allowlist; src = pathlib.Path('web/main.py').read_text(encoding='utf-8'); v = _scan_file(pathlib.Path('web/main.py'), src); allowed = {e['file']: e for e in _load_allowlist().get('allowed_raw_access', [])}; unallowed = [f'{lno}: {seg}' for lno, seg in v if not _is_allowlisted('web/main.py', seg, allowed)]; assert not unallowed, unallowed; print(f'OK: {len(v)} raw accesses, all allowlisted')"</automated>
  </verify>
  <acceptance_criteria>
    - `web/main.py` parses: `python -c "import ast; ast.parse(open('web/main.py').read())"` exits 0
    - `ruff check web/main.py` exits 0
    - AST scanner reports raw accesses, but ALL of them match the OAuth allowlist patterns (verified by `<verify>` block — `unallowed` list is empty)
    - The 3 remaining raw accesses are the OAuth callback's 3 keys (USER_KEY, PROFILE_KEY, 'auth_session')
    - Local helpers DELETED: `python -c "import re; src = open('web/main.py').read(); assert not re.search(r'^def _safe_user_storage_get', src, re.MULTILINE); assert not re.search(r'^def set_current_page', src, re.MULTILINE); print('OK')"` prints `OK`
    - No callers of deleted helpers remain: `python -c "import re; src = open('web/main.py').read(); assert '_safe_user_storage_get(' not in src and 'set_current_page(' not in src; print('OK')"` prints `OK`
    - `from web.safe_storage import` line in main.py contains all 4 needed names (ensure_session_uuid + safe_user_get + safe_user_set + safe_user_pop): verified by Python regex
    - **Plan 02 B1 wiring preserved**: `ensure_session_uuid()` is still called inside `create_layout()`. Verify with `python -c "import re; src = open('web/main.py').read(); body = src[src.index('def create_layout()'):src.index('def create_layout()')+800]; assert 'ensure_session_uuid()' in body; print('OK')"` prints `OK`
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (16 tests pass — Plan 02 invariant preserved)
  </acceptance_criteria>
  <done>main.py: 14 sites migrated (12 inline + 2 helper call-site batches); 2 helpers deleted (AFTER migrations); 3 OAuth sites left raw and allowlisted; B1 wiring preserved.</done>
</task>

<task type="auto">
  <name>Task 2: Migrate web/api.py (3 nicegui_app alias sites at lines 1932, 1968, 2073)</name>
  <read_first>
    - web/api.py — at minimum read these regions:
      - Top of file (find the `from nicegui import app as nicegui_app` import line; check for any existing `from web.safe_storage import`)
      - Lines 1925-1940 (verify line 1932 read of parallels_source_text)
      - Lines 1960-1975 (verify line 1968 read — identical pattern)
      - Lines 2060-2080 (verify line 2073 read — same key, different code path)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/api.py" section)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (Pitfall 1 — the nicegui_app alias-tracking issue)
  </read_first>
  <files>web/api.py</files>
  <action>
**File: `web/api.py`** — 3 raw access sites using the `nicegui_app` alias. All three read the same key `'parallels_source_text'` in different export handlers.

**Step 1: Confirm/add safe_storage import.**

Check first (Windows-safe):
```
python -c "import re; print('existing safe_storage import:', re.search(r'from web\\.safe_storage import', open('web/api.py').read()))"
```

If absent, add (after the existing `from nicegui import app as nicegui_app` or co-located with other web.* imports):
```python
from web.safe_storage import safe_user_get
```

If present (e.g., line 2106 per research already uses `safe_user_get`), no change needed.

**Step 2: Migrate 3 sites in DESCENDING line-number order (per L1 ordering principle, applied within-file).**

Order: 2073 first, then 1968, then 1932.

| Order | Line | Before | After |
|-------|------|--------|-------|
| 1 | 2073 | `storage_source_text = nicegui_app.storage.user.get('parallels_source_text', '') or ''` | `storage_source_text = safe_user_get('parallels_source_text', '') or ''` |
| 2 | 1968 | `source_text = nicegui_app.storage.user.get('parallels_source_text', '') or ''` | `source_text = safe_user_get('parallels_source_text', '') or ''` |
| 3 | 1932 | `source_text = nicegui_app.storage.user.get('parallels_source_text', '') or ''` | `source_text = safe_user_get('parallels_source_text', '') or ''` |

**Apply M3 wrapper audit:** At lines 1932 and 1968, the current code may already be wrapped in `try: ... except Exception: source_text = ''`. Read 5 lines of context around each line BEFORE deciding:

- If the wrapper catches ONLY `Exception` (broad) AND the body is `source_text = ''` (just a default-fallback): Class A. Safe to collapse — `safe_user_get` provides the same default behavior.
- If the wrapper has additional logic inside the try (e.g., json.loads on the result, or downstream processing that could raise ValueError): Class B. KEEP the wrapper but replace only the inner storage call.

Example BEFORE (likely Class A, per research excerpt):
```python
        # source_text: prefer meta, fall back to legacy app.storage.user key.
        try:
                source_text = nicegui_app.storage.user.get('parallels_source_text', '') or ''
        except Exception:
                source_text = ''
```

Example AFTER (Class A collapse):
```python
        # source_text: prefer meta, fall back to legacy app.storage.user key.
        source_text = safe_user_get('parallels_source_text', '') or ''
```

**Step 3: Check whether `nicegui_app` alias is still used anywhere in the file.**

After the 3 migrations:
```
python -c "import re; print('remaining nicegui_app uses:', len(re.findall(r'\\bnicegui_app\\b', open('web/api.py').read())))"
```

If this returns 0 (the alias is no longer used anywhere), remove the `from nicegui import app as nicegui_app` import line. If it returns >0, the alias is still used elsewhere (e.g., for `nicegui_app.add_static_files`); leave the import.

**Step 4: Verify (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/api.py').read()); print('parses OK')"
ruff check web/api.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/api.py'), pathlib.Path('web/api.py').read_text(encoding='utf-8')); assert len(v) == 0, v; print(f'OK: 0 violations')"
```
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/api.py'), pathlib.Path('web/api.py').read_text(encoding='utf-8')); assert len(v) == 0, v; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `web/api.py` parses: `python -c "import ast; ast.parse(open('web/api.py').read())"` exits 0
    - `ruff check web/api.py` exits 0
    - AST scanner reports 0 violations for api.py (verified by `<verify>` block)
    - `python -c "import re; assert re.search(r'from web\\.safe_storage import.*safe_user_get', open('web/api.py').read()); print('OK')"` prints `OK`
    - All 3 sites preserved the key string `'parallels_source_text'`: `python -c "import re; print('parallels_source_text mentions:', len(re.findall(r\"'parallels_source_text'\", open('web/api.py').read())))"` returns 3 or more
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0
    - `pytest tests/ -k 'api or export' --tb=short -q` exits 0 if any such tests exist
  </acceptance_criteria>
  <done>api.py has 0 AST violations (3 nicegui_app sites migrated).</done>
</task>

<task type="auto">
  <name>Task 3: Migrate web/supabase_client.py:263 (sign_out _app alias site) — preserve line 111 allowlist</name>
  <read_first>
    - web/supabase_client.py lines 95-130 (verify line 111 is `storage = _app.storage.user` inside `get_user_client()` — DO NOT TOUCH this; allowlisted)
    - web/supabase_client.py lines 255-275 (verify line 263 is inside `sign_out` and reads auth_session)
    - .planning/phase87_storage_allowlist.yaml (confirm web/supabase_client.py entry's pattern `"storage = _app.storage.user"` substring-matches line 111's access)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md ("web/supabase_client.py" section)
  </read_first>
  <files>web/supabase_client.py</files>
  <action>
**File: `web/supabase_client.py`** — 2 raw access sites total. Migrate ONLY line 263 (in `sign_out`). Line 111 stays raw (allowlisted; Phase 90 AUTHC-01 deletes the entire `get_user_client` function).

**Step 1: Read line 263's context.**

Per research, lines 261-264 contain:
```python
            from nicegui import app as _app
            auth_session = (_app.storage.user.get('auth_session') or {})
```

This is an INLINE local import inside the `sign_out` function — different from line 111's module-level usage (line 110 also has `from nicegui import app as _app` but inside `get_user_client`).

**Step 2: Add safe_storage import (if not already present).**

Check (Windows-safe):
```
python -c "import re; print('existing safe_storage import:', re.search(r'from web\\.safe_storage import', open('web/supabase_client.py').read()))"
```

If absent, add at the top of the file (with the other top-level imports):
```python
from web.safe_storage import safe_user_get
```

**Step 3: Migrate line 263.**

Replace lines 261-263 (the inline import + the read):

BEFORE:
```python
            from nicegui import app as _app
            auth_session = (_app.storage.user.get('auth_session') or {})
```

AFTER:
```python
            auth_session = (safe_user_get('auth_session') or {})
```

The local `from nicegui import app as _app` inline import can be deleted because line 263 was the only use of `_app` in `sign_out`. (Line 111's `_app` is a SEPARATE local import inside `get_user_client` at line 110 — DO NOT TOUCH that.)

**Step 4: VERIFY LINE 111 IS UNTOUCHED.**

Windows-safe:
```
python -c "import re; src = open('web/supabase_client.py').read(); matches = re.findall(r'_app\\.storage\\.user', src); print('_app.storage.user occurrences:', len(matches))"
```

Expected: exactly 1 match (line 111: `storage = _app.storage.user`).

If 2+: you accidentally left line 263 raw.
If 0: you accidentally deleted line 111 — restore from git.

**Step 5: Verify get_user_client function untouched.**

```
python -c "import re; src = open('web/supabase_client.py').read(); m = re.search(r'def get_user_client.*?(?=^def )', src, re.MULTILINE | re.DOTALL); body = m.group(0) if m else ''; print('get_user_client present:', bool(m)); print('captures storage = _app:', 'storage = _app.storage.user' in body)"
```

Both flags should be True.

**Step 6: Final verification (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/supabase_client.py').read()); print('parses OK')"
ruff check web/supabase_client.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file, _is_allowlisted, _load_allowlist; v = _scan_file(pathlib.Path('web/supabase_client.py'), pathlib.Path('web/supabase_client.py').read_text(encoding='utf-8')); allowed = {e['file']: e for e in _load_allowlist().get('allowed_raw_access', [])}; unallowed = [f'{lno}: {seg}' for lno, seg in v if not _is_allowlisted('web/supabase_client.py', seg, allowed)]; assert not unallowed, unallowed; print(f'OK: {len(v)} raw accesses, all allowlisted')"
```

The lint scanner should now allow the 1 remaining raw access at line 111 (allowlist entry's pattern `"storage = _app.storage.user"` substring-matches it).
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file, _is_allowlisted, _load_allowlist; v = _scan_file(pathlib.Path('web/supabase_client.py'), pathlib.Path('web/supabase_client.py').read_text(encoding='utf-8')); allowed = {e['file']: e for e in _load_allowlist().get('allowed_raw_access', [])}; unallowed = [f'{lno}: {seg}' for lno, seg in v if not _is_allowlisted('web/supabase_client.py', seg, allowed)]; assert not unallowed, unallowed; print(f'OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `web/supabase_client.py` parses: `python -c "import ast; ast.parse(open('web/supabase_client.py').read())"` exits 0
    - `ruff check web/supabase_client.py` exits 0
    - AST scanner reports raw accesses, but ALL are allowlisted (`<verify>` block)
    - Exactly one `_app.storage.user` occurrence remains (line 111): `python -c "import re; print(len(re.findall(r'_app\\.storage\\.user', open('web/supabase_client.py').read())))"` prints `1`
    - `get_user_client` function still present: `python -c "import re; print(bool(re.search(r'def get_user_client', open('web/supabase_client.py').read())))"` prints `True`
    - safe_storage import added: `python -c "import re; assert re.search(r'from web\\.safe_storage import.*safe_user_get', open('web/supabase_client.py').read()); print('OK')"` prints `OK`
    - `auth_session = (safe_user_get('auth_session')` migration applied in sign_out: `python -c "import re; assert re.search(r'auth_session\\s*=\\s*\\(safe_user_get\\(\\\\'auth_session\\\\'\\)', open('web/supabase_client.py').read()) or \"auth_session = (safe_user_get('auth_session')\" in open('web/supabase_client.py').read(); print('OK')"` prints `OK`
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0
  </acceptance_criteria>
  <done>supabase_client.py has 1 allowlisted raw access (line 111); sign_out migration applied; function bodies of get_user_client and sign_out intact aside from intended edits.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| OAuth callback (main.py:1458-1463) -> app.storage.user | Allowlisted; Phase 91 will migrate to atomic safe_user_set sequence with proper rollback |
| get_user_client cache (supabase_client.py:111) -> app.storage.user | Allowlisted; Phase 90 deletes entirely |
| /api/export/* handlers (api.py:1932/1968/2073) -> safe_storage helpers | Migration target; cross-user export leak threat (the original v7.11.1 bug source) is what these reads feed — now wrapped |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-04 | Tampering | Allowlist substring matching for line 111 of supabase_client.py | mitigate | Allowlist pattern `"storage = _app.storage.user"` substring-matches the source segment at line 111. The H1 expected_count=1 ensures no second captured-handle is silently legalized. |
| T-87-04 | Tampering | Allowlist substring matching for OAuth callback in main.py | mitigate | Allowlist patterns substring-match the 3 OAuth writer lines. Each has expected_count=1. |
| T-87-05 | Information disclosure | Alias resolution (api.py uses `nicegui_app`, supabase_client.py uses `_app`) | mitigate | After this plan, the api.py alias is fully retired (or retained only for non-storage uses); supabase_client.py retains `_app` ONLY inside the allowlisted `get_user_client` function (line 110-111). Lint scanner detects both aliases via `_find_app_aliases()`. |
| -- | Information disclosure | Cross-user export via stale `parallels_source_text` | accept (Phase 88 handles) | safe_user_get wrapping does not change the cross-user-leak character of this field; Phase 88 STATE-02/03 moves the field to per-request export_state. Phase 87 just stops the raw-access prune-race 500. |
| -- | Bootstrap timing | Plan 02 B1 wiring (`ensure_session_uuid()` in create_layout) | mitigate | Plan 04's `<acceptance_criteria>` includes an explicit "B1 wiring preserved" gate so accidental deletion is caught immediately |

Block on: T-87-04 (MEDIUM) — verified by lint test running against migrated code and confirming line 111 + OAuth callback lines pass via allowlist match.
</threat_model>

<verification>
After all 3 tasks (Windows-safe):

```
# Verify main.py: only allowlisted raw accesses remain
python -c "
import sys, pathlib
sys.path.insert(0, '.')
from tests.test_no_raw_storage_access import _scan_file, _is_allowlisted, _load_allowlist
allowed = {e['file']: e for e in _load_allowlist().get('allowed_raw_access', [])}
for f in ['web/main.py', 'web/api.py', 'web/supabase_client.py']:
    src = pathlib.Path(f).read_text(encoding='utf-8')
    v = _scan_file(pathlib.Path(f), src)
    unallowed = [(lno, seg) for lno, seg in v if not _is_allowlisted(f, seg, allowed)]
    print(f, 'total:', len(v), 'unallowed:', len(unallowed))
    assert not unallowed, (f, unallowed)
print('OK: all 3 files clean (only allowlisted raw accesses remain)')
"

# Verify Plan 02 B1 wiring preserved in main.py
python -c "
import re
src = open('web/main.py').read()
body = src[src.index('def create_layout()'):src.index('def create_layout()')+800]
assert 'ensure_session_uuid()' in body, 'B1 wiring lost!'
print('OK: Plan 02 B1 bootstrap wiring preserved')
"

# Verify safe_storage helpers imported in all 3 files
python -c "
import re
for f in ['web/main.py', 'web/api.py', 'web/supabase_client.py']:
    src = open(f).read()
    has = bool(re.search(r'from web\\.safe_storage import', src))
    print(f, 'has safe_storage import:', has)
    assert has, f
"

# All 3 files parse and pass ruff
python -c "
import ast
for f in ['web/main.py', 'web/api.py', 'web/supabase_client.py']:
    ast.parse(open(f).read(), filename=f)
print('All 3 files parse OK')
"
ruff check web/main.py web/api.py web/supabase_client.py

# Plan 02 invariants preserved
python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x

# Plan 01 lint scanner standalone tests still pass
python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x
```
</verification>

<success_criteria>
1. `web/main.py`: AST scanner reports only allowlisted raw accesses (the 3 OAuth callback writes)
2. `web/api.py`: 0 violations
3. `web/supabase_client.py`: 1 allowlisted raw access (line 111)
4. `_safe_user_storage_get` and `set_current_page` local helpers DELETED from main.py (after migrations, per L1 ordering)
5. All 3 files import safe_user_get (and set/pop where needed) from web.safe_storage
6. `ruff check` clean on all 3 files
7. `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariant preserved)
8. **Plan 02 B1 wiring preserved**: `ensure_session_uuid()` still called in `create_layout()`
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-04-SUMMARY.md` summarizing:
- main.py: 17 → 3 raw accesses (14 sites migrated + 2 local helpers deleted; 3 OAuth allowlisted)
- api.py: 3 → 0 raw accesses (alias-bearing sites all migrated)
- supabase_client.py: 2 → 1 raw access (sign_out migrated; line 111 allowlisted)
- Total Phase 87 progress so far: 14 + 3 + 1 + 16 (Plan 03) = 34 sites migrated
- L1 ordering applied: confirm that all 14 migrations landed BEFORE helper deletion (note any line-number shift surprises observed)
- M3 audit notes for any defensive wrappers preserved in api.py
- B1 preservation confirmed: `ensure_session_uuid()` still present in create_layout()
- Verification: lint scanner accepts all 3 files (only allowlisted accesses remain)
</output>
