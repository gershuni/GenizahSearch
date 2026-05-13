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
    - "web/main.py local helpers _safe_user_storage_get (line 949) and set_current_page (line 959) DELETED; all callers use web.safe_storage helpers"
    - "web/api.py raw accesses reduced from 3 (nicegui_app aliased) to 0"
    - "web/supabase_client.py raw accesses reduced from 1 (the line 263 _app aliased sign_out site) to 0; line 111 still raw (allowlisted as Phase 90 deletes the entire get_user_client function)"
    - "All non-FOUND-04 tests pass"
  artifacts:
    - path: "web/main.py"
      provides: "Migrated 14 sites (lines 327, 493, 567, 587, 598, 657, 663, 664, 691, 820, 952-in-helper-body, 960-in-helper-body, 968, 1283); deleted 2 local helpers; OAuth 3-key write at 1458-1463 left raw (allowlisted)"
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
Migrate `web/main.py` (14 of 17 raw sites; 3 OAuth sites stay allowlisted), `web/api.py` (all 3 alias-bearing sites), and `web/supabase_client.py` (1 of 2 sites; line 111 stays allowlisted) to use `web.safe_storage` helpers. Also consolidate `web/main.py`'s 2 local duplicate helpers (`_safe_user_storage_get` line 949 + `set_current_page` line 959) by deleting them and routing all callers through the chokepoint module.

Purpose: These are the "central" files of the web app — main entry, API endpoints, and Supabase glue. Two of them (`api.py`, `supabase_client.py`) use non-standard aliases (`nicegui_app`, `_app`) which research R-02 and Pitfall 1 flagged as the grep-misses risk. After this plan, only the OAuth callback (Phase 91) and the get_user_client cache (Phase 90) retain raw access — both deliberately allowlisted.

Output: 3 files modified; 18 raw access sites migrated (14 + 3 + 1); 2 local helpers deleted; allowlist invariant preserved.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
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

<!-- Current main.py local helpers (TO BE DELETED): -->

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

<!-- Allowlisted (DO NOT migrate in this plan): -->

`web/main.py:1458-1463` — OAuth callback's 3-key atomic write (already in allowlist YAML; Phase 91 AUTHW-02 will migrate)
`web/supabase_client.py:111` — get_user_client captured-handle (already in allowlist YAML; Phase 90 AUTHC-01 deletes the function)
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Migrate web/main.py — delete local helpers, migrate 14 raw sites, preserve OAuth allowlist</name>
  <read_first>
    - web/main.py lines 320-340 (verify line 327 read of 'ui_language')
    - web/main.py lines 485-505 (verify line 493 write of 'ui_language')
    - web/main.py lines 560-605 (verify lines 567, 587, 598 — whats_new_dismissed writes and drawer_open read+write combo)
    - web/main.py lines 650-700 (verify lines 657, 663, 664, 691 — show_translations + theme)
    - web/main.py lines 815-830 (verify line 820 — current_theme read)
    - web/main.py lines 945-980 (verify the LOCAL HELPERS at 949-962 + the line 968 raw read inside set_current_page or nearby)
    - web/main.py lines 1275-1295 (verify line 1283 pop)
    - web/main.py lines 1450-1470 (CRITICAL — verify the OAuth callback at 1458, 1460, 1463 to KEEP RAW; allowlisted)
    - .planning/phase87_storage_allowlist.yaml (confirm the web/main.py entry's 3 patterns match the OAuth callback strings)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/main.py" section for the migration table)
  </read_first>
  <files>web/main.py</files>
  <action>
**File: `web/main.py`** — 17 raw sites; migrate 14 (delete 2 local helpers + 12 inline sites); keep 3 OAuth sites raw (allowlisted).

**Step 1: Add safe_storage import at module top.**

Find the existing `from nicegui import app` line. Immediately AFTER it (or in the project-imports section), add:
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

**Step 2: Delete the two local helpers at lines 949-962.**

Delete the function definitions for `_safe_user_storage_get` (lines 949-957) and `set_current_page` (lines 959-962). After deletion, this region of the file becomes empty space between the surrounding functions — preserve the surrounding blank lines and don't accidentally merge unrelated functions together.

**Step 3: Update ALL callers of `_safe_user_storage_get` and `set_current_page` in main.py.**

Run this grep BEFORE deletion to find every caller (so you know what to replace):
```bash
grep -n "_safe_user_storage_get\|set_current_page" web/main.py
```

Expected callers (from research):
- `_safe_user_storage_get('current_page', '/')` → replace with `safe_user_get('current_page', '/')`
- `set_current_page(some_path)` → replace with `safe_user_set('current_page', some_path)`

Replace every caller. The new `safe_user_set('current_page', ...)` is equivalent in behavior — both swallow AssertionError, both return cleanly.

**Step 4: Migrate the 11 inline raw sites (NOT the OAuth callback).**

| Line | Current | Replace With |
|------|---------|--------------|
| 327 | `saved_lang = app.storage.user.get('ui_language')` | `saved_lang = safe_user_get('ui_language')` |
| 493 | `app.storage.user['ui_language'] = new_lang` | `safe_user_set('ui_language', new_lang)` |
| 567 | `app.storage.user['whats_new_dismissed'] = WHATS_NEW_VERSION` | `safe_user_set('whats_new_dismissed', WHATS_NEW_VERSION)` |
| 587 | `app.storage.user['whats_new_dismissed'] = WHATS_NEW_VERSION` | `safe_user_set('whats_new_dismissed', WHATS_NEW_VERSION)` |
| 598 | `app.storage.user['drawer_open'] = not app.storage.user.get('drawer_open', True)` | `safe_user_set('drawer_open', not safe_user_get('drawer_open', True))` |
| 657 | `show_translations = app.storage.user.get('show_translations', False)` | `show_translations = safe_user_get('show_translations', False)` |
| 663 | `current = app.storage.user.get('show_translations', False)` | `current = safe_user_get('show_translations', False)` |
| 664 | `app.storage.user['show_translations'] = not current` | `safe_user_set('show_translations', not current)` |
| 691 | `app.storage.user['theme'] = theme_name` | `safe_user_set('theme', theme_name)` |
| 820 | `current_theme = app.storage.user.get('theme', 'light')` | `current_theme = safe_user_get('theme', 'light')` |
| 968 | `current_theme = app.storage.user.get('theme', 'light')` | `current_theme = safe_user_get('theme', 'light')` |
| 1283 | `app.storage.user.pop(key, None)` | `safe_user_pop(key, None)` |

NOTE: After deleting the local helpers (Step 2), the line numbers above will SHIFT (line 968 may move up by ~14 lines, line 1283 by ~14 lines). Apply migrations BEFORE deletion to preserve the line numbers, OR perform deletion last. Either approach works — but be consistent. Recommended: do migrations in descending line-number order (1283, 968, 820, ...) so prior-position line numbers don't shift, THEN do the deletion last.

**Step 5: DO NOT touch lines 1458, 1460, 1463 (OAuth callback).**

Verify these 3 lines remain unchanged after your edits:
```python
app.storage.user[GlobalAuthState.USER_KEY] = user
app.storage.user[GlobalAuthState.PROFILE_KEY] = profile
app.storage.user['auth_session'] = {
```

These are allowlisted per `.planning/phase87_storage_allowlist.yaml` for atomic OAuth-callback semantics. Phase 91 AUTHW-02 migrates them.

**Step 6: Verify.**

```bash
# Count raw accesses — expect exactly 3 (the OAuth callback)
grep -c "app\.storage\.user" web/main.py
# Expect: 3

# Verify the 3 remaining are exactly the OAuth callback (substring match)
grep -E "app\.storage\.user\[GlobalAuthState\.(USER|PROFILE)_KEY\]|app\.storage\.user\['auth_session'\]" web/main.py
# Expect: 3 matches at lines 1458, 1460, 1463 (line numbers may shift due to helper deletion)

# Verify local helpers deleted
grep -c "def _safe_user_storage_get\|def set_current_page" web/main.py
# Expect: 0

# Verify safe_storage helpers imported and used
grep -c "from web.safe_storage import" web/main.py
# Expect: 1
grep -c "safe_user_get\|safe_user_set\|safe_user_pop" web/main.py
# Expect: at least 13 (1 import line + 12 use sites = 13; could be higher if any uses are on same line)

# Verify file parses
python -c "import ast; ast.parse(open('web/main.py').read())"

# Verify ruff
ruff check web/main.py
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/main.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/main.py` returns exactly 3 (the OAuth callback's 3 lines)
    - The 3 remaining raw accesses are exactly the OAuth callback: `grep -E "app\.storage\.user\[(GlobalAuthState\.USER_KEY|GlobalAuthState\.PROFILE_KEY|'auth_session')\]" web/main.py | wc -l` returns 3
    - `grep -c "def _safe_user_storage_get" web/main.py` returns 0 (local helper deleted)
    - `grep -c "def set_current_page" web/main.py` returns 0 (local helper deleted)
    - `grep -c "_safe_user_storage_get(" web/main.py` returns 0 (no callers remain)
    - `grep -c "set_current_page(" web/main.py` returns 0 (no callers remain)
    - `grep -c "from web.safe_storage import" web/main.py` returns 1
    - `grep -c "safe_user_get\|safe_user_set\|safe_user_pop" web/main.py` returns at least 13 (1 import + ~12 use sites)
    - File parses: `python -c "import ast; ast.parse(open('web/main.py').read())"` exits 0
    - `ruff check web/main.py` exits 0
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariant preserved)
    - Lint scanner now reports fewer violations: `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist 2>&1 | grep -c "main.py"` returns 0 OR the only main.py violations are the OAuth callback (substring-matches allowlist patterns)
  </acceptance_criteria>
  <done>main.py has 3 raw accesses left (OAuth callback only); local helpers deleted; 14 sites migrated.</done>
</task>

<task type="auto">
  <name>Task 2: Migrate web/api.py (3 nicegui_app alias sites at lines 1932, 1968, 2073)</name>
  <read_first>
    - web/api.py — at minimum read these regions:
      - Top of file (find the `from nicegui import app as nicegui_app` import line)
      - Lines 1925-1940 (verify line 1932 read of parallels_source_text)
      - Lines 1960-1975 (verify line 1968 read — identical pattern)
      - Lines 2060-2080 (verify line 2073 read — same key, different code path)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/api.py" section)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (Pitfall 1 — the nicegui_app alias-tracking issue)
  </read_first>
  <files>web/api.py</files>
  <action>
**File: `web/api.py`** — 3 raw access sites using the `nicegui_app` alias. All three read the same key `'parallels_source_text'` in different export handlers.

**Step 1: Add safe_storage import at top of file.**

Find the existing `from nicegui import app as nicegui_app` import. Immediately after it (or co-located with other web.* imports), add:
```python
from web.safe_storage import safe_user_get
```

(There may already be a `from web.safe_storage import safe_user_get` import elsewhere in api.py — per research line 2106 uses it. Check first with `grep "from web.safe_storage" web/api.py`. If it's already imported, no change needed at this step.)

**Step 2: Migrate 3 sites.**

| Line | Current | Replace With |
|------|---------|--------------|
| 1932 | `source_text = nicegui_app.storage.user.get('parallels_source_text', '') or ''` | `source_text = safe_user_get('parallels_source_text', '') or ''` |
| 1968 | `source_text = nicegui_app.storage.user.get('parallels_source_text', '') or ''` | `source_text = safe_user_get('parallels_source_text', '') or ''` |
| 2073 | `storage_source_text = nicegui_app.storage.user.get('parallels_source_text', '') or ''` | `storage_source_text = safe_user_get('parallels_source_text', '') or ''` |

NOTE: At lines 1932 and 1968, the current code may already be wrapped in `try: ... except Exception: source_text = ''`. The `safe_user_get` wrapper handles this internally — you can simplify by removing the outer try/except. Read the actual code context (5 lines around each line number) before deciding. If the try/except has a SECOND statement inside it (not just the storage read), keep the try/except. If it ONLY wraps the storage read, remove it.

For example, BEFORE at line 1928-1934 (per research excerpt):
```python
        # source_text: prefer meta, fall back to legacy app.storage.user key.
        try:
                source_text = nicegui_app.storage.user.get('parallels_source_text', '') or ''
        except Exception:
                source_text = ''
```

AFTER:
```python
        # source_text: prefer meta, fall back to legacy app.storage.user key.
        source_text = safe_user_get('parallels_source_text', '') or ''
```

**Step 3: Check whether `nicegui_app` is still used.**

After the 3 migrations, run:
```bash
grep -c "nicegui_app" web/api.py
```

If this returns 0 (the alias is no longer used anywhere in the file), remove the `from nicegui import app as nicegui_app` import line. If it returns > 0, the alias is still used elsewhere (e.g., for `nicegui_app.add_static_files`); leave the import.

**Step 4: Verify.**

```bash
grep -c "nicegui_app\.storage\.user" web/api.py            # expect 0
grep -c "app\.storage\.user" web/api.py                    # expect 0
grep -c "safe_user_get" web/api.py                         # expect at least 4 (1 import + 3 sites; or higher if line 2106 was already present)
python -c "import ast; ast.parse(open('web/api.py').read())"
ruff check web/api.py
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/api.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/api.py` returns 0
    - `grep -c "nicegui_app\.storage\.user" web/api.py` returns 0
    - `grep -c "safe_user_get" web/api.py` returns at least 4 (1 import + 3 migrated sites, plus possible pre-existing safe_user_get at line 2106 per research)
    - `grep -c "parallels_source_text" web/api.py` returns at least 3 (all 3 site references preserved)
    - File parses: `python -c "import ast; ast.parse(open('web/api.py').read())"` exits 0
    - `ruff check web/api.py` exits 0
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0
    - `pytest tests/ -k "api or export" --tb=short -q` exits 0 if any such tests exist
  </acceptance_criteria>
  <done>api.py has 0 raw accesses (3 nicegui_app sites migrated).</done>
</task>

<task type="auto">
  <name>Task 3: Migrate web/supabase_client.py:263 (sign_out _app alias site) — preserve line 111 allowlist</name>
  <read_first>
    - web/supabase_client.py lines 95-130 (verify line 111 is `storage = _app.storage.user` inside `get_user_client()` — DO NOT TOUCH this; allowlisted)
    - web/supabase_client.py lines 255-275 (verify line 263 is inside `sign_out` and reads auth_session)
    - .planning/phase87_storage_allowlist.yaml (confirm web/supabase_client.py entry includes pattern `"_app.storage.user"` which substring-matches line 111's `_app.storage.user` access)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/supabase_client.py" section)
  </read_first>
  <files>web/supabase_client.py</files>
  <action>
**File: `web/supabase_client.py`** — 2 raw access sites total. Migrate ONLY line 263 (in `sign_out`). Line 111 stays raw (allowlisted; Phase 90 AUTHC-01 deletes the entire `get_user_client` function).

**Step 1: Read line 263's context.**

The code at lines 261-264 currently is (per research):
```python
            from nicegui import app as _app
            auth_session = (_app.storage.user.get('auth_session') or {})
```

This is an INLINE local import inside the `sign_out` function — different from line 111's module-level usage (though line 111's `from nicegui import app as _app` is on line 110 inside `get_user_client`).

**Step 2: Add safe_storage import.**

Check if there's already a module-level `from web.safe_storage import` in supabase_client.py:
```bash
grep "from web.safe_storage" web/supabase_client.py
```

If absent, add at the top of the file (after the existing imports):
```python
from web.safe_storage import safe_user_get
```

**Step 3: Migrate line 263.**

Replace lines 261-263 (the inline import + the read):
```python
            from nicegui import app as _app
            auth_session = (_app.storage.user.get('auth_session') or {})
```

with:
```python
            auth_session = (safe_user_get('auth_session') or {})
```

The local `from nicegui import app as _app` inline import can be deleted because line 263 was the only use of `_app` in `sign_out`. (Line 111's `_app` is a separate local import inside `get_user_client` — DO NOT TOUCH that.)

**Step 4: VERIFY LINE 111 IS UNTOUCHED.**

```bash
grep -n "_app\.storage\.user" web/supabase_client.py
# Expect: exactly 1 match at line 111 (`storage = _app.storage.user`)
```

If this shows 2 or more matches, you accidentally left line 263 raw. If it shows 0 matches, you accidentally deleted line 111 — restore from git.

**Step 5: Verify get_user_client function untouched.**

```bash
grep -A 30 "def get_user_client" web/supabase_client.py | head -35
```

This should show the get_user_client function body INCLUDING line 111 (`storage = _app.storage.user`). Compare to git history to confirm zero diff in lines 95-150.

**Step 6: Final verification.**

```bash
grep -c "app\.storage\.user" web/supabase_client.py        # expect 1 (line 111 only — allowlisted)
grep -c "_app\.storage\.user" web/supabase_client.py       # expect 1 (line 111 only)
grep -c "safe_user_get" web/supabase_client.py             # expect at least 2 (1 import + 1 use)
grep -c "from nicegui import app as _app" web/supabase_client.py  # expect 1 (line 110, inside get_user_client — DO NOT TOUCH)
python -c "import ast; ast.parse(open('web/supabase_client.py').read())"
ruff check web/supabase_client.py
```

The lint scanner should now allow the 1 remaining raw access at line 111 (allowlist entry's pattern `"_app.storage.user"` substring-matches it).
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/supabase_client.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/supabase_client.py` returns exactly 1 (line 111 allowlisted entry preserved)
    - `grep -c "_app\.storage\.user" web/supabase_client.py` returns exactly 1 (line 111)
    - The single remaining raw access is on a line containing `storage = _app.storage.user` (inside `get_user_client` — verify with `grep "storage = _app.storage.user" web/supabase_client.py | wc -l` returns 1)
    - `grep -c "safe_user_get" web/supabase_client.py` returns at least 2 (1 import + 1 site)
    - `grep -c "auth_session = (safe_user_get('auth_session')" web/supabase_client.py` returns 1 (migration applied)
    - `grep -c "def get_user_client" web/supabase_client.py` returns 1 (function not accidentally deleted)
    - File parses: `python -c "import ast; ast.parse(open('web/supabase_client.py').read())"` exits 0
    - `ruff check web/supabase_client.py` exits 0
    - Lint scanner: the remaining raw access matches the allowlist pattern (substring `"_app.storage.user"` from allowlist YAML matches the line 111 source segment). Verify by running `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist 2>&1 | grep -c "supabase_client.py:111"` returns 0 (no longer flagged).
  </acceptance_criteria>
  <done>supabase_client.py has 1 allowlisted raw access (line 111); sign_out migration applied; function bodies of get_user_client and sign_out intact aside from intended edits.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| OAuth callback (main.py:1458-1463) → app.storage.user | Allowlisted; Phase 91 will migrate to atomic safe_user_set sequence with proper rollback |
| get_user_client cache (supabase_client.py:111) → app.storage.user | Allowlisted; Phase 90 deletes entirely |
| /api/export/* handlers (api.py:1932/1968/2073) → safe_storage helpers | Migration target; cross-user export leak threat (the original v7.11.1 bug source) is what these reads feed — now wrapped |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-04 | Tampering | Allowlist substring matching for line 111 of supabase_client.py | mitigate | Allowlist pattern `"_app.storage.user"` substring-matches the source segment `storage = _app.storage.user` — verified by lint scanner. If line 111 is moved or refactored, the substring match still works as long as the same identifier prefix `_app.storage.user` appears. |
| T-87-04 | Tampering | Allowlist substring matching for OAuth callback in main.py | mitigate | Allowlist patterns `"app.storage.user[GlobalAuthState.USER_KEY]"` etc. substring-match the OAuth writer lines. Same robustness as above. |
| T-87-05 | Information disclosure | Alias resolution (api.py uses `nicegui_app`, supabase_client.py uses `_app`) | mitigate | After this plan, the api.py alias is fully retired (or retained only for non-storage uses); supabase_client.py retains `_app` ONLY inside the allowlisted `get_user_client` function (line 110-111). Lint scanner detects both aliases via `_find_app_aliases()`. |
| — | Information disclosure | Cross-user export via stale `parallels_source_text` | accept (Phase 88 handles) | safe_user_get wrapping does not change the cross-user-leak character of this field; Phase 88 STATE-02/03 moves the field to per-request export_state. Phase 87 just stops the raw-access prune-race 500. |

Block on: T-87-04 (MEDIUM) — verified by lint test running against migrated code and confirming line 111 + OAuth callback lines pass via allowlist match.
</threat_model>

<verification>
After all 3 tasks:

```bash
# Verify main.py allowlist invariant: exactly 3 raw accesses, all OAuth-callback
grep -n "app\.storage\.user" web/main.py
# Expect 3 lines: GlobalAuthState.USER_KEY write, GlobalAuthState.PROFILE_KEY write, 'auth_session' write

# Verify api.py fully migrated
grep -c "app\.storage\.user\|nicegui_app\.storage\.user" web/api.py
# Expect: 0

# Verify supabase_client.py allowlist invariant: exactly 1 raw access (line 111)
grep -n "app\.storage\.user" web/supabase_client.py
# Expect 1 line: "        storage = _app.storage.user"

# Verify safe_storage helpers imported in all 3 files
for f in web/main.py web/api.py web/supabase_client.py; do
  echo -n "$f: "
  grep -c "from web.safe_storage import" "$f"
done

# All 3 files parse and pass ruff
python -c "
import ast
for f in ['web/main.py', 'web/api.py', 'web/supabase_client.py']:
    ast.parse(open(f).read(), filename=f)
print('All 3 files parse OK')
"
ruff check web/main.py web/api.py web/supabase_client.py

# Plan 02 invariants preserved
pytest tests/test_safe_storage.py tests/test_session_uuid.py -x

# Lint scanner: verify only allowlisted sites remain in these 3 files
pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist 2>&1 | grep -E "main\.py|api\.py|supabase_client\.py" || echo "No violations in main/api/supabase — allowlist working"
```
</verification>

<success_criteria>
1. `grep -c "app\.storage\.user" web/main.py` returns 3 (OAuth callback only; allowlisted)
2. `grep -c "app\.storage\.user\|nicegui_app\.storage\.user" web/api.py` returns 0
3. `grep -c "app\.storage\.user" web/supabase_client.py` returns 1 (line 111 only; allowlisted)
4. `_safe_user_storage_get` and `set_current_page` local helpers deleted from main.py
5. All 3 files import `safe_user_get` (and set/pop where needed) from `web.safe_storage`
6. `ruff check` clean on all 3 files
7. `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariant preserved)
8. Lint scanner test does not flag main.py, api.py, or supabase_client.py for unallowlisted raw access
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-04-SUMMARY.md` summarizing:
- main.py: 17 → 3 raw accesses (14 sites migrated + 2 local helpers deleted; 3 OAuth allowlisted)
- api.py: 3 → 0 raw accesses (alias-bearing sites all migrated)
- supabase_client.py: 2 → 1 raw access (sign_out migrated; line 111 allowlisted)
- Total Phase 87 progress so far: 14 + 3 + 1 + 16 (Plan 03) = 34 sites migrated
- Verification: lint scanner accepts all 3 files
</output>
