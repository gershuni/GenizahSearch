# Phase 87: Foundations -- Session UUID and Safe Storage Chokepoint - Pattern Map

**Mapped:** 2026-05-13
**Files analyzed:** 18 (3 new, 15 modified)
**Analogs found:** 17 / 18

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `web/safe_storage.py` (additive) | utility/storage-helper | request-response | `web/safe_storage.py` (self) | self — additive only |
| `tests/test_session_uuid.py` | test | request-response | `tests/test_safe_storage.py` | exact |
| `tests/test_no_raw_storage_access.py` | test/lint-scanner | batch/transform | `tests/test_crawler_visibility.py` (walks source constants) | partial — no AST-walk analog; new pattern |
| `.planning/phase87_storage_allowlist.yaml` | config/allowlist | — | none | no analog |
| `web/main.py` (migrate 11+ sites) | route/bootstrap | request-response | `web/main.py` itself (local `_safe_user_storage_get` at line 949) | self — consolidate local helper to module |
| `web/auth_state.py` | service/auth | request-response | ALLOWLIST (Phase 91) — no migration | no migration |
| `web/api.py` (3 sites) | route/API | request-response | `web/api.py` lines 1932, 1968, 2073 (nicegui_app alias) | self |
| `web/supabase_client.py` (1 site) | service | request-response | `web/supabase_client.py` line 263 (_app alias) | self |
| `web/components/text_editor.py` (3 sites) | component | event-driven | `web/components/filter_panel.py` (cca23db3 migration) | role-match |
| `web/components/translation_report.py` (1 site) | component | request-response | `web/components/filter_panel.py` (cca23db3 migration) | role-match |
| `web/pages/browse.py` (4 sites) | page-handler | request-response | `web/pages/browse_state.py` (inline-guarded pattern) | role-match |
| `web/pages/browse_state.py` (10 sites) | service/state | CRUD | `web/pages/browse_state.py` itself (already has inline guards) | self — replace inline guards with helpers |
| `web/pages/catalog_browse.py` (3 sites) | page-handler | request-response | `web/pages/settings.py` (mixed read/write pattern) | role-match |
| `web/pages/parallels.py` (9 sites) | page-handler | request-response | `web/pages/settings.py` (mixed pattern) | role-match |
| `web/pages/search.py` (~17 sites) | page-handler | request-response | `web/pages/settings.py` | role-match |
| `web/pages/search_state.py` (11 sites) | service/state | CRUD | `web/pages/browse_state.py` (persist/restore functions) | exact |
| `web/pages/search_results.py` (3 sites) | component | request-response | `web/components/filter_panel.py` | role-match |
| `web/pages/settings.py` (7 sites) | page-handler | request-response | `web/pages/settings.py` itself | self |
| `web/pages/home.py` (2 sites) | page-handler | request-response | `web/pages/home.py` itself | self |
| `.github/workflows/ci.yml` | config/CI | — | `.github/workflows/ci.yml` itself | self |

---

## Pattern Assignments

### `web/safe_storage.py` — additive: `get_session_uuid()` + `ensure_session_uuid()`

**Analog:** `web/safe_storage.py` (existing helpers — keep byte-identical)

**Imports pattern** (lines 30-37 — copy verbatim):
```python
from __future__ import annotations

import logging
from typing import Any

from nicegui import app

logger = logging.getLogger(__name__)
```

**New import needed** (add after existing imports):
```python
import uuid as _uuid
```

**Core pattern for new helpers** — follow the exact try/except shape of the existing three:
```python
_SESSION_UUID_KEY = '_session_uuid'

def get_session_uuid() -> str:
    """Return this session's stable UUID, minting one on first call."""
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if uid and isinstance(uid, str) and len(uid) == 32:
            return uid
        uid = _uuid.uuid4().hex
        app.storage.user[_SESSION_UUID_KEY] = uid
        return uid
    except AssertionError as e:
        logger.debug("get_session_uuid: session storage unavailable: %s", e)
        return _uuid.uuid4().hex
    except Exception as e:
        logger.warning("get_session_uuid unexpected failure: %s", e)
        return _uuid.uuid4().hex


def ensure_session_uuid() -> bool:
    """Eagerly mint session UUID if not present. Returns True on success."""
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if not (uid and isinstance(uid, str) and len(uid) == 32):
            app.storage.user[_SESSION_UUID_KEY] = _uuid.uuid4().hex
        return True
    except AssertionError as e:
        logger.debug("ensure_session_uuid: session storage unavailable: %s", e)
        return False
    except Exception as e:
        logger.warning("ensure_session_uuid unexpected failure: %s", e)
        return False
```

**Constraint:** Do NOT modify the signatures or bodies of `safe_user_get`, `safe_user_set`, `safe_user_pop` (lines 40-79). The 6 existing tests must pass without change (FOUND-05).

---

### `tests/test_session_uuid.py` (new test file)

**Analog:** `tests/test_safe_storage.py`

**Mock setup pattern** (lines 12-28 of test_safe_storage.py — copy exactly):
```python
from unittest.mock import patch, MagicMock

def test_some_helper():
    storage = {}  # or MagicMock() with side_effect

    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        result = get_session_uuid()
```

**Key rule:** Always patch `'web.safe_storage.app'` (the module-level import), not `'nicegui.app'`. This is the established pattern in the existing 6 tests.

**Happy path test — 100-session uniqueness** (FOUND-01 SC1):
```python
def test_session_uuid_unique_across_100_sessions():
    uuids_seen = set()
    for i in range(100):
        storage = {}  # Fresh dict = fresh "session"
        with patch('web.safe_storage.app') as mock_app:
            mock_app.storage.user = storage
            from web.safe_storage import get_session_uuid
            uid = get_session_uuid()
        assert uid not in uuids_seen, f"UUID collision at session {i}"
        uuids_seen.add(uid)
    assert len(uuids_seen) == 100
```

**Prune-race test** (matches existing AssertionError tests):
```python
def test_session_uuid_returns_ephemeral_on_assertion():
    storage = MagicMock()
    storage.get.side_effect = AssertionError("user storage for x should be created before accessing it")

    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        uid = get_session_uuid()
    assert isinstance(uid, str) and len(uid) == 32  # Still returns a UUID, never None
```

**Idempotency test** (FOUND-01 SC2 — same call, same key, stable value):
```python
def test_session_uuid_stable_across_calls():
    storage = {}
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        uid1 = get_session_uuid()
        uid2 = get_session_uuid()
    assert uid1 == uid2
```

---

### `tests/test_no_raw_storage_access.py` (new AST-lint test)

**Analog:** No exact analog in repo. `tests/test_crawler_visibility.py` walks module-level constants but does not parse source files. This is a new pattern.

**File header pattern** (copy `test_safe_storage.py` docstring style):
```python
"""Lint test: reject raw app.storage.user access outside the Phase 87 allowlist.

Reads .planning/phase87_storage_allowlist.yaml and checks every .py file under
web/ for AST nodes matching app.storage.user.get/pop/[key] or aliased variants.
"""
```

**Path discovery pattern** (RESEARCH.md R-03 — critical: do NOT import web modules):
```python
import ast
from pathlib import Path
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = REPO_ROOT / 'web'
ALLOWLIST_PATH = REPO_ROOT / '.planning' / 'phase87_storage_allowlist.yaml'
```

**Alias resolver** (handles `app`, `nicegui_app`, `_app` — all three aliases found in codebase):
```python
def _find_app_aliases(tree: ast.AST) -> set[str]:
    aliases = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'nicegui':
            for alias in node.names:
                if alias.name == 'app':
                    aliases.add(alias.asname or 'app')
    return aliases
```

**AST visitor** (never import target modules — parse source text only):
```python
for path in WEB_DIR.rglob('*.py'):
    if path.name == 'safe_storage.py':
        continue  # The chokepoint itself is exempt
    with path.open('r', encoding='utf-8') as f:
        source = f.read()
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as e:
        pytest.fail(f"AST parse failed for {rel}: {e}")
```

**Self-test for lint correctness** (FOUND-04 SC4 — test that the test works):
```python
def test_lint_rejects_synthetic_violation():
    import textwrap
    synthetic = textwrap.dedent("""
        from nicegui import app
        def bad():
            return app.storage.user.get('foo')
    """)
    tree = ast.parse(synthetic)
    aliases = _find_app_aliases(tree)
    # ... assert the visitor finds the violation
```

---

### `.planning/phase87_storage_allowlist.yaml` (new allowlist)

**Analog:** None in repo. YAML format per RESEARCH.md R-04.

**Format to use:**
```yaml
# Phase 87 Storage Allowlist
# Each entry exempts a file from the raw app.storage.user lint test.
# justification is REQUIRED per FOUND-03 success criterion 3.

allowed_raw_access:
  - file: web/auth_state.py
    patterns:
      - "app.storage.user.get(cls.USER_KEY)"
      - "app.storage.user.get(cls.PROFILE_KEY)"
      - "app.storage.user[cls.USER_KEY]"
      - "app.storage.user[cls.PROFILE_KEY]"
      - "app.storage.user.pop(cls.USER_KEY, None)"
      - "app.storage.user.pop(cls.PROFILE_KEY, None)"
      - "app.storage.user.pop('auth_session', None)"
      - "app.storage.user['auth_session']"
    justification: |
      GlobalAuthState class methods already wrap every access in try/except.
      Phase 91 will migrate these to safe_storage helpers as part of the
      atomic auth write refactor (AUTHW-01). All writes here are part of the
      login/logout sequence where atomicity (writing all auth keys together)
      matters more than the prune guard.

  - file: web/main.py
    patterns:
      - "app.storage.user[GlobalAuthState.USER_KEY]"
      - "app.storage.user[GlobalAuthState.PROFILE_KEY]"
      - "app.storage.user['auth_session']"
    justification: |
      OAuth callback at main.py:1458-1466 writes USER_KEY, PROFILE_KEY, and
      auth_session together in a single handler. Atomicity of this 3-key write
      must be preserved — Phase 91 will migrate as part of auth atomicity work.
      All other main.py raw accesses are migrated to safe_user_get/set in Phase 87.
```

---

### `web/main.py` — migrate `_safe_user_storage_get` + `set_current_page` + 11 sites

**Analog:** `web/main.py` itself (the local helpers at lines 949-962 duplicate `web/safe_storage.py`)

**Before: local duplicate helper** (lines 949-962):
```python
def _safe_user_storage_get(key: str, default=None):
    """Safely read from app.storage.user, returning default if session not ready."""
    try:
        return app.storage.user.get(key, default)
    except (AssertionError, KeyError, Exception):
        return default

def set_current_page(page_path: str):
    """Safely set the current page in user storage."""
    try:
        app.storage.user['current_page'] = page_path
    except (AssertionError, KeyError, Exception):
        pass
```

**After: delete both, add to imports at top:**
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

**Before: raw read** (main.py line 327):
```python
try:
    saved_lang = app.storage.user.get('ui_language')
except Exception:
    saved_lang = None
```

**After:**
```python
saved_lang = safe_user_get('ui_language')
```

**Before: raw write** (main.py line 493):
```python
app.storage.user['ui_language'] = new_lang
```

**After:**
```python
safe_user_set('ui_language', new_lang)
```

**Before: raw read + write on same line** (main.py line 598):
```python
app.storage.user['drawer_open'] = not app.storage.user.get('drawer_open', True)
```

**After:**
```python
safe_user_set('drawer_open', not safe_user_get('drawer_open', True))
```

**OAuth callback** (lines 1458-1466) — ALLOWLIST, do not migrate. See allowlist entry above.

---

### `web/pages/settings.py` — 7 raw writes

**Analog:** `web/pages/settings.py` itself. Reads are already migrated to `_safe_get` (line 22). The 7 writes inside callback functions are the remaining raw sites.

**Current state — reads done, writes raw** (lines 49, 59-61, 76, 94, 109, 119, 134, 148):
```python
# TOP of create_settings_page():
from web.safe_storage import safe_user_get as _safe_get

# reads are already migrated:
current_theme = _safe_get('theme', 'light')

# writes are still raw (inside event callbacks):
def change_theme():
    theme = theme_select.value
    app.storage.user['theme'] = theme  # <-- migrate this
```

**After — add safe_user_set import alongside existing safe_user_get:**
```python
from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set

def change_theme():
    theme = theme_select.value
    _safe_set('theme', theme)
```

Apply same pattern for all 7 write callbacks: `results_per_page`, `default_search_mode`, `default_gap`, `lab_mode_default`, `session_persistence_enabled`, `search_history_limit`.

---

### `web/pages/home.py` — 2 raw writes

**Analog:** `web/pages/home.py` itself. Read is already migrated (line 29-30); writes are inline inside callbacks at lines 40 and 59.

**Current state** (lines 27-60):
```python
from web.safe_storage import safe_user_get as _safe_get
if not _safe_get('ocr_disclaimer_dismissed', False):
    ...
    def dismiss_banner():
        app.storage.user['ocr_disclaimer_dismissed'] = True  # raw write
    ...
    def _auto_dismiss_ocr():
        try:
            ocr_banner.delete()
        except Exception:
            return
        try:
            app.storage.user['ocr_disclaimer_dismissed'] = True  # raw write
        except Exception:
            pass
```

**After:**
```python
from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set

def dismiss_banner():
    _safe_set('ocr_disclaimer_dismissed', True)
    ...

def _auto_dismiss_ocr():
    try:
        ocr_banner.delete()
    except Exception:
        return
    _safe_set('ocr_disclaimer_dismissed', True)
```

Note: remove the bare `try/except Exception: pass` wrapper around the write — `safe_user_set` absorbs the exception internally and returns bool.

---

### `web/pages/browse_state.py` — replace 10 inline-guarded raw accesses

**Analog:** `web/pages/browse_state.py` itself. This file already has inline try/except wrappers, but they predate `safe_storage.py`. Replace each guarded block with the helper.

**Before: inline guard pattern** (lines 126-130 — characteristic of this file):
```python
try:
    stored_version = app.storage.user.get('browse_snapshot_schema_version', 0)
except (AssertionError, Exception) as e:
    logger.debug(f"[BrowseSnapshot] user storage unavailable on restore: {e}")
    return (None, None)
```

**After:**
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

stored_version = safe_user_get('browse_snapshot_schema_version', 0)
if stored_version is None:  # storage unavailable — safe_user_get returns default (0), not None
    return (None, None)
# Actually: safe_user_get returns 0 on failure, so just use the value directly:
stored_version = safe_user_get('browse_snapshot_schema_version', 0)
```

**Before: raw write in persist function** (lines 180-203):
```python
try:
    app.storage.user['browse_snapshot_schema_version'] = _BROWSE_SNAPSHOT_VERSION
    if page is not None and state.sys_id:
        app.storage.user['browse_position'] = { ... }
    ...
    app.storage.user.pop('reading_desk_state', None)
except Exception as e:
    logger.error(f"[BrowseSnapshot] Error persisting state: {e}")
```

**After:**
```python
safe_user_set('browse_snapshot_schema_version', _BROWSE_SNAPSHOT_VERSION)
if page is not None and state.sys_id:
    safe_user_set('browse_position', { ... })
...
safe_user_pop('reading_desk_state', None)
```

The outer try/except can be dropped — each helper absorbs its own exception and logs it.

---

### `web/pages/search_state.py` — 11 raw writes at lines 394-502

**Analog:** `web/pages/browse_state.py` (same persist/restore structure)

**Before: bulk writes in `persist_search_snapshot`** (lines 394-408):
```python
app.storage.user['search_snapshot_schema_version'] = _SEARCH_SNAPSHOT_VERSION
app.storage.user['search_results'] = _compact_result_rows(...)
app.storage.user['search_printed_filter'] = state.printed_filter
app.storage.user['domain_exclusions'] = list(state.domain_exclusions or [])
app.storage.user['search_refinement_chain'] = [...]
app.storage.user['search_exclusion_sources'] = list(state.exclusion_sources or [])
```

**After:**
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

safe_user_set('search_snapshot_schema_version', _SEARCH_SNAPSHOT_VERSION)
safe_user_set('search_results', _compact_result_rows(...))
safe_user_set('search_printed_filter', state.printed_filter)
safe_user_set('domain_exclusions', list(state.domain_exclusions or []))
safe_user_set('search_refinement_chain', [...])
safe_user_set('search_exclusion_sources', list(state.exclusion_sources or []))
```

**Before: raw reads in `restore_search_snapshot`** (lines 362-374):
```python
state.results = app.storage.user.get('search_results', []) or []
state.printed_filter = app.storage.user.get('search_printed_filter', 'all')
_de = app.storage.user.get('domain_exclusions')
raw_chain = app.storage.user.get('search_refinement_chain', []) or []
state.exclusion_sources = app.storage.user.get('search_exclusion_sources', []) or []
```

**After:**
```python
state.results = safe_user_get('search_results', []) or []
state.printed_filter = safe_user_get('search_printed_filter', 'all')
_de = safe_user_get('domain_exclusions')
raw_chain = safe_user_get('search_refinement_chain', []) or []
state.exclusion_sources = safe_user_get('search_exclusion_sources', []) or []
```

**Before: conditional writes in `_reset_filter_storage_keys`** (lines 441-502):
```python
app.storage.user[key] = value
# or
app.storage.user.pop(key, None)
```

**After:**
```python
safe_user_set(key, value)
# or
safe_user_pop(key, None)
```

---

### `web/api.py` — 3 sites using `nicegui_app` alias (lines 1932, 1968, 2073)

**Analog:** `web/api.py` itself. The alias is `nicegui_app` from `from nicegui import app as nicegui_app`.

**Before** (lines 1931-1934):
```python
try:
    source_text = nicegui_app.storage.user.get('parallels_source_text', '') or ''
except Exception:
    source_text = ''
```

**After:**
```python
from web.safe_storage import safe_user_get
source_text = safe_user_get('parallels_source_text', '') or ''
```

Same pattern for the identical blocks at lines 1968 and 2073. The `from web.safe_storage import safe_user_get` import should be added once at the top of the function or at module level (check if already imported elsewhere in api.py first).

---

### `web/supabase_client.py` — 1 site using `_app` alias (line 263)

**Analog:** `web/supabase_client.py` itself. The alias is `_app` from `from nicegui import app as _app`.

**Before** (lines 261-264):
```python
from nicegui import app as _app
auth_session = (_app.storage.user.get('auth_session') or {})
```

**After:**
```python
from web.safe_storage import safe_user_get
auth_session = (safe_user_get('auth_session') or {})
```

The local `from nicegui import app as _app` import at line 262 can be removed if this is the only usage in the `sign_out` function.

---

### `web/components/filter_panel.py` — ALREADY MIGRATED (cca23db3)

**Status:** Verified clean. Lines 229-231, 249, 300, 336-337 all use `safe_user_get`, `safe_user_set`, `safe_user_pop`.

**This file is the reference pattern** for how component-level migrations should look after Phase 87:
```python
# Reading with safe helper (lines 249-250):
from web.safe_storage import safe_user_get as _sg
value = _sg(key, default)

# Writing via persist_value helper (lines 229-231):
from web.safe_storage import safe_user_get, safe_user_set
if safe_user_get('session_persistence_enabled', True):
    safe_user_set(key, value)

# Popping (lines 336-337):
from web.safe_storage import safe_user_pop
safe_user_pop('incoming_filters', None)
```

Do NOT re-touch filter_panel.py during Phase 87 migration.

---

### `web/auth_state.py` — ALLOWLIST ENTIRELY (Phase 91 migrates)

**Source:** `web/auth_state.py` lines 42, 50, 95, 97, 117, 122-124, 176

**Current pattern** (lines 38-52 — already has try/except guards):
```python
@classmethod
def get_user(cls) -> Optional[Dict]:
    try:
        return app.storage.user.get(cls.USER_KEY)
    except Exception:
        return None

@classmethod
def get_profile(cls) -> Optional[Dict]:
    try:
        return app.storage.user.get(cls.PROFILE_KEY)
    except Exception:
        return None
```

**Phase 87 action:** Add to allowlist with justification. Do NOT modify the class. Phase 91 handles atomic auth writes.

---

### `.github/workflows/ci.yml` — no changes needed

**Finding:** The `tests` job already runs `pytest tests/` (line 41). Adding `tests/test_no_raw_storage_access.py` and `tests/test_session_uuid.py` requires zero CI changes — they are collected automatically by pytest.

**CI structure** (lines 20-41):
```yaml
tests:
  needs: lint-and-docs
  strategy:
    matrix:
      include:
        - os: ubuntu-latest
          python-version: '3.11'
        - os: windows-latest
          python-version: '3.11'
  runs-on: ${{ matrix.os }}
  steps:
    - run: pip install -r requirements-lock.txt
    - run: pip install pytest
    - run: pytest tests/
```

The lint test uses `pathlib.Path` for path discovery and `yaml.safe_load` for allowlist parsing. Verify PyYAML is available in `requirements-lock.txt` before plan execution (`python -c "import yaml; print(yaml.__version__)"`).

---

## Shared Patterns

### Pattern A: Safe Storage Import (apply to ALL modified files)

**Source:** `web/components/filter_panel.py` lines 229, 249, 300, 336 (cca23db3 — the reference migration)

```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

Import at module level (top of file) for files with many call sites. For files with 1-2 sites in a single function, inline import inside the function is acceptable (matches existing cca23db3 style).

### Pattern B: Replace Inline try/except Guards (apply to `browse_state.py`, `search_state.py`)

**Source:** `web/pages/browse_state.py` lines 126-130 (before) → helper (after)

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
```

The helper already logs at debug on AssertionError and warning on other exceptions. Remove redundant outer try/except.

### Pattern C: Alias Resolution for Lint Test (apply to `test_no_raw_storage_access.py`)

**Finding from codebase audit:**
- `web/main.py`, `web/pages/*`, `web/components/*` — import `from nicegui import app` → alias `app`
- `web/api.py` — import `from nicegui import app as nicegui_app` → alias `nicegui_app`
- `web/supabase_client.py` — import `from nicegui import app as _app` → alias `_app`

The AST lint must resolve all three aliases. The `_find_app_aliases()` helper in RESEARCH.md handles this correctly by walking `ast.ImportFrom` nodes.

### Pattern D: Mock Setup for New Tests (apply to `test_session_uuid.py`)

**Source:** `tests/test_safe_storage.py` lines 21-29

```python
from unittest.mock import patch, MagicMock

storage = {}  # or MagicMock() with side_effect for failure cases

with patch('web.safe_storage.app') as mock_app:
    mock_app.storage.user = storage
    from web.safe_storage import <helper_name>
    result = <helper_name>(...)
```

Critical: patch target is `'web.safe_storage.app'` — this is the module-level `app` reference inside safe_storage.py, not `nicegui.app` globally.

---

## No Analog Found

| File | Role | Data Flow | Reason |
|---|---|---|---|
| `.planning/phase87_storage_allowlist.yaml` | config/allowlist | — | No YAML allowlist files exist in repo; format is new. Use YAML per RESEARCH.md R-04 rationale. |
| `tests/test_no_raw_storage_access.py` | test/lint-scanner | batch | No existing test walks Python source files via AST. `test_crawler_visibility.py` is the closest (walks module constants) but does not parse Python source. Full pattern provided in RESEARCH.md Code Examples section. |

---

## Metadata

**Analog search scope:** `web/`, `tests/`, `.github/workflows/`
**Files scanned:** 20 source files + 85 test files
**Pattern extraction date:** 2026-05-13

### Key Findings for Planner

1. `web/safe_storage.py` already has the right structure — the 2 new functions are purely additive. Copy the exact try/except/logger shape from the existing 3 helpers.

2. The 6 existing `test_safe_storage.py` tests must not be touched (FOUND-05 hard constraint). New UUID tests go in a separate `tests/test_session_uuid.py`, using the identical `patch('web.safe_storage.app')` mock pattern.

3. `web/main.py:949-962` has a local duplicate of `safe_user_get` and `safe_user_set` — delete both functions and replace all 8 call sites in `main.py` with the imported helpers.

4. `web/auth_state.py` and `web/main.py:1458-1466` (OAuth callback) are ALLOWLISTED, not migrated in Phase 87.

5. `web/components/filter_panel.py` is already migrated — do not re-touch it.

6. `web/api.py` uses `nicegui_app` alias; `web/supabase_client.py` uses `_app` alias. These are the two alias-variant sites that a naive grep would miss. The AST lint test must handle all three aliases.

7. CI requires no changes — `pytest tests/` already collects new test files automatically.
