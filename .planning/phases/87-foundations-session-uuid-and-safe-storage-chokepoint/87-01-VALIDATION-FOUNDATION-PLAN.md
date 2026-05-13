---
phase: 87
plan: 01
type: execute
wave: 0
depends_on: []
files_modified:
  - tests/test_session_uuid.py
  - tests/test_no_raw_storage_access.py
  - .planning/phase87_storage_allowlist.yaml
autonomous: true
requirements:
  - FOUND-01
  - FOUND-03
  - FOUND-04
tags:
  - phase87
  - validation
  - test-skeleton
  - nicegui
  - storage
must_haves:
  truths:
    - "Failing test stubs exist for FOUND-01 (session UUID minting/stability)"
    - "Failing test stubs exist for FOUND-04 (lint scanner)"
    - "Allowlist YAML file exists with at least web/auth_state.py + web/supabase_client.py:111 + web/main.py:1458-1463 + web/export_state.py:48 entries"
    - "PyYAML import works at command line (already verified — version 6.0.3)"
  artifacts:
    - path: "tests/test_session_uuid.py"
      provides: "5 failing unit tests for get_session_uuid / ensure_session_uuid (skeleton; will import-fail until Plan 02 lands)"
      contains: "test_session_uuid_unique_across_100_sessions"
    - path: "tests/test_no_raw_storage_access.py"
      provides: "3 test functions: test_no_raw_storage_access_outside_allowlist, test_lint_rejects_synthetic_violation, test_allowlist_well_formed"
      contains: "_find_app_aliases"
    - path: ".planning/phase87_storage_allowlist.yaml"
      provides: "Allowlist for 4 known bootstrap sites (auth_state.py, supabase_client.py:111, main.py:1458-1463 OAuth callback, export_state.py:48 _TEST_BACKEND)"
      contains: "allowed_raw_access"
  key_links:
    - from: "tests/test_no_raw_storage_access.py"
      to: ".planning/phase87_storage_allowlist.yaml"
      via: "yaml.safe_load + Path lookup at module level"
      pattern: "ALLOWLIST_PATH.*phase87_storage_allowlist\\.yaml"
    - from: "tests/test_session_uuid.py"
      to: "web/safe_storage.py"
      via: "from web.safe_storage import get_session_uuid (will fail until Plan 02)"
      pattern: "from web\\.safe_storage import get_session_uuid"
---

<objective>
Lay the validation foundation for Phase 87 by creating the test files and allowlist scaffold that all subsequent plans will execute against. After this plan: `pytest tests/test_session_uuid.py` and `pytest tests/test_no_raw_storage_access.py` both EXIST and FAIL (the failures are evidence the skeleton is correctly wired; they go GREEN once Plan 02 adds the helpers and Plans 03-06 finish migrations).

Purpose: per research R-09 Wave 0 — establish the failing-test gate so every downstream plan has a concrete pass/fail signal. The allowlist YAML must be created NOW so Plan 04 (which touches main.py and supabase_client.py) can reference it for the bootstrap sites it intentionally leaves raw.

Output: 2 failing test files + 1 allowlist YAML committed.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/ROADMAP.md
@.planning/STATE.md
@.planning/REQUIREMENTS.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-VALIDATION.md
@tests/test_safe_storage.py
@web/safe_storage.py

<interfaces>
<!-- These are the contracts Plan 02 will implement against. Tests in this plan reference these names; Plan 02 must export them. -->

New additions to web/safe_storage.py (Plan 02 will implement):
```python
def get_session_uuid() -> str:
    """Returns this session's _session_uuid, minting one lazily on first call.

    Returns a 32-char hex string (uuid4().hex). NEVER returns None.
    On prune-race AssertionError: returns ephemeral UUID (do NOT cache).
    Stable across token refresh because keyed in storage, not auth_session.
    """

def ensure_session_uuid() -> bool:
    """Eagerly mints session UUID. Returns True on success, False on prune-race."""

_SESSION_UUID_KEY = '_session_uuid'  # Module constant
```

Existing helpers (unchanged — DO NOT modify):
```python
def safe_user_get(key: str, default: Any = None) -> Any
def safe_user_set(key: str, value: Any) -> bool
def safe_user_pop(key: str, default: Any = None) -> Any
```

Allowlist YAML schema (Plan 04+ will add entries; this plan seeds 4):
```yaml
allowed_raw_access:
  - file: <relative POSIX path from repo root>
    patterns:
      - "<exact substring matched against ast.get_source_segment output>"
    justification: |
      <multi-line text — REQUIRED per FOUND-03>
```
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Create tests/test_session_uuid.py with 5 failing unit-test stubs (FOUND-01)</name>
  <read_first>
    - tests/test_safe_storage.py (FULL FILE — this is the mock pattern reference; copy the `with patch('web.safe_storage.app') as mock_app` style exactly)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (read the "Concurrency Test for FOUND-01" code block at lines 519-601)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "tests/test_session_uuid.py (new test file)" section)
    - web/safe_storage.py (so you see the module-level `from nicegui import app` — that is what tests patch via `'web.safe_storage.app'`)
  </read_first>
  <files>tests/test_session_uuid.py</files>
  <action>
Create `tests/test_session_uuid.py` (NEW file) with 5 unit tests. The tests import `get_session_uuid` and `ensure_session_uuid` from `web.safe_storage` — those helpers do NOT exist yet, so all 5 tests will FAIL with ImportError. That failure is the expected Wave 0 state; Plan 02 makes them green.

Exact file content to write:

```python
"""Tests for Phase 87 FOUND-01: per-session UUID minting.

Success criterion (ROADMAP Phase 87 SC1): a second concurrent browser session
never receives the same _session_uuid as the first session across 100 simulated
independent requests.

Uses the same mock pattern as tests/test_safe_storage.py — patch
'web.safe_storage.app' (the module-level import) and set mock_app.storage.user
to a per-iteration dict (= per-session simulation).
"""
from unittest.mock import patch, MagicMock


PRUNED_SESSION_MSG = (
    "user storage for 6432b6d0-538a-4129-90a3-3ba9a6085e93 should be "
    "created before accessing it"
)


def test_session_uuid_unique_across_100_sessions():
    """FOUND-01 SC1: 100 simulated sessions each get a unique UUID."""
    uuids_seen = set()
    for i in range(100):
        storage = {}  # Fresh "session" per iteration
        with patch('web.safe_storage.app') as mock_app:
            mock_app.storage.user = storage
            from web.safe_storage import get_session_uuid
            uid = get_session_uuid()
            assert uid, f"Iteration {i}: get_session_uuid returned empty"
            assert isinstance(uid, str), f"Iteration {i}: not a str"
            assert len(uid) == 32, f"Iteration {i}: not 32-char hex (got {len(uid)})"
            uuids_seen.add(uid)
    assert len(uuids_seen) == 100, f"Expected 100 unique UUIDs, got {len(uuids_seen)} (collision!)"


def test_session_uuid_stable_within_session():
    """FOUND-01: Calling get_session_uuid() twice returns the same UUID."""
    storage = {}
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        uid1 = get_session_uuid()
        uid2 = get_session_uuid()
        assert uid1 == uid2
        assert storage.get('_session_uuid') == uid1


def test_session_uuid_survives_token_refresh():
    """FOUND-01: Mutating auth_session does NOT change _session_uuid."""
    storage = {'auth_session': {'access_token': 'tok-A'}}
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        uid_before = get_session_uuid()
        # Simulate token refresh
        storage['auth_session'] = {'access_token': 'tok-B'}
        uid_after = get_session_uuid()
        assert uid_before == uid_after


def test_session_uuid_returns_ephemeral_on_prune():
    """When storage raises AssertionError, return ephemeral UUID without caching."""
    storage = MagicMock()
    storage.get.side_effect = AssertionError(PRUNED_SESSION_MSG)
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        uid = get_session_uuid()
        assert uid
        assert isinstance(uid, str)
        assert len(uid) == 32


def test_ensure_session_uuid_idempotent():
    """ensure_session_uuid() can be called repeatedly with no effect."""
    storage = {}
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import ensure_session_uuid
        assert ensure_session_uuid() is True
        first_uid = storage.get('_session_uuid')
        assert first_uid
        assert ensure_session_uuid() is True
        assert storage.get('_session_uuid') == first_uid  # Unchanged
```

After writing: `pytest tests/test_session_uuid.py -x` MUST fail with ImportError (because `get_session_uuid` doesn't yet exist in `web/safe_storage.py`). This failure is the Wave 0 evidence.
  </action>
  <verify>
    <automated>pytest tests/test_session_uuid.py --collect-only 2>&1 | grep -E "(test_session_uuid_unique_across_100_sessions|test_session_uuid_stable_within_session|test_session_uuid_survives_token_refresh|test_session_uuid_returns_ephemeral_on_prune|test_ensure_session_uuid_idempotent)" | wc -l</automated>
  </verify>
  <acceptance_criteria>
    - File `tests/test_session_uuid.py` exists
    - `grep -c "def test_" tests/test_session_uuid.py` returns exactly 5
    - `grep -c "from web.safe_storage import get_session_uuid" tests/test_session_uuid.py` returns at least 4 (4 of 5 tests use this import; idempotent test imports ensure_session_uuid)
    - `grep -c "from web.safe_storage import ensure_session_uuid" tests/test_session_uuid.py` returns 1
    - `grep -c "patch('web.safe_storage.app')" tests/test_session_uuid.py` returns 5
    - Running `pytest tests/test_session_uuid.py --collect-only` exits 0 AND lists exactly 5 tests
    - Running `pytest tests/test_session_uuid.py -x` exits non-zero (ImportError on `get_session_uuid` — expected pre-Plan-02 state)
  </acceptance_criteria>
  <done>Test file exists with 5 stubs that fail import; collect-only succeeds.</done>
</task>

<task type="auto">
  <name>Task 2: Create .planning/phase87_storage_allowlist.yaml with 4 bootstrap-site entries</name>
  <read_first>
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read ".planning/phase87_storage_allowlist.yaml" section for format)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (read R-04 ALLOWLIST FILE FORMAT and Pitfall 5 for filter_panel exclusion)
    - web/auth_state.py (FULL FILE — verify the 9 raw-access lines: 42, 50, 95, 97, 117, 122, 123, 124, 176)
    - web/supabase_client.py lines 95-130 (verify the get_user_client captured-handle anti-pattern at line 111)
    - web/main.py lines 1455-1470 (verify OAuth callback's 3-key atomic write at 1458, 1460, 1463)
    - web/export_state.py (FULL FILE — line 48 returns app.storage.user; this is the _TEST_BACKEND fallthrough; Phase 88 STATE-04 deletes this entire shim)
  </read_first>
  <files>.planning/phase87_storage_allowlist.yaml</files>
  <action>
Create `.planning/phase87_storage_allowlist.yaml` (NEW file). This is the seed allowlist; subsequent plans may add entries via Plan 07.

Exact file content:

```yaml
# Phase 87 Storage Allowlist
# Each entry exempts specific raw `app.storage.user` access patterns from the
# lint test in tests/test_no_raw_storage_access.py. Per FOUND-03, every entry
# MUST have a justification.
#
# Pattern matching is substring-based against the AST node source segment.
# Patterns are matched per-file; an entry applies ONLY to its `file:` path.
#
# To remove an entry: migrate the call site to web.safe_storage helpers.
# To add an entry: open a PR with justification — entries require review.

allowed_raw_access:
  - file: web/auth_state.py
    patterns:
      - "app.storage.user.get(cls.USER_KEY)"
      - "app.storage.user.get(cls.PROFILE_KEY)"
      - "app.storage.user[cls.USER_KEY]"
      - "app.storage.user[cls.PROFILE_KEY]"
      - "app.storage.user['auth_session']"
      - "app.storage.user.pop(cls.USER_KEY, None)"
      - "app.storage.user.pop(cls.PROFILE_KEY, None)"
      - "app.storage.user.pop('auth_session', None)"
    justification: |
      GlobalAuthState class methods (lines 42, 50, 95, 97, 117, 122, 123, 124,
      176) already wrap each access in try/except. Phase 91 AUTHW-01 explicitly
      migrates this file as part of the atomic auth-write refactor. Migrating
      here would duplicate Phase 91's work. The atomicity guarantees Phase 91
      needs (write all 3 auth keys together or roll back) require coordinated
      changes that Phase 87 cannot make in isolation. See REQUIREMENTS.md
      AUTHW-01 for the migration plan.

  - file: web/main.py
    patterns:
      - "app.storage.user[GlobalAuthState.USER_KEY]"
      - "app.storage.user[GlobalAuthState.PROFILE_KEY]"
      - "app.storage.user['auth_session']"
    justification: |
      OAuth callback handler at main.py:1458-1463 writes USER_KEY, PROFILE_KEY,
      and auth_session as a 3-key atomic block during the OAuth success path.
      Atomicity of this multi-key write must be preserved for security
      (half-login state is worse than no-login). Phase 91 AUTHW-02 explicitly
      migrates this site as part of the auth-write atomicity refactor with
      appropriate test coverage (test_auth_callback_resilience.py).
      All OTHER main.py raw accesses (lines 327, 493, 567, 587, 598, 657, 663,
      664, 691, 820, 952, 960, 968, 1283) are migrated in Phase 87 Plan 04.

  - file: web/supabase_client.py
    patterns:
      - "_app.storage.user"
    justification: |
      Line 111 (`storage = _app.storage.user`) is the captured-handle pattern
      inside `get_user_client()`. Codex round 4 CRITICAL-1 flagged this as
      unsafe (FilePersistentDict can be GC'd mid-flight). Phase 90 AUTHC-01
      DELETES `get_user_client()` entirely (and `_client_cache`,
      `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL`), making this
      allowlist entry self-eliminating. Line 263 (`sign_out`) IS migrated
      in Phase 87 Plan 04 (uses `_app.storage.user.get` alias, separate site).

  - file: web/export_state.py
    patterns:
      - "app.storage.user"
    justification: |
      Line 48 (`return app.storage.user`) is the production fallthrough inside
      `_backend()`, which exists ONLY to support the `_TEST_BACKEND` test
      injection shim. Phase 88 STATE-04 explicitly deletes `_TEST_BACKEND`
      and replaces it with proper fixture injection. Migrating to
      safe_user_get/set/pop here would not match the function's contract (it
      returns the dict-like backend object itself, not a value-for-key). This
      entry is self-eliminating once Phase 88 lands.
```

After writing, validate the YAML:
```bash
python -c "import yaml; data = yaml.safe_load(open('.planning/phase87_storage_allowlist.yaml')); print(len(data['allowed_raw_access']), 'entries:', [e['file'] for e in data['allowed_raw_access']])"
```

Expected output: `4 entries: ['web/auth_state.py', 'web/main.py', 'web/supabase_client.py', 'web/export_state.py']`
  </action>
  <verify>
    <automated>python -c "import yaml; data = yaml.safe_load(open('.planning/phase87_storage_allowlist.yaml')); assert len(data['allowed_raw_access']) == 4; assert {e['file'] for e in data['allowed_raw_access']} == {'web/auth_state.py', 'web/main.py', 'web/supabase_client.py', 'web/export_state.py'}; assert all('justification' in e and e['justification'].strip() for e in data['allowed_raw_access']); print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File `.planning/phase87_storage_allowlist.yaml` exists
    - `python -c "import yaml; yaml.safe_load(open('.planning/phase87_storage_allowlist.yaml'))"` exits 0
    - YAML contains exactly 4 entries under `allowed_raw_access` key
    - Files allowed: `web/auth_state.py`, `web/main.py`, `web/supabase_client.py`, `web/export_state.py` (exact match)
    - Every entry has a non-empty `justification` field (multi-line text)
    - Every entry has a non-empty `patterns` list
    - `grep -c "Phase 91" .planning/phase87_storage_allowlist.yaml` returns at least 2 (auth_state + main.py both cite Phase 91)
    - `grep -c "Phase 90" .planning/phase87_storage_allowlist.yaml` returns at least 1 (supabase_client cites Phase 90)
    - `grep -c "Phase 88" .planning/phase87_storage_allowlist.yaml` returns at least 1 (export_state cites Phase 88)
  </acceptance_criteria>
  <done>Allowlist YAML exists, parses cleanly, has 4 entries with justifications referencing the correct downstream phases.</done>
</task>

<task type="auto">
  <name>Task 3: Create tests/test_no_raw_storage_access.py with AST-scan + synthetic-rejection + schema tests</name>
  <read_first>
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (read "Code Examples - Lint Implementation - AST-based pytest test" at lines 396-516 — copy the structure)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "tests/test_no_raw_storage_access.py (new AST-lint test)" section)
    - .planning/phase87_storage_allowlist.yaml (CREATED IN TASK 2 — must exist before writing this test)
    - tests/test_safe_storage.py (style reference for test file header)
  </read_first>
  <files>tests/test_no_raw_storage_access.py</files>
  <action>
Create `tests/test_no_raw_storage_access.py` (NEW file). This is the lint scanner. It will INITIALLY FAIL `test_no_raw_storage_access_outside_allowlist` because 130+ raw access sites still exist (migrations happen in Plans 03-06). That failure is the Wave 0 expected state.

Exact file content:

```python
"""Lint test: reject raw app.storage.user access outside the Phase 87 allowlist.

Reads .planning/phase87_storage_allowlist.yaml and scans every .py file under
web/ for AST nodes matching:
  - <app_alias>.storage.user.get(...)
  - <app_alias>.storage.user.pop(...)
  - <app_alias>.storage.user[...]  (Subscript both read and assign)

Where <app_alias> is any name bound to `from nicegui import app[ as ALIAS]`.
The three known aliases in this codebase are `app`, `nicegui_app`, `_app`.

Phase 87 FOUND-04 success criterion: this scan returns ZERO violations
outside the allowlist after Plans 02-06 land.
"""
import ast
import textwrap
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = REPO_ROOT / 'web'
ALLOWLIST_PATH = REPO_ROOT / '.planning' / 'phase87_storage_allowlist.yaml'


def _load_allowlist() -> dict:
    if not ALLOWLIST_PATH.exists():
        return {'allowed_raw_access': []}
    with ALLOWLIST_PATH.open('r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {'allowed_raw_access': []}


def _find_app_aliases(tree: ast.AST) -> set:
    """Return names bound to `nicegui.app` in this module.

    Handles: `from nicegui import app`, `from nicegui import app as nicegui_app`,
    `from nicegui import app as _app`.
    """
    aliases = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'nicegui':
            for alias in node.names:
                if alias.name == 'app':
                    aliases.add(alias.asname or 'app')
    return aliases


def _is_storage_user_access(node, app_aliases):
    """Return True if `node` is an access to <app_alias>.storage.user.* .

    Handles:
      - ast.Call where func is Attribute on app.storage.user (e.g. .get/.pop)
      - ast.Subscript where value is Attribute on app.storage.user (e.g. ['key'])
      - ast.Attribute where the attr chain ends in .storage.user (catches assignment LHS)
    """
    target = node
    if isinstance(target, ast.Call):
        target = target.func
    elif isinstance(target, ast.Subscript):
        target = target.value
    # Walk up the Attribute chain
    chain = []
    cur = target
    while isinstance(cur, ast.Attribute):
        chain.append(cur.attr)
        cur = cur.value
    if not isinstance(cur, ast.Name):
        return False
    if cur.id not in app_aliases:
        return False
    # chain is reversed: for `app.storage.user.get(...)` chain == ['get', 'user', 'storage']
    # for `app.storage.user[...]` chain == ['user', 'storage']
    # for `app.storage.user` (bare access) chain == ['user', 'storage']
    if len(chain) < 2:
        return False
    return chain[-2:] == ['storage', 'user']


def _node_source(source_text, node):
    """Return the source segment for an AST node, or empty string if unavailable."""
    seg = ast.get_source_segment(source_text, node)
    return seg or ''


def _is_allowlisted(rel_path: str, source_segment: str, allowed: dict) -> bool:
    """Return True if (rel_path, source_segment) matches any allowlist entry."""
    entry = allowed.get(rel_path)
    if not entry:
        return False
    for pat in entry.get('patterns', []):
        if pat in source_segment:
            return True
    return False


def test_allowlist_well_formed():
    """FOUND-03 schema check: every allowlist entry has file + patterns + justification."""
    allowlist = _load_allowlist()
    entries = allowlist.get('allowed_raw_access', [])
    assert entries, "Allowlist is empty — at minimum web/auth_state.py should be allowlisted"
    for entry in entries:
        assert 'file' in entry, f"Entry missing 'file': {entry}"
        assert 'patterns' in entry, f"Entry {entry['file']} missing 'patterns'"
        assert entry['patterns'], f"Entry {entry['file']} has empty patterns list"
        assert 'justification' in entry, f"Entry {entry['file']} missing 'justification'"
        assert entry['justification'].strip(), f"Entry {entry['file']} has empty justification"


def test_lint_rejects_synthetic_violation():
    """FOUND-04 SC4: verify the lint visitor would detect a synthetic raw access."""
    synthetic = textwrap.dedent("""
        from nicegui import app
        def bad():
            return app.storage.user.get('foo')
    """)
    tree = ast.parse(synthetic)
    aliases = _find_app_aliases(tree)
    assert aliases == {'app'}, f"Expected alias 'app', got {aliases}"
    found_raw = False
    for node in ast.walk(tree):
        if isinstance(node, (ast.Call, ast.Subscript, ast.Attribute)):
            if _is_storage_user_access(node, aliases):
                found_raw = True
                break
    assert found_raw, "Lint visitor failed to detect synthetic raw access"


def test_lint_handles_aliased_imports():
    """FOUND-04: verify alias resolution catches `nicegui_app` and `_app` aliases."""
    for alias_form in [
        "from nicegui import app as nicegui_app\ndef bad():\n    return nicegui_app.storage.user.get('x')\n",
        "from nicegui import app as _app\ndef bad():\n    return _app.storage.user.get('x')\n",
    ]:
        tree = ast.parse(alias_form)
        aliases = _find_app_aliases(tree)
        assert len(aliases) == 1, f"Expected 1 alias for {alias_form!r}, got {aliases}"
        found = any(
            isinstance(n, (ast.Call, ast.Subscript, ast.Attribute))
            and _is_storage_user_access(n, aliases)
            for n in ast.walk(tree)
        )
        assert found, f"Alias resolution failed for {alias_form!r}"


def test_no_raw_storage_access_outside_allowlist():
    """FOUND-04 SC4: production code under web/ has no raw access outside allowlist.

    THIS TEST FAILS DURING WAVE 0 — migrations land in Plans 03-06.
    It must be GREEN by end of Plan 07 (Lint Finalization).
    """
    allowlist = _load_allowlist()
    allowed = {entry['file']: entry for entry in allowlist.get('allowed_raw_access', [])}
    violations = []
    for path in WEB_DIR.rglob('*.py'):
        if path.name == 'safe_storage.py':
            continue  # The chokepoint itself
        rel = path.relative_to(REPO_ROOT).as_posix()
        with path.open('r', encoding='utf-8') as f:
            source = f.read()
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as e:
            pytest.fail(f"AST parse failed for {rel}: {e}")
        app_aliases = _find_app_aliases(tree)
        if not app_aliases:
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.Call, ast.Subscript, ast.Attribute)):
                if _is_storage_user_access(node, app_aliases):
                    seg = _node_source(source, node)
                    if _is_allowlisted(rel, seg, allowed):
                        continue
                    violations.append(f"{rel}:{node.lineno}: {seg or '<no segment>'}")
    if violations:
        msg = (
            "Raw app.storage.user access found outside allowlist:\n  "
            + "\n  ".join(violations[:50])  # cap at 50 for readability
            + f"\n\nTotal violations: {len(violations)}"
            + "\n\nFix: migrate to web.safe_storage helpers (safe_user_get/set/pop)"
            + " or add to .planning/phase87_storage_allowlist.yaml with justification."
        )
        pytest.fail(msg)
```

After writing: `pytest tests/test_no_raw_storage_access.py -x` MUST behave as:
- `test_allowlist_well_formed` → PASS (allowlist YAML from Task 2 is well-formed)
- `test_lint_rejects_synthetic_violation` → PASS (synthetic detection works)
- `test_lint_handles_aliased_imports` → PASS (alias resolution works)
- `test_no_raw_storage_access_outside_allowlist` → FAIL (~130 violations because Plans 03-06 haven't run yet)
  </action>
  <verify>
    <automated>pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports -x</automated>
  </verify>
  <acceptance_criteria>
    - File `tests/test_no_raw_storage_access.py` exists
    - `grep -c "def test_" tests/test_no_raw_storage_access.py` returns exactly 4 (allowlist_well_formed, lint_rejects_synthetic, lint_handles_aliased, no_raw_storage_access)
    - `grep -c "def _find_app_aliases" tests/test_no_raw_storage_access.py` returns 1
    - `grep -c "def _is_storage_user_access" tests/test_no_raw_storage_access.py` returns 1
    - `grep -c "yaml.safe_load" tests/test_no_raw_storage_access.py` returns at least 1
    - `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x` exits 0 (PASSES — the YAML from Task 2 is valid)
    - `pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation -x` exits 0 (PASSES — synthetic detection works)
    - `pytest tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports -x` exits 0 (PASSES — alias resolution works)
    - `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist -x` exits non-zero (FAILS — production code still has raw access; this is expected at Wave 0 and gates Plans 03-06 to complete)
    - tests/test_safe_storage.py file SHA unchanged from baseline (use `git diff --stat tests/test_safe_storage.py` — must return empty)
  </acceptance_criteria>
  <done>Lint scanner file exists; 3 of 4 tests pass; the 4th (full production scan) fails as expected and will go green after migration plans land.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Test runner → web/ source files | The lint test reads .py files from disk; does NOT import them. Source files are first-party and trusted; no untrusted input crosses this boundary. |
| YAML allowlist → test runner | YAML parsed via `yaml.safe_load` (rejects arbitrary code execution). File is git-tracked and code-reviewed. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-04 | Tampering | Allowlist pattern matching | mitigate | Use substring match against `ast.get_source_segment()` output (not line numbers). Line numbers drift on refactor; source-segment patterns survive. Encoded in `_is_allowlisted()`. |
| T-87-05 | Information disclosure | Alias resolution in lint scanner | mitigate | `_find_app_aliases()` walks `ast.ImportFrom` nodes; resolves `app`, `nicegui_app`, `_app`. Verified by `test_lint_handles_aliased_imports`. |
| T-87-04b | Tampering | YAML allowlist file | mitigate | `yaml.safe_load` (not `yaml.load`) — rejects Python object instantiation. Schema validated by `test_allowlist_well_formed` (every entry must have file + patterns + justification). |
| — | Spoofing | — | accept | No spoofing surface — internal test infrastructure only |
| — | Repudiation | — | accept | No audit-log requirement for lint scanner |
| — | DoS | AST parse of 16 files | accept | ~500ms scan; negligible at this scale (verified by R-03 timing) |
| — | Elevation | — | accept | Test runs in CI with project credentials; no escalation possible |
</threat_model>

<verification>
After all 3 tasks:

```bash
# Verify Wave 0 files exist
ls tests/test_session_uuid.py tests/test_no_raw_storage_access.py .planning/phase87_storage_allowlist.yaml

# Verify YAML parses
python -c "import yaml; print(len(yaml.safe_load(open('.planning/phase87_storage_allowlist.yaml'))['allowed_raw_access']))"
# Expected: 4

# Verify test collection succeeds
pytest tests/test_session_uuid.py tests/test_no_raw_storage_access.py --collect-only -q
# Expected: 9 tests collected (5 + 4)

# Verify expected failures
pytest tests/test_session_uuid.py -x 2>&1 | grep -i "ImportError.*get_session_uuid" || echo "EXPECTED FAILURE NOT PRESENT"
# Expected: ImportError message present

pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist -x 2>&1 | grep -c "Raw app.storage.user access found outside allowlist"
# Expected: >= 1 (the fail message)

# Verify the 3 standalone tests pass
pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports -x
# Expected: exit 0, 3 passed

# Verify FOUND-05 not broken
sha256sum tests/test_safe_storage.py
# Record this hash — must be identical at end of phase
```
</verification>

<success_criteria>
1. `tests/test_session_uuid.py` exists with 5 tests; collect-only succeeds; tests fail with ImportError (expected pre-Plan-02)
2. `.planning/phase87_storage_allowlist.yaml` exists with 4 well-formed entries; parses with `yaml.safe_load`; each entry has file+patterns+justification
3. `tests/test_no_raw_storage_access.py` exists with 4 tests; 3 pass (allowlist_well_formed, lint_rejects_synthetic, lint_handles_aliased); 4th fails as expected (production scan)
4. `tests/test_safe_storage.py` is byte-identical to baseline (FOUND-05 invariant — sha256 unchanged)
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-01-SUMMARY.md` summarizing:
- Files created (with paths + line counts)
- Test counts (passing vs expected-to-fail)
- Allowlist entries (4 file paths + brief justification summary)
- Baseline sha256 of `tests/test_safe_storage.py` for end-of-phase comparison
- Confirmation that PyYAML 6.0.3 + NiceGUI 3.8.0 verified available
</output>
