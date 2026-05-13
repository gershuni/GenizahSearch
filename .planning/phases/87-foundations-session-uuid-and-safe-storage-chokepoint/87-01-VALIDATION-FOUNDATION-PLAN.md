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
    - "Failing test stubs exist for FOUND-01 (session UUID minting/stability) including poisoning, uppercase, non-string, AssertionError test cases per M5"
    - "Failing test stubs exist for FOUND-04 (lint scanner) using a corrected AST chain check that matches actual ast.walk output (see B2 fix)"
    - "Route-coverage regression guard exists (test_every_ui_page_handler_mints_uuid) — enforces that every @ui.page handler in web/main.py either calls create_layout() or ensure_session_uuid() (Fix 1 in 87-REVIEWS.md iteration 3 — Codex B1-residual)"
    - "Allowlist YAML file exists with at least web/auth_state.py + web/supabase_client.py:111 + web/main.py:1458-1463 + web/export_state.py:48 entries, each with expected_count (per H1)"
    - "PyYAML 6.0.3 verified available (yaml.__version__ matches)"
    - "AST scanner uses parent-tracking to avoid double-reporting (B2)"
    - "test_allowlist_counts_exact fails loudly (not silent-skip) when an allowlisted file's nicegui app import is removed but expected_count > 0 (Fix 3 in 87-REVIEWS.md iteration 3 — Codex MEDIUM)"
  artifacts:
    - path: "tests/test_session_uuid.py"
      provides: "10 failing unit tests for get_session_uuid / ensure_session_uuid + route-coverage wiring guard (skeleton; 5 base + 4 from M5 — uppercase, non-string, malformed, AssertionError-on-write — + 1 from B1-residual: test_every_ui_page_handler_mints_uuid)"
      contains: "test_session_uuid_unique_across_100_sessions"
    - path: "tests/test_no_raw_storage_access.py"
      provides: "6 test functions: test_no_raw_storage_access_outside_allowlist, test_lint_rejects_synthetic_violation, test_lint_handles_aliased_imports, test_lint_does_not_double_report_nested_nodes (B2 regression guard), test_allowlist_well_formed, test_allowlist_counts_exact (the H1 expected_count enforcement test)"
      contains: "_find_app_aliases"
    - path: ".planning/phase87_storage_allowlist.yaml"
      provides: "Allowlist for 4 known bootstrap sites with expected_count per entry pattern (H1)"
      contains: "expected_count"
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

**REVISION (B2, H1, M5, L2 from 87-REVIEWS.md):**
- The AST scanner is rewritten with explicit chain semantics that match actual `ast.walk` output (Codex B2). Verbatim chain shapes are documented in this plan and verified via a real `ast.parse` repl in `<action>`.
- Parent tracking prevents double-reporting nested nodes (B2 second half).
- The allowlist schema now has `expected_count` per pattern (H1), and a new `test_allowlist_counts_exact` test enforces it.
- New UUID validation tests covering uppercase, non-string, malformed, and AssertionError cases (M5).
- PyYAML availability is confirmed at the start of Wave 0 (L2).

Purpose: per research R-09 Wave 0 — establish the failing-test gate so every downstream plan has a concrete pass/fail signal.

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
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md
@tests/test_safe_storage.py
@web/safe_storage.py

<interfaces>
<!-- Contracts that Plan 02 will implement against. Tests in this plan reference these names; Plan 02 must export them. -->

New additions to web/safe_storage.py (Plan 02 will implement, revised per M5):
```python
def get_session_uuid() -> str:
    """Returns this session's _session_uuid, minting one lazily on first call.

    Returns a 32-char lowercase hex string. Uses re.fullmatch(r"^[0-9a-f]{32}$")
    for validation on read; uppercase / non-string / malformed values trigger
    regeneration. On prune-race AssertionError: returns ephemeral UUID4 hex
    (do NOT cache). NEVER returns None. NEVER raises.
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

Allowlist YAML schema (revised per H1 — every pattern now has expected_count):
```yaml
allowed_raw_access:
  - file: <relative POSIX path from repo root>
    patterns:
      - source: "<exact substring matched against ast.get_source_segment output>"
        expected_count: <int>
        enclosing: <optional function/class name string>
    justification: |
      <multi-line text — REQUIRED per FOUND-03>
```
</interfaces>

<ast_chain_facts>
<!-- Verbatim output from running ast.parse() + the proposed walker in a Python 3.11 repl.
     This block defines the chain semantics the scanner MUST implement (per B2 fix).
     Walking `ast.Attribute` nodes from an outer .func/.value reference inward
     (cur = node.func or node.value, then cur = cur.value while isinstance Attribute):

     For Call:     app.storage.user.get('foo')
       func is Attribute(attr='get', value=Attribute(attr='user', value=Attribute(attr='storage', value=Name('app'))))
       Walking cur = func.value and appending cur.attr at each step:
         step 1: cur = Attribute(attr='user', ...); chain.append('user') -> chain=['user']  ;  cur = cur.value (= Attribute('storage', ...))
         step 2: cur = Attribute(attr='storage', ...); chain.append('storage') -> chain=['user', 'storage']  ;  cur = cur.value (= Name('app'))
       Final state: chain = ['user', 'storage'], cur = Name(id='app').

     For Subscript: app.storage.user['baz']
       value is Attribute(attr='user', value=Attribute(attr='storage', value=Name('app')))
       Walking cur = node.value and appending at each step:
         step 1: cur = Attribute(attr='user', ...); chain.append('user') -> chain=['user']  ;  cur = cur.value (= Attribute('storage', ...))
         step 2: cur = Attribute(attr='storage', ...); chain.append('storage') -> chain=['user', 'storage']  ;  cur = cur.value (= Name('app'))
       Final state: chain = ['user', 'storage'], cur = Name(id='app').

     CORRECT CHECK (the bugfix): match the LAST TWO elements of `chain` against `['user', 'storage']`.
     Codex's review wrote `chain[-2:] == ['storage', 'user']` — that order is WRONG given how the loop appends.
     Actual walk produces inner-first order: ['user', 'storage'] (because .attr='user' is one step closer
     to the outermost node than .attr='storage').

     For a Call, we extract the underlying Attribute via `target = node.func`, then run the walk on `target.value`.
     For a Subscript, we extract via `target = node.value`, then run the walk on `target.value`.
     For a bare Attribute LHS (e.g., `app.storage.user = x`), we walk from the node itself.

     PARENT TRACKING (B2 second half): if we visit ast.Call(func=Attribute(...)) AND we also visit
     ast.Attribute children via ast.walk, the inner Attribute would be reported twice. To avoid:
     - First pass: collect parent of every node via ast.walk + setattr(child, '_parent', node)
       (or equivalent — using a NodeVisitor that records parent on entry).
     - When visiting an ast.Attribute, IGNORE it if its parent is an ast.Call where this Attribute is .func,
       or an ast.Subscript where this Attribute is .value, or an ast.Assign target (we want the assign target
       to be checked once, but we want it noticed as a write — check parent != Call/Subscript).
     The implementation below uses a NodeVisitor approach that visits Call and Subscript outermost-first and
     only checks bare Attribute when it is NOT the immediate child of a Call.func or Subscript.value.
-->
</ast_chain_facts>

<allowlist_matcher>
<!-- Substring matching against `ast.get_source_segment(source, node)` is the
     contract between the allowlist YAML and the AST scanner. This block makes
     the matching semantics EXPLICIT so allowlist authors don't accidentally
     write patterns that never match.

     What `ast.get_source_segment` records for each violation node:
       - For Call(func=Attribute(...)):  the WHOLE call expression including args.
         Example node `app.storage.user.get('foo', None)` → segment is
         `"app.storage.user.get('foo', None)"`.
       - For Subscript(value=Attribute(...)):  the WHOLE subscript expression.
         Example `app.storage.user[cls.PROFILE_KEY]` → segment is
         `"app.storage.user[cls.PROFILE_KEY]"`. NOTE: this does NOT include the
         enclosing assignment or return statement.
       - For bare Attribute (parent is not Call.func / Subscript.value):  ONLY
         the chain expression. Example `return app.storage.user` → segment is
         `"app.storage.user"` (NOT `"return app.storage.user"`). Example
         `storage = _app.storage.user` → segment is `"_app.storage.user"` (NOT
         `"storage = _app.storage.user"`).

     Matcher contract (in `_is_allowlisted` below): given an allowlist entry
     pattern `source: <S>` and a recorded segment `seg`, the entry matches iff
     `S in seg` (Python substring containment). The `enclosing` field is
     documentation-only in this revision — it scopes the human-readable intent
     but the matcher does NOT walk node.parent to verify the function name (a
     future strict-mode revision could add that check).

     **Authoring rule for allowlist patterns:** write `source:` as a substring
     of what `ast.get_source_segment` will record, NOT as a substring of the
     human source line. When in doubt, pull the file open, find the offending
     access, and write the pattern as the smallest unique substring of the
     `Attribute`/`Subscript`/`Call` expression itself.
-->
</allowlist_matcher>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Verify PyYAML available (L2) and create tests/test_session_uuid.py with 10 unit-test stubs (FOUND-01 + M5 + B1-residual route-coverage)</name>
  <read_first>
    - tests/test_safe_storage.py (FULL FILE — this is the mock pattern reference; copy the `with patch('web.safe_storage.app') as mock_app` style exactly)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (read the "Concurrency Test for FOUND-01" code block at lines 519-601)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "tests/test_session_uuid.py (new test file)" section)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (M5: read the specific UUID validation gaps Codex flagged)
    - web/safe_storage.py (so you see the module-level `from nicegui import app` — that is what tests patch via `'web.safe_storage.app'`)
  </read_first>
  <files>tests/test_session_uuid.py</files>
  <action>
**Step 0 (L2 — PyYAML availability gate):** Before writing anything, verify PyYAML is importable:

```
python -c "import yaml; print(yaml.__version__)"
```

Expected: `6.0.3` (matches the version installed transitively via NiceGUI). If this command fails with ImportError, STOP and add `PyYAML>=6.0` to `requirements.txt` as an explicit direct dependency before proceeding. Do not silently rely on transitive installation.

**Step 1: Create `tests/test_session_uuid.py` (NEW file) with 10 unit tests.**

Tests import `get_session_uuid` and `ensure_session_uuid` from `web.safe_storage` — those helpers do NOT exist yet, so all tests using them will FAIL with ImportError. That failure is the expected Wave 0 state; Plan 02 makes them green.

Test composition (5 original + 4 from M5 + 1 from B1-residual):
1. `test_session_uuid_unique_across_100_sessions` (FOUND-01 SC1)
2. `test_session_uuid_stable_within_session`
3. `test_session_uuid_survives_token_refresh`
4. `test_session_uuid_returns_ephemeral_on_prune` (AssertionError on read)
5. `test_ensure_session_uuid_idempotent`
6. **NEW (M5)** `test_session_uuid_rejects_uppercase_hex` — uppercase stored value triggers regeneration
7. **NEW (M5)** `test_session_uuid_rejects_non_string` — int / None / dict stored values trigger regeneration
8. **NEW (M5)** `test_session_uuid_rejects_malformed_length` — too-short / too-long hex triggers regeneration
9. **NEW (M5)** `test_ensure_session_uuid_returns_false_on_assertion` — AssertionError-during-write returns False without raising
10. **NEW (Fix 1 — B1-residual)** `test_every_ui_page_handler_mints_uuid` — route-coverage regression guard: every @ui.page handler in web/main.py must either call create_layout() or call ensure_session_uuid() directly. Exempt: `/privacy-extension` (zero storage access).

Exact file content to write:

```python
"""Tests for Phase 87 FOUND-01: per-session UUID minting.

Success criterion (ROADMAP Phase 87 SC1): a second concurrent browser session
never receives the same _session_uuid as the first session across 100 simulated
independent requests.

Uses the same mock pattern as tests/test_safe_storage.py — patch
'web.safe_storage.app' (the module-level import) and set mock_app.storage.user
to a per-iteration dict (= per-session simulation).

Revision tests (M5 from 87-REVIEWS.md): the original 5 tests did not exercise
the validation regex on read. New tests 6-9 cover uppercase hex (reject),
non-string stored values (reject), malformed length (reject), and
AssertionError-during-write (ensure_session_uuid returns False).

Iteration 3 revision (Fix 1 in 87-REVIEWS.md — Codex B1-residual): test 10
(test_every_ui_page_handler_mints_uuid) is a route-coverage regression guard
that parses web/main.py and enforces that every @ui.page handler either calls
create_layout() (which calls ensure_session_uuid()) or calls
ensure_session_uuid() directly. The exempt route /privacy-extension is a pure
static info page with zero storage access.
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
    """When storage raises AssertionError on read, return ephemeral UUID without caching."""
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


# ---------------------------------------------------------------------------
# M5 revision tests — strict UUID validation on read.
# Each test simulates a poisoned storage value and asserts that get_session_uuid
# rejects it (regenerates a fresh UUID) rather than returning the poisoned value.
# Per the threat model T-87-02, a malicious user with write access to their own
# session storage could otherwise force a known UUID for cache-key collision.
# ---------------------------------------------------------------------------

def test_session_uuid_rejects_uppercase_hex():
    """Uppercase hex stored value must be rejected; fresh lowercase UUID minted."""
    uppercase = 'ABCDEF1234567890ABCDEF1234567890'  # 32 chars, all hex, but uppercase
    storage = {'_session_uuid': uppercase}
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        uid = get_session_uuid()
        assert uid != uppercase, "Uppercase UUID accepted — validation regex too loose"
        assert isinstance(uid, str)
        assert len(uid) == 32
        # Verify the implementation used re.fullmatch(r"^[0-9a-f]{32}$"):
        assert uid == uid.lower(), "Returned UUID is not lowercase"
        assert all(c in '0123456789abcdef' for c in uid)


def test_session_uuid_rejects_non_string():
    """Non-string stored values (int, dict, None) must be rejected; fresh UUID minted."""
    for poisoned_value in (12345, None, {'malicious': 'dict'}, [1, 2, 3], b'bytes'):
        storage = {'_session_uuid': poisoned_value}
        with patch('web.safe_storage.app') as mock_app:
            mock_app.storage.user = storage
            from web.safe_storage import get_session_uuid
            uid = get_session_uuid()
            assert isinstance(uid, str), f"Non-string {poisoned_value!r} not rejected — got {uid!r}"
            assert len(uid) == 32, f"After rejecting {poisoned_value!r}, fresh UUID not minted correctly"


def test_session_uuid_rejects_malformed_length():
    """Strings of wrong length or with non-hex chars must be rejected."""
    for malformed in ('short', 'a' * 31, 'a' * 33, '!' * 32, 'g' * 32, '0' * 31 + ' '):
        storage = {'_session_uuid': malformed}
        with patch('web.safe_storage.app') as mock_app:
            mock_app.storage.user = storage
            from web.safe_storage import get_session_uuid
            uid = get_session_uuid()
            assert uid != malformed, f"Malformed {malformed!r} accepted — validation failed"
            assert len(uid) == 32, f"After rejecting {malformed!r}, fresh UUID malformed"
            assert all(c in '0123456789abcdef' for c in uid)


def test_ensure_session_uuid_returns_false_on_assertion():
    """ensure_session_uuid() must return False (NOT raise) when storage write raises AssertionError."""
    storage = MagicMock()
    storage.get.return_value = None  # No existing UUID
    # __setitem__ raises AssertionError (simulates prune-race during write)
    storage.__setitem__.side_effect = AssertionError(PRUNED_SESSION_MSG)
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import ensure_session_uuid
        result = ensure_session_uuid()
        assert result is False, "ensure_session_uuid should return False on prune-race write"


# ---------------------------------------------------------------------------
# B1-residual fix (Codex round 2): route-coverage regression guard.
# Asserts that every @ui.page handler in web/main.py either calls create_layout()
# (which calls ensure_session_uuid()) OR calls ensure_session_uuid() directly.
# This test prevents future regressions where a new @ui.page that touches
# storage is added without one of these wiring patterns.
# ---------------------------------------------------------------------------

def test_every_ui_page_handler_mints_uuid():
    """Every @ui.page handler in web/main.py either calls create_layout()
    (which calls ensure_session_uuid()) OR calls ensure_session_uuid() directly.

    The one documented exception is /privacy-extension (pure static info page,
    zero storage access). Adding a new @ui.page that touches storage without
    one of these wiring patterns will fail this test.
    """
    import ast
    import pathlib as _pathlib
    repo_root = _pathlib.Path(__file__).resolve().parent.parent
    source = (repo_root / 'web' / 'main.py').read_text(encoding='utf-8')
    tree = ast.parse(source)
    EXEMPT_ROUTES = {'/privacy-extension'}
    failures = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        # Check decorators for @ui.page('/path')
        page_path = None
        for dec in node.decorator_list:
            if (isinstance(dec, ast.Call) and
                isinstance(dec.func, ast.Attribute) and
                dec.func.attr == 'page' and
                isinstance(dec.func.value, ast.Name) and
                dec.func.value.id == 'ui' and
                dec.args and
                isinstance(dec.args[0], ast.Constant) and
                isinstance(dec.args[0].value, str)):
                page_path = dec.args[0].value
                break
        if page_path is None:
            continue
        if page_path in EXEMPT_ROUTES:
            continue
        # Walk the function body for create_layout() or ensure_session_uuid() call
        body_source = ast.unparse(node)
        has_layout = 'create_layout(' in body_source
        has_ensure = 'ensure_session_uuid(' in body_source
        if not (has_layout or has_ensure):
            failures.append(f"{page_path} (line {node.lineno}): no create_layout() or ensure_session_uuid() call")
    assert not failures, (
        "The following @ui.page handlers in web/main.py do NOT wire ensure_session_uuid():" + chr(10)
        + "  " + (chr(10) + "  ").join(failures)
        + chr(10) + chr(10)
        + "Fix: add `ensure_session_uuid()` to the function OR call create_layout()."
    )
```

After writing: `pytest tests/test_session_uuid.py --collect-only` MUST list exactly 10 tests. Full pytest run will fail with ImportError on `get_session_uuid` until Plan 02 lands.
  </action>
  <verify>
    <automated>python -c "import subprocess, sys; r = subprocess.run([sys.executable, '-m', 'pytest', 'tests/test_session_uuid.py', '--collect-only', '-q'], capture_output=True, text=True); print(r.stdout); print(r.stderr); sys.exit(0 if 'test_session_uuid_unique_across_100_sessions' in r.stdout and r.stdout.count('::test_') == 10 else 1)"</automated>
  </verify>
  <acceptance_criteria>
    - PyYAML is importable: `python -c "import yaml; print(yaml.__version__)"` exits 0 and prints `6.0.3` (or later)
    - File `tests/test_session_uuid.py` exists
    - Python AST count of `def test_*` functions in the file equals 10: `python -c "import ast; print(sum(1 for n in ast.walk(ast.parse(open('tests/test_session_uuid.py').read())) if isinstance(n, ast.FunctionDef) and n.name.startswith('test_')))"` prints `10`
    - File contains all 10 test names: `python -c "src = open('tests/test_session_uuid.py').read(); names = ['test_session_uuid_unique_across_100_sessions', 'test_session_uuid_stable_within_session', 'test_session_uuid_survives_token_refresh', 'test_session_uuid_returns_ephemeral_on_prune', 'test_ensure_session_uuid_idempotent', 'test_session_uuid_rejects_uppercase_hex', 'test_session_uuid_rejects_non_string', 'test_session_uuid_rejects_malformed_length', 'test_ensure_session_uuid_returns_false_on_assertion', 'test_every_ui_page_handler_mints_uuid']; missing = [n for n in names if 'def ' + n not in src]; assert not missing, missing; print('OK')"` prints `OK`
    - `pytest tests/test_session_uuid.py --collect-only -q` exits 0 AND its output contains exactly 10 `::test_` items
    - Running `pytest tests/test_session_uuid.py` exits non-zero (ImportError on `get_session_uuid` — expected pre-Plan-02 state)
    - tests/test_safe_storage.py byte-unchanged from baseline (FOUND-05 invariant): `python -c "import hashlib; print(hashlib.sha256(open('tests/test_safe_storage.py', 'rb').read()).hexdigest())"` produces a stable hash recorded in the SUMMARY
  </acceptance_criteria>
  <done>Test file exists with 10 stubs that fail import; collect-only succeeds with exactly 10 tests; PyYAML availability confirmed.</done>
</task>

<task type="auto">
  <name>Task 2: Create .planning/phase87_storage_allowlist.yaml with 4 bootstrap-site entries (H1 schema with expected_count)</name>
  <read_first>
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (".planning/phase87_storage_allowlist.yaml" section)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (R-04 ALLOWLIST FILE FORMAT and Pitfall 5)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (H1: expected_count schema)
    - web/auth_state.py (FULL FILE — verify the 9 raw-access lines: 42, 50, 95, 97, 117, 122, 123, 124, 176)
    - web/supabase_client.py lines 95-130 (verify the get_user_client captured-handle anti-pattern at line 111)
    - web/main.py lines 1455-1470 (verify OAuth callback's 3-key atomic write at 1458, 1460, 1463)
    - web/export_state.py (FULL FILE — line 48 returns app.storage.user; this is the _TEST_BACKEND fallthrough; Phase 88 STATE-04 deletes this entire shim)
  </read_first>
  <files>.planning/phase87_storage_allowlist.yaml</files>
  <action>
Create `.planning/phase87_storage_allowlist.yaml` (NEW file). Schema revised per H1: each pattern is now an object with `source` (exact substring), `expected_count` (int — how many times the pattern is expected to appear in the file), and optional `enclosing` (function name for human readability and future strict-mode scoping).

**Why expected_count matters (per H1 review):** Old schema used `patterns: [string1, string2]`. If a future refactor accidentally introduces NEW raw access in an allowlisted file that happens to substring-match an existing pattern (e.g., a second `_app.storage.user` line in `web/supabase_client.py`), the substring match would silently legalize it. With `expected_count`, the test `test_allowlist_counts_exact` will fail loudly.

Before writing, run a quick survey to confirm current raw-access counts in each file. **Use Python (not grep) for Windows-safe portability:**

```
python -c "import re, pathlib; src = pathlib.Path('web/auth_state.py').read_text(encoding='utf-8'); print('web/auth_state.py:', len(re.findall(r'app\.storage\.user', src)))"
python -c "import re, pathlib; src = pathlib.Path('web/main.py').read_text(encoding='utf-8'); m = re.findall(r\"app\.storage\.user\[(GlobalAuthState\.USER_KEY|GlobalAuthState\.PROFILE_KEY|'auth_session')\]\", src); print('OAuth 3-key writes in main.py:', len(m))"
python -c "import re, pathlib; src = pathlib.Path('web/supabase_client.py').read_text(encoding='utf-8'); print('_app.storage.user in supabase_client.py:', len(re.findall(r'_app\.storage\.user', src)))"
python -c "import re, pathlib; src = pathlib.Path('web/export_state.py').read_text(encoding='utf-8'); print('app.storage.user in export_state.py:', len(re.findall(r'app\.storage\.user', src)))"
```

Use the actual counts you observe to fill in `expected_count` values. The numbers below are the expected baseline counts from the research — confirm before writing:
- `web/auth_state.py`: 9 raw accesses (lines 42, 50, 95, 97, 117, 122, 123, 124, 176)
- `web/main.py` OAuth callback: 3 raw accesses (lines 1458, 1460, 1463) — but main.py has more raw accesses pre-Plan-04; the allowlist patterns must ONLY match the OAuth 3
- `web/supabase_client.py`: 2 raw accesses pre-Plan-04 (line 111 + line 263); line 263 gets migrated by Plan 04 leaving exactly 1 allowlisted access at line 111
- `web/export_state.py`: 1 raw access (line 48)

**Important:** `expected_count` reflects the POST-PLAN-04 steady state, not the Wave 0 state. The allowlist's purpose is to lock in the FINAL surface area. During Plan 07 execution, the `test_allowlist_counts_exact` will run against the migrated codebase, so the counts must match what remains after Plans 03-06 finish.

Exact file content to write:

```yaml
# Phase 87 Storage Allowlist
# Each entry exempts specific raw `app.storage.user` access patterns from the
# lint test in tests/test_no_raw_storage_access.py. Per FOUND-03, every entry
# MUST have a justification.
#
# Schema (revised per 87-REVIEWS.md H1):
#   patterns: list of objects, each with:
#     - source: exact substring matched against ast.get_source_segment output
#     - expected_count: int — how many AST nodes in the file are expected to
#       match this pattern. Enforced by test_allowlist_counts_exact.
#     - enclosing (optional): function or class name for human readability.
#
# Pattern matching is substring-based against the AST node source segment.
# Patterns are matched per-file; an entry applies ONLY to its `file:` path.
#
# To remove an entry: migrate the call site to web.safe_storage helpers.
# To add an entry: open a PR with justification — entries require review.
# To extend an entry: bump expected_count if the new raw access is also justified.

allowed_raw_access:
  - file: web/auth_state.py
    patterns:
      - source: "app.storage.user.get(cls.USER_KEY)"
        expected_count: 1
        enclosing: "GlobalAuthState.get_user"
      - source: "app.storage.user.get(cls.PROFILE_KEY)"
        expected_count: 1
        enclosing: "GlobalAuthState.get_profile"
      - source: "app.storage.user[cls.USER_KEY]"
        expected_count: 1
        enclosing: "GlobalAuthState.set_auth (line 95)"
      - source: "app.storage.user[cls.PROFILE_KEY]"
        expected_count: 2
        enclosing: "GlobalAuthState.set_auth (line 97) + GlobalAuthState.update_profile_cache (line 117)"
      - source: "app.storage.user['auth_session']"
        expected_count: 1
        enclosing: "GlobalAuthState (auth_session access)"
      - source: "app.storage.user.pop(cls.USER_KEY, None)"
        expected_count: 1
        enclosing: "GlobalAuthState.clear_user"
      - source: "app.storage.user.pop(cls.PROFILE_KEY, None)"
        expected_count: 1
        enclosing: "GlobalAuthState.clear_profile"
      - source: "app.storage.user.pop('auth_session', None)"
        expected_count: 1
        enclosing: "GlobalAuthState (auth_session clear)"
    justification: |
      GlobalAuthState class methods (lines 42, 50, 95, 97, 117, 122, 123, 124,
      176) already wrap each access in try/except. Phase 91 AUTHW-01 explicitly
      migrates this file as part of the atomic auth-write refactor. Migrating
      here would duplicate Phase 91's work. The atomicity guarantees Phase 91
      needs (write all 3 auth keys together or roll back) require coordinated
      changes that Phase 87 cannot make in isolation. See REQUIREMENTS.md
      AUTHW-01 for the migration plan.
      Total expected: 8 unique patterns covering all 9 historical raw-access sites.
      Verified counts in web/auth_state.py (revision-2 audit):
        get(cls.USER_KEY)        =1 (line 42)
        get(cls.PROFILE_KEY)     =1 (line 50)
        [cls.USER_KEY]           =1 (line 95, set_auth)
        [cls.PROFILE_KEY]        =2 (line 97 in set_auth + line 117 in update_profile_cache)
        ['auth_session']         =1 (line 176, do_login)
        pop(cls.USER_KEY, None)  =1 (line 122)
        pop(cls.PROFILE_KEY, None)=1 (line 123)
        pop('auth_session', None)=1 (line 124)
        TOTAL = 9 access sites, 8 distinct source patterns (PROFILE_KEY assignment
        appears twice at lines 97 and 117, sharing source segment).
      expected_count enforces exact match — adding a 10th raw access in this file
      (even one that substring-matches an existing pattern) will fail
      test_allowlist_counts_exact.

  - file: web/main.py
    patterns:
      - source: "app.storage.user[GlobalAuthState.USER_KEY]"
        expected_count: 1
        enclosing: "OAuth callback at main.py:1458"
      - source: "app.storage.user[GlobalAuthState.PROFILE_KEY]"
        expected_count: 1
        enclosing: "OAuth callback at main.py:1460"
      - source: "app.storage.user['auth_session']"
        expected_count: 1
        enclosing: "OAuth callback at main.py:1463"
    justification: |
      OAuth callback handler at main.py:1458-1463 writes USER_KEY, PROFILE_KEY,
      and auth_session as a 3-key atomic block during the OAuth success path.
      Atomicity of this multi-key write must be preserved for security
      (half-login state is worse than no-login). Phase 91 AUTHW-02 explicitly
      migrates this site as part of the auth-write atomicity refactor with
      appropriate test coverage (test_auth_callback_resilience.py).
      All OTHER main.py raw accesses (lines 327, 493, 567, 587, 598, 657, 663,
      664, 691, 820, 952, 960, 968, 1283) are migrated in Phase 87 Plan 04.
      With expected_count=1 per pattern, the OAuth block is locked at exactly
      3 sites — any new raw access in main.py outside this block will fail the
      count test even if it superficially substring-matches.

  - file: web/supabase_client.py
    patterns:
      - source: "_app.storage.user"
        expected_count: 1
        enclosing: "get_user_client (captured-handle pattern, Codex round 4 CRITICAL-1)"
    justification: |
      Line 111 (`storage = _app.storage.user`) is the captured-handle pattern
      inside `get_user_client()`. The AST scanner records the source segment
      of the bare `Attribute` access — which is just `_app.storage.user`, NOT
      the full assignment statement `storage = _app.storage.user`. The pattern
      must be a substring of the recorded segment, so we match on
      `_app.storage.user` and scope it via `enclosing: get_user_client`.
      Codex round 4 CRITICAL-1 flagged this as unsafe (FilePersistentDict can
      be GC'd mid-flight). Phase 90 AUTHC-01 DELETES `get_user_client()`
      entirely (and `_client_cache`, `_session_locks`, `_locks_guard`,
      `_CLIENT_CACHE_TTL`), making this allowlist entry self-eliminating.
      Line 263 (`sign_out`) IS migrated in Phase 87 Plan 04 (uses
      `_app.storage.user.get` alias — that's a Call site, not a bare
      Attribute, so it's a different AST node with its own source segment
      and does NOT count against this allowlist entry's expected_count=1).

  - file: web/export_state.py
    patterns:
      - source: "app.storage.user"
        expected_count: 1
        enclosing: "_backend (production fallthrough for _TEST_BACKEND shim)"
    justification: |
      Line 48 (`return app.storage.user`) is the production fallthrough inside
      `_backend()`, which exists ONLY to support the `_TEST_BACKEND` test
      injection shim. The AST scanner records the source segment of the bare
      `Attribute` access — which is just `app.storage.user`, NOT the full
      return statement. The pattern must be a substring of the recorded
      segment, so we match on `app.storage.user` and scope it via
      `enclosing: _backend` to prevent any future bare `app.storage.user`
      access elsewhere in this file from being silently legalized.
      Phase 88 STATE-04 explicitly deletes `_TEST_BACKEND` and replaces it
      with proper fixture injection, making this allowlist entry
      self-eliminating. Migrating to safe_user_get/set/pop here would not
      match the function's contract (it returns the dict-like backend object
      itself, not a value-for-key). expected_count=1 + enclosing=_backend
      enforces strict scope.
```

After writing, validate the YAML structurally (Windows-safe Python one-liner):
```
python -c "import yaml, pathlib; data = yaml.safe_load(pathlib.Path('.planning/phase87_storage_allowlist.yaml').read_text(encoding='utf-8')); assert len(data['allowed_raw_access']) == 4; files = {e['file'] for e in data['allowed_raw_access']}; assert files == {'web/auth_state.py', 'web/main.py', 'web/supabase_client.py', 'web/export_state.py'}, files; [print(f\"{e['file']}: {len(e['patterns'])} patterns, each with expected_count\") for e in data['allowed_raw_access']]; assert all(isinstance(p, dict) and 'source' in p and 'expected_count' in p for e in data['allowed_raw_access'] for p in e['patterns']); print('OK')"
```

Expected last line: `OK`.
  </action>
  <verify>
    <automated>python -c "import yaml, pathlib; data = yaml.safe_load(pathlib.Path('.planning/phase87_storage_allowlist.yaml').read_text(encoding='utf-8')); assert len(data['allowed_raw_access']) == 4; assert all(isinstance(p, dict) and 'source' in p and 'expected_count' in p for e in data['allowed_raw_access'] for p in e['patterns']); print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File `.planning/phase87_storage_allowlist.yaml` exists
    - YAML parses cleanly: `python -c "import yaml, pathlib; yaml.safe_load(pathlib.Path('.planning/phase87_storage_allowlist.yaml').read_text(encoding='utf-8'))"` exits 0
    - YAML contains exactly 4 entries under `allowed_raw_access` key (verified via Python)
    - Files allowed are the exact set {`web/auth_state.py`, `web/main.py`, `web/supabase_client.py`, `web/export_state.py`}
    - Every entry has a non-empty `justification` field (multi-line text)
    - Every pattern is an object (dict) with `source` (str) and `expected_count` (int) keys — verified by the Python one-liner in `<action>`
    - At least one entry's justification mentions `Phase 91` (auth migration; verify with `python -c "import pathlib; print('Phase 91' in pathlib.Path('.planning/phase87_storage_allowlist.yaml').read_text(encoding='utf-8'))"` prints `True`)
    - At least one entry's justification mentions `Phase 90` (auth client deletion)
    - At least one entry's justification mentions `Phase 88` (export_state cleanup)
  </acceptance_criteria>
  <done>Allowlist YAML exists, parses cleanly, has 4 entries with expected_count on every pattern and justifications referencing the correct downstream phases.</done>
</task>

<task type="auto">
  <name>Task 3: Create tests/test_no_raw_storage_access.py with corrected AST scanner (B2 fix) + 5 tests including the H1 count test</name>
  <read_first>
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md (read "Code Examples - Lint Implementation - AST-based pytest test" at lines 396-516 for the STRUCTURE, but DO NOT copy the chain-order bug — see B2 fix in this plan)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md ("tests/test_no_raw_storage_access.py (new AST-lint test)" section)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (B2 chain order + parent tracking; H1 allowlist count enforcement)
    - .planning/phase87_storage_allowlist.yaml (CREATED IN TASK 2 — must exist before writing this test)
    - tests/test_safe_storage.py (style reference for test file header)
    - The `<ast_chain_facts>` block in this plan's `<context>` section — it documents the EXACT chain shape produced by the corrected walker
  </read_first>
  <files>tests/test_no_raw_storage_access.py</files>
  <action>
Create `tests/test_no_raw_storage_access.py` (NEW file). This is the lint scanner with the B2-fixed AST chain handling and the H1 allowlist count enforcement.

**CRITICAL implementation detail (B2 fix — the chain order is INNER-FIRST, not outer-first).** When walking from a starting Attribute node `cur` inward with the loop:

```python
chain = []
cur = starting_attribute
while isinstance(cur, ast.Attribute):
    chain.append(cur.attr)
    cur = cur.value
```

For `app.storage.user.get('foo')` (a `Call`) the starting node is `node.func` which is `Attribute(attr='get')`. After we extract `.value` to skip the `.get`, we get `Attribute(attr='user')`. Walking from there:
- Step 1: append `'user'`, descend to `Attribute(attr='storage')`. chain = `['user']`.
- Step 2: append `'storage'`, descend to `Name('app')`. chain = `['user', 'storage']`. Loop exits.

So the CORRECT check after the walk is: `chain[-2:] == ['user', 'storage']` AND the final `cur` is an `ast.Name` whose `id` is in `app_aliases`.

For `app.storage.user[...]` (a `Subscript`) the starting node is `node.value` which is `Attribute(attr='user')`. Walking:
- Step 1: append `'user'`, descend to `Attribute(attr='storage')`. chain = `['user']`.
- Step 2: append `'storage'`, descend to `Name('app')`. chain = `['user', 'storage']`. Loop exits.

Same correct check.

For `app.storage.user.get` invoked via `Call`, we skip the trailing `.get` BEFORE entering the walk loop by setting `cur = node.func.value` (one step inward from the func attribute). This way the chain doesn't include `'get'` and we can apply the same `chain[-2:] == ['user', 'storage']` check uniformly.

Codex's review wrote `chain[-2:] == ['storage', 'user']` — that order would only be correct if you appended to the FRONT instead of the back of the list, or if you reversed at the end. Both are valid implementations; the implementation below picks the simpler "inner-first via append" form and checks `['user', 'storage']`.

**PARENT TRACKING (B2 second half — avoid double-reporting):** Use an `ast.NodeVisitor` subclass that:
1. Visits `Call` and `Subscript` nodes OUTERMOST first.
2. For each Call/Subscript that matches a storage-user access, records the violation AND marks the inner Attribute chain as "already reported" so a subsequent visit of the inner `Attribute` skips it.
3. Visits bare `Attribute` only if it is NOT already reported AND its surface form is itself a top-level access (e.g., `app.storage.user = x` as an assign target, or `x = app.storage.user` as a bare read).

Implementation uses a `_seen_attribute_nodes` set keyed by `id(node)` to track which Attribute subtrees were already consumed by a Call or Subscript visit.

Exact file content to write:

```python
"""Lint test: reject raw app.storage.user access outside the Phase 87 allowlist.

Reads .planning/phase87_storage_allowlist.yaml and scans every .py file under
web/ for AST nodes matching:
  - <app_alias>.storage.user.get(...)
  - <app_alias>.storage.user.pop(...)
  - <app_alias>.storage.user[...]  (Subscript both read and assign)
  - <app_alias>.storage.user (bare attribute access, e.g., `storage = app.storage.user`)

Where <app_alias> is any name bound to `from nicegui import app[ as ALIAS]`.
The three known aliases in this codebase are `app`, `nicegui_app`, `_app`.

Phase 87 FOUND-04 success criterion: this scan returns ZERO violations
outside the allowlist after Plans 02-06 land.

Revisions per 87-REVIEWS.md:
  - B2: corrected AST chain check (inner-first order ['user', 'storage']) and
    added parent tracking via NodeVisitor to avoid double-reporting nested
    nodes. The previous chain[-2:] == ['storage', 'user'] check did not match
    actual ast.walk output and would have caused the synthetic violation test
    to pass falsely.
  - H1: new schema with `source` + `expected_count` per pattern; added
    test_allowlist_counts_exact to enforce exact match counts.
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
    `from nicegui import app as _app`. Also handles inline (function-local)
    imports because ast.walk visits all ImportFrom nodes regardless of scope.
    """
    aliases = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'nicegui':
            for alias in node.names:
                if alias.name == 'app':
                    aliases.add(alias.asname or 'app')
    return aliases


def _walk_attribute_chain(start: ast.AST, app_aliases: set):
    """Walk an Attribute chain inward from `start`, returning (chain, root).

    chain is inner-first: for app.storage.user as the start, chain == ['user', 'storage']
    after the walk, and root is the ast.Name('app').

    Returns (chain, root_name_str) or (None, None) if the chain does not end in
    a Name in app_aliases.

    Per B2 in 87-REVIEWS.md: the previous implementation appended in the same
    order but checked chain[-2:] == ['storage', 'user'], which would never match
    because the actual order is ['user', 'storage']. This implementation makes
    the order explicit and checks chain[-2:] == ['user', 'storage'].
    """
    chain = []
    cur = start
    while isinstance(cur, ast.Attribute):
        chain.append(cur.attr)
        cur = cur.value
    if not isinstance(cur, ast.Name):
        return None, None
    if cur.id not in app_aliases:
        return None, None
    return chain, cur.id


def _matches_storage_user_access(chain) -> bool:
    """Given an inner-first chain from _walk_attribute_chain, return True iff
    it ends in `.storage.user` (i.e., the access targets app.storage.user[...] or
    app.storage.user.X)."""
    return len(chain) >= 2 and chain[-2:] == ['user', 'storage']


class _StorageAccessVisitor(ast.NodeVisitor):
    """Visit Call/Subscript/Attribute/Assign-target nodes and collect storage-user accesses.

    Uses parent tracking via a _seen set keyed by id(inner_attribute_node) so
    the inner Attribute that a Call.func or Subscript.value already consumed
    is not reported a second time when ast.walk would otherwise visit it.
    """

    def __init__(self, app_aliases: set, source: str):
        self.app_aliases = app_aliases
        self.source = source
        self.violations: list[tuple[int, str]] = []  # (lineno, source_segment)
        self._seen_inner_ids: set[int] = set()

    def _record(self, node):
        seg = ast.get_source_segment(self.source, node) or ''
        self.violations.append((node.lineno, seg))

    def visit_Call(self, node: ast.Call):
        # app.storage.user.get(...) / .pop(...) / etc.
        if isinstance(node.func, ast.Attribute):
            # The call's func is an Attribute like `<expr>.get`. To check whether
            # `<expr>` is app.storage.user, we start the walk from node.func.value.
            chain, root = _walk_attribute_chain(node.func.value, self.app_aliases)
            if chain is not None and _matches_storage_user_access(chain):
                self._record(node)
                # Mark the entire Attribute subtree under node.func as seen.
                for sub in ast.walk(node.func):
                    if isinstance(sub, ast.Attribute):
                        self._seen_inner_ids.add(id(sub))
        # Continue walking into arguments (they might contain more accesses).
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript):
        # app.storage.user[KEY] (read or assign-target)
        if isinstance(node.value, ast.Attribute):
            chain, root = _walk_attribute_chain(node.value, self.app_aliases)
            if chain is not None and _matches_storage_user_access(chain):
                self._record(node)
                for sub in ast.walk(node.value):
                    if isinstance(sub, ast.Attribute):
                        self._seen_inner_ids.add(id(sub))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute):
        # Catches bare `app.storage.user` access (e.g., `storage = app.storage.user`)
        # that did NOT appear as a Call.func or Subscript.value (those are caught
        # above and would mark this node's id as seen).
        if id(node) in self._seen_inner_ids:
            return  # Already reported as part of a parent Call/Subscript
        chain, root = _walk_attribute_chain(node, self.app_aliases)
        if chain is not None and _matches_storage_user_access(chain):
            # Only report if this Attribute is the OUTERMOST in its chain — i.e.,
            # its own parent in the AST is not another Attribute that we would
            # also visit. Since ast.NodeVisitor doesn't natively track parents,
            # we approximate: a bare `app.storage.user` reaches here only if it
            # was not part of a Call/Subscript we already consumed. Additional
            # nested Attribute would have its OUTER attr appended FIRST, so the
            # walk result for the inner-most matching node may not be 'user'/'storage'
            # at chain[-2:]. The _matches_storage_user_access guard handles that.
            self._record(node)
        self.generic_visit(node)


def _scan_file(path: Path, source: str) -> list[tuple[int, str]]:
    """Return list of (lineno, source_segment) violations for one .py file."""
    tree = ast.parse(source, filename=str(path))
    aliases = _find_app_aliases(tree)
    if not aliases:
        return []
    visitor = _StorageAccessVisitor(aliases, source)
    visitor.visit(tree)
    return visitor.violations


def _is_allowlisted(rel_path: str, source_segment: str, allowed_map: dict) -> bool:
    """Return True if the (rel_path, source_segment) tuple matches an allowlist entry.

    allowed_map: {rel_path -> entry_dict}. Each entry has `patterns` which is
    a list of {source: str, expected_count: int, enclosing?: str} dicts.
    Substring match on `source` is sufficient to legalize a violation; the
    expected_count is enforced separately by test_allowlist_counts_exact.
    """
    entry = allowed_map.get(rel_path)
    if not entry:
        return False
    for pat in entry.get('patterns', []):
        if isinstance(pat, dict):
            source_pat = pat.get('source', '')
        else:
            source_pat = pat  # Legacy schema fallback (string)
        if source_pat and source_pat in source_segment:
            return True
    return False


# ===========================================================================
# Tests
# ===========================================================================

def test_allowlist_well_formed():
    """FOUND-03 schema check: every allowlist entry has file + patterns + justification.

    Per H1, each pattern must be a dict with `source` and `expected_count`.
    """
    allowlist = _load_allowlist()
    entries = allowlist.get('allowed_raw_access', [])
    assert entries, "Allowlist is empty — at minimum web/auth_state.py should be allowlisted"
    for entry in entries:
        assert 'file' in entry, f"Entry missing 'file': {entry}"
        assert 'patterns' in entry, f"Entry {entry['file']} missing 'patterns'"
        assert entry['patterns'], f"Entry {entry['file']} has empty patterns list"
        assert 'justification' in entry, f"Entry {entry['file']} missing 'justification'"
        assert entry['justification'].strip(), f"Entry {entry['file']} has empty justification"
        for pat in entry['patterns']:
            assert isinstance(pat, dict), (
                f"Entry {entry['file']}: pattern {pat!r} must be a dict with "
                f"'source' and 'expected_count' keys (H1 schema)"
            )
            assert 'source' in pat and isinstance(pat['source'], str) and pat['source'].strip(), (
                f"Entry {entry['file']}: pattern missing/empty 'source': {pat}"
            )
            assert 'expected_count' in pat and isinstance(pat['expected_count'], int) and pat['expected_count'] >= 1, (
                f"Entry {entry['file']}: pattern '{pat.get('source')}' missing/invalid 'expected_count': {pat}"
            )


def test_lint_rejects_synthetic_violation():
    """FOUND-04 SC4: verify the lint visitor detects a synthetic raw access (with corrected chain semantics)."""
    synthetic = textwrap.dedent("""\
        from nicegui import app
        def bad():
            return app.storage.user.get('foo')
    """)
    tree = ast.parse(synthetic)
    aliases = _find_app_aliases(tree)
    assert aliases == {'app'}, f"Expected alias 'app', got {aliases}"
    visitor = _StorageAccessVisitor(aliases, synthetic)
    visitor.visit(tree)
    assert visitor.violations, "Lint visitor failed to detect synthetic raw access (B2 chain bug regression?)"


def test_lint_handles_aliased_imports():
    """FOUND-04: verify alias resolution catches `nicegui_app` and `_app` aliases."""
    for alias_form, expected_alias in [
        ("from nicegui import app as nicegui_app\ndef bad():\n    return nicegui_app.storage.user.get('x')\n", 'nicegui_app'),
        ("from nicegui import app as _app\ndef bad():\n    return _app.storage.user.get('x')\n", '_app'),
    ]:
        tree = ast.parse(alias_form)
        aliases = _find_app_aliases(tree)
        assert aliases == {expected_alias}, f"Expected {{{expected_alias!r}}}, got {aliases} for {alias_form!r}"
        visitor = _StorageAccessVisitor(aliases, alias_form)
        visitor.visit(tree)
        assert visitor.violations, f"Alias resolution failed for {alias_form!r}"


def test_lint_does_not_double_report_nested_nodes():
    """B2 second-half regression guard: walking Call/Subscript/Attribute does not
    cause the inner Attribute to be reported a second time."""
    src = textwrap.dedent("""\
        from nicegui import app
        x = app.storage.user.get('a')
        y = app.storage.user['b']
        app.storage.user['c'] = 1
        z = app.storage.user
    """)
    tree = ast.parse(src)
    aliases = _find_app_aliases(tree)
    visitor = _StorageAccessVisitor(aliases, src)
    visitor.visit(tree)
    # 4 statements, each producing exactly 1 violation.
    # Without parent tracking, the inner Attribute(value=Attribute(...)) of the
    # Call and the two Subscripts would also be visited as bare Attributes,
    # producing 3 extra reports. The B2 parent-tracking fix prevents that.
    line_numbers = sorted({v[0] for v in visitor.violations})
    assert len(line_numbers) == 4, (
        f"Expected 4 unique violation lines, got {len(line_numbers)}: {visitor.violations}"
    )
    assert len(visitor.violations) == 4, (
        f"Expected exactly 4 violations (no double-reporting), got {len(visitor.violations)}:\n"
        + "\n".join(f"  line {ln}: {seg}" for ln, seg in visitor.violations)
    )


def test_allowlist_counts_exact():
    """H1: each allowlist pattern matches AST nodes EXACTLY its expected_count.

    Prevents the failure mode where a substring pattern like `_app.storage.user`
    silently legalizes a NEW raw access added later in the same file. Counts
    are evaluated against the post-migration codebase: in Wave 0 (before Plans
    03-06 land), this test will fail for files where migrations haven't yet
    happened. By Plan 07, all migrations are done and counts must match.

    This test is GREEN-after-Plan-07. During Wave 0 it is expected to be RED
    (alongside test_no_raw_storage_access_outside_allowlist) and that failure
    is part of the Wave 0 evidence.
    """
    allowlist = _load_allowlist()
    entries = allowlist.get('allowed_raw_access', [])
    mismatches = []
    for entry in entries:
        rel = entry['file']
        path = REPO_ROOT / rel
        if not path.exists():
            mismatches.append(f"{rel}: file does not exist on disk")
            continue
        source = path.read_text(encoding='utf-8')
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as e:
            mismatches.append(f"{rel}: AST parse failed: {e}")
            continue
        aliases = _find_app_aliases(tree)
        if not aliases:
            # No nicegui app import — actual count is 0 by definition. If the
            # allowlist still lists patterns with expected_count > 0 for this file,
            # the test must fail loudly so stale allowlist entries are caught.
            # (Fix 3 in 87-REVIEWS.md iteration 3 — Codex MEDIUM finding.)
            for pat in entry['patterns']:
                if pat['expected_count'] > 0:
                    mismatches.append(
                        f"{rel}: pattern {pat['source']!r} expected_count={pat['expected_count']} "
                        f"but file has no nicegui app import (actual count = 0). "
                        f"Either remove this allowlist entry or restore the import."
                    )
            continue
        visitor = _StorageAccessVisitor(aliases, source)
        visitor.visit(tree)
        # For each pattern, count how many violation source segments contain it.
        for pat in entry['patterns']:
            source_pat = pat['source']
            expected = pat['expected_count']
            actual = sum(1 for (_ln, seg) in visitor.violations if source_pat in seg)
            if actual != expected:
                mismatches.append(
                    f"{rel}: pattern {source_pat!r} expected_count={expected} "
                    f"but found {actual} matching AST nodes"
                )
    if mismatches:
        msg = (
            "Allowlist count mismatches (H1 enforcement):\n  "
            + "\n  ".join(mismatches)
            + "\n\nFix: either adjust expected_count in the allowlist YAML (if the new count is justified), "
              "or migrate the extra raw access site(s) to web.safe_storage helpers."
        )
        pytest.fail(msg)


def test_no_raw_storage_access_outside_allowlist():
    """FOUND-04 SC4: production code under web/ has no raw access outside allowlist.

    THIS TEST FAILS DURING WAVE 0 — migrations land in Plans 03-06.
    It must be GREEN by end of Plan 07 (Lint Finalization).
    """
    allowlist = _load_allowlist()
    allowed_map = {entry['file']: entry for entry in allowlist.get('allowed_raw_access', [])}
    violations = []
    for path in WEB_DIR.rglob('*.py'):
        if path.name == 'safe_storage.py':
            continue  # The chokepoint itself
        rel = path.relative_to(REPO_ROOT).as_posix()
        source = path.read_text(encoding='utf-8')
        try:
            file_violations = _scan_file(path, source)
        except SyntaxError as e:
            pytest.fail(f"AST parse failed for {rel}: {e}")
        for lineno, seg in file_violations:
            if _is_allowlisted(rel, seg, allowed_map):
                continue
            violations.append(f"{rel}:{lineno}: {seg or '<no segment>'}")
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

After writing, verify the scanner correctness with the standalone tests (the 4 non-production tests should PASS even before Plans 03-06):

```
python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -v
```

Expected: 4 passed. The 2 production-scanning tests (`test_no_raw_storage_access_outside_allowlist` and `test_allowlist_counts_exact`) WILL FAIL at Wave 0 because Plans 03-06 haven't migrated yet — that failure is the expected gate signal.
  </action>
  <verify>
    <automated>python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x</automated>
  </verify>
  <acceptance_criteria>
    - File `tests/test_no_raw_storage_access.py` exists
    - File contains exactly 6 test functions: `python -c "import ast; names = [n.name for n in ast.walk(ast.parse(open('tests/test_no_raw_storage_access.py').read())) if isinstance(n, ast.FunctionDef) and n.name.startswith('test_')]; assert sorted(names) == sorted(['test_allowlist_well_formed', 'test_lint_rejects_synthetic_violation', 'test_lint_handles_aliased_imports', 'test_lint_does_not_double_report_nested_nodes', 'test_allowlist_counts_exact', 'test_no_raw_storage_access_outside_allowlist']), names; print('OK')"` prints `OK`
    - File contains `class _StorageAccessVisitor(ast.NodeVisitor)`: verified via Python AST scan
    - File uses `chain[-2:] == ['user', 'storage']` (the B2 corrected check) AND does NOT contain `chain[-2:] == ['storage', 'user']` (the original buggy check)
    - File imports yaml: verified via `python -c "import ast; tree = ast.parse(open('tests/test_no_raw_storage_access.py').read()); assert any(isinstance(n, ast.Import) and any(a.name == 'yaml' for a in n.names) for n in ast.walk(tree)); print('OK')"` prints `OK`
    - Running `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x` exits 0 (PASSES)
    - Running `pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation -x` exits 0 (PASSES — proves B2 fix works against the synthetic input)
    - Running `pytest tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports -x` exits 0 (PASSES — alias resolution works for `nicegui_app` and `_app`)
    - Running `pytest tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x` exits 0 (PASSES — parent tracking works)
    - Running `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist -x` exits non-zero (FAILS — production code still has raw access; this is expected at Wave 0 and gates Plans 03-06 to complete)
    - Running `pytest tests/test_no_raw_storage_access.py::test_allowlist_counts_exact -x` exits non-zero (FAILS — counts won't match until Plan 07 because main.py/supabase_client.py still have non-allowlisted raw accesses pre-Plan-04)
    - **Fix 3 regression path:** `test_allowlist_counts_exact` correctly fails when an allowlisted file's nicegui app import is removed but expected_count > 0 (the silent-skip replacement explicitly counts 0 and reports a mismatch — verify by reading the test body: it must contain `if not aliases:` followed by a loop over `entry['patterns']` that appends to mismatches when `expected_count > 0`)
    - tests/test_safe_storage.py file byte-unchanged from baseline (FOUND-05 invariant): SHA-256 stable across the Wave 0 commit
  </acceptance_criteria>
  <done>Lint scanner file exists with corrected AST chain logic + parent tracking + H1 count test; 4 of 6 tests pass; the 2 production-scanning tests fail as expected.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Test runner -> web/ source files | The lint test reads .py files from disk; does NOT import them. Source files are first-party and trusted; no untrusted input crosses this boundary. |
| YAML allowlist -> test runner | YAML parsed via `yaml.safe_load` (rejects arbitrary code execution). File is git-tracked and code-reviewed. |
| Allowlist patterns -> production runtime | NONE — the allowlist file is only read by tests, never by production code. A poisoned allowlist would fail in CI before reaching prod. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-04 | Tampering | Allowlist pattern matching | mitigate | Use substring match against `ast.get_source_segment()` output (not line numbers). Line numbers drift on refactor; source-segment patterns survive. H1 expected_count adds a second mitigation: even if a future contributor adds a NEW raw access that substring-matches an existing pattern, the count test will fail. |
| T-87-05 | Information disclosure | Alias resolution in lint scanner | mitigate | `_find_app_aliases()` walks `ast.ImportFrom` nodes; resolves `app`, `nicegui_app`, `_app`. Verified by `test_lint_handles_aliased_imports`. |
| T-87-04b | Tampering | YAML allowlist file | mitigate | `yaml.safe_load` (not `yaml.load`) — rejects Python object instantiation. Schema validated by `test_allowlist_well_formed` (every entry must have file + patterns + justification + expected_count). |
| T-87-04c | Tampering | AST scanner correctness regression (B2) | mitigate | `test_lint_rejects_synthetic_violation` + `test_lint_does_not_double_report_nested_nodes` guard against future regressions of the chain-order fix and parent-tracking fix respectively. |
| -- | Spoofing | -- | accept | No spoofing surface — internal test infrastructure only |
| -- | Repudiation | -- | accept | No audit-log requirement for lint scanner |
| -- | DoS | AST parse of 16 files | accept | ~500ms scan; negligible at this scale (verified by R-03 timing) |
| -- | Elevation | -- | accept | Test runs in CI with project credentials; no escalation possible |
</threat_model>

<verification>
After all 3 tasks (all commands Windows-safe, no `/tmp`, `grep`, or `tail` reliance):

```
# Verify PyYAML available (L2)
python -c "import yaml; print('PyYAML', yaml.__version__)"

# Verify Wave 0 files exist
python -c "import pathlib; [print(p, p.exists()) for p in [pathlib.Path('tests/test_session_uuid.py'), pathlib.Path('tests/test_no_raw_storage_access.py'), pathlib.Path('.planning/phase87_storage_allowlist.yaml')]]"

# Verify YAML parses with H1 schema
python -c "import yaml, pathlib; data = yaml.safe_load(pathlib.Path('.planning/phase87_storage_allowlist.yaml').read_text(encoding='utf-8')); print('entries:', len(data['allowed_raw_access'])); print('all patterns are dicts with expected_count:', all(isinstance(p, dict) and 'expected_count' in p for e in data['allowed_raw_access'] for p in e['patterns']))"

# Verify test collection
python -m pytest tests/test_session_uuid.py tests/test_no_raw_storage_access.py --collect-only -q

# Verify expected Wave 0 failures
python -m pytest tests/test_session_uuid.py 2>&1 | python -c "import sys; out = sys.stdin.read(); print('ImportError on get_session_uuid:', 'get_session_uuid' in out and ('ImportError' in out or 'cannot import' in out))"

# Verify the 4 standalone tests pass
python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x

# Verify FOUND-05 invariant
python -c "import hashlib, pathlib; h = hashlib.sha256(pathlib.Path('tests/test_safe_storage.py').read_bytes()).hexdigest(); print('test_safe_storage.py SHA-256:', h)"
```
</verification>

<success_criteria>
1. PyYAML 6.0.3+ confirmed importable (L2 gate)
2. `tests/test_session_uuid.py` exists with 10 tests (5 original + 4 M5 additions + 1 B1-residual route-coverage guard); collect-only succeeds; runtime fails with ImportError on the first 9 tests (expected pre-Plan-02); the 10th test (test_every_ui_page_handler_mints_uuid) may already pass at Wave 0 because it only reads web/main.py source — it gates Plan 02 from regressing the bootstrap wiring
3. `.planning/phase87_storage_allowlist.yaml` exists with 4 well-formed entries; each pattern has `source` + `expected_count` (H1 schema)
4. `tests/test_no_raw_storage_access.py` exists with 6 tests using the B2-corrected AST chain order (`chain[-2:] == ['user', 'storage']`); 4 standalone tests pass (well_formed, synthetic, aliased, no_double_report); 2 production-scanning tests fail as expected at Wave 0
5. `tests/test_safe_storage.py` is byte-identical to baseline (FOUND-05 invariant)
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-01-SUMMARY.md` summarizing:
- Files created (paths + line counts)
- Test counts (passing vs expected-to-fail per category)
- B2 verification: confirm the chain-order check is `['user', 'storage']` and the synthetic-violation test passes
- H1 verification: confirm every allowlist pattern has `expected_count` and `test_allowlist_counts_exact` is present
- M5 verification: confirm the 4 new UUID validation tests are present
- L2 verification: PyYAML version recorded
- Baseline SHA-256 of `tests/test_safe_storage.py` for end-of-phase comparison
- Allowlist entry summary (4 file paths + brief justification gist for each)
</output>
