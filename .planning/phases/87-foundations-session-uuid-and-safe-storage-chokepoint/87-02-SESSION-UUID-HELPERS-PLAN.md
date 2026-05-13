---
phase: 87
plan: 02
type: execute
wave: 1
depends_on: [87-01]
files_modified:
  - web/safe_storage.py
autonomous: true
requirements:
  - FOUND-01
  - FOUND-02
  - FOUND-05
tags:
  - phase87
  - safe-storage
  - session-uuid
  - nicegui
  - storage-chokepoint
must_haves:
  truths:
    - "get_session_uuid() returns a 32-char hex string; never None"
    - "Two consecutive calls with the same storage dict return the same UUID"
    - "Mutating auth_session keys does not affect _session_uuid (token-refresh stability)"
    - "All 5 tests in tests/test_session_uuid.py pass"
    - "All 6 existing tests in tests/test_safe_storage.py pass without modification"
  artifacts:
    - path: "web/safe_storage.py"
      provides: "Two new functions: get_session_uuid(), ensure_session_uuid(); module constant _SESSION_UUID_KEY"
      contains: "def get_session_uuid"
      min_lines: 130
  key_links:
    - from: "web/safe_storage.py:get_session_uuid"
      to: "app.storage.user['_session_uuid']"
      via: "lazy mint on first call; reuse on subsequent"
      pattern: "_SESSION_UUID_KEY"
    - from: "web/safe_storage.py:get_session_uuid"
      to: "uuid.uuid4().hex"
      via: "CSPRNG-backed UUID4 generation"
      pattern: "_uuid\\.uuid4\\(\\)\\.hex"
---

<objective>
Implement `get_session_uuid()` and `ensure_session_uuid()` in `web/safe_storage.py` such that all 5 tests in `tests/test_session_uuid.py` (created in Plan 01) go GREEN, and all 6 existing tests in `tests/test_safe_storage.py` continue passing UNCHANGED.

Purpose: Provide the foundational session-UUID API that Phases 88-92 will consume as a stable cache key (per HANDOFF_v7.11.1_path_b.md item 6: "Use this as the stable cache key wherever caching survives Path B. Tokens rotate; UUIDs don't.")

Output: Modified `web/safe_storage.py` with 2 new public functions, 1 new module constant, and 1 new import — purely additive; existing 3 helpers untouched.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@web/safe_storage.py
@tests/test_safe_storage.py
@tests/test_session_uuid.py

<interfaces>
<!-- New API contracts. Phase 88+ consumers will call these signatures. -->

Module-level addition (after existing imports):
```python
import uuid as _uuid

_SESSION_UUID_KEY = '_session_uuid'
```

New public functions:
```python
def get_session_uuid() -> str:
    """Return this session's stable UUID, minting one on first call.

    The UUID is generated lazily and stored in app.storage.user['_session_uuid'].
    Survives token refresh because it lives in storage, not in any auth dict.

    Returns a fresh ephemeral UUID4 hex string if storage is unavailable
    (prune race) — callers should treat the returned value as guaranteed
    non-empty 32-char hex but not assume it is the SAME UUID across
    AssertionError windows. Phase 88+ cache lookups against a fallback
    UUID will simply miss, which is the correct behavior (no false-positive
    cache hit).

    Validates retrieved value matches uuid4().hex shape (32-char hex). On
    storage-poisoning (non-string or wrong-length value at the key), mints
    fresh and overwrites. This defends against the T-87-02 storage-poisoning
    threat.

    Never returns None. Never raises.
    """


def ensure_session_uuid() -> bool:
    """Eagerly mint session UUID if not present.

    Returns True if minted or already exists (and is well-formed).
    Returns False only if storage is unavailable (prune race) — caller
    may retry on next request.

    Use this from a top-of-page-handler when downstream code depends on
    the UUID being present in storage (e.g., for sharing the UUID with
    browser JavaScript via add_head_html). For most consumers,
    get_session_uuid() (lazy mint) is sufficient.
    """
```
</interfaces>

<existing_safe_storage_helpers>
<!-- These MUST remain byte-identical. Plan 02 makes ZERO changes to lines 40-79 of safe_storage.py. -->

```python
def safe_user_get(key: str, default: Any = None) -> Any:
    try:
        return app.storage.user.get(key, default)
    except AssertionError as e:
        logger.debug("safe_user_get(%r): session storage unavailable: %s", key, e)
        return default
    except Exception as e:
        logger.warning("safe_user_get(%r) unexpected failure: %s", key, e, exc_info=False)
        return default

def safe_user_set(key: str, value: Any) -> bool:
    try:
        app.storage.user[key] = value
        return True
    except AssertionError as e:
        logger.debug("safe_user_set(%r): session storage unavailable: %s", key, e)
        return False
    except Exception as e:
        logger.warning("safe_user_set(%r) unexpected failure: %s", key, e, exc_info=False)
        return False

def safe_user_pop(key: str, default: Any = None) -> Any:
    try:
        return app.storage.user.pop(key, default)
    except AssertionError as e:
        logger.debug("safe_user_pop(%r): session storage unavailable: %s", key, e)
        return default
    except Exception as e:
        logger.warning("safe_user_pop(%r) unexpected failure: %s", key, e, exc_info=False)
        return default
```
</existing_safe_storage_helpers>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add get_session_uuid + ensure_session_uuid + _SESSION_UUID_KEY to web/safe_storage.py</name>
  <read_first>
    - web/safe_storage.py (CURRENT STATE — read fully so you understand the existing 3 helpers; you must NOT modify them)
    - tests/test_session_uuid.py (the 5 tests — these are your acceptance contract)
    - tests/test_safe_storage.py (FOUND-05 baseline — must remain passing UNCHANGED)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (the "Core pattern for new helpers" code block — copy the try/except shape exactly)
  </read_first>
  <files>web/safe_storage.py</files>
  <behavior>
    - Test 1 (test_session_uuid_unique_across_100_sessions): 100 separate storage dicts produce 100 distinct UUIDs (collision rate must be 0).
    - Test 2 (test_session_uuid_stable_within_session): get_session_uuid() called twice on the same storage dict returns the same value; storage now contains key '_session_uuid'.
    - Test 3 (test_session_uuid_survives_token_refresh): Mutating storage['auth_session'] = {...different...} between calls does NOT change the returned UUID.
    - Test 4 (test_session_uuid_returns_ephemeral_on_prune): When storage.get raises AssertionError, function returns a valid 32-char hex string (NOT None, NOT empty); does NOT call storage[key] = ... in this branch (verified implicitly because storage is a MagicMock with side_effect — no NEW exceptions should bubble).
    - Test 5 (test_ensure_session_uuid_idempotent): ensure_session_uuid() called twice returns True both times; second call does NOT regenerate the UUID (storage['_session_uuid'] unchanged between calls).
    - Additional safety property (NOT separately tested but required by T-87-02): on read, if the stored value is not a 32-char hex string, treat it as missing and regenerate.
  </behavior>
  <action>
Edit `web/safe_storage.py`. The file currently has 80 lines (existing 3 helpers + module docstring + imports + logger). You must:

1. **Add import after line 33** (after the existing `from typing import Any`):
```python
import uuid as _uuid
```
(Use the underscore-prefixed alias `_uuid` to make it clear this is module-internal; matches the pattern in similar Python projects and keeps the public API surface clean.)

2. **Add module constant after line 37** (after `logger = logging.getLogger(__name__)`):
```python


_SESSION_UUID_KEY = '_session_uuid'
```

3. **Append new functions at end of file** (after line 79, the closing of `safe_user_pop`). Append the following two functions verbatim:

```python


def get_session_uuid() -> str:
    """Return this session's stable UUID, minting one on first call.

    The UUID is generated lazily via :func:`uuid.uuid4` (CSPRNG-backed in
    CPython per Python docs `uuid.uuid4`) and stored in
    ``app.storage.user['_session_uuid']``. It survives token refresh
    because it lives in storage, not in any auth dict — Phase 91's
    auth-token rotation will not affect it.

    Returns a fresh ephemeral UUID4 hex string if storage is unavailable
    (prune race) -- callers receive a valid 32-char hex string but the
    same call site within a different prune window may get a DIFFERENT
    UUID. Downstream cache lookups against a fallback UUID will simply
    miss, which is the correct behavior (no false-positive cache hit).

    Validates retrieved value against the UUID4 hex shape (32-char
    lowercase hex). On storage-poisoning (non-string or wrong-length
    value at the key), mints fresh and overwrites. This defends against
    the T-87-02 storage-poisoning threat from the Phase 87 threat model.

    Security note (T-87-03): NEVER log this UUID at INFO+ level, expose
    it in URLs/query strings, or include it in PostHog events without
    HMAC. It is an opaque server-side cache key — treat as session-secret.

    :returns: A 32-character lowercase hex UUID4 string. Never None. Never raises.
    """
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if isinstance(uid, str) and len(uid) == 32:
            # Validate UUID4 hex shape (defends against storage poisoning).
            try:
                int(uid, 16)  # Must parse as hex
                return uid
            except ValueError:
                logger.warning(
                    "get_session_uuid: stored _session_uuid is not valid hex; regenerating"
                )
                # Fall through to mint
        new_uid = _uuid.uuid4().hex
        app.storage.user[_SESSION_UUID_KEY] = new_uid
        return new_uid
    except AssertionError as e:
        logger.debug("get_session_uuid: session storage unavailable: %s", e)
        return _uuid.uuid4().hex  # Ephemeral; do NOT cache anywhere persistent
    except Exception as e:
        logger.warning("get_session_uuid unexpected failure: %s", e, exc_info=False)
        return _uuid.uuid4().hex


def ensure_session_uuid() -> bool:
    """Eagerly mint session UUID if not present. Returns True on success.

    Use this from a top-of-page-handler when downstream code depends on
    the UUID being present in storage before any conditional path runs
    (e.g., for sharing the UUID with browser JavaScript via
    ``add_head_html``). For most consumers, :func:`get_session_uuid`
    (lazy mint) is sufficient.

    :returns: True if UUID is present in storage after the call (minted
              or already existed and well-formed). False only if storage
              raises AssertionError (prune race) -- caller may retry on
              next request.
    """
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if isinstance(uid, str) and len(uid) == 32:
            try:
                int(uid, 16)
                return True  # Already present and well-formed
            except ValueError:
                pass  # Fall through to regenerate
        app.storage.user[_SESSION_UUID_KEY] = _uuid.uuid4().hex
        return True
    except AssertionError as e:
        logger.debug("ensure_session_uuid: session storage unavailable: %s", e)
        return False
    except Exception as e:
        logger.warning("ensure_session_uuid unexpected failure: %s", e, exc_info=False)
        return False
```

**DO NOT MODIFY** lines 40-79 (the existing `safe_user_get`, `safe_user_set`, `safe_user_pop` functions). The tests in `tests/test_safe_storage.py` must continue passing without modification.

**DO NOT MODIFY** the module docstring at the top of the file. (It documents the v7.11.1 hotfix history; that history remains accurate.)

After editing, verify:
- File parses as valid Python: `python -c "import ast; ast.parse(open('web/safe_storage.py').read())"`
- All existing helpers still importable: `python -c "from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop; print('OK')"`
- New helpers importable: `python -c "from web.safe_storage import get_session_uuid, ensure_session_uuid, _SESSION_UUID_KEY; print('OK')"`
- All 5 Plan 01 tests pass: `pytest tests/test_session_uuid.py -x`
- All 6 existing tests still pass: `pytest tests/test_safe_storage.py -x`
  </action>
  <verify>
    <automated>pytest tests/test_safe_storage.py tests/test_session_uuid.py -x</automated>
  </verify>
  <acceptance_criteria>
    - `python -c "import ast; ast.parse(open('web/safe_storage.py').read())"` exits 0 (file is valid Python)
    - `grep -c "def get_session_uuid" web/safe_storage.py` returns exactly 1
    - `grep -c "def ensure_session_uuid" web/safe_storage.py` returns exactly 1
    - `grep -c "_SESSION_UUID_KEY = '_session_uuid'" web/safe_storage.py` returns exactly 1
    - `grep -c "import uuid as _uuid" web/safe_storage.py` returns exactly 1
    - `grep -c "_uuid.uuid4().hex" web/safe_storage.py` returns at least 3 (mint path + 2 ephemeral fallbacks)
    - `grep -c "def safe_user_get" web/safe_storage.py` returns exactly 1 (existing helper unchanged)
    - `grep -c "def safe_user_set" web/safe_storage.py` returns exactly 1 (existing helper unchanged)
    - `grep -c "def safe_user_pop" web/safe_storage.py` returns exactly 1 (existing helper unchanged)
    - `pytest tests/test_safe_storage.py -x` exits 0 with 6 tests PASSING (FOUND-05 invariant)
    - `pytest tests/test_session_uuid.py -x` exits 0 with 5 tests PASSING (FOUND-01)
    - `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x` exits 0 (Plan 01 tests still pass)
    - File `tests/test_safe_storage.py` byte-unchanged from baseline: `git diff --stat tests/test_safe_storage.py` returns empty
    - File `tests/test_session_uuid.py` byte-unchanged from Plan 01: `git diff --stat tests/test_session_uuid.py` returns empty (the test file is the contract, not modifiable)
  </acceptance_criteria>
  <done>web/safe_storage.py extended additively; 11 tests total pass (6 existing + 5 new); existing helpers and existing test file untouched.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Browser cookie → NiceGUI SessionMiddleware | Existing; not affected by this plan |
| NiceGUI session storage (app.storage.user) → safe_storage helpers | The chokepoint. After this plan, get_session_uuid / ensure_session_uuid are the canonical session-UUID accessors. |
| safe_storage helpers → callers | Helpers MUST always return valid strings (or False for ensure_*); never raise to caller. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-01 | Information disclosure / Spoofing | UUID predictability for cache-key collision | mitigate | Use `uuid.uuid4().hex` (CSPRNG-backed in CPython; 122 bits of entropy). Verified by `test_session_uuid_unique_across_100_sessions` — 0 collisions across 100 simulated sessions. NEVER use `uuid.uuid1()` (MAC-leaking) or `random.*` (predictable). Encoded in the implementation: `_uuid.uuid4().hex` is the only UUID source. |
| T-87-02 | Tampering | Storage poisoning — malicious user mutates own session storage to inject non-UUID `_session_uuid` value | mitigate | Pattern validation on read: `isinstance(uid, str) and len(uid) == 32` AND `int(uid, 16)` (must parse as hex). On validation failure, mint fresh + overwrite. Encoded in both `get_session_uuid` and `ensure_session_uuid`. |
| T-87-03 | Information disclosure | UUID leakage via logs/URLs/PostHog | mitigate | Documented in function docstring (Security note). Implementation logs only at `debug` level for storage-unavailable case (which uses an ephemeral UUID anyway) and `warning` for unexpected failures (which doesn't include the UUID value). No URL/query/PostHog exposure introduced by this plan. |
| T-87-04 | Tampering | Allowlist for raw access — N/A this plan; allowlist contents not changed | accept | This plan touches only web/safe_storage.py; allowlist scope unchanged |
| T-87-05 | Information disclosure | Alias resolution — N/A this plan | accept | No lint scanner work in this plan |

Block on: T-87-01 (HIGH) — mitigation verified by Test 1 (100-session uniqueness). T-87-02 (MEDIUM) — mitigation encoded in pattern validation branch.
</threat_model>

<verification>
After Task 1:

```bash
# Verify Wave 0 → Wave 1 transition
pytest tests/test_safe_storage.py tests/test_session_uuid.py -x -v
# Expected: 11 passed in <2 seconds (6 + 5)

# Verify FOUND-05 file invariant
git diff --stat tests/test_safe_storage.py
# Expected: (empty — no diff)

# Verify Plan 01 lint scanner still partially-passing
pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation -x
# Expected: 2 passed

# Verify no regressions
ruff check web/safe_storage.py
# Expected: All checks passed!

# Verify the additive nature
python -c "
import ast
tree = ast.parse(open('web/safe_storage.py').read())
funcs = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
print('Functions:', funcs)
assert 'safe_user_get' in funcs
assert 'safe_user_set' in funcs
assert 'safe_user_pop' in funcs
assert 'get_session_uuid' in funcs
assert 'ensure_session_uuid' in funcs
assert len(funcs) == 5
print('OK — 5 functions, all expected')
"
```
</verification>

<success_criteria>
1. `web/safe_storage.py` has 5 functions: `safe_user_get`, `safe_user_set`, `safe_user_pop`, `get_session_uuid`, `ensure_session_uuid`
2. Module constant `_SESSION_UUID_KEY = '_session_uuid'` defined
3. `pytest tests/test_safe_storage.py -x` → 6 passed (FOUND-05)
4. `pytest tests/test_session_uuid.py -x` → 5 passed (FOUND-01)
5. `tests/test_safe_storage.py` byte-identical to baseline
6. Existing 3 helper signatures and bodies UNCHANGED (verified by grep — function definitions still match)
7. Threat T-87-01 mitigated: UUID generated via `uuid.uuid4().hex` (CSPRNG); zero collisions across 100 simulated sessions
8. Threat T-87-02 mitigated: pattern validation on read; storage-poisoning triggers regeneration
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-02-SUMMARY.md` summarizing:
- Functions added to web/safe_storage.py (with signatures)
- Test results: 6/6 existing tests pass + 5/5 new tests pass = 11/11 total
- T-87-01 verification: 100-session uniqueness confirmed
- T-87-02 verification: pattern validation branch covered
- Note: `tests/test_safe_storage.py` is byte-identical to baseline (sha256 confirmation)
</output>
