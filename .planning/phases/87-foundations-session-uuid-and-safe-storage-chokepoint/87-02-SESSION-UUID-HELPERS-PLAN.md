---
phase: 87
plan: 02
type: execute
wave: 1
depends_on: [87-01]
files_modified:
  - web/safe_storage.py
  - web/main.py
  - tests/test_session_uuid.py
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
  - bootstrap-wiring
must_haves:
  truths:
    - "get_session_uuid() returns a 32-char lowercase hex string matching ^[0-9a-f]{32}$; never None"
    - "Two consecutive calls with the same storage dict return the same UUID"
    - "Mutating auth_session keys does not affect _session_uuid (token-refresh stability)"
    - "Uppercase, non-string, malformed, and AssertionError-during-write cases all handled per M5"
    - "ensure_session_uuid() is wired into web/main.py:create_layout() (covers 16 of 19 @ui.page handlers); PLUS directly wired into /reset-hints (storage pop access) and /auth/callback (OAuth atomic writes) per Fix 1 in 87-REVIEWS.md iteration 3 (Codex B1-residual); /privacy-extension is intentionally skipped (pure static info page, zero storage access) and the route-coverage test in Plan 01 documents this exemption"
    - "An automated test exercises the bootstrap path (test_create_layout_mints_session_uuid in tests/test_session_uuid.py)"
    - "A route-coverage test in tests/test_session_uuid.py (test_every_ui_page_handler_mints_uuid, created by Plan 01) enforces that every @ui.page handler in web/main.py either calls create_layout() or ensure_session_uuid() — exempts only /privacy-extension"
    - "All 11 tests in tests/test_session_uuid.py pass (9 original + 1 bootstrap + 1 route-coverage from Plan 01)"
    - "All 6 existing tests in tests/test_safe_storage.py pass without modification (FOUND-05)"
  artifacts:
    - path: "web/safe_storage.py"
      provides: "Two new functions: get_session_uuid(), ensure_session_uuid(); module constant _SESSION_UUID_KEY; strict regex validation"
      contains: "def get_session_uuid"
      min_lines: 130
    - path: "web/main.py"
      provides: "create_layout() bootstrap call to ensure_session_uuid() (B1) — covers 16 of 19 @ui.page handlers; PLUS direct ensure_session_uuid() calls at the top of reset_hints_route and auth_callback_route (Fix 1 in 87-REVIEWS.md iteration 3 — Codex B1-residual). Total coverage: 18 of 19 routes; the remaining 1 route (/privacy-extension) is intentionally skipped (zero storage access). Import line `from web.safe_storage import ensure_session_uuid` is added to the module."
      contains: "ensure_session_uuid"
    - path: "tests/test_session_uuid.py"
      provides: "New bootstrap test test_create_layout_mints_session_uuid added by this plan (B1 automated coverage). Plan 01 separately adds test_every_ui_page_handler_mints_uuid for the route-coverage regression guard (Fix 1)."
      contains: "test_create_layout_mints_session_uuid"
  key_links:
    - from: "web/main.py:create_layout"
      to: "web.safe_storage.ensure_session_uuid"
      via: "first-line call before any other layout logic"
      pattern: "ensure_session_uuid\\(\\)"
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
Implement `get_session_uuid()` and `ensure_session_uuid()` in `web/safe_storage.py` AND wire `ensure_session_uuid()` into the page bootstrap path in `web/main.py:create_layout()` so that the first page render of any session mints a UUID automatically (FOUND-01 SC1). All 9 tests in `tests/test_session_uuid.py` (created in Plan 01) plus 1 new bootstrap-wiring test go GREEN, and all 6 existing tests in `tests/test_safe_storage.py` continue passing UNCHANGED.

**REVISION (B1, M5 from 87-REVIEWS.md):**
- **B1 (BLOCKER):** Adds the bootstrap wiring `ensure_session_uuid()` call to `web/main.py:create_layout()` — the function called as the first action of every `@ui.page` handler in the codebase. Adds an automated test (`test_create_layout_mints_session_uuid`) that asserts the bootstrap path mints the UUID, so this is no longer a "discovery" job for Plan 08's smoke check.
- **M5:** Implements strict `re.fullmatch(r"^[0-9a-f]{32}$")` validation on read; covers uppercase rejection, non-string rejection, malformed-length rejection, and AssertionError-during-write paths.

Purpose: Provide the foundational session-UUID API that Phases 88-92 will consume as a stable cache key (per HANDOFF_v7.11.1_path_b.md item 6: "Use this as the stable cache key wherever caching survives Path B. Tokens rotate; UUIDs don't."), AND make the minting automatic so Phases 88+ can rely on `_session_uuid` already being in storage by the time their code runs.

Output: Modified `web/safe_storage.py` with 2 new public functions, 1 new module constant, and `import re`/`import uuid as _uuid`; modified `web/main.py` with a single-line bootstrap call at the top of `create_layout`; appended 1 test to `tests/test_session_uuid.py` for the bootstrap wiring.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md
@web/safe_storage.py
@web/main.py
@tests/test_safe_storage.py
@tests/test_session_uuid.py

<interfaces>
<!-- New API contracts. Phase 88+ consumers will call these signatures. -->

Module-level addition (after existing imports):
```python
import re
import uuid as _uuid

_SESSION_UUID_KEY = '_session_uuid'
_SESSION_UUID_RE = re.compile(r"^[0-9a-f]{32}$")
```

New public functions:
```python
def get_session_uuid() -> str:
    """Return this session's stable UUID, minting one on first call.

    Returns a fresh ephemeral UUID4 hex string if storage is unavailable
    (prune race). Validates retrieved value against ^[0-9a-f]{32}$ — uppercase,
    non-string, and malformed values trigger regeneration (defends against
    T-87-02 storage-poisoning).

    Never returns None. Never raises.
    """


def ensure_session_uuid() -> bool:
    """Eagerly mint session UUID if not present.

    Returns True if minted or already exists (and is well-formed).
    Returns False only if storage is unavailable (prune race) — caller
    may retry on next request.

    Wired into web/main.py:create_layout() per B1 — every page render
    invokes this BEFORE downstream code that depends on _session_uuid.
    """
```

Bootstrap wiring (B1) — `web/main.py:create_layout()`:
```python
def create_layout():
    """Create the main application layout with modern Header and Sidebar."""
    # B1 (Phase 87 FOUND-01): mint _session_uuid on first page render of every
    # session. ensure_session_uuid() is idempotent and returns False harmlessly
    # on prune-race; downstream code (Phases 88+) can rely on _session_uuid
    # being present in storage after this point.
    ensure_session_uuid()

    resolved_lang = _resolve_ui_language()
    # ... rest of function unchanged
```

`create_layout()` is the natural hook point because 16 of 19 `@ui.page` handlers
in `web/main.py` call it as their first action — verified via
`grep "create_layout()" web/main.py` returning 16 lines: 1014, 1042, 1068, 1140,
1176, 1198, 1217, 1295, 1313, 1327, 1345, 1359, 1373, 1391, 1411, 1430. The 3
@ui.page handlers that do NOT call create_layout() are:
  - /privacy-extension (line 1245) — static privacy-policy page
  - /reset-hints (line 1279) — internal redirect-only route
  - /auth/callback (line 1436) — OAuth callback handler

These 3 routes rely on lazy-mint inside `get_session_uuid()` on first storage
read. Per R-01 in 87-RESEARCH.md the design is lazy-mint; the bootstrap call
in create_layout() is an optimization for the common path, not a hard
eager-mint guarantee. SC1 (every session has a stable UUID by the time it is
consumed) is satisfied either way because get_session_uuid() mints on demand.

If a future phase needs eager-mint on ALL 19 routes (e.g., to share the UUID
with client-side JS via add_head_html in the page handler itself), wire
ensure_session_uuid() into the 3 missing handlers explicitly. Phase 87 does
not need this.
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
  <name>Task 1: Add get_session_uuid + ensure_session_uuid + _SESSION_UUID_KEY + strict regex validation to web/safe_storage.py (M5)</name>
  <read_first>
    - web/safe_storage.py (CURRENT STATE — read fully so you understand the existing 3 helpers; you must NOT modify them)
    - tests/test_session_uuid.py (the 9 tests from Plan 01 — these are your acceptance contract; pay special attention to the 4 M5 tests: rejects_uppercase_hex, rejects_non_string, rejects_malformed_length, ensure_session_uuid_returns_false_on_assertion)
    - tests/test_safe_storage.py (FOUND-05 baseline — must remain passing UNCHANGED)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (the "Core pattern for new helpers" code block — copy the try/except shape exactly)
  </read_first>
  <files>web/safe_storage.py</files>
  <behavior>
    - Test 1 (test_session_uuid_unique_across_100_sessions): 100 separate storage dicts produce 100 distinct UUIDs (collision rate must be 0).
    - Test 2 (test_session_uuid_stable_within_session): get_session_uuid() called twice on the same storage dict returns the same value; storage now contains key '_session_uuid'.
    - Test 3 (test_session_uuid_survives_token_refresh): Mutating storage['auth_session'] = {...different...} between calls does NOT change the returned UUID.
    - Test 4 (test_session_uuid_returns_ephemeral_on_prune): When storage.get raises AssertionError, function returns a valid 32-char hex string (NOT None, NOT empty).
    - Test 5 (test_ensure_session_uuid_idempotent): ensure_session_uuid() called twice returns True both times; second call does NOT regenerate.
    - **NEW (M5)** Test 6 (test_session_uuid_rejects_uppercase_hex): Stored value `'ABCDEF...'` (32 uppercase hex) MUST be rejected — get_session_uuid returns a fresh lowercase value.
    - **NEW (M5)** Test 7 (test_session_uuid_rejects_non_string): Stored values of types int, dict, list, bytes, None MUST be rejected — fresh UUID returned.
    - **NEW (M5)** Test 8 (test_session_uuid_rejects_malformed_length): Strings of length != 32 OR containing non-[0-9a-f] characters MUST be rejected.
    - **NEW (M5)** Test 9 (test_ensure_session_uuid_returns_false_on_assertion): When storage.__setitem__ raises AssertionError, ensure_session_uuid() MUST return False without raising.
  </behavior>
  <action>
Edit `web/safe_storage.py`. The file currently has 80 lines (existing 3 helpers + module docstring + imports + logger). You must:

1. **Add imports after line 33** (after the existing `from typing import Any`):
```python
import re
import uuid as _uuid
```
(Use the underscore-prefixed alias `_uuid` to make it clear this is module-internal; matches the pattern in similar Python projects and keeps the public API surface clean. `re` is stdlib; no requirements change.)

2. **Add module constants after line 37** (after `logger = logging.getLogger(__name__)`):
```python


_SESSION_UUID_KEY = '_session_uuid'
_SESSION_UUID_RE = re.compile(r"^[0-9a-f]{32}$")
```

The `re.compile` pre-compiles the regex at module load (one-time cost). `re.fullmatch(pattern, str)` is equivalent to `re.match(pattern_with_anchors, str)`; we use `_SESSION_UUID_RE.fullmatch(uid)` for clarity. This is strictly tighter than the old `isinstance(uid, str) and len(uid) == 32` check because:
- `len(uid) == 32` accepts uppercase, mixed case, and non-hex chars
- `re.fullmatch(r"^[0-9a-f]{32}$", uid)` rejects all three

3. **Append new functions at end of file** (after line 79, the closing of `safe_user_pop`). Append the following two functions verbatim. Note the `_is_valid_uuid` private helper — it consolidates the M5 validation logic and is called from BOTH `get_session_uuid` and `ensure_session_uuid`:

```python


def _is_valid_uuid(value: Any) -> bool:
    """Return True iff `value` is a 32-char lowercase-hex string matching uuid4().hex shape.

    Per M5 in 87-REVIEWS.md: this is the canonical validation that defends
    against T-87-02 storage-poisoning (uppercase hex, non-string types,
    malformed length, non-hex characters all rejected).
    """
    return isinstance(value, str) and bool(_SESSION_UUID_RE.fullmatch(value))


def get_session_uuid() -> str:
    """Return this session's stable UUID, minting one on first call.

    The UUID is generated lazily via :func:`uuid.uuid4` (CSPRNG-backed in
    CPython per Python docs `uuid.uuid4`) and stored in
    ``app.storage.user['_session_uuid']``. It survives token refresh
    because it lives in storage, not in any auth dict -- Phase 91's
    auth-token rotation will not affect it.

    Returns a fresh ephemeral UUID4 hex string if storage is unavailable
    (prune race) -- callers receive a valid 32-char lowercase hex string
    but the same call site within a different prune window may get a
    DIFFERENT UUID. Downstream cache lookups against a fallback UUID will
    simply miss, which is the correct behavior (no false-positive cache hit).

    Validates retrieved value against ``^[0-9a-f]{32}$`` via
    :func:`_is_valid_uuid`. On storage-poisoning (non-string, uppercase
    hex, wrong length, or non-hex characters) mints fresh and overwrites.
    This defends against the T-87-02 storage-poisoning threat.

    Security note (T-87-03): NEVER log this UUID at INFO+ level, expose
    it in URLs/query strings, or include it in PostHog events without
    HMAC. It is an opaque server-side cache key -- treat as session-secret.

    :returns: A 32-character lowercase hex UUID4 string. Never None. Never raises.
    """
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if _is_valid_uuid(uid):
            return uid
        # Stored value missing, poisoned, or malformed -- mint fresh
        if uid is not None:
            logger.warning(
                "get_session_uuid: stored _session_uuid is not a valid 32-char "
                "lowercase hex string (type=%s); regenerating",
                type(uid).__name__,
            )
        new_uid = _uuid.uuid4().hex
        try:
            app.storage.user[_SESSION_UUID_KEY] = new_uid
        except AssertionError as e:
            logger.debug("get_session_uuid: prune-race during mint write: %s", e)
            # Return the new UUID anyway; it just won't be cached this request
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
    ``add_head_html``, or for Phase 88+ code that reads _session_uuid
    without going through get_session_uuid).

    Wired into web/main.py:create_layout() per B1 in 87-REVIEWS.md — every
    page render invokes this before any other layout logic, so the UUID
    is present in storage by the time downstream handlers execute.

    :returns: True if UUID is present in storage after the call (minted
              or already existed and well-formed). False only if storage
              raises AssertionError on either read or write (prune race)
              -- caller may retry on next request.
    """
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if _is_valid_uuid(uid):
            return True  # Already present and well-formed
        # Missing, poisoned, or malformed -- mint and write
        try:
            app.storage.user[_SESSION_UUID_KEY] = _uuid.uuid4().hex
            return True
        except AssertionError as e:
            logger.debug("ensure_session_uuid: prune-race during write: %s", e)
            return False
    except AssertionError as e:
        logger.debug("ensure_session_uuid: session storage unavailable: %s", e)
        return False
    except Exception as e:
        logger.warning("ensure_session_uuid unexpected failure: %s", e, exc_info=False)
        return False
```

**DO NOT MODIFY** lines 40-79 (the existing `safe_user_get`, `safe_user_set`, `safe_user_pop` functions). The tests in `tests/test_safe_storage.py` must continue passing without modification.

**DO NOT MODIFY** the module docstring at the top of the file. (It documents the v7.11.1 hotfix history; that history remains accurate.)

After editing, verify (Windows-safe Python one-liners):
```
python -c "import ast; ast.parse(open('web/safe_storage.py').read())"
python -c "from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop; print('OK: existing helpers')"
python -c "from web.safe_storage import get_session_uuid, ensure_session_uuid, _SESSION_UUID_KEY, _SESSION_UUID_RE; print('OK: new helpers + constants')"
python -m pytest tests/test_session_uuid.py -x -v
python -m pytest tests/test_safe_storage.py -x -v
```
  </action>
  <verify>
    <automated>python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x</automated>
  </verify>
  <acceptance_criteria>
    - `python -c "import ast; ast.parse(open('web/safe_storage.py').read())"` exits 0 (file is valid Python)
    - File contains `def get_session_uuid` exactly once: `python -c "import ast; tree = ast.parse(open('web/safe_storage.py').read()); print(sum(1 for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == 'get_session_uuid'))"` prints `1`
    - File contains `def ensure_session_uuid` exactly once (same Python check pattern)
    - File contains `_SESSION_UUID_KEY = '_session_uuid'`: `python -c "import re; src = open('web/safe_storage.py').read(); assert re.search(r\"_SESSION_UUID_KEY\\s*=\\s*'_session_uuid'\", src); print('OK')"` prints `OK`
    - File contains compiled regex: `python -c "import re; src = open('web/safe_storage.py').read(); assert re.search(r'_SESSION_UUID_RE\\s*=\\s*re\\.compile', src); print('OK')"` prints `OK`
    - File imports `uuid as _uuid`: same regex check pattern
    - File imports `re` (stdlib): same regex check pattern
    - Existing 3 helpers still defined (each exactly once): function-name count via AST as above for `safe_user_get`, `safe_user_set`, `safe_user_pop`
    - `python -m pytest tests/test_safe_storage.py -x` exits 0 with 6 tests PASSING (FOUND-05 invariant)
    - `python -m pytest tests/test_session_uuid.py -x` exits 0 with 9 tests PASSING (5 original + 4 M5)
    - `python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x` exits 0 (Plan 01 schema test still passes)
    - File `tests/test_safe_storage.py` byte-unchanged from baseline: `python -c "import subprocess; r = subprocess.run(['git', 'diff', '--stat', 'tests/test_safe_storage.py'], capture_output=True, text=True); assert r.stdout.strip() == '', r.stdout; print('OK')"` prints `OK`
  </acceptance_criteria>
  <done>web/safe_storage.py extended additively; 15 tests total pass (6 existing + 9 new with M5 coverage); existing helpers and existing test file untouched.</done>
</task>

<task type="auto">
  <name>Task 2: Wire ensure_session_uuid() into web/main.py:create_layout() (B1 BLOCKER fix)</name>
  <read_first>
    - web/main.py lines 342-360 (the `create_layout()` function definition — this is the bootstrap point)
    - web/main.py top of file (find the existing imports; locate where to add `from web.safe_storage import`)
    - The 16 `create_layout()` call sites in web/main.py to confirm this function IS called by every @ui.page handler (use `python -c "import re; print(len(re.findall(r'create_layout\(\)', open('web/main.py').read())))"`)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (B1 description — bootstrap wiring requirement)
  </read_first>
  <files>web/main.py</files>
  <action>
**B1 BLOCKER FIX:** Wire the bootstrap call into `create_layout()`. This is a minimal, surgical edit — adding 4 lines to one function.

**Step 1: Verify the create_layout() function still exists at the expected location.**

Run:
```
python -c "import re, pathlib; src = pathlib.Path('web/main.py').read_text(encoding='utf-8'); m = re.search(r'^def create_layout\(\):', src, re.MULTILINE); print('found at byte offset:', m.start() if m else 'NOT FOUND')"
```

Confirm `create_layout()` exists. Per current state, it is at line 342.

**Step 2: Verify create_layout() is the right hook point.**

Run:
```
python -c "import re; src = open('web/main.py').read(); n = len(re.findall(r'\\bcreate_layout\\(\\)', src)); print('create_layout call sites:', n)"
```

Expected: 16 (the 16 routes that use the standard layout; 3 other @ui.page routes — /privacy-extension, /reset-hints, /auth/callback — do NOT call create_layout() and rely on lazy-mint, which is the design per R-01). If this returns fewer than 10, STOP and consult the user — the codebase has drifted and a different bootstrap point may be needed.

**Step 3: Add the import.**

Locate the existing imports section near the top of `web/main.py` (search for `from web.safe_storage` first — Plan 04 will add safe_user_get/set/pop imports later, but in Wave 1 this plan adds only `ensure_session_uuid`). If `from web.safe_storage` already appears (it does not in current state, but defensively check), EXTEND that import to include `ensure_session_uuid`. Otherwise add a new import line.

Use the Edit tool with the exact `old_string` matching one of the existing `from web.` import lines (so we insert nearby without disturbing import ordering). Suggested placement: after the existing `from web.auth_state import GlobalAuthState` line (which exists for the OAuth callback).

Add this import line:
```python
from web.safe_storage import ensure_session_uuid
```

**Step 4: Insert the bootstrap call at the top of create_layout().**

Current code (verify by reading first):
```python
def create_layout():
    """Create the main application layout with modern Header and Sidebar."""

    resolved_lang = _resolve_ui_language()
    set_language(resolved_lang)
```

Edit to:
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
```

This is the ONLY edit to create_layout() in this plan. Do NOT touch `_safe_user_storage_get('current_page', '/')` on line 348 — that local helper is deleted by Plan 04. Plan 04's edits to main.py happen in Wave 2, AFTER Wave 1's bootstrap wiring lands.

**Step 5: Verify the change.**

```
python -c "import ast; ast.parse(open('web/main.py').read()); print('parses OK')"
python -c "import re; src = open('web/main.py').read(); print('ensure_session_uuid import:', len(re.findall(r'from web\\.safe_storage import.*ensure_session_uuid', src)))"
python -c "import re; src = open('web/main.py').read(); print('ensure_session_uuid call:', len(re.findall(r'ensure_session_uuid\\(\\)', src)))"
# Expected: import count >= 1, call count >= 1
```

Run ruff:
```
ruff check web/main.py
```

Expected: zero new errors (the file may already have pre-existing ruff warnings — note them but do not fix them unless they are introduced by this edit).
  </action>
  <verify>
    <automated>python -c "import ast, re; src = open('web/main.py').read(); ast.parse(src); assert re.search(r'from web\\.safe_storage import.*ensure_session_uuid', src), 'import missing'; assert re.search(r'def create_layout\\(\\):', src), 'create_layout missing'; layout_body = src[src.index('def create_layout()'):src.index('def create_layout()')+800]; assert 'ensure_session_uuid()' in layout_body, 'call not in create_layout body'; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `web/main.py` parses as valid Python after the edit
    - File contains exactly one `from web.safe_storage import ... ensure_session_uuid` line: verified via the Python regex check in `<verify>`
    - The call `ensure_session_uuid()` appears within the first 30 lines of the `create_layout()` function body (verified by checking it appears in the substring from `def create_layout()` to the next `def `)
    - A comment near the call references B1 or FOUND-01 to document why the line exists (verified with `python -c "import re; src = open('web/main.py').read(); body = src[src.index('def create_layout()'):src.index('def create_layout()')+800]; assert re.search(r'B1|FOUND-01|87-REVIEWS', body), body[:400]; print('OK')"` prints `OK`)
    - `ruff check web/main.py` exits 0 (no NEW lint errors introduced — pre-existing warnings allowed)
    - `python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 Task 1 invariant preserved; the bootstrap wiring did not break helper tests because tests use mocks)
  </acceptance_criteria>
  <done>create_layout() calls ensure_session_uuid() as its first statement; import added; ruff clean.</done>
</task>

<task type="auto">
  <name>Task 2b: Wire ensure_session_uuid() into /reset-hints and /auth/callback (Fix 1 in 87-REVIEWS.md iteration 3 — Codex B1-residual)</name>
  <read_first>
    - web/main.py lines 1279-1284 (the /reset-hints route — pops 3 hint keys directly from app.storage.user; needs ensure_session_uuid wiring)
    - web/main.py lines 1436-1470 (the /auth/callback route — writes USER_KEY/PROFILE_KEY/auth_session in complete_login closure; needs ensure_session_uuid wiring BEFORE the closures are defined so the UUID is minted before any auth telemetry fires)
    - web/main.py lines 1245-1278 (the /privacy-extension route — verify it has zero storage access; this route is INTENTIONALLY SKIPPED and the route-coverage test exempts it)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md (Codex round 2 HIGH residual B1 finding)
    - tests/test_session_uuid.py (verify test_every_ui_page_handler_mints_uuid exists from Plan 01; this task's edits make that test pass)
  </read_first>
  <files>web/main.py</files>
  <action>
**Fix 1 (Codex B1-residual):** Wire `ensure_session_uuid()` into the 2 non-`create_layout()` routes that touch storage. `/privacy-extension` is INTENTIONALLY SKIPPED — no UUID needed because nothing reads or writes storage there.

**Step 1: Verify the import added by Task 2 is already in place.**

```
python -c "import re; src = open('web/main.py').read(); m = re.search(r'from web\.safe_storage import.*ensure_session_uuid', src); print('import present:', bool(m))"
```

Expected: `True` (Task 2 added it). No additional import needed for this task.

**Step 2: Wire `/reset-hints` (line ~1279-1284).**

Read 10 lines around line 1279 to confirm the current shape. The route currently looks like:

```python
@ui.page('/reset-hints')
def reset_hints_route():
    """Hidden utility route to reset all feature discovery hints."""
    for key in ('whats_new_dismissed', 'hint_responsa_seen', 'hint_tabular_seen'):
        app.storage.user.pop(key, None)
    ui.navigate.to('/')
```

Add `ensure_session_uuid()` as the FIRST statement inside the function (after the docstring). After the edit:

```python
@ui.page('/reset-hints')
def reset_hints_route():
    """Hidden utility route to reset all feature discovery hints."""
    ensure_session_uuid()  # Fix 1 in 87-REVIEWS.md iter 3 (Codex B1-residual): mint UUID before storage pops
    for key in ('whats_new_dismissed', 'hint_responsa_seen', 'hint_tabular_seen'):
        app.storage.user.pop(key, None)
    ui.navigate.to('/')
```

**Note:** The 3 `app.storage.user.pop` calls on line 1282 REMAIN AS-IS — they are migrated to `safe_user_pop` by Plan 04 (leaf-file migrations). This task only adds the bootstrap wiring; the migration is unchanged.

**Step 3: Wire `/auth/callback` (line ~1436).**

Read 10 lines around line 1436 to confirm the current shape. The route currently looks like:

```python
@ui.page('/auth/callback')
async def auth_callback_route(code: str = None, error: str = None, error_description: str = None):
    """
    OAuth callback handler.
    Supabase redirects here after Google login with ?code= parameter (PKCE flow).
    Also handles ?error= / ?error_description= from cancelled or failed OAuth attempts.
    """
    from web.supabase_client import get_profile, exchange_code_for_session
    from web.auth_state import GlobalAuthState
    ...
```

Add `ensure_session_uuid()` as the FIRST statement inside the function (after the docstring, BEFORE the `from web.supabase_client import ...` line). After the edit:

```python
@ui.page('/auth/callback')
async def auth_callback_route(code: str = None, error: str = None, error_description: str = None):
    """
    OAuth callback handler.
    Supabase redirects here after Google login with ?code= parameter (PKCE flow).
    Also handles ?error= / ?error_description= from cancelled or failed OAuth attempts.
    """
    ensure_session_uuid()  # Fix 1 in 87-REVIEWS.md iter 3 (Codex B1-residual): mint UUID before OAuth atomic writes / telemetry
    from web.supabase_client import get_profile, exchange_code_for_session
    from web.auth_state import GlobalAuthState
    ...
```

**Why BEFORE the imports:** the OAuth flow fires `posthog_capture('login_success', ...)` and writes to `app.storage.user` (USER_KEY, PROFILE_KEY, auth_session) — both of which may depend on `_session_uuid` being present in storage for downstream Phase 88+ telemetry consumers. Minting first guarantees the UUID is available before any of those side effects fire.

**Note:** The 3 `app.storage.user[...]` writes in `complete_login` (lines 1458, 1460, 1463) REMAIN AS-IS — they are allowlisted by Plan 01 (the OAuth atomic writes are deferred to Phase 91 AUTHW-01). This task only adds the bootstrap wiring; the writes are unchanged.

**Step 4: Confirm /privacy-extension is intentionally skipped.**

Read web/main.py:1245-1278. Verify the entire body uses only `ui.add_head_html`, `ui.column`, `ui.label` — zero `app.storage.user` access of any kind. This route is a pure static info page (browser-extension privacy policy). The route-coverage test (`test_every_ui_page_handler_mints_uuid` in Plan 01) hard-codes `/privacy-extension` into its `EXEMPT_ROUTES` set.

If reading reveals ANY `app.storage.user` access on /privacy-extension (this would be a surprise — current state has none), STOP and consult the user. The exempt-list assumption would be invalid and require a third wiring site here.

**Step 5: Verify (Windows-safe).**

```
python -c "import ast; ast.parse(open('web/main.py').read()); print('parses OK')"
ruff check web/main.py
```
  </action>
  <verify>
    <automated>python -c "import ast; tree = ast.parse(open('web/main.py').read()); ok = {n.name: any(isinstance(c, ast.Call) and isinstance(c.func, ast.Name) and c.func.id == 'ensure_session_uuid' for c in ast.walk(n)) for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in {'reset_hints_route', 'auth_callback_route'}}; assert ok.get('reset_hints_route'), 'reset_hints_route missing'; assert ok.get('auth_callback_route'), 'auth_callback_route missing'; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `web/main.py` parses as valid Python after the edits
    - `reset_hints_route` body contains a call to `ensure_session_uuid()` (verified via the `<verify>` Python AST scan)
    - `auth_callback_route` body contains a call to `ensure_session_uuid()` (verified via the `<verify>` Python AST scan)
    - The call in `auth_callback_route` appears BEFORE the `from web.supabase_client import ...` line (verify by reading lines 1436-1450: `ensure_session_uuid()` should appear before any other statement that isn't a docstring)
    - `/privacy-extension` route body has zero `app.storage.user` accesses
    - `ruff check web/main.py` exits 0 (no NEW lint errors introduced)
    - `python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (helpers + bootstrap tests still green)
    - `python -m pytest tests/test_session_uuid.py::test_every_ui_page_handler_mints_uuid -x` exits 0 (the route-coverage regression guard now PASSES after this task lands — Plan 01 created the test, this task makes it green)
  </acceptance_criteria>
  <done>reset_hints_route and auth_callback_route both call ensure_session_uuid() as their first statement; /privacy-extension intentionally skipped; route-coverage test passes; B1-residual closed.</done>
</task>

<task type="auto">
  <name>Task 3: Add automated test for the bootstrap wiring (B1 — test_create_layout_mints_session_uuid)</name>
  <read_first>
    - tests/test_session_uuid.py (CURRENT STATE after Plan 01 — read fully; the new test gets APPENDED, not inserted)
    - web/main.py:create_layout() body (so the test understands what it's exercising)
    - web/safe_storage.py (so the test imports the right symbol)
  </read_first>
  <files>tests/test_session_uuid.py</files>
  <action>
**B1 automated coverage:** Add a 10th test to `tests/test_session_uuid.py` that exercises the bootstrap wiring. Without this test, Plan 08's smoke check would be the only signal that wiring works — exactly the failure mode Codex flagged in B1.

The test cannot exercise `create_layout()` directly (it requires NiceGUI runtime + ui imports that won't work outside a NiceGUI app context). Instead, the test asserts the WIRING by:
1. Mock `web.safe_storage.app` (the storage module attribute that `ensure_session_uuid` accesses).
2. Import `ensure_session_uuid` from `web.main` (verifies the import was added per Task 2).
3. Call `ensure_session_uuid()` directly and assert the storage now contains `_session_uuid`.

This is a unit-level test of the wiring (does `web.main` import the function? does calling it produce the expected effect?), not an integration test of the NiceGUI page lifecycle. The smoke check in Plan 08 covers the integration angle.

**Step 1: Read the current state of tests/test_session_uuid.py.**

Run:
```
python -c "import re; src = open('tests/test_session_uuid.py').read(); print('current test count:', len(re.findall(r'^def test_', src, re.MULTILINE)))"
```

Expected: 10 (the 5 base + 4 M5 tests + 1 route-coverage test from Plan 01).

**Step 2: Append the new test at the END of the file.**

Append this code (with a leading blank line so it doesn't merge with the last existing test):

```python


def test_create_layout_mints_session_uuid():
    """B1 (87-REVIEWS.md): web/main.py:create_layout() must call ensure_session_uuid().

    This test verifies the BOOTSTRAP WIRING — that the function created in Plan 02
    Task 1 is actually IMPORTED and CALLED from the page bootstrap path. The test
    cannot exercise create_layout() directly (it requires a NiceGUI runtime), but
    it CAN verify that web.main imports ensure_session_uuid AND that calling it
    has the expected storage side effect.

    Failure modes this test catches:
    - Someone deletes the `ensure_session_uuid()` call from create_layout()
    - Someone removes the `from web.safe_storage import ensure_session_uuid` import
    - The implementation of ensure_session_uuid() regresses such that the mint
      no longer happens (also covered by test_ensure_session_uuid_idempotent,
      but doubled here as a defense-in-depth check)

    Note: this test does NOT import web.main (which would pull in NiceGUI's full
    page-router machinery). Instead it reads web/main.py's source and asserts the
    wiring at the textual level, then exercises ensure_session_uuid() directly.
    """
    import re
    import pathlib

    # Part A: textual verification that web/main.py imports and calls ensure_session_uuid
    main_src = pathlib.Path(__file__).resolve().parent.parent.joinpath('web', 'main.py').read_text(encoding='utf-8')
    assert re.search(r'from web\.safe_storage import.*ensure_session_uuid', main_src), (
        "web/main.py must import ensure_session_uuid from web.safe_storage (B1 wiring)"
    )
    # Locate create_layout body and confirm ensure_session_uuid() is called inside it
    layout_match = re.search(r'^def create_layout\(\):.*?(?=^def )', main_src, re.MULTILINE | re.DOTALL)
    assert layout_match, "web/main.py must define create_layout()"
    layout_body = layout_match.group(0)
    assert 'ensure_session_uuid()' in layout_body, (
        "web/main.py:create_layout() must call ensure_session_uuid() — B1 bootstrap wiring missing"
    )

    # Part B: functional verification that calling ensure_session_uuid mints the UUID
    storage = {}
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import ensure_session_uuid
        result = ensure_session_uuid()
        assert result is True, "ensure_session_uuid() should return True on fresh storage"
        minted = storage.get('_session_uuid')
        assert minted, "_session_uuid was not stored after ensure_session_uuid() returned True"
        assert isinstance(minted, str)
        assert len(minted) == 32
        # Verify regex shape per M5
        import re as _re
        assert _re.fullmatch(r'^[0-9a-f]{32}$', minted), (
            f"Minted UUID {minted!r} does not match ^[0-9a-f]{{32}}$"
        )
```

**Step 3: Verify the test file now has 11 tests.**

```
python -c "import re; src = open('tests/test_session_uuid.py').read(); print('test count:', len(re.findall(r'^def test_', src, re.MULTILINE)))"
```

Expected: `test count: 11` (5 base + 4 M5 + 1 route-coverage from Plan 01 + 1 bootstrap from this task).

**Step 4: Run the full file.**

```
python -m pytest tests/test_session_uuid.py -x -v
```

Expected: 11 passed (10 from Plan 01 — 5 base + 4 M5 + 1 route-coverage — + 1 new bootstrap test). If `test_create_layout_mints_session_uuid` fails:
- If it fails at Part A (textual): Task 2's edit to web/main.py was wrong or incomplete. Re-check that the import and call are present.
- If it fails at Part B (functional): Task 1's `ensure_session_uuid()` implementation has a bug. Re-check the function body.
  </action>
  <verify>
    <automated>python -m pytest tests/test_session_uuid.py::test_create_layout_mints_session_uuid -x -v</automated>
  </verify>
  <acceptance_criteria>
    - `tests/test_session_uuid.py` contains exactly 11 `def test_*` functions (verified via `python -c "import re; print(len(re.findall(r'^def test_', open('tests/test_session_uuid.py').read(), re.MULTILINE)))"` prints `11`)
    - The new test name `test_create_layout_mints_session_uuid` is present: `python -c "assert 'def test_create_layout_mints_session_uuid' in open('tests/test_session_uuid.py').read(); print('OK')"` prints `OK`
    - `python -m pytest tests/test_session_uuid.py -x` exits 0 with all 11 tests passing
    - `python -m pytest tests/test_session_uuid.py::test_create_layout_mints_session_uuid -x` exits 0 (the new bootstrap test specifically passes)
    - `python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (full Phase 87 helper test suite green: 6 + 11 = 17 tests)
  </acceptance_criteria>
  <done>Bootstrap-wiring test added and passing; FOUND-01 SC1 is now verified by automated test, not by Plan 08's manual smoke check.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Browser cookie -> NiceGUI SessionMiddleware | Existing; not affected by this plan |
| NiceGUI session storage (app.storage.user) -> safe_storage helpers | The chokepoint. After this plan, get_session_uuid / ensure_session_uuid are the canonical session-UUID accessors. |
| safe_storage helpers -> callers | Helpers MUST always return valid strings (or False for ensure_*); never raise to caller. |
| Page handler entry -> ensure_session_uuid() | B1 wiring: every @ui.page route calls create_layout() which calls ensure_session_uuid() before any session-uuid-dependent code runs |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-01 | Information disclosure / Spoofing | UUID predictability for cache-key collision | mitigate | Use `uuid.uuid4().hex` (CSPRNG-backed in CPython; 122 bits of entropy). Verified by `test_session_uuid_unique_across_100_sessions` -- 0 collisions across 100 simulated sessions. NEVER use `uuid.uuid1()` (MAC-leaking) or `random.*` (predictable). Encoded in the implementation: `_uuid.uuid4().hex` is the only UUID source. |
| T-87-02 | Tampering | Storage poisoning -- malicious user mutates own session storage to inject non-UUID `_session_uuid` value | mitigate | **STRICT** pattern validation on read via `_SESSION_UUID_RE.fullmatch(value)` where `_SESSION_UUID_RE = re.compile(r"^[0-9a-f]{32}$")`. Defends against: uppercase hex (rejected), non-string types (rejected — int, dict, list, bytes, None), wrong length (rejected), non-hex characters (rejected). On validation failure, mint fresh and overwrite. Encoded in both `get_session_uuid` and `ensure_session_uuid` via the `_is_valid_uuid` private helper. Verified by 4 dedicated tests in tests/test_session_uuid.py: rejects_uppercase_hex, rejects_non_string, rejects_malformed_length, ensure_session_uuid_returns_false_on_assertion. |
| T-87-03 | Information disclosure | UUID leakage via logs/URLs/PostHog | mitigate | Documented in function docstring (Security note). Implementation logs only at `debug` level for storage-unavailable case (which uses an ephemeral UUID anyway) and `warning` for unexpected failures / poisoned values (warning logs the TYPE of poisoned value, not its content). No URL/query/PostHog exposure introduced by this plan. |
| T-87-04 | Tampering | Allowlist for raw access -- N/A this plan; allowlist contents not changed | accept | This plan touches only web/safe_storage.py + web/main.py:create_layout() + tests/test_session_uuid.py. Allowlist scope unchanged. |
| T-87-05 | Information disclosure | Alias resolution -- N/A this plan | accept | No lint scanner work in this plan |

Block on: T-87-01 (HIGH) -- mitigation verified by Test 1 (100-session uniqueness). T-87-02 (MEDIUM-HIGH per M5 reinforcement) -- mitigation verified by 4 dedicated regex-validation tests.
</threat_model>

<verification>
After all 3 tasks (Windows-safe commands throughout):

```
# Verify Wave 0 -> Wave 1 transition
python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x -v
# Expected: 17 passed in <2 seconds (6 + 11)

# Verify FOUND-05 file invariant
python -c "import subprocess; r = subprocess.run(['git', 'diff', '--stat', 'tests/test_safe_storage.py'], capture_output=True, text=True); assert not r.stdout.strip(), r.stdout; print('FOUND-05 invariant preserved')"

# Verify the new test exists and passes
python -m pytest tests/test_session_uuid.py::test_create_layout_mints_session_uuid -x -v

# Verify Plan 01 lint scanner still partially-passing
python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x

# Verify no regressions
ruff check web/safe_storage.py web/main.py

# Verify the additive nature of safe_storage.py
python -c "
import ast
tree = ast.parse(open('web/safe_storage.py').read())
funcs = sorted(n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef))
expected = sorted(['safe_user_get', 'safe_user_set', 'safe_user_pop', 'get_session_uuid', 'ensure_session_uuid', '_is_valid_uuid'])
assert funcs == expected, f'Function set mismatch: got {funcs}, expected {expected}'
print('OK: 6 functions, all expected (3 existing + 3 new including _is_valid_uuid)')
"

# Verify B1 wiring is present in web/main.py
python -c "
import re
src = open('web/main.py').read()
assert re.search(r'from web\\.safe_storage import.*ensure_session_uuid', src), 'B1 import missing'
layout = re.search(r'^def create_layout\\(\\):.*?(?=^def )', src, re.MULTILINE | re.DOTALL).group(0)
assert 'ensure_session_uuid()' in layout, 'B1 call missing from create_layout'
print('OK: B1 bootstrap wiring present')
"
```
</verification>

<success_criteria>
1. `web/safe_storage.py` has 6 functions: `safe_user_get`, `safe_user_set`, `safe_user_pop`, `_is_valid_uuid`, `get_session_uuid`, `ensure_session_uuid`
2. Module constants `_SESSION_UUID_KEY = '_session_uuid'` and `_SESSION_UUID_RE = re.compile(r"^[0-9a-f]{32}$")` defined
3. `web/main.py:create_layout()` calls `ensure_session_uuid()` as its first statement (B1 wiring); import is present
4. `tests/test_session_uuid.py` has 11 tests including `test_create_layout_mints_session_uuid` (B1 automated coverage) and `test_every_ui_page_handler_mints_uuid` (Fix 1 — Codex B1-residual route-coverage guard, created by Plan 01, made green by Task 2b)
5. `python -m pytest tests/test_safe_storage.py -x` -> 6 passed (FOUND-05)
6. `python -m pytest tests/test_session_uuid.py -x` -> 11 passed (FOUND-01 + M5 + B1 + Fix 1 route-coverage)
7. `tests/test_safe_storage.py` byte-identical to baseline
8. Existing 3 helper signatures and bodies UNCHANGED (verified by AST function-set comparison)
9. Threat T-87-01 mitigated: UUID generated via `uuid.uuid4().hex` (CSPRNG); zero collisions across 100 simulated sessions
10. Threat T-87-02 mitigated: strict `^[0-9a-f]{32}$` validation on read; uppercase/non-string/malformed all rejected (M5 — 4 tests)
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-02-SUMMARY.md` summarizing:
- Functions added to web/safe_storage.py (with signatures): _is_valid_uuid, get_session_uuid, ensure_session_uuid
- B1 wiring: location in web/main.py (line number where ensure_session_uuid() is called inside create_layout)
- Fix 1 wiring (Codex B1-residual): line numbers in reset_hints_route and auth_callback_route where ensure_session_uuid() is called; confirmation that /privacy-extension is the single intentionally-skipped route
- Test results: 6/6 existing tests pass + 11/11 new tests pass = 17/17 total
- T-87-01 verification: 100-session uniqueness confirmed
- T-87-02 verification: strict regex validation; 4 dedicated tests for uppercase/non-string/malformed/AssertionError-on-write
- B1 verification: test_create_layout_mints_session_uuid exercises the bootstrap path
- Note: `tests/test_safe_storage.py` is byte-identical to baseline (sha256 confirmation)
</output>
