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
