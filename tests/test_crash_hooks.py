# -*- coding: utf-8 -*-
"""Phase 113 Plan 02 — Lock-free crash primitives + hook-restoring reset tests.

Covers: D-05 (lock-free), REVIEWS HIGH-2 (module-top import), REVIEWS HIGH-3
(startup distinct-id), REVIEWS MEDIUM-8 (hook-restoring reset), REVIEWS PASS2
(OS base props + traceback-id dedup).

No `qtbot` parameter is used anywhere in this file (repo is pytest-qt-FREE;
REVIEWS MEDIUM-6).
"""

from __future__ import annotations

import inspect
import sys

import pytest

import desktop.telemetry as tel


# ---------------------------------------------------------------------------
# Module-level autouse wrapper — opt-in to crash_telemetry_state fixture.
# Scoped to this file only (never project-wide).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _use(crash_telemetry_state):
    yield


# ---------------------------------------------------------------------------
# CRASH-05 / D-05 BLOCKER — lock-free hook body
# ---------------------------------------------------------------------------
def test_hook_acquires_no_locks(monkeypatch):
    """D-05 BLOCKER: _emit_crash_direct acquires no locks.

    Monkeypatches both _enabled_lock and _state_lock to a fail-on-acquire
    object. Calling _emit_crash_direct must not raise AssertionError and
    must produce exactly one send_crash_event_direct call.
    """
    class _FailLock:
        def acquire(self, *a, **kw):
            raise AssertionError("lock acquired in crash hook — D-05 violation")
        def __enter__(self):
            self.acquire()
        def __exit__(self, *a):
            pass
        def release(self, *a, **kw):
            pass

    monkeypatch.setattr(tel, '_enabled_lock', _FailLock())
    monkeypatch.setattr(tel, '_state_lock', _FailLock())
    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'test-uuid')

    sent = []
    # Monkeypatch on tel (the module-top import binding) — not ph.send_crash_event_direct,
    # which is a different name binding from the imported-at-top reference in tel.
    monkeypatch.setattr(tel, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))

    # Must not raise — lock acquisition would raise AssertionError
    tel._emit_crash_direct(ValueError, None, is_background=False)

    assert 'desktop_crash' in sent, (
        "_emit_crash_direct did not call send_crash_event_direct — "
        "possibly acquired a lock and deadlocked, or consent check used a lock"
    )


def test_recursion_guard(monkeypatch):
    """CRASH-05: crash inside crash handler does not recurse.

    Set _in_crash_hook True → assert zero sends.
    With guard False → assert one send and guard reset to False afterward.
    """
    sent = []
    monkeypatch.setattr(tel, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))
    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'test-uuid')

    # With recursion guard set, no send should happen
    monkeypatch.setattr(tel, '_in_crash_hook', True)
    tel._emit_crash_direct(ValueError, None, is_background=False)
    assert len(sent) == 0, "Recursion guard did not prevent double-emit"

    # With guard False, exactly one send + guard reset to False
    monkeypatch.setattr(tel, '_in_crash_hook', False)
    tel._emit_crash_direct(ValueError, None, is_background=False)
    assert len(sent) == 1, "Expected exactly one send with guard=False"
    # Guard should be reset to False after the call (not stuck True)
    assert tel._in_crash_hook is False, "_in_crash_hook not reset to False after emit"


def test_is_enabled_nolock_returns_enabled(monkeypatch):
    """D-05: _is_enabled_nolock() returns _enabled without acquiring a lock."""
    monkeypatch.setattr(tel, '_enabled', False)
    assert tel._is_enabled_nolock() is False

    monkeypatch.setattr(tel, '_enabled', True)
    assert tel._is_enabled_nolock() is True


def test_base_props_no_lock(monkeypatch):
    """D-05 / RESEARCH A1: _BASE_PROPS() acquires no lock.

    Pins the invariant: if _BASE_PROPS() ever reads from _state_lock, crash
    hooks would be at risk of deadlock. Monkeypatch _state_lock to fail-on-
    acquire and assert no exception is raised.
    """
    class _FailLock:
        def acquire(self, *a, **kw):
            raise AssertionError("_BASE_PROPS() acquired _state_lock — D-05 violation")
        def __enter__(self):
            self.acquire()
        def __exit__(self, *a):
            pass

    monkeypatch.setattr(tel, '_state_lock', _FailLock())
    # Must not raise
    props = tel._BASE_PROPS()
    assert 'platform' in props
    assert 'app_version' in props


# ---------------------------------------------------------------------------
# REVIEWS HIGH-2 — module-top import (no in-function import in _emit_crash_direct)
# ---------------------------------------------------------------------------
def test_no_in_function_import():
    """REVIEWS HIGH-2: send_crash_event_direct is imported at module top.

    _emit_crash_direct must NOT contain an 'import' statement inside its body
    (importing inside a failing-thread hook can take the import lock).
    Uses inspect.getsource + ast.parse to check only the code, not docstrings.
    """
    import ast
    src = inspect.getsource(tel._emit_crash_direct)
    # Strip the docstring by parsing and removing it before checking
    tree = ast.parse(src)
    func_def = tree.body[0]
    # Remove leading docstring node if present
    body_stmts = func_def.body
    if (body_stmts and isinstance(body_stmts[0], ast.Expr)
            and isinstance(body_stmts[0].value, ast.Constant)
            and isinstance(body_stmts[0].value.value, str)):
        body_stmts = body_stmts[1:]
    # Reconstruct a minimal module with only the body statements
    code_only = ast.Module(body=body_stmts, type_ignores=[])
    code_text = ast.unparse(code_only)
    assert 'import' not in code_text, (
        "_emit_crash_direct contains an 'import' statement in its body — "
        "importing inside the crash hook can take the import lock (HIGH-2)"
    )


def test_send_crash_event_direct_imported_at_module_top():
    """REVIEWS HIGH-2: send_crash_event_direct is in the module-top import block.

    The symbol must be accessible as tel.send_crash_event_direct would NOT be
    (it lives in shared.posthog_server), but the module-top `from shared.posthog_server
    import send_crash_event_direct` line must exist.
    """
    import ast
    src = inspect.getsource(tel)
    tree = ast.parse(src)
    # Look for a top-level import of send_crash_event_direct
    found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == 'send_crash_event_direct':
                    found = True
                    break
    assert found, (
        "send_crash_event_direct is NOT in the module-top import block of desktop/telemetry.py "
        "(required by REVIEWS HIGH-2 to avoid import-lock deadlock)"
    )


# ---------------------------------------------------------------------------
# REVIEWS PASS2 — OS base props (CRASH-04/SC#3/D-02)
# ---------------------------------------------------------------------------
def test_base_props_includes_os(monkeypatch):
    """REVIEWS PASS2 / CRASH-04: _BASE_PROPS() includes non-empty os_family and os_version.

    Also verifies that a crash payload (props passed to send_crash_event_direct
    via _emit_crash_direct) contains both keys.
    """
    # Check _BASE_PROPS() directly
    props = tel._BASE_PROPS()
    assert 'os_family' in props, "_BASE_PROPS() missing 'os_family'"
    assert 'os_version' in props, "_BASE_PROPS() missing 'os_version'"
    assert props['os_family'], "os_family is empty in _BASE_PROPS()"
    assert props['os_version'], "os_version is empty in _BASE_PROPS()"

    # Check that a crash payload via _emit_crash_direct also carries these keys
    captured = []

    def capture_send(ev, props, did, timeout=0.5):
        captured.append(props)

    monkeypatch.setattr(tel, 'send_crash_event_direct', capture_send)
    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'test-uuid')

    tel._emit_crash_direct(ValueError, None, is_background=False)

    assert len(captured) == 1, "Expected exactly one crash send"
    crash_props = captured[0]
    assert 'os_family' in crash_props, "Crash payload missing 'os_family'"
    assert 'os_version' in crash_props, "Crash payload missing 'os_version'"
    assert crash_props['os_family'], "os_family is empty in crash payload"
    assert crash_props['os_version'], "os_version is empty in crash payload"


# ---------------------------------------------------------------------------
# REVIEWS PASS2 — traceback-id dedup (CONTEXT D-08)
# ---------------------------------------------------------------------------
def test_duplicate_traceback_deduped(monkeypatch):
    """REVIEWS PASS2 / D-08: calling _emit_crash_direct twice with the SAME exc_tb
    yields exactly ONE send; with a DIFFERENT traceback yields a second send.
    """
    sent = []
    monkeypatch.setattr(tel, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))
    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'test-uuid')
    # Reset dedup state
    monkeypatch.setattr(tel, '_last_reported_tb_id', None)

    # Create a real traceback object via try/except
    try:
        raise ValueError("test error 1")
    except ValueError:
        import sys as _sys
        tb1 = _sys.exc_info()[2]

    try:
        raise RuntimeError("test error 2")
    except RuntimeError:
        tb2 = _sys.exc_info()[2]

    # First call with tb1 → one send
    tel._emit_crash_direct(ValueError, tb1, is_background=False)
    assert len(sent) == 1, "Expected first send for tb1"

    # Second call with same tb1 → should be deduped (still only 1 send)
    tel._emit_crash_direct(ValueError, tb1, is_background=False)
    assert len(sent) == 1, "Same traceback was not deduped — got duplicate send"

    # Call with different tb2 → should send (total 2)
    tel._emit_crash_direct(RuntimeError, tb2, is_background=False)
    assert len(sent) == 2, "Expected second send for different tb2"


# ---------------------------------------------------------------------------
# REVIEWS HIGH-3 — startup distinct-id via set_consent
# ---------------------------------------------------------------------------
def test_set_consent_populates_crash_distinct_id(monkeypatch):
    """HIGH-3: after set_consent(True), _crash_distinct_id equals the resolved distinct_id."""
    # After set_consent(True), _crash_distinct_id should be populated
    tel.set_consent(True)
    assert tel._crash_distinct_id is not None, (
        "_crash_distinct_id not set after set_consent(True)"
    )
    # Should match the install_id that was minted
    install_id = tel.get_install_id()
    assert tel._crash_distinct_id == install_id or tel._crash_distinct_id == 'system', (
        "_crash_distinct_id should match install_id after opt-in"
    )


# ---------------------------------------------------------------------------
# REVIEWS MEDIUM-8 — hook-restoring reset
# ---------------------------------------------------------------------------
def test_reset_for_tests_restores_hooks(monkeypatch):
    """REVIEWS MEDIUM-8: _reset_for_tests restores sys.excepthook and _hooks_installed=False.

    Install a sentinel sys.excepthook, call install_exception_hooks(), then
    _reset_for_tests(), assert sys.excepthook is back to the sentinel and
    _hooks_installed is False.
    """
    sentinel_calls = []

    def sentinel_hook(t, v, tb):
        sentinel_calls.append(t)

    # Install sentinel as the "prior" hook
    original = sys.excepthook
    sys.excepthook = sentinel_hook
    try:
        # Install exception hooks — this wraps sentinel_hook
        tel.install_exception_hooks()
        assert tel._hooks_installed is True, "install_exception_hooks() did not set _hooks_installed=True"

        # The installed hook should be different from sentinel
        assert sys.excepthook is not sentinel_hook, "install_exception_hooks() did not replace sys.excepthook"

        # Reset for tests — should restore sentinel_hook
        tel._reset_for_tests()

        assert sys.excepthook is sentinel_hook, (
            "_reset_for_tests() did not restore sys.excepthook to pre-install hook"
        )
        assert tel._hooks_installed is False, (
            "_reset_for_tests() did not reset _hooks_installed to False"
        )
    finally:
        # Always restore original excepthook to avoid test pollution
        sys.excepthook = original
        tel._reset_for_tests()
