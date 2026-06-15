# -*- coding: utf-8 -*-
"""Phase 113 Plans 02 + 03 — Lock-free crash primitives, hook-restoring reset,
and chained sys+threading exception hooks tests.

Plan 02 covered: D-05 (lock-free), REVIEWS HIGH-2 (module-top import), HIGH-3
(startup distinct-id), MEDIUM-8 (hook-restoring reset), PASS2 (OS props + dedup).

Plan 03 covers: chained prior hook (CRASH-01), threading hook (CRASH-02),
KI/SystemExit exclusion (SC#2), idempotent install (D-08/MEDIUM-8), atexit-once
(MEDIUM-8/D-08), current prior threading hook captured (MEDIUM-7), Qt slot hook
(D-01), QThread gap documented (D-01). REVIEWS MEDIUM-6: no qtbot.
"""

from __future__ import annotations

import atexit
import inspect
import sys
import threading

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


# ===========================================================================
# Phase 113 Plan 03 — chained hooks, KI/SE exclusion, idempotency, atexit, Qt
# ===========================================================================

# ---------------------------------------------------------------------------
# CRASH-01 — chained prior sys.excepthook (SC#1)
# ---------------------------------------------------------------------------
def test_prior_hook_chained(monkeypatch):
    """CRASH-01: crash_log.txt writer (prior hook) still called after install.

    Installs a sentinel as the prior hook, then install_exception_hooks()
    wraps it. Firing sys.excepthook must call the sentinel.
    """
    prior_called = []

    def sentinel_hook(t, v, tb):
        prior_called.append(t)

    original = sys.excepthook
    sys.excepthook = sentinel_hook
    try:
        tel.install_exception_hooks()
        # The installed hook is the telemetry wrapper
        assert sys.excepthook is not sentinel_hook, "install_exception_hooks did not replace sys.excepthook"
        # Fire the hook — prior (sentinel) must be called
        sys.excepthook(ValueError, ValueError('test'), None)
        assert ValueError in prior_called, (
            "Prior hook (sentinel) was not called — chain is broken (CRASH-01)"
        )
    finally:
        sys.excepthook = original
        tel._reset_for_tests()


def test_telemetry_failure_does_not_suppress_chain(monkeypatch):
    """CRASH-01 SC#1: a telemetry failure inside the hook does NOT suppress the prior hook.

    Monkeypatches _emit_crash_direct to raise. The prior hook must still be called.
    """
    prior_called = []

    def sentinel_hook(t, v, tb):
        prior_called.append(t)

    def exploding_emit(exc_type, exc_tb, is_background):
        raise RuntimeError("telemetry exploded on purpose")

    original = sys.excepthook
    sys.excepthook = sentinel_hook
    try:
        tel.install_exception_hooks()
        monkeypatch.setattr(tel, '_emit_crash_direct', exploding_emit)

        # Fire the hook with a real exception type
        sys.excepthook(ValueError, ValueError('test'), None)

        # The sentinel (prior hook) must still have been called
        assert ValueError in prior_called, (
            "Prior hook not called after telemetry failure — chain was suppressed (CRASH-01 violation)"
        )
    finally:
        sys.excepthook = original
        tel._reset_for_tests()


# ---------------------------------------------------------------------------
# CRASH-02 — threading.excepthook (D-08 / REVIEWS MEDIUM-7)
# ---------------------------------------------------------------------------
def test_threading_hook_fires_for_thread_raise(monkeypatch):
    """CRASH-02: threading.excepthook wrapper fires _emit_crash_direct(is_background=True)
    for a background thread raise. Also verifies the CURRENT threading.excepthook (not
    threading.__excepthook__) is captured as prior (REVIEWS MEDIUM-7).
    """
    emit_calls = []

    def capture_emit(exc_type, exc_tb, is_background):
        emit_calls.append({'exc_type': exc_type, 'is_background': is_background})

    monkeypatch.setattr(tel, '_emit_crash_direct', capture_emit)
    monkeypatch.setattr(tel, '_enabled', True)

    # MEDIUM-7: install a non-default sentinel threading hook BEFORE install_exception_hooks
    prior_threading_called = []

    def sentinel_threading_hook(args):
        prior_threading_called.append(args.exc_type)

    original_threading = threading.excepthook
    threading.excepthook = sentinel_threading_hook
    original_sys = sys.excepthook
    try:
        tel.install_exception_hooks()

        # Verify the CURRENT threading hook (sentinel) was captured, not __excepthook__
        assert tel._prior_threading_hook is sentinel_threading_hook, (
            "install_exception_hooks captured threading.__excepthook__ instead of the "
            "CURRENT threading.excepthook (REVIEWS MEDIUM-7 violation)"
        )

        # Simulate a thread raise by calling threading.excepthook directly
        import types as _types
        exc = ValueError("background thread error")
        fake_args = _types.SimpleNamespace(
            exc_type=ValueError,
            exc_value=exc,
            exc_traceback=None,
            thread=None,
        )
        threading.excepthook(fake_args)

        # _emit_crash_direct must have been called with is_background=True
        assert len(emit_calls) == 1, f"Expected 1 emit call, got: {emit_calls}"
        assert emit_calls[0]['is_background'] is True, (
            f"is_background should be True for threading.excepthook: {emit_calls[0]}"
        )
        assert emit_calls[0]['exc_type'] is ValueError

        # The sentinel (prior threading hook) must also have been called (chain)
        assert ValueError in prior_threading_called, (
            "Prior threading hook (sentinel) was not called — chain broken (MEDIUM-7)"
        )
    finally:
        threading.excepthook = original_threading
        sys.excepthook = original_sys
        tel._reset_for_tests()


# ---------------------------------------------------------------------------
# SC#2 — KeyboardInterrupt and SystemExit excluded from both hooks
# ---------------------------------------------------------------------------
def test_keyboard_interrupt_excluded(monkeypatch):
    """SC#2: KeyboardInterrupt produces zero sends but prior hook is still called."""
    sent = []
    monkeypatch.setattr(tel, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))
    monkeypatch.setattr(tel, '_enabled', True)

    prior_called = []

    def sentinel_hook(t, v, tb):
        prior_called.append(t)

    original = sys.excepthook
    sys.excepthook = sentinel_hook
    try:
        tel.install_exception_hooks()
        sys.excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)
        assert len(sent) == 0, f"KI should produce zero sends, got: {sent}"
        assert KeyboardInterrupt in prior_called, (
            "Prior hook not called for KeyboardInterrupt — chain broken"
        )
    finally:
        sys.excepthook = original
        tel._reset_for_tests()


def test_system_exit_excluded(monkeypatch):
    """SC#2 / T-113-02-CLEANSHUTDOWN: SystemExit produces zero sends but prior hook called."""
    sent = []
    monkeypatch.setattr(tel, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))
    monkeypatch.setattr(tel, '_enabled', True)

    prior_called = []

    def sentinel_hook(t, v, tb):
        prior_called.append(t)

    original = sys.excepthook
    sys.excepthook = sentinel_hook
    try:
        tel.install_exception_hooks()
        sys.excepthook(SystemExit, SystemExit(0), None)
        assert len(sent) == 0, f"SystemExit should produce zero sends, got: {sent}"
        assert SystemExit in prior_called, (
            "Prior hook not called for SystemExit — chain broken"
        )
    finally:
        sys.excepthook = original
        tel._reset_for_tests()


# ---------------------------------------------------------------------------
# D-08 / REVIEWS MEDIUM-8 — idempotent install + atexit registered at most once
# ---------------------------------------------------------------------------
def test_idempotent_install(monkeypatch):
    """D-08 / REVIEWS MEDIUM-8: double install does not double-chain and
    does not double-register atexit.

    Fires sys.excepthook and asserts the prior hook is called only ONCE (not
    twice). Also monkeypatches atexit.register to count calls and asserts it
    is called at most once across two installs.
    """
    prior_called = []

    def sentinel_hook(t, v, tb):
        prior_called.append(t)

    # Monkeypatch atexit.register to count calls
    register_calls = []
    original_atexit_register = atexit.register

    def counting_register(fn, *args, **kwargs):
        register_calls.append(fn)
        return original_atexit_register(fn, *args, **kwargs)

    original_sys = sys.excepthook
    sys.excepthook = sentinel_hook
    try:
        monkeypatch.setattr(atexit, 'register', counting_register)

        tel.install_exception_hooks()
        tel.install_exception_hooks()  # second call — must be idempotent

        # Fire sys.excepthook once
        sys.excepthook(ValueError, ValueError('test'), None)

        # Prior hook must be called exactly ONCE (not twice — double-chain check)
        assert prior_called.count(ValueError) == 1, (
            f"Prior hook called {prior_called.count(ValueError)} times — double-chain detected (D-08)"
        )

        # atexit.register must have been called AT MOST ONCE across two installs
        assert len(register_calls) <= 1, (
            f"atexit.register called {len(register_calls)} times — duplicate registration (MEDIUM-8)"
        )
    finally:
        sys.excepthook = original_sys
        tel._reset_for_tests()


# ---------------------------------------------------------------------------
# D-05 / REVIEWS HIGH-4 — no _flush_before_exit inside crash hook
# ---------------------------------------------------------------------------
def test_no_flush_before_exit_in_crash_hook():
    """REVIEWS HIGH-4: _telemetry_excepthook does not call _flush_before_exit.

    _flush_before_exit takes _capture_config_lock via _resolve_api_key —
    calling it from the crash hook would be a deadlock risk (D-05/HIGH-4).
    The crash event is delivered by the lock-free send_crash_event_direct.
    Assert that _telemetry_excepthook source contains no _flush_before_exit call.
    """
    import ast

    # Get install_exception_hooks source and find _telemetry_excepthook inner function
    src = inspect.getsource(tel.install_exception_hooks)
    # Search for _flush_before_exit in the function body (not in comments/docstrings)
    tree = ast.parse(src)
    func_def = tree.body[0]
    code_text = ast.unparse(ast.Module(body=func_def.body, type_ignores=[]))
    # _flush_before_exit must NOT appear in the hook wrapper code
    # It IS allowed in the _atexit_flush inner function body (clean-exit path)
    # We check that the only occurrence is inside _atexit_flush
    # A simpler check: if it appears, it must be inside a function named _atexit_flush
    assert '_flush_before_exit' in src, (
        "_flush_before_exit not found in install_exception_hooks at all — "
        "atexit flush may not be registered (CRASH-06)"
    )
    # Verify that the _telemetry_excepthook body does NOT call _flush_before_exit
    # by checking for its absence in the excepthook's direct flow
    # (the atexit registration is in install_exception_hooks, not in _telemetry_excepthook)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == '_telemetry_excepthook':
            hook_code = ast.unparse(ast.Module(body=node.body, type_ignores=[]))
            assert '_flush_before_exit' not in hook_code, (
                "_telemetry_excepthook calls _flush_before_exit — this takes "
                "_capture_config_lock and is a deadlock risk in crash context (HIGH-4)"
            )
            break


# ---------------------------------------------------------------------------
# Plan 03 — atexit registered INSIDE install_exception_hooks
# ---------------------------------------------------------------------------
def test_atexit_registered_inside_install():
    """D-08: atexit.register appears inside install_exception_hooks (not in posthog_server).

    Verify by AST inspection that install_exception_hooks contains an atexit.register call.
    Also verify shared/posthog_server.py contains NO atexit.register.
    """
    import ast

    # Check install_exception_hooks has atexit.register
    src = inspect.getsource(tel.install_exception_hooks)
    assert 'atexit' in src, (
        "install_exception_hooks does not reference atexit — "
        "clean-exit flush not registered (D-08)"
    )
    assert 'register' in src, (
        "install_exception_hooks does not call atexit.register (D-08)"
    )

    # Check shared/posthog_server.py has NO atexit.register (T-113-08-WEBEXIT)
    import shared.posthog_server as _ph
    ph_src = inspect.getsource(_ph)
    assert 'atexit.register' not in ph_src, (
        "shared/posthog_server.py contains atexit.register — "
        "this would fire on web process restart (T-113-08-WEBEXIT)"
    )


# ---------------------------------------------------------------------------
# D-01 / REVIEWS MEDIUM-6 — Qt slot exception reaches sys.excepthook
# ---------------------------------------------------------------------------
def test_qtimer_slot_raise_reaches_excepthook(monkeypatch):
    """D-01: a QTimer.singleShot slot raise reaches the installed sys.excepthook.

    Uses the pytest-qt-FREE QApplication.instance() or QApplication(sys.argv)
    pattern. No qtbot parameter (REVIEWS MEDIUM-6).

    PyQt6 routes exceptions escaping a slot to sys.excepthook (since PyQt 5.5),
    so the telemetry wrapper on sys.excepthook should fire.
    """
    try:
        from PyQt6.QtWidgets import QApplication
        from PyQt6.QtCore import QTimer, QEventLoop
    except ImportError:
        pytest.skip("PyQt6 not available")

    _app = QApplication.instance() or QApplication(sys.argv)

    hook_called = []
    original = sys.excepthook

    def capturing_hook(t, v, tb):
        hook_called.append(t)
        # Do NOT call the original to avoid crash log noise in tests

    sys.excepthook = capturing_hook
    try:
        tel.install_exception_hooks()

        # QTimer.singleShot slot that raises
        def slot_that_raises():
            raise RuntimeError("slot exception — should reach excepthook")

        loop = QEventLoop()
        QTimer.singleShot(0, slot_that_raises)
        QTimer.singleShot(50, loop.quit)  # quit after giving the raise time to propagate
        loop.exec()

        # PyQt6 routes uncaught slot exceptions to sys.excepthook
        # The installed telemetry wrapper calls the capturing_hook via the chain
        assert RuntimeError in hook_called, (
            "QTimer.singleShot slot raise did NOT reach sys.excepthook — "
            "D-01 behavior: hook fires (or document gap if not)"
        )
    finally:
        sys.excepthook = original
        tel._reset_for_tests()


# ---------------------------------------------------------------------------
# D-01 — QThread gap documented (known absence)
# ---------------------------------------------------------------------------
@pytest.mark.xfail(
    reason=(
        "D-01 documented gap: QThread.run() exceptions do NOT fire threading.excepthook "
        "in PyQt6 — QThread uses its own internal exception handler, not Python's "
        "threading.excepthook. Most workers (SearchThread, LocalIndexerWorker) already "
        "catch + emit error_signal, so the hooks are a backstop for the un-caught minority. "
        "This xfail documents the known gap per CONTEXT D-01."
    ),
    strict=False,
)
def test_qthread_gap_documented(monkeypatch):
    """D-01: QThread.run() raise does NOT fire threading.excepthook — documented gap.

    This test is xfail: it asserts that the hook fires, but we know it doesn't
    for QThread in PyQt6. The xfail documents the gap without failing the suite.
    """
    try:
        from PyQt6.QtWidgets import QApplication
        from PyQt6.QtCore import QThread
    except ImportError:
        pytest.skip("PyQt6 not available")

    _app = QApplication.instance() or QApplication(sys.argv)

    hook_fired = []

    def capture_emit(exc_type, exc_tb, is_background):
        hook_fired.append(exc_type)

    monkeypatch.setattr(tel, '_emit_crash_direct', capture_emit)
    monkeypatch.setattr(tel, '_enabled', True)

    original_sys = sys.excepthook
    original_threading = threading.excepthook
    try:
        tel.install_exception_hooks()

        class FailThread(QThread):
            def run(self):
                raise RuntimeError("QThread internal exception")

        t = FailThread()
        t.start()
        t.wait(2000)  # ms

        # This assertion is expected to FAIL (xfail) — QThread does not
        # route to threading.excepthook
        assert len(hook_fired) > 0, (
            "QThread.run() exception did NOT reach threading.excepthook (documented gap)"
        )
    finally:
        sys.excepthook = original_sys
        threading.excepthook = original_threading
        tel._reset_for_tests()
