# -*- coding: utf-8 -*-
"""Phase 113 Plan 01 — Wave 0 scaffold for CRASH-01/02/05 hook tests.

Tests in this file are STUBS that will be filled by Plan 02.
Collection succeeds and all stubs skip, giving Plan 02 clear verify targets.

No `qtbot` parameter is used anywhere in this file (repo is pytest-qt-FREE;
REVIEWS MEDIUM-6).
"""

import sys

import pytest

import desktop.telemetry as tel
import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Module-level autouse wrapper — opt-in to crash_telemetry_state fixture.
# Scoped to this file only (never project-wide).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _use(crash_telemetry_state):
    yield


# ---------------------------------------------------------------------------
# CRASH-01 — crash_log.txt writer still called after install_exception_hooks()
# ---------------------------------------------------------------------------
def test_prior_hook_chained():
    """CRASH-01: crash_log.txt writer (prior_hook) is still called after install."""
    pytest.skip("filled by plan 02")


def test_telemetry_failure_does_not_suppress_chain():
    """CRASH-01: a telemetry failure in try block does NOT suppress the prior hook."""
    pytest.skip("filled by plan 02")


# ---------------------------------------------------------------------------
# CRASH-02 — threading.excepthook + QTimer/QThread matrix
# ---------------------------------------------------------------------------
def test_threading_hook_fires_for_thread_raise():
    """CRASH-02: threading.Thread raise → threading.excepthook wrapper fires."""
    pytest.skip("filled by plan 02")


def test_qtimer_slot_raise_reaches_excepthook():
    """CRASH-02 D-01: QTimer.singleShot slot raise → sys.excepthook fires.

    NOTE: this test constructs a QApplication (pytest-qt-FREE pattern from
    test_join_workbench_construct.py:15) — if it causes headless CI races,
    add 'test_crash_hooks.py' to the collect_ignore_glob block in conftest.py.
    """
    pytest.skip("filled by plan 02")


def test_qthread_gap_documented():
    """CRASH-02: QThread.run() raise does NOT fire threading.excepthook (documented gap)."""
    pytest.skip("filled by plan 02")


# ---------------------------------------------------------------------------
# CRASH-02 — exclusions
# ---------------------------------------------------------------------------
def test_keyboard_interrupt_excluded():
    """CRASH-02: KeyboardInterrupt is excluded from both hooks."""
    pytest.skip("filled by plan 02")


def test_system_exit_excluded():
    """CRASH-02: SystemExit is excluded from both hooks."""
    pytest.skip("filled by plan 02")


# ---------------------------------------------------------------------------
# CRASH-05 — lock-free hook body + safety invariants
# ---------------------------------------------------------------------------
def test_hook_acquires_no_locks():
    """CRASH-05 / D-05 BLOCKER: crash hook acquires no locks."""
    pytest.skip("filled by plan 02")


def test_recursion_guard():
    """CRASH-05: crash inside crash handler does not recurse."""
    pytest.skip("filled by plan 02")


def test_idempotent_install():
    """CRASH-05: double install_exception_hooks() does not double-chain."""
    pytest.skip("filled by plan 02")


def test_reset_for_tests_restores_hooks():
    """test seam: _reset_for_tests() restores sys.excepthook to pre-install state."""
    pytest.skip("filled by plan 02")
