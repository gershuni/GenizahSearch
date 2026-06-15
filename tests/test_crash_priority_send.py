# -*- coding: utf-8 -*-
"""Phase 113 Plan 01 — CRASH-06 direct-send-bypasses-queue + lock-free assertions.

Tests in this file are FULLY implemented (Task 2).  The stubs below are
replaced by live assertions when Task 2 completes in this same plan.

No `qtbot` parameter is used anywhere in this file (repo is pytest-qt-FREE;
REVIEWS MEDIUM-6).
"""

import queue

import pytest

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Module-level autouse wrapper — opt-in to crash_telemetry_state fixture.
# Scoped to this file only (never project-wide).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _use(crash_telemetry_state):
    yield


# ---------------------------------------------------------------------------
# CRASH-06 — send_crash_event_direct bypasses the FIFO queue
# ---------------------------------------------------------------------------
def test_crash_send_bypasses_full_queue(monkeypatch):
    """CRASH-06 D-06: send_crash_event_direct POSTs even when _event_queue is full."""
    pytest.skip("filled by task 2 in this plan")


def test_direct_send_does_not_touch_queue(monkeypatch):
    """CRASH-06: send_crash_event_direct never puts to / gets from _event_queue."""
    pytest.skip("filled by task 2 in this plan")


# ---------------------------------------------------------------------------
# D-05 REVIEWS HIGH-1 — lock-free snapshot globals
# ---------------------------------------------------------------------------
def test_snapshot_globals_populated_by_setters():
    """D-05: set_capture_api_key/set_capture_host write _crash_*_snapshot globals."""
    pytest.skip("filled by task 2 in this plan")


def test_direct_send_no_key_no_post(monkeypatch):
    """D-05: send_crash_event_direct with no snapshot key makes zero POSTs."""
    pytest.skip("filled by task 2 in this plan")


def test_direct_send_payload_shape(monkeypatch):
    """CRASH-06: the POSTed JSON has event, distinct_id, properties, and ISO timestamp."""
    pytest.skip("filled by task 2 in this plan")


def test_direct_send_never_raises_on_post_error(monkeypatch):
    """CRASH-06: send_crash_event_direct never raises even if requests.post raises."""
    pytest.skip("filled by task 2 in this plan")


def test_direct_send_lock_free_static(monkeypatch):
    """REVIEWS HIGH-1 static: send_crash_event_direct source contains no lock-taking symbols."""
    pytest.skip("filled by task 2 in this plan")
