"""Captures web.api_hardening events in tests without racing the real drain thread."""
import queue
import time


class CapturedEvents:
    """Stands in for web.api_hardening._event_queue. An earlier test in the same
    process may have started the real PostHog drain thread, which re-reads that
    module global after each 60 s wait: given a real queue it took the events
    (66 of 66 in 66 s, 2026-10-11) and the MCP event tests failed by timing. Here
    it waits and finds nothing."""

    def __init__(self):
        self.items = []

    def put_nowait(self, event):
        self.items.append(event)

    def get(self, block=True, timeout=None):
        time.sleep(timeout or 0)
        raise queue.Empty

    def drain(self):
        found, self.items[:] = list(self.items), []
        return found
