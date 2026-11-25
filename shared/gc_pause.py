# -*- coding: utf-8 -*-
"""Pause the cyclic garbage collector while many long-lived rows are made at once.

Building or loading tens of thousands of result rows sets the collector off again and again, and
each full collection walks every object of the process -- the rows so far, and on the website the
whole catalogue -- to find nothing: the rows hold no cycles. It was half the time of making one
list of two legs' rows (shared/editions_search.py::group_results) and of loading a worker's rows
(web/research_jobs.py). Only for short stretches that make no cycles; nested and concurrent
pauses count, and the last one out turns the collector on again if it was on.
"""
from __future__ import annotations

import gc
import threading
from contextlib import contextmanager

_lock = threading.Lock()
_paused = 0
_was_enabled = False


@contextmanager
def gc_paused():
    global _paused, _was_enabled
    with _lock:
        if _paused == 0:
            _was_enabled = gc.isenabled()
            gc.disable()
        _paused += 1
    try:
        yield
    finally:
        with _lock:
            _paused -= 1
            if _paused == 0 and _was_enabled:
                gc.enable()
