# -*- coding: utf-8 -*-
"""What a search page left, kept on the server for Back -- one entry per search run.

Every search a page starts gets a run code (``run=`` in its address, written when the
search starts). Its entry here holds, keyed by the visitor's browser and that code:

* while the search runs: the research job itself, so a page reached by Back can wait
  for it (and stop it) instead of starting the same search again;
* once it has run: the page's snapshot (the rows, up to 1,000, and the state shown
  with them), so Back restores exactly what this run showed -- not another tab's
  search, and not the 250 rows the shared user storage keeps.

Why not NiceGUI's tab storage: a page reads its saved state while it is being built,
and ``app.storage.tab`` only exists once the browser's socket has connected -- after
the build -- so the per-tab copy was never readable when it mattered.

Kept in memory only: a restart loses it (Back then falls back to the user-storage copy
or searches again). Bounded by age, per browser and in total. Touched only from the
event loop, except ``SearchRun.watchers``/``cancelled``, read by the job's progress
callback on its waiting thread (a list copy and a bool).
"""

from __future__ import annotations

import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Optional

KEEP_SECONDS = 30 * 60   # a finished run is kept this long after it was last used
PER_BROWSER = 5          # runs kept per browser (its newest)
MAX_RUNS = 100           # runs kept in all


@dataclass
class SearchRun:
    browser_id: str
    stamp: str
    snapshot: dict                    # the page snapshot; complete once ``done``
    started: float = field(default_factory=time.time)        # wall clock, for the page's timer
    touched: float = field(default_factory=time.monotonic)
    done: bool = False
    job: Any = None                   # the research call's future while the search runs
    cutoff: Optional[dict] = None     # the run's cut-off signals, filled when the job ends
    refine: bool = False              # a search within: its page state is not rebuilt for a rejoin
    cancelled: bool = False           # Stop from any page following the run
    watchers: list = field(default_factory=list)   # search states shown this run's progress
    owner: Any = None                 # the search state that saves the run (the last to take it)

    @property
    def running(self) -> bool:
        """The search is still running: a page may wait for it."""
        return not self.done and self.job is not None and not self.job.done()

    @property
    def failed(self) -> bool:
        """It ended without a snapshot (an error, or a page that never finished it)."""
        return not self.done and (self.job is None or self.job.done())


_runs: 'OrderedDict[tuple[str, str], SearchRun]' = OrderedDict()


def new_stamp() -> str:
    """A run code for an address: random, so another visitor cannot name it."""
    return secrets.token_urlsafe(6)


def start_run(browser_id: str, stamp: str, snapshot: dict, *, refine: bool = False) -> SearchRun:
    """Register a run as it starts; *snapshot* holds what is known then (its search)."""
    run = SearchRun(browser_id=browser_id, stamp=stamp, snapshot=dict(snapshot), refine=refine)
    _runs[(browser_id, stamp)] = run
    _runs.move_to_end((browser_id, stamp))
    _prune()
    return run


def get_run(browser_id: Optional[str], stamp: Optional[str]) -> Optional[SearchRun]:
    """This browser's run with that code, if it is still kept."""
    if not browser_id or not stamp:
        return None
    _prune()
    run = _runs.get((browser_id, stamp))
    if run is not None:
        run.touched = time.monotonic()
    return run


def finish_run(browser_id: Optional[str], stamp: Optional[str], snapshot: dict) -> Optional[SearchRun]:
    """Save the page's snapshot for its run and mark it done. Every later save of the
    same page (a filter change) calls this again: the entry is what the page shows."""
    if not browser_id or not stamp:
        return None
    run = _runs.get((browser_id, stamp))
    if run is None:
        run = SearchRun(browser_id=browser_id, stamp=stamp, snapshot={})
        _runs[(browser_id, stamp)] = run
    run.snapshot = dict(snapshot)
    run.done = True
    run.touched = time.monotonic()
    _runs.move_to_end((browser_id, stamp))
    _prune()
    return run


def update_run(browser_id: Optional[str], stamp: Optional[str], **fields) -> bool:
    """Merge *fields* into a finished run's snapshot (what the page shows changed)."""
    run = _runs.get((browser_id, stamp)) if browser_id and stamp else None
    if run is None or not run.done:
        return False
    run.snapshot = {**run.snapshot, **fields}
    run.touched = time.monotonic()
    return True


def _prune(now: Optional[float] = None) -> None:
    now = time.monotonic() if now is None else now
    for key, run in list(_runs.items()):
        if not run.running and now - run.touched > KEEP_SECONDS:
            del _runs[key]
    per_browser: dict[str, list] = {}
    for key, run in _runs.items():
        per_browser.setdefault(run.browser_id, []).append(key)
    for keys in per_browser.values():
        for key in _oldest_first_removable(keys)[:max(0, len(keys) - PER_BROWSER)]:
            _runs.pop(key, None)
    if len(_runs) > MAX_RUNS:
        for key in _oldest_first_removable(list(_runs))[:len(_runs) - MAX_RUNS]:
            _runs.pop(key, None)


def _oldest_first_removable(keys: list) -> list:
    """Keys oldest first, a running search last: it is dropped only when nothing else is."""
    return sorted(keys, key=lambda k: (_runs[k].running, _runs[k].touched))


def clear() -> None:
    """Forget every run (tests)."""
    _runs.clear()
