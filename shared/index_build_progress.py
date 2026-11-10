# -*- coding: utf-8 -*-
"""One fraction and one time estimate for a running index build.

``Indexer.create_index`` reports the corpus lines it has read (``progress_callback``) and
then its later phases (``phase_callback``: aggregates, commit, seams, finish). The user
is shown one bar and one "about N minutes left", so the phases are weighted into one
fraction here. The weights are shares of a full-corpus build's wall time (the owner's
laptop, 2026-10-23: about 22 minutes in all); they are an estimate's basis, not a
measurement of this computer, which is why the estimate is "about".

Qt-free: ``desktop/index_rebuild_bar.py`` renders what :class:`BuildClock` answers.
"""
from __future__ import annotations

import math
import time

#: Phase -> its share of the build. ``pages`` is the lines phase (progress_callback);
#: the others arrive through phase_callback, in this order.
PHASE_WEIGHTS = (
    ('pages', 0.55),
    ('aggregates', 0.08),
    ('commit', 0.17),
    ('seams', 0.15),
    ('finish', 0.05),
)
_PHASES = tuple(name for name, _ in PHASE_WEIGHTS)

#: Before this much has run, no estimate is given: the first ticks (opening the files,
#: the first thousand lines) say nothing about the rate.
MIN_ELAPSED_FOR_ESTIMATE_S = 20.0
MIN_FRACTION_FOR_ESTIMATE = 0.02


def overall_fraction(phase, done, total):
    """The build's fraction, 0..1, from a phase's own progress. A phase without ticks
    (``commit``) counts as begun, not advanced; an unknown phase changes nothing (None)."""
    if phase not in _PHASES:
        return None
    start = 0.0
    for name, weight in PHASE_WEIGHTS:
        if name == phase:
            within = min(1.0, max(0.0, done / total)) if total else 0.0
            return min(1.0, start + weight * within)
        start += weight
    return None


def remaining_seconds(elapsed_s, fraction):
    """Seconds left at the rate so far, or None while that rate means nothing yet
    (too early, or nothing done)."""
    if fraction is None or fraction < MIN_FRACTION_FOR_ESTIMATE or elapsed_s < MIN_ELAPSED_FOR_ESTIMATE_S:
        return None
    if fraction >= 1.0:
        return 0.0
    return elapsed_s * (1.0 - fraction) / fraction


def remaining_minutes(seconds):
    """Whole minutes for a sentence, rounded up; 0 means "less than a minute"."""
    if seconds is None:
        return None
    if seconds < 45:
        return 0
    return max(1, int(math.ceil(seconds / 60.0)))


class BuildClock:
    """The running build's fraction and estimate. ``update`` takes a phase tick (the
    lines phase is ``'pages'``); ``percent`` and ``minutes_left`` answer for the bar.
    The fraction never goes backwards: a phase that reports less than the last one
    (two phases' ticks interleaving) keeps the bar still rather than jumping back."""

    def __init__(self, now=None):
        self.started = time.monotonic() if now is None else now
        self.fraction = 0.0
        self.phase = 'pages'

    def update(self, phase, done, total, now=None):
        fraction = overall_fraction(phase, done, total)
        if fraction is None:
            return self.fraction
        if fraction >= self.fraction:
            self.fraction = fraction
            self.phase = phase
        return self.fraction

    def elapsed(self, now=None):
        return (time.monotonic() if now is None else now) - self.started

    def percent(self):
        return int(self.fraction * 100)

    def minutes_left(self, now=None):
        """Whole minutes left (0: under a minute), or None before an estimate exists."""
        return remaining_minutes(remaining_seconds(self.elapsed(now), self.fraction))
