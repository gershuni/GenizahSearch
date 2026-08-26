# -*- coding: utf-8 -*-
"""Phase 146: the thin Qt shells around `desktop/passage_lifecycle.py`.

The lifecycle module is deliberately Qt-free -- its whole test suite runs
without a QApplication, which is what makes the build/swap/close state
machine provable at all. So the QThread wrappers live here instead of there,
and they stay THIN: no policy, no state, no decisions. Everything they do is
call one lifecycle function and forward what it returned.
"""
from __future__ import annotations

import logging
import os

from PyQt6.QtCore import QThread, pyqtSignal

from desktop import passage_lifecycle
from shared.passage_corpus import iter_records

logger = logging.getLogger(__name__)

# See `docs/specs/passage-index-build-measurements.md`: peak RSS 3.5 GB at
# P=8; the doc's own instruction for a memory-constrained machine is to
# double the partition count.
DESKTOP_PARTITIONS = 16


class PassageLoadThread(QThread):
    """Startup: recover if a previous run died mid-swap, then open the live
    index. `recover_at_startup` does BOTH -- it walks the candidates,
    promotes whichever one actually opens, and returns that opened index --
    so this is the single call that makes the feature reachable at all.

    Off the UI thread because opening scans the full CSR (~109.5 MB on the
    shipped corpus). It returns the index rather than installing it: `_state`
    is UI-thread-only, so assignment happens in the slot.
    """

    loaded_signal = pyqtSignal(object)   # RecoveryResult, or None on failure

    def __init__(self, root, parent=None):
        super().__init__(parent)
        self._root = root

    def run(self):
        try:
            result = passage_lifecycle.recover_at_startup(self._root)
        except Exception:                                  # noqa: BLE001
            # A startup path must never take the app down with it: the whole
            # feature hiding is an acceptable outcome, a failed launch is not.
            logger.exception('passage index recovery failed at startup')
            self.loaded_signal.emit(None)
            return
        self.loaded_signal.emit(result)


class PassageBuildThread(QThread):
    """Builds a passage index into staging and swaps it over the live one.

    `release_live_state` is supplied by the window, not by this thread: the
    release must happen on the UI thread (see `close_passage_state`'s
    ownership rule), so this thread can only ask for it and wait.
    """

    # phase, done, total, records. `records` is meaningful only for the
    # read phase, where the fraction is over BYTES and the user still
    # wants to see the count climbing.
    progress_signal = pyqtSignal(str, int, int, int)
    finished_signal = pyqtSignal(object)          # BuildAndSwapResult
    cancelled_signal = pyqtSignal()
    error_signal = pyqtSignal(str)

    def __init__(self, root, corpus_path, release_live_state, parent=None):
        super().__init__(parent)
        self._root = root
        self._corpus_path = corpus_path
        self._release_live_state = release_live_state
        self._cancelled = False

    def request_cancel(self):
        """Sets the latch this thread's `cancel_check` reads. There is no
        forced stop: every cancel point in the builder is cooperative, and
        that is the point -- a terminated build would leave a partial
        multi-GB artifact and, on Windows, open mappings on it."""
        self._cancelled = True

    def _cancel_check(self):
        return self._cancelled

    def run(self):
        try:
            # Reading the corpus is itself minutes of I/O on a 1.5 GB file,
            # so it belongs here and not on the UI thread. `iter_records` is
            # a generator; the builder consumes it in one pass.
            records = self._records_with_progress(self._corpus_path)
            result = passage_lifecycle.run_build_and_swap(
                self._root, records, [self._corpus_path], self._corpus_path,
                # 16, not the shared default of 8: measured peak RSS at
                # P=8 is 3.5 GB, and halving the partition size halves it.
                # A desktop is not the dev box the default was tuned on.
                partitions=DESKTOP_PARTITIONS,
                progress=self._on_progress,
                cancel_check=self._cancel_check,
                release_live_state=self._release_live_state)
        except passage_lifecycle.BuildCancelled:
            self.cancelled_signal.emit()
            return
        except Exception as exc:                       # noqa: BLE001
            # The raw text is index internals. It goes to the log; the window
            # shows a translated generic.
            logger.exception('passage index build failed')
            self.error_signal.emit(str(exc))
            return
        if result.status == 'cancelled':
            self.cancelled_signal.emit()
            return
        if result.status == 'error':
            # `run_build_and_swap` RETURNS a failed build rather than raising
            # it, so the `except` above never sees this one. Emitted as a
            # normal completion it dropped the diagnostic and told the user
            # the previous index was still in use -- which on a first build
            # names an index that never existed. The slot logs the text and
            # shows the translated generic.
            self.error_signal.emit(str(getattr(result, 'error', '') or ''))
            return
        self.finished_signal.emit(result)

    def _records_with_progress(self, path):
        """Yield every record, reporting how far through the corpus file the
        read has got.

        Pass 1 has no record total -- nothing knows how many records a file
        holds until it is read -- but the file SIZE is known, and pass 1 is
        driven by consuming it. Characters are counted against the same
        `size // 2` upper bound the disk preflight uses (Hebrew letters are
        2 UTF-8 bytes; normalization only removes), which makes this an
        estimate rather than a measurement, hence the clamp.

        Throttled to whole percents: emitting per record would put hundreds
        of thousands of queued signals on the UI thread and make the dialog
        slower than the build.
        """
        try:
            approx_total = max(1, os.path.getsize(path) // 2)
        except OSError:
            approx_total = 0
        seen = 0
        count = 0
        last_pct = -1
        for rid, text in iter_records(path):
            count += 1
            if approx_total:
                seen += len(rid) + len(text)
                pct = min(100, int(seen * 100 / approx_total))
                if pct != last_pct:
                    last_pct = pct
                    self._emit('read', pct, 100, count)
            yield rid, text
        if approx_total:
            self._emit('read', 100, 100, count)

    def _emit(self, phase, done, total, records=0):
        try:
            self.progress_signal.emit(str(phase), int(done or 0),
                                      int(total or 0), int(records or 0))
        except Exception:                              # noqa: BLE001
            pass       # a progress line must never take a build down

    def _on_progress(self, phase, done=0, total=0, elapsed=0.0):
        # The builder calls this as progress(phase, done, total, elapsed).
        # Pass 1 has no total (it reports records seen/indexed against an
        # unknown length), so the dialog shows it as indeterminate; pass 2
        # reports partition i of n and can show a real bar.
        # `total` here is the builder's own second argument, which for
        # pass 1 is n_records_INDEXED -- not a total. The read fraction comes
        # from `_records_with_progress` instead; this call still carries
        # pass 2, where `total` really is the partition count.
        if phase == 'pass1':
            return
        self._emit(phase, done, total)
