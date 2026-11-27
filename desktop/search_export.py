"""The Search tab's Export over the whole result list (GS-9, part 2).

Part 1 (desktop/result_view.py) made Export write every result the filters keep, not the
500 loaded; 28,000 results then froze the window for a minute (each row's page text is read
from the index). Here:

- ``ExportJob``: everything the file needs, taken on the window's thread before the build
  starts (Codex F2 on the plan), so a new search or New during the build cannot mix into it.
- ``ExportThread``: builds the file's bytes off the window's thread above
  ``EXPORT_IN_BACKGROUND`` rows (owner ruling D5), with progress and Cancel. The save, its
  locked-file question and the result message stay on the window's thread; the window lists
  the thread as an index reader while it runs (Codex F1).
- ``export_order``: the order of a whole-list export while the table is sorted by a column
  (owner ruling D4): the table's own sort, over every result.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from PyQt6.QtCore import QThread, pyqtSignal

EXPORT_IN_BACKGROUND = 2000      # rows: above this the build runs on a worker (D5)
PROGRESS_EVERY = 50              # rows between two progress reports


class ExportCancelled(Exception):
    """The user pressed Cancel while the file was being built."""


@dataclass
class ExportJob:
    """A Search export, read from the window at one moment."""
    fmt: str
    results: list
    selected: bool                          # the ticked rows, not the whole list
    meta: dict = field(default_factory=dict)          # sys_id -> (shelfmark, title, library code)
    lang: str = 'en'
    credit_text: str = ''
    search_query: str = ''
    search_mode: str = ''
    search_gap: str = ''
    lab_mode_on: bool = False
    deep_scan_on: bool = False
    transcriptions_label: str = ''          # the transcriptions the search read, or ''
    local_filepaths: dict = field(default_factory=dict)
    transcription_ids: set = field(default_factory=set)
    printed_ids: set = field(default_factory=set)
    result_domains: dict = field(default_factory=dict)
    count_capped: bool = False
    page_text: object = None                # searcher.get_full_text_by_id, or None

    def tracker(self, progress=None, cancelled=None):
        """A per-row callback for the builder: reports progress every PROGRESS_EVERY rows
        and raises ExportCancelled once Cancel is pressed."""
        total, seen = len(self.results), [0]

        def step():
            seen[0] += 1
            if seen[0] % PROGRESS_EVERY == 0 or seen[0] == total:
                if cancelled is not None and cancelled():
                    raise ExportCancelled()
                if progress is not None:
                    progress(min(seen[0], total), total)

        def check():                    # Cancel, without counting a row
            if cancelled is not None and cancelled():
                raise ExportCancelled()
        step.check = check
        return step


class ExportThread(QThread):
    """``build(job, progress, cancelled)`` -> bytes, off the window's thread."""
    progress_signal = pyqtSignal(int, int)
    done_signal = pyqtSignal(object)
    error_signal = pyqtSignal(str)
    cancelled_signal = pyqtSignal()

    def __init__(self, build, job, parent=None):
        super().__init__(parent)
        self.build, self.job, self._cancel = build, job, False

    def request_cancel(self):
        self._cancel = True

    def run(self):
        try:
            payload = self.build(self.job, self.progress_signal.emit, lambda: self._cancel)
        except ExportCancelled:
            self.cancelled_signal.emit()
            return
        except Exception as e:                     # the window shows it as "Failed to save ..."
            self.error_signal.emit(str(e))
            return
        if self._cancel:                            # pressed after the last row, while saving
            self.cancelled_signal.emit()
            return
        self.done_signal.emit(payload)


def export_order(results, key, descending):
    """*results* in the order the table shows them when sorted by a column whose sort key is
    ``key(result)`` (D4). Stable, as Qt's sort is: results with the same key keep their
    order, in either direction."""
    return sorted(results, key=key, reverse=descending)
