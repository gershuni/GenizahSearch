# -*- coding: utf-8 -*-
"""The line at the top of the main window while the search index is rebuilt.

The rebuild's progress bar used to live only in the Settings dialog, so after "Rebuild
now" the main window showed nothing for twenty minutes. This bar sits under the update
bars (``GenizahGUI.init_ui``) and says, in one line, how far the build is, about how long
is left, and whether searching works meanwhile: it does when the build runs beside the
live index (``shared/index_swap.py``), and it does not when the build had to run in
place. Hidden when no build runs.
"""
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QFrame, QHBoxLayout, QLabel, QProgressBar

from genizah_core import tr


def remaining_phrase(minutes):
    """The time part of the line: *minutes* is :meth:`BuildClock.minutes_left`."""
    if minutes is None:
        return tr("estimating the time left")
    if minutes == 0:
        return tr("less than a minute left")
    if minutes == 1:
        return tr("about a minute left")
    return tr("about {} minutes left").format(minutes)


def searching_phrase(mode):
    """What searching does during the build: ``mode`` is ``'beside'`` or anything else
    (an in-place build, which closed the index first)."""
    if mode == 'beside':
        return tr("Searching uses the current index meanwhile.")
    return tr("Searching is unavailable until the build ends.")


def progress_line(percent, minutes, mode, phase=None):
    """The whole line. *phase* ``'commit'`` is the one stretch without ticks (the main
    index's commit and merges), so the line says what the build is doing then."""
    if phase == 'commit':
        head = tr("Rebuilding the search index: {}%, writing it to disk").format(percent)
    else:
        head = tr("Rebuilding the search index: {}%").format(percent)
    return "{}, {}. {}".format(head, remaining_phrase(minutes), searching_phrase(mode))


class IndexRebuildBar(QFrame):
    """A narrow bar: the sentence and a small progress bar. ``begin(mode)`` shows it,
    ``set_progress(percent, minutes)`` updates it, ``finish()`` hides it."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.mode = None
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet("background-color: #fff3cd; color: #664d03; border-bottom: 1px solid #ffe69c;")
        self.setFixedHeight(36)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 0, 10, 0)
        self.lbl_msg = QLabel()
        self.lbl_msg.setStyleSheet("font-size: 13px; border: none; background: transparent;")
        self.lbl_msg.setTextInteractionFlags(Qt.TextInteractionFlag.NoTextInteraction)
        self.bar = QProgressBar()
        self.bar.setRange(0, 100)
        self.bar.setValue(0)
        self.bar.setTextVisible(False)
        self.bar.setFixedWidth(160)
        self.bar.setFixedHeight(14)
        layout.addWidget(self.lbl_msg, 1)
        layout.addWidget(self.bar)
        self.setVisible(False)

    def begin(self, mode):
        self.mode = mode
        self.bar.setValue(0)
        self.lbl_msg.setText(progress_line(0, None, mode))
        self.setVisible(True)

    def set_progress(self, percent, minutes, phase=None):
        percent = max(0, min(100, int(percent)))
        self.bar.setValue(percent)
        self.lbl_msg.setText(progress_line(percent, minutes, self.mode, phase))

    def set_message(self, text):
        """A sentence in place of the progress line (waiting for a search to end)."""
        self.lbl_msg.setText(text)

    def finish(self):
        self.mode = None
        self.setVisible(False)
