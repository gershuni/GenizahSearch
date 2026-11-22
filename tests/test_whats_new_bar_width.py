# -*- coding: utf-8 -*-
"""The desktop What's New bar never holds the main window wide.

A QLabel that does not wrap asks for its whole line as its minimum width, and the bar,
which sits in the main window's layout, passes that on. 11.0.0's line measured about
1,000 px (English) and 940 px (Hebrew) as the bar's minimum, against about 720 and 550 for
10.1.0's, so a narrow window would have been widened while the bar showed. The label may be
cut short instead.
"""
from __future__ import annotations

import pytest
from PyQt6.QtWidgets import QApplication

from desktop import update_ui

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


def test_the_bar_does_not_ask_for_its_whole_line():
    bar = update_ui.WhatsNewBar()
    try:
        bar.show_whats_new(update_ui.WHATS_NEW_CONTENT_VERSION)
        line = bar.lbl_msg.sizeHint().width()
        assert line > 500, line                       # the line itself is long ...
        assert bar.minimumSizeHint().width() < 300, bar.minimumSizeHint().width()  # ... the bar is not
    finally:
        bar.deleteLater()


def test_a_wide_bar_still_shows_the_whole_line():
    bar = update_ui.WhatsNewBar()
    try:
        bar.show_whats_new(update_ui.WHATS_NEW_CONTENT_VERSION)
        bar.resize(1600, 40)
        bar.layout().activate()
        assert bar.lbl_msg.width() >= bar.lbl_msg.sizeHint().width()
    finally:
        bar.deleteLater()
