# -*- coding: utf-8 -*-
"""A removable filter chip: one active filter as a small rounded button.

Clicking it removes that filter. Shared by the search's filter dialog and the
Joins Lab's row of active filters, so a filter looks the same in both.
"""

from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QCursor
from PyQt6.QtWidgets import QPushButton


def make_filter_chip(text: str, bg_color: str, on_click) -> QPushButton:
    """A removable chip button reading "*text*  ×" (dark-mode aware)."""
    chip = QPushButton(f"{text}  ×")
    chip.setFlat(True)
    chip.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
    # Detect dark mode via palette brightness
    palette = chip.palette()
    is_dark = palette.color(palette.ColorRole.Window).lightness() < 128
    if is_dark:
        # Darken the background for dark mode, use light text
        c = QColor(bg_color)
        dark_bg = c.darker(250).name()
        hover_bg = c.darker(180).name()
        chip.setStyleSheet(
            f"QPushButton {{ background: {dark_bg}; color: #e0e0e0; border: 1px solid #555; "
            "border-radius: 10px; padding: 2px 8px; font-size: 11px; } "
            f"QPushButton:hover {{ background: {hover_bg}; }}"
        )
    else:
        chip.setStyleSheet(
            f"QPushButton {{ background: {bg_color}; border: 1px solid #ccc; "
            "border-radius: 10px; padding: 2px 8px; font-size: 11px; } "
            "QPushButton:hover { background: #ddd; }"
        )
    chip.clicked.connect(on_click)
    return chip
