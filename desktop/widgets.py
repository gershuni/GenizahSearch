"""Shared desktop UI widgets and helper functions."""

from PyQt6.QtWidgets import QWidget, QHBoxLayout
from PyQt6.QtCore import Qt

from genizah_core import tr


class ActionsHoverWidget(QWidget):
    def __init__(self, parent=None, alignment=Qt.AlignmentFlag.AlignCenter):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(2, 2, 2, 2)
        layout.setSpacing(4)
        layout.setAlignment(alignment)
        self.buttons = []
        self.always_visible_buttons = set()

    def add_btn(self, btn, always_visible=False):
        self.layout().addWidget(btn)
        self.buttons.append(btn)
        if always_visible:
            self.always_visible_buttons.add(btn)
            btn.setVisible(True)
        else:
            btn.setVisible(False)

    def set_buttons_visible(self, visible):
        for b in self.buttons:
            if b in self.always_visible_buttons:
                b.setVisible(True)
            else:
                b.setVisible(visible)


def _format_add_to_list_label(in_list=False):
    star = "\u2b50" if in_list else "\u2606"
    return f"{star} {tr('List')}"
