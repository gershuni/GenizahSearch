# -*- coding: utf-8 -*-
"""The website-removal prompt: entries removed from a list on genizahsearch.com.

A list sync that reads a list completely and finds an entry's row gone keeps the
entry on this computer and never uploads it again on its own; the user decides here,
entry by entry: remove it from this computer too, keep it (and add it back on the
website), or decide later. Closing the prompt decides nothing.

ask_about_web_removals(parent, entries) shows it and returns only the decided rows,
{(item_id, list_id): 'remove' | 'keep'}; entries are (item_id, list_id,
shelfmark_text, list_name). The window applies the answer
(ListsManager.resolve_web_removals) and asks for an upload.
"""
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

import genizah_core
from genizah_core import tr

LATER, REMOVE, KEEP = 'later', 'remove', 'keep'


class WebRemovalsDialog(QDialog):
    """One row per entry, each with its own choice; Decide later by default."""

    def __init__(self, parent, entries):
        super().__init__(parent)
        self._entries = list(entries)
        self._decided = {}
        self.setWindowTitle(tr("Entries removed on the website"))
        self.setMinimumWidth(620)
        if genizah_core.CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        layout = QVBoxLayout(self)

        text = QLabel(tr("These entries were removed from your lists on genizahsearch.com. They are "
                         "still on this computer, and until you choose they stay here and are not "
                         "uploaded again."))
        text.setWordWrap(True)
        layout.addWidget(text)

        self.table = QTableWidget(len(self._entries), 3, self)
        self.table.setHorizontalHeaderLabels([tr("Shelfmark"), tr("List"), tr("Choice")])
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.choice_boxes = []
        for row, (_item_id, _list_id, shelfmark, list_name) in enumerate(self._entries):
            self.table.setItem(row, 0, QTableWidgetItem(str(shelfmark)))
            self.table.setItem(row, 1, QTableWidgetItem(str(list_name)))
            box = QComboBox(self.table)
            box.addItem(tr("Decide later"), LATER)
            box.addItem(tr("Remove from this computer too"), REMOVE)
            box.addItem(tr("Keep it (and add it back on the website)"), KEEP)
            self.table.setCellWidget(row, 2, box)
            self.choice_boxes.append(box)
        layout.addWidget(self.table)

        buttons = QHBoxLayout()
        self.remove_all_btn = QPushButton(tr("Remove all from this computer too"))
        self.remove_all_btn.clicked.connect(lambda: self._decide_all(REMOVE))
        buttons.addWidget(self.remove_all_btn)
        self.keep_all_btn = QPushButton(tr("Keep all (and add them back on the website)"))
        self.keep_all_btn.clicked.connect(lambda: self._decide_all(KEEP))
        buttons.addWidget(self.keep_all_btn)
        buttons.addStretch()
        self.apply_btn = QPushButton(tr("Apply"))
        self.apply_btn.setDefault(True)
        self.apply_btn.clicked.connect(self._apply)
        buttons.addWidget(self.apply_btn)
        self.close_btn = QPushButton(tr("Close"))
        self.close_btn.clicked.connect(self.reject)   # Esc too: decide later for all
        buttons.addWidget(self.close_btn)
        layout.addLayout(buttons)

    def _decide_all(self, choice):
        for box in self.choice_boxes:
            box.setCurrentIndex(box.findData(choice))
        self._apply()

    def _apply(self):
        self._decided = {(item_id, list_id): box.currentData()
                         for (item_id, list_id, _s, _l), box in zip(self._entries, self.choice_boxes)
                         if box.currentData() != LATER}
        self.accept()

    def choices(self):
        """The decided rows; nothing when the prompt was closed without Apply."""
        return dict(self._decided) if self.result() == QDialog.DialogCode.Accepted else {}


def ask_about_web_removals(parent, entries):
    dialog = WebRemovalsDialog(parent, entries)
    try:
        dialog.exec()
        return dialog.choices()
    finally:
        dialog.deleteLater()
