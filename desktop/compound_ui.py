"""Composition Search conditions: the strip under the main text and the
condition dialog (docs/plans/COMPOSITION_CONDITIONS_PLAN.md section 5).

Owner decision 2026-10-14: conditions sit UNDER the main text, each with its
own requirement (must appear / must not appear) and place (same image,
adjacent image, same manuscript). At most four short rows, so they stay on the
main window; the texts themselves live in the dialog.

Pure widgets: they hold `shared.compound_query.Condition` values and say when
the list changed. Searching, validation against the method, and persistence
belong to the window.
"""

from __future__ import annotations

import os
from typing import Callable, Optional

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QComboBox, QDialog, QDialogButtonBox, QFileDialog, QHBoxLayout, QLabel,
    QLineEdit, QPlainTextEdit, QPushButton, QToolButton, QVBoxLayout, QWidget,
)

from genizah_core import CURRENT_LANG, tr
from shared import compound_query as cq
from shared.passage_normalize import norm_stream_fast

REQUIREMENT_LABELS = (
    (cq.REQ_MUST, "Must appear"),
    (cq.REQ_MUST_NOT, "Must not appear"),
)
PLACE_LABELS = (
    (cq.WHERE_SAME_IMAGE, "On the same image"),
    (cq.WHERE_ADJACENT, "On an adjacent image"),
    (cq.WHERE_MANUSCRIPT, "Anywhere in the manuscript"),
)


def _label_for(pairs, key) -> str:
    return tr(dict(pairs).get(key, key))


def condition_summary(cond: cq.Condition, words: int = 6) -> str:
    """One line for the strip: requirement · place · name or first words."""
    name = cond.label.strip()
    if not name:
        first = (cond.versions[0] if cond.versions else '').split()
        name = ' '.join(first[:words]) + (' ...' if len(first) > words else '')
    if len(cond.versions) > 1:
        name += ' ' + tr("({} versions)").format(len(cond.versions))
    mark = '✓' if cond.requirement == cq.REQ_MUST else '✗'
    return (f"{mark} {_label_for(REQUIREMENT_LABELS, cond.requirement)} · "
            f"{_label_for(PLACE_LABELS, cond.where)} · {name}")


class ConditionDialog(QDialog):
    """Add or edit one condition. `floor` is the active letter-level length
    floor (letters), used only to warn before Search."""

    def __init__(self, parent=None, condition: Optional[cq.Condition] = None,
                 floor: int = 40):
        super().__init__(parent)
        self.setWindowTitle(tr("Search condition"))
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self._floor = int(floor)
        lay = QVBoxLayout(self)

        row = QHBoxLayout()
        self.combo_requirement = QComboBox()
        for key, text in REQUIREMENT_LABELS:
            self.combo_requirement.addItem(tr(text), key)
        self.combo_where = QComboBox()
        for key, text in PLACE_LABELS:
            self.combo_where.addItem(tr(text), key)
        row.addWidget(self.combo_requirement)
        row.addWidget(self.combo_where)
        lay.addLayout(row)

        self.edit_label = QLineEdit()
        self.edit_label.setPlaceholderText(tr("Name (optional), e.g. Halakhot Gedolot"))
        lay.addWidget(self.edit_label)

        self.edit_text = QPlainTextEdit()
        self.edit_text.setPlaceholderText(tr(
            "Paste the text. To give several versions, separate them with a "
            "blank line; any one of them counts."))
        if CURRENT_LANG == 'he':
            self.edit_text.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        lay.addWidget(self.edit_text)

        files_row = QHBoxLayout()
        self.btn_files = QPushButton(tr("Load from files..."))
        self.btn_files.clicked.connect(self._load_files)
        files_row.addWidget(self.btn_files)
        files_row.addStretch()
        lay.addLayout(files_row)

        self.lbl_info = QLabel('')
        self.lbl_info.setWordWrap(True)
        lay.addWidget(self.lbl_info)

        self.lbl_help = QLabel(tr(
            "For \"must not appear\", give the passages of the quoting work "
            "that are not quotations of your main text; otherwise the main "
            "text's own manuscripts are removed too."))
        self.lbl_help.setWordWrap(True)
        self.lbl_help.setStyleSheet("color: #7f8c8d; font-size: 11px;")
        lay.addWidget(self.lbl_help)

        self.buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setText(tr("OK"))
        self.buttons.button(QDialogButtonBox.StandardButton.Cancel).setText(tr("Cancel"))
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        lay.addWidget(self.buttons)

        if condition is not None:
            self.combo_requirement.setCurrentIndex(
                self.combo_requirement.findData(condition.requirement))
            self.combo_where.setCurrentIndex(self.combo_where.findData(condition.where))
            self.edit_label.setText(condition.label)
            self.edit_text.setPlainText('\n\n'.join(condition.versions))
        self.edit_text.textChanged.connect(self._refresh)
        self.combo_requirement.currentIndexChanged.connect(self._refresh)
        self._refresh()
        self.resize(560, 420)

    def versions(self) -> list:
        return cq.split_versions(self.edit_text.toPlainText())

    def _refresh(self):
        versions = self.versions()
        letters = [len(norm_stream_fast(v)) for v in versions]
        problems = []
        for i, n in enumerate(letters, start=1):
            if n < self._floor:
                problems.append(tr("Version {} is too short ({} letters; at least {}).")
                                .format(i, n, self._floor))
            elif n > cq.MAX_VERSION_LETTERS:
                problems.append(tr("Version {} is too long ({} letters; at most {}).")
                                .format(i, n, cq.MAX_VERSION_LETTERS))
        if len(versions) > cq.MAX_VERSIONS:
            problems.append(tr("At most {} versions.").format(cq.MAX_VERSIONS))
        info = (tr("{} versions: {} letters").format(
            len(versions), ', '.join(str(n) for n in letters)) if versions else '')
        self.lbl_info.setText('\n'.join([info] + problems).strip())
        self.lbl_info.setStyleSheet("color: #c0392b;" if problems else "color: #7f8c8d;")
        self.lbl_help.setVisible(
            self.combo_requirement.currentData() == cq.REQ_MUST_NOT)
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setEnabled(
            bool(versions) and not problems)

    def _load_files(self):
        paths, _ = QFileDialog.getOpenFileNames(
            self, tr("Load from files..."), '', tr("Text files (*.txt);;All files (*)"))
        texts = []
        for path in paths or []:
            try:
                with open(path, encoding='utf-8-sig') as fh:
                    texts.append(fh.read().strip())
            except (OSError, UnicodeDecodeError):
                continue
        texts = [t for t in texts if t]
        if not texts:
            return
        current = self.edit_text.toPlainText().strip()
        joined = '\n\n'.join(([current] if current else []) + texts)
        self.edit_text.setPlainText(joined)
        if not self.edit_label.text().strip() and len(paths) == 1:
            self.edit_label.setText(os.path.splitext(os.path.basename(paths[0]))[0])

    def condition(self) -> cq.Condition:
        return cq.Condition(self.combo_requirement.currentData(),
                            self.combo_where.currentData(),
                            tuple(self.versions()),
                            self.edit_label.text().strip())


class ConditionsStrip(QWidget):
    """The rows under the main text: one per condition, and "+ Add condition".

    `changed` carries the new list. `searches_for(n_conditions_versions)` is
    asked for the run's total search count so the strip can show it against
    the cap; `floor()` for the dialog's warnings."""

    changed = pyqtSignal(list)

    def __init__(self, parent=None, floor: Callable[[], int] = lambda: 40,
                 main_searches: Callable[[], int] = lambda: 1):
        super().__init__(parent)
        self._conditions: list = []
        self._floor = floor
        self._main_searches = main_searches
        self._enabled = True
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 2, 0, 2)
        outer.setSpacing(2)
        head = QHBoxLayout()
        self.lbl_title = QLabel(tr("More conditions") + ":")
        self.lbl_count = QLabel('')
        self.lbl_count.setStyleSheet("color: #7f8c8d;")
        self.btn_add = QPushButton(tr("+ Add condition"))
        self.btn_add.clicked.connect(self._add)
        head.addWidget(self.lbl_title)
        head.addWidget(self.lbl_count)
        head.addStretch()
        head.addWidget(self.btn_add)
        outer.addLayout(head)
        self._rows_box = QVBoxLayout()
        self._rows_box.setSpacing(1)
        outer.addLayout(self._rows_box)
        self.lbl_disabled = QLabel('')
        self.lbl_disabled.setStyleSheet("color: #e67e22; font-size: 11px;")
        self.lbl_disabled.setVisible(False)
        outer.addWidget(self.lbl_disabled)
        self._rebuild()

    # -- state ---------------------------------------------------------------
    def conditions(self) -> list:
        return list(self._conditions)

    def set_conditions(self, conditions) -> None:
        self._conditions = list(conditions or [])
        self._rebuild()

    def set_available(self, available: bool, reason: str = '') -> None:
        """Letter-level only: in another mode the rows stay visible (so the
        user can see what is set) but cannot be edited, and the reason shows."""
        self._enabled = bool(available)
        self.lbl_disabled.setText(reason if not available else '')
        self.lbl_disabled.setVisible(bool(reason) and not available)
        self._rebuild()

    # -- actions -------------------------------------------------------------
    def _add(self):
        if len(self._conditions) >= cq.MAX_CONDITIONS:
            return
        dlg = ConditionDialog(self, floor=self._floor())
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._conditions.append(dlg.condition())
            self._emit()

    def _edit(self, index: int):
        dlg = ConditionDialog(self, self._conditions[index], floor=self._floor())
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._conditions[index] = dlg.condition()
            self._emit()

    def _remove(self, index: int):
        del self._conditions[index]
        self._emit()

    def _emit(self):
        self._rebuild()
        self.changed.emit(list(self._conditions))

    # -- drawing -------------------------------------------------------------
    def _rebuild(self):
        while self._rows_box.count():
            item = self._rows_box.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()
        for i, cond in enumerate(self._conditions):
            row = QWidget()
            h = QHBoxLayout(row)
            h.setContentsMargins(12, 0, 0, 0)
            text = QLabel(condition_summary(cond))
            text.setToolTip('\n\n'.join(cond.versions)[:600])
            h.addWidget(text, 1)
            edit = QToolButton()
            edit.setText('✎')
            edit.setToolTip(tr("Edit"))
            edit.clicked.connect(lambda _c=False, i=i: self._edit(i))
            remove = QToolButton()
            remove.setText('✕')
            remove.setToolTip(tr("Remove"))
            remove.clicked.connect(lambda _c=False, i=i: self._remove(i))
            edit.setEnabled(self._enabled)
            remove.setEnabled(self._enabled)
            h.addWidget(edit)
            h.addWidget(remove)
            self._rows_box.addWidget(row)
        self.btn_add.setEnabled(self._enabled
                                and len(self._conditions) < cq.MAX_CONDITIONS)
        if self._conditions:
            total = self._main_searches() + sum(len(c.versions) for c in self._conditions)
            self.lbl_count.setText(tr("{} searches (limit {})").format(
                total, cq.MAX_SEARCHES))
            self.lbl_count.setStyleSheet(
                "color: #c0392b;" if total > cq.MAX_SEARCHES else "color: #7f8c8d;")
        else:
            self.lbl_count.setText('')
