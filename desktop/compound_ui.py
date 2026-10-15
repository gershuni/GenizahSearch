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
from desktop.ui_widgets import ElidingLabel
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


# Owner decision 2026-10-14: the dialog's requirement is fixed by its entry
# point (the "Filter texts" menu's "Quoting texts..." / "Add condition..."),
# never chosen in the dialog; the window title follows it, since "Condition"
# was taken ("מצב") -- see docs/plans/COMPOSITION_CONDITIONS_PLAN.md section 12.
_TITLE_FOR_REQUIREMENT = {
    cq.REQ_MUST: "Search condition",
    cq.REQ_MUST_NOT: "Quoting text",
}
# Only "Quoting texts..." (must-not) gets a non-default starting place; "Add
# condition..." (must) keeps the first combo item, as before. Owner, 2026-10-14
# (all four reviewers recommended it): now "On the same image" -- the default
# used to be "Anywhere in the manuscript", wider than the "own text" rule
# needs (docs/plans/COMPOSITION_CONDITIONS_PLAN.md section 12) for the common
# case of a quoting work cited right where it is quoted.
_DEFAULT_WHERE_FOR_REQUIREMENT = {
    cq.REQ_MUST_NOT: cq.WHERE_SAME_IMAGE,
}


class ConditionDialog(QDialog):
    """Add or edit one condition. `floor` is the active letter-level length
    floor (letters), used only to warn before Search.

    The requirement is FIXED for the dialog's lifetime: passed in `requirement`
    for a new condition, or taken from `condition` when editing one (editing
    always keeps the row's own requirement). There is no requirement combo."""

    def __init__(self, parent=None, condition: Optional[cq.Condition] = None,
                 floor: int = 40, requirement: Optional[str] = None):
        super().__init__(parent)
        self._requirement = condition.requirement if condition is not None else requirement
        if self._requirement not in cq.REQUIREMENTS:
            raise ValueError(f'ConditionDialog needs a fixed requirement, got {self._requirement!r}')
        self.setWindowTitle(tr(_TITLE_FOR_REQUIREMENT.get(self._requirement, "Search condition")))
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self._floor = int(floor)
        lay = QVBoxLayout(self)

        self.combo_where = QComboBox()
        for key, text in PLACE_LABELS:
            self.combo_where.addItem(tr(text), key)
        lay.addWidget(self.combo_where)

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
            "Paste the quoting work's passage as it is. A page is removed "
            "only where at least 40 letters of it are not also in your main "
            "text."))
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
            self.combo_where.setCurrentIndex(self.combo_where.findData(condition.where))
            self.edit_label.setText(condition.label)
            self.edit_text.setPlainText('\n\n'.join(condition.versions))
        else:
            default_where = _DEFAULT_WHERE_FOR_REQUIREMENT.get(self._requirement)
            if default_where is not None:
                self.combo_where.setCurrentIndex(self.combo_where.findData(default_where))
        self.edit_text.textChanged.connect(self._refresh)
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
        self.lbl_help.setVisible(self._requirement == cq.REQ_MUST_NOT)
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
        return cq.Condition(self._requirement,
                            self.combo_where.currentData(),
                            tuple(self.versions()),
                            self.edit_label.text().strip())


class ConditionsStrip(QWidget):
    """The rows under the main text: one per condition.

    There is no "Add" button here any more (owner decision 2026-10-14): the
    Composition tab's "Filter texts" menu calls `add_condition` instead. The
    whole strip is hidden when there are no conditions.

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
        head.addWidget(self.lbl_title)
        head.addWidget(self.lbl_count)
        head.addStretch()
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

    def add_disabled_reason(self) -> str:
        """Why `add_condition` cannot be used right now, or '' if it can.
        The Composition tab's "Filter texts" menu uses this for the "Quoting
        texts..." / "Add condition..." items' tooltip and enabled state."""
        if not self._enabled:
            return self.lbl_disabled.text() or tr(
                "Conditions work only with letter-level search.")
        if len(self._conditions) >= cq.MAX_CONDITIONS:
            return tr("At most {} conditions.").format(cq.MAX_CONDITIONS)
        return ''

    def can_add(self) -> bool:
        return not self.add_disabled_reason()

    # -- actions -------------------------------------------------------------
    def _dialog_for(self, requirement: str, condition: Optional[cq.Condition] = None):
        """ConditionDialog for REQ_MUST, QuotingTextsDialog for REQ_MUST_NOT
        (owner, 2026-10-15): must_not conditions are the "Quoting texts..."
        dialog, never the generic one, whether adding or editing. A lazy
        import -- desktop/quoting_texts_dialog.py reaches Sefaria over the
        network, and this module stays free of that (the back-edge guard,
        tests/test_no_back_edges_desktop.py, registers it all the same)."""
        if requirement == cq.REQ_MUST_NOT:
            from desktop.quoting_texts_dialog import QuotingTextsDialog
            return QuotingTextsDialog(self, condition=condition, floor=self._floor())
        if condition is not None:
            return ConditionDialog(self, condition, floor=self._floor())
        return ConditionDialog(self, requirement=requirement, floor=self._floor())

    def add_condition(self, requirement: str) -> bool:
        """Open the add dialog with a FIXED requirement -- the Composition
        tab's "Filter texts" menu calls this for "Add condition..."
        (REQ_MUST); `open_quoting_texts` is REQ_MUST_NOT's own entry point,
        but this still dispatches correctly if an older caller passes it.
        Owns the 4-condition cap and the letter-level rule (`can_add`), so no
        caller can add one the menu would have refused. Returns whether a
        condition was added."""
        if not self.can_add():
            return False
        dlg = self._dialog_for(requirement)
        try:
            if dlg.exec() == QDialog.DialogCode.Accepted:
                self._conditions.append(dlg.condition())
                self._emit()
                return True
            return False
        finally:
            dlg.deleteLater()

    def quoting_texts_disabled_reason(self) -> str:
        """Why "Quoting texts..." cannot be used now, or ''. With a must_not
        row already there it only EDITS that row, so the cap does not apply."""
        if not self._enabled:
            return self.add_disabled_reason()
        if any(c.requirement == cq.REQ_MUST_NOT for c in self._conditions):
            return ''
        return self.add_disabled_reason()

    def open_quoting_texts(self) -> bool:
        """"Quoting texts..." menu item (owner, 2026-10-15): edits the FIRST
        existing must_not condition if there is one, otherwise adds a new one
        -- the same cap and letter-level rule as `add_condition`, since this
        is just its must_not entry point with the "edit instead of
        duplicating" rule on top. Old sessions that already have several
        must_not rows are untouched: each keeps editing its own row via
        `_edit`."""
        if self.quoting_texts_disabled_reason():
            return False
        index = next((i for i, c in enumerate(self._conditions)
                     if c.requirement == cq.REQ_MUST_NOT), None)
        if index is not None:
            self._edit(index)
            return True
        return self.add_condition(cq.REQ_MUST_NOT)

    def _edit(self, index: int):
        condition = self._conditions[index]
        dlg = self._dialog_for(condition.requirement, condition)
        try:
            if dlg.exec() == QDialog.DialogCode.Accepted:
                self._conditions[index] = dlg.condition()
                self._emit()
        finally:
            dlg.deleteLater()

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
            # Elides rather than widening the window: a long name must not
            # push the tab past a small screen (300% scaling). The tooltip
            # carries the whole line.
            summary = condition_summary(cond)
            text = ElidingLabel(summary)
            text.setMinimumWidth(1)
            text.setToolTip(summary + '\n\n' + '\n\n'.join(cond.versions)[:600])
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
        self.setVisible(bool(self._conditions))
        if self._conditions:
            total = self._main_searches() + sum(len(c.versions) for c in self._conditions)
            self.lbl_count.setText(tr("{} searches (limit {})").format(
                total, cq.MAX_SEARCHES))
            self.lbl_count.setStyleSheet(
                "color: #c0392b;" if total > cq.MAX_SEARCHES else "color: #7f8c8d;")
        else:
            self.lbl_count.setText('')
