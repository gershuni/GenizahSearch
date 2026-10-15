"""The "Quoting texts..." dialog (owner decision 2026-10-15): looks like the
Quoted texts dialog (desktop/filter_text_dialog.py::FilterTextDialog) rather
than the generic ConditionDialog, and reuses that dialog's Sefaria library
browser under the name "Load from Sefaria...".

Kept in its own module, not desktop/compound_ui.py, so that module stays free
of network code: `QuotingTextsDialog` is built from `desktop.filter_text_
dialog`'s `AllSourcesDialog`, `AddCustomTextDialog` and `SefariaFetchThread`
(its own cache and cleaning, unchanged). `desktop/compound_ui.py` reaches this
module with a LAZY import inside `ConditionsStrip.add_condition` / `_edit` /
`open_quoting_texts`, never at module level, so the back-edge guard
(tests/test_no_back_edges_desktop.py) sees no network dependency from the
pure widgets module.

A "quoting texts" condition is always REQ_MUST_NOT, one text per source
(Sefaria passage, custom paste or file), no on/off checkboxes -- unlike the
Quoted texts dialog's sources, these texts belong to one search. `condition()`
returns exactly one `shared.compound_query.Condition`, carrying `version_names`
so editing it again shows the same names rather than "Version 1", "Version 2".
"""
from __future__ import annotations

import os
from typing import Optional

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QComboBox, QDialog, QDialogButtonBox, QFileDialog, QHBoxLayout, QLabel,
    QLineEdit, QListWidget, QMessageBox, QProgressBar, QPushButton,
    QVBoxLayout,
)

from genizah_core import CURRENT_LANG, tr
from shared import compound_query as cq
from shared.passage_normalize import norm_stream_fast

from desktop.compound_ui import PLACE_LABELS
from desktop.filter_text_dialog import (
    AddCustomTextDialog, AllSourcesDialog, SefariaFetchThread,
)


_STOP = object()     # the user cancelled a passage prompt: ask no more


class PassagePromptDialog(QDialog):
    """Small prompt for the passage within one Sefaria book title, e.g.
    "Rif Berakhot 1a-2b". The line edit is pre-filled with "<title> " so the
    user only has to type the passage; `ref()` is the whole thing, stripped."""

    def __init__(self, parent, title: str):
        super().__init__(parent)
        self._title = title
        self.setWindowTitle(tr("Load from Sefaria..."))
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        lay = QVBoxLayout(self)

        lay.addWidget(QLabel(tr("Which passage of {}?").format(title)))

        self.ref_input = QLineEdit(f"{title} ")
        lay.addWidget(self.ref_input)

        hint = QLabel(tr("For example: 1a-2b"))
        hint.setStyleSheet("color: #7f8c8d; font-size: 11px;")
        lay.addWidget(hint)

        self.buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setText(tr("OK"))
        self.buttons.button(QDialogButtonBox.StandardButton.Cancel).setText(tr("Cancel"))
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        lay.addWidget(self.buttons)

    def ref(self) -> str:
        return self.ref_input.text().strip()


class QuotingTextsDialog(QDialog):
    """Add or edit the search's one must_not ("quoting texts") condition.

    `condition`, when given, is the must_not condition being edited: its
    texts (with their names, or "Text 1" / "Text 2" ... when it predates
    `version_names`) and its place are loaded. `floor` is the active
    letter-level length floor, exactly as `ConditionDialog` takes it."""

    def __init__(self, parent=None, condition: Optional[cq.Condition] = None,
                 floor: int = 40):
        super().__init__(parent)
        self.setWindowTitle(tr("Quoting texts"))
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self._floor = int(floor)
        self._closed = False
        self.fetch_thread = None
        self._fetch_errors: list = []
        self._pending_refs: list = []

        if condition is not None:
            names = condition.version_names
            if len(names) != len(condition.versions):
                if len(condition.versions) == 1 and condition.label.strip():
                    names = (condition.label.strip(),)
                else:
                    names = tuple(tr("Text {}").format(i + 1)
                                  for i in range(len(condition.versions)))
            self._texts = [{'name': n, 'text': v}
                           for n, v in zip(names, condition.versions)]
        else:
            self._texts = []

        lay = QVBoxLayout(self)

        self.lbl_help = QLabel(tr(
            "Load or paste the passage of the quoting work that discusses "
            "your main text (for example Rif Berakhot 1a-2b), not the whole "
            "book. A page is removed only where at least 40 letters of it "
            "are not also in your main text."))
        self.lbl_help.setWordWrap(True)
        self.lbl_help.setStyleSheet("color: #7f8c8d; font-size: 11px;")
        lay.addWidget(self.lbl_help)

        btn_row = QHBoxLayout()
        self.btn_sefaria = QPushButton(tr("Load from Sefaria..."))
        self.btn_sefaria.clicked.connect(self._open_load_from_sefaria)
        btn_row.addWidget(self.btn_sefaria)
        self.btn_custom = QPushButton(tr("Add Custom Text"))
        self.btn_custom.clicked.connect(self._open_add_custom)
        btn_row.addWidget(self.btn_custom)
        self.btn_files = QPushButton(tr("Load from files..."))
        self.btn_files.clicked.connect(self._open_load_from_files)
        btn_row.addWidget(self.btn_files)
        btn_row.addStretch()
        lay.addLayout(btn_row)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        lay.addWidget(self.progress_bar)
        self.progress_label = QLabel("")
        self.progress_label.setVisible(False)
        lay.addWidget(self.progress_label)

        self.texts_list = QListWidget()
        self.texts_list.itemSelectionChanged.connect(self._update_remove_enabled)
        lay.addWidget(self.texts_list)

        list_btn_row = QHBoxLayout()
        self.btn_remove = QPushButton(tr("Remove"))
        self.btn_remove.setEnabled(False)
        self.btn_remove.clicked.connect(self._remove_selected)
        list_btn_row.addWidget(self.btn_remove)
        list_btn_row.addStretch()
        lay.addLayout(list_btn_row)

        self.combo_where = QComboBox()
        for key, text in PLACE_LABELS:
            self.combo_where.addItem(tr(text), key)
        self.combo_where.setCurrentIndex(self.combo_where.findData(
            condition.where if condition is not None else cq.WHERE_SAME_IMAGE))
        lay.addWidget(self.combo_where)

        self.lbl_info = QLabel('')
        self.lbl_info.setWordWrap(True)
        lay.addWidget(self.lbl_info)

        self.buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setText(tr("OK"))
        self.buttons.button(QDialogButtonBox.StandardButton.Cancel).setText(tr("Cancel"))
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        lay.addWidget(self.buttons)

        self._refresh_list()
        self.resize(560, 480)

    # -- the texts list --------------------------------------------------

    def _add_text(self, name: str, text: str) -> None:
        self._texts.append({'name': name, 'text': text})
        self._refresh_list()

    def _remove_selected(self) -> None:
        row = self.texts_list.currentRow()
        if row < 0:
            return
        del self._texts[row]
        self._refresh_list()

    def _update_remove_enabled(self) -> None:
        self.btn_remove.setEnabled(self.texts_list.currentRow() >= 0)

    def _refresh_list(self) -> None:
        self.texts_list.clear()
        for t in self._texts:
            letters = len(norm_stream_fast(t['text']))
            self.texts_list.addItem(tr("{} · {} letters").format(t['name'], letters))
        self._update_remove_enabled()
        self._refresh_info()

    def _refresh_info(self) -> None:
        problems = []
        if len(self._texts) > cq.MAX_VERSIONS:
            problems.append(tr("At most {} versions.").format(cq.MAX_VERSIONS))
        for i, t in enumerate(self._texts, start=1):
            letters = len(norm_stream_fast(t['text']))
            if letters < self._floor:
                problems.append(tr("Version {} is too short ({} letters; at least {}).")
                                .format(i, letters, self._floor))
            elif letters > cq.MAX_VERSION_LETTERS:
                problems.append(tr("Version {} is too long ({} letters; at most {}).")
                                .format(i, letters, cq.MAX_VERSION_LETTERS))
        self.lbl_info.setText('\n'.join(problems))
        self.lbl_info.setStyleSheet("color: #c0392b;" if problems else "color: #7f8c8d;")
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setEnabled(
            bool(self._texts) and not problems)

    # -- "Load from Sefaria..." -------------------------------------------

    def _open_load_from_sefaria(self) -> None:
        dlg = AllSourcesDialog(self)
        try:
            accepted = dlg.exec() == QDialog.DialogCode.Accepted
            titles = list(dlg.selected_refs) if accepted else []
        finally:
            dlg.deleteLater()
        if not titles:
            return
        refs = []
        for title in titles:
            ref = self._ask_passage(title)
            if ref is _STOP:
                break
            if ref:
                refs.append(ref)
        if refs:
            self._start_fetch(refs)

    def _ask_passage(self, title: str) -> Optional[str]:
        """The passage prompt for one selected book title, or None if the
        user cancelled or gave an empty / whole-book answer (refused with a
        message -- a whole book is far over the limit: Sefaria returns about
        213,000 characters for "Rif Berakhot" and 1.5 million for "Halakhot
        Gedolot")."""
        dlg = PassagePromptDialog(self, title)
        try:
            if dlg.exec() != QDialog.DialogCode.Accepted:
                return _STOP
            ref = dlg.ref()
        finally:
            dlg.deleteLater()
        if not ref or ref == title.strip():
            QMessageBox.warning(self, tr("Warning"), tr(
                "Choose a passage, not the whole book."))
            return None
        return ref

    def _start_fetch(self, refs) -> None:
        self._cancel_fetch()
        self._pending_refs = list(refs)
        self._fetch_errors = []
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(len(refs))
        self.progress_bar.setValue(0)
        self.progress_label.setVisible(True)
        self.btn_sefaria.setEnabled(False)
        thread = self.fetch_thread = SefariaFetchThread(refs)
        # Each slot is told which fetch it belongs to: a signal already queued
        # by a fetch that was replaced or cancelled is dropped, never applied
        # to the next fetch's passages or to a dialog that has closed.
        thread.progress.connect(lambda *a, t=thread: self._on_fetch_progress(*a, thread=t))
        thread.finished.connect(lambda r, t=thread: self._on_fetch_finished(r, thread=t))
        thread.error.connect(lambda m, t=thread: self._on_fetch_error(m, thread=t))
        thread.start()

    def _is_current(self, thread) -> bool:
        return not self._closed and thread is self.fetch_thread

    def _on_fetch_progress(self, current, total, item, thread=None) -> None:
        if not self._is_current(thread):
            return
        self.progress_bar.setValue(current)
        self.progress_label.setText(tr("Loading: {}").format(item))

    def _on_fetch_error(self, msg, thread=None) -> None:
        if not self._is_current(thread):
            return
        self._fetch_errors.append(msg)

    def _on_fetch_finished(self, results, thread=None) -> None:
        """`results` is {ref: cleaned_text} for every ref the fetch found --
        a ref it did not find is simply absent, same as an empty text."""
        if not self._is_current(thread):
            return
        self.fetch_thread = None
        self.btn_sefaria.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.progress_label.setVisible(False)
        messages = list(self._fetch_errors)
        self._fetch_errors = []
        pending, self._pending_refs = self._pending_refs, []
        for ref in pending:
            text = results.get(ref, '')
            letters = len(norm_stream_fast(text))
            if letters == 0:
                messages.append(tr("Not found on Sefaria: {}").format(ref))
                continue
            if letters > cq.MAX_VERSION_LETTERS:
                messages.append(tr(
                    "{} is too long ({} letters; at most {}). Choose a "
                    "shorter passage.").format(ref, letters, cq.MAX_VERSION_LETTERS))
                continue
            self._add_text(ref, text)
        if messages:
            QMessageBox.warning(self, tr("Warning"), '\n'.join(messages))

    # -- "Add Custom Text" / "Load from files..." --------------------------

    def _open_add_custom(self) -> None:
        dlg = AddCustomTextDialog(self)
        try:
            if dlg.exec() == QDialog.DialogCode.Accepted and dlg.custom_text:
                self._add_text(dlg.custom_name or dlg.custom_ref, dlg.custom_text)
        finally:
            dlg.deleteLater()

    def _open_load_from_files(self) -> None:
        paths, _ = QFileDialog.getOpenFileNames(
            self, tr("Load from files..."), '', tr("Text files (*.txt);;All files (*)"))
        for path in paths or []:
            try:
                with open(path, encoding='utf-8-sig') as fh:
                    text = fh.read().strip()
            except (OSError, UnicodeDecodeError):
                continue
            if not text:
                continue
            self._add_text(os.path.splitext(os.path.basename(path))[0], text)

    # -- result / lifetime --------------------------------------------------

    def condition(self) -> cq.Condition:
        names = tuple(t['name'] for t in self._texts)
        versions = tuple(t['text'] for t in self._texts)
        return cq.Condition(cq.REQ_MUST_NOT, self.combo_where.currentData(),
                            versions, ', '.join(names), version_names=names)

    def _cancel_fetch(self) -> None:
        thread, self.fetch_thread = self.fetch_thread, None
        if thread is not None:
            for sig in (thread.progress, thread.finished, thread.error):
                try:
                    sig.disconnect()
                except TypeError:          # nothing connected
                    pass
            if thread.isRunning():
                thread.cancel()
                thread.wait()
        self._pending_refs = []
        self.btn_sefaria.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.progress_label.setVisible(False)

    def accept(self) -> None:
        self._closed = True
        self._cancel_fetch()
        super().accept()

    def reject(self) -> None:
        self._closed = True
        self._cancel_fetch()
        super().reject()

    def closeEvent(self, event) -> None:
        self._closed = True
        self._cancel_fetch()
        super().closeEvent(event)
