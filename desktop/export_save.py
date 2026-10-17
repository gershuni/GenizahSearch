# -*- coding: utf-8 -*-
"""Writing an export file the user may still have open.

Word and Excel lock the file they have open, and saving over it raises
`PermissionError`. Every export showed that as a raw English error inside the
Hebrew interface ("Failed to save DOCX: [Errno 13] Permission denied: ...";
owner, 2026-10-17). Every desktop export now writes through `save_export`,
which asks: Try again (after closing the file), Save under another name, or
Cancel.

The export is built once; only the write is repeated. Any other error still
reaches the caller's own handler.

`write_export` is the decision loop with the question and the Save As dialog
passed in, so it is testable without Qt; `save_export` supplies the Qt ones.
"""
from __future__ import annotations

import os
from typing import Callable, Optional

RETRY = 'retry'
RENAME = 'rename'
CANCEL = 'cancel'


def free_name(path: str) -> str:
    """`report.docx` -> `report (2).docx`: the first such name not taken."""
    stem, ext = os.path.splitext(path)
    n = 2
    while os.path.exists(f"{stem} ({n}){ext}"):
        n += 1
    return f"{stem} ({n}){ext}"


def write_text(path: str, text: str, encoding: str = 'utf-8',
               newline: Optional[str] = None) -> None:
    with open(path, 'w', encoding=encoding, newline=newline) as f:
        f.write(text)


def write_bytes(path: str, data: bytes) -> None:
    with open(path, 'wb') as f:
        f.write(data)


def write_export(path: str, write: Callable[[str], None],
                 ask: Callable[[str], str],
                 pick: Callable[[str], Optional[str]]) -> Optional[str]:
    """Run `write(path)`. While it fails with `PermissionError`, `ask(path)`
    decides: RETRY writes the same path again, RENAME writes to the path
    `pick(path)` returns (None cancels), anything else cancels.

    Returns the path written, or None when the user gave up. Errors other
    than `PermissionError` propagate."""
    while True:
        try:
            write(path)
            return path
        except PermissionError:
            choice = ask(path)
            if choice == RETRY:
                continue
            if choice == RENAME:
                other = pick(path)
                if other:
                    path = other
                    continue
            return None


def save_export(parent, path: str, write: Callable[[str], None]) -> Optional[str]:
    """`write_export` with the window's question box and Save As dialog."""
    return write_export(path, write,
                        lambda p: ask_locked(parent, p),
                        lambda p: pick_other_path(parent, p))


def ask_locked(parent, path: str) -> str:
    from PyQt6.QtWidgets import QMessageBox

    from genizah_core import tr
    box = QMessageBox(parent)
    box.setIcon(QMessageBox.Icon.Warning)
    box.setWindowTitle(tr("File in use"))
    box.setText(tr('Could not save "{name}". It may be open in another '
                   'program, such as Word or Excel.').format(
                       name=os.path.basename(path)))
    box.setInformativeText(tr(
        'Close it and choose Try again, or save it under another name.'))
    retry = box.addButton(tr("Try again"), QMessageBox.ButtonRole.AcceptRole)
    rename = box.addButton(tr("Save under another name"),
                           QMessageBox.ButtonRole.ActionRole)
    box.addButton(tr("Cancel"), QMessageBox.ButtonRole.RejectRole)
    box.setDefaultButton(retry)
    box.exec()
    clicked = box.clickedButton()
    if clicked is retry:
        return RETRY
    if clicked is rename:
        return RENAME
    return CANCEL


def pick_other_path(parent, path: str) -> Optional[str]:
    from PyQt6.QtWidgets import QFileDialog

    from genizah_core import tr
    ext = os.path.splitext(path)[1]
    chosen, _ = QFileDialog.getSaveFileName(
        parent, tr("Save under another name"), free_name(path),
        f"*{ext}" if ext else "")
    if not chosen:
        return None
    if ext and not chosen.lower().endswith(ext.lower()):
        chosen += ext
    return chosen
