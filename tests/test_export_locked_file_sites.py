# -*- coding: utf-8 -*-
"""Every desktop export, saving over a file Word or Excel holds open
(owner, 2026-10-17): the user is asked, and Try again / Save under another
name / Cancel do what they say. Before, each export showed a raw English
"Failed to save DOCX: [Errno 13] Permission denied" inside the Hebrew UI.

The lock is simulated where every writer meets it -- `open` / `io.open` (CSV
and text write through the builtin; python-docx and openpyxl through
`zipfile`, which calls `io.open`) -- raising the PermissionError Windows
raises for a file open in Word. The question box and the Save As dialog are
scripted.
"""
from __future__ import annotations

import ast
import builtins
import io
import os
from pathlib import Path

import pytest
from PyQt6.QtWidgets import QCheckBox, QLineEdit, QPushButton

import desktop.export_save as es
import genizah_app
import genizah_core
from tests.test_comp_flat_markers_and_xlsx_columns import _patch_dialogs
from tests.test_desktop_export_rtl_and_credits import _host
from tests.test_exclusion_surfaces import (A, B, _res, _search,  # noqa: F401
                                           slot_errors, window)

pytestmark = pytest.mark.gui  # imports genizah_app, which imports PyQt6

FORMATS = ("xlsx", "csv", "docx", "txt")


@pytest.fixture(autouse=True)
def _english(monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en", raising=False)


@pytest.fixture
def lock(monkeypatch):
    """`lock(path)`: writes to `path` raise PermissionError until the
    returned state's `path` is cleared (the file closed)."""
    real_open = io.open
    state = {"path": None}

    def fake_open(file, mode="r", *a, **k):
        if (state["path"] and isinstance(file, (str, os.PathLike))
                and any(c in mode for c in "wax+")
                and os.path.normcase(os.path.abspath(os.fspath(file))) == state["path"]):
            raise PermissionError(13, "Permission denied", os.fspath(file))
        return real_open(file, mode, *a, **k)

    monkeypatch.setattr(builtins, "open", fake_open)
    monkeypatch.setattr(io, "open", fake_open)

    def _lock(path):
        state["path"] = os.path.normcase(os.path.abspath(str(path)))
        return state
    return _lock


def _script(monkeypatch, answers, other=None, on_ask=None):
    """Script the question box. `on_ask` runs when it is shown -- the user
    closing Word before choosing Try again. (Counting refused writes would
    not do: zipfile, under both .docx and .xlsx, retries a refused open in a
    second mode by itself.)"""
    asked = []

    def ask(parent, path):
        asked.append(path)
        if on_ask:
            on_ask()
        return answers.pop(0)
    monkeypatch.setattr(es, "ask_locked", ask)
    monkeypatch.setattr(es, "pick_other_path", lambda parent, path: other and str(other))
    return asked


# --- Composition Search ------------------------------------------------------

def _comp_export(monkeypatch, tmp_path, fmt):
    out = tmp_path / f"report.{fmt}"
    _patch_dialogs(monkeypatch, out)
    host = _host("." + fmt)
    shown = []
    host._show_export_saved_dialog = lambda p: shown.append(p)
    return host, out, shown


@pytest.mark.parametrize("fmt", FORMATS)
def test_composition_export_saves_under_another_name(monkeypatch, tmp_path, lock, fmt):
    host, out, shown = _comp_export(monkeypatch, tmp_path, fmt)
    other = tmp_path / f"report (2).{fmt}"
    lock(out)
    asked = _script(monkeypatch, [es.RENAME], other)
    host.export_comp_report(fmt)
    assert asked == [str(out)]
    assert other.exists() and other.stat().st_size > 0
    assert not out.exists()
    assert shown == [str(other)], "the saved dialog names the file that was written"


@pytest.mark.parametrize("fmt", FORMATS)
def test_composition_export_tries_again_after_the_file_is_closed(
        monkeypatch, tmp_path, lock, fmt):
    host, out, shown = _comp_export(monkeypatch, tmp_path, fmt)
    state = lock(out)
    asked = _script(monkeypatch, [es.RETRY],
                    on_ask=lambda: state.update(path=None))   # Word closed
    host.export_comp_report(fmt)
    assert asked == [str(out)]
    assert out.exists() and shown == [str(out)]


def test_composition_export_cancel_writes_nothing_and_shows_no_error(
        monkeypatch, tmp_path, lock):
    host, out, shown = _comp_export(monkeypatch, tmp_path, "docx")
    lock(out)
    _script(monkeypatch, [es.CANCEL])
    host.export_comp_report("docx")            # _patch_dialogs fails on any error box
    assert not out.exists() and shown == []


# --- the main search ----------------------------------------------------------

def _search_export(w, monkeypatch, tmp_path, fmt):
    _search(w, [_res(A, 1), _res(B, 1)])
    w.gap_input = QLineEdit()
    w.btn_lab_mode_toggle = QPushButton()
    w.btn_lab_mode_toggle.setCheckable(True)
    w.chk_lab_deep = QCheckBox()
    w._pgp_transcription_sys_ids = set()
    out = tmp_path / f"results.{fmt}"
    w._default_report_path = lambda q, name: str(out)
    _patch_dialogs(monkeypatch, out)
    shown = []
    w._show_export_saved_dialog = lambda p: shown.append(p)
    w._save_last_folder = lambda p: None
    return w, out, shown


@pytest.mark.parametrize("fmt", FORMATS)
def test_search_export_saves_under_another_name(window, monkeypatch, tmp_path, lock, fmt):  # noqa: F811 (the imported fixture)
    w, out, shown = _search_export(window, monkeypatch, tmp_path, fmt)
    other = tmp_path / f"results (2).{fmt}"
    lock(out)
    asked = _script(monkeypatch, [es.RENAME], other)
    w.export_results(fmt)
    assert asked == [str(out)]
    assert other.exists() and other.stat().st_size > 0
    assert not out.exists()
    assert shown == [str(other)]


# --- nothing writes around the helper -------------------------------------------

_APP = Path(genizah_app.__file__)
_JOINS = Path(genizah_app.__file__).parent / "desktop" / "join_workbench.py"


def _function(path, name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef) and n.name == name)


@pytest.mark.parametrize("path,name,writes", [(_APP, "export_results", 4),
                                              (_APP, "export_comp_report", 4),
                                              (_JOINS, "_export_xlsx", 1)])
def test_no_export_writes_its_file_around_the_helper(path, name, writes):
    """The nine save paths live in these three functions: four formats each
    for the search and Composition Search, one for the Joins Lab. A direct
    `open(path, 'w...')` or `.save(path)` would skip the question."""
    fn = _function(path, name)
    direct, through = [], 0
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        if isinstance(target, ast.Name) and target.id == "save_export":
            through += 1
        if isinstance(target, ast.Name) and target.id == "open" and len(node.args) >= 2:
            mode = node.args[1]
            if isinstance(mode, ast.Constant) and any(c in str(mode.value) for c in "wa"):
                direct.append(ast.unparse(node))
        if (isinstance(target, ast.Attribute) and target.attr == "save" and node.args
                and isinstance(node.args[0], ast.Name) and node.args[0].id == "path"):
            direct.append(ast.unparse(node))
    assert not direct, direct
    assert through == writes, f"{name}: {through} writes go through save_export, not {writes}"
