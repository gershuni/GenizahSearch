"""Qt's own Hebrew strings (owner hand test, 2026-10-17): in the Hebrew UI,
the Yes / No of a static `QMessageBox.question(...)` read English. Qt draws
those buttons, so `tr()` never sees them; `desktop.qt_translations` installs
Qt's Hebrew file at startup instead."""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QMessageBox

import desktop.qt_translations as qtt

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])

SB = QMessageBox.StandardButton
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _labels():
    box = QMessageBox()
    box.setStandardButtons(SB.Yes | SB.No | SB.Ok | SB.Cancel)
    return {sb: box.button(sb).text()
            for sb in (SB.Yes, SB.No, SB.Ok, SB.Cancel)}


@pytest.fixture
def installed():
    """Translators a test installed, removed afterwards: an installed one
    would turn every later test's buttons Hebrew."""
    out = []
    yield out
    for translator in out:
        _APP.removeTranslator(translator)


def test_hebrew_gets_hebrew_buttons(installed):
    translator = qtt.install_qt_translations(_APP, "he")
    assert translator is not None
    installed.append(translator)
    labels = _labels()
    assert labels[SB.Yes] == "&כן"
    assert labels[SB.No] == "&לא"
    assert labels[SB.Ok] == "אישור"
    assert labels[SB.Cancel] == "ביטול"


def test_english_installs_nothing(installed):
    assert qtt.install_qt_translations(_APP, "en") is None
    assert _labels()[SB.Yes] == "&Yes"


def test_a_missing_file_leaves_english_and_does_not_raise(
        installed, monkeypatch, tmp_path):
    monkeypatch.setattr(qtt, "_translation_dirs", lambda: [str(tmp_path)])
    assert qtt.install_qt_translations(_APP, "he") is None
    assert _labels()[SB.Yes] == "&Yes"


def test_the_pyqt6_folder_is_the_fallback(installed, monkeypatch, tmp_path):
    """Should Qt's own answer not point at the file (a frozen build), the
    PyQt6 package folder, where PyInstaller puts it, still finds it."""
    class _NoWhere:
        LibraryPath = qtt.QLibraryInfo.LibraryPath

        @staticmethod
        def path(_which):
            return str(tmp_path)

    monkeypatch.setattr(qtt, "QLibraryInfo", _NoWhere)
    translator = qtt.install_qt_translations(_APP, "he")
    assert translator is not None
    installed.append(translator)
    assert _labels()[SB.Yes] == "&כן"


def test_startup_installs_it_before_any_dialog():
    with open(os.path.join(REPO_ROOT, "genizah_app.py"),
              encoding="utf-8") as fh:
        src = fh.read()
    main = src[src.index('\nif __name__ == "__main__":'):]     # top level
    at_app = main.index("app = QApplication(sys.argv)")
    at_install = main.index("install_qt_translations(app, CURRENT_LANG)")
    at_lock = main.index("acquire_instance_lock(")   # may show "already open"
    assert at_app < at_install < at_lock
    code_before = [line for line in main[:at_install].splitlines()
                   if not line.strip().startswith("#")]
    assert not [line for line in code_before if "QMessageBox" in line]
