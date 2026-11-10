# -*- coding: utf-8 -*-
"""Qt's own Hebrew strings for the widgets Qt draws itself.

The app translates its own texts with `tr()`, but the buttons Qt supplies --
the Yes / No / OK / Cancel of a static `QMessageBox.question(...)` or a
`QDialogButtonBox`, and the Cut / Copy / Paste menu of a text field -- come
from Qt, and stay English in the Hebrew UI unless Qt's own translation file
is installed. PyQt6 ships it (`qtbase_he.qm`) and PyInstaller bundles every
`qtbase_*.qm` with QtCore, so installing it once at startup covers every
such dialog, including the ones added later.

Only in Hebrew: a language change restarts the app, so the startup language
is the language for the whole run. Qt's Hebrew file also makes the app
right-to-left, which is what `GenizahGUI.init_ui` sets in Hebrew anyway.
"""
import logging
import os
from typing import Optional

from PyQt6.QtCore import QLibraryInfo, QTranslator

LOGGER = logging.getLogger("genizah." + __name__)

QT_TRANSLATION_FILE = "qtbase_he"


def _translation_dirs():
    """Where the file may be: Qt's own answer first, then the PyQt6 package
    folder, which is where a frozen build puts it (`PyQt6/Qt6/translations`)
    should Qt's answer not point there."""
    dirs = [QLibraryInfo.path(QLibraryInfo.LibraryPath.TranslationsPath)]
    try:
        import PyQt6
        dirs.append(os.path.join(os.path.dirname(PyQt6.__file__), "Qt6",
                                 "translations"))
    except Exception:                                        # noqa: BLE001
        pass
    return [d for d in dirs if d]


def install_qt_translations(app, lang) -> Optional[QTranslator]:
    """Install Qt's Hebrew translation on `app` when `lang` is 'he', and
    return the translator (None in English, or when the file is missing --
    the buttons then stay English, which is no reason to stop the app)."""
    if lang != "he":
        return None
    translator = QTranslator(app)
    for folder in _translation_dirs():
        if translator.load(QT_TRANSLATION_FILE, folder):
            if app.installTranslator(translator):
                return translator
    LOGGER.warning("Qt's Hebrew translation (%s.qm) was not found in %s; "
                   "Qt's own buttons stay in English",
                   QT_TRANSLATION_FILE, _translation_dirs())
    return None
