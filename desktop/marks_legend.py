"""The "?" beside a Genuzot transcription, and the legend it opens (GS-274 option A).

The Genuzot team types its marks into the transcription (``?ב?``, ``@...@``,
``<...>``); the desktop shows them as typed. The legend table is the website's:
``shared.fgp_marks_legend.legend_html``, so the two apps cannot drift.

The button follows the version chooser: ``_populate_pgp_combo`` tags a Genuzot
transcription's item with ``marks_legend``, and every render path calls
``sync_marks_legend_button`` with the chooser whose item is on screen (or with
none, for the automatic transcription and corrections).
"""

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QDialog, QDialogButtonBox, QTextBrowser, QToolButton, QVBoxLayout

from shared.fgp_marks_legend import labels, legend_html


def _lang():
    # Live read: CURRENT_LANG flips at runtime on a language switch, and an
    # import-time binding would keep the old one.
    from genizah_core import CURRENT_LANG
    return CURRENT_LANG


class MarksLegendDialog(QDialog):
    """The legend. Non-modal, so it can stay open beside the text it explains."""

    def __init__(self, parent=None):
        super().__init__(parent)
        lang = _lang()
        self.setWindowTitle(labels(lang)['title'])
        self.setLayoutDirection(
            Qt.LayoutDirection.RightToLeft if str(lang).startswith('he')
            else Qt.LayoutDirection.LeftToRight)
        self.resize(560, 640)
        layout = QVBoxLayout(self)
        self.browser = QTextBrowser()
        self.browser.setHtml(legend_html(lang))
        layout.addWidget(self.browser)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.close)
        layout.addWidget(buttons)


def make_marks_legend_button(parent):
    """A small hidden "?" that opens the legend; shown by sync_marks_legend_button."""
    button = QToolButton(parent)
    button.setText("?")
    button.setVisible(False)
    button._legend = None

    def _open():
        if button._legend is None or button._legend.windowTitle() != labels(_lang())['title']:
            button._legend = MarksLegendDialog(button.window())
        button._legend.show()
        button._legend.raise_()
        button._legend.activateWindow()

    button.clicked.connect(_open)
    return button


def sync_marks_legend_button(button, combo):
    """Show ``button`` iff ``combo``'s current item is a Genuzot transcription.

    ``combo`` is None when the text on screen did not come from the chooser's
    PGP/FGP items (the automatic transcription, a correction): hidden then.
    """
    if button is None:
        return
    data = combo.currentData() if combo is not None else None
    shown = bool(isinstance(data, dict) and data.get('marks_legend'))
    if shown:
        button.setToolTip(labels(_lang())['button_tooltip'])
    button.setVisible(shown)
