"""The desktop's "?" beside a Genuzot transcription (GS-274 option A)."""

import os
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QComboBox, QTextBrowser

from desktop.marks_legend import MarksLegendDialog, make_marks_legend_button, sync_marks_legend_button
from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])

GENUZOT = {"source": "fgp", "is_fgp": True, "id": 7, "uid": "fgp:7",
           "doc_relation": "Digital Edition", "language": "Hebrew",
           "content": "ראשון <רא>\n?ב?רוך", "source_credit_he": "צוות FGP להעתקות -גנוזות",
           "source_credit_en": "FGP Transcriptions Team - Genuzos"}
OTHER = dict(GENUZOT, id=8, uid="fgp:8", content="טקסט אחר",
             source_credit_he="חיים מיליקובסקי, ראש צוות FGP למדרשי אגדה",
             source_credit_en="Haim Milikowsky")


def _combo_with(*items):
    combo = QComboBox()
    combo.addItem("V0.8", {"source": "original"})
    for label, data in items:
        combo.addItem(label, data)
    return combo


def test_the_button_follows_the_chooser():
    button = make_marks_legend_button(None)
    combo = _combo_with(("g", {"source": "fgp_edition", "marks_legend": True}),
                        ("o", {"source": "fgp_edition", "marks_legend": False}))
    combo.setCurrentIndex(1)
    sync_marks_legend_button(button, combo)
    assert not button.isHidden()
    combo.setCurrentIndex(2)
    sync_marks_legend_button(button, combo)
    assert button.isHidden()
    combo.setCurrentIndex(1)
    sync_marks_legend_button(button, combo)
    sync_marks_legend_button(button, None)   # V0.8 or a correction drawn
    assert button.isHidden()
    sync_marks_legend_button(None, combo)    # no button: nothing to do


def test_the_chooser_tags_only_genuzot_transcriptions():
    """`_populate_pgp_combo` is where both windows' items come from."""
    gui = type("_G", (), {})()
    gui._browse_full_htr_text = lambda sid: ""
    gui._populate_pgp_combo = MethodType(GenizahGUI._populate_pgp_combo, gui)
    combo = QComboBox()
    gui._populate_pgp_combo(combo, [GENUZOT, OTHER], {}, htr_text="", sys_id="99")
    tagged = {combo.itemData(i).get("uid"): combo.itemData(i).get("marks_legend")
              for i in range(combo.count())
              if (combo.itemData(i) or {}).get("source") == "fgp_edition"}
    assert tagged == {"fgp:7": True, "fgp:8": False}


def test_browse_shows_the_button_for_a_genuzot_text_and_hides_it_for_v08():
    b = type("_B", (), {})()
    b.browse_text = QTextBrowser()
    b.browse_highlight_pattern = None
    b.browse_highlight_data = None
    b.browse_find_input = type("_I", (), {"text": staticmethod(lambda: "")})()
    b.btn_b_marks_legend = make_marks_legend_button(None)
    b.browse_version_combo = _combo_with(("g", {"source": "fgp_edition", "marks_legend": True}))
    for name in ("_browse_mark_search_hits", "_browse_markers_are_ours",
                 "_browse_display_version_text", "_browse_display_pgp_text"):
        setattr(b, name, MethodType(getattr(GenizahGUI, name), b))
    b.browse_version_combo.setCurrentIndex(1)
    b._browse_display_pgp_text(GENUZOT["content"])
    assert not b.btn_b_marks_legend.isHidden()
    assert b.browse_text.toPlainText() == GENUZOT["content"]
    b._browse_display_version_text("V0.8 text")
    assert b.btn_b_marks_legend.isHidden()


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_dialog_shows_the_marks_as_typed(monkeypatch, lang):
    import genizah_core
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    dialog = MarksLegendDialog()
    text = dialog.browser.toPlainText()
    for form in ("<…>", "&…&", "$…$", "?א/ב?", "{א//ב}"):
        assert form in text
    assert ("סימני התעתוק" if lang == "he" else "Transcription marks") == dialog.windowTitle()
