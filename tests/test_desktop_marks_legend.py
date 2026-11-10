"""The desktop's "?" beside a Genuzot transcription (GS-274 option A)."""

import ast
import inspect
import os
import sys
from pathlib import Path
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QComboBox, QTextBrowser

from desktop.marks_legend import MarksLegendDialog, make_marks_legend_button, sync_marks_legend_button
from desktop.result_dialog import ResultDialog
from genizah_app import GenizahGUI
from genizah_app import tr as genizah_app_tr

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])

REPO = Path(__file__).resolve().parent.parent

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


def test_the_chooser_shows_no_text_of_a_transcription_and_numbers_alike_entries():
    """Owner, 2026-10-27: the chooser named each entry by the transcription's opening words.
    Now there is no text of it at all; entries that would read alike are numbered."""
    gui = type("_G", (), {})()
    gui._browse_full_htr_text = lambda sid: ""
    gui._populate_pgp_combo = MethodType(GenizahGUI._populate_pgp_combo, gui)

    def fgp_labels(sources):
        combo = QComboBox()
        gui._populate_pgp_combo(combo, sources, {}, htr_text="", sys_id="99")
        return [combo.itemText(i).strip() for i in range(combo.count())
                if (combo.itemData(i) or {}).get("source") == "fgp_edition"]

    transcription = genizah_app_tr("FGP Transcription")
    assert fgp_labels([GENUZOT]) == [transcription]
    assert fgp_labels([GENUZOT, OTHER]) == [f"{transcription} 1", f"{transcription} 2"]


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


def test_cancelling_an_edit_of_a_genuzot_text_keeps_the_viewer_legend():
    """Codex round 2: the viewer restored the text through _rd_display_text, which
    assumes V0.8 and hid the "?" while the chooser still held the Genuzot item."""
    d = type("_D", (), {})()
    d.text_ms = QTextBrowser()
    d.data = {"highlight_pattern": None}
    d.find_ms_input = type("_I", (), {"text": staticmethod(lambda: "")})()
    d.btn_rd_marks_legend = make_marks_legend_button(None)
    d.rd_version_combo = _combo_with(("g", {"source": "fgp_edition", "marks_legend": True}))
    d.rd_version_combo.setCurrentIndex(1)
    d._rd_exit_edit_mode = lambda: None
    for name in ("_mark_search_hits", "_htmlify", "_refresh_find_highlights", "_rd_display_text",
                 "_rd_display_pgp_text", "_rd_cancel_edit"):
        setattr(d, name, MethodType(getattr(ResultDialog, name), d))
    d._rd_display_pgp_text(GENUZOT["content"])
    assert not d.btn_rd_marks_legend.isHidden()
    d._rd_original_edit_text = GENUZOT["content"]
    d._rd_cancel_edit()
    assert not d.btn_rd_marks_legend.isHidden()
    assert d.text_ms.toPlainText() == GENUZOT["content"]


_PANE_WRITES = {"setHtml", "setText", "setPlainText", "clear", "append", "insertHtml", "insertPlainText"}
# These two only write the empty first content, before the button exists.
_BUILDS_THE_PANE = {"create_browse_tab", "init_ui"}


def _pane_writers_without_sync(path, pane):
    tree = ast.parse((REPO / path).read_text(encoding="utf-8"))
    missing = []
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) or fn.name in _BUILDS_THE_PANE:
            continue
        writes = syncs = False
        for node in ast.walk(fn):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            name = getattr(f, "attr", getattr(f, "id", None))
            syncs |= name == "sync_marks_legend_button"
            writes |= (isinstance(f, ast.Attribute) and f.attr in _PANE_WRITES
                       and isinstance(f.value, ast.Attribute) and f.value.attr == pane)
            writes |= (name == "apply_line_numbered_text" and bool(node.args)
                       and isinstance(node.args[0], ast.Attribute) and node.args[0].attr == pane)
        if writes and not syncs:
            missing.append(f"{path}:{fn.lineno} {fn.name}")
    return missing


def test_every_function_that_writes_the_text_pane_settles_the_legend_button():
    """Codex round 1: a LOCAL page and the Reading Desk replaced a Genuzot text and left
    its "?" up. Whatever writes the pane says whether the "?" belongs to what it wrote."""
    missing = (_pane_writers_without_sync("genizah_app.py", "browse_text")
               + _pane_writers_without_sync("desktop/result_dialog.py", "text_ms"))
    assert not missing, "call sync_marks_legend_button after writing the pane:\n" + "\n".join(missing)


def test_the_pane_guard_can_fail(tmp_path, monkeypatch):
    (tmp_path / "probe.py").write_text(
        "def shows(self):\n    self.browse_text.setHtml('x')\n"
        "def settles(self):\n    self.browse_text.setHtml('x')\n    sync_marks_legend_button(None, None)\n",
        encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "REPO", tmp_path)
    assert _pane_writers_without_sync("probe.py", "browse_text") == ["probe.py:1 shows"]


def test_the_similarity_preview_shows_its_text_as_plain_text():
    """Codex round 1: the preview's QLabel guessed rich text, so `<...>` in a page vanished."""
    src = inspect.getsource(GenizahGUI._show_vs_dialog)
    plain = src.index("_lbl.setTextFormat(Qt.TextFormat.PlainText)")
    assert plain < src.index("_lbl.setText(snippet)")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_dialog_shows_the_marks_as_typed(monkeypatch, lang):
    import genizah_core
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    dialog = MarksLegendDialog()
    text = dialog.browser.toPlainText()
    for form in ("<…>", "&…&", "$…$", "?א/ב?", "{א//ב}"):
        assert form in text
    assert ("סימני התעתוק" if lang == "he" else "Transcription marks") == dialog.windowTitle()
