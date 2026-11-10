"""The desktop must show an edition's `<...>` marks, not drop them.

Found 2026-10-26 while assessing how to show FGP's transcription marks
(GS-274): the PGP/FGP display paths passed the text to ``setHtml`` unescaped,
so Qt took every ``<...>`` for a tag and removed it with its contents.
``דמנח אפתורא <ד'>`` showed as ``דמנח אפתורא``, and a PGP editor's added
letter vanished from inside the word -- ``מיב<מ>נא`` read ``מיבנא``. Measured
then: 9,729 of the FGP rows and 315 of 36,642 PGP transcriptions carry one.
The website escapes (``web/components/typography.py``) and always showed them.

The Browse page render, its version text and the View All / full-manuscript
views had the same unescaped ``\\n`` -> ``<br>`` step; all now go through
``desktop.widgets.text_lines_to_html``.
"""

import ast
import os
from pathlib import Path
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QTextBrowser

from desktop.result_dialog import ResultDialog
from desktop.widgets import text_lines_to_html
from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])

REPO = Path(__file__).resolve().parent.parent

# Real texts: an FGP line filler, a PGP added letter, an `<(ה)>` restoration,
# and an editorial `<<...>>` from a PGP English source.
FGP_LINE = "דמנח אפתורא <ד'>"
PGP_WORD = "ושלום מיב<מ>נא סוף"
PGP_RESTORED = "ואמר <(ה)> לו"
PGP_ENGLISH = "of the season. <<I wanted you to know this.>> It is better"

# What SearchEngine.build_regex_pattern produces for `מיבמנא אחד`-style
# terms is irrelevant here: any pattern that hits next to a mark will do.
PATTERN = r"(שלום)"


def _is_red(widget):
    html = widget.toHtml().replace(" ", "")
    return "color:#ff0000" in html or "color:red" in html


class _DialogHarness:
    def __init__(self, pattern=None):
        self.text_ms = QTextBrowser()
        self.data = {"highlight_pattern": pattern}
        self.find_ms_input = type("_I", (), {"text": staticmethod(lambda: "")})()
        for name in ("_mark_search_hits", "_rd_display_pgp_text", "_refresh_find_highlights"):
            setattr(self, name, MethodType(getattr(ResultDialog, name), self))


class _BrowseHarness:
    def __init__(self, pattern=None):
        self.browse_text = QTextBrowser()
        self.browse_highlight_pattern = pattern
        self.browse_highlight_data = None
        self.browse_find_input = type("_I", (), {"text": staticmethod(lambda: "")})()
        for name in ("_browse_mark_search_hits", "_browse_markers_are_ours",
                     "_browse_display_version_text", "_browse_display_pgp_text"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))


@pytest.mark.parametrize("text", [FGP_LINE, PGP_WORD, PGP_RESTORED, PGP_ENGLISH])
def test_result_dialog_edition_keeps_its_marks(text):
    d = _DialogHarness()
    d._rd_display_pgp_text(text, is_rtl=True)
    assert d.text_ms.toPlainText() == text


@pytest.mark.parametrize("text", [FGP_LINE, PGP_WORD, PGP_RESTORED, PGP_ENGLISH])
def test_browse_edition_keeps_its_marks(text):
    b = _BrowseHarness()
    b._browse_display_pgp_text(text, is_rtl=True)
    assert b.browse_text.toPlainText() == text


def test_browse_version_text_keeps_a_typed_mark():
    """A community correction is typed by a reader and can carry `<...>`."""
    b = _BrowseHarness()
    b._browse_display_version_text(PGP_WORD)
    assert b.browse_text.toPlainText() == PGP_WORD


def test_a_search_hit_next_to_a_mark_is_red_and_the_mark_stays():
    d = _DialogHarness(pattern=PATTERN)
    d._rd_display_pgp_text(PGP_WORD, is_rtl=True)
    assert _is_red(d.text_ms)
    assert d.text_ms.toPlainText() == PGP_WORD

    b = _BrowseHarness(pattern=PATTERN)
    b._browse_display_pgp_text(PGP_WORD, is_rtl=True)
    assert _is_red(b.browse_text)
    assert b.browse_text.toPlainText() == PGP_WORD


def test_entities_and_line_breaks():
    """The display shows what it is given, `&deg;` included; lines still break.

    FGP's own stored entities are decoded earlier, where the row is read
    (`shared.fgp_service.decode_stored_entities`, GS-60), so this layer never
    decodes: a text that really contains `&deg;` keeps it.
    """
    assert text_lines_to_html("a &deg; b") == "a &amp;deg; b"
    assert text_lines_to_html("א\n<ב>") == "א<br>&lt;ב&gt;"
    assert text_lines_to_html("") == ""
    b = _BrowseHarness()
    b._browse_display_pgp_text("1 &deg;\n2", is_rtl=True)
    assert b.browse_text.toPlainText() == "1 &deg;\n2"


# --------------------------------------------------------------------------
# Every other `\n` -> `<br>` render site escapes first
# --------------------------------------------------------------------------

def _is_escape_call(node):
    return (isinstance(node, ast.Call)
            and getattr(node.func, "attr", getattr(node.func, "id", None)) == "escape")


def _unescaped_br_sites(path):
    """`X.replace('\\n', '<br>')` where X is neither an escape call nor a
    name assigned from one in the same function."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    bad = []
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        escaped_names = {
            t.id
            for node in ast.walk(fn) if isinstance(node, ast.Assign) and _is_escape_call(node.value)
            for t in node.targets if isinstance(t, ast.Name)
        }
        for node in ast.walk(fn):
            if not (isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "replace"
                    and len(node.args) == 2
                    and all(isinstance(a, ast.Constant) for a in node.args)
                    and node.args[0].value == "\n" and node.args[1].value == "<br>"):
                continue
            receiver = node.func.value
            if _is_escape_call(receiver):
                continue
            if isinstance(receiver, ast.Name) and receiver.id in escaped_names:
                continue
            bad.append(f"{path.relative_to(REPO)}:{node.lineno} in {fn.name}")
    return sorted(set(bad))


def test_no_desktop_render_site_turns_unescaped_text_into_html():
    files = [REPO / "genizah_app.py", *sorted((REPO / "desktop").rglob("*.py"))]
    bad = [site for f in files for site in _unescaped_br_sites(f)]
    assert not bad, (
        "these build HTML from text without escaping it, so Qt drops every "
        "`<...>` in the text; use desktop.widgets.text_lines_to_html:\n" + "\n".join(bad)
    )


def test_the_site_guard_can_fail(tmp_path):
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import html\n"
        "def bad(text):\n"
        "    return text.replace('\\n', '<br>')\n"
        "def good(text):\n"
        "    t = html.escape(text)\n"
        "    return t.replace('\\n', '<br>'), html.escape(text).replace('\\n', '<br>')\n",
        encoding="utf-8",
    )
    global REPO
    saved, REPO = REPO, tmp_path
    try:
        assert _unescaped_br_sites(probe) == ["probe.py:3 in bad"]
    finally:
        REPO = saved
