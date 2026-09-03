"""Regression tests for search-hit highlighting on the desktop Browse tab.

Two defects, both found 2026-09-03 and both fixed here:

1. The browse render path had NO ``*...*`` -> bold conversion at all. It
   inserted the markers (``browse_render_page``) and then built the HTML with a
   bare ``page_text.replace('\\n', '<br>')``, so a search hit reached the reader
   as literal asterisks -- ``VEKHEN *AMAR / RABI* YEHUDA``. Every other surface
   converts (``ResultDialog._htmlify``, ``HiddenScrollArea``, the web parallels
   page, ``shared/search_engine.py``); Browse was the lone outlier.

2. ``browse_original_page_text`` is captured from the pristine ``pd['text']``
   one line BEFORE the markers are applied, so switching the version selector
   back to V0.8 re-rendered an unmarked snapshot. That is the Browse-side twin
   of the ResultDialog ``_rd_original_marked_text`` fix (sub-issue B,
   2026-09-02), which was never applied here.

Defect 2 was masked by defect 1: with no bold conversion, all that was lost on
switch-back was the literal asterisks.
"""

import inspect
import os
import re
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QTextBrowser

from desktop.result_dialog import ResultDialog
from desktop.widgets import markers_to_bold_html
from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only.


_APP = QApplication.instance() or QApplication([])

# A hit that spans a line break -- the shape docs/OPEN_ISSUES.md records for the
# real-world case ("the exact-mode regex matches the folio text across the line
# break").
PATTERN = r"AMAR\s*\n?\s*RABI"
PAGE = "BERESHIT BARA\nVEKHEN AMAR\nRABI YEHUDA\nOD DEVARIM"


def _mark(text, pattern=PATTERN):
    """What browse_render_page does to the page text before rendering."""
    flags = re.IGNORECASE | re.MULTILINE
    return re.compile(pattern, flags).sub(r'*\g<0>*', text)


def _is_bold_red(widget):
    html = widget.toHtml().replace(" ", "")
    return "color:#ff0000" in html or "color:red" in html


class _BrowseHarness:
    def __init__(self):
        self.browse_text = QTextBrowser()
        self.browse_find_input = type("_I", (), {"text": staticmethod(lambda: "")})()
        self.browse_highlight_pattern = None
        self.browse_highlight_data = []
        for name in ("_browse_display_version_text",
                     "_browse_original_display_text",
                     "_browse_mark_search_hits",
                     "_browse_markers_are_ours"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))


@pytest.fixture
def browse():
    return _BrowseHarness()


# --------------------------------------------------------------------------
# The helper itself
# --------------------------------------------------------------------------

def test_markers_become_red_bold():
    assert markers_to_bold_html("a *hit* b") == "a <b style='color:red;'>hit</b> b"


def test_marker_spanning_a_line_break_pairs_once_the_newline_is_a_br():
    marked = _mark(PAGE)
    assert "*AMAR\nRABI*" in marked, "fixture must produce a hit across the break"
    # The order the render paths use: newline -> <br> FIRST, then the markers.
    converted = markers_to_bold_html(marked.replace("\n", "<br>"))
    assert "<b style='color:red;'>AMAR<br>RABI</b>" in converted
    # ... and the reverse order silently fails, which is why the call order is
    # pinned in the render sites and in the helper's docstring.
    assert "<b" not in markers_to_bold_html(marked)


def test_helper_matches_result_dialog_htmlify_so_the_two_cannot_drift():
    """`_htmlify` step 3 and this helper must produce identical markup."""
    body = "shalom *olam* od"
    via_helper = markers_to_bold_html(body)
    via_dialog = ResultDialog._htmlify(ResultDialog.__new__(ResultDialog), body)
    assert via_helper in via_dialog


def test_empty_input_is_returned_unchanged():
    assert markers_to_bold_html("") == ""
    assert markers_to_bold_html(None) is None


# --------------------------------------------------------------------------
# Defect 1: the browse render showed literal asterisks
# --------------------------------------------------------------------------

def test_version_text_render_shows_bold_not_literal_asterisks(browse):
    browse.browse_highlight_pattern = PATTERN   # marked text implies a search
    browse._browse_display_version_text(_mark(PAGE))
    shown = browse.browse_text.toPlainText()
    assert "*" not in shown, f"literal markers leaked to the reader: {shown!r}"
    assert _is_bold_red(browse.browse_text)


def test_unmarked_text_renders_without_any_bold(browse):
    browse._browse_display_version_text(PAGE)
    assert not _is_bold_red(browse.browse_text)
    assert "VEKHEN AMAR" in browse.browse_text.toPlainText()


def test_browse_render_page_converts_markers():
    """The page render, not just the version switch, must convert."""
    src = inspect.getsource(GenizahGUI.browse_render_page)
    newline_sub = src.index("browse_html_text = page_text.replace('\\n', '<br>')")
    convert = src.index("markers_to_bold_html(browse_html_text)")
    assert newline_sub < convert, (
        "markers_to_bold_html must run AFTER newline->handling, or a hit "
        "spanning a line break never pairs"
    )


# --------------------------------------------------------------------------
# Defect 2: V0.8 switch-back dropped the markers
# --------------------------------------------------------------------------

def test_v08_switch_back_keeps_the_search_hit(browse):
    browse.browse_original_page_text = PAGE           # plain, as captured today
    browse.browse_original_marked_text = _mark(PAGE)  # the new companion
    browse.browse_highlight_pattern = PATTERN         # a switch-back follows a search
    browse._browse_display_version_text(browse._browse_original_display_text())
    assert _is_bold_red(browse.browse_text)


def test_without_the_marked_companion_the_hit_is_lost(browse):
    """Proves the test above can fail -- this is the pre-fix behaviour."""
    browse.browse_original_page_text = PAGE
    browse._browse_display_version_text(browse._browse_original_display_text())
    assert not _is_bold_red(browse.browse_text)


def test_stale_marked_text_degrades_to_plain_rather_than_showing_a_wrong_page(browse):
    """The equality guard: a marked copy from a DIFFERENT page must not win."""
    browse.browse_original_page_text = PAGE
    browse.browse_original_marked_text = _mark("A COMPLETELY OTHER PAGE AMAR\nRABI X")
    assert browse._browse_original_display_text() == PAGE


def test_page_text_containing_a_literal_asterisk_keeps_its_highlight(browse):
    """Codex P2, PR #334 -- this used to degrade to plain, losing the highlight.

    mark_pattern_hits neutralizes the page's own asterisk to a space as it
    inserts markers, so the guard compares marker-stripped against
    asterisk-neutralized. Anything else rejects a perfectly good marked copy.
    """
    from desktop.widgets import mark_pattern_hits

    plain = "SHALOM * OLAM AMAR"
    marked = mark_pattern_hits(plain, "AMAR")
    assert marked == "SHALOM   OLAM *AMAR*"
    browse.browse_original_page_text = plain
    browse.browse_original_marked_text = marked
    assert browse._browse_original_display_text() == marked


def test_a_marked_copy_of_a_DIFFERENT_starred_page_is_still_rejected(browse):
    """Neutralizing must not weaken the staleness guard into accepting anything."""
    from desktop.widgets import mark_pattern_hits

    browse.browse_original_page_text = "SHALOM * OLAM AMAR"
    browse.browse_original_marked_text = mark_pattern_hits(
        "A DIFFERENT * PAGE AMAR", "AMAR")
    assert browse._browse_original_display_text() == "SHALOM * OLAM AMAR"


def test_missing_companion_returns_plain_not_none(browse):
    browse.browse_original_page_text = PAGE
    assert browse._browse_original_display_text() == PAGE


def test_no_page_text_at_all_returns_empty_string(browse):
    assert browse._browse_original_display_text() == ""


def test_browse_render_page_captures_the_marked_text_after_marking():
    src = inspect.getsource(GenizahGUI.browse_render_page)
    plain_capture = src.index("self.browse_original_page_text = pd['text']")
    pattern_sub = src.index(
        "page_text = mark_pattern_hits(page_text, self.browse_highlight_pattern)")
    marked_capture = src.index("self.browse_original_marked_text = page_text")
    assert plain_capture < pattern_sub < marked_capture, (
        "browse_original_marked_text must be captured AFTER the highlight "
        "substitution, or it is just another plain snapshot"
    )


def test_every_v08_fallback_goes_through_the_accessor():
    """No site may re-introduce the plain snapshot for a V0.8 re-render."""
    src = inspect.getsource(GenizahGUI._browse_load_version)
    assert "_browse_display_version_text(self.browse_original_page_text)" not in src
    assert src.count("_browse_original_display_text()") == 4


# ---------------------------------------------------------------------------
# Literal asterisks in the source text (Codex P2, PR #334)
# ---------------------------------------------------------------------------
#
# `*` is the marker. A page carrying one of its own -- a copied footnote
# marker; shared/search_engine.mark_word_highlights fixed the same thing for
# xlsx export in Codex round 6 on PR #325 -- would otherwise pair up with an
# inserted marker and bold the wrong span, strand a stray asterisk, or be
# mistaken for already-marked text and never highlighted at all.

def test_a_literal_asterisk_is_neutralized_before_markers_go_in():
    from desktop.widgets import mark_pattern_hits

    out = mark_pattern_hits("note* here FIND me", "FIND")
    assert out == "note  here *FIND* me", (
        "the page's own asterisk must become a space -- length-preserving, so "
        "nothing downstream shifts -- leaving every remaining * a real marker"
    )
    assert out.count("*") == 2


def test_marker_pairs_stay_correct_with_a_literal_asterisk():
    from desktop.widgets import mark_pattern_hits, markers_to_bold_html

    html = markers_to_bold_html(mark_pattern_hits("a* b FIND c", "FIND"))
    assert html == "a  b <b style='color:red;'>FIND</b> c"
    assert "*" not in html, "no stray asterisk may survive into the render"


def test_text_is_untouched_when_nothing_matched():
    """Only text we actually mark gets neutralized."""
    from desktop.widgets import mark_pattern_hits

    assert mark_pattern_hits("keep my * exactly", "NOPE") == "keep my * exactly"
    assert mark_pattern_hits("keep my * exactly", "") == "keep my * exactly"
    assert mark_pattern_hits("keep my * exactly", "[") == "keep my * exactly"


def test_marker_probe_ignores_a_literal_asterisk():
    from desktop.widgets import text_has_pattern_markers

    # two literal asterisks, but the text between them is not the search term
    assert text_has_pattern_markers("see *note 4* below", "FIND") is False
    assert text_has_pattern_markers("see *FIND* below", "FIND") is True
    assert text_has_pattern_markers("no markers here", "FIND") is False
    assert text_has_pattern_markers("", "FIND") is False
    assert text_has_pattern_markers("*FIND*", "") is False
    assert text_has_pattern_markers("*FIND*", "[") is False


def test_a_page_with_a_literal_asterisk_still_gets_highlighted():
    """The whole point: the guard must not mistake source text for markers."""
    from desktop.widgets import mark_pattern_hits, text_has_pattern_markers

    page = "footnote* and the term FIND appears"
    assert text_has_pattern_markers(page, "FIND") is False
    assert "*FIND*" in mark_pattern_hits(page, "FIND")


# ---------------------------------------------------------------------------
# Outside a search, an asterisk is the page's own (Codex P2, PR #334 round 5)
# ---------------------------------------------------------------------------
#
# The Browse render paths converted every `*...*` pair unconditionally, so an
# editorial `*note*` in a PGP edition -- 9 of 7,112 transcriptions carry a
# pair -- had its stars deleted and its word shown as a red search hit that
# nobody searched for.

STARRED = "SHALOM *note* OLAM"


def test_an_editorial_note_is_not_bolded_outside_a_search(browse):
    browse.browse_highlight_pattern = None
    browse.browse_highlight_data = []
    browse._browse_display_version_text(STARRED)
    shown = browse.browse_text.toPlainText()
    assert not _is_bold_red(browse.browse_text), (
        "nothing inserted these asterisks; they are source text"
    )
    assert "*note*" in shown, "the page's own stars must survive to the reader"


def test_the_predicate_is_a_search_context_test(browse):
    browse.browse_highlight_pattern = None
    browse.browse_highlight_data = []
    assert browse._browse_markers_are_ours(_mark(PAGE)) is False
    assert browse._browse_markers_are_ours("no stars at all") is False
    assert browse._browse_markers_are_ours("") is False

    browse.browse_highlight_pattern = PATTERN
    assert browse._browse_markers_are_ours(_mark(PAGE)) is True
    assert browse._browse_markers_are_ours("no stars at all") is False, (
        "no asterisk, nothing to convert -- the context alone is not enough"
    )

    # span markers carry no pattern of their own, so the data list must count
    browse.browse_highlight_pattern = None
    browse.browse_highlight_data = [{"uid": "u", "span": [0, 4]}]
    assert browse._browse_markers_are_ours("*ABCD* rest") is True
