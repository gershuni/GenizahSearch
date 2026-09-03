r"""Every re-render of the manuscript pane must keep the search-hit highlight.

Owner UAT 2026-09-03: searched `תקום רבה דיניך` (Exact, gap 0) on
MS heb. g.2/27 and the reading pane showed the V0.8 text with no highlight,
while the results-table snippet highlighted it correctly.

The regex was never the problem -- the search separator `[^\w֐-׿']+`
matches the `\n` between `רבה` and `דיניך`, and the page text is exactly
`תקום רבה\nדיניך` with no hidden marks (checked against Transcriptions.txt).
What was fragile is that keeping the highlight across a re-render depended on a
*marked* copy of the text having been captured earlier and still being
reachable, and several branches did not satisfy that:

  - `_rd_load_version_content`'s `original` fallback renders the deliberately
    plain `_rd_original_text` when the versions cache was never seeded, and
    `_rd_load_versions` used to return BEFORE seeding it whenever the app had
    no `corrections_client`;
  - `_rd_display_pgp_text` applied no highlighting at all, even though SEED-033
    deliberately prefers the source that CONTAINS the searched phrase.

The fix makes both display helpers re-derive the markers from the result's own
`highlight_pattern`, so no snapshot-capture ordering can lose the highlight.
"""

import os
import re
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QTextBrowser

from desktop.result_dialog import ResultDialog
from desktop.widgets import mark_pattern_hits
from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui


_APP = QApplication.instance() or QApplication([])

# Verbatim from Transcriptions.txt, page
# 990053489970205171_IE168181472_P000002_FL168181475 -- the folio in the report.
PAGE = (
    "רוצצנו במחש כי קברים הוציאנו מעברותיוו\n"
    "מודררים ייי פוקח עוזרים . שאפנו והיו\n"
    "שאופים . ביטה בנפילת רחופים . זוקף\n"
    "כפופים. תקותינו וסברנו בך סלחנא כדרכ\n"
    "טובך י צבאות אשרי אדם בוטח כך .\n"
    "קדושי\n"
    "שי\n"
    "תקום רבה\n"
    "דיניך וכהדרי אלצדקת עיניך ולבך מביט\n"
    "ועיניך וג במקום אחד\n"
    "ארפן"
)

# What SearchEngine.build_regex_pattern produces for these three terms at
# gap 0: each term its own group, joined by the non-word separator.
SEP = r"[^\w֐-׿\']+"
PATTERN = SEP.join(["(תקום)", "(רבה)", "(דיניך)"])


def _is_red(widget):
    html = widget.toHtml().replace(" ", "")
    return "color:#ff0000" in html or "color:red" in html


# --------------------------------------------------------------------------
# The premise: the pattern really does span the line break
# --------------------------------------------------------------------------

def test_the_search_pattern_matches_across_the_line_break():
    m = re.compile(PATTERN, re.IGNORECASE).search(PAGE)
    assert m is not None
    assert m.group(0) == "תקום רבה\nדיניך", (
        "the separator must swallow the newline, or nothing below is meaningful"
    )


def test_mark_pattern_hits_wraps_the_whole_span():
    marked = mark_pattern_hits(PAGE, PATTERN)
    assert "*תקום רבה\nדיניך*" in marked


def test_mark_pattern_hits_is_inert_without_a_pattern_or_match():
    assert mark_pattern_hits(PAGE, None) == PAGE
    assert mark_pattern_hits(PAGE, "") == PAGE
    assert mark_pattern_hits(PAGE, "(זזזזז)") == PAGE
    assert mark_pattern_hits(PAGE, "(unclosed") == PAGE, "a bad regex must not raise"


# --------------------------------------------------------------------------
# ResultDialog
# --------------------------------------------------------------------------

class _DialogHarness:
    def __init__(self, pattern=PATTERN):
        self.text_ms = QTextBrowser()
        self.data = {"highlight_pattern": pattern}
        self.find_ms_input = type("_I", (), {"text": staticmethod(lambda: "")})()
        for name in ("_mark_search_hits", "_rd_display_text", "_rd_display_pgp_text",
                     "_htmlify", "_refresh_find_highlights"):
            setattr(self, name, MethodType(getattr(ResultDialog, name), self))


def test_plain_v08_text_is_re_highlighted_on_render():
    """The reported case: the fallback renders the PLAIN snapshot."""
    d = _DialogHarness()
    d._rd_display_text(PAGE)          # exactly what the cache-miss branch passes
    assert _is_red(d.text_ms)
    assert "*" not in d.text_ms.toPlainText()


def test_already_marked_text_is_not_double_marked():
    d = _DialogHarness()
    d._rd_display_text(mark_pattern_hits(PAGE, PATTERN))
    assert _is_red(d.text_ms)
    assert "*" not in d.text_ms.toPlainText()


def test_a_result_with_no_pattern_renders_plain():
    """Opened from Browse rather than a search -- nothing to highlight."""
    d = _DialogHarness(pattern=None)
    d._rd_display_text(PAGE)
    assert not _is_red(d.text_ms)
    assert "תקום רבה" in d.text_ms.toPlainText()


def test_pgp_edition_containing_the_phrase_is_highlighted():
    """SEED-033 picks the source that CONTAINS the phrase; it must show it."""
    d = _DialogHarness()
    d._rd_display_pgp_text("פירוש קדום\nתקום רבה דיניך\nוכהדרי", is_rtl=True)
    assert _is_red(d.text_ms)
    assert "*" not in d.text_ms.toPlainText()


def test_pgp_edition_without_the_phrase_renders_plain():
    d = _DialogHarness()
    d._rd_display_pgp_text("edition text that does not contain it", is_rtl=True)
    assert not _is_red(d.text_ms)


def test_english_translation_is_untouched():
    d = _DialogHarness()
    d._rd_display_pgp_text("An English translation of the piyyut.", is_rtl=False)
    assert not _is_red(d.text_ms)
    assert "English translation" in d.text_ms.toPlainText()


def test_versions_cache_is_seeded_before_the_corrections_client_guard():
    """`_rd_load_versions` used to return before seeding when there was no client."""
    import inspect
    src = inspect.getsource(ResultDialog._rd_load_versions)
    seed = src.index("self._rd_versions_cache = {")
    guard = src.index("hasattr(parent, 'corrections_client')")
    assert seed < guard, (
        "seed the 'original' cache entry BEFORE any early return, or a switch "
        "back to V0.8 falls through to the plain _rd_original_text"
    )


# --------------------------------------------------------------------------
# Browse tab
# --------------------------------------------------------------------------

class _BrowseHarness:
    def __init__(self, pattern=PATTERN):
        self.browse_text = QTextBrowser()
        self.browse_highlight_pattern = pattern
        self.browse_find_input = type("_I", (), {"text": staticmethod(lambda: "")})()
        for name in ("_browse_mark_search_hits", "_browse_display_version_text",
                     "_browse_display_pgp_text"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))


def test_browse_version_text_is_re_highlighted():
    b = _BrowseHarness()
    b._browse_display_version_text(PAGE)
    assert _is_red(b.browse_text)
    assert "*" not in b.browse_text.toPlainText()


def test_browse_pgp_edition_containing_the_phrase_is_highlighted():
    b = _BrowseHarness()
    b._browse_display_pgp_text("פירוש\nתקום רבה דיניך\nסוף", is_rtl=True)
    assert _is_red(b.browse_text)
    assert "*" not in b.browse_text.toPlainText()


def test_browse_without_a_search_pattern_renders_plain():
    b = _BrowseHarness(pattern=None)
    b._browse_display_version_text(PAGE)
    assert not _is_red(b.browse_text)
    assert "תקום רבה" in b.browse_text.toPlainText()
