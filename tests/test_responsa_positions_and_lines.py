"""Responsa searches with a text position, and line-break queries with flex spacing
(Codex review of PR #432). A small real index: tests/responsa_fixture.py.
"""
import pytest

tantivy = pytest.importorskip("tantivy")

from shared.responsa import _parse_line_break_query  # noqa: E402
from shared.responsa_match import compile_line_groups  # noqa: E402
from tests.responsa_fixture import build_main, make_engine, release  # noqa: E402

R = {"responsa_mode": True}
PAGES = {
    "990301": [("s1", "שלום, אחר כאן")],           # starts the page, with its comma
    "990302": [("s2", "כאן אחר של[ו]ם")],           # ends the page, bracketed
    "990303": [("s3", "פתיחה כאן\nשלום. אחר")],     # starts a line, with its stop
    "990304": [("s4", "כאן שלום אחר")],             # in the middle: no position holds
    "990305": [("s5", "פתיחה של ום\nכאן סוף")],     # שלום split on its line, כאן on the next
    "990306": [("s6", "פתיחה של\nום\nכאן")],        # split over a line break, כאן after it
    "990307": [("s7", "פתיחה\nשלום, עליכם כאן")],   # a phrase opening a line
    "990308": [("s8", "פתיחה\nעליכם שלום כאן")],    # ... in the reverse order
    "990309": [("s9", 'כאן אחר "שלום')],            # ends the page, a quote before it
    "990310": [("s10", "פתיחה\n] שלום כאן")],       # a lacuna opens the line: "]" is its token
    "990311": [("s11", "כאן שלום [\nסוף")],         # ... and closes one
}


@pytest.fixture(scope="module")
def eng(tmp_path_factory):
    root = tmp_path_factory.mktemp("responsa_positions")
    build_main(root, PAGES)
    e = make_engine(root)
    yield e
    release(e)


def _uids(eng, query, text_position=None, **opts):
    rows = eng.execute_search(query, "literal", 0, responsa_options=dict(R, **opts),
                              text_position=text_position, corpus_scope="genizah")
    return {r["uid"] for r in rows}


@pytest.mark.parametrize("position, expected", [
    ("start", {"s1"}),
    ("end", {"s2", "s9"}),
    ("line_start", {"s1", "s3", "s7", "s10"}),
    ("line_end", {"s2", "s9", "s11"}),
])
def test_a_position_search_finds_the_word_with_its_punctuation_or_brackets(eng, position, expected):
    # The position fields hold whitespace tokens: שלום, and של[ו]ם are not שלום there.
    assert _uids(eng, "שלום", position) == expected


def test_only_the_edge_word_of_a_phrase_is_looked_up_at_the_position(eng):
    # עליכם is no line's first token: only שלום, which opens the match, must be one.
    assert _uids(eng, "שלום עליכם", "line_start") == {"s7"}
    # Either order: the edge word is either one.
    assert _uids(eng, "שלום עליכם", "line_start", bidirectional=True) == {"s7", "s8"}


def test_flex_spacing_holds_in_a_line_query(eng):
    # של ום on its line is שלום with flex spacing. A word does not run over the line
    # break itself: | means the next line.
    assert _uids(eng, "שלום | כאן", flex_spacing=True) == {"s5"}
    assert _uids(eng, "שלום | כאן") == set()


def test_flex_spacing_joins_the_words_of_a_line():
    def check(**opts):
        groups, gaps = _parse_line_break_query("שלום עולם | כאן")
        return compile_line_groups(groups, gaps, dict(R, **opts), None).compile_check()
    assert check(flex_spacing=True).search("שלוםעולם\nכאן") is not None
    assert check().search("שלוםעולם\nכאן") is None
    assert check(flex_spacing=True).search("שלום\nעולם\nכאן") is None   # still one line
