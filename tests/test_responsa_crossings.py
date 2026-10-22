"""Matches across a page break: plan §8 gate 4 (Responsa Phase 1, whole-manuscript docs;
Literal's word windows). In Phase 2 the same cases run against seam documents.

Each manuscript of the fixture is its pages, in physical order, joined into one whole-
manuscript document as the indexer joins them (tests/responsa_fixture.py).
"""
import pytest

tantivy = pytest.importorskip("tantivy")

import shared.search_engine as se  # noqa: E402
from shared.variants import VariantManager  # noqa: E402
from tests.responsa_fixture import build_main, make_engine, release  # noqa: E402

R = {"responsa_mode": True}
MANUSCRIPTS = {
    "990101": [("a1", "פתיחה כהנים"), ("a2", "גדולים סוף")],                     # a plain crossing
    "990102": [("b1", "פתיחה הכהנים"), ("b2", "גדולים סוף")],                    # the first word goes on before
    "990103": [("c1", "פתיחה כהנים"), ("c2", "גדוליםא סוף")],                    # the last word goes on after
    "990104": [("d1", "פתיחה כהנים רבים"), ("d2", "גדולים סוף")],                 # [1] across the break
    "990105": [("e1", "פתיחה גדולים"), ("e2", "כהנים סוף")],                     # the reverse order
    "990106": [("f1", "פתיחה כהנים"), ("f2", "עמדו"), ("f3", "גדולים סוף")],      # over two breaks
    "990107": [("g1", "פתיחה של"), ("g2", "ום סוף")],                           # one word, flex spacing
}


@pytest.fixture(scope="module")
def eng(tmp_path_factory):
    root = tmp_path_factory.mktemp("responsa_cross")
    build_main(root, MANUSCRIPTS)
    e = make_engine(root)
    yield e
    release(e)


def _crossings(eng, query, **opts):
    rows = eng.execute_search(query, "literal", 0, responsa_options=dict(R, **opts), corpus_scope="genizah")
    return {r["uid"]: r for r in rows if r.get("cross_page")}


def test_a_crossing_is_found_and_marked_on_both_pages(eng):
    row = _crossings(eng, "כהנים גדולים")["a1"]
    assert "*כהנים\nגדולים*" in row["raw_file_hl"]
    assert [(h["uid"], h["span"]) for h in row["page_highlights"]] == [("a1", (6, 12)), ("a2", (0, 6))]


def test_both_edges_of_a_crossing_are_whole_words(eng):
    found = _crossings(eng, "כהנים גדולים")
    assert "b1" not in found          # הכהנים: the first word began before
    assert "c1" not in found          # גדוליםא: the last word goes on


def test_a_per_pair_gap_holds_across_the_break(eng):
    assert "d1" in _crossings(eng, "כהנים [1] גדולים")
    assert "d1" not in _crossings(eng, "כהנים גדולים")


def test_the_reverse_order_crosses_too(eng):
    assert "e1" in _crossings(eng, "כהנים גדולים", bidirectional=True)
    assert "e1" not in _crossings(eng, "כהנים גדולים")


def test_a_span_over_two_breaks(eng):
    row = _crossings(eng, "כהנים [1] גדולים")["f1"]
    assert [h["uid"] for h in row["page_highlights"]] == ["f1", "f2", "f3"]


def test_one_flex_spaced_word_crosses_a_break(eng):
    # של | ום: with flex spacing שלום spans the break, so the lone word keeps the whole-
    # manuscript docs (Codex review of PR #432); without it, it is two words.
    row = _crossings(eng, "שלום", flex_spacing=True)["g1"]
    assert [h["uid"] for h in row["page_highlights"]] == ["g1", "g2"]
    assert "g1" not in _crossings(eng, "שלום")


# --- Literal's word windows (_cross_page_spans): the joiner between windows -------------

def test_a_match_beside_the_window_joiner_is_a_whole_word():
    """Gap 0 joins every break's window with ² (U+00B2) and runs one pass: the joiner is
    no letter and is not looked past, so a window that ends or begins at a word keeps it
    whole, and a match never reaches across two windows."""
    e = se.SearchEngine.__new__(se.SearchEngine)
    e.var_mgr = VariantManager()
    w1, w2 = "תעודדו", "תענגו"
    pages = ["עמוד " + w1, w2 + " עמוד " + w1, w2, "עמוד"]
    text, bounds, cur = [], [], 0
    for i, p in enumerate(pages):
        start = cur
        text.append(p)
        cur += len(p)
        if i != len(pages) - 1:
            text.append("\n")
            cur += 1
        bounds.append({"uid": f"p{i + 1}", "start": start, "end": cur})
    content = "".join(text)
    rx = e.build_regex_pattern([w1, w2], "literal", 0)
    spans = e._cross_page_spans(rx, content, bounds, w1, w2, 1, 0, True, whole_words=True)
    assert [content[s:t] for s, t in spans] == [f"{w1}\n{w2}", f"{w1}\n{w2}"]
    assert not se._continues_word(se._WINDOW_JOINER) and not se._looked_past(se._WINDOW_JOINER)
