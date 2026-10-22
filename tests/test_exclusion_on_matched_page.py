"""Excluded words read the page the match is on (owner, 2026-10-21; decision 7 of
docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md), in every mode: a Responsa `-word` and the
"Exclude Words" box of Exact, Variants and Fuzzy. The letters count anywhere inside a word,
as before; what changed is where they are looked for -- never the rest of the manuscript.

Before: a row found through its whole-manuscript document was checked against that whole
document, so `-אהרן` hid a row whose page never held אהרן.
"""
import pytest

tantivy = pytest.importorskip("tantivy")

from tests.responsa_fixture import build_local, build_main, make_engine, release  # noqa: E402

MANUSCRIPTS = {
    "990201": [("other_page_p1", "כהנים גדולים עמדו"), ("other_page_p2", "אהרן הלך")],
    "990202": [("same_page", "כהנים גדולים ואהרן")],
    "990203": [("inside_word", "כהנים גדולים ולאלאהרן")],
    "990204": [("cross_ok_p1", "פתיחה כהנים"), ("cross_ok_p2", "גדולים סוף"), ("cross_ok_p3", "אהרן")],
    "990205": [("cross_hit_p1", "פתיחה כהנים"), ("cross_hit_p2", "גדולים ואהרן")],
    "990206": [("and_p1", "כהנים כאן"), ("and_p2", "גדולים שם"), ("and_p3", "אהרן")],
}
LOCAL_PAGES = [("local_clean", "כהנים גדולים עמדו"), ("local_hit", "כהנים גדולים ואהרן")]


@pytest.fixture(scope="module")
def eng(tmp_path_factory):
    root = tmp_path_factory.mktemp("exclusion")
    build_main(root, MANUSCRIPTS)
    local_dir = str(root / "LocalIndex")
    build_local(local_dir, LOCAL_PAGES)
    e = make_engine(root, local_dir)
    yield e
    release(e)


def _uids(rows):
    return {r["uid"] for r in rows}


def _responsa(eng, query, scope="genizah"):
    return _uids(eng.execute_search(query, "literal", 0, responsa_options={"responsa_mode": True},
                                    corpus_scope=scope))


def _exact(eng, query, words, scope="genizah", **kw):
    return _uids(eng.execute_search(query, "literal", 0, exclude_words=words, corpus_scope=scope, **kw))


@pytest.mark.parametrize("search", ["responsa", "exact"])
def test_excluded_letters_on_another_page_of_the_manuscript_do_not_hide_the_row(eng, search):
    got = (_responsa(eng, "כהנים גדולים -אהרן") if search == "responsa"
           else _exact(eng, "כהנים גדולים", ["אהרן"]))
    assert "other_page_p1" in got


@pytest.mark.parametrize("search", ["responsa", "exact"])
def test_excluded_letters_on_the_matched_page_hide_it_anywhere_inside_a_word(eng, search):
    got = (_responsa(eng, "כהנים גדולים -אהרן") if search == "responsa"
           else _exact(eng, "כהנים גדולים", ["אהרן"]))
    assert "same_page" not in got
    assert "inside_word" not in got


@pytest.mark.parametrize("search", ["responsa", "exact"])
def test_a_crossing_row_reads_the_pages_its_span_covers(eng, search):
    got = (_responsa(eng, "כהנים גדולים -אהרן") if search == "responsa"
           else _exact(eng, "כהנים גדולים", ["אהרן"]))
    assert "cross_ok_p1" in got          # אהרן is on a third page
    assert "cross_hit_p1" not in got     # אהרן is on the page the span reaches


def test_a_row_found_through_its_whole_manuscript_reads_its_own_page(eng):
    # The AND's words sit on two pages: the row comes from the whole-manuscript doc and
    # is its first word's page; אהרן is on a third page.
    assert "and_p1" in _responsa(eng, "כהנים AND גדולים -אהרן")
    assert "and_p1" in _responsa(eng, "כהנים AND גדולים")


def test_the_complete_match_pass_decides_the_same(eng):
    full = _exact(eng, "כהנים גדולים", ["אהרן"])
    ids = _exact(eng, "כהנים גדולים", ["אהרן"], ids_only=True)
    assert ids == full


@pytest.mark.parametrize("search", ["responsa", "exact"])
def test_my_library_rows_are_excluded_on_their_page(eng, search):
    got = (_responsa(eng, "כהנים גדולים -אהרן", scope="local") if search == "responsa"
           else _exact(eng, "כהנים גדולים", ["אהרן"], scope="local"))
    assert got == {"local_clean"}


def test_no_private_key_leaves_the_engine(eng):
    rows = eng.execute_search("כהנים גדולים", "literal", 0, exclude_words=["אהרן"], corpus_scope="genizah")
    rows += eng.execute_search("כהנים גדולים -אהרן", "literal", 0, responsa_options={"responsa_mode": True},
                               corpus_scope="genizah")
    assert rows and not any(k.startswith("_") for r in rows for k in r)
