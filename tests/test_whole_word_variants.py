"""Variants match whole words, through the same paths as Exact (owner, 2026-10-01).

"I don't need to find שלום inside ושלום -- Responsa mode is for this kind of search."
So Variants retrieve every form the verifier accepts as a WHOLE token (not only the
first 200), read page docs for one word, use whole-manuscript docs only for page-break
crossings, and a match inside a longer Hebrew word is not a match -- for Exact too.

End to end through ``SearchEngine.execute_search`` on a tiny main-schema index, with a
fixed form table standing in for VariantManager (the real lists are short at the
default level; the 200 cut bites at Maximum).
"""
import gc
import json
import os
from unittest.mock import patch

import pytest

tantivy = pytest.importorskip("tantivy")

from shared.config import Config  # noqa: E402
from shared.indexer import Indexer  # noqa: E402
from shared.search_tokenizer import register_search_tokenizers  # noqa: E402
from shared.text_normalize import strip_search_diacritics  # noqa: E402
import shared.search_engine as se  # noqa: E402

W = "שלום"                    # שלום
FAR = "שלוס"                  # a form beyond the first 200
ON = "על"                     # על
FILLER = [f"זז{n:03d}" for n in range(250)]
FORMS = {W: [W] + FILLER + [FAR]}  # FAR is form #252


class _Forms:
    """VariantManager stand-in: fixed lists, cut by *limit* like the real one."""

    def get_variants(self, term, mode, limit=None):
        forms = FORMS.get(term, [term])
        return list(forms[:limit]) if limit else list(forms)


PAGES = {
    "beyond": f"אבג {FAR} דהו",                 # only form #252, as a whole word
    "beyond_br": f"אבג {FAR}[ דהו",             # ...with a lacuna bracket attached (token שלוס[)
    "marked": f"אבג ש̇לוס דהו",            # form #252 with a combining dot inside
    "inside": f"ו{W} רב",                       # only inside a longer word
    "hl": f"ו{W} {W}",                          # inside first, whole word second
    "hl_br": f"ו[{W}] {W}",                     # the same with a lacuna inside the first word
}
CROSS = [("c1", f"אבג {FAR}"), ("c2", f"{ON} דהו")]          # FAR | ON across a break
CROSS_IN = [("d1", f"{W} אבג ו{FAR}"), ("d2", f"{ON} דהו")]  # retrieved (שלום, על), crossing only inside a word
CROSS_LIT = [("e1", f"{W} אבג ו{W}"), ("e2", f"{ON} דהו")]  # the same for Exact: ושלום | על
CROSS_LAT = [("l1", "אבג abc"), ("l2", "def דהו")]                 # Latin words across a break
CROSS_DIG = [("g1", "אבג טוב2"), ("g2", "יום דהו")]               # a word with a digit
# An Oxford-style part spanning manuscripts A and B: the whole word on A's page, only
# an inside-word occurrence on B's.
PART = [("q1", "A", f"רב {W}"), ("q2", "B", f"רב ו{W}")]


def _schema():
    b = tantivy.SchemaBuilder()
    b.add_text_field("unique_id", stored=True)
    b.add_text_field("content", stored=True, tokenizer_name="hebword")
    for f in ("content_head", "content_tail", "line_starts", "line_ends"):
        b.add_text_field(f, stored=False, tokenizer_name="whitespace")
    b.add_text_field("content_search", stored=False, tokenizer_name="hebword")
    for f in ("source", "full_header", "shelfmark", "scope", "boundaries"):
        b.add_text_field(f, stored=True)
    return b.build()


class _Meta:
    def get_display_data(self, header, source):
        return {"shelfmark": header, "title": "", "img": "1", "source": source,
                "id": header, "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


def _aggregate(pages):
    text, bounds, cursor = [], [], 0
    for i, (uid, t) in enumerate(pages):
        start = cursor
        text.append(t)
        cursor += len(t)
        if i != len(pages) - 1:
            text.append("\n")
            cursor += 1
        bounds.append({"uid": uid, "p_num": i + 1, "full_header": uid, "source": "V0.8",
                       "sys_id": "99", "start": start, "end": cursor})
    return "".join(text), json.dumps(bounds, ensure_ascii=False)


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("wholeword")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, text, scope, bounds="", header=None):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text),
            source="V0.8", full_header=header or uid, shelfmark=uid, scope=scope, boundaries=bounds,
            **Indexer._extract_position_fields(text)))

    for uid, text in list(PAGES.items()) + CROSS + CROSS_IN + CROSS_LIT + CROSS_LAT + CROSS_DIG:
        add(uid, text, "page")
    for sid, pages in (("sys:c", CROSS), ("sys:d", CROSS_IN), ("sys:e", CROSS_LIT),
                       ("sys:l", CROSS_LAT), ("sys:g", CROSS_DIG)):
        add(sid, *_aggregate(pages)[:1], "system", _aggregate(pages)[1])
    for uid, sid, text in PART:
        add(uid, text, "page", header=f"IE_{uid} {sid}")
    part_text, part_bounds = _aggregate([(u, t) for u, _s, t in PART])
    add("part:x", part_text, "part", part_bounds, header="IE_q1 A")
    w.commit()
    w.wait_merging_threads()
    w = idx = None

    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), _Forms(), worker_mode=True, open_local=False)
    eng._load_browse_map = lambda: {"A": [{"uid": "q1"}], "B": [{"uid": "q2"}]}
    yield eng
    eng = None
    gc.collect()


def _rows(engine, query, mode="variants", **kw):
    return {r["uid"]: r for r in engine.execute_search(query, mode, 0, corpus_scope="genizah", **kw)}


def test_a_form_beyond_the_first_200_is_retrieved(engine):
    # Retrieval used get_variants(limit=200); the verifier up to 8,000.
    assert "beyond" in _rows(engine, W)


def test_a_form_with_an_edge_bracket_is_retrieved(engine):
    # The bracket forms were added for the typed word only (7 V0.8 pages missed).
    assert "beyond_br" in _rows(engine, W)


def test_a_marked_form_beyond_200_is_retrieved_through_the_folded_field(engine):
    assert "marked" in _rows(engine, W)


def test_a_match_only_inside_a_longer_word_is_not_returned(engine):
    for mode in ("variants", "literal"):
        assert "inside" not in _rows(engine, W, mode=mode), mode


def test_the_whole_word_occurrence_is_the_one_highlighted(engine):
    for mode in ("variants", "literal"):
        hl = _rows(engine, W, mode=mode)["hl"]["raw_file_hl"]
        assert f"*{W}*" in hl and f"ו*{W}" not in hl, (mode, hl)


def test_with_brackets_the_original_text_highlight_is_the_whole_word(engine):
    # Bracket-free text decides the match; the highlight is re-found in the original,
    # where ו[שלום] is still one word.
    for mode in ("variants", "literal"):
        hl = _rows(engine, W, mode=mode)["hl_br"]["raw_file_hl"]
        assert hl.endswith(f" *{W}*") and f"[*{W}" not in hl, (mode, hl)


def test_a_variant_phrase_across_a_page_break_is_found(engine):
    # The crossing pre-check knew only the typed words; FAR is a form of W.
    rows = _rows(engine, f"{W} {ON}")
    assert "c1" in rows and rows["c1"].get("cross_page")


@pytest.mark.parametrize("query, uid", [("abc def", "l1"), ("טוב2 יום", "g1")])
def test_a_variant_phrase_with_a_latin_or_digit_word_across_a_page_break_is_found(engine, query, uid):
    # Codex review (PR #375): the crossing pre-check looked for the forms among the
    # window's runs of Hebrew letters only, which `abc` or `טוב2` never is.
    rows = _rows(engine, query)
    assert uid in rows and rows[uid].get("cross_page")


def test_a_crossing_inside_a_longer_word_is_not_returned(engine):
    assert "d1" not in _rows(engine, f"{W} {ON}")
    # Exact's pre-check is a substring test (שלום is in ושלום), so here the
    # whole-word rule inside the crossing search is what rejects it.
    assert "e1" not in _rows(engine, f"{W} {ON}", mode="literal")


# --- the whole-word test and the window pieces on their own -------------------------

@pytest.mark.parametrize("text, span, whole", [
    (f"ו{W}", (1, 5), False),                     # a letter before
    (f"{W}ים", (0, 4), False),                    # letters after
    (f"וְ{W}", (2, 6), False),               # nikud between: still the same word
    (f'ר"{W}', (2, 6), False),                    # a quote inside an abbreviation
    (f" {W} ", (1, 5), True),
    (f"[{W}]", (1, 5), True),
    (f"{W}־{ON}", (0, 4), True),             # maqaf is a separator
    (f"{W}²", (0, 4), True),                      # the tokenizer splits '²' off
    (f"{W}̇", (0, 4), True),                 # a trailing mark belongs to the word
    (W, (0, 4), True),
])
def test_whole_word_span(text, span, whole):
    assert se._whole_word_span(text, *span) is whole


def _pages(*texts):
    text, bounds, cursor = [], [], 0
    for i, t in enumerate(texts):
        bounds.append({"uid": f"p{i}", "start": cursor, "end": cursor + len(t)})
        text.append(t)
        cursor += len(t) + 1
    return "\n".join(text), bounds


def test_joined_crossing_search_skips_an_inside_word_crossing(engine):
    # Gap 0, several windows: one regex pass over them all. The first crossing is
    # inside a longer word; the pass must go on to the next break's whole-word one.
    rx = engine.build_regex_pattern([W, ON], "literal", 0)
    content, bounds = _pages(f"אבג ו{W}", f"{ON} {W}", f"{ON} דהו")
    spans = engine._cross_page_spans(rx, content, bounds, W, ON, 1, 0, True, whole_words=True)
    assert [content[s:e] for s, e in spans] == [f"{W}\n{ON}"]
    assert spans[0][0] > content.index("\n")              # the second break's crossing
    loose = engine._cross_page_spans(rx, content, bounds, W, ON, 1, 0, True)
    assert len(loose) == 2                                  # without the rule: both


def test_per_window_crossing_search_skips_an_inside_word_crossing(engine):
    # Gap 1: the first crossing at the break starts inside ושלום (with שלום as the
    # gap word); the next one, at the same break, is the whole-word match.
    rx = engine.build_regex_pattern([W, ON], "literal", 1)
    content, bounds = _pages(f"אבג ו{W} {W}", f"{ON} דהו")
    spans = engine._cross_page_spans(rx, content, bounds, W, ON, 2, 1, True, whole_words=True)
    assert [content[s:e] for s, e in spans] == [f"{W}\n{ON}"]
    loose = engine._cross_page_spans(rx, content, bounds, W, ON, 2, 1, True)
    assert [content[s:e] for s, e in loose] == [f"{W} {W}\n{ON}"]


def test_the_crossing_pre_check_takes_a_set_of_forms(engine):
    rx = engine.build_regex_pattern([W, ON], "variants", 0)
    content, bounds = _pages(f"אבג {FAR}", f"{ON} דהו")
    forms = engine._folded_forms(W, "variants")
    assert FAR in forms
    got = engine._cross_page_spans(rx, content, bounds, forms, frozenset({ON}), 1, 0, True, whole_words=True)
    assert [content[s:e] for s, e in got] == [f"{FAR}\n{ON}"]
    assert engine._cross_page_spans(rx, content, bounds, W, ON, 1, 0, True, whole_words=True) == []


@pytest.mark.parametrize("mode", ["literal", "variants"])
def test_search_within_takes_no_inside_word_match_from_a_part_without_a_position(engine, mode, monkeypatch):
    # An index whose docs lack a scope reads the whole-manuscript docs for one word
    # too; no position check stands in for the whole-word rule there.
    monkeypatch.setattr(engine, "_every_doc_has_scope", lambda: False)
    assert "q2" not in _rows(engine, W, mode=mode, restrict_sys_ids={"B"})
    assert "q1" in _rows(engine, W, mode=mode, restrict_sys_ids={"A"})


@pytest.mark.parametrize("mode", ["literal", "variants"])
def test_search_within_takes_no_inside_word_match_from_a_part(engine, mode):
    # The part's whole-word match is on A's page; B's page holds only ושלום, which
    # also ends its line. Within B the part must give nothing (a line-end search, so
    # the part is read and the position check alone would pass ושלום).
    kw = dict(text_position="line_end", restrict_sys_ids={"B"})
    assert "q2" not in _rows(engine, W, mode=mode, **kw)
    assert "q1" in _rows(engine, W, mode=mode, text_position="line_end", restrict_sys_ids={"A"})


def test_an_index_without_the_folded_field_still_retrieves_every_form(engine):
    real = engine._has_content_search
    engine._has_content_search = False
    try:
        assert "beyond" in _rows(engine, W)
    finally:
        engine._has_content_search = real
