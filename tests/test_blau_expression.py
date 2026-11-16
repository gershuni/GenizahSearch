# -*- coding: utf-8 -*-
"""The expression search in Blau's dictionary (shared/blau_service.py::BlauService.expression, on
the ``tokens`` table scripts/build_blau_sidecar.py writes at schema 1.1.0), on the synthetic
dictionary of tests/test_build_blau_sidecar.py with entries of several words added: no text of the
dictionary itself is in the repository.

Pinned: several words are found in order inside one name or one citation, within the gap; the
tiers (the entry whose own name is the expression, then the entries whose name holds the words,
then the citations), contiguous before gapped; a typed clitic meets a printed base and the
reverse; function words only stand together among strangers; the bounds and the fail-closed
answers; a 1.0.0 file (no tokens) is served without the expression search and ``answer`` falls
back to the lookup; a 1.1.0 file with doctored tokens is refused.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from scripts.build_blau_sidecar import EXIT_OK
from scripts.build_blau_sidecar import main as build_main
from scripts.verify_blau_sidecar import verify
from shared import blau_service
from shared.blau_service import (
    MAX_EXPRESSION_HITS,
    BlauExpression,
    BlauLookup,
    BlauService,
    name_alternatives,
)
from shared.ja_expression import DEFAULT_GAP, MAX_GAP, context_windows, query_keys, quoted_gap, quoted_runs, word_keys
from tests.test_build_blau_sidecar import _form, _ja, _root, _seg, dictionary, write_source

E4, E5, E6 = "blau-e-000004", "blau-e-000005", "blau-e-000006"
SE2 = "blau-se-000002"
SE2_NAME = "כתאב אלקאצי"


def _cited(cid: str, ja: str, translation: str) -> list[dict]:
    return [{
        "id": cid + "-r", "parse_type": "standard", "editorial_notes": [],
        "segments": [_seg("גויטיין, חברה, ב, 10", "reference", ref_role="citation")],
        "parsed": [{
            "id": cid, "citation_type": "full", "source_work": "גויטיין, חברה", "source_id": "blau-bib-000002",
            "citation": [_seg("גויטיין, חברה, ב, 10", "reference", ref_role="citation")],
            "ja_text": [_ja(ja, "reference", ref_role="ja_text")],
            "translation": [_seg(translation, "reference", ref_role="translation")],
            "commentary": [],
        }],
    }]


def test_a_finite_verb_click_reaches_an_adjacent_governed_quotation(tmp_path):
    data = expressions_dictionary()
    data['כתב (كتب)']['stems'][0]['forms'][0]['references'] = _cited(
        'blau-c-000002', 'כתבתי מעה', "'synthetic example'")
    src = write_source(tmp_path / 'download', data=data)
    path = tmp_path / 'blau.db'
    assert build_main(['--source-dir', str(src), '--out', str(path)]) == EXIT_OK
    svc = BlauService(str(path))
    result = svc.answer('וכתבתי', after=('מעה',))
    assert result.context
    assert result.context[0].hits[0].root_id == E4
    assert all(place.scope == 'citation' for place in result.context[0].hits[0].places)
    from shared.ja_expression import governed_verb_run
    assert not governed_verb_run('כתאב', ('מעה',), svc.verb_form_index())
    assert not governed_verb_run('וכתבתי', ('אללה',), svc.verb_form_index())
    svc.close()


def expressions_dictionary() -> dict:
    data = dictionary()
    data["כתב (كتب)"] = _root("כתב (كتب)", E4, ["כתב"], ["كتب"], stems=[{
        "id": "blau-st-000005", "stem_head": {"binyan": "I"},
        "forms": [
            _form("blau-f-000006", [_ja("כתב כטה", "headword_ja"), _seg("'חתם'", "definition_hebrew"),
                                    _seg("'signed'", "definition_english", font="English-TNR", language="English")],
                  references=_cited("blau-c-000002", "ראובן כתב כטה פי אלשטר", "'ראובן חתם על השטר'")),
            _form("blau-f-000007", [_ja("כתב עלי", "headword_ja"), _seg("'רשם על'", "definition_hebrew")],
                  references=_cited("blau-c-000003", "וכתב פלאן עלי אלורקה", "'ופלוני רשם על הדף'")),
        ],
    }], sub_entries=[{
        "id": SE2, "name_ja": ["כתאב אלקאצי"], "arabic_spelling": ["كتاب القاضي"],
        "stems": [{"id": "blau-st-000006", "stem_head": {}, "forms": [
            _form("blau-f-000008", [_seg("'ספר הדיין'", "definition_hebrew")],
                  references=_cited("blau-c-000004", "לא אבד מן דלך לא מן אחד", "'אין מנוס מזה לאף אחד'")),
        ]}],
    }])
    data["שהד (شهد)"] = _root("שהד (شهد)", E5, ["שהד"], ["شهد"], stems=[{
        "id": "blau-st-000007", "stem_head": {"binyan": "I"},
        "forms": [
            _form("blau-f-000009", [_ja("שאהד", "headword_ja"), _seg("'עד'", "definition_hebrew")],
                  references=_cited("blau-c-000005", "אלשאהד כתב כטה הנא", "'העד חתם כאן'")
                  + _cited("blau-c-000006", "כתב אלשאהד כטה", "'חתם העד'")),
        ],
    }])
    return data


def _build(tmp_path: Path) -> Path:
    src = write_source(tmp_path / "download", data=expressions_dictionary())
    out = tmp_path / "blau_data" / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OK
    return out


@pytest.fixture
def built(tmp_path):
    return _build(tmp_path)


@pytest.fixture
def service(built):
    svc = BlauService(str(built))
    assert svc.is_ready(), svc.unavailable_reason
    assert svc.expressions_available()
    yield svc
    svc.close()


@pytest.fixture(autouse=True)
def _flag_on(monkeypatch):
    monkeypatch.delenv("BLAU_DICTIONARY_ENABLED", raising=False)


def _places(result: BlauExpression) -> list[tuple]:
    return [(h.root_id, h.sub_entry, p.scope, p.positions, p.contiguous, [(c.printed, c.via) for c in p.components])
            for h in result.hits for p in h.places]


def test_the_tiers_the_name_that_is_the_expression_then_strangers_contiguous_first(service):
    found = service.expression("כתב כטה", gap=1)
    assert found.found and found.words == ("כתב", "כטה") and found.gap == 1 and not found.truncated
    assert _places(found) == [
        (E4, "", "form", (0, 1), True, [("כתב", "word"), ("כטה", "word")]),        # the headword IS the expression
        (E4, "", "citation", (1, 2), True, [("כתב", "word"), ("כטה", "word")]),    # its own citation
        (E5, "", "citation", (1, 2), True, [("כתב", "word"), ("כטה", "word")]),    # a stranger, together
        (E5, "", "citation", (0, 2), False, [("כתב", "word"), ("כטה", "word")]),   # a stranger, apart
    ]
    first = found.hits[0]
    assert first.name == "כתב" and first.url.endswith(E4) and first.places[0].gloss_he == "'חתם'"
    assert first.places[1].quotation.translation == "'ראובן חתם על השטר'"
    assert first.places[1].quotation.locator == "גויטיין, חברה, ב, 10" and first.places[1].quotation.source == "גויטיין, חברה"
    assert first.places[1].form_text == "כתב כטה" and first.places[1].words == ("ראובן", "כתב", "כטה", "פי", "אלשטר")
    assert _places(service.expression("כתב כטה", gap=0))[-1][3] == (1, 2), "at gap 0 the gapped citation is out"
    assert not service.expression("כטה כתב", gap=5).found, "order matters"


def test_a_sub_entry_s_name_and_a_typed_clitic(service):
    found = service.expression("כתאב אלקאצי", gap=0)
    assert _places(found) == [(E4, SE2_NAME, "sub_entry", (0, 1), True, [("כתאב", "word"), ("אלקאצי", "word")])]
    assert found.hits[0].sub_entry == "כתאב אלקאצי" and found.hits[0].name == "כתב"
    typed_clitic = service.expression("וכתב עלי", gap=2)
    assert _places(typed_clitic)[0] == (E4, "", "form", (0, 1), True, [("כתב", "stem"), ("עלי", "word")]), \
        "ו peeled off the typed word meets the headword; the headword IS the expression"
    assert typed_clitic.candidates[0][:2] == ("וכתב", "כתב")
    printed_clitic = service.expression("כתב פלאן", gap=0)
    assert _places(printed_clitic) == [(E4, "", "citation", (0, 1), True, [("וכתב", "stem"), ("פלאן", "word")])]


def test_function_words_only_are_shown_among_strangers_only_together(service):
    found = service.expression("לא מן", gap=1)
    assert _places(found) == [(E4, SE2_NAME, "citation", (4, 5), True, [("לא", "word"), ("מן", "word")])], \
        "the citation holds them apart and then together: the tightest occurrence, and so it is shown"
    assert not service.expression("לא מן", gap=0).found or _places(service.expression("לא מן", gap=0))[0][3] == (4, 5)


def test_bounds_and_fail_closed(service, monkeypatch):
    assert not service.expression("כתב").found and not service.expression("").found
    assert not service.expression("א ב ג ד ה ו ז", gap=1).found, "seven words are too many"
    assert service.expression("כתב כטה", gap=99).gap == MAX_GAP and service.expression("כתב כטה", gap="x").gap == DEFAULT_GAP
    numbered = service.expression("כתב 12", gap=0)
    assert not numbered.found and numbered.candidates == (("כתב", "תב", "~כתב", "~תב", "@כתב:1"), ()), "a word without Hebrew letters has no key"
    cut = service.expression("כתב כטה", gap=1, limit=1)
    assert [h.root_id for h in cut.hits] == [E4] and cut.truncated
    assert MAX_EXPRESSION_HITS >= 1

    def boom(*args, **kwargs):
        raise RuntimeError("disk gone")

    monkeypatch.setattr(service, "_expression", boom)
    assert not service.expression("כתב כטה").found, "an error is an empty answer, never a raise"
    monkeypatch.setenv("BLAU_DICTIONARY_ENABLED", "0")
    assert not service.expression("כתב כטה").found


def test_answer_routes_one_word_to_the_lookup_and_several_to_the_expression(service):
    assert isinstance(service.answer("כתב"), BlauLookup)
    assert isinstance(service.answer("  כתב   כטה "), BlauExpression)
    assert service.answer("כתב כטה", gap=0).gap == 0


def _set_meta(path: Path, key: str, value: str) -> None:
    con = sqlite3.connect(str(path))
    try:
        con.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)", (key, value))
        con.commit()
    finally:
        con.close()


def test_a_1_0_0_file_is_served_without_the_expression_search(built):
    con = sqlite3.connect(str(built))
    try:
        con.execute("DROP TABLE tokens")
        con.execute("DELETE FROM meta WHERE key IN ('rows_tokens', 'keying_version')")
        con.execute("UPDATE meta SET value = '1.0.0' WHERE key = 'version'")
        con.commit()
    finally:
        con.close()
    svc = BlauService(str(built))
    try:
        assert svc.is_ready(), svc.unavailable_reason
        assert not svc.expressions_available()
        assert not svc.expression("כתב כטה").found
        fallback = svc.answer("כתב כטה")
        assert isinstance(fallback, BlauLookup) and fallback.found, "the phrase that IS a headword is still found as one key"
        assert isinstance(svc.answer("כתאב אלקאצי"), BlauLookup)
        assert svc.lookup("כתב").found
    finally:
        svc.close()


@pytest.mark.parametrize("sql, reason", [
    ("DROP TABLE tokens", "without the tokens table"),
    ("DROP INDEX idx_tokens_key", "expression index idx_tokens_key missing"),
    ("DELETE FROM tokens WHERE rowid = 1", "meta.rows_tokens"),
    ("UPDATE meta SET value = '0' WHERE key = 'keying_version'", "tokens keyed by expression keying '0'"),
    ("UPDATE meta SET value = '2.0.0' WHERE key = 'version'", "schema version '2.0.0'"),
])
def test_a_1_1_0_file_with_doctored_tokens_is_not_served(built, sql, reason):
    con = sqlite3.connect(str(built))
    try:
        con.execute(sql)
        con.commit()
    finally:
        con.close()
    svc = BlauService(str(built))
    assert not svc.is_ready() and reason in svc.unavailable_reason, svc.unavailable_reason


def test_the_gate_recomputes_every_token_row(built):
    assert verify(str(built), 5, 9) == []
    con = sqlite3.connect(str(built))
    try:
        total = con.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
        con.execute("UPDATE tokens SET key = 'xyz' WHERE rowid = 1")
        con.commit()
    finally:
        con.close()
    problems = verify(str(built), 5, 9, expect_tokens=total)
    assert any(p.startswith("tokens differ from the names and citations") and "1 missing, 1 extra" in p for p in problems), problems
    assert any("tokens" in p and f"!= expected {total + 1:,}" in p for p in verify(str(built), 5, 9, expect_tokens=total + 1))
    con = sqlite3.connect(str(built))
    try:
        con.execute("DELETE FROM tokens WHERE rowid = 2")
        con.execute("UPDATE meta SET value = ? WHERE key = 'rows_tokens'", (str(total - 1),))
        con.commit()
    finally:
        con.close()
    assert any("1 missing, 1 extra, -1 rows" in p or "2 missing, 1 extra, -1 rows" in p
               for p in verify(str(built), 5, 9)), "a dropped row with its count doctored"


def test_name_alternatives_split_the_stored_column():
    assert name_alternatives("כתב | כתאב") == ["כתב", "כתאב"] and name_alternatives(None) == []
    assert name_alternatives("כתב כטה") == ["כתב כטה"]


def test_the_env_var_points_the_default_path_at_a_candidate_file(tmp_path, monkeypatch):
    monkeypatch.setenv(blau_service.ENV_DB_PATH, str(tmp_path / "candidate.db"))
    assert blau_service.default_sidecar_path() == str(tmp_path / "candidate.db")
    monkeypatch.setenv(blau_service.ENV_DB_PATH, "  ")
    assert blau_service.default_sidecar_path() != "  "


def test_a_clicked_word_s_runs_with_the_words_beside_it_come_longest_first():
    """The expressions a clicked word makes with the words beside it, three words
    then two, each run holding the clicked word, left to right; never a run of function words only."""
    assert context_windows("אללה", ("כתאבי", "אטאל"), ("בקאך", "ואדאם")) == [
        ["כתאבי אטאל אללה", "אטאל אללה בקאך", "אללה בקאך ואדאם"], ["אטאל אללה", "אללה בקאך"]]
    assert context_windows("אללה", (), ("בקאך",)) == [["אללה בקאך"]]
    assert context_windows("מלה", ("אחד", "שני", "שלישי"), ()) == [["שני שלישי מלה"], ["שלישי מלה"]], "two a side"
    assert context_windows("אללה", ("x y", "", "אטאל"), ()) == [["אטאל אללה"]], "one word each"
    assert context_windows("כל", ("עלי",), ()) == [], "function words only"
    assert context_windows("אללה", (), ()) == [] and context_windows("", ("אטאל",), ()) == []


def test_a_clicked_word_with_the_words_beside_it_brings_the_entries_that_name_them_first(service):
    """אללה in "אטאל אללה בקאך" reached the entries of אללה, not the blessing. The
    runs with the words beside it are searched with no word between, and an entry or sub-entry
    whose NAME holds one is answered first, with its name places only; one that holds it in a
    citation only is not (שהד's "כתב כטה"). The word's own card for the same scope gives way."""
    [found] = service.context("כטה", ("כתב",), ())
    assert found.text == "כתב כטה" and found.gap == 0
    assert [(h.root_id, h.sub_entry, [p.scope for p in h.places]) for h in found.hits] == [(E4, "", ["form"])]
    own = [(c.root_id, c.sub_entry) for c in service.lookup("כתאב").cards]
    assert (E4, SE2_NAME) in own, "the word itself reaches the sub-entry by its name"
    answer = service.answer("כתאב", stems=False, after=("אלקאצי",))
    assert [(h.root_id, h.sub_entry) for c in answer.context for h in c.hits] == [(E4, SE2_NAME)]
    assert (E4, SE2_NAME) not in [(c.root_id, c.sub_entry) for c in answer.cards], "shown once, first"
    assert answer.found and service.answer("כתאב", stems=False).context == ()
    assert service.context("כטה", (), ()) == () and service.context("כטה", ("ענב",), ()) == ()


def test_only_runs_of_plain_words_are_searched_in_the_quotations():
    """When no name holds a run, the quotations that do show. A run with a function word
    or a word of a name put two words of the text side by side in a quotation by chance ("פי אלבר", "אבו
    אלפרג"), so only the others are searched, longest first; with the selection's gap, at most the default."""
    windows = context_windows("ואדאם", ("פי", "בקאך"), ("סלאמתך", "אבו"))
    assert quoted_runs(windows) == ["בקאך ואדאם סלאמתך", "בקאך ואדאם", "ואדאם סלאמתך"]
    assert quoted_runs(context_windows("אללה", ("אטאל",), ("בקאך",))) == [], "אללה is a function word here"
    assert quoted_runs(context_windows("סעיד", ("ואבי",), ())) == []
    assert quoted_runs([]) == []
    assert quoted_gap(0) == 0 and quoted_gap(1) == 1 and quoted_gap(MAX_GAP) == DEFAULT_GAP
    assert quoted_gap("x") == DEFAULT_GAP


def test_a_clicked_word_whose_run_only_citations_hold_brings_those_citations(service, monkeypatch):
    """ואדאם in "ואדאם סלאמתך" showed none of what selecting both words found. When no
    name holds a run, the citations that hold one come first, with the selection's gap; the word's own
    list of citations leaves them out; a name that holds a run keeps them away (כטה after כתב)."""
    answer = service.answer("ראובן", gap=DEFAULT_GAP, stems=False, after=("כטה",))
    assert answer.context_quoted and answer.found
    assert [(c.text, [(h.root_id, [p.scope for p in h.places]) for h in c.hits]) for c in answer.context] == [
        ("ראובן כטה", [(E4, ["citation"])])]
    assert E4 in [h.root_id for h in service.lookup("ראובן").quotes], "the word alone lists it"
    assert E4 not in [h.root_id for h in answer.quotes], "shown once, first"
    assert service.context("ראובן", (), ("כטה",), gap=0) == (), "a word between"
    assert not service.answer("כטה", stems=False, before=("כתב",)).context_quoted
    monkeypatch.setattr(blau_service, "MAX_CONTEXT_QUOTED", 0)
    assert service.context("ראובן", (), ("כטה",)) == ()


@pytest.fixture
def gapped(tmp_path):
    """The expressions dictionary with one more root, whose headword has a word between."""
    data = expressions_dictionary()
    data["רסל (رسل)"] = _root("רסל (رسل)", E6, ["רסל"], ["رسل"], stems=[{
        "id": "blau-st-000008", "stem_head": {"binyan": "IV"},
        "forms": [_form("blau-f-000010", [_ja("ארסל אללה רחמתה", "headword_ja"), _seg("'שלח'", "definition_hebrew")])],
    }])
    out = tmp_path / "blau_data" / "blau.db"
    assert build_main(["--source-dir", str(write_source(tmp_path / "download", data=data)), "--out", str(out)]) == EXIT_OK
    svc = BlauService(str(out))
    assert svc.expressions_available()
    yield svc
    svc.close()


def test_a_name_that_holds_the_words_beside_a_clicked_word_with_a_word_between_names_them(gapped):
    """A name that holds a run with a word between ("ארסל אללה רחמתה" for "ארסל רחמתך") is found with the
    citations' gap, and names the run: not a context of citations only."""
    service = gapped
    assert service.context("ארסל", (), ("רחמתך",), gap=0) == ()
    answer = service.answer("ארסל", gap=DEFAULT_GAP, stems=False, after=("רחמתך",))
    assert [(h.root_id, [p.scope for p in h.places]) for c in answer.context for h in c.hits] == [(E6, ["form"])]
    assert not answer.context_quoted


def test_a_typed_word_whose_article_lost_its_alif_finds_the_article_written_whole(service):
    """"יכצך בלסלאם" missed what "יכצך באלסלאם" finds. A reader's word also answers to
    the stem its elided article gives (``query_keys``); the dictionaries' words keep ``word_keys``."""
    assert "שטר" in query_keys("בלשטר") and "שטר" in word_keys("בלשטר")
    assert set(word_keys("כתאב")) <= set(query_keys("כתאב"))
    found = service.expression("כטה בלשטר", gap=1)
    assert [(h.root_id, [p.scope for p in h.places]) for h in found.hits] == [(E4, ["citation"])]
    assert not service.expression("כטה בלשטר", gap=0).found, "פי between"


def test_the_words_beside_a_clicked_word_fail_closed(service, monkeypatch):
    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(service, "expression", boom)
    assert service.context("כטה", ("כתב",), ()) == ()
    assert service.answer("כטה", stems=False, before=("כתב",)).context == ()


def test_an_expression_with_tsade_reaches_a_name_printed_with_dotted_tet_as_a_possible_spelling(tmp_path):
    """ض and ظ are written for one another: "חפצ̇ אללה" reaches the name "חפט̇ אללה" of the root
    חפט̇, after any hit of the words as written and flagged as a possible spelling. "כתב כצה" reaches
    nothing: כטה is printed under roots without the dot, so the exchange is not the ض/ظ pair."""
    data = expressions_dictionary()
    data["חפט̇ (حفظ)"] = _root("חפט̇ (حفظ)", E6, ["חפט̇"], ["حفظ"], stems=[{
        "id": "blau-st-000009", "stem_head": {"binyan": "I"},
        "forms": [_form("blau-f-000011", [_ja("חפט̇ אללה", "headword_ja"), _seg("'שמר'", "definition_hebrew")])],
    }])
    src = write_source(tmp_path / "download", data=data)
    path = tmp_path / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(path)]) == EXIT_OK
    svc = BlauService(str(path))
    try:
        own = svc.expression("חפט̇ אללה", gap=DEFAULT_GAP)
        exchanged = svc.expression("חפצ̇ אללה", gap=DEFAULT_GAP)
        assert [(h.root_id, h.possible_spelling) for h in own.hits] == [(E6, False)]
        assert [(h.root_id, h.possible_spelling) for h in exchanged.hits] == [(E6, True)]
        assert [(h.root_id, h.possible_spelling) for h in svc.expression("חפצ אללה", gap=DEFAULT_GAP).hits] == [
            (E6, True)], "the word's own dot is not needed"
        assert svc.expression("כתב כטה", gap=DEFAULT_GAP).hits
        assert not svc.expression("כתב כצה", gap=DEFAULT_GAP).hits, "ט printed without the dot is ط"
    finally:
        svc.close()


def test_an_undotted_occurrence_first_in_a_citation_does_not_hide_a_dotted_one_after_it(tmp_path):
    """One citation holds חפט אללה and later חפט̇ אללה: the exchanged pass drops the undotted
    occurrence and matches the citation again, finding the dotted one."""
    data = expressions_dictionary()
    data["שמר (حرس)"] = _root("שמר (حرس)", E6, ["שמר"], ["حرس"], stems=[{
        "id": "blau-st-000009", "stem_head": {"binyan": "I"},
        "forms": [_form("blau-f-000011", [_ja("שמר", "headword_ja"), _seg("'שמר'", "definition_hebrew")],
                        references=_cited("blau-c-000009", "חפט אללה ופי חפט̇ אללה", "'x'"))],
    }])
    src = write_source(tmp_path / "download", data=data)
    path = tmp_path / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(path)]) == EXIT_OK
    svc = BlauService(str(path))
    try:
        hit, = svc.expression("חפצ אללה", gap=0).hits
        assert hit.possible_spelling and [p.positions for p in hit.places] == [(3, 4)]
    finally:
        svc.close()
