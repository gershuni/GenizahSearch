"""A clicked word and the head it names: governed pairs, attached prepositions, and the rule that a
head is the expression only when the text holds its lexical words. Synthetic units only."""
from types import SimpleNamespace

import pytest

from scripts.build_friedman_sidecar import write_sidecar
from shared.friedman_service import SCOPE_HEAD, FriedmanService
from shared.ja_expression import (
    ALL_VERB_KINDS, EXPRESSION_VERB_KINDS, clitic_governed, clitic_preposition, governed_runs, lexical_positions, names_expression,
    query_keys, verb_keys,
)
from shared.ja_roots import StemPair
from shared.ja_verb_forms import verb_form_index
from test_friedman_service import unit


def lexical(head, root=""):
    words = head.split()
    return [words[i] for i in lexical_positions(words, root)]


def test_lexical_words_leave_out_the_heads_apparatus():
    assert lexical("נצף מן אלשהר") == ["נצף", "אלשהר"]
    assert lexical("דכל פיהא") == ["דכל"]
    assert lexical("טלב יפעל/פעלא") == ["טלב"], "a placeholder is a slot"
    assert lexical("כצץ -ה") == ["כצץ"], "an object mark is no word"
    assert lexical("גזי -ה אללה ענהא (ולא רק עליהא) כירא") == ["גזי", "כירא"], "a note is no word of the expression"
    assert lexical("נחו ר' נחא") == ["נחו"], "the plural after ר' is an alternative"
    assert lexical("נץ: אלנץ") == ["אלנץ"] and lexical("גמב (<גנב): מן גמב") == ["גמב"], "the label before a colon"
    assert lexical("מא יציע לך ענד אללה/בין ידי אללה") == ["יציע"], "words after alternatives belong to the last one"
    assert lexical("אכד/קבץ כטה/כטא מנה") == ["אכד/קבץ", "כטה/כטא"], "alternatives in the first word keep the rest"
    assert lexical("מחן אמתחן בהא", root="מחן") == ["אמתחן"], "the root opening the head is its label"
    assert lexical("דכל פי ראיה", root="דכל") == ["דכל", "ראיה"]
    assert lexical("אלי מא יפעל") == []


def place(head, positions, words, matched=None, root=""):
    components = tuple(SimpleNamespace(word=w, matched=m) for w, m in zip(words, matched or words))
    return SimpleNamespace(words=tuple(head.split()), positions=tuple(positions), components=components)


def test_a_head_is_named_when_the_text_holds_its_lexical_words():
    assert names_expression(place("דכל פיהא", (0, 1), ("דכל", "פיהא")))
    assert not names_expression(place("נצף מן אלשהר", (0, 1), ("נצף", "מן"))), "no month anywhere near"
    assert names_expression(place("נצף מן אלשהר", (0, 1), ("נצף", "מן")), around={"שהר"}), "the month stands in the text"
    assert names_expression(place("עקב אללה דלך כירא", (0, 2), ("יעקב", "דלך")), around={"כיר"}), "the accusative's alif"
    assert not names_expression(place("נתת בהא", (0, 1), ("נתן", "ב"))), "a word of one letter is an abbreviation"
    assert not names_expression(place("בקוה אללה", (0, 1), ("אבקאה", "אללה"), ("~בקה", "אללה"))), \
        "one lexical word reached only through a spelling fold"
    assert not names_expression(place("אלי מא יפעל", (1,), ("מא",))), "a head of function words needs them all"
    assert names_expression(place("אלי מא", (0, 1), ("אלי", "מא")))


def test_governed_pairs_across_other_words():
    assert governed_runs("יפלח", ("ומא",), ("אללה", "בה", "פי")) == ["יפלח בה"]
    assert governed_runs("בה", ("ומא", "יפלח", "אללה"), ("פי",)) == ["יפלח בה"]
    assert governed_runs("יפלח", (), ("בה",)) == [], "neighbours are a run already"
    assert governed_runs("אלחסן", ("אבו",), ("בן", "עאדי", "עלי")) == [], "beside a word of a name"
    assert governed_runs("יפלח", (), ("אללה", "תעאלי", "אלכרים", "בה")) == [], "too far"


def test_a_preposition_written_onto_the_next_word_needs_the_article():
    assert clitic_preposition("באלגאולה") == "ב" and clitic_preposition("ללשיך") == "ל" and clitic_preposition("ובאלמבלג") == "ב"
    assert clitic_preposition("בקאר") == "" and clitic_preposition("לוים") == "" and clitic_preposition("בלדנה") == ""
    assert clitic_preposition("בהא") == "", "a preposition's own pronoun form"
    assert clitic_governed("יטמע", (), ("באלגאולה",)) == [("יטמע", "באלגאולה", "ב")]
    assert clitic_governed("באלגאולה", ("יטמע",), ()) == [("יטמע", "באלגאולה", "ב")]
    assert clitic_governed("יטמע", (), ("פיה", "באלגאולה")) == [], "its own preposition stands first"


def test_only_a_finite_verb_governs_a_preposition_apart_from_it():
    index = verb_form_index([StemPair("טמע", "טמע", 1, "u1", "u1"), StemPair("נזל", "נזל", 1, "u2", "u2")])
    assert "@טמע:1" in verb_keys("יטמע", index, ALL_VERB_KINDS)
    assert verb_keys("מנזלתנא", index, ALL_VERB_KINDS) == (), "a noun that spells a verbal noun"
    assert "@טמע:1" in query_keys("טמע", index) and "@טמע:1" not in query_keys("טמע", index, EXPRESSION_VERB_KINDS), \
        "the bare perfect carries its root since keying 6; keying 5 left it out"


@pytest.fixture
def service(tmp_path, monkeypatch):
    units = [
        unit("covet", "entry", "טמע בהא", ["טמע בהא"], ["טמע"], binyan="I"),
        unit("succeed", "entry", "פלח בה", ["פלח בה"], ["פלח"], binyan="IV"),
        unit("half", "entry", "נצף", ["נצפ"], ["נצף"]),
        unit("midmonth", "subentry", "נצף מן אלשהר", ["נצפ מנ אלשהר"], parent="half"),
        unit("enter", "entry", "דכל פיהא", ["דכל פיהא"], ["דכל"], binyan="I"),
        unit("account", "subentry", "דכל פי חסאב", ["דכל פי חסאב"], parent="enter"),
    ]
    path = tmp_path / "named.db"
    write_sidecar({"recovery_version": 5, "units": units}, {"index_version": 1, "rows": []}, {}, path)
    monkeypatch.setenv("FRIEDMAN_DICTIONARY_ENABLED", "1")
    found = FriedmanService(str(path))
    assert found.is_ready(), found.unavailable_reason
    yield found
    found.close()


def named(answer):
    return [hit.unit.id for found in answer.context for hit in found.hits
            if any(p.scope in (SCOPE_HEAD, "construction") for p in hit.places)]


def test_an_inflected_verb_names_its_head(service):
    assert named(service.answer("נטמע", stems=False, after=("בה", "כתירא"))) == ["covet"]
    assert named(service.answer("יפלח", stems=False, before=("ומא",), after=("אללה", "בה", "פי", "מצר"))) == ["succeed"], \
        "a head under stem IV, the preposition one word on"
    assert named(service.answer("יטמע", stems=False, after=("באלגאולה",))) == ["covet"], "the preposition on the next word"


def test_two_words_of_a_longer_head_do_not_name_it(service):
    assert named(service.answer("נצף", stems=False, before=("דינאר",), after=("מן", "אלתמן"))) == []
    assert named(service.answer("נצף", stems=False, after=("מן", "אלשהר"))) == ["midmonth"]
    assert named(service.answer("נצף", stems=False, after=("מן", "הדא", "אלשהר"))) == ["midmonth"], "the month stands near"
    assert named(service.answer("ידכל", stems=False, after=("פי", "אלדרב"))) == ["enter"]
    assert named(service.answer("ידכל", stems=False, after=("פי", "חסאב"))) == ["account"]
