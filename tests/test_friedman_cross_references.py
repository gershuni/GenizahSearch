# -*- coding: utf-8 -*-
"""Friedman's cross-references ("ראה הערך חפי"): the sidecar builder keeps them in a unit's
references, and the reader names the entries to look up. Synthetic units only."""
from __future__ import annotations

import pytest

from scripts.build_friedman_sidecar import cross_references, quotation_locator, unit_references, write_sidecar
from shared.friedman_service import FriedmanService, SeeReference, see_references, see_targets
from tests.test_friedman_service import unit


def _unit(segments, references="", examples=()):
    return {"segments": [list(s) for s in segments], "gloss": {"references": references}, "examples": list(examples)}


def test_an_entry_that_is_only_a_cross_reference_keeps_it():
    unit = _unit([("head", "חפו"), ("he", ". "), ("loc", "ראה הערך "), ("head", "חפי"), ("he", ".")], references=". .")
    assert cross_references(unit) == ["ראה הערך חפי"]
    assert unit_references(unit) == "ראה הערך חפי.", "not the two full stops left around the bold name"
    assert see_targets(unit_references(unit)) == ("חפי",)


def test_the_references_stay_and_a_stem_numeral_is_kept_with_the_name():
    unit = _unit([("subentry", "❖ "), ("head", "אללה ינסי "), ("ja", "הלוואי ישכח "), ("he", "(בדרך כלל). "),
                  ("loc", "ראה הערך "), ("head", "נסי "), ("binyan", "IV"), ("he", ".")], references="(בדרך כלל). .")
    assert unit_references(unit) == "(בדרך כלל). ראה הערך נסי IV."
    assert see_targets(unit_references(unit)) == ("נסי",), "the stem is not part of what is looked up"


def test_several_entries_of_one_reference_and_a_reference_after_the_last_quotation():
    unit = _unit([("head", "אול "), ("ja", "ראשון"), ("bullet", "• "), ("ja", "קול אל אולין "), ("he", "׳תרגום׳ "),
                  ("loc", "(TS 1, שורה 2). השווה הערכים "), ("head", "סלף"), ("he", ", "), ("head", "קדמא"), ("he", ".")])
    assert cross_references(unit) == ["השווה הערכים סלף, קדמא"]
    assert see_targets(unit_references(unit)) == ("סלף", "קדמא")
    example = {"locator": "(TS 1, שורה 2). השווה הערכים", "xrefs": ["סלף", "קדמא"]}
    assert quotation_locator(example) == "(TS 1, שורה 2)", "the opening words are the unit's reference, not the locator's"
    assert quotation_locator({"locator": "(TS 1, שורה 2)", "xrefs": []}) == "(TS 1, שורה 2)"


def test_a_head_s_own_bold_and_bold_with_no_opening_words_are_no_reference():
    assert cross_references(_unit([("head", "כתב "), ("binyan", "II "), ("ja", "כתב הרבה.")])) == []
    assert cross_references(_unit([("head", "כתב "), ("ja", "כתב. "), ("he", "ובלשון אחר "), ("head", "כתאב")])) == []
    assert unit_references(_unit([("head", "כתב "), ("ja", "כתב.")], references="בלאו, שם.")) == "בלאו, שם.", (
        "references that name no entry stay as printed")


def test_the_sub_entry_before_or_after_is_a_reference_with_no_name():
    unit = _unit([("subentry", "❖ "), ("head", "מצאלחה "), ("ja", "פשרה"), ("bullet", "• "), ("ja", "מצאלחה אלשיך "),
                  ("he", "׳דמי פשרה׳ "), ("loc", "(ב26, שוליים). ראה ערך המשנה הבא.")], references="ור, שם, עמ׳ 577")
    assert cross_references(unit) == ["ראה ערך המשנה הבא"]
    assert unit_references(unit) == "ור, שם, עמ׳ 577. ראה ערך המשנה הבא."
    assert quotation_locator({"locator": "(ב26, שוליים). ראה ערך המשנה הבא.", "xrefs": []}) == "(ב26, שוליים)"
    both = _unit([("head", "דין "), ("ja", "דת. "), ("loc", "ראה ערך המשנה הקודם והערכים "), ("head", "דכל "), ("binyan", "I"),
                  ("he", ", "), ("head", "כרג "), ("binyan", "I"), ("he", ".")])
    assert cross_references(both) == ["ראה ערך המשנה הקודם והערכים דכל I, כרג I"], "once, with the entries it goes on to name"


def test_what_is_looked_up():
    assert see_targets("ראה הערך קטע I בِהِ.") == ("קטע",)
    assert see_targets("בלאו, שם. השווה הערך אَש: אِלَיש.") == ("אَש",)
    assert see_targets("ראה ערך המשנה הבא והערך נזל.") == ("נזל",)
    assert see_targets("ראה הערך חסן IV. ראה עוד הערך אَש.") == ("חסן", "אَש")
    assert see_targets("ראה הערך -הُ.") == () and see_targets("ראה ערך המשנה הקודם.") == ()
    assert see_targets("ראה ליין, מילון, עמ' 599 (שם: 'בן').") == (), "a book is no entry"
    assert see_targets("") == ()


def test_what_a_reference_says_beside_the_name():
    assert see_references("ראה הערך נסי IV.") == (SeeReference("נסי", "IV"),)
    assert see_references("ראה הערך קטע I בִהִ.") == (SeeReference("קטע", "I", "בִהִ"),)
    assert see_references("השווה הערך כט: אכד כטה.") == (SeeReference("כט", "", "אכד כטה"),)
    assert see_references("השווה ערך המשנה בה.") == (SeeReference("בה", local=True),)
    assert [r.name for r in see_references("ראה ערך המשנה הקודם והערכים דכל I, כרג I.")] == ["דכל", "כרג"]


def _referring(uid, kind, head, references, **more):
    made = unit(uid, kind, head, [head] if head else [], **more)
    made["gloss"]["references"] = references
    return made


@pytest.fixture
def service(tmp_path, monkeypatch):
    units = [
        _referring("f000001", "entry", "חפו", ". ראה הערך חפי."),
        unit("f000002", "entry", "חפי", ["חפי"], binyan="I"),
        unit("f000003", "binyan", None, [], parent="f000002", binyan="IV"),
        unit("f000004", "subentry", "בה", ["בה"], parent="f000002"),
        _referring("f000005", "entry", "נסי", "ראה הערך חפי IV."),
        _referring("f000006", "subentry", "אולא", "ראה ערך המשנה הבא.", parent="f000005"),
        _referring("f000007", "subentry", "תאניא", "השווה ערך המשנה הקודם.", parent="f000005"),
        _referring("f000008", "entry", "אכתרי", "ראה הערך אכתרי."),
        unit("f000009", "entry", "אכתרי", ["אכתרי"]),
        _referring("f000010", "entry", "מלא", "ראה הערך מלו/חפי, זזז."),
        _referring("f000011", "subentry", "תאלתא", "השווה ערך המשנה אולא.", parent="f000005"),
    ]
    path = tmp_path / "see.db"
    write_sidecar({"recovery_version": 5, "units": units}, {"index_version": 1, "rows": []}, {}, path)
    monkeypatch.setenv("FRIEDMAN_DICTIONARY_ENABLED", "1")
    found = FriedmanService(str(path))
    assert found.is_ready(), found.unavailable_reason
    yield found
    found.close()


def _referred(service, word, uid):
    [match] = [m for m in service.lookup(word).matches if m.unit.id == uid]
    return [r.id for r in match.referred]


def test_the_units_a_reference_sends_the_reader_to_come_with_the_match(service):
    assert _referred(service, "חפו", "f000001") == ["f000002"], "the entry named"
    assert service.lookup("חפו").matches[0].unit.references == "ראה הערך חפי."
    assert _referred(service, "נסי", "f000005") == ["f000003"], "the stem paragraph the reference gives"
    assert _referred(service, "אולא", "f000006") == ["f000007"], "the next sub-entry of the same entry"
    assert _referred(service, "תאניא", "f000007") == ["f000006"], "the one before"
    assert _referred(service, "תאלתא", "f000011") == ["f000006"], "a sub-entry of the same entry, by its name"
    assert _referred(service, "אכתרי", "f000008") == ["f000009"], "another entry of the same name, never itself"
    assert _referred(service, "מלא", "f000010") == ["f000002"], "by the other spelling; a name no entry has brings none"
    assert _referred(service, "חפי", "f000002") == [], "no reference, nothing brought"
