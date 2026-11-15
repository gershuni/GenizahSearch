# -*- coding: utf-8 -*-
"""Lane's tab beyond its headwords: a word that is a form of a verb stem Lane numbers under a root,
and the roots Blau's and Friedman's answers give for the word (``ArabicService.borrow``). Synthetic
sidecar only (tests/test_build_arabic_sidecar.py)."""
from __future__ import annotations

import sqlite3
from types import SimpleNamespace

from shared.arabic_normalize import root_key
from shared.arabic_service import LaneEntry, LaneLookup, LaneRoot, _in_order, _ja_strong, _letters_shown, _reads_as
from shared.ja_roots import BORROW_BLAU, BORROW_FRIEDMAN, BORROW_LANE, BorrowedRoot, borrowed_roots
from tests.test_arabic_service import _service, sidecar  # noqa: F401  (the fixture)

KTB = root_key("كتب")


def _numbered(path, entry_id: str, stem: int) -> None:
    with sqlite3.connect(path) as con:
        con.execute("UPDATE lane_entries SET text = ? WHERE id = ?", (f"{stem}. A synthetic verb.", entry_id))


def test_the_letters_a_form_keeps_and_adds():
    assert _ja_strong("דלל") == "דל" and _ja_strong("קול") == "קל" and _ja_strong("ופי") == "פ"
    assert _in_order("صرف", "وتنصرف") and not _in_order("صرف", "وتنفصر")
    assert _reads_as("صرف", "وتنصرف"), "a conjunction, a prefix and the n of stem VII"
    assert _reads_as("قر", "يقرر"), "a doubled root written out"
    assert not _reads_as("بت", "يثبت"), "a dotted letter is the root's, never a prefix"
    assert not _reads_as("حضر", "حصرت"), "the word's dot decides between the two roots"
    assert _letters_shown("נזל", "ינזל") == 3 and _letters_shown("זול", "ינזל") == 2


def test_a_form_of_a_numbered_stem_reaches_its_root_without_a_counted_reading(sidecar):  # noqa: F811
    _numbered(sidecar, "lane:t:1", 8)
    svc = _service(sidecar)
    result = svc.lookup("יכתתבון")   # yaktatibūn: stem VIII, no such word in the count
    assert [r.root_key for r in result.roots] == [KTB]
    root = result.roots[0]
    assert root.inferred and not root.borrowed
    assert (root.entries[0].entry_id, root.entries[0].match_kind, root.entries[0].matched) == ("lane:t:1", "verb_form", True)
    assert [r.skeleton for r in result.readings] == ["يكتتبون"], "the reading the form is one in"
    assert [r.root_key for r in svc.lookup("ואכתתבת").roots] == [KTB], "a conjunction peeled, a prefixed stem"
    svc.close()


def test_a_stem_lane_does_not_number_and_a_dotted_prefix_reach_nothing(sidecar):  # noqa: F811
    _numbered(sidecar, "lane:t:1", 1)
    svc = _service(sidecar)
    assert not svc.lookup("יכתתבון").roots, "stem VIII is not numbered under the root"
    svc.close()
    _numbered(sidecar, "lane:t:1", 5)
    svc = _service(sidecar)
    assert [r.root_key for r in svc.lookup("יתכתבון").roots] == [KTB]
    assert not svc.lookup("ית̇כתבון").roots, "a dotted ת is no prefix of stem V"
    svc.close()


def test_a_root_another_dictionary_gives_is_added_and_marked(sidecar):  # noqa: F811
    svc = _service(sidecar)
    nothing = svc.lookup("יכתתבון")
    assert not nothing.roots
    given = (BorrowedRoot("כתב", BORROW_BLAU, "יכתתבון (כתב VIII)"), BorrowedRoot("כתב", BORROW_FRIEDMAN, "כתב"),
             BorrowedRoot("אכ̇ד̇", BORROW_BLAU, "אכ̇ד̇"), BorrowedRoot("כתב", BORROW_LANE, "كتب"))
    result = svc.borrow(nothing, given)
    assert [r.root_key for r in result.roots] == [KTB], "אכ̇ד̇ is not in the word; Lane's own is never borrowed"
    assert result.roots[0].borrowed == ((BORROW_BLAU, "יכתתבון (כתב VIII)"), (BORROW_FRIEDMAN, "כתב"))
    assert not result.readings, "the word still has no counted reading"
    assert svc.borrow(nothing, ()) is nothing
    svc.close()


def test_lane_s_own_root_is_not_repeated_and_stays_first_when_its_entry_is_the_word(sidecar):  # noqa: F811
    svc = _service(sidecar)
    own = svc.lookup("كتاب")   # typed in Arabic letters: its own reading, whatever the synthetic count holds
    assert own.roots and own.roots[0].root_key == KTB and any(e.matched for e in own.roots[0].entries)
    assert svc.borrow(own, (BorrowedRoot("כתב", BORROW_BLAU, "כתאב (כתב)"),)) is own
    assert svc.borrow(own, (BorrowedRoot("כון", BORROW_BLAU, "כון"),)) is own, "a root the word does not hold"
    # Another root beside Lane's own: after it when one of its entries is the word, before it when
    # the root was only guessed from the letters.
    kwn = root_key("كون")

    def lane_answer(matched: bool) -> LaneLookup:
        entry = LaneEntry("x:1", "كون", None, "", False, "", matched)
        return LaneLookup("כתבכון", "ja", (), (LaneRoot(kwn, "كون", None, "", (entry,), 0),))

    given = (BorrowedRoot("כתב", BORROW_BLAU, "כתב"),)
    assert [r.root_key for r in svc.borrow(lane_answer(True), given).roots] == [kwn, KTB]
    assert [r.root_key for r in svc.borrow(lane_answer(False), given).roots] == [KTB, kwn]
    svc.close()


def test_a_root_as_printed_then_its_other_readings(sidecar):  # noqa: F811
    svc = _service(sidecar)
    assert svc._lane_keys("כתב", ()) == [KTB]
    assert svc._lane_keys("אכד", ("اخذ",)) == [root_key("أخذ")], "اكد is no root here: the reading the word has"
    assert svc._lane_keys("אכ̇ד̇", ()) == [root_key("أخذ")], "the dots as printed"
    assert svc._lane_keys("כתב אלכתאב", ()) == [] and svc._lane_keys("", ()) == []
    svc.close()


def test_a_personal_pronoun_borrows_nothing(sidecar):  # noqa: F811
    svc = _service(sidecar)
    typed = svc.lookup("نحن")
    assert typed.readings and not typed.roots
    assert svc.borrow(typed, (BorrowedRoot("נחן", BORROW_BLAU, "נחן"),)) is typed
    svc.close()


def test_lane_s_tab_is_given_the_roots_found_by_verb_form_and_never_its_own_back():
    blau = SimpleNamespace(stem=0, cards=(
        SimpleNamespace(name="דלל", root_label="דלל", matched_key="", borrowed=(("friedman", "דלל"),), verb_form="אסתדלו (דלל X)"),
        SimpleNamespace(name="קול", root_label="קול", matched_key="קול", borrowed=(("lane", "قول"),), verb_form="")))
    assert borrowed_roots(blau=blau) == ()
    assert [r.root for r in borrowed_roots(blau=blau, verb_forms=True)] == ["דלל"], "not the card Blau borrowed"
    friedman = SimpleNamespace(stem=0, matches=(
        SimpleNamespace(root="דלל", via="verb_form", matched="אסתדלו (דלל X)", unit=None),))
    assert borrowed_roots(friedman) == ()
    assert [r.root for r in borrowed_roots(friedman, verb_forms=True)] == ["דלל"]
    lane = SimpleNamespace(roots=(SimpleNamespace(name="دل", root_key="دلل", borrowed=(("blau", "דלל"),)),
                                  SimpleNamespace(name="قول", root_key="قWل", borrowed=())))
    assert [(r.root, r.source) for r in borrowed_roots(lane=lane)] == [("קול", BORROW_LANE)], "Lane's own first root"
