# -*- coding: utf-8 -*-
"""shared/friedman_service.py on a synthetic sidecar written by scripts/build_friedman_sidecar.py.

No dictionary text in the repository. Pinned: the reader serves only a file that passes every
check and says why otherwise; the switch defaults to off; a lookup reaches units by head key,
by peeled stem and by the book's index of forms through the DOTTED root; an entry brings its
sub-entries, a sub-entry its entry; results are bounded; nothing raises.
"""
from __future__ import annotations

import sqlite3

import pytest

from scripts.build_friedman_sidecar import STATUS, write_sidecar
from shared import friedman_service as fs
from shared.friedman_service import FriedmanService, get_friedman_service, reset_friedman_service
from shared.ja_roots import BORROW_BLAU, BORROW_LANE, BorrowedRoot

VIEWER = "https://ja.genizah.org/JA_Dictionary_pdfjs/web/viewer.html?file=PDF/"


def unit(uid, kind, head, keys, dotted=(), parent=None, binyan=None, examples=(), letter="א", page="1"):
    return {"id": uid, "kind": kind, "letter": letter, "parent": parent, "binyan": binyan,
            "head": None if head is None else {"text": head, "plain": head, "root": head, "root_source": "single_word",
                                                "root_candidate": None, "root_dotted": list(dotted), "lemma": head,
                                                "alternatives": [head], "frames": [], "notes": [], "keys": list(keys)},
            "gloss": {"definition": f"def {uid}", "references": "", "foreign": "", "xrefs": [], "other": "",
                      "locator": ""},
            "examples": [{"quote": q, "translation": t, "locator": loc, "foreign": "", "xrefs": [], "other": ""}
                         for q, t, loc in examples],
            "segments": [], "text": f"text {uid}", "flags": ["no_quotations"] if not examples else [],
            "locations": [{"source_id": "1", "pdf_page": 1, "printed_page": page, "paragraph": 0,
                           "bbox": [0, 0, 1, 1], "chars": 5}]}


def units_result():
    return {"recovery_version": 4, "units": [
        unit("f000001", "entry", "אבד", ["אבד"], ["אבד"], examples=[("לא אבד מן", "'no escape'", "(ח1, שורה 2)")]),
        unit("f000002", "subentry", "לא אבד", ["לא אבד"], parent="f000001"),
        unit("f000003", "binyan", None, [], parent="f000001", binyan="V"),
        unit("f000004", "entry", "ג̇לב", ["גלב"], ["ג̇לב"], letter="ג̇",
             examples=[("בעון אללה ותאיידה 12", "'with God's help'", "(ח2, שורה 3)")]),
        unit("f000005", "entry", "גלב", ["גלב"], ["גלב"], letter="ג"),
        unit("f000006", "entry", "קלב", ["קלב"], ["קלב"], letter="ק",
             examples=[("קלב אלאמר ואללה", "'turned the matter'", "(ח3, שורה 1)"),
                       ("ואללה אללה אחרז נפסך", "'by God, guard yourself'", "(ח3, שורה 4)")]),
        unit("f000007", "subentry", "לִאַ̇גְלִ דַ̇לִךְ/לִאַ̇גְלִ פִעְלٍ", ["לאגל דלך", "לאגל פעל"], parent="f000004"),
        unit("f000008", "subentry", "מא (ולא רק לא) בד ממא", ["מא בד ממא"], parent="f000005"),  # a note inside the head
    ]}


def index_result():
    def row(form, *roots):
        return {"form": form, "form_key": fs.normalize_ja(form), "line": form, "pdf_page": 1, "printed_page": "969",
                "url": f"{VIEWER}Value_Keys.pdf#page=1",
                "roots": [{"text": r, "dotted": r, "key": fs.normalize_ja(r), "alternatives": 1} for r in roots]}
    return {"index_version": 1, "rows": [row("אגלב", "ג̇לב"), row("אבאד", "אבד"), row("תקלב", "קלב", "קלוב"),
                                         row("אבריסם")]}


@pytest.fixture
def sidecar(tmp_path, monkeypatch):
    path = tmp_path / "friedman.db"
    write_sidecar(units_result(), index_result(), {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, path)
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    return path


def test_switch_defaults_off_and_lookup_is_empty_but_safe(sidecar, monkeypatch):
    monkeypatch.delenv(fs.ENV_ENABLED, raising=False)
    service = FriedmanService(str(sidecar))
    assert service.is_ready() and not service.is_available() and not fs.friedman_enabled()
    result = service.lookup("אבד")
    assert not result.found and result.status == STATUS and result.credit_he
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    assert service.is_available() and service.lookup("אבד").found


def test_an_editors_switch_serves_editors_only_and_the_lookup_answers(sidecar, monkeypatch):
    """FRIEDMAN_DICTIONARY_ENABLED=editors: a preview for editors before release (owner, 2026-11-11)."""
    monkeypatch.setenv(fs.ENV_ENABLED, "editors")
    service = FriedmanService(str(sidecar))
    assert not fs.friedman_enabled() and not service.is_available()
    assert service.served_to(True) and not service.served_to(False)
    assert service.lookup("אבד").found and service.expression("לא אבד").status == STATUS
    monkeypatch.delenv(fs.ENV_ENABLED)
    assert not service.served_to(True) and not service.lookup("אבד").found, "unset is off"


def test_head_stem_and_index_routes(sidecar):
    service = FriedmanService(str(sidecar))
    direct = service.lookup("אַבַּד")
    assert [(m.unit.id, m.via, m.matched) for m in direct.matches] == [("f000001", "head", "אבד")]
    match = direct.matches[0]
    assert [c.id for c in match.children] == ["f000002", "f000003"] and match.children_total == 2
    assert match.unit.quotations[0].quote == "לא אבד מן" and match.unit.quotations_total == 1
    assert match.unit.pdf_url == f"{VIEWER}1.pdf#page=1" and direct.status == STATUS
    peeled = service.lookup("ואבד")
    assert [(m.unit.id, m.via, m.matched) for m in peeled.matches] == [("f000001", "stem", "אבד")]
    with_harakat = service.lookup("וﱞאבד")   # a presentation-form shadda the PDF stores inside the word
    assert [(m.unit.id, m.via, m.matched) for m in with_harakat.matches] == [("f000001", "stem", "אבד")], \
        "the clitic is peeled off the cleaned key, not the raw word"
    sub = service.lookup("לא אבד")
    assert [(m.unit.id, m.via) for m in sub.matches] == [("f000002", "head")] and sub.matches[0].parent.id == "f000001"
    dotted = service.lookup("אגלב")
    assert [(m.unit.id, m.via, m.matched) for m in dotted.matches] == [("f000004", "index", "אגלב (ג̇לב)")], \
        "the dotted root reaches ג̇לב only, never גלב"
    both = service.lookup("גלב")
    assert [m.unit.id for m in both.matches] == ["f000004", "f000005"], "the plain key reaches both readings"
    fallback = service.lookup("תקלב")
    assert [(m.unit.id, m.matched) for m in fallback.matches] == [("f000006", "תקלב (קלב)")], \
        "a root the index spells differently falls back to the plain key; an unknown root reaches nothing"
    assert not service.lookup("אבריסם").found and not service.lookup("קקק").found and not service.lookup("").found
    assert not service.lookup("abc").found


def test_results_are_bounded_and_unit_by_id(sidecar):
    service = FriedmanService(str(sidecar))
    result = service.lookup("גלב", limit=1)
    assert [m.unit.id for m in result.matches] == ["f000004"] and result.truncated
    assert service.unit("f000002").parent == "f000001" and service.unit("nope") is None
    assert fs.MAX_QUOTATIONS >= 1 and fs.MAX_CHILDREN >= 1


def test_reader_refuses_a_missing_or_foreign_file(tmp_path, sidecar):
    missing = FriedmanService(str(tmp_path / "none.db"))
    assert not missing.is_ready() and "no sidecar" in missing.unavailable_reason
    assert not missing.lookup("אבד").found
    other = tmp_path / "other.db"
    with sqlite3.connect(other) as conn:
        conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
    foreign = FriedmanService(str(other))
    assert not foreign.is_ready() and "missing table" in foreign.unavailable_reason
    with sqlite3.connect(sidecar) as conn:
        conn.execute("UPDATE meta SET value = 'x' WHERE key = 'normalizer_version'")
    stale = FriedmanService(str(sidecar))
    assert not stale.is_ready() and "normalizer" in stale.unavailable_reason


def _places(result):
    return [(h.unit.id, p.scope, p.n, p.positions, [(c.printed, c.via) for c in p.components])
            for h in result.hits for p in h.places]


def test_expression_matches_in_order_inside_one_quotation_or_one_head(sidecar):
    service = FriedmanService(str(sidecar))
    both = service.expression("לא אבד", gap=0)
    assert both.words == ("לא", "אבד") and both.gap == 0 and both.status == STATUS and both.credit_he
    assert _places(both) == [("f000002", fs.SCOPE_HEAD, 0, (0, 1), [("לא", "word"), ("אבד", "word")]),
                             ("f000001", fs.SCOPE_QUOTATION, 1, (0, 1), [("לא", "word"), ("אבד", "word")])], \
        "the head that IS the expression comes first, then the quotation; both say which word carried which"
    assert both.hits[0].parent.id == "f000001" and both.hits[1].parent is None
    assert both.hits[1].places[0].quotation.translation == "'no escape'"
    assert both.hits[1].places[0].words == ("לא", "אבד", "מן")
    assert not service.expression("לא מן", gap=0).found, "a word between is not allowed at gap 0"
    assert not service.expression("לא מן", gap=1).found, "two function words apart in a stranger's quotation are not shown"
    assert _places(service.expression("ואבד מן", gap=0)) == [("f000001", fs.SCOPE_QUOTATION, 1, (1, 2),
                                                             [("אבד", "stem"), ("מן", "word")])]
    assert not service.expression("אבד לא", gap=5).found, "order matters"
    assert not service.expression("אבד ותאיידה", gap=5).found, "never one word from one unit and one from another"
    assert not service.expression("אללה אלאמר", gap=5).found, "never across two quotations of one unit"


def test_expression_clitic_alternatives_on_either_side_and_repeated_words(sidecar):
    service = FriedmanService(str(sidecar))
    typed_with_clitic = service.expression("ואבד מן", gap=0)
    assert _places(typed_with_clitic) == [("f000001", fs.SCOPE_QUOTATION, 1, (1, 2), [("אבד", "stem"), ("מן", "word")])]
    assert typed_with_clitic.candidates[0] == ("ואבד", "אבד")
    printed_with_clitic = service.expression("עון אללה", gap=0)
    assert _places(printed_with_clitic) == [("f000004", fs.SCOPE_QUOTATION, 1, (0, 1), [("בעון", "stem"), ("אללה", "word")])]
    repeated = service.expression("אללה אללה", gap=0)
    assert _places(repeated) == [("f000006", fs.SCOPE_QUOTATION, 2, (0, 1), [("ואללה", "stem"), ("אללה", "word")])], \
        "a repeated word needs two printed words: ואללה alone (two keys, one word) is no match"
    alternatives = service.expression("לאגל פעל", gap=0)
    assert _places(alternatives) == [("f000007", fs.SCOPE_HEAD, 0, (1, 2), [("דַ̇לִךְ/לִאַ̇גְלִ", "word"), ("פִעְלٍ", "word")])], \
        "a printed word with a slash answers to each of its spellings, by the spelling itself (not a peeled one)"
    assert _places(service.expression("לאגל דלך", gap=0)) == [("f000007", fs.SCOPE_HEAD, 0, (0, 1),
                                                              [("לִאַ̇גְלִ", "word"), ("דַ̇לִךְ/לִאַ̇גְלִ", "word")])]
    numbered = service.expression("אללה 12", gap=0)
    assert not numbered.found and numbered.candidates == (("אללה", "אלל"), ()), \
        "a word without Hebrew letters finds nothing; לה (the article peeled) is too short to be a key"


def test_expression_is_bounded_and_fails_closed(sidecar, monkeypatch):
    service = FriedmanService(str(sidecar))
    assert not service.expression("אבד").found and not service.expression("").found
    assert not service.expression("א ב ג ד ה ו ז", gap=1).found, "seven words are too many"
    assert service.expression("לא אבד", gap=99).gap == fs.MAX_GAP and service.expression("לא אבד", gap="x").gap == fs.DEFAULT_GAP
    cut = service.expression("לא אבד", gap=0, limit=1)
    assert [h.unit.id for h in cut.hits] == ["f000002"] and cut.truncated
    two_places = service.expression("אללה", gap=0)
    assert not two_places.found, "one word is the lookup's, not the expression's"
    monkeypatch.delenv(fs.ENV_ENABLED, raising=False)
    off = service.expression("לא אבד")
    assert not off.found and off.status == STATUS and off.word == "לא אבד"


def test_ordered_match_over_keyed_alternatives():
    items = [(0, "ואללה"), (0, "אללה"), (1, "אללה"), (3, "אחרז")]
    assert fs.ordered_match(items, [{"אללה"}, {"אללה"}], 0) == (1, 2)
    assert fs.ordered_match(items, [{"ואללה", "אללה"}, {"אחרז"}], 1) == (2, 3)
    assert fs.ordered_match(items, [{"ואללה", "אללה"}, {"אחרז"}], 0) is None
    assert fs.ordered_match(items, [{"אללה"}, {"אללה"}, {"אללה"}], 5) is None, "two keys of one word are one occurrence"
    assert fs.ordered_match(["a", "b"], ["b"], 0) == (1,) and fs.ordered_match([], ["a"], 0) is None
    # The tightest occurrence wins wherever it stands (Codex, round 5 of #487); of equal spans the earlier.
    assert fs.ordered_match(list("axbab"), ["a", "b"], 1) == (3, 4), "a contiguous occurrence after a gapped one"
    assert fs.ordered_match(list("axbaxb"), ["a", "b"], 1) == (0, 2)
    assert fs.ordered_match(list("axxbaxb"), ["a", "b"], 2) == (4, 6), "a narrower gap after a wider one"
    assert fs.ordered_match(list("abxcabc"), ["a", "b", "c"], 1) == (4, 5, 6)
    assert fs.ordered_match([(0, "a"), (1, "x"), (2, "b"), (3, "a"), (3, "c"), (4, "b")], [{"a"}, {"b"}], 1) == (3, 5)
    # Of equal spans the earlier word by word: with the words repeated the first b is the match
    # (Codex, round 6 of #487), yet a later b is still taken where the earlier cannot reach.
    assert fs.ordered_match(list("abba"), ["a", "b", "a"], 1) == (0, 1, 3)
    assert fs.ordered_match(list("abbxa"), ["a", "b", "a"], 1) == (0, 2, 4), "the second b reaches the last a; the first does not"
    assert fs.ordered_match([(0, "a"), (1, "b"), (2, "xb"), (2, "b"), (3, "a")], [{"a"}, {"b"}, {"a"}], 1) == (0, 1, 4),         "the first b as itself, not the second through an alternative"
    # A word whose own key restarts the query must not take the earlier path away from its
    # other keys: בהא בהא is בהא then ב (the second word's peeled preposition) (Codex, round 7).
    both = [(pos, key) for pos, key, _ in fs.scope_keys("בהא בהא")]
    assert both == [(0, "בהא"), (0, "ב"), (1, "בהא"), (1, "ב")]
    assert fs.ordered_match(both, [{"בהא"}, {"ב"}], 0) == (0, 3)
    assert fs.scope_keys("בעון אללה 12") == [(0, "בעונ", 0), (0, "עונ", 1), (1, "אללה", 0), (1, "אלל", 1)]
    assert fs.word_keys("לה") == ("לה", "ל"), "a word's own key is a key however short; its preposition too"


def test_a_note_inside_the_head_is_not_among_its_words(sidecar):
    assert fs.head_scope_text("קצר: מא (א)קצר (ולא רק פי) מעה ") == "קצר: מא (א)קצר מעה (ולא רק פי)", "notes come last"
    assert fs.scope_keys(fs.head_scope_text("אבד: לא אבד (=לא בד)"))[-2:] == [(3, "לא", 0), (4, "בד", 0)], "a note's words are searchable"
    service = fs.FriedmanService(str(sidecar))
    hit = next(h for h in service.expression("מא בד ממא", 0).hits if h.unit.id == "f000008")
    place = next(p for p in hit.places if p.scope == fs.SCOPE_HEAD)
    assert place.words == ("מא", "(ולא", "רק", "לא)", "בד", "ממא") and place.positions == (0, 4, 5), "shown in the printed order; the note widened no gap"
    assert place.search_positions == (0, 1, 2) and place.span == 2 and place.contiguous, \
        "the span and the gaps are the search's: the note's words come after the head's (Codex, round 8)"
    quotation_place = next(p for h in service.expression("לא אבד", 0).hits for p in h.places if p.scope == fs.SCOPE_QUOTATION)
    assert quotation_place.search_positions == () and quotation_place.span == 1, "a quotation's orders agree"
    assert fs.head_scope_map("מא (ולא רק לא) בד ממא") == (0, 4, 5, 1, 2, 3) and fs.head_scope_map("קצר") == (0,)
    service.close()


def test_expression_hits_come_in_tiers_and_function_words_only_stand_together(tmp_path, monkeypatch):
    """Own head first, then units whose head (or entry's head) holds the words, then the rest,
    contiguous before gapped; an expression of function words only shows no gapped stranger."""
    result = {"recovery_version": 4, "units": [
        unit("f000001", "entry", "אללה עליה", ["אללה עליה"], examples=[("חתם אללה עליה באלמות", "", "(ח1)")]),
        unit("f000002", "subentry", "-ה", [], parent="f000001", examples=[("קצא אללה אלמחתום עלי מן", "", "(ח2)")]),
        unit("f000003", "entry", "נעם", ["נעם"], examples=[("אנעם אללה עליה בכדא", "", "(ח3)")]),
        unit("f000004", "entry", "נטק", ["נטק"], examples=[("פאנטק אללה חכמה עלי לסאנה", "", "(ח4)")]),
        unit("f000005", "entry", "כתב כטה", ["כתב כטה"]),
        unit("f000006", "entry", "שהאדה", ["שהאדה"], examples=[("ראובן כתב כטה שהאדה", "", "(ח6)")]),
        unit("f000007", "entry", "פלאן", ["פלאן"], examples=[("כתב פלאן כטה", "", "(ח7)")]),
        unit("f000008", "entry", "כטה כתב", ["כטה כתב"], examples=[("וכתב פלאן כטה", "", "(ח8)")]),   # the words reversed in the head
        unit("f000009", "entry", "כתב אלשאהד עלי אלשטר כטה", ["כתב אלשאהד עלי אלשטר כטה"],
             examples=[("כתב כטה פי אלשטר", "", "(ח9)")]),                                            # in order, beyond the reader's gap
        unit("f000010", "entry", "אחד", ["אחד"], examples=[("לא אבד מן דלך לא מן אחד", "", "(ח10)")]),  # gapped, then together
    ]}
    path = tmp_path / "friedman.db"
    write_sidecar(result, index_result(), {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, path)
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    service = FriedmanService(str(path))
    found = service.expression("אללה עליה", 2)
    assert [h.unit.id for h in found.hits] == ["f000001", "f000002", "f000003"] and not found.truncated
    assert [p.contiguous for p in found.hits[1].places] == [False], "the entry's own sub-entry may be gapped"
    assert "f000004" not in [h.unit.id for h in service.expression("אללה עליה", 5).hits], "a gapped stranger is not shown"
    assert [h.unit.id for h in service.expression("כתב כטה", 2).hits] == ["f000005", "f000009", "f000006", "f000007", "f000008"], \
        "the exact head; then the head holding the words in order beyond the reader's gap; then strangers, contiguous first; a reversed head is a stranger"
    assert fs._is_function_word(fs.word_keys("ואללה")) and fs._is_function_word(fs.word_keys("עליהם")) and not fs._is_function_word(fs.word_keys("כטה"))
    together = service.expression("לא מן", 1)
    assert [(h.unit.id, h.places[0].positions, h.places[0].contiguous) for h in together.hits] == [("f000010", (4, 5), True)], \
        "a stranger's quotation holding the function words apart and then together is shown by the latter (Codex, round 5)"
    service.close()


def test_short_clitic_remainders_and_preposition_stems():
    assert fs.word_keys("ולא") == ("ולא", "לא") and fs.word_keys("לסד") == ("לסד", "סד")
    assert fs.word_keys("אלהם") == ("אלהמ", "המ"), "after the article a two-letter remainder is a word"
    assert fs.word_keys("אללה") == ("אללה", "אלל") and "לה" not in fs.word_keys("ואללה"), "the name never leaves לה behind"
    assert fs.word_keys("בהא") == ("בהא", "ב") and fs.word_keys("להם") == ("להמ", "ל"), "a pronoun after a proclitic is not a word; its preposition is"
    assert fs.word_keys("בה") == ("בה", "ב") and fs.word_keys("מנה") == ("מנה", "מנ") and fs.word_keys("עליך") == ("עליכ", "עלי")
    keys = fs.word_keys("ביני")
    assert keys[0] == "ביני" and "בינ" in keys and "ב" not in keys, "a preposition stem only of a key, not of a peeled scrap"
    assert fs.word_keys("(א)קצר") == ("אקצר", "קצר"), "a printed head's optional letters key both ways"
    tokens = [(pos, key) for pos, key, _ in fs.scope_keys("אנה קאדר עלי דלך ומלי בה")]
    found = fs.ordered_match(tokens, [set(fs.word_keys("מלי")), set(fs.word_keys("בהא"))], 0)
    assert tuple(tokens[i][0] for i in found) == (4, 5), "ומלי בה is מלי בהא"


def test_singleton_and_thread_safe_connection(sidecar, monkeypatch):
    monkeypatch.setenv(fs.ENV_DB_PATH, str(sidecar))
    reset_friedman_service()
    try:
        service = get_friedman_service()
        assert service is get_friedman_service() and service.is_ready()
        assert service.lookup("אבד").found
    finally:
        reset_friedman_service()
    assert fs.default_sidecar_path() == str(sidecar)


def _quote_places(result):
    return [(h.unit.id, h.parent.id if h.parent else None, h.places_total,
             [(p.scope, p.n, p.positions, [(c.printed, c.via) for c in p.components]) for p in h.places])
            for h in result.quotes]


def test_a_word_inside_a_quotation_or_a_longer_head_is_found_with_its_place_marked(sidecar):
    service = FriedmanService(str(sidecar))
    quoted = service.lookup("אחרז")
    assert quoted.matches == () and quoted.found and quoted.more_quotes == 0
    assert _quote_places(quoted) == [("f000006", None, 1, [(fs.SCOPE_QUOTATION, 2, (2,), [("אחרז", "word")])])]
    assert quoted.quotes[0].places[0].quotation.translation == "'by God, guard yourself'"
    peeled = service.lookup("אמר")
    assert _quote_places(peeled) == [("f000006", None, 1, [(fs.SCOPE_QUOTATION, 1, (1,), [("אלאמר", "stem")])])]
    in_head = service.lookup("לאגל")
    assert [(h.unit.id, h.parent.id, [p.scope for p in h.places]) for h in in_head.quotes] == [
        ("f000007", "f000004", [fs.SCOPE_HEAD])], "a word of a longer head reaches the sub-entry, under its entry"


def test_quotes_leave_out_the_matches_their_sub_entries_function_words_and_several_words(sidecar):
    service = FriedmanService(str(sidecar))
    matched = service.lookup("אבד")
    assert [m.unit.id for m in matched.matches] == ["f000001"] and matched.quotes == ()
    direct, _more = service._quotes("אבד", set(), set())
    assert sorted(h.unit.id for h in direct) == ["f000001", "f000002"], "only the match keeps its own units out"
    assert service.lookup("אללה").quotes == (), "a function word is in every other quotation"
    assert service.lookup("אחרז נפסך").quotes == (), "several words are never looked up one by one"
    assert not service.lookup("קקק").found


def test_quotes_are_bounded_and_a_common_word_has_none(sidecar, monkeypatch):
    service = FriedmanService(str(sidecar))
    monkeypatch.setattr(fs, "MAX_QUOTE_UNITS", 0)
    assert not service.lookup("אחרז").found
    monkeypatch.setattr(fs, "MAX_QUOTE_UNITS", 25)
    monkeypatch.setattr(fs, "MAX_QUOTE_HITS", 0)
    result = service.lookup("אחרז")
    assert result.quotes == () and result.more_quotes == 1


# ---------------------------------------------------------------------------
# A root's stems (owner, 2026-11-11; the private dictionary lookup plan, section 4.6): the stems an
# entry lists (its own label and its paragraphs', noisy labels read as every numeral they name), a
# root typed with a stem ("דלל X") answering with that stem's paragraph, and the dot typed as an
# apostrophe. Synthetic units, no dictionary text.
# ---------------------------------------------------------------------------

def stem_units():
    return {"recovery_version": 4, "units": [
        # A noun that lists וצל as a later spelling, earlier in the book than the verb וצל itself.
        unit("f000091", "entry", "אצל/אצל/וצל ר' אצול", ["אצל", "וצל", "אצול"], ["אצל"], letter="א"),
        unit("f000092", "entry", "וצל", ["וצל"], ["וצל"], letter="ו"),
        unit("f000101", "entry", "דלל אן", ["דלל"], ["דלל"], binyan="I", letter="ד"),
        unit("f000102", "binyan", "עליה", [], parent="f000101", binyan="II(<IV)"),   # names II and IV
        unit("f000103", "binyan", None, [], parent="f000101", binyan="IV"),
        unit("f000104", "subentry", "-ה אן", [], parent="f000101"),
        unit("f000105", "binyan", "מנהא", [], parent="f000101", binyan="X"),
        unit("f000106", "entry", "ד̇לל", ["דלל"], ["ד̇לל"], binyan="(", letter="ד̇"),  # a label naming none
        unit("f000107", "binyan", None, [], parent="f000106", binyan="IV"),
        unit("f000108", "entry", "קרב", ["קרב"], ["קרב"], binyan="V", letter="ק"),
        unit("f000109", "entry", "כתב", ["כתב"], ["כתב"], letter="כ"),
    ]}


@pytest.fixture
def stems(tmp_path, monkeypatch):
    path = tmp_path / "friedman.db"
    write_sidecar(stem_units(), {"index_version": 1, "rows": []},
                  {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, path)
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    return FriedmanService(str(path))


def _matches(result):
    return [(m.unit.id, m.via, m.stem) for m in result.matches]


@pytest.fixture
def indexed_stems(tmp_path, monkeypatch):
    """The stem units, and the book's index giving אסתדלו and יקרב their roots."""
    def row(form, root):
        return {"form": form, "form_key": fs.normalize_ja(form), "line": form, "pdf_page": 1, "printed_page": "969",
                "url": f"{VIEWER}Value_Keys.pdf#page=1",
                "roots": [{"text": root, "dotted": root, "key": fs.normalize_ja(root), "alternatives": 1}]}
    path = tmp_path / "friedman.db"
    write_sidecar(stem_units(), {"index_version": 1, "rows": [row("אסתדלו", "דלל"), row("יקרב", "קרב")]},
                  {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, path)
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    return FriedmanService(str(path))


def test_an_entry_the_index_reached_opens_on_the_stem_the_word_s_shape_gives(indexed_stems):
    """Owner, 2026-11-11: the index gives אסתקר the root קרר, and the entry opened on its own stem. The
    word's shape (אסת + the root: X) opens the stem X paragraph, the index still named as the way in;
    a word no listed stem holds opens the entry as before."""
    [match] = indexed_stems.lookup("אסתדלו").matches
    assert (match.unit.id, match.via, match.stem, match.matched) == ("f000105", "index", 10, "אסתדלו (דלל)")
    assert match.parent is not None and match.parent.id == "f000101" and match.stems == (1, 2, 4, 10)
    [entry] = indexed_stems.lookup("יקרב").matches
    assert (entry.unit.id, entry.via, entry.stem) == ("f000108", "index", 0), "V's forms are תקרב, יתקרב"


def test_friedman_borrows_the_entries_of_the_roots_blau_and_lane_give(stems):
    """Owner, 2026-11-11: ויכון reached nothing in Friedman though Lane gave كون. The entries filed under
    a borrowed root come after Friedman's own, the one spelled with the same dots, each opened on the
    word's stem; an entry already shown gets no second match; two sources make one match naming both."""
    nothing = stems.lookup("אסתדלו")
    assert nothing.matches == ()
    result = stems.borrow(nothing, (BorrowedRoot("דלל", BORROW_BLAU, "דלל"), BorrowedRoot("דלל", BORROW_LANE, "دلل")))
    assert [(m.unit.id, m.via, m.stem, m.borrowed) for m in result.matches] == [
        ("f000105", fs.VIA_BORROWED, 10, (("blau", "דלל"), ("lane", "دلل")))]
    dotted = stems.borrow(nothing, (BorrowedRoot("ד̇לל", BORROW_LANE, "ذلل"),))
    assert [(m.unit.id, m.stem) for m in dotted.matches] == [("f000106", 0)], "ד̇לל lists IV only: the entry"
    agreed = stems.borrow(nothing, (BorrowedRoot("קרב", BORROW_BLAU, "קרב"), BorrowedRoot("דלל", BORROW_BLAU, "דלל"),
                                    BorrowedRoot("דלל", BORROW_LANE, "دلل")))
    assert [m.unit.id for m in agreed.matches] == ["f000105", "f000108"], "the entry two dictionaries give first"
    own = stems.lookup("דלל")
    assert stems.borrow(own, (BorrowedRoot("דלל", BORROW_BLAU, "דלל"),)).matches == own.matches
    chosen = stems.lookup("דלל X")
    assert stems.borrow(chosen, (BorrowedRoot("קרב", BORROW_LANE, "قرب"),)) is chosen, "a typed stem borrows nothing"
    assert stems.borrow(nothing, ()) is nothing


def test_the_verb_form_index_is_built_once_from_the_listed_pairs(stems, monkeypatch):
    """PR 2 (plan 4.3): the book's pairs, three-letter roots only, every form they may be written as."""
    index = stems.verb_form_index()
    assert [(p.entry_id, p.unit_id, p.stem) for p in index["אסתדלו"]] == [("f000101", "f000105", 10)]
    assert stems.verb_form_index() is index, "built once"

    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    fresh = FriedmanService(stems.db_path)
    monkeypatch.setattr(fresh, "_stems", boom)
    assert fresh.verb_form_index() == {}, "fails closed"


def test_friedman_s_borrowing_fails_closed(stems, monkeypatch):
    nothing = stems.lookup("אסתדלו")

    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(stems, "_borrow", boom)
    assert stems.borrow(nothing, (BorrowedRoot("דלל", BORROW_BLAU, "דלל"),)) is nothing


def test_the_stem_pairs_read_every_numeral_of_a_label(stems):
    assert [(p.entry_id, p.stem, p.unit_id, p.root, p.root_key) for p in stems.stem_pairs()] == [
        ("f000101", 1, "f000101", "דלל", "דלל"),
        ("f000101", 2, "f000102", "דלל", "דלל"), ("f000101", 4, "f000102", "דלל", "דלל"),
        ("f000101", 4, "f000103", "דלל", "דלל"), ("f000101", 10, "f000105", "דלל", "דלל"),
        ("f000106", 4, "f000107", "ד̇לל", "דלל"),
        ("f000108", 5, "f000108", "קרב", "קרב"),
    ], 'the entry\'s own stem is a pair; "(" names no stem and gives none'
    assert stems.entry_stems("f000101") == (1, 2, 4, 10) and stems.entry_stems("f000106") == (4,)
    assert stems.entry_stems("f000108") == (5,) and stems.entry_stems("f000109") == ()
    assert stems.entry_stems("f000102") == () and stems.entry_stems("nope") == ()


def test_a_plain_lookup_carries_each_entry_s_stems_and_root(stems):
    result = stems.lookup("דלל")
    assert [(m.unit.id, m.stems, m.root, m.stem, m.dot_mismatch) for m in result.matches] == [
        ("f000101", (1, 2, 4, 10), "דלל", 0, False), ("f000106", (4,), "ד̇לל", 0, False)]
    assert [c.id for c in result.matches[0].children] == ["f000102", "f000103", "f000104", "f000105"]
    assert result.stem == 0 and not result.stem_missing


@pytest.mark.parametrize("typed", ["דלל X", "דלל 10", "X דלל"])
def test_a_typed_stem_answers_with_that_stem_s_paragraph(stems, typed):
    result = stems.answer(typed)
    assert isinstance(result, fs.FriedmanLookup), "a root with a stem is never an expression search"
    assert _matches(result) == [("f000105", fs.VIA_CHOSEN, 10)] and result.stem == 10 and not result.stem_missing
    [match] = result.matches
    assert match.parent.id == "f000101" and match.stems == (1, 2, 4, 10) and match.root == "דלל"
    assert result.quotes == () and result.word == " ".join(typed.split())


@pytest.mark.parametrize("typed", ["דלל X", "דלל 10"])
def test_words_from_the_manuscript_are_never_a_typed_stem(stems, typed):
    """``stems=False`` (words clicked or selected in the manuscript): two words are the expression
    search, whatever the second one looks like."""
    assert isinstance(stems.answer(typed, stems=False), fs.FriedmanExpression)


def test_a_noisy_label_and_several_entries_of_the_root(stems):
    assert _matches(stems.lookup("דלל IV")) == [("f000102", "chosen", 4), ("f000103", "chosen", 4),
                                                ("f000107", "chosen", 4)]
    assert _matches(stems.lookup("דלל II")) == [("f000102", "chosen", 2)]


def test_the_entry_s_own_stem_is_the_entry_its_children_carried_for_a_click(stems):
    """The entry is stem I's paragraph; its other units come with it, and the panel shows them only on a
    click (owner, 2026-11-11: nothing only on the PDF page; web/components/friedman_panel.py)."""
    [match] = stems.lookup("דלל I").matches
    assert (match.unit.id, match.stem, len(match.children), match.children_total) == ("f000101", 1, 4, 4)
    assert _matches(stems.lookup("קרב V")) == [("f000108", "chosen", 5)]


def test_a_dotted_root_puts_the_dotted_entry_first_and_marks_the_other(stems):
    result = stems.lookup("ד'לל IV")
    assert [(m.unit.id, m.dot_mismatch) for m in result.matches] == [
        ("f000107", False), ("f000102", True), ("f000103", True)]
    only_undotted = stems.lookup("ד'לל X")
    assert [(m.unit.id, m.dot_mismatch) for m in only_undotted.matches] == [("f000105", True)]


@pytest.mark.parametrize("word", ["ד'לל", "ד׳לל", "ד̇לל"])
def test_a_word_written_with_the_dot_puts_the_dotted_reading_first(stems, word):
    result = stems.lookup(word)
    assert [(m.unit.id, m.dot_mismatch) for m in result.matches] == [("f000106", False), ("f000101", True)]


def test_a_stem_the_root_does_not_list_says_so_and_shows_the_root(stems):
    result = stems.lookup("דלל III")
    assert result.stem == 3 and result.stem_missing
    assert [(m.unit.id, m.via, m.stem, m.stems) for m in result.matches] == [
        ("f000101", "head", 0, (1, 2, 4, 10)), ("f000106", "head", 0, (4,))]
    assert len(result.matches[0].children) == 4, "the root as a plain lookup shows it"
    none = stems.lookup("כתב II")
    assert none.stem_missing and [(m.unit.id, m.stems) for m in none.matches] == [("f000109", ())]
    unknown = stems.lookup("קקק X")
    assert not unknown.found and not unknown.stem_missing


def test_an_entry_headed_by_the_word_comes_before_one_that_lists_it_as_another_spelling(stems):
    assert [(m.unit.id, m.via) for m in stems.lookup("וצל").matches] == [("f000092", "head"), ("f000091", "head")], \
        "the verb וצל before the noun אצל/אצל/וצל, though the noun is earlier in the book (owner, 2026-11-11)"
    assert [m.unit.id for m in stems.lookup("אצל").matches] == ["f000091"]
    assert [m.unit.id for m in stems.lookup("ווצל").matches] == ["f000092", "f000091"], "a peeled candidate too"
    assert fs.head_begins_with("אَצْל/אُצْל/וَצْל", "אצל") and not fs.head_begins_with("אَצْל/אُצْל/וَצْל", "וצל")
    assert fs.head_begins_with("אול בِהِ אלאَמْרُ", "אול"), "a head that begins with the word, then a complement"
    assert fs.head_begins_with("דלל אן", "דלל אנ") and not fs.head_begins_with("דלל אן", "אנ")
    assert not fs.head_begins_with(None, "וצל") and not fs.head_begins_with("", "וצל")


def test_the_stems_fail_closed(stems, monkeypatch):
    monkeypatch.delenv(fs.ENV_ENABLED)
    assert stems.stem_pairs() == () and not stems.lookup("דלל X").found
    monkeypatch.setenv(fs.ENV_ENABLED, "1")

    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(stems, "_stems", boom)
    assert stems.stem_pairs() == () and stems.entry_stems("f000101") == ()
    assert not stems.lookup("דלל X").found, "a lookup never raises"
    assert not stems.lookup("אסתדלו", verb_forms=frozenset("ABCDE")).found, "nor one by verb form"


# ---------------------------------------------------------------------------
# Matches found by verb form (the plan's PR 3, sections 4.4 and 6.1): after the word's own matches,
# the entries whose listed stems have the word among their generated forms; only the kinds the switch
# names (FRIEDMAN_VERB_FORM_KINDS), none by default. Synthetic units, no dictionary text.
# ---------------------------------------------------------------------------

ALL_KINDS = frozenset("ABCDE")


@pytest.fixture(autouse=True)
def _verb_form_switches_off(monkeypatch):
    """On by default (A, B, D); set empty, off, so the other tests see Friedman's own lookup."""
    monkeypatch.setenv(fs.ENV_VERB_FORM_KINDS, "")
    monkeypatch.setenv(fs.ENV_VERB_FORM_STEM_KINDS, "")


def _verb(result):
    return [(m.unit.id, m.via, m.kind, m.matched, m.stem) for m in result.matches]


def test_no_match_is_found_by_verb_form_unless_its_kind_is_switched_on(stems, monkeypatch):
    """The graded sample (owner, 2026-11-11): A, B and D on by default, each opened on the stem; a
    switch set empty turns them off."""
    assert stems.lookup("אסתדלו").matches == ()
    monkeypatch.delenv(fs.ENV_VERB_FORM_KINDS)
    monkeypatch.delenv(fs.ENV_VERB_FORM_STEM_KINDS)
    assert fs.verb_form_kinds() == {"A", "B", "D"} and fs.verb_form_stem_kinds() == {"A", "B", "D"}
    assert _matches(stems.lookup("אסתדלו")) == [("f000105", fs.VIA_VERB_FORM, 10)], "on by default, on the stem"
    assert stems.lookup("אסתדלו", verb_forms=frozenset("BCDE")).matches == (), "kind A not switched on"
    assert _verb(stems.lookup("אסתדלו", verb_forms=frozenset("A"), verb_form_stems=frozenset())) == [
        ("f000101", fs.VIA_VERB_FORM, "A", "אסתדלו (דלל X)", 0)], "no stem kind: the entry, its stems as buttons"
    [opened] = stems.lookup("אסתדלו", verb_forms=frozenset("A"), verb_form_stems=frozenset("A")).matches
    assert (opened.unit.id, opened.stem, opened.parent.id, opened.stems) == ("f000105", 10, "f000101", (1, 2, 4, 10)), (
        "a stem kind opens the stem's paragraph, as 'דלל X' does")


def test_the_switches_are_read_on_every_lookup(stems, monkeypatch):
    monkeypatch.setenv(fs.ENV_VERB_FORM_KINDS, "d")
    assert fs.verb_form_kinds() == {"D"}
    assert _verb(stems.lookup("ואסתדלו")) == [("f000101", fs.VIA_VERB_FORM, "D", "אסתדלו (דלל X)", 0)]
    monkeypatch.setenv(fs.ENV_VERB_FORM_STEM_KINDS, "D")
    assert [(m.unit.id, m.stem) for m in stems.lookup("ואסתדלו").matches] == [("f000105", 10)]
    assert stems.lookup("ואסתדלו", verb_forms=frozenset()).matches == (), "an explicit kind set wins over the switch"


def test_the_entry_the_book_s_index_reached_gets_no_match_by_verb_form(indexed_stems):
    assert _matches(indexed_stems.lookup("אסתדלו", verb_forms=ALL_KINDS)) == [("f000105", "index", 10)]


def test_matches_found_by_verb_form_are_bounded_and_never_repeat_an_entry(stems, monkeypatch):
    both = stems.lookup("ידלל", verb_forms=ALL_KINDS, verb_form_stems=ALL_KINDS)
    assert _verb(both) == [("f000101", fs.VIA_VERB_FORM, "B", "ידלל (דלל I, II, IV)", 0),
                           ("f000107", fs.VIA_VERB_FORM, "B", "ידלל (ד̇לל IV)", 4)], (
        "several stems fit the first entry: the entry; one fits the second: its paragraph")
    assert stems.lookup("דלל", verb_forms=ALL_KINDS).matches == stems.lookup("דלל").matches, (
        "the entries the word reaches are not repeated")
    assert stems.lookup("דלל X", verb_forms=ALL_KINDS).matches == stems.lookup("דלל X").matches, "nor a typed stem"
    monkeypatch.setattr(fs, "MAX_VERB_FORM_CARDS", 1)
    one = stems.lookup("ידלל", verb_forms=ALL_KINDS)
    assert [m.unit.id for m in one.matches] == ["f000101"] and one.truncated


def test_the_quotes_under_an_entry_found_by_verb_form_give_way_to_its_match(tmp_path, monkeypatch):
    path = tmp_path / "friedman.db"
    units = {"recovery_version": 4, "units": [
        unit("f000201", "entry", "קרב", ["קרב"], ["קרב"], binyan="V", letter="ק",
             examples=[("תקרב אליה", "'drew near'", "(ח1, שורה 1)")]),
        unit("f000202", "subentry", "קרב מן", ["קרב מן"], parent="f000201",
             examples=[("ותקרב מנה", "'and drew near him'", "(ח1, שורה 2)")])]}
    write_sidecar(units, {"index_version": 1, "rows": []},
                  {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, path)
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    svc = FriedmanService(str(path))
    plain = svc.lookup("תקרב")
    assert plain.matches == () and sorted(h.unit.id for h in plain.quotes) == ["f000201", "f000202"]
    found = svc.lookup("תקרב", verb_forms=ALL_KINDS)
    assert _verb(found) == [("f000201", fs.VIA_VERB_FORM, "A", "תקרב (קרב V)", 0)] and found.quotes == (), (
        "neither the entry's quotations nor its sub-entry's")


def test_beside_the_word_s_own_matches_the_verb_forms_are_suggestions(tmp_path, monkeypatch):
    """Owner, 2026-11-11 (the graded sample): beside a unit the word itself reaches, the roots its verb
    forms give are chips at the top ("קרב V"), not matches; a root another dictionary gives becomes a
    borrowed match, and no chip."""
    path = tmp_path / "friedman.db"
    units = {"recovery_version": 4, "units": [
        unit("f000301", "entry", "קרב", ["קרב"], ["קרב"], binyan="V", letter="ק"),
        unit("f000302", "entry", "תקרב", ["תקרב"], ["תקרב"], letter="ת")]}
    write_sidecar(units, {"index_version": 1, "rows": []},
                  {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, path)
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    svc = FriedmanService(str(path))
    result = svc.lookup("תקרב", verb_forms=ALL_KINDS)
    assert _matches(result) == [("f000302", "head", 0)] and result.suggestions == (("קרב", 5),)
    borrowed = svc.borrow(result, (BorrowedRoot("קרב", BORROW_LANE, "قرب"),))
    assert [m.unit.id for m in borrowed.matches] == ["f000302", "f000301"] and borrowed.suggestions == ()


def test_a_match_found_by_verb_form_names_the_dictionaries_that_also_give_its_root(stems):
    found = stems.lookup("ידלל", verb_forms=ALL_KINDS)
    result = stems.borrow(found, (BorrowedRoot("ד̇לל", BORROW_BLAU, "ד̇לל"), BorrowedRoot("קרב", BORROW_LANE, "قرب")))
    assert [(m.unit.id, m.via, m.borrowed) for m in result.matches] == [
        ("f000101", fs.VIA_VERB_FORM, ()), ("f000106", fs.VIA_VERB_FORM, (("blau", "ד̇לל"),)),
        ("f000108", fs.VIA_BORROWED, (("lane", "قرب"),))], "it keeps its place and gets no second match"
