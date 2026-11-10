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
