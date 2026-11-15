# -*- coding: utf-8 -*-
"""shared/blau_service.py on the synthetic sidecar that scripts/build_blau_sidecar.py builds from
tests/test_build_blau_sidecar.py's fixture (no dictionary text in the repository).

What is pinned: the reader opens only a file that passes every check (schema version, normalizer
version, credit line, lookup index, row count, quick_check) and says why otherwise; a lookup
answers one card per entry in candidate order with the matched sense first, lists example-only
hits apart, is bounded, and never raises; the kill switch; thread safety; the singleton.
"""
from __future__ import annotations

import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from scripts.build_blau_sidecar import EXIT_OK, SCHEMA_VERSION as BUILD_SCHEMA_VERSION
from scripts.build_blau_sidecar import main as build_main
from shared import blau_service
from shared.blau_normalize import NORMALIZER_VERSION
from shared.blau_service import (
    MAX_SENSES,
    SCHEMA_VERSION,
    BlauService,
    arabic_candidates,
    arabic_key,
    arabic_keys,
    blau_enabled,
    dot_pattern,
    entry_url,
    get_blau_service,
    reset_blau_service,
)
from shared.ja_roots import BORROW_FRIEDMAN, BORROW_LANE, BorrowedRoot
from tests.test_build_blau_sidecar import _form, _ja, _mutate, _root, _seg, dictionary, write_source

ROOT_1, ROOT_2, ROOT_3 = "blau-e-000001", "blau-e-000002", "blau-e-000003"


def _build(tmp_path: Path, data=None) -> Path:
    src = write_source(tmp_path / "download", data=data)
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
    yield svc
    svc.close()


@pytest.fixture(autouse=True)
def _flag_on(monkeypatch):
    monkeypatch.delenv("BLAU_DICTIONARY_ENABLED", raising=False)
    # The cards found by verb form are on by default (A, B, D); set empty, they are off, so the tests
    # of everything else see Blau's own lookup. The verb-form tests ask for kinds or unset these.
    monkeypatch.setenv(blau_service.ENV_VERB_FORM_KINDS, "")
    monkeypatch.setenv(blau_service.ENV_VERB_FORM_STEM_KINDS, "")


# ---------------------------------------------------------------------------
# Opening
# ---------------------------------------------------------------------------

def test_the_reader_and_the_builder_agree_on_the_schema_version():
    assert SCHEMA_VERSION == BUILD_SCHEMA_VERSION


def test_a_built_sidecar_is_ready_with_its_credit_lines(service):
    assert service.is_ready() and service.is_available()
    assert service.unavailable_reason == ""
    assert service.credit_he.startswith("יהושע בלאו")
    assert service.credit_en.startswith("Joshua Blau")
    assert service.meta("version") == SCHEMA_VERSION
    assert service.meta("normalizer_version") == str(NORMALIZER_VERSION)


@pytest.mark.parametrize("sql, reason", [
    ("UPDATE meta SET value = '9.9.9' WHERE key = 'version'", "schema version '9.9.9'"),
    ("UPDATE meta SET value = '999' WHERE key = 'normalizer_version'", "keyed by normalizer '999'"),
    ("DELETE FROM meta WHERE key = 'credit_he'", "no credit line"),
    ("UPDATE meta SET value = '' WHERE key = 'credit_he'", "no credit line"),
    ("DROP INDEX idx_forms_norm_norm", "lookup index idx_forms_norm_norm missing"),
    ("UPDATE meta SET value = '1' WHERE key = 'rows_forms_norm'", "meta.rows_forms_norm = '1'"),
    ("DROP TABLE stems", "missing table(s): stems"),
])
def test_a_sidecar_that_fails_a_check_is_not_served(built, sql, reason):
    _mutate(built, sql)
    svc = BlauService(str(built))
    assert not svc.is_ready() and not svc.is_available()
    assert reason in svc.unavailable_reason
    assert svc.lookup("אכ̇ד̇").cards == ()
    assert svc.credit_he == ""


def test_a_missing_empty_or_foreign_file_is_not_served(tmp_path):
    missing = BlauService(str(tmp_path / "nowhere.db"))
    assert not missing.is_ready() and "no sidecar at" in missing.unavailable_reason

    empty = tmp_path / "empty.db"
    empty.write_bytes(b"")
    svc = BlauService(str(empty))
    assert not svc.is_ready(), svc.unavailable_reason

    text = tmp_path / "text.db"
    text.write_text("not a database", encoding="utf-8")
    svc = BlauService(str(text))
    assert not svc.is_ready() and svc.unavailable_reason
    assert svc.lookup("אכ̇ד̇").found is False


def test_the_kill_switch_hides_a_ready_sidecar(service, monkeypatch):
    monkeypatch.setenv("BLAU_DICTIONARY_ENABLED", "0")
    assert not blau_enabled()
    assert service.is_ready() and not service.is_available()
    assert service.lookup("אכ̇ד̇").cards == ()
    monkeypatch.setenv("BLAU_DICTIONARY_ENABLED", "yes")
    assert service.is_available() and service.lookup("אכ̇ד̇").cards


def test_an_editors_switch_serves_editors_only_and_the_lookup_answers(service, monkeypatch):
    """BLAU_DICTIONARY_ENABLED=editors (shared/dictionary_switch.py): hidden from a surface that does
    not know the reader (``is_available``), shown to an editor (``served_to``); the lookup itself
    answers, since the page decided who may ask. Off: no one, and the lookup is empty."""
    monkeypatch.setenv("BLAU_DICTIONARY_ENABLED", " Editors ")
    assert not blau_enabled() and not service.is_available()
    assert service.served_to(True) and not service.served_to(False)
    assert service.lookup("אכ̇ד̇").cards
    monkeypatch.setenv("BLAU_DICTIONARY_ENABLED", "0")
    assert not service.served_to(True) and service.lookup("אכ̇ד̇").cards == ()
    monkeypatch.delenv("BLAU_DICTIONARY_ENABLED")
    assert service.is_available() and service.served_to(False) and service.served_to(True)


def test_close_makes_the_service_unavailable(built):
    svc = BlauService(str(built))
    svc.close()
    assert not svc.is_ready() and svc.unavailable_reason == "closed"
    assert svc.lookup("אכ̇ד̇").found is False


def test_entry_url_is_the_site_page_of_the_root():
    assert entry_url("blau-e-002260") == "https://blau-dict.netlify.app/entry/blau-e-002260"
    assert entry_url("a b/c") == "https://blau-dict.netlify.app/entry/a%20b%2Fc"


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def test_a_root_hit_is_one_card_with_the_entry_s_senses_in_dictionary_order(service):
    result = service.lookup("אכ̇ד̇")
    assert result.key == "אכד" and result.candidates == ("אכד",)
    assert result.found and len(result.cards) == 1 and result.more_cards == 0
    card = result.cards[0]
    assert card.root_id == ROOT_1 and card.name == "אכ̇ד̇" and card.arabic == "أَخَذ"
    assert card.page == 7 and card.url == entry_url(ROOT_1)
    assert card.matched_kind == "root" and card.matched_key == "אכ̇ד̇" and card.candidate == "אכד"
    assert card.sub_entry == "" and card.via == ""
    assert [s.form_id for s in card.senses] == ["blau-f-000001", "blau-f-000002", "blau-f-000003"]
    assert not any(s.matched for s in card.senses)
    first = card.senses[0]
    assert first.gloss_he == "'לקח'" and first.gloss_en == "'to take'" and first.binyan == "I"
    assert card.more_senses == 0
    assert result.examples == () and result.more_examples == 0


def test_a_variant_hit_shows_the_matched_form_first_with_its_flags(service):
    result = service.lookup("מאכ̇וד̇")
    assert len(result.cards) == 1
    card = result.cards[0]
    assert card.matched_kind == "variant" and card.matched_key == "מאכ̇וד̇"
    matched, *rest = card.senses
    assert matched.matched and matched.form_id == "blau-f-000002"
    assert matched.ja_text == "מאכ̇וד̇" and matched.arabic == "مأخوذ"
    assert matched.gloss_en == "'taken'" and matched.is_classical and matched.binyan == "IV"
    assert not matched.is_reconstructed and not matched.is_erroneous
    assert [s.form_id for s in rest] == ["blau-f-000001", "blau-f-000003"]


def test_a_sub_entry_hit_scopes_the_senses_to_that_sub_entry(service):
    result = service.lookup("מאכ̇ד̇")
    assert len(result.cards) == 1
    card = result.cards[0]
    assert card.root_id == ROOT_1 and card.matched_kind == "sub_entry" and card.sub_entry == "מאכ̇ד̇"
    assert [s.form_id for s in card.senses] == ["blau-f-000003"]
    assert card.senses[0].gloss_he == "'מקור'" and card.more_senses == 0


def test_a_proclitic_is_peeled_and_the_headword_under_a_sub_entry_found(service):
    result = service.lookup("ומאכ̇ד̇ים")
    assert result.candidates[:2] == ("ומאכדימ", "מאכדימ")
    assert len(result.cards) == 1
    card = result.cards[0]
    assert card.candidate == "מאכדימ" and card.matched_kind == "headword"
    assert card.matched_key == "מאכ̇ד̇ים" and card.sub_entry == "מאכ̇ד̇"
    assert [(s.form_id, s.matched) for s in card.senses] == [("blau-f-000003", True)]


def test_two_candidates_that_reach_the_same_entry_are_two_cards_in_candidate_order(tmp_path):
    # Codex on #473: a card is one scope reached by one candidate, never a merge.
    data = dictionary()
    data["בלג (بلغ)"]["stems"][0]["forms"][0]["segments"].append(_ja("ובלג", "variant_ja"))
    svc = BlauService(str(_build(tmp_path, data)))
    result = svc.lookup("ובלג")
    assert result.candidates[:2] == ("ובלג", "בלג")  # then לג, with ב read as a preposition: no hit
    assert [(c.root_id, c.candidate, c.matched_kind, c.matched_key) for c in result.cards] == [
        (ROOT_3, "ובלג", "variant", "ובלג"), (ROOT_3, "בלג", "root", "בלג")]
    first, second = result.cards
    assert first.senses[0].matched and first.senses[0].ja_text == "ובלג"
    assert not any(s.matched for s in second.senses)
    assert result.examples == ()


def test_a_root_hit_and_a_sub_entry_hit_of_one_candidate_are_two_scopes(tmp_path):
    data = dictionary()
    data["בלג (بلغ)"]["name_ja"] = ["מאכ̇ד̇"]  # a root named like root 1's sub-entry
    data["בלג (بلغ)"]["name_ja_clean"] = ["מאכ̇ד̇"]
    svc = BlauService(str(_build(tmp_path, data)))
    cards = svc.lookup("מאכ̇ד̇").cards
    assert [(c.root_id, c.matched_kind, c.sub_entry) for c in cards] == [
        (ROOT_3, "root", ""), (ROOT_1, "sub_entry", "מאכ̇ד̇")]
    assert [s.form_id for s in cards[1].senses] == ["blau-f-000003"]


def test_a_cross_reference_root_leads_to_its_target(service):
    result = service.lookup("חוה")
    assert [c.root_id for c in result.cards] == [ROOT_1]
    card = result.cards[0]
    assert card.via == "חוה" and card.matched_key == "חוה" and card.name == "אכ̇ד̇"


def test_a_form_that_is_a_cross_reference_names_its_target(service):
    # Root 2's only form is "see أخذ" with the target root in forms.target_ids.
    svc = service
    row = svc._conn.execute("SELECT id FROM forms WHERE root_id = ?", (ROOT_2,)).fetchone()
    sense = svc._sense(svc._conn.execute(
        f"SELECT {blau_service._FORM_COLUMNS} FROM forms WHERE id = ?", (row[0],)).fetchone(), {}, False)
    assert sense.cross_reference == "أخذ"
    assert sense.see == (("אכ̇ד̇", entry_url(ROOT_1)),)


def test_a_phrase_hit_is_listed_as_an_example_not_a_card(service):
    result = service.lookup("ואכ̇ד̇ת אלכתאב להם")
    assert result.key == "ואכדת אלכתאב להמ" and result.candidates == (result.key,)
    assert result.cards == ()
    assert [(e.root_id, e.name, e.phrase, e.url) for e in result.examples] == [
        (ROOT_1, "אכ̇ד̇", "ואכ̇ד̇ת אלכתאב להם", entry_url(ROOT_1))]
    assert result.found


def test_an_example_under_an_entry_already_carded_is_not_repeated(tmp_path):
    data = dictionary()
    # A one-word example phrase under root 3 that is also root 1's name.
    data["בלג (بلغ)"]["stems"][0]["forms"][0]["segments"].append(_ja("אכ̇ד̇"))
    svc = BlauService(str(_build(tmp_path, data)))
    result = svc.lookup("אכ̇ד̇")
    assert [c.root_id for c in result.cards] == [ROOT_1]
    assert [e.root_id for e in result.examples] == [ROOT_3]


@pytest.mark.parametrize("word", ["", "   ", "shalom", "123", "،", "ذ"])
def test_a_word_without_hebrew_letters_answers_nothing(service, word):
    result = service.lookup(word)
    assert result.found is False and result.cards == () and result.examples == ()
    assert result.candidates == ()


def test_a_word_the_dictionary_lacks_answers_its_candidates(service):
    result = service.lookup("ואלקלם")
    assert result.found is False and result.quotes == () and result.more_quotes == 0
    assert result.candidates[0] == "ואלקלמ" and "קלמ" in result.candidates


def _quote_places(result):
    return [(h.root_id, h.sub_entry, h.places_total,
             [(p.scope, " ".join(p.words), p.positions, [(c.printed, c.via) for c in p.components]) for p in h.places])
            for h in result.quotes]


def test_a_word_inside_a_citation_is_found_with_its_place_marked(service):
    """The word is no entry's name; root 1 quotes it. The exact printed word is via "word", a
    clitic alternative via "stem"; the citation carries its text and the entry its link."""
    exact = service.lookup("אלכתאב")
    assert exact.cards == () and exact.examples == () and exact.found and exact.more_quotes == 0
    assert _quote_places(exact) == [
        (ROOT_1, "", 1, [("citation", "ואכ̇ד̇ת אלכתאב להם", (1,), [("אלכתאב", "word")])])]
    [place] = exact.quotes[0].places
    assert place.quotation is not None and place.quotation.quote == "ואכ̇ד̇ת אלכתאב להם"
    assert exact.quotes[0].url == entry_url(ROOT_1)
    peeled = service.lookup("ואלכתאב")
    assert _quote_places(peeled) == [
        (ROOT_1, "", 1, [("citation", "ואכ̇ד̇ת אלכתאב להם", (1,), [("אלכתאב", "stem")])])]


def test_quotes_leave_out_the_cards_entries_function_words_and_several_words(service):
    carded = service.lookup("אכ̇ד̇")
    assert [c.root_id for c in carded.cards] == [ROOT_1] and carded.quotes == ()
    direct, _more = service._quotes("אכ̇ד̇", set(), {})
    assert ROOT_1 in [h.root_id for h in direct], "the entry's own names hold the word: only the card keeps it out"
    assert service.lookup("להם").quotes == (), "a preposition with its pronoun is in every other citation"
    assert service._conn.execute("SELECT COUNT(*) FROM tokens WHERE key = 'להמ'").fetchone()[0] == 1
    phrase = service.lookup("ואכ̇ד̇ת אלכתאב להם")
    assert phrase.quotes == () and [e.root_id for e in phrase.examples] == [ROOT_1], (
        "several words are never looked up one by one")


def test_quotes_are_bounded_and_a_common_word_has_none(service, monkeypatch):
    monkeypatch.setattr(blau_service, "MAX_QUOTE_UNITS", 0)
    assert service.lookup("אלכתאב").quotes == () and not service.lookup("אלכתאב").found
    monkeypatch.setattr(blau_service, "MAX_QUOTE_UNITS", 25)
    monkeypatch.setattr(blau_service, "MAX_QUOTE_HITS", 0)
    result = service.lookup("אלכתאב")
    assert result.quotes == () and result.more_quotes == 1


def test_a_card_carries_every_sense_and_only_a_bound_far_above_the_largest_entry_cuts(tmp_path, monkeypatch):
    """Owner, 2026-11-11: no sense only on the site. The card carries every sense of its scope (the
    panel shows the first few and the rest on a click); MAX_SENSES is a bound above the largest entry
    of release 1.1 (115 forms), and what lies beyond it is counted."""
    data = dictionary()
    forms = data["בלג (بلغ)"]["stems"][0]["forms"]
    for i in range(6, 12):
        forms.append(_form(f"blau-f-{i:06d}", [_seg(f"'sense {i}'", "definition_english",
                                                   font="English-TNR", language="English")]))
    data["בלג (بلغ)"]["num_forms"] = len(forms)
    svc = BlauService(str(_build(tmp_path, data)))
    every = [f"blau-f-{i:06d}" for i in (5, 6, 7, 8, 9, 10, 11)]
    card = svc.lookup("בלג").cards[0]
    assert [s.form_id for s in card.senses] == every and card.more_senses == 0
    assert MAX_SENSES >= 115
    monkeypatch.setattr(blau_service, "MAX_SENSES", 4)
    card = svc.lookup("בלג").cards[0]
    assert [s.form_id for s in card.senses] == every[:4] and card.more_senses == 3


def test_cards_and_examples_are_bounded_and_the_rest_counted(tmp_path, monkeypatch):
    data = dictionary()
    data["בלג (بلغ)"]["name_ja"] = ["אכ̇ד̇"]  # a second entry under the same name
    data["בלג (بلغ)"]["name_ja_clean"] = ["אכ̇ד̇"]
    svc = BlauService(str(_build(tmp_path, data)))
    assert [c.root_id for c in svc.lookup("אכ̇ד̇").cards] == [ROOT_1, ROOT_3]
    monkeypatch.setattr(blau_service, "MAX_CARDS", 1)
    result = svc.lookup("אכ̇ד̇")
    assert [c.root_id for c in result.cards] == [ROOT_1] and result.more_cards == 1
    monkeypatch.setattr(blau_service, "MAX_EXAMPLES", 0)
    result = svc.lookup("ואכ̇ד̇ת אלכתאב להם")
    assert result.examples == () and result.more_examples == 1


def test_a_lookup_never_raises(service, monkeypatch):
    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(service, "_lookup", boom)
    result = service.lookup("אכ̇ד̇")
    assert result.found is False and result.word == "אכ̇ד̇" and result.key == "אכד"


# ---------------------------------------------------------------------------
# Two-word entry names and the dot above (owner, 2026-11-04: דלך reached only
# دلك "to polish", never ذلك, whose entry is named "ד̇לך ו")
# ---------------------------------------------------------------------------

ROOT_POLISH, ROOT_THAT = "blau-e-000004", "blau-e-000005"


def _with_dalik_and_dhalik():
    data = dictionary()
    data["דלך (دلك)"] = _root("דלך (دلك)", ROOT_POLISH, ["דלך"], ["دلك"], stems=[{
        "id": "blau-st-000005", "stem_head": {"binyan": "I"},
        "forms": [_form("blau-f-000006", [_seg("'to polish'", "definition_english", font="English-TNR",
                                                language="English")])]}])
    data["ד̇לך ו (ذٰلِكَ و)"] = _root("ד̇לך ו (ذٰلِكَ و)", ROOT_THAT, ["ד̇לך ו"], ["ذٰلِكَ و"], stems=[{
        "id": "blau-st-000006", "stem_head": {},
        "forms": [_form("blau-f-000007", [_seg("'and that'", "definition_english", font="English-TNR",
                                                language="English")])]}])
    return data


@pytest.fixture
def dalik(tmp_path):
    svc = BlauService(str(_build(tmp_path, _with_dalik_and_dhalik())))
    assert svc.is_ready(), svc.unavailable_reason
    yield svc
    svc.close()


def test_dot_pattern_marks_the_dot_however_it_is_written():
    assert dot_pattern("ד'לך") == dot_pattern("ד׳לך") == dot_pattern("ד̇לך") == "ד*לכ"
    assert dot_pattern("דלך") == "דלכ"
    assert dot_pattern("ואלד'לך") == "ואלד*לכ"
    assert dot_pattern("ד̇לך ו") == "ד*לכ ו"
    assert dot_pattern("ר'") == "ר", "an abbreviation mark on a letter Judeo-Arabic never dots"
    assert dot_pattern("צ̇ץ̇") == "צ*צ*"


def test_a_word_reaches_an_entry_whose_name_begins_with_it(dalik):
    result = dalik.lookup("דלך")
    assert [c.root_id for c in result.cards] == [ROOT_POLISH, ROOT_THAT]
    assert result.cards[1].matched_key == "ד̇לך ו" and result.cards[1].matched_kind == "root"
    assert not any(c.dot_mismatch for c in result.cards), "an undotted word marks nothing"


def test_a_word_written_with_the_dot_puts_the_dotted_reading_first(dalik):
    for word in ("ד'לך", "ד׳לך", "ד̇לך", "וד'לך"):
        result = dalik.lookup(word)
        assert [c.root_id for c in result.cards][:2] == [ROOT_THAT, ROOT_POLISH], word
        assert [c.dot_mismatch for c in result.cards][:2] == [False, True], word


def test_a_multi_word_variant_is_not_reached_by_its_first_word(tmp_path):
    data = _with_dalik_and_dhalik()
    data["בלג (بلغ)"]["stems"][0]["forms"][0]["segments"].append(_ja("דלך מן", "variant_ja"))
    svc = BlauService(str(_build(tmp_path, data)))
    assert ROOT_3 not in [c.root_id for c in svc.lookup("דלך").cards]


# ---------------------------------------------------------------------------
# Typed Arabic
# ---------------------------------------------------------------------------

def test_arabic_keys_undo_the_extraction_s_spacing_and_fold_what_typists_vary():
    assert arabic_key("أخ ذ") == "اخذ"
    assert arabic_key("ج أَبايا") == "ابايا", "the plural mark is not part of the word"
    assert arabic_key("مدرسة") == arabic_key("مدرسه")
    assert arabic_key("على") == arabic_key("علي")
    assert arabic_keys("إِجا ص | إِنْجا ص") == ["اجاص", "انجاص"]
    assert arabic_keys("ـ") == []
    assert arabic_candidates("والكتاب") == ["والكتاب", "الكتاب", "كتاب"]
    assert arabic_candidates("ذ") == []


def test_a_typed_arabic_word_finds_the_entry_by_its_arabic_spelling(service):
    result = service.lookup("أخذ")
    assert result.script == "arabic" and result.key == "اخذ"
    assert [c.root_id for c in result.cards] == [ROOT_1]
    assert result.cards[0].matched_key == ""


def test_a_typed_arabic_form_marks_that_form(service):
    [card] = service.lookup("مأخوذ").cards
    assert card.root_id == ROOT_1 and card.senses[0].form_id == "blau-f-000002" and card.senses[0].matched


def test_a_typed_arabic_word_peels_the_conjunction_and_the_article(service):
    result = service.lookup("والمأخذ")
    assert result.candidates == ("والماخذ", "الماخذ", "ماخذ")
    [card] = result.cards
    assert card.root_id == ROOT_1 and card.sub_entry == "מאכ̇ד̇" and card.candidate == "ماخذ"


def test_a_typed_arabic_word_the_dictionary_lacks_says_so(service):
    result = service.lookup("قاضي")
    assert result.script == "arabic" and result.found is False and result.candidates == ("قاضي",)


def test_hebrew_letters_win_over_arabic_in_one_word(service):
    assert service.lookup("אכ̇ד̇ أخذ").script == "ja"


def test_thread_safe_lookups_agree_across_threads(built):
    svc = BlauService(str(built), thread_safe=True)
    assert svc.is_ready(), svc.unavailable_reason
    words = ["אכ̇ד̇", "מאכ̇וד̇", "חוה", "בלג", "أخذ"] * 5
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(svc.lookup, words))
    expected = {w: svc.lookup(w) for w in set(words)}
    assert all(r == expected[w] for w, r in zip(words, results))
    svc.close()


# ---------------------------------------------------------------------------
# The default instance
# ---------------------------------------------------------------------------

def test_the_default_service_opens_the_sidecar_where_it_lives(built, monkeypatch):
    reset_blau_service()
    monkeypatch.setattr(blau_service, "default_sidecar_path", lambda: str(built))
    try:
        first = get_blau_service()
        assert first.is_ready() and first.db_path == str(built)
        assert get_blau_service() is first
        reset_blau_service()
        assert not first.is_ready()
        assert get_blau_service() is not first
    finally:
        reset_blau_service()


def test_the_default_path_is_the_repository_sidecar_unless_the_user_has_a_copy(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    repo = Path(blau_service.__file__).resolve().parents[1] / "blau_data" / "blau.db"
    assert blau_service.default_sidecar_path() == str(repo)
    user = tmp_path / "GenizahSearchPro" / "data" / "blau_data" / "blau.db"
    user.parent.mkdir(parents=True)
    user.write_bytes(b"")
    assert blau_service.default_sidecar_path() == str(user)
    monkeypatch.delenv("LOCALAPPDATA")
    assert blau_service.default_sidecar_path() == str(repo)
    assert os.path.isabs(blau_service.default_sidecar_path())


# ---------------------------------------------------------------------------
# A root's stems (owner, 2026-11-11; the private dictionary lookup plan, section 4.6): the stems
# each entry lists, read from a stem row's column, shift and printed head together; a root typed
# with a stem ("דלל X") opens that stem's paragraph. Synthetic roots, no dictionary text.
# ---------------------------------------------------------------------------

ROOT_DLL, ROOT_DHLL, ROOT_ONE, ROOT_NONE = "blau-e-000110", "blau-e-000111", "blau-e-000112", "blau-e-000113"


def _sense(fid, gloss):
    return _form(fid, [_seg(f"'{gloss}'", "definition_english", font="English-TNR", language="English")])


def _head(binyan=None, printed=None, shift=None):
    """A stem's head: the ``binyan`` column, and the printed head's binyan (or shift) segment."""
    head = {}
    if binyan:
        head["binyan"] = binyan
    if shift:
        head["binyan_shift"] = shift
    segments = []
    if printed:
        segments.append(_seg(printed, "binyan", font="English-TNR", language="English"))
    if shift:
        segments.append(_seg(shift, "binyan_shift", font="English-TNR", language="English"))
    if segments:
        head["segments"] = segments
    return head


def _with_stems():
    data = dictionary()
    data["דלל (دلل)"] = _root("דלל (دلل)", ROOT_DLL, ["דלל"], ["دلل"], stems=[
        {"id": "blau-st-000110", "stem_head": _head("I", "I"),
         "forms": [_sense("blau-f-000110", "to prove"), _sense("blau-f-000111", "to name")]},
        {"id": "blau-st-000111", "stem_head": _head("II", "II"), "forms": [_sense("blau-f-000112", "to pamper")]},
        {"id": "blau-st-000112", "stem_head": _head("IV", "IV"), "forms": [_sense("blau-f-000113", "to rely on")]},
        # The column says IV where the printed head says V: both numbers (plan section 4.1).
        {"id": "blau-st-000113", "stem_head": _head("IV", "V"), "forms": [_sense("blau-f-000114", "to be pampered")]},
        {"id": "blau-st-000114", "stem_head": _head("X", "X"), "forms": [_sense("blau-f-000115", "to deduce")]},
    ], sub_entries=[{
        "id": "blau-se-000110", "name_ja": ["דאלה̈"], "arabic_spelling": ["دالة"],
        "stems": [{"id": "blau-st-000115", "stem_head": _head("VII"),
                   "forms": [_sense("blau-f-000116", "familiarity")]}],
    }])
    data["ד̇לל (ذلل)"] = _root("ד̇לל (ذلل)", ROOT_DHLL, ["ד̇לל"], ["ذلل"], stems=[
        {"id": "blau-st-000116", "stem_head": _head(shift="I > IV"), "forms": [_sense("blau-f-000117", "to humiliate")]},
    ])
    data["קרב (قرب)"] = _root("קרב (قرب)", ROOT_ONE, ["קרב"], ["قرب"], stems=[
        {"id": "blau-st-000117", "stem_head": _head("V"), "forms": [_sense("blau-f-000118", "to draw near")]},
    ])
    data["כתב (كتب)"] = _root("כתב (كتب)", ROOT_NONE, ["כתב"], ["كتب"], stems=[
        {"id": "blau-st-000118", "stem_head": {}, "forms": [_sense("blau-f-000119", "to write")]},
    ])
    return data


@pytest.fixture
def stems(tmp_path):
    svc = BlauService(str(_build(tmp_path, _with_stems())))
    assert svc.is_ready(), svc.unavailable_reason
    yield svc
    svc.close()


def _cards(result):
    return [(c.root_id, c.stem, [s.form_id for s in c.senses]) for c in result.cards]


def test_the_stem_pairs_read_the_column_the_shift_and_the_printed_head_together(stems):
    mine = [(p.entry_id, p.stem, p.unit_id, p.root, p.root_key) for p in stems.stem_pairs()
            if p.entry_id in (ROOT_DLL, ROOT_DHLL, ROOT_ONE, ROOT_NONE)]
    assert mine == [
        (ROOT_DLL, 1, "blau-st-000110", "דלל", "דלל"), (ROOT_DLL, 2, "blau-st-000111", "דלל", "דלל"),
        (ROOT_DLL, 4, "blau-st-000112", "דלל", "דלל"),
        (ROOT_DLL, 4, "blau-st-000113", "דלל", "דלל"), (ROOT_DLL, 5, "blau-st-000113", "דלל", "דלל"),
        (ROOT_DLL, 10, "blau-st-000114", "דלל", "דלל"), (ROOT_DLL, 7, "blau-st-000115", "דלל", "דלל"),
        (ROOT_DHLL, 1, "blau-st-000116", "ד̇לל", "דלל"), (ROOT_DHLL, 4, "blau-st-000116", "ד̇לל", "דלל"),
        (ROOT_ONE, 5, "blau-st-000117", "קרב", "קרב"),
    ], "a row with no number gives no pair; a sub-entry's stem row is its root's"
    assert stems.root_stems(ROOT_DLL) == (1, 2, 4, 5, 7, 10)
    assert stems.root_stems(ROOT_DHLL) == (1, 4) and stems.root_stems(ROOT_ONE) == (5,)
    assert stems.root_stems(ROOT_NONE) == () and stems.root_stems("no-such-root") == ()
    assert blau_service.stem_row_numbers("IV", None, '{"segments": [{"text": "V", "text_type": "binyan"}]}') == (4, 5)
    assert blau_service.stem_row_numbers(None, None, "not json") == ()
    assert blau_service.stem_row_numbers(None, "I > IV", None) == (1, 4), "the shift column alone names both"


def test_a_plain_lookup_shows_each_entry_s_stems_and_chooses_none(stems):
    result = stems.lookup("דלל")
    assert [(c.root_id, c.stems, c.stem, c.root_label) for c in result.cards] == [
        (ROOT_DLL, (1, 2, 4, 5, 7, 10), 0, "דלל"), (ROOT_DHLL, (1, 4), 0, "ד̇לל")]
    assert result.stem == 0 and not result.stem_missing
    assert stems.lookup("כתב").cards[0].stems == (), "an entry with no stem number has no buttons"


@pytest.mark.parametrize("typed", ["דלל X", "דלל 10", "דלל x", "X דלל"])
def test_a_typed_stem_opens_that_stem_s_paragraph(stems, typed):
    result = stems.answer(typed)
    assert isinstance(result, blau_service.BlauLookup), "a root with a stem is never an expression search"
    assert _cards(result) == [(ROOT_DLL, 10, ["blau-f-000115"])]
    assert result.cards[0].more_senses == 0 and result.stem == 10 and not result.stem_missing
    assert result.examples == () and result.quotes == () and result.word == " ".join(typed.split())


@pytest.mark.parametrize("typed", ["דלל X", "דלל 10"])
def test_words_from_the_manuscript_are_never_a_typed_stem(stems, typed):
    """``stems=False`` (words clicked or selected in the manuscript): "פרק 2" there is two words, an
    expression search, never the root פרק at stem II."""
    result = stems.answer(typed, stems=False)
    assert isinstance(result, blau_service.BlauExpression) == stems.expressions_available()
    assert not (isinstance(result, blau_service.BlauLookup) and result.stem)
    plain = stems.lookup(typed, stems=False)
    assert plain.stem == 0 and not any(card.stem for card in plain.cards)


def test_a_stem_row_whose_column_and_head_disagree_answers_to_both(stems):
    assert _cards(stems.lookup("דלל V")) == [(ROOT_DLL, 5, ["blau-f-000114"])]
    assert _cards(stems.lookup("דלל IV")) == [(ROOT_DLL, 4, ["blau-f-000113", "blau-f-000114"]),
                                              (ROOT_DHLL, 4, ["blau-f-000117"])]


def test_a_stem_under_a_sub_entry_names_it_and_a_shift_counts_both_sides(stems):
    [card] = stems.lookup("דלל VII").cards
    assert (card.stem, [s.form_id for s in card.senses], card.sub_entry) == (7, ["blau-f-000116"], "דאלה̈")
    assert _cards(stems.lookup("דלל I")) == [(ROOT_DLL, 1, ["blau-f-000110", "blau-f-000111"]),
                                             (ROOT_DHLL, 1, ["blau-f-000117"])]


def test_a_dotted_root_puts_the_dotted_entry_first_and_marks_the_other(stems):
    result = stems.lookup("ד'לל IV")
    assert [(c.root_id, c.dot_mismatch) for c in result.cards] == [(ROOT_DHLL, False), (ROOT_DLL, True)]
    only_undotted = stems.lookup("ד'לל X")
    assert [(c.root_id, c.stem, c.dot_mismatch) for c in only_undotted.cards] == [(ROOT_DLL, 10, True)]


def test_a_stem_the_root_does_not_list_says_so_and_shows_the_root(stems):
    result = stems.lookup("דלל III")
    assert result.stem == 3 and result.stem_missing
    assert [(c.root_id, c.stem, c.stems) for c in result.cards] == [(ROOT_DLL, 0, (1, 2, 4, 5, 7, 10)),
                                                                     (ROOT_DHLL, 0, (1, 4))]
    assert [s.form_id for s in result.cards[0].senses] == [f"blau-f-{i:06d}" for i in range(110, 117)], \
        "the root's own senses, unscoped, every one"
    one = stems.lookup("קרב II")
    assert one.stem_missing and [(c.root_id, c.stems) for c in one.cards] == [(ROOT_ONE, (5,))]
    none = stems.lookup("כתב II")
    assert none.stem_missing and [(c.root_id, c.stems) for c in none.cards] == [(ROOT_NONE, ())]
    unknown = stems.lookup("קקק X")
    assert not unknown.found and not unknown.stem_missing and unknown.stem == 10


def test_an_entry_whose_first_name_is_the_word_comes_before_one_that_gives_it_as_a_later_name(tmp_path):
    data = dictionary()
    data["אגאץ | אנגאץ"] = _root("אגאץ | אנגאץ", "blau-e-000120", ["אגאץ", "אנגאץ"], ["إجاص"],
                                 stems=[{"id": "blau-st-000120", "stem_head": {}, "forms": [_sense("blau-f-000120", "pear")]}])
    data["אנגאץ"] = _root("אנגאץ", "blau-e-000121", ["אנגאץ"], ["إنجاص"],
                          stems=[{"id": "blau-st-000121", "stem_head": {}, "forms": [_sense("blau-f-000121", "plum")]}])
    svc = BlauService(str(_build(tmp_path, data)))
    assert [c.root_id for c in svc.lookup("אנגאץ").cards] == ["blau-e-000121", "blau-e-000120"], \
        "the entry named אנגאץ first, though the other is earlier and gives it as its second name (owner, 2026-11-11)"
    assert [c.root_id for c in svc.lookup("אגאץ").cards] == ["blau-e-000120"]


def test_the_stems_fail_closed(stems, monkeypatch):
    monkeypatch.setenv("BLAU_DICTIONARY_ENABLED", "0")
    assert stems.stem_pairs() == () and not stems.lookup("דלל X").found
    monkeypatch.delenv("BLAU_DICTIONARY_ENABLED")

    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(stems, "_stems", boom)
    assert stems.stem_pairs() == () and stems.root_stems(ROOT_DLL) == ()


# ---------------------------------------------------------------------------
# Roots borrowed from Friedman and Lane (owner, 2026-11-11): Blau has no list of inflected forms, so
# a verb form (קאל) reached no entry of its own; the entries of the roots the other dictionaries give
# come after Blau's own cards. Synthetic roots, no dictionary text.
# ---------------------------------------------------------------------------

def _borrowed(result):
    return [(c.root_id, c.borrowed) for c in result.cards]


def test_a_word_blau_lacks_reaches_the_entry_of_a_borrowed_root(stems):
    plain = stems.lookup("קארב")
    assert plain.cards == () and not plain.found
    result = stems.borrow(plain, (BorrowedRoot("קרב", BORROW_LANE, "قرب"),))
    assert _borrowed(result) == [(ROOT_ONE, ((BORROW_LANE, "قرب"),))]
    card = result.cards[0]
    assert card.stems == (5,) and [s.form_id for s in card.senses] == ["blau-f-000118"]
    assert result.found and result.word == "קארב" and plain.cards == ()


def test_borrowed_cards_come_after_blau_own_and_never_repeat_an_entry(stems):
    own = stems.lookup("כתב")
    assert _borrowed(own) == [(ROOT_NONE, ())]
    result = stems.borrow(own, (BorrowedRoot("כתב", BORROW_FRIEDMAN, "כתב"), BorrowedRoot("קרב", BORROW_LANE, "قرب")))
    assert _borrowed(result) == [(ROOT_NONE, ()), (ROOT_ONE, ((BORROW_LANE, "قرب"),))], (
        "Blau's own card first and unmarked; the root it already shows gets no second card")


def test_two_dictionaries_giving_one_entry_make_one_card_naming_both(stems):
    result = stems.borrow(stems.lookup("קארב"), (BorrowedRoot("קרב", BORROW_FRIEDMAN, "קארב (קרב)"),
                                                BorrowedRoot("קרב", BORROW_LANE, "قرب")))
    assert _borrowed(result) == [(ROOT_ONE, ((BORROW_FRIEDMAN, "קארב (קרב)"), (BORROW_LANE, "قرب")))]
    agreed = stems.borrow(stems.lookup("קארב"), (BorrowedRoot("כתב", BORROW_FRIEDMAN, "כתב"),
                                                BorrowedRoot("קרב", BORROW_FRIEDMAN, "קרב"),
                                                BorrowedRoot("קרב", BORROW_LANE, "قرب")))
    assert [c.root_id for c in agreed.cards] == [ROOT_ONE, ROOT_NONE], "the entry two dictionaries give first"


def test_a_borrowed_root_reaches_the_entry_spelled_with_its_dots(stems):
    nothing = stems.lookup("קארב")
    assert [c.root_id for c in stems.borrow(nothing, (BorrowedRoot("ד̇לל", BORROW_LANE, "ذلل"),)).cards] == [ROOT_DHLL]
    assert [c.root_id for c in stems.borrow(nothing, (BorrowedRoot("דלל", BORROW_LANE, "دلل"),)).cards] == [ROOT_DLL]
    # Dots no entry of those letters has (כ̇תב, خ ت ب; the entry is כתב): every entry of the letters.
    assert [c.root_id for c in stems.borrow(nothing, (BorrowedRoot("כ̇תב", BORROW_LANE, "ختب"),)).cards] == [ROOT_NONE]


def test_a_borrowed_cross_reference_leads_to_its_target(service):
    result = service.borrow(service.lookup("קארב"), (BorrowedRoot("חוה", BORROW_FRIEDMAN, "חוה"),))
    assert [(c.root_id, c.via, c.borrowed) for c in result.cards] == [(ROOT_1, "חוה", ((BORROW_FRIEDMAN, "חוה"),))]


def test_the_examples_and_quotes_of_a_borrowed_entry_give_way_to_its_card(service):
    root_1 = (BorrowedRoot("אכ̇ד̇", BORROW_LANE, "أخذ"),)
    phrase = service.lookup("ואכ̇ד̇ת אלכתאב להם")
    assert [e.root_id for e in phrase.examples] == [ROOT_1]
    borrowed = service.borrow(phrase, root_1)
    assert [c.root_id for c in borrowed.cards] == [ROOT_1] and borrowed.examples == ()
    quoted = service.lookup("אלכתאב")
    assert [h.root_id for h in quoted.quotes] == [ROOT_1]
    borrowed = service.borrow(quoted, root_1)
    assert [c.root_id for c in borrowed.cards] == [ROOT_1] and borrowed.quotes == ()
    other = service.borrow(quoted, (BorrowedRoot("בלג", BORROW_LANE, "بلغ"),))
    assert [h.root_id for h in other.quotes] == [ROOT_1], "the quotes under another entry stay"


def test_nothing_is_borrowed_for_a_typed_stem_an_expression_or_no_root(stems):
    root = (BorrowedRoot("קרב", BORROW_LANE, "قرب"),)
    chosen = stems.lookup("דלל X")
    assert chosen.stem == 10 and stems.borrow(chosen, root) is chosen
    expression = blau_service.empty_expression("קאל אללה")
    assert stems.borrow(expression, root) is expression
    nothing = stems.lookup("קארב")
    assert stems.borrow(nothing, ()) is nothing
    assert stems.borrow(nothing, (BorrowedRoot("זזז", BORROW_LANE, "ززز"),)) is nothing, "no entry of that root"
    assert stems.borrow(nothing, (BorrowedRoot("", BORROW_LANE, ""),)) is nothing


def test_borrowed_cards_stop_at_the_card_limit_and_the_rest_are_counted(stems, monkeypatch):
    monkeypatch.setattr(blau_service, "MAX_CARDS", 2)
    own = stems.lookup("כתב")
    result = stems.borrow(own, (BorrowedRoot("קרב", BORROW_LANE, "قرب"), BorrowedRoot("דלל", BORROW_FRIEDMAN, "דלל"),
                                BorrowedRoot("ד̇לל", BORROW_FRIEDMAN, "ד̇לל")))
    assert [c.root_id for c in result.cards] == [ROOT_NONE, ROOT_ONE]
    assert result.more_cards == own.more_cards + 2


def test_a_borrowed_card_opens_on_the_stem_the_word_s_shape_gives(stems):
    """Owner, 2026-11-11: אסתקר reached קרר and its card opened on stem I. The word's shape (stem X:
    אסת + the root) opens the borrowed card on that stem's paragraph, the stem marked; a word no
    listed stem holds, or several do, opens the whole entry."""
    root = (BorrowedRoot("דלל", BORROW_LANE, "دلل"),)
    x = stems.borrow(stems.lookup("אסתדלו"), root)
    assert _cards(x) == [(ROOT_DLL, 10, ["blau-f-000115"])] and x.cards[0].borrowed
    whole = stems.borrow(stems.lookup("קארב"), (BorrowedRoot("קרב", BORROW_LANE, "قرب"),))
    assert [(c.root_id, c.stem) for c in whole.cards] == [(ROOT_ONE, 0)], "קארב is no form of V, the one stem listed"
    own = stems.lookup("דלל")
    assert all(card.stem == 0 for card in stems.borrow(own, root).cards), "Blau's own cards are never re-opened"


def test_a_borrowed_card_the_word_fits_no_stem_of_shows_the_nouns_first(tmp_path):
    """אלחרוף (owner, 2026-11-11): Lane gives حرف; Blau's entry חרף opens with its stem II verb and
    then the noun sub-entry ("letter", pl. חרוף). The word fits no listed stem, so the senses outside
    the stem paragraphs come first; a word that fits the stem still opens on it."""
    data = dictionary()
    data["חרף (حرف)"] = _root("חרף (حرف)", "blau-e-000160", ["חרף"], ["حرف"], stems=[
        {"id": "blau-st-000160", "stem_head": _head("II", "II"), "forms": [_sense("blau-f-000160", "to cut")]},
    ], sub_entries=[{
        "id": "blau-se-000160", "name_ja": ["חרף"], "arabic_spelling": ["حرف"],
        "stems": [{"id": "blau-st-000161", "stem_head": {},
                   "forms": [_sense("blau-f-000161", "occurrence"), _sense("blau-f-000162", "letter")]}],
    }])
    svc = BlauService(str(_build(tmp_path, data)))
    root = (BorrowedRoot("חרף", BORROW_LANE, "حرف"),)
    noun = svc.borrow(svc.lookup("אלחרוף"), root)
    assert _cards(noun) == [("blau-e-000160", 0, ["blau-f-000161", "blau-f-000162", "blau-f-000160"])]
    verb = svc.borrow(svc.lookup("תחריף"), root)
    assert _cards(verb) == [("blau-e-000160", 2, ["blau-f-000160"])], "a form of II opens on II"
    own = svc.lookup("חרף")
    assert _cards(own)[0] == ("blau-e-000160", 0, ["blau-f-000160", "blau-f-000161", "blau-f-000162"]), (
        "Blau's own card keeps the dictionary's order")
    svc.close()


def test_blau_s_own_cards_put_the_entry_another_dictionary_names_first(tmp_path):
    """ויכון (owner, 2026-11-11): יכון is printed under כ̇לו (in a phrase) and under כון (the verb), one
    kind of hit each, so the book's order put כ̇לו first. Lane names כון: its card comes first."""
    data = dictionary()
    for rid, name, arabic, gloss in (("blau-e-000130", "כ̇לו", "خلو", "to make"), ("blau-e-000131", "כון", "كون", "to be")):
        data[f"{name} ({arabic})"] = _root(f"{name} ({arabic})", rid, [name], [arabic], stems=[{
            "id": rid.replace("-e-", "-st-"), "stem_head": {},
            "forms": [_form(rid.replace("-e-", "-f-"), [_seg(f"'{gloss}'", "definition_english", font="English-TNR",
                                                             language="English"), _ja("יכון", "headword_ja")])]}])
    svc = BlauService(str(_build(tmp_path, data)))
    own = svc.lookup("יכון")
    assert [c.root_id for c in own.cards] == ["blau-e-000130", "blau-e-000131"], "the book's order"
    named = svc.borrow(own, (BorrowedRoot("כון", BORROW_LANE, "كون"),))
    assert [(c.root_id, c.borrowed) for c in named.cards] == [("blau-e-000131", ()), ("blau-e-000130", ())]
    assert svc.borrow(own, (BorrowedRoot("קרב", BORROW_LANE, "قرب"),)).cards == own.cards, "named by none: as it was"


def test_the_entry_another_dictionary_names_comes_first_whichever_key_reached_it(tmp_path):
    """ואלקול (owner, 2026-11-11): with the conjunction peeled, אלקול is a variant printed under טלע;
    with the article peeled too, קול is the entry קול. The key order put טלע first; Friedman and Lane
    both name קול, and its card now comes first. Named by none, the key order stays."""
    data = dictionary()
    for rid, name, arabic, gloss, extra in (("blau-e-000150", "טלע", "طلع", "to rise", "אלקול"),
                                           ("blau-e-000151", "קול", "قول", "to say", None)):
        segments = [_seg(f"'{gloss}'", "definition_english", font="English-TNR", language="English")]
        if extra:
            segments.append(_ja(extra, "variant_ja"))
        data[f"{name} ({arabic})"] = _root(f"{name} ({arabic})", rid, [name], [arabic], stems=[{
            "id": rid.replace("-e-", "-st-"), "stem_head": {}, "forms": [_form(rid.replace("-e-", "-f-"), segments)]}])
    svc = BlauService(str(_build(tmp_path, data)))
    own = svc.lookup("ואלקול")
    assert [(c.root_id, c.candidate) for c in own.cards] == [("blau-e-000150", "אלקול"), ("blau-e-000151", "קול")]
    named = svc.borrow(own, (BorrowedRoot("קול", BORROW_FRIEDMAN, "קול"), BorrowedRoot("קול", BORROW_LANE, "قول")))
    assert [(c.root_id, c.borrowed) for c in named.cards] == [("blau-e-000151", ()), ("blau-e-000150", ())]
    assert svc.borrow(own, (BorrowedRoot("קרב", BORROW_LANE, "قرب"),)).cards == own.cards, "named by none: as it was"
    svc.close()


def test_the_verb_form_index_is_built_once_from_the_listed_pairs(stems, monkeypatch):
    """PR 2 (plan 4.3): every form key the listed pairs may be written as, and the pairs that give it;
    built once, and only when a kind of card found by verb form is switched on (PR 3)."""
    index = stems.verb_form_index()
    assert [(p.entry_id, p.stem) for p in index["אסתדלו"]] == [(ROOT_DLL, 10)]
    assert {(p.entry_id, p.stem) for p in index["ידלל"]} >= {(ROOT_DLL, 1), (ROOT_DHLL, 1)}
    assert stems.verb_form_index() is index, "built once"
    monkeypatch.setenv("BLAU_DICTIONARY_ENABLED", "0")
    assert stems.verb_form_index() is index, "read from the open file: the switch is the lookup's"
    fresh = BlauService(stems.db_path)

    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(fresh, "_stems", boom)
    assert fresh.verb_form_index() == {}, "fails closed"
    fresh.close()


def test_borrowing_fails_closed(stems, monkeypatch):
    nothing = stems.lookup("קארב")
    root = (BorrowedRoot("קרב", BORROW_LANE, "قرب"),)

    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(stems, "_borrow", boom)
    assert stems.borrow(nothing, root) is nothing
    monkeypatch.undo()
    monkeypatch.setenv("BLAU_DICTIONARY_ENABLED", "0")
    assert stems.borrow(nothing, root) is nothing


# ---------------------------------------------------------------------------
# Cards found by verb form (the plan's PR 3, sections 4.4 and 6.1): after the word's own cards, the
# entries whose listed stems have the word among their generated forms; only the kinds the switch
# names (BLAU_VERB_FORM_KINDS), none by default. Synthetic roots, no dictionary text.
# ---------------------------------------------------------------------------

ALL_KINDS = frozenset("ABCDE")


def _verb(result):
    return [(c.root_id, c.kind, c.verb_form, c.stem) for c in result.cards]


def test_no_card_is_found_by_verb_form_unless_its_kind_is_switched_on(stems, monkeypatch):
    """The graded sample (owner, 2026-11-11): A, B and D on by default, A and D opened on the stem;
    a switch set empty turns them off, and then the index is not even built."""
    called = []
    with monkeypatch.context() as m:
        m.setattr(stems, "verb_form_index", lambda: called.append(1) or {})
        assert stems.lookup("אסתדלו").cards == () and called == [], "set empty: off, nothing built"
    monkeypatch.delenv(blau_service.ENV_VERB_FORM_KINDS)
    monkeypatch.delenv(blau_service.ENV_VERB_FORM_STEM_KINDS)
    assert blau_service.verb_form_kinds() == {"A", "B", "D"} and blau_service.verb_form_stem_kinds() == {"A", "D"}
    assert _cards(stems.lookup("אסתדלו")) == [(ROOT_DLL, 10, ["blau-f-000115"])], "on by default, A opens on X"
    assert stems.lookup("אסתדלו", verb_forms=frozenset("BCDE")).cards == (), "kind A not switched on"
    result = stems.lookup("אסתדלו", verb_forms=frozenset("A"), verb_form_stems=frozenset())
    assert _verb(result) == [(ROOT_DLL, "A", "אסתדלו (דלל X)", 0)], "no stem kind: the whole entry, the stems as buttons"
    card = result.cards[0]
    assert card.matched_kind == blau_service.MATCHED_VERB_FORM and card.matched_key == "" and card.borrowed == ()
    assert card.stems == (1, 2, 4, 5, 7, 10) and len(card.senses) == 7 and result.found
    opened = stems.lookup("אסתדלו", verb_forms=frozenset("A"), verb_form_stems=frozenset("A"))
    assert _cards(opened) == [(ROOT_DLL, 10, ["blau-f-000115"])], "a stem kind opens the stem's paragraph"


def test_the_switches_are_read_on_every_lookup(stems, monkeypatch):
    monkeypatch.setenv(blau_service.ENV_VERB_FORM_KINDS, "a, d")
    assert blau_service.verb_form_kinds() == {"A", "D"}
    assert _verb(stems.lookup("ואסתדלו")) == [(ROOT_DLL, "D", "אסתדלו (דלל X)", 0)]
    monkeypatch.setenv(blau_service.ENV_VERB_FORM_STEM_KINDS, "D")
    assert _verb(stems.lookup("ואסתדלו")) == [(ROOT_DLL, "D", "אסתדלו (דלל X)", 10)]
    monkeypatch.setenv(blau_service.ENV_VERB_FORM_KINDS, "B")
    assert stems.lookup("ואסתדלו").cards == ()
    assert stems.lookup("ואסתדלו", verb_forms=frozenset()).cards == (), "an explicit kind set wins over the switch"


def test_beside_the_word_s_own_cards_the_verb_forms_are_suggestions(tmp_path):
    """Owner, 2026-11-11 (the graded sample): beside an entry the word itself reaches, a card found by
    verb form was mostly wrong; its root is a suggestion at the top instead ("קרב V", a press looks
    it up). A suggested root another dictionary also gives becomes a borrowed card, and no chip."""
    data = _with_stems()
    data["תקרב (تقرب)"] = _root("תקרב (تقرب)", "blau-e-000140", ["תקרב"], ["تقرب"], stems=[
        {"id": "blau-st-000140", "stem_head": {}, "forms": [_sense("blau-f-000140", "a near one")]}])
    svc = BlauService(str(_build(tmp_path, data)))
    result = svc.lookup("תקרב", verb_forms=ALL_KINDS)
    assert _verb(result) == [("blau-e-000140", "", "", 0)] and result.suggestions == (("קרב", 5),)
    assert svc.lookup("תקרב", verb_forms=frozenset("BCDE")).suggestions == (), "kind A not switched on"
    named = svc.borrow(result, (BorrowedRoot("קרב", BORROW_LANE, "قرب"),))
    assert [(c.root_id, c.borrowed) for c in named.cards] == [("blau-e-000140", ()), (ROOT_ONE, ((BORROW_LANE, "قرب"),))]
    assert named.suggestions == (), "the root is a card now"
    plain = svc.lookup("דלל")
    with_forms = svc.lookup("דלל", verb_forms=ALL_KINDS)
    assert with_forms.cards == plain.cards and with_forms.suggestions == (), "an entry the word reaches is not suggested"
    assert svc.lookup("אסתדלו", verb_forms=ALL_KINDS).suggestions == (), "nothing else found: cards, not chips"
    svc.close()


def test_cards_found_by_verb_form_are_bounded_and_the_rest_counted(stems, monkeypatch):
    both = stems.lookup("ידלל", verb_forms=ALL_KINDS, verb_form_stems=ALL_KINDS)
    assert _verb(both) == [(ROOT_DLL, "B", "ידלל (דלל I, II, IV)", 0), (ROOT_DHLL, "B", "ידלל (ד̇לל I, IV)", 0)], (
        "several stems fit: the whole entry")
    monkeypatch.setattr(blau_service, "MAX_VERB_FORM_CARDS", 1)
    one = stems.lookup("ידלל", verb_forms=ALL_KINDS)
    assert [c.root_id for c in one.cards] == [ROOT_DLL] and one.more_cards == 1


def test_no_card_by_verb_form_for_a_typed_stem_or_an_arabic_word(stems):
    assert stems.lookup("דלל X", verb_forms=ALL_KINDS).cards == stems.lookup("דלל X").cards
    assert stems.lookup("استدل", verb_forms=ALL_KINDS).cards == ()


def test_a_card_found_by_verb_form_names_the_dictionaries_that_also_give_its_root(stems):
    """It keeps its place, after the word's own cards and before the borrowed ones, and gets no second
    card; a root it does not show is borrowed as before."""
    found = stems.lookup("ידלל", verb_forms=ALL_KINDS)
    result = stems.borrow(found, (BorrowedRoot("ד̇לל", BORROW_LANE, "ذلل"), BorrowedRoot("קרב", BORROW_FRIEDMAN, "קרב")))
    assert [(c.root_id, c.kind, c.borrowed) for c in result.cards] == [
        (ROOT_DLL, "B", ()), (ROOT_DHLL, "B", ((BORROW_LANE, "ذلل"),)), (ROOT_ONE, "", ((BORROW_FRIEDMAN, "קרב"),))]
    assert stems.borrow(found, (BorrowedRoot("ד̇לל", BORROW_LANE, "ذلل"),)).cards[1].verb_form == "ידלל (ד̇לל I, IV)"
