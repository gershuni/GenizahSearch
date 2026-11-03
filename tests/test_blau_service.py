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
    blau_enabled,
    entry_url,
    get_blau_service,
    reset_blau_service,
)
from tests.test_build_blau_sidecar import _form, _ja, _mutate, _seg, dictionary, write_source

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
    result = service.lookup("ואלכתאב")
    assert result.found is False
    assert result.candidates[0] == "ואלכתאב" and "כתאב" in result.candidates


def test_senses_are_bounded_and_the_rest_counted(tmp_path):
    data = dictionary()
    forms = data["בלג (بلغ)"]["stems"][0]["forms"]
    for i in range(6, 12):
        forms.append(_form(f"blau-f-{i:06d}", [_seg(f"'sense {i}'", "definition_english",
                                                   font="English-TNR", language="English")]))
    data["בלג (بلغ)"]["num_forms"] = len(forms)
    svc = BlauService(str(_build(tmp_path, data)))
    card = svc.lookup("בלג").cards[0]
    assert len(card.senses) == MAX_SENSES == 4
    assert card.more_senses == 7 - MAX_SENSES
    assert [s.form_id for s in card.senses] == ["blau-f-000005", "blau-f-000006", "blau-f-000007", "blau-f-000008"]


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


def test_thread_safe_lookups_agree_across_threads(built):
    svc = BlauService(str(built), thread_safe=True)
    assert svc.is_ready(), svc.unavailable_reason
    words = ["אכ̇ד̇", "מאכ̇וד̇", "חוה", "בלג"] * 5
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
