# -*- coding: utf-8 -*-
"""shared/catalog_text_service.py over a sidecar built from synthetic inputs
(tests/catalog_text_fixture.py): fail-closed opening, ranking, the translations switch, snippets."""
from __future__ import annotations

import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from catalog_text_fixture import A, B, C, D, E, F, H, I, J, K, M, N, P, build_sidecar  # noqa: E402
from scripts.build_catalog_text_sidecar import SCHEMA_VERSION as BUILDER_SCHEMA_VERSION  # noqa: E402
from shared import catalog_text_service as svc  # noqa: E402
from shared.catalog_text_service import CatalogTextService, make_snippet  # noqa: E402
from shared.near_spellings import near_spellings  # noqa: E402
from shared.catalog_text_normalize import parse_query  # noqa: E402


@pytest.fixture(scope="module")
def sidecar(tmp_path_factory):
    return build_sidecar(tmp_path_factory.mktemp("catalog_text"))


@pytest.fixture(scope="module")
def service(sidecar):
    service = CatalogTextService(db_path=str(sidecar))
    assert service.is_ready(), service.unavailable_reason
    yield service
    service.close()


def _ids(result):
    return [h.sys_id for h in result.hits]


def test_reader_and_builder_agree_on_the_schema_version():
    assert svc.SCHEMA_VERSION == BUILDER_SCHEMA_VERSION


def _damaged(sidecar: Path, tmp_path: Path, sql: str) -> str:
    copy = tmp_path / "damaged.db"
    shutil.copyfile(sidecar, copy)
    con = sqlite3.connect(copy)
    con.execute(sql)
    con.commit()
    con.close()
    return str(copy)


@pytest.mark.parametrize("sql, reason", [
    ("UPDATE meta SET value = '0.9.0' WHERE key = 'version'", "schema version"),
    ("UPDATE meta SET value = '999' WHERE key = 'normalizer_version'", "normalizer"),
    ("DELETE FROM records WHERE id = 1", "rows_total"),
    ("DROP TABLE records", "missing table"),
])
def test_a_file_this_reader_cannot_trust_is_not_served(sidecar, tmp_path, sql, reason):
    service = CatalogTextService(db_path=_damaged(sidecar, tmp_path, sql))
    assert not service.is_ready() and not service.is_available()
    assert reason in service.unavailable_reason
    assert service.search("כתובה").total == 0


def test_no_file_or_not_a_database_is_not_served(tmp_path):
    assert "no sidecar" in CatalogTextService(db_path=str(tmp_path / "absent.db")).unavailable_reason
    junk = tmp_path / "junk.db"
    junk.write_bytes(b"not a database" * 100)
    service = CatalogTextService(db_path=str(junk))
    assert not service.is_ready() and "cannot open" in service.unavailable_reason


def test_available_needs_the_flag_which_is_off_by_default(service, monkeypatch):
    monkeypatch.delenv("CATALOG_TEXT_SEARCH_ENABLED", raising=False)
    assert service.is_ready() and not service.is_available()
    monkeypatch.setenv("CATALOG_TEXT_SEARCH_ENABLED", "1")
    assert service.is_available()
    monkeypatch.setenv("CATALOG_TEXT_SEARCH_ENABLED", "off")
    assert not service.is_available()


def test_a_title_outranks_a_full_text_and_prefixes_come_last(service):
    result = service.search("כתובה")
    assert _ids(result) == [A, C, B]
    assert [h.exact for h in result.hits] == [True, True, False]
    assert result.hits[0].field == "GenizahTitle" and result.hits[1].field == "FullText"
    assert result.total == 3 and not result.truncated


def test_quotes_turn_the_prefixes_off(service):
    assert _ids(service.search('"כתובה"')) == [A, C]


def test_the_translations_switch(service):
    with_translations = service.search("marriage contract")
    assert set(_ids(with_translations)) == {D, E, F}
    hit_d = next(h for h in with_translations.hits if h.sys_id == D)
    assert hit_d.automatic and hit_d.source == "fjms"
    assert set(_ids(service.search("marriage contract", include_translations=False))) == {E, F}


def test_an_original_outranks_an_automatic_translation(service):
    hits = service.search("ketubba").hits
    assert {h.sys_id for h in hits[:2]} == {E, F} and hits[-1].sys_id == D


def test_every_spelling_of_an_abbreviation_and_of_a_pointed_word(service):
    for query in ('רמב"ם', "רמב״ם", "רמבם"):
        assert H in _ids(service.search(query)), query
    assert _ids(service.search("שלום")) == [I]
    assert _ids(service.search("שָׁלוֹם")) == [I]


def test_work_title_and_author_translations_are_found(service):
    assert set(_ids(service.search("questions answers"))) == {H, M}
    assert _ids(service.search("Nahorai")) == [C]
    assert _ids(service.search("unknown")) == []


def test_an_english_plural_finds_the_singular_and_the_reverse(service):
    assert _ids(service.search("letters")) == [K]
    assert _ids(service.search("cheeses")) == _ids(service.search("cheese"))


def test_latin_accents_fold_both_ways(service):
    assert set(_ids(service.search("Fustat"))) == {E, F}
    assert set(_ids(service.search("fusṭāṭ"))) == {E, F}


def test_a_phrase_needs_its_words_in_order(service):
    assert set(_ids(service.search('"marriage contract"'))) == {D, E, F}
    assert _ids(service.search('"contract marriage"')) == []


def test_every_word_must_be_in_the_same_record(service):
    assert _ids(service.search("cheese Sicily")) == [K]
    assert _ids(service.search("cheese ketubba")) == []


@pytest.mark.parametrize("query, expected", [
    ("cheese OR ketubba", set()),        # OR is a word here, not an operator (else D, E, F, J, K)
    ("NEAR(cheese ketubba)", set()),
    ("title:cheese", set()),             # not a column filter
    ("-cheese", {J, K}),                 # not a NOT
    ("chees*", set()),                   # not a prefix query
    ("*", set()), ('"', set()), ("''", set()), (")", set()), ("^", set()),
])
def test_query_syntax_never_reaches_fts5(service, query, expected):
    assert set(_ids(service.search(query))) == expected


def test_an_empty_query_finds_nothing(service):
    assert service.search("").total == 0
    assert service.search("   ; ").total == 0


def test_limit_bounds_the_hits_not_the_count(service):
    result = service.search("כתובה", limit=1)
    assert len(result.hits) == 1 and result.total == 3 and result.truncated


def test_snippet_marks_the_matched_words(service):
    result = service.search("כתובה")
    by_id = {h.sys_id: h for h in result.hits}
    assert service.snippet(by_id[A], result) == "*כתובה*"
    assert "*הכתובה*" in service.snippet(by_id[B], result)
    full = service.snippet(by_id[C], result)
    assert "*כתובה*" in full and full.startswith("…") and full.endswith("…")


def test_snippet_keeps_a_literal_star_out_of_the_markers(service):
    result = service.search("cheese")
    hit = next(h for h in result.hits if h.sys_id == J)
    assert service.snippet(hit, result) == "Note ∗ with a star, about *cheese*"


def test_a_long_text_is_cut_around_its_match_without_cutting_words(service):
    result = service.search("cheese")
    hit = next(h for h in result.hits if h.sys_id == K)
    text = service.snippet(hit, result)
    assert "*cheese*" in text and text.startswith("…") and text.endswith("…")
    assert len(text) < 320
    body = text.strip("…")
    assert body.split()[0] in service.record_text(hit.record_id).split()


def test_a_phrase_snippet_shows_the_phrase_not_an_earlier_lone_word():
    text = ("The marriage of the writer's daughter is mentioned first. "
            + "Accounts of flax and silk follow at great length. " * 8
            + "At the end: the marriage contract itself, with its witnesses.")
    snippet = make_snippet(text, parse_query('"marriage contract"'), expand_prefixes=False)
    assert "*marriage* *contract*" in snippet
    assert snippet.startswith("…") and "daughter" not in snippet
    assert snippet.count("*") == 4   # the lone "marriage" elsewhere is not marked


def test_make_snippet_on_text_without_a_match():
    assert make_snippet("short text", parse_query("other"), expand_prefixes=False) == "short text"
    assert make_snippet("", parse_query("x"), expand_prefixes=False) == ""


# --- near spellings (plan §4.7) ---------------------------------------------------------

def test_near_spellings_are_off_unless_asked(service):
    result = service.search("כתובה")
    assert N not in _ids(result) and not result.near_spellings and result.near_forms == ()


def test_near_spellings_come_after_the_words_as_typed_and_their_prefixes(service):
    result = service.search("כתובה", near_spellings=True)
    assert _ids(result) == [A, C, B, N, P]
    assert [(h.exact, h.near) for h in result.hits] == [
        (True, False), (True, False), (False, False), (False, True), (False, True)]
    assert result.near_forms == ("כתבה",) and result.near_spellings


def test_a_hebrew_near_spelling_takes_the_prefixes_too(service):
    # P holds only הכתבה: the defective spelling after the article.
    result = service.search("כתובה", near_spellings=True)
    hit = next(h for h in result.hits if h.sys_id == P)
    assert "*הכתבה*" in service.snippet(hit, result)


def test_a_latin_near_spelling(service):
    assert _ids(service.search("Saadia")) == []
    result = service.search("Saadia", near_spellings=True)
    assert _ids(result) == [P] and result.near_forms == ("saadya",)
    assert "*Saadya*" in service.snippet(result.hits[0], result)


def test_only_near_spellings_the_file_holds_are_searched(service):
    result = service.search("cheese", near_spellings=True)
    assert result.near_forms == () and _ids(result) == _ids(service.search("cheese"))
    assert len(near_spellings("cheese")) > 100       # the candidates, none of them in the file


def test_a_phrase_gets_no_near_spellings(service):
    result = service.search('"כתובה"', near_spellings=True)
    assert _ids(result) == [A, C] and result.near_forms == ()


def test_near_spellings_need_every_word_in_one_record(service):
    assert _ids(service.search("Saadia commentary", near_spellings=True)) == [P]
    assert _ids(service.search("Saadia cheese", near_spellings=True)) == []


def test_only_the_first_words_get_near_spellings(service, monkeypatch):
    monkeypatch.setattr(svc, "MAX_NEAR_WORDS", 1)
    service._near_cache.clear()
    result = service.search("כתובה Saadia", near_spellings=True)
    assert result.terms[0].near == ("כתבה",) and result.terms[1].near == ()


def test_near_spellings_are_remembered_per_word(service, monkeypatch):
    service._near_cache.clear()
    calls = []
    real = svc._near_spellings_of
    monkeypatch.setattr(svc, "_near_spellings_of", lambda w: calls.append(w) or real(w))
    service.search("Saadia", near_spellings=True)
    service.search("Saadia", near_spellings=True)
    assert calls == ["saadia"]


def test_the_near_spelling_cache_is_bounded_and_safe_across_threads(service, monkeypatch):
    import threading
    monkeypatch.setattr(svc, "NEAR_CACHE_SIZE", 3)
    service._near_cache.clear()
    errors = []

    def search(word):
        try:
            for _ in range(5):
                service.search(word, near_spellings=True)
        except Exception as exc:     # noqa: BLE001
            errors.append(exc)
    words = ["Saadia", "ketuba", "Fustat", "cheese", "כתובה", "תפילין", "letters", "contract"]
    threads = [threading.Thread(target=search, args=(w,)) for w in words]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert not errors and len(service._near_cache) <= 3


def test_near_spellings_respect_the_translations_switch(service):
    result = service.search("ketuba", near_spellings=True, include_translations=False)
    assert set(_ids(result)) == {E, F} and "ketubba" in result.near_forms


def test_near_forms_are_shown_with_their_final_letters():
    from shared.catalog_text_normalize import display_form
    assert display_form("תפלינ") == "תפלין"
    assert display_form("saadya") == "saadya" and display_form("מ") == "מ"


# --- the inputs drifting (plan §4.6) ---------------------------------------------------

def test_input_drift_names_what_changed(service):
    assert service.input_drift({"input_fjms_version": "5.1.0", "input_pgp_upstream_commit": "abc123"}) == []
    lines = service.input_drift({"input_fjms_version": "5.2.0", "input_pgp_upstream_commit": "def456"})
    assert len(lines) == 2
    assert "5.1.0" in lines[0] and "5.2.0" in lines[0]
    assert "abc123" in lines[1] and "def456" in lines[1]


def test_an_unknown_installed_input_is_not_drift(service):
    assert service.input_drift({"input_fjms_version": None}) == []
    assert service.input_drift({}) == []


def test_open_and_check_logs_drift_and_never_raises(monkeypatch, sidecar, caplog):
    monkeypatch.setattr(svc, "default_sidecar_path", lambda: str(sidecar))
    monkeypatch.setattr(svc, "installed_inputs", lambda: {"input_fjms_version": "9.9.9"})
    svc.reset_catalog_text_service()
    try:
        with caplog.at_level("WARNING"):
            assert svc.open_and_check().is_ready()
        assert "9.9.9" in caplog.text and "5.1.0" in caplog.text
    finally:
        svc.reset_catalog_text_service()


def test_open_and_check_logs_a_file_it_cannot_serve(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(svc, "default_sidecar_path", lambda: str(tmp_path / "absent.db"))
    svc.reset_catalog_text_service()
    try:
        with caplog.at_level("WARNING"):
            assert not svc.open_and_check().is_ready()
        assert "not served" in caplog.text
    finally:
        svc.reset_catalog_text_service()


def test_not_ready_service_answers_empty_and_never_raises(tmp_path):
    service = CatalogTextService(db_path=str(tmp_path / "absent.db"))
    assert service.search("כתובה").hits == ()
    assert service.record_text(1) == ""


def test_the_process_wide_instance_can_be_reset(monkeypatch, sidecar):
    monkeypatch.setattr(svc, "default_sidecar_path", lambda: str(sidecar))
    svc.reset_catalog_text_service()
    first = svc.get_catalog_text_service(thread_safe=False)
    assert first is svc.get_catalog_text_service() and first.is_ready()
    svc.reset_catalog_text_service()
    assert svc.get_catalog_text_service(thread_safe=False) is not first
    svc.reset_catalog_text_service()
