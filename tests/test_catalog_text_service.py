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

from catalog_text_fixture import A, B, C, D, E, F, H, I, J, K, build_sidecar  # noqa: E402
from scripts.build_catalog_text_sidecar import SCHEMA_VERSION as BUILDER_SCHEMA_VERSION  # noqa: E402
from shared import catalog_text_service as svc  # noqa: E402
from shared.catalog_text_service import CatalogTextService, make_snippet  # noqa: E402
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


def test_make_snippet_on_text_without_a_match():
    assert make_snippet("short text", parse_query("other"), expand_prefixes=False) == "short text"
    assert make_snippet("", parse_query("x"), expand_prefixes=False) == ""


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
