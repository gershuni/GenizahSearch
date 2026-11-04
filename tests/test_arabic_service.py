# -*- coding: utf-8 -*-
"""shared/arabic_service.py on a synthetic sidecar written by the real builder.

The lookup is the plan's method v4 over the sidecar: the readings ranked by the stored count, the
first reading's root through the closed-class table, Lane's headwords, then stripping, and the
root's entries with the matched ones first. Fail-closed: a missing file, another schema version, a
sidecar keyed by another normalizer, one without the Perseus credit line, a missing lookup index,
or the kill switch off each leave the service unavailable and every lookup empty.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import scripts.build_arabic_sidecar as build
from shared import arabic_service
from shared.arabic_normalize import root_key
from shared.arabic_service import ArabicService, cut_text, entry_url, root_url
from tests.test_build_arabic_sidecar import METADATA_SHA256, SHARDS, make_closed_class, make_source


@pytest.fixture
def sidecar(tmp_path, monkeypatch) -> Path:
    monkeypatch.setattr(build, "EMEIRI_SHARDS", SHARDS)
    monkeypatch.setattr(build, "OPENITI_METADATA_SHA256", METADATA_SHA256)
    monkeypatch.delenv("LANE_DICTIONARY_ENABLED", raising=False)
    source = make_source(tmp_path / "src")
    closed = make_closed_class(tmp_path / "closed.json")
    out = tmp_path / "arabic.db"
    assert build.main(["build", "--source-dir", str(source), "--out", str(out), "--closed-class", str(closed),
                       "--workers", "1"]) == build.EXIT_OK
    return out


def _service(path: Path) -> ArabicService:
    service = ArabicService(str(path))
    assert service.is_ready(), service.unavailable_reason
    return service


def test_the_reader_understands_the_builder_s_schema():
    assert arabic_service.SCHEMA_VERSION == build.SCHEMA_VERSION
    assert arabic_service.PERSEUS_CREDIT == build.PERSEUS_CREDIT
    assert set(arabic_service.COUNTED_TABLES) == set(build.COUNTED_TABLES)


def test_the_commonest_reading_first_and_its_closed_class_root(sidecar):
    """דלך: ذلك is counted 4 times, دلك once (not stored); the closed-class table files ذلك under
    Lane's ذا and names that headword."""
    service = _service(sidecar)
    for word in ("דלך", "ד'לך"):
        result = service.lookup(word)
        assert result.script == "ja" and [r.skeleton for r in result.readings] == ["ذلك"]
        assert result.readings[0].count == 4
        [root] = result.roots
        assert root.root_key == root_key("ذا") and root.name == "ذا"
        assert [(e.entry_id, e.matched) for e in root.entries] == [("lane:t:5", True)]
        assert root.entries[0].url == entry_url("lane:t:5") and root.url == root_url("ذا")
    assert service.credit == build.PERSEUS_CREDIT


def test_a_preposition_with_a_pronoun_reaches_the_letter_s_article(sidecar):
    [root] = _service(sidecar).lookup("לנא").roots
    assert root.root_key == "ل" and root.entries[0].entry_id == "lane:t:4" and root.entries[0].matched


def test_the_headword_table_finds_the_root_and_its_entry_comes_first(sidecar):
    result = _service(sidecar).lookup("כתאב")
    assert [r.skeleton for r in result.readings] == []  # كتاب is counted once: not offered
    result = _service(sidecar).lookup("כתב")
    assert [r.skeleton for r in result.readings] == ["كتب"]
    [root] = result.roots
    assert root.root_key == "كتب"
    assert [(e.entry_id, e.matched) for e in root.entries] == [("lane:t:1", True), ("lane:t:2", False)]


def test_typed_arabic_is_its_own_reading(sidecar):
    """No readings step: كِتَاب is looked up as it is, even uncounted, and reaches كتاب's entry."""
    result = _service(sidecar).lookup("كِتَاب")
    assert result.script == "arabic" and [(r.skeleton, r.spelling) for r in result.readings] == [("كتاب", "كتاب")]
    [root] = result.roots
    assert root.root_key == "كتب" and root.entries[0].entry_id == "lane:t:2" and root.entries[0].matched
    assert _service(sidecar).lookup("ذلك الكتاب").readings == (), "one word only"


def test_nothing_for_an_abbreviation_a_word_with_no_reading_or_no_letters(sidecar):
    service = _service(sidecar)
    # "ד לך" joined would be דלך, which is counted: two words are never read as one.
    for word in ("ר'", "קקק", "abc", "", "   ", "ד לך", "ד־לך", "ד-לך", "כל מה"):
        result = service.lookup(word)
        assert not result.found and not result.readings, word


def test_one_word_survives_its_dot_marks_and_niqqud():
    from shared.arabic_service import one_ja_word
    for word in ("דלך", "ד'לך", "ד׳לך", "ד̇לך", "דָלִךְ", "ר'"):
        assert one_ja_word(word), word
    for word in ("ד לך", "ד־לך", "ד-לך", "abc", ""):
        assert not one_ja_word(word), word


def test_the_entries_are_bounded_and_the_rest_counted(sidecar, monkeypatch):
    monkeypatch.setattr(arabic_service, "MAX_ENTRIES", 1)
    [root] = _service(sidecar).lookup("כתב").roots
    assert [e.entry_id for e in root.entries] == ["lane:t:1"] and root.more_entries == 1


def test_a_long_text_is_cut_at_a_space():
    text, cut = cut_text("word " * 200, limit=100)
    assert cut and text.endswith("…") and len(text) <= 101 and not text[:-1].endswith(" ")
    assert cut_text("  a short\n entry ", limit=100) == ("a short entry", False)


def _mutated(sidecar: Path, sql: str) -> ArabicService:
    con = sqlite3.connect(sidecar)
    con.executescript(sql)
    con.commit()
    con.close()
    return ArabicService(str(sidecar))


@pytest.mark.parametrize("sql, reason", [
    ("UPDATE meta SET value = '9.9.9' WHERE key = 'version'", "schema version"),
    ("UPDATE meta SET value = '99' WHERE key = 'normalizer_version'", "normalizer"),
    ("UPDATE meta SET value = '' WHERE key = 'lane_credit'", "credit"),
    ("UPDATE meta SET value = 'Text provided by Perseus.' WHERE key = 'lane_credit'", "Perseus credit line"),
    ("UPDATE meta SET value = '3' WHERE key = 'freq_min_stored'", "freq_min_stored"),
    ("DELETE FROM closed_class", "rows_closed_class"),
    ("DELETE FROM lane_forms WHERE entry_id = 'lane:t:1'", "rows_lane_forms"),
    ("DROP INDEX idx_lane_forms_skeleton", "index"),
    ("DROP TABLE closed_class", "missing table"),
    ("DELETE FROM freq WHERE skeleton = 'كتب'", "rows_freq"),
])
def test_a_sidecar_it_cannot_vouch_for_is_not_served(sidecar, sql, reason):
    service = _mutated(sidecar, sql)
    assert not service.is_ready() and reason in service.unavailable_reason, service.unavailable_reason
    assert not service.lookup("דלך").found


def test_a_missing_file_and_the_kill_switch(sidecar, tmp_path, monkeypatch):
    missing = ArabicService(str(tmp_path / "nope.db"))
    assert not missing.is_ready() and "no sidecar" in missing.unavailable_reason
    assert not missing.lookup("דלך").found
    service = _service(sidecar)
    monkeypatch.setenv("LANE_DICTIONARY_ENABLED", "0")
    assert not service.is_available() and not service.lookup("דלך").found
    monkeypatch.setenv("LANE_DICTIONARY_ENABLED", "1")
    assert service.lookup("דלך").found
