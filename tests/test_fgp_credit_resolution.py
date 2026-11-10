# -*- coding: utf-8 -*-
"""Team-credit resolution + DataSource parsing (Codex PR #309 review fixes).

P1 — overlapping team tokens (132 'חומר תיעודי' is a prefix of 131
'חומר תיעודי מאוחר') must NOT cross-contaminate: a 132-prefix row whose
manuscript also carries team 131's DataSource part must keep 132's head credit,
not Avraham David (131).

P3 — ``_datasource_parts`` must handle genuine dict/list values, not only the
``{eng:.., heb:..}`` pseudo-dict string that the live data always uses.
"""
from __future__ import annotations

from scripts.fgp_fill_credits_bilingual import (
    TEAM_CREDITS,
    TEAM_TOKENS,
    _datasource_parts,
    _token_matches,
    resolve_team_credit,
)

# DataSource parts as (en, he) pairs, mirroring build_credit_parts output.
_PART_131 = (None, "אברהם דוד, צוות FGP לחומר תיעודי מאוחר (עברית)")
_PART_132 = (None, "מרק כהן, צוות FGP לחומר תיעודי (גויטין)")
_PART_FIRKOVITCH = (None, "אילה אליהו, צוות FGP לאוספי פירקוביץ' (דוד סקליר, ראש הצוות)")


def test_token_132_does_not_match_131_superset():
    # 132's token must not match team 131's longer credit text.
    assert _token_matches(TEAM_TOKENS[132], _PART_131[1]) is False
    # ...but matches its own part and team 131 matches its own.
    assert _token_matches(TEAM_TOKENS[132], _PART_132[1]) is True
    assert _token_matches(TEAM_TOKENS[131], _PART_131[1]) is True


def test_132_row_with_131_part_keeps_132_head_not_avraham_david():
    # A 132 transcription on a multi-team manuscript carrying BOTH parts.
    he, en, cat = resolve_team_credit(132, [_PART_131, _PART_132])
    assert "אברהם דוד" not in he  # no 131 contamination
    assert "גויטין" in he  # kept its own (Goitein) part
    assert en == TEAM_CREDITS[132][1]
    assert cat == "team:132:matched"


def test_132_row_with_only_131_part_falls_back_to_head():
    # No own part present -> verified head credit, never the wrong team.
    he, en, cat = resolve_team_credit(132, [_PART_131])
    assert he == TEAM_CREDITS[132][0]
    assert cat == "team:132:fallback"


def test_firkovitch_keeps_individual_transcriber():
    he, en, cat = resolve_team_credit(107, [_PART_FIRKOVITCH])
    assert he == _PART_FIRKOVITCH[1]  # richer individual credit preserved
    assert "אילה אליהו" in he
    assert en == TEAM_CREDITS[107][1]
    assert cat == "team:107:matched"


def test_team_with_no_parts_uses_head_credit():
    he, en, cat = resolve_team_credit(105, [])
    assert he == TEAM_CREDITS[105][0]
    assert en == TEAM_CREDITS[105][1]
    assert cat == "team:105:fallback"


def test_token_matches_end_of_string():
    assert _token_matches("שו\"ת", 'צוות FGP לשו"ת') is True


def test_datasource_parts_pseudo_dict_string():
    parts = _datasource_parts("{eng: Gil, heb: גיל}")
    assert parts == [("Gil", "גיל")]


def test_datasource_parts_multiple_blocks():
    parts = _datasource_parts("{eng: A, heb: א}{eng: B, heb: ב}")
    assert parts == [("A", "א"), ("B", "ב")]


def test_datasource_parts_real_dict():
    # A genuine dict would survive json.dumps with quoted keys -> the regex would
    # miss it; handled directly now.
    assert _datasource_parts({"eng": "Gil", "heb": "גיל"}) == [("Gil", "גיל")]


def test_datasource_parts_real_list():
    parts = _datasource_parts([{"eng": "A", "heb": "א"}, {"eng": "B", "heb": "ב"}])
    assert parts == [("A", "א"), ("B", "ב")]


def test_datasource_parts_empty():
    assert _datasource_parts(None) == []
    assert _datasource_parts("") == []
