# -*- coding: utf-8 -*-
"""web/joins_lab_link.py: the Joins Lab screen as a link, and reading one safely.

A shared link is untrusted input: anything it carries is coerced or dropped, and
an unreadable or oversized token gives None, never an exception.
"""
from __future__ import annotations

import base64
import json
import zlib
from urllib.parse import parse_qs, urlparse

import pytest

from web import joins_lab_link as jl

ANCHOR = "990000000010205171"
CAND = "990000000020205171"


def _row(*terms):
    return {
        "words": [{"term": t, "mods": {"prefix": True}, "gap_to_next_word": 0} for t in terms],
        "line_start": False, "line_end": False, "gap_to_next_line": 0,
    }


FULL = {
    "b": {"rows": [_row("קדוש", "ברוך")], "type": "responsa", "var": True,
          "text": "", "pos": "anywhere"},
    "g": {"flex": True, "bidir": False},
    "o": {"on": True, "rows": [_row("שלום")], "combine": "OR", "pos": "end"},
    "vs": True,
    "f": {"materials": ["paper"], "has_dims": True, "exclude_mismatch": False,
          "triage_states": ["Yes"], "text_q": "T-S"},
    "view": "table",
    "gp": 3,
    "cmp": [CAND, 2],
    "m": {CAND: "yes", ANCHOR: "maybe"},
}


def _token(payload) -> str:
    raw = json.dumps(payload).encode()
    return base64.urlsafe_b64encode(zlib.compress(raw)).decode().rstrip("=")


def test_round_trip_keeps_every_part():
    token = jl.encode_share_state(FULL)
    assert jl.decode_share_state(token) == jl.clean_share_state(FULL)
    assert jl.decode_share_state(token)["m"] == {CAND: "yes", ANCHOR: "maybe"}


def test_url_keeps_the_anchor_readable():
    url = jl.build_share_url(ANCHOR, page=2, fl_id="T-S 1.1r", volume_ie="IE1", state=FULL)
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    assert parsed.path == "/joins-lab"
    assert q["sys_id"] == [ANCHOR] and q["page"] == ["2"]
    assert q["fl_id"] == ["T-S 1.1r"] and q["volume_ie"] == ["IE1"]
    assert jl.decode_share_state(q[jl.SHARE_PARAM][0])["gp"] == 3


def test_an_empty_screen_adds_no_state_parameter():
    assert jl.build_share_url(ANCHOR) == f"/joins-lab?sys_id={ANCHOR}"


def test_a_link_without_marks_carries_none():
    state = dict(FULL)
    state.pop("m")
    assert "m" not in jl.decode_share_state(jl.encode_share_state(state))


@pytest.mark.parametrize("token", [
    None, 123, "", "!!!not base64!!!", _token([1, 2]), _token({"v": 99, "gp": 1}),
    base64.urlsafe_b64encode(b"not zlib").decode(),
    "A" * (jl.MAX_TOKEN_CHARS + 1),
])
def test_unreadable_tokens_give_none(token):
    assert jl.decode_share_state(token) is None


def test_a_decompression_bomb_is_refused():
    bomb = {"v": 1, "b": {"text": "x" * (jl.MAX_DECODED_BYTES * 2)}}
    token = _token(bomb)
    assert len(token) < jl.MAX_TOKEN_CHARS      # small on the wire...
    assert jl.decode_share_state(token) is None  # ...but refused when inflated


def test_unknown_keys_and_bad_values_are_dropped():
    hostile = {
        "v": 1,
        "evil": "<script>",
        "b": {"rows": "not a list", "type": "rm -rf", "text": 5, "pos": "nowhere"},
        "g": "yes",
        "o": {"on": True, "rows": [], "combine": "XOR"},
        "view": "carousel",
        "gp": -4,
        "cmp": ["not-a-sys-id", 1],
        "m": {"bogus": "yes", CAND: "definitely", "990000000030205171": "no"},
        "f": {"materials": ["paper", 7], "triage_states": ["Yes", "Delete"], "text_q": ["x"]},
    }
    got = jl.decode_share_state(_token(hostile))
    assert "evil" not in got and "g" not in got and "view" not in got and "gp" not in got
    assert "cmp" not in got
    assert got["b"] == {"rows": [], "var": False, "text": ""}
    assert got["o"]["combine"] == "AND"
    assert got["m"] == {"990000000030205171": "no"}
    assert got["f"]["materials"] == ["paper"] and got["f"]["triage_states"] == ["Yes"]
    assert got["f"]["text_q"] == ""


def test_rows_are_capped_and_mods_kept_boolean():
    rows = [_row(f"w{i}") for i in range(40)]
    rows[0]["words"][0]["mods"] = {"prefix": True, "evil": "x", "suffix": False}
    rows[0]["words"][0]["gap_to_next_word"] = 999
    got = jl.clean_share_state({"b": {"rows": rows}})
    assert len(got["b"]["rows"]) == 20
    assert got["b"]["rows"][0]["words"][0]["mods"] == {"prefix": True, "suffix": False}
    assert got["b"]["rows"][0]["words"][0]["gap_to_next_word"] == 0


def test_marks_are_capped():
    marks = {f"99{i:016d}": "yes" for i in range(jl.MAX_LINK_MARKS + 50)}
    assert len(jl.clean_share_state({"m": marks})["m"]) == jl.MAX_LINK_MARKS


def test_compare_page_may_be_unknown():
    got = jl.clean_share_state({"cmp": [CAND, None]})
    assert got["cmp"] == [CAND, None]


def test_stored_session_in_link_shape():
    stored = {
        "builder_rows": [_row("קדוש")], "builder_mode": "variants",
        "single_text": "", "text_position": "anywhere",
        "flex_spacing": False, "bidirectional": True,
        "other_side_enabled": False, "other_side_rows": [_row("x")],
        "active_filter": {"has_dims": True}, "view_mode": "grid",
    }
    got = jl.share_inputs_from_full_state(stored)
    assert got["b"]["type"] == "variants" and got["b"]["var"] is True   # legacy inference
    assert got["g"] == {"flex": False, "bidir": True}
    assert "o" not in got                                               # other side off
    assert got["f"]["has_dims"] is True


def test_the_other_side_keeps_its_own_text_position():
    """The cross-side search uses the other side's own position, so a link and a
    stored session carry it; a bad one is dropped, and a blob saved before it was
    stored has none (the restore then falls back to the anchor's, as before)."""
    assert jl.decode_share_state(jl.encode_share_state(FULL))["o"]["pos"] == "end"
    assert "pos" not in jl.clean_share_state(
        {"o": {"on": True, "rows": [], "pos": "middle"}})["o"]
    stored = {"builder_rows": [_row("a")], "text_position": "anywhere",
              "other_side_enabled": True, "other_side_rows": [_row("b")],
              "other_side_combine": "AND", "other_side_text_position": "line_end"}
    assert jl.share_inputs_from_full_state(stored)["o"]["pos"] == "line_end"
    del stored["other_side_text_position"]
    assert "pos" not in jl.share_inputs_from_full_state(stored)["o"]
    a = jl.clean_share_state(FULL)
    b = jl.clean_share_state({**FULL, "o": {**FULL["o"], "pos": "start"}})
    assert not jl.same_inputs(a, b), "another other-side position is another search"


def test_same_inputs_ignores_view_page_and_marks():
    a = jl.clean_share_state(FULL)
    b = jl.clean_share_state({**FULL, "gp": 0, "view": "grid", "m": {}, "cmp": None})
    assert jl.same_inputs(a, b)
    c = jl.clean_share_state({**FULL, "vs": False})
    assert not jl.same_inputs(a, c)


def _huge_builder():
    """A builder at its caps: 20 lines x 50 words x 200 random letters."""
    import random
    rng = random.Random(7)
    letters = "אבגדהוזחטיכלמנסעפצקרשת"
    return {"b": {"rows": [
        {"words": [{"term": "".join(rng.choice(letters) for _ in range(200)),
                    "mods": {}, "gap_to_next_word": 0} for _ in range(50)],
         "line_start": False, "line_end": False, "gap_to_next_line": 0}
        for _ in range(20)
    ]}}


def test_a_screen_too_big_for_a_link_is_refused_not_broken():
    """encode never returns a token the decoder would refuse (Codex P2 on #408)."""
    with pytest.raises(jl.LinkTooLong):
        jl.encode_share_state(_huge_builder())
    with pytest.raises(jl.LinkTooLong):
        jl.build_share_url(ANCHOR, state=_huge_builder())
    assert jl.build_share_url(ANCHOR) == f"/joins-lab?sys_id={ANCHOR}"


@pytest.mark.parametrize("lines", [1, 5, 10, 15, 20])
def test_every_token_encode_returns_decodes(lines):
    state = _huge_builder()
    state["b"]["rows"] = state["b"]["rows"][:lines]
    for row in state["b"]["rows"]:
        row["words"] = row["words"][:10]
    try:
        token = jl.encode_share_state(state)
    except jl.LinkTooLong:
        return
    assert jl.decode_share_state(token) == jl.clean_share_state(state)


def test_library_domain_filters_and_the_sort_ride_in_the_link():
    """PR 4: the Library/Domain filters and the grid's Sort are part of the screen.
    A stored session goes through the same cleaner, so they survive a restore too;
    a bad Sort is the search's order, a bad list item is dropped."""
    f = {"libraries": ["CUL", 3, "JTS"], "domains": ["Bible"], "order": "domain"}
    got = jl.decode_share_state(jl.encode_share_state({"f": f}))["f"]
    assert got["libraries"] == ["CUL", "JTS"]
    assert got["domains"] == ["Bible"]
    assert got["order"] == "domain"
    assert jl.clean_share_state({"f": {"order": "nonsense"}})["f"]["order"] == "rank"
    stored = {"builder_rows": [_row("a")], "active_filter": f}
    assert jl.share_inputs_from_full_state(stored)["f"]["domains"] == ["Bible"]


def test_the_domain_filters_mode_rides_in_the_link():
    """Show only or hide the Domain filter's domains: kept by a link and a stored
    session; a link from before it, or a bad value, shows only (as it always did)."""
    f = {"domains": ["Bible", "Law"], "domains_mode": "hide"}
    got = jl.decode_share_state(jl.encode_share_state({"f": f}))["f"]
    assert got["domains"] == ["Bible", "Law"] and got["domains_mode"] == "hide"
    stored = {"builder_rows": [_row("a")], "active_filter": f}
    assert jl.share_inputs_from_full_state(stored)["f"]["domains_mode"] == "hide"
    for bad in ({"domains": ["Bible"]}, {"domains_mode": "exclude"}, {"domains_mode": 1}):
        assert jl.clean_share_state({"f": bad})["f"]["domains_mode"] == "show_only"
