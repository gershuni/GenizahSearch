# -*- coding: utf-8 -*-
"""Headless builder tests: slash-group OR + PER-ROW modifier HOIST round-trip.

No QApplication. No PyQt6 import. Pure logic exercised through:
  - shared.joins_lab: BuilderRow, SideQuery, compose()
  - genizah_core: parse_responsa_query

Tests pin the contracts:
  RR-1:  OR boxes compose to (w1/w2) slash-group, NOT | — verified via the parser.
  RR-13: Modifiers are PER-ROW and HOISTED OUTSIDE the OR group.
         Single-box → decorate the lone token (#שלום / שלום* / -עץ).
         Multi-box  → build the group (a/b) THEN hoist mods outside:
                      #(a/b) / %(a/b) / -(a/b) / (a/b)# / (a/b)*.
         wildcard-PREFIX is NOT hoistable onto a multi-box group (single-box only).

The `build_term` helper here IS the exact logic the Wave-2 widget's build_side_query
will inline per RR-13 (genizah_core.py:6014-6027 single-token decoration order).
"""
import pytest

from shared.joins_lab import BuilderRow, SideQuery, compose
from genizah_core import parse_responsa_query


# ── Local helpers ─────────────────────────────────────────────────────────────


def build_term(tokens: list[str], mods: dict) -> str:
    """Reproduce the Wave-2 build_side_query PER-ROW HOIST (RR-13).

    `tokens` = non-empty cleaned box texts for the row.
    `mods`   = the row's single mods dict (keys: negation, plene, prefix, suffix,
               wildcard_suffix, wildcard_prefix).

    Single-box row → decorate the lone token in the exact genizah_core.py:6014-6027 order:
      negation → "-" + t (overrides all other mods)
      else: % (plene) → # (prefix) → append # (suffix) → * (wildcard_prefix pre) →
            append * (wildcard_suffix)
    Multi-box row → group = "(a/b/c)", then HOIST row mods OUTSIDE the group:
      negation → "-(group)"; else apply % # #-append *-append in order.
      wildcard-PREFIX is NOT hoistable onto a group — single-box only.
    """
    if not tokens:
        return ""
    if len(tokens) == 1:
        t = tokens[0]
        if mods.get("negation"):
            return "-" + t
        if mods.get("plene"):
            t = "%" + t
        if mods.get("prefix"):
            t = "#" + t
        if mods.get("suffix"):
            t = t + "#"
        if mods.get("wildcard_prefix"):
            t = "*" + t
        if mods.get("wildcard_suffix"):
            t = t + "*"
        return t
    else:
        # Multi-box: build the group, then hoist row mods outside
        group = "(" + "/".join(tokens) + ")"
        if mods.get("negation"):
            return "-" + group
        t = group
        if mods.get("plene"):
            t = "%" + t
        if mods.get("prefix"):
            t = "#" + t
        if mods.get("suffix"):
            t = t + "#"
        # wildcard-PREFIX is NOT hoistable on a multi-box group (parser limitation)
        if mods.get("wildcard_suffix"):
            t = t + "*"
        return t


def side_from_rows(rows_spec, variants=False, page_position=None) -> SideQuery:
    """Build a SideQuery from a list of (boxes, mods, line_start, line_end, gap) tuples.

    boxes is a list of single-token box strings; mods is the row's mods dict.
    """
    builder_rows = []
    for boxes, mods, line_start, line_end, gap in rows_spec:
        cleaned = [b.strip() for b in boxes if b.strip()]
        term = build_term(cleaned, mods)
        builder_rows.append(BuilderRow(
            term=term,
            line_start=line_start,
            line_end=line_end,
            gap_to_next=gap,
        ))
    return SideQuery(
        rows=tuple(builder_rows),
        variants=variants,
        page_position=page_position,
    )


# ── Empty / bare term ─────────────────────────────────────────────────────────


class TestEmptyAndBare:
    """Empty and bare-term contracts."""

    def test_all_empty_rows_compose_to_falsy(self):
        """A SideQuery whose every row has an empty term composes to a falsy query string."""
        side = SideQuery(rows=(BuilderRow(term=""), BuilderRow(term="  ")))
        query_str, ro, pp = compose(side)
        assert not query_str

    def test_single_box_no_mods_bare_term(self):
        """Single box, no mods → BuilderRow.term == 'פירוש' (no parens — additive)."""
        side = side_from_rows([
            (["פירוש"], {}, False, False, 0),
        ])
        assert side.rows[0].term == "פירוש"
        query_str, _ro, _pp = compose(side)
        assert query_str and "פירוש" in query_str

    def test_single_box_compose_returns_bare_term(self):
        """Single-row, single-box compose returns the bare term (no wrapper parens)."""
        side = SideQuery(rows=(BuilderRow(term="שלום"),))
        query_str, _ro, _pp = compose(side)
        assert query_str == "שלום"


# ── OR slash-group (RR-1) ─────────────────────────────────────────────────────


class TestOrSlashGroup:
    """RR-1: OR boxes must produce (w1/w2) slash-groups, not | strings."""

    def test_two_boxes_produce_slash_group_term(self):
        """Two no-mod boxes → BuilderRow.term == '(פירוש/פירש)' (slash-group, no spaces)."""
        side = side_from_rows([
            (["פירוש", "פירש"], {}, False, False, 0),
        ])
        assert side.rows[0].term == "(פירוש/פירש)"

    def test_compose_contains_slash_group(self):
        """compose() on a two-box row contains the slash-group substring."""
        side = side_from_rows([
            (["פירוש", "פירש"], {}, False, False, 0),
        ])
        query_str, _ro, _pp = compose(side)
        assert "(פירוש/פירש)" in query_str

    def test_engine_documented_or_sanity(self):
        """Sanity: the engine parser documents (עץ/אילן) → words=['עץ','אילן']
        (genizah_core.py:5727 — mirrors the plan's explicit example)."""
        comps = parse_responsa_query("(עץ/אילן)")
        assert len(comps) == 1
        assert sorted(comps[0].words) == sorted(["עץ", "אילן"])

    def test_parser_level_or_regression(self):
        """RR-1 REGRESSION: the composed (פירוש/פירש) term parses as an OR group
        with words=[פירוש, פירש] — NOT as a single word with a | separator.

        This is the load-bearing parser-level assertion (a compose()-only | test
        would lock in the bug per RR-1).
        """
        side = side_from_rows([
            (["פירוש", "פירש"], {}, False, False, 0),
        ])
        query_str, _ro, _pp = compose(side)
        assert query_str  # must produce a non-empty query

        # Parse the composed string and find the OR component
        comps = parse_responsa_query(query_str)
        or_comps = [c for c in comps if len(c.words) > 1]
        assert or_comps, (
            f"RR-1: parse_responsa_query({query_str!r}) returned no OR component. "
            f"Components: {comps}"
        )
        or_comp = or_comps[0]
        assert sorted(or_comp.words) == sorted(["פירוש", "פירש"]), (
            f"RR-1: OR component words {or_comp.words!r} != ['פירוש','פירש']"
        )


# ── Single-box modifier round-trip (RR-13 single-box) ────────────────────────


class TestSingleBoxModifiers:
    """RR-13: single-box rows decorate the lone token and the parser recognizes it."""

    def test_prefix_mod_single_box(self):
        """prefix mod on ['שלום'] → build_term == '#שלום'."""
        t = build_term(["שלום"], {"prefix": True})
        assert t == "#שלום"

    def test_prefix_parse_round_trip(self):
        """#שלום → parse_responsa_query → grammatical_prefixes=True."""
        comps = parse_responsa_query("#שלום")
        assert len(comps) == 1
        assert comps[0].grammatical_prefixes is True

    def test_suffix_mod_single_box(self):
        """suffix mod on ['שלום'] → 'שלום#'."""
        t = build_term(["שלום"], {"suffix": True})
        assert t == "שלום#"

    def test_suffix_parse_round_trip(self):
        """שלום# → grammatical_suffixes=True."""
        comps = parse_responsa_query("שלום#")
        assert len(comps) == 1
        assert comps[0].grammatical_suffixes is True

    def test_plene_mod_single_box(self):
        """plene mod on ['שלום'] → '%שלום'."""
        t = build_term(["שלום"], {"plene": True})
        assert t == "%שלום"

    def test_plene_parse_round_trip(self):
        """%שלום → plene_defective=True."""
        comps = parse_responsa_query("%שלום")
        assert len(comps) == 1
        assert comps[0].plene_defective is True

    def test_wildcard_suffix_mod_single_box(self):
        """wildcard_suffix mod on ['שלום'] → 'שלום*'."""
        t = build_term(["שלום"], {"wildcard_suffix": True})
        assert t == "שלום*"

    def test_wildcard_suffix_parse_round_trip(self):
        """שלום* → wildcard=='suffix'."""
        comps = parse_responsa_query("שלום*")
        assert len(comps) == 1
        assert comps[0].wildcard == "suffix"

    def test_wildcard_prefix_mod_single_box(self):
        """wildcard_prefix mod on ['נדר'] → '*נדר'."""
        t = build_term(["נדר"], {"wildcard_prefix": True})
        assert t == "*נדר"

    def test_wildcard_prefix_parse_round_trip(self):
        """*נדר → wildcard=='prefix'."""
        comps = parse_responsa_query("*נדר")
        assert len(comps) == 1
        assert comps[0].wildcard == "prefix"

    def test_negation_mod_single_box(self):
        """negation mod on ['עץ'] → '-עץ' (leading minus form)."""
        t = build_term(["עץ"], {"negation": True})
        assert t == "-עץ"

    def test_prefix_full_chain(self):
        """Full chain: side_from_rows with prefix mod → compose → parse → grammatical_prefixes.

        This exercises the builder→compose→parser chain end-to-end (not just build_term).
        """
        side = side_from_rows([
            (["שלום"], {"prefix": True}, False, False, 0),
        ])
        query_str, _ro, _pp = compose(side)
        assert query_str
        comps = parse_responsa_query(query_str)
        assert comps[0].grammatical_prefixes is True, (
            f"Full chain: prefix mod did not produce grammatical_prefixes=True. "
            f"query={query_str!r}, comp={comps[0]!r}"
        )


# ── Multi-box HOISTED group modifier (RR-13 multi-box) ───────────────────────


class TestMultiBoxHoistedModifiers:
    """RR-13: multi-box rows hoist mods OUTSIDE the OR group.

    Assert the HOISTED forms (#(a/b), %(a/b), -(a/b), (a/b)#, (a/b)*) round-trip
    through parse_responsa_query to the correct group-level flags.

    The round-2 per-box (# a/%b) design is NOT tested here — that is the parser-broken
    form that RR-13 supersedes.
    """

    def test_prefix_hoist_build_term(self):
        """Row prefix on ['שלום','שלומות'] → build_term == '#(שלום/שלומות)'."""
        t = build_term(["שלום", "שלומות"], {"prefix": True})
        assert t == "#(שלום/שלומות)"

    def test_prefix_hoist_parse(self):
        """#(שלום/שלומות) → OR component with words=[שלום,שלומות] AND grammatical_prefixes=True."""
        comps = parse_responsa_query("#(שלום/שלומות)")
        assert len(comps) == 1
        c = comps[0]
        assert sorted(c.words) == sorted(["שלום", "שלומות"]), (
            f"Words: {c.words!r}"
        )
        assert c.grammatical_prefixes is True

    def test_prefix_hoist_full_chain(self):
        """Full chain: side_from_rows with prefix on multi-box → compose → parse → group-level flag."""
        side = side_from_rows([
            (["שלום", "שלומות"], {"prefix": True}, False, False, 0),
        ])
        query_str, _ro, _pp = compose(side)
        assert query_str
        comps = parse_responsa_query(query_str)
        or_comps = [c for c in comps if len(c.words) > 1]
        assert or_comps, f"No OR component in parse of {query_str!r}"
        c = or_comps[0]
        assert sorted(c.words) == sorted(["שלום", "שלומות"])
        assert c.grammatical_prefixes is True, (
            f"Full chain: hoisted prefix did not produce grammatical_prefixes=True on the group. "
            f"query={query_str!r}, comp={c!r}"
        )

    def test_negation_hoist_build_term(self):
        """Row negation on ['עץ','אילן'] → build_term == '-(עץ/אילן)'."""
        t = build_term(["עץ", "אילן"], {"negation": True})
        assert t == "-(עץ/אילן)"

    def test_negation_hoist_parse(self):
        """-(עץ/אילן) → negated=True with both alternatives in .words."""
        comps = parse_responsa_query("-(עץ/אילן)")
        assert len(comps) == 1
        c = comps[0]
        assert c.negated is True
        assert "עץ" in c.words
        assert "אילן" in c.words

    def test_negation_hoist_full_chain(self):
        """Full chain: negation on multi-box → compose → parse → negated group."""
        side = side_from_rows([
            (["עץ", "אילן"], {"negation": True}, False, False, 0),
        ])
        query_str, _ro, _pp = compose(side)
        assert query_str
        comps = parse_responsa_query(query_str)
        neg_comps = [c for c in comps if c.negated]
        assert neg_comps, f"No negated component in parse of {query_str!r}"
        c = neg_comps[0]
        assert "עץ" in c.words and "אילן" in c.words

    def test_wildcard_suffix_hoist_build_term(self):
        """Row wildcard_suffix on ['שלום','שלומות'] → build_term == '(שלום/שלומות)*'."""
        t = build_term(["שלום", "שלומות"], {"wildcard_suffix": True})
        assert t == "(שלום/שלומות)*"

    def test_wildcard_suffix_hoist_parse(self):
        """(שלום/שלומות)* → wildcard=='suffix' on the OR group."""
        comps = parse_responsa_query("(שלום/שלומות)*")
        assert len(comps) == 1
        c = comps[0]
        assert c.wildcard == "suffix"
        assert sorted(c.words) == sorted(["שלום", "שלומות"])

    def test_suffix_hoist_build_term(self):
        """Row suffix on ['שלום','שלומות'] → build_term == '(שלום/שלומות)#'."""
        t = build_term(["שלום", "שלומות"], {"suffix": True})
        assert t == "(שלום/שלומות)#"

    def test_suffix_hoist_parse(self):
        """(שלום/שלומות)# → grammatical_suffixes=True on the OR group."""
        comps = parse_responsa_query("(שלום/שלומות)#")
        assert len(comps) == 1
        c = comps[0]
        assert c.grammatical_suffixes is True
        assert sorted(c.words) == sorted(["שלום", "שלומות"])

    def test_plene_hoist_build_term(self):
        """Row plene on ['שלום','שלומות'] → build_term == '%(שלום/שלומות)'."""
        t = build_term(["שלום", "שלומות"], {"plene": True})
        assert t == "%(שלום/שלומות)"

    def test_plene_hoist_parse(self):
        """%(שלום/שלומות) → plene_defective=True on the OR group."""
        comps = parse_responsa_query("%(שלום/שלומות)")
        assert len(comps) == 1
        c = comps[0]
        assert c.plene_defective is True
        assert sorted(c.words) == sorted(["שלום", "שלומות"])

    def test_wildcard_prefix_not_hoistable_on_multi_box(self):
        """wildcard-PREFIX is NOT applied to multi-box groups — single-box only.

        The parser doesn't strip leading * before the OR check, so *(a/b) is not
        a wildcard-prefix on the group. Our build_term respects this by skipping
        wildcard_prefix on multi-box rows.
        """
        t = build_term(["שלום", "שלומות"], {"wildcard_prefix": True})
        # wildcard_prefix is silently ignored on multi-box — returns bare group
        assert t == "(שלום/שלומות)"
        assert not t.startswith("*")

    def test_three_box_slash_group(self):
        """Three boxes produce a three-way slash-group."""
        t = build_term(["א", "ב", "ג"], {})
        assert t == "(א/ב/ג)"
        comps = parse_responsa_query(t)
        assert sorted(comps[0].words) == sorted(["א", "ב", "ג"])


# ── Line anchors ──────────────────────────────────────────────────────────────


class TestLineAnchors:
    """Line-start / line-end anchors flow through compose()."""

    def test_line_start_two_row_side(self):
        """A two-row side where first row has line_start=True produces a non-empty query
        with the line-start marker (| prefixed to the first token)."""
        side = SideQuery(rows=(
            BuilderRow(term="שלום", line_start=True),
            BuilderRow(term="עולם"),
        ))
        query_str, _ro, _pp = compose(side)
        assert query_str
        assert "|שלום" in query_str

    def test_line_end_two_row_side(self):
        """A two-row side where second row has line_end=True produces a query
        with the line-end marker (| appended to the last token)."""
        side = SideQuery(rows=(
            BuilderRow(term="שלום"),
            BuilderRow(term="עולם", line_end=True),
        ))
        query_str, _ro, _pp = compose(side)
        assert query_str
        assert "עולם|" in query_str


# ── Page-position pass-through ────────────────────────────────────────────────


class TestPagePosition:
    """page_position is forwarded through compose() and validated."""

    def test_page_position_start_returned(self):
        """SideQuery with page_position='start' and a non-empty first row → compose()[2]=='start'."""
        side = SideQuery(
            rows=(BuilderRow(term="שלום"),),
            page_position="start",
        )
        _q, _ro, pp = compose(side)
        assert pp == "start"

    def test_page_position_end_empty_first_row_raises(self):
        """Pitfall 7: compose() raises ValueError when page_position='end' but last row is empty."""
        side = SideQuery(
            rows=(BuilderRow(term=""),),
            page_position="end",
        )
        with pytest.raises(ValueError):
            compose(side)

    def test_page_position_start_empty_first_row_raises(self):
        """Pitfall 7: compose() raises ValueError when page_position='start' but first row is empty."""
        side = SideQuery(
            rows=(BuilderRow(term=""),),
            page_position="start",
        )
        with pytest.raises(ValueError):
            compose(side)


# ── Self-check: no old | OR assertion ────────────────────────────────────────

def test_no_pipe_or_assertion_in_this_file():
    """RR-1 self-check: this file must NOT contain the old |.join() OR test pattern.

    Parsed as a raw string so the assertion itself doesn't trigger the guard.
    """
    import pathlib
    src = pathlib.Path(__file__).read_text(encoding="utf-8")
    # The forbidden pattern: joining boxes with a literal pipe as OR
    # (the pre-RR-1 design that locked in the bug)
    forbidden = ['"|\\"'.replace("\\'", "'") + ".join", "'|'" + ".join"]
    # Use a character-level check to avoid the pattern matching the guard itself
    pipe_join = chr(0x22) + "|" + chr(0x22) + ".join"
    assert pipe_join not in src, (
        "RR-1 self-check: test file contains a |.join OR assertion "
        "that would lock in the pre-RR-1 bug"
    )
