# -*- coding: utf-8 -*-
"""BLD-03 RED stubs — per-row modifier hoist + compose() line_start/end/gap assertions.

Two layers:

Part 1 (GREEN NOW): compose() line_start/line_end/gap behavior tested directly against
shared.joins_lab. These test existing code and pass immediately.

Part 2 (RED until Plan 03): per-row text-modifier hoist via
web.components.joins_builder._apply_modifiers_to_term. Import is inside each test
body so collection does NOT hard-fail before Plan 03 lands.

Modifier rules (from PATTERNS.md §"Per-row hoist rules", desktop parity RR-13):
  negation       → '-term' (single-token) or '-(term)' (slash-group)
  plene          → '%term'
  prefix         → '#term'
  suffix         → 'term#'
  wildcard_suffix → 'term*'
  wildcard_prefix → '*term' (NOT on slash-groups — RR-13 parity)
"""

from shared.joins_lab import BuilderRow, SideQuery, compose


# ---------------------------------------------------------------------------
# Part 1: compose() line_start / line_end / gap behavior (GREEN NOW)
# ---------------------------------------------------------------------------


def test_line_start_prepends_pipe():
    """BuilderRow(line_start=True) causes the first token to gain a '|' prefix."""
    rows = (
        BuilderRow(term='שלום', line_start=True),
        BuilderRow(term='עולם'),
    )
    side = SideQuery(rows=rows, variants=False)
    query_str, _, _ = compose(side)
    assert query_str is not None
    # The first token should start with '|'
    tokens = query_str.split()
    assert tokens[0].startswith('|'), f"Expected leading | in: {query_str!r}"


def test_line_end_appends_pipe():
    """BuilderRow(line_end=True) causes the last token of that row to gain a '|' suffix."""
    rows = (
        BuilderRow(term='שלום', line_end=True),
        BuilderRow(term='עולם'),
    )
    side = SideQuery(rows=rows, variants=False)
    query_str, _, _ = compose(side)
    assert query_str is not None
    # Split on gap markers + spaces to find the trailing pipe
    # The compose output for line_end is: 'שלום| [|0] עולם'
    # The token 'שלום|' should appear
    assert '|' in query_str, f"Expected | in: {query_str!r}"
    # More specifically: שלום| appears before the gap marker
    parts_before_gap = query_str.split('[|')[0]
    # Last meaningful token before gap/next row should end with '|'
    tokens_before = parts_before_gap.split()
    assert any(t.endswith('|') for t in tokens_before), (
        f"Expected a token ending with | before gap in: {query_str!r}"
    )


def test_gap_marker_emitted():
    """BuilderRow(gap_to_next=2) followed by another row emits [|2] in the query string."""
    rows = (
        BuilderRow(term='א', gap_to_next=2),
        BuilderRow(term='ב'),
    )
    side = SideQuery(rows=rows, variants=False)
    query_str, _, _ = compose(side)
    assert query_str is not None
    assert '[|2]' in query_str, f"Expected [|2] gap marker in: {query_str!r}"


def test_gap_zero_emitted_for_consecutive():
    """gap_to_next=0 (default) emits [|0] for consecutive rows."""
    rows = (
        BuilderRow(term='א', gap_to_next=0),
        BuilderRow(term='ב'),
    )
    side = SideQuery(rows=rows, variants=False)
    query_str, _, _ = compose(side)
    assert query_str is not None
    assert '[|0]' in query_str, f"Expected [|0] in: {query_str!r}"


# ---------------------------------------------------------------------------
# Part 2: _apply_modifiers_to_term — RED until Plan 03
# ---------------------------------------------------------------------------


def _get_apply_modifiers():
    """Import _apply_modifiers_to_term inside test body to defer ImportError (RED)."""
    from web.components.joins_builder import _apply_modifiers_to_term  # noqa: F401
    return _apply_modifiers_to_term


def test_negation_single_token():
    """Negation on a single token: 'שלום' → '-שלום'."""
    fn = _get_apply_modifiers()
    result = fn('שלום', {'negation': True})
    assert result == '-שלום', f"Got: {result!r}"


def test_negation_slash_group():
    """Negation on a slash-group: 'א/ב' → '-(א/ב)'."""
    fn = _get_apply_modifiers()
    result = fn('א/ב', {'negation': True})
    assert result == '-(א/ב)', f"Got: {result!r}"


def test_plene_prefix():
    """plene modifier: 'שלום' → '%שלום'."""
    fn = _get_apply_modifiers()
    result = fn('שלום', {'plene': True})
    assert result == '%שלום', f"Got: {result!r}"


def test_prefix_modifier():
    """prefix modifier: 'שלום' → '#שלום'."""
    fn = _get_apply_modifiers()
    result = fn('שלום', {'prefix': True})
    assert result == '#שלום', f"Got: {result!r}"


def test_suffix_modifier():
    """suffix modifier: 'שלום' → 'שלום#'."""
    fn = _get_apply_modifiers()
    result = fn('שלום', {'suffix': True})
    assert result == 'שלום#', f"Got: {result!r}"


def test_wildcard_suffix():
    """wildcard_suffix modifier: 'שלום' → 'שלום*'."""
    fn = _get_apply_modifiers()
    result = fn('שלום', {'wildcard_suffix': True})
    assert result == 'שלום*', f"Got: {result!r}"


def test_wildcard_prefix_not_applied_to_slash_group():
    """wildcard_prefix is NOT applied to slash-groups (RR-13 parity).

    'א/ב' with wildcard_prefix should NOT start with '*'.
    """
    fn = _get_apply_modifiers()
    result = fn('א/ב', {'wildcard_prefix': True})
    assert not result.startswith('*'), (
        f"wildcard_prefix must not apply to slash-groups, got: {result!r}"
    )


def test_wildcard_prefix_not_applied_to_prewrapped_slash_group():
    """CR LOW: an ALREADY-wrapped slash-group '(א/ב)' must also be exempt.

    The guard keyed off is_group (which excludes the pre-wrapped form), so '*'
    wrongly applied to '(א/ב)'. It must key off has_slash_group ('/' anywhere).
    """
    fn = _get_apply_modifiers()
    result = fn('(א/ב)', {'wildcard_prefix': True})
    assert not result.startswith('*'), (
        f"wildcard_prefix must not apply to pre-wrapped slash-groups, got: {result!r}"
    )
    assert result == '(א/ב)', result
