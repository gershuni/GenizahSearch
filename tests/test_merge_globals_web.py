# -*- coding: utf-8 -*-
"""BLD-04 RED stubs — _merge_globals_web re-injection assertions.

These tests verify that `_merge_globals_web` (to be added in Plan 04 to
web/pages/joins_lab.py) correctly re-injects global UI toggle state into the
compose()-produced `ro` dict. compose() hardcodes ja/flex_spacing/bidirectional=False;
_merge_globals_web restores flex_spacing + bidirectional from toggle state (RR-14
desktop parity, D-11). ja intentionally excluded per D-10.

Tests are RED until Plan 04 adds _merge_globals_web to web.pages.joins_lab.
Import is inside each test body so collection does NOT hard-fail before Plan 04 lands.
"""

from shared.joins_lab import BuilderRow, SideQuery, compose


def _get_merge_globals_web():
    """Import _merge_globals_web inside each test body to defer ImportError."""
    from web.pages.joins_lab import _merge_globals_web  # noqa: F401 — RED until Plan 04
    return _merge_globals_web


def test_flex_spacing_injected_into_ro():
    """compose() hardcodes flex_spacing=False; _merge_globals_web must set it to True."""
    _merge_globals_web = _get_merge_globals_web()
    side = SideQuery(rows=(BuilderRow(term='שלום'),), variants=False)
    _, ro, _ = compose(side)
    assert ro['flex_spacing'] is False  # compose hardcodes False
    _merge_globals_web(ro, {'flex_spacing': True, 'bidirectional': False})
    assert ro['flex_spacing'] is True


def test_bidirectional_injected_into_ro():
    """compose() hardcodes bidirectional=False; _merge_globals_web must set it to True."""
    _merge_globals_web = _get_merge_globals_web()
    side = SideQuery(rows=(BuilderRow(term='שלום'),), variants=False)
    _, ro, _ = compose(side)
    assert ro['bidirectional'] is False  # compose hardcodes False
    _merge_globals_web(ro, {'flex_spacing': False, 'bidirectional': True})
    assert ro['bidirectional'] is True


def test_ja_not_injected():
    """D-10: ja stays False regardless of any opts passed (user decision to drop JA)."""
    _merge_globals_web = _get_merge_globals_web()
    side = SideQuery(rows=(BuilderRow(term='אמת'),), variants=False)
    _, ro, _ = compose(side)
    # Call with ja in opts — must NOT propagate
    _merge_globals_web(ro, {'flex_spacing': False, 'bidirectional': False, 'ja': True})
    assert ro.get('ja') is False  # ja must never become True


def test_variants_not_touched_by_merge():
    """variants flows via SideQuery.variants; _merge_globals_web must not override it."""
    _merge_globals_web = _get_merge_globals_web()
    side = SideQuery(rows=(BuilderRow(term='שלום'),), variants=True)
    _, ro, _ = compose(side)
    assert ro['variants'] is True  # set by SideQuery.variants=True
    _merge_globals_web(ro, {'flex_spacing': False, 'bidirectional': False})
    assert ro['variants'] is True  # still True — not clobbered by merge


def test_merge_applies_to_other_side_ro():
    """_merge_globals_web is side-agnostic: can be called twice (anchor + other-side).

    Proves the helper is not anchor-specific — calling it on a second SideQuery's ro
    produces the same injection (BLD-04: applies to BOTH sides).
    """
    _merge_globals_web = _get_merge_globals_web()
    # Anchor side
    anchor = SideQuery(rows=(BuilderRow(term='שלום'),), variants=False)
    _, anchor_ro, _ = compose(anchor)
    _merge_globals_web(anchor_ro, {'flex_spacing': True, 'bidirectional': True})

    # Other side (separate SideQuery, separate ro)
    other = SideQuery(rows=(BuilderRow(term='אמת'),), variants=False)
    _, other_ro, _ = compose(other)
    _merge_globals_web(other_ro, {'flex_spacing': True, 'bidirectional': True})

    assert anchor_ro['flex_spacing'] is True
    assert anchor_ro['bidirectional'] is True
    assert other_ro['flex_spacing'] is True
    assert other_ro['bidirectional'] is True
