"""Phase 118 UAT follow-up: the words row reads RIGHT-TO-LEFT.

The Genizah corpus is Hebrew, so within a line the FIRST word must sit rightmost
and each '+ Add word' must appear to its LEFT (Hebrew reading order) — in every UI
language. This is a `direction: rtl` on the words-row container; DOM order is
unchanged (word0, gap0, word1, ...) so the composed query order is unaffected.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _storage_secret():
    from nicegui import app
    try:
        app.storage.secret = 'test-secret'  # noqa: S105 (test-only)
    except Exception:
        pass
    yield


def _words_rows():
    from nicegui import context, ui
    from web.components.joins_builder import create_joins_builder

    before = set(context.client.elements.keys())
    with ui.column():
        create_joins_builder(allow_page_position=True)
    return [
        el for eid, el in context.client.elements.items()
        if eid not in before and 'jl-words-row' in ' '.join(getattr(el, '_classes', []) or [])
    ]


def test_words_row_is_rtl():
    rows = _words_rows()
    assert rows, 'expected a words row tagged jl-words-row'
    style = dict(getattr(rows[0], '_style', {}) or {})
    assert style.get('direction') == 'rtl', f'words row must be RTL, got {style}'


def test_compose_order_unaffected_by_rtl_layout():
    """RTL is layout-only — DOM/state order stays word0..wordN, so compose() order
    is left untouched (sanity: build_side_query still produces words in typed order)."""
    from shared.joins_lab import compose
    from web.components.joins_builder import build_side_query

    lines = [{
        'words': [
            {'term': 'אמר', 'mods': {}, 'gap_to_next_word': 0},
            {'term': 'רבי', 'mods': {}, 'gap_to_next_word': 0},
        ],
        'line_start': False, 'line_end': False, 'gap_to_next_line': 0,
    }]
    sq = build_side_query(lines, variants=False, page_position=None)
    query_str, _ro, _pp = compose(sq)
    # first typed word precedes the second in the query string (order preserved)
    assert query_str.index('אמר') < query_str.index('רבי')
