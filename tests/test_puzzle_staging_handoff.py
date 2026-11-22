# -*- coding: utf-8 -*-
"""Opening a whole join or document in the Fragment Puzzle (the ``puzzle_staging`` handoff).

The joins panel and the Reading Desk leave the join's fragments for the puzzle
page, which adds them when it opens. What is left must be what the page takes:
schema 1, each fragment once, in order, no more than the page reads.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import web.joins_lab_storage as storage

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def stored(monkeypatch):
    kept = {}
    monkeypatch.setattr(storage, 'safe_user_set', lambda key, value: kept.__setitem__(key, value) or True)
    return kept


def test_a_join_is_left_once_each_in_order(stored):
    left = storage.stage_fragments_for_puzzle(['990001', ' 990002 ', None, '', '990001', 990003])

    assert left == 3
    payload = stored['puzzle_staging']
    assert payload['fragments'] == ['990001', '990002', '990003']
    assert payload['schema_version'] == 1 and payload['source'] == 'join'
    assert payload['created_at']


def test_no_more_is_left_than_the_puzzle_page_takes(stored):
    left = storage.stage_fragments_for_puzzle([str(n) for n in range(40)])

    # the Puzzle's limit on both sides of the hand-off (the shelf picker's Add to Puzzle hands over 30)
    assert left == storage.PUZZLE_STAGING_MAX == 30
    assert stored['puzzle_staging']['fragments'] == [str(n) for n in range(30)]


def test_a_write_the_storage_refuses_is_reported_as_nothing_left(monkeypatch):
    """The caller navigates to /puzzle on a positive count; with nothing stored
    there, the visitor would land on an empty or unrelated canvas."""
    monkeypatch.setattr(storage, 'safe_user_set', lambda key, value: False)
    assert storage.stage_fragments_for_puzzle(['990001', '990002']) == 0


def test_the_joins_panel_stages_the_fragment_being_viewed_first():
    """Some joins' details list only the partners; the whole join includes the anchor."""
    tree = ast.parse((ROOT / 'web' / 'components' / 'joins_panel.py').read_text(encoding='utf-8'))
    handler = next(node for node in ast.walk(tree)
                   if isinstance(node, ast.FunctionDef) and node.name == 'handle_open_in_puzzle')
    staged = next(node for node in ast.walk(handler) if isinstance(node, ast.Assign)
                  and getattr(node.targets[0], 'id', '') == 'sys_ids')
    first = staged.value.left.elts[0]
    assert isinstance(first, ast.Name) and first.id == 'document_id'


@pytest.mark.parametrize('sys_ids', [[], None, ['', None, '  ']])
def test_nothing_is_written_for_no_fragments(stored, sys_ids):
    assert storage.stage_fragments_for_puzzle(sys_ids) == 0
    assert stored == {}


@pytest.mark.parametrize('same_canvas, added', [(True, 1), (False, 0)])
def test_a_fragment_looked_up_for_a_canvas_since_replaced_is_not_added(monkeypatch, same_canvas, added):
    """The folio lookup can take seconds. If another draft was opened meanwhile, a
    join's remaining fragments must not land in it (and be auto-saved there)."""
    import asyncio

    from web.pages import puzzle

    async def _folios(_func, _sys_id):
        return [{'fl_id': 'FL1', 'label': '1r'}]

    scripts = []
    monkeypatch.setattr(puzzle.run, 'io_bound', _folios)
    monkeypatch.setattr(puzzle.ui, 'run_javascript', scripts.append)
    monkeypatch.setattr(puzzle.state, 'meta_mgr', None)
    pending = {}

    asyncio.run(puzzle._add_fragment_by_sys_id('990001', 'ENA 1', {}, pending, None,
                                               still_wanted=lambda: same_canvas))

    assert len([s for s in scripts if 'addFragment(' in s]) == added
    assert len(pending) == added


def test_the_whole_join_is_tied_to_the_canvas_it_was_opened_for():
    """The call site: every fragment of the handoff is added with the canvas check."""
    source = (ROOT / 'web' / 'pages' / 'puzzle.py').read_text(encoding='utf-8')
    tree = ast.parse(source)
    bulk = next(node for node in ast.walk(tree)
                if isinstance(node, ast.AsyncFunctionDef) and node.name == 'auto_add_bulk')
    calls = [node for node in ast.walk(bulk) if isinstance(node, ast.Call)
             and getattr(node.func, 'id', '') == '_add_fragment_by_sys_id']
    assert len(calls) == 1
    # and with the canvas's room, asked once the fragment's key is known (a key already there is free)
    assert [kw.arg for kw in calls[0].keywords] == ['still_wanted', 'room_for']


def test_every_add_that_waits_for_a_folio_lookup_is_tied_to_its_canvas():
    """The shelfmark box, the picker, Add from List (two buttons), Known Joins, the
    join handoff, ``?add=`` and Pick from the shelf: each waits on a lookup during which
    another draft can be opened, so each must carry the canvas check."""
    tree = ast.parse((ROOT / 'web' / 'pages' / 'puzzle.py').read_text(encoding='utf-8'))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and getattr(node.func, 'id', '') == '_add_fragment_by_sys_id']
    assert len(calls) == 7
    unguarded = [call.lineno for call in calls if 'still_wanted' not in [kw.arg for kw in call.keywords]]
    assert unguarded == []

    def _function(name):
        return next(node for node in ast.walk(tree)
                    if isinstance(node, ast.AsyncFunctionDef) and node.name == name)

    def _names(node):
        return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}

    # The three toolbar actions wait before they add (a shelfmark lookup, the
    # visitor's lists, the known joins, an open dialog): each takes its check
    # as its first step, before any wait, and takes it once.
    for name in ('on_add_shelfmark', 'on_add_from_list', 'on_add_from_joins'):
        action = _function(name)
        taken = [n.lineno for n in ast.walk(action) if isinstance(n, ast.Call)
                 and getattr(n.func, 'id', '') == '_canvas_guard']
        first_wait = min(n.lineno for n in ast.walk(action) if isinstance(n, ast.Await))
        assert len(taken) == 1 and taken[0] < first_wait, name

    # ?add= and a join handed over from another page were asked for before the
    # page existed: they are held to the canvas the page started with, and
    # give up before their first lookup if it has been replaced.
    for name in ('auto_add', 'auto_add_bulk'):
        assert 'startup_canvas' in _names(_function(name)), name


def test_the_puzzle_page_reads_the_cap_and_the_source_this_module_writes():
    """The two halves agree: the page cuts at the Puzzle's limit, the one this module writes up to
    (both ``shared.shelfmark_tree_limits.PUZZLE_MAX_ITEMS``), and it knows the source name."""
    from shared.shelfmark_tree_limits import PUZZLE_MAX_ITEMS
    tree = ast.parse((ROOT / 'web' / 'pages' / 'puzzle.py').read_text(encoding='utf-8'))
    uppers = [node.slice.upper for node in ast.walk(tree)
              if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Slice)
              and isinstance(node.value, ast.Call) and getattr(node.value.func, 'id', '') == 'list'
              and node.slice.upper is not None]
    assert any(isinstance(u, ast.Name) and u.id == 'PUZZLE_MAX_ITEMS' for u in uppers)
    assert storage.PUZZLE_STAGING_MAX == PUZZLE_MAX_ITEMS
    compared = {node.comparators[0].value for node in ast.walk(tree)
                if isinstance(node, ast.Compare) and isinstance(node.comparators[0], ast.Constant)}
    assert storage.PUZZLE_STAGING_SOURCE_JOIN in compared
