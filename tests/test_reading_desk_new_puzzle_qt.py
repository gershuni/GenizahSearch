# -*- coding: utf-8 -*-
"""The Reading Desk's "New Puzzle" (GS-294): the desk's fragments to a new puzzle in the real
Fragment Puzzle window (``PuzzleCanvasWindow.start_new_with``), after the window's own question
about the puzzle on its canvas. Cancelling it, or a save that fails, changes nothing."""
import pytest

pytestmark = pytest.mark.gui  # constructs the real window: gui bucket only

import puzzle_window_harness as pwh  # noqa: E402

SB = pwh.SB
DESK = [('990101', 'T-S 1'), ('990102', 'T-S 2')]


@pytest.fixture
def env(tmp_path, monkeypatch):
    e = pwh.PuzzleEnv.create(tmp_path, monkeypatch)
    yield e
    e.close()


def _canvas(env):
    return sorted(env.win._fragment_items)


def test_on_an_empty_canvas_the_desks_fragments_are_asked_for_at_once(env):
    assert env.win.start_new_with(DESK) is True
    assert env.asks == []  # nothing to lose, nothing asked
    assert [(m.sys_id, m.shelfmark) for m in pwh.FakeMetaLoader.made] == DESK


def test_cancel_leaves_the_puzzle_on_the_canvas(env):
    env.scratch_pad()
    before = _canvas(env)
    env.answer = SB.Cancel
    assert env.win.start_new_with(DESK) is False
    assert len(env.asks) == 1
    assert _canvas(env) == before and pwh.FakeMetaLoader.made == []


def test_discard_starts_the_new_puzzle_with_the_desks_fragments(env):
    env.scratch_pad()
    env.answer = SB.Discard
    assert env.win.start_new_with(DESK) is True
    assert _canvas(env) == [] and env.win._current_doc_id is None
    assert [m.sys_id for m in pwh.FakeMetaLoader.made] == ['990101', '990102']


def test_fragments_whose_folios_come_before_any_image_get_their_own_places(env):
    assert env.win.start_new_with(DESK) is True
    for meta in list(pwh.FakeMetaLoader.made):  # both folio lists arrive; no image yet
        meta.meta_ready.emit(meta.sys_id, meta.shelfmark, [{'label': '1r', 'fl_id': f'FL{meta.sys_id}'}])
    xs = sorted(pf.x for pf in env.win._pending_fragments.values())
    assert len(xs) == 2 and xs[1] - xs[0] >= 800
    pwh.finish_loads()
    pwh.pump()
    placed = sorted(item.pos().x() for item in env.win._fragment_items.values())
    assert len(placed) == 2 and placed[1] - placed[0] >= 800


def test_the_desks_order_is_kept_whatever_order_the_lookups_finish_in(env):
    assert env.win.start_new_with(DESK) is True
    for meta in reversed(list(pwh.FakeMetaLoader.made)):  # the second fragment's folios first
        meta.meta_ready.emit(meta.sys_id, meta.shelfmark, [{'label': '1r', 'fl_id': f'FL{meta.sys_id}'}])
    xs = {key[0]: pf.x for key, pf in env.win._pending_fragments.items()}
    assert xs['990101'] < xs['990102']
    # Each placeholder stands where its fragment will be.
    assert {key[0]: item.pos().x() for key, item in env.win._placeholder_items.items()} == xs
    pwh.finish_loads()
    pwh.pump()
    placed = {key[0]: item.pos().x() for key, item in env.win._fragment_items.items()}
    assert placed['990101'] < placed['990102']


def test_an_open_saved_puzzle_is_left_as_saved(env):
    doc_id = env.save(pwh.fragments('99030'), title='saved')
    env.open(doc_id)
    env.answer = SB.Discard
    assert env.win.start_new_with(DESK) is True
    # The saved puzzle is still there, whole; the canvas is a new, unsaved one.
    assert len(env.svc.load_document(doc_id).fragments) == 2
    assert env.win._current_doc_id is None
