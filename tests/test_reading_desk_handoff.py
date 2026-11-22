# -*- coding: utf-8 -*-
"""web/reading_desk_handoff.py: several ticked in the header's or the home page's shelf picker, kept
once for Browse (the render tests drive the pickers and Browse: tests/render_smoke/
test_shelf_pick_hosts_render_smoke.py, test_shelf_pick_multi_hosts_render_smoke.py)."""
from __future__ import annotations

import pytest

from web import reading_desk_handoff as handoff

A, B = '990000000000000101', '990000000000000102'


@pytest.fixture
def kept(monkeypatch):
    box = {}
    monkeypatch.setattr(handoff, 'safe_user_set', lambda key, value: box.__setitem__(key, value) or True)
    monkeypatch.setattr(handoff, 'safe_user_pop', lambda key, default=None: box.pop(key, default))
    return box


def _entry(sys_id, placement=None):
    return {'sys_id': sys_id, 'shelfmark': f'Frag {sys_id[-3:]}', 'placement_id': placement}


def test_kept_once_each_in_order_and_browse_at_the_first(kept):
    url = handoff.stage_for_reading_desk([_entry(A, 'inv:1:x'), _entry(B), _entry(A), _entry('970000000000000001')])
    assert url == f'/browse?sys_id={A}&place=inv%3A1%3Ax&desk=shelf'
    assert [e['sys_id'] for e in kept[handoff.HANDOFF_KEY]['entries']] == [A, B]
    assert [e['sys_id'] for e in handoff.take_reading_desk_handoff(A)] == [A, B]
    assert handoff.take_reading_desk_handoff(A) == []                   # taken once


def test_an_old_list_is_not_taken(kept, monkeypatch):
    handoff.stage_for_reading_desk([_entry(A), _entry(B)])
    now = handoff.time.time()
    monkeypatch.setattr(handoff.time, 'time', lambda: now + handoff.HANDOFF_MAX_AGE + 1)
    assert handoff.take_reading_desk_handoff(A) == []
    assert handoff.HANDOFF_KEY not in kept                              # dropped all the same


def test_nothing_to_keep_or_a_refused_write_opens_nothing(monkeypatch):
    monkeypatch.setattr(handoff, 'safe_user_set', lambda key, value: False)
    assert handoff.stage_for_reading_desk([_entry(A)]) is None
    assert handoff.stage_for_reading_desk([_entry('970000000000000001')]) is None
