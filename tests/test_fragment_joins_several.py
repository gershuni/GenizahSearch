# -*- coding: utf-8 -*-
"""``create_fragment_joins``: one fragment joined to several in ONE insert (shelf tree picker, DX15:
a join may hold many fragments, each joined to this one). All are made or none, so the add-join form
never leaves half a join behind; the rows are the ones ``create_fragment_join`` makes, one by one."""
from __future__ import annotations

from types import SimpleNamespace

from web import supabase_client as sc


class _Table:
    def __init__(self, sink, answer):
        self._sink, self._answer = sink, answer

    def insert(self, data):
        self._sink.append(data)
        return self

    def execute(self):
        if isinstance(self._answer, Exception):
            raise self._answer
        return SimpleNamespace(data=self._answer(self._sink[-1]))


class _Client:
    def __init__(self, answer=lambda rows: rows if isinstance(rows, list) else [rows]):
        self.inserts = []
        self._answer = answer

    def table(self, name):
        assert name == 'fragment_joins'
        return _Table(self.inserts, self._answer)


def test_all_rows_go_in_one_insert_as_the_single_join_makes_them(monkeypatch):
    client = _Client()
    monkeypatch.setattr(sc, 'get_user_client', lambda: client)
    result = sc.create_fragment_joins('u1', '990000000000000111', 'T-S A1.1',
                                      [('990000000000000201', 'T-S 2.1'), ('990000000000000202', 'T-S 2.2')],
                                      join_type='physical_join', notes='same hand')
    assert result['success'] is True and len(result['joins']) == 2
    [rows] = client.inserts                                       # one request
    assert [(r['fragment_b_sys_id'], r['fragment_b_shelfmark']) for r in rows] == [
        ('990000000000000201', 'T-S 2.1'), ('990000000000000202', 'T-S 2.2')]
    assert {r['fragment_a_sys_id'] for r in rows} == {'990000000000000111'}
    assert {r['join_type'] for r in rows} == {'physical'}          # the UI value mapped, as for one
    single = _Client()
    monkeypatch.setattr(sc, 'get_user_client', lambda: single)
    sc.create_fragment_join('u1', '990000000000000111', 'T-S A1.1', '990000000000000201', 'T-S 2.1',
                            join_type='physical_join', notes='same hand')
    assert single.inserts[0] == rows[0]


def test_a_refused_or_short_insert_is_an_error_never_a_success(monkeypatch):
    monkeypatch.setattr(sc, 'get_user_client', lambda: _Client(answer=RuntimeError('RLS')))
    assert sc.create_fragment_joins('u1', 'a', 'A', [('b', 'B'), ('c', 'C')]) == {'error': 'RLS'}
    monkeypatch.setattr(sc, 'get_user_client', lambda: _Client(answer=lambda rows: rows[:1]))
    assert 'error' in sc.create_fragment_joins('u1', 'a', 'A', [('b', 'B'), ('c', 'C')])
    assert 'error' in sc.create_fragment_joins('u1', 'a', 'A', [])
