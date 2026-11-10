# -*- coding: utf-8 -*-
"""Desktop letter-level Composition Search keeps EVERY manuscript it finds
(owner ruling 2026-10-17; `passage_witnesses.DESKTOP_GROUP_CAP`).

Until then every run kept at most 200 manuscripts -- the public API's group
cap -- and the window never said so: a single search, a batch of witnesses, a
run with conditions, the All merge, and the re-publish after a witness is
removed. Measured on the real index, 6 of 11 common texts were cut (Birkat
Hamazon: 867 found, 200 kept).

Each test drives the REAL dispatch (`GenizahGUI.run_composition` on a mock
window, as tests/test_desktop_variant_prefix_level.py does) or the REAL
worker / window method with 250 manuscripts, one past-the-cap number, and
asserts all 250 arrive. Qt-free: no QApplication is constructed.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import genizah_app
import desktop.passage_lifecycle as pl
import desktop.passage_local as pll
from desktop import passage_witnesses as pw
from shared import compound_query as cq
from shared.compound_query import Condition
from shared.parallels_service import (PARALLELS_GROUP_CAP,
                                      _cap_main_results_by_group)
from shared.passage_parallels import _RegexSysIdParser
from shared.sys_id_patterns import ANY_SYS_ID_RE
from tests.test_compound_worker import _Searcher as _CompoundSearcher
from tests.test_compound_worker import _worker as _compound_worker
from tests.test_desktop_multi_witness import _StubSearcher
from tests.test_desktop_multi_witness import _worker as _batch_worker
from tests.test_passage_local import _make_full_header

APP = genizah_app.GenizahGUI
SEED = pw.WITNESS_SEED_ID
TEXT = 'the text of the work being searched'
N = 250            # manuscripts found: past the API's 200


def _header(i, prefix='99'):
    return f'{prefix}{i:016d}_IE1_P000001_FL1'


def _rows(ids, prefix='99'):
    return [{'raw_header': _header(i, prefix), 'uid': _header(i, prefix),
             'score': 1000.0 - i} for i in ids]


def _manuscripts(rows):
    return len({ANY_SYS_ID_RE.search(r['raw_header']).group(1)
                for r in rows})


def _dispatch(monkeypatch, *, scope='genizah', witnesses=False,
              conditions=()):
    """`run_composition` up to the worker it starts, letter-level method. The
    three worker classes are recorders; everything else is the real code."""
    w = MagicMock()
    w._run_seq = 0
    w._pre_search_filters_blocked.return_value = False
    w.comp_text_area.toPlainText.return_value = TEXT
    w.comp_title_input.text.return_value = 'title'
    w.comp_mode_combo.currentIndex.return_value = 0
    w.boundary_mode_combo.currentData.return_value = 'full'
    w.comp_corpus_scope_combo.currentData.return_value = scope
    w.btn_lab_mode_toggle_comp.isChecked.return_value = False
    w._comp_method.return_value = 'passage'
    w._passage_disabled_reason_now.return_value = None
    w._comp_passage_axis.side_effect = {
        'width': 'widest-40', 'length': 'normal', 'depth': 'normal'}.get
    w._comp_compound_gate.return_value = (
        list(conditions), [(SEED, 'Your text', TEXT)], 'fp')
    w._comp_has_witnesses.return_value = witnesses
    w._comp_witness_state.return_value = pw.WitnessSet()
    w._comp_witness_prior_rows.return_value = ({}, {})
    threads = {}
    for key, name in (('single', 'CompositionThread'),
                      ('batch', 'MultiWitnessCompositionThread'),
                      ('compound', 'CompoundCompositionThread')):
        threads[key] = MagicMock()
        monkeypatch.setattr(genizah_app, name, threads[key])
    APP.run_composition(w)
    started = [k for k, t in threads.items() if t.call_count]
    assert len(started) == 1, started
    return threads[started[0]].call_args


class _Index:
    """One index's PassageSearcher, with the searcher's own cap rule: no
    `render_cap` means its PARALLELS_GROUP_CAP default, 0 means none, and
    `truncated_to_200` reports the main bucket."""

    def __init__(self, rows, render_cap):
        self.rows = rows
        self.cap = PARALLELS_GROUP_CAP if render_cap is None else render_cap

    def search_composition_logic(self, *_a, **_k):
        rows = [dict(r) for r in self.rows]
        cut = False
        if self.cap and self.cap > 0:
            rows, cut = _cap_main_results_by_group(
                rows, _RegexSysIdParser(ANY_SYS_ID_RE), cap=self.cap)
        return {'main': rows, 'filtered': [], 'truncated_to_200': cut,
                'dropped_text_lookup_failures': 0,
                'duplicate_photography_demoted': 0, 'query_report': {}}


def _indexes(monkeypatch, genizah=(), local=()):
    monkeypatch.setattr(
        pl, 'get_passage_searcher',
        lambda *a, slot=pl.SLOT_GENIZAH, render_cap=None, **k: _Index(
            list(local) if slot == pl.SLOT_LOCAL else list(genizah),
            render_cap))
    monkeypatch.setattr(pll.LocalPageTextFetcher, 'shelfmark',
                        lambda self, h: 'a.txt')


def test_a_single_search_keeps_every_manuscript(monkeypatch):
    call = _dispatch(monkeypatch)
    searcher = call.args[0]
    _indexes(monkeypatch, genizah=_rows(range(N)))
    out = searcher.search_composition_logic(TEXT, corpus_scope='genizah')
    assert _manuscripts(out['main']) == N
    assert out['truncated_to_200'] is False


def test_an_all_search_keeps_every_manuscript_of_both_indexes(monkeypatch):
    """Each index under 200, the merge over it: the merge cap is the
    router's, read from the cap the dispatch asked for."""
    call = _dispatch(monkeypatch, scope='all')
    searcher = call.args[0]
    local = [{'raw_header': _make_full_header(f'97{i:016d}', 1, 1),
              'score': 999.5 - i} for i in range(150)]
    _indexes(monkeypatch, genizah=_rows(range(150)), local=local)
    out = searcher.search_composition_logic(TEXT, corpus_scope='all')
    assert _manuscripts(out['main']) == 300
    assert out['truncated_to_200'] is False


def test_a_batch_of_witnesses_keeps_every_fused_manuscript(monkeypatch):
    """Two witnesses, 150 manuscripts each, 50 in common: 250 fused."""
    call = _dispatch(monkeypatch, witnesses=True)
    assert call.args[0]._render_cap == 0, 'each witness must come back whole'
    s = _StubSearcher({'one': {'main': _rows(range(0, 150)), 'filtered': []},
                       'two': {'main': _rows(range(100, 250)), 'filtered': []}})
    w = _batch_worker([('w1', 'A', 'one'), ('w2', 'B', 'two')], s,
                      cap=call.kwargs.get('group_cap'))
    w.run()
    [payload] = w.scan_finished_signal.calls
    assert _manuscripts(payload['main']) == N
    assert payload['truncated_to_200'] is False


def test_a_search_with_conditions_keeps_every_judged_manuscript(monkeypatch):
    """A must-not condition that matches nothing keeps every page in Main;
    the cap that followed the judgement is gone with the rest."""
    cond = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('x' * 50,))
    call = _dispatch(monkeypatch, conditions=[cond])
    assert call.args[0]._render_cap == 0
    s = _CompoundSearcher(main={TEXT: {'main': _rows(range(N)),
                                       'filtered': []}})
    w = _compound_worker([(SEED, 'Your text', TEXT)], [cond], s)
    w.group_cap = call.kwargs.get('group_cap')
    w.run()
    assert w.error_signal.calls == [], w.error_signal.calls
    [payload] = w.scan_finished_signal.calls
    assert _manuscripts(payload['main']) == N
    assert payload['truncated_to_200'] is False


def test_removing_a_witness_republishes_every_manuscript():
    """The re-fuse after a removal publishes the cached rows again; at the
    helper's default it cut a full list back to 200."""
    published = []
    state = pw.WitnessSet()
    state.rows = {SEED: _rows(range(N))}

    w = SimpleNamespace(
        _comp_compound=None, comp_conditions=[],
        _comp_witness_state=lambda: state,
        _refresh_witness_panel=lambda: None,
        _has_comp_results=lambda: True,
        _schedule_session_save=lambda: None,
        _witness_notify=lambda m: published.append(('note', m)),
        on_comp_scan_finished=published.append)
    APP._after_witness_removal(w)
    [payload] = published
    assert _manuscripts(payload['main']) == N
