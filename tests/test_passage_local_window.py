# -*- coding: utf-8 -*-
"""Letter-level search for My Library: the window's and the My Library tab's
glue -- the staleness notice after a scan, the build preflight off the UI
thread, and the My Library readiness the gate reads.

Qt-free, in the style of `tests/test_desktop_passage_gate.py`: the methods
under test are bound UNBOUND onto stub classes, so nothing here needs a
QApplication. The two QThread workers are run with `.run()` on this thread.
"""
from __future__ import annotations

import inspect
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import desktop.passage_lifecycle as pl        # noqa: E402
import desktop.passage_local as pll           # noqa: E402
import genizah_app                            # noqa: E402

APP = genizah_app.GenizahGUI


class _Label:
    def __init__(self):
        self.text = None

    def setText(self, t):
        self.text = t


class _Thread:
    def __init__(self, running):
        self._running = running

    def isRunning(self):
        return self._running


# ---------------------------------------------------------------------------
# The staleness notice after every scan (owner, 2026-10-16).
# ---------------------------------------------------------------------------

class _FreshWin:
    _on_my_library_changed = APP._on_my_library_changed
    _on_local_freshness = APP._on_local_freshness

    def __init__(self):
        self.status_label = _Label()
        self.checks = 0

    def _start_local_freshness_check(self):
        self.checks += 1


def test_a_library_change_rechecks_the_index():
    w = _FreshWin()
    w._on_my_library_changed()
    assert w.checks == 1


@pytest.mark.parametrize('attr', ['_close_pending',
                                  '_local_passage_build_in_flight'])
def test_no_recheck_while_closing_or_building(attr):
    """A build in flight is checked when it installs instead; it never
    rebuilds anything either way."""
    w = _FreshWin()
    setattr(w, attr, True)
    w._on_my_library_changed()
    assert w.checks == 0


def test_a_stale_answer_says_so():
    w = _FreshWin()
    w._on_local_freshness(pl.STALE)
    assert w.status_label.text and 'Settings' in w.status_label.text
    w2 = _FreshWin()
    w2._on_local_freshness(pl.FRESH)
    assert w2.status_label.text is None


def test_a_change_during_a_check_runs_it_again_before_answering():
    """The running check may have read the library before the change: its
    answer is not shown, and a fresh check runs instead."""
    class _W(_FreshWin):
        _start_local_freshness_check = APP._start_local_freshness_check

    w = _W()
    w.local_passage_freshness_thread = _Thread(running=True)
    w._start_local_freshness_check()
    assert w._local_freshness_recheck is True

    restarted = []
    w._start_local_freshness_check = lambda: restarted.append(1)
    w._on_local_freshness(pl.FRESH)
    assert restarted == [1] and w._local_freshness_recheck is False
    assert w.status_label.text is None


def test_the_tab_reports_every_change_and_never_raises():
    from desktop.my_library_tab import MyLibraryTab

    class _Parent:
        calls = 0

        def _on_my_library_changed(self):
            _Parent.calls += 1

    class _Tab:
        _notify_library_changed = MyLibraryTab._notify_library_changed

    t = _Tab()
    t._parent_window = _Parent()
    t._notify_library_changed()
    assert _Parent.calls == 1

    class _Broken:
        def _on_my_library_changed(self):
            raise RuntimeError('boom')
    t._parent_window = _Broken()
    t._notify_library_changed()                      # logged, not raised
    t._parent_window = None
    t._notify_library_changed()                      # no window: no-op
    # Both ways the library changes report it: the end of every scan, and a
    # folder removal (synchronous, so it never reaches the scan's end).
    for name in ('_on_worker_finished', '_on_remove_folder_clicked'):
        assert 'self._notify_library_changed()' in inspect.getsource(
            getattr(MyLibraryTab, name)), name


def test_a_finished_build_is_checked_too():
    src = inspect.getsource(APP._on_local_passage_build_finished)
    assert 'self._start_local_freshness_check()' in src


# ---------------------------------------------------------------------------
# The build preflight runs on a worker.
# ---------------------------------------------------------------------------

def test_the_preflight_worker_counts_and_sizes(monkeypatch):
    from desktop.passage_workers import LocalPassagePreflightThread
    import shared.passage_local_corpus as plc
    monkeypatch.setattr(os.path, 'exists', lambda p: True)
    monkeypatch.setattr(plc, 'count_local_pages', lambda p: 7)
    monkeypatch.setattr(pll, 'estimate_local_build_bytes',
                        lambda p: 3 * 2 ** 33)       # past 2**31 on purpose
    got = []
    t = LocalPassagePreflightThread('db')
    t.result_signal.connect(lambda n, b: got.append((n, b)))
    t.run()
    assert got == [(7, 3 * 2 ** 33)]


def test_a_failed_estimate_still_answers(monkeypatch):
    from desktop.passage_workers import LocalPassagePreflightThread
    import shared.passage_local_corpus as plc
    monkeypatch.setattr(os.path, 'exists', lambda p: True)
    monkeypatch.setattr(plc, 'count_local_pages', lambda p: 7)

    def _boom(p):
        raise OSError('locked')
    monkeypatch.setattr(pll, 'estimate_local_build_bytes', _boom)
    got = []
    t = LocalPassagePreflightThread('db')
    t.result_signal.connect(lambda n, b: got.append((n, b)))
    t.run()
    assert got == [(7, 0)]


def test_the_build_command_no_longer_reads_the_library_on_the_ui_thread():
    src = inspect.getsource(APP.run_local_passage_index_build)
    assert 'count_local_pages' not in src
    assert 'estimate_local_build_bytes' not in src
    assert 'LocalPassagePreflightThread' in src


class _PreWin:
    _on_local_passage_preflight = APP._on_local_passage_preflight

    def __init__(self):
        self.status_label = _Label()
        self.started = []

    def _revalidate_comp_method(self):
        pass

    def _start_local_passage_build_worker(self, db_path):
        self.started.append(db_path)


def test_an_empty_library_is_told_so_and_nothing_starts(monkeypatch):
    shown = []
    monkeypatch.setattr(genizah_app.QMessageBox, 'information',
                        lambda *a, **k: shown.append(a[1]))
    w = _PreWin()
    w._on_local_passage_preflight('db', 0, 0, True)
    assert shown and w.started == []


def test_a_counted_library_goes_on_to_the_build(monkeypatch, tmp_path):
    monkeypatch.setattr(pll, 'local_passage_root', lambda: str(tmp_path))
    w = _PreWin()
    w._on_local_passage_preflight('db', 5, 0, False)
    assert w.started == ['db']
    assert w._local_passage_build_in_flight is True


def test_an_answer_after_close_or_during_a_build_does_nothing():
    for attr in ('_close_pending', '_local_passage_build_in_flight'):
        w = _PreWin()
        setattr(w, attr, True)
        w._on_local_passage_preflight('db', 5, 0, False)
        assert w.started == []


# ---------------------------------------------------------------------------
# My Library results as witnesses (owner, 2026-10-16).
# ---------------------------------------------------------------------------

G_SYS = '990051620920205171'
L_SYS = '970000000100000001'
L_SYS_2 = '970000000100000002'
G_HEAD = f'{G_SYS}_IE104538562_P5_FL104538575'
G_UID = 'IE104538562_P5_FL104538575'        # what `_derive_uid` keeps


def _l_head(sys_id, page=3, file_id=7):
    from shared.local_indexer import _make_full_header
    return _make_full_header(sys_id, page, file_id)


def _l_uid(sys_id, page=3):
    from shared.local_indexer import _make_uid
    return _make_uid(sys_id, page)


def test_a_row_is_read_in_either_namespace():
    assert pll.row_sys_id({'raw_header': G_HEAD}) == G_SYS
    assert pll.row_sys_id({'raw_header': _l_head(L_SYS)}) == L_SYS
    assert pll.row_sys_id({}) is None
    assert pll.is_local_sys_id(L_SYS) and not pll.is_local_sys_id(G_SYS)


class _CheckWin:
    _checked_comp_sys_ids = APP._checked_comp_sys_ids

    def __init__(self, uids):
        self._uids = uids

    def _collect_checked_comp_page_uids(self):
        return sorted(self._uids)


def test_checked_results_of_both_indexes_are_promoted_in_display_order():
    """A Genizah page's uid holds no sys_id (`IE.._P.._FL..`), so the id is
    taken from the row the uid belongs to -- reading it out of the uid found
    nothing, and a checked Genizah result was never promoted. A My Library
    result is promoted too, not skipped."""
    rows = [{'raw_header': _l_head(L_SYS), 'uid': _l_uid(L_SYS)},
            {'raw_header': G_HEAD, 'uid': G_UID},
            {'raw_header': _l_head(L_SYS_2), 'uid': _l_uid(L_SYS_2)}]
    w = _CheckWin({G_UID, _l_uid(L_SYS)})
    assert w._checked_comp_sys_ids(rows) == [L_SYS, G_SYS]
    # The Genizah half alone: the defect that predates My Library.
    assert _CheckWin({G_UID})._checked_comp_sys_ids([rows[1]]) == [G_SYS]
    # Nothing checked, nothing promoted.
    assert _CheckWin(set())._checked_comp_sys_ids(rows) == []


def test_a_genizah_uid_is_never_read_as_an_id():
    """Only a My Library uid is read directly; a Genizah uid no row lists is
    not guessed at."""
    w = _CheckWin({'IE9712345678_P1_FL1'})
    assert w._checked_comp_sys_ids(
        [{'raw_header': _l_head(L_SYS), 'uid': _l_uid(L_SYS)}]) == []


class _Engine2:
    """A main engine and a LOCAL Tantivy stand-in, each answering only its
    own headers."""

    def __init__(self, local_pages, main_pages):
        from tests.test_passage_local import _FakeLocalIndex
        self.local_index = _FakeLocalIndex(local_pages)
        self.local_searcher = self.local_index
        self._main = main_pages

    def get_full_text_by_header(self, header):
        return self._main.get(header)

    def get_full_manuscript(self, sys_id):
        return [{'text': 'whole manuscript'}]


def test_witness_text_comes_from_each_pages_own_index():
    page = 'א ב ג ד ה ו'
    eng = _Engine2({_l_head(L_SYS): (page, 'a.pdf')}, {G_HEAD: 'genizah text'})
    f = pll.WitnessTextFetcher(eng)
    assert f.get_full_text_by_header(_l_head(L_SYS)) == page
    assert f.get_full_text_by_header(G_HEAD) == 'genizah text'
    # No whole-file fallback for a My Library file; Genizah keeps its own.
    assert f.get_full_manuscript(L_SYS) == []
    assert f.get_full_manuscript(G_SYS) == [{'text': 'whole manuscript'}]


class _PromoteWin:
    _promote_sys_ids = APP._promote_sys_ids
    _witness_label = APP._witness_label
    meta_mgr = None                  # a Genizah lookup would raise -> caught

    def __init__(self, engine):
        from desktop import passage_witnesses
        self.searcher = engine
        self._state = passage_witnesses.WitnessSet()
        self.notes, self.searched = [], 0

    def _comp_witness_state(self):
        return self._state

    def _comp_seed_text(self):
        return 'השמים מספרים כבוד אל ומעשה ידיו'

    def _witness_notify(self, msg):
        self.notes.append(msg)

    def _refresh_witness_panel(self):
        pass

    def _search_pending_witnesses(self):
        self.searched += 1

    def _lookup_local_filepath(self, sid):
        return None


def test_a_my_library_result_becomes_a_witness_named_by_its_file():
    page = 'ויאמר ראובן אל אחיו הלא אמרתי אליכם לאמר אל תחטאו בילד'
    head = _l_head(L_SYS)
    eng = _Engine2({head: (page, 'a.pdf')}, {})
    w = _PromoteWin(eng)
    rows = [{'raw_header': head, 'uid': _l_uid(L_SYS), 'shelfmark': 'a.pdf'}]
    assert w._promote_sys_ids([L_SYS], rows) == 1
    entry = w._state.entries[0]
    assert (entry.sys_id, entry.label, entry.kind) == (L_SYS, 'a.pdf',
                                                       'manuscript')
    assert entry.text == page and entry.headers == [head]
    assert w.searched == 1 and w.notes == []


def test_a_my_library_page_that_no_longer_resolves_is_named_not_dropped():
    head = _l_head(L_SYS)
    w = _PromoteWin(_Engine2({}, {}))
    rows = [{'raw_header': head, 'uid': _l_uid(L_SYS), 'shelfmark': 'a.pdf'}]
    assert w._promote_sys_ids([L_SYS], rows) == 0
    assert w._state.entries == [] and w.searched == 0
    assert any('a.pdf' in n for n in w.notes)


def test_auto_expand_and_rehydrate_read_both_namespaces():
    for name in ('_advance_auto_expand', '_rehydrate_witness_texts',
                 '_promote_sys_ids'):
        src = inspect.getsource(getattr(APP, name))
        assert 'passage_witness_source.witness_sys_id(' not in src, name
    for name in ('_rehydrate_witness_texts', '_promote_sys_ids'):
        src = inspect.getsource(getattr(APP, name))
        assert 'passage_local.WitnessTextFetcher(' in src, name
        assert 'sys_id_of=passage_local.row_sys_id' in src, name


def test_the_fusion_cap_counts_each_my_library_file_as_its_own_group():
    """With the corpus parser every My Library row fell into ONE 'unknown'
    group, so a cap of 1 kept the whole library. Each file is its own group
    now, like each Genizah manuscript."""
    from desktop import passage_witnesses as pw
    from shared.passage_witness_source import WITNESS_SEED_ID
    a, b = _l_head(L_SYS), _l_head(L_SYS_2)
    st = pw.fusion_set(
        [(WITNESS_SEED_ID, 'Your text'), ('w1', 'A')],
        {WITNESS_SEED_ID: [{'raw_header': a, 'score': 90},
                           {'raw_header': b, 'score': 40}],
         'w1': [{'raw_header': a, 'score': 80},
                {'raw_header': b, 'score': 30}]})
    main, _filtered, truncated = pw.fuse_and_cap(st, cap=1)
    assert {r['raw_header'] for r in main} == {a}
    assert truncated is True


# ---------------------------------------------------------------------------
# A multi-witness batch pins every index its scope searches.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('scope, g, l_pin', [
    ('genizah', True, False), ('local', False, True), ('all', True, True)])
def test_scope_pins(scope, g, l_pin):
    pins = pll.scope_pins(scope)
    assert (pins['pinned_generation'] is not None) is g
    assert (pins['pinned_local_generation'] is not None) is l_pin


def test_a_my_library_swap_voids_a_batch_that_searches_it(monkeypatch):
    from desktop.gui_threads import MultiWitnessCompositionThread
    w = MultiWitnessCompositionThread(
        object(), [], corpus_scope='local', **pll.scope_pins('local'))
    w._abort_if_generation_moved()                    # nothing moved yet
    monkeypatch.setattr(pl._SLOTS[pl.SLOT_LOCAL], 'generation',
                        pl.current_state_generation(pl.SLOT_LOCAL) + 1)
    with pytest.raises(pl.PassageIndexReplaced):
        w._abort_if_generation_moved()


def test_a_my_library_swap_leaves_a_genizah_batch_alone(monkeypatch):
    from desktop.gui_threads import MultiWitnessCompositionThread
    w = MultiWitnessCompositionThread(
        object(), [], corpus_scope='genizah', **pll.scope_pins('genizah'))
    monkeypatch.setattr(pl._SLOTS[pl.SLOT_LOCAL], 'generation',
                        pl.current_state_generation(pl.SLOT_LOCAL) + 1)
    w._abort_if_generation_moved()


def test_dispatch_pins_by_scope():
    src = inspect.getsource(APP.run_composition)
    assert '**passage_local.scope_pins(_comp_scope)' in src


# ---------------------------------------------------------------------------
# The gate: My Library readiness includes the tab's own "searchable".
# ---------------------------------------------------------------------------

class _Combo:
    def __init__(self, data):
        self._data = data

    def currentData(self):
        return self._data


class _GateWin:
    _passage_disabled_reason_now = APP._passage_disabled_reason_now

    def __init__(self, engine, scope='local'):
        self.searcher = engine
        self.comp_corpus_scope_combo = _Combo(scope)
        self._comp_corpus_scope = scope


class _Tab:
    def __init__(self, searchable):
        self.is_searchable = searchable


class _Engine:
    def __init__(self, tab=None):
        self.index = self.searcher = object()
        self.local_index = self.local_searcher = object()
        if tab is not None:
            self._my_library_tab_ref = lambda: tab


@pytest.mark.parametrize('tab, expected', [
    (None, None),
    (_Tab(True), None),
    (_Tab(False), pll.REASON_LOCAL_INDEX_MISSING),
])
def test_my_library_mid_rebuild_is_not_ready(monkeypatch, tab, expected):
    """While My Library rebuilds its own index after a schema change the
    tab says it is not searchable, and chunk search skips LOCAL; letter-level
    must refuse too, since every row reads its text from that index."""
    monkeypatch.setattr(pll, 'local_passage_available', lambda: True)
    w = _GateWin(_Engine(tab))
    assert APP._passage_disabled_reason_now(w) == expected
