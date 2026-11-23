# -*- coding: utf-8 -*-
"""The desktop Reading Desk across a restart: an open desk is kept in the session
file (genizah_app.py::_save_session, ``browse_reading_desk``; shared/reading_desk_model.py) and the
restore reopens it over the manuscript Browse restores (_restore_session ->
_restore_browse_reading_desk -> _browse_enter_reading_desk): its manuscripts in their order, the PGP
document, the text chosen for each and the image side's choices. A manuscript that no longer
resolves is left out quietly; a session with no desk restores one manuscript, as before.

The session file is never touched: the save is caught, the load answers the saved state, the
restore runs on a stand-in host whose own restore methods are GenizahGUI's.
"""
import json
import types
from unittest import mock

import pytest

pytestmark = pytest.mark.gui  # QApplication.processEvents runs in the restore: gui bucket only

from PyQt6.QtWidgets import QApplication  # noqa: E402

import genizah_app  # noqa: E402
import shared.session_persistence as session_persistence  # noqa: E402
from shared.reading_desk_model import (  # noqa: E402
    ReadingDeskEntry, ReadingDeskState, desk_from_session, desk_session_state,
)

APP = QApplication.instance() or QApplication([])
GUI = genizah_app.GenizahGUI

A, B, C = '990051243280205171', '990051243290205171', '990051243300205171'
SHELF = {A: 'T-S 13J1.1', B: 'T-S 13J1.2', C: 'T-S 13J1.3'}


def _open_desk():
    """A desk of three as the reader left it: moved into the order C, A, B, a translation chosen
    for A, one image at a time with C's verso on show, a puzzle image ticked."""
    state = ReadingDeskState(pgpid=1234, image_view='one', image_shown=f'{C}:1', puzzles_shown='77')
    for order, sid in enumerate((C, A, B), start=1):
        state.entries.append(ReadingDeskEntry(sys_id=sid, shelfmark=SHELF[sid], sequence_order=order,
                                              pages=[{'p_num': 1, 'text': 'x'}]))
    state.entries[1].chosen = 'pgp:52'
    return state


def _saved(monkeypatch, **host):
    """The session file ``_save_session`` writes for a stand-in host, read back as JSON."""
    written = []
    monkeypatch.setattr(genizah_app, 'load_app_config', lambda: {'restore_mode': 'always'})
    monkeypatch.setattr(session_persistence, 'save_session_state', lambda s: written.append(s) or True)
    GUI._save_session(types.SimpleNamespace(_is_browsing_local=lambda: False, **host))
    assert len(written) == 1, 'the session was not written'
    return json.loads(json.dumps(written[0]))


class _Timers:
    """QTimer.singleShot as the restore uses it: the callbacks are kept and run by the test."""
    def __init__(self):
        self.calls = []

    def singleShot(self, _ms, fn):
        self.calls.append(fn)

    def run(self):
        while self.calls:
            self.calls.pop(0)()


def _restore(monkeypatch, state, known=(A, B, C), busy=None, before_timers=None, queue=False):
    """``_restore_session`` on a stand-in host: the Browse and desk restore are GenizahGUI's own
    methods, everything else a recorder. ``known``: the manuscripts the catalogue and index still
    have. ``busy(host)``: the startup's background loading is still running (asked at each look;
    by default it has finished). ``before_timers(host)``: run once ``_restore_session`` has returned,
    before its deferred Browse restore. ``queue``: the loading is the startup queue's
    (desktop/startup_work.py), busy until its first phase is done; else the old loaders'. Answers the
    host and what the desk held when it was first drawn."""
    monkeypatch.setattr(genizah_app, 'load_app_config', lambda: {'restore_mode': 'always'})
    monkeypatch.setattr(session_persistence, 'load_session_state', lambda: state)
    monkeypatch.setattr(session_persistence, 'get_interrupted_search', lambda: None)
    timers = _Timers()
    monkeypatch.setattr(genizah_app, 'QTimer', timers)
    host = mock.MagicMock()
    host._migrate_saved_pre_search_filters.return_value = ({}, False)
    host._passage_snapshot_must_wait.return_value = False
    host._display_restored_comp_snapshot.return_value = False
    host._restore_notices = None
    host.tabs.count.return_value = 8
    host.comp_corpus_scope_combo.findData.return_value = -1
    host.browse_reading_desk_active = False
    host.current_browse_sid = None
    host.current_browse_volume_ie = None
    host._browse_rd_image_scroll = object()  # the image pane exists: none is built here
    host._browse_rd_preloaded = None
    host._browse_rd_restore_pending = None
    host._browse_enrich_gen = 0          # moved on by every load Browse starts, as the app's is
    host._start_browse_enrichment.side_effect = lambda *a, **k: _browse_to(host, host.current_browse_sid)
    host.searcher._fl_id_index_building = False
    host.looks = 0

    def finished():
        host.looks += 1
        return not (busy and busy(host))
    if queue:
        class _Queue:                          # the steps the restore waits on are done; phase 1 asked
            browse_map_loaded = browse_resolver_built = True

            @property
            def phase1_done(self):
                return finished()
        host._startup_work = _Queue()
        host.meta_mgr.background_loading_finished.return_value = True   # never started: True
    else:
        host._startup_work = None
        host.meta_mgr.background_loading_finished.side_effect = finished
    reads = host.reads = []

    def full_manuscript(sid):
        reads.append(sid)
        return [{'p_num': 1, 'text': f'text of {sid}', 'full_header': '', 'fl_id': ''}] if sid in known else []
    host.searcher.get_full_manuscript.side_effect = full_manuscript
    host.meta_mgr.get_meta_for_id.side_effect = lambda sid: (SHELF[sid], '') if sid in known else ('Unknown', '')
    drawn = []

    def render():
        st = host.browse_reading_desk_state
        drawn.append({'ids': [e.sys_id for e in st.entries], 'chosen': [e.chosen for e in st.entries],
                      'order': [e.sequence_order for e in st.entries], 'pgpid': st.pgpid,
                      'view': st.image_view, 'shown': st.image_shown, 'puzzles': st.puzzles_shown,
                      'texts': [e.pages[0]['text'] if e.pages else '' for e in st.entries]})
    host._browse_rd_render = render
    for name in ('_restore_browse_reading_desk', '_browse_enter_reading_desk', '_browse_rd_full_manuscript'):
        if hasattr(GUI, name):  # (a build without the desk's restore runs the rest as it is)
            setattr(host, name, types.MethodType(getattr(GUI, name), host))
    GUI._restore_session(host)
    if before_timers:
        before_timers(host)
    timers.run()
    return host, drawn


def test_an_open_desk_comes_back_after_a_restart(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk(),
                   current_browse_sid=A)
    kept = state['browse_reading_desk']
    assert [e['sys_id'] for e in kept['entries']] == [C, A, B]
    assert kept['pgpid'] == 1234 and kept['image_view'] == 'one'
    # Browse's own fields, as the app had them: the manuscript shown under the desk.
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    host, drawn = _restore(monkeypatch, state)
    assert host.current_browse_sid == A
    host.browse_load_page.assert_called_once()                 # Browse's manuscript first
    assert host.browse_reading_desk_active is True             # then the desk over it
    assert drawn[0] == {'ids': [C, A, B], 'chosen': ['', 'pgp:52', ''], 'order': [1, 2, 3], 'pgpid': 1234,
                        'view': 'one', 'shown': f'{C}:1', 'puzzles': '77',
                        'texts': [f'text of {C}', f'text of {A}', f'text of {B}']}
    assert sorted(host.reads) == sorted([A, B, C])             # each manuscript's pages read once
    assert host._browse_rd_preloaded is None


def test_a_manuscript_that_no_longer_resolves_is_left_out_quietly(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    host, drawn = _restore(monkeypatch, state, known=(A, B))
    assert drawn[0]['ids'] == [A, B] and drawn[0]['order'] == [1, 2]
    host.statusBar.return_value.showMessage.assert_called()   # "Session restored", no error dialog


def test_with_nothing_left_on_the_desk_browse_stays_as_restored(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    host, drawn = _restore(monkeypatch, state, known=(A,))
    assert drawn[0]['ids'] == [A]                              # one left: still a desk of one
    host, drawn = _restore(monkeypatch, state, known=())
    assert drawn == [] and host.browse_reading_desk_active is False
    host.browse_load_page.assert_called_once()


def test_a_session_with_no_desk_restores_one_manuscript_as_before(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=False, browse_reading_desk_state=_open_desk())
    assert state['browse_reading_desk'] is None                # a closed desk is not kept
    state['browse_shelfmark'] = {'sys_id': B, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    host, drawn = _restore(monkeypatch, state)
    assert host.current_browse_sid == B and host.browse_reading_desk_active is False
    host.browse_load_page.assert_called_once()
    assert drawn == [] and host.reads == []                    # no desk, nothing more read


def test_a_desk_kept_without_browse_fields_opens_over_its_first_manuscript(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': '', 'shelfmark': '', 'fl_id': '', 'last_field': 'shelf'}
    host, drawn = _restore(monkeypatch, state)
    assert host.current_browse_sid == C
    assert drawn[0]['ids'] == [C, A, B]


def test_a_desk_kept_alone_opens_over_its_first_manuscript_that_still_resolves(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': '', 'shelfmark': '', 'fl_id': '', 'last_field': 'shelf'}
    host, drawn = _restore(monkeypatch, state, known=(A, B))        # C, the first, is gone
    assert host.current_browse_sid == A
    host.browse_sys_input.setText.assert_called_with(A)
    assert drawn[0]['ids'] == [A, B]
    host, drawn = _restore(monkeypatch, state, known=())            # none left: nothing opens
    assert drawn == [] and host.current_browse_sid is None
    host.browse_load_page.assert_not_called()
    assert host._browse_rd_restore_pending is None


def _browse_to(host, sid):
    """Browse loads ``sid``: what the reader's Go, a list entry or previous / next does to the
    window (a new load generation, _start_browse_enrichment)."""
    host.current_browse_sid = sid
    host._browse_enrich_gen += 1


def _saved_now(monkeypatch, host):
    """What a save on ``host`` writes for the desk right now (closing the app, a background save)."""
    active = host.browse_reading_desk_active
    return _saved(monkeypatch, browse_reading_desk_active=active,
                  browse_reading_desk_state=host.browse_reading_desk_state if active else None,
                  _browse_rd_restore_pending=host._browse_rd_restore_pending,
                  _browse_enrich_gen=host._browse_enrich_gen,
                  current_browse_sid=host.current_browse_sid)['browse_reading_desk']


def test_the_desk_is_kept_from_the_moment_its_restore_is_scheduled(monkeypatch):
    """The restoring flag is cleared when _restore_session returns, before the deferred Browse
    restore runs: a save in between (closing the app, a background save) keeps the desk."""
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    seen = []
    host, drawn = _restore(monkeypatch, state, before_timers=lambda h: seen.append(_saved_now(monkeypatch, h)))
    assert seen == [state['browse_reading_desk']]
    assert drawn[0]['ids'] == [C, A, B] and host._browse_rd_restore_pending is None
    assert _saved_now(monkeypatch, host) == state['browse_reading_desk']   # the desk, now open


# -- the reader moves on before the desk opens: before the deferred restore runs, or between looks --

def _reader_desk(host):
    host._browse_enter_reading_desk([{'sys_id': B, 'shelfmark': SHELF[B], 'sequence_order': 1}])
    host.browse_reading_desk_active = False                     # and left it again


def test_a_reader_who_loads_a_manuscript_before_the_restore_runs_keeps_it(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    saved = []

    def reader(h):
        _browse_to(h, B)
        saved.append(_saved_now(monkeypatch, h))
    host, drawn = _restore(monkeypatch, state, before_timers=reader)
    assert saved == [None]                                       # closing now keeps no old desk
    assert host.current_browse_sid == B and drawn == []          # Browse stays where the reader went
    host.browse_load_page.assert_not_called()
    assert host._browse_rd_restore_pending is None


def test_a_reader_who_opens_and_closes_a_desk_before_the_restore_runs_gets_no_old_desk(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    host, drawn = _restore(monkeypatch, state, before_timers=_reader_desk)
    assert [d['ids'] for d in drawn] == [[B]]                    # the reader's desk, and only it
    assert _saved_now(monkeypatch, host) is None


# -- the desk waits for the startup's own loading (it froze the window 4.5-9 s while that ran) --

def test_the_desk_opens_once_the_startup_loading_has_finished(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    waiting = []

    def busy(host):
        if host.looks <= 3:                                   # three looks: still loading
            assert not host.browse_reading_desk_active and host.reads == []
            waiting.append(_saved_now(monkeypatch, host))      # closed while it waits: kept
            return True
        return False
    host, drawn = _restore(monkeypatch, state, busy=busy)
    assert host.looks == 4 and len(drawn) == 1 and drawn[0]['ids'] == [C, A, B]
    assert waiting == [state['browse_reading_desk']] * 3
    assert host._browse_rd_restore_pending is None


def test_the_desk_waits_for_the_startup_queues_first_phase(monkeypatch):
    # PR #111: the old loaders never start; the desk opens once phase 1 has read the catalogue.
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}

    def busy(host):
        if host.looks <= 3:
            assert not host.browse_reading_desk_active and host.reads == []
            return True
        return False
    host, drawn = _restore(monkeypatch, state, busy=busy, queue=True)
    assert host.looks == 4 and len(drawn) == 1 and drawn[0]['ids'] == [C, A, B]
    assert host._browse_rd_restore_pending is None


def test_a_reader_who_loads_another_manuscript_meanwhile_gets_no_desk(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    saved = []

    def reader(h):
        _browse_to(h, B)
        saved.append(_saved_now(monkeypatch, h))

    def busy(host):
        if host.looks == 1:                                     # between this look and the next
            genizah_app.QTimer.singleShot(0, lambda: reader(host))
        return True
    host, drawn = _restore(monkeypatch, state, busy=busy)
    assert saved == [None]                                       # closing then keeps no old desk
    assert drawn == [] and host.browse_reading_desk_active is False and host.looks == 1
    assert host._browse_rd_restore_pending is None


def test_a_reader_who_reloads_the_same_manuscript_meanwhile_gets_no_desk(monkeypatch):
    """Loaded again (Go on the same shelfmark, another volume): a new load generation."""
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}

    def busy(host):
        if host.looks == 1:
            genizah_app.QTimer.singleShot(0, lambda: _browse_to(host, A))
        return True
    host, drawn = _restore(monkeypatch, state, busy=busy)
    assert drawn == [] and _saved_now(monkeypatch, host) is None


def test_a_reader_who_opens_and_closes_a_desk_meanwhile_gets_no_old_desk(monkeypatch):
    """Between two looks the reader opens a desk of their own and leaves it again: Browse still
    shows the same manuscript and no desk is open, but the one waiting is not restored."""
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}

    def busy(host):
        if host.looks == 1:                                     # queued before the next look
            genizah_app.QTimer.singleShot(0, lambda: _reader_desk(host))
        return host.looks <= 2
    host, drawn = _restore(monkeypatch, state, busy=busy)
    assert [d['ids'] for d in drawn] == [[B]]                   # the reader's desk, and only it
    assert host.current_browse_sid == A and host._browse_rd_restore_pending is None
    assert _saved_now(monkeypatch, host) is None                # nor kept for the next start


def test_a_startup_that_never_settles_still_gets_its_desk(monkeypatch):
    state = _saved(monkeypatch, browse_reading_desk_active=True, browse_reading_desk_state=_open_desk())
    state['browse_shelfmark'] = {'sys_id': A, 'shelfmark': '', 'fl_id': '', 'last_field': 'sys'}
    host, drawn = _restore(monkeypatch, state, busy=lambda host: True)
    assert drawn[0]['ids'] == [C, A, B]
    assert host.looks == genizah_app._DESK_RESTORE_WAIT_MS // genizah_app._DESK_RESTORE_POLL_MS


def test_the_session_file_entry_is_checked_on_the_way_back():
    assert desk_from_session(None) is None
    assert desk_from_session({'entries': 'x'}) is None
    assert desk_from_session({'entries': [{'sys_id': ''}, 5, {'sys_id': 7}]}) is None
    back = desk_from_session({'entries': [{'sys_id': f' {A} ', 'shelfmark': 3}, {'sys_id': A},
                                          {'sys_id': B, 'chosen': 'v08'}],
                              'pgpid': '12', 'image_view': 'all', 'image_shown': None})
    assert back == {'entries': [{'sys_id': A, 'shelfmark': '', 'chosen': ''},
                                {'sys_id': B, 'shelfmark': '', 'chosen': 'v08'}],
                    'pgpid': 12, 'image_view': 'all', 'image_shown': '', 'puzzles_shown': ''}
    assert desk_from_session({'entries': [{'sys_id': A}], 'pgpid': True})['pgpid'] is None
    assert desk_session_state(ReadingDeskState()) is None
    kept = desk_session_state(_open_desk())
    assert desk_from_session(kept) == kept                     # what is written reads back as is


# -- leaving the desk: Browse's image, when it arrived while the desk was open ------------------

def _leaving_host(owed):
    """_browse_exit_reading_desk on a stand-in host: Browse shows A, the desk is open, and A's
    images are in the catalogue's cache."""
    host = mock.MagicMock()
    host.browse_reading_desk_active = True
    host.current_browse_sid = A
    host._browse_rd_fork_worker = None
    host._browse_rd_enrich_threads = {}
    host._browse_viewer_owed = owed
    host.meta = {'images_nli': [{'url': 'https://iiif.nli.org.il/IIIFv21/FL1'}]}
    host.meta_mgr.nli_cache = {A: host.meta}
    host._browse_exit_reading_desk = types.MethodType(GUI._browse_exit_reading_desk, host)
    return host


def test_leaving_the_desk_shows_the_image_browse_was_owed():
    """A restored desk opens before Browse's manuscript is enriched: its images arrive while the
    desk is open, are not drawn then (on_browse_enriched_loaded), and are drawn when it is left."""
    host = _leaving_host(owed=A)
    host._browse_exit_reading_desk()
    host._browse_show_images.assert_called_once_with(A, host.meta, host._browse_display_shelf.return_value)
    assert host._browse_viewer_owed is None
    host.browse_load_page.assert_called_once()


def test_leaving_the_desk_redraws_nothing_browse_already_shows():
    for owed in (None, B):                                    # nothing owed, or owed for another
        host = _leaving_host(owed=owed)
        host._browse_exit_reading_desk()
        host._browse_show_images.assert_not_called()


@pytest.mark.parametrize('desk_open', [True, False])
def test_images_that_arrive_while_the_desk_is_open_are_owed_to_browse(desk_open):
    host = _leaving_host(owed=None)
    host.browse_reading_desk_active = desk_open
    host.current_browse_volume_ie = None
    host._browse_ext_info_restore = False
    host.on_browse_enriched_loaded = types.MethodType(GUI.on_browse_enriched_loaded, host)
    host.on_browse_enriched_loaded(A, host.meta)
    if desk_open:
        assert host._browse_viewer_owed == A
        host._browse_show_images.assert_not_called()
    else:
        assert host._browse_viewer_owed is None
        host._browse_show_images.assert_called_once()
