# -*- coding: utf-8 -*-
"""Browse's neighbour prefetch (desktop/browse_prefetch.py, 2026-11-22).

The next and the previous manuscript on the shelf are prepared while the reader looks at
one: their metadata (unless fresh), then their first page's preview, then its sharp image,
one manuscript at a time, the reader's direction first; dropped when the reader moves
elsewhere, except the manuscript being opened, whose downloads the viewer then waits on.
Threads are stand-ins whose signals the tests fire; nothing touches the network.
"""
from __future__ import annotations

import pytest
from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtTest import QTest

from desktop.browse_prefetch import BrowsePrefetcher

pytestmark = pytest.mark.gui

THREADS = []


class _Thread(QObject):
    finished = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.started = self.running = self.cancelled = False
        THREADS.append(self)

    def start(self):
        self.started = self.running = True

    def isRunning(self):
        return self.running

    def cancel(self):
        self.cancelled = True

    def end(self):
        self.running = False
        self.finished.emit()


class _Enrich(_Thread):
    finished_signal = pyqtSignal(str, dict)

    def __init__(self, meta_mgr, sid):
        super().__init__()
        self.sid = sid

    def answer(self, meta=None):
        self.finished_signal.emit(self.sid, meta if meta is not None else {'sys_id': self.sid})
        self.end()


class _Loader(_Thread):
    image_loaded = pyqtSignal(object)
    load_failed = pyqtSignal()

    def __init__(self, url):
        super().__init__()
        self.url = url

    def ok(self):
        self.image_loaded.emit(object())
        self.end()

    def fail(self):
        self.load_failed.emit()
        self.end()


@pytest.fixture(autouse=True)
def _clear():
    THREADS.clear()
    yield
    THREADS.clear()


def _make(neighbours, fresh=None):
    calls = {'urls': []}

    def urls(sid, meta):
        calls['urls'].append(sid)
        return [f'{sid}/preview', f'{sid}/full']

    p = BrowsePrefetcher(meta_mgr=lambda: object(), neighbours=lambda sid: neighbours.get(sid, []),
                         fresh_meta=lambda sid: (fresh or {}).get(sid), image_urls=urls,
                         enrich_thread=_Enrich, loader=_Loader, delay_ms=5)
    return p, calls


def _running(cls):
    return [t for t in THREADS if isinstance(t, cls) and t.running]


def test_neighbours_are_prepared_one_at_a_time_preview_then_sharp():
    p, calls = _make({'A': ['B', 'Z']})
    p.schedule('A')
    assert not THREADS, 'started before the delay'
    QTest.qWait(30)
    [enrich] = _running(_Enrich)
    assert enrich.sid == 'B', 'the direction of travel first'
    enrich.answer()
    [preview] = _running(_Loader)
    assert preview.url == 'B/preview'
    preview.ok()
    [full] = _running(_Loader)
    assert full.url == 'B/full'
    full.ok()
    [enrich_z] = _running(_Enrich)            # then the other neighbour
    assert enrich_z.sid == 'Z'
    enrich_z.answer()
    _running(_Loader)[0].ok()
    _running(_Loader)[0].ok()
    assert p.prepared == {'B', 'Z'}
    assert not _running(_Thread)


def test_fresh_metadata_is_not_enriched_again():
    p, calls = _make({'A': ['B']}, fresh={'B': {'sys_id': 'B'}})
    p.schedule('A')
    QTest.qWait(30)
    assert not [t for t in THREADS if isinstance(t, _Enrich)]
    assert calls['urls'] == ['B']
    assert _running(_Loader)[0].url == 'B/preview'


def test_moving_elsewhere_drops_the_job_and_keeps_its_threads_until_they_end():
    p, _ = _make({'A': ['B', 'Z']})
    p.schedule('A')
    QTest.qWait(30)
    _running(_Enrich)[0].answer()
    preview = _running(_Loader)[0]
    p.cancel(keep='Q')                      # the reader opened another manuscript
    assert preview.cancelled
    assert preview in p._kept, 'a running thread was let go'
    preview.ok()                            # its answer comes, and is ignored
    assert not _running(_Loader), 'the dropped job went on'
    assert preview not in p._kept
    QTest.qWait(30)
    assert not _running(_Thread), 'the queue went on after the reader moved'


def test_moving_onto_the_manuscript_being_prepared_lets_its_downloads_finish():
    p, _ = _make({'A': ['B', 'Z'], 'B': ['C', 'A']})
    p.schedule('A')
    QTest.qWait(30)
    _running(_Enrich)[0].answer()
    preview = _running(_Loader)[0]
    p.cancel(keep='B')                      # the reader opened B, which is being prepared
    assert not preview.cancelled
    p.schedule('B')                         # B drawn: its neighbours are next
    QTest.qWait(30)
    assert p.job_sid() == 'B', "B's downloads were dropped though the viewer waits on them"
    preview.ok()
    _running(_Loader)[0].ok()               # B's sharp image
    assert 'B' in p.prepared
    assert [t.sid for t in _running(_Enrich)] == ['C'], "B's own neighbours follow, the reader's way first"


def test_a_move_that_keeps_the_job_among_the_new_neighbours_lets_it_finish():
    """A loader dropped before its answer came threw the download away (2026-11-22)."""
    p, _ = _make({'A': ['B', 'Z']})
    p.schedule('A')
    QTest.qWait(30)
    _running(_Enrich)[0].answer()
    preview = _running(_Loader)[0]
    p.cancel(keep=['Q', 'B', 'Y'])          # the reader opened Q, whose neighbours include B
    assert not preview.cancelled
    assert p.job_sid() == 'B'
    p.cancel(keep=('Q',))
    assert preview.cancelled


def test_a_failed_image_leaves_the_rest_of_that_manuscript_to_the_visit():
    p, _ = _make({'A': ['B', 'Z']})
    p.schedule('A')
    QTest.qWait(30)
    _running(_Enrich)[0].answer()
    _running(_Loader)[0].fail()
    assert [t.sid for t in _running(_Enrich)] == ['Z'], 'stuck on the failed manuscript'
    assert not [t for t in THREADS if isinstance(t, _Loader) and t.url == 'B/full']


def test_stop_starts_nothing_more():
    p, _ = _make({'A': ['B']})
    p.stop()
    p.schedule('A')
    QTest.qWait(30)
    assert not THREADS


def test_prepared_manuscripts_are_skipped_and_the_one_on_screen_never_queued():
    p, _ = _make({'A': ['A', 'B']})
    p.prepared.add('B')
    p.schedule('A')
    QTest.qWait(30)
    assert not THREADS


def test_a_neighbour_lookup_that_fails_disturbs_nothing():
    p = BrowsePrefetcher(meta_mgr=lambda: object(), neighbours=lambda sid: 1 / 0,
                         fresh_meta=lambda sid: None, image_urls=lambda s, m: [],
                         enrich_thread=_Enrich, loader=_Loader, delay_ms=5)
    p.schedule('A')
    QTest.qWait(30)
    assert not THREADS


def test_the_metadata_thread_preparing_a_manuscript_is_named():
    p, _ = _make({'A': ['B', 'Z']})
    p.schedule('A')
    QTest.qWait(30)
    enrich = _running(_Enrich)[0]
    assert p.pending_enrichment('B') is enrich
    assert p.pending_enrichment('Z') is None
    enrich.finished_signal.emit('B', {'sys_id': 'B'})   # its answer came; the thread is still ending
    assert enrich.running
    assert p.pending_enrichment('B') is None, 'named after its answer came'
