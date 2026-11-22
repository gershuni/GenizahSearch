# -*- coding: utf-8 -*-
"""Browse shows a manuscript enriched in this session at once, and names the neighbours the
prefetch prepares (2026-11-22).

Measured in the desktop window before the change: a return to a manuscript seen a minute
before took 0.8 s, most of it a metadata refresh (Cambridge's image list downloaded again);
the next manuscript on a Cambridge shelf took 5.2 s to its sharp image. The real GenizahGUI
methods run on a stand-in window; the enrichment thread is a stand-in.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtTest import QTest

pytestmark = pytest.mark.gui

SID, NEXT, PREV = "990051247450205171", "990051247460205171", "990051247440205171"


@pytest.fixture(autouse=True)
def _isolated_profile(tmp_path, monkeypatch):
    import genizah_core
    for var in ("USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA"):
        folder = tmp_path / "profile" / var
        folder.mkdir(parents=True)
        monkeypatch.setenv(var, str(folder))
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    yield


class _Meta:
    def __init__(self, cache=None, fresh=()):
        self.nli_cache = dict(cache or {})
        self._fresh = set(fresh)
        self.supports_cambridge_decision = False
        self.csv_bank = {}

    def enriched_recently(self, sid):
        return sid in self._fresh

    def get_part_for_folio(self, sid):
        return 'MS. Heb. x/1' if sid == 'PART' else None

    def get_library_for_id(self, sid):
        return 'CUL'


class _Button:
    def setVisible(self, _v):
        pass


class _Prefetcher:
    def __init__(self):
        self.cancelled = []
        self.pending = {}

    def cancel(self, keep=None):
        self.cancelled.append(keep)

    def pending_enrichment(self, sid):
        return self.pending.get(sid)


def _window(monkeypatch, meta_mgr):
    import genizah_app
    G = genizah_app.GenizahGUI
    started = []

    class _Thread:
        def __init__(self, mm, sid, suffix=1):
            self.sid = sid
            self.finished_signal = SimpleNamespace(connect=lambda slot: None, disconnect=lambda slot: None)
            started.append(self)

        def start(self):
            pass

    monkeypatch.setattr(genizah_app, "EnrichMetadataThread", _Thread)
    shown = []
    prefetcher = _Prefetcher()
    w = SimpleNamespace(
        meta_mgr=meta_mgr, _browse_enrich_gen=0, current_browse_volume_ie=None,
        current_browse_part_id=None, current_browse_part_folios=[], current_browse_part_folio_idx=0,
        btn_b_external_link=_Button(), _update_browse_pgp_button=lambda *_: None,
        enrich_browse_worker=None, browse_reading_desk_active=False,
        on_browse_enriched_loaded=lambda s, m, g=None, v=None: shown.append((s, m, g, v)),
        _browse_prefetcher=lambda: prefetcher,
    )
    for name in ("_start_browse_enrichment", "_browse_fresh_meta", "_browse_neighbour_sids", "_browse_adjacent_sid",
                 "_browse_in_part", "_browse_prefetch_urls", "_browse_display_shelf",
                 "_browse_image_choice", "_is_cambridge_display"):
        setattr(w, name, getattr(G, name).__get__(w))
    return w, started, shown, prefetcher


def _complete(sid):
    return {'sys_id': sid, 'marc': {'title': 'x'}, 'shelfmark': 'T-S 13J19.10',
            'images_nli': [{'url': 'https://iiif.nli.org.il/IIIFv21/FL777', 'fl_id': '777'}]}


def test_a_manuscript_enriched_this_session_is_shown_without_a_refresh(monkeypatch):
    entry = _complete(SID)
    w, started, shown, prefetcher = _window(monkeypatch, _Meta({SID: entry}, fresh={SID}))
    w._start_browse_enrichment(SID)
    assert not started, 'enriched again'
    assert not shown, 'shown before this load finished (its page text comes first)'
    QTest.qWait(20)
    assert shown == [(SID, entry, 1, None)]
    assert [SID in keep for keep in prefetcher.cancelled] == [True], "the move did not drop the other neighbours' work"


@pytest.mark.parametrize("cache, fresh", [
    ({SID: _complete(SID)}, ()),                                   # not from this session
    ({SID: {**_complete(SID), 'marc': None}}, {SID}),              # no catalogue record kept
    ({}, {SID}),                                                   # nothing cached
])
def test_otherwise_the_refresh_runs_as_before(monkeypatch, cache, fresh):
    cache = {k: {kk: vv for kk, vv in v.items() if vv is not None} for k, v in cache.items()}
    w, started, shown, _ = _window(monkeypatch, _Meta(cache, fresh=fresh))
    w._start_browse_enrichment(SID)
    assert [t.sid for t in started] == [SID]
    QTest.qWait(20)
    assert not shown


def test_a_part_or_another_volume_is_refreshed_as_before(monkeypatch):
    w, started, _, _ = _window(monkeypatch, _Meta({SID: _complete(SID)}, fresh={SID}))
    w._start_browse_enrichment(SID, is_part=True)
    assert [t.sid for t in started] == [SID]


def test_the_neighbours_are_the_shelf_s_the_reader_s_way_first(monkeypatch):
    w, *_ = _window(monkeypatch, _Meta())
    w._browse_shelf_step = lambda sid: (SimpleNamespace(sys_id=PREV), SimpleNamespace(sys_id=NEXT))
    assert w._browse_neighbour_sids(SID) == [NEXT, PREV]
    w._browse_last_direction = -1
    assert w._browse_neighbour_sids(SID) == [PREV, NEXT]
    w._browse_shelf_step = lambda sid: (None, SimpleNamespace(sys_id='PART'))
    assert w._browse_neighbour_sids(SID) == [], 'a folio that opens as a Part is not prepared'
    w.browse_reading_desk_active = True
    assert w._browse_neighbour_sids(SID) == []


def test_without_a_shelf_the_file_order_gives_the_neighbours(monkeypatch):
    w, *_ = _window(monkeypatch, _Meta())
    w._browse_shelf_step = lambda sid: None
    w.searcher = SimpleNamespace(get_adjacent_sys_id_by_file_order=lambda sid, d: NEXT if d > 0 else PREV)
    assert w._browse_neighbour_sids(SID) == [NEXT, PREV]


def test_the_prefetch_fetches_the_page_browse_will_show(monkeypatch):
    w, *_ = _window(monkeypatch, _Meta())
    assert w._browse_prefetch_urls(NEXT, _complete(NEXT)) == [
        'https://iiif.nli.org.il/IIIFv21/FL777/full/400,/0/default.jpg',
        'https://iiif.nli.org.il/IIIFv21/FL777/full/2000,/0/default.jpg']
    cudl = 'https://images.lib.cam.ac.uk/iiif/MS-TS-1-000-0000{}.jp2'
    meta = {**_complete(NEXT), 'external_provider': 'cambridge',
            'images_ext': [{'url': cudl.format(1)}, {'url': cudl.format(2)}],
            'images_nli': [{'url': 'n1'}, {'url': 'n2'}], 'cambridge_alignment': {'verdict': 'aligned'}}
    w._resolve_cambridge_page_or_fallback = lambda sid, page, dm, folio: (dm, 1)   # page 1 is canvas 2
    assert w._browse_prefetch_urls(NEXT, meta) == [f"{cudl.format(2)}/full/400,/0/default.jpg",
                                                   f"{cudl.format(2)}/full/2000,/0/default.jpg"]
    assert w._browse_prefetch_urls(NEXT, {**meta, 'external_provider': 'katz'}) == []
    assert w._browse_prefetch_urls(NEXT, {}) == []


def test_the_drawn_manuscript_starts_its_page_at_once_and_schedules_its_neighbours():
    """on_browse_enriched_loaded builds dozens of widgets, so this checks its shape: the page
    starts right after load_images (flush_pending_page), before the panels it then builds
    (~0.25 s, which the viewer's debounce waited out), and the neighbours are scheduled. The
    effect was measured in the desktop window (IMAGE_LOADING_SPEED_PLAN, private repository)."""
    import ast
    import inspect
    import textwrap
    import genizah_app
    src = textwrap.dedent(inspect.getsource(genizah_app.GenizahGUI.on_browse_enriched_loaded))
    fn = ast.parse(src).body[0]
    calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
    names = [(ast.unparse(c.func), c.lineno) for c in calls]
    load = [ln for name, ln in names if name == 'self.browse_viewer.load_images']
    flush = [ln for name, ln in names if name == 'self.browse_viewer.flush_pending_page']
    assert load and flush, (load, flush)
    following = min(ln for _name, ln in names if ln > load[0])
    assert following == flush[0], 'something runs between load_images and the page start'
    panels = [ln for name, ln in names if name == 'self._build_browse_enriched_html']
    assert panels and flush[0] < panels[0]
    after_flush = min(ln for _name, ln in names if ln > flush[0])
    assert ('self._browse_release_image', after_flush) in names, 'the image waits for the panels to be painted'
    assert any(name == 'self._browse_prefetcher().schedule' for name, _ in names)


def test_opening_a_neighbour_being_prepared_takes_the_prefetch_s_answer(monkeypatch):
    """Codex on #98: a second metadata fetch for a neighbour the prefetch is still preparing
    raced it for the cache entry. Browse listens to the prefetch's thread instead."""
    from PyQt6.QtCore import QObject, pyqtSignal

    class _Running(QObject):
        finished_signal = pyqtSignal(str, dict)

    w, started, shown, prefetcher = _window(monkeypatch, _Meta())
    running = _Running()
    prefetcher.pending[SID] = running
    w._start_browse_enrichment(SID)
    assert not started, 'a second fetch raced the prefetch'
    assert w.enrich_browse_worker is running
    entry = _complete(SID)
    running.finished_signal.emit(SID, entry)
    assert shown == [(SID, entry, 1, None)]
    w._start_browse_enrichment(NEXT)              # the reader moves on: no stale answer
    running.finished_signal.emit(SID, entry)
    assert shown == [(SID, entry, 1, None)]


def _chain(order):
    """A shelf: each sys_id's previous and next placement."""
    def step(sid):
        if sid not in order:
            return None
        i = order.index(sid)
        return (SimpleNamespace(sys_id=order[i - 1], id=i - 1) if i > 0 else None,
                SimpleNamespace(sys_id=order[i + 1], id=i + 1) if i + 1 < len(order) else None)
    return step


def test_the_next_two_the_reader_s_way_are_prepared_then_the_one_behind(monkeypatch):
    """Walking a Cambridge shelf, the one after next was not ready: about 2 s (2026-11-22)."""
    w, *_ = _window(monkeypatch, _Meta())
    w._browse_shelf_step = _chain(['A', 'B', 'C', 'D', 'E'])
    assert w._browse_neighbour_sids('C') == ['D', 'E', 'B']
    w._browse_last_direction = -1
    assert w._browse_neighbour_sids('C') == ['B', 'A', 'D']
    w._browse_last_direction = 1
    assert w._browse_neighbour_sids('D') == ['E', 'C'], 'at the end of the shelf'
    w._browse_shelf_step = _chain(['A', 'PART', 'C'])
    assert w._browse_neighbour_sids('A') == [], 'nothing beyond a folio that opens as a Part'


def test_a_move_keeps_the_prefetch_working_on_the_new_manuscript_s_neighbours(monkeypatch):
    """A loader dropped before its answer came threw the download away: the manuscript after
    next, being fetched when the reader stepped onto the next one, was fetched again."""
    w, _started, _shown, prefetcher = _window(monkeypatch, _Meta())
    w._browse_shelf_step = _chain([PREV, SID, NEXT, 'AFTER'])
    w._start_browse_enrichment(SID)
    assert sorted(prefetcher.cancelled[-1]) == sorted([SID, NEXT, 'AFTER', PREV])


class _Resolved:
    def __init__(self, answer):
        self.answer, self.pages = answer, []

    def __call__(self, sys_id, page, images_ext, svc=None):
        self.pages.append(page)
        return self.answer


@pytest.mark.parametrize("page_num, page_idx", [(1, 0), (5, 4)])
def test_the_cambridge_resolver_gets_the_page_from_0(monkeypatch, page_num, page_idx):
    """Browse keeps the page from 1; the resolver and the NLI image URL count from 0. Page 1
    resolved to the second leaf side, which the page text's own choice replaced a moment later:
    a second download, and the image shown, hidden and shown again (2026-11-22)."""
    import shared.nli_crossref_service as ncs
    import genizah_app
    meta = {'images_ext': [{'url': f'c{i}'} for i in range(6)]}
    w = SimpleNamespace(_nli_crossref_service=object())
    w._resolve = genizah_app.GenizahGUI._resolve_cambridge_page_or_fallback.__get__(w)

    found = _Resolved({'canvas_index': 3})
    monkeypatch.setattr(ncs, 'resolve_cambridge_canvas_for_page', found)
    assert w._resolve(SID, page_num, meta, None) == (meta, 3)
    assert found.pages == [page_idx]

    urls = []
    w._build_nli_iiif_url_for_page = lambda sid, idx: urls.append(idx) or f'nli{idx}'
    monkeypatch.setattr(ncs, 'resolve_cambridge_canvas_for_page', _Resolved(None))
    shown, idx = w._resolve(SID, page_num, meta, None)
    assert urls == [page_idx] and shown['images_ext'][idx]['url'] == f'nli{page_idx}'

    monkeypatch.setattr(ncs, 'resolve_cambridge_canvas_for_page', _Resolved({'degraded': True}))
    assert w._resolve(SID, page_num, meta, None) == (meta, page_idx), 'the positional fallback'


def test_the_info_line_puts_the_title_on_its_own_line_in_both_places():
    """on_browse_enriched_loaded wrote "shelfmark | title" on one line, and browse_render_page,
    which it calls a moment later, the title on a second line: the image pane moved up and down
    and the image with it (2026-11-22)."""
    import ast
    import inspect
    import textwrap
    import genizah_app
    for fn in (genizah_app.GenizahGUI.on_browse_enriched_loaded, genizah_app.GenizahGUI.browse_render_page):
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
        joins = [n.value for n in ast.walk(tree) if isinstance(n, ast.AugAssign)
                 and isinstance(n.value, ast.JoinedStr)
                 and any(isinstance(x, ast.Name) and x.id == 'truncated' for x in ast.walk(n.value))]
        assert joins, fn.__name__
        for js in joins:
            assert isinstance(js.values[0], ast.Constant) and js.values[0].value.startswith('<br/>'), (
                fn.__name__, ast.unparse(js))


def _bind(w, *names):
    import genizah_app
    for name in names:
        # plain functions: PyQt refuses a slot bound to a non-QObject stand-in
        setattr(w, name, (lambda f: lambda *a, **k: f(w, *a, **k))(getattr(genizah_app.GenizahGUI, name)))


def test_the_old_image_stays_painted_until_the_prepared_one_is_drawn():
    """browse_load empties the viewer a turn before a prepared manuscript's image is drawn: that
    blank was painted between the two images. The hold ends when it is drawn, or after a limit."""
    from PyQt6.QtWidgets import QWidget
    viewport = QWidget()
    viewer = SimpleNamespace(isVisible=lambda: True, scroll_area=SimpleNamespace(viewport=lambda: viewport))
    w = SimpleNamespace(browse_viewer=viewer, BROWSE_IMAGE_HOLD_MS=30)
    _bind(w, '_browse_hold_image', '_browse_release_image')
    w._browse_hold_image()
    assert not viewport.updatesEnabled()
    w._browse_release_image()
    assert viewport.updatesEnabled()
    w._browse_hold_image()
    QTest.qWait(100)
    assert viewport.updatesEnabled(), 'the hold never ended'


@pytest.mark.parametrize("fresh", [True, False])
def test_the_hold_is_for_a_move_to_a_prepared_manuscript_only(fresh):
    from PyQt6.QtWidgets import QLineEdit
    calls = []
    w = SimpleNamespace(
        current_browse_sid=SID, current_browse_part_id=None, current_browse_part_folios=[],
        current_browse_part_folio_idx=0, browse_reading_desk_active=False,
        _browse_shelf_step=_chain([SID, NEXT]),
        meta_mgr=SimpleNamespace(get_part_for_folio=lambda s: None, get_meta_for_id=lambda s: ('T-S 1', None)),
        browse_sys_input=QLineEdit(), browse_shelf_input=QLineEdit(),
        _set_last_browse_field=lambda f: None,
        _browse_fresh_meta=lambda s: ({'sys_id': s} if fresh else None),
        _browse_hold_image=lambda: calls.append('hold'),
        browse_load=lambda: calls.append('load'),
    )
    _bind(w, 'navigate_manuscript')
    w.navigate_manuscript(1)
    assert calls == (['hold', 'load'] if fresh else ['load'])


def test_a_prefetch_thread_deleted_after_it_was_taken_disturbs_no_later_move(monkeypatch):
    """The prefetch deletes its metadata thread when it finishes (desktop/browse_prefetch.py),
    while Browse may still name it as its worker: the next move only disconnects from it."""
    from PyQt6 import sip
    from PyQt6.QtCore import QCoreApplication, QEvent, QObject, pyqtSignal

    class _Running(QObject):
        finished_signal = pyqtSignal(str, dict)

    w, started, _shown, prefetcher = _window(monkeypatch, _Meta())
    running = _Running()
    prefetcher.pending[SID] = running
    w._start_browse_enrichment(SID)
    running.deleteLater()
    QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
    assert sip.isdeleted(running)
    assert w.enrich_browse_worker is running
    w._start_browse_enrichment(NEXT)
    assert [t.sid for t in started] == [NEXT]
