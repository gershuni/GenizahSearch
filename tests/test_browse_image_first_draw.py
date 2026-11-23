# -*- coding: utf-8 -*-
"""Browse draws a library's image from its first draw (2026-11-22).

A page turn on a Cambridge-aligned manuscript used to draw NLI's image in Phase A (a
fresh BrowsePage has no external images), then Cambridge's after Phase B: two or three
image requests, the one shown the slowest (measured on the live site,
IMAGE_LOADING_SPEED_PLAN in the private repository). Phase A now takes the manuscript's
external images from memory when Phase B would take the same ones without enriching
(``cached_external_images``), never from the network; Phase B replaces them whole.
"""
from __future__ import annotations

import asyncio
import itertools
import re

import pytest
from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation

from shared import cudl_identity
from web.components import image_resolution as ir

SYS = '990051246990205171'
CANVASES = [{'url': f'https://images.lib.cam.ac.uk/iiif/MS-TS-00013-J-00017-00018-000-0000{i}.jp2'}
            for i in (1, 2, 3)]
_paths = itertools.count()


class _Meta:
    """A metadata manager whose enrichment is counted (and empty): Phase A must never call it."""
    supports_cambridge_decision = False

    def __init__(self, cache):
        self.nli_cache = cache
        self.csv_bank = {}
        self.codico_mgr = None
        self.enrich_calls = 0

    def enrich_metadata(self, sys_id):
        self.enrich_calls += 1
        return {}

    def get_meta_for_id(self, sys_id):
        return ('T-S 13J17.18', '')

    def get_library_for_id(self, sys_id):
        return 'CUL'

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


CUL_CREDIT = 'Cambridge University Library (manifest credit)'
# Under Cambridge's own image, its library alone (owner, 2026-11-23: not the Ktiv credit the cache keeps).
CUL_LIBRARY = ('Cambridge University Library', 'ספריית אוניברסיטת קיימברידג׳')


def _cambridge_credit(text):
    return text in CUL_LIBRARY


def _entry():
    return {'images_ext': CANVASES, 'external_provider': '', 'cambridge_alignment': {'verdict': 'aligned'},
            'attribution': CUL_CREDIT, 'attribution_nli': 'NLI manifest credit'}


# --- the cache-only accessor --------------------------------------------------------------------


def test_a_fresh_entry_is_answered_from_memory():
    meta = _Meta({SYS: _entry()})
    assert cudl_identity.cached_external_entry(meta, SYS) is meta.nli_cache[SYS]
    assert meta.enrich_calls == 0


@pytest.mark.parametrize('cache', [{}, {SYS: {}}, {SYS: {'images_ext': []}}])
def test_no_images_in_memory_is_no_answer(cache):
    meta = _Meta(cache)
    assert cudl_identity.cached_external_entry(meta, SYS) is None
    assert ir.cached_external_images(SYS, meta_mgr=meta) is None
    assert meta.enrich_calls == 0


def test_a_stale_decision_is_no_answer_and_still_not_enriched(monkeypatch):
    meta = _Meta({SYS: _entry()})
    meta.supports_cambridge_decision = True  # the entry carries no current decision
    assert cudl_identity.cached_external_entry(meta, SYS) is None
    assert meta.enrich_calls == 0
    cudl_identity.fresh_external_entry(meta, SYS)  # the Phase B path still enriches
    assert meta.enrich_calls == 1


def test_katz_or_bodleian_data_to_refresh_is_no_answer(monkeypatch):
    meta = _Meta({SYS: _entry()})
    monkeypatch.setattr('shared.nli_crossref_service.katz_cache_needs_refresh', lambda *a: True)
    assert cudl_identity.cached_external_entry(meta, SYS) is None
    monkeypatch.setattr('shared.nli_crossref_service.katz_cache_needs_refresh', lambda *a: False)
    monkeypatch.setattr('shared.codicological.bodleian_cache_needs_refresh', lambda *a: True)
    assert cudl_identity.cached_external_entry(meta, SYS) is None
    assert meta.enrich_calls == 0


def test_the_snapshot_goes_on_a_fresh_page_only():
    from web.services import BrowsePage
    meta = _Meta({SYS: _entry()})
    page = BrowsePage(uid='u', p_num=2, text='', full_header='', total_pages=3, current_idx=2, sys_id=SYS)
    assert ir.apply_cached_external_images(page, meta_mgr=meta)
    assert (page.cambridge_images, page.external_provider, page.cambridge_alignment) == (
        CANVASES, '', {'verdict': 'aligned'})
    page.cambridge_images = ['own']
    assert not ir.apply_cached_external_images(page, meta_mgr=meta)
    assert page.cambridge_images == ['own']
    assert not ir.apply_cached_external_images(None, meta_mgr=meta)
    assert not ir.apply_cached_external_images(page, meta_mgr=object())  # no nli_cache


def test_the_snapshot_brings_its_credit():
    """Codex on #96: a Cambridge image drawn from memory kept the fresh page's NLI credit
    until Phase B (for good, if the enrichment failed)."""
    from web.services import BrowsePage
    meta = _Meta({SYS: _entry()})
    page = BrowsePage(uid='u', p_num=2, text='', full_header='', total_pages=3, current_idx=2, sys_id=SYS,
                      library_code='CUL', attribution='National Library of Israel')
    assert ir.apply_cached_external_images(page, meta_mgr=meta)
    assert _cambridge_credit(page.attribution) and page.attribution_nli == 'NLI manifest credit'


def test_the_credit_rule():
    from web.services import _get_library_attribution
    entry = {'attribution': 'manifest', 'attribution_nli': 'nli manifest'}
    assert ir.image_credits(entry, 'CUL', False) == ('manifest', 'nli manifest')  # the manifest's own
    assert ir.image_credits(entry, 'Manchester', False) == (_get_library_attribution('Manchester'), 'nli manifest')
    # A library with no credit of its own, held at the Bodleian.
    assert ir.image_credits(entry, 'NoSuchLibrary', True) == (_get_library_attribution('Oxford') or '', 'nli manifest')
    assert ir.image_credits(entry, 'NoSuchLibrary', False) == ('manifest', 'nli manifest')
    katz = {'attribution': 'Penn', 'external_provider': 'katz'}
    assert ir.image_credits(katz, 'Manchester', False)[0] == 'Penn'
    # Phase B applies the Katz credit after its enrichment, not here.
    assert ir.image_credits(katz, 'Manchester', False, with_katz=False)[0] == _get_library_attribution('Manchester')
    assert ir.image_credits({}, 'CUL', False) == ('', '')


# --- the preview URL ----------------------------------------------------------------------------


@pytest.mark.parametrize('url, expected', [
    (f'/api/nli_image_by_sysid/{SYS}?page=0&_cb=1', f'/api/nli_image_by_sysid/{SYS}?page=0&_cb=1&width=400'),
    (f'/api/cambridge_image/{SYS}?page=1&_cb=2&cv=1', f'/api/cambridge_image/{SYS}?page=1&_cb=2&cv=1&width=400'),
    (f'/api/manchester_image/{SYS}?page=0&_cb=1', f'/api/manchester_image/{SYS}?page=0&_cb=1&width=400'),
    (f'/api/jts_image/{SYS}?page=0', f'/api/jts_image/{SYS}?page=0&width=400'),
    (f'/api/katz_image/{SYS}?page=0&fl=123', f'/api/katz_image/{SYS}?page=0&fl=123&width=400'),
    (f'/api/oxford_image/{SYS}?page=0&_cb=1', f'/api/oxford_image/{SYS}?page=0&_cb=1&width=400'),
    ('/api/nli_image/262908052', None),  # always 2000 px: a preview would be a second full image
    ('https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_f_21_21a.jpg?_cb=1', None),
    (f'/api/cambridge_image/{SYS}?page=0&width=300', None),
    ('', None),
])
def test_preview_url(url, expected):
    assert ir.preview_url(url) == expected


# --- a real /browse page turn -------------------------------------------------------------------


@pytest.fixture
def supabase_offline(monkeypatch):
    import shared.supabase_provider as provider
    import web.supabase_client as supabase_client

    for module in (supabase_client, provider):
        monkeypatch.setattr(module, 'SUPABASE_URL', 'http://127.0.0.1:9', raising=False)
        monkeypatch.setattr(module, '_client', None, raising=False)
    monkeypatch.setattr(supabase_client, '_public_read_client', None)


def _browse(monkeypatch, meta, *, phase_b=True, phase_b_images=None, phase_b_resolve=None, turn=True,
            cudl_page=None):
    """Open /browse on page 1 of SYS, turn to page 2 (``turn``); the src of every main <img> drawn."""
    from web.services import BrowsePage
    from web.state import state as app_state
    import web.pages.browse as browse

    def page(p):
        return BrowsePage(uid=f'u{p}', p_num=p, text='שלום', full_header='hdr', total_pages=3,
                          current_idx=p, sys_id=SYS, shelfmark='T-S 13J17.18', library_code='CUL',
                          attribution='National Library of Israel')   # Phase A's default (web/services.py)

    class _Service:
        is_ready = True

        def get_browse_page(self, sys_id, p_num=1, direction=0, volume_ie=None, **_kwargs):
            return page(max(1, min(3, (p_num or 1) + (direction or 0))))

        def get_browse_page_by_fl(self, fl_id, sys_id=None):
            return page(1)

        def __getattr__(self, _name):
            return lambda *a, **k: None

    monkeypatch.setattr(browse, 'get_service', lambda: _Service())
    monkeypatch.setattr(app_state, 'meta_mgr', meta)
    # Which CUDL canvas a page has: the local cross-reference sidecar's answer would depend on this machine's
    # data; without a sidecar the route is positional (cudl_page: the resolver's answer, for a test that asks).
    import shared.nli_crossref_service as ncs
    monkeypatch.setattr(ncs, 'resolve_cambridge_canvas_for_page',
                        cudl_page or (lambda *a, **k: {'degraded': True}))
    monkeypatch.setattr('shared.nli_circuit_breaker.is_open', lambda: False)
    if not phase_b:
        async def _no_phase_b(*_args, **_kwargs):
            return None
        monkeypatch.setattr(browse, '_load_enrichment_fn', _no_phase_b)
    if phase_b_images is not None:
        monkeypatch.setattr(ir, 'resolve_external_images', lambda *a, **k: dict(phase_b_images))
    if phase_b_resolve is not None:
        monkeypatch.setattr(ir, 'resolve_external_images', phase_b_resolve)
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()
    seen = {}

    def drawn(client):
        out = []
        for element in list(client.elements.values()):
            content = getattr(element, 'content', '') or ''
            m = re.search(r'<img\s[^>]*?src="([^"]+)"[^>]*class="zoomable-image"', content, re.S)
            if m:
                full = re.search(r'data-full-src="([^"]+)"', content)
                out.append((m.group(1), full.group(1) if full else None))
                seen['fallback'] = 'data-credit-fallback="nli"' in content
        seen['credits'] = [getattr(e, 'text', '') for e in list(client.elements.values())
                           if str(e._props.get('data-role', '')).strip('"') == 'image-credit']
        seen['credit_nli'] = [e._props.get('data-credit-nli') for e in list(client.elements.values())
                              if str(e._props.get('data-role', '')).strip('"') == 'image-credit']
        seen['credit_rows'] = [list(e.classes) for e in list(client.elements.values())
                               if 'browse-image-credit' in e.classes]
        return out

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_browse_first_draw_{next(_paths)}')) as client:
            browse.create_browse_page(initial_sys_id=SYS)
        await asyncio.sleep(1.0)
        if turn:
            nexts = [e for e in client.elements.values() if isinstance(e, ui.button)
                     and str(e._props.get('data-action', '')).strip('"') == 'next']
            for listener in nexts[0]._event_listeners.values():
                if listener.type == 'click':
                    listener.handler(None)
            await asyncio.sleep(1.0)
        seen['images'] = drawn(client)

    asyncio.run(_run())
    _last_credits[:] = seen.get('credits') or []
    _last_credit_rows[:] = seen.get('credit_rows') or []
    _last_credit_nli[:] = seen.get('credit_nli') or []
    _last_marks['fallback'] = seen.get('fallback')
    return seen['images']


_last_credits: list = []
_last_credit_rows: list = []
_last_credit_nli: list = []
_last_marks: dict = {}


def test_a_page_turn_draws_the_cambridge_image_first(monkeypatch, supabase_offline):
    meta = _Meta({SYS: _entry()})
    images = _browse(monkeypatch, meta, phase_b=False)
    assert images, 'no image drawn'
    src, full = images[-1]
    assert src == f'/api/cambridge_image/{SYS}?page=1&_cb=2&cv={cudl_identity.RESOLVER_REVISION}&width=400', src
    assert full == f'/api/cambridge_image/{SYS}?page=1&_cb=2&cv={cudl_identity.RESOLVER_REVISION}', full
    assert meta.enrich_calls == 0, 'Phase A enriched'
    # ...under Cambridge's credit, not the fresh page's NLI one (Codex on #96).
    assert _last_credits and _cambridge_credit(_last_credits[-1]), _last_credits


def test_nothing_in_memory_draws_as_before(monkeypatch, supabase_offline):
    meta = _Meta({})
    images = _browse(monkeypatch, meta, phase_b=False)
    src, full = images[-1]
    assert src == f'/api/nli_image_by_sysid/{SYS}?page=1&_cb=2&width=400', src
    assert full == f'/api/nli_image_by_sysid/{SYS}?page=1&_cb=2'
    assert meta.enrich_calls == 0


def test_phase_b_withdraws_images_it_no_longer_finds(monkeypatch, supabase_offline):
    meta = _Meta({SYS: _entry()})
    withdrawn = {'cambridge_images': [], 'folio_images': [], 'external_provider': '',
                 'cambridge_alignment': None, 'cambridge_resolution': None, 'external_url': ''}
    images = _browse(monkeypatch, meta, phase_b_images=withdrawn)
    src, _full = images[-1]
    assert src.startswith(f'/api/nli_image_by_sysid/{SYS}?page=1'), f'a stale Cambridge image stayed: {src}'


def test_phase_b_keeps_the_same_image(monkeypatch, supabase_offline):
    meta = _Meta({SYS: _entry()})
    same = {'cambridge_images': CANVASES, 'folio_images': [], 'external_provider': '',
            'cambridge_alignment': {'verdict': 'aligned'}, 'cambridge_resolution': None, 'external_url': ''}
    images = _browse(monkeypatch, meta, phase_b_images=same)
    src, _full = images[-1]
    assert src.startswith(f'/api/cambridge_image/{SYS}?page=1&_cb=2'), src
    # Phase B's credit (the same rule, image_credits) is the one Phase A drew.
    assert _last_credits and _cambridge_credit(_last_credits[-1]), _last_credits


def test_a_first_visit_draws_the_cambridge_image_under_its_own_credit(monkeypatch, supabase_offline):
    """Owner, 2026-11-23: a Cambridge image, on a first visit, under NLI's credit until a reload. Phase B
    read the credit before its own enrichment filled the cache; it reads it again once the cache is filled."""
    meta = _Meta({})

    def enrich_and_resolve(sys_id, meta_mgr=None):
        meta.nli_cache[sys_id] = _entry()          # what enrich_metadata leaves in the cache
        return {'cambridge_images': CANVASES, 'folio_images': [], 'external_provider': '',
                'cambridge_alignment': {'verdict': 'aligned'}, 'cambridge_resolution': None, 'external_url': ''}

    images = _browse(monkeypatch, meta, phase_b_resolve=enrich_and_resolve, turn=False)
    src, _full = images[-1]
    assert src.startswith(f'/api/cambridge_image/{SYS}?page=0'), src
    assert _last_credits and _cambridge_credit(_last_credits[-1]), _last_credits


def test_an_nli_image_kept_for_misaligned_cambridge_images_has_nlis_credit(monkeypatch, supabase_offline):
    """Codex on #116: Cambridge images found but misaligned leave NLI's image on screen; under it goes NLI's
    own credit, not the Cambridge credit Phase B now reads once the cache is filled."""
    meta = _Meta({})
    misaligned = {'verdict': 'misaligned'}

    def enrich_and_resolve(sys_id, meta_mgr=None):
        meta.nli_cache[sys_id] = dict(_entry(), cambridge_alignment=misaligned)
        return {'cambridge_images': CANVASES, 'folio_images': [], 'external_provider': '',
                'cambridge_alignment': misaligned, 'cambridge_resolution': None, 'external_url': ''}

    images = _browse(monkeypatch, meta, phase_b_resolve=enrich_and_resolve, turn=False)
    src, _full = images[-1]
    assert src.startswith(f'/api/nli_image_by_sysid/{SYS}?page=0'), src
    assert _last_credits and 'NLI manifest credit' in _last_credits[-1], _last_credits


def test_no_cambridge_images_after_the_enrichment_keep_nlis_credit(monkeypatch, supabase_offline):
    """Codex on #116: an enrichment that finds no Cambridge images (CUDL's manifest down) can leave the old
    Cambridge credit in the cache; the NLI image then on screen keeps NLI's credit, not that one."""
    meta = _Meta({})

    def enrich_without_images(sys_id, meta_mgr=None):
        meta.nli_cache[sys_id] = dict(_entry(), images_ext=[])
        return {'cambridge_images': [], 'folio_images': [], 'external_provider': '',
                'cambridge_alignment': None, 'cambridge_resolution': None, 'external_url': ''}

    images = _browse(monkeypatch, meta, phase_b_resolve=enrich_without_images, turn=False)
    src, _full = images[-1]
    assert src.startswith(f'/api/nli_image_by_sysid/{SYS}?page=0'), src
    assert _last_credits and 'NLI manifest credit' in _last_credits[-1], _last_credits


def test_the_first_draw_keeps_its_default_credit_out_of_sight(monkeypatch, supabase_offline):
    """Owner, 2026-11-23: NLI's credit showed, then the library's. Nothing in memory, the first draw knows only
    NLI's default credit: its strip is drawn pending (out of sight, its room kept) until Phase B draws it again."""
    _browse(monkeypatch, _Meta({}), phase_b=False)
    assert _last_credit_rows and 'is-pending' in _last_credit_rows[-1], _last_credit_rows


def test_a_library_image_from_memory_has_its_credit_in_sight(monkeypatch, supabase_offline):
    _browse(monkeypatch, _Meta({SYS: _entry()}), phase_b=False)
    assert _last_credit_rows and 'is-pending' not in _last_credit_rows[-1], _last_credit_rows


def test_phase_b_draws_the_credit_in_sight(monkeypatch, supabase_offline):
    _browse(monkeypatch, _Meta({}), phase_b_resolve=lambda *a, **k: {
        'cambridge_images': [], 'folio_images': [], 'external_provider': '', 'cambridge_alignment': None,
        'cambridge_resolution': None, 'external_url': ''}, turn=False)
    assert _last_credit_rows and 'is-pending' not in _last_credit_rows[-1], _last_credit_rows


def test_a_first_visit_without_library_images_has_the_catalogue_credit(monkeypatch, supabase_offline):
    """Owner, 2026-11-23: NLI's image is Ktiv's photograph, under the catalogue's credit (the library and
    Ktiv). On a first visit the cache was filled only by Phase B's enrichment, after its first read: the
    image kept NLI's bare default until a reload."""
    ktiv = 'Cambridge University Library, "Ktiv" Project, The National Library of Israel.'
    meta = _Meta({})

    def enrich_nli_only(sys_id, meta_mgr=None):
        meta.nli_cache[sys_id] = {'images_ext': [], 'attribution': ktiv, 'marc': {'attribution': ktiv}}
        return {'cambridge_images': [], 'folio_images': [], 'external_provider': '',
                'cambridge_alignment': None, 'cambridge_resolution': None, 'external_url': ''}

    images = _browse(monkeypatch, meta, phase_b_resolve=enrich_nli_only, turn=False)
    src, _full = images[-1]
    assert src.startswith(f'/api/nli_image_by_sysid/{SYS}?page=0'), src
    assert _last_credits == [ktiv], _last_credits


def test_the_fallbacks_nli_credit_keeps_its_quotes(monkeypatch, supabase_offline):
    """The catalogue's credits quote "Ktiv"; escaped into a props string, the attribute the fallback script
    shows held `&quot;Ktiv&quot;` as text. Set directly, it holds the credit itself."""
    ktiv = 'Cambridge University Library, "Ktiv" Project, The National Library of Israel.'
    _browse(monkeypatch, _Meta({SYS: dict(_entry(), marc={'attribution': ktiv})}), phase_b=False)
    assert _last_credit_nli and _last_credit_nli[-1] == ktiv, _last_credit_nli


@pytest.mark.parametrize('answer, stand_in', [
    (None, True),                                              # the page's (folio, side) has no CUDL canvas
    ({'canvas_index': 0, 'folio_num': 1, 'side': 'r'}, False),
    ({'canvas_index': 9, 'folio_num': 5, 'side': 'r'}, True),  # out of range: the route serves NLI's too
    ({'degraded': True}, False),                               # no sidecar: the route's positional CUDL image
])
def test_the_viewer_knows_when_the_cambridge_proxy_serves_nlis_image(monkeypatch, answer, stand_in):
    """Codex on #116: /api/cambridge_image answers a page CUDL lacks with NLI's image (200, so no error fallback
    in the browser); the viewers' credit must follow. resolve_image_url says so for the viewer, never per
    thumbnail."""
    import shared.nli_crossref_service as ncs
    asked = []
    monkeypatch.setattr(ncs, 'resolve_cambridge_canvas_for_page', lambda *a, **k: (asked.append(a), answer)[1])
    kw = dict(sys_id=SYS, p_num=1, cambridge_images=CANVASES, cambridge_alignment={'verdict': 'aligned'},
              total_pages=3, nli_circuit_open=False)
    got = ir.resolve_image_url(**kw)
    assert got['active_source'] == 'cambridge' and got['nli_stand_in'] is stand_in
    asked.clear()
    assert ir.resolve_image_url(surface='thumbnail', **kw)['nli_stand_in'] is False and not asked


def test_a_cambridge_page_served_from_nli_has_nlis_credit(monkeypatch, supabase_offline):
    """The Cambridge image's URL, NLI's photograph behind it (the proxy's stand-in): NLI's credit under it."""
    images = _browse(monkeypatch, _Meta({SYS: _entry()}), phase_b=False, cudl_page=lambda *a, **k: None)
    src, _full = images[-1]
    assert src.startswith(f'/api/cambridge_image/{SYS}'), src
    assert _last_credits and 'NLI manifest credit' in _last_credits[-1], _last_credits



def test_a_library_image_turns_to_nlis_credit_when_the_browser_shows_nlis(monkeypatch, supabase_offline):
    """Codex on #116: when the Cambridge (JTS, Manchester) proxy fails, handleImageError shows NLI's image and
    turns the credit to NLI's only for an image marked data-credit-fallback="nli" -- Katz's alone were."""
    _browse(monkeypatch, _Meta({SYS: _entry()}), phase_b=False)
    assert _last_marks['fallback'] is True
    _browse(monkeypatch, _Meta({}), phase_b=False)                       # NLI's own image: nothing to turn
    assert _last_marks['fallback'] is False


def test_the_full_screen_image_says_unavailable_in_the_readers_language():
    """Codex on #116: the full-screen <img> lacked the texts handleImageError shows, so it said them in English."""
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / 'web/pages/browse.py').read_text(encoding='utf-8')
    tag = src[src.index('id="fs-zoomable-image"') - 400:src.index('id="fs-zoomable-image"')]
    assert '{_fs_said}' in tag
    said = src[src.index('_fs_said = ('):src.index('_fs_said = (') + 300]
    assert 'data-oxford-direct-label=' in said and 'data-unavailable-label=' in said
