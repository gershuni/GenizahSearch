# -*- coding: utf-8 -*-
"""The page count of a manuscript the index holds no pages of (no automatic transcription at all).

The index gives such a page ``total_pages`` 0, so Browse and Quick View showed "Page 1/0" and could not page on
(T-S NS 243.145 has 4 NLI images). ``web.page_count.manuscript_image_count`` counts its images: the NLI crossref
folio list (local), else the NLI FL-id list the image route pages through (``web.api.nli_fl_ids``, which may fetch
the manifest). ``count_pages`` waits for it only ``PAGE_COUNT_WAIT_S``; a slower count is reported later, so neither
view is held up by NLI (Codex on #454). Browse's Phase B and Quick View both use it; their render-smokes are tests/render_smoke/test_browse_page_count_render_smoke.py and
tests/render_smoke/test_quick_view_without_automatic_text_render_smoke.py.
"""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import web.api as api
from web import page_count
from web.page_count import apply_page_count, count_pages, manuscript_image_count

SID = '990051614250205171'          # T-S NS 243.145
SYNTHETIC_SID = '990001234560000000'


class _Crossref:
    def __init__(self, folios, available=True):
        self.folios, self.available, self.asked = folios, available, []

    def is_available(self):
        return self.available

    def get_folio_images(self, sys_id):
        self.asked.append(sys_id)
        return self.folios


@pytest.fixture
def registered(monkeypatch):
    """Register a fake fetch_fl_ids_from_nli / _fl_ids_complete pair, as init_api_routes() does."""
    calls = {'fetch': [], 'ids': {}, 'complete': True}

    def fetch(sys_id, suffix=1):
        calls['fetch'].append((sys_id, suffix))
        return calls['ids'].get(sys_id, [])

    monkeypatch.setitem(api._registered_nli_fl_ids, 'fetch', fetch)
    monkeypatch.setitem(api._registered_nli_fl_ids, 'complete', lambda sys_id, suffix=1: calls['complete'])
    return calls


def test_nli_fl_ids_is_empty_before_the_routes_are_set_up(monkeypatch):
    monkeypatch.setattr(api, '_registered_nli_fl_ids', {})
    assert api.nli_fl_ids(SID) == []


def test_nli_fl_ids_returns_the_manifest_list(registered):
    registered['ids'][SID] = ['1', '2', '3', '4']
    assert api.nli_fl_ids(SID) == ['1', '2', '3', '4']


def test_nli_fl_ids_ignores_the_marc_records_partial_list(registered):
    registered['ids'][SID] = ['1']
    registered['complete'] = False
    assert api.nli_fl_ids(SID) == []


def test_the_crossref_folios_count_the_pages_without_asking_nli(registered):
    registered['ids'][SID] = ['1', '2', '3', '4']
    with patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=_Crossref([{}, {}, {}])):
        assert manuscript_image_count(SID) == 3
    assert registered['fetch'] == []                 # local is enough: no manifest on Phase B's path


def test_the_nli_image_list_counts_them_when_the_crossref_does_not_know_it(registered):
    registered['ids'][SID] = ['1', '2', '3', '4']
    with patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=_Crossref([])):
        assert manuscript_image_count(SID) == 4
    with patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=_Crossref([], available=False)):
        assert manuscript_image_count(SID) == 4


def test_the_callers_folio_list_is_used_as_given(registered):
    registered['ids'][SID] = ['1', '2', '3', '4']
    crossref = _Crossref([{}, {}, {}])
    with patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref):
        assert manuscript_image_count(SID, folio_images=[{}, {}]) == 2
        assert manuscript_image_count(SID, folio_images=[]) == 4    # an empty list: ask NLI
    assert crossref.asked == []


def test_nothing_known_is_zero(registered):
    with patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=_Crossref([], available=False)):
        assert manuscript_image_count(SID) == 0


@pytest.mark.parametrize('sys_id', [SYNTHETIC_SID, '', None])
def test_no_nli_images_to_count(registered, sys_id):
    assert manuscript_image_count(sys_id) == 0
    assert registered['fetch'] == []


@pytest.mark.parametrize('p_num, count, expected', [(1, 4, 1), (3, 4, 3), (9, 4, 4), (0, 4, 1)])
def test_a_count_keeps_the_page_number_within_it(p_num, count, expected):
    page = SimpleNamespace(total_pages=0, p_num=p_num, current_idx=p_num)
    assert apply_page_count(page, count) is True
    assert (page.total_pages, page.p_num, page.current_idx) == (count, expected, expected)


@pytest.mark.parametrize('count, total', [(0, 0), (None, 0), (4, 2)])
def test_an_unknown_count_or_a_page_with_one_changes_nothing(count, total):
    page = SimpleNamespace(total_pages=total, p_num=1, current_idx=1)
    assert apply_page_count(page, count) is False
    assert page.total_pages == total


def _run_count(monkeypatch, seconds, count):
    """count_pages over a count that takes ``seconds``; returns (answer, late calls after the count is done)."""
    async def io_bound(fn, *args):
        await asyncio.sleep(seconds)
        return count

    monkeypatch.setattr(page_count.run, 'io_bound', io_bound)
    monkeypatch.setattr(page_count.background_tasks, 'create', lambda coro, name=None: asyncio.ensure_future(coro))
    monkeypatch.setattr(page_count, 'PAGE_COUNT_WAIT_S', 0.05)
    late = []

    async def go():
        answer = await count_pages(SID, on_late=late.append)
        await asyncio.sleep(seconds + 0.1)
        return answer

    return asyncio.run(go()), late


def test_a_quick_count_is_the_answer(monkeypatch):
    assert _run_count(monkeypatch, 0, 4) == (4, [])


def test_a_slow_count_does_not_hold_the_page_and_comes_later(monkeypatch):
    t0 = time.monotonic()
    answer, late = _run_count(monkeypatch, 0.3, 4)
    assert answer is None and late == [4]
    assert time.monotonic() - t0 < 1.0


def test_a_slow_unknown_count_reports_nothing(monkeypatch):
    assert _run_count(monkeypatch, 0.3, 0) == (None, [])
