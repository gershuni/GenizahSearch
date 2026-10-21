# -*- coding: utf-8 -*-
"""Browse data built in a worker thread comes out in the visitor's language.

``run.io_bound`` hands a callable to a thread with no context at all, so
``get_language()`` there fell back to the process default ('he'). Since the UI
language became per visitor (6a0946e1) that made every English visitor's /browse
library name and image credit Hebrew: the header, the Library field, Quick View,
the print plate, the "How to cite" chip and the Joins Lab / Compare viewers all
show ``BrowsePage.library_name`` built by ``GenizahService.get_browse_page`` in
such a thread. ``in_visitor_language`` carries the language (only the language)
into the thread.
"""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import pytest
from nicegui import run

import genizah_core
import web.services as services
import web.translations as translations
from web.state import state

ROOT = Path(__file__).resolve().parents[1]
CUL_EN = 'Cambridge University Library'


class _Searcher:
    index = object()

    def get_browse_page(self, sys_id, **_kwargs):
        return {'sys_id': sys_id, 'uid': 'u', 'p_num': 1, 'text': 'x', 'full_header': '',
                'total_pages': 1, 'current_idx': 0, 'internal_index': 0}


class _Meta:
    def get_meta_for_id(self, _sys_id):
        return ('T-S 1.1', 'title')

    def get_library_for_id(self, _sys_id):
        return 'CUL'

    def parse_full_id_components(self, _header):
        return {}


@pytest.fixture
def english_visitor_service(monkeypatch):
    monkeypatch.setattr(state, 'searcher', _Searcher(), raising=False)
    monkeypatch.setattr(state, 'meta_mgr', _Meta(), raising=False)
    monkeypatch.setattr(state, 'is_ready', lambda: True, raising=False)
    monkeypatch.setattr(genizah_core, 'get_volumes_for_sys_id', lambda _sid: [])
    monkeypatch.setattr(translations, '_current_lang', 'he')  # the process default
    return services.GenizahService()


def test_a_worker_thread_gets_the_visitors_language(english_visitor_service):
    service = english_visitor_service

    async def visitor():
        translations.set_language('en')
        plain = await run.io_bound(service.get_browse_page, '990000000000000000')
        carried = await run.io_bound(translations.in_visitor_language(service.get_browse_page),
                                     '990000000000000000')
        return plain, carried

    plain, carried = asyncio.run(visitor())
    assert plain.library_name != CUL_EN, 'the trap this helper exists for is gone; recheck'
    assert carried.library_name == CUL_EN
    assert carried.attribution == services._NLI_EN


def test_the_worker_language_does_not_stay_on_the_pool_thread():
    async def main():
        translations.set_language('en')
        await run.io_bound(translations.in_visitor_language(lambda: None))
        return await run.io_bound(translations.get_language)

    assert asyncio.run(main()) == translations._current_lang


# Worker calls that build browse pages, keyed by file: the name of the callable
# handed to run.io_bound, or a marker its lambda calls. Each must go through
# in_visitor_language.
_WORKER_SITES = {
    'web/pages/browse.py': {'_fetch_page', '_fetch_metadata_only'},
    'web/pages/browse_enrichment.py': {'_browse_enrich_sync'},
    'web/pages/search_results.py': {'get_browse_page'},
    'web/components/anchor_viewer.py': {'_resolve'},
}


def _unwrapped_worker_calls(source: str, markers: set[str]) -> list[int]:
    lines = []
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'io_bound' and node.args):
            continue
        target = node.args[0]
        if (isinstance(target, ast.Call) and isinstance(target.func, ast.Name)
                and target.func.id == 'in_visitor_language'):
            continue
        names = {n.id for n in ast.walk(target) if isinstance(n, ast.Name)}
        names |= {n.attr for n in ast.walk(target) if isinstance(n, ast.Attribute)}
        if names & markers:
            lines.append(node.lineno)
    return lines


@pytest.mark.parametrize('relpath', sorted(_WORKER_SITES))
def test_browse_worker_calls_carry_the_language(relpath):
    source = (ROOT / relpath).read_text(encoding='utf-8')
    assert _unwrapped_worker_calls(source, _WORKER_SITES[relpath]) == []


def test_the_call_site_check_can_fail():
    seeded = ('async def f():\n'
              '    await run.io_bound(_fetch_page)\n'
              '    await run.io_bound(lambda: service.get_browse_page(1))\n'
              '    await run.io_bound(in_visitor_language(_fetch_page))\n')
    assert _unwrapped_worker_calls(seeded, {'_fetch_page', 'get_browse_page'}) == [2, 3]
