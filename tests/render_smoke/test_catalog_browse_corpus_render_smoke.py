"""/catalog-browse over the whole corpus (PR 1a of
docs/plans/CATALOGUE_CRITERIA_PLAN.md): the page hands the metadata manager's
corpus list to the service (D16), marks a manuscript with no catalogue record,
and shows an error -- not "no manuscripts match" -- when the list cannot be
read (rule 9)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import web.main as web_main  # noqa: F401  (registers /catalog-browse)
import web.pages.catalog_browse as catalog_browse
from shared.fjms_service import FilterUnavailable
from tests.render_smoke.test_catalog_browse_initial_load_render_smoke import (
    _FakeFjms, _page_text, _run,
)
from web.state import state

_UNCATALOGUED = '990099999990205171'


def _meta():
    return SimpleNamespace(
        corpus_sys_ids=lambda timeout=30.0: frozenset({_UNCATALOGUED}),
        get_meta_for_id=lambda sid: ('T-S Smoke 1', ''),
        get_library_for_id=lambda sid: 'CUL',
    )


async def _wait_for(user, needle, seconds=5.0):
    deadline = asyncio.get_running_loop().time() + seconds
    while asyncio.get_running_loop().time() < deadline:
        if needle in _page_text(user):
            return
        await asyncio.sleep(0.05)


class _CorpusFjms(_FakeFjms):
    def __init__(self):
        super().__init__()
        self.universes = []
        self.facet_universes = []

    def get_browse_results(self, *args, **kwargs):
        universe = kwargs.get('universe')
        self.universes.append(universe() if callable(universe) else universe)
        return {'results': [{'sys_id': _UNCATALOGUED, 'catalogued': False}], 'total': 1}

    def get_browse_library_facets(self, *args, **kwargs):
        universe = kwargs.get('universe')
        self.facet_universes.append(universe() if callable(universe) else universe)
        return {}


def test_the_page_lists_the_corpus_and_marks_uncatalogued_rows():
    fake = _CorpusFjms()

    async def driver(user):
        await user.open('/catalog-browse')
        await _wait_for(user, 'No catalogue record')
        assert fake.universes and fake.universes[-1] == frozenset({_UNCATALOGUED}), fake.universes
        assert fake.facet_universes[-1] == frozenset({_UNCATALOGUED}), fake.facet_universes
        assert 'No catalogue record' in _page_text(user)

    with patch.object(catalog_browse, 'get_fjms_service', return_value=fake), \
            patch.object(state, 'meta_mgr', _meta()):
        _run(driver)


class _FailingFjms(_FakeFjms):
    def get_browse_results(self, *args, **kwargs):
        raise FilterUnavailable('query_failed', 'boom')


def test_a_list_that_cannot_be_read_is_an_error_not_no_matches():
    async def driver(user):
        await user.open('/catalog-browse')
        await _wait_for(user, 'could not be listed')
        text = _page_text(user)
        assert 'The catalog could not be listed. Try again in a moment.' in text, text
        assert 'No manuscripts match the current filters' not in text, text

    with patch.object(catalog_browse, 'get_fjms_service', return_value=_FailingFjms()), \
            patch.object(state, 'meta_mgr', _meta()):
        _run(driver)
