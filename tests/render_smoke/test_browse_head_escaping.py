# -*- coding: utf-8 -*-
"""Render-smoke: /browse never writes its address's values into the page as they came.

Until 2026-11-27 the ``sys_id`` and ``shelfmark`` of the address went into the served
HTML unescaped: into ``og:url``, ``twitter:url`` and the canonical link (a ``"`` closed
the attribute), and into both JSON-LD blocks (``</script>`` closed the block). So a
crafted link ran its sender's markup in the visitor's browser.

Each test requests the real page and reads the HTML it serves. The markers are inert
(``<i id=...>``). A page that answered with an error, or that dropped its tags, would
pass the marker checks alone, so the last tests require a real manuscript's tags too.
"""
from __future__ import annotations

import html as html_lib
import json
import re
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote

import pytest

from tests.render_smoke.test_ai_listings_render import _run
from web.state import state

SID = '990053928670205171'
SHELFMARK = 'L-G Bib.VII.37'
CATALOGUE = SimpleNamespace(csv_bank={SID: {'shelfmark': SHELFMARK, 'library_code': 'CUL', 'title': ''}})
JSON_LD = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.DOTALL)


def _get(**params):
    """(status, served HTML) of /browse with ``params``, over a catalogue of one record."""
    address = '/browse?' + '&'.join(f'{name}={quote(value, safe="")}' for name, value in params.items())

    async def driver(_user, client):
        response = await client.get(address)
        return response.status_code, response.text

    with patch.object(state, 'meta_mgr', CATALOGUE):
        return _run(driver)


def _blocks(page):
    """The page's JSON-LD blocks, parsed, by their @type."""
    return {data['@type']: data for data in (json.loads(body) for body in JSON_LD.findall(page))}


def _attribute(page, pattern):
    [value] = re.findall(pattern, page)
    return html_lib.unescape(value)


@pytest.mark.parametrize('params', [
    {'sys_id': '99"><i id=probe>'},                       # closes a meta tag's attribute
    {'sys_id': '99</script><i id=probe>'},                # closes a JSON-LD block
    {'sys_id': SID + '"><i id=probe>'},                   # a real system number, then more
    {'sys_id': SID + '</script><i id=probe>'},
    {'sys_id': SID, 'shelfmark': 'X</script><i id=probe>'},
    {'sys_id': SID, 'shelfmark': 'X"><i id=probe>'},
    {'shelfmark': 'X</script><i id=probe>"'},
    {'shelfmark': '"><i id=probe>'},
])
def test_no_value_of_the_address_reaches_the_page_as_markup(params):
    status, page = _get(**params)
    assert status == 200
    assert '<i id=probe>' not in page
    for body in JSON_LD.findall(page):
        json.loads(body)  # no block was cut short


def test_a_shelfmark_with_markup_is_still_the_manuscripts_name():
    hostile = 'X</script><i id=probe>'
    status, page = _get(sys_id=SID, shelfmark=hostile)
    assert status == 200
    blocks = _blocks(page)
    assert blocks['Manuscript']['name'] == hostile
    assert blocks['BreadcrumbList']['itemListElement'][-1]['name'] == hostile


def test_a_sys_id_that_is_not_a_system_number_gets_the_generic_tags():
    status, page = _get(sys_id=SID + '"><i id=probe>')
    assert status == 200
    assert _attribute(page, r'<link rel="canonical" href="([^"]*)">') == 'https://genizahsearch.com/browse'
    assert _blocks(page) == {}


def test_page_meta_escapes_the_address_it_writes():
    # /browse hands page_meta a checked system number; this holds for every other caller.
    from web.main import page_meta
    tags = page_meta('/browse?sys_id=99"><i id=probe>&page=2')
    assert '<i id=probe>' not in tags
    address = 'https://genizahsearch.com/browse?sys_id=99"><i id=probe>&page=2'
    assert _attribute(tags, r'<link rel="canonical" href="([^"]*)">') == address
    assert _attribute(tags, r'<meta property="og:url" content="([^"]*)">') == address
    assert _attribute(tags, r'<meta name="twitter:url" content="([^"]*)">') == address


@pytest.mark.parametrize('params', [{'sys_id': SID}, {'sys_id': SID, 'shelfmark': SHELFMARK}])
def test_a_manuscript_keeps_its_own_tags(params):
    status, page = _get(**params)
    assert status == 200
    address = f'https://genizahsearch.com/browse?sys_id={SID}'
    assert _attribute(page, r'<link rel="canonical" href="([^"]*)">') == address
    assert _attribute(page, r'<meta property="og:url" content="([^"]*)">') == address
    blocks = _blocks(page)
    manuscript = blocks['Manuscript']
    assert manuscript['name'] == SHELFMARK
    assert manuscript['identifier'] == SID and manuscript['url'] == address
    assert 'Cambridge' in manuscript['holdingArchive']['name']
    last = blocks['BreadcrumbList']['itemListElement'][-1]
    assert last == {'@type': 'ListItem', 'position': 3, 'name': SHELFMARK, 'item': address}
