# -*- coding: utf-8 -*-
"""web/browse_head.py: what a /browse address may put in the page's head.

The page-level check, over the served HTML, is
tests/render_smoke/test_browse_head_escaping.py.
"""
from __future__ import annotations

import json

import pytest

from web.browse_head import breadcrumb_data, json_ld_script, manuscript_data, tag_sys_id

SID = '990053928670205171'
OPEN, CLOSE = '<script type="application/ld+json">\n', '\n</script>'

HOSTILE = [
    '</script><script>alert(1)</script>',
    '<!-- <script>',
    'a & b < c > d',
    'quote " backslash ' + chr(92) + ' newline \n tab \t',
    'L-G Bib.VII.37',
    'כתב יד "מקרא" <תפילה>',
    'line' + chr(0x2028) + 'separator',
]


def _body(block: str) -> str:
    assert block.startswith(OPEN) and block.endswith(CLOSE), block
    return block[len(OPEN):-len(CLOSE)]


@pytest.mark.parametrize('value', [SID, '99' + '1' * 8, '990000000001000000'])
def test_a_whole_corpus_system_number_is_accepted(value):
    assert tag_sys_id(value) == value


@pytest.mark.parametrize('value', [
    None, '', 0, 990053928670205171,
    '97' + '1' * 16,                      # "My Library": never on the website
    '99', '9912345',                      # too short
    ' ' + SID, SID + ' ', SID + '\n',     # the whole value, nothing around it
    SID + '"><i id=x>', SID + '</script>', 'x' + SID, SID + 'a',
    '99"><i id=x>', '99</script><i id=x>',
    'L-G Bib.VII.37',
])
def test_anything_else_is_refused(value):
    assert tag_sys_id(value) is None


@pytest.mark.parametrize('value', HOSTILE)
def test_no_value_can_close_or_leave_a_json_ld_block(value):
    data = {'name': value, 'nested': {'list': [value, value]}}
    body = _body(json_ld_script(data))
    assert not set(body) & set('<>&'), body
    assert json.loads(body) == data


def test_the_angle_bracket_becomes_its_json_escape():
    # The escape is built from the character code: typed out, it can be decoded on its
    # way into a file, which is how the page's earlier guard came to replace '<' with '<'.
    escape = chr(92) + 'u003c'
    assert len(escape) == 6
    assert escape in _body(json_ld_script({'name': '<'}))


def test_the_breadcrumb_names_the_manuscript_and_its_address():
    data = breadcrumb_data('L-G Bib.VII.37', SID, 'https://example.org')
    last = data['itemListElement'][-1]
    assert data['@type'] == 'BreadcrumbList'
    assert [item['position'] for item in data['itemListElement']] == [1, 2, 3]
    assert last['name'] == 'L-G Bib.VII.37'
    assert last['item'] == f'https://example.org/browse?sys_id={SID}'


def test_the_manuscript_names_its_library_only_when_it_is_known():
    known = manuscript_data('L-G Bib.VII.37', SID, 'https://example.org', 'Cambridge University Library')
    unknown = manuscript_data('L-G Bib.VII.37', SID, 'https://example.org')
    assert known['@type'] == 'Manuscript' and known['identifier'] == SID
    assert known['url'] == f'https://example.org/browse?sys_id={SID}'
    assert known['holdingArchive'] == {'@type': 'ArchiveOrganization', 'name': 'Cambridge University Library'}
    assert 'holdingArchive' not in unknown
