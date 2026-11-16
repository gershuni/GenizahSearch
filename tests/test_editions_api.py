# -*- coding: utf-8 -*-
"""The public Search API with the scholarly editions (editions plan §3.5, PR 3): the source
choice, one item per page with its members, the edition locator on the browse endpoint, and
today's responses wherever the editions are not searched.

The real engine on a small main index and an editions generation (tests/test_editions_search.py's
``world``), called in process (no research worker: tests/test_editions_parallel_jobs.py covers
that route)."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

pytest.importorskip("tantivy")

from tests.test_editions_search import MS1, MS2, P11, P12, world  # noqa: E402,F401


@pytest.fixture(autouse=True)
def _api_env(monkeypatch):
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '9999')
    from web.search_api import _daily_budget, _rate_limiter, _browse_rate_limiter
    for limiter in (_rate_limiter, _daily_budget, _browse_rate_limiter):
        limiter.reset_for_tests()


class _Resolver:
    """The sidecars' texts, as EditionResolver.resolve answers (its own tests read real files)."""
    def resolve(self, version):
        kind, eid = version.split(':')
        return {'kind': kind, 'id': eid, 'version': version, 'original': f'edition text {version}',
                'credit': {'en': 'Sidecar', 'he': 'Sidecar'}}


@pytest.fixture
def client(world, monkeypatch):  # noqa: F811 -- the imported fixture
    # Imported here, not at collection: a web import reads the language before the test
    # isolation applies (this machine's desktop profile is Hebrew).
    from web import editions_assets
    from web.search_api import init_search_api
    from web.state import state
    monkeypatch.setattr(state, 'searcher', world['eng'])
    meta = world['eng'].meta_mgr
    # The fixture's catalogue stand-in, with what the browse page reads.
    monkeypatch.setattr(meta, 'get_meta_for_id', lambda sid: (f'MS {sid[-6:]}', ''), raising=False)
    monkeypatch.setattr(meta, 'get_library_for_id', lambda sid: 'CUL', raising=False)
    monkeypatch.setattr(state, 'meta_mgr', meta)
    monkeypatch.setattr(editions_assets, '_resolver', _Resolver())
    monkeypatch.setenv('EDITIONS_SEARCH_ENABLED', '1')
    bare = FastAPI()
    init_search_api(app_override=bare)
    return TestClient(bare)


def _search(client, **body):
    body.setdefault('search_mode', 'exact')
    r = client.post('/api/search', json=body)
    return r.status_code, r.json()


def test_editions_are_searched_by_default_one_item_per_page(client):
    status, env = _search(client, query='שלום עליכם')
    assert status == 200, env
    assert env['request']['sources'] is None and env['request']['sources_effective'] == 'all'
    items = {i['uid']: i for i in env['results']}
    assert P11 in items
    p11 = items[P11]
    assert set(p11['sources']) == {'automatic', 'fgp_edition', 'pgp_edition'}
    members = {m['uid']: m for m in p11['members']}
    assert 'ED:fgp:1:' + MS1 in members or any(u.startswith('ED:fgp:1') for u in members)
    fgp1 = next(m for u, m in members.items() if u.startswith('ED:fgp:1'))
    assert fgp1['source'] == 'fgp_edition'
    assert fgp1['edition']['version'] == 'fgp:1'
    assert fgp1['edition']['credit'] == {'en': 'Team 1', 'he': 'Team 1'}
    assert fgp1['locator']['sys_id'] == MS1 and fgp1['locator']['p_num'] == '1'
    assert set(fgp1['edition']) == {'kind', 'id', 'version', 'sys_id', 'located', 'cross_page', 'credit'}
    automatic = next(m for m in p11['members'] if m['source'] == 'automatic')
    assert automatic['edition'] is None
    # The same PGP text on the verso: a second result.
    assert any(m['edition'] and m['edition']['version'] == 'pgp:10' for m in items[P12]['members'])
    # An edition whose page is not identified: its own result, no page number (never 0).
    loose = [i for i in env['results'] if not i['page_identified']]
    assert loose and loose[0]['locator']['p_num'] is None and loose[0]['locator']['sys_id'] == MS2


def test_automatic_only_is_todays_response(client, monkeypatch):
    _, chosen = _search(client, query='שלום עליכם', sources='automatic')
    monkeypatch.setenv('EDITIONS_SEARCH_ENABLED', '0')
    _, today = _search(client, query='שלום עליכם')
    for env in (chosen, today):
        for item in env['results']:
            assert not {'members', 'sources', 'edition', 'page_identified'} & set(item)
    strip = lambda env: [{k: v for k, v in i.items() if k != 'score'} for i in env['results']]  # noqa: E731
    assert strip(chosen) == strip(today)
    assert 'sources' not in today['request']
    assert chosen['request']['sources_effective'] == 'automatic'


def test_where_editions_are_not_searched(client, monkeypatch):
    for value in ('0', 'editors'):        # editors: a website preview; the API knows no reader
        monkeypatch.setenv('EDITIONS_SEARCH_ENABLED', value)
        status, env = _search(client, query='שלום עליכם', sources='all')
        assert status == 200 and 'editions_unavailable' in env['warnings']
        assert env['request']['sources_effective'] == 'automatic'
        assert all('members' not in i for i in env['results'])
        status, env = _search(client, query='שלום עליכם', sources='editions')
        assert status == 400 and env['error']['code'] == 'editions_unavailable', env


def test_editions_only(client):
    status, env = _search(client, query='שלום עליכם', sources='editions')
    assert status == 200
    assert env['results'] and all('automatic' not in i['sources'] for i in env['results'])


def test_sources_with_a_catalogue_mode_is_refused(client):
    status, env = _search(client, query='T-S 1', search_mode='shelfmark', sources='editions')
    assert status == 400 and env['error']['code'] == 'invalid_combination'


def test_browse_an_edition_on_its_page(client):
    r = client.get('/api/browse', params={'sys_id': MS1, 'version': 'fgp:1'})
    env = r.json()
    assert r.status_code == 200, env
    assert env['text_source'] == 'fgp_edition'
    assert env['text'] == 'edition text fgp:1'
    assert env['edition']['version'] == 'fgp:1' and env['edition']['page_identified'] is True
    assert env['edition']['credit'] == {'en': 'Team 1', 'he': 'Team 1'}
    assert env['locator']['uid'] == P11
    # With the page given, that page.
    r = client.get('/api/browse', params={'sys_id': MS1, 'version': 'pgp:10', 'p_num': 2})
    assert r.status_code == 200 and r.json()['locator']['uid'] == P12


def test_browse_an_edition_whose_page_is_not_identified(client):
    r = client.get('/api/browse', params={'sys_id': MS2, 'version': 'fgp:4'})
    env = r.json()
    assert r.status_code == 200, env
    assert env['edition']['page_identified'] is False
    assert 'page_not_identified' in [w.get('code') for w in env['warnings']]
    assert env['locator']['p_num'] == 1


def test_browse_refuses_an_edition_of_another_manuscript(client, monkeypatch):
    for params in ({'sys_id': MS2, 'version': 'fgp:1'}, {'sys_id': MS1, 'version': 'fgp:99'}):
        r = client.get('/api/browse', params=params)
        assert r.status_code == 404 and r.json()['error']['code'] == 'edition_not_found'
    r = client.get('/api/browse', params={'sys_id': MS1, 'version': 'fgp:x'})
    assert r.status_code == 400
    monkeypatch.setenv('EDITIONS_SEARCH_ENABLED', '0')
    r = client.get('/api/browse', params={'sys_id': MS1, 'version': 'fgp:1'})
    assert r.status_code == 400 and r.json()['error']['code'] == 'editions_unavailable'


# --- ChatGPT / MCP pages: a group is trimmed inside, never refused -------------------------------

def _oversized_group(n, snippet_chars):
    member = lambda i: {  # noqa: E731
        'source': 'fgp_edition', 'uid': f'ED:fgp:{i}:990000000110205171',
        'locator': {'sys_id': '990000000110205171', 'volume_ie': 'IE11', 'p_num': '1'},
        'snippet': 'א' * snippet_chars, 'match_terms': [],
        'edition': {'kind': 'fgp', 'id': str(i), 'version': f'fgp:{i}', 'sys_id': '990000000110205171',
                    'located': True, 'cross_page': False,
                    'credit': {'en': f'Team {i}', 'he': f'צוות {i}'}}}
    row = {'uid': 'IE11_P1_FL111', 'locator': {'sys_id': '990000000110205171', 'volume_ie': 'IE11', 'p_num': '1'},
           'shelfmark': 'MS', 'snippet': 'x', 'sources': ['automatic', 'fgp_edition'], 'edition': None,
           'page_identified': True, 'members': [member(i) for i in range(n)]}
    return {'schema_version': 1, 'source': 'search', 'generated_at': 'now', 'count': 1, 'total': 1,
            'results': [row], 'warnings': [], 'request': {}}


@pytest.mark.parametrize('n, chars', [(40, 3000), (150, 3000)])
@pytest.mark.parametrize('channel', ['chatgpt', 'mcp'])
def test_an_oversized_group_pages_with_its_identity_credits_and_locators(channel, n, chars):
    import json
    from web.chatgpt_pagination import build_pages, page_budget
    payload = _oversized_group(n, chars)
    page_bytes, measure = page_budget(channel)
    pages, _ = build_pages(payload, 'job-1', page_bytes=page_bytes, measure=measure)
    got = json.loads(pages['results'][0])['results'][0]
    want = payload['results'][0]
    assert got['uid'] == want['uid'] and got['sources'] == want['sources']
    assert [m['uid'] for m in got['members']] == [m['uid'] for m in want['members']]
    assert [m['edition'] for m in got['members']] == [m['edition'] for m in want['members']]
    assert all(m['locator'] == want['members'][0]['locator'] for m in got['members'])
    assert got['member_snippets_shortened'] is True
    if n == 150:          # forty shortened snippets fit; a hundred and fifty do not
        assert got['member_snippets_omitted'] is True and 'snippet' not in got['members'][0]
    else:
        assert len(got['members'][0]['snippet']) == 201


def test_the_synchronous_chatgpt_answer_trims_a_group_too():
    from web.chatgpt_api import compact_payload
    got = compact_payload(_oversized_group(40, 3000))
    assert got is not None and len(got['results'][0]['members']) == 40
