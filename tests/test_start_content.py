"""Contract tests for the versioned, code-free /start curation surface."""

from copy import deepcopy
from urllib.parse import parse_qs, urlsplit

import pytest

from web.start_content import (
    StartContentError,
    _validate_start_content,
    demo_url,
    live_computed_candidates,
    load_start_content,
    manuscript_url,
    puzzle_url,
    search_url,
    work_url,
)


def test_launch_pool_shape_balance_and_unique_ids():
    content = load_start_content()

    assert len(content['searches']) == 12
    assert len(content['manuscripts']) == 12
    assert len(content['works']) == 6
    assert len(content['computed_candidates']) == 8
    assert all(set(entry['category']) == {'en', 'he'} for entry in content['manuscripts'])
    assert {'Prayer', 'Bible and book art', 'Personal letter', 'Calendar'} <= {
        entry['category']['en'] for entry in content['manuscripts']
    }
    assert {level: sum(s['difficulty'] == level for s in content['searches'])
            for level in ('simple', 'advanced', 'research')} == {
                'simple': 4, 'advanced': 4, 'research': 4,
            }

    ids = [entry['id'] for group in (
        content['searches'], content['manuscripts'], content['works'],
        content['computed_candidates'], [content['puzzle']],
        list(content['demos'].values()),
    ) for entry in group]
    assert len(ids) == len(set(ids))


def test_all_generated_destinations_are_safe_internal_links_and_encoded():
    content = load_start_content()
    urls = (
        [search_url(entry) for entry in content['searches']]
        + [manuscript_url(entry) for entry in content['manuscripts']]
        + [work_url(entry) for entry in content['works']]
        + [manuscript_url(entry, computed=True) for entry in content['computed_candidates']]
        + [puzzle_url(content['puzzle'])]
        + [demo_url(name, content['demos'][name]) for name in ('parallels', 'joins')]
    )

    assert all(url.startswith('/') and not url.startswith('//') for url in urls)
    for entry in content['searches']:
        url = search_url(entry)
        params = parse_qs(urlsplit(url).query)
        assert params['q'] == [entry['query']]
        assert params['mode'] == [entry['mode']]
        assert ' ' not in url and '#' not in url
    assert puzzle_url(content['puzzle']) == '/puzzle'  # unvalidated saved doc safely falls back


def test_launch_searches_are_specific_and_use_the_intended_search_features():
    content = load_start_content()
    queries = {entry['query'] for entry in content['searches']}

    assert 'ירושלים' not in queries
    assert {'המלך המשיח', 'כדת משה וישראל', 'אלהי אל תדינני כמעלי'} <= queries
    assert sum(entry['mode'] == 'responsa' for entry in content['searches']) == 3
    assert all(
        set(entry['responsa_flags']) == {'variants', 'ja', 'flex_spaces', 'bidirectional'}
        for entry in content['searches']
        if entry['mode'] == 'responsa'
    )

    yonah = next(entry for entry in content['searches'] if entry['id'] == 'search-yonah-variants')
    assert yonah['mode'] == 'variants'
    assert yonah['query'] == 'יונה מצאה בו מנוח'
    assert 'responsa_flags' not in yonah

    table_talk = next(
        entry for entry in content['searches']
        if entry['id'] == 'search-table-talk-responsa'
    )
    assert table_talk['mode'] == 'responsa'
    assert table_talk['query'] == '#אין (משיחין/מסיחין) [2] #סעודה'
    params = parse_qs(urlsplit(search_url(table_talk)).query)
    assert params['q'] == ['#אין (משיחין/מסיחין) [2] #סעודה']
    assert params['mode'] == ['responsa']
    assert 'flex_spaces' not in params


def test_invalid_editorial_change_fails_before_rendering():
    content = deepcopy(load_start_content())
    content['searches'][1]['id'] = content['searches'][0]['id']
    with pytest.raises(StartContentError, match='duplicate stable ID'):
        _validate_start_content(content)

    content = deepcopy(load_start_content())
    content['manuscripts'][0]['thumbnail'] = 'https://example.invalid/image.jpg'
    with pytest.raises(StartContentError, match='internal image endpoint'):
        _validate_start_content(content)


def test_computed_examples_require_availability_and_exact_frame(monkeypatch):
    import web.discovery_assets as assets

    content = load_start_content()
    launch_hash = content['computed_candidates'][0]['frame_content_hashes'][0]
    next_frame_hash = 'dc15864d2ecd32367c858457a961d5a1aef0e47c399d466adc31d632da96aac4'

    assert content['computed_candidates_published'] is True
    assert all(next_frame_hash in entry['frame_content_hashes']
               for entry in content['computed_candidates'])

    monkeypatch.setattr(assets, 'discovery_available', lambda: False)
    assert live_computed_candidates(content) == []

    monkeypatch.setattr(assets, 'discovery_available', lambda: True)
    monkeypatch.setattr(assets, 'discovery_meta', lambda key: '0' * 64)
    assert live_computed_candidates(content) == []

    monkeypatch.setattr(assets, 'discovery_meta', lambda key: launch_hash)
    assert [entry['id'] for entry in live_computed_candidates(content)] == [
        entry['id'] for entry in content['computed_candidates']
    ]

    monkeypatch.setattr(assets, 'discovery_meta', lambda key: next_frame_hash)
    assert [entry['id'] for entry in live_computed_candidates(content)] == [
        entry['id'] for entry in content['computed_candidates']
    ]


def test_manuscript_reference_and_prepared_research_demos_are_validated():
    content = load_start_content()
    palimpsest = next(
        entry for entry in content['manuscripts']
        if entry['shelfmark'] == 'T-S E2.51'
    )
    assert palimpsest['reference']['url'].startswith('https://www.lib.cam.ac.uk/')
    assert 'Cherlow' in palimpsest['reference']['label']['en']

    parallels = content['demos']['parallels']
    assert parallels['enabled'] is True
    assert parallels['text'].startswith('דרור יקרא לבן עם בת')
    assert parse_qs(urlsplit(demo_url('parallels', parallels)).query)['text'] == [
        parallels['text']
    ]

    joins = content['demos']['joins']
    assert joins['enabled'] is True
    assert parse_qs(urlsplit(demo_url('joins', joins)).query) == {
        'sys_id': ['990001403820205171'],
        'page': ['1'],
    }


def test_computed_examples_are_the_selected_launch_set_with_exact_pages():
    content = load_start_content()
    assert {entry['id'] for entry in content['computed_candidates']} == {
        'computed-hayyuj-kitab-al-nutaf',
        'computed-maimonides-logic',
        'computed-keter-malkhut',
        'computed-twenty-chapters',
        'computed-ben-hofni-tzitzit',
        'computed-rashi-numbers',
        'computed-saadiah-beliefs-opinions',
        'computed-mevasser-objections-saadiah',
    }
    assert all('automatic' in entry['description']['en'].lower()
               for entry in content['computed_candidates'])
    twenty = next(entry for entry in content['computed_candidates'] if entry['id'] == 'computed-twenty-chapters')
    assert parse_qs(urlsplit(manuscript_url(twenty, computed=True)).query) == {
        'sys_id': ['990051284490205171'],
        'page': ['1'],
        'computed': ['1'],
    }
    objections = next(
        entry for entry in content['computed_candidates']
        if entry['id'] == 'computed-mevasser-objections-saadiah'
    )
    assert parse_qs(urlsplit(manuscript_url(objections, computed=True)).query) == {
        'sys_id': ['990001538420205171'],
        'page': ['42'],
        'computed': ['1'],
    }
