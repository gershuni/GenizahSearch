from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import init_api_routes
from web.crawler_visibility import (
    ARCHIVE_DISALLOWED_PATHS,
    ARCHIVE_RUNTIME_BLOCKED_PATHS,
    ARCHIVE_USER_AGENT_TOKENS,
    should_block_archive_request,
    should_mark_noindex,
)


def test_robots_txt_limits_wayback_to_public_pages():
    bare = FastAPI()
    init_api_routes(app_override=bare)

    response = TestClient(bare).get('/robots.txt')

    assert response.status_code == 200
    text = response.text
    assert 'User-agent: *' in text
    for agent in ARCHIVE_USER_AGENT_TOKENS:
        assert f'User-agent: {agent}' in text
    for path in ARCHIVE_DISALLOWED_PATHS:
        assert f'Disallow: {path}' in text


def test_archive_crawler_runtime_block_only_hits_non_public_paths():
    archive_ua = (
        'Mozilla/5.0 (compatible; archive.org_bot '
        '+http://www.archive.org/details/archive.org_bot)'
    )

    blocked_paths = tuple(
        f'{path}probe'
        for path in ARCHIVE_RUNTIME_BLOCKED_PATHS
        if path.endswith('/')
    ) + ('/favicon.ico', '/search', '/search/results')
    for path in blocked_paths:
        assert should_block_archive_request(path, archive_ua)

    assert not should_block_archive_request('/', archive_ua)
    assert not should_block_archive_request('/about', archive_ua)
    assert not should_block_archive_request('/api/search', archive_ua)
    assert not should_block_archive_request('/static/common.css', 'Googlebot/2.1')


def test_non_document_paths_get_x_robots_tag_policy():
    assert should_mark_noindex('/_nicegui/3.8.0/components/input.js')
    assert should_mark_noindex('/static/og-image.png')
    assert should_mark_noindex('/favicon.ico')
    assert not should_mark_noindex('/')
    assert not should_mark_noindex('/api/search')
    assert not should_mark_noindex('/about')


def test_homepage_schema_does_not_advertise_literal_search_template():
    source = Path('web/main.py').read_text(encoding='utf-8')

    assert 'SearchAction' not in source
    assert 'search_term_string' not in source
