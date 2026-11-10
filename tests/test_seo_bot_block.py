"""SEO-tool crawler policy tests (2026-07-08 web-memory Tier-2).

Covers web/crawler_visibility.py matcher + decision function and the
robots.txt SEO group emitted by web/api.py::robots_txt. The middleware in
web/main.py is a one-line call into should_block_seo_tool_request, mirroring
the existing archive-crawler pattern, so the decision function carries the
behavioral tests (init_api_routes(app_override=...) does not install
web/main.py middleware — Codex pre-flight drift note).
"""

import pytest

from web.crawler_visibility import (
    SEO_TOOL_ROBOTS_AGENTS,
    SEO_TOOL_USER_AGENT_TOKENS,
    is_seo_tool_crawler,
    should_block_seo_tool_request,
)


SEMRUSH_UA = 'Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)'
AHREFS_UA = 'Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)'
MJ12_UA = 'Mozilla/5.0 (compatible; MJ12bot/v1.4.8; http://mj12bot.com/)'


class TestIsSeoToolCrawler:
    @pytest.mark.parametrize('ua', [SEMRUSH_UA, AHREFS_UA, MJ12_UA])
    def test_matches_known_seo_bots(self, ua):
        assert is_seo_tool_crawler(ua) is True

    def test_matches_case_insensitively(self):
        assert is_seo_tool_crawler(SEMRUSH_UA.upper()) is True

    @pytest.mark.parametrize('ua', [
        'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
        'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm) Chrome/116.0.1938.76 Safari/537.36',
        'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
        'python-requests/2.32.5',
    ])
    def test_does_not_match_search_engines_or_users(self, ua):
        assert is_seo_tool_crawler(ua) is False

    @pytest.mark.parametrize('ua', [None, ''])
    def test_missing_user_agent_is_not_a_bot(self, ua):
        assert is_seo_tool_crawler(ua) is False

    def test_tokens_are_lowercase_for_substring_matching(self):
        assert all(t == t.lower() for t in SEO_TOOL_USER_AGENT_TOKENS)


class TestShouldBlockSeoToolRequest:
    @pytest.mark.parametrize('path', ['/', '/browse', '/browse?x=1', '/search', '/api/search'])
    def test_blocks_all_regular_paths(self, path):
        assert should_block_seo_tool_request(path, SEMRUSH_UA) is True

    def test_robots_txt_stays_reachable(self):
        # RFC 9309: a 4xx robots.txt means "allow all" — crawlers must be
        # able to read their Disallow group to deregister.
        assert should_block_seo_tool_request('/robots.txt', SEMRUSH_UA) is False

    def test_normal_browser_is_never_blocked(self):
        assert should_block_seo_tool_request('/browse', 'Mozilla/5.0 Chrome/149.0') is False


class TestRobotsTxtSeoGroup:
    @pytest.fixture()
    def robots_body(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from web.api import init_api_routes

        test_app = FastAPI()
        init_api_routes(app_override=test_app)
        client = TestClient(test_app)
        response = client.get('/robots.txt')
        assert response.status_code == 200
        return response.text

    def test_group_lists_every_canonical_agent(self, robots_body):
        for agent in SEO_TOOL_ROBOTS_AGENTS:
            assert f'User-agent: {agent}\n' in robots_body

    def test_group_disallows_everything_once(self, robots_body):
        # The SEO group is the only "Disallow: /" (root) in the file.
        assert robots_body.count('Disallow: /\n') == 1

    def test_group_precedes_sitemap(self, robots_body):
        assert robots_body.index('User-agent: SemrushBot') < robots_body.index('Sitemap:')

    def test_general_allow_group_survives(self, robots_body):
        assert robots_body.startswith('User-agent: *\nAllow: /\n')

    def test_archive_group_survives(self, robots_body):
        # Adding the SEO group must not disturb the pre-existing archive group.
        from web.crawler_visibility import (
            ARCHIVE_DISALLOWED_PATHS,
            ARCHIVE_USER_AGENT_TOKENS,
        )
        for agent in ARCHIVE_USER_AGENT_TOKENS:
            assert f'User-agent: {agent}\n' in robots_body
        for path in ARCHIVE_DISALLOWED_PATHS:
            assert f'Disallow: {path}\n' in robots_body


class TestMiddlewareSourceOrder:
    """Pin the SEO 403 branch position inside the web/main.py middleware.

    The pure decision function is tested above; this static source guard
    covers the wiring that init_api_routes(app_override=...) cannot exercise
    (Codex code-review LOW: a regression moving the 403 below call_next
    would otherwise pass the suite).
    """

    def test_seo_block_precedes_archive_block_and_call_next(self):
        import re
        from pathlib import Path

        source = (
            Path(__file__).resolve().parent.parent / 'web' / 'main.py'
        ).read_text(encoding='utf-8')
        match = re.search(
            r'async def _mark_non_document_paths_noindex.*?(?=\n@|\nasync def |\nclass |\ndef )',
            source,
            re.S,
        )
        assert match, 'noindex middleware not found in web/main.py'
        body = match.group(0)
        i_seo = body.index('should_block_seo_tool_request')
        i_archive = body.index('should_block_archive_request')
        i_call_next = body.index('await call_next(')
        assert i_seo < i_archive < i_call_next
