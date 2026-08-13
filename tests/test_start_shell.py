"""Shared-shell and crawler visibility checks for the guided /start page."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import init_api_routes


def test_start_route_and_navigation_are_registered():
    source = Path('web/main.py').read_text(encoding='utf-8')

    assert "@ui.page('/start'" in source
    assert "safe_user_set('current_page', '/start')" in source
    assert "page_meta(\n        '/start'" in source
    assert "from web.pages.start import create_start_page" in source

    home_item = "('/', 'home', tr('Home'), None)"
    start_item = "('/start', 'explore', tr('Start Here'), None)"
    about_item = "('/about', 'info', tr('About the Genizah'), None)"
    assert source.index(home_item) < source.index(start_item) < source.index(about_item)


def test_start_is_a_primary_llms_entry_and_static_sitemap_page():
    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    llms = client.get('/llms.txt')
    assert llms.status_code == 200
    assert '[Start here](https://genizahsearch.com/start)' in llms.text

    sitemap = client.get('/sitemap-static.xml')
    assert sitemap.status_code == 200
    assert '<loc>https://genizahsearch.com/start</loc>' in sitemap.text


def test_homepage_has_native_start_link_and_bounded_event_properties():
    source = Path('web/pages/home.py').read_text(encoding='utf-8')

    assert "ui.link(target='/start')" in source
    assert "'welcome_action_clicked'" in source
    assert "'route_id': 'home'" in source
    assert "'action_id': 'home_start_here'" in source
    assert "'difficulty': 'introductory'" in source
    assert "What's on this website? Start here to explore the Cairo Genizah" in source
    assert 'background: var(--primary-700); color: var(--text-inverse)' in source
    assert 'Not sure what to search? Start here' not in source
    assert "'query'" not in source[source.index("'welcome_action_clicked'"):source.index("'welcome_action_clicked'") + 500]
    assert "'shelfmark'" not in source[source.index("'welcome_action_clicked'"):source.index("'welcome_action_clicked'") + 500]
