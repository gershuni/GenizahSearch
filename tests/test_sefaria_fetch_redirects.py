# -*- coding: utf-8 -*-
"""Sefaria fetches in the web process follow redirects one checked hop at a time.

The web server saves the Sefaria table of contents (sefaria_toc.json) and each
reference's text for every later visitor, so a redirect may only move between
Sefaria's own hosts. The web asks for this explicitly (its own library instance
and checked=True on each text fetch), so there is no window at start-up in which
it fetches unchecked. The desktop keeps its plain requests call.
"""
from __future__ import annotations

import ast
import os
import pathlib
from urllib.parse import urljoin

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'sefaria-redirect-test-secret-0123456789abcd')

REPO = pathlib.Path(__file__).resolve().parents[1]
OTHER = 'https://other.example.org/api/index/'


class _Resp:
    def __init__(self, status, payload=None, location=None):
        self.status_code = status
        self._payload = payload
        self.headers = {'Content-Type': 'application/json'}
        if location:
            self.headers['Location'] = location
        self.is_redirect = bool(location)
        self.next = None

    def json(self):
        return self._payload

    def close(self):
        pass


def _fake_get(routes, calls):
    def get(url, *args, allow_redirects=True, **kwargs):
        calls.append((url, allow_redirects, dict(kwargs)))
        resp = routes[url]
        while allow_redirects and resp.status_code in (301, 302, 303, 307, 308):
            url = urljoin(url, resp.headers['Location'])
            calls.append((url, 'followed by requests', {}))
            resp = routes[url]
        return resp
    return get


@pytest.fixture
def su(tmp_path, monkeypatch):
    import shared.sefaria_utils as module
    home = tmp_path / 'home'
    home.mkdir()
    monkeypatch.setenv('HOME', str(home))
    monkeypatch.setenv('USERPROFILE', str(home))
    monkeypatch.setattr(module, 'get_cache_dir', lambda: str(tmp_path / 'sefaria_cache'))
    return module


def _toc_file(tmp_path):
    return tmp_path / 'sefaria_cache' / 'sefaria_toc.json'


def test_web_table_of_contents_is_not_taken_from_a_redirect_off_sefaria(su, tmp_path,
                                                                        monkeypatch):
    import requests
    calls = []
    monkeypatch.setattr(requests, 'get', _fake_get({
        su.SefariaLibraryManager.TOC_URL: _Resp(302, location=OTHER),
        OTHER: _Resp(200, [{'category': 'Other catalogue'}]),
    }, calls))

    toc = su.SefariaLibraryManager(checked_fetches=True).get_toc()

    assert toc is None
    assert not any('example.org' in url for url, _, _ in calls)
    assert all(flag is False for _, flag, _ in calls)
    assert not _toc_file(tmp_path).exists()


def test_web_table_of_contents_follows_a_redirect_within_sefaria(su, tmp_path, monkeypatch):
    import requests
    moved = 'https://sefaria.org/api/index/'
    calls = []
    monkeypatch.setattr(requests, 'get', _fake_get({
        su.SefariaLibraryManager.TOC_URL: _Resp(301, location=moved),
        moved: _Resp(200, [{'category': 'Tanakh'}]),
    }, calls))

    toc = su.SefariaLibraryManager(checked_fetches=True).get_toc()

    assert toc == [{'category': 'Tanakh'}]
    assert [(u, f) for u, f, _ in calls] == [(su.SefariaLibraryManager.TOC_URL, False), (moved, False)]
    assert _toc_file(tmp_path).exists()


def test_desktop_table_of_contents_call_is_unchanged(su, tmp_path, monkeypatch):
    import requests
    calls = []
    monkeypatch.setattr(requests, 'get', _fake_get({
        su.SefariaLibraryManager.TOC_URL: _Resp(200, [{'category': 'Tanakh'}]),
    }, calls))

    assert su.SefariaLibraryManager().get_toc() == [{'category': 'Tanakh'}]
    assert calls == [(su.SefariaLibraryManager.TOC_URL, True, {'timeout': 30})]


def test_web_reference_text_is_not_taken_from_a_redirect_off_sefaria(su, tmp_path,
                                                                     monkeypatch):
    import requests
    from web.pages import parallels
    ref = 'Mishnah Berakhot 1:1'
    url = 'https://www.sefaria.org/api/texts/Mishnah%20Berakhot%201:1?context=0&pad=0'
    other = 'https://other.example.org/texts/x'
    calls = []
    monkeypatch.setattr(requests, 'get', _fake_get({
        url: _Resp(302, location=other),
        other: _Resp(200, {'he': ['טקסט אחר']}),
    }, calls))

    got = parallels.fetch_sefaria_text(ref)

    assert got == ''
    assert not any('example.org' in u for u, _, _ in calls)
    assert not su.read_sefaria_cache(ref)   # nothing saved for the next visitor


def test_the_web_library_instance_is_its_own_and_checked(su):
    web_library = su.get_sefaria_library(checked_fetches=True)
    desktop_library = su.get_sefaria_library()
    assert web_library is not desktop_library
    assert web_library.checked_fetches is True
    assert desktop_library.checked_fetches is False
    assert su.get_sefaria_library(checked_fetches=True) is web_library


def _calls_in(path, name):
    tree = ast.parse(path.read_text(encoding='utf-8'))
    return [node for node in ast.walk(tree) if isinstance(node, ast.Call)
            and (getattr(node.func, 'id', None) == name or getattr(node.func, 'attr', None) == name)]


def _kw_true(call, keyword):
    return any(kw.arg == keyword and isinstance(kw.value, ast.Constant) and kw.value.value is True
               for kw in call.keywords)


def test_every_web_sefaria_fetch_asks_for_checked_hops():
    """Every call under web/ asks for the checked form: the library instance
    with checked_fetches=True and each text fetch with checked=True."""
    web_files = sorted((REPO / 'web').rglob('*.py'))
    libs = [(f, c) for f in web_files for c in _calls_in(f, 'get_sefaria_library')]
    gets = [(f, c) for f in web_files for c in _calls_in(f, 'get_sefaria_json')]
    assert libs and gets, 'no web Sefaria call found -- the check would prove nothing'
    bad = [f'{f.relative_to(REPO)}:{c.lineno}' for f, c in libs if not _kw_true(c, 'checked_fetches')]
    bad += [f'{f.relative_to(REPO)}:{c.lineno}' for f, c in gets if not _kw_true(c, 'checked')]
    assert not bad, 'web Sefaria calls without the checked form: ' + ', '.join(bad)


@pytest.mark.parametrize('url, ok', [
    ('https://www.sefaria.org/api/index/', True),
    ('https://sefaria.org/api/index/', True),
    ('http://www.sefaria.org/api/index/', False),
    ('https://notsefaria.org/api/index/', False),
    ('https://www.sefaria.org.example.org/api/', False),
    ('https://user@www.sefaria.org/api/', False),
    ('https://www.sefaria.org:8443/api/', False),
])
def test_sefaria_host_check(su, url, ok):
    assert su.is_sefaria_url(url) is ok
