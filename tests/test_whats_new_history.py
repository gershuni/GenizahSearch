# -*- coding: utf-8 -*-
"""What's New in every version (shared/whats_new_history.py, 2026-11-05): the entries, the
reader functions, GET /api/whats-new, and the desktop dialog's HTML.

The release gate: every release adds its entry, so ``version.APP_VERSION`` must be listed.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared import whats_new_history as wnh
from version import APP_VERSION


def _release(version, items, apps=('web', 'desktop'), date='2026-11-05'):
    return {'version': version, 'date': date, 'apps': list(apps),
            'title': {'en': f'T {version}', 'he': f'כ {version}'}, 'items': items}


def _item(category='feature', apps=('web', 'desktop'), en='x', he='א'):
    return {'category': category, 'apps': list(apps), 'en': en, 'he': he}


# --- the entries ------------------------------------------------------------------

def test_the_entries_are_sound():
    assert wnh.problems() == []


def test_the_current_version_has_its_entry():
    """Each release adds its entry with its release texts (owner, 2026-11-05)."""
    assert APP_VERSION in [r['version'] for r in wnh.releases()], (
        f'shared/whats_new_data.py has no entry for {APP_VERSION}')


def test_the_history_starts_at_9_0_0():
    assert wnh.version_key(wnh.releases()[-1]['version']) >= wnh.version_key(wnh.FIRST_VERSION)


@pytest.mark.parametrize('bad, says', [
    ([_release('9.1.0', [_item()]), _release('9.2.0', [_item()])], 'not newest first'),
    ([_release('9.1.0', [_item()]), _release('9.1.0', [_item()])], 'listed twice'),
    ([_release('8.9.0', [_item()])], 'older than 9.0.0'),
    ([_release('9.1', [_item()])], 'not X.Y.Z'),
    ([_release('9.1.0', [_item(category='news')])], "category 'news'"),
    ([_release('9.1.0', [_item(he='')])], 'needs English and Hebrew'),
    ([_release('9.1.0', [_item(apps=('desktop',))], apps=('web',))], 'the release shipped to'),
    ([_release('9.1.0', [])], 'no items'),
    ([_release('9.1.0', [_item()], date='5.11.2026')], 'is not YYYY-MM-DD'),
    ([_release('9.1.0', [dict(_item(), he_source='draft')])], 'unknown fields'),
])
def test_problems_names_each_fault(bad, says):
    found = wnh.problems(bad)
    assert any(says in p for p in found), found


# --- the reader functions ----------------------------------------------------------

def test_version_key():
    assert wnh.version_key('9.13.1') == (9, 13, 1)
    assert wnh.version_key('v9.10.0') == (9, 10, 0)
    assert wnh.version_key('9.10.0') > wnh.version_key('9.9.0')
    for bad in ('', '9.1', 'x.y.z', '9.1.0.0', None):
        with pytest.raises(ValueError):
            wnh.version_key(bad)


@pytest.fixture
def fake_history(monkeypatch):
    history = [
        _release('9.3.0', [_item('fix', ('web',)), _item('feature', ('web',))], apps=('web',)),
        _release('9.2.0', [_item('improvement', ('desktop',), en='d'), _item('feature', ('web', 'desktop'), en='b')]),
        _release('9.1.0', [_item('feature', ('desktop',))], apps=('desktop',)),
    ]
    monkeypatch.setattr(wnh, 'HISTORY', history)
    return history


def test_releases_newest_first_and_by_app(fake_history):
    assert [r['version'] for r in wnh.releases()] == ['9.3.0', '9.2.0', '9.1.0']
    desktop = wnh.releases('desktop')
    assert [r['version'] for r in desktop] == ['9.2.0', '9.1.0']   # 9.3.0 had nothing for it
    assert [i['en'] for i in desktop[0]['items']] == ['d', 'b']
    assert len(fake_history[1]['items']) == 2, 'the filter must not touch the entries'
    with pytest.raises(ValueError):
        wnh.releases('phone')


def test_a_version_newer_than_the_app_is_not_shown(fake_history, monkeypatch):
    """Codex, PR #482: an entry written before the bump is not announced until the app is
    that version -- on every surface, since they all read releases()."""
    monkeypatch.setattr(wnh, 'current_version', lambda: '9.2.0')
    assert [r['version'] for r in wnh.releases()] == ['9.2.0', '9.1.0']
    assert [r['version'] for r in wnh.releases_between('9.1.0')] == ['9.2.0']
    assert wnh.problems(fake_history) == [], 'an unreleased entry is still a sound entry'
    monkeypatch.setattr(wnh, 'current_version', lambda: '9.3.0')
    assert wnh.releases()[0]['version'] == '9.3.0'


def test_the_api_hides_an_unreleased_version(client, fake_history, monkeypatch):
    monkeypatch.setattr(wnh, 'current_version', lambda: '9.2.0')
    body = client.get('/api/whats-new').json()
    assert body['latest'] == '9.2.0' and [r['version'] for r in body['releases']] == ['9.2.0', '9.1.0']


def test_releases_between_is_after_since_up_to_upto(fake_history):
    assert [r['version'] for r in wnh.releases_between('9.1.0')] == ['9.3.0', '9.2.0']
    assert [r['version'] for r in wnh.releases_between('9.1.0', '9.2.0')] == ['9.2.0']
    assert [r['version'] for r in wnh.releases_between('9.0.0', '9.3.0', app='desktop')] == ['9.2.0', '9.1.0']
    assert wnh.releases_between('9.3.0') == []


def test_localize_orders_the_categories(fake_history):
    view = wnh.localize(wnh.releases()[0], 'he')
    assert [c for c, _ in view['sections']] == ['feature', 'fix']
    assert view['title'] == 'כ 9.3.0' and view['sections'][0][1][0]['text'] == 'א'
    assert wnh.localize(wnh.releases()[0], 'en')['title'] == 'T 9.3.0'


def test_app_note():
    assert wnh.app_note(['web']) == 'Website'
    assert wnh.app_note(['desktop']) == 'Desktop app'
    assert wnh.app_note(['web', 'desktop']) is None


def test_from_public_round_trips_and_refuses_junk():
    payload = {'releases': [wnh.to_public(r) for r in wnh.releases()]}
    assert wnh.from_public(payload) == [wnh.to_public(r) for r in wnh.releases()]
    for junk in (None, [], {'releases': 'x'}, {'releases': [1]},
                 {'releases': [{'version': '9.1.0', 'items': []}]}):
        with pytest.raises(ValueError):
            wnh.from_public(junk)


# --- GET /api/whats-new -------------------------------------------------------------

@pytest.fixture
def client():
    from web.api import init_api_routes
    bare = FastAPI()
    init_api_routes(app_override=bare)
    return TestClient(bare)


def test_the_api_serves_every_release(client):
    resp = client.get('/api/whats-new')
    assert resp.status_code == 200
    body = resp.json()
    assert body['latest'] == wnh.releases()[0]['version']
    assert [r['version'] for r in body['releases']] == [r['version'] for r in wnh.releases()]
    assert 'max-age' in resp.headers.get('cache-control', '')
    assert wnh.from_public(body)  # what the desktop reads


def test_the_api_answers_the_desktop_update_question(client, fake_history):
    resp = client.get('/api/whats-new', params={'since': '9.1.0', 'upto': '9.3.0', 'app': 'desktop'})
    assert resp.status_code == 200
    releases = resp.json()['releases']
    assert [r['version'] for r in releases] == ['9.2.0']
    assert {k for k in releases[0]['items'][0]} == {'category', 'apps', 'en', 'he'}


@pytest.mark.parametrize('params', [{'since': 'nine'}, {'app': 'phone'}, {'upto': '9.1'}])
def test_the_api_refuses_a_bad_question(client, params):
    resp = client.get('/api/whats-new', params=params)
    assert resp.status_code == 400 and resp.json()['error'] == 'invalid_request'


# --- the desktop dialog's HTML (no Qt needed) ----------------------------------------

def test_the_desktop_html_lists_versions_categories_and_app_notes(fake_history):
    from desktop.version_history import render_html
    out = render_html(wnh.releases(), 'en')
    assert out.index('Version 9.3.0') < out.index('Version 9.2.0') < out.index('Version 9.1.0')
    assert 'New features' in out and 'Fixes' in out and 'Improvements' in out
    assert '(Website)' in out and '(Desktop app)' in out
    he = render_html(wnh.releases(), 'he')
    assert 'dir="rtl"' in he


def test_the_desktop_html_escapes_text(monkeypatch):
    from desktop.version_history import render_html
    monkeypatch.setattr(wnh, 'HISTORY', [_release('9.1.0', [_item(en='<b>x</b> & y')])])
    out = render_html(wnh.releases(), 'en')
    assert '&lt;b&gt;x&lt;/b&gt; &amp; y' in out and '<b>x</b>' not in out
