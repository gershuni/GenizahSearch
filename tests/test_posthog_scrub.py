# -*- coding: utf-8 -*-
"""Sign-in tokens never reach PostHog.

Supabase's confirmation and password-reset links land on the site with the session in the
address (`#access_token=...&refresh_token=...`); PostHog recorded that address in
`$current_url`, `$pageleave` and the person's initial URL. web/static/js/posthog_scrub.js is
PostHog's `before_send` and redacts every token-bearing parameter. Pinned here: the page wires
it in, and, run in Node over the real file, it redacts the tokens and leaves the rest alone.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
JS = ROOT / 'web' / 'static' / 'js' / 'posthog_scrub.js'
MAIN = ROOT / 'web' / 'main.py'

ACCESS = 'eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJl'
REFRESH = 'onuurlbbgvgl'
CONFIRM_URL = (f'https://genizahsearch.com/#access_token={ACCESS}&expires_at=1794819695'
               f'&expires_in=3600&refresh_token={REFRESH}&sb=&token_type=bearer&type=signup')


def test_page_passes_every_event_through_the_scrub():
    src = MAIN.read_text(encoding='utf-8')
    assert "posthog_scrub.js" in src
    assert 'before_send: window.__ghScrubEvent' in src
    # The scrub is defined before PostHog is initialised.
    assert src.index('<script>{_POSTHOG_SCRUB_JS}</script>') < src.index('posthog.init(')


def _run(event: dict) -> object:
    node = shutil.which('node')
    if not node:
        pytest.skip('node not installed')
    script = (JS.read_text(encoding='utf-8')
              + '\nprocess.stdout.write(JSON.stringify(window.__ghScrubEvent('
              + json.dumps(event) + ')));')
    out = subprocess.run([node, '-e', 'var window = {};\n' + script],
                         capture_output=True, text=True, encoding='utf-8', check=True)
    return json.loads(out.stdout)


def test_tokens_are_redacted_everywhere_in_the_event():
    event = {
        'event': '$pageleave',
        'properties': {
            '$current_url': CONFIRM_URL,
            '$referrer': CONFIRM_URL,
            '$snapshot_data': [{'type': 4, 'data': {'href': CONFIRM_URL}}],
        },
        '$set_once': {'$initial_current_url': CONFIRM_URL},
    }
    out = _run(event)
    text = json.dumps(out)
    assert ACCESS not in text
    assert REFRESH not in text
    assert out['properties']['$current_url'] == (
        'https://genizahsearch.com/#access_token=[redacted]&expires_at=1794819695'
        '&expires_in=3600&refresh_token=[redacted]&sb=&token_type=bearer&type=signup')
    assert '[redacted]' in out['properties']['$snapshot_data'][0]['data']['href']
    assert '[redacted]' in out['$set_once']['$initial_current_url']


def test_token_named_properties_are_redacted_by_key():
    event = {
        'event': 'login_success',
        'properties': {
            'access_token': ACCESS,
            'session': {'refresh_token': REFRESH, 'token_type': 'bearer'},
            'method': 'password',
            # PostHog's own project key, which ingestion needs: never touched.
            'token': 'phc_projectkey123',
        },
    }
    out = _run(event)
    assert out['properties']['token'] == 'phc_projectkey123'
    assert out['properties']['access_token'] == '[redacted]'
    assert out['properties']['session'] == {'refresh_token': '[redacted]', 'token_type': 'bearer'}
    assert out['properties']['method'] == 'password'


def test_nested_bare_token_is_redacted():
    out = _run({'event': 'x', 'properties': {'token': 'phc_key', 'session': {'token': REFRESH}}})
    assert out['properties']['token'] == 'phc_key'
    assert out['properties']['session']['token'] == '[redacted]'


def test_deeply_nested_token_is_redacted():
    deep: dict = {'href': CONFIRM_URL}
    for _ in range(40):
        deep = {'childNodes': [deep]}
    out = _run({'event': '$snapshot', 'properties': {'$snapshot_data': deep}})
    text = json.dumps(out)
    assert ACCESS not in text and REFRESH not in text


def test_a_cycle_does_not_hang_or_drop_the_event():
    node = shutil.which('node')
    if not node:
        pytest.skip('node not installed')
    script = ('var window = {};\n' + JS.read_text(encoding='utf-8')
              + "\nvar p = {'$current_url': " + json.dumps(CONFIRM_URL) + '}; p.self = p;'
              + '\nvar out = window.__ghScrubEvent({event: "x", properties: p});'
              + '\nprocess.stdout.write(out.properties.$current_url);')
    out = subprocess.run([node, '-e', script], capture_output=True, text=True,
                         encoding='utf-8', check=True, timeout=30)
    assert ACCESS not in out.stdout and '[redacted]' in out.stdout


@pytest.mark.parametrize('name', ['hebrew_keyboard_opened', 'hebrew_keyboard_used'])
def test_keyboard_events_carry_no_query_in_any_address(name):
    search = 'https://genizahsearch.com/search?q=%D7%A9%D7%9C%D7%95%D7%9D&mode=exact#x'
    event = {
        'event': name,
        'properties': {
            '$current_url': search,
            '$referrer': search,
            '$session_entry_url': search,
            '$pathname': '/search',
            'field': 'main_search',
        },
        '$set_once': {'$initial_current_url': search},
    }
    out = _run(event)
    assert '%D7' not in json.dumps(out)
    assert out['properties']['$current_url'] == 'https://genizahsearch.com/search'
    assert out['properties']['$referrer'] == 'https://genizahsearch.com/search'
    assert out['properties']['$pathname'] == '/search'
    assert out['properties']['field'] == 'main_search'
    assert out['$set_once']['$initial_current_url'] == 'https://genizahsearch.com/search'


def test_ordinary_events_are_unchanged():
    event = {
        'event': '$pageview',
        'properties': {
            '$current_url': 'https://genizahsearch.com/search?q=T-S%2012.123&mode=Shelfmark',
            'platform': 'web',
            'count': 3,
            'nested': {'list': ['אבג', None, True]},
        },
    }
    assert _run(event) == event
