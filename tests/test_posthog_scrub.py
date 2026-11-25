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


def _run_js(body: str) -> dict:
    node = shutil.which('node')
    if not node:
        pytest.skip('node not installed')
    script = 'var window = {};\n' + JS.read_text(encoding='utf-8') + '\n' + body
    out = subprocess.run([node, '-e', script], capture_output=True, text=True,
                         encoding='utf-8', check=True, timeout=30)
    return json.loads(out.stdout)


def test_the_recorder_event_leaves_posthog_own_config_alone():
    """The recorder's first `$snapshot` holds `posthog.config` itself, not a copy of it.

    The scrub once redacted `token` there in place: the page's project key became "[redacted]"
    and every event after the recorder started was rejected, for eight days.
    """
    out = _run_js("""
        var config = {token: 'phc_key', api_host: 'https://eu.i.posthog.com',
                      before_send: window.__ghScrubEvent};
        var sent = window.__ghScrubEvent({event: '$snapshot', properties: {
            token: config.token,
            $snapshot_data: [{type: 5, data: {tag: '$posthog_config', payload: {config: config}}}],
        }});
        var later = window.__ghScrubEvent({event: '$autocapture', properties: {token: config.token}});
        process.stdout.write(JSON.stringify({
            live: config.token,
            recorded: sent.properties.$snapshot_data[0].data.payload.config.token,
            sent: sent.properties.token,
            later: later.properties.token,
        }));
    """)
    assert out == {'live': 'phc_key', 'recorded': '[redacted]', 'sent': 'phc_key',
                   'later': 'phc_key'}


@pytest.mark.parametrize('name', ['$pageleave', 'hebrew_keyboard_used'])
def test_the_event_it_was_given_is_never_written_to(name):
    out = _run_js("""
        var url = %s;
        function freeze(o) { Object.values(o).forEach(function (v) {
            if (v && typeof v === 'object') { freeze(v); } }); return Object.freeze(o); }
        var event = freeze({event: %s, properties: {token: 'phc_key', $current_url: url,
            deep: {access_token: 'a', list: [url]}}, $set: {u: url}, $set_once: {u: url},
            timestamp: new Date(0)});
        var before = JSON.stringify(event);
        var sent = window.__ghScrubEvent(event);
        process.stdout.write(JSON.stringify({same: before === JSON.stringify(event), sent: sent,
            date: sent.timestamp instanceof Date}));
    """ % (json.dumps(CONFIRM_URL), json.dumps(name)))
    assert out['same'] and out['date']
    assert ACCESS not in json.dumps(out['sent'])
    assert out['sent']['properties']['deep']['access_token'] == '[redacted]'
    assert out['sent']['properties']['token'] == 'phc_key'


def test_values_json_writes_its_own_way_are_kept_and_scrubbed():
    out = _run_js("""
        var when = new Date(0);
        var sent = window.__ghScrubEvent({event: 'x', properties: {
            when: when, page: new URL(%s), shared: [when, when]}});
        process.stdout.write(JSON.stringify(sent.properties));
    """ % json.dumps(CONFIRM_URL))
    assert out['when'] == '1970-01-01T00:00:00.000Z'
    assert out['shared'] == [out['when'], out['when']]
    assert ACCESS not in out['page'] and '[redacted]' in out['page']


def test_a_copied_event_serializes_as_the_original_would():
    """Same JSON as the event itself when there is nothing to redact: a toJSON gets its key (and
    is asked again under another one), boxed primitives are their values, a shared object stays."""
    out = _run_js("""
        var byKey = {toJSON: function (key) { return 'key ' + key; }};
        var self = {a: 1, toJSON: function () { return this; }};
        var shared = {x: 1};
        var event = {event: 'x', properties: {
            one: byKey, two: byKey, list: [byKey, byKey], self: self, again: self,
            number: new Number(7), no: new Boolean(false), text: new String('x'),
            boxes: [new Number(0)], shared: shared, twice: [shared, shared],
            gone: {toJSON: function () { return undefined; }}, none: null}};
        var plain = JSON.stringify(event);
        process.stdout.write(JSON.stringify({plain: JSON.parse(plain),
            sent: JSON.parse(JSON.stringify(window.__ghScrubEvent(event)))}));
    """)
    assert out['sent'] == out['plain']
    assert out['sent']['properties']['one'] == 'key one'
    assert out['sent']['properties']['number'] == 7


def test_a_token_in_a_boxed_string_or_a_to_json_value_is_redacted():
    out = _run_js("""
        var url = %s;
        process.stdout.write(JSON.stringify(window.__ghScrubEvent({event: 'x', properties: {
            boxed: new String(url), own: {toJSON: function () { return {u: url}; }}}})));
    """ % json.dumps(CONFIRM_URL))
    assert ACCESS not in json.dumps(out) and REFRESH not in json.dumps(out)


def test_a_to_json_runs_once_and_is_not_on_the_copy():
    """A toJSON that returns `this` would otherwise run again when PostHog serializes the copy,
    and what it adds then has passed no scrub."""
    out = _run_js("""
        var url = %s, calls = 0;
        var value = {toJSON: function () { calls += 1;
            if (calls > 1) { return {late: url}; } return this; }, a: 1};
        var text = JSON.stringify(window.__ghScrubEvent({event: 'x', properties: {v: value}}));
        process.stdout.write(JSON.stringify({calls: calls, sent: JSON.parse(text)}));
    """ % json.dumps(CONFIRM_URL))
    assert out == {'calls': 1, 'sent': {'event': 'x', 'properties': {'v': {'a': 1}}}}
