# -*- coding: utf-8 -*-
"""ux_timing: user-perceived latency of page turns, searches, Quick View and saves.

The server half (web/ux_timing.py) tells the page's browser when an update has
been sent; web/static/js/ux_timing.js measures from the user's click to the next
paint. These tests pin the server half: what it sends, that it never sends
through ``ui.context`` (a page-less task would lose it, as browse_manuscript was
lost), that every call site and trigger names a registered workflow, and that a
real /browse load and page turn report themselves after Phase A.
"""

from __future__ import annotations

import ast
import asyncio
import itertools
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation

import web.ux_timing as ux

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / 'web'
_paths = itertools.count()


class _Recorder:
    def __init__(self):
        self.code = []

    def run_javascript(self, code, **_kwargs):
        self.code.append(code)


@pytest.fixture
def measuring(monkeypatch):
    monkeypatch.setenv('POSTHOG_API_KEY', 'phc_test')
    monkeypatch.delenv('UX_TIMING_ENABLED', raising=False)
    monkeypatch.delenv('UX_TIMING_SAMPLE_RATE', raising=False)


def test_ux_done_sends_one_escaped_call(measuring):
    client = _Recorder()
    ux.ux_done(client, 'browse_page', 'ok', server_ms=12.345, nav=True, img_selector="img.a'b")
    assert len(client.code) == 1
    args = json.loads('[' + client.code[0][len('window.gsUx&&gsUx.done('):-1] + ']')
    assert args == ['browse_page', 'ok', 12.3, True, "img.a'b"]


@pytest.mark.parametrize('workflow,outcome', [('nope', 'ok'), ('browse_page', 'nope')])
def test_unknown_names_are_not_sent(measuring, workflow, outcome):
    client = _Recorder()
    ux.ux_done(client, workflow, outcome)
    assert client.code == []


def test_nothing_is_sent_when_switched_off(monkeypatch):
    client = _Recorder()
    monkeypatch.delenv('POSTHOG_API_KEY', raising=False)
    ux.ux_done(client, 'browse_page', 'ok')
    monkeypatch.setenv('POSTHOG_API_KEY', 'phc_test')
    monkeypatch.setenv('UX_TIMING_ENABLED', '0')
    ux.ux_done(client, 'browse_page', 'ok')
    assert client.code == []
    assert ux.head_html() == ''


def test_a_gone_page_never_raises(measuring):
    class _Gone:
        def run_javascript(self, *_a, **_k):
            raise RuntimeError('The client this element belongs to has been deleted.')

    ux.ux_done(_Gone(), 'browse_page', 'ok')
    ux.ux_done(None, 'browse_page', 'ok')


def test_head_html_loads_the_browser_side(measuring, monkeypatch):
    monkeypatch.setenv('UX_TIMING_SAMPLE_RATE', '0.25')
    html = ux.head_html()
    config = json.loads(re.search(r'window\.GS_UX=(\{.*?\});</script>', html).group(1))
    from shared.config import Config
    # A search may run to the website's limit before its first rows; the
    # browser must keep its start that long (Codex 2026-10-21, P2).
    assert config == {'rate': 0.25, 'v': 1,
                      'wait': {'search_results': (Config.WEB_SEARCH_TIME_LIMIT + 30) * 1000}}
    assert re.search(r'src="/static/js/ux_timing\.js\?v=[0-9a-f]{10}" defer', html)
    assert (WEB / 'static' / 'js' / 'ux_timing.js').is_file()


def _ux_done_calls():
    for path in sorted(WEB.rglob('*.py')):
        if path.name == 'ux_timing.py':
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
            if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'ux_done':
                yield path.relative_to(ROOT).as_posix(), node


def _literals(node):
    if isinstance(node, ast.Constant):
        return {node.value}
    if isinstance(node, ast.IfExp):
        return _literals(node.body) | _literals(node.orelse)
    return {None}  # not a literal: refused by the check


def test_every_call_site_passes_a_client_and_registered_names():
    calls = list(_ux_done_calls())
    assert len(calls) >= 5, 'call sites went missing'
    for where, call in calls:
        assert len(call.args) >= 3, f'{where}:{call.lineno} must pass client, workflow, outcome'
        assert _literals(call.args[1]) <= ux.WORKFLOWS, f'{where}:{call.lineno}'
        if getattr(call.args[2], 'id', None) == 'outcome':
            continue  # a local helper forwarding its own literal outcomes (checked below)
        assert _literals(call.args[2]) <= ux.OUTCOMES, f'{where}:{call.lineno}'


def test_search_outcomes_are_registered():
    source = (WEB / 'pages' / 'search.py').read_text(encoding='utf-8')
    outcomes = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == '_ux_first_rows':
            outcomes |= _literals(node.args[0])
    assert outcomes and outcomes <= ux.OUTCOMES, outcomes


def test_every_trigger_names_a_registered_workflow():
    found = set()
    for path in WEB.rglob('*.py'):
        found |= set(re.findall(r'data-ux(?:-enter)?="?([a-z_]+)', path.read_text(encoding='utf-8')))
    assert found, 'no triggers found'
    assert found <= ux.WORKFLOWS, found - ux.WORKFLOWS
    assert found >= {'browse_page', 'search_results', 'result_quick_view', 'save_list_add', 'save_comment'}


def test_ux_ignored_sends_one_call_and_never_raises(measuring):
    client = _Recorder()
    ux.ux_ignored(client, 'search_results')
    ux.ux_ignored(client, 'nope')
    assert client.code == ['window.gsUx&&gsUx.ignored&&gsUx.ignored("search_results")']

    class _Gone:
        def run_javascript(self, *_a, **_k):
            raise RuntimeError('The client this element belongs to has been deleted.')

    ux.ux_ignored(_Gone(), 'search_results')
    ux.ux_ignored(None, 'search_results')


def test_a_search_turned_away_while_one_runs_tells_the_browser():
    """execute_search ignores a submit while a search runs; the browser had
    already marked that Enter as the start (Codex 2026-10-21, P2)."""
    source = (WEB / 'pages' / 'search.py').read_text(encoding='utf-8')
    execute = [node for node in ast.walk(ast.parse(source))
               if isinstance(node, ast.AsyncFunctionDef) and node.name == 'execute_search']
    assert len(execute) == 1
    guard = execute[0].body[0]
    assert isinstance(guard, ast.If) and ast.unparse(guard.test) == 'search_state.is_running', \
        'execute_search no longer opens with its double-submit guard'
    calls = [ast.unparse(n) for n in ast.walk(guard) if isinstance(n, ast.Call)]
    assert "ux_ignored(_page_client, 'search_results')" in calls, ast.unparse(guard)


# --- the browser side, in Node --------------------------------------------------------------

_NODE_HARNESS = r"""
const fs = require('fs'), vm = require('vm');
const src = fs.readFileSync(process.argv[2], 'utf8');
const cases = JSON.parse(fs.readFileSync(0, 'utf8'));
const out = [];
for (const steps of cases) {
  let t = 0; const timers = []; const listeners = {}; const captured = [];
  const win = {GS_UX: {rate: 1, v: 1, wait: {search_results: 210000}},
               posthog: {capture: (name, props) => captured.push(props)}};
  const ctx = {
    window: win, Math, URL,
    document: {addEventListener: (type, fn) => { listeners[type] = fn; },
               querySelector: () => null, visibilityState: 'visible', referrer: ''},
    performance: {now: () => t, getEntriesByType: () => []},
    requestAnimationFrame: fn => timers.push({at: t, fn}),
    setTimeout: (fn, ms) => timers.push({at: t + (ms || 0), fn}),
    navigator: {webdriver: false}, location: {origin: 'http://x'},
  };
  vm.runInNewContext(src, ctx);
  function flush() {
    while (timers.length) {
      timers.sort((a, b) => a.at - b.at);
      const next = timers.shift();
      t = Math.max(t, next.at);
      next.fn();
    }
  }
  for (const [at, op, workflow] of steps) {
    t = at;
    if (op === 'enter') {
      listeners.keydown({key: 'Enter', isComposing: false, isTrusted: true, timeStamp: t,
        target: {closest: sel => sel === '[data-ux-enter]' ? {getAttribute: () => workflow} : null}});
    } else if (op === 'click') {
      listeners.click({isTrusted: true, timeStamp: t,
        target: {closest: sel => sel === '[data-ux]' ? {getAttribute: () => workflow} : null}});
    } else if (op === 'ignored') {
      win.gsUx.ignored(workflow);
    } else if (op === 'done') {
      win.gsUx.done(workflow, 'ok', 100, false, null);
      flush();
    }
  }
  out.push(captured.map(p => [p.workflow, p.total_ms]));
}
process.stdout.write(JSON.stringify(out));
"""


@pytest.mark.skipif(shutil.which('node') is None, reason='node is not installed')
def test_browser_side_measures_from_the_action_the_server_is_working_on(tmp_path):
    """Codex 2026-10-21 (two P2s): a second Enter during a search must not
    restart the clock, and a search slower than 60 s must still be measured."""
    harness = tmp_path / 'harness.js'
    harness.write_text(_NODE_HARNESS, encoding='utf-8')
    S = 'search_results'
    cases = [
        # Enter, Enter again at 9 s (turned away), first rows at 10 s.
        [[0, 'enter', S], [9000, 'enter', S], [9001, 'ignored', S], [10000, 'done', S]],
        # Two accepted searches: each measured from its own Enter.
        [[0, 'enter', S], [1000, 'done', S], [5000, 'enter', S], [6500, 'done', S]],
        # Turned away twice in a row: still the first Enter.
        [[0, 'enter', S], [4000, 'enter', S], [4001, 'ignored', S],
         [6000, 'enter', S], [6001, 'ignored', S], [8000, 'done', S]],
        # Two extra Enters before either refusal arrives (Codex round 2).
        [[0, 'enter', S], [4000, 'enter', S], [4100, 'enter', S],
         [4200, 'ignored', S], [4300, 'ignored', S], [10000, 'done', S]],
        # A turned-away submit with nothing before it measures nothing.
        [[0, 'enter', S], [1, 'ignored', S], [2000, 'done', S]],
        # First rows after 70 s: kept for a search, dropped for a page turn.
        [[0, 'enter', S], [70000, 'done', S]],
        [[0, 'click', 'browse_page'], [70000, 'done', 'browse_page']],
        # A held key: more extra Enters than the browser keeps (Codex round 3). That
        # search goes unmeasured rather than timed from a later Enter; the next is timed.
        ([[0, 'enter', S]] + [[1000 + i, 'enter', S] for i in range(205)]
         + [[3000 + i, 'ignored', S] for i in range(205)]
         + [[10000, 'done', S], [12000, 'enter', S], [13000, 'done', S]]),
    ]
    proc = subprocess.run(['node', str(harness), str(WEB / 'static' / 'js' / 'ux_timing.js')],
                          input=json.dumps(cases), capture_output=True, text=True,
                          encoding='utf-8', timeout=60)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)
    assert out[0] == [[S, 10000]], out[0]
    assert out[1] == [[S, 1000], [S, 1500]], out[1]
    assert out[2] == [[S, 8000]], out[2]
    assert out[3] == [[S, 10000]], out[3]
    assert out[4] == [], out[4]
    assert out[5] == [[S, 70000]], out[5]
    assert out[6] == [], out[6]
    assert out[7] == [[S, 1000]], out[7]


# --- a real /browse load and page turn ------------------------------------------------------


@pytest.fixture
def supabase_offline(monkeypatch):
    """Any Supabase client the page builds points at a refused local port: no network."""
    import shared.supabase_provider as provider
    import web.supabase_client as supabase_client

    for module in (supabase_client, provider):
        monkeypatch.setattr(module, 'SUPABASE_URL', 'http://127.0.0.1:9', raising=False)
        monkeypatch.setattr(module, '_client', None, raising=False)
    monkeypatch.setattr(supabase_client, '_public_read_client', None)


def test_a_browse_load_and_a_page_turn_report_after_phase_a(measuring, monkeypatch, supabase_offline):
    from web.services import BrowsePage
    import web.pages.browse as browse

    def page(p):
        return BrowsePage(uid=f'u{p}', p_num=p, text='שלום', full_header='hdr',
                          total_pages=3, current_idx=p, sys_id='990000000000017',
                          shelfmark='T-S 1.1', fl_id=f'FL{p}')

    class _Service:
        is_ready = True

        def get_browse_page(self, sys_id, p_num=1, direction=0, volume_ie=None, **_kwargs):
            return page(max(1, min(3, (p_num or 1) + (direction or 0))))

        def get_browse_page_by_fl(self, fl_id, sys_id=None):
            return page(1)

        def __getattr__(self, _name):
            return lambda *a, **k: None

    monkeypatch.setattr(browse, 'get_service', lambda: _Service())
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()
    seen = {}

    def dones(client):
        return [m[2]['code'] for m in client.outbox.messages
                if m[1] == 'run_javascript' and 'gsUx.done(' in m[2].get('code', '')]

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_ux_timing_browse_{next(_paths)}')) as client:
            browse.create_browse_page(initial_sys_id='990000000000017')
        await asyncio.sleep(1.0)
        seen['bootstrap'] = dones(client)
        nexts = [e for e in client.elements.values() if isinstance(e, ui.button)
                 and str(e._props.get('data-action', '')).strip('"') == 'next']
        seen['next_has_trigger'] = bool(nexts) and str(nexts[0]._props.get('data-ux', '')).strip('"') == 'browse_page'
        for listener in nexts[0]._event_listeners.values():
            if listener.type == 'click':
                listener.handler(None)
        await asyncio.sleep(1.0)
        seen['after_click'] = dones(client)[len(seen['bootstrap']):]

    asyncio.run(_run())
    assert len(seen['bootstrap']) == 1 and seen['bootstrap'][0].startswith(
        'window.gsUx&&gsUx.done("browse_page","ok",')
    assert ',true,' in seen['bootstrap'][0], 'the URL-started load must be measured from navigation'
    assert seen['next_has_trigger']
    assert len(seen['after_click']) == 1 and ',false,' in seen['after_click'][0]
