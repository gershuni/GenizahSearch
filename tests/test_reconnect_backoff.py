# -*- coding: utf-8 -*-
"""Reconnect backoff: a website restart must not reload every open tab at once.

Tracker: "A website restart can set off reload loops from open pages, and the
site then stays saturated". During the restart every tab's socket drops and
socket.io retries on a rigid 5 s cadence; when the server is back, each tab's
implicit handshake carries a client id the new process never had, NiceGUI
refuses it and nicegui.js reloads AT ONCE -- every tab within one 5 s window,
each a full page build on the one event loop -- and the saturated server's late
answers ("timeout") reload them again. nicegui.js cannot be changed (immutable
cached URL) and ``window.location.reload`` cannot be overridden, so
web/static/js/reconnect_backoff.js runs in ``<head>`` before the deferred
socket.io.min.js and intercepts at ``window.io`` and the socket's ``on``.

Pinned here: the server half (web/reconnect_backoff.py) inlines the script and
its config and is wired into every page; the browser half, run in Node over the
real file, delays the right handlers by the right amounts and leaves the rest
alone; and the strings in the INSTALLED nicegui.js and template that the whole
mechanism depends on are still there, so a NiceGUI upgrade fails CI loudly
instead of silently restoring the storm.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import nicegui
import pytest

import web.reconnect_backoff as rb

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / 'web'
JS = WEB / 'static' / 'js' / 'reconnect_backoff.js'
NICEGUI = Path(nicegui.__file__).resolve().parent

IHF = 'Implicit handshake failed'


@pytest.fixture
def defaults(monkeypatch):
    for name in ('RECONNECT_BACKOFF_ENABLED', 'RECONNECT_BACKOFF_BASE_MS',
                 'RECONNECT_BACKOFF_CAP_MS', 'RECONNECT_DELAY_MAX_MS'):
        monkeypatch.delenv(name, raising=False)


# --- the server half ----------------------------------------------------------------------


def test_head_html_inlines_the_script_after_its_config(defaults):
    html = rb.head_html()
    config = json.loads(re.search(r'<script>window\.GS_RECONNECT=(\{.*?\});</script>', html).group(1))
    assert config == {'base_ms': 1500, 'cap_ms': 60000, 'delay_max_ms': 0}  # 0: socket.io's own 5 s
    assert JS.read_text(encoding='utf-8') in html, 'the script must be inlined, not fetched'
    assert html.index('GS_RECONNECT=') < html.index("defineProperty(window, 'io'"), \
        'the config must be set before the script reads it'
    # A src with defer would run AFTER the deferred socket.io.min.js, too late to wrap io.
    assert re.findall(r'<script[^>]*>', html) == ['<script>', '<script>']
    assert html.count('</script>') == 2


def test_head_html_reads_its_numbers_from_the_environment(defaults, monkeypatch):
    monkeypatch.setenv('RECONNECT_BACKOFF_BASE_MS', '2000')
    monkeypatch.setenv('RECONNECT_BACKOFF_CAP_MS', '90000')
    monkeypatch.setenv('RECONNECT_DELAY_MAX_MS', '20000')
    assert rb.config() == {'base_ms': 2000, 'cap_ms': 90000, 'delay_max_ms': 20000}
    monkeypatch.setenv('RECONNECT_BACKOFF_BASE_MS', 'soon')
    monkeypatch.setenv('RECONNECT_BACKOFF_CAP_MS', '0')
    assert rb.config()['base_ms'] == 1500 and rb.config()['cap_ms'] == 60000


@pytest.mark.parametrize('value', ['0', 'false', 'off', 'No'])
def test_nothing_is_served_when_switched_off(defaults, monkeypatch, value):
    monkeypatch.setenv('RECONNECT_BACKOFF_ENABLED', value)
    assert not rb.enabled()
    assert rb.head_html() == ''


def test_the_backoff_does_not_depend_on_posthog(defaults, monkeypatch):
    """It is not analytics: a deployment with no PostHog key still spreads its reloads."""
    monkeypatch.delenv('POSTHOG_API_KEY', raising=False)
    assert rb.enabled()
    assert 'GS_RECONNECT' in rb.head_html()


def test_web_main_adds_the_snippet_to_every_page():
    """Shared head HTML precedes every page's own head (nicegui/client.py ``head_html``),
    so the 2 of 24 routes that do not add POSTHOG_SCRIPT are covered too."""
    source = (WEB / 'main.py').read_text(encoding='utf-8')
    assert 'ui.add_head_html(_reconnect_backoff_head_html(), shared=True)' in source
    assert 'from web.reconnect_backoff import head_html as _reconnect_backoff_head_html' in source


# --- the installed NiceGUI the browser half relies on ---------------------------------------


def test_the_installed_nicegui_still_has_the_seams_this_relies_on():
    """A NiceGUI upgrade that renames these must fail here, not in production."""
    js = (NICEGUI / 'static' / 'nicegui.js').read_text(encoding='utf-8')
    assert 'Implicit handshake failed' in js, 'the implicit-handshake refusal message changed'
    assert '"timeout"' in js, 'the connect timeout branch changed'
    assert 'socket.on(' in js, 'handlers are no longer registered through socket.on'
    assert 'window.location.reload()' in js
    sio = (NICEGUI / 'static' / 'socket.io.min.js').read_text(encoding='utf-8')
    assert '.io=' in sio, 'socket.io.min.js no longer assigns globalThis.io (the accessor would miss it)'
    template = (NICEGUI / 'templates' / 'index.html').read_text(encoding='utf-8')
    assert 0 < template.index('{{ head_html | safe }}') < template.index('socket.io.min.js'), \
        'head HTML must be emitted before the deferred socket.io.min.js'
    server = (NICEGUI / 'nicegui.py').read_text(encoding='utf-8')
    assert "ConnectionRefusedError('Implicit handshake failed')" in server


# --- the browser half, in Node ------------------------------------------------------------------

_NODE_HARNESS = r"""
const fs = require('fs'), vm = require('vm');
const src = fs.readFileSync(process.argv[2], 'utf8');
const cases = JSON.parse(fs.readFileSync(0, 'utf8'));
const out = [];
for (const c of cases) {
  // c: {steps, rnd, cfg, hidden, storage: 'throws' | {key: value}, posthog, installs, opts}
  let t = 0; const timers = []; const events = []; const captured = [];
  const store = {}; const storageThrows = c.storage === 'throws';
  if (c.storage && typeof c.storage === 'object') Object.assign(store, c.storage);
  const sessionStorage = {
    getItem: (k) => { if (storageThrows) throw new Error('SecurityError'); return k in store ? store[k] : null; },
    setItem: (k, v) => { if (storageThrows) throw new Error('SecurityError'); store[k] = String(v); },
  };
  const docListeners = {};
  const document = {
    hidden: !!c.hidden,
    addEventListener: (type, fn) => { (docListeners[type] = docListeners[type] || []).push(fn); },
    removeEventListener: (type, fn) => { docListeners[type] = (docListeners[type] || []).filter((f) => f !== fn); },
  };
  const win = { sessionStorage, document };
  if (c.cfg) win.GS_RECONNECT = c.cfg;
  if (c.posthog) win.posthog = { capture: (name, props) => captured.push([name, props]) };
  const M = Object.create(Math); M.random = () => (c.rnd === undefined ? 0.5 : c.rnd);
  let nextTimer = 1;
  const ctx = vm.createContext({
    window: win, document, Math: M,
    setTimeout: (fn, ms) => { const id = nextTimer++; timers.push({ id, at: t + (ms || 0), fn }); return id; },
    clearTimeout: (id) => { const i = timers.findIndex((x) => x.id === id); if (i >= 0) timers.splice(i, 1); },
  });
  for (let i = 0; i < (c.installs || 1); i++) vm.runInContext(src, ctx);  // the <head> script
  // socket.io.min.js loads: `globalThis.io = ...`
  const ioCalls = [];
  const fakeIo = function (url, opts) {
    ioCalls.push(JSON.parse(JSON.stringify(opts || {})));
    // socket.io 4.8 turns the transport names in the options it is given into transport
    // classes, IN PLACE (seen in Chromium): a wrapper must read them before calling io().
    if (opts && Array.isArray(opts.transports)) opts.transports = opts.transports.map(() => function () {});
    const listeners = {};
    return {
      on(ev, fn) { (listeners[ev] = listeners[ev] || []).push(fn); return this; },
      emitReserved(ev, ...args) { for (const fn of (listeners[ev] || []).slice()) fn.apply(this, args); },
    };
  };
  fakeIo.protocol = 5;
  win.io = fakeIo;
  // nicegui.js: window.socket = io(url, {...}); window.socket.on(event, handler)
  const socket = win.io('ws://x', c.opts || { path: '/_nicegui_ws/socket.io', query: { client_id: 'c1' } });
  socket.on('connect_error', function (err) { events.push([t, err.message]); });
  const events2 = [];
  if (c.listeners === 2) socket.on('connect_error', function (err) { events2.push([t, err.message]); });
  function runUntil(limit) {
    while (timers.length) {
      timers.sort((a, b) => a.at - b.at);
      if (timers[0].at > limit) break;
      const next = timers.shift();
      t = Math.max(t, next.at);
      next.fn();
    }
  }
  for (const [at, op, arg] of c.steps) {
    runUntil(at);
    t = Math.max(t, at);
    if (op === 'error') socket.emitReserved('connect_error', new Error(arg));
    else if (op === 'connect') socket.emitReserved('connect');
    else if (op === 'disconnect') socket.emitReserved('disconnect', 'transport close');
    else if (op === 'show' || op === 'hide') {
      document.hidden = op === 'hide';
      for (const fn of (docListeners.visibilitychange || []).slice()) fn();
    }
  }
  runUntil(Infinity);
  out.push({ events, events2, captured, storage: store, io_opts: ioCalls, flag: win.__gsReconnectBackoff,
             protocol: win.io.protocol, waiting: (docListeners.visibilitychange || []).length });
}
process.stdout.write(JSON.stringify(out));
"""


def _run_cases(tmp_path, cases):
    harness = tmp_path / 'harness.js'
    harness.write_text(_NODE_HARNESS, encoding='utf-8')
    proc = subprocess.run(['node', str(harness), str(JS)], input=json.dumps(cases),
                          capture_output=True, text=True, encoding='utf-8', timeout=60)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


needs_node = pytest.mark.skipif(shutil.which('node') is None, reason='node is not installed')


@needs_node
def test_a_refused_handshake_reloads_only_after_the_backoff(tmp_path):
    first, second, timeout, other = _run_cases(tmp_path, [
        {'steps': [[0, 'error', IHF]]},
        # The handler ran at 1.5 s (jitter x1.0); the next refusal in the same tab doubles.
        {'steps': [[0, 'error', IHF], [1500, 'error', IHF]]},
        {'steps': [[0, 'error', 'timeout']]},
        {'steps': [[0, 'error', 'xhr poll error'], [10, 'error', 'websocket error']]},
    ])
    assert first['events'] == [[1500, IHF]], first
    assert first['storage'] == {'gs_reload_n': '1'}
    assert second['events'] == [[1500, IHF], [4500, IHF]], second
    assert second['storage'] == {'gs_reload_n': '2'}
    assert timeout['events'] == [[1500, 'timeout']], timeout
    # socket.io's own retry errors during the restart: NiceGUI ignores them, at once.
    assert other['events'] == [[0, 'xhr poll error'], [10, 'websocket error']], other
    assert other['storage'] == {}


@needs_node
def test_the_delay_is_jittered_and_capped(tmp_path):
    low, high, capped, junk = _run_cases(tmp_path, [
        {'steps': [[0, 'error', IHF]], 'rnd': 0},
        {'steps': [[0, 'error', IHF]], 'rnd': 0.9},
        {'steps': [[0, 'error', IHF]], 'storage': {'gs_reload_n': '10'}},   # 1500 * 2^10 > the cap
        {'steps': [[0, 'error', IHF]], 'storage': {'gs_reload_n': 'junk'}},
    ])
    assert low['events'] == [[750, IHF]]      # x0.5
    assert high['events'] == [[2100, IHF]]    # x1.4
    assert capped['events'] == [[60000, IHF]] and capped['storage'] == {'gs_reload_n': '11'}
    assert junk['events'] == [[1500, IHF]] and junk['storage'] == {'gs_reload_n': '1'}


@needs_node
def test_a_hidden_tab_waits_to_be_viewed(tmp_path):
    hidden, shown_then_hidden = _run_cases(tmp_path, [
        {'steps': [[0, 'error', IHF]], 'hidden': True, 'posthog': True},
        {'steps': [[0, 'error', IHF], [10000, 'show']], 'hidden': True, 'posthog': True},
    ])
    assert hidden['events'] == [] and hidden['waiting'] == 1, hidden
    # Sent when the reload is SCHEDULED (a hidden tab may never be viewed again).
    assert hidden['captured'] == [['reload_scheduled', {
        'reason': IHF, 'attempt': 1, 'delay_ms': 1500, 'hidden': True}]]
    # Viewed at 10 s: the countdown starts then, and the event says the tab had been hidden.
    assert shown_then_hidden['events'] == [[11500, IHF]], shown_then_hidden
    assert shown_then_hidden['captured'] == [['reload_scheduled', {
        'reason': IHF, 'attempt': 1, 'delay_ms': 1500, 'hidden': True}]]
    assert shown_then_hidden['waiting'] == 0, 'the one-shot listener was not removed'


@needs_node
def test_posthog_is_told_when_a_reload_is_scheduled_and_is_optional(tmp_path):
    with_ph, without_ph = _run_cases(tmp_path, [
        {'steps': [[0, 'error', IHF], [1500, 'error', 'timeout']], 'posthog': True},
        {'steps': [[0, 'error', IHF]]},
    ])
    assert with_ph['captured'] == [
        ['reload_scheduled', {'reason': IHF, 'attempt': 1, 'delay_ms': 1500, 'hidden': False}],
        ['reload_scheduled', {'reason': 'timeout', 'attempt': 2, 'delay_ms': 3000, 'hidden': False}],
    ]
    assert without_ph['events'] == [[1500, IHF]]


@needs_node
def test_a_connection_that_stays_up_a_minute_ends_the_run(tmp_path):
    stable, dropped = _run_cases(tmp_path, [
        {'steps': [[0, 'error', IHF], [2000, 'connect'], [70000, 'error', IHF]]},
        {'steps': [[0, 'error', IHF], [2000, 'connect'], [30000, 'disconnect'], [31000, 'error', IHF]]},
    ])
    assert stable['events'] == [[1500, IHF], [71500, IHF]], stable   # back to base after 60 s up
    assert stable['storage'] == {'gs_reload_n': '1'}
    assert dropped['events'] == [[1500, IHF], [34000, IHF]], dropped  # 28 s up: still doubled


@needs_node
def test_session_storage_that_throws_falls_back_to_memory(tmp_path):
    (case,) = _run_cases(tmp_path, [
        {'steps': [[0, 'error', IHF], [1500, 'error', IHF]], 'storage': 'throws'},
    ])
    assert case['events'] == [[1500, IHF], [4500, IHF]], case
    assert case['storage'] == {}


@needs_node
def test_io_gets_the_merged_options_and_the_caller_wins(tmp_path):
    merged, caller, configured = _run_cases(tmp_path, [
        {'steps': []},
        {'steps': [], 'opts': {'path': '/p', 'reconnectionDelayMax': 7, 'randomizationFactor': 0.2}},
        {'steps': [[0, 'error', IHF]], 'cfg': {'base_ms': 100, 'cap_ms': 60000, 'delay_max_ms': 9000}},
    ])
    # socket.io's own reconnection schedule is left alone by default: a 30 s maximum
    # (the server's reconnect_timeout) turned ordinary 15-25 s outages into reloads.
    assert merged['io_opts'] == [{'path': '/_nicegui_ws/socket.io', 'query': {'client_id': 'c1'}}]
    assert merged['protocol'] == 5, "socket.io's own properties (Manager, protocol, ...) must survive"
    assert caller['io_opts'] == [{'reconnectionDelayMax': 7, 'randomizationFactor': 0.2, 'path': '/p'}]
    assert configured['io_opts'] == [{'reconnectionDelayMax': 9000, 'path': '/_nicegui_ws/socket.io',
                                      'query': {'client_id': 'c1'}}]
    assert configured['events'] == [[100, IHF]]


@needs_node
def test_a_second_install_is_a_no_op(tmp_path):
    (case,) = _run_cases(tmp_path, [{'steps': [[0, 'error', IHF]], 'installs': 2}])
    assert case['flag'] is True
    assert len(case['io_opts']) == 1, 'io was wrapped twice'
    assert case['events'] == [[1500, IHF]], 'a doubly wrapped handler would wait twice'


# --- the review's scenarios (2026-10-21; reproduced in Chromium against socket.io 4.8.1) --------

NICEGUI_OPTS = {'path': '/_nicegui_ws/socket.io', 'query': {'client_id': 'c1'},
                'transports': ['websocket', 'polling']}  # what nicegui.js passes: a polling fallback


@needs_node
def test_niceguis_first_timeout_is_its_polling_fallback_and_passes_at_once(tmp_path):
    """nicegui.js answers its FIRST "timeout" by switching to long polling, synchronously,
    before socket.io's next attempt. Delaying it sent that attempt out on the hanging
    websocket again, and the second timeout reloaded: a reload loop behind proxies that
    hang websockets. Only a later timeout (which reloads) is deferred."""
    fallback, websocket_only = _run_cases(tmp_path, [
        {'steps': [[0, 'error', 'timeout'], [20000, 'error', 'timeout']], 'opts': NICEGUI_OPTS,
         'posthog': True},
        {'steps': [[0, 'error', 'timeout']],
         'opts': dict(NICEGUI_OPTS, transports=['websocket'])},  # no fallback: it reloads
    ])
    assert fallback['events'] == [[0, 'timeout'], [21500, 'timeout']], fallback
    assert fallback['storage'] == {'gs_reload_n': '1'}, 'the fallback is not a reload'
    assert [c[1]['reason'] for c in fallback['captured']] == ['timeout'], fallback['captured']
    assert websocket_only['events'] == [[1500, 'timeout']], websocket_only


@needs_node
def test_a_refusal_arriving_while_a_timeout_waits_is_never_dropped(tmp_path):
    """socket.io destroys a socket whose CONNECT is refused: NiceGUI's reload is the page's
    only way back. A refusal during a pending timeout replaced nothing and was dropped,
    so the tab stayed dead (20 of 20 hidden tabs in Chromium)."""
    visible, hidden = _run_cases(tmp_path, [
        {'steps': [[0, 'error', 'timeout'], [500, 'error', IHF]]},
        {'steps': [[0, 'error', 'timeout'], [500, 'error', IHF], [12000, 'show']], 'hidden': True},
    ])
    assert visible['events'] == [[1500, IHF]], visible   # the refusal, at the timeout's slot
    assert hidden['events'] == [[13500, IHF]], hidden


@needs_node
def test_a_reconnect_cancels_a_pending_timeout_reload_but_not_a_refusal(tmp_path):
    """A page that reconnected and is in use must not be reloaded by a stale timeout;
    a refused handshake's socket is gone, so that reload still runs."""
    timeout, refusal = _run_cases(tmp_path, [
        {'steps': [[0, 'error', 'timeout'], [200, 'connect']]},
        {'steps': [[0, 'error', IHF], [200, 'connect']]},
    ])
    assert timeout['events'] == [], timeout
    assert refusal['events'] == [[1500, IHF]], refusal


@needs_node
def test_each_connect_error_listener_keeps_its_own_pending_reload(tmp_path):
    """One page-global slot let a second listener take the deferred error from NiceGUI's."""
    (case,) = _run_cases(tmp_path, [{'steps': [[0, 'error', IHF]], 'listeners': 2}])
    assert case['events'] == [[1500, IHF]] and case['events2'] == [[3000, IHF]], case
