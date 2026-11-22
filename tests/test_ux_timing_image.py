# -*- coding: utf-8 -*-
"""ux_timing follows the image the reader ends up seeing (2026-11-22, event v2).

``img`` / ``img_ms`` time the first ``<img>`` present after Phase A: on Browse that can
be a 400-px preview, or an image the next draw replaces (a Cambridge page used to draw
NLI's image first). ``img_final`` / ``img_full_ms`` / ``img_source`` follow the selector
across draws until its full-size image (or a fallback) is on screen. Run in node against
the shipped web/static/js/ux_timing.js with a fake clock and DOM.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
JS = ROOT / 'web' / 'static' / 'js' / 'ux_timing.js'
SEL = '.img-loading-container img.zoomable-image'
PREVIEW = '/api/cambridge_image/990051246990205171?page=1&_cb=2&cv=1&width=400'
FULL = '/api/cambridge_image/990051246990205171?page=1&_cb=2&cv=1'

_HARNESS = r"""
const fs = require('fs'), vm = require('vm');
const src = fs.readFileSync(process.argv[2], 'utf8');
const cases = JSON.parse(fs.readFileSync(0, 'utf8'));
const out = [];
function makeImg(attrs) {
  const listeners = {};
  const el = Object.assign({src: '', complete: false, naturalWidth: 0, dataset: {}, style: {},
    attrs: {}, listeners}, attrs || {});
  el.getAttribute = name => (name === 'src' ? el.src : (el.attrs[name] === undefined ? null : el.attrs[name]));
  el.addEventListener = (type, fn) => { (listeners[type] = listeners[type] || []).push(fn); };
  el.fire = type => { (listeners[type] || []).slice().forEach(fn => fn()); listeners[type] = []; };
  return el;
}
for (const c of cases) {
  let t = 0; const timers = []; const doc = {}; const captured = [];
  let current = null;
  const win = {GS_UX: {rate: 1, v: 2, wait: {}}, posthog: {capture: (n, p) => captured.push(p)}};
  const ctx = {
    window: win, Math, URL,
    document: {addEventListener: (type, fn) => { doc[type] = fn; },
               querySelector: sel => (sel === c.sel ? current : null),
               visibilityState: 'visible', referrer: ''},
    performance: {now: () => t, getEntriesByType: () => []},
    requestAnimationFrame: fn => timers.push({at: t, fn}),
    setTimeout: (fn, ms) => timers.push({at: t + (ms || 0), fn}),
    navigator: {webdriver: false}, location: {origin: 'http://x'},
  };
  vm.runInNewContext(src, ctx);
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
    if (op === 'click') {
      doc.click({isTrusted: true, timeStamp: t,
        target: {closest: s => s === '[data-ux]' ? {getAttribute: () => 'browse_page'} : null}});
    } else if (op === 'draw') {          // a new <img> (Phase A or Phase B's draw)
      current = makeImg(arg);
    } else if (op === 'set') {           // change the image on screen
      Object.assign(current, arg.props || {});
      Object.assign(current.dataset, arg.dataset || {});
      if (arg.fire) current.fire(arg.fire);
    } else if (op === 'remove') {
      current = null;
    } else if (op === 'done') {
      win.gsUx.done('browse_page', 'ok', 30, false, c.sel, c.settle || undefined);
    } else if (op === 'settled') {
      win.gsUx.settled('browse_page');
    }
  }
  runUntil(Infinity);
  out.push(captured.map(p => ({img: p.img, img_ms: p.img_ms, img_final: p.img_final,
                               img_full_ms: p.img_full_ms, img_source: p.img_source, v: p.v})));
}
process.stdout.write(JSON.stringify(out));
"""


def _draw(src, full):
    return {'src': src, 'attrs': {'data-full-src': full}}


def _run(cases, tmp_path):
    harness = tmp_path / 'harness.js'
    harness.write_text(_HARNESS, encoding='utf-8')
    proc = subprocess.run(['node', str(harness), str(JS)], input=json.dumps(cases), capture_output=True,
                          text=True, encoding='utf-8', timeout=60)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


@pytest.mark.skipif(shutil.which('node') is None, reason='node is not installed')
def test_the_image_the_reader_ends_up_seeing_is_followed(tmp_path):
    nli_preview = '/api/nli_image_by_sysid/990051246990205171?page=1&_cb=2&width=400'
    nli_full = '/api/nli_image_by_sysid/990051246990205171?page=1&_cb=2'
    cases = [
        # Preview at 1.5 s, the full image swapped in at 3 s.
        {'sel': SEL, 'steps': [
            [0, 'click'], [500, 'draw', _draw(PREVIEW, FULL)], [500, 'done'],
            [1500, 'set', {'props': {'complete': True, 'naturalWidth': 400}, 'fire': 'load'}],
            [3000, 'set', {'props': {'src': FULL, 'naturalWidth': 1367}}],
        ]},
        # The old Cambridge page turn: Phase A draws NLI, Phase B the Cambridge image.
        {'sel': SEL, 'steps': [
            [0, 'click'], [500, 'draw', _draw(nli_preview, nli_full)], [500, 'done'],
            [550, 'draw', _draw(FULL, FULL)],
            [4000, 'set', {'props': {'complete': True, 'naturalWidth': 1367}, 'fire': 'load'}],
        ]},
        # The reader turns the page again before the full image came.
        {'sel': SEL, 'steps': [
            [0, 'click'], [500, 'draw', _draw(PREVIEW, FULL)], [500, 'done'],
            [1000, 'set', {'props': {'complete': True, 'naturalWidth': 400}, 'fire': 'load'}],
            [2000, 'click'],
        ]},
        # The preview failed and NLI's image, fetched by the fallback, is shown.
        {'sel': SEL, 'steps': [
            [0, 'click'], [500, 'draw', _draw(PREVIEW, FULL)], [500, 'done'],
            [900, 'set', {'fire': 'error', 'dataset': {'triedManifest': 'true'},
                          'props': {'src': 'https://iiif.nli.org.il/IIIFv21/FL1/full/2000,/0/default.jpg'}}],
            [2000, 'set', {'props': {'complete': True, 'naturalWidth': 2000}}],
        ]},
        # Every source failed: the fallback chain put its notice in place of the image.
        {'sel': SEL, 'steps': [
            [0, 'click'], [500, 'draw', _draw(PREVIEW, FULL)], [500, 'done'],
            [900, 'set', {'fire': 'error'}], [5000, 'remove'],
        ]},
        # Nothing ever arrives.
        {'sel': SEL, 'steps': [[0, 'click'], [500, 'draw', _draw(PREVIEW, FULL)], [500, 'done']]},
    ]
    out = _run(cases, tmp_path)

    def near(ms, expected):  # the image is looked at every 100 ms
        return ms is not None and expected <= ms <= expected + 100

    first = out[0][0]
    assert (first['img'], first['img_ms'], first['img_final'], first['img_source'], first['v']) == (
        'loaded', 1000, 'full', 'cambridge', 2), out[0]
    assert near(first['img_full_ms'], 2500), out[0]
    assert len(out[0]) == 1, 'one event per page turn'
    # img_ms timed NLI's image, never shown; the full Cambridge image is what counts.
    assert out[1][0]['img_final'] == 'full' and out[1][0]['img_source'] == 'cambridge', out[1]
    assert near(out[1][0]['img_full_ms'], 3500), out[1]
    assert out[2][0]['img_final'] == 'moved_on' and out[2][0]['img_full_ms'] is None, out[2]
    assert out[3][0]['img_final'] == 'fallback' and out[3][0]['img_source'] == 'nli_direct', out[3]
    assert near(out[3][0]['img_full_ms'], 1500), out[3]
    assert out[4][0]['img_final'] == 'error' and out[4][0]['img_full_ms'] is None, out[4]
    assert out[5][0]['img_final'] == 'timeout', out[5]


@pytest.mark.skipif(shutil.which('node') is None, reason='node is not installed')
def test_with_settle_the_image_after_the_last_draw_counts(tmp_path):
    """Browse draws again after its enrichment (ux_done(..., settle=True), then
    ux_settled). An NLI image already sharp from the cache in the first draw is not what
    the reader is shown when the second draw puts Cambridge's in its place (seen in the
    local app, 2026-11-22)."""
    nli_full = '/api/nli_image_by_sysid/990051246990205171?page=0&_cb=1'
    cached_nli = {'src': nli_full, 'complete': True, 'naturalWidth': 2000, 'attrs': {'data-full-src': nli_full}}
    cases = [
        {'sel': SEL, 'settle': True, 'steps': [
            [0, 'click'], [500, 'draw', cached_nli], [500, 'done'],
            [3000, 'draw', _draw(PREVIEW, FULL)], [3000, 'settled'],
            [4000, 'set', {'props': {'complete': True, 'naturalWidth': 400}, 'fire': 'load'}],
            [6000, 'set', {'props': {'src': FULL, 'naturalWidth': 1367}}],
        ]},
        # The word never comes (the enrichment failed): the image sharp at the end counts.
        {'sel': SEL, 'settle': True, 'steps': [
            [0, 'click'], [500, 'draw', cached_nli], [500, 'done'],
        ]},
        # Settled at once and the first image is already sharp: as without settle.
        {'sel': SEL, 'settle': True, 'steps': [
            [0, 'click'], [500, 'draw', cached_nli], [500, 'done'], [600, 'settled'],
        ]},
    ]
    out = _run(cases, tmp_path)
    final = out[0][0]
    assert (final['img_final'], final['img_source']) == ('full', 'cambridge'), out[0]
    assert 5500 <= final['img_full_ms'] <= 5600, out[0]
    unsettled = out[1][0]
    assert (unsettled['img_final'], unsettled['img_source']) == ('full', 'nli'), out[1]
    assert unsettled['img_full_ms'] is not None and unsettled['img_full_ms'] <= 100, out[1]
    quick = out[2][0]
    assert (quick['img_final'], quick['img_source']) == ('full', 'nli'), out[2]
    assert quick['img_full_ms'] <= 100, out[2]


@pytest.mark.skipif(shutil.which('node') is None, reason='node is not installed')
def test_no_selector_reports_none_at_once(tmp_path):
    out = _run([{'sel': None, 'steps': [[0, 'click'], [500, 'done']]}], tmp_path)
    assert out[0] == [{'img': 'none', 'img_ms': None, 'img_final': 'none', 'img_full_ms': None,
                       'img_source': None, 'v': 2}], out
