# -*- coding: utf-8 -*-
"""Cross-language golden decode + DOM-XSS neutralization + static no-innerHTML
guard for the Phase 133 (ATLAS-01) client renderer web/static/js/atlas_decode.js.

Why here (tests/atlas_bake/)
----------------------------
Two of these checks shell out to ``node``. The dedicated ``atlas-bake-tests`` CI
job runs this directory with ``actions/setup-node`` available (the main ``tests``
job deselects ``-m "not atlas_bake"``). Locally the Node-driven tests SKIP
cleanly when ``node`` is not on PATH — they never fake a pass. The static
no-innerHTML guard is pure Python and always runs.

What is proven
--------------
1. test_js_golden_decode_matches_python — Node loads atlas_decode.js, decodes the
   committed ``tests/fixtures/atlas/golden-v1.bin`` and asserts PER-FIELD equality
   with ``golden-v1-expected.json`` (the SAME values Python asserts in
   test_atlas_bake.py). sys_id is compared via ``BigInt(str)`` so no precision is
   lost above 2^53; floats are the Float32->Float64 widened values. This catches
   silent encoder/decoder schema drift (T-133-16).
2. test_js_dom_xss_neutralized — Node drives the module's DOM builders
   (buildTooltipContent + buildFocusRow) over the fabricated malicious golden
   string via an injected fake ``document`` that records ``.textContent`` vs
   ``.innerHTML`` per element, and asserts the hostile string is assigned to
   ``.textContent`` (inert text) and NEVER to any ``.innerHTML`` (HIGH-7 / T-133-15).
3. test_static_no_innerhtml_guard — pure Python: neither atlas_decode.js nor the
   renderer JS injected by web/pages/atlas.py assigns a catalogue-derived value
   into innerHTML (defense-in-depth against a future edit reintroducing the sink).
"""
from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.atlas_bake

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ATLAS_JS = REPO_ROOT / "web" / "static" / "js" / "atlas_decode.js"
ATLAS_PY = REPO_ROOT / "web" / "pages" / "atlas.py"
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures" / "atlas"
GOLDEN_BIN = FIXTURES_DIR / "golden-v1.bin"
GOLDEN_EXPECTED = FIXTURES_DIR / "golden-v1-expected.json"

_NODE = shutil.which("node")

# ---------------------------------------------------------------------------
# Node harnesses (written to a tmp dir, invoked with absolute posix paths).
# ---------------------------------------------------------------------------

_DECODE_HARNESS = r"""
'use strict';
const fs = require('fs');
const AtlasDecode = require(process.argv[2]);
const binPath = process.argv[3];
const expectedPath = process.argv[4];

const buf = fs.readFileSync(binPath);
const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
const decoded = AtlasDecode.decodeAtlas(ab);
const expected = JSON.parse(fs.readFileSync(expectedPath, 'utf-8'));

const errs = [];
function cmpVal(path, a, b) {
  if (path.endsWith('sys_id')) {
    // Compare as BigInt so no precision is lost above 2^53 (schema §7).
    if (BigInt(a) !== BigInt(b)) errs.push(path + ': ' + a + ' != ' + b);
    return;
  }
  if (a !== b) errs.push(path + ': ' + JSON.stringify(a) + ' != ' + JSON.stringify(b));
}
function cmpArr(name, A, B) {
  if (!Array.isArray(A) || !Array.isArray(B)) { errs.push(name + ': not both arrays'); return; }
  if (A.length !== B.length) { errs.push(name + '.length: ' + A.length + ' != ' + B.length); return; }
  for (let i = 0; i < A.length; i++) {
    const keys = new Set([].concat(Object.keys(A[i]), Object.keys(B[i])));
    keys.forEach(function (k) { cmpVal(name + '[' + i + '].' + k, A[i][k], B[i][k]); });
  }
}

cmpVal('schema_version', decoded.schema_version, expected.schema_version);
cmpArr('nodes', decoded.nodes, expected.nodes);
cmpArr('edges', decoded.edges, expected.edges);
cmpArr('flows', decoded.flows, expected.flows);
cmpArr('cluster_labels', decoded.cluster_labels, expected.cluster_labels);

if (errs.length) {
  console.error('MISMATCHES (' + errs.length + '):');
  errs.slice(0, 40).forEach(function (e) { console.error('  ' + e); });
  process.exit(1);
}
console.log('OK nodes=' + decoded.nodes.length + ' edges=' + decoded.edges.length +
  ' flows=' + decoded.flows.length + ' labels=' + decoded.cluster_labels.length);
process.exit(0);
"""

_XSS_HARNESS = r"""
'use strict';
const fs = require('fs');
const AtlasDecode = require(process.argv[2]);
const binPath = process.argv[3];

// Fake document recording textContent vs innerHTML per element.
function makeEl(tag) {
  return {
    tagName: tag, children: [], attributes: {}, className: '', style: {},
    _tc: undefined, _ih: undefined,
    appendChild: function (c) { this.children.push(c); return c; },
    setAttribute: function (k, v) { this.attributes[k] = v; },
    removeChild: function (c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; },
    get firstChild() { return this.children.length ? this.children[0] : null; },
    set textContent(v) { this._tc = v; },
    get textContent() { return this._tc; },
    set innerHTML(v) { this._ih = v; },
    get innerHTML() { return this._ih; }
  };
}
AtlasDecode.setDocument({ createElement: function (t) { return makeEl(t); } });

const buf = fs.readFileSync(binPath);
const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
const decoded = AtlasDecode.decodeAtlas(ab);

// Locate the fabricated malicious node by its hostile shape (never assume index).
const evil = decoded.nodes.find(function (n) { return n.title && n.title.indexOf('onerror=') >= 0; });
if (!evil) { console.error('FAIL: malicious fixture node not found in decoded payload'); process.exit(2); }
const MAL = evil.title;

const state = {
  lang: 'en',
  labels: { domain: 'Domain', library: 'Library' },
  domainGroups: [], libraries: [], colorBy: 'domain'
};
// minimal domain_groups / libraries so label lookups don't throw
for (let i = 0; i < 13; i++) state.domainGroups.push(['D' + i, 'H' + i, '#101010']);
state.libraries = ['BL', 'CUL', 'JTS', 'Oxford', 'RNL'];

function walk(el, tcs, ihs) {
  if (!el || typeof el !== 'object') return;
  if (typeof el._tc === 'string') tcs.push(el._tc);
  if (el._ih !== undefined) ihs.push(el._ih);
  (el.children || []).forEach(function (c) { walk(c, tcs, ihs); });
}

const problems = [];
['buildTooltipContent', 'buildFocusRow'].forEach(function (fn) {
  const root = AtlasDecode[fn](state, evil);
  const tcs = [], ihs = [];
  walk(root, tcs, ihs);
  const inText = tcs.some(function (t) { return t.indexOf(MAL) >= 0; });
  const anyInnerHtml = ihs.length > 0;
  const inHtml = ihs.some(function (h) { return typeof h === 'string' && h.indexOf('<img') >= 0; });
  if (!inText) problems.push(fn + ': malicious string NOT rendered as textContent');
  if (anyInnerHtml) problems.push(fn + ': assigned innerHTML ' + ihs.length + ' time(s) (must be zero)');
  if (inHtml) problems.push(fn + ': malicious markup landed in innerHTML');
});

if (problems.length) {
  console.error('DOM-XSS FAIL:');
  problems.forEach(function (p) { console.error('  ' + p); });
  process.exit(1);
}
console.log('OK dom-xss neutralized (textContent-only, zero innerHTML)');
process.exit(0);
"""

# Corrupts one specific structural invariant of the committed golden .bin
# (byte-patched in a throwaway Buffer copy, never the fixture on disk), then
# asserts decodeAtlas() REFUSES it (throws) instead of silently producing
# truncated/undefined records or reading out of bounds (Codex MEDIUM-1).
# `mode` selects which invariant to break; each targets one of the specific
# trust points the finding calls out: duplicate section ids, a heap
# (offset,length) ref past the string heap, an edge endpoint past node_count,
# a section's dtype disagreeing with its schema-fixed type (while keeping
# elem_size unchanged, so only the FIXED_DTYPE cross-check — not the
# elem_size check — can catch it), and an absurd section_count.
_MALFORMED_HARNESS = r"""
'use strict';
const fs = require('fs');
const AtlasDecode = require(process.argv[2]);
const binPath = process.argv[3];
const mode = process.argv[4];

const buf = Buffer.from(fs.readFileSync(binPath)); // mutable copy; fixture on disk is untouched

function findEntryOffset(secId) {
  const sectionCount = buf.readUInt32LE(12);
  for (let idx = 0; idx < sectionCount; idx++) {
    const entryOff = 16 + idx * 32;
    if (buf.readUInt32LE(entryOff) === secId) return entryOff;
  }
  throw new Error('golden fixture harness: section ' + secId + ' not found');
}

switch (mode) {
  case 'dup-section': {
    // Make the second section-table entry claim the SAME section_id as the
    // first (whatever ids those happen to be) -- a real duplicate.
    const e0 = 16, e1 = 16 + 32;
    buf.writeUInt32LE(buf.readUInt32LE(e0), e1);
    break;
  }
  case 'heap-oob': {
    // NODE_TITLE_REF (section_id 7): corrupt ref[0].offset to a value that
    // cannot possibly fit inside the string heap.
    const dataOffset = Number(buf.readBigUInt64LE(findEntryOffset(7) + 16));
    buf.writeUInt32LE(0xfffffff0, dataOffset);
    break;
  }
  case 'edge-oob': {
    // EDGE_TARGET_DELTA (section_id 10): edge 0's target is stored ABSOLUTE
    // (schema §6, i===0 case) -- corrupt it far past any real node_count.
    const dataOffset = Number(buf.readBigUInt64LE(findEntryOffset(10) + 16));
    buf.writeUInt32LE(0xffffff, dataOffset);
    break;
  }
  case 'bad-dtype': {
    // NODE_POS (section_id 1) is schema-fixed FLOAT32 (dtype_code 1).
    // Relabel it UINT32 (dtype_code 4) -- SAME elem_size (4), so only the
    // FIXED_DTYPE cross-check (not the elem_size/dtype agreement check) can
    // catch this.
    buf.writeUInt32LE(4, findEntryOffset(1) + 4);
    break;
  }
  case 'section-count': {
    // section_count is read BEFORE the section table is walked -- an
    // uncapped value must be rejected before any table entry is read.
    buf.writeUInt32LE(0xffffff, 12);
    break;
  }
  default:
    throw new Error('unknown mode ' + mode);
}

const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
let threw = false;
let message = '';
try {
  AtlasDecode.decodeAtlas(ab);
} catch (e) {
  threw = true;
  message = (e && e.message) || String(e);
}

if (!threw) {
  console.error('FAIL (' + mode + '): malformed asset decoded WITHOUT throwing -- MEDIUM-1 regression');
  process.exit(1);
}
console.log('OK malformed asset rejected (' + mode + '): ' + message);
process.exit(0);
"""

# Drives the FULL AtlasDecode.init(config) flow (whenCanvasReady -> fetch
# manifest+asset -> decode -> draw -> attachInteractions) against a minimal
# fake browser (DOM element graph with an id registry mirroring
# getElementById/appendChild/removeChild, a no-op Canvas2D context proxy, and
# a stubbed fetch()) to prove the actual UX contract end-to-end (Codex
# MEDIUM-2 / MEDIUM-3): the "Loading…" placeholder is always removed exactly
# once (never stuck), and a fetch/decode failure OR a canvas that never
# mounts both surface the SAME normal, visible load-error UI rather than an
# indefinite silent spinner or an overlapping stuck placeholder.
_INIT_HARNESS = r"""
'use strict';
const fs = require('fs');
const AtlasDecode = require(process.argv[2]);
const binPath = process.argv[3];
const mode = process.argv[4]; // 'success' | 'fetch-error' | 'canvas-timeout'

// whenCanvasReady's poll loop uses plain setTimeout(fn, 50) -- collapse it to
// setImmediate so the up-to-200-iteration canvas-timeout path resolves in
// milliseconds rather than ~10 real seconds.
global.setTimeout = function (fn) { setImmediate(fn); };

const registry = {}; // id -> element, mirrors document.getElementById

function makeStyleProxy() {
  return new Proxy({}, { get: function () { return ''; }, set: function () { return true; } });
}

function makeCtxProxy() {
  const target = {};
  return new Proxy(target, {
    get: function (t, prop) {
      if (prop === 'addColorStop') return function () {};
      if (prop in t) return t[prop];
      return function () { return makeCtxProxy(); }; // any called ctx method is a harmless no-op
    },
    set: function (t, prop, value) { t[prop] = value; return true; }
  });
}

function makeElement(tag) {
  const el = {
    tagName: tag, id: '', children: [], attributes: {}, className: '',
    style: makeStyleProxy(), parentNode: null, _tc: undefined, _ih: undefined,
    clientWidth: 800, clientHeight: 720,
    addEventListener: function () {},
    removeEventListener: function () {},
    setPointerCapture: function () {},
    getBoundingClientRect: function () { return { left: 0, top: 0 }; },
    getContext: function () { return makeCtxProxy(); }
  };
  el.appendChild = function (c) { c.parentNode = el; el.children.push(c); return c; };
  el.removeChild = function (c) {
    const i = el.children.indexOf(c);
    if (i >= 0) el.children.splice(i, 1);
    c.parentNode = null;
    return c;
  };
  el.setAttribute = function (k, v) {
    el.attributes[k] = v;
    if (k === 'id') { el.id = v; registry[v] = el; }
  };
  Object.defineProperty(el, 'firstChild', { get: function () { return el.children.length ? el.children[0] : null; } });
  Object.defineProperty(el, 'parentElement', { get: function () { return el.parentNode; } });
  Object.defineProperty(el, 'textContent', {
    get: function () { return el._tc; }, set: function (v) { el._tc = v; }
  });
  Object.defineProperty(el, 'innerHTML', {
    get: function () { return el._ih; }, set: function (v) { el._ih = v; }
  });
  return el;
}

// Reserved box + loading placeholder always mount; the canvas mounts UNLESS
// this run is simulating the MEDIUM-3 canvas-mount timeout.
const box = makeElement('div');
box.setAttribute('id', 'atlas-canvas-box');
const loading = makeElement('div');
loading.setAttribute('id', 'atlas-loading');
box.appendChild(loading);

let canvas = null;
if (mode !== 'canvas-timeout') {
  canvas = makeElement('canvas');
  canvas.setAttribute('id', 'atlas-canvas');
  box.appendChild(canvas);
}

global.document = {
  createElement: makeElement,
  getElementById: function (id) { return registry[id] || null; },
  body: makeElement('body')
};
global.window = {
  devicePixelRatio: 1,
  addEventListener: function () {},
  requestAnimationFrame: function (fn) { setImmediate(fn); },
  matchMedia: function () { return { matches: true }; }, // reduced-motion -> intro finishes synchronously
  console: console
};
// _doc (used by the XSS-safe DOM builders) is captured once at require()
// time -- explicitly hand it our fake document (same pattern as the DOM-XSS
// harness above).
AtlasDecode.setDocument(global.document);

const binBuf = fs.readFileSync(binPath);

global.fetch = function (url) {
  if (mode === 'fetch-error') {
    return Promise.reject(new Error('simulated network failure'));
  }
  if (String(url).indexOf('manifest.json') >= 0) {
    return Promise.resolve({
      ok: true,
      json: function () {
        return Promise.resolve({ domain_groups: [], libraries: [], asset_basename: 'golden-v1' });
      }
    });
  }
  return Promise.resolve({
    ok: true,
    arrayBuffer: function () {
      const ab = binBuf.buffer.slice(binBuf.byteOffset, binBuf.byteOffset + binBuf.byteLength);
      return Promise.resolve(ab);
    }
  });
};

const config = {
  manifestUrl: '/atlas-data/manifest.json',
  dataBase: '/atlas-data/',
  canvasId: 'atlas-canvas',
  loadingId: 'atlas-loading',
  boxId: 'atlas-canvas-box',
  lang: 'en',
  rtl: false,
  labels: { loadError: 'The atlas could not be loaded.' }
};

function walkFindText(el, text) {
  if (!el || typeof el !== 'object') return false;
  if (el._tc === text) return true;
  for (let i = 0; i < (el.children || []).length; i++) {
    if (walkFindText(el.children[i], text)) return true;
  }
  return false;
}

AtlasDecode.init(config);

function tick(n, done) {
  if (n <= 0) { done(); return; }
  setImmediate(function () { tick(n - 1, done); });
}

tick(500, function () {
  const problems = [];
  if (loading.parentNode !== null) problems.push('loading placeholder was NOT removed');
  const hasErrorText = walkFindText(box, config.labels.loadError);

  if (mode === 'success') {
    if (hasErrorText) problems.push('error overlay unexpectedly present on a successful render');
    if (!global.window.__atlasRenderer) problems.push('window.__atlasRenderer was never set (render did not complete)');
    if (box.children.indexOf(canvas) < 0) problems.push('canvas element unexpectedly detached from the box');
  } else {
    // fetch-error / canvas-timeout: the SAME normal load-error UI must appear.
    if (!hasErrorText) problems.push('load-error overlay text not found under the reserved box');
  }

  if (problems.length) {
    console.error('FAIL (' + mode + '):');
    problems.forEach(function (p) { console.error('  ' + p); });
    process.exit(1);
  }
  console.log('OK init UX (' + mode + '): loading placeholder hidden, ' +
    (mode === 'success' ? 'canvas rendered, no error overlay' : 'normal load-error UI shown'));
  process.exit(0);
});
"""


def _run_node(tmp_path, harness_src: str, *args) -> subprocess.CompletedProcess:
    harness = tmp_path / "harness.js"
    harness.write_text(harness_src, encoding="utf-8")
    argv = [_NODE, str(harness), ATLAS_JS.as_posix(), *args]
    return subprocess.run(
        argv, capture_output=True, text=True, encoding="utf-8", timeout=120
    )


# ---------------------------------------------------------------------------
# 1. Cross-language golden decode (JS == Python, per-field)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(_NODE is None, reason="node not on PATH — runs in the atlas-bake CI job")
def test_js_golden_decode_matches_python(tmp_path):
    assert GOLDEN_BIN.exists(), f"missing golden fixture {GOLDEN_BIN}"
    assert GOLDEN_EXPECTED.exists(), f"missing expected values {GOLDEN_EXPECTED}"
    proc = _run_node(tmp_path, _DECODE_HARNESS, GOLDEN_BIN.as_posix(), GOLDEN_EXPECTED.as_posix())
    assert proc.returncode == 0, (
        "JS golden decode does NOT match the Python per-field expected values "
        "(silent encoder/decoder schema drift — T-133-16):\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )


# ---------------------------------------------------------------------------
# 2. DOM-XSS neutralization against the committed malicious fixture (HIGH-7)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(_NODE is None, reason="node not on PATH — runs in the atlas-bake CI job")
def test_js_dom_xss_neutralized(tmp_path):
    proc = _run_node(tmp_path, _XSS_HARNESS, GOLDEN_BIN.as_posix())
    assert proc.returncode == 0, (
        "DOM-XSS neutralization FAILED — a catalogue string was not rendered as "
        "inert textContent, or an innerHTML sink was used (HIGH-7 / T-133-15):\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )


# ---------------------------------------------------------------------------
# 3. Static no-innerHTML guard (pure Python — always runs, no node)
# ---------------------------------------------------------------------------

# Matches the plan's guard: an innerHTML assignment whose RHS (up to the next ';')
# mentions a catalogue-derived field.
_INNERHTML_CATALOGUE_RE = re.compile(
    r"innerHTML\s*=\s*[^;]*(title|shelf|label|domain|library|name)", re.IGNORECASE
)


def test_static_no_innerhtml_guard():
    js = ATLAS_JS.read_text(encoding="utf-8")
    py = ATLAS_PY.read_text(encoding="utf-8")

    # No catalogue string interpolated into innerHTML in either file.
    assert not _INNERHTML_CATALOGUE_RE.search(js), (
        "atlas_decode.js assigns a catalogue-derived value into innerHTML (HIGH-7)."
    )
    assert not _INNERHTML_CATALOGUE_RE.search(py), (
        "web/pages/atlas.py renderer JS assigns a catalogue-derived value into innerHTML."
    )

    # Stronger: the decoder/renderer module uses NO innerHTML assignment at all,
    # and no insertAdjacentHTML — every data-bearing node goes through textContent.
    assert not re.search(r"\.innerHTML\s*=", js), (
        "atlas_decode.js contains a `.innerHTML =` assignment — build DOM via "
        "createElement/textContent instead (HIGH-7)."
    )
    assert "insertAdjacentHTML" not in js, (
        "atlas_decode.js uses insertAdjacentHTML — forbidden for catalogue DOM (HIGH-7)."
    )
    assert "textContent" in js, (
        "atlas_decode.js must build data-bearing DOM via textContent."
    )
    # The renderer's injected JS in atlas.py must not use innerHTML either.
    assert ".innerHTML" not in py, (
        "web/pages/atlas.py renderer JS references innerHTML — forbidden (HIGH-7)."
    )


# ---------------------------------------------------------------------------
# 4. Malformed-asset structural validation (Codex MEDIUM-1 defense-in-depth).
#    A structurally-servable-but-corrupted golden .bin (duplicate section id,
#    out-of-bounds heap ref, out-of-range edge endpoint, a section's dtype
#    disagreeing with its schema-fixed type, or an absurd section_count) must
#    make decodeAtlas() throw a clean Error — never silently return truncated/
#    wrong data, never OOB-read, never hang. Valid-asset decode (test 1 above)
#    is unaffected by these checks.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(_NODE is None, reason="node not on PATH — runs in the atlas-bake CI job")
@pytest.mark.parametrize(
    "mode", ["dup-section", "heap-oob", "edge-oob", "bad-dtype", "section-count"]
)
def test_js_decode_rejects_malformed_asset(tmp_path, mode):
    assert GOLDEN_BIN.exists(), f"missing golden fixture {GOLDEN_BIN}"
    proc = _run_node(tmp_path, _MALFORMED_HARNESS, GOLDEN_BIN.as_posix(), mode)
    assert proc.returncode == 0, (
        f"malformed-asset case {mode!r} was NOT rejected by decodeAtlas "
        "(MEDIUM-1 regression — a corrupted-but-structurally-servable asset must "
        "raise, never silently decode):\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )


# ---------------------------------------------------------------------------
# 5. init() UX — loading placeholder + normal load-error UI (Codex MEDIUM-2 /
#    MEDIUM-3). Drives the FULL AtlasDecode.init(config) flow against a
#    minimal fake browser: on a successful render the "Loading…" placeholder
#    is removed and no error overlay appears; on a fetch/decode failure OR a
#    canvas that never mounts within the poll window, the placeholder is
#    STILL removed and the SAME normal load-error UI is shown instead of an
#    indefinite silent spinner or an overlapping stuck placeholder.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(_NODE is None, reason="node not on PATH — runs in the atlas-bake CI job")
@pytest.mark.parametrize("mode", ["success", "fetch-error", "canvas-timeout"])
def test_js_init_ux_states(tmp_path, mode):
    assert GOLDEN_BIN.exists(), f"missing golden fixture {GOLDEN_BIN}"
    proc = _run_node(tmp_path, _INIT_HARNESS, GOLDEN_BIN.as_posix(), mode)
    assert proc.returncode == 0, (
        f"init() UX regression in mode {mode!r} (MEDIUM-2/MEDIUM-3):\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )
