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
