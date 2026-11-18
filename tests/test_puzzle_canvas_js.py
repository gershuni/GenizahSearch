# -*- coding: utf-8 -*-
"""The web Fragment Puzzle's canvas script, run in Node against a small stand-in for Fabric.

Pictures reach the canvas slowly (the browser extension, a library that takes
seconds) and out of order. The script used to draw every answer when it came:
flipping the puzzle twice before the first side arrived left the fragments on
the wrong side at the wrong place, a picture asked for by a document opened
earlier landed on the one opened after it, and a crop was lost with every flip.

Run here over the real ``PUZZLE_CANVAS_JS``: the test decides when each picture
and each list of folios answers, so the orders that went wrong are replayed
exactly.

The stand-in keeps what matters of Fabric's geometry: an object is anchored at
its corner, so changing its width or height moves its centre.
"""

from __future__ import annotations

import json
import shutil
import subprocess

import pytest

from web.pages import puzzle as puzzle_page

NODE = shutil.which('node')
pytestmark = pytest.mark.skipif(NODE is None, reason='node is not installed')

HARNESS = r'''
const fs = require('fs');
const vm = require('vm');
const source = fs.readFileSync(process.argv[2], 'utf-8');

const events = [];
const canvasEl = {
    parentElement: { clientWidth: 1000, clientHeight: 600 },
    dispatchEvent(e) { events.push({ name: e.name, detail: JSON.parse(e.detail) }); },
};
const noop = () => {};
const classes = new Set();
global.window = global;
global.location = { search: '', href: 'http://localhost/puzzle' };
global.localStorage = { getItem: () => '0', setItem: noop };
global.addEventListener = noop;
global.removeEventListener = noop;
global.postMessage = noop;
global.CustomEvent = class { constructor(name, init) { this.name = name; this.detail = init.detail; } };
global.document = {
    getElementById: (id) => (id === 'puzzleCanvas' ? canvasEl : null),
    querySelector: () => null,
    addEventListener: noop,
    removeEventListener: noop,
    documentElement: { dir: 'ltr' },
    body: {
        dir: 'ltr',
        classList: {
            contains: (c) => classes.has(c), remove: (c) => classes.delete(c),
            toggle: (c, on) => (on ? classes.add(c) : classes.delete(c)),
        },
    },
};
global.URL = { createObjectURL: (blob) => 'blob:' + blob.url };
console.log = noop;
console.warn = noop;
console.error = noop;

// A recto is 800 x 1000 and a verso 800 x 800; a picture "loads" on the next
// turn of the loop, or fails if its fragment side is listed in `failing`.
const failing = new Set();
global.Image = class {
    removeAttribute() {}
    set src(value) {
        this._src = value;
        const side = (/fl_id=([A-Z]+[rv])/.exec(value) || [])[1] || '';
        this.naturalWidth = 800;
        this.naturalHeight = side.endsWith('v') ? 800 : 1000;
        setImmediate(() => (failing.has(side) ? this.onerror && this.onerror() : this.onload && this.onload()));
    }
    get src() { return this._src; }
};

// Picture requests wait until the test releases them. Folio lists answer at
// once unless `holdFolios` is set, when they wait too.
const waiting = [];
const folioWaiting = [];
let holdFolios = false;
global.fetch = (url) => {
    if (url.startsWith('/api/puzzle_folios/')) {
        const sys = url.split('/').pop();
        const answer = { ok: true, json: async () => [
            { fl_id: sys + 'r', label: '1r' }, { fl_id: sys + 'v', label: '1v' },
        ] };
        if (!holdFolios) return Promise.resolve(answer);
        return new Promise((resolve) => folioWaiting.push(() => resolve(answer)));
    }
    return new Promise((resolve) => waiting.push({
        url, release: () => resolve({ ok: true, blob: async () => ({ url }), headers: { get: () => '' } }),
    }));
};
const flush = async () => { for (let i = 0; i < 12; i++) await new Promise((r) => setImmediate(r)); };
const release = async (pick) => {
    const i = waiting.findIndex(pick || (() => true));
    if (i < 0) throw new Error('no such picture request is waiting');
    waiting.splice(i, 1)[0].release();
    await flush();
};
const releaseAll = async () => { while (waiting.length) await release(); };

class FakeObject {
    set(props) { Object.assign(this, props); return this; }
    setCoords() {}
    setControlsVisibility() {}
}
class FakeImage extends FakeObject {
    constructor(el) {
        super();
        this._el = el; this.width = el.naturalWidth; this.height = el.naturalHeight;
        this.cx = 0; this.cy = 0; this.angle = 0; this.scaleX = 1; this.scaleY = 1;
        this.flipX = false; this.flipY = false; this.cropX = 0; this.cropY = 0;
    }
    // Anchored at its corner, as in Fabric: a new width or height moves the centre.
    set(props) {
        const w = this.width, h = this.height;
        Object.assign(this, props);
        if (w !== undefined && (this.width !== w || this.height !== h)) {
            const dx = (this.width - w) / 2 * this.scaleX, dy = (this.height - h) / 2 * this.scaleY;
            const a = this.angle * Math.PI / 180;
            this.cx += dx * Math.cos(a) - dy * Math.sin(a);
            this.cy += dx * Math.sin(a) + dy * Math.cos(a);
        }
        return this;
    }
    getElement() { return this._el; }
    getCenterPoint() { return { x: this.cx, y: this.cy }; }
    setPositionByOrigin(pt) { this.cx = pt.x; this.cy = pt.y; }
}
class FakeCanvas {
    constructor() {
        this._objects = []; this._active = null; this.viewportTransform = [1, 0, 0, 1, 0, 0];
        this.upperCanvasEl = { addEventListener: noop, removeEventListener: noop };
    }
    on() {} off() {} requestRenderAll() {} dispose() {} setViewportTransform(v) { this.viewportTransform = v; }
    getWidth() { return 1000; } getHeight() { return 600; }
    add(o) { this._objects.push(o); }
    insertAt(i, o) { this._objects.splice(i, 0, o); }
    remove(o) { this._objects = this._objects.filter((x) => x !== o); if (this._active === o) this._active = null; }
    getObjects() { return this._objects; }
    getActiveObject() { return this._active; }
    getActiveObjects() { return this._active ? [this._active] : []; }
    setActiveObject(o) { this._active = o; }
    discardActiveObject() { this._active = null; }
}
global.fabric = {
    Canvas: FakeCanvas, Image: FakeImage,
    Text: class extends FakeObject { constructor(text, props) { super(); this.text = text; Object.assign(this, props); } },
    Point: function (x, y) { this.x = x; this.y = y; },
};

vm.runInThisContext(source);
const pc = global.puzzleCanvas;

const add = (sys, x, y, angle) => pc.addFragment(
    sys + ',1r', '', x, y, angle, 0.5, false, false, { fl_id: sys + 'r', sys_id: sys }, false, true);
const r3 = (n) => Math.round(n * 1000) / 1000;
const shown = () => pc.canvas.getObjects().filter((o) => o._fragmentKey).map((o) => ({
    key: o._fragmentKey, side: o._fragmentMeta.fl_id.slice(-1), x: r3(o.cx), y: r3(o.cy), angle: o.angle,
    scale: o.scaleX, flipX: !!o.flipX,
    crop: [r3(o.cropX), r3(o.cropY), r3(o.width), r3(o.height)],
}));
const saves = () => events.filter((e) => e.name === 'puzzle-object-modified').length;
const fresh = async () => {
    pc.clearAll(); waiting.length = 0; folioWaiting.length = 0; holdFolios = false; failing.clear();
    add('A', 100, 200, 10); add('B', 500, 300, 350);
    await releaseAll();
    const a = pc.fragments['A,1r'];
    // 100 off the left, 40 off the right, 102 off the top, 98 off the bottom.
    a.set({ cropX: 100, width: 660, cropY: 102, height: 800 });
    a.setPositionByOrigin({ x: 100, y: 200 });
    events.length = 0;
};

(async () => {
    await flush();
    const out = {};

    await fresh();
    out.start = shown();
    pc.flipAllPuzzle();
    out.busyWhileFlipping = pc.isBusy();
    out.savesWhileFlipping = saves();
    await releaseAll();
    out.flipped = shown();
    out.busyAfter = pc.isBusy();
    out.savesAfter = saves();
    pc.flipAllPuzzle(); await releaseAll();
    out.flippedBack = shown();

    // Flipped twice before the first side arrived: nothing may change.
    await fresh();
    pc.flipAllPuzzle(); pc.flipAllPuzzle();
    out.picturesAskedFor = waiting.length;   // the second flip asks for the side already shown
    await releaseAll();
    out.twiceWhileLoading = shown();

    // Three flips, the pictures of the FIRST flip answering last.
    await fresh();
    pc.flipAllPuzzle(); pc.flipAllPuzzle(); pc.flipAllPuzzle();
    while (waiting.length) { waiting.pop().release(); await flush(); }
    out.thriceNewestFirst = shown();
    out.wanted = Object.values(pc.folioData).map((d) => d.currentIndex);

    // The other side's picture fails: the fragment is shown mirrored in its
    // place, and flipping back shows it as it was.
    await fresh();
    failing.add('Bv');
    pc.flipAllPuzzle(); await releaseAll();
    out.otherSideFailed = shown().find((o) => o.key === 'B,1r');
    out.wantedAfterFailure = pc.folioData['B,1r'].currentIndex;
    failing.clear();
    pc.flipAllPuzzle(); await releaseAll();
    out.afterFailureFlippedBack = shown();

    // Flipped before the list of sides arrived, then again after it did.
    pc.clearAll(); waiting.length = 0; holdFolios = true;
    add('A', 100, 200, 10); await releaseAll();
    pc.flipAllPuzzle();
    while (folioWaiting.length) folioWaiting.pop()();
    await flush();
    pc.flipAllPuzzle(); await flush(); await releaseAll();
    out.flippedBeforeItsSidesWereKnown = shown();
    holdFolios = false;

    // Removed and added again; the first one's list of sides answers late,
    // after the new one has been turned over.
    pc.clearAll(); waiting.length = 0; holdFolios = true;
    add('A', 100, 200, 0); await releaseAll();
    const lateList = folioWaiting.pop();
    pc.canvas.setActiveObject(pc.fragments['A,1r']); pc.removeSelected();
    add('A', 100, 200, 0); await releaseAll();
    folioWaiting.pop()(); await flush();
    pc.navigateFolio('A,1r', 1);
    lateList(); await flush();
    await releaseAll();
    out.lateList = { wanted: pc.folioData['A,1r'].currentIndex, shownIndex: pc.fragments['A,1r']._folioIndex,
                     side: shown()[0].side };
    holdFolios = false;

    // Another threshold asked for while the other side is on its way, naming
    // the folio the caller knew: the picture shown is the one reloaded, and
    // the folio wanted goes back to it.
    await fresh();
    pc.navigateFolio('B,1r', 1);
    pc._reloadFragment('B,1r', '', { fl_id: 'ZZr', threshold: 77 });
    await releaseAll();
    const b = pc.fragments['B,1r'];
    out.reloadDuringTurn = { side: b._fragmentMeta.fl_id, threshold: b._fragmentMeta.threshold,
                             wanted: pc.folioData['B,1r'].currentIndex, shownIndex: b._folioIndex };
    out.nextAfterReload = await (async () => { const p = pc.navigateFolio('B,1r', 1); await releaseAll(); return p; })();
    out.sideAfterNext = pc.fragments['B,1r']._fragmentMeta.fl_id;

    // A removed fragment does not come back when its picture arrives.
    await fresh();
    pc.navigateFolio('A,1r', 1);
    pc.canvas.setActiveObject(pc.fragments['A,1r']); pc.removeSelected();
    await releaseAll();
    out.afterRemove = shown().map((o) => o.key);
    out.busyAfterRemove = pc.isBusy();

    // Cropping again a fragment cropped before: measured against the whole
    // picture, opened up beyond the earlier crop, and reverted to it.
    await fresh();
    pc.canvas.setActiveObject(pc.fragments['A,1r']);
    pc.toggleCropMode(true);
    out.cropStateInSession = JSON.parse(pc.getCropState())['A,1r'];
    pc.cropEdge('right', -40);
    out.openedUp = pc.fragments['A,1r'].width;
    pc.fragments['A,1r'].left = 99;              // as dragging the left handle moves the corner
    pc.cropRevert();
    out.cornerAfterRevert = pc.fragments['A,1r'].left;
    out.afterRevert = shown()[0];
    out.cropStateAfter = JSON.parse(pc.getCropState())['A,1r'];

    // Deleted while being cropped: the next fragment can be cropped.
    await fresh();
    pc.canvas.setActiveObject(pc.fragments['A,1r']);
    pc.toggleCropMode(true);
    pc.removeSelected();
    out.croppingAfterDelete = pc._cropMode;
    pc.canvas.setActiveObject(pc.fragments['B,1r']);
    pc.toggleCropMode(true);
    pc.cropEdge('left', 50);
    out.cropAfterDelete = pc.fragments['B,1r'].cropX;
    pc.cropRevert();

    // Cleared after the picture's bytes arrived but before the browser had
    // decoded them: still not drawn.
    pc.clearAll(); waiting.length = 0;
    add('OLD', 100, 100, 0);
    waiting.pop().release();
    for (let i = 0; i < 20; i++) await null;      // the bytes are in; the decode is still to come
    pc.clearAll();
    await flush();
    out.clearedWhileDecoding = shown().map((o) => o.key);

    // A picture asked for by the canvas that was cleared is not drawn on the next one.
    pc.clearAll(); waiting.length = 0;
    add('OLD', 100, 100, 0);
    const late = waiting.pop();
    pc.clearAll();
    add('NEW', 300, 300, 0);
    late.release(); await flush();
    await releaseAll();
    out.afterClear = shown().map((o) => o.key);
    out.leftovers = pc.canvas.getObjects().filter((o) => !o._fragmentKey).length;

    // The same fragment asked for twice while loading is one fragment.
    pc.clearAll(); waiting.length = 0; events.length = 0;
    add('A', 100, 100, 0); add('A', 400, 400, 0);
    await releaseAll();
    out.addedTwice = shown().map((o) => [o.key, o.x]);
    out.addResults = events.filter((e) => e.name === 'puzzle-add-result').length;

    // Fragments queued before the canvas existed go with the canvas they were for.
    const canvas = pc.canvas; pc.canvas = null;
    add('QUEUED', 100, 100, 0);
    pc.clearAll();
    out.queuedAfterClear = pc._pendingAdds.length;
    pc.canvas = canvas;

    process.stdout.write(JSON.stringify(out) + '\n');
    process.exit(0);
})().catch((e) => { process.stderr.write(String(e && e.stack || e) + '\n'); process.exit(1); });
'''


def _script_body(page_script: str) -> str:
    body = page_script.replace('__EXT_IMAGE_VERSION__', '')
    assert body.count('<script>') == 1 and body.count('</script>') == 1
    return body.replace('<script>', '').replace('</script>', '')


@pytest.fixture(scope='module')
def played(tmp_path_factory):
    folder = tmp_path_factory.mktemp('puzzle_canvas_js')
    script = folder / 'puzzle_canvas.js'
    harness = folder / 'harness.js'
    script.write_text(_script_body(puzzle_page.PUZZLE_CANVAS_JS), encoding='utf-8', newline='\n')
    harness.write_text(HARNESS, encoding='utf-8', newline='\n')
    done = subprocess.run([NODE, str(harness), str(script)], capture_output=True, text=True,
                          encoding='utf-8', timeout=120)
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def _frag(key, side, x, y, angle, crop, flip_x=False):
    return {'key': key, 'side': side, 'x': x, 'y': y, 'angle': angle, 'scale': 0.5,
            'flipX': flip_x, 'crop': crop}


# A is cropped: 100 off the left, 40 off the right, 102 off the top, 98 off the bottom of 800 x 1000.
START = [
    _frag('A,1r', 'r', 100, 200, 10, [100, 102, 660, 800]),
    _frag('B,1r', 'r', 500, 300, 350, [0, 0, 800, 1000]),
]
# Mirrored about x = 300 (half way between the two centres), angles negated, the
# other side shown. The verso picture is 800 x 800, so A's crop is mirrored (40
# off the left) and scaled by 0.8 in height: 81.6 off the top, 640 high.
FLIPPED = [
    _frag('A,1r', 'v', 500, 200, 350, [40, 81.6, 660, 640]),
    _frag('B,1r', 'v', 100, 300, 10, [0, 0, 800, 800]),
]


def test_the_harness_starts_from_the_layout_the_cases_assume(played):
    assert played['start'] == START


def test_a_flip_mirrors_the_places_and_the_crop_and_shows_the_other_side(played):
    assert played['flipped'] == FLIPPED


def test_flipping_back_restores_the_puzzle_exactly_between_pictures_of_two_sizes(played):
    assert played['flippedBack'] == START


def test_a_half_flipped_canvas_is_busy_and_reports_its_edit_only_when_settled(played):
    """Python does not save a busy canvas, and saves on the edit reported at the end."""
    assert played['busyWhileFlipping'] is True and played['savesWhileFlipping'] == 0
    assert played['busyAfter'] is False and played['savesAfter'] >= 1


def test_two_flips_before_the_first_side_arrives_change_nothing(played):
    assert played['picturesAskedFor'] == 2
    assert played['twiceWhileLoading'] == START


def test_three_flips_end_on_the_other_side_whatever_order_the_pictures_answer_in(played):
    assert played['thriceNewestFirst'] == FLIPPED
    assert played['wanted'] == [1, 1]


def test_a_side_that_fails_to_load_is_shown_mirrored_and_flips_back(played):
    assert played['otherSideFailed'] == _frag('B,1r', 'r', 100, 300, 10, [0, 0, 800, 1000], flip_x=True)
    assert played['wantedAfterFailure'] == 0
    assert played['afterFailureFlippedBack'] == START


def test_a_flip_before_the_sides_are_known_is_undone_by_the_next_flip(played):
    assert played['flippedBeforeItsSidesWereKnown'] == [_frag('A,1r', 'r', 100, 200, 10, [0, 0, 800, 1000])]


def test_a_late_list_of_sides_for_a_removed_fragment_changes_nothing(played):
    assert played['lateList'] == {'wanted': 1, 'shownIndex': 1, 'side': 'v'}


def test_a_reload_keeps_the_picture_shown_and_cancels_the_turn_in_progress(played):
    assert played['reloadDuringTurn'] == {'side': 'Br', 'threshold': 77, 'wanted': 0, 'shownIndex': 0}
    assert played['nextAfterReload'] == '1v' and played['sideAfterNext'] == 'Bv'


def test_a_removed_fragment_does_not_come_back_with_its_picture(played):
    assert played['afterRemove'] == ['B,1r']
    assert played['busyAfterRemove'] is False


def test_cropping_again_measures_against_the_whole_picture_and_reverts_to_the_earlier_crop(played):
    whole = {'cropX': 100, 'cropY': 102, 'width': 660, 'height': 800,
             'originalWidth': 800, 'originalHeight': 1000}
    assert played['cropStateInSession'] == whole
    assert played['openedUp'] == 700
    assert played['afterRevert'] == START[0]
    assert played['cornerAfterRevert'] == 100, 'Revert left the corner where the handle had dragged it'
    assert played['cropStateAfter'] == whole


def test_deleting_the_fragment_being_cropped_ends_the_crop(played):
    assert played['croppingAfterDelete'] is False
    assert played['cropAfterDelete'] == 50


def test_a_canvas_cleared_while_a_picture_is_being_decoded_stays_clear(played):
    assert played['clearedWhileDecoding'] == []


def test_a_picture_for_a_cleared_canvas_is_not_drawn_on_the_next_one(played):
    assert played['afterClear'] == ['NEW,1r']
    assert played['leftovers'] == 0, 'a "Loading..." text was left on the canvas'
    assert played['queuedAfterClear'] == 0


def test_a_fragment_asked_for_twice_while_loading_is_drawn_once(played):
    assert played['addedTwice'] == [['A,1r', 400]]
    assert played['addResults'] == 1


def test_the_one_time_address_parameters_are_dropped_by_name():
    script = puzzle_page._drop_query_param_js('add')
    assert 'u.searchParams.has("add")' in script and 'u.searchParams.delete("add")' in script
    assert 'history.replaceState' in script and 'location.reload' not in script
    assert script.count('{') == script.count('}') and script.count('(') == script.count(')')
