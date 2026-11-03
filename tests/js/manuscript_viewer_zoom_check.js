// The viewer's zoom arithmetic, run by node against the real
// web/static/manuscript_viewer.js with a small fake DOM. Driven by
// tests/test_manuscript_viewer_js.py; exits non-zero on the first failure.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

class FakeElement {
    constructor(tag, rect) {
        this.tagName = tag;
        this.style = {};
        this.children = [];
        this.dataset = {};
        this.listeners = {};
        this.rect = rect || { left: 0, top: 0, width: 0, height: 0 };
        this.textContent = '';
        this.className = '';
        this.isConnected = true;
        this.parentElement = null;
        this.display = 'block';
    }
    addEventListener(type, fn) { (this.listeners[type] = this.listeners[type] || []).push(fn); }
    removeEventListener(type, fn) { this.listeners[type] = (this.listeners[type] || []).filter((f) => f !== fn); }
    dispatch(type, event) { (this.listeners[type] || []).forEach((fn) => fn(event)); }
    getBoundingClientRect() { return this.rect; }
    appendChild(child) { this.children.push(child); child.parentElement = this; return child; }
    setAttribute() {}
    querySelectorAll() { return []; }
}

function translateOf(transform) {
    const m = /translate\(\s*(-?[\d.]+)px\s*,\s*(-?[\d.]+)px\s*\)/.exec(transform || '');
    return m ? { x: parseFloat(m[1]), y: parseFloat(m[2]) } : { x: 0, y: 0 };
}

// A page with one image in one container, a toolbar label and the
// browser-level functions the viewer reaches for.
function makePage(opts) {
    opts = opts || {};
    const image = new FakeElement('IMG', opts.imageRect || { left: 100, top: 50, width: 400, height: 300 });
    const container = new FakeElement('DIV', opts.containerRect || { left: 100, top: 50, width: 400, height: 300 });
    const label = new FakeElement('SPAN');
    if (opts.labelHidden) label.display = 'none';
    const byId = {};
    const document = {
        querySelector(sel) {
            if (sel === '.image') return image;
            if (sel === '.box') return container;
            if (sel === '.label') return opts.noLabel ? null : label;
            return null;
        },
        getElementById(id) { return byId[id] || null; },
        createElement(tag) { return new FakeElement(tag.toUpperCase()); },
    };
    const sandbox = {
        document,
        console: { log() {}, warn() {}, error() {} },
        setTimeout, clearTimeout,
        requestAnimationFrame(fn) { fn(); },
        // The drawn transform: the inline one, which the fake page draws at once.
        getComputedStyle(el) {
            const t = translateOf(el.style.transform);
            return {
                transform: el.style.transform ? `matrix(1, 0, 0, 1, ${t.x}, ${t.y})` : 'none',
                display: el.display,
            };
        },
        DOMMatrix: class {
            constructor(text) {
                const m = /matrix\(([^)]*)\)/.exec(text);
                const parts = m ? m[1].split(',').map(parseFloat) : [1, 0, 0, 1, 0, 0];
                [this.a, this.b, this.c, this.d, this.e, this.f] = parts;
            }
        },
        fetch() { return Promise.reject(new Error('no network in the check')); },
        Date, Math,
    };
    sandbox.window = sandbox;
    vm.createContext(sandbox);
    const src = fs.readFileSync(path.join(__dirname, '..', '..', 'web', 'static', 'manuscript_viewer.js'), 'utf8');
    vm.runInContext(src, sandbox, { filename: 'manuscript_viewer.js' });
    const changes = [];
    const viewer = sandbox.createManuscriptViewer({
        imageSelector: '.image', containerSelector: '.box', zoomLabelSelector: '.label',
        gammaFilterId: 'gamma', onChange: (v) => changes.push(v.state.scale),
    });
    viewer.init();
    return { viewer, image, container, label, changes, constants: {
        factor: vm.runInContext('ZOOM_FACTOR', sandbox),
        min: vm.runInContext('MIN_ZOOM', sandbox),
        max: vm.runInContext('MAX_ZOOM', sandbox),
        dblclick: vm.runInContext('DOUBLE_CLICK_ZOOM', sandbox),
        notifyMs: vm.runInContext('ZOOM_NOTIFY_MS', sandbox),
    } };
}

const near = (a, b, eps) => Math.abs(a - b) <= (eps || 1e-9);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const checks = {
    'a click multiplies the level and the label follows'() {
        const { viewer, label, constants } = makePage();
        assert.equal(constants.factor, 1.5);
        viewer.zoomIn();
        assert.equal(viewer.state.scale, 1.5);
        assert.equal(label.textContent, '150%');
        viewer.zoomIn();
        assert.equal(viewer.state.scale, 2.25);
        assert.equal(label.textContent, '225%');
        viewer.zoomOut();
        assert.ok(near(viewer.state.scale, 1.5));
        viewer.zoomOut(); viewer.zoomOut();
        assert.ok(near(viewer.state.scale, 1 / 1.5));
        assert.equal(label.textContent, '67%');
    },

    'five clicks reach the cap and the floor holds'() {
        const { viewer, constants } = makePage();
        for (let i = 0; i < 5; i++) viewer.zoomIn();
        assert.ok(near(viewer.state.scale, 7.59375));
        viewer.zoomIn();
        assert.equal(viewer.state.scale, constants.max);
        assert.equal(constants.max, 8);
        viewer.zoomIn();
        assert.equal(viewer.state.scale, 8);
        for (let i = 0; i < 20; i++) viewer.zoomOut();
        assert.equal(viewer.state.scale, constants.min);
        assert.equal(constants.min, 0.25);
    },

    'a zoom at a point keeps that point of the page under the pointer'() {
        const { viewer, image } = makePage();
        // Image centre (300, 200); the pointer 50px right and below it.
        viewer.zoomBy(2, 350, 250);
        assert.equal(viewer.state.scale, 2);
        assert.equal(viewer.state.x, -50);
        assert.equal(viewer.state.y, -50);
        assert.equal(image.style.transform, 'translate(-50px, -50px) rotate(0deg) scale(2)');
        // The page point under (350, 250) was 50px from the centre; now drawn
        // at centre + translate + 2 * 50 = 350 again.
        const drawnCentre = { x: 300 + viewer.state.x, y: 200 + viewer.state.y };
        assert.equal(drawnCentre.x + 2 * 50, 350);
        assert.equal(drawnCentre.y + 2 * 50, 250);
        // Once more from the moved state: the maths holds with a translate in place.
        // The drawn box at 2x: 800x600 centred on centre + translate = (250, 150).
        image.rect = { left: 250 - 400, top: 150 - 300, width: 800, height: 600 };
        viewer.zoomBy(1.5, 350, 250);
        assert.ok(near(viewer.state.scale, 3));
        // translate' = translate * k + (anchor - centre) * (1 - k), k = 1.5, centre still (300, 200)
        assert.ok(near(viewer.state.x, -50 * 1.5 + 50 * (1 - 1.5)));
        assert.ok(near(viewer.state.y, -50 * 1.5 + 50 * (1 - 1.5)));
    },

    'a button zoom anchors at the middle of the box'() {
        // The image sits in the left half of a wide box: centre 300, box centre 400.
        const { viewer } = makePage({ containerRect: { left: 0, top: 50, width: 800, height: 300 } });
        viewer.zoomIn();
        assert.ok(near(viewer.state.x, (400 - 300) * (1 - 1.5)));
        assert.equal(viewer.state.y, 0);
        // Centred image, centred box: a button zoom never moves it.
        const centred = makePage();
        centred.viewer.zoomIn(); centred.viewer.zoomIn();
        assert.equal(centred.viewer.state.x, 0);
        assert.equal(centred.viewer.state.y, 0);
    },

    'the wheel zooms at the pointer, a notch at a time, never more than a click'() {
        const { viewer } = makePage();
        let prevented = 0;
        const wheel = (deltaY, deltaMode) => viewer.onWheel({
            deltaY, deltaMode: deltaMode || 0, clientX: 350, clientY: 250, preventDefault() { prevented++; },
        });
        wheel(100);                                   // one notch down: out
        assert.ok(near(viewer.state.scale, Math.exp(-0.2), 1e-12));
        assert.equal(prevented, 1);
        viewer.fit();
        wheel(-100);                                  // one notch up: in, at the pointer
        assert.ok(near(viewer.state.scale, Math.exp(0.2), 1e-12));
        assert.ok(viewer.state.x < 0 && viewer.state.y < 0, 'anchored at the pointer, not the centre');
        viewer.fit();
        wheel(-5000);                                 // a fling: one click, not a jump to the cap
        assert.ok(near(viewer.state.scale, 1.5));
        viewer.fit();
        wheel(-3, 1);                                 // three lines (Firefox): 48px
        assert.ok(near(viewer.state.scale, Math.exp(48 * 0.002), 1e-12));
    },

    'a double-click zooms to 2x at the pointer and the next one fits'() {
        const { viewer, constants } = makePage();
        assert.equal(constants.dblclick, 2);
        viewer.onDoubleClick({ clientX: 350, clientY: 250, preventDefault() {} });
        assert.equal(viewer.state.scale, 2);
        assert.equal(viewer.state.x, -50);
        viewer.onDoubleClick({ clientX: 350, clientY: 250, preventDefault() {} });
        assert.equal(viewer.state.scale, 1);
        assert.equal(viewer.state.x, 0);
        assert.equal(viewer.state.y, 0);
        // From any zoomed level, a double-click fits; from fit, it goes to 2x whatever came before.
        viewer.zoomIn();
        viewer.onDoubleClick({ clientX: 300, clientY: 200, preventDefault() {} });
        assert.equal(viewer.state.scale, 1);
        viewer.state.rotation = 90;
        viewer.onDoubleClick({ clientX: 300, clientY: 200, preventDefault() {} });
        assert.equal(viewer.state.scale, 2);
        assert.equal(viewer.state.rotation, 90, 'the rotation stays');
    },

    'a double-tap does the same and keeps the click pair from the browser'() {
        const { viewer, image } = makePage();
        let prevented = 0;
        const tap = () => {
            const touch = { clientX: 350, clientY: 250 };
            image.dispatch('touchstart', { touches: [touch], preventDefault() { prevented++; } });
            image.dispatch('touchend', { touches: [], preventDefault() { prevented++; } });
        };
        tap();
        assert.equal(viewer.state.scale, 1, 'one tap is a tap');
        assert.equal(prevented, 0, 'a single finger on the whole page is left to the page');
        tap();
        assert.equal(viewer.state.scale, 2);
        assert.equal(viewer.state.x, -50);
        assert.equal(prevented, 1, 'the second touchend is taken');
        // The dblclick some browsers still send right after is not a third toggle.
        viewer.onDoubleClick({ clientX: 350, clientY: 250, preventDefault() {} });
        assert.equal(viewer.state.scale, 2);
    },

    'a pinch scales from the pinch start and clamps at the cap'() {
        const { viewer, label } = makePage();
        const t = (x1, x2) => [{ clientX: x1, clientY: 100 }, { clientX: x2, clientY: 100 }];
        const ev = (touches) => ({ touches, preventDefault() {} });
        viewer.onTouchStart(ev(t(100, 200)));
        viewer.onTouchMove(ev(t(100, 400)));
        assert.equal(viewer.state.scale, 3);
        assert.equal(label.textContent, '300%');
        viewer.onTouchMove(ev(t(0, 2000)));
        assert.equal(viewer.state.scale, 8);
        viewer.onTouchEnd(ev([]));
        assert.equal(viewer.state.isDragging, false);
    },

    async 'a click reports at once; the wheel once, after the last notch, with the level it ended on'() {
        const { viewer, changes, constants } = makePage();
        viewer.zoomIn(); viewer.zoomIn(); viewer.zoomIn();
        assert.deepEqual(changes, [1.5, 2.25, 3.375], 'every click is reported as it happens');
        assert.equal(viewer.reportSeq, 3);
        const wheel = (deltaY) => viewer.onWheel({ deltaY, deltaMode: 0, clientX: 300, clientY: 200, preventDefault() {} });
        wheel(-100); wheel(-100); wheel(-100);
        assert.deepEqual(changes, [1.5, 2.25, 3.375], 'nothing before the pause');
        await sleep(constants.notifyMs + 60);
        assert.equal(changes.length, 4);
        assert.ok(near(changes[3], 3.375 * Math.exp(0.6), 1e-9));
        assert.equal(viewer.reportSeq, 4);
        // A fit reports at once and cancels a level still waiting: the browse
        // page fits on a folio change, and the last notch's level must not
        // land after it.
        wheel(-100);
        viewer.fit();
        assert.equal(changes.length, 5);
        assert.equal(changes[4], 1);
        await sleep(constants.notifyMs + 60);
        assert.equal(changes.length, 5);
        viewer.zoomIn();
        viewer.reset();
        assert.deepEqual(changes.slice(5), [1.5, 1]);
        // A level pushed in by the server is not reported back.
        viewer.update(2, 0);
        viewer.setRotation(90);
        await sleep(constants.notifyMs + 60);
        assert.equal(changes.length, 7);
    },

    async 'an image the server drew before hearing the latest level is not taken over'() {
        // The page was rebuilt (Show Metadata, Edit) right after a click: the
        // server drew the new image at the level it last heard, numbered
        // behind this viewer's last report (Codex on #471).
        const page = makePage();
        page.viewer.zoomIn();
        assert.equal(page.viewer.reportSeq, 1);
        page.image.style.transform = 'translate(0px, 0px) rotate(0deg) scale(1.0)';
        page.image.dataset.zoomSeq = '0';
        page.viewer.el = null;
        page.viewer._boundEl = {};  // a different element than the one found now
        page.viewer.init();
        assert.equal(page.viewer.state.scale, 1.5, 'the live level stays');
        assert.equal(page.image.style.transform, 'translate(0px, 0px) rotate(0deg) scale(1.5)', 'and goes back onto the new image');
        assert.equal(page.label.textContent, '150%');
        assert.deepEqual(page.changes, [1.5, 1.5], 'and is said again');
        assert.equal(page.viewer.reportSeq, 2);
        // Drawn after the latest report: the server's level is the newer one (a folio change reset it).
        page.image.style.transform = 'translate(0px, 0px) rotate(0deg) scale(1.0)';
        page.image.dataset.zoomSeq = '2';
        page.viewer.el = null;
        page.viewer._boundEl = {};
        page.viewer.init();
        assert.equal(page.viewer.state.scale, 1);
        // A wheel report still waiting counts as newer than anything drawn.
        page.viewer.onWheel({ deltaY: -100, deltaMode: 0, clientX: 300, clientY: 200, preventDefault() {} });
        const pending = page.viewer.state.scale;
        page.image.style.transform = 'translate(0px, 0px) rotate(0deg) scale(1.0)';
        page.image.dataset.zoomSeq = '2';
        page.viewer.el = null;
        page.viewer._boundEl = {};
        page.viewer.init();
        assert.equal(page.viewer.state.scale, pending);
        assert.equal(page.changes.length, 3, 'reported at once, not after the pause');
        await sleep(page.constants.notifyMs + 60);
        assert.equal(page.changes.length, 3);
        // A viewer meeting its first element takes it over whatever the number says.
        const fresh = makePage();
        fresh.image.style.transform = 'translate(0px, 0px) rotate(0deg) scale(3)';
        fresh.image.dataset.zoomSeq = '0';
        fresh.viewer.el = null;
        fresh.viewer._boundEl = undefined;
        fresh.viewer.init();
        assert.equal(fresh.viewer.state.scale, 3);
    },

    'a new element is taken over at the transform it was drawn with'() {
        const page = makePage();
        page.viewer.zoomIn();
        // The page was rebuilt: a new image, drawn by the server at 225% turned by 90.
        const fresh = new FakeElement('IMG', page.image.rect);
        fresh.style.transform = 'translate(0px, 0px) rotate(90deg) scale(2.25)';
        fresh.dataset.zoomSeq = String(page.viewer.reportSeq);  // drawn after the click was heard
        page.image.rect = null;
        const image = page.image;
        Object.assign(image, fresh);  // the selector now finds the new element's state
        page.viewer.el = null;
        page.viewer._boundEl = null;
        page.viewer.init();
        assert.equal(page.viewer.state.scale, 2.25);
        assert.equal(page.viewer.state.rotation, 90);
        assert.equal(page.label.textContent, '225%');
        page.viewer.zoomIn();
        assert.ok(near(page.viewer.state.scale, 3.375), 'the next click steps from what is on screen');
        // init() on the SAME element keeps the live state.
        page.viewer.init();
        assert.ok(near(page.viewer.state.scale, 3.375));
    },

    'where the toolbar figure is hidden a badge over the image shows the level'() {
        const hidden = makePage({ labelHidden: true });
        hidden.viewer.zoomIn();
        const badge = hidden.container.children.find((c) => c.className === 'zoom-badge');
        assert.ok(badge, 'a badge in the box');
        assert.equal(badge.textContent, '150%');
        assert.equal(badge.style.opacity, '1');
        const shown = makePage();
        shown.viewer.zoomIn();
        assert.equal(shown.container.children.length, 0, 'no badge beside a visible label');
        // A folio change fits an unzoomed page: nothing to flash.
        const quiet = makePage({ labelHidden: true });
        quiet.viewer.fit();
        assert.equal(quiet.container.children.length, 0, 'no badge for a fit that changed nothing');
        quiet.viewer.zoomIn();
        quiet.viewer.fit();
        const flashed = quiet.container.children.find((c) => c.className === 'zoom-badge');
        assert.equal(flashed.textContent, '100%');
    },
};

(async () => {
    let failed = 0;
    for (const [name, fn] of Object.entries(checks)) {
        try {
            await fn();
            console.log('ok   ' + name);
        } catch (err) {
            failed++;
            console.log('FAIL ' + name + '\n' + (err && err.stack || err));
        }
    }
    if (failed) {
        console.log(failed + ' check(s) failed');
        process.exit(1);
    }
    console.log('all ' + Object.keys(checks).length + ' checks passed');
})();
