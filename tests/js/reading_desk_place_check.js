// The Reading Desk keeps the reader's place when it is redrawn: run by node against the real
// web/static/reading_desk_place.js with a small fake DOM. Driven by
// tests/test_reading_desk_place_js.py; exits non-zero on the first failure.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const SOURCE = fs.readFileSync(path.join(__dirname, '..', '..', 'web', 'static', 'reading_desk_place.js'), 'utf8');

class FakeElement {
    constructor(doc, id, classes, rect) {
        this.doc = doc;
        this.id = id || '';
        this.classes = new Set(classes || []);
        this.parent = null;
        this.rect = rect || { top: 0 };
        this.scrollTop = 0;
        this.scrolledTo = [];
        this.intoView = 0;
    }
    append(child) { child.parent = this; return child; }
    closest(selector) {
        const cls = selector.replace(/^\./, '');
        for (let el = this; el; el = el.parent) if (el.classes.has(cls)) return el;
        return null;
    }
    contains(other) {
        for (let el = other; el; el = el.parent) if (el === this) return true;
        return false;
    }
    focus() { this.doc.activeElement = this; }
    blur() { if (this.doc.activeElement === this) this.doc.activeElement = this.doc.body; }
    getBoundingClientRect() { return this.rect; }
    scrollTo(options) { this.scrolledTo.push(JSON.parse(JSON.stringify(options))); }  // out of the vm's realm
    scrollIntoView() { this.intoView += 1; this.doc.pageScrolled += 1; }
}

function makePage() {
    const timers = [];
    const observers = [];
    const doc = {
        listeners: [], pageScrolled: 0, byId: {}, bySelector: {}, all: {},
        addEventListener(type, fn, capture) { this.listeners.push({ type, fn, capture }); },
        querySelector(sel) { return this.bySelector[sel] || null; },
        querySelectorAll(sel) { return this.all[sel] || []; },
        getElementById(id) { return this.byId[id] || null; },
    };
    doc.body = new FakeElement(doc, 'body');
    doc.activeElement = doc.body;
    class FakeObserver {
        constructor(callback, options) {
            this.callback = callback; this.options = options; this.targets = []; this.disconnected = false;
            observers.push(this);
        }
        observe(target) { this.targets.push(target); }
        disconnect() { this.disconnected = true; this.targets = []; }
    }
    const window = {
        scrolls: 0,
        scrollTo() { this.scrolls += 1; },
    };
    const context = {
        window, document: doc, IntersectionObserver: FakeObserver,
        setTimeout(fn, ms) { const timer = { fn, ms, id: timers.nextId = (timers.nextId || 0) + 1 }; timers.push(timer); return timer.id; },
        clearTimeout(id) { const i = timers.findIndex((t) => t.id === id); if (i >= 0) timers.splice(i, 1); },
        console,
    };
    vm.createContext(context);
    const page = { context, window, doc, timers, observers };
    page.load = () => vm.runInContext(SOURCE, context);
    page.flush = () => { while (timers.length) timers.shift().fn(); };
    page.click = (target, detail) => {
        for (const l of doc.listeners) if (l.type === 'click') l.fn({ target, detail: detail === undefined ? 1 : detail });
    };
    return page;
}

let checks = 0;
function check(name, fn) { fn(); checks += 1; console.log('ok', name); }

check('a click on a control that redraws the desk lets go of its focus', () => {
    const page = makePage();
    page.load();
    const toggle = new FakeElement(page.doc, '', ['rd-image-view', 'rd-redraws']);
    const button = toggle.append(new FakeElement(page.doc, '', ['q-btn']));
    const helper = button.append(new FakeElement(page.doc, '', ['q-focus-helper']));
    helper.focus();  // where a mouse click leaves Quasar's focus
    page.click(button);
    assert.equal(page.doc.activeElement, helper, 'not before the click has been handled');
    page.flush();
    assert.equal(page.doc.activeElement, page.doc.body);
    // A keyboard click (Enter, Space) too: the control is destroyed either way.
    helper.focus();
    page.click(button, 0);
    page.flush();
    assert.equal(page.doc.activeElement, page.doc.body);
});

check('other controls, and focus elsewhere, are left alone', () => {
    const page = makePage();
    page.load();
    const zoom = new FakeElement(page.doc, '', ['q-btn']);  // zoom, rotate: the desk is not redrawn
    zoom.focus();
    page.click(zoom);
    page.flush();
    assert.equal(page.doc.activeElement, zoom);
    const toggle = new FakeElement(page.doc, '', ['rd-redraws']);
    const box = new FakeElement(page.doc, '', ['q-input']);
    box.focus();  // the focus is not in the control clicked
    page.click(toggle);
    page.flush();
    assert.equal(page.doc.activeElement, box);
});

check('the listener is installed once however often the script is loaded', () => {
    const page = makePage();
    page.load();
    page.load();
    assert.equal(page.doc.listeners.filter((l) => l.type === 'click' && l.capture === true).length, 1);
});

function deskWithPanes(page) {
    const imgPane = new FakeElement(page.doc, '', ['q-scrollarea__container'], { top: 100 });
    const textPane = new FakeElement(page.doc, '', ['q-scrollarea__container'], { top: 100 });
    textPane.scrollTop = 40;
    page.doc.bySelector['.rd-image-pane .q-scrollarea__container'] = imgPane;
    page.doc.bySelector['.rd-text-pane .q-scrollarea__container'] = textPane;
    const imgs = [0, 1].map((n) => new FakeElement(page.doc, `rd-img-frag-${n}`, [], { top: 100 + 500 * n }));
    const texts = [0, 1].map((n) => new FakeElement(page.doc, `rd-text-frag-${n}`, [], { top: 100 + 700 * n }));
    for (const el of imgs.concat(texts)) page.doc.byId[el.id] = el;
    page.doc.all['[id^="rd-img-frag-"]'] = imgs;
    page.doc.all['[id^="rd-text-frag-"]'] = texts;
    return { imgPane, textPane, imgs, texts };
}

check('the panes follow each other inside themselves, never by scrolling the page', () => {
    const page = makePage();
    page.load();
    const desk = deskWithPanes(page);
    page.window.rdSyncPanes(2, 0);
    page.flush();
    assert.equal(page.observers.length, 2);
    const [imgObserver, textObserver] = page.observers;
    assert.equal(imgObserver.options.root, desk.imgPane);
    assert.equal(textObserver.options.root, desk.textPane);
    assert.deepEqual(imgObserver.targets.map((t) => t.id), ['rd-img-frag-0', 'rd-img-frag-1']);
    // The first report is where the panes already are, as soon as the observers are set up:
    // nothing moves (it used to bring the text side's top into view, page and all).
    imgObserver.callback([{ target: desk.imgs[0], isIntersecting: true }]);
    textObserver.callback([{ target: desk.texts[0], isIntersecting: true }]);
    assert.deepEqual(desk.textPane.scrolledTo, []);
    assert.deepEqual(desk.imgPane.scrolledTo, []);
    // The reader scrolls the images to the second fragment: the text pane follows, by its own
    // scroll -- its offset plus the header's distance from the pane's top.
    imgObserver.callback([{ target: desk.imgs[1], isIntersecting: true }]);
    assert.deepEqual(desk.textPane.scrolledTo, [{ top: 40 + 800 - 100, behavior: 'smooth' }]);
    // While it follows, the other pane's report is not followed back.
    textObserver.callback([{ target: desk.texts[1], isIntersecting: true }]);
    assert.deepEqual(desk.imgPane.scrolledTo, []);
    page.flush();
    textObserver.callback([{ target: desk.texts[0], isIntersecting: true }]);
    assert.deepEqual(desk.imgPane.scrolledTo, [{ top: 0 + 100 - 100, behavior: 'smooth' }]);
    assert.equal(page.window.scrolls, 0, 'the page itself never scrolls');
    assert.equal(page.doc.pageScrolled, 0, 'no scrollIntoView, which scrolls the page too');
});

check("each drawing of the desk replaces the last one's observers", () => {
    const page = makePage();
    page.load();
    deskWithPanes(page);
    page.window.rdSyncPanes(2, 0);
    page.flush();
    const [oldImg, oldText] = page.observers;
    page.window.rdSyncPanes(2, 0);  // the desk drawn again
    assert.ok(oldImg.disconnected && oldText.disconnected, "the last drawing's observers are let go");
    page.flush();
    assert.equal(page.observers.length, 4);
    assert.ok(!page.observers[2].disconnected && !page.observers[3].disconnected);
    // Two drawings close together: the first's setup, still waiting, is not made.
    page.window.rdSyncPanes(2, 0);
    page.window.rdSyncPanes(2, 0);
    page.flush();
    assert.equal(page.observers.length, 6, 'one pair for the two calls');
    assert.ok(page.observers[2].disconnected && page.observers[3].disconnected);
    // A desk of one, drawn after: the last pair is let go and none is made.
    page.window.rdSyncPanes(1, 0);
    page.flush();
    assert.ok(page.observers[4].disconnected && page.observers[5].disconnected);
    assert.equal(page.observers.length, 6);
});

check('a desk of one fragment, or one without both panes, is not synced', () => {
    const page = makePage();
    page.load();
    deskWithPanes(page);
    page.window.rdSyncPanes(1, 0);
    page.flush();
    assert.equal(page.observers.length, 0);
    delete page.doc.bySelector['.rd-text-pane .q-scrollarea__container'];  // One at a time
    page.window.rdSyncPanes(2, 0);
    page.flush();
    assert.equal(page.observers.length, 0);
});

console.log(`all ${checks} checks passed`);
