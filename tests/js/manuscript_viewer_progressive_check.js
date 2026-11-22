// The preview and the full image asked for together (2026-11-22), run by node against the
// real web/static/manuscript_viewer.js with a small fake DOM. Driven by
// tests/test_manuscript_viewer_js.py; exits non-zero on the first failure.
//
// progressiveLoad used to ask for the full image only after the 400-px preview had
// arrived, so the two waits added up. Now both start together and one owner decides what
// is shown: a full image that arrives first wins, a failed full image leaves the preview,
// a fallback (handleImageError) is never overridden, and an image that left the page
// (the browse page draws again after its enrichment) is neither upgraded nor kept
// downloading.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const SYS = '990051340280205171';
const THUMB = `/api/cambridge_image/${SYS}?page=0&width=400`;
const FULL = `/api/cambridge_image/${SYS}?page=0`;

// fullOutcome: 'load' (after fullDelay ms), 'error', or 'never'.
function makePage({ props = {}, fullOutcome = 'load', fullDelay = 0, connection = null } = {}) {
    const listeners = {};
    const container = { loaded: false, classList: { add(c) { if (c === 'img-loaded') container.loaded = true; } } };
    const img = {
        src: THUMB,
        complete: false,
        naturalWidth: 0,
        isConnected: true,
        dataset: {},
        style: {},
        parentElement: null,
        onload: null,
        getAttribute(name) {
            if (name === 'data-full-src') return FULL;
            if (name === 'src') return img.src;
            return null;
        },
        closest() { return container; },
        addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
        removeEventListener(type, fn) { listeners[type] = (listeners[type] || []).filter((f) => f !== fn); },
        fire(type) {
            if (type === 'load' && typeof img.onload === 'function') img.onload();
            (listeners[type] || []).slice().forEach((fn) => fn());
        },
    };
    Object.assign(img, props);
    const document = {
        querySelector() { return null; },
        querySelectorAll(sel) { return sel === 'img[data-full-src]' ? [img] : []; },
        createElement() { return { style: {}, appendChild() {}, setAttribute() {} }; },
    };
    const preloads = [];
    class Image {
        constructor() { preloads.push(this); this.cleared = false; }
        set src(v) {
            this._src = v;
            if (v === '') { this.cleared = true; return; }
            if (fullOutcome === 'load') setTimeout(() => this.onload && this.onload(), fullDelay);
            else if (fullOutcome === 'error') setTimeout(() => this.onerror && this.onerror(), fullDelay);
        }
        get src() { return this._src; }
    }
    const manifest = { sequences: [{ canvases: [
        { images: [{ resource: { service: { '@id': 'https://iiif.nli.org.il/IIIFv21/FL167042239' } } }] },
    ] }] };
    const sandbox = {
        document, Image, console: { log() {}, warn() {}, error() {} }, setTimeout, clearTimeout,
        fetch: async () => ({ ok: true, status: 200, json: async () => manifest }),
    };
    if (connection) sandbox.navigator = { connection };
    sandbox.window = sandbox;
    vm.createContext(sandbox);
    const src = fs.readFileSync(path.join(__dirname, '..', '..', 'web', 'static', 'manuscript_viewer.js'), 'utf8');
    vm.runInContext(src, sandbox, { filename: 'manuscript_viewer.js' });
    return { sandbox, img, container, preloads };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const checks = {
    async 'the full image is asked for together with the preview'() {
        const page = makePage({ fullOutcome: 'never' });
        page.sandbox.initProgressiveImages();
        assert.equal(page.preloads.length, 1, 'no full image was asked for before the preview arrived');
        assert.equal(page.preloads[0].src, FULL);
        page.img.fire('load');                                  // the preview arrives
        assert.equal(page.preloads.length, 1, 'the full image was asked for twice');
        assert.ok(page.container.loaded);
    },
    async 'a full image that arrives first replaces the preview'() {
        const page = makePage();
        page.sandbox.initProgressiveImages();
        await sleep(10);                                        // no preview load event at all
        assert.equal(page.img.src, FULL);
        assert.ok(page.container.loaded, 'the spinner stayed');
    },
    async 'a full image that fails leaves the preview'() {
        const page = makePage({ fullOutcome: 'error' });
        page.sandbox.initProgressiveImages();
        page.img.fire('load');
        await sleep(10);
        assert.equal(page.img.src, THUMB);
    },
    async 'a fallback is never overridden by a late full image'() {
        const page = makePage({ fullDelay: 30 });
        page.sandbox.initProgressiveImages();
        page.img.fire('error');                                 // the preview fails
        await page.sandbox.handleImageError(page.img, SYS, 0, false, null);
        assert.match(page.img.src, /iiif\.nli\.org\.il\/IIIFv21\/FL167042239\//);
        page.img.fire('load');                                  // NLI's image loads
        await sleep(50);                                        // the library's full image arrives
        assert.match(page.img.src, /iiif\.nli\.org\.il/, `the library image came back: ${page.img.src}`);
    },
    async 'an image whose src was changed by someone else is not swapped'() {
        const page = makePage({ fullDelay: 20 });
        page.sandbox.initProgressiveImages();
        page.img.src = '/api/nli_image_by_sysid/1?page=0';
        await sleep(40);
        assert.equal(page.img.src, '/api/nli_image_by_sysid/1?page=0');
    },
    async 'an image no longer in the page starts nothing'() {
        const page = makePage({ props: { isConnected: false } });
        page.sandbox.initProgressiveImages();
        page.img.fire('load');
        await sleep(10);
        assert.equal(page.preloads.length, 0);
        assert.equal(page.img.src, THUMB);
    },
    async 'an image that left the page while its full image downloads is not swapped'() {
        const page = makePage({ fullDelay: 20 });
        page.sandbox.initProgressiveImages();
        page.img.isConnected = false;                           // the page was drawn again
        await sleep(40);
        assert.equal(page.img.src, THUMB);
    },
    async 'the next draw drops the full download of an image that left the page'() {
        const page = makePage({ fullOutcome: 'never' });
        page.sandbox.initProgressiveImages();
        page.img.isConnected = false;
        page.sandbox.initProgressiveImages();                   // the new image's init
        assert.equal(page.preloads[0].cleared, true, 'the old full download kept going');
        assert.equal(page.preloads[0].onload, null);
    },
    async 'a full download of an image still in the page is kept by the next draw'() {
        const page = makePage({ fullOutcome: 'never' });
        page.sandbox.initProgressiveImages();
        page.sandbox.initProgressiveImages();
        assert.equal(page.preloads[0].cleared, false);
    },
    async 'with Save-Data the full image waits for the preview'() {
        const page = makePage({ connection: { saveData: true } });
        page.sandbox.initProgressiveImages();
        assert.equal(page.preloads.length, 0);
        page.img.fire('load');
        await sleep(10);
        assert.equal(page.preloads.length, 1);
        assert.equal(page.img.src, FULL);
    },
    async 'on a 2G connection the full image waits for the preview'() {
        const page = makePage({ connection: { effectiveType: 'slow-2g' } });
        page.sandbox.initProgressiveImages();
        assert.equal(page.preloads.length, 0);
    },
    async 'on 3G both start together'() {
        const page = makePage({ connection: { effectiveType: '3g' }, fullOutcome: 'never' });
        page.sandbox.initProgressiveImages();
        assert.equal(page.preloads.length, 1);
    },
};

(async () => {
    let n = 0;
    for (const [name, fn] of Object.entries(checks)) {
        try {
            await fn();
            n += 1;
        } catch (err) {
            console.error(`FAIL: ${name}\n${err && err.stack || err}`);
            process.exit(1);
        }
    }
    console.log(`all ${n} progressive checks passed`);
})();
