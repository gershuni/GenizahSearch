// The image credit after the viewer falls back to NLI, run by node against the real
// web/static/manuscript_viewer.js with a small fake DOM (2026-11-11, Codex review of the
// Quick View image change). Driven by tests/test_manuscript_viewer_js.py; exits non-zero
// on the first failure.
//
// A library image (Cambridge) loads as a 400-px thumbnail first and is upgraded to its
// full size from data-full-src (progressiveLoad). When the thumbnail fails,
// handleImageError shows NLI's image and switches the credit to NLI's. The upgrade must
// not then put the library image back under the NLI credit.
//
// A thumbnail from the browser's cache can finish loading before initProgressiveImages
// runs (Quick View calls it 200 ms after drawing the image): it must still be upgraded.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const SYS = '990051340280205171';
const THUMB = `/api/cambridge_image/${SYS}?page=0&width=400`;
const FULL = `/api/cambridge_image/${SYS}?page=0`;

function makePage(attrs, props) {
    const listeners = {};
    const container = { classList: { add() {} } };
    const img = {
        src: THUMB,
        complete: false,
        naturalWidth: 0,
        dataset: Object.assign({}, attrs),
        style: {},
        parentElement: null,
        onload: null,
        getAttribute(name) { return name === 'data-full-src' ? FULL : null; },
        closest() { return container; },
        addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
        removeEventListener(type, fn) { listeners[type] = (listeners[type] || []).filter((f) => f !== fn); },
        fire(type) {
            if (type === 'load' && typeof img.onload === 'function') img.onload();
            (listeners[type] || []).slice().forEach((fn) => fn());
        },
    };
    Object.assign(img, props || {});
    const credit = { textContent: 'Cambridge University Library', dataset: {
        creditOxford: 'Cambridge University Library', creditNli: 'National Library of Israel' } };
    const document = {
        querySelector(sel) { return sel === '[data-role="image-credit"]' ? credit : null; },
        querySelectorAll(sel) { return sel === 'img[data-full-src]' ? [img] : []; },
        createElement() { return { style: {}, appendChild() {}, setAttribute() {} }; },
    };
    // Any full-size image the viewer preloads loads at once (the library's full image works).
    const preloads = [];
    class Image {
        constructor() { preloads.push(this); }
        set src(v) { this._src = v; setTimeout(() => this.onload && this.onload(), 0); }
        get src() { return this._src; }
    }
    const manifest = { sequences: [{ canvases: [
        { images: [{ resource: { service: { '@id': 'https://iiif.nli.org.il/IIIFv21/FL167042239' } } }] },
    ] }] };
    const sandbox = {
        document, Image, console: { log() {}, warn() {}, error() {} }, setTimeout, clearTimeout,
        fetch: async () => ({ ok: true, status: 200, json: async () => manifest }),
    };
    sandbox.window = sandbox;
    vm.createContext(sandbox);
    const src = fs.readFileSync(path.join(__dirname, '..', '..', 'web', 'static', 'manuscript_viewer.js'), 'utf8');
    vm.runInContext(src, sandbox, { filename: 'manuscript_viewer.js' });
    return { sandbox, img, credit, preloads };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function thumbnailFailsThenNliLoads(attrs) {
    const page = makePage(attrs);
    page.sandbox.initProgressiveImages();
    page.img.fire('error');                                  // the 400-px Cambridge thumbnail fails
    await page.sandbox.handleImageError(page.img, SYS, 0, false, null);
    assert.match(page.img.src, /iiif\.nli\.org\.il\/IIIFv21\/FL167042239\//, 'fell back to NLI');
    page.img.fire('load');                                   // NLI's image loads
    await sleep(20);                                         // any full-size upgrade would land now
    return page;
}

const checks = {
    async 'a Penn image falls back to the same NLI scan even if its page number differs'() {
        const page = makePage({ nliFl: '262908052', creditFallback: 'nli' });
        await page.sandbox.handleImageError(page.img, SYS, 0, false, null);
        assert.equal(page.img.src, '/api/nli_image/262908052');
        page.img.fire('load');
        assert.equal(page.credit.textContent, 'National Library of Israel');
    },
    async 'a library image that fell back to NLI keeps NLI image and NLI credit'() {
        const { img, credit } = await thumbnailFailsThenNliLoads({ creditFallback: 'nli' });
        assert.match(img.src, /iiif\.nli\.org\.il/, `the upgrade put the library image back: ${img.src}`);
        assert.equal(credit.textContent, 'National Library of Israel');
    },
    async 'an image without the mark keeps its credit, as before'() {
        const { credit } = await thumbnailFailsThenNliLoads({});
        assert.equal(credit.textContent, 'Cambridge University Library');
    },
    async 'a thumbnail that loads is still upgraded to the full image'() {
        const page = makePage({ creditFallback: 'nli' });
        page.sandbox.initProgressiveImages();
        page.img.fire('load');
        await sleep(20);
        assert.equal(page.img.src, FULL);
        assert.equal(page.credit.textContent, 'Cambridge University Library');
    },
    async 'a thumbnail that loaded from the cache before init is upgraded too'() {
        const page = makePage({ creditFallback: 'nli' }, { complete: true, naturalWidth: 400 });
        page.sandbox.initProgressiveImages();                // no load event follows
        await sleep(20);
        assert.equal(page.img.src, FULL, 'the cached thumbnail stayed at 400 px');
    },
    async 'a thumbnail that failed before init is not upgraded'() {
        const page = makePage({ creditFallback: 'nli' }, { complete: true, naturalWidth: 0 });
        page.sandbox.initProgressiveImages();
        await sleep(20);
        assert.equal(page.img.src, THUMB);
    },
    async 'an image drawn at full size is not fetched again'() {
        // The browse page's full screen writes the full URL as both src and data-full-src;
        // the browser resolves img.src to an absolute URL (Codex on #64).
        const page = makePage({}, {
            src: `http://localhost:8081${FULL}`,
            getAttribute(name) { return (name === 'data-full-src' || name === 'src') ? FULL : null; },
        });
        page.sandbox.initProgressiveImages();
        page.img.fire('load');
        await sleep(20);
        assert.equal(page.preloads.length, 0, 'the full image was preloaded a second time');
        assert.equal(page.img.src, `http://localhost:8081${FULL}`);
    },
    async 'a thumbnail is upgraded with one preload'() {
        const page = makePage({});
        page.sandbox.initProgressiveImages();
        page.img.fire('load');
        await sleep(20);
        assert.equal(page.preloads.length, 1);
        assert.equal(page.img.src, FULL);
    },
    async 'an NLI fallback that loaded before init is not upgraded'() {
        const page = makePage({ creditFallback: 'nli', triedManifest: 'true' },
            { complete: true, naturalWidth: 2000, src: 'https://iiif.nli.org.il/IIIFv21/FL167042239/full/max/0/default.jpg' });
        page.sandbox.initProgressiveImages();
        await sleep(20);
        assert.match(page.img.src, /iiif\.nli\.org\.il/);
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
    console.log(`all ${n} credit checks passed`);
})();
