/**
 * Shared Manuscript Viewer Module
 *
 * Provides:
 * - fetchFlIdsFromManifest(sysId) -- IIIF manifest FL ID resolution
 * - handleImageError(img, sysId, pageIdx, isOxford, viewerName) -- fallback chain
 * - progressiveLoad(img) -- thumbnail-to-full-resolution upgrade for one image
 * - initProgressiveImages() -- auto-init all [data-full-src] images on the page
 * - createManuscriptViewer(options) -- viewer factory (zoom/pan/rotate/adjustments)
 *
 * Usage:
 *   <script src="/static/manuscript_viewer.js" defer></script>
 *   window.manuscriptViewer = createManuscriptViewer({
 *       imageSelector: '.zoomable-image',
 *       containerSelector: '.image-container',
 *       zoomLabelSelector: '.zoom-level-label',
 *       gammaFilterId: 'gamma-main'
 *   });
 */

// NLI IIIF base URL for direct browser access
const NLI_IIIF_BASE = 'https://iiif.nli.org.il/IIIFv21';

// Cache for FL IDs fetched from IIIF manifests (shared across all viewers)
const _flIdCache = {};

/**
 * Fetch FL IDs from IIIF manifest (client-side, bypasses server blocking).
 * Server-side /api/nli_image_by_sysid resolves FL IDs locally from crossref
 * sidecar (815K pre-resolved records). This client-side manifest fetch is a
 * last-resort fallback for uncovered manuscripts.
 *
 * Phase 85 D-06/D-14: synthetic sys_ids skip the NLI manifest fetch entirely.
 * The window.GENIZAH_IS_SYNTHETIC flag is set by web/pages/browse.py at
 * page render time. Saves cold-cache external requests + prevents
 * polluting NLI access logs with PNX_MANUSCRIPTS99...000000 404s.
 */
async function fetchFlIdsFromManifest(sysId) {
    if (typeof window !== 'undefined' && window.GENIZAH_IS_SYNTHETIC) {
        console.log(`[Manifest] Skipping NLI manifest fetch for synthetic ${sysId}`);
        return [];
    }

    if (_flIdCache[sysId]) {
        console.log(`[Manifest] Cache hit for ${sysId}`);
        return _flIdCache[sysId];
    }

    const manifestUrl = `${NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS${sysId}-1/manifest`;
    console.log(`[Manifest] Fetching ${manifestUrl}`);
    try {
        const resp = await fetch(manifestUrl);
        console.log(`[Manifest] Response status: ${resp.status} for ${sysId}`);
        if (!resp.ok) {
            console.log(`[Manifest] Failed (${resp.status}) for ${sysId}`);
            return [];
        }

        const data = await resp.json();
        const flIds = [];

        if (data.sequences && data.sequences[0] && data.sequences[0].canvases) {
            console.log(`[Manifest] Found ${data.sequences[0].canvases.length} canvases for ${sysId}`);
            for (const canvas of data.sequences[0].canvases) {
                const images = canvas.images || [];
                if (images[0] && images[0].resource && images[0].resource.service) {
                    const serviceId = images[0].resource.service['@id'] || '';
                    const match = serviceId.match(/FL(\d+)/);
                    if (match) {
                        flIds.push(match[1]);
                    }
                }
            }
        } else {
            console.log(`[Manifest] No canvases found in manifest for ${sysId}`);
        }

        if (flIds.length > 0) {
            _flIdCache[sysId] = flIds;
            console.log(`[Manifest] Cached ${flIds.length} FL IDs for ${sysId}`);
        } else {
            console.log(`[Manifest] No FL IDs extracted for ${sysId}`);
        }
        return flIds;
    } catch (e) {
        console.error(`[Manifest] Error fetching for ${sysId}:`, e);
        return [];
    }
}

/**
 * Handle image error with fallback chain.
 * Uses lazy viewer resolution via viewerName string to prevent stale refs.
 *
 * @param {HTMLImageElement} img - The image element that failed
 * @param {string} sysId - System ID of the manuscript
 * @param {number} pageIdx - Page index
 * @param {boolean} isOxford - Whether this is an Oxford manuscript
 * @param {string} viewerName - Name of window property for viewer (e.g., 'manuscriptViewer')
 */

/**
 * 260902: when the fallback chain ends up showing an NLI image on a page whose
 * native source is another library (Oxford), swap the credit footer to the NLI
 * credit + link. Server-side (browse.py) renders both variants as data attributes.
 */
function switchImageCredit(source) {
    try {
        const lbl = document.querySelector('[data-role="image-credit"]');
        const lnk = document.querySelector('[data-role="image-credit-link"]');
        if (!lbl) return;
        const text = source === 'nli' ? lbl.dataset.creditNli : lbl.dataset.creditOxford;
        const href = source === 'nli' ? (lnk && lnk.dataset.linkNli) : (lnk && lnk.dataset.linkOxford);
        if (text) lbl.textContent = text;
        if (lnk && href) lnk.setAttribute('href', href);
    } catch (e) { /* cosmetic */ }
}

async function handleImageError(img, sysId, pageIdx, isOxford, viewerName) {
    const currentSrc = img.src || '';
    const isOxfordApiUrl = currentSrc.includes('/api/oxford_image/');
    console.log(`[handleImageError] src=${currentSrc}, sysId=${sysId}, pageIdx=${pageIdx}, isOxford=${isOxford}, viewer=${viewerName}`);

    // Try 1: If Oxford and the CURRENT src is NOT already the Oxford API, try the server proxy
    if (isOxford && sysId && !isOxfordApiUrl && !img.dataset.triedOxford) {
        img.dataset.triedOxford = 'true';
        const oxfordUrl = `/api/oxford_image/${sysId}?page=${pageIdx || 0}`;
        console.log(`Trying Oxford API: ${oxfordUrl}`);
        img.src = oxfordUrl;
        img.onload = function() {
            console.log('Oxford API image loaded');
            // The reverse fallback: the page was explicitly showing NLI, that image
            // failed, and Oxford answered. The footer would otherwise keep crediting
            // and linking NLI under a Bodleian image (Codex P2, 2026-09-02).
            switchImageCredit('oxford');
            if (viewerName && window[viewerName] && typeof window[viewerName].init === 'function') window[viewerName].init();
        };
        return;
    }

    // If Oxford API already failed, mark it as tried
    if (isOxfordApiUrl) {
        img.dataset.triedOxford = 'true';
    }

    // Try 2: Fetch FL IDs from NLI IIIF manifest (client-side fallback)
    if (sysId && !img.dataset.triedManifest) {
        img.dataset.triedManifest = 'true';
        console.log(`Trying NLI manifest for sysId: ${sysId}, page: ${pageIdx}`);

        const flIds = await fetchFlIdsFromManifest(sysId);
        if (flIds.length > 0) {
            const idx = Math.min(pageIdx || 0, flIds.length - 1);
            const newUrl = `${NLI_IIIF_BASE}/FL${flIds[idx]}/full/2000,/0/default.jpg`;
            console.log(`Trying FL ID from manifest: ${flIds[idx]}`);
            img.src = newUrl;
            img.onload = function() {
                console.log('Manifest-based image loaded, initializing viewer');
                if (isOxford) switchImageCredit('nli');
                if (viewerName && window[viewerName] && typeof window[viewerName].init === 'function') window[viewerName].init();
            };
            return;
        }
    }

    // Try 3: Use server-side NLI proxy (handles collections that block browser requests)
    // Phase 85 D-06/D-14: synthetic sys_ids skip the proxy entirely — the
    // /api/nli_image_by_sysid endpoint returns 204 for synthetic, but the
    // <img> would still issue an unnecessary request. Skip cleanly to the
    // "All fallbacks exhausted" branch.
    const isSynth = (typeof window !== 'undefined' && window.GENIZAH_IS_SYNTHETIC);
    if (sysId && !img.dataset.triedServerProxy && !isSynth) {
        img.dataset.triedServerProxy = 'true';
        const proxyUrl = `/api/nli_image_by_sysid/${sysId}?page=${pageIdx || 0}`;
        console.log(`Trying server-side NLI proxy: ${proxyUrl}`);
        img.src = proxyUrl;
        img.onload = function() {
            console.log('Server proxy image loaded');
            if (isOxford) switchImageCredit('nli');
            if (viewerName && window[viewerName] && typeof window[viewerName].init === 'function') window[viewerName].init();
        };
        return;
    }

    // All fallbacks exhausted
    console.log('All image sources failed for:', currentSrc);
    img.style.display = 'none';
    const parent = img.parentElement;
    if (parent) {
        // 260902: when the manuscript is at the Bodleian, offer the reader a direct
        // link to the folio image on the Genizah Fragments site. Their own browser
        // can pass the site's JS bot-challenge; an <img> cannot. Attributes are
        // set server-side (browse.py) and HTML-escaped there; textContent/setAttribute
        // keep them inert here.
        const direct = img.dataset.oxfordDirect || '';
        const label = img.dataset.oxfordDirectLabel || 'Open in Bodleian Libraries';
        const box = document.createElement('div');
        box.style.cssText = 'text-align: center; color: #888;';
        box.innerHTML = '<i class="material-icons" style="font-size: 4rem;">image_not_supported</i><p>Image not available</p>';
        if (direct && /^https:\/\/hebrew\.bodleian\.ox\.ac\.uk\//.test(direct)) {
            const p = document.createElement('p');
            const a = document.createElement('a');
            a.setAttribute('href', direct);
            a.setAttribute('target', '_blank');
            a.setAttribute('rel', 'noopener noreferrer');
            a.style.cssText = 'color: #9ecbff;';
            a.textContent = label;
            p.appendChild(a);
            box.appendChild(p);
        }
        parent.innerHTML = '';
        parent.appendChild(box);
    }
}

/**
 * Progressive image loading: show spinner → thumbnail (400px) → full (2000px).
 * Marks the surrounding .img-loading-container as loaded on first paint (or
 * error), then upgrades the <img> to its data-full-src resolution once the
 * full image has preloaded.
 *
 * @param {HTMLImageElement} img - Image element with an optional data-full-src
 */
function progressiveLoad(img) {
    var container = img.closest('.img-loading-container');
    var fullSrc = img.getAttribute('data-full-src');
    // Mark loaded on first successful paint
    img.addEventListener('load', function onThumbLoad() {
        if (container) container.classList.add('img-loaded');
        img.removeEventListener('load', onThumbLoad);
        // Upgrade to full resolution if available
        if (fullSrc && img.src !== fullSrc) {
            var full = new Image();
            full.onload = function() {
                img.src = fullSrc;
            };
            full.src = fullSrc;
        }
    });
    // Also mark loaded on error (hide spinner)
    img.addEventListener('error', function onErr() {
        if (container) container.classList.add('img-loaded');
        img.removeEventListener('error', onErr);
    });
}

/**
 * Auto-init all progressive images on the page (idempotent per image).
 */
function initProgressiveImages() {
    document.querySelectorAll('img[data-full-src]').forEach(function(img) {
        if (!img.dataset.progressiveInit) {
            img.dataset.progressiveInit = 'true';
            progressiveLoad(img);
        }
    });
}

// Zoom (2026-11-03). PostHog's 30 days to 2026-11-03 held 50 rage clicks by 19
// readers on /browse's + button: each click added a linear 0.25 and went
// through a server round-trip. Every step is now multiplicative -- ZOOM_FACTOR
// per click, so five clicks take the page from 100% to 759% where the old steps
// needed 27 -- anchored at the cursor (wheel, double-click, double-tap) or at
// the middle of the box (buttons, keyboard), and applied here at once. A viewer
// created with `onChange` tells its owner ZOOM_NOTIFY_MS after the last change;
// the browse page sends the level to the server with it, so a page it rebuilds
// keeps the zoom. web/pages/browse_state.py carries the same numbers for the
// server side; tests/test_browse_zoom_steps.py keeps the two in step.
const ZOOM_FACTOR = 1.5;
const MIN_ZOOM = 0.25;
const MAX_ZOOM = 8;
const DOUBLE_CLICK_ZOOM = 2;
// One mouse-wheel notch (100 px) is x1.22; a trackpad's small deltas add up,
// and no single event moves more than one click's worth.
const WHEEL_ZOOM_SENSITIVITY = 0.002;
const DOUBLE_TAP_MS = 300;
const DOUBLE_TAP_PX = 30;
const ZOOM_NOTIFY_MS = 150;
const ZOOM_BADGE_MS = 900;

/**
 * Create a manuscript viewer instance with zoom, pan, rotate, and image adjustments.
 *
 * @param {Object} options
 * @param {string} options.imageSelector - CSS selector for the zoomable image
 * @param {string} options.containerSelector - CSS selector for the image container
 * @param {string} options.zoomLabelSelector - CSS selector for the zoom level label
 * @param {string} options.gammaFilterId - ID of the SVG gamma filter element
 * @param {number} [options.minZoom=0.25] - Smallest zoom level
 * @param {number} [options.maxZoom=8] - Largest zoom level
 * @param {function} [options.onChange] - Called with the viewer ZOOM_NOTIFY_MS
 *     after the last zoom change made here (buttons, wheel, double-click,
 *     pinch, fit, reset); never for a level pushed in with update()/setTransform().
 * @returns {Object} Viewer object with full public API
 */
function createManuscriptViewer(options) {
    const imageSelector = options.imageSelector;
    const containerSelector = options.containerSelector;
    const zoomLabelSelector = options.zoomLabelSelector;
    const gammaFilterId = options.gammaFilterId;
    const minZoom = options.minZoom || MIN_ZOOM;
    const maxZoom = options.maxZoom || MAX_ZOOM;
    const onChange = typeof options.onChange === 'function' ? options.onChange : null;

    const viewer = {
        el: null,
        container: null,
        state: {
            scale: 1,
            rotation: 0,
            x: 0,
            y: 0,
            isDragging: false,
            startX: 0,
            startY: 0,
            brightness: 0,
            contrast: 0,
            gamma: 1.0,
            invert: false
        },

        init: function() {
            this.el = document.querySelector(imageSelector);
            this.container = document.querySelector(containerSelector);

            if (!this.el) {
                console.log('viewer: image not found (' + imageSelector + ')');
                return;
            }
            if (!this.container) {
                console.log('viewer: container not found (' + containerSelector + ')');
                return;
            }

            console.log('viewer: initializing drag on ' + imageSelector);

            // A new element (the page was rebuilt, the folio changed) is drawn
            // with whatever transform the server put on it; the state takes
            // that over, so the next step starts from what is on screen.
            if (this._boundEl !== this.el) {
                this._boundEl = this.el;
                this._adoptInlineTransform();
            }

            // Attach mousedown directly to the IMAGE element. SEED-010: move/up are
            // bound to `document` on drag START and removed on release (see
            // onMouseDown/onMouseUp) instead of overwriting window.onmousemove
            // globally — so multiple viewers on one page (e.g. Compare's two panes)
            // each drag independently instead of the last-initialised one winning.
            this.el.onmousedown = this.onMouseDown.bind(this);
            this.el.ondragstart = function(e) { e.preventDefault(); };

            // Mouse wheel zoom - attach to image
            this.el.onwheel = this.onWheel.bind(this);
            // Double-click: in to DOUBLE_CLICK_ZOOM at the pointer, or back to fit.
            this.el.ondblclick = this.onDoubleClick.bind(this);
            // The browser's own double-tap zoom would fight the one below;
            // `manipulation` leaves panning and pinching to the handlers.
            this.el.style.touchAction = 'manipulation';

            // Touch (2026-10-22): two fingers pinch-zoom; one finger pans once
            // the image is zoomed in. A single finger on an unzoomed image is
            // left to the page, so a phone can still scroll past the picture.
            // init() runs again whenever the image reloads or is replaced (the
            // inline onload, the scheduled init, the progressive-load swap), so
            // the handlers are bound ONCE and taken off the previous element
            // first: adding the same function to the same element is a no-op,
            // where a fresh bind() per call stacked a handler set per init
            // (Codex on #434). The mouse handlers are assigned, not added, so
            // they never stacked.
            if (!this._touch) {
                this._touch = {
                    start: this.onTouchStart.bind(this),
                    move: this.onTouchMove.bind(this),
                    end: this.onTouchEnd.bind(this)
                };
            }
            if (this._touchEl && this._touchEl !== this.el) {
                this._touchEl.removeEventListener('touchstart', this._touch.start);
                this._touchEl.removeEventListener('touchmove', this._touch.move);
                this._touchEl.removeEventListener('touchend', this._touch.end);
                this._touchEl.removeEventListener('touchcancel', this._touch.end);
            }
            this._touchEl = this.el;
            this.el.addEventListener('touchstart', this._touch.start, { passive: false });
            this.el.addEventListener('touchmove', this._touch.move, { passive: false });
            // touchend must be able to preventDefault too: a double-tap's
            // second touch would otherwise also arrive as a click pair.
            this.el.addEventListener('touchend', this._touch.end, { passive: false });
            this.el.addEventListener('touchcancel', this._touch.end);

            // Set initial cursor on the image
            this.el.style.cursor = 'grab';
        },

        clampZoom: function(scale) {
            return Math.max(minZoom, Math.min(maxZoom, scale));
        },

        // The translate / rotate / scale of the element's inline style (the
        // server renders `translate(0px, 0px) rotate(Rdeg) scale(S)`); an
        // element with none is drawn untransformed.
        _adoptInlineTransform: function() {
            var t = (this.el && this.el.style && this.el.style.transform) || '';
            var m = /translate\(\s*(-?[\d.]+)px\s*,\s*(-?[\d.]+)px\s*\)/.exec(t);
            this.state.x = m ? parseFloat(m[1]) : 0;
            this.state.y = m ? parseFloat(m[2]) : 0;
            m = /rotate\(\s*(-?[\d.]+)deg\s*\)/.exec(t);
            this.state.rotation = m ? parseFloat(m[1]) : 0;
            m = /scale\(\s*(-?[\d.]+)\s*\)/.exec(t);
            this.state.scale = m ? this.clampZoom(parseFloat(m[1])) : 1;
            this._setLabelText(Math.round(this.state.scale * 100) + '%');
        },

        _setLabelText: function(text) {
            var label = zoomLabelSelector ? document.querySelector(zoomLabelSelector) : null;
            if (label) label.textContent = text;
            return label;
        },

        // Where the image's centre sits with no translate, in client pixels.
        // Measured from the drawn box and the DRAWN translate (the computed
        // matrix), not the state's: the 0.1 s transform transition may still
        // be running, and the box moves with it. transform-origin is the
        // centre on every surface, so the drawn box is centred on
        // centre + drawn translate whatever the rotation and scale.
        _layoutCentre: function() {
            var rect = this.el.getBoundingClientRect();
            var drawn = { x: this.state.x, y: this.state.y };
            try {
                var t = getComputedStyle(this.el).transform;
                if (!t || t === 'none') {
                    drawn = { x: 0, y: 0 };
                } else {
                    var m = new DOMMatrix(t);
                    drawn = { x: m.e, y: m.f };
                }
            } catch (err) { /* no DOMMatrix: the state's translate will do */ }
            return { x: rect.left + rect.width / 2 - drawn.x, y: rect.top + rect.height / 2 - drawn.y };
        },

        // Zoom by `factor` about the client point (px, py), or, without one,
        // about the middle of the box (the buttons and the keyboard), so what
        // the reader is looking at stays put. The point under the anchor stays
        // under it: with k = new / old,
        //     translate' = translate * k + (anchor - centre) * (1 - k).
        zoomBy: function(factor, px, py) {
            if (!this.el) {
                this.el = document.querySelector(imageSelector);
                if (!this.el) return;
            }
            var scale = this.state.scale;
            var next = this.clampZoom(scale * factor);
            if (next === scale) return;
            var k = next / scale;
            var anchor;
            if (typeof px === 'number' && typeof py === 'number') {
                anchor = { x: px, y: py };
            } else {
                var box = (this.container || this.el).getBoundingClientRect();
                anchor = { x: box.left + box.width / 2, y: box.top + box.height / 2 };
            }
            var centre = this._layoutCentre();
            this.state.x = this.state.x * k + (anchor.x - centre.x) * (1 - k);
            this.state.y = this.state.y * k + (anchor.y - centre.y) * (1 - k);
            this.state.scale = next;
            this.applyTransform();
            this.updateLabel();
            this._changed();
        },

        // Back to the whole page in its box: scale 1 and no pan; the rotation
        // and the image adjustments stay.
        fit: function() {
            this.state.x = 0;
            this.state.y = 0;
            this.state.scale = 1;
            this.applyTransform();
            this.updateLabel();
            this._changed();
        },

        // Double-click and double-tap: in to DOUBLE_CLICK_ZOOM at the point,
        // or, from any zoomed-in level, back to fit.
        toggleZoomAt: function(px, py) {
            if (this.state.scale > 1.01) {
                this.fit();
            } else {
                this.zoomBy(DOUBLE_CLICK_ZOOM / this.state.scale, px, py);
            }
        },

        onDoubleClick: function(e) {
            e.preventDefault();
            // A double-tap that toggled just now also reaches some browsers
            // as a dblclick; one toggle is enough.
            if (this._tapToggledAt && Date.now() - this._tapToggledAt < 500) return;
            this.toggleZoomAt(e.clientX, e.clientY);
        },

        onWheel: function(e) {
            e.preventDefault();
            var dy = e.deltaY;
            if (e.deltaMode === 1) dy *= 16;        // lines
            else if (e.deltaMode === 2) dy *= 100;  // pages
            var factor = Math.exp(-dy * WHEEL_ZOOM_SENSITIVITY);
            factor = Math.max(1 / ZOOM_FACTOR, Math.min(ZOOM_FACTOR, factor));
            this.zoomBy(factor, e.clientX, e.clientY);
        },

        // Tell the owner, once, ZOOM_NOTIFY_MS after the last change. The
        // callback reads the live state when it fires, so a burst of clicks
        // reports the level the reader ended on.
        _changed: function() {
            if (!onChange) return;
            var self = this;
            if (this._notifyTimer) clearTimeout(this._notifyTimer);
            this._notifyTimer = setTimeout(function() {
                self._notifyTimer = null;
                onChange(self);
            }, ZOOM_NOTIFY_MS);
        },

        update: function(scale, rotation) {
            this.state.scale = scale;
            this.state.rotation = rotation;
            this.applyTransform();
        },

        // The server's rotation; the zoom stays what the reader set here.
        setRotation: function(rotation) {
            this.state.rotation = rotation;
            this.applyTransform();
        },

        setTransform: function(x, y, scale, rotation) {
            this.state.x = x;
            this.state.y = y;
            this.state.scale = scale;
            this.state.rotation = rotation;
            this.applyTransform();
        },

        onMouseDown: function(e) {
            if (e.button !== 0) return; // Only left click
            e.preventDefault();
            e.stopPropagation();
            this.state.isDragging = true;
            this.state.startX = e.clientX - this.state.x;
            this.state.startY = e.clientY - this.state.y;
            this.el.style.cursor = 'grabbing';
            // SEED-010: bind move/up for THIS drag only (removed on mouseup) so
            // concurrent viewers don't clobber each other's window handlers.
            if (!this._boundMove) this._boundMove = this.onMouseMove.bind(this);
            if (!this._boundUp) this._boundUp = this.onMouseUp.bind(this);
            document.addEventListener('mousemove', this._boundMove);
            document.addEventListener('mouseup', this._boundUp);
        },

        onMouseMove: function(e) {
            if (!this.state.isDragging) return;
            e.preventDefault();

            this.state.x = e.clientX - this.state.startX;
            this.state.y = e.clientY - this.state.startY;

            var self = this;
            requestAnimationFrame(function() { self.applyTransform(); });
        },

        onMouseUp: function() {
            this.state.isDragging = false;
            if (this.el) this.el.style.cursor = 'grab';
            // SEED-010: detach this drag's document listeners (paired with onMouseDown).
            if (this._boundMove) document.removeEventListener('mousemove', this._boundMove);
            if (this._boundUp) document.removeEventListener('mouseup', this._boundUp);
        },

        _touchDistance: function(touches) {
            var dx = touches[0].clientX - touches[1].clientX;
            var dy = touches[0].clientY - touches[1].clientY;
            return Math.sqrt(dx * dx + dy * dy);
        },

        onTouchStart: function(e) {
            if (e.touches.length === 2) {
                e.preventDefault();
                this._pinch = { distance: this._touchDistance(e.touches), scale: this.state.scale };
                this.state.isDragging = false;
                this._tap = null;
            } else if (e.touches.length === 1) {
                var touch = e.touches[0];
                // A tap that ends where it began, quickly, may be half of a
                // double-tap (onTouchEnd).
                this._tap = { x: touch.clientX, y: touch.clientY, time: Date.now(), moved: false };
                if (this.state.scale > 1.01) {
                    e.preventDefault();
                    this.state.isDragging = true;
                    this.state.startX = touch.clientX - this.state.x;
                    this.state.startY = touch.clientY - this.state.y;
                }
            }
        },

        onTouchMove: function(e) {
            var self = this;
            if (this._tap && e.touches.length === 1) {
                var t = e.touches[0];
                if (Math.abs(t.clientX - this._tap.x) > 10 || Math.abs(t.clientY - this._tap.y) > 10) {
                    this._tap.moved = true;
                }
            }
            if (this._pinch && e.touches.length === 2) {
                e.preventDefault();
                var distance = this._touchDistance(e.touches);
                if (this._pinch.distance > 0) {
                    this.state.scale = this.clampZoom(this._pinch.scale * distance / this._pinch.distance);
                    requestAnimationFrame(function() {
                        self.applyTransform();
                        self.updateLabel();
                    });
                    this._changed();
                }
            } else if (this.state.isDragging && e.touches.length === 1) {
                e.preventDefault();
                this.state.x = e.touches[0].clientX - this.state.startX;
                this.state.y = e.touches[0].clientY - this.state.startY;
                requestAnimationFrame(function() { self.applyTransform(); });
            }
        },

        onTouchEnd: function(e) {
            if (e.touches.length < 2 && this._pinch) {
                this._pinch = null;
                // A pinch that drops to one finger goes on as a pan of the
                // zoomed image from where that finger is (Codex on #434):
                // without this hand-off the remaining finger moved nothing
                // until it was lifted and put down again.
                if (e.touches.length === 1 && this.state.scale > 1.01) {
                    this.state.isDragging = true;
                    this.state.startX = e.touches[0].clientX - this.state.x;
                    this.state.startY = e.touches[0].clientY - this.state.y;
                }
            }
            if (e.touches.length === 0) {
                this.state.isDragging = false;
                var tap = this._tap;
                this._tap = null;
                if (tap && !tap.moved && Date.now() - tap.time < DOUBLE_TAP_MS) {
                    var last = this._lastTap;
                    this._lastTap = { x: tap.x, y: tap.y, time: Date.now() };
                    if (last && this._lastTap.time - last.time < DOUBLE_TAP_MS &&
                            Math.abs(last.x - tap.x) < DOUBLE_TAP_PX && Math.abs(last.y - tap.y) < DOUBLE_TAP_PX) {
                        // Ours: no click pair, no browser zoom, and one toggle only.
                        e.preventDefault();
                        this._lastTap = null;
                        this._tapToggledAt = Date.now();
                        this.toggleZoomAt(tap.x, tap.y);
                    }
                }
            }
        },

        applyTransform: function() {
            if (!this.el) {
                this.el = document.querySelector(imageSelector);
                if (!this.el) return;
            }
            this.el.style.transform = 'translate(' + this.state.x + 'px, ' + this.state.y + 'px) rotate(' + this.state.rotation + 'deg) scale(' + this.state.scale + ')';
            this._applyFilters();
        },

        _applyFilters: function() {
            if (!this.el) return;
            var s = this.state;
            var b = 1 + s.brightness / 100;
            var c = 1 + s.contrast / 100;
            var inv = s.invert ? 1 : 0;
            var f = 'brightness(' + b + ') contrast(' + c + ') invert(' + inv + ')';
            if (s.gamma !== 1.0) {
                var svgFilter = document.getElementById(gammaFilterId);
                if (svgFilter) {
                    var exp = 1.0 / s.gamma;
                    svgFilter.querySelectorAll('feFuncR, feFuncG, feFuncB').forEach(function(fn) {
                        fn.setAttribute('exponent', exp);
                    });
                }
                f += ' url(#' + gammaFilterId + ')';
            }
            this.el.style.filter = f;
        },

        setBrightness: function(val) { this.state.brightness = val; this._applyFilters(); },
        setContrast: function(val) { this.state.contrast = val; this._applyFilters(); },
        setGamma: function(val) { this.state.gamma = val; this._applyFilters(); },
        toggleInvert: function() { this.state.invert = !this.state.invert; this._applyFilters(); },
        resetAdjustments: function() {
            this.state.brightness = 0;
            this.state.contrast = 0;
            this.state.gamma = 1.0;
            this.state.invert = false;
            this._applyFilters();
        },

        zoomIn: function() { this.zoomBy(ZOOM_FACTOR); },
        zoomOut: function() { this.zoomBy(1 / ZOOM_FACTOR); },
        rotateLeft: function() {
            this.state.rotation = (this.state.rotation - 90) % 360;
            this.applyTransform();
        },
        rotateRight: function() {
            this.state.rotation = (this.state.rotation + 90) % 360;
            this.applyTransform();
        },

        // The toolbar figure; where that is hidden (a phone) or absent, a badge
        // over the image shows the level for a moment instead.
        updateLabel: function() {
            var text = Math.round(this.state.scale * 100) + '%';
            var label = this._setLabelText(text);
            var shown = false;
            try { shown = !!label && getComputedStyle(label).display !== 'none'; } catch (err) { shown = !!label; }
            if (!shown) this._showBadge(text);
        },

        _showBadge: function(text) {
            var host = this.container || (this.el && this.el.parentElement);
            if (!host) return;
            var badge = null;
            for (var i = 0; i < host.children.length; i++) {
                if (host.children[i].className === 'zoom-badge') { badge = host.children[i]; break; }
            }
            if (!badge) {
                badge = document.createElement('div');
                badge.className = 'zoom-badge';
                badge.setAttribute('aria-hidden', 'true');
                badge.style.cssText = 'position:absolute;bottom:8px;left:50%;transform:translateX(-50%);' +
                    'background:rgba(0,0,0,0.65);color:#fff;font-size:12px;line-height:1;padding:4px 10px;' +
                    'border-radius:10px;pointer-events:none;opacity:0;transition:opacity 0.2s;z-index:2;';
                host.appendChild(badge);
            }
            badge.textContent = text;
            badge.style.opacity = '1';
            if (this._badgeTimer) clearTimeout(this._badgeTimer);
            var self = this;
            this._badgeTimer = setTimeout(function() {
                self._badgeTimer = null;
                badge.style.opacity = '0';
            }, ZOOM_BADGE_MS);
        },

        reset: function() {
            this.state.x = 0;
            this.state.y = 0;
            this.state.rotation = 0;
            this.state.scale = 1;
            this.resetAdjustments();
            this.applyTransform();
            this.updateLabel();
            this._changed();
        }
    };

    return viewer;
}
