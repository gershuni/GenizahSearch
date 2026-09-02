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

/**
 * Create a manuscript viewer instance with zoom, pan, rotate, and image adjustments.
 *
 * @param {Object} options
 * @param {string} options.imageSelector - CSS selector for the zoomable image
 * @param {string} options.containerSelector - CSS selector for the image container
 * @param {string} options.zoomLabelSelector - CSS selector for the zoom level label
 * @param {string} options.gammaFilterId - ID of the SVG gamma filter element
 * @param {number} [options.zoomStep=0.25] - Zoom increment per step
 * @param {number} [options.maxZoom=4] - Maximum zoom level
 * @returns {Object} Viewer object with full public API
 */
function createManuscriptViewer(options) {
    const imageSelector = options.imageSelector;
    const containerSelector = options.containerSelector;
    const zoomLabelSelector = options.zoomLabelSelector;
    const gammaFilterId = options.gammaFilterId;
    const zoomStep = options.zoomStep || 0.25;
    const maxZoom = options.maxZoom || 4;

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

            // Attach mousedown directly to the IMAGE element. SEED-010: move/up are
            // bound to `document` on drag START and removed on release (see
            // onMouseDown/onMouseUp) instead of overwriting window.onmousemove
            // globally — so multiple viewers on one page (e.g. Compare's two panes)
            // each drag independently instead of the last-initialised one winning.
            this.el.onmousedown = this.onMouseDown.bind(this);
            this.el.ondragstart = function(e) { e.preventDefault(); };

            // Mouse wheel zoom - attach to image
            this.el.onwheel = this.onWheel.bind(this);

            // Set initial cursor on the image
            this.el.style.cursor = 'grab';
        },

        onWheel: function(e) {
            e.preventDefault();
            var delta = e.deltaY > 0 ? -zoomStep : zoomStep;
            this.state.scale = Math.max(0.25, Math.min(maxZoom, this.state.scale + delta));
            this.applyTransform();
            this.updateLabel();
        },

        update: function(scale, rotation) {
            this.state.scale = scale;
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

        zoomIn: function() {
            this.state.scale = Math.min(maxZoom, this.state.scale + zoomStep);
            this.applyTransform();
            this.updateLabel();
        },
        zoomOut: function() {
            this.state.scale = Math.max(0.25, this.state.scale - zoomStep);
            this.applyTransform();
            this.updateLabel();
        },
        rotateLeft: function() {
            this.state.rotation = (this.state.rotation - 90) % 360;
            this.applyTransform();
        },
        rotateRight: function() {
            this.state.rotation = (this.state.rotation + 90) % 360;
            this.applyTransform();
        },

        updateLabel: function() {
            var label = document.querySelector(zoomLabelSelector);
            if (label) label.textContent = Math.round(this.state.scale * 100) + '%';
        },

        reset: function() {
            this.state.x = 0;
            this.state.y = 0;
            this.state.rotation = 0;
            this.state.scale = 1;
            this.resetAdjustments();
            this.applyTransform();
            this.updateLabel();
        }
    };

    return viewer;
}
