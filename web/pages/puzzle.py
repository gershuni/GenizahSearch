# -*- coding: utf-8 -*-
"""
Fragment Puzzle page for GenizahSearch web application.

Provides a Fabric.js canvas for assembling and manipulating fragment images.
Users can add fragments by shelfmark, drag/rotate/flip/resize them, and
navigate folios. Session state persists across page navigations via
app.storage.tab.
"""

import asyncio
import logging
import json

from nicegui import ui, app, run
from web.translations import tr
from web.state import state

logger = logging.getLogger(__name__)

# Fabric.js CDN (v6.4.3 stable)
FABRIC_JS_CDN = '<script src="https://cdn.jsdelivr.net/npm/fabric@6.4.3/dist/index.min.js"></script>'

# ── JavaScript: window.puzzleCanvas global object ─────────────────

PUZZLE_CANVAS_JS = '''
<script>
if (window.puzzleCanvas && typeof window.puzzleCanvas.destroy === 'function') {
    try { window.puzzleCanvas.destroy(); } catch (e) { console.warn('Puzzle canvas cleanup failed:', e); }
}

window.puzzleCanvas = {
    canvas: null,
    fragments: {},
    bgModeIndex: 0,
    BG_MODES: ['#333333', '#000000', '#FFFFFF', 'checker', '#F5F0E0', 'grid'],
    contextMenu: null,
    _panning: false,
    _lastPosX: 0,
    _lastPosY: 0,
    _pendingAdds: [],
    _listenersAttached: false,
    _resizeHandler: null,
    _keyboardHandler: null,
    _hideMenuClickHandler: null,
    _hideMenuEscapeHandler: null,
    _contextMenuSuppressHandler: null,
    _contextMenuTarget: null,

    /**
     * Tear down the puzzle canvas, removing all event listeners, fragments,
     * folio data, and disposing the Fabric.js canvas instance.
     * Called on page navigation or before re-initialization.
     */
    destroy: function() {
        this._hideContextMenu();
        if (this._resizeHandler) {
            window.removeEventListener('resize', this._resizeHandler);
            this._resizeHandler = null;
        }
        if (this._keyboardHandler) {
            document.removeEventListener('keydown', this._keyboardHandler);
            this._keyboardHandler = null;
        }
        if (this._hideMenuClickHandler) {
            document.removeEventListener('click', this._hideMenuClickHandler);
            this._hideMenuClickHandler = null;
        }
        if (this._hideMenuEscapeHandler) {
            document.removeEventListener('keydown', this._hideMenuEscapeHandler);
            this._hideMenuEscapeHandler = null;
        }
        if (this._contextMenuTarget && this._contextMenuSuppressHandler) {
            this._contextMenuTarget.removeEventListener('contextmenu', this._contextMenuSuppressHandler);
        }
        this._contextMenuTarget = null;
        this._contextMenuSuppressHandler = null;
        this._listenersAttached = false;
        this._pendingAdds = [];
        this.fragments = {};
        this.folioData = {};
        if (this.canvas) {
            try { this.canvas.dispose(); } catch (e) {}
            this.canvas = null;
        }
    },

    /**
     * Dispatch a CustomEvent on the canvas DOM element for Python-side handling.
     * @param {string} name - Event name (e.g. 'puzzle-add-result', 'puzzle-selection').
     * @param {Object} detail - Event payload, JSON-stringified into event.detail.
     */
    _emitEvent: function(name, detail) {
        var el = document.getElementById('puzzleCanvas');
        if (!el) return;
        el.dispatchEvent(new CustomEvent(name, {
            detail: JSON.stringify(detail || {}),
            bubbles: true
        }));
    },

    /**
     * Construct a Fabric.js Image object from an HTML Image element.
     * Disables middle edge handles (corners-only for proportional resize)
     * and calls setCoords() for accurate hit detection.
     * @param {HTMLImageElement} htmlImg - Loaded HTML image element.
     * @param {Object} [options] - Fabric.js properties to set (left, top, angle, etc.).
     * @returns {fabric.Image} The constructed Fabric image object.
     */
    _buildFragmentImage: function(htmlImg, options) {
        var ImageCtor = fabric.Image || fabric.FabricImage;
        if (!ImageCtor) {
            throw new Error('Fabric image constructor is unavailable');
        }
        var img = new ImageCtor(htmlImg);
        if (options) {
            img.set(options);
        }
        // Disable middle edge handles — corners only for proportional resize
        img.setControlsVisibility({
            mt: false, mb: false, ml: false, mr: false,
            tl: true, tr: true, bl: true, br: true, mtr: true
        });
        if (typeof img.setCoords === 'function') {
            img.setCoords();
        }
        return img;
    },

    _removePlaceholder: function(key) {
        if (!this.canvas) return;
        this.canvas.getObjects().forEach(function(obj) {
            if (obj._isPlaceholder && obj._placeholderKey === key) {
                this.canvas.remove(obj);
            }
        }, this);
    },

    _removeFragmentsByKey: function(key) {
        if (!this.canvas) return;
        this.canvas.getObjects().slice().forEach(function(obj) {
            if (obj && obj._fragmentKey === key) {
                this.canvas.remove(obj);
            }
        }, this);
        delete this.fragments[key];
    },

    /**
     * Initialize the Fabric.js canvas on the given DOM element.
     * Sets up wheel zoom, pan, keyboard shortcuts, context menu, selection
     * sync, window resize handling, and processes any fragments queued
     * before initialization.
     * @param {string} canvasId - DOM id of the <canvas> element.
     */
    init: function(canvasId) {
        var el = document.getElementById(canvasId);
        if (!el) { console.error('Canvas element not found:', canvasId); return; }

        // Dispose previous canvas if re-visiting
        if (this.canvas) {
            try { this.canvas.dispose(); } catch(e) {}
            this.canvas = null;
            this.fragments = {};
        }
        if (this._resizeHandler) {
            window.removeEventListener('resize', this._resizeHandler);
            this._resizeHandler = null;
        }

        var container = el.parentElement;
        var w = container.clientWidth || 1200;
        var h = Math.max(container.clientHeight, 400) || 600;

        this.canvas = new fabric.Canvas(canvasId, {
            backgroundColor: '#333333',
            selection: true,
            preserveObjectStacking: true,
            stopContextMenu: true,
            fireRightClick: true,
            uniformScaling: true,
            width: w,
            height: h
        });

        // Enable Ctrl+click multi-select (Shift+click is default)
        this.canvas.altSelectionKey = 'ctrlKey';

        this.setupWheelZoom();
        this.setupPan();
        if (!this._listenersAttached) {
            this.setupKeyboard();
            this.setupContextMenu();
            this._listenersAttached = true;
        }
        this.setupSelectionSync();

        // Event-driven auto-save: fires on drag-end, rotate-end, scale-end
        var self = this;
        this.canvas.on('object:modified', function(e) {
            self._emitEvent('puzzle-object-modified', {
                key: e.target ? e.target._fragmentKey : null
            });
        });

        // Handle window resize (with cleanup reference)
        var self = this;
        this._resizeHandler = function() {
            if (!self.canvas) return;
            var c = document.getElementById(canvasId);
            if (!c) return;
            var p = c.parentElement;
            self.canvas.setWidth(p.clientWidth);
            self.canvas.setHeight(Math.max(p.clientHeight, 400));
            self.canvas.requestRenderAll();
        };
        window.addEventListener('resize', this._resizeHandler);

        console.log('Puzzle canvas initialized:', w, 'x', h);

        // Process any fragments queued before init
        if (this._pendingAdds.length > 0) {
            console.log('Processing', this._pendingAdds.length, 'queued fragments');
            var pending = this._pendingAdds.slice();
            this._pendingAdds = [];
            for (var i = 0; i < pending.length; i++) {
                var p = pending[i];
                this.addFragment(p.key, p.url, p.x, p.y, p.rotation, p.scale, p.flipH, p.flipV, p.meta, p.posOrigin, p.centerOrigin);
            }
        }
    },

    /**
     * Load a fragment image and add it to the canvas.
     * Shows a "Loading..." placeholder while the image fetches. On success,
     * auto-fits large images to ~60% of canvas, selects the new fragment,
     * auto-loads its folio list, and emits 'puzzle-add-result'.
     * If the canvas is not yet initialized, the request is queued.
     * @param {string} key - Unique fragment key, typically "sys_id,folio_label".
     * @param {string} imageUrl - URL to fetch the fragment image from.
     * @param {number} x - Initial left position on canvas.
     * @param {number} y - Initial top position on canvas.
     * @param {number} rotation - Initial rotation angle in degrees.
     * @param {number} scale - Initial uniform scale factor (1.0 = original).
     * @param {boolean} flipH - Whether to flip horizontally.
     * @param {boolean} flipV - Whether to flip vertically.
     * @param {Object} [meta] - Fragment metadata (fl_id, threshold, sys_id, etc.).
     */
    addFragment: function(key, imageUrl, x, y, rotation, scale, flipH, flipV, meta, posOrigin, centerOrigin) {
        // posOrigin: if true, x/y come from the shared desktop model and will be
        // placed via the fragment's visual center after the image is created.
        // centerOrigin: if true, x/y are already the desired visual center.
        if (!this.canvas) {
            // Queue for when canvas is ready
            console.warn('Canvas not ready, queuing fragment:', key);
            this._pendingAdds.push({key:key, url:imageUrl, x:x, y:y, rotation:rotation, scale:scale, flipH:flipH, flipV:flipV, meta:meta, posOrigin:posOrigin, centerOrigin:centerOrigin});
            return;
        }
        var self = this;

        // Remove existing fragment with same key
        if (this.fragments[key]) {
            this._removeFragmentsByKey(key);
        }

        // Show loading placeholder
        var placeholder = new fabric.Text('Loading...', {
            left: x, top: y,
            fontSize: 14, fill: '#888',
            selectable: false, evented: false,
            _isPlaceholder: true, _placeholderKey: key
        });
        this.canvas.add(placeholder);
        this.canvas.requestRenderAll();

        console.log('Loading fragment image:', key, imageUrl);
        var htmlImg = new Image();
        htmlImg.crossOrigin = 'anonymous';
        htmlImg.decoding = 'async';
        htmlImg.onload = function() {
            console.log('Image loaded:', key, htmlImg.naturalWidth, 'x', htmlImg.naturalHeight);
            try {
                self._removePlaceholder(key);
                // Auto-fit: if using default scale (1.0), shrink large images to ~60% of canvas
                var effectiveScale = scale || 1.0;
                if (effectiveScale === 1.0 && self.canvas && !posOrigin && !centerOrigin) {
                    var cw = self.canvas.getWidth();
                    var ch = self.canvas.getHeight();
                    var maxW = cw * 0.6;
                    var maxH = ch * 0.6;
                    if (htmlImg.naturalWidth > maxW || htmlImg.naturalHeight > maxH) {
                        effectiveScale = Math.min(maxW / htmlImg.naturalWidth, maxH / htmlImg.naturalHeight);
                    }
                }
                var img = self._buildFragmentImage(htmlImg, {
                    left: x, top: y,
                    angle: rotation || 0,
                    scaleX: effectiveScale,
                    scaleY: effectiveScale,
                    flipX: !!flipH,
                    flipY: !!flipV,
                    hasControls: true,
                    hasBorders: true,
                    cornerSize: 12,
                    transparentCorners: false,
                    lockUniScaling: true,
                    perPixelTargetFind: true,
                    _fragmentKey: key,
                    _imageUrl: imageUrl,
                    _fragmentMeta: meta ? Object.assign({}, meta) : null
                });
                if (posOrigin && typeof img.setPositionByOrigin === 'function') {
                    // Persisted puzzle docs store x/y in the desktop model:
                    // the translation component whose visual center is x + width/2, y + height/2.
                    // Using center placement here is more reliable than reverse-engineering Fabric's
                    // left/top semantics under scaling/cropping.
                    var centerX = x + (img.width || htmlImg.naturalWidth || 0) / 2;
                    var centerY = y + (img.height || htmlImg.naturalHeight || 0) / 2;
                    var pt = (typeof fabric.Point === 'function')
                        ? new fabric.Point(centerX, centerY)
                        : { x: centerX, y: centerY };
                    img.setPositionByOrigin(pt, 'center', 'center');
                    img.setCoords();
                } else if (centerOrigin && typeof img.setPositionByOrigin === 'function') {
                    var centerPt = (typeof fabric.Point === 'function')
                        ? new fabric.Point(x, y)
                        : { x: x, y: y };
                    img.setPositionByOrigin(centerPt, 'center', 'center');
                    img.setCoords();
                }

                self.canvas.add(img);
                self.fragments[key] = img;
                self.canvas.setActiveObject(img);
                self.canvas.requestRenderAll();
                self._syncSelection();

                // Auto-load folios if meta has sys_id
                if (meta && meta.sys_id) {
                    self.loadFolios(key, meta.sys_id);
                }

                self._emitEvent('puzzle-add-result', {
                    key: key,
                    success: true,
                    meta: img._fragmentMeta || null
                });
            } catch (err) {
                console.error('Failed to construct fabric image for', key, err);
                self._removePlaceholder(key);
                var constructErr = new fabric.Text('Render failed: ' + key, {
                    left: x, top: y,
                    fontSize: 12, fill: '#ff6666',
                    selectable: false, evented: false
                });
                self.canvas.add(constructErr);
                self.canvas.requestRenderAll();
                self._emitEvent('puzzle-add-result', {
                    key: key,
                    success: false,
                    error: String(err)
                });
            }
        };
        htmlImg.onerror = function(err) {
            console.error('Failed to load image for', key, imageUrl, err);
            self._removePlaceholder(key);
            var errText = new fabric.Text('Image load failed: ' + key, {
                left: x, top: y,
                fontSize: 12, fill: '#ff6666',
                selectable: false, evented: false
            });
            self.canvas.add(errText);
            self.canvas.requestRenderAll();
            self._emitEvent('puzzle-add-result', {
                key: key,
                success: false,
                error: 'image-load-failed'
            });
        };
        htmlImg.src = imageUrl;
    },

    /**
     * Remove all currently selected fragment(s) from the canvas.
     * Dispatches a 'puzzle-delete' CustomEvent with the removed keys array
     * so Python can update session storage.
     * @returns {string[]} Array of removed fragment keys.
     */
    removeSelected: function() {
        if (!this.canvas) return;
        var active = this.canvas.getActiveObjects();
        if (!active || active.length === 0) return;

        var self = this;
        var removedKeys = [];
        active.forEach(function(obj) {
            for (var key in self.fragments) {
                if (self.fragments[key] === obj) {
                    removedKeys.push(key);
                    delete self.fragments[key];
                    break;
                }
            }
            self.canvas.remove(obj);
        });
        this.canvas.discardActiveObject();
        this.canvas.requestRenderAll();

        // Notify Python of removed keys (works for keyboard, context-menu, and toolbar)
        if (removedKeys.length > 0) {
            var el = document.getElementById('puzzleCanvas');
            if (el) {
                el.dispatchEvent(new CustomEvent('puzzle-delete', {
                    detail: JSON.stringify(removedKeys), bubbles: true
                }));
            }
        }
        return removedKeys;
    },

    setupWheelZoom: function() {
        var self = this;
        this.canvas.on('mouse:wheel', function(opt) {
            var delta = opt.e.deltaY;
            var zoom = self.canvas.getZoom();
            zoom *= 0.999 ** delta;
            zoom = Math.min(Math.max(0.05, zoom), 10);
            self.canvas.zoomToPoint({ x: opt.e.offsetX, y: opt.e.offsetY }, zoom);
            opt.e.preventDefault();
            opt.e.stopPropagation();
        });
    },

    setupPan: function() {
        var self = this;
        this.canvas.on('mouse:down', function(opt) {
            if (opt.e.button === 0 && !opt.target) {
                self._panning = true;
                self.canvas.setCursor('grabbing');
                self._lastPosX = opt.e.clientX;
                self._lastPosY = opt.e.clientY;
                self.canvas.selection = false;
            }
        });
        this.canvas.on('mouse:move', function(opt) {
            if (self._panning) {
                var vpt = self.canvas.viewportTransform;
                vpt[4] += opt.e.clientX - self._lastPosX;
                vpt[5] += opt.e.clientY - self._lastPosY;
                self.canvas.requestRenderAll();
                self._lastPosX = opt.e.clientX;
                self._lastPosY = opt.e.clientY;
            }
        });
        this.canvas.on('mouse:up', function() {
            if (self._panning) {
                self._panning = false;
                self.canvas.setCursor('default');
                self.canvas.selection = true;
            }
        });
    },

    setupKeyboard: function() {
        var self = this;
        this._keyboardHandler = function(e) {
            if (!self.canvas) return;
            // Don't capture when typing in inputs
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

            var active = self.canvas.getActiveObject();

            // In crop mode, arrow keys crop edges; Enter confirms, Escape reverts
            if (self._cropMode) {
                var step = e.shiftKey ? 40 : 20;
                switch(e.key) {
                    case 'ArrowUp':    self.cropEdge('top', step); e.preventDefault(); break;
                    case 'ArrowDown':  self.cropEdge('bottom', step); e.preventDefault(); break;
                    case 'ArrowLeft':  self.cropEdge('left', step); e.preventDefault(); break;
                    case 'ArrowRight': self.cropEdge('right', step); e.preventDefault(); break;
                    case 'Enter':      self.cropConfirm(); e.preventDefault(); break;
                    case 'Escape':     self.cropRevert(); e.preventDefault(); break;
                }
                return;
            }

            switch(e.key) {
                case 'Delete':
                case 'Backspace':
                    self.removeSelected();
                    e.preventDefault();
                    break;
                case 'ArrowLeft':
                    if (active) {
                        active.set('left', active.left - (e.shiftKey ? 10 : 1));
                        self.canvas.requestRenderAll();
                        e.preventDefault();
                    }
                    break;
                case 'ArrowRight':
                    if (active) {
                        active.set('left', active.left + (e.shiftKey ? 10 : 1));
                        self.canvas.requestRenderAll();
                        e.preventDefault();
                    }
                    break;
                case 'ArrowUp':
                    if (active) {
                        active.set('top', active.top - (e.shiftKey ? 10 : 1));
                        self.canvas.requestRenderAll();
                        e.preventDefault();
                    }
                    break;
                case 'ArrowDown':
                    if (active) {
                        active.set('top', active.top + (e.shiftKey ? 10 : 1));
                        self.canvas.requestRenderAll();
                        e.preventDefault();
                    }
                    break;
                case 'r':
                    if (active && !e.ctrlKey && !e.metaKey) {
                        active.rotate((active.angle || 0) + 1);
                        self.canvas.requestRenderAll();
                    }
                    break;
                case 'R':
                    if (active && !e.ctrlKey && !e.metaKey) {
                        active.rotate((active.angle || 0) - 1);
                        self.canvas.requestRenderAll();
                    }
                    break;
            }
        };
        document.addEventListener('keydown', this._keyboardHandler);
    },

    setupContextMenu: function() {
        var self = this;

        // Prevent browser context menu on canvas
        var upperCanvas = this.canvas.upperCanvasEl || this.canvas.wrapperEl;
        if (upperCanvas) {
            this._contextMenuTarget = upperCanvas;
            this._contextMenuSuppressHandler = function(e) { e.preventDefault(); };
            upperCanvas.addEventListener('contextmenu', this._contextMenuSuppressHandler);
        }

        this.canvas.on('mouse:down', function(opt) {
            if (opt.button === 3 && opt.target) {
                self._showContextMenu(opt.e, opt.target);
            } else {
                self._hideContextMenu();
            }
        });

        // Hide on click anywhere
        this._hideMenuClickHandler = function() { self._hideContextMenu(); };
        this._hideMenuEscapeHandler = function(e) {
            if (e.key === 'Escape') self._hideContextMenu();
        };
        document.addEventListener('click', this._hideMenuClickHandler);
        document.addEventListener('keydown', this._hideMenuEscapeHandler);
    },

    _showContextMenu: function(event, target) {
        this._hideContextMenu();

        var menu = document.createElement('div');
        menu.id = 'puzzle-ctx-menu';
        menu.style.cssText = 'position:fixed;z-index:10000;background:#2d2d2d;border:1px solid #555;' +
            'border-radius:4px;padding:4px 0;box-shadow:0 4px 12px rgba(0,0,0,0.3);min-width:160px;';
        menu.style.left = event.clientX + 'px';
        menu.style.top = event.clientY + 'px';

        var items = [
            {label: 'Flip Horizontal', action: function() { target.set('flipX', !target.flipX); }},
            {label: 'Flip Vertical', action: function() { target.set('flipY', !target.flipY); }},
            {label: '---'},
            {label: 'Delete', action: function() {
                this.canvas.setActiveObject(target);
                this.removeSelected();
            }.bind(this)},
            {label: '---'},
            {label: 'Toggle Background', action: function() {
                self._toggleFragmentBg(target);
            }}
        ];

        var self = this;
        items.forEach(function(item) {
            if (item.label === '---') {
                var sep = document.createElement('div');
                sep.style.cssText = 'height:1px;background:#555;margin:4px 0;';
                menu.appendChild(sep);
            } else {
                var div = document.createElement('div');
                div.textContent = item.label;
                div.style.cssText = 'padding:6px 16px;color:#eee;cursor:pointer;font-size:13px;';
                div.onmouseenter = function() { this.style.background = '#3a3a3a'; };
                div.onmouseleave = function() { this.style.background = 'none'; };
                div.onclick = function(e) {
                    e.stopPropagation();
                    item.action();
                    self.canvas.requestRenderAll();
                    self._hideContextMenu();
                };
                menu.appendChild(div);
            }
        });

        document.body.appendChild(menu);
        this.contextMenu = menu;
    },

    _hideContextMenu: function() {
        if (this.contextMenu) {
            this.contextMenu.remove();
            this.contextMenu = null;
        }
    },

    /**
     * Get the fragment key of the currently selected canvas object.
     * @returns {string|null} The fragment key (e.g. "sys_id,1r") or null if
     *     no fragment is selected.
     */
    getSelectedKey: function() {
        var active = this.canvas.getActiveObject();
        if (!active) return null;
        return active._fragmentKey || null;
    },

    // === Folio Navigation (CANV-07) ===

    folioData: {},  // key -> {sys_id, folios: [{fl_id, label}], currentIndex}

    /**
     * Fetch the ordered list of folios for a manuscript from the server
     * and store them in folioData for navigation. Identifies the current
     * folio index by matching the fragment's fl_id.
     * @param {string} key - Fragment key to associate folio data with.
     * @param {string} sys_id - Manuscript system identifier for the API call.
     */
    loadFolios: async function(key, sys_id) {
        try {
            var resp = await fetch('/api/puzzle_folios/' + sys_id);
            if (!resp.ok) return;
            var folios = await resp.json();
            this.folioData[key] = { sys_id: sys_id, folios: folios, currentIndex: 0 };
            // Find current folio index based on fragment meta
            var frag = this.fragments[key];
            if (frag && frag._fragmentMeta && frag._fragmentMeta.fl_id) {
                var currentFlId = frag._fragmentMeta.fl_id;
                var idx = folios.findIndex(function(f) { return f.fl_id === currentFlId; });
                if (idx >= 0) this.folioData[key].currentIndex = idx;
            }
        } catch(e) {
            console.error('Failed to load folios for', key, e);
        }
    },

    /**
     * Navigate a fragment to a different folio page within the same manuscript.
     * Loads the new folio image at the same position/rotation/scale/flip as
     * the current one, then emits 'puzzle-fragment-meta' to sync Python state.
     * @param {string} key - Fragment key to navigate.
     * @param {number} direction - Folio offset: +1 for next, -1 for previous.
     * @returns {Promise<string>} The label of the new folio (e.g. "1v", "2r").
     */
    navigateFolio: async function(key, direction) {
        var data = this.folioData[key];
        if (!data || !data.folios.length) return '';
        var newIndex = Math.max(0, Math.min(data.folios.length - 1, data.currentIndex + direction));
        if (newIndex === data.currentIndex) return data.folios[data.currentIndex].label || '';
        data.currentIndex = newIndex;
        var folio = data.folios[newIndex];
        var obj = this.fragments[key];
        if (!obj) return folio.label;

        // Use stashed target center from flipAllPuzzle if available (avoids
        // reading stale Fabric.js geometry after bulk position changes)
        var center = obj._flipTargetCenter
            || (typeof obj.getCenterPoint === 'function' ? obj.getCenterPoint() : {
                x: (obj.left || 0) + ((obj.width || 0) * (obj.scaleX || 1) / 2),
                y: (obj.top || 0) + ((obj.height || 0) * (obj.scaleY || 1) / 2)
            });
        // Consume the stashed center so subsequent navigations use live geometry
        delete obj._flipTargetCenter;
        var pos = {
            centerX: center.x,
            centerY: center.y,
            angle: obj.angle,
            scaleX: obj.scaleX,
            scaleY: obj.scaleY,
            flipX: obj.flipX,
            flipY: obj.flipY
        };
        var self = this;
        var meta = obj._fragmentMeta || {};
        var threshold = meta.threshold || 30;
        var size = meta.size || 800;
        var processed = meta.processed !== false;
        var isCul = meta.is_cul || false;
        var url = '/api/puzzle_image?fl_id=' + folio.fl_id +
                  '&threshold=' + threshold +
                  '&size=' + size +
                  '&processed=' + processed +
                  '&is_cul=' + isCul;

        return new Promise(function(resolve) {
            var htmlImg = new Image();
            htmlImg.crossOrigin = 'anonymous';
            htmlImg.decoding = 'async';
            htmlImg.onload = function() {
                try {
                    if (self.canvas && typeof self.canvas.discardActiveObject === 'function') {
                        self.canvas.discardActiveObject();
                    }
                    self._removeFragmentsByKey(key);
                    var img = self._buildFragmentImage(htmlImg, Object.assign({}, pos, {
                        hasControls: true, hasBorders: true,
                        cornerSize: 12, transparentCorners: false,
                        perPixelTargetFind: true, _fragmentKey: key,
                        _imageUrl: url,
                        _fragmentMeta: Object.assign({}, meta, {
                            fl_id: folio.fl_id,
                            threshold: threshold,
                            size: size,
                            processed: processed
                        })
                    }));
                    if (typeof img.setPositionByOrigin === 'function') {
                        var pt = (typeof fabric.Point === 'function')
                            ? new fabric.Point(pos.centerX, pos.centerY)
                            : { x: pos.centerX, y: pos.centerY };
                        img.setPositionByOrigin(pt, 'center', 'center');
                        img.setCoords();
                    }
                    self.canvas.add(img);
                    self.fragments[key] = img;
                    self.canvas.setActiveObject(img);
                    self.canvas.requestRenderAll();
                    self._syncSelection();
                    self._emitEvent('puzzle-fragment-meta', {
                        key: key,
                        meta: Object.assign({}, img._fragmentMeta || {}, {
                            folio_label: folio.label || ''
                        })
                    });
                } catch (err) {
                    console.error('Failed to render folio image:', key, err);
                }
                resolve(folio.label);
            };
            htmlImg.onerror = function() {
                console.error('Failed to load folio image:', url);
                resolve(folio.label);
            };
            htmlImg.src = url;
        });
    },

    getCurrentFolioLabel: function(key) {
        var data = this.folioData[key];
        if (!data || !data.folios.length) return '';
        return data.folios[data.currentIndex] ? data.folios[data.currentIndex].label : '';
    },

    // === Snap Guides (CANV-08) ===

    setupSelectionSync: function() {
        var self = this;
        this.canvas.on('selection:created', function() { self._syncSelection(); });
        this.canvas.on('selection:updated', function() { self._syncSelection(); });
        this.canvas.on('selection:cleared', function() {
            self._emitEvent('puzzle-selection', { key: null, hasSelection: false });
        });
    },

    _syncSelection: function() {
        if (!this.canvas) return;
        var active = this.canvas.getActiveObject();
        if (!active) {
            this._emitEvent('puzzle-selection', { key: null, hasSelection: false });
            return;
        }
        var meta = active._fragmentMeta || {};
        var key = active._fragmentKey || null;
        this._emitEvent('puzzle-selection', {
            key: key,
            hasSelection: true,
            processed: meta.processed !== false,
            threshold: meta.threshold || 30,
            scale: active.scaleX || 1,
            rotation: active.angle || 0,
            folioLabel: key ? (this.getCurrentFolioLabel(key) || '') : ''
        });
    },

    /**
     * Cycle the canvas background through 6 modes in order:
     * dark gray (#333), black, white, checkerboard, light table (#F5F0E0),
     * and measurement grid. Updates bgModeIndex for round-robin cycling.
     */
    cycleBgMode: function() {
        if (!this.canvas) return;
        this.bgModeIndex = (this.bgModeIndex + 1) % this.BG_MODES.length;
        var mode = this.BG_MODES[this.bgModeIndex];

        if (mode === 'checker') {
            this._setCheckerBackground();
        } else if (mode === 'grid') {
            this._setGridBackground();
        } else {
            this.canvas.backgroundImage = null;
            this.canvas.backgroundColor = mode;
        }
        this.canvas.requestRenderAll();
    },

    _setCheckerBackground: function() {
        // Create a checkerboard pattern
        var size = 20;
        var patternCanvas = document.createElement('canvas');
        patternCanvas.width = size * 2;
        patternCanvas.height = size * 2;
        var ctx = patternCanvas.getContext('2d');
        ctx.fillStyle = '#cccccc';
        ctx.fillRect(0, 0, size * 2, size * 2);
        ctx.fillStyle = '#999999';
        ctx.fillRect(0, 0, size, size);
        ctx.fillRect(size, size, size, size);

        var pattern = new fabric.Pattern({
            source: patternCanvas,
            repeat: 'repeat'
        });
        this.canvas.backgroundColor = pattern;
        this.canvas.backgroundImage = null;
    },

    _setGridBackground: function() {
        // Create a 50px grid pattern
        var size = 50;
        var patternCanvas = document.createElement('canvas');
        patternCanvas.width = size;
        patternCanvas.height = size;
        var ctx = patternCanvas.getContext('2d');
        ctx.fillStyle = '#333333';
        ctx.fillRect(0, 0, size, size);
        ctx.strokeStyle = '#555555';
        ctx.lineWidth = 0.5;
        ctx.beginPath();
        ctx.moveTo(size, 0);
        ctx.lineTo(size, size);
        ctx.moveTo(0, size);
        ctx.lineTo(size, size);
        ctx.stroke();

        var pattern = new fabric.Pattern({
            source: patternCanvas,
            repeat: 'repeat'
        });
        this.canvas.backgroundColor = pattern;
        this.canvas.backgroundImage = null;
    },

    // === Crop Mode ===
    _cropMode: false,
    _cropTarget: null,
    _cropOffsets: null,  // {top, bottom, left, right}

    /**
     * Enter or exit crop mode on the currently selected fragment.
     * In crop mode, movement/rotation are locked, edge handles become crop
     * handles (scaling events are intercepted and converted to cropEdge calls),
     * and the border turns red-dashed. Arrow keys crop edges; Enter confirms,
     * Escape reverts.
     * @param {boolean} enable - True to enter crop mode, false to exit.
     * @returns {boolean} Whether crop mode is now active.
     */
    toggleCropMode: function(enable) {
        if (enable) {
            var active = this.canvas.getActiveObject();
            if (!active || !active._fragmentKey) return false;
            this._cropMode = true;
            this._cropTarget = active;
            this._cropOffsets = { top: 0, bottom: 0, left: 0, right: 0 };
            if (!active._originalWidth) {
                active._originalWidth = active.width;
                active._originalHeight = active.height;
                active._originalCropX = active.cropX || 0;
                active._originalCropY = active.cropY || 0;
            }
            // Show only edge handles for crop (not corners for resize)
            // Do NOT lock scaling — we intercept scaling events as crop operations
            active.set({
                lockMovementX: true, lockMovementY: true,
                lockRotation: true,
                hasControls: true,
                borderColor: '#ff4444', borderDashArray: [5, 3]
            });
            active.setControlsVisibility({
                mt: true, mb: true, ml: true, mr: true,  // edge handles for crop
                tl: false, tr: false, bl: false, br: false, mtr: false  // no corners/rotation
            });

            // Override scaling to become cropping
            var self = this;
            active._lastScaleX = active.scaleX;
            active._lastScaleY = active.scaleY;
            active._lastWidth = active.width;
            active._lastHeight = active.height;

            this._cropScalingHandler = function(e) {
                if (!self._cropMode || !self._cropTarget) return;
                var obj = e.target || (e.transform && e.transform.target);
                if (obj !== self._cropTarget) return;

                var transform = e.transform;
                if (!transform) return;
                var corner = transform.corner;

                // Compute drag delta in image-space pixels
                var newW = obj.width * obj.scaleX;
                var oldW = obj._lastWidth * obj._lastScaleX;
                var newH = obj.height * obj.scaleY;
                var oldH = obj._lastHeight * obj._lastScaleY;

                // Revert the scale — all changes go through cropEdge
                obj.scaleX = obj._lastScaleX;
                obj.scaleY = obj._lastScaleY;

                // Positive delta = shrunk = crop inward; negative = expanded = un-crop
                var dw = Math.round((oldW - newW) / obj._lastScaleX);
                var dh = Math.round((oldH - newH) / obj._lastScaleY);

                if (corner === 'ml' && Math.abs(dw) > 3) { self.cropEdge('left', dw); }
                else if (corner === 'mr' && Math.abs(dw) > 3) { self.cropEdge('right', dw); }
                else if (corner === 'mt' && Math.abs(dh) > 3) { self.cropEdge('top', dh); }
                else if (corner === 'mb' && Math.abs(dh) > 3) { self.cropEdge('bottom', dh); }

                obj._lastWidth = obj.width;
                obj._lastHeight = obj.height;
            };
            this.canvas.on('object:scaling', this._cropScalingHandler);

            this.canvas.requestRenderAll();
            return true;
        } else {
            if (this._cropScalingHandler) {
                this.canvas.off('object:scaling', this._cropScalingHandler);
                this._cropScalingHandler = null;
            }
            if (this._cropTarget) {
                this._cropTarget.set({
                    lockMovementX: false, lockMovementY: false,
                    lockRotation: false,
                    hasControls: true,
                    borderColor: null, borderDashArray: null
                });
                this._cropTarget.setControlsVisibility({
                    mt: false, mb: false, ml: false, mr: false,
                    tl: true, tr: true, bl: true, br: true, mtr: true
                });
            }
            this._cropMode = false;
            this._cropTarget = null;
            this._cropOffsets = null;
            this.canvas.requestRenderAll();
            return false;
        }
    },

    /**
     * Crop a specific edge of the fragment in crop mode.
     * Positive amount crops inward (removes pixels), negative un-crops
     * (restores previously cropped pixels up to the original dimensions).
     * Minimum remaining dimension is 50px.
     * @param {string} edge - Which edge to crop: 'top', 'bottom', 'left', or 'right'.
     * @param {number} [amount=20] - Pixels to crop (positive) or restore (negative).
     */
    cropEdge: function(edge, amount) {
        // Positive amount = crop inward, negative = un-crop (restore)
        if (!this._cropMode || !this._cropTarget) return;
        amount = amount || 20;
        var obj = this._cropTarget;
        var origW = obj._originalWidth || obj.width;
        var origH = obj._originalHeight || obj.height;
        var cropX = obj.cropX || 0;
        var cropY = obj.cropY || 0;
        var w = obj.width;
        var h = obj.height;

        if (edge === 'top') {
            var newCropY = Math.max(0, Math.min(cropY + amount, cropY + h - 50));
            var delta = newCropY - cropY;
            if (delta !== 0) {
                obj.set({ cropY: newCropY, height: h - delta });
                this._cropOffsets.top += delta;
            }
        } else if (edge === 'bottom') {
            // Max we can crop: current height - 50. Un-crop limited by original.
            var maxH = origH - (obj.cropY || 0) - this._cropOffsets.bottom;
            var newBottom = Math.max(0, Math.min(this._cropOffsets.bottom + amount, origH - (obj.cropY || 0) - 50));
            var delta = newBottom - this._cropOffsets.bottom;
            if (delta !== 0) {
                obj.set({ height: h - delta });
                this._cropOffsets.bottom = newBottom;
            }
        } else if (edge === 'left') {
            var newCropX = Math.max(0, Math.min(cropX + amount, cropX + w - 50));
            var delta = newCropX - cropX;
            if (delta !== 0) {
                obj.set({ cropX: newCropX, width: w - delta });
                this._cropOffsets.left += delta;
            }
        } else if (edge === 'right') {
            var newRight = Math.max(0, Math.min(this._cropOffsets.right + amount, origW - (obj.cropX || 0) - 50));
            var delta = newRight - this._cropOffsets.right;
            if (delta !== 0) {
                obj.set({ width: w - delta });
                this._cropOffsets.right = newRight;
            }
        }
        obj.setCoords();
        this.canvas.requestRenderAll();
    },

    /**
     * Confirm the current crop, making it permanent.
     * Clears the original dimension references so cropRevert can no longer
     * undo, then exits crop mode.
     */
    cropConfirm: function() {
        if (!this._cropTarget) return;
        this._cropTarget._originalWidth = null;
        this._cropTarget._originalHeight = null;
        this.toggleCropMode(false);
    },

    /**
     * Revert all crop changes on the current fragment, restoring original
     * dimensions and crop offsets, then exit crop mode.
     */
    cropRevert: function() {
        if (!this._cropTarget) return;
        var obj = this._cropTarget;
        if (obj._originalWidth) {
            obj.set({
                width: obj._originalWidth,
                height: obj._originalHeight,
                cropX: obj._originalCropX || 0,
                cropY: obj._originalCropY || 0
            });
            obj._originalWidth = null;
            obj._originalHeight = null;
            obj.setCoords();
        }
        this.toggleCropMode(false);
    },

    /**
     * Navigate each selected fragment to its recto/verso counterpart.
     * Even-indexed folios (0, 2, 4...) are recto; odd (1, 3, 5...) are verso.
     * Toggles each selected fragment to the opposite side by calling
     * navigateFolio with +1 or -1.
     */
    flipRectoVerso: function() {
        var self = this;
        var active = this.canvas.getActiveObjects();
        if (!active || active.length === 0) return;
        active.forEach(function(obj) {
            var key = obj._fragmentKey;
            if (!key) return;
            var data = self.folioData[key];
            if (!data || data.folios.length < 2) return;
            var ci = data.currentIndex;
            var newIdx = (ci % 2 === 0) ? Math.min(ci + 1, data.folios.length - 1) : Math.max(ci - 1, 0);
            if (newIdx !== ci) {
                self.navigateFolio(key, newIdx - ci);
            }
        });
    },

    /**
     * Flip the entire puzzle arrangement for verso viewing.
     * Mirrors all fragment positions horizontally around the group center axis,
     * negates rotation angles, and navigates each fragment to its recto/verso
     * counterpart image. This simulates turning over the assembled join.
     */
    flipAllPuzzle: function() {
        var self = this;
        var keys = Object.keys(this.fragments);
        if (keys.length === 0) return;
        if (this.canvas && typeof this.canvas.discardActiveObject === 'function') {
            this.canvas.discardActiveObject();
        }

        // 1. Snapshot visual centers BEFORE any changes
        var snapshots = {};
        var allCx = [];
        keys.forEach(function(key) {
            var obj = self.fragments[key];
            if (!obj) return;
            var cp = obj.getCenterPoint();
            snapshots[key] = { cx: cp.x, cy: cp.y, origAngle: obj.angle || 0 };
            allCx.push(cp.x);
        });

        // 2. Compute mirror axis
        var axisX = (Math.min.apply(null, allCx) + Math.max.apply(null, allCx)) / 2;

        // 3. Compute mirrored positions and new angles
        keys.forEach(function(key) {
            var s = snapshots[key];
            if (!s) return;
            s.mirroredCx = 2 * axisX - s.cx;
            s.newAngle = (360 - s.origAngle) % 360;
        });

        // 4. Apply mirror + angle to current objects, then navigate to verso.
        //    Store the target center on the object so _reloadFragment / navigateFolio
        //    uses it instead of re-reading the (stale) geometry.
        keys.forEach(function(key) {
            var obj = self.fragments[key];
            var s = snapshots[key];
            if (!obj || !s) return;

            // Apply angle + position
            obj.set({ angle: s.newAngle });
            if (typeof obj.setPositionByOrigin === 'function') {
                var pt = (typeof fabric.Point === 'function')
                    ? new fabric.Point(s.mirroredCx, s.cy)
                    : { x: s.mirroredCx, y: s.cy };
                obj.setPositionByOrigin(pt, 'center', 'center');
            }
            obj.setCoords();

            // Stash target center for the async reload to use
            obj._flipTargetCenter = { x: s.mirroredCx, y: s.cy };
        });
        this.canvas.requestRenderAll();

        // 5. Navigate each fragment to recto/verso counterpart
        keys.forEach(function(key) {
            var data = self.folioData[key];
            if (!data || data.folios.length < 2) return;
            var ci = data.currentIndex;
            var newIdx = (ci % 2 === 0) ? Math.min(ci + 1, data.folios.length - 1) : Math.max(ci - 1, 0);
            if (newIdx !== ci) {
                self.navigateFolio(key, newIdx - ci);
            }
        });
    },

    setSelectedScale: function(scale) {
        var active = this.canvas.getActiveObject();
        if (active) {
            active.set({ scaleX: scale, scaleY: scale });
            this.canvas.requestRenderAll();
        }
    },

    setSelectedRotation: function(degrees) {
        var active = this.canvas.getActiveObject();
        if (active) {
            active.rotate(degrees);
            this.canvas.requestRenderAll();
        }
    },

    /**
     * Zoom and pan the canvas viewport to fit all fragment objects within
     * 90% of the visible area. Caps zoom at 3x to avoid over-magnification.
     * Excludes placeholder text objects.
     */
    bringForward: function() {
        if (!this.canvas) return;
        var active = this.canvas.getActiveObject();
        if (active) {
            this.canvas.bringObjectForward(active);
            this.canvas.requestRenderAll();
        }
    },

    sendBackward: function() {
        if (!this.canvas) return;
        var active = this.canvas.getActiveObject();
        if (active) {
            this.canvas.sendObjectBackwards(active);
            this.canvas.requestRenderAll();
        }
    },

    fitAll: function() {
        if (!this.canvas) return;
        var objects = this.canvas.getObjects().filter(function(o) { return !o._isPlaceholder; });
        if (objects.length === 0) return;

        // Calculate bounding box of all objects
        var minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
        objects.forEach(function(obj) {
            var bounds = obj.getBoundingRect();
            minX = Math.min(minX, bounds.left);
            minY = Math.min(minY, bounds.top);
            maxX = Math.max(maxX, bounds.left + bounds.width);
            maxY = Math.max(maxY, bounds.top + bounds.height);
        });

        var groupWidth = maxX - minX;
        var groupHeight = maxY - minY;
        var canvasWidth = this.canvas.getWidth();
        var canvasHeight = this.canvas.getHeight();

        var zoom = Math.min(
            (canvasWidth * 0.9) / groupWidth,
            (canvasHeight * 0.9) / groupHeight,
            3  // max zoom
        );

        // Center the view
        var centerX = (minX + maxX) / 2;
        var centerY = (minY + maxY) / 2;

        this.canvas.setViewportTransform([1, 0, 0, 1, 0, 0]);
        this.canvas.zoomToPoint(
            { x: canvasWidth / 2, y: canvasHeight / 2 },
            zoom
        );
        var vpt = this.canvas.viewportTransform;
        vpt[4] = canvasWidth / 2 - centerX * zoom;
        vpt[5] = canvasHeight / 2 - centerY * zoom;
        this.canvas.requestRenderAll();
    },

    /**
     * Serialize the spatial state of all fragments to a JSON string.
     * Captures position (x, y), rotation, scale (scaleX, scaleY), and
     * flip state (flipH, flipV) for each fragment key.
     * @returns {string} JSON string of fragment spatial states.
     */
    getState: function() {
        var stateObj = {};
        for (var key in this.fragments) {
            var obj = this.fragments[key];
            var center = (typeof obj.getCenterPoint === 'function')
                ? obj.getCenterPoint()
                : {
                    x: (obj.left || 0) + ((obj.width || 0) * (obj.scaleX || 1) / 2),
                    y: (obj.top || 0) + ((obj.height || 0) * (obj.scaleY || 1) / 2)
                };
            stateObj[key] = {
                x: obj.left,
                y: obj.top,
                centerX: center.x,
                centerY: center.y,
                rotation: obj.angle || 0,
                scaleX: obj.scaleX || 1,
                scaleY: obj.scaleY || 1,
                flipH: !!obj.flipX,
                flipV: !!obj.flipY,
                naturalWidth: obj.width || 800,
                naturalHeight: obj.height || 800,
                unscaledWidth: obj.width || 800,
                unscaledHeight: obj.height || 800,
            };
        }
        return JSON.stringify(stateObj);
    },

    restoreState: function(stateJson) {
        if (!stateJson) return;
        try {
            var parsed = typeof stateJson === 'string' ? JSON.parse(stateJson) : stateJson;
            for (var key in parsed) {
                if (this.fragments[key]) {
                    var s = parsed[key];
                    var obj = this.fragments[key];
                    obj.set({
                        angle: s.rotation || 0,
                        scaleX: s.scaleX || 1,
                        scaleY: s.scaleY || 1,
                        flipX: !!s.flipH,
                        flipV: !!s.flipV,
                    });
                    if (typeof obj.setPositionByOrigin === 'function' &&
                        s.centerX !== undefined && s.centerY !== undefined) {
                        var pt = (typeof fabric.Point === 'function')
                            ? new fabric.Point(s.centerX, s.centerY)
                            : { x: s.centerX, y: s.centerY };
                        obj.setPositionByOrigin(pt, 'center', 'center');
                    } else {
                        obj.set({
                            left: s.x,
                            top: s.y,
                        });
                    }
                    obj.setCoords();
                }
            }
            this.canvas.requestRenderAll();
        } catch(e) {
            console.error('Failed to restore puzzle state:', e);
        }
    },

    toggleSelectedBg: function() {
        var active = this.canvas.getActiveObject();
        if (active) this._toggleFragmentBg(active);
    },

    setSelectedOriginal: function(showOriginal) {
        if (!this.canvas) return;
        var active = this.canvas.getActiveObject();
        if (!active || !active._fragmentMeta) return;
        var processed = active._fragmentMeta.processed !== false;
        var desiredProcessed = !showOriginal;
        if (processed !== desiredProcessed) {
            this._toggleFragmentBg(active);
        }
    },

    /**
     * Reload a fragment's image in-place, preserving its position, rotation,
     * scale, and flip state. Used when toggling background removal or
     * changing the threshold.
     * @param {string} key - Fragment key to reload.
     * @param {string} newUrl - New image URL to load.
     * @param {Object} [newMeta] - Updated metadata to attach to the fragment.
     */
    _reloadFragment: function(key, newUrl, newMeta) {
        var obj = this.fragments[key];
        if (!obj) return;
        var self = this;
        var center = (typeof obj.getCenterPoint === 'function')
            ? obj.getCenterPoint()
            : {
                x: (obj.left || 0) + ((obj.width || 0) * (obj.scaleX || 1) / 2),
                y: (obj.top || 0) + ((obj.height || 0) * (obj.scaleY || 1) / 2)
            };
        var pos = {
            centerX: center.x,
            centerY: center.y,
            angle: obj.angle,
            scaleX: obj.scaleX, scaleY: obj.scaleY,
            flipX: obj.flipX, flipY: obj.flipY
        };
        var htmlImg = new Image();
        htmlImg.crossOrigin = 'anonymous';
        htmlImg.decoding = 'async';
        htmlImg.onload = function() {
            try {
                if (self.canvas && typeof self.canvas.discardActiveObject === 'function') {
                    self.canvas.discardActiveObject();
                }
                self._removeFragmentsByKey(key);
                var img = self._buildFragmentImage(htmlImg, Object.assign({}, pos, {
                    hasControls: true, hasBorders: true,
                    cornerSize: 12, transparentCorners: false,
                    perPixelTargetFind: true, _fragmentKey: key,
                    _imageUrl: newUrl, _fragmentMeta: newMeta || obj._fragmentMeta
                }));
                if (typeof img.setPositionByOrigin === 'function') {
                    var pt = (typeof fabric.Point === 'function')
                        ? new fabric.Point(pos.centerX, pos.centerY)
                        : { x: pos.centerX, y: pos.centerY };
                    img.setPositionByOrigin(pt, 'center', 'center');
                    img.setCoords();
                }
                self.canvas.add(img);
                self.fragments[key] = img;
                self.canvas.setActiveObject(img);
                self.canvas.requestRenderAll();
                self._syncSelection();
                self._emitEvent('puzzle-fragment-meta', {
                    key: key,
                    meta: img._fragmentMeta || null
                });
            } catch (err) {
                console.error('Failed to rebuild fragment:', key, err);
            }
        };
        htmlImg.onerror = function() {
            console.error('Failed to reload fragment:', key);
        };
        htmlImg.src = newUrl;
    },

    /**
     * Toggle background removal on a specific fragment.
     * Switches between processed (transparent background) and original
     * (full IIIF image) by reloading via _reloadFragment with the
     * flipped 'processed' flag.
     * @param {fabric.Image} target - The Fabric image object to toggle.
     */
    clearAll: function() {
        if (!this.canvas) return;
        var keys = Object.keys(this.fragments);
        for (var i = 0; i < keys.length; i++) {
            var obj = this.fragments[keys[i]];
            if (obj) this.canvas.remove(obj);
        }
        this.fragments = {};
        this.folioData = {};
        if (typeof this.canvas.discardActiveObject === 'function') {
            this.canvas.discardActiveObject();
        }
        this.canvas.setViewportTransform([1, 0, 0, 1, 0, 0]);
        this.canvas.requestRenderAll();
        this._emitEvent('puzzle-clear', {});
    },

    getCropState: function() {
        var cropState = {};
        for (var key in this.fragments) {
            var obj = this.fragments[key];
            if (obj._originalWidth && (
                (obj.cropX || 0) > 0 || (obj.cropY || 0) > 0 ||
                obj.width < obj._originalWidth || obj.height < obj._originalHeight
            )) {
                cropState[key] = {
                    cropX: obj.cropX || 0,
                    cropY: obj.cropY || 0,
                    width: obj.width,
                    height: obj.height,
                    originalWidth: obj._originalWidth,
                    originalHeight: obj._originalHeight
                };
            }
        }
        return JSON.stringify(cropState);
    },

    _toggleFragmentBg: function(target) {
        if (!target || !target._fragmentKey) return;
        var meta = target._fragmentMeta;
        if (!meta) return;

        var isProcessed = meta.processed !== false;
        var newProcessed = !isProcessed;
        var url = '/api/puzzle_image?fl_id=' + meta.fl_id +
                  '&threshold=' + (meta.threshold || 30) +
                  '&size=' + (meta.size || 800) +
                  '&processed=' + newProcessed +
                  '&is_cul=' + (meta.is_cul || false);
        var newMeta = Object.assign({}, meta, { processed: newProcessed });
        this._reloadFragment(target._fragmentKey, url, newMeta);
    }
};
</script>
'''

# Context menu styles
PUZZLE_STYLES = '''
<style>
.puzzle-container {
    display: flex;
    flex-direction: column;
    height: calc(100vh - 80px);
    background: #1a1a1a;
}
.puzzle-toolbar {
    display: flex;
    flex-wrap: nowrap;
    gap: 4px;
    padding: 4px 8px;
    background: #2d2d2d;
    border-bottom: 1px solid #444;
    align-items: center;
    min-height: 32px;
    color: #e0e0e0 !important;
}
.puzzle-toolbar * {
    color: #e0e0e0 !important;
}
.puzzle-toolbar .q-btn {
    color: #90caf9 !important;
}
.puzzle-toolbar .q-field__native,
.puzzle-toolbar .q-field__input,
.puzzle-toolbar input {
    color: #e0e0e0 !important;
}
.puzzle-toolbar .q-checkbox__label {
    color: #e0e0e0 !important;
}
.puzzle-toolbar .q-field__control {
    background: rgba(255,255,255,0.1) !important;
}
.puzzle-toolbar .q-field--outlined .q-field__control:before {
    border-color: #666 !important;
}
.puzzle-toolbar .q-field__native::placeholder {
    color: #999 !important;
}
.puzzle-toolbar .q-input {
    max-width: 220px;
}
.puzzle-toolbar .q-slider {
    padding: 0;
    margin: 0;
}
.puzzle-canvas-wrap {
    flex: 1;
    position: relative;
    overflow: hidden;
    min-height: 400px;
}
.puzzle-canvas-wrap canvas {
    display: block;
}
.puzzle-slider-group {
    display: flex;
    align-items: center;
    gap: 4px;
}
.puzzle-slider-group .q-slider {
    min-width: 120px;
}
</style>
'''


def _resolve_folios(sys_id: str) -> list:
    """Resolve folio FL IDs from NLI IIIF manifest for a manuscript.

    Fetches the IIIF v2.1 manifest for the given sys_id, extracts FL IDs
    from each canvas's image service URL, and assigns recto/verso labels
    based on page index parity (even=recto, odd=verso).

    Args:
        sys_id: NLI system number (e.g. '990001234560205171').

    Returns:
        List of dicts with 'fl_id' (str) and 'label' (str, e.g. '1r', '1v').
        Empty list if manifest fetch fails or contains no FL IDs.
    """
    import re as _re
    import requests as _requests
    try:
        url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        resp = _requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
        fl_ids = []
        if 'sequences' in data and data['sequences']:
            for canvas in data['sequences'][0].get('canvases', []):
                images = canvas.get('images', [])
                if images:
                    resource = images[0].get('resource', {})
                    service = resource.get('service', {})
                    service_id = service.get('@id', '')
                    m = _re.search(r'/FL(\d+)', service_id)
                    if m:
                        fl_ids.append(m.group(1))
        if not fl_ids:
            return []
        return [
            {'fl_id': fid, 'label': f'{(i // 2) + 1}{"r" if i % 2 == 0 else "v"}'}
            for i, fid in enumerate(fl_ids)
        ]
    except Exception as e:
        logger.error(f"Failed to resolve folios for {sys_id}: {e}")
        return []


def _invalidate_and_refetch(fl_id: str, new_threshold: float):
    """Invalidate the cached processed image and pre-fetch at a new threshold.

    Called when the user adjusts the background removal threshold slider.
    Clears existing cached images for the given fl_id (all thresholds),
    then triggers a new background removal at the specified threshold.

    Args:
        fl_id: NLI folio leaf identifier.
        new_threshold: New HSV distance threshold for background removal.
    """
    try:
        from shared.puzzle_image_service import get_puzzle_image_service
        service = get_puzzle_image_service()
        service.invalidate_cache(fl_id, threshold=None)
        # Pre-fetch at new threshold
        service.resolve_fragment_image(fl_id=fl_id, size=800, threshold=new_threshold, processed=True)
    except Exception as e:
        logger.error(f"Threshold refetch failed for {fl_id}: {e}")


def _parse_puzzle_event_args(args):
    """Parse CustomEvent payloads dispatched from the puzzle canvas JavaScript.

    NiceGUI delivers CustomEvent args in various forms depending on the
    event source. This normalizes them: extracts 'detail' from wrapper dicts,
    JSON-decodes strings, and passes through dicts/lists unchanged.

    Args:
        args: Raw event arguments from NiceGUI event handler.

    Returns:
        Parsed payload as dict, list, or string.
    """
    payload = args
    if isinstance(payload, dict) and 'detail' in payload:
        payload = payload['detail']
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload
    return payload


async def _add_fragment_by_sys_id(sys_id, shelfmark, puzzle_meta, pending_fragment_meta, threshold_slider):
    """Resolve a manuscript's folios and add the first folio image to the canvas.

    Fetches the NLI IIIF manifest to get FL IDs, determines the appropriate
    background removal threshold (150 for CUL/T-S blue backgrounds, 30 default),
    issues a JavaScript addFragment() call, and registers the fragment in
    pending_fragment_meta for confirmation tracking.

    Args:
        sys_id: Manuscript system number.
        shelfmark: Display shelfmark string (used for CUL detection and notifications).
        puzzle_meta: Dict of confirmed fragment metadata keyed by fragment key.
        pending_fragment_meta: Dict of fragments awaiting JS confirmation.
        threshold_slider: NiceGUI slider widget (unused here, passed for API compat).
    """
    folios = await run.io_bound(_resolve_folios, sys_id)
    if not folios:
        ui.notify(tr('No images found'), type='warning')
        return

    first = folios[0]
    fl_id = first.get('fl_id', '')
    folio_label = first.get('label', '1r')
    key = f"{sys_id},{folio_label}"

    # CUL/T-S threshold matching desktop defaults (115 for blue backgrounds)
    threshold = 30.0
    is_cul = False
    if state.meta_mgr:
        lib_code = state.meta_mgr.get_library_for_id(sys_id) or ''
        if lib_code == 'CUL':
            is_cul = True
    if not is_cul and shelfmark:
        s = shelfmark.upper()
        if s.startswith(('T-S', 'OR.', 'ADD.')):
            is_cul = True
    if is_cul:
        threshold = 150.0
    url = f"/api/puzzle_image?fl_id={fl_id}&threshold={threshold}&size=800&processed=true&is_cul={'true' if is_cul else 'false'}"

    frag_offset = len(puzzle_meta) * 50
    meta = {
        'fl_id': fl_id, 'threshold': threshold,
        'size': 800, 'processed': True,
        'sys_id': sys_id, 'is_cul': is_cul
    }
    meta_json = json.dumps(meta)
    ui.run_javascript(
        f'window.puzzleCanvas.addFragment("{key}", "{url}", '
        f'{100 + frag_offset}, {100 + frag_offset}, 0, 1.0, false, false, {meta_json})'
    )

    pending_fragment_meta[key] = {
        'sys_id': sys_id,
        'shelfmark': shelfmark,
        'folio_label': folio_label,
        'fl_id': fl_id,
        'threshold': threshold,
        'processed': True,
        'size': 800,
    }


def create_puzzle_page(initial_add: str = None, initial_doc: str = None):
    """Create the Fragment Puzzle page with Fabric.js canvas and toolbar.

    Builds the full puzzle UI: shelfmark input for adding fragments, action
    buttons (delete, flip, crop, background cycle, fit-all), folio navigation
    (prev/next), scale/rotation/threshold sliders, and the Fabric.js canvas.

    Handles session persistence via app.storage.tab: fragment metadata is saved
    to 'puzzle_fragments' and spatial state (positions/rotations) to 'puzzle_state'.
    State is auto-saved every 30 seconds and restored on page revisit.

    Listens for JavaScript CustomEvents (puzzle-delete, puzzle-add-result,
    puzzle-fragment-meta, puzzle-selection) to keep Python-side state in sync
    with the Fabric.js canvas.

    Args:
        initial_add: Optional fragment to auto-add on page load. Format is
            'sys_id,fl_id' or just 'sys_id' (first folio resolved automatically).
            Passed via /puzzle?add=... query parameter from entry points.
        initial_doc: Optional document ID to load on page init. Passed via
            /puzzle?doc=... query parameter (used by fork flow and deep links).
    """
    # Add Fabric.js CDN and page-specific styles
    ui.add_head_html(FABRIC_JS_CDN)
    ui.add_head_html(PUZZLE_STYLES)
    ui.add_body_html(PUZZLE_CANVAS_JS)

    # Track fragment metadata (Python-side)
    # NOTE: app.storage.tab requires client connection, so start empty
    # and populate in init_canvas() after client connects
    puzzle_meta = {}
    pending_fragment_meta = {}
    control_sync = {'active': False}

    # Join document state tracking
    # NOTE: Web joins.db is shared across all users (same as pgp.db, fjms_enrichment.db).
    # User-scoped persistence with auth is Phase 52 (Community + Integration).
    # For v1, the web app serves a single researcher.
    doc_state = {
        'current_doc_id': None,       # None = scratch pad
        'has_unsaved_changes': False,
        'auto_save_handle': None,     # for cancelling debounced auto-save
        'loading': False,             # True while fragment images are loading (guards auto-save)
        'load_pending': 0,            # Number of fragments still loading
        'pending_crops': {},          # Crop state to apply after each fragment loads {key: {cropX, cropY, width, height}}
        'is_published': False,        # Whether current doc is published to community
    }

    from shared.puzzle_service import get_puzzle_service
    from shared.puzzle_export import auto_suggest_title
    from shared.puzzle_model import PuzzleDocument, PuzzleFragment

    # ── Document management functions ──

    async def build_fragments_list():
        """Build PuzzleFragment list from current canvas state."""
        state_json = await ui.run_javascript(
            'window.puzzleCanvas.getState()', timeout=5.0
        )
        state_data = json.loads(state_json) if state_json else {}
        # Get crop state from per-object Fabric.js properties
        crop_json = await ui.run_javascript(
            'window.puzzleCanvas.getCropState()', timeout=5.0
        )
        crop_data = json.loads(crop_json) if crop_json else {}

        fragments = []
        for key, meta in puzzle_meta.items():
            parts = key.split(',', 1)
            sys_id = parts[0]
            folio_label = meta.get('folio_label') or (parts[1] if len(parts) > 1 else '1r')
            spatial = state_data.get(key, {})
            crop = crop_data.get(key, {})

            # Convert Fabric.js crop properties to crop_top/bottom/left/right.
            crop_x = crop.get('cropX', 0)
            crop_y = crop.get('cropY', 0)
            crop_w = crop.get('width', 0)
            crop_h = crop.get('height', 0)
            orig_w = crop.get('originalWidth', 0)
            orig_h = crop.get('originalHeight', 0)
            if orig_w > 0 and orig_h > 0:
                c_left = int(crop_x)
                c_top = int(crop_y)
                c_right = int(orig_w - (crop_x + crop_w))
                c_bottom = int(orig_h - (crop_y + crop_h))
            else:
                c_left = c_top = c_right = c_bottom = 0

            # Build export/save coordinates from the visual center rather than Fabric's
            # left/top translation. This matches the desktop/QGraphics model:
            #   center = pos + (unscaled cropped size / 2)
            center_x = spatial.get('centerX')
            center_y = spatial.get('centerY')
            sx = spatial.get('scaleX', 1.0)
            sy = spatial.get('scaleY', 1.0)
            nat_w = spatial.get('unscaledWidth', spatial.get('naturalWidth', 800))
            nat_h = spatial.get('unscaledHeight', spatial.get('naturalHeight', 800))
            if center_x is None or center_y is None:
                fab_x = spatial.get('x', 0)
                fab_y = spatial.get('y', 0)
                center_x = fab_x + nat_w * sx / 2.0
                center_y = fab_y + nat_h * sy / 2.0
            pos_x = center_x - nat_w / 2.0
            pos_y = center_y - nat_h / 2.0

            frag = PuzzleFragment(
                sys_id=sys_id,
                folio_label=folio_label,
                fl_id=meta.get('fl_id', ''),
                shelfmark=meta.get('shelfmark', ''),
                x=pos_x,
                y=pos_y,
                rotation=spatial.get('rotation', 0),
                scale=sx,
                flip_h=spatial.get('flipH', False),
                flip_v=spatial.get('flipV', False),
                bg_removal_threshold=meta.get('threshold', 30.0),
                crop_top=c_top,
                crop_bottom=c_bottom,
                crop_left=c_left,
                crop_right=c_right,
                processed=meta.get('processed', True),
            )
            fragments.append(frag)
        return fragments

    def schedule_auto_save():
        """Schedule a debounced auto-save using asyncio."""
        if doc_state['current_doc_id'] is None:
            doc_state['has_unsaved_changes'] = True
            return
        # Do NOT schedule auto-save while document is still loading.
        if doc_state.get('loading'):
            return
        import asyncio
        if doc_state.get('auto_save_handle'):
            doc_state['auto_save_handle'].cancel()

        async def do_auto_save():
            await asyncio.sleep(1.5)
            if doc_state['current_doc_id'] and not doc_state.get('loading'):
                fragments = await build_fragments_list()
                svc = get_puzzle_service(thread_safe=True)
                doc = await run.io_bound(svc.load_document, doc_state['current_doc_id'])
                if doc:
                    doc.fragments = fragments
                    doc.title = title_input.value or doc.title
                    doc.notes = notes_input.value or ''
                    from datetime import datetime
                    doc.updated_at = datetime.now().isoformat()
                    await run.io_bound(lambda: _save_doc_with_thumbnail(doc, fragments))

        loop = asyncio.get_event_loop()
        doc_state['auto_save_handle'] = loop.create_task(do_auto_save())

    def _save_doc_with_thumbnail(doc, fragments):
        """Save document with thumbnail (runs in thread via io_bound)."""
        from shared.puzzle_export import generate_thumbnail
        from shared.puzzle_image_service import get_puzzle_image_service
        img_svc = get_puzzle_image_service()
        thumb = generate_thumbnail(fragments, img_svc, thumb_size=150)
        svc = get_puzzle_service(thread_safe=True)
        svc.save_document(doc, thumbnail_b64=thumb)

    async def refresh_docs_list():
        """Refresh the saved documents list in the left drawer."""
        docs_container.clear()
        svc = get_puzzle_service(thread_safe=True)
        docs = await run.io_bound(svc.list_documents)
        with docs_container:
            if not docs:
                ui.label(tr('No saved joins')).style('color: var(--text-secondary); font-style: italic;')
                return
            for d in docs:
                doc_id = d['id']
                title = d.get('title', '') or 'Untitled'
                shelfmarks = d.get('shelfmarks_summary', '')
                updated = d.get('updated_at', '')[:10]
                thumb_b64 = d.get('thumbnail_b64', '')

                with ui.card().classes('w-full cursor-pointer').style(
                    'background: var(--bg-secondary); padding: 8px;'
                ).on('click', lambda _, did=doc_id: load_document(did)):
                    with ui.row().classes('items-center gap-2 w-full no-wrap'):
                        if thumb_b64:
                            ui.image(f'/api/puzzle_thumbnail/{doc_id}').style(
                                'width: 48px; height: 48px; object-fit: contain; border-radius: 4px;'
                            )
                        else:
                            ui.icon('image', size='lg').style('color: var(--text-secondary);')
                        with ui.column().classes('gap-0').style('flex: 1; min-width: 0;'):
                            ui.label(title).classes('text-body2 ellipsis').style(
                                'color: var(--text-primary); max-width: 160px; font-weight: 500;'
                            )
                            # Only show shelfmarks if not already contained in title
                            if shelfmarks and shelfmarks.strip() not in title:
                                ui.label(shelfmarks).classes('text-caption ellipsis').style(
                                    'color: var(--text-tertiary); max-width: 160px; font-size: 10px; font-family: monospace;'
                                )
                            ui.label(updated).classes('text-caption').style('color: var(--text-tertiary); font-size: 10px;')
                    with ui.row().classes('justify-end gap-1'):
                        ui.button(icon='delete', on_click=lambda _, did=doc_id, t=title: confirm_delete(did, t)).props(
                            'dense flat round size=xs color=negative'
                        ).tooltip(tr('Delete'))
                        is_active = doc_state['current_doc_id'] == doc_id
                        if is_active:
                            ui.icon('check_circle', size='xs').style('color: #4caf50;')

    async def save_join():
        """Save current canvas as a join document."""
        if not puzzle_meta:
            ui.notify(tr('Add fragments before saving'), type='warning')
            return
        fragments = await build_fragments_list()
        suggested_title = auto_suggest_title(fragments)

        with ui.dialog() as dlg, ui.card().classes('w-96 p-4').style(
            'background: var(--bg-card); color: var(--text-primary);'
        ):
            ui.label(tr('Save Puzzle Document')).classes('text-lg font-bold').style('color: var(--text-primary);')
            save_title = ui.input(
                label=tr('Title'), value=doc_state.get('current_doc_id') and title_input.value or suggested_title
            ).props('outlined dense').classes('w-full')
            save_notes = ui.textarea(
                label=tr('Notes'), value=doc_state.get('current_doc_id') and notes_input.value or ''
            ).props('outlined dense').classes('w-full')

            async def do_save():
                import uuid
                doc_id = doc_state['current_doc_id'] or str(uuid.uuid4())
                doc = PuzzleDocument(
                    id=doc_id,
                    title=save_title.value or suggested_title,
                    notes=save_notes.value or '',
                    fragments=fragments,
                )
                await run.io_bound(lambda: _save_doc_with_thumbnail(doc, fragments))
                doc_state['current_doc_id'] = doc_id
                doc_state['has_unsaved_changes'] = False
                title_input.value = doc.title
                notes_input.value = doc.notes
                frag_text = '\n'.join(f"{f.shelfmark} ({f.folio_label})" for f in fragments)
                fragments_label.text = frag_text or tr('No fragments')
                details_container.style('display: block;')
                dlg.close()
                await refresh_docs_list()
                ui.notify(tr('Saved: {}').format(doc.title), type='positive')

            with ui.row().classes('w-full justify-end gap-2'):
                ui.button(tr('Save'), on_click=do_save).props('flat color=primary')
                ui.button(tr('Cancel'), on_click=dlg.close).props('flat')
        dlg.open()

    async def load_document(doc_id):
        """Load a saved document onto the canvas."""
        svc = get_puzzle_service(thread_safe=True)
        doc = await run.io_bound(svc.load_document, doc_id)
        if doc is None:
            ui.notify(tr('Could not load document'), type='warning')
            return

        await ui.run_javascript('window.puzzleCanvas.clearAll()')
        puzzle_meta.clear()
        pending_fragment_meta.clear()

        doc_state['current_doc_id'] = doc.id
        doc_state['has_unsaved_changes'] = False

        # Set loading guard to prevent auto-save from overwriting
        # a partially-loaded document with a subset of fragments.
        doc_state['loading'] = True
        doc_state['load_pending'] = len(doc.fragments)

        # Store pending crop state per fragment key.
        doc_state['pending_crops'] = {}

        for frag in doc.fragments:
            key = f"{frag.sys_id},{frag.folio_label}"
            threshold = frag.bg_removal_threshold
            processed = frag.processed
            frag_is_cul = bool(frag.shelfmark and frag.shelfmark.upper().startswith(('T-S', 'OR.', 'ADD.')))
            if not frag_is_cul and state.meta_mgr:
                frag_is_cul = (state.meta_mgr.get_library_for_id(frag.sys_id) or '') == 'CUL'
            url = f"/api/puzzle_image?fl_id={frag.fl_id}&threshold={threshold}&size=800&processed={'true' if processed else 'false'}&is_cul={'true' if frag_is_cul else 'false'}"
            js_meta = json.dumps({
                'fl_id': frag.fl_id, 'threshold': threshold,
                'size': 800, 'processed': processed, 'sys_id': frag.sys_id,
                'is_cul': frag_is_cul
            })
            ui.run_javascript(
                f'window.puzzleCanvas.addFragment("{key}", "{url}", '
                f'{frag.x}, {frag.y}, {frag.rotation}, {frag.scale}, '
                f'{"true" if frag.flip_h else "false"}, {"true" if frag.flip_v else "false"}, {js_meta}, true)'
            )

            # If fragment has crop, store in pending_crops for on_puzzle_add_result
            if frag.crop_top or frag.crop_bottom or frag.crop_left or frag.crop_right:
                doc_state['pending_crops'][key] = {
                    'cropX': frag.crop_left,
                    'cropY': frag.crop_top,
                    'crop_right': frag.crop_right,
                    'crop_bottom': frag.crop_bottom,
                    'x': frag.x,
                    'y': frag.y,
                }

            puzzle_meta[key] = {
                'sys_id': frag.sys_id, 'shelfmark': frag.shelfmark,
                'folio_label': frag.folio_label, 'fl_id': frag.fl_id,
                'threshold': threshold, 'processed': processed, 'size': 800,
            }

        # If doc has zero fragments, clear loading guard immediately
        if not doc.fragments:
            doc_state['loading'] = False
            doc_state['load_pending'] = 0

        title_input.value = doc.title
        notes_input.value = doc.notes
        frag_text = '\n'.join(f"{f.shelfmark} ({f.folio_label})" for f in doc.fragments)
        fragments_label.text = frag_text or tr('No fragments')
        details_container.style('display: block;')
        joins_dialog.close()
        ui.notify(tr('Loaded: {}').format(doc.title), type='info')
        await check_publish_state()

    async def new_puzzle():
        """Clear canvas and reset to scratch pad state."""
        await ui.run_javascript('window.puzzleCanvas.clearAll()')
        puzzle_meta.clear()
        pending_fragment_meta.clear()
        doc_state['current_doc_id'] = None
        doc_state['has_unsaved_changes'] = False
        doc_state['loading'] = False
        doc_state['load_pending'] = 0
        doc_state['pending_crops'] = {}
        title_input.value = ''
        notes_input.value = ''
        fragments_label.text = ''
        details_container.style('display: none;')
        doc_state['is_published'] = False
        try:
            app.storage.tab['puzzle_fragments'] = {}
            app.storage.tab['puzzle_state'] = None
        except RuntimeError:
            pass
        try:
            _refresh_fragment_select()
        except NameError:
            pass  # Not yet created during init

    async def check_publish_state():
        """Check if current doc is published (using anon client)."""
        if not doc_state['current_doc_id']:
            doc_state['is_published'] = False
            try:
                publish_btn.props('flat')
                publish_btn.props(remove='color')
                publish_btn.tooltip(tr('Publish to Community'))
            except Exception:
                pass
            return
        try:
            from shared.puzzle_publish_service import get_published_join_detail
            from web.supabase_client import get_client
            detail = await run.io_bound(get_published_join_detail, get_client(), doc_state['current_doc_id'])
            if detail and detail.get('is_published'):
                doc_state['is_published'] = True
                publish_btn.props(remove='flat')
                publish_btn.props('color=green')
                publish_btn.tooltip(tr('Published -- click to unpublish'))
            else:
                doc_state['is_published'] = False
                publish_btn.props('flat')
                publish_btn.props(remove='color')
                publish_btn.tooltip(tr('Publish to Community'))
        except Exception:
            pass

    async def toggle_publish():
        """Publish or unpublish current join."""
        from web.auth_state import GlobalAuthState
        if not GlobalAuthState.is_logged_in():
            ui.notify(tr('Please log in to publish'), type='warning')
            return
        if not puzzle_meta:
            ui.notify(tr('Add fragments before publishing'), type='warning')
            return
        if not doc_state['current_doc_id']:
            ui.notify(tr('Save the puzzle first'), type='warning')
            return

        user_id = GlobalAuthState.get_user_id()

        if doc_state['is_published']:
            # UNPUBLISH flow
            try:
                from shared.puzzle_publish_service import unpublish_join
                from web.supabase_client import get_user_client
                client = get_user_client()
                await run.io_bound(unpublish_join, client, user_id, doc_state['current_doc_id'])
                doc_state['is_published'] = False
                publish_btn.props('flat')
                publish_btn.props(remove='color')
                publish_btn.tooltip(tr('Publish to Community'))
                publish_btn.update()
                ui.notify(tr('Unpublished'), type='info')
            except Exception as e:
                logger.error(f"Unpublish failed: {e}")
                ui.notify(tr('Unpublish failed: {}').format(str(e)), type='negative')
            return

        # PUBLISH flow -- confirm dialog
        with ui.dialog() as dlg, ui.card().classes('w-96 p-4').style(
            'background: var(--bg-card); color: var(--text-primary);'
        ):
            ui.label(tr('Publish to Community')).classes('text-lg font-bold').style('color: var(--text-primary);')
            ui.label(tr('This will make your puzzle join visible to all users.')).classes('text-sm').style('color: var(--text-secondary);')

            async def do_publish():
                dlg.close()
                ui.notify(tr('Publishing...'), type='info')
                try:
                    from shared.puzzle_publish_service import publish_join
                    from shared.puzzle_image_service import get_puzzle_image_service
                    from web.supabase_client import get_user_client

                    svc = get_puzzle_service(thread_safe=True)
                    doc = await run.io_bound(svc.load_document, doc_state['current_doc_id'])
                    if not doc:
                        ui.notify(tr('Could not load document'), type='negative')
                        return
                    doc.fragments = await build_fragments_list()
                    doc.title = title_input.value or doc.title
                    doc.notes = notes_input.value or ''

                    img_svc = get_puzzle_image_service()
                    client = get_user_client()
                    published_id = await run.io_bound(publish_join, client, user_id, doc, img_svc)
                    doc_state['is_published'] = True
                    publish_btn.props(remove='flat')
                    publish_btn.props('color=green')
                    publish_btn.tooltip(tr('Published -- click to unpublish'))
                    publish_btn.update()
                    share_url = f'/puzzle?doc={published_id}'
                    # Show share dialog with copyable link
                    with ui.dialog() as share_dlg, ui.card().classes('w-96 p-4').style(
                        'background: var(--bg-card); color: var(--text-primary);'
                    ):
                        ui.label(tr('Published successfully!')).classes('text-lg font-bold').style('color: var(--text-primary);')
                        ui.label(tr('Share this link:')).classes('text-sm mt-2').style('color: var(--text-secondary);')
                        share_input = ui.input(value=share_url).props('readonly outlined dense').classes('w-full mt-1')
                        with ui.row().classes('w-full justify-end gap-2 mt-2'):
                            async def _copy_share_link(url=share_url):
                                await ui.run_javascript(f'navigator.clipboard.writeText(window.location.origin + "{url}")')
                                ui.notify(tr('Link copied!'), type='positive')
                            ui.button(tr('Copy Link'), icon='content_copy', on_click=_copy_share_link).props('flat color=primary')
                            ui.button(tr('Close'), on_click=share_dlg.close).props('flat')
                    share_dlg.open()
                except Exception as e:
                    logger.error(f"Publish failed: {e}")
                    ui.notify(tr('Publish failed: {}').format(str(e)), type='negative')

            with ui.row().classes('w-full justify-end gap-2'):
                ui.button(tr('Publish'), on_click=do_publish).props('flat color=primary')
                ui.button(tr('Cancel'), on_click=dlg.close).props('flat')
        dlg.open()

    async def export_png():
        """Export current canvas as full-resolution PNG with metadata banner."""
        if not puzzle_meta:
            ui.notify(tr('Add fragments before exporting'), type='warning')
            return
        fragments = await build_fragments_list()
        ui.notify(tr('Generating export...'), type='info')

        def _do_export():
            from shared.puzzle_export import compose_puzzle_export, add_metadata_banner
            from shared.puzzle_image_service import get_puzzle_image_service
            import io
            img_svc = get_puzzle_image_service()
            result = compose_puzzle_export(fragments, img_svc, export_size=3000, margin=20)
            if result is None:
                return None
            result = add_metadata_banner(result, fragments, app_variant='web')
            buf = io.BytesIO()
            result.save(buf, 'PNG')
            return buf.getvalue()

        png_bytes = await run.io_bound(_do_export)
        if png_bytes:
            import tempfile, os
            filename = auto_suggest_title(fragments).replace(' ', '_').replace('+', '_') + '.png'
            tmp = os.path.join(tempfile.gettempdir(), filename)
            with open(tmp, 'wb') as fout:
                fout.write(png_bytes)
            ui.download(tmp, filename)
            ui.notify(tr('Export ready'), type='positive')
        else:
            ui.notify(tr('Export failed'), type='negative')

    async def confirm_delete(doc_id, title):
        """Delete a saved document with confirmation."""
        with ui.dialog() as dlg, ui.card().classes('p-4').style('background: var(--bg-card); color: var(--text-primary);'):
            ui.label(tr('Delete "{}"?').format(title)).classes('text-lg').style('color: var(--text-primary);')
            with ui.row().classes('w-full justify-end gap-2'):
                async def do_delete():
                    svc = get_puzzle_service(thread_safe=True)
                    # Auto-unpublish from Supabase if published
                    try:
                        from shared.puzzle_publish_service import unpublish_join
                        from web.supabase_client import get_user_client
                        from web.auth_state import GlobalAuthState
                        if GlobalAuthState.is_logged_in():
                            client = get_user_client()
                            user_id = GlobalAuthState.get_user_id()
                            await run.io_bound(unpublish_join, client, user_id, doc_id)
                    except Exception:
                        pass  # Not published or not logged in — fine
                    await run.io_bound(svc.delete_document, doc_id)
                    if doc_state['current_doc_id'] == doc_id:
                        doc_state['current_doc_id'] = None
                        doc_state['is_published'] = False
                        details_container.style('display: none;')
                    dlg.close()
                    await refresh_docs_list()
                    ui.notify(tr('Deleted'), type='info')
                ui.button(tr('Delete'), on_click=do_delete).props('flat color=negative')
                ui.button(tr('Cancel'), on_click=dlg.close).props('flat')
        dlg.open()

    # ── Saved Joins dialog ──
    # A modal dialog for browsing, loading, and managing saved join documents.
    # Elements (title_input, notes_input, etc.) persist in the DOM even when
    # the dialog is closed, so auto-save can still read their values.
    joins_dialog = ui.dialog().props('maximized=false position=standard')
    with joins_dialog, ui.card().classes('w-96').style(
        'background: var(--bg-card); color: var(--text-primary); max-height: 80vh; overflow-y: auto;'
    ):
        with ui.row().classes('w-full items-center justify-between q-mb-sm'):
            ui.label(tr('Saved Joins')).classes('text-subtitle1 font-bold').style('color: var(--text-primary);')
            ui.button(icon='close', on_click=joins_dialog.close).props('flat dense round size=sm').style('color: var(--text-secondary);')
        docs_container = ui.column().classes('w-full gap-1')

        ui.separator().classes('q-my-sm')

        details_container = ui.column().classes('w-full').style('display: none;')
        with details_container:
            ui.label(tr('Details')).classes('text-subtitle2').style('color: var(--text-primary);')
            title_input = ui.input(label=tr('Title')).props('outlined dense').classes('w-full')
            notes_input = ui.textarea(label=tr('Notes')).props('outlined dense').classes('w-full').style('max-height: 80px;')
            fragments_label = ui.label('').classes('text-caption').style('color: var(--text-secondary); white-space: pre-wrap;')

            def on_title_edit():
                if doc_state['current_doc_id']:
                    schedule_auto_save()
            title_input.on('blur', on_title_edit)
            notes_input.on('blur', lambda: schedule_auto_save() if doc_state['current_doc_id'] else None)

    # ── Main container ──
    with ui.column().classes('puzzle-container w-full'):

        # ── Toolbar Row 1: Add fragments + actions ──
        with ui.row().classes('puzzle-toolbar items-center gap-1'):
            shelfmark_input = ui.input(
                placeholder=tr('Enter shelfmark...')
            ).props('dense outlined dark color=white').style(
                'width: 220px; --q-field-border-color: #666;'
            )

            async def on_add_shelfmark():
                """Resolve the shelfmark input to a sys_id and add that fragment to the canvas."""
                text = shelfmark_input.value
                if not text or not text.strip():
                    return
                text = text.strip()

                if not state.meta_mgr:
                    ui.notify(tr('Shelfmark not found'), type='warning')
                    return

                result = await run.io_bound(
                    state.meta_mgr.resolve_system_by_shelfmark, text
                )
                sys_id = result.get('sys_id') if result else None
                if not sys_id:
                    ui.notify(tr('Shelfmark not found'), type='warning')
                    return

                shelfmark = result.get('selected_shelfmark') or text
                shelfmark_input.value = ''
                await _add_fragment_by_sys_id(
                    sys_id, shelfmark, puzzle_meta, pending_fragment_meta, threshold_slider
                )

            shelfmark_input.on('keydown.enter', lambda: on_add_shelfmark())

            ui.button(icon='add', on_click=on_add_shelfmark).props(
                'dense flat dark color=primary round size=sm'
            ).tooltip(tr('Add fragment'))

            async def on_add_from_list():
                """Show dialog to pick a manuscript from personal lists."""
                from web.supabase_client import get_user_lists, get_list_items
                from web.auth_state import GlobalAuthState
                logged_in = GlobalAuthState.is_logged_in()
                user_id = GlobalAuthState.get_user_id()
                logger.info(f"Add from list: logged_in={logged_in}, user_id={user_id}")
                if not user_id:
                    ui.notify(tr('Please log in to access lists'), type='warning')
                    return

                lists = await run.io_bound(get_user_lists, user_id)
                if not lists:
                    ui.notify(tr('No lists found'), type='info')
                    return

                with ui.dialog() as dlg, ui.card().classes('w-96').style(
                    'background: #2d2d2d; color: #e0e0e0;'
                ):
                    ui.label(tr('Add from List')).classes('text-lg font-bold').style('color: #e0e0e0;')
                    list_select = ui.select(
                        {l['id']: l['name'] for l in lists},
                        label=tr('Select list')
                    ).props('dark outlined dense').classes('w-full')
                    items_container = ui.column().classes('w-full max-h-64 overflow-auto')

                    selected_items = []

                    async def on_list_selected(e):
                        items_container.clear()
                        selected_items.clear()
                        list_id = e.value if hasattr(e, 'value') else list_select.value
                        if not list_id:
                            return
                        items = await run.io_bound(get_list_items, int(list_id))
                        with items_container:
                            if not items:
                                ui.label(tr('Empty list')).style('color: #999;')
                            else:
                                for item in items:
                                    sid = item.get('sys_id', '')
                                    if not sid:
                                        continue
                                    shelf = item.get('shelfmark', '')
                                    # Resolve shelfmark from metadata if not stored in list
                                    if not shelf and state.meta_mgr:
                                        resolved, _ = state.meta_mgr.get_meta_for_id(sid)
                                        shelf = resolved or ''
                                    if not shelf:
                                        shelf = sid  # last resort fallback
                                    cb = ui.checkbox(shelf).props('dark dense').style(
                                        'color: #e0e0e0;'
                                    )
                                    selected_items.append({'sys_id': sid, 'shelfmark': shelf, 'cb': cb})

                    list_select.on('update:model-value', on_list_selected)

                    with ui.row().classes('w-full justify-end gap-2'):
                        async def add_selected():
                            to_add = [(s['sys_id'], s['shelfmark']) for s in selected_items if s['cb'].value]
                            dlg.close()
                            for sid, shelf in to_add:
                                await _add_fragment_by_sys_id(
                                    sid, shelf, puzzle_meta, pending_fragment_meta, threshold_slider
                                )

                        async def add_all():
                            to_add = [(s['sys_id'], s['shelfmark']) for s in selected_items]
                            dlg.close()
                            for sid, shelf in to_add:
                                await _add_fragment_by_sys_id(
                                    sid, shelf, puzzle_meta, pending_fragment_meta, threshold_slider
                                )

                        ui.button(tr('Add Selected'), on_click=add_selected).props('flat dark color=primary')
                        ui.button(tr('Add All'), on_click=add_all).props('flat dark color=positive')
                        ui.button(tr('Close'), on_click=dlg.close).props('flat dark')
                dlg.open()

            ui.button(icon='list', on_click=on_add_from_list).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Add from List'))

            async def on_add_from_joins():
                """Show joined fragments for selected fragment and add them."""
                from web.components.joins_panel import fetch_connected_fragments
                from shared.fjms_service import get_fjms_service

                # Get selected fragment's sys_id
                try:
                    key = await ui.run_javascript(
                        'window.puzzleCanvas && window.puzzleCanvas.canvas ? '
                        'window.puzzleCanvas.getSelectedKey() : null',
                        timeout=3.0
                    )
                except TimeoutError:
                    key = None

                if not key:
                    ui.notify(tr('Select a fragment first'), type='warning')
                    return

                sel_sys_id = key.split(',')[0] if key else ''
                if not sel_sys_id:
                    return

                # Resolve shelfmark from puzzle_meta or meta_mgr
                sel_shelfmark = ''
                if key in puzzle_meta:
                    sel_shelfmark = puzzle_meta[key].get('shelfmark', '')
                if not sel_shelfmark and state.meta_mgr:
                    sel_shelfmark, _ = state.meta_mgr.get_meta_for_id(sel_sys_id)
                    sel_shelfmark = sel_shelfmark or sel_sys_id

                # Fetch connected fragments — run in UI context (needs app.storage.user for auth)
                joins_data = fetch_connected_fragments(
                    shelfmark=sel_shelfmark,
                    document_id=sel_sys_id,
                    pgpid=None,
                    force_refresh=True
                )

                total = joins_data.get('total_joins', 0) if joins_data else 0
                logger.info(f"Joins for {sel_sys_id} ({sel_shelfmark}): {total} user/PGP joins")

                # Also fetch FJMS scientific joins
                fjms_joins = []
                fjms_svc = get_fjms_service()
                if fjms_svc and fjms_svc.is_available():
                    fjms_joins = await run.io_bound(fjms_svc.get_join_group, sel_sys_id)

                # Build unique fragment map: sys_id -> shelfmark
                frag_map = {}
                if joins_data:
                    # Use fragment_details which has document_id + shelfmark
                    for fd in joins_data.get('fragment_details', []):
                        doc_id = fd.get('document_id', '')
                        shelf = fd.get('shelfmark', '')
                        if doc_id and doc_id != sel_sys_id and doc_id not in frag_map:
                            frag_map[doc_id] = shelf or doc_id

                    # Fallback: resolve shelfmarks from fragments list
                    for frag_shelf in joins_data.get('fragments', []):
                        if isinstance(frag_shelf, str):
                            # Check if already in frag_map by shelfmark
                            if frag_shelf in [v for v in frag_map.values()]:
                                continue
                            # Resolve to sys_id
                            if state.meta_mgr:
                                from genizah_core import normalize_shelfmark
                                norm = normalize_shelfmark(frag_shelf)
                                sid = state.meta_mgr._shelf_to_sys.get(norm) if hasattr(state.meta_mgr, '_shelf_to_sys') else None
                                if sid and sid != sel_sys_id and sid not in frag_map:
                                    frag_map[sid] = frag_shelf

                # Add FJMS joins
                for fj in fjms_joins:
                    alma_id = fj.get('alma_id', '')
                    if alma_id and alma_id != sel_sys_id and alma_id not in frag_map:
                        shelf = ''
                        if state.meta_mgr:
                            shelf, _ = state.meta_mgr.get_meta_for_id(alma_id)
                        scholars = ', '.join(fj.get('scholar_names', []))
                        label = (shelf or alma_id)
                        if scholars:
                            label += f' ({scholars})'
                        frag_map[alma_id] = label

                if not frag_map:
                    ui.notify(tr('No joins found for {}').format(sel_shelfmark), type='info')
                    return

                # Show dialog with checkboxes
                with ui.dialog() as dlg, ui.card().classes('w-96').style(
                    'background: #2d2d2d; color: #e0e0e0;'
                ):
                    ui.label(tr('Joins for: {}').format(sel_shelfmark)).classes(
                        'text-lg font-bold'
                    ).style('color: #e0e0e0;')

                    items_list = []
                    with ui.column().classes('w-full max-h-72 overflow-auto'):
                        # Skip fragments already on canvas
                        canvas_sys_ids = set(k.split(',')[0] for k in puzzle_meta)
                        for doc_id, label in frag_map.items():
                            if doc_id in canvas_sys_ids:
                                continue
                            cb = ui.checkbox(label, value=True).props('dark dense').style(
                                'color: #e0e0e0;'
                            )
                            # Extract clean shelfmark (strip scholar attribution)
                            clean_shelf = label.split(' (')[0] if ' (' in label else label
                            items_list.append({'sys_id': doc_id, 'shelfmark': clean_shelf, 'cb': cb})

                    if not items_list:
                        ui.label(tr('All joined fragments already on canvas')).style('color: #999;')

                    with ui.row().classes('w-full justify-end gap-2'):
                        async def add_joins():
                            to_add = [(s['sys_id'], s['shelfmark']) for s in items_list if s['cb'].value]
                            dlg.close()
                            for sid, shelf in to_add:
                                await _add_fragment_by_sys_id(
                                    sid, shelf, puzzle_meta, pending_fragment_meta, threshold_slider
                                )

                        ui.button(tr('Add Selected'), on_click=add_joins).props('flat dark color=primary')
                        ui.button(tr('Close'), on_click=dlg.close).props('flat dark')
                dlg.open()

            ui.button(icon='link', on_click=on_add_from_joins).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Add from Known Joins'))

            async def on_delete_selected():
                """Remove selected fragment(s) via JS and update session storage."""
                try:
                    removed = await ui.run_javascript(
                        'window.puzzleCanvas.removeSelected()',
                        timeout=3.0
                    )
                except TimeoutError:
                    return
                if removed and isinstance(removed, list):
                    for key in removed:
                        puzzle_meta.pop(key, None)
                    app.storage.tab['puzzle_fragments'] = puzzle_meta

            ui.separator().props('vertical').style('height: 20px')

            ui.button(icon='delete', on_click=on_delete_selected).props(
                'dense flat dark round size=sm color=negative'
            ).tooltip(tr('Remove Selected'))

            ui.separator().props('vertical').style('height: 20px')

            ui.button(tr('Flip'), icon='swap_horiz', on_click=lambda: ui.run_javascript(
                'window.puzzleCanvas.flipRectoVerso()'
            )).props('dense flat dark size=sm').tooltip(tr('Show other side (recto/verso)'))
            ui.button(tr('Flip Puzzle'), icon='sync_alt', on_click=lambda: ui.run_javascript(
                'window.puzzleCanvas.flipAllPuzzle()'
            )).props('dense flat dark size=sm').tooltip(tr('Flip all + mirror positions'))

            ui.separator().props('vertical').style('height: 20px')

            # Crop controls
            crop_btn = ui.button(tr('Crop'), icon='crop', on_click=lambda: _toggle_crop()).props(
                'dense flat dark size=sm'
            ).tooltip(tr('Crop edges of selected fragment'))
            crop_ok_btn = ui.button(icon='check', on_click=lambda: _crop_confirm()).props(
                'dense flat dark round size=sm color=positive'
            ).tooltip(tr('Apply crop'))
            crop_revert_btn = ui.button(icon='undo', on_click=lambda: _crop_revert()).props(
                'dense flat dark round size=sm color=warning'
            ).tooltip(tr('Revert crop'))
            crop_ok_btn.set_visibility(False)
            crop_revert_btn.set_visibility(False)

            def _toggle_crop():
                ui.run_javascript('''
                    var active = window.puzzleCanvas._cropMode;
                    if (active) {
                        window.puzzleCanvas.cropRevert();
                    } else {
                        window.puzzleCanvas.toggleCropMode(true);
                    }
                ''')
                is_cropping = not crop_ok_btn.visible
                crop_ok_btn.set_visibility(is_cropping)
                crop_revert_btn.set_visibility(is_cropping)

            def _crop_confirm():
                ui.run_javascript('window.puzzleCanvas.cropConfirm()')
                crop_ok_btn.set_visibility(False)
                crop_revert_btn.set_visibility(False)

            def _crop_revert():
                ui.run_javascript('window.puzzleCanvas.cropRevert()')
                crop_ok_btn.set_visibility(False)
                crop_revert_btn.set_visibility(False)

            ui.separator().props('vertical').style('height: 20px')

            ui.button(icon='flip_to_front', on_click=lambda: ui.run_javascript(
                'window.puzzleCanvas.bringForward()'
            )).props('dense flat dark round size=sm').tooltip(tr('Bring Forward'))
            ui.button(icon='flip_to_back', on_click=lambda: ui.run_javascript(
                'window.puzzleCanvas.sendBackward()'
            )).props('dense flat dark round size=sm').tooltip(tr('Send Backward'))

            ui.separator().props('vertical').style('height: 20px')

            ui.button(icon='palette', on_click=lambda: ui.run_javascript(
                'window.puzzleCanvas.cycleBgMode()'
            )).props('dense flat dark round size=sm').tooltip(tr('Cycle background'))
            ui.button(icon='fit_screen', on_click=lambda: ui.run_javascript(
                'window.puzzleCanvas.fitAll()'
            )).props('dense flat dark round size=sm').tooltip(tr('Fit All'))

            ui.separator().props('vertical').style('height: 20px')

            # Folio navigation (CANV-07)
            folio_label_display = ui.label('').classes('text-grey-3 text-caption').style(
                'min-width: 24px; text-align: center;'
            )

            async def on_folio_prev():
                """Navigate the selected fragment to the previous folio page."""
                try:
                    key = await ui.run_javascript(
                        'window.puzzleCanvas && window.puzzleCanvas.canvas ? window.puzzleCanvas.getSelectedKey() : null',
                        timeout=3.0
                    )
                except TimeoutError:
                    return
                if key:
                    try:
                        label = await ui.run_javascript(
                            f'window.puzzleCanvas.navigateFolio("{key}", -1)',
                            timeout=15.0
                        )
                    except TimeoutError:
                        return
                    folio_label_display.text = label or ''
                    if key in puzzle_meta:
                        try:
                            new_fl_id = await ui.run_javascript(
                                f'window.puzzleCanvas.fragments["{key}"] && '
                                f'window.puzzleCanvas.fragments["{key}"]._fragmentMeta ? '
                                f'window.puzzleCanvas.fragments["{key}"]._fragmentMeta.fl_id : ""',
                                timeout=3.0
                            )
                        except TimeoutError:
                            new_fl_id = None
                        if new_fl_id:
                            puzzle_meta[key]['fl_id'] = new_fl_id
                            puzzle_meta[key]['folio_label'] = label or ''
                            app.storage.tab['puzzle_fragments'] = puzzle_meta

            async def on_folio_next():
                """Navigate the selected fragment to the next folio page."""
                try:
                    key = await ui.run_javascript(
                        'window.puzzleCanvas && window.puzzleCanvas.canvas ? window.puzzleCanvas.getSelectedKey() : null',
                        timeout=3.0
                    )
                except TimeoutError:
                    return
                if key:
                    try:
                        label = await ui.run_javascript(
                            f'window.puzzleCanvas.navigateFolio("{key}", 1)',
                            timeout=15.0
                        )
                    except TimeoutError:
                        return
                    folio_label_display.text = label or ''
                    # Update stored fl_id
                    if key in puzzle_meta:
                        try:
                            new_fl_id = await ui.run_javascript(
                                f'window.puzzleCanvas.fragments["{key}"] && '
                                f'window.puzzleCanvas.fragments["{key}"]._fragmentMeta ? '
                                f'window.puzzleCanvas.fragments["{key}"]._fragmentMeta.fl_id : ""',
                                timeout=3.0
                            )
                        except TimeoutError:
                            new_fl_id = None
                        if new_fl_id:
                            puzzle_meta[key]['fl_id'] = new_fl_id
                            puzzle_meta[key]['folio_label'] = label or ''
                            app.storage.tab['puzzle_fragments'] = puzzle_meta

            ui.button('<', on_click=on_folio_prev).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Previous Folio')).style('min-width: 28px; font-weight: bold;')
            ui.button('>', on_click=on_folio_next).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Next Folio')).style('min-width: 28px; font-weight: bold;')

            ui.separator().props('vertical').style('height: 20px')

            # Fragment selector combobox + Browse button
            fragment_select = ui.select(
                options={}, value=None
            ).props('dense dark outlined options-dense').style(
                'min-width: 180px; max-width: 260px; --q-field-border-color: #666;'
            ).tooltip(tr('Select fragment'))

            def _refresh_fragment_select():
                """Rebuild fragment selector options from puzzle_meta."""
                opts = {}
                for key, meta in puzzle_meta.items():
                    sm = meta.get('shelfmark', key.split(',')[0])
                    folio = meta.get('folio_label', '')
                    label = f"{sm} / {folio}" if folio else sm
                    opts[key] = label
                fragment_select.options = opts
                fragment_select.update()

            async def _on_fragment_select_change(e):
                """Select the fragment on the canvas when combo changes."""
                key = e.value if hasattr(e, 'value') else fragment_select.value
                if key and key in puzzle_meta:
                    try:
                        await ui.run_javascript(
                            f'(function() {{'
                            f'  var c = window.puzzleCanvas;'
                            f'  if (!c || !c.canvas) return;'
                            f'  var obj = c.fragments["{key}"];'
                            f'  if (obj) {{ c.canvas.setActiveObject(obj); c.canvas.requestRenderAll(); }}'
                            f'}})()',
                            timeout=2.0
                        )
                    except TimeoutError:
                        pass

            fragment_select.on('update:model-value', _on_fragment_select_change)

            def _browse_selected_fragment():
                """Open browse page for the selected fragment in a new tab."""
                key = fragment_select.value
                if not key or key not in puzzle_meta:
                    ui.notify(tr('Select a fragment first'), type='warning')
                    return
                sys_id = puzzle_meta[key].get('sys_id', '')
                if sys_id:
                    ui.run_javascript(f'window.open("/browse?sys_id={sys_id}", "_blank")')

            ui.button(icon='open_in_new', on_click=_browse_selected_fragment).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Browse this fragment'))

            ui.separator().props('vertical').style('height: 20px')

            ui.button(icon='save', on_click=save_join).props(
                'dense flat dark color=primary round size=sm'
            ).tooltip(tr('Save Puzzle'))

            ui.button(icon='note_add', on_click=new_puzzle).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('New Puzzle'))

            ui.button(icon='image', on_click=export_png).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Export PNG'))

            publish_btn = ui.button(icon='publish', on_click=toggle_publish).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Publish to Community'))

            async def _open_joins_dialog():
                await refresh_docs_list()
                joins_dialog.open()
            ui.button(icon='folder_open', on_click=_open_joins_dialog).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Saved Joins'))

        # ── Toolbar Row 2: Sliders (compact) ──
        with ui.row().classes('puzzle-toolbar items-center gap-1'):
            ui.icon('zoom_in', size='xs').classes('text-grey-5')
            scale_slider = ui.slider(
                min=10, max=400, value=100, step=1
            ).props('dense dark').style('width: 120px')

            def on_scale_change(e):
                """Apply scale slider value to the selected fragment via JS."""
                if control_sync['active']:
                    return
                val = e.value if hasattr(e, 'value') else scale_slider.value
                ui.run_javascript(
                    f'window.puzzleCanvas.setSelectedScale({val / 100})'
                )
            scale_slider.on('update:model-value', on_scale_change)

            ui.separator().props('vertical').style('height: 20px')

            ui.icon('rotate_right', size='xs').classes('text-grey-5')
            rotation_slider = ui.slider(
                min=-180, max=180, value=0, step=1
            ).props('dense dark').style('width: 120px')

            def on_rotation_change(e):
                """Apply rotation slider value to the selected fragment via JS."""
                if control_sync['active']:
                    return
                val = e.value if hasattr(e, 'value') else rotation_slider.value
                ui.run_javascript(
                    f'window.puzzleCanvas.setSelectedRotation({val})'
                )
            rotation_slider.on('update:model-value', on_rotation_change)

            ui.separator().props('vertical').style('height: 20px')

            ui.icon('tune', size='xs').classes('text-grey-5')
            threshold_slider = ui.slider(
                min=10, max=150, value=30, step=1
            ).props('dense dark').style('width: 100px')

            async def on_threshold_change():
                """Re-fetch selected fragment image at new threshold."""
                if control_sync['active']:
                    return
                try:
                    key = await ui.run_javascript(
                        'window.puzzleCanvas && window.puzzleCanvas.canvas ? '
                        'window.puzzleCanvas.getSelectedKey() : null',
                        timeout=3.0
                    )
                except TimeoutError:
                    return
                if not key or key not in puzzle_meta:
                    return
                meta = puzzle_meta[key]
                new_threshold = threshold_slider.value
                fl_id = meta.get('fl_id', '')
                if not fl_id:
                    return
                processed = meta.get('processed', True)
                # Invalidate cache for old threshold
                await run.io_bound(
                    _invalidate_and_refetch, fl_id, new_threshold
                )
                meta_is_cul = meta.get('is_cul', False)
                url = f"/api/puzzle_image?fl_id={fl_id}&threshold={new_threshold}&size=800&processed={str(bool(processed)).lower()}&is_cul={str(bool(meta_is_cul)).lower()}"
                js_meta = json.dumps({
                    'fl_id': fl_id, 'threshold': new_threshold,
                    'size': 800, 'processed': processed,
                    'sys_id': meta.get('sys_id', ''),
                    'is_cul': meta_is_cul
                })
                ui.run_javascript(
                    f'window.puzzleCanvas._reloadFragment("{key}", "{url}", {js_meta})'
                )
                # Update Python storage
                meta['threshold'] = new_threshold
                app.storage.tab['puzzle_fragments'] = puzzle_meta

            threshold_slider.on('change', lambda: on_threshold_change())

        # ── Canvas area ──
        with ui.element('div').classes('puzzle-canvas-wrap') as canvas_wrap:
            ui.element('canvas').props('id=puzzleCanvas').style('width: 100%; height: 100%; display: block;')

        # Listen for JS delete events (keyboard, context menu, toolbar all dispatch this)
        def _sync_control_values(*, processed=None, threshold=None, scale=None, rotation=None, folio_label=None):
            """Update toolbar slider/label values from JS selection state without triggering change handlers."""
            control_sync['active'] = True
            try:
                if threshold is not None:
                    threshold_slider.value = int(round(threshold))
                    threshold_slider.update()
                if scale is not None:
                    scale_slider.value = int(round(scale * 100))
                    scale_slider.update()
                if rotation is not None:
                    rotation_slider.value = int(round(rotation))
                    rotation_slider.update()
                if folio_label is not None:
                    folio_label_display.text = folio_label
                    folio_label_display.update()
            finally:
                control_sync['active'] = False

        def _prune_saved_state(removed_keys):
            """Remove deleted fragment keys from the saved spatial state in session storage."""
            try:
                saved_state = app.storage.tab.get('puzzle_state')
            except RuntimeError:
                return
            if not saved_state:
                return
            try:
                state_data = json.loads(saved_state) if isinstance(saved_state, str) else saved_state
            except (json.JSONDecodeError, TypeError):
                return
            if not isinstance(state_data, dict):
                return
            changed = False
            for key in removed_keys:
                if key in state_data:
                    state_data.pop(key, None)
                    changed = True
            if changed:
                app.storage.tab['puzzle_state'] = json.dumps(state_data)

        def on_puzzle_delete(e):
            """Handle 'puzzle-delete' event: remove deleted keys from puzzle_meta and session."""
            try:
                removed = _parse_puzzle_event_args(e.args)
                if isinstance(removed, list):
                    for key in removed:
                        puzzle_meta.pop(key, None)
                        pending_fragment_meta.pop(key, None)
                    app.storage.tab['puzzle_fragments'] = puzzle_meta
                    _prune_saved_state(removed)
                    _refresh_fragment_select()
                    schedule_auto_save()
            except Exception:
                pass
        canvas_wrap.on('puzzle-delete', on_puzzle_delete)

        def on_puzzle_object_modified(e):
            """Handle Fabric.js object:modified -- trigger auto-save for saved documents."""
            schedule_auto_save()
        canvas_wrap.on('puzzle-object-modified', on_puzzle_object_modified)

        async def on_puzzle_add_result(e):
            """Handle 'puzzle-add-result' event: promote pending fragment to puzzle_meta on success."""
            payload = _parse_puzzle_event_args(e.args)
            if not isinstance(payload, dict):
                return
            key = payload.get('key')
            if not key:
                return
            pending = pending_fragment_meta.pop(key, None)
            if not payload.get('success'):
                if pending:
                    ui.notify(f'Failed to load {pending.get("shelfmark", key)}', type='negative')
                # Still decrement loading counter on failure
                if doc_state.get('loading'):
                    doc_state['load_pending'] = doc_state.get('load_pending', 1) - 1
                    if doc_state['load_pending'] <= 0:
                        doc_state['loading'] = False
                        doc_state['load_pending'] = 0
                return
            if not pending:
                # Fragment loaded but not from pending_fragment_meta (e.g. from load_document)
                # Still need to handle crop restore and loading counter
                pass
            else:
                js_meta = payload.get('meta') or {}
                if isinstance(js_meta, dict):
                    pending['fl_id'] = js_meta.get('fl_id', pending.get('fl_id', ''))
                    pending['threshold'] = js_meta.get('threshold', pending.get('threshold', 30))
                    pending['processed'] = js_meta.get('processed', pending.get('processed', True))
                    pending['size'] = js_meta.get('size', pending.get('size', 800))
                puzzle_meta[key] = pending
                app.storage.tab['puzzle_fragments'] = puzzle_meta
                ui.notify(f'{pending.get("shelfmark", key)} ({pending.get("folio_label", "")})', type='positive')

            # Apply pending crop state for this fragment (from load_document).
            # Reliable because on_puzzle_add_result fires AFTER the image is fully loaded.
            pending_crop = doc_state.get('pending_crops', {}).get(key)
            if pending_crop:
                try:
                    await ui.run_javascript(f'''
                        var obj = window.puzzleCanvas.fragments["{key}"];
                        if (obj) {{
                            obj._originalWidth = obj._originalWidth || obj.width;
                            obj._originalHeight = obj._originalHeight || obj.height;
                            var cropX = {pending_crop['cropX']};
                            var cropY = {pending_crop['cropY']};
                            var cropRight = {pending_crop['crop_right']};
                            var cropBottom = {pending_crop['crop_bottom']};
                            obj.set({{
                                cropX: cropX,
                                cropY: cropY,
                                width: obj._originalWidth - cropX - cropRight,
                                height: obj._originalHeight - cropY - cropBottom
                            }});
                            if (typeof obj.setPositionByOrigin === 'function') {{
                                var centerX = {pending_crop['x']} + obj.width / 2;
                                var centerY = {pending_crop['y']} + obj.height / 2;
                                var pt = (typeof fabric.Point === 'function')
                                    ? new fabric.Point(centerX, centerY)
                                    : {{ x: centerX, y: centerY }};
                                obj.setPositionByOrigin(pt, 'center', 'center');
                            }}
                            obj.setCoords();
                            window.puzzleCanvas.canvas.requestRenderAll();
                        }}
                    ''')
                except Exception:
                    pass
                doc_state['pending_crops'].pop(key, None)

            # Decrement loading counter and clear guard when all fragments loaded.
            if doc_state.get('loading'):
                doc_state['load_pending'] = doc_state.get('load_pending', 1) - 1
                if doc_state['load_pending'] <= 0:
                    doc_state['loading'] = False
                    doc_state['load_pending'] = 0
                    try:
                        await ui.run_javascript('window.puzzleCanvas && window.puzzleCanvas.fitAll()')
                    except Exception:
                        pass

            # Refresh fragment selector and trigger auto-save
            _refresh_fragment_select()
            schedule_auto_save()
        canvas_wrap.on('puzzle-add-result', on_puzzle_add_result)

        def on_puzzle_fragment_meta(e):
            """Handle 'puzzle-fragment-meta' event: update stored metadata after folio nav or bg toggle."""
            payload = _parse_puzzle_event_args(e.args)
            if not isinstance(payload, dict):
                return
            key = payload.get('key')
            meta = payload.get('meta')
            if not key or key not in puzzle_meta or not isinstance(meta, dict):
                return
            puzzle_meta[key]['fl_id'] = meta.get('fl_id', puzzle_meta[key].get('fl_id', ''))
            puzzle_meta[key]['threshold'] = meta.get('threshold', puzzle_meta[key].get('threshold', 30))
            puzzle_meta[key]['processed'] = meta.get('processed', puzzle_meta[key].get('processed', True))
            puzzle_meta[key]['size'] = meta.get('size', puzzle_meta[key].get('size', 800))
            if meta.get('folio_label'):
                puzzle_meta[key]['folio_label'] = meta.get('folio_label')
            app.storage.tab['puzzle_fragments'] = puzzle_meta
            schedule_auto_save()
        canvas_wrap.on('puzzle-fragment-meta', on_puzzle_fragment_meta)

        def on_puzzle_selection(e):
            """Handle 'puzzle-selection' event: sync toolbar controls with selected fragment state."""
            payload = _parse_puzzle_event_args(e.args)
            if not isinstance(payload, dict):
                return
            if not payload.get('hasSelection'):
                _sync_control_values(folio_label='')
                try:
                    fragment_select.value = None
                    fragment_select.update()
                except NameError:
                    pass
                return
            _sync_control_values(
                processed=payload.get('processed', True),
                threshold=payload.get('threshold', 30),
                scale=payload.get('scale', 1),
                rotation=payload.get('rotation', 0),
                folio_label=payload.get('folioLabel', '')
            )
            # Sync fragment selector combobox
            sel_key = payload.get('key')
            if sel_key:
                try:
                    fragment_select.set_value(sel_key)
                except (NameError, Exception):
                    pass
        canvas_wrap.on('puzzle-selection', on_puzzle_selection)

    # ── Initialize canvas after DOM is ready ──
    async def init_canvas():
        """Initialize the Fabric.js canvas and restore saved session state.

        Waits for Fabric.js CDN to load (retries up to 10 seconds), then
        initializes the canvas. Restores previously saved fragment metadata
        and spatial state from app.storage.tab, re-adding each fragment
        at its saved position/rotation/scale.
        """
        # Fire-and-forget: JS will retry until Fabric.js CDN loads
        ui.run_javascript('''
            (function tryInit(attempts) {
                if (typeof fabric !== "undefined") {
                    console.log("Fabric.js loaded:", fabric.version);
                    window.puzzleCanvas.init("puzzleCanvas");
                    if (window.puzzleCanvas.canvas) {
                        console.log("Canvas size:", window.puzzleCanvas.canvas.getWidth(), "x", window.puzzleCanvas.canvas.getHeight());
                    } else {
                        console.error("Canvas init failed — canvas element missing?");
                    }
                } else if (attempts < 50) {
                    setTimeout(function() { tryInit(attempts + 1); }, 200);
                } else {
                    console.error("Fabric.js failed to load after 10s");
                }
            })(0);
        ''')

        # Restore saved fragments (client should be connected via timer delay)
        try:
            saved_meta = app.storage.tab.get('puzzle_fragments', {})
            saved_state = app.storage.tab.get('puzzle_state')
        except RuntimeError:
            saved_meta = {}
            saved_state = None
        # Populate outer puzzle_meta so callbacks can find existing fragments
        if saved_meta and isinstance(saved_meta, dict):
            puzzle_meta.update(saved_meta)
            doc_state['loading'] = True
            doc_state['load_pending'] = len(saved_meta)
        else:
            doc_state['loading'] = False
            doc_state['load_pending'] = 0

        if saved_meta and isinstance(saved_meta, dict):
            for key, meta in saved_meta.items():
                fl_id = meta.get('fl_id', '')
                threshold = meta.get('threshold', 30)
                processed = meta.get('processed', True)
                if not fl_id:
                    continue

                meta_is_cul = meta.get('is_cul', False)
                url = f"/api/puzzle_image?fl_id={fl_id}&threshold={threshold}&size=800&processed={str(bool(processed)).lower()}&is_cul={str(bool(meta_is_cul)).lower()}"

                # Default positions if no saved state
                x, y = 100, 100
                rotation, scaleX, scaleY = 0, 1.0, 1.0
                flipH, flipV = False, False

                if saved_state:
                    try:
                        state_data = json.loads(saved_state) if isinstance(saved_state, str) else saved_state
                        if key in state_data:
                            s = state_data[key]
                            x = s.get('x', 100)
                            y = s.get('y', 100)
                            rotation = s.get('rotation', 0)
                            scaleX = s.get('scaleX', 1.0)
                            scaleY = s.get('scaleY', 1.0)
                            flipH = s.get('flipH', False)
                            flipV = s.get('flipV', False)
                            if s.get('centerX') is not None and s.get('centerY') is not None:
                                x = s.get('centerX', x)
                                y = s.get('centerY', y)
                                center_origin = True
                            else:
                                center_origin = False
                        else:
                            center_origin = False
                    except (json.JSONDecodeError, TypeError):
                        center_origin = False
                else:
                    center_origin = False

                sys_id = meta.get('sys_id', '')
                js_meta = json.dumps({
                    'fl_id': fl_id, 'threshold': threshold,
                    'size': 800, 'processed': processed,
                    'sys_id': sys_id
                })
                ui.run_javascript(
                    f'window.puzzleCanvas.addFragment("{key}", "{url}", '
                    f'{x}, {y}, {rotation}, {scaleX}, '
                    f'{"true" if flipH else "false"}, {"true" if flipV else "false"}, {js_meta}, false, '
                    f'{"true" if center_origin else "false"})'
                )

        # Saved documents list is refreshed on-demand when dialog opens

    ui.timer(0.5, init_canvas, once=True)

    # ── Handle initial_add query parameter ──
    if initial_add:
        parts = initial_add.split(',', 1)
        add_sys_id = parts[0].strip()
        add_fl_id = parts[1].strip() if len(parts) > 1 else ''

        async def auto_add():
            """Auto-add a fragment from the initial_add query parameter after canvas init."""
            import asyncio
            await asyncio.sleep(1.0)

            fl_id = add_fl_id
            folio_label = '1r'

            if not fl_id:
                folios = await run.io_bound(_resolve_folios, add_sys_id)
                if folios:
                    fl_id = folios[0].get('fl_id', '')
                    folio_label = folios[0].get('label', '1r')

            if not fl_id:
                ui.notify(tr('No images found for this manuscript'), type='warning')
                return

            key = f"{add_sys_id},{folio_label}"
            # CUL/T-S threshold matching desktop defaults
            threshold = 30.0
            is_cul = False
            if state.meta_mgr:
                lib_code = state.meta_mgr.get_library_for_id(add_sys_id) or ''
                if lib_code == 'CUL':
                    is_cul = True
            if not is_cul:
                sm = ''
                if state.meta_mgr:
                    sm, _ = state.meta_mgr.get_meta_for_id(add_sys_id)
                if sm:
                    s = sm.upper()
                    if s.startswith(('T-S', 'OR.', 'ADD.')):
                        is_cul = True
            if is_cul:
                threshold = 150.0

            url = f"/api/puzzle_image?fl_id={fl_id}&threshold={threshold}&size=800&processed=true&is_cul={'true' if is_cul else 'false'}"

            frag_offset = len(puzzle_meta) * 50
            js_meta = json.dumps({
                'fl_id': fl_id, 'threshold': threshold,
                'size': 800, 'processed': True,
                'sys_id': add_sys_id, 'is_cul': is_cul
            })
            ui.run_javascript(
                f'window.puzzleCanvas.addFragment("{key}", "{url}", '
                f'{100 + frag_offset}, {100 + frag_offset}, 0, 1.0, false, false, {js_meta})'
            )

            shelfmark = ''
            if state.meta_mgr:
                sm, _ = state.meta_mgr.get_meta_for_id(add_sys_id)
                shelfmark = sm or ''

            pending_fragment_meta[key] = {
                'sys_id': add_sys_id,
                'shelfmark': shelfmark,
                'folio_label': folio_label,
                'fl_id': fl_id,
                'threshold': threshold,
                'processed': True,
                'size': 800,
            }

        ui.timer(1.5, auto_add, once=True)

    # ── Handle initial_doc query parameter (load saved document by ID) ──
    if initial_doc:
        async def auto_load_doc():
            """Auto-load a saved document from the initial_doc query parameter."""
            import asyncio
            await asyncio.sleep(1.0)
            await load_document(initial_doc)

        ui.timer(1.5, auto_load_doc, once=True)

    # ── Periodic state save ──
    async def save_state():
        """Periodically save canvas spatial state to session storage (every 30s)."""
        try:
            state_json = await ui.run_javascript(
                'window.puzzleCanvas && window.puzzleCanvas.canvas ? window.puzzleCanvas.getState() : null',
                timeout=5.0
            )
            if state_json:
                app.storage.tab['puzzle_state'] = state_json
        except Exception:
            pass  # Page may not be active or JS not ready

    ui.timer(30, save_state)


def _get_port():
    """Get the app port for internal API calls."""
    import os
    return int(os.environ.get('GENIZAH_PORT', 8081))
