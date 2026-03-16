# -*- coding: utf-8 -*-
"""
Fragment Puzzle page for GenizahSearch web application.

Provides a Fabric.js canvas for assembling and manipulating fragment images.
Users can add fragments by shelfmark, drag/rotate/flip/resize them, and
navigate folios. Session state persists across page navigations via
app.storage.tab.
"""

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
window.puzzleCanvas = {
    canvas: null,
    fragments: {},
    bgModeIndex: 0,
    BG_MODES: ['#333333', '#000000', '#FFFFFF', 'checker', '#F5F0E0', 'grid'],
    contextMenu: null,
    _panning: false,
    _lastPosX: 0,
    _lastPosY: 0,

    init: function(canvasId) {
        var el = document.getElementById(canvasId);
        if (!el) { console.error('Canvas element not found:', canvasId); return; }

        var container = el.parentElement;
        var w = container.clientWidth || 1200;
        var h = container.clientHeight || 800;

        this.canvas = new fabric.Canvas(canvasId, {
            backgroundColor: '#333333',
            selection: true,
            preserveObjectStacking: true,
            stopContextMenu: true,
            fireRightClick: true,
            width: w,
            height: h
        });

        // Enable Ctrl+click multi-select (Shift+click is default)
        this.canvas.altSelectionKey = 'ctrlKey';

        this.setupWheelZoom();
        this.setupPan();
        this.setupKeyboard();
        this.setupContextMenu();
        this.setupSnapGuides();
        this.setupSelectionSync();

        // Handle window resize
        var self = this;
        window.addEventListener('resize', function() {
            if (!self.canvas) return;
            var c = document.getElementById(canvasId);
            if (!c) return;
            var p = c.parentElement;
            self.canvas.setWidth(p.clientWidth);
            self.canvas.setHeight(p.clientHeight);
            self.canvas.requestRenderAll();
        });

        console.log('Puzzle canvas initialized:', w, 'x', h);
    },

    addFragment: function(key, imageUrl, x, y, rotation, scale, flipH, flipV, meta) {
        if (!this.canvas) return;
        var self = this;

        // Remove existing fragment with same key
        if (this.fragments[key]) {
            this.canvas.remove(this.fragments[key]);
            delete this.fragments[key];
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
        htmlImg.onload = function() {
            console.log('Image loaded:', key, htmlImg.naturalWidth, 'x', htmlImg.naturalHeight);
            self.canvas.getObjects().forEach(function(obj) {
                if (obj._isPlaceholder && obj._placeholderKey === key) {
                    self.canvas.remove(obj);
                }
            });

            var img = new fabric.Image(htmlImg, {
                left: x, top: y,
                angle: rotation || 0,
                scaleX: scale || 1.0,
                scaleY: scale || 1.0,
                flipX: !!flipH,
                flipY: !!flipV,
                hasControls: true,
                hasBorders: true,
                cornerSize: 12,
                transparentCorners: false,
                perPixelTargetFind: true,
                _fragmentKey: key,
                _imageUrl: imageUrl,
                _fragmentMeta: meta || null
            });

            self.canvas.add(img);
            self.fragments[key] = img;
            self.canvas.setActiveObject(img);
            self.canvas.requestRenderAll();

            // Auto-load folios if meta has sys_id
            if (meta && meta.sys_id) {
                self.loadFolios(key, meta.sys_id);
            }
        };
        htmlImg.onerror = function(err) {
            console.error('Failed to load image for', key, imageUrl, err);
            self.canvas.getObjects().forEach(function(obj) {
                if (obj._isPlaceholder && obj._placeholderKey === key) {
                    self.canvas.remove(obj);
                }
            });
            var errText = new fabric.Text('Image load failed: ' + key, {
                left: x, top: y,
                fontSize: 12, fill: '#ff6666',
                selectable: false, evented: false
            });
            self.canvas.add(errText);
            self.canvas.requestRenderAll();
        };
        htmlImg.src = imageUrl;
    },

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
        // Return removed keys so Python can update storage
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
        document.addEventListener('keydown', function(e) {
            if (!self.canvas) return;
            // Don't capture when typing in inputs
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

            var active = self.canvas.getActiveObject();

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
        });
    },

    setupContextMenu: function() {
        var self = this;

        // Prevent browser context menu on canvas
        var upperCanvas = this.canvas.upperCanvasEl || this.canvas.wrapperEl;
        if (upperCanvas) {
            upperCanvas.addEventListener('contextmenu', function(e) { e.preventDefault(); });
        }

        this.canvas.on('mouse:down', function(opt) {
            if (opt.button === 3 && opt.target) {
                self._showContextMenu(opt.e, opt.target);
            } else {
                self._hideContextMenu();
            }
        });

        // Hide on click anywhere
        document.addEventListener('click', function() { self._hideContextMenu(); });
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') self._hideContextMenu();
        });
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
                for (var key in this.fragments) {
                    if (this.fragments[key] === target) { delete this.fragments[key]; break; }
                }
                this.canvas.remove(target);
                this.canvas.discardActiveObject();
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

    getSelectedKey: function() {
        var active = this.canvas.getActiveObject();
        if (!active) return null;
        return active._fragmentKey || null;
    },

    // === Folio Navigation (CANV-07) ===

    folioData: {},  // key -> {sys_id, folios: [{fl_id, label}], currentIndex}

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

    navigateFolio: async function(key, direction) {
        var data = this.folioData[key];
        if (!data || !data.folios.length) return '';
        var newIndex = Math.max(0, Math.min(data.folios.length - 1, data.currentIndex + direction));
        if (newIndex === data.currentIndex) return data.folios[data.currentIndex].label || '';
        data.currentIndex = newIndex;
        var folio = data.folios[newIndex];
        var obj = this.fragments[key];
        if (!obj) return folio.label;

        var pos = { left: obj.left, top: obj.top, angle: obj.angle, scaleX: obj.scaleX, scaleY: obj.scaleY, flipX: obj.flipX, flipY: obj.flipY };
        var self = this;
        var meta = obj._fragmentMeta || {};
        var threshold = meta.threshold || 30;
        var url = '/api/puzzle_image?fl_id=' + folio.fl_id + '&threshold=' + threshold + '&size=800';

        return new Promise(function(resolve) {
            var htmlImg = new Image();
            htmlImg.crossOrigin = 'anonymous';
            htmlImg.onload = function() {
                self.canvas.remove(obj);
                var img = new fabric.Image(htmlImg, pos);
                img.set({
                    hasControls: true, hasBorders: true,
                    cornerSize: 12, transparentCorners: false,
                    perPixelTargetFind: true, _fragmentKey: key,
                    _imageUrl: url
                });
                img._fragmentMeta = Object.assign({}, meta, { fl_id: folio.fl_id });
                self.canvas.add(img);
                self.fragments[key] = img;
                self.canvas.setActiveObject(img);
                self.canvas.requestRenderAll();
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

    setupSnapGuides: function() {
        var SNAP_THRESHOLD = 8;
        var self = this;
        var guidelines = [];

        this.canvas.on('object:moving', function(e) {
            // Remove old guidelines
            guidelines.forEach(function(g) { self.canvas.remove(g); });
            guidelines = [];

            var moving = e.target;
            if (!moving) return;
            var movingBR = moving.getBoundingRect();

            self.canvas.getObjects().forEach(function(obj) {
                if (obj === moving || obj._isGuideline || obj._isPlaceholder) return;
                var br = obj.getBoundingRect();

                // Left edge alignment
                if (Math.abs(movingBR.left - br.left) < SNAP_THRESHOLD) {
                    moving.set('left', br.left + (moving.left - movingBR.left));
                    var line = new fabric.Line([br.left, 0, br.left, self.canvas.height / self.canvas.getZoom()],
                        { stroke: '#00FFFF', strokeDashArray: [4, 4], selectable: false, evented: false, _isGuideline: true });
                    guidelines.push(line);
                    self.canvas.add(line);
                }
                // Right edge alignment
                var movingRight = movingBR.left + movingBR.width;
                var objRight = br.left + br.width;
                if (Math.abs(movingRight - objRight) < SNAP_THRESHOLD) {
                    moving.set('left', objRight - movingBR.width + (moving.left - movingBR.left));
                    var line = new fabric.Line([objRight, 0, objRight, self.canvas.height / self.canvas.getZoom()],
                        { stroke: '#00FFFF', strokeDashArray: [4, 4], selectable: false, evented: false, _isGuideline: true });
                    guidelines.push(line);
                    self.canvas.add(line);
                }
                // Top edge alignment
                if (Math.abs(movingBR.top - br.top) < SNAP_THRESHOLD) {
                    moving.set('top', br.top + (moving.top - movingBR.top));
                    var line = new fabric.Line([0, br.top, self.canvas.width / self.canvas.getZoom(), br.top],
                        { stroke: '#00FFFF', strokeDashArray: [4, 4], selectable: false, evented: false, _isGuideline: true });
                    guidelines.push(line);
                    self.canvas.add(line);
                }
                // Bottom edge alignment
                var movingBottom = movingBR.top + movingBR.height;
                var objBottom = br.top + br.height;
                if (Math.abs(movingBottom - objBottom) < SNAP_THRESHOLD) {
                    moving.set('top', objBottom - movingBR.height + (moving.top - movingBR.top));
                    var line = new fabric.Line([0, objBottom, self.canvas.width / self.canvas.getZoom(), objBottom],
                        { stroke: '#00FFFF', strokeDashArray: [4, 4], selectable: false, evented: false, _isGuideline: true });
                    guidelines.push(line);
                    self.canvas.add(line);
                }
                // Horizontal center alignment
                var movingCX = movingBR.left + movingBR.width / 2;
                var objCX = br.left + br.width / 2;
                if (Math.abs(movingCX - objCX) < SNAP_THRESHOLD) {
                    moving.set('left', objCX - movingBR.width / 2 + (moving.left - movingBR.left));
                    var line = new fabric.Line([objCX, 0, objCX, self.canvas.height / self.canvas.getZoom()],
                        { stroke: '#00FFFF', strokeDashArray: [3, 3], selectable: false, evented: false, _isGuideline: true });
                    guidelines.push(line);
                    self.canvas.add(line);
                }
                // Vertical center alignment
                var movingCY = movingBR.top + movingBR.height / 2;
                var objCY = br.top + br.height / 2;
                if (Math.abs(movingCY - objCY) < SNAP_THRESHOLD) {
                    moving.set('top', objCY - movingBR.height / 2 + (moving.top - movingBR.top));
                    var line = new fabric.Line([0, objCY, self.canvas.width / self.canvas.getZoom(), objCY],
                        { stroke: '#00FFFF', strokeDashArray: [3, 3], selectable: false, evented: false, _isGuideline: true });
                    guidelines.push(line);
                    self.canvas.add(line);
                }
            });
            self.canvas.requestRenderAll();
        });

        this.canvas.on('object:modified', function() {
            guidelines.forEach(function(g) { self.canvas.remove(g); });
            guidelines = [];
            self.canvas.requestRenderAll();
        });
    },

    setupSelectionSync: function() {
        var self = this;
        this.canvas.on('selection:created', function(e) { self._syncSelection(e); });
        this.canvas.on('selection:updated', function(e) { self._syncSelection(e); });
        this.canvas.on('selection:cleared', function() {
            // Could notify Python side if needed
        });
    },

    _syncSelection: function(e) {
        // Sync selected object properties — called on selection change
        // No-op for now; Python reads state via getSelectedKey/getState when needed
    },

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

    flipSelectedH: function() {
        var active = this.canvas.getActiveObject();
        if (active) {
            active.set('flipX', !active.flipX);
            this.canvas.requestRenderAll();
        }
    },

    flipSelectedV: function() {
        var active = this.canvas.getActiveObject();
        if (active) {
            active.set('flipY', !active.flipY);
            this.canvas.requestRenderAll();
        }
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

    getState: function() {
        var stateObj = {};
        for (var key in this.fragments) {
            var obj = this.fragments[key];
            stateObj[key] = {
                x: obj.left,
                y: obj.top,
                rotation: obj.angle || 0,
                scaleX: obj.scaleX || 1,
                scaleY: obj.scaleY || 1,
                flipH: !!obj.flipX,
                flipV: !!obj.flipY,
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
                    this.fragments[key].set({
                        left: s.x, top: s.y,
                        angle: s.rotation || 0,
                        scaleX: s.scaleX || 1,
                        scaleY: s.scaleY || 1,
                        flipX: !!s.flipH,
                        flipY: !!s.flipV,
                    });
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

    _reloadFragment: function(key, newUrl, newMeta) {
        // Reload a fragment image in-place (same position/rotation/scale)
        var obj = this.fragments[key];
        if (!obj) return;
        var self = this;
        var pos = {
            left: obj.left, top: obj.top, angle: obj.angle,
            scaleX: obj.scaleX, scaleY: obj.scaleY,
            flipX: obj.flipX, flipY: obj.flipY
        };
        var htmlImg = new Image();
        htmlImg.crossOrigin = 'anonymous';
        htmlImg.onload = function() {
            self.canvas.remove(obj);
            var img = new fabric.Image(htmlImg, pos);
            img.set({
                hasControls: true, hasBorders: true,
                cornerSize: 12, transparentCorners: false,
                perPixelTargetFind: true, _fragmentKey: key,
                _imageUrl: newUrl, _fragmentMeta: newMeta || obj._fragmentMeta
            });
            self.canvas.add(img);
            self.fragments[key] = img;
            self.canvas.setActiveObject(img);
            self.canvas.requestRenderAll();
        };
        htmlImg.onerror = function() {
            console.error('Failed to reload fragment:', key);
        };
        htmlImg.src = newUrl;
    },

    _toggleFragmentBg: function(target) {
        if (!target || !target._fragmentKey) return;
        var meta = target._fragmentMeta;
        if (!meta) return;

        var self = this;
        var isProcessed = meta.processed !== false;
        var newProcessed = !isProcessed;
        var url = '/api/puzzle_image?fl_id=' + meta.fl_id +
                  '&threshold=' + (meta.threshold || 30) +
                  '&size=' + (meta.size || 800) +
                  '&processed=' + newProcessed;

        var htmlImg = new Image();
        htmlImg.crossOrigin = 'anonymous';
        htmlImg.onload = function() {
            target.setElement(htmlImg);
            meta.processed = newProcessed;
            self.canvas.requestRenderAll();
        };
        htmlImg.onerror = function() {
            console.error('Failed to toggle background for', target._fragmentKey);
        };
        htmlImg.src = url;
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
    flex-wrap: wrap;
    gap: 8px;
    padding: 8px 12px;
    background: #2d2d2d;
    border-bottom: 1px solid #444;
    align-items: center;
}
.puzzle-toolbar .q-input {
    max-width: 300px;
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
    """Resolve folio list for a sys_id using NLI manifest (same process, no HTTP)."""
    try:
        from web.api import fetch_fl_ids_from_nli
        fl_ids = fetch_fl_ids_from_nli(sys_id)
        if not fl_ids:
            return []
        return [
            {'fl_id': fid, 'label': f'{(i // 2) + 1}{"r" if i % 2 == 0 else "v"}'}
            for i, fid in enumerate(fl_ids)
        ]
    except Exception:
        return []


def _invalidate_and_refetch(fl_id: str, new_threshold: float):
    """Invalidate cache for a fragment so it gets re-processed at new threshold."""
    try:
        from shared.puzzle_image_service import get_puzzle_image_service
        service = get_puzzle_image_service()
        service.invalidate_cache(fl_id, threshold=None)
        # Pre-fetch at new threshold
        service.resolve_fragment_image(fl_id=fl_id, size=800, threshold=new_threshold, processed=True)
    except Exception as e:
        logger.error(f"Threshold refetch failed for {fl_id}: {e}")


def create_puzzle_page(initial_add: str = None):
    """Create the Fragment Puzzle page content.

    Args:
        initial_add: Optional 'sys_id,fl_id' string to auto-add a fragment on load.
    """
    # Add Fabric.js CDN and page-specific styles
    ui.add_head_html(FABRIC_JS_CDN)
    ui.add_head_html(PUZZLE_STYLES)
    ui.add_body_html(PUZZLE_CANVAS_JS)

    # Track fragment metadata (Python-side)
    # NOTE: app.storage.tab requires client connection, so start empty
    # and populate in init_canvas() after client connects
    puzzle_meta = {}

    # ── Main container ──
    with ui.column().classes('puzzle-container w-full'):

        # ── Toolbar Row 1: Shelfmark input + action buttons ──
        with ui.row().classes('puzzle-toolbar'):
            shelfmark_input = ui.input(
                placeholder=tr('Enter shelfmark...')
            ).props('dense outlined dark').classes('q-mr-sm').style('min-width: 250px')

            async def on_add_shelfmark():
                text = shelfmark_input.value
                if not text or not text.strip():
                    return
                text = text.strip()

                # Resolve shelfmark to sys_id
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
                # Get folios directly (same process, no localhost call)
                folios = await run.io_bound(_resolve_folios, sys_id)

                if not folios:
                    ui.notify(tr('No images found'), type='warning')
                    return

                first = folios[0]
                fl_id = first.get('fl_id', '')
                folio_label = first.get('label', '1r')
                key = f"{sys_id},{folio_label}"

                threshold = threshold_slider.value if threshold_slider else 30
                url = f"/api/puzzle_image?fl_id={fl_id}&threshold={threshold}&size=800&processed=true"

                # Position at center with offset for each new fragment
                frag_offset = len(puzzle_meta) * 50
                x = 100 + frag_offset
                y = 100 + frag_offset

                # Pass meta into JS addFragment so _fragmentMeta is set
                # synchronously when the image loads (no race condition)
                meta = {
                    'fl_id': fl_id, 'threshold': threshold,
                    'size': 800, 'processed': True,
                    'sys_id': sys_id
                }
                meta_json = json.dumps(meta)
                await ui.run_javascript(
                    f'window.puzzleCanvas.addFragment("{key}", "{url}", '
                    f'{x}, {y}, 0, 1.0, false, false, {meta_json})'
                )

                # Store metadata in Python storage
                puzzle_meta[key] = {
                    'sys_id': sys_id,
                    'shelfmark': shelfmark,
                    'folio_label': folio_label,
                    'fl_id': fl_id,
                    'threshold': threshold,
                }
                app.storage.tab['puzzle_fragments'] = puzzle_meta

                shelfmark_input.value = ''
                ui.notify(f'{shelfmark} ({folio_label})', type='positive')

            shelfmark_input.on('keydown.enter', lambda: on_add_shelfmark())

            ui.button(tr('Add'), icon='add', on_click=on_add_shelfmark).props(
                'dense flat dark color=primary'
            )
            async def on_delete_selected():
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

            ui.button(tr('Remove Selected'), icon='delete', on_click=on_delete_selected).props(
                'dense flat dark color=negative'
            )

            ui.separator().props('vertical')

            ui.button(icon='palette', on_click=lambda: ui.run_javascript(
                'window.puzzleCanvas.cycleBgMode()'
            )).props('dense flat dark').tooltip('Cycle background')

            ui.button(tr('Fit All'), icon='fit_screen', on_click=lambda: ui.run_javascript(
                'window.puzzleCanvas.fitAll()'
            )).props('dense flat dark')

            # Folio navigation (CANV-07)
            ui.separator().props('vertical')
            folio_label_display = ui.label('').classes('text-grey-3 text-caption').style(
                'min-width: 30px; text-align: center;'
            )

            async def on_folio_prev():
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

            ui.button(icon='chevron_left', on_click=on_folio_prev).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Previous Folio'))
            ui.button(icon='chevron_right', on_click=on_folio_next).props(
                'dense flat dark round size=sm'
            ).tooltip(tr('Next Folio'))

        # ── Toolbar Row 2: Sliders ──
        with ui.row().classes('puzzle-toolbar'):
            ui.label(tr('Scale')).classes('text-grey-5 text-caption')
            scale_slider = ui.slider(
                min=10, max=400, value=100, step=1
            ).props('dense dark label-always').style('min-width: 140px')

            async def on_scale_change(e):
                val = e.value if hasattr(e, 'value') else scale_slider.value
                await ui.run_javascript(
                    f'window.puzzleCanvas.setSelectedScale({val / 100})'
                )
            scale_slider.on('update:model-value', on_scale_change)

            ui.separator().props('vertical')

            ui.label(tr('Rotation')).classes('text-grey-5 text-caption')
            rotation_slider = ui.slider(
                min=-180, max=180, value=0, step=1
            ).props('dense dark label-always').style('min-width: 140px')

            async def on_rotation_change(e):
                val = e.value if hasattr(e, 'value') else rotation_slider.value
                await ui.run_javascript(
                    f'window.puzzleCanvas.setSelectedRotation({val})'
                )
            rotation_slider.on('update:model-value', on_rotation_change)

            ui.separator().props('vertical')

            ui.label(tr('Threshold')).classes('text-grey-5 text-caption')
            threshold_slider = ui.slider(
                min=10, max=80, value=30, step=1
            ).props('dense dark label-always').style('min-width: 120px')

            async def on_threshold_change():
                """Re-fetch selected fragment image at new threshold."""
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
                # Invalidate cache for old threshold
                await run.io_bound(
                    _invalidate_and_refetch, fl_id, new_threshold
                )
                url = f"/api/puzzle_image?fl_id={fl_id}&threshold={new_threshold}&size=800&processed=true"
                js_meta = json.dumps({
                    'fl_id': fl_id, 'threshold': new_threshold,
                    'size': 800, 'processed': True,
                    'sys_id': meta.get('sys_id', '')
                })
                await ui.run_javascript(
                    f'window.puzzleCanvas._reloadFragment("{key}", "{url}", {js_meta})',
                    timeout=15.0
                )
                # Update Python storage
                meta['threshold'] = new_threshold
                app.storage.tab['puzzle_fragments'] = puzzle_meta

            threshold_slider.on('change', lambda: on_threshold_change())

            ui.separator().props('vertical')

            show_original = ui.checkbox(tr('Show Original')).props('dense dark')
            show_original.on('update:model-value', lambda: ui.run_javascript(
                'window.puzzleCanvas.toggleSelectedBg()'
            ))

        # ── Canvas area ──
        with ui.element('div').classes('puzzle-canvas-wrap'):
            ui.html('<canvas id="puzzleCanvas"></canvas>')

    # ── Initialize canvas after DOM is ready ──
    async def init_canvas():
        # Wait for Fabric.js CDN to load (may take a few seconds on first visit)
        await ui.run_javascript(
            '''
            await new Promise(function(resolve) {
                if (typeof fabric !== "undefined") return resolve();
                var check = setInterval(function() {
                    if (typeof fabric !== "undefined") { clearInterval(check); resolve(); }
                }, 100);
                setTimeout(function() { clearInterval(check); resolve(); }, 10000);
            });
            window.puzzleCanvas.init("puzzleCanvas");
            ''',
            timeout=15.0
        )

        # Restore saved fragments (app.storage.tab is safe here — client connected)
        saved_meta = app.storage.tab.get('puzzle_fragments', {})
        saved_state = app.storage.tab.get('puzzle_state')
        # Populate outer puzzle_meta so callbacks can find existing fragments
        if saved_meta and isinstance(saved_meta, dict):
            puzzle_meta.update(saved_meta)

        if saved_meta and isinstance(saved_meta, dict):
            for key, meta in saved_meta.items():
                fl_id = meta.get('fl_id', '')
                threshold = meta.get('threshold', 30)
                if not fl_id:
                    continue

                url = f"/api/puzzle_image?fl_id={fl_id}&threshold={threshold}&size=800&processed=true"

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
                    except (json.JSONDecodeError, TypeError):
                        pass

                sys_id = meta.get('sys_id', '')
                js_meta = json.dumps({
                    'fl_id': fl_id, 'threshold': threshold,
                    'size': 800, 'processed': True,
                    'sys_id': sys_id
                })
                await ui.run_javascript(
                    f'window.puzzleCanvas.addFragment("{key}", "{url}", '
                    f'{x}, {y}, {rotation}, {scaleX}, '
                    f'{"true" if flipH else "false"}, {"true" if flipV else "false"}, {js_meta})'
                )

    ui.timer(0.5, init_canvas, once=True)

    # ── Handle initial_add query parameter ──
    if initial_add:
        parts = initial_add.split(',', 1)
        add_sys_id = parts[0].strip()
        add_fl_id = parts[1].strip() if len(parts) > 1 else ''

        async def auto_add():
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
            threshold = 30
            if state.meta_mgr:
                lib_code = state.meta_mgr.get_library_for_id(add_sys_id) or ''
                if lib_code == 'CUL':
                    threshold = 50

            url = f"/api/puzzle_image?fl_id={fl_id}&threshold={threshold}&size=800&processed=true"

            frag_offset = len(puzzle_meta) * 50
            js_meta = json.dumps({
                'fl_id': fl_id, 'threshold': threshold,
                'size': 800, 'processed': True,
                'sys_id': add_sys_id
            })
            await ui.run_javascript(
                f'window.puzzleCanvas.addFragment("{key}", "{url}", '
                f'{100 + frag_offset}, {100 + frag_offset}, 0, 1.0, false, false, {js_meta})'
            )

            shelfmark = ''
            if state.meta_mgr:
                sm, _ = state.meta_mgr.get_meta_for_id(add_sys_id)
                shelfmark = sm or ''

            puzzle_meta[key] = {
                'sys_id': add_sys_id,
                'shelfmark': shelfmark,
                'folio_label': folio_label,
                'fl_id': fl_id,
                'threshold': threshold,
            }
            app.storage.tab['puzzle_fragments'] = puzzle_meta

        ui.timer(1.5, auto_add, once=True)

    # ── Periodic state save ──
    async def save_state():
        try:
            state_json = await ui.run_javascript('window.puzzleCanvas.getState()')
            if state_json:
                app.storage.tab['puzzle_state'] = state_json
        except Exception:
            pass  # Page may not be active

    ui.timer(30, save_state)


def _get_port():
    """Get the app port for internal API calls."""
    import os
    return int(os.environ.get('GENIZAH_PORT', 8081))
