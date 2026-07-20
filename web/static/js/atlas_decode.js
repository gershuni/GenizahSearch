/* atlas_decode.js — Connections Atlas (Phase 133, ATLAS-01) client renderer.
 *
 * A self-contained Canvas 2D renderer for the Visual Atlas Preview. It has NO
 * CDN / framework dependency and is loadable in BOTH environments:
 *   - the browser page (attaches window.AtlasDecode; web/pages/atlas.py calls
 *     AtlasDecode.init(config) after the shell mounts the reserved canvas), and
 *   - Node (module.exports) — so the cross-language reference-decode test and
 *     the DOM-XSS neutralization test can require() the exact same decode + DOM
 *     builder code the browser runs.
 *
 * The binary payload is NEVER inlined in a <script> (Pitfall #3). At runtime the
 * renderer fetches the mutable pointer /atlas-data/manifest.json, reads the
 * content-hashed asset_basename from it, then fetches the immutable
 * /atlas-data/<asset_basename>.bin (the browser transparently Brotli-decodes via
 * Content-Encoding: br) and decodes it field-for-field against the FROZEN schema
 * docs/specs/atlas-asset-schema-v1.md.
 *
 * sys_id is decoded from NODE_SYS_ID via BigUint64Array ONLY — the frozen schema
 * (§7) guarantees every sys_id is pure-digit < 2^64 and the bake fails otherwise,
 * so there is a single code path and no fallback branch. It is emitted via
 * BigInt .toString(); a Number() cast would silently corrupt digits above 2^53
 * and break the /browse?sys_id= click-through.
 *
 * This payload carries NO candidate / claim / overlay fields (D-04): no per-MS
 * highlight, no counts, no identification scores. The atlas is a claim-free
 * connections overview; edge classes are continuation (same-work evidence) vs
 * island (citation / quotation) — an edge is NEVER a physical join (Pitfall #2).
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.AtlasDecode = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  // -----------------------------------------------------------------------
  // Section ID enum (frozen schema §4). The section table is self-describing
  // (dtype/elem_size/count/offset all read from it), so these ids are the only
  // fixed convention a decoder needs.
  // -----------------------------------------------------------------------
  var SEC = {
    NODE_POS: 1, NODE_CLUSTER: 2, NODE_DOMAIN: 3, NODE_LIBRARY: 4,
    NODE_PROMINENCE: 5, NODE_SYS_ID: 6, NODE_TITLE_REF: 7, NODE_SHELFMARK_REF: 8,
    EDGE_SOURCE_DELTA: 9, EDGE_TARGET_DELTA: 10, EDGE_CLASS: 11,
    FLOW_SOURCE_CLUSTER: 12, FLOW_TARGET_CLUSTER: 13, FLOW_WEIGHT: 14,
    CLUSTER_LABEL_CI: 15, CLUSTER_LABEL_X: 16, CLUSTER_LABEL_Y: 17,
    CLUSTER_LABEL_R: 18, CLUSTER_LABEL_N: 19, CLUSTER_LABEL_DGRP: 20,
    CLUSTER_LABEL_TITLE_REF: 21, CLUSTER_LABEL_DOM_REF: 22, STRING_HEAP: 23
  };
  // ASCII "ATLAS001" — the fixed-header magic (schema §2).
  var MAGIC = [0x41, 0x54, 0x4c, 0x41, 0x53, 0x30, 0x30, 0x31];
  var SCHEMA_VERSION = 1;

  // Edge class colors + continuation/island legend (Pitfall #2 — never a join).
  var EDGE_CONT_COLOR = '#39d98a';   // 0 = continuation (same-work evidence)
  var EDGE_ISLAND_COLOR = '#ff9d4d'; // 1 = island (citation / quotation)
  // Fallback categorical palette for the "color by library" mode (D-02).
  var LIB_PALETTE = [
    '#4ea3ff', '#ff5ed0', '#8fe65e', '#ffd35e', '#a06bff', '#5effd0',
    '#ff7d5e', '#2ec8e6', '#d4ff5e', '#c98a4b', '#9aa7ff', '#e8e8e8'
  ];
  var LIB_OTHER = '#77808f';
  // Zoom past this world-scale and per-manuscript edges are drawn; below it the
  // overview shows baked aggregate community flows + stars only (D-09 — the raw
  // pairwise web is an illegible hairball at overview scale).
  var EDGE_ZOOM = 1.7;

  // =======================================================================
  // 1. Binary decode — implements docs/specs/atlas-asset-schema-v1.md §10.
  // =======================================================================

  /**
   * Decode a frozen-schema atlas .bin ArrayBuffer into a plain object mirroring
   * the Python reference decoder (build_atlas_asset.decode_asset): sys_id is a
   * decimal string (never Number), floats are the Float32→Float64 widened values.
   * @param {ArrayBuffer} buffer
   * @returns {{schema_version:number, nodes:Array, edges:Array, flows:Array, cluster_labels:Array}}
   */
  function decodeAtlas(buffer) {
    var dv = new DataView(buffer);
    var i;
    // -- fixed header (16 bytes) --
    for (i = 0; i < 8; i++) {
      if (dv.getUint8(i) !== MAGIC[i]) {
        throw new Error('atlas decode: bad magic bytes (not ATLAS001)');
      }
    }
    var schemaVersion = dv.getUint32(8, true);
    if (schemaVersion !== SCHEMA_VERSION) {
      throw new Error('atlas decode: unsupported schema_version ' + schemaVersion);
    }
    var sectionCount = dv.getUint32(12, true);

    // -- section table (32 bytes per entry, from byte 16) --
    var sections = {};
    var off = 16;
    for (i = 0; i < sectionCount; i++) {
      var secId = dv.getUint32(off, true);
      var dtype = dv.getUint32(off + 4, true);
      var elemSize = dv.getUint32(off + 8, true);
      var count = dv.getUint32(off + 12, true);
      var byteOffset = Number(dv.getBigUint64(off + 16, true));
      var byteLength = Number(dv.getBigUint64(off + 24, true));
      sections[secId] = {
        dtype: dtype, elemSize: elemSize, count: count,
        byteOffset: byteOffset, byteLength: byteLength
      };
      off += 32;
    }

    // -- typed-array view over a section (dtype from the table — never assumed) --
    function viewOf(secId) {
      var s = sections[secId];
      if (!s) throw new Error('atlas decode: missing section ' + secId);
      switch (s.dtype) {
        case 1: return new Float32Array(buffer, s.byteOffset, s.count);
        case 2: return new Uint8Array(buffer, s.byteOffset, s.count);
        case 3: return new Uint16Array(buffer, s.byteOffset, s.count);
        case 4: return new Uint32Array(buffer, s.byteOffset, s.count);
        case 5: return new BigUint64Array(buffer, s.byteOffset, s.count);
        default: throw new Error('atlas decode: unknown dtype_code ' + s.dtype);
      }
    }

    var heap = viewOf(SEC.STRING_HEAP);
    var decoder = new TextDecoder('utf-8');
    function heapStr(refArr, idx) {
      var o = refArr[2 * idx], l = refArr[2 * idx + 1];
      if (l === 0) return '';
      return decoder.decode(heap.subarray(o, o + l));
    }

    // -- nodes (canonical node order shared by every NODE_* section) --
    var nodePos = viewOf(SEC.NODE_POS);
    var nodeCluster = viewOf(SEC.NODE_CLUSTER);
    var nodeDomain = viewOf(SEC.NODE_DOMAIN);
    var nodeLibrary = viewOf(SEC.NODE_LIBRARY);
    var nodeProm = viewOf(SEC.NODE_PROMINENCE);
    var nodeSysId = viewOf(SEC.NODE_SYS_ID); // BigUint64Array (schema §7)
    var titleRef = viewOf(SEC.NODE_TITLE_REF);
    var shelfRef = viewOf(SEC.NODE_SHELFMARK_REF);
    var nodeCount = sections[SEC.NODE_SYS_ID].count;

    var nodes = new Array(nodeCount);
    for (i = 0; i < nodeCount; i++) {
      nodes[i] = {
        x: nodePos[2 * i], y: nodePos[2 * i + 1],
        cluster: nodeCluster[i], domain: nodeDomain[i], library: nodeLibrary[i],
        prominence: nodeProm[i],
        // SINGLE sys_id path: BigUint64 -> decimal string (no Number cast).
        sys_id: nodeSysId[i].toString(),
        title: heapStr(titleRef, i),
        shelfmark: heapStr(shelfRef, i)
      };
    }

    // -- edges: decode the source/target deltas (schema §6) --
    var srcDelta = viewOf(SEC.EDGE_SOURCE_DELTA);
    var tgtDelta = viewOf(SEC.EDGE_TARGET_DELTA);
    var edgeClass = viewOf(SEC.EDGE_CLASS);
    var edges = new Array(srcDelta.length);
    var runS = 0, runT = 0;
    for (i = 0; i < srcDelta.length; i++) {
      var sd = srcDelta[i], td = tgtDelta[i];
      runS += sd;
      if (sd > 0 || i === 0) { runT = td; } else { runT += td; }
      edges[i] = { source: runS, target: runT, cls: edgeClass[i] };
    }

    // -- aggregate inter-cluster flows --
    var flowSrc = viewOf(SEC.FLOW_SOURCE_CLUSTER);
    var flowTgt = viewOf(SEC.FLOW_TARGET_CLUSTER);
    var flowW = viewOf(SEC.FLOW_WEIGHT);
    var flows = new Array(flowSrc.length);
    for (i = 0; i < flowSrc.length; i++) {
      flows[i] = {
        source_cluster: flowSrc[i], target_cluster: flowTgt[i], weight: flowW[i]
      };
    }

    // -- cluster labels (filtered subset, member_count >= 25) --
    var clCi = viewOf(SEC.CLUSTER_LABEL_CI);
    var clX = viewOf(SEC.CLUSTER_LABEL_X);
    var clY = viewOf(SEC.CLUSTER_LABEL_Y);
    var clR = viewOf(SEC.CLUSTER_LABEL_R);
    var clN = viewOf(SEC.CLUSTER_LABEL_N);
    var clDgrp = viewOf(SEC.CLUSTER_LABEL_DGRP);
    var clTitleRef = viewOf(SEC.CLUSTER_LABEL_TITLE_REF);
    var clDomRef = viewOf(SEC.CLUSTER_LABEL_DOM_REF);
    var clusterLabels = new Array(clCi.length);
    for (i = 0; i < clCi.length; i++) {
      clusterLabels[i] = {
        ci: clCi[i], x: clX[i], y: clY[i], r: clR[i], n: clN[i], dgrp: clDgrp[i],
        title: heapStr(clTitleRef, i), dom: heapStr(clDomRef, i)
      };
    }

    return {
      schema_version: schemaVersion,
      nodes: nodes, edges: edges, flows: flows, cluster_labels: clusterLabels
    };
  }

  // Mean (x, y) per cluster index, derived from the placed nodes — gives a
  // centroid for EVERY cluster (flows can reference clusters that never earned a
  // >=25-member label), so the overview aggregate flows can always be drawn.
  function computeClusterCentroids(nodes) {
    var acc = {};
    for (var i = 0; i < nodes.length; i++) {
      var c = nodes[i].cluster;
      var a = acc[c] || (acc[c] = { x: 0, y: 0, n: 0 });
      a.x += nodes[i].x; a.y += nodes[i].y; a.n += 1;
    }
    var out = {};
    for (var k in acc) {
      if (Object.prototype.hasOwnProperty.call(acc, k)) {
        out[k] = { x: acc[k].x / acc[k].n, y: acc[k].y / acc[k].n, n: acc[k].n };
      }
    }
    return out;
  }

  // =======================================================================
  // 2. Renderer — Canvas 2D. The whole interactive experience lives on the
  //    client against the decoded typed-array payload; no server round-trips
  //    after the initial manifest + asset fetch.
  // =======================================================================

  // Read the manifest pointer then the content-hashed payload (never inline).
  function fetchDecoded(config) {
    var manifestUrl = config.manifestUrl; // '/atlas-data/manifest.json'
    var dataBase = config.dataBase || '/atlas-data/';
    return fetch(manifestUrl, { cache: 'no-cache' })
      .then(function (r) {
        if (!r.ok) throw new Error('atlas manifest fetch failed: ' + r.status);
        return r.json();
      })
      .then(function (manifest) {
        var assetName = manifest.asset_basename + '.bin';
        return fetch(dataBase + assetName)
          .then(function (r) {
            if (!r.ok) throw new Error('atlas asset fetch failed: ' + r.status);
            return r.arrayBuffer();
          })
          .then(function (buf) {
            return { manifest: manifest, decoded: decodeAtlas(buf) };
          });
      });
  }

  function domainColor(state, node) {
    var g = state.domainGroups[node.domain];
    return (g && g[2]) || LIB_OTHER;
  }
  function libraryColor(node) {
    return LIB_PALETTE[node.library % LIB_PALETTE.length] || LIB_OTHER;
  }
  function nodeColor(state, node) {
    return state.colorBy === 'library' ? libraryColor(node) : domainColor(state, node);
  }

  // Localised domain label (D-15): manifest domain_groups is [en, he, color];
  // pick the column by the active UI language.
  function domainLabel(state, domainIdx) {
    var g = state.domainGroups[domainIdx];
    if (!g) return '';
    return state.lang === 'he' ? (g[1] || g[0]) : (g[0] || g[1]);
  }
  function libraryLabel(state, libIdx) {
    return state.libraries[libIdx] || '?';
  }

  // World<->screen transforms around the camera {x, y, k}.
  function toScreen(state, wx, wy) {
    return [(wx - state.cam.x) * state.cam.k + state.viewW / 2,
            (wy - state.cam.y) * state.cam.k + state.viewH / 2];
  }
  function toWorld(state, sx, sy) {
    return [(sx - state.viewW / 2) / state.cam.k + state.cam.x,
            (sy - state.viewH / 2) / state.cam.k + state.cam.y];
  }

  // Center + scale the camera so the whole galaxy fits the reserved canvas.
  function fitView(state) {
    var b = state.bounds;
    state.cam.x = (b.minx + b.maxx) / 2;
    state.cam.y = (b.miny + b.maxy) / 2;
    var spanX = Math.max(1, b.maxx - b.minx);
    var spanY = Math.max(1, b.maxy - b.miny);
    var kx = state.viewW / (spanX * 1.15);
    var ky = state.viewH / (spanY * 1.15);
    state.cam.k = Math.max(0.02, Math.min(kx, ky));
    state.baseK = state.cam.k;
  }

  function computeBounds(nodes) {
    var minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
    for (var i = 0; i < nodes.length; i++) {
      var n = nodes[i];
      if (n.x < minx) minx = n.x;
      if (n.x > maxx) maxx = n.x;
      if (n.y < miny) miny = n.y;
      if (n.y > maxy) maxy = n.y;
    }
    if (!isFinite(minx)) { minx = miny = -1; maxx = maxy = 1; }
    return { minx: minx, miny: miny, maxx: maxx, maxy: maxy };
  }

  // A manuscript is not drawn when a search filter excludes it or its library
  // is hidden (Task 2 populates matchSet / libHidden; inert defaults here).
  function isHidden(state, idx) {
    if (state.matchSet && !state.matchSet.has(idx)) return true;
    if (state.libHidden.has(state.decoded.nodes[idx].library)) return true;
    return false;
  }

  function drawEdges(ctx, state, list, alphaHex, dragMode) {
    var nodes = state.decoded.nodes;
    ctx.globalCompositeOperation = 'lighter';
    ctx.lineWidth = 0.5;
    var stride = dragMode ? 3 : 1;
    for (var i = 0; i < list.length; i += stride) {
      var ed = list[i];
      var a = nodes[ed.source], b = nodes[ed.target];
      if (!a || !b) continue;
      if (isHidden(state, ed.source) || isHidden(state, ed.target)) continue;
      var p1 = toScreen(state, a.x, a.y), p2 = toScreen(state, b.x, b.y);
      ctx.strokeStyle = (ed.cls ? EDGE_ISLAND_COLOR : EDGE_CONT_COLOR) + alphaHex;
      ctx.beginPath();
      ctx.moveTo(p1[0], p1[1]);
      ctx.lineTo(p2[0], p2[1]);
      ctx.stroke();
    }
    ctx.globalCompositeOperation = 'source-over';
  }

  // Aggregate inter-cluster flows between computed centroids (overview only).
  function drawFlows(ctx, state) {
    var cen = state.centroids;
    ctx.globalCompositeOperation = 'lighter';
    ctx.lineWidth = 0.6;
    for (var i = 0; i < state.decoded.flows.length; i++) {
      var f = state.decoded.flows[i];
      var a = cen[f.source_cluster], b = cen[f.target_cluster];
      if (!a || !b) continue;
      var p1 = toScreen(state, a.x, a.y), p2 = toScreen(state, b.x, b.y);
      ctx.strokeStyle = '#6f7fae22';
      ctx.beginPath();
      ctx.moveTo(p1[0], p1[1]);
      ctx.lineTo(p2[0], p2[1]);
      ctx.stroke();
    }
    ctx.globalCompositeOperation = 'source-over';
  }

  function drawStars(ctx, state, dragMode) {
    var nodes = state.decoded.nodes;
    var k = state.cam.k;
    var sz = Math.max(1, Math.min(4, 1.1 + k * 0.6));
    ctx.globalCompositeOperation = 'lighter';
    var stride = dragMode ? 3 : 1;
    for (var i = 0; i < nodes.length; i += stride) {
      if (isHidden(state, i)) continue;
      var nd = nodes[i];
      if (state.focusCluster >= 0 && nd.cluster !== state.focusCluster) continue;
      var p = toScreen(state, nd.x, nd.y);
      if (p[0] < -4 || p[0] > state.viewW + 4 || p[1] < -4 || p[1] > state.viewH + 4) continue;
      var hit = (i === state.hover);
      ctx.globalAlpha = hit ? 0.98 : 0.75;
      ctx.fillStyle = nodeColor(state, nd);
      ctx.fillRect(p[0] - sz, p[1] - sz, sz * 2, sz * 2);
    }
    ctx.globalCompositeOperation = 'source-over';
    ctx.globalAlpha = 1;
    // Ring the hovered star.
    if (state.hover >= 0 && !dragMode) {
      var hp = toScreen(state, nodes[state.hover].x, nodes[state.hover].y);
      ctx.strokeStyle = '#ffffff';
      ctx.lineWidth = 1.4;
      ctx.beginPath();
      ctx.arc(hp[0], hp[1], 7, 0, Math.PI * 2);
      ctx.stroke();
    }
  }

  // Cluster region labels painted on the canvas (not the DOM — pixels, not
  // markup, so no DOM-XSS surface; still masking-safe catalogue text).
  function drawClusterLabels(ctx, state) {
    if (state.focusCluster >= 0) return;
    var labels = state.decoded.cluster_labels;
    ctx.textAlign = 'center';
    for (var i = 0; i < labels.length; i++) {
      var lab = labels[i];
      var p = toScreen(state, lab.x, lab.y);
      if (p[0] < 0 || p[0] > state.viewW || p[1] < 0 || p[1] > state.viewH) continue;
      var text = domainLabel(state, lab.dgrp) || lab.dom || lab.title || '';
      if (!text) continue;
      ctx.font = 'bold 12px "Segoe UI", system-ui, sans-serif';
      ctx.fillStyle = '#ffffffdd';
      ctx.shadowColor = '#000';
      ctx.shadowBlur = 4;
      ctx.fillText(text, p[0], p[1] - 2);
      ctx.font = '10px "Segoe UI", system-ui, sans-serif';
      ctx.fillStyle = '#ffffff88';
      ctx.fillText(lab.n.toLocaleString(), p[0], p[1] + 12);
      ctx.shadowBlur = 0;
    }
  }

  function draw(state, dragMode) {
    var ctx = state.ctx;
    ctx.setTransform(state.dpr, 0, 0, state.dpr, 0, 0);
    // Dark radial background.
    var bg = ctx.createRadialGradient(
      state.viewW / 2, state.viewH * 0.42, 0,
      state.viewW / 2, state.viewH * 0.42, Math.max(state.viewW, state.viewH) * 0.75);
    bg.addColorStop(0, '#0d1526');
    bg.addColorStop(1, '#05070d');
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, state.viewW, state.viewH);

    var zoomedIn = state.cam.k > EDGE_ZOOM;
    var filtering = !!state.matchSet || state.libHidden.size > 0;

    if (state.focusCluster >= 0) {
      // Focus: dim the rest, draw the constellation's own edges + members.
      if (!dragMode && state.focusEdges) drawEdges(ctx, state, state.focusEdges, '88', dragMode);
      drawStars(ctx, state, dragMode);
    } else {
      // Overview: aggregate flows + stars; per-MS edges only once zoomed in.
      if (!dragMode && state.cam.k <= EDGE_ZOOM && !filtering) drawFlows(ctx, state);
      if (!dragMode && zoomedIn) drawEdges(ctx, state, state.decoded.edges, '22', dragMode);
      drawStars(ctx, state, dragMode);
      if (!dragMode) drawClusterLabels(ctx, state);
    }

    if (state.onAfterDraw) state.onAfterDraw(state);
  }

  // Size the canvas backing store to the reserved box * devicePixelRatio.
  function resizeCanvas(state) {
    var parent = state.canvas.parentElement || state.canvas;
    var cssW = parent.clientWidth || state.canvas.clientWidth || 800;
    var cssH = state.canvas.clientHeight || parent.clientHeight || 720;
    state.dpr = Math.min(window.devicePixelRatio || 1, 2);
    state.viewW = cssW;
    state.viewH = cssH;
    state.canvas.width = Math.round(cssW * state.dpr);
    state.canvas.height = Math.round(cssH * state.dpr);
  }

  // Locate the reserved canvas, then poll until it exists (NiceGUI renders the
  // element after the websocket connects, so it may not be present synchronously).
  function whenCanvasReady(canvasId, cb) {
    var tries = 0;
    (function poll() {
      var c = document.getElementById(canvasId);
      if (c) { cb(c); return; }
      if (tries++ > 200) return; // ~10s at 50ms
      setTimeout(poll, 50);
    })();
  }

  // Global handle so the browser page (and Task 2 interaction wiring) can reach
  // the live renderer state; also handy for debugging.
  function makeState(canvas, manifest, decoded, config) {
    var state = {
      canvas: canvas,
      ctx: canvas.getContext('2d'),
      manifest: manifest,
      decoded: decoded,
      domainGroups: manifest.domain_groups || [],
      libraries: manifest.libraries || [],
      centroids: computeClusterCentroids(decoded.nodes),
      bounds: computeBounds(decoded.nodes),
      cam: { x: 0, y: 0, k: 1 },
      baseK: 1,
      dpr: 1, viewW: 800, viewH: 720,
      colorBy: 'domain',
      lang: config.lang || 'he',
      rtl: !!config.rtl,
      labels: config.labels || {},
      // interaction state (Task 2 mutates these; draw() already respects them)
      hover: -1,
      matchSet: null,
      libHidden: new Set(),
      focusCluster: -1,
      focusMembers: null,
      focusEdges: null,
      onAfterDraw: null
    };
    return state;
  }

  function init(config) {
    config = config || {};
    var canvasId = config.canvasId || 'atlas-canvas';
    whenCanvasReady(canvasId, function (canvas) {
      fetchDecoded(config).then(function (res) {
        var state = makeState(canvas, res.manifest, res.decoded, config);
        window.__atlasRenderer = state;
        resizeCanvas(state);
        fitView(state);
        draw(state, false);
        window.addEventListener('resize', function () {
          resizeCanvas(state);
          draw(state, false);
        });
        if (typeof AtlasDecodeInteractions !== 'undefined') {
          // Interaction layer (Task 2) attaches here when present.
          AtlasDecodeInteractions(state, draw);
        }
        if (config.onReady) config.onReady(state);
      }).catch(function (err) {
        if (window && window.console) window.console.error('atlas init failed', err);
        // Surface the claim-free error copy without any markup interpolation.
        var box = canvas.parentElement;
        if (box) {
          var msg = document.createElement('div');
          msg.textContent = (config.labels && config.labels.loadError) ||
            'The atlas could not be loaded.';
          msg.setAttribute('style',
            'position:absolute;inset:0;display:flex;align-items:center;' +
            'justify-content:center;color:#8b93a7;font-size:14px;');
          box.appendChild(msg);
        }
      });
    });
  }

  return {
    SEC: SEC,
    decodeAtlas: decodeAtlas,
    computeClusterCentroids: computeClusterCentroids,
    computeBounds: computeBounds,
    domainLabel: domainLabel,
    libraryLabel: libraryLabel,
    nodeColor: nodeColor,
    fetchDecoded: fetchDecoded,
    draw: draw,
    init: init
  };
});
