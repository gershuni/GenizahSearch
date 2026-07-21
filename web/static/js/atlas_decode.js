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

  // -----------------------------------------------------------------------
  // Structural validation (Codex MEDIUM-1 — defense-in-depth). The 133-03
  // server loader already validates header + section-table bounds before
  // ever serving the asset, and the asset is our own content-hashed bake, so
  // this is NOT a live attack vector — but the decoder should never trust a
  // structurally-servable-yet-malformed payload into an OOB read, an
  // uncaught throw, a hang, or silently-wrong (truncated/undefined) data.
  // Any violation raises a plain Error, caught by init()'s .catch() and
  // surfaced via the normal load-error UI (never regresses valid-asset
  // decode — every check below holds for anything build_atlas_asset.py's
  // encoder can produce).
  // -----------------------------------------------------------------------

  // Sane upper bounds so a corrupted section-table count/section_count (each
  // an independent uint32, so up to 2**32-1) can never drive a pathological
  // allocation or loop. Real bakes are orders of magnitude smaller than these
  // (the D-10/PERF-01 byte-budget cap keeps the whole compressed asset under
  // ~6 MB) — generous headroom for corpus growth, still a hard ceiling.
  var MAX_SECTION_COUNT = 64;       // schema §4 defines 23 named sections
  var MAX_ELEMENT_COUNT = 20000000; // per-section element cap (nodes/edges/etc.)
  var MAX_HEAP_BYTES = 256 * 1024 * 1024; // STRING_HEAP is byte-addressed, not element-addressed

  // Every section §4 requires MUST be present (by construction the section-id
  // keyed `sections` map could otherwise silently omit one and leave a later
  // `viewOf()` throw as the only signal — this gives a clearer diagnostic and
  // fails BEFORE any typed-array view is constructed over a partial table).
  var REQUIRED_SECTIONS = [
    SEC.NODE_POS, SEC.NODE_CLUSTER, SEC.NODE_DOMAIN, SEC.NODE_LIBRARY,
    SEC.NODE_PROMINENCE, SEC.NODE_SYS_ID, SEC.NODE_TITLE_REF, SEC.NODE_SHELFMARK_REF,
    SEC.EDGE_SOURCE_DELTA, SEC.EDGE_TARGET_DELTA, SEC.EDGE_CLASS,
    SEC.FLOW_SOURCE_CLUSTER, SEC.FLOW_TARGET_CLUSTER, SEC.FLOW_WEIGHT,
    SEC.CLUSTER_LABEL_CI, SEC.CLUSTER_LABEL_X, SEC.CLUSTER_LABEL_Y,
    SEC.CLUSTER_LABEL_R, SEC.CLUSTER_LABEL_N, SEC.CLUSTER_LABEL_DGRP,
    SEC.CLUSTER_LABEL_TITLE_REF, SEC.CLUSTER_LABEL_DOM_REF, SEC.STRING_HEAP
  ];
  // dtype_code -> elem_size the schema mandates (§5) — cross-checks the
  // table's own (possibly inconsistent) dtype/elem_size pair.
  var ELEM_SIZE_BY_DTYPE = { 1: 4, 2: 1, 3: 2, 4: 4, 5: 8 };
  // Fixed dtype per section (§4) for every section EXCEPT the four whose
  // width is bake-dependent (§3 "dynamic dtype").
  var FIXED_DTYPE = {};
  FIXED_DTYPE[SEC.NODE_POS] = 1;
  FIXED_DTYPE[SEC.NODE_DOMAIN] = 3;
  FIXED_DTYPE[SEC.NODE_LIBRARY] = 3;
  FIXED_DTYPE[SEC.NODE_PROMINENCE] = 3;
  FIXED_DTYPE[SEC.NODE_SYS_ID] = 5;
  FIXED_DTYPE[SEC.NODE_TITLE_REF] = 4;
  FIXED_DTYPE[SEC.NODE_SHELFMARK_REF] = 4;
  FIXED_DTYPE[SEC.EDGE_SOURCE_DELTA] = 4;
  FIXED_DTYPE[SEC.EDGE_TARGET_DELTA] = 4;
  FIXED_DTYPE[SEC.EDGE_CLASS] = 2;
  FIXED_DTYPE[SEC.FLOW_WEIGHT] = 1;
  FIXED_DTYPE[SEC.CLUSTER_LABEL_X] = 1;
  FIXED_DTYPE[SEC.CLUSTER_LABEL_Y] = 1;
  FIXED_DTYPE[SEC.CLUSTER_LABEL_R] = 1;
  FIXED_DTYPE[SEC.CLUSTER_LABEL_N] = 4;
  FIXED_DTYPE[SEC.CLUSTER_LABEL_DGRP] = 3;
  FIXED_DTYPE[SEC.CLUSTER_LABEL_TITLE_REF] = 4;
  FIXED_DTYPE[SEC.CLUSTER_LABEL_DOM_REF] = 4;
  FIXED_DTYPE[SEC.STRING_HEAP] = 2;
  // §3: NODE_CLUSTER / FLOW_SOURCE_CLUSTER / FLOW_TARGET_CLUSTER /
  // CLUSTER_LABEL_CI all share ONE bake-chosen dtype (UINT16 or UINT32) —
  // must be internally consistent across all four.
  var DYNAMIC_DTYPE_SECTIONS = [
    SEC.NODE_CLUSTER, SEC.FLOW_SOURCE_CLUSTER, SEC.FLOW_TARGET_CLUSTER, SEC.CLUSTER_LABEL_CI
  ];

  function _decodeFail(msg) {
    throw new Error('atlas decode: ' + msg);
  }

  /**
   * Validate the parsed section table BEFORE any typed-array view or DOM
   * string is built from it: presence of every required section, dtype/
   * elem_size agreement (incl. the shared dynamic cluster-index dtype),
   * every section's byte range within the buffer and 8-byte aligned, every
   * related count internally consistent (schema §4's `2 * node_count` /
   * `node_count` / `edge_count` / `flow_count` / `label_count` relations),
   * and every count capped to a sane limit. Returns the cross-checked
   * {nodeCount, edgeCount, flowCount, labelCount}.
   */
  function validateSections(sections, bufferByteLength) {
    for (var r = 0; r < REQUIRED_SECTIONS.length; r++) {
      if (!sections[REQUIRED_SECTIONS[r]]) {
        _decodeFail('missing required section ' + REQUIRED_SECTIONS[r]);
      }
    }

    var dynDtype = null;
    for (var idKey in sections) {
      if (!Object.prototype.hasOwnProperty.call(sections, idKey)) continue;
      var id = Number(idKey);
      var s = sections[id];

      var expectedElem = ELEM_SIZE_BY_DTYPE[s.dtype];
      if (!expectedElem) _decodeFail('section ' + id + ': unknown dtype_code ' + s.dtype);
      if (expectedElem !== s.elemSize) {
        _decodeFail('section ' + id + ': elem_size ' + s.elemSize +
          ' does not match dtype_code ' + s.dtype);
      }

      if (DYNAMIC_DTYPE_SECTIONS.indexOf(id) >= 0) {
        if (s.dtype !== 3 && s.dtype !== 4) {
          _decodeFail('section ' + id + ': cluster-index dtype must be UINT16 or UINT32, got ' + s.dtype);
        }
        if (dynDtype === null) { dynDtype = s.dtype; }
        else if (dynDtype !== s.dtype) {
          _decodeFail('cluster-index sections disagree on dtype (NODE_CLUSTER/FLOW_*/CLUSTER_LABEL_CI must share one)');
        }
      } else if (FIXED_DTYPE[id] !== undefined && FIXED_DTYPE[id] !== s.dtype) {
        _decodeFail('section ' + id + ': expected dtype ' + FIXED_DTYPE[id] + ', got ' + s.dtype);
      }

      var maxCount = (id === SEC.STRING_HEAP) ? MAX_HEAP_BYTES : MAX_ELEMENT_COUNT;
      if (s.count > maxCount) {
        _decodeFail('section ' + id + ': count ' + s.count + ' exceeds the sane limit ' + maxCount);
      }
      if (s.byteOffset % 8 !== 0) {
        _decodeFail('section ' + id + ': byte_offset ' + s.byteOffset + ' is not 8-byte aligned');
      }
      if (s.byteLength !== s.count * s.elemSize) {
        _decodeFail('section ' + id + ': byte_length does not match count * elem_size');
      }
      if (s.byteOffset + s.byteLength > bufferByteLength) {
        _decodeFail('section ' + id + ': [' + s.byteOffset + ', ' + (s.byteOffset + s.byteLength) +
          ') extends past the end of the buffer (' + bufferByteLength + ' bytes)');
      }
    }

    // -- related-count consistency (schema §4) --
    var nodeCount = sections[SEC.NODE_SYS_ID].count;
    var nodeCountSections = [SEC.NODE_CLUSTER, SEC.NODE_DOMAIN, SEC.NODE_LIBRARY, SEC.NODE_PROMINENCE];
    for (var n = 0; n < nodeCountSections.length; n++) {
      if (sections[nodeCountSections[n]].count !== nodeCount) {
        _decodeFail('section ' + nodeCountSections[n] + ': count does not match node_count');
      }
    }
    var doubleNodeCountSections = [SEC.NODE_POS, SEC.NODE_TITLE_REF, SEC.NODE_SHELFMARK_REF];
    for (var d = 0; d < doubleNodeCountSections.length; d++) {
      if (sections[doubleNodeCountSections[d]].count !== 2 * nodeCount) {
        _decodeFail('section ' + doubleNodeCountSections[d] + ': count does not match 2 * node_count');
      }
    }

    var edgeCount = sections[SEC.EDGE_SOURCE_DELTA].count;
    if (sections[SEC.EDGE_TARGET_DELTA].count !== edgeCount) {
      _decodeFail('EDGE_TARGET_DELTA: count does not match EDGE_SOURCE_DELTA (edge_count)');
    }
    if (sections[SEC.EDGE_CLASS].count !== edgeCount) {
      _decodeFail('EDGE_CLASS: count does not match edge_count');
    }

    var flowCount = sections[SEC.FLOW_SOURCE_CLUSTER].count;
    if (sections[SEC.FLOW_TARGET_CLUSTER].count !== flowCount) {
      _decodeFail('FLOW_TARGET_CLUSTER: count does not match flow_count');
    }
    if (sections[SEC.FLOW_WEIGHT].count !== flowCount) {
      _decodeFail('FLOW_WEIGHT: count does not match flow_count');
    }

    var labelCount = sections[SEC.CLUSTER_LABEL_CI].count;
    var labelSections = [
      SEC.CLUSTER_LABEL_X, SEC.CLUSTER_LABEL_Y, SEC.CLUSTER_LABEL_R,
      SEC.CLUSTER_LABEL_N, SEC.CLUSTER_LABEL_DGRP
    ];
    for (var l = 0; l < labelSections.length; l++) {
      if (sections[labelSections[l]].count !== labelCount) {
        _decodeFail('label section ' + labelSections[l] + ': count does not match label_count');
      }
    }
    if (sections[SEC.CLUSTER_LABEL_TITLE_REF].count !== 2 * labelCount) {
      _decodeFail('CLUSTER_LABEL_TITLE_REF: count does not match 2 * label_count');
    }
    if (sections[SEC.CLUSTER_LABEL_DOM_REF].count !== 2 * labelCount) {
      _decodeFail('CLUSTER_LABEL_DOM_REF: count does not match 2 * label_count');
    }

    return { nodeCount: nodeCount, edgeCount: edgeCount, flowCount: flowCount, labelCount: labelCount };
  }

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
    // MEDIUM-1: bound section_count BEFORE looping the table with it — an
    // uncapped uint32 here could otherwise walk the DataView far past the
    // buffer (each entry read would eventually RangeError, but not before a
    // confusing partial parse; failing fast with a clean message is safer).
    if (sectionCount > MAX_SECTION_COUNT || 16 + 32 * sectionCount > buffer.byteLength) {
      throw new Error('atlas decode: invalid section_count ' + sectionCount);
    }

    // -- section table (32 bytes per entry, from byte 16) --
    var sections = {};
    var off = 16;
    for (i = 0; i < sectionCount; i++) {
      var secId = dv.getUint32(off, true);
      if (sections[secId]) {
        throw new Error('atlas decode: duplicate section_id ' + secId);
      }
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

    // MEDIUM-1: validate the whole table (required sections present, dtype/
    // elem_size/count relations, every byte range in-bounds + aligned)
    // BEFORE any typed-array view or heap string is built from it.
    validateSections(sections, buffer.byteLength);

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
      // MEDIUM-1: a malformed (offset, length) pair would otherwise clamp
      // silently in .subarray() and decode to a truncated/wrong string
      // instead of failing — refuse it explicitly.
      if (o < 0 || l < 0 || o + l > heap.length) {
        _decodeFail('string heap reference out of bounds (offset=' + o + ', length=' + l +
          ', heap_length=' + heap.length + ')');
      }
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
      // MEDIUM-1: refuse an edge whose decoded endpoint falls outside the
      // node array rather than silently drawing (or dropping) a corrupt edge.
      if (runS < 0 || runS >= nodeCount || runT < 0 || runT >= nodeCount) {
        _decodeFail('edge ' + i + ' endpoint out of node range (source=' + runS +
          ', target=' + runT + ', node_count=' + nodeCount + ')');
      }
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

  // Axis-aligned bounding-box overlap test (label declutter, #5).
  function _rectsOverlap(a, b) {
    return !(a.x1 < b.x0 || a.x0 > b.x1 || a.y1 < b.y0 || a.y0 > b.y1);
  }

  // Rounded-rect path (label backing plate). Uses the native ctx.roundRect when
  // available, else builds the path with arcTo so it degrades on older engines.
  function _roundRectPath(ctx, x, y, w, h, r) {
    if (typeof ctx.roundRect === 'function') {
      ctx.beginPath();
      ctx.roundRect(x, y, w, h, r);
      return;
    }
    var rr = Math.max(0, Math.min(r, w / 2, h / 2));
    ctx.beginPath();
    ctx.moveTo(x + rr, y);
    ctx.arcTo(x + w, y, x + w, y + h, rr);
    ctx.arcTo(x + w, y + h, x, y + h, rr);
    ctx.arcTo(x, y + h, x, y, rr);
    ctx.arcTo(x, y, x + w, y, rr);
    ctx.closePath();
  }

  // Cluster region labels painted on the canvas (not the DOM — pixels, not
  // markup, so no DOM-XSS surface; still masking-safe catalogue text).
  //   #4: the whole layer is suppressed when the user hides domain labels.
  //   #2: while a search is active, only labels whose text/domain/title match
  //       the query stay up (so the map isn't left cluttered with unrelated
  //       domain titles when the stars themselves are filtered).
  //   #5: labels are placed biggest-cluster-first and any whose box would
  //       collide with an already-placed label is skipped — so a zoomed-out
  //       (crowded) view shows only the few that fit, and more reveal
  //       themselves as the user zooms in and screen positions spread apart.
  function drawClusterLabels(ctx, state) {
    if (state.focusCluster >= 0) return;
    if (!state.showLabels) return;
    var labels = state.decoded.cluster_labels;
    var q = state.matchSet ? (state.matchQuery || '') : '';

    // 1. Gather on-screen candidates (applying the #2 search filter).
    var cand = [];
    for (var i = 0; i < labels.length; i++) {
      var lab = labels[i];
      var p = toScreen(state, lab.x, lab.y);
      if (p[0] < 0 || p[0] > state.viewW || p[1] < 0 || p[1] > state.viewH) continue;
      var text = domainLabel(state, lab.dgrp) || lab.dom || lab.title || '';
      if (!text) continue;
      if (q) {
        var hay = (text + ' ' + (lab.title || '') + ' ' + (lab.dom || '')).toLowerCase();
        if (hay.indexOf(q) < 0) continue;
      }
      cand.push({ x: p[0], y: p[1], n: lab.n || 0, text: text });
    }
    // 2. Biggest clusters first so they win the space when crowded (#5).
    cand.sort(function (a, b) { return b.n - a.n; });

    // 3. Greedy placement: draw a candidate only if its box is clear.
    ctx.textAlign = 'center';
    var placed = [];
    var PAD = 6;
    for (var c = 0; c < cand.length; c++) {
      var d = cand[c];
      ctx.font = 'bold 12px "Segoe UI", system-ui, sans-serif';
      var w = ctx.measureText(d.text).width;
      var box = { x0: d.x - w / 2 - PAD, x1: d.x + w / 2 + PAD, y0: d.y - 14, y1: d.y + 16 };
      var clash = false;
      for (var pI = 0; pI < placed.length; pI++) {
        if (_rectsOverlap(box, placed[pI])) { clash = true; break; }
      }
      if (clash) continue;
      placed.push(box);
      // Legibility: a semi-opaque dark rounded PLATE behind the label guarantees
      // contrast over the bright/white star cores as well as the dark void — a
      // stroke/shadow alone vanished on pure white (owner feedback 2026-07-21).
      // On the near-black background the plate is barely visible; over white it
      // darkens enough for the white text to read. Reuses the declutter box.
      ctx.shadowBlur = 0;
      _roundRectPath(ctx, box.x0, box.y0, box.x1 - box.x0, box.y1 - box.y0, 6);
      ctx.fillStyle = 'rgba(6,9,16,0.66)';
      ctx.fill();
      ctx.lineJoin = 'round';
      // title — thin dark outline + white fill on top of the plate.
      ctx.font = 'bold 12px "Segoe UI", system-ui, sans-serif';
      ctx.strokeStyle = 'rgba(0,0,0,0.55)';
      ctx.lineWidth = 2;
      ctx.strokeText(d.text, d.x, d.y - 2);
      ctx.fillStyle = '#ffffff';
      ctx.fillText(d.text, d.x, d.y - 2);
      // member count (dimmer).
      ctx.font = '10px "Segoe UI", system-ui, sans-serif';
      ctx.fillStyle = '#c9d4e6';
      ctx.fillText(d.n.toLocaleString(), d.x, d.y + 12);
    }
  }

  // First words of a title (or shelfmark), truncated for a compact per-dot label.
  function _firstWords(s, maxChars) {
    s = (s || '').trim();
    if (s.length <= maxChars) return s;
    var cut = s.slice(0, maxChars);
    var sp = cut.lastIndexOf(' ');
    if (sp > maxChars * 0.5) cut = cut.slice(0, sp);
    return cut + '…';
  }

  // Per-dot titles inside a focused constellation. Mobile has no hover tooltip,
  // so the dots are otherwise anonymous there; this shows the first words of each
  // dot's title. Greedy overlap-avoidance keeps it sparse — a dense cluster shows
  // only the labels that fit, and more appear as you zoom in. Bounded per frame
  // (<= CAP drawn, <= MAX_MEASURE measured) and only on settled (non-drag) frames.
  function drawFocusLabels(ctx, state) {
    if (state.focusCluster < 0 || !state.focusMembers) return;
    var nodes = state.decoded.nodes;
    ctx.textAlign = 'center';
    ctx.font = '10px "Segoe UI", system-ui, sans-serif';
    var placed = [];
    var CAP = 60, MAX_MEASURE = 400, PAD = 4;
    // Label the hovered node first so it always wins its space (desktop bonus).
    var order = [];
    if (state.hover >= 0 && nodes[state.hover] &&
        nodes[state.hover].cluster === state.focusCluster) {
      order.push(state.hover);
    }
    for (var m = 0; m < state.focusMembers.length; m++) {
      if (state.focusMembers[m] !== state.hover) order.push(state.focusMembers[m]);
    }
    var measured = 0;
    for (var i = 0; i < order.length && placed.length < CAP && measured < MAX_MEASURE; i++) {
      var idx = order[i];
      if (isHidden(state, idx)) continue;
      var nd = nodes[idx];
      var p = toScreen(state, nd.x, nd.y);
      if (p[0] < 0 || p[0] > state.viewW || p[1] < 0 || p[1] > state.viewH) continue;
      var raw = nd.title || nd.shelfmark || '';
      if (!raw) continue;
      var text = _firstWords(raw, 18);
      var w = ctx.measureText(text).width;
      measured++;
      var lx = p[0], ly = p[1] - 9;   // just above the dot
      var box = { x0: lx - w / 2 - PAD, x1: lx + w / 2 + PAD, y0: ly - 9, y1: ly + 4 };
      var clash = false;
      for (var pI = 0; pI < placed.length; pI++) {
        if (_rectsOverlap(box, placed[pI])) { clash = true; break; }
      }
      if (clash) continue;
      placed.push(box);
      _roundRectPath(ctx, box.x0, box.y0, box.x1 - box.x0, box.y1 - box.y0, 4);
      ctx.fillStyle = 'rgba(6,9,16,0.62)';
      ctx.fill();
      ctx.fillStyle = '#eaf1fb';
      ctx.fillText(text, lx, ly);
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
      if (!dragMode) drawFocusLabels(ctx, state);
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
    state.narrow = cssW <= 640;   // phones/narrow windows get the mobile focus UX
    state.canvas.width = Math.round(cssW * state.dpr);
    state.canvas.height = Math.round(cssH * state.dpr);
  }

  // Locate the reserved canvas, then poll until it exists (NiceGUI renders the
  // element after the websocket connects, so it may not be present synchronously).
  // If the canvas never mounts within the poll window, `onTimeout` (if given)
  // is invoked instead of returning silently (Codex MEDIUM-3 — an indefinite
  // silent spinner with no diagnostic is a bug, not acceptable degradation).
  function whenCanvasReady(canvasId, cb, onTimeout) {
    var tries = 0;
    (function poll() {
      var c = document.getElementById(canvasId);
      if (c) { cb(c); return; }
      if (tries++ > 200) { // ~10s at 50ms
        if (onTimeout) onTimeout();
        return;
      }
      setTimeout(poll, 50);
    })();
  }

  // Remove the server-rendered "Loading…" placeholder (Codex MEDIUM-2). Safe
  // to call multiple times / when the id is absent (config-less callers, or a
  // page variant without a placeholder) — always a no-op in that case.
  function hideLoadingPlaceholder(config) {
    if (!config || !config.loadingId || typeof document === 'undefined') return;
    var el = document.getElementById(config.loadingId);
    if (el && el.parentNode) el.parentNode.removeChild(el);
  }

  // Surface the SAME normal, tr()'d load-error UI from every failure path
  // (fetch/decode failure OR the canvas never mounting — MEDIUM-3). Always
  // hides the loading placeholder first so at most one of {canvas, error} is
  // ever visible, never a stuck "Loading…" underneath either (MEDIUM-2).
  function showLoadError(container, config, err) {
    if (err && typeof window !== 'undefined' && window.console) {
      window.console.error('atlas init failed', err);
    }
    hideLoadingPlaceholder(config);
    if (!container || typeof document === 'undefined') return;
    var msg = document.createElement('div');
    msg.textContent = (config && config.labels && config.labels.loadError) ||
      'The atlas could not be loaded.';
    msg.setAttribute('style',
      'position:absolute;inset:0;display:flex;align-items:center;' +
      'justify-content:center;color:#8b93a7;font-size:14px;');
    container.appendChild(msg);
  }

  // Best-effort analytics: the page already loads PostHog globally, so capture a
  // few semantic atlas events (pageviews are already auto-captured). NEVER sends
  // the raw search text — only its length — matching the project's "never
  // transmit search text" telemetry posture. Never throws.
  function _track(event, props) {
    try {
      if (typeof window !== 'undefined' && window.posthog &&
          typeof window.posthog.capture === 'function') {
        window.posthog.capture(event, props || {});
      }
    } catch (e) { /* analytics must never break the render */ }
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
      narrow: false,       // viewW <= 640 -> mobile focus UX (bottom sheet, no auto-open)
      colorBy: 'domain',
      lang: config.lang || 'he',
      rtl: !!config.rtl,
      labels: config.labels || {},
      // interaction state (Task 2 mutates these; draw() already respects them)
      hover: -1,
      matchSet: null,
      matchQuery: '',      // lowercased active search string (drives label filtering, #2)
      showLabels: true,    // domain-label visibility toggle (#4)
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
        // MEDIUM-2: hide the "Loading…" placeholder on the successful first
        // draw — BEFORE the bloom-in intro starts — so it never persists
        // over the rendered canvas.
        hideLoadingPlaceholder(config);
        var state = makeState(canvas, res.manifest, res.decoded, config);
        window.__atlasRenderer = state;
        resizeCanvas(state);
        fitView(state);
        draw(state, false);
        _track('atlas_loaded', {
          nodes: res.decoded.nodes.length,
          edges: res.decoded.edges.length
        });
        window.addEventListener('resize', function () {
          resizeCanvas(state);
          draw(state, false);
        });
        // Wire the full D-08 interactive experience (zoom/pan, search, color
        // toggle, library filter, focus constellation, reduced-motion intro,
        // click-through). All catalogue-derived DOM is built XSS-safely.
        attachInteractions(state);
        if (config.onReady) config.onReady(state);
      }).catch(function (err) {
        // Surface the claim-free error copy without any markup interpolation.
        // showLoadError() hides the loading placeholder first (MEDIUM-2), so
        // exactly one of {canvas, error} is ever visible.
        showLoadError(canvas.parentElement, config, err);
      });
    }, function () {
      // MEDIUM-3: the canvas never mounted within the ~10s poll window.
      // Surface the SAME normal load-error UI instead of an indefinite,
      // undiagnosable silent spinner. Fall back to the reserved box (by id)
      // or document.body if even that never mounted.
      var box = (config.boxId && typeof document !== 'undefined' &&
        document.getElementById(config.boxId)) ||
        (typeof document !== 'undefined' ? document.body : null);
      showLoadError(box, config,
        new Error('atlas canvas #' + canvasId + ' did not mount within the poll window'));
    });
  }

  // =======================================================================
  // 3. XSS-safe DOM builders (HIGH-7 / T-133-15).
  //
  //    EVERY catalogue-derived UI node (tooltip fields, focus-constellation
  //    rows, search results, legend, library chips) is built with
  //    document.createElement + .textContent / .setAttribute. A catalogue
  //    title / shelfmark / domain / library string is NEVER interpolated into
  //    an innerHTML assignment — so a hostile string in the payload (the
  //    committed reference fixture carries a fabricated one) renders as inert
  //    text, never parsed as markup. The Node DOM-XSS test drives the two
  //    builders below over that fabricated string via an injected fake document.
  // =======================================================================

  // Document handle: the live document in the browser, or a fake injected by
  // the Node test via setDocument(). Accessed lazily so require()-ing the module
  // in Node (no global document) never throws at load time.
  var _doc = (typeof document !== 'undefined') ? document : null;
  function setDocument(d) { _doc = d; }

  function _el(tag, cls) {
    var e = _doc.createElement(tag);
    if (cls) e.className = cls;
    return e;
  }
  // The single primitive that binds catalogue text to the DOM — .textContent
  // ONLY (never innerHTML). All data-bearing nodes funnel through here.
  function _txt(tag, text, cls) {
    var e = _el(tag, cls);
    e.textContent = (text == null) ? '' : String(text);
    return e;
  }
  function _clear(node) {
    while (node.firstChild) node.removeChild(node.firstChild);
  }

  /**
   * Tooltip body for a hovered manuscript: shelfmark + catalogue title + domain
   * + library, all masking-safe catalogue data (D-06), all via textContent.
   */
  function buildTooltipContent(state, node) {
    var box = _el('div', 'atlas-tip-inner');
    box.appendChild(_txt('div', node.shelfmark || node.sys_id, 'atlas-tip-shelf'));
    if (node.title) box.appendChild(_txt('div', node.title, 'atlas-tip-title'));
    var meta = _el('div', 'atlas-tip-meta');
    meta.appendChild(_txt('span', state.labels.domain + ': ' + domainLabel(state, node.domain)));
    meta.appendChild(_txt('span', '  ·  ' + state.labels.library + ': ' + libraryLabel(state, node.library)));
    box.appendChild(meta);
    return box;
  }

  /**
   * A focus-constellation member row: a domain-colored dot + catalogue title,
   * with the shelfmark + domain beneath. data-sys carries the sys_id for the
   * click-through the caller wires. All text via textContent.
   */
  function buildFocusRow(state, node) {
    var row = _el('div', 'atlas-prow');
    row.setAttribute('data-sys', node.sys_id);
    var head = _el('div', 'atlas-prow-head');
    var dot = _txt('span', '●', 'atlas-dot');
    dot.setAttribute('style', 'color:' + nodeColor(state, node) + ';');
    head.appendChild(dot);
    head.appendChild(_txt('span', ' ' + (node.title || node.shelfmark || node.sys_id), 'atlas-prow-t'));
    row.appendChild(head);
    row.appendChild(_txt('div', (node.shelfmark || '') + '  ·  ' + domainLabel(state, node.domain), 'atlas-prow-d'));
    return row;
  }

  // =======================================================================
  // 4. Interaction layer — zoom/pan, hover tooltip, search, color toggle,
  //    library filter (hide-one / solo-one), focus constellation, reduced-motion
  //    bloom-in intro, same-origin /browse click-through (D-08).
  // =======================================================================

  // Spatial hash grid over world coords for O(1)-ish hover/click picking.
  function buildPickGrid(state) {
    var nodes = state.decoded.nodes;
    var b = state.bounds;
    var span = Math.max(b.maxx - b.minx, b.maxy - b.miny, 1);
    var cell = span / 120;
    var grid = {};
    function key(cx, cy) { return cx + '_' + cy; }
    for (var i = 0; i < nodes.length; i++) {
      var cx = Math.floor((nodes[i].x - b.minx) / cell);
      var cy = Math.floor((nodes[i].y - b.miny) / cell);
      var k = key(cx, cy);
      (grid[k] || (grid[k] = [])).push(i);
    }
    state._grid = { grid: grid, cell: cell, b: b, key: key };
  }

  function pick(state, sx, sy) {
    if (!state._grid) return -1;
    var w = toWorld(state, sx, sy);
    var g = state._grid;
    var cx = Math.floor((w[0] - g.b.minx) / g.cell);
    var cy = Math.floor((w[1] - g.b.miny) / g.cell);
    var nodes = state.decoded.nodes;
    var best = -1, bestD = 14 * 14; // ~14px screen radius
    for (var dx = -1; dx <= 1; dx++) {
      for (var dy = -1; dy <= 1; dy++) {
        var bucket = g.grid[g.key(cx + dx, cy + dy)];
        if (!bucket) continue;
        for (var j = 0; j < bucket.length; j++) {
          var idx = bucket[j];
          if (isHidden(state, idx)) continue;
          if (state.focusCluster >= 0 && nodes[idx].cluster !== state.focusCluster) continue;
          var p = toScreen(state, nodes[idx].x, nodes[idx].y);
          var d = (p[0] - sx) * (p[0] - sx) + (p[1] - sy) * (p[1] - sy);
          if (d < bestD) { bestD = d; best = idx; }
        }
      }
    }
    return best;
  }

  function zoomBy(state, factor, sx, sy) {
    // Zoom toward the cursor world point.
    var before = toWorld(state, sx, sy);
    state.cam.k = Math.max(0.02, Math.min(2000, state.cam.k * factor));
    var after = toWorld(state, sx, sy);
    state.cam.x += before[0] - after[0];
    state.cam.y += before[1] - after[1];
  }

  // --- search filter: title + shelfmark + sys_id ONLY (never domain/library,
  //     which would flood the map). Builds a match set of node indices. ---
  function applySearch(state, query) {
    var s = (query || '').trim().toLowerCase();
    if (!s) { state.matchSet = null; state.matchQuery = ''; return; }
    var nodes = state.decoded.nodes;
    var set = new Set();
    for (var i = 0; i < nodes.length; i++) {
      var nd = nodes[i];
      if ((nd.title && nd.title.toLowerCase().indexOf(s) >= 0) ||
          (nd.shelfmark && nd.shelfmark.toLowerCase().indexOf(s) >= 0) ||
          (nd.sys_id.indexOf(s) >= 0)) {
        set.add(i);
      }
    }
    state.matchSet = set;
    // #2: remember the query so drawClusterLabels can keep only the domain
    // labels that match the searched term (instead of leaving them all up).
    state.matchQuery = s;
  }

  function focusOn(state, idx) {
    var nodes = state.decoded.nodes;
    state.focusCluster = nodes[idx].cluster;
    state.focusMembers = [];
    for (var i = 0; i < nodes.length; i++) {
      if (nodes[i].cluster === state.focusCluster) state.focusMembers.push(i);
    }
    state.focusEdges = state.decoded.edges.filter(function (ed) {
      return nodes[ed.source].cluster === state.focusCluster &&
             nodes[ed.target].cluster === state.focusCluster;
    });
    // Fit the constellation to the view.
    var minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
    for (var m = 0; m < state.focusMembers.length; m++) {
      var nd = nodes[state.focusMembers[m]];
      if (nd.x < minx) minx = nd.x; if (nd.x > maxx) maxx = nd.x;
      if (nd.y < miny) miny = nd.y; if (nd.y > maxy) maxy = nd.y;
    }
    if (isFinite(minx)) {
      state.cam.x = (minx + maxx) / 2;
      state.cam.y = (miny + maxy) / 2;
      var spanX = Math.max(1, maxx - minx), spanY = Math.max(1, maxy - miny);
      state.cam.k = Math.max(0.2, Math.min(state.viewW / (spanX * 1.4), state.viewH / (spanY * 1.4)));
    }
  }

  function backToGalaxy(state) {
    state.focusCluster = -1;
    state.focusMembers = null;
    state.focusEdges = null;
  }

  function attachInteractions(state) {
    var canvas = state.canvas;
    var box = canvas.parentElement || canvas;
    buildPickGrid(state);

    // --- overlay scaffolding (absolute over the reserved relative box) ---
    var dirStyle = state.rtl ? 'direction:rtl;' : 'direction:ltr;';
    var focusPanelOpen = false;   // is the focus member-list panel currently open?

    // Toolbar (top): search + color toggle + library toggle + zoom buttons.
    var toolbar = _el('div', 'atlas-toolbar');
    toolbar.setAttribute('style',
      'position:absolute;top:8px;left:8px;right:8px;z-index:8;display:flex;gap:6px;' +
      'flex-wrap:wrap;align-items:center;' + dirStyle);

    var search = _el('input', 'atlas-search');
    search.setAttribute('type', 'search');
    search.setAttribute('placeholder', state.labels.searchPlaceholder || '');
    search.setAttribute('aria-label', state.labels.searchPlaceholder || '');
    search.setAttribute('style',
      'flex:0 1 240px;padding:5px 9px;border-radius:6px;border:1px solid #33405c;' +
      'background:#0d1526cc;color:#dfe8f5;font-size:13px;');

    var colorBtn = _btn(state.labels.colorByLibrary || 'Color by library');
    var libBtn = _btn(state.labels.showAll || 'Libraries');
    var zoomInBtn = _btn('+');
    zoomInBtn.setAttribute('aria-label', state.labels.zoomIn || 'Zoom in');
    var zoomOutBtn = _btn('−');
    zoomOutBtn.setAttribute('aria-label', state.labels.zoomOut || 'Zoom out');
    var resetBtn = _btn(state.labels.resetView || 'Reset view');
    // #4: toggle the domain-title layer. Starts visible -> button offers "Hide".
    var labelsBtn = _btn(state.labels.hideDomainLabels || 'Hide domain labels');
    // #3: fullscreen the reserved box. Starts collapsed -> button offers "Full screen".
    var fsBtn = _btn(state.labels.fullScreen || 'Full screen');

    toolbar.appendChild(search);
    toolbar.appendChild(colorBtn);
    toolbar.appendChild(libBtn);
    toolbar.appendChild(labelsBtn);
    toolbar.appendChild(zoomInBtn);
    toolbar.appendChild(zoomOutBtn);
    toolbar.appendChild(resetBtn);
    toolbar.appendChild(fsBtn);
    box.appendChild(toolbar);

    // Tooltip (hover).
    var tip = _el('div', 'atlas-tip');
    tip.setAttribute('style',
      'position:absolute;z-index:9;display:none;pointer-events:none;max-width:260px;' +
      'padding:7px 9px;border-radius:7px;background:#0d1526f2;border:1px solid #33405c;' +
      'color:#dfe8f5;font-size:12px;' + dirStyle);
    box.appendChild(tip);

    // Legend (bottom).
    var legend = _el('div', 'atlas-legend');
    legend.setAttribute('style',
      'position:absolute;bottom:8px;left:8px;right:8px;z-index:7;display:flex;gap:10px;' +
      'flex-wrap:wrap;font-size:11px;color:#aab4c8;pointer-events:none;' + dirStyle);
    box.appendChild(legend);

    // Library filter panel (toggled).
    var libPanel = _el('div', 'atlas-libpanel');
    libPanel.setAttribute('style',
      'position:absolute;top:46px;right:8px;z-index:8;display:none;flex-wrap:wrap;gap:6px;' +
      'max-width:320px;padding:8px;border-radius:8px;background:#0d1526f2;' +
      'border:1px solid #33405c;' + dirStyle);
    box.appendChild(libPanel);

    // Focus constellation member panel (shown in focus mode; positioned as a
    // bottom sheet on narrow screens, a side drawer on wide — set in rebuildFocusPanel).
    var focusPanel = _el('div', 'atlas-focus');
    focusPanel.setAttribute('style', 'position:absolute;z-index:9;display:none;');
    box.appendChild(focusPanel);

    // Floating focus controls (Back to map + Titles toggle), shown only while a
    // cluster is focused. Kept clear of the top toolbar (which wraps on narrow
    // screens and used to hide the panel's ✕) — this is the reliable way to
    // exit a cluster on mobile. Sits just above the bottom sheet when open.
    var focusBar = _el('div', 'atlas-focusbar');
    focusBar.setAttribute('style', 'position:absolute;left:8px;bottom:8px;z-index:11;display:none;gap:6px;' + dirStyle);
    var backBtn = _btn('← ' + (state.labels.backToMap || 'Back to map'));
    backBtn.setAttribute('aria-label', state.labels.backToMap || 'Back to map');
    backBtn.onclick = function () { backToGalaxy(state); focusPanelOpen = false; rebuildFocusPanel(); redraw(); };
    var titlesToggle = _btn(state.labels.titles || 'Titles');
    titlesToggle.onclick = function () { focusPanelOpen = !focusPanelOpen; rebuildFocusPanel(); };
    focusBar.appendChild(backBtn);
    focusBar.appendChild(titlesToggle);
    box.appendChild(focusBar);

    // --- legend builder (domain groups or library palette) ---
    function rebuildLegend() {
      _clear(legend);
      // edge-class key first (continuation vs island — never a physical join).
      legend.appendChild(_swatch(EDGE_CONT_COLOR, state.labels.continuation || 'Continuation'));
      legend.appendChild(_swatch(EDGE_ISLAND_COLOR, state.labels.citation || 'Citation'));
      if (state.colorBy === 'domain') {
        for (var d = 0; d < state.domainGroups.length; d++) {
          legend.appendChild(_swatch(state.domainGroups[d][2], domainLabel(state, d)));
        }
      } else {
        for (var l = 0; l < state.libraries.length; l++) {
          legend.appendChild(_swatch(LIB_PALETTE[l % LIB_PALETTE.length], state.libraries[l]));
        }
      }
    }

    // --- library filter panel (hide-one / solo-one) ---
    function rebuildLibPanel() {
      _clear(libPanel);
      var hint = _txt('div', (state.labels.hideLibrary || 'Hide library') +
        '  ·  ' + (state.labels.showOnly || 'Show only this'), 'atlas-lhint');
      hint.setAttribute('style', 'flex-basis:100%;color:#8b93a7;font-size:11px;margin-bottom:3px;');
      libPanel.appendChild(hint);
      var allChip = _txt('span', '✓ ' + (state.labels.showAll || 'Show all'), 'atlas-libchip');
      allChip.setAttribute('style', _chipStyle(false));
      allChip.onclick = function () { state.libHidden.clear(); rebuildLibPanel(); redraw(); };
      libPanel.appendChild(allChip);
      for (var l = 0; l < state.libraries.length; l++) {
        (function (idx) {
          var off = state.libHidden.has(idx);
          var chip = _el('span', 'atlas-libchip');
          chip.setAttribute('style', _chipStyle(off));
          var sw = _txt('span', '', 'atlas-sw');
          sw.setAttribute('style',
            'display:inline-block;width:9px;height:9px;border-radius:2px;margin-inline-end:5px;' +
            'background:' + (LIB_PALETTE[idx % LIB_PALETTE.length]) + ';');
          chip.appendChild(sw);
          chip.appendChild(_txt('span', state.libraries[idx]));
          chip.onclick = function (ev) {
            if (ev && ev.shiftKey) {
              // solo-one: hide every OTHER library.
              state.libHidden.clear();
              for (var k = 0; k < state.libraries.length; k++) {
                if (k !== idx) state.libHidden.add(k);
              }
            } else if (state.libHidden.has(idx)) {
              state.libHidden.delete(idx);
            } else {
              state.libHidden.add(idx);
            }
            rebuildLibPanel();
            redraw();
          };
          libPanel.appendChild(chip);
        })(l);
      }
    }

    // --- focus constellation panel ---
    function rebuildFocusPanel() {
      _clear(focusPanel);
      if (state.focusCluster < 0 || !state.focusMembers) {
        // Back in the galaxy: hide the focus UI and restore the legend.
        focusPanel.style.display = 'none';
        focusBar.style.display = 'none';
        focusPanelOpen = false;
        legend.style.display = 'flex';
        return;
      }
      var nodes = state.decoded.nodes;
      var vis = state.focusMembers.filter(function (i) { return !state.libHidden.has(nodes[i].library); });

      // Focus controls: always shown while focused. The legend is hidden so it
      // doesn't collide with the bottom controls / sheet.
      legend.style.display = 'none';
      focusBar.style.display = 'flex';
      // Lift the bar above the sheet when the sheet is open on a narrow screen.
      focusBar.style.bottom = (state.narrow && focusPanelOpen) ? '48%' : '8px';
      titlesToggle.textContent =
        (focusPanelOpen ? (state.labels.hideTitles || 'Hide titles')
                        : (state.labels.titles || 'Titles')) +
        ' (' + vis.length.toLocaleString() + ')';

      // The member list: a bottom sheet on narrow screens (leaves the cluster
      // visible above it), a side drawer on wide. Only rendered when opened.
      var vis_css = focusPanelOpen ? 'flex' : 'none';
      if (state.narrow) {
        focusPanel.setAttribute('style',
          'position:absolute;left:0;right:0;bottom:0;top:auto;width:auto;max-height:46%;z-index:9;' +
          'display:' + vis_css + ';flex-direction:column;padding:10px;' +
          'border-radius:12px 12px 0 0;background:#0d1526f6;border-top:1px solid #33405c;' +
          'color:#dfe8f5;' + dirStyle);
      } else {
        focusPanel.setAttribute('style',
          'position:absolute;top:46px;bottom:34px;width:300px;z-index:9;' +
          (state.rtl ? 'left:8px;' : 'right:8px;') +
          'display:' + vis_css + ';flex-direction:column;padding:10px;' +
          'border-radius:8px;background:#0d1526f2;border:1px solid #33405c;color:#dfe8f5;' + dirStyle);
      }
      if (!focusPanelOpen) return;   // controls shown; list stays collapsed

      var head = _el('div', 'atlas-focus-head');
      head.setAttribute('style', 'display:flex;align-items:center;gap:8px;margin-bottom:4px;');
      var hideBtn = _btn('✕');
      hideBtn.setAttribute('aria-label', state.labels.hideTitles || 'Hide titles');
      hideBtn.onclick = function () { focusPanelOpen = false; rebuildFocusPanel(); };
      head.appendChild(hideBtn);
      head.appendChild(_txt('span', state.labels.focusConstellation || 'Focus constellation', 'atlas-focus-title'));
      focusPanel.appendChild(head);
      focusPanel.appendChild(_txt('div',
        vis.length.toLocaleString() + '  ·  ' + (state.labels.connections || 'Connections') +
        ': ' + state.focusEdges.length.toLocaleString(), 'atlas-focus-meta'));
      var list = _el('div', 'atlas-focus-list');
      list.setAttribute('style', 'overflow:auto;flex:1 1 auto;margin-top:4px;min-height:0;');
      // Bound the number of rows built for very large constellations.
      var cap = Math.min(vis.length, 400);
      for (var r = 0; r < cap; r++) {
        (function (nodeIdx) {
          var nd = nodes[nodeIdx];
          var row = buildFocusRow(state, nd);
          row.setAttribute('style', 'padding:3px 5px;border-radius:5px;cursor:pointer;');
          row.onclick = function () { openBrowsePane(nd.sys_id); };
          list.appendChild(row);
        })(vis[r]);
      }
      focusPanel.appendChild(list);
    }

    function redraw() { draw(state, false); }

    // draw() calls this after each paint so DOM overlays stay in sync with mode.
    state.onAfterDraw = function () { /* overlays are event-driven; no per-frame DOM work */ };

    // --- in-atlas browse pane (#6): a drawer against the inner edge (right in
    //     LTR, left in RTL) holding an <iframe> onto the bare ?embed=1 browse
    //     viewer, so clicking a dot opens the manuscript WITHOUT leaving the
    //     atlas. The header offers Close + a link to the full standalone /browse
    //     page (new tab). Built lazily on first open. ---
    var browsePane = null, browseFrame = null, browseFull = null;
    function ensureBrowsePane() {
      if (browsePane) return;
      var side = state.rtl ? 'left:0;' : 'right:0;';
      browsePane = _el('div', 'atlas-browse-pane');
      browsePane.setAttribute('style',
        'position:absolute;top:0;bottom:0;' + side +
        'width:min(560px,46%);z-index:15;display:none;flex-direction:column;' +
        'background:#0b1120;border-inline-start:1px solid #33405c;' +
        'box-shadow:0 0 26px rgba(0,0,0,0.55);' + dirStyle);
      var hdr = _el('div', 'atlas-bp-head');
      hdr.setAttribute('style',
        'display:flex;align-items:center;gap:8px;padding:7px 9px;' +
        'border-bottom:1px solid #33405c;background:#0d1526;');
      var closeBtn = _btn('✕');
      closeBtn.setAttribute('aria-label', state.labels.close || 'Close');
      closeBtn.onclick = closeBrowsePane;
      browseFull = _el('a', 'atlas-bp-full');
      browseFull.textContent = state.labels.openFullBrowse || 'Open full browse ↗';
      browseFull.setAttribute('target', '_blank');
      browseFull.setAttribute('rel', 'noopener');
      browseFull.setAttribute('style',
        'margin-inline-start:auto;color:#8fb2d8;font-size:12px;text-decoration:none;');
      hdr.appendChild(closeBtn);
      hdr.appendChild(browseFull);
      browsePane.appendChild(hdr);
      browseFrame = _el('iframe', 'atlas-bp-frame');
      browseFrame.setAttribute('title', state.labels.manuscriptViewer || 'Manuscript viewer');
      browseFrame.setAttribute('style', 'flex:1 1 auto;width:100%;border:0;background:#fff;');
      browsePane.appendChild(browseFrame);
      box.appendChild(browsePane);
    }
    function openBrowsePane(sysId) {
      // sys_id is a decimal string (BigUint64 .toString) — all digits, but
      // encodeURIComponent it anyway. Same-origin; never interpolated into
      // markup (setAttribute only), so no attribute/URL injection (HIGH-7).
      var qs = 'sys_id=' + encodeURIComponent('' + sysId);
      _track('atlas_manuscript_opened', { sys_id: '' + sysId });
      ensureBrowsePane();
      browseFrame.setAttribute('src', '/browse?embed=1&' + qs);
      browseFull.setAttribute('href', '/browse?' + qs);
      browsePane.style.display = 'flex';
    }
    function closeBrowsePane() {
      if (!browsePane) return false;
      var wasOpen = browsePane.style.display !== 'none';
      browsePane.style.display = 'none';
      // Stop the framed page loading / free its resources while hidden.
      if (browseFrame) browseFrame.setAttribute('src', 'about:blank');
      return wasOpen;
    }

    // --- pointer: pan (1 finger/mouse) + pinch-zoom (2 fingers) + hover ---
    // Multi-touch is tracked in a pointer map so phones/tablets can pinch to
    // zoom (there is no wheel on touch). touch-action:none stops the browser
    // swallowing the gesture as page scroll/zoom before our handlers see it.
    if (canvas.style) canvas.style.touchAction = 'none';
    var pointers = {};           // active pointerId -> {x, y} in canvas px
    var dragMoved = false;       // gate: a drag/pinch is never treated as a tap
    var lastX = 0, lastY = 0;    // single-pointer pan anchor
    var pinchDist = 0;           // last two-pointer separation (screen px)
    function relPos(ev) {
      var rect = canvas.getBoundingClientRect();
      return [ev.clientX - rect.left, ev.clientY - rect.top];
    }
    function _pids() { return Object.keys(pointers); }
    function _dist(a, b) { var dx = a.x - b.x, dy = a.y - b.y; return Math.sqrt(dx * dx + dy * dy); }
    canvas.style.cursor = 'grab';
    canvas.addEventListener('pointerdown', function (ev) {
      var p = relPos(ev);
      pointers[ev.pointerId] = { x: p[0], y: p[1] };
      dragMoved = false;
      var ids = _pids();
      if (ids.length === 1) {
        lastX = p[0]; lastY = p[1];
        canvas.style.cursor = 'grabbing';
      } else if (ids.length === 2) {
        pinchDist = _dist(pointers[ids[0]], pointers[ids[1]]);
        dragMoved = true;  // a two-finger gesture is a zoom, not a tap
      }
      canvas.setPointerCapture && canvas.setPointerCapture(ev.pointerId);
    });
    canvas.addEventListener('pointermove', function (ev) {
      var p = relPos(ev);
      if (pointers[ev.pointerId]) { pointers[ev.pointerId].x = p[0]; pointers[ev.pointerId].y = p[1]; }
      var ids = _pids();
      if (ids.length >= 2) {
        // Pinch-zoom toward the midpoint of the first two pointers.
        var a = pointers[ids[0]], b = pointers[ids[1]];
        var mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2;
        var d = _dist(a, b);
        if (pinchDist > 0 && d > 0) zoomBy(state, d / pinchDist, mx, my);
        pinchDist = d;
        dragMoved = true;
        tip.style.display = 'none';
        draw(state, true);
        return;
      }
      if (ids.length === 1 && pointers[ev.pointerId]) {
        // Single-pointer pan.
        var dx = (p[0] - lastX) / state.cam.k, dy = (p[1] - lastY) / state.cam.k;
        if (Math.abs(p[0] - lastX) + Math.abs(p[1] - lastY) > 2) dragMoved = true;
        state.cam.x -= dx; state.cam.y -= dy;
        lastX = p[0]; lastY = p[1];
        tip.style.display = 'none';
        draw(state, true);
        return;
      }
      // No active pointer (mouse hover) -> tooltip.
      var idx = pick(state, p[0], p[1]);
      if (idx !== state.hover) {
        state.hover = idx;
        if (idx >= 0) {
          _clear(tip);
          tip.appendChild(buildTooltipContent(state, state.decoded.nodes[idx]));
          tip.style.display = 'block';
        } else {
          tip.style.display = 'none';
        }
        draw(state, false);
      }
      if (idx >= 0) {
        tip.style.left = Math.min(p[0] + 14, state.viewW - 250) + 'px';
        tip.style.top = Math.min(p[1] + 14, state.viewH - 90) + 'px';
      }
    });
    function endPointer(ev) {
      if (!(ev.pointerId in pointers)) return;
      var releasePos = relPos(ev);
      delete pointers[ev.pointerId];
      var ids = _pids();
      if (ids.length === 0) {
        canvas.style.cursor = 'grab';
        // A tap (single pointer, no drag/pinch) enters a focus constellation or
        // opens the manuscript pane.
        if (!dragMoved) {
          var idx = pick(state, releasePos[0], releasePos[1]);
          if (idx >= 0) {
            if (state.focusCluster >= 0) {
              openBrowsePane(state.decoded.nodes[idx].sys_id);
            } else {
              focusOn(state, idx);
              var _fnode = state.decoded.nodes[idx];
              var _fgrp = _fnode && state.domainGroups[_fnode.domain];
              _track('atlas_cluster_focused', {
                members: state.focusMembers ? state.focusMembers.length : 0,
                domain: (_fgrp && _fgrp[0]) || ''   // English domain label (cross-language stable)
              });
              // Wide: auto-open the member list (side drawer). Narrow: keep it
              // closed so the focused cluster is visible — the user opens the
              // bottom sheet via the "Titles" control. This also avoids the
              // mobile ghost-click where a panel row opened under the tap point.
              focusPanelOpen = !state.narrow;
              rebuildFocusPanel();
              redraw();
            }
          }
        } else {
          // A pan/pinch just ended. Labels (cluster + per-dot titles) are skipped
          // on drag frames for performance, so repaint once now that motion has
          // stopped — otherwise they'd stay hidden until the next interaction.
          redraw();
        }
      } else if (ids.length === 1) {
        // Dropped 2 -> 1: re-anchor the surviving finger so pan doesn't jump.
        lastX = pointers[ids[0]].x; lastY = pointers[ids[0]].y;
        pinchDist = 0;
      }
    }
    canvas.addEventListener('pointerup', endPointer);
    canvas.addEventListener('pointercancel', endPointer);

    // --- wheel zoom (toward cursor) ---
    canvas.addEventListener('wheel', function (ev) {
      ev.preventDefault();
      var p = relPos(ev);
      zoomBy(state, ev.deltaY < 0 ? 1.15 : 1 / 1.15, p[0], p[1]);
      tip.style.display = 'none';
      redraw();
    }, { passive: false });

    // --- keyboard: Esc leaves focus mode ---
    window.addEventListener('keydown', function (ev) {
      if (ev.key === 'Escape') {
        // #6: close the browse pane first if it is open; otherwise leave focus.
        if (closeBrowsePane()) return;
        if (state.focusCluster >= 0) {
          backToGalaxy(state);
          rebuildFocusPanel();
          redraw();
        }
      }
    });

    // --- toolbar wiring ---
    var _searchTrackTimer = null;
    search.addEventListener('input', function () {
      applySearch(state, search.value);
      redraw();
      // Debounced so a search "burst" logs once. Sends only the query LENGTH +
      // result count, never the raw text.
      if (_searchTrackTimer) clearTimeout(_searchTrackTimer);
      _searchTrackTimer = setTimeout(function () {
        var q = (search.value || '').trim();
        if (q) _track('atlas_search', {
          query_len: q.length,
          matches: state.matchSet ? state.matchSet.size : 0
        });
      }, 700);
    });
    colorBtn.onclick = function () {
      state.colorBy = (state.colorBy === 'domain') ? 'library' : 'domain';
      colorBtn.textContent = (state.colorBy === 'domain')
        ? (state.labels.colorByLibrary || 'Color by library')
        : (state.labels.colorByDomain || 'Color by domain');
      rebuildLegend();
      redraw();
    };
    libBtn.onclick = function () {
      var open = libPanel.style.display === 'flex';
      libPanel.style.display = open ? 'none' : 'flex';
      if (!open) rebuildLibPanel();
    };
    zoomInBtn.onclick = function () { zoomBy(state, 1.3, state.viewW / 2, state.viewH / 2); redraw(); };
    zoomOutBtn.onclick = function () { zoomBy(state, 1 / 1.3, state.viewW / 2, state.viewH / 2); redraw(); };
    resetBtn.onclick = function () { backToGalaxy(state); rebuildFocusPanel(); fitView(state); redraw(); };

    // #4: show/hide the domain-title layer (button label flips with the state).
    labelsBtn.onclick = function () {
      state.showLabels = !state.showLabels;
      labelsBtn.textContent = state.showLabels
        ? (state.labels.hideDomainLabels || 'Hide domain labels')
        : (state.labels.showDomainLabels || 'Show domain labels');
      redraw();
    };

    // #3: fullscreen the reserved box. On enter/exit we resize the backing
    // store to the new viewport and repaint, and flip the button label.
    // Vendor-prefixed fallbacks cover older WebKit.
    _injectAtlasStyleOnce();
    if (box.classList && box.classList.add) box.classList.add('atlas-fs-box');
    function _isFs() {
      return document.fullscreenElement === box || document.webkitFullscreenElement === box;
    }
    function _reqFs(el) {
      if (el.requestFullscreen) return el.requestFullscreen();
      if (el.webkitRequestFullscreen) return el.webkitRequestFullscreen();
    }
    function _exitFs() {
      if (document.exitFullscreen) return document.exitFullscreen();
      if (document.webkitExitFullscreen) return document.webkitExitFullscreen();
    }
    fsBtn.onclick = function () {
      try { if (_isFs()) { _exitFs(); } else { _reqFs(box); } }
      catch (e) { /* fullscreen may be blocked by the environment; non-fatal */ }
    };
    function _onFsChange() {
      var on = _isFs();
      fsBtn.textContent = on
        ? (state.labels.exitFullScreen || 'Exit full screen')
        : (state.labels.fullScreen || 'Full screen');
      _track('atlas_fullscreen', { on: on });
      // The box (hence the canvas) just changed size — re-fit after the layout
      // settles, then repaint.
      setTimeout(function () { resizeCanvas(state); redraw(); }, 60);
    }
    if (document.addEventListener) {
      document.addEventListener('fullscreenchange', _onFsChange);
      document.addEventListener('webkitfullscreenchange', _onFsChange);
    }

    rebuildLegend();

    // Reposition the focus panel (bottom sheet <-> side drawer) on viewport /
    // orientation change. init's resize listener (registered earlier) refreshes
    // state.narrow via resizeCanvas before this fires.
    window.addEventListener('resize', function () {
      if (state.focusCluster >= 0) rebuildFocusPanel();
    });

    // --- reduced-motion-aware bloom-in intro (skippable) ---
    runIntro(state, box, redraw);
  }

  // Small toolbar button (textContent label).
  function _btn(label) {
    var b = _el('button', 'atlas-btn');
    b.textContent = label;
    b.setAttribute('type', 'button');
    b.setAttribute('style',
      'padding:5px 10px;border-radius:6px;border:1px solid #33405c;background:#141d33cc;' +
      'color:#dfe8f5;font-size:12px;cursor:pointer;');
    return b;
  }
  // Legend swatch: a colored square + a text label (textContent).
  function _swatch(color, label) {
    var wrap = _el('span', 'atlas-legend-item');
    wrap.setAttribute('style', 'display:inline-flex;align-items:center;gap:4px;');
    var sw = _el('span');
    sw.setAttribute('style',
      'display:inline-block;width:10px;height:10px;border-radius:2px;background:' + color + ';');
    wrap.appendChild(sw);
    wrap.appendChild(_txt('span', label));
    return wrap;
  }
  function _chipStyle(off) {
    return 'display:inline-flex;align-items:center;padding:3px 7px;border-radius:12px;cursor:pointer;' +
      'font-size:11px;border:1px solid #33405c;' +
      (off ? 'background:#0d1526;color:#67708a;opacity:0.5;' : 'background:#1c2334;color:#dfe8f5;');
  }

  // Fullscreen sizing CSS (#3), injected once. In fullscreen the reserved box
  // (and its canvas) must fill the whole screen; :fullscreen + the WebKit
  // prefix are written as SEPARATE rules because an unknown pseudo in a
  // selector list drops the whole rule in some engines.
  function _injectAtlasStyleOnce() {
    // Progressive enhancement: skip silently if the DOM surface is missing (a
    // minimal SSR/test document has no head/documentElement) — fullscreen still
    // works, just without the fill-screen sizing rule.
    try {
      if (!document || typeof document.getElementById !== 'function') return;
      if (document.getElementById('atlas-decode-style')) return;
      var host = document.head || document.documentElement;
      if (!host || typeof host.appendChild !== 'function') return;
      var st = document.createElement('style');
      st.id = 'atlas-decode-style';
      st.textContent =
        '.atlas-fs-box:fullscreen{width:100vw !important;height:100vh !important;border-radius:0 !important;}' +
        '.atlas-fs-box:fullscreen>canvas{height:100% !important;}' +
        '.atlas-fs-box:-webkit-full-screen{width:100vw !important;height:100vh !important;border-radius:0 !important;}' +
        '.atlas-fs-box:-webkit-full-screen>canvas{height:100% !important;}' +
        // On narrow screens the ~46% browse drawer is unreadably thin, so let
        // it cover the whole atlas (a full-screen manuscript viewer with a
        // Close button in its header). #6 mobile.
        '@media (max-width:640px){.atlas-browse-pane{width:100% !important;}}';
      host.appendChild(st);
    } catch (e) { /* non-fatal */ }
  }

  // Bloom-in intro: a count-up of the node total, skippable by click. Honors
  // prefers-reduced-motion — under reduce, the final count shows immediately
  // with no animation, then the overlay clears.
  function runIntro(state, box, redraw) {
    var reduce = false;
    try {
      reduce = !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
    } catch (e) { reduce = false; }

    var overlay = _el('div', 'atlas-intro');
    overlay.setAttribute('style',
      'position:absolute;inset:0;z-index:20;display:flex;flex-direction:column;' +
      'align-items:center;justify-content:center;background:#05070de6;cursor:pointer;');
    var big = _txt('div', '0', 'atlas-intro-n');
    big.setAttribute('style', 'font-size:min(11vw,96px);font-weight:700;color:#cfe3ff;');
    var sub = _txt('div', state.labels.connections || 'Connections', 'atlas-intro-l');
    sub.setAttribute('style', 'color:#8fb2d8;font-size:15px;margin-top:6px;');
    var skip = _txt('div', state.labels.skipIntro || 'Skip intro', 'atlas-intro-s');
    skip.setAttribute('style', 'margin-top:20px;color:#67708a;font-size:12px;');
    overlay.appendChild(big);
    overlay.appendChild(sub);
    overlay.appendChild(skip);
    box.appendChild(overlay);

    var total = state.decoded.nodes.length;
    var done = false;
    function finish() {
      if (done) return;
      done = true;
      if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
      redraw();
    }
    overlay.onclick = finish;

    if (reduce) {
      big.textContent = total.toLocaleString();
      finish();
      return;
    }
    var start = null, dur = 900;
    function step(ts) {
      if (done) return;
      if (start == null) start = ts;
      var t = Math.min(1, (ts - start) / dur);
      var eased = 1 - Math.pow(1 - t, 3);
      big.textContent = Math.round(total * eased).toLocaleString();
      if (t >= 1) { finish(); return; }
      window.requestAnimationFrame(step);
    }
    window.requestAnimationFrame(step);
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
    init: init,
    setDocument: setDocument,
    buildTooltipContent: buildTooltipContent,
    buildFocusRow: buildFocusRow
  };
});
