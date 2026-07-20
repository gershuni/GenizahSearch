---
phase: 133-visual-atlas-preview-early-quick-win
plan: 04
subsystem: web
tags: [atlas, canvas-2d, typed-arrays, decoder, xss-safe-dom, i18n, rtl, render-smoke, node-test]

# Dependency graph
requires:
  - phase: 133-01
    provides: "scripts/check_atlas_masking.py (--scan-repo run before every commit); atlas_data/ gitignored + off /static"
  - phase: 133-03
    provides: "web/pages/atlas.py chrome + documented renderer injection point; /atlas-data/manifest.json + /atlas-data/<asset_basename>.bin fetch contract; pre-registered HE atlas translation keys"
provides:
  - "web/static/js/atlas_decode.js — self-contained UMD renderer (browser + Node): frozen-schema binary decoder (decodeAtlas) + XSS-safe DOM builders (buildTooltipContent/buildFocusRow) + Canvas 2D draw + full D-08 interaction layer"
  - "web/pages/atlas.py::_inject_atlas_renderer() — loads /static/js/atlas_decode.js + hands it a tr()'d, language-aware config (manifest URL + data base as data, never the bytes)"
  - "tests/render_smoke/test_atlas_render_smoke.py — server-render smoke (chrome + CLS-reserved canvas + decoder injection + EN/HE + RTL)"
  - "tests/atlas_bake/test_atlas_golden_js.py — Node cross-language golden decode (JS == Python per-field) + DOM-XSS neutralization + static no-innerHTML guard"
affects: [133-05, 133-06]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Frozen-schema binary decode in JS mirroring the Python reference decoder field-for-field (typed-array views by section-table dtype, delta-decoded edges, BigUint64 sys_id via .toString() — single path, no fallback)"
    - "UMD module loadable by BOTH the browser page and a Node test harness (module.exports / window.AtlasDecode) so the SAME decode + DOM-builder code is proven cross-language"
    - "XSS-safe DOM (HIGH-7): every catalogue-derived node built via document.createElement + .textContent (setDocument() injectable for the Node DOM-XSS test); zero innerHTML"
    - "Payload never inlined (Pitfall #3): fetch /atlas-data/manifest.json -> read asset_basename -> fetch content-hashed .bin -> decode client-side"
    - "Client-side config handoff: Python tr()'s the D-15 bilingual labels + language + manifest URL into a JSON config; the JS owns all drawing + DOM building"

key-files:
  created:
    - web/static/js/atlas_decode.js
    - tests/render_smoke/test_atlas_render_smoke.py
    - tests/atlas_bake/test_atlas_golden_js.py
  modified:
    - web/pages/atlas.py

key-decisions:
  - "Renderer shipped as a static /static/js/atlas_decode.js module (served via the existing public /static mount — it carries NO data; the data comes from the flag+readiness-gated /atlas-data routes) rather than an inline <script>, so the exact same file is require()'d by the Node golden/DOM-XSS tests (cross-language proof)"
  - "Interaction state (matchSet / libHidden / focusCluster / hover / colorBy) is initialized inert in Task 1's makeState so draw() already respects it; Task 2 only wires the event handlers that mutate it — keeping draw() stable across the two commits"
  - "Overview draws baked aggregate community flows between node-derived cluster centroids (computeClusterCentroids) rather than only the >=25-member labeled clusters, so flows referencing unlabeled clusters still render (D-09)"
  - "The Node golden test compares floats with strict === (Float32->Float64 widening is exact + deterministic across Python/JS) and sys_id via BigInt(str) (no precision loss above 2^53); a generic per-field array comparator catches ANY encoder/decoder drift (T-133-16)"
  - "sys_id decoded as BigUint64Array only, emitted via .toString() — a single code path, no string-heap/alternate fallback (schema §7 / Codex NEW LOW)"

requirements-completed: [ATLAS-01]

# Metrics
duration: 55min
completed: 2026-07-21
---

# Phase 133 Plan 04: Atlas Canvas 2D Renderer — Decoder, Interactions & XSS-safe DOM Summary

**Filled the 133-03 renderer contract: `web/static/js/atlas_decode.js` is a self-contained UMD module that fetches the manifest + content-hashed asset (never inline), decodes it field-for-field against the FROZEN schema (BigUint64 sys_id, single path), and draws the domain-colored galaxy (overview aggregate flows + stars, per-MS edges on zoom) with the full D-08 interactive experience — zoom/pan, title+shelfmark search, domain↔library color toggle, library hide/solo filter, click-a-region focus constellation, reduced-motion-aware skippable bloom-in intro, and same-origin `/browse?sys_id=` click-through — with the gold discovery overlay stripped (D-04), bilingual domain labels (D-15), and EVERY catalogue-derived DOM node built XSS-safe via createElement/textContent (HIGH-7). Cross-language decode (JS == Python per-field) and DOM-XSS neutralization (the fabricated malicious fixture string renders as inert text, never innerHTML) are proven by Node tests; server render (chrome/CLS/EN-HE/RTL/injection) by a render-smoke.**

## Performance

- **Duration:** ~55 min
- **Completed:** 2026-07-21
- **Tasks:** 3/3
- **Files:** 4 (3 created: web/static/js/atlas_decode.js, tests/render_smoke/test_atlas_render_smoke.py, tests/atlas_bake/test_atlas_golden_js.py; 1 modified: web/pages/atlas.py)

## Accomplishments

- **Frozen-schema decoder (`decodeAtlas`).** Reads the 16-byte header (verifies `ATLAS001` magic + `schema_version==1`), the 32-byte section-table entries (dtype/elem_size/count/offset all read from the table — never assumed, honoring the dynamic Uint16/Uint32 cluster/flow dtype), then constructs typed-array views over the `ArrayBuffer`: Float32 positions, node cluster/domain/library/prominence indices, **BigUint64 `sys_id` via `.toString()` (single code path, no fallback — §7)**, `(offset,length)` UTF-8 string-heap slices for title/shelfmark/cluster-label title+dom, delta-decoded edge source/target pairs + the Uint8 class byte (§6), and aggregate flows. Output object mirrors the Python reference `decode_asset` field-for-field.
- **Canvas 2D draw.** Camera (`toScreen`/`toWorld`/`fitView`) + `draw()`: dark radial background, overview aggregate community flows (between node-derived centroids) + domain-colored stars, per-MS edges once zoomed past `EDGE_ZOOM` (D-09), on-canvas cluster region labels; focus mode dims the rest and draws the constellation's own edges + members. Default coloring is FJMS domain (D-02). **No discovery/gold overlay anywhere (D-04).**
- **XSS-safe DOM builders (HIGH-7).** `buildTooltipContent` (shelfmark + catalogue title + domain + library — D-06) and `buildFocusRow` build every node with `document.createElement` + `.textContent`; `setDocument()` lets the Node test inject a fake document. **Zero `innerHTML` in the module** — containers are cleared via `removeChild`, never `innerHTML=''`.
- **Full D-08 interactions.** Wheel zoom-to-cursor, drag pan, hover→tooltip (grid-based `pick`), title+shelfmark+sys_id search filter (never domain/library — would flood), domain↔library color toggle (never both — D-02) with legend rebuild, library filter panel (click hide-one / **shift-click solo-one**), click-a-region → focus constellation member panel (rows → `/browse`), Esc back-to-galaxy, and same-origin click-through `window.open(window.location.origin + '/browse?sys_id=' + sysId.toString(), '_blank')` (Pitfall #4). Legend keeps **continuation vs island (citation/quotation)** distinct — never a physical join (Pitfall #2). Domain labels select EN/HE by UI language (D-15).
- **Reduced-motion bloom-in intro.** A skippable count-up overlay gated by `window.matchMedia('(prefers-reduced-motion: reduce)')` — under reduce it shows the final count immediately with no animation.
- **Page wiring (`web/pages/atlas.py`).** `_inject_atlas_renderer()` loads `/static/js/atlas_decode.js` and a bootstrap that polls for `window.AtlasDecode` (the module loads async; NiceGUI mounts the canvas after the socket connects) then calls `AtlasDecode.init(config)`. The config carries the manifest URL + data base **as data (never the bytes)**, the UI language/RTL, and the tr()'d D-15 bilingual label set.
- **Tests.** Render-smoke (2) proves the server-render surface only (chrome + CLS-reserved 720px canvas + decoder/manifest injection + EN/HE with RTL/LTR) — explicitly NOT fetch/decode/Canvas (MEDIUM-2). Node atlas_bake (3): cross-language golden decode (per-field == Python, sys_id via `BigInt(str)`), DOM-XSS neutralization (malicious fixture → textContent, zero innerHTML), and a pure-Python static no-innerHTML guard over both `atlas_decode.js` and `atlas.py`.

## Task Commits

Each task committed atomically with explicit-path staging (never `git add -A`); masking `--scan-repo` exited 0 before each commit.

1. **Task 1: decoder JS + draw (frozen-schema decode, fetch manifest+content-hashed asset, galaxy/flows/stars, overlay stripped)** — `36d14d6b` (feat)
2. **Task 2: interactions + XSS-safe DOM (zoom/pan, search, color toggle, library filter, focus constellation, reduced-motion intro, click-through)** — `06f5d029` (feat)
3. **Task 3: render-smoke + cross-language golden decode + DOM-XSS neutralization + static no-innerHTML guard** — `9dae1396` (test)

**Plan metadata:** this SUMMARY + STATE/ROADMAP/REQUIREMENTS — see the final docs commit.

## Environment Note (Node availability for the JS tests)

**Node IS available locally (`v24.14.0`), so both Node-driven `atlas_bake` tests RAN FOR REAL** (not skipped) and passed:
- `test_js_golden_decode_matches_python` — JS decode of the committed `golden-v1.bin` equals `golden-v1-expected.json` per-field (schema_version + all 40 nodes + 46 edges + 1 flow + 1 cluster label; sys_id via BigInt, floats exact).
- `test_js_dom_xss_neutralized` — the fabricated malicious fixture title (an `<img ... onerror=...>` / `</script>` / RTL-override-bidi string, located at runtime by its `onerror=` shape) is assigned to `.textContent` in both `buildTooltipContent` and `buildFocusRow`, with **zero** `.innerHTML` assignments.
The same tests also run in the dedicated `atlas-bake-tests` CI job (`actions/setup-node`). They `pytest.skip` cleanly (never fake a pass) where `node` is absent. The static no-innerHTML guard is pure Python and always runs.

## Decoder module export shape (for the Node test)

UMD: `module.exports` in Node, `window.AtlasDecode` in the browser. Exports:
`decodeAtlas(arrayBuffer) -> {schema_version, nodes, edges, flows, cluster_labels}`,
`setDocument(doc)`, `buildTooltipContent(state, node) -> Element`,
`buildFocusRow(state, node) -> Element`, plus `init(config)`, `draw`, `fetchDecoded`,
`nodeColor`, `domainLabel`, `libraryLabel`, `computeClusterCentroids`, `computeBounds`, `SEC`.

## Single BigUint64 sys_id decode path

`NODE_SYS_ID` is read via `BigUint64Array` and emitted with `.toString()`. There is exactly one code path — the frozen schema (§7) guarantees pure-digit < 2^64 (the bake fails otherwise), so there is no string-heap/alternate/Number() fallback branch. This keeps `/browse?sys_id=` links lossless above 2^53.

## Deviations from Plan

### Prototype adaptations (expected by the plan — "port the interaction LOGIC, not the DOM")

- **Overlays repositioned from `position:fixed` fullscreen (prototype) to `position:absolute` inside the reserved 720px `#atlas-canvas` parent box** — the atlas is embedded in the shared shell, not a standalone fullscreen HTML. No behavior change; required by the embed architecture (Pattern 1).
- **Prototype `innerHTML`-based focus rows / legend / library panel rebuilt DOM-safely** via `createElement`/`textContent` (HIGH-7) — the explicit transformation the plan mandated; the verbatim port would have carried a DOM-XSS sink onto a public page.
- **Gold discovery overlay (SP_GOLD, `showDisc`, per-MS gold stars, discovery counts) fully stripped** (D-04) — the payload has no discovery field.
- **Data source swapped from the prototype's inline `__DATA__` blob to a manifest+asset fetch** (Pitfall #3).
- **Click-through base** changed from the prototype's hardcoded `https://genizahsearch.com` input field to `window.location.origin` (Pitfall #4, RESEARCH note) — the atlas now ships same-origin.

### Auto-fixed Issues

**None (Rules 1-3).** No bugs, missing critical functionality, or blocking issues required inline fixes beyond the planned prototype transformations. Two pre-commit ruff F401s (unused `json` / `pytest` imports in the new test files) were removed before the Task 3 commit (routine lint hygiene, not a behavior change).

**Total deviations:** prototype adaptations only (all mandated by the plan). No auto-fixed bugs; no architectural (Rule 4) escalations.

## Known Stubs

None. The renderer is fully implemented against the live fetch/decode contract. (The real production bake asset is uploaded in plan 133-06; the smoke asset already in the gitignored `atlas_data/` makes `/atlas` live locally with `ATLAS_PREVIEW_ENABLED=1`.)

## Threat Flags

None. No new network endpoint, auth path, file-access pattern, or schema change was introduced — the renderer consumes the existing flag+readiness-gated `/atlas-data/*` routes and the FROZEN schema. The DOM-XSS surface (T-133-15) is mitigated and proven; edge semantics (T-133-11) keep continuation/island distinct (never a join).

## Live-UAT Checklist (plan 133-06 — the part headless/Node tests cannot cover)

With `ATLAS_PREVIEW_ENABLED=1` and a baked asset in `atlas_data/`, open `/atlas` and confirm:
1. The galaxy draws (domain-colored stars + overview flows); zoom in → per-MS continuation/island edges appear; cluster region labels render.
2. Zoom (wheel + +/− buttons) and pan (drag) are smooth; **Reset view** re-fits.
3. **Search** by title/shelfmark narrows the visible stars + labels; clearing restores.
4. **Color toggle** recolors by library (never both at once); the legend updates.
5. **Library filter** panel: click hides one library; **shift-click** solos one; **Show all** restores.
6. Click a region → **focus constellation** member panel; a member row (and a star click in focus) opens `/browse?sys_id=…` in a new tab with the correct manuscript. Esc returns.
7. Hover tooltip shows shelfmark + domain + library + catalogue title only.
8. The **bloom-in intro** animates and is click-skippable; with OS "reduce motion" ON it shows the final count immediately (no animation).
9. **EN/HE**: domain labels + all chrome switch language; HE is RTL.
10. **CLS**: no layout shift when the renderer attaches (canvas box is pre-reserved at 720px).
11. Phase-exit masking: the browser-captured client DOM + rendered `/atlas` HTML feed `check_atlas_masking.py --scan-asset` → exits 0.

## Self-Check: PASSED

- FOUND: web/static/js/atlas_decode.js
- FOUND: web/pages/atlas.py
- FOUND: tests/render_smoke/test_atlas_render_smoke.py
- FOUND: tests/atlas_bake/test_atlas_golden_js.py
- FOUND: .planning/phases/133-visual-atlas-preview-early-quick-win/133-04-SUMMARY.md
- FOUND: commit `36d14d6b` (Task 1)
- FOUND: commit `06f5d029` (Task 2)
- FOUND: commit `9dae1396` (Task 3)

---
*Phase: 133-visual-atlas-preview-early-quick-win*
*Completed: 2026-07-21*
