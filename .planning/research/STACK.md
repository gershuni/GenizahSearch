# Stack Research

**Domain:** Discovery module for a scholarly manuscript web app (NiceGUI) — same-work identification tables + a corpus connection/network visualization ("Atlas") over ~52K manuscripts / ~4K works / hundreds of thousands of edges. Web-only, v9.0.0.
**Researched:** 2026-07-19
**Confidence:** HIGH (grounded in the existing working full-corpus atlas prototype + the installed dependency baseline + verified library versions/licenses; MEDIUM on exact browser-renderer node-count thresholds, which are corroborated across sources but not benchmarked on this corpus)

---

## TL;DR verdict

The discovery module needs **almost no new runtime stack.** Split the visualization into three surfaces and match each to the lightest sufficient tool:

| Surface | Scale rendered at once | Recommendation | New runtime dep? |
|---------|------------------------|----------------|------------------|
| MS connections panel (ego-network on browse) | tens–low hundreds of nodes | **`ui.echart` graph series** (native, bundled) | **None** |
| Work → witnesses browse | table (0 graph) | **plain NiceGUI** table/list | **None** |
| Interactive graph explorer (server-extracted bounded neighborhood) | ≤ ~1–2K nodes | **`ui.echart` graph series** (native) | **None** |
| Whole-corpus "stun" Atlas (all ~52K nodes) | ~52K nodes / 100Ks edges | **Reuse the existing precomputed-layout Canvas 2D starfield, served as a static asset + embedded via `<iframe>`** | **None** (escalate to sigma.js only if live interop over all 52K is required) |

The one genuinely load-bearing decision: **graph layout for the whole-corpus view MUST be precomputed offline (Python, on the dev box) and baked to a positions asset.** No browser library force-lays-out 52K nodes / hundreds of thousands of edges interactively. This is already how the prototype works (`same_work_spike/probe/scripts/build_atlas_draft.py`), and the layout/community libraries it uses (`networkx`, `python-louvain`, `python-igraph`, `leidenalg`) are installed on the dev box but **deliberately absent from `requirements.txt`/`requirements-lock.txt`** — that separation is correct and must be preserved.

---

## Recommended Stack

### Core Technologies (for the NEW capabilities only)

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| **NiceGUI `ui.echart`** (Apache ECharts) | NiceGUI **3.8.0** (installed; `ui.echart` present since 1.2.8), ECharts **5.x** bundled | Render bounded graphs: the MS ego-network panel + the interactive explorer's server-extracted neighborhoods | Already in the framework — **zero new dependency, zero added prod RAM, zero JS to vendor/audit.** Accepts any ECharts option dict incl. `series:[{type:'graph', layout:'force'\|'circular'\|'none'}]`; native Hebrew/RTL label rendering (canvas text); click interop via `on('chart:click')` / `on_point_click`; drive from Python via `run_chart_method`; `:`-prefixed JS formatters for custom labels. Comfortable to ~3K nodes on Canvas — well above any *bounded* neighborhood. |
| **Existing Canvas 2D starfield + precomputed layout** | in-repo (`build_atlas_draft.py`), no library | The whole-corpus flagship "Atlas" (all ~52K nodes, pan/zoom, LOD, click→/browse) | **Already built and proven at full corpus scale.** Server-precomputed positions + pre-rendered sprites + additive-blend draw loop + a spatial picking grid render 52K "stars" and level-of-detail edges smoothly. Self-contained HTML, no CDN, no runtime lib. Serve as a static/generated asset; prod cost is just file serving. |
| **SQLite (stdlib `sqlite3`)** | Python 3.10+ stdlib | The distilled discovery **product sidecar** (tier-A identifications, band memberships + confidence labels, connection edges, work metadata) **and** server-side bounded-subgraph extraction | Same read-only mmap'd sidecar pattern as `pgp.db`/`fjms_enrichment.db`/`nli_crossref.db`. An indexed edge table (`idx(sys_a)`, `idx(sys_b)`) supports N-hop ego-graph BFS in plain Python — no graph DB, no new runtime dep. Distilling the 3.08 GB `fullcorpus_v2.db` → a compact product DB is a plain build-time Python script. |
| **Supabase (existing)** | `supabase==2.28.0` (installed) | Community judgment capture (confirm / reject / annotate work-witness claims) | Confirmed: **no new stack.** Reuse the existing corrections/comments RLS + explicit-GRANT pattern (new table(s), 4/5-way verdict, `user.id` FK). Same auth, same `web/safe_storage.py` chokepoint discipline. |

### Supporting Libraries

#### Build-time only (offline, on the dev box — MUST NOT enter `requirements.txt`)

| Library | Version (installed) | License | Purpose | When to Use |
|---------|--------------------|---------|---------|-------------|
| **numpy** | 2.4.3 (already a prod dep) | BSD-3 | Vectorized force-layout refinement; typed-array position/edge asset emission | Layout precompute + baking positions to a compact binary/JSON asset |
| **scipy** | 1.17.1 (already a prod dep) | BSD-3 | `sparse.csgraph.connected_components` for the same-work backbone | Component decomposition before per-community layout |
| **networkx** | 3.6.1 (dev box only) | BSD-3 | Graph model, Louvain/ego-graph helpers, community split | Offline layout/community build; **optional** at runtime for ego-graph BFS (pure-Python, safe to add if wanted — see note below) |
| **python-louvain** (`community`) | 0.16 (dev box only) | **BSD** | Louvain community detection for cluster coloring | Offline community detection — **preferred** over igraph/leidenalg because BSD (no copyleft) |
| **python-igraph** | 1.0.0 (dev box only) | **GPL** | Fast graph ops / Leiden backing | Offline only; acceptable at build time. **Do not add to the shipped app** (see What NOT to Use) |
| **leidenalg** | 0.12.0 (dev box only) | **GPL** | Leiden community detection (higher-quality than Louvain) | Offline only, same GPL caveat |
| **zstandard** | 0.25.0 (already a prod dep) | BSD | Compress large text/blob columns in the sidecar if needed | Sidecar distillation, only if size matters |

> **Runtime note:** bounded-subgraph extraction can be done with **plain Python + SQLite adjacency BFS (zero new dep)**. If richer runtime graph ops are wanted (shortest paths, ego_graph), `networkx` (BSD-3, pure-Python, lightweight) is the only graph library safe to add to `requirements.txt`. Do **not** add igraph/leidenalg to runtime (GPL + native-compiled).

#### Escalation renderer (add ONLY if the whole-corpus view needs live WebGL interactivity beyond the canvas)

| Library | Version | License | Purpose | When to Use |
|---------|---------|---------|---------|-------------|
| **sigma.js** | **3.0.3** (stable; avoid the v4 **alpha**) | **MIT** | WebGL renderer for large graphs | Only if the whole-corpus Atlas must become a *live* pan/zoom/filter WebGL graph with tight NiceGUI node-click interop, exceeding the canvas prototype. Renders ~100K edges comfortably; still consumes **precomputed** positions (does not force-lay-out 52K live). |
| **graphology** | latest 0.26.x | **MIT** | Graph data model backing sigma.js | Ships with sigma.js; supply nodes with `{x, y}` precomputed positions |

### Development / Build Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| Offline layout builder | Precompute positions + communities → bake typed-array/JSON positions asset | Model directly on `same_work_spike/probe/scripts/build_atlas_draft.py`; runs on the dev box (12C/24T, 63 GB), **never on the memory-constrained prod EC2** |
| Sidecar distill script | 3.08 GB `fullcorpus_v2.db` → product SQLite sidecar | Plain Python + `sqlite3`; snapshot-ship + documented rebuild recipe (refresh pipeline out of scope per PROJECT.md) |
| Static asset serving | Serve the baked Atlas HTML / positions binary | Existing FastAPI route pattern (`web/api.py::init_api_routes`); `ui.element('iframe')` or `ui.html('<iframe>')` to embed in the NiceGUI page |

## Installation

```bash
# RUNTIME (web app): NOTHING NEW.
#   ui.echart ships with nicegui==3.8.0 (already pinned)
#   sqlite3 is stdlib; numpy/scipy/zstandard/supabase already pinned
#   -> requirements.txt is UNCHANGED for the core recommendation.

# BUILD-TIME ONLY (dev box; keep OUT of requirements.txt / requirements-lock.txt):
pip install networkx python-louvain   # BSD — layout + community precompute
# python-igraph / leidenalg already present on the dev box (GPL — offline only)

# ESCALATION (only if adopting sigma.js as a NiceGUI custom component):
#   vendor the JS bundle into web/static/ (do NOT rely on a CDN for a scholarly site):
npm pack sigma@3.0.3 graphology@latest graphology-layout-forceatlas2@latest
#   then commit the built bundle; no Python dependency is added.
```

## NiceGUI integration paths (concrete, per candidate)

| Candidate | How it embeds in NiceGUI | Interop (node click → /browse) | Verdict |
|-----------|--------------------------|--------------------------------|---------|
| **`ui.echart` graph series** | Native: `ui.echart({'series':[{'type':'graph','layout':'none','data':nodes,'links':edges,...}]})`. Positions passed as `x`/`y` with `layout:'none'` (precomputed), or `layout:'force'` for small live graphs. | Server-side: `.on('chart:click', handler)` → `ui.navigate.to(f'/browse?sys_id=...')`. Fully within NiceGUI's event loop. | **Recommended for bounded neighborhoods.** No custom component, no vendored JS. |
| **Custom Canvas 2D (existing prototype)** | Serve generated self-contained HTML via a FastAPI route; embed with `ui.element('iframe').props('src=/atlas/full')`. Data baked into the asset (or fetched as a binary typed-array). | Prototype already does `window.open(base+'/browse?sys_id='+id)`. Tighter NiceGUI interop via `window.postMessage` if needed. | **Recommended for the whole-corpus flagship.** Lowest risk — it exists and works at 52K. |
| **sigma.js v3 + graphology** | Custom NiceGUI Vue component: subclass `ui.element`, declare the vendored JS bundle as a component dependency, pass nodes/edges/positions as props, emit node-click via `emitEvent` → Python handler. | First-class via the component's emitted events. | **Escalation only.** Best-in-class WebGL scale, but adds a vendored bundle + a component to maintain. |
| **cytoscape.js** | Custom Vue component (same pattern as sigma). | First-class. | Alternative to sigma; see table below. |
| **vis-network** | Custom Vue component. | First-class. | Not recommended (see below). |

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| Canvas 2D starfield (whole-corpus) | **sigma.js v3** (WebGL, MIT) | When the whole-corpus Atlas must be a *live* WebGL graph (real-time pan/zoom over all 52K with dynamic filtering/highlighting and node-level interop) rather than a precomputed starfield. Sigma renders ~100K edges comfortably; still needs precomputed positions. |
| `ui.echart` (bounded views) | **cytoscape.js 3.34.0** (MIT) | When you need rich graph *styling/analysis* (compound nodes, complex selectors, built-in graph algorithms) on bounded neighborhoods and are willing to build a custom Vue component. Canvas renderer ~1–3K nodes; a WebGL renderer landed in preview (v3.31+). Heavier than `ui.echart` for our needs. |
| `ui.echart` (bounded views) | **d3-force** | When a fully bespoke, custom-drawn small interactive graph is needed. More control, much more code; only if `ui.echart`'s graph series proves too constraining. |
| Precompute layout in Python | **In-browser ForceAtlas2** (graphology-layout-forceatlas2, WebWorker) | Only for *small, live* subgraphs (hundreds of nodes) where a fresh layout per view is desired. Never for the full corpus. |
| python-louvain (BSD) | **leidenalg + python-igraph** (GPL) | When Leiden's higher community-quality is worth it AND the run stays strictly offline (build-time). Already used in `motif_v2_communities.py`. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| **Live in-browser force layout over the full 52K graph** (ECharts `layout:'force'`, cytoscape layouts, sigma ForceAtlas2 on all nodes) | Perf cliff: ECharts high-FPS ceiling is ~3K nodes; sigma's force layout degrades past ~50K *edges*; we have hundreds of thousands. Also produces a misleading "hairball." | **Precompute layout offline** (numpy + networkx/louvain), ship positions; render with `layout:'none'` |
| **`ui.echart` (or any DOM/SVG renderer) for the whole-corpus view** | ECharts/SVG choke at a few thousand nodes; SVG/DOM cannot hold 52K nodes + 100Ks edges | Canvas 2D starfield (WebGL sigma if live interactivity is required) |
| **Adding `python-igraph` / `leidenalg` to `requirements.txt`** | **GPL** copyleft in a distributed context; also native-compiled → heavier install/CI. They are build-time layout tools, not runtime. | Keep them dev-box-only; prefer BSD `python-louvain`/`networkx` for anything that could drift toward runtime |
| **Neo4j / any graph database** | The graph is static, read-only, and fits in SQLite; a graph DB is operational overhead (a new service, new auth, new backups) on a memory-constrained shared box with zero payoff | Indexed SQLite edge table + Python BFS for bounded-subgraph extraction |
| **Plotly / Dash / Bokeh / Streamlit** | Redundant with NiceGUI; a second UI/dashboard stack, heavier payloads, and a duplicate rendering path | `ui.echart` (native) + the Canvas asset |
| **A separate React/Vue SPA frontend** | NiceGUI already supports custom Vue components for the one place we might need them (sigma); a full SPA fractures the app | NiceGUI page + optional single custom component |
| **3D graphs (three.js / echarts-gl 3D graph)** | 3D node-link "wows" but hurts legibility, honesty, and accessibility — the brief explicitly requires *scholarly-credible, not misleading*; occlusion + no stable reading order | 2D precomputed layout with domain/library color + glow |
| **CDN-loaded graph libraries** | A public scholarly site must not depend on third-party CDN availability/privacy; also breaks offline/air-gapped review builds | Vendor any JS (sigma/graphology) into `web/static/` and commit it |
| **A new DB engine or ORM for the sidecar** | The sidecar is a read-only reference file exactly like the existing sidecars | stdlib `sqlite3`, no ORM |
| **Upgrading NiceGUI to 3.14.0 for this milestone** | `ui.echart` is fully present in the pinned 3.8.0; a framework-wide bump is a separate, risky change touching the whole app | Stay on `nicegui==3.8.0`; scope any bump to its own task |

## Stack Patterns by Variant

**If the whole-corpus Atlas stays a "stun + explore-then-drill" experience (browse, pan/zoom, click → /browse):**
- Use the **existing Canvas 2D starfield** with an offline-precomputed positions asset, embedded via `<iframe>`.
- Because it is already proven at 52K scale, adds no dependency, and costs the prod box only static file serving.

**If product feedback demands a *live* whole-corpus graph (dynamic filter/highlight/select across all 52K with server round-trips per node):**
- Escalate to **sigma.js v3 + graphology** as a single NiceGUI custom Vue component; keep positions precomputed offline.
- Because sigma's WebGL renderer is the only browser option that handles 100K edges interactively.

**If the interactive explorer is scoped as "pick a manuscript/work → server extracts an N-hop neighborhood → render it":**
- Use **`ui.echart` graph series** with `layout:'none'` (precomputed) or `layout:'force'` (small live), server-side BFS over the SQLite edge table.
- Because bounded neighborhoods are ≤ ~1–2K nodes — comfortably inside ECharts' native envelope, with zero new dependency.

**If community detection quality matters for cluster coloring:**
- Prefer **python-louvain (BSD)** for a clean-license default; use **leidenalg (GPL)** offline only when Leiden's quality is needed.

## Version Compatibility

| Package A | Compatible With | Notes |
|-----------|-----------------|-------|
| `nicegui==3.8.0` | ECharts 5.x (bundled), FastAPI 0.135.1, Starlette 0.52.1, uvicorn 0.41.0 | `ui.echart` + `ui.element('iframe')` + custom Vue components all supported; no upgrade required |
| `numpy==2.4.3` | `scipy==1.17.1`, `networkx>=3.4` | Already the prod baseline; networkx 3.6.1 (dev) is numpy-2-compatible |
| `sigma@3.0.3` | `graphology@0.26.x`, `graphology-layout-forceatlas2` | Use v3 **stable**; sigma v4 is alpha — do not adopt until GA |
| `python-igraph==1.0.0` | `leidenalg==0.12.0` | leidenalg links against igraph; pin together if used offline |
| product sidecar (SQLite) | same mmap/read-only pattern as `pgp.db`/`fjms_enrichment.db` | Ships as a bundled read-only reference file; WAL not needed (read-only) |

## Sources

- Repo: `same_work_spike/probe/scripts/build_atlas_draft.py`, `build_rehearsal_atlas.py`, `motif_v2_communities.py`, `same_work_spike/probe/results/CODEX-BRIEF-atlas.md`, `ROAD2-DESIGN-OPTIONS.md` — **HIGH** (first-party working prototype: Canvas 2D + precomputed numpy/networkx/louvain layout at 52K/1.34M scale; confirms scale and constraints)
- `requirements.txt` + `requirements-lock.txt` (nicegui 3.8.0, numpy 2.4.3, scipy 1.17.1, supabase 2.28.0, zstandard 0.25.0) and `pip index/show` (networkx 3.6.1, python-louvain 0.16 BSD, python-igraph 1.0.0 GPL, leidenalg 0.12.0 — all dev-box-only, absent from prod deps) — **HIGH** (installed-environment ground truth)
- https://nicegui.io/documentation/echart — `ui.echart` events (`on('chart:click')`, `on_point_click`), `run_chart_method`, `:`-prefixed JS formatters — **HIGH**
- https://github.com/jacomyal/sigma.js + https://www.sigmajs.org/ — WebGL, "thousands of nodes," ~100K edges, accepts precomputed positions, v4 alpha — **HIGH**; sigma@3.0.3 latest stable, MIT — **MEDIUM** (version via npm listing; MIT is well-established)
- https://graphology.github.io/ + https://github.com/graphology/graphology — MIT, data backend for sigma.js — **HIGH**
- https://github.com/cytoscape/cytoscape.js/releases + https://js.cytoscape.org/ + WebGL preview blog — v3.34.0 (2026-06), MIT, canvas ~1–3K nodes / WebGL renderer preview — **HIGH**
- Apache ECharts docs + PMC "Graph visualization efficiency of popular web-based libraries" (PMC12061801) — ECharts-Canvas high-FPS to ~3K nodes; D3-WebGL ~7K; motivates precompute for large graphs — **MEDIUM** (cross-source corroborated, not benchmarked on this corpus)
- `.planning/PROJECT.md` (v9.0.0 milestone) + `.planning/seeds/SEED-029-*.md` — scope, corpus counts (52,497 MSS / 4,093 works / 275,894 tier-A ids), sidecar/distillation + community-judgment requirements — **HIGH**

---
*Stack research for: NiceGUI discovery module — same-work identification + corpus connection Atlas*
*Researched: 2026-07-19*
