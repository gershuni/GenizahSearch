# Phase 133: Visual Atlas Preview (early quick win) - Research

**Researched:** 2026-07-20
**Domain:** Offline graph-layout bake (Python/networkx/Louvain) + static asset packaging (typed arrays + Brotli) + NiceGUI page/route integration
**Confidence:** HIGH (prototype fully read and executed in this session; live byte measurements taken; NiceGUI/Starlette internals inspected directly; provenance-masking gap independently confirmed by direct SQL query)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** The atlas's primary rendered object is the **manuscript** — each star = one connected manuscript (`sys_id`), positioned by the offline force-layout of its algorithmic community; communities read as luminous "regions." This is the **durable** primary object for the whole atlas (preview → the Phase 139 explorer), not just the preview. Works-as-nodes are **rejected as the primary object** (a work node asserts work–witness identification = claim-level, forbidden here; Codex also warns against making the projected manuscript clique primary evidence). A work-lens / bipartite (manuscripts↔works) view remains a possible **future secondary** view.
- **D-02:** Default categorical color = **FJMS domain** (מקרא / פיוט / הלכה / …); **library** is an alternate recoloring **toggle** (never simultaneous). Regions are framed honestly as **"algorithmically detected regions,"** labeled by dominant domain composition — never as works, genres, or historical schools.
- **D-03:** The ~43K liturgical **"giant" component is recursively sub-divided** (Louvain split, prototype `SPLIT_AT≈800`) into legible sub-regions rather than shown as one illegible blob.
- **D-04:** **Remove the gold "discovery candidates" overlay entirely** — the toggle, the per-MS gold stars, and any discovery counts. Per-manuscript discovery highlighting asserts an identification status = a claim-level statement, which ATLAS-01 forbids in the preview. Discoveries return in the certified claim surfaces (Phases 136–138).
- **D-05:** **Keep catalogue manuscript titles as region labels, as-is** (no special "representative/cluster-label" framing wording). This is a **deliberate, informed owner decision** under the ATLAS-PREVIEW exception. Rationale: catalogue titles come from `libraries.csv` (**our own catalogue data — masking-safe, not M-source**), and SC#1 explicitly permits "cluster-level visualization." The standing algorithmic-provenance banner (D-15) still applies. **NOTE FOR VERIFIER:** this is intentional — do NOT flag it as SC#1 "work-identification" non-compliance; the owner weighed the "region = work" reading and accepted it.
- **D-06:** Per-star hover tooltips keep that manuscript's own **shelfmark + domain + library + catalogue title** (our catalogue data, masking-safe). Shelfmark labels surface on deep zoom.
- **D-07:** The masking check is a **reusable scan script — the forerunner of the permanent DATA-05 CI guard**. It scans **both** (a) the built atlas asset (HTML/JSON, every embedded string incl. region titles + tooltips) and the page's rendered output, **and** (b) committed repo content, for the M-source name/aliases + reference-corpus sigla patterns. Run as a Phase-133 exit gate; Phase 134 extends it to the sidecar. Its first run performs the one-time committed-repo cleanup verification (any uncommitted prototype M-source strings — e.g. the `genizah_translations.py` working-tree additions — must be scrubbed before commit).
- **D-08:** Ship the **full interactive prototype experience**: zoom/pan, title + shelfmark search, domain↔library color toggle, library-filter panel (hide-one / solo-one), click-a-region → focus "constellation" (member-list panel), **click-through to `/browse`** (opens the manuscript — not claim-level, and makes the atlas genuinely useful), and the **reduced-motion-aware bloom-in intro** (skippable).
- **D-09:** Keep **all 62,414 connected-manuscript stars** and **all per-MS node metadata always** (nodes are never trimmed). Keep **all per-MS edges** available, **drawn on zoom/focus** (prototype behavior); at the zoomed-out overview show baked **aggregate community flows** + stars (the raw pairwise-edge web is an illegible hairball at overview anyway).
- **D-10:** Shrink the payload — **typed/delta-encoded arrays instead of JSON** for nodes/edges + **Brotli** over the wire — targeting a **generous beta byte cap ~6 MB compressed** (the full unoptimized bake is ~13 MB). This is the **preview's own cap** and feeds `discovery-budgets.md` in Phase 134; the Phase 139 server-bounded explorer (ATLAS-02) tightens it later. Layout fully baked offline (never at request time); **CLS-safe render** (reserve canvas dimensions).
- **D-11:** Catalogue **"dust" (the ~193K unconnected manuscripts) is NOT shipped in the beta** (deferred — it would blow the byte budget). The preview shows the connected corpus and is framed as a "connections atlas."
- **D-12:** Standalone **`/atlas`** NiceGUI route (`@ui.page` in `web/main.py`), serving the **static baked asset** (via `app.add_static_files('/static', …)` or an embedded self-contained page). No request-time computation.
- **D-13:** Gate with a **dedicated atlas-preview feature flag** (following `web/feature_flags.py::_env_enabled`) so the preview page can be **ON in prod for the beta** while the **main discovery flag** (gating the claim surfaces) stays **OFF per REL-01**. With the flag OFF or the asset absent, `/atlas` hides cleanly (zero errors); the rest of the app is untouched.
- **D-14:** Entry points: a **"beta"-tagged link in the site's top nav** **and** a **claim-free homepage teaser card** (see Requirements Impact / D-16). No live homepage graph or suggestions.
- **D-15:** **Bilingual EN/HE page chrome following the site's active UI language** (correct RTL for HE); baked atlas labels (domain names) carry both languages and select by active language. Keep the standing honesty banner ("positions & clusters are algorithmically derived from textual connections; proximity is not physical provenance"). Beta labeling = a **"Beta / preview" badge** in the header + a one-line intro naming it a preview of the connections work.
- **D-16:** **Extend the ATLAS-PREVIEW exception to permit a claim-free homepage teaser in Phase 133.** The teaser is a small **CLS-safe static card** linking to `/atlas`, gated by the atlas-preview flag, carrying **no claim-level statements**, passing the masking scan + i18n/RTL basics, and set **`noindex` until the REL-01 gate**. This **revises ATLAS-03 and REL-01**, which currently hold the homepage band OFF until Phase 139.

### Claude's Discretion

- Clustering algorithm (prototype uses Louvain; Codex recommends **Leiden** for better-behaved communities — planner's call), exact force-layout parameters, the typed/delta edge-encoding format, intro choreography details, and the no-WebGL / asset-absent **fallback** (the prototype is **Canvas 2D**, so broadly compatible — a static-image/cluster-cards fallback covers render failure or a missing asset). Record a **deterministic seed + algorithm/version metadata** with the bake so the layout is reproducible and users' spatial memory survives rebuilds.

### Deferred Ideas (OUT OF SCOPE)

- **Catalogue "dust"** (~193K unconnected manuscripts) for a literal whole-Genizah view — Codex later add-on; deferred past the beta (byte budget).
- **Sigma.js v3 + binary multilevel assets** rebuild for the server-bounded drill-down explorer — **Phase 139 (ATLAS-02)**.
- **Work-lens** (bipartite manuscripts↔works) as a secondary view — future.
- **Per-region companion edge chunks** (serve per-MS edges per focused region instead of embedding the full set) — an optimization to reach for only if the ~6 MB beta cap proves tight.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ATLAS-01 | Offline-precomputed, canon-masked aggregated corpus overview ships as a static asset (layout never computed at request time); FIRST deployable artifact, early standalone beta page under REL-01 atlas-preview exception. Also implicitly locks ATLAS-02's "primary graph object = manuscript" decision (D-01), and (via the D-16 owner revision) extends to a claim-free homepage teaser under the widened ATLAS-03/REL-01 exception. | §"The Prototype Bake" (bake pipeline mapped end-to-end + gap found: continuation-only graph excludes ~13K island-only manuscripts from D-09's "all 62,414" target — see Common Pitfalls #1); §"Payload Optimization" (measured byte breakdown + typed-array/Brotli design); §"NiceGUI Integration" (`@ui.page('/atlas')`, static serving, Content-Encoding); §"Masking Scan" (D-07 design); §"Data Inputs" (libraries.csv + fjms_enrichment.db join confirmed); §Validation Architecture (test plan for all 6 ROADMAP success criteria). |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Hebrew RTL / bilingual convention:** every other page follows `is_rtl()`/`get_language()`/`tr()` — the atlas page's chrome (badge, banner, intro, nav label) must use the same primitives (already reflected in D-15/Pattern 1 above), not a bespoke language mechanism.
- **Sidecar convention:** all read-only reference data ships as local SQLite/binary sidecars that are gitignored and deployed alongside code (scp, never committed) — e.g. `pgp.db`, `fjms_enrichment.db`, `nli_crossref.db`. The baked atlas asset (binary payload, potentially several MB) should follow this exact convention rather than being committed to git, both for repo hygiene and because a masking-sensitive generated artifact is exactly the kind of file this project already treats as "generated, not committed."
- **Web is continuous-deploy; desktop is not.** This phase is web-only, so there is no installer/release-gate ceremony to plan for — `ATLAS_PREVIEW_ENABLED` (D-13) is the actual safety mechanism controlling exposure, not deploy timing.
- **Never launch a web server from Bash on Windows** (project memory `feedback_no_background_webserver.md` — creates unkillable zombie processes). Use the `nicegui.testing.User` + `httpx.ASGITransport` render-smoke harness (already modeled in `tests/render_smoke/test_joins_lab_render_smoke.py`) for exercising `/atlas`'s live render path instead of a manually-started dev server.
- **Supabase Data API grants rule (2026-05-30):** only applies to new `public` tables intended for `supabase-js`/PostgREST/GraphQL access. Not triggered by this phase — no new Supabase schema is introduced.
- **Documentation maintenance:** `docs/OPEN_ISSUES.md` should be checked at session start and updated at session end if any bug is found/fixed; `python scripts/check_docs.py` (run with `PYTHONUTF8=1` per project memory `reference_check_docs_utf8.md` to avoid a cp1255-console crash on emoji output) should pass before the phase's work is considered finished if any `docs/` content changes.
- **M-source masking is a hard, structural constraint** (CLAUDE.md + project memory `project_msource_codename_rule.md`): the restricted reference-corpus name must never appear in any tracked/committed file, under any alias, anywhere in this project — including inside this phase's own masking-scan script, whose pattern list must therefore be sourced from a gitignored/env-var location (see Open Questions #3), not hardcoded in a committed `.py` file.

## Summary

The prototype (`same_work_spike/probe/scripts/build_atlas_draft.py`, gitignored research code, read-only for planning — see provenance note below) is a complete, already-executed, already-working offline bake + self-contained Canvas 2D renderer. It reads `accepted_pairs_canonmask` from the research DB, aggregates 1.3M page-pairs into ~437K manuscript-pairs, splits the same-work ("continuation") graph into Louvain communities (recursively decomposing the giant liturgical component at `SPLIT_AT=800`), force-lays-out community circles, phyllotaxis-scatters member manuscripts inside each community, and emits one self-contained HTML file with an inlined JSON payload and a hand-written Canvas 2D renderer (zoom/pan/search/library-filter/focus-constellation/click-through/reduced-motion intro — all already implemented and matching every item in D-08). This phase forks that script, not the FJMS-cluster-circle predecessor `build_reuse_graph.py` (a different, older visualization the fork does **not** need).

Three concrete engineering findings from direct measurement/execution materially de-risk this phase. **First**, a live compression test of the actual generated 13 MB draft asset shows the whole file (JSON payload plus renderer) compresses to **1.90 MB with Brotli quality 11** (2.65 MB with gzip -9) — already 3x under the ~6 MB cap D-10 targets, even before any typed-array rework; the typed/delta-array rewrite the locked decision requires is still the right architecture (smaller raw payload, simpler decode, avoids inline-`<script>` JSON entirely) but the byte budget itself is not a tight constraint. **Second**, a direct SQL re-run of the manuscript-pair aggregation against the live `fullcorpus_v2.db` shows the prototype's **continuation-only graph places only 49,622 of the 62,645 manuscripts** that appear in *any* manuscript-pair relation (continuation or island) — a ~13,000-manuscript gap between what the current script actually plots and what D-09 requires ("all 62,414 connected-manuscript stars"). This is a real pre-fork design gap, not a documentation slip, and the plan must decide how island-only-connected manuscripts get a home in the layout (see Common Pitfalls #1). **Third**, HTTP-level Brotli (`Content-Encoding: br`) has ~96% global browser support and is the correct transport mechanism — NOT the JS `DecompressionStream('br')` API, which is Chromium-only; NiceGUI's `add_static_files` (a thin wrapper over Starlette `StaticFiles`) does not set `Content-Encoding` for precompressed files, so a small dedicated FastAPI route is needed.

**Primary recommendation:** Fork `build_atlas_draft.py` into a new committed script `scripts/build_atlas_asset.py` (paralleling existing sidecar-building scripts like `scripts/fgp_fill_credits_bilingual.py`); keep the proven Louvain/force-layout/phyllotaxis math verbatim (seeded, already deterministic); strip the discovery overlay (D-04); extend node inclusion to the full manuscript-pair universe (62,645, not just the continuation backbone) with island-only orphans placed as their own small dust-ring clusters; re-encode nodes/edges as struct-of-typed-arrays + a small string heap (not JSON); serve via a new `web/pages/atlas.py` + `@ui.page('/atlas')` registration in `web/main.py`, with a dedicated small route (not `add_static_files`) that serves the precompressed `.br` payload with a correct `Content-Encoding: br` header and a plain fallback for the ~4% of clients without Brotli support; gate everything behind a new `ATLAS_PREVIEW_ENABLED` flag in `web/feature_flags.py`; and build the D-07 masking scan as a standalone `scripts/check_atlas_masking.py` (modeled on the existing `scripts/check_docs.py` term-list pattern) whose restricted-string pattern list is sourced from a gitignored/env-var location — never committed in cleartext — exactly like the project's existing secret-handling convention (`.env`, never committed).

## Architectural Responsibility Map

> The 5 standard GSD tiers do not perfectly fit an offline-bake + static-asset product; this phase adds an implicit **Build/Offline Batch** tier for the bake pipeline, which has no request-time presence at all.

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Offline graph bake (aggregation, Louvain clustering, force-layout, phyllotaxis scatter, typed-array + Brotli encoding) | Build / Offline Batch | Database/Storage | Reads `fullcorpus_v2.db` (gitignored research DB), `libraries.csv`, `fjms_enrichment.db`; writes a static asset. Zero runtime presence — must never run inside the web process. |
| Masking scan (D-07) | Build / Offline Batch | — | A CI/pre-deploy gate script, not a runtime code path. |
| `/atlas` route registration + flag gate + bilingual chrome (badge, banner, intro copy) | Frontend Server (SSR) | Browser/Client | NiceGUI `@ui.page` renders server-side per request; the flag check and language resolution happen server-side before any client JS runs. |
| Precompressed asset serving (`Content-Encoding: br`) | CDN/Static | API/Backend | Conceptually a static-asset-serving concern; mechanically implemented as one small FastAPI route in `web/main.py` since NiceGUI/Starlette has no built-in precompressed-static support. |
| Interactive Canvas 2D renderer (zoom/pan/search/filter/focus/click-through/intro) | Browser/Client | — | 100% client-side JS against the fetched typed-array payload; no server round-trips after initial load. |
| Click-through to `/browse?sys_id=` | Browser/Client | API/Backend | `window.open()` client-side; the existing `/browse` route (`web/main.py`) already accepts `sys_id` as a query param — zero server change needed. |
| Homepage teaser card | Frontend Server (SSR) | Browser/Client | Static server-rendered NiceGUI card (`web/pages/home.py`), same pattern as the existing "Main Action Cards Grid"; click-through only, no data fetch. |

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `networkx` | 3.6.1 `[VERIFIED: PyPI registry — pip index versions; already imported and executed successfully by the prototype in this environment]` | Graph construction for Louvain community detection input | Already proven working in the existing prototype (produced the 13 MB draft asset on disk); no new risk. |
| `python-louvain` (import name `community`) | 0.16 `[VERIFIED: PyPI registry — pip index versions; already imported and executed successfully by the prototype in this environment]` | Louvain `best_partition` community detection, seeded (`random_state=42`) | Same — already used, deterministic, proven on this exact corpus at this exact scale. |
| `scipy` | 1.17.1 `[VERIFIED: already pinned in requirements-lock.txt]` | `coo_matrix` + `connected_components` for the continuation-graph component pass | Already a **runtime** product dependency (not new). |
| `numpy` | 2.4.3 `[VERIFIED: already pinned in requirements-lock.txt]` | Vectorized force-layout iteration, typed-array construction (`.tobytes()`) | Already a **runtime** product dependency (not new). |
| `Brotli` (PyPI package `Brotli`, import name `brotli`) | 1.2.0 `[VERIFIED: PyPI registry — pip index versions; slopcheck OK]` | Bake-time compression of the typed-array payload to `.bin.br` | Only needed at **bake time** (offline tooling), not by the running web app — the web app just serves precompressed bytes with a header, so `Brotli` need not become a runtime/production dependency at all. |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `leidenalg` + `python-igraph` | n/a — not currently used, not installed | Leiden community detection (Codex's recommendation for better-behaved communities) | Only if the planner chooses Leiden over Louvain for this phase (Claude's Discretion). Introduces two new dependencies with a C-extension build (`igraph`), a real setup-risk increase for an "early quick win" phase — **recommend deferring Leiden to Phase 139** (where Codex's answer already targets it alongside Sigma.js + ForceAtlas2 for the production drill-down explorer) and keeping the proven Louvain pipeline here. |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Louvain (`python-louvain`) | Leiden (`leidenalg`) | Leiden guarantees well-connected communities and is Codex's stated production recommendation, but adds two new dependencies (one C-extension) with zero existing precedent in this repo, for a phase explicitly framed as an "early quick win." Low risk/reward for Phase 133; good fit for Phase 139. |
| Custom Starlette route + manual `Content-Encoding: br` header | `brotli-asgi` middleware package | `brotli-asgi` is not installed/pinned anywhere in this repo; a middleware compresses *at request time* by default (though it can cache), which cuts against "no request-time computation." A tiny hand-written route that serves a pre-built `.br` file with a hardcoded header has zero new runtime dependency and is ~15 lines of code — simpler and more auditable for a masking-sensitive asset. |
| Typed-array binary payload | Ship the existing JSON payload as-is, rely on Starlette's built-in `GZipMiddleware` | Measured: the current draft's JSON, gzip'd, is already 2.65 MB — comfortably under the 6 MB cap. This is a legitimate low-risk fallback plan if the typed-array rewrite proves more time-consuming than expected, **but D-10 is a locked decision** requiring typed/delta arrays + Brotli specifically, so this is documented as a fallback/de-risking data point, not a substitute for the plan. |

**Installation:**
```bash
# Bake-time only (not added to requirements.txt / requirements-lock.txt — this tooling
# never runs inside the web process; treat like the other one-off scripts/ tools):
pip install networkx==3.6.1 python-louvain==0.16 Brotli==1.2.0
# scipy/numpy are already product dependencies; no action needed.
```

**Version verification:** Confirmed live via `pip index versions <pkg>` in this session (see table above); all three additional bake-tool packages passed `slopcheck` with verdict `OK` (see Package Legitimacy Audit).

## Package Legitimacy Audit

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| `networkx` | PyPI | ~20 yrs (est.; versions back to 0.34) | Very high (foundational scientific-Python graph library) | github.com/networkx/networkx | `[OK]` | Approved — bake-time only |
| `python-louvain` | PyPI | ~10 yrs (versions back to 0.1) | High (standard Louvain implementation, widely depended on) | github.com/taynaud/python-louvain | `[OK]` (slopcheck note: "Name starts with 'python-' — classic LLM naming pattern... but package is established") | Approved — bake-time only |
| `Brotli` | PyPI | ~10 yrs (Google-authored codec) | Very high | github.com/google/brotli | `[OK]` | Approved — bake-time only |
| `scipy`, `numpy` | PyPI | already product-pinned | — | — | — | Already approved (existing runtime deps, not newly introduced) |

**Packages removed due to slopcheck `[SLOP]` verdict:** none.
**Packages flagged as suspicious `[SUS]`:** none.

All three new packages were discovered by reading the actual, already-executing prototype source code in this repo (not by WebSearch or training-data guess) and confirmed installed/importable/already-producing-output in this exact environment, then independently re-verified against the PyPI registry and slopcheck in this session — tagged `[VERIFIED]` accordingly rather than `[ASSUMED]`.

## Architecture Patterns

### System Architecture Diagram

```
                    OFFLINE / BUILD TIME (never at request time)
   ┌──────────────────────────────────────────────────────────────────────┐
   │  fullcorpus_v2.db          libraries.csv         fjms_enrichment.db  │
   │  (accepted_pairs_          (titles, library      (domains table:    │
   │   canonmask; gitignored     codes; committed,     AlmaId==sys_id;   │
   │   research DB)              masking-safe)         gitignored sidecar)│
   │        │                        │                       │           │
   │        └───────────┬────────────┴───────────────────────┘           │
   │                    ▼                                                │
   │   scripts/build_atlas_asset.py  (fork of build_atlas_draft.py)      │
   │     1. aggregate 1.3M page-pairs -> ~437K MS-pairs (dedup by pair)  │
   │     2. classify continuation (same-work) vs island (citation)       │
   │     3. connected-components -> recursive Louvain split (SPLIT_AT)   │
   │     4. force-layout community circles; phyllotaxis-scatter members  │
   │     5. aggregate inter-community flows (top LINK_CAP)                │
   │     6. STRIP discovery overlay (D-04); encode struct-of-typed-arrays│
   │     7. Brotli-compress -> atlas-<ver>.bin.br + plain atlas-<ver>.bin│
   │                    │                                                 │
   │                    ▼                                                │
   │   scripts/check_atlas_masking.py  (D-07 exit gate, must pass)       │
   │     scans (a) the built asset's every embedded string                │
   │            (b) `git ls-files` committed repo content                 │
   │     for the M-source name/aliases/sigla pattern list                 │
   │     (pattern list itself sourced from a gitignored/.env location)    │
   └──────────────────────────────────────────────────────────────────────┘
                    │  (manual/CI step: scp or deploy alongside code,
                    │   like other sidecar files — not committed to git)
                    ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │                    RUNTIME (web process, request time)               │
   │                                                                      │
   │  Browser  ──GET /atlas──▶  web/main.py @ui.page('/atlas')            │
   │                              │                                       │
   │                              ├─ ATLAS_PREVIEW_ENABLED flag check     │
   │                              │    OFF or asset missing -> clean-hide │
   │                              ├─ create_layout() (nav/footer/RTL)     │
   │                              └─ web/pages/atlas.py renders chrome    │
   │                                 (badge, honesty banner, intro text)  │
   │                              │                                       │
   │  Browser  ──GET atlas-<ver>.bin.br──▶  dedicated small route         │
   │                              (checks Accept-Encoding: br,            │
   │                               serves precompressed bytes +           │
   │                               Content-Encoding: br + far-future      │
   │                               Cache-Control; falls back to plain      │
   │                               .bin if client lacks br support)        │
   │                              │                                       │
   │                              ▼                                       │
   │             Browser-side Canvas 2D renderer (self-contained JS,      │
   │             no CDN): decodes typed arrays, draws galaxy/constellation,│
   │             zoom/pan/search/filter/focus, click -> window.open       │
   │             ('/browse?sys_id=...')  ──▶  existing /browse route      │
   │                                                                      │
   │  Browser  ──GET /──▶  web/pages/home.py renders teaser card          │
   │             (same ATLAS_PREVIEW_ENABLED flag; noindex on /atlas       │
   │              until REL-01; static, CLS-safe, click -> /atlas)        │
   └──────────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

```
scripts/
├── build_atlas_asset.py       # NEW — fork of same_work_spike/probe/scripts/build_atlas_draft.py
│                               #   (bake-time only; not a runtime import; not in requirements.txt)
└── check_atlas_masking.py     # NEW — D-07 reusable masking scan, modeled on check_docs.py

web/
├── feature_flags.py           # + atlas_preview_enabled() alongside _env_enabled()
├── main.py                    # + @ui.page('/atlas') registration, nav item, precompressed-asset route
├── pages/
│   ├── atlas.py                # NEW — page chrome (badge/banner/intro) + asset-embed logic
│   └── home.py                 # + teaser card in the existing "Main Action Cards Grid"
└── static/
    └── atlas/                  # NEW — gitignored output dir (like other sidecar data),
                                 #   scp'd to the server like fjms_enrichment.db etc.
        ├── manifest.json        # seed, algo/version, counts, source-DB snapshot info
        ├── atlas-<ver>.bin      # plain typed-array payload (fallback for non-Brotli clients)
        └── atlas-<ver>.bin.br   # Brotli-compressed payload (primary; served via Content-Encoding: br)
```

### Pattern 1: Feature-flag-gated NiceGUI page with clean hide (D-13)

**What:** A page whose route only serves content when an env-driven flag is on and its backing asset exists; otherwise it must produce zero errors and leave the rest of the app untouched.

**When to use:** `/atlas` exactly mirrors the existing `WEB_PUZZLE_ENABLED` pattern already in production.

**Example:**
```python
# web/feature_flags.py — existing pattern, extend with the same idiom (source: web/feature_flags.py:8-15)
def _env_enabled(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}

ATLAS_PREVIEW_ENABLED = _env_enabled("ATLAS_PREVIEW_ENABLED", False)  # default OFF until beta launch

# web/main.py — existing nav-gating pattern (source: web/main.py:1158-1159)
if WEB_PUZZLE_ENABLED:
    nav_items.append(('/puzzle', 'extension', tr('Fragment Puzzle'), None))
# same shape for the new item:
if ATLAS_PREVIEW_ENABLED:
    nav_items.append(('/atlas', 'hub', tr('Connections Atlas'), tr('Beta')))
```
The route handler itself must ALSO check the flag (not just the nav link) — hiding the nav entry without gating the route is a real access-control gap (see Security Domain).

### Pattern 2: NiceGUI page function returning a raw `Response` (bypass page-shell rendering)

**What:** NiceGUI's `@ui.page` decorator explicitly supports a page function that returns a `starlette.responses.Response` object directly; when it does, NiceGUI skips its own page-shell rendering entirely.

**When to use:** Confirmed by reading `nicegui/page.py` in this environment: `if isinstance(result, Response): # NOTE if setup returns a response, we don't need to render the page`. This is a legitimate way to serve the baked, fully self-contained `atlas.html` (own `<html lang dir>`, own CSS/JS, matching the prototype's existing structure almost unchanged) directly at `/atlas`, if the planner prefers NOT to re-thread the renderer through NiceGUI's component tree. Reading `app.storage.user`/session language BEFORE returning the early Response is still safe (the "no UI context" error only affects code running off the request path, e.g. background threads — not the synchronous page-function body itself).

**Example:**
```python
# source: nicegui/page.py (installed package, this environment) — decorated() checks
# `isinstance(result, Response)` after calling the page function and short-circuits render.
from fastapi.responses import FileResponse

@ui.page('/atlas', title='...')
def atlas_page_route():
    if not (ATLAS_PREVIEW_ENABLED and os.path.exists(ATLAS_HTML_PATH)):
        return None  # falls through to normal (empty) NiceGUI render; combine with a redirect/404 as desired
    lang = _resolve_ui_language()  # safe_user_get still works here — same request context
    return FileResponse(ATLAS_HTML_PATH_FOR(lang))
```
**Tradeoff vs. embedding in the shared NiceGUI shell:** this path is the lowest-risk/lowest-effort way to ship the prototype's own self-contained banner/RTL/badge chrome verbatim, but it forgoes the site's shared top-nav/sidebar/footer (the atlas becomes a true "standalone" page, matching the phase's own framing of `/atlas` as a "standalone beta page"). **This is a real architecture fork the plan must decide explicitly** — see Open Questions.

### Pattern 3: Precompressed static asset served with `Content-Encoding: br`

**What:** Read a pre-built `.br` file's bytes once (or per request — it's small and local disk I/O) and return them with the HTTP header that tells the browser it is Brotli-encoded, so the browser's native, universally-supported (~96%) transport-level decompression handles it — no client-side Brotli library needed at all.

**When to use:** Any time a static, precomputed payload should ship pre-compressed with no request-time CPU cost. This is the correct mechanism for D-10; NiceGUI's `add_static_files` (confirmed in this session to wrap Starlette's `StaticFiles` via `CacheControlledStaticFiles`) does **not** auto-serve precompressed `.br`/`.gz` variants the way nginx's `gzip_static`/`brotli_static` modules do.

**Example:**
```python
# source: reasoned from nicegui/app/app.py::add_static_files (installed package, this
# environment) + verified HTTP Content-Encoding Brotli support ~96% global (caniuse.com/brotli,
# 2026 snapshot: Chrome 50+, Firefox 44+, Safari 11+, Edge 15+ — NOT the JS DecompressionStream
# API, which is Chromium-only for 'br' as of this research).
from fastapi import Request
from fastapi.responses import Response

_ATLAS_BR_BYTES = None  # load once at startup from web/static/atlas/atlas-<ver>.bin.br
_ATLAS_PLAIN_BYTES = None  # fallback

@app.get('/atlas-data/atlas.bin')
async def atlas_data(request: Request):
    if 'br' in request.headers.get('accept-encoding', ''):
        return Response(content=_ATLAS_BR_BYTES, media_type='application/octet-stream',
                         headers={'Content-Encoding': 'br',
                                  'Cache-Control': 'public, max-age=31536000, immutable'})
    return Response(content=_ATLAS_PLAIN_BYTES, media_type='application/octet-stream',
                     headers={'Cache-Control': 'public, max-age=31536000, immutable'})
```
Use a content-hashed or version-suffixed filename/route (`atlas-<ver>.bin`) so a re-bake never collides with a browser's cached previous version (classic cache-invalidation pitfall — see Common Pitfalls).

### Anti-Patterns to Avoid

- **Relying on `DecompressionStream('br')` client-side:** Firefox and Safari do not support Brotli in this JS API as of this research (Chromium-only) — a client-side "decompress the raw `.br` bytes in JS" approach would silently break on ~30-40% of desktop browser share. Always use HTTP `Content-Encoding: br` instead.
- **Embedding the full JSON/typed-array payload inline in a `<script>` tag inside the HTML** (as the current prototype does via `_HTML.replace('__DATA__', data)`): this (a) prevents independent browser caching of the (large, rarely-changing) data payload from the (small, page-chrome) HTML, and (b) creates a `</script>`-breakout risk if any catalogue string ever contains that literal substring (see Security Domain). Fetch the data payload as a separate file/route instead.
- **Running any part of the bake (Louvain, force-layout, phyllotaxis) inside the NiceGUI request handler:** ATLAS-01 explicitly forbids request-time layout computation; the entire pipeline belongs in the offline `scripts/build_atlas_asset.py`.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Community detection over the same-work graph | A custom clustering heuristic | `python-louvain`'s `best_partition` (already proven on this exact corpus, seeded, already produces the working 13 MB draft) | Reinventing this for an "early quick win" phase is pure risk with no benefit; the existing seeded implementation is deterministic and already validated at this scale. |
| HTTP compression negotiation | A bespoke "is this client capable" sniffing scheme | Standard `Accept-Encoding` / `Content-Encoding: br` HTTP semantics | Universally understood by every HTTP client/proxy/CDN; inventing a custom scheme breaks caching intermediaries and browser devtools expectations. |
| Binary payload framing | A hand-rolled ad hoc byte layout with no header | A small fixed header (magic + version + per-section counts/offsets) + `numpy`'s built-in `.tobytes()`/`np.frombuffer` for the typed arrays | `numpy` already a pinned dependency; a documented header avoids "magic offset" bugs and makes the format self-describing for future maintainers/Phase 139's eventual binary-asset upgrade. |
| Force-directed layout math | A new physics simulation from scratch | The prototype's existing vectorized `numpy` force-layout + phyllotaxis scatter (already tuned, already seeded) | Already executes in seconds over ~2,700 communities and ~62K member scatter positions in this environment; re-deriving it is wasted effort for a preview phase whose production successor (Phase 139) is explicitly planned to use a different stack (ForceAtlas2/Leiden/Sigma.js) anyway. |

**Key insight:** almost everything genuinely hard about this phase (the graph math, the renderer interactions, the honesty-banner/RTL chrome) is **already built and already working** in the gitignored prototype. The actual new engineering surface is narrow: (1) close the node-inclusion gap (Common Pitfalls #1), (2) re-encode the payload, (3) wire three small pieces of NiceGUI plumbing (route, flag, static-serving header), and (4) build the masking scan. Resist the temptation to rewrite the renderer or clustering "properly" for this phase — that rewrite is Phase 139's job.

## Common Pitfalls

### Pitfall 1: The current prototype's "connected manuscript" count silently excludes ~13,000 manuscripts that D-09 requires

**What goes wrong:** The locked decision requires "all 62,414 connected-manuscript stars," but the current `build_atlas_draft.py` only places manuscripts that belong to the **continuation** (same-work) graph's connected components. A direct re-run of the exact aggregation query against the live `fullcorpus_v2.db` in this session shows: 437,373 total manuscript-pairs; of those, only 235,669 pairs are continuation-dominant, spanning **49,622** distinct manuscripts placed into communities — while **62,645** distinct manuscripts appear across *all* manuscript-pairs (continuation OR island/citation-only). A manuscript connected to the corpus *only* via citation/quotation (island) relations, with zero same-work relations, currently has no home in `node_idx` and is silently dropped from the rendered galaxy entirely.

**Why it happens:** `comp_members` (and therefore every downstream `clusters`/`node_idx` structure) is built from `connected_components()` over a sparse matrix constructed *only* from `cont_keys` (continuation-dominant pairs). Island-only pairs are aggregated into `ms_pairs` but never contribute nodes.

**How to avoid:** Explicitly decide (and implement) where island-only-connected manuscripts go before forking the script further — e.g., give each such manuscript (or small connected sub-graph of them) its own tiny "cluster" placed in the existing dust-ring logic (`n > K` tail-community placement already exists for oversized-community overflow and can be reused/extended for this), or fold them into the endpoint's existing continuation-cluster if it has one and treat citation-only orphans as a distinct micro-cluster otherwise. Re-measure the final node/edge counts against 62,414 as an explicit bake-time assertion, and treat any further discrepancy (e.g. from `dup_shelf`/`dup_lines` filter differences, or a newer DB snapshot) as something to reconcile with the ATLAS-01 SC#1 language, not silently absorb.

**Warning signs:** The bake script's own printed node count not matching the CONTEXT/ROADMAP-cited 62,414 figure; any manuscript that a user can find via `/browse` or `/search` but never via the atlas search box.

### Pitfall 2: Conflating "island" (citation) edges, "continuation" (same-work) edges, and physical joins

**What goes wrong:** METHOD.md §8.1 defines two edge classes from flank-contrast: **continuation** (flanks also align → same running text → same-unit/same-work evidence) and **island** (flanks dissimilar → citation/quotation of a shared or different source, NOT a common work). METHOD.md §7 separately reports that **every one of 36 "join anomaly" pairs turned out to be duplicate photography, not textual overlap**, and states as a corpus fact that physically-joined fragments share running text in ~0% of cases (they are consecutive, not overlapping) — i.e., **a physical join is a structurally different relation that this graph's edges never represent.**

**Why it happens:** All three (same-work continuation, citation/island, physical join) are "two manuscripts are related" relations, and it is easy to blur them in UI copy, tooltips, or edge-color legends.

**How to avoid:** Keep the green/orange (continuation/island) edge-color legend from the prototype, and never describe any atlas edge as "these two fragments join" or "these two fragments are pieces of one object." The honesty banner (D-15) plus the "algorithmically detected regions" framing (D-02) already covers most of this, but edge tooltips/legends need equal care.

### Pitfall 3: Script-tag breakout from inline JSON-in-`<script>` embedding

**What goes wrong:** The prototype embeds its entire data payload via `_HTML.replace('__DATA__', data)` where `data` is a `json.dumps(..., ensure_ascii=False)` string placed directly inside a `<script>` block. If any catalogue title or shelfmark string (sourced from `libraries.csv`, not adversarial input, but not guaranteed clean either) ever contains the literal substring `</script`, it terminates the script block early and corrupts the page (a well-known JSON-in-HTML injection class).

**Why it happens:** `json.dumps` correctly escapes JSON syntax but does **not** know about, or escape for, its HTML embedding context.

**How to avoid:** Either (a) escape `</` to `<\/` in the serialized JSON/typed-array-adjacent manifest before embedding, or — the recommended, cleaner fix already implied by the typed-array rework — (b) stop inlining the payload altogether and fetch it as a separate binary file (see Pattern 3), which sidesteps this entire injection class structurally.

### Pitfall 4: Typed-array byte alignment and `sys_id` numeric precision

**What goes wrong:** (a) `Float32Array`/`Uint16Array`/`BigUint64Array` views over a shared `ArrayBuffer` require their starting byte offset to be a multiple of their element size (4, 2, 8 bytes respectively) — a naive concatenation of variable-length sections without padding will throw `RangeError: start offset ... is not a multiple of ...` in the browser. (b) `sys_id` values (e.g. `990001562160205171`) **exceed `Number.MAX_SAFE_INTEGER` (2^53−1 ≈ 9.007×10^15)** — storing them as a JS `Number`/`Float64Array` silently loses precision (the last 1-2 digits become wrong), which breaks `/browse?sys_id=` click-through links.

**How to avoid:** Pad every section to its next element-size boundary when building the binary header/offset table; store `sys_id` as `BigUint64Array` (all observed sys_ids are pure-digit numeric strings well under 2^64) and reconstruct the decimal string client-side via `.toString()` — or, more conservatively, keep `sys_id` in the shared UTF-8 string heap alongside shelfmark/title if any non-numeric or leading-zero-significant sys_id is ever found in `libraries.csv` (verify this assumption at bake time with an explicit assertion before committing to `BigUint64Array`).

### Pitfall 5: `add_static_files` cannot itself add `Content-Encoding`

**What goes wrong:** Assuming that simply dropping `atlas-<ver>.bin.br` into the existing `/static` directory (served via `app.add_static_files('/static', STATIC_DIR)`) "just works" for Brotli. Confirmed in this session by reading `nicegui/app/app.py::add_static_files`: it wraps a `CacheControlledStaticFiles` (Starlette `StaticFiles` + a `Cache-Control` header) and does not know about precompressed variants or set `Content-Encoding` at all — a browser would receive raw Brotli bytes with no encoding header and try to render them as if they were the declared `media_type` (garbage).

**How to avoid:** Use the dedicated small route from Pattern 3, not `add_static_files`, for the compressed payload specifically.

### Pitfall 6: A rebake silently invalidating a cached older payload

**What goes wrong:** If the compressed asset is served under a stable filename/URL with a long `Cache-Control: immutable`, a future rebake (new layout, new node count) could serve stale cached bytes to returning visitors whose browser cache has not expired, potentially mismatching a renderer-JS update that changed the expected binary format.

**How to avoid:** Version the filename/route (`atlas-<schema-version>.bin.br`) and bump it whenever the binary layout or content changes meaningfully — mirroring the project's existing DATA-08 sidecar convention of a schema-versioned filename so old code never opens an incompatible snapshot.

### Pitfall 7: Perpetuating the existing uncommitted M-source leak in `genizah_translations.py`

**What goes wrong:** `git diff genizah_translations.py` (checked live in this session) shows an **uncommitted** working-tree addition containing the literal restricted source name in several new translation keys/values (a Discovery Review deck glossary added for other SEED-029 tooling, not by this phase). If this phase's work is committed without first scrubbing that pre-existing diff, the restricted name enters git history for the first time in this repo (confirmed via `git ls-files | grep same_work_spike` = 0 tracked files, and `git check-ignore` confirms the whole `same_work_spike/` tree is correctly gitignored today — so the *only* current leak vector is this uncommitted file).

**How to avoid:** Before the first commit in this phase, either scrub those lines from `genizah_translations.py` or make sure they are not part of any commit this phase creates. This is exactly what D-07's "first run performs the one-time cleanup verification" is for — run the masking scan against the current working tree BEFORE committing anything, and treat any hit as a hard stop.

### Pitfall 8: Naming collision with the existing `/discoveries` route

**What goes wrong:** The site already has a live `/discoveries` nav item ("Community discoveries, questions, and contributions" — corrections-style user content), unrelated to the v9.0.0 "Discovery" milestone's claim-level identifications. Naming the new atlas feature or its nav badge "Discoveries" (or similar) risks user/reviewer confusion between two unrelated features.

**How to avoid:** Use "Atlas" / "Connections Atlas" / "אטלס" consistently (matching the prototype's own `<title>` and `<b>אטלס הגניזה</b>` header bar), never "Discoveries."

## Code Examples

### Existing bilingual language resolution (reuse verbatim for D-15)
```python
# Source: web/main.py:851-859 (existing code, already used by every other page)
def _resolve_ui_language() -> str:
    """Return the persisted UI language so layout and bootstrap agree on first render."""
    saved_lang = safe_user_get('ui_language')
    if saved_lang in ('he', 'en'):
        return saved_lang
    current_lang = get_language()
    return current_lang if current_lang in ('he', 'en') else 'he'
```

### Existing `/browse` click-through target (zero change needed)
```python
# Source: web/main.py:1704-1705 — confirms sys_id is already a first-class query param
@ui.page('/browse', title='Manuscript Browser | עיון בכתבי יד — Dicta Genizah Search')
def browse_page_route(sys_id: str = None, highlight: str = None, fl_id: str = None,
                       page: int = None, shelfmark: str = None, volume_ie: str = None,
                       embed: str = None):
    ...
```
The prototype's existing `window.open(base+'/browse?sys_id='+N[p][6],'_blank')` (build_atlas_draft.py:709) already targets exactly this contract; only the hardcoded `base` input-field default needs to become `window.location.origin` (or a relative `/browse?sys_id=...`) since the atlas now ships on the same origin.

### Existing `noindex` SEO primitive (reuse for D-16's `/atlas` noindex requirement)
```python
# Source: web/main.py:752-765 — page_meta() already supports noindex, no new code needed
def page_meta(path: str = '/', title: str = _DEFAULT_TITLE, description: str = _DEFAULT_DESCRIPTION,
              og_type: str = 'website', noindex: bool = False, needs_iiif: bool = False) -> str:
    robots = '<meta name="robots" content="noindex, noarchive, follow">\n' if noindex else ''
    ...
# usage on the new route:
ui.add_head_html(page_meta('/atlas', title='...', noindex=True))
```
Do **not** add `/atlas` to `sitemap-static.xml` (generated in `web/api.py`) until the REL-01 gate — SEO-01 (Phase 139) owns full sitemap/hreflang treatment.

### Measured byte breakdown of the current (unoptimized) draft asset
```
# Measured live in this session against same_work_spike/probe/review/atlas_draft.html:
nodes_json:  raw=6.47 MB  gzip-9=1.22 MB  brotli-11=0.83 MB   (49,622 nodes)
edges_json:  raw=6.34 MB  gzip-9=1.39 MB  brotli-11=1.04 MB   (408,593 edges)
clabels/flows: ~135 KB raw combined (negligible)
whole_html:  raw=12.97 MB gzip-9=2.65 MB brotli-11=1.90 MB
```
Scaling to the full 62,645-node target (D-09) and dropping the discovery-overlay field (D-04) will move these numbers, but even a naive +30% node-count scale-up projects to roughly ~2.5-3.5 MB compressed — comfortable headroom under the 6 MB cap. **Treat this as a planning input, not a substitute for measuring the actual rebuilt asset** — add an explicit bake-time assertion (`assert brotli_size <= 6_000_000`) as the phase's own byte-budget gate, mirroring the house `discovery-budgets.md` convention.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| `build_reuse_graph.py` (FJMS-cluster circles, sampled top-300 members per cluster, drill-in force-sim in-browser) | `build_atlas_draft.py` (per-manuscript galaxy, ALL members always included, layout fully baked offline) | Explicitly built as an evolution within the same research spike (per its own docstring: "forked from build_reuse_graph.py... renders a per-MANUSCRIPT galaxy instead of cluster circles") | The fork target for this phase is the newer, per-manuscript script — do not fork `build_reuse_graph.py`. |
| Raw JSON payload inlined in `<script>` | Typed/delta-encoded arrays + Brotli, fetched separately | This phase (D-10, locked) | Smaller, cache-friendlier, avoids the script-breakout injection class. |
| This phase's Canvas 2D preview | Phase 139's planned Sigma.js v3 + WebGL + binary multilevel assets + Leiden + ForceAtlas2 (Codex's stated production recommendation) | Phase 139 (ATLAS-02), explicitly deferred | Do not over-invest in Phase 133's renderer/clustering — the production stack is intentionally different and later. |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | All real `sys_id` values in `libraries.csv`/the graph are pure-digit numeric strings that fit safely in an unsigned 64-bit integer with no leading-zero significance. | Common Pitfalls #4 (typed-array design) | If wrong, `BigUint64Array` encoding would corrupt `/browse?sys_id=` links for affected rows; mitigated by recommending an explicit bake-time round-trip assertion before committing to this encoding. |
| A2 | Reading `app.storage.user` (via `safe_user_get`) inside a `@ui.page` function body works correctly even when that function returns an early `Response` object (Pattern 2), because the "no UI context" failure mode is specific to code running off the request path (background threads), not the synchronous handler itself. | Pattern 2 (NiceGUI raw-Response page) | If wrong, the standalone-HTML-passthrough architecture couldn't read the site's language preference server-side and would need a different mechanism (e.g., a `?lang=` query param or a client-side cookie read inside the baked HTML itself). Low implementation cost either way if this needs correcting during execution. |
| A3 | The projected byte-budget scale-up (49,622→62,645 nodes; discovery-field removal) still lands comfortably under 6 MB compressed. | Code Examples (byte breakdown) | If wrong, the plan needs an explicit LOD/per-region-chunk fallback (already listed as a deferred idea in CONTEXT.md) — low risk since it's already flagged and measured headroom is currently ~3x. |

**All other claims in this research were verified via direct execution/measurement in this session (PyPI registry lookups, slopcheck, live compression benchmarks, direct SQL against the live research DB, direct reads of installed `nicegui`/`starlette` package source) or cited from the project's own committed code/docs — no further user confirmation needed for those.**

## Open Questions

1. **Standalone-page architecture: NiceGUI-shell-embedded vs. raw-`Response`-passthrough (Pattern 1 vs Pattern 2)?**
   - What we know: Both are technically viable (confirmed via reading `nicegui/page.py`); the phase is explicitly framed as a "standalone `/atlas` beta page," and the prototype already has its own complete header/banner/RTL chrome that would need to be either re-expressed inside the shared NiceGUI layout (`create_layout()`) or kept as-is in a bypassed raw response.
   - What's unclear: Whether "the site's active UI language" (D-15) is best satisfied by inheriting the shared top-nav/sidebar chrome (more consistent site UX, more integration work) or by a self-contained page that independently reads the same persisted language preference (less integration work, loses shared nav/footer).
   - Recommendation: Lean toward the shared-shell approach (Pattern 1-style, embedding the renderer inside `create_layout()`) for consistency with every other page in the app and because it makes the beta/preview badge and honesty banner trivially reusable UI components rather than duplicated HTML strings — but this is a real decision point for the plan to pin explicitly, not silently default.

2. **Where does island-only-connected-manuscript placement go, concretely?**
   - What we know: ~13,000 manuscripts are currently dropped (Common Pitfalls #1); the existing dust-ring / oversized-tail-community placement code (`n > K` branch) is a plausible extension point.
   - What's unclear: Whether these should render as individually-labeled micro-clusters, be silently folded into their nearest continuation neighbor's community, or get a distinct "citation-only" visual treatment (e.g., a different sprite/desaturation) so users aren't confused about why some stars have no place in any labeled region.
   - Recommendation: Simplest correct option — treat each island-only connected component as its own micro-cluster using the exact same Louvain/dust-ring code path already used for oversized components, just with a lower `MIN_CLUSTER` floor (or `MIN_CLUSTER=1` for these) so singletons still get a position; skip labeling (no `clabels` entry) below the existing size threshold (`c['n'] >= 25`) as already implemented.

3. **Masking-pattern storage mechanism for the D-07 scan script.**
   - What we know: The restricted name/aliases/sigla must never appear in cleartext in any committed file (including the scan script itself, which would otherwise need to contain its own search target in plaintext). The project's established convention for exactly this kind of "sensitive string that must never be committed" is `.env`/gitignored local files (already used for Supabase keys, HMAC secrets, etc.).
   - What's unclear: The exact mechanism (single env var with a delimited list vs. a gitignored local JSON/text file path referenced by an env var) and how CI would inject it (a repository secret, presumably, mirroring however other CI secrets are already handled in this repo's GitHub Actions config — not inspected in this research pass).
   - Recommendation: An env var (e.g. `MASKING_SCAN_PATTERNS_FILE` pointing at a gitignored local file, analogous to how `.env` itself is gitignored) is the lowest-effort, most-consistent-with-existing-conventions choice; verify the CI secrets mechanism during planning by checking `.github/workflows/*.yml` (not read in this research pass).

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `networkx` | Offline bake (community graph construction) | ✓ (installed, already used by the prototype) | 3.6.1 | — |
| `python-louvain` (`community`) | Offline bake (Louvain clustering) | ✓ (installed, already used by the prototype) | 0.16 | Leiden (`leidenalg`+`igraph`) if the planner chooses that path instead |
| `scipy` | Offline bake (connected components) | ✓ (already a pinned product dependency) | 1.17.1 | — |
| `numpy` | Offline bake (vectorized layout) + typed-array packing | ✓ (already a pinned product dependency) | 2.4.3 | — |
| `Brotli` (Python, bake-time only) | Precompressing the shipped payload | ✓ (installed in this session; not yet a repo dependency anywhere) | 1.2.0 | Ship gzip-only (Starlette's built-in `GZipMiddleware`, zero new deps) — measured 2.65 MB, still under budget, if Brotli tooling is ever unavailable in a build environment |
| HTTP `Content-Encoding: br` (browser-side) | Serving the compressed asset | ✓ (~96% global browser support per caniuse.com, 2026 snapshot) | Chrome 50+, Firefox 44+, Safari 11+, Edge 15+ | Plain uncompressed `.bin` fallback for the remaining ~4% (IE11, Opera Mini) — trivial to implement (Pattern 3 already includes it) |
| SQLite (bake-time read of `fullcorpus_v2.db`, `fjms_enrichment.db`) | Offline bake data sources | ✓ (stdlib `sqlite3`; both DB files present on disk) | — | — |

**Missing dependencies with no fallback:** none.
**Missing dependencies with fallback:** Brotli tooling (gzip fallback, already measured safe).

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (config in `pyproject.toml` `[tool.pytest.ini_options]`) |
| Config file | `pyproject.toml` |
| Quick run command | `pytest tests/test_atlas_*.py -x` (new files this phase creates) |
| Full suite command | `pytest tests/` (existing project convention; GUI/render-smoke tests are marker-split — see below) |

### Existing render-smoke harness to model on

`tests/render_smoke/test_joins_lab_render_smoke.py` is the only existing precedent in this repo for a live NiceGUI async-render test: it drives `nicegui.testing.User` over `httpx.ASGITransport(core.app)` against a real route with heavy seams mocked, and is auto-tagged with the `render_smoke` pytest marker (via `tests/conftest.py`'s path-based marker injection for anything under `tests/render_smoke/`) so it runs in the dedicated fresh-process CI job rather than the main `pytest tests/` run. New atlas render-smoke tests should live under `tests/render_smoke/test_atlas_render_smoke.py` following the same pattern.

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ATLAS-01 SC#1 | Bake produces a static asset; node/edge/cluster counts match the 62,414-target (post Pitfall-#1 fix); no discovery-overlay fields present | unit | `pytest tests/test_atlas_bake.py -x` | ❌ Wave 0 |
| ATLAS-01 SC#1 | Byte budget: compressed asset ≤ 6 MB | unit | `pytest tests/test_atlas_bake.py::test_byte_budget -x` | ❌ Wave 0 |
| ATLAS-01 SC#2 | `/atlas` renders when flag ON + asset present; hides cleanly (no 500, no broken nav) when flag OFF or asset absent | render_smoke | `pytest tests/render_smoke/test_atlas_render_smoke.py -m render_smoke` | ❌ Wave 0 |
| ATLAS-01 SC#3 | Masking scan passes over the built asset + committed repo content | unit (script self-test) | `python scripts/check_atlas_masking.py --scan-repo --scan-asset <path>` (exit 0) | ❌ Wave 0 |
| ATLAS-01 SC#4 | Static payload within byte cap; no request-time compute (route returns from disk-loaded bytes only, no bake call in request path); CLS-safe render (canvas dimensions reserved) | unit + render_smoke | `pytest tests/test_atlas_bake.py::test_byte_budget` + `pytest tests/render_smoke/test_atlas_render_smoke.py -k cls` | ❌ Wave 0 |
| ATLAS-01 SC#4 | EN/HE + RTL page chrome | render_smoke | `pytest tests/render_smoke/test_atlas_render_smoke.py -k i18n` | ❌ Wave 0 |
| ATLAS-01 SC#6 | Homepage teaser card: CLS-safe, claim-free, flag-gated, masking-clean, EN/HE+RTL, `noindex` on `/atlas` | render_smoke | `pytest tests/render_smoke/test_atlas_render_smoke.py -k teaser` | ❌ Wave 0 |
| D-13 clean-hide | Flag OFF ⇒ nav item absent, route hides, teaser card absent, zero errors | unit + render_smoke | `pytest tests/test_atlas_flag_gating.py -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `pytest tests/test_atlas_bake.py tests/test_atlas_flag_gating.py -x` (fast, no GUI)
- **Per wave merge:** `pytest tests/render_smoke/test_atlas_render_smoke.py -m render_smoke` + full masking scan
- **Phase gate:** Full suite green (`pytest tests/`, marker-split per existing CI convention) + `python scripts/check_atlas_masking.py` exit 0 before `/gsd:verify-work`

### Wave 0 Gaps

- [ ] `tests/test_atlas_bake.py` — covers ATLAS-01 SC#1/SC#4 (node/edge/byte-budget assertions on the bake output)
- [ ] `tests/test_atlas_flag_gating.py` — covers D-13 (flag OFF/asset-absent clean-hide, headless)
- [ ] `tests/render_smoke/test_atlas_render_smoke.py` — covers SC#2/SC#4/SC#6 (live render, i18n/RTL, CLS, teaser card), modeled on `tests/render_smoke/test_joins_lab_render_smoke.py`
- [ ] `scripts/check_atlas_masking.py` — the D-07 scan itself; needs a small self-test (`tests/test_atlas_masking_scan.py`) proving it actually catches a known injected test pattern (a "sanity-injection" test, mirroring the pattern already used in `tests/test_joins_lab_i18n.py`'s "sanity-injection proven to bite" per project history)
- [ ] Framework install: none — pytest + `nicegui.testing.User` harness already present; no new test-framework dependency

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | `/atlas` is a public, unauthenticated, read-only page (like `/browse`, `/search`). |
| V3 Session Management | No | Only reads the existing session-scoped language preference; writes nothing new to session state. |
| V4 Access Control | Yes (feature-flag gating) | Server-side flag check inside the route handler itself (not just omitting the nav link) — env-driven `ATLAS_PREVIEW_ENABLED`, same idiom as `WEB_PUZZLE_ENABLED`. |
| V5 Input Validation | Yes (minimal, reused) | The only "input" is the existing, already-validated `sys_id` query param on `/browse` (unchanged by this phase); the new precompressed-asset route takes no user-controlled path/query input at all (hardcoded server-side file path — no path-traversal surface). |
| V6 Cryptography | No | No cryptographic operations introduced by this phase. |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| M-source / reference-corpus provenance leak into a public static asset or page output | Information Disclosure | D-07 reusable masking scan as a hard pre-deploy/CI gate over both the built asset and committed repo content; pattern list sourced from a gitignored/env-var location, never committed in cleartext (see Open Questions #3). |
| `</script>` breakout via an inline JSON-in-`<script>` payload containing an unescaped catalogue string | Tampering (script injection) | Prefer fetching the data payload as a separate binary file (Pattern 3) over inline embedding; if inline embedding is ever used elsewhere, escape `</` to `<\/` before embedding. |
| Feature-flag bypass (nav link hidden but route still serves content when flag is OFF) | Elevation of Privilege (of a sort — accessing a not-yet-released surface) | Gate the route handler itself, not just the nav link; the existing `WEB_PUZZLE_ENABLED` pattern already does this correctly (`if WEB_PUZZLE_ENABLED: nav_items.append(...)` is only the nav side — verify the actual `/puzzle` route ALSO checks the flag as the model to copy, not just its nav-list gating). |
| Path traversal on the new precompressed-asset route | Tampering | The route must use a hardcoded server-side file path (no user-supplied path/filename segment) — unlike `add_static_files`, which does accept a path segment; the dedicated route in Pattern 3 takes no path parameter at all, closing this off structurally. |
| Stale/mismatched cached binary payload after a rebake (renderer JS expects a new format, cached old bytes served) | Tampering (data-integrity, not an attacker threat, but a correctness/safety issue) | Version-suffixed filename/route + far-future immutable cache headers only on the versioned URL (see Pitfall 6). |

## Sources

### Primary (HIGH confidence — direct execution/inspection in this session)

- `same_work_spike/probe/scripts/build_atlas_draft.py` — read in full; the prototype bake + renderer (gitignored, read-only for planning, never to be committed or referenced by real name outside "M-source" context)
- `same_work_spike/probe/scripts/build_reuse_graph.py` — read in full; the predecessor script (confirmed NOT the fork target)
- `same_work_spike/probe/results/CODEX-ATLAS-answer.md` — read in full; design critique + production-stack recommendation (Phase 139-scoped)
- `same_work_spike/probe/METHOD.md` — read relevant sections (§7-8: duplicate-photography/physical-join findings, flank-contrast classification)
- Direct SQL execution against `same_work_spike/probe/data/fullcorpus_v2.db` (`accepted_pairs_canonmask` table) in this session — confirmed schema, row counts, and the continuation-graph node-count gap (Pitfall #1)
- Direct SQL execution against `fist_data/fjms_enrichment.db` (`domains` table) in this session — confirmed schema and row counts
- `git diff genizah_translations.py`, `git ls-files | grep same_work_spike`, `git check-ignore -v` — confirmed the current masking posture (Pitfall #7)
- `nicegui/page.py`, `nicegui/app/app.py` (installed package source, this environment) — confirmed the raw-`Response` page-shortcut and the `add_static_files`/`CacheControlledStaticFiles` implementation
- Live compression benchmark (`gzip`, `brotli` Python packages) against the actual 13 MB generated draft asset in this session
- `pip index versions networkx / python-louvain / Brotli` + `slopcheck install networkx python-louvain Brotli` — run live in this session
- `web/main.py`, `web/feature_flags.py`, `web/pages/home.py`, `web/pages/browse.py`, `web/pages/puzzle.py`, `web/safe_storage.py`, `web/translations.py`, `tests/render_smoke/test_joins_lab_render_smoke.py`, `scripts/check_docs.py` — read in full or in relevant part, this session
- `.planning/phases/133-visual-atlas-preview-early-quick-win/133-CONTEXT.md`, `.planning/REQUIREMENTS.md`, `.planning/ROADMAP.md`, `.planning/PROJECT.md`, `.planning/STATE.md`, `CLAUDE.md` — read this session

### Secondary (MEDIUM confidence — WebSearch verified with an authoritative source)

- caniuse.com/brotli (fetched via WebFetch this session) — HTTP `Content-Encoding: br` browser support (~96% global, Chrome 50+/Firefox 44+/Safari 11+/Edge 15+)
- MDN `DecompressionStream` + general web-search synthesis — confirms `'br'` support in the JS `DecompressionStream` API is Chromium-only, distinct from HTTP-level Content-Encoding support

### Tertiary (LOW confidence)

- None used as load-bearing claims in this document.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — every package version verified live against PyPI in this session; two of three new build-tool packages are already installed and already proven-executing in this repo's own prototype.
- Architecture: HIGH for the bake pipeline and byte-budget analysis (measured directly); MEDIUM for the exact NiceGUI-shell-vs-raw-Response integration choice (both are viable, genuinely a planning decision, not something this research can resolve unilaterally — see Open Questions #1).
- Pitfalls: HIGH — the node-count gap (Pitfall #1), the M-source leak in the current working tree (Pitfall #7), and the Brotli-serving mechanics (Pitfalls #3/#5) were all independently confirmed by direct execution/inspection in this session, not inferred from documentation alone.

**Research date:** 2026-07-20
**Valid until:** ~30 days for the architecture/stack guidance (stable); the specific byte-count/node-count measurements should be **re-verified against the live `fullcorpus_v2.db` at bake time** regardless of staleness window, since that research DB is explicitly still under active growth per project memory (`same_work_spike` probe work is ongoing).
