# Project Research Summary

**Project:** GenizahSearch — v9.0.0 Discovery (Same-Work Identification & Connection Atlas, web-only)
**Domain:** Corpus-scale same-work / text-reuse discovery + witness-mapping + community-verification module folded into an existing scholarly manuscript web app (NiceGUI + FastAPI + read-only SQLite sidecars + Supabase; bilingual EN/HE RTL; SEED-029 productization)
**Researched:** 2026-07-19
**Confidence:** HIGH (stack + architecture + pitfalls grounded in the working prototype, the real research DB, and house code/history; MEDIUM only on the exact "right" UX for the mixed scholar/lay audience)

## Executive Summary

This is an **integration milestone, not a greenfield build.** The Discovery module surfaces an already-completed research program (SEED-029: 275,894 tier-A page-level identifications across 52,497 manuscripts / 4,093 works, banded R-A 0.889 / R-B 0.859 / R-CANON 0.647) inside the existing GenizahSearch web app. The finding across all four research files is that **almost no new runtime stack is required.** The connections panel and bounded graph views ride on NiceGUI's native `ui.echart`; the whole-corpus atlas reuses the already-proven Canvas 2D starfield at 52K scale; the product data ships as a distilled read-only SQLite sidecar exactly like `fjms_enrichment.db`/`pgp.db`; community judgments reuse the corrections/Supabase pattern. Graph layout and community detection are **offline, build-time-only** (numpy/networkx/python-louvain on the dev box, deliberately kept out of `requirements.txt`). The one hard escalation option (sigma.js v3, MIT, vendored) is only needed if the flagship atlas must become a *live* WebGL graph over all 52K nodes — which the research recommends against for v1.

The recommended approach is a **thin de-risk data spine first, then progressively heavier surfaces**, conforming to fixed house patterns (read-only sidecar → `shared/*_service.py` → `run.io_bound` off the event loop → NiceGUI enrichment; Supabase only for community writes via a fresh `get_user_client()`). Every architecture recommendation points at a real file/function. The build order all three "how-to-build" files converge on: (1) sidecar distillation + masking + `DiscoveryService` (the spine, because a wrong sidecar shape invalidates every surface), (2) the MS connections panel on `/browse` (lowest UI risk, highest reach — reuses the proven enrichment path), (3) the work→witness page, (4) community judgment capture, (5) the leads queue, (6) the atlas + homepage promotion as the capstone.

The risk profile is dominated by **two hard constraints and one performance cliff.** (a) **M-source provenance masking is a hard release blocker** — 3,468/4,093 works trace to a licensed reference site; the fix is a *projection at the sidecar-build boundary* (the shipped DB physically cannot contain reference text, sigla, or source tags), enforced by a permanent leak-vector CI scan across titles/URLs/API/exports/SEO/logs. (b) **Epistemic honesty** — screening leads must never be citable as facts (band travels inseparably with the claim), "expert-verified" must not overclaim while the R-A independent audit is pending, and "no identification shown ≠ none exists" must appear on every empty state. (c) **The atlas cannot naively render the full graph** — an 89% liturgical giant component (15,969 MSS) is both a meaningless hairball and a browser-melting payload; server-side ego-network bounding + hard node/edge caps + canon-masked aggregation are mandatory, and every heavy query must run off the event loop or it takes down the shared 15.4 GB prod box.

## Key Findings

### Recommended Stack

The core recommendation adds **zero new runtime dependencies.** Split the visualization into three surfaces and match each to the lightest sufficient tool: bounded views (ego-network panel, interactive explorer ≤1–2K nodes) → native `ui.echart` graph series; the work→witnesses view → a plain NiceGUI table; the whole-corpus "stun" atlas (~52K nodes / 100Ks edges) → the existing precomputed-layout Canvas 2D starfield served as a static asset via `<iframe>`. The one load-bearing decision: **graph layout for the whole-corpus view MUST be precomputed offline in Python and baked to a positions asset** — no browser library force-lays-out 52K nodes interactively. This is already how the prototype (`same_work_spike/probe/scripts/build_atlas_draft.py`) works. See STACK.md.

**Core technologies:**
- **NiceGUI `ui.echart`** (bundled ECharts 5.x, already on `nicegui==3.8.0`): render bounded graphs (MS ego-network panel + interactive explorer) — zero new dep, native Hebrew/RTL canvas labels, `on('chart:click')` → `ui.navigate.to('/browse?...')` interop, comfortable to ~3K nodes.
- **Existing Canvas 2D starfield + precomputed layout** (in-repo, no library): the whole-corpus flagship atlas — already proven at 52K scale, self-contained HTML, no CDN, prod cost is just file serving.
- **SQLite (stdlib `sqlite3`)**: the distilled read-only product sidecar (`discovery.db`, ~130–160 MB) + server-side bounded-subgraph BFS over an indexed edge table — same mmap'd pattern as the existing sidecars, no graph DB.
- **Supabase (existing `supabase==2.28.0`)**: community judgment capture — reuse the corrections RLS + explicit-GRANT pattern; no new stack.
- **Build-time only (dev box, NEVER in `requirements.txt`):** numpy/scipy (already prod deps) + networkx + python-louvain (BSD). Keep python-igraph/leidenalg (GPL) offline-only.
- **Escalation (only if the atlas must be a live WebGL graph):** sigma.js v3.0.3 + graphology (MIT), vendored into `web/static/`, never a CDN.

### Expected Features

Precedent systems (Sefaria "Related Texts", KITAB/Passim, impresso Text Reuse, Zooniverse/FromThePage) establish clear table stakes; the module's signature differentiator is its **multi-band dual-lane design** (precision by default + high-recall "leads" on demand). See FEATURES.md.

**Must have (table stakes):**
- MS connections panel on the reading page ("identified as ⟨work⟩" + related MSS) — the Sefaria-familiar primitive; a viewer without connections reads as incomplete.
- Explicit glanceable confidence labels on every claim (named bands + color + word, never bare decimals for laypeople; color never the only signal).
- Click-through navigation from every connection to its target.
- On-demand evidence: the shared passage side-by-side — **but masked** (our MS text only; never the reference edition).
- Work → witness-list page (all carrier MSS, filterable by band + library).
- Recall-honesty disclaimer in-UI ("no identification shown ≠ none exists").
- Bilingual EN/HE + RTL parity on all new surfaces; login-gated community actions mirroring corrections.

**Should have (competitive):**
- Multi-band dual-lane design (precision-default + explicit "show uncertified leads" toggle) — the module's signature; never blend lanes silently.
- Corpus connection atlas / graph explorer (homepage flagship) — highest-value, highest-risk.
- Indirect-witness / citation surfacing (the flank-contrast "island" class — a discovery no join/keyword tool can produce).
- Evidence viewer showing only our manuscript's scholarly text (masking inverted into a positioning win).
- Measured-precision certificate surfaced in-UI; community judgment capture that feeds future certification.

**Defer (v2+ / later phases):**
- Atlas is the marketed flagship but the highest-risk surface — scope to work-centric ego-graphs, treat as the capstone, and it is the first thing to cut to fast-follow under schedule pressure.
- Precision-certificate auto-refresh / re-certification pipeline (snapshot ship this cycle).
- Text-reuse engine as `/parallels` backend (explicitly deferred by user 2026-07-19); desktop parity (web-only); collation / variant apparatus / stemma (deliberately never — anti-feature).

### Architecture Approach

An **integration architecture that conforms to fixed house patterns.** The discovery module adds a new read-only sidecar (`discovery.db`, distilled offline from the 2.9 GB research DB with masking applied *at build time*), a single `shared/discovery_service.py` reader (copy `FjmsService` verbatim: singleton factory, thread-local RO connection, graceful degrade when absent), the connections panel as a new **browse enrichment section** (a `fetch_discovery()` sibling to `fetch_pgp`/`fetch_fjms`, off the event loop), three new pages (`/work/{id}`, `/atlas`, `/leads`), and one new Supabase table (`work_witness_judgments`, split write via `get_user_client()` / read via a client-param service). See ARCHITECTURE.md.

**Major components:**
1. **Distillation script** (`scripts/build_discovery_sidecar.py`) — offline, never shipped: selects tier-A ids + bands + MS-MS edges + work metadata, **applies M-source masking**, emits the sidecar. Masking is a build-time projection, so runtime code cannot leak it.
2. **`discovery.db` sidecar** (`work` 4,093 / `identification` 275,894 / `connection_edge` 442,696 / `meta`) — ~130–160 MB, denormalized `library_code` for zero-join filters, span offsets into OUR HTR text only.
3. **`shared/discovery_service.py`** — the only reader; every heavy query wrapped in `await run.io_bound(...)`.
4. **MS connections panel** — modified `browse_enrichment.py` / `browse_state.py` / `browse.py` (reuses the generation-token staleness + batched-fetch machinery for free).
5. **`/work/{id}` witness page + `/atlas` explorer + `/leads` queue** — new pages mirroring `catalog_browse.py` / heavy-async-render / `discoveries.py`.
6. **`work_witness_judgments` Supabase table** — corrections-style RLS + **explicit GRANTs** (the old tables predate the 2026-05-30 rule and are a misleading template); append-only for a free audit trail.

### Critical Pitfalls

Top pitfalls (from 16 in PITFALLS.md, grounded in SEED-029 probe artifacts + house incident history):

1. **M-source provenance leak (HARD RELEASE BLOCKER)** — mask as a *projection at the sidecar-build boundary*, not a render-time filter; the shipped DB physically cannot contain reference text/sigla/source columns. Enforce with a permanent leak-vector CI scan across titles / URLs / API / exports / SEO-JSON-LD / atlas labels / logs. Once a title or `source='m_source'` field ships in an export or cached SEO snippet, it is unrecallable.
2. **Screening leads cited as facts** — bind the band label to the *claim string itself*, not to UI chrome. Every exported/copied/API-serialized claim carries its band inseparably; default to high bands, hide R-B/R-CANON behind an explicit toggle, and never surface internal codes (R-A/R-B/R-CANON).
3. **"Expert-verified" overclaim** — the R-A 0.889 band is single-expert, independent audit *pending*. Pin the label to a single `certification_status` source of truth; never use "certified"/"מאומת" for R-A until the auditor gate passes; guard-test it.
4. **Naive full-graph atlas melts browsers AND is meaningless** — the 89% liturgical giant component (15,969 MSS) is a hairball and a killer payload. Server-side ego-network bounding + hard ≤300–500 node/edge cap + canon-masked aggregates; precompute clusters/components in the sidecar, never on the request path.
5. **Heavy queries block the NiceGUI event loop and take down the shared box** — the 2026-05-25 NLI-hang failure class. Run all sidecar/graph queries off the event loop (`run.io_bound`); per-query timeouts + a concurrency cap (mirror `SEARCH_API_HEAVY_CONCURRENCY` → 503). Establish this in the de-risk spine *before* any UI lands.
6. **Supabase table without GRANTs / audit trail** (old tables are a bad template) + **community votes polluting the certified bands** — new table needs explicit GRANTs + append-only immutable rows; judgments are a *separate channel* feeding future curated certification, never auto-mutating a shipped band. Also: **deploy the sidecar FIRST, then code** (feature-flag fail-open when absent); **homepage CLS** (reserve height, lazy-load atlas JS); **i18n/RTL on graph labels** (manual bidi for canvas/SVG); **recall dishonesty** on empty states; **precision certificate** must be pre-registered/blind on the actual shipped population (never a cherry-picked stratum — R-CANON round 3 correctly REFUSED certification at 0.647).

## Implications for Roadmap

Based on combined research, a **condensed structure with a thin de-risk data spine first, then theme-grouped surfaces** (matching the "prefer condensed roadmaps" house lesson). Phase numbering continues from 132. A UX discuss-phase precedes planning (Phase 11 lesson, already noted in the milestone).

### Phase 1: Discovery data spine (sidecar + masking gate + service + de-risk)
**Rationale:** Everything reads from the sidecar; a wrong shape invalidates every surface. This phase proves the three riskiest things at once — masking (P1 blocker), sidecar size/shape (P6), and event-loop safety (P5) — before any UI is built.
**Delivers:** `scripts/build_discovery_sidecar.py` + curated `discovery_work_titles.csv` (neutral titles) + the distilled `discovery.db`; `shared/discovery_service.py` (copy `FjmsService`, RO thread-local conn, `is_available()`, graceful degrade, `run.io_bound` discipline + timeout/concurrency wrapper); feature flag for fail-open deploy.
**Addresses:** Discovery sidecar + band schema; masking helper (as a build-time projection).
**Avoids:** P1 (masked-first schema + leak-vector CI scan as a release gate), P5 (off-loop discipline established here), P6 (clean minimal schema + size/integrity assertions), P7 (feature flag + fail-open).

### Phase 2: Read surfaces — MS connections panel + work→witness page
**Rationale:** The panel is the lowest-risk, highest-reach UI (reuses the proven browse-enrichment path); the witness page is the click-through target for every work name. They share the band-labeling, evidence-viewer (our-text-only), recall-honesty, and masking-display logic, so build them together.
**Delivers:** `fetch_discovery()` enrichment section on `/browse` (high bands default + "show uncertified leads" toggle + evidence viewer + recall caveat); `/work/{work_id}` witness page (band/library filters, rows → `/browse`).
**Uses:** `ui.echart` for the bounded ego-network; the existing transcription/version-selector for the evidence span; the `library_filter` component (band facet added).
**Implements:** Browse enrichment hook (Pattern 2); `catalog_browse.py`-style witness page.
**Avoids:** P2 (band travels with the claim), P3 (single `certification_status` flag), P12 (i18n/RTL + `tr()` guard + HE render-smoke), P14 (recall caveat on empty states), P15 (bounded/timed-out/fail-open enrichment + LRU cache).

### Phase 3: Community judgment capture
**Rationale:** Depends on the read surfaces existing (there's nothing to judge otherwise) + Supabase auth; mirrors how corrections/comments layered onto browse.
**Delivers:** `work_witness_judgments` migration (RLS + **explicit GRANTs** + append-only + moderation status); `create_work_judgment()` write (`get_user_client()`) + `shared/discovery_judgments_service.py` read; verdict controls wired into the panel + witness page (login-gated).
**Implements:** Community-write pattern (Pattern 3), corrections split.
**Avoids:** P8 (GRANTs + RLS + audit trail), P9 (votes are a separate channel, never mutate bands), P10 (auth gate + rate limit + moderation + disagreement modeled + XSS-sanitize annotations).

### Phase 4: Leads queue
**Rationale:** The panel's lead-lane logic proven in Phase 2; the queue is just the corpus-wide view (`WHERE band IN ('R-B','R-CANON')`) — no new table.
**Delivers:** `/leads` page (mirrors `discoveries.py`), explicitly labeled not-certified, with per-category deprioritization/labeling.
**Avoids:** P16 (deprioritize/separately label the canon lane; flag the known Targum 0.483 systematic confusion; Bible IDs are not headline discoveries).

### Phase 5: Atlas / graph explorer + homepage promotion (capstone)
**Rationale:** Highest complexity and risk (hairball / giant component); should be the capstone AFTER the panel + witness page de-risk the data, even though it's the marketed flagship. Homepage promotion rides on the atlas existing.
**Delivers:** `/atlas` page with server-side capped neighborhood queries (canon-masked aggregates for any global view) using the existing Canvas 2D starfield (or `ui.echart` for bounded views); homepage flagship cards.
**Uses:** Existing precomputed-layout starfield asset; `<iframe>` embed; escalate to sigma.js v3 only if a live WebGL graph is required.
**Avoids:** P4 (server-side bounding + hard caps + no full-graph path), P5 (off-loop + timeout + concurrency cap), P11 (CLS-safe homepage: reserved height + lazy-loaded atlas JS + static teaser + render-smoke).

### Parallel track: Tier-A stratified precision certificate
**Rationale:** A measurement activity on the research side, not a software phase — but it gates the release copy ("expert-verified" vs "certified"). Architecturally it only touches `band_precision` / `certified` / `meta.ra_audit_status` in the sidecar. Run alongside Phase 1; must land before launch copy is finalized.
**Avoids:** P13 (pre-registered, frozen-frame, blind round on the *actual shipped* tier-A population — never a cherry-picked stratum; if the gate fails, ship as screening).

### Optional: Public API endpoints
`init_discovery_api()` following `init_search_api()` — `GET /api/work/{id}`, `GET /api/connections/{sys_id}`; allowlist-only Pydantic models (`extra='forbid'`, mandatory `band`, no `source`/`ref_*`). Depends only on the service (Phase 1).

### Phase Ordering Rationale

- **Data spine first** because the sidecar shape (and the masking projection) is the hard prerequisite for all five surfaces and the object of the UX discuss-phase; a wrong shape invalidates downstream work.
- **De-risk the two hard constraints (masking, event-loop) in Phase 1** — both are "wire it wrong once and it's a prod incident / unrecallable leak" failures, so they must be proven before UI reach.
- **Read surfaces before write surfaces** — judgment capture has nothing to attach to until the panel + witness page render; mirrors corrections/comments layering.
- **Atlas as capstone, not opener** — the marketed flagship is the riskiest surface; the rehearsal already proved a naive graph is a blob, so it comes after the panel/witness page de-risk the data and can be cut to fast-follow under pressure.
- **Grouping read surfaces (panel + witness page)** because they share masking-display, band-labeling, evidence-viewer, and recall-honesty logic — build the shared display helpers once.

### Research Flags

Phases likely needing deeper research / careful discuss-phase during planning:
- **Phase 1 (sidecar):** the neutral work-title curation approach — 3,468 works trace to M-source + 224 to Sefaria, so nearly every title needs a de-provenancing pass; how much bulk-derivation vs human review is a design decision. Also final band-selection decisions (still open) affect row counts.
- **Phase 5 (atlas):** MEDIUM — the STACK research strongly recommends the canvas starfield / `ui.echart` split, but the *scope* (ego-network vs aggregated census, giant-component aggregation strategy, live-vs-static) benefits from the UX discuss-phase; browser node-count thresholds are cross-source corroborated but not benchmarked on this exact corpus.
- **Precision certificate (parallel):** pre-registration of the estimand + gates is methodology-critical (P13) — a research-side activity, not software research.

Phases with standard patterns (skip research-phase):
- **Phase 2 (panel + witness page):** HIGH — reuses the documented browse-enrichment (`fetch_pgp`/`fetch_fjms`) + `catalog_browse.py` patterns; every recommendation points at a real file/line.
- **Phase 3 (community judgment):** HIGH — the corrections/Supabase write pattern is well-documented; the only new wrinkle is adding the GRANTs the old tables lack.
- **Phase 4 (leads queue):** HIGH — mirrors `discoveries.py` list/filter; the data is already in `identification`.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Grounded in the working full-corpus prototype + installed dependency baseline + verified versions/licenses. MEDIUM only on exact browser node-count thresholds (corroborated, not benchmarked on this corpus). |
| Features | MEDIUM | Precedent systems are HIGH on *what they do*; mapping to this specific mixed scholar/lay Genizah audience and the "right" UX is inferential. Band schema + masking constraint are internal-brief facts (HIGH). |
| Architecture | HIGH | Grounded in the actual research DB schema (counts measured via sqlite3) + real house sidecar/enrichment/community-write code; every recommendation cites a real file/function. MEDIUM only on final distilled row counts (depend on open band-selection decisions). |
| Pitfalls | HIGH | Grounded in the SEED-029 probe artifacts (PROBE-RESULTS, METHOD, E1 release notes, rehearsal atlases) + direct inspection of `supabase_setup.sql`, `corrections_service.py`, and the CLS/NLI-breaker/`safe_storage`/deploy-ordering history. |

**Overall confidence:** HIGH (the *how-to-build* is well-grounded; residual uncertainty is concentrated in UX specifics for the mixed audience and open band-selection decisions, both resolvable in the UX discuss-phase and Phase 1).

### Gaps to Address

- **Neutral work-title curation (masking source of truth):** ~4,093 works, most tracing to M-source/Sefaria, need reviewed neutral canonical titles. → Resolve the curation/review workflow in Phase 1 planning; the leak-vector denylist regex is the backstop.
- **Final band-selection / distilled row counts:** which bands and thresholds ship affects sidecar size and the leads queue. → Settle in the UX discuss-phase before Phase 1 distillation.
- **Atlas scope (flagship UX):** static starfield vs live interactivity, ego-network vs aggregated census, giant-component aggregation. → UX discuss-phase; escalation to sigma.js is a bounded, well-understood fallback if live WebGL is demanded.
- **Precision-certificate estimand:** must be pre-registered and blind on the shipped population before grading. → Parallel research track, gated before release copy.
- **Browser rendering thresholds not benchmarked on this corpus:** the ~3K-node ECharts ceiling / 52K starfield are cross-source + prototype-corroborated but not load-tested here. → Validate with a render-smoke test during Phase 2/Phase 5.
- **NiceGUI render-smoke gap:** headless pytest misses the async render path (house lesson). → Require a live-client render-smoke test on every new surface (panel, witness page, atlas, homepage).

## Sources

### Primary (HIGH confidence)
- `same_work_spike/probe/` — `scripts/build_atlas_draft.py`, `PROBE-RESULTS.md`, `METHOD.md`, `results/E1-ROUND2/3-RELEASE.md`, `CODEX-BRIEF-atlas.md`, `data/fullcorpus_v2.db` (counts measured directly: tier-A 275,894 / 4,093 works / 52,497 MSS / 442,696 distinct MS-MS edges; M-source 3,468 + Sefaria 224 works; 89% liturgical giant component / 15,969 MSS; R-A 0.889 / R-B 0.859 / R-CANON 0.647) — the first-party research program.
- House code: `shared/fjms_service.py`, `shared/thread_local_db.py`, `web/pages/browse_enrichment.py` / `browse.py` / `browse_state.py`, `shared/corrections_service.py`, `web/supabase_client.py` (`get_user_client`, `create_correction`), `supabase_setup.sql`, `web/main.py`, `web/pages/home.py` — the fixed integration patterns.
- `requirements.txt` / `requirements-lock.txt` + installed-environment ground truth (nicegui 3.8.0, numpy 2.4.3, scipy 1.17.1, supabase 2.28.0; networkx/python-louvain/igraph/leidenalg dev-box-only).
- `CLAUDE.md` + `MEMORY.md` — 2026-05-30 GRANT rule, scp-DB-first deploy, CLS fix, NLI-breaker/event-loop incident, `safe_storage`/multitenant invariants, 15.4 GB box + allocator-ratchet, render-smoke gap, "catalogue = recall yardstick, never acceptance evidence", "prefer condensed roadmaps".
- `.planning/PROJECT.md` (Current Milestone v9.0.0) + `.planning/seeds/SEED-029-*.md` — milestone scope, band schema, masking rationale, recall honesty.
- NiceGUI ECharts docs (`ui.echart` events, `run_chart_method`); sigma.js / graphology / cytoscape.js docs (versions + MIT licenses).

### Secondary (MEDIUM confidence)
- Precedent-system feature analysis — Sefaria "Related Texts", KITAB/Passim DiffViewer, impresso Text Reuse at Scale ("a lot going on" evaluation finding), Zooniverse consensus retirement (anti-pattern for expert claims), FromThePage provenance/versioning model.
- Apache ECharts + PMC "Graph visualization efficiency" study — ~3K-node Canvas ceiling / D3-WebGL ~7K (motivates offline precompute; not benchmarked on this corpus).
- Scholarly-certainty / provenance-UI research — certainty-gradient classification, "color-as-glanceable-credibility" over-trust risk.

### Tertiary (LOW confidence)
- Exact final distilled row counts / sidecar size (~130–160 MB estimate) — depend on open band-selection decisions, validate in Phase 1.
- The "right" UX for the mixed scholar/lay audience — inferential from precedent; resolve in the UX discuss-phase.

---
*Research completed: 2026-07-19*
*Ready for roadmap: yes*
