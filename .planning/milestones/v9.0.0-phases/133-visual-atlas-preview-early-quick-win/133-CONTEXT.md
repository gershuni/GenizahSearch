# Phase 133: Visual Atlas Preview (early quick win) - Context

**Gathered:** 2026-07-20
**Status:** Ready for planning

<domain>
## Phase Boundary

Ship the v9.0.0 milestone's **first deployable artifact**: a static, canon-masked visual atlas of the Cairo Genizah **corpus connection map**, served on a standalone `/atlas` beta page. The layout is **precomputed offline** (never at request time), forked from the prototype `same_work_spike/probe/scripts/build_atlas_draft.py`. The preview carries **no claim-level statements** (no work–witness identifications, no confidence bands, no precision numbers) and deploys **early**, ahead of every claim surface, under the REL-01 ATLAS-PREVIEW exception.

Delivers requirement **ATLAS-01**. Also **locks ATLAS-02's "primary graph object" decision** (a prerequisite the ROADMAP requires fixed before the bake), and **revises ATLAS-03/REL-01** to widen the ATLAS-PREVIEW exception to include a claim-free homepage teaser (see Requirements Impact).

**In scope:** the offline bake → shipped static asset; the `/atlas` NiceGUI page + interactive renderer; a dedicated atlas-preview feature flag; a reusable asset-level masking scan; a beta-tagged nav link + a claim-free homepage teaser; EN/HE + RTL page chrome.

**Out of scope (later phases):** the claim-model sidecar (134), bands/certificate (135), connections panel + work pages (136), community judgments (137), leads queue (138), the server-bounded drill-down explorer + homepage discovery band + full release gate (139). No discovery-claim data of any kind renders here.

</domain>

<decisions>
## Implementation Decisions

### Primary Graph Object (locks ATLAS-02)
- **D-01:** The atlas's primary rendered object is the **manuscript** — each star = one connected manuscript (`sys_id`), positioned by the offline force-layout of its algorithmic community; communities read as luminous "regions." This is the **durable** primary object for the whole atlas (preview → the Phase 139 explorer), not just the preview. Works-as-nodes are **rejected as the primary object** (a work node asserts work–witness identification = claim-level, forbidden here; Codex also warns against making the projected manuscript clique primary evidence). A work-lens / bipartite (manuscripts↔works) view remains a possible **future secondary** view.
- **D-02:** Default categorical color = **FJMS domain** (מקרא / פיוט / הלכה / …); **library** is an alternate recoloring **toggle** (never simultaneous). Regions are framed honestly as **"algorithmically detected regions,"** labeled by dominant domain composition — never as works, genres, or historical schools.
- **D-03:** The ~43K liturgical **"giant" component is recursively sub-divided** (Louvain split, prototype `SPLIT_AT≈800`) into legible sub-regions rather than shown as one illegible blob.

### Claim-free Content & Masking
- **D-04:** **Remove the gold "discovery candidates" overlay entirely** — the toggle, the per-MS gold stars, and any discovery counts. Per-manuscript discovery highlighting asserts an identification status = a claim-level statement, which ATLAS-01 forbids in the preview. Discoveries return in the certified claim surfaces (Phases 136–138).
- **D-05:** **Keep catalogue manuscript titles as region labels, as-is** (no special "representative/cluster-label" framing wording). This is a **deliberate, informed owner decision** under the ATLAS-PREVIEW exception. Rationale: catalogue titles come from `libraries.csv` (**our own catalogue data — masking-safe, not M-source**), and SC#1 explicitly permits "cluster-level visualization." The standing algorithmic-provenance banner (D-15) still applies. **NOTE FOR VERIFIER:** this is intentional — do NOT flag it as SC#1 "work-identification" non-compliance; the owner weighed the "region = work" reading and accepted it.
- **D-06:** Per-star hover tooltips keep that manuscript's own **shelfmark + domain + library + catalogue title** (our catalogue data, masking-safe). Shelfmark labels surface on deep zoom.
- **D-07:** The masking check is a **reusable scan script — the forerunner of the permanent DATA-05 CI guard**. It scans **both** (a) the built atlas asset (HTML/JSON, every embedded string incl. region titles + tooltips) and the page's rendered output, **and** (b) committed repo content, for the M-source name/aliases + reference-corpus sigla patterns. Run as a Phase-133 exit gate; Phase 134 extends it to the sidecar. Its first run performs the one-time committed-repo cleanup verification (any uncommitted prototype M-source strings — e.g. the `genizah_translations.py` working-tree additions — must be scrubbed before commit).

### Interactivity & Byte Budget
- **D-08:** Ship the **full interactive prototype experience**: zoom/pan, title + shelfmark search, domain↔library color toggle, library-filter panel (hide-one / solo-one), click-a-region → focus "constellation" (member-list panel), **click-through to `/browse`** (opens the manuscript — not claim-level, and makes the atlas genuinely useful), and the **reduced-motion-aware bloom-in intro** (skippable).
- **D-09:** Keep **all 62,414 connected-manuscript stars** and **all per-MS node metadata always** (nodes are never trimmed). Keep **all per-MS edges** available, **drawn on zoom/focus** (prototype behavior); at the zoomed-out overview show baked **aggregate community flows** + stars (the raw pairwise-edge web is an illegible hairball at overview anyway).
- **D-10:** Shrink the payload — **typed/delta-encoded arrays instead of JSON** for nodes/edges + **Brotli** over the wire — targeting a **generous beta byte cap ~6 MB compressed** (the full unoptimized bake is ~13 MB). This is the **preview's own cap** and feeds `discovery-budgets.md` in Phase 134; the Phase 139 server-bounded explorer (ATLAS-02) tightens it later. Layout fully baked offline (never at request time); **CLS-safe render** (reserve canvas dimensions).
- **D-11:** Catalogue **"dust" (the ~193K unconnected manuscripts) is NOT shipped in the beta** (deferred — it would blow the byte budget). The preview shows the connected corpus and is framed as a "connections atlas."

### Page Framing & Beta Labeling
- **D-12:** Standalone **`/atlas`** NiceGUI route (`@ui.page` in `web/main.py`), serving the **static baked asset** (via `app.add_static_files('/static', …)` or an embedded self-contained page). No request-time computation.
- **D-13:** Gate with a **dedicated atlas-preview feature flag** (following `web/feature_flags.py::_env_enabled`) so the preview page can be **ON in prod for the beta** while the **main discovery flag** (gating the claim surfaces) stays **OFF per REL-01**. With the flag OFF or the asset absent, `/atlas` hides cleanly (zero errors); the rest of the app is untouched.
- **D-14:** Entry points: a **"beta"-tagged link in the site's top nav** **and** a **claim-free homepage teaser card** (see Requirements Impact / D-16). No live homepage graph or suggestions.
- **D-15:** **Bilingual EN/HE page chrome following the site's active UI language** (correct RTL for HE); baked atlas labels (domain names) carry both languages and select by active language. Keep the standing honesty banner ("positions & clusters are algorithmically derived from textual connections; proximity is not physical provenance"). Beta labeling = a **"Beta / preview" badge** in the header + a one-line intro naming it a preview of the connections work.

### Requirements Impact (owner-authorized 2026-07-20) — MUST sync docs
- **D-16:** **Extend the ATLAS-PREVIEW exception to permit a claim-free homepage teaser in Phase 133.** The teaser is a small **CLS-safe static card** linking to `/atlas`, gated by the atlas-preview flag, carrying **no claim-level statements**, passing the masking scan + i18n/RTL basics, and set **`noindex` until the REL-01 gate**. This **revises ATLAS-03 and REL-01**, which currently hold the homepage band OFF until Phase 139. **ACTION REQUIRED before/at planning:** sync `.planning/REQUIREMENTS.md` (ATLAS-03 + REL-01) and the `.planning/ROADMAP.md` Phase 133/139 text to widen the exception. Captured here as the authoritative locked decision; the requirements-doc edit was deliberately left out of this discuss-phase to avoid silently rewriting the Codex-converged requirements.

### Claude's Discretion
- Clustering algorithm (prototype uses Louvain; Codex recommends **Leiden** for better-behaved communities — planner's call), exact force-layout parameters, the typed/delta edge-encoding format, intro choreography details, and the no-WebGL / asset-absent **fallback** (the prototype is **Canvas 2D**, so broadly compatible — a static-image/cluster-cards fallback covers render failure or a missing asset). Record a **deterministic seed + algorithm/version metadata** with the bake so the layout is reproducible and users' spatial memory survives rebuilds.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

> Provenance-masking note: the `same_work_spike/probe/**` tree is **gitignored research** (on disk for the researcher/planner). Its contents must **never be committed** and must **never leak M-source** into any product surface. Reference the restricted source in committed material only as **"M-source."**

### Phase / milestone docs
- `.planning/ROADMAP.md` — Phase 133 detail (goal, 5 success criteria, ATLAS-PREVIEW exception) + the milestone framing and REL-01 gate sequence.
- `.planning/REQUIREMENTS.md` — **ATLAS-01** (this phase), **ATLAS-02** (primary graph object — locked here in D-01), **ATLAS-03** (homepage band — revised by D-16), **PERF-01** (byte/latency budgets), **REL-01** (release gate + ATLAS-PREVIEW exception — revised by D-16), **DATA-05** (permanent masking guard this phase forerunns).
- `.planning/PROJECT.md` — milestone goal + the M-source masking hard constraint + epistemic-honesty posture.

### Research provenance (gitignored — read-only for planning; never commit; never leak)
- `same_work_spike/probe/scripts/build_atlas_draft.py` — **the prototype offline bake** ATLAS-01 SC#1 names; the starting point for the shipped bake + the self-contained Canvas 2D renderer (no CDN).
- `same_work_spike/probe/results/CODEX-ATLAS-answer.md` — atlas design critique (form, offline-bake pipeline, "stun" design, honesty rules, concrete pitfalls, the eventual Sigma.js-v3 stack for Phase 139).
- `same_work_spike/probe/data/fullcorpus_v2.db` — source research DB; table **`accepted_pairs_canonmask`** (canon-masked `sys_id` page pairs + `aligned_len`/`density`/`flank_class`; no reference text).
- `same_work_spike/probe/scripts/build_reuse_graph.py` — the pipeline the prototype forked (FJMS domain color groups, recursive-Louvain clustering, `/browse` link construction).
- `same_work_spike/probe/METHOD.md` + `same_work_spike/probe/SYNTHESIS-AND-PLAN.md` — method + the relation-type distinctions (page-pair vs manuscript-pair vs physical join vs textual overlap) that must not be conflated.
- `.planning/seeds/SEED-029-fragment-textual-similarity-same-work-detection.md` — the research seed this milestone productizes.

### Web plumbing
- `web/main.py` — `@ui.page('/route', title=…)` registration pattern (bilingual titles), `app.add_static_files('/static', STATIC_DIR)`, `app.mount('/api', …)`; the `/` home page (homepage-teaser host) and site nav.
- `web/feature_flags.py` — `_env_enabled(name, default)` flag pattern (model for the dedicated atlas-preview flag).
- `libraries.csv` — catalogue titles + `library_code` (region labels, tooltips, library coloring — our data, masking-safe).
- `fist_data/fjms_enrichment.db` — `domains` table (FJMS domain / parent-domain for the default coloring).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`build_atlas_draft.py`** — the entire offline bake (domain load, recursive-Louvain clustering, community force-layout, phyllotaxis star scatter, aggregate flows) + a self-contained Canvas 2D renderer with zoom/pan/search/library-filter/focus-constellation/click-through and a reduced-motion-aware intro. The shipped bake is a hardened fork of this (strip discovery overlay per D-04; optimize encoding + Brotli per D-10; bilingual chrome per D-15).
- **`web/main.py` `@ui.page` + `add_static_files`** — the exact registration + static-serving path for the new `/atlas` route and the baked asset.
- **`web/feature_flags.py::_env_enabled`** — the env-flag idiom for the atlas-preview flag (D-13).
- **Site language / RTL state** — the same mechanism other pages use to render EN/HE + RTL chrome (D-15).

### Established Patterns
- Static assets under `/static`; feature flags via env with a code default; NiceGUI `@ui.page` routes with bilingual titles; the honesty-banner + `prefers-reduced-motion` handling already present in the prototype.

### Integration Points
- New `@ui.page('/atlas')` in `web/main.py`; new atlas-preview flag in `web/feature_flags.py`; a beta-tagged nav link in the site header/nav; a claim-free teaser card on the `/` home page; a bake script (fork of `build_atlas_draft.py`) emitting the optimized asset into `/static` (or a served data dir); the reusable masking scan wired as a build/exit gate.

</code_context>

<specifics>
## Specific Ideas

- Codex's verdict frames the **eventual** stack as **Sigma.js v3 + binary multilevel assets** — but that targets the Phase 139 server-bounded drill-down explorer (ATLAS-02). The **Phase 133 preview** deliberately uses the existing **self-contained Canvas 2D** approach, which already satisfies "static asset, no CDN, no request-time layout" and is broadly device-compatible.
- The prototype's bottom honesty banner (HE + EN: "positions & clusters are algorithmic, not physical provenance") is the model for the standing D-15 banner.
- "Whole corpus" honesty: the beta shows the **62,414 connected** manuscripts and is named a **connections atlas** (per Codex — don't claim "whole Genizah" without the faint catalogue dust, which is deferred, D-11).

</specifics>

<deferred>
## Deferred Ideas

- **Catalogue "dust"** (~193K unconnected manuscripts) for a literal whole-Genizah view — Codex later add-on; deferred past the beta (byte budget).
- **Sigma.js v3 + binary multilevel assets** rebuild for the server-bounded drill-down explorer — **Phase 139 (ATLAS-02)**.
- **Work-lens** (bipartite manuscripts↔works) as a secondary view — future.
- **Per-region companion edge chunks** (serve per-MS edges per focused region instead of embedding the full set) — an optimization to reach for only if the ~6 MB beta cap proves tight.

### Reviewed Todos (not folded)
- `todo.match-phase 133` returned only **low-relevance keyword collisions** (desktop corrections migration, NLI MARC crawl, unified metadata search, reading-desk UX fixes, etc.) — none relate to the atlas preview. Reviewed; **none folded**.

</deferred>

---

*Phase: 133-Visual Atlas Preview (early quick win)*
*Context gathered: 2026-07-20*
