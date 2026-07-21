# Roadmap: GenizahSearch

## Milestones

- **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)
- **v5.9.0 Multi-Source Image & Metadata Integration** -- Phases 29-34 (shipped 2026-02-16)
- **v6.0.0 Local Data Architecture** -- Phases 35-40 (shipped 2026-02-22)
- **v6.1.0 Catalog Browse & Navigation** -- Phase 41 (shipped 2026-02-27)
- **v6.5.0 Search UX & Filtered Search** -- Phases 42-46 (shipped 2026-03-14)
- **v7.0.0 Fragment Puzzle** -- Phases 47-52 (shipped 2026-03-17)
- **v7.1.0 FIST Gap Fill** -- Phase 53 (shipped 2026-03-19)
- **v7.6 Search Refinement & Scholarly Joins** -- Phases 54-57 (shipped 2026-03-31)
- **v7.7 Volume-Aware Browse** -- Phases 58-61 (shipped 2026-04-01)
- **v7.8 Structural Foundation** -- Phases 63-66 (shipped 2026-04-15)
- **v7.9 Decomposition** -- Phases 67-76 (complete 2026-04-17)
- **v7.10 Search API** -- Phases 77-83 (shipped 2026-05-05)
- **v7.11 CUDL Coverage & Synthetic Inventories** -- Phases 84-86 (shipped 2026-05-12)
- **v7.12 Multitenant Architecture (Path B)** -- Phases 87-92 + 92.1 + 92.2 + promoted 999.1/999.4 (shipped 2026-05-18)
- **v7.13 Research-Grade Downloads & PGP Filter** -- Phases 93-94 (shipped 2026-05-21)
- **v7.14 My Library — Local Document Search** -- Phases 95-98 (shipped 2026-05-24; closed 2026-05-27)
- **v7.15 My Library Visual** -- Phases 99-101 (shipped 2026-05-28). See `milestones/v7.15-ROADMAP.md`
- **v7.16 Hebrew PDF Text Quality** -- Phase 102 + no-phase quality work (shipped 2026-06-01). See `milestones/v7.16-ROADMAP.md`
- **v8.0.0 Dicta Rebrand & Joins Lab** -- BRAND (no-phase) + Phases 103, 105 (folded from v7.17; Phase 104 → EXP-F3) + Phases 106-110 Joins Lab (shipped 2026-06-09; closed 2026-06-11). Component B (JSA-01/02/03 + JWB-05) + web Joins Lab UI deferred post-v8.0.0. See `milestones/v8.0.0-ROADMAP.md`
- **v8.1.0 Desktop Telemetry** -- Phases 111-116 (shipped 2026-06-16; closed 2026-06-16). See `milestones/v8.1.0-ROADMAP.md`
- ✅ **v8.2.0 Web Joins Lab, FGP Transcriptions & Hebrew Search** -- Phases 117-121 (shipped 2026-06-23, both apps)
- ✅ **v8.3.0 God-File Decomposition + Search & Browse UX** -- Phases 122-129 (shipped 2026-06-29, both apps; closed 2026-06-30). Decomposition (122-127, zero behavior change) + SEED-025 Space-scroll + SEED-026 library filter. See `.planning/milestones/v8.3.0-ROADMAP.md`.
- ✅ **v8.4.0 Dual-Mode Library Filter** -- Phases 130-131 (shipped 2026-07-01, both apps; closed 2026-07-01). Evolved the v8.3.0 inclusion-only allowlist into a dual-mode (Show-only / Hide) library filter persisted across searches, at full web + desktop parity. Evolution of SEED-026. See `milestones/v8.4.0-ROADMAP.md`.
- ✅ **v8.4.1 Public API Dual-Mode** -- Phase 132 (shipped 2026-07-01, web; closed 2026-07-01). The public-API half of the dual-mode filter (DMF-11): `library_filter_mode` (include/exclude) on `POST /api/search` + `/api/parallels`, backward-compatible; skill clients gained `--library-mode`. Web point-release on the 8.4.0 tree (no version.py bump/tag). See `milestones/v8.4.1-ROADMAP.md`.
- 🚧 **v9.0.0 Discovery — Same-Work Identification & Connection Atlas (web)** -- Phases 133-139 (in planning, started 2026-07-20)

---

## Current Milestone: v9.0.0 Discovery — Same-Work Identification & Connection Atlas (web-only)

Fold the SEED-029 corpus-wide same-work text-reuse map (275,894 tier-A page-level identifications on 52,497 MSS across 4,093 works; bands R-A 0.889 / R-B 0.859 / R-CANON 0.647) into genizahsearch.com as a multi-band discovery module. The journey opens with an **early quick win** — the static Visual Atlas Preview (Phase 133), the milestone's FIRST deployable artifact, shipped as a standalone beta page under the REL-01 ATLAS-PREVIEW exception (no claim-level statements, asset-level masking, behind the flag) — then runs strictly along the REL-01 gate sequence: a thin de-risk **data spine** (masked, versioned sidecar + async service + budgets), the **certificate & band-display** contract (with a pre-registered tier-A precision measurement grading in parallel), the **read surfaces** (browse connections panel + work→witnesses pages), **community judgments** (Supabase + voting), the **leads queue** (the high-recall screening lane), and finally the **atlas drill-down + homepage promotion** capstone, with the main discovery feature flag / SEO / full homepage discovery band held OFF until the full release gate passes (a DEDICATED atlas-preview flag ships the Phase 133 beta and a claim-free homepage teaser early under the widened ATLAS-PREVIEW exception). Integration milestone, not greenfield: almost no new runtime stack (native `ui.echart`, existing Canvas 2D starfield, read-only SQLite sidecar, Supabase corrections pattern). Two hard blockers dominate the risk profile — **M-source provenance masking** (structural, at the build boundary) and **epistemic honesty** (band label travels inseparably with every claim; "expert-verified" must not overclaim while the R-A audit is pending; "no identification shown ≠ none exists" everywhere).

**UX discuss-phase precedes Phase 133/134 planning** (Phase 11 lesson). Its decisions feed the atlas bake and the spine and are NOT separate phases:

- **ATLAS-02** — the atlas primary graph object (works vs manuscripts vs clusters), fixed BEFORE sidecar/layout design — i.e., before the Phase 133 preview layout bake.
- **DATA-01** — bilingual display wording for the frozen relation vocabulary (the semantic engine→claim_type mapping is already frozen; only wording is deferred).
- **BAND-04** — per-surface recall-honesty disclaimer wording (fixed at application time per surface).
- Also settled here: final band-selection / distilled row counts, the neutral work-title curation/review workflow, and atlas scope (static starfield vs bounded interactivity).

## Phases

**Phase Numbering:** integer phases are planned milestone work; decimal phases (e.g., 133.1) are urgent insertions marked INSERTED, appearing between their surrounding integers.

- [ ] **Phase 133: Visual Atlas Preview (early quick win)** - Offline layout bake → static, canon-masked corpus-overview asset on a standalone `/atlas` beta page, deployed early under the REL-01 atlas-preview exception.
- [ ] **Phase 134: Discovery Data Spine** - Masked, versioned `discovery.db` sidecar + async DiscoveryService + frozen-frame & budget artifacts; proves masking, event-loop safety, and fail-open.
- [ ] **Phase 135: Precision Certificate & Confidence Bands** - Data-driven four-band display contract + bilingual methods page + pre-registered tier-A precision measurement (grades in parallel).
- [ ] **Phase 136: Read Surfaces — Connections Panel & Work→Witnesses** - Browse "computed identifications" panel (banded, masked evidence) + `/work/{id}` witness-map page grouped by codicological unit.
- [ ] **Phase 137: Community Judgments** - Supabase `work_witness_judgments` (RLS + GRANTs + append-only) + ✓/?/✗ voting UI as a separate, non-band-affecting layer.
- [ ] **Phase 138: Leads Queue** - `/leads` high-recall R-B screening lane, explicitly uncertified, canon-lane caveated, same voting.
- [ ] **Phase 139: Atlas Drill-down, Homepage & Release Hardening** - Server-bounded drill-down explorer (absorbing/upgrading the preview page) + CLS-safe homepage band + SEO/i18n/RTL/a11y/observability + the REL-01 flag-flip gate.

## Phase Details

### Phase 133: Visual Atlas Preview (early quick win)

**Goal**: The milestone's FIRST deployable artifact ships: a static, canon-masked visual atlas of the corpus connection map on a standalone `/atlas` beta page — precomputed offline from the research data, showing no claim-level statements, deployed early under the REL-01 ATLAS-PREVIEW exception.
**Depends on**: Nothing (first phase; self-contained — does NOT depend on the claim-model sidecar). Requires the UX discuss-phase ATLAS-02 decision (atlas primary graph object: works vs manuscripts vs clusters) BEFORE the layout bake, plus the atlas-scope decision. Deploy of THIS page is governed by the REL-01 ATLAS-PREVIEW EXCEPTION (owner, 2026-07-20); the full REL-01 gate sequence still governs every other discovery surface.
**Requirements**: ATLAS-01 (+ a claim-free homepage teaser under the widened ATLAS-03/REL-01 ATLAS-PREVIEW exception — owner, Phase 133 discuss 2026-07-20)
**Success Criteria** (what must be TRUE):

  1. An offline layout bake from the research data (the prototype `same_work_spike/probe/scripts/build_atlas_draft.py` approach) produces a canon-masked aggregated corpus-overview STATIC asset — layout never computed at request time; cluster/shelfmark-level visualization only, with NO claim-level statements (no work–witness identifications, no bands, no precision numbers); region/cluster labels come only from our own catalogue titles (libraries.csv — masking-safe catalogue metadata, distinct from the DATA-04 reviewed neutral WORK titles), reviewed neutral titles, or are omitted.
  2. A standalone `/atlas` beta page serves the static asset behind the discovery feature flag; with the flag OFF or the asset absent, the page hides cleanly with zero errors and the rest of the app is untouched.
  3. An asset-level masking scan passes over the shipped atlas asset and the page's rendered output (no reference-corpus text, sigla, or provenance; the restricted source appears in committed material only as "M-source") — the forerunner of the permanent DATA-05 guard.
  4. The page meets its performance basics — static payload within the PERF-01 atlas byte cap, no request-time computation, CLS-safe render — and EN/HE + RTL basics for this page only (page chrome + labels).
  5. The beta page is live in production ahead of every claim surface, clearly labeled as a beta/preview, per the owner's first-deployable-artifact priority.
  6. A small claim-free homepage teaser card links to `/atlas` under the widened ATLAS-PREVIEW exception — CLS-safe, no claim-level statements, gated by the dedicated atlas-preview flag, masking-scan-clean, EN/HE + RTL, and `noindex` until the REL-01 gate (the full homepage discovery band remains Phase 139).

**Plans**: 6 plans (5 waves)

- [x] 133-01-PLAN.md — (wave 1, atomic precondition) Masking scan (D-07, multi-surface + recursive) + working-tree M-source scrub + gitignore-first + atlas_data/ off `/static`
- [x] 133-02-PLAN.md — (wave 2) Frozen binary schema + offline atlas bake: strip overlay, EXACT eligible==placed node-set, content-hashed typed-array + Brotli asset, golden fixture + pinned atlas-bake CI
- [x] 133-03-PLAN.md — (wave 3) ATLAS_PREVIEW flag + authoritative web/atlas_assets loader + `atlas_preview_available()` predicate + /atlas route + off-static Brotli data routes (q-negotiation) + page chrome + HE strings + env/CODE_INDEX docs
- [x] 133-04-PLAN.md — (wave 4) Frozen-schema decoder JS + XSS-safe interactions + /browse click-through + render-smoke + Node golden cross-language decode + DOM-XSS test
- [x] 133-05-PLAN.md — (wave 4) Claim-free homepage teaser card gated on the shared availability predicate (noindex /atlas target)
- [ ] 133-06-PLAN.md — (wave 5, checkpoint) Production deploy: asset-first upload (off static root) → flag set → restart → live render/Brotli/noindex/EN-HE/masking smoke → rollback drill (SC#5) — **LOCAL portion done (Tasks 1-2: real bake `atlas-v1-61519a85a2d0` + capture helper + four-surface test + deploy docs, both masking gates exit 0); Tasks 3-4 = human production deploy PENDING**

**UI hint**: yes

### Phase 134: Discovery Data Spine

**Goal**: A masked, versioned discovery sidecar and one async service exist so every downstream surface reads banded same-work claims safely — with provenance masking, event-loop safety, and fail-open behavior all proven before any claim UI is built.
**Depends on**: Phase 133 in sequence only (the preview is self-contained; no data dependency). Opens the REL-01 main gate sequence (claim semantics + masked schema → title map + sidecar + frozen-frame). Consumes the remaining UX discuss-phase decisions (DATA-01 relation wording, band-selection, title-curation workflow).
**Requirements**: DATA-01, DATA-02, DATA-03, DATA-04, DATA-05, DATA-06, DATA-07, DATA-08, DATA-10, PERF-01
**Success Criteria** (what must be TRUE):

  1. The offline distillation produces `discovery.db` (≤ 300 MB, schema-versioned filename, release contract + `PRAGMA integrity_check`) carrying both claim families (work–witness with page→witness aggregation; MS–MS relation claims with child page-alignment records) each with a deterministic namespaced `claim_id`, exactly one band per claim key, codicological witness-unit memberships, and human-reviewed neutral work titles — with ZERO reference text, sigla, or provenance columns (evidence stored only as offsets into our HTR text with the snapshot hash recorded).
  2. A permanent CI leak-vector guard scans the shipped sidecar (schema + every cell), every product surface (including the Phase 133 atlas assets), and committed repo content, and fails the build if the restricted source appears as anything other than the codename "M-source"; the one-time cleanup verification passes on first run.
  3. All web access flows through one async DiscoveryService with per-query timeouts, bounded concurrency, indexed bounded queries, LRU browse-enrichment caching, and server-side pagination; under overload the caller gets a "temporarily unavailable" response (never a hang) and heavy queries never block the event loop.
  4. With the feature flag OFF, or the sidecar absent / corrupt / incompatible, every discovery surface hides cleanly with zero errors and the rest of the app stays fully available; deploy is temp-upload → verify → atomic rename → code, with a documented rollback + reproducible rebuild recipe.
  5. The version-controlled frozen-frame artifact (`discovery-frames.md`: per-band dedup counts, page→claim dedup formula, overlap-resolution counts, frame content hash) and the acceptance-budget artifact (`discovery-budgets.md`: browse-enrichment p95 ≤ 150 ms, atlas/work/leads caps + timeouts, ≤ 250 MB added RSS) are committed as phase exit criteria — frame frozen so CERT-01 can freeze against it.

**Plans**: 8 plans

Plans:
**Wave 1**

- [ ] 134-01-PLAN.md — Freeze the sidecar schema + deterministic claim_id/unit_id id module (wave 1)
- [ ] 134-02-PLAN.md — Masking guard --scan-sqlite + R-source tokens + gitignore + PERF-01 budgets doc (wave 1)

**Wave 2** *(blocked on Wave 1 completion)*

- [ ] 134-03-PLAN.md — Deterministic masking-safe fixture DB + build-output invariant tests (wave 2)

**Wave 3** *(blocked on Wave 2 completion)*

- [ ] 134-04-PLAN.md — Offline distillation build script (opaque ids, both claim families, bands, witness units, evidence offsets, review artifact) (wave 3)
- [ ] 134-05-PLAN.md — Fail-closed versioned loader + DISCOVERY_ENABLED flag + startup wiring (wave 3)

**Wave 4** *(blocked on Wave 3 completion)*

- [ ] 134-06-PLAN.md — Async DiscoveryService chokepoint (off-loop, timeouts, bounded concurrency, LRU, pagination) (wave 4)
- [ ] 134-07-PLAN.md — Owner title-review -> re-distill real discovery.db -> freeze discovery-frames.md (wave 4, human gate)

**Wave 5** *(blocked on Wave 4 completion)*

- [ ] 134-08-PLAN.md — PERF-01 measurement + budgets finalization + deploy/rollback/rebuild recipe (wave 5, human gate)

### Phase 135: Precision Certificate & Confidence Bands

**Goal**: The four-band confidence model is displayable everywhere with data-driven labels and honest status copy, a bilingual methods page documents each band, and a pre-registered tier-A precision measurement is drawn against the frozen frame and enters grading — so no claim can ever appear without its band and status.
**Depends on**: Phase 134 (frozen-frame artifact + sidecar band metadata). CERT-01's frame freezes AFTER Phase 134 distillation stabilizes; its grading runs as a research track IN PARALLEL with Phases 136–138 and its completed certificate gates public promotion in Phase 139 (REL-01).
**Requirements**: BAND-01, BAND-02, BAND-03, BAND-04, BAND-05, CERT-01, CERT-02
**Success Criteria** (what must be TRUE):

  1. Every displayed and serialized/copied claim carries its confidence-band label inseparably, and each band shows ONLY a number measured in its own unit paired with its status — tier-A shows no precision number until its certificate lands, R-A shows 0.889 with "audit pending", R-B/R-CANON show screening values with "not certified".
  2. Band labels and precision copy are data-driven from sidecar metadata: flipping certification status changes the copy with NO code change (R-A reads "expert-verified (independent audit pending)" until the audit passes).
  3. High-confidence bands show by default; screening bands appear only behind an explicit "show more possible identifications" toggle (screening rows labeled "possible identification", precision reachable via tooltip), and a recall-honesty disclaimer ("no identification shown ≠ none exists") is available for every discovery surface.
  4. A bilingual methods/confidence page documents each band (population, unit of measurement, sample size, strata, weighted estimate, confidence interval, measurement date, grader + audit status, immutable report identifier) and every band tooltip links to it.
  5. A pre-registered stratified tier-A precision measurement with a written protocol (estimand + dedup unit, frozen eligible-frame hash, mutually-exclusive strata + weights, seed, blindness, gold treatment, exclusion/indeterminate rules, CI method, pass/fail gates, per-outcome release copy, and the FAIL action = reband `tier_a` to screening) has its ~200–250 cards drawn against the frozen frame and enters owner grading.

**Plans**: TBD

### Phase 136: Read Surfaces — Connections Panel & Work→Witnesses

**Goal**: A researcher browsing a manuscript sees its computed same-work identifications and related manuscripts — banded, masked, with our-text-only evidence — and can navigate to a per-work witness-map page listing every carrier grouped by codicological unit.
**Depends on**: Phase 135 (band-display contract + methods page + certificate framework). Translations, RTL, and accessibility are built into these surfaces from line one per house convention; comprehensive cross-surface i18n/RTL/a11y verification is gated in Phase 139.
**Requirements**: PANEL-01, PANEL-02, PANEL-03, WORK-01, WORK-02
**Success Criteria** (what must be TRUE):

  1. On a browse page, a "Computed identifications / זיהויים מחושבים" button (on the staleness-guarded enrichment path, within the PERF-01 browse-enrichment latency budget) opens the manuscript's banded identification list — high bands by default, screening only behind the BAND-03 toggle — with placeholder inline voting controls (wired to JUDGE-01 in Phase 137).
  2. The panel shows both relation types with band labels + click-through: "other manuscripts of ⟨work⟩" (MS-to-MS, derived from shared work–witness claims, displaying the WEAKER band of the pair) and "pages in other manuscripts related to this page" (direct page-to-page alignments in the context of the viewed page).
  3. An on-demand evidence view shows the supporting span(s) from OUR manuscript text with match stats (each side's own span for MS–MS claims); reference text is NEVER rendered, and evidence fails closed on HTR-version drift (offsets validated at render time).
  4. A `/work/{id}` page lists all identified carriers in the dated snapshot grouped by codicological witness unit (joined/part-grouped fragments appear as ONE witness, displaying the highest band among members) with per-witness band labels, band + library filters (AND-composed; empty = all enabled bands), server-side pagination (PERF-01 page cap), deterministic sort, and visible counts.
  5. Works are findable by neutral title (bilingual title normalization + alias/duplicate handling), linked from the panel, via the extended existing `/catalog` Browse-by-Identification structure (not a new browse paradigm).

**Plans**: TBD
**UI hint**: yes

### Phase 137: Community Judgments

**Goal**: Logged-in users can confirm / reject / annotate any stored work-witness or MS–MS relation claim, captured append-only in Supabase as a structurally separate display layer that never touches band assignment, precision, or ranking.
**Depends on**: Phase 136 (there is nothing to judge until claims render on the panel + work pages) + Supabase auth. REL-01 sequences this phase internally: Supabase migration + security smoke FIRST, then the judgment UI goes live.
**Requirements**: JUDGE-01, JUDGE-02, JUDGE-03, JUDGE-04, JUDGE-05
**Success Criteria** (what must be TRUE):

  1. A logged-in user casts ONE editable judgment per claim (✓ / ? / ✗ + optional note + optional typed refinement) on the panel and work pages; changing a vote inserts a superseding DB-enforced append-only event, and each judgment persists claim id, claim type, sidecar version, and the band shown at judgment time.
  2. The `work_witness_judgments` Supabase schema ships with RLS + explicit GRANTs (2026-05-30 rule) + role-matrix tests; note free-text is private by default (owner + moderators) via the simplest safe RLS posture.
  3. Community judgments never affect band assignment, precision copy, ranking, or certified styling — verified as a structurally separate layer.
  4. Aggregate judgment signals (including explicit disagreement counts) are visible on claims; free-text annotations are captured but NEVER publicly rendered in v9; admin hide/spam are append-only moderation events in a separate table and hidden/spam judgments are excluded from aggregates.
  5. Judgment writes are abuse-resistant — per-user rate limits, annotation length limits, input escaping/sanitization — and a Supabase security smoke passes before the voting UI goes live.

**Plans**: TBD
**UI hint**: yes

### Phase 138: Leads Queue

**Goal**: The high-recall R-B / R-CANON screening lane becomes a reviewable, explicitly-uncertified queue where users triage leads with the same voting used everywhere else.
**Depends on**: Phase 137 (lead review reuses the JUDGE-01 voting + typed refinement). No new table — the data already lives in the sidecar claims banded `screening_rb` / `screening_canon`.
**Requirements**: LEADS-01, LEADS-02
**Success Criteria** (what must be TRUE):

  1. A `/leads` page presents the R-B lane as the lead queue with the BAND-03 "possible identification" framing, pagination, deterministic sort, and band/library filters.
  2. The canon lane is separately caveated (including the known Targum-confusion class), not merely ranked lower.
  3. Reviewing a lead uses the same JUDGE-01 voting (main verdict ✓ / ? / ✗ + note) plus an OPTIONAL advanced typed refinement drawn from the fixed vocabulary (A cites B / B cites A / common source / compilation / another language or edition / …).

**Plans**: TBD
**UI hint**: yes

### Phase 139: Atlas Drill-down, Homepage & Release Hardening

**Goal**: The connection atlas becomes the flagship — the Phase 133 preview page upgraded/absorbed into a server-bounded drill-down explorer — promoted on a CLS-safe homepage, and the whole discovery module passes its cross-cutting release gates before the feature flag, SEO/sitemap, and homepage band turn ON.
**Depends on**: Phase 138 (REL-01 orders bounded atlas → public promotion last) and the Phase 135 CERT-01 certificate being graded to completion. This is the capstone; the drill-down explorer is the highest-risk surface and the first candidate to cut to fast-follow under schedule pressure (the Phase 133 static preview already exists as the fallback flagship).
**Requirements**: ATLAS-02, ATLAS-03, SEO-01, I18N-01, I18N-02, A11Y-01, A11Y-02, OBS-01, OBS-02, REL-01
**Success Criteria** (what must be TRUE):

  1. A server-bounded drill-down explorer — upgrading/absorbing the Phase 133 static preview page — serves capped neighborhoods (PERF-01 node/edge/byte caps, single-hop server-side expansion) without the client ever loading the full edge set; the ATLAS-01 static overview remains the offline-precomputed entry view.
  2. A CLS-safe static homepage band promotes the atlas + discovery (no live suggestions/graph in v9), enabled ONLY via the REL-01 gates.
  3. All discovery surfaces are fully translated EN/HE with passing HE-mode render-smoke tests and RTL/bidi correctness including graph/atlas labels; confidence is never signaled by color alone, the graph has a textual/table equivalent, contrast passes on all new text/controls/badges/marks, keyboard + screen-reader labels work, and animations honor reduced-motion.
  4. SEO outputs pass inside the DATA-05 masking gate — `/work/{id}` + atlas get canonical URLs, EN/HE hreflang, titles/descriptions, sitemap inclusion, neutral-only JSON-LD; `/leads` + uncertified-toggle states are `noindex` — and privacy-allowlisted PostHog product events (panel impressions, lead-toggle, evidence opens, work/atlas nav, judgment completion) + operational metrics (timeouts, truncation, sidecar unavailable/incompatible, atlas payload sizes, judgment rate-limit/moderation) are wired with the denylist enforced (never titles, text, shelfmarks, raw research IDs, or free text).
  5. The REL-01 release gate flips the feature flag, sitemap/SEO discovery, and homepage band ON only after CERT-01 is graded to completion, the BAND-05 immutable methods report is published, CERT-02 outcome-specific copy is applied (tier-A goes public WITH its measured number), and the masking (DATA-05), RTL (I18N-02), accessibility (A11Y-01/02), performance (PERF-01), and deployment (DATA-08) checks pass — the ATLAS-PREVIEW exception having applied only to the Phase 133 beta page and its claim-free homepage teaser.

**Plans**: TBD
**UI hint**: yes

## Progress

**Execution Order:** Phases execute in numeric order: 133 → 134 → 135 → 136 → 137 → 138 → 139. Phase 133 (Visual Atlas Preview) is the early quick win and deploys under the REL-01 ATLAS-PREVIEW exception; the Phase 135 CERT-01 grading runs as a parallel research track spanning 136–138 and must complete before the Phase 139 REL-01 gate.

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 133. Visual Atlas Preview (early quick win) | 5/6 | In Progress|  |
| 134. Discovery Data Spine | 0/TBD | Not started | - |
| 135. Precision Certificate & Confidence Bands | 0/TBD | Not started | - |
| 136. Read Surfaces — Connections Panel & Work→Witnesses | 0/TBD | Not started | - |
| 137. Community Judgments | 0/TBD | Not started | - |
| 138. Leads Queue | 0/TBD | Not started | - |
| 139. Atlas Drill-down, Homepage & Release Hardening | 0/TBD | Not started | - |

---

## Archived Milestones

<details>
<summary>✅ v8.4.1 Public API Dual-Mode (Phase 132) — SHIPPED 2026-07-01, web; closed 2026-07-01</summary>

See: .planning/milestones/v8.4.1-ROADMAP.md

1 phase (132), 3 plans. The public-API half of the dual-mode library filter (DMF-11) — the API counterpart to v8.4.0. `POST /api/search` + `/api/parallels` accept an optional `filters.library_filter_mode` (`include` / `exclude`) alongside `filters.library`. One shared `FiltersModel.library_filter_mode` field (`Optional[Literal['include','exclude']]`, default=None); `exclude` resolves to the complement (single-pass `resolve_library_complement_sys_ids`) via `run_in_executor`, intersected into `restrict_sys_ids`. Byte-for-byte backward-compatible (`default=None` + `model_dump(exclude_none=True)`; omitted = include — Codex R1 caught that `default='include'` would break every caller's echo); invalid mode → 400 via Pydantic `Literal` + `extra='forbid'`. Skill clients (`search.py`/`parallels.py`) gained `--library` / `--library-mode`. Web-only point-release on the 8.4.0 tree (no `version.py` bump / git tag); live-verified on the real 255K corpus. Phase dir archived to `.planning/milestones/v8.4.1-phases/`.

</details>

<details>
<summary>✅ v8.4.0 Dual-Mode Library Filter (Phases 130-131) — SHIPPED 2026-07-01, both apps; closed 2026-07-01</summary>

See: .planning/milestones/v8.4.0-ROADMAP.md

2 phases (130-131), 13 plans. Evolved the v8.3.0 inclusion-only library allowlist (SEED-026) into a **dual-mode** UI filter — **Show-only** (allowlist) *or* **Hide** (denylist) — persisted so each intent survives across searches, at full web + desktop parity. **Phase 130** (lead) settled the shared `{'mode','codes'}` state shape + `safe_storage` persistence + legacy-allowlist migration + edge-state sentinels + 3-state button on web `/search`. **Phase 131** mirrored it on the desktop catalog `LibraryFilterDialog`, web Browse-by-Identification, and a NEW web `/parallels` control (scoping via `restrict_sys_ids`), plus UAT-driven per-library counts + type-to-find + sort + searchable English codes. `'LOCAL'` guard (DMF-10) held on every surface; 131 SECURED 24/24. Web deployed; desktop installer `GenizahSearchPro_V8.4.0_Setup.exe` published to GitHub Release latest @ `v8.4.0`. DMF-13 (zero-count exclusion) Partial on non-`/search` surfaces (behaviorally safe, fail-open). The public-API piece (DMF-11) shipped as v8.4.1 (above). Phase dirs archived to `.planning/milestones/v8.4.0-phases/`.

</details>

<details>
<summary>✅ v8.3.0 God-File Decomposition + Search & Browse UX (Phases 122-129) — SHIPPED 2026-06-29, both apps; closed 2026-06-30</summary>

See: .planning/milestones/v8.3.0-ROADMAP.md

8 phases (122-129). Two strands shipped together as the public 8.2.2→8.3.0 release: (1) god-file decomposition (122-127) — split genizah_app.py + genizah_core.py into cohesive shared/+desktop/ modules behind permanent re-export facades, zero behavior change; (2) Search & Browse UX (128-129) — SEED-025 Space-key results scroll + SEED-026 library filter (web /search + Browse-by-Identification + desktop catalog + filters.library API), both apps. Also shipped: SEED-017 Joins-Lab viewer rotate/fullscreen, SEED-024 desktop Joins-Lab parity + XLSX export, SEED-015 desktop image NLI breaker.
</details>

<details>
<summary>✅ v8.2.0 Web Joins Lab, FGP Transcriptions & Hebrew Search (Phases 117-121) — SHIPPED 2026-06-23, both apps</summary>

See: .planning/milestones/v8.2.0-ROADMAP.md

5 phases (117-121). Ported the desktop Joins Lab (Component A) to the web at `/joins-lab` at full parity — anchor pane + line-by-line builders for both leaf sides + deduped candidate grid/table + side-by-side Compare + Visual Similarity toggle + Add-as-Join/Puzzle/list; bilingual EN/HE + RTL, no login, server-side per-session state via `safe_storage`. Bundled beyond scope: FGP transcriptions go-live (both apps), SEED-006 Hebrew/Judeo-Arabic search, Responsa-operators-over-My-Library (desktop). Phase dirs archived to `.planning/milestones/v8.2.0-phases/`.

</details>

<details>
<summary>✅ v8.1.0 Desktop Telemetry (Phases 111-116) — SHIPPED 2026-06-16, closed 2026-06-16</summary>

See: .planning/milestones/v8.1.0-ROADMAP.md

6 phases (111-116), 20 plans, 32 tasks. Opt-in, privacy-preserving desktop telemetry for "Dicta Genizah Search Pro" — anonymous usage analytics, crash reports, and per-session performance summaries flow to the shared web PostHog project (id 134161, EU), identity-aligned with the web app (logged-in users → same Supabase `user.id`), split by `platform=desktop`. Default OFF until the user consents via a bilingual first-run dialog; never transmits search content or My Library data. Also bundled: desktop "Public API & AI Tools" advertising and web Search API enhancements (quick task 260616-p9x) + the `platform=web` super-property.

</details>

<details>
<summary>✅ v8.0.0 Dicta Rebrand & Joins Lab (Phases 103, 105 + 106-110) — SHIPPED 2026-06-09, closed 2026-06-11</summary>

See: .planning/milestones/v8.0.0-ROADMAP.md

7 phases — 103 + 105 (folded from the v7.17 cycle) + 106-110 (Joins Lab Component A). 25 requirements satisfied (BRAND 2 + LEXP 7 + EXPUX 4 + JWB 9 + COMP-LOC 2 + EXP-F3 1). Desktop Joins Lab: shared core (`shared/joins_lab.py`) + anchor pane + line-by-line query builders for both sides of the leaf + deduped candidate grid/table + side-by-side Compare + pairwise→group join model + Visual Similarity toggle. Component B (JSA-01/02/03 + JWB-05) and web Joins Lab UI deferred.

</details>
