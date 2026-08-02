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
- [x] **Phase 134: Discovery Data Spine** - Masked, versioned `discovery.db` sidecar + async DiscoveryService + frozen-frame & budget artifacts; proves masking, event-loop safety, and fail-open. **CLOSED 2026-07-23** on spine success criteria SC1–SC3 (all met by the v1 build); the owner-review data-quality re-distill (discovery-v2) is re-bracketed as Phase 135's leadoff task, gated on the twin census.
- [x] **Phase 135: Precision Certificate & Confidence Bands** — CLOSED 2026-07-28 (9/9). ✅ **CERT-01 MEASURED = PASS**: owner graded all 280 cards catalogue-blind, validator 12/12; pre-registered weighted precision **0.9382, 95% CI [0.9084, 0.9644]** vs the 0.85 Strict floor (`135-09-CERT01-MEASUREMENT.md`). Public-scope (Sefaria-only) subgroup 0.9580 CI [0.9240, 0.9847] — descriptive, not pre-registered. ⚠ Still open for Phase 139: `band_precision` has NOT been re-baked (`tier_a` carries no number yet — needs `--precision-spec` + deploy), the CERT-02 outcome copy is unapplied, and the per-stratum spread (1.000 ja → 0.471 msource:medium) must reach the BAND-05 methods page. CERT-01 stays `Pending` until those land. Liturgical-containment FP class (one work = 45% of error; D-17 structurally can't catch it) → v2.1 candidate. Data-driven four-band display contract + bilingual methods page + pre-registered tier-A precision measurement (grades in parallel). **Leadoff task = the discovery-v2 data re-distill** (canonical merge + w001239 drop + `work_relations` + Lever-1 coverage routing + (B) band-enum rename; plan `docs/specs/discovery-v2-bake-plan.md`; gated on the SEED-029 census) — the band/precision/display contract binds against v2, not v1.
- [ ] **Phase 136: Read Surfaces — Connections Panel & Work→Witnesses** - Browse "computed identifications" panel (banded, masked evidence) + `/work/{id}` witness-map page grouped by codicological unit + computed identifications on `/catalog-browse` + a corpus-wide findings page. **SCOPE EXPANDED 2026-07-30** (owner): the novelty axis (NOVEL-01/02) moves IN from post-136, VIS-01 is homed here, and ONE authorized rebuild + flag-OFF redeploy is the first gate.
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

  1. The offline distillation produces `discovery.db` (≤ 300 MB, schema-versioned filename, release contract + `PRAGMA integrity_check`) carrying both claim families (work–witness with page→witness aggregation; MS–MS relation claims with child page-alignment records) each with a deterministic namespaced `claim_id`, exactly one band per claim key, codicological witness-unit memberships, and human-reviewed neutral work titles — with ZERO reference text, sigla, or provenance columns (evidence stored only as offsets into our HTR text with the snapshot hash recorded). *(**SUPERSEDED 2026-07-21 — Phase-134 CONTRACT CORRECTION, `134-CONTEXT.md` C-1..C-9; pending owner ratification:** "both claim families" → a **two-table** model — `discovery_claim` PK `(page_id, work_id)` + `discovery_evidence` with an `evidence_kind ∈ {witness, shared_text}` discriminator and an orthogonal `evidence_source ∈ {track1_direct, propagated}` axis; `claim_type ∈ {direct_witness, quotes_this_work, shared_text}`; `claim_id = SHA-256(namespace, page_id, work_id)` — NOT claim_type; "exactly one band per claim key" DROPPED — a claim carries MULTIPLE evidence rows/bands with ONE deterministic `display_evidence_id`; physical-MS grouping is a DATA-10 unit×work projection via `witness_units`, never a claim collapse.)*
  2. A permanent CI leak-vector guard scans the shipped sidecar (schema + every cell), every product surface (including the Phase 133 atlas assets), and committed repo content, and fails the build if the restricted source appears as anything other than the codename "M-source"; the one-time cleanup verification passes on first run.
  3. All web access flows through one async DiscoveryService with per-query timeouts, bounded concurrency, indexed bounded queries, LRU browse-enrichment caching, and server-side pagination; under overload the caller gets a "temporarily unavailable" response (never a hang) and heavy queries never block the event loop.
  4. With the feature flag OFF, or the sidecar absent / corrupt / incompatible, every discovery surface hides cleanly with zero errors and the rest of the app stays fully available; deploy is temp-upload → verify → atomic rename → code, with a documented rollback + reproducible rebuild recipe.
  5. The version-controlled frozen-frame artifact (`discovery-frames.md`: per-band dedup counts, page→claim dedup formula, overlap-resolution counts, frame content hash) and the acceptance-budget artifact (`discovery-budgets.md`: browse-enrichment p95 ≤ 150 ms, atlas/work/leads caps + timeouts, ≤ 250 MB added RSS) are committed as phase exit criteria — frame frozen so CERT-01 can freeze against it.

**Plans**: 8 plans

Plans:
**Wave 1**

- [x] 134-01-PLAN.md — Freeze the CORRECTED two-table schema (discovery_claim + discovery_evidence, evidence_source axis, per-source bands) + deterministic claim_id/evidence_id/unit_id id module (wave 1)
- [x] 134-02-PLAN.md — Masking guard --scan-sqlite + R-source tokens + gitignore + PERF-01 budgets doc (wave 1)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 134-03-PLAN.md — Deterministic masking-safe fixture DB (two-table model, both evidence_sources) + standalone all-invariant verifier + build-output tests (wave 2)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 134-04-PLAN.md — Offline distillation: unified witness family (track1_direct + PROPAGATED Q2) + shared_text family, per-source bands, witness units, offsets-only evidence, review artifact (wave 3)
- [x] 134-05-PLAN.md — Fail-closed versioned loader + DISCOVERY_ENABLED flag + startup wiring (wave 3)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 134-06-PLAN.md — Async DiscoveryService chokepoint (off-loop, timeouts, bounded concurrency, LRU, pagination) (wave 4)
- [x] 134-07-PLAN.md — Owner title-review -> re-distill real discovery.db -> freeze discovery-frames.md (corrected per-band+evidence_source counts + C-7 precision reporting) (wave 4, human gate) — DONE 2026-07-22: 1,270 works (508 sefaria+106 ja+656 owner-titled M-source), 268,490 claims / 297,559 evidence / 5,547 units; frame_content_hash 17bf5601…; verify + strict masking gate clean; discovery-frames.md FROZEN; DB 368.5MB (owner-accepted)

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 134-08-PLAN.md — PERF-01 measurement + budgets finalization + deploy/rollback/rebuild recipe (wave 5, human gate) — Tasks 1-2 DONE 2026-07-23: scripts/bench_discovery.py (flag-bypass predicate, nonzero-result assertion, over the real 368.5MB sidecar via DiscoveryService) + discovery-budgets.md §4 dev-box actuals (browse-enrichment p95 ~0.6ms vs 150ms; get_work_witnesses p95 ~116ms; added RSS ~11MB vs 250MB) + docs/specs/discovery-deploy.md (asset-first deploy/rollback/rebuild); Task1+2 auto-verify + ruff + strict masking scan all clean. **Task 3 (prod deploy) DEFERRED → Phase 135 prerequisite** — deploy the FINAL discovery-v2 asset once; the flag-OFF v1 sidecar has no user surface, so deploying it buys nothing.

> **✅ PHASE 134 CLOSED (2026-07-23) on spine SC1–SC3; DATA-QUALITY REMEDIATION RE-BRACKETED → PHASE 135.** Owner review of the discovery-v1 build found 3 disqualifying defects: cross-corpus duplicate works (`canonical_work_id` unpopulated), band-label overclaim (`expert_verified`=121 confirmed+1,067 unreviewed; `tier_a`=238,618 all unreviewed), and anthology/quotation false positives (T-S C 2.191). Frame → **SUPERSEDED-PENDING**, re-distill to **discovery-v2**. Fix ownership: **(B) band-label honesty contract LANDED** (`docs/specs/discovery-band-labels-v1.md`, this track); **(A) relation-aware canonical merge + (C) direction-aware shadow router = SEED-029/R-source track** (its current shadow is density-only, mis-routes ~26% of anthology co-claims; blocked on Track-1 ref-subspan re-instrumentation). The v2 re-distill is Phase 135's leadoff task (plan `docs/specs/discovery-v2-bake-plan.md`; gated on the SEED-029 census); 134-08 Task 3 prod-deploy DEFERRED → Phase 135 prereq (deploy the final v2 asset once). The v1 frame is the reference build. See STATE.md for the Codex-blessed v2 order-of-ops + the resolved merge/drop/relation decision table.

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

**Plans**: 9 plans (6 waves)

Plans:
**Wave 1** *(Track A parallel + Track B leadoff, no deps)*

- [x] 135-01-PLAN.md — Band-label values module + band_precision reader + drift-guard test (BAND-01/02/03/04, CERT-02)
- [x] 135-03-PLAN.md — Written pre-registered CERT-01 tier-A protocol doc (CERT-01)
- [x] 135-04-PLAN.md — REWRITE discovery-v2-bake-plan.md + BLOCKING Codex re-review gate (autonomous: false)

**Wave 2**

- [x] 135-02-PLAN.md — Bilingual methods/confidence Help section + /help noindex + render-smoke (BAND-05)
- [x] 135-05-PLAN.md — v2 vocabulary lockstep: routing_reason += later_shared_text + expert_verified→high_confidence_algorithmic rename + spec amendments

**Wave 3**

- [x] 135-06-PLAN.md — v2 build logic: canonical merge + drop + Lever-1 routing + D-17 chronological demotion (DELTA=100y) + verifier invariants + fixture tests

**Wave 4**

- [x] 135-07-PLAN.md — Run v2 bake + verifier + strict masking gate + freeze discovery-frames-v2.md

**Wave 5**

- [x] 135-08-PLAN.md — v2 production deploy (asset-first, human-approved checkpoint, ONCE) (autonomous: false)

**Wave 6**

- [x] 135-09-PLAN.md — CERT-01 frame freeze + pre-outcome OC table + ~200–250 card draw → grading STARTED (phase close)

### Phase 136: Read Surfaces — Connections Panel & Work→Witnesses

**Goal**: A researcher browsing a manuscript sees its computed same-work identifications and related manuscripts — banded, masked, with our-text-only evidence, and honestly framed as text matches rather than asserted identifications — can navigate to a per-work witness-map page grouped by codicological unit, and can sweep the whole corpus for findings that no finding aid already records. Everything ships behind the flag; nothing goes public until Phase 139.

**⚠ SCOPE EXPANDED 2026-07-30** (owner, `136-CONTEXT.md`; discuss-phase ran both the real-data mockup gate and the Codex adversarial gate before planning). Four decisions reshape this entry:

- **The novelty axis (NOVEL-01/02) moved IN**, reversing the 2026-07-28 addendum's post-136 homing — the owner's primary reason the surfaces are worth shipping is finding what is *not* already in the finding aids, and the surfaces cannot be designed around a filter that does not exist. This carries the `track1_direct` coverage gap (the frozen v2 asset leaves all 254,612 direct rows at `is_new = 0`), the rewired LLM title gate, and the heuristic funnel.
- **VIS-01** (public/private projection) is homed here and must be derived at BUILD time, before raw provenance ids are discarded.
- **Two NEW surfaces** beyond the panel and the work page: computed identifications on `/catalog-browse`, and a corpus-wide **findings page** with its own nav entry (owner: *"a big new amazing feature… maximum ability to see new findings"*).
- **ONE rebuild + ONE production redeploy** (flag-OFF) is the first execution gate — the surfaces need stored fields the frozen v2 asset does not carry, and the owner authorized exactly one, not a series.

**Depends on**: Phase 135 (band-display contract + methods page + certificate framework; CERT-01 measured PASS — weighted 0.9382 CI [0.9084, 0.9644]). Builds on the LIVE v2 asset, NOT on the gen-2 evidence refresh — that becomes discovery-v2.1 in its own later phase (D-01). Translations, RTL, and accessibility are built into these surfaces from line one per house convention; comprehensive cross-surface i18n/RTL/a11y verification is gated in Phase 139.
**Requirements**: PANEL-01, PANEL-02, PANEL-03, WORK-01, WORK-02, VIS-01, NOVEL-01, NOVEL-02 — and this phase AMENDS BAND-03 + BAND-05 (no precision percentages anywhere; qualitative methods page) plus `discovery-band-labels-v1.md` §2/§3 and `discovery-budgets.md`.
**Success Criteria** (what must be TRUE):

  1. **The one authorized rebuild lands in production, flag-OFF, before any surface work** — one new asset carrying: the D-02a *authorization* for tier-A default visibility (`measurement_status='measured_pass'` + `ci_low`, precision still NULL — no number is stored or shown), direct-family `coverage_ppm` + validity status, the tri-state novelty flag for ALL evidence families with masked provenance, the VIS-01 public/private projection fields, and the materialized sort keys + indexes the new surfaces need. **ADDED 2026-08-01/02** (see `docs/specs/discovery-v2-bake-plan.md` Amendment 2026-08-01): **work-side match offsets `w_start`/`w_end` for every corpus** plus Sefaria reference resolution from the 322 existing `*.versemap.json` sidecars (stage 1; JA divisions deferred, M-source stored-not-displayed) — the matcher already computes the position and discards it at ingest; **the materialized main-pool bucket flag + its reason code**, since recomputing coverage/competition/aggregation at query time is not viable inside PERF-01; **`works.genre`** (entirely NULL today) for the findings-page domain facet; and a **`discovery_routing_audit` fix** so `kept_tie` rows carry `demoted_work_id`. Note `coverage_ppm` already subsumes the page-letter denominator the main-pool coverage gate needs — no separate table. The verifier, the DATA-05 masking scan, the golden fixture, and a rebuild-preservation gate (nothing silently lost versus the frozen v2 row counts) all pass; the CERT-01 pre-registration artifact is untouched.
  2. On a browse page, a "Computed identifications / זיהויים מחושבים" button (on the staleness-guarded enrichment path, within the PERF-01 browse-enrichment latency budget) opens the manuscript's list in honest disclosure levels — with "on this page" and "elsewhere in this manuscript" that NAMES the works, per-row matched-letter coverage labelled as such, match framing rather than asserted identity, no precision percentage and no review badge, and placeholder inline voting controls (wired to JUDGE-01 in Phase 137). **AMENDED 2026-08-01/02 (owner):** the disclosure model is **two buckets — "main pool" / "more matches"** drawn by the rule in `.claude/skills/sketch-findings-genizahsearch/references/main-pool-rule.md` (multi-folio agreement **or** near-full page coverage, as non-compensating floors; 56% / 44%), **not** a three-level confidence scale — one was designed and then retired on 2026-08-01 as a duplicate of a rule the codebase already had, which had mislabelled the best-measured population in the system. Band labels become **tooltip-only**; the visible row chip states the relation. **Whether the panel's third disclosure level survives is an OPEN D-13e decision** — its middle "also shares text with" bucket is behind-the-default on quality *and* distinguished only by relation, which the relation chip now carries. Variant D (even panes, ≥900px 1fr/1fr, stacking page-then-manuscript on mobile) is the selected layout — **D-09 owes a narrow amendment striking "collapsed"** while keeping its ordering.
  3. An on-demand evidence view shows the supporting span(s) from OUR manuscript text with match stats, highlighting only where offsets exist, and evidence fails closed on HTR-version drift (offsets validated at render time). **AMENDED 2026-08-02 (owner): reference text MAY be rendered — but per-work and licence-gated, never per-corpus.** The gate is the acquisition manifest's per-work `reuse_ok`, which already exists: `yes` (277 works — Public Domain 264, CC-BY 11, CC0 1, CC-BY-SA 1) may render, carrying the manifest's generated `attribution_text` where the licence requires it; `unclear` (46 — 42 `unknown`, 4 CC-BY-NC), `noncommercial_only` (1) and **absent** (21 — every JA work, which has no licence metadata at all) all fail closed. M-source never renders. "Public corpus" is NOT the operative test: ~17% of Sefaria works are not clearly reusable, and JA — public, and displayed elsewhere in the UI — currently has zero licence evidence, so its *text* cannot render until that is answered (a separate question from its missing divisions). Also decide the render source: the staged bodies are lossy (nikud, punctuation and rubrics stripped at acquisition), so either accept stripped text as the comparison view or re-fetch display text at render time.
  4. A `/work/{id}` page lists all identified carriers in the dated snapshot grouped by codicological witness unit (joined/part-grouped fragments appear as ONE witness, displaying the highest band among members) with per-witness band labels, filters AND-composed (empty = all), server-side pagination with the real total visible (PERF-01 page cap), deterministic tier-first sort, and counts that count units rather than claim rows. **AMENDED 2026-08-01: the "tier filter" is DELETED, not renamed** — quality is now the bucket (a default plus a "show more" toggle) and kind is the relation filter, so a tier-labelled control would be a second vocabulary for an axis the rows no longer speak. Tier A stays reachable because §4 already holds it behind the screening toggle. Coverage and novelty filters remain.
  5. Works are findable by neutral title (bilingual normalization + alias/duplicate handling) and reachable from the panel, from an extended `/catalog-browse` Browse-by-Identification that now carries computed identifications alongside catalogued ones, and from a **new corpus-wide findings page with its own nav entry** — server-paginated inside the PERF-01 budget with the indexes that requires. **AMENDED 2026-08-01/02:** nav label **"Computed Identifications / זיהויים מחושבים"** ("Discoveries" is taken by the Community page, a bare "Identifications" collides with Browse-by-Identification, and D-23b bars "new"). **All three row units ship, user-selectable** via a "Show as" control, defaulting to one row per identification (65,200; per-page counting inflates same-work matches ~2.3×). Filters: the two buckets, novelty, coverage, plus a **domain/author/work cascade on the IDENTIFIED WORK's domain — never the manuscript's catalogue domain** (Moss. V,374 is catalogued *Court Records* while carrying a correct Rashi finding; 338 tier-A findings sit on documentary-catalogued manuscripts). The tier filter is deleted here too. A **mode strip** ships now with Phase 138's leads and Phase 137's saved judgments greyed and phase-tagged, so both add a tab rather than a page. **OPEN (D-16/PANEL-01):** whether this page also gains the panel's relation filter.
  6. **The novelty axis is live and structurally orthogonal to the tier**: tri-state and fail-closed (`not_in_finding_aids` / `already_recorded` / `not_checked` — never novel by default), computed per `(sys_id, work)` against an enumerable versioned source set recorded in `meta`, keyed on a reviewed alias-aware identity, worded "Not found in the finding aids checked" with the checked-source list and as-of date, provenance masked to a non-identifying label, and never feeding band assignment, ranking, precision copy or styling. The LLM gate has a reproducible contract (pinned prompt hash, model + version, measured cost).
  7. **No precision percentage is reachable from any surface** — the methods page is rewritten qualitatively (tiers explained in words plus the non-percentage facts: that grading happened, population, unit, sample size, grader, date, method, audit state, immutable report id), `web/pages/help.py`'s existing estimates and intervals are removed, and no surface claims human review until the provenance of the 121 `human_confirmed` rows is established.
  8. Every surface hides cleanly with the flag off or the sidecar absent, stays inside the PERF-01 budgets (a versioned `discovery-budgets.md` entry for the findings page), and passes the masking scan on rendered output, JSON payloads, copy/export paths and error paths.

**Plans:** 31 plans in 26 waves, organised around the **six execution gates** (D-04): (1) rebuild + the D-13d granularity rule + the D-13c threshold [136-01..136-15], (2) the panel [136-16, 136-18..136-21], (3) the evidence view [136-17, 136-22, 136-23], (4) `/work/{id}` [136-24, 136-25], (5) `/catalog-browse` + the findings page [136-26..136-29], (6) novelty wiring + the methods-page rewrite [136-30, 136-31]. Gate 1 blocks every surface wave: the rebuilt asset must be live in production, flag OFF, before any UI code that reads its new columns is deployed. Four blocking owner checkpoints: 136-04 (the five open gate-1 decisions), 136-11 (authorize the novelty funnel run), 136-15 (approve the one authorized production redeploy), 136-22 (the evidence view's render source and b-side form). A second mockup pass and a Codex pass are owed on the built surfaces, not only on the context.

Plans:
**Wave 1**

- [ ] 136-01-PLAN.md — Requirement + band-label + budget contract amendments
- [ ] 136-02-PLAN.md — Schema + deploy contract amendments
- [ ] 136-03-PLAN.md — Rebuild-preservation harness + expectations pinned from the LIVE asset
- [ ] 136-04-PLAN.md — Gate-1 evidence pack + the five open owner decisions (checkpoint)

**Wave 2** *(blocked on Wave 1 completion)*

- [ ] 136-05-PLAN.md — D-02a tier_a authorization lockstep (6 sites, both branches)

**Wave 3** *(blocked on Wave 2 completion)*

- [ ] 136-06-PLAN.md — coverage_ppm + band_rank + the D-10a index set

**Wave 4** *(blocked on Wave 3 completion)*

- [ ] 136-07-PLAN.md — The main-pool rule + discovery_identification + manuscript_display + findings bench

**Wave 5** *(blocked on Wave 4 completion)*

- [ ] 136-08-PLAN.md — Work-side offsets w_start/w_end, corpus-wide, + the containment signal

**Wave 6** *(blocked on Wave 5 completion)*

- [ ] 136-09-PLAN.md — Sefaria reference resolution + the per-work licence metadata

**Wave 7** *(blocked on Wave 6 completion)*

- [ ] 136-10-PLAN.md — works.genre curation + author aliases + the kept_tie fix

**Wave 8** *(blocked on Wave 7 completion)*

- [ ] 136-11-PLAN.md — Novelty identity key + pinned LLM contract + verdict artifact (checkpoint)

**Wave 9** *(blocked on Wave 8 completion)*

- [ ] 136-12-PLAN.md — Novelty tri-state ingestion + fail-closed verification

**Wave 10** *(blocked on Wave 9 completion)*

- [ ] 136-13-PLAN.md — VIS-01 two-axis visibility + closed-graph public projection + VIS-02 control

**Wave 11** *(blocked on Wave 10 completion)*

- [ ] 136-14-PLAN.md — The rebuild run + the full gate battery + the compatibility attestation

**Wave 12** *(blocked on Wave 11 completion)*

- [ ] 136-15-PLAN.md — Owner approval + the asset-first production deploy, flag OFF (checkpoint)

**Wave 13** *(blocked on Wave 12 completion)*

- [ ] 136-16-PLAN.md — Panel service layer: envelope, the D-13g routing fix, manuscript scope
- [ ] 136-17-PLAN.md — Offset renderer + the reference-text licence gate (pure)

**Wave 14** *(blocked on Wave 13 completion)*

- [ ] 136-18-PLAN.md — Panel display model + the bilingual display vocabulary (pure)

**Wave 15** *(blocked on Wave 14 completion)*

- [ ] 136-19-PLAN.md — Panel UI: entry control, enrichment seam, service states

**Wave 16** *(blocked on Wave 15 completion)*

- [ ] 136-20-PLAN.md — Panel UI: rows, buckets, panes, placeholder voting

**Wave 17** *(blocked on Wave 16 completion)*

- [ ] 136-21-PLAN.md — Panel render-smoke + positive controls + masking capture
- [ ] 136-22-PLAN.md — Evidence-view decisions: render source and b-side form (checkpoint)

**Wave 18** *(blocked on Wave 17 completion)*

- [ ] 136-23-PLAN.md — Evidence view UI + licence-gated reference text

**Wave 19** *(blocked on Wave 18 completion)*

- [ ] 136-24-PLAN.md — Work-page service: D-17a display fields + the count query

**Wave 20** *(blocked on Wave 19 completion)*

- [ ] 136-25-PLAN.md — `/work/{id}` page

**Wave 21** *(blocked on Wave 20 completion)*

- [ ] 136-26-PLAN.md — `/catalog-browse` computed identifications + WORK-02 title findability

**Wave 22** *(blocked on Wave 21 completion)*

- [ ] 136-27-PLAN.md — Findings service: three row units, the facet cascade, the perf gate

**Wave 23** *(blocked on Wave 22 completion)*

- [ ] 136-28-PLAN.md — Findings page: route, gated nav entry, shell

**Wave 24** *(blocked on Wave 23 completion)*

- [ ] 136-29-PLAN.md — Findings page: rows, the novelty switch, render-smoke

**Wave 25** *(blocked on Wave 24 completion)*

- [ ] 136-30-PLAN.md — Methods-page qualitative rewrite + the no-percentage gate

**Wave 26** *(blocked on Wave 25 completion)*

- [ ] 136-31-PLAN.md — Novelty across all surfaces + the final cross-surface masking sweep

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
**Depends on**: Phase 137 (lead review reuses the JUDGE-01 voting + typed refinement). No new table — the data already lives in the sidecar claims banded `screening_rb` / `screening_canon`. **Relationship to Phase 136's findings page (owner, 2026-07-30):** they are the same machinery pointed at different lanes and `/leads` should be built as a MODE of it, not a second implementation — 136 ships the corpus-wide sweep over the *default-shown* lanes (tier + novelty + coverage filters, server-paginated), 138 adds the screening lanes plus triage. If 136's page cannot host that as a filter state, say so and justify a separate page at planning; the shared query, sort keys and indexes are Phase 136's deliverable either way.
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
| 134. Discovery Data Spine | 6/8 | In Progress|  |
| 135. Precision Certificate & Confidence Bands | 9/9 | Complete   | 2026-07-28 |
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
