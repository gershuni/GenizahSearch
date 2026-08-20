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

**SCOPE ADDED 2026-08-20 (second re-map).** The milestone now carries three lanes, not one. The
discovery surface lane (136.1 → 139b) is unchanged. Two more were entered because work was happening
in them with no phase to belong to: a **discovery data lane** (148, 149) for the V4.1/V4.2
reference-expansion track and the serving-performance work that landed after the 2026-08-16
ratification, and a **passage-matching lane** (141-147) — a second selectable parallels method,
owner-planned outside GSD and already substantially built on an unmerged branch, placed inside
v9.0.0 by owner decision as the **desktop release's headline**, v9.0.0 having shipped web-only on
2026-08-16. If the desktop release must go first, 141-147 re-cut cleanly into v9.1.0.

## Phases

**Phase Numbering:** integer phases are planned milestone work; decimal phases (e.g., 133.1) are urgent insertions marked INSERTED, appearing between their surrounding integers.

- [x] **Phase 133: Visual Atlas Preview (early quick win)** - Offline layout bake → static, canon-masked corpus-overview asset on a standalone `/atlas` beta page, deployed early under the REL-01 atlas-preview exception. **CLOSED 2026-07-21** — live in production (`ATLAS_PREVIEW_ENABLED=1`), re-confirmed 2026-07-29.
- [x] **Phase 134: Discovery Data Spine** - Masked, versioned `discovery.db` sidecar + async DiscoveryService + frozen-frame & budget artifacts; proves masking, event-loop safety, and fail-open. **CLOSED 2026-07-23** on spine success criteria SC1–SC3 (all met by the v1 build); the owner-review data-quality re-distill (discovery-v2) is re-bracketed as Phase 135's leadoff task, gated on the twin census.
- [x] **Phase 135: Precision Certificate & Confidence Bands** — CLOSED 2026-07-28 (9/9). ✅ **CERT-01 MEASURED = PASS**: owner graded all 280 cards catalogue-blind, validator 12/12; pre-registered weighted precision **0.9382, 95% CI [0.9084, 0.9644]** vs the 0.85 Strict floor (`135-09-CERT01-MEASUREMENT.md`). Public-scope (Sefaria-only) subgroup 0.9580 CI [0.9240, 0.9847] — descriptive, not pre-registered. ⚠ Still open, now homed in **Phase 139a**: `band_precision` has NOT been re-baked (`tier_a` carries no number yet — needs `--precision-spec` + deploy), the CERT-02 outcome copy is unapplied, and the per-stratum spread (1.000 ja → 0.471 msource:medium) must reach the BAND-05 methods page. CERT-01 stays `Pending` until those land. Liturgical-containment FP class (one work = 45% of error; D-17 structurally can't catch it) → **discovery-v3** candidate (renamed from "v2.1" 2026-08-05, `docs/specs/discovery-v3-naming.md`). Data-driven four-band display contract + bilingual methods page + pre-registered tier-A precision measurement (grades in parallel). **Leadoff task = the discovery-v2 data re-distill** (canonical merge + w001239 drop + `work_relations` + Lever-1 coverage routing + (B) band-enum rename; plan `docs/specs/discovery-v2-bake-plan.md`; gated on the SEED-029 census) — the band/precision/display contract binds against v2, not v1.
- [x] **Phase 136: Read Surfaces — Connections Panel & Work→Witnesses** - Browse "computed identifications" panel (banded, masked evidence) + `/work/{id}` witness-map page grouped by codicological unit + computed identifications on `/catalog-browse` + a corpus-wide findings page. **SCOPE EXPANDED 2026-07-30** (owner): the novelty axis (NOVEL-01/02) moves IN from post-136, VIS-01 is homed here, and ONE authorized rebuild + flag-OFF redeploy is the first gate. **CLOSED 2026-08-08** — 22/22 on the code. The checkbox was left unticked when the Progress table was reconciled on 2026-08-16, so this file said Complete and In Progress about the same phase; `init.manager` read the checkbox and reported `roadmap_complete: false`. Ticked 2026-08-20.
- [ ] **Phase 137: Community Judgments — Hardening** - **RETITLED 2026-08-16 (owner: "ratify reality").** A beta reviews feature shipped 2026-08-13 outside any plan and is live: it meets JUDGE-03 and JUDGE-05, partially meets JUDGE-02. This phase's job is no longer to build from zero but to close the two it **structurally fails** — JUDGE-01 (votes overwrite instead of appending a superseding event; no band captured at judgment time) and JUDGE-04 (one row per review instead of an aggregate; moderation mutates status columns instead of writing separate append-only events) — plus a live role-matrix test for JUDGE-02. Carries a live-data migration question: vote history for judgments already cast under overwrite semantics is not recoverable.
- [ ] **Phase 138: Leads Queue** - `/leads` high-recall R-B screening lane, explicitly uncertified, canon-lane caveated, same voting. **Unchanged — the one later phase where nothing was pulled forward** (`web/pages/findings.py` still renders the mode strip's leads tab as "Coming soon"; no `/leads` route exists).
- [ ] **Phase 139a: Release Hardening & REL-01 Gate Closure** - **SPLIT OUT 2026-08-16.** Close the five REL-01 gate items still open at the 2026-08-08 flag flip, re-run the cross-surface masking sweep over the three surfaces that shipped after the last attestation (2026-08-05), and apply or explicitly defer the CERT-02 tier-A copy. Cheap relative to 139b and unblocks the "beta → released" transition.
- [ ] **Phase 139b: Atlas Drill-down & Homepage Capstone** - Server-bounded drill-down explorer (absorbing/upgrading the preview page) + SEO/i18n/RTL/a11y/observability. **SC2 (the CLS-safe homepage band) shipped early on 2026-08-12/13** (`c413d5e9`, `fcb1eb8e`) and is retro-credited: it was correctly gated in code on `discovery_available()`/`atlas_preview_available()` rather than on a bare flag. The still-unbuilt explorer is the highest-risk surface in the milestone and remains the first candidate to cut to fast-follow.
- [ ] **Phase 140: Milestone Bookkeeping & Debt Closure** - **NEW 2026-08-16.** Retro-plan the two production-affecting features that have no plan trace (the relation matrix, the excerpt bake), reconcile the `STATE.md`↔ROADMAP novelty-run contradiction, and decide the phase home for the reference-expansion track (discovery-v3 / V4 / V4.1) and the locus track. Mostly documentation plus one owner-decision checkpoint; sequenced FIRST because every other phase plans against numbers this phase makes true.

### Passage-matching parallels search (phases 141-147, desktop lane)

**ADDED 2026-08-20.** A second selectable parallels method: the discovery pipeline's
character-level seed-and-extend matcher brought to composition search, proven or disproven against
the shipped token-window method before any default changes. Owner-planned outside GSD and already
substantially built on branch `claude/computed-id-parallels-search-a7c8fd` (20 commits, unmerged).
Placed **inside v9.0.0** by owner decision as the desktop release's headline, v9.0.0 having shipped
web-only. Full plan: `docs/plans/passage-matching-parallels-search.md`.

- [x] **Phase 141: Passage Matching — Algorithm Specification** - One tracked, self-contained spec for the character-level matcher: normalization, gram coding, index orientation, diagonal-keyed candidate generation, the two verification boundaries, one minimum-span contract, mandatory Stage-0 hygiene, the DF/budget policy, a parameter table with provenance, and an explicit "not established" section. **BUILT on branch** — `docs/specs/passage-matching-algorithm.md` v1, 13 sections (`ce7c8beb`); the 25/30/40 minimum-span conflict is settled and classified as query policy, not an artifact input.
- [x] **Phase 142: Passage Matching — Spike, Closed-Subset Comparator & GO/NO-GO** - Build over a corpus slice, measure the artifact, benchmark the two constructions, choose DF cap / stride / budget policy, and compare both methods validly at small scale before any multi-GB artifact exists. **BUILT on branch** — `docs/specs/passage-index-build-measurements.md`, 7 sections. Findings that overturned the plan: `spool` beat mass-partitioned `scatter` and needs no scratch; batching by records cost 3 GB of RAM; the query caps were "pure waste"; the author's own rarity proposal was refuted by his own measurement (`f2e19e1a`).
- [x] **Phase 143: Passage Matching — Shared Engine, Builder & Full Index** - The ported normalizer, the corpus-resident CSR index, the query path, the release verifier, and the full-corpus build. **BUILT on branch** — full-corpus index built, a cap that truncated by catalog position removed, 5 s queries down to 0.6 s (`b9d5c594`); normalizer ported byte-exact against 737k letters of real corpus (`bf17eb67`); the release verifier's own sampled order check was caught letting a corrupted artifact pass (`318a0336`).
- [ ] **Phase 144: Passage Matching — Full-Scale Evaluation & Default Decision** - Four instruments, both methods, page-scoped comparator, frozen eligible-page manifest, per-method rank strata, and the pre-declared non-inferiority endpoints. **IN PROGRESS on branch, tuning split only — the holdout has never been touched.** Interim FGP self-retrieval at n=120: passage recall@50 **0.750** [0.666, 0.819] against the incumbent's **0.708** [0.622, 0.782] at `chunk_size=3` — **intervals overlap heavily, so there is no demonstrated recall difference**; sweeping the incumbent's chunk size overturned the author's own conclusion (`7cc50fe7`), and a corpus inequality worth 26% was found and fixed (`926bb1c9`). Passage separates on RANKING and speed, not retrieval: recall@1 0.592 vs 0.467, MRR 0.639 vs 0.533, 391 ms vs 14,565 ms (37x).
- [ ] **Phase 145: Passage Matching — Web Surface** - `method: 'chunk' | 'passage'` on the parallels API with scope rejection, a fail-closed asset loader mirroring `web/discovery_assets.py`, its OWN `ThreadPoolExecutor` sized to its semaphore, a safe highlight contract with an RTL+LTR DOM matrix, an i18n inventory, and declared cold-cache SLOs before deploy.
- [ ] **Phase 146: Passage Matching — Desktop Surface & Release** - Method selector with the enforced `local`/`all` scope restriction, a background build worker (never `__init__`, never the UI thread) with progress/checkpoint/free-space preflight, stale-index detection from the input manifest, then flip-or-don't-flip per Phase 144 and the v9.0.0 desktop release.
- [ ] **Phase 147: Passage Mode on Regular Search (optional)** - A fourth `search_mode`, gated on its OWN comparison against `execute_search(..., mode="fuzzy")`. Instruments 1-4 compare composition methods and cannot license a regular-search change.

### Discovery data lane (phases 148-149, parallel)

**ADDED 2026-08-20.** Homes the reference-expansion and operability work that landed after the
2026-08-16 ratification with no phase to belong to. Numbered after 147 only because 141-147 were
already claimed by in-flight work — these are **not** sequenced behind the passage track and 148
carries a live operational risk.

- [ ] **Phase 148: Reference Expansion & Bake Reproducibility** - Records V4.1 + V4.2 as shipped and closes what they left open: the checked-in V4.2 recipe **cannot rebuild the artifact production serves** (two stale hash pins fail closed, so rollback-by-rebuild does not currently exist), the Track-1 matcher non-monotonicity behind the owner-accepted 103-identification loss, and the scoping of discovery-v3 — which Phase 136.1's reference-side display waits on.
- [ ] **Phase 149: Discovery Performance & Operability** - The findings page costs ~2 s with 1.1-3.0 s unaccounted for; a second index candidate on `discovery_identification` (P3); and two fixes made 2026-08-19 that are **written but not deployed** (perf-watch misattributing every slow request, `/admin` blocking the event loop on every page build). Source: `docs/specs/discovery-performance-situation-2026-08-20.md`.

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
  6. A small claim-free homepage teaser card links to `/atlas` under the widened ATLAS-PREVIEW exception — CLS-safe, no claim-level statements, gated by the dedicated atlas-preview flag, masking-scan-clean, EN/HE + RTL, and `noindex` until the REL-01 gate (the full homepage discovery band was Phase 139's, now **139b SC2** — and it shipped early on 2026-08-12/13, retro-credited).

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
**Depends on**: Phase 134 (frozen-frame artifact + sidecar band metadata). CERT-01's frame freezes AFTER Phase 134 distillation stabilizes; its grading runs as a research track IN PARALLEL with Phases 136–138 and its completed certificate gates public promotion in **Phase 139a** (REL-01) — though the flag was in fact flipped ahead of that gate on 2026-08-08; see the REL-01 section below.
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

**Goal**: A researcher browsing a manuscript sees its computed same-work identifications and related manuscripts — banded, masked, with our-text-only evidence, and honestly framed as text matches rather than asserted identifications — can navigate to a per-work witness-map page grouped by codicological unit, and can sweep the whole corpus for findings that no finding aid already records. Everything ships behind the flag; nothing goes public until Phase 139. *(Historical — written before the flag flip. `DISCOVERY_ENABLED=1` in production since 2026-08-08 and these surfaces are public; the sentence is kept as the phase's own record, not as current policy.)*

**⚠ SCOPE EXPANDED 2026-07-30** (owner, `136-CONTEXT.md`; discuss-phase ran both the real-data mockup gate and the Codex adversarial gate before planning). Four decisions reshape this entry:

- **The novelty axis (NOVEL-01/02) moved IN**, reversing the 2026-07-28 addendum's post-136 homing — the owner's primary reason the surfaces are worth shipping is finding what is *not* already in the finding aids, and the surfaces cannot be designed around a filter that does not exist. This carries the `track1_direct` coverage gap (the frozen v2 asset leaves all 254,612 direct rows at `is_new = 0`), the rewired LLM title gate, and the heuristic funnel.
- **VIS-01** (public/private projection) is homed here and must be derived at BUILD time, before raw provenance ids are discarded.
- **Two NEW surfaces** beyond the panel and the work page: computed identifications on `/catalog-browse`, and a corpus-wide **findings page** with its own nav entry (owner: *"a big new amazing feature… maximum ability to see new findings"*).
- **ONE rebuild + ONE production redeploy** (flag-OFF) is the first execution gate — the surfaces need stored fields the frozen v2 asset does not carry, and the owner authorized exactly one, not a series.

**Depends on**: Phase 135 (band-display contract + methods page + certificate framework; CERT-01 measured PASS — weighted 0.9382 CI [0.9084, 0.9644]). Builds on the LIVE v2 asset, NOT on the gen-2 evidence refresh — that becomes **discovery-v3** in its own later phase (D-01). ⚠ **Renamed 2026-08-05:** this line read "discovery-v2.1" until Phase 136's own additive rebuild took that name on 2026-08-03 (`docs/specs/discovery-frames-v2.1.md` — built, deployed, currently serving), at which point the sentence read as waiting on something already shipped. Rationale and the full list of affected records: `docs/specs/discovery-v3-naming.md`. Translations, RTL, and accessibility are built into these surfaces from line one per house convention; comprehensive cross-surface i18n/RTL/a11y verification is now **Phase 139b SC3** — and since the surfaces are already public it is a live quality obligation, not a pre-launch gate.
**Requirements**: PANEL-01, PANEL-02, VIS-01, NOVEL-01, NOVEL-02 — and this phase AMENDS BAND-03 + BAND-05 (no precision percentages anywhere; qualitative methods page) plus `discovery-band-labels-v1.md` §2/§3 and `discovery-budgets.md`. **PANEL-03, WORK-01 and WORK-02 moved to Phase 136.1** (owner, 2026-08-02).
**Success Criteria** (what must be TRUE):

  1. **The one authorized rebuild lands in production, flag-OFF, before any surface work** — one new asset carrying: the D-02a *authorization* for tier-A default visibility (`measurement_status='measured_pass'` + `ci_low`, precision still NULL — no number is stored or shown), direct-family `coverage_ppm` + validity status, the novelty shade flag (ten values — see success criterion 6) for ALL evidence families with masked provenance, the VIS-01 public/private projection fields, and the materialized sort keys + indexes the new surfaces need. **ADDED 2026-08-01/02** (see `docs/specs/discovery-v2-bake-plan.md` Amendment 2026-08-01): **work-side match offsets `w_start`/`w_end` for every corpus** plus Sefaria reference resolution from the 322 existing `*.versemap.json` sidecars (stage 1; JA divisions deferred, M-source stored-not-displayed) — the matcher already computes the position and discards it at ingest; **the materialized main-pool bucket flag + its reason code**, since recomputing coverage/competition/aggregation at query time is not viable inside PERF-01; **`works.genre`** (entirely NULL today) for the findings-page domain facet; and a **`discovery_routing_audit` fix** so `kept_tie` rows carry `demoted_work_id`. Note `coverage_ppm` already subsumes the page-letter denominator the main-pool coverage gate needs — no separate table. The verifier, the DATA-05 masking scan, the golden fixture, and a rebuild-preservation gate (nothing silently lost versus the frozen v2 row counts) all pass; the CERT-01 pre-registration artifact is untouched.
  2. On a browse page, a "Computed identifications / זיהויים מחושבים" button (on the staleness-guarded enrichment path, within the PERF-01 browse-enrichment latency budget) opens the manuscript's list in honest disclosure levels — with "on this page" and "elsewhere in this manuscript" that NAMES the works, per-row matched-letter coverage labelled as such, match framing rather than asserted identity, no precision percentage and no review badge, and placeholder inline voting controls (wired to JUDGE-01 in Phase 137). **AMENDED 2026-08-01/02 (owner):** the disclosure model is **two buckets — "main pool" / "more matches"** drawn by the rule in `.claude/skills/sketch-findings-genizahsearch/references/main-pool-rule.md` (multi-folio agreement **or** near-full page coverage, as non-compensating floors; 56% / 44%), **not** a three-level confidence scale — one was designed and then retired on 2026-08-01 as a duplicate of a rule the codebase already had, which had mislabelled the best-measured population in the system. Band labels become **tooltip-only**; the visible row chip states the relation. **Whether the panel's third disclosure level survives is an OPEN D-13e decision** — its middle "also shares text with" bucket is behind-the-default on quality *and* distinguished only by relation, which the relation chip now carries. Variant D (even panes, ≥900px 1fr/1fr, stacking page-then-manuscript on mobile) is the selected layout — **D-09 owes a narrow amendment striking "collapsed"** while keeping its ordering.
  3. An on-demand evidence view shows the supporting span(s) from OUR manuscript text with match stats, highlighting only where offsets exist, and evidence fails closed on HTR-version drift (offsets validated at render time). **AMENDED 2026-08-02 (owner): reference text MAY be rendered — but per-work and licence-gated, never per-corpus.** The gate is the acquisition manifest's per-work `reuse_ok`, which already exists: `yes` (277 works — Public Domain 264, CC-BY 11, CC0 1, CC-BY-SA 1) may render, carrying the manifest's generated `attribution_text` where the licence requires it; `unclear` (46 — 42 `unknown`, 4 CC-BY-NC), `noncommercial_only` (1) and **absent** (21 — every JA work, which has no licence metadata at all) all fail closed. M-source never renders. "Public corpus" is NOT the operative test: ~17% of Sefaria works are not clearly reusable, and JA — public, and displayed elsewhere in the UI — currently has zero licence evidence, so its *text* cannot render until that is answered (a separate question from its missing divisions). Also decide the render source: the staged bodies are lossy (nikud, punctuation and rubrics stripped at acquisition), so either accept stripped text as the comparison view or re-fetch display text at render time.
  4. A `/work/{id}` page lists all identified carriers in the dated snapshot grouped by codicological witness unit (joined/part-grouped fragments appear as ONE witness, displaying the highest band among members) with per-witness band labels, filters AND-composed (empty = all), server-side pagination with the real total visible (PERF-01 page cap), deterministic tier-first sort, and counts that count units rather than claim rows. **AMENDED 2026-08-01: the "tier filter" is DELETED, not renamed** — quality is now the bucket (a default plus a "show more" toggle) and kind is the relation filter, so a tier-labelled control would be a second vocabulary for an axis the rows no longer speak. Tier A stays reachable because §4 already holds it behind the screening toggle. Coverage and novelty filters remain.
  5. Works are findable by neutral title (bilingual normalization + alias/duplicate handling) and reachable from the panel, from an extended `/catalog-browse` Browse-by-Identification that now carries computed identifications alongside catalogued ones, and from a **new corpus-wide findings page with its own nav entry** — server-paginated inside the PERF-01 budget with the indexes that requires. **AMENDED 2026-08-01/02:** nav label **"Computed Identifications / זיהויים מחושבים"** ("Discoveries" is taken by the Community page, a bare "Identifications" collides with Browse-by-Identification, and D-23b bars "new"). **All three row units ship, user-selectable** via a "Show as" control, defaulting to one row per identification (65,200; per-page counting inflates same-work matches ~2.3×). Filters: the two buckets, novelty, coverage, plus a **domain/author/work cascade on the IDENTIFIED WORK's domain — never the manuscript's catalogue domain** (Moss. V,374 is catalogued *Court Records* while carrying a correct Rashi finding; 338 tier-A findings sit on documentary-catalogued manuscripts). The tier filter is deleted here too. A **mode strip** ships now with Phase 138's leads and Phase 137's saved judgments greyed and phase-tagged, so both add a tab rather than a page. **OPEN (D-16/PANEL-01):** whether this page also gains the panel's relation filter.
  6. **The novelty axis is live and structurally orthogonal to the tier**: a TEN-VALUE shade enum, fail-closed (`confirms` / `refines_granularity` / `aid_more_specific` / `diverges_work` / `diverges_part` / `container_predicts` / `fills_gap` / `extends` / `alias_merge` / `not_checked` — never novel by default; `fills_gap` is the sole "Candidates for new finds" value) — **⟨AMENDED 2026-08-02, owner rulings E/E′/F/G/H, `136-GATE1-DECISIONS.md` §§ E-H; this criterion originally recorded a three-value tri-state that was never updated as the shade enum widened in-session — see `136-NOVELTY-PRIOR-ART.md` for the propagation-failure finding this amendment closes⟩** — computed per `(sys_id, work)` against an enumerable versioned source set recorded in `meta`, keyed on a reviewed alias-aware identity, worded "Not found in the finding aids checked" with the checked-source list and as-of date, provenance masked to a non-identifying label, and never feeding band assignment, ranking, precision copy or styling. `diverges_work`/`diverges_part` carry a default-hidden, explicit-warned toggle (ruling F); `container_predicts` does NOT (ruling H — no disagreement to warn about). A separate `divergence_correctness` field records which side is right on a divergence row, never folded into the shade token. The LLM gate has a reproducible contract (pinned prompt hash, model + version, measured cost) — **its prior validation (40/40 agreement; 99% vs 103 human grades) covers the FIVE-way vocabulary and the ORIGINAL one-title-string input contract only; owner ruling I gates the production run on a fresh re-measurement against the CURRENT ten-value/free-text contract before that validation may be relied on.** **⟨AMENDED 2026-08-03, owner rulings K/L, `136-GATE1-DECISIONS.md` §§ K, L⟩** The ruling-I re-measurement (60 shade cases, 78.3% agreement, ZERO true `fills_gap` cases in the pool) re-derived the real production cost at **~$301** (an 11× jump from the stale `~$27` figure) and left the axis decision B cares about most (false-novel rate) entirely untested — **ruling K keeps the ~$301 run UNAUTHORIZED** pending a purpose-built probe measuring the false-novel rate on both paths to candidacy (the model's `fills_gap` verdict, and the ungated no-source-text bypass). **Ruling L drops `divergence_correctness` from the model's job** (measured 8/28, at or below chance) — it remains a human/owner annotation only; the column and prompt/spec citations above are otherwise unaffected in shape. **⟨CORRECTED 2026-08-05, owner-confirmed — two claims above are stale and must not be re-cited⟩** (1) **The production run HAPPENED.** The owner confirms it on 2026-08-05, and the trail corroborates: `discovery_data/novelty_production_manifest.json` (started 2026-08-03T12:57, `batch_size: 10`, ceiling $45), a 55,184-entry checkpoint, and the resulting cache `novelty_production_verdicts.json` — SHA-256 `eb6fc4f8…`, **the exact hash the LIVE asset pins**, recorded as gated PASS in `136-REBUILD-GATES.md`. So "ruling K keeps the run UNAUTHORIZED" describes the state when it was written, not the outcome. (2) **The `~$301` figure is wrong by ~7.5×.** Real spend, summed from the run's own per-call `usage.cost` log (`novelty_production_cost_log.jsonl`, 5,528 calls): **$40.12**. `~$301` was a per-case *unbatched* projection off a stratified hard-case pool; `batch_size: 10` is the whole difference, and the batching lever was itself ruling O. Unit cost **$0.000727/case**. Full derivation + the discovery-v3 projections: `docs/specs/discovery-v3-bake-plan.md` §1.3/§4.
  7. **No precision percentage is reachable from any surface** — the methods page is rewritten qualitatively (tiers explained in words plus the non-percentage facts: that grading happened, population, unit, sample size, grader, date, method, audit state, immutable report id), `web/pages/help.py`'s existing estimates and intervals are removed, and no surface claims human review until the provenance of the 121 `human_confirmed` rows is established.
  8. Every surface hides cleanly with the flag off or the sidecar absent, stays inside the PERF-01 budgets (a versioned `discovery-budgets.md` entry for the findings page), and passes the masking scan on rendered output, JSON payloads, copy/export paths and error paths.

**Plans**: **22 plans in 10 waves** — replanned 2026-08-02 at `standard` granularity after the owner re-scope, then revised 2026-08-04 after the Codex pre-flight's **round 5** (the six then-unexecuted plans re-audited against the code 136-01..136-14 had actually built: 1 BLOCKER / 8 HIGH / 3 MEDIUM / 1 LOW). Round 5's BLOCKER added **136-22**, the launch-statistics reader owner ruling U requires. Three gates, not six: (1) the trimmed rebuild + the D-13d granularity rule + the D-13c threshold, (2) the panel, (3) the findings page — then the methods-page rewrite and the cross-surface masking sweep, both of which gate the public flag-on. Gate 1 still blocks every surface wave: the rebuilt asset must be live in production, flag OFF, before any UI code that reads its new columns deploys. **Three production mutations, two of them surface deploys**: the wave-5 code-plus-asset redeploy (136-13), then the panel (136-17, wave 8), then the findings page on the same asset (136-18, wave 9). Blocking owner checkpoints: **136-03** (ONE consolidated wave-1 sitting — the five open gate-1 decisions, the novelty-funnel authorization and evaluation-set size, the owner-supplied hard-case labels, and the needs-ruling domain posture) and **136-13** (the one authorized production redeploy). The Codex pre-flight ran five times — three rounds on 2026-08-02 converging to SIGN-OFF at 0 HIGH, then **round 5 on 2026-08-03** re-auditing the six unexecuted surface plans against the substrate 136-01..136-14 had by then built (`136-CODEX-PREFLIGHT.md`). Round 5's central finding class was plans describing a service layer that had since been written differently, plus **owner rulings R, S, T and U all recorded 2026-08-03, after the round-4 sign-off**, so no plan could reference them. All findings are folded in as of 2026-08-04; a second mockup pass is still owed on the built surfaces. **The panel and findings deploys are in DIFFERENT waves on purpose:** both mutate production, and a `files_modified` check cannot see that conflict because the contended resource is the box rather than the repo.

Wave rationale: two real chokepoints, not twenty-six. `scripts/build_discovery_sidecar.py` is split into parallel MODULE work (waves 2-3) and two serial WIRING passes (waves 3-4); `shared/discovery_service.py` + `web/discovery.py` are done once (wave 6) so the panel and findings tracks run concurrently afterwards. The methods rewrite (136-02) sits in wave 1 and blocks nothing. **Owner latency is concentrated into ONE wave-1 sitting** (136-03): because this project's waves are hard barriers, a checkpoint anywhere in a wave blocks the next wave regardless of dependency edges, so the fix is to minimise round-trips rather than to scatter checkpoints. Every wave-1 decision — the five gate-1 answers, the novelty spend and evaluation size, the hard-case ground-truth labels, and the needs-ruling domain posture — is answered in that one sitting, and the novelty funnel (136-04) starts immediately after it.

Plans:

- [x] 136-01-PLAN.md — Contract & requirement amendments (REQUIREMENTS, band-labels §2/§3, budgets, schema new-field contract + the narrow tier_a amendment, deploy runbook) · wave 1
- [x] 136-02-PLAN.md — Methods-page rewrite (qualitative BAND-05) + the ONE shared honesty gate · wave 1
- [x] 136-03-PLAN.md — ALL wave-1 owner decisions in one sitting: the five gate-1 answers (D-13e, D-16, D-13c, D-13b, D-13d), the novelty authorization + evaluation size, the owner-supplied hard-case labels, the needs-ruling posture · wave 1 · **checkpoint**
- [x] 136-04-PLAN.md — Novelty: identity key, pinned LLM contract, committed funnel runner, authorized run, verdict cache · wave 2
- [x] 136-05-PLAN.md — Rebuild-preservation gate, pinned from the live asset BEFORE the rebuild · wave 1
- [x] 136-06-PLAN.md — D-02a tier_a authorization lockstep (builder + verifier + both-branch fixtures) · wave 2
- [x] 136-07-PLAN.md — Main-pool rule + grouping predicates as shared pure modules · wave 2
- [x] 136-08-PLAN.md — VIS-01 two-axis derivation + closed-graph public projection + leak control · wave 2
- [x] 136-09-PLAN.md — `works.genre` curation artifact + author alias map · wave 2
- [x] 136-10-PLAN.md — Display strings, translations and the discovery CSS block (shared by both surfaces) · wave 3
- [x] 136-11-PLAN.md — Build wiring A: coverage_ppm, band_rank, indexes, discovery_identification, manuscript_display, bench · wave 3
- [x] 136-12-PLAN.md — Build wiring B: novelty ingestion, visibility axes, curated load, kept_tie fix, verifier extensions · wave 4
- [ ] 136-13-PLAN.md — The rebuild, the gate battery, the owner authorization and the one production redeploy · wave 5 · **checkpoint**
- [x] 136-14-PLAN.md — Service layer, once: envelope, panel paths, findings query, facet cascade · wave 6
- [ ] 136-15-PLAN.md — Panel display model (pure) · wave 7
- [x] 136-16-PLAN.md — Findings page shell: route, nav, caveat, modes, filter bar, result bar, pager · wave 7
- [x] 136-17-PLAN.md — Panel: browse attachment, body render, render-smoke, **panel deploy** · wave 8 (built + gated; **deploy outstanding**)
- [x] 136-18-PLAN.md — Findings rows, novelty switch, combination benchmark, render-smoke, **findings deploy** · wave 9
- [x] 136-19-PLAN.md — Cross-surface masking sweep + flag-on readiness attestation + closeout · wave 10
- [x] 136-20-PLAN.md — **Public/private loader boundary + readiness contract** (audience gate; the two new tables and their row counts in startup validation; rollback + partial-asset tests) · wave 3
- [ ] 136-21-PLAN.md — **Work-expansion service work** (anchor-side relation + band, weaker-band rule, carrier library/shelfmark + band label, count query for a real total, raising query helper) · wave 7
- [ ] 136-22-PLAN.md — **Launch-statistics reader** (ruling U: the main-pool contribution total + its three shades, artifact-backed and version-aware, with a guard forbidding the figures as literals in code OR translations) · wave 8

Numbering note: 136-20 and 136-21 were added by Codex pre-flight rounds 1-3, and **136-22 by round 5**; each carries the next free number rather than forcing a renumber. The `wave` field, not the plan number, drives execution order. **136-22 sits at wave 8 although its only `depends_on` (136-14) is at wave 6**: 136-21 already owns `shared/discovery_service.py`, `shared/discovery_surface_projection.py` and `web/discovery.py` in wave 7, and file ownership — not a contract edge — is what forces the later wave. Verified: wave 8 otherwise contains only 136-17, which shares no file with it.

**UI hint**: yes

### Phase 136.1: Read Surfaces — Evidence View, Work Pages & Catalogue Integration

**Goal**: The deferred half of Phase 136's read surfaces — an on-demand evidence view showing the
supporting span(s) from OUR manuscript text (and, where the per-work licence permits, the reference
text), a `/work/{id}` witness-map page grouped by codicological unit, and computed identifications
folded into `/catalog-browse`. Work titles on the panel and the findings page render as plain text
until this phase lands, then become links — no dead ends at either stage.

**⚠ CREATED 2026-08-02** by an owner re-scope of Phase 136, to get the panel and the findings page
live inside a one-to-two week window. Nothing here was cut for being unwanted; it was cut for being
off the shortest path to a live surface.

**STATUS 2026-08-20 — 1 of 6 criteria, and the one that moved was met by a different mechanism.**
The excerpt / "View text match" view shipped 2026-08-13 (`74702fa8` + follow-ups) outside any plan.
It satisfies the *user-visible* half of SC1 but not the specified one: excerpts are **baked at build
time**, not offset-validated at render time, so the version-drift fail-closed behaviour SC1 exists to
guarantee is not the mechanism in production. SC2's per-work `reuse_ok` licence gate is **not
visibly enforced** in `scripts/bake_discovery_excerpts.py`. SC3, SC4 and SC5 are unbuilt — no route
exists for `/work/{id}` or for the `/catalog-browse` integration, so work titles are still dead text.

**Depends on**: Phase 136 (the trimmed rebuild, the deployed asset, and both launch surfaces must
exist before there is anywhere to link from). **PANEL-03's reference-side display additionally waits on
discovery-v3**, whose scoping is now homed in **Phase 148** — it is not `docs/specs/discovery-frames-v2.1.md`,
which is built and deployed (see `docs/specs/discovery-v3-naming.md`). `w_start`/`w_end` and the Sefaria
versemap resolution were trimmed out of Phase 136's rebuild (owner, 2026-08-02) because they serve only
the reference locus and the side-by-side view, and they carried the build's hardest work (the `body` ↔
`norm_stream` coordinate mapping). **The our-text-only evidence highlight does NOT wait** — it uses the
page-side `span_start`/`span_end` offsets that already ship.

**Requirements**: PANEL-03, WORK-01, WORK-02

**Success Criteria** (what must be TRUE):

  1. An on-demand evidence view shows the supporting span(s) from OUR manuscript text with match stats,
     highlighting only where offsets exist, failing closed on HTR-version drift. The stored offsets index
     the NORMALIZED letter stream (652 chars off on the sampled case if sliced raw), the result must be
     clipped per line (72 of 148 rows vs 1, silently), one renderer must emit both discovery spans and
     search-term highlights, and the highlight drops on version change. **Partially met by the shipped
     build-time excerpt bake; the render-time validation and the drift fail-close are what remain.**

  2. Reference text renders only where the acquisition manifest's per-work `reuse_ok` is `yes` (277 works),
     carrying `attribution_text` where the licence requires it; `unclear` (46), `noncommercial_only` (1) and
     **absent** (21 — every JA work) all fail closed, asserted on the absence of the flag too. M-source
     never renders. **Not met — the gate is not visibly enforced in the shipped bake script.**

  3. A `/work/{id}` page lists all identified carriers grouped by codicological witness unit (joined
     fragments appear as ONE witness), AND-composed filters, server-side pagination with the real total,
     deterministic sort, and counts that count units rather than claim rows. No tier filter. **Not met.**

  4. `/catalog-browse` carries computed identifications alongside catalogued ones, visibly separated and
     separately worded. **Not met.**

  5. Work titles on the Phase 136 surfaces become links to `/work/{id}`. **Not met.**

  6. Every surface hides cleanly with the flag off or the sidecar absent, stays inside the PERF-01
     budgets, and passes the masking scan on rendered output, JSON payloads, copy/export and error paths.
     **The excerpt view is already covered by the masking sweep** (`test_discovery_masking_sweep.py::_excerpt_loader`,
     six states, dated 2026-08-13); the unbuilt surfaces are not.

**Plans**: TBD — **seed from the six already-written, checker-verified plans** archived at
`.planning/phases/136-read-surfaces-connections-panel-work-witnesses/superseded-2026-08-02/`:
136-17 (offset renderer), 136-22 (gate-3 decisions), 136-23 (evidence view), 136-24 (work service
extension), 136-25 (`/work/{id}` page), 136-26 (titles + `/catalog-browse`). Replan against them rather
than from scratch — they passed the plan-checker on 2026-08-02, but 136-23 must now be replanned
*against the shipped excerpt bake* rather than against a blank page.
**UI hint**: yes

### Phase 137: Community Judgments — Hardening

**⚠ RETITLED AND RE-SCOPED 2026-08-16** (owner: "ratify reality"), detail block rewritten 2026-08-20.
A beta identification-reviews feature shipped 2026-08-13 (`7268a7eb`, `b7618f5a`) outside any plan and
is live and default-ON (`IDENTIFICATION_REVIEWS_ENABLED=true`). This phase's job is no longer to build
from zero.

**Goal**: The shipped reviews feature acquires the two properties it structurally lacks — append-only
judgment history with the band captured at judgment time, and aggregate signals kept separate from
moderation state — without regressing what already works.

**Depends on**: the shipped beta feature (`web/identification_reviews.py`, Supabase SECURITY DEFINER
RPCs) + Supabase auth. No longer depends on Phase 136 in any blocking sense — the claims it judges
have been rendering in production since 2026-08-08.

**Requirements**: JUDGE-01, JUDGE-02, JUDGE-03, JUDGE-04, JUDGE-05

**Credit already earned** (recorded 2026-08-16): **JUDGE-03 and JUDGE-05 in full; JUDGE-02 partially**
— RLS, explicit GRANTs and SECURITY DEFINER RPCs exist, but the only test is a substring assertion
against the SQL file's text. **2 of 5.**

**Success Criteria** (what must be TRUE):

  1. **JUDGE-01 — append-only.** Changing a vote inserts a superseding DB-enforced event rather than
     overwriting the row, and every judgment persists claim id, claim type, sidecar version, **and the
     band shown at judgment time**. Carries a live-data decision the phase must answer explicitly:
     **vote history for judgments already cast under overwrite semantics is not recoverable** — the
     migration either back-fills a synthetic genesis event per existing row or declares the pre-migration
     history lost, in writing.
  2. **JUDGE-02 — proven, not asserted.** A live role-matrix test exercises anonymous / authenticated /
     owner / moderator against every RPC and table, replacing the substring assertion.
  3. **JUDGE-04 — aggregates and moderation are separate append-only events.** One aggregate per claim
     rather than one row per review; admin hide/spam are events in a separate table, not mutations of a
     status column; hidden and spam judgments are excluded from aggregates.
  4. Community judgments still never affect band assignment, precision copy, ranking, or certified
     styling — re-verified as a structurally separate layer after the schema change.
  5. The masking sweep over the reviews surface (`tests/render_smoke/test_discovery_masking_sweep_reviews_home.py`,
     21 tests, added 2026-08-19) stays green through the migration, including the mailto link target and
     the outbound Supabase write.

**Plans**: TBD
**UI hint**: yes

### Phase 138: Leads Queue

**Goal**: The high-recall R-B / R-CANON screening lane becomes a reviewable, explicitly-uncertified queue where users triage leads with the same voting used everywhere else.

**Depends on**: Phase 137's hardened voting (lead review reuses JUDGE-01 voting + typed refinement). No new table — the data already lives in the sidecar claims banded `screening_rb` / `screening_canon`.

**STATUS 2026-08-20 — unchanged, and the one later phase where nothing was pulled forward.**
`web/pages/findings.py` renders the mode strip's leads tab as "Coming soon"; no `/leads` route exists.
The 2026-07-30 owner ruling — that `/leads` is a MODE of the findings page, not a second
implementation — is now testable rather than hypothetical: the findings page shipped with a mode
strip, so the phase either fills that tab or justifies a separate page against a surface that exists.

**Requirements**: LEADS-01, LEADS-02

**Success Criteria** (what must be TRUE):

  1. The R-B lane is presented as the lead queue with the BAND-03 "possible identification" framing,
     pagination, deterministic sort, and band/library filters — **as the findings page's leads mode**,
     or with a written justification for a separate route measured against the shipped mode strip.
  2. The canon lane is separately caveated (including the known Targum-confusion class), not merely ranked lower.
  3. Reviewing a lead uses the same JUDGE-01 voting (main verdict ✓ / ? / ✗ + note) plus an OPTIONAL advanced typed refinement drawn from the fixed vocabulary (A cites B / B cites A / common source / compilation / another language or edition / …).
  4. The leads lanes carry the same masking discipline as the default lanes, swept as rendered output rather than inferred from the default-lane sweep.

**Plans**: TBD
**UI hint**: yes

### Phase 139a: Release Hardening & REL-01 Gate Closure

**⚠ SPLIT OUT of Phase 139 on 2026-08-16**; detail block written 2026-08-20.

**Goal**: The discovery module stops being a beta with open gate items — the five REL-01 items still
open at the 2026-08-08 flag flip are closed or formally retired, so "beta → released" becomes a
decision the record can support.

**Depends on**: nothing blocking. The flag is already on; this phase closes the gap between what
shipped and what the gate asked for.

**Requirements**: REL-01, VIS-02, CERT-02, D-06b

**Already closed — do not re-do**: the cross-surface masking sweep, which was the one item NOT waived.
Its artifact half was re-run 2026-08-16 against the real deployed build (the sweep had been attesting
an artifact production stopped serving on 2026-08-15) and its surface half on 2026-08-19
(`ef373de2`, 21 tests, mutation-proven at both data and code level). One of the three named surfaces
— the excerpt view — turned out to be already swept; the entry was stale in saying otherwise.

**Success Criteria** (what must be TRUE):

  1. **D-06b** is either applied to the live surfaces or formally retired with the reason recorded —
     not left open indefinitely as "disclosure detail".
  2. A **correction / retraction policy** exists and is reachable from the surfaces that make claims.
     It lost its carrying requirement when the curated-surface exception was declined on 2026-07-28,
     and is genuinely owed.
  3. **VIS-02 reconciliation** is completed or explicitly folded into VIS-01's build-time enforcement,
     with the difference between them stated.
  4. The **browser-check attestation** is recorded. The CI `findings-browser-check` job exists and runs;
     what is missing is the written record, not the check.
  5. The **CERT-02 tier-A copy** is either applied or explicitly deferred with its trigger named. It is
     moot only while the no-percentages ruling stands, and `band_precision` remains un-re-baked — so
     "deferred until the ruling changes" is an acceptable outcome, but it must be written down.
  6. The **per-stratum precision spread** (1.000 ja → 0.471 `msource:medium`, one work causing 45% of
     all measured error) reaches the BAND-05 methods page. A weighted headline without the spread
     misleads anyone in the weak stratum — the CERT-01 measurement document says so itself.

**Plans**: TBD
**UI hint**: no

### Phase 139b: Atlas Drill-down & Homepage Capstone

**⚠ SPLIT OUT of Phase 139 on 2026-08-16**; detail block written 2026-08-20.

**Goal**: The connection atlas becomes the flagship — the Phase 133 static preview upgraded or absorbed
into a server-bounded drill-down explorer — with the cross-cutting i18n / RTL / a11y / SEO / observability
work that the whole discovery module owes.

**Depends on**: Phase 139a for the release-gate half. The drill-down explorer is the highest-risk surface
in the milestone and remains the first candidate to cut to fast-follow — the Phase 133 static preview
already exists as the fallback flagship.

**Requirements**: ATLAS-02, ATLAS-03, SEO-01, I18N-01, I18N-02, A11Y-01, A11Y-02, OBS-01, OBS-02

**Credit already earned**: **SC2 shipped early on 2026-08-12/13** (`c413d5e9`, `fcb1eb8e`) and is
retro-credited in full. Every route and teaser is gated on `atlas_preview_available()` /
`discovery_available()` — the availability predicate, never a bare flag — so the clean-hide discipline
this phase exists to enforce was respected by the code that jumped the queue.

**Success Criteria** (what must be TRUE):

  1. A server-bounded drill-down explorer — upgrading or absorbing the Phase 133 static preview page —
     serves capped neighborhoods (PERF-01 node/edge/byte caps, single-hop server-side expansion) without
     the client ever loading the full edge set; the ATLAS-01 static overview remains the offline-precomputed
     entry view.
  2. ~~A CLS-safe static homepage band promotes the atlas + discovery~~ — **MET 2026-08-12/13, retro-credited.**
  3. All discovery surfaces are fully translated EN/HE with passing HE-mode render-smoke tests and RTL/bidi
     correctness including graph/atlas labels; confidence is never signaled by color alone, the graph has a
     textual/table equivalent, contrast passes on all new text/controls/badges/marks, keyboard + screen-reader
     labels work, and animations honor reduced-motion. **This now covers surfaces that are already public** —
     it is a live quality obligation, not a pre-launch gate.
  4. SEO outputs pass inside the DATA-05 masking gate — `/work/{id}` + atlas get canonical URLs, EN/HE
     hreflang, titles/descriptions, sitemap inclusion, neutral-only JSON-LD; `/leads` + uncertified-toggle
     states are `noindex` — and privacy-allowlisted PostHog product events + operational metrics are wired
     with the denylist enforced (never titles, text, shelfmarks, raw research IDs, or free text).
  5. The **"beta → released"** transition, not the flag flip. The flag flip happened on 2026-08-08 and is
     recorded below; what remains is removing the beta label once 139a's gate items and this phase's
     cross-cutting checks pass.

**Plans**: TBD
**UI hint**: yes

### Phase 140: Milestone Bookkeeping & Debt Closure

**⚠ NEW 2026-08-16**, re-scoped 2026-08-20 (one of its three jobs was done by this re-map).

**Goal**: Every production-affecting feature in the discovery module has a plan trace, and the planning
record stops contradicting itself.

**Depends on**: nothing. Sequenced early because other phases plan against numbers this phase makes true.

**Requirements**: none directly — this phase serves REL-01's auditability.

**Success Criteria** (what must be TRUE):

  1. Retro-plans exist for the two production-affecting features that have no plan file at all: the
     **relation precedence matrix** (`shared/discovery_relation_matrix.py`, 2026-08-12/13 — production code
     governing what a claim is allowed to assert) and the **excerpt bake**.
  2. The `STATE.md` ↔ ROADMAP contradiction about the production LLM novelty run is reconciled against
     what actually ran (the gate was re-run on gemini-3.7-flash on 2026-08-18: 69,732 verdicts, $9.43,
     81.7% label agreement with 3.6).
  3. The two plans left without a SUMMARY — `134-07` and `136-13` — are either summarised from the shipped
     code or explicitly marked withdrawn.
  4. ~~Decide the phase home for the reference-expansion track~~ — **DONE 2026-08-20 by this re-map:
     Phases 148 and 149.**

**Plans**: TBD
**UI hint**: no

### Phase 141: Passage Matching — Algorithm Specification

**Goal**: One tracked, self-contained document lays out the character-level passage matcher, so it is
implementable without access to the gitignored research tree.

**Depends on**: nothing. The algorithm exists and is in production inside the discovery pipeline; what
did not exist was a narration of it — the closest document (`same_work_spike/probe/METHOD.md`, 606 lines)
is gitignored and covers Track 2, while the Track-1 direction parallels search needs was design-level only.

**Requirements**: PASS-01

**STATUS: BUILT on branch `claude/computed-id-parallels-search-a7c8fd`, unmerged** —
`docs/specs/passage-matching-algorithm.md` v1, 13 sections (`ce7c8beb`).

**Success Criteria** (what must be TRUE):

  1. Normalization is specified and **versioned** (`normalizer_version`), as a hard artifact-identity input.
  2. Gram coding, index orientation, diagonal-keyed candidate generation and verification are each
     specified with their real constants and a provenance column.
  3. **Both** verification boundaries are stated with the reason there are two — two-sided (both noisy,
     survival ∝ (1−CER)⁴) and one-sided (clean query, ∝ (1−CER)²) — with calibration provenance.
  4. **One** minimum-span number in normalized letters, ending the probe's 25/30/40 conflict, and
     classified as **query policy, not an artifact input** — changing it must not invalidate a built index.
  5. Stage-0 hygiene is documented as mandatory with its measured precision/recall.
  6. An explicit **"what is not established"** section: corpus-wide precision never measured, confusion-weighted
     alignment costs unimplemented, matres-light normalization unvalidated, Judeo-Arabic without an evaluation stratum.
  7. `scripts/check_atlas_masking.py` passes **with `MASKING_SCAN_PATTERNS_FILE` configured** — reference
     corpora appear only under masked codenames. A green run with the variable unset is meaningless.

**Plans**: retro-plan owed (built outside GSD)
**UI hint**: no

### Phase 142: Passage Matching — Spike, Closed-Subset Comparator & GO/NO-GO

**Goal**: A genuine go/no-go before any multi-GB artifact exists — the artifact measured, the
construction chosen on evidence, and both methods compared validly at small scale.

**Depends on**: Phase 141.

**Requirements**: PASS-02, PASS-03, PASS-04

**STATUS: BUILT on branch, unmerged** — `docs/specs/passage-index-build-measurements.md`, 7 sections.
Three findings overturned the plan rather than confirming it: **`spool` beat mass-partitioned `scatter`**
and needs no scratch space; batching by records cost 3 GB of RAM and the removed sort was faster
(`d80c357c`); the query caps were **pure waste**; and the author's own rarity proposal was refuted by his
own measurement (`f2e19e1a`). The build benchmark caught two bugs in itself on first run (`ffe0595d`).

**Success Criteria** (what must be TRUE):

  1. Artifact size, postings/page, bytes/page and DF distribution are measured, not projected.
  2. The two constructions are **benchmarked against each other** on wall-clock and peak RSS, and the
     winner ships — declaring a winner in advance is what broke the first design.
  3. A full latency acceptance table with **declared thresholds**: warm **and cold** cache p50/p95/p99 at a
     stated concurrency, postings consumed, candidates, verifications, page faults, peak RSS, queue time.
  4. DF cap, stride and budget policy chosen by measurement — no-cap vs band-allocated vs rarest-first
     under identical budgets.
  5. A **closed corpus subset** with all positives wholly inside it, so both methods compare validly at
     small scale, with the incumbent page-scoped and the eligible-page manifest applied.

**Plans**: retro-plan owed (built outside GSD)
**UI hint**: no

### Phase 143: Passage Matching — Shared Engine, Builder & Full Index

**Goal**: The engine, the builder and the full-corpus artifact exist behind two gates — builder/integrity
and query determinism — with integrity meaning reconstruction, not cursor accounting.

**Depends on**: Phase 142's GO.

**Requirements**: PASS-02, PASS-03

**STATUS: BUILT on branch, unmerged** — full-corpus index built; a cap that truncated by catalog position
removed; 5 s queries down to 0.6 s (`b9d5c594`). The normalizer was ported **byte-exact** and proved so
against 737k letters of real corpus (`bf17eb67`). The release verifier's own sampled order check was
caught letting a corrupted artifact pass (`318a0336`) — found by the gate, not by review.

**Success Criteria** (what must be TRUE):

  1. Modules land in `shared/` framework-agnostic: normalize, builder, index reader, search, hygiene,
     plus build/verify/bench scripts.
  2. **Integrity is not cursor equality.** Artifact hash, per-page stream hashes, page-boundary assertions
     so grams never cross a page, byte-for-byte parity against an in-memory reference builder, and a
     reconstruction check that re-derives sampled postings from `streams.bin` to their CSR location.
  3. The layout version is in the manifest and load **refuses on mismatch**; overflow fails the build loudly.
  4. Budget determinism with a **pass/fail acceptance rule** for query-prefix monotonicity — a bounded
     regression tolerance on held-out positives, or the monotonicity claim is dropped. A test that merely
     detects the problem changes nothing.
  5. Every cap that fires is reported in the envelope; nothing truncates silently.
  6. The display-span path (bounded re-normalization of only the rendered pages) is inside the latency budget.

**Plans**: retro-plan owed (built outside GSD)
**UI hint**: no

### Phase 144: Passage Matching — Full-Scale Evaluation & Default Decision

**Goal**: Whether passage matching beats the incumbent is settled by measurement against pre-declared
endpoints, on data that was not used to tune it.

**Depends on**: Phase 143's artifact.

**Requirements**: PASS-04, PASS-05

**STATUS: IN PROGRESS on branch — TUNING SPLIT ONLY. The holdout has never been touched.**
Interim FGP self-retrieval, n = 120, ground truth deliberately retreated to **manuscript grain, not
folio grain** (only 18,362 of 45,034 FGP rows carry a folio label, and using the CER script's fuzzy
content matching would let a fuzzy matcher decide what a passage matcher is supposed to find):

| method | recall@1 | recall@10 | recall@50 | MRR | p50 | p95 |
|---|---|---|---|---|---|---|
| passage `standard-40` | 0.592 | 0.708 | **0.750** [0.666, 0.819] | 0.639 | 391 ms | 508 ms |
| chunk `c3-exact-f100` | 0.467 | 0.658 | **0.708** [0.622, 0.782] | 0.533 | 14,565 ms | — |
| chunk `c5-exact-f100` | 0.383 | 0.542 | **0.575** [0.486, 0.660] | 0.440 | 22,262 ms | 195,143 ms |

**The hypothesis is under real pressure and the record should say so.** Sweeping the incumbent's chunk
size **overturned the author's own conclusion** (`7cc50fe7`): at `chunk_size=5` the intervals do not
overlap, at `chunk_size=3` they overlap heavily and passage's lower bound (0.666) sits below the
incumbent's point estimate. **On recall@50 there is no demonstrated difference at n=120.** Where passage
separates is **ranking and speed**, not retrieval: recall@1 0.592 vs 0.467, MRR 0.639 vs 0.533, 391 ms vs
14,565 ms (37×). A further inequality worth 26% — the two methods searching different corpora — was found
and fixed (`926bb1c9`).

**Success Criteria** (what must be TRUE):

  1. **The comparison is equal on all three axes it was unequal on**: a page-scoped incumbent comparator
     (composition search passes no scope filter, so it also matches `sys:`/`part:` pseudo-documents), one
     frozen eligible-page manifest applied to both methods, and stratification by per-method rank quantile
     — never by pooled raw score, which is not numerically comparable across methods.
  2. Four instruments run: FGP self-retrieval (with its same-folio-recognition limit stated), witness-index
     recall over **at least three** compositions in different genres (one formulaic composition cannot
     establish default behaviour), the catalogue yardstick **as recall only, never acceptance evidence**,
     and a pooled blinded precision deck in CERT-01 shape with method label and rank stripped and evidence
     rendered method-neutrally.
  3. The incumbent is swept across chunk sizes 2/3/5+ × exact/variants/fuzzy — the sweep the Context
     hypothesis owes, and the one that has already changed the answer once.
  4. **The default flip is decided against pre-declared endpoints**: recall@50 primary, precision@k
     secondary, a one-sided 95% CI lower bound **no worse than 3 points** versus the incumbent in **every**
     named protected class (query-length band, genre, language, page-CER band), multiplicity handled, sample
     sizes declared before drawing. A class whose sample cannot support that precision **blocks by
     insufficiency** — which is an honest outcome, not a failure.
  5. Tuning and evaluation splits stay separate — DF cap, budget, stride and boundary are chosen by
     measurement and must not be fitted on the deciding data.
  6. **Not earning the flip is an acceptable result.** Shipping as a selectable method is cheap to be wrong
     about; flipping the default is asymmetric and mostly invisible, because in a research tool a recall
     regression is indistinguishable from an absence of evidence.

**Plans**: TBD
**UI hint**: no

### Phase 145: Passage Matching — Web Surface

**Goal**: `/parallels` offers both methods with the passage path fail-closed, scope-restricted, and
inside declared capacity limits.

**Depends on**: Phase 144 (a surface for a method whose evaluation is unfinished ships as an option, not
a default).

**Requirements**: PASS-06, PASS-07

**Success Criteria** (what must be TRUE):

  1. `method: 'chunk' | 'passage'` on `ParallelsRequest` with `SEARCH_API_PASSAGE_TIMEOUT`; **`passage`
     with a non-Genizah scope is rejected by the API**, not merely hidden in the widget.
  2. `web/passage_assets.py` mirrors `web/discovery_assets.py`: flag **AND** manifest **AND** input-hash
     **AND** layout-version, else `ready=False` and a clean hide.
  3. Its **own `ThreadPoolExecutor` with `max_workers` equal to its semaphore capacity**. Two semaphores
     over one unconfigured default pool are two names for one budget — that already cost a fix in
     `shared/discovery_service.py`.
  4. A safe highlight markup contract with a DOM test matrix in **both RTL and LTR**. NiceGUI's
     client-side sanitizer strips `class` from `ui.html` on this very page.
  5. Declared cold-cache SLOs met before deploy: cold p95, concurrency target, RSS/page-cache residency
     budget, executor saturation limit, loop-lag ceiling. One Uvicorn worker, and a 7.8 GB index has been
     evicted on this host by a 1.4 GB read/write before.
  6. i18n inventory through both translation paths, with tests — only raw Hebrew literals leak.
  7. `docs/SEARCH_API.md` documents the new field, the span-shaped `matches[]`, and the scope restriction.

**Plans**: TBD
**UI hint**: yes

### Phase 146: Passage Matching — Desktop Surface & Release

**Goal**: The desktop app builds the index locally and offers the method, and v9.0.0 ships on the desktop
line with this as its headline.

**Depends on**: Phases 144 and 145.

**Requirements**: PASS-06, PASS-08

**Success Criteria** (what must be TRUE):

  1. `build_index.py` gains a third target; the build runs in a **background worker — never `__init__`,
     never the UI thread** — with progress, a resumable checkpoint and a free-space preflight. Building
     inline on the UI thread has already frozen launch here once.
  2. A stale-index rebuild prompt fires from the artifact's input manifest, and **changing the
     minimum-span number does NOT trigger a rebuild** — it is query policy.
  3. The method selector is **disabled with a translated explanation whenever scope is `local` or `all`**,
     enforced in API validation and in session/history restore, not only in the widget. My Library passage
     search is deferred, and the UI says so.
  4. A clean "index not built yet" state, and the whole flow never blocks the UI.
  5. Release plumbing untouched: no installer data change, no hosted asset, no 2 GB split.
  6. Flip or don't flip per Phase 144 — **routing is explicitly out of scope**; an eligibility rule ships,
     a router does not.

**Plans**: TBD
**UI hint**: yes

### Phase 147: Passage Mode on Regular Search (optional)

**Goal**: A fourth `search_mode` on regular search, if and only if it earns it on its own evidence.

**Depends on**: Phase 146.

**Requirements**: PASS-09

**Success Criteria** (what must be TRUE):

  1. Gated on its **own** comparison against `execute_search(..., mode="fuzzy")`. Instruments 1-4 compare
     composition methods and cannot license a regular-search change.
  2. The eligibility floor is expressed as a **minimum span in normalized letters**, not a word count —
     two distinct 5-grams need only six letters, so "5-7 words" is the wrong unit.

**Plans**: TBD
**UI hint**: yes

### Phase 148: Reference Expansion & Bake Reproducibility

**⚠ NEW 2026-08-20.** Homes the V4.1/V4.2 reference-expansion track, which shipped between 2026-08-16 and
2026-08-19 with no phase to belong to. Numbered after 147 only because 141-147 were already claimed by
in-flight work — this is **not** sequenced behind the passage track.

**Goal**: The reference corpus can be rebuilt from what is checked in, and the matcher behaviour that
silently deleted live identifications is either fixed or bounded by a gate.

**Depends on**: nothing blocking. Phase 136.1's reference-side display waits on this phase's discovery-v3
scoping.

**Requirements**: OPS-01, OPS-02, DATA-08

**Already shipped, recorded here rather than re-planned**: public-first identities minting standalone
canonicals from an owner-approved artifact, container sources at book grain, per-daf Wikisource acquisition
taking locus identity from each page's own title, the cohort registry and contract v2, identification
eligibility (a reference can match without identifying), liturgy as a masking authority, and the locus
label fix (a citation no longer repeats the title the surface already shows).

**Success Criteria** (what must be TRUE):

  1. **The checked-in recipe rebuilds what production serves.** `_tmp/build_v42lit_sidecar.ps1` currently
     fails two of its own hash pins against disk and the loaders fail closed, so it **aborts before
     distillation — rollback-by-rebuild does not exist right now.** Rollback by atomic manifest repoint is
     unaffected and remains the primary path. Both hashes must be **derived at run time from the files**,
     never hand-copied; the recipe's own comment names the anti-pattern it fell into.
  2. Byte-identity is **not** the target and the record says why: `build_date = _now_iso()` is written into
     `meta` before the file hash is computed, which is what `frame_content_hash` exists to route around.
  3. **The Track-1 non-monotonicity is fixed or gated.** `REF_DF_CAP` is a raw **posting** cap that drops
     *all* postings of an over-cap code, which is how the V4.2 append deleted 103 live identifications. The
     loss is accepted by owner ruling (2026-08-19); what is not accepted is the next append repeating it
     blind — **every append diffs against live before ship**, and the diff is a gate, not a report.
  4. discovery-v3 is scoped to a decision: what the gen-2 evidence refresh must contain for Phase 136.1's
     reference-side display, and whether the liturgical-containment FP class (one work = 45% of CERT-01's
     measured error, which D-17 structurally cannot catch) is in or out.

**Plans**: TBD
**UI hint**: no

### Phase 149: Discovery Performance & Operability

**⚠ NEW 2026-08-20.** Source: `docs/specs/discovery-performance-situation-2026-08-20.md`.

**Goal**: The discovery surfaces' latency is accounted for rather than inferred, and the two fixes already
written for it are actually in production.

**Depends on**: nothing.

**Requirements**: OPS-03, PERF-01

**Already closed — do not re-do**: the citation-range P1 (a correlated `EXISTS` became an uncorrelated
`IN (SELECT ...)`, 10,478 ms → 97 ms measured on production, plus a second bug found by review where the
row expansion dropped the range — `4f6e31f4`), and `bench_discovery.py` off the deploy path after it hung
a deploy (`449e4039`).

**Success Criteria** (what must be TRUE):

  1. The findings page's ~2 s is **attributed**: ~0.9 s is accounted for and 1.1-3.0 s is not. The two
     Supabase calls and a browser trace are measured **first**, then a fix is chosen. Do not naively
     `asyncio.gather` the facets — the spec names that trap.
  2. The two fixes made 2026-08-19 are **deployed**: perf-watch was misattributing every slow request, and
     `/admin` blocked the event loop on every page build. Written but not shipped is not fixed.
  3. The second index candidate on `discovery_identification` (P3) is evaluated and either bundled into the
     next canonical rebuild or dropped with the reason recorded.
  4. Every budget or cap that fires is reported, and no page reports a capped total as an exact one.

**Plans**: TBD
**UI hint**: no


## Progress

**Execution Order — lanes, not a single numeric line.** *Rewritten 2026-08-20; the old line
("133 → 134 → 135 → 136 → 137 → 138 → 139") stopped describing this milestone some time ago —
139b's SC2 shipped before 138 exists, CERT-01 always ran as a parallel research track, and the
data work never queued behind the surfaces it feeds.*

| Lane | Phases | Sequencing |
|---|---|---|
| **Discovery surfaces** | 136.1 → 137 → 138 → 139a → 139b | In order. 139a before 139b for the release-gate half. |
| **Discovery data** | 148, 149 | Parallel, not queued behind 141-147. 148 gates Phase 136.1's reference-side display and carries a live operational risk (no rollback-by-rebuild today). |
| **Passage matching (desktop)** | 141 → 142 → 143 → 144 → 145 → 146 → (147) | In order, with 144's GO/NO-GO deciding whether 146 flips the default. Parallel to both discovery lanes. |
| **Bookkeeping** | 140 | Early — other phases plan against numbers it makes true. |

Numbers 148-149 sort after the passage track only because 141-147 were already claimed by in-flight
work when this re-map ran. **Read the lane, not the integer.**

> **RECONCILED 2026-08-16 (owner ruling: "ratify reality").** This table had drifted from the
> phase checkboxes above it and from `STATE.md`, three ways at once: it read 134 as `6/8, In
> Progress` while the same file's checkboxes showed 8/8 and its own prose said CLOSED; it read 136
> as `14/21` when the phase has 22 plans and 18 boxes were ticked, while `STATE.md` said 22 of 22.
> Bookkeeping was abandoned mid-phase during the beta push and never caught up with ~60 later
> `feat(136)`/`fix(136)` commits. The numbers below are reconciled against shipped code, not against
> whichever document was most optimistic.

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 133. Visual Atlas Preview (early quick win) | 6/6 | Complete   | 2026-07-21 |
| 134. Discovery Data Spine | 8/8 | Complete   | 2026-07-23 |
| 135. Precision Certificate & Confidence Bands | 9/9 | Complete   | 2026-07-28 |
| 136. Read Surfaces — Connections Panel & Work→Witnesses | 22/22 | Complete   | 2026-08-08 |
| 136.1 Evidence View, Work Pages & Catalogue Integration | 1/6 SC | In Progress|  |
| 137. Community Judgments — Hardening | 2/5 JUDGE | In Progress|  |
| 138. Leads Queue | 0/TBD | Not started | - |
| 139a. Release Hardening & REL-01 Gate Closure | 0/TBD | Not started | - |
| 139b. Atlas Drill-down & Homepage Capstone | SC2 shipped early | In Progress|  |
| 140. Milestone Bookkeeping & Debt Closure | 0/TBD | Not started | - |
| 141. Passage Matching — Algorithm Specification | 1/1 spec | Built, unmerged | 2026-08-19 |
| 142. Passage Matching — Spike & GO/NO-GO | measured | Built, unmerged | 2026-08-20 |
| 143. Passage Matching — Engine, Builder & Full Index | built | Built, unmerged | 2026-08-20 |
| 144. Passage Matching — Evaluation & Default Decision | tuning split only | In Progress| |
| 145. Passage Matching — Web Surface | 0/TBD | Not started | - |
| 146. Passage Matching — Desktop Surface & Release | 0/TBD | Not started | - |
| 147. Passage Mode on Regular Search (optional) | 0/TBD | Not started | - |
| 148. Reference Expansion & Bake Reproducibility | shipped + 4 SC open | In Progress| |
| 149. Discovery Performance & Operability | 2 of 5 problems closed | In Progress| |

**Why 133 is Complete and not 5/6:** `133-06`'s outstanding Tasks 3-4 were the human production
deploy. That deploy happened — `ATLAS_PREVIEW_ENABLED=1` on genizahsearch.com from 2026-07-21
(CHANGELOG.md, release commit `155758f0`), independently re-confirmed live on 2026-07-29
(`d725e14d`, phone-confirmed, live page + asset fetch). The checkbox was simply never flipped.

**Why 136 is Complete and not 18/22:** the four unticked plans' functionality is in production under
other commit messages — `136-13`'s rebuild and redeploy (the asset the site serves today),
`136-15`'s pure panel display model (`shared/discovery_panel_model.py`), `136-21`'s work-expansion
service work (`get_work_witnesses`, `build_work_expansion_count_sql` in
`shared/discovery_service.py`), and `136-22`'s launch-statistics reader (`a4ce0b31`, 2026-08-06).
The work shipped; the plan files were never marked. Counted as done on the code, not on the boxes.

**Why 136.1, 137 and 139b read as In Progress despite having no plan files:** scope belonging to each
was built during the beta push, outside any plan. See "Scope that shipped ahead of its phase" below.

## Scope that shipped ahead of its phase

*Recorded 2026-08-16. Between 2026-08-08 and 2026-08-14 the beta launch pulled real scope out of
three later phases. None of it was wrong to ship; all of it shipped without a plan file, and the
planning record never caught up. Retro-credit is granted where the built thing actually satisfies
the criterion, and withheld where it does not — partial credit is recorded as partial.*

| Phase | What shipped early | Credit |
|---|---|---|
| **136.1** | The excerpt / "View text match" view (`74702fa8` + follow-ups, 2026-08-13) | **PANEL-03 partially, by a different mechanism.** Excerpts are baked at build time, not offset-validated at render time as SC1 specifies, and the per-work `reuse_ok` licence gate SC2 requires is not visibly enforced in `scripts/bake_discovery_excerpts.py`. SC3 (`/work/{id}`), SC4 (`/catalog-browse`) and SC5 (title links) are unbuilt — no route exists for either. **1 of 6.** |
| **137** | Beta community identification reviews (`7268a7eb`, `b7618f5a`, 2026-08-13), default-ON | **JUDGE-03 and JUDGE-05 in full; JUDGE-02 partially** (RLS + explicit GRANTs + SECURITY DEFINER RPCs exist, but the only test is a substring assertion against the SQL file text, not a live role-matrix test). **JUDGE-01 and JUDGE-04 structurally not met** — see the Phase 137 entry. **2 of 5.** |
| **139b** | `/start` guided launchpad + homepage discovery entry points (`c413d5e9`, `fcb1eb8e`, 2026-08-12/13) | **SC2 in full, retro-credited.** Every route and teaser is gated on `atlas_preview_available()` / `discovery_available()` — the availability predicate, never a bare flag — so the clean-hide discipline the phase exists to enforce was respected by the code that jumped the queue. |
| **136** | The relation precedence matrix (`shared/discovery_relation_matrix.py`, 2026-08-12/13) and the launch-statistics reader (`a4ce0b31`) | Production code that governs what a claim is allowed to assert, with **no plan file at all**. Retro-plans owed — Phase 140. |

## Scope that shipped after the 2026-08-16 ratification

*Recorded 2026-08-20 by a second re-map. The 2026-08-16 ratification was thorough about everything
up to 2026-08-16 — and then roughly 85 commits landed in four days with no phase to belong to. The
ratification's own Phase 140 anticipated part of this ("decide the phase home for the
reference-expansion track"), which this table and Phases 148-149 now answer. As before, credit is
recorded where the built thing satisfies something, and withheld where it does not.*

| Track | What shipped | Home | Credit |
|---|---|---|---|
| **Reference expansion V4.1 + V4.2** | Public-first identities minting standalone canonicals from an owner-approved artifact (`4647c73a`), container sources at book grain (`94da69eb`), per-daf Wikisource acquisition taking locus identity from each page's own title (`88f31191`), the shared contract v2 + run identity + cohort registry (`c0cf1eb2`, `e3c9f6fb`), identification eligibility — a reference can match without identifying (`d843a1c8`), liturgy as a masking authority (`8f765bf7`) | **Phase 148** | Shipped. Four criteria remain open, the first of which is that **the checked-in recipe can no longer rebuild what production serves**. |
| **Track-1 matcher non-monotonicity** | The V4.2 append deleted 103 live identifications; `REF_DF_CAP` is a raw posting cap that drops *all* postings of an over-cap code (`8aa92453`) | **Phase 148** | Loss accepted by owner ruling 2026-08-19. The Codex counter-design is deferred; the gate that stops a blind repeat is not built. |
| **A provider name written into a masked-code column** | The build discarded 14 approved works without saying so (`32e2a8d7`, `990688ee`) | **Phase 148** | Fixed. Recorded because the silent-discard shape is the milestone's characteristic defect, now in its eighth instance. |
| **Locus / display strings** | A citation stops repeating the title the surface already shows (`7cde7b07`, `16b3ca7c`) | **Phase 148** | Fixed and live. |
| **Novelty gate on gemini-3.7-flash** | 69,732 verdicts, $9.43, 81.7% label agreement with 3.6, movement conservative (`b5ac780d`); bounded concurrency (`2c6cbb2c`) | **Phase 140** | Done — and it settles the `STATE.md` ↔ ROADMAP contradiction about whether a production novelty run ever happened. |
| **Citation-range outage** | A correlated `EXISTS` became an uncorrelated `IN (SELECT ...)`: 10,478 ms → 97 ms on production, plus a second bug found by review where the row expansion dropped the range (`4f6e31f4`) | **Phase 149** | **P1 CLOSED 2026-08-20**, deployed, owner-confirmed in a browser. |
| **Deploy readiness** | `bench_discovery.py` off the deploy path after it hung a deploy — real smoke, bounded statements, ssh keepalive (`449e4039`) | **Phase 149** | Closed. |
| **Masking sweep, surface half** | The two genuinely unswept REL-01 surfaces covered, gate proven able to fail (`ef373de2`, 21 tests) | **Phase 139a** | Closed — this was the one REL-01 item that was never waived. |
| **Passage-matching parallels search** | 20 commits on `claude/computed-id-parallels-search-a7c8fd`: the tracked algorithm spec, the build measurements, the full-corpus index, the release verifier, the evaluation core, and the first head-to-head numbers | **Phases 141-147** | Planned by the owner outside GSD and entered into the roadmap on 2026-08-20 rather than retro-credited, because it is live work rather than finished work. **Unmerged.** |

**One honest note about this table.** It exists because the same thing happened twice in two weeks:
work shipped, the record did not move, and the gap was found by a tool disagreeing with a document
rather than by anyone noticing. The 2026-08-16 entry called that out and it recurred anyway. Phase
140 is the standing answer; whether it works is measurable the next time this section needs writing.


## REL-01: the 2026-08-08 flag flip, recorded after the fact

**Owner waiver, 2026-08-16.** `DISCOVERY_ENABLED` was set to 1 in production on 2026-08-08. At that
moment this repository's written record said the opposite in two places: `STATE.md`'s *"The flag
must NOT be flipped yet"*, and the standing ruling *"NOTHING SHIPS BEFORE THE DISCOVERY GATE (owner,
2026-07-28) — Phase 136 BUILDS the read surfaces; Phase 139 flips them on."* The flip was an owner
decision and remains one; what was missing is that **nobody wrote it down**, so for eight days the
planning record contradicted the live site. This entry closes that gap rather than re-litigating the
decision.

Of the six items recorded as gating flag-on, **one is resolved** (the 58 NULL-genre works the
release verifier failed on — `2e9b409e`, 2026-08-13). The remaining five are **waived in writing,
with the surfaces staying live under an explicit beta label**:

| Gate item | Status | Reason for waiver |
|---|---|---|
| D-06b | Open | Disclosure detail; the shipped surfaces carry the D-06a disclosures and the qualitative methods page, which is the honesty load that matters to a reader. |
| CERT-02 tier-A-with-its-number copy | Open | Moot under the standing no-percentages ruling — the surfaces deliberately show no precision number, so the copy has nothing to apply to until that ruling changes. `band_precision` remains un-re-baked. |
| Correction / retraction policy | Open | Lost its carrying requirement when the curated-surface exception was declined on 2026-07-28; genuinely owed, and now homed in **Phase 139a** rather than left unassigned. |
| VIS-02 reconciliation | Open | A Phase 139 requirement by original registration; stays in 139a. The Phase 136 VIS-01 public projection — the one that actually gates what leaves the building — did ship and is enforced at build time. |
| Browser-check record | Open | The CI `findings-browser-check` job exists and runs; what is missing is the recorded attestation, not the check. |

**One item was NOT waived and was carried as a live obligation — now CLOSED (2026-08-19):** the
cross-surface masking sweep was last attested on 2026-08-05 (`136-19`). Three surfaces shipped after
it — the beta reviews, the excerpt view, and the homepage promotion. The artifact half was re-run on
2026-08-16 and the surface half on 2026-08-19; see the two notes below. One of the three
(the excerpt view) was already covered and the entry was stale in saying otherwise.

> **CLOSED, 2026-08-19 — the SURFACE half, and the list was stale by one.** The three surfaces
> were checked against the sweep rather than against this entry, and the excerpt view turned out to
> be **already swept**: `test_discovery_masking_sweep.py::_excerpt_loader` drives
> `render_excerpt_disclosure` through six states (direct / reprojected / nowork / empty / raise /
> busy), dated with the surface on 2026-08-13, and the line-coverage gate over `findings_rows.py`
> keeps it driven. The other two were genuinely unswept and are now covered by
> **`tests/render_smoke/test_discovery_masking_sweep_reviews_home.py`** (21 tests, green):
>
> * **Beta identification reviews** — three egress classes, two of which no existing sweep modelled:
>   a **mailto link target** carrying `identification_id` + `sidecar_version` (the four-class sweep's
>   copy/export inventory asserts an absence over four modules and this was not one of them, so the
>   absence was true of the scanned set and false of the product), and the **outbound Supabase
>   write**, captured by intercepting the storage boundary and driving the dialog to actually submit.
>   Rendered in both languages, dialog opened (it is built lazily, so an unopened action paints none
>   of the form), plus `aria-label`/tooltip text, which is where the published-review provenance and
>   each verdict live.
> * **Homepage discovery promotion** — all four gated entry points by MARKER (the promotion navigates
>   from a click handler, not an `href`, and its title goes through `tr()`, so route- and
>   English-title assertions were both wrong about the capture rather than the page), with the
>   DEFERRED count driven. The stronger finding: `home.py` reads the artifact in exactly one place
>   and renders exactly one value from it, `meta.work_total`, behind
>   `isinstance(total, int) and total > 0` — so **no artifact string can reach that surface**, which
>   is asserted directly rather than inferred from a clean scan.
>
> **Mutation-proven at two levels**, per the standing rule that a gate must be watched failing: at
> the DATA level a seeded needle enters through the same dict shape the panel hands the component and
> the shipped renderer puts it on screen — each class's scan fires, *and so does the exact
> `--strict --scan-repo --scan-asset` invocation the gate runs; and at the CODE level an unreached
> renderer added to `identification_review.py` made the coverage derivation fail by name
> (`['_mutation_probe_renderer']`), then was reverted. Three defects in the first draft were found by
> the gate rather than by reading it: an empty outbound-write capture (the dialog needs the loop
> yielded to while the client is still alive), a `pytest.skip` substring check that failed on its own
> forbidden-names list (fixed by reusing the sweep's AST walk — whose docstring predicts exactly that
> mistake), and an unset-pattern-file test that passed for the wrong reason because on Windows
> `Path('nul').parent` resolves to the repository and finds the real pattern file.

> **Partial progress, 2026-08-16 — the ARTIFACT half only.** Recording the deployed V4 build
> (`528f6d36…`) revealed that the obligation was in worse shape than written: the sweep's scan
> record named `e9365edc…`, so it was attesting an artifact production had stopped serving on
> 2026-08-15, and the manifests said the same. The four-mode scan was re-run over the real deployed
> artifact — clean, and shown non-vacuous against `discovery_identification`, a table the 2026-08-05
> walk never reached. **The three unswept SURFACES remain unswept**; Phase 139a's first task is
> unchanged in scope. What changed is that the artifact it will attest is now the right one.

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
