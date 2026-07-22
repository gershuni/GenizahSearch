# Requirements: GenizahSearch — Milestone v9.0.0 Discovery

**Defined:** 2026-07-20 (Codex 6-round convergence + full owner requirement walk)
**Core Value:** Researchers can find what they need in the Genizah corpus
**Milestone:** v9.0.0 Discovery — Same-Work Identification & Connection Atlas (web-only)

## v9.0.0 Requirements

### Claim Model & Data Spine

- [x] **DATA-01**: A canonical claim model defines TWO stored claim families: (a) **work–witness claims** (`sys_id`, `work_id`, supporting page IDs, page→witness aggregation rules) and (b) **MS–MS relation claims** (`sys_id_a` < `sys_id_b` canonical ordering, direct-alignment basis) carrying CHILD alignment records — (`page_id_a`, `page_id_b`, per-side evidence spans), queryable by page so PANEL-02's "pages related to this page" view is served directly — while the stable parent claim ID remains the voting target. `claim_id` is a deterministic content key namespaced by family — SHA-256 over the canonical UTF-8 serialization `{claim_family}|{sys_id}|{work_id}|{claim_type}` (work–witness) / `{claim_family}|{sys_id_a}|{sys_id_b}|{claim_type}` (MS–MS, `sys_id_a` < `sys_id_b`) — hash algorithm + serialization frozen in the schema doc, stable across rebuilds, uniqueness constraint on the key fields. Each stored claim carries a `claim_type` from a fixed relation vocabulary (direct witness / quotes-this-work / textual parallel / direct text overlap — the semantic mapping from engine flank-class is FROZEN before distillation; only bilingual display wording is deferred to the UX discuss-phase), a sidecar version, and exactly one band assigned at distillation. A manuscript — even a single page — can legitimately carry MULTIPLE work–witness claims (separate codicological parts; interleaved works such as Talmud+Rashi+Tosafot or Mikra+Targum+Tafsir; one main work plus quoted works): deduplication and band precedence operate only within a single claim key, never across different works. "Related manuscripts via shared work" lists are deterministic projections of work–witness claims (never stored as separate claims); a projection displays the WEAKER of the two claims' bands. Judgments target stored claims of either family. Text-relations are never presented AS physical joins (a same-work identification is not join evidence); known physical joins feed DATA-10 witness-unit aggregation instead.
  - **⟨SUPERSEDED 2026-07-21 — Phase-134 CONTRACT CORRECTION (`134-CONTEXT.md` C-1..C-9); pending owner ratification⟩** The two-stored-claim-families framing above (separate work–witness + MS–MS claim tables; sys-level MS–MS claim IDs; `claim_id` hashing `claim_type`; the four-value relation vocabulary; "exactly one band assigned at distillation") is REPLACED by a **two-table** model: `discovery_claim` PK `(page_id, work_id)` — the REAL page, NO physical-MS claim-key collapse — 1-to-many `discovery_evidence` (PK `evidence_id`, with an `evidence_kind ∈ {witness, shared_text}` discriminator). `claim_type ∈ {direct_witness, quotes_this_work, shared_text}` is DERIVED from the evidence (`textual_parallel`/`direct_text_overlap` DROPPED into `shared_text`); `claim_id = SHA-256` over the STABLE key `(namespace, page_id, work_id)` — NOT `claim_type`. Recall-widening is an orthogonal `evidence_source ∈ {track1_direct, propagated}` axis (a propagated witness is HOW we know, not a separate claim). "MS–MS relation" becomes `shared_text` evidence still anchored to a catalogued `works.work_id` (anchorless MS-MS residue EXCLUDED per the frozen OQ2). A claim MAY carry MULTIPLE evidence rows with DIFFERENT bands; ONE deterministic `display_evidence_id` is selected for presentation. Physical-MS grouping is a DATA-10 unit×work PROJECTION via `witness_units`, never a claim collapse.
- [x] **DATA-02**: The band model is frozen in the requirements: allowed band set = `expert_verified` (R-A) > `tier_a` (algorithmic) > `screening_rb` (R-B) > `screening_canon` (R-CANON); the SAME claim key appearing in multiple frames takes the highest band by that precedence (exactly-one-band per claim key, post-precedence) — distinct works carried by the same manuscript are distinct claims and all ship with their own bands; MS–MS relation claims take the band of the frame that produced them under the same precedence. The distillation phase must emit a version-controlled **frozen-frame artifact** (`discovery-frames.md`: per-band deduplicated claim counts, page→claim dedup formula, overlap-resolution counts, frame content hash) BEFORE certificate cards are drawn; work IDs are opaque product IDs (no raw research IDs); schema is allowlisted with uniqueness + referential-integrity checks
  - **⟨SUPERSEDED 2026-07-21 — Phase-134 CONTRACT CORRECTION (`134-CONTEXT.md` C-4/C-5/C-7); pending owner ratification⟩** The band model is per-`evidence_source`, NOT a single global precedence with "exactly-one-band per claim key": **track1_direct** → `{expert_verified, tier_a, screening_rb, screening_canon}`; **propagated** → `{corroborated, weak, not_evaluated}` (`not_evaluated` is a real enum for `shared_text`). Bands are assigned per evidence row; the "exactly-one-band-per-claim-key" invariant is DROPPED (a claim legitimately holds multiple evidence rows/bands, resolved to one `display_evidence_id`). The held-out **0.926 [0.875,0.968]** attaches at the **propagated witness COLLECTION** level (corroborated ∪ weak; 200-card draw = 90 corroborated + 110 weak, 176/190), NOT to `corroborated` alone; `corroborated` ranks above `weak` structurally but neither carries a separate manufactured band interval (both PROVISIONAL). The word "certified" is PROHIBITED until the independent audit gate (registry-gated `audit_status`). The frozen-frame artifact (`discovery-frames.md`) + the `band_precision` table (scope-discriminated — the 0.926 stored ONCE at `scope='collection'`) remain phase exit criteria.
- [x] **DATA-03**: Provenance masking is structural: the shipped sidecar cannot contain reference text, sigla, or provenance columns; claim evidence is stored ONLY as offsets into our own HTR text with the HTR snapshot hash recorded, offsets validated at render time, failing closed (no evidence shown) on text-version drift
- [x] **DATA-04**: The shipped work set is a curated SUBSET of the research works (all ~4,093 need not ship; the v9.0 launch may restrict to works sourced from open corpora — Sefaria + the JA corpus — deferring M-source-derived works; the selection is recorded at distillation); every shipped work carries a human-reviewed neutral title with NO fallback to research titles, and all displayable fields (title, author, genre) pass the same review
- [x] **DATA-05**: A permanent CI leak-vector guard scans the shipped sidecar (schema + all cell values) and every product surface — rendered pages, clipboard/copy output, atlas assets, JSON payloads, SEO/JSON-LD, sitemap, error messages, Supabase claim payloads — for provenance leaks; the guard also scans COMMITTED repo content (code, docs, planning artifacts — git index/HEAD, not gitignored research trees): the restricted source appears in committed material only under the internal codename M-source, never by name; the guard's first run includes the one-time cleanup verification (research trees untracked + gitignored — done 2026-07-20; no tracked occurrence remains) and any legacy uncommitted prototype strings (e.g., the current `genizah_translations.py` working-tree additions) must be scrubbed before they may be committed
- [x] **DATA-06**: All web access goes through one async DiscoveryService chokepoint with per-query timeouts, bounded concurrency with defined user-facing overload behavior ("temporarily unavailable", no hang), indexed bounded queries, LRU caching on the browse-enrichment path, and server-side pagination everywhere lists can grow
- [x] **DATA-07**: A feature flag gates all discovery surfaces (pages, panels, homepage band, sitemap/SEO discovery); when off or when the sidecar is absent, every surface hides cleanly with zero errors
- [x] **DATA-08**: The sidecar release contract includes schema version, source-DB hash, build date, data-as-of date, expected row counts, `PRAGMA integrity_check`, and a disk budget (≤ 300 MB); the sidecar filename is schema-versioned so old code never opens an incompatible snapshot; a corrupt/incompatible sidecar is rejected at startup while the rest of the app stays fully available; deploy = temp upload → verify → atomic rename → code deploy, rollback documented, full rebuild recipe reproducible
- [x] **DATA-10**: A **codicological witness unit** is defined over stored claims as a UNION of sys_ids: sys_ids merge into one unit via (a) catalogued codicological parts (e.g., an Oxford part ID grouping several sys_ids into one MS) and (b) known physical join groups (PGP/FJMS/user; joins whose basis is only "same scribe" do NOT merge); witness lists and counts group by unit (a joined/part-grouped codex counts as ONE witness) while claims stay stored per sys_id so claim IDs remain stable as join knowledge evolves; `unit_id` is deterministic per snapshot (hash over the sorted member sys_ids) and membership is recorded in the sidecar; the unit×work projection rule is fixed: a unit's row on a work page displays the HIGHEST band among its members' claims for that work (band filters act on that displayed band), member claims are visible on expansion, and same-unit members are suppressed from "other manuscripts of ⟨work⟩" lists (a joined fragment is not "another manuscript")
- [ ] **PERF-01**: A versioned acceptance-budget artifact (`discovery-budgets.md`) is a phase exit criterion, with initial numeric caps: browse-enrichment added latency p95 ≤ 150 ms; atlas drill-down response ≤ 1,500 nodes / 6,000 edges / ≤ 2 MB, server p95 ≤ 3 s, timeout 10 s; work/leads pages ≤ 200 rows per page, response ≤ 500 KB, server p95 ≤ 1.5 s, timeout 5 s; browse-enrichment query timeout 2 s; discovery adds ≤ 250 MB RSS on the prod box — measured before release, tunable only by versioning the artifact

### Bands & Certification

- [ ] **BAND-01**: Every displayed claim carries its confidence-band label inseparably through every UI surface and every serializer/copy path shipped in v9 (v9 adds no new generalized export features)
- [ ] **BAND-02**: Band labels and precision copy are data-driven from sidecar metadata — R-A reads "expert-verified (independent audit pending)" until the audit passes; certification-status changes flip copy with no code change
- [ ] **BAND-03**: High-confidence bands are shown by default; screening bands appear only behind an explicit toggle worded as "show more possible identifications" (probability framing, not "uncertified"; HE wording fixed in the UX discuss-phase); screening rows are labeled "possible identification" with the measured screening precision reachable via the band tooltip → BAND-05 methods page
- [ ] **BAND-04**: A recall-honesty disclaimer appears on all discovery surfaces (sense: "no identification shown ≠ none exists"; per-surface wording varies and is fixed at application time in the UX discuss-phase)
- [ ] **BAND-05**: A bilingual methods/confidence page documents each band: population, unit of measurement, sample size, strata, weighted estimate, confidence interval, measurement date, grader + audit status, immutable report identifier; band tooltips link to it
- [ ] **CERT-01**: A pre-registered stratified tier-A precision measurement runs inside the milestone with a written protocol fixing: estimand + unit (deduplicated manuscript–work claim), the frozen eligible-frame hash (frame freezes AFTER distillation stabilizes, BEFORE cards are drawn), mutually exclusive strata + weights, sampling seed, blindness procedure, treatment of existing gold, exclusion/indeterminate rules, confidence-interval method, pass/fail gates, release copy for every possible outcome, and the FAILURE ACTION: if tier-A fails its pre-registered floor, the `tier_a` band rebands to screening (moves behind the BAND-03 toggle with not-certified labeling, homepage/promotion copy adjusts per the pre-registered outcome branch) — release proceeds with the certified bands as the default view; it is not blocked
- [ ] **CERT-02**: Each band displays ONLY a number measured in that band's own unit, always paired with its status: tier-A shows no precision number until its CERT-01 certificate lands; R-A shows 0.889 with "audit pending" status; R-B/R-CANON show screening values with "not certified"; unit + status are those recorded on the BAND-05 methods page

### MS Connections Panel

- [ ] **PANEL-01**: The browse page offers a "Computed identifications" / "זיהויים מחושבים" button (enrichment section on the existing staleness-guarded path) opening the manuscript's list of potential identifications, each with band label and inline voting — ✓ / ? / ✗ + optional note — wired to JUDGE-01
- [ ] **PANEL-02**: The panel shows two relation types, both with band labels + click-through: (1) "Other manuscripts of ⟨work⟩" — MS-to-MS with title info, derived from shared work–witness claims (displays the weaker band of the pair; shows each side's own relation type when they differ); (2) "Pages in other manuscripts related to this page" — direct page-to-page alignment claims presented in the context of the currently viewed page
- [ ] **PANEL-03**: An on-demand evidence view shows the supporting span(s) from OUR manuscript text with match stats; for MS–MS relation claims each side shows its own span; reference text is never rendered

### Work → Witnesses

- [ ] **WORK-01**: A `/work/{id}` page lists all identified carriers in the dated snapshot grouped by codicological witness unit (DATA-10: joined fragments appear as one witness) with per-witness band labels, server-side pagination (PERF-01 page cap), deterministic sorting, visible counts, and band + library filters with defined semantics (filters compose as AND; empty filter = all currently ENABLED bands — screening rows appear on any surface only while the BAND-03 toggle is on)
- [ ] **WORK-02**: Works are findable by neutral title with defined bilingual title normalization and alias/duplicate-title handling, linked from panel + atlas; the identification-browse surface reuses/extends the existing `/catalog` Browse-by-Identification structure (domain/author/work facets) rather than inventing a new browse paradigm (exact shape fixed in the UX discuss-phase)

### Atlas & Homepage

- [x] **ATLAS-01**: An offline-precomputed, canon-masked aggregated corpus overview ships as a static asset (layout never computed at request time); OWNER PRIORITY (2026-07-20): this is the milestone's FIRST deployable artifact — an early standalone beta page, shipped before the claim surfaces, under the REL-01 atlas-preview exception
- [ ] **ATLAS-02**: A server-bounded drill-down explorer serves capped neighborhoods (PERF-01 node/edge/byte caps, single-hop server-side expansion); the client never loads the full edge set; the graph's primary object (works vs manuscripts vs clusters) is fixed in the UX discuss-phase BEFORE sidecar/layout design
- [ ] **ATLAS-03**: Homepage promotion is a CLS-safe static band (no live suggestions/graph in v9), enabled only via the REL-01 gates. ATLAS-PREVIEW EXCEPTION (owner, 2026-07-20, Phase 133 discuss): a small claim-free homepage TEASER card linking to the Phase 133 `/atlas` beta may ship early — a CLS-safe static card with NO claim-level statements, gated by the dedicated atlas-preview flag, passing the DATA-05 masking scan + i18n/RTL basics, and `noindex` until the full REL-01 gate; the full homepage discovery band (promoting claims) still waits for Phase 139

### Leads Queue

- [ ] **LEADS-01**: A screening-leads page presents the R-B lane as the lead queue, labeled with the BAND-03 "possible identification" framing, with pagination, deterministic sort, and filters; the canon lane is separately caveated (incl. the known Targum-confusion class), not merely ranked lower
- [ ] **LEADS-02**: Reviewing a lead uses the same JUDGE-01 voting as every other surface: main verdict ✓ / ? / ✗ + note, plus an OPTIONAL advanced typed refinement drawn from a fixed vocabulary (A cites B / B cites A / both cite a common source / compilation / another language or edition / …— finalized in the UX discuss-phase, informed by the E1 reason codes)

### Community Judgments

- [ ] **JUDGE-01**: Logged-in users vote ✓ / ? / ✗ + optional note (+ optional typed refinement per LEADS-02) on a stored claim; the UX is ONE editable judgment per user per claim; storage underneath is DB-enforced append-only (changing a vote inserts a superseding event — audit trail for future certification, visible-vandalism protection, per-sidecar-version context); each judgment persists claim id, claim type, sidecar version, and the band shown at judgment time
- [ ] **JUDGE-02**: The Supabase schema ships with RLS + explicit GRANTs (2026-05-30 rule) and role-matrix tests; note free-text is private by DEFAULT (owner + moderators), implemented by the simplest safe means (e.g., RLS on the notes column/table) — not a place to spend engineering effort beyond the default posture
- [ ] **JUDGE-03**: Community judgments never affect band assignment, precision copy, ranking, or certified styling — structurally a separate display layer
- [ ] **JUDGE-04**: Aggregate judgment signals (incl. explicit disagreement counts) are visible on claims; free-text annotations are captured but NEVER publicly rendered in v9 (public rendering is FUT-06); moderation actions (admin hide, spam mark) are append-only moderation events in a separate table, and hidden/spam judgments are excluded from aggregates
- [ ] **JUDGE-05**: Judgment writes are abuse-resistant: per-user rate limits, annotation length limits, input escaping/sanitization

### Cross-cutting

- [ ] **I18N-01**: All discovery surfaces are fully translated EN/HE with HE-mode render-smoke tests
- [ ] **I18N-02**: RTL/bidi correctness across all discovery surfaces including graph/atlas labels
- [ ] **A11Y-01**: Confidence is never signaled by color alone; graph information has a textual/table equivalent; keyboard navigation and screen-reader labels on interactive discovery controls
- [ ] **A11Y-02**: Contrast checks pass on all new discovery text, controls, focus indicators, band labels/badges, atlas marks/labels, and textual equivalents; atlas transitions/loading/highlight animations honor reduced-motion preferences
- [ ] **SEO-01**: `/work/{id}` + atlas pages get canonical URLs, EN/HE hreflang, titles/descriptions, sitemap inclusion; `/leads` and uncertified-toggle states are `noindex`; JSON-LD carries neutral work metadata only (no algorithmic witness associations); all SEO outputs inside the DATA-05 masking gate; activation gated by REL-01
- [ ] **OBS-01**: Privacy-allowlisted PostHog product events (panel impressions, lead-toggle use, evidence opens, work/atlas navigation, judgment completion) with an explicit denylist: never work titles, manuscript text, shelfmarks, raw research IDs, or annotation/free text in any event property
- [ ] **OBS-02**: Operational metrics cover query timeouts, result truncation, sidecar unavailable/incompatible events, atlas payload sizes, and judgment rate-limit/moderation activity
- [ ] **REL-01**: Release order is gated: claim semantics + masked schema → validated title map + sidecar + frozen-frame artifact → certificate card draw → read surfaces (panel/work) → Supabase migration + security smoke → judgment UI → leads → bounded atlas → public promotion; the feature flag, sitemap/SEO discovery, and homepage band stay OFF until: the CERT-01 measurement is graded to completion, the BAND-05 immutable methods report is published, the CERT-02 outcome-specific copy is applied (tier-A goes public WITH its measured number), and the masking (DATA-05), RTL (I18N-02), accessibility (A11Y-01/02), performance (PERF-01), and deployment (DATA-08) checks pass. ATLAS-PREVIEW EXCEPTION (owner, 2026-07-20): the static atlas overview (ATLAS-01) may deploy EARLY as a standalone beta page, before the certificate and the claim surfaces, PROVIDED it displays no claim-level statements (no work–witness identifications, no bands, no precision numbers — cluster/shelfmark-level visualization only), region/cluster labels come only from our own catalogue titles (libraries.csv — masking-safe catalogue metadata, distinct from the DATA-04 reviewed neutral WORK titles), reviewed neutral titles, or are omitted, an asset-level masking scan passes, its PERF/i18n basics hold, and it sits behind a DEDICATED atlas-preview feature flag distinct from the main discovery flag (the main discovery flag, the full homepage discovery band, and sitemap/SEO discovery stay OFF until the full gate). The exception EXTENDS (Phase 133 discuss, owner 2026-07-20) to a claim-free homepage TEASER card linking to the /atlas beta: same claim-free + masking + i18n/RTL conditions, CLS-safe, gated by the atlas-preview flag, and noindex until the full REL-01 gate; the full homepage discovery band (promoting claims) still waits for Phase 139

## Future Requirements (deferred)

- **FUT-01**: Text-reuse engine as an alternative/main backend for `/parallels` (desktop: composition) search (user, 2026-07-19)
- **FUT-02**: Public API endpoints for discovery (band-labeled, masked) + skill parity
- **FUT-03**: Desktop parity for the discovery module
- **FUT-04**: Refresh pipeline/cadence for the discovery snapshot
- **FUT-05**: Live-interactive full-corpus WebGL atlas (sigma.js escalation) + multi-hop exploration
- **FUT-06**: Public rendering of moderated free-text annotations
- **FUT-07**: R-B / gen-2-at-scale certification round; R-A independent audit completion (external gate)
- **FUT-08**: New generalized discovery exports (xlsx/CSV)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Crowd auto-certification (votes promote claims into certified bands) | Would pollute the certified bands; judgments are a separate additive layer (Codex R1 #7, FEATURES anti-pattern) |
| Rendering reference-corpus (M-source) text anywhere | Hard provenance-masking constraint |
| Public display of judgment free-text in v9 | Moderation not yet operational; FUT-06 |
| Full-corpus force-directed client-side graph | 89% liturgical giant component: meaningless + browser-melting |
| Live recomputation of identifications | Snapshot ship; rebuild is offline |
| Stemma/variant collation views | Different product; far beyond band-labeled identification |
| Canon-lane certification (round-4 Bible stratum) | User decision 2026-07-19: low scholarly value |
| Per-row decimal probabilities in UI | False precision for lay users; bands only |

## Traceability

Every v9.0.0 requirement maps to exactly one phase. Phases continue from the previous milestone (last phase 132), starting at 133. Owner revision 2026-07-20: Phase 133 = Visual Atlas Preview (ATLAS-01, the milestone's first deployable artifact under the REL-01 atlas-preview exception); the remaining phases run 134-139.

| Requirement | Phase | Status |
|-------------|-------|--------|
| ATLAS-01 | Phase 133 | Complete |
| DATA-01 | Phase 134 | Complete |
| DATA-02 | Phase 134 | Complete |
| DATA-03 | Phase 134 | Complete |
| DATA-04 | Phase 134 | Complete |
| DATA-05 | Phase 134 | Complete |
| DATA-06 | Phase 134 | Complete |
| DATA-07 | Phase 134 | Complete |
| DATA-08 | Phase 134 | Complete |
| DATA-10 | Phase 134 | Complete |
| PERF-01 | Phase 134 | Pending |
| BAND-01 | Phase 135 | Pending |
| BAND-02 | Phase 135 | Pending |
| BAND-03 | Phase 135 | Pending |
| BAND-04 | Phase 135 | Pending |
| BAND-05 | Phase 135 | Pending |
| CERT-01 | Phase 135 | Pending |
| CERT-02 | Phase 135 | Pending |
| PANEL-01 | Phase 136 | Pending |
| PANEL-02 | Phase 136 | Pending |
| PANEL-03 | Phase 136 | Pending |
| WORK-01 | Phase 136 | Pending |
| WORK-02 | Phase 136 | Pending |
| JUDGE-01 | Phase 137 | Pending |
| JUDGE-02 | Phase 137 | Pending |
| JUDGE-03 | Phase 137 | Pending |
| JUDGE-04 | Phase 137 | Pending |
| JUDGE-05 | Phase 137 | Pending |
| LEADS-01 | Phase 138 | Pending |
| LEADS-02 | Phase 138 | Pending |
| ATLAS-02 | Phase 139 | Pending |
| ATLAS-03 | Phase 139 | Pending |
| SEO-01 | Phase 139 | Pending |
| I18N-01 | Phase 139 | Pending |
| I18N-02 | Phase 139 | Pending |
| A11Y-01 | Phase 139 | Pending |
| A11Y-02 | Phase 139 | Pending |
| OBS-01 | Phase 139 | Pending |
| OBS-02 | Phase 139 | Pending |
| REL-01 | Phase 139 | Pending |

**Coverage:** 40 / 40 v9.0.0 requirements mapped ✓ (no orphans, no duplicates). Note: the requirement set skips DATA-09 by design — the Claim Model & Data Spine block runs DATA-01..08 then DATA-10.

**Cross-cutting note:** I18N-01/02, A11Y-01/02, SEO-01, OBS-01/02, and REL-01 are homed in Phase 139 (the release-hardening capstone) because their completion criterion is a comprehensive cross-surface gate and several explicitly reference graph/atlas labels. Translations, RTL, and accessibility are nonetheless BUILT INTO every UI surface (Phases 133, 136–139) from line one per house convention — Phase 133's atlas-preview page carries its own PERF/i18n basics per the REL-01 atlas-preview exception; Phase 139 owns their final verification. Likewise PERF-01, DATA-05, and DATA-08 are delivered in Phase 134 but re-verified at the REL-01 gate.

---
*Requirements defined: 2026-07-19 (draft v5 after Codex rounds 1–4; R1 critique at .planning/research/REQUIREMENTS-CODEX-CRITIQUE.md)*
*Traceability populated: 2026-07-20 (roadmapper — Phases 133-139; owner atlas-preview revision applied same day)*
*Revised 2026-07-20 (Phase 133 discuss): ATLAS-03 + REL-01 ATLAS-PREVIEW exception widened to include a claim-free homepage teaser + a dedicated atlas-preview flag; catalogue-title region/cluster labels clarified as allowed in the preview. See .planning/phases/133-visual-atlas-preview-early-quick-win/133-CONTEXT.md (D-05, D-13, D-16).*
