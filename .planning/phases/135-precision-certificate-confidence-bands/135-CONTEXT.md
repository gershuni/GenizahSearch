# Phase 135: Precision Certificate & Confidence Bands - Context

**Gathered:** 2026-07-23
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 135 makes the four-band confidence model **honestly displayable** (data-driven labels + status copy + a bilingual methods page) and **measures the big unmeasured `tier_a` band** with a pre-registered protocol — preceded by a **leadoff data re-distill (discovery v1 → v2)** so the band/precision/display contract binds against corrected data, not the defective v1.

The phase runs as **two tracks**:
- **Track A (census-independent — starts now):** the band-label display/values module, the methods page (Help-page section), and the *written* pre-registered CERT-01 protocol.
- **Track B (was census-blocked — now UNBLOCKED, census delivered mid-discussion):** the v2 re-distill (Codex-reviewed) → owner ratification of a relation batch → freeze the v2 frame → draw ~200–250 tier_a cards → deploy the v2 asset ONCE (human-approved) → grading **starts**.

**NOT in this phase:** the connections panel and `/work/{id}` claim-rendering surfaces (Phase 136), community judgments (137), leads (138), atlas drill-down + homepage band + REL-01 flip (139). SC#1 ("every displayed/serialized claim carries its band inseparably") is architecturally established here via a shared band component + values module and **enforced from Phase 136 onward** — Phase 135 itself renders no claim lists, only the methods page.

**Phase closes when grading has STARTED** (Track A live behind the flag, protocol pre-registered, v2 baked + deployed, cards drawn, grading begun). Owner grading completion + the published certificate run in parallel through Phases 136–138 and gate only the Phase 139 public-promotion flip (REL-01).
</domain>

<decisions>
## Implementation Decisions

### Scope & Sequencing
- **D-01:** Split into Track A (census-independent, now) + Track B (fires on census). Track A ships independently behind the discovery flag.
- **D-02:** "Done" = grading **STARTED**, not completed. Grading + published certificate finish in parallel (136–138) and gate only the Phase 139 flip.
- **D-03:** The census is the accepted **critical path** for v2 *and* Phase 136 (read surfaces need corrected v2 data). No partial-census fallback. **RESOLVED mid-discussion — census delivered** (see Census Handoff below); the blocker is lifted.
- **D-04:** The v2 production deploy is a **human-approved checkpoint**, asset-first (per `docs/specs/discovery-deploy.md` / the 133-06 pattern), deployed **ONCE** (never v1-then-v2).

### CERT-01 Measurement Design
- **D-05:** **Estimand unit = (page, work).** This deliberately refines the CERT-01 requirement's "deduplicated manuscript–work claim" wording. Owner rationale: a manuscript carries many *different* identifications across its pages (citation-heavy MSS especially), so a whole-MS verdict blurs *which* claim is judged; page-level is sharper. **Multi-register preserved** — several identifications can be TRUE for one page (Bible + Targum + Tafsir); each (page, work) claim is judged on its own, never "one work per page." **Confidence interval clustered by physical MS** (pages within a MS aren't independent — E1 precedent).
- **D-06:** **Posture = "expert-measured · independent audit pending"** — parity with R-A's 0.889 today. Individual rows stay "unreviewed · algorithmic estimate" (grading a sample does not verify ~238K rows). The word **"certified" is never used** (discovery-band-labels-v1.md Rule 1). The independent audit is the deferred FUT-07 gate.
- **D-07:** **Pass gate = Strict — lower confidence bound ≥ 0.85.** Owner accepts the demotion risk (the raw tier-A audit flagged ~39% "suspect"; v2 coverage routing removes the cov<0.45 tail, but tier_a could still fail 0.85). **FAIL action (pre-registered) = reband `tier_a` → screening** (moves behind the BAND-03 toggle, not-certified labeling); release proceeds with the remaining certified bands as default, per CERT-01.
- **D-08:** **Strata = source corpus (Sefaria / Judeo-Arabic / M-source) × coverage band (high ≥0.60 / med 0.45–0.60)**, weighted to the shipped tier_a population. Both are reliably known pre-grading and strongly predict the outcome. **Work-category is a diagnostic breakdown only, NOT a weighting stratum** (genre is unreliable — deliberately dropped in 134-07). CERT-01 also carries a **later-quotes-earlier quotation-FP diagnostic family** (from the D-17 date rule) so the quotation problem is visible in the measurement.
- **D-09:** **Reuse the E1/Q2 adjudication harness as-is** (`same_work_spike/probe/scripts/e1_*.py`), pointed at the v2 shipped tier_a frame. Standard kit retained: catalogue-blind with logged reveals, gold-card repeatability gate, **physical-MS-clustered bootstrap** for the CI, a pre-outcome OC (operating-characteristics) table, one **freeze manifest** (RNG seed + artifact fingerprints + all cutoffs), ~200–250 cards + a pre-reserved confirmation draw. The measured number flows into the sidecar `band_precision` table (BAND-02 reads it — already the mechanism from 134-07).

### Band Display & Wordings
- **D-10:** The methods/confidence page (BAND-05) = a **gated, bilingual SECTION inside the existing Help page** (not a new route), with **per-band anchors** for tooltip deep-links (e.g. `Help#confidence-tier_a`). `noindex` until the REL-01 gate. Immutable report identifier = a **content-hashed, versioned string** (e.g. `cert-tier_a-<hash>`), consistent with the frozen frames/budgets discipline. Documents per band: population, unit, sample size, strata, weighted estimate, CI, measurement date, grader + audit status, report id.
- **D-11:** "Show more" toggle wording = EN **"Show more possible matches"** / HE **"הצג התאמות אפשריות נוספות"** (HE candidate — tunable at 136 application time). Behind it: `screening_rb` / `screening_canon` + `routing_status='review_only'` + shadowed rows (per discovery-band-labels-v1.md §4). NOTE: this reconciles the two spec phrasings — BAND-03's user-facing "possible-match" framing wins over §4's technical "show screening/algorithmic matches."
- **D-12:** Recall-honesty disclaimer canonical base sentence (BAND-04) = EN **"Not exhaustive — more identifications may exist."** / HE **"אינו ממצה — ייתכנו זיהויים נוספים."** Per-surface variants are tuned where each surface is built (136+).

### Census / Canonical-Merge Handoff (RESOLVED mid-discussion, 2026-07-23)
- **D-13:** The twin census + canonical-merge decisions are **delivered as an owner-ratified handoff artifact**: `same_work_spike/probe/rsource/data/v2_canonical_merges.json` (+ `results/v2_canonical_merges.md`), `owner_ratified: true`. Contents (counts only): **16 merges** (each → one `canonical_work_id` = the Sefaria id; includes Rabbeinu Chananel ×12 tractates — the large one), **1 ratified `part_of` relation** (Haggadah embedded in MT Sefer Zmanim — not a merge), **1 contested** (Hai / RCh Shabbat), **174 `provisional_relations`** (unratified), **8 `residual_direct`**, and a **`dropped_by_135` list (n=1)** = the w001239 drop handed to 135. This **SUPERSEDES** the v2-bake-plan draft's "7 merges + 3 relations" → the plan MUST be updated (and re-Codex-reviewed) to consume the full artifact. **MASKING-SENSITIVE:** the artifact carries `title_msource` fields — the build *consumes* it, nothing renders it, and it never enters a committed doc.
- **D-14:** **RCh-Shabbat three-way resolution** (w000451 Hai Gaon / w000452 M-source RCh Shabbat / w001239 Sefaria RCh Shabbat, all the same text): the merge table auto-resolves the interaction — because w000452↔w001239 is a merge and 135 drops w001239, that group's canonical **flips from the Sefaria id to the M-source id w000452** (the documented exception to "canonical = Sefaria rep"). Flagged in both the merge row and the contested note, so **the drop does not orphan the M-source RCh Shabbat**. Hai Gaon (w000451) stays standalone; the disputed-author schema is confirmed unnecessary.
- **D-15 (SUPERSEDED by D-17):** ~~The 174 provisional relations → owner ratifies a batch.~~ Owner reframed 2026-07-23: relations should be a **rule, not a list** — see D-17. The generated review sheet (`same_work_spike/probe/rsource/results/v2_provisional_relations_review.csv`, gitignored — 174 rows, 9 `review` flagged on top, `subset_partof` sorted by `span_jac` desc) is retained as a **validation / spot-check / residual-queue** aid for the rule, not the primary mechanism.
- **D-16:** Three return-questions the parallel session posed back to 135 — resolve them in the **v2-bake-plan update + its Codex review**, not in this discuss: (a) output-shape mapping (handoff JSON → sidecar `canonical_work_id` + `work_relations` table); (b) whether to **filter merges to the shipped 1,270 works**; (c) the **`ref_corpus_v2.pkl` stability check**. The parallel session's two deliberate non-actions: `gen2_workid_registry.json` left untouched (folds in at their #17 mask rebuild to avoid changing the MASK-2 fingerprint), and the provisional relations left unratified.

### Date-driven directional relation rule (NEW 2026-07-23 — UNRESEARCHED; flag for Codex adversarial review)
- **D-17:** The provisional-relation handling is generalized **from a curated pair-list to a date-driven directional RULE.** Phenomenon: a *later* work routinely contains/cites *earlier* ones — a Rif carries Mishnah / Bavli / Tosefta; any Rishon carries prayers & blessings (Rambam the extreme). Using each work's **composition date** as the direction signal: `A later than B + shared text ⇒ A cites/embeds B` (directional; NOT a same-work merge, NOT a witness). This is exactly the anthology/quotation false-positive class (defect #3, e.g. Yalkut Shimoni → Midrash Tehillim).
  - **Reach = option 1 — "represent + measure; route later" (owner-chosen).** v2 uses the rule to (a) **generate directional `work_relations` BY RULE** (replacing the 174-pair list) and (b) **LABEL** likely later-quotes-earlier witness claims and feed them to CERT-01 as a **named quotation-FP diagnostic family**, so tier_a precision is measured with the quotation problem visible. **NO auto-demotion in v2** — actually routing quotations out of the witness/tier_a band stays **Lever 2 / v2.1**, after the rule is validated. (Consistent with the cautious-routing discipline: the density-only shadow mis-routed ~26%; a date rule is a different, unproven signal.)
  - **Direction is work-level; per-claim routing is span-level.** The date orders works; whether a specific `(page, work)` witness claim IS the quoted text is a span-level question (why Lever 2 needs ref-subspans). So the rule flags/labels candidates in v2; per-claim demotion is validated later.
  - **Date source (masking-safe reference):** the M-source composition-date table — owner-held, **external to the repo**, column `תאריך`; **100% coverage over ~8,233 M-source works**; values in descriptive Hebrew (ranges / "after year N") needing parsing into a comparable chronology, with overlapping ranges leaving some directions ambiguous → residual/manual queue. Sefaria (508) / JA (106) sidecar works get a date only by **inheriting through a twin-merge/crosswalk link** to an M-source copy (near-complete for the classical Hebrew canon; thinnest for JA). Build needs a **clean join** from that table to the `w000xxx` ids via `discovery_data/crosswalk.json`.
  - **⚠ FLAG FOR CODEX (owner instruction):** this date-driven-rule approach is a **NEW, unresearched option** introduced during discuss. The owner will send this CONTEXT (+ the v2-bake-plan update) to Codex for **adversarial review**, explicitly calling out the date rule as an unvetted idea. Research/planning MUST treat it as a **hypothesis to validate** — coverage, date parsing, crosswalk-join feasibility, over-routing risk, ambiguous/overlapping-date handling, and whether work-level direction is sufficient without span info — not a settled mechanism.

### Claude's Discretion
- **Band-label values module = hand-authored** to match `discovery-band-labels-v1.md` + a **guard test that fails on drift** (same pattern as the frozen-enum guards), rather than auto-parsing the markdown. The numbers + status stay data-driven from the sidecar `band_precision` table (BAND-02). Owner raised no objection.
- **The (B) enum rename** (`expert_verified` → `high_confidence_algorithmic`) rides the v2 bake in lockstep across the 7 files in discovery-band-labels-v1.md §5. The display layer maps **both** the v1 stored key and the v2 key → the same display label, so labels work pre- and post-bake (surfaces never show the raw key).
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Discovery contract specs (single sources of truth)
- `docs/specs/discovery-band-labels-v1.md` — THE contract for band labels + precision presentation (BAND-01/04, CERT-02). §2 EN/HE labels + review overlay; §3 precision rules; §3.1 page-level coverage bands + the Lever-1 0.45 cliff; §4 default-shown policy + multi-register invariant; §5 the v2 enum-rename lockstep (7 files).
- `docs/specs/discovery-v2-bake-plan.md` — the leadoff re-distill plan (DRAFT). MUST be updated to consume the full `v2_canonical_merges.json` handoff (16 merges + ratified/provisional relations, not the drafted 7+3) and re-Codex-reviewed before any code change.
- `docs/specs/discovery-frames.md` — the v1 frozen frame (SUPERSEDED-PENDING; reference build). v2 needs a new `docs/specs/discovery-frames-v2.md` (corrected per-band / per-evidence_source counts, merge/drop/relation summary, new frame_content_hash + DB content_hash).
- `docs/specs/discovery-budgets.md` — PERF-01 caps + the tunable-only-by-versioning discipline (the model for the methods report + immutable report id).
- `docs/specs/discovery-sidecar-schema-v1.md` — frozen two-table schema + frozen enum vocab.
- `docs/specs/discovery-deploy.md` — asset-first deploy / rollback / reproducible rebuild recipe (the v2 deploy checkpoint follows it).

### Census / canonical-merge handoff (Track B input — MASKING-SENSITIVE, gitignored, never render)
- `same_work_spike/probe/rsource/data/v2_canonical_merges.json` (+ `same_work_spike/probe/rsource/results/v2_canonical_merges.md`) — owner-ratified merges / relations / drop; the authoritative Track-B input.
- `same_work_spike/probe/rsource/data/v2_cross_census_v2.json` (+ `results/mask2_v2_cross_census_v2.md`) — the raw typed census (twin_merge/contested/subset_partof/review/weak) for context.
- `same_work_spike/probe/rsource/results/v2_provisional_relations_review.csv` — the gitignored review sheet for the 174 provisional relations (now a validation/spot-check aid for the D-17 rule, not the primary mechanism).
- **The M-source composition-date table** (D-17 direction signal) — owner-held, external to the repo, column `תאריך`, ~8,233 works, 100% dated (descriptive Hebrew format). MASKING-SENSITIVE (M-source titles + the folder name is the restricted codename) — reference functionally only; the build joins it to `w000xxx` via `discovery_data/crosswalk.json`. **Flagged as the new unresearched input for Codex adversarial review.**

### Build / verify / service code (v2 changes + values module land here)
- `scripts/build_discovery_sidecar.py` — the bake (Codex-reviewed 6 rounds; the 4 v2 build changes + enum rename land here).
- `scripts/verify_discovery_sidecar.py` — all-invariant verifier + strict masking gate; assert v1 enum names absent from the v2 asset.
- `scripts/discovery_ids.py` — frozen id/enum module (the enum rename lands here in lockstep).
- `web/discovery_assets.py` — fail-closed versioned loader + `_CONFIDENCE_BANDS_BY_SOURCE`.
- `shared/discovery_service.py` — async chokepoint + `_BAND_RANK_ORDER` / `_BAND_RANK_CASE_SQL`.
- `scripts/check_atlas_masking.py` — the permanent masking gate (runs on the v2 asset + repo + every surface).

### CERT-01 protocol precedent (reuse this harness — gitignored spike tree)
- `same_work_spike/probe/scripts/e1_deck.py`, `e1_r2_confirm.py`, `e1_r2_audit.py`, `e1_confirm_sizing.py`, `e1_band_frame.py` — deck builder (blind image pane + reveal-lock), analyzer (physMS-clustered bootstrap + gold/K_eff gates), confirmation-sizing, frame freeze.
- `same_work_spike/probe/results/PLAN-e1-round2.md`, `PLAN-e1-round3-canon.md` — pre-registration protocol templates (estimand, strata, freeze manifest, OC table, gold gate, deviations register).
- `same_work_spike/probe/results/E1-ROUND2-RELEASE.md`, `E1-ROUND3-RELEASE.md` — worked examples (R-A 0.889 confirmed; R-CANON gate FAIL → screening — the pattern the tier_a FAIL branch mirrors).

### Phase & requirement context
- `.planning/phases/134-discovery-data-spine/134-CONTEXT.md` — the two-table model contract (C-1..C-9), band sources, DATA-10 unit×work projection, the `band_precision` table mechanism.
- `.planning/REQUIREMENTS.md` — BAND-01..05, CERT-01, CERT-02, and the REL-01 gate ordering.
- `.planning/ROADMAP.md` — Phase 135 goal + Success Criteria 1–5.

### Discipline (memory)
- `feedback_catalogue_never_evidence` — catalogue = recall yardstick, NEVER acceptance evidence; adjudication stays catalogue-blind (governs CERT-01).
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`band_precision` table** — already baked in v1 (via `--frozen-precision-defaults`); BAND-02 reads it. The CERT-01 result writes tier_a's measured number here; no code change to flip copy.
- **DiscoveryService** (`shared/discovery_service.py`) async chokepoint + versioned loader (`web/discovery_assets.py`) — the display/methods layer reads bands through these.
- **Frozen-enum guard-test pattern** — the model for the D-Claude values-module drift guard.
- **E1/Q2 adjudication harness** (`same_work_spike/probe/scripts/`) — the CERT-01 measurement reuses it wholesale (D-09).
- **134-07 owner-review CSV artifact pattern** — the model for both the 174-provisional-relation ratification (D-15) and any owner-facing tier_a card review.
- **Help page** (bilingual EN/HE, has an `id="api"` anchor precedent) — hosts the new methods section (D-10).

### Established Patterns
- Content-hashed frozen artifacts (frames/budgets); tunable-only-by-versioning contract docs; display labels rendered over stored keys (never show the raw key); fail-closed versioned loader; masking gate on the asset + repo + every surface.

### Integration Points
- Methods section → the existing Help page (bilingual, per-band anchors, flag-gated + `noindex`).
- Band values module → a new shared module the Phase 136 surfaces MUST consume (inseparable-band enforcement).
- v2 asset → deploys via the `discovery-deploy.md` asset-first recipe, once, human-approved.
</code_context>

<specifics>
## Specific Ideas

- **Toggle:** "Show more possible matches" / "הצג התאמות אפשריות נוספות"
- **Disclaimer base:** "Not exhaustive — more identifications may exist." / "אינו ממצה — ייתכנו זיהויים נוספים."
- **tier_a gate:** Strict — lower confidence bound ≥ 0.85; FAIL ⇒ reband to screening.
- **Estimand:** (page, work) unit; CI clustered by physical MS.
- **Posture:** "expert-measured · independent audit pending" — never "certified".
</specifics>

<deferred>
## Deferred Ideas

- **Independent audit of tier_a + R-A** (external second grader) → FUT-07 (external gate; not in v9 delivery).
- **Lever 2 — direction-aware routing** (actually demoting later-quotes-earlier / quoted-works witness claims out of the tier_a band; the ~6% high-coverage residual) → v2.1 follow-up. v2 only *labels + measures* these via the D-17 date rule; per-claim routing needs the Track-1 ref-subspan re-instrumentation + validation of the date rule (and/or the direction signal) so it doesn't over-route.
- **`gen2_workid_registry.json` fold-in** → the parallel session's #17 mask rebuild (deferred to avoid a MASK-2 fingerprint change now).
- **Provisional relations not in the first ratified batch** → a later re-distill.
- **The connections panel + `/work/{id}` claim-rendering surfaces** → Phase 136 (SC#1's inseparable-band invariant is enforced there via the 135 shared component/values module).
- **Parallel-session return items #14 (Track-1 re-run) and #17 (mask rebuild)** — owned by the SEED-029 track, not Phase 135.

None of the above are scope creep on Phase 135 — discussion stayed within the band/certificate/v2-data boundary.
</deferred>

---

*Phase: 135-precision-certificate-confidence-bands*
*Context gathered: 2026-07-23*
