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
- **D-05:** **Estimand unit = (page, work).** This deliberately refines the CERT-01 requirement's "deduplicated manuscript–work claim" wording. Owner rationale: a manuscript carries many *different* identifications across its pages (citation-heavy MSS especially), so a whole-MS verdict blurs *which* claim is judged; page-level is sharper. **Multi-register preserved** — several identifications can be TRUE for one page (Bible + Targum + Tafsir); each (page, work) claim is judged on its own, never "one work per page." **Confidence interval clustered by physical MS** (pages within a MS aren't independent — E1 precedent). **Estimand precisely = the shipped, display-deduplicated `(page_id, canonical_work_id)` population** (Codex F7): sample only AFTER canonical merges + the w001239 drop + Lever-1 coverage routing + the D-17 chronological demotion + dedup have run; when a surviving claim's evidence spans corpora, a deterministic pre-grading rule fixes its corpus stratum.
- **D-06:** **Posture = "expert-measured · independent audit pending"** — parity with R-A's 0.889 today. Individual rows stay "unreviewed · algorithmic estimate" (grading a sample does not verify ~238K rows). The word **"certified" is never used** (discovery-band-labels-v1.md Rule 1). The independent audit is the deferred FUT-07 gate.
- **D-07:** **Pass gate = Strict — lower confidence bound ≥ 0.85.** Owner accepts the demotion risk (the raw tier-A audit flagged ~39% "suspect"; v2 coverage routing removes the cov<0.45 tail, but tier_a could still fail 0.85). **FAIL action (pre-registered) = reband `tier_a` → screening** — which must actually flip `routing_status` (→ `review_only`) / drop it from the default-visible set, NOT merely relabel (Codex F11); release proceeds with the remaining **measured + human-confirmed** bands as the default view (the word "certified" is never used — F11). Per Codex F13, distinguish *measured-below-floor* (reband) from *insufficient-evidence / wide-CI* (keep non-default pending the confirmation draw, don't permanently relabel). Because the D-17 demotion runs first, CERT-01 measures the **post-demotion** tier_a (fewer quotation FPs → likelier to clear the floor).
- **D-08:** **Strata = source corpus (Sefaria / Judeo-Arabic / M-source) × coverage band (high ≥0.60 / med 0.45–0.60)**, weighted to the shipped tier_a population. Both are reliably known pre-grading and strongly predict the outcome. **Work-category is a diagnostic breakdown only, NOT a weighting stratum** (genre is unreliable — deliberately dropped in 134-07). CERT-01 also carries a **later-shared-text quotation-FP diagnostic family** (from the D-17 chronological demotion rule) so the quotation problem is visible in the measurement — but **graders stay blind to the demotion tag**, and it is reported only as classifier validation (coverage / PPV / sensitivity), never used as adjudication evidence (Codex F6, avoids circularity).
- **D-09:** **Reuse the E1/Q2 adjudication harness as-is** (`same_work_spike/probe/scripts/e1_*.py`), pointed at the v2 shipped tier_a frame. Standard kit retained: catalogue-blind with logged reveals, gold-card repeatability gate, **physical-MS-clustered bootstrap** for the CI, a pre-outcome OC (operating-characteristics) table, one **freeze manifest** (RNG seed + artifact fingerprints + all cutoffs), ~200–250 cards + a pre-reserved confirmation draw. The measured number flows into the sidecar `band_precision` table (BAND-02 reads it — already the mechanism from 134-07).

### Band Display & Wordings
- **D-10:** The methods/confidence page (BAND-05) = a **gated, bilingual SECTION inside the existing Help page** (not a new route), with **per-band anchors** for tooltip deep-links (e.g. `Help#confidence-tier_a`). `noindex` until the REL-01 gate. Immutable report identifier = a **content-hashed, versioned string** (e.g. `cert-tier_a-<hash>`), consistent with the frozen frames/budgets discipline. Documents per band: population, unit, sample size, strata, weighted estimate, CI, measurement date, grader + audit status, report id.
- **D-11:** "Show more" toggle wording = EN **"Show more possible matches"** / HE **"הצג התאמות אפשריות נוספות"** (HE candidate — tunable at 136 application time). Behind it: `screening_rb` / `screening_canon` + `routing_status='review_only'` + shadowed rows (per discovery-band-labels-v1.md §4). NOTE: this reconciles the two spec phrasings — BAND-03's user-facing "possible-match" framing wins over §4's technical "show screening/algorithmic matches."
- **D-12:** Recall-honesty disclaimer canonical base sentence (BAND-04) = EN **"Not exhaustive — more identifications may exist."** / HE **"אינו ממצה — ייתכנו זיהויים נוספים."** Per-surface variants are tuned where each surface is built (136+).

### Census / Canonical-Merge Handoff (RESOLVED mid-discussion, 2026-07-23)
- **D-13:** The twin census + canonical-merge decisions are **delivered as an owner-ratified handoff artifact**: `same_work_spike/probe/rsource/data/v2_canonical_merges.json` (+ `results/v2_canonical_merges.md`), `owner_ratified: true`. Contents (counts only): **16 merges** (each → one `canonical_work_id` = the Sefaria id; includes Rabbeinu Chananel ×12 tractates — the large one), **1 ratified `part_of` relation** (Haggadah embedded in MT Sefer Zmanim — not a merge), **1 contested** (Hai / RCh Shabbat), **174 `provisional_relations`** (unratified), **8 `residual_direct`**, and a **`dropped_by_135` list (n=1)** = the w001239 drop handed to 135. This **SUPERSEDES** the v2-bake-plan draft's "7 merges + 3 relations" → the plan MUST be updated (and re-Codex-reviewed) to consume the full artifact. **MASKING-SENSITIVE:** the artifact carries `title_msource` fields — the build *consumes* it, nothing renders it, and it never enters a committed doc. **The 16 merges are TEXT-CONFIRMED ratified data** (span-jaccard 0.98–1.00 + same author — e.g. M-source "commentary on Talmud, X" == Sefaria Rabbeinu Chananel ×12) → they populate `canonical_work_id` directly, NOT any candidates list (SEED-029, 2026-07-23; resolves the ratified-vs-candidate half of Codex F2). Only date-*inferred* signal gets non-ratified treatment — and per D-17 that is a routing demotion, not a relation table.
- **D-14:** **RCh-Shabbat three-way resolution** (w000451 Hai Gaon / w000452 M-source RCh Shabbat / w001239 Sefaria RCh Shabbat, all the same text): the merge table auto-resolves the interaction — because w000452↔w001239 is a merge and 135 drops w001239, that group's canonical **flips from the Sefaria id to the M-source id w000452** (the documented exception to "canonical = Sefaria rep"). Flagged in both the merge row and the contested note, so **the drop does not orphan the M-source RCh Shabbat**. Hai Gaon (w000451) stays standalone; the disputed-author schema is confirmed unnecessary.
- **D-15 (DEAD — no enumerated relations at all):** the 174 provisional relations are NOT ratified, NOT loaded, and there is **no `work_relations` table** in v2. Containment is many-to-many and open-ended (an anthology contains hundreds of works; MT contains haggadah + amidah + birkat hamazon; every commentary embeds its base), so any hand-list is arbitrary + stale (SEED-029, 2026-07-23). Replaced wholesale by the D-17 chronological demotion rule. The review CSV generated earlier (`…/v2_provisional_relations_review.csv`) is now **obsolete**.
- **D-16:** Three return-questions the parallel session posed back to 135 — resolve them in the **v2-bake-plan update + its Codex review**, not in this discuss: (a) output-shape mapping (handoff JSON → sidecar `canonical_work_id` + `work_relations` table); (b) whether to **filter merges to the shipped 1,270 works**; (c) the **`ref_corpus_v2.pkl` stability check**. The parallel session's two deliberate non-actions: `gen2_workid_registry.json` left untouched (folds in at their #17 mask rebuild to avoid changing the MASK-2 fingerprint), and the provisional relations left unratified.

### Chronological co-claim demotion rule (D-17, REVISED 2026-07-23 per SEED-029 — supersedes the earlier "date-driven relation" framing)
- **D-17:** The date signal is NOT a relation generator — it is a **bake-time co-claim DEMOTION router** (spec: `same_work_spike/probe/rsource/results/chronological_demotion_rule.md`). On a page, for each cluster of works whose Track-1 matched spans **overlap** (a co-claim on shared text): keep the **earliest-dated** work (the more informative witness of the shared text's origin); **demote** every **materially-later** (≥ `DELTA`) co-claimant on that shared span to `routing_status='review_only'` + tag `later_shared_text`. It **names no relation** (honors Codex F2 — chronology can only say "later + overlapping", never embed/abridge/quote); there is **no `work_relations` table** and no relation_candidates list.
  - **Fixes defect #3 at bake** (Yalkut Shimoni demoted under Midrash Tehillim; MT↔Haggadah; Rif↔Bavli) with **no per-pair curation.** Runs at bake from dates + census, **with no dependency on the SEED-029 Track-1 re-run** → this IS the launch-grade coarse direction router. The FINE span-level (ref-subspan) router is a later SEED-029 refinement, **not a launch blocker.** (This REVERSES the earlier "Lever 2 deferred ⇒ default unsafe" framing: the coarse router in v2 makes "show all shipped by default" safe per band-labels §4 — see D-18.)
  - **Invariants (bake + verifier/Codex gates):** merges applied FIRST (never chrono-compare a work vs its own cross-corpus twin); demote **per shared span, not whole work** (a fragment's DISTINCTIVE later-work text still IDs correctly — e.g. a real Rashi fragment stays Rashi; only the co-claimed base-text span is credited to the earlier work); **never orphans a shipped row** (earliest = kept = shipped, so a `review_only` row can never shadow a shipped one — 135's hard invariant); **unknown/unreliable date → NEVER demoted** (fail-safe); same-era pairs within `DELTA` → none demoted (or → `contested` if a surface needs one winner); demoted rows are **recoverable** in `review_only`.
  - **DELTA** ("materially later") = **100 yr — SET (date-coverage audit DONE 2026-07-23, see D-19)**; ≈ one date-band step (E1 ≤1050 / E2 1050–1470 / E2L 1470–1550). **Date source:** work `date` field where present, else band midpoint, else UNKNOWN → never-demote. The M-source composition-date table (owner-held, external to the repo, column `תאריך`, ~8,233 works 100% dated, descriptive-Hebrew format; MASKING-SENSITIVE — reference functionally only) is joined to `w000xxx` via `discovery_data/crosswalk.json`; Sefaria/JA works inherit a date only through a ratified twin link.
  - **Known limits (measure, don't hide):** common-source pairs (A,B both quote a lost third text → the later is demoted though neither is the source — safe because no relation is asserted; the earlier is a defensible default, the later recoverable; flagged for the `later_shared_text` measurement). Multi-register (Bible + Targum + Tafsir) is preserved because those occupy **non-overlapping** spans → no co-claim cluster → no demotion.
  - **⚠ Codex adversarial pass DONE (VERDICT REWORK — `135-CODEX-CRITIQUE.md`).** The demotion-rule spec RESOLVES F2/F3 and mitigates F8 (per-span + non-overlapping-multi-register); the residual date-parsing / coverage / crosswalk items (F4/F5/F9/F10/F14) route into the v2-bake-plan rewrite + its own Codex pass.

### Sequencing, audit & Codex disposition (2026-07-23)
- **D-18 (sequencing — resolves Codex blocker #3):** `tier_a` is **held behind the "show more possible matches" toggle until its CERT-01 certificate passes** (owner choice). Until then the DEFAULT view shows only `human_confirmed` rows + the already-measured top algorithmic band (0.889, audit-pending). The D-17 demotion rule fixes the *contamination* half of the blocker (anthology FPs → `review_only` at bake, no Lever-2 wait); D-18 covers the *unmeasured* half. When CERT-01 passes, tier_a promotes to default and "show-all-shipped" is safe because the FPs are already demoted. **Amends band-labels-v1 §4** (tier_a not-default-until-certified) — a versioned label-contract change to fold into v2.
- **D-19 (DELTA + date-coverage audit — Codex F4/F14):** the SEED-029 track offered to run a date-coverage audit against `ref_corpus_v2` (dated-direct / inherited-by-ratified-twin / ambiguous / conflicting / missing — by corpus and by real shared-text pairs) to set `DELTA` and quantify undated coverage before the rule is adopted. **REQUESTED — owner-confirmed 2026-07-23**; handed back to the SEED-029 track. **DONE — owner-relayed 2026-07-23 (supersedes the earlier "gated on SEF/JA dates" caveat):** date coverage **99.9%** (M-source authored-works sheet 100% / SEF 100% / JA 92.7%; 8 anonymous JA fragments fail-safe → never demoted). **`DELTA = 100 yr` is now SET** for the bake — at 100y the rule orders + demotes **69.1%** of the 10,837 launch co-claim work-pairs; **30.7%** are within-100y ties (leave both / `contested` — the deferred #14 fine span-level router / Lever 2 handles these later); **0.2%** undated → fail-safe. **Anthology FP confirmed routed:** Yalkut Shimoni (1250) demoted under Midrash Tehillim (early). Artifacts: `same_work_spike/probe/rsource/data/seftja_dates.json` (407 SEF/JA interim dates) + `same_work_spike/probe/rsource/results/chrono_date_coverage.md`. **Interim SEF/JA dates are swap-in-only** for authoritative Sefaria `compDate` when 135 has it (won't change the mechanism); the 3 web-sourced author dates + anthology-compilation estimates are the softest and worth later confirmation, but none sit in the top co-claim traffic. **Consequence: the coarse D-17 router is now launch-grade** (no Track-1 dependency, no per-pair curation) and `tier_a` is safe to default-show once the demotion runs in the v2 bake — the bake-plan rewrite MUST hardcode `DELTA=100y` (Codex F4/F14 is now a solved input, not an open gate).
- **Codex R1 disposition (`135-CODEX-CRITIQUE.md`, VERDICT REWORK):** BLOCKER-1 (stale bake plan) → the v2-bake-plan rewrite is the gating Track-B task (D-13/D-16), re-Codex-reviewed before code. BLOCKER-2 (no semantic relations) → RESOLVED by D-17 (demote + neutral tag, no `work_relations`). BLOCKER-3 (default sequencing) → RESOLVED by D-17 (coarse router in v2) + D-18 (hold-until-certified). FOLDED here: F6 (graders blind → D-08), F7 (canonical estimand → D-05), F11 (no "certified" + real routing flip → D-07), F13 (measured-vs-insufficient → D-07). ROUTED to the bake-plan rewrite + its Codex pass: F4/F5/F9/F10/F12/F14 (coverage audit, interval/UNKNOWN date parsing, crosswalk-join safety, self-loop/contested guards, full survey-design spec, shared-text threshold). F8 mitigated by the per-span + non-overlapping-multi-register invariants; residual routing accuracy → the fine span router (v2.1).

### Claude's Discretion
- **Band-label values module = hand-authored** to match `discovery-band-labels-v1.md` + a **guard test that fails on drift** (same pattern as the frozen-enum guards), rather than auto-parsing the markdown. The numbers + status stay data-driven from the sidecar `band_precision` table (BAND-02). Owner raised no objection.
- **The (B) enum rename** (`expert_verified` → `high_confidence_algorithmic`) rides the v2 bake in lockstep across the 7 files in discovery-band-labels-v1.md §5. The display layer maps **both** the v1 stored key and the v2 key → the same display label, so labels work pre- and post-bake (surfaces never show the raw key).
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Discovery contract specs (single sources of truth)
- `docs/specs/discovery-band-labels-v1.md` — THE contract for band labels + precision presentation (BAND-01/04, CERT-02). §2 EN/HE labels + review overlay; §3 precision rules; §3.1 page-level coverage bands + the Lever-1 0.45 cliff; §4 default-shown policy + multi-register invariant; §5 the v2 enum-rename lockstep (7 files).
- `docs/specs/discovery-v2-bake-plan.md` — the leadoff re-distill plan (DRAFT, **STALE** per Codex BLOCKER-1). MUST be rewritten to: consume the full `v2_canonical_merges.json` handoff (16 text-confirmed merges → `canonical_work_id`), **DROP the `work_relations` / enumerated-relations design entirely**, adopt the D-17 chronological demotion rule, and resolve D-16; re-Codex-reviewed before any code change.
- `same_work_spike/probe/rsource/results/chronological_demotion_rule.md` — **the D-17 bake spec** (co-claim demotion, invariants, DELTA, worked examples). The v2-bake-plan rewrite implements this. (gitignored tree)
- `docs/specs/discovery-frames.md` — the v1 frozen frame (SUPERSEDED-PENDING; reference build). v2 needs a new `docs/specs/discovery-frames-v2.md` (corrected per-band / per-evidence_source counts, merge/drop/relation summary, new frame_content_hash + DB content_hash).
- `docs/specs/discovery-budgets.md` — PERF-01 caps + the tunable-only-by-versioning discipline (the model for the methods report + immutable report id).
- `docs/specs/discovery-sidecar-schema-v1.md` — frozen two-table schema + frozen enum vocab.
- `docs/specs/discovery-deploy.md` — asset-first deploy / rollback / reproducible rebuild recipe (the v2 deploy checkpoint follows it).

### Census / canonical-merge handoff (Track B input — MASKING-SENSITIVE, gitignored, never render)
- `same_work_spike/probe/rsource/data/v2_canonical_merges.json` (+ `same_work_spike/probe/rsource/results/v2_canonical_merges.md`) — owner-ratified merges / relations / drop; the authoritative Track-B input.
- `same_work_spike/probe/rsource/data/v2_cross_census_v2.json` (+ `results/mask2_v2_cross_census_v2.md`) — the raw typed census (twin_merge/contested/subset_partof/review/weak) for context.
- `same_work_spike/probe/rsource/results/v2_provisional_relations_review.csv` — **OBSOLETE** (the 174-pair review is superseded by the D-17 demotion rule; retained only as a historical artifact).
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
- **134-07 owner-review CSV artifact pattern** — the model for any owner-facing tier_a card review / title curation (the 174-relation ratification is dead per D-15/D-17).
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
- **Sequencing:** tier_a stays behind the toggle until CERT-01 passes (D-18); the coarse chronological demotion (D-17) runs at bake to clear anthology FPs first.
- **SEED-029 date-coverage audit — DONE 2026-07-23 (D-19):** coverage 99.9%; **`DELTA=100y` SET** → 69.1% of co-claims demotable, 30.7% within-100y ties (→ deferred fine router / `contested`), 0.2% fail-safe; anthology FP (Yalkut Shimoni → Midrash Tehillim) confirmed routed. `tier_a` safe to default-show once the demotion runs in the v2 bake. Interim SEF/JA dates swap-in-only for Sefaria `compDate`.
</specifics>

<deferred>
## Deferred Ideas

- **Independent audit of tier_a + R-A** (external second grader) → FUT-07 (external gate; not in v9 delivery).
- **Lever 2 — FINE span-level (ref-subspan) direction router** → v2.1 follow-up. The COARSE chronological demotion (D-17) now runs in v2 at bake (no Track-1 dependency) and is the launch-grade router; the fine span-level router (needs the SEED-029 Track-1 ref-subspan re-instrumentation) is a later refinement of routing *accuracy*, not a launch blocker.
- **`gen2_workid_registry.json` fold-in** → the parallel session's #17 mask rebuild (deferred to avoid a MASK-2 fingerprint change now).
- **Provisional relations not in the first ratified batch** → a later re-distill.
- **The connections panel + `/work/{id}` claim-rendering surfaces** → Phase 136 (SC#1's inseparable-band invariant is enforced there via the 135 shared component/values module).
- **Parallel-session return items #14 (Track-1 re-run) and #17 (mask rebuild)** — owned by the SEED-029 track, not Phase 135.

None of the above are scope creep on Phase 135 — discussion stayed within the band/certificate/v2-data boundary.
</deferred>

---

*Phase: 135-precision-certificate-confidence-bands*
*Context gathered: 2026-07-23*
