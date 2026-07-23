# Phase 135: Precision Certificate & Confidence Bands - Research

**Researched:** 2026-07-23
**Domain:** Data-driven confidence-band display (bilingual NiceGUI methods page + shared label module) + a pre-registered statistical precision measurement (physMS-clustered bootstrap, reusing an existing research harness) + an offline sidecar re-distillation (canonical merge + coverage routing + a new chronological co-claim demotion mechanism)
**Confidence:** MEDIUM-HIGH — the display/methods-page track (Track A) is HIGH confidence (every file, function, and precedent was read directly from the repo). The v2 bake-plan rewrite and CERT-01 protocol design (Track B) are MEDIUM confidence — the mechanism is well-precedented (E1/Q2 harness, existing schema) but a couple of concrete parameters (the exact routing_reason schema amendment, the CERT-01 structural gate floors) are explicitly still open per CONTEXT.md and the Codex critique.

> **⚠ ORCHESTRATOR CORRECTION (2026-07-23, added post-research — this pass predates it).** The **D-19 date-coverage audit is now DONE** (owner-relayed via the SEED-029 track; CONTEXT.md D-19/D-17 updated + committed `84054c45`). **`DELTA = 100 yr` is SET — no longer an open external gate.** Coverage is 99.9% (M-source authored-works 100% / SEF 100% / JA 92.7%; 8 anonymous JA fragments fail-safe). At DELTA=100y the D-17 coarse router orders+demotes 69.1% of the 10,837 launch co-claim work-pairs; 30.7% are within-100y ties (→ the deferred #14 fine span-level router / `contested`); 0.2% undated → fail-safe. Anthology FP confirmed routed (Yalkut Shimoni 1250 demoted under Midrash Tehillim). Artifacts: `same_work_spike/probe/rsource/data/seftja_dates.json` (407 SEF/JA interim dates, **swap-in-only** for authoritative Sefaria `compDate`) + `same_work_spike/probe/rsource/results/chrono_date_coverage.md`. **Consequences for planning:** (a) the coarse D-17 router is **launch-grade** (no Track-1 dependency, no per-pair curation); (b) the v2-bake-plan rewrite MUST **hardcode `DELTA=100y`** (with citation to the audit) rather than flag it provisional; (c) **Codex F4/F14 are solved inputs, not open items**; (d) **do NOT plan any "run the date audit / set DELTA" task** — it is already delivered. Everywhere below that treats DELTA / D-19 / F4 as unresolved (notably **Pitfall 4**, the primary recommendation's "external gate", and the Metadata) is **superseded by this note.**

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** Split into Track A (census-independent, now) + Track B (fires on census). Track A ships independently behind the discovery flag.
- **D-02:** "Done" = grading **STARTED**, not completed. Grading + published certificate finish in parallel (136–138) and gate only the Phase 139 flip.
- **D-03:** The census is the accepted **critical path** for v2 *and* Phase 136 (read surfaces need corrected v2 data). No partial-census fallback. **RESOLVED mid-discussion — census delivered**; the blocker is lifted.
- **D-04:** The v2 production deploy is a **human-approved checkpoint**, asset-first (per `docs/specs/discovery-deploy.md` / the 133-06 pattern), deployed **ONCE** (never v1-then-v2).
- **D-05:** **Estimand unit = (page, work).** Owner rationale: a manuscript carries many *different* identifications across its pages, so a whole-MS verdict blurs *which* claim is judged; page-level is sharper. **Multi-register preserved.** **Confidence interval clustered by physical MS.** **Estimand precisely = the shipped, display-deduplicated `(page_id, canonical_work_id)` population** (Codex F7): sample only AFTER canonical merges + the w001239 drop + Lever-1 coverage routing + the D-17 chronological demotion + dedup have run; when a surviving claim's evidence spans corpora, a deterministic pre-grading rule fixes its corpus stratum.
- **D-06:** **Posture = "expert-measured · independent audit pending"** — parity with R-A's 0.889 today. Individual rows stay "unreviewed · algorithmic estimate." The word **"certified" is never used**. The independent audit is the deferred FUT-07 gate.
- **D-07:** **Pass gate = Strict — lower confidence bound ≥ 0.85.** **FAIL action (pre-registered) = reband `tier_a` → screening** — which must actually flip `routing_status` (→ `review_only`) / drop it from the default-visible set, NOT merely relabel. Distinguish *measured-below-floor* (reband) from *insufficient-evidence / wide-CI* (keep non-default pending the confirmation draw, don't permanently relabel). Because the D-17 demotion runs first, CERT-01 measures the **post-demotion** tier_a.
- **D-08:** **Strata = source corpus (Sefaria / Judeo-Arabic / M-source) × coverage band (high ≥0.60 / med 0.45–0.60)**, weighted to the shipped tier_a population. Work-category is diagnostic only, NOT a weighting stratum. CERT-01 also carries a **later-shared-text quotation-FP diagnostic family** so the quotation problem is visible — but **graders stay blind to the demotion tag**, reported only as classifier validation, never adjudication evidence.
- **D-09:** **Reuse the E1/Q2 adjudication harness as-is** (`same_work_spike/probe/scripts/e1_*.py`), pointed at the v2 shipped tier_a frame. Standard kit: catalogue-blind with logged reveals, gold-card repeatability gate, **physical-MS-clustered bootstrap** for the CI, a pre-outcome OC table, one **freeze manifest**, ~200–250 cards + a pre-reserved confirmation draw. The measured number flows into the sidecar `band_precision` table.
- **D-10:** The methods/confidence page (BAND-05) = a **gated, bilingual SECTION inside the existing Help page** (not a new route), with **per-band anchors**. `noindex` until the REL-01 gate. Immutable report identifier = a **content-hashed, versioned string** (e.g. `cert-tier_a-<hash>`). Documents per band: population, unit, sample size, strata, weighted estimate, CI, measurement date, grader + audit status, report id.
- **D-11:** "Show more" toggle wording = EN **"Show more possible matches"** / HE **"הצג התאמות אפשריות נוספות"** (HE candidate — tunable at 136 application time). Behind it: `screening_rb` / `screening_canon` + `routing_status='review_only'` + shadowed rows.
- **D-12:** Recall-honesty disclaimer canonical base sentence (BAND-04) = EN **"Not exhaustive — more identifications may exist."** / HE **"אינו ממצה — ייתכנו זיהויים נוספים."** Per-surface variants tuned where each surface is built (136+).
- **D-13:** The twin census + canonical-merge decisions are **delivered** as an owner-ratified handoff artifact (`same_work_spike/probe/rsource/data/v2_canonical_merges.json` + `.md`, `owner_ratified: true`). **16 merges** (each → one `canonical_work_id`), **1 ratified `part_of` relation**, **1 contested** (resolved by drop), **174 `provisional_relations`** (unratified, NOT loaded), **8 `residual_direct`**, **`dropped_by_135` (n=1)** = w001239. This **SUPERSEDES** the v2-bake-plan draft's "7 merges + 3 relations." **MASKING-SENSITIVE:** the artifact carries `title_msource` fields — consumed, never rendered, never in a committed doc. The 16 merges are **TEXT-CONFIRMED ratified data** → populate `canonical_work_id` directly, not a candidates list.
- **D-14:** RCh-Shabbat resolution: because w000452↔w001239 is a merge and 135 drops w001239, that group's canonical **flips from the Sefaria id to the M-source id w000452** — the documented exception to "canonical = Sefaria rep." Hai Gaon (w000451) stays standalone.
- **D-15 (DEAD — no enumerated relations at all):** the 174 provisional relations are NOT ratified, NOT loaded; **no `work_relations` table** in v2. Replaced wholesale by the D-17 chronological demotion rule. The earlier review CSV is now **obsolete**.
- **D-16:** Three return-questions resolved **in the v2-bake-plan update + its Codex review**, not in discuss: (a) output-shape mapping (handoff JSON → sidecar `canonical_work_id`; NOT `work_relations`, that's dead); (b) whether to filter merges to the shipped 1,270 works; (c) the `ref_corpus_v2.pkl` stability check.
- **D-17 (chronological co-claim demotion rule, REVISED 2026-07-23):** The date signal is a **bake-time co-claim DEMOTION router**, not a relation generator (spec: `same_work_spike/probe/rsource/results/chronological_demotion_rule.md`). On a page, for each cluster of works whose Track-1 matched spans **overlap**: keep the **earliest-dated** work; **demote** every **materially-later** (≥ `DELTA`) co-claimant on that shared span to `routing_status='review_only'` + tag `later_shared_text`. Names no relation. **Invariants:** merges applied FIRST; demote **per shared span, not whole work**; **never orphans a shipped row**; **unknown/unreliable date → NEVER demoted**; same-era pairs within `DELTA` → none demoted (or → `contested`). **DELTA** = **100 yr — SET (date-coverage audit DONE 2026-07-23; see D-19+ the correction at top)**; ≈ one date-band step. Date source: work `date` field, else band midpoint, else UNKNOWN → never-demote. The M-source composition-date table (owner-held, external, MASKING-SENSITIVE) joins to `w000xxx` via `discovery_data/crosswalk.json`. **Known limits:** common-source pairs (both quote a lost third text) demote the later though neither is the source (safe — no relation asserted, flagged for measurement). Multi-register preserved (non-overlapping spans → no cluster → no demotion). **Codex adversarial pass DONE (VERDICT REWORK)** — residual items (date-parsing / coverage / crosswalk / F4/F5/F9/F10/F14) route into the v2-bake-plan rewrite + its own Codex pass.
- **D-18 (sequencing):** `tier_a` is **held behind the "show more possible matches" toggle until its CERT-01 certificate passes**. Until then the DEFAULT view shows only `human_confirmed` rows + the top algorithmic band (0.889, audit-pending). Amends band-labels-v1 §4 (tier_a not-default-until-certified) — a versioned label-contract change to fold into v2.
- **D-19 [RESOLVED 2026-07-23 — see orchestrator correction at top]:** the date-coverage audit against `ref_corpus_v2` is DONE; **`DELTA=100y` is SET** (coverage 99.9%; 69.1% of co-claims demotable, 30.7% within-100y ties, 0.2% fail-safe). The bake adopts the rule with a hardcoded, audit-cited DELTA — no remaining external gate.
- **Codex R1 disposition (`135-CODEX-CRITIQUE.md`, VERDICT REWORK):** 3 BLOCKER + 9 HIGH + 2 MEDIUM. BLOCKER-1 (stale bake plan) → the v2-bake-plan rewrite is the gating Track-B task, re-Codex-reviewed before code. BLOCKER-2 (no semantic relations) → RESOLVED by D-17. BLOCKER-3 (default sequencing) → RESOLVED by D-17 + D-18. Folded into CONTEXT already: F6→D-08, F7→D-05, F11→D-07, F13→D-07. **ROUTED to the bake-plan rewrite + its own Codex pass**: F4 (date-coverage audit — **RESOLVED 2026-07-23: DELTA=100y, drop from this list**), F5 (interval/UNKNOWN date parsing), F9 (crosswalk-join safety), F10 (self-loop/contested guards), F12 (full survey-design spec — inclusion probabilities, PSU bootstrap, effective n), F14 (shared-text/overlap threshold spec). F8 (systematic multi-register failure modes) mitigated by per-span + non-overlapping invariants; residual → the fine span router (v2.1).

### Claude's Discretion

- **Band-label values module = hand-authored** to match `discovery-band-labels-v1.md` + a **guard test that fails on drift** (same pattern as the frozen-enum guards), rather than auto-parsing the markdown. The numbers + status stay data-driven from the sidecar `band_precision` table (BAND-02).
- **The (B) enum rename** (`expert_verified` → `high_confidence_algorithmic`) rides the v2 bake in lockstep across the 7 files in discovery-band-labels-v1.md §5. The display layer maps **both** the v1 stored key and the v2 key → the same display label.

### Deferred Ideas (OUT OF SCOPE)

- Independent audit of tier_a + R-A (external second grader) → FUT-07.
- Lever 2 (fine span-level direction router) → v2.1 follow-up; the coarse D-17 router is the launch-grade mechanism for v2.
- `gen2_workid_registry.json` fold-in → a later SEED-029 mask rebuild.
- Provisional relations not in the first ratified batch → a later re-distill.
- The connections panel + `/work/{id}` claim-rendering surfaces → Phase 136 (Phase 135 renders no claim lists — only the methods page).
- Parallel-session return items #14 (Track-1 re-run) and #17 (mask rebuild) — owned by SEED-029, not Phase 135.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| BAND-01 | Every displayed claim carries its band inseparably through every UI surface / serializer / copy path | §"Band values/display module" — a shared, importable label module + a shared band-badge component skeleton establish the ONE rendering path; enforced with real claim lists starting Phase 136. Phase 135's own consumer is the methods page (iterates all 7 bands). |
| BAND-02 | Band labels/precision copy are data-driven from sidecar metadata; certification-status changes flip copy with no code change | §"Band values/display module" — values module reads `band_precision` via `DiscoveryService`; §"Code Examples" shows the fixture-driven no-code-change test pattern already proven in `tests/test_discovery_bands.py`. |
| BAND-03 | High-confidence bands shown by default; screening bands behind an explicit toggle | §"'Show more' toggle + recall disclaimer primitives" — Phase 135 establishes ONLY the wording constants + component skeleton (D-11); real routing_status-driven show/hide logic is Phase 136 (PANEL-01..03, WORK-01). |
| BAND-04 | Recall-honesty disclaimer on all discovery surfaces | §"'Show more' toggle + recall disclaimer primitives" — same treatment as BAND-03; D-12 base sentence established as a shared constant now, surface-specific wording tuned at 136+. |
| BAND-05 | Bilingual methods/confidence page documents each band | §"Methods page" — exact Help-page insertion point, anchor convention, per-band field set, and the noindex/report-id open questions. |
| CERT-01 | Pre-registered stratified tier-A precision measurement with a written protocol; frame freezes after v2 stabilizes; ~200–250 cards drawn; enters grading | §"CERT-01 protocol + card draw", §"v2 bake-plan rewrite", §"Sequencing" — the E1/Q2 harness reuse, freeze-manifest mechanics, 3-way outcome tree, and the explicit "grading STARTED" completion signal. |
| CERT-02 | Each band shows ONLY a number in its own unit, paired with status | §"Band values/display module" — the `tier_a` "not yet measured" rendering rule and the "estimated band precision [CI]" phrasing rule are both mechanically testable against `band_precision.precision IS NULL`. |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Dual app / shared layer:** any new module Phase 135 authors (values module, label maps) belongs in `shared/` — importable by both `web/` now and a future desktop consumer (FUT-03), never `web/`-only, mirroring the existing `shared/*_service.py` convention and `shared/discovery_service.py`'s own placement.
- **Hebrew RTL / bilingual:** every new string (toggle wording, disclaimer, band labels, methods-page fields) ships EN + HE from line one.
- **Supabase Data API grants rule:** not applicable this phase — no new `public` Supabase table is created (that's Phase 137/JUDGE-*).
- **Documentation maintenance:** `docs/OPEN_ISSUES.md` review at session start/end; `docs/specs/*.md` versioned-artifact discipline (never a silent edit — new dated amendment sections only) governs every touched spec (`discovery-band-labels-v1.md`, `discovery-v2-bake-plan.md`, `discovery-sidecar-schema-v1.md`, `discovery-frames.md`/`-v2.md`).
- **`python scripts/check_docs.py` before finishing a session.**
- **No mass re-indexing / no background webserver launched from Bash** — not directly implicated (no Tantivy index or web-server changes in this phase), but the v2 rebuild + deploy steps must follow `docs/specs/discovery-deploy.md`'s asset-first recipe exactly (memory: `feedback_deploy_db_sync` — scp DBs FIRST, then code).
- **Testing:** `pytest tests/` — Windows Qt/Tantivy full-suite caveats don't apply to discovery-only tests (no Qt, no Tantivy), so `pytest tests/test_discovery_*.py` is safe to run directly and fast.

## Summary

Phase 135 has two structurally independent tracks. **Track A** (display) is architecturally simple and low-risk: author a small, hand-typed `shared/` label module that maps the FROZEN `(evidence_source, confidence_band)` enum (already defined in `scripts/discovery_ids.py`) to bilingual display strings and precision-presentation rules exactly as specified in `docs/specs/discovery-band-labels-v1.md`, guard it with a completeness/drift test, and surface it as a new bilingual section inside the existing `web/pages/help.py` Help page (which already has a proven anchor/TOC/feature-flag-gating convention to copy). No new libraries, no new routes, no new Supabase tables. Track A's only genuinely open design question is how to apply `noindex` to a *section* of an already-indexed, single-route page (there is no native per-section robots directive) — resolved below as a recommendation to conditionally noindex the whole `/help` page while the discovery flag is on, mirroring the `/atlas` precedent.

**Track B** (the v2 re-distillation + CERT-01 measurement) is the hard, high-stakes half of this phase. The existing `docs/specs/discovery-v2-bake-plan.md` is confirmed STALE against `135-CONTEXT.md`: it still frames the census as blocking, lists only 7 of the real 16 owner-ratified merges, and — critically — still specifies a `work_relations` table that CONTEXT.md's D-15/D-17 kill entirely in favor of a general **chronological co-claim demotion rule**. This research traced the demotion rule's exact mechanics against `chronological_demotion_rule.md` and the real `v2_canonical_merges.json` handoff artifact, and found a schema consequence the CONTEXT decisions do not spell out: the rule's `later_shared_text` tag has no home in the FROZEN `routing_reason` enum (`{impurity, runner_up_conflict, co_citation, none}`) — the bake-plan rewrite must add it via a dated schema amendment, in lockstep with `discovery_ids.py` and `verify_discovery_sidecar.py`, exactly like the existing 7-file enum-rename lockstep for `expert_verified`. CERT-01's measurement machinery is well-precedented: the E1/Q2 harness (`same_work_spike/probe/scripts/e1_*.py`) already implements everything D-09 asks for — physMS-clustered bootstrap, freeze manifests, pre-outcome OC tables, gold-repeatability gates, reveal-locked blind decks — and has ALREADY run three real rounds with documented pass/fail/deviation outcomes that are the direct template for CERT-01's own pre-registered outcome branches. The harness is stdlib-only (no numpy/scipy), so reuse is a matter of pointing existing scripts at the v2 frame, not new dependencies.

**Primary recommendation:** Plan Track A and Track B as two largely-parallel plan tracks. Track A can start immediately (values module + drift guard + methods-page section) and does not block on anything. Track B's FIRST task must be the `docs/specs/discovery-v2-bake-plan.md` rewrite (Codex-re-reviewed before any code, per the phase-134 discipline the plan itself already invokes) — that rewrite is NO LONGER blocked on the SEED-029 date-coverage audit — it was delivered 2026-07-23 and `DELTA=100y` is SET (see the orchestrator correction at top); the rewrite hardcodes `DELTA=100y` with an audit citation, and its only remaining gate is its own Codex re-review. Sequence the v2 build code, verifier extensions, human-approved deploy checkpoint, and CERT-01 card draw strictly after the rewrite is approved. "Grading STARTED" (D-02) needs an objective, checkable completion signal defined by the planner — recommend "≥1 verdict recorded in the CERT-01 deck's ledger" as the concrete phase-closing test.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Band label + precision values module (BAND-01/02/CERT-02) | API/Backend (`shared/`) | Frontend Server (SSR) | Pure-Python, no NiceGUI import — must be importable by the current SSR render layer AND any future desktop consumer (FUT-03); the render call site lives in SSR |
| "Show more" toggle + disclaimer copy primitives (BAND-03/04) | Frontend Server (SSR) | API/Backend | NiceGUI component/copy constants established now; the `routing_status`-driven data they will gate on (Phase 136) lives in the API/Backend service layer |
| Methods/confidence page (BAND-05) | Frontend Server (SSR) | — | A rendered NiceGUI page section inside the existing Help page; reads the values module, no new backend endpoint |
| v2 bake / re-distillation (Track B leadoff) | Database/Storage | API/Backend | Offline distillation writing `discovery.db`; `DiscoveryService` (API/Backend) is the sole reader |
| Verifier + masking-gate extensions | Database/Storage | — | Integrity/leak-vector checks run against the built asset before it is ever served |
| CERT-01 measurement harness (E1/Q2 reuse) | API/Backend (offline research script) | Database/Storage | Runs against exported research artifacts; writes its result into the sidecar's `band_precision` table |
| v2 asset deploy checkpoint | Database/Storage | API/Backend | Asset-first scp + atomic manifest swap; `DiscoveryService`'s loader (API/Backend) picks it up on restart |

## Standard Stack

No new external packages are required for this phase.

### Core (all already in the codebase / stdlib)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Python stdlib (`hashlib`, `sqlite3`, `random`, `math`, `statistics`) | 3.10+ (project floor) | ID hashing, DB access, the physMS-clustered bootstrap, Wilson intervals | The entire E1/Q2 harness (`e1_deck.py`, `e1_confirm_sizing.py`) is confirmed stdlib-only — `comp_bootstrap`/`wilson_bounds`/`size_confirmation` use only `math`/`random`/`collections`, no numpy/scipy. Reuse requires zero new installs. |
| NiceGUI (existing dependency) | already pinned | Methods-page section rendering | The Help page (`web/pages/help.py`) already uses `ui.card`/`ui.markdown`/`ui.element('a')` anchors; the new section is more of the same. |
| `scripts/discovery_ids.py`, `scripts/check_atlas_masking.py`, `scripts/build_discovery_sidecar.py`, `scripts/verify_discovery_sidecar.py` (in-repo, Phase 134) | current | Frozen enum/id vocab, masking gate, bake, verifier | Single source of truth already established; Phase 135 extends these files, does not replace them. |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Hand-authored label module + drift-guard test (CONTEXT discretion) | Auto-parse `discovery-band-labels-v1.md`'s markdown tables at import/build time | `docs/specs/discovery-band-labels-v1.md`'s own intro text ("Code renders labels from a values module generated from this file") actually suggests auto-generation — but `135-CONTEXT.md`'s "Claude's Discretion" section explicitly OVERRIDES this in favor of hand-authoring + a drift-guard test. **Flag this contradiction for the planner**: CONTEXT.md is the authoritative, later decision and must be followed; do not "fix" the discrepancy by reverting to auto-parsing. |
| physMS-clustered component bootstrap (reused from E1) | A generic per-row (non-clustered) binomial CI | Would understate variance — pages within one physical MS are not independent draws (the whole reason E1 adopted component bootstrapping in the first place; Codex F12 flags that even the clustered bootstrap alone may be statistically insufficient for CERT-01's much stricter 0.85 floor with only ~200-250 cards — see Pitfalls). |

**Installation:** none required.

**Version verification:** N/A — no new package versions to pin. If the planner later decides CERT-01 needs a proper survey-design library (per Codex F12's inclusion-probability/PSU-bootstrap ask), that would be a genuinely new dependency decision requiring its own legitimacy check at that time — not assumed here.

## Package Legitimacy Audit

Not applicable — this phase installs no external packages (confirmed: the reused E1/Q2 harness and all new build/verify code are Python-stdlib-only). No `slopcheck`/registry verification is required. If a future statistical-power fix (Codex F12) introduces a new dependency (e.g. a survey-sampling library), it must go through this gate at that time.

## Architecture Patterns

### System Architecture Diagram

```
[docs/specs/discovery-band-labels-v1.md]  (spec, source of truth for wording)
              |
              v  (hand-authored, drift-guard tested)
[shared/discovery_band_labels.py]  <-------------------------+
              |                                               |
              | reads band_precision rows via                 |
              v                                               |
[shared/discovery_service.py :: DiscoveryService]              |
              |  (async chokepoint, existing)                  |
              v                                               |
[discovery_data/discovery-v1|v2-<hash>.db :: band_precision]   |
                                                               |
[web/pages/help.py :: create_help_page()]  ---- imports ------+
   new "Confidence & Methods" section (per-band anchors,
   gated on discovery_available(), bilingual EN/HE)


TRACK B (offline, dev-box only):

[same_work_spike/probe/rsource/data/v2_canonical_merges.json]  (owner-ratified census)
              |
              v
[docs/specs/discovery-v2-bake-plan.md]  (REWRITE — Codex re-review gate)
              |
              v
[scripts/build_discovery_sidecar.py]  (+4 build changes: canonical merge,
   drop, Lever-1 coverage routing, D-17 chronological demotion)
              |          ^
              |          | joins via discovery_data/crosswalk.json
              |     [M-source composition-date table] (owner-held, masked)
              v
[discovery-v2-<hash>.db]  --verify--> [scripts/verify_discovery_sidecar.py]
              |                              (+new invariants)
              v
[scripts/check_atlas_masking.py --scan-sqlite --scan-asset --scan-repo --strict]
              |
              v  (human-approved, asset-first, per discovery-deploy.md)
[production discovery_data/ on web box]  (ONCE, D-04)
              |
              v
[same_work_spike/probe/scripts/e1_*.py]  (CERT-01 harness, reused as-is)
   pointed at the v2 shipped tier_a frame -> freeze manifest -> ~200-250
   card deck -> grading STARTS (Phase 135 closes) -> measured number written
   into band_precision (parallel with Phases 136-138) -> gates Phase 139 REL-01
```

### Recommended Project Structure

```
shared/
├── discovery_service.py         # existing (134) — extend with band_precision read helpers
└── discovery_band_labels.py     # NEW — hand-authored label/status/precision-copy module (BAND-01/02/CERT-02)

web/
├── pages/help.py                # extend — new bilingual "Confidence & Methods" section, per-band anchors
└── components/                  # candidate home for a shared "band badge" widget skeleton (established now, wired 136+)

scripts/
├── build_discovery_sidecar.py   # extend — 4 v2 build changes (merge, drop, Lever-1, D-17 demotion)
├── verify_discovery_sidecar.py  # extend — new invariants (routing_reason enum, never-orphan, unknown-date)
├── discovery_ids.py             # extend — routing_reason enum amendment; v1/v2 dual-key band lookup helper
└── check_atlas_masking.py       # extend — register M-source date-table vocabulary defensively

docs/specs/
├── discovery-v2-bake-plan.md            # REWRITE (gating Track-B task, Codex re-review required)
├── discovery-sidecar-schema-v1.md       # dated amendment section (routing_reason enum add)
├── discovery-frames-v2.md               # NEW (corrected per-band/per-evidence_source counts)
└── discovery-band-labels-v1.md          # amend §4 (D-18: tier_a not-default-until-certified)

same_work_spike/probe/scripts/           # (gitignored, reused as-is per D-09)
├── e1_deck.py, e1_confirm_sizing.py, e1_band_frame.py, e1_r2_confirm.py, e1_r2_audit.py
└── (a small new adapter script pointing the harness at the v2 frame — NEW, needs its own tests)
```

### Pattern 1: Frozen enum, hand-typed display strings, machine-checked completeness

**What:** `scripts/discovery_ids.py` already exposes `CONFIDENCE_BANDS_BY_SOURCE` as the single frozen source of truth for which `(evidence_source, confidence_band)` pairs exist. The new label module must NOT redeclare this set — it imports it and asserts, in a test, that its own label dict's key set is a superset (covering the v1→v2 dual-key exception below) with no gaps and no orphans.

**When to use:** Any time a hand-authored display artifact must track a machine-frozen enum without becoming a duplicate, driftable copy of it.

**Example (illustrative shape, not final code):**
```python
# Source: scripts/discovery_ids.py (existing, read directly)
CONFIDENCE_BANDS_BY_SOURCE = {
    "track1_direct": frozenset({"expert_verified", "tier_a", "screening_rb", "screening_canon"}),
    "propagated": frozenset({"corroborated", "weak", "not_evaluated"}),
}

# shared/discovery_band_labels.py (NEW, hand-authored)
# v1 -> v2 key normalization BEFORE lookup (both keys must resolve to ONE label)
_V1_TO_V2_BAND_KEY = {"expert_verified": "high_confidence_algorithmic"}

def _canon_band_key(confidence_band: str) -> str:
    return _V1_TO_V2_BAND_KEY.get(confidence_band, confidence_band)

BAND_LABELS = {
    # keyed by (evidence_source, CANONICAL v2 band key) -- values module normalizes
    # the incoming v1-or-v2 stored key via _canon_band_key() before lookup, so a
    # single label table serves both pre- and post-bake sidecars.
    ("track1_direct", "high_confidence_algorithmic"): {
        "en": "High-confidence match (algorithmic)",
        "he": "התאמה בוודאות גבוהה (אלגוריתמית)",
    },
    # ... remaining 6 rows per docs/specs/discovery-band-labels-v1.md §2
}
```

```python
# tests/test_discovery_band_labels.py (NEW) -- the drift guard
import scripts.discovery_ids as ids
from shared.discovery_band_labels import BAND_LABELS, _canon_band_key

def test_every_frozen_band_has_a_label():
    for source, bands in ids.CONFIDENCE_BANDS_BY_SOURCE.items():
        for band in bands:
            key = (source, _canon_band_key(band))
            assert key in BAND_LABELS, f"missing label for {key}"

def test_no_orphan_labels():
    frozen_keys = {
        (source, _canon_band_key(band))
        for source, bands in ids.CONFIDENCE_BANDS_BY_SOURCE.items()
        for band in bands
    }
    assert set(BAND_LABELS.keys()) <= frozen_keys
```

### Pattern 2: Help-page section with bilingual anchor + flag-gated TOC entry

**What:** `web/pages/help.py::_create_english_content()` / `_create_hebrew_content()` render a `toc_items` list of `(anchor, title)` tuples, then one `ui.card()` per section carrying `ui.element('a').props(f'name="help-{anchor}"')`. Feature-flag gating is already precedented: `if WEB_PUZZLE_ENABLED: toc_items.insert(9, ('puzzle', ...))` / `if not WEB_PUZZLE_ENABLED and anchor in {...}: continue`.

**When to use:** Adding the new methods/confidence section (BAND-05).

**Example:**
```python
# Source: web/pages/help.py (existing, read directly, lines 44-70)
toc_items = [
    ('intro', 'Introduction: How it Works'),
    # ...
    ('api', 'Public API & AI Tools'),
    ('my-library', 'My Library — Local Documents'),
]
# NEW insertion, gated exactly like WEB_PUZZLE_ENABLED above:
if discovery_available():
    toc_items.append(('confidence', 'Confidence Bands & Methods'))
for anchor, title in toc_items:
    if not discovery_available() and anchor == 'confidence':
        continue
    ui.link(f'• {title}', f'#help-{anchor}').classes('text-primary hover:underline')
```

Per-band anchors inside the section should follow the SAME convention at finer grain, e.g. `ui.element('a').props('name="help-confidence-tier_a"')`, matching CONTEXT D-10's `Help#confidence-tier_a` deep-link requirement.

### Pattern 3: physMS-clustered bootstrap (reuse verbatim, do not reimplement)

**What:** `same_work_spike/probe/scripts/e1_deck.py::components_of()` builds a bipartite work↔physMS union-find over graded cards; `comp_bootstrap()` resamples WHOLE COMPONENTS (not raw cards) B=10,000 times to compute a percentile lower bound. This IS the "physMS-clustered bootstrap" D-05/D-09 call for.

**Example (already-proven code, read directly):**
```python
# Source: same_work_spike/probe/scripts/e1_deck.py, lines 444-485
def components_of(cards):
    """Bipartite work<->physMS connected components over cards."""
    parent = {}
    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    for c in cards:
        union(('w', c['work_id']), ('m', c['phys']))
    return {c['uid']: find(('w', c['work_id'])) for c in cards}

def comp_bootstrap(rows, B=10000, seed=7, pct=2.5):
    """rows: [(comp, isA)] over DETERMINATE cards -> (p, lo, hi, n_components)."""
    # ... resamples COMPONENTS, not rows; stdlib random only
```
CERT-01's written protocol should cite this function (or a faithful adapter) directly rather than re-deriving component bootstrapping from scratch — this is exactly the "Don't Hand-Roll" case below.

### Anti-Patterns to Avoid

- **Auto-parsing `discovery-band-labels-v1.md`'s markdown at runtime/build time** to generate the label module — explicitly rejected by CONTEXT.md's Claude's Discretion in favor of hand-authoring + a drift-guard test, even though the spec doc's own intro text suggests the opposite. Follow CONTEXT.md.
- **Renaming the band without flipping `routing_status`** — a CERT-01 FAIL that only changes the display string (leaves `routing_status='shipped'`) does not actually hide `tier_a` from the default view, since `discovery-band-labels-v1.md` §4's default policy shows every `shipped` claim regardless of band. This was Codex HIGH-11's exact finding.
- **Sampling raw `(page_id, work_id)` rows for CERT-01** instead of the display-deduplicated `(page_id, canonical_work_id)` population — double-counts cross-corpus twins and measures a population users never actually see (Codex F7 / D-05).
- **Treating the census's `provisional_relations_measurement_only` (174 rows) or `residual_direct` (8 rows) as build inputs** — per D-15/D-16 these are NOT loaded into v2 at all; only `merges` (16, all `owner_verdict: 'approve'`) and `dropped_by_135` (1) drive real build behavior. The `contested` entry (1) is informational/audit-trail only — its outcome is already fully captured by the merge + drop entries.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Clustered confidence interval for a precision estimate | A new bootstrap/CI routine | `e1_deck.py::comp_bootstrap` / `components_of` (physMS union-find + component-level percentile bootstrap) | Already implements exactly D-05's "CI clustered by physical MS" requirement; stdlib-only; has three real rounds of production use with documented edge cases (control-mix composition sensitivity, K_eff/dominance gates). |
| Sample-size / power calculation under intra-cluster correlation | A fresh power analysis | `e1_confirm_sizing.py::size_confirmation` / `n_det_required` / `anova_icc` | Already handles ICC-adjusted design effect + fixed-point component-size estimation; CERT-01's Strict 0.85 floor is a harder test than any E1 round attempted, so reusing the SAME sizing math (not a simplified version) matters. |
| Blind/reveal-locked grading deck UI | A new NiceGUI grading tool | `e1_deck.py::render_deck` / `adapt_template` (static HTML deck generator, reveal-lock via JS) | CERT-01 is an internal researcher tool, not a public web surface — the existing static-HTML deck mechanism (already proven across 3 rounds + hundreds of graded cards) is the right tool, not a new in-app UI. |
| Leak-vector / masking scan for a new external data source (M-source date table) | A bespoke scanner for date-table strings | `scripts/check_atlas_masking.py` + `MASKING_SCAN_PATTERNS_FILE` pattern registration | The scanner already supports registering additional restricted-token patterns without code changes (used for R-source pre-registration in Phase 134, D-03c) — register the M-source date table's vocabulary the same way rather than writing a parallel checker. |
| Deploy/rollback mechanism for the v2 asset | A new deploy script | `docs/specs/discovery-deploy.md`'s asset-first + atomic-manifest-swap recipe (already proven in 134-08 for v1) | Identical shape applies to v2; D-04 only adds the "ONCE, never v1-then-v2" ordering constraint on top of the existing recipe — no new mechanism needed. |
| Frozen id/enum machinery | A parallel enum module | `scripts/discovery_ids.py` (extend in place) | Single source of truth already established in Phase 134; a second enum module anywhere would immediately create the drift risk the guard-test pattern exists to prevent. |

**Key insight:** almost everything Track B needs already exists in the codebase or the gitignored research tree in a battle-tested form (three real E1/Q2 rounds, a working masking scanner, a working deploy runbook). The genuinely NEW code is narrow: the four v2 build changes to `build_discovery_sidecar.py` (merge/drop/Lever-1/D-17-demotion), the corresponding verifier invariants, and a thin adapter pointing the E1/Q2 harness at the v2 frame instead of a spike-internal frame.

## Common Pitfalls

### Pitfall 1: `routing_reason` enum has no slot for `later_shared_text`

**What goes wrong:** The D-17 chronological demotion rule's spec says "tag evidence `later_shared_text`" — but the FROZEN `routing_reason` enum in `docs/specs/discovery-sidecar-schema-v1.md` / `scripts/discovery_ids.py` / the `discovery_evidence` DDL's `CHECK` constraint is `{impurity, runner_up_conflict, co_citation, none}`. There is no fifth value today.
**Why it happens:** D-17 was designed and adversarially reviewed (135-CODEX-CRITIQUE.md) largely in isolation from the frozen schema doc's own "no silent edit, only dated amendment sections" discipline; the CONTEXT.md decisions describe the ROUTING behavior in full but do not explicitly call out the enum consequence.
**How to avoid:** The v2-bake-plan rewrite must include an explicit schema amendment: add `later_shared_text` to `ROUTING_REASONS` in `discovery_ids.py`, the `discovery_evidence.routing_reason` CHECK constraint in `build_discovery_sidecar.py`'s DDL, the validators in `verify_discovery_sidecar.py`, and a new DATED amendment section in `discovery-sidecar-schema-v1.md` (never a silent edit, per that doc's own closing rule). This is an EIGHTH lockstep change, alongside (not instead of) the existing 7-file `expert_verified`→`high_confidence_algorithmic` rename lockstep in `discovery-band-labels-v1.md` §5.
**Warning signs:** A `sqlite3.IntegrityError` on the CHECK constraint the first time real demotion code tries to write `routing_reason='later_shared_text'`; or (worse) a silent fallback to `routing_reason='none'` that would make the demotion's own diagnostic-family measurement (D-08) impossible to compute.

### Pitfall 2: `select_display_evidence`'s precedence lattice has no `routing_status` tier

**What goes wrong:** `scripts/discovery_ids.py::select_display_evidence` (the deterministic function that picks which evidence row a claim displays) ranks purely by `(human_confirmed-track1_direct dominance, band_rank, adjudication_rank, evidence_id)` — it never looks at `routing_status` at all. If a single `(page_id, work_id)` claim ever carries TWO witness evidence rows on the SAME page — one on a co-claimed span (demoted to `review_only` by D-17) and one on a distinctive span (stays `shipped`) — the current lattice could select the `review_only` row as `display_evidence_id` purely because it happens to rank higher on band/adjudication, silently hiding a claim that DOES have a shipped, displayable evidence row.
**Why it happens:** The lattice was frozen in Phase 134 before D-17 (a Phase 135 concept) existed; nothing in the Phase 134 contract required multi-evidence-per-claim routing-status conflicts to be possible, because Lever-1 coverage routing (the only routing mechanism at the time) operates at the CLAIM level, not the individual-evidence-row level within one claim.
**How to avoid:** Before finalizing the D-17 build code, explicitly determine whether Track-1 can ever emit >1 witness evidence row for the same `(page_id, work_id)` (distinct spans on one page for one work). If yes, add a `routing_status` tier to `_display_sort_key` (shipped ranks above review_only, mirroring the "review_only never dominates shipped" cross-claim invariant already documented at the cluster level) and add a regression test exercising exactly this two-evidence-rows-one-shipped-one-demoted case. This is a concrete, previously-undocumented finding from this research session — flag it explicitly in the bake-plan rewrite for Codex's re-review.
**Warning signs:** A claim whose only VISIBLE (queried, `routing_status='shipped'`) evidence exists but whose `display_evidence_id` points at a `review_only` row — a real "orphaned shipped evidence" bug distinct from the already-documented cross-work orphan check.

### Pitfall 3: Order-of-operations — D-17 demotion must run AFTER Lever-1 coverage routing, not interleaved before it

**What goes wrong:** The STALE `docs/specs/discovery-v2-bake-plan.md`'s §6 "Order of operations" places its (now-dead) relation-table population as step 4, BEFORE Lever-1 coverage routing (step 5). `chronological_demotion_rule.md`'s own text is explicit: "Applied AFTER merges (unify same-work) and Lever-1 coverage routing, at bake time." A rewrite that keeps the old ordering (chronology before coverage) would run the two demotion mechanisms in the wrong sequence.
**Why it happens:** The stale plan's step numbering was written before D-17 existed at all (it was written for the OLD relation-table design, which had no ordering dependency on Lever-1).
**How to avoid:** The rewrite's new order-of-operations must read: (1) canonical merge + drop-list exclusion, (2) span-paired claim generation, (3) distinctive/shared routing, (4) Lever-1 coverage routing (`cov<0.45→review_only`), (5) **D-17 chronological co-claim demotion** (NEW, replaces the old step 4), (6) tier-A assignment, (7) bake+verify+masking+manifest.
**Warning signs:** A co-claim cluster where the "kept" (earliest) work has itself already been coverage-demoted by Lever-1 before the chronology step runs — the chronology step would then be comparing against a work that isn't going to ship anyway, producing a confusing or wrong `later_shared_text` tag.

### Pitfall 4: [SUPERSEDED 2026-07-23] DELTA is now RESOLVED (=100y) — the risk is now the inverse: re-planning a delivered audit

**What goes wrong (NOW OBSOLETE):** This research pass predated the audit. D-19's date-coverage audit has since been DELIVERED (owner-relayed 2026-07-23) and `DELTA=100y` is SET with 99.9% coverage. The risk is now the *inverse*: a planner treating DELTA as still-open and creating a defunct "run the audit / set DELTA" gate task, or the bake-plan rewrite flagging DELTA provisional when it should hardcode the delivered value.
**Why it happens:** Track B's plan wants to move fast now that the census landed; it's tempting to just pick a round number (e.g. 100 years) and proceed.
**How to avoid:** In the rewritten bake plan, **hardcode `DELTA=100y` with a citation to `chrono_date_coverage.md`**. Do NOT add an external-gate task for the audit — it is delivered. The remaining real gate is the Codex re-review of the rewrite itself.
**Warning signs:** A plan task proposes to "run the date-coverage audit" or "set DELTA"; or `DELTA=100y` appears in the rewrite with no citation to the audit result (`chrono_date_coverage.md`).

### Pitfall 5: Only one of the four worked chronological-demotion examples is data-confirmed so far

**What goes wrong:** `chronological_demotion_rule.md`'s "Worked examples" table lists FOUR cases (Midrash Tehillim/Yalkut Shimoni, Haggadah/Mishneh Torah Zmanim, Bavli/Rif, base-text/commentary). Inspecting the actual delivered `v2_canonical_merges.json`, only ONE of these (`chronological_rule_examples`, a single list entry: the Haggadah/Zmanim pair, with concrete `span_jac`/`co_pages`/`breadth` evidence numbers) has been operationalized and measured against real data. The other three are qualitative illustrations in the spec doc, not yet computed.
**Why it happens:** The census/demotion-rule delivery focused on the merge list (16 pairs, fully computed) and gave one demonstration of the demotion mechanism, not a full validation sweep.
**How to avoid:** The bake-plan rewrite (or its Codex re-review) should require computing/checking the OTHER three worked examples against real data before shipping the rule at corpus scale — or at minimum documenting that they remain qualitative/unverified pending the full bake run. This directly overlaps Codex HIGH-14 ("pair-generation universe + overlap threshold unspecified — freeze candidate universe... BEFORE the v2 frame freezes").
**Warning signs:** The v2 frame ships with `later_shared_text` counts that look plausible in aggregate but were never spot-checked against the specific worked examples the rule was designed to fix.

### Pitfall 6: Masking coverage gap — the M-source composition-date table is a brand-new external input, never scanned before

**What goes wrong:** D-17 introduces the FIRST use of the M-source composition-date table (owner-held, external, column `תאריך`, descriptive Hebrew dates) as a build input. `docs/specs/discovery-sidecar-schema-v1.md`'s masking discipline and the existing `MASKING_SCAN_PATTERNS_FILE` pattern set were built before this table existed as an input — CONTEXT.md's own canonical_refs section flags it explicitly as "the new unresearched input for Codex adversarial review."
**Why it happens:** Masking pattern registration is a manual, owner-driven step (Phase 134's R-source pre-registration was likewise deferred as "an owner-only operational step" per STATE.md) — it's easy for a genuinely new external data source to slip past the existing pattern set if nobody explicitly re-registers it.
**How to avoid:** (a) Never store the raw descriptive Hebrew date string anywhere in the shipped sidecar — normalize to a numeric year/date-band value only, at build time, before the row is written. (b) Register the M-source date table's known vocabulary (any distinguishing terms) in the masking pattern file defensively, exactly like R-source's D-03c pre-registration. (c) Re-run the full masking gate (`--scan-repo --scan-sqlite --scan-asset --strict`) against the v2 asset AND every newly-authored doc (the rewritten bake plan, the new `discovery-frames-v2.md`) before any deploy step.
**Warning signs:** A masking-gate false negative because the pattern file was never updated for this new input — this is exactly the failure mode DATA-05's whole design exists to prevent, so treat it as a hard blocker, not a nice-to-have.

### Pitfall 7: "Grading STARTED" needs an objective completion signal

**What goes wrong:** D-02 defines Phase 135's closing bar as "grading STARTED, not completed" — but without a concrete, checkable definition, this becomes a subjective judgment call at `/gsd:verify-work` time.
**Why it happens:** The phase intentionally straddles a research process (owner grading, which genuinely can't be rushed to completion) and a software-delivery gate (the phase needs SOME checkable "done").
**How to avoid:** Recommend the planner define a concrete signal, e.g.: (a) the CERT-01 freeze manifest is committed (RNG seed + all cutoffs + frame hash, written BEFORE any card is drawn, mirroring `e1_r2_freeze.json`'s discipline) AND (b) the deck is rendered against the v2 frame AND (c) at least one verdict is recorded in the deck's ledger/verdicts file. All three are mechanically checkable without waiting for the measurement to finish.
**Warning signs:** A phase-close review where "grading started" is asserted verbally with no artifact to point to.

### Pitfall 8: Statistical power — CERT-01's Strict 0.85 floor is harder than anything E1 has attempted

**What goes wrong:** `PLAN-e1-round2.md`'s own Objective section states that with a ~0.788 tier-A control ceiling, "a ≤400-card confirmation needs ~0.81 discovery lower bound for Balanced, ~0.90 for Strict — implausible," which is WHY E1 rounds targeted Broad (≥0.60) as the primary certifiable gate and treated Strict as descriptive-only. CERT-01 (D-07) sets Strict (≥0.85) as its ONE pass gate, with only ~200-250 cards — a materially harder statistical test than any E1 round has ever cleared.
**Why it happens:** CERT-01 is measuring the BULK 89%-of-spine `tier_a` band (not a narrow discovery-stage screening band), so the owner reasonably wants a stricter floor before it can be the DEFAULT view — but the sample-size math from three real E1 rounds suggests this combination (Strict floor + ~200-250 cards) may have materially LOW power even if the true precision is comfortably above 0.85, especially once physMS-clustering variance and the post-D-17-demotion population's likely lower component count are factored in (Codex F12).
**How to avoid:** Before drawing any cards, run the SAME pre-outcome OC-table exercise E1 always runs (`e1_r2_oc.py`/`e1_r3_oc.py` precedent) for CERT-01's exact frame size, strata, and Strict floor — publish the joint pass-probability table BEFORE the deck is drawn (this is D-09's own stated requirement, not a new ask), and treat a low pre-outcome pass probability as a signal to negotiate the card count or reconsider the pre-registered decision rule (per Codex MEDIUM-13's "insufficient evidence" vs. "measured below floor" distinction) — not as a reason to skip the OC step.
**Warning signs:** A written CERT-01 protocol with no pre-outcome OC table, or one that assumes the confirmation-sizing module (`e1_confirm_sizing.py`) will trivially return a workable `n_drawn`.

### Pitfall 9: Per-section `noindex` is not natively expressible on a single-route bilingual Help page

**What goes wrong:** D-10 asks for the new methods section to be `noindex` until REL-01. `/help` is ONE NiceGUI route (`@ui.page('/help', ...)` in `web/main.py`) serving all sections in one response; the existing route has NO `noindex` (it is deliberately indexed — it has real SEO value today, per its full markdown content). HTML robots directives (`<meta name="robots">` / `X-Robots-Tag`) are page-scoped, not section-scoped — there is no way to noindex just a fragment of the response.
**Why it happens:** D-10 was likely written by analogy to the `/atlas` route (which IS its own dedicated page and so CAN carry a page-level `noindex`), without accounting for the fact that the methods content is explicitly scoped as "a SECTION inside the existing Help page (not a new route)."
**How to avoid:** In practice this is largely moot in production because `DISCOVERY_ENABLED` stays OFF (default) through Phases 135-138 (`web/feature_flags.py`, DATA-07) — the section is entirely absent from the rendered HTML pre-Phase-139, so there is nothing to index either way. The residual risk window is internal QA/staging with the flag manually set to `1`. Recommended mechanism (mirrors the `/atlas` precedent of a whole-route conditional `noindex`): conditionally pass `noindex=discovery_available()` to the existing `page_meta('/help', ...)` call in `web/main.py` — i.e., the WHOLE `/help` page goes `noindex` only while the discovery section is actually rendering, reverting to indexed the instant the flag is off. Flag this explicitly as an Open Question for the planner/owner to confirm (see below) — it is a genuinely new mechanism, not a copy-paste of an existing one.
**Warning signs:** A plan that tries to add a per-`<section>` robots tag (does not exist in the HTML spec) or that silently de-indexes `/help` permanently by hardcoding `noindex=True` on the whole route.

## Code Examples

### Fixture-driven "no code change" test for BAND-02 (existing precedent to extend)

```python
# Source: tests/test_discovery_bands.py (existing, read directly) shows the
# fixture-DB pattern Phase 135 should extend for BAND-02's "flip band_precision,
# get new copy, zero code change" requirement:
FIXTURE_DB = Path(__file__).resolve().parent / "fixtures" / "discovery" / "discovery-v1-fixture.db"

def test_valid_evidence_combinations_over_fixture():
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT evidence_kind, evidence_source, confidence_band FROM discovery_evidence"
    ).fetchall()
    # ... assert against scripts.verify_discovery_sidecar.VALID_EVIDENCE_COMBOS
```
The analogous BAND-02 test: load the fixture, read a `band_precision` row via the values module's precision-formatting function, assert the rendered string; then (in a SEPARATE fixture copy) mutate ONLY the `band_precision.precision`/`ci_low`/`ci_high` values via raw SQL, re-render with the SAME code, and assert the output string changed — proving no code-path branch was needed.

### CERT-02's "no number until measured" rule as a targeted regression test

```python
# Illustrative shape -- band_precision.precision IS NULL for tier_a in the
# CURRENT frozen release contract (docs/specs/discovery-sidecar-schema-v1.md
# §1.6): "precision REAL, -- NULL where no valid band-specific measurement
# exists (C-7/G8)". CERT-02 requires the values module to render this as
# "precision not yet measured" and NEVER fabricate a number.
def test_tier_a_shows_no_number_before_cert01():
    row = query_band_precision(evidence_source="track1_direct", confidence_band="tier_a")
    assert row["precision"] is None
    rendered = format_precision_copy(row)
    assert "not yet measured" in rendered
    assert not re.search(r"\d+(\.\d+)?%", rendered)  # never a bare percentage
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| `expert_verified` label implying human review | `high_confidence_algorithmic` (v2 stored key) + a hard `adjudication_status`-gated review badge (`docs/specs/discovery-band-labels-v1.md` Rule 1) | 2026-07-23 (band-labels-v1.md landed, Phase 134 remediation) | Root-caused an actual overclaim (1,067 of 1,188 `expert_verified` rows are `unreviewed`) — Phase 135 must never regress this by treating band strength as review status anywhere in the new label module. |
| Enumerated `work_relations` (embeds/abridges/base_text) | General chronological co-claim demotion rule (D-17), no relation named | 2026-07-23 (D-15/D-17, this phase's own discuss session) | Containment is many-to-many and open-ended; a hand-curated relation table would have been permanently incomplete and stale on corpus growth. The demotion rule is the load-bearing replacement — do not resurrect `work_relations` anywhere in the rewrite. |
| Density-only anthology/quotation router (shadow, mis-routes ~26% of co-claims) | D-17's chronology-based coarse router, launch-grade in v2 with no Track-1 re-run dependency | 2026-07-23 | The fine span-level (direction-aware) router (Lever 2) remains a v2.1 follow-up; v2 ships the coarser but immediately-available chronology mechanism. |

**Deprecated/outdated:** the entire "7 merges + 3 relations" framing in the CURRENT `docs/specs/discovery-v2-bake-plan.md` is superseded (16 merges, 0 relations, +1 general demotion mechanism) — this document must be rewritten, not incrementally patched.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The whole-`/help`-page conditional `noindex=discovery_available()` mechanism is the right way to satisfy D-10's per-section noindex ask | Pitfall 9 / Methods page | If wrong, either the existing indexed `/help` content loses SEO value unnecessarily, or the methods section leaks into search results before REL-01 — LOW risk in practice since `DISCOVERY_ENABLED` stays OFF through Phase 138, but should be explicitly confirmed with the owner/planner rather than assumed. |
| A2 | Track-1 CAN in principle emit >1 witness evidence row for the same `(page_id, work_id)` on distinct spans of one page (motivating the `select_display_evidence` routing_status-tier fix in Pitfall 2) | Common Pitfalls #2 | If Track-1 in fact never does this (always exactly one witness evidence row per claim), Pitfall 2's fix is unnecessary defense-in-depth rather than a required fix — LOW risk either way (adding the tier is safe even if never exercised), but the planner should verify against real v1 data before deciding whether it's a MUST-FIX or a nice-to-have. |
| A3 | `later_shared_text` needs a genuine `routing_reason` CHECK-constraint / frozen-enum amendment (not, e.g., a reuse of an existing reason code or a new column) | Pitfall 1 | If the actual intended design was to reuse `impurity` or add a wholly separate boolean/column instead of extending `routing_reason`, the lockstep-file list and DDL changes described here would need adjustment — but SOME schema change is unavoidable either way since no existing enum value captures "later co-claimant on shared text." |
| A4 | CERT-01's structural gates (minimum determinate-card floor, K_eff floor, dominance ceiling) should be set analogously to E1's own precedent values (150 / ≥20 / ≤0.40) rather than fresh numbers | CERT-01 protocol section | E1's specific numbers were tuned for E1's OWN frame sizes and bands; CERT-01's v2 tier_a frame will differ in size/composition, so these are a reasonable STARTING template, not a guaranteed correct final choice — the pre-outcome OC table (Pitfall 8) is the actual mechanism that should validate whatever numbers are chosen. |

## Open Questions (RESOLVED 2026-07-23 — operationally adopted in the plans)

> **Resolution note (orchestrator, post-plan-checker):** all three questions were operationally resolved by the 9 plans; the recommendations below stand as the adopted answers. (1) report-id hashes the FREEZE MANIFEST alone → adopted in 135-03 / 135-09; (2) whether `select_display_evidence` needs a `routing_status` tier → 135-06 Task 2 embeds an investigate-then-decide against real v2-candidate data; (3) validating the other D-17 worked examples → 135-06 / 135-07 spot-check one worked example (MEDIUM, not a hard gate) per Pitfall 5.

1. **[RESOLVED → 135-03/135-09] Exact hash recipe for the CERT-01 "immutable report identifier" (D-10: `cert-tier_a-<hash>`)**
   - What we know: the project's existing convention is content-hashing over a canonical serialization (e.g. `frame_content_hash` = a membership-based hash excluding volatile `meta`, per `discovery-sidecar-schema-v1.md`). The E1 harness's own freeze-manifest convention hashes multiple artifacts + the scoring script + a `pip freeze` capture.
   - What's unclear: whether the CERT-01 report id should hash just the freeze manifest, or the freeze manifest PLUS the final measured result (which would make it not exist until grading completes — in tension with "immutable ... report identifier" being documented on the methods page BEFORE the measurement lands, since BAND-05 needs to show "not yet measured" with an id placeholder).
   - Recommendation: hash the FREEZE MANIFEST alone (available before any card is drawn) as the report id's basis — this lets the methods page show a stable, real id immediately with "not yet measured" status, and the SAME id persists once the result lands (rather than the id itself changing when the measurement completes, which would break any external link/citation made before completion).

2. **[RESOLVED → 135-06 Task 2] Does `select_display_evidence` actually need the `routing_status` tier (Pitfall 2), or is it structurally unreachable?**
   - What we know: the lattice function as written has no `routing_status` awareness; the schema permits >1 evidence row per claim.
   - What's unclear: whether Track-1's actual match-generation logic (in the gitignored research pipeline, not the sidecar-build code) ever produces >1 witness evidence row for the SAME `(page_id, work_id)` — this requires inspecting `track1_matches`/the tier_a ingest path against real data, which is a Track-B-rewrite-time investigation, not something resolvable from the schema doc alone.
   - Recommendation: the bake-plan rewrite should explicitly check real v2-candidate data for this case before deciding whether the lattice fix is required-for-correctness or defense-in-depth-only.

3. **[RESOLVED → 135-06/135-07] Should the D-17 rule's other three worked examples (Yalkut/Midrash Tehillim, Rif/Bavli, base-text/commentary) be independently computed and validated before the full v2 bake, or is the one delivered example (Haggadah/Zmanim) sufficient evidence the mechanism works?**
   - What we know: only one example ships computed evidence numbers (`span_jac`, `co_pages`, `breadth`) in the actual census JSON today.
   - What's unclear: whether computing the other three is a prerequisite gate for the rewrite's Codex re-review, or an acceptable post-hoc validation once the full bake runs.
   - Recommendation: treat as a MEDIUM-priority ask for the rewrite's Codex re-review (ties directly to Codex HIGH-14's broader "freeze candidate universe... before the frame freezes" concern) rather than a hard blocker, since the demotion mechanism's core logic (date ordering + span overlap) is generic and not example-specific.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `same_work_spike/probe/data/fullcorpus_v2.db` (gitignored research DB) | v2 bake (unchanged from v1) | ✓ | ~3.09 GB on disk, confirmed present | none — hard requirement for the bake, dev-box only |
| `same_work_spike/probe/rsource/data/v2_canonical_merges.json` (+ `.md`) | v2 canonical merge (D-13) | ✓ | confirmed present, `owner_ratified: true`, 16 merges / 1 contested / 174 provisional (unloaded) / 8 residual / 1 drop | none |
| `same_work_spike/probe/rsource/results/chronological_demotion_rule.md` | D-17 bake spec | ✓ | confirmed present | none |
| `same_work_spike/probe/scripts/e1_*.py` (E1/Q2 harness) | CERT-01 (D-09) | ✓ | confirmed present (deck/sizing/audit/band-frame scripts all found) | none — this is the reuse target itself |
| M-source composition-date table (owner-held, external to repo, masking-sensitive) | D-17 date signal | UNVERIFIED — owner-held, not confirmed accessible from this session | — | if unavailable, D-17 cannot compute dates for M-source works; those works fall to UNKNOWN date → never-demoted (fail-safe default per the rule itself, so absence degrades safely rather than blocking) |
| `.masking_patterns` (gitignored pattern file for the masking gate) | strict masking gate (`--strict`) | ✓ | 716 bytes, present | if unset, `MASKING_SCAN_PATTERNS_FILE` env var must point at it or the strict gate fails closed (exit 1) per existing project convention — not a silent green |
| `discovery_data/crosswalk.json` | canonical merge + D-17 date join | ✓ | 51,324 bytes, present (matches `same_work_spike/probe/rsource` copy) | none |
| Python stdlib (`hashlib`, `sqlite3`, `random`, `math`, `statistics`) | all new code, harness reuse | ✓ | project floor 3.10+ | none needed |

**Missing dependencies with no fallback:** none identified as fully blocking — the one genuinely unverified item (the M-source date table's live accessibility) has a documented, safe degradation path (unknown-date-never-demote) built into the rule itself.

**Missing dependencies with fallback:** M-source date table (falls back to UNKNOWN/never-demote per the rule's own fail-safe design, at the cost of reduced D-17 coverage for M-source works specifically).

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (project-wide; no dedicated discovery-specific config file) |
| Config file | none dedicated — project root `pytest.ini`/config governs; the existing `tests/test_discovery_*.py` suite (9 files) + `tests/fixtures/discovery/discovery-v1-fixture.db` are the direct precedent to extend |
| Quick run command | `pytest tests/test_discovery_bands.py tests/test_discovery_ids.py -q` (no Qt, no Tantivy — fast, safe to run directly per-commit) |
| Full suite command | `pytest tests/test_discovery_*.py tests/render_smoke/ -q` (discovery-scoped full run; project-wide full suite per CLAUDE.md's Qt/Tantivy caveats is a separate, heavier CI-only run not required for this phase's own gate) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| BAND-01 | Values-module label lookup is TOTAL over the frozen enum (no gaps, no orphans) | unit | `pytest tests/test_discovery_band_labels.py -x` | ❌ Wave 0 |
| BAND-02 | Flipping a `band_precision` row's value (fixture-only mutation) changes rendered copy with zero code change | unit (fixture-driven) | `pytest tests/test_discovery_band_labels.py::test_precision_copy_is_data_driven -x` | ❌ Wave 0 |
| BAND-03 | Toggle wording constants exactly match D-11 (EN+HE); no rendering logic yet | unit | `pytest tests/test_discovery_band_labels.py::test_toggle_wording_matches_d11 -x` | ❌ Wave 0 |
| BAND-04 | Disclaimer base sentence exactly matches D-12 (EN+HE) | unit | `pytest tests/test_discovery_band_labels.py::test_disclaimer_matches_d12 -x` | ❌ Wave 0 |
| BAND-05 | Methods section renders all 7 bands with every required field (population/unit/sample size/strata/CI/date/status/report id), bilingual, correctly flag-gated | render-smoke | `pytest tests/render_smoke/test_help_methods_render_smoke.py -x` (model: `tests/render_smoke/test_atlas_render_smoke.py`) | ❌ Wave 0 |
| CERT-01 | Freeze-manifest-before-draw ordering; harness-adapter functions (if any new wrapper code is written) behave identically to the reused E1 functions | unit + manual | `pytest tests/test_cert01_harness_adapter.py -x` for any NEW adapter code; the manifest-before-draw ordering itself is **manual-only** (justification: a research-protocol timestamp discipline, not application code — mirrors how E1's own freeze manifests were verified by inspection, not a unit test) | ❌ Wave 0 (adapter tests only if adapter code is written) |
| CERT-02 | `tier_a` renders "not yet measured" (no bare number) while `precision IS NULL`; every band's rendered copy always pairs a number with a status | unit | `pytest tests/test_discovery_band_labels.py::test_tier_a_shows_no_number_before_cert01 -x` | ❌ Wave 0 |
| v2 build changes (merge/drop/Lever-1/D-17) | Fixture rows exercising: a merge pair, the w001239-equivalent drop, a low-coverage row, a synthetic chronological-demotion cluster (never-orphan-shipped, unknown-date-never-demoted, merge-before-chrono ordering) | unit | `pytest tests/test_discovery_v2_bake.py -x` (extends the existing `tests/test_discovery_build.py`/`test_discovery_schema.py` precedent) | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `pytest tests/test_discovery_band_labels.py tests/test_discovery_ids.py -q` (Track A) or the relevant new `tests/test_discovery_v2_bake.py::test_<specific_case>` (Track B), whichever the task touched.
- **Per wave merge:** `pytest tests/test_discovery_*.py tests/render_smoke/ -q` (full discovery-scoped suite).
- **Phase gate:** full discovery-scoped suite green AND `python scripts/check_atlas_masking.py --scan-repo --scan-sqlite <v2.db> --scan-asset <v2.db> --strict` exit 0 (mandatory, not optional — this is a hard release gate per DATA-05, not merely a test) before `/gsd:verify-work`.

### Wave 0 Gaps

- [ ] `tests/test_discovery_band_labels.py` — covers BAND-01/02/03/04, CERT-02 (new file; no existing coverage for the not-yet-authored values module)
- [ ] `tests/render_smoke/test_help_methods_render_smoke.py` — covers BAND-05 (new file; model directly on `tests/render_smoke/test_atlas_render_smoke.py`'s existing shape)
- [ ] `tests/test_discovery_v2_bake.py` (or an extension of `tests/test_discovery_build.py`) — covers the 4 v2 build changes + the never-orphan/unknown-date/merge-before-chrono invariants (new fixture rows needed in a v2-analog of `discovery-v1-fixture.db`)
- [ ] Extend `tests/test_discovery_ids.py` — new golden-digest/enum-membership assertions once `routing_reason` gains `later_shared_text` (Pitfall 1) and (if Assumption A2 resolves to "yes") the `routing_status` lattice tier (Pitfall 2)
- [ ] `tests/test_cert01_harness_adapter.py` — ONLY if new adapter code is written to point the E1/Q2 scripts at the v2 frame (if the harness is invoked with zero new Python code — e.g. purely via CLI args pointing at a new DB path — this gap may not apply; confirm at plan time)

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | No new auth surface — methods page is public-readable content like the rest of Help; no login state involved |
| V3 Session Management | no | No new session state |
| V4 Access Control | no | No new role/permission boundary; the discovery flag is a feature gate, not an access-control mechanism |
| V5 Input Validation | no | No new user-input surface this phase (no forms, no new query params beyond existing page routing) |
| V6 Cryptography | partial | `frame_content_hash`/`content_hash`/`evidence_id`/`claim_id` are SHA-256 CONTENT-INTEGRITY hashes (tamper-evidence for the frozen artifacts), not a cryptographic secrecy boundary — never hand-roll a new hash recipe; extend `scripts/discovery_ids.py`'s existing frozen recipes only |

The operative threat model for this phase is **not** a classic web-application attack surface — it is (a) provenance/data-exfiltration (M-source masking) and (b) integrity of frozen contract artifacts (enum/schema tamper-evidence), both already governed by existing Phase 134 machinery that this phase extends rather than invents.

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| M-source title/date leaking via a sidecar cell value, an error message, or a newly-authored spec doc (the bake-plan rewrite, `discovery-frames-v2.md`) | Information Disclosure | `scripts/check_atlas_masking.py --scan-sqlite --scan-asset --scan-repo --strict`, extended with M-source date-table vocabulary registration (Pitfall 6); every touched doc re-scanned before commit |
| A hand-edited or drifted `discovery.db` silently shipping a stale v1 enum value, or a `routing_reason` value outside the (amended) frozen set | Tampering | `scripts/verify_discovery_sidecar.py`'s CHECK constraints + enum-membership assertions + `frame_content_hash` verification; fail-closed loader in `web/discovery_assets.py` |
| A demoted (`review_only`) evidence row silently becoming the ONLY reachable evidence for a claim that also has a shipped sibling row (Pitfall 2) | Tampering (data-integrity) | New verifier invariant: assert no claim's shipped evidence exists while `display_evidence_id` points at a `review_only` row |
| CERT-01's freeze manifest being edited after cards are drawn (undermining the pre-registration's whole point) | Repudiation | Commit the freeze manifest to git (or an equivalent tamper-evident store) BEFORE the deck is rendered; the manifest's own content hash is the repudiation-resistance mechanism, mirroring `e1_r2_freeze.json`'s discipline |
| A raw M-source title accidentally quoted in a Codex-review brief, a scratchpad file, or an uncommitted `tmp/*.md` (the exact recurrence pattern already flagged once in Phase 134's deferred-items) | Information Disclosure | Treat `tmp/` and scratchpad Codex-review artifacts with the SAME masking discipline as committed docs; STATE.md already flags this as a recurring risk — do not assume "uncommitted" means "safe" |

## Sources

### Primary (HIGH confidence — read directly from the repo this session)
- `.planning/phases/135-precision-certificate-confidence-bands/135-CONTEXT.md` — full decision set D-01..D-19
- `.planning/phases/135-precision-certificate-confidence-bands/135-CODEX-CRITIQUE.md` — adversarial review, 3 BLOCKER/9 HIGH/2 MEDIUM
- `.planning/REQUIREMENTS.md`, `.planning/STATE.md`, `.planning/ROADMAP.md` — requirement text, project history, phase goal/SC#1-5
- `.planning/phases/134-discovery-data-spine/134-CONTEXT.md` — the C-1..C-9 two-table contract this phase's schema extends
- `docs/specs/discovery-band-labels-v1.md`, `discovery-v2-bake-plan.md` (confirmed stale), `discovery-frames.md`, `discovery-budgets.md`, `discovery-sidecar-schema-v1.md`, `discovery-deploy.md`
- `same_work_spike/probe/rsource/results/chronological_demotion_rule.md` (D-17 bake spec, gitignored, read directly)
- `same_work_spike/probe/rsource/data/v2_canonical_merges.json` (the real census artifact — inspected structurally: 16 merges, 1 contested, 174 unloaded provisional, 8 residual, 1 drop, all field names and non-title values verified directly)
- `scripts/build_discovery_sidecar.py`, `scripts/verify_discovery_sidecar.py`, `scripts/discovery_ids.py` (full function inventory read directly)
- `web/discovery_assets.py`, `shared/discovery_service.py` (band-rank/loader code read directly)
- `web/pages/help.py`, `web/main.py` (Help page + `/help`/`/atlas` route structure read directly)
- `same_work_spike/probe/scripts/e1_deck.py`, `e1_confirm_sizing.py`, `e1_band_frame.py`, `e1_r2_confirm.py`, `e1_r2_audit.py` (function inventories + `comp_bootstrap`/`wilson_bounds` bodies read directly, confirmed stdlib-only)
- `same_work_spike/probe/results/PLAN-e1-round2.md`, `E1-ROUND2-RELEASE.md`, `E1-ROUND3-RELEASE.md` (full text read — the direct template for CERT-01's pre-registration + outcome-branch mechanics)
- `tests/test_discovery_bands.py`, `tests/test_discovery_ids.py` (existing test patterns/precedents read directly)

### Secondary (MEDIUM confidence)
- None — every claim in this document traces to a directly-read repo file or gitignored research artifact this session; no WebSearch was needed (this is an entirely internal, project-specific research task).

### Tertiary (LOW confidence)
- None.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new packages; every reused module/function was read directly, including confirming the E1/Q2 harness is stdlib-only.
- Architecture (Track A, display): HIGH — the Help page structure, anchor convention, and flag-gating precedent were all read directly and are simple, low-risk extensions.
- Architecture (Track B, v2 bake): MEDIUM — the mechanism is well-precedented, but the routing_reason schema amendment and the `select_display_evidence` routing_status-tier question are NEW findings from this research session, not yet validated against real v2-candidate data. (DELTA is no longer open — resolved 2026-07-23 to 100y, D-19 audit delivered; see the orchestrator correction at top.)
- Pitfalls: HIGH — every pitfall traces to either a direct schema/code inspection (Pitfalls 1, 2, 3) or an explicit CONTEXT.md/Codex-critique statement (Pitfalls 4, 6, 7) or a direct quote from the E1 protocol's own stated limitations (Pitfalls 5, 8).

**Research date:** 2026-07-23
**Valid until:** ~14 days (fast-moving — the leadoff v2-bake-plan rewrite is Codex-gated; its one former external dependency, the D-19 date-coverage audit setting DELTA, has since landed (2026-07-23, DELTA=100y) and is folded into the orchestrator correction at top)
