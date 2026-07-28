# v9.0.0 Requirements Addendum — ✅ APPROVED AND APPLIED 2026-07-28

**Status: APPLIED to `.planning/REQUIREMENTS.md`. Coverage moved 40 → 44.**
This file is now the rationale record; `REQUIREMENTS.md` is authoritative.

**Owner decisions, 2026-07-28:**

| Item | Decision |
|---|---|
| A. NOVEL-01 / NOVEL-02 | **Registered**, but homed **AFTER Phase 136** — 136 stays read-surfaces-only. The novelty axis needs the LLM title-gate rewired to the validated `gemini-3.6-flash` + `reasoning:{effort:"low"}` config, the heuristic funnel run, and the 254,612-row `track1_direct` coverage gap closed; that is a phase of work in its own right. Traceability rows read `Post-136 (owner: not in 136)` pending a phase assignment. |
| B. VIS-01 | **Registered, homed Phase 136** (not a 135 reopen — 135 is closed and its asset deployed). VIS-02 stays Phase 139 as drafted. |
| C. REL-01 CURATED-SURFACE EXCEPTION | **GRANTED as drafted, all eight conditions (a)–(h)**, appended verbatim to REL-01. Phase 136's read surfaces may deploy early behind a dedicated flag. |
| C. open sub-question — per-claim human confirmation | **NOT required.** The honesty load is carried by (c), (d), (f) and the BAND-03/04 framing; a banded, disclaimered "possible identification" is a different speech act from a certified claim. The owner accepts that a future gen-2 reclassification (~32K–45K publishable rows moving same-work → quotation) is handled as a correction under (f)'s retraction policy rather than pre-empted by per-claim review. |
| D. bookkeeping | Four traceability rows added; coverage 40 → 44. All three pre-existing inconsistencies fixed: DATA-01/02 contract correction **ratified**; DATA-08 reworded to a working target with a recorded size re-acceptance (368.5 MB v1 / ~370 MiB v2 accepted; the only hard budget is PERF-01's RSS ≤ 250 MB, measured at 11.2 MB); the `/leads` phase reference in `discovery-budgets.md` corrected to 138. |

**One premise changed after drafting:** section C was written 2026-07-27 while CERT-01 was ungraded.
CERT-01 was graded and PASSED on 2026-07-28 (weighted 0.9382, CI [0.9084, 0.9644] vs the 0.85
Strict floor). Condition (c) — no population-level `tier_a` precision number — nevertheless still
binds, because the certificate is not yet PUBLISHED: `band_precision` has not been re-baked and the
CERT-02 outcome copy is unapplied. Revisit (c) when those land.

---

## Original draft text (retained as the rationale record)

**Why this exists:** two features are now central to what v9.0.0 ships and **neither has a
requirement ID**. In a milestone whose release gate (REL-01) is requirement-driven and
traceability-checked, an unregistered feature does not get planned, does not get success criteria,
and does not get verified — it simply falls through. Source of the decisions:
`.planning/v9-PUBLICATION-STRATEGY.md` (owner, 2026-07-27).

**Masking:** restricted corpora appear ONLY as **M-source** / **R-source**.

---

## A. New requirement block — Novelty Axis

Proposed placement: a new `### Novelty Axis` block after `### Bands & Certification`.
Proposed phase homing: **NOVEL-01 → Phase 136** (it is a display axis on the read surfaces);
**NOVEL-02 → Phase 136** (same surfaces, but it is the masking/provenance half and could equally
sit in 139 with the other cross-cutting gates — owner's call).

- [ ] **NOVEL-01**: Every claim carries a computed **novelty flag** decided per `(sys_id, work)` — NOT
  per work — by checking whether ANY available finding aid already ties THAT fragment to THAT work.
  The checked source set is enumerable, versioned, and recorded in the sidecar `meta`: FJMS and NLI
  catalogue + bibliography, titles, PGP, FGP, and M-source shelfmark attributions. The flag is
  computed for **ALL evidence families** (`track1_direct` AND `propagated`) — the frozen v2 asset
  computes it only for `propagated`, leaving all 254,612 `track1_direct` rows at `is_new = 0`, which
  is a coverage gap, not a result. Public display wording is **"not identified in any available
  finding aid" / "לא מזוהה באמצעי העזר הקיימים"**, shown with the checked-source list and the
  as-of date, and with the owner's confidence estimate stated AS an estimate. The wordings
  "new discovery" / "new" / "unknown to scholarship" are PROHIBITED on public surfaces (they assert
  a construct the check cannot establish). Novelty is a filterable axis on the panel and
  `/work/{id}`, and is **structurally orthogonal to the confidence band**: it must never feed band
  assignment, precision copy, ranking weight, or certified styling — absence from a finding aid is
  not evidence a claim is correct (the catalogue-never-evidence rule, applied in reverse).
- [ ] **NOVEL-02**: The novelty flag's **provenance** (`known_source` — which aid already had it) is
  masked on the public side: the boolean is publishable, but a restricted-corpus provenance value
  collapses to a non-identifying label (e.g. "recorded in a restricted corpus"), never the corpus
  name, and passes the DATA-05 masking scan on every surface that renders or exports it — including
  copy/clipboard output, JSON payloads, and error paths. Public surfaces therefore support
  "filter **and explain**" only where the explaining source is itself public; elsewhere they support
  "filter only". The heuristic-plus-LLM funnel's verdict cache is a build-time artifact and is never
  shipped in the sidecar.

**Notes for the planner (not requirement text):** cost is measured at ~$27 one-time / ~15 min
parallelized over ~61K distinct (work, catalogue-title) pairs with `google/gemini-3.6-flash` +
`reasoning:{effort:"low"}`; verdicts cache, so re-bakes pay only for new pairs; do NOT downgrade the
model (a weaker flash model scored 62.5% agreement vs 100%). Always read real `usage.cost`.

---

## B. New requirement block — Public/Private Visibility

Proposed placement: a new `### Public/Private Visibility` block after `### Claim Model & Data Spine`.
Proposed phase homing: **VIS-01 → Phase 135** (it is a bake/packaging capability and the frozen v2
asset already carries the required carrier fields) or **Phase 136** if the owner prefers not to reopen
135; **VIS-02 → Phase 139** (it is a release-gate verification).

- [ ] **VIS-01**: The bake emits **two assets from one normalized build** via deterministic
  projections — a PUBLIC asset restricted to open-licensable provenance (launch scope: Sefaria-direct
  matches ∪ all MS-relationship/`propagated` claims) and a PRIVATE asset carrying the full register —
  each with its own manifest, content hash, release-contract row counts, and per-projection invariant
  tests. M-source / JA / R-source remain permitted private **inputs** (dating, deduplication,
  canonicalization) while being excluded from public **outputs**. Exclusion is **structural**: private
  rows are ABSENT from the public artifact, never merely hidden by a UI filter or a query predicate.
  The projection keys on the origin of the DISPLAYED assertion, not on `works.source_corpus` alone,
  which may be insufficient after cross-corpus merge/dating/canonicalization. Implementation is ONE
  gate at the packaging boundary — never duplicated in the matching engine — and adding a new
  `source_corpus` / `evidence_source` code remains a dated amendment to
  `docs/specs/discovery-sidecar-schema-v1.md`.
- [ ] **VIS-02**: A release-gate check proves the public asset is free of restricted provenance: the
  DATA-05 scan runs over the public asset's schema AND all cell values, plus every surface that reads
  it (rendered pages, copy/export output, JSON payloads, SEO/JSON-LD, sitemap, error messages), and a
  positive control confirms the scan would FAIL on a deliberately seeded restricted row (no silent
  green). A public/private row-count reconciliation is recorded in the frame artifact.

---

## C. Amendment to REL-01 — proposed CURATED-SURFACE EXCEPTION

**This is the decision that sets Phase 136's success criteria, so it should be settled before 136
planning, not during it.**

REL-01 today holds the main discovery flag, sitemap/SEO discovery, and the homepage band OFF until
CERT-01 is graded to completion (plus methods report, CERT-02 copy, and the masking/RTL/a11y/perf/
deployment checks). Read plainly, REL-01 gates **public claim surfaces**, not merely those three
switches — so putting Phase 136 in front of real visitors before CERT-01 completes needs an explicit
exception, on the same pattern as the ATLAS-PREVIEW EXCEPTION the owner already granted.

Proposed text to append to REL-01:

> **CURATED-SURFACE EXCEPTION (owner, 2026-07-__):** the Phase 136 read surfaces (PANEL-01/02/03,
> WORK-01/02) may deploy EARLY, before the CERT-01 measurement is graded to completion, PROVIDED:
> (a) they sit behind a DEDICATED surface flag distinct from the main discovery flag, and the main
> discovery flag, the homepage discovery band, and sitemap/SEO discovery all stay OFF;
> (b) they are `noindex` until the full gate;
> (c) they display NO population-level `tier_a` precision number (band label + BAND-05 methods link
> only), and CERT-02's prohibition on unmeasured numbers holds unchanged;
> (d) they render ONLY `routing_status='shipped'` display rows — a `review_only` row is never
> presented as an identification;
> (e) they are served from the PUBLIC projection (VIS-01), so restricted provenance is structurally
> absent rather than filtered;
> (f) BAND-01 band labels, BAND-03 screening-toggle defaults, BAND-04 recall-honesty disclaimers,
> and the BAND-05 methods page are all live on them from line one, together with a published
> data-as-of date and a correction/retraction policy;
> (g) the DATA-05 masking scan, I18N-02 RTL/bidi checks, A11Y-01/02 checks, and the PERF-01 caps pass
> **for these surfaces**;
> (h) the algorithmic band is preserved as its own field — any editorial or human review status is
> recorded ALONGSIDE it and never silently rewrites it.
> The full REL-01 gate continues to govern public promotion, indexing, the homepage band, and the
> main discovery flag.

**Open sub-question for the owner (Codex pressed on this and it is not settled):** whether the
exception should additionally require **per-claim human confirmation** of everything displayed.
Arguments both ways:
- *For:* without it, an early surface shows ~230K unreviewed `tier_a` claims, and gen-2 is projected
  to reclassify roughly 32K–45K of the publishable rows (mostly "same work" → quotation), which is a
  retraction if they were first presented as identifications.
- *Against:* it does not scale to 240K rows, and the honesty load is already carried by (c), (d), (f)
  and the BAND-03/04 framing — an explicitly banded, disclaimered "possible identification" surface
  is a different speech act from a certified claim.
- *Middle option:* require per-claim review only for anything **promoted** (homepage, exhibition,
  announcement), never for the general banded browse surfaces.

---

## D. Traceability rows to add on approval

| Requirement | Phase | Status |
|-------------|-------|--------|
| NOVEL-01 | Phase 136 | Pending |
| NOVEL-02 | Phase 136 | Pending |
| VIS-01 | Phase 135 *(or 136 — owner's call)* | Pending |
| VIS-02 | Phase 139 | Pending |

Coverage line becomes: **44 / 44 v9.0.0 requirements mapped** (still no orphans, no duplicates; the
set continues to skip DATA-09 by design).

Also worth fixing while REQUIREMENTS.md is open (pre-existing, unrelated to this addendum):
- The Phase-134 CONTRACT CORRECTION superseding DATA-01/DATA-02 is still marked *"pending owner
  ratification"* even though the corrected two-table model is what actually shipped and was verified.
  It should be ratified or explicitly re-opened.
- DATA-08 is worded as *"a disk budget (≤ 300 MB)"* and marked Complete, but `discovery-frames.md`
  records the owner accepting 368.5 MB with the note that *"the ≤300 MB figure was a working target,
  not a numbered cap — the only hard budget contract is RSS ≤ 250 MB."* The frozen v2 asset is
  ~370 MiB. Either reword DATA-08 or record a size re-acceptance.
- `docs/specs/discovery-budgets.md:140` says *"Phase 136 ships the `/work/{id}` + `/leads`
  surfaces"*, while ROADMAP.md and the traceability table place `/leads` (LEADS-01/02) in Phase 138.

---

*Drafted 2026-07-27 from `.planning/v9-PUBLICATION-STRATEGY.md`. Not applied.*
