# Phase 135 — Codex adversarial review of CONTEXT (D-17 focus)

**Reviewer:** codex-cli 0.144.4 (adversarial)
**Date:** 2026-07-23
**Inputs reviewed:** `135-CONTEXT.md`, `docs/specs/discovery-v2-bake-plan.md`, `docs/specs/discovery-band-labels-v1.md` (masking-clean set only; restricted trees excluded by brief)
**Brief:** scratchpad `135-codex-brief.md` — focus on D-17 (the new date-driven rule) as an unresearched option.

## VERDICT: REWORK

3 BLOCKER, 9 HIGH, 2 MEDIUM. Masking self-check on the raw output: 0 leak hits.

---

## BLOCKERS

**1. D-13 / D-16 / D-17 — The v2 bake plan does not contain the decisions it is supposed to implement.**
CONTEXT says the census is delivered (16 merges, w001239 drop/canonical flip, a rule-generated relation set), but `discovery-v2-bake-plan.md` still says census-blocking, lists the older 7-merge set + 3 hand-curated relations, and has no date ingestion / interval semantics / missing-date policy / diagnostic-family definition / verification gates. Building from it would produce a *different* v2 than the one CERT-01 samples.
**Fix:** Revise the bake plan FIRST — executable D-17 spec + all 16 merges + resolve D-16 (output shape, shipped-work filter, ref stability) — then a fresh adversarial review of the revised plan + build diff before baking or freezing CERT-01.

**2. D-17 — Chronology + shared text cannot populate *semantic* `work_relations`.**
The signal establishes at most "A is later than B and overlaps B." It does NOT distinguish embeds / abridges / base_text / quotation / commentary-lemma / translation / common-source / formulaic reuse / interpolation / independent parallel. Calling the rule's output `work_relations` converts an unvalidated hypothesis into product data.
**Fix:** Keep owner-ratified semantic relations in `work_relations`. Put rule output in a SEPARATE `relation_candidates` diagnostic artifact (fields: date provenance, parsed intervals, overlap evidence, rule version, status, reason) with a neutral type like `later_shared_text_candidate`. Do not expose or use it operationally until its PPV + error modes are measured.

**3. D-02 / D-07 / D-17 — Release sequencing violates the default-display contract.**
`discovery-band-labels-v1.md` §4 says "show all shipped by default" is trustworthy ONLY together with direction-aware quotation routing. D-17 defers that routing to v2.1; D-02 lets Phase 135 close when grading merely *starts* and allows Phase 136 claim surfaces before CERT-01 completes. So `tier_a` could be the default view while unmeasured AND still carrying the known high-coverage quotation residual. If CERT-01 later fails, users have already seen it under the default posture.
**Fix:** Choose ONE coherent sequence — either land validated Lever 2 before default claim surfaces, OR keep `tier_a` behind the "possible matches" toggle until CERT-01 passes. Do not treat "grading started" as sufficient for a default-view trust decision.

---

## HIGH

**4. D-17 — Date coverage is unmeasured; could disable the rule for ~half the inventory.** 508 Sefaria + 106 JA = 614 works (48.3%) lack a direct M-source date; inheritance rate unmeasured. Pair coverage degrades ~`1-(1-u)^2`. **Fix:** masking-clean coverage audit on the exact shipped canonical frame (dated-direct / inherited-by-ratified-twin / ambiguous / conflicting / missing, by corpus and by real shared-text pairs) BEFORE adopting the rule; an undated pair → `UNKNOWN` (no direction, no label), never "contemporaneous/independent."

**5. D-17 — Descriptive-date parsing can fabricate or reverse direction.** "after N" as a point; midpoint of "between N and M"; Hebrew vs civil calendar; BCE/CE + year-zero; wrong century endpoints; floruit vs composition vs redaction vs copy date; composite works with no single date. **Fix:** parse to provenance-bearing intervals (open/closed, unbounded); assert "A later than B" only when strictly separated (`earliest(A) > latest(B)`, optional margin); overlapping/touching/unbounded/unparseable/composite → `UNKNOWN` → residual queue. No point estimates/midpoints for direction.

**6. D-17 / D-08 — The diagnostic family can contaminate CERT-01 even without routing.** A work-level temporal relation doesn't prove the sampled page's match IS the quoted subspan; "quotation-FP candidate" is the output of the unvalidated classifier under test. If the label changes sampling/exclusions/reveal/grader expectations it biases the estimate; missing dates make it corpus-selective (esp. JA); reporting its error rate as quotation-FP prevalence is circular. **Fix:** freeze parser/rule/thresholds/missingness/universe before the draw; graders blind to the label + dates; draw the primary sample independently (or record inclusion probabilities + design weights); report the family only as classifier validation (coverage/PPV/sensitivity vs independently-graded quotation status, by date-provenance class).

**7. D-05 / D-08 / D-13 — Estimand is ambiguous after canonical merging.** Spine keeps raw `(page_id, work_id)`; display collapses to `(page_id, canonical_work_id)`. Sampling raw rows double-counts twins + measures a population users never see; after collapse the `corpus` stratum is undefined without a deterministic pre-grading selection rule. **Fix:** define the estimand explicitly as the shipped, display-deduplicated `(page_id, canonical_work_id)` population; freeze how multiple evidence rows pick band/routing/display-evidence/corpus-stratum; sample only AFTER canonicalization + drop + coverage routing + dedup.

**8. D-17 — Systematic multi-register / textual-layer failures, not random noise.** Counterexamples in-domain: Bible+Targum/Onkelos; Bible+JA Tafsir; Bavli+Mishnah lemmata; commentary MSS with copied base-text lemmata; Rif–Bavli; MT Zmanim–Haggadah / Yalkut–Midrash Tehillim (embedded unit vs separately transmitted vs third-source dependence). Errors concentrate in structured genres → a date rule is not inherently safer than the density router. **Fix:** chronology is only ONE feature; routing must require span containment/overlap + register/language + direction-specific ref-subspans + family validation; preserve non-overlapping multi-register claims categorically.

**9. D-17 — Crosswalk inheritance is vulnerable to false identity + circular inference.** Title collisions, tractate-vs-whole-work, recensions, variant attributions, 1-to-many / many-to-1, missing namespaces, multiple M-source IDs per work. Inheriting a date THROUGH a text-similarity crosswalk is doubly dangerous — the same overlap both supplies the date and "proves" the relation. **Fix:** join only on stable source ids (namespace + cardinality constrained), never normalized titles; inherit only through owner-ratified twin links independent of the D-17 overlap; hard-fail unexpected cardinalities; require compatible intervals or mark ambiguous; retain provenance.

**10. D-14 / D-17 — Merges can create self-relations; RCh-Shabbat can be falsely converted to a dependency.** Twin dates may differ/be missing; generating candidates before canonical collapse can create a twin relation that becomes a self-loop. The Hai/RCh-Shabbat texts stay standalone because attribution is *contested* despite matching text — a date rule could reinterpret that as "later cites earlier," undoing D-14 by another mechanism. w001239 drop risks dangling candidates/diagnostics. **Fix:** resolve dates at canonical-group level with cross-member consistency checks; canonicalize endpoints before generation; reject self-relations; purge dropped endpoints; keep contested-identity pairs on an explicit exclusion/residual status; verifier checks for self-loops / dropped ids / conflicting twin dates / duplicate relations.

**11. D-07 / D-10 — Fail action conflicts with the display rules AND the terminology contract.** D-07 says rebanding tier_a→screening moves it behind the toggle, but the display contract shows every `routing_status='shipped'` claim by default — rebanding without changing routing may hide nothing. CONTEXT also wrote "remaining certified bands," but "certified" is prohibited (D-06 / band-labels Rule 1) and the top algorithmic band has no completed CI. **Fix:** one executable default-eligibility predicate, test pass AND fail branches; if failure = hide, set `routing_status='review_only'` or a versioned default-visible band set (not an enum rename alone); replace "certified bands" with the approved posture; every default band must meet its own evidence standard.

**12. D-05 / D-07 / D-08 / D-09 — CI + decision rule need a full survey-design spec.** PhysMS clustering is right but insufficient: need inclusion probabilities, MSS crossing strata, within-stratum resampling, claim weights, uncertain physMS linkage, effective cluster count; a naive cluster bootstrap misstates uncertainty when a few MSS dominate; the pre-reserved confirmation draw is optional-stopping risk unless the two-stage rule + error spending are frozen; an aggregate lower bound can pass while a small stratum fails. **Fix:** freeze a survey-aware estimator + stratified PSU bootstrap (physMS = PSU, population weights preserved); publish cluster counts / size concentration / effective n / confidence level / one-vs-two-sided / bootstrap method / confirmation trigger; preregister per-stratum minimum sample + guardrail reporting.

## MEDIUM

**13. D-07 / D-08 — Wholesale demotion may be driven by imprecision, not low precision.** With ~200–250 clustered cards, a band with point estimate > 0.85 can fail on a wide lower bound. **Fix:** use the OC table to guarantee power under plausible design effects; distinguish "estimated precision below threshold" from "insufficient evidence" — the latter keeps tier_a non-default pending the confirmation draw rather than permanently relabeling to screening.

**14. D-17 — Pair-generation universe + overlap threshold unspecified.** "Shared text" is undefined (co-claim / reference overlap / Jaccard / matched-letter containment / span intersection) → combinatorial graph dominated by boilerplate/prayers/biblical phrases/legal formulae; post-hoc threshold change breaks preregistration. **Fix:** freeze candidate universe, minimum distinctive span, containment/asymmetry metrics, formulaic-text exclusions, dedup, max fan-out BEFORE the v2 frame freezes; report candidate counts/rates by corpus / relation family / date status / canonical work.
