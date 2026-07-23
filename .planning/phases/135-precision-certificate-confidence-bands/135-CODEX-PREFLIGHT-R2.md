## Section A: Prior-finding status

#1 — PARTIAL — `135-04-PLAN.md:Task 2` records the current bake-plan SHA-256, but its verifier accepts `VERDICT: APPROVE` anywhere in the review rather than requiring it to be the final non-empty line; an earlier quoted APPROVE followed by REWORK still passes.

#2 — PARTIAL — `135-03-PLAN.md:Task 1` and `135-09-PLAN.md:Task 1` move protocol/manifest/OC artifacts into tracked paths, but the manifest does not explicitly record `protocol_sha256`, is not frozen in a separate pre-draw commit/artifact, and is mutated after the draw in Task 2.

#3 — PARTIAL — `135-09-PLAN.md:Task 3` is now a blocking human checkpoint with the requested six mechanical checks, but its report/freeze integrity depends on the internally inconsistent mutable manifest design identified in Section B.

#4 — RESOLVED — `135-06-PLAN.md:Task 1` threads `cross_corpus_map` through work insertion, claim assembly, and D-17 grouping by `canonical_work_id`, with an overlapping merged-twin no-demotion fixture.

#5 — PARTIAL — `135-04-PLAN.md:Task 1`, `135-06-PLAN.md:Tasks 2–3`, and `135-07-PLAN.md:Task 1` add required composition-date arguments, hashing, release coverage, and routing audit; however, the parser/file schema remains merely “functional,” and the separately consumed `seftja_dates.json` has no required CLI argument, hash pin, or recorded provenance.

#6 — RESOLVED — `135-06-PLAN.md:Task 2` defines D-17 over currently shipped spans, prohibits promotion, preserves Lever-1 routing, and tests earliest-low-coverage/later-shipped and multi-cause cases.

#7 — PARTIAL — `135-06-PLAN.md:Task 2` adds a tested outcome-application path, but “reband tier_a → screening” still names no legal stored band key; the frozen enum contains `screening_rb` and `screening_canon`, not `screening`.

#8 — PARTIAL — `135-05-PLAN.md:Tasks 1–3` now includes `discovery-frames.md`, §2/§4 amendments, and v1/v2 runtime compatibility, but the contract’s literal “one commit / one bake” remains contradicted by three separately committed GSD tasks followed by build logic and the bake in later plans.

#9 — PARTIAL — `135-02-PLAN.md:Task 1` correctly distinguishes population, draw size, determinate count, successes, collection scope, and registry metadata; however, Plan 02 runs in Wave 2 while its required `discovery-frames-v2.md` population source is not produced until `135-07-PLAN.md:Task 2` in Wave 4.

#10 — RESOLVED — `135-01-PLAN.md:Tasks 2–3` omits wholly absent intervals, rejects partial intervals, and introduces a data-driven `band_measurement_status` consumed by precision copy and default eligibility.

#11 — RESOLVED — `135-02-PLAN.md:Tasks 1–2` independently gates both Help bodies, adds supported async wrappers, converts the route to async, and patches the real `web.pages.help` and `web.main` call sites.

#12 — PARTIAL — `135-08-PLAN.md:Task 1` now specifies candidate-manifest staging, staged-target verification, prior-manifest preservation, atomic swap, restart, and smoke; its automated log gate nevertheless accepts any one 64-hex value from the frame document rather than requiring both labeled `content_hash` and `frame_content_hash`.

#13 — PARTIAL — `135-03-PLAN.md:Task 1` requires frozen SQL, physical-MS mapping, and stratum tie-break, while `135-09-PLAN.md:Task 1` records population/stratum/allocation fields; no retained validator recomputes the population hash/counts, and Task 3 does not validate strata or gold/confirmation allocations against the deck.

#14 — RESOLVED — `135-03-PLAN.md:Task 1` and `135-09-PLAN.md:Task 2` specify a separately identified blinded sample spanning demoted and retained candidates, with the hidden tag joined only after verdict lock.

#15 — RESOLVED — `135-01-PLAN.md:Tasks 2–3` now provides and invariant-tests the band-bearing serializer/presentation object and central D-18 default-eligibility predicate.

#16 — RESOLVED — Adjudicated false positive as directed; `135-04-PLAN.md:Task 1` correctly documents that the referenced name is the Sefaria-side canonical name, not the restricted M-source title.

#17 — PARTIAL — `135-07-PLAN.md:Task 1` resolves the exact DB from the manifest and removes the DB glob, but Plans 03–09 still contain numerous `&&` chains despite `135-VALIDATION.md` claiming every command avoids them.

#18 — RESOLVED — `135-02-PLAN.md:Task 2` adds `discovery_methods_noindex()` with explicit pre-release, REL-01-released, and discovery-off tests.

#19 — RESOLVED — `135-05-PLAN.md:Task 3` uses “CERT-01 gate passes” wording and explicitly rejects the prohibited wording.

## Section B: New findings

- [BLOCKER] — `135-09-PLAN.md:Tasks 1–3` — The freeze manifest is self-referential and mutable: Task 1 says its content hash is stored inside itself as `report_id`, which has no ordinary finite construction, and Task 2 then adds `deck_manifest_hash`, changing the bytes and invalidating that report ID. This also cannot prove the manifest preceded the draw. — Freeze an immutable canonical preregistration payload whose hash excludes its hash field, record `protocol_sha256`, and commit it before drawing; put `deck_manifest_hash` in a separate tracked deck manifest referencing the immutable preregistration hash.

- [HIGH] — `135-06-PLAN.md:Task 1 / 135-07-PLAN.md:Task 1` — The authoritative `v2_canonical_merges.json` is said to feed the release build, but no merge-input CLI argument, parser schema, SHA-256 pin, release gate, or meta/frame provenance is specified; the real builder currently has no such input. — Add required `--canonical-merges` and hash arguments, define and validate the exact accepted schema/ratification fields, thread them through `finalize_build`, and record the verified hash in `meta`, the manifest, and the v2 frame.

- [HIGH] — `135-01-PLAN.md:Task 2 / 135-05-PLAN.md:Task 1 / 135-06-PLAN.md:Task 2` — Tier-A default eligibility trusts the stored `measurement_status="measured_pass"`, while the new column has no enum/consistency constraint and no verifier ties that status to the strict `ci_low >= 0.85` result. A contradictory precision spec could therefore default-show tier_a despite failing the locked gate. — Add closed status vocabulary and cross-field verification, derive the outcome from the hash-bound analysis result and CI, reject contradictory specs, and test that `measured_pass` with missing or sub-0.85 LCB fails closed.

VERDICT: REWORK