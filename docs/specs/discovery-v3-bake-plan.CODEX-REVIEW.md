# Codex review — discovery-v3 bake plan

## BLOCKER — `docs/specs/discovery-v3-bake-plan.md` §§1.2, 3.1a, and 3.2: the proposed adapter cannot deliver `w_start`/`w_end`

The statement that a `track1_matches`-shaped view plus `shadowed_by` and `source_corpus` is the whole ingest is false for the stated offset debt. `scripts/build_discovery_sidecar.py::_ingest_tier_a` reads `spans_json` and emits the largest page-side span only; it neither selects nor reads `ref_spans_json`, `ref_start`, or `ref_end`. `source_corpus` is not read at all: source assignment is recalculated from `cat` by `_assertion_source_corpus` / `_map_cat_to_source_corpus`.

The producer makes the mismatch material. `gen2_track1_pilot.py::assign_page_rich` stores multiple dual-side spans in one 14-column row, while `gen2_evidence_ingest.py::_iter_ref_spans` expands each one into an evidence row with its own exact reference coordinates. The v2 amendment requires exact work-side offsets, not an unspecified projection of a multi-span match; it also distinguishes the old propagated B-side offsets from this new coordinate. A simple scalar join from `g_launch3` therefore has no defined choice when the builder’s largest merged page span corresponds to multiple underlying reference spans.

Required change: specify and implement a deterministic page-span-to-reference-span projection (or preserve multiple direct evidence rows), carry the selected `norm_stream` coordinates through the real ingest, and add a parity gate covering multi-span rows. The adapter must be tested against exact producer evidence, not merely non-NULL output columns.

## BLOCKER — `docs/specs/discovery-v3-bake-plan.md` §§3.1a, 3.4, and 6: the bake drops the gen-2 coverage-router semantics

The claim that the builder need only read two research tables omits a required *semantic* input, even if the Python connection touches only those tables. The handoff defines the two v3 surfaces from `coverage_route` at `(page_id, canonical_work_id)` grain and gives that router its own threshold and provenance. The current builder does not read that table. It recomputes coverage from `pages.text` and `track1_matches.matched_letters`; when dates are supplied, `build_claims_and_evidence` applies the v2 Lever-1 rule before D-17.

Thus inheriting the v2 order is safe only after proving that the operator, threshold, grain, and pre-D-17 population are the same. The plan establishes none of those equivalences and its row-count gate cannot establish them. Re-running a different router while labelling its output as the validated v3 two-surface result is not an ingest.

Required change: either ingest the v3 routing result with a declared mapping and parity checks, or explicitly make a new routing decision, validate it, and stop claiming the handoff’s measured v3 surface counts and quality apply. Re-derive the D-17 order against the chosen router; do not inherit it by assertion.

## BLOCKER — `docs/specs/discovery-v3-bake-plan.md` §§1.4a and 4: cache reuse does not prove an unchanged question

The reversal is unsound. `scripts/discovery_novelty_production_run.py::render_case` sends the claimed title and author as well as the finding-aid bundle. `scripts/discovery_novelty_probe.py::build_all_candidates` obtains those claim-side values from the baked `works` row (`neutral_title`, `author`). Same `sys_id` plus the same `w######` / alias representative does not pin either value.

It also does not pin the alias-groups artifact used by `build_novelty_grain_index`, nor the external finding-aid databases at the time of the later run. A title/author correction, an alias-group edit, or any changed source text can change the heuristic and model question while the lookup key still hits. None is an in-place re-graining. The code confirms that band, coverage, routing, matched-letter count, competing works, and span text are not rendered into the model prompt; that limited orthogonality is real, but it does not rescue the broader claim.

Required change: compute a per-pair normalized-input fingerprint from every actual prompt field (including claim title/author and all evidence text), plus a hash of the alias-group input and the pinned model/prompt configuration. Reuse only exact fingerprint matches, or run the gate afresh. “Never re-grain an existing `w######`” is necessary but not sufficient.

## HIGH — `docs/specs/discovery-v3-bake-plan.md` §3.1a: the database read-surface conclusion overstates what was verified

The enumeration is partly correct but the conclusion is not. Across both real-mode and `--emit-review-artifact-only`, the research connection reads only `track1_matches` and `pages`; it does not query another research table or issue a research `PRAGMA`. The actual required columns are:

- `track1_matches`: `work_id`, `cat`, `genre`, `author`, `title`, `page_id`, `shadowed_by`, `sys_id`, `matched_letters`, `best_density`, `n_spans`, and `spans_json`.
- `pages`: `page_id`, `provenance`, `text`, and `n_chars`.

The review-only path calls `select_shown_works`, `PageTextIndex`, and the same claim/evidence assembly. `--from-approved` does the same and, in release mode, additionally calls `_count_tier_a_rows`; normal real mode also calls `_compute_htr_snapshot_hash`. The other database connection used for optional enrichment is separate, not `conn_research`.

Therefore “two tables” is a useful boundary, but “12 of 14 columns plus two derived columns is the whole adapter” is false: the proposed `source_corpus` column is unused, the exact-offset data is unused, and column/value parity for the listed reads is still required. The estimate cannot rest on a table count.

## HIGH — `docs/specs/discovery-v3-bake-plan.md` §§3.1, 3.1a, and 6 gate 2: the claimed 52-work gap is a policy classification, not a demonstrated cause of missing crosswalk entries

`select_shown_works` does apply its current candidate policy exactly as described: it maps `cat`, takes the first row by `(work_id, page_id)`, and then applies the M-source genre keep-set. That proves how the present builder would select a representative; it does not prove why every crosswalk-missing work lacks a crosswalk entry, nor that every historical D-05/D-06 decision was encoded by that representative’s current fields.

In particular, `_map_cat_to_source_corpus` maps every non-empty, non-open `cat` to M-source by elimination. It is a compatibility classification, not an authoritative provenance check. The builder also documents that occurrence-level `cat` can differ from a work’s selected representative. Finally, owner approval is a separate stage after candidate selection. Those facts leave several other explanations for a missing mapping: an approval omission, a prior title/metadata decision, changed metadata, or a mapping process gap.

Required change: rename the result to “current-policy drops” unless historical crosswalk/approval provenance is audited; run an occurrence-consistency census for `cat` and genre; and define gate 2 over the *approved selected population*. “Every raw `ref_work` resolves” cannot coexist with intentionally excluded works, while a generic completeness check can silently certify the wrong universe.

## HIGH — `docs/specs/discovery-v3-bake-plan.md` §§3.5, 5, and 8: MAPV2-8 has three mutually inconsistent decisions

Section 3.5 concludes that the persisted exact cut is unrecoverable and recommends 595; the owner question asks for confirmation of 595; but the operative “DO” list says “152-severe” while assigning it the 595-page / 301-claim blast radius. These are not interchangeable. The upstream request is to revert an exact page set to v1 HTR, whereas exclusion both changes the remedy and discards findings. Excluding 595 is a fourfold expansion of the requested page set, even if its claim rate is small.

The lossy rounding evidence supports neither an unlabelled 152 implementation nor an automatic expansion to 595. Before execution, either re-run the audit to recover the intended set and perform the requested revert, or obtain explicit owner approval for a named exclusion set and its known recall loss. The current text is not executable deterministically.

## HIGH — `docs/specs/discovery-v3-bake-plan.md` §3.1 “shadowed_by”: the measurement is not an adapter contract

For the current producer, mixed status is not merely absent by observation. `gen2_shadow.py::shadow_pass` builds a competition unit at `(claim_id, ref_work)` and updates **all** its evidence rows together. The correct rule for a normalized one-row Track-1 adapter is therefore unit-level: retain an unshadowed unit and omit a shadowed unit. The plan instead groups by `(page_id, ref_work)` without establishing that this is always identical to the producer’s unit key.

This is a producer invariant, not a universal promise for a later run or a changed producer. A future mixed group must not be reduced with an undocumented `ANY` or `ALL`. Required change: derive `shadowed_by` at the producer’s `(claim_id, ref_work)` grain, assert that all constituent rows agree, and halt on a mixed group. This both preserves the present semantics and makes future drift visible.

## HIGH — `docs/specs/discovery-v3-bake-plan.md` §3.1 R-source containment: the stated gate does not catch the stated failure

The slim database is the right containment boundary only if its construction is itself verified. `select_shown_works` has no direct R-source-prefix rejection; an accidentally included row can be classified through the ordinary `cat`/genre logic. Gate 2 checks mapping completeness, not absence. An unintended row can resolve and make that gate pass.

The remaining builder paths do not independently create a work: all evidence collections are filtered through the selected-work index, and `--from-approved` is intersected with that selected set. That is useful containment. It does not validate the slim table. Add a fail-closed input gate that asserts the exact source table identity/fingerprint and zero R-source-prefix rows before every review-artifact and build invocation; keep an output-level containment scan as a second check.

## MEDIUM — `docs/specs/discovery-v3-bake-plan.md` §6: several gates can pass without establishing their claimed property

Gates 6 and 7 have no stated failure demonstration despite the section’s premise. Gate 1’s “mutate the expected count” only proves the comparator is live; without independently derived, per-stage expectations it can pass a self-consistent wrong mapping. The listed gates also do not prove preservation of routing, shadowing, source assignment, or offset pairing.

The masking controls are strong at the mechanics level: an unset file fails closed, strict mode requires the named surfaces, and the self-test exercises the matcher. They do not show that the loaded secret pattern set is complete or current; the self-test deliberately uses a synthetic pattern. Add a local, non-disclosing pattern-set attestation owned by the masking authority, and record the exact asset/sqlite paths and post-build hashes scanned. Require a real-pattern positive control that does not print the pattern.

## MEDIUM — `docs/specs/discovery-v3-naming.md`: the naming resolution is sound, but its approval state must remain explicit

`schema_version='discovery-v1'` is a loader compatibility contract, not the product/artifact version, so it does not refute `discovery-v3`. Giving a new, unbuilt membership-and-engine replacement a distinct major name also avoids collision with the already deployed `v2.1` artifact. The file itself says owner acknowledgement is still required; the bake plan should not treat the proposed name as ratified until that acknowledgement is recorded.

## LOW — `docs/specs/discovery-v3-bake-plan.md` §§0, 5, and 8: current scope is internally stale

The current sections simultaneously describe cache reuse as operative and fresh novelty as the “DO” item, and describe the policy-restricted set as both the recommended default and a superseded owner decision. Retaining reversals is appropriate, but the executable scope needs one unambiguous, dated decision table that names the selected population, novelty mode, MAPV2 set/remedy, router source, and required owner approvals.

VERDICT: CHANGES-REQUIRED
