## Section A: Round-2 outstanding-item status

1. **#1 — RESOLVED** — `135-04-PLAN.md:Task 2` recomputes the bake-plan SHA-256 and requires `VERDICT: APPROVE` to be the final non-empty line via `re.fullmatch`; quoted earlier verdicts cannot pass.

2. **#2 — RESOLVED** — `135-03-PLAN.md:Task 1` and `135-09-PLAN.md:Task 1–2` use tracked artifacts, record `protocol_sha256`, commit an immutable preregistration before drawing, compute `report_id` without self-reference, and place `deck_manifest_hash` in a separate manifest.

3. **#3 — RESOLVED** — `135-09-PLAN.md:Task 3` remains a blocking human checkpoint and adds a retained validator covering report-ID recomputation, deck binding, verdict vocabulary/membership/attribution, blindness, population/strata, and allocations.

4. **#5 — PARTIAL** — `135-04-PLAN.md:Task 1` and `135-06-PLAN.md:Task 2` now require and hash-pin both date inputs, enforce release coverage, and record provenance. However, neither freezes an exact accepted schema/parser grammar for the composition-date table or the actual `seftja_dates.json` shape—raw-ID keys with `{year,basis}` values—so the round-2 “merely functional schema” concern remains.

5. **#7 — RESOLVED** — `135-03-PLAN.md:Task 1` and `135-06-PLAN.md:Task 2` consistently use the legal frozen key `screening_rb`, flip affected rows to `review_only`, distinguish insufficient evidence, and test both branches.

6. **#8 — PARTIAL** — `135-05-PLAN.md:Tasks 1–3` now includes `discovery-frames.md` and preserves v1/v2 runtime compatibility. It nevertheless only reinterprets the contract as asset-level atomicity while `docs/specs/discovery-band-labels-v1.md:§5` still literally requires the files to change together in “one commit / one bake”; the edits remain split across separately committed tasks/plans.

7. **#9 — PARTIAL** — `135-01-PLAN.md:Task 1` removes the Wave-2 dependency on the Wave-4 frame document through a runtime counter. But its SQL counts shipped `discovery_evidence` rows, whereas `docs/specs/discovery-frames.md:§4` defines per-band population as claims grouped by their single `display_evidence_id`. Multiple evidence rows per claim therefore inflate the displayed population.

8. **#12 — RESOLVED** — `135-08-PLAN.md:Task 1` extracts both labeled hashes from `discovery-frames-v2.md`, requires both values and both labels in the deploy log, and also checks candidate/previous manifests and atomic-swap evidence.

9. **#13 — PARTIAL** — `135-03-PLAN.md:Task 1` freezes the SQL, stratum tie-break, and physical-MS mapping rule, while `135-09-PLAN.md:Tasks 1–3` now records and recomputes population/stratum/allocation values. The preregistration and validator still do not hash or compare the resolved page→physical-MS cluster mapping, so that CI-critical mapping is not actually pinned.

10. **#17 — RESOLVED** — Plans 03–09 contain no `&&` in `<automated>` commands, consistent with `135-VALIDATION.md` and PowerShell 5.1.

11. **Round-2 freeze-manifest BLOCKER — RESOLVED** — `135-09-PLAN.md:Tasks 1–2` defines a finite payload-minus-`report_id` hash, commits it before drawing, never mutates it afterward, and binds the deck through a separate tracked manifest.

12. **Round-2 census-input HIGH — PARTIAL** — `135-06-PLAN.md:Task 1` adds required CLI arguments, SHA verification, schema validation, approve-only loading, three-way threading, release tests, and `meta` provenance; `135-07-PLAN.md:Task 2` records the hash in the frame. But `135-04-PLAN.md:Task 1` requires provenance in `meta + manifest + frame`, while Plans 06–07 never modify the emitted manifest accordingly; the current builder manifest contains only schema, basename, content hash, and frame hash.

13. **Round-2 measurement_status HIGH — RESOLVED** — `135-01-PLAN.md:Tasks 2–3`, `135-05-PLAN.md:Task 1`, and `135-06-PLAN.md:Task 3` add a closed stored vocabulary, CI-fail-closed presentation/default eligibility, verifier cross-field checks, and missing/sub-0.85 regression tests.

## Section B: New findings

- [HIGH] — `135-01-PLAN.md:Task 1 / 135-02-PLAN.md:Task 1` — The new runtime population solution counts all shipped evidence rows, not display-deduplicated claims, and can materially overstate populations where one claim owns multiple evidence rows. — Count `discovery_claim` joined to its `display_evidence_id`, define whether the documented population includes all or only shipped claims, and test a claim with multiple evidence rows.

- [HIGH] — `135-06-PLAN.md:Task 2` — Rebanding failed `tier_a` rows into the existing `screening_rb` population leaves that band’s legacy 0.859 precision record intact, even though it measured the original screening population rather than the newly combined population. The Help page would therefore display an invalid band-population estimate. — Atomically invalidate the legacy `screening_rb` precision/status after rebanding or calculate a valid combined-population estimate; add a verifier/test preventing a population-changing reband from retaining the old measurement.

- [HIGH] — `135-09-PLAN.md:Tasks 1–3` — The executable preregistration payload omits the four frozen input hashes required by `135-03-PLAN.md:Task 1` and does not bind the DB content hash or resolved physical-MS cluster mapping. The current `frame_content_hash` excludes routing and witness-unit membership, while `population_hash` contains only `(page_id, canonical_work_id, stratum)`, so a cluster-map change can pass every proposed validator check while altering the clustered CI. — Include and validate the four input hashes plus either the exact deployed DB `content_hash` or a dedicated cluster-map hash in `cert01_prereg.json`; recompute them in `verify_cert01_grading.py`.

VERDICT: REWORK