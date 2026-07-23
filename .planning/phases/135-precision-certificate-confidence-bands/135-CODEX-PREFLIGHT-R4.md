## Section A: Round-3 outstanding-item status

1. **#5 date schema — PARTIAL** — `135-04-PLAN.md:Task 1` and `135-06-PLAN.md:Task 2` correctly freeze the real `seftja_dates.json` shape as raw-source-ID → exactly `{year:int, basis:str}`. The composition-date side remains described only as raw ID → “descriptive date value” accepted when a frozen normalizer produces a “plausible” year. The accepted file shape, date-string grammar, year range, and range/century normalization rules are not enumerated; the malformed-input test is correspondingly generic. Fix by specifying those exact rules in Tasks 04/06 and testing every accepted production form plus near-miss rejection.

2. **#8 one-commit/one-bake — RESOLVED** — `135-05-PLAN.md:Task 3` requires a dated §5 amendment defining atomicity at asset/bake level, while `135-06-PLAN.md:Task 3` and `135-07-PLAN.md:Task 1` enforce a single-bake, no-mixed-enum v2 asset.

3. **#9 population source — RESOLVED** — `135-01-PLAN.md:Tasks 1 and 3` freeze the correct SQL joining `discovery_claim.display_evidence_id` to the shipped display evidence and include a regression where one claim owns multiple evidence rows. `135-02-PLAN.md:Tasks 1–2` consumes that runtime claim count.

4. **#13 cluster-map pin — RESOLVED** — `135-03-PLAN.md:Task 1` defines `cluster_map_hash` over sorted `(page_id, canonical_work_id, unit_key)` assignments. `135-09-PLAN.md:Tasks 1 and 3` record it and require independent recomputation from the deployed sidecar.

5. **Census-input provenance — RESOLVED** — `135-06-PLAN.md:Task 1` records `canonical_merges_sha256` in `meta`; `135-07-PLAN.md:Task 2` records it in the frozen frame. This matches the locked meta+frame contract; the deploy manifest remains minimal.

6. **Round-3 NEW HIGH: population counts must be evidence, not claims — RESOLVED** — `135-01-PLAN.md:Tasks 1 and 3` provide the exact display-pointer SQL and a load-bearing multiple-evidence-per-claim test. `135-VALIDATION.md:135-01-01/03 and 135-02-01/02` explicitly validate display-deduplicated shipped claims.

7. **Round-3 NEW HIGH: stale `screening_rb` precision after reband — RESOLVED** — `135-04-PLAN.md:Task 1` and `135-06-PLAN.md:Task 2` require the same-transaction invalidation of precision, CI, numerator, and denominator. `135-06-PLAN.md:Task 3` adds the `tier_a_reband_target` verifier gate and a retained-precision negative test.

8. **Round-3 NEW HIGH: preregistration input and cluster hashes — PARTIAL** — DB-content and cluster-map binding are now present in `135-09-PLAN.md:Tasks 1 and 3`. However, `135-03-PLAN.md:Task 1` explicitly requires `crosswalk_sha256`, and `135-07-PLAN.md:Task 2` freezes it in the frame, while `cert01_prereg.json`, the Task-3 validator, and `135-VALIDATION.md:135-09-01/03` enumerate only canonical-merges, composition-dates, seftja-dates, and DB content. Add `crosswalk_sha256` to the preregistration payload/report hash and require the validator to compare it with deployed `meta`.

## Section B: NEW findings

- [HIGH] — `135-06-PLAN.md:Task 2` — Rebanding changes `confidence_band`, which is part of the frozen `evidence_id` tuple in `docs/specs/discovery-sidecar-schema-v1.md:§2` and also changes display-evidence precedence. The task requires rebands but never requires them to occur before `ids.evidence_id()` and `select_display_evidence()`, nor requires evidence-ID regeneration. A post-assembly update can therefore leave content-inconsistent IDs and stale display pointers. — Apply the reband to evidence specifications before evidence-ID generation and display selection, or regenerate IDs and all dependent pointers transactionally; add a verifier/test that recomputes the frozen evidence-ID recipe and exercises a tier_a claim with a competing shipped sibling.

VERDICT: REWORK