# 136-13 — Rebuild gate battery

**Run:** 2026-08-03, on the main checkout (NOT a worktree — the rebuild needs the gitignored
sidecars, `.env` and `.masking_patterns`, none of which exist in a worktree).

**Rebuild command:** `scripts/run_136_rebuild.sh`, which is the `docs/specs/discovery-deploy.md` §4
command with the 2026-08-03 amendment applied.

- Private asset: `discovery_data/discovery-v1-136rebuild.db`
- `content_hash` = `28b11b6b46ba7f3cf53312b8000a911a7be9ddf88b9a3fbffab3a1fe91ffc76a`
- `frame_content_hash` = `53725098ece6cf152a72425587dc2fe9119261427fc82e008a5b953dcbd2bce7`
  — **identical to the pre-rebuild pinned value**, so membership did not change and the rebuild is
  additive exactly as designed.
- Row counts: works 1,269 · discovery_claim 268,361 · discovery_evidence 297,415 ·
  witness_units 5,547 · discovery_identification 64,522 · manuscript_display 44,375
- `evidence_id_collisions=187` (known deduped-on-insert figure, shipped preferred over review_only;
  Phase 134 recorded 115 on the v1 spine).

## Input re-verification (before any gate)

`docs/specs/discovery-deploy.md` §4 instructs re-verifying the pinned inputs against the LIVE asset's
own `meta` table before reuse. Doing so found a defect in the documented command:

| Pinned input | Live `meta` | File on disk | Result |
|---|---|---|---|
| `source_db_sha256` | `1dc28d6d…` | `1dc28d6d…` | MATCH |
| `crosswalk_sha256` | `bcde04bd…` | `bcde04bd…` | MATCH |
| `canonical_merges_sha256` | `cc054d11…` | `86b1d0ea…` | **MISMATCH** |
| `composition_dates_sha256` | `2b46b470…` | `2b46b470…` | MATCH |
| `seftja_dates_sha256` | `00760289…` | `00760289…` | MATCH |

The mismatch was the DOC, not drift: the command named `v2_canonical_merges.json` (86,163 bytes)
while the live asset was built from `v2_canonical_merges.build.json` (2,382 bytes, `cc054d11…`).
Naming the census is not a harmless synonym — it carries masking-sensitive Hebrew titles and the
build parser halts on extra fields; the slim file is a masking control. Membership is identical
(16 merges, same `dropped_by_135`). Corrected in place as a dated amendment (commit `77623b29`).

`--precision-spec` is `136-PRECISION-SPEC.json`, built from the LIVE `band_precision` rows with ONLY
`tier_a`'s `measurement_status`/`ci_low` amended per D-02a. See gate 1 for why.

## Gates

| # | Gate | Command | Outcome | Numbers |
|---|---|---|---|---|
| 1 | Rebuild preservation | `verify_rebuild_preservation.py <old> <new> --expected 136-REBUILD-PRESERVATION-EXPECTED.json` | **PASS** | works 1,269 (2 cols allowlisted) · discovery_claim 268,361 (0) · discovery_evidence 297,415 (6) · witness_units 5,547 (0) · witness_unit_members 19,554 (0) · discovery_routing_audit 6,270 (1) · band_precision 7/7 |
| 2 | Recomputed hashes | same run `--research-db fullcorpus_v2.db --manifest manifest.json` | **PASS** | population_hash / cluster_map_hash / stratum_counts recomputed and compared against the external pin |
| 3 | CERT-01 card binding | same run `--cert01-cards <280 graded>` | **PASS** | 280 graded cards checked; **240 bound identically**; 40 resolve in neither asset (the 20 `diagnostic_demoted` + 20 `gold` controls, absent from the shipped estimand by construction) |
| 4 | Release verification (private asset) | `verify_discovery_sidecar.py <new> --expected-frame-hash 53725098…` | **PASS** | all invariants clean; routing_audit decisions = demoted 2,062 / kept_tie 4,208 |
| 5 | Release verification (public projection) | `verify_discovery_sidecar.py <public> --expected-frame-hash 53725098…` | **FAIL — 168 violations** | see "Gate 5 failure" below |
| 6 | Masking (both artifacts) | `check_atlas_masking.py --strict --scan-repo --scan-asset <db> --scan-sqlite <db>` | **PASS** | `MASKING_SCAN_PATTERNS_FILE` **was set** (`C:/Genizahsearch/.masking_patterns`); private asset clean, public projection clean; the projection also runs its own scan internally — clean |
| 7 | Golden fixture | — | **NOT RUN** | blocked: gate 5 must pass first, and the public artifact will be rebuilt |
| 8 | Performance | `bench_discovery.py … --write-budgets` | **NOT RUN** | blocked: same reason |
| 9 | Launch-scope reconciliation | projection reconciliation report | **REPORTED** (not a pass/fail) | VIS-01 launch-scope 240,566 vs two-axis conjunction 251,765 over 297,415 rows; symmetric difference **36,989 (12.4%)** = `ja\|track1_direct` 24,094 + `msource\|propagated` 12,895. The projection shipped the **conjunction**. Material — goes to the owner. |

## Gate 5 failure — the PUBLIC artifact is defective (STOPPED; not deployed)

Unlike the four defects below, here the checks are correct and the **artifact** is wrong. Both live in
`scripts/project_discovery_public.py` (plan 136-08). Per this plan's own instruction, the battery
STOPPED here: gates 7 and 8 were not run, and no deploy authorization is being requested.

The 168 violations are four classes:

| Count | Violation | Assessment |
|---|---|---|
| 164 | evidence with `routing_reason='later_shared_text'` has no `demoted` audit row in the public asset | **REAL — closed-graph break** |
| 1 | `discovery_identification` row count 95,149 != shipped-backed pair count 53,616 | **REAL — 1.77× inflation** |
| 1 | `meta.audience` must be `'private'` | **NOT APPLICABLE** — the verifier has no public mode |
| 2 | `frame_content_hash` recomputed != `meta` / != `--expected` | **NOT APPLICABLE** — the public asset has a different membership (613 works vs 1,269), so its frame hash necessarily differs; `meta` carries the frame it was projected FROM |

**(a) Identification inflation.** The private builder derives `discovery_identification` over evidence
that is `shipped` OR `human_confirmed` (the D-13g rule) and lands exactly 1:1 — 64,522 rows, 64,522
pairs. The projection re-derives "bottom-up from the SURVIVING evidence set" WITHOUT that restriction,
so it counts the 92,710 `review_only` rows it also carries, producing **95,149 rows where only 53,616
are shipped-backed**. A public artifact cannot have MORE identifications than the private superset it
was projected from; every downstream identification count on the findings page would be inflated ~1.77×.

**(b) Closed-graph break, and it hides a visibility decision.** 680 `demoted` audit rows exist
privately but not publicly, and 164 surviving public evidence rows cite them. The projection dropped
them for a SOUND reason: **486 of the 680 name a `demoted_work_id` that is not a public work** (246
name a non-public `kept_work_id`), so retaining them as-is would publish the identity of a private
work. The fix is therefore not mechanical — it is a choice between dropping the citing evidence rows
(narrows the public surface), redacting the private id (may violate the audit table's own schema
rules), or scoping the check for public artifacts. **That is a visibility design decision belonging to
136-08 and the owner, not something to improvise inside the rebuild plan.**

**Expectation files were NOT edited.** `git diff` on `136-REBUILD-PRESERVATION-EXPECTED.json` and on
`.planning/phases/135-precision-certificate-confidence-bands/cert01_prereg.json` is empty.

## Defects found BY the battery (fixed in the checks, never in the expectations)

Four checks failed on first execution. In every case the pinned expectations were left untouched and
the CHECK was corrected, because each was demonstrably mis-specified rather than detecting drift.

1. **Precision spec carried an unauthorized `notes` change** (gate 1, commit `38fefa95`).
   Generating the spec from the build module's own frozen row-set carried a newer explanatory `notes`
   string on the `tier_a` row. D-02a authorizes exactly two fields (`measurement_status`, `ci_low`);
   `notes` is not one of them. **The gate was right and my spec was wrong** — regenerated from the
   live rows. Note for the record: `_frozen_real_band_precision_rows` and the live asset now disagree
   on that one `notes` string. The shipped asset follows the ruling, not the code comment.

2. **Author-key coverage rejected the LIVE production asset** (commit `d85775f4`).
   `assert_author_key_coverage` (136-12) compared RAW `works.author` strings against an index its own
   docstring calls `alias_by_normalized_author`, over ALL works rather than the shipped scope the
   artifact was built from. 28 uncovered on the rebuild — and **the identical 28 on the live,
   certified asset**: 16 from the key-space mismatch (16 of 96 curated rows normalize to a different
   string) and 12 from the scope error. A gate that rejects what is already in production is wrong by
   construction. Drift detection is unchanged in strength; a new test mutates an author and asserts
   the check still raises.

3. **Card binding could not run, then failed on its own controls** (commit `38fefa95`).
   `resolve_card_bindings` built one `IN(...)` over every `display_evidence_id` in the ranked
   estimand — thousands — and raised `too many SQL variables`. Once chunked, it reported 40/280
   violations, all "could not be resolved in the OLD asset": exactly the deck's own
   `diagnostic_demoted` + `gold` control cards. The check asks whether the rebuild MOVED a card, so
   the comparison is old-vs-new; unresolved-in-both is unchanged. Resolving in one but not the other
   is still a violation.

4. **The verifier held two contradictory `kept_tie` rules** (commit, gate 4).
   `check_kept_tie_names_its_pair` (136-12, schema amendment (F)) requires a non-NULL
   `demoted_work_id`; `check_routing_audit_replayability` in the SAME script required NULL. Whichever
   way the builder wrote the column, one check was guaranteed to fail — 4,208 violations. 136-12
   fixed the builder and added the new check but left the old branch on the pre-amendment rule.
   Confirmed real: LIVE has 4,208 `kept_tie` rows ALL with NULL `demoted_work_id`; the rebuild has
   4,208 with ZERO NULL. Same tie population, now reconstructable — amendment (F) is satisfied for
   the first time by this rebuild.

**Common cause worth recording:** all four are checks that had never been executed against a real
build — they passed synthetic fixtures where the distinguishing condition (raw ≠ normalized, an
out-of-scope work, a large id set, a filled `demoted_work_id`) did not occur.

## Novelty coverage (from the build report)

`verdict_cache_sha256 = eb6fc4f8…`, 65,200 verdict entries, 0 malformed keys, 0 failed-closed,
0 grain alias conflicts.

Shade counts over 297,415 evidence rows: `confirms` 77,316 · `diverges_work` 52,831 ·
`refines_granularity` 50,779 · `not_checked` 69,533 · `fills_gap` 21,446 · `container_predicts`
13,521 · `aid_more_specific` 10,354 · `diverges_part` 1,615 · `alias_merge` 20.

**`not_checked` is concentrated where it cannot be seen:** 68,280 of the 69,533 are `review_only`
rows that never ship. Only **1,253 shipped rows** are unchecked — 0.7% of the 187,070 shipped
evidence rows — corresponding to the 652 candidate pairs the model left unresolved. These fail in
the safe direction: `not_checked` never renders as a candidate find, so the cost is a small number
of possible discoveries staying hidden, never a false claim.
