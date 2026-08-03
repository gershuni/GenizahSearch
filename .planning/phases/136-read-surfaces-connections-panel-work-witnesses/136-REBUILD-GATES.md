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
| 5 | Release verification (public projection) | `verify_discovery_sidecar.py <public> --audience public --expected-frame-hash 53725098…` | **PASS** (after two artifact fixes + an audience mode — see below) | all invariants clean; routing_audit decisions = demoted 1,348 / kept_tie 3,466 |
| 6 | Masking (both artifacts) | `check_atlas_masking.py --strict --scan-repo --scan-asset <db> --scan-sqlite <db>` | **PASS** | `MASKING_SCAN_PATTERNS_FILE` **was set** (`C:/Genizahsearch/.masking_patterns`); private asset clean, public projection clean; the projection also runs its own scan internally — clean |
| 7 | Golden fixture | `pytest` over the discovery suites (assets/audience, build, VIS-01 projection, work domains, novelty contract, display strings) | **PASS** | 413 passed |
| 8 | Performance | `bench_discovery.py --sample 50 --warm-passes 1 --write-budgets`, against the PUBLIC artifact | **PASS** | see the findings-page table below; actuals written into `docs/specs/discovery-budgets.md` |
| 9 | Launch-scope reconciliation | projection reconciliation report | reported, then **PASS** via owner ruling S | VIS-01 launch-scope 240,566 vs two-axis conjunction 251,765 over 297,415 rows; symmetric difference **36,989 (12.4%)**. Measured by DIRECTION: conjunction-only = `ja\|track1_direct` 24,094; shortcut-only = `msource\|propagated` 12,895. The artifact ships the **conjunction**. Ruling S: JA direct matches ship (CERT-01's graded deck holds 44 JA works among 220 candidate cards, so they are inside the measured frame); the shortcut would have published 12,895 restricted-identity rows and must never gate publication again. |

## Gate 8 — the D-10a performance claim, measured for the first time

D-10a's premise was that the findings-page query took **3.41–3.55 s against a 1.5 s cap**, which is
why 136-11 added materialized columns and indexes. That fix had only ever been unit-tested on
fixtures. Measured here against the PUBLIC artifact (375.5 MB, 53,581 identifications), with the
latency cache disabled so every call is a real DB query:

| shape | p50 ms | p95 ms | cap ms |
|---|---|---|---|
| `findings_default_ordering` | 146.17 | **159.38** | 1500 |
| `findings_novelty_filter` | 16.07 | 17.09 | 1500 |
| `findings_relation_filter` | 26.87 | 36.06 | 1500 |
| `findings_domain_filter` | 128.84 | 133.66 | 1500 |
| `findings_visible_total` | 0.04 | 1.16 | 500 |
| `findings_deep_page_20` | 145.79 | 148.80 | 1500 |

The previously-failing default ordering is **159 ms p95 against a 1,500 ms cap** — roughly 21× faster
than the shape that motivated D-10a, with ~9× headroom.

Other families: `get_claims_for_page` p95 0.65 ms, `get_pages_related_to_page` p95 0.64 ms,
`get_work_witnesses` p95 523.65 ms (informational, under the 1.5 s work-page request cap),
browse-enrichment p95 0.65 ms (cap 150 ms), added RSS 12.2 MB (cap 250 MB).

## Gate 5 — the PUBLIC artifact was defective; two artifact fixes + an audience mode

Unlike the four check defects below, here the checks were correct and the ARTIFACT was wrong. Both
defects were in `scripts/project_discovery_public.py` (plan 136-08). The battery STOPPED, the defects
were reviewed externally (Codex) and traced to their originating plans before any fix was written,
and one of the two needed an owner ruling because no document decided it.

Violations went **168 → 57 → 3 → 0**.

**(a) Identification inflated 1.77× — RULED, obeyed.** The private builder derives
`discovery_identification` over evidence that is `shipped` OR `human_confirmed` (D-13g) and lands 1:1
— 64,522 rows. The projection re-derived it over ALL surviving evidence including 92,710
`review_only` rows, producing **95,149 rows where only 53,616 were shipped-backed** — more rows than
the private superset it came from.

Fixed NOT by adding the missing filter but by **deleting the projection's parallel implementation
entirely** and calling the production materializer
(`build_discovery_sidecar.populate_discovery_identification`) against the populated public artifact.
Codex's point: filtering alone would fix the row count while leaving `main_pool`/`main_pool_reason`
computed by the module's own documented "simplified stand-in" and `eligibility_basis` unwritten. One
rule now produces both sides. Public identification is **53,581** — a proper subset of the private
64,522. New `check_identification_key_subset` pins it: removing evidence cannot mint a
`(sys_id, canonical_work_id)` key, so a public key absent privately proves the two were materialized
over different populations. Deliberately a KEY-set check, not a row-count one — public rows may
legitimately carry different aggregate VALUES.

**(b) Closed-graph break — genuinely UNDECIDED, owner ruled.** 680 `demoted` audit rows exist
privately but not publicly (486 name a `demoted_work_id` that is not a public work), orphaning 164
public evidence rows whose `routing_reason='later_shared_text'` asserts a demotion the artifact could
no longer substantiate. Nothing in the schema, `136-CONTEXT.md`, `v9-PUBLICATION-STRATEGY.md` or
136-08 addressed the case.

**Owner ruling 2026-08-03: drop the citing evidence.** An evidence row whose stated routing reason
cannot be substantiated in the artifact carrying it is asserting a fact its own provenance cannot
back; redaction or a surrogate id would still disclose that a hidden competitor exists.

Implemented as **survival-time pruning iterated to a fixed point**, not a post-hoc delete — pruning
can make a work unreachable, which drops further audit rows, which orphans further evidence. Two
cascade rules, reported separately in the offline reconciliation report (never inside the artifact,
so a reader cannot infer how much was withheld):

- `pruned_unreplayable_evidence` = **164** — the orphaned `later_shared_text` rows.
- `pruned_g9_cascade_evidence` = **54** — claims asserting a witness relation (31 `direct_witness` +
  23 `quotes_this_work`) that lost their LAST witness-kind row while keeping non-witness rows. These
  surfaced only after (b) was fixed: they would otherwise have survived asserting a relation nothing
  in the artifact supports.

**(c) The verifier had no audience mode.** The remaining 3 violations were structural false
positives: it hard-coded `meta.audience='private'` and recomputed `frame_content_hash`, so run over a
public projection it ALWAYS failed, and the result could only be read by eyeballing which violations
"did not apply". A gate whose output needs manual triage is not a gate. Added `--audience
public|private` (default `private`, so the private profile is unchanged and still passes): the public
profile expects the artifact to declare itself public, and checks `meta.frame_content_hash` as the
SOURCE frame it was projected from rather than recomputing over the deliberately-smaller public
membership — recomputing and demanding equality was checking that the projection did nothing.

**Root cause, and the fixture repair.** Per the provenance trace, neither defect was a divergence
from a stated rule: 136-08 (wave 2) wrote the projection's survival logic BEFORE 136-11 (later wave)
specified the D-13g eligibility rule, and 136-08 never knew about Phase-135's `later_shared_text`
invariant (grep: zero occurrences in its plan or summary). A sequencing gap, not an execution slip.
`tests/test_vis01_projection.py` could not have caught either: its evidence table lacked columns the
production rule reads, and its private identification rows were hand-written rather than derived, so
the two sides were never comparable. Fixtures now build BOTH sides with the production rule, carry
the real columns, and two new tests cover the previously-missing shapes — a `review_only` row inside
an otherwise-public group, and a public `later_shared_text` row whose demotion names a restricted
work. `tests/test_discovery_build.py`'s test that existed to pin the two implementations against
drift did NOT catch this (they agreed on the id recipe while disagreeing on the population); it now
asserts the second implementation stays deleted.

## Artifacts

| | path | content_hash |
|---|---|---|
| private (never deployed) | `discovery_data/discovery-v1-136rebuild.db` | `9b4e740efaca09a89bc37d356c23864c433ac3460a20b9e508278b160bd6e07e` |
| public (the deploy candidate) | `discovery_data/discovery-public-136rebuild.db` | `e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181` |

Final public counts: works 613 · discovery_claim 231,244 · discovery_evidence 251,547 ·
discovery_identification 53,581 · witness_units 1,959 · manuscript_display 39,518 ·
discovery_routing_audit 4,814.

`discovery_data/manifest.json` currently describes the PUBLIC artifact (written for the gate-8
benchmark, and the manifest a deploy would carry). The private build's own manifest is preserved at
`discovery_data/manifest.private-136rebuild.json`.

## Superseded gate-5 failure record

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

## Task 2 — owner deploy authorization

**Owner, 2026-08-03, verbatim:** *"OK proceed to deploy"* — given after reviewing the pre-deploy
content of the artifact itself (`PREDEPLOY-candidates-by-work.PRIVATE.html`, all 4,152 shipping
candidate finds) and the held-out set (`EXCLUDED-STRONG-nonbible.PRIVATE.html`, 975 rows), and after
rulings S and T.

Authorized: deploy **the public projection and only the public projection**, code first then the
artifact, `DISCOVERY_ENABLED` OFF throughout. The private asset is NOT deployed and NOT staged on the
web host.

**Pre-flight checks run before touching production:**

| check | result |
|---|---|
| production HEAD | `b3faedd8` (2026-07-31, the v8.5.2 web release) — an ancestor of local HEAD, 157 commits behind. The box's branch NAME (`phase-98-nli-resilience`) is stale leftover, not stale code. |
| `DISCOVERY_ENABLED` on the box | not set in `.env` → defaults OFF |
| live manifest | points at `discovery-v1-33499c5b…` (the current v2 asset), the rollback target |
| web-facing diff | `web/discovery_assets.py`, `web/pages/help.py`, `web/static/common.css` only — every help section gated on `discovery_available()`, CSS purely additive (0 deletions) |
| other app files | `genizah_translations.py` (additive discovery strings, dead until the surfaces exist) and `CLAUDE.md` |

Conclusion: with the flag OFF the code deploy is behavior-neutral for every existing surface.

## Task 3 — the ONE authorized production deploy (DONE, 2026-08-03)

Deployed **the public projection only**, code first then the artifact, `DISCOVERY_ENABLED` OFF
throughout. The private asset was never uploaded and is not on the box.

| step | action | result |
|---|---|---|
| 1 | push `master-main` | `b3faedd8 → 4824e5cf` (157 commits) |
| 2 | `./deploy.sh master-main` | box HEAD now `4824e5cf`; service active |
| 3 | live-site check after code deploy | `/` `/search` `/browse` `/atlas` all HTTP 200; cold-start 11 s settling to **0.6 s warm** |
| 4 | `scp` artifact to `.uploading` temp name | live manifest still pointed at the OLD asset throughout |
| 5 | verify transferred bytes ON THE BOX | `sha256 = e9365edc…` — matches the local hash exactly |
| 6 | `mv` to final content-hashed name | both assets on disk; old one retained as the rollback target |
| 7 | stage `manifest.json.candidate` | live manifest untouched |
| 8 | verify staged asset ON THE BOX | `verify_discovery_sidecar.py --audience public --expected-frame-hash 53725098…` (EXTERNAL pin, never the candidate's own manifest) → **all invariants pass** |
| 9 | masking gate the staged asset ON THE BOX | `--scan-sqlite --scan-asset --scan-repo --strict` with `MASKING_SCAN_PATTERNS_FILE=.masking_patterns` (present on the box, `-rw-------`) → **no matches, clean** |
| 10 | preserve rollback target | `manifest.prev.json` ← the `33499c5b…` manifest |
| 11 | **ATOMIC swap** | `mv -f manifest.json.candidate manifest.json` |
| 12 | restart `genizah-web` | active; **no "Discovery sidecar not loaded (fail-closed)" line** in the log |
| 13 | flag-bypassing readiness smoke | `bench_discovery.py` → **53,581 identifications**, 2,626 warm-burst rows, every findings shape inside cap (see `discovery-budgets.md` §5.1) |
| 14 | post-deploy live check | `/` `/search` `/browse` `/atlas` HTTP 200; **`/findings` and `/discovery` 404** — flag OFF, no surface exposed |

**Live asset:** `discovery-v1-e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181.db`
**Rollback:** single atomic manifest repoint to `manifest.prev.json` (`33499c5b…`); the old asset is
still on disk and the loader ignores any file that is not the manifest's exact `asset_basename`. No
file deletion, no re-upload.

Plan 136-13 is COMPLETE. The rebuilt asset is live, flag OFF, ahead of any surface code that reads
its new columns — which is exactly the ordering the plan exists to guarantee.
