# 136-13 — Compatibility attestation for the ONE authorized rebuild

**Date:** 2026-08-03. **Plan:** 136-13, Task 1. **Status:** all gates pass; see
`136-REBUILD-GATES.md` for the battery.

## Identity

| | value |
|---|---|
| ORIGINAL asset (live in production) | `discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db` |
| ORIGINAL `db_content_hash` | `33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff` |
| NEW private asset (never deployed) | `discovery_data/discovery-v1-136rebuild.db` |
| NEW `db_content_hash` | `9b4e740efaca09a89bc37d356c23864c433ac3460a20b9e508278b160bd6e07e` |
| NEW public projection (the deploy candidate) | `discovery_data/discovery-public-136rebuild.db` |
| public `content_hash` | `e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181` |
| `frame_content_hash` (both) | `53725098ece6cf152a72425587dc2fe9119261427fc82e008a5b953dcbd2bce7` |

The frame hash is **byte-identical to the pre-rebuild value**. Membership did not change; the rebuild
is additive (new columns and tables only), exactly as `docs/specs/discovery-deploy.md` §4 predicts for
a same-membership rebuild.

## Allowlisted diff

`scripts/verify_rebuild_preservation.py <old> <new> --expected 136-REBUILD-PRESERVATION-EXPECTED.json`
— run against the **externally pinned** expectation, never the candidate's own manifest (D-02b: a
build over the wrong inputs still produces an internally self-consistent manifest).

| table | rows compared | allowlisted columns | result |
|---|---|---|---|
| `works` | 1,269 | 2 | PASS |
| `discovery_claim` | 268,361 | 0 | PASS |
| `discovery_evidence` | 297,415 | 6 | PASS |
| `witness_units` | 5,547 | 0 | PASS |
| `witness_unit_members` | 19,554 | 0 | PASS |
| `discovery_routing_audit` | 6,270 | 1 | PASS |
| `band_precision` | 7 old / 7 new | 1 authorized ROW-level exception | PASS |

`band_precision` carries exactly ONE authorized change, D-02a: the `tier_a` row's
`measurement_status` (`NULL` → `measured_pass`) and `ci_low` (`NULL` → `0.9084`). `precision` remains
NULL. **Nothing else on that row changed** — including `notes`, which gate 1 caught on the first
attempt when the precision spec was generated from the build module's own frozen row-set and carried a
newer explanatory string. The spec was regenerated from the LIVE rows with only the two authorized
fields amended. The asset follows the ruling, not the code comment; `_frozen_real_band_precision_rows`
and the shipped asset now disagree on that one `notes` string, deliberately.

## Recomputed population hashes

Recomputed from the research corpus (`--research-db same_work_spike/probe/data/fullcorpus_v2.db`) and
compared against the pinned expectation — not copied from either asset's manifest:

| | pinned value | result |
|---|---|---|
| `population_hash` | `d3d1cc44e96fc3b7971a8239949478ffb2a14b61f26daf3f7a234574b8845ca3` | recomputed + matched |
| `cluster_map_hash` | `00fa8bf47ae9a48d8b3c21e4c3ce1a39ee66857ef227c113debfe7c2185bdaa3` | recomputed + matched |
| per-stratum counts | (6 strata) | recomputed + matched |

## CERT-01: the pre-registration is UNMODIFIED, and its check 10 fails by design

`git diff` on both is **empty**:

- `.planning/phases/135-precision-certificate-confidence-bands/cert01_prereg.json`
- `scripts/verify_cert01_grading.py`

The pre-registration pins the ORIGINAL asset's byte-stream (`db_content_hash` `33499c5b…`). Its check
10 therefore **fails against ANY rebuilt asset, by design** — that is the pre-registration doing its
job, not a defect and not a licence to edit it. **This attestation is the answer to that failure.**
The pre-registration must never be re-pinned to the rebuilt hash: doing so would retroactively make a
frozen commitment describe an artifact it never measured.

What matters for the certificate is that the graded cards still point at the same evidence, and they
do:

- **280 graded cards checked. 240 bind identically** — same `claim_id`, `display_evidence_id`,
  `span_start`, `span_end` and `snapshot_hash`, old versus new.
- **40 resolve in NEITHER asset** — exactly the deck's own 20 `diagnostic_demoted` + 20 `gold` control
  cards, which by construction are absent from the shipped ranked estimand. They do not resolve
  against the live production asset either.

Separately (ruling S), the certificate's measured population was checked for corpus coverage before
the JA-direct scope decision: of the 220 candidate-role graded cards, 44 are JA works, 133 Sefaria, 43
restricted-corpus. The shipped JA direct matches are inside the measured frame, not outside it.

## What changed in the asset

Additive only. New/populated columns and tables per the 2026-08-02 schema amendment: `coverage_ppm`
/`coverage_status`, `band_rank`, `novelty_status`/`novelty_source_label`, `divergence_correctness`,
`assertion_visibility`/`identity_visibility`, `works.genre`, `meta.audience`, and the
`discovery_identification` + `manuscript_display` tables.

One pre-existing table gained data it should always have carried: `discovery_routing_audit.kept_tie`
rows now name the work they beat. The live asset has **4,208 `kept_tie` rows, all with NULL
`demoted_work_id`**; the rebuild has **4,208 with ZERO NULL**. Same tie population — schema amendment
(F) is satisfied for the first time, so those ties are now reconstructable from the audit table alone.
