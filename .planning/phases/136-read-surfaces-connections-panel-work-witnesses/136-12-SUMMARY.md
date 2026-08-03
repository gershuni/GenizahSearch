---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 12
subsystem: discovery-sidecar-build
tags: [novelty, visibility-axes, curated-artifacts, release-verifier, masking, schema-amendment]
requires:
  - "136-04 (shared/discovery_novelty.py — the ten-value contract, masking, novelty_work_key)"
  - "136-08 (shared/discovery_visibility.py — the two axes, is_public, reconcile_launch_scope)"
  - "136-09 (the two hash-pinned curated artifacts + owner rulings P/Q)"
  - "136-11 (build wiring A — coverage_ppm, band_rank, the identification grain)"
provides:
  - "novelty_status / novelty_source_label / divergence_correctness on BOTH evidence families"
  - "discovery_evidence.assertion_visibility + works.identity_visibility (VIS-01 / D-22)"
  - "works.genre from the hash-pinned curated artifact; the author key bound by coverage"
  - "meta.audience='private' + three hash-pinned-input provenance keys"
  - "twelve registered release-verifier checks covering every field both wiring passes added"
  - "docs/specs/discovery-sidecar-schema-v1.md § Amendment 2026-08-03"
affects:
  - "136-13 (the rebuild, the gate battery, the one production redeploy)"
  - "136-15/136-16/136-17/136-18 (they read novelty_status and the visibility axes)"
tech-stack:
  added: []
  patterns:
    - "hash-pinned fail-closed build inputs (the existing canonical-merges mechanism, reused)"
    - "vocabulary mirrors in the verifier + drift guards, never a builder import"
key-files:
  created:
    - ".planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-12-SUMMARY.md"
  modified:
    - "scripts/build_discovery_sidecar.py"
    - "scripts/verify_discovery_sidecar.py"
    - "shared/discovery_novelty.py"
    - "docs/specs/discovery-sidecar-schema-v1.md"
    - "tests/test_discovery_build.py"
    - "tests/test_discovery_schema.py"
    - "tests/test_discovery_v2_bake.py"
    - "tests/test_discovery_band_labels.py"
    - "tests/test_discovery_bands.py"
    - "tests/test_discovery_release_contract.py"
    - "tests/fixtures/discovery_v2_fixture.py"
    - "tests/fixtures/discovery/discovery-v1-fixture.db (+ manifest + expected)"
decisions:
  - "divergence_correctness is NEVER ingested from the verdict cache (owner ruling L); the column stays NULL until a human/owner annotation pathway exists"
  - "The curated author key stores NO column — none is authorized — and is bound to the asset by an enforced coverage check instead"
  - "works.genre stores the full '{parent} / {leaf}' path, with the bare 'Unassigned' sentinel for the unplaceable bucket"
  - "The verifier MIRRORS every vocabulary as a local literal (never imports the builder's), with drift guards making a divergence a red suite"
metrics:
  duration: "~4h"
  completed: "2026-08-03"
---

# Phase 136 Plan 12: Build Wiring B — Novelty, Visibility Axes, Curated Load, Verifier Summary

The last plan that edits `scripts/build_discovery_sidecar.py`: the bake now writes the ten-value
novelty shade on both evidence families with masked provenance, derives both VIS-01 visibility axes
while the raw evidence origin is still in scope, loads the curated genre/author artifacts by content
hash, makes a `kept_tie` audit row name its pair, and is checked field-by-field by twelve new
release-verifier checks.

## What was built

### Task 1 — novelty shade ingestion (commit `e5e1e5d7`)

`discovery_evidence` gains `novelty_source_label` and `divergence_correctness`; the values are ingested
at the centralized `(sys_id, novelty_work_key)` grain onto **both** `track1_direct` and `propagated`
rows. That coverage split is the whole point: the legacy flag was computed for the propagated family
only, which is why 144,294 shipped direct rows sit at a value meaning UNCHECKED.

- The verdict cache is **hash-pinned and has no unpinned load path** — `load_novelty_verdicts(path,
  sha256=None)` raises. A build input that is not the input that was measured is not a pinned input.
- **Fail-closed everywhere to `not_checked`**, with five tests, one per mode: no cache at all, a work
  with no reviewable identity, a partial snapshot (a run stopped at its cost ceiling), a stale entry
  carrying a retired vocabulary token, and a structured model abstention.
- **Provenance is written only through `masked_provenance_label`.** A build assertion restricts the
  distinct written labels to `MASKED_PROVENANCE_LABELS`, and its message names the count and the
  column but never the value — echoing it would perform the leak the assertion exists to prevent. A
  test seeds an unmasked value and proves the assertion fires.
- **The one-result-per-claim invariant (D-23a)** runs on every build, including the no-cache path. The
  live v1 asset has 665 claims whose evidence rows disagree on the legacy boolean; the build cannot
  produce a 666th.
- `container_predicts` stays distinguishable from `diverges_work`/`diverges_part` by `novelty_status`
  alone — no shared "hidden" flag column exists, asserted directly against `PRAGMA table_info`.

### Task 2 — the two visibility axes, `meta.audience`, `kept_tie` (commit `dcfcf04b`)

`discovery_evidence.assertion_visibility` and `works.identity_visibility`, both `NOT NULL DEFAULT
'private'`, derived by `shared/discovery_visibility.py` at ingest — before the raw origin is dropped.

- `_assertion_source_corpus` resolves the occurrence's **own raw `cat`** first. This is a real
  distinction, not a formality: `select_shown_works` fixes a work's IDENTITY corpus from a single
  representative row ("first by page_id"), while `track1_matches` carries a `cat` per ROW, so an
  individual occurrence's origin genuinely can differ from its work's. `_ingest_tier_a` now selects
  `cat` for exactly this reason. Sources that arrive as JSONL carry no per-row `cat` and fall back to
  the matched work's own ingest-time corpus; anything unresolvable fails closed to `private`.
- The **golden fixture now carries both mislabelling directions**: a restricted-identity work with an
  open assertion, and an open-identity work with a restricted assertion. Both are covered by tests
  that assert the corpus field alone gives the wrong answer.
- `is_public` is **not** restated in the builder — a tokenizer-based guard strips comments and string
  literals before checking, because the file discusses `is_public` at length and a naive substring
  assertion would only have been pinning that prose.
- `meta.audience = 'private'` on both build paths; the builder never writes `'public'` (only the public
  projection may), pinned by a source guard.
- `reconcile_launch_scope` runs over the built rows and lands in the build report with both counts and
  the symmetric difference by corpus × family, JSON-safe. It **reports, never resolves** — a test
  asserts running it leaves every stored axis byte-identical.
- **Amendment (F):** a `kept_tie` audit row now names the other member of its pair. It previously wrote
  NULL on every tie, making the pair unreconstructable from the audit alone — which matters because the
  main-pool rule's competition gate reads exactly those ties. Asserted before the INSERT, not after.

### Task 3 — curated artifacts, verifier, golden refresh (commit `ae20f6b8`)

Both curated artifacts load through the same hash-pinned fail-closed mechanism the existing curated
inputs use, and refuse three distinct tampering shapes: an unpinned load, a caller-pin mismatch, and a
payload edited after the artifact declared its own hash. Verified against the live artifacts:

| artifact | pin | result |
|---|---|---|
| `work_domains-v1.json` | `sha256:57393773…` | 1,073 assignments, 0 held |
| `work_author_aliases-v1.json` | `sha256:acce47f6…` | 96 rows, 76 matched |

The superseded pre-ruling pin (`sha256:4cc103ff…`) is refused, proven by test.

**Twelve registered verifier checks**, in the amendment's own subsection order, each independent of the
builder (vocabularies mirrored as local literals, per the verifier's standing convention, with drift
guards asserting every mirror equals its contract module). A sentinel test seeds a fabricated value into
every column the new checks read and asserts no violation message echoes it.

## Deviations from Plan

### Auto-fixed

**1. [Rule 1 — Bug] The plan's `divergence_correctness` acceptance criterion is wrong twice over**

- **Found during:** Task 1, before writing the DDL.
- **Issue:** the plan requires a test asserting the build/verifier "fails on a NULL value paired with
  `diverges_work`/`diverges_part`". Two independent problems. (a) **Owner ruling L** (2026-08-03) removed
  the field from the model's output contract entirely, so NULL is the *only* value any current build can
  write on a divergence row — requiring non-NULL would make every divergence row unbuildable and would
  silently delete ruling F's opt-in category from the asset. (b) Even setting ruling L aside, the CHECK
  literal in the schema doc **never enforced that direction**: under SQL three-valued logic a CHECK
  passes unless it evaluates to FALSE, and `NULL IN (...)` is NULL. I proved this against SQLite directly
  before deciding.
- **Fix:** kept the CHECK verbatim as the schema doc states it (identical to the mirrored
  `discovery_identification` CHECK); implemented and tested the direction that *is* enforceable and does
  matter (non-NULL ⇒ divergence shade ∧ in-vocabulary); and wrote an explicit test asserting the
  NULL-on-divergence case is **accepted**, naming ruling L, so a later plan cannot "fix" it back.
  Recorded as schema Amendment 2026-08-03 (L).
- **Files:** `scripts/build_discovery_sidecar.py`, `scripts/verify_discovery_sidecar.py`,
  `tests/test_discovery_build.py`, `tests/test_discovery_schema.py`, `docs/specs/discovery-sidecar-schema-v1.md`
- **Commits:** `e5e1e5d7`, `ae20f6b8`

**2. [Rule 1 — Bug] `discovery_identification` could never have carried a divergence shade**

- **Found during:** Task 1, tracing the same CHECK.
- **Issue:** 136-11 copied the CHECK verbatim onto `discovery_identification` and defaulted every row to
  `not_checked`, so the constraint was satisfiable only while novelty was unwired. Had the biconditional
  reading been correct, materializing the grain would have failed on the first divergence row.
- **Fix:** the grain now inherits `novelty_status`/`divergence_correctness` from its own best evidence
  row (the existing band-rank + lexicographic-`evidence_id` total order, reused rather than a second one
  invented). A test builds a `diverges_work` fixture and materializes the grain over it.
- **Commit:** `e5e1e5d7`

**3. [Rule 3 — Blocking] The golden fixture was stale since 136-11, and 136-20 depended on that staleness**

- **Found during:** Task 3's required golden refresh.
- **Issue:** the committed fixture carried **none** of the Amendment 2026-08-02 tables or columns — no
  `discovery_identification`, no `manuscript_display`, no `discovery_routing_audit`, no `coverage_ppm`,
  no `band_rank`, no `novelty_status`. 136-11 added the DDL but never regenerated it. Worse,
  `tests/fixtures/discovery_v2_fixture.py` (plan 136-20) was built on the premise that *the golden
  fixture IS the pre-rebuild shape* — true only by accident of staleness. Refreshing the golden inverted
  four readiness tests.
- **Fix:** refreshed the golden, and made the pre-rebuild case a **stated contract** instead of an
  accident: `_write_v1_shaped_copy` strips the amendment's additions from the golden, and both
  `materialize_pre_rebuild_sidecar` and `materialize_sidecar` build up from that base — so every
  `omit_*` knob behaves exactly as before and no consumer test needed changing.
- **Files:** `tests/fixtures/discovery_v2_fixture.py`, `tests/fixtures/discovery/*`
- **Commit:** `ae20f6b8`

**4. [Rule 1 — Bug] `_BAND_RANK_MAX`/`MIN` mis-stated the lattice**

- **Found during:** Task 3, first full-suite run.
- **Issue:** the schema doc's prose says the lattice runs 1…7, but the implementation
  (`shared.discovery_service._BAND_RANK_ORDER`) is 0-indexed with 8 entries (both the v1 and v2 top-tier
  keys are present through the transition) and an unknown pair ranking at 8. A v2 asset's
  `high_confidence_algorithmic` rows rank 0 and tripped my `[1, 8]` check.
- **Fix:** `[0, 8]`, with the reason recorded at the constant. The sentinel 8 is permitted here rather
  than rejected because `check_evidence_combinations` already rejects an unknown pair more precisely —
  duplicating it would report one defect as two.
- **Commit:** `ae20f6b8`

### Documented interpretations

**5. [Rule 3] "Write the author key" has no authorized destination column**

The plan's Task 3 says to "write `works.genre` and the author key". `works.author` is the only author
field the schema authorizes, the 2026-08-02 amendment states "Nothing outside this list is authorized to
appear in the asset", and 136-09 explicitly **deferred** author corrections. Adding an author-key column
would also flow straight into the public asset, since `_project_works` copies `works` rows verbatim.

Rather than invent an unauthorized column or silently skip the artifact, the build **loads it by hash and
ENFORCES it**: every distinct non-NULL `works.author` must appear in the pinned artifact or the build
fails, and the artifact's verified content hash is recorded in `meta`. Recorded as schema Amendment
2026-08-03 (M); a future plan wanting the FJMS person id materialized (the findings-page author facet,
136-16/136-18) owes its own dated amendment first.

**6. [Rule 3] Two 135/136-11-era tests pinned behaviour this plan supersedes**

`test_d17_within_delta_tie_demotes_neither` asserted `demoted_work_id is None` — the exact NULL
Amendment (F) forbids. `test_identification_columns_and_defaults_left_for_136_12` asserted all four
columns sat at their fail-closed defaults, which is what "left for 136-12" meant. Both were rewritten to
assert the superseding behaviour, keeping their original point intact (a tie still demotes neither row;
novelty still fails closed on the synthetic fixture).

**7. [Rule 3] Three tests depended on fixture staleness or on constraints that now hold the door shut**

`test_precision_reader_tolerates_missing_135_05_columns` asserted `"measurement_status" not in row`
against the golden fixture — true only because the fixture predated 135-05. It now **constructs** the
legacy shape, so it keeps testing tolerance rather than silently testing nothing; a sibling test asserts
the current fixture does carry the registry columns. The two cross-claim display-pointer tests are now
blocked at the DDL layer by the D-10a UNIQUE index, so they drop that index first — defence in depth
means both layers must work, and only one is testable while the other holds the door shut.

## Golden-fixture refresh — every assertion it changed, and why

The plan requires this stated explicitly; a golden fixture updated without a reason is how a regression
gets blessed.

| changed | why |
|---|---|
| `content_hash` `e71de1f1…` → `f6b16db4…` | the file gained the Amendment 2026-08-02 tables/columns and the new values |
| `frame_content_hash` `b5f7970e…` | **UNCHANGED** — it hashes only claim/evidence membership fields, so every addition here is provably additive |
| `display_evidence_choices` | **UNCHANGED**, byte-identical — no claim's display pointer moved |
| `row_counts` gains `discovery_identification: 18`, `manuscript_display: 0` | 136-11's tables materialize for the first time in the committed fixture |
| expected-json gains `identification`, `novelty` blocks | new build-report sections |
| `works.genre` `"Synthetic Genre A/B/C"` → `"Synthetic Parent A / Synthetic Leaf A"` etc., and two rows → `Unassigned` | the column now carries the curated value SHAPE (Amendment (J)); the `Unassigned` sentinel had to be reachable so the fixture proves it survives as a real, queryable value |
| C8 (`p008`/`w000006`) and C10 (`p010`/`w000008`) gain an explicit `assertion_source_corpus` | the two D-22 mislabelling directions, baked into the fixture rather than only into a unit test |
| `meta` gains `audience`, `work_domains_content_hash`, and the two 136-11 count keys | Amendments (C1) and (K) |

No evidence row, claim, span, band, routing status or `evidence_id` changed — which is exactly what the
unchanged `frame_content_hash` and `display_evidence_choices` prove.

## Schema amendment (dated 2026-08-03)

Five gaps closed: **(I)** authorizes `discovery_identification.eligibility_basis`, which 136-11 shipped
ahead of the contract (recorded as a STATE.md blocker by that plan — now resolved); **(J)** fixes the
`works.genre` stored value shape; **(K)** adds the three hash-pinned-input provenance meta keys and makes
`work_domains_content_hash` contractual whenever `genre` is populated; **(L)** states what the
`divergence_correctness` CHECK actually enforces and why ruling L makes that reading mandatory; **(M)**
records the no-author-column decision.

## Verification

- `pytest tests/ -k discovery` — **793 passed**, 3 skipped (baseline before this plan: 716).
- `python scripts/verify_discovery_sidecar.py tests/fixtures/discovery/discovery-v1-fixture.db` — clean,
  all invariants including the twelve new checks.
- Launch-scope reconciliation present in the build report: `vis01=16 / conjunction=17 /
  symmetric_difference=5`, broken down by corpus × family.
- `ruff check .` — clean, repo-wide.
- **Masking (D-25):** `--scan-repo` and `--scan-asset` over the refreshed fixture both exit 0 with
  `MASKING_SCAN_PATTERNS_FILE` pointing at the real gitignored pattern file; unsetting it exits 1
  (fail-closed proven, never a silent green).
- `tests/test_work_domains.py` + the repo's AST guards: 105 passed.

## Known Stubs

None. Every column this plan is authorized to add is written on both build paths.

`divergence_correctness` is NULL on every row — that is **not** a stub. It is the correct and only
possible value under owner ruling L, which removed the field from the model's job; the column, its
vocabulary, its CHECK and `novelty_columns_for`'s parameter are all live and tested, awaiting a
human/owner annotation pathway that no plan has yet been authorized to build.

## Threat Flags

None. No new network endpoint, auth path, file-access pattern or trust-boundary schema change beyond the
fields the 2026-08-02 amendment already authorizes and the 2026-08-03 amendment above adds.

## What remains for 136-13

1. **The production run is still UNAUTHORIZED** (ruling O): the ~$34 batched run needs its own explicit
   owner go, an explicit `cost_ceiling_usd`, and `BATCH_PROMPT_SHA256` in the cache key.
2. **The verdict cache must be merged before ingest.** `discovery_novelty_production_run.py` writes only
   the **model arm's** verdicts; the heuristic-funnel-resolved rows (ruling J's Arm 1 `confirms` and Arm 3
   `fills_gap`) are computed but never written to that file. 136-13 must merge both into one cache before
   pinning it, or every heuristically-resolved row will correctly-but-uselessly ingest as `not_checked`.
   The ingest is agnostic to which arm produced an entry — the merge is the only missing piece.
3. **Pin the merged cache** and pass `--novelty-verdicts` + `--novelty-verdicts-sha256`, plus
   `--work-domains`/`--work-author-aliases` and their content hashes, to `finalize_build`.
4. **Put the launch-scope reconciliation in front of the owner** — the build report now carries a real
   number; D-22 is explicit that the build must not resolve the disagreement itself.
5. `scripts/project_discovery_public.py` can now run end-to-end on a builder-produced asset: the
   `assertion_visibility`/`identity_visibility` columns it reads exist and are populated. It has not been
   exercised against a real bake yet.
6. The D-02b rebuild-preservation diff harness should be reviewed for the widened `novelty_status`
   vocabulary and the new `divergence_correctness` column before that gate runs (flagged by ruling F item 6).

## Self-Check: PASSED

All modified files present, all three task commits (`e5e1e5d7`, `dcfcf04b`, `ae20f6b8`) resolve in
`git log`, and all five subsections of schema Amendment 2026-08-03 are present in the contract file.
