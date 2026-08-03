---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 11
subsystem: discovery-bake
tags: [discovery, bake, schema, perf, PANEL-01, PANEL-02, D-08a, D-10a, D-13g, D-17a]
requires:
  - "136-06 (D-02a tier_a authorization lockstep -- band_precision measurement_status/ci_low, read by the main-pool gate 2)"
  - "136-07 (shared/discovery_main_pool.py -- main_pool_decision + the closed reason vocabulary)"
  - "docs/specs/discovery-sidecar-schema-v1.md Amendment 2026-08-02 (A)/(B)/(B1)/(C1)/(D)"
provides:
  - "discovery_evidence.coverage_ppm + coverage_status (D-08a, direct family only)"
  - "discovery_evidence.band_rank (materialized, equal to the runtime lattice)"
  - "discovery_evidence.novelty_status column + its D-10a index (values are 136-12's)"
  - "discovery_identification (the identification grain, main_pool + reason materialized)"
  - "manuscript_display (library + shelfmark sort keys from libraries.csv only)"
  - "the D-10a index set incl. UNIQUE discovery_claim(display_evidence_id)"
  - "scripts/bench_discovery.py::bench_findings_page() + the §4.4 budgets slot"
affects:
  - "136-12 (novelty + visibility values; owes the schema-doc amendment for eligibility_basis)"
  - "136-13 (the rebuild + gate battery; first run that can measure the findings shapes)"
  - "136-14/15/16/17/18 (the panel + findings page read these two tables)"
  - "scripts/project_discovery_public.py (already carries projection rules for both new tables)"
tech-stack:
  added: []
  patterns:
    - "shared rule imported, never re-implemented (main_pool_decision, _BAND_RANK_ORDER)"
    - "validity status as a separate axis from the value it qualifies"
    - "build-time consistency assertions that fail the build rather than the total"
key-files:
  created:
    - ".planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-11-SUMMARY.md"
  modified:
    - "scripts/build_discovery_sidecar.py"
    - "scripts/bench_discovery.py"
    - "tests/test_discovery_build.py"
    - "docs/specs/discovery-budgets.md"
decisions:
  - "coverage_status maps missing-denominator to NULL coverage_ppm, never to a stored zero"
  - "eligibility is shipped OR human_confirmed at the identification grain (D-13g, second half)"
  - "eligibility_basis added as a NULLABLE column -- schema-doc amendment owed at 136-12"
  - "manuscript_display eligibility matches the identification grain, a superset of shipped-only"
  - "the findings probe measures SQL directly; the service read path does not exist yet"
metrics:
  duration: "~2h"
  completed: "2026-08-03"
  tasks: 3
  commits: 3
  tests_added: 62
---

# Phase 136 Plan 11: Build Wiring A — Sort Keys, Identification Grain, Findings Benchmark

Persisted the two sort keys the surfaces cannot compute at request time (`coverage_ppm` +
its validity status, `band_rank`), materialized the identification grain and the manuscript
display keys, added the D-10a index set, and landed a findings-page benchmark that names the
3.41–3.55 s / 16 s baseline it must beat.

## Accomplishments

**Task 1 — coverage, band rank, indexes** (`e8f094b8`)

The coverage metric was already computed at ingestion and thrown away: `_mk_evidence`'s returned
dict had no `coverage` key, so the value never reached either `INSERT INTO discovery_evidence`.
It is now threaded through both column lists as `coverage_ppm` (fixed-point integer), direct
family only per D-08a, with `coverage_status` as a **separate validity axis** —
`compute_page_coverage` returns `0.0` on a missing denominator, which is not the same fact as a
genuine near-zero match, so a missing denominator stores `NULL` + `no_denominator` and never a
coverage of zero (T-136-11-04). `norm_stream_letter_count` and `compute_page_coverage` are
untouched; the computation was always correct.

`band_rank` is materialized from `shared.discovery_service._band_rank`, **imported rather than
re-declared** — a second literal ordering in the builder is precisely the drift that would make
rows appear in a different order depending on which code path produced the ordering
(T-136-11-02). A test asserts the full mapping, not a sample.

The D-10a index set landed, including the UNIQUE index on `discovery_claim(display_evidence_id)`
(verified unique on the live v2 asset: 268,361 claims / 268,361 distinct values) and an index on
the novelty **status** column rather than the legacy `is_new` boolean.

**Task 2 — the identification grain** (`f7f940ab`)

`discovery_identification` is one row per `(sys_id, canonical_work_id)`, so the D-13a duplicate
collapse is structural: every count derived from the table is already deduplicated. Eligibility
is `shipped` **OR** `human_confirmed` — the second half of the D-13g fix, since a shipped-only
table would drop the restored review-only rows one layer down regardless of join type.
`display_work_id` resolves through the schema §(B1) ordered total rule, and a test demonstrates
the 1 → 3 fan-out a `canonical_work_id` join produces on a duplicated group. `main_pool` /
`main_pool_reason` come from `shared.discovery_main_pool`; a test stubs the shared predicate and
watches the stored bucket follow it.

`manuscript_display` reads `libraries.csv` and nothing else — no title, no reference text, no
locus — picking the shelfmark exactly as `shared/metadata_manager.py` already does, with
digit-run-padded sort keys so "T-S 12.9" sorts before "T-S 12.123".

**Task 3 — the findings benchmark** (`4f317ac3`)

`bench_findings_page()` measures six named shapes with p50/p95/max, drawing filter values live
from the asset so no shape can silently benchmark an empty query, and naming the prior failing
measurement in its assertion message. A shape over its cap prints its SQLite query plan and
exits nonzero; no cap is relaxed.

## Measured numbers (live deployed v2 asset, read-only)

| Figure | Value |
|---|---|
| Distinct `(sys_id, canonical_work_id)` pairs, shipped only | **64,509** (matches the schema doc exactly) |
| Same, under `shipped OR human_confirmed` | **64,522** |
| **Identifications restored by the D-13g fix** | **13** |
| Duplicated `canonical_work_id` groups on `works` | **15** |
| Claims / distinct `display_evidence_id` | **268,361 / 268,361** (UNIQUE is a real invariant) |
| Max distinct canonical works on one page | **9** (so gate 3's pairwise competition check is cheap) |
| Max evidence rows on one page | **20** |
| `kept_tie` routing-audit rows | **4,208** |

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 — Bug] `--write-budgets` would have deleted the findings-page CAP section**
- **Found during:** Task 3
- **Issue:** `_write_budgets` replaced everything from `## 4. Measured Actuals` to the next
  `\n---\n`. Since the 2026-08-02 amendment added §5, that delimiter now sits *after* §5 — so a
  `--write-budgets` run would have silently deleted the findings-page caps this very probe is
  measured against. A benchmark that can rewrite the number it is judged by is not a gate.
- **Fix:** the §4 region this script owns now ends at `### 4.2`; a separate `_upsert_findings_block`
  replaces or inserts §4.4. A test asserts §1, §2 and §5 are byte-identical after a write.
- **Commit:** `4f317ac3`

**2. [Rule 1 — Bug] `--write-budgets` would have destroyed the recorded prod-box actuals**
- **Found during:** Task 3
- **Issue:** the same replacement block re-wrote §4.2 as "PENDING", overwriting the human-measured
  prod-box actuals of 2026-07-28 with a placeholder.
- **Fix:** §4.2 and §4.3 are now left untouched; a test pins `**0.49 ms** ✓` surviving a write.
- **Commit:** `4f317ac3`

**3. [Rule 3 — Blocking] `populate_synthetic`'s `""` display-evidence placeholder**
- **Found during:** Task 1
- **Issue:** the synthetic path inserts claims with an empty-string `display_evidence_id` and
  backfills it in a second pass. The new UNIQUE index rejects the second such row.
- **Fix:** the placeholder is now the claim's own `claim_id` (unique per claim, overwritten by the
  backfill before anything reads it). `claim_id` and `evidence_id` are sha256 digests from
  different recipes, so a placeholder can never collide with a real winner.
- **Commit:** `e8f094b8`

**4. [Rule 3 — Blocking] `libraries.csv` sys_id normalization**
- **Found during:** Task 2
- **Issue:** `shared/metadata_manager.py` keys the catalogue on a digits-only sys_id, which is
  right for production but silently drops any non-numeric identifier.
- **Fix:** `_load_manuscript_catalogue` indexes both the digits-only and the raw form, with the raw
  form winning a collision — an exact match is never displaced by a normalized one.
- **Commit:** `f7f940ab`

### Plan ↔ contract drift (needs a decision downstream, not fixed here)

**A. `eligibility_basis` is not in the schema doc's authorized column list.**

The plan's Task-2 action text requires it twice ("carry a column recording which of the two
admitted it", and an acceptance criterion asserting "the column recording which rule admitted
it"), while the same task's first acceptance criterion requires "exactly the column sets from the
schema amendment" — and the amendment's `discovery_identification` DDL has 14 columns, none of
them this one, under the words "Nothing outside this list is authorized to appear in the asset."

Resolution taken: the column is added **NULLABLE**, CHECK'd to `{shipped, human_confirmed}`. That
satisfies the plan's explicit requirement, keeps all 14 authorized columns exactly as specified,
and — critically — keeps `scripts/project_discovery_public.py` working unchanged, since it
inserts only the 14 keys it returns and a NOT NULL column here would break the public projection
outright.

**Owed:** a dated amendment adding `eligibility_basis` to
`docs/specs/discovery-sidecar-schema-v1.md` § Amendment 2026-08-02 (B). Plan **136-12** already
edits that contract and is the natural home. If the amendment is declined, the column must be
dropped and the D-13g coverage note derived some other way — but not from `main_pool_reason`,
which does not carry the same fact (a *shipped* row can also be `human_confirmed`).

**B. `band_rank` numbering: the schema doc says 1…7, the runtime lattice is 0…7.**

Amendment (A) describes `band_rank` as "`track1_direct`/`expert_verified` = 1 … `propagated`/
`not_evaluated` = 7". The runtime `_band_rank` is 0-based over an 8-entry lattice (the v1/v2
read-compat window lists the strongest track1_direct band under both keys at the same top
position), with 8 as the unranked sentinel. The plan's acceptance criterion is explicit and wins:
"`band_rank` values equal `shared.discovery_service._band_rank`". Stored values are therefore
0-based. The schema doc's prose is a loose description of the same §6 lattice, not a second
numbering; a future amendment should say "equal to `shared.discovery_service._band_rank`" rather
than restating indices.

**C. The `coverage_status` enum cannot separate "no denominator" from "never computed".**

The plan's must-have asks for a status that "distinguishes 'no denominator' from 'never
computed'". The schema freezes a three-value enum `{measured, no_denominator, not_applicable}`
with `not_applicable` reserved for propagated rows, so both sub-cases of an unmeasurable direct
row map to `no_denominator`. The distinction that actually matters — "we could not measure"
(NULL) versus "we measured almost nothing" (a small integer) — **is** made, and is the one
T-136-11-04 names. Widening the enum would need its own dated amendment and was not taken.

**D. `identification_id`'s recipe now has two implementations.**

`scripts/project_discovery_public.py` implements the frozen §(B) recipe verbatim because no
helper existed; the builder now does too, and `scripts/discovery_ids.py` is outside this plan's
closed `files_modified` list. Mitigated by
`test_identification_id_recipe_matches_the_public_projection`, which pins the two to each other,
so a drift is a red suite. A later plan should centralize it into `scripts/discovery_ids.py`.

## Known Stubs

| Stub | File | Reason |
|---|---|---|
| `discovery_identification.novelty_status` always `not_checked` | `scripts/build_discovery_sidecar.py` | Fail-closed default. Values are computed by **136-12**, which the plan explicitly assigns ("Leave `novelty_status`, `assertion_visibility` and `identity_visibility` for plan 136-12"). `not_checked` is not a placeholder — it is the correct, honest value for an unrun check. |
| `discovery_identification.assertion_visibility` / `identity_visibility` always `private` | same | Same assignment. Fail-closed: public eligibility requires BOTH to be `public` (D-22), so an un-derived row can never leak public by default. |
| `discovery_evidence.novelty_status` always `not_checked` | same | The column + its D-10a index are created here because the authorized index set names the status column; the values are 136-12's. |
| `manuscript_display` empty on the synthetic path | same | The synthetic fixture has no `libraries.csv`, and that file is the only sanctioned source (T-136-11-03). Never back-filled from elsewhere. |

None of these prevent the plan's goal. The findings page cannot ship a novelty toggle until
136-12 lands, which is exactly the sequencing the phase already assumes.

## Threat Flags

None. No new network endpoint, auth path, file-access pattern or trust-boundary schema change was
introduced. `manuscript_display` crosses catalogue metadata into the asset, which is the
`libraries.csv`-only boundary the plan's own threat register anticipates (T-136-11-03), asserted
by `test_manuscript_display_carries_only_libraries_csv_catalogue_fields`.

## Verification Results

- `tests/test_discovery_build.py` — **128 passed** (92 → 128; 36 new here, 62 across the plan
  counting the ones under other `-k discovery` files touched).
- Full `-k discovery` suite — **684 passed, 3 skipped** (622 before this plan; 0 failures).
- `python -m ruff check` — clean on all three modified source files.
- `python scripts/bench_discovery.py --help` — documents the three new findings flags.
- Findings probe against the **live deployed v2 asset** — skips cleanly, naming all six shapes and
  the two absent tables; no exception, no fabricated number.
- Findings probe against a synthetic fixture — five shapes measured (deep paging correctly skipped
  at 18 identifications), all under cap, nonzero rows asserted.
- Masking gate (`MASKING_SCAN_PATTERNS_FILE` set, `--scan-repo`) — **no matches, clean.**

## Issues Encountered

- **`scripts/project_discovery_public.py` cannot yet run end-to-end** against a builder-produced
  asset: it reads `discovery_evidence.assertion_visibility` and `works.identity_visibility`, which
  **136-12** adds. This is pre-existing (those columns never existed) and is not a regression from
  this plan — its own unit tests, which build a standalone fixture schema, still pass. Flagged so
  136-12 knows the projection becomes runnable on its watch.
- The plan's Task-3 acceptance criterion asking for a written actuals table with real p50/p95/max
  is **not satisfiable before the 136-13 rebuild** — the tables it measures do not exist on the
  deployed asset. The mechanism is implemented and unit-tested against a fabricated measured
  result; the budgets doc records §4.4 as PENDING with the reason, and the probe fills it in
  automatically on the first run against a rebuilt asset. No number was invented.

## Next Phase Readiness

136-12 can wire novelty and the visibility axes straight onto the columns and grain that now
exist. It additionally owes: the `eligibility_basis` schema amendment (drift A above), and the
verifier extensions for both new tables.

## Self-Check: PASSED
