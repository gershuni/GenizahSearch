# Discovery Frame v2 — corrected re-distill (Phase 135, plan 135-07)

> **Status: BUILT + VERIFIED, awaiting the 135-08 human production-deploy gate.**
> This frame supersedes the v1 reference frames in `discovery-frames.md`
> (local v1 builds `8e434513…`/frame `17bf5601…` and `89dfa444…`/frame
> `e538aa0b…`; neither was production-deployed — Phase 134 Task 3 deferred).
> Built 2026-07-26 by the 135-06 build logic over the real research corpus
> with all hash-pinned v2 inputs. Every gate below passed on the exact
> artifact named here. This document contains ONLY counts, hashes, opaque
> ids, and integer years — no corpus text, titles, or restricted names.

## 1. Frame identity & provenance

| Field | Value |
|---|---|
| `schema_version` | `discovery-v1` (schema unchanged; v2-ness is marked by `band_vocab_version`) |
| `band_vocab_version` | `v2` |
| `asset_basename` | `discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff` |
| DB `content_hash` | `33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff` |
| `frame_content_hash` | `53725098ece6cf152a72425587dc2fe9119261427fc82e008a5b953dcbd2bce7` |
| `source_db_sha256` | `1dc28d6d5ba44b91ecad883b8be38a1e9941f371d59219202fa37dc367b27a1f` |
| `crosswalk_sha256` | `bcde04bd460bbf6c91e354121109b793c4c1e7e9b203433ca640aed848345f0e` |
| `canonical_merges_sha256` | `cc054d111b9b4a76dd69912923ba50cd2b63f7820cb632617f645c12c207429a` |
| `composition_dates_sha256` | `2b46b4708ddccb9f26961dcb9ba6d62b23d64cc1da225d133af1be21bf2e9476` (7,443 entries, window [100,1600]) |
| `seftja_dates_sha256` | `0076028917c60044ac72ee36504c173b9e6decd0a5aef9890ec0f0fe934b22d7` (410 entries, window [100,1600]) |

Gates passed on this exact artifact: `verify_discovery_sidecar.py
--expected-frame-hash 53725098… --require-v2` (all invariants, incl. the
135-06 set: no-mixed-enum-state, narrowed never-orphan/non-displacement,
unknown-date-never-demoted, routing-audit full replay, measurement_status
consistency, reband-precision-invalidation, evidence_id-content-consistency)
= exit 0; strict masking gate (`--scan-sqlite <db> --scan-asset <db>
--scan-repo --strict`) = exit 0 (recorded in the 135-07 SUMMARY).

## 2. What v2 corrects vs v1

1. **Canonical merges + drop (owner census):** 16 same-work merges applied;
   1 work dropped (`w001239`, the contested attribution — dropped, not
   merged). Duplicate works no longer appear as independent co-claims.
2. **Band honesty (v2 vocabulary):** the v1 `expert_verified` top tier is
   renamed `high_confidence_algorithmic` (same 0.889 measured value; the
   name no longer overclaims human verification). `expert_verified` is
   absent from the artifact (verifier-asserted).
3. **Lever-1 coverage metric fixed (field-name collision):** routing now
   uses real page coverage = `matched_letters / norm-page-letters`
   (SEED-029 definition; 200-grade replication reproduced row-level 200/200
   and bands 94.0% / 91.7% / 37.5% exactly) instead of the edit-distance
   `density` (capped 0.35) that had demoted ~100% of witnesses.
4. **D-17 chronological demotion live at full coverage:** ordered-stateful
   router (cascade-safe), two date tables, `pair_coverage = 1.0` (floor
   0.99) after the 135-07 classical-strata recovery (see §6).

## 3. Stored row counts (release contract)

| Table | Rows |
|---|---|
| `works` | 1,269 |
| `discovery_claim` | 268,361 (100% carry a `display_evidence_id`) |
| `discovery_evidence` | 297,415 |
| `witness_units` | 5,547 |

`evidence_id` content-hash collisions observed and resolved: 187 (expected
class, handled by the deterministic collision policy).

## 4. Per-band × per-evidence_source counts

### 4.1 Deduped claim counts (by DISPLAY evidence row)

| evidence_source | confidence_band | claims |
|---|---|---|
| track1_direct | tier_a | 230,267 |
| propagated | not_evaluated | 20,435 |
| track1_direct | screening_canon | 9,967 |
| track1_direct | screening_rb | 4,801 |
| track1_direct | high_confidence_algorithmic | 1,083 |
| propagated | weak | 1,078 |
| propagated | corroborated | 730 |

By `claim_type`: direct_witness 197,177; quotes_this_work 59,243;
shared_text 11,941.

Display routing split: 166,537 claims display a `shipped` row; 101,824
display a `review_only` row (all-low-coverage pages — recoverable; the
narrowed non-displacement invariant guarantees no shipped sibling is ever
displaced by a review_only display row).

### 4.2 Stored evidence rows (all rows, not just display)

| evidence_source | evidence_kind | rows |
|---|---|---|
| track1_direct | witness | 254,612 |
| propagated | shared_text | 40,995 |
| propagated | witness | 1,808 |

Bands over all stored rows: tier_a 238,507; not_evaluated 40,995;
screening_canon 9,993; screening_rb 4,924; high_confidence_algorithmic
1,188; weak 1,078; corroborated 730.

## 5. Routing summary

| routing_status | routing_reason | rows |
|---|---|---|
| shipped | none | 187,070 |
| review_only | low_coverage (Lever-1) | 108,235 |
| review_only | later_shared_text (D-17) | 2,083 |
| review_only | co_citation | 27 |

Shipped by source: track1_direct 144,294 / 254,612 track1 witness rows
(56.7% ship under the corrected 0.45 coverage cliff — matching the
pre-bake simulated distribution); propagated 42,776.

## 6. D-17 chronological demotion — universe semantics & coverage

**Two numbers, two meanings (never conflate):**

- **Date-independent pairwise universe: 6,508** overlapping co-claim pairs
  (identical before/after the date recovery — membership never depends on
  dates). Reconciliation, exact: 4,208 tie pairs + 2,300 materially-later
  pairs + 0 undated pairs; of the material pairs, 2,298 are covered by a
  demoted row and **2** are the ratified Option-A invalid-reference no-row
  deferral (the earlier side had no currently-shipped overlapping spec).
- **Audit rows `U`: 6,270** (`discovery_routing_audit`): 4,208 `kept_tie` +
  2,062 `demoted` + **0** `fail_safe_unknown_date`. Rows are per demoted
  WORK per page, so multiple covered pairs collapse into one row — 6,270 is
  NOT "the universe", it is the row count.

`pair_coverage = 1.0000` (floor 0.99). D-17 demoted 2,062 works-on-pages
(2,083 evidence rows) as `later_shared_text` — all recoverable review_only.

**Composition-date provenance (135-07 recovery):** the M-source table was
regenerated to 7,443 entries after diagnosing that the upstream emitter's
own [500,1600] window had silently dropped the classical strata (127 works
with true years in [200,499] recovered from the owner date source; 39
pre-100 works recorded at the **antiquity floor 100 — a ROUTING FLOOR, not
a true composition year**; 1 post-1600 work and 6 non-M works left undated,
all verified degree-0). A release-contract gate
(`assert_composition_release_contract`: >= 7,443 entries, >= 166 pre-500,
>= 39 at-floor) HALTs any future `--release` build over a regressed table.

**Bounded known imprecision (counted, disclosed):** 40 of 2,062 demotions
have a recovered inexact-basis work on the demoted side; all 23
range-midpoint rows survive the strict interval rule using the true source
range starts, 14 of 17 upper-bound rows have antiquity-clamped demoters
(factually safe), leaving **3 residual rows** whose wrongness would require
the source's "before-N" bound to overstate the true date by 50–100+ years —
worst case a recoverable review_only routing. Interval-aware routing +
per-side `year_basis` audit columns are deferred to v2.1.

## 7. Per-band precision (`band_precision`, frozen release contract)

| scope | source / band | precision | interval |
|---|---|---|---|
| collection | propagated (corroborated ∪ weak) | 0.926 | [0.875, 0.968] (held-out 200-card draw, frame 2,109) |
| band | track1_direct / high_confidence_algorithmic | 0.889 | pre-registered E1 registry figure |
| band | track1_direct / screening_rb | 0.859 | pre-registered E1 registry figure |
| band | track1_direct / screening_canon | 0.647 | pre-registered (known Targum-confusion caveat) |
| band | track1_direct / tier_a | — | NO measured precision in the frozen contract (never fabricated); CERT-01 (135-09) measures it |
| band | propagated / corroborated, weak | — | no valid band-specific split (G8); see the collection row |

## 8. Deferred register (v2.1 / CERT-01 track)

- Interval-aware D-17 routing + per-side `year_basis` (closed vocab incl.
  `antiquity_floor`) audit columns.
- `kept_invalid_reference` audit provenance (the 2 no-row pairs).
- The pinned independent coverage anchor (`chrono_coverage_prebuild`) —
  explicitly superseded for this bake by the absolute 0.99 floor + the
  mutation-tested verifier.
- Full-corpus re-validation of the coverage framework before any PUBLIC
  precision certificate (CERT-01, owner-run, 135-09).
- Upstream emitter sync: widen its [500,1600] window + adopt the antiquity
  clamp so a re-emit reproduces (not regresses) the 7,443-entry table; sync
  the 410-entry SEF/JA table.

## 9. Lineage

v1 frames (both local-only, superseded): build `8e434513…` / frame
`17bf5601…`; build `89dfa444…` / frame `e538aa0b…`. The v1 manifest is
preserved at `discovery_data/manifest.v1-89dfa.backup.json` for rollback.
The live `discovery_data/manifest.json` now points at this v2 asset. Old
sibling `.db` files are left in place per the immutable-asset deploy
contract (`discovery-deploy.md`).
