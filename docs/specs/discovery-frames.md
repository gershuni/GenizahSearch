# Discovery Frame v1 — reference build, SUPERSEDED-PENDING (Phase 134, DATA-07)

> **Status: SUPERSEDED-PENDING (2026-07-23).** Owner review of this `discovery-v1`
> build (2026-07-23) found three defects that disqualify it from certification/
> release: (1) cross-corpus **duplicate works** (`canonical_work_id` never
> populated — same composition once per corpus); (2) **band-label overclaim**
> (`expert_verified` = 121 human_confirmed + 1,067 unreviewed; `tier_a` = 238,618
> all unreviewed); (3) **anthology/quotation false positives** (a page top-ranked
> to a later work that merely quotes its actual text, e.g. T-S C 2.191). The frame
> will be **re-distilled to `discovery-v2`**. Ownership of the fixes: the SEED-029
> / R-source track owns (A) the relation-aware canonical merge and (C) the
> direction-aware shadow router (its current shadow is density-only and mis-routes
> ~26% of anthology co-claims, so it is NOT yet direction-correct); the
> discovery-spine track owns (B) band-label honesty (`docs/specs/discovery-band-labels-v1.md`,
> landed) + the v2 re-distillation plumbing. Phase 135 (CERT-01) grades **v2**,
> not this build.
>
> This document remains an accurate **reference record of what `discovery-v1`
> IS** (identity, counts, contracts) for the v2 work to build against — it is NOT
> a certified/shipped frame. A new `discovery-frames-v2.md` supersedes it at the
> v2 bake; never edit membership numbers here in place. All source names are
> MASKED (codenames only); this file is committed and MUST stay masking-clean
> (`M-source` codename, never the restricted source name).

## 1. Frame identity & provenance

| Field | Value |
|---|---|
| `schema_version` | `discovery-v1` |
| `sidecar_version` | `discovery-v1-real` |
| **`frame_content_hash`** | **`17bf5601bc1ef89404ee5ccdeb1ce9616f3e3274432c4297f79b5c8a99ba6efd`** |
| DB `content_hash` | `8e43451300429ed4ace5e29e5513359a29674ac49731d5c969eb1d607e0ca065` |
| DB asset | `discovery-v1-8e43451300429ed4ace5e29e5513359a29674ac49731d5c969eb1d607e0ca065.db` (gitignored `discovery_data/`; deployed asset-first in 134-08) |
| `source_db_sha256` | `1dc28d6d5ba44b91ecad883b8be38a1e9941f371d59219202fa37dc367b27a1f` |
| `crosswalk_sha256` | `bcde04bd460bbf6c91e354121109b793c4c1e7e9b203433ca640aed848345f0e` |
| `htr_snapshot_hash` | `feadfd52567ec03f118b39c1a8d2c4e7c6f1359ff66d80542b2edb2bc700e71b` |
| `data_as_of` | 2026-07-22 |
| `build_date` | 2026-07-22T20:13:42Z |

**Assertion (DC3):** the `frame_content_hash` above EQUALS both the built DB's
`meta.frame_content_hash` and the `manifest.json` `frame_content_hash`. Verified
by `scripts/verify_discovery_sidecar.py <DB> --expected-frame-hash <hash>` → exit 0,
and by the strict blocking masking gate (`check_atlas_masking.py --scan-sqlite
<DB> --scan-asset <DB> --scan-repo --strict`) → exit 0. The membership-based hash
recipe (volatile meta excluded) is defined in `docs/specs/discovery-sidecar-schema-v1.md`.

## 2. Corpus scope statement

The reference sources present at THIS distillation:

- **Open-corpus:** Sefaria + JA (Judeo-Arabic / Friedberg).
- **M-source literary** (masked codename): the owner-reviewed literary subset only.

Gen-2 **R-source** is NOT in this frame; when it is ingested it becomes a NEW
versioned frame (`discovery-v2`), never an in-place addition to this one.

### 2.1 Owner title-review outcome (134-07 human gate)

Works ship ONLY with an owner-approved **neutral** title (fail-closed: no title →
excluded; no research-title fallback). Result:

| | Works |
|---|---|
| **Shipped** | **1,270** |
| &nbsp;&nbsp;• Sefaria | 508 |
| &nbsp;&nbsp;• JA | 106 |
| &nbsp;&nbsp;• M-source (owner-titled) | 656 |
| Excluded — open-corpus, owner-removed (compilations "surely M-sourced") | 11 |
| Excluded — M-source, **removed permanently** (late / Haskalah-period) | 26 |
| Excluded — M-source, **deferred** (uniquely-M-sourced; addable in a later presentation pass via the stable crosswalk) | 108 |
| Excluded — M-source, omitted (unmarked) | 7 |

Every opaque `work_id` is stable in `discovery_data/crosswalk.json`, so deferred
works can be added by a future id-stable re-distillation without disturbing shipped ids.

**Metadata:** each work carries `neutral_title` + optional owner-vetted `author`
(625 / 1,270 filled); **`genre` is intentionally empty** this frame (owner decision —
the FJMS-derived genre was error-prone and dropped rather than shipped unvetted).

## 3. Stored row counts (release contract)

| Table | Rows |
|---|---|
| `works` | 1,270 |
| `discovery_claim` | 268,490 |
| `discovery_evidence` | 297,559 |
| `witness_units` | 5,547 |
| `band_precision` | 7 |

These equal the `meta.expected_rows_*` release-contract keys (verifier-enforced).

## 4. Per-band × per-evidence_source counts (deduped)

`discovery_claim` PK is `(page_id, work_id)`; each claim carries exactly ONE
`display_evidence_id`. "Deduped claim count per band" = claims grouped by the
band of their DISPLAY evidence row (so it sums to the total claim count).

### 4.1 Deduped claim counts (by display-evidence band)

| evidence_source | confidence_band | claims |
|---|---|---:|
| track1_direct | tier_a | 238,618 |
| propagated | not_evaluated (shared_text) | 11,953 |
| track1_direct | screening_canon | 9,993 |
| track1_direct | screening_rb | 4,930 |
| track1_direct | expert_verified | 1,188 |
| propagated | weak | 1,078 |
| propagated | corroborated | 730 |
| **total** | | **268,490** |

### 4.2 Stored evidence counts (all evidence rows, not just display)

| evidence_source | confidence_band | evidence |
|---|---|---:|
| track1_direct | tier_a | 238,618 |
| propagated | not_evaluated (shared_text) | 41,022 |
| track1_direct | screening_canon | 9,993 |
| track1_direct | screening_rb | 4,930 |
| track1_direct | expert_verified | 1,188 |
| propagated | weak | 1,078 |
| propagated | corroborated | 730 |
| **total** | | **297,559** |

### 4.3 By claim_type / evidence_kind

| claim_type | claims | | evidence_kind | evidence |
|---|---:|---|---|---:|
| direct_witness | 197,214 | | witness | 256,537 |
| quotes_this_work | 59,323 | | shared_text | 41,022 |
| shared_text | 11,953 | | | |

## 5. Within-key dedup formula (display_evidence_id)

Multiple evidence rows can attach to one claim key `(page_id, work_id)`. The single
`display_evidence_id` is chosen by a **deterministic total-precedence lattice** over
`(evidence_kind, evidence_source, confidence_band, …)` with fully-ordered tie-breaks
— the 21-cell precedence table is specified in `docs/specs/discovery-sidecar-schema-v1.md`
(C-5). No probability is ever computed; selection is purely structural.

### 5.1 Overlap / collision resolution counts

- **29,042** claim keys carry BOTH `witness` and `shared_text` evidence; each is
  resolved to exactly one `display_evidence_id` by the precedence lattice
  (witness evidence outranks shared_text for the display pointer).
- **187** evidence-id collisions were resolved deterministically at build time
  (build-side dedup within the frozen id recipe; child-uniqueness of `evidence_id`
  is verifier-enforced).

## 6. DATA-10 unit × work projection (DISPLAY-time, NOT a claim collapse)

The stored claim key stays the real `(page_id, work_id)`. Physical-manuscript
grouping is a **display-time projection**: witness claims (`direct_witness` +
`quotes_this_work`) for a work are grouped by physical-MS `unit_key =
COALESCE(witness_unit_members.unit_id, 'sys:'||sys_id)` and ONE representative row
is shown per unit (highest band; fully-ordered tie-break — `_project_work_witnesses`
/ the `ROW_NUMBER() OVER (PARTITION BY unit_key …)` SQL mirror each other).

| Measure | Count |
|---|---:|
| Raw stored witness claims (`direct_witness` + `quotes_this_work`) | 256,537 |
| DATA-10 projected (work × unit_key) display rows | 107,006 |
| &nbsp;&nbsp;• of which multi-folio, grouped to a real physical `unit_id` | 6,411 |
| Rows collapsed by the projection (raw − projected) | 149,531 |

This is a PROJECTION applied per-query at display time; it does NOT alter the
268,490 stored claims. (`shared_text` claims are not part of the witness projection.)

## 7. C-7 per-band precision reporting (`band_precision` table)

Precision lives in the `band_precision` TABLE INSIDE the hashed DB (F13 — populated
during `finalize_build` before the content hash; never a post-hoc UPDATE). Phase 135
BAND-02 reads it with no code change. **Labels are registry-gated: the word
"certified" is PROHIBITED** in this frame — labels are "expert-verified" and
"algorithmically supported by matching witnesses." No per-row probability is ever written.

| scope | collection_id | evidence_source | band | num/denom | precision [CI] | method / policy |
|---|---|---|---|---|---|---|
| **collection** | propagated_witness_collection_v1 | — | — | 176 / 190 | **0.926 [0.875, 0.968]** | work-cluster bootstrap; locked-rule evaluation |
| band | propagated_witness_collection_v1 | propagated | corroborated | — | *provisional, no number* | locked-rule evaluation |
| band | propagated_witness_collection_v1 | propagated | weak | — | *provisional, no number* | locked-rule evaluation |
| band | e1_certification_registry_v1 | track1_direct | expert_verified | — | **0.889** | E1 registry pre-registered |
| band | e1_certification_registry_v1 | track1_direct | tier_a | — | *no number* | — |
| band | e1_certification_registry_v1 | track1_direct | screening_rb | — | **0.859** | E1 registry pre-registered |
| band | e1_certification_registry_v1 | track1_direct | screening_canon | — | **0.647** | E1 registry pre-registered |

**Interpretation (C-7):**
- The **0.926 [0.875, 0.968]** attaches at the **propagated witness COLLECTION**
  level (corroborated ∪ weak) — a work-cluster bootstrap over the full router-cleaned
  witness collection under its exact locked rule (held-out 200-card draw = 90
  corroborated + 110 weak, determinate 176 / 190). It is **NOT** a corroborated-only
  interval.
- `corroborated` ranks above `weak` **structurally** (two-seed vs one-seed) but
  NEITHER carries a manufactured separate band interval; both ship PROVISIONAL with
  the collection-level 0.926 as the only measured propagated number. The ~802
  adjudicated cards are never pooled/split across frames (so post-hoc corroborated-only
  81/86 and weak-only 95/104 are NOT valid band intervals).
- The three `track1_direct` screening/verified bands carry their own pre-registered
  E1-registry numbers (0.889 / 0.859 / 0.647). `tier_a` and `not_evaluated` carry no
  precision number.
- `expert_verified`: the 174 individually-adjudicated rows carry
  `adjudication_status=human_confirmed` (a display/adjudication axis, NOT a separate
  precision); the 0.889 is the band-population precision.

## 8. Frame lineage

- Corrected two-family, evidence-source-axis model (134 CONTEXT C-1..C-9):
  `discovery_claim` + 1-to-many `discovery_evidence`; `evidence_source` ∈
  {track1_direct, propagated}; `claim_type` ∈ {direct_witness, quotes_this_work,
  shared_text}; per-evidence_source bands; Q2 recall-ladder / cluster-propagation
  included.
- Reproducible rebuild = the exact `scripts/build_discovery_sidecar.py` invocation
  (positional `fullcorpus_v2.db` + `--from-approved <approved.csv> --crosswalk
  discovery_data/crosswalk.json --research-data-dir <collections> --libraries-csv
  --fjms-db --release --frozen-precision-defaults`, with `MASKING_SCAN_PATTERNS_FILE`
  set), reusing the durable crosswalk for id stability; source-DB + crosswalk hashes
  pinned in `meta` (above).
- On-disk size = **368.5 MB** (owner-accepted 2026-07-22; the ≤300 MB figure was a
  working target, not a numbered cap — the only hard budget contract is RSS ≤ 250 MB,
  measured on the prod box in 134-08 / PERF-01).
