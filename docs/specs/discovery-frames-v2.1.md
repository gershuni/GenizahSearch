# Discovery Frame v2.1 — additive rebuild + public projection (Phase 136, plan 136-13)

> **Status: BUILT + VERIFIED (all eight gates), awaiting the 136-13 owner deploy authorization.**
> This frame does NOT supersede `discovery-frames-v2.md` as a MEMBERSHIP frame — the membership is
> unchanged and the `frame_content_hash` is byte-identical. v2.1 records the ADDITIVE rebuild (new
> columns/tables), the first PUBLIC projection of that membership, and the public/private row
> reconciliation. Built 2026-08-03 over the same hash-pinned v2 inputs.
> This document contains ONLY counts, hashes, opaque ids and integer years — no corpus text, titles,
> or restricted names.

## 1. Frame identity & provenance

| Field | Private (never deployed) | Public (the deploy candidate) |
|---|---|---|
| `schema_version` | `discovery-v1` | `discovery-v1` |
| `band_vocab_version` | `v2` | `v2` |
| `meta.audience` | `private` | `public` |
| `asset_basename` | `discovery-v1-136rebuild` | `discovery-public-136rebuild` |
| DB `content_hash` | `9b4e740efaca09a89bc37d356c23864c433ac3460a20b9e508278b160bd6e07e` | `e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181` |
| `frame_content_hash` | `53725098ece6cf152a72425587dc2fe9119261427fc82e008a5b953dcbd2bce7` | `53725098…` (the SOURCE frame it was projected from) |

**On the public artifact's frame hash:** it records the private frame it was projected FROM, and
recomputing the membership-based hash over the public row set necessarily yields a different value —
the projection deliberately removes rows. `verify_discovery_sidecar.py --audience public` checks the
stored source frame instead of recomputing; demanding equality after a projection would be checking
that the projection did nothing.

Pinned inputs are unchanged from v2 and were re-verified against the LIVE asset's own `meta` before
the rebuild (`source_db_sha256` `1dc28d6d…`, `crosswalk_sha256` `bcde04bd…`, `canonical_merges_sha256`
`cc054d11…`, `composition_dates_sha256` `2b46b470…`, `seftja_dates_sha256` `00760289…`).

> **Correction carried by this frame:** the §4 rebuild command in `docs/specs/discovery-deploy.md`
> named `v2_canonical_merges.json`; the pinned `cc054d11…` is `v2_canonical_merges.build.json`, the
> slim masking-safe projection. Corrected in place (deploy-doc amendment 2026-08-03). Membership
> unaffected — both files carry the same 16 merges and the same `dropped_by_135`.

## 2. What v2.1 adds vs v2

Additive only; no membership change, no re-banding, no re-adjudication.

1. **Novelty axis** — `novelty_status` / `novelty_source_label` over the ten-shade vocabulary,
   ingested from a hash-pinned verdict cache (`eb6fc4f8…`, 65,200 entries). The cache is a BUILD-TIME
   artifact and is **never shipped inside the sidecar** (NOVEL-02).
2. **Visibility axes** — `assertion_visibility` / `identity_visibility`, the two-axis conjunction that
   the public projection consumes.
3. **Coverage + band rank** — `coverage_ppm` / `coverage_status` / `band_rank`.
4. **Identification grain** — the `discovery_identification` and `manuscript_display` tables.
5. **Curated work metadata** — `works.genre` from the hash-pinned domain artifact
   (`sha256:57393773…`, 1,073 canonical works, 0 held for ruling); the author-alias artifact
   (`sha256:acce47f6…`) is bound by an enforced coverage check and writes no column.
6. **`kept_tie` repair** — schema amendment (F): all 4,208 `kept_tie` audit rows now name the work
   they beat (the live v2 asset has 4,208 with NULL). Same tie population, now replayable.

## 3. Row reconciliation, private vs public

| table | private | public | withheld |
|---|---|---|---|
| `works` | 1,269 | 613 | 656 |
| `discovery_claim` | 268,361 | 231,244 | 37,117 |
| `discovery_evidence` | 297,415 | 251,547 | 45,868 |
| `discovery_identification` | 64,522 | 53,581 | 10,941 |
| `witness_units` | 5,547 | 1,959 | 3,588 |
| `witness_unit_members` | 19,554 | 8,587 | 10,967 |
| `manuscript_display` | 44,375 | 39,518 | 4,857 |
| `discovery_routing_audit` | 6,270 | 4,814 | 1,456 |
| `band_precision` | 7 | 7 | 0 (external pre-registered measurements, copied verbatim) |
| `meta` | 22 | 22 | 0 (row-count keys RECOMPUTED, never copied) |

`discovery_identification` is materialized by the production builder against the projected public
tables — one rule for both sides — and its key set is asserted to be a subset of the private key set.

**Dependency-closure prunes** (reported here, in the offline frame record, and in the projection's
reconciliation report — never inside the deployed artifact):

- **164** evidence rows whose `routing_reason='later_shared_text'` was backed by an audit row naming a
  non-public work. Owner ruling 2026-08-03: the artifact must not assert a routing decision it cannot
  substantiate, and redaction would still disclose that a hidden competitor exists.
- **54** evidence rows dropped as a cascade — claims asserting a witness relation (31 `direct_witness`
  + 23 `quotes_this_work`) that lost their last witness-kind row.

## 4. Launch scope (ruling S)

The VIS-01 launch-scope shortcut and the two-axis conjunction disagree on **36,989 of 297,415 rows
(12.4%)**, measured by direction:

| | rows | corpus × family |
|---|---|---|
| conjunction ships, shortcut would not | 24,094 | `ja` × `track1_direct` |
| shortcut would ship, conjunction does not | 12,895 | `msource` × `propagated` |

The artifact ships the **conjunction**. Owner ruling S: JA direct matches ship — the CERT-01 graded
deck contains 44 JA works among its 220 candidate-role cards (vs 133 Sefaria, 43 restricted), so the
certificate covers those rows rather than being stretched over them.

`_vis01_shortcut` returns True for EVERY `propagated` row regardless of corpus and would therefore
publish restricted-identity material. **It is a stale, unsafe rule and must never gate publication
again** — it survives only as one input to this reconciliation.

## 5. Gates passed on these exact artifacts

Full battery in `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-REBUILD-GATES.md`.

1. Rebuild preservation vs the externally pinned expectation — PASS
2. Recomputed population / cluster-map / stratum hashes — PASS
3. CERT-01 card binding — PASS (240 of 280 bind identically; 40 are the deck's own controls)
4. Release verification, private asset — PASS
5. Release verification, public projection (`--audience public`) — PASS
6. Masking, BOTH artifacts, `--strict --scan-repo --scan-asset --scan-sqlite`, with
   `MASKING_SCAN_PATTERNS_FILE` set — PASS, clean
7. Golden fixture / discovery suites — PASS (413 tests)
8. Performance — PASS; findings default ordering **159 ms p95 against a 1,500 ms cap** (the D-10a
   shape was 3.41–3.55 s), actuals written into `docs/specs/discovery-budgets.md`
9. Launch-scope reconciliation — REPORTED, then ruled (§4)
