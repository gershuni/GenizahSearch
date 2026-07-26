---
phase: 135-precision-certificate-confidence-bands
plan: 07
subsystem: database
tags: [discovery, sidecar, v2-bake, cert-01, lever-1, d17, verifier]
status: complete
requires:
  - phase: 135-06
    provides: "v2 build logic (canonical merge + drop + date inputs + Lever-1 + D-17 + reband) + 7 verifier invariants"
provides:
  - "BUILT + VERIFIED v2 asset discovery-v1-33499c5b….db (gitignored) + manifest.json pointing at it"
  - "docs/specs/discovery-frames-v2.md (frozen frame identity, counts, provenance, disclosures)"
  - "corrected Lever-1 coverage metric (matched_letters/norm-page-letters) + SEED-029 replication gate"
  - "ordered-stateful cascade-safe D-17 router + §4.3 year-resolution contract"
  - "composition_dates.json 7,443 entries ([100,1600] window + antiquity clamp) + release-contract gate"
affects: [135-08, 135-09]

tech-stack:
  added: []
  patterns: ["counted-impact-audit as Codex deferral basis", "release-only semantic regression gate on a pinned data input"]

key-files:
  created:
    - docs/specs/discovery-frames-v2.md
    - tests/test_discovery_coverage_replication.py
  modified:
    - scripts/build_discovery_sidecar.py
    - scripts/discovery_ids.py
    - scripts/verify_discovery_sidecar.py
    - tests/test_discovery_v2_bake.py
    - tests/test_discovery_ids.py
    - docs/specs/discovery-v2-bake-plan.md

key-decisions:
  - "Lever-1 blocker root-caused as a field-name collision (best_density edit-distance fed as coverage) — fixed to the SEED-029 metric, proven by exact 200-grade replication (94.0/91.7/37.5)"
  - "never-orphan invariant narrowed to non-displacement (all-low pages may be review_only) per bake-plan §4.4"
  - "D-17 coverage HALT (0.5929) root-caused to the upstream emitter's [500,1600] window, NOT a merge bug; owner directed self-recovery from the owner xlsx"
  - "composition window widened [500,1600]→[100,1600] + antiquity clamp @100 (Codex PROCEED-WITH-CHANGES; counted basis audit: 3 residual rows)"
  - "coverage anchor gate explicitly superseded → v2.1/CERT-01; year_basis + interval-aware routing deferred → v2.1"

requirements-completed: [BAKE-01, BAKE-02, BAKE-03]

duration: ~3 days (2 fail-closed HALT/fix iterations)
completed: 2026-07-26
---

# Phase 135 Plan 07: v2 Production Bake — COMPLETE (built, verified, frozen; awaiting 135-08 deploy)

**The v2 asset is BUILT and passes every gate on the exact artifact:** build exit 0; `verify_discovery_sidecar.py --expected-frame-hash 53725098… --require-v2` = **all invariants pass, exit 0**; strict masking (`--scan-sqlite <db> --scan-asset <db> --scan-repo --strict`) = **clean, exit 0**; `docs/specs/discovery-frames-v2.md` frozen and committed (`cc114a74`). The D-17 date-coverage gate passed at **pair_coverage 1.0** (floor 0.99) with **zero** fail_safe rows. 135-08 (production deploy) and 135-09 (CERT-01 grading) are human gates — PAUSED.

## Final asset identity (gitignored; never committed)

- `asset_basename` = `discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff`
- `content_hash` = `33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff`
- `frame_content_hash` = `53725098ece6cf152a72425587dc2fe9119261427fc82e008a5b953dcbd2bce7`
- Rows: works 1,269 / discovery_claim 268,361 (100% display) / discovery_evidence 297,415 / witness_units 5,547
- Pinned inputs in meta: canonical `cc054d11…`, composition `2b46b470…` (7,443 entries), seftja `0076028917…` (410), crosswalk `bcde04bd…`, source_db `1dc28d6d…`; `band_vocab_version=v2`
- Full counts/tables: `docs/specs/discovery-frames-v2.md` (the frozen record)

## The two fail-closed HALTs this plan hit — both root-caused and fixed (never bypassed)

### HALT A — Lever-1 fed the wrong metric (155,912 shadow-orphans; tier_a shipped = 0)

The first bake's verifier failed because `apply_lever1_coverage` read `density` = `best_density`, a normalized Levenshtein **edit-distance** hard-capped at 0.35 by `track1_match.accept_density` — it can never reach the 0.45 cliff, so ~98% of track1 witnesses demoted and every witness-bearing page orphaned. **Fix (owner-approved 5-part):** compute the real SEED-029 coverage = `matched_letters / len(norm_stream(page_text))` at ingestion for all 4 track1 sources; feed Lever-1 (cliff unchanged); `routing_reason='low_coverage'`; narrow `check_never_orphan_shipped` to **non-displacement** (an all-low page may be entirely review_only — recoverable — but a claim with any shipped evidence must display a shipped row); a **SEED-029 replication gate** over the 200 graded units reproduced row-level cov 200/200 (worst diff 0.0005) and the precision bands **94.0 / 91.7 / 37.5% exactly** — proving the ported normalizer byte-faithful. Result on the real corpus: 144,294/254,612 track1 witnesses ship (56.7%), matching the independent recompute.

### HALT B — D-17 date-coverage 0.5929 (3,753/6,330): 173 shipped works undated

The corrected universe exposed 173 undated shipped works (167 M-source). Diagnosed (2 agents + Codex + programmatic lineage audit): **the upstream date-emitter's own [500,1600] window silently dropped the classical strata** — not the Sefaria dedup, not a crosswalk/merge bug. Owner chose full fix and directed self-recovery from the owner xlsx: replaying the emitter's own extraction (window removed) recovered **127 true classical years [200,499]** + **39 pre-100 works clamped at the widened floor 100** (antiquity clamp — a ROUTING FLOOR, order-preserving for D-17); 1 post-1600 + 6 non-M left undated (verified degree-0). Composition window widened to [100,1600] (`5e0b729e`), table 7,277→7,443 (new SHA pinned), plus `assert_composition_release_contract` so a regressed re-emit can never pass a future repin (`6fd9435d`). Result: **pair_coverage 1.0000** (4,208 kept_tie + 2,062 demoted + 0 fail_safe) — reproduced exactly in the final bake.

Also fixed en route (Codex-flagged): `resolve_year_by_canonical` rewritten to the §4.3 contract (`9ad14ff0`); the D-17 router rewritten ordered-stateful so an already-demoted work can never demote a third work (`04d08a3f`, Codex-confirmed); verifier fail-closed cascade gate (`4a52641c`); SEF/JA window decoupled [100,1600] (`2e60ad88`).

## Codex gates (3 rounds this plan)

1. **Lever-1 metric fix** — confirmed the diagnosis and the 5-part fix; demanded the pre-registered replication gate (implemented, reproduced exactly).
2. **D-17 router rewrite** — PROCEED (cascade closed; counterexample now a test).
3. **Window widen + recovery** — PROCEED-WITH-CHANGES ("the clamp itself is sound"); resolved by: counted basis-exposure audit (40/2,062 exposed → 23/23 range rows survive the strict interval rule with true range starts; 14/17 before-N rows have antiquity-clamp demoters; **3 residual rows** disclosed in the frame doc), exact U-reconciliation (6,508 date-independent pairs vs 6,270 audit rows; **2** invalid-ref no-row pairs — the ratified Option-A deferral, not 60), the release-contract gate + clamp boundary tests (implemented), and explicit deferrals (year_basis + interval routing + coverage anchor → v2.1/CERT-01) recorded in the bake-plan addendum.

## Deviations from plan

- The plan's frozen expectation of 407 seftja entries / 7,277 composition entries was owner-superseded mid-plan (410 / 7,443 after the two owner-ratified date recoveries); the real-artifact smoke tests were synced (`bcc111e1`, and in `5e0b729e`).
- The `chrono_coverage_prebuild` anchor gate (bake-plan §7 #9/#11) was never implemented in the build CLI and is EXPLICITLY SUPERSEDED for this bake (documented in the addendum) — gating stands on the absolute 0.99 floor + the mutation-tested verifier.

## Follow-ups handed to 135-08 / 135-09 / GEN2

- **135-08 (human):** deploy per `discovery-deploy.md` §2 (scp asset-first immutable name → candidate manifest → on-box verify → atomic swap). `DISCOVERY_ENABLED` stays OFF until the owner flips it.
- **135-09 (human):** CERT-01 card-draw/grading over the now-non-empty shipped tier_a population (230,267 display claims), incl. the full-corpus coverage-framework re-validation Codex assigned there.
- **GEN2 sync:** widen the emitter's [500,1600] window + adopt the antiquity clamp (else its next re-emit regresses the table — the new release gate would HALT it); sync seftja_dates.json (410) and note the new composition SHA `2b46b470…`.
- Deferred to v2.1: interval-aware D-17 routing, per-side `year_basis` audit columns, `kept_invalid_reference` provenance.

## Self-check: PASSED

- Asset present + bytes re-hash to `content_hash`; manifest `asset_basename` resolves it exactly (no glob).
- Verifier `--require-v2` exit 0; strict masking exit 0 (real SCAN_EXIT read from the log, not the wrapper echo).
- `discovery_routing_audit` decisions in the built DB = the pre-bake simulation exactly (4,208/2,062/0).
- Frame doc committed (`cc114a74`) and single-file masking-scanned clean; built `.db` + `manifest.json` gitignored, never staged.
