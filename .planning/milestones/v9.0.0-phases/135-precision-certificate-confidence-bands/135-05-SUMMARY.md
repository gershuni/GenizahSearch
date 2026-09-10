---
phase: 135-precision-certificate-confidence-bands
plan: 05
subsystem: discovery
tags: [frozen-enum, ddl, routing-reason, band-rename, v1-read-compat, measurement-registry, spec-amendment, masking-safe]

# Dependency graph
requires:
  - phase: 135-01
    provides: "shared/discovery_band_labels.py (MEASUREMENT_STATUSES closed vocab, _canon_band_key v1->v2 dual-key), get_band_precision/_collection/get_band_claim_counts readers"
  - phase: 135-04
    provides: "docs/specs/discovery-v2-bake-plan.md (Codex-gated v2 bake contract this vocabulary/DDL lockstep serves)"
provides:
  - "scripts/discovery_ids.py: ROUTING_REASON_LATER_SHARED_TEXT (5-member ROUTING_REASONS) + CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC (added to CONFIDENCE_BANDS_BY_SOURCE['track1_direct'] alongside the retained v1 expert_verified)"
  - "scripts/build_discovery_sidecar.py DDL: discovery_evidence.routing_reason CHECK += later_shared_text; band_precision + 5 CERT-01 registry columns (closed-vocab measurement_status CHECK); new masking-safe discovery_routing_audit table (decision CHECK); _LATER_SHARED_TEXT + _HIGH_CONFIDENCE_ALGORITHMIC forward-ref constants"
  - "web/discovery_assets.py + shared/discovery_service.py: dual-key band spot-check + band-rank (both v1 + v2 keys, v1-read-compat)"
  - "dated spec amendments: discovery-sidecar-schema-v1.md (v2 vocab + registry columns + routing_audit + meta provenance keys), discovery-band-labels-v1.md §4 (D-18) + §5 (asset/bake-level atomicity), discovery-frames.md (rename note)"
affects: [135-06-v2-build-logic, 135-07-v2-bake-freeze, 135-08-v2-deploy, 135-09-cert01-grading]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "v1-read-compat dual-key window: the frozen enum, verifier VALID_EVIDENCE_COMBOS (auto-derived), web spot-check, and service band-rank all accept BOTH expert_verified (v1) AND high_confidence_algorithmic (v2) until the v2 manifest is live; the built v2 asset carries only the v2 key (verifier no-mixed-enum-state check, 135-06/07)"
    - "closed-vocab measurement_status DDL CHECK mirrors shared/discovery_band_labels.MEASUREMENT_STATUSES exactly, so a free-text status can never reach the D-18 default-eligibility predicate (Codex #B3)"
    - "masking-safe audit table by construction: discovery_routing_audit carries opaque work ids + integer years + closed decision/reason enums ONLY — no title, reference text, or raw id"
    - "dated-amendment-only spec edits: the frozen enum block + §1.5/§1.6 left untouched in place; every change is a new dated section (T-135-05-02)"

key-files:
  created: []
  modified:
    - scripts/discovery_ids.py
    - scripts/build_discovery_sidecar.py
    - scripts/verify_discovery_sidecar.py
    - web/discovery_assets.py
    - shared/discovery_service.py
    - docs/specs/discovery-sidecar-schema-v1.md
    - docs/specs/discovery-band-labels-v1.md
    - docs/specs/discovery-frames.md
    - tests/test_discovery_ids.py

key-decisions:
  - "CONFIDENCE_BANDS_BY_SOURCE['track1_direct'] ADDS high_confidence_algorithmic while RETAINING expert_verified (not a replace) — VALID_EVIDENCE_COMBOS auto-derives from this set, so a replace would reject the live v1 asset + the byte-identical synthetic golden fixture. This is the v1-read-compat reading of 'update to the v2 key' forced by the byte-identical-fixture constraint (Codex #8)"
  - "The real-mode build band assignment + _frozen_real_band_precision_rows + the verifier's release-strict expected keys KEEP writing/keying expert_verified — flipping them to high_confidence_algorithmic is the v2 bake (135-06), gated by 'NO bake logic yet' and by the test_discovery_build.py tests that assert expert_verified==0.889. 135-05 established the v2 key as a valid vocab member + the DDL, not the switch that WRITES it"
  - "The verifier has NO standalone routing_reason enum check to extend — routing_reason is enforced solely by the discovery_evidence DDL CHECK (mirroring ids.ROUTING_REASONS); the verifier edit is a lockstep-documenting comment on the VALID_EVIDENCE_COMBOS auto-derivation, no behavior change"
  - "discovery_routing_audit.routing_reason is a plain (unconstrained) TEXT annotation column per the plan's explicit column spec; the constrained routing_reason enum lives on discovery_evidence"
  - "Skipped requirements.mark-complete for BAND-01/BAND-02/CERT-01 — this plan lands the vocabulary/DDL/spec lockstep the display + measurement surfaces write against, but BAND-01/02 ship in 135-02/Phase 136 and the CERT-01 measurement runs in 135-09"

requirements-completed: []  # BAND-01/BAND-02/CERT-01 intentionally deferred — see key-decisions

# Metrics
duration: 45min
completed: 2026-07-24
---

# Phase 135 Plan 05: v2 Vocabulary + Schema Lockstep Summary

**Landed the discovery-v2 vocabulary + schema + spec lockstep as ONE atomic unit — `routing_reason` gains `later_shared_text`, the stored band adds `high_confidence_algorithmic` (v1 `expert_verified` retained for read-compat), `band_precision` gains the five CERT-01 registry columns with a closed-vocab `measurement_status` CHECK, and a masking-safe `discovery_routing_audit` table appears — with NO bake logic (population is 135-06).**

## Performance

- **Duration:** ~45 min
- **Completed:** 2026-07-24
- **Tasks:** 3/3 completed (Task 1 TDD: RED → GREEN)
- **Files modified:** 9

## Accomplishments

- **Task 1 (TDD):** `scripts/discovery_ids.py` gained `ROUTING_REASON_LATER_SHARED_TEXT` (5-member `ROUTING_REASONS`) and `CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC` (added to `CONFIDENCE_BANDS_BY_SOURCE['track1_direct']` alongside the retained v1 `expert_verified`). The build DDL gained: `later_shared_text` in the `discovery_evidence.routing_reason` CHECK; the five NULLABLE `band_precision` registry columns (`measurement_status`/`measurement_date`/`grader`/`audit_status`/`report_id`) with the closed-vocab `measurement_status` CHECK (Codex #B3); the new masking-safe `discovery_routing_audit` table (opaque ids + numeric years + a closed `decision` CHECK); and the `_LATER_SHARED_TEXT`/`_HIGH_CONFIDENCE_ALGORITHMIC` forward-reference constants. The verifier got a lockstep-documenting comment (its `VALID_EVIDENCE_COMBOS` auto-derives both keys; routing_reason is DDL-enforced).
- **Task 2:** `web/discovery_assets.py::_CONFIDENCE_BANDS_BY_SOURCE` and `shared/discovery_service.py::_BAND_RANK_ORDER` now accept BOTH the v2 key and the retained v1 key (v1-read-compat; the v2 key sits at the same top rank position). No `shared/`→`web/` or `shared/`→`genizah_core` back-edge introduced.
- **Task 3:** dated spec amendments — `discovery-sidecar-schema-v1.md` (a 2026-07-24 amendment adding `later_shared_text`, the `band_precision` registry columns + closed-vocab `measurement_status` CHECK, the `discovery_routing_audit` table, and the three new `meta` provenance keys `canonical_merges_sha256`/`composition_dates_sha256`/`seftja_dates_sha256`; frozen block untouched in place); `discovery-band-labels-v1.md` §4 D-18 amendment (exact wording "tier_a is NOT default-shown until its CERT-01 gate passes …") + §5 asset/bake-level-atomicity amendment (Codex #8, literal phrase "asset/bake-level", not per-git-commit); `discovery-frames.md` dated C-7 rename note (v1 counts NOT rewritten). Extended `tests/test_discovery_ids.py` with golden-pin assertions for the amended 5-member `ROUTING_REASONS` and the v2 band key.

## Task Commits

1. **Task 1 RED** — `55d51f80` (test): failing enum/DDL guards (routing_reason CHECK, band constant/membership, band_precision measurement_status CHECK, discovery_routing_audit decision CHECK).
2. **Task 1 GREEN** — `da280883` (feat): amend frozen enum + DDL for the v2 vocab lockstep.
3. **Task 2** — `22e9bf28` (feat): dual-key band rename in the web spot-check + service band-rank (v1-read-compat retained).
4. **Task 3** — `2f391131` (docs): dated spec amendments (schema + band-labels §4/§5 + frames) + golden pin.

## Files Modified

- `scripts/discovery_ids.py` — `ROUTING_REASON_LATER_SHARED_TEXT` + 5-member `ROUTING_REASONS`; `CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC` + its addition to `CONFIDENCE_BANDS_BY_SOURCE['track1_direct']` (v1 key retained).
- `scripts/build_discovery_sidecar.py` — DDL: routing_reason CHECK += later_shared_text; band_precision + 5 registry columns (closed-vocab measurement_status CHECK); new discovery_routing_audit table (decision CHECK); `_LATER_SHARED_TEXT`/`_HIGH_CONFIDENCE_ALGORITHMIC` constants.
- `scripts/verify_discovery_sidecar.py` — lockstep-documenting comment on `VALID_EVIDENCE_COMBOS` (auto-derives both keys; routing_reason DDL-enforced; release-strict expected keys deferred to 135-06). No behavior change.
- `web/discovery_assets.py` — dual-key `_CONFIDENCE_BANDS_BY_SOURCE` spot-check.
- `shared/discovery_service.py` — dual-key `_BAND_RANK_ORDER` (v2 key at top rank).
- `docs/specs/discovery-sidecar-schema-v1.md` — dated 2026-07-24 amendment (frozen block untouched in place).
- `docs/specs/discovery-band-labels-v1.md` — §4 D-18 + §5 asset/bake-level-atomicity dated amendments + header note.
- `docs/specs/discovery-frames.md` — dated C-7 rename note (v1 counts preserved).
- `tests/test_discovery_ids.py` — Task 1 TDD enum/DDL behavioral tests + Task 3 golden-pin tests.

## Deviations from Plan

- **[Rule 3 — blocking-correction] Masking verification command corrected.** The plan's Task 3 `<verify>` and `<verification>` invoke `python scripts/check_atlas_masking.py --scan-repo --strict`, but `--strict` requires a `--scan-asset PATH` and this wave produces NO built asset, so the literal command exits 2. Per the orchestrator's instruction I verified masking BY CONSTRUCTION over my specific added diff lines instead: a Python scan of all 405 added lines across the nine edited files found **0** Hebrew characters (U+0590–U+05FF) and **0** occurrences of the restricted M-source real name. A whole-file grep would false-positive on PRE-EXISTING legitimate Hebrew (the bilingual HE band labels in `discovery-band-labels-v1.md` §2 and the Hebrew genre-taxonomy constants already in `build_discovery_sidecar.py`), so the added-lines scan is the faithful "construction on my specific edits" check. The orchestrator runs the authoritative full `--scan-repo` (with `MASKING_SCAN_PATTERNS_FILE` set) separately after this plan lands.
- **[in-spirit adaptation] `CONFIDENCE_BANDS_BY_SOURCE` ADDS rather than replaces.** The plan action reads "update `CONFIDENCE_BANDS_BY_SOURCE`'s track1_direct set to the v2 key", but the verifier's `VALID_EVIDENCE_COMBOS` auto-derives from that set and the committed byte-identical synthetic golden fixture (+ the live v1 asset) still carries `expert_verified` rows, so a replace would reject them. Added the v2 key alongside the retained v1 key — the v1-read-compat reading mandated by the must_haves (Codex #8) and enforced by `tests/test_discovery_bands.py`.
- **[in-spirit adaptation] Real-build band flip deferred to 135-06.** The plan's "update the band-assignment code that references the renamed band constant" was satisfied by adding the `_HIGH_CONFIDENCE_ALGORITHMIC` forward-reference constant to the build's constant block; the synthetic dataset (byte-identical fixture) and the real-mode `build_claims_and_evidence`/`_frozen_real_band_precision_rows` keep writing `expert_verified`, and the verifier's release-strict `_EXPECTED_BAND_KEYS`/`_EXPECTED_MEASURED_BAND_PRECISIONS` keep keying on it. Flipping the WRITE is bake logic (135-06) — explicitly out of scope ("NO bake logic yet") and would break the `test_discovery_build.py` tests that assert `expert_verified == 0.889`.
- **[in-spirit adaptation] Verifier had no routing_reason enum check to extend.** `routing_reason` is enforced only by the `discovery_evidence` DDL CHECK (mirroring `ids.ROUTING_REASONS`); the verifier change is a documenting comment, no behavior change.

## Verification Results

- `pytest tests/test_discovery_ids.py tests/test_discovery_band_labels.py tests/test_discovery_bands.py -q` → **71 passed**.
- Broader discovery suite (`test_discovery_build.py test_discovery_release_contract.py test_discovery_service.py test_discovery_loader.py test_discovery_composition.py test_no_back_edges_core.py test_no_back_edges_discovery.py`) → **221 passed** (the real build+verify round-trip is unaffected by the new nullable columns / new empty table / frame-hash-invariant additions).
- `python -m ruff check scripts/discovery_ids.py scripts/build_discovery_sidecar.py scripts/verify_discovery_sidecar.py web/discovery_assets.py shared/discovery_service.py` → **All checks passed**; `ruff check tests/test_discovery_ids.py` → clean.
- AST back-edge guards (`tests/test_no_back_edges_core.py`) → green (no new `shared/`→`web/` or `shared/`→`genizah_core` import).
- Masking construction check (added diff lines): **Hebrew hits = 0, realname hits = 0** across 405 added lines.
- Task acceptance one-liners: AC1 (discovery_ids membership) OK; AC2 (build file text: `later_shared_text`/`discovery_routing_audit`/`measurement_status`/`measured_pass`) OK; Task 2 dual-key present-in-both-files OK; Task 3 doc-checks (`CERT-01 gate passes` present, `not-default-until-certified` absent, `asset/bake-level` present, meta provenance keys present) OK.

## TDD Gate Compliance

Task 1 followed RED → GREEN: `test(135-05)` commit `55d51f80` (8 failing guards) preceded the `feat(135-05)` GREEN commit `da280883`. No unexpected pass during RED (the bogus-value rejection guards legitimately pass both before and after, since a non-enum value was always rejected by the DDL CHECK). No REFACTOR commit was needed.

## Known Stubs

None. This plan ships vocabulary + DDL + spec + tests only; no UI rendering, no placeholder/mock data paths, no rows populated (population of the new columns/table is 135-06).

## Threat Flags

None — the plan's own STRIDE register (`enum/DDL/verifier drift`, `silent frozen-block edit`, `tier_a over-claim`, `renamed-key v1-read-compat / mixed-enum`) fully captures this plan's surface. The one new table (`discovery_routing_audit`) is masking-safe by construction (opaque ids + numeric years + closed enums only) and is loaded by no runtime read path this plan touches; it is not added to `web/discovery_assets._REQUIRED_TABLES` (so the live v1 asset still loads cleanly).

## Out-of-scope observation

`docs/specs/discovery-coordination.md` carries an unrelated working-tree modification delivered by the parallel GEN2/SEED-029 session (a `v2_canonical_merges.build.json` SHA-256 hand-off note). It is NOT part of this plan's `files_modified` and was left unstaged/untouched.

## Self-Check: PASSED

- FOUND: all 9 modified files + `135-05-SUMMARY.md`
- FOUND commits: `55d51f80` (Task 1 RED), `da280883` (Task 1 GREEN), `22e9bf28` (Task 2), `2f391131` (Task 3)
- Verification suite: `pytest tests/test_discovery_ids.py tests/test_discovery_band_labels.py tests/test_discovery_bands.py -q` → 71 passed; broader discovery suite → 221 passed
- `ruff check` (5 code files + test file) → clean
- Back-edge guard (`test_no_back_edges_core.py`) → green
- Masking construction check (added diff lines) → 0 Hebrew / 0 realname
