---
phase: 135-precision-certificate-confidence-bands
plan: 01
subsystem: discovery
tags: [sqlite, sidecar, band-labels, precision-copy, ci-fail-closed, sc1-serializer, d18-eligibility, bilingual]

# Dependency graph
requires:
  - phase: 134-discovery-data-spine
    provides: "discovery.db sidecar schema (band_precision/discovery_claim/discovery_evidence), DiscoveryService async chokepoint, scripts/discovery_ids.py frozen enum vocab"
provides:
  - "shared/discovery_band_labels.py: bilingual band label table + data-driven precision copy + band_measurement_status + the SC#1 serialize_banded_claim + the CI-fail-closed D-18 is_default_eligible predicate + STRICT_FLOOR/SHOW_MORE_TOGGLE/RECALL_DISCLAIMER constants"
  - "shared/discovery_service.py: get_band_precision(_async), get_band_precision_collection(_async), get_band_claim_counts(_async) -- fail-closed band_precision + display-deduplicated shipped-claim population readers"
  - "tests/test_discovery_band_labels.py: 57-test invariant suite (drift guard, word gate incl. rendered output, CI-omission/fail-closed-partial, data-driven, display-dedup population, SC#1, D-18 predicate table, CI-fail-closed downgrade)"
affects: [136-read-surfaces-panel-work-witnesses, 135-02-methods-page, cert-01-measurement-track]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Hand-typed, drift-guarded values module over a frozen enum (mirrors scripts/discovery_ids.py's own TOTAL-coverage discipline)"
    - "v1/v2 dual-key normalization (_canon_band_key) so one label table serves both the pre- and post-bake stored band key"
    - "CI-fail-closed status derivation: a stored measured_pass is downgraded to measured_fail whenever its own ci_low is missing or sub-floor, so no code path can display a contradictory pass"
    - "Band-inseparable claim serialization (SC#1): a structural raise, not a lint rule, prevents a claim presentation from ever omitting its band"

key-files:
  created:
    - shared/discovery_band_labels.py
    - tests/test_discovery_band_labels.py
  modified:
    - shared/discovery_service.py

key-decisions:
  - "STRICT_FLOOR=0.85 (D-07) lives as a module constant in discovery_band_labels.py, not duplicated in discovery_service.py"
  - "get_band_claim_counts uses the frozen SQL join (discovery_claim -> its display_evidence_id) filtered to routing_status='shipped' on the DISPLAY row -- proven load-bearing by inserting a second (non-display) evidence row on an existing claim and confirming the count does not inflate, then flipping the display row to review_only and confirming the claim drops out entirely"
  - "serialize_banded_claim defaults an absent routing_status to review_only (conservative -- an unknown routing status must never default-show)"
  - "Skipped requirements.mark-complete for BAND-01/02/03/04/CERT-02 (shared frontmatter IDs on this plan) -- this plan establishes and invariant-tests the values module + serializer + predicate, but BAND-03/04 describe live UI toggle/disclaimer surfaces (135-02+/Phase 136), BAND-05's methods page is a separate plan, and CERT-02's paired-status display requires the CERT-01 measurement track to actually run. Marking Complete here would be premature; mirrors the 134-01/02/03 precedent of deferring mark-complete until the consuming surface lands."

patterns-established:
  - "Whole-word (regex \\b) forbidden-word matching for the Rule-1 word gate -- a naive substring check false-positives on 'unreviewed' containing 'reviewed', which is the HONEST negation of it, not a violation"

requirements-completed: []  # See key-decisions -- BAND-01/02/03/04/CERT-02 intentionally deferred, not yet satisfied end-to-end

# Metrics
duration: 55min
completed: 2026-07-24
---

# Phase 135 Plan 01: Band Values Module + SC#1 Serializer + D-18 Eligibility Predicate Summary

**Hand-authored bilingual band-label + precision-copy values module with a CI-fail-closed default-eligibility predicate and a band-inseparable claim serializer, backed by a new fail-closed `band_precision`/display-deduplicated-population reader on `DiscoveryService`.**

## Performance

- **Duration:** ~55 min
- **Completed:** 2026-07-24
- **Tasks:** 3/3 completed
- **Files modified:** 3 (1 modified, 2 created)

## Accomplishments

- `shared/discovery_band_labels.py` is now the ONE shared band-presentation module: TOTAL bilingual labels over the frozen `CONFIDENCE_BANDS_BY_SOURCE` enum (with v1/v2 dual-key normalization), data-driven precision copy that fails closed on a partial confidence interval, a CI-fail-closed `band_measurement_status`, the SC#1 `serialize_banded_claim` band-inseparable serializer, and the central D-18 `is_default_eligible` predicate.
- `DiscoveryService` gained its first `band_precision` reader (`get_band_precision` / `_collection` / async variants) and the version-aware, SHIPPED, display-deduplicated `get_band_claim_counts()` population source (Codex #B1/#9) -- the population is proven, by test, to differ from a naive raw-evidence-row count.
- 57 new tests all green, including a manually-verified load-bearing drift guard (temporarily injected "Verified" into a label string, confirmed both word-gate tests failed, then reverted) and a CI-fail-closed proof that a contradictory stored `measured_pass` (missing/sub-0.85 `ci_low`) is downgraded to `measured_fail` and never shows as default-eligible.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add the fail-closed band_precision + display-deduplicated per-band claim-count readers to DiscoveryService** - `2a08769b` (feat)
2. **Task 2: Values module + SC#1 banded-claim serializer + CI-fail-closed D-18 default-eligibility predicate** - `52fce559` (feat)
3. **Task 3: Drift-guard + data-driven + word-gate + SC#1-inseparability + CI-fail-closed D-18 + display-deduplicated-population test suite** - `065ee4c0` (test)

_Note: tasks were implemented and validated together (Task 3's tests exercise Tasks 1+2's code), then committed in plan task order._

## Files Created/Modified

- `shared/discovery_band_labels.py` (NEW) - Bilingual band label table (`BAND_LABELS`), `_canon_band_key` (v1/v2 dual-key normalization), `band_label`, `review_overlay`, `format_precision_copy`, `band_measurement_status`, `is_default_eligible` (the D-18 predicate), `serialize_banded_claim` (the SC#1 serializer), `STRICT_FLOOR`, `MEASUREMENT_STATUSES`, `SHOW_MORE_TOGGLE` (D-11), `RECALL_DISCLAIMER` (D-12), `NUMERATOR_LABEL`/`DENOMINATOR_LABEL`/`DRAW_SIZE_LABEL` (distinct-from-population sample-size labels for future callers).
- `shared/discovery_service.py` (MODIFIED) - Added `get_band_precision`/`_async`, `get_band_precision_collection`/`_async`, `get_band_claim_counts`/`_async`, mirroring `get_version`'s fail-closed idiom exactly; no new caching.
- `tests/test_discovery_band_labels.py` (NEW) - 57 tests covering totality/no-orphan drift guard, static + rendered-output word gate, CI-omission/fail-closed-partial-interval, data-driven precision copy and measurement status (fixture-DB mutation), display-deduplicated population (Codex #B1/#9), SC#1 band-inseparability, the D-18 eligibility table, and the Codex #B3 CI-fail-closed downgrade.

## Decisions Made

- `STRICT_FLOOR = 0.85` lives in `shared/discovery_band_labels.py` (not duplicated in `discovery_service.py`) -- the service layer never needs to know the CERT-01 pass threshold, only the values/predicate module does.
- `get_band_claim_counts`'s SQL joins `discovery_claim` to its `display_evidence_id` and filters `routing_status='shipped'` on that DISPLAY row specifically (not any sibling evidence row) -- verified via a test that inserts a second, non-display evidence row on an existing claim (count unchanged) and then flips the DISPLAY row to `review_only` (claim drops out of the population entirely, even though its non-display sibling is still `shipped`).
- `serialize_banded_claim` defaults a missing `routing_status` to `review_only` (the conservative choice -- an unknown routing status must never be treated as default-shown).
- Word-gate tests use whole-word (`\b`) regex matching rather than a naive substring check, because "unreviewed" legitimately contains "reviewed" as a substring while being its honest negation (the explicit "absence of review" marker required by `docs/specs/discovery-band-labels-v1.md` §2) -- a naive check would have false-positived on the module's own correct output.
- Skipped `requirements mark-complete` for BAND-01/02/03/04/CERT-02 (the plan's shared frontmatter `requirements` field) -- see key-decisions above and REQUIREMENTS.md rationale. These requirements describe end-to-end UI/measurement behavior (default-shown toggles, disclaimers, the methods page, the CERT-01 outcome) that this plan's values-module/service-layer foundation enables but does not itself display; Phase 136 is the documented consumer (CONTEXT.md: "Phase 135 itself renders no claim lists, only the methods page").

## Deviations from Plan

None — plan executed exactly as written. One test-design correction was made and validated in-flight (not a deviation from the PLAN's behavior, but from my own first draft of the test): the initial `test_word_gate_rendered_output` used naive substring matching and false-positived on `review_overlay`'s own correct "unreviewed · algorithmic estimate" output (since "unreviewed" contains "reviewed"). Fixed by switching to whole-word `\b`-boundary regex matching before the final commit; re-verified the drift guard is still load-bearing afterward (temporary "Verified" injection -> confirmed failure -> reverted -> confirmed pass).

## Known Stubs

None. This plan ships pure values-module + service-reader logic with no UI rendering and no placeholder/mock data paths.

## Threat Flags

None — this plan's threat model (band label strings, precision copy, drift, fail-closed reads, SC#1 inseparability + CI-fail-closed default-eligibility) is fully captured by the plan's own `<threat_model>` STRIDE register; no new surface (network endpoint, auth path, file access, schema change) was introduced beyond what that register already dispositions.

## Self-Check: PASSED

- FOUND: `shared/discovery_band_labels.py`
- FOUND: `shared/discovery_service.py`
- FOUND: `tests/test_discovery_band_labels.py`
- FOUND: `.planning/phases/135-precision-certificate-confidence-bands/135-01-SUMMARY.md`
- FOUND commit: `2a08769b` (Task 1)
- FOUND commit: `52fce559` (Task 2)
- FOUND commit: `065ee4c0` (Task 3)
- Verification suite: `pytest tests/test_discovery_band_labels.py tests/test_discovery_ids.py -q` → 57 passed
- `python -m ruff check shared/discovery_band_labels.py shared/discovery_service.py tests/test_discovery_band_labels.py` → clean
- `pytest tests/test_no_back_edges_core.py tests/test_no_back_edges_desktop.py -q` → 62 passed (no new shared/→web/ or shared/→genizah_core back-edge)
