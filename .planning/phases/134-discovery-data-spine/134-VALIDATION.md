---
phase: 134
slug: discovery-data-spine
status: final
nyquist_compliant: true
wave_0_complete: true
created: 2026-07-21
updated: 2026-07-21
---

# Phase 134 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `134-RESEARCH.md` → "Validation Architecture". The planner wires the
> Per-Task Verification Map with concrete task IDs; the phase-level infra/sampling/Wave-0
> rows below are authoritative. Updated 2026-07-21 for the Codex pre-flight rework
> (D1–D7 / DC1–DC14): valid strict gate command + new invariant rows.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, `tests/`); golden-fixture precedent in `tests/atlas_bake/` |
| **Config file** | none discovery-specific — Wave 0 adds fixture + verifier + test files |
| **Quick run command** | `pytest tests/test_discovery_*.py tests/test_masking_sqlite.py tests/test_no_back_edges_discovery.py -x` |
| **Full suite command** | `pytest tests/` (Windows: marker-based GUI split per `feedback_full_suite_testing_windows`; discovery tests are non-GUI) |
| **Estimated runtime** | ~30–60 s (discovery subset); minutes for full suite |

**Masking gate is a first-class validation surface** (not just pytest). D4: `--scan-sqlite`
is now a first-class `--strict` surface, so the VALID ship-gate invocation is:
`MASKING_SCAN_PATTERNS_FILE=<gitignored patterns> python scripts/check_atlas_masking.py --scan-sqlite <discovery.db> --scan-asset <discovery.db> --scan-repo --strict` must exit 0
(the old `--scan-sqlite <db> --scan-repo --strict` form was rejected by the CLI arg rules).

**Standalone verifier is a first-class release gate** (DC7):
`python scripts/verify_discovery_sidecar.py <discovery.db>` must exit 0 — the SAME
path-parameterized verifier CI runs over the fixture is run over the real DB in 134-07.

---

## Sampling Rate

- **After every task commit:** `pytest tests/test_discovery_*.py -x` + masking `--scan-sqlite` on any freshly built fixture DB.
- **After every plan wave:** full discovery test set + `check_atlas_masking.py --scan-repo` (committed-content leak check).
- **Before `/gsd:verify-work`:** the VALID strict masking gate (`--scan-sqlite <db> --scan-asset <db> --scan-repo --strict`) over the real built `.db` + `scripts/verify_discovery_sidecar.py <db>` exit 0; loader fail-open matrix green; overload + slot-recycling tests green.
- **Max feedback latency:** < 60 s for the discovery subset.

---

## Per-Task Verification Map

> Task ID / Plan / Wave filled from the FINAL 8-plan / 5-wave breakdown (incl. the 134-01 OQ1
> checkpoint and the 134-04 works/claims/units split). Requirement → observable-signal → command rows
> are pre-derived from research + the Codex-rework fixes. Status is execution-time (⬜ until the owning plan runs).

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| Task 2 (test) / Task 1 (mode) | 134-02 | 1 | DATA-05 | T-134-leak | No M-source/R-source string in sidecar/repo/surface (schema + every cell, str + BLOB) | integration (CI gate) | `check_atlas_masking.py --scan-sqlite <db> --scan-asset <db> --scan-repo --strict` (exit 0; real DB run in 134-07 T3) | ❌ (134-02) | ⬜ pending |
| Task 2 | 134-02 | 1 | DATA-05 | T-134-leak | BLOB/bytes cells scanned, not just str (DC8) | unit | `pytest tests/test_masking_sqlite.py -k blob` | ❌ (134-02) | ⬜ pending |
| Task 2 | 134-02 | 1 | DATA-05 | T-134-leak | Valid `--strict` combination accepted (D4) | unit | `pytest tests/test_masking_sqlite.py -k strict` | ❌ (134-02) | ⬜ pending |
| Task 1 | 134-03 | 2 | DATA-03 | T-134-leak | No EXACT-name `text`/`cat`/`provenance`/raw-`work_id`/`title`/`author`/`genre` reference columns (text_layer allowed); only offsets + snapshot hash | unit | `pytest tests/test_discovery_schema.py::test_no_reference_columns` | ❌ (134-03) | ⬜ pending |
| Task 1 (verifier) | 134-03 | 2 | DATA-01/02/03/08/10 | T-134-tamper | Path-parameterized verifier runs ALL invariants over any DB path (DC7) | integration | `python scripts/verify_discovery_sidecar.py <fixture-db>` exit 0 | ❌ (134-03) | ⬜ pending |
| Task 4 | 134-01 | 1 | DATA-01/02 | — | Deterministic `claim_id`/`unit_id` stable across rebuilds | unit (golden) | `pytest tests/test_discovery_ids.py::test_claim_id_golden` | ❌ (134-01) | ⬜ pending |
| Task 4 | 134-01 | 1 | DATA-03 | T-134-leak | `validate_source_corpus_code` raises on non-codes; NO raw literal committed (DC1) | unit | `pytest tests/test_discovery_ids.py::test_validate_source_corpus_code` | ❌ (134-01) | ⬜ pending |
| Task 4 | 134-01 | 1 | DATA-01 | — | `claim_type_for_flank` TOTAL incl edge + ambig -> one code or EXCLUDE (DC9) | unit | `pytest tests/test_discovery_ids.py::test_claim_type_routing_total` | ❌ (134-01) | ⬜ pending |
| Task 1 | 134-04 | 3 | DATA-01 | — | Opaque work_id stable across two builds (crosswalk-anchored; absent crosswalk aborts) (DC2) | unit | `pytest tests/test_discovery_build.py -k work_id_stable` | ❌ (134-04) | ⬜ pending |
| Task 1 | 134-04 | 3 | DATA-04 | T-134-leak | Only owner-approved neutral columns reach works rows; `--from-approved` round-trip (D6/DC13) | unit | `pytest tests/test_discovery_build.py -k approved` | ❌ (134-04) | ⬜ pending |
| Task 2 | 134-04 | 3 | DATA-01 | — | MS-MS claim_type <- flank_class; work-witness <- track1 semantics; `shadowed_by IS NULL` on track1_matches ONLY (D2/D3) | unit | `pytest tests/test_discovery_build.py -k claims` | ❌ (134-04) | ⬜ pending |
| Task 2 | 134-03 | 2 | DATA-02 | — | Exactly one band per claim key post-precedence; within-key only (multi-work-per-MS preserved) | unit | `pytest tests/test_discovery_bands.py::test_one_band_per_key` | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-02 | — | Fixture logical `frame_content_hash` == pinned golden (recipe excludes volatile meta) (DC3/DC10) | integration | `pytest tests/test_discovery_frame.py::test_frame_hash_golden` | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-08 | T-134-tamper | `PRAGMA foreign_key_check` clean + child-row composite UNIQUE enforced (DC4) | unit | `pytest tests/test_discovery_release_contract.py` | ❌ (134-03) | ⬜ pending |
| Task 2 | 134-06 | 4 | DATA-06 | T-134-dos | Overload → `DiscoveryUnavailable` within timeout; loop stays responsive (never hangs) | async unit | `pytest tests/test_discovery_service.py::test_overload_returns_unavailable` | ❌ (134-06) | ⬜ pending |
| Task 2 | 134-06 | 4 | DATA-06 | T-134-dos | Timed-out heavy slot NOT recycled until the thread finishes (add_done_callback release, not finally) (DC6) | async unit | `pytest tests/test_discovery_service.py::test_timed_out_slot_not_recycled_until_thread_finishes` | ❌ (134-06) | ⬜ pending |
| Task 1 | 134-06 | 4 | DATA-06 | T-134-layer | `shared/discovery_service.py` has no runtime `web.*` import (D5) | unit (AST guard) | `pytest tests/test_no_back_edges_discovery.py` | ❌ (134-06) | ⬜ pending |
| Task 1 | 134-06 | 4 | DATA-06 | — | Every list query bounded (`LIMIT`) + server-side pagination; injected (path/availability/token) | unit | `pytest tests/test_discovery_service.py::test_pagination_bounds` | ❌ (134-06) | ⬜ pending |
| Task 3 | 134-05 | 3 | DATA-07 | — | Flag OFF → `discovery_available()` False; all reads no-op | unit | `pytest tests/test_discovery_flag.py::test_flag_off_hides` | ❌ (134-05) | ⬜ pending |
| Task 2 | 134-05 | 3 | DATA-07/08 | T-134-tamper | Sidecar absent / corrupt / incompatible / manifest-hash-mismatch / missing-meta-key / missing-table / invalid-vocab → `ready=False`; app stays up (DC5/DC11) | unit | `pytest tests/test_discovery_loader.py` | ❌ (134-05) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-08 | — | `PRAGMA integrity_check == ok`; release-contract row counts match actuals; source-DB + crosswalk hash recorded | integration | `pytest tests/test_discovery_release_contract.py` | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-10 | — | Units merge Oxford parts + physical joins but NOT `Scribe join`; `unit_id` deterministic; ≤1 unit/sys_id | unit | `pytest tests/test_discovery_units.py::{test_scribe_not_merged,test_unit_id_deterministic}` | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-07 | 4 | DATA-02/04/08 | T-134-leak/tamper | Real DB passes the shared verifier + the VALID strict gate; `meta.frame_content_hash` == discovery-frames.md (DC3/DC7/DC8/DC13) | integration (release gate) | `verify_discovery_sidecar.py <db>` + `check_atlas_masking.py --scan-sqlite <db> --scan-asset <db> --scan-repo --strict` exit 0 | ❌ (134-07) | ⬜ pending |
| Task 1 | 134-08 | 5 | PERF-01 | T-134-dos | Measurable query latencies within caps; discovery adds ≤250 MB RSS (measured); later-surface caps PENDING (DC14) | measurement | benchmark script (p95 timings) + RSS probe — recorded, not a hard unit gate | ❌ (134-08) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

> Witness-unit merging (DATA-10) is authored in `134-04` Task 3 (`units.py`) and PROVEN against the
> fixture in `134-03` Task 3 (`test_discovery_units.py`, via `scripts/verify_discovery_sidecar.py`) — the row above points at the proving test.

---

## Wave 0 Requirements

> Wave 0 is DISTRIBUTED, not a single pre-wave: the test infra is created inside each owning plan's
> own wave, dependency-ordered — ids + masking (Wave 1) → fixture + verifier + invariants (Wave 2) →
> loader/flag (Wave 3) → service (Wave 4) → bench (Wave 5). Each item below is allocated to a plan.

- [x] Extend `scripts/check_atlas_masking.py` with a `--scan-sqlite` mode (str + BLOB; first-class `--strict` surface) + `tests/test_masking_sqlite.py`. → **134-02** (Wave 1)
- [x] `scripts/verify_discovery_sidecar.py` (path-parameterized all-invariant verifier) + `tests/test_discovery_schema.py`, `test_discovery_bands.py`, `test_discovery_frame.py`, `test_discovery_units.py`, `test_discovery_release_contract.py` — build-output invariants over a small deterministic fixture DB → **134-03** (Wave 2); `test_discovery_ids.py` (golden id recipe + validate_source_corpus_code + total routing) → **134-01** (Wave 1).
- [x] `tests/test_discovery_service.py` + `tests/test_no_back_edges_discovery.py` (layering guard) → **134-06** (Wave 4); `test_discovery_flag.py`, `test_discovery_loader.py` (manifest + full fail-closed matrix) → **134-05** (Wave 3); `test_discovery_release_contract.py` → **134-03** (Wave 2). (Loader tests model on the existing `atlas_assets` tests.)
- [x] A benchmark/RSS script feeding `discovery-budgets.md` (measurable PERF-01 numbers measured, not asserted; later-surface caps PENDING) → **134-08** (Wave 5); the budgets doc itself created in **134-02** (Wave 1).
- [x] A tiny committed fixture `discovery.db` (+ manifest.json) builder (deterministic, masking-safe synthetic data, frozen constant timestamps) so CI never needs the 3.1 GB research DB → **134-03** (Wave 2).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Per-family E1 band-source file + join key + raw->band translation + TOTAL flank->claim_type routing (OQ1/D1/DC9) confirmation | DATA-02 | Domain fact, not a computable invariant; CERT-01 (Phase 135) freezes against this frame (RESEARCH §OQ1 / A5) | Owner/researcher confirms, PER CLAIM FAMILY (MS-MS AND work-witness), the E1 file + join key + band translation + a total routing at the 134-01 Task 2 blocking checkpoint before the schema sections are frozen |
| Neutral-title owner review (approve/hand-pick M-source literary subset) | DATA-04 | Human curation gate — fail-closed; owner is the authority (D-06/D-08). Guarantee = only approved neutral columns ship (not a token scan of the artifact) | Owner edits the generated review artifact; only approved rows re-distill; unreviewed = excluded |
| PERF-01 latency/RSS budgets on the real prod-scale `.db` | PERF-01 | PERF-01 mandates measurement (not assertion); prod box RSS not reproducible in CI; later-surface caps unmeasurable this phase | Run the benchmark/RSS script over the built `discovery.db`; record measurable actuals in `discovery-budgets.md`; leave later-surface caps PENDING |
| Deploy: temp-upload → verify → ATOMIC manifest swap → code | DATA-08 | Requires live server access + asset-first deploy posture | Follow the documented rollback (repoint manifest) + rebuild recipe on the web box |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (checkpoint/human-only tasks carry `<human-check>`)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (distributed across waves, dependency-ordered)
- [x] No watch-mode flags
- [x] Feedback latency < 60s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** 2026-07-21 (updated for Codex pre-flight rework D1–D7 / DC1–DC14)
