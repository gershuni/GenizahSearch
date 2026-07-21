---
phase: 134
slug: discovery-data-spine
status: final
nyquist_compliant: true
wave_0_complete: true
created: 2026-07-21
---

# Phase 134 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `134-RESEARCH.md` → "Validation Architecture". The planner wires the
> Per-Task Verification Map with concrete task IDs; the phase-level infra/sampling/Wave-0
> rows below are authoritative.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, `tests/`); golden-fixture precedent in `tests/atlas_bake/` |
| **Config file** | none discovery-specific — Wave 0 adds fixture + test files |
| **Quick run command** | `pytest tests/test_discovery_*.py tests/test_masking_sqlite.py -x` |
| **Full suite command** | `pytest tests/` (Windows: marker-based GUI split per `feedback_full_suite_testing_windows`; discovery tests are non-GUI) |
| **Estimated runtime** | ~30–60 s (discovery subset); minutes for full suite |

**Masking gate is a first-class validation surface** (not just pytest):
`MASKING_SCAN_PATTERNS_FILE=<gitignored patterns> python scripts/check_atlas_masking.py --scan-sqlite <discovery.db> --scan-repo --strict` must exit 0.

---

## Sampling Rate

- **After every task commit:** `pytest tests/test_discovery_*.py -x` + masking `--scan-sqlite` on any freshly built fixture DB.
- **After every plan wave:** full discovery test set + `check_atlas_masking.py --scan-repo` (committed-content leak check).
- **Before `/gsd:verify-work`:** full masking gate over the real built `.db` + `--scan-repo` (exit 0); loader fail-open tests green; overload test green.
- **Max feedback latency:** < 60 s for the discovery subset.

---

## Per-Task Verification Map

> Task ID / Plan / Wave filled from the FINAL 8-plan / 5-wave breakdown (incl. the 134-01 OQ1
> checkpoint and the 134-04 claims/units split). Requirement → observable-signal → command rows
> are pre-derived from research. Status is execution-time (⬜ until the owning plan runs).

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| Task 2 (test) / Task 1 (mode) | 134-02 | 1 | DATA-05 | T-134-leak | No M-source/R-source string in sidecar/repo/surface (schema + every cell) | integration (CI gate) | `check_atlas_masking.py --scan-sqlite <db> --scan-repo --strict` (exit 0; real DB run in 134-07 T3) | ❌ (134-02) | ⬜ pending |
| Task 1 | 134-03 | 2 | DATA-03 | T-134-leak | No EXACT-name `text`/`cat`/`provenance`/raw-`work_id`/`title`/`author`/`genre` reference columns (text_layer allowed); only offsets + snapshot hash | unit | `pytest tests/test_discovery_schema.py::test_no_reference_columns` | ❌ (134-03) | ⬜ pending |
| Task 4 | 134-01 | 1 | DATA-01/02 | — | Deterministic `claim_id`/`unit_id` stable across rebuilds | unit (golden) | `pytest tests/test_discovery_ids.py::test_claim_id_golden` | ❌ (134-01) | ⬜ pending |
| Task 2 | 134-03 | 2 | DATA-02 | — | Exactly one band per claim key post-precedence; precedence within-key only (multi-work-per-MS preserved) | unit | `pytest tests/test_discovery_bands.py::test_one_band_per_key` | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-02 | — | Frozen-frame reproducibility: rebuild → identical frame content hash | integration | `pytest tests/test_discovery_frame.py::test_frame_hash_reproducible` | ❌ (134-03) | ⬜ pending |
| Task 2 | 134-06 | 4 | DATA-06 | T-134-dos | Overload → `DiscoveryUnavailable` within timeout; loop stays responsive (never hangs) | async unit | `pytest tests/test_discovery_service.py::test_overload_returns_unavailable` | ❌ (134-06) | ⬜ pending |
| Task 1 | 134-06 | 4 | DATA-06 | — | Every list query bounded (`LIMIT`) + server-side pagination | unit | `pytest tests/test_discovery_service.py::test_pagination_bounds` | ❌ (134-06) | ⬜ pending |
| Task 3 | 134-05 | 3 | DATA-07 | — | Flag OFF → `discovery_available()` False; all reads no-op | unit | `pytest tests/test_discovery_flag.py::test_flag_off_hides` | ❌ (134-05) | ⬜ pending |
| Task 2 | 134-05 | 3 | DATA-07/08 | T-134-tamper | Sidecar absent / corrupt / incompatible-schema → `ready=False`; app stays up | unit | `pytest tests/test_discovery_loader.py::{test_absent,test_corrupt_integrity,test_incompatible_version}` | ❌ (134-05) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-08 | — | `PRAGMA integrity_check == ok`; release-contract row counts match actuals; source-DB hash recorded | integration | `pytest tests/test_discovery_release_contract.py` | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-10 | — | Units merge Oxford parts + physical joins but NOT `Scribe join`; `unit_id` deterministic; ≤1 unit/sys_id | unit | `pytest tests/test_discovery_units.py::{test_scribe_not_merged,test_unit_id_deterministic}` | ❌ (134-03) | ⬜ pending |
| Task 1 | 134-08 | 5 | PERF-01 | T-134-dos | Query latencies within caps; discovery adds ≤250 MB RSS (measured, recorded in `discovery-budgets.md`) | measurement | benchmark script (p95 timings) + RSS probe — recorded, not a hard unit gate | ❌ (134-08) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

> Witness-unit merging (DATA-10) is authored in `134-04` Task 3 (`units.py`) and PROVEN against the
> fixture in `134-03` Task 3 (`test_discovery_units.py`) — the row above points at the proving test.

---

## Wave 0 Requirements

> Wave 0 is DISTRIBUTED, not a single pre-wave: the test infra is created inside each owning plan's
> own wave, dependency-ordered — ids + masking (Wave 1) → fixture + invariants (Wave 2) →
> loader/flag (Wave 3) → service (Wave 4) → bench (Wave 5). Each item below is allocated to a plan.

- [x] Extend `scripts/check_atlas_masking.py` with a `--scan-sqlite` mode + `tests/test_masking_sqlite.py`. → **134-02** (Wave 1)
- [x] `tests/test_discovery_schema.py`, `test_discovery_bands.py`, `test_discovery_frame.py`, `test_discovery_units.py` — build-output invariants over a small deterministic fixture DB → **134-03** (Wave 2); `test_discovery_ids.py` (golden id recipe) → **134-01** (Wave 1).
- [x] `tests/test_discovery_service.py` → **134-06** (Wave 4); `test_discovery_flag.py`, `test_discovery_loader.py` → **134-05** (Wave 3); `test_discovery_release_contract.py` → **134-03** (Wave 2). (Loader tests model on the existing `atlas_assets` tests.)
- [x] A benchmark/RSS script feeding `discovery-budgets.md` (PERF-01 numbers measured, not asserted) → **134-08** (Wave 5); the budgets doc itself created in **134-02** (Wave 1).
- [x] A tiny committed fixture `discovery.db` builder (deterministic, masking-safe synthetic data) so CI never needs the 3.1 GB research DB → **134-03** (Wave 2).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Authoritative E1 band-source file + join key (OQ1) confirmation | DATA-02 | Domain fact, not a computable invariant; CERT-01 (Phase 135) freezes against this frame (RESEARCH §OQ1 / A5) | Owner/researcher confirms the proposed E1 file + join key at the 134-01 Task 2 blocking checkpoint before the schema band-source section is frozen |
| Neutral-title owner review (approve/hand-pick M-source literary subset) | DATA-04 | Human curation gate — fail-closed; owner is the authority (D-06/D-08) | Owner edits the generated review artifact; only approved rows re-distill; unreviewed = excluded |
| PERF-01 latency/RSS budgets on the real prod-scale `.db` | PERF-01 | PERF-01 mandates measurement (not assertion); prod box RSS not reproducible in CI | Run the benchmark/RSS script over the built `discovery.db`; record results in `discovery-budgets.md` before release |
| Deploy: temp-upload → verify → atomic-rename → code | DATA-08 | Requires live server access + asset-first deploy posture | Follow the documented rollback + rebuild recipe on the web box |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (checkpoint/human-only tasks carry `<human-check>`)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (distributed across waves, dependency-ordered)
- [x] No watch-mode flags
- [x] Feedback latency < 60s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** 2026-07-21
