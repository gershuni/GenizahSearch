---
phase: 134
slug: discovery-data-spine
status: draft
nyquist_compliant: false
wave_0_complete: false
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

> Planner fills Task ID / Plan / Wave from the actual PLAN.md task breakdown.
> Requirement → observable-signal → command rows are pre-derived from research.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | — | 0 | DATA-05 | T-134-leak | No M-source/R-source string in sidecar/repo/surface (schema + every cell) | integration (CI gate) | `check_atlas_masking.py --scan-sqlite <db> --scan-repo --strict` (exit 0) | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-03 | T-134-leak | No `text`/`cat`/raw-`work_id`/`title`/`author`/`genre`/`provenance` reference columns; only offsets + snapshot hash | unit | `pytest tests/test_discovery_schema.py::test_no_reference_columns` | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-01/02 | — | Deterministic `claim_id`/`unit_id` stable across rebuilds | unit (golden) | `pytest tests/test_discovery_ids.py::test_claim_id_golden` | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-02 | — | Exactly one band per claim key post-precedence; precedence within-key only (multi-work-per-MS preserved) | unit | `pytest tests/test_discovery_bands.py::test_one_band_per_key` | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-02 | — | Frozen-frame reproducibility: rebuild → identical frame content hash | integration | `pytest tests/test_discovery_frame.py::test_frame_hash_reproducible` | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-06 | T-134-dos | Overload → `DiscoveryUnavailable` within timeout; loop stays responsive (never hangs) | async unit | `pytest tests/test_discovery_service.py::test_overload_returns_unavailable` | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-06 | — | Every list query bounded (`LIMIT`) + server-side pagination | unit | `pytest tests/test_discovery_service.py::test_pagination_bounds` | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-07 | — | Flag OFF → `discovery_available()` False; all reads no-op | unit | `pytest tests/test_discovery_flag.py::test_flag_off_hides` | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-07/08 | T-134-tamper | Sidecar absent / corrupt / incompatible-schema → `ready=False`; app stays up | unit | `pytest tests/test_discovery_loader.py::{test_absent,test_corrupt_integrity,test_incompatible_version}` | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-08 | — | `PRAGMA integrity_check == ok`; release-contract row counts match actuals; source-DB hash recorded | integration | `pytest tests/test_discovery_release_contract.py` | ❌ W0 | ⬜ pending |
| TBD | — | — | DATA-10 | — | Units merge Oxford parts + physical joins but NOT `Scribe join`; `unit_id` deterministic; ≤1 unit/sys_id | unit | `pytest tests/test_discovery_units.py::{test_scribe_not_merged,test_unit_id_deterministic}` | ❌ W0 | ⬜ pending |
| TBD | — | — | PERF-01 | T-134-dos | Query latencies within caps; discovery adds ≤250 MB RSS (measured, recorded in `discovery-budgets.md`) | measurement | benchmark script (p95 timings) + RSS probe — recorded, not a hard unit gate | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] Extend `scripts/check_atlas_masking.py` with a `--scan-sqlite` mode + `tests/test_masking_sqlite.py`.
- [ ] `tests/test_discovery_schema.py`, `test_discovery_ids.py`, `test_discovery_bands.py`, `test_discovery_frame.py`, `test_discovery_units.py` — build-output invariants over a small deterministic fixture DB.
- [ ] `tests/test_discovery_service.py`, `test_discovery_flag.py`, `test_discovery_loader.py`, `test_discovery_release_contract.py` — service + loader (model loader tests on the existing `atlas_assets` tests).
- [ ] A benchmark/RSS script feeding `discovery-budgets.md` (PERF-01 numbers measured, not asserted).
- [ ] A tiny committed fixture `discovery.db` builder (deterministic, masking-safe synthetic data) so CI never needs the 3.1 GB research DB.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Neutral-title owner review (approve/hand-pick M-source literary subset) | DATA-04 | Human curation gate — fail-closed; owner is the authority (D-06/D-08) | Owner edits the generated review artifact; only approved rows re-distill; unreviewed = excluded |
| PERF-01 latency/RSS budgets on the real prod-scale `.db` | PERF-01 | PERF-01 mandates measurement (not assertion); prod box RSS not reproducible in CI | Run the benchmark/RSS script over the built `discovery.db`; record results in `discovery-budgets.md` before release |
| Deploy: temp-upload → verify → atomic-rename → code | DATA-08 | Requires live server access + asset-first deploy posture | Follow the documented rollback + rebuild recipe on the web box |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
