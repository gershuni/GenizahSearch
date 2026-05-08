---
phase: 85
slug: synthetic-fjms-inventory-rows
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-08
---

# Phase 85 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Detailed test→requirement mapping comes from 85-RESEARCH.md `## Validation Architecture`.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (project standard) |
| **Config file** | `pytest.ini` / `pyproject.toml` |
| **Quick run command** | `pytest tests/test_synthetic_sys_id.py tests/test_generate_synthetic_rows.py -x` |
| **Full suite command** | `pytest tests/` |
| **Estimated runtime** | ~120 seconds (full suite); ~3 seconds (synthetic-only) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_synthetic_sys_id.py -x` (~1s)
- **After every plan wave:** Run `pytest tests/test_synthetic_*.py tests/test_generate_synthetic_rows.py -x` (~5s)
- **Before `/gsd-verify-work`:** Full suite must be green; `python scripts/check_docs.py` green
- **Max feedback latency:** 5 seconds for synthetic-scoped tests

---

## Per-Task Verification Map

> Planner fills this table during plan creation — one row per `<task>` block, mapped to test file from RESEARCH.md `## Validation Architecture`.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 85-01-XX | 01 | 1 | SYNTH-01 | — | helper functions deterministic + collision-safe | unit | `pytest tests/test_synthetic_sys_id.py -x` | ❌ W0 | ⬜ pending |
| 85-02-XX | 02 | 2 | SYNTH-02, SYNTH-03 | T-85-01 (CSV injection on shelfmark) | regenerated CSV idempotent + ambiguity-excluded | unit + integration | `pytest tests/test_generate_synthetic_rows.py -x` | ❌ W0 | ⬜ pending |
| 85-03-XX | 03 | 2 | SYNTH-05 | T-85-02 (SQL injection in FIST harvest) | export emits synthetic AlmaId rows for 11 tables idempotently | integration | `pytest tests/test_export_fist_synthetic.py -x` | ❌ W0 | ⬜ pending |
| 85-04-XX | 04 | 3 | SYNTH-04 | T-85-03 (XSS via FJMS title) | browse hides NLI elements + CUDL default; web/desktop parity | integration + smoke | `pytest tests/test_browse_synthetic.py -x` | ❌ W0 | ⬜ pending |
| 85-05-XX | 05 | 3 | SYNTH-06 | T-85-04 (data leakage via PostHog) | community writes round-trip; serializer emits is_synthetic; corrections defer | integration | `pytest tests/test_synthetic_round_trip.py tests/test_search_serializer.py -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

> All test files below are NEW. Wave 0 is the test-stub creation prerequisite (RED phase if TDD applies).

- [ ] `tests/test_synthetic_sys_id.py` — helper unit tests (encode/decode round-trip, boundary cases, normalization tolerance per D-13)
- [ ] `tests/test_generate_synthetic_rows.py` — regeneration script idempotency + collision-detection + ambiguity-residue
- [ ] `tests/test_export_fist_synthetic.py` — FJMS sidecar exporter UNION test (synthetic AlmaId rows present in 11 tables)
- [ ] `tests/test_browse_synthetic.py` — browse smoke (hide-NLI branches; CUDL-default image source; metadata-only fallback)
- [ ] `tests/test_synthetic_round_trip.py` — lists/exclusions/parallels/comments round-trip with synthetic sys_id
- [ ] `tests/test_search_serializer.py` — public-API `is_synthetic` field assertion (additive, schema_version=1)
- [ ] `tests/fixtures/synthetic_fixtures.py` — golden T-S NS 329.96 fixture + collision-edge fixtures

*Existing infrastructure (pytest, conftest.py) covers framework needs — no install step required.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Browse renders T-S NS 329.96 with CUDL image, no broken NLI panels | SYNTH-04 | Visual; CUDL IIIF live network call | Web: `python -m web.main`, navigate to `/browse?sys_id=99{INVENTORYID}000000`; verify image renders, no NLI section, no console errors. Desktop: `python genizah_app.py`, paste shelfmark `T-S NS 329.96`, verify same. |
| Public `/api/browse?sys_id=99...000000` returns clean JSON with `is_synthetic: true` | SYNTH-06 D-14 | External API consumers | `curl https://genizahsearch.com/api/browse?sys_id=99{ID}000000` and inspect JSON shape against Phase 83 OpenAPI spec |
| Phase 86 scan_cudl_orphans.py post-Phase-85 delta confirms residue ≤300 (Phase 84 target) and synthetic-row creation accounts for the difference | SYNTH-02 + Phase 86 prep | Empirical; depends on FIST.db state | `python scripts/scan_cudl_orphans.py > reports/cudl_orphans_post_phase85.csv && wc -l` and diff against `reports/cudl_orphans_post_phase84.csv` |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s for synthetic-scoped tests
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
