---
phase: 86
slug: cudl-coverage-audit-and-synthetic-reattempt
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-10
---

# Phase 86 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Sourced from `86-RESEARCH.md` "## Validation Architecture" section.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (existing — see `tests/` and `pytest.ini`) |
| **Config file** | `pytest.ini` (project-root) |
| **Quick run command** | `pytest tests/test_fist_cudl_bridge.py tests/test_synthetic_sys_id.py tests/test_nli_oxford_attribution.py -q` |
| **Full suite command** | `pytest tests/ -q` |
| **Estimated runtime** | ~12s (quick), ~45s (full per existing baseline) |

---

## Sampling Rate

- **After every task commit:** Run quick run command
- **After every plan wave:** Run full suite command
- **Before `/gsd-verify-work`:** Full suite must be green AND HUMAN-UAT signed off
- **Max feedback latency:** 12s (quick), 45s (full)

---

## Per-Task Verification Map

> Filled in by the planner during plan generation. Each plan task gets a row referencing its file, requirement (AUDIT-01..03 / SYNTH-01..06 carry-forward), threat reference (if any), and a verifiable command.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| _to be populated by planner_ | — | — | — | — | — | — | — | — | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

> Test scaffolding the planner must create in Wave 0 BEFORE implementation begins (Nyquist contract).

- [ ] `tests/test_fist_cudl_bridge.py` — stubs for D-02 (`fist_to_cudl_keys`, `lookup_fist_by_cudl`), D-02a normalizer fixtures (Mosseri Roman, FIST data-noise prefix-strip, `(N)` series-suffix, Or. multi-segment dot-fix)
- [ ] `tests/test_synthetic_generation_phase86.py` — stubs for `_build_qualifying_inventories` CUDL-walked path, D-04 multi_signature relax (T-S NS 329.96 fixture), D-06 parent-shadow filter, D-01a image-bearing-only invariant
- [ ] `tests/test_nli_oxford_attribution.py` — golden 20-row fixture from v7.9.4 commit (AUDIT-03 D-10) + scan-sweep (`test_no_new_oxford_with_nli_text`)
- [ ] `tests/conftest.py` — confirm existing fixtures cover `tmp_path`, FIST.db read-only fixture, libraries.csv backup/restore (or extend if needed)
- [ ] `tests/test_synthetic_sys_id.py` — existing `TestNoIntCoercion` lint must continue passing (no Phase 86 violations introduced)

---

## Manual-Only Verifications

> Cases where a human must drive the verification because the behavior involves browser/UI interaction, human pattern adjudication, or release artifacts.

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| HUMAN-UAT-01: Browse 5–10 representative synthetic sys_ids (T-S NS 329.96 + Mosseri + Or. + T-S F if covered + bib-only edge case) | D-12.1 | Visual confirmation of CUDL image rendering, FJMS enrichment dialogs, no-Alma graceful degradation | Open `/browse?sys_id=99...` for each fixture sys_id; verify image loads, FJMS catalog renders when present, no console errors, no 5xx |
| HUMAN-UAT-02: Search T-S NS 329.96 in Shelfmark mode → result row → browse opens | D-12.2 | End-to-end search→browse round-trip with the originating user case | `/search?q=T-S NS 329.96&mode=shelfmark`; click result; verify browse loads with CUDL image |
| HUMAN-UAT-03: List round-trip with synthetic sys_id | D-12.3 | Cloud sync of synthetic sys_id; cross-device list integrity | Add a synthetic sys_id to a list; verify it appears in `/lists`; reload; verify still present |
| HUMAN-UAT-04: Correction button hidden on web + desktop btn_b_edit hidden + Ctrl+Shift+S gives QMessageBox without crash | D-12.4 | Phase 85 SYNTH-05 D-13 contract — synthetic rows are read-only | Web `/browse?sys_id=99...` → no correction button visible; desktop browse same sys_id → btn_b_edit hidden; Ctrl+Shift+S → QMessageBox not crash |
| HUMAN-UAT-05: Open desktop app post-build, repeat browse + list flows | D-12.5 | Desktop installer parity (or bundle-with-next-release) | Desktop app browse + list flows replicate web flows |
| HUMAN-UAT-06: PostHog confirms `is_synthetic: true` events fire on synthetic browse | D-12.6 | Telemetry contract from Phase 85 SYNTH-06 still load-bearing under new data | PostHog Live → filter `event = $pageview AND properties.is_synthetic = true` → sample event matches a real synthetic browse |
| RESIDUE-PATTERN-ADJ: User adjudicates 5 residue pattern families with sample fixtures | D-02b, D-02c | Pattern recovery requires human judgment about FIST↔CUDL semantic equivalence | Read `86-RESIDUE-PATTERNS.md`; for each family (T-S F flattened-series, T-S NS minute-fragments, Or. single-segment ambiguity, Mosseri exotics, T-S Misc multi-segment), accept/reject/spot-check; planner-listed accepted rules get integrated into `fist_cudl_bridge.py` before generation runs |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (`tests/test_fist_cudl_bridge.py`, `tests/test_synthetic_generation_phase86.py`, `tests/test_nli_oxford_attribution.py`)
- [ ] No watch-mode flags
- [ ] Feedback latency < 45s (full suite)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
