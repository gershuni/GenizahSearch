---
phase: 119
slug: candidates-compare-visual-similarity
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-19
---

# Phase 119 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source of truth for test seams: `119-RESEARCH.md` → "## Validation Architecture".

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | `pytest.ini` / `conftest.py` (GUI split via `_GUI_TEST_FILES`) |
| **Quick run command** | `python -m pytest tests/test_joins_lab_off_loop.py tests/test_no_raw_storage_access.py -q` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -q` |
| **Estimated runtime** | ~quick <30s / full several min (see CI marker-based gui-tests split) |

---

## Sampling Rate

- **After every task commit:** Run quick command (off-loop + safe_storage guards)
- **After every plan wave:** Run full suite command
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** ~30 seconds (quick), full on wave boundaries

---

## Per-Task Verification Map

> Filled by the planner / nyquist-auditor during execution. Seed seams from RESEARCH.md "Validation Architecture":
> - Off-loop discipline (VS lookup + enrichment batch must join `execute_search` under the guard) — `tests/test_joins_lab_off_loop.py`
> - Multitenant Phase-87 invariant — `tests/test_no_raw_storage_access.py` (allowlist MUST stay `[]`)
> - `badge_and_tooltip()` precedence (⚓ › ⇄ › 👁) consistent across grid / table / Compare
> - Self-match: `dedup_candidates(include_self=False)` excludes the anchor, NO banner (D-13)

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | — | 0 | VSM-02 | — | `badge_and_tooltip` precedence ⚓›⇄›👁 | unit | `pytest tests/test_joins_lab.py -k badge` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] Implement `shared/joins_lab.badge_and_tooltip()` — referenced by CONTEXT.md/UI-SPEC but does NOT yet exist (RESEARCH finding #1; Wave 0 blocker for all badge rendering)
- [ ] Extend `tests/test_joins_lab_off_loop.py` (or add parallel tests) to cover the NEW VS lookup AND enrichment batch call sites (RESEARCH finding #4 — guard currently covers only `execute_search`)
- [ ] Confirm `tests/test_no_raw_storage_access.py` allowlist stays `[]` (Phase-87 invariant; 119 triage/filter/view is in-memory page state)

*Final Wave 0 list is the planner's responsibility; the above are the RESEARCH-identified blockers.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Large-thumbnail visual triageability | CND-04 / D-09 | "Large enough" is a human visual judgment | Load Joins Lab, run a search, confirm grid thumbnails are visually triageable (160×160) |
| Compare per-pane zoom/pan + folio nav feel | CMP-01/02 | Interactive image manipulation | Open Compare, zoom/pan each pane independently, navigate folios per pane |
| 👁 toggle empty/disabled/no-VS-data/empty-intersection states render clearly | VSM-01/06 | Visual affordance, not a blank surface | Toggle 👁 in each of: no anchor, anchor w/o VS data, ON+query→0 intersection |

*Compare and image-interaction behaviors are GUI-only; automated coverage targets the state/triage/badge logic beneath them.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (esp. `badge_and_tooltip`)
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
