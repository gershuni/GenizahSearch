---
phase: 81B
slug: claude-skill-consumer
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-04
---

# Phase 81B — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (project standard) |
| **Config file** | `pyproject.toml` / `pytest.ini` (existing) |
| **Quick run command** | `pytest tests/test_skill_consumer.py -x` |
| **Full suite command** | `pytest tests/test_skill_consumer.py tests/test_skill_throttle.py tests/test_skill_smoke.py` |
| **Estimated runtime** | ~5–10 seconds (stubbed HTTP); ~30–60 seconds when smoke runs against a live deployment |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_skill_consumer.py -x` (quick — stubbed HTTP, no network)
- **After every plan wave:** Run the full suite above
- **Before `/gsd-verify-work`:** Full suite must be green AND a live smoke run against the production deployment is observed by the user (phase gate per ROADMAP.md)
- **Max feedback latency:** 10 seconds for the quick run

---

## Per-Task Verification Map

> Filled in by the planner from PLAN.md tasks. Every task with code output must have an `<automated>` verify command or a Wave 0 dependency. Tasks that produce only docs (e.g., SKILL.md frontmatter prose) may use `file_exists` plus a frontmatter-schema lint command.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD by planner | — | — | SKILL-01..06 | — | — | — | — | — | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_skill_consumer.py` — stubs for SKILL-02 (ranked output), SKILL-03 (text-source/image-availability annotations), SKILL-05 (known-witness flag/exclude)
- [ ] `tests/test_skill_throttle.py` — token-bucket persistence + per-endpoint isolation (SKILL-06)
- [ ] `tests/test_skill_smoke.py` — live-mode smoke harness gated by env var (e.g. `SKILL_SMOKE=1`); skips otherwise
- [ ] `tests/conftest.py` shared HTTP-stub fixtures (mirror `tests/test_search_api_v2.py` Layer-1 stub pattern from Phase 81A)

*Pytest infrastructure already exists project-wide — no framework install required.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Live end-to-end run against production deployment | Phase gate (ROADMAP.md) | Requires user-observed scholarly query against `genizahsearch.com`; ranking must be user-signed-off | Run skill with at least one representative query; user reviews ranked candidates, confirms tier assignments and browse-honesty annotations are present and correct |
| Skill discoverability across surfaces | SKILL-01 (D-01 portability) | Requires loading the skill in Claude Code (and ideally Claude Desktop) and confirming Claude invokes it on a representative query | Install per SKILL.md `## Installation`; ask Claude a Genizah scholarly question; confirm skill is invoked and runs to completion |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s for quick run
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
