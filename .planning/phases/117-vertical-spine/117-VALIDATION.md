---
phase: 117
slug: vertical-spine
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-17
---

# Phase 117 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini / pyproject.toml (existing) |
| **Quick run command** | `pytest tests/test_joins_lab_web.py -q` |
| **Full suite command** | `pytest tests/ -q` |
| **Estimated runtime** | ~see RESEARCH.md Validation Architecture |

---

## Sampling Rate

- **After every task commit:** Run the quick run command
- **After every plan wave:** Run the full suite command
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** see RESEARCH.md

---

## Per-Task Verification Map

> Filled by the planner / nyquist auditor during planning. Derived from the
> "## Validation Architecture" section of 117-RESEARCH.md. Key required signals:
> - CI test: no raw `app.storage.user` access under `web/` (`tests/test_no_raw_storage_access.py` allowlist stays `[]`)
> - CI test: `execute_search` is NOT called on the event loop (off-loop via `run.io_bound`)
> - Two-anonymous-session no-state-bleed test for the `joins_lab` safe_storage schema

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | — | — | — | — | — | — | — | — | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_joins_lab_web.py` — stubs for FND/ANC/BLD/CND requirements
- [ ] Shared fixtures for anonymous-session safe_storage isolation

*Filled during planning from RESEARCH.md Validation Architecture.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Anchor image renders with zoom/pan + folio nav in a real browser | ANC-01 | Visual / IIIF-dependent rendering | Open `/joins-lab?sys_id=...` and confirm image + controls |

*Remaining behaviors target automated verification — see RESEARCH.md.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < threshold
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
