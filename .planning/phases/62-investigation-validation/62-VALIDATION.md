---
phase: 62
slug: investigation-validation
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-03
---

# Phase 62 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | `tests/` directory (existing) |
| **Quick run command** | `pytest tests/test_nli_investigation.py -x` |
| **Full suite command** | `pytest tests/test_nli_investigation.py -v` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_nli_investigation.py -x`
- **After every plan wave:** Run `pytest tests/test_nli_investigation.py -v`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 5 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 62-01-xx | 01 | 1 | INV-04 | manual | N/A (TOS review is human judgment) | N/A | ⬜ pending |
| 62-02-xx | 02 | 1 | INV-01 | integration | `python scripts/nli_rate_test.py --dry-run` | ❌ W0 | ⬜ pending |
| 62-03-xx | 03 | 2 | INV-02, INV-05 | integration | `python scripts/nli_storage_sample.py --dry-run` | ❌ W0 | ⬜ pending |
| 62-04-xx | 04 | 2 | INV-03 | unit | `pytest tests/test_nli_investigation.py -k filesystem` | ❌ W0 | ⬜ pending |
| 62-05-xx | 05 | 3 | INV-01-05 | manual | Review 62-REPORT.md completeness | N/A | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_nli_investigation.py` — stubs for NLI-only subset query, filesystem inode calculation
- [ ] `scripts/nli_rate_test.py` — rate test script with --dry-run mode
- [ ] `scripts/nli_storage_sample.py` — storage sampling script with --dry-run mode

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| TOS review and NLI outreach | INV-04 | Requires human judgment on legal terms | Read NLI TOS, draft outreach email, record gate decision |
| Image quality comparison | INV-05 | Subjective visual assessment | Compare 800px vs 1200px samples side-by-side for research usability |
| Investigation report completeness | INV-01-05 | Report is a prose document | Verify 62-REPORT.md covers all 5 INV requirements with data |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
