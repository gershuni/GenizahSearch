---
phase: 106
slug: joins-lab-shared-core-pure-logic-service-adapters-unit-tests
status: planned
nyquist_compliant: true
wave_0_complete: false
created: 2026-06-03
---

# Phase 106 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing project standard; ~2500 tests) |
| **Config file** | none — pytest discovers via `tests/` directory |
| **Quick run command** | `pytest tests/test_joins_lab.py -x -q` |
| **Full suite command** | `pytest tests/ -x` |
| **Estimated runtime** | quick ~3–8s (pure-logic unit tests, no engine init); full suite ~minutes |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_joins_lab.py -x -q`
- **After every plan wave:** Run `pytest tests/ -x` (no regression in the wider suite)
- **Before `/gsd-verify-work`:** Full suite must be green + `python -m ruff check shared/joins_lab.py tests/test_joins_lab.py`
- **Max feedback latency:** < 10 seconds (quick run)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 106-01-01 | 01 | 1 | JWB-10, ARCH-SEARCHEXECUTOR-ADAPTER, ARCH-WEB-REUSABLE | T-106-02 | normalize_candidate tolerates malformed/partial dicts (safe .get + page_of try/except → None; no KeyError, no wrong-typed page) | unit | `pytest tests/test_joins_lab.py::TestPageOf tests/test_joins_lab.py::TestNormalize tests/test_joins_lab.py::TestProtocol -x -q` | ❌ W0 (Plan 01 T1 creates it) | ⬜ pending |
| 106-01-02 | 01 | 1 | JWB-10 | T-106-01 | compose() emits only literal terms + engine control tokens, no regex metachars built here (ReDoS accepted — engine owns regex compile) | unit | `pytest tests/test_joins_lab.py::TestCompose -x -q` | ✅ (after 106-01-01) | ⬜ pending |
| 106-01-03 | 01 | 1 | ARCH-NO-PYQT, ARCH-NO-FIST-SQLITE, ARCH-WEB-REUSABLE | T-106-03 | static AST guard proves no PyQt import + no fist_data / sqlite3.connect (info-disclosure mitigated) | unit (AST) | `pytest tests/test_joins_lab.py::TestStaticImport -x -q` | ✅ (after 106-01-01) | ⬜ pending |
| 106-02-01 | 02 | 2 | JWB-11, ARCH-SEARCHEXECUTOR-ADAPTER | T-106-05 | every executor call wrapped try/except → []/None/"" (service hiccup degrades, no raise); OR neighbor synth bounded by len(b_set)*2 | unit (+FakeSearchExecutor) | `pytest tests/test_joins_lab.py::TestResolveOtherSide tests/test_joins_lab.py::TestCrossSide -x -q` | ✅ (after 106-01-01) | ⬜ pending |
| 106-02-02 | 02 | 2 | JWB-12 | T-106-04, T-106-06 | dedup is O(n) reducing pass (DoS accepted, no new unbounded alloc); merge annotates via dataclasses.replace (frozen-safe, no corruption) | unit | `pytest tests/test_joins_lab.py::TestDedup tests/test_joins_lab.py::TestMerge -x -q` | ✅ (after 106-01-01) | ⬜ pending |
| 106-03-01 | 03 | 3 | JWB-12, ARCH-WEB-REUSABLE | T-106-07 | _match_line wraps re.compile in try/except re.error → -1 (ReDoS accepted: pattern is engine-produced highlight, no new surface) | unit | `pytest tests/test_joins_lab.py::TestSelfMatch tests/test_joins_lab.py::TestMatchLine -x -q` | ✅ (after 106-01-01) | ⬜ pending |
| 106-03-02 | 03 | 3 | JWB-12 | T-106-08 | htmlify html.escape(text) before injecting fixed `<b>` tags → corpus markup cannot inject (XSS mitigated) | unit | `pytest tests/test_joins_lab.py::TestSnippet -x -q` | ✅ (after 106-01-01) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_joins_lab.py` — does NOT exist; created in Plan 01 Task 1 (RED tests written before
      implementation per TDD). Covers SC#1 (TestCompose), SC#3/4 (TestDedup/TestMerge), SC#2
      (TestResolveOtherSide/TestCrossSide), SC#5 (TestSelfMatch/TestMatchLine/TestSnippet),
      SC#6 (TestStaticImport) + foundational TestPageOf/TestNormalize/TestProtocol.
- [ ] No framework install needed — pytest is the project standard.
- [ ] No `conftest.py` — the project has none; `FakeSearchExecutor` + `_make_result` are module-level
      in the test file (PATTERNS convention).

*The first task of Plan 01 IS the Wave 0 task: it creates `tests/test_joins_lab.py` with the failing
(RED) test scaffolds before `shared/joins_lab.py` implements against them.*

---

## Manual-Only Verifications

*All phase behaviors have automated verification.* This is a pure-logic module — every one of the six
success criteria is covered by a deterministic `expect(fn(input)) == output` pytest case. There is no
UI, no network, no human-in-the-loop step in Phase 106.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (every task has `<automated>` commands;
      the lone test-file-creating task IS the Wave 0 task)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (all 7 tasks have automated verify)
- [x] Wave 0 covers all MISSING references (`tests/test_joins_lab.py` created in Plan 01 Task 1)
- [x] No watch-mode flags (all commands are `-x -q` one-shot)
- [x] Feedback latency < 10s (pure-logic quick run)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-06-03
