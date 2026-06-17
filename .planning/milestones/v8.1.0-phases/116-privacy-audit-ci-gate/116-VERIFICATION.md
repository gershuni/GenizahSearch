---
phase: 116-privacy-audit-ci-gate
verified: 2026-06-16T13:30:00Z
status: human_needed
score: 6/7 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Run `GenizahSearchPro.exe --telemetry-selftest` on a CLEAN Windows VM with NO Python installed (network UP)"
    expected: "stdout: SSL_OK, exit code 0. Confirms certifi/cacert.pem is bundled inside the frozen binary (NOT borrowed from the dev-machine Python). Also confirm `desktop_selftest` event appears in PostHog project 134161 (EU) — this closes Phase 114 live-delivery UAT. Then disable network adapter and run `--telemetry-selftest-offline` → OFFLINE_OK printed fast (well under 2s). Then launch normally with adapter still disabled → app is usable and silent (no telemetry error, no crash, no delay). This run satisfies SC#3 + closes INFRA-06 + closes Phase 114 live-delivery UAT."
    why_human: "Requires a frozen PyInstaller .exe built at /release time running on a clean no-Python Windows VM. Cannot be verified in a dev session: the frozen binary bundles certifi's cacert.pem separately from the dev-machine Python's SSL stack; only a clean-VM run proves the bundle shipped. No such build exists in this session (Task 3, 116-02-PLAN.md, is explicitly a `checkpoint:human-verify` gate)."
---

# Phase 116: Privacy Audit + CI Gate — Verification Report

**Phase Goal:** The complete telemetry stack is validated end-to-end: the AST guard runs green in CI on both Ubuntu and Windows, automated tests prove that no forbidden field ever reaches `enqueue_event`, frozen-binary SSL and offline degradation are verified on a clean Windows machine, and the operational runbook documents the desktop PostHog project, embedded key posture, and drop-counter monitoring.

**Verified:** 2026-06-16T13:30:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| 1 | PRIV-03 AST guard runs green (SC#1) | VERIFIED | `tests/test_telemetry_no_direct_posthog.py` — 6 passed. File unchanged from Phase 111-03 commit `a376890b`. CI runs it on both Ubuntu + Windows via the existing `tests` job (no ci.yml change needed per D-09). |
| 2 | Automated tests prove no forbidden field ever reaches `enqueue_event` (SC#2 / PRIV-04) | VERIFIED | `tests/test_telemetry_priv04.py` — 9 tests, all green. 8 forbidden-field/value tests (key-absent AND raw-needle-absent from `json.dumps(payload)`) + 1 pre-consent zero-emit test across all 3 entry points. `_safe_context` hardened with `_CONTEXT_FILENAME_RE` to block filename-extension-shaped contexts. |
| 3 | Frozen-binary SSL proof on a clean no-Python Windows VM (SC#3) | HUMAN-NEEDED | `send_selftest_event_sync()` and `--telemetry-selftest` block exist and are code-complete (verified below). The gold-standard proof requires running the frozen exe on a clean VM — deliberately deferred to `/release` time (116-02 Task 3 is a `checkpoint:human-verify`). |
| 4 | Offline degradation verified — `--telemetry-selftest-offline` prints OFFLINE_OK fast; normal offline launch is silent | HUMAN-NEEDED | Part of the same clean-VM HUMAN-UAT as SC#3 above. |
| 5 | Operational runbook documents shared PostHog project, embedded key posture, and drop-counter monitoring (INFRA-06 / D-08) | VERIFIED | `docs/guides/TELEMETRY_RUNBOOK.md` exists, 205+ lines, 6 sections. All required tokens present: `platform=desktop`, `134161`, `GENIZAH_TELEMETRY_KEY`, `web.api_hardening`, `shared.posthog_server`, `--telemetry-selftest`, `SSL_OK`, `OFFLINE_OK`, `NO_KEY`, `send_selftest_event_sync`, `queue saturation`, `milestone-exit`, rotation procedure. Word "isolated" absent. Drop counters explicitly documented as queue-saturation-only, NOT delivery proof. |
| 6 | REQUIREMENTS.md INFRA-06 wording corrected to shared-project posture (D-07) | VERIFIED | Stale "isolated from the web project" clause removed; `platform=desktop` + id 134161 wording added; dated `AMENDED 2026-06-16` note citing `POSTHOG-PROJECT-DECISION.md` appended. `[ ]` checkbox and Pending status-table rows unchanged (completion flip deferred to milestone verification pass). |
| 7 | Milestone-exit regression gate documented in VERIFICATION.md and runbook (D-10) | VERIFIED | `116-VERIFICATION.md` (this file) contains the `## Milestone-exit regression gate (D-10)` section with the exact pytest command and completion-flip-deferral note. Runbook cross-references it. |

**Score:** 5/7 truths fully verified (truths 3 and 4 require human gate); 6/7 code artifacts verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `tests/test_telemetry_priv04.py` | PRIV-04 forbidden-field + pre-consent tests (9 functions, 130+ lines) | VERIFIED | 9 tests, all green. Contains `test_priv04_my_library_path_not_in_payload`, all 8 forbidden-field tests + 1 pre-consent zero-emit. `json.dumps` needle assertions present. ruff: clean. |
| `desktop/telemetry.py` | `_safe_context` hardened with `_CONTEXT_FILENAME_RE` | VERIFIED | `_safe_context('manuscript_notes.docx')` → `'unregistered'`; `_safe_context('search_tab.run_query')` → `'search_tab.run_query'`. Both confirmed by live `python -c` invocation. |
| `shared/posthog_server.py` | `send_selftest_event_sync()` — synchronous, return-valued, `__all__` exported | VERIFIED | Function exists, has docstring, in `__all__`. Returns `NO_KEY` (no network call) when unconfigured; `SSL_OK` on mocked HTTP 200; `SSL_FAIL` on mocked `SSLError`. Never raises. Never touches `_event_queue`. |
| `genizah_app.py` | `--telemetry-selftest` / `--telemetry-selftest-offline` block before QApplication | VERIFIED | Block at line 27489; QApplication at line 27584 (precedes by 95 lines). All 4 tokens present: `SSL_OK`, `SSL_FAIL`, `NO_KEY`, `OFFLINE_OK`. In-memory consent toggle via `_enabled_lock` (not `set_consent`). `send_selftest_event_sync` drives the decision (no `get_dropped_event_count`). `finally` restores `_enabled`. |
| `docs/guides/TELEMETRY_RUNBOOK.md` | 5 D-08 sections + milestone-exit gate, ≥120 lines | VERIFIED | 205+ lines, 6 `##` sections. All required tokens present. "isolated" absent. "does not prove delivery" language present for drop counters. |
| `.planning/phases/116-privacy-audit-ci-gate/116-VERIFICATION.md` | Milestone-exit gate + completion-flip-deferral note | VERIFIED | This file — contains both required sections (preserved below). |
| `docs/DOCUMENTATION_INDEX.md` | TELEMETRY_RUNBOOK.md bullet under "For Developers"; timestamp 2026-06-16 | VERIFIED | Bullet present; timestamp bumped from 2026-03-26 to 2026-06-16. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `tests/test_telemetry_priv04.py` | `shared.posthog_server._event_queue` | monkeypatched fresh queue + `.get(timeout=1.0)` | VERIFIED | All 9 tests capture from the monkeypatched queue; 9 passed. |
| `tests/test_telemetry_priv04.py` | `desktop.telemetry.track / track_performance / track_error` | full chokepoint pipeline under consent gate | VERIFIED | Tests call `tel.track(...)`, `tel.track_performance(...)`, `tel.track_error(...)` directly. |
| `genizah_app.py __main__` | `shared.posthog_server.send_selftest_event_sync` | in-memory consent toggle → `_wire_transport_config` → `send_selftest_event_sync` → token | VERIFIED | `send_selftest_event_sync` present in genizah_app.py selftest block. `_enabled_lock` toggle confirmed. `set_consent` appears only in comment text (`# NEVER call set_consent`), not as a call. |
| `docs/DOCUMENTATION_INDEX.md` | `docs/guides/TELEMETRY_RUNBOOK.md` | "For Developers" bullet | VERIFIED | `TELEMETRY_RUNBOOK.md` present in index. |
| `.planning/REQUIREMENTS.md INFRA-06` | `.planning/research/POSTHOG-PROJECT-DECISION.md` | dated AMENDED wording note | VERIFIED | `POSTHOG-PROJECT-DECISION` referenced in REQUIREMENTS.md. |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `_safe_context` rejects filename extensions | `python -c "import desktop.telemetry as t; assert t._safe_context('manuscript_notes.docx')=='unregistered'"` | CTX_OK | PASS |
| `_safe_context` preserves dotted code labels | `python -c "import desktop.telemetry as t; assert t._safe_context('search_tab.run_query')=='search_tab.run_query'"` | CTX_OK | PASS |
| `send_selftest_event_sync` NO_KEY without network call | `python -c "..."` (mocked requests.post raises) | NO_KEY_OK | PASS |
| `send_selftest_event_sync` SSL_OK on HTTP 200 | `python -c "..."` (mocked 200 response) | SSL_OK_OK | PASS |
| `send_selftest_event_sync` SSL_FAIL on SSLError | `python -c "..."` (mocked SSLError) | SSL_FAIL_OK | PASS |
| All 9 PRIV-04 tests | `pytest tests/test_telemetry_priv04.py -q` | 9 passed in 0.26s | PASS |
| All 12 selftest tests | `pytest tests/test_telemetry_selftest.py -q` | 12 passed in 0.26s | PASS |
| PRIV-03 AST guard | `pytest tests/test_telemetry_no_direct_posthog.py -q` | 6 passed in 0.44s | PASS |
| Full telemetry suite (256 tests) | `pytest tests/ -k "telemetry or no_direct or no_dynamic" -q` | 256 passed, 3 skipped | PASS |
| ruff: modified files | `python -m ruff check desktop/telemetry.py shared/posthog_server.py tests/test_telemetry_priv04.py tests/test_telemetry_selftest.py` | All checks passed | PASS |
| Frozen-binary SSL proof | `GenizahSearchPro.exe --telemetry-selftest` on clean VM | NOT RUN — requires /release build | HUMAN-NEEDED |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|---------|
| PRIV-03 | 116-01 (Task 2) | AST guard enforces chokepoint — no bypass | SATISFIED | `tests/test_telemetry_no_direct_posthog.py` green; file unchanged from Phase 111-03 |
| PRIV-04 | 116-01 (Tasks 0-2) | Forbidden-field tests; no filenames/paths/queries/usernames/hostnames in payload; zero emit before consent | PARTIALLY SATISFIED | All 9 tests green; `_safe_context` hardened. Status stays Pending per deferral note — completion flip gated on HUMAN-UAT. |
| INFRA-06 | 116-02 (Tasks 1-2), 116-03 (Tasks 1-2) | Runbook (D-08 sections), selftest flag (SC#3), drop-counter documentation | PARTIALLY SATISFIED | Runbook exists and verified; selftest code exists and verified. Status stays Pending per deferral note — completion flip gated on HUMAN-UAT (SC#3 clean-VM run). |

**Note:** PRIV-04 and INFRA-06 remaining as `Pending` in REQUIREMENTS.md is CORRECT and EXPECTED behavior per the completion-status flip deferral design documented in the `## Completion-status flip deferral note` section below. This is NOT a gap.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None detected | — | — | — | — |

All modified files scanned. No `TBD`, `FIXME`, `XXX` markers. No stub returns. No hardcoded empty payloads in production paths. The `set_consent` text in `genizah_app.py` appears only in a comment (`# NEVER call set_consent`), not as a live call.

### Human Verification Required

#### 1. SC#3 Clean-VM SSL Proof + Offline Degradation + Phase 114 Live-Delivery UAT

**Test:**
1. Build `GenizahSearchPro.exe` (happens at `/release`).
2. On a CLEAN Windows VM with NO Python installed, network UP, run: `.\GenizahSearchPro.exe --telemetry-selftest`
3. Confirm PostHog project 134161 (EU) shows a `desktop_selftest` event (closes Phase 114 live-delivery UAT).
4. Disable the VM network adapter. Run: `.\GenizahSearchPro.exe --telemetry-selftest-offline`
5. Launch `.\GenizahSearchPro.exe` normally with the adapter still disabled — confirm the app is usable and silent.

**Expected:**
- Step 2: stdout `SSL_OK`, exit code 0. (`SSL_FAIL` = certifi/SSL NOT bundled — release blocker. `NO_KEY` = phc_ key not baked — release blocker.)
- Step 3: `desktop_selftest` event visible in PostHog 134161.
- Step 4: `OFFLINE_OK` printed quickly (well under 2s — offline arm makes zero network calls).
- Step 5: App opens normally, no delay, no crash, no telemetry error message.

**Why human:** Requires a PyInstaller frozen binary built at `/release` time running on a clean no-Python Windows VM. Dev-session Python has its own SSL/certifi stack — only a clean-VM test proves the frozen binary's bundled `cacert.pem` is present and trusted. No such build exists in this session (116-02 Task 3 is an explicit `checkpoint:human-verify` gate).

---

### Gaps Summary

No gaps. All automated work is complete and verified. The single `human_needed` item is the deliberate clean-VM HUMAN-UAT gate from Plan 116-02 Task 3 — a `checkpoint:human-verify` that cannot be satisfied in a dev session and is designed to run at `/release` time.

---

_Verified: 2026-06-16T13:30:00Z_
_Verifier: Claude (gsd-verifier)_

---

## Milestone-exit regression gate (D-10)

The full telemetry / crash / PostHog regression suite (~290 tests accumulated across Phases 111-116)
**MUST be green on both Ubuntu and Windows** before v8.1.0 ships. This is the milestone-exit gate.

### Components of the exit gate

| Component | Source | Guard |
|-----------|--------|-------|
| **PRIV-03 AST guard** | Phase 111-03 | `tests/test_telemetry_no_direct_posthog.py` — enforces all emission routes through the desktop chokepoint only |
| **D-17 dynamic-string guard** | Phase 111 | `tests/test_no_dynamic_telemetry_strings.py` — enforces no dynamic event names |
| **PRIV-04 scrubber tests** | Phase 116-01 | `tests/test_telemetry_priv04.py` — asserts forbidden fields never appear in payloads; asserts zero emission before consent |
| **SC#3 synchronous self-test** | Phase 116-02 | `tests/test_telemetry_selftest.py` — asserts `--telemetry-selftest` flag wiring and `send_selftest_event_sync()` behavior |
| **All other telemetry regression tests** | Phases 111-115 | `tests/test_telemetry*.py` — consent, identity, crash, usage, perf, scrubbing suites |

### Exact regression command

Run on **both** Ubuntu and Windows before each release:

```
pytest tests/test_telemetry*.py tests/test_no_direct*.py tests/test_no_dynamic*.py -m "not gui"
```

### CI coverage

The existing CI `tests` job (`/.github/workflows/ci.yml`) already runs:

```
pytest tests/ -m "not gui"
```

on **both** `ubuntu-latest` and `windows-latest` (SC#1 already satisfied — no dedicated privacy-gate
job is needed). All Phase 116 tests are added to this same suite. The milestone-exit gate is satisfied
when this job is green on the release commit on both OS runners.

### Additional human-UAT gate (D-06 / SC#3)

Before v8.1.0 ships, run `GenizahSearchPro.exe --telemetry-selftest` on a **clean Windows VM with
NO Python installed** and confirm:

- Result token is `SSL_OK` (exit 0) — proves `certifi`'s `cacert.pem` is bundled into the frozen
  binary.
- This same run also closes the Phase 114 "live PostHog event delivery" UAT item.

This is a one-time manual step; it cannot be automated in CI (requires a clean no-Python VM).

---

## Completion-status flip deferral note

**Marking PRIV-04 and INFRA-06 "Complete" in `.planning/REQUIREMENTS.md` is the milestone
verification pass's responsibility — NOT this plan (116-03).**

This documentation plan (`116-03`, `depends_on: []`) deliberately does not flip any requirement
status. The completion flips are gated on:

1. **116-01 landing** — PRIV-04 scrubber tests (`tests/test_telemetry_priv04.py`) must exist and
   be green on both OSes.
2. **116-02 landing** — SC#3 `--telemetry-selftest` CLI flag must be implemented in `genizah_app.py`
   and its tests green.
3. **D-06 human UAT** — clean-VM `--telemetry-selftest SSL_OK` confirmed.

Only after all three conditions are met should the milestone verification pass flip PRIV-04 and
INFRA-06 to `Complete` in the Traceability table. Until then, both rows stay `Pending` — a
parallel doc-only plan cannot assert that unwritten code has shipped.

This gate is also cross-referenced in `docs/guides/TELEMETRY_RUNBOOK.md` (the Milestone-exit
regression gate section).
