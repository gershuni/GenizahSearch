---
phase: 116
slug: privacy-audit-ci-gate
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-16
---

# Phase 116 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `116-RESEARCH.md` § Validation Architecture. Task IDs below are
> assigned by the planner — rows are keyed by requirement/behavior until then.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (CI-pinned, unlocked via `pip install pytest`) |
| **Config file** | none (no pytest.ini / pyproject.toml test config) |
| **Quick run command** | `pytest tests/test_telemetry_priv04.py -x` |
| **Full suite command** | `pytest tests/test_telemetry*.py tests/test_no_direct*.py tests/test_no_dynamic*.py -m "not gui"` |
| **Estimated runtime** | ~5–10s (scrubber tests are pure-function; no Qt, no network) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_telemetry_priv04.py -x`
- **After every plan wave:** Run the full telemetry suite (above)
- **Before `/gsd:verify-work`:** `pytest tests/ -m "not gui"` green on **both** Ubuntu and Windows via CI (SC#1)
- **Max feedback latency:** < 15 seconds (quick run)

---

## Per-Task Verification Map

> Plan/Wave/Task-ID columns filled by the planner. Behavior + automated command
> are the locked validation signals from research.

| Behavior | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|----------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| Windows My-Library `path` key dropped from payload | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | scrubber drops banned key | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_my_library_path_not_in_payload -x` | ❌ W0 | ⬜ pending |
| `filename` key dropped | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | scrubber drops banned key | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_filename_key_dropped -x` | ❌ W0 | ⬜ pending |
| Hebrew query string scrubbed to `[REDACTED]` | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | value-side scrub | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_hebrew_query_value_redacted -x` | ❌ W0 | ⬜ pending |
| Crash `frame_locals` + `traceback_raw` both dropped | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | scrubber drops banned keys | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_crash_forbidden_fields_dropped -x` | ❌ W0 | ⬜ pending |
| `hostname` + `username` keys dropped | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | scrubber drops banned keys | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_hostname_username_dropped -x` | ❌ W0 | ⬜ pending |
| Pre-consent: `track`/`track_performance`/`track_error` → zero events | PRIV-04 (D-02), CONSENT-01 | T-PRE-CONSENT-EMIT | consent gate in every entry point | unit/consent-gate | `pytest tests/test_telemetry_priv04.py::test_priv04_pre_consent_zero_emit_all_entry_points -x` | ❌ W0 | ⬜ pending |
| AST guard green on both Ubuntu + Windows | PRIV-03 (REFERENCE) | T-CHOKEPOINT-BYPASS | no direct `enqueue_event` outside chokepoint | static AST | `pytest tests/test_telemetry_no_direct_posthog.py` | ✅ exists | ⬜ verify |
| `--telemetry-selftest` prints `SSL_OK` on clean Windows VM | SC#3 / D-04 | T-SSL-SPOOF | certifi bundled + functional | HUMAN-UAT | manual: `GenizahSearchPro.exe --telemetry-selftest` (clean no-Python VM) | ❌ W0 (code) | ⬜ pending |
| `--telemetry-selftest-offline` returns fast, no crash/dialog | SC#3 / D-05 | — | fire-and-forget offline | smoke/manual | `GenizahSearchPro.exe --telemetry-selftest-offline` → `OFFLINE_OK` | ❌ W0 (code) | ⬜ pending |
| Runbook `docs/guides/TELEMETRY_RUNBOOK.md` has required sections | INFRA-06 / D-08 | T-KEY-EXPOSURE | write-only key + rotation documented | documentation | human review of file (5 sections a–e) | ❌ W0 | ⬜ pending |
| REQUIREMENTS.md INFRA-06 "isolated" wording amended | INFRA-06 / D-07 | — | docs internally consistent | documentation | `grep -n "isolated" .planning/REQUIREMENTS.md` → no stale claim in INFRA-06 | ❌ W0 | ⬜ pending |
| ~290-test telemetry regression documented as milestone-exit gate | D-10 | — | exit gate green before ship | documentation | human review of VERIFICATION.md section | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_telemetry_priv04.py` — new file; 6 forbidden-field tests (D-01) + 1 pre-consent zero-emit test covering all three entry points (D-02). Autouse fixture pattern copied from `tests/test_telemetry_review_fixes.py` (monkeypatch `ph._event_queue` + `fake_config`).
- [ ] `genizah_app.py` `__main__` — `--telemetry-selftest` / `--telemetry-selftest-offline` block (~30 lines), parsed BEFORE `QApplication`, modeled on `--self-test-pymupdf` (`genizah_app.py:~27489`); reuses `desktop/telemetry.run_selftest()`.

*Existing infrastructure needs NO changes: 234 telemetry tests from Phases 111–115 already pass; CI `tests` job already runs both OSes; PRIV-03 AST guard already green.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| certifi/`cacert.pem` bundled in the FROZEN binary (not borrowed from dev Python) | SC#3 / D-06 | A dev machine's Python certs could mask a missing bundle; only a clean no-Python Windows VM proves the cert ships inside the exe | On a clean Windows VM with NO Python installed, run `GenizahSearchPro.exe --telemetry-selftest` → expect `SSL_OK` + exit 0 |
| Real PostHog event delivery end-to-end (closes Phase 114 open UAT) | SC#3 / D-06 | Requires a live network + the desktop PostHog project; cannot be asserted in CI | After the selftest above, confirm the `desktop_selftest` event appears in PostHog project 134161 (EU) |
| Offline degradation: app starts, no dialog, no delay, no crash with network disabled | SC#3 / D-05 | Requires toggling the host network adapter on the frozen exe | Disable network, launch `GenizahSearchPro.exe` normally → app usable + silent; then `--telemetry-selftest-offline` → `OFFLINE_OK` fast |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies (HUMAN-UAT rows in Manual-Only are exempt)
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (`test_telemetry_priv04.py`, selftest CLI block)
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter (after planner assigns task IDs)

**Approval:** pending
