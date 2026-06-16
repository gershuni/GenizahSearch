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
| Hebrew `context` value collapsed to `unregistered` (NOT `[REDACTED]` — context goes through `_safe_context`) | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | `_safe_context` collapse | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_hebrew_query_context_unregistered -x` | ❌ W0 | ⬜ pending |
| Hebrew value on a non-context allowed path scrubbed to `[REDACTED]` (`_scrub_value`) | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | value-side scrub | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_hebrew_value_redacted_on_scrub_path -x` | ❌ W0 | ⬜ pending |
| Crash `frame_locals` + `traceback_raw` both dropped | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | scrubber drops banned keys | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_crash_forbidden_fields_dropped -x` | ❌ W0 | ⬜ pending |
| `hostname` + `username` keys dropped | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | scrubber drops banned keys | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_hostname_username_dropped -x` | ❌ W0 | ⬜ pending |
| No raw forbidden needle (path/filename/Hebrew/username/hostname) anywhere in serialized payload | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | `json.dumps(...)` value-absence | unit/scrubber | covered by the assertions inside the rows above (each asserts `needle not in json.dumps(payload, ensure_ascii=False)`) | ❌ W0 | ⬜ pending |
| Filename-shaped `context` (`manuscript_notes.docx`) cannot ride through `_safe_context` (→ `unregistered`) | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | allowed-key leak closed | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_filename_shaped_context_not_leaked -x` | ❌ W0 | ⬜ pending |
| `track_error()` path/query-shaped context + path-bearing exception message leak neither into the payload | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | context collapsed + message never added | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_track_error_path_context_and_message_not_leaked -x` | ❌ W0 | ⬜ pending |
| `_safe_context` hardened: filename-extension shape → `unregistered`, dotted code preserved | PRIV-04 (D-01) | T-FORBIDDEN-ESCAPE | one-function scrubber hardening | unit/scrubber | `python -c "import desktop.telemetry as t; assert t._safe_context('manuscript_notes.docx')=='unregistered'"` | ❌ W0 | ⬜ pending |
| Pre-consent: `track`/`track_performance`/`track_error` → zero events | PRIV-04 (D-02), CONSENT-01 | T-PRE-CONSENT-EMIT | consent gate in every entry point | unit/consent-gate | `pytest tests/test_telemetry_priv04.py::test_priv04_pre_consent_zero_emit_all_entry_points -x` | ❌ W0 | ⬜ pending |
| AST guard green on both Ubuntu + Windows | PRIV-03 (REFERENCE) | T-CHOKEPOINT-BYPASS | no direct `enqueue_event` outside chokepoint | static AST | `pytest tests/test_telemetry_no_direct_posthog.py` | ✅ exists | ⬜ verify |
| `send_selftest_event_sync()` returns `SSL_OK`/`SSL_FAIL`/`NO_KEY` (synchronous HTTP-2xx delivery proof, NOT the drop counter) | SC#3 / D-04 (REVIEWS HIGH #1) | T-116-10 | one synchronous POST, status-checked | unit + HUMAN-UAT | `python -c "import shared.posthog_server as ph; assert 'send_selftest_event_sync' in ph.__all__"` + manual clean-VM run | ❌ W0 (code) | ⬜ pending |
| `--telemetry-selftest` prints `SSL_OK` (exit 0) / `NO_KEY` (exit 2) / `SSL_FAIL` (exit 1) on clean Windows VM | SC#3 / D-04 | T-SSL-SPOOF | certifi bundled + functional, driven by sync helper | HUMAN-UAT | manual: `GenizahSearchPro.exe --telemetry-selftest` (clean no-Python VM) | ❌ W0 (code) | ⬜ pending |
| `--telemetry-selftest-offline` returns fast (no network call), no crash/dialog — OFFLINE_OK is a smoke token (real offline proof = network-disabled normal launch) | SC#3 / D-05 | — | fire-and-forget offline | smoke/manual | `GenizahSearchPro.exe --telemetry-selftest-offline` → `OFFLINE_OK` fast | ❌ W0 (code) | ⬜ pending |
| Runbook `docs/guides/TELEMETRY_RUNBOOK.md` has required sections | INFRA-06 / D-08 | T-KEY-EXPOSURE | write-only key + rotation documented | documentation | human review of file (5 sections a–e) | ❌ W0 | ⬜ pending |
| REQUIREMENTS.md INFRA-06 "isolated" wording amended | INFRA-06 / D-07 | — | docs internally consistent | documentation | `grep -n "isolated" .planning/REQUIREMENTS.md` → no stale claim in INFRA-06 | ❌ W0 | ⬜ pending |
| ~290-test telemetry regression documented as milestone-exit gate | D-10 | — | exit gate green before ship | documentation | human review of VERIFICATION.md section | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `desktop/telemetry.py` — one-function hardening of `_safe_context` so filename-extension-shaped contexts collapse to `unregistered` (PRIV-04 allowed-key leak close; legitimate dotted codes preserved).
- [ ] `tests/test_telemetry_priv04.py` — new file; 8 forbidden-field/value tests (D-01, incl. serialized-payload `json.dumps` needle-absence, Hebrew-context→`unregistered`, `_scrub_value`→`[REDACTED]`, filename-shaped-context, and track_error path/message) + 1 pre-consent zero-emit test covering all three entry points (D-02). Autouse fixture copied from `tests/test_telemetry_review_fixes.py` (monkeypatch `ph._event_queue` + `fake_config`).
- [ ] `shared/posthog_server.py` — new `send_selftest_event_sync()` (synchronous, return-valued: one `requests.post`, returns `SSL_OK`/`SSL_FAIL`/`NO_KEY`; the actual SC#3 delivery proof, NOT the queue-saturation drop counter). Sibling to `send_crash_event_direct`.
- [ ] `genizah_app.py` `__main__` — `--telemetry-selftest` / `--telemetry-selftest-offline` block (~35 lines), parsed BEFORE `QApplication`, modeled on `--self-test-pymupdf` (`genizah_app.py:~27489`); online arm calls `send_selftest_event_sync()` (SSL_OK/SSL_FAIL/NO_KEY); offline arm prints `OFFLINE_OK` with NO network call.

*Existing infrastructure needs NO changes: 234 telemetry tests from Phases 111–115 already pass; CI `tests` job already runs both OSes; PRIV-03 AST guard already green.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| certifi/`cacert.pem` bundled in the FROZEN binary (not borrowed from dev Python) | SC#3 / D-06 | A dev machine's Python certs could mask a missing bundle; only a clean no-Python Windows VM proves the cert ships inside the exe | On a clean Windows VM with NO Python installed, run `GenizahSearchPro.exe --telemetry-selftest` → expect `SSL_OK` + exit 0 |
| Real PostHog event delivery end-to-end (closes Phase 114 open UAT) | SC#3 / D-06 | Requires a live network + the desktop PostHog project; cannot be asserted in CI. NOTE: `SSL_OK` is now a real HTTP-2xx confirmation from `send_selftest_event_sync()` (not a drop-counter inference), so it directly evidences delivery | After the selftest above, confirm the `desktop_selftest` event appears in PostHog project 134161 (EU) |
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
