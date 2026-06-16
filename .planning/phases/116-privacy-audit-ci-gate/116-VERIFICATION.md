# Phase 116: Privacy Audit + CI Gate — Verification

**Phase:** 116-privacy-audit-ci-gate
**Milestone:** v8.1.0 Desktop Telemetry
**Status:** Documentation gate (D-10)

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
