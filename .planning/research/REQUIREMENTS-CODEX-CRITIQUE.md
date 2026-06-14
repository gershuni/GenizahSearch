# Codex critique — v8.1.0 Desktop Telemetry requirements (gap analysis)

**Reviewer:** Codex CLI `gpt-5.5` (xhigh), 2026-06-14. Brief: `_tmp/codex-telemetry-reqs-brief.md`. Raw transcript: `_tmp/codex-telemetry-reqs-output.md`.

**Caveat:** A Windows sandbox spawn error blocked Codex from reading the live files, so it reviewed from the brief alone. Several "missing" items it raised were in fact already covered in `REQUIREMENTS.md` (property allowlist PRIV-02, crash flush CRASH-06, separate desktop project INFRA-01, structural scrubber PRIV-01, content-leak tests PRIV-04). The dispositions below separate genuinely-new gaps from already-covered/strengthened items.

## Dispositions

| # | Codex finding | Sev | Disposition |
|---|---------------|-----|-------------|
| 1 | No-emission-before-consent invariant (+ test) | HIGH | **Strengthen CONSENT-01** — add explicit "no event of any kind enqueued before consent loaded AND true," tested in PRIV-04. |
| 2 | Enum-only / registered event NAMES (dynamic names leak) | HIGH | **NEW PRIV-06** — event names from a fixed registry; never derived from content/UI strings. |
| 3 | Opt-out queue semantics (discard already-queued events) | HIGH | **NEW CONSENT-08** — opt-out drains queued un-sent events, not just stops producers. |
| 4 | Forbid deriving props from UI strings (titles/QAction/recent files) + env allowlist excludes hostname/username/exe path/cwd | HIGH | **Strengthen PRIV-02 + USAGE-01** — env props limited to OS family/version, app/Python/PyQt version, UI lang; forbidden-source list added. |
| 5 | Session/clock correctness — one session id per process, UTC ts, monotonic perf durations, crash-restart dup behavior | MED | **NEW USAGE-06**. |
| 6 | Native crashes invisible at crash time → next-launch "prior crash detected" emission | HIGH | **NEW CRASH-07** — faulthandler local marker re-emitted once on next launch after consent. |
| 7 | excepthook try/finally so telemetry never suppresses crash_log.txt; consent value cached/no-throw/no-disk-read in hook | HIGH | **Strengthen CRASH-01 + CRASH-05**. |
| 8 | Crash flush: bounded synchronous send + crash-event priority over a full queue | HIGH | **Strengthen CRASH-06**. |
| 9 | Consent audit record (consent version + timestamp + app version) | MED | **Strengthen CONSENT-03**. |
| 10 | Events memory-only — no disk spool | MED | **Strengthen INFRA-05**. |
| 11 | Operational: project isolation + embedded-key rotation procedure + monitor both drop counters | MED | **NEW INFRA-06**. |
| 12 | distinct_id is pseudonymous personal data; disclose; optional "reset telemetry id" | MED | Disclosure folded into **PRIV-05**; "reset id" affordance → **Future (CONSENT-F1)** (user chose keep-id-on-opt-out). |
| 13 | Replace broad "no PII" with testable forbidden-field fixtures | MED | **Strengthen PRIV-04** — name forbidden fields (My Library paths, filenames, query text, usernames, hostnames). |

## Phasing (Codex, matches research Option A — 6 phases)

Foundation first (scrubber + consent gate + event/property allowlist + no-op default + tests proving default-off & no pre-consent emission) → consent UX → crash hooks (need finalized no-throw gate) → usage events → perf summaries (timers/aggregation/flush last) → privacy audit + CI gate **before the first real event is enabled**.

## Verdict

Not ready to roadmap as-is → **addressed**: 5 new requirements (PRIV-06, CONSENT-08, USAGE-06, CRASH-07, INFRA-06) + 1 Future (CONSENT-F1) added, and 8 existing requirements strengthened. Re-presented to user for approval before roadmapping.
