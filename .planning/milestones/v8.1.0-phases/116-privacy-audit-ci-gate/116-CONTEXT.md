# Phase 116: Privacy Audit + CI Gate - Context

**Gathered:** 2026-06-16
**Status:** Ready for planning

<domain>
## Phase Boundary

The **final v8.1.0 phase** — end-to-end validation + hardening + documentation of the
telemetry stack built in Phases 111-115. It adds **NO new producers and no new chokepoint
machinery**. It proves (via tests + the existing both-OS CI matrix + a frozen-binary self-test
on a clean machine) that no forbidden field can ever leak and that telemetry degrades silently
offline, and it writes the operational runbook.

**In scope (requirements PRIV-03, PRIV-04, INFRA-06 + ROADMAP SC#1/#2/#3):**
- **PRIV-04** forbidden-field tests — **lightweight unit/scrubber level** (D-01/D-02).
- **SC#3** frozen-binary SSL + offline self-test via a new `--telemetry-selftest` CLI flag,
  verified on a clean no-Python Windows VM (D-04/D-05/D-06).
- **INFRA-06** operational runbook (`docs/guides/TELEMETRY_RUNBOOK.md`) + amend the stale
  "isolated project" wording in REQUIREMENTS.md (D-07/D-08).
- **SC#1 / CI** — verify the existing privacy guards are green on both OSes; document the
  full-telemetry regression as a milestone-exit gate (D-09/D-10).

**Already delivered (REFERENCE, do not re-implement):** PRIV-03's AST guard
(`tests/test_telemetry_no_direct_posthog.py`) shipped early in Phase 111-03 and already runs on
Ubuntu + Windows via the `tests` CI job.

**Out of scope:** any new telemetry feature; producer-path integration harnesses (user: "nothing
heavy"); a dedicated privacy-gate CI job; ERR-01/FLAG-F1/CONSENT-F1/CRASH-F1 (all Future); the
web `search_executed` query-leak cleanup (WEB-F1, separate web follow-up).
</domain>

<decisions>
## Implementation Decisions

### PRIV-04 — Forbidden-field tests (test depth)
- **D-01 (LIGHTWEIGHT, unit/scrubber level — chosen over producer-path):** Extend the existing
  scrubber-fixture tests (`tests/test_telemetry_scrubbing.py`, `tests/test_telemetry_review_fixes.py`)
  with representative forbidden **inputs** shaped like what the crash / search / My-Library /
  composition producers would pass — Windows My-Library paths, Hebrew query strings,
  traceback-shaped strings with frame locals, exclusion-list-shaped data. Push each through
  `track()` / `track_error()` / `_scrub_props` and assert the captured `enqueue_event` payload
  contains **no** path, filename, query/search text, username, or hostname. **Rationale (user-accepted):**
  the scrubber is *structural* and runs inside `track()` on every payload, and the PRIV-03 AST
  guard already forces ALL desktop emission through the chokepoint — so unit-proof of the filter is
  sufficient. Do NOT build a Qt producer-path harness (111-115 already exercised the wiring).
- **D-02 (pre-consent zero-emit — CONSENT-01):** Add a fresh-`config.pkl` (consent unset/false)
  assertion: exercise representative `track()`/`track_performance()`/`track_error()` calls and
  assert **zero** events reach the queue. Keep it light.
- **D-03 (Claude's discretion — user: "your discretion, nothing heavy"):** exact fixtures and which
  representative scenarios to include. Cover the SC#2-named cases (crash traceback, My-Library
  search, composition) **at the input level**; reuse existing fixtures; no heavy end-to-end harness.

### SC#3 — Frozen-binary SSL + offline self-test
- **D-04 (`--telemetry-selftest` CLI flag):** Add a headless flag to `genizah_app.py`, modeled on the
  existing `--self-test-pymupdf` (`genizah_app.py:~27486`) — parsed **BEFORE QApplication
  construction** so it runs headlessly. Reuse `desktop/telemetry.run_selftest()` to attempt one real
  POST and print a machine-readable result (e.g. `SSL_OK` / `SSL_FAIL`) + a non-zero exit on failure.
  Lightweight (~30 lines). Requires a key (embedded or `GENIZAH_TELEMETRY_KEY`) + consent true for the
  probe.
- **D-05 (offline arm):** The offline-degradation check is the same flag invoked with the network
  disabled — must return **fast** (bounded by the transport's `requests.post(timeout=2.0)`), with NO
  crash, NO dialog, NO indefinite delay. Normal app startup with network off must also be silent.
  (INFRA-05 already guarantees fire-and-forget memory-only behavior; this proves it for the frozen exe.)
- **D-06 (clean-machine run = HUMAN-UAT):** Verification runs on a **clean Windows VM with NO Python
  installed** — the only condition that proves certifi/SSL is bundled into the frozen binary, not
  borrowed from a dev Python. Logged as a HUMAN-UAT item run once before milestone close. **This same
  run closes Phase 114's still-open "live PostHog event delivery" UAT.**
- *Claude's discretion:* exact flag name/output tokens, whether the offline arm uses a second flag or
  an env toggle, exit-code conventions.

### INFRA-06 — Operational runbook + stale-requirement fix
- **D-07 (resolve the contradiction — document SHARED + amend the req):** REQUIREMENTS.md INFRA-06
  currently says "the desktop PostHog project is **isolated** from the web project" — this is **stale**
  and contradicts the 2026-06-14 reversal. The runbook and the amended requirement must reflect the
  ACTUAL posture: **ONE shared web PostHog project** (id 134161, EU); web↔desktop separation is by
  `platform=desktop` + the `desktop_` event-name namespace — NOT a separate project. Edit the stale
  wording in `.planning/REQUIREMENTS.md` INFRA-06 with a dated note pointing at
  `.planning/research/POSTHOG-PROJECT-DECISION.md`.
- **D-08 (runbook location + content):** NEW `docs/guides/TELEMETRY_RUNBOOK.md`; add an entry to
  `docs/DOCUMENTATION_INDEX.md`. Required content:
  - (a) shared-project + `platform=desktop` / `desktop_`-namespace separation;
  - (b) the embedded ingest key is a **publishable, write-only** key — abuse-tolerant, NOT a secret —
    with a **rotation procedure** + the `GENIZAH_TELEMETRY_KEY` / host override knobs;
  - (c) the **two** `get_dropped_event_count()` drop counters to monitor after launch
    (`web.api_hardening` + `shared.posthog_server`) — growth in EITHER signals queue saturation
    (see the existing CLAUDE.md Phase-98 two-queue note);
  - (d) `--telemetry-selftest` usage;
  - (e) opt-out behavior (install id retained on disk, emission stops immediately, queue drained).

### CI gate structure + milestone-exit check
- **D-09 (keep guards in the existing `tests` job — no CI YAML churn):** SC#1 is **already satisfied** —
  `tests` runs `pytest tests/ -m "not gui"` on **both** ubuntu-latest and windows-latest, and the
  stdlib AST guards (`test_telemetry_no_direct_posthog.py`, `test_no_dynamic_telemetry_strings.py`,
  `test_no_raw_storage_access.py`) run there. Phase 116 **adds** the PRIV-04 scrubber tests (D-01/D-02)
  to that same suite and **verifies** the existing guards are green on both OSes. No dedicated
  privacy-gate job; do NOT re-implement the PRIV-03 guard (it shipped in 111-03).
- **D-10 (milestone-exit regression — documented gate, no new code):** Document the full
  telemetry/crash/posthog regression (~290 tests from 111-115) as an explicit milestone-EXIT gate in
  VERIFICATION.md + the runbook: must be green before v8.1.0 ships.

### Claude's Discretion
- PRIV-04 fixture/scenario selection (keep light, reuse existing).
- `--telemetry-selftest` flag name, output tokens, exit codes, and offline-arm mechanism.
- Runbook section ordering and wording.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & milestone decisions
- `.planning/REQUIREMENTS.md` — **PRIV-03** (Complete, delivered 111-03), **PRIV-04** (Pending),
  **INFRA-06** (Pending) are this phase's reqs. INFRA-06's "isolated" wording is **STALE** and MUST be
  amended (D-07). The Fixed-constraints + Out-of-Scope tables are LOCKED.
- `.planning/research/POSTHOG-PROJECT-DECISION.md` — the 2026-06-14 reversal: ONE shared web PostHog
  project + web-aligned identity. The authority the runbook + amended INFRA-06 cite.
- `.planning/ROADMAP.md` — Phase 116 SC#1 / SC#2 / SC#3 (the validation criteria this phase proves).

### Prior-phase context (the stack being validated)
- `.planning/phases/111-telemetry-foundation/111-CONTEXT.md` — chokepoint, consent gate,
  `_ALLOWED_PROPS`, `DesktopEvent` registry, structural recursive scrubber. **PRIV-03 AST guard shipped
  here (111-03).**
- `.planning/phases/113-crash-reporting/113-CONTEXT.md` — crash payload shape (exc-type / scrubbed
  module-basename / line / version / OS only), `_flush_before_exit`, `send_crash_event_direct`.
- `.planning/phases/114-usage-analytics/114-CONTEXT.md` — `search_mode` / `corpus_scope` enums, result
  buckets, value-side hardcoded-constant discipline; the **open "live delivery" UAT this phase closes**.
- `.planning/phases/115-performance-metrics/115-CONTEXT.md` — perf-summary container keys; the most
  recent allowlist additions.

### Live code the audit + self-test wire into
- `desktop/telemetry.py` — recursive structural scrubber `_scrub_props`/`_scrub_value` (`:245`),
  `_ALLOWED_PROPS` allowlist (`:292`), `DesktopEvent` enum (`:132`; `SELFTEST` at `:168`), `is_enabled()`
  consent gate, `run_selftest()` (`:836`) + the `__main__` self-test probe (`:1729`). The audit asserts
  against these; the CLI flag reuses `run_selftest()`.
- `shared/posthog_server.py` — transport `requests.post` → `https://eu.i.posthog.com/capture` (certifi
  SSL), `send_crash_event_direct` (`:366`), `_drain_and_discard`, **`get_dropped_event_count()`** (the
  drop counter the runbook monitors).
- `tests/test_telemetry_no_direct_posthog.py` — the **PRIV-03 AST guard** (REFERENCE, do not
  re-implement). Modeled on `tests/test_no_raw_storage_access.py`.
- `tests/test_no_dynamic_telemetry_strings.py` — the D-17 producer-layer dynamic-string guard (stdlib).
- `tests/test_telemetry_scrubbing.py`, `tests/test_telemetry_review_fixes.py` — the existing scrubber
  fixture tests to **EXTEND** for PRIV-04 (D-01).
- `genizah_app.py` — the `--self-test-pymupdf` block (`~:27486`, parsed BEFORE QApplication
  construction) — the **template** for `--telemetry-selftest` (D-04).
- `.github/workflows/ci.yml` — the `tests` job already runs both-OS `pytest -m "not gui"`
  (SC#1 already satisfied; D-09).
- `GenizahSearchPro.spec` — frozen build; certifi/SSL bundling that SC#3 validates. **Note:** the spec
  has no explicit certifi entry — PyInstaller's `requests` hook bundles `cacert.pem`; the clean-VM run
  (D-06) is what actually proves it.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `run_selftest()` + the `__main__` probe in `desktop/telemetry.py` — reuse for `--telemetry-selftest`.
- The `--self-test-pymupdf` headless-CLI pattern in `genizah_app.py` — the exact template (pre-Qt parse).
- The existing scrubber-fixture tests — **extend**, don't rewrite (D-01).
- `get_dropped_event_count()` in `shared/posthog_server.py` + the web `web.api_hardening` counter — both
  monitored per the runbook (D-08c).

### Established Patterns
- All emission routes through `desktop/telemetry.py` only (AST-enforced). PRIV-04 tests assert at the
  chokepoint boundary (capture the `enqueue_event` payload).
- Headless self-test flags MUST be parsed BEFORE `QApplication` construction (no event loop, no GUI).
- Two-queue drop-counter monitoring is the existing operational pattern (CLAUDE.md Phase-98 note).

### Integration Points
- `--telemetry-selftest` → `genizah_app.py __main__` (pre-Qt) → `run_selftest()` → posthog transport.
- PRIV-04 tests → monkeypatch/capture `enqueue_event` → assert forbidden-field-free + pre-consent zero.
- Runbook → `docs/guides/TELEMETRY_RUNBOOK.md` + `docs/DOCUMENTATION_INDEX.md`.

</code_context>

<specifics>
## Specific Ideas

- Hillel wants Phase 116 **lightweight** — "nothing heavy." Verification + docs, leaning on the heavy
  testing already done in 111-115 (chose scrubber-unit over producer-path; chose the existing CI job
  over a new privacy-gate job).
- The real-world posture continues: the **clean, no-Python Windows VM** run is the gold-standard SSL
  proof and is the single human gate for this phase (and doubles as the Phase-114 live-delivery UAT).
- The INFRA-06 "isolated project" wording is a known stale artifact from before the 2026-06-14
  shared-project reversal — fixing it keeps the planning docs internally consistent.

</specifics>

<deferred>
## Deferred Ideas

- **WEB-F1** — clean the pre-existing web `search_executed` event that sends `query: clean_query[:100]`
  (a query-text privacy gap surfaced during research). A **web** follow-up, NOT blocking this desktop
  milestone — noted here so it isn't lost.
- **ERR-01** — handled/non-fatal error counting at high-value sites. Future (out of v8.1.0 by user
  decision; v8.1.0 = hard crashes only).
- **CONSENT-F1** — "reset telemetry id" affordance in Settings. Future.
- **CRASH-F1** — "send logs" native-crash upload flow. Future.
- **FLAG-F1** — PostHog feature flags / remote config on desktop. Future.

None of the above are gaps in Phase 116 — they are explicitly Future/parity items.

</deferred>

---

*Phase: 116-privacy-audit-ci-gate*
*Context gathered: 2026-06-16*
