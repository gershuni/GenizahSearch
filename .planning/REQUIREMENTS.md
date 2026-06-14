# Requirements: GenizahSearch — v8.1.0 Desktop Telemetry

**Defined:** 2026-06-14
**Core Value:** Researchers can find what they need in the Genizah corpus
**Milestone goal:** Add opt-in, privacy-preserving telemetry to the desktop app ("Dicta Genizah Search Pro") so real-world usage, version adoption, performance, crashes, and **cross-surface (web↔desktop) per-user journeys** become visible in PostHog — without ever transmitting My Library data or search content. Desktop telemetry feeds the **existing shared web PostHog project** and is identity-aligned with the web app; **no web code change is required** (the web already identifies logged-in users).

**Fixed constraints (user decisions — design within them):**
- Opt-in only; default **OFF** until the user consents.
- First-run consent dialog shown **on first launch after updating** (existing v8.0.0 users + fresh installs) + a Settings/About toggle.
- **Identity-aligned with web:** logged-out users → anonymous per-install `uuid4` (opt-out **keeps the id**, stops emission); logged-in + consented users → `identify(distinct_id = Supabase user.id)` exactly matching `web/auth_state.py:160-170`, aliasing the per-install anon id on login, resetting to anonymous on logout. Desktop sends **only the user id** for identity (never email/name — web already attaches those); desktop never sends PII beyond that id.
- HARD RULE: never transmit My Library data (paths/filenames/content) or any search/query content.
- Reuse `shared/posthog_server.py` (fire-and-forget queue → EU PostHog), incl. hand-rolled `$identify`/alias events; **do NOT add the `posthog` Python SDK**.
- Events go to the **existing shared web PostHog project** (id 134161); web↔desktop separation is by a `platform=desktop` base prop + a `desktop_` event-name namespace (NOT a separate project). Reuse the web publishable key.
- v8.1.0 captures **hard crashes only** (uncaught exceptions); handled/non-fatal error counting is deferred.

---

## v8.1.0 Requirements

### Consent & Identity (CONSENT)

- [ ] **CONSENT-01**: Telemetry is OFF by default — **no event of any kind (session/usage/perf/crash) is enqueued before consent state has been loaded and is true.** Startup order guarantees the consent gate is resolved before any producer can fire (proven by test, see PRIV-04).
- [ ] **CONSENT-02**: On first launch after updating to v8.1.0 (and on fresh installs), the user sees a bilingual (EN/HE) first-run consent dialog with an explicit, equal-weight yes/no choice (nothing pre-selected) that plainly states what is and is not collected.
- [ ] **CONSENT-03**: The consent dialog is shown at most once — the choice and a "prompt shown" flag persist so it never re-prompts on later launches. The stored consent record also captures the consent timestamp, the app version, and the consent-UI version (lightweight audit trail).
- [ ] **CONSENT-04**: User can turn telemetry on or off at any time from a Settings/About toggle that reads and writes the same consent source of truth as the first-run dialog.
- [ ] **CONSENT-05**: An anonymous per-install identifier (uuid4) is minted on opt-in and used as the PostHog `distinct_id` for **logged-out** users; it is never derived from hardware/MAC. For **logged-in** users the `distinct_id` is the Supabase `user.id` (see IDENT-01). No hardware fingerprinting either way.
- [ ] **CONSENT-06**: Opting out stops all event emission immediately; the per-install ID is **retained** on disk (not deleted) so re-opt-in preserves continuity.
- [ ] **CONSENT-07**: Consent + identity state persists in the existing config store (`config.pkl` via `load_app_config`/`save_app_config`) — no new settings file, no `QSettings`.
- [ ] **CONSENT-08**: Opting out not only stops new producers but also **drains/discards any already-queued, un-sent events** so nothing buffered before opt-out is transmitted afterward.

### Usage Analytics (USAGE)

- [ ] **USAGE-01**: A session-start event records ONLY allowlisted environment properties — app version, OS family + version, Python/PyQt version, UI language (EN/HE). Explicitly **never** hostname/machine name, username, executable path, or working directory.
- [ ] **USAGE-02**: Feature usage is captured as counts — which tab/view and which key surfaces (Joins Lab, Fragment Puzzle, major dialogs) are opened — with no free-text or content properties.
- [ ] **USAGE-03**: Search executions are captured with the search MODE (keyword/Responsa/composition/parallels) and corpus (Genizah/Local/ALL) as enums — never the query text or any My Library path/filename.
- [ ] **USAGE-04**: An active-user/session signal is emitted so DAU/MAU and version adoption are derivable in PostHog.
- [ ] **USAGE-05**: Every event carries base properties (`platform=desktop`, `app_version`) and a `desktop_` event-name namespace, applied through one shared helper so no callsite bypasses them. `$process_person_profile=false` is set for **anonymous (logged-out)** events; **identified** events use real person profiles (see IDENT-03).
- [ ] **USAGE-06**: Session/clock correctness — exactly one telemetry session id per process; all timestamps UTC; performance durations measured from a monotonic clock; a crash-restart starts a fresh session without emitting a duplicate/ghost session-start for the crashed process.

### Identity & Cross-Surface Journey (IDENT)

- [ ] **IDENT-01**: A logged-in, consented desktop user is identified to PostHog with `distinct_id = Supabase user.id` — the **exact same value** the web app uses (`web/auth_state.py:160-170`) — so the same researcher's web and desktop activity merge into one person in the shared project. (A hash/derivation would NOT merge — it must be the raw id.)
- [ ] **IDENT-02**: A logged-out user's anonymous per-install events are **aliased** to their account on login via `$identify` with `$anon_distinct_id = <per-install uuid>` (no pre-login history orphaned); on logout the desktop **resets to the anonymous per-install id** (mirrors web `posthog.reset()`).
- [ ] **IDENT-03**: Desktop sends **only the user id** on identify — never email/name or other profile PII (web already attaches those to the shared person). Anonymous events stay `$process_person_profile=false`; identified events use real person profiles.
- [ ] **IDENT-04**: `$identify`/alias/reset are emitted through the **same desktop chokepoint + raw `shared/posthog_server.py` queue** (hand-rolled events, no SDK) and are consent-gated exactly like all other emission (nothing fires before consent).

### Performance Metrics (PERF)

- [ ] **PERF-01**: Search and indexing durations are measured (e.g. a perf signal on the search/composition worker threads) without capturing any query text.
- [ ] **PERF-02**: Result counts are reported as bounded buckets, not raw content-tied values.
- [ ] **PERF-03**: Performance data is aggregated into a per-session summary (e.g. median/p95 + counts) flushed once at session end plus a periodic flush — not one event per search — and the sampling/aggregation parameters are configurable (env/config) so volume can be tuned without a code change. (Target: ~tens/day, not ~50/day × dozens of users.)

### Crash Reporting (CRASH)

- [ ] **CRASH-01**: Uncaught main-thread exceptions are captured via `sys.excepthook`, **chaining to (never replacing)** the existing `_setup_crash_handler` (`genizah_app.py:148-170`) so `crash_log.txt` keeps working. The telemetry step runs in a `try/finally` such that a telemetry failure can never suppress the existing crash-log handler.
- [ ] **CRASH-02**: Uncaught worker/QThread exceptions are captured via `threading.excepthook` (currently uninstalled) and Qt-slot exceptions via a `QApplication.notify` override.
- [ ] **CRASH-03**: Native crashes (C extensions — Tantivy/PyMuPDF) are captured to a local log via `faulthandler` — local file only, not transmitted.
- [ ] **CRASH-04**: Crash events contain only the exception type name, a scrubbed/sanitized stack location, app version, and OS — never frame locals, exception-message strings, file paths, filenames, or query text.
- [ ] **CRASH-05**: The exception hooks are non-blocking (enqueue only, no network I/O in the hook), re-entrancy-safe, and respect the consent gate using a **cached** consent value (no disk read / settings init inside the hook) so the gate itself cannot throw during crash handling.
- [ ] **CRASH-06**: The final crash event is delivered via a **bounded synchronous flush** before process exit (and gets priority over a full queue) so the fire-and-forget daemon-thread queue does not silently drop the one event the milestone most needs.
- [ ] **CRASH-07**: A native/hard crash that cannot emit at crash time (segfault from a C extension caught only by `faulthandler`'s local log) is detected on the **next launch** and emitted once (after consent), so native crashes are not invisible.

### Privacy Guardrails (PRIV)

- [ ] **PRIV-01**: A single structural scrubber sanitizes every outgoing payload (drops banned keys, redacts path-like strings, strips frame locals, caps lengths) so the no-content/no-PII rule holds structurally — not by per-call discipline.
- [ ] **PRIV-02**: A telemetry property allowlist constrains which properties may be sent; anything not on the allowlist is dropped. The allowlist explicitly excludes environment identifiers (hostname/machine name, username, executable path, cwd) and any property derived from visible UI strings (window/tab titles, `QAction` text, recent-file labels) or file dialogs.
- [ ] **PRIV-03**: A static AST CI guard (modeled on `tests/test_no_raw_storage_access.py`) enforces that `shared/posthog_server.enqueue_event` is reached **only** through the desktop telemetry chokepoint, so no call site can bypass the consent gate or scrubber.
- [ ] **PRIV-04**: Automated tests assert that representative crash tracebacks and search / My-Library scenarios never emit any **forbidden field** — explicitly: My Library paths, filenames, query/search text, usernames, or hostnames — and that nothing is emitted before consent (CONSENT-01).
- [ ] **PRIV-05**: The Help/About privacy disclosure is updated bilingually (EN/HE) to describe exactly what telemetry collects, that it is opt-in, how to turn it off, and that the anonymous install id is a pseudonymous identifier — consistent with the existing disclosure posture.
- [ ] **PRIV-06**: Event NAMES are drawn from a fixed registry/enum — no event name (and no property) is ever derived from query text, filenames, corpus/document labels, or visible UI strings. (Dynamic event names leak even when properties are scrubbed.)

### Backend & Packaging (INFRA)

- [ ] **INFRA-01**: Desktop events go to the **existing shared web PostHog project** (id 134161, EU), reusing the web app's publishable (write-only) ingest key embedded in the desktop binary (overridable via `GENIZAH_TELEMETRY_KEY`/host). NO separate project. Web↔desktop separation is by `platform=desktop` + the `desktop_` event-name namespace (USAGE-05).
- [ ] **INFRA-02**: A desktop telemetry chokepoint module (`desktop/telemetry.py`) exposes the only public API for emitting events (track / track_performance / consent / install-hooks), each internally consent-gated and scrubbed, delegating to `shared/posthog_server.enqueue_event`.
- [ ] **INFRA-03**: `shared/posthog_server.py` gains backward-compatible additions only (consent-gate hook, `distinct_id` injection, flush-before-exit) without breaking its existing web/breaker consumers or the 5 test monkeypatches.
- [ ] **INFRA-04**: Telemetry adds **zero** new pip dependencies and requires no PyInstaller spec changes beyond what's already bundled (reuse the raw queue; do NOT add the `posthog` SDK).
- [ ] **INFRA-05**: Telemetry degrades silently and never blocks the UI thread when offline/air-gapped or when the key is absent (fire-and-forget; SSL certs verified in the frozen binary). Events are **memory-only** — never spooled to disk.
- [ ] **INFRA-06**: Operational runbook — the desktop PostHog project is isolated from the web project; the embedded ingest key is documented as write-only (treated as abuse-tolerant with a rotation procedure, not a secret); and both `get_dropped_event_count()` drop counters (`web.api_hardening` + `shared.posthog_server`) are monitored after launch.

---

## Future Requirements (deferred)

### Consent Follow-ups (CONSENT-F)

- **CONSENT-F1**: A "reset telemetry id" affordance in Settings (mints a fresh uuid4 on demand). Deferred — the user chose to keep the id on opt-out; this is a privacy nicety, not required for v8.1.0.

### Handled / Non-Fatal Errors (ERR)

- **ERR-01**: Curated non-fatal error counting at high-value sites (PDF/format extraction failures, NLI/IIIF fetch failures, search/index errors). Deferred per user decision — v8.1.0 captures hard crashes only.

### Crash Follow-ups (CRASH-F)

- **CRASH-F1**: "Send logs" flow that lets a consenting user upload the local `faulthandler` native-crash log (v8.1.0 keeps native-crash output local only).

### Web Parity (WEB)

- **WEB-F1**: Clean the pre-existing web `search_executed` event that currently sends `query: clean_query[:100]` — a query-text privacy gap surfaced during research. Not blocking the desktop milestone; tracked as a web follow-up.

### Remote Config (FLAG)

- **FLAG-F1**: PostHog feature flags / remote config / A-B testing on the desktop (infrastructure becomes available once the desktop project exists).

---

## Out of Scope

| Feature | Reason |
|---------|--------|
| Adding the `posthog` Python SDK | Its `capture_exception_code_variables` ships frame-local values to PostHog **before** `before_send` scrubbing runs — a direct PII-leak risk for a search/My-Library app. The existing raw queue is sufficient and adds zero deps. |
| Transmitting any My Library data or search/query content | Hard privacy invariant — structurally forbidden, not a feature to scope. |
| Session replay / screen capture / autocapture | Inappropriate for a privacy-first scholarly desktop tool. |
| Sending email / name / profile PII from desktop | Desktop identifies logged-in users by the bare Supabase `user.id` only (IDENT-03); the web already attaches email/name to the shared person. Desktop adds no profile PII. |
| Web-side telemetry code changes | None needed — the web already identifies logged-in users by `user.id` (`web/auth_state.py`), which is exactly what desktop aligns to. (The `search_executed` query-text cleanup is the optional WEB-F1 follow-up.) |
| A separate desktop PostHog project | Reversed 2026-06-14 — desktop uses the shared web project so cross-surface journeys work and there's no org/billing friction (see `research/POSTHOG-PROJECT-DECISION.md`). |
| Hardware/MAC-derived or registry-based install IDs | PII / fingerprinting risk; uuid4 in the config store is sufficient. |
| Third-party ad / marketing SDKs | Anti-feature for this audience. |

---

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| CONSENT-01 | Phase 111 | Pending |
| CONSENT-02 | Phase 112 | Pending |
| CONSENT-03 | Phase 112 | Pending |
| CONSENT-04 | Phase 112 | Pending |
| CONSENT-05 | Phase 111 | Pending |
| CONSENT-06 | Phase 111 | Pending |
| CONSENT-07 | Phase 111 | Pending |
| CONSENT-08 | Phase 112 | Pending |
| USAGE-01 | Phase 114 | Pending |
| USAGE-02 | Phase 114 | Pending |
| USAGE-03 | Phase 114 | Pending |
| USAGE-04 | Phase 114 | Pending |
| USAGE-05 | Phase 114 | Pending |
| USAGE-06 | Phase 114 | Pending |
| IDENT-01 | Phase 114 | Pending |
| IDENT-02 | Phase 114 | Pending |
| IDENT-03 | Phase 111 | Pending |
| IDENT-04 | Phase 111 | Pending |
| PERF-01 | Phase 115 | Pending |
| PERF-02 | Phase 115 | Pending |
| PERF-03 | Phase 115 | Pending |
| CRASH-01 | Phase 113 | Pending |
| CRASH-02 | Phase 113 | Pending |
| CRASH-03 | Phase 113 | Pending |
| CRASH-04 | Phase 113 | Pending |
| CRASH-05 | Phase 113 | Pending |
| CRASH-06 | Phase 113 | Pending |
| CRASH-07 | Phase 113 | Pending |
| PRIV-01 | Phase 111 | Pending |
| PRIV-02 | Phase 111 | Pending |
| PRIV-03 | Phase 116 | Pending |
| PRIV-04 | Phase 116 | Pending |
| PRIV-05 | Phase 112 | Pending |
| PRIV-06 | Phase 111 | Pending |
| INFRA-01 | Phase 111 | Pending |
| INFRA-02 | Phase 111 | Pending |
| INFRA-03 | Phase 111 | Pending |
| INFRA-04 | Phase 111 | Pending |
| INFRA-05 | Phase 111 | Pending |
| INFRA-06 | Phase 116 | Pending |

**Coverage:**
- v8.1.0 requirements: 40 total
- Mapped to phases: 40/40 ✓
- Unmapped: 0 ✓

---
*Requirements defined: 2026-06-14*
*Last updated: 2026-06-14 — REVISED during Phase 111 discussion: reversed to ONE shared web PostHog project + web-aligned identity (new IDENT category, 4 reqs → 40 total); CONSENT-05 / USAGE-05 / INFRA-01 amended; Out-of-Scope updated. See `research/POSTHOG-PROJECT-DECISION.md`.*
