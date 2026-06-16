# GenizahSearch Desktop Telemetry Runbook

> Last updated: 2026-06-16
> For: Developers, Release Engineers

---

## (a) Project and namespace separation

Desktop telemetry events go to the **existing shared web PostHog project** (id **134161**, EU region,
endpoint `eu.i.posthog.com`). There is **no separate desktop project** — this posture was
reversed on 2026-06-14 after discovering the web app already identifies logged-in users by
`user.id`, enabling real cross-surface (web↔desktop) journey tracking with zero web-side changes.
See `.planning/research/POSTHOG-PROJECT-DECISION.md` for the full reversal rationale.

**How desktop events are separated from web events inside the shared project:**

1. **`platform=desktop` super-property** — injected on every event by `_desktop_default_props_hook`
   (registered via `register_scrub_hook` in `desktop/telemetry.py`). Web events carry `platform=web`.
   Filter any PostHog insight by `platform = desktop` to see desktop-only data.
2. **`desktop_` event-name namespace** — all desktop events are registered in the `DesktopEvent` enum
   and are prefixed `desktop_` (e.g. `desktop_session_start`, `desktop_search_executed`). Web events use
   different names with no such prefix.
3. **`$process_person_profile=False`** — injected alongside `platform=desktop` by the same hook while
   consent is on and the user is logged out (anonymous events). Identified events (logged-in users) use
   real person profiles so web↔desktop journeys merge correctly in PostHog.

The hook is installed once by `desktop/telemetry.py` when consent is granted and covers all events
routed through `shared/posthog_server.enqueue_event`, including the NLI circuit-breaker events
(`nli_breaker_opened` / `nli_breaker_closed`) that the shared queue also carries.

---

## (b) Embedded ingest key posture, rotation, and override knobs

The embedded `_TELEMETRY_KEY_DEFAULT` constant in `desktop/telemetry.py` is a **publishable,
write-only** PostHog ingest key (begins with `phc_`). It is **not a secret**:

- PostHog publishable keys are intentionally public — the same key is already embedded in the web
  app's client-side JavaScript served to every browser that loads the site.
- Write-only means it can only push events in; it cannot read any data out of the project.
- PostHog's architecture treats embedded publishable keys as **abuse-tolerant**: volume spikes are
  rate-limited at the server and cannot read or modify project data. Rotation is a routine hygiene
  step, not an incident response.
- **NEVER embed a personal API key (`POSTHOG_PERSONAL_API_KEY`, prefix `phx_`)** — those have
  read/write access to the project and ARE secrets. The code explicitly rejects `phx_` keys.

**Key resolution order** (from `_wire_transport_config` in `desktop/telemetry.py`):

1. `GENIZAH_TELEMETRY_KEY` env var — accepted in all builds (frozen .exe and source). This is the
   override knob for CI, staging, and key rotation without a rebuild.
2. `POSTHOG_API_KEY` env var — accepted in source/dev builds only. A frozen `.exe` ignores this env
   var (PyInstaller strips the source path, so the existing web key env var does not accidentally
   redirect desktop events in production).
3. Embedded `_TELEMETRY_KEY_DEFAULT` — baked into the binary at build time. Stays as the sentinel
   `_UNFILLED_KEY_SENTINEL` until explicitly set, causing local dev builds to drop events silently
   (no network call, no error).

The **host override knob** follows the same resolution order — set `GENIZAH_TELEMETRY_HOST` (or the
corresponding build constant) to redirect events to a different PostHog endpoint (e.g. a staging
project). Default is `eu.i.posthog.com`.

**Key rotation procedure:**

1. Log into PostHog project 134161 → Project Settings → Project API Keys.
2. Mint a new publishable (write-only) key. Record the new `phc_` value.
3. **Option A (env var — no rebuild required):** Set `GENIZAH_TELEMETRY_KEY=phc_<new>` in the build
   environment or deployment config. Events start using the new key immediately. Verify with
   `--telemetry-selftest` (see section d).
4. **Option B (bake into binary):** Update `_TELEMETRY_KEY_DEFAULT` in `desktop/telemetry.py`,
   rebuild and ship the new binary.
5. After the new key is confirmed working (via `--telemetry-selftest SSL_OK`), revoke the old key in
   PostHog Project Settings.

---

## (c) Two drop counters — queue saturation ONLY

After every desktop launch (and periodically in long sessions), monitor **both** drop counters:

```
shared.posthog_server.get_dropped_event_count()
web.api_hardening.get_dropped_event_count()
```

Growth in EITHER counter signals **queue saturation** — events are being enqueued faster than the
background daemon thread can drain them, causing `queue.Full` drops. The two-queue split is
intentional (Phase 98 REVIEWS.md Issue 5 Option A): `web.api_hardening` handles `search_api_request`
events; `shared.posthog_server` handles all other events including breaker telemetry. Both must be
monitored independently.

> **IMPORTANT — scope of these counters:**
>
> These counters count **only events dropped due to `queue.Full` (queue saturation)**.
>
> They do **NOT** detect — and will show ZERO for — any of the following delivery failures:
>
> - SSL/TLS errors on the outgoing `requests.post` call
> - Network failures (DNS, timeout, TCP reset, offline)
> - A missing, invalid, or revoked ingest key
> - A non-2xx HTTP response from PostHog's ingest endpoint
> - Any other transport-layer failure
>
> The background daemon thread swallows these exceptions silently (fire-and-forget design). **A zero
> drop count therefore does NOT prove that events are being delivered.** The counters are purely a
> queue-health signal.
>
> **The only way to prove delivery is the synchronous `--telemetry-selftest` flag** (section d), whose
> `SSL_OK` result is backed by a real HTTP-2xx confirmation from the PostHog ingest endpoint. A zero
> drop count with no selftest is not a delivery proof.

---

## (d) `--telemetry-selftest` usage

The headless CLI flag `--telemetry-selftest` is the **delivery proof tool** for release engineering
and clean-machine validation. It calls `send_selftest_event_sync()` (a synchronous, non-fire-and-forget
POST with a short timeout) and exits with a machine-readable result token:

| Token | Exit code | Meaning |
|-------|-----------|---------|
| `SSL_OK` | 0 | Real HTTP-2xx received from PostHog ingest endpoint. Delivery confirmed. |
| `SSL_FAIL` | 1 | SSL error, network failure, or non-2xx response. Check SSL cert bundling and network. |
| `NO_KEY` | 2 | No `phc_` key is baked into the build (the sentinel `_UNFILLED_KEY_SENTINEL` is present). Distinct from an SSL failure. |

**Usage:**

```
GenizahSearchPro.exe --telemetry-selftest
```

The flag is parsed **before `QApplication` construction** so it runs fully headlessly (no GUI, no
event loop). It toggles consent in-memory for the duration of the probe (does **not** write to
`config.pkl`). On a clean no-Python Windows VM, `SSL_OK` proves that `certifi`'s `cacert.pem` is
correctly bundled into the frozen binary by PyInstaller's `requests` hook — the standard
`GenizahSearchPro.spec` has no explicit `certifi` entry; PyInstaller bundles it automatically, but
the clean-VM run (D-06, Phase 116) is the only condition that proves it.

**Offline arm:**

```
GenizahSearchPro.exe --telemetry-selftest-offline
```

Returns `OFFLINE_OK` (exit 0). This is a **smoke token only** — it makes no network call and proves
nothing about actual delivery. Its purpose is to confirm the binary launches and the selftest code path
runs without crashing in an air-gapped environment. The real offline-degradation proof is a
**normal app launch with the network disabled**: the app must start silently (no dialog, no crash, no
indefinite delay) with all telemetry activity bounded by the transport's `requests.post(timeout=2.0)`.

**Use in release engineering:**

- Run `--telemetry-selftest` on a **clean Windows VM with NO Python installed** once per release to
  confirm SSL cert bundling (D-06 / Phase 116 HUMAN-UAT). This same run closes the Phase 114
  "live PostHog event delivery" UAT item.
- Set `GENIZAH_TELEMETRY_KEY` for staging/rotation testing; run `--telemetry-selftest` to confirm the
  new key resolves and delivers before revoking the old one.

---

## (e) Opt-out behavior

When a user opts out via the Settings/About consent toggle:

1. `is_enabled()` immediately returns `False` — the consent gate blocks all new event emission
   before anything reaches the queue.
2. `_drain_and_discard()` is called to drain and discard any events already in the queue; nothing
   buffered before opt-out is transmitted.
3. The **per-install ID** (`uuid4` stored in `config.pkl`) is **retained on disk** (CONSENT-06). It
   is NOT deleted. This is by user decision: re-opting in preserves installation continuity (the same
   person's pre- and post-gap sessions can be correlated in PostHog).
4. On re-opt-in, emission resumes with the same install ID already on disk.

The opt-out is effective immediately and persists across launches — it is stored in the same
`config.pkl` consent record as the initial opt-in choice.

---

## Milestone-exit regression gate (D-10)

The full telemetry / crash / PostHog regression suite (~290 tests accumulated across Phases 111-116)
**MUST be green on both Ubuntu and Windows** before v8.1.0 ships. This suite includes:

- **PRIV-03 AST guard** (Phase 111-03) — `tests/test_telemetry_no_direct_posthog.py`: enforces that
  `shared/posthog_server.enqueue_event` is reached only through the desktop chokepoint.
- **D-17 dynamic-string guard** — `tests/test_no_dynamic_telemetry_strings.py`: enforces no dynamic
  event names.
- **PRIV-04 scrubber tests** (Phase 116-01) — `tests/test_telemetry_priv04.py`: asserts representative
  crash / search / My-Library scenarios never emit forbidden fields, and nothing emits before consent.
- **SC#3 synchronous self-test** (Phase 116-02) — `tests/test_telemetry_selftest.py`: asserts
  `--telemetry-selftest` flag wiring and `send_selftest_event_sync()` behavior.
- All other `test_telemetry*.py` and `test_no_direct*.py` / `test_no_dynamic*.py` tests from
  Phases 111-115.

**Exact regression command (run on both OSes before release):**

```
pytest tests/test_telemetry*.py tests/test_no_direct*.py tests/test_no_dynamic*.py -m "not gui"
```

The existing CI `tests` job already runs `pytest tests/ -m "not gui"` on **both** `ubuntu-latest`
and `windows-latest` (SC#1 already satisfied — no new CI job needed). This milestone-exit gate is
also documented in `.planning/phases/116-privacy-audit-ci-gate/116-VERIFICATION.md`.
