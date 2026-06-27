---
id: SEED-012
status: dormant
planted: 2026-06-22
planted_during: desktop release crash-data review (PostHog desktop telemetry, v8.1.0 line)
trigger_when: A post-release desktop stability/startup-hardening pass. NOT release-blocking — it is a network-robustness improvement, not a regression. Pairs naturally with any future work on the desktop startup path (StartupThread / cloud-client init) or a follow-on to the Phase 98 NLI-resilience timeout philosophy.
scope: medium (touches startup ordering + network/SSL robustness; not a one-liner)
---

> **ROUTING:** Pre-existing desktop startup-robustness issue, surfaced 2026-06-22 while reviewing desktop
> crash telemetry. Captured as a seed (NOT implemented inline) because it is bigger/riskier than the
> sibling `Qt::ApplicationState` fix that went into the release, and slipping a startup-ordering change
> into an in-flight release is risky. Deliberately kept OUT of the current release per user direction.

# SEED-012: Desktop — Supabase corrections client init hangs app launch on flaky network

## Symptom (observed 2026-06-22, dev machine, from crash_log.txt + PostHog)

On a launch with a slow/blocked network, the app froze at startup and had to be killed
(`KeyboardInterrupt` while hung). Captured in `crash_log.txt`:

```
Crash at 2026-06-22T07:23:57.430615
Traceback (most recent call last):
  File "C:\Genizahsearch\genizah_app.py", line 27678, in <module>
    window = GenizahGUI()
  File "C:\Genizahsearch\genizah_app.py", line 3322, in __init__
    self.corrections_client = get_corrections_client()
  File "C:\Genizahsearch\corrections_client.py", line 1620, in get_corrections_client
    _client_instance = get_supabase_corrections_client()
  File "C:\Genizahsearch\supabase_corrections_client.py", line 2157, in get_supabase_corrections_client
    _supabase_client_instance = SupabaseCorrectionsClient()
  File "C:\Genizahsearch\supabase_corrections_client.py", line 316, in __init__
    self._load_credentials()
  File "C:\Genizahsearch\supabase_corrections_client.py", line 347, in _load_credentials
    client = self._get_client()
  File "C:\Genizahsearch\supabase_corrections_client.py", line 329, in _get_client
    self._client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
  ... supabase create_client -> httpx Client -> create_ssl_context ->
  File ".../ssl.py", line 770, in create_default_context
    context.load_verify_locations(cafile, capath, cadata)
KeyboardInterrupt
```

The `KeyboardInterrupt` is the tell: the process was **hung** in synchronous network/SSL setup and the
user killed it — not a code exception. (Telemetry did NOT capture this as a `desktop_crash`; it surfaced
only because it's the dev machine with a local `crash_log.txt`.)

## Root cause

`GenizahGUI.__init__` (`genizah_app.py:3322`) creates the Supabase corrections client **synchronously on
the startup/UI path**: `self.corrections_client = get_corrections_client()`. That call chain builds an
httpx client and an SSL context (`ssl.create_default_context` / `load_verify_locations`), with **no
timeout** and no fail-open. When the network/DNS/SSL is slow or blocked, app launch blocks indefinitely.

Same machine, same day, the NLI circuit breaker also tripped — i.e. the network was genuinely flaky, which
is exactly the condition this exposes. This is the desktop analogue of the 2026-05-25 web NLI-hang class
(synchronous blocking network call with a generous/absent timeout on a hot path).

## Scope of the fix (directional — to be designed in the phase)

Goal: a network blip must never freeze app launch; cloud (corrections/lists/comments) features may come up
late or degrade, but the app window must appear promptly.

Options (pick during discuss/plan):
1. **Defer cloud-client creation off the startup path** — don't call `get_corrections_client()` in
   `__init__`; lazily create on first actual use, or warm it on the existing background `StartupThread`
   instead of the constructor. `get_corrections_client()` is already a lazy singleton getter, so the bug is
   *where it's called* (the constructor), not the getter itself.
2. **Bound the client creation with a timeout + fail-open** — if `create_client`/SSL setup exceeds a few
   seconds, log and continue without the cloud client; retry later. Mirror the Phase 98 NLI-resilience
   env-knob/timeout pattern (`shared/nli_circuit_breaker.py`, connect/read timeouts).
3. Both: deferred init AND a guarded/timeout-bounded creation for robustness.

## Notes / pointers

- Pre-existing (constructor has created the corrections client for a long time); NOT a v8.1.0 regression.
- Desktop-only. Web's Supabase usage is separate.
- Verify whether `_load_credentials()` / `_get_client()` already has any retry/timeout — design the fix to
  not double-wrap.
- Faulthandler note (separate, unrecoverable): the `desktop_prior_crash` (`unknown_native`) recorded
  06-22 06:34 had its `faulthandler_dump.txt` overwritten by later relaunches (opened `'w'` per launch in
  `desktop/telemetry.py:_setup_faulthandler`), so its root cause can't be recovered — unrelated to this seed.

## Related

- Sibling fix (shipped via the release session, NOT here): `Qt::ApplicationState` enum marshaling crash in
  the telemetry active-ping — `genizah_app.py` `_on_app_state_changed` made a zero-arg slot.
- See also [[project_nli_iiif_hang_pattern]] and the Phase 98 NLI-resilience timeout philosophy.
