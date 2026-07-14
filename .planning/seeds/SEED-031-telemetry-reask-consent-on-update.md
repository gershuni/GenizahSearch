---
id: SEED-031
status: dormant
planted: 2026-07-14
planted_during: "none (post-v8.4.1 close, no active milestone)"
trigger_when: "revisiting desktop telemetry / opt-in rates, or the next desktop release milestone"
scope: small-medium
intended_runner: gsd-quick
---

# SEED-031: Re-ask desktop telemetry consent on update (throttled)

## Why This Matters

Desktop telemetry is opt-in via a **single first-run modal** (`desktop/consent_dialog.py`
+ `desktop/telemetry.py`), gated on `FIRST_RUN_SHOWN_KEY` so it shows **at most once, ever**.
Every non-Enable exit — "Not now / לא עכשיו", Escape, the X, even Enter — routes through the
single `done()` finalizer which sets `set_consent(False)` **and** locks
`FIRST_RUN_SHOWN_KEY=True`, so it never asks again.

Consequences:
- Only ~9 opt-in users observed in a 2-week PostHog window (2026-06-30 → 2026-07-14) — the
  telemetry sample is tiny and its representativeness is unknowable (non-consenters send
  nothing, so there's no denominator).
- The **"Not now" label is dishonest**: it promises deferral but behaves as a permanent No.
  Users who reflexively dismiss a first-run dialog before feeling any value become lifetime
  opt-outs.

Goal: gently give not-yet-opted-in users another chance **on app update**, WITHOUT nagging.
This is a low-risk lift because the current design *under*-asks — the anti-nag hardening is
already in place; we're loosening a too-strict lock, not adding pressure.

## When to Surface

**Trigger:** revisiting desktop telemetry / opt-in rates, or when planning the next desktop
release milestone (this ships in a desktop build).

## Scope Estimate

**Small–Medium.** Intended runner: **`/gsd-quick`** (with care). It touches the
heavily-reviewed Phase 112–116 consent subsystem and a large test suite, so treat it as a
small planned task, not a blind inline edit.

## The Rule to Implement

Re-ask the consent invite on a **new-version launch** ONLY IF **all** hold:
1. Not already opted in (`consent=True` short-circuits everything — never ask an opted-in user).
2. **≥ 30 days** since the last ask (time-based cooldown).
3. Under a **lifetime cap** (~3 ignored asks, then stop permanently).
4. The user has not chosen **"Don't ask again"** (hard opt-out).

Prefer a **NON-MODAL** surface for the repeat ask — reuse the existing `WhatsNewBar` /
update-notification bar in `desktop/update_ui.py` rather than a blocking modal. Piggyback on
the existing "new version since last launch" detection that already drives What's New (the
version-change plumbing exists). Add a "Don't ask again" affordance to the repeat prompt.

**Why 30-day cooldown, not "every update":** desktop ships ~weekly — **8 desktop releases in
35 days** (v8.0.0 Jun 9 → v8.5.0 Jul 14), including **3 in the Jun 22–24 window** (8.2.0 /
8.2.1 / 8.2.2). Literal "every update" would prompt a decliner **8× in 5 weeks** — exactly the
nagging to avoid, and it trains reflexive dismissal. A time-based cooldown decouples
ask-frequency from release-frequency (~2 asks over that same span). Semver-gating (only on X.Y
bumps) does NOT help, since minors also ship ~weekly.

## Data-Model Change

Replace the permanent "lock on decline" with re-askable state, stored in the **same
`config.pkl` app-config** used today (consent-independent — must work *before* opt-in):
- `telemetry_last_asked_version` + `telemetry_last_asked_ts` → drive the 30-day cooldown.
- `telemetry_ask_count` → drive the lifetime cap.
- `telemetry_never_ask` → the hard opt-out ("Don't ask again").
- Keep `FIRST_RUN_SHOWN_KEY` for the **very first** launch only; stop treating a plain decline
  as a permanent terminal state.

## Invariants to Preserve (Phase 112–116 anti-dark-pattern hardening)

- Enter/Return routes to **decline** — Enter can never silently opt in.
- Neither button is default (`setDefault(False)` + `setAutoDefault(False)`).
- Single `done()` finalizer remains the SOLE path that writes consent state.
- Consent chokepoint + property allowlist + fixed event-name registry in
  `desktop/telemetry.py` unchanged; UUID minted only inside `set_consent(True)`.
- Bilingual EN/HE copy; opt-out still immediate in Settings → General → Preferences.

## Explicitly OUT of Scope

- Two-tier "crash-reports-only" consent — over-engineering at ~9-user scale; would require
  threading a second state through the whole chokepoint (allowlist / event registry / drain).
  Revisit only if the install base grows materially.

## Breadcrumbs

- `desktop/consent_dialog.py` — `ConsentDialog` (first-run modal), `PrivacyDialog`, single
  `done()` finalizer, Enter→decline `keyPressEvent`.
- `desktop/telemetry.py` — `FIRST_RUN_SHOWN_KEY`, `CONSENT_*` keys, `set_consent()`,
  `show_first_run_prompt()` (gates on `FIRST_RUN_SHOWN_KEY`), `_load_consent_state()`.
- `desktop/update_ui.py` — `WhatsNewBar` / `UpdateNotificationBar` / `WhatsNewDialog`;
  existing "new version since last launch" detection to reuse.
- `genizah_app.py` — call site of `show_first_run_prompt` / What's New on startup.
- Tests: `tests/test_telemetry_consent_ux.py`, `tests/test_telemetry_consent_gate.py`,
  `tests/test_telemetry_*.py` (large suite — run the consent/UX ones locally before push).

## Notes

Origin: 2026-07-14 PostHog usage review (desktop + public API, last 2 weeks) → discussion of
lifting opt-in without nagging. User chose "ask again on every update (only if not opted in)";
refined to a throttled cooldown here. GUI/consent change → verify with a live desktop launch
smoke (headless pytest can't exercise the modal/bar render path).
