# Codex Brief — Phase 114 (Usage Analytics) discuss-phase decisions

**Your task:** Critique the implementation decisions below BEFORE we write CONTEXT.md and plan the phase.
Focus on: privacy-leak risks, wiring pitfalls in a PyQt6 app, identity-merge correctness,
event-volume/cost sanity, and any MISSING gray area we should have decided. Be concrete and
adversarial. Rank findings HIGH / MEDIUM / LOW. It's fine to approve.

## Project context

GenizahSearch — dual app (PyQt6 desktop + NiceGUI web) for Cairo Genizah manuscript research.
v8.1.0 milestone adds **opt-in, privacy-preserving telemetry to the DESKTOP app only** (web already
instrumented). Phases 111–113 are DONE: the telemetry chokepoint, consent UX, and crash reporting
all ship. **Phase 114 only WIRES USAGE PRODUCERS + the IDENTITY LIFECYCLE** into `genizah_app.py`
and the search threads — the chokepoint API already exists.

### Read these live files before critiquing
- `desktop/telemetry.py` — the chokepoint. Note: `track()`, `track_performance()`, `identify()`,
  `reset_identity()`, the `DesktopEvent` enum (fixed event-name registry, PRIV-06), `_ALLOWED_PROPS`
  allowlist, `_scrub_props`/`_scrub_value` scrubber, consent gate. ALL emission must go through this.
- `.planning/REQUIREMENTS.md` — USAGE-01..06, IDENT-01/02 are the phase-114 requirements.
- `genizah_app.py` — tab widget + `self.tabs.currentChanged.connect(self._on_tab_changed)` (~line 3728);
  login/logout: `_show_login_dialog`, `_do_logout`, `_corner_login_clicked`, `_update_corner_login_state`;
  startup auto-login + `_restore_session` (deferred via QTimer); search via `mode_combo` + SearchThread.
- `supabase_corrections_client.py` — `login()`, `logout()`, `current_user`, `current_user.id` (= Supabase user.id).

### LOCKED (do NOT re-litigate — these are prior-phase decisions)
- Reuse `shared/posthog_server.py` raw queue; **no `posthog` Python SDK** (PII-leak via frame locals).
- ONE shared web PostHog project (id 134161, EU); web↔desktop split by `platform=desktop` base prop
  + `desktop_` event-name namespace. Embedded publishable phc_ key.
- distinct_id = Supabase `user.id` for logged-in (EXACT same value web uses, so personas merge);
  anonymous per-install `uuid4` for logged-out; alias anon→user on login; reset to anon on logout.
- HARD: never transmit My-Library paths/filenames/content or any search/query text. Enforced by
  scrubber + allowlist + fixed event registry + AST CI guard.
- Opt-in only, default OFF; consent gate resolved before any event fires.

## Phase 114 decisions to critique

### Area 1 — Feature/tab usage (USAGE-02)
- `desktop_tab_activated` fires on **EVERY** tab switch (full navigation fidelity), `tab_name` enum.
- `desktop_feature_opened` for: **Joins Lab**, **Fragment Puzzle**, **major dialogs** (ResultDialog,
  FJMS catalog, Visual Similarity, export dialog — via `dialog_name` enum), and **export actions**
  (which format: xlsx/CSV/DOCX/TXT — via the allowlisted `action` prop).

### Area 2 — Search event (USAGE-03)
- `desktop_search_executed` carries: **exact UI search mode** as an enum (each `mode_combo` entry —
  keyword/phrase/regex/variants/shelfmark/etc. — plus responsa/composition/parallels flows),
  `corpus_scope` (Genizah/Local/ALL), and a **coarse `result_count_bucket`** (0 / 1-9 / 10-99 / 100+).
- Fires on **user-initiated completed AND cancelled** runs (status via `action`=completed|cancelled;
  cancelled carries NO result bucket). Auto/incremental reruns are NOT counted.

### Area 3 — Identity lifecycle (IDENT-01/02)
- On startup, once consent gate is True AND auto-login has populated `current_user`, call
  `identify(user.id)` **immediately** — and **BEFORE** the `session_start` event, so session_start
  attributes to the merged person.
- Mid-session explicit login → `identify()` (aliases anon→user). Logout → `reset_identity()`.

### Area 4 — Session & active-user (USAGE-04/06)
- One `session_start` per launch (after identity is resolved).
- A **daily heartbeat** (active-ping ~once per 24h uptime / on resume after long idle) so a
  multi-day open session still counts toward DAU. (Requires a NEW `DesktopEvent` enum member,
  e.g. `ACTIVE_PING='desktop_active_ping'`, added to the fixed registry per PRIV-06.)
- `session_end` is **best-effort** on clean exit (closeEvent/atexit); absent on crash/kill (the 113
  crash event + next-launch native-crash detection already cover crashes).

### Default decisions (mechanical, not asked)
- USAGE-06: exactly one `uuid4` `session_id` per process; UTC timestamps; monotonic clock for any
  durations; `session_start` fires EXACTLY ONCE so a crash-restart begins a fresh session with no
  ghost duplicate.

## Questions for you
1. Any way My-Library paths/filenames, query text, or UI strings could leak through these specific
   producers (esp. `tab_name`/`dialog_name`/`feature_name`/`action` if sourced from QAction text or
   window titles instead of fixed enums)?
2. Identity-ordering race: at startup, auto-login is deferred (`_restore_session` via QTimer) — is the
   "identify before session_start" guarantee actually achievable, or could session_start fire on the
   anon id and rely on PostHog alias-merge? Is that acceptable?
3. Is `desktop_tab_activated` on every switch a volume/cost concern for a shared PostHog project
   (tens of desktop users)? Should we cap/debounce?
4. Cancelled-search instrumentation — any race where a cancelled SearchThread emits content?
5. Heartbeat design — QTimer pitfalls, sleep/resume clock skew, double-count risk vs session_start.
6. Any gray area we MISSED that the planner will get wrong without a decision here?
