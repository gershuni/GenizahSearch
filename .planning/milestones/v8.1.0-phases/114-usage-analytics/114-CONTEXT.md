# Phase 114: Usage Analytics - Context

**Gathered:** 2026-06-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Wire the **usage-analytics producers** and the **identity lifecycle** into the PyQt6 desktop app.
The telemetry chokepoint already exists (Phases 111/113): `desktop/telemetry.py` exposes
`track()`, `track_performance()`, `identify()`, `reset_identity()`, the `DesktopEvent` fixed
event-name registry, the `_ALLOWED_PROPS` allowlist, the structural scrubber, and the consent gate.
Phase 114 does NOT build new chokepoint machinery — it **calls the existing API from the right
places** in `genizah_app.py`, the search threads (`gui_threads.py`), and the auth flow.

**In scope (requirements USAGE-01..06, IDENT-01/02):**
- `desktop_session_start` (allowlisted env props) + best-effort `desktop_session_end` + a daily
  active-user heartbeat (`desktop_active_ping`).
- Feature/tab usage: `desktop_tab_activated` + `desktop_feature_opened`.
- `desktop_search_executed` (mode + corpus + result bucket).
- Identity: `identify(uuid)` on login / startup-restored session; `reset_identity()` on logout.
- Session/clock correctness (USAGE-06).

**Out of scope:** performance summary/timings (Phase 115, PERF-01..03), the privacy CI audit
(Phase 116, PRIV-04/INFRA-06). No new chokepoint primitives, no SDK, no web changes.
</domain>

<decisions>
## Implementation Decisions

### Feature & tab usage (USAGE-02)
- **D-01:** `desktop_tab_activated` fires on **every** user tab switch (full navigation fidelity),
  `tab_name` carried as a **hardcoded enum constant** (one of the 7 tabs), never `tabText()` (labels
  are translated EN/HE).
- **D-02:** `desktop_tab_activated` MUST ignore **programmatic** tab changes — guard the
  `currentChanged` handler with the existing `_restoring_session` flag and any `setCurrentIndex()`
  call path so `_restore_session()`-driven restores and code-driven jumps don't count as navigation
  (Codex MED-2).
- **D-03:** `desktop_feature_opened` instrumented for **Joins Lab**, **Fragment Puzzle**, **major
  dialogs** (ResultDialog, FJMS catalog, Visual Similarity, export dialog — via `dialog_name` enum),
  and **export actions** (which format: xlsx/CSV/DOCX/TXT — via the allowlisted `action` prop).
- **D-04 (HARD, Codex HIGH-2):** ALL of `tab_name`/`dialog_name`/`feature_name`/`action` values come
  from **producer-side hardcoded constants**. NEVER source them from `tabText()`/`currentText()`/
  `windowTitle()`/`QAction.text()`/dialog titles/`selectedFiles()`. Proven leak vectors in live code:
  Visual Similarity dialog title embeds the shelfmark (`genizah_app.py:4970`); FJMS dialogs receive
  `sys_id`/`shelfmark` (`:9248`); export sits in query/path-rich state (`:20347`). The allowlist gates
  KEYS, not value content — so this discipline is the value-side guard.

### Search event (USAGE-03)
- **D-05:** `desktop_search_executed` carries **exact UI search mode** as an enum (each `mode_combo`
  entry — keyword/phrase/regex/variants/shelfmark/etc. — plus the responsa/composition/parallels
  flows), via a **static index→enum map** (NOT `currentText()` — Codex LOW). Hillel chose maximal
  granularity ("you can aggregate up later").
- **D-06:** For prefix-parsed searches (e.g. a `#` shelfmark prefix typed in keyword mode), report the
  **effective** mode after parsing, not the raw combo selection (Codex LOW).
- **D-07:** Also carry `corpus_scope` (Genizah/Local/ALL, enum) and a coarse `result_count_bucket`
  (`0` / `1-9` / `10-99` / `100+`) — included NOW, not deferred to Phase 115, to surface zero-result
  rate / effectiveness from day one.
- **D-08:** Emit on **user-initiated completed AND cancelled** runs; status via the allowlisted
  `action` prop (`completed` | `cancelled`). Cancelled runs carry NO `result_count_bucket`.
  Auto/incremental reruns are NOT counted.
- **D-09 (Codex MED-1):** Track each search with a **per-run state object**; emit **exactly once**,
  from either completion OR explicit user-stop. `LabSearchThread` lacks a `cancel_flag` check in its
  callback (`gui_threads.py:144`) and `stop_search()` may terminate a worker with no finish signal
  (`genizah_app.py:17280`) — the per-run object closes both gaps. Do NOT count closeEvent / app-
  shutdown cancellation as a user cancellation.

### Identity lifecycle (IDENT-01/02)
- **D-10 (HARD, Codex HIGH-1 — VERIFIED):** The identity `distinct_id` is the **Supabase UUID =
  `corrections_client.current_user._uuid`** (or `client.auth.get_session().user.id`), **NEVER
  `current_user.id`**. `User.id` is a compatibility int hash (`hash(user_id) % 10**9`,
  `supabase_corrections_client.py:731`); `_uuid` (`:111`) holds the real UUID and is what every cloud
  query already keys on. Web identifies with `user['id']` = the same raw UUID (`web/auth_state.py:164`).
  Using `.id` would attach desktop activity to a hash that merges with nothing on web — silently
  defeating IDENT-01.
- **D-11:** On startup, once consent is True AND auto-login has populated `current_user`, call
  `identify(_uuid)` **immediately**, and **BEFORE** the `desktop_session_start` event, so session_start
  attributes to the merged person.
- **D-12 (Codex HIGH-3):** Implement a single **startup identity coordinator** rather than scattering
  calls: resolve consent → resolve the auth UUID (or `reset_identity()` if persisted
  `IDENTIFIED_USER_KEY` is stale and Supabase is no longer logged in) → `identify(uuid)` if logged in →
  emit `session_start` exactly once. `corrections_client` is built early (`genizah_app.py:3302`) but
  consent + `_restore_session` are QTimer-deferred (`:3539`/`:3542`); `_load_consent_state` currently
  trusts persisted `IDENTIFIED_USER_KEY` (`telemetry.py:471`) without re-checking Supabase. PostHog
  alias-merge is acceptable ONLY as a timeout fallback, never the primary attribution path.
- **D-13:** Mid-session explicit login → `identify(_uuid)` (aliases anon→user via `$anon_distinct_id`).
  Logout (`_do_logout`) → `reset_identity()` (reverts to the per-install anon uuid, mirrors web
  `posthog.reset()`). If a logged-in user opts in via Settings mid-session, `set_consent(True)` should
  also trigger identify before any usage event.

### Session & active-user (USAGE-04/06)
- **D-14:** Exactly one `uuid4` `session_id` per process; all timestamps UTC; monotonic clock for any
  durations. `desktop_session_start` fires **exactly once** (after identity resolves) so a crash-restart
  begins a fresh session with no ghost duplicate.
- **D-15:** `desktop_session_end` is **best-effort** on clean exit (closeEvent/atexit), with an
  **exactly-once guard** if wired through both paths (Codex LOW). Absent on crash/kill — the Phase 113
  crash event + next-launch native-crash detection already cover crashes.
- **D-16:** Daily active-user heartbeat — add `ACTIVE_PING = 'desktop_active_ping'` to the
  `DesktopEvent` registry (new member, PRIV-06 fixed registry). Emit **at most once per UTC day**,
  only when the app is **active/resumed**, and **not** on the same UTC day as that launch's
  `session_start` (Codex MED-3 — avoid sleep/resume skew, fabricated DAU, and launch-day double-count).
  NOT a naive 24h QTimer; use a periodic check + focus/window-state awareness.

### Privacy / guard (cross-cutting)
- **D-17 (Codex LOW):** Add an AST/test guard (sibling to `tests/test_no_raw_storage_access.py`) that
  asserts Phase 114 producers build payloads from **literal enum constants only** and never call
  `currentText()`/`windowTitle()`/`selectedFiles()`/query fields/filepath helpers in telemetry payload
  construction. Strengthens PRIV-04 (Phase 116) at the producer layer.
- **D-18 (Codex LOW):** Do NOT send `install_id` as a normal event property — `distinct_id` +
  `$anon_distinct_id` already carry identity. (`install_id` is allowlisted but reserve it.)

### Claude's Discretion
- Exact `tab_name`/`dialog_name`/`feature_name` enum string values; the search index→mode-enum map;
  the heartbeat polling interval and focus/resume detection mechanism; the per-run search state object
  shape; where the startup identity coordinator lives. All within the constraints above.
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & milestone decisions
- `.planning/REQUIREMENTS.md` — USAGE-01..06 + IDENT-01/02 are this phase's reqs (Pending rows in
  the traceability table). The "Fixed constraints" + Out-of-Scope tables are LOCKED.
- `.planning/research/POSTHOG-PROJECT-DECISION.md` — why ONE shared web project + web-aligned identity
  (the basis for D-10/D-11).
- `.planning/phases/111-telemetry-foundation/111-CONTEXT.md` — foundation decisions (consent gate only
  in `desktop/telemetry.py`; `posthog_server` stays ungated; embedded key resolution).
- `.planning/phases/113-crash-reporting/113-CONTEXT.md` — crash hooks + `_crash_distinct_id` snapshot +
  next-launch native-crash re-emit (interacts with D-14 session correctness).

### Review artifacts for THIS phase
- `.planning/phases/114-usage-analytics/114-CODEX-CRITIQUE.md` — the Codex review that produced D-02,
  D-04, D-09, D-10, D-12, D-16, D-17 (HIGH-1 verified by Claude). Read before planning.

### Live code the producers wire into
- `desktop/telemetry.py` — the SOLE emission API. `DesktopEvent` enum (add `ACTIVE_PING`), `track()`,
  `identify()`, `reset_identity()`, `_ALLOWED_PROPS`, `set_consent`, `_load_consent_state` (the stale-
  identity concern at `:471`).
- `genizah_app.py` — tabs + `self.tabs.currentChanged.connect(self._on_tab_changed)` (~`:3728`);
  `_restoring_session` guard (`:3428`); `_restore_session` QTimer defer (`:3542`); login/logout
  (`_show_login_dialog`/`_do_logout`/`_corner_login_clicked`/`_update_corner_login_state`); `mode_combo`;
  `corpus_scope_combo`/`comp_corpus_scope_combo`; `stop_search()` (`:17280`).
- `gui_threads.py` — `SearchThread` (cancel emits `[]`, `:96`) + `LabSearchThread` (no cancel check,
  `:144`).
- `supabase_corrections_client.py` — `User._uuid` (`:111`) is the canonical identity; `.id` is a hash
  (`:731`). `login()`/`logout()`/`current_user`/`get_current_user`.
- `web/auth_state.py:160-170` — `_posthog_identify` uses `user['id']` (the merge target for D-10).
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`desktop/telemetry.py` chokepoint** — fully built; reserves `SESSION_START`, `SESSION_END`,
  `TAB_ACTIVATED`, `SEARCH_EXECUTED`, `FEATURE_OPENED` in `DesktopEvent` and `tab_name`/`search_mode`/
  `corpus_scope`/`result_count_bucket`/`feature_name`/`dialog_name`/`action`/`session_id` in
  `_ALLOWED_PROPS`. Phase 114 adds only `ACTIVE_PING` to the enum.
- **`_restoring_session` flag** (`genizah_app.py:3428`) — already exists to suppress spurious saves
  during startup; reuse it to gate programmatic tab-change telemetry (D-02).
- **`current_user._uuid`** — already the canonical id across all cloud writes; reuse as `distinct_id`.

### Established Patterns
- All emission MUST route through `desktop/telemetry.py` (AST guard `test_no_raw_storage_access.py`
  enforces no raw `enqueue_event`). Event names from `DesktopEvent` only; props from allowlist only.
- Identity: anon `uuid4` logged-out, Supabase UUID logged-in, alias on login, reset on logout — exactly
  mirroring web (`posthog.identify`/`posthog.reset`).
- Translated UI (EN/HE) means any `*.text()`/`*Text()` accessor returns localized strings — forbidden
  as telemetry values (D-04, D-05).

### Integration Points
- Tab telemetry: inside `_on_tab_changed` (guarded by `_restoring_session`).
- Search telemetry: at SearchThread/LabSearchThread completion AND `stop_search()`, via a per-run
  state object (D-09).
- Identity + session_start: a new startup coordinator invoked after consent + auto-login resolve (D-12).
- Login/logout identity: in `_do_logout` and the login-success path (and `set_consent(True)`).
</code_context>

<specifics>
## Specific Ideas

- Hillel wants **maximal navigation/search fidelity** (every tab switch; exact UI search mode) — he'd
  rather have granular data and aggregate up in PostHog than lose distinctions.
- The cross-surface (web↔desktop) per-user journey is the headline value of the IDENT work — getting
  D-10 (the `_uuid` identity source) right is what makes that journey real.
</specifics>

<deferred>
## Deferred Ideas

- **Performance timings / per-session perf summary** — Phase 115 (PERF-01..03). The result-count
  *bucket* lives on the usage event now (D-07), but durations/p95/sampling are Phase 115.
- **Privacy CI audit + operational runbook** — Phase 116 (PRIV-04, INFRA-06). The producer-layer AST
  guard (D-17) is a down-payment, not the full audit.
- **Handled/non-fatal error counting** — ERR-01 (Future), explicitly out of v8.1.0.

None of the above is scope creep into Phase 114 — they are already-mapped later phases.
</deferred>

---

*Phase: 114-usage-analytics*
*Context gathered: 2026-06-15*
