# Codex Critique — Phase 114 (Usage Analytics) discuss decisions

**Reviewer:** Codex (`codex exec`, gpt-5.5) — 2026-06-15, during `/gsd-discuss-phase 114`.
**Verdict:** "Would NOT approve as written." Chokepoint is strong; producer/identity decisions need
tighter implementation rules. All findings reviewed against live code; **HIGH-1 verified by Claude**
(see below) and is a genuine bug-in-waiting.

---

## HIGH

### HIGH-1 — Wrong identity source breaks web↔desktop merge ✅ VERIFIED, ADOPTED
The brief said `current_user.id`. Live code: `SupabaseCorrectionsClient.User.id` is a compatibility
**int hash** (`hash(user_id) % 10**9`, `supabase_corrections_client.py:731`), NOT the Supabase UUID.
The real UUID is `current_user._uuid` (`:111`, set `=user_id` at `:738`/`:748`). Every cloud query
already keys on `_uuid` (corrections/comments/lists/joins `author_id`/`user_id`). Web identifies with
`user['id']` = the raw Supabase UUID (`web/auth_state.py:164`).
→ **Desktop MUST identify with `current_user._uuid` (or `auth.get_session().user.id`), never `.id`.**
Using `.id` would attach desktop activity to a hashed int that merges with nothing on web.

### HIGH-2 — Dynamic UI strings can leak/fragment despite the allowlist ✅ ADOPTED
The chokepoint validates property KEYS, not VALUE shapes (`_ALLOWED_PROPS`, `desktop/telemetry.py:288`).
`tab_name`/`dialog_name`/`feature_name`/`action` VALUES must come from **hardcoded constants**, never
from `tabText()`/`currentText()`/`windowTitle()`/QAction text/dialog titles/`selectedFiles()`. Proven
leak vectors: Visual Similarity dialog embeds shelfmark in its title (`genizah_app.py:4970`); FJMS
dialogs receive `sys_id`/`shelfmark` (`:9248`); export code sits in query/path-rich state (`:20347`).
(Scrubber would catch Hebrew/paths but English/transliterated content and analytics-fragmentation would
slip through.) → producer-side enum constants + a test/AST guard.

### HIGH-3 — Startup identity ordering needs an explicit coordinator ✅ ADOPTED
Achievable but not by casually adding `session_start` to startup. `corrections_client` is built early
(`:3302`); consent prompt + `_restore_session` are QTimer-deferred (`:3539`/`:3542`); telemetry trusts
persisted `IDENTIFIED_USER_KEY` before proving Supabase is still logged in (`telemetry.py:471`).
→ One boot coordinator: **resolve consent → resolve auth UUID (or reset stale identity) → identify(uuid)
if logged in → emit `session_start` exactly once.** PostHog alias-merge is acceptable only as a
timeout fallback, not the primary path.

## MEDIUM

### MED-1 — Cancelled-search accounting underspecified ✅ ADOPTED
Standard `SearchThread` cancel emits `[]` (no content, `gui_threads.py:96`) — safe. But `LabSearchThread`
has no `cancel_flag` check in its callback (`:144`), and `stop_search()` may terminate a worker with no
finish signal (`genizah_app.py:17280`). → Track each run with a per-run state object; emit **once** from
completion OR explicit user-stop. Do NOT count closeEvent/app-shutdown cancellation as a user cancel.

### MED-2 — Suppress programmatic tab changes ✅ ADOPTED
Volume of every-switch is fine for tens of users (no debounce needed), BUT `_restore_session()`
programmatically restores `active_tab` (`:26248`) and `setCurrentIndex(0)` calls abound — those must NOT
count as user navigation. → Guard the `currentChanged` producer with the existing `_restoring_session`
flag (and ignore programmatic `setCurrentIndex`).

### MED-3 — Heartbeat must be focus/resume-aware ✅ ADOPTED
Naive 24h QTimer is fragile: sleep/resume fires late, app-open-while-asleep fabricates DAU, and it can
double-count the launch day with `session_start`. → Emit `desktop_active_ping` **at most once per UTC
day**, only when the app is active/resumed, and not on the same UTC day as `session_start`. Add
`ACTIVE_PING='desktop_active_ping'` to the `DesktopEvent` registry (PRIV-06).

## LOW (all adopted)
- **search_mode** = static index→enum map (or effective-flow), never `currentText()` (tab labels are
  translated, `:3624`).
- Prefix-parsed searches (e.g. `#` shelfmark prefix) → report the **effective** mode, not the raw combo mode.
- `session_end` needs an **exactly-once** guard if wired via BOTH `closeEvent` and `atexit`.
- Do **not** send `install_id` as a normal event prop — `distinct_id` + `$anon_distinct_id` already cover identity.
- Add an AST/test guard: Phase 114 producers use literal enums and never call
  `currentText()`/`windowTitle()`/`selectedFiles()`/query fields/filepath helpers in payload construction.

---

**Disposition:** ALL findings adopted into `114-CONTEXT.md` `<decisions>`/`<constraints>`. None reverse
Hillel's product choices (every-switch tab tracking, exact-mode enum, result bucket now, include
cancelled, daily heartbeat, identify-before-session-start) — they refine HOW to implement them safely.
