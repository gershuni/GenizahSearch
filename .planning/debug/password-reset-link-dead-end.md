---
slug: password-reset-link-dead-end
status: awaiting_human_verify
trigger: |
  DATA_START
  Got a message from user: "thank you very much for developing such an useful tool.
  Today I tried to reset my login password. I received an automated message with the
  reset link, but when I click on it, it redirects me to the genizahsearch website.
  I've tried several times, but it seems that the link isn't working as expected."
  DATA_END
created: 2026-07-28
updated: 2026-07-28T(fix-applied+specialist-reviewed+patch-reverified)
surface: web + desktop (shared Supabase auth)
---

# Debug: Password reset link is a dead end

## Symptoms

**Expected behavior**
Clicking the password-reset link in the Supabase recovery email should land the user on
a "set a new password" screen where they can choose a new password and then log in
(desktop and/or web).

**Actual behavior**
The link redirects to the genizahsearch.com website (apparently the homepage) and
nothing else happens. No set-new-password UI is presented. Reproducible — the reporter
tried several times.

**Error messages**
None reported by the user. No client-side error surfaced. (Server log / browser console
not yet inspected.)

**Timeline**
Reported 2026-07-28 by an end user via email. Unknown whether a reset has EVER
succeeded for anyone — owner answered "Don't know". Treat as possibly never-worked.

**Reproduction**
1. Request a password reset (most likely via the desktop app's login dialog →
   "Forgot password?" link; the reporter did not specify, owner's guess is desktop).
2. Receive the Supabase automated recovery email.
3. Click the reset link in the email.
4. Browser lands on genizahsearch.com; no password-reset form appears.

**Surface attribution (owner answer)**
Reporter did not say. Owner's guess: desktop app. NOTE: this matters less than it
looks — the web app has NO "Forgot password?" entry point at all, so the request
almost certainly came from the desktop dialog, but the *landing* is always the web app.

**Supabase dashboard access**
Owner CAN check the Supabase dashboard on request (Authentication → URL Configuration:
Site URL + Redirect URLs; and the "Reset Password" email template).

## Pre-investigation code reconnaissance (orchestrator, before agent spawn)

Grep-level facts already established — verify, don't re-derive:

1. **Desktop is the only requester.** `corrections_ui.py:99-104` renders the
   "Forgot password?" link; `corrections_ui.py:159-181` (`open_forgot_password`) calls
   `self.client.request_password_reset(email)`.
2. **The reset call passes NO redirect target.**
   `supabase_corrections_client.py:693-717` (`request_password_reset`) calls
   `client.auth.reset_password_for_email(email)` with **no `options={'redirect_to': ...}`**.
   Consequence: Supabase falls back to the project's default **Site URL**, which is
   presumably `https://genizahsearch.com` → exactly the observed landing.
3. **The web app has no recovery handler and no set-password page.** Repo-wide greps
   for `reset_password_for_email` / `resetPasswordForEmail` / `type=recovery` /
   `forgot` / `/reset-password` find NOTHING in `web/`. The only auth-callback route is
   `web/main.py:2466` `@ui.page('/auth/callback')`, which handles the **Google OAuth
   PKCE `?code=` param only** — and the recovery email does not point there anyway.
4. **The only password-change path requires an already-logged-in session.**
   `web/supabase_client.py:603-640` (`change_password`) is a REST helper used from
   `web/pages/profile.py`; it needs a live user JWT. Useless to someone who is locked out.
5. **Web login dialog has no "forgot password" affordance at all**
   (`web/auth_state.py:370-440`) — so even a user who lands on the site cannot
   re-initiate or complete the flow from the web.

**Leading hypothesis (to be confirmed/refuted by the debugger, not assumed):**
The recovery flow is structurally incomplete, not merely misconfigured. Two stacked
defects: (a) `reset_password_for_email` sends no `redirect_to`, so the link lands on
the Site URL root; (b) even with a correct `redirect_to`, there is no page anywhere in
the app that consumes a `type=recovery` token / `?code=` and lets the user set a new
password. Either alone would break the flow; both are present.

**Secondary things worth checking**
- Whether the Supabase project is on the **implicit** flow (token in URL `#fragment`,
  which a server-rendered NiceGUI page never sees without JS) or **PKCE** (`?code=`
  query param, server-visible). This changes the shape of any fix.
- Whether `Redirect URLs` allowlist in Supabase would even permit a new
  `redirect_to` target (an unlisted target is silently downgraded to Site URL — a
  second, independent way to produce this exact symptom).
- The desktop angle: the reporter wanted a **desktop** login password. Even a working
  web reset page solves their problem only if the new password is then usable in the
  desktop app (it should be — same Supabase user), so confirm the desktop login reads
  the same credential store.
- Recovery tokens are single-use and short-lived; "tried several times" may also have
  burned links. Not the root cause, but do not let it mask the diagnosis.

## Current Focus

reasoning_checkpoint:
  hypothesis: "Path A (web-only, root-page fragment interception) fully closes the dead
    end because (1) Site URL is confirmed bare root and Redirect URLs allowlist does
    NOT include /reset-password (any redirect_to there would silently downgrade to
    Site URL anyway), so intercepting the fragment on `/` is the ONLY viable landing
    target without a dashboard write; (2) `set_session_from_url()` already exists and
    is Class-A-allowlisted by the Phase 90 D-15 AST guard specifically for this
    scenario, so completing the session requires zero new auth-boundary surface;
    (3) the expired/consumed-token case is handled by classifying Supabase's own
    `#error=access_denied&error_code=...` fragment, which is a documented, deterministic
    shape, not a guess."
  confirming_evidence:
    - "Dashboard read-back (checkpoint response): Site URL = https://genizahsearch.com
      (bare root); Redirect URLs allowlist = only /auth/callback variants; Reset
      Password template = unmodified default {{ .ConfirmationURL }}."
    - "gotrue-py source inspection: reset_password_for_email never builds a
      code_challenge (unlike sign_in_with_oauth), confirming implicit-flow (fragment)
      tokens, not PKCE (?code=)."
    - "set_session_from_url() has zero callers pre-fix (grep-verified) yet is already
      allowlisted by tests/test_no_set_session_outside_oauth.py -- direct evidence the
      consumer was anticipated, not that a new unaudited call path is being invented."
  falsification_test: "If a fresh recovery email's link, when clicked, does NOT show
    the set-new-password dialog (and the browser console shows no __genizahRecovery
    payload / a JS error), Path A does not work and Path B (email-template edit) would
    be required instead. This is exactly the live smoke test requested below."
  fix_rationale: "Addresses the root cause directly: fixes the missing CONSUMER (no
    fragment interceptor + no session-establishing page existed) rather than
    papering over the symptom (e.g. a generic 'contact support' message on the
    homepage). Reuses the already-audited set_session_from_url helper instead of
    adding a new set_session call site, keeping the Phase 90 D-15 auth-boundary
    invariant intact."
  blind_spots: "Not tested against a REAL Supabase recovery email in a real browser
    (no live environment in this pass) -- only unit-tested via mocks. The exact
    timing of history.replaceState vs PostHog/GA script execution order was reasoned
    about via script source-order, not observed in a live network trace. Owner
    live-site smoke test (below) is the closing verification step."

status: FIX APPLIED + specialist-reviewed + specialist-review patch RE-VERIFIED. The
one WARNING-level correctness gap found by the Python specialist review (fail-open on
falsy `session` from `set_session_from_url`) has now been patched in
`web/main.py::_complete_password_recovery_session` (lines ~1113-1123) and re-verified
against the full test sweep (19/19 in tests/test_password_reset_recovery.py + 35/35
guard tests; ruff clean). Not yet moved to resolved/; not yet committed -- awaiting the
owner's live-site smoke test only.

## Evidence

- timestamp: 2026-07-28T(session)
  checked: `corrections_ui.py:99-104,159-182` (Forgot-password link + handler)
  found: Desktop-only entry point. Calls `self.client.request_password_reset(email)`.
    No web "Forgot password?" affordance exists (confirmed separately below).
  implication: Confirms recon fact 1 exactly as stated.

- timestamp: 2026-07-28T(session)
  checked: `supabase_corrections_client.py:693-717` (`request_password_reset`)
  found: `client.auth.reset_password_for_email(email)` — no `options={'redirect_to': ...}`
    passed at all.
  implication: Confirms recon fact 2. Supabase falls back to the project's Site URL.

- timestamp: 2026-07-28T(session)
  checked: `gotrue/_sync/gotrue_client.py:774-789` (`reset_password_for_email` impl, the
    actual library method desktop calls) + `gotrue/_sync/gotrue_client.py:1123-1131`
    (`sign_in_with_oauth`'s PKCE code_challenge generation, for contrast) +
    `supabase/lib/client_options.py:56` (`ClientOptions.flow_type` default = `'pkce'`)
  found: `reset_password_for_email` POSTs `{email, gotrue_meta_security, redirect_to}` to
    `/recover` — it NEVER generates/sends a `code_challenge`, unlike `sign_in_with_oauth`
    which explicitly builds one when `flow_type == 'pkce'`. So even though the Python
    client's default `flow_type` is `'pkce'`, that setting is irrelevant to the recovery
    email request — no PKCE challenge is ever registered for this flow.
  implication: Without a registered `code_challenge`, Supabase Auth (GoTrue server) has no
    basis to return a `?code=` in the confirmation redirect. It falls back to embedding
    session tokens in the URL FRAGMENT (`#access_token=...&refresh_token=...&type=recovery`)
    on redirect to `redirect_to` (or Site URL). This is the **implicit flow**, per
    orchestrator investigation item 1 — confirmed, not PKCE.

- timestamp: 2026-07-28T(session)
  checked: Web search — Supabase docs/discussions on `resetPasswordForEmail` +
    SSR + implicit flow (see Sources below)
  found: "If the password reset link is valid... the user will get redirected to the
    password reset form with the following values in the url fragment: access_token,
    refresh_token, type, expires_in." AND explicitly: "A common problem occurs when using
    SSR: ... your server-side environment will not be able to parse the URL fragments
    since that is only present in the browser." Also found the officially-recommended SSR
    workaround: customize the email template to
    `{{ .SiteURL }}/auth/confirm?token_hash={{ .TokenHash }}&type=recovery&next=...` and
    implement a server route that calls `verify_otp({token_hash, type})`.
  implication: External documentation independently corroborates the code-level finding.
    Also surfaces the two concrete, separable fix shapes now in Current Focus.

- timestamp: 2026-07-28T(session)
  checked: `web/main.py:2466-2540` (`@ui.page('/auth/callback')`, full body)
  found: Handles ONLY `?code=`/`?error=`/`?error_description=` query params (Google OAuth
    PKCE). If none present (which is exactly what a recovery redirect would show, since
    its tokens are in the fragment, never in the query string), it falls through to
    `ui.navigate.to('/')` — i.e. it silently redirects to the homepage. This route CANNOT
    help even if the recovery link pointed here.
  implication: Confirms recon fact 3 in full. `/auth/callback` is not a viable landing
    target as-is for recovery links; a new page/route is required either way.

- timestamp: 2026-07-28T(session)
  checked: `web/supabase_client.py:761-789` (`set_session_from_url`, `exchange_code_for_session`)
    + `docs/guides/SUPABASE_GUIDE.md:551-604` ("OAuth Callback Flow" / implicit-flow
    token-extraction section) + repo-wide grep for callers of `set_session_from_url`
  found: `set_session_from_url(access_token, refresh_token)` exists, is documented for
    EXACTLY this fragment-extraction scenario, and is allowlisted by the D-15 AST guard
    (`tests/test_no_set_session_outside_oauth.py`) as a legitimate `set_session` call site
    — but it has ZERO callers anywhere in `web/` or `desktop/`. Only referenced in docs and
    the AST allowlist.
  implication: This is strong independent confirmation that the recovery/implicit-flow
    consumer was ANTICIPATED and partially built (the helper + the security allowlist
    entry exist) but the actual page/route/JS that would call it was never implemented.
    Directly supports "structurally incomplete, not merely misconfigured."

- timestamp: 2026-07-28T(session)
  checked: `web/main.py` grep for `@ui.page(` (full route list) + `web/auth_state.py:370-457`
    (login dialog body)
  found: No `/reset-password`, `/auth/confirm`, or any recovery-shaped route exists. Login
    dialog (`create_login_dialog`) has Login/Register tabs + Google button only — no
    "Forgot password?" link or affordance anywhere in the web UI.
  implication: Confirms recon fact 5. Even a user who successfully lands with valid tokens
    has nothing to do next; even re-requesting the flow from the web is impossible today.

- timestamp: 2026-07-28T(session)
  checked: `supabase_corrections_client.py:588-607` (desktop `login()`) +
    `shared/supabase_provider.py:24-32` (`SUPABASE_URL`/`SUPABASE_ANON_KEY` defaults)
  found: Desktop login calls `client.auth.sign_in_with_password(...)` against the SAME
    Supabase project (`ylcpglwxompwjcufdemz.supabase.co`, same anon key default) that the
    web app uses. There is exactly one Supabase Auth user table shared by both apps.
  implication: Answers orchestrator investigation item 4 — YES, a password set via ANY web
    reset mechanism (PUT `/auth/v1/user` bearer-token pattern already used by
    `web/supabase_client.py::change_password`, or GoTrue's `update_user` after
    `verify_otp`) is immediately usable for desktop login. No desktop-side change is
    required for the fix to fully solve the reporter's stated goal (a desktop-login
    password).

- timestamp: 2026-07-28T(session, specialist review)
  checked: `web/main.py:1113-1120` (`_complete_password_recovery_session`), via
    gsd-code-reviewer acting as Python/NiceGUI specialist, scoped to the applied fix
  found: If `set_session_from_url()` returns a truthy `user` but a falsy/`None` `session`
    (a malformed Supabase response — not impossible, GoTrue has returned partial payloads
    under load/error conditions before), the code silently skips writing `auth_session`
    via `safe_user_set`, yet still proceeds to `GlobalAuthState.set_auth(user, profile)`
    and returns `{'success': True, ...}`. `_handle_recovery_payload` then opens the
    "Set new password" dialog believing login succeeded; submitting calls
    `change_password()` (`web/supabase_client.py:657-660`), which reads
    `safe_user_get('auth_session')`, finds nothing, and returns `{'error': 'Not logged
    in'}` — a confusing dead end. This is the same CLASS of bug as the one being fixed
    (a flow that silently fails to reach its terminus), just in a narrower, rarer branch.
  implication: MUST-FIX before closing out. Everything else in the specialist's review
    checked out clean (script ordering vs analytics scripts verified via source + a
    regression test; no raw app.storage access; password fields properly masked; no
    unawaited-coroutine or dialog-lifecycle bugs; all 6+2 new tr() keys present with
    real Hebrew values in both languages).

## Eliminated

- hypothesis: The recovery link might be PKCE (`?code=` query param), making
  `/auth/callback`'s existing `code` handling a plausible near-fit / one-line fix.
  evidence: `reset_password_for_email` (gotrue-py) never sends a `code_challenge` — see
  Evidence entries above (library code inspection + external Supabase docs). PKCE only
  applies here if a code_challenge was registered at request time, which this call path
  never does. The `flow_type='pkce'` client default is a red herring — it only wires
  `sign_in_with_oauth`, not `reset_password_for_email`.
  timestamp: 2026-07-28T(session)

## Specialist Review

**Skill:** python (dispatched to `gsd-code-reviewer` as a Python/NiceGUI specialist
substitute — the literal `python-expert-best-practices-code-review` skill named in the
dispatch table is not registered as an invocable agent type in this environment; the
substitute was scoped identically: review only the fix's new/changed code, not the whole
repo).

**Verdict:** SUGGEST_CHANGE (one WARNING-level correctness gap; everything else verified
clean).

**Confirmed clean (checked directly against source, not assumed):**
1. `_RECOVERY_FRAGMENT_INTERCEPT_SCRIPT` is genuinely the first `ui.add_head_html()` call
   in `dashboard_page()` (`web/main.py:1895`), before `ANALYTICS_SCRIPT`/`POSTHOG_SCRIPT`
   (`web/main.py:2059-2060`). GA's loader is `<script async>` (network-fetched) and
   PostHog's init is deferred (`requestIdleCallback`/`setTimeout(2000)`) — both fire
   strictly after the synchronous inline scrub script runs during HTML parse, so
   `location.href`/pageview capture never sees the token fragment. Backed by a source-order
   regression test.
2. No raw `app.storage` access anywhere in the new code — only `safe_user_set`/
   `safe_user_pop` (Phase 87 chokepoint intact).
3. `new_pw`/`confirm_pw` both use `password=True` — masked independent of PostHog's
   `maskAllInputs`.
4. No unawaited-coroutine / dialog-lifecycle / bare-except bugs found; throwaway
   `create_client()` per `request_password_reset` call avoids shared-client races.
5. All 6 new (+2 more actually added: "Set Password", "Send reset link", "Please enter
   your email address", "If an account exists…") `tr()` keys present with real,
   non-identical Hebrew values.

**MUST-FIX (PATCHED + RE-VERIFIED):** `web/main.py:1113-1123`,
`_complete_password_recovery_session` — the `session` check now fails closed:
```python
session = result.get('session')
if not session:
    posthog_capture('password_recovery_failed', {'error_code': 'no_session_returned'})
    return {'error': 'no_session_returned'}

if not safe_user_set('auth_session', {
    'access_token': session.get('access_token'),
    'refresh_token': session.get('refresh_token'),
}):
    posthog_capture('password_recovery_failed', {'error_code': 'session_storage_unavailable'})
    return {'error': 'session_storage_unavailable'}
```
instead of proceeding to `set_auth`/`success: True` when `session` is falsy. Docstring's
`Returns:` reason-code list updated to include `no_session_returned`. Confirmed
`_handle_recovery_payload`'s existing generic `if 'error' in result: open_error_dialog();
return` already routes this new error code to the expired-link dialog with no additional
wiring needed (verified by a new end-to-end test, see below).

**Nice-to-have (INFO, not blocking):** `_render_password_recovery_handler()`
(`web/main.py:1185-1187`) builds a second, independent `create_forgot_password_dialog()`
instance on every `/` load, separate from the header's lazily-built one
(`web/auth_state.py:636`) — harmless duplicate DOM/dialog object, could be consolidated
later. Not applied in this pass (owner scoped this to a minimal shippable fix).

## Resolution

root_cause: |
  The password-reset flow is structurally incomplete on the web side, with two stacked
  defects that each independently produce the reported symptom:

  (1) `supabase_corrections_client.py::request_password_reset` (desktop) calls
      `client.auth.reset_password_for_email(email)` with no `redirect_to` and — more
      fundamentally — the underlying gotrue-py method never registers a PKCE
      `code_challenge` for this flow. Supabase Auth therefore issues an **implicit-flow**
      confirmation link: after the user clicks it, GoTrue verifies the token server-side
      and 302-redirects to the Site URL (default, since no `redirect_to` was supplied)
      with session tokens embedded in the URL **fragment**
      (`#access_token=...&refresh_token=...&type=recovery`), NOT the query string.

  (2) Because NiceGUI is server-rendered, the Python backend never sees URL fragments —
      only client-side JavaScript can read `window.location.hash`. No page in `web/`
      currently runs any such JS, and no route (`/auth/callback` included — verified,
      handles only Google OAuth's `?code=` query param) consumes a recovery token in any
      form. The consuming helper `set_session_from_url()` and its AST-guard allowlist
      entry already exist in `web/supabase_client.py`, clearly anticipating this exact
      scenario, but were never wired to a page — confirming the flow was left
      intentionally stubbed / never finished, not accidentally broken by a recent change.

  Net effect: the user lands on the plain homepage with an inert, unused
  `#access_token=...&type=recovery` sitting in the address bar that nobody looks at or
  acts on — exactly the reported "redirects me to the genizahsearch website ... nothing
  else happens."

  Dashboard read-back (owner, 2026-07-28) confirmed: Site URL = bare
  `https://genizahsearch.com`; Redirect URLs allowlist = only `/auth/callback`
  variants (so a `redirect_to=/reset-password` would have been silently downgraded
  to Site URL anyway — ruled out as a viable fix); Reset Password template =
  unmodified Supabase default `{{ .ConfirmationURL }}`. Owner selected Path A
  (web-only root-page fragment interception), explicitly deferring Path B
  (dashboard email-template edit to `token_hash`/`verify_otp`).

fix: |
  Path A implemented, web-only, one recovery code path for both apps:

  1. **`web/main.py`** — `_RECOVERY_FRAGMENT_INTERCEPT_SCRIPT` (a synchronous inline
     `<script>` emitted as the FIRST `ui.add_head_html(...)` call in `dashboard_page()`,
     i.e. before `ANALYTICS_SCRIPT`/`POSTHOG_SCRIPT`) reads `window.location.hash`,
     classifies it (recovery tokens / Supabase error fragment / ordinary load),
     scrubs it via `history.replaceState` SYNCHRONOUSLY (before GA/PostHog can ever
     observe the tokens via `location.href`), and stashes the classified payload in
     a transient `window.__genizahRecovery` global (never the URL/query string again).
     `_RECOVERY_PAYLOAD_READ_JS` + `ui.run_javascript` read it exactly once (NiceGUI's
     existing websocket channel) and delete it.
  2. `_complete_password_recovery_session(access_token, refresh_token)` (factored out
     mirroring the Phase 91 AUTHW-02 `_oauth_complete_login` pattern) calls the
     pre-existing, already-AST-allowlisted `set_session_from_url()` and persists the
     session through the `safe_storage` chokepoint only. **Post-specialist-review
     patch (RE-VERIFIED this pass):** now fails closed -- returns
     `{'error': 'no_session_returned'}` immediately if `set_session_from_url()` returns
     a falsy `session`, instead of silently skipping the `auth_session` write and
     proceeding to `set_auth`/`success: True` anyway. Docstring's reason-code list
     updated to match.
  3. `_handle_recovery_payload(payload, open_set_password_dialog, open_error_dialog)`
     dispatches to a "Set a new password" dialog (submits via the existing
     `change_password()` REST helper) on success, or a "This link has expired or was
     already used" dialog (REQUIRED path, not an edge case — recovery tokens are
     single-use) with a route back to a fresh reset request, on any failure
     (including the newly-added `no_session_returned` case).
  4. `web/supabase_client.py::request_password_reset(email)` — new function, sends
     the recovery email via a throwaway client with **no `redirect_to`** (matching
     the desktop caller exactly, so both origins land on the same consumer).
  5. `web/auth_state.py::create_forgot_password_dialog()` — the web app's first-ever
     "Forgot password?" entry point, wired into the login dialog.
  6. `genizah_translations.py` — 6 new bilingual `tr()` keys; reused 12 pre-existing
     keys (New Password / Confirm New Password / Passwords do not match / etc.).
  7. `docs/OPEN_ISSUES.md` — P1 entry marked fixed; Path B logged as an explicit
     deferred-follow-up callout.

  Path B (dashboard email-template edit + server-side `/auth/confirm` + `verify_otp`)
  NOT implemented, per owner instruction — logged in docs/OPEN_ISSUES.md as the
  recommended future SSR hardening.

verification: |
  Self-verified (mocked, no live environment in this pass):
    - 19 tests in tests/test_password_reset_recovery.py (17 original + 2 new added this
      pass), ALL PASSING: request_password_reset sends no redirect_to;
      _complete_password_recovery_session happy path (all 3 storage keys persisted) +
      session_exchange_failed error path + prune-race + defensive-3-key-rollback
      resilience (mirrors test_auth_callback_resilience.py T-B/T-E exactly);
      _handle_recovery_payload dispatch for recovery/error/ignore/malformed-payload
      cases; embedded-JS structural regression guards + head-html call-order guard;
      new tr() keys present + translated in both languages. **New this pass (specialist
      MUST-FIX regression coverage):**
      test_complete_password_recovery_session_missing_session_fails_closed (truthy
      `user` + falsy `session` -> `{'error': 'no_session_returned'}`, storage
      untouched, posthog `no_session_returned` event fired) and
      test_handle_recovery_payload_missing_session_routes_to_error_dialog (end-to-end:
      same falsy-session condition routes `_handle_recovery_payload` to the
      expired-link error dialog, confirming the generic `'error' in result` dispatch
      already covers the new error code with zero additional wiring). Full run:
      `python -m pytest tests/test_password_reset_recovery.py -v` -> 19 passed.
    - Full existing guard-test sweep RE-RUN and still green (35 passed):
      test_no_raw_storage_access.py (empty allowlist unaffected),
      test_no_set_session_outside_oauth.py (Class A/B AST guards unaffected — new code
      only calls the pre-allowlisted set_session_from_url helper, never set_session
      directly), test_auth_callback_resilience.py, test_auth_revocation_and_headers.py.
    - `python -m ruff check web/main.py tests/test_password_reset_recovery.py` ->
      All checks passed (re-run this pass on the two files touched by the
      specialist-review patch).
    - Python/NiceGUI specialist review (gsd-code-reviewer, scoped to this fix):
      script-order vs analytics scripts confirmed via source + regression test; no
      raw app.storage access; password fields properly masked; no unawaited-coroutine
      or dialog-lifecycle bugs; all tr() keys present bilingually. The one WARNING-level
      gap found (fail-open on falsy `session`) is now PATCHED and RE-VERIFIED (this
      pass) — `_complete_password_recovery_session` returns `{'error':
      'no_session_returned'}` immediately on a falsy `session`, before any
      `auth_session` write is attempted and before `set_auth`/`success: True` can ever
      be reached.
  NOT YET verified: an actual Supabase recovery email clicked in a real browser
  (requires the owner's live site — see the smoke-test steps below). This is the ONLY
  remaining verification step, which is why status is awaiting_human_verify, not
  resolved.

files_changed:
  - web/main.py
  - web/supabase_client.py
  - web/auth_state.py
  - genizah_translations.py
  - docs/OPEN_ISSUES.md
  - tests/test_password_reset_recovery.py (new)

## Smoke Test Steps (owner, after `deploy.sh master-main`)

**Success path:**
1. On the LIVE site, open the login dialog and click "Forgot password?" (new link) —
   OR trigger the desktop app's existing "Forgot password?" flow. Either sends the
   identical untouched email.
2. Open the email, click the reset link.
3. Expect: browser lands on `https://genizahsearch.com/` and, within roughly a second,
   a "Set a new password" dialog appears (no `#access_token=...` visible in the
   address bar — it should already be gone).
4. Enter a new password (8+ chars) twice, click "Set Password".
5. Expect: a "Password changed successfully" toast, then the page reloads and the
   header shows you logged in.
6. Confirm the NEW password logs into BOTH the web app and the desktop app.

**Expired/consumed-link path (recovery tokens are single-use):**
1. Click the SAME reset link a second time (or wait for it to expire), OR request a
   new one and let the first go stale.
2. Expect: browser lands on `/` and an "expired or already used" dialog appears
   (bilingual — check both EN and HE UI language settings), with a button that opens
   the "Forgot password?" dialog to request a fresh link.

**If either path does NOT show the expected dialog:** check the browser console for
a JS error (look for anything referencing `__genizahRecovery`) and report back —
that would mean Path A needs a follow-up fix, or Path B (dashboard email-template
edit) should be reconsidered.
</content>

---

## Codex external code review round (2026-07-28)

Brief: `_tmp/codex-pwreset-brief.md`. Full log: `_tmp/codex-pwreset-review.log`
(729 KB — critique is at the END; `tail` it, do not Read the whole file).
Invocation: `codex exec --dangerously-bypass-approvals-and-sandbox -C C:\Genizahsearch`
via the PowerShell tool, prompt piped on stdin (see the
`reference_codex_exec_stdin` / `feedback_review_workflow` memories).

**VERDICT: REJECT** — 1 BLOCKER, 1 HIGH, 2 MEDIUM. All addressed below.

### BLOCKER-1 (confirmed independently, then fixed) — the feature could never work

`handle_set_password` dispatched `change_password` through `run.io_bound`.
Decisive evidence, verified in the installed library source rather than taken on
trust:

- `nicegui/storage.py::Storage.user` reads `request_contextvar.get()` and raises
  `RuntimeError('app.storage.user can only be used within a UI context')` when unset.
- `nicegui/run.py::io_bound` -> `_run` -> `loop.run_in_executor(...)`, which does NOT
  propagate contextvars into the worker thread.
- `web/safe_storage.py:59-67` documents this precise case BY NAME ("inside a
  run.io_bound worker thread") and deliberately degrades it to `default`.
- Therefore `change_password` saw `auth_session == {}` -> `access_token is None` ->
  returned `{'error': 'Not logged in'}` on EVERY attempt.

Why it escaped the original tests: they exercise `_complete_password_recovery_session`
and `_handle_recovery_payload` as PURE HELPERS and never cross the
event-handler <-> storage boundary. Why the pre-existing profile flow was unaffected:
`web/pages/profile.py:153` calls `change_password` DIRECTLY on the event-loop thread.

User-visible severity had this shipped: the dialog would have appeared correctly,
accepted a password, and failed permanently — and because recovery tokens are
single-use, every retry burned another emailed link.

**Fix:** optional `access_token` parameter on `change_password` (default `None`, so
profile.py is unchanged); the recovery handler reads `auth_session` via
`safe_user_get` on the event-loop thread and passes the token into the worker.

### HIGH-2 (fixed) — payload destroyed before server acknowledgement

`_RECOVERY_PAYLOAD_READ_JS` returned and deleted the only browser copy, and the
fragment was already scrubbed, with no durable record that a recovery was underway.
A reload, tab close, websocket drop, or a `run_javascript` response arriving after
the 5s timeout left the user on a plain homepage with the fragment gone AND the
single-use email link spent — i.e. the exact silent dead end this session exists to
remove.

**Fix:** a durable non-secret `password_recovery_pending` boolean written through
`safe_user_set` once the session is established. Any later `/` render re-offers the
set-password dialog with no further token exchange (the session already exists
server-side). Cleared on success or explicit user Cancel so it cannot nag forever.

### MEDIUM-4 (fixed) — raw backend error text bypassed `tr()`

`status_label.text = result['error']` and `result.get('error') or tr(...)` rendered
English-only Supabase/httpx strings into the Hebrew UI (i18n invariant violation) and
exposed backend operational detail — throttle windows, SMTP failures — on an
ANONYMOUS form. Now mapped to the pre-existing translated keys
`Failed to send reset email` / `Failed to change password`, with the detail logged.

### MEDIUM-3 (logged, deliberately not fixed)

No app-side rate limit or CAPTCHA on the anonymous reset request. Codex assessed it
as email-nuisance / quota risk rather than account compromise (Supabase enforces a
documented 60s per-user reset window) and noted `web/api_hardening.py` does NOT apply
to a NiceGUI event handler. Recorded as a deferred follow-up row in
`docs/OPEN_ISSUES.md`; Turnstile/CAPTCHA is the recommended mitigation.

### Claims Codex independently CONFIRMED

- Head-ordering is genuinely sufficient: the interceptor runs synchronously before the
  parser reaches the async gtag tag or the PostHog stub; URL fragments are never sent
  in HTTP requests or referrers; replay does not record JS heap globals.
- Phase-87 `safe_storage` chokepoint preserved; no raw `app.storage.user` added.
- Phase-90 D-15 guard unchanged — `set_session` remains solely inside the
  already-allowlisted `set_session_from_url`.
- The lazy-build AST guard has real teeth (evadable only via aliasing, as with any
  name-based static guard).
- Cross-tab replay is impossible — window globals are tab-local.
- The 8-char client check matches `web/pages/profile.py`.

### Post-review state

26 tests in `tests/test_password_reset_recovery.py` (Group F = the review
regressions, including the event-handler/storage boundary the earlier tests could not
reach); 61 passing across the recovery + 4 auth guard suites; `ruff` clean;
`scripts/check_docs.py` all blocking checks pass.

**Added to the live smoke checklist by this round:**
- Confirm the password ACTUALLY CHANGES and logs in (BLOCKER-1 would have passed
  every earlier check right up to this step).
- Reload the tab mid-exchange, and force a >5s websocket disconnect, to exercise the
  `password_recovery_pending` resume path.
- Inspect GA/PostHog `$current_url` and a session replay for any fragment leakage.
- Check the hidden-anchor Quasar dialog portal, the password visibility toggles, RTL
  rendering in Hebrew, and mobile sizing — all UNVERIFIED until a real browser.
