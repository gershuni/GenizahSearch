# Phase 92 SWEEP-05 -- Cross-User Concurrent Smoke Test

**Plan:** 92-01 commits this scaffold (unchecked). Plan 92-02 is GATED on a follow-up commit by Hillel that checks all boxes with `Overall: PASS`.

**Tester:** Hillel Gershuni
**Date run:** _________ (Hillel fills this in)
**Server commit at test time:** _________ (Hillel fills this in -- `git rev-parse --short HEAD`)

## Setup

**Server start (manual, human-controlled):** Per `feedback_no_background_webserver.md`, do NOT run the web server from Bash -- it creates unkillable zombie processes on Windows. Start the server in a dedicated PowerShell terminal Hillel controls:

```powershell
cd C:\Genizahsearch
python -m web.main
```

Wait for `NiceGUI ready on http://localhost:8080` (or 8081).

**Test accounts:**
- User A: `gershuni+a@gmail.com`
- User B: `gershuni+b@gmail.com`

**Browser setup:**
- Browser 1: Chrome regular window for User A
- Browser 2: Firefox OR Chrome incognito window for User B (different `session` cookie -- distinct `_session_uuid`)

**Pre-flight check:** Confirm SWEEP-01 verdict on Surface 4 (joins.db) BEFORE deciding R3 path:
- If `92-SWEEP-01-AUDIT.md` Surface 4 verdict is `N/A community-share` (the expected case): mark R3 as `N/A` and skip the puzzle-write subscenario.
- If Surface 4 found per-user joins.db state (unexpected): R3 must be exercised; skip the N/A line.

## R0 -- Baseline cross-user isolation

- [ ] User A logs in (account: gershuni+a@gmail.com)
- [ ] User B logs in in second browser (account: gershuni+b@gmail.com), confirms different `_session_uuid` from A (DevTools -> Application -> Storage; OR inspect `.nicegui/storage-user-*.json` -- two distinct files with distinct 32-char hex UUIDs)
- [ ] A and B run different searches concurrently
- [ ] Each navigates to browse on a hit
- [ ] Each adds the hit to a personal list
- [ ] Each downloads xlsx of search results
- [ ] A's xlsx contains only A's results (sample 5 rows + filename)
- [ ] B's xlsx contains only B's results (sample 5 rows + filename)
- [ ] A's `/lists` page shows only A's lists
- [ ] B's `/lists` page shows only B's lists
- [ ] Evidence column (screenshots / paste of first 5 rows): _________

## R1 -- Logout-mid-flight race (Phase 90 throwaway.auth.admin.sign_out validation)

- [ ] B starts an xlsx download (heavy search, hold the export)
- [ ] A clicks Logout while B's export is in flight
- [ ] B's xlsx completes normally and contains only B's data
- [ ] B's session is unaffected (token refresh still works after; B can run another search)
- [ ] A's logout actually revoked server-side (verify by attempting an authenticated API call with A's old access_token -- should 401)
- [ ] Evidence column: _________

## R2 -- Token refresh race (Phase 90 _session_uuid lock validation)

**Why R2 matters:** Tests Phase 90's per-`_session_uuid` refresh-lock keying. If R2 were keyed on access tokens instead, B's xlsx download could be serialized behind A's refresh (cross-user lock contention). The expected behavior: A's refresh and B's xlsx run in parallel because they live under distinct `_session_uuid` keys.

**Setup (pick ONE of these deterministic paths -- per Codex CM4):**

- [ ] **Path A -- wait for natural expiry (slow but deterministic):** Login as User A, then leave the tab idle until A's `expires_at` in browser localStorage `sb-<project>-auth-token` is within 60s of expiry (default Supabase JWT TTL is 60 minutes per Supabase dashboard `Auth -> Settings -> JWT expiry`). Confirm in DevTools -> Application -> Local Storage before continuing.
- [ ] **Path B -- manual JWT tamper (RECOMMENDED -- fastest deterministic):** Open browser DevTools -> Application -> Cookies (or Local Storage) -> find `sb-<project>-auth-token` -> decode the JWT payload at jwt.io -> confirm `exp` is in the near future -> EITHER set the browser system clock forward by `(exp - now + 30s)` OR temporarily edit the stored `expires_at` value to a past timestamp. The next request after that change triggers the Supabase client refresh path.
- [ ] **Path C -- code-side TTL override (fallback if A or B both fail):** Temporarily set the Supabase JWT TTL env var (if supported in the dev config) to 60 seconds and restart the app; login again; the next request after 60s will refresh.

**Execution steps (run AFTER one of the setup paths above):**

- [ ] A's tab is heavily interacting with /parallels (longer requests) while access_token expires mid-flight (the chosen setup path has triggered refresh-readiness)
- [ ] B is downloading xlsx in parallel
- [ ] A's requests succeed (refresh fires; B is unaffected)
- [ ] B's xlsx completes with only B's data
- [ ] No "set_session() called mid-flight" log warnings on the server (Phase 90 AUTHC-02 invariant)
- [ ] Both users' `_session_uuid` values remain stable across the refresh (refresh does NOT mint a new UUID -- confirm via `.nicegui/storage-user-*.json` is unchanged AND/OR via `get_session_uuid()` debug log)
- [ ] Setup path used (mark which): _________
- [ ] Evidence column (paste of A's network panel + B's xlsx filename): _________

## R3 -- Concurrent puzzle write (CONDITIONAL -- only if SWEEP-01 reveals per-user puzzle ownership in joins.db)

- [ ] Skip if SWEEP-01 confirmed joins.db is community-share with no per-user columns (expected case per D-04 -- mark this scenario `N/A`)
- [ ] Otherwise: A and B both add/edit puzzle documents at /puzzle simultaneously
- [ ] Each sees only their own changes
- [ ] Evidence column: _________

## Final disposition

- [ ] R0 PASS / FAIL: __________
- [ ] R1 PASS / FAIL: __________
- [ ] R2 PASS / FAIL: __________
- [ ] R3 PASS / FAIL / N/A: __________
- [ ] Overall: PASS / FAIL -- __________
- [ ] Tester: Hillel Gershuni, date: __________

---

*Plan 92-02 cannot start until this file is committed with all checkboxes checked AND `Overall: PASS` is recorded. Plan 92-02 Task 0 pre-flight gate verifies this state.*

*Source: D-08 in `.planning/phases/92-final-sweep-and-acceptance/92-CONTEXT.md`*
