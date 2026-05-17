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

---

## Smoke run 1 -- 2026-05-17 -- FAILED at R0

**Tester:** Hillel Gershuni
**Server commit at test time:** `3f6919bb` (master-main, post Plan 92-01 closeout commit)
**Verdict:** Overall FAIL -- smoke aborted at R0 before R1/R2/R3 could be exercised.

**Symptoms observed (3 distinct):**

1. **Logged-in main user: existing lists not fetched.** User logs in to his main account; the `/lists` page shows no lists despite the account having pre-existing lists in Supabase.
2. **New list creation from `/lists` page silently fails.** From a freshly registered user account, clicking "New list" (or "New project") in the lists module shows a positive toast "list/project added" but the list/project does NOT actually persist (does not appear in the page; not in DB on inspection). No console error logged in this path.
3. **New list from search-results path errors in console.** Path: search results -> "Add to list" -> "Create new list" -> Save. Browser console emits a single error: `safe_user_get('auth_session') unexpected failure: app.storage.user can only be used within a UI context`. The new list does not get added.

**Root cause diagnosed in this session (P0 regression):**

Phase 90's "singleton-anonymous-only" invariant (D-09 / D-10) closed a multitenant leak by routing all five bootstrap helpers (`sign_in` / `sign_up` / `set_session_from_url` / `exchange_code_for_session` / `get_oauth_url`) through throwaway clients, so the module-level singleton `_client` returned by `get_client()` is no longer auto-authenticated by the Supabase SIGNED_IN event listener after login.

But Phase 90 did **not** migrate the ~13 reader functions in `web/supabase_client.py` that still use `get_client()` (anonymous) instead of `get_user_client()`:
- `get_profile` (line 711) -- masked because `profiles` SELECT RLS is `TO public`
- `get_user_lists` (line 752) -- **broken**: RLS `TO authenticated USING (auth.uid()=user_id)`, anon role gets 0 rows
- `get_deleted_lists` (line 783)
- `get_list_items` (line 899)
- recent-items reader (line 958)
- `get_projects` (line 1004)
- project list-items reader (line 1067)
- Lines 1171, 1191, 1243, 1326, 1461, 1579 (other readers, all `TO authenticated` per `docs/guides/SUPABASE_GUIDE.md:430-451`)

**Smoking gun in Phase 90 planning artifacts:**
`.planning/phases/90-auth-caching-rewrite-no-set-session/90-DISCUSSION-LOG.md:147` records the false assumption:
> "Existing callers already check `is_logged_in()` before doing user-scoped operations. Keeping the fallback preserves the read-paths where the anonymous client is the correct choice (`get_user_lists` reads work anonymously; only writes need auth)."

This is wrong -- `user_lists` SELECT policy is `TO authenticated`, so the anon role gets zero rows. Before Phase 90, the Supabase SIGNED_IN event listener auto-authenticated the singleton so reads worked despite using `get_client()`; Phase 90 closed that channel for the (correct) multitenant-safety reason but missed the reader migration.

**Why no gate caught this:**
- Phase 87 lint scanner only checks raw `app.storage.user` access -- not RLS reachability
- Phase 92-01 SWEEP-01 AST scan checks the same invariant -- correctly verified zero violations
- Phase 90 tests likely mocked Supabase or exercised write paths only (no end-to-end SELECT-RLS coverage)
- No live cross-user smoke test was run between Phase 90 ship (2026-05-14) and this Phase 92-01 closeout
- Phase 92-01 SWEEP-04 transcript audit's claim "all 23 Codex findings addressed" is now incomplete -- this regression escaped the cross-AI review window

**Disposition:**
- **Smoke = FAIL.** Plan 92-02 cannot run (D-02 gate intact).
- **v7.12 milestone NOT shippable.** `deploy.sh` stays blocked.
- **Plan 92-01 artifacts remain valid** -- the AST scan / grep evidence / surface audit / transcript audit are correct as far as they cover. SWEEP-05 is correctly recorded as FAIL.
- **Phase 92.1 (Reader-Client Retrofit) inserted 2026-05-17** to migrate the ~13 reader functions in `supabase_client.py` from `get_client()` to `get_user_client()`, add a regression test suite, and trace + fix the secondary symptom 3 (`safe_user_get('auth_session') ... UI context` console error in the add-to-list-dialog post-create path). Directory `.planning/phases/92.1-reader-client-retrofit/` created via `gsd-sdk query phase.insert`; ROADMAP.md Backlog entry has `(INSERTED)` marker.
- After Phase 92.1 ships and passes its own UAT, redo this smoke (Smoke run 2) -- if PASS, then Plan 92-02 closeout docs proceed.
- See `docs/OPEN_ISSUES.md` P1 section for the formal bug entry (added 2026-05-17).
