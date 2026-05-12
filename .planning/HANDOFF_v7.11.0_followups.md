# Handoff: v7.11.0 follow-ups (2026-05-12)

## TL;DR

v7.11.0 milestone (Phase 84 + 85 + 86) shipped today and is live on production.
Two bugs surfaced AFTER the release and have been fixed locally + pushed to
GitHub + deployed to production, but the **GitHub Release v7.11.0 still has
the original (buggy) installer attached** and **one user-reported issue
(lists-sync) has not been investigated yet**.

Decide release strategy at session start (see "Release Strategy Options" below).

## Production state (2026-05-12 ~12:08 UTC)

| Surface | State |
|---|---|
| Web (genizahsearch.com) | Live at commit `a372828e` (server on `master-main` branch, just fast-forwarded from a stale `deploy/memory-leak-fix-20260430` branch — see "Note 1" below) |
| `fjms_enrichment.db` | Server has the Phase 86 augmented DB (3,264 synthetic rows, 1.59 GB, mtime 2026-05-12 10:14 UTC). Pre-Phase-86 backup at `fjms_enrichment.db.pre-phase86-20260512` for rollback |
| `libraries.csv` | Phase 86 synthetic block deployed (108 rows) |
| GitHub Release `v7.11.0` | https://github.com/gershuni/GenizahSearch/releases/tag/v7.11.0 — published 2026-05-12, installer `GenizahSearchPro_V7.11.0_Setup.exe` (276 MB) **attached BUT compiled before the comments_scope_check fix** |
| Desktop installer in `dist/` | Same buggy version. `_tmp/build_v7.11.0.log` has the PyInstaller build log |

## Commits since v7.11.0 release (in push order)

1. `aff67c8d` `fix(browse-86): desktop pagination for synthetic sys_ids` — in v7.11.0 release
2. `ca573ee1` `release(7.11.0): CUDL Coverage & Synthetic Inventories milestone` — version bump + CHANGELOG (the release commit)
3. `ac5492fa` `fix(ci): bump test_release_artifacts.py target to 7.11.0 + make it version-agnostic` — CI greening, post-release
4. `24bb4644` `fix(browse): wrap detached-task JS dispatch + extend slot-race guard` — **bug #1 fix, post-release**
5. `a372828e` `fix(desktop-comments): scope mapping for comments table check constraint` — **bug #2 fix, post-release**

All pushed to `origin/master-main`.

## Bug 1: Web — metadata panel missing PGP/FJMS sections — FIXED

**Symptom:** On many real (non-synthetic) manuscripts on production, clicking
"SHOW METADATA" showed only the basic NLI header fields (Library, Shelfmark,
System ID, Title, Pages, FL ID, NLI Ktiv link, Related Fragments) — but the
PGP description, FJMS catalog records, bibliography chips, measurement chip,
and visual-similarity chip were all absent. Localhost showed everything fine.

**Root cause:** `load_page()` runs inside `asyncio.ensure_future(...)` (detached
task). During Phase A it dispatches a few `ui.run_javascript(...)` calls
(manuscript-viewer adjust reset; `window.GENIZAH_IS_SYNTHETIC` flag). When the
NiceGUI client slot has churned (rapid navigation, hard refresh after stale ws),
the JS dispatcher raises an exception. That exception propagated to load_page's
try/except, set `state.error`, and Phase B (`load_enrichment`) was skipped
because of the `not state.error` gate — so `state.pgp_metadata` and
`state.fjms_data` stayed None.

**Fix (commit `24bb4644`):** Codex-supplied. Added `_run_page_javascript(js)`
helper that wraps `_page_client.run_javascript()` in try/except. Routed 3
detached-task JS callsites through it. Also extended the slot-lifecycle race
exception filter to catch `'current slot cannot be determined'` variant.

**Verified:** print()-based diagnostic confirmed `load_enrichment ENTER` was
never being called pre-fix on prod for sys_id=990051753360205171. After fix +
service restart, panel renders all expected sections.

## Bug 2: Desktop — comments_scope_check violation on save — FIXED (code)

**Symptom:** Posting a comment with type "Problem" / "Suggestion" / "Question"
/ "Scholarly Note" / "Issue Report" raised
`23514 comments_scope_check` from Supabase. Only "General Comment" worked.

**Root cause:** Desktop CommentDialog has a categorical "Type" dropdown
(general/question/scholarly_note/suggestion/issue). The client was passing
that string straight as `comments.scope`, but the column is CHECK-constrained
to ('page', 'manuscript', 'general'). Only 'general' overlapped. Worse, the
dropdown selection was silently discarded on read even when it didn't violate
the constraint — `_parse_comment` reads DB `scope` back into the misleadingly-
named `comment_type` field — so the category never had real persistence.

**Fix (commit `a372828e`):** Mirror the web client's behavior. Derive `scope`
from `page_number` presence (`'page'` if set, else `'manuscript'`). Dropdown
left in place; its value is silently discarded on save (no regression vs.
pre-fix behavior — never persisted before). Real category persistence is a
Phase 87 follow-up.

**Test coverage:** 12 new regression tests in
`tests/test_supabase_corrections_client.py`:
- For each of 5 dropdown values, assert insert payload's `scope` is in valid
  set and that no category-flavored value leaks into scope
- `scope='page'` with page_number set; `scope='manuscript'` without

**Codex review concurred** with this approach. See `_tmp/codex_comments_scope_prompt.md`
+ Codex output for full reasoning. Phase 87 work logged in OPEN_ISSUES.md.

**Still pending for this bug:** desktop installer is NOT rebuilt with the fix
yet. Live desktop users who installed v7.11.0 still hit this bug. See "Release
Strategy Options" below.

## Bug 3: Desktop → Web list sync — NOT INVESTIGATED

**User report (Hebrew):** "יצרתי רשימות בתכנה שבמחשב ואז כשנכנסתי לאתר (בעודי
מחובר בחשבון באתר ובתכנה, ועם קליטה אינטרנטית טובה) וניסיתי לסנכרן רשימות
(הכפתור 'סנכרן עכשיו'), האתר לא סנכרן בפועל מהתכנה."

Translation: created lists in the desktop app while logged in. Opened the web
(same account, good connection). Clicked "Sync Now" button. Web didn't show
the desktop lists.

**Initial findings before pivot to bug #1 investigation:**
- The web "Sync Now" button at `web/pages/lists.py:676` and `:722` is wired to
  `show_migration_dialog` → `state.lists_mgr.migrate_local_to_user()`. This
  migrates *browser-localStorage* anonymous lists into the logged-in account.
  It does NOT pull lists FROM the cloud.
- Web is supposed to read lists DIRECTLY from Supabase on page load (no manual
  pull needed).
- Desktop auto-syncs to cloud after list changes via `_lists_auto_sync()` at
  `genizah_app.py:11610`. Called from 14 sites in genizah_app.py (post-create,
  post-add-item, etc.). Has 2-second debounce + 30-second timeout + network
  check.
- Question 1: did desktop actually push the user's new lists to Supabase?
  (Possible: auto-sync silent-failed, e.g. on network check or quota.)
- Question 2: if pushed, why isn't the web showing them on next page load?

**Things to check next session:**
1. Query Supabase `user_lists` table for the user's `user_id` — did desktop
   push lists? Compare against what desktop has locally.
2. Inspect desktop logs for `_lists_auto_sync` warnings/failures (search
   `logger.debug("Auto-sync ...")`).
3. Verify web's `state.lists_mgr` is reading from Supabase, not local-only
   mode. The `cloud_done` vs `cloud_off` icon next to "Personal Lists" header
   at `web/pages/lists.py:686-691` indicates auth state.
4. The "Sync Now" button is misleadingly named — it only migrates browser-local
   to cloud (one-way, anonymous→account). Consider renaming or adding a
   separate "Refresh from cloud" button.

## Release Strategy Options

Pick at session start:

### Option A: Defer fixes to v7.11.1 patch release later
- Leave v7.11.0 GitHub Release as-is (with buggy comments behavior)
- Web is already fixed (bug #1 fix deployed via direct file restart)
- Desktop users on v7.11.0 hit bug #2 until they install v7.11.1
- Investigate bug #3 (lists sync); fold it into v7.11.1
- Bump version → 7.11.1, build new installer, new GitHub Release

### Option B: Re-cut v7.11.0 (delete current release + recreate)
- Reword the v7.11.0 release notes to mention "+ post-release hotfixes for
  comment save + browse Phase B"
- Delete GitHub Release tag v7.11.0
- Rebuild desktop installer (PyInstaller + Inno Setup)
- Recreate v7.11.0 release with same tag + new installer
- Investigate bug #3 next session (separate cycle)
- Risk: rewriting a published release is unconventional; users who already
  installed v7.11.0 still have the buggy version unless they reinstall

### Recommendation
**Option A.** v7.11.0 is already out the door. Treating the post-release
hotfixes as a v7.11.1 follow-up is more honest about what shipped when. It
also lets us bundle bug #3 (lists sync) — which we haven't investigated yet —
into the same patch, avoiding two desktop installer builds in 24 hours.

## Open tasks at session start

Use these as a starting checklist. Mark in_progress as you start work.

- [ ] **Pending: Phase 86 verification + completion** (Task #6 from previous
      session) — run `gsd-verifier`, code review, mark phase complete in
      ROADMAP/STATE/REQUIREMENTS. Lower priority than user-facing bugs.
- [ ] **Bug 3: lists sync** — investigate per "Things to check next session"
      above. Probably need user to ssh to their machine OR run a script that
      reads the desktop's `lists.json` (or wherever the desktop persists
      pickled state) and compares against Supabase.
- [ ] **Decide release strategy** (A vs B). Then either:
  - (A) Build v7.11.1 once bug #3 is also fixed
  - (B) Re-cut v7.11.0 now (after deleting current release)
- [ ] **Rebuild desktop installer** (when ready): `python -m PyInstaller ...`
      (see `build_app.bat` or my command in this session's history at
      `_tmp/build_v7.11.0.log`). Then ISCC compile of
      `CompileScriptGenizah.iss`. The .iss currently has `MyAppVersion=7.11.0`
      — bump to 7.11.1 if going Option A.

## Notes

**Note 1 (server git branch state):** Until 2026-05-12 ~12:00 UTC, the server's
working git was on branch `deploy/memory-leak-fix-20260430` (an old leak-fix
investigation branch) — NOT `master-main`. The branch had been fast-forwarding
to track new commits on master-main during deploys, but `git rev-parse HEAD`
was returning the right commit while `git branch --show-current` would have
shown the wrong name. This session ended after I switched to `master-main` and
pulled 354 commits to fast-forward. The next `./deploy.sh` will work on the
correct branch.

**Note 2 (server fjms_enrichment.db rollback path):** The pre-Phase-86 DB is
preserved on server as `fjms_enrichment.db.pre-phase86-20260512` (1.61 GB).
To roll back Phase 86's synthetic injection: stop service, mv current → .new,
mv .pre-phase86 → main, start service. Don't do this casually — synthetic
rows in libraries.csv would then point at AlmaIds that don't exist in catalog.

**Note 3 (deploy-DB-sync memory now codified):** The 2026-05-11 incident where
deploying code without DB sync caused catalog/PGP/bib data loss is now a
codified pattern (memory entry `feedback_deploy_db_sync.md`). When this
session ran `scp fjms_enrichment.db` BEFORE `git push`, the deploy was clean.

**Note 4 (Codex review pattern for desktop comments fix):** This session
explicitly consulted Codex via stdin pipe (`cat prompt.md | codex exec
--skip-git-repo-check`) and got a concrete recommendation that matched
the proposed plan, plus an additive regression-test ask. Pattern worth
reusing for ambiguous design decisions during release sequences.

## Quick re-orientation commands for next session

```bash
# Where am I?
git log --oneline -8 origin/master-main

# Server state
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd GenizahSearch && git rev-parse --short HEAD && sudo systemctl is-active genizah-web"

# Live API smoke test (should return shelfmark + fjms_sources + pgp description)
curl -s "https://genizahsearch.com/api/browse?sys_id=990051753360205171&p_num=1&fl_id=164978060" | python -c "import json,sys; d=json.load(sys.stdin); print('shelfmark:',d['shelfmark']); print('fjms:',d['metadata']['fjms']['source_names']); print('pgp:',(d['metadata']['pgp'] or {}).get('description','')[:80])"

# Verify regression tests still green
python -m pytest tests/test_supabase_corrections_client.py tests/test_browse_synthetic.py -q
```
