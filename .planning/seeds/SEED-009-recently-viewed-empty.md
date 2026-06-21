---
id: SEED-009
status: dormant
planted: 2026-06-21
planted_during: v8.2.0 / Phase 120 (Actions & Persistence) live UAT
trigger_when: Standalone — app-wide (NOT Joins Lab). Triage on its own branch off origin, not inline during a phase. Investigate whether it shares the off-loop-auth root cause fixed in Phase 120 before scoping.
scope: small-to-medium (own investigation; likely a lists_mgr load / auth-context issue)
---

> **ROUTING:** Surfaced during the 2026-06-21 live UAT of the Web Joins Lab, but it is a GLOBAL issue
> in the `lists_mgr` "recent" system list — entirely separate from Phase 120 (Joins Lab Actions &
> Persistence). Per the mid-phase-fix preference, captured as a seed rather than fixed inline so the
> Phase-120 checkpoint stays focused. Run later on its own branch.

# SEED-009: "Recently Viewed" is empty (app-wide)

> Captured as a seed (NOT implemented inline). User reported it empty during Phase-120 UAT.

## Symptom (observed 2026-06-21)

The "Recently Viewed" list renders empty even after viewing documents.

## What it is (NOT Joins Lab)

"Recently Viewed" is a `lists_mgr` **system list**, distinct from the Joins Lab feature surface:
- Read via `state.lists_mgr.get_items_in_list_sync('recent')` / `recent_items`.
- Tracked on document view at `web/pages/browse.py:836`.
- Rendered in the project tree / discoveries surfaces.

## Hypotheses to investigate (start here)

1. **Same off-loop-auth root cause as Phase 120?** If `lists_mgr` loads/writes the `'recent'` system list
   from Supabase inside a `run.io_bound` worker thread, it would hit the SAME bug fixed in Phase 120
   (`14198955`): NiceGUI `run.io_bound` runs without the request context, so `app.storage.user` is
   unavailable → `get_user_client()` falls back to the ANON client → RLS returns 0 rows / denies writes.
   Check whether the view-tracking write (`browse.py:836`) and the read both run on the event loop.
   The established fix pattern: run authenticated Supabase calls ON the event loop (see `create_correction`).
2. **Local vs cloud:** Is "recent" a cloud-backed list or local state? If cloud, RLS/grants + auth context;
   if local, check the tracking-on-view path actually fires and persists.
3. **System-list provisioning:** Does the `'recent'` system list exist / get created for the current user?

## Pointers

- `web/pages/browse.py:836` (view tracking), `lists_mgr.get_items_in_list_sync('recent')` (read).
- Off-loop-auth root cause + fix precedent: Phase 120 commit `14198955`; `web/supabase_client.py::get_user_client`
  (reads `safe_user_get('auth_session')` → contextvar-scoped `app.storage.user`, unavailable in thread pool).
- Compare to the working on-loop authed pattern: `create_correction(...)` in `web/pages/browse.py` /
  `web/pages/search_results.py`.

## Notes

- App-wide; affects users beyond the Joins Lab. Worth a quick check on `/lists` and the discoveries panel too.
- If it IS the off-loop-auth class, the fix is mechanical (move the authed lists_mgr calls on-loop) and the
  Phase-120 regression test pattern (`test_authenticated_list_calls_run_on_event_loop`) can be extended.
