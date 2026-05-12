# Handoff: v7.11.1 shipped + Path B multitenant refactor planning (2026-05-12)

## TL;DR

v7.11.1 shipped to production at **commit 242664d3**, closing 4
user-reported bugs from the v7.11.0 follow-up cycle:
- Cross-user xlsx export filename leak (the *original* user report)
- /help 500 (`set_visibility()` chained returning None)
- /browse 500 (pruned NiceGUI session AssertionError)
- Lists "Sync Now" UX confusion (rename + add Refresh from Cloud)

After deploying, four rounds of Codex code review on the post-release
commits (`22b45f68`, `aab16e6d`, `ed8a71ac`, `8ac93eff`, `cca23db3`)
surfaced the **deeper architectural problem behind the user-reported
bug**: GenizahSearch's web layer inherited a single-user mental model
from its desktop sibling. The cross-user export leak was one instance
of a class of bugs spanning auth caching, the `AppState` singleton,
`UserListsManager` cache, and raw `app.storage.user` access at dozens
of bootstrap sites.

The Codex-cycle commits on master-main are partial fixes that get
**superseded by Path B**, the multitenant architecture refactor scoped
in this handoff. The new session should start Path B and treat the 5
hold commits as load-bearing scaffolding to be folded into intentional
work, not as code to ship.

Decide milestone-creation strategy at session start (see "How to start
Path B" below).

## Production state (2026-05-12 ~18:30 UTC)

| Surface | State |
|---|---|
| Web (genizahsearch.com) | Live at **detached HEAD `242664d3`** (v7.11.1 tag); systemd unit running; smoke-tested OK on /help, /lists, /browse, /search, /parallels, and /api/browse full-enrichment path |
| Git tag | `v7.11.1` → `242664d3`, pushed to origin (no GitHub Release per web-only convention) |
| Server systemd override | `/etc/systemd/system/genizah-web.service.d/override.conf` adds `Environment="SEARCH_API_BROWSE_CORE_TIMEOUT=5.0"` (bumped from 2.0s default) — applies on next restart, NOT applied to the current process |
| `fjms_enrichment.db` | Unchanged from v7.11.0 (Phase 86 augmented, 1.59 GB) |
| `libraries.csv` | Unchanged from v7.11.0 (108 synthetic rows) |
| Desktop installer | Still v7.11.0 — v7.11.1 is web-only |
| GitHub Release v7.11.0 | Still has the original (pre-comments-scope-check-fix) installer attached |

## master-main vs. v7.11.1 — the 5 hold commits

`master-main` HEAD is `cca23db3`; production is `242664d3`. The 5 commits
on master-main beyond the deploy point are the Codex review cycle:

```
cca23db3 fix(safety): Codex 3rd-pass — auth resurrection, persist_value, more reads
8ac93eff fix(safety): address Codex review CRITICAL + HIGH on v7.11.0 hotfixes
ed8a71ac fix(ci): drop unused MagicMock / Optional imports from aab16e6d
aab16e6d fix(storage): safe wrapper for app.storage.user; migrate 5 raw read sites
22b45f68 fix(lists): key UserListsManager cache by user_id to stop cross-user leak
```

These were progressively-deeper attempts to patch the multitenant gaps
Codex surfaced. Each round's verdict was "recall" because each round
exposed a new instance of the same root cause. **Do not deploy these
commits as-is.** Instead, treat them as the audit-trail input for Path B.

The 4 Codex review prompts + responses are preserved at:
```
_tmp/codex_post_711_review_prompt.md           + .._response.txt
_tmp/codex_critical_high_review_prompt.md      + .._response.txt
_tmp/codex_3rdpass_review_prompt.md            + .._response.txt
_tmp/codex_4thpass_review_prompt.md            + .._response.txt
```

## Architectural diagnosis (the case for Path B)

GenizahSearch began as a PyQt6 desktop app. The web variant
(`web/main.py` + `web/pages/`) reuses much of the desktop's
single-user architecture. Symptoms surfaced over 4 Codex rounds:

| Site | Singleton-thinking artefact | Cross-user risk |
|---|---|---|
| `web/state.py` `AppState` | `last_results`, `current_search_query`, `last_selected_uids`, `parallels_results`, `parallels_filtered`, `parallels_search_meta` | Last-writer-wins across all users sharing process. Caused the user-reported xlsx filename leak. |
| `web/user_lists.py` `UserListsManager` | Instance is singleton via `AppState._user_lists_mgr`; `_cache` (now `_cache_entry` after 22b45f68) is per-instance | User A's lists served to User B inside 10s TTL window. Same class as the export leak but on lists. |
| `web/supabase_client.py` `_client_cache` + `_session_locks` | Process-wide dicts keyed first by `id(storage)` (object-id reuse after GC), then by `access_token` (rotates on refresh → multiple locks per session) | Cached authenticated client served to wrong user; refresh-token contention; auth resurrection after logout via captured `FilePersistentDict` |
| All pages | Raw `app.storage.user.get(...)` in 30+ bootstrap sites | Each can 500 the page on prune-mid-flight `AssertionError`. Patched at most sites via `safe_user_get` in commits 7-10 |
| `web/auth_state.py` `clear_auth`, `do_login`, OAuth callback | Multi-step writes across auth boundary without atomicity | Half-login state if session pruned mid-flight; logout sequence pops `auth_session` BEFORE evicting cached client (the post-cca23db3 cache-eviction is a no-op because tokens are gone) |
| `web/components/filter_panel.py` `persist_value` | Was raw read+write before commit cca23db3 | Same prune-mid-flight 500 risk on browse→search/parallels filter handoff |

Each new Codex round dug one layer deeper. The pattern won't terminate
under continued patching because the underlying assumption — "one user
per process" — is structural, not syntactic.

## Path B — proposed scope

The user agreed on a phased architectural fix as a new milestone (their
words: "Path B is what we need"). Scope sketch:

1. **State separation (highest priority).** Move all per-user fields off
   `AppState` into `app.storage.user`. Concretely:
   - Delete `AppState.last_results`, `current_search_query`,
     `current_search_mode`, `current_search_gap`, `last_filters_applied`,
     `last_search_warnings`, `last_selected_uids`, `parallels_results`,
     `parallels_filtered`, `parallels_search_meta`.
   - Migrate all writers (search.py, search_results.py, parallels.py) and
     readers (api.py export handlers) to read/write via
     `web/export_state.py` ONLY (no more singleton mirror).
   - Delete the `_TEST_BACKEND` shim from `web/export_state.py` once
     tests can rely on actual session storage (or a properly-scoped
     fixture).
   - Existing test files `tests/test_export_state_selection.py`,
     `tests/test_api_export_json.py`, `tests/test_api_legacy_unchanged.py`
     can drop their `state.*` setup and use only the export_state
     helpers.

2. **Lists cache redesign.**
   - Move `UserListsManager` off `AppState`. Either (a) instantiate
     per-request in the page handler, or (b) keep it on session storage
     keyed by user_id with proper invalidation. Option (a) is simpler;
     the 10s cache TTL was a perf optimization that's not load-bearing
     for normal use.
   - Drop `_cache_entry` if going per-request; drop the user_id-key
     plumbing.

3. **Auth: rip out `get_user_client()` caching entirely.**
   - Codex round 4's CRITICAL hinges on `FilePersistentDict` semantics
     and TOCTOU on the captured `storage` object. The cleanest fix is
     to stop caching authenticated clients.
   - Before deleting: verify `supabase.Client.auth.set_session(...)` is
     a local-only state mutation (no network call). If so, the cost of
     building a fresh client per request is negligible.
   - If `set_session` IS networked, alternative: store a single client
     per session in `app.storage.user['_supabase_client']` (NiceGUI may
     refuse to serialize a Client object — verify; if so, store
     `(access_token, refresh_token)` and reconstruct per-request).
   - Delete `_client_cache`, `_session_locks`, `_locks_guard`,
     `_CLIENT_CACHE_TTL` constants.

4. **Auth state writes — atomic + safe.**
   - Migrate `web/auth_state.py:set_auth/clear_auth/do_login` and OAuth
     callback in `web/main.py:1456+` to use `safe_user_get/set/pop`.
   - Sequence `clear_auth` to evict cached client BEFORE popping
     `auth_session`, so the eviction has a token to key on.
   - `sign_out` should call `client.auth.sign_out()` on the *user's*
     authenticated client (not the anonymous singleton) so the token
     is actually revoked server-side.

5. **Audit remaining bootstrap reads.**
   - Codex round 4 found `parallels.py:3520` and `text_editor.py`
     auto-save still raw-touching storage in deferred callbacks. Sweep
     `web/` once more for any remaining `app.storage.user.get/pop` and
     `app.storage.user[key] = ...` outside `safe_storage` helpers.
   - Decide: do we want a single chokepoint adapter that always wraps,
     or is the helper pattern sufficient?

6. **Stable session UUID.**
   - On first request to any page, mint a `session_uuid` and store in
     `app.storage.user['_session_uuid']`. Use this as the stable cache
     key wherever caching survives Path B. Tokens rotate; UUIDs don't.

### What to keep from the hold commits

| Commit | What survives Path B |
|---|---|
| `22b45f68` | Nothing — superseded by "no singleton cache" decision |
| `aab16e6d` | The `web/safe_storage.py` module + `safe_user_get/set/pop` helpers. Used extensively in Path B. |
| `ed8a71ac` | Nothing (cosmetic cleanup) |
| `8ac93eff` | `safe_user_get` migrations in search.py, parallels.py, filter_panel.py — keep. Atomic cache tuple in user_lists.py — discard if UserListsManager is going per-request. access_token-keyed client cache — discard. |
| `cca23db3` | persist_value safe-wrap — keep. More bootstrap-read migrations — keep. Auth-resurrection guard — discard since `get_user_client` is going away. |

### Test coverage to preserve

| File | Purpose | Keep? |
|---|---|---|
| `tests/test_export_cross_user_isolation.py` | 3 tests for the user-reported xlsx leak | YES — re-write to assert against per-session storage directly, no `_TEST_BACKEND` shim |
| `tests/test_user_lists_cache_isolation.py` | 3 tests for UserListsManager cross-user | RE-WRITE to match Path B's per-request model |
| `tests/test_safe_storage.py` | 6 tests for the helpers | YES — keep as-is |
| `tests/test_browse_state.py` (post-fix) | 7 tests including pruned-session AssertionError | YES |

## How to start Path B

Three reasonable entry points:

1. **`gsd-new-milestone v7.12 multitenant-architecture`** — creates a
   roadmap with phases mapped to the Path B work items above. Each
   phase becomes a plan-then-execute cycle. Highest structure; slowest
   start.

2. **`gsd-add-phase "Path B: multitenant architecture refactor"`** —
   appends to the current milestone instead of starting a new one.
   Lower ceremony but mixes architectural work with the v7.11.x
   release stream.

3. **Manual** — open a new branch, work directly without GSD scaffold,
   merge to master-main when complete. Lowest overhead; loses the GSD
   plan/verify guardrails.

**Recommendation:** option 1 (`/gsd-new-milestone`). The 4 Codex rounds
give the new milestone a clear requirements document already (one
phase per Path B work item).

## Open follow-ups carrying over from v7.11.0

From `.planning/HANDOFF_v7.11.0_followups.md`:

- **Bug 3 (lists sync)**: Web "Sync Now" UX clarified in v7.11.1 commit
  `9daa949c`. The underlying desktop-side issue (silent auto-sync
  failures with no user-visible status) is NOT fixed and remains open
  for a desktop installer cycle. Path B does not address it.
- **GitHub Release v7.11.0 installer**: Still has the pre-comments-fix
  binary. v7.11.1 is web-only so no new installer. Whenever the next
  desktop installer ships, it'll need to include both the comments fix
  (a372828e) and the lists-sync visibility work.

## Notes

**Note 1 (server systemd override applied 2026-05-12 ~18:55 UTC):**
`/etc/systemd/system/genizah-web.service.d/override.conf` adds
`Environment="SEARCH_API_BROWSE_CORE_TIMEOUT=5.0"` to mitigate the
cold-Tantivy-reader race that surfaced as `core_timeout` traces in
`journalctl` immediately after the v7.11.1 restart. `daemon-reload`
was applied but the service was NOT restarted (would have created a
new cold-start window). The 5s timeout takes effect on the next
restart, whenever that happens.

**Note 2 (server git state):** Server is on detached HEAD at the
`v7.11.1` tag. `deploy.sh` does `git pull` on `master-main` — if you
re-run `deploy.sh` it WILL move the server forward to master-main HEAD
(currently `cca23db3`, the partial-fix commits Codex flagged as
recall-grade). Either skip `deploy.sh` until Path B lands, OR
explicitly `git checkout master-main` on the server first if you
intend to deploy further work.

**Note 3 (Codex review pattern proven valuable):** This cycle showed
Codex review catching real bugs that internal review missed for at
least 3 rounds. Worth reusing as a standard pre-deploy gate for any
auth/multi-tenant work.

**Note 4 (test count baseline):** 1862 passed / 20 skipped at HEAD
(`cca23db3`). 1858 at `242664d3` (deploy target). Difference is the
test files added by the hold commits.

## Quick re-orientation commands for next session

```bash
# Confirm v7.11.1 still live
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com \
  "cd GenizahSearch && git rev-parse --short HEAD && sudo systemctl is-active genizah-web"
# Expected: 242664d3 + active

# Verify smoke path
curl -s "https://genizahsearch.com/api/browse?sys_id=990051753360205171&p_num=1&fl_id=164978060" \
  | python -c "import json,sys; d=json.load(sys.stdin); print(d.get('shelfmark'), '/', (d.get('metadata',{}).get('pgp') or {}).get('description','')[:50])"

# Read the 4 Codex review transcripts
ls _tmp/codex_*_response.txt

# Check the 5 hold commits
git log --oneline 242664d3..cca23db3

# Quick "is master-main still as expected" check
git rev-parse --short master-main
# Expected: cca23db3
```
