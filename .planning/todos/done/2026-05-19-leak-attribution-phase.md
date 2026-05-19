---
title: "Attribute the secondary web memory leak (~411 MB/hr) and ship a fix"
created: 2026-05-19
closed: 2026-05-19
area: web
priority: high
status: done
status_note: "Closed 2026-05-19 PM after SEED-002 fix deploy + production memstat/objgraph/backref investigation. Attribution path identified, evidence captured, future-phase fix sketched. See verdict block below."
source: P1 web memory leak re-opened 2026-05-19 -- 11h soak verdict warning band (411 MB/hr)
predecessor: .planning/todos/done/2026-05-18-verify-memstat-after-export-cap-fix.md
predecessor_outcome: "Cap fix (commit f2e456d4) closed the export_search_payload row-count surface (498 MB -> 512 KB live payload; 906 MB -> 4.5 MB top file on disk) but the row-FIELD surface was still leaking: each row carried multi-MB full_text / raw_file_hl / content fields, so 35 rows × MB each still ballooned per-session storage. Codex CLI diagnosed and patched this in commit ed6f89c4."
attribution: ".planning/quick/260519-hoi-investigate-framework-retention/INVESTIGATION.md (full open-log) + PROPOSED-FIX.md (future-phase workaround sketch)"
verdict: "The residual ~200 MB per heavy UI search is allocator high-water mark from NiceGUI ObservableDict wrapping work on the storage write path, NOT a logical leak. Decisive proof: 20 heavy API calls (same Tantivy scan, same regex work, NO user-storage persistence) grew RSS by only 34 MB total (1.7 MB/call) vs ~200 MB/call via UI = 117x difference. ObservableDict count is stable at ~44K (legitimate working set), only bytes grow (pymalloc holds pages once allocated). SEED-002 is doing its job (per-row payload 22 KB -> 1.6 KB, capped at 5000 rows). Operationally acceptable with periodic restart; future-phase workaround is to store the payload as a JSON-encoded string to bypass NiceGUI's auto-wrapping entirely."
---

# Attribute the secondary web memory leak and ship a fix

## STATUS: PROVISIONALLY SUPERSEDED (2026-05-19)

**A second fix was shipped before this attribution work began.** Commit
`ed6f89c4` (authored by Codex CLI, applied to master-main by Claude) ships
row-level stripping of `full_text` / `raw_file_hl` / `content` fields at write
time in `web/export_state.py` -- the missing half of the 2026-05-18 count cap.
Diagnosis: capping rows alone could not bound multi-MB transcripts × few rows;
35 results carrying full manuscript text could still write hundreds of MB per
session. The fix also adds:

- Lazy Tantivy rehydration in `web/export_service.py:_resolve_result_full_text`
  -- Excel exports still get full text via `searcher.get_full_text_by_id(uid)`
  at download time, not from session storage.
- `app.on_startup` hook `compact_export_storage_on_startup` in `web/main.py`
  rewriting legacy oversized `.nicegui/storage-user-*.json` files in-place via
  `tmp + os.replace`.
- `top_keys` per user payload in `web/storage_diagnostics.py` so post-deploy we
  can directly observe whether the export payloads are still dominant.

Tests: 2051 passed / 20 skipped / 2 xfailed / 0 failures in the full pytest
suite (3m 56s). Ruff clean on all 8 source files.

### Awaiting verification

Pull `/_internal/memstat` after >=2h uptime under real traffic. Success criteria:

1. `export_search_payload` / `export_parallels_payload` no longer appear as
   dominant `top_keys` in any user payload.
2. RSS growth rate < 30 MB/hr (band-1 threshold from the predecessor todo).

**If both pass:** this todo CLOSES (move to `.planning/todos/done/` with a
verdict note; flip the OPEN_ISSUES P1 row to ✅ Fixed (2026-05-19, ed6f89c4);
decrement P1 Open 1->0 / Total Open 34->33). The attribution work below is no
longer needed.

**If either fails:** the field-strip fix was also insufficient and the
attribution work below RE-ACTIVATES. Proceed to Plan 01 (objgraph +
tracemalloc endpoints) as originally scoped. The 6 candidate surfaces below
remain the same.

---

## Context

The 2026-05-18 P1 memory-leak hotfix (commit `f2e456d4`) capped
`export_search_payload.results` at 5000 entries via `_EXPORT_RESULTS_CAP = 5000`
in `web/export_state.py`. The cap fix worked for its surface: post-deploy the
top live user payload dropped 498 MB -> 512 KB and the top file on disk dropped
906 MB -> 4.5 MB.

But the 11-hour post-deploy soak (2026-05-19) measured RSS growth of
**~411 MB/hr** (1.78 GB post-deploy baseline -> 6.3 GB at 11h, peak 6.8G,
PID 2332735 per `systemctl status genizah-web.service`). 411 MB/hr is deep
in the warning band (>100 MB/hr) of the verdict thresholds defined in the
predecessor todo, and actually WORSE than the pre-fix 300 MB/hr baseline.

So a DIFFERENT surface is leaking. This todo scopes the attribution work.

## Suspect surfaces

Carried forward from the 2026-04-28 OPEN_ISSUES P1 entry, with one new
candidate added 2026-05-19:

1. **NLI/IIIF manifest cache** -- unbounded module-level `dict` in
   `shared/nli_crossref_service.py`. Likely candidate: every shelfmark lookup
   accumulates manifest JSON forever.
2. **csv_bank shelfmark variants** in `genizah_core.py` -- in-memory variant
   index, possibly grows without bound under repeated browse traffic.
3. **Detached `asyncio.ensure_future` / `ui.timer` callbacks** holding closure
   refs to large objects (search.py, browse.py, parallels.py,
   visual_similarity_dialog.py, puzzle.py). Search for sites that still
   capture full result lists or NiceGUI client refs in closure scope.
4. **Image-byte buffers** retained on per-user puzzle / visual-similarity
   paths. Check `web/pages/puzzle.py` + `web/pages/visual_similarity_dialog.py`
   (if it exists) + `shared/puzzle_image_service.py`.
5. **Per-user puzzle image-adjustment LUTs / canvas state** -- v7.2.0
   image-adjustment path retains large per-fragment arrays.
6. **NEW 2026-05-19: Phase 92.2 `WeakKeyDictionary` task-memo on
   `get_user_client()`** in `web/supabase_client.py` keyed by
   `(get_persisted_session_uuid(), access_token)`. Should be GC-safe by
   construction (entries die when the `asyncio.Task` is GC'd), but worth
   verifying that tasks actually finalize and the memo dict shrinks in real
   traffic. If asyncio tasks are being retained somewhere (e.g. background
   loops in `web/main.py` holding strong refs to task handles), the memo
   never reclaims -- and the auth-token-bound Client objects pile up.

## Investigation approach (ordered by signal-per-effort)

### Plan 01 -- live attribution endpoints (code change)

Add two new endpoints next to the existing `/_internal/memstat` at
`web/main.py:~125`, both gated by the same `MEMSTAT_SECRET` header check:

(a) `/_internal/objgraph` -- returns `objgraph.show_growth(limit=30)` between
two snapshots taken N seconds apart (configurable via `?seconds=N` query, default
60). Snapshot 1 is taken on request entry; the handler sleeps; snapshot 2 is
taken on exit; the diff is serialized as JSON. Gives concrete class names
instead of guesses about which subsystem is leaking.

(b) `/_internal/tracemalloc` -- assumes `tracemalloc.start()` was called at
process startup (add to `web/main.py` top-level alongside the existing
startup hooks). Endpoint takes a snapshot, compares it to a previous snapshot
stored on the module (or to a baseline captured on first call), returns the
top 30 allocators by net size growth. Use `tracemalloc.Snapshot.compare_to()`.

Both endpoints must NOT be exposed publicly -- only via the `MEMSTAT_SECRET`
header same as `/_internal/memstat`. Test by capturing baselines both on a
freshly-restarted local server (zero growth expected) and on the live
EC2 instance after the user reproduces some traffic.

### Plan 02 -- audit + fix the surface objgraph attributes

Once `/_internal/objgraph` and `/_internal/tracemalloc` identify the top
growing class(es), the fix is surface-specific:

- If it's NLI manifest cache or csv_bank: wrap the unbounded dict with
  `cachetools.LRUCache(maxsize=N)` -- pick N from observed hit-rate data.
- If it's detached asyncio tasks: switch to `run.io_bound()` or `weakref`
  the captured UI refs; ensure every `ui.timer` is cancelled on disconnect.
- If it's image buffers: ensure the byte arrays are released after upload to
  the cache file (currently they may be retained in module-level caches).
- If it's the Phase 92.2 task-memo: the WeakKeyDictionary is correct; check
  whether the asyncio tasks themselves are being retained somewhere
  (e.g. as strong refs in a background-task list in `web/main.py`).

The fix surface is unknown until objgraph data is captured -- this todo
deliberately does NOT predict the fix.

## Done-criteria

This work is complete when:

1. `/_internal/objgraph` and `/_internal/tracemalloc` endpoints exist in
   `web/main.py` next to `/_internal/memstat`, gated by `MEMSTAT_SECRET`.
2. A baseline objgraph and tracemalloc snapshot have been captured on live
   prod EC2 and recorded as an artifact under the new phase's directory.
3. The dominant leaking surface has been identified by name (specific class
   and code location).
4. A bounded-cache / weak-ref / cancellation fix has been shipped to prod.
5. A 24h soak after the fix ship measures RSS growth rate **< 30 MB/hr**
   (the band-1 threshold in the predecessor todo).
6. P1 OPEN_ISSUES row flipped back to `Fixed (date)` with date stamp and
   change-log row, and the Quick Summary counts decremented (P1 Open 1->0;
   Total Open 34->33).

## Mitigation until the fix ships

Continue manual `sudo systemctl restart genizah-web.service` when RSS climbs
past ~6 GB. The 411 MB/hr rate gives ~14 hours of headroom from a fresh start
before the unit OOM-risks at, say, 12 GB.

## Scheduling

- **NOT scoped into v7.13** -- v7.13 milestone roadmap was locked 2026-05-19
  at Phase 93 (PGP filter on /search, 5 reqs, 1 plan, web-only) and Phase 94
  (research-grade export metadata, 9 reqs, 4 plans, web + desktop xlsx).
  Adding a 3rd phase would re-litigate that lock.
- **Default scheduling: separate post-v7.13 phase** -- once v7.13 ships,
  insert this as the next phase (e.g. Phase 95 if numbering is sequential).
- **Override trigger: urgent insertion** -- if RSS growth rate worsens
  (e.g. >800 MB/hr observed, or unit OOMs in production before v7.13 ships),
  insert this work ahead of v7.13 via `/gsd-insert-phase`.

## Acceptance signal for the user

The user (Hillel) should be able to (a) hit `/_internal/objgraph` with the
secret header, (b) read a JSON list of top growers naming concrete Python
classes, (c) review the proposed fix in the new phase's CONTEXT.md, and
(d) verify the 24h soak rate is < 30 MB/hr post-fix.
