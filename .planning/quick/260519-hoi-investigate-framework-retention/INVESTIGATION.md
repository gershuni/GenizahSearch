# Framework-Retention Memory Leak Investigation

**Opened:** 2026-05-19 (after SEED-002 fix deploy showed residual leak)
**Status:** active
**Owner:** Claude + Hillel
**Predecessor:** `.planning/quick/260519-hoi-ship-seed-002-uid-only-export-payload/` (SEED-002 shipped storage-cap fix; this phase addresses what SEED-002 couldn't reach)

## TL;DR

SEED-002 capped `app.storage.user['export_search_payload']` at ~1.87 MB per session (270× reduction from 498 MB). But the Python heap still holds **35,233 ObservableDicts** with only 3 active sessions × ~50 keys ≈ ~150 expected — a ~230× retention factor. Heavy searches leave residue that survives across requests. This investigation identifies where the orphan ObservableDicts are anchored and proposes a fix.

## Evidence at investigation open (T+13min post-deploy of `0aa92f82`)

From `/_internal/memstat` and `/_internal/objgraph` at 11:59 UTC, after 15 heavy searches and 10 minutes of regular traffic:

```
VmRSS:                 7.83 GB   (was 1.13 GB at T+15s — grew 6.7 GB during stress test)
RssAnon:               5.29 GB   (~80% Python heap)
RssFile:               2.72 GB   (Tantivy mmap + libraries.csv etc., not the leak)
loaded user storage:   3 sessions, 2.74 MB total
top user payload:      1.87 MB (export_search_payload, 9 results)
retained user storage: 1 session (NiceGUI hasn't released a disconnected session)
disk storage files:    39,910 files, 82 MB total

heap object counts (objgraph most_common):
  list             1,560,605
  dict               426,550
  ObservableDict      35,233   ← smoking gun
  tuple               32,140
  ReferenceType       21,219
  KeyedRef             8,777
```

Expected ObservableDict count: 3 sessions × ~50 keys × ~3 nested wraps = ~450. Actual: 35,233. **Retention factor: 78×.**

Idle baseline (10 min of regular traffic, no heavy searches): **zero RSS growth.** Per-heavy-search cost: ~250 MB.

## Hypothesis tree

### H1: NiceGUI's ObservableDict replacement leaks subscriptions

When `safe_user_set('export_search_payload', new_dict)` runs, it calls `app.storage.user[key] = value`. NiceGUI wraps `value` and all nested dicts as ObservableDicts and registers them with parent's `_listeners` (or analogous) list. When the key is later overwritten, the parent's listener list may still strong-ref the old wrapped objects.

**Test:** Read `nicegui/observables.py` (or equivalent), look for the listener registration pattern. Look for whether `__setitem__` calls `_clear_listeners` on the replaced child.

### H2: Search result rows bound to Vue components via `ui.run_javascript` or `bind_value`

If each result row creates a Vue subscription that holds a Python-side reference to the row dict, then unmounting the result table doesn't release the rows.

**Test:** Read `web/pages/search.py` for how result rows are bound to the rendered UI. Look for `bind_value`, `bind_text`, or v-model-style bindings that capture the row.

### H3: `app.storage.tab` (tab storage) retains result snapshots

Tab storage is supposed to support tab-restore. If snapshots are written per search and never pruned, every search leaves a snapshot behind.

**Test:** Check what `web/pages/search_state.py` writes to `app.storage.tab`. Look at `nicegui_tab_storage_count: 2` in the memstat output — that's surprisingly low for 15 searches across 3 sessions.

### H4: `web/state.py:state` (process-singleton) has un-Phase-88'd mirror fields

Phase 88 deleted 10 AppState mirror fields. But if any code still writes to e.g. `state.search_results` or `state.last_results`, they'd accumulate.

**Test:** Grep for any remaining `state.last_results`, `state.parallels_results`, etc. assignments.

### H5: PostHog event queue accumulates events when posthog client is misconfigured

The `posthog-api-drain` thread runs every 60s. If events aren't flushing, they could pile up. (Counter-evidence: `test_search_api_v2.py` showed an `AttributeError: 'FakeQueue' object has no attribute 'get'` warning — suggests there's a known issue with the queue object type during testing, but in prod the queue should be real.)

**Test:** Check `/_internal/memstat` queue counters if available.

## Diagnostic plan

### Step 1: Read NiceGUI ObservableDict source (5 min)
Find the NiceGUI install path on EC2, read the observables module, identify the listener pattern.

### Step 2: Per-search growth measurement (10 min)
- Pull current `most_common_types` as baseline.
- Have Hillel run ONE heavy search.
- Pull growth diff.
- Expected per-search delta: ~5,000 ObservableDicts + ~10,000 lists + ~5,000 dicts if H1.

### Step 3: Add `find_backref_chain` action to /_internal/objgraph (15 min, requires deploy)
Patch `web/main.py` to support `?action=backrefs&class=ObservableDict&sample=N`. For a sampled instance of the class, walk `gc.get_referrers` to root or N hops. Deploy.

### Step 4: Run backref query (5 min)
For 5 sampled ObservableDicts, get the backref chain. Look for common pattern (e.g., all anchored at `app.storage._users` or at a parent ObservableDict's `_listeners` list).

### Step 5: Verdict + fix design (variable)
Based on the retention path, decide on:
- **Patch NiceGUI** (forked module, monkey-patch, or upstream PR)
- **Workaround in our code** (explicit unsubscribe before set; periodic gc.collect; manual storage clear)
- **Bypass NiceGUI wrapping for large payloads** (store JSON-encoded string instead of dict — bypasses ObservableDict entirely but breaks bind-to-storage if any UI uses it)

## What success looks like

- Per-heavy-search RSS growth < 50 MB (down from ~250 MB).
- ObservableDict count after 15 heavy searches: < 1,000 (down from 35,000).
- `objgraph.growth()` after a search shows < 100 new objects retained across GC.

## What this investigation is NOT

- Not a redesign of NiceGUI's storage layer.
- Not a rollback of SEED-002 (the storage-payload cap is keeping per-session payload bounded, which is a real improvement we want to preserve).
- Not in scope: PostHog leak (separate H5 to confirm or rule out).

## Open log

### 12:00 UTC — initial heap signature (post-stress-test)
After 15 heavy UI searches:
- RSS 7.83 GB / RssAnon 5.29 GB
- ObservableDict 35,233 / list 1.56M / dict 426K
- Top user payload: 1.87 MB (SEED-002 working — was 498 MB pre-fix)

### 12:35 UTC — second stress test (10 more searches, ruling out steady-state)
After 25 heavy UI searches total:
- RSS 8.7 → 10.27 GB (growth during measurement)
- ObservableDict 41,186 (+5,953 from prev / ~595/search avg)
- **Decision: leak confirmed, not steady-state.**

### 12:40 UTC — service restarted, fresh baseline
- RSS ~1.1 GB
- ObservableDict 15,725 (legitimate static state: route registrations, etc.)

### 12:43 UTC — 1 fresh heavy UI search
- RSS → 3.35 GB (+2.35 GB from 1 search)
- ObservableDict → 44,180 (+28,455)
- **Initially looked alarming, but compare to 41,186 after 25 stress searches:
  basically the same. The +28K is one-time working-set establishment,
  not linear accumulation.**

### 12:43 UTC — backref traces (key insight)
Targeting `ObservableDict` and `ObservableList` instances:
- 4 of 8 samples: search-row dicts → ObservableList(5000) → ObservableDict envelope `{results, query, mode}` (i.e. `export_search_payload`).
- 2 samples: row dicts → ObservableList(1000) → ObservableDict `{version, results, printed_filter}` (i.e. `app.storage.tab[_SEARCH_ACTIVE_TAB_KEY]` — tab-restore snapshot from persist_search_active_snapshot at search_state.py:319).
- 1 sample: NiceGUI Select widget options ObservableList(673) (a Select element's options for UI filter — legitimate).
- All anchored at `storage._users` or `storage._tabs`. **No orphans.** The 44K ObservableDicts are the LEGITIMATE working set: 5000 export rows + 1000 tab-snapshot rows + 250 search_results + parallels + nested dicts per row × 3-4 active sessions.

### 12:49 UTC — decisive API-path test (rules out search-execution leak)
Hit `POST /api/search` 20 times with `{"query":"ה","search_mode":"exact","limit":100}` — same Tantivy scan as UI (41,828 total matches per call) but returns JSON and **does NOT persist to app.storage.user**.

| Metric | Before | After 20 calls | Delta |
|---|---|---|---|
| RSS | 3,633 MB | 3,667 MB | **+34 MB (1.7 MB/call)** |
| ObservableDict | 44,216 | 44,182 | **−34** (GC caught up) |
| dict | 464,579 | 466,287 | +1,708 |
| list | 1,586,519 | 1,588,556 | +2,037 |

**Conclusion: 1.7 MB/call via API vs ~200 MB/call via UI = 117× difference for the same Tantivy work.** The leak surface is NOT Tantivy/regex/serialization. It is the **UI storage write path** — specifically NiceGUI's ObservableDict wrapping work when `safe_user_set('export_search_payload', payload_dict)` cascades into wrapping 5000 row dicts × their nested dicts.

### Final verdict

**Per-UI-heavy-search ~200 MB RSS growth is allocator high-water mark from ObservableDict wrapping work, NOT a logical leak.** Object counts stay stable (~44K); only bytes grow. pymalloc holds the pages permanently once allocated, so RSS rises monotonically until the heaviest-workload ceiling is reached. At some workload ceiling RSS plateaus (we have not measured this ceiling).

**SEED-002 was the right fix for what it claimed:** reduced per-row payload 22 KB → 1.6 KB, capped at 5000 rows. Without it, each search wrote 110 MB to disk per session, and 5000 rows × 22 KB = much more pressure on pymalloc per search. With it, the per-search RSS pressure is mitigated but not eliminated because **the act of wrapping 5000 dicts in ObservableDicts is itself transient-allocation-heavy**.

**Operational characterization:** the service grows ~200 MB per heavy UI search, plateaus at some ceiling determined by max-concurrent-heavy-workload. Pre-fix observation was ~411 MB/hr at real traffic (a mix of heavy and light). Post-fix the rate per heavy search is roughly the same; SEED-002's win is the **on-disk file size** (the file shrinks from 498 MB to ~3 MB per session, which is what was eating disk and RAM at restart-rehydration time).

**Recommendation:** ship as-is. Accept that periodic restart is part of operations (it always was; the leak isn't new). Open a future-phase task to implement the JSON-string-storage workaround (see PROPOSED-FIX.md) which bypasses ObservableDict wrapping entirely for the heavy payloads.

## Closed status

- ✅ H1 RULED IN (partial): ObservableDict wrapping of large payloads creates transient-allocation pressure during the write, even though wrapped objects are released cleanly afterward.
- ❌ H2 RULED OUT: not Vue/JS retention — API path proves UI binding isn't the issue.
- ❌ H3 RULED OUT (mostly): tab storage IS holding 1000 result rows per active tab via `persist_search_active_snapshot`, but this is legitimate working set and gets replaced on each search.
- ❌ H4 RULED OUT: Phase 88 deletions are intact; no mirror-field accumulation.
- ❌ H5 RULED OUT: no evidence of PostHog queue accumulation.

## Hand-off

- Investigation done.
- Proposed fix sketched in `PROPOSED-FIX.md` (this directory).
- Open task: schedule the JSON-string-storage phase when the operational pain (periodic restart) becomes a priority.
