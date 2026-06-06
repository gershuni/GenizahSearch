# Codex review brief — web memory "leak" fix (2026-06-06)

You are reviewing a proposed fix for a long-standing memory growth problem in the
`genizah-web` service (a NiceGUI/FastAPI app, single uvicorn process). I want a
**skeptical, premise-challenging review** — not just a code review of the patch, but
a check on whether we even have the diagnosis right for the *current* production
situation. Be willing to say "you are about to fix the wrong thing."

## Current production symptom (the trigger for this review)

`systemctl status genizah-web.service`:
- Active since 2026-06-03 06:38 UTC; uptime ~3 days at observation.
- Memory: **11.2G (peak 11.2G)**, 33 tasks, single `python -m web.main` PID.
- This is **organic traffic** over 3 days, NOT a concentrated heavy-search soak.
- Fresh-process baseline after the 2026-05 fixes was ~1.78 GB.
- Implied growth ≈ (11.2 − 1.78) / 72h ≈ **~130 MB/hr** sustained, and it does not
  appear to plateau — it climbs until a manual `systemctl restart`.
- Known-normal baseline: 5–6 GB resident is expected (multiple Tantivy indexes +
  SQLite sidecars held in RAM). 11.2 GB and climbing is not.

## History of what we already shipped (all in current code)

1. `f2e456d4` (2026-05-18) — `_EXPORT_RESULTS_CAP = 5000` row cap in
   `web/export_state.py`. Top per-user storage payload dropped 498 MB → 512 KB.
2. `ed6f89c4` (2026-05-19, Codex-authored) — row field-strip (drop
   `full_text`/`raw_file_hl`/`content`), lazy Tantivy rehydration at export time,
   `compact_export_storage_on_startup` hook, `top_keys` diagnostics.
3. SEED-002 (`2a7440d6` + 3 fixups, 2026-05-19, Codex round-1 reviewed) — per-row
   allowlist compaction. Each stored search row is now
   `{uid, sys_id, sort_score, snippet, match_terms, raw_header, img, source}` only.
   Per-row payload 22 KB → ~1.6 KB in-RAM.

After (3), an investigation ran the decisive experiment:
- 25 heavy searches **via the UI** → RSS 1.13 GB → 8.7 GB (~200 MB/search).
- 20 of the same Tantivy/regex searches **via the HTTP API** (no `app.storage.user`
  persistence) → RSS +34 MB total (~1.7 MB/call).
- **117× difference.** ObservableDict instance count stayed flat at ~44K; only bytes
  grew. Backref tracing anchored all 44K ObservableDicts at `storage._users` /
  `storage._tabs` (live working set, not orphans).

**Conclusion drawn at the time:** the residual is *not* a logical retention leak —
it is NiceGUI's `ObservableDict` **wrapping work** on the storage write path
pressuring pymalloc, which never returns freed pages to the OS (high-water mark).
Marked "operationally acceptable with periodic restart."

## The proposed fix (under review)

Full sketch: `.planning/quick/260519-hoi-investigate-framework-retention/PROPOSED-FIX.md`.

Core idea: stop storing the export payloads as nested dicts. NiceGUI's
`ObservableDict.__setitem__` recursively wraps every nested dict/list — so a payload
of `{'results': [ {...} × 5000 ]}` allocates ~5,002 ObservableDict/List objects per
write. Instead, **`json.dumps` the payload and store a single string** under the same
key; `json.loads` on read. NiceGUI then sees one scalar string slot → one allocation,
no nested wrapping. Projected to drop per-heavy-search RSS growth from ~200 MB to
<50 MB. Rollout is env-flag gated (`GS_EXPORT_STORAGE_STRING=1`), then flip default,
then remove the hatch.

## Current code you should read before answering

- `web/export_state.py`:
  - `set_search_export` (~line 533) — writes the plain dict via `safe_user_set`.
  - `get_search_export` (~line 563) — note it **re-compacts and re-writes on every
    read** (`safe_user_set(_SEARCH_KEY, compacted)` when `changed`), so reads also
    trigger wrapping today.
  - `update_search_export_results` / `update_search_export_selection` /
    `update_search_export_enrichment` — read-modify-write helpers.
  - `_compact_results`, `_compact_search_result_row`, the `_SEARCH_ROW_ALLOWLIST`,
    `compact_user_storage_export_payloads` (~line 406, operates on a dict payload).
  - Parallels equivalents (`set_parallels_export`, etc.).
- `web/safe_storage.py` lines 46–85 — `safe_user_get/set/pop` chokepoint
  (`app.storage.user[key] = value`).
- `web/main.py`:
  - `compact_export_storage_on_startup` (~line 2231, `app.on_startup`) — rewrites
    legacy oversized `.nicegui/storage-user-*.json` files; operates on dict payloads.
  - `/_internal/memstat` (~243), `/_internal/objgraph` (~298),
    `/_internal/tracemalloc` (~516) — **all three live in prod**, gated by the
    `MEMSTAT_SECRET` header. We can attribute the *current* heap on demand.
- Consumers that read these payloads: `web/api.py` (export endpoints),
  `shared/search_serializer.py` (public API JSON), `web/pages/parallels.py`
  (live UI reads compacted rows — but from `compact_parallels_result_rows`, a
  separate path from storage).

## What I want from you

Answer these explicitly and in order:

1. **Premise challenge (most important).** The 117× experiment was a *heavy-search
   soak*. Production just grew to 11.2 GB over 3 days of *organic* traffic that is
   mostly NOT 5000-row wildcard searches. Does sustained, roughly-linear,
   non-plateauing multi-GB growth fit the "pymalloc high-water mark from per-write
   wrapping" story? Wrapping pressure should plateau once the largest payload size
   is reached and pages are reused. Linear growth that never plateaus smells like
   genuine **retention** (disconnected clients/sessions/tasks not reclaimed,
   `prune_user_storage` blocked, unbounded module caches, detached `ui.timer`/
   `asyncio` tasks). Is the team about to ship the JSON-string fix against the wrong
   root cause? What single measurement, using the **already-deployed** objgraph /
   tracemalloc endpoints, would confirm-or-refute the wrapping hypothesis on the
   live 11.2 GB process *before* we write any code?

2. **If the wrapping hypothesis is right: will the fix actually work?** Specifically:
   (a) NiceGUI's `FilePersistentDict` re-`json.dumps` the entire user storage on
   change — does storing a pre-serialized string actually reduce steady-state heap,
   or just relocate the cost? (b) Does the fix reduce the *plateau height* but leave
   *linear growth* untouched (in which case it's a partial mitigation, not a fix)?

3. **Patch correctness & migration.** Enumerate concrete breakage from switching the
   stored value dict→string: `get_search_export`'s read-recompact-rewrite cycle;
   `compact_user_storage_export_payloads` / `compact_export_storage_on_startup`
   (both `isinstance(..., dict)`-gated — they'd silently skip string payloads);
   legacy on-disk dict payloads; any consumer doing `payload['results']` expecting a
   live list. What back-compat is mandatory vs optional?

4. **Higher-leverage alternatives.** Rank against the JSON-string fix: (a) pop export
   payloads on client disconnect; (b) move export payloads off `app.storage.user`
   entirely into a process-level bounded LRU keyed by session id (cap N sessions);
   (c) attack retention directly (verify prune runs, bound live client/session
   count, audit detached tasks/module caches named in OPEN_ISSUES: NLI manifest
   cache, csv_bank, Phase 92.2 WeakKeyDictionary task-memo). Which gives the most
   GB-per-hour reduction for the least risk?

5. **Verification protocol.** Given memstat/objgraph/tracemalloc are already live,
   give the exact before/after measurement sequence that would let us claim the leak
   is fixed (target: <30 MB/hr over a 24h soak) — and the measurement that would
   catch a false "fixed" if the fix only lowers the plateau.

Be concrete and cite file:line where relevant. Prefer "measure first" over "patch
first" if the evidence for the current root cause is thin. Output a prioritized
verdict: is the JSON-string fix the right next step, or is a re-attribution pass on
the live process the right next step?
