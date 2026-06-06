# Codex critique — web memory "leak" fix (2026-06-06)

**Reviewer:** Codex CLI v0.136.0, model gpt-5.5, reasoning effort xhigh.
**Input brief:** `CODEX-REVIEW-BRIEF-2026-06-06.md` (delivered inline; Codex's Windows
read-only sandbox failed with `windows sandbox: spawn setup refresh`, so the brief +
current source were pasted into the prompt rather than read from disk).
**Verdict (headline):** Do the **live re-attribution pass next**, before shipping the
JSON-string change as "the fix."

---

## 1. Premise challenge (most important)

> No, the production shape does not cleanly fit the wrapping-only story.

The 117× UI-vs-API experiment implicates the NiceGUI storage write path for *heavy UI
searches*. But sustained growth ~1.78 GB → 11.2 GB over ~72h with **no plateau**, under
mostly non-heavy organic traffic, is not what pure pymalloc high-water behavior looks
like — once the largest allocation pattern has occurred, arenas should get reused.
Roughly-linear multi-GB growth "smells more like live retention." The team may be
about to ship a good mitigation for one known allocator-pressure path, **but not
necessarily the root cause**.

**Single measurement to run before any code:**
1. `/_internal/tracemalloc?action=start&depth=N`
2. Wait 1–2h on the live process for RSS to grow materially.
3. `/_internal/tracemalloc?action=snapshot`
4. Compare RSS delta (`/_internal/memstat`) vs Python-allocation delta (tracemalloc).

Interpretation:
- RSS +250 MB, tracemalloc net flat, objgraph flat → allocator high-water / transient
  wrapping churn (wrapping hypothesis supported).
- RSS +250 MB, tracemalloc net similar magnitude, growing dict/list/ObservableDict /
  client/session/task objects or growing top user/tab payloads → **refutes
  wrapping-only; points to retention.**
- RSS grows while NiceGUI client/user/tab counts grow → session/client pruning suspect.

## 2. If wrapping is right, will the fix work?

Reduces recursive wrapping cost (one scalar string vs thousands of ObservableDicts per
write). Two limits:
- `FilePersistentDict` still `json.dumps` the **whole** user storage on change — a
  pre-serialized string doesn't eliminate full-storage serialization, adds a
  double-encoded string on disk, and still allocates during dump. Reduces *one* large
  allocation surface, not all storage-write cost.
- If prod growth is true retention, the fix lowers the plateau / per-write spike but
  leaves the GB/hr slope intact → **partial mitigation, not a leak fix.**

## 3. Patch correctness & migration (concrete breakage)

- `get_search_export()` returns `None` for non-dict → a string payload makes exports
  vanish unless it decodes strings.
- `get_search_export()` recompacts+rewrites on read → must decode strings, compact
  legacy dicts, rewrite legacy dicts to string form.
- `compact_user_storage_export_payloads()` is dict-gated → silently skips string
  payloads (acceptable only for already-compacted strings).
- `compact_export_storage_on_startup()` → must still handle legacy oversized dict files.
- Existing on-disk payloads are dicts → read paths must support dict for ≥1 deploy cycle.
- `update_*` helpers assume dict read-modify-write → need shared decode/encode helpers.
- Any consumer doing `payload['results']` must get a decoded dict from public helper
  APIs, not the raw storage value.
- Parallels needs equivalent handling.

**Mandatory:** read both dict+string; write new format behind flag; convert legacy
dict→new on next safe write/read; keep startup compaction handling dict files;
centralize decode/encode so no caller handles mixed types ad hoc.
**Optional:** rewrite already-compacted strings at startup; remove dict support after a
migration window; eager startup migration.

## 4. Higher-leverage alternatives (ranked by GB/hr reduction vs risk)

1. **Live re-attribution / attack retention directly** — verify prune runs, live
   client/user/tab counts, task counts, detached timers/tasks, module caches,
   WeakKeyDictionary, NLI manifest cache, csv_bank. Highest leverage because the symptom
   is linear organic growth. If retention exists, this is the actual fix path.
2. **Move export payloads off `app.storage.user` into a bounded process-level LRU** keyed
   by session id — strong architectural fix, hard cap by session/payload count, avoids
   wrapping. Moderate risk (lifecycle, session keys, multi-tab, restart, export
   availability).
3. **JSON-string storage** — good targeted mitigation for the proven heavy-search path;
   lower risk than LRU, but may only reduce per-write spikes and plateau height.
4. **Pop payloads on disconnect** — useful but incomplete (reconnects, persistent
   storage survives, doesn't address active-session retention or churn while connected).

> If production is climbing 130 MB/hr organically, I would not rank JSON-string first
> without the tracemalloc/RSS attribution pass.

## 5. Verification protocol

**Before fix:** record `/memstat` (RSS/VmData/RssAnon, client/user/tab counts, top live
payload sizes, top `.nicegui` files, top keys); start tracemalloc at meaningful depth;
`/objgraph` baseline; 24h organic soak / canary; record `/memstat` hourly; periodic
tracemalloc snapshots + objgraph growth.
**After fix:** repeat with same traffic class; claim fixed only if RSS slope <30 MB/hr
over 24h AND tracemalloc/object counts show no accumulating live structures; separately
run the heavy-UI-search repro and confirm per-search growth ≪ ~200 MB/search.
**Catch a false "fixed":** measure *slope*, not absolute RSS, past the old plateau
window. If heavy-search jumps shrink but organic RSS still grows linearly → fix only
lowered plateau/spike. If client/user/tab/task counts or tracemalloc live allocations
keep rising with RSS → retention remains.

## Prioritized verdict

> Do the live re-attribution pass next, before shipping the JSON-string change as "the
> fix." The JSON-string patch is plausible and probably worth doing as a gated
> mitigation for the proven heavy-search storage path. But the production evidence does
> not yet justify calling it the root-cause fix. The next decision should be driven by
> one live tracemalloc-vs-RSS delta measurement on the current 11.2 GB process.
