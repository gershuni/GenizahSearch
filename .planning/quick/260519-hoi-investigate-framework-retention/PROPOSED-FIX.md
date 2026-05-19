# Proposed fix: JSON-string storage for heavy payloads

**Status:** sketch (not implemented). Future phase.
**Origin:** investigation `INVESTIGATION.md` 2026-05-19 attributed ~200 MB per heavy UI search to NiceGUI ObservableDict wrapping pressure on the storage write path.

## Problem statement

`safe_user_set('export_search_payload', payload_dict)` ultimately calls `app.storage.user[key] = payload_dict`. NiceGUI's `ObservableDict.__setitem__` wraps the entire value tree as ObservableDicts/Lists:

```python
# nicegui/observables.py:46-56
def _observe(self, data: Any) -> Any:
    if isinstance(data, ObservableCollection):
        data.on_change(self._handle_change)
        return data
    if isinstance(data, dict):
        return ObservableDict(data, _parent=self)   # ← creates 1 OD per nested dict
    if isinstance(data, list):
        return ObservableList(data, _parent=self)   # ← creates 1 OL per nested list
    if isinstance(data, set):
        return ObservableSet(data, _parent=self)
    return data
```

For a `payload = {'results': [{...}, {...}, ... × 5000], ...}`:
- Top-level dict → 1 ObservableDict
- `results` list → 1 ObservableList
- Each of 5000 row dicts → 5000 ObservableDicts
- Plus nested structures per row (none in current SEED-002 schema — pure scalars)

= ~5,002 ObservableDicts/Lists allocated per write. The previous payload's 5,002 wrappers are released (no reference path to them after the dict-storage replacement), but **pymalloc holds the pages**. Each search costs ~200 MB of pymalloc growth that never returns to the OS.

NiceGUI's automatic wrapping is what enables `bind_value_to(app.storage.user, 'key')` and similar reactivity, BUT we never bind to `export_search_payload` — it's a non-reactive payload read only at export/JSON-API time. We pay the wrapping cost for zero benefit.

## Proposed fix

Store the export payloads as **JSON-encoded strings**, not nested dicts. NiceGUI sees a single string; no wrapping cost; no nested ObservableDict creation. On read, deserialize on demand.

### API shape (web/export_state.py)

```python
import json

# Stored as JSON string under the existing key.
def set_search_export(results, query, mode='text', gap=None, filters=None, warnings=None, selected_uids=None) -> None:
    capped, truncated, original, _ = _compact_results(results, _compact_search_result_row)
    payload = {
        'results': capped,
        'query': query,
        'mode': mode,
        'gap': gap,
        'filters': filters,
        'warnings': warnings or [],
        'selected_uids': selected_uids,
        'truncated': truncated,
        'total_count': original,
    }
    # Single string write -> single ObservableDict slot write; no nested wrapping.
    safe_user_set(_SEARCH_KEY, json.dumps(payload, ensure_ascii=False, separators=(',', ':')))


def get_search_export() -> Optional[Dict[str, Any]]:
    raw = safe_user_get(_SEARCH_KEY, None)
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        return payload
    # Back-compat: legacy ObservableDict payloads (pre-migration sessions on
    # disk) — read once, rewrite as string for next access.
    if isinstance(raw, dict):
        payload = dict(raw)  # convert ObservableDict -> plain dict
        # Optionally rewrite as string:
        try:
            safe_user_set(_SEARCH_KEY, json.dumps(payload, ensure_ascii=False, separators=(',', ':')))
        except Exception:
            pass
        return payload
    return None
```

Symmetric changes for `_PARALLELS_KEY`, `update_search_export_results`, `update_parallels_export_filtered`, `update_search_export_selection`.

### Disk-file impact

NiceGUI persists `app.storage.user[key]` via `FilePersistentDict.backup()` which calls `json.dumps(self)` over the whole user storage. If `self[key]` is already a string, json.dumps emits the string with escapes — fine but double-escaped on disk (`"{\"results\":[...]}"`). Slightly larger than dict-form. Not user-visible.

Alternative to avoid double-escape: split the export payload off NiceGUI storage entirely and write to a separate file in `.nicegui/exports/storage-export-{session_id}.json`. More complex; skip for v1 unless disk bloat measured.

## Trade-offs

### Pros
- **Eliminates per-search wrapping pressure.** A single string-slot write is one allocator hit, not 5,002.
- **GC immediate:** the prior string has no `_parent`/`_change_handlers` retention. Released the instant the slot is overwritten.
- **Backward compatible read-side:** legacy dict payloads are read-and-converted; new sessions write strings; existing on-disk files migrate as users next access them.
- **Read-side perf:** json.loads of ~3 MB string takes ~10–30 ms — within export-click latency budget.

### Cons
- **Read cost:** every `get_search_export()` parses the full string. If the live UI reads it frequently during render (e.g., re-rendering result table after filter change), parse cost compounds. Mitigation: cache the deserialized result behind a small wrapper that invalidates on next `safe_user_set`.
- **Loses reactivity:** nothing can `bind_value_to(app.storage.user, 'export_search_payload')`. Confirmed: nothing in the codebase does (search verified at start of investigation).
- **Subtle behavior change:** consumers reading nested fields via `payload['results']` get a plain list now, not ObservableList. If anything was using ObservableList-specific behavior (in-place mutation triggering reactivity), it would break. Confirmed: nothing in the codebase mutates `payload['results']` in place — all writes go through the export_state helpers.
- **`update_*` partial-write helpers** (e.g. `update_search_export_selection`) become read-modify-write-decode cycles: parse string → mutate dict → re-encode → store. Same as today's get-modify-set pattern but with explicit parse/dump steps.

## Implementation plan

Phased rollout to control risk:

### Phase 1 — instrument (1 plan)
Add an opt-in env var `GS_EXPORT_STORAGE_STRING=1`. When set, `safe_user_set('export_search_payload', ...)` json-dumps the payload; reads json-load it. When unset, current behavior unchanged. Deploy. Stress-test under heavy load with the flag on for ONE session. Measure RSS delta per heavy search.

**Pass gate:** RSS growth per heavy search drops from ~200 MB to <50 MB. ObservableDict count stays roughly flat.

### Phase 2 — flip default (1 plan)
If Phase 1 passes, flip the default to ON, keep `GS_EXPORT_STORAGE_STRING=0` as escape hatch. Same change for parallels payload.

### Phase 3 — remove escape hatch (1 plan)
After 1 week of production soak with no incident, remove the env var, lock string storage as the only path. Delete legacy dict-read fallback after a 30-day session-expiry window.

## Test strategy

- **Unit tests:** all 13 tests in `test_export_state_cap.py` extended with a `parametrize` on storage mode (dict / string). Both modes must produce the same `get_search_export()` return value.
- **Integration test:** new test that exercises a real `FilePersistentDict`, writes a search payload, reads it back, asserts dict equality.
- **Heap regression test:** in a test process, call `set_search_export(results=[...5000 rows...])` 10 times, run `gc.collect()`, assert `objgraph.count('ObservableDict')` is bounded by `N + constant` (not `N + 5000 × calls`).
- **Soak test:** the production memstat workflow we already have. Stress 20 heavy searches with the flag on; expect RSS plateau around 2–3 GB (vs ~10 GB current).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Some consumer reads `payload['results']` as ObservableList expecting mutation reactivity | Grep verified zero in-place mutations of payload['results'] |
| Cookie-loaded legacy dict payloads break under string-read path | Back-compat branch in `get_search_export` (shown above) |
| json.dumps on large Hebrew strings is slow | Benchmark: ~10–30 ms for 3 MB. Within budget. |
| Disk-file double-escaping makes files harder to inspect | Accept; or split to separate file (Phase 4 cleanup) |
| Future feature wants reactivity on export_search_payload | Revert easy: re-set as dict |

## What this fix does NOT solve

- Tantivy/regex transient pressure in `genizah_core.execute_search` (~1.7 MB/search, allocator overhead, not worth fixing).
- Search-history accumulation (small, ~20 entries × ~1 KB each, bounded).
- Tab-restore snapshot wrapping (1000 rows in `persist_search_active_snapshot`). **This is the second-biggest wrapping surface.** Apply the same JSON-string treatment as a follow-up.

## Estimated effort

- Phase 1 (instrument + flag-gated): ~3-4 hours of code + 1 hour of stress-test.
- Phase 2-3 (flip default + cleanup): ~1 hour each.
- Total: ~half a day of focused work plus a week of production soak.

## Open questions

- Should `app.storage.tab[_SEARCH_ACTIVE_TAB_KEY]` be in the same fix or separate? Recommendation: same fix — tab storage uses the same NiceGUI ObservableDict wrapping.
- Should `state.search_results` (in `web/pages/search_state.py`) also bypass wrapping? It's not in `app.storage.user` so it's NOT wrapped — only the persistence copy via `safe_user_set('search_results', ...)` is. So the fix already covers it.
