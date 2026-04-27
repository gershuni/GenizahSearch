# Phase 77: Serializer & JSON Export - Research

**Researched:** 2026-04-27
**Domain:** Python serializer module + FastAPI download handlers + NiceGUI toolbar buttons (JSON export over existing search/parallels result pipelines)
**Confidence:** HIGH

## Summary

Phase 77 builds `shared/search_serializer.py` (two named functions sharing a private `_serialize_item()` per D-14) plus `GET /api/export/json` and `GET /api/export/parallels/json` handlers in `web/api.py`, plus toolbar JSON download buttons on `/search` (web/pages/search.py:1441-1446) and `/parallels` (web/pages/parallels.py:1230-1233). The implementation reuses the existing Excel/Word export pipeline pattern almost verbatim — server-side `state.last_results` / `state.parallels_results` consumption, FastAPI `Response` with `Content-Disposition` header, `make_safe_filename` + `remove_highlight_markers` from `shared_export_utils.py`.

Three findings change the planner's assumptions versus what CONTEXT.md asserts. **First**, `display['img']` is the manuscript **page number** (`p_num`), NOT a URL — the field name is misleading. The actual image URL is constructed at render time as `/api/nli_image_by_sysid/{sys_id}?page={page_idx}` (web/pages/search_results.py:641) with Oxford fallback. The serializer must either (a) emit a server-relative URL using this same template, (b) emit only the page index and let consumers construct URLs from the locator, or (c) re-discuss D-08. Recommendation: emit a relative URL like `/api/nli_image_by_sysid/{sys_id}?page={p_num}` since the JSON file is "Claude-friendly" and the consumer (a Claude skill) can resolve it via the same web app. **Second**, `state.current_search_query` is declared in `web/state.py:27` but is never assigned anywhere in the codebase — making the existing Excel/Word filename always default to `"genizah.xlsx"` and the planned JSON `query` echo and filename also fall through to defaults. Phase 77 should populate `state.current_search_query = clean_query` (and analogously `state.current_search_mode = mode`) inside `web/pages/search.py:~4076` before `state.last_results = results`. **Third**, per-chunk attribution is **lost** in `lab_composition_search` (genizah_core.py:1373) — every chunk hit increments `total_score` and `hits_count` on a single `results_map[uid]` record but the chunk index variable `i` from the outer loop is NOT stored. D-13's required `matches: [{chunk_index, source_chunk_text, manuscript_snippet, score}, ...]` shape literally cannot be produced from current data without changes to core. Three viable paths exist (extend core, degenerate single-element matches array, or re-discuss); the planner must pick one explicitly.

**Primary recommendation:** Build the module + handlers + buttons exactly as CONTEXT.md prescribes, BUT (a) treat `display['img']` as `p_num` and emit a constructed image URL, (b) patch `state.current_search_query` population during the search code path, (c) escalate the parallels per-chunk-attribution gap to either a small core extension or a de-scoped `match[]`. Use FastAPI `JSONResponse` (already imported elsewhere in `web/api.py`) for the handlers; `make_safe_filename(default='genizah-search')` for filename basis; `remove_highlight_markers` then `re.findall(r'\*([^*]+)\*', snippet)` for the `match_terms` extraction.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Payload shape (envelope + per-item fields) | shared/ (cross-app reusable Python module) | — | D-14 mandates `shared/search_serializer.py`; Phase 78 `web/api.py` and Phase 80 `web/api.py` import the same two functions. Cross-app convention matches `shared/document_service.py`, `shared/fjms_service.py`, etc. |
| HTTP download endpoint | API / Backend (FastAPI route in `web/api.py`) | shared/ (calls serializer) | Routes live in `init_api_routes()` (web/api.py:174) alongside Excel/Word counterparts. Statelful download — reads `state.last_results` like Excel/Word. Phase 78 stateless `/api/search` is a separate route that calls the serializer directly. |
| Toolbar JSON button | Frontend Server (NiceGUI page render) | API / Backend (download URL) | `ui.button(icon='data_object', on_click=lambda: ui.download('/api/export/json'))` slot in next to existing buttons in `web/pages/search.py:1446` and `web/pages/parallels.py:1233`. NiceGUI's `ui.download()` triggers a browser navigation to the FastAPI route. |
| Filename + Content-Disposition | API / Backend (handler) | — | `encode_filename_for_header()` from `shared_export_utils.py` already handles RFC 5987 for non-ASCII; reuse verbatim. |
| `match_terms` extraction | shared/ (serializer logic) | — | Pure function over the snippet string. No I/O. Lives in private `_serialize_item()`. |
| Image URL resolution | NOT this phase — emit only the relative URL constructible from locator | API / Backend (`/api/nli_image_by_sysid/...` already serves images at request time) | D-08 says "no new IIIF resolution work" — the serializer emits `/api/nli_image_by_sysid/{sys_id}?page={p_num}` (NLI default) or `null` for metadata-only hits. The actual image fetch happens when the consumer follows the URL. Oxford/multi-IE branching deferred to Phase 79 `/api/browse`. |
| Filter echo | API / Backend (passes filters dict into serializer) | — | Filters live in `search_state` (page-scoped) — not in `state` (global). The download handler must read from `state` only OR a parallel piece of state must be added. Recommendation: extend `state.last_filters_applied: dict` populated at search-time (mirrors `state.last_results` pattern). |
| Test fixtures | tests/ (pytest, mocks `MetadataManager`) | — | Existing pattern in `tests/test_export_service.py` uses MagicMock for `meta_mgr` and dict fixtures for results — serializer tests follow same pattern. |

## Standard Stack

### Core (already in place — no installs needed)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Python stdlib `json` | 3.10+ | JSON serialization | [VERIFIED: `web/api.py:2`] Already imported and used throughout. `json.dumps(..., ensure_ascii=False)` correctly emits Hebrew as native UTF-8 chars. |
| FastAPI | 0.135.1 | HTTP route decoration | [VERIFIED: requirements-lock.txt:fastapi==0.135.1] `@app.get(...)` pattern used by all 4 existing export handlers. |
| Starlette `JSONResponse` | 0.52.1 | JSON response with auto media-type | [VERIFIED: web/api.py:1374, 1538, 1585] Already imported and used 8+ times in `web/api.py`. Sets `Content-Type: application/json` automatically. Handles non-ASCII via Pydantic JSON encoder. |
| Starlette `Response` | 0.52.1 | Custom Content-Disposition | [VERIFIED: web/api.py:1818] Used by Excel/Word handlers when custom headers needed. JSON download path may use this for explicit filename header (or use `JSONResponse(headers={...})`). |
| NiceGUI `ui.download` | 3.8.0 | Browser-triggered download | [VERIFIED: web/pages/search.py:1441] Identical to Excel/Word button pattern; takes a URL string. |
| `shared_export_utils.remove_highlight_markers` | local | Strip `*term*` from snippet | [VERIFIED: shared_export_utils.py:84-96] One-line `text.replace('*', '')`. Idempotent. Returns `""` for `None` input. Hebrew-safe (no encoding ops). |
| `shared_export_utils.make_safe_filename` | local | Filename base | [VERIFIED: shared_export_utils.py:103-139] `preserve_hebrew=True` by default; replaces unsafe chars with `_`, truncates to 50 chars, returns default `"genizah"` if empty. |
| `shared_export_utils.encode_filename_for_header` | local | RFC 5987 Content-Disposition | [VERIFIED: shared_export_utils.py:162-182] Reused by all 4 existing export handlers. Pure ASCII path uses `filename="..."`; non-ASCII uses `filename*=UTF-8''<percent-encoded>`. |

### Supporting (existing services, called by serializer for enrichment)

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `genizah_core.MetadataManager.parse_full_id_components` | local | Header → `{sys_id, ie_id, p_num, fl_id}` for locator | [VERIFIED: genizah_core.py:3506-3535] Called per-result on `result['raw_header']` to derive locator. Returns dict with `None` values when fields are missing — directly maps to D-04 "may be null". |
| `genizah_core.get_library_display(code, short=False, lang='en')` | local | Library code → full English name | [VERIFIED: genizah_core.py:1699-1717] `short=True` returns code unchanged; `short=False` returns full name. D-01 emits BOTH fields → call once with each. |
| `shared.fjms_service.FjmsService.get_domains_for_sys_ids` | local | Batch domain lookup | [VERIFIED: shared/fjms_service.py:788-825] Returns `dict[sys_id, list[domain_dict]]`. The serializer should call this in batch (one call for all results) rather than per-item to stay sub-millisecond. Each domain dict has `domain` (English) and `domain_heb`. |
| `shared.fjms_service.FjmsService.get_catalog` | local | Single-record catalog → `copy_date` for `dating` field | [VERIFIED: shared/fjms_service.py:2136-2177] Returns dict with `copy_date` key (Hebrew text like "המאה ה-12"). For batch this is N queries; consider deferred lookup or per-item if performance acceptable. ~50 results ≈ 50ms — acceptable. |
| `genizah_core.SearchEngine.execute_search` | local | Returns `state.last_results` shape | [VERIFIED: genizah_core.py:7185] No change needed — serializer is read-only. |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `shared/search_serializer.py` | `web/serializers/json_payload.py` | shared/ wins per D-14 — Phase 78 imports without web→core circular dep; matches existing convention. |
| `dict` payload + `json.dumps` in handler | Pydantic `BaseModel` + `model_dump()` | Pydantic adds runtime validation guarantees but couples Phase 77 to FastAPI typing. Plain dicts keep `shared/` framework-agnostic. **Picked: dict.** |
| Per-item domain/catalog lookup | Batch lookup at start of `serialize_search_payload` | Batch is 1 SQLite query for N results vs N queries. **Picked: batch.** Pattern matches `web/pages/search.py:4266`. |
| Reuse `web/export_service.export_search_results_excel` signature `(results, search_query)` | Add full kwargs `(results, *, query, mode, filters, total, warnings, source)` | Excel signature is too narrow for D-06 full echo. **Picked: full kwargs**, default to None where caller doesn't have data. |

**Installation:** None — all libraries already in `requirements.txt` / `requirements-lock.txt`.

**Version verification:** All packages above are pinned in `requirements-lock.txt`; no new pins needed.

## Architecture Patterns

### System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│  /search page (NiceGUI)                                                   │
│                                                                            │
│  [Filter] [Word] [Excel] [JSON]  ← Phase 77 adds 3rd button               │
│              │       │       │                                             │
│              │       │       └──── on_click=ui.download('/api/export/json')│
│              │       └──── on_click=ui.download('/api/export/excel')       │
│              └──── on_click=ui.download('/api/export/word')                │
│                                                                            │
│  state.last_results ←── populated by execute_search() at search.py:4077    │
│  state.current_search_query ←── PHASE 77 PATCH: populate at search.py:4076│
│  state.last_filters_applied ←── PHASE 77 ADD: populate at search.py:4076  │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼  HTTP GET
┌──────────────────────────────────────────────────────────────────────────┐
│  web/api.py — init_api_routes()                                           │
│                                                                            │
│  @app.get('/api/export/json')                                             │
│  def export_json():                                                        │
│      if not state.last_results: return Response(...400)                   │
│      payload = serialize_search_payload(                                   │
│          state.last_results,                                               │
│          query=state.current_search_query,                                 │
│          mode=state.current_search_mode,                                   │
│          filters=state.last_filters_applied,                               │
│          warnings=state.last_search_warnings,                              │
│      )                                                                     │
│      return JSONResponse(payload, headers={                                │
│          'Content-Disposition': encode_filename_for_header(filename)       │
│      })                                                                    │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼  function call
┌──────────────────────────────────────────────────────────────────────────┐
│  shared/search_serializer.py (NEW)                                        │
│                                                                            │
│  SCHEMA_VERSION = 1                                                        │
│                                                                            │
│  def serialize_search_payload(results, *, query, mode, gap=None,          │
│      filters=None, warnings=None, total=None) -> dict:                    │
│      sys_ids = [r['display'].get('id') for r in results if ...]            │
│      domains_batch = fjms.get_domains_for_sys_ids(sys_ids)  # batch       │
│      return {                                                              │
│          'schema_version': SCHEMA_VERSION,                                 │
│          'source': 'search',                                               │
│          'query': query, 'mode': mode, 'gap': gap,                         │
│          'filters': filters,                                               │
│          'count': len(results), 'total': total or len(results),            │
│          'warnings': warnings or [],                                       │
│          'generated_at': datetime.utcnow().isoformat() + 'Z',              │
│          'results': [_serialize_item(r, domains_batch) for r in results],  │
│      }                                                                     │
│                                                                            │
│  def serialize_parallels_payload(main, filtered, *, source_text,          │
│      chunk_size, mode, max_freq=None, ...) -> dict:                       │
│      ...                                                                   │
│      grouped_main = _group_parallels_by_manuscript(main)                  │
│      return {                                                              │
│          'schema_version': SCHEMA_VERSION, 'source': 'parallels',          │
│          'source_text': source_text, 'chunk_size': chunk_size,             │
│          'mode': mode, 'max_freq': max_freq,                               │
│          'count': len(grouped_main), 'total': len(grouped_main),           │
│          'warnings': [], 'generated_at': ...,                              │
│          'results': [_serialize_parallels_item(g) for g in grouped_main],  │
│          'filtered': [_serialize_parallels_item(g) for g in grouped_filt], │
│      }                                                                     │
│                                                                            │
│  def _serialize_item(result_dict, domain_batch_lookup) -> dict:           │
│      # Shared by both. Emits locator + metadata fields.                    │
│      display = result_dict.get('display', {})                              │
│      sys_id = display.get('id', '')                                        │
│      raw_header = result_dict.get('raw_header', '')                        │
│      parsed = MetadataManager.parse_full_id_components(raw_header)        │
│      snippet_clean = remove_highlight_markers(result_dict.get('snippet'))  │
│      match_terms = _extract_match_terms(result_dict.get('snippet'))        │
│      return {                                                              │
│          'uid': result_dict.get('uid', ''),                                │
│          'locator': {                                                      │
│              'sys_id': sys_id or parsed.get('sys_id'),                     │
│              'volume_ie': parsed.get('ie_id'),                             │
│              'p_num': parsed.get('p_num'),                                 │
│          },                                                                │
│          'score': round(result_dict.get('sort_score', 0.0), 4),            │
│          'shelfmark': display.get('shelfmark', ''),                        │
│          'title': display.get('title', ''),                                │
│          'library': {                                                      │
│              'code': display.get('library_code', ''),                      │
│              'name': get_library_display(...code..., short=False, lang='en'),│
│          },                                                                │
│          'domain': domain_batch_lookup.get(sys_id, [{}])[0].get('domain'), │
│          'dating': fjms.get_catalog(sys_id).get('copy_date'),  # or batch  │
│          'snippet': snippet_clean,                                         │
│          'excerpt': (result_dict.get('full_text') or '')[:500],            │
│          'match_terms': match_terms,                                       │
│          'image_url': _build_image_url(sys_id, parsed.get('p_num')),       │
│      }                                                                     │
└──────────────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure
```
shared/
├── search_serializer.py     ← NEW (Phase 77)
├── document_service.py      # existing precedent for shared/ module
├── fjms_service.py          # existing
└── ...

web/
├── api.py                   # ADD: /api/export/json + /api/export/parallels/json handlers
├── state.py                 # PATCH: add current_search_mode, last_filters_applied,
│                            #         last_search_warnings (mirror last_results pattern)
├── pages/
│   ├── search.py            # PATCH: populate state.current_search_query at line ~4076;
│   │                        #         add JSON button at line ~1446
│   └── parallels.py         # PATCH: add JSON button at line ~1233; populate disable state
└── export_service.py        # OPTIONAL thin wrappers export_search_results_json,
                             #          export_parallels_json (per CONTEXT.md option)

tests/
└── test_search_serializer.py  ← NEW (Phase 77)
```

### Pattern 1: Existing Excel/Word handler pattern (mirror exactly)

**What:** Stateful FastAPI handler reading server-side `state.last_results`, calling export service, returning Response with Content-Disposition.

**When to use:** All 5 toolbar download buttons (search/parallels × Excel/Word/JSON).

**Example:**
```python
# web/api.py — Phase 77 adds these alongside lines 1806-1908
@app.get('/api/export/json')
def export_json():
    """Export search results as Claude-friendly JSON (Phase 77, EXPORT-01/03/04)."""
    from starlette.responses import JSONResponse
    from shared.search_serializer import serialize_search_payload
    from datetime import datetime

    if not state.last_results:
        return Response("No results to export", status_code=400)

    try:
        payload = serialize_search_payload(
            state.last_results,
            query=state.current_search_query or "",
            mode=getattr(state, 'current_search_mode', 'text'),
            gap=getattr(state, 'current_search_gap', None),
            filters=getattr(state, 'last_filters_applied', None),
            warnings=getattr(state, 'last_search_warnings', None),
        )
        ts = datetime.utcnow().strftime('%Y-%m-%dT%H%M')
        filename = f"genizah-search-{ts}.json"
        return JSONResponse(
            payload,
            headers={"Content-Disposition": encode_filename_for_header(filename)}
        )
    except ValueError as e:
        return Response(str(e), status_code=400)
    except Exception as e:
        logger.error(f"Export JSON error: {e}")
        return Response("Export failed", status_code=500)
```

### Pattern 2: Toolbar button placement (slot in identically)

**What:** `ui.button` immediately after Excel button with matching props.

**When to use:** Both `/search` and `/parallels` toolbars.

**Example:**
```python
# web/pages/search.py — slot in after line 1446
ui.button(icon='data_object', on_click=lambda: ui.download('/api/export/json')).props(
    'flat round dense size=sm'
).tooltip(tr('Export JSON'))

# web/pages/parallels.py — slot in after line 1235; capture handle for enable/disable
export_json_btn = ui.button(icon='data_object', on_click=lambda: ui.download('/api/export/parallels/json')).props(
    'flat round dense disable'
).tooltip(tr('Export JSON'))

# Then in render_results() at lines 2608/2615 add:
export_json_btn.props('disable')           # at empty-state branch
export_json_btn.props(remove='disable')    # at has-results branch
```

### Pattern 3: Locator construction from `raw_header`

**What:** Use `MetadataManager.parse_full_id_components(raw_header)` to derive `{sys_id, ie_id, p_num, fl_id}` from the header string.

**When to use:** Every item in `_serialize_item()` to populate `locator`.

**Example:**
```python
# shared/search_serializer.py
from genizah_core import MetadataManager

# In _serialize_item:
raw_header = result.get('raw_header', '')
if raw_header:
    parsed = MetadataManager.parse_full_id_components(raw_header)
    # parse_full_id_components is a regular method; needs an instance OR refactor to staticmethod.
    # ALTERNATIVE: re-import the regex logic inline since it's pure parsing.
else:
    parsed = {'sys_id': None, 'ie_id': None, 'p_num': None}
```

**Anti-Pattern caveat:** `parse_full_id_components` is an instance method on `MetadataManager` even though its body uses no instance state (genizah_core.py:3506-3535). Calling it requires `state.meta_mgr.parse_full_id_components(...)`. Either accept that coupling, refactor it to a staticmethod or module-level function in core (low risk), or duplicate the small regex block in the serializer. Recommendation: pass `state.meta_mgr` as an argument to the serializer functions (similar to how `ExportService(meta_mgr=...)` works in `web/export_service.py`).

### Pattern 4: D-13 parallels grouping (CRITICAL — see Common Pitfalls)

**What:** D-13 says "one result per manuscript with `matches: [...]` array." But core's `lab_composition_search` (genizah_core.py:1352-1374) ALREADY merges all chunk hits into a single `results_map[uid]` record by SUMMING `total_score` and incrementing `hits_count` — the per-chunk source-text identity is LOST.

**When to use:** Document this in the plan and pick path A, B, or C below.

**Path A — Extend core (preferred for fidelity):**
Add `results_map[uid]['chunk_hits'] = []` at line 1354 and append `(i, chunk_text, match_score, ms_snippet_for_this_chunk)` inside the loop. Backwards compatible — existing code reading `total_score`/`hits_count` is unchanged. Adds ~30 lines to core. Phase 80 `/api/parallels` benefits too.

**Path B — Degenerate (preserves existing core):**
Emit `matches: [{chunk_index: null, source_chunk_text: <full source_ctx joined>, manuscript_snippet: <text>, score: <total_score>}]` — single-element array. Document in response that core does not currently track per-chunk attribution. EXPORT-03 (single source of truth) is preserved; D-12 fidelity is partially lost.

**Path C — Re-discuss:**
Bring D-13 back to discuss-phase. Cost: a discuss round-trip.

**Recommendation:** Path A — small, well-scoped core addition that unlocks both Phase 77 and Phase 80. Add a single core test asserting `chunk_hits` populated. If user pushes back at plan-check, fall back to Path B.

### Pattern 5: Image URL construction

**What:** `display['img']` is `p_num`, NOT a URL. Existing UI builds the URL at render time.

**Reference (web/pages/search_results.py:629-657):**
```python
page_idx = max(0, int(display.get('img', '1')) - 1) if display.get('img') else 0
_img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}&width=300{_thumb_suffix}"
# Oxford branching omitted from JSON — too involved for Phase 77 scope.
```

**Phase 77 emit:**
```python
def _build_image_url(sys_id: str, p_num: str | None, page_idx_offset: int = -1) -> str | None:
    """Build server-relative IIIF URL. Returns None for metadata-only hits.
    page_idx is 0-based int; p_num is the 1-based string from header parse."""
    if not sys_id or not p_num:
        return None
    try:
        page_idx = max(0, int(p_num) + page_idx_offset)
    except (ValueError, TypeError):
        return None
    return f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}"
```

(Volume suffix and Oxford fallback explicitly out of scope per D-08 / Phase 79 ownership.)

### Anti-Patterns to Avoid

- **Using `display['img']` AS the URL:** It is `p_num` (page number string). The CONTEXT.md description "primary image URL from existing display['img']" is factually incorrect about field semantics.
- **Calling `fjms.get_catalog()` per-item without batching:** ~50 sequential SQLite queries adds 50ms+. Either batch via a new `get_catalog_batch_for_sys_ids()` helper OR accept the cost (acceptable for downloads, not for Phase 78 latency-sensitive endpoint).
- **Hand-rolling JSON encoder for Hebrew:** Stdlib `json.dumps(payload, ensure_ascii=False)` (or starlette `JSONResponse` with default config) emits Hebrew as native UTF-8 — no special handling needed.
- **Reading from `app.storage.user` inside the FastAPI handler:** That works for parallels source_text (existing pattern), but `state.*` mirroring is preferred for the search path so the JSON handler stays decoupled from NiceGUI session lifecycle.
- **Two parallel `_serialize_item` implementations** (one per top-level function): Direct violation of D-14 and EXPORT-03. The structural test (see Validation Architecture below) must enforce sharing.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| JSON serialization of Python dict with Hebrew | Custom encoder, manual escape | `starlette.responses.JSONResponse` | Already imported in `web/api.py` 8+ times; handles UTF-8, indentation, Pydantic-aware encoding. |
| Filename safety for Hebrew | Manual char filter | `shared_export_utils.make_safe_filename(text, preserve_hebrew=True)` | Existing utility used by 4 other export paths; centralized regex; safe truncation. |
| RFC 5987 Content-Disposition | Manual `urllib.parse.quote` | `shared_export_utils.encode_filename_for_header(filename)` | Existing utility; handles ASCII vs non-ASCII automatically. |
| Stripping `*term*` markers | `text.replace('*', '')` inline | `shared_export_utils.remove_highlight_markers(text)` | Idempotent; handles `None`; D-03 explicitly references this. |
| Library code → full name | `LIBRARY_CODES.get(...)` inline | `genizah_core.get_library_display(code, short=False, lang='en')` | Handles HE/EN/fallback; centralized in core. |
| Header → locator parts | Inline regex | `MetadataManager.parse_full_id_components(header)` | Returns the exact `{sys_id, ie_id, p_num, fl_id}` shape — directly maps to D-04. |
| Domain lookup for N results | N individual `get_domains()` calls | `fjms.get_domains_for_sys_ids(sys_ids)` (batch) | 1 SQL query vs N; matches search.py:4266 pattern. |
| ISO timestamp | `time.strftime` with custom format | `datetime.utcnow().isoformat() + 'Z'` (envelope), `strftime('%Y-%m-%dT%H%M')` (filename) | Stdlib; explicit UTC with Z suffix; collision-free at minute granularity. |
| Match-term extraction from `*term*` | Recompute the search-term list | `re.findall(r'\*([^*]+)\*', snippet)` then dedupe in-order | Operates on the snippet itself, so it captures terms that actually matched (not query terms that didn't); zero coupling to search engine internals. |

**Key insight:** Every primitive Phase 77 needs is already in the codebase. The serializer is a 100-200 line module that mostly composes existing helpers.

## Runtime State Inventory

> Phase 77 is a NEW feature add (no rename/refactor/migration). Inventory not applicable.

**Stored data:** None — JSON exports do not write to any datastore.
**Live service config:** None — no external service registrations.
**OS-registered state:** None.
**Secrets/env vars:** None — no new env vars (Phase 78 introduces `SEARCH_API_MODE`).
**Build artifacts:** None.

## Common Pitfalls

### Pitfall 1: `display['img']` is a page number, not a URL

**What goes wrong:** Naive read of CONTEXT.md D-08 → emit `display['img']` directly as `image_url`. JSON consumers receive integers like `"7"` or `"23"` where they expected URLs.

**Why it happens:** Field name is misleading. Was named `img` because it's *passed to* image rendering code, not because it *holds* an image URL.

**How to avoid:** Always use `_build_image_url(sys_id, p_num)` helper. Never inline `display['img']` as a URL value.

**Warning signs:** A unit test that asserts `result['image_url'].startswith('http')` or `result['image_url'].startswith('/api/')` will catch this immediately.

### Pitfall 2: `state.current_search_query` is never assigned

**What goes wrong:** Existing Excel/Word filenames silently default to `genizah.xlsx` because `make_safe_filename("")` falls through to default. JSON `query` envelope echo also empty.

**Why it happens:** `state.current_search_query` was declared in `web/state.py:27` but the search page never sets it. Code review precedent: `web/api.py:1816, 1839` reads it; nothing writes it. Likely lost in v7.9 decomposition.

**How to avoid:** Phase 77 plan must include a **task** that adds `state.current_search_query = clean_query` (and `state.current_search_mode = mode`, `state.current_search_gap = gap`, `state.last_filters_applied = ...`, `state.last_search_warnings = ...`) at the right point in `web/pages/search.py:~4076` (before `state.last_results = results`). Mirror in `web/pages/search_results.py:123` if results are filtered.

**Warning signs:** Test the JSON download immediately after a search and inspect the envelope — if `query` is empty after running "אגדה", the patch is missing.

### Pitfall 3: Per-chunk attribution lost in core (D-13 architectural gap)

**What goes wrong:** D-13's `matches: [{chunk_index, source_chunk_text, manuscript_snippet, score}, ...]` cannot be produced because `lab_composition_search` collapses per-chunk data into `total_score`/`hits_count` integers.

**Why it happens:** Core was designed for UI rendering, which only needed manuscript-level cards. Per-chunk attribution was never exposed.

**How to avoid:** Pick path A (extend core), B (degenerate), or C (re-discuss) explicitly in plan. Do NOT silently emit a single-item matches array without acknowledging the loss in the response shape OR documentation.

**Warning signs:** A unit test that runs a synthetic 3-chunk source against 1 manuscript matching all 3 chunks should produce `matches: [..., ..., ...]` (3 entries). Path A passes; Path B emits 1 entry. Path B's test must explicitly assert the degenerate behavior to make the trade-off visible.

### Pitfall 4: Filters echo lives in page-scoped `search_state`, not global `state`

**What goes wrong:** D-06 says "echoes filters? exactly as used by the search call." But `search_state.filter_domains` etc. are page-scoped (per browser tab). The download handler in `web/api.py` only sees `state` (global singleton).

**Why it happens:** v7.9 decomposition introduced page-scoped state to reduce `app.storage.user` sprawl. Excel/Word handlers don't echo filters so this was never surfaced.

**How to avoid:** Add `state.last_filters_applied: dict` to `web/state.py` and populate at the same point as `state.last_results`. Shape: `{'domains': [...], 'authors': [...], 'works': [...], 'date_from': ..., 'date_to': ..., 'material_exclude': [...], 'include_mode': bool}` — copy from `search_state.filter_*` fields verbatim.

**Warning signs:** A unit test that runs a search with `filter_domains=['Letters']` and downloads JSON should assert `payload['filters']['domains'] == ['Letters']`.

### Pitfall 5: `parse_full_id_components` is an instance method, not module-level

**What goes wrong:** Naive `from genizah_core import parse_full_id_components` fails — it's an `MetadataManager` method.

**Why it happens:** Historical OO grouping; the function uses no instance state but is bound to the class.

**How to avoid:** Either (a) pass `state.meta_mgr` to the serializer (existing precedent in `ExportService(meta_mgr=...)`), OR (b) refactor to `@staticmethod` in core (zero behavioral change), OR (c) duplicate the small regex block in the serializer (~10 lines).

**Recommendation:** Option (a) — pass `meta_mgr` as a serializer argument. Cleanest separation; serializer module stays free of `genizah_core` import-time side effects.

### Pitfall 6: Two-consecutive-download filename collision (EXPORT-04)

**What goes wrong:** Filename uses minute-resolution ISO timestamp (`2026-04-27T1530`). User clicks twice within same minute → both downloads have same filename → browser asks to replace OR auto-renames `(1)`.

**Why it happens:** Minute resolution is too coarse for fast-clicking users.

**How to avoid:** Use second-resolution (`%Y-%m-%dT%H%M%S`) OR append a 4-char random hex suffix (`uuid.uuid4().hex[:4]`). Recommendation: second resolution — predictable for users, EXPORT-04 success criterion met.

**Warning signs:** Test asserts that two `serialize_search_payload(...)` calls separated by `time.sleep(0)` produce distinct `filename` values. Minute-resolution implementation will fail this test.

### Pitfall 7: Empty `state.last_results` after browser refresh

**What goes wrong:** User runs search, refreshes page, clicks JSON download — `state` is the global singleton (process-scoped), so the previous search's results are still there from the OTHER user's session. Cross-user data leak.

**Why it happens:** `state` is a process-global singleton in NiceGUI single-process deployments. The Excel/Word handlers have the same architectural issue — but they're stateful by design.

**How to avoid:** This is a **Phase 78 problem** (API-06: stateless endpoints), NOT Phase 77. The download endpoints inherit the existing Excel/Word issue and don't make it worse. Document this in the plan as "inherits existing limitation; Phase 78 `/api/search` is the stateless replacement." Do not try to fix in Phase 77.

**Warning signs:** Multi-user pen-test discovers cross-tenant leak — out of scope for v7.10 (single-server, low traffic, no PII in search results).

## Code Examples

### Example 1: Minimal `_serialize_item` for the search path

```python
# shared/search_serializer.py
import re
from typing import Any, Optional

SCHEMA_VERSION = 1


def _extract_match_terms(snippet: Optional[str]) -> list[str]:
    """Extract unique *term* matches in order of first appearance.

    D-03: snippet has *foo* *bar* *foo* *baz* markers.
    Returns: ['foo', 'bar', 'baz'] (deduped, order preserved).
    """
    if not snippet:
        return []
    found = re.findall(r'\*([^*]+)\*', snippet)
    seen: set[str] = set()
    out: list[str] = []
    for t in found:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _build_image_url(sys_id: Optional[str], p_num: Optional[str]) -> Optional[str]:
    """Server-relative URL or None for metadata-only hits."""
    if not sys_id or not p_num:
        return None
    try:
        page_idx = max(0, int(p_num) - 1)
    except (ValueError, TypeError):
        return None
    return f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}"


def _serialize_item(
    result: dict,
    *,
    meta_mgr: Any,
    domain_batch: dict[str, list[dict]],
    catalog_batch: dict[str, dict],
    library_resolver,
) -> dict:
    """Shared serializer for both search and parallels per D-14.

    Args:
        result: a single dict from state.last_results or state.parallels_results.
        meta_mgr: MetadataManager instance for parse_full_id_components.
        domain_batch: dict[sys_id, list[domain_dict]] from fjms.get_domains_for_sys_ids.
        catalog_batch: dict[sys_id, catalog_dict] for dating lookup.
        library_resolver: callable(code) -> full library name.

    Returns:
        Per-item JSON-ready dict matching D-01..D-04.
    """
    from shared_export_utils import remove_highlight_markers

    display = result.get('display', {}) or {}
    sys_id = display.get('id', '') or ''
    raw_header = result.get('raw_header', '') or ''

    parsed = meta_mgr.parse_full_id_components(raw_header) if raw_header else {
        'sys_id': None, 'ie_id': None, 'p_num': None,
    }
    final_sys_id = sys_id or parsed.get('sys_id') or ''

    snippet_raw = result.get('snippet', '') or ''
    snippet_clean = remove_highlight_markers(snippet_raw)
    match_terms = _extract_match_terms(snippet_raw)

    full_text = result.get('full_text', '') or ''
    excerpt = full_text[:500] if full_text else ''

    library_code = display.get('library_code', '') or ''
    library_name = library_resolver(library_code) if library_code else ''

    domains = domain_batch.get(final_sys_id, []) if final_sys_id else []
    primary_domain = domains[0]['domain'] if domains else None

    catalog = catalog_batch.get(final_sys_id, {}) if final_sys_id else {}
    dating = catalog.get('copy_date') if catalog else None

    score_raw = result.get('sort_score') or result.get('score') or 0
    try:
        score = round(float(score_raw), 4)
    except (ValueError, TypeError):
        score = 0.0

    return {
        'uid': result.get('uid', '') or '',
        'locator': {
            'sys_id': final_sys_id or None,
            'volume_ie': parsed.get('ie_id'),
            'p_num': parsed.get('p_num'),
        },
        'score': score,
        'shelfmark': display.get('shelfmark', '') or '',
        'title': display.get('title', '') or '',
        'library': {'code': library_code, 'name': library_name},
        'domain': primary_domain,
        'dating': dating,
        'snippet': snippet_clean,
        'excerpt': excerpt,
        'match_terms': match_terms,
        'image_url': _build_image_url(final_sys_id, parsed.get('p_num')),
    }
```

### Example 2: Top-level search payload assembly

```python
def serialize_search_payload(
    results: list[dict],
    *,
    meta_mgr: Any,
    query: str = '',
    mode: str = 'text',
    gap: Optional[int] = None,
    filters: Optional[dict] = None,
    warnings: Optional[list[str]] = None,
    total: Optional[int] = None,
) -> dict:
    """Phase 77 EXPORT-01/03. Same shape Phase 78 /api/search will inherit."""
    from datetime import datetime
    from genizah_core import get_library_display
    from shared.fjms_service import get_fjms_service

    fjms = get_fjms_service(thread_safe=True)
    sys_ids = [
        (r.get('display', {}) or {}).get('id', '')
        for r in results
        if (r.get('display', {}) or {}).get('id')
    ]

    domain_batch = (
        fjms.get_domains_for_sys_ids(sys_ids) if (fjms.is_available() and sys_ids) else {}
    )
    # Catalog batch — N small queries; consider adding a get_catalog_batch
    # helper to fjms_service if profiling shows >100ms for 50 results.
    catalog_batch = {}
    if fjms.is_available() and sys_ids:
        for sid in sys_ids:
            cat = fjms.get_catalog(sid)
            if cat:
                catalog_batch[sid] = cat

    def _lib(code):
        try:
            return get_library_display(code, short=False, lang='en') if code else ''
        except Exception:
            return code or ''

    items = [
        _serialize_item(
            r,
            meta_mgr=meta_mgr,
            domain_batch=domain_batch,
            catalog_batch=catalog_batch,
            library_resolver=_lib,
        )
        for r in results
    ]

    return {
        'schema_version': SCHEMA_VERSION,
        'source': 'search',
        'query': query or '',
        'mode': mode or 'text',
        'gap': gap,
        'filters': filters,
        'count': len(items),
        'total': total if total is not None else len(items),
        'warnings': list(warnings) if warnings else [],
        'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'results': items,
    }
```

### Example 3: Parallels grouping + matches[] assembly (Path A — assumes core extension)

```python
def serialize_parallels_payload(
    main_results: list[dict],
    filtered_results: Optional[list[dict]] = None,
    *,
    meta_mgr: Any,
    source_text: str = '',
    chunk_size: int = 5,
    mode: str = 'exact',
    max_freq: Optional[float] = None,
    boundary_options: Optional[dict] = None,
    warnings: Optional[list[str]] = None,
) -> dict:
    """Phase 77 EXPORT-02/03. Phase 80 /api/parallels inherits this shape."""
    from datetime import datetime
    from genizah_core import get_library_display
    from shared.fjms_service import get_fjms_service

    filtered_results = filtered_results or []

    # Phase 1: group raw results by sys_id derived from raw_header
    def _group(items: list[dict]) -> list[dict]:
        groups: dict[str, dict] = {}
        for item in items:
            raw_header = item.get('raw_header', '') or ''
            parsed = meta_mgr.parse_full_id_components(raw_header) if raw_header else {}
            sys_id = parsed.get('sys_id') or 'unknown'
            grp = groups.setdefault(sys_id, {
                'sys_id': sys_id,
                'representative': item,    # first item; used for shelfmark/title/etc.
                'items': [],
                'aggregate_score': 0.0,
            })
            grp['items'].append(item)
            # D-13 aggregate rule: SUM (matches existing UI: rendered max as sort key
            # but each item's score is already a SUM of chunk_score across hits_count chunks).
            # Picking SUM here gives "manuscript-level total intensity"; alternative is MAX.
            # Existing UI sorts by max_score across uid items but each item's score IS sum.
            # This is what user sees as "the score" of the manuscript card. Use SUM.
            grp['aggregate_score'] += float(item.get('score', 0.0) or 0.0)
        return sorted(groups.values(), key=lambda g: g['aggregate_score'], reverse=True)

    fjms = get_fjms_service(thread_safe=True)
    all_sys_ids = list({
        g['sys_id']
        for g in _group(main_results) + _group(filtered_results)
        if g['sys_id'] != 'unknown'
    })
    domain_batch = (
        fjms.get_domains_for_sys_ids(all_sys_ids) if (fjms.is_available() and all_sys_ids) else {}
    )
    catalog_batch = {}
    if fjms.is_available():
        for sid in all_sys_ids:
            cat = fjms.get_catalog(sid)
            if cat:
                catalog_batch[sid] = cat

    def _lib(code):
        try:
            return get_library_display(code, short=False, lang='en') if code else ''
        except Exception:
            return code or ''

    def _to_envelope_item(group: dict) -> dict:
        rep = group['representative']
        # Build a synthetic "result" the shared _serialize_item can consume.
        # We do NOT include matches[] here -- that's parallels-specific.
        # Strategy: lift display from rep, override score with aggregate, then add matches[].
        synth = dict(rep)
        synth['sort_score'] = round(group['aggregate_score'], 4)
        # Use representative for shelfmark/title via display; build display if missing.
        if 'display' not in synth:
            from shared_export_utils import remove_highlight_markers
            synth['display'] = {
                'id': group['sys_id'],
                'shelfmark': '', 'title': '', 'library_code': '',
            }
            # Populate via meta_mgr lookup
            shelf, title = meta_mgr.get_meta_for_id(group['sys_id'])
            synth['display']['shelfmark'] = shelf or ''
            synth['display']['title'] = title or ''
            synth['display']['library_code'] = meta_mgr.get_library_for_id(group['sys_id']) or ''

        item = _serialize_item(
            synth,
            meta_mgr=meta_mgr,
            domain_batch=domain_batch,
            catalog_batch=catalog_batch,
            library_resolver=_lib,
        )

        # D-13 matches[] -- one entry per chunk hit (Path A: requires core extension).
        # If core extension landed, each item in group['items'] has a 'chunk_hits' list.
        # If NOT (Path B), emit a single degenerate match.
        from shared_export_utils import remove_highlight_markers
        matches = []
        for sub in group['items']:
            chunk_hits = sub.get('chunk_hits')  # populated only if core extended
            if chunk_hits:
                for ch_idx, ch_text, ch_score, ms_snip in chunk_hits:
                    matches.append({
                        'chunk_index': ch_idx,            # 0-based per existing core loop
                        'source_chunk_text': ch_text,
                        'manuscript_snippet': remove_highlight_markers(ms_snip),
                        'score': round(float(ch_score), 4),
                    })
            else:
                # Path B fallback (degenerate single match)
                matches.append({
                    'chunk_index': None,
                    'source_chunk_text': sub.get('source_ctx', '') or '',
                    'manuscript_snippet': remove_highlight_markers(sub.get('text', '') or ''),
                    'score': round(float(sub.get('score', 0.0) or 0.0), 4),
                })
        item['matches'] = matches
        return item

    main_envelope = [_to_envelope_item(g) for g in _group(main_results)]
    filtered_envelope = [_to_envelope_item(g) for g in _group(filtered_results)]

    return {
        'schema_version': SCHEMA_VERSION,
        'source': 'parallels',
        'source_text': source_text,
        'chunk_size': chunk_size,
        'mode': mode,
        'max_freq': max_freq,
        'boundary_options': boundary_options,
        'count': len(main_envelope),
        'total': len(main_envelope),
        'warnings': list(warnings) if warnings else [],
        'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'results': main_envelope,
        'filtered': filtered_envelope,
    }
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Per-page state in `state` (global singleton) | Page-scoped `search_state` / `p_state` dataclasses | v7.9 (April 2026) | Filters, refinement chain, exclusions live on page-scoped state, NOT in `state`. Phase 77 download handlers must mirror filters into `state` to access them, OR use page-scoped storage carefully. |
| Excel/Word always-on (no enable check) | Excel/Word disabled when no results | v6.5.x (parallels page) | Phase 77 JSON button must mirror this on parallels (search.py never disables — different UX choice). |
| FastAPI's raw 422 errors | Custom error envelope `{error: {code, message}}` | Phase 78 (upcoming) | Phase 77 returns `Response("text", status_code=400)` like Excel/Word; Phase 78 introduces the envelope. **Do NOT pre-introduce the envelope in Phase 77** — it would diverge from existing handlers. |

**Deprecated/outdated:**
- The `state.current_search_query` reference in CONTEXT.md "already populated for filename generation" — INCORRECT; it is declared but never assigned. The planner must explicitly fix this.

## Project Constraints (from CLAUDE.md)

- **Python 3.10+** — type hints with `dict[str, ...]` syntax OK. (verified: requirements-lock.txt specifies modern syntax usage throughout.)
- **NiceGUI for web** — use `ui.button`, `ui.download` per existing pattern.
- **Dual app maintenance** — but Phase 77 is web-only per CONTEXT.md (desktop has no parallels download UI). The serializer module being in `shared/` keeps the option open for desktop in v7.11+.
- **Hebrew RTL** — JSON is encoding-neutral; Hebrew passes through as UTF-8. NO `\uXXXX` escaping (i.e., do not use `json.dumps(..., ensure_ascii=True)`). `JSONResponse` defaults are correct.
- **Type hints encouraged** — apply throughout serializer.
- **Documentation Maintenance** — Phase 77 closes by:
  - Updating `docs/OPEN_ISSUES.md` to mark `state.current_search_query` latent bug as fixed.
  - Updating `docs/CODE_INDEX.md` with the new `shared/search_serializer.py` module.
  - NO release version bump in Phase 77 itself — milestone v7.10 ships at end of Phase 82.
- **Testing** — `pytest tests/` is the canonical command. No `pytest.ini` exists; defaults apply. New tests at `tests/test_search_serializer.py`.
- **CI** — GitHub Actions matrix runs ruff + check_docs + pytest. Phase 77 must keep all three green.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `dating` field maps to FJMS `catalog.copy_date` (Hebrew text). | Standard Stack / Code Examples | If user wanted ISO-formatted year range or numeric centuries, planner picks wrong source. Confirm in plan. |
| A2 | `domain` field is the primary (first) domain from `get_domains_for_sys_ids` lookup, English only (not `domain_heb`). | Code Examples | Manuscripts often have multiple domains; picking first is non-deterministic without sort. Could emit `domains: [str, ...]` instead. |
| A3 | Score aggregation rule (D-13) for parallels is SUM of per-uid item scores. | Pattern 4 / Code Examples | UI uses `max_score` for sort but `score` per card is already SUM. User may have meant MAX-of-grouped-uids. |
| A4 | `chunk_index` is 0-based (per existing `for i, ...` loop in core). | Pattern 4 | If user expected 1-based human-readable, off-by-one in skill output. |
| A5 | Image URL emits `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` only — no Oxford fallback, no volume suffix. | Pattern 5 | Skill receives 404 for ~15% of Oxford manuscripts. Acceptable per D-08 ("no new IIIF resolution work"). |
| A6 | EXPORT-04 requires second-resolution timestamp (`%Y-%m-%dT%H%M%S`) to guarantee uniqueness for fast clicks. | Pitfall 6 | If minute-resolution is acceptable, simpler filename; if EXPORT-04 enforced strictly, second-resolution required. |
| A7 | Phase 77 inherits the same single-process `state` singleton risk as existing Excel/Word handlers; no per-session isolation needed. | Pitfall 7 | Multi-tenant deployment would cross-leak. Out of scope for v7.10 per requirements doc (no API keys, no auth). |

**A1, A2, A3, A4 are the high-risk ones — planner must lock or escalate to discuss-phase.**

## Open Questions (RESOLVED)

> All five questions resolved during planning; decisions are locked in `77-01-PLAN.md` `<plan_locked_decisions>` and `77-02-PLAN.md` `<plan_locked_decisions>`. Inline `RESOLVED:` markers below propagate the locked answers verbatim.

1. **D-13 path: A (extend core), B (degenerate), or C (re-discuss)?**
   - What we know: Core's `lab_composition_search` does not track per-chunk attribution.
   - What's unclear: Whether user accepts ~30 lines of additive core change to satisfy D-13 fidelity.
   - Recommendation: **Path A** — add `'chunk_hits': []` to `results_map[uid]` and append `(i, chunk_text, match_score, ms_snip_for_chunk)` per hit. Backwards compatible. Phase 80 benefits. Add a single core test asserting populated `chunk_hits`. If plan-checker objects to core scope creep, fall back to Path B with explicit response-shape doc note.
   - **RESOLVED:** Path A — Plan 02 extends `lab_composition_search` to track `chunk_hits` per uid (additive, backwards-compatible). One static-contract test in `tests/test_lab_composition_chunk_hits.py` locks the append signature `(i, chunk_text, match_score, ms_snip)`.

2. **Image URL: emit at all, or null + locator?**
   - What we know: D-01 says emit `image_url`; CONTEXT.md says use `display['img']` (which is a page number).
   - What's unclear: Whether the consumer wants a working URL or just the locator.
   - Recommendation: Emit a server-relative URL (`/api/nli_image_by_sysid/{sys_id}?page={p_num-1}`) — Claude skill running against the same web app can resolve it; metadata-only hits emit `null`. Don't add Oxford or volume-suffix complexity (Phase 79's job).
   - **RESOLVED:** Emit server-relative `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` for transcription-mode hits where `p_num` is non-null; emit `null` for metadata-only / Oxford-only hits. No multi-IE branching this phase.

3. **Filter echo: where does the dict come from?**
   - What we know: Filters live in page-scoped `search_state.filter_*`, not in global `state`.
   - What's unclear: Whether to mirror to `state.last_filters_applied` (clean) or read via `app.storage.user` (existing precedent for parallels source_text).
   - Recommendation: Mirror to `state.last_filters_applied` at the same line that sets `state.last_results`. Cleaner, testable, decoupled from NiceGUI session.
   - **RESOLVED:** Mirror to `state.last_filters_applied: dict` at search-time (Plan 01 Task 2, immediately before `state.last_results = results` at `web/pages/search.py:~4076`). Serializer reads from `state` only (not `app.storage.user`).

4. **`primary_domain` vs `domains: [str, ...]` array?**
   - What we know: D-01 says singular `domain`. UI shows multiple domain badges.
   - What's unclear: Whether singular is enough for the Claude skill consumer.
   - Recommendation: Singular for now (matches CONTEXT.md), but add a comment in the serializer flagging this — future plans may upgrade to array without breaking existing consumers if the field becomes `domain: str | None` → `domain: str | None | list[str]` is a breaking change. Better to pick `domains: list[str]` early. Escalate.
   - **RESOLVED:** Emit `domains: list[str]` (empty list when no domains known). This is a deliberate **deviation from CONTEXT.md D-01** documented in `77-01-PLAN.md` `<plan_locked_decisions>` item 7 — plural is forward-compatible; singular would be a breaking change to upgrade later.

5. **Score aggregation for parallels (D-13 footnote): MAX or SUM?**
   - What we know: UI sorts by `max_score` across uids per sys_id, but each item's `score` is already SUM of chunk hits.
   - What's unclear: At the manuscript-group level, what the "manuscript-level top-line score" should be.
   - Recommendation: SUM (across uids in same sys_id) — preserves D-13 footnote "planner picks aggregate rule from existing UI logic — e.g., max or sum". UI logic shows MAX as the sort key but card-shown score is already SUM. Document choice in plan.
   - **RESOLVED:** SUM (across uids in same sys_id). Matches the per-card score the existing UI already displays. Locked in `77-01-PLAN.md` `<plan_locked_decisions>` item 5; asserted by `tests/test_search_serializer.py::test_parallels_score_aggregate_is_sum`.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python `json` | Stdlib | ✓ | stdlib | — |
| `fastapi` | route decoration | ✓ | 0.135.1 | — |
| `starlette.responses.JSONResponse` | response | ✓ | 0.52.1 | — |
| `nicegui.ui.download` | toolbar button | ✓ | 3.8.0 | — |
| `shared_export_utils` | filename, sanitize | ✓ | local | — |
| `shared.fjms_service` | domain/catalog batch | ✓ | local | If `fjms.is_available()` is False, emit `domain: null`, `dating: null` (graceful degrade — not a hard fail). |
| `pytest` | tests | ✓ | per requirements-lock | — |

**Missing dependencies with no fallback:** None.

**Missing dependencies with fallback:** None — `fjms.is_available()` is checked already and gracefully returns `{}`.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (standard via requirements-lock) |
| Config file | none — pytest defaults; `tests/conftest.py` adds repo root to sys.path |
| Quick run command | `pytest tests/test_search_serializer.py -x -q` |
| Full suite command | `pytest tests/` (~50 test files; phase 77 should keep baseline green) |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| EXPORT-01 | `/search` toolbar JSON button triggers download via `/api/export/json` | unit (serializer) + manual (button click on running server) | `pytest tests/test_search_serializer.py::test_search_envelope_shape -x` | ❌ Wave 0 |
| EXPORT-01 | `/api/export/json` returns 400 when `state.last_results` empty | unit (handler error path) | `pytest tests/test_search_serializer.py::test_export_json_handler_empty -x` | ❌ Wave 0 |
| EXPORT-01 | Filename includes ISO timestamp + `genizah-search-` prefix | unit (serializer / filename helper) | `pytest tests/test_search_serializer.py::test_filename_format -x` | ❌ Wave 0 |
| EXPORT-02 | `/parallels` toolbar JSON button triggers download via `/api/export/parallels/json` | unit + manual | `pytest tests/test_search_serializer.py::test_parallels_envelope_shape -x` | ❌ Wave 0 |
| EXPORT-02 | `serialize_parallels_payload` separates `results` and `filtered` (D-11) | unit | `pytest tests/test_search_serializer.py::test_parallels_filtered_separation -x` | ❌ Wave 0 |
| EXPORT-02 | One result per manuscript with `matches[]` (D-13) | unit | `pytest tests/test_search_serializer.py::test_parallels_groups_by_manuscript -x` | ❌ Wave 0 |
| EXPORT-03 | Single source of truth: both serialize functions share `_serialize_item` | unit (structural) | `pytest tests/test_search_serializer.py::test_serializers_share_serialize_item -x` | ❌ Wave 0 |
| EXPORT-03 | Modifying `_serialize_item` to add a field shows up in BOTH search and parallels output | unit (behavioral cross-test) | `pytest tests/test_search_serializer.py::test_search_and_parallels_share_item_shape -x` | ❌ Wave 0 |
| EXPORT-04 | Two consecutive downloads produce two distinct filenames | unit | `pytest tests/test_search_serializer.py::test_filename_uniqueness_consecutive -x` | ❌ Wave 0 |
| D-04 | Every result has BOTH `uid` and `locator` fields | unit | `pytest tests/test_search_serializer.py::test_locator_always_both_present -x` | ❌ Wave 0 |
| D-04 | `volume_ie`/`p_num` are `null` for metadata-only hits | unit | `pytest tests/test_search_serializer.py::test_metadata_only_hit_shape -x` | ❌ Wave 0 |
| D-03 | `snippet` stripped of `*term*`; `match_terms` populated correctly | unit | `pytest tests/test_search_serializer.py::test_snippet_stripped_match_terms_extracted -x` | ❌ Wave 0 |
| D-05 | Empty results envelope has well-formed shape (count=0, results=[], warnings=[]) | unit | `pytest tests/test_search_serializer.py::test_empty_results_envelope -x` | ❌ Wave 0 |
| D-07 | `warnings: []` always present | unit | `pytest tests/test_search_serializer.py::test_warnings_always_present -x` | ❌ Wave 0 |
| D-09 | `source: 'search'` vs `source: 'parallels'` correctly tagged | unit | `pytest tests/test_search_serializer.py::test_source_field_tags -x` | ❌ Wave 0 |
| D-10 | `schema_version: 1` is a top-level constant accessible from module | unit | `pytest tests/test_search_serializer.py::test_schema_version_constant -x` | ❌ Wave 0 |
| Locator round-trip readiness for Phase 79 | `{sys_id, volume_ie, p_num}` shape matches what Phase 79 will accept | unit (asserts dict keys + types) | `pytest tests/test_search_serializer.py::test_locator_phase79_shape -x` | ❌ Wave 0 |
| Manual integration | Run server, query "אגדה", click JSON button, open downloaded file, assert valid JSON + visible Hebrew + correct shape | manual smoke | `python -m web.main` + browser test | n/a |
| CI baseline | `pytest tests/` (full suite) stays green | regression | `pytest tests/` | ✓ existing |

### Sampling Rate
- **Per task commit:** `pytest tests/test_search_serializer.py -x -q` (~3 sec for ~20 unit tests)
- **Per wave merge:** `pytest tests/test_search_serializer.py tests/test_export_service.py -x` (~10 sec — verifies no regression in adjacent export module)
- **Phase gate:** `pytest tests/` full suite (~3-4 min). CI matrix re-runs on push.

### Wave 0 Gaps
- [ ] `tests/test_search_serializer.py` — covers EXPORT-01/02/03/04 + D-03/04/05/07/09/10/13
- [ ] Test fixtures: synthetic search result dict (with `display`, `snippet`, `full_text`, `uid`, `raw_header`, `sort_score`)
- [ ] Test fixture: synthetic parallels result list with multiple uids per sys_id (proves grouping)
- [ ] Test fixture: synthetic parallels result list with empty `chunk_hits` (Path B degenerate)
- [ ] Mock `MetadataManager` (parse_full_id_components stub returning known {sys_id, ie_id, p_num}) — pattern from `tests/test_export_service.py:240-245`
- [ ] Mock `FjmsService.get_domains_for_sys_ids` and `get_catalog`
- [ ] Manual smoke-check checklist for `/search` and `/parallels` JSON downloads — added to phase gate, not automated

**Test-shape examples:**

```python
# tests/test_search_serializer.py

def test_serializers_share_serialize_item():
    """EXPORT-03 structural: both top-level functions reach into the same private helper."""
    from shared import search_serializer as ss
    # Inspect module: only ONE _serialize_item should exist
    private_helpers = [n for n in dir(ss) if n.startswith('_serialize')]
    # Must include _serialize_item (shared) but NOT _serialize_search_item or _serialize_parallels_item
    assert '_serialize_item' in private_helpers
    assert '_serialize_search_item' not in private_helpers, "EXPORT-03 violation: separate search-specific serializer"
    assert '_serialize_parallels_item' not in private_helpers, "EXPORT-03 violation: separate parallels-specific serializer"

def test_search_and_parallels_share_item_shape(monkeypatch):
    """EXPORT-03 behavioral: every key emitted by search items is also emitted by parallels items."""
    # Build matched fixtures so both serializers see the same underlying result dict.
    # Assert search_payload['results'][0].keys() ⊇ parallels_payload['results'][0].keys() - {'matches'}
    ...

def test_locator_always_both_present():
    """D-04: every item has BOTH uid and locator, even for metadata-only hits."""
    payload = serialize_search_payload([metadata_only_fixture()], meta_mgr=fake_mgr, query="x", mode="Title")
    item = payload['results'][0]
    assert 'uid' in item, "uid field must always exist"
    assert 'locator' in item, "locator field must always exist"
    assert item['uid'] == ''  # metadata-only -> empty string per D-04
    assert item['locator']['sys_id']  # always populated when sys_id is known
    assert item['locator']['volume_ie'] is None  # null for metadata-only
    assert item['locator']['p_num'] is None  # null for metadata-only

def test_filename_uniqueness_consecutive():
    """EXPORT-04: two consecutive serialize calls produce distinct filenames."""
    # Either hit the handler twice OR call the filename helper twice with sleep(1)
    fn1 = build_search_filename()  # uses second-resolution ts
    time.sleep(1.0)
    fn2 = build_search_filename()
    assert fn1 != fn2, "EXPORT-04 violation: minute-resolution filename collision"
```

### Locator round-trip readiness for Phase 79
Even though Phase 79 `/api/browse` is not built, the locator shape is locked NOW. Add a unit test:

```python
def test_locator_phase79_shape():
    """Locator emits exactly the keys Phase 79 /api/browse will accept (per ROADMAP)."""
    payload = serialize_search_payload([standard_hit_fixture()], meta_mgr=fake_mgr, query="x", mode="text")
    locator = payload['results'][0]['locator']
    assert set(locator.keys()) == {'sys_id', 'volume_ie', 'p_num'}, \
        "Locator keys must match Phase 79 contract: {sys_id, volume_ie, p_num}"
    assert isinstance(locator['sys_id'], (str, type(None)))
    assert isinstance(locator['volume_ie'], (str, type(None)))
    assert isinstance(locator['p_num'], (str, type(None)))
```

This test fails fast if Phase 79's expected shape diverges — caught at Phase 77 plan-check, not Phase 79 implementation.

## Sources

### Primary (HIGH confidence)
- [VERIFIED: web/api.py:1806-1908] Existing `/api/export/excel`, `/api/export/word`, `/api/export/parallels/{excel,word}` handlers — Phase 77 mirrors exactly.
- [VERIFIED: web/export_service.py:286-411] `export_search_results_excel/word` body — confirms `state.last_results` consumption pattern, `make_safe_filename` usage, `Response` with `Content-Disposition`.
- [VERIFIED: web/export_service.py:477-655] Parallels Excel/Word — confirms D-11 split between `main_results` and `filtered_results`.
- [VERIFIED: shared_export_utils.py:84-96, 103-139, 162-182] `remove_highlight_markers`, `make_safe_filename`, `encode_filename_for_header` — exact reusable surface.
- [VERIFIED: web/state.py:26-32] `last_results` and `parallels_results`/`parallels_filtered` fields.
- [VERIFIED: web/state.py:27 + grep] `current_search_query` declared but never assigned — latent bug.
- [VERIFIED: web/pages/search.py:1441-1446] Toolbar Word/Excel button construction.
- [VERIFIED: web/pages/parallels.py:1230-1235, 1923-1924, 2607-2616] Parallels toolbar buttons + enable/disable lifecycle.
- [VERIFIED: web/pages/search.py:4015-4080] Search execution path where `state.last_results` is set; identifies line for `state.current_search_query` patch.
- [VERIFIED: web/pages/search_results.py:629-657] Image URL construction proves `display['img']` is a page index.
- [VERIFIED: genizah_core.py:3506-3535] `MetadataManager.parse_full_id_components` returns `{sys_id, ie_id, p_num, fl_id}`.
- [VERIFIED: genizah_core.py:1699-1717] `get_library_display(code, short=False, lang='en')`.
- [VERIFIED: genizah_core.py:1280-1374] `lab_composition_search` confirms per-chunk attribution is collapsed into `total_score`/`hits_count`.
- [VERIFIED: genizah_core.py:7138-7180] `_execute_metadata_search` confirms metadata-only hits emit `uid: ''`, `full_text: ''`, `metadata_only: True`.
- [VERIFIED: genizah_core.py:4620-4636] `MetadataManager.get_display_data` confirms `display['img']` = `p_num` (string).
- [VERIFIED: shared/fjms_service.py:730-825] `get_domains` / `get_domains_for_sys_ids` shapes.
- [VERIFIED: shared/fjms_service.py:2136-2177] `get_catalog` returning `copy_date` for `dating`.
- [VERIFIED: tests/test_export_service.py:240-300] Existing test fixture/mock patterns to follow.
- [VERIFIED: tests/conftest.py] No `pytest.ini` — pytest defaults; conftest adds repo root to sys.path.
- [VERIFIED: requirements-lock.txt] FastAPI 0.135.1, starlette 0.52.1, nicegui 3.8.0.

### Secondary (MEDIUM confidence)
- [CITED: .planning/phases/77-serializer-json-export/77-CONTEXT.md] All D-01..D-14 decisions, score-aggregation footnote, image-URL claim flagged for correction.
- [CITED: .planning/REQUIREMENTS.md §JSON Export] EXPORT-01..04 requirement text.
- [CITED: .planning/ROADMAP.md §Phase 77 + §Phase 79] Locator obligation, success criteria.

### Tertiary (LOW confidence — flagged in Open Questions)
- ASSUMED: SUM is the right manuscript-level score aggregation rule for parallels D-13 — based on reading existing UI grouping logic but not confirmed with user.
- ASSUMED: 0-based `chunk_index` matches existing core convention (`for i, ...` loop variable) — true if Path A core extension is taken; null for Path B.
- ASSUMED: Hebrew passes through `JSONResponse` without `\uXXXX` escaping — based on starlette default `ensure_ascii=False` behavior; would need verification by manual download spot-check (which is in the phase gate anyway).

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — every component verified in current codebase.
- Architecture: HIGH — pattern is "mirror the existing 4 Excel/Word handlers"; well-trodden ground.
- Pitfalls: HIGH — three are verified factual gaps (display['img'], current_search_query, chunk_attribution); the rest are well-understood NiceGUI/FastAPI gotchas.
- Open questions: MEDIUM — most are user-decision territory rather than research gaps.

**Research date:** 2026-04-27
**Valid until:** 2026-05-27 (stable surface; v7.10 still in early phases — re-verify if Phase 78 lands first and changes `state` shape).

---

## RESEARCH COMPLETE

**Phase:** 77 — Serializer & JSON Export
**Confidence:** HIGH

### Key Findings

1. **`display['img']` is a page number, NOT a URL.** CONTEXT.md D-08 / Discussion-Log Q1 implies it's a URL; codebase confirms otherwise (genizah_core.py:4632, web/pages/search_results.py:641). Planner must lock the actual image URL strategy: emit `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` server-relative.

2. **`state.current_search_query` is declared but never assigned.** The existing Excel/Word handlers silently fall back to `make_safe_filename("")` → `"genizah.xlsx"`. Phase 77 plan must include a small patch in `web/pages/search.py:~4076` to populate it (and add `state.current_search_mode`, `state.last_filters_applied`, `state.last_search_warnings`).

3. **D-13 per-chunk attribution is impossible from current core.** `lab_composition_search` (genizah_core.py:1373) collapses chunk hits into a single `total_score`/`hits_count` per uid. Three planner paths: A (extend core, recommended), B (degenerate single-element matches array), C (re-discuss). Plan-check decision point.

4. **Reuse surface is rich.** `shared_export_utils` provides every primitive Phase 77 needs: `remove_highlight_markers`, `make_safe_filename`, `encode_filename_for_header`. `genizah_core.MetadataManager.parse_full_id_components` produces the exact locator shape D-04 requires. `shared.fjms_service` batch-lookups for domain/catalog. The serializer is a 200-line composer.

5. **EXPORT-03 (single source of truth) is a structural test, not just a code-review note.** `tests/test_search_serializer.py::test_serializers_share_serialize_item` should inspect the module to assert ONE `_serialize_item` exists (no `_serialize_search_item` or `_serialize_parallels_item` shadows). This catches drift even if both functions look correct individually.

### File Created
`.planning/phases/77-serializer-json-export/77-RESEARCH.md`

### Confidence Assessment
| Area | Level | Reason |
|------|-------|--------|
| Standard Stack | HIGH | Every library and helper verified live in codebase. |
| Architecture | HIGH | Pattern is "mirror 4 existing handlers" + "add 1 new shared/ module"; precedents abundant. |
| Pitfalls | HIGH | Three pitfalls (img/current_query/chunk_attribution) are verified factual gaps with grep evidence. |
| D-13 path choice | MEDIUM | Three options enumerated; user-decision territory but recommendation provided. |
| Score aggregation rule | MEDIUM | Inferred from existing UI logic; needs lock-in at plan-check. |

### Open Questions (escalation candidates)
1. D-13 path A vs B vs C — recommend Path A but flag for plan-check.
2. `domain` singular vs `domains[]` plural — recommend escalating to discuss-phase if user signals this matters.
3. Image URL strategy — recommend emitting server-relative URL, not just locator.
4. Filter echo source — recommend `state.last_filters_applied` mirror.
5. Score aggregation rule for D-13 — recommend SUM; document in plan.

### Ready for Planning
Research complete. Planner can now create PLAN.md files. Recommended plan decomposition (granularity=fine):

- Plan 01: Module skeleton + envelope + `_serialize_item` + search payload + state.current_search_query patch + state field additions
- Plan 02: Parallels payload + grouping + matches[] (with D-13 path decision documented)
- Plan 03 (optional, only if Path A): core extension to track `chunk_hits` + core unit test
- Plan 04: HTTP handlers (`/api/export/json`, `/api/export/parallels/json`) + filename builder
- Plan 05: Toolbar buttons (search.py + parallels.py) + enable/disable wiring + Hebrew/English tooltip translations
- Plan 06: Test suite + manual smoke-check checklist + OPEN_ISSUES update for `state.current_search_query` latent bug
