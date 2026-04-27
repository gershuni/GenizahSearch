# Phase 77: Serializer & JSON Export - Pattern Map

**Mapped:** 2026-04-27
**Files analyzed:** 8 (2 new, 6 modified — 1 of which is conditional)
**Analogs found:** 8 / 8 (every file has a precise in-repo analog; coverage is exact)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `shared/search_serializer.py` (NEW) | shared/ service module | transform (read-only) | `shared/document_service.py` (lines 1-80) + `web/export_service.py` shape | exact (cross-app shared service convention) |
| `tests/test_search_serializer.py` (NEW) | test | unit (pure transform + MagicMock) | `tests/test_export_service.py` (lines 237-306, fixtures incl. `mock_meta_mgr`) | exact |
| `web/api.py` (MODIFIED — add 2 routes) | FastAPI handler | request-response (stateful download) | `web/api.py:1806-1908` (Excel/Word handlers) | exact (copy-and-mutate; only `media_type` and serializer call differ) |
| `web/pages/search.py` (MODIFIED — button + state populate) | NiceGUI page render + state plumbing | event-driven UI + state mutation | Button: `web/pages/search.py:1441-1446`; State: `web/pages/search.py:4076-4077` | exact |
| `web/pages/parallels.py` (MODIFIED — button + lifecycle) | NiceGUI page render | event-driven UI with enable/disable lifecycle | `web/pages/parallels.py:1230-1235, 1923-1924, 2607-2616` | exact |
| `web/state.py` (MODIFIED — add 4 fields) | state container | global singleton attribute add | `web/state.py:26-31` (existing `last_results`, `current_search_query`, `parallels_results`, `parallels_filtered` declarations) | exact (mirror existing pattern) |
| `web/export_service.py` (MODIFIED — optional thin wrappers) | service-layer thin wrapper | transform (returns `(bytes, filename)`) | `web/export_service.py:286-355` (search Excel) and `:477-561` (parallels Excel) | role-match (signature + return-shape only; no openpyxl/python-docx work) |
| `genizah_core.py` (CONDITIONAL — Path A only, D-13) | core engine extension | transform (per-chunk attribution accumulation) | `genizah_core.py:1352-1381` (`results_map[uid]` initialization + per-chunk accumulator block) | role-match (additive: append a `chunk_hits` list to the same dict already being mutated) |

---

## Pattern Assignments

### `shared/search_serializer.py` (NEW — shared/ service module, transform)

**Analog:** `shared/document_service.py` for module shape; `web/export_service.py` for the per-result data extraction pattern.

**Module header pattern** (copy from `shared/document_service.py:1-41`):
```python
# -*- coding: utf-8 -*-
"""
Search Serializer for Phase 77 JSON exports + Phase 78+ /api/* responses.

This module is the single source of truth for the "Claude-friendly JSON" payload
shape. Two named functions share a private _serialize_item() per D-14 / EXPORT-03:

  - serialize_search_payload(results, *, meta_mgr, query, mode, ...) -> dict
  - serialize_parallels_payload(main, filtered, *, meta_mgr, source_text, ...) -> dict

Both Phase 78 /api/search and Phase 80 /api/parallels import these functions;
modifying _serialize_item() updates download AND API in lockstep.
"""

import logging
import re
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
```

**Public API surface** (D-14):
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
) -> dict: ...

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
) -> dict: ...
```

**Per-result extraction pattern** — copy the field-pulling structure from `web/export_service.py:315-323`:
```python
# This is the existing Excel exporter pattern that the serializer mirrors:
for res in results:
    display = res.get('display', {})
    snippet = clean_text_single_line(remove_highlight_markers(res.get('snippet', '')))
    # Get full library name
    library_code = display.get('library_code', '')
    library_name = self.get_library_display(library_code, short=False) if library_code else ''
```
JSON serializer mirrors but: emits structured dict (not row), uses `round(score, 4)`, adds locator + match_terms + image_url, and DOES NOT include `full_text` (D-02 — replaced by `excerpt` slice `[:500]`).

**Locator extraction** — `meta_mgr.parse_full_id_components(raw_header)` returns `{sys_id, ie_id, p_num, fl_id}` per RESEARCH §Pattern 3 (genizah_core.py:3506-3535). Pass `meta_mgr` as a serializer kwarg per RESEARCH §Pitfall 5 — do NOT import from `genizah_core` at module top (avoids `web → core` import-time coupling and matches `ExportService(meta_mgr=...)` precedent).

**Match-term extraction** (D-03; RESEARCH §Don't Hand-Roll):
```python
def _extract_match_terms(snippet: Optional[str]) -> list[str]:
    if not snippet:
        return []
    found = re.findall(r'\*([^*]+)\*', snippet)
    seen, out = set(), []
    for t in found:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out
```

**Image URL builder** (D-08; RESEARCH §Pattern 5 — note: `display['img']` is `p_num`, NOT a URL):
```python
def _build_image_url(sys_id: Optional[str], p_num: Optional[str]) -> Optional[str]:
    if not sys_id or not p_num:
        return None
    try:
        page_idx = max(0, int(p_num) - 1)
    except (ValueError, TypeError):
        return None
    return f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}"
```

**Batch-lookup pattern** (avoid N+1 SQLite hits; per RESEARCH §Anti-Patterns):
```python
from shared.fjms_service import get_fjms_service
fjms = get_fjms_service(thread_safe=True)
sys_ids = [(r.get('display') or {}).get('id') for r in results if (r.get('display') or {}).get('id')]
domain_batch = fjms.get_domains_for_sys_ids(sys_ids) if (fjms.is_available() and sys_ids) else {}
```

**Envelope shape** (D-05/06/07/08/09/10):
```python
return {
    'schema_version': SCHEMA_VERSION,            # D-10
    'source': 'search',                          # D-09
    'query': query or '',                        # D-06
    'mode': mode or 'text',                      # D-06
    'gap': gap,                                  # D-06
    'filters': filters,                          # D-06
    'count': len(items),                         # D-08
    'total': total if total is not None else len(items),  # D-08
    'warnings': list(warnings) if warnings else [],       # D-07 (always present)
    'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
    'results': items,
}
```

**Per-item shape** (D-01/02/03/04):
```python
return {
    'uid': result.get('uid', '') or '',          # D-04: may be empty string
    'locator': {                                 # D-04: ALWAYS present, fields may be null
        'sys_id': final_sys_id or None,
        'volume_ie': parsed.get('ie_id'),
        'p_num': parsed.get('p_num'),
    },
    'score': round(float(score_raw or 0), 4),    # Discretion: rename sort_score -> score
    'shelfmark': display.get('shelfmark', '') or '',
    'title': display.get('title', '') or '',
    'library': {'code': library_code, 'name': library_name},  # D-01
    'domain': primary_domain,                    # D-01 (English; first domain)
    'dating': catalog.get('copy_date'),          # D-01 (HE text from FJMS)
    'snippet': snippet_clean,                    # D-03 (markers stripped)
    'excerpt': (full_text or '')[:500],          # D-02 (no full_text field)
    'match_terms': match_terms,                  # D-03
    'image_url': _build_image_url(...),          # D-01 (server-relative)
}
```

**Parallels grouping** (D-13; RESEARCH §Pattern 4) — group `main_results` and `filtered_results` separately by `sys_id` extracted via `meta_mgr.parse_full_id_components(item['raw_header'])`. Each group becomes one envelope item with a `matches: [...]` array. Path A (preferred): consume per-chunk `chunk_hits` list added to core's `results_map[uid]`; Path B fallback: emit single degenerate match using existing `source_ctx`/`text`/`score` fields.

---

### `tests/test_search_serializer.py` (NEW — pytest unit tests)

**Analog:** `tests/test_export_service.py:237-306` (mock fixtures + sample data dicts).

**Mock meta_mgr fixture** (copy verbatim from `tests/test_export_service.py:240-245`):
```python
@pytest.fixture
def mock_meta_mgr(self):
    """Create a mock MetadataManager."""
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ("T-S 12.345", "Test Title")
    return mgr
```
**Delta from analog:** also stub `mgr.parse_full_id_components.return_value = {'sys_id': '99...', 'ie_id': 'IE...', 'p_num': '7', 'fl_id': '...'}` — Phase 77 needs this method which `tests/test_export_service.py` does not.

**Sample search-results fixture** (copy from `tests/test_export_service.py:253-276`):
```python
@pytest.fixture
def sample_search_results(self):
    return [
        {
            'display': {
                'shelfmark': 'T-S 12.345',
                'title': 'כתב יד עברי',
                'id': '9912345678901234',
            },
            'snippet': 'This is a *highlighted* snippet',
            'full_text': 'Full text content here',
            'sort_score': 0.95,
        },
        ...
    ]
```
Add a few extra rows for Phase 77 coverage: one row with `'raw_header': 'header_99...IE_p7'` (locator-from-raw-header path), one with NO display.id (uid-fallback path), one with snippet containing `*foo* *bar* *foo*` (match-term dedupe).

**Sample parallels fixture** (copy from `tests/test_export_service.py:297-306`):
```python
return [
    {
        'raw_header': 'header_9912345678901234_page1',
        'score': 85,
        'source_ctx': 'Source *context* text',
        'text': 'Manuscript match text',
    },
]
```
Phase 77 additions: 2-3 rows sharing the same `sys_id` (so the grouping path produces a multi-`matches` item) plus 1 row in `filtered_results` (so D-11 separation is exercised).

**Test cases to write** (mapped to success criteria):
- `test_schema_version_is_one` — module exports `SCHEMA_VERSION == 1` (D-10)
- `test_envelope_has_required_keys` — `{schema_version, source, query, mode, count, total, warnings, generated_at, results}` (D-05/07/09/10)
- `test_warnings_always_present_on_clean_query` — `warnings == []` (D-07)
- `test_locator_always_both_uid_and_locator` — both keys present, fields may be null (D-04)
- `test_snippet_stripped_of_asterisks_and_match_terms_populated` (D-03)
- `test_score_rounded_to_4_decimals` (Discretion)
- `test_no_full_text_field_in_item` (D-02)
- `test_image_url_is_relative_or_null` (D-08)
- `test_parallels_filtered_separated_into_filtered_array` (D-11)
- `test_parallels_one_result_per_manuscript_with_matches_array` (D-13)
- `test_empty_results_envelope_is_well_formed`

---

### `web/api.py` (MODIFIED — add 2 routes inside `init_api_routes()`)

**Analog:** `web/api.py:1806-1908` — Excel/Word handlers. The JSON handlers are structural copies with three deltas:

1. `media_type='application/json'` (or use `JSONResponse` which sets it automatically — `web/api.py:1374, 1538, 1585` already does this 8+ times)
2. Call `serialize_search_payload(...)` instead of `export_svc.export_search_results_excel(...)` and pass the returned dict to `JSONResponse`
3. Filename uses ISO timestamp instead of search-query-derived (`make_safe_filename`) — per CONTEXT.md Discretion

**Direct verbatim copy from `web/api.py:1806-1827` for the search route:**
```python
@app.get('/api/export/excel')
def export_excel():
    """Export search results to Excel format using unified export service."""
    if not state.last_results:
        return Response("No results to export", status_code=400)

    try:
        export_svc = get_export_service(state.meta_mgr)
        content, filename = export_svc.export_search_results_excel(
            state.last_results,
            state.current_search_query or ""
        )
        return Response(
            content=content,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": encode_filename_for_header(filename)}
        )
    except ValueError as e:
        return Response(str(e), status_code=400)
    except Exception as e:
        logger.error(f"Export Excel error: {e}")
        return Response("Export failed", status_code=500)
```

**Phase 77 JSON handler — mutate the above as follows:**
```python
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
            meta_mgr=state.meta_mgr,
            query=state.current_search_query or "",
            mode=getattr(state, 'current_search_mode', 'text'),
            gap=getattr(state, 'current_search_gap', None),
            filters=getattr(state, 'last_filters_applied', None),
            warnings=getattr(state, 'last_search_warnings', None),
        )
        ts = datetime.utcnow().strftime('%Y-%m-%dT%H%M%S')  # second resolution per RESEARCH §Pitfall 6
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

**Parallels JSON handler — copy from `web/api.py:1852-1879`** (existing `export_parallels_excel`):
```python
@app.get('/api/export/parallels/excel')
def export_parallels_excel():
    from nicegui import app as nicegui_app

    parallels_results = state.parallels_results or []
    filtered_results = state.parallels_filtered or []
    source_text = nicegui_app.storage.user.get('parallels_source_text', '')

    if not parallels_results and not filtered_results:
        return Response("No parallels results to export", status_code=400)
    ...
```
Phase 77 mirrors: `source_text` from same `app.storage.user.get('parallels_source_text', '')`, but call `serialize_parallels_payload(parallels_results, filtered_results, meta_mgr=state.meta_mgr, source_text=source_text, chunk_size=..., mode=..., ...)` and return `JSONResponse(payload, headers={...})`. Filename: `f"genizah-parallels-{ts}.json"`.

**`JSONResponse` import is already a known idiom** (`web/api.py:1374, 1538, 1585, 1596, 1610, 1718` all do `from starlette.responses import JSONResponse` inline). Phase 77 follows the inline-import convention rather than adding to module top.

**`encode_filename_for_header` is already imported at module top** (`web/api.py:9`) — no new top-level import needed.

---

### `web/pages/search.py` (MODIFIED — JSON button + state populate)

**Analog (button placement):** `web/pages/search.py:1441-1446` — exact slot, immediately after Excel button.

**Existing toolbar** (lines 1441-1446):
```python
ui.button(icon='description', on_click=lambda: ui.download('/api/export/word')).props(
    'flat round dense size=sm'
).tooltip(tr('Export Word'))
ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/excel')).props(
    'flat round dense size=sm'
).tooltip(tr('Export Excel'))
```

**Phase 77 addition — slot in third button after line 1446:**
```python
ui.button(icon='data_object', on_click=lambda: ui.download('/api/export/json')).props(
    'flat round dense size=sm'
).tooltip(tr('Export JSON'))
```
**Delta from parallels:** search.py does NOT disable export buttons when no results (UX choice — different from parallels). Per RESEARCH §State of the Art: keep the same always-enabled behavior so the button is consistent with its Excel/Word neighbors.

**Analog (state populate):** `web/pages/search.py:4076-4077` — the existing single-line `state.last_results = results`.

**Existing block** (lines 4076-4077):
```python
# Finalize search state for immediate display
state.last_results = results
```

**Phase 77 patch — populate the additional state fields BEFORE `state.last_results = results`** (per RESEARCH §Pitfall 2):
```python
# Phase 77: populate state for JSON export envelope echo (D-06) and filename
state.current_search_query = clean_query
state.current_search_mode = mode
state.current_search_gap = int(gap_input.value) if gap_input.value else None
state.last_filters_applied = {
    'domains': list(search_state.filter_domains) if search_state.filter_domains else [],
    'authors': list(search_state.filter_authors) if search_state.filter_authors else [],
    'works': list(search_state.filter_works) if search_state.filter_works else [],
    # ... copy verbatim from search_state.filter_* fields per RESEARCH §Pitfall 4
}
state.last_search_warnings = []  # Phase 78 will populate; Phase 77 always [] per D-07
state.last_results = results
```
**Critical:** the `clean_query` variable is already in scope at line 4076 (it is the input the search just executed against). Verify at execute-time but RESEARCH §Pitfall 2 confirms it.

---

### `web/pages/parallels.py` (MODIFIED — JSON button + lifecycle)

**Analog (button placement):** `web/pages/parallels.py:1230-1235`.

**Existing toolbar**:
```python
export_word_btn = ui.button(icon='description', on_click=lambda: ui.download('/api/export/parallels/word')).props(
    'flat round dense disable'
).tooltip(tr('Export Word'))
export_excel_btn = ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/parallels/excel')).props(
    'flat round dense disable'
).tooltip(tr('Export Excel'))
```

**Phase 77 addition (capture handle for lifecycle disable/enable):**
```python
export_json_btn = ui.button(icon='data_object', on_click=lambda: ui.download('/api/export/parallels/json')).props(
    'flat round dense disable'
).tooltip(tr('Export JSON'))
```

**Lifecycle wiring (analog: lines 1923-1924, 2607-2616):**

**Disable on no-results / reset** (line 1923-1924 pattern):
```python
# Disable export buttons (no results)
export_word_btn.props('disable')
export_excel_btn.props('disable')
export_json_btn.props('disable')   # Phase 77 add
```

**Disable in render_results empty branch** (line 2607-2612 pattern):
```python
if not results and not filtered_results:
    export_word_btn.props('disable')
    export_excel_btn.props('disable')
    export_json_btn.props('disable')   # Phase 77 add
    with results_container:
        show_empty_state()
    return
```

**Enable when results arrive** (line 2614-2616 pattern):
```python
# Enable export buttons now that we have results
export_word_btn.props(remove='disable')
export_excel_btn.props(remove='disable')
export_json_btn.props(remove='disable')   # Phase 77 add
```

---

### `web/state.py` (MODIFIED — add 4 fields)

**Analog:** `web/state.py:14-31` — the existing `init()` body that declares `last_results`, `current_search_query`, `parallels_results`, `parallels_filtered`.

**Existing block** (lines 26-31):
```python
self.last_results: List[Dict[str, Any]] = []
self.current_search_query: str = ""

# Parallels results (for export functionality)
self.parallels_results: List[Dict[str, Any]] = []
self.parallels_filtered: List[Dict[str, Any]] = []
```

**Phase 77 patch — add four mirror fields adjacent to `current_search_query`** (RESEARCH §Pitfall 2 confirms `current_search_query` is declared but never assigned; Phase 77 is the right place to fix that AND add the rest):
```python
self.last_results: List[Dict[str, Any]] = []
self.current_search_query: str = ""
self.current_search_mode: str = "text"          # Phase 77: D-06 echo
self.current_search_gap: Optional[int] = None   # Phase 77: D-06 echo
self.last_filters_applied: Optional[Dict[str, Any]] = None  # Phase 77: D-06 echo
self.last_search_warnings: List[str] = []       # Phase 77: D-07 always-present

# Parallels results (for export functionality)
self.parallels_results: List[Dict[str, Any]] = []
self.parallels_filtered: List[Dict[str, Any]] = []
```
**Note:** the existing `Optional` import at line 1 already covers the new declarations; no new top imports needed.

---

### `web/export_service.py` (MODIFIED — OPTIONAL thin wrappers)

**Analog (search):** `web/export_service.py:286-355` — `export_search_results_excel`. **Analog (parallels):** `web/export_service.py:477-561` — `export_parallels_excel`.

**Decision per CONTEXT.md Discretion ("Planner's call"):** include these wrappers only if the planner wants `web/api.py` call sites to stay structurally symmetric across all three formats (Word/Excel/JSON). Functionally, the JSON handler can call `serialize_search_payload(...)` directly without going through `ExportService` — there is no openpyxl/python-docx work to share.

**If included — wrapper signature** (mirror `export_search_results_excel`'s `(content, filename)` tuple return):
```python
def export_search_results_json(
    self,
    results: List[Dict[str, Any]],
    *,
    query: str = "",
    mode: str = "text",
    filters: Optional[Dict] = None,
    warnings: Optional[List[str]] = None,
) -> tuple:
    """Export search results as JSON. Returns (bytes, filename)."""
    if not results:
        raise ValueError("No results to export")
    from shared.search_serializer import serialize_search_payload
    from datetime import datetime
    import json as _json

    payload = serialize_search_payload(
        results, meta_mgr=self.meta_mgr, query=query, mode=mode,
        filters=filters, warnings=warnings,
    )
    content = _json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8')
    ts = datetime.utcnow().strftime('%Y-%m-%dT%H%M%S')
    filename = f"genizah-search-{ts}.json"
    return content, filename
```
**Delta from analog:** no openpyxl `wb`/`ws` setup, no `style_excel_header`, no `add_excel_credits` (D-08 — JSON has no banner), no `make_safe_filename` (filename uses ISO timestamp not search-query). Single 5-line function vs the 70-line Excel sibling.

**Recommendation per RESEARCH:** SKIP this wrapper. The handler in `web/api.py` calling `serialize_search_payload(...)` + `JSONResponse(...)` directly is more honest — `ExportService` exists to encapsulate openpyxl/docx ceremony that JSON does not have. Adding a 5-line passthrough wrapper hides what is actually happening. Planner: choose deliberately, do not auto-add for "symmetry."

---

### `genizah_core.py` (CONDITIONAL — Path A only, D-13)

**Analog:** `genizah_core.py:1352-1381` — the `results_map[uid]` initialization block + per-chunk accumulator (`total_score += match_score; hits_count += 1`).

**Existing block** (lines 1352-1381 — Path A target):
```python
if uid not in results_map:
    results_map[uid] = {
        'uid': uid, 'total_score': 0, 'hits_count': 0,
        'raw_header': doc['full_header'][0], 'source': doc['source'][0],
        'content': content, 'best_chunk_score': -1,
        'all_found_words': set(), 'src_indices': set(), 'ms_matches': [],
        'is_text_filtered': False,
        'boundary_chunk_scores': [],
        'crossed_boundaries': set()
    }
rec = results_map[uid]
...
rec['total_score'] += match_score
rec['hits_count'] += 1
```

**Phase 77 Path A addition** — add `'chunk_hits': []` to the dict literal at line 1354 and append per-iteration. The outer chunk loop variable is `i` (per RESEARCH §Pitfall 3); `chunk_text` is the current source chunk; `match_score` is in scope; the manuscript snippet for this chunk is reconstructible from `content[matches[start_m]['start']:matches[end_m]['end']]`:
```python
if uid not in results_map:
    results_map[uid] = {
        ...                          # all existing fields unchanged
        'crossed_boundaries': set(),
        'chunk_hits': [],            # Phase 77 D-13: per-chunk attribution
    }
rec = results_map[uid]
...
rec['total_score'] += match_score
rec['hits_count'] += 1
# Phase 77 D-13: capture per-chunk attribution for parallels matches[] payload
if matches:
    ms_snip = content[matches[start_m]['start']:matches[end_m]['end']]
    rec['chunk_hits'].append((i, chunk_text, match_score, ms_snip))
```
**Delta from analog:** purely additive — existing readers of `total_score`/`hits_count` are unchanged, and any downstream code that does not know about `chunk_hits` is unaffected. Add a single core test in `tests/` asserting `rec['chunk_hits']` is populated for a synthetic 3-chunk source matching one manuscript thrice.

**Path B (no core change):** skip this file entirely. The serializer's `_to_envelope_item()` handles the absence by emitting a single degenerate `matches` entry (see Pattern Assignments → search_serializer.py).

---

## Shared Patterns

### Highlight-marker stripping
**Source:** `shared_export_utils.py:84-96` (`remove_highlight_markers`).
**Apply to:** `_serialize_item()` for snippet cleanup; `serialize_parallels_payload()` for `manuscript_snippet` in matches[].
```python
def remove_highlight_markers(text: Optional[str]) -> str:
    if not text:
        return ""
    return text.replace('*', '')
```
**Idempotent + None-safe** — Phase 77 reuses verbatim. D-03 explicitly references this.

### RFC 5987 Content-Disposition
**Source:** `shared_export_utils.py:162-182` (`encode_filename_for_header`).
**Apply to:** Both new `web/api.py` handlers (`/api/export/json`, `/api/export/parallels/json`).
```python
def encode_filename_for_header(filename: str) -> str:
    from urllib.parse import quote
    try:
        filename.encode('ascii')
        return f'attachment; filename="{filename}"'
    except UnicodeEncodeError:
        encoded = quote(filename, safe='')
        return f"attachment; filename*=UTF-8''{encoded}"
```
**Already imported** at `web/api.py:9` — no new top-level import.

### NiceGUI download button
**Source:** `web/pages/search.py:1441-1446` and `web/pages/parallels.py:1230-1235`.
**Apply to:** Both new JSON buttons.
**Props convention:** `'flat round dense size=sm'` for search; `'flat round dense disable'` for parallels (lifecycle disabled by default until results arrive).
**Icon:** `'data_object'` (Material Symbols) per CONTEXT.md Discretion.

### Stateful download handler skeleton (read-state → call-service → wrap-in-Response)
**Source:** `web/api.py:1806-1827`.
**Apply to:** Both new JSON handlers. Skeleton:
```python
@app.get('/api/export/<format>')
def export_<format>():
    if not state.<source>:
        return Response("No results to export", status_code=400)
    try:
        # ... call shared serializer / export_service
        return <Response | JSONResponse>(payload, headers={...})
    except ValueError as e:
        return Response(str(e), status_code=400)
    except Exception as e:
        logger.error(f"Export <format> error: {e}")
        return Response("Export failed", status_code=500)
```
**Delta for JSON path:** use `JSONResponse(payload, headers={...})` instead of `Response(content=bytes, media_type=..., headers=...)` — Starlette sets `Content-Type: application/json` automatically and emits Hebrew as native UTF-8 by default (no `ensure_ascii` work needed).

### MagicMock-based service test fixture
**Source:** `tests/test_export_service.py:240-306`.
**Apply to:** `tests/test_search_serializer.py`.
**Pattern:** mock `MetadataManager` with `MagicMock`, build dict-only fixtures for results, assert on returned dict structure. No live SQLite, no live FJMS. The serializer's `fjms.is_available()` branch returns `False` cleanly when no sidecar is present, so `domain_batch` and `catalog_batch` will be empty `{}` — tests assert that `domain` and `dating` are `None` in this mode.

---

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| (none) | — | — | All Phase 77 files map cleanly to in-repo precedents. No new file types or patterns are introduced. |

---

## Metadata

**Analog search scope:**
- `web/api.py` (Excel/Word handler region 1806-1908; JSONResponse usage scan 1374-1718)
- `web/export_service.py` (search Excel/Word 286-411; parallels Excel/Word 477-655)
- `web/pages/search.py` (toolbar 1430-1446; state populate 4060-4099)
- `web/pages/parallels.py` (toolbar 1220-1240; lifecycle 1915-1925, 2600-2616)
- `web/state.py` (full file — 82 lines)
- `tests/test_export_service.py` (fixtures + test methods 1-50, 230-320)
- `shared/document_service.py` (module shape 1-80)
- `shared_export_utils.py` (`remove_highlight_markers`, `make_safe_filename`, `encode_filename_for_header` 80-185)
- `genizah_core.py` (composition core 1340-1390; `parse_full_id_components` referenced via RESEARCH)

**Files scanned:** 9 (all read-only).
**Pattern extraction date:** 2026-04-27.
**Cross-checked against:** RESEARCH.md Standard Stack + Architecture Patterns + Pitfalls; CONTEXT.md canonical_refs (every line range cited above is also cited in CONTEXT.md so the planner can dual-verify).

## PATTERN MAPPING COMPLETE
