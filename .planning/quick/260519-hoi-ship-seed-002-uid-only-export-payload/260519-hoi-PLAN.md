---
phase: quick-260519-hoi
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - web/export_state.py
  - web/export_service.py
  - shared/search_serializer.py
  - tests/test_export_state_cap.py
  - tests/test_export_service.py
autonomous: true
requirements:
  - SEED-002
must_haves:
  truths:
    - "Compacted search-export rows contain ONLY {uid, sort_score, snippet, match_terms} keys (no `display`, no `full_text_excerpt`, no `raw_file_hl`, no `content`)."
    - "Compacted parallels-export rows contain ONLY keys from the allowlist {uid, sort_score, score, snippet, match_terms, source_ctx, text, raw_header} (no `chunk_hits`, `display`, `full_text`, `raw_file_hl`, `content`). `score` AND `raw_header` ARE INTENTIONALLY KEPT — they are read by the live parallels UI (`web/pages/parallels.py` lines 2827/2831/2865/2868/3123/3310/3372 read `score`; lines 2841/3134/3140/3359/3373 read `raw_header`) AND by the public-API grouper (`shared/search_serializer.py:691` reads `item.get('score', 0.0)` to compute group `aggregate_score`)."
    - "A representative compacted search row weighs <2 KB (sys.getsizeof of dict + values)."
    - "A 5000-row capped search payload serializes to <5 MB JSON."
    - "Excel + Word exports rehydrate shelfmark/title/library_code/library_name from `meta_mgr` (via uid → sys_id, with `raw_header` regex fallback for legacy rows) at export time, with graceful 'Unknown' fallback when neither uid nor raw_header is resolvable."
    - "Public JSON output of `serialize_search_payload` and `serialize_parallels_payload` (the /api/search and /api/parallels response shape) is content-equivalent pre- vs post-fix when fed compacted rows: same shelfmark, title, library.code, library.name values per item; parallels group `aggregate_score` is preserved because `score` is kept on parallels rows."
    - "`web/pages/search_state.py` is NOT modified by this plan. `_compact_result_rows` there serves the tab-restore path (lines 254-273 → restore at line 299 writes `state.results = raw.get('results', [])`), and the live `search_state.results` is read at 25+ sites in `web/pages/search.py` (`r.get('display', ...)`). Shrinking that compactor to uid-only would silently break tab-restored search pages. That allocator-pressure source remains a SEPARATE follow-up, out of scope for SEED-002."
    - "All pre-existing export-related tests stay green: test_export_state_cap (15 prior tests, 2 of them updated in-place to expect the new shape, plus 5 brand-new tests), test_export_service (51 prior + 3 new), test_export_cross_user_isolation, test_api_export_json, test_api_legacy_unchanged, and the Phase 87/88 invariant scanners (test_no_raw_storage_access, test_no_appstate_export_fields, test_no_deleted_state_references)."
    - "Final pytest tree-wide run reports >=2059 passing tests (2051 prior baseline + 5 brand-new in test_export_state_cap + 3 brand-new in test_export_service; the 2 in-place updates in test_export_state_cap don't change total count)."
    - "ruff check on every touched source file reports zero issues."
  artifacts:
    - path: web/export_state.py
      provides: "uid-only `_compact_search_result_row` (allowlist {uid, sort_score, snippet, match_terms}) and parallels-row allowlist `_compact_parallels_result_row` ({uid, sort_score, score, snippet, match_terms, source_ctx, text, raw_header}). Drops `display`, `full_text`, `full_text_excerpt`, `raw_file_hl`, `content` from search rows; drops `chunk_hits`, `display`, `full_text`, `raw_file_hl`, `content` from parallels rows (KEEPS `score` and `raw_header`)."
      contains: "_compact_search_result_row"
    - path: web/export_service.py
      provides: "`_resolve_result_display(row, meta_mgr) -> (shelfmark, title, library_code, library_name)` helper with 3-tier fallback (legacy `display` dict → uid parse → `raw_header` regex → 'Unknown'); every export path uses it."
      contains: "_resolve_result_display"
    - path: shared/search_serializer.py
      provides: "`_serialize_item` rehydrates display fields from uid when row's `display` is empty (so public JSON shape is preserved)"
      contains: "_serialize_item"
    - path: tests/test_export_state_cap.py
      provides: "5 NEW uid-only assertions (search-row-key set, parallels-row-key set, per-row bytes <2 KB with realistic Hebrew snippet, 5000-row payload <5 MB JSON, ed6f89c4 field-strip invariants) + 2 IN-PLACE updates: `test_set_search_export_strips_heavy_text_fields_even_for_few_results` (line 106) drops the `full_text_excerpt` cell assertion + asserts the key is ABSENT; `test_set_parallels_export_strips_full_text_and_caps_chunk_hits` (line 203) asserts `chunk_hits` is ABSENT and drops the `_PARALLELS_CHUNK_HITS_CAP` / `_PARALLELS_CHUNK_TEXT_STORAGE_CHARS` references."
      min_lines: 1
    - path: tests/test_export_service.py
      provides: "3 NEW rehydration assertions (Excel rehydrates shelfmark from uid via mocked meta_mgr; graceful 'Unknown' fallback with `meta_mgr.get_meta_for_id` explicitly mocked to ('Unknown', ''); pre-vs-post Excel cell-value equivalence)."
      min_lines: 1
  key_links:
    - from: "web/export_state.py:_compact_search_result_row"
      to: "stored row schema in app.storage.user['export_search_payload'].results"
      via: "set_search_export / update_search_export_results / _compact_search_export_payload"
      pattern: "_compact_search_result_row"
    - from: "web/export_state.py:_compact_parallels_result_row"
      to: "stored row schema in app.storage.user['export_parallels_payload'].results + .filtered + the live `p_state.results` (because `web/pages/parallels.py:2338` reassigns `p_state.results = compact_parallels_result_rows(main_results)`)"
      via: "set_parallels_export / update_parallels_export_filtered / _compact_parallels_export_payload / compact_parallels_result_rows"
      pattern: "_compact_parallels_result_row"
    - from: "web/export_service.py::ExportService.export_search_results_excel"
      to: "meta_mgr.get_meta_for_id / get_library_for_id / get_library_display"
      via: "_resolve_result_display(res, self.meta_mgr) with 3-tier fallback (display dict → uid → raw_header)"
      pattern: "_resolve_result_display"
    - from: "shared/search_serializer.py::_serialize_item"
      to: "meta_mgr.get_meta_for_id when display is empty / missing"
      via: "rehydration fallback (mirrors export_service helper)"
      pattern: "get_meta_for_id"
    - from: "web/pages/parallels.py (lines 2827/2831/2865/2868/3123/3310/3372 read `score`; 2841/3134/3140/3359/3373 read `raw_header`)"
      to: "compacted parallels rows in p_state.results"
      via: "schema allowlist explicitly retains both fields"
      pattern: "item.get\\('score'|item.get\\('raw_header'"
    - from: "shared/search_serializer.py:691 (_group_parallels_by_sys_id)"
      to: "compacted parallels rows feeding /api/parallels response"
      via: "schema allowlist retains `score`; group `aggregate_score` sums it"
      pattern: "aggregate_score"
---

<objective>
Ship SEED-002: shrink per-row stored bytes in export payloads from ~22 KB to ~500 bytes by storing ONLY query-specific fields plus the small fields actively read by the live UI / public API ({uid, sort_score, snippet, match_terms} for search; same plus {score, source_ctx, text, raw_header} for parallels). Display fields (shelfmark, title, library_code, library_name) rehydrate from `meta_mgr` at export/serialize time. Full-text rehydration via Tantivy is already wired (`_resolve_result_full_text`); this fix extends the same pattern to display fields.

Purpose: Per-search Python allocations drop materially. The 2026-05-19 production attribution (`tracemalloc` + `objgraph` evidence in commit `899fe7af`) identified two distinct allocator clusters: (a) `web/export_state.py` row compactors writing into storage payloads, and (b) `web/pages/search_state.py:262/258/267` writing into tab-storage active-search snapshot. This plan targets ONLY (a). Cluster (b) requires preserving the tab-restore contract (compacted rows are written back to live `state.results` and read at 25+ sites in `web/pages/search.py`) and remains a separate follow-up.

Output: Smaller stored export payloads (~44× per-row reduction for search exports, similar magnitude for parallels exports), unchanged user-visible behavior, unchanged public API JSON shape, +8 new tests, +2 in-place test updates, every touched file ruff-clean.

Out of scope (deliberate): `web/pages/search_state.py:_compact_result_rows` (tab-storage path with separate restore-contract semantics — see threat model T-260519-hoi-06); NiceGUI Observable retention fix; deploy step; OPEN_ISSUES.md / SEED-002 status flips (those happen in a separate post-deploy doc-only quick task once `/_internal/memstat` confirms KB-range payloads).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@CLAUDE.md
@.planning/STATE.md
@.planning/seeds/SEED-002-uid-only-export-payload.md

@web/export_state.py
@web/export_service.py
@shared/search_serializer.py
@web/api.py
@tests/test_export_state_cap.py
@tests/test_export_service.py

<interfaces>
<!-- Key contracts the executor needs. Extracted from the codebase 2026-05-19. -->
<!-- Use these directly — no codebase exploration required. -->

### Already-shipped rehydration pattern (the model to extend)

From `web/export_service.py:55-73`:
```python
def _resolve_result_full_text(result: Dict[str, Any]) -> str:
    """Return full text for export, rehydrating compact session rows if needed."""
    full_text = result.get('full_text') or ''
    if full_text:
        return str(full_text)

    uid = result.get('uid') or ''
    if uid:
        try:
            from web.state import state as web_state
            searcher = getattr(web_state, 'searcher', None)
            if searcher and hasattr(searcher, 'get_full_text_by_id'):
                fetched = searcher.get_full_text_by_id(uid)
                if fetched:
                    return str(fetched)
        except Exception:
            pass

    return str(result.get('full_text_excerpt') or '')
```

### meta_mgr API the new helper calls

From `web/export_service.py:277-305` (already in production):
```python
def get_metadata(self, sys_id: str) -> tuple:
    """Returns (shelfmark, title) with ('Unknown', '') graceful fallback."""
    ...
    shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)
    return (shelfmark or 'Unknown', title or '')

def get_library_code(self, sys_id: str) -> str:
    """Returns library code string with '' graceful fallback."""
    ...
    return self.meta_mgr.get_library_for_id(sys_id) or ''

def get_library_display(self, library_code: str, short: bool = True) -> str:
    """Resolves library code -> full localized display name."""
    ...
    return core_get_library_display(library_code, short=short, lang=get_language())
```

### uid → sys_id extraction convention (project-wide)

From `genizah_core.py:3652-3681`:
```python
def parse_full_id_components(self, full_header):
    """Parse header into components regardless of order/separators."""
    result = {'sys_id': None, 'ie_id': None, 'p_num': None, 'fl_id': None}
    sys_match = re.search(r'(99\d{8,})', full_header)
    if sys_match:
        result['sys_id'] = sys_match.group(1)
    ...
```

Note: `meta_mgr.parse_full_id_components` works on full headers AND on `uid` strings — both contain the `99\d{8,}` sys_id prefix. The new `_resolve_result_display` helper calls this method first on `uid`, then falls back to a direct `re.search(r'(99\d{8,})', raw_header)` on the row's `raw_header` field (legacy parallels-row idiom — see `shared/search_serializer.py:716-738`). If neither resolves, returns `('Unknown', '', '', '')`.

### Existing raw_header → sys_id idiom we must preserve (parallels-envelope fallback)

From `shared/search_serializer.py:716-738`:
```python
# When display is empty, extract sys_id from raw_header via regex
if 'display' not in synth or not synth['display']:
    raw_header = synth.get('raw_header', '')
    if raw_header:
        m = re.search(r'(99\d{8,})', raw_header)
        if m:
            sid = m.group(1)
            # ... meta_mgr lookup, populate display
```

`_resolve_result_display` MUST mirror this fallback so existing parallels test fixtures (e.g. `tests/test_export_service.py:301` `sample_parallels_results` which carries `raw_header` but no `uid` and no `display`) keep working unchanged.

### Live parallels UI sites reading `score` and `raw_header` (CRITICAL: must NOT drop these fields)

From `web/pages/parallels.py` (verified 2026-05-19 via grep):
- `score` reads: lines 2827, 2831, 2865, 2868, 3123, 3310, 3372 — group max/avg score for sort ordering, per-item displayed score, metadata-dialog Score row
- `raw_header` reads: lines 2841, 3134, 3140, 3359, 3373 — sys_id/shelfmark extraction in result cards + filtered-grouping path + `extract_shelfmark` helper

From `shared/search_serializer.py:691`:
```python
grp['aggregate_score'] += float(item.get('score', 0.0) or 0.0)
```
The public `/api/parallels` grouper sums `score` across uids per sys_id. Dropping `score` collapses every group's `aggregate_score` to 0.0 → sort order becomes arbitrary → public API JSON shape regresses.

**Resolution:** The parallels-row allowlist KEEPS `score` and `raw_header` in addition to `{uid, sort_score, snippet, match_terms, source_ctx, text}`. `score` and `sort_score` are both 8-byte floats; `raw_header` is ~30 bytes. Total per-row stays well under the 500-byte design target.

### Current `_compact_search_result_row` (the row that must shrink)

From `web/export_state.py:62-97` — extract the dance:
```python
def _compact_search_result_row(row: Any) -> Tuple[Any, bool]:
    if not isinstance(row, dict):
        return row, False
    compact = dict(row)
    changed = False
    full_text = compact.pop('full_text', None)
    if full_text is not None:
        changed = True
        excerpt = _text_prefix(full_text, _SEARCH_FULL_TEXT_EXCERPT_CHARS)
        if excerpt:
            compact['full_text_excerpt'] = excerpt
    # ... (keeps full_text_excerpt, display dict, etc.)
```

Target (post-fix): explicit allowlist:
```python
def _compact_search_result_row(row: Any) -> Tuple[Any, bool]:
    if not isinstance(row, dict):
        return row, False
    kept = {}
    for key in ('uid', 'sort_score', 'snippet', 'match_terms'):
        if key in row:
            kept[key] = row[key]
    changed = set(row.keys()) != set(kept.keys())
    return kept, changed
```

### Public JSON serializer (`shared/search_serializer.py:_serialize_item:250-322`)

The serializer reads `display.shelfmark`, `display.title`, `display.id`, `display.library_code` and `full_text` / `full_text_excerpt`. **It currently expects `display` and `full_text` to be present in the row.** After this fix, the export-state rows have no `display` dict, so the serializer must rehydrate from `meta_mgr.get_meta_for_id(sys_id) + get_library_for_id(sys_id) + _safe_library_name(library_code)` when `display` is missing/empty. The `_to_parallels_envelope_item` path already has this fallback wired (`shared/search_serializer.py:716-738` — `if 'display' not in synth or not synth['display']: ...`). Mirror that idiom in `_serialize_item`.

### Existing read sites in `web/api.py` (no display-dict reads found)

All of `/api/export/excel`, `/api/export/word`, `/api/export/parallels/excel`, `/api/export/parallels/word`, `/api/export/json`, `/api/export/parallels/json` read `payload['results']` and pass the list straight to `ExportService.export_*` or `serialize_*_payload`. There are NO `display.shelfmark` / `display.title` / `display.library_code` reads in `web/api.py` outside the export functions themselves. So no `web/api.py` edits are required beyond verification — Tasks 1 and 3 already cover the call sites.

### EXPLICITLY OUT OF SCOPE: `web/pages/search_state.py:_compact_result_rows`

The plan-checker's CRITICAL #1 finding: shrinking this function silently breaks tab-restore. Evidence:
- Line 321 calls `_compact_result_rows((state.results or [])[:1000])` and stores into tab storage.
- Line 299 restores: `state.results = raw.get('results', []) or []`.
- 25+ sites in `web/pages/search.py` then read `r.get('display', {}).get('id')` (sys_id), `r.get('display', {}).get('title')`, `r.get('display', {}).get('library_code')`, etc. from the live `state.results` (lines 1637, 1652, 1759, 1953, 2205, 2232, 2264, 2941, 3138, 3157, 3203, 3576, 3620, 3670, 4145, 4163, 4247, 4249, 4468, 4718, 4720, and more).

If `_compact_result_rows` shrinks to uid-only, every page-resume read returns `None` / `{}` and the search results page silently degrades after tab restore (no shelfmark, no title, no library). That's a regression the seed never asked for. The tracemalloc finding stays valid — that allocator-pressure source will still exist after this fix. Resolving it requires a different design (possibly keep `display` but drop `full_text_excerpt` only) and its own threat model. Tracked as a separate follow-up, NOT in scope for SEED-002.
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Shrink row compactors to uid-only (search) / safe-allowlist (parallels) in export_state.py</name>
  <files>web/export_state.py, tests/test_export_state_cap.py</files>
  <behavior>
    - `_compact_search_result_row(row)` returns a dict whose key set is EXACTLY a subset of `{uid, sort_score, snippet, match_terms}` (only keys present in input are kept; missing keys are simply absent). Drops `display`, `full_text`, `full_text_excerpt`, `raw_file_hl`, `content`, and any other field. Returns `(kept, True)` when at least one input key was dropped, `(kept, False)` when input was already uid-only.
    - `_compact_parallels_result_row(row)` returns a dict whose key set is a subset of `{uid, sort_score, score, snippet, match_terms, source_ctx, text, raw_header}`. `source_ctx` and `text` retain the existing 4000-char cap. **`score` and `raw_header` are INTENTIONALLY KEPT** — see the <interfaces> note: dropping them breaks the live parallels UI (`web/pages/parallels.py` reads `score` at 8 sites and `raw_header` at 5 sites) AND collapses the public API parallels `aggregate_score` to 0.0 (`shared/search_serializer.py:691`). Drops `chunk_hits`, `display`, `full_text`, `raw_file_hl`, `content`, and every other field not on the allowlist.
    - `set_search_export`, `update_search_export_results`, `set_parallels_export`, `update_parallels_export_filtered`, and the read-path getters continue to call the same row compactor functions — the only change is the row-level schema.
    - Existing 5000-row cap (`_EXPORT_RESULTS_CAP`) is preserved unchanged.
    - `truncated` + `total_count` payload-level metadata is preserved unchanged.
    - Test: per-row `sys.getsizeof(row) + sum(sys.getsizeof(v) for v in row.values()) < 2048` for a representative search row (uid 32 chars + sort_score float + Hebrew snippet 2000 chars + match_terms list of 3 strings).
    - Test: full 5000-row payload `len(json.dumps(payload, ensure_ascii=False, separators=(',',':')).encode('utf-8')) < 5 * 1024 * 1024` (5 MB), using realistic Hebrew snippets and 3-5 match_terms per row.
    - Test: existing `test_set_search_export_caps_oversized_list` and friends in `test_export_state_cap.py` continue to pass — the cap logic is independent of the row schema.
    - **Two in-place test updates** (existing tests that assert the OLD shape):
      - `test_set_search_export_strips_heavy_text_fields_even_for_few_results` (line 106-127) — line 127 asserts `payload['results'][0]['full_text_excerpt'] == 'x' * export_state._SEARCH_FULL_TEXT_EXCERPT_CHARS`. After the fix: drop that assertion, replace with `assert all('full_text_excerpt' not in r for r in payload['results'])`.
      - `test_set_parallels_export_strips_full_text_and_caps_chunk_hits` (line 203-227) — lines 225-227 assert `len(stored['chunk_hits']) == export_state._PARALLELS_CHUNK_HITS_CAP` and dig into `chunk_hits[0][1]` / `chunk_hits[0][3]`. After the fix: replace with `assert 'chunk_hits' not in stored`. Lines 221-224 (`full_text not in stored`, `content not in stored`, source_ctx/text caps) still hold and stay. Rename or keep the function name as-is — the spirit (strip heavy text) is preserved.
  </behavior>
  <action>
**PRE-FLIGHT (gate — must complete BEFORE editing production code):**

0. Pre-flight grep:
   ```
   grep -nE "display" tests/test_export_state_selection.py tests/test_export_cross_user_isolation.py
   ```
   - If any match reads `display.shelfmark` / `display.title` / `display.library_code` / `display.id` from a stored payload AFTER `set_search_export` or `set_parallels_export` (i.e. from `storage['export_search_payload']['results'][N]['display']` or similar), HALT and add those tests to this task's explicit update list (along with the 2 already-listed below).
   - If all matches are only on the INPUT-row side (building input rows for `set_search_export`, e.g. `row['display'] = {...}` before calling the setter), that is FINE — the setter strips them. Proceed to step 1.

**RED phase (tests first):**

1. Open `tests/test_export_state_cap.py`. Add five new test functions at the end of the file (after `test_update_parallels_export_filtered_caps_oversized_filter` or wherever the last test sits):
   - `test_search_export_row_has_only_uid_keys(monkeypatch)` — call `set_search_export` with a single row containing the full production shape (uid, display dict, snippet, full_text, raw_file_hl, content, match_terms, sort_score, score). Read back via `storage['export_search_payload']['results'][0]`. Assert `set(row.keys()) <= {'uid', 'sort_score', 'snippet', 'match_terms'}` and assert `display`, `full_text`, `full_text_excerpt`, `raw_file_hl`, `content`, `score` (the search `score`, not parallels score) are all absent.
   - `test_parallels_export_row_keeps_safe_allowlist(monkeypatch)` — call `set_parallels_export` with a row containing uid + display + chunk_hits + full_text + raw_file_hl + content + source_ctx + text + match_terms + sort_score + score + raw_header. Assert `set(row.keys()) <= {'uid', 'sort_score', 'score', 'snippet', 'match_terms', 'source_ctx', 'text', 'raw_header'}` and explicitly assert: `'chunk_hits' not in row`, `'display' not in row`, `'full_text' not in row`, `'raw_file_hl' not in row`, `'content' not in row`. Also assert `row.get('score') == <input score>` and `row.get('raw_header') == <input raw_header>` (PROVES we KEEP them — this is the critical CRITICAL #3 invariant).
   - `test_per_row_bytes_drops_to_under_2kb(monkeypatch)` — build a realistic row (uid 32 chars, sort_score float, snippet 2000 chars Hebrew `'א' * 1000 + '*ב*' * 100`, match_terms = `['אבל', 'אמר', 'דרש', 'תני', 'הא']`). Run it through `_compact_search_result_row` directly. Compute `total = sys.getsizeof(compacted) + sum(sys.getsizeof(v) for v in compacted.values())`. Assert `total < 2048`. (Import sys at top of test file.)
   - `test_5000_row_payload_under_5mb(monkeypatch)` — call `set_search_export` with 5000 rows, each row: `{'uid': f'9912345678901234_IE{i}_P1_FL1', 'sort_score': 0.95 - (i * 1e-5), 'snippet': 'א' * 1000 + '*ב*' * 100, 'match_terms': ['אבל', 'אמר', 'דרש', 'תני', 'הא']}`. Read payload back. Assert `len(json.dumps(payload, ensure_ascii=False, separators=(',',':')).encode('utf-8')) < 5 * 1024 * 1024`.
   - `test_field_strip_invariants_still_hold(monkeypatch)` — sanity check that the `ed6f89c4` invariants survive: call `set_search_export` with a row containing `full_text='x'*500_000`, `raw_file_hl='y'*500_000`, `content='z'*500_000`. Read back. Assert NONE of those three keys is present in the stored row (regardless of whether `full_text_excerpt` is present — we expect it absent now).
2. In `tests/test_export_state_cap.py`, update the two existing tests in-place:
   - `test_set_search_export_strips_heavy_text_fields_even_for_few_results` (lines 106-127):
     - DELETE line 127: `assert payload['results'][0]['full_text_excerpt'] == 'x' * export_state._SEARCH_FULL_TEXT_EXCERPT_CHARS`.
     - REPLACE with: `assert all('full_text_excerpt' not in r for r in payload['results'])`.
   - `test_set_parallels_export_strips_full_text_and_caps_chunk_hits` (lines 203-227):
     - KEEP lines 220-224 (full_text/content absent, source_ctx/text caps).
     - DELETE lines 225-227 (the three `chunk_hits` assertions referencing `_PARALLELS_CHUNK_HITS_CAP` and `_PARALLELS_CHUNK_TEXT_STORAGE_CHARS`).
     - REPLACE with: `assert 'chunk_hits' not in stored`.
3. Run `pytest tests/test_export_state_cap.py -x -q` — confirm the 5 NEW tests + 2 UPDATED tests FAIL with clear errors about extra keys / unexpected `full_text_excerpt` / unexpected `chunk_hits` (NOT with import errors or AttributeError on `_PARALLELS_CHUNK_*`).

**GREEN phase (production code):**

4. In `web/export_state.py`:
   - Replace the body of `_compact_search_result_row` (lines 62-97) with the explicit-allowlist version (see <interfaces> block above). Keep only `{uid, sort_score, snippet, match_terms}`. Compute `changed` as `bool(set(row.keys()) - {'uid', 'sort_score', 'snippet', 'match_terms'})`.
   - Replace the body of `_compact_parallels_result_row` (lines 122-154) with an analogous allowlist: keep `{uid, sort_score, score, snippet, match_terms, source_ctx, text, raw_header}`. For `source_ctx` and `text`, retain the existing `[:_PARALLELS_TEXT_STORAGE_CHARS]` (4000) truncation. **EXPLICITLY KEEP `score` and `raw_header` unchanged** — they pass through to the output dict as-is. Drop `chunk_hits` entirely. Drop the `_compact_chunk_hit` helper and the `_PARALLELS_CHUNK_HITS_CAP` / `_PARALLELS_CHUNK_TEXT_STORAGE_CHARS` constants — they are no longer referenced from production code OR from tests after step 2 (verify with `grep -rn "_PARALLELS_CHUNK_HITS_CAP\|_PARALLELS_CHUNK_TEXT_STORAGE_CHARS\|_compact_chunk_hit" web/ shared/ tests/`).
   - The public `compact_parallels_result_rows` function (the wrapper used by `web/pages/parallels.py:2338` to compact rows into live `p_state.results`) should also follow the new allowlist shape — verify it calls `_compact_parallels_result_row` per row (or that its inline logic matches the new allowlist) so the live UI receives `score` and `raw_header`.
   - Update the `_SEARCH_FULL_TEXT_EXCERPT_CHARS` constant — DELETE it (no excerpt field anymore). Verify no other module imports it via `grep -rn "_SEARCH_FULL_TEXT_EXCERPT_CHARS" web/ tests/ shared/`. If a test now references it, that test was already updated in step 2.
   - Leave `_compact_results`, `_compact_search_export_payload`, `_compact_parallels_export_payload`, and the public `set_*` / `update_*` / `get_*` functions untouched — they call the row compactors, which is exactly the surface that changed.
5. Run `pytest tests/test_export_state_cap.py -x -q` — all tests (13 prior + 2 updated + 5 new = 20) must pass.
6. Run `pytest tests/test_export_state_cap.py tests/test_export_state_selection.py tests/test_export_cross_user_isolation.py -x -q` — all green; this validates the cap, selection, and cross-user-isolation suites against the new row schema. If `test_export_state_selection.py` or `test_export_cross_user_isolation.py` fails because it asserts `display.*` from a stored payload, that means step 0 pre-flight missed something — STOP, audit, and fix the test in-place to use rehydrated values (or update the test to use the new shape).
7. Run `python -m ruff check web/export_state.py tests/test_export_state_cap.py` — must be clean (zero issues). Fix any unused-import / unused-name warnings introduced by deleting `_compact_chunk_hit` / `_SEARCH_FULL_TEXT_EXCERPT_CHARS` / `_PARALLELS_CHUNK_*`.

**Do NOT** modify `web/export_service.py` or `shared/search_serializer.py` yet — that is Task 2's responsibility. After Task 1, search/parallels export will be technically broken at the EXPORT path (display rehydration missing) — that is the expected intermediate state, which is exactly why Task 2 sequences immediately after.

**Do NOT** modify `web/pages/search_state.py` — out of scope per CRITICAL #1 / threat T-260519-hoi-06. The 5 existing tests in `tests/test_search_state.py` (lines 29-46, 103, 208, 231) exercise the tab-storage path with their own data and stay green automatically.
  </action>
  <verify>
    <automated>pytest tests/test_export_state_cap.py tests/test_export_state_selection.py tests/test_export_cross_user_isolation.py -x -q &amp;&amp; python -m ruff check web/export_state.py tests/test_export_state_cap.py</automated>
  </verify>
  <done>
- `_compact_search_result_row` uses the explicit-allowlist pattern, dropping `display`, `full_text`, `full_text_excerpt`, `raw_file_hl`, `content`.
- `_compact_parallels_result_row` uses the explicit-allowlist pattern, dropping `chunk_hits`, `display`, `full_text`, `raw_file_hl`, `content`; KEEPING `score` and `raw_header` (verified by `test_parallels_export_row_keeps_safe_allowlist`).
- 5 new tests added to `test_export_state_cap.py`, all passing.
- 2 existing tests updated in-place to assert the new shape (excerpt absent, chunk_hits absent), passing.
- Existing 13 cap tests + cross-user isolation + selection tests all stay green.
- ruff clean on `web/export_state.py` and `tests/test_export_state_cap.py`.
- Dead constants (`_SEARCH_FULL_TEXT_EXCERPT_CHARS`, `_PARALLELS_CHUNK_HITS_CAP`, `_PARALLELS_CHUNK_TEXT_STORAGE_CHARS`) and the `_compact_chunk_hit` helper deleted; grep returns zero references.
- `web/pages/search_state.py` is UNTOUCHED.
  </done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Rehydrate display fields at export time (export_service.py + search_serializer.py)</name>
  <files>web/export_service.py, shared/search_serializer.py, tests/test_export_service.py</files>
  <behavior>
    - New helper `_resolve_result_display(result: Dict[str, Any], meta_mgr) -> Tuple[str, str, str, str]` returning `(shelfmark, title, library_code, library_name)` with a **3-tier fallback**:
        1. If `result.get('display')` is a non-empty dict, return its values verbatim (back-compat for callers passing the legacy shape — e.g. live search results before they hit export_state compaction).
        2. Otherwise extract `sys_id` from `result.get('uid', '')` via `meta_mgr.parse_full_id_components(uid)`. If sys_id resolved, do meta_mgr lookups + return 4-tuple.
        3. Otherwise (uid path failed), try `re.search(r'(99\d{8,})', result.get('raw_header', ''))` — mirrors the existing parallels-envelope idiom at `shared/search_serializer.py:716-738`. This keeps legacy parallels test fixtures (e.g. `tests/test_export_service.py:301` `sample_parallels_results`) working unchanged.
        4. If `meta_mgr` is None OR neither uid nor raw_header parses to a sys_id, return `('Unknown', '', '', '')`.
        5. Otherwise: call `meta_mgr.get_meta_for_id(sys_id)` -> (shelfmark, title) with `('Unknown', '')` fallback on any exception; call `meta_mgr.get_library_for_id(sys_id)` -> library_code with `''` fallback; resolve library_name via existing `get_library_display(library_code, short=False)` helper with library_code fallback.
    - `ExportService.export_search_results_excel` reads shelfmark/title/library_code/library_name from `_resolve_result_display(res, self.meta_mgr)` instead of `res.get('display', {})`. The `id` cell (column D — "System ID") reads sys_id rehydrated from uid (or display.id if legacy row carries it).
    - `ExportService.export_search_results_word` same change.
    - `ExportService.export_parallels_excel` — the inner `add_results` function replaces the existing `raw_header` → sys_id regex + meta_mgr lookup with the new `_resolve_result_display` helper applied to each item. The helper's tier-3 raw_header fallback handles legacy fixtures; the tier-2 uid path handles post-Task-1 rows.
    - `ExportService.export_parallels_word` same change.
    - **DO NOT rename `score` to `sort_score`** in the parallels export paths — `score` is kept on the parallels row schema per Task 1 (CRITICAL #3). Continue reading `item.get('score', 0)` at the existing sites (no change needed for that field).
    - `shared/search_serializer.py::_serialize_item` rehydrates `shelfmark`, `title`, `library_code` from `meta_mgr.get_meta_for_id(sys_id) + get_library_for_id(sys_id)` when `result.get('display')` is empty/missing. Mirrors the idiom in `_to_parallels_envelope_item` (lines 716-738). `sys_id` extraction uses `meta_mgr.parse_full_id_components(uid)` first, then falls back to `re.search(r'(99\d{8,})', raw_header)` if `raw_header` is present.
    - All exports stay byte-content-equivalent: Hebrew text preserved, RTL alignment preserved, highlight fill behavior preserved.
    - Test: pass a compacted row (uid only) into Excel export with a mocked meta_mgr returning ('T-S K1.1', 'Test Title') for sys_id 9912345678901234 (extracted from uid `9912345678901234_IE1_P1_FL1`). Assert Excel cell (2, 1) contains 'T-S K1.1'.
    - Test: pass a row with an unresolvable uid (and no raw_header), assert Excel cell (2, 1) is 'Unknown' (NOT crash, NOT empty, NOT a `<MagicMock ...>` string).
    - Test: build a "legacy" row (with `display` dict present) and a "compacted" row (uid only) that resolve to the same metadata. Excel exports should produce IDENTICAL cell values for shelfmark, library, title across rows.
  </behavior>
  <action>
**RED phase (tests first):**

1. Open `tests/test_export_service.py`. Find the existing `test_export_search_results_excel_rehydrates_compact_full_text` test (line 339) — it already proves the `_resolve_result_full_text` pattern works. Add three new tests immediately after it, inside the same `TestExportService` class:
   - `test_excel_export_rehydrates_display_from_uid(self, export_service, monkeypatch)`:
     - Set `export_service.meta_mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test Title')`.
     - Set `export_service.meta_mgr.get_library_for_id.return_value = 'CUL'`.
     - Set `export_service.meta_mgr.parse_full_id_components.return_value = {'sys_id': '9912345678901234'}`.
     - Pass a compacted row: `[{'uid': '9912345678901234_IE1_P1_FL1', 'sort_score': 0.95, 'snippet': 'snippet', 'match_terms': []}]`.
     - Call `export_search_results_excel`. Load the workbook. Assert `ws.cell(row=2, column=1).value == 'T-S 12.345'` (Shelfmark), `ws.cell(row=2, column=3).value == 'Test Title'` (Title).
     - Verify `get_meta_for_id` was called with the extracted sys_id `'9912345678901234'`.
   - `test_excel_export_graceful_degradation_on_unknown_uid(self, export_service, monkeypatch)`:
     - **STRENGTHEN MOCKS (HIGH #7):** Explicitly set BOTH:
       ```python
       export_service.meta_mgr.parse_full_id_components.return_value = {'sys_id': None}
       export_service.meta_mgr.get_meta_for_id.return_value = ('Unknown', '')
       export_service.meta_mgr.get_library_for_id.return_value = ''
       ```
       (Without these explicit returns, `get_meta_for_id` returns a `MagicMock` object that coerces to `"<MagicMock id=...>"` in the cell — false-positive failure.)
     - Pass `[{'uid': 'malformed-no-sys-id', 'sort_score': 0.0, 'snippet': 's', 'match_terms': []}]`.
     - Assert `ws.cell(row=2, column=1).value == 'Unknown'` and no exception raised.
   - `test_excel_output_equivalent_legacy_vs_compacted(self, export_service)`:
     - `export_service.meta_mgr.get_meta_for_id.return_value = ('T-S 99.1', 'Same Title')`.
     - `export_service.meta_mgr.get_library_for_id.return_value = 'CUL'`.
     - `export_service.meta_mgr.parse_full_id_components.return_value = {'sys_id': '9912345678901111'}`.
     - Build `legacy_row = {'display': {'shelfmark': 'T-S 99.1', 'title': 'Same Title', 'library_code': 'CUL', 'id': '9912345678901111'}, 'uid': '9912345678901111_IE1_P1_FL1', 'sort_score': 0.5, 'snippet': 's', 'match_terms': []}`.
     - Build `compact_row = {'uid': '9912345678901111_IE1_P1_FL1', 'sort_score': 0.5, 'snippet': 's', 'match_terms': []}`.
     - Export both as single-row Excels. Load both workbooks. Assert columns 1, 2, 3 (Shelfmark, Library, Title) match between the two workbooks.
2. Run `pytest tests/test_export_service.py -x -k "rehydrate or graceful or equivalent" -q` — confirm the 3 new tests FAIL with assertion errors about missing/Unknown cells (NOT import errors).

**GREEN phase (production code):**

3. In `web/export_service.py`:
   - Add the new module-level helper immediately after `_resolve_result_full_text` (around line 75):
     ```python
     def _resolve_result_display(result, meta_mgr) -> tuple:
         """Return (shelfmark, title, library_code, library_name).

         3-tier fallback:
         1. Row's `display` dict (non-empty) -> verbatim (back-compat with live
            search-result rows pre-compaction).
         2. Extract sys_id from `uid` via meta_mgr.parse_full_id_components
            -> meta_mgr lookups.
         3. Extract sys_id from `raw_header` via regex `(99\\d{8,})`
            -> meta_mgr lookups (mirrors shared/search_serializer.py:716-738
            for legacy parallels-row fixtures that carry raw_header but no uid).
         4. Fallback to ('Unknown', '', '', '') when meta_mgr is unavailable
            or neither uid nor raw_header parses to a sys_id.

         Mirrors the lazy-rehydration pattern of `_resolve_result_full_text`,
         and the legacy-vs-compact fallback in
         `shared/search_serializer.py:_to_parallels_envelope_item`.
         """
         display = result.get('display') if isinstance(result, dict) else None
         if isinstance(display, dict) and display:
             shelfmark = display.get('shelfmark', '') or 'Unknown'
             title = display.get('title', '') or ''
             library_code = display.get('library_code', '') or ''
         else:
             if not meta_mgr:
                 return ('Unknown', '', '', '')
             # Tier 2: uid -> sys_id
             sys_id = ''
             uid = (result.get('uid') if isinstance(result, dict) else '') or ''
             if uid:
                 try:
                     parsed = meta_mgr.parse_full_id_components(uid) or {}
                     sys_id = parsed.get('sys_id') or ''
                 except Exception:
                     sys_id = ''
             # Tier 3: raw_header regex fallback (legacy parallels-row idiom)
             if not sys_id:
                 raw_header = (result.get('raw_header') if isinstance(result, dict) else '') or ''
                 if raw_header:
                     try:
                         import re as _re
                         m = _re.search(r'(99\d{8,})', raw_header)
                         if m:
                             sys_id = m.group(1)
                     except Exception:
                         sys_id = ''
             if not sys_id:
                 return ('Unknown', '', '', '')
             try:
                 meta = meta_mgr.get_meta_for_id(sys_id)
                 if isinstance(meta, tuple) and len(meta) >= 2:
                     shelfmark = meta[0] or 'Unknown'
                     title = meta[1] or ''
                 else:
                     shelfmark, title = 'Unknown', ''
             except Exception:
                 shelfmark, title = 'Unknown', ''
             try:
                 library_code = meta_mgr.get_library_for_id(sys_id) or ''
             except Exception:
                 library_code = ''
         # Resolve library display name via existing helper (handles localization + fallback)
         library_name = ''
         if library_code:
             try:
                 from genizah_core import get_library_display as core_get_library_display
                 from web.translations import get_language
                 library_name = core_get_library_display(library_code, short=False, lang=get_language()) or library_code
             except Exception:
                 library_name = library_code
         return (shelfmark, title, library_code, library_name)
     ```
   - In `ExportService.export_search_results_excel` (line 307), replace the per-row display lookup. Current code (lines 336-349):
     ```python
     for res in results:
         display = res.get('display', {})
         snippet = clean_text_single_line(remove_highlight_markers(res.get('snippet', '')))
         full_text = clean_text_single_line(_resolve_result_full_text(res))[:32000]
         library_code = display.get('library_code', '')
         library_name = self.get_library_display(library_code, short=False) if library_code else ''
         row = [
             sanitize_text_for_excel(display.get('shelfmark', '')),
             ...
             sanitize_text_for_excel(display.get('id', '')),
             ...
         ]
     ```
     Replace with:
     ```python
     for res in results:
         shelfmark, title, library_code, library_name = _resolve_result_display(res, self.meta_mgr)
         snippet = clean_text_single_line(remove_highlight_markers(res.get('snippet', '')))
         full_text = clean_text_single_line(_resolve_result_full_text(res))[:32000]
         # Resolve sys_id for the "System ID" column - prefer display.id when row is legacy,
         # else parse from uid (graceful '' fallback).
         display = res.get('display') if isinstance(res.get('display'), dict) else {}
         sys_id_for_cell = display.get('id') or ''
         if not sys_id_for_cell and self.meta_mgr:
             try:
                 parsed = self.meta_mgr.parse_full_id_components(res.get('uid', '') or '') or {}
                 sys_id_for_cell = parsed.get('sys_id') or ''
             except Exception:
                 sys_id_for_cell = ''
         row = [
             sanitize_text_for_excel(shelfmark),
             sanitize_text_for_excel(library_name),
             sanitize_text_for_excel(title),
             sanitize_text_for_excel(sys_id_for_cell),
             str(res.get('sort_score', '')),
             sanitize_text_for_excel(snippet),
             sanitize_text_for_excel(full_text),
         ]
     ```
   - In `ExportService.export_search_results_word` (line 378), apply the same change: replace the `display = res.get('display', {})` + per-field `display.get(...)` lookups with one `_resolve_result_display(res, self.meta_mgr)` call. The "System ID" string also gets the same sys_id-from-uid fallback.
   - In `ExportService.export_parallels_excel` (line 498), the inner `add_results` function currently extracts sys_id from `raw_header` via regex (lines 528-540). Replace that with `_resolve_result_display(item, self.meta_mgr)` returning the 4-tuple — sys_id extraction now happens inside the helper via uid (post-Task-1 rows) OR via raw_header (legacy fixtures / live UI rows that still carry raw_header per Task 1's allowlist). `source_ctx` and `text` still come from the row directly. **Continue reading `item.get('score', 0)` unchanged** — `score` is kept on the parallels row per Task 1.
   - In `ExportService.export_parallels_word` (line 584), apply the same change to `add_results`. **Do NOT rename `score`** anywhere.
4. In `shared/search_serializer.py::_serialize_item` (line 232):
   - Currently reads `display = result.get('display', {}) or {}`. Replace with a fallback chain that rehydrates from `meta_mgr` when the dict is empty.
   - At the top of `_serialize_item`, after the existing `display` extraction (line 250), add:
     ```python
     # SEED-002: rehydrate display fields from uid when row carries the
     # compact uid-only shape stored by web/export_state.py. Mirrors the
     # fallback already used in _to_parallels_envelope_item (lines 716-738).
     if not display and meta_mgr is not None:
         _sid = ''
         uid_for_parse = result.get('uid', '') or ''
         if uid_for_parse:
             try:
                 parsed_uid = meta_mgr.parse_full_id_components(uid_for_parse) or {}
                 _sid = parsed_uid.get('sys_id') or ''
             except Exception:
                 _sid = ''
         if not _sid:
             # Tier 3: raw_header regex fallback (matches existing parallels-envelope idiom)
             rh = result.get('raw_header', '') or ''
             if rh:
                 try:
                     import re as _re
                     m = _re.search(r'(99\d{8,})', rh)
                     if m:
                         _sid = m.group(1)
                 except Exception:
                     _sid = ''
         if _sid:
             try:
                 _meta = meta_mgr.get_meta_for_id(_sid)
                 if isinstance(_meta, tuple) and len(_meta) >= 2:
                     _shelf, _title = _meta[0] or '', _meta[1] or ''
                 else:
                     _shelf, _title = '', ''
             except Exception:
                 _shelf, _title = '', ''
             try:
                 _lib = meta_mgr.get_library_for_id(_sid) or ''
             except Exception:
                 _lib = ''
             display = {
                 'id': _sid,
                 'shelfmark': _shelf,
                 'title': _title,
                 'library_code': _lib,
             }
     ```
   - Leave the rest of `_serialize_item` unchanged — once `display` is populated (either from the row OR from rehydration), the existing logic (lines 251-322) works as-is.
   - The same path needs the `excerpt` field (line 270-271) — `full_text` and `full_text_excerpt` are both absent now on compacted search rows. Update the excerpt fallback to call `_resolve_result_full_text`-style Tantivy rehydration ONLY when `result.get('full_text')` is present (live search rows). For compacted rows, set `excerpt = ''` — the public JSON contract already allows empty excerpt strings. Test `tests/test_search_serializer.py` (existing) will catch any regression.
5. Run `pytest tests/test_export_service.py -x -q` — 51 existing + 3 new = 54 tests must pass.
6. Run `pytest tests/test_search_serializer.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -x -q` — all green; this validates the public JSON shape pre- vs post-fix.
7. Run `pytest tests/test_export_cross_user_isolation.py -x -q` — cross-user isolation untouched.
8. Run `python -m ruff check web/export_service.py shared/search_serializer.py tests/test_export_service.py` — must be clean.
  </action>
  <verify>
    <automated>pytest tests/test_export_service.py tests/test_search_serializer.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py tests/test_export_cross_user_isolation.py -x -q &amp;&amp; python -m ruff check web/export_service.py shared/search_serializer.py tests/test_export_service.py</automated>
  </verify>
  <done>
- `_resolve_result_display` helper exists in `web/export_service.py`, returns 4-tuple with 3-tier fallback (display dict → uid → raw_header regex → 'Unknown').
- All 4 export paths (`export_search_results_excel`, `export_search_results_word`, `export_parallels_excel`, `export_parallels_word`) call the new helper instead of reading `res.get('display', {})` directly.
- `_serialize_item` in `shared/search_serializer.py` rehydrates display fields from `meta_mgr.get_meta_for_id(sys_id)` when `display` is empty, with both uid and raw_header fallbacks.
- `score` (parallels) is NOT renamed anywhere — the export sites continue reading `item.get('score', 0)`.
- 3 new rehydration tests pass in `test_export_service.py` (display-from-uid, graceful Unknown with explicit `('Unknown', '')` mock, legacy-vs-compact equivalence).
- All existing tests in `test_export_service.py` (including the parallels tests that use the `sample_parallels_results` fixture with `raw_header` + `score` and no `uid`) stay green via the tier-3 raw_header fallback.
- All existing tests in `test_search_serializer.py`, `test_api_export_json.py`, `test_api_legacy_unchanged.py`, `test_export_cross_user_isolation.py` stay green.
- ruff clean on `web/export_service.py`, `shared/search_serializer.py`, `tests/test_export_service.py`.
  </done>
</task>

<task type="auto">
  <name>Task 3: Tree-wide test sweep + invariant scanners + ruff finalization + commit</name>
  <files>web/export_state.py, web/export_service.py, shared/search_serializer.py, tests/test_export_state_cap.py, tests/test_export_service.py</files>
  <behavior>
    - Full pytest tree-wide run reports >=2059 passing (2051 prior baseline + 5 brand-new in test_export_state_cap + 3 brand-new in test_export_service = 2059 minimum; the 2 in-place test updates in test_export_state_cap don't change total count). No new failures introduced beyond the 5+3 added test functions and the 2 in-place updates.
    - The Phase 87/88 invariant scanners stay green: `tests/test_no_raw_storage_access.py`, `tests/test_no_appstate_export_fields.py`, `tests/test_no_deleted_state_references.py`.
    - `web/api.py` has zero direct reads of `payload['results'][i]['display']` (verified via grep). If any such read is found, this is a NEW MIGRATION CASE — `meta_mgr` is not in scope in `web/api.py` handler bodies (handlers route through `ExportService` which carries the meta_mgr), so there is no recipe to fix it inline. HALT and ESCALATE to the user: a new code path is reading `display` directly and needs a separate migration design.
    - ruff check on every touched source file reports zero issues.
    - Git working tree contains a single coherent commit (Tasks 1 + 2 MUST land together — see threat T-260519-hoi-05): `fix(web): SEED-002 — uid-only export payload (~44x per-row reduction)` covering all three production files + two test files.
  </behavior>
  <action>
1. Run the full tree-wide test sweep:
   ```
   pytest -x -q
   ```
   Expected: >=2059 passing tests. If any failure occurs:
   - If failure is in a test that asserts the OLD row shape (e.g., expects `display.shelfmark` to be readable from a stored payload), update that test to use the rehydration path (mock `meta_mgr` and call through `ExportService` or `serialize_search_payload`).
   - If failure is a genuine regression (e.g., an unrelated test broken by Task 1/2), fix the production code — do NOT modify the test to hide it.
2. Run the Phase 87/88/90 invariant scanners explicitly:
   ```
   pytest tests/test_no_raw_storage_access.py tests/test_no_appstate_export_fields.py tests/test_no_deleted_state_references.py tests/test_no_anonymous_reads_on_authenticated_tables.py -x -q
   ```
   All must pass — this fix does not touch `app.storage.user` directly (it routes through the unchanged `web/export_state.py` chokepoint helpers) and does not reference any deleted state fields.
3. Grep `web/api.py` for any direct `display` reads against export payloads (final sanity check):
   ```
   grep -nE "display\b" web/api.py
   ```
   Expected: zero matches inside export handler bodies (lines 2069-2311).
   **If any match exists, HALT AND ESCALATE TO THE USER.** This means a new code path reads `display` directly from `web/api.py`, and `meta_mgr` is not in scope there. There is no recipe to wire `_resolve_result_display` into a handler that doesn't already have the meta_mgr injected — that would require a separate design pass. DO NOT silently try to add an import or refactor; surface the finding and stop.
4. Confirm no orphaned imports / dead constants survived:
   ```
   grep -rn "_SEARCH_FULL_TEXT_EXCERPT_CHARS\|_PARALLELS_CHUNK_HITS_CAP\|_PARALLELS_CHUNK_TEXT_STORAGE_CHARS\|_compact_chunk_hit" web/ shared/ tests/
   ```
   Expected: zero matches (Task 1 deleted these from web/export_state.py + tests/test_export_state_cap.py). If any survive, they are dead references — delete them. (`_SEARCH_RESULT_EXCERPT_CHARS` is in `web/pages/search_state.py` which is OUT OF SCOPE — it stays.)
5. Final ruff sweep over every touched file:
   ```
   python -m ruff check web/export_state.py web/export_service.py shared/search_serializer.py tests/test_export_state_cap.py tests/test_export_service.py
   ```
   Must be clean (zero issues).
6. Stage and commit (single atomic commit — see threat T-260519-hoi-05: Tasks 1 + 2 MUST NOT land separately):
   ```
   git add web/export_state.py web/export_service.py shared/search_serializer.py tests/test_export_state_cap.py tests/test_export_service.py
   git commit -m "$(cat <<'EOF'
   fix(web): SEED-002 uid-only export payload (~44x per-row reduction)

   Shrinks export-payload row schema from ~22 KB to ~500 bytes by storing
   only query-specific fields (uid, sort_score, snippet, match_terms for
   search; same plus score, source_ctx, text, raw_header for parallels)
   and rehydrating display fields (shelfmark, title, library, library_name)
   from meta_mgr at export/serialize time. Full-text rehydration via
   Tantivy was already wired in ed6f89c4 (_resolve_result_full_text);
   this extends the same lazy-rehydration pattern to display fields.

   Per-search Python allocations drop materially. Even if NiceGUI
   Observable retention pins stale payloads (separate framework bug,
   out of scope), the retained objects are now negligible.

   - web/export_state.py: _compact_search_result_row uses explicit
     allowlist {uid, sort_score, snippet, match_terms}.
     _compact_parallels_result_row uses explicit allowlist
     {uid, sort_score, score, snippet, match_terms, source_ctx, text,
     raw_header} — `score` and `raw_header` are intentionally KEPT
     because (a) live parallels UI reads them at 13 sites in
     web/pages/parallels.py (compact_parallels_result_rows feeds
     p_state.results) and (b) shared/search_serializer.py:691 sums
     score into the public /api/parallels aggregate_score. chunk_hits
     dropped from parallels rows.
   - web/export_service.py: new _resolve_result_display(row, meta_mgr)
     helper with 3-tier fallback (display dict -> uid -> raw_header
     regex -> 'Unknown'); all 4 export paths (search xlsx/docx,
     parallels xlsx/docx) route through it.
   - shared/search_serializer.py: _serialize_item rehydrates display
     when row's display is empty, with uid and raw_header fallbacks
     (mirrors existing parallels-envelope idiom).
   - Tests: +5 new in test_export_state_cap.py (search-row uid-only,
     parallels-row safe-allowlist proving score/raw_header KEPT,
     per-row <2 KB with realistic Hebrew snippet, 5000-row <5 MB,
     ed6f89c4 invariants); +3 new in test_export_service.py
     (rehydration from uid, graceful Unknown with explicit mocks,
     legacy-vs-compact equivalence); 2 in-place updates in
     test_export_state_cap.py (excerpt key absent, chunk_hits absent).

   Explicitly OUT OF SCOPE: web/pages/search_state.py compactor
   (tab-restore path; would silently break 25+ display.* reads in
   web/pages/search.py if shrunk to uid-only — separate follow-up).

   Status flips (OPEN_ISSUES.md P1, SEED-002 status: dormant -> shipped)
   happen in a separate post-deploy doc-only quick task once
   /_internal/memstat soak confirms KB-range payloads.

   Refs: .planning/seeds/SEED-002-uid-only-export-payload.md
   Predecessor: ed6f89c4 (field-strip fix, 2026-05-19)
   EOF
   )"
   ```
7. Run `git status` to confirm a clean tree post-commit.
  </action>
  <verify>
    <automated>pytest -x -q &amp;&amp; pytest tests/test_no_raw_storage_access.py tests/test_no_appstate_export_fields.py tests/test_no_deleted_state_references.py tests/test_no_anonymous_reads_on_authenticated_tables.py -x -q &amp;&amp; python -m ruff check web/export_state.py web/export_service.py shared/search_serializer.py tests/test_export_state_cap.py tests/test_export_service.py</automated>
  </verify>
  <done>
- Full tree-wide pytest: >=2059 passing, 0 failing.
- Phase 87/88/90 invariant scanners: all green.
- `web/api.py` grep for `display\b`: zero matches inside export handler bodies (lines 2069-2311). If any match, escalation happened — not auto-fixed.
- Dead-constant grep returns zero matches.
- ruff check on 5 touched files: clean.
- Single coherent commit created with the multi-line message above (Tasks 1 + 2 landed atomically per threat T-260519-hoi-05).
- `git status` shows a clean working tree.
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Browser → NiceGUI export endpoint | Authenticated session reads its own `app.storage.user['export_search_payload']` via `web.safe_storage` chokepoint (Phase 87 invariant) |
| Export handler → meta_mgr | Read-only lookups against the local `csv_bank` / FJMS / NLI sidecars (no PII); failure paths degrade to `'Unknown'`/`''` |
| Export handler → Tantivy `get_full_text_by_id` | Read-only index lookup; already in production via `_resolve_result_full_text` since `ed6f89c4` |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-260519-hoi-01 | Information Disclosure | `_resolve_result_display` cross-user leak | mitigate | Helper takes `meta_mgr` and `result` as args (no global storage reads). Per-session storage isolation is enforced by the Phase 87 chokepoint upstream of the row compactors; this fix doesn't touch the chokepoint. Cross-user isolation tests in `tests/test_export_cross_user_isolation.py` (4 tests) re-run in Task 3 to confirm no regression. |
| T-260519-hoi-02 | Denial of Service | Tantivy / meta_mgr rehydration burst on heavy export | accept | Heavy exports (5000 rows) trigger 5000 `meta_mgr.get_meta_for_id` calls + up to 5000 `get_full_text_by_id` calls. Each call is O(1) against the in-memory `csv_bank` dict (meta) or a Tantivy point-query (full text). Acceptable — same pattern is already shipped in `_resolve_result_full_text` for full text. If user-visible export latency regresses, the mitigation is to batch-resolve sys_ids upfront (mirror `_safe_fjms_lookups` in `shared/search_serializer.py`). Out of scope for this fix. |
| T-260519-hoi-03 | Tampering | Compacted row with malicious uid | accept | uid is user-provided via search input -> Tantivy -> result row -> `parse_full_id_components` regex `99\d{8,}`. Regex captures only digit sequences; no shell/SQL/code-execution surface. `get_meta_for_id` does a dict lookup against `csv_bank`; failure path returns `('Unknown', '')`. No new threat introduced beyond what `_resolve_result_full_text` already shipped. |
| T-260519-hoi-04 | Repudiation | Public API JSON shape drift breaking skill consumers | mitigate | `_serialize_item` rehydration is a fallback (only triggers when `display` is empty). Live search results continue to populate `display` as today; only stored-then-exported rows hit the rehydration path. `score` is INTENTIONALLY KEPT on parallels rows (Task 1 allowlist) so `shared/search_serializer.py:691` continues to compute non-zero `aggregate_score` for `/api/parallels` group sort. Task 3 explicitly runs `test_api_export_json` (5 tests) and `test_api_legacy_unchanged` (4 tests) to confirm byte-content equivalence. `test_search_serializer.py` (existing) also re-runs. |
| T-260519-hoi-05 | Tampering / Atomicity | Tasks 1+2 landing in separate commits | mitigate | Task 1 alone leaves the export-path technically broken (display dict is gone but `_resolve_result_display` doesn't exist yet); Task 2 alone has no broken intermediate. A future hot-fix touching ONLY `web/export_state.py` OR ONLY `web/export_service.py` reintroduces the broken intermediate. **Mitigation:** Task 3 stages and commits ALL touched files in a single `git commit`. The commit message explicitly documents the invariant. If a future cherry-pick or partial revert is attempted, the reviewer must reject any patch that touches one of these files without the other. (Reverting BOTH together is safe; reverting one alone is not.) |
| T-260519-hoi-06 | Repudiation / Scope | Silent breakage of tab-restore in `web/pages/search_state.py` | mitigate | The plan-checker identified that `_compact_result_rows` in `web/pages/search_state.py:254-273` feeds `restore_search_active_snapshot` at line 299 (`state.results = raw.get('results', [])`), and 25+ sites in `web/pages/search.py` read `r.get('display', {}).get('id')`, `.get('title')`, `.get('library_code')` from the live `state.results`. Shrinking that compactor to uid-only (as an earlier revision of this plan proposed) would silently produce `None` / `{}` reads after every tab-restore, breaking the displayed search results page with no warning. **Mitigation:** `web/pages/search_state.py` is EXPLICITLY OUT OF SCOPE for SEED-002. The tracemalloc allocator pressure at `search_state.py:262/258/267` remains as a separate follow-up requiring its own design (possibly preserve `display` and drop only `full_text_excerpt`) and its own threat model. Documented in the plan's `<objective>` "Out of scope" section, `must_haves.truths`, and the commit message. |

## Why no `security_enforcement` block beyond STRIDE

The chokepoint at `web/safe_storage.py` (Phase 87) and the request-scoped auth at `web/supabase_client.py` (Phase 90) are unchanged by this fix. No new RLS reachability surface, no new auth-state writes, no new browser-storage reads. This fix is a per-row size optimization inside the existing per-session storage path.
</threat_model>

<verification>
## Plan-level verification

The plan's must-haves are verified by:

1. **Row schema** — Task 1's 5 new tests in `test_export_state_cap.py` directly assert the uid-only key set on search rows, the safe-allowlist key set on parallels rows (with explicit `score` + `raw_header` retention assertions), the <2 KB per-row size with a realistic Hebrew snippet, the <5 MB 5000-row payload size, and the preservation of the `ed6f89c4` field-strip invariants. Task 1's 2 in-place test updates assert the negative shape (no `full_text_excerpt`, no `chunk_hits`).

2. **Export rehydration** — Task 2's 3 new tests in `test_export_service.py` directly assert that Excel exports rehydrate shelfmark/title/library_code from uid via meta_mgr, degrade gracefully to 'Unknown' on unresolvable uid (with explicit `('Unknown', '')` mock to prevent MagicMock string coercion false-positives), and produce content-equivalent output for legacy-vs-compacted rows. The existing parallels test fixtures (using `raw_header` + `score`, no `uid`, no `display`) keep working via the tier-3 raw_header regex fallback in `_resolve_result_display`.

3. **Public JSON shape preservation** — Task 2's `_serialize_item` rehydration + Task 3's `test_api_export_json` + `test_api_legacy_unchanged` + `test_search_serializer` re-runs prove the public JSON contract is unchanged. The `score`-retention invariant in Task 1 ensures `shared/search_serializer.py:691` continues computing meaningful `aggregate_score` for `/api/parallels` group ordering.

4. **Live parallels UI** — Task 1's parallels-row allowlist explicitly keeps `score` and `raw_header`, so the 13 read sites in `web/pages/parallels.py` (8 for `score`, 5 for `raw_header`) continue functioning without any source-code change in `web/pages/parallels.py`.

5. **Cross-user isolation** — Task 3's `test_export_cross_user_isolation` re-run + Task 1's `test_no_raw_storage_access` re-run prove the Phase 87/88 invariants survive.

6. **Tab-restore safety** — `web/pages/search_state.py` is intentionally untouched. The 5 existing tests in `tests/test_search_state.py` (lines 29-46, 103, 208, 231) continue to exercise the tab-storage path with their own data and pass without modification.

7. **Atomicity** — Task 3 commits all touched files in a single git commit (threat T-260519-hoi-05). The commit message documents the invariant for future reviewers.

8. **No deploy-step coupling** — The plan deliberately stops at "clean commit; git status clean". Deploy is a separate human decision; status flips happen in a separate post-deploy doc-only quick task.

## Post-plan (out of scope for this task, surfaced for the user)

Once committed and (separately) deployed:
- Pull `/_internal/memstat` from production; the dominant `top_keys` entry for `export_search_payload` should drop from the current MB range to the KB range.
- Optional: `tracemalloc.start(1)` -> heavy search -> snapshot; the `web/export_state.py` row-compactor allocators should drop materially. The `web/pages/search_state.py:262/258/267` allocator pressure REMAINS (not addressed by this plan) — that's a separate follow-up tracked separately.
- Once verified, flip OPEN_ISSUES.md P1 row and `.planning/seeds/SEED-002-uid-only-export-payload.md` status `dormant` -> `shipped` in a separate doc-only quick task.
</verification>

<success_criteria>
- All three tasks' `done` blocks satisfied.
- Full pytest tree-wide run >=2059 passing tests (2051 prior baseline + 5 brand-new in test_export_state_cap + 3 brand-new in test_export_service = 2059 minimum; the 2 in-place updates don't change count).
- ruff clean on all 5 touched files (3 production + 2 test).
- Single commit landed: `fix(web): SEED-002 uid-only export payload (~44x per-row reduction)`.
- `git status` clean.
- No direct reads of `payload['results'][i]['display']` survive in `web/api.py` (verified via grep). If any found, the plan halted and escalated to the user instead of auto-fixing.
- The Phase 87 lint scanner (`tests/test_no_raw_storage_access.py` with allowlist `[]`) stays green — confirms the fix didn't introduce new raw `app.storage.user` access.
- The Phase 88 invariant scanners (`test_no_appstate_export_fields.py`, `test_no_deleted_state_references.py`) stay green — confirms no AppState mirror fields resurrected.
- Public JSON shape on `/api/search` and `/api/parallels` is byte-content-equivalent pre-vs-post fix when fed compacted rows (`test_api_export_json` + `test_api_legacy_unchanged` re-runs).
- Parallels `aggregate_score` sort order on `/api/parallels` is preserved — `score` is kept on the parallels row per the allowlist, so `shared/search_serializer.py:691` continues to sum it.
- `web/pages/search_state.py` is UNTOUCHED; the 5 tests in `tests/test_search_state.py` pass without modification.
</success_criteria>

<output>
After completion, create `.planning/quick/260519-hoi-ship-seed-002-uid-only-export-payload/260519-hoi-SUMMARY.md` capturing:
- Commit hash of the SEED-002 fix
- Final test counts (pytest line) showing >=2059 passing
- Bullet list of per-file changes with line-count deltas (`git diff --stat`)
- Pointer to the seed file (`.planning/seeds/SEED-002-uid-only-export-payload.md`) noting it stays `status: dormant` until the separate post-deploy verification quick task flips it to `shipped`
- Pointer to OPEN_ISSUES.md P1 row noting it remains in current state until the separate post-deploy verification quick task
- A "Followups" section: (a) the tracemalloc allocator-pressure source at `web/pages/search_state.py:262/258/267` remains; future work to address it must preserve the tab-restore contract (25+ `display.*` reads in `web/pages/search.py`); (b) deploy via `scp web/export_state.py web/export_service.py shared/search_serializer.py ubuntu@…:GenizahSearch/…` + `sudo systemctl restart genizah-web.service`; (c) post-soak: separate doc-only quick task to flip SEED-002 + OPEN_ISSUES.md.
</output>
