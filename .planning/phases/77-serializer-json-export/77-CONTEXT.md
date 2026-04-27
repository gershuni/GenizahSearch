# Phase 77: Serializer & JSON Export - Context

**Gathered:** 2026-04-27
**Status:** Ready for planning

<domain>
## Phase Boundary

Build a single serializer module that owns the "Claude-friendly JSON" payload shape, then wire toolbar JSON download buttons on `/search` and `/parallels` to consume it. No HTTP endpoint yet — Phase 78 builds `/api/search` over the same serializer. This phase establishes the locator and payload contract before anything depends on it.

**In scope:** the serializer module, JSON download buttons next to existing Excel/Word buttons on `/search` and `/parallels`, the locator emitted on every result item, the envelope shape that Phase 78+ API responses inherit.

**Out of scope:** HTTP endpoints, rate limiting, mode flag (`SEARCH_API_MODE`), error envelope, PostHog observability, query-length / result caps — all bundled into Phase 78. Image proxy / NLI / puzzle-upload routes in `web/api.py` remain untouched.

</domain>

<decisions>
## Implementation Decisions

### Per-item Field Set

- **D-01:** Each result item carries the **minimum API-01 field set + a primary image URL**: `score`, `shelfmark`, `title`, `library` (full name + code), `domain`, `dating`, `snippet`, `excerpt`, `match_terms[]`, `image_url`, plus the locator (D-04). Full FJMS catalog/bibliography NOT included — those drill in via Phase 79 `/api/browse`.
- **D-02:** **No `full_text` field.** Each item carries an `excerpt` field (~500 plain-text characters from the start of the transcription/metadata). Skill drills in to full text via the locator + Phase 79 `/api/browse` when bulk text is needed. Keeps a 50-result download under a few hundred KB.
- **D-03:** `snippet` is **stripped clean** of `*term*` highlight markers (use existing `shared_export_utils.remove_highlight_markers()`). A separate `match_terms: [string, ...]` array on each item exposes which terms matched, so downstream consumers can re-apply highlights without parsing asterisks.
- **D-04:** **Locator emitted on every item with both fields always populated**: top-level `uid: string` (may be empty string for metadata-only / Title / Shelfmark hits where core does not assign a uid) AND `locator: {sys_id, volume_ie, p_num}` where `volume_ie` and `p_num` may be `null` for metadata-only hits. Skill picks whichever path it wants; Phase 79 `/api/browse` accepts both.

### Top-level Envelope

- **D-05:** **Flat envelope keys** — `{results, query, mode, count, total, generated_at, warnings, source, schema_version, filters?, ...}`. No `meta` namespace.
- **D-06:** **Full query echo.** Envelope echoes `query`, `mode`, `gap?`, `filters?` exactly as used by the search call. Parallels payload additionally echoes `chunk_size`, `max_freq?`, and the input source-text fields. Lets a downloaded JSON file be reopened weeks later and remain self-describing.
- **D-07:** `warnings: []` is **always present**, even on clean queries. Phase 78 will populate it for Responsa cascade downgrades and query-length caps; consistent shape now means no field appearing/disappearing later.
- **D-08:** Pagination metadata: `count` (items in this payload) + `total` (full result count from the search). No `offset` / `limit` in v1.
- **D-09:** Envelope carries `source: 'search' | 'parallels'` so a JSON file is self-identifying when reopened. Phase 78+ API extends to `'browse'` for Phase 79.
- **D-10:** Envelope carries `schema_version: 1`. Cheap insurance against a future format drift (`v2`); does not contradict the milestone's "no public stability promise" disclaimer.

### Parallels Shape

- **D-11:** Filtered / high-frequency hits live in a **separate top-level `filtered: [...]` array** alongside `results: [...]`. Mirrors the existing UI separation (`state.parallels_results` vs `state.parallels_filtered`). Phase 80 `/api/parallels` inherits this shape.
- **D-12:** Each parallels result carries source-chunk metadata: `source_chunk_text` (the input chunk that matched) and `chunk_index` (0- or 1-based position — planner verifies which core uses, documents in the response). When a result groups multiple matches (D-13), these fields move into the `matches[]` entries.
- **D-13:** **One result per manuscript with a `matches: [...]` array.** When shelfmark X matches input chunks 2, 5, and 7, X appears as a single result with `matches: [{chunk_index, source_chunk_text, manuscript_snippet, score}, ...]`. Mirrors what the UI shows (one card per manuscript) rather than chunk-level granularity. Top-level `score` on the result is the manuscript-level aggregate (planner picks aggregate rule from existing UI logic — e.g., max or sum).
- **D-14:** **Module API: two named functions sharing private helpers** — `serialize_search_payload(...)` and `serialize_parallels_payload(...)` in `shared/search_serializer.py`. Both call a private `_serialize_item()` for the common locator + title + library + domain + dating + image_url shape. Phase 78 `/api/search` and Phase 80 `/api/parallels` import the same two functions; modifying `_serialize_item()` updates download AND API in lockstep — proves EXPORT-03's single-source-of-truth requirement is structural, not aspirational.

### Claude's Discretion

- **Module placement:** `shared/search_serializer.py` — cross-app importable, sits with other shared services (`shared/document_service.py`, `shared/fjms_service.py`, `shared/nli_crossref_service.py`), avoids any web → core circular dependency when Phase 78 `web/api.py` imports it.
- **Button placement & icon:** Add to existing toolbars at `web/pages/search.py:~1446` and `web/pages/parallels.py:~1233`, immediately after the Word/Excel buttons. Use `icon='data_object'` (Material Symbols) with tooltip `tr('Export JSON')` / Hebrew equivalent. Match `props('flat round dense size=sm')` styling.
- **Disabled state:** Disable JSON button when no results loaded (mirrors existing Word/Excel pattern at `web/pages/parallels.py:1923-1924, 2608-2609, 2615-2616`).
- **Filename format:** `genizah-search-{ISO timestamp}.json` (e.g., `genizah-search-2026-04-27T1530.json`) and `genizah-parallels-{ISO timestamp}.json`. Use `make_safe_filename()` from `shared_export_utils.py`. Two consecutive downloads produce two distinct filenames — verified by EXPORT-04 success criterion.
- **HTTP route for the download:** add `GET /api/export/json` and `GET /api/export/parallels/json` to `web/api.py`, modeled exactly on existing `/api/export/excel` and `/api/export/parallels/excel` handlers (lines 1806–1908). Returns `application/json` with `Content-Disposition` filename header. These export routes are NOT the Phase 78 search-helper endpoints — they consume server-side `state.last_results` / `state.parallels_results` like the Excel/Word counterparts. Phase 78 `/api/search` is a separate, stateless POST endpoint that calls `serialize_search_payload(...)` directly without going through `state`.
- **Score normalization:** Rename core's `sort_score` → JSON `score`. Round to 4 decimals (e.g., `0.8731` not `0.8731129...`). Applies to both search and parallels.
- **Highlight extraction for `match_terms`:** Parse the existing `*term*` markers in `snippet` once at serialize time; emit unique terms in the order they first appeared. Reuses the same input the existing Excel highlight-fill logic walks (`web/export_service.py:344-350`).
- **Image URL field:** Use the existing `display['img']` field where populated; null when no image is yet resolved. Do NOT add new IIIF resolution work in this phase — Phase 79 `/api/browse` is the place for image-URL canonicalization.
- **Hebrew/RTL:** JSON is encoding-neutral; Hebrew text passes through as UTF-8. No per-field RTL flags needed; downstream consumers handle direction.
- **Tests:** unit tests for `serialize_search_payload` / `serialize_parallels_payload` covering field presence, locator both-present, snippet stripped + match_terms populated, filtered separation, multi-chunk grouping, empty-results envelope shape. Live download spot-check on `/search` and `/parallels` per phase gate.

### Folded Todos

None — no pending todos matched Phase 77 scope.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase / milestone specs
- `.planning/ROADMAP.md` §`Phase 77: Serializer & JSON Export` — success criteria including the cross-phase locator obligation (Phase 77 emits → Phase 78 emits → Phase 79 consumes → Phase 80 emits).
- `.planning/REQUIREMENTS.md` §`JSON Export` (EXPORT-01..04) — the four requirements this phase satisfies.
- `.planning/REQUIREMENTS.md` §`API Endpoints` (API-01, API-05) — Phase 78 contract the serializer must support; field set in API-01 directly informs D-01.
- `.planning/PROJECT.md` §Architecture — `shared/` service-layer convention motivating D-14's module placement.
- `.planning/STATE.md` — v7.10 milestone position and v7.10 watch list (existing `/api/*` routes must remain unchanged).

### Existing code (single source of truth for current shape)
- `web/api.py:1806-1908` — existing `/api/export/excel`, `/api/export/word`, `/api/export/parallels/{excel,word}` handlers; new JSON handlers follow the exact same pattern (server-side state → export_service call → Response with Content-Disposition).
- `web/export_service.py:286-411` — `export_search_results_excel/word` showing the `state.last_results` consumption pattern, library-name lookup, and credits/citation handling that JSON does NOT replicate (no banner needed in JSON).
- `web/export_service.py:477-655` — parallels Excel/Word exports showing how `state.parallels_results` + `state.parallels_filtered` flow today; informs D-11 split.
- `web/pages/search.py:1441-1446` — existing toolbar Excel/Word button placement; JSON button slots in alongside.
- `web/pages/parallels.py:1230-1233, 1923-1924, 2608-2609, 2615-2616` — parallels toolbar buttons + their enable/disable lifecycle.
- `genizah_core.py:7140-7183` — `_execute_metadata_search` showing the `display` dict + `metadata_only` flag (relevant to D-04 locator behavior when `uid` is empty).
- `genizah_core.py:7185` — `SearchEngine.execute_search` signature defining the input shape `serialize_search_payload` consumes.
- `genizah_core.py:1170-1210, 1470-1475, 2050-2055` — examples of `uid`, `sort_score`, `p_num`, `ie_id` field assembly in core results.
- `shared_export_utils.py` — `remove_highlight_markers`, `make_safe_filename`, `sanitize_text_for_excel`; the first two are reused by JSON path.

### Cross-phase obligations (read before planning Phase 77 to avoid contract drift)
- `.planning/ROADMAP.md` §`Phase 78: /api/search + Hardening Shell` — confirms the serializer module is imported by `/api/search`, justifying `shared/` placement.
- `.planning/ROADMAP.md` §`Phase 79: /api/browse Drill-Down` — confirms the locator round-trip; D-04 must produce locators Phase 79 can consume verbatim.
- `.planning/ROADMAP.md` §`Phase 80: /api/parallels` — confirms parallels payload shape Phase 80 inherits; D-11/D-12/D-13 lock that shape now.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared_export_utils.remove_highlight_markers()` — strips `*term*` markers; reused in D-03 for clean snippet, called once per item before extracting `match_terms`.
- `shared_export_utils.make_safe_filename()` — builds the filename base; combined with ISO timestamp for D-04 compliance.
- `web/state.py: state.last_results` / `state.parallels_results` / `state.parallels_filtered` — server-side data source for the download handlers, identical to Excel/Word path.
- `state.current_search_query` / `nicegui_app.storage.user.get('parallels_source_text', '')` — already populated for filename generation.

### Established Patterns
- `ui.download('/api/export/...')` toolbar button → FastAPI handler in `web/api.py` → call into `web/export_service.py` → Response with `Content-Disposition` header. JSON path repeats this exactly with `media_type='application/json'`.
- Server-side state for export handlers is established. Phase 77 inherits it; Phase 78 explicitly does NOT (the API endpoints are stateless).
- `shared/*_service.py` modules are imported by both web AND desktop. `shared/search_serializer.py` follows the same convention (web-only consumer in v7.10, but desktop could consume it later if v7.11 ever exposes desktop API).

### Integration Points
- New module: `shared/search_serializer.py` — exports `serialize_search_payload`, `serialize_parallels_payload`, and a `SCHEMA_VERSION = 1` constant.
- New routes: `GET /api/export/json` and `GET /api/export/parallels/json` in `web/api.py`, slotted next to existing export handlers.
- Toolbar wires: `web/pages/search.py:~1446` and `web/pages/parallels.py:~1233` add a third `ui.button` after Excel.
- `web/export_service.py` MAY get thin `export_search_results_json` / `export_parallels_json` wrappers that just call the `shared/search_serializer.py` functions and return `(bytes, filename)` — keeps the FastAPI handler call sites symmetric with Excel/Word. Planner's call.

### Test surface
- New unit tests (in `tests/`) for the two serializer functions: field presence, locator-both-populated invariant, snippet/match_terms separation, filtered key separation, multi-chunk grouping, empty-results envelope shape, schema_version constant.
- Spot-check downloads on `/search` and `/parallels` per phase gate; no automated browser test required.

</code_context>

<specifics>
## Specific Ideas

- The user prompt ("an option to fetch the full text if needed") explicitly chose the locator → Phase 79 path over a bulk-text-in-JSON path. This is the intent behind D-02. Planner: do not "helpfully" add a `full_text` field back in.
- The "single source of truth" requirement (EXPORT-03) is structural, not aspirational. Two named functions sharing a private `_serialize_item()` (D-14) is the *minimum* form that satisfies it. Two parallel implementations — even if started identical — are a violation.
- Locator both-fields-always-populated (D-04) is intentionally redundant. The roadmap success criterion says "uid preferred, fallback locator" but implementing it as "uid OR locator, never both" forces every consumer to branch. Always-both is one stable shape; the consumer branches once at the top: `target = uid or locator`.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

A few topics surfaced as "Claude's discretion" rather than user-decision territory and are captured above (module placement, button styling, score normalization, score-aggregation rule for parallels D-13). The planner should resolve these inside the plan rather than re-asking.

</deferred>

---

*Phase: 77-serializer-json-export*
*Context gathered: 2026-04-27*
