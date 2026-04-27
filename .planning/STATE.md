---
gsd_state_version: 1.0
milestone: v7.10
milestone_name: Search API
status: executing
stopped_at: Plan 77-04 complete; ready for Plan 77-05 (docs/OPEN_ISSUES + docs/CODE_INDEX update + manual smoke check)
last_updated: "2026-04-27T17:25:51Z"
last_activity: 2026-04-27 -- Plan 77-04 complete (4 commits, 5 new tests GREEN, HTTP handlers + toolbar buttons + Hebrew translations, ~6 min)
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 5
  completed_plans: 4
  percent: 13
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-27)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 77 — serializer-json-export

## Current Position

Phase: 77 (serializer-json-export) — EXECUTING
Plan: 5 of 5 (next; 77-01, 77-02, 77-03, 77-04 complete)
Status: Executing Phase 77
Last activity: 2026-04-27 -- Plan 77-04 complete (HTTP handlers /api/export/json + /api/export/parallels/json; toolbar buttons on /search and /parallels; init_api_routes app_override refactor for HIGH-08; 5 new tests GREEN; full suite 1189 → 1194 passed)

Progress: [#         ] 13% (0/6 phases complete; 4/5 Phase 77 plans complete)

**Phase queue (v7.10):**

1. **Phase 77** — Serializer & JSON Export (EXPORT-01..04) ← in progress (4/5 plans complete)
2. Phase 78 — /api/search + Hardening Shell (API-01,04,05,06,07 + HARDEN-01..05)
3. Phase 79 — /api/browse Drill-Down (API-03) — Codex-recommended: validates locator round-trip via real consumer before a second producer
4. Phase 80 — /api/parallels (API-02)
5. Phase 81 — Claude Skill Consumer (SKILL-01..03)
6. Phase 82 — Internal Documentation (DOC-01, DOC-02)

Next step: `/gsd-plan-phase 77` to decompose Phase 77 into plans.

## Performance Metrics

**Velocity:**

- Total plans completed: ~210 (across 15 shipped milestones)
- Average duration: ~12 min (historical)

**Recent Trend:**

- v7.9: 10 phases, 23 plans (complete 2026-04-17, internal milestone)
- v7.8: 4 phases, 9 plans (shipped 2026-04-15, ~14 hours wall clock)
- v7.7: 4 phases, 8 plans
- Trend: Stable

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

**v7.10 roadmap-time decisions:**

- Serializer module is built **first** (Phase 77) so the JSON export and API responses share a single source of truth from day one — preventing drift before any consumer exists.
- Hardening primitives (rate limit, mode flag, error envelope, query/result caps, PostHog) bundle into the **first** API endpoint phase (Phase 78) rather than a separate hardening phase, so /api/parallels and /api/browse inherit them by reuse rather than retrofit.
- API-05 (drill-down locator) is mapped to Phase 78 only; Phase 79 inherits the locator on parallels responses as a behavioral consequence reflected in its success criteria. This keeps every requirement single-mapped while preserving the cross-phase obligation.
- The Claude skill (Phase 81) is the milestone's acceptance harness — it must run end-to-end against a live deployment before documentation closeout in Phase 82, so the docs reflect what shipped, not what was planned.

**Plan 77-01 decisions (2026-04-27):**

- Filter dict shape locked to **10 keys** matching the live snapshot at search.py:4232-4242 (HIGH-02 fix — earlier 6-key dict was incomplete, would not survive replay through search history restore).
- Search history restore extends to populate **state.last_results AND envelope-echo fields** drawn from the snapshot's stored query/mode/gap/filters (HIGH-01) — restored exports are now byte-identical-shape to live exports.
- Parallels history restore uses **state_snapshot['source_text'] + params dict** as canonical source, NOT inferred from p_state.results[0]['source_ctx'] (HIGH-03 — result rows lose chunk_size/mode/filters fidelity).
- Side-effect: `state.current_search_query` latent bug (declared at web/state.py:27, never assigned per RESEARCH §Pitfall 2) fixed at all 3 search-execute paths. Excel/Word filenames will produce meaningful filenames as a ride-along benefit.
- Wave 0 TDD: 22 RED tests written before implementation module exists. `pytest tests/test_search_serializer.py --collect-only` succeeds (file syntactically valid); each test fails with `ModuleNotFoundError: No module named 'shared.search_serializer'` until Plan 03 lands.

**Plan 77-02 decisions (2026-04-27):**

- D-13 Path A landed: `lab_composition_search` populates `chunk_hits` per uid as `(chunk_index_0_based, source_chunk_text, match_score, manuscript_snippet)` tuples — additive change, existing readers of `total_score`/`hits_count`/`ms_matches` unaffected.
- Rule 2 deviation: `chunk_hits` also surfaced onto the per-uid `item` dict at lines 1479-1497 (the dict callers actually receive via the returned `main`/`filtered`/`known` lists). Without this surface, the field would be internal-only — Plan 03's `serialize_parallels_payload` could not consume it. Plan 01's RED fixture `sample_parallels_results` already assumed each row carries `chunk_hits`, confirming this is the intended end-to-end contract.
- Behavioral test strategy (HIGH-04 fix): `LabEngine.__new__(LabEngine)` to bypass heavy `__init__`, real `LabSettings()` (constructable with no args) + lowered `comp_min_score=1` and `min_should_match=50` so synthetic match_score=100 with fingerprint-aligned matches passes the production filter gates. Monkeypatched `lab_index.parse_query`, `lab_searcher.search/.doc`, `_calculate_match_metrics`, `_is_phrase_statistically_weak` to drive the real loop end-to-end without a Tantivy index.
- Plan stub signature corrected: `_calculate_match_metrics` returns `(match_score, matches, best_window)` (score-first), not `(matches, best_window, match_score)` as the plan's prose suggested. Confirmed via `genizah_core.py:892` (`return 0, [], (0, 0)`) and the destructure at `:1343` before writing the test stub.

**Plan 77-03 decisions (2026-04-27):**

- shared/search_serializer.py is the single source of truth for Claude-friendly JSON: 5 public exports (SCHEMA_VERSION=1, serialize_search_payload, serialize_parallels_payload, build_search_filename, build_parallels_filename) and one private `_serialize_item` shared structurally by both top-level functions per D-14 / EXPORT-03; `test_serializers_share_serialize_item` enforces no shadow `_serialize_search_item` / `_serialize_parallels_item` exists via `dir()` introspection — shape divergence is impossible without removing tests.
- HIGH-05 fix preserved: `_safe_fjms_lookups` retrieves the FJMS singleton via `get_fjms_service(thread_safe=True)` and does NOT call `.close()` on it — closing the module-level singleton at `shared/fjms_service.py:3164` would break every subsequent caller (search/browse/parallels enrichment, FjmsCatalogDialog) until process restart. Close remains owned exclusively by `reset_fjms_service()` (sidecar swap).
- HIGH-06 fix preserved: filename builders combine `%H%M%S` + `microsecond//1000` ms suffix + `next(itertools.count())` monotonic counter — `test_filename_uniqueness_consecutive` runs without `time.sleep` and still asserts distinct outputs. Format `genizah-search-2026-04-27T153042_837_n.json` is grep-friendly and time-sortable.
- HIGH-07 fix preserved: `_build_image_url(sys_id, p_num, library_code)` returns `None` when `library_code not in NLI_RESOLVABLE_LIBRARY_CODES` (frozenset of CUL/JTS/BL/Manchester/RNL/AIU/Mosseri/Gaster/Halper). Oxford-only hits emit null even with sys_id+p_num populated, preventing leaked /api/nli_image_by_sysid URLs that would 404. Phase 79 /api/browse owns Oxford image canonicalization.
- Lazy imports for `shared_export_utils.remove_highlight_markers`, `genizah_core.get_library_display`, `shared.fjms_service.get_fjms_service` keep the module fast to import and resilient when optional deps are missing or sidecars unconfigured. Top-level uses only stdlib (itertools, logging, re, datetime, typing).
- Path B fallback for parallels matches[]: when an item arrives without `chunk_hits` (future-proofing if a caller bypasses Plan 02), emit a single degenerate match using `source_ctx`/`text`/`score`. Plan 02's surface guarantees real callers always populate `chunk_hits`, but the fallback keeps the contract behaviorally graceful.
- Score rounding at 4 decimals applied uniformly: per-item `score`, parallels group `aggregate_score` (SUM then round), per-match `score` in `matches[]`. Test `test_score_rounded_to_4_decimals` asserts `0.873112948 → 0.8731`.

**Plan 77-04 decisions (2026-04-27):**

- HIGH-08 fix landed: `init_api_routes(app_override=None)` accepts an optional FastAPI app instance; tests register routes onto a bare app per fixture, the NiceGUI singleton is provably untouched (`test_init_api_routes_does_not_mutate_nicegui_singleton` asserts `nicegui_app.routes` count unchanged after `init_api_routes(bare)`).
- Mechanical decorator rename inside `init_api_routes`: 37 `@app.X` → `@target_app.X` (where `target_app = app_override if app_override is not None else app`); zero module-level `@app.` decorators existed, so module scope was untouched.
- Two new GET handlers: `/api/export/json` (consumes `state.last_results` + envelope-echo fields) and `/api/export/parallels/json` (consumes `state.parallels_results` + `state.parallels_filtered` + `state.parallels_search_meta`); both return `JSONResponse` with `Content-Disposition` filename from Plan 03's `build_search_filename` / `build_parallels_filename`.
- Rule 1 fix (auto-applied during Task 2 test execution): `export_parallels_json` originally crashed in non-NiceGUI request contexts because `app.storage.user.get(...)` requires a session cookie. Fix: empty-state check moved BEFORE storage access (empty path never touches storage), and storage access for the populated path wrapped in `try/except` so the handler degrades gracefully. Production behavior with active NiceGUI session is unchanged.
- Toolbar UX divergence preserved: search-page button is always-enabled (matches existing Excel/Word neighbors); parallels-page button captured into `export_json_btn` with full lifecycle gating (3 wiring sites: `_reset_parallels` line 1942, render-empty line 2659, render-populated line 2667).
- LOW-01 closed: Hebrew translations `"Export JSON" → "יצוא ל-JSON"` and `"Download JSON" → "הורד JSON"` added adjacent to existing `"Export Word" / "Export Excel"` entries in `genizah_translations.py:1589-1590`.

### Pending Todos

- Migrate desktop corrections fetch to shared corrections_service
- CUT-01: Remove read-only PGP tables from Supabase (legacy desktop users depend on them)
- Date range filter using CopyToDate (21K rows)
- Creation type filter via code_values (CreationTypeCode, 69K rows)

### Blockers/Concerns

- DESK-03/DESK-02 shared image helpers: ManuscriptViewerWidget and PuzzleCanvasWindow may share IIIF fetch / image adjustment code. Phase 69 discuss-phase must map this surface before extraction.
- WEBM-03 architectural risk: page-scoped state refactor changes runtime data flow, not just file layout. Phases 72-73 splits should be stable before attempting.
- v7.10 watch: existing `/api/*` routes (image proxies, puzzle uploads, NLI proxies) must remain unchanged through every phase touching `web/api.py`. Each phase gate spot-checks at least the image proxy + puzzle upload routes for response parity.

### Quick Tasks Completed

| # | Description | Date | Commit | Status | Directory |
|---|-------------|------|--------|--------|-----------|
| 260419-nwv | Bug: images don't fit the text on paired-leaf CUL shelfmarks (T-S NS 158.112) — parse_folio_label regex fix; CUL positional follow-up logged | 2026-04-19 | 5e87f55d | | [260419-nwv-bug-with-some-shelfmarks-images-esp-cul-](./quick/260419-nwv-bug-with-some-shelfmarks-images-esp-cul-/) |
| 260419-cfx | CUL CUDL positional canvas mismatch fix (H1) — folio+side resolver + NLI fallback in web `/api/cambridge_image` and desktop browse; H3 retracted (text-layer vs image-layer FL ids, not an IE bug) | 2026-04-19 | a854a5ee | Needs Review | [260419-cfx-cul-cudl-folio-side-mapping](./quick/260419-cfx-cul-cudl-folio-side-mapping/) |

## Session Continuity

Last session: 2026-04-27T17:25:51Z
Stopped at: Plan 77-04 complete; ready for Plan 77-05 (docs/OPEN_ISSUES + docs/CODE_INDEX update + manual smoke check)
Resume file: .planning/phases/77-serializer-json-export/77-05-PLAN.md

## Performance Metrics — Phase 77

| Plan | Duration | Tasks | Files | Commits |
|------|----------|-------|-------|---------|
| 77-01 | ~8 min | 3 | 4 | cdd91928, 2c5e94d5, d64ccb2b |
| 77-02 | ~6 min | 2 (3 commits) | 2 | 6ebefb71, e0259e6f, 25a4f769 |
| 77-03 | ~3 min | 1 | 1 | 78edec4b |
| 77-04 | ~6 min | 4 | 5 | 20972e66, f8b508de, 2c1fa26c, 01e18602 |
