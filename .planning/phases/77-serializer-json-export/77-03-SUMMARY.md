---
phase: 77-serializer-json-export
plan: 03
subsystem: search-api
tags: [search-api, serializer, json-export, single-source-of-truth, locator-contract, tdd-green]

# Dependency graph
requires:
  - phase: 77 plan 01
    provides: 22 RED tests in tests/test_search_serializer.py + 8 fixtures (mock_meta_mgr, sample_search_results, oxford_only_hit, sample_parallels_results, sample_parallels_filtered, metadata_only_hit, autouse _disable_fjms) defining the contract this plan turns GREEN
  - phase: 77 plan 02
    provides: chunk_hits field surfaced on lab_composition_search returned items as (chunk_index, source_chunk_text, score, manuscript_snippet) tuples — consumed by serialize_parallels_payload to emit truthful matches[] arrays per D-13 Path A
provides:
  - shared/search_serializer.py — single-source-of-truth module exporting SCHEMA_VERSION=1, serialize_search_payload, serialize_parallels_payload, build_search_filename, build_parallels_filename
  - One private _serialize_item helper shared by both top-level functions (D-14 / EXPORT-03 structural — modifying it updates both download AND API in lockstep)
  - Per-item shape: uid (str, may be ''), locator {sys_id, volume_ie, p_num}, score (rounded 4dp), shelfmark, title, library {code, name}, domains (plural list per Plan 01 lock), dating, snippet (* stripped), excerpt (full_text[:500], no full_text field per D-02), match_terms (deduped in order), image_url
  - HIGH-05 fix: FJMS singleton (shared/fjms_service.py:3164) is NOT closed by the serializer; close remains exclusive to reset_fjms_service()
  - HIGH-06 fix: filename builders use millisecond timestamp + monotonic _filename_counter so two consecutive calls produce distinct filenames without sleep
  - HIGH-07 fix: image_url=null for non-NLI providers via NLI_RESOLVABLE_LIBRARY_CODES whitelist (CUL/JTS/BL/Manchester/RNL/AIU/Mosseri/Gaster/Halper) — Oxford-only hits emit null even when sys_id+p_num populated
  - Parallels grouping: SUM aggregation across uids in same sys_id, sorted desc by aggregate_score, separate top-level results[] and filtered[] arrays per D-11
affects: [77-04 (web/api.py JSON download handlers + toolbar buttons import these 5 exports), 78-* (POST /api/search inherits the same envelope and item shape via serialize_search_payload), 80-* (POST /api/parallels inherits via serialize_parallels_payload)]

# Tech tracking
tech-stack:
  added: []  # No new libraries — pure Python module composing existing helpers
  patterns:
    - "Single-source-of-truth serializer: one _serialize_item powers both top-level functions; structurally enforced by test_serializers_share_serialize_item via dir() introspection"
    - "Module-level itertools.count() filename counter combined with strftime millisecond timestamp — eliminates same-second collision without sleep"
    - "Conservative provider whitelist for image_url construction: NLI_RESOLVABLE_LIBRARY_CODES is the explicit set, all other providers (Oxford et al.) get null"
    - "Graceful degradation pattern: try/except around get_library_display + ImportError around fjms_service so the serializer works even when optional deps are missing or sidecars are not configured"
    - "meta_mgr passed as kwarg (RESEARCH §Pitfall 5) — module never imports MetadataManager at top level, avoiding circular-import risk"

key-files:
  created:
    - shared/search_serializer.py
  modified: []

key-decisions:
  - "Module placement: shared/search_serializer.py (per CONTEXT D-14 + analog shared/document_service.py) — under shared/ alongside other read-only service modules so both web and (future) desktop callers can import without web/ → shared/ direction violations"
  - "SCHEMA_VERSION=1 module-level constant; envelope/item shape changes that break consumers will bump this — Phase 78+ inherit version 1 unchanged"
  - "Score rounding at 4 decimals applied uniformly: per-item score (round(sort_score, 4)), parallels aggregate_score (round(sum, 4)), and per-match score in matches[] (round(chunk_score, 4))"
  - "_serialize_item is THE per-item shape — parallels reuses it via _to_parallels_envelope_item which builds a synthetic result dict (rep + sort_score=aggregate + minimal display from meta_mgr) then adds matches[] on top. This guarantees parallels items have every search-item key plus matches, not a parallel implementation"
  - "Path B fallback for matches[]: if a parallels item arrives without chunk_hits (e.g. future caller bypasses Plan 02), emit a degenerate single match using source_ctx + text + score so the contract holds"
  - "Generated_at timestamp at second resolution (sufficient for response metadata) is INDEPENDENT of filename timestamp at millisecond+counter resolution (required for filesystem uniqueness on consecutive saves) — separate concerns, separate helpers"

patterns-established:
  - "Wave 0 → Wave 2 TDD pattern: Plan 01 writes RED tests + fixtures, intermediate plans extend producer surfaces (Plan 02 chunk_hits), final plan implements consumer turning all tests GREEN — fixtures define the contract"
  - "Provider whitelist as frozenset literal at module top: clear, immutable, easy to audit and grow when a new provider gains NLI mirroring"
  - "Two-stage filename uniqueness: (1) %H%M%S base + microsecond//1000 ms suffix for time-sortability, (2) itertools.count() counter for absolute uniqueness — survives even pathological same-millisecond bursts"

requirements-completed: [EXPORT-01, EXPORT-02, EXPORT-03, EXPORT-04]

# Metrics
duration: 3min
completed: 2026-04-27
---

# Phase 77 Plan 03: shared/search_serializer.py (single source of truth) Summary

**shared/search_serializer.py is now the single source of truth for the Claude-friendly JSON payload shape — one module, two top-level functions, one private _serialize_item, all 22 contract tests GREEN, and three review hardening fixes (HIGH-05 singleton-no-close, HIGH-06 millisecond+counter filename, HIGH-07 Oxford-null image_url) baked in.**

## Performance

- **Duration:** ~3 min (17:08:05Z → 17:11:15Z)
- **Started:** 2026-04-27T17:08:05Z
- **Completed:** 2026-04-27T17:11:15Z
- **Tasks:** 1 (single atomic commit)
- **Files modified:** 0
- **Files created:** 1 (shared/search_serializer.py, 556 lines)

## Accomplishments

- **shared/search_serializer.py created** as the single-source-of-truth module:
  - 5 public exports: `SCHEMA_VERSION=1`, `serialize_search_payload`, `serialize_parallels_payload`, `build_search_filename`, `build_parallels_filename`
  - 1 shared private helper `_serialize_item` powering both top-level functions (D-14 / EXPORT-03 structural — `test_serializers_share_serialize_item` enforces by `dir()` introspection)
  - Supporting helpers: `_extract_match_terms`, `_build_image_url`, `_safe_library_name`, `_safe_fjms_lookups`, `_group_parallels_by_sys_id`, `_to_parallels_envelope_item`, `_filename_timestamp_with_ms`, `_utc_iso_now`
- **All 22 RED tests turn GREEN** in tests/test_search_serializer.py — this plan satisfies the contract Plan 01 wrote:
  - `TestSchemaConstant` (1 test): SCHEMA_VERSION = 1 importable
  - `TestSearchEnvelope` (4 tests): envelope shape, warnings always [] on clean queries, source='search' tag, empty-results envelope is well-formed
  - `TestPerItemShape` (9 tests): locator always-both-present, locator phase79 shape `{sys_id, volume_ie, p_num}` only, metadata-only hits get sys_id from display.id with locator volume_ie/p_num null, snippet * stripped + match_terms deduped in order, no full_text field on items, score rounded to 4 decimals, image_url server-relative or null, **HIGH-07 Oxford-only image_url null** (the new test that locks the fix), domains is plural list
  - `TestParallelsEnvelope` (4 tests): source='parallels' tag + filtered key present, results[]/filtered[] arrays disjoint, manuscripts grouped with matches[] (chunk_indices 0/2/4 from fixture), aggregate_score is SUM (50+15=65)
  - `TestSingleSourceOfTruth` (2 tests): no shadow `_serialize_search_item`/`_serialize_parallels_item` helpers exist (EXPORT-03 structural); search keys ⊂ parallels keys with parallels adding only `matches`
  - `TestFilenameUniqueness` (2 tests): **HIGH-06 no-sleep consecutive uniqueness** (the new test that locks the fix), filename starts with `genizah-search-` and ends with `.json` with ISO-ish timestamp
- **No regression**: full pytest suite is `1189 passed, 8 skipped` — exactly +22 from Plan 02's 1167 baseline, no other test affected
- **Three review hardening fixes preserved:**
  - HIGH-05: `_safe_fjms_lookups` does NOT call `.close()` on the FJMS singleton — `grep -c "fjms\.close()" shared/search_serializer.py` returns 0
  - HIGH-06: `_filename_timestamp_with_ms` combines `%H%M%S` + `microsecond//1000` ms + `next(_filename_counter)` so consecutive calls (no sleep) produce distinct filenames
  - HIGH-07: `_build_image_url(sys_id, p_num, library_code)` returns None when `library_code not in NLI_RESOLVABLE_LIBRARY_CODES`; whitelist contains 9 codes (CUL/JTS/BL/Manchester/RNL/AIU/Mosseri/Gaster/Halper) — Oxford excluded by design

## Task Commits

Single atomic commit:

1. **Task 1: shared/search_serializer.py** — `78edec4b` (feat)

## Files Created/Modified

- `shared/search_serializer.py` (NEW, 556 lines including the module docstring + section-divider comments) — Five public functions, eight private helpers, two module-level constants (SCHEMA_VERSION, NLI_RESOLVABLE_LIBRARY_CODES), one module-level monotonic counter (_filename_counter). Imports are split: top-level uses only stdlib (itertools, logging, re, datetime, typing); `shared_export_utils.remove_highlight_markers`, `genizah_core.get_library_display`, `shared.fjms_service.get_fjms_service` are imported lazily inside helpers — keeps the module fast to import and resilient when optional deps are missing.

## Decisions Made

- **Single-source-of-truth structurally enforced**: `_serialize_item` lives at one site (line 178). Both top-level functions reach into it: `serialize_search_payload` directly via list comprehension, `serialize_parallels_payload` indirectly via `_to_parallels_envelope_item` which builds a synthetic result dict (rep + sort_score=aggregate + minimal display synthesized from meta_mgr) then adds `matches: [...]` on top. `test_serializers_share_serialize_item` asserts no `_serialize_search_item` or `_serialize_parallels_item` shadow exists. Shape divergence is not possible without removing tests.
- **Path B fallback for matches[]**: when a parallels item arrives without `chunk_hits` (future-proofing if a caller bypasses Plan 02), emit a single degenerate match using `source_ctx`/`text`/`score`. Plan 02's surface guarantees real callers always populate `chunk_hits`, but Path B keeps the serializer behaviorally graceful.
- **Provider-aware image_url (HIGH-07)**: the whitelist `NLI_RESOLVABLE_LIBRARY_CODES = frozenset({'CUL', 'JTS', 'BL', 'Manchester', 'RNL', 'AIU', 'Mosseri', 'Gaster', 'Halper'})` is the conservative truth set. Adding a new provider requires a single literal-set edit. Oxford is intentionally excluded — Phase 79 `/api/browse` is the place for Oxford image canonicalization, not the serializer.
- **Counter + ms filename uniqueness (HIGH-06)**: chose `next(itertools.count())` over `random.randint(...)` for time-sortability. Format `2026-04-27T153042_837_n` is grep-friendly and human-readable. The test `test_filename_uniqueness_consecutive` calls `build_search_filename()` twice in two adjacent Python statements with NO sleep — the counter alone would suffice but the ms suffix makes filenames also time-monotonic, which helps users sorting downloads in Explorer/Finder.
- **Singleton lifecycle (HIGH-05)**: `_safe_fjms_lookups` retrieves `get_fjms_service(thread_safe=True)`, calls `is_available()` and per-record `get_catalog`/`get_domains_for_sys_ids`, and **returns** without `.close()`. The earlier revision called `.close()` and that would have broken every subsequent caller (search enrichment, browse enrichment, parallels enrichment, FJMS catalog dialog) until process restart. Close is owned exclusively by `shared.fjms_service.reset_fjms_service()` (sidecar swap on `fjms_enrichment.db` replacement).
- **Lazy imports**: `from shared_export_utils import remove_highlight_markers` and `from genizah_core import get_library_display` happen inside `_serialize_item` and `_safe_library_name` (not at module top) — this keeps the serializer importable even when those modules fail (e.g. CI worker without the full search index). The graceful-degrade fallback returns the library code unchanged when `get_library_display` is unavailable.
- **Score rounding at 4 decimals everywhere**: per-item `score` from `sort_score` (or `score`), parallels group `aggregate_score` (sum then round), per-match `score` in `matches[]`. Picked 4 decimals because Plan 01's `test_score_rounded_to_4_decimals` asserts `0.873112948 → 0.8731`, leaving 5+ decimal callers (if any) to lose precision rather than the JSON shape becoming inconsistent.

## Deviations from Plan

None — plan executed exactly as written. The plan's `<action>` block contained the complete implementation code; this plan was a pure copy-and-verify exercise. The only stylistic adjustment was using ASCII em-dashes (`--`) instead of Unicode em-dashes inside Python docstrings to avoid encoding issues on the Windows codepage — the plan's literal Unicode em-dashes were preserved in markdown comments only where they appeared in imports (none did).

## Issues Encountered

None — all 22 tests passed on the first run after the file was written. The plan's embedded code was verified end-to-end during the planning phase per RESEARCH.md §Code Examples (lines 432-753), so no runtime debugging was required.

The only minor edge case during verification was the Windows shell warning `LF will be replaced by CRLF the next time Git touches it` from the autocrlf config — cosmetic, no functional impact.

## User Setup Required

None — no external service configuration required. The serializer is a pure-Python read-only transform module with optional FJMS enrichment that gracefully degrades when the sidecar is absent.

## Next Phase Readiness

**Plan 77-04 (web/api.py JSON download handlers + toolbar buttons) is now unblocked:**

- All 5 exports are stable and tested: `SCHEMA_VERSION`, `serialize_search_payload`, `serialize_parallels_payload`, `build_search_filename`, `build_parallels_filename`
- Plan 04 wires HTTP handlers that call:
  ```python
  payload = serialize_search_payload(state.last_results, meta_mgr=state.meta_mgr,
                                     query=state.current_search_query,
                                     mode=state.current_search_mode,
                                     gap=state.current_search_gap,
                                     filters=state.last_filters_applied,
                                     warnings=state.last_search_warnings)
  filename = build_search_filename()
  return Response(content=json.dumps(payload, ensure_ascii=False),
                  media_type='application/json',
                  headers={'Content-Disposition': encode_filename_for_header(filename)})
  ```
  All envelope-echo state was populated by Plan 01 at six execute-time sites; the handler is a pure read-from-state operation.

**Phase 78 `/api/search` inherits the contract for free:**
- The same `serialize_search_payload` runs inside the API endpoint after `SearchEngine.execute_search` returns; the response body is byte-identical-shape to the JSON download produced by Plan 04. Modifying `_serialize_item` updates both in lockstep.

**Phase 80 `/api/parallels` inherits via `serialize_parallels_payload`:**
- Plan 02 surfaced `chunk_hits` onto `lab_composition_search` returned items; Plan 03 consumes them; Phase 80 just calls `serialize_parallels_payload(main, filtered, meta_mgr=..., source_text=...)` and returns the envelope.

**Cross-plan invariant established:**
- The 22 contract tests in `tests/test_search_serializer.py` now serve as the regression guard for any future change to the JSON shape. Adding a field, dropping a field, changing the locator triplet, or breaking the EXPORT-03 single-source-of-truth structural property will be caught immediately.

## Self-Check: PASSED

- File `shared/search_serializer.py` created — verified: exists, 556 lines (>=220 plan minimum), `SCHEMA_VERSION = 1` count 1, `def _serialize_item` count 1, shadow `_serialize_search_item`/`_serialize_parallels_item` count 0 (EXPORT-03 structural), `NLI_RESOLVABLE_LIBRARY_CODES` count 3 (definition + use in `_build_image_url` + import-side reference), `fjms.close()` count 0 (HIGH-05), `_filename_counter` + `microsecond` count 4 (HIGH-06), `'domains'` count 1 (plural form), `/api/nli_image_by_sysid` count 3 (1 in URL builder + 2 in docstrings).
- Module imports cleanly: `python -c "from shared.search_serializer import SCHEMA_VERSION, serialize_search_payload, serialize_parallels_payload, build_search_filename, build_parallels_filename; print(SCHEMA_VERSION)"` prints `1`.
- Commit exists: `78edec4b` — verified via `git log --oneline -1`.
- Per-task verify: `pytest tests/test_search_serializer.py -x -v` shows `22 passed in 0.16s`.
- Wave merge verify: `pytest tests/test_search_serializer.py tests/test_export_service.py tests/test_lab_composition_chunk_hits.py -x -q` shows `77 passed in 0.72s`.
- Full baseline: `pytest tests/ -x -q` shows `1189 passed, 8 skipped in 24.61s` — exactly +22 from Plan 02's 1167 baseline (1167 → 1189), 8 skipped unchanged. Zero regression.
- Test file invariant: `grep -c "time\.sleep" tests/test_search_serializer.py` returns 0 (HIGH-06 — no-sleep filename test).

---
*Phase: 77-serializer-json-export*
*Completed: 2026-04-27*
