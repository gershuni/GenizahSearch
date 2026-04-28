---
phase: 77-serializer-json-export
verified_at: 2026-04-28
status: passed
score: 4/4 success criteria met
success_criteria_met: 4/4
re_verification: false
roadmap_truths:
  - "From /search after running any query, a toolbar button downloads the visible result set as a JSON file whose filename contains the page identifier and an ISO timestamp; two consecutive downloads produce two distinct files."
  - "From /parallels after running a composition search, the same export button downloads results in the parallels-shaped payload — never silently overwriting a prior download."
  - "The exported JSON for both pages is produced by exactly one serializer module; modifying the result-item shape in that module changes both downloads (and the API response in later phases) in lockstep — no parallel implementation exists."
  - "Each downloaded payload includes the drill-down locator on every result item (uid preferred, {sys_id, volume_ie, p_num} fallback)."
gaps: []
deferred: []
---

# Phase 77: Serializer & JSON Export — Verification Report

**Phase Goal:** A single serializer module owns the "Claude-friendly JSON" payload shape, and `/search` and `/parallels` pages let users download the current results in that shape — establishing the contract before any HTTP endpoint consumes it.
**Verified:** 2026-04-28
**Status:** ✅ PASS
**Re-verification:** No — initial verification

---

## Goal Achievement Summary

**Phase verdict: PASS (4/4 ROADMAP success criteria met)**

Phase 77 ships a single-source-of-truth serializer module (`shared/search_serializer.py`) consumed by exactly two stateful FastAPI handlers (`/api/export/json`, `/api/export/parallels/json`) wired into toolbar buttons on `/search` and `/parallels`. The locator contract (`{sys_id, volume_ie, p_num}` plus a sibling `uid` string) is unconditionally emitted on every result item, satisfying the Phase 79 round-trip readiness obligation. Two pre-existing UAT gaps (search-side reset state pollution + checkbox-selection-ignored-by-export) were closed under Plan 06 on the final day; OPEN_ISSUES.md L81 (open since 2026-04-17) flipped to ✅ Fixed (2026-04-28). Pytest baseline 1213 passed / 8 skipped (cited from executor + UAT Test 7), up from 1162 at phase start (+51 tests).

---

## ROADMAP Success Criteria — Per-Criterion Audit

### SC-1 — `/search` JSON download: filename has page identifier + ISO timestamp; two consecutive downloads produce distinct files

**Verdict: ✅ MET**

**Codebase artifacts:**
- `shared/search_serializer.py:299` — `build_search_filename()` returns `f"genizah-search-{_filename_timestamp_with_ms()}.json"`.
- `shared/search_serializer.py:278-296` — `_filename_timestamp_with_ms()` builds `'{YYYY-MM-DDTHHMMSS}_{ms}_{counter}'`. Counter is `itertools.count()` at module level (line 72), guaranteeing distinct outputs even on same-millisecond consecutive calls — no `time.sleep` required (HIGH-06 design).
- `shared/search_serializer.py:269-271` — `_utc_iso_now()` provides envelope `generated_at` (separate from filename timestamp).
- `web/pages/search.py:1448-1450` — toolbar button (`icon='data_object'`) wired to `ui.download('/api/export/json')`. Always-enabled per UAT Test 1; tooltip `tr('Export JSON')`.
- `web/api.py:1950-1997` — `export_json` handler imports `serialize_search_payload` + `build_search_filename`, returns `JSONResponse` with `Content-Disposition` filename; 400 on empty `state.last_results`.

**Verification evidence:**
- `tests/test_search_serializer.py:691-702` (`test_filename_uniqueness_consecutive`) — asserts `f1 != f2` from two back-to-back calls **without** sleeping. Exercises the actual contract.
- `tests/test_search_serializer.py:704-712` (`test_filename_format`) — asserts prefix `genizah-search-` + suffix `.json` + regex match `\d{4}-\d{2}-\d{2}T\d{6}`.
- `tests/test_api_export_json.py::test_export_json_handler_populated` (per VALIDATION.md) — full FastAPI TestClient round-trip on bare app (HIGH-08 — does not mutate NiceGUI singleton, also tested at `test_init_api_routes_does_not_mutate_nicegui_singleton`).
- UAT Test 3 (passed): manual `/search` Hebrew query → JSON file `genizah-search-{timestamp}.json`, valid JSON, envelope keys correct.

**Skepticism check:** the "no-sleep distinctness" test is the contract — counter design is what the test exercises. If the counter were ever removed and only seconds-resolution timestamp remained, the test would fail deterministically. The contract is robust.

---

### SC-2 — `/parallels` JSON download: parallels-shaped payload, no overwrite

**Verdict: ✅ MET**

**Codebase artifacts:**
- `shared/search_serializer.py:527-595` — `serialize_parallels_payload(main_results, filtered_results, *, ...)` emits envelope with `source: 'parallels'`, separate top-level `results` and `filtered` arrays (D-11), `schema_version: 1`, plus per-item `matches[]` array (D-13).
- `shared/search_serializer.py:304` — `build_parallels_filename()` returns `f"genizah-parallels-{_filename_timestamp_with_ms()}.json"` (same counter mechanism as search → no overwrite).
- `shared/search_serializer.py:378-406` — `_group_parallels_by_sys_id()` consolidates raw items into one entry per manuscript with `aggregate_score` SUM aggregation.
- `shared/search_serializer.py:409-524` — `_to_parallels_envelope_item()` consumes Plan 02's `chunk_hits` list-of-tuples, builds `matches[]`, applies group-level dedup keyed on `(chunk_index, manuscript_snippet)` (line 485), sorts ascending by `chunk_index` (line 522).
- `web/pages/parallels.py:1236-1238` — toolbar button starts disabled.
- `web/pages/parallels.py:2659, 2667` — disable/enable lifecycle wired into search-execute and reset paths.
- `web/api.py:1999-2054` — `export_parallels_json` handler reads `state.parallels_results` + `state.parallels_filtered` + `state.parallels_search_meta`; returns 400 when both result lists empty.

**Verification evidence:**
- `tests/test_search_serializer.py::TestParallelsEnvelope::test_parallels_envelope_shape` — envelope shape with `source='parallels'`.
- `tests/test_search_serializer.py:446-462` (`test_parallels_filtered_separation`) — `results` and `filtered` are separate top-level arrays.
- `tests/test_search_serializer.py:464-483` (`test_parallels_groups_by_manuscript`) — D-13 grouping verified.
- `tests/test_search_serializer.py:522-573` (`test_parallels_group_dedup_same_chunk_same_snippet_across_uids`) — cross-uid dedup catches NLI multi-Alma-uid cataloging.
- `tests/test_search_serializer.py:575-603` (`test_parallels_matches_sorted_by_chunk_index`) — ascending sort.
- `tests/test_search_serializer.py:605-641` (`test_parallels_chunk_hits_int_falls_back_to_path_b`) — defensive int-counter collision regression (lessons learned from smoke-check fix chain `baf481fb`/`c24fcc48`/`2e2d2b75`/`327aea31`).
- `tests/test_lab_composition_chunk_hits.py::TestChunkHitsBehavior` — Plan 02 behavioral test (HIGH-04) confirming `lab_composition_search` populates `chunk_hits` per uid at runtime via the real loop.
- UAT Test 2 (passed): button lifecycle disabled→enabled→disabled mirrors Excel/Word.
- UAT Test 4 (passed): manual `/parallels` Hebrew query → file `genizah-parallels-{timestamp}.json`, native UTF-8 Hebrew, `matches[]` sorted by `chunk_index`, no duplicate (chunk_index, snippet) pairs, aggregate_score SUM across uids.

**Skepticism check:** the field-name collision between standard-mode `search_composition_logic` (int counter, since 2026-03-12) and lab-mode `lab_composition_search` (Plan 02 list-of-tuples) was a real near-miss caught only during Plan 05 smoke-check. The serializer now has both a defensive `isinstance(chunk_hits, list)` guard (line 499) AND the standard-mode producer was renamed to mirror Plan 02's shape (`genizah_core.py:7670, 7782, 7903-7908`), AND the int counter renamed to `chunk_count` (genizah_core.py:7903 comment), AND tests pin the contract. Multi-layer defense — robust.

---

### SC-3 — Single source of truth: modifying result-item shape changes BOTH downloads in lockstep; no parallel implementation

**Verdict: ✅ MET**

**Codebase artifacts:**
- `shared/search_serializer.py:182-266` — single private `_serialize_item()` helper.
- `shared/search_serializer.py:350` — `serialize_search_payload` calls `_serialize_item` directly.
- `shared/search_serializer.py:467` — `serialize_parallels_payload` calls `_serialize_item` via `_to_parallels_envelope_item` (which wraps a synthetic result dict — see lines 426-465 — and adds only `matches[]` on top, see line 523).
- `web/api.py:1959-1961, 2009-2011` — both JSON handlers import only the public `serialize_*` functions and the `build_*_filename` helpers from `shared.search_serializer`. No serialization logic inlined in `web/api.py`.

**Negative-evidence (proves no parallel impl exists):**
- Repo-wide grep for `_serialize_item` / `serialize_search_payload` / `serialize_parallels_payload` / `build_search_filename` / `build_parallels_filename` returns matches **only** in: `shared/search_serializer.py` (definitions), `web/api.py` (imports + calls), `tests/` (test suite), and `genizah_core.py` (comment references only — no implementation). No copy in `shared/export_service.py`, `web/pages/search.py`, or `web/pages/parallels.py`.

**Verification evidence:**
- `tests/test_search_serializer.py:658-666` (`TestSingleSourceOfTruth::test_serializers_share_serialize_item`) — structural introspection: asserts `_serialize_item` exists in module dir, AND no `_serialize_search_item` / `_serialize_parallels_item` shadows. EXPORT-03 invariant.
- `tests/test_search_serializer.py:668-687` (`test_search_and_parallels_share_item_shape`) — behavioral cross-test: emits payloads through both functions, asserts `s_keys.issubset(p_keys)` and `p_keys - s_keys == {'matches'}`. **This is the strongest possible test of the lockstep guarantee** — adding a key to `_serialize_item` would naturally appear on both sides; removing one would fail both halves of the assertion.

**Skepticism check:** the structural test alone is grep-style; the behavioral cross-test is the contract enforcer. They are complementary, not redundant. Moving `_serialize_item` to a different module would break both. Forking a parallel impl in `web/api.py` would not break the structural test (because the module-level introspection still holds) but WOULD break the behavioral cross-test the moment fields drifted. Two-layer defense.

---

### SC-4 — Drill-down locator on every result item

**Verdict: ✅ MET**

**Codebase artifacts:**
- `shared/search_serializer.py:248-254` — every item dict unconditionally returns `{'uid': ..., 'locator': {'sys_id': ..., 'volume_ie': ..., 'p_num': ...}}`. Both keys always present; field values may be `None` for metadata-only hits.
- `shared/search_serializer.py:203-211` — locator built from `meta_mgr.parse_full_id_components(raw_header)` with try/except → empty-dict fallback (defensive).
- `shared/search_serializer.py:467-472` — parallels items get the same locator via the shared `_serialize_item` (no duplicate locator-construction logic in the parallels path).

**Verification evidence:**
- `tests/test_search_serializer.py:323-333` (`test_locator_always_both_present`) — asserts `'uid' in item AND isinstance(item['uid'], str)` AND `'locator' in item AND isinstance(item['locator'], dict)` for every item. The "always" half of D-04.
- `tests/test_search_serializer.py:335-345` (`test_locator_phase79_shape`) — asserts `set(loc.keys()) == {'sys_id', 'volume_ie', 'p_num'}` exactly. Phase 79 round-trip key contract.
- `tests/test_search_serializer.py:347-358` (`test_metadata_only_hit_shape`) — degenerate case: metadata-only hit has `uid=''`, `sys_id` populated from `display.id` fallback, `volume_ie/p_num=None`. Confirms graceful degradation.
- `tests/test_search_serializer.py:668-687` (cross-test) — locator key set is part of the `s_keys ⊆ p_keys` assertion, so parallels items are also covered.
- UAT Test 3 (passed): manual `/search` JSON inspection confirms `locator={sys_id, volume_ie, p_num}` on every item.
- UAT Test 4 (passed): manual `/parallels` JSON inspection confirms locator presence on every parallels item.

**Skepticism check:** the locator contract is the load-bearing piece for Phase 78/79/80. The "exact key set" test (Phase79 shape) prevents accidental drift — adding e.g. `fl_id` to the locator dict would break this test deliberately. Phase 79 will consume `?uid=…` (preferred) and `?sys_id=…&volume_ie=…&page=…` (fallback) — both forms are now emitted on every item.

---

## Plan 06 Gap-Closure Audit (landed 2026-04-28)

**Gap #1 — `_reset_search` clears global export state on "New Search":**
- ✓ `web/pages/search.py:2038-2047` mirrors 6 envelope-echo fields + `last_selected_uids` to global state singleton (commit `4944880c`).
- ✓ Mirrors precedent at `web/pages/parallels.py:1959-1962`.
- ✓ Regression: `tests/test_export_state_selection.py::test_reset_clears_global_state_then_export_returns_400` (commit `55543316`).
- UAT Test 6 + Test 8: passed (manual smoke 2026-04-28).

**Gap #2 — Export honors row checkbox selection (OPEN_ISSUES.md L81, open since 2026-04-17):**
- ✓ `web/state.py:42` — new `last_selected_uids: Optional[List[str]] = None` field.
- ✓ `web/pages/search_helpers.py:12-35` — new `compute_selected_uids(search_state)` helper (separate module to avoid `search.py`↔`search_results.py` circular-import surface).
- ✓ Mirrored from 3 callsites: `toggle_select_all` (search.py:2069), per-row `toggle_card_selection` (search_results.py:372), `_reset_search` (search.py:2047).
- ✓ All 3 search-side export handlers in `web/api.py` (lines 1827, 1864, 1967) filter `state.last_results` by uid when truthy.
- ✓ Filename gets `-selected-N` suffix when filtered (api.py:1842, 1878, 1986).
- ✓ Empty list `[]` defensively treated as None (handlers fall back to full export).
- ✓ Parallels handlers untouched (no per-row selection UI on /parallels — out of scope per Plan 06).
- ✓ Regression: 8 tests in `tests/test_export_state_selection.py` (3 formats × 3 selection scenarios + filename invariants) + 4 tests in `tests/test_compute_selected_uids.py`.
- ✓ `docs/OPEN_ISSUES.md` line 81 flipped ❌ Open → ✅ Fixed (2026-04-28) in commit `ff620251`.
- UAT Test 9: passed (manual smoke 2026-04-28: 1-of-2 selection on /search produced JSON + xlsx with count=1, only the JTS Ms. 2922 row).

---

## Acknowledged Deviations

**1. Plan 06 Excel-handler comment wording adjusted (commit `d5f603b5`):**

Plan 06 prescribed an Excel-handler comment block that mentioned `last_selected_uids` by name in a docstring; this would have produced 4 grep matches in `web/api.py` and violated the verification gate `grep -c "last_selected_uids" web/api.py = 3` (one per handler at the `_sel = state.last_selected_uids` assignment). The executor reworded the comment so the symbol no longer appears in comments, leaving exactly 3 occurrences. Documented in `77-06-SUMMARY.md` lines 137-153. Functionally identical; only comment phrasing changed. **Accepted — no behavioral impact.**

**2. CONTEXT.md D-01 deviation (`domains: list[str]` plural):**

`shared/search_serializer.py:228-231` emits `domains` as a plural list, not the originally-specified singular `domain` field (CONTEXT.md D-01). This deviation was locked in 77-01-PLAN.md and is positively asserted by `tests/test_search_serializer.py::test_domains_is_a_list` (MED-01). **Accepted — forward-compatible with multi-domain manuscripts.**

**3. CONTEXT.md D-08 deviation (`image_url` semantics):**

CONTEXT.md D-08 originally treated `display['img']` as a URL; investigation showed it is a page number. `shared/search_serializer.py:96-126` (`_build_image_url`) now emits server-relative `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` for NLI-resolvable providers OR `null` for non-NLI providers (HIGH-07 — `NLI_RESOLVABLE_LIBRARY_CODES` whitelist at line 61-63). Documented in module docstring lines 17-21. **Accepted — Phase 79 `/api/browse` will own image canonicalization.**

---

## Anti-Pattern Scan

No blockers found. Notable patterns:

- ✓ **No silent except handlers introduced** — `web/api.py:1996, 2053` use `logger.exception(...)` (after the smoke-check fix `baf481fb` upgraded `logger.error` → `logger.exception` to surface stack traces).
- ✓ **Defensive isinstance guards** — `_to_parallels_envelope_item` line 499 guards against non-list `chunk_hits` (the int-counter collision described in OPEN_ISSUES.md L80).
- ✓ **Memory note documented** — module docstring lines 34-37 explains the parallels chunk_hit memory characteristic (bounded by already-retained `content`).
- ✓ **HIGH-05 lifecycle compliance** — `_safe_fjms_lookups` (lines 140-179) explicitly documents NOT calling `.close()` on the FJMS service singleton (line 175 comment + module-level singleton reference at `shared/fjms_service.py:3160-3169`).
- ⚠ **Pre-existing P2 issues NOT closed by this phase** — desktop search latency (OPEN_ISSUES.md L87, L88) and FjmsService row_factory race (L89). All triaged for future attention; out of Phase 77 scope.

---

## Behavioral Spot-Checks

- ✓ pytest baseline: 1213 passed / 8 skipped (cited from UAT Test 7 + executor confirmation 2026-04-28; +51 tests vs phase-start 1162). Not re-run by verifier per user directive.
- ✓ `scripts/check_docs.py`: green (cited from 77-06-SUMMARY.md line 122 — "All checks passed! Documentation is healthy.").
- ✓ Manual smoke (UAT 9/9 passed): /search and /parallels JSON downloads, button lifecycle, Hebrew UTF-8 native rendering, empty-state guards, New-Search reset, checkbox selection.

---

## Carry-Over to Next Phase (Phase 78)

1. **Locator contract is now consumed by 3 export handlers** (`/api/export/json`, `/api/export/excel`, `/api/export/word` all read `state.last_results` which contains items shaped via the same execute path — though only the JSON handler currently routes through `_serialize_item`). Any drift in `_serialize_item` will hit `/api/search` once Phase 78 ships, and the `test_search_and_parallels_share_item_shape` cross-test will catch it.

2. **`AppState.last_selected_uids` field** is search-only (parallels has no per-row selection UI). Phase 78's stateless `/api/search` POST endpoint will not need to honor it — but Phase 78 should be aware the global singleton has selection-aware export semantics on the GET-download path.

3. **`chunk_hits` field-name discipline** — both producers (`search_composition_logic` standard-mode + `lab_composition_search` lab-mode in `genizah_core.py`) now write list-of-tuples shape, and the int counter was renamed to `chunk_count`. Phase 80's `/api/parallels` will consume the same shape; do not reintroduce a counter named `chunk_hits` anywhere.

4. **HIGH-08 init pattern** — `init_api_routes(app_override=...)` (web/api.py:174-186) supports a bare-app override for tests. Phase 78 hardening shell (rate-limit middleware, error envelope, mode-flag) should hook into the same pattern so the hardened endpoints can be tested without polluting the NiceGUI singleton.

5. **`SCHEMA_VERSION = 1` constant** — exported at `shared/search_serializer.py:52`. Phase 78/80 inherit unchanged. Bump only on incompatible envelope/item-shape change.

6. **Filename `-selected-N` suffix convention** — established for partial exports (web/api.py:1845, 1880, 1988). Phase 78's stateless POST endpoint does not need this (no in-session selection state to honor), but should stay consistent if any future stateful export path is added.

---

## Verification Sign-Off

- All 4 ROADMAP success criteria: ✅ MET
- All 9 UAT tests: passed
- 2 pre-existing gaps closed by Plan 06 (gap-closure same phase)
- 0 new gaps surfaced by verifier
- 0 blockers, 0 warnings beyond the 3 acknowledged deviations (all documented)

**Final phase verdict: ✅ PASS — proceed to Phase 78.**

---

_Verified: 2026-04-28_
_Verifier: Claude (gsd-verifier)_
