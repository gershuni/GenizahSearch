---
phase: 118-joins-entry-full-builders
plan: "01"
subsystem: web/joins-lab
tags: [tdd, testing, wave-0, joins-lab, anc-04, anc-05, bld-02, bld-03, bld-04]
dependency_graph:
  requires: []
  provides:
    - tests/test_merge_globals_web.py (BLD-04 RED stubs)
    - tests/test_other_side_page_contract.py (BLD-02 GREEN integration test + resolve_other_side_pages contract)
    - tests/test_builder_modifier_hoist.py (BLD-03 RED + compose line_start/end/gap GREEN)
    - tests/test_known_joins_group.py (ANC-04 source attribution GREEN + community RED)
    - tests/test_joins_anc05_rls.py (ANC-05 confirmed-only RED stubs)
    - .planning/phases/118-joins-entry-full-builders/118-fragment-joins-schema-probe.md (schema confirmation + Plan 02 directive)
  affects:
    - plans/118-02 (confirmed by schema probe; status='confirmed' is primary ANC-05 fix)
    - plans/118-03 (BLD-03 test stubs are RED until Plan 03 adds _apply_modifiers_to_term)
    - plans/118-04 (BLD-04 test stubs are RED until Plan 04 adds _merge_globals_web)
tech_stack:
  added: []
  patterns:
    - Deferred-import RED test pattern (import inside test body, not at module top)
    - Fake-executor integration test for apply_cross_side
    - monkeypatch I/O isolation for fetch_connected_fragments
key_files:
  created:
    - tests/test_merge_globals_web.py
    - tests/test_other_side_page_contract.py
    - tests/test_builder_modifier_hoist.py
    - tests/test_known_joins_group.py
    - tests/test_joins_anc05_rls.py
    - .planning/phases/118-joins-entry-full-builders/118-fragment-joins-schema-probe.md
  modified: []
decisions:
  - "Wave-0 test pattern: import RED symbols inside test body (not at module top) so pytest collection never fails — errors are INSIDE test bodies (TypeError/ModuleNotFoundError), not collection errors"
  - "apply_cross_side integration test (test_cross_side_uses_p_num_and_handles_metadata_only) is GREEN now — exercises existing shared core; Plan 04 wires the web caller to feed p_num/volume_ie/total_pages=0->None into this path"
  - "schema probe uses static fallback (no SUPABASE env vars present); two independent live-code sources (supabase_setup.sql:162 + web/supabase_client.py:1593-1594) confirm status column exists — treated as authoritative"
  - "PRIMARY ANC-05 fix confirmed: get_fragment_joins(status='confirmed') + ':confirmed' cache key; RLS USING(true) means app-layer filter is the sole D-17 mechanism"
metrics:
  duration: "~35min"
  completed: "2026-06-18"
  tasks: 3
  files: 6
---

# Phase 118 Plan 01: Wave-0 RED Test Scaffolding Summary

**One-liner:** Five RED test stubs (BLD-02/03/04 + ANC-04/05) plus schema probe confirming `status='confirmed'` as the primary ANC-05 mechanism.

---

## What Was Built

### Task 1: BLD-02/03/04 Pure-Logic RED Test Files (commit `34e9bbdd`)

Three new test files scaffolded as Wave-0 RED stubs:

**`tests/test_merge_globals_web.py`** (BLD-04 — _merge_globals_web re-injection):
- 5 tests asserting that `_merge_globals_web(ro, global_opts)` correctly injects `flex_spacing` + `bidirectional` into a `compose()`-produced `ro` dict
- `ja` stays `False` per D-10 (user decision)
- `variants` flows via `SideQuery.variants` and is not clobbered
- Helper is side-agnostic: called on both anchor and other-side ro
- All 5 RED until Plan 04 adds `_merge_globals_web` to `web.pages.joins_lab`

**`tests/test_other_side_page_contract.py`** (BLD-02 — web page contract):
- 6 pure `resolve_other_side_pages` tests (all GREEN NOW — exercises existing shared core)
- `test_cross_side_uses_p_num_and_handles_metadata_only`: fake-executor integration test against `apply_cross_side` (GREEN NOW — proves p_num not internal_index, total_pages=0 graceful degradation, volume_ie forwarded to executor)
- `test_cross_side_volume_ie_forwarded`: confirms executor API accepts `volume_ie` kwarg
- All 8 tests PASS immediately

**`tests/test_builder_modifier_hoist.py`** (BLD-03 — modifier hoist + compose):
- Part 1 (GREEN NOW): 4 `compose()` line_start/line_end/gap tests against existing shared core
- Part 2 (RED until Plan 03): 7 `_apply_modifiers_to_term` tests (negation/plene/prefix/suffix/wildcard; slash-group grouping; wildcard_prefix not on slash-groups per RR-13)

### Task 2: ANC-04/ANC-05 Data-Layer RED Test Files (commit `6c74fde1`)

**`tests/test_known_joins_group.py`** (ANC-04):
- `test_user_join_has_user_source`: GREEN — user join rows produce `sources=['user']`
- `test_fragment_details_populated`: GREEN — `fragment_details` list has `shelfmark`+`document_id` entries
- `test_empty_returns_zero_joins`: GREEN — no-join scenario produces `total_joins=0`, `joins=[]`
- `test_multi_source_dedup_merges_sources`: GREEN — FJMS merge branch; same fragment in user+FJMS joins ends with `sources=['user','FJMS']`
- `test_community_member_appears_in_lab_group`: RED — `confirmed_only=True` kwarg not yet in `fetch_connected_fragments`; Plan 02 adds community merge on the Lab path

**`tests/test_joins_anc05_rls.py`** (ANC-05 / T-118-01):
- `test_confirmed_only_uses_separate_cache_key`: RED — `':confirmed'` cache key not yet written
- `test_default_call_uses_unconfirmed_key`: GREEN — unconfirmed key test passes (no new kwarg needed)
- `test_confirmed_path_passes_status_confirmed_to_get_fragment_joins`: RED — `confirmed_only` kwarg not yet in `fetch_connected_fragments`
- `test_default_path_passes_no_status_filter`: GREEN — default call never passes status kwarg
- `test_no_cross_user_poisoning`: RED — no cache-key isolation yet

Module docstring records the complete ANC-05 directive: `status='confirmed'` primary, `:confirmed` cache key, conditional fallback only if live probe proves column absent.

### Task 3: Schema Probe Note (commit `9f8f6599`)

**`.planning/phases/118-joins-entry-full-builders/118-fragment-joins-schema-probe.md`**:
- Method: static fallback (no SUPABASE env vars present)
- Confirmation: `fragment_joins.status` EXISTS — `supabase_setup.sql:162` defines `TEXT DEFAULT 'proposed' CHECK (status IN ('proposed','confirmed','rejected'))`; `web/supabase_client.py:1593-1594` already applies `.eq('status', status)` when truthy
- Note on stale SUPABASE_GUIDE.md: status column omitted from guide's schema diagram; canonical SQL is authoritative
- RLS: `USING(true)` — all rows publicly readable; app-layer filter is the sole D-17 mechanism
- Unambiguous directive: PRIMARY = `get_fragment_joins(status='confirmed')` + `':confirmed'` cache key; CONDITIONAL fallback only if column absent in live deployment

---

## Test Status Summary

| File | GREEN | RED | Collection errors |
|------|-------|-----|-------------------|
| test_other_side_page_contract.py | 8 | 0 | 0 |
| test_known_joins_group.py | 4 | 1 | 0 |
| test_joins_anc05_rls.py | 2 | 3 | 0 |
| test_builder_modifier_hoist.py | 4 | 7 | 0 |
| test_merge_globals_web.py | 0 | 5 | 0 |
| **Total** | **18** | **16** | **0** |

All RED tests fail inside test bodies (TypeError/ModuleNotFoundError on missing Phase-118 symbols), never at collection. No existing guards were broken.

---

## Existing Guards

- `tests/test_joins_lab_off_loop.py` — 7 passed
- `tests/test_no_raw_storage_access.py` — 6 passed

---

## Deviations from Plan

None — plan executed exactly as written.

The schema probe used the static fallback path (as specified in Task 3's fallback procedure) because no Supabase credentials were present in the execution environment. Both evidence sources clearly confirm the `status` column exists; the static fallback is treated as authoritative.

---

## Threat Surface Scan

No production code was modified in this plan. All new files are test scaffolds + one `_tmp/` note. No new network endpoints, auth paths, or schema changes introduced.

## Self-Check: PASSED

All created files verified to exist:
- tests/test_merge_globals_web.py — FOUND
- tests/test_other_side_page_contract.py — FOUND
- tests/test_builder_modifier_hoist.py — FOUND
- tests/test_known_joins_group.py — FOUND
- tests/test_joins_anc05_rls.py — FOUND
- .planning/phases/118-joins-entry-full-builders/118-fragment-joins-schema-probe.md — FOUND
- .planning/phases/118-joins-entry-full-builders/118-01-SUMMARY.md — FOUND

Commits verified:
- 34e9bbdd: test(118-01): scaffold BLD-02/03/04 RED test stubs (Wave 0)
- 6c74fde1: test(118-01): scaffold ANC-04/ANC-05 RED test stubs (Wave 0)
- 9f8f6599: docs(118-01): record fragment_joins.status schema probe (Wave 0)
