---
phase: 77
slug: serializer-json-export
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-27
---

# Phase 77 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `.planning/phases/77-serializer-json-export/77-RESEARCH.md` § Validation Architecture.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (pinned via `requirements-lock.txt`) |
| **Config file** | none — pytest defaults; `tests/conftest.py` adds repo root to `sys.path` |
| **Quick run command** | `pytest tests/test_search_serializer.py -x -q` |
| **Full suite command** | `pytest tests/` |
| **Estimated runtime** | quick ~3 sec; full ~3–4 min |

---

## Sampling Rate

- **After every task commit:** `pytest tests/test_search_serializer.py -x -q`
- **After every plan wave:** `pytest tests/test_search_serializer.py tests/test_export_service.py -x` (verifies adjacent export module did not regress)
- **Before `/gsd-verify-work`:** Full `pytest tests/` must be green
- **Max feedback latency:** ~3 seconds for the per-task sampler

---

## Per-Task Verification Map

> The planner fills in concrete `Task ID` / `Plan` / `Wave` columns. The Requirement → Test → Command rows below are the contract every plan must satisfy.

| Requirement / Decision | Behavior | Test Type | Automated Command | File Exists | Status |
|------------------------|----------|-----------|-------------------|-------------|--------|
| EXPORT-01 | `serialize_search_payload` returns well-formed envelope | unit | `pytest tests/test_search_serializer.py::test_search_envelope_shape -x` | ❌ Wave 0 | ⬜ pending |
| EXPORT-01 | `/api/export/json` returns 400 when `state.last_results` empty | behavior (FastAPI TestClient) | `pytest tests/test_api_export_json.py::test_export_json_handler_empty -x` | ❌ Plan 04 | ⬜ pending |
| EXPORT-01 | Filename includes ISO timestamp + `genizah-search-` prefix | unit (filename helper) | `pytest tests/test_search_serializer.py::test_filename_format -x` | ❌ Wave 0 | ⬜ pending |
| EXPORT-02 | `serialize_parallels_payload` returns well-formed envelope | unit | `pytest tests/test_search_serializer.py::test_parallels_envelope_shape -x` | ❌ Wave 0 | ⬜ pending |
| EXPORT-02 | `/api/export/parallels/json` returns 400 when `state.parallels_results` AND `state.parallels_filtered` both empty | behavior (FastAPI TestClient) | `pytest tests/test_api_export_json.py::test_export_parallels_json_handler_empty -x` | ❌ Plan 04 | ⬜ pending |
| EXPORT-01/02 | Both new JSON handlers return 200 + Content-Disposition + valid JSON body when state is populated | behavior (FastAPI TestClient) | `pytest tests/test_api_export_json.py::test_export_json_handler_populated tests/test_api_export_json.py::test_export_parallels_json_handler_populated -x` | ❌ Plan 04 | ⬜ pending |
| EXPORT-02 | `results` and `filtered` are separate top-level arrays (D-11) | unit | `pytest tests/test_search_serializer.py::test_parallels_filtered_separation -x` | ❌ Wave 0 | ⬜ pending |
| EXPORT-02 | One result per manuscript with `matches[]` array (D-13) | unit | `pytest tests/test_search_serializer.py::test_parallels_groups_by_manuscript -x` | ❌ Wave 0 | ⬜ pending |
| EXPORT-03 | Both serialize functions reach into the same `_serialize_item` helper | unit (structural introspection) | `pytest tests/test_search_serializer.py::test_serializers_share_serialize_item -x` | ❌ Wave 0 | ⬜ pending |
| EXPORT-03 | Adding a key to `_serialize_item` shows up in BOTH search and parallels output | unit (behavioral cross-test) | `pytest tests/test_search_serializer.py::test_search_and_parallels_share_item_shape -x` | ❌ Wave 0 | ⬜ pending |
| EXPORT-04 | Two consecutive serialize calls produce two distinct filenames | unit | `pytest tests/test_search_serializer.py::test_filename_uniqueness_consecutive -x` | ❌ Wave 0 | ⬜ pending |
| D-04 | Every result has BOTH `uid` (string, may be `""`) AND `locator` dict | unit | `pytest tests/test_search_serializer.py::test_locator_always_both_present -x` | ❌ Wave 0 | ⬜ pending |
| D-04 | `volume_ie` / `p_num` are `null` for metadata-only hits | unit | `pytest tests/test_search_serializer.py::test_metadata_only_hit_shape -x` | ❌ Wave 0 | ⬜ pending |
| D-03 | `snippet` stripped of `*term*`; `match_terms` populated from removed markers | unit | `pytest tests/test_search_serializer.py::test_snippet_stripped_match_terms_extracted -x` | ❌ Wave 0 | ⬜ pending |
| D-05 | Empty results envelope is well-formed (`count=0`, `results=[]`, `warnings=[]`) | unit | `pytest tests/test_search_serializer.py::test_empty_results_envelope -x` | ❌ Wave 0 | ⬜ pending |
| D-07 | `warnings: []` is always present (never absent) | unit | `pytest tests/test_search_serializer.py::test_warnings_always_present -x` | ❌ Wave 0 | ⬜ pending |
| D-09 | `source: 'search'` for search payloads, `source: 'parallels'` for parallels | unit | `pytest tests/test_search_serializer.py::test_source_field_tags -x` | ❌ Wave 0 | ⬜ pending |
| D-10 | `schema_version: 1` is a top-level constant exported by the module | unit | `pytest tests/test_search_serializer.py::test_schema_version_constant -x` | ❌ Wave 0 | ⬜ pending |
| Locator round-trip readiness (Phase 79) | `locator` dict keys are exactly `{sys_id, volume_ie, p_num}` | unit | `pytest tests/test_search_serializer.py::test_locator_phase79_shape -x` | ❌ Wave 0 | ⬜ pending |
| Regression baseline | Full suite stays green | regression | `pytest tests/` | ✓ existing | ⬜ pending |

*Status legend: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_search_serializer.py` — covers EXPORT-01..04 + D-03/04/05/07/09/10/11/13 + locator round-trip
- [ ] Fixture: synthetic search-result dict (with `display`, `snippet`, `full_text`, `uid`, `raw_header`, `sort_score`)
- [ ] Fixture: synthetic parallels-result list spanning multiple `uid`s on one `sys_id` (proves D-13 grouping)
- [ ] Fixture: synthetic parallels result with empty `chunk_hits` (degenerate Path B fallback, if planner picks Path B/C)
- [ ] Mock for `MetadataManager.parse_full_id_components` returning known `{sys_id, ie_id, p_num, fl_id}` (modeled after `tests/test_export_service.py:240-245`)
- [ ] Mock for `FjmsService.get_domains_for_sys_ids` and `FjmsService.get_catalog`
- [ ] No new framework install required — pytest is already installed.

*If the planner picks Path A for D-13 (extend core to track per-chunk attribution), Wave 0 also requires:*
- [ ] One additive unit test asserting `lab_composition_search` populates `chunk_hits` per `uid` after the core extension.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Toolbar JSON button visible on `/search` and `/parallels`, disabled before any results, enabled after | EXPORT-01, EXPORT-02 | NiceGUI toolbar render is a live DOM concern — automated browser test is out of scope per CONTEXT.md "no automated browser test required" | Run `python -m web.main`; navigate to `/search`, confirm button is disabled; run any query; confirm button becomes enabled. Repeat on `/parallels`. |
| Downloaded JSON opens in a text editor with UTF-8 Hebrew rendered as native characters (not `\uXXXX`) | D-08 (Hebrew/RTL) | Encoding behavior depends on the running FastAPI/Starlette stack and is verified end-to-end | After running the server, query `אגדה`, click the JSON button, open the downloaded file in a UTF-8-aware editor, confirm Hebrew is readable. |
| Two clicks within ~10 seconds yield two distinct filenames in the browser download bar | EXPORT-04 | Browser download dedup behavior varies; spot-check confirms the second-resolution timestamp scheme works in practice | Click the JSON button twice in succession on the same query and confirm both files land with distinct names. |
| `/api/export/excel` and `/api/export/word` continue to function unchanged after the refactor | regression | Hand-verified spot-check confirms no regression in the adjacent Excel/Word path | Run the existing Excel and Word downloads from `/search` and `/parallels`. Open both files. Confirm content matches pre-Phase-77 behavior. |

---

## Validation Sign-Off

- [ ] All tasks have an `<automated>` verify command OR a Wave 0 dependency
- [ ] Sampling continuity: no 3 consecutive tasks without automated verification
- [ ] Wave 0 covers every ❌ row above before its consuming task runs
- [ ] No watch-mode flags (pytest runs one-shot)
- [ ] Feedback latency < 5 seconds for the quick sampler
- [ ] `nyquist_compliant: true` set in frontmatter once planner closes the per-task map

**Approval:** pending
