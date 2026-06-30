---
phase: 125-core-engines
verified: 2026-06-26T13:05:00Z
status: passed
score: 5/5 success criteria verified (8/8 requirements satisfied)
overrides_applied: 0
re_verification:
  previous_status: none
  previous_score: n/a
  note: initial verification (no prior VERIFICATION.md)
---

# Phase 125: Core Engines — Verification Report

**Phase Goal:** Pure internal refactor, zero behavior change. SEED-011 composition double-prep
dedup lands first (125a). Then `SearchEngine`, `LabSettings`, `LabEngine` are extracted to
`shared/` with the 3 CORE-10 hazards preserved (BrowseMap class-cache, SEED-006 `content_search`
gates, `_LAST_RESPONSA_DOWNGRADE` thread-local), the LOCAL-LAB mirror (CR-01/CR-02,
`_lab_weights_hash_override`) intact, `_my_library_tab_ref` modeled as an injected optional
local-search-gate on both engines, and `genizah_core.py` left as a permanent same-object
re-export facade. GUARD-01..04 held.

**Verified:** 2026-06-26
**Status:** PASSED
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | SEED-011 dedup landed before engine moves; `corpus_scope='all'` composition no longer double-builds the query set; relevant composition tests pass | ✓ VERIFIED | Commits `42ed2477`/`d003a533`/`8b35b1a2` precede `478959ec`/`0fc24dc7`/`4902a8b7`. `_ChunkPlan` pre-pass at `search_engine.py:2878`; `_LabChunkPlan` pre-pass at `lab_engine.py:972`. `test_seed011_composition_dedup.py` (incl. `test_search_composition_logic_shared_prep_once`, `test_lab_composition_search_no_double_prep`, `test_scoped_run_skips_opposite_flavor_build`, `test_lab_prepass_skipped_when_no_index`) all GREEN; `test_comp_corpus_scope.py` 25/25 GREEN |
| 2 | `from shared.{search_engine,lab_engine,lab_settings} import {SearchEngine,LabEngine,LabSettings}` resolve; `from genizah_core import …` resolve via shims; both paths yield the same class objects | ✓ VERIFIED | Runtime check: standalone imports OK + facade imports OK; `genizah_core.X is shared.Y.X == True` for all three. Full 20-name SearchEngine facade also 20/20 same-object |
| 3 | Full search test suite passes (incl. `test_corpus_scope_routing`, `test_cross_side_contract`, `test_comp_corpus_scope`, `test_lab_composition_chunk_hits`, `test_local_lab_invalidation`); no behavior change in any search mode | ✓ VERIFIED | Bulk run `-m "not gui and not render_smoke"`: **6 failed, 4853 passed** — the 6 are exactly the documented pre-existing baseline (`test_search_api_v2::test_search_mode_real_index_returns_at_least_one_result[*]`, `state.searcher is None` env failure, red at base `3050eb2a`). Zero net-new failures. Named files all GREEN |
| 4 | BrowseMap class-level cache, SEED-006 `content_search` compat gates, and `_LAST_RESPONSA_DOWNGRADE` thread-local all work identically after the move | ✓ VERIFIED | Hazard A: `_shared_browse_map`/`_browse_map_lock` class attrs at `search_engine.py:1117-1118`, used at 1126-1151. Hazard B: `_index_has_field` (308), `content_search_staleness_messages` (332), `_has_content_search`/`_local_has_content_search` instance attrs (415/425/513). Hazard C: `_LAST_RESPONSA_DOWNGRADE = threading.local()` (72) + all 6 channel names moved together and shimmed individually. Responsa retarget tests GREEN |
| 5 | `LabEngine.lab_composition_search()` + `SearchEngine.attach_my_library_tab()` accept the injected optional local-search-gate; no `shared/` imports `desktop/`/`genizah_app`; GUARD-01 back-edge test green | ✓ VERIFIED | `attach_my_library_tab` at `search_engine.py:441` (weakref); duck-typed `is_searchable` gate via `getattr` on both engines (`search_engine.py:788-792`, `lab_engine.py:818-819/1149-1150`). Zero `import desktop`/`genizah_app` in any shared engine module. `test_no_back_edges_core.py`: 41 passed |

**Score:** 5/5 success criteria verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/search_engine.py` | SearchEngine + pre-cluster, extracted intact | ✓ VERIFIED | 3,961 lines; `class _ChunkPlan` (152), `class _LabChunkPlan` (176), `class SearchEngine`; no BOM, valid UTF-8, ruff clean |
| `shared/lab_engine.py` | LabEngine extracted | ✓ VERIFIED | 1,525 lines; LabEngine + LOCAL-LAB mirror (CR-02); no BOM, valid UTF-8, ruff clean |
| `shared/lab_settings.py` | LabSettings (stdlib-only) | ✓ VERIFIED | 156 lines; stdlib + `shared.config` only; no BOM, valid UTF-8, ruff clean |
| `genizah_core.py` | Permanent same-object facade, no engine class defs | ✓ VERIFIED | 755 lines; zero `class {SearchEngine,LabEngine,LabSettings}` defs; facade shims at L396/L400/L609 |
| `tests/test_seed011_composition_dedup.py` | NEW invocation-count guard | ✓ VERIFIED | 380 lines, substantive assertions (call_count == n_chunks; scoped-run skips opposite flavor; no-index skips pre-pass) |

### Key Link Verification (SEED-011 gating fix — Codex Gate-2 BLOCKER/HIGH)

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| build gate `_do_genizah_pp` (`corpus_scope != 'local'`) | Genizah consume loop | `search_engine.py:2871` → `:2915` | ✓ WIRED | Same predicate; loop gated on `corpus_scope != 'local'` |
| build gate `_do_local_pp` (snapshot) | LOCAL consume loop | `search_engine.py:2874` → `:3060` | ✓ WIRED | Consume loop gated on the SAME `_do_local_pp` boolean snapshot (round-2 HIGH fix — build/consume can't disagree) |
| build gate `_do_genizah_lab_pp`/`_do_local_lab_pp` | Genizah-LAB consume loop | `lab_engine.py:966-971` → `:1019-1020` | ✓ WIRED | Loop requires `(_do_genizah_lab_pp or _do_local_lab_pp)` — same snapshot that populated `lab_chunk_plans` |
| build gate `_do_*_lab_pp` | LOCAL-LAB consume loop | `lab_engine.py:966-971` → `:1188-1191` | ✓ WIRED | Loop requires `(_do_genizah_lab_pp or _do_local_lab_pp)` — same snapshot |

### GUARD-01 Lazy Imports (no module-level back-edge)

| Module | Module-level `genizah_core` import | Lazy (function-body) imports |
|--------|-----------------------------------|------------------------------|
| `shared/search_engine.py` | NONE (✓) | 7, all `# noqa: PLC0415` (CURRENT_LANG, text_to_fingerprint×2, boundary helpers, scoring helpers, get_volume_pages×2); + lazy `from shared.lab_engine import LabEngine` at 758 |
| `shared/lab_engine.py` | NONE (✓) | 8 lazy, all `# noqa: PLC0415`; + lazy `from shared.search_engine import _LabChunkPlan` at 884 |
| `shared/lab_settings.py` | NONE (✓) | stdlib + `shared.config` only |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| 20-name SearchEngine facade same-object | runtime identity loop | 20/20 | ✓ PASS |
| LabEngine/LabSettings same-object | `gc.X is shared.Y.X` | True/True | ✓ PASS |
| Standalone + facade imports resolve (SC#2) | import both paths | both OK, all same objects | ✓ PASS |
| GUARD-01 back-edge AST scan | `pytest test_no_back_edges_core.py` | 41 passed | ✓ PASS |
| Targeted composition/dedup/local-lab/responsa | `pytest` 7 files | 138 passed | ✓ PASS |
| SEED-011 guard + responsa + cross-side + tokenizer | `pytest` 7 files | 147 passed | ✓ PASS |
| Full bulk suite (GUARD-02) | `pytest -m "not gui and not render_smoke"` | 6 failed (baseline), 4853 passed | ✓ PASS |

### Requirements Coverage

| Requirement | Description | Status | Evidence |
|-------------|-------------|--------|----------|
| PREP-01 | SEED-011 dedup lands before engine moves | ✓ SATISFIED | Commit ordering + truth #1 |
| CORE-10 | SearchEngine extracted intact w/ DI + 3 hazards preserved | ✓ SATISFIED | Truth #4, artifact, hazard greps |
| CORE-11 | LabSettings → shared/lab_settings.py | ✓ SATISFIED | Same-object identity True |
| CORE-12 | LabEngine → shared/lab_engine.py; LOCAL-LAB mirror (CR-01/CR-02, `_lab_weights_hash_override`) preserved | ✓ SATISFIED | Same-object; CR-01/CR-02 + override greps |
| CORE-13 | `_my_library_tab_ref` injected gate on BOTH engines; no shared→desktop import | ✓ SATISFIED | Truth #5, duck-typed gate greps, zero desktop imports |
| GUARD-02 | Zero behavior change, full suite passes | ✓ SATISFIED | Bulk run = documented baseline, 0 net-new |
| GUARD-03 | Source-scan tests retargeted to new module homes | ✓ SATISFIED | Retargets in test_local_lab_invalidation/test_audit_2026_06_23_guards/test_phase_97_invariants read shared/*.py; all GREEN |
| GUARD-04 | genizah_core permanent facade for all moved names | ✓ SATISFIED | 20/20 + LabEngine/LabSettings same-object |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `tests/test_no_back_edges_core.py` | 44-46 | Stale comment "not yet created — skip-until-exists" on the 3 Phase-125 modules, which now exist | ℹ️ Info | Cosmetic only — modules exist, back-edge scan actively enforces them (test green). No functional impact |
| `tests/test_phase_97_invariants.py` | 242, 267 | Assertion message text says "genizah_core.py" but the test reads `shared/search_engine.py` (L217) | ℹ️ Info | Cosmetic stale string in an error message; scan target is correctly retargeted; test passes |

No 🛑 blocker or ⚠️ warning anti-patterns. No `TBD`/`FIXME`/`XXX` debt markers introduced. No stubs (extraction preserves all method bodies verbatim).

### Human Verification Required

None. This is a pure internal refactor with full automated coverage (identity, back-edge AST,
behavior-equivalence suite). No visual/UX/real-time/external-service behavior was changed.

### Gaps Summary

No gaps. All 5 ROADMAP success criteria and all 8 requirements are independently verified against
the live codebase:
- SEED-011 dedup landed first; the Codex Gate-2 BLOCKER/HIGH gating fix is correctly wired in all
  4 build→consume sites (build and consume share the same predicate snapshot, so they cannot
  disagree — verified by reading `search_engine.py:2871/2874→2915/3060` and
  `lab_engine.py:966-971→1019/1188`, and by the `test_scoped_run_skips_opposite_flavor_build` /
  `test_lab_prepass_skipped_when_no_index` guard tests).
- All three engine classes extracted to `shared/`; `genizah_core.py` (755 ln) is a permanent
  same-object re-export facade (20/20 SearchEngine names + LabEngine + LabSettings).
- 3 CORE-10 hazards present and intact; CR-01/CR-02 LOCAL-LAB mirror intact; CORE-13 duck-typed
  `is_searchable` gate on both engines with zero shared→desktop imports.
- GUARD-01 (no module-level back-edges; 41-test AST scan green), GUARD-02 (bulk suite = documented
  6-failure baseline, 0 net-new), GUARD-03 (source-scan retargets), GUARD-04 (facade) all held.
- BOM/UTF-8 clean, `git diff --check` clean, ruff clean on all four files.

Two cosmetic stale-string nits noted (info-level) do not affect behavior or test outcomes.

---

_Verified: 2026-06-26T13:05:00Z_
_Verifier: Claude (gsd-verifier)_
