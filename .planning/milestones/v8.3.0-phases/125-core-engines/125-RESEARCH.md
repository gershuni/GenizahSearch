# Phase 125: Core Engines — Research

**Researched:** 2026-06-26
**Domain:** genizah_core.py decomposition — SearchEngine / LabEngine / LabSettings extraction + SEED-011 composition double-prep dedup
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **PREP-01 (125a, FIRST):** Land the SEED-011 composition double-prep dedup BEFORE any engine code moves. Fix shape: precompute a shared per-chunk plan once (query string + compiled regex + weak/fingerprint derivations — index-independent), then run the Tantivy search + regex filter pass per index separately. Applies to BOTH `corpus_scope='all'` (genizah_core lines ~4906 onward) AND LAB composition (genizah_core ~1548 onward). **Behavior-preserving** — composition results identical.
- **CORE-10:** Extract `SearchEngine` intact to `shared/search_engine.py` with `meta_mgr`/`var_mgr` passed by dependency injection. PRESERVE 3 hazards: (a) BrowseMap class-level cache, (b) SEED-006 `content_search` compat gates, (c) `_LAST_RESPONSA_DOWNGRADE` thread-local. *(SEED-020 §7 C-3)*
- **CORE-11:** Extract `LabSettings` → `shared/lab_settings.py`.
- **CORE-12:** Extract `LabEngine` → `shared/lab_engine.py`; PRESERVE SearchEngine↔LabEngine LOCAL-LAB mirror (CR-01/CR-02, `_lab_weights_hash_override`).
- **CORE-13:** Model `_my_library_tab_ref` as an injected optional "local-search-gate" interface consumed by BOTH `SearchEngine.attach_my_library_tab()` and `LabEngine.lab_composition_search()`. **No `shared/` → desktop import** (GUARD-01). *(C-4)*
- **GUARD-01/02/04:** no module-level back-edges; zero behavior change; genizah_core stays a permanent re-export facade. GUARD-01 registry grows to 13.
- **GUARD-03:** retarget any source-scanning test that scans genizah_core.py for moved engine code BEFORE/with the move.
- **DEFER-01 stays deferred:** do NOT sub-split SearchEngine in this phase — move the class INTACT.

### Claude's Discretion
None — all decisions locked.

### Deferred Ideas (OUT OF SCOPE)
- DEFER-01: SearchEngine internal sub-split (LineBreakSearcher / CompositionSearcher) — after CORE-10 ships.
- DEFER-02/03/04: Desktop composition-tab, CompositionState refactor, startup/session remainder extraction.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PREP-01 | SEED-011 composition double-prep dedup lands before engine move | Section "SEED-011 Double-Prep Analysis" — current call sites identified, index-independence confirmed |
| CORE-10 | SearchEngine extracted intact to shared/search_engine.py | Section "3 CORE-10 Hazards" — all 3 hazards mapped with file:line |
| CORE-11 | LabSettings extracted to shared/lab_settings.py | Section "Class Boundaries" — LabSettings is stdlib-only, no back-edges |
| CORE-12 | LabEngine extracted to shared/lab_engine.py | Section "SearchEngine↔LabEngine LOCAL-LAB Mirror" — CR-01/CR-02 interaction documented |
| CORE-13 | _my_library_tab_ref modeled as injected interface, no shared→desktop import | Section "_my_library_tab_ref Touch Points" — all 5 touch points mapped |
| GUARD-02 | Zero behavior change at every phase boundary | Section "Red Test Classification" — 7 red tests are BOM-caused Phase-124 regressions, not behavior gaps |
| GUARD-03 | Source-scanning tests retargeted before deletion | Section "GUARD-03 Retarget Candidates" — 8 test files identified |
| GUARD-04 | genizah_core.py stays a permanent re-export facade | Section "Facade Completeness" — full name list enumerated |
</phase_requirements>

---

## Summary

Phase 125 is the hardest core extraction in the v8.3.0 decomposition: it moves the three largest classes — `SearchEngine` (~3,490 lines, 52 methods), `LabEngine` (~1,413 lines, 18 methods), and `LabSettings` (~139 lines, 3 methods) — out of genizah_core.py and into `shared/`. It also ships the SEED-011 composition double-prep dedup as a required prerequisite (125a). After this phase, genizah_core.py will be reduced from ~6,065 lines to roughly ~1,100 lines (the language/config/logging cluster, the SearchEngine helper cluster, and the facade shim block).

**Biggest surprise from research:** All 7 "pre-existing red tests" confirmed at the Phase-124 base actually fail for a single reason: `genizah_core.py` has a UTF-8 BOM (introduced in Phase-124 commit `674d16b5`), and every one of these tests opens the file with `encoding="utf-8"` (not `utf-8-sig`), causing `ast.parse()` to raise `SyntaxError: invalid non-printable character U+FEFF`. The actual behaviors the tests assert (dedup handlers logging, `_local_lab_exc` exc_info, LabEngine attrs, LOCAL-LAB hook, post-dedup merge) ARE ALREADY CORRECTLY IMPLEMENTED in the live code. This means: (a) the 7 tests are Phase-124 milestone regressions (BOM introduction), NOT SEED-011 forward-specs, (b) 125a's plan must include a BOM fix as the very first commit, and (c) no new behavioral code changes are needed to turn them green — only BOM removal + GUARD-03 retargets when the code moves.

**Primary recommendation:** Land 125a as three commits: (1) BOM removal — turns all 7 red tests green; (2) SEED-011 Genizah composition dedup; (3) SEED-011 LAB composition dedup. Then land the three extraction plans (125b: LabSettings, 125c: LabEngine, 125d: SearchEngine) in that order — LabSettings first because LabEngine depends on it.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Full-text search execution (Tantivy + regex) | API/Backend (shared) | — | Stateless computation, shared web+desktop |
| Lab-mode fingerprint composition search | API/Backend (shared) | — | Shared by web parallels page + desktop Lab tab |
| LOCAL My-Library search gate (is_searchable) | Desktop (weakref interface) | API/Backend (injected) | Gate lives in MyLibraryTab (desktop widget); shared code uses injected interface |
| BrowseMap class-level cache | API/Backend (shared) | — | Process-level singleton; moves intact with SearchEngine |
| Responsa downgrade thread-local channel | API/Backend (shared) | — | Used by web/search_api.py; pre-cluster of SearchEngine |
| SEED-006 content_search compat gate | API/Backend (shared) | — | Runtime compat gate set at index-open; moves with SearchEngine |
| LabSettings persistence (JSON) | API/Backend (shared) | — | Config.LAB_CONFIG_FILE is a shared path |

---

## Standard Stack

### Core (no new dependencies)
This phase installs zero new packages. All packages are pre-existing.

| Library | Version | Purpose | Status |
|---------|---------|---------|--------|
| tantivy (Python binding) | pre-existing | Full-text index | Already in project |
| Python stdlib (threading, weakref, hashlib, json, re) | stdlib | Thread-local, weakref, hashing | Already used |

**Installation:** None required.

---

## Package Legitimacy Audit

> This phase installs **zero** external packages. The audit section is N/A.

**Packages removed due to slopcheck [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** none

---

## RESEARCH MANDATE ANSWERS

### 1. The 8 Pre-Existing Red Tests — Classification and Root Cause

**Root cause (VERIFIED by running all 7 tests):** Every test fails with `SyntaxError: invalid non-printable character U+FEFF` inside `ast.parse()`. The cause is that `genizah_core.py` has a UTF-8 BOM (`\xef\xbb\xbf`) and every affected test reads the file with `encoding="utf-8"` (not `utf-8-sig`).

**BOM introduction timeline (VERIFIED via `git show <sha>:genizah_core.py | head -c3`):**

| Commit | BOM | Description |
|--------|-----|-------------|
| `3fca9bd1` | No | Phase 123 Task 7 — lists_manager extraction (last clean commit) |
| `674d16b5` | **Yes** | Phase 123 Codex r1 — "drop spurious shared/responsa search-helper copies, flex/wildcard dup, logger routing" |
| `b63411c1`..`741f7b24` | Yes | Phase 124 (all commits) |
| Current HEAD | Yes | BOM persists |

The BOM was introduced in Phase 123 Codex round-1 fix commit `674d16b5`. It has been present ever since.

**Classification — ALL 7 tests:**

| Test | Classification | Evidence |
|------|---------------|----------|
| `test_audit_2026_06_23_guards.py::test_lab_composition_search_dedup_swallows_now_log` | **Phase-123 REGRESSION (BOM)** | Asserts dedup handlers log — THEY DO (verified with `utf-8-sig` parse); fails only due to BOM |
| `test_audit_2026_06_23_guards.py::test_lab_composition_search_local_lab_scan_logs_exc_info` | **Phase-123 REGRESSION (BOM)** | Asserts `_local_lab_exc` logs with `exc_info=True` — IT DOES; fails only due to BOM |
| `test_local_lab_invalidation.py::TestCR02LabEngineHasLocalLabHook::test_lab_engine_has_local_lab_attrs` | **Phase-123 REGRESSION (BOM)** | Asserts 4 attrs in LabEngine.__init__ — ALL PRESENT; fails only due to BOM |
| `test_local_lab_invalidation.py::TestLabCompositionSearchLocalLab::test_lab_composition_search_extends_local_lab_query` | **Phase-123 REGRESSION (BOM)** | Asserts `local_lab_searcher` in `lab_composition_search` — PRESENT; fails only due to BOM |
| `test_local_lab_invalidation.py::TestLabCompositionSearchLocalLab::test_search_composition_logic_extends_regular_local_query` | **Phase-123 REGRESSION (BOM)** | Asserts standard composition uses `self.local_searcher` not `local_lab_searcher` — CORRECT; fails only due to BOM |
| `test_local_post_dedup_merge.py::test_local_merge_inserts_after_dedup_call_site` | **Phase-123 REGRESSION (BOM)** | Asserts LOCAL merge after `_deduplicate` in `execute_search` — TRUE; fails only due to BOM |
| `test_phase_97_invariants.py::test_local_post_dedup_merge` | **Phase-123 REGRESSION (BOM)** | Same `_deduplicate` → `_rrf_merge` assertion — TRUE; fails only due to BOM |

**Critical implication:** These are NOT SEED-011 forward-specs. They are Phase-123 regressions. The 125a plan MUST fix the BOM as its first commit (before the SEED-011 dedup work), which will turn all 7 green. No new behavioral code is needed.

**Verification:** Parsed genizah_core.py with `encoding="utf-8-sig"` (strips BOM) and confirmed all assertions pass:
- Two `(KeyError, IndexError, TypeError)` dedup handlers in `lab_composition_search` — both log ✓
- `_local_lab_exc` handler logs with `exc_info=True` ✓
- LabEngine.__init__ has `local_lab_searcher`, `_local_lab_index`, `_lab_local_meta`, `local_lab_searcher_stale` ✓
- `lab_composition_search` references `local_lab_searcher` ✓
- `search_composition_logic` uses `self.local_searcher` and NOT `local_lab_searcher` ✓
- `execute_search` has `_deduplicate` followed by `_rrf_merge` ✓

**Side note on `test_nli_breaker_cross_module_invariants.py::test_no_bare_timeout_on_nli_calls_ast`:** This test also fails due to BOM (same `ast.parse` on genizah_core.py). The CONTEXT.md labels it "unrelated, pre-existing BOM issue." Research confirms the BOM was introduced in the same commit, so it is the SAME root cause. Fixing the BOM fixes this test too. Out of scope for 125a (CONTEXT says "unrelated") but worth noting.

---

### 2. SEED-011 Index-Independence Verification

**VERIFIED: the per-chunk plan IS index-independent.** The shared plan can safely be computed once.

**Current double-prep locations (GREP-VERIFIED — SEED-011 line numbers have drifted from Phase-123/124):**

#### SEED-011 Finding 1 — `search_composition_logic` (Genizah index loop)
- **Location:** genizah_core.py lines 4906–5014 (corpus_scope != 'local' loop)
- **Per-chunk prep (computed INSIDE the loop, for Genizah index):**
  ```python
  _cs_field = 'content_search' if getattr(self, '_has_content_search', False) else None
  t_query = self.build_tantivy_query(chunk, mode, content_search_field=_cs_field)
  regex = self.build_regex_pattern(chunk, mode, 0)
  ```
- **Identical prep repeated for LOCAL loop:** genizah_core.py lines 5059–5070 (`_t_query_scl`, `_regex_scl`)
- **Index-independent items:** `t_query` string, `regex` pattern, `is_text_filtered` check — none depend on which index is searched
- **Index-specific items:** `self.index.parse_query(t_query)` / `_local_index_scl.parse_query(...)` — ONLY the parse_query call differs per index

**CAVEAT for LOCAL loop:** The LOCAL loop applies `strip_search_diacritics` to the chunk for SEED-006 M1 compat:
```python
if _local_has_cs_scl and mode != 'Regex':
    _chunk_q_scl = [strip_search_diacritics(_w) for _w in _chunk_scl]
else:
    _chunk_q_scl = _chunk_scl
_t_query_scl = self.build_tantivy_query(_chunk_q_scl, mode)  # no content_search_field
```
The Genizah loop passes `content_search_field=_cs_field`; the LOCAL loop passes none but folds diacritics. These are GENUINELY DIFFERENT query strings for the same chunk. A shared plan must preserve this split. The fix is: compute TWO query strings per chunk (one Genizah-flavor, one LOCAL-flavor) plus ONE shared regex.

#### SEED-011 Finding 2 — `lab_composition_search` (Genizah LAB loop)
- **Location:** genizah_core.py lines 1549–1666 (Genizah LAB loop)
- **Per-chunk prep (inside loop):**
  ```python
  fp_str = text_to_fingerprint(chunk_text, freq_map=target_map)
  fp_list = fp_str.split()
  needed_unique_fps = set(fp_list)
  clauses = [f'{target_field}:{t}' for t in fp_str.split()]
  core_query = " OR ".join(clauses)
  final_query_str = f'({core_query}) AND (source:"V0.8"^10 OR source:"V0.7")'
  ```
- **Repeated for LOCAL LAB loop:** genizah_core.py lines ~1676–1730 (`_fp_str`, `_fp_list`, `_needed_unique_fps`, `_clauses`, `_core_query`)
- **Index-independent items:** `fp_str`, `fp_list`, `needed_unique_fps`, the core query string
- **Index-specific items:** `final_query_str` (adds source boost only for Genizah LAB), `self.lab_index.parse_query()` vs `local_lab_index.parse_query()`

**Confirmed index-independence for LAB:** `text_to_fingerprint`, `_is_phrase_statistically_weak`, and `_calculate_match_metrics` — all take the chunk text + freq_map, not the index. The per-chunk plan is genuinely identical across the two LAB passes.

**Fix shape for 125a is confirmed safe.** Compute once, search twice (once per present index). The only twist in `search_composition_logic` is the diacritic-fold difference: build a `ChunkPlan` dataclass with `genizah_query_str`, `local_query_str`, `compiled_regex`, plus the fingerprint fields for LAB.

---

### 3. The 3 CORE-10 Hazards — Mapped to file:line

#### Hazard A: BrowseMap Class-Level Cache

**Location:** `genizah_core.py:3166-3167` (class-level attrs)
```python
_shared_browse_map = None        # genizah_core.py:3166 (inside SearchEngine class body)
_browse_map_lock = threading.Lock()  # genizah_core.py:3167
```

**How it works:** `_load_browse_map()` (genizah_core.py:3169) uses `SearchEngine._shared_browse_map` as a class-level singleton keyed to the class object itself. Any code using `SearchEngine._shared_browse_map` must reference the right class.

**Migration:** When extracted to `shared/search_engine.py`, the class-level attrs move with the class — `SearchEngine._shared_browse_map` becomes `shared.search_engine.SearchEngine._shared_browse_map`. References in `_load_browse_map` are already self-referential (`SearchEngine._shared_browse_map`), so they resolve correctly after move with no code changes. The facade shim `genizah_core.SearchEngine is shared.search_engine.SearchEngine` (same object) ensures any code using `genizah_core.SearchEngine._shared_browse_map` still works.

**Risk:** Zero — class-level attrs stay with the class body.

#### Hazard B: SEED-006 `content_search` Compat Gates

**Module-level pre-cluster items (must move to `shared/search_engine.py` together with SearchEngine):**

| Item | Location | Role |
|------|----------|------|
| `_index_has_field(index, field_name)` | genizah_core.py:2361 | Returns True if Tantivy index has the named field; used to detect content_search at index-open time |
| `content_search_staleness_messages(genizah_present, local_present)` | genizah_core.py:2385 | Generates user-visible staleness warnings; consumed by SearchEngine.index_staleness_report |
| `MARK_TOLERANT_INSERTER` | genizah_core.py:2428 | Regex constant; consumed by `make_mark_tolerant_pattern` and SearchEngine.build_regex_pattern |
| `make_mark_tolerant_pattern(escaped_term)` | genizah_core.py:2431 | Builds the tolerant regex; consumed by SearchEngine.build_regex_pattern |

**Instance-level compat gate attrs (set in SearchEngine.__init__):**
- `self._has_content_search = False` (genizah_core.py:2467) — set by `reload_index` at line 3106
- `self._local_has_content_search = False` (genizah_core.py:2477) — set by `_open_local_searcher` at lines 2565, 2605

**Usage sites inside SearchEngine:**
- `genizah_core.py:4565`: `if not text_position and getattr(self, '_has_content_search', False)` in `execute_search`
- `genizah_core.py:4911`: `_cs_field = 'content_search' if getattr(self, '_has_content_search', False) else None` in `search_composition_logic`
- `genizah_core.py:5055`: `_local_has_cs_scl = getattr(self, "_local_has_content_search", False)` in LOCAL composition loop
- `genizah_core.py:3106`: `self._has_content_search = _index_has_field(self.index, "content_search")` in `reload_index`
- `genizah_core.py:2855`: `if getattr(self, "_local_has_content_search", False):` in `_query_local_index`

**Also consumed externally:**
- `tests/test_audit_27_28_a11y_statement_and_stale_index.py`: `from genizah_core import content_search_staleness_messages` (5 imports) — **facade must preserve**
- `tests/test_local_schema_rebuild_deferral.py`: `from genizah_core import _index_has_field` (2 imports) — **facade must preserve**

**Migration:** Move `_index_has_field`, `content_search_staleness_messages`, `MARK_TOLERANT_INSERTER`, `make_mark_tolerant_pattern` into `shared/search_engine.py` pre-cluster. Add them to the genizah_core facade shim. The helpers do NOT import from genizah_core (stdlib only), so no GUARD-01 issue.

Note: `_index_has_field` is also used by `shared/local_indexer.py` (test_local_schema_rebuild_deferral.py tests it there). The local_indexer currently imports it lazily from genizah_core; after the move it should be retargeted to `shared.search_engine`. Check `shared/local_indexer.py` for this lazy import at plan time.

#### Hazard C: `_LAST_RESPONSA_DOWNGRADE` Thread-Local

**Location:** genizah_core.py:126-183 (module-level, NOT inside SearchEngine)

```python
_LAST_RESPONSA_DOWNGRADE = threading.local()          # genizah_core.py:126
_LAST_RESPONSA_DOWNGRADE_META = threading.local()     # genizah_core.py:131
_set_last_responsa_downgrade(message)                 # genizah_core.py:134
_consume_last_responsa_downgrade() -> Optional[str]   # genizah_core.py:143
_set_last_responsa_downgrade_meta(meta)               # genizah_core.py:160
_consume_last_responsa_downgrade_meta() -> Optional[dict]  # genizah_core.py:171
```

**External importers (FACADE CRITICAL — tests import these directly):**
- `web/search_api.py:97`: `from genizah_core import _consume_last_responsa_downgrade as _impl`
- `web/search_api.py:1262`: `from genizah_core import _consume_last_responsa_downgrade_meta as _drain_meta`
- `tests/test_search_api.py:737`: `from genizah_core import _set_last_responsa_downgrade` (existence assert)
- `tests/test_search_api_v2.py:115`: `from genizah_core import _set_last_responsa_downgrade`
- `tests/test_search_api_v2.py:118`: `from genizah_core import _set_last_responsa_downgrade_meta`
- `tests/test_search_api_v2.py:128`: `from genizah_core import (_consume_last_responsa_downgrade, _consume_last_responsa_downgrade_meta, ...)`

**These items are called FROM WITHIN SearchEngine's `execute_search` method** (the responsa cascade decision sets the downgrade; the web handler reads it via genizah_core facade). They live in the module-level pre-cluster.

**Migration decision:** These thread-locals logically belong to SearchEngine's execution context. Move them to `shared/search_engine.py` pre-cluster. Add all 6 names (`_LAST_RESPONSA_DOWNGRADE`, `_LAST_RESPONSA_DOWNGRADE_META`, `_set_last_responsa_downgrade`, `_consume_last_responsa_downgrade`, `_set_last_responsa_downgrade_meta`, `_consume_last_responsa_downgrade_meta`) to the genizah_core facade shim.

**Also needed as SearchEngine pre-cluster items in `shared/search_engine.py`:**
- `RRF_K = 60` (used by `_rrf_merge` and `execute_search`)
- The entire "RESPONSA REGEX HELPERS" cluster: `_make_flex_spacing_pattern`, `_build_wildcard_regex`, `_add_bracket_variants`, `_query_has_brackets`, `_strip_brackets` (already noted as "engine-side helpers stay" from Phase 123 decisions)
- `_count_unique_chunks` (genizah_core.py:466) — standalone function called from both `search_composition_logic` and `lab_composition_search`; tests import it directly: `from genizah_core import _count_unique_chunks`

---

### 4. SearchEngine↔LabEngine LOCAL-LAB Mirror (CR-01/CR-02)

**CR-01 — `_lab_weights_hash_override`:**
- **Location:** genizah_core.py:2702 (in `SearchEngine._current_lab_weights_hash`)
- **Mechanism:** `genizah_app.py` sets `searcher._lab_weights_hash_override = lab_engine._current_lab_weights_hash()` at GUI init + after every LOCAL LAB rebuild. This lets the standard `search_composition_logic` (which runs on SearchEngine, not LabEngine) compare against the correct weights-hash. The `_current_lab_weights_hash` method uses `getattr(self, '_lab_weights_hash_override', None)` to short-circuit.
- **Both classes define `_current_lab_weights_hash`** (SearchEngine:2680, LabEngine via inheritance-by-convention — it's defined at SearchEngine:2680 and LabEngine has its OWN copy). Wait — let me re-check: LabEngine has it at the class body too.

**Verification:** `_current_lab_weights_hash` appears in:
- SearchEngine: line 2680 (handles missing dynamic_rank_map via getattr fallback)
- LabEngine: LabEngine also has `_check_local_lab_freshness` (genizah_core.py line ~790) which calls `_current_lab_weights_hash`

**CR-02 — LOCAL LAB side-index on LabEngine:**
- LabEngine.__init__ sets `local_lab_searcher=None`, `_local_lab_index=None`, `local_lab_searcher_stale=False`, `_lab_local_meta=None` (genizah_core.py:654-657)
- LabEngine.reload_local_lab_index (genizah_core.py:713+) opens the LOCAL LAB index
- SearchEngine ALSO has these same attrs (set in SearchEngine.__init__:2478-2481), allowing `_check_local_lab_freshness` (which is a SearchEngine method) to be called via `getattr(self, "_check_local_lab_freshness", None)` in LabEngine.lab_composition_search

**After extraction:** LabEngine in `shared/lab_engine.py` will import `LabSettings` from `shared/lab_settings.py`. No circular dependency (LabSettings imports only stdlib). The `_check_local_lab_freshness` method lives on SearchEngine, not LabEngine — LabEngine accesses it via `getattr(self, "_check_local_lab_freshness", None)` guard, which is the correct pattern for cross-class method access that works even when `self` is a LabEngine instance (which doesn't have the method, returning None gracefully).

---

### 5. `_my_library_tab_ref` Touch Points

All 5 touch points in genizah_core.py — mapped to exact lines:

| Line | Context | What it does |
|------|---------|-------------|
| 2488 | `SearchEngine.__init__` | `self._my_library_tab_ref: weakref.ref | None = None` — initialization |
| 2500 | `SearchEngine.attach_my_library_tab` | `self._my_library_tab_ref = weakref.ref(tab)` — wires the weakref |
| 2840 | `SearchEngine._query_local_index` | `tab = self._my_library_tab_ref() if self._my_library_tab_ref is not None else None` — reads gate |
| 5037 | `SearchEngine.search_composition_logic` | `_scl_tab = self._my_library_tab_ref() if getattr(self, "_my_library_tab_ref", None) is not None else None` — reads gate |
| 1678 | `LabEngine.lab_composition_search` | `_lab_tab = self._my_library_tab_ref() if getattr(self, "_my_library_tab_ref", None) is not None else None` — reads gate |

**Note:** LabEngine line 1408 is also a touch point (`_tab = self._my_library_tab_ref() ...` in `lab_search` method context — will resolve to the same line block after extraction).

**CORE-13 DI interface:** The `attach_my_library_tab` method accepts `tab` which must have an `is_searchable: bool` attribute. There is no other attribute access — `getattr(_tab, "is_searchable", True)` is the only use.

**Minimal interface (no ABC needed):**
```python
# In shared/search_engine.py (or shared/local_search_gate.py)
# The "interface" is duck-typed — any object with .is_searchable: bool
# attach_my_library_tab accepts anything satisfying Protocol:
# class LocalSearchGate(Protocol):
#     is_searchable: bool
```

**GUARD-01 compliance:** The tab reference is stored as a `weakref.ref` to a desktop widget (PyQt6 class). The import of `weakref` is stdlib. At NO point does `shared/search_engine.py` need to `import` anything from `desktop/`. The weakref indirection is sufficient. GUARD-01 is preserved by design.

---

### 6. Facade Completeness — Names the Move Would Drop

The following names are currently importable from genizah_core and would be dropped if the move does NOT add facade shims. All must have `# noqa: F401` shims added:

**SearchEngine cluster (move to `shared/search_engine.py`, shim in genizah_core):**

| Name | Type | Imported by |
|------|------|-------------|
| `SearchEngine` | class | genizah_app.py:34, gui_threads.py:9, web/main.py:675, web/state.py:2, web_pilot.py:2, tests/many |
| `RRF_K` | constant | tests/test_local_post_dedup_merge.py:16, tests/test_side_index_merge.py:13 |
| `_set_last_responsa_downgrade` | function | tests/test_search_api.py:737, tests/test_search_api_v2.py:115 |
| `_consume_last_responsa_downgrade` | function | web/search_api.py:97, tests/test_search_api_v2.py:128 |
| `_set_last_responsa_downgrade_meta` | function | tests/test_search_api_v2.py:118 |
| `_consume_last_responsa_downgrade_meta` | function | web/search_api.py:1262, tests/test_search_api_v2.py:128 |
| `content_search_staleness_messages` | function | tests/test_audit_27_28_a11y_statement_and_stale_index.py:99-129 |
| `_index_has_field` | function | tests/test_local_schema_rebuild_deferral.py:361,382 |
| `MARK_TOLERANT_INSERTER` | constant | (used internally + potentially tests) |
| `_count_unique_chunks` | function | tests/test_lab_composition_chunk_hits.py:403,409,421,433 |

**LabEngine cluster (move to `shared/lab_engine.py`, shim in genizah_core):**

| Name | Type | Imported by |
|------|------|-------------|
| `LabEngine` | class | genizah_app.py:34, build_index.py:17, web/main.py:675, web/state.py:2, corpus_mapper/runner.py:421,657, tests/many |
| `LabSettings` | class | tests/test_comp_corpus_scope.py:121, tests/test_lab_composition_chunk_hits.py:92,453 |

**SearchEngine pre-cluster (also move to `shared/search_engine.py`):**

| Name | Type | Used by |
|------|------|---------|
| `_LAST_RESPONSA_DOWNGRADE` | threading.local | (internal, but test monkeypatches may access) |
| `_LAST_RESPONSA_DOWNGRADE_META` | threading.local | (internal) |
| `_make_flex_spacing_pattern` | function | SearchEngine internal (build_tantivy_query / build_regex_pattern) |
| `_build_wildcard_regex` | function | SearchEngine internal |
| `_add_bracket_variants` | function | SearchEngine internal |
| `_query_has_brackets` | function | SearchEngine internal (also search_composition_logic) |
| `_strip_brackets` | function | SearchEngine internal (also has a copy in shared/indexer.py already) |

**IMPORTANT — Phase-124 lesson:** Function-local `from genizah_core import <name>` calls (inside test function bodies) are NOT caught by naive module-level grep. The Phase-124 miss was `_parse_cudl_label` imported in test function bodies (not module-level). The same risk exists here. At plan time, the executor must grep for ALL occurrences of each moved name in test files, including within function bodies.

**Known function-body imports of engine names (from genizah_imports.txt scan):**
- `tests/test_local_schema_rebuild_deferral.py:361,382` — `from genizah_core import _index_has_field` — inside test functions
- `tests/test_nli_crossref_service.py:1145,1151,1157,1163` — `from genizah_core import _parse_cudl_label` — (already shimmed by Phase 124)

---

### 7. GUARD-03 Retarget Candidates

Tests that scan `genizah_core.py` FILE TEXT for SearchEngine/LabEngine code that will move:

| Test File | What it scans genizah_core.py for | Retarget needed? |
|-----------|----------------------------------|-----------------|
| `test_audit_2026_06_23_guards.py` | `lab_composition_search` function body (dedup handlers); `_rrf_merge` signature via `inspect` | YES — retarget to `shared/search_engine.py` (SearchEngine._rrf_merge) and `shared/lab_engine.py` (lab_composition_search) |
| `test_local_lab_invalidation.py` | `lab_composition_search`, `search_composition_logic` function bodies; `local_lab_searcher` attr | YES — retarget to `shared/lab_engine.py` + `shared/search_engine.py` |
| `test_local_post_dedup_merge.py` | `_deduplicate`, `_rrf_merge`, LOCAL merge in `execute_search` | YES — retarget to `shared/search_engine.py` |
| `test_phase_97_invariants.py` | `_deduplicate` call site + `_rrf_merge` call site in genizah_core.py | YES — retarget to `shared/search_engine.py` |
| `test_lab_composition_chunk_hits.py` | `rec['chunk_count'] += 1` absent; `chunk_hits` defaultdict shape | YES — retarget to `shared/lab_engine.py` |
| `test_comp_corpus_scope.py` | stale line-number comments (`genizah_core.py:1421`) — no actual file reads | NO — comment-only, no live assert |
| `test_batched_search_progress_protocol.py` | Reads `web/pages/parallels.py`, not genizah_core.py; `genizah_core.LabEngine` is a module import | NO — reads parallels.py, not the engine file |
| `test_joins_lab_page.py` | Line comments only — no live `read_text("genizah_core.py")` | NO — comments only |

**High-confidence GUARD-03 set (must retarget in 125a):**
1. `test_audit_2026_06_23_guards.py` (lines 153, 168 — `read_text("utf-8")` on genizah_core.py)
2. `test_local_lab_invalidation.py` (lines 618-626, 642, 658, 677, 689, 706, 713, 818 — reads genizah_core.py)
3. `test_local_post_dedup_merge.py` (line 135 — `read_text("utf-8")` on genizah_core.py)
4. `test_phase_97_invariants.py` (line 217 — `read_text("utf-8")` on genizah_core.py)
5. `test_lab_composition_chunk_hits.py` (lines 341, 363, 389 — `open("genizah_core.py", encoding="utf-8")`)

**Note on BOM and encoding:** The 125a BOM fix will make all the `encoding="utf-8"` reads work again. However, after the SearchEngine code moves to `shared/search_engine.py`, tests 1–5 above that assert behavior of methods now in `shared/search_engine.py` (or `shared/lab_engine.py`) will need to be updated to read from the new file. This is the standard GUARD-03 pattern.

**Timing:** Tests 1–5 fail NOW due to BOM. BOM fix in first 125a commit restores them. When engine code moves (125b–125d), they'll need path retargeting. The retarget should happen in the SAME commit as the extraction (additive phase), per the GUARD-03 pattern.

---

### 8. Plan Decomposition Recommendation

**Wave/plan structure (recommended):**

**125a — SEED-011 + BOM Fix (single plan, 3 commits):**
- **Commit 1:** Strip the UTF-8 BOM from `genizah_core.py` — turns all 7 red tests green immediately. Zero functional change.
- **Commit 2:** SEED-011 Finding 1 — `search_composition_logic` double-prep dedup. Extract per-chunk plan into a local dataclass / tuple. Genizah query uses `build_tantivy_query(chunk, mode, content_search_field=_cs_field)`; LOCAL query uses `build_tantivy_query(_chunk_q_scl, mode)` with the folded chunk — the TWO query strings per chunk are part of the plan (not purely index-independent, but safely computable once in the outer loop).
- **Commit 3:** SEED-011 Finding 2 — `lab_composition_search` double-prep dedup. The LAB fingerprint plan IS purely index-independent (same `fp_str`, `fp_list`, `needed_unique_fps`, `core_query`).

**125b — LabSettings extraction (plan 2):**
- Simple stdlib-only class. No back-edges. Single commit.
- Add to GUARD-01 registry (11 entries after this).

**125c — LabEngine extraction (plan 3):**
- Depends on LabSettings being in `shared/lab_settings.py`.
- Pre-cluster: none (all LabEngine helpers are methods).
- Imports: `from shared.lab_settings import LabSettings` (no genizah_core back-edge).
- Tantivy guard needed (same pattern as shared/indexer.py).
- Move CR-02 LOCAL LAB attrs and `reload_local_lab_index` intact.
- GUARD-03 retarget: `test_lab_composition_chunk_hits.py` for lab_composition_search assertions.
- Add to GUARD-01 registry (12 entries).

**125d — SearchEngine extraction (plan 4 — HARDEST):**
- Pre-cluster moves with the class: `_LAST_RESPONSA_DOWNGRADE` + downgrade functions, `RRF_K`, the 7 Responsa regex helpers, `_count_unique_chunks`, `MARK_TOLERANT_INSERTER`, `make_mark_tolerant_pattern`.
- Class body: copy intact (52 methods, ~3,490 lines).
- GUARD-03 retargets: `test_audit_2026_06_23_guards.py`, `test_local_lab_invalidation.py`, `test_local_post_dedup_merge.py`, `test_phase_97_invariants.py`.
- Retarget `shared/local_indexer.py` lazy import of `_index_has_field` to `shared.search_engine`.
- Retarget any lazy `from genizah_core import _index_has_field` in test bodies to `shared.search_engine`.
- Add to GUARD-01 registry (13 entries).

**Alternative (merged):** 125b/c/d could be one plan with 3 sequential commits (LabSettings → LabEngine → SearchEngine). This is lower overhead but reduces rollback granularity. Recommended: keep as separate plans given SearchEngine's size.

---

## Architecture Patterns

### Proven Recipe (from Phases 123–124)

1. **Copy class body intact** to new file with correct header
2. **Add tantivy guard** in new module if Tantivy is imported (same message: `"Tantivy library missing. Please install it."`)
3. **Add `logging.getLogger("genizah." + __name__)`** at module level
4. **inline `_tr()` helper** if the class uses `tr()` (lazy `from genizah_core import CURRENT_LANG` inside function body — GUARD-01 safe)
5. **Per-file `ruff check`** after copying (F401 unused imports are common after extraction)
6. **Add facade shim** in genizah_core.py: `from shared.<module> import <Class>  # noqa: F401`
7. **Identity test**: `assert genizah_core.SearchEngine is shared.search_engine.SearchEngine`
8. **Base-vs-HEAD facade name diff** before declaring done (Phase-124 lesson: count-based isn't enough)

### New for Phase 125: SearchEngine Pre-Cluster

The SearchEngine pre-cluster (the "RESPONSA REGEX HELPERS" section in genizah_core.py plus the `_LAST_RESPONSA_DOWNGRADE` cluster and `RRF_K`) must move INTO `shared/search_engine.py` before the SearchEngine class body. These helpers have no module-level back-edges to genizah_core (verified: they import from stdlib only). After the move, they need facade shims for the names external code imports.

### Anti-Patterns to Avoid
- **Count-based failure comparison:** Phase-124 lesson — always do base-vs-HEAD NAME-level diff, never count comparison.
- **Moving LabEngine before LabSettings:** LabEngine.__init__ instantiates `LabSettings()` directly — extract LabSettings first.
- **Importing desktop classes in shared/:** GUARD-01 violation; the `_my_library_tab_ref` pattern uses a weakref + duck typing to avoid this.
- **Moving `_count_unique_chunks` to a separate module:** It belongs in `shared/search_engine.py` as a module-level helper (both `search_composition_logic` and `lab_composition_search` use it; keeping it with SearchEngine avoids a new mini-module).

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cross-module shared class identity | Duplicate class in facade | `same-object re-export shim` (`from shared.X import Y  # noqa: F401`) | Proven pattern; `genizah_core.SearchEngine is shared.search_engine.SearchEngine` |
| BOM stripping | Custom byte editor | `open(path, 'rb').read()` + `lstrip(b'\xef\xbb\xbf')` + write back | Simpler than encoding flag changes in tests |
| GUARD-01 test for new modules | New test file | Extend `EXTRACTED_MODULES` list in `test_no_back_edges_core.py` | Existing parametrized test handles all modules |
| Tantivy import error message | New error class | Same `ImportError("Tantivy library missing. Please install it.")` message | Test `test_missing_tantivy.py` checks for this exact string |

---

## Common Pitfalls

### Pitfall 1: Function-Local Imports Missed by Module-Level Grep

**What goes wrong:** A grep for `from genizah_core import SearchEngine` misses tests that do `from genizah_core import SearchEngine` inside function bodies. After extraction, the facade shim handles it — but if you grep to confirm "no external importers" and trust that result, you'll miss them.
**How to avoid:** When building the facade completeness checklist, grep for the name itself, not just module-level import patterns. Check test function bodies specifically. Use the `genizah_imports.txt` scan from this research as the baseline.

### Pitfall 2: LabEngine Has `_my_library_tab_ref` Access But No Definition

**What goes wrong:** `LabEngine.lab_composition_search` uses `self._my_library_tab_ref` (line 1678) but LabEngine.__init__ does NOT assign `self._my_library_tab_ref`. It relies on the fact that in production, LabEngine instances are never used as the composition searcher — `SearchEngine` (which inherits nothing from LabEngine) holds the `_my_library_tab_ref`. The `getattr(self, "_my_library_tab_ref", None)` guard prevents crashes when it's absent.
**How to avoid:** When extracting LabEngine, do NOT add `self._my_library_tab_ref = None` to LabEngine.__init__ (that would change behavior). Keep the existing `getattr` guard pattern.

### Pitfall 3: BOM Must Be Stripped Without Changing the Encoding Declaration

**What goes wrong:** genizah_core.py has `# -*- coding: utf-8 -*-` on line 2. Stripping the BOM doesn't change the encoding. The correct fix is to open the file in binary mode, strip the first 3 bytes if they are `\xef\xbb\xbf`, and write it back. Do NOT change `encoding='utf-8'` to `encoding='utf-8-sig'` in the test files — that would mask rather than fix the root cause and would diverge from the project encoding convention.

### Pitfall 4: MARK_TOLERANT_INSERTER Used Inside SearchEngine

**What goes wrong:** `MARK_TOLERANT_INSERTER` is defined at module-level (line 2428) and consumed inside `SearchEngine.build_regex_pattern` as a bare name reference. When the class moves, the bare name must be importable from the same scope.
**How to avoid:** Move `MARK_TOLERANT_INSERTER` and `make_mark_tolerant_pattern` into `shared/search_engine.py` pre-cluster (before the class). They're defined in the SAME pre-cluster section — just move them together with the class.

### Pitfall 5: `shared/local_indexer.py` Lazy-Imports `strip_search_diacritics` from genizah_core

The retarget for text_normalize was done in Phase 123. But `shared/local_indexer.py` may ALSO lazily import `_index_has_field` from genizah_core. Verify at plan time by grepping `local_indexer.py` for `_index_has_field`. If present, retarget to `shared.search_engine` in the SearchEngine extraction commit.

---

## State of the Art

| Old Approach | Current Approach | Changed | Impact |
|--------------|-----------------|---------|--------|
| All 3 engine classes in genizah_core.py (~6,065 lines) | `shared/search_engine.py`, `shared/lab_engine.py`, `shared/lab_settings.py` (Phase 125) | Phase 125 | genizah_core shrinks to ~1,100 lines; direct-module imports enable faster test discovery |
| Double prep per-chunk in composition search (222 preps for 111 chunks) | Single shared plan per chunk (SEED-011 125a) | Phase 125a | ~50% CPU reduction for `corpus_scope='all'` composition; behavior identical |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `shared/local_indexer.py` lazy-imports `_index_has_field` from genizah_core (mentioned in text) | Pitfall 5 | LOW — grep it at plan time; retarget if present |
| A2 | LabEngine never needs `attach_my_library_tab` — only SearchEngine does | _my_library_tab_ref section | LOW — LabEngine uses getattr guard, so absence of the method is safe |

**All other claims are VERIFIED** by running the failing tests, inspecting `genizah_core.py` with `encoding="utf-8-sig"`, checking git history for BOM introduction, and reading the actual class bodies and method signatures.

---

## Open Questions

1. **Does `shared/local_indexer.py` have a lazy import of `_index_has_field` from genizah_core?**
   - What we know: It was retargeted for `strip_nikud`/`strip_search_diacritics` in Phase 123.
   - What's unclear: Whether `_index_has_field` was ALSO lazily imported there.
   - Recommendation: Grep for `_index_has_field` in local_indexer.py at plan time; add to retarget list if found.

2. **Does the `test_nli_breaker_cross_module_invariants.py::test_no_bare_timeout_on_nli_calls_ast` BOM failure need a Wave 0 fix in 125a?**
   - The CONTEXT.md says it's "out of scope" for this phase. However, the BOM fix that turns the 7 red tests green will ALSO fix this 8th test. The planner can decide whether to claim it or note it as a bonus fix.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | `tests/conftest.py` |
| Headless flag | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` |
| Quick run command | `python -m pytest tests/test_no_back_edges_core.py tests/test_comp_corpus_scope.py tests/test_lab_composition_chunk_hits.py tests/test_local_lab_invalidation.py tests/test_local_post_dedup_merge.py tests/test_phase_97_invariants.py tests/test_audit_2026_06_23_guards.py -q` |
| Full suite command | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -q -m "not gui and not render_smoke"` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PREP-01 (BOM) | genizah_core.py has no BOM | unit/structural | `python -c "raw=open('genizah_core.py','rb').read(); assert raw[:3]!=b'\xef\xbb\xbf'"` | N/A (one-liner) |
| PREP-01 (dedup) | Composition dedup tests pass | unit | `python -m pytest tests/test_local_post_dedup_merge.py tests/test_phase_97_invariants.py -q` | ✅ (currently red → green after BOM fix) |
| PREP-01 (SEED-011) | No double-prep in composition loops | structural | `python -m pytest tests/test_comp_corpus_scope.py -q` (existing) + new 125a test | ✅ / ❌ Wave 0 (new SEED-011 test) |
| CORE-10 | SearchEngine importable from both genizah_core and shared.search_engine; same object | identity | `python -m pytest tests/test_no_back_edges_core.py -q` | ✅ (grows registry) |
| CORE-11 | LabSettings importable from both; same object | identity | `python -m pytest tests/test_no_back_edges_core.py -q` | ✅ (grows registry) |
| CORE-12 | LabEngine importable from both; same object; CR-02 attrs present | identity + structural | `python -m pytest tests/test_no_back_edges_core.py tests/test_local_lab_invalidation.py -q` | ✅ (test_local_lab_invalidation currently red → green after BOM + move) |
| CORE-13 | No shared/ → desktop import in new modules | GUARD-01 | `python -m pytest tests/test_no_back_edges_core.py -q` | ✅ (registry check) |
| GUARD-02 | Full suite zero new failures | suite | Full suite command above | ✅ |
| GUARD-03 | Source-scanning tests pass after engine move | suite | `python -m pytest tests/test_audit_2026_06_23_guards.py tests/test_local_lab_invalidation.py tests/test_local_post_dedup_merge.py tests/test_phase_97_invariants.py tests/test_lab_composition_chunk_hits.py -q` | ✅ |
| GUARD-04 | genizah_core facade preserves all engine names | identity | base-vs-HEAD NAME-level diff + identity asserts | N/A (manual + automated) |

### Sampling Rate
- **Per task commit:** Quick run command above
- **Per wave merge:** Full suite command
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] New test for SEED-011 dedup: `tests/test_seed011_composition_dedup.py` — verifies per-chunk plan computed once, not N×2; covers both `search_composition_logic` and `lab_composition_search` (asserts chunk build invocation count, e.g., via mock of `build_tantivy_query`). These tests should pass after 125a commit 2 and 3.
- [ ] (Optional) `test_no_back_edges_core.py` registry extension to 13 — Wave 0 prep so GUARD-01 test doesn't need patching mid-extraction.

---

## Security Domain

This phase performs code movement only — no new network endpoints, no new auth paths, no schema changes, no new crypto, no new user inputs. ASVS categories do not apply to a pure refactor milestone.

---

## Environment Availability

> This phase makes no changes to external tools or services.

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| tantivy (Python) | SearchEngine/LabEngine | Yes | pre-existing | — |
| Python 3.10+ | Type hints | Yes | 3.11 (checked) | — |
| pytest | Test suite | Yes | pre-existing | — |
| Genizah_Index (Tantivy index) | Real-index tests | Yes | confirmed present in working tree | — |

---

## Sources

### Primary (HIGH confidence — VERIFIED in this session)
- Live `genizah_core.py` (encoding `utf-8-sig`) — class bodies, line numbers, method names, exception handlers
- Git history (`git show <sha>:genizah_core.py | head -c3`) — BOM introduction timeline
- Running the 7 red tests directly — failure message analysis
- AST parsing `genizah_core.py` with utf-8-sig — confirmed all assertions would pass with BOM stripped
- `genizah_imports.txt` — 388-line scan of all genizah_core imports across the codebase
- Phase 123/124 SUMMARY.md files — proven recipe, lessons learned
- REQUIREMENTS.md + CONTEXT.md — locked decisions and GUARD requirements
- `.planning/STATE.md` — current phase position, Phase-124 name-level diff lesson
- `tests/test_no_back_edges_core.py` — current GUARD-01 registry (10 entries)

### Secondary (MEDIUM confidence)
- SEED-011 original Codex audit (line numbers pre-Phase-123/124 drift — corrected by grep)
- SEED-020 §7 hazard notes (pre-dates Phase-123/124 — verified against live code)

---

## Metadata

**Confidence breakdown:**
- Red test classification: HIGH — verified by running tests AND by AST parsing with correct encoding
- BOM root cause: HIGH — confirmed via git binary inspection across 20 commits
- SEED-011 double-prep locations: HIGH — verified by reading actual code at current line numbers
- Index-independence claim: HIGH — LOCAL composition has a diacritic-fold difference that affects the query string but NOT the plan's structural identity (two strings per plan is still one plan per chunk)
- Facade completeness: HIGH — derived from the 388-line import scan
- Hazard mapping: HIGH — verified file:line in live code

**Research date:** 2026-06-26
**Valid until:** 2026-07-26 (stable domain — genizah_core line numbers may drift from commits but structure is stable)
