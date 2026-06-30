# Phase 125 — Core Engines — CONTEXT

**Discuss phase: consciously SKIPPED (2026-06-26).** No genuine user-facing gray area.
Assessment: (1) SEED-011 (125a) is a **behavior-preserving** performance refactor with a
well-specified fix shape (precompute a shared per-chunk plan once; the plan is asserted
index-independent; then search per index separately) — success criterion #1 requires the
relevant composition tests to PASS, i.e. results unchanged; the only choices are
implementation structure (planner) and validating index-independence (research). (2) The
engine extractions (SearchEngine / LabSettings / LabEngine) are the same proven
copy→shim→retarget recipe. (3) The 3 hazards and `_my_library_tab_ref` DI are already-locked
preservation requirements (see below), not open questions. The one real investigation — the
status of the 8 currently-red composition/dedup/local-lab tests — is a RESEARCH mandate, not
a user decision. No CONTEXT questions were asked of the user. (Standing autonomous directive
for phases 125–127; [[feedback_no_auto_discuss]] honored — discuss skipped, not auto-answered.)

## Locked decisions (from ROADMAP + REQUIREMENTS + milestone key decisions)

- **PREP-01 (125a, FIRST):** Land the SEED-011 composition double-prep dedup BEFORE any
  engine code moves, so the dedup isn't reworked post-move. Fix shape (SEED-011 Findings 1+2):
  precompute a **shared per-chunk plan once** (query string + compiled regex +
  weak/fingerprint derivations — index-independent), then run the Tantivy search + regex
  filter pass **per index separately**. Applies to BOTH `corpus_scope='all'` (genizah_core
  ~9216/~9362) AND LAB composition (~1604/~1772 — line numbers are pre-Phase-123/124 and have
  DRIFTED; grep, don't trust them). **Behavior-preserving** — composition results identical;
  this is the success gate, not just perf.
- **CORE-10:** Extract `SearchEngine` **intact** to `shared/search_engine.py` with
  `meta_mgr`/`var_mgr` passed by **dependency injection**. PRESERVE the 3 hazards with behavior
  unchanged: (a) the **BrowseMap class-level cache** migration, (b) the **SEED-006
  `content_search` compat gates**, (c) the **`_LAST_RESPONSA_DOWNGRADE` thread-local** downgrade
  channel. *(SEED-020 §7 C-3)*
- **CORE-11:** Extract `LabSettings` → `shared/lab_settings.py`.
- **CORE-12:** Extract `LabEngine` → `shared/lab_engine.py`; PRESERVE the SearchEngine↔LabEngine
  LOCAL-LAB mirror (CR-01/CR-02, `_lab_weights_hash_override`).
- **CORE-13:** Model `_my_library_tab_ref` as an **injected optional "local-search-gate"
  interface** consumed by BOTH `SearchEngine.attach_my_library_tab()` and
  `LabEngine.lab_composition_search()`. **No `shared/` → desktop import** (GUARD-01). *(C-4)*
- **GUARD-01/02/04:** no module-level back-edges (lazy/function-body imports + the injected
  interface for the desktop coupling); zero behavior change; genizah_core stays a permanent
  re-export facade for all moved names (`SearchEngine`, `LabEngine`, `LabSettings`, + any
  previously-importable helpers). GUARD-01 registry grows to 13.
- **GUARD-03:** retarget any source-scanning test that scans genizah_core.py for the moved
  engine code BEFORE/with the move (the 5 named files + any others surfaced by research).
- **DEFER-01 stays deferred:** do NOT sub-split SearchEngine (LineBreakSearcher /
  CompositionSearcher) in this phase — move the class INTACT.

## Process guidance (carry forward — TWO Codex gates; see project_godfile_extraction_import_lesson + project_v83_decomposition_milestone)

- **Derive imports from the ACTUAL copied bodies** — RESEARCH import lists are indicative only.
  Two-sided gate: per-file `ruff check` (excess/F401) + full-suite-green (missing, incl.
  method-runtime NameErrors). NEVER repo-wide `ruff --fix` (strips noqa shims).
- **Gate ① — Codex PLAN PRE-FLIGHT (after gsd-plan-checker, BEFORE execute):** review the
  PLAN + RESEARCH against the LIVE codebase for plan↔code drift the internal checker can't see
  (e.g. a "no external importers" claim a grep disproves — the exact miss that bit Phase 124's
  `_parse_cudl_label`). Must clear before execution. ([[feedback_codex_preflight_before_plan_complete]])
- **Gate ② — Codex CODE review 3-round convergence (after execute):** + the systematic
  base-vs-HEAD facade-name diff + the base-vs-HEAD **NAME-level** test comparison. Do NOT trust
  the executor's "N pre-existing, 0 new" count — run the name-level comparison yourself (the
  Phase-124 lesson). Both gates must reach APPROVE.
- **Logging:** `logging.getLogger("genizah." + __name__)` in every new module (not bare).
- **Phase 124 base for any cross-phase diffs:** the Phase 125 base is current HEAD
  (`741f7b24`-and-later, post-124-closeout).

## RESEARCH MANDATE (must resolve before planning)

1. **The 8 pre-existing red tests (confirmed RED at the Phase-124 base `e6714343`)** — all
   composition / dedup / local-lab:
   - `test_audit_2026_06_23_guards.py::test_lab_composition_search_dedup_swallows_now_log`
   - `test_audit_2026_06_23_guards.py::test_lab_composition_search_local_lab_scan_logs_exc_info`
   - `test_local_lab_invalidation.py::TestCR02LabEngineHasLocalLabHook::test_lab_engine_has_local_lab_attrs`
   - `test_local_lab_invalidation.py::TestLabCompositionSearchLocalLab::test_lab_composition_search_extends_local_lab_query`
   - `test_local_lab_invalidation.py::TestLabCompositionSearchLocalLab::test_search_composition_logic_extends_regular_local_query`
   - `test_local_post_dedup_merge.py::test_local_merge_inserts_after_dedup_call_site`
   - `test_phase_97_invariants.py::test_local_post_dedup_merge`
   - (also `test_nli_breaker_cross_module_invariants.py::...::test_no_bare_timeout_on_nli_calls_ast` — UNRELATED, pre-existing BOM/AST issue; out of scope)
   **git-log each** ([[feedback_regression_attribution]] — don't trust gap notes): when did they
   start failing? Are they (a) FORWARD-SPEC for SEED-011 (125a should turn them green — likely),
   (b) a Phase-122/123 milestone regression (flag loudly), or (c) long-standing tech debt
   unrelated to SEED-011 (planner decides disposition)? Their resolution shapes the 125a plan.
2. **Validate SEED-011 index-independence:** confirm the per-chunk plan (query/regex/weak/
   fingerprint) is truly identical across indices before sharing it (else the dedup would change
   results — that WOULD be a behavior change to surface).
3. **Map the 3 CORE-10 hazards** in the live code (BrowseMap class-cache, SEED-006 `content_search`
   gates, `_LAST_RESPONSA_DOWNGRADE` thread-local) + the LOCAL-LAB mirror (`_lab_weights_hash_override`,
   CR-01/CR-02) + every `_my_library_tab_ref` touch point — so the DI interface (CORE-13) covers them.
4. **Facade completeness:** enumerate every name currently importable from genizah_core that the
   SearchEngine/LabEngine/LabSettings move would drop (the Phase-124 facade-diff technique, ahead of time).
5. **Plan decomposition:** likely 125a (SEED-011 dedup) as its own plan/wave FIRST, then the
   engine extractions. Planner decides; SEED-011 must land before the engine code moves.

## Open questions requiring user input

None at assessment time. **Possible escalation:** if the research finds the 8 red tests are a
genuine Phase-122/123 milestone regression (not SEED-011 forward-spec), that may warrant a user
heads-up before planning — surface it then, don't pre-decide.
