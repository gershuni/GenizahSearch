# Phase 124 — Core Metadata & Index — CONTEXT

**Discuss phase: consciously SKIPPED (2026-06-26).** No user-facing gray areas. The
roadmap success criteria are concrete and locked, this is the same proven
copy→shim→retarget recipe validated on Phase 123, and the remaining choices (plan
decomposition, import lists, lazy-import handling) are planner/research concerns —
not user decisions. No CONTEXT questions were asked of the user.

## Locked decisions (from ROADMAP + milestone key decisions)

- **CORE-08:** Extract `MetadataManager` to `shared/metadata_manager.py`, with
  `_BoundedLRUCache` **co-located in the same module** (not left orphaned in
  genizah_core.py).
- **CORE-09:** Extract `Indexer` to `shared/indexer.py`.
- **GUARD-04 (permanent facade):** genizah_core re-exports both as same-object
  `# noqa: F401` shims; `from genizah_core import MetadataManager/Indexer` must keep
  resolving to the same class objects. Never delete genizah_core shims.
- **GUARD-01 (no back-edges):** No module-level import from the new shared modules
  back into genizah_core. `MetadataManager` has heavy outbound deps (NLI circuit
  breaker, FJMS service, nli_crossref_service, csv_bank/library loading); any
  genizah_core dependency must be a **lazy/function-local import**, mirroring the
  Phase 123 pattern. The GUARD-01 AST test must stay green.
- **local_indexer retarget:** Retarget `shared/local_indexer.py`'s lazy back-edges
  into genizah_core helpers as part of this phase (per roadmap goal).
- **Behavior preservation (GUARD-02):** These named integration tests must pass
  unchanged: `tests/test_browse_synthetic.py`, `tests/test_audit_followup_2026_05_29.py`,
  `tests/test_api_nli_breaker_integration.py`; `build_index.py` must still resolve
  `Indexer.create_index`.

## Process guidance (carry forward from Phase 123 — see project_godfile_extraction_import_lesson memory)

- **Derive imports from the ACTUAL copied class bodies** — do NOT hand-prescribe import
  lists. `ruff F401` (excess) + full-suite-green-at-every-commit (missing, incl.
  method-runtime NameErrors a bare `import shared.X` smoke test misses) are the
  two-sided authoritative gate. Per-file ruff only — never repo-wide `ruff --fix`
  (strips noqa shims).
- **After execution, run the 3-round Codex code-review convergence loop** (plain
  `codex exec -s read-only "$(cat brief)" < /dev/null`) + a systematic base-vs-HEAD
  module-level name diff to catch any GUARD-04 facade-name drops (the technique that
  caught `UNIFIED_VARIANT_PAIRS`/`get_top_pairs`/`LIBRARY_CODES_HE` in Phase 123).
- **GUARD-03:** Before any deletion, check whether the 5 named source-scanning tests
  reference `MetadataManager`/`Indexer`; retarget to the new module location if so.
  (genizah_core keeps its shims this phase, so deletion may not apply — assess.)

## Plan decomposition (planner's call)

Likely 1 plan (both classes) or 2 (metadata_manager, then indexer). Both touch
genizah_core.py, so plans would serialize regardless. `MetadataManager` is the larger,
dependency-heavier extraction; `Indexer` is smaller. Planner decides via
file-conflict/dependency analysis.

## Open questions requiring user input

None.
