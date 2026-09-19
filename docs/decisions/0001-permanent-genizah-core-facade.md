# 0001 -- `genizah_core.py` is a permanent facade over `shared/`

- **Status:** binding
- **Date:** 2026-06-26 (v8.3.0 "God-File Decomposition", internal release)
- **Source:** the v8.3.0 milestone (`.planning/milestones/v8.3.0-ROADMAP.md`, success criterion
  SC#3) and the CLAUDE.md fact "Where code lives"; the contract text is the docstring of
  [tests/test_genizah_core_facade.py](../../tests/test_genizah_core_facade.py).

## Decision

`genizah_core.py` stays at the repository root as a thin facade re-exporting names from 14
`shared/` modules; 27 of those names, spanning 13 modules, are pinned by the identity test. The
contract is **identity**, not equivalence: `genizah_core.X is shared.Y.X`. The facade is never
removed. (Contrast: the `genizah_app` D1 shims from the same decomposition were temporary and were
retired in Phase 127.)

## Why

The decomposition moved roughly 12,000 lines out of one module. Dozens of importers -- both apps,
scripts, tests and external notebooks -- say `import genizah_core`. Keeping the old name valid made
the move a zero-behaviour change and keeps it one for anything outside the repository.

## Consequences

- New code imports from `shared.*` directly; the facade is for compatibility, not a convenience.
- Adding a name to the facade means adding it to the identity test in the same commit.
- The facade is one-directional: no `shared/` module may import `genizah_core` at module level.
  GUARD-01 ([tests/test_no_back_edges_core.py](../../tests/test_no_back_edges_core.py)) enforces
  this for the modules in its `EXTRACTED_MODULES` registry -- a new `shared/` module must be added
  there to be covered (the rule holds for all of `shared/` today; the registry-completeness check
  that makes registration unforgettable is part of the gates work that follows). Lazy imports
  inside function bodies are allowed and are not scanned.
- `genizah_core.py` imports neither PyQt6 nor NiceGUI, so both apps and the tests can import it.

## Supersedes

The pre-v8.3.0 arrangement in which `genizah_core.py` held the search engine, metadata, variants,
responsa and engines itself. The GSD codebase map of 2026-02-05 describes that arrangement and is
archived under `docs/archive/codebase-snapshot-2026-02-05/`.

## Enforced by

[tests/test_genizah_core_facade.py](../../tests/test_genizah_core_facade.py) (identity of all 27
names) and [tests/test_no_back_edges_core.py](../../tests/test_no_back_edges_core.py) (GUARD-01).
