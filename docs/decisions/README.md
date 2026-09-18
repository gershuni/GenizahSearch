# Decisions

One page per binding decision the project has already made, so that a reader or an agent does not
re-open it and knows what it supersedes. Each page names its **source** -- the release, phase,
review or instruction-file fact where the decision was taken -- and **what enforces it**. A
decision with no enforcer is a convention; the pages say which is which.

| # | Decision | Enforced by |
|---|---|---|
| [0001](0001-permanent-genizah-core-facade.md) | `genizah_core.py` is a permanent facade over `shared/` | [tests/test_genizah_core_facade.py](../../tests/test_genizah_core_facade.py) |
| [0002](0002-sidecars-instead-of-a-backend-process.md) | Read-only data comes from local SQLite sidecars; Supabase holds community data; no standalone backend process | `scripts/check_docs.py` outdated-term scan |
| [0003](0003-bounded-test-runner.md) | Broad test selections run through the bounded runner, never one `pytest tests/` process | [tests/test_run_local_tests_gate.py](../../tests/test_run_local_tests_gate.py) |
| [0004](0004-flag-and-readiness.md) | An asset-backed feature flag is necessary but never sufficient: its surface is ANDed with a fail-closed readiness check (plain toggles are read directly) | the `*_available()` predicates and their tests |
| [0005](0005-document-classes.md) | Four document classes decide which file is authoritative for what | `scripts/check_docs.py` scan scope |
| [0006](0006-scripts-namespace-and-skill-collision.md) | `scripts/` is a flat namespace package; the skill's `scripts/` package collides and is pinned in `tests/conftest.py`; the skill directory never moves | [tests/conftest.py](../../tests/conftest.py) |
| [0007](0007-tests-stay-flat.md) | `tests/` stays flat apart from its existing lane subdirectories | convention (many tests compute the repo root from their own depth) |
| [0008](0008-root-assets-stay.md) | Data assets that `shared/config.py` or the PyInstaller spec resolve at the repository root stay there | [shared/config.py](../../shared/config.py), [GenizahSearchPro.spec](../../GenizahSearchPro.spec) |
| [0009](0009-masked-corpus-rule.md) | Restricted-source strings never appear in tracked files or shipped surfaces | [scripts/check_atlas_masking.py](../../scripts/check_atlas_masking.py) |

**Format** of a page: Status, Date, Source, Decision, Why, Consequences, Supersedes, Enforced by
(0009 omits Why and Supersedes on purpose: either would describe the masked corpora).

**Adding one:** copy the format, give it the next number, add a row here and a line in
[docs/DOCUMENTATION_INDEX.md](../DOCUMENTATION_INDEX.md). A decision that reverses one of these
gets its own page and marks the old one *Superseded by NNNN* -- pages are never edited into their
opposite.

The architecture these decisions shape is described in
[docs/architecture/OVERVIEW.md](../architecture/OVERVIEW.md).
