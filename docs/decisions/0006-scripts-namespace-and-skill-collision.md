# 0006 -- `scripts/` is a flat namespace package; the skill's `scripts/` collides and is pinned

- **Status:** binding
- **Date:** 2026-05-08 (Phase 85, plan 85-03; commit `5188119c`)
- **Source:** [tests/conftest.py](../../tests/conftest.py), `_pin_root_scripts_namespace()`, and
  the `python_files` comment in [pyproject.toml](../../pyproject.toml).

## Decision

- `scripts/` has no `__init__.py`; it is a namespace package and stays flat.
- `skills/cairo-genizah-research/scripts/__init__.py` is a regular package with the same top-level
  name. `tests/conftest.py` binds `sys.modules['scripts']` to the root directory before collection
  so `from scripts import X` resolves to the root.
- `pyproject.toml` restricts collection to `test_*.py`, because the default also matched the skill's
  `smoke_test.py` and re-bound the name.
- **`skills/cairo-genizah-research/` never moves or renames.** It is a published contract: linked
  from `Help.html` in shipped installers and from `web/pages/ai_tools.py`, checked in CI by
  `tests/test_search_api_docs.py` (which reads the skill's `SKILL.md` by path), and exercised
  against the live deployment by `tests/test_skill_smoke.py`, which is opt-in (`SKILL_SMOKE=1`) and
  skips in CI.

## Why

Both trees must import as `scripts`; renaming the skill's package would break its published layout,
and adding `scripts/__init__.py` at the root would change every `python scripts/x.py` invocation's
import behaviour.

## Consequences

- New tooling goes in `scripts/` (flat); it may not be imported by `shared/`. The one known
  exception is `scripts/discovery_ids.py`, imported at module level by seven `shared/discovery_*`
  modules and by `web/discovery.py`; moving it under `shared/` is a Round 2 item.
- Scripts that import siblings do so through `sys.path` insertion; a `scripts/` regrouping is a
  Round 2 item with its own blast-radius list.

## Supersedes

Nothing.

## Enforced by

[tests/conftest.py](../../tests/conftest.py) (the pin) and
[pyproject.toml](../../pyproject.toml) (`python_files`).
