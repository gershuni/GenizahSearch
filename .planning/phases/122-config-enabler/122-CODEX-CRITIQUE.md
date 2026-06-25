# Phase 122 — Codex Pre-Execution Review (2026-06-25)

> External adversarial review of `122-01-PLAN.md` via `codex exec -s read-only` (codex-cli 0.139.0).
> Verdict: **BLOCK** → 1 BLOCKER + 1 HIGH. Both verified against live code and **folded into the plan**
> (commit follows). The plan-checker (sonnet) PASSED the plan; Codex caught a real bug it, the
> researcher, and the pattern-mapper all missed — this is the plan↔code-drift class of finding.

## Finding 1 — [BLOCKER] `__file__` semantics change on move (FIXED in plan)

`Config` resolves its base path from `__file__` in the **non-frozen** branch:
`genizah_core.py:2347` → `BASE_DIR = os.path.dirname(os.path.abspath(__file__))`. In
`genizah_core.py` (repo root) this is the repo root. Copied **verbatim** into `shared/config.py`,
`__file__` is one directory deeper, so `BASE_DIR` → `…\Genizahsearch\shared` — wrong root.

Everything derived breaks: `FILE_V8` (Transcriptions.txt), `FILE_V7`, `_PORTABLE_INDEX_PATH`
(`genizah_core.py:2351-2355`), and the `INTERNAL_DIR`-derived `LIBRARIES_CSV`, `OXFORD_DB`,
`HELP_FILE`, `resource_path()` (`genizah_core.py:2408-2425`). Real callers depend on repo-root
paths: `build_index.py`, `genizah_app.py:1525` (icon), `genizah_app.py:26286` (Transcriptions.txt
check), `genizah_core.py:3896` (libraries.csv). The **CONFIG-01 identity test passes regardless**
(same class object), so this bug is invisible to every gate already run — it needs an explicit
path-resolution test.

**Fix (applied):** in `shared/config.py`, the only non-verbatim change is the non-frozen branch:
`BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))` (climb from `shared/`
back to repo root); keep `INTERNAL_DIR = BASE_DIR`. The **frozen** branch (`sys.executable` /
`sys._MEIPASS`) is `__file__`-independent and stays byte-for-byte. New test
`test_config_paths_resolve_to_repo_root` asserts `BASE_DIR/FILE_V8/FILE_V7/LIBRARIES_CSV` resolve
under repo root in non-frozen mode.

## Finding 2 — [HIGH] GUARD-01 false-negative for top-level guarded imports (FIXED in plan)

The plan specified flagging only **direct** `ast.iter_child_nodes(tree)` `Import`/`ImportFrom`
nodes. That misses import-time back-edges nested in top-level control flow —
`try: from genizah_core import X` / `if cond: import genizah_core` — which is a real repo pattern
(`genizah_core.py:67-72`). Those still execute at import time and ARE back-edges. Meanwhile the
imports that must stay ignored are **function-body** lazy imports (`shared/nli_crossref_service.py:365-376`,
`shared/local_indexer.py:3154`).

**Fix (applied):** scope-aware shallow traversal — start at `tree.body`, recurse into top-level
`If`/`Try`/`With` (and their `else`/`handlers`/`finalbody`) bodies, but **stop at**
`FunctionDef`/`AsyncFunctionDef`. Catches module-import-time back-edges; still ignores lazy
function-body imports. New guard unit tests assert a `try:`-guarded module-level back-edge is
caught and a function-body lazy import is not.

## Confirmed OK by Codex (no change)
- Shim placement after `genizah_core.py:63` is safe; the hard requirement is *before* `configure_logger()`
  dereferences `Config` (`genizah_core.py:2788-2790`, `LOGGER` at `:2809`).
- Top-level `shared/` back-edges are exactly `shared/exclusion_service.py:17` + `shared/session_persistence.py:32`;
  retargeting only `session_persistence` matches the locked D-01/D-02 scope.
- Locked decisions D-01..D-06 intact — these fixes serve the zero-behavior-change goal (the literal
  "verbatim copy" mechanic was the bug; D-05's *intent* is preserved).
