---
phase: 122-config-enabler
reviewed: 2026-06-25T00:00:00Z
depth: standard
files_reviewed: 4
files_reviewed_list:
  - shared/config.py
  - tests/test_no_back_edges_core.py
  - shared/session_persistence.py
  - genizah_core.py
findings:
  critical: 0
  warning: 0
  info: 3
  total: 3
status: clean
---

# Phase 122: Code Review Report

**Reviewed:** 2026-06-25T00:00:00Z
**Depth:** standard
**Files Reviewed:** 4
**Status:** clean

## Summary

Phase 122 extracts the `Config` class out of the ~12.5K-line `genizah_core.py`
god file into a new stdlib-only leaf module `shared/config.py`, behind a
permanent same-object re-export shim (`from shared.config import Config  # noqa: F401`).
The diff was reviewed against `31979cac~1..HEAD` and limited to the four
in-scope files. This is a faithful pure refactor — I found **no BLOCKER and no
WARNING defects**. The three INFO items below are documentation/clarity notes,
not behavioral risks.

Adversarial verification performed (all passed):

- **Same-object identity:** `genizah_core.Config is shared.config.Config` is
  `True` at runtime (confirmed live, not just via the CONFIG-01 test). All 23
  downstream files that do `from genizah_core import Config` or
  `genizah_core.Config` continue to resolve to the one class object.
- **The one intentional non-verbatim line:** the non-frozen
  `BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))`
  correctly climbs `shared/` → repo root. Live check: `Config.BASE_DIR ==
  C:\Genizahsearch` (repo root), `INTERNAL_DIR == BASE_DIR`, `LIBRARIES_CSV`
  and `FILE_V8` resolve under the repo root. The frozen branch is byte-for-byte
  unchanged and does not depend on `__file__`, so the module relocation does
  not affect EXE builds.
- **Load-time side effect preserved:** `os.makedirs(INDEX_DIR, exist_ok=True)`
  still runs at class-body evaluation; importing `shared.config` creates the
  index dir (verified `INDEX_DIR exists: True`). The except-fallback to the
  portable path is verbatim.
- **stdlib-only / no back-edge:** `shared/config.py` imports only `os` and
  `sys`; the `ctypes.wintypes` import stays lazy inside `_get_documents_dir`.
  GUARD-01 confirms no module-level `genizah_core` import.
- **`import sys` removal in `genizah_core.py`:** genuinely safe. The only
  remaining `sys` token is the deliberate local-variable shadow at
  `genizah_core.py:4021-4022` (`sys = re.search(...)`); all other hits are
  string literals (`'sys:'`, `f"sys:{sid}"`). No module-level `sys` use
  remains, and `genizah_core` imports cleanly.
- **`session_persistence.py` retarget:** one-line change to
  `from shared.config import Config`; `Config` resolves to the same object and
  `HISTORY_FILE` builds correctly. This is a genuine improvement — it removes a
  module-level back-edge from `session_persistence` into the heavy god file.
- **AST guard robustness:** probed try/if/with/for/while/match/else nesting
  (all flagged), method bodies and (async) function bodies (correctly NOT
  flagged), submodule forms (`import genizah_core.x`, `from genizah_core.x
  import y` — flagged), and prefix false-positives (`genizah_core_helper` —
  correctly NOT flagged). `EXTRACTED_MODULES` is non-empty so the parametrized
  test runs a real case. `ruff` clean on all three Python files; the new test
  file passes 5/5.

## Info

### IN-01: WORD_TOKEN_PATTERN rewritten as literal Hebrew chars instead of `\u` escapes

**File:** `shared/config.py:136`
**Issue:** The scope note states everything outside the `BASE_DIR` else-branch
must be a verbatim copy. `WORD_TOKEN_PATTERN` is textually different from the
original: the source had `r"[\w֐-׿\']+"` (explicit escapes) and the
extracted copy has `r"[\w֐-׿\']+"` (literal `U+0590`–`U+05FF` Hebrew
characters). This is **functionally identical** — `֐` is `֐` and `׿` is
`׿`, verified by decoding the runtime value (`repr` is
`"[\\w֐-׿\\']+"`). No behavior change, and the file is UTF-8 with a
docstring header, so the literals render fine. Worth a note so a future
"verbatim diff" audit does not mistake this for accidental drift. The escape
form is also marginally more robust against editor/encoding mishaps.
**Fix (optional):** Restore the explicit-escape form for byte-for-byte fidelity
with the tombstoned original:
```python
WORD_TOKEN_PATTERN = r"[\w֐-׿\']+"
```

### IN-02: Unreachable early-return guard in `_collect_stmt_lists`

**File:** `tests/test_no_back_edges_core.py:62-63`
**Issue:** `_collect_stmt_lists` opens with
`if isinstance(node, _LAZY_SCOPE): return`, but its only caller `_visit_stmts`
(line 127-131) already gates the call with
`isinstance(stmt, _IMPORT_TIME_COMPOUND) and not isinstance(stmt, _LAZY_SCOPE)`.
Since `_LAZY_SCOPE` (`FunctionDef`/`AsyncFunctionDef`) is disjoint from
`_IMPORT_TIME_COMPOUND`, the early-return branch is never reached from the
actual call site. It is harmless defensive code, not a bug. Either keep it as
documented defense-in-depth, or drop it to avoid implying `_collect_stmt_lists`
is called on lazy scopes.
**Fix (optional):** No action required; if removing dead branches, delete lines
62-63 and rely on the caller's guard.

### IN-03: Existing module-level back-edge in shared/exclusion_service.py is out of guard scope (by design)

**File:** `shared/exclusion_service.py:17`
**Issue:** `shared/exclusion_service.py` still has a module-level
`from genizah_core import normalize_shelfmark`, a real import-time back-edge
into the god file. GUARD-01 does NOT flag it because `EXTRACTED_MODULES`
contains only `shared/config.py`, and the guard intentionally enforces the
no-back-edge invariant only on modules that have been explicitly extracted.
This is correct and consistent with the phase's scoped design (the
decomposition of other `shared/` modules is tracked in Phases 123-127), but it
means the guard is an allowlist, not a blanket `shared/`-wide scan — a reader
could over-read its name. Pre-existing; not introduced by this phase.
**Fix:** None for Phase 122. As later phases extract `exclusion_service` (and
the other lazy-import back-edges in `local_indexer`, `nli_crossref_service`,
`search_serializer`, `shelfmark_bridge`), add them to `EXTRACTED_MODULES` so
the registry stays the single source of truth for the invariant.

---

_Reviewed: 2026-06-25T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
