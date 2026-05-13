---
phase: 88-state-separation-by-deletion
plan: 03
type: execute
wave: 3
depends_on: [88-02]
files_modified:
  - web/state.py
  - web/api.py
  - web/search_api.py
  - tests/test_no_appstate_export_fields.py
  - tests/test_no_deleted_state_references.py
  - docs/OPEN_ISSUES.md
  - CLAUDE.md
autonomous: true
requirements: [STATE-01, STATE-02, STATE-03]
must_haves:
  truths:
    - "The 10 per-user fields are physically deleted from web/state.py:AppState.init() — direct attribute access on an AppState instance raises AttributeError."
    - "A runtime attr-absence test (tests/test_no_appstate_export_fields.py) parametrized over the 10 field names asserts AttributeError on every direct access — defensive against fixture-time regressions."
    - "A static AST/grep test (tests/test_no_deleted_state_references.py) scans web/ AND tests/ for any state.<deleted_field> attribute access OR setattr(state, '<deleted_field>', ...) call — fails CI if a regression slips through."
    - "The AST scanner has a seed-trap unit test proving it ignores comments/docstrings but catches actual code references."
    - "Stale docstring/comment mentions at web/api.py:1846-1848 (2026-05-12 cross-user fix) and web/search_api.py:1198-1199 (MUST NOT touch state.last_results) are refreshed to reflect the post-Phase-88 reality."
    - "web/export_state.py module docstring (Phase 88 D-16) is refreshed to remove the 'singleton state.* writes are intentionally left in place' paragraph — the writes are now gone."
    - "Plan-boundary green: pytest + ruff check + python scripts/check_docs.py all exit 0; full Phase 88 success criteria from ROADMAP.md verified."
  artifacts:
    - path: "web/state.py"
      provides: "AppState class with the 10 per-user export fields deleted; only meta_mgr/var_mgr/searcher/lab_engine/indexer/_local_lists_mgr/_user_lists_mgr remain"
      contains: "class AppState:"
      min_lines: 60
    - path: "tests/test_no_appstate_export_fields.py"
      provides: "Runtime attr-absence test parametrized over 10 field names"
      contains: "pytest.raises(AttributeError)"
    - path: "tests/test_no_deleted_state_references.py"
      provides: "Static AST scanner for state.<deleted_field> references in web/ + tests/"
      contains: "ast.Attribute"
  key_links:
    - from: "AppState.init()"
      to: "(deletion)"
      via: "removal of self.last_results, self.current_search_query, ..., self.parallels_search_meta lines"
      pattern: "self\\.(last_results|current_search_query|parallels_results)"
    - from: "tests/test_no_deleted_state_references.py"
      to: "AST node walker"
      via: "ast.Attribute(value=Name('state'), attr=<deleted_field>) detection + ast.Call setattr detection"
      pattern: "ast\\.(Attribute|Call)"
---

<objective>
Delete the 10 per-user mirror fields from web/state.py:AppState; install two complementary regression guards (runtime attr-absence test D-06 + static AST scanner D-07); refresh the stale docstring/comment mentions per D-16; verify full Phase 88 success criteria from ROADMAP.md.

Purpose: Close the cross-user state-leak attack surface permanently. The Plan 88-01 + 88-02 migration left the AppState fields write-orphaned and reader-free — they are now dead code. Deletion eliminates the singleton mirrors entirely. The two enforcement tests (runtime + static) ensure no future PR can dynamically re-create one of these attributes (AppState has no `__setattr__` guard, so a `state.last_results = ...` line in new code would silently re-create the attr without the static scanner).

Output: 7 modified files (3 source, 2 new tests, 2 docs). AppState class shrinks by 10 fields. Two new tests are added to the permanent CI surface. Stale comments updated. Full Phase 88 success criteria from ROADMAP.md proven via verification commands in Task 6.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@./CLAUDE.md
@.planning/STATE.md
@.planning/ROADMAP.md
@.planning/REQUIREMENTS.md
@.planning/phases/88-state-separation-by-deletion/88-CONTEXT.md
@web/state.py
@tests/test_no_raw_storage_access.py

<interfaces>
The 10 fields being deleted from web/state.py:AppState.init() (current lines 26-50):

```python
self.last_results: List[Dict[str, Any]] = []
self.current_search_query: str = ""
self.current_search_mode: str = "text"
self.current_search_gap: Optional[int] = None
self.last_filters_applied: Optional[Dict[str, Any]] = None
self.last_search_warnings: List[str] = []
self.last_selected_uids: Optional[List[str]] = None
self.parallels_results: List[Dict[str, Any]] = []
self.parallels_filtered: List[Dict[str, Any]] = []
self.parallels_search_meta: Optional[Dict[str, Any]] = None
```

Phase 87 lint scanner template (tests/test_no_raw_storage_access.py — full file already loaded into context for the AST scanner pattern):
- Uses ast.walk with parent tracking via _seen_inner_ids set keyed by id(node)
- Walks Call (for setattr / function calls), Subscript (for state['attr']), Attribute (for state.attr direct reads)
- File scan: REPO_ROOT / 'web' rglob *.py, plus the new scanner extends to REPO_ROOT / 'tests' rglob *.py
- Emits pytest.fail with all violations enumerated

AppState class is a singleton via __new__ pattern — but the `state` module-level instance at web/state.py:100 is what most callers import as `from web.state import state` or `from web.api import state`. The runtime D-06 test instantiates a fresh AppState() to bypass the singleton cache (calling AppState() returns the cached singleton, but we want to test the class shape regardless of cache state — see Task 2 for the exact pattern).
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Delete 10 per-user fields from web/state.py:AppState.init()</name>
  <files>web/state.py</files>
  <read_first>
    - web/state.py (full current file — 101 lines)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-04 plan ordering rationale; this task assumes Plans 88-01 + 88-02 have landed)
  </read_first>
  <action>
Delete lines 26-50 (the 10 per-user mirror fields) from web/state.py:AppState.init().

Before edit, web/state.py:AppState.init() contains 18 lines of assignments (lines 15-50): the 7 service-related fields (meta_mgr, var_mgr, searcher, lab_engine, indexer, _local_lists_mgr, _user_lists_mgr) plus the 10 per-user fields plus a few comment blocks.

After edit, AppState.init() contains ONLY:
- self.meta_mgr (line 15)
- self.var_mgr (line 16)
- self.searcher (line 17)
- self.lab_engine (line 18)
- self.indexer (line 19)
- self._local_lists_mgr (line 21)
- self._user_lists_mgr (line 24)

These 7 fields are NOT in scope for Phase 88 — they're either non-per-user (singletons by design) OR are scheduled for Phase 89 deletion (LISTS-01: `_user_lists_mgr`). Leave them alone.

Concrete deletion targets (lines 26-50 of current file):

```python
        self.last_results: List[Dict[str, Any]] = []
        self.current_search_query: str = ""
        # Phase 77: search-context echo for JSON export envelope (D-06) and Excel/Word filename
        # (current_search_query was declared above but never assigned — fixed by web/pages/search.py
        # in the same plan). These mirror the page-scoped search_state into the global singleton
        # so the stateful FastAPI download handlers in web/api.py can read them without coupling
        # to NiceGUI session lifecycle.
        self.current_search_mode: str = "text"
        self.current_search_gap: Optional[int] = None
        self.last_filters_applied: Optional[Dict[str, Any]] = None
        self.last_search_warnings: List[str] = []
        # Phase 77 gap-closure (Plan 06): per-row checkbox selection mirrored from
        # SearchUIState.selected_indices. None means "no selection — export full
        # state.last_results". Non-empty list means "filter state.last_results by
        # these uids in the export handlers". Empty list `[]` is treated as None
        # by the handlers (defensive — see web/api.py export handlers).
        self.last_selected_uids: Optional[List[str]] = None

        # Parallels results (for export functionality)
        self.parallels_results: List[Dict[str, Any]] = []
        self.parallels_filtered: List[Dict[str, Any]] = []
        # Phase 77: parallels-context echo for JSON export envelope (D-06)
        # Shape: {'source_text': str, 'chunk_size': int, 'mode': str, 'max_freq': Optional[float],
        #         'filters': Optional[dict], 'boundary_options': Optional[dict], 'warnings': List[str]}
        self.parallels_search_meta: Optional[Dict[str, Any]] = None
```

DELETE all 25 lines (10 assignments + 15 comment lines). After deletion, the gap between `self._user_lists_mgr = None` and `@property def lists_mgr(self):` collapses to a single blank line.

Import cleanup: AppState.init() no longer references `List`, `Dict`, `Any` for the deleted fields. But the existing 7 fields (meta_mgr etc. use `Optional[MetadataManager]` etc.) and the `lists_mgr` property may still reference these typing imports. Check `from typing import Optional, List, Dict, Any` at line 1 — after deletion, `List`, `Dict`, `Any` may be unused. Run `python -m ruff check web/state.py` to detect; if F401 is reported on any of these, remove from the import. Conservative: keep them all (some are used in init or property type hints). The executor decides based on ruff output.

Replace the deleted block with a one-line comment placeholder so future readers know what was removed:

```python
        # Per-user export state migrated to web.export_state (Phase 88, 2026-05-13).
        # See .planning/phases/88-state-separation-by-deletion/ for migration history.
```

After Task 1 the file should be ~76 lines (down from 101).
  </action>
  <verify>
    <automated>python -c "from web.state import AppState; s = AppState(); import pytest; assert not hasattr(s, 'last_results'); assert not hasattr(s, 'current_search_query'); assert not hasattr(s, 'parallels_results'); assert not hasattr(s, 'parallels_search_meta'); print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `grep -nE "^\\s+self\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\\s*[:=]" web/state.py` returns 0 matches.
    - `python -c "from web.state import AppState; s = AppState(); assert not hasattr(s, 'last_results')"` exits 0.
    - `python -c "from web.state import AppState; s = AppState(); assert not hasattr(s, 'parallels_search_meta')"` exits 0.
    - `python -c "from web.state import state; assert state.meta_mgr is None or state.meta_mgr is not None"` exits 0 (sanity: surviving fields still accessible).
    - `python -c "import ast; ast.parse(open('web/state.py', encoding='utf-8').read())"` exits 0.
    - `python -m ruff check web/state.py` exits 0.
  </acceptance_criteria>
  <done>The 10 per-user fields physically deleted from AppState.init(); surviving 7 fields untouched; AppState() instance has no direct attribute path to any deleted field.</done>
</task>

<task type="auto">
  <name>Task 2: Create tests/test_no_appstate_export_fields.py (D-06 runtime guard)</name>
  <files>tests/test_no_appstate_export_fields.py</files>
  <read_first>
    - web/state.py (post-Task-1)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-06)
  </read_first>
  <action>
Create the runtime attribute-absence regression test. Parametrize over the 10 deleted field names; assert that direct getattr on a fresh AppState instance raises AttributeError. This guards against direct re-instantiation regressions (e.g., a future PR adds back `self.last_results = []` in init).

CRITICAL caveat about the AppState singleton: `AppState.__new__` returns the cached singleton if already initialized. To test the CLASS shape regardless of cached state, the test must check direct attribute presence via `hasattr` OR force a fresh init via `AppState._instance = None` then `AppState()` then assert.

Use the `hasattr` form — simpler, doesn't mutate global singleton state. The contract being tested is: "After Phase 88, the AppState class does NOT declare these attributes in its __init__/init body, so a fresh-or-cached instance has no attribute with this name unless something dynamically set it."

Concrete file content (write verbatim):

```python
"""Phase 88 D-06 — runtime attribute-absence regression test.

Asserts the 10 per-user export-state fields deleted from web/state.py:AppState
in Phase 88 cannot be accessed directly on an AppState instance.

This is the RUNTIME guard. The companion STATIC guard
(tests/test_no_deleted_state_references.py) catches `state.last_results = ...`
re-introductions at AST-scan time even when the dynamic attr would not yet
have been read.

Both tests are intentionally in Phase 88 — they do not belong in Phase 92
SWEEP-01 because SWEEP-01 audits raw `app.storage.user.get/pop/[]` access,
which is a different class of regression.
"""
import pytest

from web.state import AppState


DELETED_FIELDS = [
    'last_results',
    'current_search_query',
    'current_search_mode',
    'current_search_gap',
    'last_filters_applied',
    'last_search_warnings',
    'last_selected_uids',
    'parallels_results',
    'parallels_filtered',
    'parallels_search_meta',
]


@pytest.mark.parametrize('field', DELETED_FIELDS)
def test_appstate_does_not_have_deleted_field(field):
    """Direct attribute access on AppState() raises AttributeError for each
    Phase-88-deleted per-user mirror field.

    The AppState class is a __new__-based singleton, so AppState() returns
    the cached instance. We test hasattr (not isinstance / getattr-with-default
    / pytest.raises) because hasattr correctly reports "this attribute does
    not exist on the instance or its class" — exactly the contract we want
    to enforce post-deletion.

    If a future PR re-introduces `self.last_results = []` in AppState.init()
    OR sets the attribute dynamically anywhere before this test runs (e.g.
    `state.last_results = [...]` in fixture setup), the corresponding
    parametrized test will fail with a clear message naming the field.
    """
    instance = AppState()
    assert not hasattr(instance, field), (
        f"AppState.{field} still exists. Phase 88 STATE-01 deleted this field; "
        f"it must not be re-added directly or dynamically. "
        f"See .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md D-06."
    )


def test_appstate_still_has_non_deleted_fields():
    """Sanity check: surviving fields (Phase 88 left untouched) are still present.

    Confirms Task 1's deletion was surgical — did not accidentally remove
    non-export-state attributes.
    """
    instance = AppState()
    survivors = [
        'meta_mgr', 'var_mgr', 'searcher', 'lab_engine', 'indexer',
        '_local_lists_mgr', '_user_lists_mgr',
    ]
    for attr in survivors:
        assert hasattr(instance, attr), (
            f"AppState.{attr} should still exist after Phase 88 deletion. "
            f"Task 1 was supposed to be surgical."
        )
```

11 tests total: 10 parametrized + 1 sanity-survivors test.
  </action>
  <verify>
    <automated>python -m pytest tests/test_no_appstate_export_fields.py -v --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `test -f tests/test_no_appstate_export_fields.py && echo OK` returns OK.
    - `python -m pytest tests/test_no_appstate_export_fields.py -v` exits 0 with 11 tests passing.
    - `grep -c "DELETED_FIELDS = " tests/test_no_appstate_export_fields.py` returns 1 (single list of 10 names).
    - `grep -c "@pytest.mark.parametrize" tests/test_no_appstate_export_fields.py` returns 1.
    - `python -m ruff check tests/test_no_appstate_export_fields.py` exits 0.
  </acceptance_criteria>
  <done>tests/test_no_appstate_export_fields.py created; 11 tests parametrize over 10 deleted fields + 1 survivor sanity check; all pass; D-06 runtime guard installed.</done>
</task>

<task type="auto">
  <name>Task 3: Create tests/test_no_deleted_state_references.py (D-07 static AST scanner)</name>
  <files>tests/test_no_deleted_state_references.py</files>
  <read_first>
    - tests/test_no_raw_storage_access.py (full file — canonical AST scanner pattern, especially _walk_attribute_chain and _StorageAccessVisitor patterns)
    - web/state.py (post-Task-1)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-07, D-08)
  </read_first>
  <action>
Create the static AST scanner that walks all .py files under web/ AND tests/ and flags any reference matching:
1. `<X>.last_results` (or any of the other 9 deleted fields) where `<X>` is an identifier matching the names typically bound to `AppState()` instances (`state`, `app_state`).
2. `setattr(<X>, 'last_results', ...)` or any of the 9 other field names as the second arg.

Per CONTEXT.md "Claude's Discretion": AST is preferred over pure regex (mirrors test_no_raw_storage_access.py Phase 87 pattern). Per D-07: scan both web/ AND tests/ (Plan 88-01 + 88-02 already cleaned these directories; the scanner is a forward-going guard).

Concrete file content (write verbatim, modeled on test_no_raw_storage_access.py shape):

```python
"""Phase 88 D-07 — static AST scanner for deleted AppState reference regressions.

Walks web/ AND tests/ looking for references to the 10 per-user export-state
fields deleted from web/state.py:AppState in Phase 88. Catches:

  1. Attribute access:  state.last_results, state.parallels_results, etc.
                        Also app_state.last_results (alternate binding name).
  2. setattr calls:     setattr(state, 'last_results', ...).
  3. getattr calls:     getattr(state, 'last_results', ...).

This is the STATIC guard. The companion RUNTIME guard
(tests/test_no_appstate_export_fields.py) asserts AttributeError on direct
instance access. The static check survives forever as a CI guard against
dynamic re-introductions that runtime tests might miss when order-dependent.

Per Phase 88 D-07: scan both web/ and tests/ — Plan 88-01 + 88-02 cleaned
both directories; this scanner is the forward-going regression guard.
Per D-08: this test lives in Phase 88, NOT deferred to Phase 92 SWEEP-01.
"""
import ast
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
SCAN_DIRS = [REPO_ROOT / 'web', REPO_ROOT / 'tests']

# The 10 fields deleted from web/state.py:AppState in Phase 88.
DELETED_FIELDS = frozenset({
    'last_results',
    'current_search_query',
    'current_search_mode',
    'current_search_gap',
    'last_filters_applied',
    'last_search_warnings',
    'last_selected_uids',
    'parallels_results',
    'parallels_filtered',
    'parallels_search_meta',
})

# Identifier names that commonly bind to AppState instances in this codebase.
# Conservative: only catch state.X / app_state.X — does NOT catch self.X,
# which would mass-false-positive (every class with a `last_results` attr
# in any other module would trip). The AppState class itself defines none
# of these post-Phase-88, so self.X inside AppState is safe to skip.
STATE_BINDING_NAMES = frozenset({'state', 'app_state'})

# Files exempt from this scan (the test itself, the AppState class definition,
# anything that intentionally references these names for migration history).
EXEMPT_FILES = frozenset({
    'tests/test_no_deleted_state_references.py',
    'tests/test_no_appstate_export_fields.py',  # parametrizes over DELETED_FIELDS literally
})


class _DeletedStateAccessVisitor(ast.NodeVisitor):
    """Visit Attribute / Call nodes; flag references to deleted AppState fields."""

    def __init__(self, source: str):
        self.source = source
        self.violations: list[tuple[int, str]] = []

    def _record(self, node):
        seg = ast.get_source_segment(self.source, node) or ''
        self.violations.append((node.lineno, seg))

    def visit_Attribute(self, node: ast.Attribute):
        # Catches `<name>.last_results` where <name> is a binding to an AppState
        # instance. We do NOT recurse into the value child for the matching
        # detection because we want the OUTERMOST attribute on `state` —
        # `state.last_results.append(...)` is one violation (the .last_results
        # attribute access), not two.
        if (
            isinstance(node.value, ast.Name)
            and node.value.id in STATE_BINDING_NAMES
            and node.attr in DELETED_FIELDS
        ):
            self._record(node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        # Catches setattr(state, 'last_results', ...) and getattr(state, 'last_results', ...).
        if isinstance(node.func, ast.Name) and node.func.id in ('setattr', 'getattr'):
            if len(node.args) >= 2:
                arg0, arg1 = node.args[0], node.args[1]
                if (
                    isinstance(arg0, ast.Name)
                    and arg0.id in STATE_BINDING_NAMES
                    and isinstance(arg1, ast.Constant)
                    and isinstance(arg1.value, str)
                    and arg1.value in DELETED_FIELDS
                ):
                    self._record(node)
        self.generic_visit(node)


def _scan_file(path: Path, source: str) -> list[tuple[int, str]]:
    tree = ast.parse(source, filename=str(path))
    visitor = _DeletedStateAccessVisitor(source)
    visitor.visit(tree)
    return visitor.violations


def test_scanner_detects_synthetic_attribute_access():
    """Seed-trap: confirm the visitor catches a deliberately bad access pattern.

    Includes 3 forms: direct attribute, setattr, getattr. All must be reported.
    """
    synthetic = (
        "from web.state import state\n"
        "def bad_attribute():\n"
        "    return state.last_results\n"
        "def bad_setattr():\n"
        "    setattr(state, 'parallels_results', [])\n"
        "def bad_getattr():\n"
        "    return getattr(state, 'current_search_query', '')\n"
    )
    tree = ast.parse(synthetic)
    visitor = _DeletedStateAccessVisitor(synthetic)
    visitor.visit(tree)
    assert len(visitor.violations) >= 3, (
        f"Expected at least 3 violations (attr, setattr, getattr), got "
        f"{len(visitor.violations)}: {visitor.violations}"
    )


def test_scanner_ignores_strings_and_comments():
    """Seed-trap: deliberate field-name mentions inside comments/strings/docstrings
    must NOT be reported. Only real code references count.
    """
    synthetic = (
        "from web.state import state  # state.last_results is gone post-Phase-88\n"
        '"""Module docstring mentioning state.last_results historically."""\n'
        "def innocent():\n"
        "    msg = 'state.parallels_results was deleted'\n"
        "    return state.meta_mgr  # surviving field is fine\n"
    )
    tree = ast.parse(synthetic)
    visitor = _DeletedStateAccessVisitor(synthetic)
    visitor.visit(tree)
    assert visitor.violations == [], (
        f"Scanner false-positive on string/comment mentions: {visitor.violations}"
    )


def test_no_deleted_state_references_in_web_and_tests():
    """Production guard: no Python file under web/ or tests/ references any of
    the 10 Phase-88-deleted AppState fields via state.X / app_state.X /
    setattr(state, ...) / getattr(state, ...).
    """
    violations = []
    for scan_dir in SCAN_DIRS:
        for path in scan_dir.rglob('*.py'):
            rel = path.relative_to(REPO_ROOT).as_posix()
            if rel in EXEMPT_FILES:
                continue
            source = path.read_text(encoding='utf-8')
            try:
                file_violations = _scan_file(path, source)
            except SyntaxError as e:
                pytest.fail(f"AST parse failed for {rel}: {e}")
            for lineno, seg in file_violations:
                violations.append(f"{rel}:{lineno}: {seg or '<no segment>'}")
    if violations:
        msg = (
            "References to Phase-88-deleted AppState fields found:\n  "
            + "\n  ".join(violations[:50])
            + f"\n\nTotal violations: {len(violations)}"
            + "\n\nFix: route through web.export_state helpers (see Phase 88 D-04)."
            + "\nThe 10 deleted fields: " + ", ".join(sorted(DELETED_FIELDS))
        )
        pytest.fail(msg)
```

Edge cases the executor MUST verify after writing:

1. `tests/test_no_appstate_export_fields.py` references the 10 field names AS PARAMETRIZE ARGUMENTS (strings inside a list literal). The scanner must NOT flag this as a violation because (a) the strings aren't passed to setattr/getattr against `state`, and (b) the file is in `EXEMPT_FILES`. Confirm by running pytest and ensuring `test_no_deleted_state_references_in_web_and_tests` passes.

2. `tests/test_export_state_selection.py` (post-Plan-88-02) might still have a fixture name like `session_with_5_results` that mentions "5 results" — the word "results" is not the field name `last_results`, so no false positive. The scanner only fires on the EXACT field names from DELETED_FIELDS.

3. Comments/docstrings/strings mentioning `state.last_results` HISTORICALLY — caught by `test_scanner_ignores_strings_and_comments` seed trap. The AST scanner sees ast.Attribute nodes only, not Comment tokens or Constant strings — so the trap is structural, not behavioral.

4. If Task 1 left a one-line comment placeholder mentioning the deleted fields (e.g., "Per-user export state migrated to web.export_state (Phase 88, 2026-05-13)"), the scanner should ignore it because comments aren't AST nodes — confirmed by the seed-trap.

5. If a file IS exempt (e.g., test_no_appstate_export_fields.py), it must contain the strings `last_results`, `parallels_results`, etc. — that's the whole point of D-06. EXEMPT_FILES handles this.
  </action>
  <verify>
    <automated>python -m pytest tests/test_no_deleted_state_references.py -v --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `test -f tests/test_no_deleted_state_references.py && echo OK` returns OK.
    - `python -m pytest tests/test_no_deleted_state_references.py -v` exits 0 with all 3 tests passing (synthetic-attribute-access seed-trap, ignores-strings-and-comments seed-trap, production scan).
    - `grep -c "DELETED_FIELDS = frozenset" tests/test_no_deleted_state_references.py` returns 1.
    - `grep -c "STATE_BINDING_NAMES" tests/test_no_deleted_state_references.py` returns at least 2 (1 declaration + 1+ use).
    - `grep -c "EXEMPT_FILES" tests/test_no_deleted_state_references.py` returns at least 2.
    - `python -m ruff check tests/test_no_deleted_state_references.py` exits 0.
  </acceptance_criteria>
  <done>tests/test_no_deleted_state_references.py created; 3 tests pass (seed-trap attribute, seed-trap strings/comments, production scan); D-07 static guard installed; scanner is now permanent CI guard.</done>
</task>

<task type="auto">
  <name>Task 4: Refresh stale docstring/comment mentions (D-16)</name>
  <files>web/api.py, web/search_api.py, web/export_state.py</files>
  <read_first>
    - web/api.py (lines 1843-1855)
    - web/search_api.py (lines 1190-1210)
    - web/export_state.py (post-Plan-88-02 — full file; the module docstring already updated by Plan 88-02 Task 1, but verify)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-16)
  </read_first>
  <action>
Update three stale text artifacts that reference the deleted singleton mirrors as if they still exist.

Site 1 — web/api.py:1846-1848 (the 2026-05-12 cross-user-fix comment in export_excel).

BEFORE:
```
@target_app.get('/api/export/excel')
def export_excel():
    """Export search results to Excel format using unified export service."""
    # Per-session read (web.export_state). The previous singleton path
    # (state.last_results / state.current_search_query) leaked User A's
    # query name into User B's xlsx filename across separate devices.
    from web.export_state import get_search_export
    payload = get_search_export()
```

AFTER (refresh the comment to historical-yet-current wording — DO NOT delete the historical context):
```
@target_app.get('/api/export/excel')
def export_excel():
    """Export search results to Excel format using unified export service."""
    # Reads per-session payload via web.export_state (Phase 88, 2026-05-13).
    # Historical context: the previous singleton path (state.last_results /
    # state.current_search_query, deleted in Phase 88 STATE-01) leaked
    # User A's query name into User B's xlsx filename across separate
    # devices (2026-05-12 incident). Phase 88 removed the singleton mirrors
    # entirely; export state is now per-session via app.storage.user.
    from web.export_state import get_search_export
    payload = get_search_export()
```

Site 2 — web/search_api.py:1198-1201 (the "MUST NOT touch state.last_results" docstring in the parallels handler).

BEFORE:
```
        Statelessness D-20: handler MUST NOT touch state.last_results /
        state.parallels_results / state.current_search_query / app.storage /
        request.cookies.
        """
```

AFTER (rewrite the rule statement to reflect post-Phase-88 reality):
```
        Statelessness D-20: handler MUST NOT touch the per-session export
        state (web.export_state) or app.storage / request.cookies — handlers
        are stateless and respond purely from request body + corpus indexes.
        Historical note: pre-Phase-88, the rule named the AppState singleton
        fields (state.last_results / state.parallels_results /
        state.current_search_query) which Phase 88 deleted; the rule now
        reads against the per-session payload helper surface instead.
        """
```

Site 3 — web/export_state.py module docstring (verify post-Plan-88-02 state).

Plan 88-02 Task 1 already rewrote the docstring. Verify it does NOT contain the paragraph "The singleton ``state.*`` writes are intentionally left in place for the moment; they are dead code once the export handlers stop reading them, and will be removed in a follow-up cleanup phase." — that paragraph from the pre-Phase-88 docstring is stale. If it appears in the post-88-02 file, delete it. If Plan 88-02 already removed it, this site is a no-op.

Expected post-task state of web/export_state.py module docstring (verbatim from Plan 88-02 Task 1):
- References Phase 87 chokepoint helpers
- References Phase 88 removed `_TEST_BACKEND` and `_backend()`
- References D-11 isinstance guard and D-12 copy-on-update
- Does NOT contain "singleton state.* writes are intentionally left in place"

If verification finds the stale paragraph, delete it.

Site 4 — sanity check: walk through any other comments mentioning the 10 deleted fields in `web/api.py` AND `web/pages/search.py` AND `web/pages/parallels.py` AND `web/pages/search_results.py`. Use grep:

```
grep -nE "state\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)" web/api.py web/pages/search.py web/pages/parallels.py web/pages/search_results.py
```

For each match, evaluate: is it a comment/docstring mention OR an actual code reference?
- Code reference: the static scanner from Task 3 will already catch it and fail the build. If found, that's a Plan 88-01 miss — go back to Plan 88-01 and migrate.
- Comment/docstring mention: rewrite to make it clearly historical ("Pre-Phase-88, this path read from state.last_results; now reads from web.export_state.get_search_export()." style).

This is editorial cleanup; aim for clarity, not bulk rewrites. Comments that already use the past tense ("previous singleton path", "originally wrote to state.last_results") are fine and need no edit.
  </action>
  <verify>
    <automated>python -m pytest tests/test_no_deleted_state_references.py -v --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `grep -n "previous singleton path" web/api.py` returns 0 matches (replaced with refreshed comment per Site 1).
    - `grep -nE "MUST NOT touch state\\.last_results" web/search_api.py` returns 0 matches (replaced per Site 2).
    - `grep -n "singleton .*state.* writes are intentionally left in place" web/export_state.py` returns 0 matches (Plan 88-02 should have already cleaned this; verify).
    - `python -m pytest tests/test_no_deleted_state_references.py -v` exits 0 (static scanner still green — any remaining code references would have failed it).
    - `python -m ruff check web/api.py web/search_api.py web/export_state.py` exits 0.
  </acceptance_criteria>
  <done>3 docstring/comment sites refreshed; historical context preserved as past-tense wording; static scanner still green; no actual code references to the deleted fields remain.</done>
</task>

<task type="auto">
  <name>Task 5: Update docs/OPEN_ISSUES.md and CLAUDE.md "Recently Changed"</name>
  <files>docs/OPEN_ISSUES.md, CLAUDE.md</files>
  <read_first>
    - docs/OPEN_ISSUES.md (full file — locate Phase 88-relevant entries; the 2026-05-12 cross-user xlsx leak entry, if present, is now fully closed by Phase 88)
    - CLAUDE.md (the "Recently Changed" section, in particular the existing v7.12 Path B Phase 87 entry)
    - .planning/STATE.md (post-Phase-87 close; may have a Phase 88 placeholder to update)
  </read_first>
  <action>
Per CLAUDE.md "Documentation Maintenance" contract:

Part A — docs/OPEN_ISSUES.md.

Read docs/OPEN_ISSUES.md and search for any open issue that Phase 88 closes. Candidates:
- The 2026-05-12 cross-user xlsx leak issue (if logged as open)
- Any "AppState singleton sprawl" or "_TEST_BACKEND production-shim" entry
- Any "parallels_source_text legacy fallback" or "cross-user state leak" entry

For each match: change the status icon from "❌ Open" to "✅ Fixed (2026-05-13)" and append a note: "Phase 88 STATE-01..06 deleted singleton mirrors; per-session export state via web.export_state."

If no relevant entry exists: ADD a new entry under the appropriate section (likely "Cross-User / Multitenancy") with status `✅ Fixed (2026-05-13)` documenting the resolution. Format:

```
| Cross-user export state leak (2026-05-12 incident) | ✅ Fixed (2026-05-13) | Phase 88 deleted the 10 per-user singleton fields from web.state.AppState; export state now per-session via web.export_state routed through Phase 87 safe_storage chokepoint. Static AST scanner + runtime attr-absence test installed as permanent CI guards. |
```

Update the "Last Updated" timestamp at the file footer to 2026-05-13.

Part B — CLAUDE.md "Recently Changed" section.

Add a new entry at the top of the "Recently Changed" section. Format (match the existing v7.12 Phase 87 entry style — terse, factual, version-tagged):

```
- May 2026: v7.12 Path B Phase 88 (State Separation by Deletion) — internal milestone, not a release. Second phase of v7.12 Multitenant Architecture (Path B) refactor. Deletes the 10 per-user export-state singleton mirror fields from `web/state.py:AppState` (`last_results`, `current_search_query`, `current_search_mode`, `current_search_gap`, `last_filters_applied`, `last_search_warnings`, `last_selected_uids`, `parallels_results`, `parallels_filtered`, `parallels_search_meta`). Plan 88-01 migrated 13 writer sites across `web/pages/search.py`, `web/pages/search_results.py`, `web/pages/parallels.py` from `state.X = value` to local-variable threading through the existing `web.export_state.set_*` / `update_*` / `clear_*` calls (Codex round-5 catch: reorder of original plan ordering required because `set_search_export(...)` calls passed `state.current_search_gap` etc. as kwargs two lines below their assignments). Plan 88-02 rewrote `web/export_state.py` to route through Phase 87 `web.safe_storage` chokepoint helpers, deleted the `_TEST_BACKEND` production-shim + `_backend()` helper (per D-09 + Phase 87 chokepoint discipline), hardened `update_*` functions with `isinstance(payload, dict)` guard (D-11) + copy-on-update (D-12), folded `parallels_source_text` into the `set_parallels_export(meta={'source_text': ...})` payload (D-13), deleted the reader-side fallback at `web/api.py:1928-1931, 1962-1964, 2063-2066` (D-14), rewrote 4 affected test files (`test_export_cross_user_isolation`, `test_export_state_selection`, `test_api_export_json`, `test_api_legacy_unchanged`) to `monkeypatch.setattr('web.safe_storage.app', ...)` pattern (D-01, D-02) — deleting `_StateProxy` wrapper, dropping all `state.X =` fixture setup, adding a source_text cross-user leak regression test (D-15), and deleted the `web/export_state.py` entry from `.planning/phase87_storage_allowlist.yaml` (allowlist count: 4 → 3 entries). Plan 88-03 deleted the 10 fields from `AppState.init()`, installed two permanent CI regression guards: runtime attr-absence test `tests/test_no_appstate_export_fields.py` (11 tests = 10 parametrized + 1 survivor sanity per D-06) and static AST scanner `tests/test_no_deleted_state_references.py` (D-07 — walks `web/` + `tests/` for `state.<deleted_field>` / `setattr(state, ...)` / `getattr(state, ...)`, 3 tests = 2 seed-traps + 1 production scan), refreshed stale docstring/comment mentions per D-16 at `web/api.py:1846-1848` and `web/search_api.py:1198-1201`. Codex round-5 review reshaped plan ordering (locals-first instead of fields-first per D-04 + D-05) to eliminate the data-loss window where deletion-first ordering would feed stale defaults into `set_search_export(...)` kwargs. Full pytest suite green at each plan boundary (D-05). All 6 STATE-XX requirements satisfied. Zero user-visible behavior change. Web-only milestone — desktop unaffected. Hand-off chain: Phase 89 deletes `UserListsManager._cache_entry` singleton (LISTS-01..04); Phase 90 deletes `_client_cache` / `_session_locks` / `_CLIENT_CACHE_TTL` and the auth `_app.storage.user` allowlist entry (AUTHC-01..05); Phase 91 deletes the `web/auth_state.py` + `web/main.py` OAuth allowlist entries (AUTHW-01..06); Phase 92 final sweep + acceptance (SWEEP-01..06). (web)
```

This entry mirrors the Phase 87 entry shape and length. The full justification is intentional — these CLAUDE.md entries are the milestone history for future maintainers and are the canonical source of "what changed and why" outside the PR descriptions.

Part C — .planning/STATE.md (optional, only if a placeholder exists).

Read .planning/STATE.md and search for any "Phase 88" placeholder. If found, update with the actual landed status (plans complete, requirements satisfied). If no placeholder, skip.
  </action>
  <verify>
    <automated>python scripts/check_docs.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -nE "Phase 88|State Separation by Deletion" docs/OPEN_ISSUES.md` returns at least 1 match.
    - `grep -nE "Phase 88|State Separation by Deletion" CLAUDE.md` returns at least 1 match.
    - `python scripts/check_docs.py` exits 0.
    - `grep -nE "Last Updated.*2026-05-13" docs/OPEN_ISSUES.md` returns at least 1 match (footer updated).
  </acceptance_criteria>
  <done>docs/OPEN_ISSUES.md updated with Phase 88 closure; CLAUDE.md "Recently Changed" has a new Phase 88 entry mirroring the Phase 87 entry style; check_docs.py green.</done>
</task>

<task type="auto">
  <name>Task 6: Plan-boundary green + full Phase 88 success-criteria verification</name>
  <files></files>
  <read_first>
    - .planning/ROADMAP.md (Phase 88 5 success criteria)
    - .planning/REQUIREMENTS.md (STATE-01..06)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-05 plan-boundary discipline)
  </read_first>
  <action>
Run the full verification matrix proving Phase 88 success criteria from ROADMAP.md are all met.

Commands (each must exit 0):

1. `python -m pytest -q` (full suite — target: 1879 baseline + ~14 new tests from this phase (11 from test_no_appstate_export_fields + 3 from test_no_deleted_state_references + 1+ from D-15) = ~1894+ passed / ~20 skipped).

2. `python -m ruff check .` (full repo lint clean).

3. `python scripts/check_docs.py` (docs health check clean post-Task-5 updates).

ROADMAP.md Phase 88 success criteria verification (run these commands and confirm each returns the expected output):

SC#1 — "Static grep of web/state.py:AppState returns zero matches for the 10 deleted per-user fields":
- `grep -nE "^\\s+self\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\\s*[:=]" web/state.py`
- Expected: 0 matches.

SC#2 — "A user opens two concurrent browser sessions, searches in session A, then triggers an xlsx export in session B; the exported file contains session B's result set":
- Test proxy: `python -m pytest tests/test_export_cross_user_isolation.py::test_two_sessions_get_independent_filenames -v`
- Expected: 1 test passes. Note this is sequential simulation per CONTEXT.md D-03; true concurrent coverage deferred to Phase 92 SWEEP-05.

SC#3 — "Static grep of web/export_state.py returns zero matches for _TEST_BACKEND":
- `grep -n "_TEST_BACKEND" web/export_state.py`
- Expected: 0 matches.
- Additionally: `grep -rn "_TEST_BACKEND" .` (entire repo) returns 0 matches.

SC#4 — "tests/test_export_cross_user_isolation.py passes and asserts cross-user isolation directly against per-session storage, with no reference to _TEST_BACKEND":
- `python -m pytest tests/test_export_cross_user_isolation.py -v` exits 0.
- `grep -n "_TEST_BACKEND" tests/test_export_cross_user_isolation.py` returns 0 matches.

SC#5 — "tests/test_export_state_selection.py, tests/test_api_export_json.py, and tests/test_api_legacy_unchanged.py all pass after dropping any state.* setup":
- `python -m pytest tests/test_export_state_selection.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -v` exits 0.
- `grep -nE "state\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\\s*=" tests/test_export_state_selection.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py` returns 0 matches.

STATE-XX requirement satisfaction matrix:
- STATE-01 (10 fields deleted): SC#1 verifies.
- STATE-02 (writers route through export_state): `grep -nE "state\\.(last_results|current_search_query|<...10 fields...>)\\s*=" web/` returns 0 matches.
- STATE-03 (readers route through export_state): `grep -n "parallels_source_text" web/api.py` returns 0 matches (Plan 88-02 deleted the only remaining reader-side reference).
- STATE-04 (_TEST_BACKEND removed): SC#3 verifies.
- STATE-05 (test_export_cross_user_isolation rewritten): SC#4 verifies.
- STATE-06 (3 other tests rewritten): SC#5 verifies.

Static AST scanner final check:
- `python -m pytest tests/test_no_deleted_state_references.py::test_no_deleted_state_references_in_web_and_tests -v` exits 0.
- This is the FINAL gate: if any file under web/ or tests/ still has a runtime reference to one of the 10 deleted fields, this test fails. By extension, the test passing proves SC#1 PLUS extends to dynamic references that SC#1 alone would miss.

Phase 87 lint scanner final check (should still be green):
- `python -m pytest tests/test_no_raw_storage_access.py -v` exits 0 with all 6 tests passing.
- `grep -c "^  - file:" .planning/phase87_storage_allowlist.yaml` returns 3 (auth_state, main, supabase_client — web/export_state.py entry deleted by Plan 88-02 Task 6).

Sanity grep — no stranded references:
- `grep -rn "_TEST_BACKEND\|export_state\\._backend\|_StateProxy" .` returns 0 matches across the entire repo.

If ANY of the above commands fails or returns unexpected output, the task is NOT done. Diagnose and fix; do not declare the plan complete with a red gate.

Final commit hint (the executor performs the commit after all acceptance criteria pass): single commit message body referencing all 5 ROADMAP success criteria + 6 STATE-XX requirements + Codex round-5 plan-ordering refinement.
  </action>
  <verify>
    <automated>python -m pytest -q</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest -q` exits 0 with at least 1893 tests passing (Phase 87 baseline 1879 + 11 from test_no_appstate_export_fields + 3 from test_no_deleted_state_references = 1893 minimum; Plan 88-02 D-15 adds 1+ more).
    - `python -m ruff check .` exits 0.
    - `python scripts/check_docs.py` exits 0.
    - `grep -nE "^\\s+self\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\\s*[:=]" web/state.py` returns 0 matches (SC#1).
    - `grep -rn "_TEST_BACKEND" .` returns 0 matches across the entire repo (SC#3 strict form).
    - `grep -rn "_StateProxy" tests/` returns 0 matches.
    - `python -m pytest tests/test_no_deleted_state_references.py::test_no_deleted_state_references_in_web_and_tests -v` exits 0 (Phase 88 final gate).
    - `python -m pytest tests/test_no_appstate_export_fields.py -v` exits 0 with 11 tests passing.
    - `python -m pytest tests/test_no_raw_storage_access.py -v` exits 0 with all Phase 87 tests still passing.
    - `grep -c "^  - file:" .planning/phase87_storage_allowlist.yaml` returns 3.
  </acceptance_criteria>
  <done>Phase 88 complete: all 5 ROADMAP success criteria verified, all 6 STATE-XX requirements satisfied, plan-boundary green at full pytest + ruff + check_docs, two permanent CI guards (runtime + static) installed.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| AppState.init() field deletion | Internal class refactor; risk is a test or fixture somewhere in the codebase that still does `state.last_results = [...]` after Plan 88-01 + 88-02 cleaned the 4 known files — would silently re-create the attribute dynamically (AppState has no `__setattr__` guard per CONTEXT.md Deferred Ideas). Mitigation: D-07 static AST scanner catches this at CI time. |
| Runtime D-06 test order dependency | If another test runs BEFORE D-06 and sets `state.last_results = [...]` dynamically, the AppState singleton carries the attribute and the D-06 hasattr assertion fails — but for the WRONG reason (not the deletion regression we want to guard). Mitigation: D-07 catches the order-dependent re-introduction at the AST scan layer; D-06 is the secondary runtime confirmation. |
| Static D-07 scanner false-positive | If the scanner flags a comment/docstring/string literal containing the field name, the build fails for a non-issue. Mitigation: seed-trap test `test_scanner_ignores_strings_and_comments` proves the AST-based approach is structurally safe (no Comment nodes in ast.walk; string literals are Constant nodes not Attribute nodes). |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-88-03-01 | Tampering | Dynamic re-introduction of deleted field | mitigate | A future PR adds `state.last_results = [...]` (or any of the 9 others) somewhere in `web/` or `tests/`. AppState has no `__setattr__` guard, so this silently re-creates the attribute and re-opens the cross-user leak. D-07 static AST scanner (Task 3) catches this at CI time before merge. D-06 runtime hasattr test (Task 2) provides defense-in-depth — if the dynamic write happens during test setup BEFORE D-06 runs, D-06 catches it then (although D-07 catches it at lint time, earlier). Both layers needed because runtime order matters for D-06 but doesn't for D-07. |
| T-88-03-02 | Information Disclosure | Scanner false-negative on alternate binding | accept | The scanner catches `state.X` and `app_state.X` but NOT `self.X` (intentional — would mass-false-positive across unrelated classes). If a future contributor adds `self.last_results = [...]` to AppState init OR aliases `import_alias = state` then does `import_alias.last_results = [...]`, the scanner misses it. Acceptable because (a) the AppState class definition is small and well-known; (b) the runtime D-06 test catches the init-body case by checking hasattr on a fresh instance; (c) the alias case is exotic and the production reader path (web.api.py) was already migrated in v7.11.1 to web.export_state, so even a re-introduced AppState attribute would have no consumer. The attack surface re-opening requires BOTH writer reintroduction AND reader reintroduction. |
| T-88-03-03 | Denial of Service | Plan-boundary test failure | mitigate | Task 6 enumerates 8 separate verification commands. If ANY fails, the plan cannot be declared complete. Each command targets a specific success criterion. The 1893+ test target gives a precise pass threshold. |
| T-88-03-04 | Repudiation | Stale documentation misleads future contributors | mitigate | D-16 in Task 4 refreshes the 3 stale comment/docstring sites. Task 5 adds the Phase 88 entry to CLAUDE.md "Recently Changed" and OPEN_ISSUES.md — the canonical milestone history for future maintainers. Without these updates, a future contributor reading `web/search_api.py:1198` would see a rule that names fields no longer existing — confusing but not security-impacting. The mitigation is editorial clarity. |

**No HIGH-severity threats.** Plan 88-03 closes the cross-user export-state attack surface PERMANENTLY by:
(a) Physically deleting the 10 singleton mirror fields (Task 1).
(b) Installing a permanent CI guard against dynamic re-introduction (Tasks 2 + 3 — runtime + static).
(c) Refreshing stale documentation so future contributors don't accidentally reintroduce the pattern based on stale rules (Task 4 + Task 5).

The mitigation chain is layered: D-07 catches at CI lint time (earliest), D-06 catches at test runtime (defense-in-depth), and the documentation refresh provides forward guidance that prevents the regression from being attempted in the first place.

**Defense-in-depth note:** Phase 87 chokepoint (`web/safe_storage.py`) provides the per-session storage isolation that ALL export-state reads/writes now route through. Phase 88 closes the AppState singleton bypass that was the leak vector. Phase 89-92 close the other singleton/cache patterns identified across 4 rounds of Codex review.
</threat_model>

<verification>
1. All 6 tasks pass acceptance criteria.
2. Plan-boundary green (Task 6): full pytest at Phase 87 baseline + 14+ new Phase 88 tests, ruff clean, check_docs clean.
3. ROADMAP.md Phase 88 5 success criteria all verified via Task 6's enumerated commands.
4. STATE-01..06 all satisfied:
   - STATE-01: 10 fields deleted from AppState (SC#1).
   - STATE-02: Writers migrated (Plan 88-01) — grep confirms 0 writer references.
   - STATE-03: Readers migrated (Plan 88-02) — grep confirms 0 reader references to parallels_source_text.
   - STATE-04: _TEST_BACKEND removed (SC#3).
   - STATE-05: test_export_cross_user_isolation rewritten (SC#4).
   - STATE-06: 3 other tests rewritten (SC#5).
5. Two new permanent CI guards installed: tests/test_no_appstate_export_fields.py (D-06) + tests/test_no_deleted_state_references.py (D-07).
6. Phase 87 lint scanner remains green; allowlist trimmed from 4 to 3 entries.
7. Documentation refreshed: docs/OPEN_ISSUES.md + CLAUDE.md "Recently Changed" + 3 stale docstring/comment sites in source.
8. Zero user-visible behavior change.
</verification>

<success_criteria>
- All 5 ROADMAP.md Phase 88 success criteria verified.
- All 6 STATE-XX requirements satisfied (collectively across Plans 88-01, 88-02, 88-03).
- Phase 87 invariants intact (lint scanner green, 3 allowlist entries remaining all scoped to Phase 90/91 deletion).
- Two new permanent CI regression guards installed (D-06 runtime + D-07 static).
- Documentation maintenance contract honored (CLAUDE.md + OPEN_ISSUES updated per CLAUDE.md doc-maintenance rules).
- Plan-boundary green: 1893+ pytest passed, ruff clean, check_docs clean.
- Phase 88 ready to mark complete in ROADMAP.md (3/3 plans complete).
</success_criteria>

<output>
After completion, create `.planning/phases/88-state-separation-by-deletion/88-03-appstate-deletion-and-enforcement-SUMMARY.md` per @$HOME/.claude/get-shit-done/templates/summary.md.
</output>
