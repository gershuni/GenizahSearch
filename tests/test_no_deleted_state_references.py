"""Phase 88 D-07 -- static AST scanner for deleted AppState reference regressions.

Walks web/ AND tests/ looking for references to the 10 per-user export-state
fields deleted from web/state.py:AppState in Phase 88. Catches:

  1. Attribute access:  state.last_results, state.parallels_results, etc.
                        Also app_state.last_results (alternate binding name).
                        Also aliased imports per Refinement 5:
                          from web.state import state as s   ->  s.last_results
                          from web.api import state as api_state -> api_state.last_results
                          import web.state as web_state      -> web_state.state.last_results
  2. setattr calls:     setattr(state, 'last_results', ...).
  3. getattr calls:     getattr(state, 'last_results', ...).

This is the STATIC guard. The companion RUNTIME guard
(tests/test_no_appstate_export_fields.py) asserts AttributeError on direct
instance access. The static check survives forever as a CI guard against
dynamic re-introductions that runtime tests might miss when order-dependent.

Per Phase 88 D-07: scan both web/ and tests/ -- Plan 88-01 + 88-02 cleaned
both directories; this scanner is the forward-going regression guard.
Per D-08: this test lives in Phase 88, NOT deferred to Phase 92 SWEEP-01.
Per Phase 88 Refinement 5 (Codex MEDIUM): the scanner tracks per-file
import aliases so it survives `from web.state import state as s` and
chained `web_state.state.X` access patterns -- important because this
scanner is a PERMANENT CI guard and must survive long enough to catch
regressions where contributors don't use the canonical binding names.
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
# Conservative: only catch state.X / app_state.X -- does NOT catch self.X,
# which would mass-false-positive (every class with a `last_results` attr
# in any other module would trip). The AppState class itself defines none
# of these post-Phase-88, so self.X inside AppState is safe to skip.
#
# Per Refinement 5: this set is the DEFAULT -- the visitor extends it
# per-file via ImportFrom alias tracking.
STATE_BINDING_NAMES = frozenset({'state', 'app_state'})

# Files exempt from this scan (the test itself, the AppState class definition,
# anything that intentionally references these names for migration history).
EXEMPT_FILES = frozenset({
    'tests/test_no_deleted_state_references.py',
    'tests/test_no_appstate_export_fields.py',  # parametrizes over DELETED_FIELDS literally
})


class _DeletedStateAccessVisitor(ast.NodeVisitor):
    """Visit Import/ImportFrom to collect AppState bindings (including aliases),
    then Attribute/Call nodes to flag references to deleted AppState fields.

    Per Phase 88 D-07 extension (Codex review, Refinement 5): the scanner
    tracks aliased imports so that `from web.state import state as s;
    s.last_results = []` is caught alongside the canonical
    `state.last_results = []`.
    """

    # Default binding names always recognized (canonical idiom).
    _DEFAULT_BINDINGS = frozenset({'state', 'app_state'})
    # Modules whose `state` symbol binds to the AppState singleton.
    _STATE_SOURCE_MODULES = frozenset({'web.state', 'web.api'})

    def __init__(self, source: str):
        self.source = source
        self.violations: list[tuple[int, str]] = []
        self.aliases: set[str] = set(self._DEFAULT_BINDINGS)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        # Catches: from web.state import state [as s], from web.api import state [as s]
        if node.module in self._STATE_SOURCE_MODULES:
            for alias in node.names:
                if alias.name == 'state':
                    self.aliases.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import):
        # Catches: import web.state as web_state (then web_state.state.X is access via attribute chain)
        # For simplicity we don't fully chase web_state.state.X here -- we add web_state to a
        # secondary set tracked via attribute-chain inspection in visit_Attribute. The
        # primary alias set still catches the common case.
        # No-op for the alias set; chain-resolution handled in visit_Attribute below.
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute):
        # Direct case: <name>.last_results where <name> is in self.aliases.
        if (
            isinstance(node.value, ast.Name)
            and node.value.id in self.aliases
            and node.attr in DELETED_FIELDS
        ):
            self._record(node)
        # Chained case: <module_alias>.state.last_results
        # Only fires when the inner attribute chain ends in `.state.<field>`.
        elif (
            isinstance(node.value, ast.Attribute)
            and node.value.attr == 'state'
            and node.attr in DELETED_FIELDS
        ):
            # We accept this as a possible AppState ref -- conservative but bounded.
            # (False-positive risk: any object with a `.state.<field>` chain matching.
            # Acceptable in this codebase -- `.state` is overwhelmingly AppState.)
            self._record(node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        # setattr / getattr against any aliased binding.
        if isinstance(node.func, ast.Name) and node.func.id in ('setattr', 'getattr'):
            if len(node.args) >= 2:
                arg0, arg1 = node.args[0], node.args[1]
                # arg0 is a Name in self.aliases, OR an Attribute ending in .state
                arg0_is_state = (
                    (isinstance(arg0, ast.Name) and arg0.id in self.aliases)
                    or (isinstance(arg0, ast.Attribute) and arg0.attr == 'state')
                )
                if (
                    arg0_is_state
                    and isinstance(arg1, ast.Constant)
                    and isinstance(arg1.value, str)
                    and arg1.value in DELETED_FIELDS
                ):
                    self._record(node)
        self.generic_visit(node)

    def _record(self, node):
        seg = ast.get_source_segment(self.source, node) or ''
        self.violations.append((node.lineno, seg))


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


def test_scanner_catches_aliased_imports():
    """Phase 88 D-07 extension (Codex review, Refinement 5): scanner must catch
    aliased state bindings, not just the canonical `state` and `app_state` names.

    Aliased forms exercised:
      - from web.state import state as s   ->  s.last_results
      - from web.api import state as api_state  ->  api_state.parallels_results
      - import web.state as web_state      ->  web_state.state.current_search_query
      - setattr(s, 'last_filters_applied', None)
    """
    aliased = (
        "from web.state import state as s\n"
        "from web.api import state as api_state\n"
        "import web.state as web_state\n"
        "def bad_aliased():\n"
        "    s.last_results = []\n"
        "    api_state.parallels_results = []\n"
        "    web_state.state.current_search_query = ''\n"
        "    setattr(s, 'last_filters_applied', None)\n"
    )
    tree = ast.parse(aliased)
    visitor = _DeletedStateAccessVisitor(aliased)
    visitor.visit(tree)
    assert len(visitor.violations) >= 4, (
        f"Expected at least 4 aliased violations (s.X, api_state.X, "
        f"web_state.state.X, setattr(s,...)), got {len(visitor.violations)}: "
        f"{visitor.violations}"
    )


def test_no_deleted_state_references_in_web_and_tests():
    """Production guard: no Python file under web/ or tests/ references any of
    the 10 Phase-88-deleted AppState fields via state.X / app_state.X / aliased /
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
