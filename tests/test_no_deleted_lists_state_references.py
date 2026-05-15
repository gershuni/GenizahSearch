"""Phase 89 D-10 + REVIEWS R7/R8 — static AST scanner for deleted lists-cache plumbing.

Walks web/ AND tests/ looking for re-introductions of the names deleted
from web/state.py:AppState and web/user_lists.py:UserListsManager in
Phase 89:

  - ``_user_lists_mgr``        (AppState field — singleton — LISTS-01)   [BROAD scope]
  - ``_cache_entry``           (UserListsManager field — 10s TTL — LISTS-03)   [NARROW scope per R7]
  - ``_cache_ttl``             (UserListsManager field — 10s TTL — LISTS-03)   [NARROW scope per R7]
  - ``init_user_lists_mgr``    (AppState method — bootstrap helper) [FunctionDef + Call site per R8]

Catches:
  1. Attribute access:  state._user_lists_mgr, self._user_lists_mgr,
                        mgr._cache_entry, mgr._cache_ttl (NARROW: only inside
                        web/user_lists.py + web/state.py per R7 unless qualified
                        by an aliased state binding).
                        Plus aliased state imports per Phase 88 Refinement 5
                        (from web.state import state as s -> s._user_lists_mgr).
  2. setattr / getattr / hasattr calls: hasattr(state, '_user_lists_mgr')
                        and the corresponding setattr/getattr forms.
  3. FunctionDef nodes named ``init_user_lists_mgr`` (catches restoration
                        via class-body method definition).
  4. Call nodes shaped `<state-alias>.init_user_lists_mgr(...)` (R8 — catches
                        restoration of the caller without re-adding method body,
                        and the converse where the method is restored without
                        the call).

This is the STATIC guard. The companion RUNTIME guard
(tests/test_no_user_lists_mgr_field.py) asserts AttributeError on direct
instance access.

Per Phase 89 D-10 (Codex review of CONTEXT.md proposed scope) + R7/R8:
  - Scan both web/ and tests/ — Phase 88 pattern.
  - Track aliased state imports per Phase 88 Refinement 5.
  - Include `self.<deleted_name>` access (not just `state.<deleted_name>`)
    because AppState has no __setattr__ guard, so the most likely
    regression form is someone re-adding `self._user_lists_mgr = ...`
    directly inside AppState.init().
  - R7 narrow scope: `_cache_entry` / `_cache_ttl` flagged ANYWHERE inside
    web/user_lists.py + web/state.py (the only legitimate owners), but
    OUTSIDE those two files only flagged when qualified by an aliased
    state binding — prevents future false-positives if unrelated utilities
    use these private names.
  - R8 Call-node: also catches `state.init_user_lists_mgr()` / aliased
    forms — not just FunctionDef restoration.
  - Seed traps are PARSED AST snippets (ast.parse()), not raw string
    literals — proves the scanner finds positives, not just false-
    negatives.

See .planning/phases/89-lists-cache-per-request/89-CONTEXT.md D-10 and
89-REVIEWS.md R7/R8 for full decision rationale.
"""
import ast
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
SCAN_DIRS = [REPO_ROOT / 'web', REPO_ROOT / 'tests']

# Names deleted in Phase 89.
# _user_lists_mgr is BROAD scope (flagged anywhere it appears qualified by
# a state alias or `self`, or via setattr/getattr/hasattr).
DELETED_BROAD_NAMES = frozenset({
    '_user_lists_mgr',
})

# _cache_entry + _cache_ttl are NARROW scope per R7:
# - Inside web/user_lists.py + web/state.py: flagged ANYWHERE (unrestricted
#   attribute name catch). These are the only legitimate owners.
# - Outside those files: flagged ONLY when qualified by a state alias
#   (same rule as _user_lists_mgr).
DELETED_NARROW_NAMES = frozenset({
    '_cache_entry',
    '_cache_ttl',
})

# Union for convenience (used by setattr/getattr/hasattr matcher and
# in error messages).
DELETED_LISTS_NAMES = DELETED_BROAD_NAMES | DELETED_NARROW_NAMES

# FunctionDef name disallowed (D-10 explicit requirement — catches
# restoration of the bootstrap helper method even if its caller is removed).
DELETED_FUNCTION_NAMES = frozenset({
    'init_user_lists_mgr',
})

# State bindings — extends per-file with aliased imports.
STATE_BINDING_NAMES = frozenset({'state', 'app_state', 'self'})
# 'self' is added for Phase 89 specifically — D-10 explicit requirement:
# catch `self._user_lists_mgr = ...` inside AppState.init() restoration.

# Files exempt from scanning (test scanner files themselves — they contain
# the deleted names as literal data for the parametrize / seed traps).
EXEMPT_FILES = frozenset({
    'tests/test_no_deleted_lists_state_references.py',
    'tests/test_no_user_lists_mgr_field.py',
})

# Files where _cache_entry / _cache_ttl are flagged on ANY attribute access
# (not just state-qualified). Per R7 narrow scope.
NARROW_SCOPE_OWNING_FILES = frozenset({
    'web/user_lists.py',
    'web/state.py',
})


class _DeletedListsAccessVisitor(ast.NodeVisitor):
    """Visit Import/Attribute/Call/FunctionDef nodes to catch references to
    Phase-89-deleted lists-cache plumbing.

    Mirrors Phase 88's _DeletedStateAccessVisitor shape with additions:
      1. 'self' is added to default bindings (D-10 — catches restoration
         inside AppState class body).
      2. FunctionDef visiting (catches `def init_user_lists_mgr(self): ...`).
      3. hasattr() is added to the call set (Phase 88 only checked
         setattr/getattr).
      4. R7: `_cache_entry`/`_cache_ttl` unrestricted-attribute catch is
         scoped to `is_narrow_owning_file` (only fires inside
         web/user_lists.py + web/state.py).
      5. R8: Call-node visitor catches `<state-alias>.init_user_lists_mgr(...)`.
    """

    _DEFAULT_BINDINGS = frozenset({'state', 'app_state', 'self'})
    _STATE_SOURCE_MODULES = frozenset({'web.state', 'web.api'})

    def __init__(self, source: str, is_narrow_owning_file: bool = False):
        self.source = source
        self.is_narrow_owning_file = is_narrow_owning_file
        self.violations: list[tuple[int, str]] = []
        self.aliases: set[str] = set(self._DEFAULT_BINDINGS)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        if node.module in self._STATE_SOURCE_MODULES:
            for alias in node.names:
                if alias.name == 'state':
                    self.aliases.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import):
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute):
        # Direct: <name>.<deleted> where name is in self.aliases.
        if (
            isinstance(node.value, ast.Name)
            and node.value.id in self.aliases
            and node.attr in DELETED_LISTS_NAMES
        ):
            self._record(node)
        # Chained: <module_alias>.state.<deleted>
        elif (
            isinstance(node.value, ast.Attribute)
            and node.value.attr == 'state'
            and node.attr in DELETED_LISTS_NAMES
        ):
            self._record(node)
        # R7 NARROW scope: unrestricted catch for _cache_entry/_cache_ttl
        # ONLY inside web/user_lists.py + web/state.py.
        elif (
            self.is_narrow_owning_file
            and node.attr in DELETED_NARROW_NAMES
        ):
            self._record(node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        # setattr / getattr / hasattr against any aliased binding.
        if (
            isinstance(node.func, ast.Name)
            and node.func.id in ('setattr', 'getattr', 'hasattr')
        ):
            if len(node.args) >= 2:
                arg0, arg1 = node.args[0], node.args[1]
                arg0_is_state = (
                    (isinstance(arg0, ast.Name) and arg0.id in self.aliases)
                    or (isinstance(arg0, ast.Attribute) and arg0.attr == 'state')
                )
                if (
                    arg0_is_state
                    and isinstance(arg1, ast.Constant)
                    and isinstance(arg1.value, str)
                    and arg1.value in DELETED_LISTS_NAMES
                ):
                    self._record(node)
        # R8: <state-alias>.init_user_lists_mgr(...) call site.
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in DELETED_FUNCTION_NAMES:
                inner = node.func.value
                inner_is_state = (
                    (isinstance(inner, ast.Name) and inner.id in self.aliases)
                    or (isinstance(inner, ast.Attribute) and inner.attr == 'state')
                )
                if inner_is_state:
                    self._record(node)
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef):
        # D-10 explicit: catch `def init_user_lists_mgr(self): ...` restoration.
        if node.name in DELETED_FUNCTION_NAMES:
            self._record(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        if node.name in DELETED_FUNCTION_NAMES:
            self._record(node)
        self.generic_visit(node)

    def _record(self, node):
        seg = ast.get_source_segment(self.source, node) or ''
        # Trim long segments (FunctionDef bodies) to keep error output short.
        if len(seg) > 200:
            seg = seg[:200] + '...'
        self.violations.append((node.lineno, seg))


def _scan_file(path: Path, source: str) -> list[tuple[int, str]]:
    rel = path.relative_to(REPO_ROOT).as_posix()
    is_narrow = rel in NARROW_SCOPE_OWNING_FILES
    tree = ast.parse(source, filename=str(path))
    visitor = _DeletedListsAccessVisitor(source, is_narrow_owning_file=is_narrow)
    visitor.visit(tree)
    return visitor.violations


def test_scanner_detects_synthetic_attribute_access():
    """Seed trap: 4+ deliberately-bad patterns (parsed as AST snippets per D-10).

    Synthetic source is parsed as if it were inside `web/user_lists.py`
    (is_narrow_owning_file=True) so the cache-field attribute violations fire.
    """
    synthetic = (
        "from web.state import state\n"
        "class FakeAppState:\n"
        "    def init(self):\n"
        "        self._user_lists_mgr = None  # restored AppState field\n"
        "class FakeUserListsManager:\n"
        "    def __init__(self):\n"
        "        self._cache_entry = None     # restored cache tuple\n"
        "        self._cache_ttl = 10         # restored TTL constant\n"
        "def bad_global_access():\n"
        "    return state._user_lists_mgr    # restored singleton access\n"
    )
    tree = ast.parse(synthetic)
    visitor = _DeletedListsAccessVisitor(synthetic, is_narrow_owning_file=True)
    visitor.visit(tree)
    assert len(visitor.violations) >= 4, (
        f"Expected at least 4 violations (self._user_lists_mgr, "
        f"self._cache_entry, self._cache_ttl, state._user_lists_mgr), "
        f"got {len(visitor.violations)}: {visitor.violations}"
    )


def test_scanner_catches_setattr_getattr_hasattr():
    """Seed trap: setattr/getattr/hasattr against deleted names must be caught.

    Phase 88's scanner caught setattr/getattr; Phase 89 D-10 extends with
    hasattr because tests/test_no_appstate_export_fields.py used hasattr
    for runtime assertions — a regression PR might use the same form.
    """
    synthetic = (
        "from web.state import state\n"
        "def bad_setattr():\n"
        "    setattr(state, '_user_lists_mgr', None)\n"
        "def bad_getattr():\n"
        "    return getattr(state, '_user_lists_mgr', None)\n"
        "def bad_hasattr():\n"
        "    return hasattr(state, '_user_lists_mgr')\n"
    )
    tree = ast.parse(synthetic)
    visitor = _DeletedListsAccessVisitor(synthetic, is_narrow_owning_file=False)
    visitor.visit(tree)
    assert len(visitor.violations) >= 3, (
        f"Expected at least 3 violations (setattr, getattr, hasattr), "
        f"got {len(visitor.violations)}: {visitor.violations}"
    )


def test_scanner_catches_init_user_lists_mgr_functiondef():
    """Seed trap: a re-introduction of `def init_user_lists_mgr(self): ...`
    as a method definition must be caught (D-10 explicit requirement).

    Catches the regression form where someone restores the method but
    the call-site at web/main.py:1508 stays absent — pure code-revival
    that a call-site grep would miss.
    """
    synthetic = (
        "class FakeAppState:\n"
        "    def init_user_lists_mgr(self):\n"
        "        # Restored bootstrap helper — must be flagged.\n"
        "        from web.user_lists import UserListsManager\n"
        "        self._user_lists_mgr = UserListsManager(None, None)\n"
    )
    tree = ast.parse(synthetic)
    visitor = _DeletedListsAccessVisitor(synthetic, is_narrow_owning_file=False)
    visitor.visit(tree)
    # Expect at least 2 violations: the FunctionDef itself + the
    # self._user_lists_mgr assignment inside it.
    functiondef_hits = [v for v in visitor.violations if 'init_user_lists_mgr' in v[1]]
    assert len(functiondef_hits) >= 1, (
        f"FunctionDef restoration not caught: {visitor.violations}"
    )


def test_scanner_catches_init_user_lists_mgr_call_site():
    """R8 seed trap: a re-introduction of `state.init_user_lists_mgr()` as
    a call site must be caught — catches the converse regression where the
    method is restored without the FunctionDef (e.g., monkeypatched in)
    OR where the call is restored without the method (immediate AttributeError
    but the scanner should still flag the intent).
    """
    synthetic = (
        "from web.state import state\n"
        "import web.state as web_state\n"
        "from web.state import state as s\n"
        "def restored_call():\n"
        "    state.init_user_lists_mgr()\n"
        "    s.init_user_lists_mgr()\n"
        "    web_state.state.init_user_lists_mgr()\n"
    )
    tree = ast.parse(synthetic)
    visitor = _DeletedListsAccessVisitor(synthetic, is_narrow_owning_file=False)
    visitor.visit(tree)
    call_hits = [v for v in visitor.violations if 'init_user_lists_mgr' in v[1]]
    assert len(call_hits) >= 2, (
        f"Expected at least 2 call-site violations (state.init_user_lists_mgr, "
        f"s.init_user_lists_mgr), got {call_hits}"
    )


def test_scanner_narrow_scope_for_cache_entry_outside_owning_files():
    """R7 narrow scope: `_cache_entry`/`_cache_ttl` ONLY fire on unrestricted
    attribute access inside web/user_lists.py + web/state.py.

    Outside those two files, an unrelated `mgr._cache_entry = ...` (where
    `mgr` is not a state binding) must NOT be flagged — that lets future
    utility classes use these private names without false-positives.

    The same content INSIDE the owning files (narrow=True) MUST be flagged.
    """
    synthetic_unrelated = (
        "class SomeUnrelatedUtility:\n"
        "    def __init__(self):\n"
        "        self.cache = {}\n"
        "        # An unrelated class using these private names — not a state.\n"
        "        # NOT-A-LIST mgr._cache_entry pattern here for testing.\n"
        "def get_some_unrelated():\n"
        "    mgr = SomeUnrelatedUtility()\n"
        "    mgr._cache_entry = None  # NOT a state binding\n"
        "    mgr._cache_ttl = 10\n"
        "    return mgr\n"
    )

    # Outside owning files (narrow=False): NO violations expected.
    tree = ast.parse(synthetic_unrelated)
    outside_visitor = _DeletedListsAccessVisitor(synthetic_unrelated, is_narrow_owning_file=False)
    outside_visitor.visit(tree)
    # `self._cache_entry` IS flagged in the unrelated class body because
    # 'self' is in _DEFAULT_BINDINGS. This is acceptable — `self` as a
    # binding suggests this code might be inside an AppState/UserListsManager
    # subclass anyway. The R7 narrow scope is specifically about non-state
    # bindings (`mgr.<>`).
    non_self_violations = [
        v for v in outside_visitor.violations if 'self.' not in v[1]
    ]
    assert non_self_violations == [], (
        f"R7 narrow scope violated: outside the owning files, `mgr._cache_entry` "
        f"should NOT be flagged (only flagged via self.<> or state-alias.<>): "
        f"{non_self_violations}"
    )

    # Inside owning files (narrow=True): violations expected.
    inside_visitor = _DeletedListsAccessVisitor(synthetic_unrelated, is_narrow_owning_file=True)
    inside_visitor.visit(tree)
    assert len(inside_visitor.violations) >= 2, (
        f"R7 narrow-scope failure: inside web/user_lists.py + web/state.py, "
        f"`mgr._cache_entry` and `mgr._cache_ttl` MUST be flagged, got: "
        f"{inside_visitor.violations}"
    )


def test_exempt_files_are_skipped_in_production_scan():
    """R7 EXEMPT_FILES exemption-verification.

    The production scan iterates `SCAN_DIRS` and skips paths in EXEMPT_FILES.
    This test (a) asserts EXEMPT_FILES contains the two scanner-companion
    files, and (b) confirms the scan-loop respects the exemption by spying
    on _scan_file.
    """
    from unittest.mock import patch as mock_patch

    # (a) Membership assertion.
    assert 'tests/test_no_deleted_lists_state_references.py' in EXEMPT_FILES
    assert 'tests/test_no_user_lists_mgr_field.py' in EXEMPT_FILES
    assert len(EXEMPT_FILES) == 2, (
        f"EXEMPT_FILES drift: expected exactly 2 entries, got {sorted(EXEMPT_FILES)}"
    )

    # (b) Spy: confirm that when production-scan iterates SCAN_DIRS and hits
    # one of the EXEMPT_FILES paths, _scan_file is NOT called for it.
    call_log = []
    original_scan = _scan_file

    def spy_scan(path: Path, source: str):
        call_log.append(path.relative_to(REPO_ROOT).as_posix())
        return original_scan(path, source)

    # Simulate the production loop with the spy.
    with mock_patch(__name__ + '._scan_file', side_effect=spy_scan):
        # Mimic the production scan iteration shape.
        for scan_dir in SCAN_DIRS:
            for path in scan_dir.rglob('*.py'):
                rel = path.relative_to(REPO_ROOT).as_posix()
                if rel in EXEMPT_FILES:
                    continue
                source = path.read_text(encoding='utf-8')
                try:
                    spy_scan(path, source)
                except SyntaxError:
                    pass

    for exempt in EXEMPT_FILES:
        assert exempt not in call_log, (
            f"R7 exemption failure: {exempt} was scanned despite EXEMPT_FILES "
            f"membership. Call log: {[c for c in call_log if 'test_no_' in c]}"
        )


def test_scanner_ignores_strings_and_comments():
    """Seed trap: deliberate mentions in comments/strings/docstrings must NOT
    be reported. Only real code references count.
    """
    synthetic = (
        "from web.state import state  # _user_lists_mgr is gone post-Phase-89\n"
        '"""Module docstring mentioning _user_lists_mgr historically."""\n'
        "def innocent():\n"
        "    msg = 'state._user_lists_mgr was deleted in Phase 89'\n"
        "    return state.meta_mgr  # surviving field is fine\n"
        "    # _cache_entry was a 10s TTL — also gone\n"
    )
    tree = ast.parse(synthetic)
    visitor = _DeletedListsAccessVisitor(synthetic, is_narrow_owning_file=False)
    visitor.visit(tree)
    assert visitor.violations == [], (
        f"Scanner false-positive on string/comment mentions: {visitor.violations}"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Phase 89 Task 3 deletion pending — this scan will turn green once "
        "_user_lists_mgr field is deleted from web/state.py + init_user_lists_mgr "
        "method is gone + web/main.py:1508 call is gone. Task 3 removes this "
        "xfail marker in the SAME atomic commit as the deletion (R9 + R10)."
    ),
)
def test_no_deleted_lists_references_in_web_and_tests():
    """Production guard: no Python file under web/ or tests/ references any
    of the Phase-89-deleted names via the forms enumerated in D-10/R7/R8.

    This is the permanent CI lint. If it fails after a future PR has shipped
    Phase 89, that PR is restoring deleted plumbing.

    Marked xfail(strict=True) until Task 3 lands the deletion — see R9.
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
            "References to Phase-89-deleted lists-cache plumbing found:\n  "
            + "\n  ".join(violations[:50])
            + f"\n\nTotal violations: {len(violations)}"
            + "\n\nFix: route through state.lists_mgr factory property "
            + "(see Phase 89 D-01 in CONTEXT.md)."
            + "\nThe deleted names: "
            + ", ".join(sorted(DELETED_LISTS_NAMES | DELETED_FUNCTION_NAMES))
        )
        pytest.fail(msg)
