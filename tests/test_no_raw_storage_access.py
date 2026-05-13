"""Lint test: reject raw app.storage.user access outside the Phase 87 allowlist.

Reads .planning/phase87_storage_allowlist.yaml and scans every .py file under
web/ for AST nodes matching:
  - <app_alias>.storage.user.get(...)
  - <app_alias>.storage.user.pop(...)
  - <app_alias>.storage.user[...]  (Subscript both read and assign)
  - <app_alias>.storage.user (bare attribute access, e.g., `storage = app.storage.user`)

Where <app_alias> is any name bound to `from nicegui import app[ as ALIAS]`.
The three known aliases in this codebase are `app`, `nicegui_app`, `_app`.

Phase 87 FOUND-04 success criterion: this scan returns ZERO violations
outside the allowlist after Plans 02-06 land.

Revisions per 87-REVIEWS.md:
  - B2: corrected AST chain check (inner-first order ['user', 'storage']) and
    added parent tracking via NodeVisitor to avoid double-reporting nested
    nodes. The previous chain[-2:] == ['storage', 'user'] check did not match
    actual ast.walk output and would have caused the synthetic violation test
    to pass falsely.
  - H1: new schema with `source` + `expected_count` per pattern; added
    test_allowlist_counts_exact to enforce exact match counts.
"""
import ast
import textwrap
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = REPO_ROOT / 'web'
ALLOWLIST_PATH = REPO_ROOT / '.planning' / 'phase87_storage_allowlist.yaml'


def _load_allowlist() -> dict:
    if not ALLOWLIST_PATH.exists():
        return {'allowed_raw_access': []}
    with ALLOWLIST_PATH.open('r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {'allowed_raw_access': []}


def _find_app_aliases(tree: ast.AST) -> set:
    """Return names bound to `nicegui.app` in this module.

    Handles: `from nicegui import app`, `from nicegui import app as nicegui_app`,
    `from nicegui import app as _app`. Also handles inline (function-local)
    imports because ast.walk visits all ImportFrom nodes regardless of scope.
    """
    aliases = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'nicegui':
            for alias in node.names:
                if alias.name == 'app':
                    aliases.add(alias.asname or 'app')
    return aliases


def _walk_attribute_chain(start: ast.AST, app_aliases: set):
    """Walk an Attribute chain inward from `start`, returning (chain, root).

    chain is inner-first: for app.storage.user as the start, chain == ['user', 'storage']
    after the walk, and root is the ast.Name('app').

    Returns (chain, root_name_str) or (None, None) if the chain does not end in
    a Name in app_aliases.

    Per B2 in 87-REVIEWS.md: the previous implementation appended in the same
    order but checked chain[-2:] == ['storage', 'user'], which would never match
    because the actual order is ['user', 'storage']. This implementation makes
    the order explicit and checks chain[-2:] == ['user', 'storage'].
    """
    chain = []
    cur = start
    while isinstance(cur, ast.Attribute):
        chain.append(cur.attr)
        cur = cur.value
    if not isinstance(cur, ast.Name):
        return None, None
    if cur.id not in app_aliases:
        return None, None
    return chain, cur.id


def _matches_storage_user_access(chain) -> bool:
    """Given an inner-first chain from _walk_attribute_chain, return True iff
    it ends in `.storage.user` (i.e., the access targets app.storage.user[...] or
    app.storage.user.X)."""
    return len(chain) >= 2 and chain[-2:] == ['user', 'storage']


class _StorageAccessVisitor(ast.NodeVisitor):
    """Visit Call/Subscript/Attribute/Assign-target nodes and collect storage-user accesses.

    Uses parent tracking via a _seen set keyed by id(inner_attribute_node) so
    the inner Attribute that a Call.func or Subscript.value already consumed
    is not reported a second time when ast.walk would otherwise visit it.
    """

    def __init__(self, app_aliases: set, source: str):
        self.app_aliases = app_aliases
        self.source = source
        self.violations: list[tuple[int, str]] = []  # (lineno, source_segment)
        self._seen_inner_ids: set[int] = set()

    def _record(self, node):
        seg = ast.get_source_segment(self.source, node) or ''
        self.violations.append((node.lineno, seg))

    def visit_Call(self, node: ast.Call):
        # app.storage.user.get(...) / .pop(...) / etc.
        if isinstance(node.func, ast.Attribute):
            # The call's func is an Attribute like `<expr>.get`. To check whether
            # `<expr>` is app.storage.user, we start the walk from node.func.value.
            chain, root = _walk_attribute_chain(node.func.value, self.app_aliases)
            if chain is not None and _matches_storage_user_access(chain):
                self._record(node)
                # Mark the entire Attribute subtree under node.func as seen.
                for sub in ast.walk(node.func):
                    if isinstance(sub, ast.Attribute):
                        self._seen_inner_ids.add(id(sub))
        # Continue walking into arguments (they might contain more accesses).
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript):
        # app.storage.user[KEY] (read or assign-target)
        if isinstance(node.value, ast.Attribute):
            chain, root = _walk_attribute_chain(node.value, self.app_aliases)
            if chain is not None and _matches_storage_user_access(chain):
                self._record(node)
                for sub in ast.walk(node.value):
                    if isinstance(sub, ast.Attribute):
                        self._seen_inner_ids.add(id(sub))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute):
        # Catches bare `app.storage.user` access (e.g., `storage = app.storage.user`)
        # that did NOT appear as a Call.func or Subscript.value (those are caught
        # above and would mark this node's id as seen).
        if id(node) in self._seen_inner_ids:
            return  # Already reported as part of a parent Call/Subscript
        chain, root = _walk_attribute_chain(node, self.app_aliases)
        if chain is not None and _matches_storage_user_access(chain):
            # Only report if this Attribute is the OUTERMOST in its chain — i.e.,
            # its own parent in the AST is not another Attribute that we would
            # also visit. Since ast.NodeVisitor doesn't natively track parents,
            # we approximate: a bare `app.storage.user` reaches here only if it
            # was not part of a Call/Subscript we already consumed. Additional
            # nested Attribute would have its OUTER attr appended FIRST, so the
            # walk result for the inner-most matching node may not be 'user'/'storage'
            # at chain[-2:]. The _matches_storage_user_access guard handles that.
            self._record(node)
        self.generic_visit(node)


def _scan_file(path: Path, source: str) -> list[tuple[int, str]]:
    """Return list of (lineno, source_segment) violations for one .py file."""
    tree = ast.parse(source, filename=str(path))
    aliases = _find_app_aliases(tree)
    if not aliases:
        return []
    visitor = _StorageAccessVisitor(aliases, source)
    visitor.visit(tree)
    return visitor.violations


def _is_allowlisted(rel_path: str, source_segment: str, allowed_map: dict) -> bool:
    """Return True if the (rel_path, source_segment) tuple matches an allowlist entry.

    allowed_map: {rel_path -> entry_dict}. Each entry has `patterns` which is
    a list of {source: str, expected_count: int, enclosing?: str} dicts.
    Substring match on `source` is sufficient to legalize a violation; the
    expected_count is enforced separately by test_allowlist_counts_exact.
    """
    entry = allowed_map.get(rel_path)
    if not entry:
        return False
    for pat in entry.get('patterns', []):
        if isinstance(pat, dict):
            source_pat = pat.get('source', '')
        else:
            source_pat = pat  # Legacy schema fallback (string)
        if source_pat and source_pat in source_segment:
            return True
    return False


# ===========================================================================
# Tests
# ===========================================================================

def test_allowlist_well_formed():
    """FOUND-03 schema check: every allowlist entry has file + patterns + justification.

    Per H1, each pattern must be a dict with `source` and `expected_count`.
    """
    allowlist = _load_allowlist()
    entries = allowlist.get('allowed_raw_access', [])
    assert entries, "Allowlist is empty — at minimum web/auth_state.py should be allowlisted"
    for entry in entries:
        assert 'file' in entry, f"Entry missing 'file': {entry}"
        assert 'patterns' in entry, f"Entry {entry['file']} missing 'patterns'"
        assert entry['patterns'], f"Entry {entry['file']} has empty patterns list"
        assert 'justification' in entry, f"Entry {entry['file']} missing 'justification'"
        assert entry['justification'].strip(), f"Entry {entry['file']} has empty justification"
        for pat in entry['patterns']:
            assert isinstance(pat, dict), (
                f"Entry {entry['file']}: pattern {pat!r} must be a dict with "
                f"'source' and 'expected_count' keys (H1 schema)"
            )
            assert 'source' in pat and isinstance(pat['source'], str) and pat['source'].strip(), (
                f"Entry {entry['file']}: pattern missing/empty 'source': {pat}"
            )
            assert 'expected_count' in pat and isinstance(pat['expected_count'], int) and pat['expected_count'] >= 1, (
                f"Entry {entry['file']}: pattern '{pat.get('source')}' missing/invalid 'expected_count': {pat}"
            )


def test_lint_rejects_synthetic_violation():
    """FOUND-04 SC4: verify the lint visitor detects a synthetic raw access (with corrected chain semantics)."""
    synthetic = textwrap.dedent("""\
        from nicegui import app
        def bad():
            return app.storage.user.get('foo')
    """)
    tree = ast.parse(synthetic)
    aliases = _find_app_aliases(tree)
    assert aliases == {'app'}, f"Expected alias 'app', got {aliases}"
    visitor = _StorageAccessVisitor(aliases, synthetic)
    visitor.visit(tree)
    assert visitor.violations, "Lint visitor failed to detect synthetic raw access (B2 chain bug regression?)"


def test_lint_handles_aliased_imports():
    """FOUND-04: verify alias resolution catches `nicegui_app` and `_app` aliases."""
    for alias_form, expected_alias in [
        ("from nicegui import app as nicegui_app\ndef bad():\n    return nicegui_app.storage.user.get('x')\n", 'nicegui_app'),
        ("from nicegui import app as _app\ndef bad():\n    return _app.storage.user.get('x')\n", '_app'),
    ]:
        tree = ast.parse(alias_form)
        aliases = _find_app_aliases(tree)
        assert aliases == {expected_alias}, f"Expected {{{expected_alias!r}}}, got {aliases} for {alias_form!r}"
        visitor = _StorageAccessVisitor(aliases, alias_form)
        visitor.visit(tree)
        assert visitor.violations, f"Alias resolution failed for {alias_form!r}"


def test_lint_does_not_double_report_nested_nodes():
    """B2 second-half regression guard: walking Call/Subscript/Attribute does not
    cause the inner Attribute to be reported a second time."""
    src = textwrap.dedent("""\
        from nicegui import app
        x = app.storage.user.get('a')
        y = app.storage.user['b']
        app.storage.user['c'] = 1
        z = app.storage.user
    """)
    tree = ast.parse(src)
    aliases = _find_app_aliases(tree)
    visitor = _StorageAccessVisitor(aliases, src)
    visitor.visit(tree)
    # 4 statements, each producing exactly 1 violation.
    # Without parent tracking, the inner Attribute(value=Attribute(...)) of the
    # Call and the two Subscripts would also be visited as bare Attributes,
    # producing 3 extra reports. The B2 parent-tracking fix prevents that.
    line_numbers = sorted({v[0] for v in visitor.violations})
    assert len(line_numbers) == 4, (
        f"Expected 4 unique violation lines, got {len(line_numbers)}: {visitor.violations}"
    )
    assert len(visitor.violations) == 4, (
        f"Expected exactly 4 violations (no double-reporting), got {len(visitor.violations)}:\n"
        + "\n".join(f"  line {ln}: {seg}" for ln, seg in visitor.violations)
    )


def test_allowlist_counts_exact():
    """H1: each allowlist pattern matches AST nodes EXACTLY its expected_count.

    Prevents the failure mode where a substring pattern like `_app.storage.user`
    silently legalizes a NEW raw access added later in the same file. Counts
    are evaluated against the post-migration codebase: in Wave 0 (before Plans
    03-06 land), this test will fail for files where migrations haven't yet
    happened. By Plan 07, all migrations are done and counts must match.

    This test is GREEN-after-Plan-07. During Wave 0 it is expected to be RED
    (alongside test_no_raw_storage_access_outside_allowlist) and that failure
    is part of the Wave 0 evidence.
    """
    allowlist = _load_allowlist()
    entries = allowlist.get('allowed_raw_access', [])
    mismatches = []
    for entry in entries:
        rel = entry['file']
        path = REPO_ROOT / rel
        if not path.exists():
            mismatches.append(f"{rel}: file does not exist on disk")
            continue
        source = path.read_text(encoding='utf-8')
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as e:
            mismatches.append(f"{rel}: AST parse failed: {e}")
            continue
        aliases = _find_app_aliases(tree)
        if not aliases:
            # No nicegui app import — actual count is 0 by definition. If the
            # allowlist still lists patterns with expected_count > 0 for this file,
            # the test must fail loudly so stale allowlist entries are caught.
            # (Fix 3 in 87-REVIEWS.md iteration 3 — Codex MEDIUM finding.)
            for pat in entry['patterns']:
                if pat['expected_count'] > 0:
                    mismatches.append(
                        f"{rel}: pattern {pat['source']!r} expected_count={pat['expected_count']} "
                        f"but file has no nicegui app import (actual count = 0). "
                        f"Either remove this allowlist entry or restore the import."
                    )
            continue
        visitor = _StorageAccessVisitor(aliases, source)
        visitor.visit(tree)
        # For each pattern, count how many violation source segments contain it.
        for pat in entry['patterns']:
            source_pat = pat['source']
            expected = pat['expected_count']
            actual = sum(1 for (_ln, seg) in visitor.violations if source_pat in seg)
            if actual != expected:
                mismatches.append(
                    f"{rel}: pattern {source_pat!r} expected_count={expected} "
                    f"but found {actual} matching AST nodes"
                )
    if mismatches:
        msg = (
            "Allowlist count mismatches (H1 enforcement):\n  "
            + "\n  ".join(mismatches)
            + "\n\nFix: either adjust expected_count in the allowlist YAML (if the new count is justified), "
              "or migrate the extra raw access site(s) to web.safe_storage helpers."
        )
        pytest.fail(msg)


def test_no_raw_storage_access_outside_allowlist():
    """FOUND-04 SC4: production code under web/ has no raw access outside allowlist.

    THIS TEST FAILS DURING WAVE 0 — migrations land in Plans 03-06.
    It must be GREEN by end of Plan 07 (Lint Finalization).
    """
    allowlist = _load_allowlist()
    allowed_map = {entry['file']: entry for entry in allowlist.get('allowed_raw_access', [])}
    violations = []
    for path in WEB_DIR.rglob('*.py'):
        if path.name == 'safe_storage.py':
            continue  # The chokepoint itself
        rel = path.relative_to(REPO_ROOT).as_posix()
        source = path.read_text(encoding='utf-8')
        try:
            file_violations = _scan_file(path, source)
        except SyntaxError as e:
            pytest.fail(f"AST parse failed for {rel}: {e}")
        for lineno, seg in file_violations:
            if _is_allowlisted(rel, seg, allowed_map):
                continue
            violations.append(f"{rel}:{lineno}: {seg or '<no segment>'}")
    if violations:
        msg = (
            "Raw app.storage.user access found outside allowlist:\n  "
            + "\n  ".join(violations[:50])  # cap at 50 for readability
            + f"\n\nTotal violations: {len(violations)}"
            + "\n\nFix: migrate to web.safe_storage helpers (safe_user_get/set/pop)"
            + " or add to .planning/phase87_storage_allowlist.yaml with justification."
        )
        pytest.fail(msg)
