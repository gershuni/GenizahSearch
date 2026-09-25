# -*- coding: utf-8 -*-
"""Static AST guard: no Supabase WRITE may go through the anonymous singleton.

Bans, in ``web/`` and ``shared/``:
  any ``.insert`` / ``.update`` / ``.delete`` / ``.upsert`` / ``.rpc`` call
  whose receiver chain is rooted at the literal anonymous singleton
  ``get_client()`` -- either written out (``get_client().table('x').delete()``)
  or through a name bound to ``get_client()`` in an enclosing function
  (``c = get_client(); c.table('x').update(...)``).

Why: ``get_client()`` carries no user JWT, so ``auth.uid()`` is NULL inside
Postgres. Every owner- or admin-scoped policy then filters the target rows to
zero, PostgREST answers 200 with ``[]``, and the page shows a success toast
for a change that never happened. That is exactly how the /corrections page
lost draft deletes and comment edits/deletes, and how the admin join delete in
``web/components/joins_panel.py`` reported success without deleting.

What this guard does NOT claim: that no anonymous write exists. Community
identification reviews are a deliberate anonymous-capable RPC
(``web/components/identification_review.py`` builds ``get_user_client()``,
which falls back to the anonymous singleton for a logged-out visitor, and
passes it as ``client=``). A write that is MEANT to work anonymously must be
expressed that way -- through ``get_user_client()`` or an explicit ``client=``
argument -- never by reaching for the literal ``get_client()``.

Scope rules:
  - Aliases are scoped per function: a name counts as the anonymous client at
    a call site only if ``name = get_client()`` is assigned in that function's
    own body or in an enclosing function's own body (lexical closure), or at
    module level. Walking a whole Module as one scope floods the scan with
    false positives.
  - Hits are de-duplicated by (file, line).

Known blind spots (the same ones documented in
tests/test_no_anonymous_reads_on_authenticated_tables.py):
  - a client received as a parameter (verified by reading the callers:
    web/pages/admin.py, shared/puzzle_publish_service.py,
    web/discovery_suppression.py, web/identification_reviews.py);
  - a wrapper helper that returns ``get_client().table(name)``.
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCAN_DIRS = ['web', 'shared']
WRITE_METHODS = {'insert', 'update', 'delete', 'upsert', 'rpc'}
_FUNC_TYPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)


def _is_get_client_call(node):
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name) and func.id == 'get_client':
        return True
    if isinstance(func, ast.Attribute) and func.attr == 'get_client':
        return True
    return False


def _build_parent_map(tree):
    parents = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _own_nodes(scope):
    """Yield the nodes of ``scope`` without descending into nested functions/classes."""
    stack = list(ast.iter_child_nodes(scope))
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, _FUNC_TYPES + (ast.ClassDef,)):
            continue
        stack.extend(ast.iter_child_nodes(node))


def _scope_names(scope):
    """Return (aliases, bound): names bound to get_client() here, and all names bound here.

    ``bound`` lets an inner binding (``c = get_user_client()``, a parameter
    ``c``) shadow an outer ``c = get_client()``.
    """
    aliases, bound = set(), set()
    if isinstance(scope, _FUNC_TYPES):
        a = scope.args
        for arg in a.posonlyargs + a.args + a.kwonlyargs + [a.vararg, a.kwarg]:
            if arg is not None:
                bound.add(arg.arg)
    for node in _own_nodes(scope):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)
        if isinstance(node, ast.Assign) and _is_get_client_call(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    aliases.add(target.id)
        elif (isinstance(node, (ast.AnnAssign, ast.NamedExpr))
              and node.value is not None and _is_get_client_call(node.value)
              and isinstance(node.target, ast.Name)):
            aliases.add(node.target.id)
    return aliases, bound


def _chain_root(expr):
    """Follow a method chain down to its root: a get_client() Call, a Name, or other."""
    while True:
        if isinstance(expr, ast.Call):
            if _is_get_client_call(expr):
                return expr
            expr = expr.func
        elif isinstance(expr, (ast.Attribute, ast.Subscript)):
            expr = expr.value
        else:
            return expr


def _enclosing_scopes(node, parents):
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, _FUNC_TYPES + (ast.Module,)):
            yield cur
        cur = parents.get(cur)


def _violations(tree, file_relpath):
    parents = _build_parent_map(tree)
    alias_cache = {}
    seen = set()
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr in WRITE_METHODS):
            continue
        root = _chain_root(func.value)
        pattern = None
        if isinstance(root, ast.Call):
            pattern = 'get_client()...%s(...)' % func.attr
        elif isinstance(root, ast.Name):
            for scope in _enclosing_scopes(node, parents):
                if scope not in alias_cache:
                    alias_cache[scope] = _scope_names(scope)
                aliases, bound = alias_cache[scope]
                if root.id in aliases:
                    pattern = '%s = get_client(); %s...%s(...)' % (root.id, root.id, func.attr)
                    break
                if root.id in bound:
                    break  # bound to something else in this scope: shadows outer aliases
        if pattern is None:
            continue
        key = (file_relpath, node.lineno)
        if key in seen:
            continue
        seen.add(key)
        violations.append({'file': file_relpath, 'line': node.lineno, 'pattern': pattern})
    return violations


def _iter_py_files():
    for scan_dir in SCAN_DIRS:
        base = REPO_ROOT / scan_dir
        if not base.exists():
            continue
        for path in sorted(base.rglob('*.py')):
            yield path, path.relative_to(REPO_ROOT).as_posix()


def test_no_anonymous_client_writes_in_web():
    all_violations = []
    for path, relpath in _iter_py_files():
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        all_violations.extend(_violations(tree, relpath))
    if all_violations:
        pytest.fail(
            "Found %d Supabase write(s) through the anonymous singleton get_client(). "
            "RLS filters those to 0 rows and the page reports a success that never "
            "happened. Use get_user_client() (on the event loop) or an explicit client= "
            "argument, and check response.data.\n\n" % len(all_violations)
            + "\n".join("  %s:%s -- %s" % (v['file'], v['line'], v['pattern'])
                        for v in all_violations)
        )


SEED_TRAP_SNIPPETS = [
    ('literal_delete', "get_client().table('comments').delete().eq('id', 1).execute()"),
    ('literal_insert', "get_client().table('comments').insert({}).execute()"),
    ('literal_upsert', "get_client().table('comments').upsert({}).execute()"),
    ('literal_rpc', "get_client().rpc('submit_review', {}).execute()"),
    ('module_attr_literal', "sb.get_client().table('comments').delete().eq('id', 1).execute()"),
    ('aliased_update', "def f():\n    c = get_client()\n    c.table('comments').update({}).eq('id', 1).execute()"),
    ('aliased_delete', "def f():\n    client = get_client()\n    client.table('corrections').delete().eq('id', 1).execute()"),
    ('async_aliased', "async def f():\n    c = get_client()\n    c.table('comments').delete().eq('id', 1).execute()"),
    ('closure_alias', "def outer():\n    c = get_client()\n    def inner():\n        c.table('comments').delete().eq('id', 1).execute()\n    return inner"),
]


@pytest.mark.parametrize('trap_id,snippet', SEED_TRAP_SNIPPETS)
def test_seed_traps_are_flagged(trap_id, snippet):
    """Regression guard, green before and after: proves the scanner can fail."""
    v = _violations(ast.parse(snippet), 'web/_synthetic_seed_trap_writer.py')
    assert len(v) >= 1, "Seed trap %r was NOT flagged. Snippet:\n%s" % (trap_id, snippet)


def test_nested_closure_hits_are_deduplicated():
    """One call site inside nested functions is reported once, not once per scope."""
    snippet = (
        "def a():\n"
        "    def b():\n"
        "        def c():\n"
        "            get_client().table('comments').delete().eq('id', 1).execute()\n"
        "        return c\n"
        "    return b\n"
    )
    v = _violations(ast.parse(snippet), 'web/_synthetic_nested.py')
    assert len(v) == 1, v


NEGATIVE_CONTROLS = [
    ('user_client_literal', "get_user_client().table('comments').delete().eq('id', 1).execute()"),
    ('user_client_alias', "def f():\n    c = get_user_client()\n    c.table('comments').update({}).eq('id', 1).execute()"),
    ('anon_read_only', "def f():\n    c = get_client()\n    return c.table('profiles').select('*').execute()"),
    ('dict_update', "def f():\n    d = {}\n    d.update({'a': 1})"),
    ('alias_in_sibling_function', "def f():\n    c = get_client()\n    return c\ndef g(c):\n    c.table('comments').delete().eq('id', 1).execute()"),
    ('shadowed_in_inner', "def outer():\n    c = get_client()\n    def inner():\n        c = get_user_client()\n        c.table('comments').delete().eq('id', 1).execute()\n    return inner"),
]


@pytest.mark.parametrize('ctrl_id,snippet', NEGATIVE_CONTROLS)
def test_negative_controls_are_not_flagged(ctrl_id, snippet):
    """Regression guard, green before and after: no false positives on correct code."""
    v = _violations(ast.parse(snippet), 'web/_synthetic_negative_control.py')
    assert v == [], "False positive for %r: %s" % (ctrl_id, v)
