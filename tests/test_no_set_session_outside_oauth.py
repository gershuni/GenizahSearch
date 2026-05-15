"""Static AST scanner -- Phase 90 D-15 permanent CI guard.

Bans:
  Class A: any Call whose terminal attribute is `set_session` or
    `exchange_code_for_session`, EXCEPT inside the per-method allowlisted
    enclosing FunctionDef (set_session inside set_session_from_url;
    exchange_code_for_session inside exchange_code_for_session -- both in
    web/supabase_client.py).
  Class B: any Call whose receiver chain begins with `get_client()` AND
    traverses `.auth` AND whose terminal attribute is in CLASS_B_METHODS
    (set_session, sign_in_with_password, sign_in_with_oauth,
    sign_in_with_otp, sign_up, exchange_code_for_session, refresh_session,
    update_user, sign_out). No allowlist -- singleton resurrection is
    always wrong (Codex F3).

Mirrors Phase 88's test_no_deleted_state_references.py (Attribute scanner)
and Phase 89's test_no_deleted_lists_state_references.py (Attribute +
FunctionDef + Call scanner), widening the disallowed-node surface to
arbitrary Call invocations.

See:
  - .planning/phases/90-auth-caching-rewrite-no-set-session/90-CONTEXT.md D-15
  - supabase_auth/_sync/gotrue_client.py:713 (set_session is networked)
  - supabase/_sync/client.py:338-346 (singleton event-listener leak -- Codex F3)
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCAN_DIRS = ['web', 'tests']
EXEMPT_FILES = {
    # This file references the banned names as parsed strings + AST node
    # patterns; scanning it would self-flag the seed traps.
    'tests/test_no_set_session_outside_oauth.py',
}

CLASS_A_METHODS = {'set_session', 'exchange_code_for_session'}
CLASS_A_ALLOWLIST = {
    # method_name -> (enclosing_function_name, expected_file_suffix)
    'set_session': ('set_session_from_url', 'web/supabase_client.py'),
    'exchange_code_for_session': ('exchange_code_for_session', 'web/supabase_client.py'),
}

CLASS_B_METHODS = {
    'set_session', 'sign_in_with_password', 'sign_in_with_oauth',
    'sign_in_with_otp', 'sign_up', 'exchange_code_for_session',
    'refresh_session', 'update_user', 'sign_out',
}


def _build_parent_map(tree):
    parents = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _enclosing_function_name(node, parents):
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return cur.name
        cur = parents.get(cur)
    return None


def _class_a_violations(tree, file_relpath):
    parents = _build_parent_map(tree)
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        method = func.attr
        if method not in CLASS_A_METHODS:
            continue
        enclosing = _enclosing_function_name(node, parents)
        allowed_fn, allowed_file = CLASS_A_ALLOWLIST[method]
        if enclosing == allowed_fn and file_relpath.endswith(allowed_file):
            continue  # allowlisted use
        violations.append({
            'file': file_relpath,
            'line': node.lineno,
            'method': method,
            'enclosing': enclosing,
        })
    return violations


def _is_get_client_call(call_node):
    """Match get_client(...) -- bare Name or any .get_client attribute access."""
    if not isinstance(call_node, ast.Call):
        return False
    func = call_node.func
    if isinstance(func, ast.Name) and func.id == 'get_client':
        return True
    if isinstance(func, ast.Attribute) and func.attr == 'get_client':
        return True
    return False


def _collect_get_client_aliases(funcdef):
    """Return set of local-variable names assigned the result of get_client(...) within `funcdef`."""
    aliases = set()
    for node in ast.walk(funcdef):
        if isinstance(node, ast.Assign) and _is_get_client_call(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    aliases.add(target.id)
    return aliases


def _class_b_violations(tree, file_relpath):
    violations = []

    # Pass 1: literal chain `get_client().auth.<method>(...)`.
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        terminal_method = func.attr
        if terminal_method not in CLASS_B_METHODS:
            continue
        mid = func.value
        if not (isinstance(mid, ast.Attribute) and mid.attr == 'auth'):
            continue
        base = mid.value
        if isinstance(base, ast.Call) and _is_get_client_call(base):
            violations.append({
                'file': file_relpath,
                'line': node.lineno,
                'method': terminal_method,
                'pattern': 'get_client().auth.<method>(...)',
            })

    # Pass 2 (Codex review round 1 M4): intra-function alias tracking.
    for funcdef in ast.walk(tree):
        if not isinstance(funcdef, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        aliases = _collect_get_client_aliases(funcdef)
        if not aliases:
            continue
        for node in ast.walk(funcdef):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute):
                continue
            terminal_method = func.attr
            if terminal_method not in CLASS_B_METHODS:
                continue
            mid = func.value
            if not (isinstance(mid, ast.Attribute) and mid.attr == 'auth'):
                continue
            base = mid.value
            if isinstance(base, ast.Name) and base.id in aliases:
                violations.append({
                    'file': file_relpath,
                    'line': node.lineno,
                    'method': terminal_method,
                    'pattern': f'{base.id} = get_client(); {base.id}.auth.<method>(...)',
                })
    return violations


def _iter_py_files():
    for scan_dir in SCAN_DIRS:
        base = REPO_ROOT / scan_dir
        if not base.exists():
            continue
        for path in base.rglob('*.py'):
            relpath = path.relative_to(REPO_ROOT).as_posix()
            if relpath in EXEMPT_FILES:
                continue
            yield path, relpath


def _scan_file(path):
    source = path.read_text(encoding='utf-8')
    return ast.parse(source, filename=str(path))


def test_no_set_session_class_a_violations():
    all_violations = []
    for path, relpath in _iter_py_files():
        tree = _scan_file(path)
        all_violations.extend(_class_a_violations(tree, relpath))
    if all_violations:
        msg = (
            f"Phase 90 D-15 Class A violations -- found {len(all_violations)} "
            "disallowed `set_session(...)` / `exchange_code_for_session(...)` "
            "calls outside their per-method allowlisted helpers. "
            "set_session is networked (gotrue_client.py:713) and must NOT "
            "be called mid-flight. Allowlist: set_session inside "
            "set_session_from_url; exchange_code_for_session inside "
            "exchange_code_for_session. Both in web/supabase_client.py.\n\n"
            "Violations:\n"
            + "\n".join(f"  {v['file']}:{v['line']} -- {v['method']} (enclosing: {v['enclosing']!r})" for v in all_violations)
        )
        pytest.fail(msg)


def test_no_get_client_class_b_violations():
    all_violations = []
    for path, relpath in _iter_py_files():
        tree = _scan_file(path)
        all_violations.extend(_class_b_violations(tree, relpath))
    if all_violations:
        msg = (
            f"Phase 90 D-15 Class B violations -- found {len(all_violations)} "
            "singleton-resurrection vectors `get_client().auth.<method>(...)`. "
            "The module singleton MUST remain anonymous-only (Codex F3: "
            "supabase/_sync/client.py:338-346 event listener mutates "
            "singleton headers on SIGNED_IN/TOKEN_REFRESHED). Use a "
            "throwaway: `c = create_client(...); c.auth.<method>(...)`.\n\n"
            "Violations:\n"
            + "\n".join(f"  {v['file']}:{v['line']} -- {v['method']} ({v['pattern']})" for v in all_violations)
        )
        pytest.fail(msg)


SEED_TRAP_SNIPPETS = [
    # Class A traps -- auth-mutating method anywhere outside the 2 helpers
    ('class_a_direct',         "client.auth.set_session(a, r)"),
    ('class_a_short_alias',    "c.auth.set_session(a, r)"),
    ('class_a_aliased',        "auth = client.auth\nauth.set_session(a, r)"),
    ('class_a_oauth_direct',   "client.auth.exchange_code_for_session({})"),
    ('class_a_oauth_aliased',  "auth = c.auth\nauth.exchange_code_for_session({})"),
    # Class B traps -- get_client() resurrection vectors (literal chain)
    ('class_b_set_session',    "get_client().auth.set_session(a, r)"),
    ('class_b_sign_in',        "get_client().auth.sign_in_with_password({})"),
    ('class_b_oauth',          "get_client().auth.exchange_code_for_session({})"),
    ('class_b_refresh',        "get_client().auth.refresh_session(r)"),
    ('class_b_update_user',    "get_client().auth.update_user({})"),
    # Class B traps -- aliased pattern (Codex review round 1 M4)
    # Each must be inside a FunctionDef for intra-function alias
    # tracking to fire; bare module-level assignments are NOT tracked.
    ('class_b_aliased_sign_out',
        "def f():\n    c = get_client()\n    c.auth.sign_out()"),
    ('class_b_aliased_sign_in',
        "def f():\n    client = get_client()\n    client.auth.sign_in_with_password({})"),
    ('class_b_aliased_update_user',
        "def f():\n    c = get_client()\n    c.auth.update_user({})"),
]


@pytest.mark.parametrize('trap_id,snippet', SEED_TRAP_SNIPPETS)
def test_seed_traps_are_flagged(trap_id, snippet):
    tree = ast.parse(snippet)
    # For seed-trap testing, simulate an enclosing file path that is NOT
    # in CLASS_A_ALLOWLIST and NOT exempt -- use a synthetic path so
    # neither Class A allowlist nor EXEMPT_FILES short-circuits.
    synthetic_path = 'web/_synthetic_seed_trap.py'
    a = _class_a_violations(tree, synthetic_path)
    b = _class_b_violations(tree, synthetic_path)
    total = len(a) + len(b)
    assert total >= 1, (
        f"Seed trap {trap_id!r} was NOT flagged by either Class A or "
        f"Class B scanner. Scanner has a false-negative gap. "
        f"Snippet:\n{snippet}\n"
        f"Class A hits: {a}\nClass B hits: {b}"
    )


def test_exempt_files_includes_self():
    # Sanity: this test file references all the banned names as strings
    # and as parsed snippets. If we forgot to exempt ourselves, the
    # main tests would fail on this very file.
    assert 'tests/test_no_set_session_outside_oauth.py' in EXEMPT_FILES
