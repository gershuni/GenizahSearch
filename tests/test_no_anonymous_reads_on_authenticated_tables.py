# -*- coding: utf-8 -*-
"""Static AST scanner -- Phase 92.1 READER-04 permanent CI guard.

Bans:
  Any Call whose receiver chain resolves to `get_client()` (literal chain
  OR local-variable alias) AND whose `.table(<name>)` first-argument constant
  is in BANNED_TABLES. These tables have RLS SELECT policies
  `TO authenticated USING (auth.uid() = user_id)` per
  docs/guides/SUPABASE_GUIDE.md:429-451 -- the anonymous singleton role
  gets 0 rows. Use `get_user_client()` instead; it falls back to the
  anonymous singleton internally when no auth_session is present, so
  anonymous browsers reading `TO public` tables (profiles, fragment_joins)
  remain unaffected.

Mirrors tests/test_no_set_session_outside_oauth.py (Phase 90 D-15 scanner)
in style: REPO_ROOT, SCAN_DIRS, EXEMPT_FILES, _build_parent_map,
_enclosing_function_name, _iter_py_files, _scan_file helpers,
SEED_TRAP_SNIPPETS parametrize block.

Discovered 2026-05-17 during Phase 92 SWEEP-05 smoke run 1 -- Phase 90
closed the multitenant SIGNED_IN-event-listener leak (Codex F3) but did
not migrate ~12 reader functions; logged-in users saw empty lists.

Known scanner blind spots (Reviews M scanner-completeness, 2026-05-17):
  - module-level aliases: `table = get_client().table; table('user_lists')`
  - wrapper helpers: `def _q(name): return get_client().table(name)`
Both are seed-trapped with xfail-strict so a future scanner upgrade flips
them to PASS. Until then, code review remains the second line of defense
for these patterns.

See:
  - .planning/phases/92.1-reader-client-retrofit/92.1-01-PLAN.md
  - .planning/phases/92-final-sweep-and-acceptance/92-SWEEP-05-SMOKE.md
    ("Smoke run 1 -- 2026-05-17 -- FAILED at R0" block)
  - .planning/phases/90-auth-caching-rewrite-no-set-session/90-DISCUSSION-LOG.md:147
    (the false-assumption smoking gun)
  - docs/guides/SUPABASE_GUIDE.md:429-451 (RLS policy SQL)
  - docs/guides/SUPABASE_GUIDE.md BANNED_TABLES extension protocol section
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCAN_DIRS = ['web', 'tests']
EXEMPT_FILES = {
    # This file references the banned patterns as parsed strings + AST node
    # snippets; scanning it would self-flag the seed traps.
    'tests/test_no_anonymous_reads_on_authenticated_tables.py',
}

# Tables whose RLS SELECT policy is `TO authenticated USING (auth.uid() = user_id)`.
# Anon role returns 0 rows. Source: docs/guides/SUPABASE_GUIDE.md:429-451 (user_lists
# block is canonical) + same policy shape applied to list_items, recent_items, projects.
# Extension protocol: see SUPABASE_GUIDE.md "BANNED_TABLES extension protocol" section.
BANNED_TABLES = {'user_lists', 'list_items', 'recent_items', 'projects'}


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


def _is_get_client_call(call_node):
    if not isinstance(call_node, ast.Call):
        return False
    func = call_node.func
    if isinstance(func, ast.Name) and func.id == 'get_client':
        return True
    if isinstance(func, ast.Attribute) and func.attr == 'get_client':
        return True
    return False


def _collect_get_client_aliases(funcdef):
    aliases = set()
    for node in ast.walk(funcdef):
        if isinstance(node, ast.Assign) and _is_get_client_call(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    aliases.add(target.id)
    return aliases


def _extract_table_name(call_node):
    """For a Call like X.table('foo'), return 'foo' if first arg is a string constant; else None."""
    if not isinstance(call_node, ast.Call):
        return None
    if not call_node.args:
        return None
    first = call_node.args[0]
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return first.value
    return None


def _violations(tree, file_relpath):
    violations = []

    # Pass 1: literal chain get_client().table('<banned>')
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == 'table'):
            continue
        table_name = _extract_table_name(node)
        if table_name not in BANNED_TABLES:
            continue
        receiver = func.value
        if isinstance(receiver, ast.Call) and _is_get_client_call(receiver):
            violations.append({
                'file': file_relpath,
                'line': node.lineno,
                'table': table_name,
                'pattern': "get_client().table('%s')" % table_name,
            })

    # Pass 2: intra-function alias tracking. `client = get_client(); client.table('<banned>')`
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
            if not (isinstance(func, ast.Attribute) and func.attr == 'table'):
                continue
            table_name = _extract_table_name(node)
            if table_name not in BANNED_TABLES:
                continue
            receiver = func.value
            if isinstance(receiver, ast.Name) and receiver.id in aliases:
                violations.append({
                    'file': file_relpath,
                    'line': node.lineno,
                    'table': table_name,
                    'pattern': "%s = get_client(); %s.table('%s')" % (receiver.id, receiver.id, table_name),
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


def test_no_anonymous_reads_on_banned_tables():
    all_violations = []
    for path, relpath in _iter_py_files():
        tree = _scan_file(path)
        all_violations.extend(_violations(tree, relpath))
    if all_violations:
        msg = (
            "Phase 92.1 READER-04 violations -- found %d disallowed "
            "`get_client().table(<authenticated-only table>)` reads. "
            "These tables have RLS SELECT policy `TO authenticated` and "
            "the anonymous singleton role returns 0 rows. Use "
            "`get_user_client()` instead (it falls back to the anonymous "
            "singleton internally when no auth_session is present, so "
            "anonymous browsers reading `TO public` tables stay unaffected).\n\n"
            "Violations:\n" % len(all_violations)
            + "\n".join(
                "  %s:%s -- %s (%s)" % (v['file'], v['line'], v['table'], v['pattern'])
                for v in all_violations
            )
        )
        pytest.fail(msg)


SEED_TRAP_SNIPPETS = [
    ('literal_user_lists',  "get_client().table('user_lists').select('*')"),
    ('literal_list_items',  "get_client().table('list_items').select('*')"),
    ('literal_recent',      "get_client().table('recent_items').select('*')"),
    ('literal_projects',    "get_client().table('projects').select('*')"),
    ('aliased_client',      "def f():\n    client = get_client()\n    return client.table('user_lists').select('*').execute()"),
    ('aliased_short',       "def f():\n    c = get_client()\n    return c.table('projects').select('*').execute()"),
]


@pytest.mark.parametrize('trap_id,snippet', SEED_TRAP_SNIPPETS)
def test_seed_traps_are_flagged(trap_id, snippet):
    tree = ast.parse(snippet)
    synthetic_path = 'web/_synthetic_seed_trap_reader.py'
    v = _violations(tree, synthetic_path)
    assert len(v) >= 1, (
        "Seed trap %r was NOT flagged. Scanner has a false-negative gap. "
        "Snippet:\n%s\nHits: %s" % (trap_id, snippet, v)
    )


# Reviews M scanner-completeness (2026-05-17): known blind spots.
# These bypass patterns are documented but currently NOT detected by the scanner.
# The xfail-strict assertion locks the blind spot in place: if a future scanner
# upgrade detects them, the test flips RED so the blind-spot doc gets updated.
BLIND_SPOT_SNIPPETS = [
    ('module_level_alias',
     "table = get_client().table\ntable('user_lists').select('*')"),
    ('wrapper_helper',
     "def _q(name):\n    return get_client().table(name)\n_q('user_lists').select('*')"),
]


@pytest.mark.parametrize('trap_id,snippet', BLIND_SPOT_SNIPPETS)
@pytest.mark.xfail(strict=True, reason="Known scanner blind spot (Reviews M, 2026-05-17). "
                                        "Code review is the second line of defense. "
                                        "If a future scanner upgrade detects these, this xfail flips "
                                        "to XPASS and forces a docstring update.")
def test_blind_spot_bypass_patterns_currently_undetected(trap_id, snippet):
    tree = ast.parse(snippet)
    synthetic_path = 'web/_synthetic_blind_spot_%s.py' % trap_id
    v = _violations(tree, synthetic_path)
    assert len(v) >= 1, (
        "Blind spot %r unexpectedly detected -- scanner has been improved. "
        "Update the docstring + remove xfail. Hits: %s" % (trap_id, v)
    )


def test_partial_auth_tables_not_in_banned_set():
    """corrections and comments have BOTH `TO public` (status='approved' / is_public=true)
    AND `TO authenticated` (own pending / own private) RLS branches. They are deliberately
    NOT in BANNED_TABLES because their `get_client()` reads are valid for the public branch.
    The per-function migration (lines 1067, 1171, 1191) handles the user-private case
    by switching to get_user_client(). The scanner should NOT flag generic
    `get_client().table('corrections')` reads -- those are valid for status='approved' only.

    Reviews M scanner (2026-05-17): if a future schema change extends `corrections` or
    `comments` to a user-private-only filter (no public branch), THIS test must flip:
    add the table to BANNED_TABLES."""
    snippet = "get_client().table('corrections').select('*').eq('status', 'approved').execute()"
    tree = ast.parse(snippet)
    v = _violations(tree, 'web/_synthetic_partial_auth.py')
    assert v == [], (
        "False positive: get_client().table('corrections') was flagged. "
        "corrections is a partial-auth table with valid public branch. Hits: %s" % v
    )


def test_get_user_client_call_is_not_flagged():
    """Positive control: get_user_client() chains must NEVER be flagged."""
    snippet = "def f():\n    c = get_user_client()\n    return c.table('user_lists').select('*').execute()"
    tree = ast.parse(snippet)
    v = _violations(tree, 'web/_synthetic_positive_control.py')
    assert v == [], (
        "False positive: get_user_client() chain was flagged. Violations: %s" % v
    )


def test_exempt_files_includes_self():
    assert 'tests/test_no_anonymous_reads_on_authenticated_tables.py' in EXEMPT_FILES
