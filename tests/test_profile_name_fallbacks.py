# -*- coding: utf-8 -*-
"""A profile's username, full name and affiliation may be NULL: never rely on
``.get(key, default)`` for them.

The column is there with a NULL in it, so ``profile.get('username', 'Unknown')``
returns None and the page shows "None" (or nothing). From 2026-10-21 most
usernames are NULL: migrations/profiles_no_email_username.sql stops putting
the account's email address there and clears the 187 it found. Write
``profile.get('username') or 'Unknown'``.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NULLABLE = {'username', 'full_name', 'affiliation'}

#: (file, enclosing function) whose dict is not a profile row.
ALLOWED = {
    ('web/auth_state.py', 'api_call'),  # the legacy /auth/register request body
}


def _enclosing(tree, lineno):
    best = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.lineno <= lineno <= getattr(node, 'end_lineno', node.lineno):
                if best is None or node.lineno >= best.lineno:
                    best = node
    return best.name if best else None


def _violations(sources):
    bad = []
    for rel, src in sources.items():
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == 'get' and len(node.args) == 2
                    and isinstance(node.args[0], ast.Constant) and node.args[0].value in NULLABLE
                    and not (isinstance(node.args[1], ast.Constant) and node.args[1].value is None)):
                if (rel, _enclosing(tree, node.lineno)) not in ALLOWED:
                    bad.append(f'{rel}:{node.lineno}: {ast.unparse(node)}')
    return bad


def test_the_check_flags_a_default_that_never_applies():
    sources = {
        'web/x.py': "def f(p):\n    return p.get('username', 'Unknown')\n",
        'web/y.py': "def g(p):\n    return p.get('username') or 'Unknown'\n",
        'web/auth_state.py': "def api_call(data):\n    return data.get('username', '')\n",
    }
    assert _violations(sources) == ["web/x.py:2: p.get('username', 'Unknown')"]


def test_web_never_defaults_a_nullable_profile_name():
    sources = {p.relative_to(ROOT).as_posix(): p.read_text(encoding='utf-8')
               for p in (ROOT / 'web').rglob('*.py') if '__pycache__' not in p.parts}
    bad = _violations(sources)
    assert not bad, ('a NULL column makes these defaults never apply; use `or`:\n  '
                     + '\n  '.join(bad))
