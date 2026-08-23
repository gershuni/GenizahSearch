# -*- coding: utf-8 -*-
"""Every third-party import in shipped code must be a declared dependency.

Why this exists. Phase 143 added `from rapidfuzz.distance import Levenshtein`
to shared/passage_search.py and shared/passage_hygiene.py and never declared
rapidfuzz in requirements.txt or requirements-lock.txt. It survived two
adversarial review rounds, a 9,326-test local suite and a `ruff` pass,
because the dev box had rapidfuzz installed for unrelated reasons. Only CI
caught it -- as three collection errors that took every job down at once.

The blast radius was bounded by luck rather than design: web/passage_assets.py
happens to import the passage modules lazily, and returns before touching them
when PASSAGE_PARALLELS_ENABLED is off, so a deploy would not have failed at
startup. Flip that flag on a box without rapidfuzz and every passage search
500s at request time instead.

An import you did not guard is a dependency you must declare. This test says
so mechanically, so the next one cannot depend on what happens to be installed
on the machine that wrote it.

Deliberately NOT flagged, because both are real optional-import idioms this
repo uses:
  * an import inside `try:` whose handler catches ImportError (or Exception)
  * an import inside an `if TYPE_CHECKING:` block

Deliberately IS flagged: a function-local import. Laziness is an import-cost
decision, not a declaration exemption -- an unguarded lazy import of a missing
package is a 500 at call time, which is strictly harder to notice than an
ImportError at startup.
"""
from __future__ import annotations

import ast
import os
import re
import sys
from importlib.metadata import packages_distributions

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Shipped code. `scripts/` is excluded on purpose: those are dev/CI tools that
# legitimately use bake-time-only packages, and they are not what a user runs.
SHIPPED_DIRS = ('shared', 'web', 'desktop')

FIRST_PARTY = frozenset({'shared', 'web', 'desktop', 'tests', 'scripts'})

REQUIREMENTS_FILES = (
    'requirements.txt',
    'requirements-lock.txt',
    'requirements-atlas-bake.txt',
)


def _norm(name: str) -> str:
    return name.lower().replace('-', '_')


def _declared() -> set:
    """Distribution names from every requirements file, PEP-503 normalized."""
    names = set()
    for fn in REQUIREMENTS_FILES:
        path = os.path.join(ROOT, fn)
        if not os.path.exists(path):
            continue
        with open(path, encoding='utf-8') as fh:
            for line in fh:
                line = line.split('#', 1)[0].strip()
                if not line or line.startswith('-'):
                    continue
                m = re.match(r'^([A-Za-z0-9_.\-]+)', line)
                if m:
                    names.add(_norm(m.group(1)))
    return names


def _is_optional(ancestors) -> bool:
    """An import inside try/except ImportError, or under TYPE_CHECKING."""
    for node in ancestors:
        if isinstance(node, ast.Try):
            for handler in node.handlers:
                exc = handler.type
                if exc is None:
                    return True
                caught = []
                if isinstance(exc, ast.Name):
                    caught = [exc.id]
                elif isinstance(exc, ast.Tuple):
                    caught = [e.id for e in exc.elts if isinstance(e, ast.Name)]
                if 'ImportError' in caught or 'Exception' in caught:
                    return True
        if isinstance(node, ast.If) and 'TYPE_CHECKING' in ast.dump(node.test):
            return True
    return False


def _root_modules() -> set:
    return {f[:-3] for f in os.listdir(ROOT) if f.endswith('.py')}


def _imported_third_party():
    """-> {(import_name, relative_path)} for unguarded third-party imports."""
    local = FIRST_PARTY | _root_modules()
    found = set()

    def visit(node, ancestors, rel):
        for child in ast.iter_child_nodes(node):
            modules = []
            if isinstance(child, ast.Import):
                modules = [a.name.split('.')[0] for a in child.names]
            elif isinstance(child, ast.ImportFrom) and child.level == 0:
                if child.module:
                    modules = [child.module.split('.')[0]]
            for mod in modules:
                if (mod in local or mod == '__future__'
                        or mod in sys.stdlib_module_names):
                    continue
                if _is_optional(ancestors):
                    continue
                found.add((mod, rel))
            visit(child, ancestors + [child], rel)

    for directory in SHIPPED_DIRS:
        base = os.path.join(ROOT, directory)
        if not os.path.isdir(base):
            continue
        for dirpath, _dirnames, filenames in os.walk(base):
            for fn in filenames:
                if not fn.endswith('.py'):
                    continue
                path = os.path.join(dirpath, fn)
                with open(path, encoding='utf-8') as fh:
                    try:
                        tree = ast.parse(fh.read(), path)
                    except SyntaxError:  # pragma: no cover
                        continue
                visit(tree, [], os.path.relpath(path, ROOT).replace('\\', '/'))
    return found


def test_every_third_party_import_in_shipped_code_is_declared():
    declared = _declared()
    pkg_map = packages_distributions()

    undeclared = {}
    for mod, rel in sorted(_imported_third_party()):
        dists = pkg_map.get(mod)
        if not dists:
            # Not installed here, so the import name cannot be resolved to a
            # distribution. Reported rather than skipped: on a machine where
            # the suite runs at all, an unresolvable shipped import is either
            # a missing dependency or a typo, and both are defects.
            undeclared.setdefault(f'{mod} (no installed distribution)',
                                  set()).add(rel)
            continue
        if not any(_norm(d) in declared for d in dists):
            key = f'{mod} (distribution: {"/".join(sorted(dists))})'
            undeclared.setdefault(key, set()).add(rel)

    assert not undeclared, (
        'shipped code imports these without declaring them in '
        + ' / '.join(REQUIREMENTS_FILES) + ':\n'
        + '\n'.join(f'  {k}\n      imported by: {", ".join(sorted(v))}'
                    for k, v in sorted(undeclared.items()))
    )


def test_the_scanner_actually_looks_at_the_passage_modules():
    """A guard against the gate silently scanning nothing.

    The expensive failure mode for a check like this is not a false positive
    -- it is a walk that matches no files and passes forever. Pin the two
    modules whose undeclared import motivated it.
    """
    scanned = {rel for _mod, rel in _imported_third_party()}
    for expected in ('shared/passage_search.py', 'shared/passage_hygiene.py'):
        assert expected in scanned, (
            f'{expected} was not scanned -- the walk is not reaching shipped '
            f'code, so this gate cannot fail'
        )


def test_rapidfuzz_specifically_is_declared():
    """The regression this file was written for, named explicitly so a future
    dependency reshuffle that drops it fails by name rather than as a generic
    scanner miss."""
    assert 'rapidfuzz' in _declared(), (
        'rapidfuzz is imported by shared/passage_search.py and '
        'shared/passage_hygiene.py; removing its declaration breaks every '
        'CI job at collection time'
    )
