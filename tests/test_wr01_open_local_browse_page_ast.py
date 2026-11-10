"""WR-01 AST guards for _open_local_browse_page (Phase 101 / REVIEWS round 2 BLOCKER #4).

REV-2b reachability: exactly ONE binding of the name `filepath` across all rebind
                     forms (Assign / AugAssign / AnnAssign / NamedExpr-walrus /
                     tuple-or-list unpacking / for-loop target / except-as target).
REV-2d uniqueness:   exactly ONE definition of _open_local_browse_page.
Single-lookup:       exactly ONE call to self._lookup_local_filepath inside it.
"""
import ast


def _names_in_target(t):
    """Yield every `Name.id` reachable inside an assignment-target subtree."""
    if isinstance(t, ast.Name):
        yield t.id
    elif isinstance(t, (ast.Tuple, ast.List)):
        for elt in t.elts:
            yield from _names_in_target(elt)
    elif isinstance(t, ast.Starred):
        yield from _names_in_target(t.value)


def _get_method():
    src = open('genizah_app.py', encoding='utf-8').read()
    tree = ast.parse(src)
    fns = [
        n for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        and n.name == '_open_local_browse_page'
    ]
    return fns


def test_open_local_browse_page_single_definition():
    """REV-2d: exactly one _open_local_browse_page definition exists."""
    fns = _get_method()
    assert len(fns) == 1, (
        f'REV-2d uniqueness: expected exactly one _open_local_browse_page '
        f'definition in genizah_app.py, found {len(fns)}'
    )


def test_open_local_browse_page_single_lookup_call():
    """WR-01: exactly one self._lookup_local_filepath call inside the method."""
    fn = _get_method()[0]
    calls = [
        c for c in ast.walk(fn)
        if isinstance(c, ast.Call)
        and isinstance(c.func, ast.Attribute)
        and c.func.attr == '_lookup_local_filepath'
    ]
    assert len(calls) == 1, (
        f'WR-01 single-lookup: expected 1 self._lookup_local_filepath call '
        f'inside _open_local_browse_page, found {len(calls)}'
    )


def test_open_local_browse_page_single_filepath_assignment():
    """REV-2b (STRENGTHENED per REVIEWS round 2 BLOCKER #4): exactly ONE
    binding of the name `filepath` across ALL rebind forms.
    """
    fn = _get_method()[0]
    assigns = 0
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if 'filepath' in set(_names_in_target(t)):
                    assigns += 1
        elif isinstance(node, ast.AugAssign):
            if isinstance(node.target, ast.Name) and node.target.id == 'filepath':
                assigns += 1
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == 'filepath':
                assigns += 1
        elif isinstance(node, ast.NamedExpr):
            if isinstance(node.target, ast.Name) and node.target.id == 'filepath':
                assigns += 1
        elif isinstance(node, ast.For):
            if 'filepath' in set(_names_in_target(node.target)):
                assigns += 1
        elif isinstance(node, ast.ExceptHandler) and node.name == 'filepath':
            assigns += 1
    assert assigns == 1, (
        f'WR-01 reachability (REV-2b, round-2-strengthened): expected exactly 1 '
        f'binding of `filepath` across ALL rebind forms (Assign / AugAssign / '
        f'AnnAssign / NamedExpr / unpack / For / except-as), found {assigns}'
    )
