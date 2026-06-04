# -*- coding: utf-8 -*-
"""Static AST guard: desktop/join_workbench.py must not call any _vs_* private methods.

SC#5 invariant: all actions in the Join Workbench go through public wrappers
(open_anchor_in_puzzle, open_anchor_as_join, etc.). A _vs_* call would couple the
workbench to GenizahGUI private internals.

Pattern source: tests/test_pgp_filter_cascade.py (AST scanner) and
tests/test_web_library_options_no_local.py (file-level offender pattern).
"""
import ast
import pathlib

TARGET = pathlib.Path(__file__).parent.parent / "desktop" / "join_workbench.py"


def _iter_calls(tree):
    """Yield (callee_name, lineno) for every Call node in the AST."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            callee = node.func
            if isinstance(callee, ast.Attribute):
                yield callee.attr, node.lineno
            elif isinstance(callee, ast.Name):
                yield callee.id, node.lineno


def test_no_vs_private_calls_in_join_workbench():
    """SC#5: join_workbench.py must not call _vs_* private methods directly."""
    source = TARGET.read_text(encoding="utf-8")
    tree = ast.parse(source)
    offenders = [
        (name, lineno)
        for name, lineno in _iter_calls(tree)
        if name.startswith("_vs_")
    ]
    assert not offenders, (
        "SC#5 violation — desktop/join_workbench.py calls _vs_* private methods:\n"
        + "\n".join(f"  {name}() at line {lineno}" for name, lineno in offenders)
        + "\n\nFix: call the public wrapper (open_anchor_in_puzzle / open_anchor_as_join) instead."
    )


def test_target_file_exists():
    """Guard against the file being accidentally removed."""
    assert TARGET.exists(), f"desktop/join_workbench.py not found at {TARGET}"
