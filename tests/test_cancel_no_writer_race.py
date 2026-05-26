# -*- coding: utf-8 -*-
"""Phase 97.1 — discard_run/keep_run must NOT run before worker.finished_signal.

Prior to Phase 97.1, _on_cancel_clicked called wait(5000) then
discard_run unconditionally — racing the Tantivy writer between the UI
thread and a still-running scan thread.

This is an AST/source-level regression: rather than depend on a live PyQt
event loop (which is slow + flaky in CI), we statically verify that the
fix-2a flow holds — discard_run/keep_run no longer appears synchronously
inside _on_cancel_clicked, only inside the deferred finished-signal slot.

Debug session: `.planning/debug/phase-97-freeze-winerror-3.md`.
"""
import ast
import os
import pathlib


_THIS = pathlib.Path(__file__).resolve()
_REPO_ROOT = _THIS.parent.parent
_MY_LIB = _REPO_ROOT / "desktop" / "my_library_tab.py"


def _func_source(tree: ast.AST, name: str) -> ast.FunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name} not found in {_MY_LIB}")


def _calls_in(node: ast.AST) -> set:
    """Return the set of called attribute names inside this AST subtree."""
    calls = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            f = sub.func
            if isinstance(f, ast.Attribute):
                calls.add(f.attr)
            elif isinstance(f, ast.Name):
                calls.add(f.id)
    return calls


def test_on_cancel_clicked_does_not_call_discard_run_synchronously():
    """The Cancel→Discard flow must defer discard_run to the finished_signal slot."""
    src = _MY_LIB.read_text(encoding="utf-8")
    tree = ast.parse(src)

    handler = _func_source(tree, "_on_cancel_clicked")
    calls = _calls_in(handler)

    assert "discard_run" not in calls, (
        "_on_cancel_clicked still calls discard_run directly — FIX-2a regressed. "
        "discard_run must only be called from the deferred finished_signal slot "
        "(_on_cancel_finished_drain)."
    )
    assert "keep_run" not in calls, (
        "_on_cancel_clicked still calls keep_run directly — FIX-2a regressed."
    )


def test_on_cancel_clicked_does_not_block_with_wait():
    """The UI thread must not call worker.wait(...) from _on_cancel_clicked."""
    src = _MY_LIB.read_text(encoding="utf-8")
    tree = ast.parse(src)

    handler = _func_source(tree, "_on_cancel_clicked")
    calls = _calls_in(handler)

    # wait() is the specific UI-thread blocker we removed; isRunning() / cancel()
    # are still allowed.
    assert "wait" not in calls, (
        "_on_cancel_clicked still calls worker.wait() — FIX-2a regressed. "
        "The cancel flow must be non-blocking; deferred via finished_signal."
    )


def test_finished_drain_slot_exists_and_dispatches():
    """The deferred slot must exist and call discard_run or keep_run."""
    src = _MY_LIB.read_text(encoding="utf-8")
    tree = ast.parse(src)

    drain = _func_source(tree, "_on_cancel_finished_drain")
    calls = _calls_in(drain)
    assert "discard_run" in calls and "keep_run" in calls, (
        f"_on_cancel_finished_drain must call BOTH discard_run AND keep_run "
        f"(branched on _pending_cancel_action). Got calls: {sorted(calls)}"
    )
