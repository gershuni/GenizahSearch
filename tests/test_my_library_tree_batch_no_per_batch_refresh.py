"""Regression guard: the per-folder opt-out checkbox refresh must NOT run
once per FolderWalkWorker batch.

`_UnifiedFileTreeWidget._on_tree_batch` fires once per 100 files. It used to
call `_sync_master_optout_checkbox()` at the end of every batch, which runs
`_folder_optout_aggregate` → `get_folder_filepaths` (a full `local_files`
table scan) for EVERY registered folder. For a 16.8K-file library that is
O(batches × folders × files) ≈ 15s per tree population on the UI thread, and
the tree is repopulated ~3× by the LAB-stale reload churn → the ~45s startup
freeze the user hit (v7.16). The refresh now runs ONCE, in `_on_tree_finished`.

Measured: 14.96s → 0.10s per population. This AST guard pins it so the
per-batch call cannot be reintroduced.
"""
import ast
import os

_SRC = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "desktop", "my_library_tab.py",
)


def _method(class_name: str, method_name: str) -> ast.FunctionDef:
    with open(_SRC, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for sub in node.body:
                if isinstance(sub, ast.FunctionDef) and sub.name == method_name:
                    return sub
    raise AssertionError(f"{class_name}.{method_name} not found in {_SRC}")


def _calls_sync(fn: ast.FunctionDef) -> bool:
    """True if the function body references `_sync_master_optout_checkbox`."""
    for n in ast.walk(fn):
        if isinstance(n, ast.Attribute) and n.attr == "_sync_master_optout_checkbox":
            return True
    return False


def test_on_tree_batch_does_not_refresh_folder_checkboxes_per_batch():
    fn = _method("_UnifiedFileTreeWidget", "_on_tree_batch")
    assert not _calls_sync(fn), (
        "_on_tree_batch must NOT call _sync_master_optout_checkbox — that ran a "
        "full folder scan per 100-file batch and caused the ~45s startup freeze "
        "on large libraries. Move it to _on_tree_finished (once per population)."
    )


def test_on_tree_finished_refreshes_folder_checkboxes_once():
    fn = _method("_UnifiedFileTreeWidget", "_on_tree_finished")
    assert _calls_sync(fn), (
        "_on_tree_finished must call _sync_master_optout_checkbox once so the "
        "per-folder opt-out checkboxes are synced after tree population."
    )
