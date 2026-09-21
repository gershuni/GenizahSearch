# -*- coding: utf-8 -*-
"""Every app module the crash classifier knows by basename has one canonical path.

``desktop/telemetry.py`` classifies a crash frame as in-app when its basename is in
``_APP_SOURCE_FILES`` (root-level modules that carry no ``desktop/``/``shared/`` path segment to
match on). That set is a list of names, not of locations, so nothing so far asserted that the
file behind each name exists, is tracked, or is the real module rather than a compatibility
stub. While the Round 1 moves were in flight the old path kept a four-line alias stub -- and a
test that resolved the name through the import system would happily resolve the stub and pass.
The stubs are gone now, but the check stays: it is what would catch one coming back.

So this file keeps an explicit table instead: basename -> the canonical path, maintained by hand
and checked three ways: the path exists, ``git ls-files`` knows it, and its source is not an alias
stub. It also asserts the telemetry set is a subset of the table, so a new basename there needs a
row here in the same commit. A move updates the row.

``tests/test_no_root_alias_stubs.py`` is the successor to the stub-identity gate: its table must
list every row here whose path left the root, and it proves the old root path is gone and that no
tracked file still imports the old bare name. Crash-frame classification itself stays covered by
``tests/test_crash_payload.py``. ``genizah_app.py::_SELF_TEST_IMPORT_MODULES`` -- what
``GenizahSearchPro.exe --self-test-imports`` imports inside the frozen process -- must equal this
table minus the entry point; the last test below keeps it so.
"""
from __future__ import annotations

import ast
import pathlib
import subprocess

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# basename -> canonical repo-relative path (forward slashes). Update the value when a module
# moves; never delete a row while the basename is still in _APP_SOURCE_FILES.
CANONICAL: dict[str, str] = {
    "genizah_app.py": "genizah_app.py",
    "genizah_core.py": "genizah_core.py",
    "gui_threads.py": "desktop/gui_threads.py",
    "corrections_client.py": "desktop/corrections_client.py",
    "corrections_ui.py": "desktop/corrections_ui.py",
    "supabase_corrections_client.py": "desktop/supabase_corrections_client.py",
    "lists_sync.py": "shared/lists_sync.py",
    "filter_text_dialog.py": "desktop/filter_text_dialog.py",
    "column_filter_dialog.py": "desktop/column_filter_dialog.py",  # moved 2026-09-19 (Stage 2 trial)
    "list_filter_dialog.py": "desktop/list_filter_dialog.py",
    "genizah_translations.py": "shared/genizah_translations.py",
    "pgp_tag_translations.py": "shared/pgp_tag_translations.py",
    "sefaria_utils.py": "shared/sefaria_utils.py",
    "shared_export_utils.py": "shared_export_utils.py",
    "unified_variants.py": "shared/unified_variants.py",
}

# What an alias stub looks like (see the Round 1 plan, "Root alias stub"): it rebinds its own
# entry in sys.modules to the real module. A canonical path must never be one of these.
_STUB_MARKERS = ("sys.modules[__name__]",)


def _tracked_files() -> set[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO_ROOT, capture_output=True, check=True,
    ).stdout.decode("utf-8", "surrogateescape")
    return {p for p in out.split("\0") if p}


def _telemetry_app_source_files() -> frozenset[str]:
    """Read ``_APP_SOURCE_FILES`` from desktop/telemetry.py without importing it.

    Importing the module wires consent state and PostHog transport at import time; the set is a
    literal, so the AST is the honest way to read it.
    """
    src = (REPO_ROOT / "desktop" / "telemetry.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == "_APP_SOURCE_FILES" for t in targets):
                value = node.value
                # frozenset({...}) -> the inner set literal
                if isinstance(value, ast.Call) and value.args:
                    value = value.args[0]
                return frozenset(ast.literal_eval(value))
    raise AssertionError("desktop/telemetry.py no longer defines _APP_SOURCE_FILES as a literal")


@pytest.fixture(scope="module")
def tracked() -> set[str]:
    return _tracked_files()


@pytest.mark.parametrize("basename,rel_path", sorted(CANONICAL.items()))
def test_canonical_path_is_real_and_tracked(basename, rel_path, tracked):
    path = REPO_ROOT / rel_path
    assert path.is_file(), f"{basename}: canonical path {rel_path} does not exist"
    assert rel_path in tracked, f"{basename}: canonical path {rel_path} is not tracked by git"
    assert pathlib.PurePosixPath(rel_path).name == basename, (
        f"{basename}: canonical path {rel_path} has a different basename"
    )
    src = path.read_text(encoding="utf-8", errors="replace")
    for marker in _STUB_MARKERS:
        assert marker not in src, (
            f"{basename}: {rel_path} looks like an alias stub ({marker!r} in source); "
            "point CANONICAL at the real module instead"
        )


def test_every_telemetry_basename_has_a_canonical_row():
    missing = sorted(_telemetry_app_source_files() - set(CANONICAL))
    assert not missing, (
        f"desktop/telemetry.py::_APP_SOURCE_FILES names {missing} but CANONICAL has no row for "
        "them; add the basename -> path row here (this is what keeps a moved module honest)."
    )


def test_canonical_rows_are_not_stale():
    """A CANONICAL row for a basename telemetry no longer classifies is a leftover."""
    extra = sorted(set(CANONICAL) - _telemetry_app_source_files())
    assert not extra, (
        f"CANONICAL has rows {extra} that _APP_SOURCE_FILES does not classify; remove them or "
        "add the basename to the telemetry set on purpose."
    )


def _self_test_import_modules() -> tuple:
    """``_SELF_TEST_IMPORT_MODULES`` from genizah_app.py, read from the AST (importing the app is Qt)."""
    src = (REPO_ROOT / "genizah_app.py").read_text(encoding="utf-8", errors="replace")
    for node in ast.parse(src).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_SELF_TEST_IMPORT_MODULES" for t in node.targets
        ):
            return tuple(ast.literal_eval(node.value))
    raise AssertionError(
        "genizah_app.py no longer defines _SELF_TEST_IMPORT_MODULES as a module-level tuple literal"
    )


def dotted(rel_path: str) -> str:
    """``desktop/x.py`` -> ``desktop.x``; a root file is its bare name."""
    return rel_path[:-3].replace("/", ".")


def test_frozen_self_test_imports_exactly_the_canonical_modules():
    """``GenizahSearchPro.exe --self-test-imports`` imports every canonical module by its dotted name,
    minus the entry point (``__main__`` in the frozen process). A move that updates CANONICAL without
    the tuple -- or the tuple without CANONICAL -- fails here, before any build."""
    expected = {dotted(p) for b, p in CANONICAL.items() if b != "genizah_app.py"}
    actual = _self_test_import_modules()
    assert len(actual) == len(set(actual)), f"duplicates in _SELF_TEST_IMPORT_MODULES: {actual}"
    assert set(actual) == expected, (
        "genizah_app.py::_SELF_TEST_IMPORT_MODULES disagrees with CANONICAL: "
        f"missing {sorted(expected - set(actual))}, extra {sorted(set(actual) - expected)}"
    )
