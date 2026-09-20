# -*- coding: utf-8 -*-
"""Two things the 2026-09-20 desktop moves could break silently, pinned.

``--self-test-imports`` proves each moved module is importable inside the frozen app, and
``tests/test_root_alias_stubs.py`` proves no consumer still names the old module. Neither sees
these two, because both are about what a SUCCESSFUL import leaves behind:

1. ``desktop/join_workbench.py`` imports Qt and ``desktop.gui_threads`` inside one module-level
   ``try``; its handler sets ``_QT_AVAILABLE = False``, and every Qt class in the file is defined
   under ``if _QT_AVAILABLE:``. A wrong module name there raises nothing -- the Join Workbench UI
   simply stops existing, and importing the module still succeeds.
2. ``desktop/gui_threads.py`` reads the few-shot translation prompts out of the repository's
   ``data/`` directory. The module moved one level down on 2026-09-20, so the path had to climb
   one level further; get it wrong and field translation quietly falls back to no prompt
   (``shared/dicta_client.py`` swallows the read error).
3. ``scripts/server.py`` anchors the repository root on its own location. It ``chdir``s there and
   then launches ``python -m web.main`` with output to DEVNULL, so a wrong anchor makes the child
   die with ``ModuleNotFoundError: web`` and the only symptom is "not running" two seconds later.
   It also decides where ``.server.pid`` lives.
4. ``GenizahSearchPro.spec`` bundles a few root ``.py`` files by name into ``_internal/``, which is
   on ``sys.path`` in a onedir build. If one of those names becomes an alias stub, the stub ships
   and the "no stub reaches the frozen app" invariant behind ``--self-test-imports`` is false.
"""
from __future__ import annotations

import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def test_join_workbench_qt_half_is_defined():
    """_QT_AVAILABLE False means the guarded import failed -- including a wrong module name."""
    pytest.importorskip("PyQt6.QtWidgets")
    from desktop import join_workbench

    assert join_workbench._QT_AVAILABLE is True, (
        "desktop/join_workbench.py caught an ImportError in its module-level try block, so every "
        "Qt class in the file is undefined and the Join Workbench UI is gone. With PyQt6 present "
        "the usual cause is a stale module name in that block (it imports desktop.gui_threads)."
    )
    assert hasattr(join_workbench, "JoinWorkbenchDialog") or any(
        n.startswith("JoinWorkbench") for n in vars(join_workbench)
    ), "the Qt half of join_workbench defined no JoinWorkbench* class"


def test_few_shot_data_dir_is_the_repository_data_directory():
    pytest.importorskip("PyQt6.QtWidgets")
    from desktop.gui_threads import few_shot_data_dir

    resolved = pathlib.Path(few_shot_data_dir()).resolve()
    assert resolved == (REPO_ROOT / "data").resolve(), (
        f"few-shot prompts would be read from {resolved}, not {REPO_ROOT / 'data'}; the module "
        "sits one level below the repository root since 2026-09-20."
    )


@pytest.mark.parametrize("name", ["few_shot_en2he_scholarly.json", "few_shot_he2en_scholarly.json"])
def test_the_few_shot_templates_are_where_that_path_points(name):
    pytest.importorskip("PyQt6.QtWidgets")
    from desktop.gui_threads import few_shot_data_dir

    assert (pathlib.Path(few_shot_data_dir()) / name).is_file(), (
        f"{name} is not at the path desktop/gui_threads.py reads (data/ is tracked; the frozen "
        "build not bundling it is a separate, recorded defect)."
    )


def test_dev_server_cli_anchors_the_repository_root():
    """scripts/server.py::PROJECT_DIR must be the repo root, not scripts/."""
    import importlib.util

    path = REPO_ROOT / "scripts" / "server.py"
    spec = importlib.util.spec_from_file_location("_dev_server_cli", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert pathlib.Path(module.PROJECT_DIR).resolve() == REPO_ROOT, (
        f"PROJECT_DIR is {module.PROJECT_DIR}, not the repository root. start() chdir()s there "
        "and then runs `python -m web.main`, which can only resolve the `web` package from the "
        "root -- and the child's output goes to DEVNULL, so the failure is invisible."
    )
    assert pathlib.Path(module.PID_FILE).resolve().parent == REPO_ROOT
    assert (REPO_ROOT / "web" / "main.py").is_file(), "the package that anchor exists to reach"


def _spec_datas_sources() -> list:
    """The first element of every tuple in GenizahSearchPro.spec's literal `datas` list."""
    import ast

    source = (REPO_ROOT / "GenizahSearchPro.spec").read_text(encoding="utf-8")
    for node in ast.parse(source).body:
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "datas" for t in node.targets)
            and isinstance(node.value, ast.List)
        ):
            return [
                ast.literal_eval(elt)[0]
                for elt in node.value.elts
                if isinstance(elt, ast.Tuple)
            ]
    raise AssertionError("GenizahSearchPro.spec no longer assigns a literal `datas` list")


def test_the_installer_never_bundles_an_alias_stub():
    """A stub listed in the spec ships into _internal/, which is on sys.path in a onedir build.

    That would put the old top-level name back into the frozen app -- the exact thing
    ``--self-test-imports`` exists to rule out -- and the stub's own import of the real module
    would still work, so nothing would fail loudly.
    """
    shipped_stubs = []
    for source in _spec_datas_sources():
        rel = source.replace("\\", "/")
        path = REPO_ROOT / rel
        if not path.is_file() or path.suffix != ".py":
            continue
        if "sys.modules[__name__]" in path.read_text(encoding="utf-8", errors="replace"):
            shipped_stubs.append(rel)
    assert not shipped_stubs, (
        f"GenizahSearchPro.spec bundles alias stub(s) {shipped_stubs} into the bundle root. "
        "Drop the tuple: the real module already ships inside its package."
    )
