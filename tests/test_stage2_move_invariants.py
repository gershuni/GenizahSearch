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
