# -*- coding: utf-8 -*-
"""Phase 146 Task 1: PASSAGE_INDEX_DIR must inherit LAB_INDEX_DIR's portable /
AppData / legacy resolution -- it is derived from the same INDEX_DIR, not from
its own environment variable, registry lookup, or eager directory creation.
"""
from __future__ import annotations

import inspect
import os

from shared.config import Config


def test_passage_index_dir_is_derived_from_index_dir():
    assert Config.PASSAGE_INDEX_DIR == os.path.join(
        Config.INDEX_DIR, "passage_index")


def test_passage_index_dir_sits_beside_lab_index_dir():
    """Same parent as LAB_INDEX_DIR -- proof it inherited the same resolution
    (portable-mode / AppData / legacy) rather than a fresh one."""
    assert (os.path.dirname(Config.PASSAGE_INDEX_DIR)
            == os.path.dirname(Config.LAB_INDEX_DIR))


def test_no_env_var_override_and_no_eager_mkdir():
    """Source-text check, not a filesystem probe: INDEX_DIR itself IS created
    eagerly by config.py, so a live-machine existence check of PASSAGE_INDEX_DIR
    can't distinguish "eagerly created" from "a real prior build made it" --
    only the source can. Neither os.getenv nor os.makedirs may ever mention the
    new constant.
    """
    src = inspect.getsource(Config)
    line = next(ln for ln in src.splitlines() if "PASSAGE_INDEX_DIR = " in ln)
    assert "os.getenv" not in line and "os.environ" not in line, (
        f"PASSAGE_INDEX_DIR must have no env-var override, got: {line!r}")
    for ln in src.splitlines():
        assert not ("makedirs" in ln and "PASSAGE_INDEX_DIR" in ln), (
            f"PASSAGE_INDEX_DIR must not be eagerly created: {ln!r}")
