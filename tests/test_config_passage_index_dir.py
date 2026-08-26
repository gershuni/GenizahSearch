# -*- coding: utf-8 -*-
"""Phase 146 Task 1: PASSAGE_INDEX_DIR must inherit LAB_INDEX_DIR's portable /
AppData / legacy resolution -- it is derived from the same INDEX_DIR, not from
its own environment variable, registry lookup, or eager directory creation.
"""
from __future__ import annotations

import importlib
import os

import shared.config as config_mod
from shared.config import Config


def test_passage_index_dir_is_derived_from_index_dir():
    assert Config.PASSAGE_INDEX_DIR == os.path.join(
        Config.INDEX_DIR, "passage_index")


def test_passage_index_dir_sits_beside_lab_index_dir():
    """Same parent as LAB_INDEX_DIR -- proof it inherited the same resolution
    (portable-mode / AppData / legacy) rather than a fresh one."""
    assert (os.path.dirname(Config.PASSAGE_INDEX_DIR)
            == os.path.dirname(Config.LAB_INDEX_DIR))


def test_no_env_var_override_and_no_eager_mkdir(tmp_path, monkeypatch):
    """Behavioural, not textual: a helper or alias that eagerly creates
    PASSAGE_INDEX_DIR would sail past a source-text grep for `makedirs`
    on the same line, so this points Config's whole resolution root (no
    portable index next to BASE_DIR, no legacy `~/Genizah_Tantivy_Index`,
    a fresh empty LOCALAPPDATA) at a temp tree, reloads the module fresh,
    and checks the directory is simply absent from disk afterward.
    """
    fake_home = tmp_path / "home"
    fake_appdata = tmp_path / "appdata"
    fake_home.mkdir()
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.setenv("USERPROFILE", str(fake_home))
    monkeypatch.setenv("LOCALAPPDATA", str(fake_appdata))

    # `importlib.reload` mints a brand-new Config class object -- modules
    # that already did `from shared.config import Config` (genizah_core.py's
    # re-export shim included) keep pointing at THIS original object no
    # matter what `shared.config.Config` gets reassigned to next, so that
    # object identity is what has to come back, not merely equal values.
    original_config = config_mod.Config

    try:
        importlib.reload(config_mod)
        reloaded = config_mod.Config

        # Proves the reload actually resolved against the fake root instead
        # of silently reusing a cached real Config: INDEX_DIR itself IS
        # created eagerly (by design), so its presence under tmp_path is
        # the isolation check, not the thing under test.
        assert reloaded.INDEX_DIR.startswith(str(tmp_path))
        assert os.path.isdir(reloaded.INDEX_DIR)

        assert not os.path.exists(reloaded.PASSAGE_INDEX_DIR), (
            "PASSAGE_INDEX_DIR must not be created on disk merely by "
            "importing/resolving Config")
    finally:
        # A second reload (even against the real env) would still mint yet
        # another new class object -- restore the exact pre-test object
        # directly so `shared.config.Config is genizah_core.Config` (and
        # every other module's already-bound reference) holds again.
        config_mod.Config = original_config
        monkeypatch.undo()
