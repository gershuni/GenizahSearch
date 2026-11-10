# -*- coding: utf-8 -*-
"""Tests never write the developer's own session.json, config.pkl or lang.pkl.

Config resolves INDEX_DIR to the developer's real data folder when one exists, and
New saves session.json synchronously, so an unisolated test that reaches a real save
overwrites that developer's files. tests/conftest.py points the three files at a
fresh directory for every test; these check it does, and that two tests never share
one.
"""
import os

import pytest

from shared.config import Config

_FILES = ("SESSION_FILE", "CONFIG_FILE", "LANGUAGE_FILE")


def _isolated_paths():
    """The three paths, after checking none of them is the real one.

    Checked before anything is written, so this file can never touch the real
    files even when the isolation is missing.
    """
    index_dir = os.path.normcase(os.path.abspath(Config.INDEX_DIR))
    paths = {}
    for name in _FILES:
        path = getattr(Config, name)
        folder = os.path.normcase(os.path.dirname(os.path.abspath(path)))
        assert folder != index_dir, f"{name} is the real one: {path}"
        assert os.path.isdir(folder), f"{name}'s folder does not exist: {path}"
        paths[name] = path
    return paths


def test_personal_state_files_are_outside_the_data_folder():
    _isolated_paths()


@pytest.mark.parametrize("run", ["first", "second"])
def test_each_test_starts_with_no_personal_state(run):
    """Whichever of the two runs second would find the other's files."""
    for name, path in _isolated_paths().items():
        assert not os.path.exists(path), f"{name} left over from another test: {path}"
        with open(path, "wb") as fh:
            fh.write(run.encode())
