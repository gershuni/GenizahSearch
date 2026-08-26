# -*- coding: utf-8 -*-
"""`scripts/rebuild_browse_map.py` must survive the damage it repairs.

The tool exists because a browse map can end up truncated or otherwise
unreadable. Reading the live map as a PREREQUISITE for rebuilding it made
the one file the tool is for the one file it could not handle.
"""
from __future__ import annotations

import importlib.util
import os
import pickle
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)


def _module():
    path = os.path.join(REPO_ROOT, 'scripts', 'rebuild_browse_map.py')
    spec = importlib.util.spec_from_file_location('rebuild_browse_map', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope='module')
def rbm():
    return _module()


def test_a_truncated_map_is_reported_not_raised(rbm, tmp_path):
    damaged = tmp_path / 'browse_map.pkl'
    damaged.write_bytes(pickle.dumps({'a': [1]})[:7])
    line = rbm.describe_live_map(str(damaged))
    assert 'UNREADABLE' in line, line
    assert 'Rebuilding' in line, (
        'the report must say the rebuild continues; an operator reading '
        '"unreadable" alone will assume the tool refused')


def test_an_empty_map_is_reported_not_raised(rbm, tmp_path):
    damaged = tmp_path / 'browse_map.pkl'
    damaged.write_bytes(b'')
    assert 'UNREADABLE' in rbm.describe_live_map(str(damaged))


def test_a_missing_map_is_reported(rbm, tmp_path):
    assert 'ABSENT' in rbm.describe_live_map(str(tmp_path / 'nope.pkl'))


def test_a_readable_map_is_described(rbm, tmp_path):
    good = tmp_path / 'browse_map.pkl'
    good.write_bytes(pickle.dumps({'a': [1], 'b': [2, 3]}))
    line = rbm.describe_live_map(str(good))
    assert '2 manuscripts' in line, line


def test_reading_the_live_map_is_not_a_prerequisite(rbm):
    """Source-anchored: the read must stay inside the reporting helper. A
    bare `pickle.load` back in `main` would restore the defect while every
    behavioural test above still passed."""
    import inspect
    src = inspect.getsource(rbm.main)
    assert 'pickle.load' not in src, (
        'main() reads the live map directly again -- a corrupt one then '
        'stops the repair before it starts')
    assert 'describe_live_map(' in src
