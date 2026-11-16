"""Execute the shipped dictionary click extractor against reading/formatting boundaries."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest


def test_dictionary_reading_boundaries_under_node():
    node = shutil.which('node')
    if not node:
        if os.environ.get('CI'):
            pytest.fail('node is required for dictionary click checks on CI')
        pytest.skip('node is not installed')
    repo = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, str(repo / 'tests/js/blau_lookup_check.js')], cwd=repo,
        capture_output=True, text=True, encoding='utf-8', timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'dictionary reading checks passed' in result.stdout


def test_dictionary_click_script_in_a_dialog_under_node():
    """Quick View: the dialog the panel is in does not stand the script down (a menu or a second
    dialog does), the phone chip lives beside the panel, and an Esc that closes the panel goes no
    further (tests/js/blau_lookup_dialog_check.js)."""
    node = shutil.which('node')
    if not node:
        if os.environ.get('CI'):
            pytest.fail('node is required for dictionary click checks on CI')
        pytest.skip('node is not installed')
    repo = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, str(repo / 'tests/js/blau_lookup_dialog_check.js')], cwd=repo,
        capture_output=True, text=True, encoding='utf-8', timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'dictionary dialog checks passed: 6' in result.stdout
