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
