# -*- coding: utf-8 -*-
"""The viewer's zoom arithmetic, run under node (2026-11-03).

``tests/js/manuscript_viewer_zoom_check.js`` loads the real
``web/static/manuscript_viewer.js`` into a small fake DOM and checks the
multiplicative steps, the clamps, the zoom-at-the-pointer arithmetic, the
wheel, the double-click and double-tap, the pinch, the debounced ``onChange``
and the take-over of a rebuilt image. GitHub-hosted runners ship node, so on
CI a missing node is a broken gate and fails; a machine without node skips.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
CHECK = REPO / 'tests' / 'js' / 'manuscript_viewer_zoom_check.js'


def _node() -> str:
    node = shutil.which('node')
    if node:
        return node
    if os.environ.get('CI'):
        pytest.fail('node is not on PATH: GitHub-hosted runners ship it, so the viewer check cannot run')
    pytest.skip('node is not installed here; CI runs this check')


def test_the_viewer_zoom_checks_pass_under_node():
    run = subprocess.run([_node(), str(CHECK)], capture_output=True, text=True, cwd=str(REPO), timeout=120)
    sys.stdout.write(run.stdout)
    sys.stderr.write(run.stderr)
    assert run.returncode == 0, run.stdout + run.stderr
    assert 'all ' in run.stdout and ' checks passed' in run.stdout


def test_the_check_reads_the_shipped_viewer_file():
    src = CHECK.read_text(encoding='utf-8')
    assert "'web', 'static', 'manuscript_viewer.js'" in src
