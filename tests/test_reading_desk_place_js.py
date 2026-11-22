# -*- coding: utf-8 -*-
"""The Reading Desk keeps the reader's place when it is redrawn (2026-11-22).

Switching the desk's image view jumped the page to its top: the redraw destroyed the toggle a
mouse click had left the focus in, and the panes' sync scrolled the page with scrollIntoView.
``tests/js/reading_desk_place_check.js`` runs the real ``web/static/reading_desk_place.js`` under
node with a small fake DOM; the rest checks that the page loads it and that the controls which
redraw the desk carry the class it acts on. GitHub-hosted runners ship node, so on CI a missing
node is a broken gate and fails; a machine without node skips.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
CHECK = REPO / 'tests' / 'js' / 'reading_desk_place_check.js'


def _node() -> str:
    node = shutil.which('node')
    if node:
        return node
    if os.environ.get('CI'):
        pytest.fail('node is not on PATH: GitHub-hosted runners ship it, so the desk check cannot run')
    pytest.skip('node is not installed here; CI runs this check')


def test_the_desk_place_checks_pass_under_node():
    run = subprocess.run([_node(), str(CHECK)], capture_output=True, text=True, cwd=str(REPO), timeout=120)
    sys.stdout.write(run.stdout)
    sys.stderr.write(run.stderr)
    assert run.returncode == 0, run.stdout + run.stderr
    assert 'all ' in run.stdout and ' checks passed' in run.stdout


def test_the_browse_page_loads_the_script_and_syncs_through_it():
    from web.pages import browse
    assert re.search(r'<script src="/static/reading_desk_place\.js\?v=[0-9a-f]{10}"></script>', browse.VIEWER_STYLES)
    source = (REPO / 'web' / 'pages' / 'browse.py').read_text(encoding='utf-8')
    assert 'window.rdSyncPanes(' in source
    # The desk's own scrolling is the static script's: nothing on the page scrolls it into view.
    assert 'scrollIntoView' not in source
