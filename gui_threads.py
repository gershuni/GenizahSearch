"""Compatibility alias (repo-structure Round 1, Stage 2, 2026-09-20).

Real module: desktop/gui_threads.py. Import ``desktop.gui_threads``; this file only rebinds the old name to that
module so ``import gui_threads`` and ``from gui_threads import X`` keep working in the source tree. Remove in
Round 2 once nothing imports the old name (tests/test_root_alias_stubs.py pins the identity).
"""
import sys

from desktop import gui_threads as _real

sys.modules[__name__] = _real
