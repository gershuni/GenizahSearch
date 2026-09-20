"""Compatibility alias (repo-structure Round 1, Stage 2, 2026-09-20).

Real module: desktop/list_filter_dialog.py. Import ``desktop.list_filter_dialog``; this file only rebinds the old name to that
module so ``import list_filter_dialog`` and ``from list_filter_dialog import X`` keep working in the source tree. Remove in
Round 2 once nothing imports the old name (tests/test_root_alias_stubs.py pins the identity).
"""
import sys

from desktop import list_filter_dialog as _real

sys.modules[__name__] = _real
