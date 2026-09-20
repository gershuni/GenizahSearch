"""Compatibility alias (repo-structure Round 1, Stage 2, 2026-09-20).

Real module: desktop/filter_text_dialog.py. Import ``desktop.filter_text_dialog``; this file only rebinds the old name to that
module so ``import filter_text_dialog`` and ``from filter_text_dialog import X`` keep working in the source tree. Remove in
Round 2 once nothing imports the old name (tests/test_root_alias_stubs.py pins the identity).
"""
import sys

from desktop import filter_text_dialog as _real

sys.modules[__name__] = _real
