"""Compatibility alias (repo-structure Round 1, Stage 2, 2026-09-20).

Real module: desktop/corrections_ui.py. Import ``desktop.corrections_ui``; this file only rebinds the old name to that
module so ``import corrections_ui`` and ``from corrections_ui import X`` keep working in the source tree. Remove in
Round 2 once nothing imports the old name (tests/test_root_alias_stubs.py pins the identity).
"""
import sys

from desktop import corrections_ui as _real

sys.modules[__name__] = _real
