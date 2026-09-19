"""Compatibility alias (repo-structure Round 1, Stage 2 trial, 2026-09-19).

Real module: desktop/column_filter_dialog.py. Import ``desktop.column_filter_dialog``; this file only
rebinds the old name to that module so ``import column_filter_dialog`` and
``from column_filter_dialog import ColumnFilterDialog`` keep working in the source tree. Remove in
Round 2 once nothing imports the old name (tests/test_root_alias_stubs.py pins the identity).
"""
import sys

from desktop import column_filter_dialog as _real

sys.modules[__name__] = _real
