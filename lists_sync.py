"""Compatibility alias (repo-structure Round 1, Stage 2, 2026-09-20).

Real module: shared/lists_sync.py. Import ``shared.lists_sync``; this file only rebinds the old name to that
module so ``import lists_sync`` and ``from lists_sync import X`` keep working in the source tree. Remove in
Round 2 once nothing imports the old name (tests/test_root_alias_stubs.py pins the identity).
"""
import sys

from shared import lists_sync as _real

sys.modules[__name__] = _real
