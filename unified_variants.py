"""Compatibility alias (repo-structure Round 1, Stage 2, 2026-09-20).

Real module: shared/unified_variants.py. Import ``shared.unified_variants``; this file only rebinds the old name to that
module so ``import unified_variants`` and ``from unified_variants import X`` keep working in the source tree. Remove in
Round 2 once nothing imports the old name (tests/test_root_alias_stubs.py pins the identity).
"""
import sys

from shared import unified_variants as _real

sys.modules[__name__] = _real
