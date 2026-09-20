"""Compatibility alias (repo-structure Round 1, Stage 2, 2026-09-20).

Real module: shared/sefaria_utils.py. Import ``shared.sefaria_utils``; this file only rebinds the old name to that
module so ``import sefaria_utils`` and ``from sefaria_utils import X`` keep working in the source tree. Remove in
Round 2 once nothing imports the old name (tests/test_root_alias_stubs.py pins the identity).
"""
import sys

from shared import sefaria_utils as _real

sys.modules[__name__] = _real
