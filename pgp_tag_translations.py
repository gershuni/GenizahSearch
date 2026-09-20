"""Compatibility alias (repo-structure Round 1, Stage 2, 2026-09-20).

Real module: shared/pgp_tag_translations.py. Import ``shared.pgp_tag_translations``; this file only rebinds the old name to that
module so ``import pgp_tag_translations`` and ``from pgp_tag_translations import X`` keep working in the source tree. Remove in
Round 2 once nothing imports the old name (tests/test_root_alias_stubs.py pins the identity).
"""
import sys

from shared import pgp_tag_translations as _real

sys.modules[__name__] = _real
