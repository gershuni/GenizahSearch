# -*- coding: utf-8 -*-
"""Phase 95 LOCAL sys_id test fixtures.

Mirrors tests/fixtures/synthetic_fixtures.py shape. Per CONTEXT D-19:
LOCAL sys_id = 97 + machine_id(8 decimal digits) + content_hash(8 decimal digits) = 18 digits.
"""
from __future__ import annotations

# Valid LOCAL sys_ids (18 digits, 97-prefix). Real machine_id + content_hash
# values from any deterministic SHA256 % 10**8 derivations.
LOCAL_GOLDEN_CASES = [
    "970012345601234567",  # machine_id=00123456, content_hash=01234567
    "979999999999999999",  # max machine_id + max content_hash
    "970000000000000000",  # all zeros after prefix
    "971234567812345678",  # mixed
]

# Real Alma sys_ids — MUST NOT classify as LOCAL.
LOCAL_REAL_ALMA_NEGATIVE_CASES = [
    "990025143260205171",  # real Alma NLI 205171 suffix
    "991234560205171000",  # real Alma generic
    "990012345601234567",  # 99-prefix synthetic (Phase 85 SYNTH-06)
]

# 99-prefix synthetic sys_ids (Phase 85) — MUST NOT classify as LOCAL.
LOCAL_SYNTHETIC_99_NEGATIVE_CASES = [
    "990001234560000000",
    "990025143260000000",
]

# Negative cases: wrong length, wrong prefix, non-numeric.
LOCAL_NEGATIVE_CASES = [
    "",                          # empty
    None,                        # None
    "97001234560123456",         # 17 digits (too short)
    "9700123456012345678",       # 19 digits (too long)
    "98" + "0" * 16,             # 98-prefix (wrong prefix)
    "96" + "0" * 16,             # 96-prefix (wrong prefix)
    "97" + "a" * 16,             # non-numeric body
    "97 0012345601234567",       # contains space
]

# D-19 normalization negatives: integer overflow attempts (modulo missing).
# If sys_id derivation forgets `% 10**8`, the machine_id slot can be 9-10
# digits and total length blows past 18.
D_19_NORMALIZATION_NEGATIVES = [
    "97" + "4294967295" + "01234567",  # 18 -> 20 (full uint32 in machine slot)
]
