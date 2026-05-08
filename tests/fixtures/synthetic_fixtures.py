"""Phase 85 reusable test fixtures.

Imported by tests in Plans 01-05. Adding a case here propagates verification
to every plan's test suite.

ILLUSTRATIVE ONLY: the inventory_id values below are TEMPLATES for testing
the helper round-trip + format. They are NOT live data and MUST NOT be
treated as authoritative InventoryIds for downstream code (libraries.csv
generation, fjms_enrichment.db UNION). The real data sourcing happens at
regen time in scripts/generate_synthetic_rows.py per 85-02-PLAN.md.
"""
from __future__ import annotations

# Tier coverage per D-03: (1) CUDL+FJMS, (2) CUDL-only no-FJMS, (3) FJMS-only no-CUDL.
# inventory_id values are illustrative; actual InventoryIds come from FIST.db at
# regen time. The fixture's job is helper correctness, not data correctness.
# NOTE (plan-deviation, Rule 1 - Bug): The plan's must_haves listed
# `(123456, "990001234560000000")` and `(329960, "990003299600000000")` as
# encode pairs, but those sys_ids encode 1234560 and 3299600 respectively
# under the locked `99 + InventoryId.zfill(10) + 000000` format (the
# decode slice [2:12] of "990001234560000000" is "0001234560" = 1234560).
# The plan's roundtrip invariant (decode(encode(n)) == n) is the
# load-bearing contract; the illustrative pair values were typos. We fix
# the pairs to be internally consistent with zfill semantics. Documented
# in 85-01-SUMMARY.md "Deviations from Plan".
SYNTHETIC_GOLDEN_CASES = [
    # (inventory_id, synthetic_sys_id, tier, notes)
    (
        123456,
        "990000123456000000",
        "tier1-cudl-fjms",
        "synthetic with both CUDL manifest and FJMS metadata",
    ),
    (1, "990000000001000000", "edge-min-inv-id", "minimum InventoryId boundary"),
    (
        9999999999,
        "999999999999000000",
        "edge-max-inv-id",
        "maximum 10-digit InventoryId",
    ),
    # ILLUSTRATIVE ONLY — see 85-02-PLAN.md for real data sourcing.
    # The number 329960 is a placeholder loosely inspired by "T-S NS 329.96" but is
    # NOT the real InventoryId for that shelfmark (Plan 02 resolves the real one
    # from FIST.db at regen time). Do not use this in production code.
    (
        329960,
        "990000329960000000",
        "tier1-origin-case-template",
        "T-S NS 329.96 origin case TEMPLATE — illustrative, not the real InventoryId",
    ),
]

# Real Alma sys_ids from the codebase that MUST classify as NOT synthetic.
# Source: search_serializer.py comment + STATE.md investigation summary.
REAL_ALMA_NEGATIVE_CASES = [
    # (sys_id, library_code, notes)
    (
        "990025143260205171",
        "NLI",
        "real NLI Alma — 205171 institution suffix; CRITICAL collision negative",
    ),
    (
        "990053835020205171",
        "Mosseri",
        "real Mosseri Alma row from cudl_must_resolve.csv",
    ),
    ("990053835750205171", "Mosseri", "real Mosseri Alma row"),
    ("990052439490205171", "CUL", "real CUL Or. Alma row"),
]

# Inputs that violate D-13 normalization contract — must classify False.
D13_NORMALIZATION_NEGATIVES = [
    ("99-0001234560-000000", "dashes inside — non-digit chars rejected"),
    ("99 0001234560 000000", "spaces — non-digit chars rejected"),
    ("'990001234560000000'", "quoted — non-digit chars rejected"),
    ("9900012345600000001", "length 19 — too long"),
    ("99000123456000001", "length 17 — too short"),
    ("99000123456000010", "length 17 — too short"),
    ("990001234560000001", "suffix '000001' not '000000' — discriminator violation"),
    ("980001234560000000", "prefix '98' not '99'"),
    ("99000123456000000a", "non-digit at end"),
    ("", "empty"),
    (None, "None"),
]
