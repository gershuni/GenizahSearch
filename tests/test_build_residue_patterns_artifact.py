# -*- coding: utf-8 -*-
"""Phase 86 Plan 03: deterministic tests for the bridge-aware residue
patterns ranker in `scripts/build_residue_patterns_artifact.py`.

Builds small in-memory FIST.db schemas mirroring the 3-table production
join (Inventory + InventorySignature + Signature + UnitCatalogRec) and
exercises `nearest_fist_candidates` end to end.

Coverage:
  - Pass 2 MEDIUM-1: bridge-aware ranker prioritises exact bridge-key
    matches over shared-prefix matches over unrelated candidates.
  - Pass 3 MED-86-03: prefetched buckets file noisy-prefix records via
    post-rsplit(':', 1)[1] tail; cache is built once and reused.

The ranker's module-level prefetch cache `_FIST_CANDIDATE_BUCKETS` is
reset at the start of each test that exercises a fresh in-memory DB so
state from prior tests does not leak.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

# Make sibling `scripts/` package importable when this test runs from the
# repo root (mirrors the bootstrap in build_residue_patterns_artifact.py).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _seed_fist_schema(conn: sqlite3.Connection) -> None:
    """Create the minimum FIST schema needed by the prefetch query.

    Mirrors the 3-table production join shape (Pass 2 HIGH-2):
    Inventory -> InventorySignature -> Signature -> UnitCatalogRec.
    """
    conn.executescript(
        """
        CREATE TABLE dbo_Inventory (InventoryId INTEGER PRIMARY KEY, Shelfmark TEXT);
        CREATE TABLE dbo_InventorySignature (InventoryId INTEGER, SetSignatureId INTEGER);
        CREATE TABLE dbo_Signature (SetSignatureId INTEGER, SignatureId INTEGER);
        CREATE TABLE dbo_UnitCatalogRec (UnitCatalogRecId INTEGER PRIMARY KEY AUTOINCREMENT,
                                         SignatureId INTEGER, Title TEXT, GenizahTitleText TEXT);
        """
    )


class TestResiduePatternRanker:
    """Pass 2 MEDIUM-1: bridge-aware ranker correctness."""

    def test_ranker_prioritizes_exact_bridge_match(self):
        """Seed 3 candidates: 1 exact bridge-key match, 1 shared-prefix, 1 unrelated.
        Assert the ranker returns them in score order (exact > prefix > unrelated)."""
        import scripts.build_residue_patterns_artifact as art
        from scripts.build_residue_patterns_artifact import nearest_fist_candidates

        # Reset prefetch cache so this test sees a fresh build.
        art._FIST_CANDIDATE_BUCKETS = None

        db = sqlite3.connect(":memory:")
        _seed_fist_schema(db)
        db.executescript(
            """
            -- Candidate A: T-S NS 329.96 -> fist_to_cudl_keys produces 'tsns329.96'
            --              (exact bridge match for residue 'tsns329.96')
            INSERT INTO dbo_Inventory VALUES (100, 'T-S NS 329.96');
            -- Candidate B: T-S NS 329.97 -> 'tsns329.97' shares >=3-char prefix
            --              'tsns329' with residue key 'tsns329.96'
            INSERT INTO dbo_Inventory VALUES (200, 'T-S NS 329.97');
            -- Candidate C: T-S NS 1.1 -> 'tsns1.1' no shared prefix beyond 'tsns'
            INSERT INTO dbo_Inventory VALUES (300, 'T-S NS 1.1');
            """
        )
        db.commit()

        results = nearest_fist_candidates("tsns329.96", db, limit=3)
        # Must rank A first (exact bridge match score >= 100).
        assert results, f"expected non-empty results, got {results}"
        assert results[0]["inventory_id"] == 100, (
            f"exact-match expected first; got {results[0]['inventory_id']}"
        )
        assert results[0]["score"] >= 100
        # B (prefix) MUST outrank C (token-overlap only).
        inv_ids = [r["inventory_id"] for r in results]
        assert 200 in inv_ids, f"prefix-share candidate missing; got {inv_ids}"
        # Ordering: 100 before 200 before 300 (if present).
        idx100 = inv_ids.index(100)
        idx200 = inv_ids.index(200)
        assert idx100 < idx200, (
            f"exact-match should rank before prefix-share; got order {inv_ids}"
        )
        if 300 in inv_ids:
            idx300 = inv_ids.index(300)
            assert idx200 < idx300, (
                f"prefix-share should outrank unrelated; got order {inv_ids}"
            )

    def test_ranker_prefetch_handles_noisy_prefix(self):
        """Pass 3 MED-86-03 (Codex): a FIST.Shelfmark with a noisy leading
        prefix (e.g. 'AIU: CUL: Or.1080 1.5') still lands in the right
        family bucket because the prefetch helper also files candidates
        by their post-rsplit(':', 1)[1] tail."""
        import scripts.build_residue_patterns_artifact as art
        from scripts.build_residue_patterns_artifact import nearest_fist_candidates

        # Reset prefetch cache so this test sees a fresh build.
        art._FIST_CANDIDATE_BUCKETS = None

        db = sqlite3.connect(":memory:")
        _seed_fist_schema(db)
        db.executescript(
            """
            -- Noisy-prefix candidate: starts with 'AIU: CUL: ' instead of 'Or.1080'.
            -- Without the post-colon tail filing, this record would never enter
            -- the or1080 bucket and the ranker would miss it entirely.
            INSERT INTO dbo_Inventory VALUES (901, 'AIU: CUL: Or.1080 1.5');
            """
        )
        db.commit()

        results = nearest_fist_candidates("or1080.1.5", db, limit=3)
        inv_ids = {r["inventory_id"] for r in results}
        assert 901 in inv_ids, (
            f"Pass 3 MED-86-03: noisy-prefix record must reach the or1080 "
            f"bucket via the post-colon tail; got {inv_ids}"
        )

    def test_ranker_prefetch_runs_only_once(self):
        """Pass 3 MED-86-03: subsequent calls reuse the cache instead of
        issuing another SQL query."""
        import scripts.build_residue_patterns_artifact as art
        from scripts.build_residue_patterns_artifact import nearest_fist_candidates

        art._FIST_CANDIDATE_BUCKETS = None

        db = sqlite3.connect(":memory:")
        _seed_fist_schema(db)
        db.executescript(
            """
            INSERT INTO dbo_Inventory VALUES (1, 'T-S NS 329.96');
            """
        )
        db.commit()

        # First call populates the cache.
        nearest_fist_candidates("tsns329.96", db, limit=3)
        assert art._FIST_CANDIDATE_BUCKETS is not None
        cached = art._FIST_CANDIDATE_BUCKETS

        # Second call MUST NOT rebuild (object identity unchanged).
        nearest_fist_candidates("tsns329.96", db, limit=3)
        assert art._FIST_CANDIDATE_BUCKETS is cached, (
            "Pass 3 MED-86-03: prefetch cache should be reused across calls"
        )
