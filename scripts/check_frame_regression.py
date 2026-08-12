#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CERT-01 frame-regression gate (CD batch, schema Amendment 2026-08-12 (T);
Codex pre-flight finding 5).

Given a BEFORE and an AFTER discovery sidecar, prove the CERT-01 frame did not
move: `compute_frame_content_hash` (the ONE canonical membership recipe,
imported from the builder, never duplicated) must recompute to the SAME value
over both, and -- for sharp diagnostics when it does not -- the claim key set
and evidence id set are differenced explicitly.

The invariant this rests on, stated once: the frame hash reads ONLY the eight
claim/evidence membership fields. Nothing the 2026-08-12 amendment adds --
locus tables, region map, curated quoter list, withholding, stratum
membership, the two discovery_identification columns -- enters that tuple set,
so a schema-batch rebuild, a withholding change, or a rendered-relation change
CANNOT move the frame. A flipped confidence band or a dropped claim MUST.
Both directions are pinned by test (the mutation control); this script is the
deploy-time execution of the same contract against real assets.

Deploy-1 note: deploy 1 changes RENDERED LABELS deliberately; it must not
change membership. So this gate runs old-live vs new-candidate and must pass
even while B's --compare --expect-delta reports the (approved) label delta.

Usage:
    python scripts/check_frame_regression.py <before_db> <after_db>

Exit 0: frame identical. Exit 1: frame moved (every difference printed).
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import List, Set, Tuple

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
for _p in (_REPO_ROOT, _SCRIPTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import build_discovery_sidecar as sidecar_build  # canonical frame-hash recipe


def _connect_ro(db_path: str) -> sqlite3.Connection:
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _claim_keys(conn: sqlite3.Connection) -> Set[Tuple[str, str]]:
    return {
        (page_id, work_id)
        for page_id, work_id in conn.execute(
            "SELECT page_id, work_id FROM discovery_claim")
    }


def _evidence_ids(conn: sqlite3.Connection) -> Set[str]:
    return {r[0] for r in conn.execute("SELECT evidence_id FROM discovery_evidence")}


def check_frame_regression(before_path: str, after_path: str) -> List[str]:
    """Return every frame difference between the two assets (empty = clean).

    Callable-alone (mirrors the verifier's check_* convention) so a test can
    hand it a mutated fixture and assert the gate reports -- the gate must be
    PROVEN able to fail, not read as able to fail."""
    violations: List[str] = []
    before = _connect_ro(before_path)
    after = _connect_ro(after_path)
    try:
        hash_before = sidecar_build.compute_frame_content_hash(before)
        hash_after = sidecar_build.compute_frame_content_hash(after)
        if hash_before == hash_after:
            return []

        violations.append(
            f"frame_content_hash moved: {hash_before} (before) != {hash_after} (after)"
        )
        # The hash says THAT it moved; the set differences say WHERE.
        claims_before, claims_after = _claim_keys(before), _claim_keys(after)
        for page_id, work_id in sorted(claims_before - claims_after):
            violations.append(f"claim removed: (page_id={page_id!r}, work_id={work_id!r})")
        for page_id, work_id in sorted(claims_after - claims_before):
            violations.append(f"claim added: (page_id={page_id!r}, work_id={work_id!r})")
        ev_before, ev_after = _evidence_ids(before), _evidence_ids(after)
        for evidence_id in sorted(ev_before - ev_after):
            violations.append(f"evidence removed: {evidence_id}")
        for evidence_id in sorted(ev_after - ev_before):
            violations.append(f"evidence added: {evidence_id}")
        if len(violations) == 1:
            violations.append(
                "membership key sets are IDENTICAL -- the hash moved on a per-row "
                "FIELD (claim_type / display_evidence_id / evidence_kind / "
                "evidence_source / confidence_band): diff the eight hashed fields "
                "row-by-row to locate it"
            )
        return violations
    finally:
        before.close()
        after.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("before_db", help="The BEFORE asset (e.g. the live sidecar)")
    parser.add_argument("after_db", help="The AFTER asset (e.g. the deploy candidate)")
    args = parser.parse_args(argv)
    violations = check_frame_regression(args.before_db, args.after_db)
    if violations:
        for v in violations:
            print(f"FRAME REGRESSION: {v}", file=sys.stderr)
        print(
            f"check_frame_regression: {len(violations)} difference(s) -- FAILED "
            "(the CERT-01 frame moved)",
            file=sys.stderr,
        )
        return 1
    print("check_frame_regression: frame identical -- clean.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
