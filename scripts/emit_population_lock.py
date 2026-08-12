#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Emit the population lock (schema Amendment 2026-08-12 (S); CD schema batch).

Measures the PRE-withholding public candidate population — identification
grain, ``main_pool = 1`` — per fam-v1 family, ONCE, against a named public
asset, and freezes the result as a tracked JSON. The builder copies these
constants into every subsequent asset's meta (never recomputes them — the
projector's copy-vs-recompute table says why), and the verifier's retention
gate enforces the floors (overall >= 90%, per family >= 80%, A0b ratifies)
against the CURRENT population on every shipped asset.

The daf stratum's canonical ids are measured here from the locus artifact
(base family ``other_staged`` AND locus grain ``daf_rif``) and pinned into the
lock, so every future family recomputation is possible from the asset alone —
the locus tables are legitimately empty between the schema batch and the
D-track import (Codex pre-flight finding 8: the lock's denominator is
INDEPENDENT of locus addressability).

Usage:
    python scripts/emit_population_lock.py \
        --public-db _tmp/v3_out2/discovery-v3-PUBLIC.db \
        --locus-db _tmp/work_divisions.db \
        --crosswalk discovery_data/crosswalk.json \
        --out docs/specs/discovery-population-lock-v1.json

Masking posture: the emitted JSON carries OPAQUE ids and counts only — no
title, no reference text, no raw work id (the crosswalk is consumed here, in
scope, and never stored).
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import sqlite3
import sys
from datetime import date
from pathlib import Path

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
for _p in (_REPO_ROOT, _SCRIPTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared import discovery_family as fam

LOCK_VERSION = "poplock-v1"

# The retention floors the verifier enforces against this lock. Recorded IN
# the lock so the numbers and their floors travel together; A0b ratifies or
# amends (a new floor = a new lock file version, never an in-place edit).
RETENTION_FLOOR_OVERALL = 0.90
RETENTION_FLOOR_PER_FAMILY = 0.80


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _connect_ro(path: str) -> sqlite3.Connection:
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def measure_daf_overrides(public_conn, locus_conn, crosswalk: dict) -> list:
    """Canonical ids of the daf stratum: base other_staged AND grain daf_rif."""
    works = {
        w: (g, c) for w, g, c in public_conn.execute(
            "SELECT work_id, genre, source_corpus FROM works")
    }
    canon_of = {
        w: c for w, c in public_conn.execute(
            "SELECT work_id, canonical_work_id FROM works")
    }
    overrides = set()
    for locus_ref_id, grain in locus_conn.execute(
        "SELECT locus_ref_id, grain FROM locus_work"
    ):
        if grain != "daf_rif":
            continue
        work_id = crosswalk.get(locus_ref_id)
        if work_id is None or work_id not in works:
            continue
        genre, corpus = works[work_id]
        if fam.base_family(genre, corpus) == fam.FAMILY_OTHER_STAGED:
            overrides.add(canon_of[work_id])
    return sorted(overrides)


def measure_population(public_conn, daf_overrides) -> dict:
    """Per-family main-pool identification counts under fam-v1."""
    works = {
        w: (g, c) for w, g, c in public_conn.execute(
            "SELECT work_id, genre, source_corpus FROM works")
    }
    counts = collections.Counter()
    for display_work_id, canonical_work_id in public_conn.execute(
        "SELECT display_work_id, canonical_work_id FROM discovery_identification "
        "WHERE main_pool = 1"
    ):
        genre, corpus = works.get(display_work_id, (None, None))
        counts[fam.assign_family(genre, corpus, canonical_work_id, daf_overrides)] += 1
    return {family: counts.get(family, 0) for family in fam.FAMILIES}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--public-db", required=True)
    parser.add_argument("--locus-db", required=True)
    parser.add_argument("--crosswalk", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    with open(args.crosswalk, encoding="utf-8") as fh:
        crosswalk = json.load(fh)
    crosswalk = crosswalk.get("crosswalk", crosswalk)

    public_conn = _connect_ro(args.public_db)
    locus_conn = _connect_ro(args.locus_db)
    try:
        # The lock is only meaningful over a PUBLIC asset.
        audience = {
            k: v for k, v in public_conn.execute("SELECT key, value FROM meta")
        }.get("audience")
        if audience != "public":
            print(f"ERROR: --public-db has meta.audience={audience!r}, expected "
                  "'public' -- the lock governs the public candidate population",
                  file=sys.stderr)
            return 1
        daf_overrides = measure_daf_overrides(public_conn, locus_conn, crosswalk)
        by_family = measure_population(public_conn, daf_overrides)
    finally:
        public_conn.close()
        locus_conn.close()

    lock = {
        "lock_version": LOCK_VERSION,
        "family_version": fam.FAMILY_VERSION,
        "emitted": date.today().isoformat(),
        "measured_against": {
            "public_db_sha256": _sha256_file(args.public_db),
            "locus_db_sha256": _sha256_file(args.locus_db),
            "crosswalk_sha256": _sha256_file(args.crosswalk),
        },
        "retention_floor_overall": RETENTION_FLOOR_OVERALL,
        "retention_floor_per_family": RETENTION_FLOOR_PER_FAMILY,
        "total": sum(by_family.values()),
        "by_family": by_family,
        "daf_override_canonical_ids": daf_overrides,
    }
    out_path = Path(args.out)
    out_path.write_text(
        json.dumps(lock, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"population lock written: {out_path}")
    print(f"  total={lock['total']:,}  by_family={by_family}")
    print(f"  daf overrides: {len(daf_overrides)} canonical id(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
