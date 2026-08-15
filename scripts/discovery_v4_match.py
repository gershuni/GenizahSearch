#!/usr/bin/env python3
"""Run and promote the Discovery V4 full-corpus Track-1 frame.

The expensive matcher remains the reviewed, reference-coordinate-aware research
runner.  This wrapper pins that runner and its calibration input, guarantees that
no page allowlist is active, keeps output in a staged table, and promotes the
result only after every page batch is committed.  Promotion happens only inside
the operator-supplied research DB; the original V2 table is retained there as
``track1_matches_v2_snapshot``.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import pickle
import re
import sqlite3
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable

try:
    from scripts.discovery_v4_common import require_hash, sha256_file, stable_json_dump
except ModuleNotFoundError:  # direct invocation
    from discovery_v4_common import require_hash, sha256_file, stable_json_dump


PILOT_RELATIVE = Path("rsource/scripts/gen2_track1_pilot.py")
PILOT_SHA256 = "1d2b2695d87a46e28b143f2567c6743b78a0ee9deda87d8d526dfaf2251f0f6d"
CALIBRATION_RELATIVE = Path("data/p_calibration_final.json")
CALIBRATION_SHA256 = "61d47db486af4bd5af64230db615d2e8e2c0b13537bba4631aa4602d024824a3"
EXPECTED_PAGE_COUNT = 667_411
GENERATION = "live"
OVERLAP_FRAC = 0.6
MIN_DENSITY_GAP = 0.03
TRACK1_PROMOTED_COLUMNS = (
    "page_id",
    "sys_id",
    "work_id",
    "cat",
    "genre",
    "author",
    "title",
    "matched_letters",
    "best_density",
    "n_spans",
    "spans_json",
    "generation",
    "ref_spans_json",
    "shadowed_by",
)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]


def validate_inputs(args: argparse.Namespace) -> dict:
    probe_root = Path(args.probe_root).resolve()
    db = Path(args.db).resolve()
    reference = Path(args.reference).resolve()
    masks = Path(args.masks).resolve()
    pilot = probe_root / PILOT_RELATIVE
    calibration = probe_root / CALIBRATION_RELATIVE
    for label, path in (
        ("research DB", db),
        ("V4 reference", reference),
        ("V4 masks", masks),
        ("Track-1 pilot", pilot),
        ("calibration model", calibration),
    ):
        if not path.is_file():
            raise FileNotFoundError(f"{label} not found: {path}")
    require_hash(reference, args.reference_sha256, "V4 reference")
    require_hash(masks, args.masks_sha256, "V4 masks")
    require_hash(pilot, PILOT_SHA256, "Track-1 pilot")
    require_hash(calibration, CALIBRATION_SHA256, "calibration model")
    if not re.fullmatch(r"[0-9a-f]{64}", args.source_db_sha256):
        raise ValueError("--source-db-sha256 must be a lowercase SHA-256")
    with reference.open("rb") as stream:
        works = pickle.load(stream)
    raw_ids = [work.get("id") for work in works]
    if len(raw_ids) != len(set(raw_ids)):
        raise ValueError("V4 reference contains duplicate raw work ids")
    if not any(str(raw_id).startswith("REF4:") for raw_id in raw_ids):
        raise ValueError("V4 reference contains no REF4 works")
    with sqlite3.connect(db) as conn:
        if not table_exists(conn, "pages"):
            raise ValueError("research DB has no pages table")
        page_count = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
        if page_count != EXPECTED_PAGE_COUNT:
            raise ValueError(
                f"research page frame drift: expected {EXPECTED_PAGE_COUNT}, got {page_count}"
            )
        if not table_exists(conn, "stage0_sys_flags"):
            raise ValueError("research DB has no stage0_sys_flags table")
    return {
        "db": str(db),
        "source_db_seed_sha256": args.source_db_sha256,
        "reference": str(reference),
        "reference_sha256": sha256_file(reference),
        "masks": str(masks),
        "masks_sha256": sha256_file(masks),
        "pilot": str(pilot),
        "pilot_sha256": sha256_file(pilot),
        "calibration_sha256": sha256_file(calibration),
        "page_count": page_count,
        "reference_count": len(works),
        "ref4_count": sum(str(raw_id).startswith("REF4:") for raw_id in raw_ids),
    }


def staged_table(tag: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", tag):
        raise ValueError("tag must contain only letters, digits, and underscores")
    return f"track1_matches_pilot_{tag}_{GENERATION}"


def pin_batch_geometry(db: str, tag: str, page_batch: int) -> None:
    """Prevent the upstream resume key from being reused at a new batch size."""
    key = f"discovery_v4_page_batch_{tag}_{GENERATION}"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS gen2_meta (key TEXT PRIMARY KEY, value TEXT)"
        )
        row = conn.execute("SELECT value FROM gen2_meta WHERE key=?", (key,)).fetchone()
        if row is not None and int(row[0]) != page_batch:
            raise ValueError(
                f"V4 resume batch-size mismatch: stored {row[0]}, requested {page_batch}"
            )
        conn.execute("INSERT OR REPLACE INTO gen2_meta VALUES (?, ?)", (key, page_batch))
        conn.commit()


def pin_source_db_seed(db: str, tag: str, expected_sha256: str) -> None:
    """Hash the pristine copied DB once, before the first staged write."""
    key = f"discovery_v4_source_db_seed_sha256_{tag}_{GENERATION}"
    with sqlite3.connect(db) as conn:
        have_meta = table_exists(conn, "gen2_meta")
        row = (
            conn.execute("SELECT value FROM gen2_meta WHERE key=?", (key,)).fetchone()
            if have_meta
            else None
        )
    if row is not None:
        if row[0] != expected_sha256:
            raise ValueError("stored V4 source-DB seed hash differs from the requested pin")
        return
    table = staged_table(tag)
    with sqlite3.connect(db) as conn:
        if table_exists(conn, table):
            raise ValueError("cannot establish a source-DB seed hash after staging began")
    require_hash(db, expected_sha256, "source research DB seed")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS gen2_meta (key TEXT PRIMARY KEY, value TEXT)"
        )
        conn.execute("INSERT INTO gen2_meta VALUES (?, ?)", (key, expected_sha256))
        conn.commit()


def run_match(args: argparse.Namespace) -> dict:
    report = validate_inputs(args)
    pin_source_db_seed(report["db"], args.tag, args.source_db_sha256)
    pin_batch_geometry(report["db"], args.tag, args.page_batch)
    pilot = Path(report["pilot"])
    env = os.environ.copy()
    env.update(
        {
            "GEN2_GEN": GENERATION,
            "GEN2_TIER": "a",
            "GEN2_MASKS": report["masks"],
            "MAPV2_PAGE_BATCH": str(args.page_batch),
            "PYTHONUTF8": "1",
        }
    )
    # V4 must discover matches on pages that were absent from the old frame.
    env.pop("MAPV2_SAMPLE_PAGES", None)
    env.pop("GEN2_PILOT_FORCE", None)
    command = [
        sys.executable,
        "-X",
        "utf8",
        "-u",
        str(pilot),
        report["db"],
        args.tag,
        report["reference"],
    ]
    print("Running full-corpus V4 matcher (no page allowlist):", flush=True)
    print(" ".join(command), flush=True)
    completed = subprocess.run(command, env=env, check=False)
    if completed.returncode:
        raise SystemExit(completed.returncode)
    status = inspect_stage(args, report=report)
    if not status["complete"]:
        raise RuntimeError("matcher exited without committing every page batch")
    return status


def inspect_stage(args: argparse.Namespace, report: dict | None = None) -> dict:
    report = report or validate_inputs(args)
    table = staged_table(args.tag)
    key_done = f"pilot_done_{args.tag}_{GENERATION}"
    key_fp = f"fp_{args.tag}_{GENERATION}"
    expected_batches = math.ceil(report["page_count"] / args.page_batch)
    with sqlite3.connect(report["db"]) as conn:
        if not table_exists(conn, table):
            return {
                **report,
                "table": table,
                "complete": False,
                "reason": "staged table absent",
                "expected_batches": expected_batches,
            }
        done_row = conn.execute(
            "SELECT value FROM gen2_meta WHERE key=?", (key_done,)
        ).fetchone()
        fp_row = conn.execute(
            "SELECT value FROM gen2_meta WHERE key=?", (key_fp,)
        ).fetchone()
        done_batch = int(done_row[0]) if done_row else -1
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        ref4_rows = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE work_id LIKE 'REF4:%'"
        ).fetchone()[0]
        missing_offsets = conn.execute(
            f"SELECT COUNT(*) FROM {table} "
            "WHERE ref_spans_json IS NULL OR ref_spans_json='' OR ref_spans_json='[]'"
        ).fetchone()[0]
        duplicate_pairs = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT page_id, work_id, generation, COUNT(*) n "
            f"FROM {table} GROUP BY page_id, work_id, generation HAVING n != 1)"
        ).fetchone()[0]
    complete = done_batch == expected_batches - 1
    return {
        **report,
        "table": table,
        "expected_batches": expected_batches,
        "done_batch": done_batch,
        "complete": complete,
        "fingerprint": fp_row[0] if fp_row else None,
        "row_count": row_count,
        "ref4_rows": ref4_rows,
        "missing_ref_offsets": missing_offsets,
        "duplicate_pairs": duplicate_pairs,
    }


def _best_span(spans_json: str) -> tuple[int, int, float]:
    spans = [
        (int(span[0]), int(span[1]), float(span[2]))
        for span in json.loads(spans_json)
    ]
    if not spans:
        raise ValueError("Track-1 row has no page spans")
    return max(spans, key=lambda span: span[1] - span[0])


def shadow_rows(
    rows: Iterable[tuple[int, str, str, float, str]],
) -> list[tuple[str, int]]:
    """Return ``(winner_work_id, shadowed_rowid)`` using the frozen algorithm."""
    by_page: dict[str, list[tuple[int, str, float, tuple[int, int, float]]]] = (
        defaultdict(list)
    )
    for rowid, page_id, work_id, best_density, spans_json in rows:
        by_page[page_id].append(
            (rowid, work_id, float(best_density), _best_span(spans_json))
        )
    shadows: list[tuple[str, int]] = []
    for items in by_page.values():
        if len(items) < 2:
            continue
        items.sort(key=lambda item: item[3][2])
        live: list[tuple[str, int, int, float]] = []
        for rowid, work_id, _best_density, span in items:
            start, end, density = span
            winner = None
            for live_work, live_start, live_end, live_density in live:
                overlap = min(end, live_end) - max(start, live_start)
                if (
                    overlap >= OVERLAP_FRAC * (end - start)
                    and density - live_density >= MIN_DENSITY_GAP
                ):
                    winner = live_work
                    break
            if winner is None:
                live.append((work_id, start, end, density))
            else:
                shadows.append((winner, rowid))
    return shadows


def promote(args: argparse.Namespace) -> dict:
    status = inspect_stage(args)
    if not status["complete"]:
        raise RuntimeError("cannot promote an incomplete V4 matcher stage")
    if status["missing_ref_offsets"] or status["duplicate_pairs"]:
        raise RuntimeError("cannot promote a stage with missing offsets or duplicate pairs")
    table = status["table"]
    with sqlite3.connect(status["db"]) as conn:
        conn.execute("PRAGMA busy_timeout=120000")
        if not table_exists(conn, "track1_matches_v2_snapshot"):
            if not table_exists(conn, "track1_matches"):
                raise ValueError("research DB has no original track1_matches table")
            if "ref_spans_json" in table_columns(conn, "track1_matches"):
                raise ValueError("refusing to snapshot an already-promoted Track-1 table")
            conn.execute(
                "ALTER TABLE track1_matches RENAME TO track1_matches_v2_snapshot"
            )
            conn.commit()
        if table_exists(conn, "track1_matches"):
            conn.execute("DROP TABLE track1_matches")
        conn.execute(
            """CREATE TABLE track1_matches (
                page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT,
                author TEXT, title TEXT,
                matched_letters INTEGER, best_density REAL, n_spans INTEGER,
                spans_json TEXT, generation TEXT, ref_spans_json TEXT,
                shadowed_by TEXT
            )"""
        )
        conn.execute(
            f"""INSERT INTO track1_matches (
                page_id, sys_id, work_id, cat, genre, author, title,
                matched_letters, best_density, n_spans, spans_json, generation,
                ref_spans_json, shadowed_by
            ) SELECT page_id, sys_id, work_id, cat, genre, author, title,
                matched_letters, best_density, n_spans, spans_json, generation,
                ref_spans_json, NULL
              FROM {table} ORDER BY rowid"""
        )
        conn.execute(
            "CREATE UNIQUE INDEX uq_track1_matches_v4 "
            "ON track1_matches(page_id, work_id, generation)"
        )
        conn.execute("CREATE INDEX ix_track1_matches_page ON track1_matches(page_id)")
        rows = conn.execute(
            "SELECT rowid, page_id, work_id, best_density, spans_json "
            "FROM track1_matches ORDER BY rowid"
        ).fetchall()
        shadows = shadow_rows(rows)
        conn.executemany(
            "UPDATE track1_matches SET shadowed_by=? WHERE rowid=?", shadows
        )
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM track1_matches").fetchone()[0]
        live = conn.execute(
            "SELECT COUNT(*) FROM track1_matches WHERE shadowed_by IS NULL"
        ).fetchone()[0]
        ref4_total = conn.execute(
            "SELECT COUNT(*) FROM track1_matches WHERE work_id LIKE 'REF4:%'"
        ).fetchone()[0]
        ref4_live = conn.execute(
            "SELECT COUNT(*) FROM track1_matches "
            "WHERE work_id LIKE 'REF4:%' AND shadowed_by IS NULL"
        ).fetchone()[0]
        snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM track1_matches_v2_snapshot"
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if total != status["row_count"] or snapshot_count != 381_341:
        raise RuntimeError("promoted or preserved Track-1 row count drift")
    if integrity != "ok":
        raise RuntimeError(f"research DB integrity check failed: {integrity}")
    report = {
        **status,
        "schema_version": "discovery-v4-track1-frame-v1",
        "promoted_rows": total,
        "live_rows": live,
        "shadowed_rows": len(shadows),
        "ref4_promoted_rows": ref4_total,
        "ref4_live_rows": ref4_live,
        "v2_snapshot_rows": snapshot_count,
        "integrity_check": integrity,
    }
    contract = {
        "schema_version": "discovery-v4-track1-release-contract-v1",
        "reference_corpus_sha256": status["reference_sha256"],
        "canonical_masks_sha256": status["masks_sha256"],
        "source_db_seed_sha256": status["source_db_seed_sha256"],
        "matcher_fingerprint": status["fingerprint"],
        "page_count": status["page_count"],
        "total_rows": total,
        "live_rows": live,
        "ref4_total_rows": ref4_total,
        "ref4_live_rows": ref4_live,
        "v2_snapshot_rows": snapshot_count,
        "missing_ref_offsets": status["missing_ref_offsets"],
        "duplicate_pairs": status["duplicate_pairs"],
        "shadow_algorithm": "track1-shadow-v1",
        "promoted_columns": list(TRACK1_PROMOTED_COLUMNS),
    }
    report["release_contract"] = contract
    if args.report:
        stable_json_dump(report, args.report)
    if args.contract:
        stable_json_dump(contract, args.contract)
    print(json.dumps(report, indent=2))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("preflight", "run", "status", "promote"))
    parser.add_argument("--probe-root", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--source-db-sha256", required=True)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--reference-sha256", required=True)
    parser.add_argument("--masks", required=True)
    parser.add_argument("--masks-sha256", required=True)
    parser.add_argument("--tag", default="v4full")
    parser.add_argument("--page-batch", type=int, default=2000)
    parser.add_argument("--report")
    parser.add_argument("--contract")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.page_batch <= 0:
        raise ValueError("--page-batch must be positive")
    if args.action == "preflight":
        print(json.dumps(validate_inputs(args), indent=2))
    elif args.action == "run":
        print(json.dumps(run_match(args), indent=2))
    elif args.action == "status":
        print(json.dumps(inspect_stage(args), indent=2))
    else:
        promote(args)


if __name__ == "__main__":
    main()
