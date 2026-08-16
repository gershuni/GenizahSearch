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
    from scripts.discovery_track1_contract import (
        CONTRACT_V2_SCHEMA_VERSION,
        IDENTITY_MODES,
        classify_work_id,
        derive_run_id,
        extrapolated_namespaces,
        load_cohort_registry,
        validate_contract_v2,
    )
except ModuleNotFoundError:  # direct invocation
    from discovery_v4_common import require_hash, sha256_file, stable_json_dump
    from discovery_track1_contract import (
        CONTRACT_V2_SCHEMA_VERSION,
        IDENTITY_MODES,
        classify_work_id,
        derive_run_id,
        extrapolated_namespaces,
        load_cohort_registry,
        validate_contract_v2,
    )


PILOT_RELATIVE = Path("rsource/scripts/gen2_track1_pilot.py")
PILOT_SHA256 = "1d2b2695d87a46e28b143f2567c6743b78a0ee9deda87d8d526dfaf2251f0f6d"
CALIBRATION_RELATIVE = Path("data/p_calibration_final.json")
CALIBRATION_SHA256 = "61d47db486af4bd5af64230db615d2e8e2c0b13537bba4631aa4602d024824a3"
EXPECTED_PAGE_COUNT = 667_411
GENERATION = "live"
OVERLAP_FRAC = 0.6
MIN_DENSITY_GAP = 0.03
DEFAULT_COHORT_REGISTRY = Path(__file__).with_name("discovery_routing_cohorts.json")
# The shadow-feeding SELECT (see ``promote``) is pinned ``ORDER BY rowid`` --
# verified (docs/specs/discovery-v4.2-combined-bake-and-public-first-plan.md
# condition C1) to be the exact order SQLite produces for that query, since an
# explicit ORDER BY is authoritative regardless of the access path SQLite's
# planner chooses. Recorded as a fact in the v2 release contract rather than
# left implicit, so a reproduction attempt can tell what order was used
# without changing ``shadow_rows`` itself (frozen algorithm).
SHADOW_ALGORITHM_FACT = "track1-shadow-v1/input-order:rowid"
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


def classify_reference_ids(raw_ids: Iterable[str], registry: dict) -> dict[str, int]:
    """Classify reference raw ids by namespace using the shared registry.

    Every ``REF*``-prefixed id must belong to a registered namespace;
    ``classify_work_id`` raises for an unregistered prefix and this function
    deliberately lets that propagate (V4.2 plan C4/C12: an unknown reference
    generation is a hard error, never a silent legacy fallback).
    """
    counts: dict[str, int] = defaultdict(int)
    for raw_id in raw_ids:
        classified = classify_work_id(str(raw_id), registry)
        if classified is not None:
            namespace, _cohort = classified
            counts[namespace] += 1
    return dict(counts)


def assert_has_extrapolated_reference(
    reference_namespace_counts: dict[str, int], extrapolated: set[str]
) -> None:
    """The generalized V4.2 acceptance gate (plan C4, amending the V4 REF4-only
    check): at least one reference work from SOME extrapolated namespace,
    not specifically REF4. A REF6-only or REF5-only reference build now
    passes this gate exactly as a REF4-only one always has.
    """
    if not any(reference_namespace_counts.get(ns, 0) for ns in extrapolated):
        raise ValueError(
            "V4 reference contains no works from any extrapolated namespace "
            f"(checked {sorted(extrapolated)}, found {reference_namespace_counts})"
        )


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
    registry = load_cohort_registry(args.cohort_registry)
    extrapolated = extrapolated_namespaces(registry)
    reference_namespace_counts = classify_reference_ids(raw_ids, registry)
    assert_has_extrapolated_reference(reference_namespace_counts, extrapolated)
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
        "reference_namespace_counts": dict(sorted(reference_namespace_counts.items())),
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


# --- C1: matcher run identity -------------------------------------------------
#
# Binds the staged/resumed table to ALL of a run's inputs (reference, masks,
# source-DB seed, pilot, calibration hashes, page frame, batch geometry, tag,
# generation) via the shared ``derive_run_id``. ``run`` writes this write-once;
# ``status``/``promote`` only verify. A stored id that differs from what the
# CURRENT inputs compute is the stale-table guard: a staged table can only be
# resumed or promoted by the run that created it.


def compute_run_id(report: dict, *, page_batch: int, tag: str) -> str:
    facts = {
        "reference_corpus_sha256": report["reference_sha256"],
        "canonical_masks_sha256": report["masks_sha256"],
        "source_db_seed_sha256": report["source_db_seed_sha256"],
        "pilot_sha256": report["pilot_sha256"],
        "calibration_sha256": report["calibration_sha256"],
        "page_count": report["page_count"],
        "page_batch": page_batch,
        "generation": GENERATION,
        "tag": tag,
    }
    return derive_run_id(facts)


def run_identity_key(tag: str) -> str:
    return f"discovery_v4_run_id_{tag}_{GENERATION}"


def pin_run_identity(db: str, tag: str, run_id: str) -> None:
    """Write-once bind of a staged/resumed table to the run that created it."""
    key = run_identity_key(tag)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS gen2_meta (key TEXT PRIMARY KEY, value TEXT)"
        )
        row = conn.execute("SELECT value FROM gen2_meta WHERE key=?", (key,)).fetchone()
        if row is not None and row[0] != run_id:
            raise ValueError(
                "V4 run-identity mismatch: this staged table was created by run_id "
                f"{row[0]!r}, but the current inputs compute run_id {run_id!r}. A "
                "staged table can only be resumed by the run that created it."
            )
        if row is None:
            conn.execute("INSERT INTO gen2_meta VALUES (?, ?)", (key, run_id))
            conn.commit()


def verify_run_identity(db: str, tag: str, run_id: str) -> None:
    """Re-verify a recorded run id at ``status``/``promote`` time. Never a warning."""
    key = run_identity_key(tag)
    with sqlite3.connect(db) as conn:
        if not table_exists(conn, "gen2_meta"):
            stored = None
        else:
            row = conn.execute("SELECT value FROM gen2_meta WHERE key=?", (key,)).fetchone()
            stored = row[0] if row else None
    if stored is None:
        raise ValueError(
            f"V4 run identity is not recorded for tag={tag!r} (expected {run_id!r}); "
            "run the matcher via 'run' before checking status or promoting."
        )
    if stored != run_id:
        raise ValueError(
            f"V4 run-identity mismatch: stored run_id {stored!r}, current inputs "
            f"compute run_id {run_id!r}. A staged table can only be resumed or "
            "promoted by the run that created it."
        )


# --- C1: batch ledger ----------------------------------------------------------
#
# The upstream matcher records only a single high-water ``pilot_done_*``
# integer per (tag, generation) in gen2_meta (see ``inspect_stage``'s
# ``key_done`` read) -- no per-batch ledger. This wrapper-maintained table
# proves every batch 0..expected_batches-1 was individually observed complete,
# so promotion cannot rely on the high-water mark alone.

BATCH_LEDGER_TABLE = "track1_v4_batch_ledger"


def _ensure_batch_ledger_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {BATCH_LEDGER_TABLE} ("
        "tag TEXT NOT NULL, generation TEXT NOT NULL, batch_index INTEGER NOT NULL, "
        "PRIMARY KEY (tag, generation, batch_index))"
    )


def record_batch_ledger(db: str, tag: str, done_batch: int) -> None:
    """Fill every batch index ``0..done_batch`` (inclusive) not yet recorded.

    Deliberately re-scans the full ``0..done_batch`` range on every call
    rather than resuming from the previously-recorded maximum: the upstream
    matcher exposes only a high-water ``pilot_done_*`` mark (see
    ``inspect_stage``), so an earlier invocation could in principle have
    observed a non-contiguous state. Re-filling from zero each time is what
    actually closes a gap once the run progresses past it, instead of
    permanently missing it because the recorded maximum had already moved on.

    HONEST LIMIT (V4.2 plan C1): because the only observable upstream state is
    that high-water mark, this ledger records what the wrapper OBSERVED at
    each invocation boundary -- it cannot independently prove a batch the
    upstream never individually marked. The real per-batch guarantee is the
    conjunction of (a) the upstream advancing ``pilot_done_*`` strictly
    sequentially, only after committing that batch, and (b) the run-identity
    pin ensuring every invocation that fed this table was the same run over
    the same geometry. A truly independent per-batch proof needs the upstream
    runner to emit per-batch markers; that runner lives in the restricted
    research tree and is out of this repo's scope.
    """
    if done_batch < 0:
        return
    with sqlite3.connect(db) as conn:
        _ensure_batch_ledger_table(conn)
        recorded = {
            row[0]
            for row in conn.execute(
                f"SELECT batch_index FROM {BATCH_LEDGER_TABLE} WHERE tag=? AND generation=?",
                (tag, GENERATION),
            )
        }
        new_rows = [
            (tag, GENERATION, batch_index)
            for batch_index in range(done_batch + 1)
            if batch_index not in recorded
        ]
        if new_rows:
            conn.executemany(
                f"INSERT OR IGNORE INTO {BATCH_LEDGER_TABLE} VALUES (?, ?, ?)", new_rows
            )
            conn.commit()


def missing_ledger_batches(db: str, tag: str, expected_batches: int) -> list[int]:
    """Return every batch index in ``0..expected_batches-1`` absent from the ledger."""
    with sqlite3.connect(db) as conn:
        if not table_exists(conn, BATCH_LEDGER_TABLE):
            return list(range(expected_batches))
        recorded = {
            row[0]
            for row in conn.execute(
                f"SELECT batch_index FROM {BATCH_LEDGER_TABLE} WHERE tag=? AND generation=?",
                (tag, GENERATION),
            )
        }
    return [index for index in range(expected_batches) if index not in recorded]


# --- C3: release-contract v2 namespace/identity-mode counting -----------------


def classify_source_identity_mode(source: dict) -> str:
    """Return a REF6-style source entry's identity_mode (V4.2 plan C3/C4).

    An explicit ``identity_mode`` field is authoritative. Absent that, an
    entry whose mappings carry a ``target_work_id`` is ``private_sibling``
    (the V4/V4.1 shape); anything else cannot be classified and is a hard
    error rather than a silent guess.
    """
    identity_mode = source.get("identity_mode")
    if identity_mode is not None:
        if identity_mode not in IDENTITY_MODES:
            raise ValueError(
                f"source {source.get('key')!r} has an invalid identity_mode: "
                f"{identity_mode!r}"
            )
        return identity_mode
    mappings = source.get("mappings") or []
    if any(isinstance(mapping, dict) and mapping.get("target_work_id") for mapping in mappings):
        return "private_sibling"
    raise ValueError(
        f"source {source.get('key')!r} cannot be classified: no identity_mode and "
        "no mapping carries a target_work_id"
    )


def load_identity_mode_map(path: str | Path) -> dict[str, str]:
    """Load a V4-family source map and return ``{source_key: identity_mode}``."""
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if doc.get("schema_version") != "discovery-v4-sources-v1":
        raise ValueError(f"unsupported source map schema_version: {path}")
    sources = doc.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ValueError(f"source map has no sources: {path}")
    modes: dict[str, str] = {}
    for source in sources:
        key = source.get("key")
        if not isinstance(key, str) or not key:
            raise ValueError(f"source map {path} has an entry with no key")
        if key in modes:
            raise ValueError(f"source map {path} has a duplicate key: {key}")
        modes[key] = classify_source_identity_mode(source)
    return modes


def per_entry_identity_mode_counts(
    conn: sqlite3.Connection, namespace: str, key_to_mode: dict[str, str]
) -> dict[str, dict[str, int]]:
    """Split a namespace's promoted-table rows by identity_mode.

    The raw id a source with a single mapping produces is
    ``f"{namespace}:{source_key}"``; with more than one mapping it gains a
    ``:suffix`` (see ``discovery_v4_build_reference.raw_reference_id``). The
    source key is always the first colon-delimited component after the
    namespace prefix, and source keys never themselves contain a colon
    (enforced at source-map load time), so splitting on the first remaining
    colon recovers it unambiguously.
    """
    counts: dict[str, dict[str, int]] = {
        mode: {"total_rows": 0, "live_rows": 0} for mode in IDENTITY_MODES
    }
    prefix = f"{namespace}:"
    rows = conn.execute(
        "SELECT work_id, shadowed_by FROM track1_matches WHERE work_id LIKE ?",
        (f"{prefix}%",),
    ).fetchall()
    for work_id, shadowed_by in rows:
        source_key = str(work_id)[len(prefix):].split(":", 1)[0]
        mode = key_to_mode.get(source_key)
        if mode is None:
            raise ValueError(
                f"{namespace} raw id {work_id!r} has source key {source_key!r} "
                "absent from the source map"
            )
        counts[mode]["total_rows"] += 1
        if shadowed_by is None:
            counts[mode]["live_rows"] += 1
    return counts


def _assert_no_unknown_ref_namespaces(conn: sqlite3.Connection, registry: dict) -> None:
    """Classify every distinct REF*-prefixed promoted work id.

    ``classify_work_id`` raises for a prefix absent from the registry; that
    propagation IS the gate (V4.2 plan C3/C12: an unknown reference
    generation in promoted match data must never be silently uncounted).
    """
    rows = conn.execute(
        "SELECT DISTINCT work_id FROM track1_matches WHERE work_id LIKE 'REF%'"
    ).fetchall()
    for (work_id,) in rows:
        classify_work_id(work_id, registry)


def namespace_counts_for_contract(
    conn: sqlite3.Connection, registry: dict, registry_path: str | Path
) -> dict[str, dict]:
    """Build the v2 contract's ``namespaces`` object from a promoted DB.

    Every extrapolated namespace appears, including at an explicit zero. The
    registry's own ``per_entry`` cohorts (today: REF6) additionally get a
    ``by_identity_mode`` split, matching what the frozen
    ``validate_contract_v2`` requires.
    """
    _assert_no_unknown_ref_namespaces(conn, registry)
    registry_path = Path(registry_path)
    extrapolated = extrapolated_namespaces(registry)
    # ``per_entry`` cohorts (today: only REF6) get a ``by_identity_mode`` split
    # below. It is necessarily a subset of ``extrapolated`` (both are filtered
    # from the same registry cohorts by ``cohort == "extrapolated"``); if a
    # future registry ever named a per_entry namespace the shared
    # ``validate_contract_v2`` does not also expect a split for, that call
    # rejects the contract rather than this function guessing.
    per_entry = {
        cohort["namespace"]
        for cohort in registry["cohorts"]
        if cohort.get("cohort") == "extrapolated" and cohort.get("identity_mode") == "per_entry"
    }
    namespaces: dict[str, dict] = {}
    for namespace in sorted(extrapolated):
        total = conn.execute(
            "SELECT COUNT(*) FROM track1_matches WHERE work_id LIKE ?",
            (f"{namespace}:%",),
        ).fetchone()[0]
        live = conn.execute(
            "SELECT COUNT(*) FROM track1_matches WHERE work_id LIKE ? AND shadowed_by IS NULL",
            (f"{namespace}:%",),
        ).fetchone()[0]
        counts: dict = {"total_rows": total, "live_rows": live}
        if namespace in per_entry:
            cohort = next(c for c in registry["cohorts"] if c["namespace"] == namespace)
            source_map_path = registry_path.parent / cohort["source_map"]
            key_to_mode = load_identity_mode_map(source_map_path)
            modes = per_entry_identity_mode_counts(conn, namespace, key_to_mode)
            split_total = sum(mode_counts["total_rows"] for mode_counts in modes.values())
            if split_total != total:
                raise RuntimeError(
                    f"{namespace} identity-mode split does not reconcile with its "
                    f"total: split={split_total} total={total}"
                )
            counts["by_identity_mode"] = modes
        namespaces[namespace] = counts
    return namespaces


def build_release_contract_v2(
    status: dict,
    conn: sqlite3.Connection,
    *,
    total_rows: int,
    live_rows: int,
    snapshot_rows: int,
    registry: dict,
    registry_path: str | Path,
) -> dict:
    """Build and self-validate the v2 release contract (V4.2 plan C3).

    Validated against the shared ``validate_contract_v2`` BEFORE returning: an
    emitter that cannot validate its own output must fail rather than write a
    document a consumer would reject.
    """
    namespaces = namespace_counts_for_contract(conn, registry, registry_path)
    contract = {
        "schema_version": CONTRACT_V2_SCHEMA_VERSION,
        "run_id": status["run_id"],
        "reference_corpus_sha256": status["reference_sha256"],
        "canonical_masks_sha256": status["masks_sha256"],
        "source_db_seed_sha256": status["source_db_seed_sha256"],
        "pilot_sha256": status["pilot_sha256"],
        "calibration_sha256": status["calibration_sha256"],
        "matcher_fingerprint": status["fingerprint"],
        "page_count": status["page_count"],
        "page_batch": status["page_batch"],
        "expected_batches": status["expected_batches"],
        "total_rows": total_rows,
        "live_rows": live_rows,
        "v2_snapshot_rows": snapshot_rows,
        "missing_ref_offsets": status["missing_ref_offsets"],
        "duplicate_pairs": status["duplicate_pairs"],
        "shadow_algorithm": SHADOW_ALGORITHM_FACT,
        "promoted_columns": list(TRACK1_PROMOTED_COLUMNS),
        "namespaces": namespaces,
    }
    validate_contract_v2(contract, expected_namespaces=extrapolated_namespaces(registry))
    return contract


def run_match(args: argparse.Namespace) -> dict:
    report = validate_inputs(args)
    run_id = compute_run_id(report, page_batch=args.page_batch, tag=args.tag)
    pin_run_identity(report["db"], args.tag, run_id)
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
    if "done_batch" in status:
        record_batch_ledger(report["db"], args.tag, status["done_batch"])
    if not status["complete"]:
        raise RuntimeError("matcher exited without committing every page batch")
    return status


def inspect_stage(args: argparse.Namespace, report: dict | None = None) -> dict:
    report = report or validate_inputs(args)
    table = staged_table(args.tag)
    key_done = f"pilot_done_{args.tag}_{GENERATION}"
    key_fp = f"fp_{args.tag}_{GENERATION}"
    expected_batches = math.ceil(report["page_count"] / args.page_batch)
    run_id = compute_run_id(report, page_batch=args.page_batch, tag=args.tag)
    with sqlite3.connect(report["db"]) as conn:
        if not table_exists(conn, table):
            return {
                **report,
                "table": table,
                "complete": False,
                "reason": "staged table absent",
                "expected_batches": expected_batches,
                "run_id": run_id,
                "page_batch": args.page_batch,
            }
        # C1: a staged table can only be resumed/inspected by the run that
        # created it -- verify BEFORE trusting anything else read from it.
        verify_run_identity(report["db"], args.tag, run_id)
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
        "run_id": run_id,
        "page_batch": args.page_batch,
        "ledger_missing_batches": missing_ledger_batches(
            report["db"], args.tag, expected_batches
        ),
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
    # C1: prove EVERY batch completed, not merely that the upstream's own
    # high-water done_batch reached expected_batches-1 (a gap earlier in the
    # range would otherwise pass silently).
    missing_batches = missing_ledger_batches(
        status["db"], args.tag, status["expected_batches"]
    )
    if missing_batches:
        raise RuntimeError(
            f"V4 batch ledger is missing batches, refusing to promote: {missing_batches}"
        )
    registry_path = Path(args.cohort_registry)
    registry = load_cohort_registry(registry_path)
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
        snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM track1_matches_v2_snapshot"
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        # C3: the v2 release contract, built and self-validated against the
        # shared schema BEFORE anything is written to disk.
        contract = build_release_contract_v2(
            status,
            conn,
            total_rows=total,
            live_rows=live,
            snapshot_rows=snapshot_count,
            registry=registry,
            registry_path=registry_path,
        )
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
        "v2_snapshot_rows": snapshot_count,
        "integrity_check": integrity,
        "namespaces": contract["namespaces"],
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
    parser.add_argument(
        "--cohort-registry",
        default=str(DEFAULT_COHORT_REGISTRY),
        help=(
            "routing cohort registry (V4.2 plan C4); defaults to the committed "
            "discovery_routing_cohorts.json beside this script"
        ),
    )
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
