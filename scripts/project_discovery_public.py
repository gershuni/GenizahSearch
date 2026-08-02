#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""VIS-01 closed-graph public projection (Phase 136, plan 136-08).

Emits a NEW `discovery.db`-shaped SQLite artifact containing ONLY the rows
`shared.discovery_visibility.is_public` allows, then closes the graph so the
public artifact is internally consistent: no dangling foreign key, no
unreachable work, every count/aggregate/auxiliary table projected -- never
claim rows alone.

Usage:
    python scripts/project_discovery_public.py <private_db> <public_db_out>

`is_public` (`shared/discovery_visibility.py`) is the ONE eligibility rule
this script consumes -- it is never restated here. Every table present in
the private input MUST have an explicit projection rule in
`PROJECTION_RULES` below; an unrecognized table is a BUILD ERROR
(`ProjectionError`), never silently copied whole.

The projection recomputes rather than copies every stored count/aggregate,
including `discovery_identification` (re-derived bottom-up from the
SURVIVING evidence set, never filtered from the private build's own rows --
a private claim's contribution can therefore never survive into a public
aggregate) and `display_work_id` (re-selected among the canonical group's
PUBLIC members only -- the schema's own SS(B1) selection rule, restricted).

The projection's own final gate replays the emitted artifact through BOTH
masking scan surfaces (`--scan-asset` the physical bytes, `--scan-sqlite` the
logical cell content -- a SQLite file needs both, per
`check_atlas_masking.py`'s own module docstring) under `--strict --scan-repo`,
and refuses to leave a masking-dirty artifact on disk.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
for _p in (_REPO_ROOT, _SCRIPTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import discovery_ids as ids  # scripts/discovery_ids.py -- FROZEN enum vocab + select_display_evidence
import check_atlas_masking as masking  # scripts/check_atlas_masking.py -- the D-07 masking gate

import shared.discovery_visibility as visibility  # the ONE conjunction rule (is_public)


class ProjectionError(Exception):
    """Raised on a genuine BUILD ERROR: an unprojected table, an unsafe
    output path, or a masking-gate failure the caller must not silently
    swallow. Distinct from an ordinary "no violations" empty-list return --
    this exception means the build itself must stop."""


# ---------------------------------------------------------------------------
# Output-path safety: never write inside web/static/ (the served-statically
# tree -- an artifact placed there would be reachable with no route gate at
# all, defeating every other control in this file).
# ---------------------------------------------------------------------------

def _assert_output_path_safe(output_path: str) -> None:
    parts = [p.lower() for p in Path(output_path).parts]
    for i in range(len(parts) - 1):
        if parts[i] == "web" and parts[i + 1] == "static":
            raise ProjectionError(
                f"refusing to write the public projection inside web/static/: {output_path!r} "
                "-- that path is served statically with no runtime gate at all"
            )


# ---------------------------------------------------------------------------
# Reading the private asset (read-only).
# ---------------------------------------------------------------------------

def _connect_ro(path: str) -> sqlite3.Connection:
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _table_names(conn: sqlite3.Connection) -> Set[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return {row["name"] for row in cur.fetchall()}


def _schema_ddl(conn: sqlite3.Connection, table_names: Iterable[str]) -> List[str]:
    """Replay the PRIVATE db's OWN CREATE TABLE / CREATE INDEX statements
    verbatim for the given tables, so the output schema (columns, CHECK
    constraints, indexes -- "sort behaviour preserved") is identical in
    shape to the input, without this script re-typing a second copy of the
    DDL that could silently drift from the real schema."""
    table_names = list(table_names)
    ddl: List[str] = []
    cur = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE tbl_name IN ({}) AND sql IS NOT NULL "
        "ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END".format(
            ",".join("?" for _ in table_names)
        ),
        table_names,
    )
    for row in cur.fetchall():
        ddl.append(row["sql"])
    return ddl


def _rows_as_dicts(conn: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    cur = conn.execute(f"SELECT * FROM {table}")
    return [dict(row) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Reachability computation over the private asset.
# ---------------------------------------------------------------------------

class ProjectionContext:
    """Everything the per-table projection rules need, computed ONCE from
    the private asset before any output row is written."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.works_by_id: Dict[str, Dict[str, Any]] = {
            w["work_id"]: w for w in _rows_as_dicts(conn, "works")
        }
        self.canonical_groups: Dict[str, List[Dict[str, Any]]] = {}
        for w in self.works_by_id.values():
            self.canonical_groups.setdefault(w["canonical_work_id"], []).append(w)

        self.claims_by_id: Dict[str, Dict[str, Any]] = {
            c["claim_id"]: c for c in _rows_as_dicts(conn, "discovery_claim")
        }

        all_evidence = _rows_as_dicts(conn, "discovery_evidence")

        # --- Evidence survival (the D-22 conjunction, per row) -----------
        surviving_evidence_by_claim: Dict[str, List[Dict[str, Any]]] = {}
        surviving_evidence_ids: Set[str] = set()
        reachable_sys_ids: Set[str] = set()
        for ev in all_evidence:
            claim = self.claims_by_id.get(ev["claim_id"])
            if claim is None:
                continue  # orphan evidence in the PRIVATE asset -- not this script's concern
            work = self.works_by_id.get(claim["work_id"])
            identity_vis = work.get("identity_visibility") if work else None
            assertion_vis = ev.get("assertion_visibility")
            if visibility.is_public(assertion_vis, identity_vis):
                surviving_evidence_by_claim.setdefault(ev["claim_id"], []).append(ev)
                surviving_evidence_ids.add(ev["evidence_id"])
                reachable_sys_ids.add(ev["sys_id"])
        self.surviving_evidence_by_claim = surviving_evidence_by_claim
        self.surviving_evidence_ids = surviving_evidence_ids
        self.reachable_sys_ids = reachable_sys_ids
        self.surviving_claim_ids: Set[str] = {
            cid for cid, rows in surviving_evidence_by_claim.items() if rows
        }

        # --- Recomputed display_evidence_id per surviving claim ----------
        self.recomputed_display_evidence: Dict[str, str] = {
            cid: ids.select_display_evidence(rows)
            for cid, rows in surviving_evidence_by_claim.items()
            if rows
        }

        # --- Public works: identity-public AND referenced by >=1 surviving
        #     claim (a work with zero surviving claims is unreachable and
        #     must be dropped even if its OWN identity_visibility is public).
        referenced_work_ids: Set[str] = set()
        for cid in self.surviving_claim_ids:
            referenced_work_ids.add(self.claims_by_id[cid]["work_id"])
        self.public_work_ids: Set[str] = {
            wid for wid in referenced_work_ids
            if self.works_by_id.get(wid, {}).get("identity_visibility") == visibility.VISIBILITY_PUBLIC
        }

        # --- discovery_identification groups, built BOTTOM-UP from
        #     surviving evidence only -- never filtered from the private
        #     build's own discovery_identification rows (Task 2 action
        #     text: "re-derived over the claims that survive the
        #     projection, never copied from the private build").
        groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for cid in self.surviving_claim_ids:
            claim = self.claims_by_id[cid]
            work = self.works_by_id.get(claim["work_id"])
            if work is None:
                continue
            canonical_work_id = work["canonical_work_id"]
            for ev in surviving_evidence_by_claim[cid]:
                key = (ev["sys_id"], canonical_work_id)
                groups.setdefault(key, []).append(ev)
        self.identification_groups = groups


# ---------------------------------------------------------------------------
# display_work_id re-selection (schema SS(B1)), restricted to PUBLIC members.
# ---------------------------------------------------------------------------

def _select_public_display_work_id(
    canonical_work_id: str, ctx: ProjectionContext
) -> Optional[str]:
    """The SS(B1) deterministic-representative rule, restricted to the
    canonical group's PUBLIC members only: (1) the canonical anchor
    (work_id == canonical_work_id) if it is itself public; (2) else the
    lowest `source_corpus` in the fixed order sefaria < ja < msource among
    the public members; (3) tie -> lexicographically smallest `work_id`.
    Returns `None` (the identification must then be DROPPED, never left
    dangling) when the canonical group has NO public member at all."""
    members = ctx.canonical_groups.get(canonical_work_id, [])
    public_members = [w for w in members if w["work_id"] in ctx.public_work_ids]
    if not public_members:
        return None
    for w in public_members:
        if w["work_id"] == canonical_work_id:
            return w["work_id"]
    ranked = sorted(
        public_members,
        key=lambda w: (
            visibility.SOURCE_CORPUS_RANK.get(w.get("source_corpus"), 99),
            w["work_id"],
        ),
    )
    return ranked[0]["work_id"]


# ---------------------------------------------------------------------------
# discovery_identification recomputation policy.
#
# NOTE (documented simplification, not the real main-pool-rule engine): the
# ACTUAL main-pool bucketing predicate
# (`.claude/skills/sketch-findings-genizahsearch/references/main-pool-rule.md`,
# owner ruling D-13c/D-13d/D-13e) is owned by plans 136-07/136-11/136-12,
# which had not yet landed a shared, importable module
# (`shared/discovery_main_pool.py`) as of this plan's execution. This
# function implements a SIMPLE, deterministic, and clearly-labeled stand-in
# so the CLOSED-GRAPH / RECOMPUTE-NOT-COPY structural properties this plan
# is actually responsible for (VIS-01) are genuinely testable. When
# `shared/discovery_main_pool.py` lands, THIS function should import and
# call it directly rather than reimplementing bucket logic -- flagged here
# for 136-11/136-12 to reconcile (see the plan's own SUMMARY.md).
# ---------------------------------------------------------------------------

def _recompute_identification_row(
    sys_id: str, canonical_work_id: str, group_evidence: List[Dict[str, Any]], ctx: ProjectionContext
) -> Optional[Dict[str, Any]]:
    display_work_id = _select_public_display_work_id(canonical_work_id, ctx)
    if display_work_id is None:
        return None  # no public representative -- drop, never dangling

    page_count = len({ev["a_page_id"] for ev in group_evidence})
    band_ranks = [ev["band_rank"] for ev in group_evidence if ev.get("band_rank") is not None]
    # A sentinel "unranked" fallback for the (should-not-happen-in-practice)
    # case a surviving evidence row carries no materialized band_rank at all
    # -- every real asset row does (Amendment A), this only guards synthetic
    # fixtures from raising.
    _UNRANKED_BAND_FALLBACK = 99
    best_band_rank = min(band_ranks) if band_ranks else _UNRANKED_BAND_FALLBACK
    coverage_values = [ev["coverage_ppm"] for ev in group_evidence if ev.get("coverage_ppm") is not None]
    max_coverage_ppm = max(coverage_values) if coverage_values else None

    winning_evidence_id = ids.select_display_evidence(group_evidence)
    winning_evidence = next(ev for ev in group_evidence if ev["evidence_id"] == winning_evidence_id)
    winning_claim = ctx.claims_by_id[winning_evidence["claim_id"]]

    if any(ev.get("adjudication_status") == ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED for ev in group_evidence):
        main_pool, main_pool_reason = True, "main_human_confirmed"
    elif page_count >= 2:
        main_pool, main_pool_reason = True, "main_multifolio"
    elif best_band_rank == 1:
        main_pool, main_pool_reason = True, "main_full_coverage"
    else:
        main_pool, main_pool_reason = False, "insufficient_length"

    # Frozen recipe (docs/specs/discovery-sidecar-schema-v1.md SS(B)): SHA-256
    # over "discovery_identification_v1|{sys_id}|{canonical_work_id}". No
    # `identification_id()` helper exists yet in `scripts/discovery_ids.py`
    # (the table is new in this Phase-136 amendment) -- implemented here
    # verbatim per the frozen recipe rather than duplicating a competing one.
    ident_id = hashlib.sha256(
        f"discovery_identification_v1|{sys_id}|{canonical_work_id}".encode("utf-8")
    ).hexdigest()

    return {
        "identification_id": ident_id,
        "sys_id": sys_id,
        "canonical_work_id": canonical_work_id,
        "display_work_id": display_work_id,
        "main_pool": 1 if main_pool else 0,
        "main_pool_reason": main_pool_reason,
        "best_band_rank": best_band_rank,
        "page_count": page_count,
        "max_coverage_ppm": max_coverage_ppm,
        "relation_kind": winning_claim.get("claim_type"),
        "novelty_status": winning_evidence.get("novelty_status") or "not_checked",
        "divergence_correctness": winning_evidence.get("divergence_correctness"),
        "assertion_visibility": visibility.VISIBILITY_PUBLIC,
        "identity_visibility": visibility.VISIBILITY_PUBLIC,
    }


# ---------------------------------------------------------------------------
# Per-table projection rules. EVERY table in the private asset MUST have an
# entry here -- an unrecognized table is a build error (see `project()`).
# ---------------------------------------------------------------------------

def _project_works(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    return [ctx.works_by_id[wid] for wid in sorted(ctx.public_work_ids)]


def _project_discovery_claim(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    out = []
    for cid in sorted(ctx.surviving_claim_ids):
        row = dict(ctx.claims_by_id[cid])
        row["display_evidence_id"] = ctx.recomputed_display_evidence[cid]
        out.append(row)
    return out


def _project_discovery_evidence(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    out = []
    for rows in ctx.surviving_evidence_by_claim.values():
        out.extend(rows)
    return out


def _project_witness_units(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    all_members = _rows_as_dicts(ctx.conn, "witness_unit_members")
    members_by_unit: Dict[str, List[Dict[str, Any]]] = {}
    for m in all_members:
        members_by_unit.setdefault(m["unit_id"], []).append(m)
    kept_units = {
        uid for uid, members in members_by_unit.items()
        if any(m["sys_id"] in ctx.reachable_sys_ids for m in members)
    }
    return [
        u for u in _rows_as_dicts(ctx.conn, "witness_units") if u["unit_id"] in kept_units
    ]


def _project_witness_unit_members(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    kept_units = {u["unit_id"] for u in _project_witness_units(ctx)}
    return [
        m for m in _rows_as_dicts(ctx.conn, "witness_unit_members")
        if m["unit_id"] in kept_units
    ]


def _project_manuscript_display(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    if "manuscript_display" not in _table_names(ctx.conn):
        return []
    return [
        row for row in _rows_as_dicts(ctx.conn, "manuscript_display")
        if row["sys_id"] in ctx.reachable_sys_ids
    ]


def _project_discovery_routing_audit(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    if "discovery_routing_audit" not in _table_names(ctx.conn):
        return []
    out = []
    for row in _rows_as_dicts(ctx.conn, "discovery_routing_audit"):
        kept = row.get("kept_work_id")
        demoted = row.get("demoted_work_id")
        if kept is not None and kept not in ctx.public_work_ids:
            continue
        if demoted is not None and demoted not in ctx.public_work_ids:
            continue
        out.append(row)
    return out


def _project_band_precision(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    # Explicit rule: copy verbatim. `band_precision` rows are EXTERNAL,
    # pre-registered certification measurements (a held-out sample draw
    # over a whole collection/band), never a per-asset row aggregate of
    # THIS build -- so "recompute, don't copy" (which governs COUNTS of
    # projected rows) does not apply here. Stated explicitly so a future
    # table added to this schema is never silently assumed to share this
    # same pass-through rule without its own citation.
    if "band_precision" not in _table_names(ctx.conn):
        return []
    return _rows_as_dicts(ctx.conn, "band_precision")


def _project_discovery_identification(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    out = []
    for (sys_id, canonical_work_id), group_evidence in ctx.identification_groups.items():
        row = _recompute_identification_row(sys_id, canonical_work_id, group_evidence, ctx)
        if row is not None:
            out.append(row)
    return out


def _project_meta(ctx: ProjectionContext, projected_counts: Dict[str, int]) -> List[Dict[str, Any]]:
    private_meta = {row["key"]: row["value"] for row in _rows_as_dicts(ctx.conn, "meta")}
    out_meta = dict(private_meta)
    out_meta["audience"] = "public"
    # Recompute -- never copy -- every release-contract row-count key so a
    # reader can never infer how many private rows were removed from a
    # count that quietly stayed at the private total.
    _COUNT_KEY_BY_TABLE = {
        "discovery_claim": "expected_rows_claims",
        "discovery_evidence": "expected_rows_evidence",
        "works": "expected_rows_works",
        "witness_units": "expected_rows_units",
        "discovery_identification": "expected_rows_discovery_identification",
        "manuscript_display": "expected_rows_manuscript_display",
    }
    for table, meta_key in _COUNT_KEY_BY_TABLE.items():
        if meta_key in out_meta or table in projected_counts:
            out_meta[meta_key] = str(projected_counts.get(table, 0))
    return [{"key": k, "value": v} for k, v in out_meta.items()]


PROJECTION_RULES: Dict[str, Callable] = {
    "works": _project_works,
    "discovery_claim": _project_discovery_claim,
    "discovery_evidence": _project_discovery_evidence,
    "witness_units": _project_witness_units,
    "witness_unit_members": _project_witness_unit_members,
    "manuscript_display": _project_manuscript_display,
    "discovery_routing_audit": _project_discovery_routing_audit,
    "band_precision": _project_band_precision,
    "discovery_identification": _project_discovery_identification,
    # `meta` is handled specially (needs the OTHER tables' projected counts
    # first) -- see `project()`. Listed here so the table-inventory check
    # below treats it as covered rather than unprojected.
    "meta": None,
}


# ---------------------------------------------------------------------------
# FK closure + release-contract count checks over the OUTPUT artifact.
# ---------------------------------------------------------------------------

def check_fk_closure(conn: sqlite3.Connection) -> List[str]:
    """Independent, callable-alone FK-closure check over an EMITTED public
    artifact -- deliberately separate from `project()` so a test can hand-craft
    a broken artifact (e.g. delete a referenced work) and assert this reports
    the violation directly (the orphan control)."""
    violations: List[str] = []
    cur = conn.cursor()

    work_ids = {r[0] for r in cur.execute("SELECT work_id FROM works").fetchall()}
    claim_rows = cur.execute("SELECT claim_id, work_id, display_evidence_id FROM discovery_claim").fetchall()
    claim_ids = {r[0] for r in claim_rows}
    for claim_id, work_id, display_evidence_id in claim_rows:
        if work_id not in work_ids:
            violations.append(f"claim {claim_id}: work_id {work_id!r} not in projected works (dangling FK)")

    evidence_rows = cur.execute("SELECT evidence_id, claim_id FROM discovery_evidence").fetchall()
    evidence_by_claim: Dict[str, Set[str]] = {}
    for evidence_id, claim_id in evidence_rows:
        evidence_by_claim.setdefault(claim_id, set()).add(evidence_id)
        if claim_id not in claim_ids:
            violations.append(f"evidence {evidence_id}: claim_id {claim_id!r} not in projected claims (dangling FK)")

    for claim_id, work_id, display_evidence_id in claim_rows:
        if display_evidence_id not in evidence_by_claim.get(claim_id, set()):
            violations.append(
                f"claim {claim_id}: display_evidence_id {display_evidence_id!r} does not own that claim "
                "(stale/dangling display pointer)"
            )

    referenced_work_ids = {work_id for _, work_id, _ in claim_rows}
    for wid in work_ids - referenced_work_ids:
        violations.append(f"work {wid}: unreachable -- no surviving claim references it")

    if _table_exists(conn, "discovery_identification"):
        for row in cur.execute("SELECT identification_id, display_work_id FROM discovery_identification").fetchall():
            ident_id, display_work_id = row
            if display_work_id not in work_ids:
                violations.append(
                    f"identification {ident_id}: display_work_id {display_work_id!r} not in "
                    "projected works (dangling FK)"
                )

    if _table_exists(conn, "witness_unit_members"):
        unit_ids = {r[0] for r in cur.execute("SELECT unit_id FROM witness_units").fetchall()}
        for row in cur.execute("SELECT unit_id, sys_id FROM witness_unit_members").fetchall():
            unit_id, sys_id = row
            if unit_id not in unit_ids:
                violations.append(f"witness_unit_member {sys_id}: unit_id {unit_id!r} not in projected witness_units")

    if _table_exists(conn, "discovery_routing_audit"):
        for row in cur.execute(
            "SELECT id, kept_work_id, demoted_work_id FROM discovery_routing_audit"
        ).fetchall():
            audit_id, kept_work_id, demoted_work_id = row
            if kept_work_id is not None and kept_work_id not in work_ids:
                violations.append(
                    f"routing_audit {audit_id}: kept_work_id {kept_work_id!r} not in projected works"
                )
            if demoted_work_id is not None and demoted_work_id not in work_ids:
                violations.append(
                    f"routing_audit {audit_id}: demoted_work_id {demoted_work_id!r} not in projected works"
                )

    return violations


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def check_meta_counts(conn: sqlite3.Connection) -> List[str]:
    """Independent, callable-alone check that every `meta.expected_rows_*`
    key equals the ACTUAL row count of its table in THIS artifact --
    deliberately separate from `project()` so a test can hand-craft an
    artifact with a stale (un-recomputed) total and assert this reports it
    (the copied-total control)."""
    violations: List[str] = []
    meta = {r[0]: r[1] for r in conn.execute("SELECT key, value FROM meta").fetchall()}
    table_by_key = {
        "expected_rows_claims": "discovery_claim",
        "expected_rows_evidence": "discovery_evidence",
        "expected_rows_works": "works",
        "expected_rows_units": "witness_units",
        "expected_rows_discovery_identification": "discovery_identification",
        "expected_rows_manuscript_display": "manuscript_display",
    }
    for key, table in table_by_key.items():
        if key not in meta:
            continue
        if not _table_exists(conn, table):
            continue
        actual = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        try:
            expected = int(meta[key])
        except (TypeError, ValueError):
            violations.append(f"meta.{key} = {meta[key]!r} is not an integer")
            continue
        if expected != actual:
            violations.append(
                f"meta.{key} = {expected} != actual COUNT(*) FROM {table} = {actual} "
                "(stored total was not recomputed over the projected rows)"
            )
    return violations


def check_meta_audience(conn: sqlite3.Connection, expected: str) -> List[str]:
    meta = {r[0]: r[1] for r in conn.execute("SELECT key, value FROM meta").fetchall()}
    actual = meta.get("audience")
    if actual != expected:
        return [f"meta.audience = {actual!r}, expected {expected!r}"]
    return []


# ---------------------------------------------------------------------------
# The masking gate -- the projection's own final control. BOTH surfaces are
# required (never just one); see the module docstring for why.
# ---------------------------------------------------------------------------

def _masking_gate_argv(db_path: str) -> List[str]:
    """The LITERAL required production invocation (this plan's Task 2 action
    text): BOTH `--scan-asset` (physical bytes) and `--scan-sqlite` (logical
    cell content) are required together for a SQLite artifact, under
    `--strict --scan-repo` -- never just one surface."""
    return ["--strict", "--scan-repo", "--scan-asset", str(db_path), "--scan-sqlite", str(db_path)]


def run_masking_gate(
    db_path: str, *, patterns: Optional[List[str]] = None
) -> Tuple[bool, int]:
    """Run the projection's own final masking gate over the emitted
    artifact. Returns `(passed, issue_count)`.

    Two modes:

    - `patterns=None` (the PRODUCTION default): replays the literal required
      CLI invocation (`_masking_gate_argv` -- `--strict --scan-repo
      --scan-asset <db> --scan-sqlite <db>`) via `check_atlas_masking.main`,
      loading patterns from `MASKING_SCAN_PATTERNS_FILE` per the standing
      dev/CI convention (fails closed, exit 1, if that env var is unset --
      see the module's own fail-safe design). The per-issue count is not
      separately observable through `main()`'s int-only return in this mode
      (`issue_count` is `-1`, a sentinel meaning "not counted").
    - `patterns=<explicit list>` (test-only): calls `scan_asset` (physical
      bytes, `strict=True`) and `scan_sqlite` (logical cells, BOTH schema
      and content surfaces) directly with the SUPPLIED pattern set, bypassing
      `MASKING_SCAN_PATTERNS_FILE` and `--scan-repo`'s expensive whole-repo
      walk -- mirroring the SAME test-injectable-patterns shape
      `scripts/build_discovery_sidecar.py::finalize_build(masking_patterns=...)`
      already established, so a unit test can seed a small, disposable
      marker instead of depending on the gitignored dev secrets file or a
      slow full-tree scan.
    """
    if patterns is not None:
        validated = masking._require_patterns(patterns)
        issues = (
            masking.scan_asset(db_path, validated, strict=True)
            + masking.scan_sqlite(db_path, validated)
        )
        return (len(issues) == 0, len(issues))
    rc = masking.main(_masking_gate_argv(db_path))
    return (rc == 0, -1)


# ---------------------------------------------------------------------------
# Launch-scope reconciliation over the PRIVATE (already-built) asset, using
# its STORED axes (assertion_visibility/identity_visibility) rather than raw
# build-time corpus origins (which no longer exist post-build). This is a
# parallel REPORTING routine to `shared.discovery_visibility.reconcile_launch_scope`
# (that function is build-time-only, consuming raw `assertion_source_corpus`
# before it is discarded) -- both report the SAME symmetric-difference shape
# and both call `is_public` as the ONE conjunction rule; neither restates it.
# ---------------------------------------------------------------------------

def _vis01_shortcut(evidence_source: Optional[str], source_corpus: Optional[str]) -> bool:
    if evidence_source == ids.EVIDENCE_SOURCE_PROPAGATED:
        return True
    if evidence_source == ids.EVIDENCE_SOURCE_TRACK1_DIRECT:
        return source_corpus == ids.SOURCE_CORPUS_SEFARIA
    return False


def compute_launch_scope_reconciliation(conn: sqlite3.Connection) -> Dict[str, Any]:
    total = 0
    vis01_count = 0
    conjunction_count = 0
    sym_diff = 0
    by_corpus_family: Dict[Tuple[Any, Any], int] = {}

    rows = conn.execute(
        "SELECT e.evidence_source, c.source_corpus AS claim_corpus, "
        "e.assertion_visibility, w.identity_visibility "
        "FROM discovery_evidence e "
        "JOIN discovery_claim c ON c.claim_id = e.claim_id "
        "JOIN works w ON w.work_id = c.work_id"
    ).fetchall()
    for evidence_source, claim_corpus, assertion_vis, identity_vis in rows:
        total += 1
        vis01_included = _vis01_shortcut(evidence_source, claim_corpus)
        conjunction_included = visibility.is_public(assertion_vis, identity_vis)
        if vis01_included:
            vis01_count += 1
        if conjunction_included:
            conjunction_count += 1
        if vis01_included != conjunction_included:
            sym_diff += 1
            key = (claim_corpus, evidence_source)
            by_corpus_family[key] = by_corpus_family.get(key, 0) + 1

    return {
        "total_rows": total,
        "vis01_launch_scope_count": vis01_count,
        "conjunction_count": conjunction_count,
        "symmetric_difference_count": sym_diff,
        "symmetric_difference_by_corpus_family": {
            f"{k[0]}|{k[1]}": v for k, v in by_corpus_family.items()
        },
    }


# ---------------------------------------------------------------------------
# Orchestration.
# ---------------------------------------------------------------------------

def project(
    private_db_path: str,
    public_db_out_path: str,
    *,
    masking_patterns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build the public projection at `public_db_out_path` from
    `private_db_path`. Returns the reconciliation report dict (also written
    to `<public_db_out_path>.reconciliation.json`). Raises `ProjectionError`
    on any build error (unprojected table, unsafe output path, masking-gate
    failure) -- on a masking-gate failure the emitted artifact is REMOVED
    rather than left on disk half-clean.

    `masking_patterns`, when given explicitly (test-only), is passed through
    to `run_masking_gate` verbatim -- see that function's docstring."""
    _assert_output_path_safe(public_db_out_path)

    private_conn = _connect_ro(private_db_path)
    try:
        table_names = _table_names(private_conn)
        unknown = sorted(t for t in table_names if t not in PROJECTION_RULES)
        if unknown:
            raise ProjectionError(
                f"no projection rule for table(s): {unknown} -- a table with no explicit "
                "projection rule is a build error, never a silent whole-table copy"
            )

        ctx = ProjectionContext(private_conn)

        out_path = Path(public_db_out_path)
        if out_path.exists():
            out_path.unlink()
        out_path.parent.mkdir(parents=True, exist_ok=True)

        out_conn = sqlite3.connect(str(out_path))
        try:
            out_conn.execute("PRAGMA foreign_keys=OFF")  # during bulk load; re-enabled for checks
            for stmt in _schema_ddl(private_conn, [t for t in table_names if t != "meta"] + ["meta"]):
                out_conn.execute(stmt)

            projected_rows: Dict[str, List[Dict[str, Any]]] = {}
            for table in sorted(t for t in table_names if t != "meta"):
                rule = PROJECTION_RULES[table]
                rows = rule(ctx)
                projected_rows[table] = rows
                _insert_rows(out_conn, table, rows)

            projected_counts = {t: len(rows) for t, rows in projected_rows.items()}
            meta_rows = _project_meta(ctx, projected_counts)
            _insert_rows(out_conn, "meta", meta_rows)
            out_conn.commit()

            private_counts = {
                t: private_conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                for t in table_names
            }

            out_conn.execute("PRAGMA foreign_keys=ON")
            violations = check_fk_closure(out_conn)
            violations += check_meta_counts(out_conn)
            violations += check_meta_audience(out_conn, "public")
            if violations:
                out_conn.close()
                out_path.unlink(missing_ok=True)
                raise ProjectionError(
                    "public projection failed its own post-build invariant checks: "
                    + "; ".join(violations)
                )
        finally:
            try:
                out_conn.close()
            except Exception:
                pass

        gate_passed, gate_issue_count = run_masking_gate(str(out_path), patterns=masking_patterns)
        if not gate_passed:
            out_path.unlink(missing_ok=True)
            raise ProjectionError(
                f"public projection's own masking gate failed (issue_count={gate_issue_count}) -- "
                "artifact removed rather than left on disk"
            )

        reconciliation = {
            "per_table": {
                t: {
                    "private_count": private_counts.get(t, 0),
                    "public_count": projected_counts.get(t, 0),
                    "delta": private_counts.get(t, 0) - projected_counts.get(t, 0),
                }
                for t in sorted(table_names)
            },
            "launch_scope_reconciliation": compute_launch_scope_reconciliation(private_conn),
        }
        report_path = str(out_path) + ".reconciliation.json"
        with open(report_path, "w", encoding="utf-8") as fh:
            json.dump(reconciliation, fh, indent=2, sort_keys=True)
        reconciliation["report_path"] = report_path
        return reconciliation
    finally:
        private_conn.close()


def _insert_rows(conn: sqlite3.Connection, table: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ",".join("?" for _ in columns)
    col_list = ",".join(columns)
    conn.executemany(
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
        [tuple(row.get(c) for c in columns) for row in rows],
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("private_db", help="Path to the PRIVATE discovery.db sidecar (input)")
    parser.add_argument("public_db_out", help="Path to write the PUBLIC projection to (output)")
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        report = project(args.private_db, args.public_db_out)
    except ProjectionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"project_discovery_public: wrote {args.public_db_out}")
    print(f"project_discovery_public: reconciliation report at {report.get('report_path')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
