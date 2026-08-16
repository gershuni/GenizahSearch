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

`is_public` (`shared/discovery_visibility.py`) remains the row-level
visibility rule. A separate, versioned owner-curated exclusion policy may
withhold otherwise-public canonical works while retaining them in the private
research bake as routing competitors. Every table present in the private input
MUST have an explicit projection rule in
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
import json
import os
import re
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


# The routing reason whose meaning DEPENDS on a surviving audit row (see the
# dependency-closure block in ProjectionContext).
_LATER_SHARED_TEXT = "later_shared_text"

# Dependency closure iterates because pruning evidence can make a work
# unreachable, which can drop a further audit row, which can orphan further
# evidence. A bound that is never reached in practice, so a non-converging
# graph raises instead of looping forever.
_MAX_CLOSURE_PASSES = 25

_DEFAULT_PUBLIC_EXCLUSIONS_PATH = os.path.join(
    _REPO_ROOT, "docs", "specs", "discovery-public-projection-exclusions-v1.json"
)
_PUBLIC_EXCLUSIONS_SCHEMA = "discovery-public-projection-exclusions-v1"
_OPAQUE_WORK_ID_RE = re.compile(r"^w[0-9]{6}$")

# G9: claim types that ASSERT a witness relation and therefore require at least
# one witness-kind evidence row to survive with them.
_WITNESS_ASSERTING_CLAIM_TYPES = (
    ids.CLAIM_TYPE_DIRECT_WITNESS,
    ids.CLAIM_TYPE_QUOTES_THIS_WORK,
)


def load_public_projection_exclusions(path: str) -> Tuple[str, Set[str]]:
    """Load the owner-curated canonical-work exclusion policy.

    The policy is projection-only: excluded works remain in the private asset
    and continue to participate in routing and competition. The public build
    drops their evidence first, then closes and recomputes the graph normally.
    """
    with open(path, encoding="utf-8") as fh:
        payload = json.load(fh)
    expected_top_keys = {
        "schema_version", "policy_version", "ruled_date", "ruling", "entries"
    }
    if not isinstance(payload, dict) or set(payload) != expected_top_keys:
        raise ProjectionError(
            "public projection exclusions must use the exact versioned policy shape"
        )
    if payload["schema_version"] != _PUBLIC_EXCLUSIONS_SCHEMA:
        raise ProjectionError("unsupported public projection exclusions schema_version")
    policy_version = payload["policy_version"]
    if not isinstance(policy_version, str) or not policy_version:
        raise ProjectionError("public projection exclusions policy_version must be non-empty")
    entries = payload["entries"]
    if not isinstance(entries, list):
        raise ProjectionError("public projection exclusions entries must be a list")
    work_ids: Set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {
            "canonical_work_id", "title", "reason"
        }:
            raise ProjectionError(
                "every public projection exclusion must have canonical_work_id, title, reason"
            )
        work_id = entry["canonical_work_id"]
        if not isinstance(work_id, str) or not _OPAQUE_WORK_ID_RE.fullmatch(work_id):
            raise ProjectionError(
                "public projection exclusion canonical_work_id must be an opaque work id"
            )
        if work_id in work_ids:
            raise ProjectionError("public projection exclusions contain a duplicate work id")
        if not isinstance(entry["title"], str) or not entry["title"]:
            raise ProjectionError("public projection exclusion title must be non-empty")
        if not isinstance(entry["reason"], str) or not entry["reason"]:
            raise ProjectionError("public projection exclusion reason must be non-empty")
        work_ids.add(work_id)
    return policy_version, work_ids


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

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        exclusion_policy_version: str = "test-no-public-exclusions",
        excluded_canonical_work_ids: Set[str] = frozenset(),
    ):
        self.conn = conn
        self.exclusion_policy_version = exclusion_policy_version
        self.excluded_canonical_work_ids = frozenset(excluded_canonical_work_ids)
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
        present_canonical_ids = {
            work["canonical_work_id"] for work in self.works_by_id.values()
        }
        self.present_excluded_canonical_work_ids = (
            self.excluded_canonical_work_ids & present_canonical_ids
        )
        self.policy_excluded_claim_ids = {
            claim_id for claim_id, claim in self.claims_by_id.items()
            if self.works_by_id.get(claim["work_id"], {}).get("canonical_work_id")
            in self.excluded_canonical_work_ids
        }
        self.policy_excluded_evidence_ids = {
            ev["evidence_id"] for ev in all_evidence
            if ev["claim_id"] in self.policy_excluded_claim_ids
        }

        # --- Evidence survival (the D-22 conjunction, per row) -----------
        surviving_evidence_by_claim: Dict[str, List[Dict[str, Any]]] = {}
        surviving_evidence_ids: Set[str] = set()
        reachable_sys_ids: Set[str] = set()
        for ev in all_evidence:
            claim = self.claims_by_id.get(ev["claim_id"])
            if claim is None:
                continue  # orphan evidence in the PRIVATE asset -- not this script's concern
            work = self.works_by_id.get(claim["work_id"])
            if (
                work
                and work.get("canonical_work_id") in self.excluded_canonical_work_ids
            ):
                continue
            identity_vis = work.get("identity_visibility") if work else None
            assertion_vis = ev.get("assertion_visibility")
            if visibility.is_public(assertion_vis, identity_vis):
                surviving_evidence_by_claim.setdefault(ev["claim_id"], []).append(ev)
                surviving_evidence_ids.add(ev["evidence_id"])
                reachable_sys_ids.add(ev["sys_id"])
        # --- Dependency-closure pruning (2026-08-03, 136-13 gate 5) --------
        #
        # `routing_reason='later_shared_text'` ASSERTS that an earlier work beat
        # this one on this page, and `check_unknown_date_never_demoted` requires
        # a backing `demoted` audit row with both years non-NULL. But an audit
        # row naming a non-public work is dropped by
        # `_project_discovery_routing_audit` -- correctly, since publishing it
        # would disclose a restricted work's identity. Measured on the first real
        # run: 680 demoted audit rows dropped (486 naming a non-public
        # demoted_work_id), orphaning 164 surviving evidence rows.
        #
        # Owner ruling (2026-08-03): DROP the citing evidence. An evidence row
        # whose stated routing reason cannot be substantiated in the artifact
        # that carries it is asserting a fact its own provenance cannot back --
        # and redaction/surrogate ids would still disclose that a hidden
        # competitor exists.
        #
        # This runs as part of SURVIVAL, not as a post-hoc delete, because
        # `public_work_ids` depends on surviving claims which depends on
        # surviving evidence: pruning can make a further work unreachable, whose
        # loss can drop further audit rows. Iterate to a fixed point.
        audit_rows = (
            _rows_as_dicts(conn, "discovery_routing_audit")
            if "discovery_routing_audit" in _table_names(conn) else []
        )
        self.pruned_unreplayable_evidence_ids: Set[str] = set()
        self.pruned_g9_cascade_evidence_ids: Set[str] = set()
        for _ in range(_MAX_CLOSURE_PASSES):
            surviving_claim_ids = {cid for cid, rows in surviving_evidence_by_claim.items() if rows}
            public_work_ids = {
                self.claims_by_id[cid]["work_id"] for cid in surviving_claim_ids
            }
            public_work_ids = {
                wid for wid in public_work_ids
                if self.works_by_id.get(wid, {}).get("identity_visibility")
                == visibility.VISIBILITY_PUBLIC
            }
            # (page, demoted work) pairs that will still carry a replayable
            # demotion after projection.
            #
            # Keyed by BOTH, never by page alone. A page-only key lets an
            # unrelated but publishable demotion on the same page vouch for
            # evidence whose OWN backing demotion was dropped for naming a
            # private work -- the evidence survives while the audit row that
            # explains it does not, which is precisely the unreplayable state
            # this prune exists to prevent (Codex code review 2026-08-03,
            # finding 2). 65 pages in the deployed public artifact carry more
            # than one demotion, so the collision is reachable; it happens to be
            # unrealised there today (0 mismatches measured), which is why the
            # deployed bytes are unaffected by this correction.
            #
            # `demoted_work_id` is compared against the claim work's CANONICAL
            # id, because audit rows carry canonical ids. A NULL demoted_work_id
            # substantiates nothing and is therefore excluded -- fail closed.
            replayable_page_works = {
                (r["page_id"], r["demoted_work_id"]) for r in audit_rows
                if r.get("decision") == "demoted"
                and r.get("kept_year") is not None and r.get("demoted_year") is not None
                and r.get("demoted_work_id") is not None
                and r["demoted_work_id"] in public_work_ids
                and (r.get("kept_work_id") is None or r["kept_work_id"] in public_work_ids)
            }
            doomed: Set[str] = set()
            for cid, rows in surviving_evidence_by_claim.items():
                claim = self.claims_by_id[cid]
                page_id = claim["page_id"]
                canonical = self.works_by_id.get(claim["work_id"], {}).get(
                    "canonical_work_id")
                for ev in rows:
                    if (ev.get("routing_reason") == _LATER_SHARED_TEXT
                            and (page_id, canonical) not in replayable_page_works):
                        doomed.add(ev["evidence_id"])
            self.pruned_unreplayable_evidence_ids |= doomed

            # G9 cascade: a claim whose type ASSERTS a witness relation must keep
            # at least one witness-kind evidence row. Pruning above can strip a
            # claim's ONLY witness row while leaving non-witness rows behind --
            # the claim would then survive asserting a relation nothing in the
            # artifact supports. Measured on the first real run: 54 such claims
            # (31 direct_witness + 23 quotes_this_work). Drop the whole claim.
            for cid, rows in surviving_evidence_by_claim.items():
                claim_type = self.claims_by_id[cid].get("claim_type")
                if claim_type not in _WITNESS_ASSERTING_CLAIM_TYPES:
                    continue
                kept = [e for e in rows if e["evidence_id"] not in doomed]
                if kept and not any(
                    e.get("evidence_kind") == ids.EVIDENCE_KIND_WITNESS for e in kept
                ):
                    cascade = {e["evidence_id"] for e in kept}
                    doomed |= cascade
                    self.pruned_g9_cascade_evidence_ids |= cascade

            if not doomed:
                break
            for cid in list(surviving_evidence_by_claim):
                kept = [e for e in surviving_evidence_by_claim[cid]
                        if e["evidence_id"] not in doomed]
                if kept:
                    surviving_evidence_by_claim[cid] = kept
                else:
                    del surviving_evidence_by_claim[cid]
            surviving_evidence_ids -= doomed
        else:
            raise ProjectionError(
                f"dependency closure did not converge in {_MAX_CLOSURE_PASSES} passes -- "
                "refusing to emit a public artifact whose graph may still be open"
            )
        reachable_sys_ids = {
            ev["sys_id"] for rows in surviving_evidence_by_claim.values() for ev in rows
        }

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
            and self.works_by_id.get(wid, {}).get("canonical_work_id")
            not in self.excluded_canonical_work_ids
        }



# ---------------------------------------------------------------------------
# discovery_identification is NOT recomputed here.
#
# Until 2026-08-03 this module carried its own `_select_public_display_work_id`
# (an SS(B1) re-implementation) and `_recompute_identification_row` (a
# deliberately "simplified, clearly-labeled stand-in" for the main-pool
# bucketing predicate, written when `shared/discovery_main_pool.py` did not yet
# exist). Both are DELETED: the projection now calls the production
# materializer, `build_discovery_sidecar.populate_discovery_identification`,
# against the populated public artifact. See `_materialize_public_identification`.
#
# The stand-in was not merely redundant -- it derived the grain over a different
# evidence population than the private builder (no D-13g eligibility rule),
# which is how a public artifact came to hold 95,149 identification rows against
# a 64,522-row private superset. A second implementation of a shared rule is the
# hazard, not the fix.
# ---------------------------------------------------------------------------

def _project_works(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    return [ctx.works_by_id[wid] for wid in sorted(ctx.public_work_ids)]


def _project_discovery_claim(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    out = []
    for cid in sorted(ctx.surviving_claim_ids):
        row = dict(ctx.claims_by_id[cid])
        row["display_evidence_id"] = ctx.recomputed_display_evidence[cid]
        # Locus labels are projections of evidence, not private annotations.
        # Fail closed during the copy and recompute after every public-only
        # input table has been populated.
        if "locus_status" in row:
            row["locus_status"] = "unavailable"
            row["locus_work_id"] = None
            row["locus_label"] = None
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


def _project_locus_work(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    """CD batch / schema Amendment 2026-08-12 (N): a locus row follows its
    work -- rows for pruned (e.g. M-source-identity) works never ship. The
    table-presence guards on the rules below are defensive style (mirroring
    _project_manuscript_display), NOT a pre-batch compatibility promise: a
    pre-batch private input already fails loudly one step later, when the
    identification materializer's INSERT names the amendment's two new
    columns. Projection input is a post-batch private asset by contract."""
    if "locus_work" not in _table_names(ctx.conn):
        return []
    return [
        row for row in _rows_as_dicts(ctx.conn, "locus_work")
        if row["work_id"] in ctx.public_work_ids
    ]


def _kept_locus_work_ids(ctx: ProjectionContext) -> Set[str]:
    return {row["work_id"] for row in _project_locus_work(ctx)}


def _project_locus_unit(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    if "locus_unit" not in _table_names(ctx.conn):
        return []
    kept = _kept_locus_work_ids(ctx)
    return [
        row for row in _rows_as_dicts(ctx.conn, "locus_unit")
        if row["work_id"] in kept
    ]


def _project_locus_edition(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    if "locus_edition" not in _table_names(ctx.conn):
        return []
    kept = _kept_locus_work_ids(ctx)
    return [
        row for row in _rows_as_dicts(ctx.conn, "locus_edition")
        if row["work_id"] in kept
    ]


def _project_discovery_locus_piece(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    """Derived from public-only evidence after identification materialization."""
    return []


def _project_discovery_region_map(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    """Amendment 2026-08-12 (R): region rows follow their locus_work row."""
    if "discovery_region_map" not in _table_names(ctx.conn):
        return []
    kept = _kept_locus_work_ids(ctx)
    return [
        row for row in _rows_as_dicts(ctx.conn, "discovery_region_map")
        if row["work_id"] in kept
    ]


def _project_discovery_curated_quoter(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    """Explicit rule: copy verbatim (Amendment 2026-08-12 (R)). The curated
    quoter list is versioned, owner-ruled CONFIG -- opaque canonical ids +
    dates only, never per-asset row aggregates -- so the band_precision
    pass-through rationale applies, with its own citation as that rule's
    comment demands."""
    if "discovery_curated_quoter" not in _table_names(ctx.conn):
        return []
    return _rows_as_dicts(ctx.conn, "discovery_curated_quoter")


def _project_discovery_region_band(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    """Amendment 2026-08-13 (V): bands follow their `works` row, like the region
    map follows its `locus_work` row — NOT verbatim like the curated list.

    The difference is the key. The curated list is canonical, work-level config
    whose rows stay meaningful (if inert) after pruning. A band is a pair of
    offsets into ONE work's stream, keyed on `work_id`; carried past its work it
    is a dangling coordinate nothing can resolve, and it would still be counted
    by the release-contract count key, which is a side channel about the private
    population on a public asset."""
    if "discovery_region_band" not in _table_names(ctx.conn):
        return []
    return [
        row for row in _rows_as_dicts(ctx.conn, "discovery_region_band")
        if row["work_id"] in ctx.public_work_ids
    ]


def _project_discovery_withholding(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    """Explicit rule: copy verbatim (Amendment 2026-08-12 (Q)). Withholding
    scopes are versioned control-plane config the RUNTIME compiles against
    the public rows; carrying a predicate whose stratum was pruned is inert
    (it matches nothing), and the frame-time bijection gates own consistency."""
    if "discovery_withholding" not in _table_names(ctx.conn):
        return []
    return _rows_as_dicts(ctx.conn, "discovery_withholding")


def _project_discovery_stratum_membership(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    """Emits NOTHING here by design (Amendment 2026-08-12 (Q)) -- membership
    rows reference discovery_identification, which is itself materialized
    AFTER the base tables (see _project_discovery_identification below). The
    filtered copy runs in _materialize_public_stratum_membership; returning []
    keeps the table under the every-table-needs-a-rule guard."""
    return []


def _materialize_public_stratum_membership(
    private_conn: sqlite3.Connection, out_conn: sqlite3.Connection
) -> int:
    """Copy stratum-membership rows whose identification survives into the
    JUST-materialized public discovery_identification (same ordering
    dependency as the identification itself; identification_ids are stable
    sha256 keys, so surviving rows keep their ids)."""
    if "discovery_stratum_membership" not in _table_names(private_conn):
        return 0
    public_ids = {
        r[0] for r in out_conn.execute(
            "SELECT identification_id FROM discovery_identification")
    }
    rows = [
        row for row in _rows_as_dicts(private_conn, "discovery_stratum_membership")
        if row["identification_id"] in public_ids
    ]
    out_conn.execute("DELETE FROM discovery_stratum_membership")
    _insert_rows(out_conn, "discovery_stratum_membership", rows)
    return len(rows)


def _project_discovery_identification(ctx: ProjectionContext) -> List[Dict[str, Any]]:
    """Emits NOTHING here by design (2026-08-03, 136-13 gate 5).

    `discovery_identification` is materialized AFTER the base tables are
    written, by calling the private builder's own
    `populate_discovery_identification` against the PUBLIC connection -- see
    `_materialize_public_identification`. Returning [] keeps this table in
    `PROJECTION_RULES` (so the "every table needs an explicit rule" guard still
    covers it) while making the ordering dependency explicit.

    Why not filter-and-recompute here: this function used to re-derive the
    table over ALL surviving evidence, omitting the `shipped OR
    human_confirmed` eligibility rule (D-13g) that the private builder and
    `check_identification_grain` both apply. That produced 95,149 public rows
    where only 53,616 were shipped-backed -- MORE rows than the 64,522-row
    private superset it was projected from. Adding the missing filter would
    have fixed the row count while leaving `main_pool`/`main_pool_reason`
    computed by the documented stand-in below rather than the shared rule, and
    `eligibility_basis` unwritten entirely. Reusing the production materializer
    removes the whole divergence class instead of patching one symptom."""
    return []


def _materialize_public_identification(
    out_conn: sqlite3.Connection, private_conn: sqlite3.Connection
) -> int:
    """Run the PRIVATE builder's identification materializer against the public
    artifact, after its base tables are populated.

    Every input that materializer reads -- `discovery_evidence`,
    `discovery_claim`, `works`, `discovery_routing_audit`, `band_precision` --
    is already projected at this point, and each holds ONLY public rows. So the
    same code that produced the private table produces the public one from the
    public row set: the eligibility rule (D-13g), the shared main-pool decision
    (`shared.discovery_main_pool`), `eligibility_basis`, and the SS(B1)
    `display_work_id` selection all come out right by construction. The
    canonical-group index is built from the PUBLIC `works` table, so the
    representative is automatically chosen among public members only.

    C-track adds one more input to that list: `discovery_curated_quoter`, read
    by Contract 1's step 4 when the materializer recomputes `rendered_relation`
    as its last act. It too is already projected here, which is what makes the
    PUBLIC relation a function of the PUBLIC asset -- necessary, not cosmetic,
    because step 4's divergence ratio is a per-work aggregate whose denominators
    shrink with pruning. Copying the column instead would ship values that are
    true of the private row population and false of this one.

    Amendment 2026-08-13 (W): the matrix parameterization is no longer a
    constant the materializer reaches for, so this passes the PRIVATE asset's
    own -- read from its meta, which is the only authority for what an asset's
    stored relations were produced under. Recomputing the public rows under
    deploy 1 while copying a private meta that says otherwise would produce an
    artifact that fails its OWN recompute gate.

    Absent parameterization meta (a pre-C-track private asset) means deploy 1,
    which is what such an asset was in fact built under."""
    import build_discovery_sidecar as builder  # local: avoid a module-load cycle
    from shared import discovery_relation_matrix as relation_matrix

    private_meta = dict(private_conn.execute("SELECT key, value FROM meta"))
    if all(k in private_meta for k in relation_matrix.PARAMETERIZATION_META_KEYS):
        parameterization = relation_matrix.parameterization_from_meta(private_meta)
    else:
        parameterization = relation_matrix.DEPLOY_1_PARAMETERIZATION

    out_conn.execute("DELETE FROM discovery_identification")
    builder.populate_discovery_identification(out_conn, parameterization)
    if private_meta.get("locus_display_version") == builder.LOCUS_DISPLAY_VERSION:
        builder.materialize_locus_labels(out_conn)
    (n,) = out_conn.execute("SELECT COUNT(*) FROM discovery_identification").fetchone()
    return n


def check_identification_key_subset(
    private_conn: sqlite3.Connection, public_conn: sqlite3.Connection
) -> List[str]:
    """The public identification KEY SET must be a subset of the private one.

    Removing evidence cannot mint a new `(sys_id, canonical_work_id)` key, so a
    public key absent privately means the two were materialized over different
    populations -- which is exactly the defect this check was added for (gate 5,
    136-13: 95,149 public rows against a 64,522-row private superset).

    Deliberately a KEY-set check, not a row-count check: public rows may
    legitimately carry different aggregate VALUES (page counts, coverage, band
    rank) because they are computed over fewer evidence rows. Only the key set
    is constrained."""
    if not _has_identification(private_conn) or not _has_identification(public_conn):
        return []
    priv = {
        tuple(r) for r in private_conn.execute(
            "SELECT sys_id, canonical_work_id FROM discovery_identification")
    }
    pub = {
        tuple(r) for r in public_conn.execute(
            "SELECT sys_id, canonical_work_id FROM discovery_identification")
    }
    extra = pub - priv
    if extra:
        return [
            f"discovery_identification: {len(extra)} public key(s) absent from the private "
            "asset -- the two were materialized over different populations (a projection "
            "cannot mint a key its source lacks)"
        ]
    return []


def _has_identification(conn: sqlite3.Connection) -> bool:
    # Deliberately does NOT use `_table_names`, which indexes rows by name and
    # therefore requires `conn.row_factory = sqlite3.Row`. The production
    # identification materializer resets `row_factory` to None on exit, so this
    # runs against a plain-tuple connection.
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_identification'"
    ).fetchone() is not None


def _project_meta(ctx: ProjectionContext, projected_counts: Dict[str, int]) -> List[Dict[str, Any]]:
    private_meta = {row["key"]: row["value"] for row in _rows_as_dicts(ctx.conn, "meta")}
    out_meta = dict(private_meta)
    out_meta["audience"] = "public"
    out_meta["public_projection_exclusion_version"] = ctx.exclusion_policy_version
    out_meta["public_projection_excluded_canonical_work_count"] = str(
        len(ctx.present_excluded_canonical_work_ids)
    )
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
        # Amendment 2026-08-12 (U): RECOMPUTED like every count (zero is a
        # legitimate count). The lock/version/hash keys of the same amendment
        # are deliberately NOT here -- they are copied constants, and
        # recomputing them would un-lock them.
        "locus_work": "expected_rows_locus_work",
        "locus_unit": "expected_rows_locus_unit",
        "locus_edition": "expected_rows_locus_edition",
        "discovery_locus_piece": "expected_rows_discovery_locus_piece",
        "discovery_region_map": "expected_rows_discovery_region_map",
        "discovery_curated_quoter": "expected_rows_discovery_curated_quoter",
        "discovery_region_band": "expected_rows_discovery_region_band",
        "discovery_stratum_membership": "expected_rows_discovery_stratum_membership",
        "discovery_withholding": "expected_rows_discovery_withholding",
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
    # CD batch / schema Amendment 2026-08-12 -- registered WITH the DDL, in
    # the same commit set, because this registry hard-rejects unknown tables
    # (Codex pre-flight finding 4): a batch asset must project cleanly with
    # zero rows in all seven. `rendered_relation` needs no rule of its own --
    # it rides the identification rematerialization (the production
    # materializer recomputes it per asset, post-pruning, by construction).
    #
    # This dict's order is NOT the projection order: `project()` iterates the
    # tables SORTED, and sequences the order-sensitive steps explicitly
    # afterwards (`_materialize_public_identification`, then
    # `_materialize_public_stratum_membership`). That post-pass is what
    # guarantees Contract 1's input tables are already populated when the grain
    # recomputes `rendered_relation` -- reordering these keys would not.
    "locus_work": _project_locus_work,
    "locus_unit": _project_locus_unit,
    "locus_edition": _project_locus_edition,
    "discovery_locus_piece": _project_discovery_locus_piece,
    "discovery_region_map": _project_discovery_region_map,
    "discovery_curated_quoter": _project_discovery_curated_quoter,
    "discovery_region_band": _project_discovery_region_band,
    "discovery_withholding": _project_discovery_withholding,
    "discovery_stratum_membership": _project_discovery_stratum_membership,
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

    # CD batch / schema Amendment 2026-08-12: the locus family + Contract-4
    # membership close over the same graph.
    if _table_exists(conn, "locus_work"):
        locus_work_ids = set()
        for work_id, in cur.execute("SELECT work_id FROM locus_work").fetchall():
            locus_work_ids.add(work_id)
            if work_id not in work_ids:
                violations.append(
                    f"locus_work {work_id}: not in projected works (dangling FK)"
                )
        for table in ("locus_unit", "locus_edition", "discovery_region_map"):
            if not _table_exists(conn, table):
                continue
            for work_id, in cur.execute(f"SELECT DISTINCT work_id FROM {table}").fetchall():
                if work_id not in locus_work_ids:
                    violations.append(
                        f"{table}: work_id {work_id!r} not in projected locus_work (dangling FK)"
                    )

    if _table_exists(conn, "discovery_locus_piece"):
        ident_ids = {
            row[0] for row in cur.execute(
                "SELECT identification_id FROM discovery_identification"
            ).fetchall()
        }
        for ident_id, work_id in cur.execute(
            "SELECT identification_id, locus_work_id FROM discovery_locus_piece"
        ).fetchall():
            if ident_id not in ident_ids:
                violations.append(
                    f"discovery_locus_piece: identification_id {ident_id!r} is dangling"
                )
            if work_id not in locus_work_ids:
                violations.append(
                    f"discovery_locus_piece: locus_work_id {work_id!r} is dangling"
                )

    if _table_exists(conn, "discovery_stratum_membership") and _table_exists(
        conn, "discovery_identification"
    ):
        ident_ids = {
            r[0] for r in cur.execute(
                "SELECT identification_id FROM discovery_identification").fetchall()
        }
        for ident_id, in cur.execute(
            "SELECT DISTINCT identification_id FROM discovery_stratum_membership"
        ).fetchall():
            if ident_id not in ident_ids:
                violations.append(
                    f"stratum_membership: identification_id {ident_id!r} not in projected "
                    "discovery_identification (dangling FK)"
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
        # Amendment 2026-08-12 (U).
        "expected_rows_locus_work": "locus_work",
        "expected_rows_locus_unit": "locus_unit",
        "expected_rows_locus_edition": "locus_edition",
        "expected_rows_discovery_locus_piece": "discovery_locus_piece",
        "expected_rows_discovery_region_map": "discovery_region_map",
        "expected_rows_discovery_curated_quoter": "discovery_curated_quoter",
        "expected_rows_discovery_stratum_membership": "discovery_stratum_membership",
        "expected_rows_discovery_withholding": "discovery_withholding",
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
    """!! STALE AND UNSAFE AS A VISIBILITY RULE -- REPORTING INPUT ONLY. !!

    This is the SUPERSEDED VIS-01 launch-scope shortcut, retained for exactly
    one purpose: to be differenced against the real rule inside
    `compute_launch_scope_reconciliation`, so the report can state HOW the two
    disagree. It must never gate what ships.

    Why it is unsafe: the first branch returns True for EVERY `propagated`
    row regardless of `source_corpus`, so a propagated row carrying restricted
    (M-source / R-source) content would be admitted. The ONE rule that decides
    publication is the two-axis conjunction
    `shared.discovery_visibility.is_public(assertion_visibility,
    identity_visibility)`; every projection rule in `PROJECTION_RULES` reaches
    publication decisions through that, never through this function.

    `tests/test_vis01_shortcut_containment.py` pins the containment: this
    function may be called from `compute_launch_scope_reconciliation` and
    nowhere else, and its name may not appear in shipping code at all."""
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
    public_exclusions_path: Optional[str] = None,
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

        exclusions_path = public_exclusions_path or _DEFAULT_PUBLIC_EXCLUSIONS_PATH
        exclusion_policy_version, excluded_canonical_work_ids = (
            load_public_projection_exclusions(exclusions_path)
        )
        ctx = ProjectionContext(
            private_conn,
            exclusion_policy_version=exclusion_policy_version,
            excluded_canonical_work_ids=excluded_canonical_work_ids,
        )

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
            # discovery_identification is materialized from the now-populated
            # public base tables by the production builder, not projected
            # row-by-row -- see _materialize_public_identification. Must run
            # BEFORE _project_meta, which publishes its row count.
            if "discovery_identification" in projected_counts:
                projected_counts["discovery_identification"] = (
                    _materialize_public_identification(out_conn, private_conn)
                )
            if "discovery_locus_piece" in projected_counts:
                projected_counts["discovery_locus_piece"] = out_conn.execute(
                    "SELECT COUNT(*) FROM discovery_locus_piece"
                ).fetchone()[0]
            # Amendment 2026-08-12 (Q): membership rows reference the
            # identification table, so their filtered copy runs only AFTER it
            # is materialized -- and, like it, before _project_meta publishes
            # the row count.
            if "discovery_stratum_membership" in projected_counts:
                projected_counts["discovery_stratum_membership"] = (
                    _materialize_public_stratum_membership(private_conn, out_conn)
                )
            meta_rows = _project_meta(ctx, projected_counts)
            private_meta = dict(private_conn.execute("SELECT key, value FROM meta"))
            if private_meta.get("locus_display_version") == "locus-display-v1":
                import build_discovery_sidecar as builder

                locus_keys = {
                    "locus_display_version",
                    "locus_filter_version",
                    "expected_rows_discovery_locus_piece",
                } | {
                    f"expected_locus_{grain}_{status}"
                    for grain in ("claim", "identification")
                    for status in builder.LOCUS_STATUSES
                }
                meta_rows = [row for row in meta_rows if row["key"] not in locus_keys]
                meta_rows.extend(
                    {"key": key, "value": value}
                    for key, value in builder.locus_display_meta_rows(out_conn)
                )
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
            violations += check_identification_key_subset(private_conn, out_conn)
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
            # Owner ruling 2026-08-03: evidence dropped because its
            # `later_shared_text` routing reason could not be substantiated by a
            # publishable audit row. Reported in the OFFLINE release report, never
            # inside the deployed artifact -- a reader must not be able to infer
            # how many rows were withheld or why.
            "pruned_unreplayable_evidence": len(ctx.pruned_unreplayable_evidence_ids),
            # Evidence dropped as a CASCADE of the above: a claim asserting a
            # witness relation that lost its last witness row goes entirely.
            "pruned_g9_cascade_evidence": len(ctx.pruned_g9_cascade_evidence_ids),
            "public_projection_exclusions": {
                "policy_version": ctx.exclusion_policy_version,
                "configured_canonical_work_count": len(
                    ctx.excluded_canonical_work_ids
                ),
                "present_canonical_work_count": len(
                    ctx.present_excluded_canonical_work_ids
                ),
                "excluded_claim_count": len(ctx.policy_excluded_claim_ids),
                "excluded_evidence_count": len(ctx.policy_excluded_evidence_ids),
            },
            # A green `project()` is NOT a release decision. The self-gate above
            # is deliberately narrow -- it checks that what was just emitted is
            # internally coherent (closed graph, honest counts, right audience,
            # no invented identification keys) plus the masking gate. It does
            # NOT re-derive the ~30 corpus invariants. Stated here in machine
            # -readable form so a reader of the report cannot mistake this for
            # the release gate; see docs/specs/discovery-deploy.md.
            "self_gate": {
                "is_release_gate": False,
                "checks_run": [
                    "check_fk_closure",
                    "check_meta_counts",
                    "check_meta_audience",
                    "check_identification_key_subset",
                    "run_masking_gate",
                ],
                "release_gate": (
                    "scripts/verify_discovery_sidecar.py <asset> --audience public "
                    "-- run against the exact bytes going live, on the box, before "
                    "the swap"
                ),
            },
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
    parser.add_argument(
        "--public-exclusions",
        default=_DEFAULT_PUBLIC_EXCLUSIONS_PATH,
        help=(
            "Versioned public-projection canonical-work exclusion policy "
            "(default: docs/specs/discovery-public-projection-exclusions-v1.json)"
        ),
    )
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        report = project(
            args.private_db,
            args.public_db_out,
            public_exclusions_path=args.public_exclusions,
        )
    except ProjectionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"project_discovery_public: wrote {args.public_db_out}")
    print(f"project_discovery_public: reconciliation report at {report.get('report_path')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
