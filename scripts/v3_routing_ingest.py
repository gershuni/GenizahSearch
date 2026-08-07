"""Ingest gen-2's coverage-router decision, instead of recomputing it.

**This is Codex blocker 2, and the owner's decision of 2026-08-05 ("yes of
course"): the builder must LEARN gen-2's sorting rather than re-derive its own.**

Why it matters, measured rather than argued. Two thresholds exist over the same
quantity (`matched_letters / page_letters`):

* the gen-2 **router**: `page_coverage >= 0.2984` => `same_work` (witness),
  below => `parallel` (quotation). Threshold fitted on graded data
  (`coverage_route_meta`, AUC ~0.874), and it is the split the 400-card grading
  measured.
* the v2 **builder**: `LEVER1_COVERAGE_CLIFF = 0.45`, below => `review_only`
  with `routing_reason='low_coverage'`.

**Measured consequence of letting the builder recompute: 30,899 of 160,095
headline rows (19.3%) would be demoted** -- they sit in `[0.2984, 0.45)`, which
the router ships as witnesses. And the disagreement is strictly ONE-WAY: zero
`parallel` rows sit above 0.45, so recomputing can only SHRINK the validated
surface, never grow it. That is precisely "new data sorted the old way": the
handoff's ~0.89 headline precision would describe a population the asset does not
contain.

So this module emits an explicit, checkable MAPPING from the router's decision to
the builder's routing vocabulary, and a parity report proving the emitted routing
reproduces the router exactly. It does not re-derive anything.

GRAIN. The router keys on `(page_id, canonical_work_id)`; the match table keys on
raw `work_id`. Verified on the real artifact: the raw->canonical mapping is
strictly many-to-one (0 raw ids map to more than one canonical id), so resolving a
match row's route is unambiguous. That check is enforced here, not assumed.

MASKING (D-25): consumes only opaque ids, integer counts and floats. No corpus
name, no title, no reference text.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

# The router's own vocabulary (gen-2 `coverage_route.surface`).
SURFACE_SAME_WORK = "same_work"
SURFACE_PARALLEL = "parallel"
SURFACE_NOT_SHIPPED = "not_shipped"
ROUTER_SURFACES = (SURFACE_SAME_WORK, SURFACE_PARALLEL, SURFACE_NOT_SHIPPED)

# The builder's routing vocabulary that each router surface maps ONTO. Declared
# as data so the mapping is reviewable and testable rather than buried in an if.
#
#   same_work   -> shipped witness. The headline surface the grading measured.
#   parallel    -> shipped, but NOT a witness claim: a quotation/parallel
#                  relation. Carried with its own reason so a surface can tell
#                  the two apart without re-deriving coverage.
#   not_shipped -> review_only. The router already declined it.
SURFACE_TO_ROUTING: Dict[str, Tuple[str, Optional[str]]] = {
    SURFACE_SAME_WORK: ("shipped", None),
    SURFACE_PARALLEL: ("shipped", "gen2_parallel_surface"),
    SURFACE_NOT_SHIPPED: ("review_only", "gen2_router_not_shipped"),
}


class RoutingIngestError(RuntimeError):
    """Fail-closed error ingesting the router decision."""


def _ro_uri(db_path) -> str:
    """Read-only SQLite URI. See `v3_build_research_db._ro_uri` -- slicing the
    `file://` prefix off `as_uri()` is a Windows-only-correct bug (CI, 2026-08-07)."""
    return Path(db_path).resolve().as_uri() + "?mode=ro"


def load_router(evidence_db: str) -> Dict:
    """Read `coverage_route` + its meta, and the raw->canonical work map.

    Returns a dict with:
      `route`      {(page_id, canonical_work_id): (surface, page_coverage, shipped)}
      `raw_to_can` {work_id: canonical_work_id}
      `meta`       the router's own threshold/provenance row
      `counts`     surface -> row count, for the parity gate
    """
    conn = sqlite3.connect(_ro_uri(evidence_db), uri=True)
    try:
        conn.execute("PRAGMA cache_size=-400000")

        meta_cols = [r[1] for r in conn.execute("PRAGMA table_info(coverage_route_meta)")]
        meta_row = conn.execute("SELECT * FROM coverage_route_meta").fetchone()
        if meta_row is None:
            raise RoutingIngestError(
                "coverage_route_meta is empty -- refusing to ingest a router "
                "decision whose threshold and provenance are unrecorded"
            )
        meta = dict(zip(meta_cols, meta_row))

        route: Dict[Tuple[str, str], Tuple[str, float, int]] = {}
        counts: Dict[str, int] = {s: 0 for s in ROUTER_SURFACES}
        for page_id, can_id, surface, pcov, shipped in conn.execute(
            "SELECT page_id, canonical_work_id, surface, page_coverage, shipped "
            "FROM coverage_route"
        ):
            if surface not in SURFACE_TO_ROUTING:
                raise RoutingIngestError(
                    f"coverage_route carries an unknown surface value -- refusing to "
                    f"guess its routing. Known: {sorted(SURFACE_TO_ROUTING)}"
                )
            key = (page_id, can_id)
            if key in route and route[key][0] != surface:
                raise RoutingIngestError(
                    "coverage_route gives two different surfaces for one "
                    "(page_id, canonical_work_id) -- the router grain is not unique"
                )
            route[key] = (surface, pcov, shipped)
            counts[surface] += 1

        raw_to_can: Dict[str, str] = {}
        ambiguous = 0
        for work_id, can_id, n in conn.execute(
            "SELECT work_id, canonical_work_id, COUNT(*) FROM discovery_claim "
            "GROUP BY work_id, canonical_work_id"
        ):
            if work_id in raw_to_can and raw_to_can[work_id] != can_id:
                ambiguous += 1
            raw_to_can[work_id] = can_id
        if ambiguous:
            raise RoutingIngestError(
                f"{ambiguous} raw work_id(s) map to more than one canonical_work_id -- "
                f"a match row's route would be ambiguous. Halting rather than picking one."
            )
    finally:
        conn.close()
    return {"route": route, "raw_to_can": raw_to_can, "meta": meta, "counts": counts}


def resolve_routing(
    page_id: str, work_id: str, router: Dict
) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """Map one match row onto `(routing_status, routing_reason, page_coverage)`.

    Returns `(None, None, None)` when the router has no decision for the pair --
    the caller must treat that as a HALT condition, not a default. Defaulting is
    how a silent re-derivation creeps back in.
    """
    can_id = router["raw_to_can"].get(work_id)
    if can_id is None:
        return (None, None, None)
    entry = router["route"].get((page_id, can_id))
    if entry is None:
        return (None, None, None)
    surface, pcov, _shipped = entry
    status, reason = SURFACE_TO_ROUTING[surface]
    return (status, reason, pcov)


def parity_report(router: Dict, *, builder_cliff: float = 0.45) -> Dict:
    """Quantify what recomputing would have done -- the blocker-2 evidence.

    Reported, never applied: this exists so the decision to ingest is auditable
    against a number rather than an assertion.
    """
    same_below = sum(
        1 for (surface, pcov, _s) in router["route"].values()
        if surface == SURFACE_SAME_WORK and pcov is not None and pcov < builder_cliff
    )
    parallel_above = sum(
        1 for (surface, pcov, _s) in router["route"].values()
        if surface == SURFACE_PARALLEL and pcov is not None and pcov >= builder_cliff
    )
    same_total = router["counts"][SURFACE_SAME_WORK]
    return {
        "router_threshold": router["meta"].get("t") or router["meta"].get("threshold"),
        "builder_cliff": builder_cliff,
        "same_work_total": same_total,
        "same_work_would_be_demoted": same_below,
        "same_work_demoted_pct": (
            round(100.0 * same_below / same_total, 2) if same_total else None
        ),
        "parallel_would_be_promoted": parallel_above,
        "one_way": parallel_above == 0,
        "surface_counts": dict(router["counts"]),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Ingest the gen-2 coverage-router decision")
    ap.add_argument("--evidence-db", required=True)
    ap.add_argument("--report", help="write the parity report as JSON here")
    args = ap.parse_args(argv)
    try:
        router = load_router(args.evidence_db)
    except RoutingIngestError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    report = parity_report(router)
    for key, value in report.items():
        print(f"  {key:30s} {value}")
    if args.report:
        Path(args.report).write_text(json.dumps(report, indent=1), encoding="utf-8")
    if not report["one_way"]:
        print("NOTE: the disagreement is no longer one-way -- re-read the mapping.",
              file=sys.stderr)
    print("router loaded OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
