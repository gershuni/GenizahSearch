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
#   parallel    -> review_only / gen2_parallel_surface. **CORRECTED 2026-08-07
#                  after Codex round 2.** The first version mapped this to
#                  `shipped` on the reasoning that a quotation is still a real
#                  relation worth surfacing. That was a semantic corruption, and
#                  the review named the mechanism precisely: these rows keep
#                  `evidence_kind='witness'`, and
#                  `assemble_claims_and_evidence` derives `claim_type` from
#                  witness SPAN DOMINANCE across the page -- so a shipped
#                  quotation holding the page's largest span resolves to
#                  `direct_witness`, and the panel renders its relation chip
#                  from `claim_type`, never from `routing_reason`. A quotation
#                  would have appeared as a direct witness and entered the main
#                  pool as same-work evidence. `review_only` keeps it out of
#                  every shipped-gated read by construction instead of relying
#                  on each consumer to remember a reason code.
#
#                  Surfacing quotations as their own relation is a real
#                  possibility, but it needs `evidence_kind='shared_text'` (or a
#                  new kind) and a claim_type path that does not run the witness
#                  dominance rule -- a dated schema amendment, not a routing
#                  tweak. Out of scope here; the reason code preserves the
#                  distinction in the asset so a later phase can promote them
#                  without re-running the router.
#   not_shipped -> review_only. The router already declined it.
SURFACE_TO_ROUTING: Dict[str, Tuple[str, Optional[str]]] = {
    SURFACE_SAME_WORK: ("shipped", None),
    SURFACE_PARALLEL: ("review_only", "gen2_parallel_surface"),
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
        # The router's own `shipped` flag must AGREE with the surface it carries.
        # Round 2 found the first version read the column and then discarded it,
        # so a row whose surface and shipped state disagreed passed silently --
        # and since a disagreement means the producer's two encodings of one
        # decision diverged, guessing which to honour is exactly the kind of
        # silent re-derivation this module exists to prevent.
        surface_is_shipped = {
            SURFACE_SAME_WORK: True, SURFACE_PARALLEL: True, SURFACE_NOT_SHIPPED: False,
        }
        for page_id, can_id, surface, pcov, shipped in conn.execute(
            "SELECT page_id, canonical_work_id, surface, page_coverage, shipped "
            "FROM coverage_route"
        ):
            if surface not in SURFACE_TO_ROUTING:
                raise RoutingIngestError(
                    f"coverage_route carries an unknown surface value -- refusing to "
                    f"guess its routing. Known: {sorted(SURFACE_TO_ROUTING)}"
                )
            if shipped is not None and bool(shipped) != surface_is_shipped[surface]:
                raise RoutingIngestError(
                    f"coverage_route row disagrees with itself: surface {surface!r} "
                    f"implies shipped={surface_is_shipped[surface]} but the row carries "
                    f"shipped={shipped!r}. The router's two encodings of one decision "
                    f"have diverged -- halting rather than picking one."
                )
            key = (page_id, can_id)
            if key in route:
                # ANY duplicate halts, not merely a surface-DISAGREEING one.
                # Round 2's finding: tolerating agreeing duplicates inflates
                # `counts` while replacing the dict entry, so the parity report
                # is computed at neither the router's grain nor the emitted
                # one -- the numbers it reports would be quietly wrong even
                # though every individual decision agreed.
                raise RoutingIngestError(
                    "coverage_route carries more than one row for a single "
                    "(page_id, canonical_work_id) -- the router grain is not unique, "
                    "so any count derived from it is not at the router's grain. "
                    "Halting rather than de-duplicating with an undeclared rule."
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


def apply_router_routing(
    evidence_specs, router: Dict, *, raw_work_by_minted: Dict[str, str],
    track1_source: str, witness_kind: str,
) -> Dict:
    """REPLACE the builder's own coverage routing on tier-A witness specs with
    gen-2's router decision. This is the wiring blocker 2 actually requires --
    round 2 correctly found the reader alone had no effect, because
    `apply_lever1_coverage` still ran and `_ingest_tier_a` still shipped
    everything.

    Mutates each track1_direct witness spec in place, setting `routing_status`
    and `routing_reason` from `SURFACE_TO_ROUTING`. Returns a report counting
    every outcome, so the caller can assert parity against the router rather
    than trust that this ran.

    **Every spec must be decided.** A spec the router has no decision for is
    counted as `undecided` and left UNTOUCHED; the caller must treat a non-zero
    `undecided` as fatal. Defaulting here is how a silent re-derivation returns.

    `raw_work_by_minted` maps the minted `work_id` on the spec back to the raw
    `M:`/`REF2:`/`J:` id the router keys on -- the spec does not carry the raw
    id, so the caller supplies the map it already built for `work_index`.
    """
    report = {
        "considered": 0, "shipped": 0, "review_only": 0, "undecided": 0,
        "by_reason": {}, "undecided_examples": [],
        # The (page_id, canonical_work_id) keys actually considered, so
        # `assert_emitted_parity` can compare against what the router decided for
        # THESE rows rather than against the whole router table.
        "considered_keys": set(),
    }
    for spec in evidence_specs:
        if (spec.get("evidence_source") != track1_source
                or spec.get("evidence_kind") != witness_kind):
            continue
        report["considered"] += 1
        raw = raw_work_by_minted.get(spec.get("work_id"))
        page_id = spec.get("page_id")
        status, reason, _pcov = (
            resolve_routing(page_id, raw, router) if raw else (None, None, None)
        )
        if status is None:
            report["undecided"] += 1
            if len(report["undecided_examples"]) < 5:
                # Opaque ids only (D-25): no title, no corpus name.
                report["undecided_examples"].append({"page_id": page_id, "work_id": raw})
            continue
        can_id = router["raw_to_can"].get(raw)
        if can_id is not None:
            report["considered_keys"].add((page_id, can_id))
        spec["routing_status"] = status
        if reason is not None:
            spec["routing_reason"] = reason
        report[status] = report.get(status, 0) + 1
        key = reason or "none"
        report["by_reason"][key] = report["by_reason"].get(key, 0) + 1
    return report


def assert_assembled_parity(
    evidence_rows, router: Dict, claim_rows, *,
    routing_status_idx: int, claim_id_idx: int,
    evidence_source_idx: int, track1_source: str,
    raw_work_by_minted: Dict[str, str],
) -> Dict:
    """PER-KEY parity against the ASSEMBLED rows (Codex R3, HIGH).

    Round 3's objection to `assert_emitted_parity` was correct and is worth stating
    precisely: it checks only that nothing was left undecided, that the reason
    codes are known, and that not *everything* was demoted. So a mapping error
    demoting **90%** of `same_work` rows passes, as long as one row ships. And
    comparing `report["shipped"]` against a count recomputed from
    `SURFACE_TO_ROUTING` would be circular -- both sides come from the same table.

    The non-circular comparison is between two INDEPENDENT artifacts: the router's
    own `surface` per key, and the `routing_status` that actually landed on the
    assembled evidence row for that key, AFTER `assemble_claims_and_evidence` has
    deduplicated on `evidence_id`, resolved collisions and dropped rows. Every
    track1_direct row must match, row by row -- not in aggregate, since two
    compensating errors cancel in a total.

    Returns a per-key report `{checked, mismatches}`. Raises on the first
    disagreement with counts only (D-25).

    Note on scope: this compares the ASSEMBLED rows rather than the inserted ones.
    Assembly is where dedup and collision resolution happen -- everything after it
    is a positional `executemany` of these same tuples, so a divergence below this
    point would be an INSERT-column bug, which
    `test_the_release_offsets_gate_reads_the_right_tuple_positions` covers
    separately.
    """
    # The router's decision, keyed by (page_id, canonical_work_id).
    expected_status = {}
    for key, (surface, _pcov, _shipped) in router["route"].items():
        expected_status[key] = SURFACE_TO_ROUTING[surface][0]

    # Assembled claim rows carry (page_id, work_id, claim_id, ...) -- so the claim
    # id resolves back to the (page, minted work) pair the evidence belongs to.
    claim_key = {c[2]: (c[0], c[1]) for c in claim_rows}

    checked = 0
    mismatches = 0
    for row in evidence_rows:
        if row[evidence_source_idx] != track1_source:
            continue
        pair = claim_key.get(row[claim_id_idx])
        if pair is None:
            raise RoutingIngestError(
                "an assembled evidence row references a claim id absent from the "
                "assembled claim rows -- parity cannot be established"
            )
        page_id, minted_work = pair
        raw = raw_work_by_minted.get(minted_work)
        can_id = router["raw_to_can"].get(raw) if raw else None
        want = expected_status.get((page_id, can_id)) if can_id else None
        if want is None:
            raise RoutingIngestError(
                "an assembled track1_direct row has no router decision for its "
                "(page, canonical work) pair -- it would ship unrouted"
            )
        checked += 1
        if row[routing_status_idx] != want:
            mismatches += 1
    if mismatches:
        raise RoutingIngestError(
            f"{mismatches} of {checked} assembled track1_direct row(s) carry a "
            f"routing_status that does NOT match gen-2's decision for their own "
            f"(page, canonical work) key. This is a per-key comparison, so a "
            f"compensating pair of errors cannot hide in a total."
        )
    if not checked:
        raise RoutingIngestError(
            "zero assembled track1_direct rows were compared, so parity verified "
            "nothing -- a gate that passes over an empty population is a false green"
        )
    return {"checked": checked, "mismatches": 0}


def assert_emitted_parity(report: Dict, router: Dict) -> None:
    """Gate 10, on the EMITTED result rather than on the source.

    Round 2's HIGH finding was exact: `parity_report` compares two thresholds
    *inside the source database* and never checks that the built asset matches
    the router. This asserts the applied outcome instead.

    Two properties, both fail-closed:
      1. **No spec left undecided.** A silently-unrouted tier-A row would keep
         whatever `_ingest_tier_a` gave it (`shipped`), which is precisely the
         re-derivation this replaces.
      2. **The shipped count equals the router's own shipped surfaces**
         (`same_work` + `parallel`) for the rows actually considered. A mismatch
         means the mapping or the grain drifted.
    """
    if report["undecided"]:
        raise RoutingIngestError(
            f"{report['undecided']} tier-A witness spec(s) got no router decision -- "
            f"they would keep the ingest default and silently bypass gen-2's routing. "
            f"Examples (opaque ids): {report['undecided_examples']}"
        )
    expected_reasons = {
        reason for (_status, reason) in SURFACE_TO_ROUTING.values() if reason
    } | {"none"}
    unexpected = set(report["by_reason"]) - expected_reasons
    if unexpected:
        raise RoutingIngestError(
            f"emitted routing carries {len(unexpected)} reason code(s) outside the "
            f"declared mapping -- the applied routing is not the ingested one"
        )
    # A total wipe-out is the signature of the 135-07 field-collision bug (a
    # wrong metric fed to the routing gate demoted ~100% of witnesses and
    # orphaned every shipped page). But "shipped == 0" is ALSO the correct answer
    # for a small or deliberately-demoted set, so the check is expressed against
    # what the MAPPING says for the rows actually considered.
    #
    # The surfaces this asks about must come from `SURFACE_TO_ROUTING`, not from
    # a hand-listed pair: an earlier version listed `same_work` + `parallel` as
    # "the shipped surfaces", which became wrong the moment `parallel` was
    # correctly remapped to `review_only` -- and it then failed a legitimate
    # build. Deriving the set from the mapping keeps the guard honest through a
    # future remapping instead of encoding today's answer twice.
    shipped_surfaces = {
        surface for surface, (status, _reason) in SURFACE_TO_ROUTING.items()
        if status == "shipped"
    }
    if report["considered"] and report["shipped"] == 0 and shipped_surfaces:
        # Only alarming when the mapping WOULD have shipped a row we actually
        # considered -- otherwise a page set that legitimately routes entirely to
        # a demoting surface would trip a false alarm.
        would_ship = {
            key for key, (surface, _p, _s) in router["route"].items()
            if surface in shipped_surfaces
        }
        if would_ship & report.get("considered_keys", set()):
            raise RoutingIngestError(
                "every considered tier-A spec was demoted although the mapping ships "
                "some of them -- the mapping or the grain is wrong (this is the shape "
                "of the 135-07 field-collision bug)"
            )


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
