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
import re
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


class RoutingRegrainError(RoutingIngestError):
    """Fail-closed error re-graining the router decision to the split grain."""


# ---------------------------------------------------------------------------
# SPLIT-GRAIN RE-GRAINING (option 4, owner-approved 2026-08-07)
# ---------------------------------------------------------------------------
#
# THE PROBLEM, measured. gen-2's router scored COLLAPSED canonical ids -- most
# consequentially `M:Ytext1000`, which is all 39 books of the Bible as ONE work
# (its own handoff names this as the over-collapsed case). The v3 slim table
# carries the SPLIT ids (`M:Ytext1000_00` = Genesis ... `_38`), i.e. book /
# tractate grain. So 141,358 of 275,894 tier-A rows (51.2%) have no decision at
# their own key and the wipe-out guard halts -- correctly.
#
# WHY NOT INHERIT THE PARENT'S VERDICT. The parent's `page_coverage` is computed
# from MAX(matched_letters) over the WHOLE collapsed group, so it describes "this
# page overlaps somewhere in the 39-book Bible", not "this page is a copy of
# Genesis". Measured: inheritance would over-promote 6,233 rows (parent
# `same_work`, own-grain coverage below threshold), 46.4% of them below HALF the
# threshold. Coverage NEVER rises when the unit narrows (verified: 0 of 138,800
# comparable rows), so the error is strictly one-way -- over-promotion.
#
# WHAT THIS DOES INSTEAD. Recompute the router's OWN estimand at the split grain
# and apply the router's OWN threshold. Two properties make that legitimate, both
# measured on the real artifacts rather than argued:
#
#  1. THE ESTIMAND IS REACHABLE AT SPLIT GRAIN. The producer
#     (`gen2_coverage_router.py` L19) defines the calibrated feature as
#     "MAX(matched_letters_legacy) over ALL evidence of the group / page n_chars",
#     NOT shadow-filtered. `coverage_route.matched_letters` equals that MAX on
#     354,528/354,528 rows (100.000%). And `discovery_evidence.ref_work` DOES
#     carry all 39 book-level ids even though `discovery_claim.work_id` collapses
#     them -- so grouping evidence by `(page_id, ref_work)` yields the same
#     estimand per book, available for 275,894/275,894 tier-A rows.
#  2. IT REPRODUCES GEN-2 WHERE THE GRAINS AGREE. Applied to rows gen-2 scored at
#     the same key, this reproduces its surface on 134,512/134,536 (99.982%). All
#     24 exceptions are one-way (`same_work` -> `parallel`) and each is a case
#     where a DIFFERENT `ref_work` under the same canonical id carried a wider
#     span, so gen-2's group MAX exceeded this work's own. Scoring each work on
#     its own widest span is the intended change.
#
# THREE HAZARDS THIS CLOSES BY CONSTRUCTION, each one measured:
#
#  A. `not_shipped` IS NOT A COVERAGE VERDICT. Cross-tab over all 354,528 router
#     rows: `same_work` is exactly coverage>=T and `parallel` exactly coverage<T,
#     but `not_shipped` carries 14,406 rows ABOVE the threshold. It is an upstream
#     eligibility decision (111,144 shadowed + 24,720 `later_shared_text`; it maps
#     to `routing_status='review_only'` on 121,114/121,114 gen-2 claims). So a
#     `not_shipped` key is PRESERVED verbatim and never recomputed. Recomputing it
#     would silently ship rows gen-2 declined, and no existing gate checks that
#     axis -- it is not a monotonicity violation, it is a different axis.
#  B. MERGES ARE A SECOND RE-GRAINING, INDEPENDENT OF `_NN`. `raw_to_can` holds 16
#     alias entries where raw != canonical -- owner-ratified canonical MERGES
#     (Jaccard 0.978-1.000, all `owner_verdict: approve`), e.g.
#     `M:Ytext273001 -> REF2:sef_tur_orach_chaim`. These carry NO `_NN` suffix, so
#     a suffix-stripping-only reconciliation misses them: that is exactly the
#     "2,558 rows over 16 works the router never scored" which was a JOIN BUG in
#     my gap measurement, not a gap. Resolving exact -> `_NN` parent -> canonical
#     alias leaves ZERO unresolved. Scoring an alias side independently would
#     split one owner-approved work into two, and `discovery_identification` is
#     `UNIQUE (sys_id, canonical_work_id)` -- so it corrupts the identification
#     grain, not merely a label. Alias rows therefore keep the CANONICAL decision.
#  C. THE UNITS ARE MISMATCHED ON PURPOSE. `matched_letters` is a NORMALIZED
#     Hebrew-letter width; `pages.n_chars` is the RAW character length (verified
#     400/400 raw, 0/400 normalized; ratio p50 0.767). So every coverage value is
#     ~23% "too low" -- and the threshold was calibrated against that same
#     mismatched ratio. Numerator and denominator must stay mismatched TOGETHER:
#     "correcting" the denominator while keeping the frozen threshold flips 3.9%
#     of rows to `same_work` with no error signal. Never normalize here.
#
# THE THRESHOLD IS REUSED, NOT RE-DERIVED, AND THAT IS RECORDED. It was calibrated
# at the COLLAPSED grain on 1,395 graded claims, of which 340 (24.2%) fall on the
# three works v3 splits. Refitting on the grain-clean 1,055 gives 0.2531 -- so
# reusing 0.2984 costs 0.47pp accuracy on those rows and is the CONSERVATIVE
# choice (the refit would ship 10,224 MORE rows). Reuse is therefore defensible,
# but it is a JUDGEMENT, not a validated calibration at this grain, and
# `regrain_router_to_split` records both grains in its report so the artifact
# cannot imply otherwise.

# The re-grain decision's provenance, recorded in `meta` so a reader needs no
# build log. Keys are meta row names; see `docs/specs/discovery-v3-bake-plan.md`.
REGRAIN_META_CALIBRATED_GRAIN = "coverage_threshold_grain_calibrated"
REGRAIN_META_APPLIED_GRAIN = "coverage_threshold_grain_applied"
REGRAIN_META_THRESHOLD = "coverage_threshold"
REGRAIN_META_SOURCE = "coverage_threshold_source"

GRAIN_COLLAPSED = "collapsed_canonical"
GRAIN_SPLIT = "split_work"


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


_SPLIT_SUFFIX = re.compile(r"^(.*)_(\d+)$")


def _router_threshold(router: Dict) -> float:
    """The router's OWN threshold, read from its meta row. NEVER a literal.

    Measured reason this is enforced rather than trusted: truncated variants of
    this value already exist in the tree (`0.298` as the producer's fallback,
    `0.2984` in a test fixture), and either moves 90 rows across the line versus
    the artifact's exact 0.2984126984126984.
    """
    meta = router.get("meta") or {}
    # Accept either column name. The real `g_launch3` artifact calls it
    # `threshold`; `parity_report` already reads `meta.get("t") or
    # meta.get("threshold")`, so a second, stricter contract here would reject an
    # input the rest of the module accepts.
    raw = meta.get("threshold")
    if raw is None:
        raw = meta.get("t")
    if raw is None:
        raise RoutingRegrainError(
            "coverage_route_meta carries neither `threshold` nor `t` -- refusing to "
            "re-grain a routing decision against an unknown cut"
        )
    try:
        t = float(raw)
    except (TypeError, ValueError):
        raise RoutingRegrainError(
            "coverage_route_meta.threshold is not a number -- refusing to guess it"
        ) from None
    if not (0.0 < t < 1.0):
        raise RoutingRegrainError(
            f"coverage_route_meta.threshold is out of range ({t!r}) -- a coverage "
            f"cut must lie strictly between 0 and 1"
        )
    return t


def load_split_grain_coverage(evidence_db: str) -> Dict[Tuple[str, str], int]:
    """`(page_id, ref_work) -> MAX(matched_letters_legacy)`, the producer's estimand.

    Grouped by `discovery_evidence.ref_work` rather than `discovery_claim.work_id`,
    because `ref_work` carries the BOOK-level ids (all 39 under `M:Ytext1000`)
    while `work_id` collapses them -- that asymmetry is what makes the split grain
    computable at all.

    Deliberately NOT shadow-filtered, matching `gen2_coverage_router.py`'s
    "over ALL evidence of the group ... NOT shadow-filtered". Filtering here would
    compute a different quantity from the one the threshold was fitted on.
    """
    conn = sqlite3.connect(_ro_uri(evidence_db), uri=True)
    try:
        conn.execute("PRAGMA cache_size=-400000")
        best: Dict[Tuple[str, str], int] = {}
        for page_id, ref_work, ml in conn.execute(
            "SELECT cl.page_id, e.ref_work, e.matched_letters_legacy "
            "FROM discovery_claim cl JOIN discovery_evidence e ON e.claim_id = cl.claim_id"
        ):
            if ml is None or ref_work is None or page_id is None:
                continue
            key = (page_id, ref_work)
            if ml > best.get(key, -1):
                best[key] = ml
    finally:
        conn.close()
    if not best:
        raise RoutingRegrainError(
            "no (page_id, ref_work) coverage groups could be built from the evidence "
            "DB -- refusing to re-grain against an empty estimand"
        )
    return best


def regrain_router_to_split(
    router: Dict, split_max: Dict[Tuple[str, str], int],
    page_chars: Dict[str, int], tier_a_keys,
) -> Dict:
    """ADD split-grain decisions to `router["route"]`, in place, and report.

    `tier_a_keys` is the iterable of `(page_id, raw_work_id)` pairs the builder will
    actually ingest (tier A = unshadowed). Every one of them must end up with a
    decision at its OWN key, so `resolve_routing` finds it without any caller
    needing to know re-graining happened. That is the whole point: the entry is
    written into the SAME dict under the SAME shape, so `resolve_routing`,
    `apply_router_routing` and `assert_assembled_parity` are untouched.

    `page_chars` is `pages.page_id -> n_chars`, the RAW character length. Do not
    normalize it (hazard C above).

    Resolution order per key, and why each step exists:
      1. EXACT `(page_id, raw_work_id)` already in the route -- gen-2 scored this
         very key; keep its decision verbatim, recompute nothing.
      2. `not_shipped` at the resolved key -> PRESERVE (hazard A: eligibility, not
         coverage).
      3. CANONICAL ALIAS (`raw_to_can[w] != w`) -> keep the canonical decision
         (hazard B: splitting an owner-ratified merge corrupts the identification
         grain).
      4. Otherwise RECOMPUTE from this work's own MAX and the router's own
         threshold, with `>=` (matching the producer; 3 rows sit exactly at T).

    A key that cannot be decided by any step is counted in `undecided` and left
    absent -- the caller's wipe-out guard must then halt. Defaulting here is how a
    silent re-derivation returns.
    """
    threshold = _router_threshold(router)
    route = router["route"]
    raw_to_can = router.get("raw_to_can") or {}

    # The keys this call ADDS, so `assert_assembled_parity` can report them
    # separately from the ones gen-2 decided. Without this the parity gate is
    # TAUTOLOGICAL on them: it builds `expected_status` from `router["route"]`,
    # which is the very dict mutated below, so a re-grained row is compared
    # against the recomputation itself. That is self-consistency, not independent
    # verification, and the gate's docstring advertises non-circularity -- so the
    # two populations must be counted apart rather than summed into one number
    # that reads like 275,894 rows of independent agreement.
    router.setdefault("regrained_keys", set())

    report = {
        "threshold": threshold,
        "threshold_grain_calibrated": GRAIN_COLLAPSED,
        "threshold_grain_applied": GRAIN_SPLIT,
        "considered": 0,
        "kept_exact": 0,
        "kept_not_shipped": 0,
        "kept_canonical_alias": 0,
        "recomputed_same_work": 0,
        "recomputed_parallel": 0,
        "undecided": 0,
        "undecided_examples": [],
        # Rows whose recomputed surface DISAGREES with the collapsed parent's --
        # the population inheriting the parent verdict would have got wrong.
        "disagrees_with_parent": 0,
        "added": 0,
    }

    for page_id, raw_work in tier_a_keys:
        report["considered"] += 1
        key = (page_id, raw_work)

        if key in route:
            report["kept_exact"] += 1
            continue

        # Which router key does this row belong to, if any?
        parent_key = None
        m = _SPLIT_SUFFIX.match(raw_work)
        if m and (page_id, m.group(1)) in route:
            parent_key = (page_id, m.group(1))
        alias_key = None
        can = raw_to_can.get(raw_work)
        if can is not None and can != raw_work:
            if (page_id, can) in route:
                alias_key = (page_id, can)
            else:
                m2 = _SPLIT_SUFFIX.match(can)
                if m2 and (page_id, m2.group(1)) in route:
                    alias_key = (page_id, m2.group(1))

        anchor = parent_key or alias_key
        if anchor is None:
            report["undecided"] += 1
            if len(report["undecided_examples"]) < 5:
                # Opaque ids only (D-25).
                report["undecided_examples"].append(
                    {"page_id": page_id, "work_id": raw_work}
                )
            continue

        anchor_surface, anchor_pcov, anchor_shipped = route[anchor]

        # (2) eligibility, not coverage -- never recomputed.
        if anchor_surface == SURFACE_NOT_SHIPPED:
            route[key] = (SURFACE_NOT_SHIPPED, anchor_pcov, anchor_shipped)
            router["regrained_keys"].add(key)
            report["kept_not_shipped"] += 1
            report["added"] += 1
            continue

        # (3) an owner-ratified merge keeps the canonical decision.
        if parent_key is None and alias_key is not None:
            route[key] = (anchor_surface, anchor_pcov, anchor_shipped)
            router["regrained_keys"].add(key)
            report["kept_canonical_alias"] += 1
            report["added"] += 1
            continue

        # (4) recompute this work's OWN coverage.
        n_chars = page_chars.get(page_id)
        own_max = split_max.get(key)
        if not n_chars or own_max is None:
            report["undecided"] += 1
            if len(report["undecided_examples"]) < 5:
                report["undecided_examples"].append(
                    {"page_id": page_id, "work_id": raw_work}
                )
            continue
        pcov = own_max / n_chars
        # SANITY BOUND. `matched_letters` is a NORMALIZED Hebrew-letter width and
        # `n_chars` is the RAW character length, so this ratio is structurally
        # bounded well below 1.0 (measured: max 0.842 over 275,894 tier-A rows,
        # and 0 rows with matched_letters > n_chars). A value above 1.0 therefore
        # means the numerator and denominator are no longer the pair the threshold
        # was calibrated on -- e.g. someone "helpfully" normalized the
        # denominator, which silently promotes ~3.7% of rows with no error signal.
        # Without this, such a defect emits impossible coverage and every other
        # gate accepts it. Fail loudly instead; see also the paired
        # `n_chars == len(text)` assertion in the bake plan's gate list.
        if pcov > 1.0:
            raise RoutingRegrainError(
                f"recomputed split-grain coverage exceeds 1.0 ({pcov:.4f}) -- the "
                f"numerator and denominator are not the pair the threshold was "
                f"calibrated on. Refusing to route on an impossible coverage. "
                f"(counts only, D-25: matched={own_max}, denominator={n_chars})"
            )
        surface = SURFACE_SAME_WORK if pcov >= threshold else SURFACE_PARALLEL
        route[key] = (surface, pcov, 1)
        router["regrained_keys"].add(key)
        report["added"] += 1
        if surface == SURFACE_SAME_WORK:
            report["recomputed_same_work"] += 1
        else:
            report["recomputed_parallel"] += 1
        if surface != anchor_surface:
            report["disagrees_with_parent"] += 1

    return report


def resolve_routing(
    page_id: str, work_id: str, router: Dict
) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """Map one match row onto `(routing_status, routing_reason, page_coverage)`.

    Returns `(None, None, None)` when the router has no decision for the pair --
    the caller must treat that as a HALT condition, not a default. Defaulting is
    how a silent re-derivation creeps back in.

    OWN KEY FIRST (discovery-v3 split-grain re-graining). `regrain_router_to_split`
    writes a decision under the row's OWN `(page_id, raw_work_id)`, and a split id
    like `M:Ytext1000_26` is deliberately absent from `raw_to_can` -- gen-2 never
    had it. So an exact own-key hit is honoured BEFORE the canonical translation;
    without this, every re-grained row would resolve to `None` and the wipe-out
    guard would halt on a decision that had in fact been made. The canonical path
    is unchanged for every key the re-grainer did not touch.
    """
    entry = router["route"].get((page_id, work_id))
    if entry is None:
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
    d17_audit_rows=(),
    canonical_by_minted: Optional[Dict[str, str]] = None,
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

    D-17 IS RECONCILED, NOT EXEMPTED (Codex R4, HIGH). The first version SKIPPED
    this gate entirely whenever D-17 demoted anything, on the reasoning that D-17
    legitimately changes routing after the router. Round 4 was right that this
    disables parity on exactly the builds that matter: one valid demotion suppresses
    the comparison for every row, and nothing then reconciles the final statuses
    with the router's. So instead of skipping, each `review_only` row that the
    router shipped must be justified by a MATCHING D-17 audit row. A demotion with
    no audit row, or an audit row for a row nobody demoted, both fail.

    THE TWO CANONICAL SPACES, which is the subtlety here and cost two wrong
    attempts. `apply_d17_demotion` writes `demoted_work_id` as the BUILDER's
    canonical id (`ids.canonical_work_id` over the MINTED `w######`, so a merged
    twin is never compared against itself). The router keys on GEN-2's
    `canonical_work_id`. Those are different id spaces, and matching them directly
    silently matches nothing -- which is exactly what happened: my first version
    keyed on the raw id, the second on gen-2's canonical, and both reported an
    "audit trail disagrees with the asset" failure whose real cause was this
    mismatch. So the audit key is translated through `canonical_by_minted`, which
    the caller supplies because it already computed it.

    A caller that omits it gets NO reconciliation -- every D-17 demotion then reads
    as a parity failure, loudly. That is the right default for a translation nobody
    can infer.

    Note on scope: this compares the ASSEMBLED rows rather than the inserted ones.
    Assembly is where dedup and collision resolution happen -- everything after it
    is a positional `executemany` of these same tuples, so a divergence below this
    point would be an INSERT-column bug, which
    `test_the_release_offsets_gate_reads_the_right_tuple_positions` covers
    separately.
    """
    # (page_id, demoted_raw_work_id) -> the audited D-17 demotions.
    audited = {
        (a.get("page_id"), a.get("demoted_work_id"))
        for a in d17_audit_rows
        if a.get("decision") == "demoted" and a.get("demoted_work_id") is not None
    }
    audited_used = set()
    # The router's decision, keyed by (page_id, canonical_work_id).
    expected_status = {}
    for key, (surface, _pcov, _shipped) in router["route"].items():
        expected_status[key] = SURFACE_TO_ROUTING[surface][0]

    # Assembled claim rows carry (page_id, work_id, claim_id, ...) -- so the claim
    # id resolves back to the (page, minted work) pair the evidence belongs to.
    claim_key = {c[2]: (c[0], c[1]) for c in claim_rows}

    checked = 0
    mismatches = 0
    # Split the total: `checked_independent` is the only part that is genuine
    # cross-artifact verification (gen-2 decided it, the builder emitted it).
    # `checked_regrained` compares the re-grainer's own output against itself.
    checked_independent = 0
    checked_regrained = 0
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
        # OWN KEY FIRST, mirroring `resolve_routing`: a re-grained split row's
        # decision lives under its own raw id, which is absent from `raw_to_can`.
        # Reading only the canonical translation here would report every
        # re-grained row as unrouted -- a parity gate failing on the rows it was
        # extended to cover.
        resolved_key = (page_id, raw) if raw else None
        want = expected_status.get(resolved_key) if resolved_key else None
        if want is None:
            can_id = router["raw_to_can"].get(raw) if raw else None
            if can_id:
                resolved_key = (page_id, can_id)
                want = expected_status.get(resolved_key)
            else:
                resolved_key = None
        if want is None:
            raise RoutingIngestError(
                "an assembled track1_direct row has no router decision for its "
                "(page, canonical work) pair -- it would ship unrouted"
            )
        checked += 1
        # WHICH population this row belongs to. A re-grained key's `expected_status`
        # was written by `regrain_router_to_split` into the SAME dict this gate
        # reads, so comparing them is SELF-CONSISTENCY, not the independent
        # cross-artifact verification this gate's docstring advertises. Counted
        # apart so `checked` is never read as N rows of independent agreement.
        if resolved_key in (router.get("regrained_keys") or ()):
            checked_regrained += 1
        else:
            checked_independent += 1
        got = row[routing_status_idx]
        if got == want:
            continue
        # The ONE permitted divergence: the router shipped it and D-17 demoted it,
        # with an audit row naming exactly this (page, work). Anything else is a
        # parity failure.
        builder_canon = (canonical_by_minted or {}).get(minted_work)
        if (want == "shipped" and got == "review_only"
                and builder_canon is not None
                and (page_id, builder_canon) in audited):
            audited_used.add((page_id, builder_canon))
            continue
        mismatches += 1
    # An audit row that matches no demoted assembled row means the audit trail and
    # the asset disagree -- the trail claims a demotion the asset does not show.
    #
    # Reported AFTER the per-key mismatches, and both are named in one message: an
    # unjustified demotion and an unmatched audit row are usually two views of the
    # SAME defect, so raising on whichever is checked first told half the story and
    # sent the reader after the wrong cause. (My first version raised on the audit
    # side first and reported an audit/asset disagreement for what was actually a
    # grain error in this function.)
    unused = audited - audited_used
    if mismatches or unused:
        detail = []
        if mismatches:
            detail.append(
                f"{mismatches} of {checked} assembled track1_direct row(s) carry a "
                f"routing_status that does NOT match gen-2's decision for their own "
                f"(page, canonical work) key, and no D-17 audit row justifies the "
                f"difference"
            )
        if unused:
            detail.append(
                f"{len(unused)} D-17 audit row(s) record a demotion that no assembled "
                f"track1_direct row reflects, so the trail is not replayable from the "
                f"shipped artifact"
            )
        raise RoutingIngestError(
            "routing parity failed (per-key, so a compensating pair of errors cannot "
            "hide in a total): " + "; ".join(detail)
        )
    if not checked:
        raise RoutingIngestError(
            "zero assembled track1_direct rows were compared, so parity verified "
            "nothing -- a gate that passes over an empty population is a false green"
        )
    return {
        "checked": checked,
        "mismatches": 0,
        # Reported apart so `checked` is never read as N rows of INDEPENDENT
        # agreement. Only `checked_independent` is that; `checked_regrained` is
        # self-consistency, because `regrain_router_to_split` wrote those
        # `expected_status` entries into the same dict this gate reads.
        "checked_independent": checked_independent,
        "checked_regrained": checked_regrained,
    }


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
