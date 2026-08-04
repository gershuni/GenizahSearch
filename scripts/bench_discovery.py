# -*- coding: utf-8 -*-
"""PERF-01 benchmark probe for the Discovery Data Spine (Phase 134, plan 134-08).

Measures, over the REAL ``discovery.db`` sidecar (the EXACT manifest-resolved
filename -- never a shell ``*`` glob, N6) *through* the async
``shared.discovery_service.DiscoveryService`` chokepoint:

  1. p95 query latency of a browse-enrichment-scale read
     (``get_claims_for_page`` / ``get_pages_related_to_page``) and a
     work/leads-scale paginated read (``get_work_witnesses``);
  2. the added RSS of loading + warm-querying the sidecar (RSS sampled before
     the sidecar load and after a warm query burst; the delta is reported).

Flag-bypass (F14): the benchmark injects a BENCHMARK-ONLY availability
predicate ``lambda: web.discovery_assets._state.ready`` -- i.e. the loader's
readiness WITHOUT the ``DISCOVERY_ENABLED`` UI flag (which is OFF this phase).
This bypasses the flag WITHOUT ever setting or monkeypatching it, so the
benchmark exercises REAL query work even though no UI ships in Phase 134.

Nonzero-result assertion (F14): every measured query is driven with a KNOWN
key drawn live from the real DB (a page_id / work_id that provably has shipped
rows), and each call is asserted to return NONZERO rows -- so the benchmark can
never silently measure an empty no-op. Executed-query counts + nonzero-row
counts are printed alongside latency / RSS.

Latency isolation: browse-path latency is measured with the browse LRU
DISABLED (``DISCOVERY_BROWSE_LRU_MAX_ENTRIES=0`` -- one of the service's OWN
tunable knobs, NOT the UI flag) so every timed call is a real cache-miss DB
query (worst case; the production cache only lowers this). The RSS burst then
runs with the default cache ON so the reported delta reflects the real
production working set (sidecar + connection + populated LRU).

Run:  PYTHONUTF8=1 python scripts/bench_discovery.py
"""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import platform
import re
import sqlite3
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

# NOTE: web/ and shared/ imports are deferred INTO functions (never at module
# top) so this file imports with zero heavy side effects -- the plan's
# `exec_module` verify only loads the module + checks the budgets doc.


# ---------------------------------------------------------------------------
# Portable current-RSS reader (stdlib-first; guards Windows dev-box vs Linux
# prod box). Returns bytes, or -1 if no method is available.
# ---------------------------------------------------------------------------

def get_rss_bytes() -> int:
    # 1. psutil (most accurate current RSS, if installed)
    try:
        import psutil  # type: ignore

        return int(psutil.Process().memory_info().rss)
    except Exception:
        pass
    # 2. Linux /proc (current VmRSS)
    try:
        with open("/proc/self/status", "r", encoding="ascii", errors="ignore") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except Exception:
        pass
    # 3. Windows psapi WorkingSetSize (current RSS-equivalent)
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            class _PMC(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = _PMC()
            counters.cb = ctypes.sizeof(_PMC)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            ):
                return int(counters.WorkingSetSize)
        except Exception:
            pass
    # 4. resource fallback (PEAK, not current -- last resort)
    try:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(peak) if sys.platform == "darwin" else int(peak) * 1024
    except Exception:
        pass
    return -1


def _mb(n_bytes: float) -> float:
    return n_bytes / (1024.0 * 1024.0)


def _pct(values: List[float], p: float) -> float:
    """Nearest-rank percentile (ceil), returning the sampled value in ms."""
    if not values:
        return 0.0
    s = sorted(values)
    k = int(math.ceil((p / 100.0) * len(s))) - 1
    k = max(0, min(len(s) - 1, k))
    return s[k]


# ---------------------------------------------------------------------------
# Live-key discovery: pick real page_ids / work_ids that provably return rows
# through the SHIPPED-only default read path, so the nonzero-result assertion
# is meaningful (never a hand-fabricated key that happens to hit nothing).
# ---------------------------------------------------------------------------

def pick_live_keys(db_path: str, sample: int) -> Dict[str, List[Any]]:
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        # Pages with a SHIPPED display-evidence witness/quote claim -> get_claims_for_page.
        claim_pages = [
            r["page_id"]
            for r in conn.execute(
                """
                SELECT dc.page_id AS page_id
                FROM discovery_claim dc
                JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
                WHERE de.routing_status = 'shipped'
                GROUP BY dc.page_id
                LIMIT ?
                """,
                (sample,),
            ).fetchall()
        ]

        # Pages appearing in a SHIPPED shared_text alignment -> get_pages_related_to_page.
        related_pages = [
            r["page_id"]
            for r in conn.execute(
                """
                SELECT a_page_id AS page_id
                FROM discovery_evidence
                WHERE evidence_kind = 'shared_text'
                  AND routing_status = 'shipped'
                  AND a_page_id IS NOT NULL
                GROUP BY a_page_id
                LIMIT ?
                """,
                (sample,),
            ).fetchall()
        ]

        # Works with witness claims -> get_work_witnesses. Bias toward the
        # heaviest works (largest claim counts) so the p95 reflects the true
        # worst case, not just tiny works.
        work_ids = [
            r["work_id"]
            for r in conn.execute(
                """
                SELECT dc.work_id AS work_id, COUNT(*) AS n
                FROM discovery_claim dc
                WHERE dc.claim_type IN ('direct_witness', 'quotes_this_work')
                GROUP BY dc.work_id
                ORDER BY n DESC
                LIMIT ?
                """,
                (sample,),
            ).fetchall()
        ]
        return {
            "claim_pages": claim_pages,
            "related_pages": related_pages,
            "work_ids": work_ids,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 136-11 Task 3: the corpus-wide findings page ("Computed Identifications").
#
# Measured DIRECTLY against the built asset rather than through
# ``DiscoveryService``: the findings read path does not exist on the service yet
# (it is wired later in Phase 136), so what is benchmarked here is the SQL the
# surface will issue against the materialized ``discovery_identification`` grain.
#
# This probe exists because the pre-materialization shape was MEASURED to fail:
# see ``_PRIOR_ORDERING_MEASUREMENT`` / ``_PRIOR_COUNT_MEASUREMENT`` below.
# ---------------------------------------------------------------------------

# docs/specs/discovery-budgets.md §5 (Amendment 2026-08-02). READ, never
# rewritten by this script: a shape that exceeds its cap is reported and the run
# fails -- relaxing a cap requires versioning the budget artifact (T-136-11-06).
FINDINGS_ORDERING_CAP_MS = 1500.0
FINDINGS_COUNT_CAP_MS = 500.0

# The known FAILING baseline this materialization exists to beat. Quoted
# verbatim in the regression assertion message -- a performance assertion that
# says what the number used to be is worth several that do not.
_PRIOR_ORDERING_MEASUREMENT = (
    "3.41-3.55 s across four runs (D-10a), against the 1.5 s cap, when the same "
    "ordering was computed over display CLAIMS with no materialized band_rank / "
    "coverage_ppm and no identification grain"
)
_PRIOR_COUNT_MEASUREMENT = (
    "16 s for the deduped identification COUNT alone (main-pool-rule.md finding "
    "13, \"PERF-01 confirmed twice\")"
)

_FINDINGS_REQUIRED_TABLES = ("discovery_identification", "manuscript_display")

# The default findings ordering: main pool first, tier-first within it, then
# coverage. Mirrors the composite index
# discovery_identification(main_pool, best_band_rank, max_coverage_ppm).
_FINDINGS_ORDER_BY = (
    "ORDER BY di.main_pool DESC, di.best_band_rank ASC, "
    "di.max_coverage_ppm IS NULL, di.max_coverage_ppm DESC, di.identification_id ASC"
)

_FINDINGS_SELECT = """
SELECT di.identification_id, di.sys_id, di.display_work_id, di.main_pool,
       di.best_band_rank, di.max_coverage_ppm, di.relation_kind,
       md.library_code, md.shelfmark_display
FROM discovery_identification di
LEFT JOIN manuscript_display md ON md.sys_id = di.sys_id
"""


def findings_probe_readiness(db_path: str) -> Dict[str, Any]:
    """Whether the asset carries the tables the findings probe needs.

    Against a PRE-rebuild asset the materialized tables simply do not exist yet.
    That is an expected state, not a crash: this returns a structured skip with
    the missing table names so the caller can say exactly which shapes it did
    not measure and why."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        present = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
    finally:
        conn.close()
    missing = [t for t in _FINDINGS_REQUIRED_TABLES if t not in present]
    if missing:
        return {
            "ready": False,
            "missing_tables": missing,
            "reason": (
                "the materialized findings tables are absent from this asset ("
                + ", ".join(missing)
                + ") -- this is a PRE-REBUILD asset; the findings shapes are "
                "measurable only after the Phase-136 rebuild materializes them"
            ),
        }
    return {"ready": True, "missing_tables": [], "reason": ""}


def pick_findings_filters(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Live filter VALUES drawn from the asset itself, so the filtered shapes
    below can never silently benchmark an empty query (the same nonzero-result
    discipline ``pick_live_keys`` already applies to the browse/work reads).

    Novelty prefers ``fills_gap`` -- the value the public "Candidates for new
    finds" toggle actually selects -- and otherwise falls back to the most
    frequent status present, so the shape is still measured on a pre-novelty
    asset instead of being skipped.

    **Every value is drawn from the MAIN POOL where one exists there.** A value
    picked globally can be empty inside the bucket the combination filters on,
    which turns a legitimate measurement into a zero-row abort -- observed
    against the synthetic build fixture, whose most frequent genre carries no
    main-pool rows at all. The global count is kept as the fallback so a
    main-pool-empty asset still measures something rather than skipping."""
    out: Dict[str, Any] = {"novelty_status": None, "relation_kind": None, "domain": None}

    def _pick(sql_body: str, prefer: Optional[str] = None) -> Any:
        for predicate in ("di.main_pool = 1", "1 = 1"):
            rows = conn.execute(sql_body.format(predicate=predicate)).fetchall()
            rows = [r for r in rows if r[0] is not None and r[0] != ""]
            if not rows:
                continue
            preferred = [r for r in rows if r[0] == prefer] if prefer else []
            return (preferred or rows)[0][0]
        return None

    out["novelty_status"] = _pick(
        "SELECT di.novelty_status, COUNT(*) n FROM discovery_identification di "
        "WHERE {predicate} GROUP BY di.novelty_status ORDER BY n DESC",
        prefer="fills_gap",
    )
    out["relation_kind"] = _pick(
        "SELECT di.relation_kind, COUNT(*) n FROM discovery_identification di "
        "WHERE {predicate} GROUP BY di.relation_kind ORDER BY n DESC",
    )
    # D-19/A-6: the domain facet cascades on the IDENTIFIED WORK's domain, never
    # the manuscript's catalogue domain. `works.genre` is NULL corpus-wide until
    # the 136-09 curation pass lands, so this shape skips cleanly until then.
    out["domain"] = _pick(
        "SELECT w.genre, COUNT(*) n FROM discovery_identification di "
        "JOIN works w ON w.work_id = di.display_work_id "
        "WHERE w.genre IS NOT NULL AND w.genre != '' AND {predicate} "
        "GROUP BY w.genre ORDER BY n DESC",
    )
    return out


def _time_sql(conn: sqlite3.Connection, sql: str, params, repeats: int) -> Dict[str, Any]:
    latencies_ms: List[float] = []
    rows = 0
    for _ in range(max(1, repeats)):
        t0 = time.perf_counter()
        fetched = conn.execute(sql, params).fetchall()
        latencies_ms.append((time.perf_counter() - t0) * 1000.0)
        rows = len(fetched)
    return {
        "rows": rows,
        "p50_ms": _pct(latencies_ms, 50),
        "p95_ms": _pct(latencies_ms, 95),
        "max_ms": max(latencies_ms),
    }


def _query_plan(conn: sqlite3.Connection, sql: str, params) -> str:
    try:
        plan = conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
    except sqlite3.Error as exc:  # pragma: no cover -- diagnostic path only
        return f"(query plan unavailable: {exc})"
    return "\n".join("  " + " ".join(str(c) for c in row) for row in plan)


def artifact_provenance(db_path: str) -> Dict[str, Any]:
    """The artifact a number was measured on, and its AUDIENCE.

    The public projection and the private rebuild are different databases with
    different row counts that report the IDENTICAL ``sidecar_version`` string, so
    a timing without its artifact is not comparable to the next one (ruling U's
    basis correction is the same defect one layer up)."""
    out = {"basename": os.path.basename(db_path), "audience": None,
           "sidecar_version": None, "data_as_of": None,
           "size_mb": round(_mb(os.path.getsize(db_path)), 1)}
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = conn.execute("SELECT key, value FROM meta").fetchall()
    except sqlite3.Error:                                # pragma: no cover
        rows = []
    finally:
        conn.close()
    meta = {k: v for k, v in rows}
    for key in ("audience", "sidecar_version", "data_as_of"):
        out[key] = meta.get(key)
    return out


#: Combinations the SURFACE cannot issue, named with the reason rather than
#: silently omitted. Both were in the plan's enumeration; neither is reachable.
_FINDINGS_OUT_OF_SCOPE: Tuple[Tuple[str, str], ...] = (
    ("findings_coverage_filter",
     "the findings service exposes NO coverage predicate -- `get_findings_enveloped` "
     "takes unit/bucket/novelty/domain/author/work_id/sort/page only, and the page "
     "renders the coverage control visibly disabled and tagged for exactly that "
     "reason. Measuring a coverage filter here would time a query the surface "
     "cannot issue"),
    ("findings_relation_filter",
     "D-16 was ratified 2026-08-02: the findings page ships WITHOUT a relation "
     "filter, and `_build_findings_query` carries no relation predicate. The "
     "pre-136-14 probe measured one against hand-written SQL that mirrored a "
     "surface which does not exist"),
)


def _findings_combination_specs(conn, *, page_size: int, deep_page: int,
                                filters: Dict[str, Any], total_rows: int
                                ) -> List[Dict[str, Any]]:
    """The FULL combination space, built through the SHIPPED query builder.

    Closes 136-14's owed follow-up: the probe no longer mirrors the findings
    service in hand-written SQL. `_build_findings_query` is the exact builder
    `DiscoveryService.get_findings_enveloped` calls, so the two can no longer
    diverge -- which is what a benchmark measuring a near-copy cannot promise.

    Enumerated: every ROW UNIT x every SORT MODE x every meaningful FILTER STATE
    (main pool, the SECOND BUCKET, novelty on, a domain leaf selected), plus the
    bounded COUNT query per unit against its own separate cap, plus a deep page
    per unit, plus ruling U's launch-statistics queries.
    """
    from shared.discovery_service import (
        BUCKET_MAIN,
        BUCKET_MORE,
        FINDINGS_SORTS,
        FINDINGS_UNIT_WORK,
        FINDINGS_UNITS,
        _build_findings_query,
        _build_launch_contribution_sql,
        _build_launch_manuscript_sql,
    )

    units = sorted(FINDINGS_UNITS)
    sorts = sorted(FINDINGS_SORTS)
    novelty = [filters["novelty_status"]] if filters["novelty_status"] else None
    domain = filters["domain"]

    def _population(where: str, params: Tuple[Any, ...] = ()) -> int:
        """How many identifications this asset carries for a POPULATION.

        Used only for populations that are properties of the ASSET (which
        buckets it has rows in; whether it carries any contribution shade), so a
        combination that cannot exist here is a NAMED SKIP. A zero row count on
        a combination whose filter value was PICKED from this same asset stays
        the loud abort F14 requires -- that one is a probe bug, not an asset
        fact, and the distinction is the whole point.
        """
        try:
            return int(conn.execute(
                f"SELECT COUNT(*) FROM discovery_identification di WHERE {where}",
                params).fetchone()[0])
        except sqlite3.Error:                                # pragma: no cover
            return 0

    bucket_population = {
        "main": _population("di.main_pool = 1"),
        "more": _population("di.main_pool = 0"),
    }

    filter_states: List[Tuple[str, Dict[str, Any], Optional[str]]] = [
        ("main", {"bucket": BUCKET_MAIN},
         None if bucket_population["main"] else
         "this asset carries no main-pool identifications"),
        # Ruling T: the second bucket is a FIRST-CLASS benchmarked state, not an
        # afterthought. It is roughly the same order of magnitude as the main
        # pool and a reader is expected to use it.
        ("more", {"bucket": BUCKET_MORE},
         None if bucket_population["more"] else
         "this asset carries no second-bucket identifications"),
        ("novelty", {"bucket": BUCKET_MAIN, "novelty": novelty},
         None if (novelty and bucket_population["main"])
         else "no novelty_status value present in this asset's main pool"),
        ("domain", {"bucket": BUCKET_MAIN, "domain": domain},
         None if (domain and bucket_population["main"]) else (
             "works.genre carries no value in this asset's main pool, so the "
             "domain facet has nothing to filter on")),
    ]

    specs: List[Dict[str, Any]] = []
    for unit in units:
        for sort in sorts:
            for state, kwargs, skip in filter_states:
                label = f"findings_{unit}_{sort}_{state}"
                if state == "novelty" and unit == FINDINGS_UNIT_WORK:
                    specs.append({
                        "label": label, "kind": "ordering",
                        "cap_ms": FINDINGS_ORDERING_CAP_MS, "sql": "", "params": (),
                        "skip": "novelty is not offered on the per-work unit -- a "
                                "work spanning many manuscripts has no single "
                                "verdict, and the service RAISES rather than "
                                "returning an envelope",
                    })
                    continue
                if skip:
                    specs.append({"label": label, "kind": "ordering",
                                  "cap_ms": FINDINGS_ORDERING_CAP_MS, "sql": "",
                                  "params": (), "skip": skip})
                    continue
                sql, params = _build_findings_query(
                    unit=unit, sort=sort, page=1, page_size=page_size, **kwargs)
                specs.append({
                    "label": label, "kind": "ordering",
                    "cap_ms": FINDINGS_ORDERING_CAP_MS, "sql": sql, "params": params,
                    "skip": None if total_rows else "the asset carries no identifications",
                })

        # Deep paging -- where an ordering index earns its keep, and the shape a
        # spot check at page 1 will never expose.
        #
        # The depth bound is the count AT THIS UNIT, not the identification
        # count: the per-work unit groups ~1,000x fewer rows, so a bound taken
        # from the identification grain says "deep paging is measurable" for a
        # unit whose whole result set fits on page 13. The nonzero-result
        # discipline caught exactly that.
        count_sql, count_params = _build_findings_query(
            unit=unit, bucket=BUCKET_MAIN, count_only=True)
        try:
            unit_rows = int(conn.execute(count_sql, count_params).fetchone()[0])
        except sqlite3.Error:                                # pragma: no cover
            unit_rows = 0
        sql, params = _build_findings_query(
            unit=unit, bucket=BUCKET_MAIN, page=deep_page, page_size=page_size)
        specs.append({
            "label": f"findings_{unit}_deep_page_{deep_page}", "kind": "ordering",
            "cap_ms": FINDINGS_ORDERING_CAP_MS, "sql": sql, "params": params,
            "skip": None if unit_rows > (deep_page - 1) * page_size else (
                f"the {unit} unit carries only {unit_rows} rows in the main pool "
                f"-- fewer than the page-{deep_page} offset, so deep paging cannot "
                "be measured on a nonzero result set"),
        })

        # The visible COUNT, against its own SEPARATE cap (§5). Measured in the
        # bounded form the surface issues when DISCOVERY_FINDINGS_COUNT_MAX is
        # set; with the knob off the total rides on COUNT(*) OVER () inside the
        # ordering query above and costs no second statement.
        sql, params = _build_findings_query(
            unit=unit, bucket=BUCKET_MAIN, count_only=True)
        specs.append({
            "label": f"findings_{unit}_visible_total", "kind": "count",
            "cap_ms": FINDINGS_COUNT_CAP_MS, "sql": sql, "params": params,
            "skip": None if total_rows else "the asset carries no identifications",
        })

    # Ruling U's launch statistics -- both halves of the one grouped statement,
    # plus the distinct-manuscript count that is NOT derivable by summing them.
    from shared.discovery_service import LAUNCH_CONTRIBUTION_SHADES

    shade_placeholders = ",".join("?" * len(LAUNCH_CONTRIBUTION_SHADES))
    shade_population = {
        "main_pool": _population(
            f"di.main_pool = 1 AND di.novelty_status IN ({shade_placeholders})",
            tuple(LAUNCH_CONTRIBUTION_SHADES)),
        "all_bucket": _population(
            f"di.novelty_status IN ({shade_placeholders})",
            tuple(LAUNCH_CONTRIBUTION_SHADES)),
    }
    for main_pool_only in (True, False):
        basis = "main_pool" if main_pool_only else "all_bucket"
        sql, params = _build_launch_contribution_sql(main_pool_only=main_pool_only)
        specs.append({
            "label": f"findings_launch_contribution_{basis}",
            "kind": "count", "cap_ms": FINDINGS_COUNT_CAP_MS,
            "sql": sql, "params": params,
            "skip": None if shade_population[basis] else (
                "this asset carries no identification in any ruling-U "
                f"contribution shade on the {basis} basis"),
        })
    sql, params = _build_launch_manuscript_sql(main_pool_only=True)
    specs.append({
        "label": "findings_launch_manuscripts_main_pool", "kind": "count",
        "cap_ms": FINDINGS_COUNT_CAP_MS, "sql": sql, "params": params,
        "skip": None if shade_population["main_pool"] else (
            "this asset carries no identification in any ruling-U contribution "
            "shade in the main pool"),
    })
    return specs


def bench_findings_page(
    db_path: str, *, page_size: int = 50, repeats: int = 5, deep_page: int = 20
) -> Dict[str, Any]:
    """Measure the FULL corpus-wide findings combination space.

    Every ROW UNIT x every SORT MODE x every meaningful FILTER STATE (main pool,
    the SECOND BUCKET, novelty on, a domain leaf), plus the bounded count query
    per unit against its own separate cap, plus a deep page per unit, plus
    ruling U's launch-statistics queries -- all built through the SHIPPED
    `_build_findings_query`, never a hand-written mirror of it.

    Every combination asserts a NONZERO row count before its timing is recorded,
    and a combination whose live filter value or page depth does not exist in
    this asset is SKIPPED with a stated reason rather than measured as an empty
    no-op. Combinations the SURFACE cannot issue are named with their reason too.

    Returns a structured result; the caller decides the exit code. Caps are READ
    from docs/specs/discovery-budgets.md §5 and never rewritten here."""
    readiness = findings_probe_readiness(db_path)
    if not readiness["ready"]:
        return {
            "skipped": True,
            "reason": readiness["reason"],
            "missing_tables": readiness["missing_tables"],
            "artifact": artifact_provenance(db_path),
            "combinations": 0,
            "shapes": [],
            "skipped_shapes": [
                {"label": label, "reason": readiness["reason"]}
                for label in ("findings_combination_space",)
            ],
            "out_of_scope": [{"label": label, "reason": reason}
                             for label, reason in _FINDINGS_OUT_OF_SCOPE],
            "failures": [],
        }

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        (total_rows,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_identification"
        ).fetchone()
        filters = pick_findings_filters(conn)
        specs = _findings_combination_specs(
            conn, page_size=page_size, deep_page=deep_page, filters=filters,
            total_rows=total_rows)

        shapes: List[Dict[str, Any]] = []
        skipped_shapes: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        for spec in specs:
            if spec["skip"]:
                skipped_shapes.append({"label": spec["label"], "reason": spec["skip"]})
                continue
            measured = _time_sql(conn, spec["sql"], spec["params"], repeats)
            if measured["rows"] == 0:
                raise AssertionError(
                    f"{spec['label']}: the measured query returned ZERO rows "
                    "(the benchmark must never silently measure an empty no-op -- "
                    "F14). Fix the live filter selection rather than recording "
                    "the timing."
                )
            row = {
                "label": spec["label"],
                "kind": spec["kind"],
                "cap_ms": spec["cap_ms"],
                **measured,
            }
            if measured["p95_ms"] > spec["cap_ms"]:
                row["query_plan"] = _query_plan(conn, spec["sql"], spec["params"])
                failures.append(row)
            shapes.append(row)

        return {
            "skipped": False,
            "reason": "",
            "missing_tables": [],
            "artifact": artifact_provenance(db_path),
            "identifications": total_rows,
            "filters": filters,
            "page_size": page_size,
            "deep_page": deep_page,
            "combinations": len(specs),
            "shapes": shapes,
            "skipped_shapes": skipped_shapes,
            "out_of_scope": [{"label": label, "reason": reason}
                             for label, reason in _FINDINGS_OUT_OF_SCOPE],
            "failures": failures,
        }
    finally:
        conn.close()


def report_findings_page(result: Dict[str, Any]) -> None:
    """Print the findings probe result, including a NAMED reason for every
    combination that was not measured."""
    artifact = result.get("artifact") or {}
    print("-" * 78)
    print("Findings page (Computed Identifications) -- the FULL combination space")
    print("-" * 78)
    print(f"artifact         : {artifact.get('basename')} "
          f"({artifact.get('size_mb')} MB)")
    print(f"audience         : {artifact.get('audience')!r}   "
          f"sidecar_version: {artifact.get('sidecar_version')!r}   "
          f"data_as_of: {artifact.get('data_as_of')!r}")
    if result["skipped"]:
        print(f"SKIPPED: {result['reason']}")
        for s in result["skipped_shapes"]:
            print(f"  - {s['label']}: not measured")
        return

    print(f"identifications  : {result['identifications']}")
    print(f"filters in use   : {result['filters']}")
    print(f"combinations     : {result['combinations']} enumerated, "
          f"{len(result['shapes'])} measured, "
          f"{len(result['skipped_shapes'])} skipped, "
          f"{len(result.get('out_of_scope') or ())} out of scope")
    print(f"{'combination':<46}{'rows':>6}{'p50 ms':>9}{'p95 ms':>9}"
          f"{'max ms':>9}{'cap ms':>8}  result")
    for r in result["shapes"]:
        verdict = "FAIL" if r in result["failures"] else "PASS"
        print(
            f"{r['label']:<46}{r['rows']:>6}{r['p50_ms']:>9.2f}{r['p95_ms']:>9.2f}"
            f"{r['max_ms']:>9.2f}{r['cap_ms']:>8.0f}  {verdict}"
        )
    for s in result["skipped_shapes"]:
        print(f"  - {s['label']}: SKIPPED -- {s['reason']}")
    for s in result.get("out_of_scope") or ():
        print(f"  - {s['label']}: OUT OF SCOPE -- {s['reason']}")

    for r in result["failures"]:
        print()
        print(
            f"FAIL {r['label']}: p95 {r['p95_ms']:.2f} ms exceeds its "
            f"{r['cap_ms']:.0f} ms cap.\n"
            f"  This shape previously measured {_PRIOR_ORDERING_MEASUREMENT}, and "
            f"{_PRIOR_COUNT_MEASUREMENT}.\n"
            "  The cap is NOT relaxed to make this pass -- a cap change requires "
            "versioning docs/specs/discovery-budgets.md. SQLite query plan:",
            file=sys.stderr,
        )
        print(r.get("query_plan", "(unavailable)"), file=sys.stderr)


# ---------------------------------------------------------------------------
# Async measurement over the DiscoveryService chokepoint.
# ---------------------------------------------------------------------------

async def _time_calls(
    label: str,
    keys: List[Any],
    call: Callable[[Any], "asyncio.Future"],
) -> Dict[str, Any]:
    """Await ``call(key)`` for each key, timing each; assert nonzero rows."""
    latencies_ms: List[float] = []
    total_rows = 0
    empties = 0
    for key in keys:
        t0 = time.perf_counter()
        rows = await call(key)
        latencies_ms.append((time.perf_counter() - t0) * 1000.0)
        n = len(rows or [])
        total_rows += n
        if n == 0:
            empties += 1
    if empties:
        raise AssertionError(
            f"{label}: {empties}/{len(keys)} measured queries returned ZERO rows "
            f"(benchmark must never measure an empty no-op -- F14)"
        )
    return {
        "label": label,
        "queries": len(keys),
        "rows": total_rows,
        "p50_ms": _pct(latencies_ms, 50),
        "p95_ms": _pct(latencies_ms, 95),
        "max_ms": max(latencies_ms) if latencies_ms else 0.0,
    }


async def run_benchmark(service, keys: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    results = []
    results.append(
        await _time_calls(
            "get_claims_for_page",
            keys["claim_pages"],
            lambda k: service.get_claims_for_page_async(k, page=1, page_size=50),
        )
    )
    results.append(
        await _time_calls(
            "get_pages_related_to_page",
            keys["related_pages"],
            lambda k: service.get_pages_related_to_page_async(k, page=1, page_size=50),
        )
    )
    results.append(
        await _time_calls(
            "get_work_witnesses",
            keys["work_ids"],
            lambda k: service.get_work_witnesses_async(k, page=1, page_size=50),
        )
    )
    return results


async def warm_burst(service, keys: Dict[str, List[Any]], passes: int) -> int:
    """Warm the connection + LRU by re-querying every key `passes` times.
    Returns the total rows touched (a nonzero-result sanity signal)."""
    touched = 0
    for _ in range(max(1, passes)):
        for k in keys["claim_pages"]:
            touched += len(await service.get_claims_for_page_async(k, page=1, page_size=50) or [])
        for k in keys["related_pages"]:
            touched += len(await service.get_pages_related_to_page_async(k, page=1, page_size=50) or [])
        for k in keys["work_ids"]:
            touched += len(await service.get_work_witnesses_async(k, page=1, page_size=50) or [])
    return touched


def main() -> int:
    parser = argparse.ArgumentParser(description="PERF-01 discovery.db benchmark probe")
    parser.add_argument(
        "--sample", type=int, default=200,
        help="max distinct live keys per query family (default 200)",
    )
    parser.add_argument(
        "--warm-passes", type=int, default=2,
        help="warm-burst passes over every key for the RSS delta (default 2)",
    )
    parser.add_argument(
        "--write-budgets", action="store_true",
        help="record the measured dev-box actuals into docs/specs/discovery-budgets.md "
             "(including the findings-page actuals table; caps are never rewritten)",
    )
    parser.add_argument(
        "--findings-repeats", type=int, default=5,
        help="timed repeats per corpus-wide findings shape (default 5)",
    )
    parser.add_argument(
        "--findings-page-size", type=int, default=50,
        help="findings rows/page (default 50, matching DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT)",
    )
    parser.add_argument(
        "--findings-deep-page", type=int, default=20,
        help="which pager page to measure for deep paging (default 20)",
    )
    parser.add_argument(
        "--findings-db", default=None,
        help="measure the findings combination space against THIS artifact "
             "instead of the manifest-resolved one. The findings probe opens "
             "SQLite read-only and needs no loader, while the service-level "
             "benchmark does -- and the loader is fail-closed, so it refuses an "
             "artifact the repository manifest does not select. Every recorded "
             "number carries the artifact and audience it came from.",
    )
    parser.add_argument(
        "--findings-only", action="store_true",
        help="run ONLY the findings combination space (skip the service-level "
             "browse/work benchmark, which requires a manifest-resolved sidecar)",
    )
    args = parser.parse_args()

    # The repo root goes on sys.path BEFORE any branch: the findings probe
    # imports the SHIPPED query builder from `shared/`, and it must be able to
    # do so on the `--findings-only` path too.
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    if args.findings_only:
        db_path = args.findings_db
        if not db_path or not os.path.exists(db_path):
            print(f"FAIL: --findings-only needs --findings-db <path>; got {db_path!r}",
                  file=sys.stderr)
            return 1
        findings = bench_findings_page(
            db_path,
            page_size=args.findings_page_size,
            repeats=args.findings_repeats,
            deep_page=args.findings_deep_page,
        )
        report_findings_page(findings)
        if args.write_budgets:
            write_findings_budgets(findings)
            print("\nWrote the findings-page MEASURED ACTUALS into "
                  "docs/specs/discovery-budgets.md (caps untouched)")
        return 1 if findings["failures"] else 0

    # RSS baseline sampled BEFORE the sidecar load (the delta then attributes
    # the sidecar + service + populated caches; importing the lightweight
    # web.discovery_assets / shared.discovery_service modules is negligible).
    rss_before = get_rss_bytes()

    # Deferred heavy imports (kept out of module scope; see file docstring).
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from web import discovery_assets
    from shared.discovery_service import DiscoveryService

    if not discovery_assets.load_discovery_state():
        print(
            "FAIL: discovery sidecar did not load (fail-closed). "
            "Check discovery_data/manifest.json + the content-hashed .db are present locally.",
            file=sys.stderr,
        )
        return 1

    db_path = discovery_assets.discovery_db_path()
    if not db_path or not os.path.exists(db_path):
        print(f"FAIL: resolved discovery.db path is absent: {db_path!r}", file=sys.stderr)
        return 1

    # F14: benchmark-only readiness predicate = loader readiness WITHOUT the
    # DISCOVERY_ENABLED UI flag. We never touch/set/monkeypatch that flag.
    service = DiscoveryService(
        path_provider=discovery_assets.discovery_db_path,
        availability_callable=lambda: discovery_assets._state.ready,
        sidecar_version_provider=discovery_assets.discovery_sidecar_version,
    )
    if not service.is_available():
        print("FAIL: DiscoveryService not available under the flag-bypass predicate", file=sys.stderr)
        return 1

    keys = pick_live_keys(db_path, args.sample)
    for fam, ids in keys.items():
        if not ids:
            print(f"FAIL: no live keys found for {fam} (cannot assert nonzero results)", file=sys.stderr)
            return 1

    # --- Latency: LRU DISABLED so every timed call is a real cache-miss query.
    prev_lru = os.environ.get("DISCOVERY_BROWSE_LRU_MAX_ENTRIES")
    os.environ["DISCOVERY_BROWSE_LRU_MAX_ENTRIES"] = "0"
    try:
        latency_results = asyncio.run(run_benchmark(service, keys))
    finally:
        if prev_lru is None:
            os.environ.pop("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", None)
        else:
            os.environ["DISCOVERY_BROWSE_LRU_MAX_ENTRIES"] = prev_lru

    # --- RSS: warm burst with the default cache ON (production working set).
    touched = asyncio.run(warm_burst(service, keys, args.warm_passes))
    rss_after = get_rss_bytes()
    added_rss_bytes = (rss_after - rss_before) if (rss_before >= 0 and rss_after >= 0) else -1

    # ----------------------------------------------------------------- report
    sidecar_size = os.path.getsize(db_path)
    print("=" * 72)
    print("Discovery PERF-01 benchmark (dev-box) -- through DiscoveryService")
    print("=" * 72)
    print(f"sidecar          : {os.path.basename(db_path)}")
    print(f"sidecar size     : {_mb(sidecar_size):.1f} MB on disk")
    print(f"flag bypass      : DISCOVERY_ENABLED untouched; predicate = _state.ready")
    print(f"latency cache    : DISCOVERY_BROWSE_LRU_MAX_ENTRIES=0 (every call a real DB query)")
    print("-" * 72)
    print(f"{'query':<30}{'n':>6}{'rows':>9}{'p50 ms':>10}{'p95 ms':>10}{'max ms':>10}")
    print("-" * 72)
    for r in latency_results:
        print(
            f"{r['label']:<30}{r['queries']:>6}{r['rows']:>9}"
            f"{r['p50_ms']:>10.2f}{r['p95_ms']:>10.2f}{r['max_ms']:>10.2f}"
        )
    print("-" * 72)
    print(f"RSS before load  : {_mb(rss_before):.1f} MB" if rss_before >= 0 else "RSS before load  : (unavailable)")
    print(f"RSS after burst  : {_mb(rss_after):.1f} MB" if rss_after >= 0 else "RSS after burst  : (unavailable)")
    if added_rss_bytes >= 0:
        print(f"added RSS        : {_mb(added_rss_bytes):.1f} MB   (cap <= 250 MB; dev-box, indicative)")
    else:
        print("added RSS        : (unavailable on this platform)")
    print(f"warm-burst rows  : {touched} (nonzero-result sanity: cache path returns real rows)")
    print("=" * 72)

    # Browse-enrichment p95 = the max of the two browse-path reads (PANEL-01/02).
    browse_p95 = max(latency_results[0]["p95_ms"], latency_results[1]["p95_ms"])
    work_p95 = latency_results[2]["p95_ms"]
    print(f"browse-enrichment p95 : {browse_p95:.2f} ms   (cap <= 150 ms)")
    print(f"work-page query p95   : {work_p95:.2f} ms   (informational; work-page request cap <= 1.5 s)")

    # --- Corpus-wide findings page (136-11 Task 3). Measured directly against
    #     the asset; skips CLEANLY (never a bare exception) on a pre-rebuild
    #     asset that has no materialized identification grain.
    findings = bench_findings_page(
        args.findings_db or db_path,
        page_size=args.findings_page_size,
        repeats=args.findings_repeats,
        deep_page=args.findings_deep_page,
    )
    print()
    report_findings_page(findings)

    if args.write_budgets:
        _write_budgets(
            latency_results=latency_results,
            browse_p95=browse_p95,
            work_p95=work_p95,
            added_rss_mb=(_mb(added_rss_bytes) if added_rss_bytes >= 0 else None),
            sidecar_size_mb=_mb(sidecar_size),
            sidecar_basename=os.path.basename(db_path),
            findings=findings,
        )
        print("\nWrote MEASURED ACTUALS (dev-box) into docs/specs/discovery-budgets.md")

    if findings["failures"]:
        # Report and STOP -- never relax a cap to make a slow shape pass.
        return 1
    return 0


def _findings_actuals_block(findings: Optional[Dict[str, Any]]) -> str:
    """The §4.4 findings-page actuals sub-block.

    Records the ORDERING numbers and the visible-COUNT number in SEPARATE rows,
    because §5 gives them separate caps (p95 <= 1.5 s vs p95 <= 0.5 s). Writes
    an explicit PENDING block -- never an invented number -- when the probe
    could not run."""
    if not findings or findings.get("skipped"):
        reason = (findings or {}).get("reason") or "the probe did not run"
        return (
            "### 4.4 Corpus-wide findings page (§5 caps) — PENDING\n\n"
            f"Not yet measurable: {reason}.\n\n"
            "`scripts/bench_discovery.py` carries the `bench_findings_page()` probe\n"
            "(six named shapes: default ordering, novelty filter, relation filter,\n"
            "domain filter, the visible TOTAL count, and deep paging), which records\n"
            "these actuals automatically on the first run against a rebuilt asset.\n"
            "The prior, PRE-materialization measurement this probe must beat is\n"
            f"**{_PRIOR_ORDERING_MEASUREMENT}**, and **{_PRIOR_COUNT_MEASUREMENT}**.\n\n"
        )

    artifact = findings.get("artifact") or {}
    lines = [
        "### 4.4 Corpus-wide findings page (§5 caps) — the FULL combination space, measured\n",
        "",
        f"Measured by `scripts/bench_discovery.py::bench_findings_page()` over "
        f"{findings['identifications']} materialized identifications "
        f"(`discovery_identification`), page size {findings['page_size']}, "
        f"deep page {findings['deep_page']}. "
        f"**{findings.get('combinations', 0)} combinations enumerated, "
        f"{len(findings['shapes'])} measured.** Every combination asserted a "
        "NONZERO row count before its timing was recorded.",
        "",
        "**Artifact, audience and host** — a timing without its artifact is not "
        "comparable to the next one, because the public projection and the "
        "private rebuild are different databases with different row counts that "
        "report the identical `sidecar_version` string; and a laptop measurement "
        "is not a server measurement, which is where a slow query does its "
        "damage on a single-worker box:",
        "",
        f"- artifact: `{artifact.get('basename')}` ({artifact.get('size_mb')} MB)",
        f"- audience: `{artifact.get('audience')}` · sidecar_version: "
        f"`{artifact.get('sidecar_version')}` · data_as_of: "
        f"`{artifact.get('data_as_of')}`",
        f"- host: `{platform.system()} {platform.machine()}` "
        f"({'prod-box class' if platform.system() == 'Linux' else 'dev-box'})",
        "",
        "Every combination is built through the SHIPPED "
        "`shared/discovery_service.py::_build_findings_query` — the exact builder "
        "`get_findings_enveloped` calls — so the probe and the service can no "
        "longer diverge (136-14's owed follow-up, closed). The launch-statistics "
        "rows come from `_build_launch_contribution_sql` / "
        "`_build_launch_manuscript_sql` for the same reason.",
        "",
        "The ordering and the visible-count numbers are recorded SEPARATELY "
        "because §5 gives them separate caps. The prior, PRE-materialization "
        f"measurement was {_PRIOR_ORDERING_MEASUREMENT}, and "
        f"{_PRIOR_COUNT_MEASUREMENT}.",
        "",
        "| Combination | Cap | p50 | p95 | max | Rows | Result |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in findings["shapes"]:
        verdict = "PASS ✓" if r["p95_ms"] <= r["cap_ms"] else "FAIL ✗"
        lines.append(
            f"| `{r['label']}` | p95 ≤ {r['cap_ms']:.0f} ms | {r['p50_ms']:.2f} ms | "
            f"**{r['p95_ms']:.2f} ms** | {r['max_ms']:.2f} ms | {r['rows']} | {verdict} |"
        )
    if findings["skipped_shapes"]:
        lines.append("")
        lines.append("Combinations NOT measured, and why:")
        lines.append("")
        for s in findings["skipped_shapes"]:
            lines.append(f"- `{s['label']}` — {s['reason']}")
    if findings.get("out_of_scope"):
        lines.append("")
        lines.append("Combinations the SURFACE cannot issue, named rather than omitted:")
        lines.append("")
        for s in findings["out_of_scope"]:
            lines.append(f"- `{s['label']}` — {s['reason']}")
    lines.append("")
    return "\n".join(lines)


def write_findings_budgets(findings: Dict[str, Any]) -> None:
    """Record the findings actuals ALONE, leaving every other section of the
    budget document byte-identical.

    A separate entry point from `_write_budgets` because the findings probe can
    run against an explicitly-named artifact while the service-level benchmark
    cannot: the loader is fail-closed and refuses an artifact the repository
    manifest does not select. Rewriting §4.1 from a run that never measured it
    would destroy a real measurement."""
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "docs", "specs", "discovery-budgets.md",
    )
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(_upsert_findings_block(text, _findings_actuals_block(findings)))


def _write_budgets(
    *,
    latency_results: List[Dict[str, Any]],
    browse_p95: float,
    work_p95: float,
    added_rss_mb: Optional[float],
    sidecar_size_mb: float,
    sidecar_basename: str,
    findings: Optional[Dict[str, Any]] = None,
) -> None:
    """Replace the §4 Measured Actuals block in docs/specs/discovery-budgets.md
    with the measured dev-box actuals (prod-box RSS stays a PENDING human step).

    Rewrites the §4 header + §4.1 (this script's own dev-box measurement) and
    the §4.4 findings block, and NOTHING ELSE. Two bugs are fixed here
    (T-136-11-06 -- a benchmark must never edit a cap, nor destroy a
    measurement):

      * the replacement used to run to the next `\\n---\\n`, which since the
        2026-08-02 amendment sits AFTER §5 -- so a `--write-budgets` run would
        have silently DELETED the findings-page CAP section this probe measures
        against;
      * the replacement block also re-wrote §4.2 as "PENDING", which would have
        destroyed the human-recorded prod-box actuals of 2026-07-28.

    §4.2 and §4.3 are now left byte-identical."""
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "docs", "specs", "discovery-budgets.md",
    )
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()

    marker = "## 4. Measured Actuals"
    idx = text.find(marker)
    if idx == -1:
        raise RuntimeError("could not find '## 4. Measured Actuals' section to replace")
    # The §4 region this script owns ends where the FIRST subsection it does not
    # own begins (§4.2, the prod-box actuals). Fall back to the next top-level
    # heading if the document ever loses §4.2.
    tail_idx = text.find("### 4.2", idx)
    if tail_idx == -1:
        next_section = re.search(r"^## (?!4\. )", text[idx + len(marker):], re.MULTILINE)
        tail_idx = (
            idx + len(marker) + next_section.start() if next_section else len(text)
        )

    claims = latency_results[0]
    related = latency_results[1]
    work = latency_results[2]
    rss_line = (
        f"| **Additional RSS (dev-box, sidecar+service+LRU warm)** | ≤ 250 MB | "
        f"**{added_rss_mb:.1f} MB** | dev-box indicative; prod-box authoritative (Task 3) |"
        if added_rss_mb is not None
        else "| Additional RSS (dev-box) | ≤ 250 MB | (unavailable on dev platform) | prod-box authoritative (Task 3) |"
    )

    block = f"""## 4. Measured Actuals — dev-box measured (prod-box PENDING)

Measured 2026-07-23 by `scripts/bench_discovery.py` over the real sidecar
`{sidecar_basename}` ({sidecar_size_mb:.1f} MB on disk), through the
`shared.discovery_service.DiscoveryService` async chokepoint, using a
benchmark-only readiness predicate that ANDs the loader's `_state.ready`
WITHOUT the `DISCOVERY_ENABLED` UI flag (F14 — the flag was never set) and
asserting every measured query returns nonzero rows (never an empty no-op).
Browse-path latency was measured with the browse LRU DISABLED
(`DISCOVERY_BROWSE_LRU_MAX_ENTRIES=0`), so every timed call is a real
cache-miss DB query (worst case; the production cache only lowers this).

### 4.1 Query latency + RSS (dev-box actuals vs §1 caps)

| Metric | Cap | Dev-box actual | Note |
|---|---|---|---|
| Browse-enrichment added latency (p95) | ≤ 150 ms | **{browse_p95:.2f} ms** | max of the two browse reads below; cache OFF (worst case) |
| &nbsp;&nbsp;• `get_claims_for_page` (p95 / max) | — | {claims['p95_ms']:.2f} / {claims['max_ms']:.2f} ms | {claims['queries']} distinct pages, {claims['rows']} rows total |
| &nbsp;&nbsp;• `get_pages_related_to_page` (p95 / max) | — | {related['p95_ms']:.2f} / {related['max_ms']:.2f} ms | {related['queries']} distinct pages, {related['rows']} rows total |
| `get_work_witnesses` query (p95 / max) | (request cap ≤ 1.5 s) | {work['p95_ms']:.2f} / {work['max_ms']:.2f} ms | {work['queries']} works incl. the heaviest; {work['rows']} unit rows total |
{rss_line}

**Executed-query counts (nonzero-result assertion passed for all):**
`get_claims_for_page` = {claims['queries']} queries / {claims['rows']} rows;
`get_pages_related_to_page` = {related['queries']} queries / {related['rows']} rows;
`get_work_witnesses` = {work['queries']} queries / {work['rows']} unit rows.

"""
    text = text[:idx] + block + text[tail_idx:]
    text = _upsert_findings_block(text, _findings_actuals_block(findings))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def _upsert_findings_block(text: str, block: str) -> str:
    """Replace §4.4 in place, or insert it at the END of §4 (immediately before
    the next top-level heading) when it does not exist yet.

    Never touches §5 or any other cap section -- a benchmark that can silently
    rewrite the number it is measured against is not a gate (T-136-11-06)."""
    start = text.find("### 4.4")
    if start != -1:
        rest = text[start + len("### 4.4"):]
        nxt = re.search(r"^(?:### |## )", rest, re.MULTILINE)
        end = start + len("### 4.4") + (nxt.start() if nxt else len(rest))
        return text[:start] + block + text[end:]

    section4 = text.find("## 4. Measured Actuals")
    if section4 == -1:
        return text + "\n" + block
    rest = text[section4 + len("## 4. Measured Actuals"):]
    nxt = re.search(r"^## ", rest, re.MULTILINE)
    insert_at = (
        section4 + len("## 4. Measured Actuals") + (nxt.start() if nxt else len(rest))
    )
    return text[:insert_at] + block.rstrip("\n") + "\n\n" + text[insert_at:]


if __name__ == "__main__":
    raise SystemExit(main())
