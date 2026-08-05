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
    out["buckets"] = {
        bucket: _coherent_bucket_pick(
            conn, main_pool=(bucket == "main"), prefer_novelty=out["novelty_status"])
        for bucket in ("main", "more")
    }
    return out


def _coherent_bucket_pick(conn: sqlite3.Connection, *, main_pool: bool,
                          prefer_novelty: Optional[str]) -> Dict[str, Any]:
    """One representative identification per bucket -- domain, author and work
    taken from the SAME row, plus a novelty status present in that bucket.

    Picked together rather than axis by axis on purpose. A globally-frequent
    genre, a globally-frequent author and a heavy work chosen INDEPENDENTLY need
    not co-occur on any identification, and their AND-composition then measures
    nothing while looking measured -- the same class of defect as recording a
    count of zero as a pass. The work chosen is the HEAVIEST one satisfying the
    other preferences, so a work-filtered timing is a worst case rather than a
    lucky singleton.

    The preference order is stated in the ORDER BY: a work carrying rows in the
    candidate novelty status first (that switch is the page's headline), then
    one with a domain, then one with an author, then the heaviest.

    EVERY returned value is present in THIS bucket, which is what lets a
    single-axis filter state keep F14's loud abort: if such a state measures
    nothing, the probe picked a value the asset does not have, and that is a
    probe bug rather than an asset fact. Only the CO-OCCURRENCE of two or more
    axes is an asset fact, and only that is settled by an EXISTS probe.
    """
    bucket_sql = "di.main_pool = 1" if main_pool else "di.main_pool = 0"
    out: Dict[str, Any] = {"work_id": None, "domain": None, "author": None,
                           "novelty_status": None}
    try:
        row = conn.execute(
            f"""
            SELECT di.display_work_id AS work_id,
                   w.genre            AS genre,
                   w.author           AS author,
                   SUM(CASE WHEN di.novelty_status = ? THEN 1 ELSE 0 END) AS candidates,
                   COUNT(*)           AS n
            FROM discovery_identification di
            JOIN works w ON w.work_id = di.display_work_id
            WHERE {bucket_sql}
            GROUP BY di.display_work_id
            ORDER BY (candidates > 0) DESC,
                     (w.genre IS NOT NULL AND w.genre != '') DESC,
                     (w.author IS NOT NULL AND w.author != '') DESC,
                     n DESC
            LIMIT 1
            """,
            (prefer_novelty,),
        ).fetchone()
        # The novelty status is picked for the BUCKET, not globally: the
        # candidate status the page's headline switch selects may be absent from
        # one bucket while present in the other, and a globally-picked value
        # would make every novelty state in that bucket measure nothing.
        statuses = conn.execute(
            f"SELECT di.novelty_status, COUNT(*) n FROM discovery_identification di "
            f"WHERE {bucket_sql} AND di.novelty_status IS NOT NULL "
            f"AND di.novelty_status != '' GROUP BY di.novelty_status ORDER BY n DESC"
        ).fetchall()
    except sqlite3.Error:                                    # pragma: no cover
        return out
    if row is not None:
        out.update({"work_id": row[0], "domain": row[1] or None,
                    "author": row[2] or None})
    preferred = [s for s in statuses if s[0] == prefer_novelty]
    if preferred or statuses:
        out["novelty_status"] = (preferred or statuses)[0][0]
    return out


# ---------------------------------------------------------------------------
# WHAT POPULATION DID THIS TIMING MEASURE?
#
# The benchmark's one substantive assertion is that a timing was taken over a
# NON-EMPTY population. For a row query that is the row count; for an aggregate
# it is the VALUE, because `SELECT COUNT(*) FROM empty_table` returns one row
# whatever it counts and a row-count assertion therefore passes on nothing
# (round 13, finding 3).
#
# THE AUTHORITY IS NO LONGER THE STATEMENT TEXT. Two derivations tried to read
# the shape out of the SQL and each closed only the syntaxes its review had
# named:
#
#   * a regex -- "starts with SELECT COUNT(" and has no outer GROUP BY -- which
#     round 15, finding 3 broke with a CTE prefix, an outer wrapper and a
#     window function;
#   * a parenthesis-depth walker, which round 16 broke with `CASE WHEN`, a
#     comment and a scalar subquery.
#
# The walker's failures were MEASURED against SQLite before it was deleted,
# rather than argued from the review text, and they were not all of one kind:
#
#     SELECT CASE WHEN 1 THEN COUNT(*) ELSE 9 END FROM t   -> row_set, pop 1
#     SELECT -- the total\n COUNT(*) FROM t                -> row_set, pop 1
#     SELECT (SELECT COUNT(*) FROM t WHERE 1=0)            -> row_set, pop 1
#     /* which page? */ SELECT COUNT(*) FROM t             -> unknown  (aborts)
#
# The first three are the silent failure: one scalar row carrying ZERO recorded
# as a population of 1, i.e. the round-13 defect restored. The fourth is the
# loud one -- a legitimate statement the benchmark refuses to measure at all.
# Two syntaxes the review predicted would break it, `COUNT(*) FILTER (WHERE ...)`
# and a nested CTE, in fact classified correctly; that is stated here because
# the same measurement that convicts a reader has to acquit it where it is
# right.
#
# Both readers failed the same way: they defaulted to `row_set` for expression
# grammar they had not enumerated, and `row_set` is the reading that keeps the
# benchmark green. Writing a third parser would be asserting that SQL's
# expression grammar has now been enumerated -- an assertion no one can check,
# which is exactly the kind this phase keeps having to retract.
#
# So no SQL is parsed at all. Two independent sources settle it:
#
#   1. THE SPEC DECLARES the shape, at the call site that CHOSE it -- the same
#      statement that passes `count_only=True` to the shipped builder says
#      SHAPE_SCALAR in the same breath. Nothing is remembered later and nothing
#      is inferred. A spec that declares NO shape is `unknown`, and the
#      benchmark ABORTS by name: defaulting is what produced this finding three
#      times, so there is no default to fall into.
#
#   2. THE EXECUTED RESULT VERIFIES it -- `cursor.description` and the fetched
#      rows, which no syntax can fool because no syntax is read.
#
# What the result can and cannot decide, stated plainly rather than overclaimed:
#
#   * it REFUTES a declared scalar decisively. An un-grouped aggregate returns
#     exactly ONE row carrying a NUMBER over any data; anything else -- 0 rows,
#     2 rows, a text cell, a NULL -- is a contradiction and aborts. That covers
#     every "this is really a row set" mis-declaration, including a window
#     function (`COUNT(*) OVER ()` returns one row per row) and a compound
#     `UNION` (two arms, two rows).
#   * it CANNOT refute a declared row set from the result alone: one row of one
#     integer column is character-for-character what a scalar aggregate returns.
#     So a row-set spec must ALSO carry `expected_rows`, computed independently
#     of the statement being timed (the state's own count at that unit, and the
#     page arithmetic). Observed != expected aborts. That is what gives the
#     row-set declaration teeth in the direction the result cannot see.
#   * a declared scalar reading column 0 of a MULTI-column row aborts unless the
#     spec names the column: `SELECT 1, COUNT(*)` once recorded a population of
#     1 for an empty count by reading the literal (round 13), and a defaulted
#     index is precisely that door.
#
# Note on what is NOT the authority: `spec['kind']`. It is a BUDGET class, not a
# shape -- `findings_launch_contribution_*` is `kind='count'` and returns one
# row per shade -- so agreeing with it would be agreeing with the wrong thing.
# ---------------------------------------------------------------------------

SHAPE_ROWS = "row_set"
SHAPE_SCALAR = "scalar_aggregate"
SHAPE_UNKNOWN = "unknown"
_DECLARABLE_SHAPES = (SHAPE_ROWS, SHAPE_SCALAR)


class ShapeContradiction(AssertionError):
    """What SQLite returned refutes the shape the spec declared.

    RAISED, never returned. A contradiction handed back as a string is a
    contradiction the next caller forgets to read, and an unread contradiction
    is the same silence this class exists to end.
    """


def population_of_result(
    label: str,
    *,
    shape: str,
    description: Any,
    fetched: List[Any],
    value_index: Optional[int] = None,
    expected_rows: Optional[int] = None,
    expected_value: Optional[int] = None,
) -> int:
    """The population a timing was taken over -- from the RESULT, not the SQL.

    `shape` is what the spec DECLARED; `description` / `fetched` are what SQLite
    actually returned. Returns the population, or raises `ShapeContradiction`
    naming exactly which of the two is refuted by the other. There is no path
    that returns a population it could not justify.
    """
    columns = len(description or ())
    rows = len(fetched)

    if shape not in _DECLARABLE_SHAPES:
        raise ShapeContradiction(
            f"{label}: the spec declares no result shape ({shape!r}), so the "
            "benchmark cannot say what population its timing was taken over. "
            f"Declare {SHAPE_SCALAR!r} (population = the aggregate's VALUE) or "
            f"{SHAPE_ROWS!r} (population = the ROW COUNT) at the call site that "
            "built the statement. There is deliberately no default: defaulting "
            "to a row count is how this assertion went vacuous three times."
        )

    if shape == SHAPE_SCALAR:
        if rows != 1:
            raise ShapeContradiction(
                f"{label}: declared a scalar aggregate, but SQLite returned "
                f"{rows} rows. An un-grouped aggregate returns EXACTLY one row "
                "over any data -- so this statement is not one (a window "
                "function, a GROUP BY or a compound SELECT all look like this)."
            )
        if columns == 0:
            raise ShapeContradiction(
                f"{label}: declared a scalar aggregate, but the statement "
                "returned no columns at all.")
        if value_index is None:
            if columns != 1:
                raise ShapeContradiction(
                    f"{label}: declared a scalar aggregate and returned "
                    f"{columns} columns without saying which one carries it. "
                    "Reading column 0 by default is how `SELECT 1, COUNT(*)` "
                    "recorded a population of 1 for a count of nothing (round "
                    "13) -- state `value_index` instead."
                )
            index = 0
        else:
            index = int(value_index)
            if not 0 <= index < columns:
                raise ShapeContradiction(
                    f"{label}: declared the aggregate in column {index}, but "
                    f"the statement returned {columns} columns.")
        cell = fetched[0][index]
        if isinstance(cell, bool) or not isinstance(cell, int):
            name = description[index][0] if description else "?"
            raise ShapeContradiction(
                f"{label}: declared a scalar aggregate, but column {index} "
                f"({name!r}) holds {cell!r} ({type(cell).__name__}), which is "
                "not a countable number. Either the shape or the column index "
                "is wrong; a non-numeric cell can never be a population."
            )
        population = int(cell)
        if expected_value is not None and population != int(expected_value):
            raise ShapeContradiction(
                f"{label}: the timed statement counted {population}, but the "
                f"same population counted independently is {int(expected_value)}."
            )
        return population

    # --- SHAPE_ROWS ---------------------------------------------------------
    if expected_rows is None:
        raise ShapeContradiction(
            f"{label}: declared a row set without an independently-computed "
            "expected row count. One row of one integer column is "
            "character-for-character what a scalar aggregate returns, so the "
            "RESULT alone can never confirm a row-set declaration -- the "
            "expectation is the only thing that can."
        )
    if rows != int(expected_rows):
        raise ShapeContradiction(
            f"{label}: declared a row set returning {int(expected_rows)} rows "
            f"(that filter state's own count at this unit, through the page "
            f"arithmetic), but SQLite returned {rows}. Either the statement no "
            "longer returns the rows it is credited with, or it is not a row "
            "set at all -- a shape that silently became an aggregate returns "
            "exactly 1 here."
        )
    return rows


def _time_sql(conn: sqlite3.Connection, sql: str, params, repeats: int,
              *, shape: str, value_index: Optional[int] = None,
              expected_rows: Optional[int] = None,
              expected_value: Optional[int] = None,
              label: str = "(unlabelled statement)") -> Dict[str, Any]:
    """Time ``sql`` and record the population it actually measured.

    ``rows`` is the number of RESULT ROWS. For an aggregate that returns one row
    carrying a number -- ``SELECT COUNT(*) ...`` -- that figure is 1 no matter
    what the count is, so the F14 nonzero-result assertion cannot see an empty
    measurement through it: ``SELECT COUNT(*) FROM empty_table`` records
    ``rows = 1`` and a count predicate matching nothing is documented as a
    passing measurement (code review round 13, finding 3). ``population`` --
    never ``rows`` -- is what the caller must assert on.

    ``shape`` is the spec's DECLARATION and is checked against what SQLite
    returned by `population_of_result`, which raises rather than guessing. No
    SQL text is inspected anywhere on this path.
    """
    latencies_ms: List[float] = []
    fetched: List[Any] = []
    description: Any = None
    for _ in range(max(1, repeats)):
        t0 = time.perf_counter()
        cursor = conn.execute(sql, params)
        fetched = cursor.fetchall()
        latencies_ms.append((time.perf_counter() - t0) * 1000.0)
        description = cursor.description
    population = population_of_result(
        label, shape=shape, description=description, fetched=fetched,
        value_index=value_index, expected_rows=expected_rows,
        expected_value=expected_value)
    return {
        "shape": shape,
        "rows": len(fetched),
        "columns": len(description or ()),
        "value": population if shape == SHAPE_SCALAR else None,
        "expected_rows": expected_rows,
        "population": population,
        "p50_ms": _pct(latencies_ms, 50),
        "p95_ms": _pct(latencies_ms, 95),
        "max_ms": max(latencies_ms),
    }


# ---------------------------------------------------------------------------
# Spec constructors. The ONLY places a shape is declared, each one adjacent to
# the builder call whose arguments determined it -- so the declaration cannot
# drift from the statement the way a remembered flag drifts from its sibling.
# ---------------------------------------------------------------------------

def _row_spec(label: str, *, kind: str, cap_ms: float, sql: str, params,
              expected_rows: int) -> Dict[str, Any]:
    """A statement whose population is its ROW COUNT.

    `expected_rows` is mandatory and must be computed WITHOUT executing `sql`:
    it is the only evidence that can contradict this declaration (see
    `population_of_result`).
    """
    return {"label": label, "kind": kind, "cap_ms": cap_ms, "sql": sql,
            "params": params, "skip": None, "shape": SHAPE_ROWS,
            "expected_rows": int(expected_rows)}


def _scalar_spec(label: str, *, kind: str, cap_ms: float, sql: str, params,
                 value_index: Optional[int] = None,
                 expected_value: Optional[int] = None) -> Dict[str, Any]:
    """A statement whose population is the VALUE of its aggregate column."""
    return {"label": label, "kind": kind, "cap_ms": cap_ms, "sql": sql,
            "params": params, "skip": None, "shape": SHAPE_SCALAR,
            "value_index": value_index, "expected_value": expected_value}


def _skipped_spec(label: str, *, kind: str, cap_ms: float,
                  reason: str) -> Dict[str, Any]:
    """A combination that is NOT measured, carrying the reason it was not.

    Its shape stays `unknown` on purpose: if a future edit removes the skip
    without declaring a shape, the benchmark aborts by name instead of quietly
    counting rows.
    """
    return {"label": label, "kind": kind, "cap_ms": cap_ms, "sql": "",
            "params": (), "skip": reason, "shape": SHAPE_UNKNOWN}


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


#: The page's own filter axes, in the order a label spells them. The BUCKET is
#: the fifth and is spelled as the label's stem because it is the one axis with
#: no "off" -- a reader is always in one bucket or the other.
#:
#: This tuple IS the space. `web/pages/findings.py::fetch_findings` hands
#: `_build_findings_query` exactly `bucket`, `novelty`, `domain`, `author` and
#: `work_id` out of the persisted page state, each independently settable, and
#: `_build_findings_filter` composes them as AND. Enumerating four hand-chosen
#: states out of that (main / more / novelty-on-main / domain-on-main) left
#: `author`, `work`, every AND-composition and every filtered SECOND-BUCKET
#: combination unmeasured, while the report said "the FULL combination space"
#: (code review round 13, finding 4).
_FINDINGS_FILTER_AXES: Tuple[str, ...] = (
    "novelty", "include_divergent", "domain", "author", "work")


def _findings_filter_states(picks: Dict[str, Dict[str, Any]],
                            ) -> List[Tuple[str, str, Dict[str, Any], Tuple[str, ...]]]:
    """`(bucket stem, label, builder kwargs, axes on)` for the WHOLE space.

    Both buckets x every subset of the optional axes. Nothing is chosen here;
    the cartesian product is generated, and whether a given state has rows in
    THIS asset is settled afterwards.

    `include_divergent` is a BOOLEAN axis rather than a value pick, and it
    belongs in this space for the same reason the others do: turning it on
    drops a `novelty_status NOT IN (...)` predicate from the WHERE clause, so
    the two settings are genuinely different queries over genuinely different
    row counts (~23.6% of the grain), and a probe that measured only the
    default one would report a narrower page than the reader can reach.
    """
    from shared.discovery_service import BUCKET_MAIN, BUCKET_MORE

    states: List[Tuple[str, str, Dict[str, Any], Tuple[str, ...]]] = []
    for stem, bucket in (("main", BUCKET_MAIN), ("more", BUCKET_MORE)):
        pick = picks.get(stem) or {}
        novelty_value = pick.get("novelty_status")
        for mask in range(1 << len(_FINDINGS_FILTER_AXES)):
            on = tuple(axis for i, axis in enumerate(_FINDINGS_FILTER_AXES)
                       if mask & (1 << i))
            kwargs: Dict[str, Any] = {"bucket": bucket}
            if "novelty" in on:
                kwargs["novelty"] = [novelty_value] if novelty_value else None
            if "include_divergent" in on:
                kwargs["include_divergent"] = True
            if "domain" in on:
                kwargs["domain"] = pick.get("domain")
            if "author" in on:
                kwargs["author"] = pick.get("author")
            if "work" in on:
                kwargs["work_id"] = pick.get("work_id")
            states.append((stem, "+".join((stem,) + on), kwargs, on))
    return states


def _findings_combination_specs(conn, *, page_size: int, deep_page: int,
                                filters: Dict[str, Any], total_rows: int
                                ) -> List[Dict[str, Any]]:
    """The FULL combination space, built through the SHIPPED query builder.

    Closes 136-14's owed follow-up: the probe no longer mirrors the findings
    service in hand-written SQL. `_build_findings_query` is the exact builder
    `DiscoveryService.get_findings_enveloped` calls, so the two can no longer
    diverge -- which is what a benchmark measuring a near-copy cannot promise.

    Enumerated:

    * every ROW UNIT x every SORT MODE x every FILTER STATE, where the filter
      state space is the cartesian product `_findings_filter_states` generates
      -- BOTH buckets x every subset of the optional axes
      (`_FINDINGS_FILTER_AXES`), AND-composed exactly as the page composes
      them;
    * the bounded COUNT per unit x filter state, against its own separate cap;
    * a DEEP PAGE per unit x filter state, measured wherever that state really
      carries enough rows to have one (decided by the state's own count, never
      by an assertion that a filtered set "must be smaller");
    * ruling U's launch-statistics queries.

    A state that this asset has no rows for is a NAMED SKIP carrying the exact
    combination it lacked. A state that HAS rows and measures zero stays the
    loud F14 abort -- that one is a probe bug, not an asset fact.
    """
    from shared.discovery_service import (
        FINDINGS_SORTS,
        FINDINGS_UNIT_IDENTIFICATION,
        FINDINGS_UNITS,
        _FINDINGS_FROM,
        _build_findings_filter,
        _build_findings_query,
        _build_launch_contribution_sql,
        _build_launch_manuscript_sql,
        findings_novelty_offered,
    )

    units = sorted(FINDINGS_UNITS)
    sorts = sorted(FINDINGS_SORTS)
    # Backward compatibility with a caller (or a test) that supplies only the
    # flat pre-round-13 filter dict: fall back to the single global domain and
    # novelty status with no author/work pick, so those axes become named skips
    # rather than crashing -- and so a deliberately bogus flat value still
    # reaches the F14 abort through a single-axis state.
    picks = filters.get("buckets") or {
        stem: {"domain": filters.get("domain"), "author": None, "work_id": None,
               "novelty_status": filters.get("novelty_status")}
        for stem in ("main", "more")
    }

    def _population(where: str, params: Tuple[Any, ...] = ()) -> int:
        """How many identifications this asset carries for a POPULATION.

        Used only for populations that are properties of the ASSET, so a
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

    def _state_skip(stem: str, kwargs: Dict[str, Any],
                    on: Tuple[str, ...]) -> Optional[str]:
        """Why this filter state cannot be measured on THIS asset, or None.

        Four questions, in order, and the LAST one is where F14's teeth stay.

        1. Does the asset carry identifications at all?
        2. Does this BUCKET carry any? (Ruling T makes the second bucket a
           first-class surface, but an asset may still have none.)
        3. Does every axis the state turns on have a VALUE in this bucket -- a
           novelty status, a curated `works.genre`, an author?
        4. For TWO OR MORE axes, do those values CO-OCCUR on some row?

        Question 4 is answered by an EXISTS probe built from the SHIPPED
        predicate rather than reasoned about. It is asked ONLY of multi-axis
        states, deliberately: every value comes from `_coherent_bucket_pick`,
        which draws it from THIS bucket, so a single-axis state that matches
        nothing means the probe picked a value the asset does not have. That is
        a PROBE BUG and must stay the loud F14 abort -- turning it into a quiet
        skip is exactly how a benchmark comes to measure less than it claims.
        Only co-occurrence is an asset fact.
        """
        if not total_rows:
            return "the asset carries no identifications"
        if not bucket_population.get(stem):
            return ("this asset carries no main-pool identifications"
                    if stem == "main" else
                    "this asset carries no second-bucket identifications")
        # `include_divergent` is deliberately absent: it is a BOOLEAN axis, not
        # a value the asset either offers or does not, so "this asset has no
        # value for it" is not a question that can be asked of it. Turning it on
        # WIDENS the predicate (it drops a `NOT IN`), so the state it produces
        # always has at least as many rows as the same state without it --
        # meaning it can never be the reason a combination is unissuable, and a
        # skip attributed to it would be a skip for a reason that cannot exist.
        keys = {"novelty": "novelty", "domain": "domain",
                "author": "author", "work": "work_id"}
        missing = [axis for axis in on
                   if axis in keys and not kwargs.get(keys[axis])]
        if missing:
            return (f"this asset offers no value for the {', '.join(missing)} "
                    f"filter in the {stem} bucket, so that combination cannot be "
                    "issued against it")
        if len(on) <= 1:
            return None
        # The identification grain, deliberately: the predicate is the same at
        # every unit (the units differ only in grouping, and grouping >=1 row
        # never yields zero groups), and `novelty` is rejected outright on the
        # per-work unit by the builder.
        where_sql, params = _build_findings_filter(
            unit=FINDINGS_UNIT_IDENTIFICATION, **kwargs)
        try:
            found = conn.execute(
                f"SELECT 1 {_FINDINGS_FROM} {where_sql} LIMIT 1", params).fetchone()
        except sqlite3.Error:                                # pragma: no cover
            found = None
        if found is None:
            return (f"each of {', '.join(on)} has a value in the {stem} bucket, "
                    "but this asset carries no identification where they hold "
                    "TOGETHER")
        return None

    filter_states = _findings_filter_states(picks)
    state_skips = {label: _state_skip(stem, kwargs, on)
                   for stem, label, kwargs, on in filter_states}

    _WORK_UNIT_NOVELTY_SKIP = (
        "the SURFACE cannot issue this: `shared.discovery_service."
        "findings_novelty_offered` says the candidacy axis does not apply to the "
        "per-work unit (a work spanning many manuscripts has no single verdict), "
        "so `web/pages/findings.py::normalise_state` drops the selection the "
        "moment the unit changes and the switch is disabled there -- and if one "
        "ever did get through, `_build_findings_filter` RAISES rather than "
        "returning an envelope")

    def _novelty_unreachable(unit: str, on: Tuple[str, ...]) -> bool:
        """Whether this (unit, axes-on) pair is one the SURFACE cannot produce.

        Derived from the SERVICE's own predicate, never restated as
        `unit == 'work'`. It was restated once, and the restatement was not the
        problem: the problem was that the sentence it stood for -- "unreachable"
        -- was FALSE. The page left the candidacy switch live while a separate
        control changed the row unit, so a reader really could reach all 80 of
        these states, and reaching one raised on a live page (code review round
        15, finding 1). They are unreachable NOW because the page settles the
        pair; `tests/test_discovery_build.py::
        test_the_work_unit_novelty_skip_is_unreachable_THROUGH_THE_PAGE` holds
        that claim against `web/pages/findings.py` rather than against this
        docstring.
        """
        return "novelty" in on and not findings_novelty_offered(unit)

    specs: List[Dict[str, Any]] = []
    for unit in units:
        # The count AT THIS UNIT for each filter state: it decides the
        # visible-total spec, whether that state has a deep page at all, AND how
        # many rows each paged query is expected to return -- the independent
        # expectation that gives every row-set declaration its teeth
        # (`population_of_result`). One query, three uses -- and the depth bound
        # is never taken from a different grain (the per-work unit groups
        # ~1,000x fewer rows, so a bound from the identification grain claims
        # deep paging is measurable for a unit whose whole result set fits on
        # page 13).
        unit_rows: Dict[str, int] = {}
        for _stem, label, kwargs, on in filter_states:
            if state_skips[label] or _novelty_unreachable(unit, on):
                continue
            count_sql, count_params = _build_findings_query(
                unit=unit, count_only=True, **kwargs)
            try:
                unit_rows[label] = int(conn.execute(count_sql, count_params).fetchone()[0])
            except sqlite3.Error:                            # pragma: no cover
                unit_rows[label] = 0

        def _page_rows(label: str, page: int) -> int:
            """How many rows page `page` of this state returns AT THIS UNIT.

            Derived from the state's own count and the builder's own
            `LIMIT ? OFFSET ?` -- never from executing the statement being
            timed, which is the whole point: an expectation read off the thing
            it is meant to check cannot contradict it.
            """
            offset = (page - 1) * page_size
            return max(0, min(page_size, unit_rows.get(label, 0) - offset))

        for sort in sorts:
            for _stem, label, kwargs, on in filter_states:
                spec_label = f"findings_{unit}_{sort}_{label}"
                if _novelty_unreachable(unit, on):
                    specs.append(_skipped_spec(
                        spec_label, kind="ordering",
                        cap_ms=FINDINGS_ORDERING_CAP_MS,
                        reason=_WORK_UNIT_NOVELTY_SKIP))
                    continue
                if state_skips[label]:
                    specs.append(_skipped_spec(
                        spec_label, kind="ordering",
                        cap_ms=FINDINGS_ORDERING_CAP_MS,
                        reason=state_skips[label]))
                    continue
                sql, params = _build_findings_query(
                    unit=unit, sort=sort, page=1, page_size=page_size, **kwargs)
                # `count_only` was NOT passed, so this returns page rows: the
                # shape is declared here, beside the argument that chose it.
                specs.append(_row_spec(
                    spec_label, kind="ordering", cap_ms=FINDINGS_ORDERING_CAP_MS,
                    sql=sql, params=params,
                    expected_rows=_page_rows(label, 1)))

        # Deep paging -- where an ordering index earns its keep, and the shape a
        # spot check at page 1 will never expose. Enumerated for EVERY filter
        # state; whether a state is deep enough is read off its own count.
        for _stem, label, kwargs, on in filter_states:
            spec_label = f"findings_{unit}_deep_page_{deep_page}_{label}"
            if _novelty_unreachable(unit, on):
                specs.append(_skipped_spec(
                    spec_label, kind="ordering", cap_ms=FINDINGS_ORDERING_CAP_MS,
                    reason=_WORK_UNIT_NOVELTY_SKIP))
                continue
            if state_skips[label]:
                specs.append(_skipped_spec(
                    spec_label, kind="ordering", cap_ms=FINDINGS_ORDERING_CAP_MS,
                    reason=state_skips[label]))
                continue
            available = unit_rows.get(label, 0)
            if available <= (deep_page - 1) * page_size:
                specs.append(_skipped_spec(
                    spec_label, kind="ordering", cap_ms=FINDINGS_ORDERING_CAP_MS,
                    reason=(f"the {unit} unit carries only {available} rows under "
                            f"that filter state -- fewer than the page-{deep_page} "
                            "offset, so deep paging cannot be measured on a "
                            "nonzero result set")))
                continue
            sql, params = _build_findings_query(
                unit=unit, page=deep_page, page_size=page_size, **kwargs)
            specs.append(_row_spec(
                spec_label, kind="ordering", cap_ms=FINDINGS_ORDERING_CAP_MS,
                sql=sql, params=params,
                expected_rows=_page_rows(label, deep_page)))

        # The visible COUNT, against its own SEPARATE cap (§5), for every filter
        # state -- the page issues it for whichever state is active. Measured in
        # the bounded form the surface uses when DISCOVERY_FINDINGS_COUNT_MAX is
        # set; with the knob off the total rides on COUNT(*) OVER () inside the
        # ordering query above and costs no second statement.
        for _stem, label, kwargs, on in filter_states:
            spec_label = f"findings_{unit}_visible_total_{label}"
            if _novelty_unreachable(unit, on):
                specs.append(_skipped_spec(
                    spec_label, kind="count", cap_ms=FINDINGS_COUNT_CAP_MS,
                    reason=_WORK_UNIT_NOVELTY_SKIP))
                continue
            if state_skips[label]:
                specs.append(_skipped_spec(
                    spec_label, kind="count", cap_ms=FINDINGS_COUNT_CAP_MS,
                    reason=state_skips[label]))
                continue
            sql, params = _build_findings_query(unit=unit, count_only=True, **kwargs)
            # `count_only=True` -- one row carrying the total. Declared HERE,
            # in the same statement that asked for it.
            specs.append(_scalar_spec(
                spec_label, kind="count", cap_ms=FINDINGS_COUNT_CAP_MS,
                sql=sql, params=params,
                expected_value=unit_rows.get(label)))

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

    def _distinct_shades(where: str) -> int:
        """How many of ruling U's shades this asset actually carries.

        The contribution statement GROUPs by shade, so this -- not the
        identification total above -- is how many rows it returns, and it is the
        independent expectation that verifies its row-set declaration.
        """
        try:
            return int(conn.execute(
                "SELECT COUNT(DISTINCT di.novelty_status) FROM "
                f"discovery_identification di WHERE {where}",
                tuple(LAUNCH_CONTRIBUTION_SHADES)).fetchone()[0])
        except sqlite3.Error:                                # pragma: no cover
            return 0

    shade_groups = {
        "main_pool": _distinct_shades(
            f"di.main_pool = 1 AND di.novelty_status IN ({shade_placeholders})"),
        "all_bucket": _distinct_shades(
            f"di.novelty_status IN ({shade_placeholders})"),
    }
    for main_pool_only in (True, False):
        basis = "main_pool" if main_pool_only else "all_bucket"
        label = f"findings_launch_contribution_{basis}"
        if not shade_population[basis]:
            specs.append(_skipped_spec(
                label, kind="count", cap_ms=FINDINGS_COUNT_CAP_MS,
                reason=("this asset carries no identification in any ruling-U "
                        f"contribution shade on the {basis} basis")))
            continue
        sql, params = _build_launch_contribution_sql(main_pool_only=main_pool_only)
        # `kind='count'` but SHAPE_ROWS: this statement GROUPs BY shade and
        # returns one row per shade. It is exactly why `spec['kind']` is a
        # budget class and never the shape authority.
        specs.append(_row_spec(
            label, kind="count", cap_ms=FINDINGS_COUNT_CAP_MS,
            sql=sql, params=params, expected_rows=shade_groups[basis]))

    if shade_population["main_pool"]:
        sql, params = _build_launch_manuscript_sql(main_pool_only=True)
        specs.append(_scalar_spec(
            "findings_launch_manuscripts_main_pool", kind="count",
            cap_ms=FINDINGS_COUNT_CAP_MS, sql=sql, params=params))
    else:
        specs.append(_skipped_spec(
            "findings_launch_manuscripts_main_pool", kind="count",
            cap_ms=FINDINGS_COUNT_CAP_MS,
            reason=("this asset carries no identification in any ruling-U "
                    "contribution shade in the main pool")))
    return specs


def bench_findings_page(
    db_path: str, *, page_size: int = 50, repeats: int = 5, deep_page: int = 20
) -> Dict[str, Any]:
    """Measure the FULL corpus-wide findings combination space.

    The space is the cartesian product the SHIPPED page can put into the SHIPPED
    builder: every ROW UNIT x every SORT MODE x every FILTER STATE, where a
    filter state is BOTH buckets x every subset of {novelty, domain, author,
    work} -- `web/pages/findings.py::fetch_findings` hands all five to
    `_build_findings_query` out of the persisted page state and
    `_build_findings_filter` composes them as AND. Plus the bounded COUNT and a
    DEEP PAGE for each of those states, plus ruling U's launch-statistics
    queries. Everything is built through `_build_findings_query`, never a
    hand-written mirror of it.

    Every combination asserts a nonzero measured POPULATION before its timing is
    recorded (result rows for a row query, the counted VALUE for an aggregate),
    and a combination this asset has no rows for is SKIPPED with the exact
    combination it lacked rather than measured as an empty no-op. Combinations
    the SURFACE cannot issue are named with their reason too.

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
            shape = spec.get("shape", SHAPE_UNKNOWN)
            if shape not in _DECLARABLE_SHAPES:
                # NOT a skip and NOT a default to row counting: a statement
                # whose shape nobody declared has an unreadable population, and
                # recording a timing beside one is the F14 failure this whole
                # mechanism exists to prevent.
                raise AssertionError(
                    f"{spec['label']}: the benchmark cannot tell what shape this "
                    "statement returns, so it cannot say what population its "
                    "timing was taken over. Build it with `_row_spec` or "
                    "`_scalar_spec` -- whichever the builder arguments chose -- "
                    "rather than assuming one:\n" + spec["sql"].strip()[:400]
                )
            scalar = shape == SHAPE_SCALAR
            measured = _time_sql(
                conn, spec["sql"], spec["params"], repeats, shape=shape,
                value_index=spec.get("value_index"),
                expected_rows=spec.get("expected_rows"),
                expected_value=spec.get("expected_value"),
                label=spec["label"])
            if measured["population"] == 0:
                # `population` and NOT `rows`: an aggregate returns exactly one
                # row whatever its value, so a row-count assertion passes for
                # every count shape regardless of what was counted (round 13,
                # finding 3). The message says WHICH quantity was zero so the
                # two cases are never confused again.
                measured_what = "counted ZERO" if scalar else "returned ZERO rows"
                raise AssertionError(
                    f"{spec['label']}: the measured query {measured_what} "
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
            # WHAT "full" means here, recorded rather than claimed in prose: the
            # axes the shipped page can set and the size of their product.
            "filter_space": {
                "buckets": ("main", "more"),
                "optional_axes": _FINDINGS_FILTER_AXES,
                "states": 2 * (1 << len(_FINDINGS_FILTER_AXES)),
            },
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
    space = result.get("filter_space") or {}
    if space:
        print(f"filter space     : {len(space['buckets'])} buckets x every subset "
              f"of {list(space['optional_axes'])} = {space['states']} states, "
              "AND-composed as the page composes them")
    print(f"combinations     : {result['combinations']} enumerated, "
          f"{len(result['shapes'])} measured, "
          f"{len(result['skipped_shapes'])} skipped, "
          f"{len(result.get('out_of_scope') or ())} out of scope")
    # `pop.` is the population the nonzero-result assertion actually tested:
    # result rows for a row query, the COUNTED VALUE for an aggregate. Printing
    # the row count for an aggregate reads as "1 row, fine" for a count of zero.
    print(f"{'combination':<70}{'pop.':>8}{'p50 ms':>9}{'p95 ms':>9}"
          f"{'max ms':>9}{'cap ms':>8}  result")
    for r in result["shapes"]:
        verdict = "FAIL" if r in result["failures"] else "PASS"
        print(
            f"{r['label']:<70}{r['population']:>8}{r['p50_ms']:>9.2f}{r['p95_ms']:>9.2f}"
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
    space = findings.get("filter_space") or {}
    lines = [
        "### 4.4 Corpus-wide findings page (§5 caps) — the FULL combination space, measured\n",
        "",
        f"Measured by `scripts/bench_discovery.py::bench_findings_page()` over "
        f"{findings['identifications']} materialized identifications "
        f"(`discovery_identification`), page size {findings['page_size']}, "
        f"deep page {findings['deep_page']}. "
        f"**{findings.get('combinations', 0)} combinations enumerated, "
        f"{len(findings['shapes'])} measured.** Every combination asserted a "
        "NONZERO measured population before its timing was recorded.",
        "",
        "**What \"full\" means, stated rather than claimed.** The filter space is "
        "the cartesian product the shipped page can put into the shipped builder: "
        + " × ".join([
            f"{len(space['buckets'])} buckets ({', '.join(space['buckets'])})",
            "every subset of {" + ", ".join(space["optional_axes"]) + "}",
        ])
        + f" = **{space['states']} filter states**, AND-composed. "
        "`web/pages/findings.py::fetch_findings` hands `bucket`, `novelty`, "
        "`domain`, `author` and `work_id` to `_build_findings_query` out of the "
        "persisted page state, each independently settable, and "
        "`_build_findings_filter` composes them with `AND`. Each state is crossed "
        "with every ROW UNIT and every SORT MODE for the ordering query, and with "
        "every ROW UNIT for the bounded COUNT and for a deep page. A state this "
        "asset has no rows for is a named skip carrying the combination it "
        "lacked — decided by an `EXISTS` probe against the shipped predicate, "
        "never by an argument that some combination \"must\" be unreachable."
        if space else "",
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
        "The **Population** column is the quantity the nonzero-result assertion "
        "tested: result ROWS for a row query, the COUNTED VALUE for an aggregate. "
        "A count query returns one row whatever it counts, so recording its row "
        "count would document a count of zero as a passing measurement.",
        "",
        "| Combination | Cap | p50 | p95 | max | Population | Result |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in findings["shapes"]:
        verdict = "PASS ✓" if r["p95_ms"] <= r["cap_ms"] else "FAIL ✗"
        lines.append(
            f"| `{r['label']}` | p95 ≤ {r['cap_ms']:.0f} ms | {r['p50_ms']:.2f} ms | "
            f"**{r['p95_ms']:.2f} ms** | {r['max_ms']:.2f} ms | {r['population']} | {verdict} |"
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
