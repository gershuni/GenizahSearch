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
import sqlite3
import sys
import time
from typing import Any, Callable, Dict, List, Optional

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
        help="record the measured dev-box actuals into docs/specs/discovery-budgets.md",
    )
    args = parser.parse_args()

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

    if args.write_budgets:
        _write_budgets(
            latency_results=latency_results,
            browse_p95=browse_p95,
            work_p95=work_p95,
            added_rss_mb=(_mb(added_rss_bytes) if added_rss_bytes >= 0 else None),
            sidecar_size_mb=_mb(sidecar_size),
            sidecar_basename=os.path.basename(db_path),
        )
        print("\nWrote MEASURED ACTUALS (dev-box) into docs/specs/discovery-budgets.md")

    return 0


def _write_budgets(
    *,
    latency_results: List[Dict[str, Any]],
    browse_p95: float,
    work_p95: float,
    added_rss_mb: Optional[float],
    sidecar_size_mb: float,
    sidecar_basename: str,
) -> None:
    """Replace the PENDING §4 block in docs/specs/discovery-budgets.md with the
    measured dev-box actuals (prod-box RSS stays a PENDING human step)."""
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
    tail_marker = "\n---\n"
    tail_idx = text.find(tail_marker, idx)
    if tail_idx == -1:
        tail_idx = len(text)

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

### 4.2 Prod-box + later-surface caps — PENDING

- **Additional RSS on the prod box (vs the ≤ 250 MB cap)** — PENDING: the
  authoritative measurement is the 134-08 Task 3 human/live-server step (owner
  runs `bench_discovery.py` / samples the web process RSS around restart on the
  web box); recorded here as **MEASURED ACTUALS (prod-box)** after that run.
- **Work/Leads request-time p95 / response size (§1.2)** — PENDING until Phase
  136 ships the `/work/{{id}}` + `/leads` surfaces (the query-latency figures
  above are the DB-side cost only; the full request-time budget is measured
  when the surface exists).
- **Atlas drill-down p95 / node-edge counts / response size (§1.3)** — PENDING
  until Phase 139 ships the bounded explorer (ATLAS-02).
- Any §2 default that measurement shows is mis-set would require a version bump
  per the "tunable only by versioning" rule above.

These later-surface caps and defaults exist now so those plans have a stable
contract to implement against, not because they are measured yet.

"""
    new_text = text[:idx] + block + text[tail_idx + 1:]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(new_text)


if __name__ == "__main__":
    raise SystemExit(main())
