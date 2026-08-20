# -*- coding: utf-8 -*-
"""Post-deploy READINESS SMOKE for the discovery sidecar. Seconds, not hours.

WHY THIS EXISTS
---------------
`docs/specs/discovery-deploy.md` used to prescribe `scripts/bench_discovery.py`
as step 8's "readiness smoke". It is not one. `bench_findings_page()` runs
2 buckets x 2**9 axis subsets x 3 units x 3 sorts plus deep-page and
visible-total specs -- about 15,363 combinations, x5 repeats, ~76,815 timed
statements. On 2026-08-19 that turned the V4.2 deploy into an hour-plus hang
that ended in `client_loop: send disconnect: Connection reset`, and the deploy
recorded a FAILURE it had not actually had: steps 0-7 had succeeded and the swap
was live and healthy.

A readiness smoke answers one question -- **can the app load the asset it is now
serving, and does that asset answer real reads?** -- and it answers it in
seconds. Depth belongs to `scripts/verify_discovery_sidecar.py` (run pre-swap,
against the staged bytes) and breadth belongs to the benchmark (run deliberately,
never on a deploy path).

WHAT IT PROVES, AND WHAT IT DOES NOT
------------------------------------
It proves the LOADER path and the SERVICE path, not just the file: it calls
`load_discovery_state()` and then reads through `web.discovery`, so a
fail-closed sidecar, a hash mismatch, a wrong audience or a schema the loader
rejects all surface here as a red. It does NOT re-prove the invariants
`verify_discovery_sidecar.py` already checked, and it is not a benchmark -- the
timings it prints are context for a human, not budgets.

Two things it reproduces on purpose, because a bare import gets both wrong:
  * `load_dotenv()` with an EXPLICIT path. Only `web/main.py` loads `.env`, so
    without this `DISCOVERY_ENABLED` is unread and the smoke reports a disabled
    feature as an outage. An explicit path is required because `find_dotenv()`
    walks caller frames and there are none under `python -`.
  * `load_discovery_state()`. Only `web/main.py` calls it, so without this
    `discovery_available()` is False and every read returns `unavailable`.

Usage (on the box, after the manifest swap and the service restart):

    cd /home/ubuntu/GenizahSearch && source venv/bin/activate \\
      && python scripts/smoke_discovery_readiness.py

Exit code 0 = ready. Non-zero = not ready, and the reason is named.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: Per-read wall-clock ceiling. Generous on purpose: this is a readiness check,
#: not a performance gate, so it should only trip on something genuinely stuck.
#: The service's own findings timeout is 5.0s, so a read that takes longer than
#: this has already failed for a reader.
DEFAULT_READ_CEILING_S = 20.0


class SmokeFailure(RuntimeError):
    """A named readiness failure. The message is the operator-facing reason."""


def _bootstrap(env_path: Optional[str]) -> None:
    if _REPO_ROOT not in sys.path:
        sys.path.insert(0, _REPO_ROOT)
    try:
        from dotenv import load_dotenv
    except ImportError as exc:                     # pragma: no cover
        raise SmokeFailure(f"python-dotenv is not installed: {exc}") from exc
    path = env_path or os.path.join(_REPO_ROOT, ".env")
    # `override=False` so a value already exported in the shell wins, which is
    # how a one-off run against a staged directory is steered.
    load_dotenv(path, override=False)


def _load_state() -> Dict[str, Any]:
    import web.discovery_assets as assets

    loaded = assets.load_discovery_state()
    available = assets.discovery_available()
    flag = os.environ.get("DISCOVERY_ENABLED")
    if not loaded:
        raise SmokeFailure(
            "load_discovery_state() returned falsey -- the sidecar did not pass "
            "the loader's fail-closed contract (manifest asset_basename, content "
            "hash, integrity_check, schema_version, audience, required tables, "
            "release-contract row counts or the frozen enum vocabulary). The app "
            "is serving NO discovery data right now.")
    if not available:
        raise SmokeFailure(
            "the sidecar LOADED but discovery_available() is False -- "
            f"DISCOVERY_ENABLED={flag!r}. The asset is fine; the feature is off, "
            "so every discovery surface is hidden. If this is production, that "
            "is an outage.")
    return {"flag": flag, "loaded": True}


def _timed(label: str, coro_factory, ceiling_s: float) -> Dict[str, Any]:
    started = time.perf_counter()
    envelope = asyncio.run(coro_factory())
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    status = (envelope or {}).get("status")
    if status != "ok":
        raise SmokeFailure(
            f"{label}: the read returned status={status!r} "
            f"(meta={(envelope or {}).get('meta')!r}) after {elapsed_ms:.0f} ms. "
            "A readiness read must be `ok`; `unavailable`/`timeout`/`busy` all "
            "mean a reader would see an outage state.")
    if elapsed_ms > ceiling_s * 1000.0:
        raise SmokeFailure(
            f"{label}: {elapsed_ms:.0f} ms exceeds the {ceiling_s:g}s readiness "
            "ceiling -- the asset answers, but not fast enough to serve.")
    return {"label": label, "ms": round(elapsed_ms, 1),
            "total": envelope.get("total"), "rows": len(envelope.get("items") or [])}


def _heaviest_locus_probe() -> Optional[Dict[str, Any]]:
    """The heaviest locus-bearing main-pool work on the SERVED asset, or None.

    Chosen at run time rather than hardcoded: a work id that an artifact stopped
    carrying would silently turn the citation-range check into a query over
    nothing, which is the failure mode this whole file exists to avoid.
    """
    import sqlite3

    import web.discovery_assets as assets

    path = getattr(assets, "discovery_db_path", lambda: None)()
    if not path or not os.path.exists(str(path)):
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"discovery_locus_piece", "locus_unit"} <= tables:
            return None
        row = conn.execute(
            "SELECT p.locus_work_id, COUNT(DISTINCT p.identification_id) n "
            "FROM discovery_locus_piece p JOIN discovery_identification di "
            "  ON di.identification_id = p.identification_id "
            "WHERE di.main_pool = 1 GROUP BY p.locus_work_id "
            "ORDER BY n DESC LIMIT 1").fetchone()
        if not row or not row[0]:
            return None
        bounds = conn.execute(
            "SELECT MIN(citation_pos), MAX(citation_pos) FROM locus_unit "
            "WHERE work_id = ? AND citation_pos IS NOT NULL", (row[0],)).fetchone()
        if not bounds or bounds[0] is None:
            return None
        return {"work_id": row[0], "low": int(bounds[0]), "high": int(bounds[1])}
    finally:
        conn.close()


def run_smoke(ceiling_s: float) -> Dict[str, Any]:
    from web.discovery import get_findings_enveloped

    state = _load_state()
    checks: List[Dict[str, Any]] = []
    skips: List[str] = []

    # 1. THE DEFAULT PAGE. The read every visitor to /computed-identifications
    #    makes. A non-empty main pool is a release contract, so zero rows here is
    #    a failure rather than an honest empty.
    default = _timed("findings default page",
                     lambda: get_findings_enveloped(page_size=10), ceiling_s)
    if not default["total"]:
        raise SmokeFailure(
            "the default findings page reports total=0 -- the asset loaded but "
            "carries no visible main-pool identifications, so the page a reader "
            "lands on is empty.")
    checks.append(default)

    # 2. THE CITATION RANGE. Deliberately included: this is the read that was
    #    silently broken from before 2026-08-19 until 4f6e31f4 -- a correlated
    #    EXISTS made it ~10-19 s against a 5 s timeout, so it returned NOTHING on
    #    every heavy work while the rest of the page looked healthy. A smoke that
    #    only fetched the default page would not have seen it.
    probe = _heaviest_locus_probe()
    if probe is None:
        skips.append("citation-range read: this asset carries no locus pieces "
                     "(a pre-locus artifact), so the range filter is not offered")
    else:
        # A SUB-RANGE, not the work's full span. Asking for low..high selects
        # everything the work has, so the range total equals the unfiltered
        # total and the narrowing assertion below could never fail -- measured
        # exactly that on the first run of this script (2,433 both ways). The
        # lower half is a strict subset by construction.
        low, high = probe["low"], probe["high"]
        half = low + max(1, (high - low) // 2)
        ranged = _timed(
            f"citation range {low}-{half} over {probe['work_id']}",
            lambda: get_findings_enveloped(
                work_id=probe["work_id"], locus_from=low,
                locus_to=half, page_size=10),
            ceiling_s)
        checks.append(ranged)
        unfiltered = _timed(
            f"same work, no range",
            lambda: get_findings_enveloped(work_id=probe["work_id"], page_size=10),
            ceiling_s)
        checks.append(unfiltered)

        # THE READ THAT WAS BROKEN RETURNED NOTHING. That is the first thing to
        # assert, and it is the whole reason this check is in a readiness smoke.
        if not ranged["total"]:
            raise SmokeFailure(
                f"the citation range {low}-{half} over {probe['work_id']} "
                "returned ZERO rows. The work carries locus pieces across "
                f"citations {low}-{high}, so a range covering its lower half "
                "must match something; zero means the range filter is "
                "non-functional for a reader.")
        if ranged["total"] > (unfiltered["total"] or 0):
            raise SmokeFailure(
                f"the citation range returned MORE rows ({ranged['total']}) than "
                f"the same work unfiltered ({unfiltered['total']}) -- the range "
                "predicate is not filtering.")
        # Strict narrowing, asserted only where the asset can actually express
        # it: on a work spanning one or two citation units the lower half IS the
        # whole work, and equality there is honest rather than inert.
        if high - low >= 4 and ranged["total"] == unfiltered["total"]:
            raise SmokeFailure(
                f"the citation range {low}-{half} over {probe['work_id']} "
                f"returned exactly as many rows ({ranged['total']}) as the work "
                f"unfiltered, across a {high - low + 1}-unit span. The predicate "
                "is present but inert -- it reads as working and is not.")

    return {"state": state, "checks": checks, "skips": skips}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fast post-deploy readiness smoke for the discovery sidecar")
    parser.add_argument("--env-file", default=None,
                        help="path to .env (default: <repo>/.env)")
    parser.add_argument("--read-ceiling-s", type=float,
                        default=DEFAULT_READ_CEILING_S,
                        help=f"per-read ceiling in seconds "
                             f"(default {DEFAULT_READ_CEILING_S:g})")
    parser.add_argument("--json", action="store_true",
                        help="emit the result as JSON instead of prose")
    args = parser.parse_args()

    try:
        _bootstrap(args.env_file)
        result = run_smoke(args.read_ceiling_s)
    except SmokeFailure as exc:
        if args.json:
            print(json.dumps({"ready": False, "reason": str(exc)}, indent=2))
        else:
            print(f"NOT READY: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:                                   # noqa: BLE001
        if args.json:
            print(json.dumps({"ready": False,
                              "reason": f"{type(exc).__name__}: {exc}"}, indent=2))
        else:
            print(f"NOT READY: unexpected {type(exc).__name__}: {exc}",
                  file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps({"ready": True, **result}, indent=2, ensure_ascii=False))
        return 0
    print("READY -- the app loaded the served sidecar and it answers real reads.")
    print(f"  DISCOVERY_ENABLED={result['state']['flag']!r}")
    for check in result["checks"]:
        print(f"  {check['label']:<48} {check['ms']:>8.1f} ms  "
              f"total={check['total']} rows={check['rows']}")
    for skip in result["skips"]:
        print(f"  SKIPPED: {skip}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
