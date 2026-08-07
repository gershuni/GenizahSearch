"""Measure the REAL novelty-cache reuse rate through the fingerprint gate.

WHY THIS EXISTS. The bake plan's "87.6% reusable -> approximately $4" figure
measured KEY OVERLAP: how many `(sys_id, work_id)` pairs the existing cache
already has an entry for. Codex round 2 showed that is not the quantity that
matters -- `render_case` also sends the claimed title, the claimed author and the
assembled finding-aid text, so an entry can exist for a pair whose QUESTION has
changed. A fingerprint can only lower reuse, never raise it, so the old figure is
an upper bound, and the plan now states plainly that no spend is authorized until
the real number is measured.

This is a MEASUREMENT, not a build. It reads the current asset and the current
finding-aid DBs, recomputes each candidate's input fingerprint, and reports how
many existing verdicts would survive the gate. It writes one report file and
touches nothing else -- no verdict cache is modified, no model is called, no money
is spent.

The existing cache predates the fingerprint, so EVERY entry is `unfingerprinted`
and the headline reuse figure through the gate is **zero**. That is the honest
answer and the point of running it: the question is not "how much survives" but
"what would it cost to re-establish", so the report also states how many pairs
the cache covers at all, which bounds the work a back-fill would avoid.

BACK-FILL. Because the cache's verdicts were produced from inputs that may or may
not have changed, a fingerprint cannot be retro-fitted honestly: stamping today's
fingerprint onto a verdict produced from yesterday's inputs asserts exactly the
thing that was never checked. What CAN be done is reported here per pair, so the
owner can decide with a number rather than a guess.

MASKING (D-25): counts and hex digests only.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.discovery_novelty_funnel import (  # noqa: E402
    candidate_input_fingerprint,
    run_heuristic_funnel,
)
from scripts.discovery_novelty_probe import (  # noqa: E402
    DEFAULT_ASSET,
    DEFAULT_FGP_DB,
    DEFAULT_FJMS_DB,
    DEFAULT_LIBRARIES_CSV,
    DEFAULT_PGP_DB,
    build_all_candidates,
)
from shared.discovery_novelty import BATCH_PROMPT_SHA256  # noqa: E402

DEFAULT_CACHE = os.path.join(REPO_ROOT, "discovery_data", "novelty_production_verdicts.json")
DEFAULT_REPORT = os.path.join(REPO_ROOT, "_tmp", "v3-novelty-reuse-measurement.json")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cache", default=DEFAULT_CACHE)
    ap.add_argument("--asset", default=DEFAULT_ASSET)
    ap.add_argument("--report", default=DEFAULT_REPORT)
    args = ap.parse_args(argv)

    def log(msg: str) -> None:
        print(msg, flush=True)

    log(f"reading cache {os.path.basename(args.cache)}")
    with open(args.cache, encoding="utf-8") as fh:
        cache = json.load(fh)
    log(f"  cache entries: {len(cache):,}")

    log("building candidates from the CURRENT asset + finding-aid DBs "
        "(this is the slow part)")
    candidates, _works, _libraries = build_all_candidates(
        asset_path=args.asset,
        libraries_csv=DEFAULT_LIBRARIES_CSV,
        fjms_db=DEFAULT_FJMS_DB,
        fgp_db=DEFAULT_FGP_DB,
        pgp_db=DEFAULT_PGP_DB,
    )
    log(f"  candidates: {len(candidates):,}")

    # The heuristic funnel resolves what it can mechanically; only the residual
    # would ever reach the model, so only the residual's reuse costs money.
    resolved, residual = run_heuristic_funnel(candidates)
    log(f"  heuristically resolved: {len(resolved):,}")
    log(f"  residual (would reach the model): {len(residual):,}")

    fingerprints = {
        f"{c.sys_id}::{c.ref_work_id}": candidate_input_fingerprint(
            c, prompt_sha256=BATCH_PROMPT_SHA256)
        for c in candidates
    }

    residual_keys = {f"{c.sys_id}::{c.ref_work_id}" for c in residual}
    counters = Counter()
    for key in residual_keys:
        entry = cache.get(key)
        if entry is None:
            counters["residual_absent_from_cache"] += 1
        elif not entry.get("input_fingerprint"):
            counters["residual_present_but_unfingerprinted"] += 1
        elif entry["input_fingerprint"] == fingerprints.get(key):
            counters["residual_fingerprint_ok"] += 1
        else:
            counters["residual_fingerprint_mismatch"] += 1

    covered = counters["residual_fingerprint_ok"]
    report = {
        "measured_utc_note": "timestamp intentionally omitted -- see git commit date",
        "cache_entries": len(cache),
        "candidates": len(candidates),
        "heuristically_resolved": len(resolved),
        "residual": len(residual),
        "residual_breakdown": dict(counters),
        # THE headline number the plan waits on.
        "reuse_rate_through_the_gate": round(covered / len(residual), 6) if residual else None,
        # What the OLD figure measured, for comparison: bare key overlap.
        "key_overlap_rate": round(
            sum(1 for k in residual_keys if k in cache) / len(residual), 6
        ) if residual else None,
        "prompt_sha256": BATCH_PROMPT_SHA256,
    }
    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=1)

    log("")
    log("=== RESULT ===")
    for key, value in report.items():
        log(f"  {key:38s} {value}")
    log("")
    log(f"report written to {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
