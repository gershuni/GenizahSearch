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

**WHAT THIS MEASURES, AND WHAT IT DOES NOT** (Codex round 4, HIGH -- the finding
was right and this is the correction). The candidate population is built from the
asset and finding-aid DBs passed in, which DEFAULT to the LEGACY v2 asset. So a
default run measures the reuse rate over the **legacy** candidate population -- and
that is precisely why its residual agreed exactly with the prior run's 55,184. It
is a valid measurement of "how much of the existing cache still answers the
questions it was built for", which is the right question for deciding whether the
cache survives at all. It is **NOT** a measurement of the v3 population: the v3
router, crosswalk and work set are not yet fixed, so the v3 candidate set does not
exist to measure.

Two consequences, both recorded in the report rather than left to a reader:
  * every input path is now recorded WITH ITS CONTENT HASH, so a later reader can
    tell which population a number describes;
  * `population` says `legacy` or `pinned` explicitly. A number without that label
    is the kind of figure the ~$4 estimate was.

The honest v3 sequence, therefore: build the router and the final work set FIRST,
then re-run this against those inputs to get the v3 residual, and only then decide
whether to spend. That is the $0 post-assembly dry measurement round 4 asked for,
and it is now the recommended option in the plan.

MASKING (D-25): counts and hex digests only.
"""
from __future__ import annotations

import argparse
import hashlib
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
    ap.add_argument("--asset", default=DEFAULT_ASSET,
                    help="the discovery asset the candidate population is built from. "
                         "DEFAULTS TO THE LEGACY v2 ASSET -- pass the v3 asset "
                         "explicitly to measure the v3 population.")
    # Codex R4: these were hardcoded, so a run could not be pointed at v3 inputs and
    # the report could not say which population it described.
    ap.add_argument("--libraries-csv", default=DEFAULT_LIBRARIES_CSV)
    ap.add_argument("--fjms-db", default=DEFAULT_FJMS_DB)
    ap.add_argument("--fgp-db", default=DEFAULT_FGP_DB)
    ap.add_argument("--pgp-db", default=DEFAULT_PGP_DB)
    ap.add_argument("--population", choices=("legacy", "pinned"), default="legacy",
                    help="LABEL for the report: what population these inputs "
                         "represent. A reuse number without this label is unusable "
                         "-- it is the mistake the retracted ~$4 estimate made.")
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
        libraries_csv=args.libraries_csv,
        fjms_db=args.fjms_db,
        fgp_db=args.fgp_db,
        pgp_db=args.pgp_db,
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
    # Codex R4: record WHICH inputs produced this number. Without these hashes the
    # report cannot be told apart from a run over a different population, which is
    # exactly how the ~$4 figure survived as long as it did.
    def _hash(path):
        try:
            digest = hashlib.sha256()
            with open(path, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        except OSError as exc:
            return f"(unreadable: {type(exc).__name__})"

    inputs = {
        "asset": args.asset, "cache": args.cache, "libraries_csv": args.libraries_csv,
        "fjms_db": args.fjms_db, "fgp_db": args.fgp_db, "pgp_db": args.pgp_db,
    }
    log("hashing inputs (so the report can name the population it measured)")
    input_hashes = {name: _hash(path) for name, path in inputs.items()}

    report = {
        "measured_utc_note": "timestamp intentionally omitted -- see git commit date",
        # THE label. `legacy` means this measures how much of the existing cache
        # still answers ITS OWN questions -- NOT the v3 population, which does not
        # exist until the router and final work set are fixed.
        "population": args.population,
        "inputs": inputs,
        "input_sha256": input_hashes,
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
