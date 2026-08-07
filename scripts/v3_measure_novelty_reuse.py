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
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Dict

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


class MeasurementError(RuntimeError):
    """Fail-closed error taking a reuse measurement."""


def _hash_or_die(path, name: str) -> str:
    """SHA-256 of `path`, or raise.

    Codex R5 (HIGH): the first version converted an unreadable input into the
    STRING "(unreadable: ...)" and still exited zero, so a report could describe
    candidates whose inputs were not all hash-bound while still looking like
    option-0 evidence. An unhashable input means the measurement cannot say what it
    measured, which is the one thing this report exists to say.
    """
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                digest.update(chunk)
    except OSError as exc:
        raise MeasurementError(
            f"cannot hash input {name!r}: {type(exc).__name__}. Refusing to write a "
            f"report whose population is unverifiable."
        ) from exc
    return digest.hexdigest()


# SQLite sidecars that can hold committed content the main file does not yet show.
# Hashing only the main path lets candidate input change while its hash is stable
# (Codex round 6, HIGH).
_SQLITE_SIDECAR_SUFFIXES = ("-journal", "-wal", "-shm")

# The COMPLETE set of `meta.coverage_routing` values `finalize_build` writes. Kept in
# step with the writer by `test_the_routing_modes_match_what_finalize_build_writes`.
# The CLOSED set of values `finalize_build` writes to `meta.coverage_routing`.
# `gen2_router_split_regrained` was added 2026-08-07 with option 4: gen-2's router
# decision OVERLAID with split-grain (book/tractate) decisions for the keys it only
# scored at the collapsed grain. It is a distinct population from `gen2_router` --
# 141,358 more tier-A rows carry a decision -- so it must be a distinct label, not
# folded into the old one. Pinned by
# `test_the_routing_modes_match_what_finalize_build_writes`, which derives the
# writer's alternatives from `finalize_build`'s own meta expression and fails if
# that expression and this set disagree.
_V3_ROUTING_MODES = frozenset({
    "gen2_router", "gen2_router_split_regrained", "lever1_cliff", "none",
})


def _hash_all(inputs: Dict[str, str]) -> Dict[str, str]:
    """Hash every input AND any SQLite sidecar that exists beside it.

    A sidecar that is ABSENT is recorded as absent rather than skipped: one
    appearing mid-measurement is exactly the change this needs to detect, and
    silently omitting it from the first pass would make the second pass agree.
    """
    out: Dict[str, str] = {}
    for name, path in inputs.items():
        out[name] = _hash_or_die(path, name)
        for suffix in _SQLITE_SIDECAR_SUFFIXES:
            sidecar = f"{path}{suffix}"
            key = f"{name}{suffix}"
            out[key] = (_hash_or_die(sidecar, key) if os.path.exists(sidecar)
                        else "(absent)")
    return out


def _verify_population(asset_path: str, claimed: str) -> Dict[str, object]:
    """Check the `--population` LABEL against the asset itself.

    A v3 asset is one whose `meta` records `coverage_routing = 'gen2_router'` -- the
    row `finalize_build` writes when it ingests gen-2's router, which is precisely
    what makes a population "v3". Anything else is legacy. Returns the evidence so
    the report carries the basis for its own label rather than just the label.
    """
    coverage_routing = None
    sidecar_version = None
    try:
        conn = sqlite3.connect(
            Path(asset_path).resolve().as_uri() + "?mode=ro", uri=True)
        try:
            meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        finally:
            conn.close()
        coverage_routing = meta.get("coverage_routing")
        sidecar_version = meta.get("sidecar_version")
    except sqlite3.Error as exc:
        raise MeasurementError(
            f"cannot read `meta` from the asset ({type(exc).__name__}), so the "
            f"population label cannot be verified."
        ) from exc

    # WHAT MAKES A POPULATION "v3" (Codex round 6, MEDIUM -- and the finding was
    # right). The first version equated v3 with `coverage_routing == 'gen2_router'`,
    # which mislabels a SUPPORTED build: `finalize_build` deliberately allows
    # `allow_lever1_coverage=True` and records `coverage_routing = 'lever1_cliff'`,
    # and such a build is still a v3 assembly with a v3 work set -- its candidate
    # population is v3 even though its routing is the legacy cliff. Rejecting it as
    # `pinned` made the measurement unavailable for a choice the build offers.
    #
    # So the test is whether the asset was built by THIS pipeline at all: it carries
    # a `coverage_routing` meta row, which only a v3-era `finalize_build` writes. The
    # ROUTING MODE is reported separately, so a reader can tell a router-routed v3
    # population from a deliberately cliff-routed one without either being mislabelled.
    # A CLOSED vocabulary (Codex round 7, MEDIUM): accepting any non-null value
    # "replaces one unsound proxy with another". These are exactly the three values
    # `finalize_build` writes -- `gen2_router` (router ingested), `lever1_cliff`
    # (legacy cliff chosen deliberately), `none` (no coverage routing ran because
    # D-17 was inactive). An unknown value means the writer changed, or the meta row
    # was hand-edited, and either way the label cannot be derived from it.
    if coverage_routing is not None and coverage_routing not in _V3_ROUTING_MODES:
        raise MeasurementError(
            f"the asset's meta.coverage_routing is {coverage_routing!r}, which is not "
            f"one of the {sorted(_V3_ROUTING_MODES)} values this pipeline writes. "
            f"Refusing to derive a population label from an unrecognised value."
        )
    is_v3_build = coverage_routing is not None
    if claimed == "pinned" and not is_v3_build:
        raise MeasurementError(
            f"--population pinned was claimed, but the asset records no "
            f"`meta.coverage_routing` row at all (sidecar_version="
            f"{sidecar_version!r}), so it predates the v3 pipeline. This is a "
            f"LEGACY-population measurement; labelling it `pinned` would present it "
            f"as the v3 price, the error that produced the retracted ~$4 figure."
        )
    if claimed == "legacy" and is_v3_build:
        raise MeasurementError(
            f"--population legacy was claimed, but the asset was built by the v3 "
            f"pipeline (meta.coverage_routing={coverage_routing!r}). Label it "
            f"`pinned`; an under-claimed number gets ignored as stale, so the real "
            f"price never reaches the owner."
        )
    return {"meta_coverage_routing": coverage_routing,
            "meta_sidecar_version": sidecar_version,
            # Reported so a `pinned` number is never ambiguous about WHICH v3
            # routing produced its population.
            "routing_mode": coverage_routing or "(pre-v3: no coverage_routing row)"}


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

    # Codex R5+R6 (HIGH): hash EVERY input BEFORE anything reads it, and re-verify
    # after. R5 fixed the candidate build; R6 found two remaining holes, both real:
    #   * the cache was LOADED before it was hashed, so a change in between left the
    #     counters computed from the old object while the report recorded the new
    #     hash -- and the stable second hash then falsely confirmed it;
    #   * SQLite can serve committed content from sibling `-journal`/`-wal` files,
    #     which were never hashed, so candidate input could change while both
    #     main-file hashes agreed.
    inputs = {
        "asset": args.asset, "cache": args.cache, "libraries_csv": args.libraries_csv,
        "fjms_db": args.fjms_db, "fgp_db": args.fgp_db, "pgp_db": args.pgp_db,
    }
    log("hashing every input BEFORE any of them is read")
    input_hashes = _hash_all(inputs)

    # The population check reads the asset, so it runs AFTER the hash (Codex round 7,
    # HIGH). Previously it ran first, so an asset changed between the verification and
    # the hash would leave the report carrying a population claim from the OLD state
    # while its hashes -- and the final re-verification -- described the new one.
    population_evidence = _verify_population(args.asset, args.population)
    log(f"population label {args.population!r} verified against the hashed asset: "
        f"{population_evidence}")

    log(f"reading cache {os.path.basename(args.cache)}")
    with open(args.cache, encoding="utf-8") as fh:
        cache = json.load(fh)
    log(f"  cache entries: {len(cache):,}")

    log("building candidates from the supplied asset + finding-aid DBs "
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

    # Re-verify every input -- including SQLite sidecars -- is byte-identical to
    # what was hashed before anything read it.
    log("re-verifying every input hash AFTER the measurement")
    again = _hash_all(inputs)
    changed = sorted(k for k in input_hashes if again.get(k) != input_hashes[k])
    changed += sorted(k for k in again if k not in input_hashes)
    if changed:
        raise MeasurementError(
            f"{len(changed)} input(s) CHANGED during the measurement "
            f"({', '.join(changed)}), so the reported counts describe a population "
            f"these hashes do not cover. Refusing to write a report that would look "
            f"pinned."
        )

    report = {
        "measured_utc_note": "timestamp intentionally omitted -- see git commit date",
        # THE label -- and it is VERIFIED against the asset, not taken on the
        # caller's word (Codex R5, HIGH: "`--population` is freely selected by the
        # caller and is not derived from, or validated against, the input asset",
        # so "a legacy invocation can be labelled `pinned`"). `_verify_population`
        # below refuses the mismatch.
        "population": args.population,
        "population_verified_against": population_evidence,
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


def _cli(argv=None) -> int:
    try:
        return main(argv)
    except MeasurementError as exc:
        print(f"FAIL (fail-closed): {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(_cli())
