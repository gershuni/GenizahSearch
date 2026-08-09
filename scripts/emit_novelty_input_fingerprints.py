"""Emit the EXPECTED per-pair novelty input fingerprints for a sidecar build.

`build_discovery_sidecar.py --novelty-input-fingerprints` needs a
`{"{sys_id}::{work_id}": fingerprint}` map describing the QUESTION each verdict
was supposed to answer, so a cached verdict whose inputs have since drifted
(retitled work, rebuilt alias group, refreshed finding aid, changed witness
map) loads as the fail-closed `not_checked` instead of being reused silently.
The cache SHA cannot do this: it proves which FILE was read, never which
question each entry answered.

INDEPENDENCE IS THE WHOLE POINT. This recomputes fingerprints from the BUILD's
own data sources -- the asset, libraries.csv, the finding-aid DBs and the
recorded-witness map -- exactly as `discovery_novelty_production_run.py` does.
It must NEVER derive them from the verdict file's own stamped
`input_fingerprint` values: comparing a file against itself passes by
construction and would turn the gate into decoration. `--verify-against` reads
the verdict file for REPORTING only, after the map is already built.

The witness map is REQUIRED for the same reason it is required to run the gate:
`known_witness_confidence` is one of the fingerprint fields, so a map-less emit
produces fingerprints for a different question than the verdicts answered, and
every pair would mismatch.

Run:
    python scripts/emit_novelty_input_fingerprints.py \
        --asset _tmp/v3_out/discovery-v3.db \
        --work-witnesses discovery_data/work_witnesses-v1.json \
        --out _tmp/v3_novelty_input_fingerprints.json \
        --verify-against _tmp/v3_novelty_verdicts.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from typing import Optional, Sequence

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from shared.discovery_novelty import (  # noqa: E402
    BATCH_PROMPT_SHA256,
    CACHE_KEY_FIELDS,
    INPUT_NORMALIZATION_SHA256,
)
from scripts.discovery_novelty_funnel import (  # noqa: E402
    candidate_input_fingerprint,
)
from scripts.discovery_novelty_probe import (  # noqa: E402
    DEFAULT_ASSET,
    DEFAULT_FGP_DB,
    DEFAULT_FJMS_DB,
    DEFAULT_LIBRARIES_CSV,
    DEFAULT_PGP_DB,
    build_all_candidates,
    load_work_witnesses,
)


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--asset", default=DEFAULT_ASSET)
    p.add_argument("--libraries-csv", default=DEFAULT_LIBRARIES_CSV)
    p.add_argument("--fjms-db", default=DEFAULT_FJMS_DB)
    p.add_argument("--pgp-db", default=DEFAULT_PGP_DB)
    p.add_argument("--fgp-db", default=DEFAULT_FGP_DB)
    p.add_argument("--work-witnesses", default=None,
                   help="emit_work_witnesses.py output. Required unless "
                        "--allow-no-witnesses.")
    p.add_argument("--crosswalk", default=os.path.join(
                       REPO_ROOT, "discovery_data", "crosswalk.json"))
    p.add_argument("--allow-no-witnesses", action="store_true",
                   help="deliberately emit fingerprints for the witness-blind "
                        "question (only valid against a witness-blind cache)")
    p.add_argument("--out", required=True)
    p.add_argument("--verify-against", default=None,
                   help="a verdict cache to REPORT match/mismatch against. Read "
                        "after the map is built; never an input to it.")
    args = p.parse_args(argv)

    if not args.work_witnesses and not args.allow_no_witnesses:
        print("ERROR: --work-witnesses not supplied. `known_witness_confidence` "
              "is a fingerprint field, so a witness-blind emit describes a "
              "different question than a witness-aware cache answered and every "
              "pair would mismatch. Pass --allow-no-witnesses to override.")
        return 2

    witnesses = load_work_witnesses(args.work_witnesses, args.crosswalk)
    print(f"recorded-witness works: {len(witnesses):,}")

    candidates, _works, _libraries = build_all_candidates(
        args.asset, args.libraries_csv, args.fjms_db, args.pgp_db, args.fgp_db,
        work_witnesses=witnesses,
    )
    # BOTH arms, not just the residual: the heuristic arm is fingerprinted too,
    # and a heuristic verdict with no expected fingerprint would load as a miss.
    fingerprints = {
        f"{c.sys_id}::{c.ref_work_id}": candidate_input_fingerprint(
            c, prompt_sha256=BATCH_PROMPT_SHA256)
        for c in candidates
    }

    # FLAT `{key: fingerprint}` -- the builder's `_load_novelty_fingerprints`
    # contract, which rejects any non-string value. Provenance therefore goes to
    # a COMPANION file rather than being wrapped around this one: wrapping would
    # make the emit unreadable by its only consumer, and dropping the provenance
    # would leave an opaque hex map that proves nothing about which question it
    # describes.
    blob = json.dumps(fingerprints, ensure_ascii=False, sort_keys=True, indent=1)
    encoded = blob.encode("utf-8")
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    # Binary write: text mode translates newlines on Windows, so the digest
    # below would identify a string that is not the file.
    with open(args.out, "wb") as fh:
        fh.write(encoded)
    digest = hashlib.sha256(encoded).hexdigest()

    meta_path = args.out + ".meta.json"
    meta = {
        "schema": "novelty-input-fingerprints-v1",
        "describes": os.path.basename(args.out),
        "describes_sha256": digest,
        "input_fingerprint_version": "v3-2026-08-07",
        "input_fingerprint_prompt_sha256": BATCH_PROMPT_SHA256,
        "input_normalization_sha256": INPUT_NORMALIZATION_SHA256,
        "input_fingerprint_fields": list(CACHE_KEY_FIELDS),
        "witness_source_supplied": bool(args.work_witnesses),
        "asset": os.path.basename(args.asset),
        "pairs": len(fingerprints),
    }
    with open(meta_path, "wb") as fh:
        fh.write(json.dumps(meta, ensure_ascii=False, sort_keys=True,
                            indent=1).encode("utf-8"))

    print(f"wrote {args.out}")
    print(f"  candidates fingerprinted : {len(fingerprints):,}")
    print(f"  sha256                   : {digest}")
    print(f"  provenance               : {meta_path}")

    if args.verify_against:
        with open(args.verify_against, encoding="utf-8") as fh:
            verdicts = json.load(fh)
        ok = missing = mismatch = unstamped = 0
        for key, rec in verdicts.items():
            stamped = (rec or {}).get("input_fingerprint")
            expected = fingerprints.get(key)
            if expected is None:
                missing += 1
            elif not stamped:
                unstamped += 1
            elif stamped == expected:
                ok += 1
            else:
                mismatch += 1
        print(f"  --- verify against {os.path.basename(args.verify_against)} ---")
        print(f"  verdict entries          : {len(verdicts):,}")
        print(f"  fingerprint MATCH        : {ok:,}")
        print(f"  fingerprint MISMATCH     : {mismatch:,}")
        print(f"  no expected fingerprint  : {missing:,}")
        print(f"  verdict unstamped        : {unstamped:,}")
        uncovered = len(fingerprints) - ok - mismatch
        print(f"  candidates with NO verdict: {uncovered:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
