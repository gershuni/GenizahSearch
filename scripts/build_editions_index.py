# -*- coding: utf-8 -*-
"""Build a generation of the editions index from the FGP and PGP sidecars.

Editions search plan (private repository), PR 1: nothing serves the index yet. The build
writes ``<out>`` (default ``<INDEX_DIR>/Editions.new``) and, with ``--promote``, puts it in
place of ``<INDEX_DIR>/Editions`` (the old one kept as ``Editions.prev``). Check it with
``scripts/verify_editions_index.py``.

    python scripts/build_editions_index.py --out <dir> [--force] [--promote]

Inputs default to the repository's own sidecars and the main index's browse map; each can
be named. The build takes about a minute.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def default_inputs():
    from shared.config import Config
    return {
        "fgp_db": os.path.join(ROOT, "fgp_data", "fgp_transcriptions.db"),
        "pgp_db": os.path.join(ROOT, "pgp_data", "pgp.db"),
        "browse_map": Config.BROWSE_MAP,
        "nli_db": os.path.join(ROOT, "nli_data", "nli_crossref.db"),
    }


def main(argv=None) -> int:
    from shared.editions_indexer import (
        build_generation, check_nli_crossref, default_generation_dir, promote_generation)

    defaults = default_inputs()
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--out", default=default_generation_dir() + ".new")
    p.add_argument("--fgp-db", default=defaults["fgp_db"])
    p.add_argument("--pgp-db", default=defaults["pgp_db"])
    p.add_argument("--browse-map", default=defaults["browse_map"])
    p.add_argument("--nli-db", default=defaults["nli_db"])
    p.add_argument("--force", action="store_true", help="replace an existing --out")
    p.add_argument("--promote", action="store_true",
                   help="then replace <INDEX_DIR>/Editions with --out (local builds only)")
    args = p.parse_args(argv)
    # The NLI crossref is required too: without it no FGP per-image text is placed on its
    # page, and a generation built so would still pass a gate that compares with itself.
    for name in ("fgp_db", "pgp_db", "browse_map", "nli_db"):
        path = getattr(args, name)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            print(f"missing input --{name.replace('_', '-')}: {path}", file=sys.stderr)
            return 2
    try:
        check_nli_crossref(args.nli_db)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 2
    started = time.perf_counter()
    meta = build_generation(args.out, fgp_db=args.fgp_db, pgp_db=args.pgp_db,
                            browse_map_path=args.browse_map, nli_db=args.nli_db,
                            force=args.force, progress=print)
    counts = meta["counts"]
    print(f"build {meta['build_id']}: {counts['documents']:,} documents "
          f"(FGP {counts['documents_by_source']['fgp']:,}, PGP {counts['documents_by_source']['pgp']:,}), "
          f"{counts['manuscripts']:,} manuscripts, {counts['also_ids']:,} identical texts folded; "
          f"located {counts['located']}; {time.perf_counter() - started:.0f} s")
    if args.promote:
        promote_generation(args.out, default_generation_dir())
        print(f"promoted to {default_generation_dir()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
