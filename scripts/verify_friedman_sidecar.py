#!/usr/bin/env python3
"""Verify a Friedman sidecar FILE on its own: the deploy gate (scripts/deploy_friedman_sidecar.ps1).

    python scripts/verify_friedman_sidecar.py --db friedman_data/friedman.db \
        --expect-units 7797 --expect-index-rows 3292 --expect-tokens 198096

Unlike ``build_friedman_sidecar.py verify``, which binds the sidecar to the candidate files it
was built from (never on the server), this needs only the file and the code: it must be one
``shared/friedman_service.py`` would serve (schema, status, normalizer, recovery, index and
sidecar versions, the credit line, ``quick_check``), its tables must hold exactly the counts
given and the counts its own ``meta`` claims, and every ``tokens`` row must be what
``scope_keys`` makes of the head and quotation texts the file holds, so a file keyed by another
normalizer or another token rule is refused even when its versions say otherwise.

Exit 0 and a JSON summary on success; exit 1 and the reason on any failure. A missing file
FAILS; nothing here skips.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.friedman_service import (  # noqa: E402
    SCOPE_HEAD, SCOPE_QUOTATION, check_sidecar, head_scope_text, scope_keys,
)


def verify(db: Path, expect_units: int, expect_index_rows: int, expect_tokens: int) -> dict:
    """The summary of a file that passes; raises ValueError with the reason otherwise."""
    reason, meta = check_sidecar(str(db))
    if reason:
        raise ValueError(f"the reader would not serve {db}: {reason}")
    conn = sqlite3.connect(f"file:{db.resolve().as_posix()}?mode=ro", uri=True)
    try:
        counts = {name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                  for name in ("units", "index_forms", "tokens", "quotations", "unit_keys")}
        for name, expected, claimed in (("units", expect_units, meta.get("units")),
                                        ("index_forms", expect_index_rows, meta.get("index_rows")),
                                        ("tokens", expect_tokens, meta.get("tokens"))):
            if counts[name] != expected:
                raise ValueError(f"{name}: {counts[name]} rows, expected {expected}")
            if str(claimed) != str(expected):
                raise ValueError(f"meta claims {name} = {claimed!r}, expected {expected}")
        stored = Counter(tuple(row) for row in conn.execute(
            "SELECT unit_id, scope, n, pos, key, peeled FROM tokens"))
        recomputed: Counter = Counter()
        for unit_id, head_text in conn.execute("SELECT id, head_text FROM units"):
            for pos, key, peeled in scope_keys(head_scope_text(head_text or "")):
                recomputed[(unit_id, SCOPE_HEAD, 0, pos, key, peeled)] += 1
        for unit_id, n, quote in conn.execute("SELECT unit_id, n, quote FROM quotations"):
            for pos, key, peeled in scope_keys(quote or ""):
                recomputed[(unit_id, SCOPE_QUOTATION, n, pos, key, peeled)] += 1
        if stored != recomputed:
            extra = sum((stored - recomputed).values())
            missing = sum((recomputed - stored).values())
            raise ValueError(f"tokens differ from the texts: {extra} row(s) the texts do not give, "
                             f"{missing} missing; the file was keyed another way")
        if not conn.execute("SELECT 1 FROM unit_keys LIMIT 1").fetchone():
            raise ValueError("no head keys at all")
    finally:
        conn.close()
    return {"verified": True, "db": str(db), "sha256": hashlib.sha256(db.read_bytes()).hexdigest(),
            "units": counts["units"], "quotations": counts["quotations"], "index_rows": counts["index_forms"],
            "tokens": counts["tokens"], "unit_keys": counts["unit_keys"], "status": meta.get("status", ""),
            "recovery_version": meta.get("recovery_version", ""), "built_at": meta.get("built_at", "")}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=ROOT / "friedman_data" / "friedman.db")
    parser.add_argument("--expect-units", type=int, required=True)
    parser.add_argument("--expect-index-rows", type=int, required=True)
    parser.add_argument("--expect-tokens", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(verify(args.db, args.expect_units, args.expect_index_rows, args.expect_tokens),
                         ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, sqlite3.Error) as exc:
        print(f"Friedman sidecar REFUSED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
