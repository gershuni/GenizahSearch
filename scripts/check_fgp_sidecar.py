# -*- coding: utf-8 -*-
"""Check an FGP transcriptions sidecar before it is served: integrity, version, row count, checksum.

Runs on the dev machine (against the gate-4 candidate) and on the server (against the uploaded copy, before the
swap and after the restart), so one check decides both sides of the deploy. Opens the file read-only; standard
library only, so the server runs it from /tmp without a code deploy.

  python scripts/check_fgp_sidecar.py --db fgp_data/inventory/fgp_transcriptions.candidate.db --version 2.0.0 --rows 59062
  python3 /tmp/check_fgp_sidecar.py --db fgp_data/fgp_transcriptions.db.new --sha256 <hex> --version 2.0.0 --rows 59062
  --gate4-summary PATH also requires the file to be the one that run built and left unchanged (its sha256 == the
  run's meta.candidate_sha256, and fgp_meta.built_at == the run's run_at) and every check of that run to have
  passed. A summary that records no checksum (a run before 2026-10-26) is refused: rerun gate 4.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", required=True)
    ap.add_argument("--version", required=True, help="the fgp_meta version the file must carry")
    ap.add_argument("--rows", type=int, required=True, help="the fgp_transcriptions row count the file must have")
    ap.add_argument("--sha256", help="the checksum the file must have")
    ap.add_argument("--gate4-summary", help="gate4_summary.json of the run that built the file")
    args = ap.parse_args(argv)
    problems = []
    if not os.path.isfile(args.db):
        print(f"FAIL: {args.db} does not exist")
        return 1
    digest = sha256_of(args.db)
    print(f"file     {args.db} ({os.path.getsize(args.db):,} bytes)")
    print(f"sha256   {digest}")
    if args.sha256 and digest != args.sha256.lower():
        problems.append(f"sha256 {digest} != expected {args.sha256}")
    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    try:
        integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
        meta = dict(con.execute("SELECT key, value FROM fgp_meta").fetchall())
        rows = con.execute("SELECT COUNT(*) FROM fgp_transcriptions").fetchone()[0]
        bad = con.execute("SELECT COUNT(*) FROM fgp_transcriptions WHERE sys_id IS NULL OR sys_id = '' "
                          "OR content IS NULL OR content = ''").fetchone()[0]
    finally:
        con.close()
    print(f"integrity {integrity}; version {meta.get('version')}; rows {rows:,}; rows without sys_id or text {bad}; "
          f"built_at {meta.get('built_at')}")
    if integrity != "ok":
        problems.append(f"integrity_check: {integrity}")
    if meta.get("version") != args.version:
        problems.append(f"version {meta.get('version')} != {args.version}")
    if rows != args.rows:
        problems.append(f"rows {rows} != {args.rows}")
    if bad:
        problems.append(f"{bad} rows without sys_id or text")
    if args.gate4_summary:
        with open(args.gate4_summary, encoding="utf-8") as fh:
            summary = json.load(fh)
        failed = [c["name"] for c in summary.get("checks", []) if not c.get("passed")]
        print(f"gate 4   run_at {summary['meta'].get('run_at')}; digest {summary['meta'].get('digest', '')[:16]}; "
              f"checks {len(summary.get('checks', []))}, failed {len(failed)}")
        if failed:
            problems.append(f"gate 4 run failed: {failed}")
        recorded = (summary["meta"].get("candidate_sha256") or "").lower()
        if not recorded:
            problems.append("the gate-4 summary records no candidate_sha256; rerun gate 4")
        elif digest != recorded:
            problems.append(f"sha256 {digest} != the file gate 4 validated ({recorded}): the file changed after the run")
        if meta.get("built_at") != summary["meta"].get("run_at"):
            problems.append(f"the file was built at {meta.get('built_at')}, not by the summarised run ({summary['meta'].get('run_at')})")
    for p in problems:
        print(f"FAIL: {p}")
    print("RESULT: " + ("FAIL" if problems else "PASS"))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
