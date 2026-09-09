# -*- coding: utf-8 -*-
"""Required full-data verification of a review artifact. FAILS on missing data.

WHY THIS IS SEPARATE FROM THE TEST SUITE
----------------------------------------
`tests/test_verify_v3_review_offsets.py` proves the offset verifier can DETECT a
bad coordinate. It does that against a tiny synthetic fixture, in about a
second, because doing it against the real 3.45 GB / 519,382-row artifact cost
3,396 seconds -- 66.5% of the entire non-GUI test suite (measured 2026-09-09).

But "the verifier rejects bad coordinates" is not "the artifact is correct".
Something still has to check the real rows. That is this script, and it is
deliberately NOT a pytest test, because a test that skips when its data is
absent reports success for a check it never ran -- which is exactly how the
expensive check ended up running on one machine and nowhere else.

So the contract here is inverted: **missing data is a FAILURE, not a skip.**
If the DB, the source keys, or the corpus file are absent, this exits non-zero
and says which one. There is no configuration under which it silently passes.

WHEN TO RUN IT
--------------
1. Nightly on the machine that holds the data (Windows Task Scheduler):
       python -X utf8 scripts/verify_review_artifact.py
2. Before promoting ANY rebuilt review artifact. A previous nightly green
   certifies the DB it ran against, not a newly built one -- `--expect-rows`
   and the recorded identity below are what tie a result to a specific file.

Exit codes: 0 verified · 1 verification failed · 2 required data missing.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERIFIER = os.path.join(REPO_ROOT, "scripts", "verify_v3_review_offsets.py")

DEFAULT_DB = os.path.join(REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db")
DEFAULT_KEYS = os.path.join(os.path.expanduser("~"), ".genizah-private",
                            "sourcekeys.json")
DEFAULT_CORPUS = os.path.join(REPO_ROOT, "Transcriptions.txt")

EXIT_OK, EXIT_FAILED, EXIT_MISSING_DATA = 0, 1, 2


def _git(*args):
    try:
        out = subprocess.run(("git",) + args, cwd=REPO_ROOT,
                             capture_output=True, text=True, timeout=30)
        return (out.stdout or "").strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def _sha256(path: str, chunk: int = 1 << 22) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            b = fh.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _row_count(db: str):
    import sqlite3
    try:
        con = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
        n = con.execute("SELECT COUNT(*) FROM review_row").fetchone()[0]
        con.close()
        return n
    except sqlite3.Error as exc:
        return "unreadable (%s)" % exc


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--sourcekeys", default=DEFAULT_KEYS)
    ap.add_argument("--corpus-file", default=DEFAULT_CORPUS)
    ap.add_argument("--max-status-fail", type=int, default=0,
                    help="rows allowed to carry a non-ok provenance status "
                         "(default 0 -- the final-mode budget)")
    ap.add_argument("--expect-rows", type=int, default=None,
                    help="fail unless review_row holds exactly this many rows; "
                         "use it to tie a run to a specific built artifact")
    ap.add_argument("--hash", action="store_true",
                    help="record the DB's sha256 (minutes on a multi-GB file)")
    ap.add_argument("--json-out", default=None,
                    help="append one JSON record of this run to this file")
    args = ap.parse_args(argv)

    # ---- required inputs: absent means FAIL, never skip --------------------
    missing = [("--db", args.db),
               ("--sourcekeys", args.sourcekeys),
               ("--corpus-file", args.corpus_file)]
    missing = [(flag, p) for flag, p in missing if not os.path.exists(p)]
    if missing:
        print("REQUIRED DATA MISSING -- this is a failure, not a skip:")
        for flag, p in missing:
            print("  %-15s %s" % (flag, p))
        print("\nThis script exists to be the one place that does NOT pass "
              "when the artifact cannot be checked. Point it at the data, or "
              "record that the artifact is unverified.")
        return EXIT_MISSING_DATA

    rows = _row_count(args.db)
    identity = {
        "when": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "host": platform.node(),
        "commit": _git("rev-parse", "HEAD"),
        "dirty": bool(_git("status", "--porcelain")),
        "db": os.path.abspath(args.db),
        "db_bytes": os.path.getsize(args.db),
        "db_mtime": time.strftime("%Y-%m-%dT%H:%M:%S",
                                  time.localtime(os.path.getmtime(args.db))),
        "review_rows": rows,
        "sourcekeys": os.path.abspath(args.sourcekeys),
        "corpus_file": os.path.abspath(args.corpus_file),
        "verifier_sha256": _sha256(VERIFIER)[:16],
    }
    if args.hash:
        print("hashing %s ..." % args.db, flush=True)
        identity["db_sha256"] = _sha256(args.db)

    print("=" * 70)
    print(" Review-artifact verification")
    print("=" * 70)
    for k in ("when", "host", "commit", "dirty", "db", "db_bytes", "db_mtime",
              "review_rows", "verifier_sha256"):
        print("  %-16s %s" % (k, identity[k]))
    if "db_sha256" in identity:
        print("  %-16s %s" % ("db_sha256", identity["db_sha256"]))
    print()

    if args.expect_rows is not None and rows != args.expect_rows:
        print("ROW COUNT MISMATCH: %s present, %d expected. A green run "
              "certifies the artifact it ran against, so this is a failure."
              % (rows, args.expect_rows))
        identity["result"] = "row_count_mismatch"
        _record(args.json_out, identity)
        return EXIT_FAILED

    cmd = [sys.executable, "-X", "utf8", "-u", VERIFIER,
           "--db", args.db,
           "--sourcekeys", args.sourcekeys,
           "--corpus-file", args.corpus_file,
           "--all", "--max-status-fail", str(args.max_status_fail)]
    print("running: %s\n" % " ".join(cmd), flush=True)
    env = dict(os.environ, PYTHONUTF8="1")
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=REPO_ROOT, env=env)
    dur = time.time() - t0

    identity["seconds"] = round(dur, 1)
    identity["returncode"] = proc.returncode
    identity["result"] = "verified" if proc.returncode == 0 else "FAILED"
    _record(args.json_out, identity)

    print("\n%s in %.1f min" % (identity["result"], dur / 60.0))
    return EXIT_OK if proc.returncode == 0 else EXIT_FAILED


def _record(path, identity):
    if not path:
        return
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(identity, ensure_ascii=False) + "\n")
    print("recorded -> %s" % path)


if __name__ == "__main__":
    sys.exit(main())
