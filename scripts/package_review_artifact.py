# -*- coding: utf-8 -*-
"""The gate the v5 review artifact must pass before it leaves this machine.

Everything here is FAIL-CLOSED: any check that cannot be performed counts as a
failure, never as a pass. The order matters and is the plan's:

  1. NOTHING MAY STILL BE WRITING. A viewer holding the db would keep rebuilding
     `facet_row`, so a scan could run against a file that changes underneath it.
  2. NO JOURNAL OR WAL SIDE FILES. A `-journal`/`-wal`/`-shm` file beside the db
     means a transaction is unfinished; shipping the db without it can ship a
     torn state, and shipping it with it leaks nothing but confuses the
     recipient. Both are refused.
  3. INTEGRITY AND FOREIGN KEYS, on the db and on the grades sidecar.
  4. THE MASKING SCAN, on the db AND the grades sidecar AND every other file in
     the bundle. `MASKING_SCAN_PATTERNS_FILE` must be set and non-empty -- a
     zero-pattern scan is a false green and is refused by the scanner itself.
  5. THE KEY FILE MUST NOT BE IN THE BUNDLE. It resolves the masked corpora and
     lives outside the repo by standing rule.
  6. THE MANIFEST: every file that will be sent, its size and its sha256, so the
     recipient can verify what arrived and we know what left.

The Access index is checked like any other bundle file, with one deliberate
exception recorded here: it CARRIES the real base file names for the masked
corpora, by explicit owner authorization (2026-08-30, reconfirmed 2026-09-01),
because the recipient already holds those corpora. That is why its own exporter
masking-scans every basename before writing.

    python -X utf8 scripts/package_review_artifact.py
    python -X utf8 scripts/package_review_artifact.py --bundle-out handoff.txt
"""
import argparse
import hashlib
import os
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

DEFAULT_DB = os.path.join(REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db")
KEY_FILE_NAMES = ("sourcekeys.json",)
SIDE_SUFFIXES = ("-journal", "-wal", "-shm")


class GateError(SystemExit):
    pass


def _sha256(path, chunk=1 << 22):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _fmt(n):
    return f"{n / (1024 * 1024):,.1f} MB"


def no_writer_holds(db_path, say):
    """Refuse while another process can still write the db."""
    try:
        import subprocess
        # OUR OWN PID IS EXCLUDED. Passing `--also scripts/serve_v3_review.py`
        # puts that name in this process's command line, so the first version of
        # this check reported itself as a live writer -- a gate that fails on the
        # act of running it is worse than no gate.
        mine = os.getpid()
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name like 'python%'\" | "
             "Where-Object { $_.CommandLine -match "
             "'serve_v3_review|attach_review_cards|build_work_registry|"
             "apply_work_author' -and $_.ProcessId -ne " + str(mine) + " } | "
             "Select-Object -ExpandProperty ProcessId"],
            capture_output=True, text=True, timeout=60)
        pids = [x.strip() for x in (out.stdout or "").splitlines()
                if x.strip() and x.strip() != str(mine)]
    except Exception as e:                      # noqa: BLE001 -- fail closed
        raise GateError(f"could not check for live writers ({e}); refusing to "
                        "package a db that may be in use")
    if pids:
        raise GateError(
            "these processes can still write the artifact: %s -- stop them "
            "(the viewer rebuilds facet_row on start) and re-run" % ", ".join(pids))
    say("no writer holds the artifact")


def no_side_files(paths, say):
    """A -journal / -wal / -shm beside a db means an unfinished transaction."""
    bad = []
    for p in paths:
        for suf in SIDE_SUFFIXES:
            if os.path.exists(p + suf):
                bad.append(p + suf)
    if bad:
        raise GateError(
            "side files present -- an unfinished transaction, or a db opened "
            "read-write since the last close: %s. Open each db once with "
            "sqlite3 and close it cleanly, then re-run." % bad)
    say("no journal/wal/shm side files")


def integrity(db_path, say, label):
    con = sqlite3.connect("file:%s?mode=ro" % db_path.replace("\\", "/"),
                          uri=True, timeout=120)
    try:
        ok = con.execute("PRAGMA integrity_check").fetchone()[0]
        if ok != "ok":
            raise GateError(f"{label}: integrity_check said {ok!r}")
        fk = con.execute("PRAGMA foreign_key_check").fetchall()
        if fk:
            raise GateError(f"{label}: {len(fk)} foreign-key violations, "
                            f"first {fk[0]}")
    finally:
        con.close()
    say(f"{label}: integrity_check ok, no foreign-key violations")


def masking(paths, say):
    """The scan, on every bundle file. Fail-closed by construction: the scanner
    itself refuses to run with zero patterns loaded."""
    if not os.environ.get("MASKING_SCAN_PATTERNS_FILE"):
        raise GateError(
            "MASKING_SCAN_PATTERNS_FILE is not set. A scan with no patterns is "
            "a false green, so this is a failure, not a skip.")
    from check_atlas_masking import (build_matcher, load_patterns, scan_asset,
                                     scan_sqlite)
    patterns = load_patterns()
    if not patterns:
        raise GateError("the pattern file loaded zero patterns (false green)")
    build_matcher(patterns)          # raises if the pattern set is unusable
    total = 0
    for p in paths:
        issues = (scan_sqlite(p, patterns) if p.endswith(".db")
                  else scan_asset(p, patterns))
        if issues:
            for i in issues[:5]:
                say(f"  MASKING HIT in {os.path.basename(p)}: {i}")
            raise GateError(f"{len(issues)} masking hit(s) in "
                            f"{os.path.basename(p)} -- nothing is packaged")
        total += 1
    say(f"masking scan clean on {total} file(s) ({len(patterns)} pattern(s))")


def key_file_absent(paths, say):
    for p in paths:
        if os.path.basename(p).lower() in KEY_FILE_NAMES:
            raise GateError(f"the key file is in the bundle: {p}")
        d = os.path.dirname(os.path.abspath(p))
        for name in KEY_FILE_NAMES:
            if os.path.exists(os.path.join(d, name)):
                raise GateError(
                    f"a key file sits in a bundle directory: "
                    f"{os.path.join(d, name)} -- move it outside before packaging")
    say("no key file in the bundle or beside it")


def manifest(paths, say):
    lines = ["v5 review artifact -- handoff manifest", ""]
    for p in paths:
        size, digest = os.path.getsize(p), _sha256(p)
        lines.append(f"{os.path.basename(p)}")
        lines.append(f"    size   {size} bytes ({_fmt(size)})")
        lines.append(f"    sha256 {digest}")
        say(f"  {os.path.basename(p):34s} {_fmt(size):>12s}  {digest[:16]}..")
    return "\n".join(lines) + "\n"


def run(db_path, extra, say=print, bundle_out=None):
    grades = db_path + ".grades.db"
    dbs = [db_path] + ([grades] if os.path.exists(grades) else [])
    bundle = list(dbs)
    for p in extra:
        if os.path.exists(p):
            bundle.append(p)
        else:
            raise GateError(f"bundle file missing: {p}")
    say("=" * 62)
    say("PACKAGING GATE -- every check fail-closed")
    say("=" * 62)
    no_writer_holds(db_path, say)
    no_side_files(dbs, say)
    for p in dbs:
        integrity(p, say, os.path.basename(p))
    masking(bundle, say)
    key_file_absent(bundle, say)
    say("bundle:")
    text = manifest(bundle, say)
    if bundle_out:
        with open(bundle_out, "w", encoding="utf-8") as fh:
            fh.write(text)
        say(f"manifest written: {bundle_out}")
    say("=" * 62)
    say("PASS -- the bundle above may be sent")
    return bundle


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--also", nargs="*", default=[
        os.path.join(REPO_ROOT, "scripts", "serve_v3_review.py"),
        os.path.join(REPO_ROOT, "docs", "v5-review-quickstart.md"),
    ], help="the other files that travel with the db")
    ap.add_argument("--bundle-out", default=None)
    args = ap.parse_args(argv)
    run(args.db, args.also, bundle_out=args.bundle_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
