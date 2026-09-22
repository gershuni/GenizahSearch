#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download Princeton's PGP metadata exports into ``pgp_data/``, pinned to one commit.

WHY THIS EXISTS
---------------
The refresh procedure in ``docs/guides/DEPLOYMENT_TECHNICAL.md`` opened with a bare
comment -- "Download latest PGP data exports to pgp_data/" -- and no command, next to a
link that pointed at Princeton's Django *application* source, which carries no exports at
all. Step 1 was not executable, which is a large part of why the data sat five months
stale: ``pgp.db`` was built 2026-04-22 and Supabase has had no row created since.

So this is step 1, as a script. It resolves the upstream HEAD once and downloads every
file **at that pinned commit**, because ``pgp-metadata`` is auto-committed several times
a day: fetching the files one at a time from ``main`` can straddle a push and produce a
set where ``fragments.csv`` knows about documents ``documents.csv`` has never heard of.

It writes ``pgp_data/upstream_provenance.json``, which ``scripts/export_pgp_sidecar.py``
folds into ``pgp.db``'s ``meta`` table. Before that, ``meta`` recorded when the sidecar was
BUILT but nothing about how old the DATA in it was -- the thing that was actually stale.

WHAT IT DOES NOT DO
-------------------
``transcriptions_linked.csv`` is **not** an upstream file; it is generated locally from
``documents.csv`` + ``footnotes.csv`` + ``libraries.csv`` by
``scripts/pgp_transcriptions_export.py``. Run that next. Per-canvas transcription HTML
lives in a second repo (``princetongenizalab/pgp-text``) and is handled by
``scripts/import_pgp_sections.py``.

USAGE
-----
    python scripts/fetch_pgp_metadata.py --dry-run   # report sizes and deltas, write nothing
    python scripts/fetch_pgp_metadata.py             # download
    python scripts/fetch_pgp_metadata.py --commit <sha>   # pin to a specific commit

Exit codes: 0 done (or dry run), 1 download/validation failure, 2 bad input.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PGP_DATA_DIR = os.path.join(PROJECT_ROOT, "pgp_data")

UPSTREAM_REPO = "princetongenizalab/pgp-metadata"
API = "https://api.github.com/repos/" + UPSTREAM_REPO
RAW = "https://raw.githubusercontent.com/" + UPSTREAM_REPO

# (upstream path, local filename, the column every row must have). The key column is
# what makes a truncated or HTML-error-page download fail loudly instead of being
# imported as an empty corpus.
FILES = (
    ("data/documents.csv", "documents.csv", "pgpid"),
    ("data/fragments.csv", "fragments.csv", "shelfmark"),
    ("data/footnotes.csv", "footnotes.csv", "doc_relation"),
)

# A download smaller than this is a redirect, an error page or a truncation, not data.
MIN_BYTES = 100_000
USER_AGENT = "genizahsearch-pgp-refresh"


PROVENANCE_FILENAME = "upstream_provenance.json"


def verify_against_provenance(dest: str) -> list:
    """Do the CSVs on disk still match what the fetch recorded? Returns a list of problems.

    The three CSVs are replaced one at a time, so an interruption between two renames can
    leave a mixed-vintage set -- which is the exact thing pinning to one upstream commit
    is meant to prevent. The replacement cannot easily be made atomic across three files,
    so instead it is made DETECTABLE: provenance carries a SHA-256, byte count and row
    count per file, and the importer refuses to run on a set that does not match. This
    also catches a hand-edited or partially copied CSV, which no amount of atomicity would.
    """
    problems = []
    path = os.path.join(dest, PROVENANCE_FILENAME)
    if not os.path.exists(path):
        return ["%s is missing -- the CSVs were not fetched by scripts/fetch_pgp_metadata.py"
                % PROVENANCE_FILENAME]
    try:
        with open(path, "r", encoding="utf-8") as fh:
            provenance = json.load(fh)
    except (OSError, ValueError) as exc:
        return ["%s is unreadable: %s" % (PROVENANCE_FILENAME, exc)]

    files = provenance.get("files") or {}
    if not files:
        return ["%s records no files" % PROVENANCE_FILENAME]

    for filename, expected in sorted(files.items()):
        target = os.path.join(dest, filename)
        if not os.path.exists(target):
            problems.append("%s is missing" % filename)
            continue
        # Older provenance recorded a bare row count; treat that as unverifiable rather
        # than as a failure, so an existing checkout is not bricked by the new format.
        if not isinstance(expected, dict):
            problems.append("%s: provenance predates checksums; re-run "
                            "scripts/fetch_pgp_metadata.py" % filename)
            continue
        with open(target, "rb") as fh:
            raw = fh.read()
        if len(raw) != expected.get("bytes"):
            problems.append("%s: %d bytes on disk, provenance says %s"
                            % (filename, len(raw), expected.get("bytes")))
            continue
        if hashlib.sha256(raw).hexdigest() != expected.get("sha256"):
            problems.append("%s: SHA-256 does not match provenance" % filename)

    return problems


def _get(url: str, timeout: int = 300) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read()


def resolve_commit(ref: str = "main") -> dict:
    """Resolve a ref to a commit sha plus its committer date."""
    payload = json.loads(_get("%s/commits/%s" % (API, ref), timeout=60).decode("utf-8"))
    return {
        "sha": payload["sha"],
        "committed": payload["commit"]["committer"]["date"],
        "message": (payload["commit"]["message"] or "").splitlines()[0][:100],
    }


def sniff_csv(raw: bytes, key_column: str) -> tuple:
    """(row count, header list) -- or raise if this is not the CSV we asked for."""
    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    header = reader.fieldnames or []
    if key_column not in header:
        raise ValueError(
            "expected a %r column, got %s"
            % (key_column, (", ".join(header[:8]) + " ...") if header else "no header")
        )
    return sum(1 for _ in reader), header


def local_sidecar_counts() -> dict:
    """Row counts in the CURRENT pgp.db, so the report can show what the refresh moves."""
    path = os.path.join(PGP_DATA_DIR, "pgp.db")
    if not os.path.exists(path):
        return {}
    counts = {}
    # mode=ro: a bare connect() on a missing path creates a 0-byte stub that later reads
    # as "no such table".
    conn = sqlite3.connect("file:%s?mode=ro" % path.replace("\\", "/"), uri=True)
    try:
        for table in ("documents", "document_fragments", "document_footnotes"):
            try:
                counts[table] = conn.execute(
                    'SELECT COUNT(*) FROM "%s"' % table
                ).fetchone()[0]
            except sqlite3.Error:
                pass
        try:
            counts["_built"] = conn.execute(
                "SELECT value FROM meta WHERE key='created'"
            ).fetchone()[0]
        except (sqlite3.Error, TypeError):
            pass
    finally:
        conn.close()
    return counts


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would be downloaded and change nothing")
    parser.add_argument("--commit", default="main",
                        help="upstream ref or sha to pin to (default: main)")
    parser.add_argument("--dest", default=PGP_DATA_DIR,
                        help="where to write the CSVs (default: %(default)s)")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    if not os.path.isdir(args.dest):
        print("ERROR: destination does not exist: %s" % args.dest, file=sys.stderr)
        return 2

    print("Upstream: https://github.com/%s" % UPSTREAM_REPO)
    try:
        commit = resolve_commit(args.commit)
    except (urllib.error.URLError, KeyError, ValueError) as exc:
        print("ERROR: could not resolve %r: %s" % (args.commit, exc), file=sys.stderr)
        return 1
    print("  commit  %s" % commit["sha"])
    print("  dated   %s" % commit["committed"])
    print("  subject %s" % commit["message"])
    print()

    local = local_sidecar_counts()
    if local.get("_built"):
        print("Current pgp.db was built %s" % local["_built"])
        print()

    # Download everything first; write nothing until all three have validated, so a
    # failure halfway cannot leave pgp_data/ holding a mixed-vintage CSV set.
    staged = []
    for upstream_path, filename, key_column in FILES:
        url = "%s/%s/%s" % (RAW, commit["sha"], upstream_path)
        print("Fetching %s ..." % upstream_path)
        try:
            raw = _get(url)
        except urllib.error.URLError as exc:
            print("ERROR: %s: %s" % (url, exc), file=sys.stderr)
            return 1

        if len(raw) < MIN_BYTES:
            print("ERROR: %s is only %d bytes -- not a data file"
                  % (filename, len(raw)), file=sys.stderr)
            return 1
        try:
            rows, header = sniff_csv(raw, key_column)
        except ValueError as exc:
            print("ERROR: %s: %s" % (filename, exc), file=sys.stderr)
            return 1

        was = {
            "documents.csv": local.get("documents"),
            "fragments.csv": local.get("document_fragments"),
            "footnotes.csv": local.get("document_footnotes"),
        }.get(filename)
        delta = ""
        if was is not None:
            delta = "   (local pgp.db has %s, %+d)" % (format(was, ","), rows - was)
        print("  %-16s %10s rows  %8.1f MB  %2d cols%s"
              % (filename, format(rows, ","), len(raw) / 1048576.0, len(header), delta))
        staged.append((filename, raw, rows))

    print()
    if args.dry_run:
        print("Dry run: nothing written. Re-run without --dry-run to download.")
        return 0

    # Drop the old provenance BEFORE touching any CSV. If the run dies halfway through
    # the renames below, there is then no provenance vouching for a set that no longer
    # exists -- and the importer refuses to run without one.
    stale_provenance = os.path.join(args.dest, PROVENANCE_FILENAME)
    if os.path.exists(stale_provenance):
        os.remove(stale_provenance)

    # Write via a temp file per CSV, then swap, so an interrupted write cannot leave a
    # half-file that the importer would happily read as a short corpus.
    for filename, raw, _rows in staged:
        final = os.path.join(args.dest, filename)
        tmp = final + ".part"
        with open(tmp, "wb") as fh:
            fh.write(raw)
        os.replace(tmp, final)
        print("  wrote %s" % final)

    provenance = {
        "upstream_repo": UPSTREAM_REPO,
        "upstream_commit": commit["sha"],
        "upstream_committed": commit["committed"],
        "upstream_fetched": datetime.now(timezone.utc).isoformat(),
        # Per-file checksums so a partially-replaced or hand-edited set is DETECTABLE at
        # import time. The three renames above are not atomic as a group and cannot
        # cheaply be made so; this makes the failure loud instead of silent.
        "files": {
            name: {
                "rows": rows,
                "bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
            for name, raw, rows in staged
        },
    }
    provenance_path = os.path.join(args.dest, PROVENANCE_FILENAME)
    with open(provenance_path, "w", encoding="utf-8") as fh:
        json.dump(provenance, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print("  wrote %s" % provenance_path)

    problems = verify_against_provenance(args.dest)
    if problems:
        print("\nERROR: the set on disk does not match what was just written:",
              file=sys.stderr)
        for problem in problems:
            print("  %s" % problem, file=sys.stderr)
        return 1
    print("  verified: all %d files match their recorded checksums" % len(staged))

    print()
    print("Next: python scripts/pgp_transcriptions_export.py")
    print("      (generates transcriptions_linked.csv, which import_pgp_full.py requires")
    print("       and which is NOT an upstream file)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
