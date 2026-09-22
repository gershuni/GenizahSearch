#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Refuse to build an installer around a ``pgp.db`` that must not ship.

WHY THIS EXISTS
---------------
On 2026-09-21 the owner ruled that the regenerated Hebrew PGP translations are not shipped
on either surface: a 180-row audit graded 27.8% of them materially wrong (95% CI
21.2-34.3%). The website never received them. The desktop was handled by lifting the
``pgp_translations`` table out of ``pgp_data/pgp.db``, because ``GenizahSearchPro.spec``
bundles that file into every installer.

But ``pgp_data/*.db`` is gitignored, so **that removal is local file state, not a property
of the repository**. Nothing in the committed build path stopped the next installer from
packaging a sidecar that still had the table -- on a build host that kept the old file, on
a fresh clone that was populated from a backup, or simply after someone ran
``scripts/restore_pgp_translations.py`` to measure against the old corpus and forgot to
undo it. The owner's decision would then be quietly reversed by a build.

So the decision is enforced here, in a committed file, on the committed build path.

It also rejects a sidecar built before ``export_pgp_sidecar.py`` v1.1.0, which silently
dropped ``documents.doc_relation`` -- shipping one of those means shipping 891 documents
whose translations are presented as transcriptions.

USAGE
-----
    python scripts/check_shipping_sidecar.py
    python scripts/check_shipping_sidecar.py --allow-translations   # one-off override

Called by ``build_app.bat`` before PyInstaller. Exit codes: 0 fit to ship, 1 not.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SIDECAR = os.path.join(PROJECT_ROOT, "pgp_data", "pgp.db")

# Where PyInstaller leaves the bundled copy. Checking the SOURCE sidecar is not enough:
# CompileScriptGenizah.iss packages dist\GenizahSearchPro recursively and never evaluates
# the spec, so an installer compiled by hand against a stale dist ships whatever is there.
BUNDLED_SIDECARS = (
    os.path.join(PROJECT_ROOT, "dist", "GenizahSearchPro", "_internal", "pgp_data", "pgp.db"),
    os.path.join(PROJECT_ROOT, "dist", "GenizahSearchPro", "pgp_data", "pgp.db"),
)

# Tables that exist locally but must NOT reach a user. One line to reverse, in the one
# place a reader would look. See docs/plans/PGP_TRANSLATION_QUALITY.md.
WITHHELD_TABLES = {
    "pgp_translations": (
        "the 2026-09-21 Hebrew translations, ~28% of which are materially wrong "
        "(owner decision; docs/plans/PGP_TRANSLATION_QUALITY.md)"
    ),
}

REQUIRED_TABLES = (
    "documents", "document_sources", "document_footnotes", "document_fragments", "meta",
)

# Carried from Supabase since export_pgp_sidecar.py 1.1.0. Its absence means the sidecar
# predates the fix and would ship translations labelled as transcriptions.
REQUIRED_DOCUMENT_COLUMNS = ("doc_relation",)


def _ro(path: str) -> sqlite3.Connection:
    """Read-only. A bare connect() on a missing path creates a 0-byte stub that later
    reads as 'no such table' -- a trap this repo has hit before."""
    return sqlite3.connect("file:%s?mode=ro" % path.replace("\\", "/"), uri=True)


def check_sidecar(path: str, allow_withheld: bool = False,
                  allow_stale_schema: bool = False) -> list:
    """Return a list of reasons this sidecar must not ship. Empty list = fit to ship."""
    problems = []

    if not os.path.exists(path):
        return ["%s does not exist" % path]

    conn = _ro(path)
    try:
        tables = {
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }

        for table in REQUIRED_TABLES:
            if table not in tables:
                problems.append("missing required table %r" % table)

        for table, reason in sorted(WITHHELD_TABLES.items()):
            if table in tables:
                count = conn.execute('SELECT COUNT(*) FROM "%s"' % table).fetchone()[0]
                message = ("contains %r (%s rows) -- %s"
                           % (table, format(count, ","), reason))
                if allow_withheld:
                    print("WARNING: sidecar %s (--allow-translations)" % message)
                else:
                    problems.append(message)

        if "documents" in tables:
            columns = {r[1] for r in conn.execute("PRAGMA table_info(documents)")}
            for column in REQUIRED_DOCUMENT_COLUMNS:
                if column not in columns:
                    message = (
                        "documents has no %r column -- this sidecar predates "
                        "export_pgp_sidecar.py 1.1.0 and would ship translation-flagged "
                        "documents as transcriptions. Rebuild it." % column
                    )
                    if allow_stale_schema:
                        print("WARNING: %s (--allow-stale-schema)" % message)
                    else:
                        problems.append(message)
    except sqlite3.Error as exc:
        problems.append("could not be read as a SQLite database: %s" % exc)
    finally:
        conn.close()

    return problems


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sidecar", default=DEFAULT_SIDECAR,
                        help="the pgp.db about to be bundled (default: %(default)s)")
    parser.add_argument("--allow-translations", action="store_true",
                        help="build anyway with the withheld tables present (one-off)")
    parser.add_argument("--allow-stale-schema", action="store_true",
                        help="build anyway from a pre-1.1.0 sidecar (one-off; it will "
                             "ship translation-flagged documents as transcriptions)")
    parser.add_argument("--bundled", action="store_true",
                        help="check the BUILT copy under dist/ instead of the source "
                             "sidecar (use before compiling the installer)")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    targets = [args.sidecar]
    if args.bundled:
        targets = [p for p in BUNDLED_SIDECARS if os.path.exists(p)]
        if not targets:
            print("No built sidecar found under dist/. Looked in:", file=sys.stderr)
            for path in BUNDLED_SIDECARS:
                print("  %s" % path, file=sys.stderr)
            print("\nBuild first (build_app.bat), or drop the stale dist/ directory.",
                  file=sys.stderr)
            return 1
        args.sidecar = targets[0]

    problems = check_sidecar(
        args.sidecar,
        allow_withheld=args.allow_translations,
        allow_stale_schema=args.allow_stale_schema,
    )
    if not problems:
        print("Sidecar fit to ship: %s" % args.sidecar)
        return 0

    print("", file=sys.stderr)
    print("REFUSING TO BUILD -- %s must not be shipped as it stands:" % args.sidecar,
          file=sys.stderr)
    for problem in problems:
        print("  - %s" % problem, file=sys.stderr)
    print("", file=sys.stderr)
    print("To withhold the translations again:", file=sys.stderr)
    print("    python scripts/export_pgp_sidecar.py    (rebuilds without them only if",
          file=sys.stderr)
    print("                                             they are already absent)", file=sys.stderr)
    print("  or drop the table from this copy, keeping the withheld sidecar as the backup.",
          file=sys.stderr)
    print("To build anyway, knowing exactly what ships:", file=sys.stderr)
    print("    --allow-translations   (withheld tables)", file=sys.stderr)
    print("    --allow-stale-schema   (pre-1.1.0 sidecar)", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
