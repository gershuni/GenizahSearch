"""Fold the changelog.d/ fragments into CHANGELOG.md (a release step).

Each PR writes its changelog entry as a file of its own,
``changelog.d/YYYY-MM-DD-<slug>.md``, instead of editing CHANGELOG.md. When
every PR added its ``## [Unreleased]`` entry at the top of CHANGELOG.md, each
merge left every other open PR in conflict on that one spot, and resolving it
restarted that PR's CI (2026-10-19: three merges, each restarting every open
PR's ~50-minute run). Two fragments never conflict.

A fragment is exactly the entry CHANGELOG.md would have held: its first line is
``## [Unreleased] -- <title> (<area>, branch `<branch>`)`` with <area> one of
web, desktop, web + desktop or ci, then the body (``###`` subsections are fine,
another ``## `` heading is not).

    python scripts/collect_changelog.py           # show what would be folded in
    python scripts/collect_changelog.py --apply   # write CHANGELOG.md, delete the fragments

The entries go in newest first (by file name, which starts with the date), above
the entries CHANGELOG.md already has, as ``[Unreleased]`` entries; the release
then turns them into its ``## [X.Y.Z]`` section as before.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
FRAGMENT_DIR = REPO_ROOT / "changelog.d"
CHANGELOG = REPO_ROOT / "CHANGELOG.md"

NAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}-[a-z0-9][a-z0-9-]*\.md$")
HEADING = "## [Unreleased] -- "
AREAS = ("web", "desktop", "web + desktop", "ci")
# The whole first line, not just its start: a missing branch or an unknown area
# would otherwise be folded into CHANGELOG.md and the fragment deleted.
HEADING_RE = re.compile(
    r"^## \[Unreleased\] -- \S.*\S \((%s), branch `[^`\s]+`\)$"
    % "|".join(re.escape(a) for a in AREAS))


def fragment_paths(fragment_dir: Path) -> list:
    """The fragments, newest first (README.md is the folder's instructions)."""
    return sorted((p for p in fragment_dir.glob("*.md") if p.name != "README.md"),
                  key=lambda p: p.name, reverse=True)


def problems(path: Path) -> list:
    """What is wrong with one fragment ([] when it is fine)."""
    found = []
    if not NAME_RE.match(path.name):
        found.append("name is not YYYY-MM-DD-<lowercase-slug>.md")
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    if not lines:
        return found + ["is empty"]
    if not HEADING_RE.match(lines[0].rstrip()):
        found.append("first line is not '%s<title> (<area>, branch `<branch>`)' "
                     "with <area> one of %s" % (HEADING, ", ".join(AREAS)))
    if len(lines) < 2:
        found.append("has a heading but no body")
    if any(line.startswith("## ") for line in lines[1:]):
        found.append("has a second '## ' heading (one entry per file; use ###)")
    return found


def fold(changelog_text: str, entries: list) -> str:
    """CHANGELOG.md with `entries` above its first ``## [`` entry."""
    block = "\n\n".join(e.strip() for e in entries) + "\n\n"
    m = re.search(r"^## \[", changelog_text, re.M)
    if m is None:
        return changelog_text.rstrip("\n") + "\n\n" + block
    return changelog_text[:m.start()] + block + changelog_text[m.start():]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write CHANGELOG.md and delete the fragments")
    ap.add_argument("--dir", type=Path, default=FRAGMENT_DIR, help=argparse.SUPPRESS)
    ap.add_argument("--changelog", type=Path, default=CHANGELOG, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    paths = fragment_paths(args.dir)
    bad = {p.name: problems(p) for p in paths if problems(p)}
    if bad:
        for name, why in bad.items():
            print("! %s: %s" % (name, "; ".join(why)))
        print("Nothing written: fix the fragment(s) above first.")
        return 1
    if not paths:
        print("No fragments in %s: CHANGELOG.md is already complete." % args.dir.name)
        return 0

    entries = [p.read_text(encoding="utf-8") for p in paths]
    for p, e in zip(paths, entries):
        print("  %s  %s" % (p.name, e.strip().splitlines()[0][len(HEADING):]))
    if not args.apply:
        print("%d fragment(s) would be folded into %s (run with --apply)."
              % (len(paths), args.changelog.name))
        return 0

    text = args.changelog.read_text(encoding="utf-8")
    args.changelog.write_text(fold(text, entries), encoding="utf-8", newline="\n")
    for p in paths:
        p.unlink()
    print("%d fragment(s) folded into %s and deleted." % (len(paths), args.changelog.name))
    return 0


if __name__ == "__main__":
    sys.exit(main())
