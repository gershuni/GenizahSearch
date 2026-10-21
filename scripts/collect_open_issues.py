"""Fold the docs/open_issues.d/ fragments into docs/OPEN_ISSUES.md.

A PR that finds a new bug writes the tracker row as a file of its own,
``docs/open_issues.d/YYYY-MM-DD-<slug>.md``, instead of editing OPEN_ISSUES.md.
Every PR that added a row also moved the Quick Summary counts, the count notes
and the "Last Updated" line, so any two open PRs conflicted on the same lines
and resolving it restarted CI (2026-10-21: PR #426 and the Responsa wildcard
plan). Two fragments never conflict. This is the tracker's version of
``changelog.d/`` (scripts/collect_changelog.py).

A fragment holds one or more blocks. Each block is a bug-section heading,
exactly as the tracker writes it, followed by that section's new table rows:

    ### P3 - Low Priority
    | **Title** | `file.py` | ❌ Open (2026-10-21) | Notes |

Only the three bug sections are accepted (P1, P2, P3): they are the ones the
Quick Summary counts by section. Every row has the tracker's four cells and a
status that starts with an open mark (❌, ⏸ or ⏳), since the tracker holds only
open items. Closing a
row is still an edit of the tracker (and the archive), as before.

    python scripts/collect_open_issues.py           # show what would be folded in
    python scripts/collect_open_issues.py --apply   # write OPEN_ISSUES.md, delete the fragments

``--apply`` puts each fragment's rows at the top of its section's table, adds
them to the section's Quick Summary count and to the total, writes one count
note naming the fragments, and rewrites the "Last Updated" line with the new
total. It writes nothing if any fragment is malformed or the tracker lacks a
section, count row or "Last Updated" line it needs.
"""
from __future__ import annotations

import argparse
import datetime
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
FRAGMENT_DIR = REPO_ROOT / "docs" / "open_issues.d"
TRACKER = REPO_ROOT / "docs" / "OPEN_ISSUES.md"

NAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}-[a-z0-9][a-z0-9-]*\.md$")
# Section heading in the tracker -> its row in the Quick Summary table.
SECTIONS = {
    "### P1 - Critical": "P1 Critical Bugs",
    "### P2 - Medium": "P2 Medium Bugs",
    "### P3 - Low Priority": "P3 Low Priority",
}
OPEN_MARKS = ("❌", "⏸", "⏳")   # ❌ ⏸ ⏳ -- what the Quick Summary counts
_CELL_SPLIT = re.compile(r"(?<!\\)\|")
_SEPARATOR_RE = re.compile(r"^\|[\s:|-]+\|\s*$")
_NOTE_RE = re.compile(r"^> \d{4}-\d{2}-\d{2} ")
_LAST_UPDATED_RE = re.compile(r"^>\s*\*\*Last Updated:\*\*")


def fragment_paths(fragment_dir: Path) -> list:
    """The fragments, newest first (README.md is the folder's instructions)."""
    return sorted((p for p in fragment_dir.glob("*.md") if p.name != "README.md"),
                  key=lambda p: p.name, reverse=True)


def _cells(row: str) -> list:
    return [c.strip() for c in _CELL_SPLIT.split(row.strip())[1:-1]]


def _is_open(status: str) -> bool:
    """The status cell STARTS with an open mark: `✅ Fixed (was ❌ Open)` is not open
    (Codex review of PR #428)."""
    return status.strip().startswith(OPEN_MARKS)


def parse(text: str) -> tuple:
    """({section heading: [rows]}, [problems]) for one fragment's text."""
    blocks, found, current = {}, [], None
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return {}, ["is empty"]
    for ln in lines:
        if ln.startswith("#"):
            if ln not in SECTIONS:
                found.append("heading %r is not one of %s" % (ln, ", ".join(SECTIONS)))
                current = None
                continue
            current = ln
            blocks.setdefault(current, [])
            continue
        if current is None:
            found.append("a line comes before any section heading: %r" % ln[:60])
            continue
        if not (ln.startswith("|") and ln.endswith("|")):
            found.append("not a table row: %r" % ln[:60])
            continue
        cells = _cells(ln)
        if len(cells) != 4:
            found.append("a row has %d cells, not 4 (Issue | File | Status | Notes): %r"
                         % (len(cells), ln[:60]))
            continue
        if not _is_open(cells[2]):
            found.append("a row's status does not start with an open mark (one of %s): %r"
                         % (" ".join(OPEN_MARKS), cells[2][:40]))
            continue
        blocks[current].append(ln)
    for heading, rows in blocks.items():
        if not rows:
            found.append("section %r has no rows" % heading)
    return blocks, found


def problems(path: Path) -> list:
    """What is wrong with one fragment ([] when it is fine)."""
    found = []
    if not NAME_RE.match(path.name):
        found.append("name is not YYYY-MM-DD-<lowercase-slug>.md")
    return found + parse(path.read_text(encoding="utf-8"))[1]


def fold(tracker: str, fragments: list, today: str) -> str:
    """The tracker with the fragments' rows, counts and one count note added.

    *fragments* is [(name, {heading: [rows]})], newest first. Raises ValueError
    when the tracker lacks a section table or a Quick Summary row it needs."""
    lines = tracker.split("\n")
    added = {h: [] for h in SECTIONS}
    for _name, blocks in fragments:
        for heading, rows in blocks.items():
            added[heading].extend(rows)

    for heading, rows in added.items():
        if not rows:
            continue
        try:
            start = lines.index(heading)
        except ValueError:
            raise ValueError("the tracker has no %r section" % heading) from None
        sep = next((i for i in range(start + 1, len(lines))
                    if _SEPARATOR_RE.match(lines[i])
                    or lines[i].startswith("#")), None)
        if sep is None or lines[sep].startswith("#"):
            raise ValueError("the %r section has no table" % heading)
        lines[sep + 1:sep + 1] = rows

    deltas = {h: sum(1 for r in rows if _is_open(_cells(r)[2])) for h, rows in added.items()}
    total_delta = sum(deltas.values())
    for heading, delta in deltas.items():
        if not delta:
            continue
        label = SECTIONS[heading]
        pat = re.compile(r"^\| %s \| (\d+) \|$" % re.escape(label))
        i = next((i for i, ln in enumerate(lines) if pat.match(ln)), None)
        if i is None:
            raise ValueError("the Quick Summary has no %r row" % label)
        lines[i] = "| %s | %d |" % (label, int(pat.match(lines[i]).group(1)) + delta)
    total_re = re.compile(r"^\| \*\*Total open\*\* \| \*\*(\d+)\*\* \|$")
    t = next((i for i, ln in enumerate(lines) if total_re.match(ln)), None)
    if t is None:
        raise ValueError("the Quick Summary has no Total open row")
    new_total = int(total_re.match(lines[t]).group(1)) + total_delta
    lines[t] = "| **Total open** | **%d** |" % new_total

    summary = lines.index("## Quick Summary") if "## Quick Summary" in lines else t
    end = next((i for i in range(summary + 1, len(lines)) if lines[i].startswith("## ")),
               len(lines))
    last_note = max((i for i in range(summary, end) if _NOTE_RE.match(lines[i])), default=t)
    names = ", ".join(n for n, _b in fragments)
    parts = ", ".join("%s +%d" % (SECTIONS[h].split()[0], d) for h, d in deltas.items() if d)
    lines.insert(last_note + 1, "> %s (folded from `docs/open_issues.d/`: %s): %s -- %d."
                 % (today, names, parts, new_total))
    # The header is what a session reads first (init.ps1 prints it as the newest entry):
    # it must not keep announcing the old total once the counts have moved.
    lu = next((i for i, ln in enumerate(lines) if _LAST_UPDATED_RE.match(ln)), None)
    if lu is None:
        raise ValueError("the tracker has no '> **Last Updated:**' line")
    lines[lu] = ("> **Last Updated:** %s (folded %d fragment(s) from `docs/open_issues.d/`: %s) "
                 "-- %s. %d open." % (today, len(fragments), names, parts, new_total))
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write OPEN_ISSUES.md and delete the fragments")
    ap.add_argument("--dir", type=Path, default=FRAGMENT_DIR, help=argparse.SUPPRESS)
    ap.add_argument("--tracker", type=Path, default=TRACKER, help=argparse.SUPPRESS)
    ap.add_argument("--today", default=None, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    paths = fragment_paths(args.dir)
    bad = {p.name: problems(p) for p in paths if problems(p)}
    if bad:
        for name, why in bad.items():
            print("! %s: %s" % (name, "; ".join(why)))
        print("Nothing written: fix the fragment(s) above first.")
        return 1
    if not paths:
        print("No fragments in %s: OPEN_ISSUES.md is already complete." % args.dir.name)
        return 0

    fragments = [(p.name, parse(p.read_text(encoding="utf-8"))[0]) for p in paths]
    for name, blocks in fragments:
        print("  %s  %s" % (name, ", ".join("%s: %d row(s)" % (h[4:], len(r))
                                            for h, r in blocks.items())))
    text = args.tracker.read_text(encoding="utf-8")
    today = args.today or datetime.date.today().isoformat()
    try:
        new_text = fold(text, fragments, today)
    except ValueError as exc:
        print("! %s. Nothing written." % exc)
        return 1
    if not args.apply:
        print("%d fragment(s) would be folded into %s (run with --apply)."
              % (len(paths), args.tracker.name))
        return 0
    # read_text already turned CRLF into "\n"; write "\n" as collect_changelog.py does
    # (git's autocrlf restores the checkout's endings).
    args.tracker.write_text(new_text, encoding="utf-8", newline="\n")
    for p in paths:
        p.unlink()
    print("%d fragment(s) folded into %s and deleted." % (len(paths), args.tracker.name))
    return 0


if __name__ == "__main__":
    sys.exit(main())
