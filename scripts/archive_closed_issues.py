# -*- coding: utf-8 -*-
"""Move closed entries out of docs/OPEN_ISSUES.md into the archive, safely.

Why not scripts/split_open_issues.ps1: that script rebuilds the archive from
scratch out of whatever it classifies in the CURRENT tracker, and writes it with
WriteAllLines. Re-running it would replace the 355 KB archive (built on
2026-08-12) with only this run's few-KB yield. It also mis-reads the
"Deferred to v7.15+" table (status lives in column 2, not the column 3 it
assumes) and its terminal-status regex misses "Non-issue", "Rolled back",
"Accepted" and lowercase "fixed".

This script APPENDS to the archive and never rewrites it, and it asserts that
every line it removes from the tracker is present in the archive afterwards.

    python scripts/archive_closed_issues.py            # dry run, prints the plan
    python scripts/archive_closed_issues.py --apply
"""
import argparse
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRACKER = os.path.join(ROOT, "docs", "OPEN_ISSUES.md")
ARCHIVE = os.path.join(ROOT, "docs", "archive", "OPEN_ISSUES_ARCHIVE.md")

# Sections whose ENTIRE body is closed: heading stays with a pointer, body moves.
WHOLE_SECTIONS = [
    "## 2026-06-23 Audit (product-quality fan-out + Codex verification)",
    "## 4. Pending Plans (Implemented)",
    "## 6. Documentation Gaps",
    "## 7. Archive Candidates",
]

# A table row moves when it carries a terminal marker and no live marker.
CLOSED = re.compile(r"✅|🟡 Accepted|~~")
OPEN = re.compile(r"❌|⏸|⏳|🔴|🟠")


def is_table_row(line):
    s = line.strip()
    return s.startswith("|") and not s.startswith("|--") and not s.startswith("| Issue |") \
        and not s.startswith("| Category |") and not s.startswith("| # |") \
        and not s.startswith("| Area |") and not s.startswith("| Item |")


def section_of(lines, idx):
    for j in range(idx, -1, -1):
        if lines[j].startswith("## "):
            return lines[j].strip()
    return "(top of file)"


def plan(lines):
    """Return (move_idx set, reason per index)."""
    move, why = set(), {}

    # 1. The mega "Last Updated" header paragraph (line 3, 1-indexed).
    for i, ln in enumerate(lines):
        if ln.startswith("> **Last Updated:**") and len(ln.encode("utf-8")) > 2000:
            move.add(i)
            why[i] = "header log"
            break

    # 2. Whole closed sections (body only; the heading keeps a pointer).
    for i, ln in enumerate(lines):
        if ln.strip() in WHOLE_SECTIONS:
            j = i + 1
            while j < len(lines) and not lines[j].startswith("## "):
                if lines[j].strip():
                    move.add(j)
                    why[j] = "closed section"
                j += 1

    # 3. Closed table rows anywhere else. Never touch the maintenance-protocol
    #    section: its "| Old text | ✅ Fixed | ... |" rows are TEMPLATE EXAMPLES
    #    showing how to mark an issue, not issues.
    for i, ln in enumerate(lines):
        if i in move or not is_table_row(ln):
            continue
        if section_of(lines, i).startswith("## AI Assistant Maintenance Protocol"):
            continue
        if CLOSED.search(ln) and not OPEN.search(ln):
            move.add(i)
            why[i] = "closed row"
    return move, why


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--date", default="2026-08-14")
    args = ap.parse_args()

    with io.open(TRACKER, "r", encoding="utf-8", newline="") as fh:
        lines = fh.readlines()
    before_bytes = sum(len(x.encode("utf-8")) for x in lines)

    move, why = plan(lines)
    moved_bytes = sum(len(lines[i].encode("utf-8")) for i in move)

    from collections import Counter
    by_section = Counter()
    for i in move:
        by_section[section_of(lines, i)] += len(lines[i].encode("utf-8"))

    print("tracker now: %d bytes (%d lines)" % (before_bytes, len(lines)))
    print("moving:      %d lines, %d bytes" % (len(move), moved_bytes))
    print("projected:   %d bytes (%.0f%% of the 180,000 ceiling)\n"
          % (before_bytes - moved_bytes, 100.0 * (before_bytes - moved_bytes) / 180000))
    for sec, n in by_section.most_common():
        print("  %-72s %6d B" % (sec[:72], n))

    if not args.apply:
        print("\n(dry run — pass --apply to write)")
        return

    # Build the archive addition, grouped by originating section.
    out = ["\n\n---\n\n## Closed entries moved out of the tracker on %s\n\n" % args.date,
           "Moved verbatim by `scripts/archive_closed_issues.py`. Nothing was edited or "
           "deleted; each block keeps the section heading it lived under.\n"]
    cur = None
    for i in sorted(move):
        sec = section_of(lines, i)
        if sec != cur:
            out.append("\n### From `%s`\n\n" % sec.lstrip("# ").strip())
            cur = sec
        out.append(lines[i] if lines[i].endswith("\n") else lines[i] + "\n")

    with io.open(ARCHIVE, "r", encoding="utf-8", newline="") as fh:
        archive_before = fh.read()
    with io.open(ARCHIVE, "w", encoding="utf-8", newline="") as fh:
        fh.write(archive_before)
        fh.write("".join(out))

    # Rewrite the tracker, leaving a pointer where a whole section was emptied.
    kept = []
    for i, ln in enumerate(lines):
        if i not in move:
            kept.append(ln)
            continue
        if why[i] == "header log":
            kept.append("> **Last Updated:** %s — see `docs/archive/OPEN_ISSUES_ARCHIVE.md` "
                        "for the dated header log; this tracker holds only what is still open.\n"
                        % args.date)
        elif why[i] == "closed section" and lines[i - 1].startswith("## "):
            kept.append("\nClosed — moved to [`docs/archive/OPEN_ISSUES_ARCHIVE.md`]"
                        "(archive/OPEN_ISSUES_ARCHIVE.md) on %s.\n" % args.date)

    with io.open(TRACKER, "w", encoding="utf-8", newline="") as fh:
        fh.writelines(kept)

    # Verify: every moved line must now exist in the archive.
    with io.open(ARCHIVE, "r", encoding="utf-8", newline="") as fh:
        archive_after = fh.read()
    missing = [lines[i] for i in sorted(move) if lines[i].strip() and lines[i].strip() not in archive_after]
    after_bytes = sum(len(x.encode("utf-8")) for x in kept)
    print("\napplied. tracker %d -> %d bytes; archive %d -> %d bytes"
          % (before_bytes, after_bytes, len(archive_before.encode("utf-8")),
             len(archive_after.encode("utf-8"))))
    if missing:
        print("!! %d moved lines are NOT in the archive — REVERT" % len(missing))
        sys.exit(1)
    print("verified: all %d moved lines present in the archive" % len(move))


if __name__ == "__main__":
    main()
