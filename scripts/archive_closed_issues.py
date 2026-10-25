# -*- coding: utf-8 -*-
"""Move closed issues out of docs/OPEN_ISSUES.md and docs/BACKLOG.md into the archive.

Close an issue by setting its status cell to ``✅ Fixed (YYYY-MM-DD, <sha>)`` (or any
status starting with ✅), then run this. Each closed row, with its section from
docs/ISSUE_DETAILS.md, is APPENDED to docs/archive/OPEN_ISSUES_ARCHIVE.md; the
archive is never rewritten (scripts/split_open_issues.ps1 once rebuilt it from
scratch and would have replaced 355 KB with one run's yield). Only after every
moved ID and headline is found in the archive are the rows and their details
removed, and the Summary and the "Last Updated" line regenerated. The IDs stay
taken: scripts/collect_open_issues.py never gives out an ID the archive holds.

    python scripts/archive_closed_issues.py            # dry run, prints the plan
    python scripts/archive_closed_issues.py --apply
"""
import argparse
import datetime
import io
import sys
from pathlib import Path

import open_issues_model as model   # scripts/ is on sys.path when this runs as a script


def archive_block(closed: list, sections: dict, date: str) -> str:
    out = ["", "---", "", "## Closed entries moved out of the tracker on %s" % date, "",
           "Moved by `scripts/archive_closed_issues.py`: each row as it stood, then its "
           "details. The IDs are not reused.", "",
           model.TABLE_HEAD]
    for place, issue in closed:
        out.append(issue.row().replace("[GS-%d](ISSUE_DETAILS.md#gs-%d)" % (issue.id, issue.id),
                                       "GS-%d (%s)" % (issue.id, place)))
    for _, issue in closed:
        out += ["", "### GS-%d" % issue.id, "", sections[issue.id]]
    return "\n".join(out) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--date", default=datetime.date.today().isoformat())
    ap.add_argument("--tracker", type=Path, default=model.TRACKER, help=argparse.SUPPRESS)
    ap.add_argument("--backlog", type=Path, default=model.BACKLOG, help=argparse.SUPPRESS)
    ap.add_argument("--details", type=Path, default=model.DETAILS, help=argparse.SUPPRESS)
    ap.add_argument("--archive", type=Path, default=model.ARCHIVE, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    docs = {"OPEN_ISSUES.md": (args.tracker, model.PRIORITIES),
            "BACKLOG.md": (args.backlog, model.KINDS)}
    parsed = {name: model.parse(path.read_text(encoding="utf-8"), groups)
              for name, (path, groups) in docs.items()}
    head, sections = model.parse_details(args.details.read_text(encoding="utf-8"))
    closed = [(name, i) for name, doc in parsed.items() for i in doc.issues if i.is_closed]
    if not closed:
        print("No closed rows (a status starting with %s) in the tracker or the backlog."
              % " ".join(model.CLOSED_MARKS))
        return 0
    for name, i in closed:
        print("  GS-%-4d %-14s %s" % (i.id, name, i.headline[:80]))
    # The details are the row's full text: archiving a row without them would lose it
    # (Codex review of PR #449). An interrupted fold can leave a row so -- finish it first.
    bare = [i.id for _, i in closed if not sections.get(i.id, "").strip()]
    if bare:
        print("! GS-%s %s no details section in %s; nothing written. Run "
              "`python scripts/collect_open_issues.py --apply` (or restore the section) first."
              % (", GS-".join(map(str, bare)), "has" if len(bare) == 1 else "have",
                 args.details.name))
        return 1
    if not args.apply:
        print("\n%d row(s) would move to %s (dry run -- pass --apply to write)."
              % (len(closed), args.archive.name))
        return 0

    # 1. Append to the archive, keeping its line endings, and verify before removing anything.
    with io.open(args.archive, "r", encoding="utf-8", newline="") as fh:
        before = fh.read()
    eol = "\r\n" if before.count("\r\n") > before.count("\n") // 2 else "\n"
    block = archive_block(closed, sections, args.date).replace("\n", eol)
    with io.open(args.archive, "a", encoding="utf-8", newline="") as fh:
        fh.write(block)
    with io.open(args.archive, "r", encoding="utf-8", newline="") as fh:
        after = fh.read()
    missing = [i.id for _, i in closed
               if "GS-%d (" % i.id not in after or i.headline not in after
               or sections[i.id].replace("\n", eol) not in after]
    if missing or not after.startswith(before):
        print("!! the archive does not hold GS-%s after the append; nothing else was changed"
              % ", GS-".join(map(str, missing)))
        return 1

    # 2. Only now take the rows and their details out.
    ids = {i.id for _, i in closed}
    label = "archived %s" % ", ".join("GS-%d" % i for i in sorted(ids))
    for name, (path, _) in docs.items():
        doc = parsed[name]
        if not any(i.id in ids for i in doc.issues):
            continue
        doc.issues = [i for i in doc.issues if i.id not in ids]
        model.set_last_updated(doc, args.date, label)
        path.write_text(model.render(doc), encoding="utf-8", newline="\n")
    args.details.write_text(model.render_details(head, {k: v for k, v in sections.items()
                                                        if k not in ids}),
                            encoding="utf-8", newline="\n")
    print("\nverified and applied: %d row(s) moved to %s." % (len(closed), args.archive.name))
    return 0


if __name__ == "__main__":
    sys.exit(main())
