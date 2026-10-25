"""Fold the docs/open_issues.d/ fragments into docs/OPEN_ISSUES.md and docs/ISSUE_DETAILS.md.

A PR that finds a new defect writes it as a file of its own,
``docs/open_issues.d/YYYY-MM-DD-<slug>.md``, instead of editing the tracker: two
PRs that each added a row conflicted on the counts and the "Last Updated" line,
and resolving it restarted CI (2026-10-21: PR #426 and the Responsa wildcard
plan). Two fragments never conflict. This is the tracker's version of
``changelog.d/`` (scripts/collect_changelog.py).

A fragment holds one or more issues, each a ``##`` heading with the priority, the
area and the headline, a few ``Key: value`` lines, a blank line, and the details:

    ## P3 search: A crossing from an earlier page can hide the next page's own
    Where: `shared/search_engine.py::_cross_page_spans`
    Opened: 2026-10-22

    What, where, how it was measured, a fix sketch.

``Where`` is required; ``Opened`` defaults to the fragment's date and ``Status`` to
``❌ Open`` (a status must start with an open mark: the tracker holds only open
items). The areas are those of scripts/open_issues_model.py. A fragment in the
form used before 2026-10-25 (a ``### P2 - Medium`` heading and four-cell rows) is
still accepted, from PRs opened before the change; its rows land under "Unsorted"
until someone gives them an area.

    python scripts/collect_open_issues.py           # show what would be folded in
    python scripts/collect_open_issues.py --apply   # write the files, delete the fragments
    python scripts/collect_open_issues.py --check   # are the tracker, backlog and details consistent?

``--apply`` gives each new issue the next free ID (never one the archive used), puts
its row in the tracker and its details in ISSUE_DETAILS.md, regenerates the Summary
and the "Last Updated" line, and deletes the fragments. It also rewrites the tracker
and the backlog in canonical form, which is how a hand edit that left a count or an
order behind is repaired. It writes nothing if a fragment is malformed or a file does
not parse.

Running it again is safe: an issue whose headline the tracker already holds is not
added twice (only its missing details section, if any, is written), so a run that
wrote the tracker but could not delete a fragment is finished by the next run.
"""
from __future__ import annotations

import argparse
import datetime
import re
import sys
from pathlib import Path

import open_issues_model as model   # scripts/ is on sys.path when this runs as a script

REPO_ROOT = model.REPO_ROOT
FRAGMENT_DIR = REPO_ROOT / "docs" / "open_issues.d"
TRACKER = model.TRACKER

NAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})-[a-z0-9][a-z0-9-]*\.md$")
_ISSUE_RE = re.compile(r"^## (P[123]) ([a-z-]+): (\S.*)$")
_FIELD_RE = re.compile(r"^(Where|Opened|Status): (\S.*)$")
_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
# The form used before 2026-10-25: a section heading, then four-cell rows.
LEGACY_SECTIONS = {"### P1 - Critical": "P1", "### P2 - Medium": "P2",
                   "### P3 - Low Priority": "P3"}


def fragment_paths(fragment_dir: Path) -> list:
    """The fragments, oldest first (README.md is the folder's instructions)."""
    return sorted(p for p in fragment_dir.glob("*.md") if p.name != "README.md")


def _norm(headline: str) -> str:
    # The tracker holds a "|" escaped (open_issues_model.escape_cell); a fragment does not.
    return re.sub(r"\s+", " ", headline.replace("**", "").replace("\\|", "|")).strip().lower()


def _legacy(lines: list, default_date: str) -> tuple:
    issues, found, priority = [], [], None
    for ln in lines:
        if ln.startswith("#"):
            priority = LEGACY_SECTIONS.get(ln)
            if priority is None:
                found.append("heading %r is not one of %s" % (ln, ", ".join(LEGACY_SECTIONS)))
            continue
        if priority is None:
            found.append("a line comes before any section heading: %r" % ln[:60])
            continue
        if not (ln.startswith("|") and ln.endswith("|")):
            found.append("not a table row: %r" % ln[:60])
            continue
        c = model.cells(ln)
        if len(c) != 4:
            found.append("a row has %d cells, not 4 (Issue | File | Status | Notes): %r"
                         % (len(c), ln[:60]))
            continue
        if not c[2].startswith(model.OPEN_MARKS):
            found.append("a row's status does not start with an open mark (one of %s): %r"
                         % (" ".join(model.OPEN_MARKS), c[2][:40]))
            continue
        date = _DATE_RE.search(c[2])
        issue = model.Issue(0, priority, "unsorted", c[0].replace("**", "").strip(), c[1],
                            date.group(1) if date else default_date, c[2])
        issues.append((issue, c[3]))
    return issues, found


def parse(text: str, default_date: str = "") -> tuple:
    """([(Issue without an ID, details)], [problems]) for one fragment's text."""
    lines = [ln.rstrip() for ln in text.splitlines()]
    if not any(ln.strip() for ln in lines):
        return [], ["is empty"]
    # The first heading decides the form: a new-form issue's details may themselves hold a
    # "### P2 - Medium" line (Codex review of PR #449).
    if next(ln for ln in lines if ln.strip()) in LEGACY_SECTIONS:
        return _legacy([ln for ln in lines if ln.strip()], default_date)
    issues, found, i = [], [], 0
    while i < len(lines) and not lines[i].strip():
        i += 1
    while i < len(lines):
        m = _ISSUE_RE.match(lines[i])
        if not m:
            found.append("expected '## P<1-3> <area>: <headline>', got %r" % lines[i][:60])
            return issues, found
        priority, area, headline = m.groups()
        if area not in model.AREAS or area == "unsorted":
            found.append("%r is not an area (one of %s)"
                         % (area, ", ".join(a for a in model.AREAS if a != "unsorted")))
        fields, i = {}, i + 1
        while i < len(lines) and lines[i].strip():
            f = _FIELD_RE.match(lines[i])
            if not f:
                found.append("%s: expected 'Where:', 'Opened:' or 'Status:', got %r"
                             % (headline[:40], lines[i][:60]))
            else:
                fields[f.group(1)] = f.group(2).strip()
            i += 1
        body = []
        while i < len(lines) and not _ISSUE_RE.match(lines[i]) \
                and not lines[i].startswith("## "):
            body.append(lines[i])
            i += 1
        details = "\n".join(body).strip()
        status = fields.get("Status", "❌ Open")
        opened = fields.get("Opened", default_date)
        if "Where" not in fields:
            found.append("%s: no 'Where:' line" % headline[:40])
        if not details:
            found.append("%s: no details after the blank line" % headline[:40])
        if not status.startswith(model.OPEN_MARKS):
            found.append("%s: the status does not start with an open mark (one of %s)"
                         % (headline[:40], " ".join(model.OPEN_MARKS)))
        if opened and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", opened):
            found.append("%s: 'Opened:' is not YYYY-MM-DD" % headline[:40])
        issues.append((model.Issue(0, priority, area, headline.strip(),
                                   fields.get("Where", ""), opened, status), details))
    return issues, found


def problems(path: Path) -> list:
    """What is wrong with one fragment ([] when it is fine)."""
    m = NAME_RE.match(path.name)
    found = [] if m else ["name is not YYYY-MM-DD-<lowercase-slug>.md"]
    return found + parse(path.read_text(encoding="utf-8"), m.group(1) if m else "")[1]


def fold(tracker: str, details: str, fragments: list, taken: set, today: str) -> tuple:
    """(tracker, details, [names that added something]) with the fragments folded in.

    *fragments* is [(name, [(Issue, details)])], oldest first; *taken* is every ID any
    file has used, the archive's included. Raises ValueError when a file does not parse."""
    doc = model.parse(tracker, model.PRIORITIES)
    head, sections = model.parse_details(details)
    known = {_norm(i.headline): i for i in doc.issues}
    next_id = max(taken | {0}) + 1
    names = []
    for name, issues in fragments:
        added = False
        for issue, body in issues:
            have = known.get(_norm(issue.headline))
            if have is not None:
                if have.id not in sections:    # a run that stopped between the two writes
                    sections[have.id] = model.details_section(have, have.group, body)
                    added = True
                continue
            issue.id, next_id = next_id, next_id + 1
            doc.issues.append(issue)
            known[_norm(issue.headline)] = issue
            sections[issue.id] = model.details_section(issue, issue.group, body)
            added = True
        if added:
            names.append(name)
    if names:
        model.set_last_updated(doc, today, "folded %d fragment(s) from `docs/open_issues.d/`: %s"
                               % (len(names), ", ".join(names)))
    return model.render(doc), model.render_details(head, sections), names


def check(tracker: Path, backlog: Path, details: Path) -> list:
    return model.problems(tracker.read_text(encoding="utf-8"),
                          backlog.read_text(encoding="utf-8"),
                          details.read_text(encoding="utf-8"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write the tracker and details, delete the fragments")
    ap.add_argument("--check", action="store_true",
                    help="report what is inconsistent in the tracker, backlog and details")
    ap.add_argument("--dir", type=Path, default=FRAGMENT_DIR, help=argparse.SUPPRESS)
    ap.add_argument("--tracker", type=Path, default=TRACKER, help=argparse.SUPPRESS)
    ap.add_argument("--backlog", type=Path, default=model.BACKLOG, help=argparse.SUPPRESS)
    ap.add_argument("--details", type=Path, default=model.DETAILS, help=argparse.SUPPRESS)
    ap.add_argument("--archive", type=Path, default=model.ARCHIVE, help=argparse.SUPPRESS)
    ap.add_argument("--today", default=None, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    if args.check:
        found = check(args.tracker, args.backlog, args.details)
        for p in found:
            print("! " + p)
        print("%d problem(s)." % len(found) if found else "Tracker, backlog and details agree.")
        return 1 if found else 0

    paths = fragment_paths(args.dir)
    bad = {p.name: problems(p) for p in paths if problems(p)}
    if bad:
        for name, why in bad.items():
            print("! %s: %s" % (name, "; ".join(why)))
        print("Nothing written: fix the fragment(s) above first.")
        return 1

    fragments = []
    for p in paths:
        issues = parse(p.read_text(encoding="utf-8"), NAME_RE.match(p.name).group(1))[0]
        fragments.append((p.name, issues))
        print("  %s  %s" % (p.name, ", ".join("%s %s: %s" % (i.group, i.area, i.headline[:50])
                                              for i, _ in issues)))
    texts = {k: getattr(args, k).read_text(encoding="utf-8")
             for k in ("tracker", "backlog", "details")}
    archive = args.archive.read_text(encoding="utf-8") if args.archive.exists() else ""
    today = args.today or datetime.date.today().isoformat()
    try:
        tracker, details, names = fold(texts["tracker"], texts["details"], fragments,
                                       model.used_ids(*texts.values(), archive), today)
        backlog = model.render(model.parse(texts["backlog"], model.KINDS))
    except ValueError as exc:
        print("! %s. Nothing written." % exc)
        return 1
    if not args.apply:
        print("%d fragment(s) would be folded in (run with --apply)." % len(paths)
              if paths else "No fragments in %s." % args.dir.name)
        return 0
    # read_text already turned CRLF into "\n"; write "\n" as collect_changelog.py does
    # (git's autocrlf restores the checkout's endings). The tracker goes first: a run
    # that stops before the details is finished by the next one (fold's `have` branch).
    for key, new in (("tracker", tracker), ("backlog", backlog), ("details", details)):
        if new != texts[key]:
            getattr(args, key).write_text(new, encoding="utf-8", newline="\n")
    kept = []
    for p in paths:
        try:
            p.unlink()
        except OSError as exc:
            kept.append("%s (%s)" % (p.name, exc.strerror or exc))
    if kept:
        print("! The tracker is written, but could not delete: %s. Delete by hand, or run "
              "again: issues already in the tracker are not folded twice." % "; ".join(kept))
        return 1
    print("%d fragment(s) folded in and deleted%s." % (
        len(paths), "" if len(names) == len(paths) else
        " (%d added nothing new)" % (len(paths) - len(names))) if paths
        else "No fragments; the tracker and backlog are in canonical form.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
