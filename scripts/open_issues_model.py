"""The shape of docs/OPEN_ISSUES.md, docs/BACKLOG.md and docs/ISSUE_DETAILS.md.

Reconstructed 2026-10-25. The tracker had grown to 179 KB of one-paragraph rows in
insertion order, a 10 KB log of count changes, hand-kept counts that had drifted
(P2 48 claimed, 45 marked open), and sections for milestones long closed. It is now
three files with one ID space:

- ``docs/OPEN_ISSUES.md`` (read every session): one line per open defect, under its
  priority (P1/P2/P3) and its area, with a generated Summary of counts.
- ``docs/BACKLOG.md`` (grepped): what is not a defect -- deferred defects, code debt,
  test gaps, ideas, server and owner tasks -- in the same row form, under its kind.
- ``docs/ISSUE_DETAILS.md`` (grepped): the full text of every row, one ``## GS-<n>``
  section per ID.

Every row is ``| [GS-12](ISSUE_DETAILS.md#gs-12) | Issue | Where | Opened | Status |``.
A file is in canonical form when ``render(parse(text)) == text``; CI checks that
(tests/test_open_issues_fragments.py), so a count or a heading cannot drift by hand.
Closed rows leave both files for docs/archive/OPEN_ISSUES_ARCHIVE.md with their
details (scripts/archive_closed_issues.py); their IDs are never reused.
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:      # run as a script, only scripts/ is on the path
    sys.path.append(str(REPO_ROOT))
from shared.atomic_io import write_bytes_atomic  # noqa: E402 -- stdlib-only leaf module
TRACKER = REPO_ROOT / "docs" / "OPEN_ISSUES.md"
BACKLOG = REPO_ROOT / "docs" / "BACKLOG.md"
DETAILS = REPO_ROOT / "docs" / "ISSUE_DETAILS.md"
ARCHIVE = REPO_ROOT / "docs" / "archive" / "OPEN_ISSUES_ARCHIVE.md"

# Area slug -> its heading. "unsorted" holds rows nobody has placed yet (a fragment in
# the pre-2026-10-25 form carries no area); the Summary shows them until someone does.
AREAS = {
    "search": "Search",
    "browse": "Browse and viewer",
    "desktop": "Desktop app",
    "web": "Website",
    "api": "API and AI",
    "discovery": "Discovery",
    "community": "Accounts and community",
    "data": "Data and sidecars",
    "build-ci": "Build, CI and tests",
    "docs": "Documentation",
    "ops": "Server and operations",
    "unsorted": "Unsorted",
}
PRIORITIES = {"P1": "P1 - Critical", "P2": "P2 - Medium", "P3": "P3 - Low Priority"}
KINDS = {
    "deferred": "Deferred defects",
    "debt": "Code debt",
    "test-gap": "Test gaps",
    "idea": "Ideas and extensions",
    "ops": "Server and owner tasks",
}
OPEN_MARKS = ("❌", "⏸", "⏳")   # ❌ open (or partly fixed), ⏸ deferred, ⏳ needs a check
CLOSED_MARKS = ("✅",)

_ID_LINK_RE = re.compile(r"^\[GS-(\d+)\]\(ISSUE_DETAILS\.md#gs-(\d+)\)$")
_CELL_SPLIT = re.compile(r"(?<!\\)\|")
_SEPARATOR_RE = re.compile(r"^\|[\s:|-]+\|\s*$")
_DETAILS_HEADING_RE = re.compile(r"^## GS-(\d+)\s*$")
_ANY_ID_RE = re.compile(r"\bGS-(\d+)\b")
TABLE_HEAD = ("| ID | Issue | Where | Opened | Status |\n"
              "|----|-------|-------|--------|--------|")
LAST_UPDATED_RE = re.compile(r"^>\s*\*\*Last Updated:\*\*")
# The generated count is only what follows the "). " that closes the line's parenthesised
# note (set_last_updated's form): a count-shaped phrase in hand-written text ("reviewed 3
# items.") is never rewritten -- the check reports such a line instead.
_COUNT_TAIL_RE = re.compile(r"(?<=\)\. )(\d+ open: P1 \d+, P2 \d+, P3 \d+|\d+ items)\.\s*$")


@dataclass
class Issue:
    id: int
    group: str        # a PRIORITIES key in the tracker, a KINDS key in the backlog
    area: str         # an AREAS key
    headline: str
    where: str
    opened: str       # YYYY-MM-DD, or "" when the row never carried a date
    status: str

    @property
    def is_open(self) -> bool:
        return self.status.strip().startswith(OPEN_MARKS)

    @property
    def is_closed(self) -> bool:
        return self.status.strip().startswith(CLOSED_MARKS)

    def row(self) -> str:
        # A literal "|" (a regex, an alternative) would end the cell: escape it. An already
        # escaped "\|" -- what parse() returns -- is left as it is, so rendering is stable.
        return "| [GS-%d](ISSUE_DETAILS.md#gs-%d) | %s | %s | %s | %s |" % (
            self.id, self.id, escape_cell(self.headline), escape_cell(self.where),
            escape_cell(self.opened) or "—", escape_cell(self.status))


def escape_cell(text: str) -> str:
    return re.sub(r"(?<!\\)\|", r"\\|", text.replace("\n", " "))


@dataclass
class Doc:
    """A parsed OPEN_ISSUES.md or BACKLOG.md: the hand-written preamble and the rows."""
    preamble: list    # lines before "## Summary", the Last Updated line among them
    issues: list
    groups: dict      # PRIORITIES or KINDS


def cells(row: str) -> list:
    return [c.strip() for c in _CELL_SPLIT.split(row.strip())[1:-1]]


def in_order(issues) -> list:
    """Newest first, a row without a date last, the same date by ID (both sorts are stable)."""
    rows = sorted(issues, key=lambda i: i.id)
    rows.sort(key=lambda i: i.opened, reverse=True)
    return rows


def parse(text: str, groups: dict) -> Doc:
    """Parse a tracker or backlog file. Raises ValueError on anything it would not write."""
    lines = text.split("\n")
    try:
        start = lines.index("## Summary")
    except ValueError:
        raise ValueError("no '## Summary' heading") from None
    by_title = {v: k for k, v in groups.items()}
    area_by_title = {v: k for k, v in AREAS.items()}
    issues, group, area = [], None, None
    in_summary = True
    for no, ln in enumerate(lines[start + 1:], start + 2):
        if ln.startswith("## "):
            title = ln[3:].strip()
            if title not in by_title:
                raise ValueError("line %d: %r is not one of the sections %s"
                                 % (no, ln, ", ".join(groups.values())))
            group, area, in_summary = by_title[title], None, False
            continue
        if in_summary or not ln.strip() or ln.strip() == "_None open._":
            continue
        if ln.startswith("### "):
            title = ln[4:].strip()
            if title not in area_by_title:
                raise ValueError("line %d: %r is not one of the areas %s"
                                 % (no, ln, ", ".join(AREAS.values())))
            area = area_by_title[title]
            continue
        if ln.startswith("|"):
            if _SEPARATOR_RE.match(ln) or ln.startswith("| ID | Issue |"):
                continue
            c = cells(ln)
            if area is None:
                raise ValueError("line %d: a row outside an area heading" % no)
            if len(c) != 5:
                raise ValueError("line %d: a row has %d cells, not 5 (ID | Issue | Where | "
                                 "Opened | Status)" % (no, len(c)))
            m = _ID_LINK_RE.match(c[0])
            if not m or m.group(1) != m.group(2):
                raise ValueError("line %d: the ID cell is not [GS-<n>](ISSUE_DETAILS.md#gs-<n>)"
                                 % no)
            issues.append(Issue(int(m.group(1)), group, area, c[1], c[2],
                                "" if c[3] == "—" else c[3], c[4]))
            continue
        raise ValueError("line %d: unexpected text %r (only area headings and rows belong "
                         "below the Summary)" % (no, ln[:60]))
    return Doc(lines[:start], issues, groups)


def summary(issues: list, groups: dict) -> list:
    keys = list(groups)
    head = "| Area | %s | Total |" % " | ".join(keys if groups is PRIORITIES
                                                else [groups[k] for k in keys])
    out = [head, "|" + "------|" * (len(keys) + 2)]
    total = {k: 0 for k in keys}
    for slug, title in AREAS.items():
        n = {k: sum(1 for i in issues if i.area == slug and i.group == k) for k in keys}
        if not sum(n.values()):
            continue
        for k in keys:
            total[k] += n[k]
        out.append("| %s | %s | %d |" % (title, " | ".join(str(n[k]) for k in keys),
                                         sum(n.values())))
    out.append("| **Total** | %s | **%d** |" % (" | ".join("**%d**" % total[k] for k in keys),
                                               sum(total.values())))
    return out


def render(doc: Doc) -> str:
    # The count closing the "Last Updated" line is generated like the Summary: a hand-edited
    # or stale one is not canonical, and --apply rewrites it (Codex review of PR #449).
    preamble = [_COUNT_TAIL_RE.sub(counts_line(doc) + ".", ln) if LAST_UPDATED_RE.match(ln)
                else ln for ln in doc.preamble]
    out = preamble + ["## Summary", ""] + summary(doc.issues, doc.groups)
    for key, title in doc.groups.items():
        out += ["", "## " + title, ""]
        mine = [i for i in doc.issues if i.group == key]
        if not mine:
            out.append("_None open._")
            continue
        first = True
        for slug, area_title in AREAS.items():
            rows = in_order(i for i in mine if i.area == slug)
            if not rows:
                continue
            if not first:
                out.append("")
            first = False
            out += ["### " + area_title, "", TABLE_HEAD] + [i.row() for i in rows]
    return "\n".join(out) + "\n"


def counts_line(doc: Doc) -> str:
    """'123 open: P1 0, P2 30, P3 93' for the tracker, '41 items' style for the backlog."""
    n = len(doc.issues)
    if doc.groups is PRIORITIES:
        return "%d open: %s" % (n, ", ".join("%s %d" % (k, sum(1 for i in doc.issues
                                                              if i.group == k))
                                             for k in PRIORITIES))
    return "%d items" % n


def set_last_updated(doc: Doc, today: str, what: str) -> None:
    line = "> **Last Updated:** %s (%s). %s." % (today, what, counts_line(doc))
    for i, ln in enumerate(doc.preamble):
        if LAST_UPDATED_RE.match(ln):
            doc.preamble[i] = line
            return
    raise ValueError("no '> **Last Updated:**' line above the Summary")


# ---------------------------------------------------------------- details ----

def parse_details(text: str) -> tuple:
    """(head lines, {id: section text without its heading}) of ISSUE_DETAILS.md."""
    lines = text.split("\n")
    head, sections, current = [], {}, None
    for ln in lines:
        m = _DETAILS_HEADING_RE.match(ln)
        if m:
            current = int(m.group(1))
            if current in sections:
                raise ValueError("GS-%d has two sections" % current)
            sections[current] = []
            continue
        (head if current is None else sections[current]).append(ln)
    return head, {k: "\n".join(v).strip("\n") for k, v in sections.items()}


def render_details(head: list, sections: dict) -> str:
    out = "\n".join(head).rstrip("\n") + "\n"
    for k in sorted(sections):
        out += "\n## GS-%d\n\n%s\n" % (k, sections[k].strip("\n"))
    return out


def details_section(issue: Issue, place: str, body: str) -> str:
    """The section a new row gets: what it is, then its full text."""
    return ("**%s** · %s · %s · %s · opened %s\n\nWhere: %s\n\n%s"
            % (issue.headline, place, AREAS[issue.area], issue.status,
               issue.opened or "—", issue.where, body.strip()))


def write_text(path, text: str) -> None:
    """Replace *path* with *text* ("\\n" endings; git's autocrlf restores the checkout's),
    all or nothing: a temporary file renamed over the target (shared/atomic_io.py). A run
    stopped mid-write can no longer leave a truncated tracker, backlog or details file --
    the class of state Codex rounds 4, 6 and 7 on PR #449 each found another way into."""
    write_bytes_atomic(str(path), text.encode("utf-8"))


def used_ids(*texts: str) -> set:
    found = set()
    for t in texts:
        found.update(int(n) for n in _ANY_ID_RE.findall(t))
    return found


# ------------------------------------------------------------------ check ----

def problems(tracker: str, backlog: str, details: str) -> list:
    """Everything wrong with the three files together ([] when consistent)."""
    found = []
    docs = {}
    for name, text, groups in (("OPEN_ISSUES.md", tracker, PRIORITIES),
                               ("BACKLOG.md", backlog, KINDS)):
        try:
            doc = parse(text, groups)
        except ValueError as exc:
            found.append("%s: %s" % (name, exc))
            continue
        docs[name] = doc
        if render(doc) != text:
            found.append("%s is not in canonical form (a count, an order or a blank line "
                         "differs): run `python scripts/collect_open_issues.py --apply`" % name)
        updated = [ln for ln in doc.preamble if LAST_UPDATED_RE.match(ln)]
        if not updated or not updated[0].rstrip().endswith("). %s." % counts_line(doc)):
            found.append("%s: the '> **Last Updated:**' line must end with '(<what>). %s.', "
                         "the generated count" % (name, counts_line(doc)))
        for i in doc.issues:
            if i.is_closed:
                found.append("%s: GS-%d is closed; run `python scripts/archive_closed_issues.py "
                             "--apply` to move it to the archive" % (name, i.id))
            elif not i.is_open:
                found.append("%s: GS-%d's status does not start with one of %s"
                             % (name, i.id, " ".join(OPEN_MARKS + CLOSED_MARKS)))
    try:
        _, sections = parse_details(details)
    except ValueError as exc:
        return found + ["ISSUE_DETAILS.md: %s" % exc]
    seen = {}
    for name, doc in docs.items():
        for i in doc.issues:
            if i.id in seen:
                found.append("GS-%d is in %s and %s" % (i.id, seen[i.id], name))
            seen[i.id] = name
            if i.id not in sections:
                found.append("GS-%d (%s) has no '## GS-%d' section in ISSUE_DETAILS.md"
                             % (i.id, name, i.id))
            elif not sections[i.id].strip():
                # The section is the only full-text record of the row (Codex review of #449).
                found.append("GS-%d (%s): its '## GS-%d' section in ISSUE_DETAILS.md is empty"
                             % (i.id, name, i.id))
    if len(docs) == 2:
        for k in sections:
            if k not in seen:
                found.append("ISSUE_DETAILS.md has a GS-%d section but no row names it "
                             "(closed? its section belongs in the archive)" % k)
    return found
