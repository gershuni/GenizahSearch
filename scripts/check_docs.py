#!/usr/bin/env python3
"""
Documentation Health Check Script

Checks GenizahSearch documentation for:
- Missing critical documents
- Outdated terminology (removed features)
- Stale documents (not updated recently)
- Broken internal links

Usage:
    python scripts/check_docs.py
"""

import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent
DOCS_DIR = ROOT_DIR / 'docs'

# Terms that may indicate outdated content
# Format: (term, reason, exclude_files)
OUTDATED_TERMS = [
    ('genizah-backend', 'Service removed - only genizah-web exists', []),
    ('backend/requirements.txt', 'File no longer exists', []),
    ('DATABASE_URL', 'No longer used - replaced by SUPABASE_URL', []),
    ('python -m backend.main', 'The standalone backend process was removed in Jan 2026', []),
]

# The instruction files at the repository root. They are not under docs/, so until 2026-09-18
# nothing checked their links or their vocabulary -- and they were the files that disagreed with
# each other (Python 3.10+ vs 3.11, 'FastAPI was removed', 'pytest tests/'). Every one enters
# the broken-link scan; the outdated-term scan skips the three that name the old terms on
# purpose, as the 'do not say this' list.
ROOT_INSTRUCTION_DOCS = (
    'CLAUDE.md',
    'AGENTS.md',
    'CONTRIBUTING.md',
    '.cursorrules',
    'README.md',
    'tests/README.md',
    '.claude/skills/release/SKILL.md',
)
ROOT_INSTRUCTION_TERM_EXEMPT = ('CLAUDE.md', 'CONTRIBUTING.md', '.cursorrules')

# Files to skip entirely (they intentionally reference old terms)
SKIP_FILES = [
    'DOCUMENTATION_MAINTENANCE.md',  # Contains terms as examples
    'SUPABASE_MIGRATION_PLAN.md',    # Historical document about migration
]

# Critical documents that must exist
CRITICAL_DOCS = [
    ('CLAUDE.md', 'AI assistant context'),
    ('README.md', 'Project overview'),
    ('docs/DOCUMENTATION_INDEX.md', 'Documentation index'),
    ('docs/guides/DEPLOYMENT_TECHNICAL.md', 'Deployment guide'),
    ('docs/guides/WEBSITE_ADMIN_GUIDE.md', 'Admin guide'),
    ('docs/guides/DEVELOPER_GUIDE.md', 'Developer guide'),
    ('docs/guides/SUPABASE_GUIDE.md', 'Supabase guide'),
]

# How old is "stale" (days)
STALE_THRESHOLD_DAYS = 90

# Docs whose header date is a PUBLISHED CLAIM, not a courtesy: an external
# integrator reads it to decide whether the body can be trusted. For these,
# a header date older than the file's own last git-commit date is a BLOCKING
# failure, not a freshness reminder.
#
# Why this exists: on 2026-09-08 the author of a third-party MCP wrapper read
# docs/SEARCH_API.md's header ("(v7.10)" / "Last updated: 2026-05-05"),
# reasonably concluded the passage / multi-witness sections in the body
# postdated the document's own version and were therefore aspirational, and
# shipped an adapter that rejected four features that are documented, live,
# and flag-enabled in production. The body had been edited through
# 2026-08-26; only the header rotted. Relaxing LAST_UPDATED_RE fixes the
# parsing; only this check stops the drift recurring.
CONTRACT_DOCS = (
    'docs/SEARCH_API.md',
)

# How far git's last-commit date may exceed a contract doc's header date before
# it is a failure rather than a note.
#
# Why there is a window at all: a SQUASH MERGE creates a commit touching the
# file that nobody made on that date. GitHub rewrites both the committer AND
# the author date to the merge time, so no git date survives to say when the
# body actually changed. PR #337 proved it -- lint-and-docs passed on the
# branch, where the file's last commit really was 2026-09-08, then failed the
# moment the squash landed on 2026-09-09 against a header that was correct.
# The gate's premise, that "a commit touching the file is the exact event that
# should have moved the header", holds for edits and not for merges.
#
# Why the window is small: the defect this gate was written for was a header
# reading 2026-05-05 (v7.10) over a body that had moved on for months. Days of
# tolerance does not bring that back, and a longer window would.
#
# A lag inside the window is PRINTED, never silently swallowed.
MERGE_LAG_GRACE_DAYS = 7

# --- "Last updated" date parsing -----------------------------------------------
# Matches a "Last updated" label followed, on the SAME line, by a YYYY-MM-DD
# date -- allowing for the label's own markdown decoration (bold/italic
# markers, a colon, a dash) but nothing else. The gap between "updated" and
# the date is a bounded, explicit character class of markdown/whitespace
# punctuation (NOT `\s`, which includes newlines, and NOT a `.*?`/`[^\d]*`
# wildcard), so the match can never cross into a different line or skip past
# unrelated prose to grab a later, unrelated date. A bound of 6 covers every
# house-style form actually used in this repo:
#   "Last updated: 2026-09-08"          -- gap is ": "        (2 chars)
#   "**Last Updated:** 2026-09-08"      -- gap is ":** "      (4 chars)
#   "> **Last Updated:** 2026-09-08"    -- the "> **" is BEFORE "Last", so
#                                          the gap is still ":** " (4 chars)
#   "_Last updated_ -- 2026-09-08"      -- gap is "_ -- "     (5 chars)
#   "Last Updated 2026-09-08"           -- gap is " "         (1 char)
LAST_UPDATED_RE = re.compile(
    r'Last[ \t]+updated[ \t:*_–—-]{0,6}(\d{4}-\d{2}-\d{2})',
    re.IGNORECASE,
)

# Loose probe for "a 'Last updated' label exists somewhere in this file",
# used ONLY to tell "no label at all" apart from "label present but the date
# next to it didn't parse" -- see check_unparsable_last_updated() below.
LAST_UPDATED_LABEL_RE = re.compile(r'Last[ \t]+updated', re.IGNORECASE)

# --- Context budget -----------------------------------------------------------
# These files are read into EVERY AI session's context, so their size is a
# RECURRING cost, not a one-time one. In August 2026 CLAUDE.md had grown to 76 KB
# (56 KB of it a second copy of CHANGELOG.md) and docs/OPEN_ISSUES.md to 465 KB,
# which CLAUDE.md itself ordered every session to read in full -- ~135k resident
# tokens before any work began. Both were split; these ceilings stop the regrowth,
# because a prose rule asking future sessions to move closed items out is advisory
# and this is enforced.
#
# If a ceiling is hit, SPLIT the file (closed/historical content -> docs/archive/),
# do not raise the number. scripts/archive_closed_issues.py does it for the tracker
# (append-only; the older split_open_issues.ps1 rebuilds the archive and must not be re-run).
CONTEXT_BUDGET = [
    ('CLAUDE.md', 40_000),
    ('docs/OPEN_ISSUES.md', 180_000),
]


def print_header(text: str):
    """Print a section header."""
    print(f"\n{'=' * 60}")
    print(f" {text}")
    print('=' * 60)


def print_status(ok: bool, message: str):
    """Print a status line."""
    icon = '✅' if ok else '❌'
    print(f"  {icon} {message}")


def print_warning(message: str):
    """Print a warning."""
    print(f"  ⚠️  {message}")


def check_critical_docs() -> list:
    """Check that all critical documents exist."""
    issues = []
    for doc_path, description in CRITICAL_DOCS:
        full_path = ROOT_DIR / doc_path
        if not full_path.exists():
            issues.append(f"Missing: {doc_path} ({description})")
    return issues


def _scanned_docs(*, term_scan: bool) -> list:
    """docs/**/*.md (archive excluded) plus the root instruction files.

    The term scan leaves out the instruction files that list the outdated terms as the
    things not to say (ROOT_INSTRUCTION_TERM_EXEMPT); the link scan covers all of them.
    """
    files = [p for p in DOCS_DIR.rglob('*.md') if 'archive' not in str(p)]
    for rel in ROOT_INSTRUCTION_DOCS:
        if term_scan and rel in ROOT_INSTRUCTION_TERM_EXEMPT:
            continue
        path = ROOT_DIR / rel
        if path.is_file():
            files.append(path)
    return files


def check_outdated_terms() -> list:
    """Search for terms that may indicate outdated content."""
    issues = []

    for md_file in _scanned_docs(term_scan=True):

        # Skip files that intentionally reference old terms
        if md_file.name in SKIP_FILES:
            continue

        try:
            content = md_file.read_text(encoding='utf-8')
        except Exception:
            continue

        relative_path = md_file.relative_to(ROOT_DIR)

        for term, reason, exclude_files in OUTDATED_TERMS:
            # Skip if file is in exclude list for this term
            if md_file.name in exclude_files:
                continue

            # Case-insensitive search
            if re.search(re.escape(term), content, re.IGNORECASE):
                issues.append(f"{relative_path}: Contains '{term}' - {reason}")

    return issues


def check_stale_docs() -> list:
    """Check for documents that haven't been updated recently."""
    issues = []
    cutoff = datetime.now() - timedelta(days=STALE_THRESHOLD_DAYS)

    for md_file in DOCS_DIR.rglob('*.md'):
        # Skip archived documents
        if 'archive' in str(md_file):
            continue

        try:
            content = md_file.read_text(encoding='utf-8')
        except Exception:
            continue

        relative_path = md_file.relative_to(ROOT_DIR)

        # Look for a "Last updated" label with a YYYY-MM-DD date next to it.
        match = LAST_UPDATED_RE.search(content)
        if match:
            try:
                date = datetime.strptime(match.group(1), '%Y-%m-%d')
                if date < cutoff:
                    days_old = (datetime.now() - date).days
                    issues.append(f"{relative_path}: Last updated {match.group(1)} ({days_old} days ago)")
            except ValueError:
                pass

    return issues


def check_unparsable_last_updated() -> list:
    """Docs that carry a "Last updated" label but no date LAST_UPDATED_RE can
    read next to it.

    Before this existed, such a file was silently skipped by check_stale_docs
    -- indistinguishable from a file with no label at all -- which is exactly
    how a real, correctly-dated "**Last Updated:** ..." header could go
    unparsed for months without ever showing up in this report (the old
    `[:\\s]+` gap rejected the bold form's `:**`). Informational only, same
    as staleness itself -- see the NOTE in main() above check_stale_docs's
    call site.
    """
    issues = []

    for md_file in DOCS_DIR.rglob('*.md'):
        # Skip archived documents
        if 'archive' in str(md_file):
            continue

        try:
            content = md_file.read_text(encoding='utf-8')
        except Exception:
            continue

        if LAST_UPDATED_RE.search(content):
            continue  # parsed fine above

        if LAST_UPDATED_LABEL_RE.search(content):
            relative_path = md_file.relative_to(ROOT_DIR)
            issues.append(
                f"{relative_path}: has a 'Last updated' label but no parsable "
                f"YYYY-MM-DD date next to it"
            )

    return issues


def check_contract_header_dates() -> tuple:
    """For every CONTRACT_DOCS entry, fail if git says the file changed after
    the date its own header claims.

    Returns ``(failures, unrunnable)``. Anything in ``failures`` is BLOCKING.
    ``unrunnable`` names docs whose git history was not available to consult --
    reported out loud rather than skipped, because a gate that quietly cannot
    run is indistinguishable from a gate that passed, which is the exact
    failure mode this check exists to end.

    A SHALLOW clone is refused outright rather than answered. Measured in a
    real ``git clone --depth 1``: ``git log -1 -- <path>`` returns HEAD's own
    commit date for EVERY file, because the grafted root commit looks like it
    added the whole tree (README.md read 2026-09-06 with full history and
    2026-09-08 at depth 1). That does not make this check skip -- it makes it
    compare against the WRONG commit, so any commit dated after a header would
    fail the build for a doc nobody touched. Hence ``fetch-depth: 0`` on CI's
    lint-and-docs job: it is what lets the check RUN, not merely what keeps it
    honest.

    The comparison is deliberately against the file's last COMMIT date rather
    than its mtime: mtime moves when you open a file in an editor, and it is
    lost entirely on a fresh clone. A commit touching the file is the exact
    event that should have moved the header.

    Uncommitted working-tree edits do NOT trip this. Their commit is still in
    the future, so today's header date is newer than the last commit and the
    check passes -- which is what you want while you are mid-edit. It fires
    once the body change is committed with a stale header still in place.

    Never FAILS for an environmental reason: no git binary, no repository, a
    shallow clone or an untracked file all land in ``unrunnable``. A source
    tarball with no .git must not fail this check.
    """
    issues = []
    unrunnable = []
    notes = []

    # Ask ONCE whether git can be trusted about per-file history here.
    # A shallow clone reports HEAD for every path (see the docstring), which
    # would make this gate fire on docs nobody touched. Don't guess -- ask.
    try:
        probe = subprocess.run(
            ['git', 'rev-parse', '--is-shallow-repository'],
            cwd=str(ROOT_DIR), capture_output=True, text=True, timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return [], [
            "{0}: no usable git binary, so the header date could not be checked "
            "against the file's last commit.".format(rel_path)
            for rel_path in CONTRACT_DOCS
        ]
    if probe.returncode != 0 or (probe.stdout or '').strip() == 'true':
        reason = ('this is a SHALLOW clone, where git reports HEAD as the last '
                  "commit for every file"
                  if (probe.stdout or '').strip() == 'true'
                  else 'git could not identify a repository here')
        return [], [
            "{0}: header-date check did NOT run -- {1}. In CI, check out with "
            "fetch-depth: 0.".format(rel_path, reason)
            for rel_path in CONTRACT_DOCS
        ]

    for rel_path in CONTRACT_DOCS:
        full_path = ROOT_DIR / rel_path
        if not full_path.exists():
            issues.append(
                "Missing: {0} (contract-doc header-date target)".format(rel_path)
            )
            continue

        try:
            content = full_path.read_text(encoding='utf-8')
        except Exception:
            continue

        match = LAST_UPDATED_RE.search(content)
        if not match:
            issues.append(
                "{0}: no parsable 'Last updated: YYYY-MM-DD' in the header. "
                "A contract doc MUST carry one -- it is what a reader uses to "
                "decide whether the body is current.".format(rel_path)
            )
            continue

        try:
            header_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
        except ValueError:
            issues.append(
                "{0}: header date {1!r} is not a valid YYYY-MM-DD "
                "date.".format(rel_path, match.group(1))
            )
            continue

        try:
            proc = subprocess.run(
                ['git', 'log', '-1', '--date=short', '--format=%cd', '--',
                 rel_path],
                cwd=str(ROOT_DIR),
                capture_output=True,
                text=True,
                timeout=20,
            )
        except (OSError, subprocess.SubprocessError):
            unrunnable.append(
                "{0}: no usable git binary, so the header date could not be "
                "checked against the file's last commit.".format(rel_path)
            )
            continue
        if proc.returncode != 0:
            unrunnable.append(
                "{0}: git log failed ({1}), so the header date could not be "
                "checked.".format(rel_path, (proc.stderr or "").strip()[:120])
            )
            continue
        stamp = (proc.stdout or "").strip()
        if not stamp:
            # Shallowness is already ruled out above, so this is the file
            # being untracked -- a contract doc that exists on disk but in no
            # commit. Reported, not skipped: the check did not run.
            unrunnable.append(
                "{0}: git knows no commit touching this file, so it appears "
                "untracked. The header-date check did NOT run.".format(rel_path)
            )
            continue
        try:
            commit_date = datetime.strptime(stamp, '%Y-%m-%d').date()
        except ValueError:
            unrunnable.append(
                "{0}: git returned an unparsable commit date {1!r}.".format(
                    rel_path, stamp)
            )
            continue

        lag = (commit_date - header_date).days
        if lag > MERGE_LAG_GRACE_DAYS:
            issues.append(
                "{0}: header says 'Last updated: {1}' but git's last commit "
                "touching this file is {2} -- {3} days later, past the "
                "{4}-day merge-lag grace. The body moved and the header did "
                "not: bump the header date (and any version string next to "
                "it) in the same commit as the body change.".format(
                    rel_path, header_date.isoformat(), commit_date.isoformat(),
                    lag, MERGE_LAG_GRACE_DAYS
                )
            )
        elif lag > 0:
            notes.append(
                "{0}: header says {1}, last commit {2} ({3} day(s) later). "
                "Within the merge-lag grace, so not blocking -- a squash "
                "merge restamps the commit date without anyone editing the "
                "body.".format(rel_path, header_date.isoformat(),
                               commit_date.isoformat(), lag)
            )

    # Printed rather than returned: the (failures, unrunnable) shape is itself
    # a tested contract, and a within-grace lag is neither. It must still be
    # SAID out loud -- a gate that silently tolerates something is how the
    # tolerance turns into a blind spot.
    for note in notes:
        print("  ℹ  {0}".format(note))

    return issues, unrunnable


def check_context_budget() -> list:
    """Check that always-loaded AI-context files stay under their size ceiling."""
    issues = []
    for rel_path, ceiling in CONTEXT_BUDGET:
        full_path = ROOT_DIR / rel_path
        if not full_path.exists():
            issues.append(f"Missing: {rel_path} (context-budget target)")
            continue
        size = full_path.stat().st_size
        if size > ceiling:
            over = size - ceiling
            issues.append(
                f"{rel_path}: {size:,} bytes exceeds the {ceiling:,}-byte context "
                f"ceiling by {over:,} (~{size // 4:,} tokens resident every session). "
                f"Split closed/historical content into docs/archive/ -- do not raise "
                f"the ceiling."
            )
    return issues


# A citation like `docs/OPEN_ISSUES.md:161` is wrong the moment anything above line 161 is
# archived, and it then points at an unrelated issue instead of failing loudly. Four such
# citations in .planning/milestones/v9.0.0-MILESTONE-AUDIT.md broke exactly that way when the
# tracker was trimmed on 2026-09-20 (Codex, PR #350). Quote the entry's headline instead.
FENCE_RE = re.compile(r'^\s*(?:```|~~~)')
LINE_CITATION_FILES = ('OPEN_ISSUES.md', 'CHANGELOG.md', 'CLAUDE.md')
# Both spellings a reader can click: ``file.md:161`` (editor/grep form) and ``file.md#L161``
# (GitHub's anchor form, with or without a ``-L170`` range end).
LINE_CITATION_RE = re.compile(
    r'(?:docs/)?(' + '|'.join(re.escape(f) for f in LINE_CITATION_FILES) + r')[:#]L?(\d+)'
)
# Verbatim tool output and historical dumps quote line numbers as data, not as a citation.
# Archived history is exempt: ``docs/archive/``, ``.planning/milestones/<version>-phases/``,
# ``.planning/quick/`` and ``.planning/debug/`` record what a file said at the time, so a line
# number there is a historical statement, not a pointer a reader is meant to follow today.
LINE_CITATION_SKIP_DIRS = ('docs/archive/', '.planning/quick/', '.planning/debug/')
LINE_CITATION_SKIP_RE = re.compile(r'^\.planning/milestones/[^/]+-phases/')


def _line_citation_candidates() -> list:
    """Tracked Markdown that a human is expected to follow a citation in."""
    out = subprocess.run(
        ['git', 'ls-files', '-z', '*.md'], cwd=ROOT_DIR, capture_output=True, check=True,
    ).stdout.decode('utf-8', 'surrogateescape')
    files = []
    for rel in out.split('\0'):
        if not rel or not rel.endswith('.md'):
            continue
        if any(rel.startswith(d) for d in LINE_CITATION_SKIP_DIRS) or LINE_CITATION_SKIP_RE.match(rel):
            continue
        files.append(rel)
    return files


def check_line_number_citations() -> list:
    """No tracked doc may CITE a moving document by line number.

    Fenced code blocks are skipped: a pasted `rg`/pytest/compiler transcript quoting
    ``CHANGELOG.md:42`` is verbatim tool output, i.e. data about a moment in time, not a pointer a
    reader is meant to follow. Inline code is NOT skipped -- `docs/OPEN_ISSUES.md:161` in running
    prose is exactly the citation this check exists to stop.
    """
    issues = []
    for rel in _line_citation_candidates():
        path = ROOT_DIR / rel
        try:
            text = path.read_text(encoding='utf-8', errors='replace')
        except OSError:
            continue
        in_fence = False
        for n, line in enumerate(text.splitlines(), start=1):
            if FENCE_RE.match(line):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for m in LINE_CITATION_RE.finditer(line):
                issues.append(
                    f"{rel}:{n} cites {m.group(1)} by line number ({m.group(0)}). "
                    f"Rows move whenever anything above them is archived, so the citation "
                    f"silently points at an unrelated entry. Quote the entry's headline "
                    f"instead (grep finds it wherever it sits)."
                )
    return issues


def check_broken_links() -> list:
    """Check for broken internal links (docs/ and the root instruction files)."""
    issues = []

    for md_file in _scanned_docs(term_scan=False):

        try:
            content = md_file.read_text(encoding='utf-8')
        except Exception:
            continue

        relative_path = md_file.relative_to(ROOT_DIR)
        md_dir = md_file.parent

        # Remove code blocks before searching for links (to avoid false positives)
        content_no_code = re.sub(r'```[\s\S]*?```', '', content)
        content_no_code = re.sub(r'`[^`]+`', '', content_no_code)

        # Find markdown links: [text](path)
        links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', content_no_code)

        for link_text, link_path in links:
            # Skip external links
            if link_path.startswith(('http://', 'https://', 'mailto:')):
                continue

            # Skip anchors
            if link_path.startswith('#'):
                continue

            # Remove anchor from path
            link_path = link_path.split('#')[0]

            if not link_path:
                continue

            # Strip trailing line-number suffix used by `file:N` / `file:N-M`
            # code references (e.g. `web/api.py:680`, `genizah_core.py:3940-3961`).
            # The link points at a real file; the suffix is editor-jump metadata.
            line_ref_path = re.sub(r':\d+(?:-\d+)?$', '', link_path)

            # Try BOTH resolution strategies: docs-relative AND project-root-relative.
            # Many doc references use project-root-relative paths (e.g.
            # `[web/api.py](web/api.py)` from inside docs/) instead of `../web/api.py`.
            # A link is "broken" only if NEITHER candidate exists.
            if line_ref_path.startswith('/'):
                candidates = [ROOT_DIR / line_ref_path.lstrip('/')]
            else:
                candidates = [md_dir / line_ref_path, ROOT_DIR / line_ref_path]

            if not any(c.resolve().exists() for c in candidates):
                issues.append(f"{relative_path}: Broken link to '{link_path}'")

    return issues


def main():
    """Run all documentation checks."""
    print_header("GenizahSearch Documentation Health Check")
    print(f"Checking: {DOCS_DIR}")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    total_issues = 0

    # 1. Check critical documents
    print("\n📁 Critical Documents")
    print("-" * 40)
    missing = check_critical_docs()
    if missing:
        for m in missing:
            print_status(False, m)
        total_issues += len(missing)
    else:
        print_status(True, "All critical documents exist")

    # 2. Check for outdated terms
    print("\n🔍 Outdated Terminology")
    print("-" * 40)
    outdated = check_outdated_terms()
    if outdated:
        for o in outdated:
            print_warning(o)
        total_issues += len(outdated)
    else:
        print_status(True, "No outdated terms found")

    # 3. Check for stale documents
    # NOTE: staleness is an INFORMATIONAL freshness reminder, not a CI failure.
    # Docs cross the day threshold on a rolling calendar basis with no code
    # change, so counting them toward the exit code turns the build red for the
    # wrong reason. Printed below for visibility but excluded from total_issues.
    # The same applies to `unparsable` (a "Last updated" label present with no
    # date our regex can read next to it) -- it is a freshness-reporting gap,
    # not a documentation defect worth failing the build over.
    print("\n📅 Document Freshness")
    print("-" * 40)
    stale = check_stale_docs()
    unparsable = check_unparsable_last_updated()
    if stale:
        for s in stale:
            print_warning(s)
    if unparsable:
        for u in unparsable:
            print_warning(u)
    if not stale and not unparsable:
        print_status(True, f"All documents updated within {STALE_THRESHOLD_DAYS} days")

    # Contract docs are held to a harder rule than freshness: their header
    # date is a published claim, so a body change committed after it is a
    # BLOCKING failure (unlike `stale` / `unparsable` above, which are
    # reminders). See CONTRACT_DOCS.
    header_drift, header_unrunnable = check_contract_header_dates()
    if header_drift:
        for h in header_drift:
            print_status(False, h)
        total_issues += len(header_drift)
    for u in header_unrunnable:
        print_warning(u)
    if not header_drift and not header_unrunnable:
        print_status(
            True,
            "Contract-doc header dates match their last git commit "
            f"({len(CONTRACT_DOCS)} checked)",
        )

    # 4. Check the always-loaded context files against their size ceiling
    print("\n🧠 AI Context Budget")
    print("-" * 40)
    oversize = check_context_budget()
    if oversize:
        for o in oversize:
            print_status(False, o)
        total_issues += len(oversize)
    else:
        for rel_path, ceiling in CONTEXT_BUDGET:
            size = (ROOT_DIR / rel_path).stat().st_size
            pct = 100 * size / ceiling
            print_status(True, f"{rel_path}: {size:,} / {ceiling:,} bytes ({pct:.0f}%)")

    # 5. Check that nothing cites a moving document by line number
    print("\n\U0001f4cd Stable Citations")
    print("-" * 40)
    line_cites = check_line_number_citations()
    if line_cites:
        for c in line_cites:
            print_status(False, c)
        total_issues += len(line_cites)
    else:
        print_status(True, "No line-numbered citations of OPEN_ISSUES.md / CHANGELOG.md / CLAUDE.md")

    # 6. Check for broken links
    print("\n🔗 Internal Links")
    print("-" * 40)
    broken = check_broken_links()
    if broken:
        for b in broken:
            print_warning(b)
        total_issues += len(broken)
    else:
        print_status(True, "All internal links valid")

    # Summary
    print_header("Summary")
    # total_issues counts only BLOCKING checks (missing / outdated / broken).
    # Stale docs (and unparsable dates) are reported separately as a
    # non-blocking freshness reminder.
    if stale:
        print(f"ℹ️  {len(stale)} stale doc(s) over {STALE_THRESHOLD_DAYS} days "
              f"(informational — does not fail CI)")
    if unparsable:
        print(f"ℹ️  {len(unparsable)} doc(s) with an unparsable 'Last updated' "
              f"date (informational — does not fail CI)")
    if total_issues == 0:
        print("✅ All blocking checks passed! Documentation is healthy.")
    else:
        print(f"❌ Found {total_issues} blocking issue(s):")
        print(f"   - Missing documents: {len(missing)}")
        print(f"   - Outdated terms: {len(outdated)}")
        print(f"   - Context-budget overruns: {len(oversize)}")
        print(f"   - Broken links: {len(broken)}")
        print(f"   - Contract-doc header drift: {len(header_drift)}")
        print("\nReview docs/archive/DOCUMENTATION_MAINTENANCE.md for guidance.")

    return 0 if total_issues == 0 else 1


if __name__ == '__main__':
    exit(main())
