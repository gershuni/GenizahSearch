#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Phase 97 LD-12: static auditor for forbidden tokens in Phase 97 plan files.

Usage:
    python scripts/check_plan_artifacts.py .planning/phases/97-more-local-features/

Forbidden tokens (absent from plan .md files unless in a negation/historical context):
  - requirements-desktop.txt
  - pytest.ini
  - browse_text_edit
  - _build_pages_html
  - _pending_cleanup

Exit code 1 on any finding, 0 on clean.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Tokens that must NOT appear in Phase 97 plan files.
FORBIDDEN_TOKENS = [
    "requirements-desktop.txt",
    "pytest.ini",
    "browse_text_edit",
    "_build_pages_html",
    "_pending_cleanup",
]

# Lines are exempt if they contain any of these negation/context indicators
# (case-insensitive).  This prevents false positives in historical/removal context.
_NEGATION_PATTERNS = [
    r"\bNOT\b",
    r"\bMUST NOT\b",
    r"\bREPLACES?\b",
    r"\bwas replaced\b",
    r"\binstead of\b",
    r"\bdo NOT\b",
    r"\bREPLACED\b",
    r"\bremoved\b",
    r"\banti-?pattern\b",
    # Meta-references: lines that list/describe the forbidden tokens themselves
    r"\bforbidden token",
    r"\bNo references? to\b",
    r"\babsent\b",
    r"auditor for forbidden",
    r"ensures the forbidden",
    # Script definition context (list of tokens to check)
    r"FORBIDDEN_TOKENS",
    r"\bcheck_plan_artifacts\b",
    # Grep/assertion context: "returns 0 matches" / "forbid X" / "0 occurrences"
    r"returns 0 matches",
    r"\bforbid\b",
    r"0 occurrences",
    r"must not appear",
    r"guard\b",
    r"\banti-bypass\b",
    # Test-name references (e.g., test_no_invented_build_pages_html)
    r"test_no_invented",
    r"test_no_widget",
    # Config key context: pytest.ini_options is a toml key, not a file reference
    r"ini_options",
    # "invented" context: referencing the name only to assert it must not appear
    r"invented",
    # grep/test assertion lines that reference the name to verify absence
    r"test_cap_is_500",
]

# Files exempt from the check entirely (review files, raw codex transcripts, etc.)
_EXEMPT_FILE_PATTERNS = [
    r".*-REVIEWS\.md$",
    r".*-CODEX-.*",
    r".*-CODEX\..+",
    r".*CODEX.*\.md$",
    r".*CODEX.*\.txt$",
]

_NEGATION_RE = re.compile(
    "|".join(_NEGATION_PATTERNS),
    re.IGNORECASE,
)

_EXEMPT_FILE_RE = re.compile(
    "|".join(_EXEMPT_FILE_PATTERNS),
    re.IGNORECASE,
)


def _is_exempt_file(path: Path) -> bool:
    return bool(_EXEMPT_FILE_RE.match(path.name))


def _check_file(path: Path) -> list[str]:
    """Return list of violation strings for this file."""
    violations: list[str] = []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return [f"{path}: cannot read file: {exc}"]

    for lineno, line in enumerate(text.splitlines(), start=1):
        for token in FORBIDDEN_TOKENS:
            if token in line:
                # Check if the line is in a negation/historical context
                if _NEGATION_RE.search(line):
                    continue
                violations.append(
                    f"{path}:{lineno}: forbidden token {token!r}"
                )
    return violations


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check Phase 97 plan files for forbidden tokens."
    )
    parser.add_argument(
        "target",
        nargs="?",
        default=".",
        help="File or directory to check (default: current directory)",
    )
    args = parser.parse_args()

    target = Path(args.target)
    if not target.exists():
        print(f"ERROR: target does not exist: {target}", file=sys.stderr)
        return 2

    # Collect .md files
    if target.is_file():
        md_files = [target] if target.suffix == ".md" else []
    else:
        md_files = list(target.glob("**/*.md"))

    all_violations: list[str] = []
    for f in sorted(md_files):
        if _is_exempt_file(f):
            continue
        all_violations.extend(_check_file(f))

    if all_violations:
        for v in all_violations:
            print(v)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
