#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""Report and repair tracked source files whose line endings are MIXED.

WHY THIS EXISTS (2026-08-27)
----------------------------
17 tracked ``.py`` files in the owner's ``C:\GenizahSearch`` working tree
carried a mix of CRLF and bare-LF terminators *inside the same file*. Git could
not see it, for two reasons that compound:

  * The committed blobs are pure LF. All 911 non-empty tracked ``.py`` files
    report ``i/lf`` under ``git ls-files --eol``, so the damage was never
    committed -- it lives only in a working tree.
  * With ``core.autocrlf=true`` and no ``text`` attribute on the path, git
    normalises the working tree CRLF -> LF on the way in. A mixed file folds
    back to exactly its pure-LF blob, so ``git diff`` shows nothing and
    ``git hash-object`` returns the blob's own OID. (Measured on git 2.43,
    ``git status`` did still mark the file ``M`` -- with an empty diff to show
    for it -- so the file may look dirty for no discoverable reason rather than
    look clean. Either way review has nothing to look at.)

So the damage is created *after* checkout, by tools that write LF lines into a
file the checkout had made CRLF. It is invisible to review, and it breaks
anything that byte-matches the source: a PowerShell ``.Replace()`` carrying LF
literals, or a mutation harness matching a CRLF pattern, silently fails to
match and then reports a false result rather than an error.

``tests/test_line_endings.py`` is the standing guard; this script is the repair.

``git ls-files --eol`` reports the ordinary case as ``w/mixed``, but do not rely
on it alone: one lone CR byte makes git classify the file as binary
(``i/-text w/-text``), which switches git's eol handling off for that path and
hides the mixing entirely. Two tracked files in this repo are in exactly that
state -- ``.planning/milestones/v6.5.0-ROADMAP.md`` and ``v7.6-ROADMAP.md``, both
mixed in the committed blob. Counting bytes, as this script does, sees them.

WHAT "NORMALISE" MEANS HERE
---------------------------
Under the default ``--eol auto`` the target is whatever git itself would check
out for that path *in this working tree*:

  * pinned ``eol=lf`` in .gitattributes  -> LF, always, whatever ``--eol`` says
  * ``core.autocrlf=true``               -> CRLF  (the owner's Windows box)
  * anything else                        -> LF    (Linux / CI checkouts)

Blindly forcing CRLF everywhere would be wrong on a Linux checkout: the blobs
are LF there and nothing converts on the way in, so CRLF files would show up as
genuinely modified. Pass ``--eol crlf`` / ``--eol lf`` to force a target when
that is what you mean.

An ``eol=lf`` pin overrides ``--eol crlf`` rather than being skipped by it: such
a path is repaired *to LF*, which is both what .gitattributes wants and what
keeps the file byte-identical to its blob. .gitattributes holds those at LF
because their sha256 is pinned in the Discovery V4 acquisition manifests, and a
CRLF rewrite would make the build refuse its own inputs.

Usage:
    python scripts/normalize_line_endings.py               # report only; exit 1 if mixed
    python scripts/normalize_line_endings.py --fix         # rewrite the mixed ones
    python scripts/normalize_line_endings.py --ext .py .ps1 --fix
    python scripts/normalize_line_endings.py --eol crlf --fix
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CRLF = b"\r\n"
LF = b"\n"

#: Extensions scanned by default. Source only -- the point of the check is
#: byte-level matching against code, and data files under .gitattributes pins
#: have their own tests.
DEFAULT_EXTS = (".py",)


# ---------------------------------------------------------------------------
# Detection -- imported by tests/test_line_endings.py, so the guard and the
# repair share ONE definition of "mixed" and cannot drift apart.
# ---------------------------------------------------------------------------

def count_endings(data: bytes) -> tuple[int, int, int]:
    """Return ``(crlf, bare_lf, lone_cr)`` terminator counts for ``data``."""
    crlf = data.count(CRLF)
    bare_lf = data.count(LF) - crlf
    lone_cr = data.count(b"\r") - crlf
    return crlf, bare_lf, lone_cr


def is_mixed(data: bytes) -> bool:
    """True when ``data`` carries BOTH CRLF and bare-LF terminators."""
    crlf, bare_lf, _lone_cr = count_endings(data)
    return bool(crlf and bare_lf)


def to_eol(data: bytes, eol: bytes) -> bytes:
    r"""Rewrite every CRLF and bare-LF terminator in ``data`` to ``eol``.

    Lone CRs are deliberately left as they are. A bare ``\r`` in a .py file is
    almost always damage too, but it can legitimately be a raw byte inside a
    literal, and folding that into CRLF would change what the program does.
    ``main`` reports them instead.
    """
    return data.replace(CRLF, LF).replace(LF, eol)


# ---------------------------------------------------------------------------
# Git interrogation
# ---------------------------------------------------------------------------

def _git(args: list[str], repo: str, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, **kwargs)


def tracked_files(exts, repo: str = REPO_ROOT) -> list[str]:
    """Repo-relative paths of tracked files matching ``exts``.

    Tracked-only on purpose: it keeps virtualenvs, build output and vendored
    trees out of the scan without maintaining an ignore list.
    """
    # Accept ".py" or "py"; without the dot the glob would also match names that
    # merely END in those letters (`--ext py` catching a file called `copy`).
    patterns = [f"*{ext if ext.startswith('.') else '.' + ext}" for ext in exts]
    proc = _git(["ls-files", "-z", "--", *patterns], repo)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode("utf-8", "replace").strip())
    return [p for p in proc.stdout.decode("utf-8").split("\0") if p]


def eol_attributes(paths: list[str], repo: str = REPO_ROOT) -> dict[str, str]:
    """Map path -> its ``eol`` gitattribute (``lf``/``crlf``), pinned paths only.

    ``git check-attr --stdin -z`` emits NUL-separated (path, attr, value)
    triples; ``unspecified`` paths are dropped here so the dict only holds pins.
    """
    if not paths:
        return {}
    proc = subprocess.run(
        ["git", "check-attr", "--stdin", "-z", "eol"],
        cwd=repo,
        input="\0".join(paths).encode("utf-8"),
        capture_output=True,
    )
    if proc.returncode != 0:
        return {}
    fields = proc.stdout.decode("utf-8").split("\0")
    pins = {}
    for i in range(0, len(fields) - 2, 3):
        path, _attr, value = fields[i], fields[i + 1], fields[i + 2]
        if value in ("lf", "crlf"):
            pins[path] = value
    return pins


def checkout_eol(repo: str = REPO_ROOT) -> bytes:
    """The ending git gives an attribute-free text file on checkout here.

    Only ``core.autocrlf=true`` produces CRLF. ``core.eol`` is deliberately not
    consulted: it applies to paths carrying the ``text`` attribute, and nothing
    in scope has one (``git check-attr text -- <any .py>`` is ``unspecified``).
    """
    autocrlf = _git(["config", "--get", "core.autocrlf"], repo).stdout
    return CRLF if autocrlf.decode("utf-8", "replace").strip().lower() == "true" else LF


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _name(eol: bytes) -> str:
    return "CRLF" if eol == CRLF else "LF"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Report/repair tracked files with mixed line endings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--fix", action="store_true",
                       help="rewrite the mixed files (default: report only)")
    parser.add_argument("--eol", choices=("auto", "crlf", "lf"), default="auto",
                       help="target ending; 'auto' (default) matches what git "
                            "checks out in this working tree")
    parser.add_argument("--ext", nargs="+", default=list(DEFAULT_EXTS),
                       metavar="EXT", help="extensions to scan (default: .py)")
    parser.add_argument("--repo", default=REPO_ROOT,
                       help="repository to scan (default: this checkout)")
    args = parser.parse_args(argv)

    repo = os.path.abspath(args.repo)
    try:
        paths = tracked_files(args.ext, repo)
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"ERROR: cannot list tracked files in {repo}: {exc}")
        return 2

    pins = eol_attributes(paths, repo)
    default_eol = {"crlf": CRLF, "lf": LF}.get(args.eol) or checkout_eol(repo)

    print(f"Scanning {len(paths):,} tracked {'/'.join(args.ext)} files in {repo}")
    print(f"Target ending: {_name(default_eol)}"
          f"{' (from git checkout behaviour here)' if args.eol == 'auto' else ' (forced)'}")

    mixed, lone_cr, fixed = [], [], 0
    for rel in paths:
        full = os.path.join(repo, rel)
        try:
            with open(full, "rb") as handle:
                data = handle.read()
        except OSError as exc:
            print(f"  ! unreadable, skipped: {rel} ({exc})")
            continue
        if b"\0" in data:
            continue  # binary; terminator counting is meaningless
        crlf, bare_lf, cr = count_endings(data)
        if cr:
            lone_cr.append((rel, cr))
        if not (crlf and bare_lf):
            continue
        # An `eol=lf` pin overrides the target even under `--eol crlf`: pure LF
        # is what .gitattributes wants for that path, and it is what keeps the
        # file's pinned sha256 intact. Repair it to LF rather than abandon it.
        target = LF if pins.get(rel) == "lf" else default_eol
        mixed.append((rel, crlf, bare_lf, target))
        if not args.fix:
            continue
        rewritten = to_eol(data, target)
        with open(full, "wb") as handle:
            handle.write(rewritten)
        with open(full, "rb") as handle:
            check = handle.read()
        if is_mixed(check):  # pragma: no cover - would mean to_eol is broken
            print(f"  ! STILL MIXED after rewrite: {rel}")
            return 2
        fixed += 1

    print()
    if not mixed:
        print(f"OK: no mixed line endings ({len(paths):,} files scanned).")
    else:
        verb = "fixed  " if args.fix else "MIXED  "
        for rel, crlf, bare_lf, target in mixed:
            pin = "  (pinned eol=lf)" if pins.get(rel) == "lf" else ""
            print(f"  {verb}{rel}  crlf={crlf} bare_lf={bare_lf}"
                  f" -> {_name(target)}{pin}")
        print(f"\n{len(mixed)} file(s) with mixed endings"
              + (f"; {fixed} rewritten" if args.fix else ""))
    if lone_cr:
        print("\nNote -- lone CR bytes (not rewritten, may be intentional literals):")
        for rel, count in lone_cr:
            print(f"  {rel}  lone_cr={count}")

    if not mixed:
        return 0
    if args.fix and fixed == len(mixed):
        return 0
    if not args.fix:
        print("\nRe-run with --fix to rewrite them.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
