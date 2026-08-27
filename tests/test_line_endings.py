# -*- coding: utf-8 -*-
r"""Guard: no tracked .py file may carry mixed line endings or a lone CR.

REGRESSION ORIGIN (2026-08-27)
------------------------------
17 tracked .py files in the owner's ``C:\GenizahSearch`` working tree held a mix
of CRLF and bare-LF terminators *inside one file*. It never reached a commit --
every tracked .py blob in this repo is pure LF -- because ``core.autocrlf=true``
normalises the working tree CRLF -> LF on the way in, folding a mixed file back
onto exactly its pure-LF blob. ``git diff`` and ``git hash-object`` therefore
report it clean, so the damage does not survive review; it survives *unreviewed*.

``git ls-files --eol`` does name the ordinary case (``w/mixed``), but it is NOT a
sufficient detector and this guard deliberately does not delegate to it: a single
lone CR byte anywhere in a file makes git classify the file as BINARY
(``i/-text w/-text``), which disables git's eol handling for it wholesale and
reports no ending at all -- verified against two real tracked files,
``.planning/milestones/v6.5.0-ROADMAP.md`` and ``v7.6-ROADMAP.md``, whose
committed blobs are mixed (299/375 CRLF + 2 bare LF + 1 lone CR) and which git
reports as ``-text``, never ``mixed``. Counting terminator bytes in Python sees
through that; asking git does not.

Nothing crashes on it. What it breaks is byte-level matching against the source:
a PowerShell ``.Replace()`` carrying LF literals, or a mutation harness matching
a CRLF pattern, finds no match and then reports a false RESULT rather than an
error -- a silent wrong answer, which is the expensive kind.

WHY THIS TEST IS PLATFORM-INVARIANT
-----------------------------------
It asserts only that a file is not *mixed*; never which ending it has. That is
the one property which holds identically in every checkout:

  * Windows, ``core.autocrlf=true``   -> checkout gives pure CRLF  (not mixed)
  * Linux / CI, no conversion applied -> checkout gives pure LF    (not mixed)

Only post-checkout damage can make a file mixed, so green means the same thing on
the owner's box and in CI. The lone-CR guard below is invariant for the same
reason -- no eol conversion in either direction creates or removes a bare CR.
Repair with::

    python scripts/normalize_line_endings.py --fix

The detector is imported from that script rather than re-implemented here, so
the guard and the repair can never disagree about what "mixed" means.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.normalize_line_endings import count_endings, is_mixed, to_eol

REPO_ROOT = Path(__file__).resolve().parents[1]

CRLF = b"\r\n"
LF = b"\n"


def _tracked_python_files():
    """Repo-relative paths of tracked .py files, or None when git is unavailable.

    Tracked-only keeps virtualenvs, build output and __pycache__ out of the scan
    without maintaining an ignore list.
    """
    if shutil.which("git") is None:
        return None
    proc = subprocess.run(
        ["git", "ls-files", "-z", "--", "*.py"],
        cwd=REPO_ROOT,
        capture_output=True,
    )
    if proc.returncode != 0:
        return None
    return [p for p in proc.stdout.decode("utf-8").split("\0") if p]


def test_no_tracked_python_file_has_mixed_line_endings():
    paths = _tracked_python_files()
    if paths is None:
        pytest.skip("git unavailable or not a work tree; cannot enumerate tracked files")
    assert paths, "git ls-files returned no .py files -- the guard would be vacuous"

    offenders = []
    for rel in paths:
        try:
            data = (REPO_ROOT / rel).read_bytes()
        except OSError:
            continue  # a tracked-but-absent path is another test's problem
        crlf, bare_lf, _lone_cr = count_endings(data)
        if crlf and bare_lf:
            offenders.append(f"{rel} (crlf={crlf}, bare_lf={bare_lf})")

    assert not offenders, (
        f"{len(offenders)} tracked .py file(s) have MIXED line endings. Git cannot "
        "see this (core.autocrlf normalises it away), but byte-level source "
        "matching silently fails on it. Repair:\n"
        "    python scripts/normalize_line_endings.py --fix\n\n"
        + "\n".join(f"  {line}" for line in offenders)
    )


def test_no_tracked_python_file_has_a_lone_cr():
    r"""A stray CR is the worse variant: it turns git's eol handling OFF.

    Once git sees a lone CR it calls the file binary, and from then on it neither
    normalises on commit nor converts on checkout -- so the file's endings become
    whatever the last tool to touch it left behind, differently on every machine,
    with `git ls-files --eol` reporting `-text` instead of naming the problem.

    Zero tracked .py files carry one today, so this guard costs nothing. A raw CR
    byte in Python source is essentially always damage; if one is ever genuinely
    needed, write it as the escape `\r` in a literal rather than embedding the
    byte, which keeps the source pure-ASCII-terminated and this guard green.
    """
    paths = _tracked_python_files()
    if paths is None:
        pytest.skip("git unavailable or not a work tree; cannot enumerate tracked files")

    offenders = []
    for rel in paths:
        try:
            data = (REPO_ROOT / rel).read_bytes()
        except OSError:
            continue
        _crlf, _bare_lf, lone_cr = count_endings(data)
        if lone_cr:
            offenders.append(f"{rel} (lone_cr={lone_cr})")

    assert not offenders, (
        f"{len(offenders)} tracked .py file(s) contain a lone CR byte, which makes "
        "git treat them as binary and silently stop managing their line endings:\n"
        + "\n".join(f"  {line}" for line in offenders)
    )


# ---------------------------------------------------------------------------
# The detector is itself under test. A guard whose instrument is broken passes
# on damaged input, which is worse than having no guard -- see the same concern
# recorded in scripts/check_sys_id_prefixes.py.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "data, mixed, why",
    [
        (b"a = 1\r\nb = 2\r\n", False, "pure CRLF (a Windows checkout)"),
        (b"a = 1\nb = 2\n", False, "pure LF (a Linux checkout)"),
        (b"", False, "empty file"),
        (b"a = 1", False, "single line, no terminator"),
        (b"a = 1\r\nb = 2\n", True, "CRLF then bare LF -- the real shape"),
        (b"a = 1\nb = 2\r\n", True, "bare LF then CRLF"),
        (b"a\r\nb\nc\r\nd\n", True, "alternating, as a line-by-line rewrite leaves it"),
    ],
)
def test_detector_recognises_the_shapes_it_must_catch(data, mixed, why):
    assert is_mixed(data) is mixed, why


def test_count_endings_does_not_double_count_the_lf_inside_a_crlf():
    # The whole detector rests on this subtraction: b'\r\n' contains b'\n', so a
    # naive count of b'\n' reports every CRLF file as carrying bare LFs too, and
    # the guard would fail on all 900+ files at once.
    crlf, bare_lf, lone_cr = count_endings(b"a\r\nb\r\n")
    assert (crlf, bare_lf, lone_cr) == (2, 0, 0)


def test_lone_cr_is_counted_but_not_treated_as_mixed():
    # A bare \r is separate damage and can be a legitimate byte in a literal, so
    # it is reported (by the script) and never rewritten.
    crlf, bare_lf, lone_cr = count_endings(b"a\rb\r\n")
    assert (crlf, bare_lf, lone_cr) == (1, 0, 1)
    assert is_mixed(b"a\rb\r\n") is False


@pytest.mark.parametrize("eol", [CRLF, LF])
def test_to_eol_normalises_and_preserves_content(eol):
    mixed = b"import os\r\n\r\nx = 1\ny = 2\r\nz = 3\n"
    result = to_eol(mixed, eol)

    assert not is_mixed(result)
    assert result.count(eol) == 5
    # Content -- everything that is not a terminator -- must survive untouched.
    assert result.replace(CRLF, LF) == mixed.replace(CRLF, LF)
    # And the rewrite is a fixed point: running the repair twice is a no-op.
    assert to_eol(result, eol) == result


def test_to_eol_does_not_produce_cr_cr_lf():
    # The classic bug in this area: replacing LF -> CRLF on text that already
    # holds CRLF yields \r\r\n. Normalising down to LF first is what prevents it.
    assert b"\r\r\n" not in to_eol(b"a\r\nb\nc\r\n", CRLF)
