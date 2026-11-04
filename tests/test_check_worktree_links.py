# -*- coding: utf-8 -*-
"""scripts/check_worktree_links.ps1 names every reparse point in a worktree and exits 1.

Why: on 2026-11-04 a worktree under .claude/worktrees/ held junctions to the main
checkout's gitignored data folders, and `git worktree remove --force` followed by
`rm -rf` walked them and emptied C:\\GenizahSearch\\nli_data and fgp_data
(docs/open_issues.d/2026-11-04-worktree-junctions-emptied-main-data.md). The scanner is
what scripts/init.ps1 runs at every session start and what the cleanup step runs before
a forced removal, so it must be shown able to fail: junctions at the top level, one
level down and two levels down are all named with their target and the exit code is 1
(Codex, PR round 1: the first version looked one level down only, and `rm -rf` walks at
any depth); a clean folder exits 0; and the scan itself changes nothing (the links stay
until `rmdir` removes them, and the target's file survives both the scan and the `rmdir`).

Windows only: junctions and `cmd /c mklink /J` exist nowhere else. The CI matrix has a
windows-latest lane, so this runs there for real rather than skipping everywhere.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="junctions are a Windows feature")

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_worktree_links.ps1"
INIT = ROOT / "scripts" / "init.ps1"


def _scan(folder: Path) -> subprocess.CompletedProcess:
    cmd = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(SCRIPT),
           "-Path", str(folder)]
    return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                          errors="replace", timeout=120)


def _junction(link: Path, target: Path) -> None:
    subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(target)],
                   check=True, capture_output=True, timeout=60)


def _rmdir(link: Path) -> None:
    # The one safe removal: deletes the link, never what it points at.
    subprocess.run(["cmd", "/c", "rmdir", str(link)], check=True, capture_output=True, timeout=60)


@pytest.fixture
def worktree(tmp_path: Path) -> Path:
    wt = tmp_path / "wt"
    wt.mkdir()
    (wt / ".git").write_text("gitdir: elsewhere\n")   # a linked worktree's .git is a file
    (wt / "shared").mkdir()
    (wt / "shared" / "module.py").write_text("")
    (wt / "shared" / "cache").mkdir()
    return wt


@pytest.fixture
def main_data(tmp_path: Path) -> Path:
    d = tmp_path / "main_checkout_data"
    d.mkdir()
    (d / "sidecar.db").write_bytes(b"keep me")
    return d


def test_a_clean_worktree_exits_0(worktree: Path) -> None:
    r = _scan(worktree)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "no reparse points" in r.stdout


def test_junctions_at_every_depth_are_named_with_their_target(
        worktree: Path, main_data: Path) -> None:
    links = [worktree / "nli_data",                        # top level
             worktree / "shared" / "fgp_data",             # one level down
             worktree / "shared" / "cache" / "data_link"]  # two levels down
    for link in links:
        _junction(link, main_data)
    try:
        r = _scan(worktree)
        assert r.returncode == 1, r.stdout + r.stderr
        for link in links:
            assert str(link) in r.stdout, r.stdout
        assert r.stdout.count(str(main_data)) >= len(links), r.stdout
        assert "Junction" in r.stdout, r.stdout
        assert "rmdir" in r.stdout and "--force" in r.stdout, r.stdout
        # The scan changed nothing.
        assert all(link.exists() for link in links)
        assert (main_data / "sidecar.db").read_bytes() == b"keep me"
    finally:
        for link in links:
            _rmdir(link)
    # rmdir took the links and left the target alone; the folder is clean again.
    assert not any(link.exists() for link in links)
    assert (main_data / "sidecar.db").read_bytes() == b"keep me"
    assert _scan(worktree).returncode == 0


def test_init_runs_the_scanner() -> None:
    """scripts/init.ps1 is where a session meets the warning; pin that it still calls
    the scanner rather than carrying a copy that could drift."""
    text = INIT.read_text(encoding="utf-8")
    assert "check_worktree_links.ps1" in text
    assert "rmdir" in text
