"""Where the private repository's files are, for the scripts that read or write them.

The private repository is checked out beside the main checkout as ``GenizahSearch-private``
and keeps the same relative paths (its planning record is at ``.planning/``). From a worktree
(``.claude/worktrees/<name>``) the folder is found beside the MAIN checkout, not beside the
worktree. ``GENIZAH_PRIVATE_DIR`` overrides the folder.

Resolving a path never touches the disk beyond reading this checkout's ``.git`` file, so a
module can compute its defaults at import time. A script that is about to read or write there
calls ``require_dir`` first and stops with a clear message when the folder is absent, rather
than failing on a missing file or creating a stray folder.
"""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PRIVATE_DIR_NAME = "GenizahSearch-private"


def _main_checkout(root: Path) -> Path:
    """The main checkout for ``root``: itself, or, in a worktree, the checkout it belongs to.

    A worktree's ``.git`` is a file holding ``gitdir: <main>/.git/worktrees/<name>``.
    """
    git = root / ".git"
    try:
        if git.is_file():
            line = git.read_text(encoding="utf-8").strip()
            if line.startswith("gitdir:"):
                gitdir = Path(line[len("gitdir:"):].strip())
                if not gitdir.is_absolute():
                    gitdir = (root / gitdir).resolve()
                if gitdir.parent.name == "worktrees":
                    return gitdir.parent.parent.parent
    except OSError:
        pass
    return root


def private_dir() -> Path:
    """The private repository's checkout (it may not exist)."""
    env = os.environ.get("GENIZAH_PRIVATE_DIR", "").strip()
    if env:
        return Path(env)
    return _main_checkout(REPO_ROOT).parent / PRIVATE_DIR_NAME


def planning_dir() -> Path:
    """The planning record's folder (it may not exist)."""
    return private_dir() / ".planning"


def require_dir(path, what: str = "this folder") -> Path:
    """Return ``path`` when it is an existing folder; otherwise stop with a clear message."""
    p = Path(path)
    if not p.is_dir():
        raise SystemExit(
            f"{what} is not there: {p}\n"
            f"Check out the private repository beside this one as {PRIVATE_DIR_NAME}, "
            "or set GENIZAH_PRIVATE_DIR, or pass the path explicitly."
        )
    return p

