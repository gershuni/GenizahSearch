"""deploy.sh deploys EXACTLY the commit it is given, and only one already on origin/<branch>.

Behavioural: the real deploy.sh runs in a throwaway "server" clone with stub sudo/systemctl/pip on PATH,
against a bare "origin". Nothing here touches a real server.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _bash():
    """Git's own bash on Windows (the first `bash` on PATH may be WSL's, which cannot read Windows paths)."""
    if os.name == "nt":
        git_exe = shutil.which("git")
        if git_exe:
            for cand in (Path(git_exe).parent.parent / "bin" / "bash.exe",
                         Path(git_exe).parent.parent.parent / "bin" / "bash.exe"):
                if cand.is_file():
                    return str(cand)
        return None
    return shutil.which("bash")


BASH = _bash()
pytestmark = pytest.mark.skipif(BASH is None, reason="needs bash")

SHIMS = {
    "sudo": '#!/bin/sh\nexec "$@"\n',
    "systemctl": '#!/bin/sh\necho "$@" >> "$SHIM_LOG"\n'
                 'if [ "$1" = "restart" ] && [ -n "$RESTART_FAILS" ]; then exit 1; fi\n'
                 'if [ "$1" = "status" ]; then echo "Active: active (running)"; fi\nexit 0\n',
    "pip": "#!/bin/sh\nexit 0\n",
    # Git's bash on Windows has no flock; the server (util-linux) does. The stand-in answers "taken" on demand.
    "flock": '#!/bin/sh\nexit ${LOCK_TAKEN:-0}\n',
}


def git(cwd, *args):
    env = {**os.environ, "GIT_AUTHOR_NAME": "T", "GIT_AUTHOR_EMAIL": "t@example.org",
           "GIT_COMMITTER_NAME": "T", "GIT_COMMITTER_EMAIL": "t@example.org"}
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True,
                          env=env).stdout.strip()


def commit(repo: Path, name: str) -> str:
    (repo / name).write_text(name + "\n", encoding="utf-8")
    git(repo, "add", name)
    git(repo, "commit", "-q", "-m", name)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def server(tmp_path):
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "master-main", str(origin)], check=True)
    seed = tmp_path / "seed"
    seed.mkdir()
    git(seed, "init", "-q", "-b", "master-main")
    a = commit(seed, "a.txt")
    git(seed, "push", "-q", str(origin), "master-main")
    work = tmp_path / "server"
    subprocess.run(["git", "clone", "-q", str(origin), str(work)], check=True)
    b = commit(seed, "b.txt")
    git(seed, "push", "-q", str(origin), "master-main")
    unpublished = commit(seed, "c.txt")  # exists nowhere the server can fetch
    shutil.copy2(REPO_ROOT / "deploy.sh", work / "deploy.sh")
    (work / "venv" / "bin").mkdir(parents=True)
    (work / "venv" / "bin" / "activate").write_text("", encoding="utf-8")
    shims = tmp_path / "shims"
    shims.mkdir()
    for name, body in SHIMS.items():
        (shims / name).write_text(body, encoding="utf-8", newline="\n")
        (shims / name).chmod(0o755)
    log = tmp_path / "systemctl.log"
    home = tmp_path / "home"
    home.mkdir()
    env = {**os.environ, "PATH": f"{shims}{os.pathsep}{os.environ['PATH']}", "SHIM_LOG": str(log),
           "HOME": str(home)}
    return {"work": work, "a": a, "b": b, "unpublished": unpublished, "env": env, "log": log}


def deploy(s, *args):
    p = subprocess.run([BASH, "deploy.sh", *args], cwd=s["work"], capture_output=True, text=True, env=s["env"])
    return p.returncode, p.stdout + p.stderr


def head(s):
    return git(s["work"], "rev-parse", "HEAD")


def restarted(s):
    return s["log"].exists() and "restart genizah-web" in s["log"].read_text(encoding="utf-8")


@pytest.mark.parametrize("args", [(), ("master-main",), ("master-main", "abc"),
                                  ("master-main", "A" * 40), ("master-main", "f" * 39 + "; reboot"),
                                  ("-x", "a" * 40), ("a:b", "a" * 40)])
def test_without_a_valid_id_nothing_changes(server, args):
    before = head(server)
    rc, out = deploy(server, *args)
    assert rc == 2, out
    assert head(server) == before and not restarted(server)


def test_an_unpublished_commit_is_refused(server):
    before = head(server)
    rc, out = deploy(server, "master-main", server["unpublished"])
    assert rc == 1, out
    assert head(server) == before and not restarted(server)


def test_the_given_commit_is_deployed_even_when_the_branch_moved_on(server):
    rc, out = deploy(server, "master-main", server["a"])  # origin/master-main is already at b
    assert rc == 0, out
    assert head(server) == server["a"]
    assert (server["work"] / ".deployed_revision").read_text(encoding="utf-8").strip() == server["a"]
    assert restarted(server)


def test_the_branch_tip_deploys_when_it_is_the_given_commit(server):
    rc, out = deploy(server, "master-main", server["b"])
    assert rc == 0, out
    assert head(server) == server["b"]


def test_a_second_deploy_while_one_runs_changes_nothing(server):
    before = head(server)
    rc, out = deploy({**server, "env": {**server["env"], "LOCK_TAKEN": "1"}}, "master-main", server["a"])
    assert rc == 3, out
    assert head(server) == before and not restarted(server)


def test_a_local_tag_named_like_the_remote_branch_is_not_trusted(server):
    work = server["work"]
    local = commit(work, "local.txt")  # the server has it, origin does not
    git(work, "reset", "-q", "--hard", "HEAD~1")
    git(work, "tag", "origin/master-main", local)
    before = head(server)
    rc, out = deploy(server, "master-main", local)
    assert rc == 1, out
    assert head(server) == before and not restarted(server)


def test_a_failed_restart_leaves_no_running_revision_record(server):
    assert deploy(server, "master-main", server["a"])[0] == 0
    assert (server["work"] / ".deployed_revision").exists()
    rc, out = deploy({**server, "env": {**server["env"], "RESTART_FAILS": "1"}}, "master-main", server["b"])
    assert rc != 0, out
    assert not (server["work"] / ".deployed_revision").exists()
