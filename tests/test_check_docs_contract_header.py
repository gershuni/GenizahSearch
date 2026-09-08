"""Tests for scripts/check_docs.py::check_contract_header_dates.

This is a BLOCKING gate: it fails the build when a contract doc's header date
is older than git's own last-commit date for that file. It exists because
docs/SEARCH_API.md carried "Last updated: 2026-05-05" for four months while its
body was edited through 2026-08-26, and an external integrator read that header,
reasonably concluded the newer sections were aspirational, and shipped a client
that rejected four live features.

Two things therefore need locking down, and the second is the subtle one:

1. The gate fires on real drift and stays quiet when the header is current.
2. When the gate CANNOT run -- no git binary, an untracked file, or (the case
   that actually bites) a shallow clone whose one fetched commit did not touch
   the file -- it must SAY SO rather than return silently. A gate that quietly
   cannot run is indistinguishable from a gate that passed, which is exactly the
   failure mode it was built to end. CI's lint-and-docs job checks out with
   fetch-depth: 0 for this reason; these tests are what keep that coupling
   honest if someone removes it.
"""
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
CHECK_DOCS = REPO_ROOT / "scripts" / "check_docs.py"


def _load_check_docs():
    """Import scripts/check_docs.py as a module (it is a script, not a package)."""
    spec = importlib.util.spec_from_file_location("_check_docs_under_test", CHECK_DOCS)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def check_docs():
    return _load_check_docs()


class _FakeCompleted:
    def __init__(self, stdout="", stderr="", returncode=0):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


def _stub_git(monkeypatch, module, *, stdout="", stderr="", returncode=0, raises=None,
              shallow="false", probe_returncode=0):
    """Control only what git ANSWERS. The function under test is the real one.

    Two distinct git calls are made, so the stub dispatches on argv rather than
    answering both the same way:

    - ``git rev-parse --is-shallow-repository`` -> ``shallow`` / ``probe_returncode``
    - ``git log -1 --format=%cd -- <path>``     -> ``stdout`` / ``returncode``

    Defaulting ``shallow`` to "false" keeps every date-comparison test exercising
    the real comparison rather than short-circuiting at the shallowness guard.
    """
    def fake_run(args, *rest, **kwargs):
        if raises is not None:
            raise raises
        if "--is-shallow-repository" in args:
            return _FakeCompleted(stdout=shallow + "\n", returncode=probe_returncode)
        return _FakeCompleted(stdout=stdout, stderr=stderr, returncode=returncode)

    monkeypatch.setattr(module.subprocess, "run", fake_run)


# ---------------------------------------------------------------------------
# The gate's own contract
# ---------------------------------------------------------------------------

def test_contract_docs_names_the_public_api_doc(check_docs):
    """The public API contract is the doc whose header is a published claim."""
    assert "docs/SEARCH_API.md" in check_docs.CONTRACT_DOCS


def test_returns_two_lists(check_docs):
    """(failures, unrunnable) -- callers must be able to treat them differently:
    the first is blocking, the second is a reported inability to check."""
    result = check_docs.check_contract_header_dates()
    assert isinstance(result, tuple) and len(result) == 2
    failures, unrunnable = result
    assert isinstance(failures, list) and isinstance(unrunnable, list)


def _repo_is_shallow() -> bool:
    """Whether THIS checkout has real per-file history to consult.

    Only CI's lint-and-docs job checks out with fetch-depth: 0 -- the `tests`
    jobs are deliberately shallow, because full history costs every one of them
    and the gate is enforced once, in lint-and-docs. So a test that needs real
    history has to say so rather than assume it.
    """
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return True
    return proc.returncode != 0 or (proc.stdout or "").strip() == "true"


@pytest.mark.skipif(
    _repo_is_shallow(),
    reason="shallow checkout: no per-file history, so the gate cannot run here "
           "(by design -- only CI's lint-and-docs job fetches full history). "
           "test_ci_checks_out_full_history_for_the_docs_job guards that job.",
)
def test_real_repo_is_currently_clean(check_docs):
    """Against the working tree as committed, the gate passes and can run.

    If this goes red, the header of a contract doc has drifted behind its own
    last commit -- the gate doing its job, not a broken test.

    Skipped on a shallow checkout rather than relaxed. Asserting `unrunnable ==
    []` in a job that deliberately has no history is a demand on the
    ENVIRONMENT, not on the code, and this test failed in exactly that way on
    `tests (ubuntu-latest)` before the skip existed. The strict assertion still
    runs wherever history is present -- locally, and in lint-and-docs, which is
    the job that actually enforces the gate.
    """
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == [], f"contract-doc header drift: {failures}"
    assert unrunnable == [], (
        f"history is available here, so the gate must have run: {unrunnable}"
    )


# ---------------------------------------------------------------------------
# It fires on real drift
# ---------------------------------------------------------------------------

def test_fires_when_commit_is_newer_than_header(monkeypatch, check_docs):
    """The exact historical shape of the bug: header 2026-05-05, body committed
    2026-08-26. This is the assertion that makes the gate worth having."""
    _stub_git(monkeypatch, check_docs, stdout="2026-08-26\n")

    doc = check_docs.ROOT_DIR / "docs" / "SEARCH_API.md"
    # BYTES, not text. read_text() collapses CRLF to LF, so restoring via
    # write_text() would silently rewrite this CRLF file as LF -- a whole-file
    # diff that `git diff` hides behind an autocrlf notice. Round-trip the raw
    # bytes so the artifact comes back byte-identical.
    original = doc.read_bytes()
    current = check_docs.LAST_UPDATED_RE.search(original.decode("utf-8"))
    assert current, "docs/SEARCH_API.md must carry a parsable Last updated date"

    doctored = original.replace(current.group(1).encode(), b"2026-05-05", 1)
    assert doctored != original, "the doctoring edit did not land"
    try:
        doc.write_bytes(doctored)
        failures, unrunnable = check_docs.check_contract_header_dates()
    finally:
        doc.write_bytes(original)
    assert doc.read_bytes() == original, "restore failed -- artifact left modified"

    assert unrunnable == []
    assert len(failures) == 1, failures
    assert "2026-05-05" in failures[0] and "2026-08-26" in failures[0]


def test_quiet_when_header_is_newer_than_the_last_commit(monkeypatch, check_docs):
    """Mid-edit is not a failure: an uncommitted header bump is ahead of git."""
    _stub_git(monkeypatch, check_docs, stdout="2026-01-01\n")
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == [] and unrunnable == []


def test_quiet_when_header_equals_the_commit_date(monkeypatch, check_docs):
    """Bumping the header in the same commit as the body is the correct flow and
    must not trip the gate (a strict > comparison, not >=)."""
    doc = check_docs.ROOT_DIR / "docs" / "SEARCH_API.md"
    header = check_docs.LAST_UPDATED_RE.search(doc.read_text(encoding="utf-8"))
    _stub_git(monkeypatch, check_docs, stdout=header.group(1) + "\n")
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == [] and unrunnable == []


# ---------------------------------------------------------------------------
# It refuses to be silent when it cannot run  (the shallow-clone class)
# ---------------------------------------------------------------------------

def test_shallow_clone_is_refused_not_answered(monkeypatch, check_docs):
    """A shallow clone must be DECLINED, not answered.

    Measured in a real `git clone --depth 1`: `git log -1 -- <path>` returns
    HEAD's own commit date for every file, because the grafted root commit looks
    like it added the whole tree. Full history vs depth 1, same four files:

        docs/SEARCH_API.md   2026-09-08  ->  2026-09-08
        README.md            2026-09-06  ->  2026-09-08
        version.py           2026-09-06  ->  2026-09-08
        docs/CODE_INDEX.md   2026-08-27  ->  2026-09-08

    So shallowness does not make the gate skip -- it makes it compare against the
    WRONG commit, and any commit dated after a header would then fail the build
    for a doc nobody touched. `stdout` here is deliberately a date that WOULD
    trip the comparison, proving the guard short-circuits before reaching it.
    """
    _stub_git(monkeypatch, check_docs, shallow="true", stdout="2099-01-01\n")
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == [], (
        "a shallow clone must never produce a build failure -- that is the "
        "false-positive this guard exists to prevent"
    )
    assert len(unrunnable) == len(check_docs.CONTRACT_DOCS)
    assert "did NOT run" in unrunnable[0]
    assert "SHALLOW" in unrunnable[0]
    assert "fetch-depth: 0" in unrunnable[0]


def test_untracked_file_is_reported_not_skipped(monkeypatch, check_docs):
    """Not shallow, but git knows no commit for the file: it is untracked."""
    _stub_git(monkeypatch, check_docs, stdout="")
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == [], "an un-runnable check must not be reported as drift"
    assert len(unrunnable) == len(check_docs.CONTRACT_DOCS)
    assert "did NOT run" in unrunnable[0]
    assert "untracked" in unrunnable[0]


def test_no_repository_is_reported_not_skipped(monkeypatch, check_docs):
    """A source tarball with no .git must report, not fail and not skip."""
    _stub_git(monkeypatch, check_docs, shallow="", probe_returncode=128)
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == []
    assert len(unrunnable) == len(check_docs.CONTRACT_DOCS)
    assert "did NOT run" in unrunnable[0]
    assert "repository" in unrunnable[0]


def test_missing_git_binary_is_reported_not_skipped(monkeypatch, check_docs):
    _stub_git(monkeypatch, check_docs, raises=OSError("git not found"))
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == []
    assert len(unrunnable) == len(check_docs.CONTRACT_DOCS)
    assert "git" in unrunnable[0].lower()


def test_git_log_failure_is_reported_not_skipped(monkeypatch, check_docs):
    _stub_git(
        monkeypatch, check_docs,
        stdout="", stderr="fatal: not a git repository", returncode=128,
    )
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == []
    assert len(unrunnable) == len(check_docs.CONTRACT_DOCS)
    assert "git log failed" in unrunnable[0]


def test_unparsable_commit_date_is_reported_not_skipped(monkeypatch, check_docs):
    _stub_git(monkeypatch, check_docs, stdout="not-a-date\n")
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == []
    assert len(unrunnable) == len(check_docs.CONTRACT_DOCS)
    assert "unparsable" in unrunnable[0]


# ---------------------------------------------------------------------------
# The CI coupling this gate depends on
# ---------------------------------------------------------------------------

def test_ci_checks_out_full_history_for_the_docs_job():
    """The gate is only real in CI if lint-and-docs has git history.

    Asserted against the workflow source because there is no other way to catch
    someone dropping fetch-depth: a depth-1 clone makes the gate pass silently,
    so its removal produces a GREEN build, not a red one.
    """
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    lint_job = ci[ci.index("lint-and-docs:"):]
    lint_job = lint_job[:lint_job.index("\n  tests:")]
    assert "fetch-depth: 0" in lint_job, (
        "lint-and-docs must check out with fetch-depth: 0, or "
        "check_contract_header_dates() silently cannot run in CI"
    )
    assert "check_docs.py" in lint_job, (
        "this test guards the checkout depth for check_docs.py; if the docs "
        "check moved to another job, move this assertion with it"
    )


def test_git_is_actually_available_here(check_docs):
    """Sanity: the un-runnable tests above stub git out, so prove the real thing
    works here -- otherwise test_real_repo_is_currently_clean could pass for the
    wrong reason.

    Runs on a shallow checkout too, and passes there: a shallow clone DOES
    answer this query, it just answers it wrongly (HEAD's date for every file),
    which is the whole reason `check_contract_header_dates` refuses to use the
    answer. This asserts only that git is reachable and responding.
    """
    proc = subprocess.run(
        ["git", "log", "-1", "--date=short", "--format=%cd", "--", "docs/SEARCH_API.md"],
        cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=20,
    )
    assert proc.returncode == 0 and proc.stdout.strip(), (
        "git could not report a commit date for docs/SEARCH_API.md in this "
        f"checkout (stdout={proc.stdout!r} stderr={proc.stderr!r})"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
