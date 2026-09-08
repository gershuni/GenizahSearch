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


def _stub_git(monkeypatch, module, *, stdout="", stderr="", returncode=0, raises=None):
    """Control only what git ANSWERS. The function under test is the real one."""
    def fake_run(*args, **kwargs):
        if raises is not None:
            raise raises
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


def test_real_repo_is_currently_clean(check_docs):
    """Against the working tree as committed, the gate passes and can run.

    If this ever goes red, the header of a contract doc has drifted behind its
    own last commit -- which is the gate doing its job, not a broken test.
    """
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == [], f"contract-doc header drift: {failures}"
    assert unrunnable == [], (
        f"the gate could not run, which CI must never accept silently: {unrunnable}"
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

def test_shallow_clone_is_reported_not_skipped(monkeypatch, check_docs):
    """A depth-1 clone answers "no commit touched this file" with empty stdout.

    Before this was reported, the gate no-opped in CI and looked green. The note
    must name the remedy, because whoever reads it is looking at a passing build.
    """
    _stub_git(monkeypatch, check_docs, stdout="")
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == [], "an un-runnable check must not be reported as drift"
    assert len(unrunnable) == len(check_docs.CONTRACT_DOCS)
    assert "did NOT run" in unrunnable[0]
    assert "fetch-depth: 0" in unrunnable[0]


def test_missing_git_binary_is_reported_not_skipped(monkeypatch, check_docs):
    _stub_git(monkeypatch, check_docs, raises=OSError("git not found"))
    failures, unrunnable = check_docs.check_contract_header_dates()
    assert failures == []
    assert len(unrunnable) == len(check_docs.CONTRACT_DOCS)
    assert "git" in unrunnable[0].lower()


def test_git_failure_is_reported_not_skipped(monkeypatch, check_docs):
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
    works in this environment -- otherwise test_real_repo_is_currently_clean
    would be passing for the wrong reason."""
    proc = subprocess.run(
        ["git", "log", "-1", "--date=short", "--format=%cd", "--", "docs/SEARCH_API.md"],
        cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=20,
    )
    assert proc.returncode == 0 and proc.stdout.strip(), (
        "git could not report a commit date for docs/SEARCH_API.md in this "
        f"checkout (stdout={proc.stdout!r} stderr={proc.stderr!r}) -- if this is "
        "a shallow clone, the gate cannot run here either"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
