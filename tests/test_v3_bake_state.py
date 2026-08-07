"""Durability tests for the discovery-v3 bake's resumable step ledger.

The point of these tests is NOT that the happy path works -- it is that the
crash-safety claims are real. This project has a measured history of checks that
reported success without performing their check, so every guarantee below is
paired with a demonstration that violating it FAILS:

* atomicity is tested by killing a real child process mid-write (not by mocking
  `os.replace`, which would only test that the mock was called);
* resumption is tested by asserting the expensive work does not re-run;
* the retry-an-interrupted-step guarantee is tested by proving a raising step is
  NOT recorded done;
* the fail-closed reads are tested against a truncated file and a foreign run id.
"""
from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from v3_bake_state import BakeState  # noqa: E402


def test_a_finished_step_is_skipped_on_a_rerun(tmp_path):
    calls = []
    state = BakeState(tmp_path / "s.json", run_id="r1")
    assert state.run_step("ingest", lambda: calls.append(1) or "v1") == "v1"

    resumed = BakeState(tmp_path / "s.json", run_id="r1")
    assert resumed.is_done("ingest")
    # The whole point: the expensive body must NOT run again.
    assert resumed.run_step("ingest", lambda: calls.append(2) or "v2") == "v1"
    assert calls == [1], "a completed step re-ran on resume"


def test_an_interrupted_step_is_retried_not_skipped(tmp_path):
    """A step that raises must NOT be recorded done."""
    state = BakeState(tmp_path / "s.json", run_id="r1")

    def boom():
        raise RuntimeError("killed mid-step")

    with pytest.raises(RuntimeError):
        state.run_step("novelty", boom)

    resumed = BakeState(tmp_path / "s.json", run_id="r1")
    assert not resumed.is_done("novelty"), "a failed step was recorded as done"
    assert resumed.run_step("novelty", lambda: "ok") == "ok"


def test_a_concurrent_reader_never_observes_a_partial_state_file(tmp_path):
    """Atomicity, tested where it is actually observable.

    A first draft of this test killed a spinning child and checked the survivor
    parsed -- and a MUTATION to the naive `open(path,'w')` writer PASSED it, so
    the test was vacuous: the kill almost never lands inside the write window,
    and `mark_done`'s payload was too small for that window to matter.

    This version removes the timing luck. A child rewrites a LARGE state file in
    a loop while the parent reads the same path hundreds of times. A naive writer
    truncates the file at `open()` and then spends most of its wall-clock inside
    `write()`, so a concurrent reader observes invalid JSON almost immediately.
    An atomic writer publishes only via `os.replace`, so **every** observation is
    a complete document. Verified to fail against the naive writer (mutation
    test, 2026-08-06) -- which is what makes the pass here mean something.
    """
    state_file = tmp_path / "s.json"
    script = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {str(Path(__file__).resolve().parents[1] / "scripts")!r})
        from v3_bake_state import BakeState
        st = BakeState({str(state_file)!r}, run_id="kill")
        big = "x" * 400_000          # wide write window: ~0.4 MB per rewrite
        i = 0
        while True:
            i += 1
            st.mark_done("step%d" % (i % 8), {{"payload": big}})
        """
    )
    child = tmp_path / "spin.py"
    child.write_text(script, encoding="utf-8")
    proc = subprocess.Popen([sys.executable, str(child)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    observations = 0
    partials = []
    try:
        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline and observations < 400:
            try:
                raw = state_file.read_text(encoding="utf-8")
            except OSError:
                # A sharing violation is the OS refusing a torn read, not a torn
                # read -- not evidence either way, so it is not counted.
                continue
            if not raw:
                partials.append("empty file")
                observations += 1
                continue
            observations += 1
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                partials.append(f"invalid JSON: {exc}")
                continue
            if data.get("run_id") != "kill" or not isinstance(data.get("steps"), dict):
                partials.append("parsed but structurally wrong")
        proc.kill()  # uncooperative kill, mid-write by construction
    finally:
        proc.wait(timeout=30)

    assert observations >= 20, (
        f"only {observations} observations -- the child never got going, so this "
        f"test proved nothing"
    )
    assert not partials, (
        f"{len(partials)} of {observations} reads saw a partial/invalid state file "
        f"(first: {partials[0]}) -- the writer is not atomic"
    )
    # A hard kill CAN leave a temp file behind -- that is the atomic pattern
    # working as intended (orphan a temp rather than corrupt the real file), not
    # a defect. An earlier version of this test asserted no temp survived, which
    # was simply wrong and failed on CI's Linux runner while passing on Windows,
    # where the kill happened to land outside the window more often. What IS
    # owed is that the orphans get reaped, so a repeatedly-killed unattended run
    # does not fill its directory: construction sweeps them.
    BakeState(state_file, run_id="kill")   # the survivor must still be loadable
    assert not list(tmp_path.glob("s.json.*.tmp")), (
        "stale temp files survived a resume -- _sweep_stale_temps did not run"
    )


def test_a_resume_reaps_temp_files_orphaned_by_a_killed_write(tmp_path):
    """Directly: the sweep runs, and it is scoped to THIS state file's temps.

    Written after CI caught the wrong assertion above. A kill-based test cannot
    reliably create an orphan (that is why the original assertion was wrong on
    one platform and not the other), so the orphan is planted instead.
    """
    path = tmp_path / "s.json"
    BakeState(path, run_id="r1").mark_done("a")
    orphan_a = tmp_path / "s.json.deadbeef.tmp"
    orphan_b = tmp_path / "s.json.cafe1234.tmp"
    orphan_a.write_text("half-written", encoding="utf-8")
    orphan_b.write_text("half-written", encoding="utf-8")
    # Must NOT be swept: a different state file's temp, and an unrelated file.
    other = tmp_path / "other.json.abc.tmp"
    other.write_text("someone else's", encoding="utf-8")
    unrelated = tmp_path / "s.json.backup"
    unrelated.write_text("keep me", encoding="utf-8")

    BakeState(path, run_id="r1")            # a resume

    assert not orphan_a.exists() and not orphan_b.exists(), "orphans were not reaped"
    assert other.exists(), "swept another state file's temp -- too broad"
    assert unrelated.exists(), "swept a non-temp file -- too broad"
    assert path.exists(), "swept the real state file"


def test_a_non_atomic_writer_would_fail_this_suite(tmp_path):
    """Control: prove the atomicity assertion above can actually fail.

    Truncation is exactly what a plain `open(path,'w')` leaves behind when the
    process dies mid-write. If BakeState tolerated that, the test above would be
    vacuous -- so assert the truncated file is REJECTED.
    """
    path = tmp_path / "s.json"
    BakeState(path, run_id="r1").mark_done("a")
    good = path.read_text(encoding="utf-8")
    path.write_text(good[: len(good) // 2], encoding="utf-8")  # simulate truncation
    with pytest.raises(RuntimeError, match="unreadable"):
        BakeState(path, run_id="r1")


def test_a_foreign_runs_state_is_refused(tmp_path):
    path = tmp_path / "s.json"
    BakeState(path, run_id="run-A").mark_done("a")
    with pytest.raises(RuntimeError, match="belongs to run"):
        BakeState(path, run_id="run-B")


def test_force_reruns_a_done_step(tmp_path):
    calls = []
    state = BakeState(tmp_path / "s.json", run_id="r1")
    state.run_step("x", lambda: calls.append(1))
    state.run_step("x", lambda: calls.append(2), force=True)
    assert calls == [1, 2]
