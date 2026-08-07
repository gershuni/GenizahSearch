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
import os
import textwrap
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from v3_bake_state import BakeState  # noqa: E402


def test_a_finished_step_is_skipped_on_a_rerun(tmp_path):
    calls = []
    # `with`, because BakeState now holds a single-writer LOCK for its lifetime
    # (Codex round 2). A resume is a NEW process in production; in-test it has to
    # release first, which is exactly the discipline the lock is enforcing.
    with BakeState(tmp_path / "s.json", run_id="r1") as state:
        assert state.run_step("ingest", lambda: calls.append(1) or "v1") == "v1"

    resumed = BakeState(tmp_path / "s.json", run_id="r1")
    assert resumed.is_done("ingest")
    # The whole point: the expensive body must NOT run again.
    assert resumed.run_step("ingest", lambda: calls.append(2) or "v2") == "v1"
    assert calls == [1], "a completed step re-ran on resume"


def test_an_interrupted_step_is_retried_not_skipped(tmp_path):
    """A step that raises must NOT be recorded done."""
    def boom():
        raise RuntimeError("killed mid-step")

    with BakeState(tmp_path / "s.json", run_id="r1") as state:
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
    # The killed child held the single-writer LOCK (Codex round 2), and a hard
    # kill cannot release it -- so a resume must REFUSE until the lock is cleared
    # explicitly. That refusal is the intended behaviour, not a bug: the whole
    # point is that a second writer never silently joins in. Verify the refusal
    # names the lock, then clear it the way an operator would.
    lock_file = tmp_path / "s.json.lock"
    assert lock_file.exists(), "the killed writer left no lock -- it never took one"
    with pytest.raises(RuntimeError, match="already locked"):
        BakeState(state_file, run_id="kill")
    lock_file.unlink()

    # Now the survivor must still be loadable -- the real assertion about
    # atomicity surviving a hard kill.
    # A hard kill CAN leave a temp behind -- that is the atomic pattern working
    # (orphan a temp rather than corrupt the real file). Those orphans are YOUNG,
    # and the sweep now deliberately spares young temps, so asserting they are
    # gone here would assert against the age gate. Age them first, then resume:
    # this checks the reaping AND leaves the age gate's own test (see
    # `test_a_resume_reaps_temp_files_orphaned_by_a_killed_write`) as the place
    # the young-temp behaviour is pinned.
    stale_mtime = time.time() - BakeState._TEMP_MIN_AGE_SECONDS - 60
    leftovers = list(tmp_path.glob("s.json.*.tmp"))
    for leftover in leftovers:
        os.utime(leftover, (stale_mtime, stale_mtime))

    BakeState(state_file, run_id="kill")
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
    with BakeState(path, run_id="r1") as _st:
        _st.mark_done("a")
    orphan_a = tmp_path / "s.json.deadbeef.tmp"
    orphan_b = tmp_path / "s.json.cafe1234.tmp"
    orphan_a.write_text("half-written", encoding="utf-8")
    orphan_b.write_text("half-written", encoding="utf-8")
    # AGE them past `_TEMP_MIN_AGE_SECONDS`. The sweep now spares young temps
    # because unlinking a LIVE writer's temp breaks its `os.replace` (Codex round
    # 2), so a test that plants a fresh orphan is asserting the OPPOSITE of the
    # intended behaviour.
    stale_mtime = time.time() - BakeState._TEMP_MIN_AGE_SECONDS - 60
    for orphan in (orphan_a, orphan_b):
        os.utime(orphan, (stale_mtime, stale_mtime))
    # Must NOT be swept: a different state file's temp, and an unrelated file.
    other = tmp_path / "other.json.abc.tmp"
    other.write_text("someone else's", encoding="utf-8")
    os.utime(other, (stale_mtime, stale_mtime))
    unrelated = tmp_path / "s.json.backup"
    unrelated.write_text("keep me", encoding="utf-8")
    os.utime(unrelated, (stale_mtime, stale_mtime))
    # A YOUNG temp for this same state file: may belong to a live writer, so it
    # must survive even though its name matches.
    young = tmp_path / "s.json.fresh0001.tmp"
    young.write_text("possibly live", encoding="utf-8")

    BakeState(path, run_id="r1")            # a resume

    assert not orphan_a.exists() and not orphan_b.exists(), "orphans were not reaped"
    assert other.exists(), "swept another state file's temp -- too broad"
    assert unrelated.exists(), "swept a non-temp file -- too broad"
    assert path.exists(), "swept the real state file"
    assert young.exists(), (
        "swept a temp file young enough to belong to a LIVE writer -- that turns "
        "litter-collection into another process's write failure"
    )


def test_a_non_atomic_writer_would_fail_this_suite(tmp_path):
    """Control: prove the atomicity assertion above can actually fail.

    Truncation is exactly what a plain `open(path,'w')` leaves behind when the
    process dies mid-write. If BakeState tolerated that, the test above would be
    vacuous -- so assert the truncated file is REJECTED.
    """
    path = tmp_path / "s.json"
    with BakeState(path, run_id="r1") as _st:
        _st.mark_done("a")
    good = path.read_text(encoding="utf-8")
    path.write_text(good[: len(good) // 2], encoding="utf-8")  # simulate truncation
    with pytest.raises(RuntimeError, match="unreadable"):
        BakeState(path, run_id="r1")


def test_a_foreign_runs_state_is_refused(tmp_path):
    path = tmp_path / "s.json"
    with BakeState(path, run_id="run-A") as _st:
        _st.mark_done("a")
    with pytest.raises(RuntimeError, match="belongs to run"):
        BakeState(path, run_id="run-B")


def test_force_reruns_a_done_step(tmp_path):
    calls = []
    state = BakeState(tmp_path / "s.json", run_id="r1")
    state.run_step("x", lambda: calls.append(1))
    state.run_step("x", lambda: calls.append(2), force=True)
    assert calls == [1, 2]


# ---------------------------------------------------------------------------
# Codex round 2 (MEDIUM): "the state ledger is safe for one writer, not for two
# processes sharing it. It has no writer lock or compare-and-swap: two instances
# can load the same JSON, independently add different completed steps, and the
# later replace loses the other step."
#
# The finding was correct. Atomic replace protects a READER from a torn write; it
# does nothing about two writers. These pin the lock.
# ---------------------------------------------------------------------------

def test_a_second_writer_is_refused_loudly(tmp_path):
    """The lost-update the lock prevents, and the refusal that replaces it."""
    path = tmp_path / "s.json"
    first = BakeState(path, run_id="r1")
    try:
        with pytest.raises(RuntimeError, match="already locked"):
            BakeState(path, run_id="r1")
    finally:
        first.release()
    # Released -> a legitimate resume works.
    BakeState(path, run_id="r1").release()


def test_the_refusal_names_the_holder_and_the_remedy(tmp_path):
    """An unattended bake that halts must say what to do.

    A lock is only better than a lost update if the operator can act on it: the
    message has to identify the holder and name the file to remove.
    """
    path = tmp_path / "s.json"
    first = BakeState(path, run_id="r1")
    try:
        with pytest.raises(RuntimeError) as exc:
            BakeState(path, run_id="r1")
    finally:
        first.release()
    message = str(exc.value)
    assert f"pid={os.getpid()}" in message, "the refusal does not identify the holder"
    # The remedy is now a NAMED RECOVERY COMMAND rather than "delete this file"
    # (Codex R3): a hard kill cannot release the lock, so recovery has to be
    # something an operator can run, and it must refuse a live holder rather than
    # letting an operator barge in with rm.
    assert "--release-stale-lock" in message, (
        "the refusal does not name the recovery command"
    )
    assert "s.json" in message, "the refusal does not name the state file"


def test_a_lost_update_is_what_the_lock_prevents(tmp_path):
    """Demonstrate the defect, so the lock is not merely asserted to be useful.

    Two ledgers over one file, WITHOUT the lock (simulated by releasing it), each
    record a different step. The second write wins and the first step is gone --
    which is precisely the state a resume would then trust.
    """
    path = tmp_path / "s.json"
    a = BakeState(path, run_id="r1")
    a.release()                     # drop the lock to simulate the pre-fix world
    b = BakeState(path, run_id="r1")
    b.release()
    a.mark_done("step_a")
    b.mark_done("step_b")           # b's snapshot predates step_a
    survivor = BakeState(path, run_id="r1")
    try:
        assert survivor.is_done("step_b")
        assert not survivor.is_done("step_a"), (
            "the lost-update scenario no longer reproduces -- if BakeState now "
            "merges concurrent writers, this test's premise is obsolete and the "
            "lock may be unnecessary"
        )
    finally:
        survivor.release()


def test_release_is_idempotent_and_safe_in_a_finally(tmp_path):
    """A detached run's teardown must not raise from cleanup."""
    state = BakeState(tmp_path / "s.json", run_id="r1")
    state.release()
    state.release()          # must not raise
    with BakeState(tmp_path / "s.json", run_id="r1"):
        pass                 # __exit__ releases; a second release inside is fine
    BakeState(tmp_path / "s.json", run_id="r1").release()


# ---------------------------------------------------------------------------
# Codex ROUND 3 (MEDIUM): the lock is correct against concurrent writers, but a
# hard kill cannot release it -- "it converts the documented unattended resume
# path into a stuck run after exactly the failure mode this ledger is intended to
# survive". A named recovery command, gated on holder liveness.
# ---------------------------------------------------------------------------

def test_a_dead_holders_lock_can_be_released_deliberately(tmp_path):
    from v3_bake_state import release_stale_lock

    path = tmp_path / "s.json"
    BakeState(path, run_id="r1").release()
    # Simulate a hard-killed writer: a lock naming a pid that is not running.
    lock = tmp_path / "s.json.lock"
    lock.write_text("pid=999999999 run_id=r1\n", encoding="utf-8")

    result = release_stale_lock(path)
    assert result["released"] is True
    assert result["holder_was_alive"] is False
    assert not lock.exists()
    # ...and the bake resumes.
    BakeState(path, run_id="r1").release()


def test_a_LIVE_holders_lock_is_refused(tmp_path):
    """The property that makes the command safe: it cannot be used to barge in.

    Without this the recovery command would just be `rm` with extra steps, and
    would reintroduce the lost-update the lock exists to prevent.
    """
    from v3_bake_state import LockHeldError, release_stale_lock

    path = tmp_path / "s.json"
    state = BakeState(path, run_id="r1")     # THIS process holds it, and is alive
    try:
        with pytest.raises(LockHeldError, match="STILL RUNNING"):
            release_stale_lock(path)
        assert (tmp_path / "s.json.lock").exists(), "the live lock was removed anyway"
        # `--force` claims the pid was REUSED, i.e. the holder is not really this
        # bake. When the holder is genuinely live and in this process, Windows
        # refuses the unlink because the handle is open -- so force still fails,
        # honestly, rather than reporting a release that did not happen. On POSIX
        # the unlink succeeds (an open file can be unlinked), which is the
        # documented reused-pid escape. Both outcomes are correct; asserting the
        # platform-specific one would make this test a platform check.
        try:
            forced = release_stale_lock(path, force=True)
            assert forced["released"] is True
        except LockHeldError as exc:
            assert "still open by a live handle" in str(exc)
    finally:
        state.release()


def test_releasing_a_nonexistent_lock_is_a_no_op(tmp_path):
    """An unattended recovery script must be safe to run unconditionally."""
    from v3_bake_state import release_stale_lock

    result = release_stale_lock(tmp_path / "never-existed.json")
    assert result["released"] is False


def test_the_recovery_cli_exists_and_refuses_a_live_holder(tmp_path):
    import v3_bake_state as vbs

    path = tmp_path / "s.json"
    state = BakeState(path, run_id="r1")
    try:
        assert vbs.main(["--release-stale-lock", str(path)]) == 1, (
            "the CLI released a live holder's lock"
        )
    finally:
        state.release()
    lock = tmp_path / "s.json.lock"
    lock.write_text("pid=999999999 run_id=r1\n", encoding="utf-8")
    assert vbs.main(["--release-stale-lock", str(path)]) == 0
    assert not lock.exists()
