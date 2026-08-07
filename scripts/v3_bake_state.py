"""Crash-safe, resumable step state for the discovery-v3 bake.

Why this exists: the v3 bake runs long, unattended and DETACHED (a Claude Code
child process dies with its session), and prior long runs in this project were
lost to restarts. So every step records its own completion durably and is
skipped on a re-run -- the driver can be killed at any instant, including
mid-write, and restarted without redoing finished work or double-applying a
finished one.

Durability contract (the part that actually matters on Windows):

* One JSON file, rewritten via **write-temp -> os.replace()**. `os.replace` is
  atomic on Windows and POSIX, so a crash leaves either the old complete file
  or the new complete file -- never a truncated one. A plain `open(...,'w')`
  would leave a zero-byte state file if the machine died mid-write, silently
  resetting the whole run.
* `flush()` + `os.fsync()` before the replace, so the bytes are on the platter
  rather than in the OS cache when the atomic swap happens. Without the fsync,
  a power loss can land the rename while the data blocks are still buffered.
* A step is recorded ONLY after its work returns, so a killed step is retried
  rather than skipped. Steps must therefore be idempotent -- re-running one
  must be safe, which is why each writes to its own output path and never
  mutates a source artifact in place.

Deliberately NOT sqlite: the state is a handful of keys, and a lock-free plain
file cannot itself become the thing that fails (this project has already lost a
run to a Tantivy `LockBusy`, and SQLite on Windows has its own locking edge
cases under hard kill).

Masking (D-25): step names and messages are written by the caller. Callers must
keep restricted corpus names out of them -- refer to M-source / R-source only.
This module never reads corpus data.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional


class LockHeldError(RuntimeError):
    """Another process holds this state file's writer lock."""


class BakeState:
    """Resumable step ledger for one bake run."""

    # A temp file younger than this may belong to a LIVE writer, so the sweep
    # leaves it alone. Generous on purpose: a stale temp is litter, while
    # unlinking a live writer's temp makes its `os.replace` fail -- so the two
    # error directions are not symmetric and the check should err toward leaving
    # files behind.
    _TEMP_MIN_AGE_SECONDS = 300

    def __init__(self, path: str | os.PathLike, *, run_id: str) -> None:
        self.path = Path(path)
        self.run_id = run_id
        self._data: Dict[str, Any] = {"run_id": run_id, "steps": {}, "log": []}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # THE WRITER LOCK, before any read or write (Codex round 2, MEDIUM).
        # Round 2's finding was correct on both counts: two instances could load
        # the same JSON, add different completed steps, and the later `os.replace`
        # would lose the other's step -- atomic replace protects a reader from a
        # torn write, not two writers from each other. And the temp sweep could
        # unlink a live first writer's temp before its replace.
        #
        # A lock is the right answer rather than a compare-and-swap merge: two
        # concurrent bakes over one state directory is an operator mistake, not a
        # mode to support, and merging their step sets would report a run
        # complete that no single process ever ran. So: fail the second launcher
        # LOUDLY.
        self._lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self._lock_fd = self._acquire_lock()
        try:
            if self.path.exists():
                self._load()
            else:
                self._save()
            self._sweep_stale_temps()
        except BaseException:
            self.release()
            raise

    # ---------------- single-writer lock ----------------

    def _acquire_lock(self) -> int:
        """Take an exclusive lock for this process's lifetime, or fail loudly.

        `O_CREAT | O_EXCL` is the portable primitive: it succeeds for exactly one
        process. Deliberately NOT `fcntl`/`msvcrt` advisory locking -- those
        differ per platform, and this must behave identically on the Windows dev
        box and a Linux runner.

        A lock file left by a killed process is the awkward case. It is NOT
        auto-reaped on age: doing so would silently re-admit the very
        double-writer this prevents if the first process were merely slow. The
        error says exactly what to do instead, because an unattended bake that
        halts with a clear instruction is strictly better than one that
        corrupts its own ledger.
        """
        try:
            fd = os.open(str(self._lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            # Who holds it, so the refusal below can say something actionable.
            try:
                os.write(fd, f"pid={os.getpid()} run_id={self.run_id}\n".encode("utf-8"))
            except OSError:
                pass
            return fd
        except FileExistsError:
            holder = ""
            try:
                holder = self._lock_path.read_text(encoding="utf-8").strip()
            except OSError:
                pass
            raise LockHeldError(
                f"bake state is already locked by another process "
                f"({holder or 'unknown'}). Two writers over one state file lose each "
                f"other's completed steps, so this launcher refuses to start. "
                f"If no bake is actually running -- a hard kill (SIGKILL, power loss) "
                f"cannot release the lock -- recover DELIBERATELY with: "
                f"python scripts/v3_bake_state.py --release-stale-lock {self.path} "
                f"-- which checks the recorded holder is not a live process before "
                f"removing the lock, so it cannot barge in on a running bake."
            ) from None

    def release(self) -> None:
        """Release the lock. Idempotent, and safe to call from a `finally`."""
        fd = getattr(self, "_lock_fd", None)
        if fd is None:
            return
        self._lock_fd = None
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(self._lock_path)
        except OSError:
            pass

    def __enter__(self) -> "BakeState":
        return self

    def __exit__(self, *_exc) -> None:
        self.release()

    # ---------------- persistence ----------------

    def _load(self) -> None:
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            # A corrupt state file is NOT silently reset -- that would restart a
            # long run from zero while reporting success. Fail loudly and let a
            # human decide, per the project's fail-closed convention.
            raise RuntimeError(
                f"bake state file is unreadable ({exc}); refusing to silently "
                f"restart the run. Inspect or delete it explicitly: {self.path}"
            ) from exc
        if not isinstance(loaded, dict) or "steps" not in loaded:
            raise RuntimeError(f"bake state file has an unexpected shape: {self.path}")
        found = loaded.get("run_id")
        if found != self.run_id:
            raise RuntimeError(
                f"bake state file belongs to run {found!r}, not {self.run_id!r} -- "
                f"refusing to mix two runs' state: {self.path}"
            )
        self._data = loaded

    def _save(self) -> None:
        """Atomic + durable: temp file -> fsync -> os.replace."""
        payload = json.dumps(self._data, indent=1, ensure_ascii=False)
        fd, tmp = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=self.path.name + ".", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)  # atomic on Windows and POSIX
            # Codex round 2: fsync the PARENT DIRECTORY, or the durability claim
            # in this docstring is only half true. `os.fsync` on the file commits
            # its CONTENTS; the rename that publishes them lives in the
            # directory's own metadata, which can still be lost on power failure.
            # Best-effort: directory fds are not openable on Windows, and a
            # failure here means weaker durability, never a wrong state file.
            try:
                dir_fd = os.open(str(self.path.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except (OSError, AttributeError, ValueError):
                pass
        except BaseException:
            # Never leave a stray temp file behind on failure -- and never let
            # cleanup mask the original error.
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _sweep_stale_temps(self) -> None:
        """Remove temp files orphaned by a previously killed write.

        A hard kill (SIGKILL, power loss) between `mkstemp` and `os.replace`
        leaves the temp file behind BY DESIGN -- that is the cost of never
        corrupting the real state file, and it is the correct trade. But an
        unattended, resumable bake can be killed many times, so the orphans must
        be reaped rather than accumulated; otherwise a long run slowly fills the
        directory with dead 0.5 MB files.

        Swept on construction, i.e. exactly when a resume happens. Failure to
        unlink is ignored deliberately: a stale temp is litter, never a
        correctness problem, and must not prevent a resume.

        AGE-GATED (Codex round 2, MEDIUM). The first version unlinked every
        matching name, which on platforms permitting unlink of an open file could
        delete a LIVE writer's temp between its `mkstemp` and its `os.replace` --
        turning litter-collection into a write failure. The writer lock above
        makes a concurrent bake impossible, but this is defense in depth for the
        case where the lock file was manually removed, and the trade is trivially
        favourable: leaving a young orphan costs one dead file until the next
        resume, whereas unlinking a live temp breaks the write.
        """
        try:
            now = time.time()
            for stale in self.path.parent.glob(self.path.name + ".*.tmp"):
                try:
                    if now - stale.stat().st_mtime < self._TEMP_MIN_AGE_SECONDS:
                        continue          # may belong to a live writer
                    stale.unlink()
                except OSError:
                    pass
        except OSError:
            pass

    # ---------------- step ledger ----------------

    def is_done(self, step: str) -> bool:
        entry = self._data["steps"].get(step)
        return bool(entry and entry.get("status") == "done")

    def result(self, step: str) -> Optional[Any]:
        entry = self._data["steps"].get(step)
        return entry.get("result") if entry else None

    def mark_done(self, step: str, result: Any = None) -> None:
        self._data["steps"][step] = {
            "status": "done",
            # Wall-clock is recorded for operator triage only, never for logic.
            "finished": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "result": result,
        }
        self._save()

    def log(self, message: str) -> None:
        """Append an operator-facing line (also echoed to stdout)."""
        stamped = f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {message}"
        self._data["log"].append(stamped)
        self._save()
        print(stamped, flush=True)

    def run_step(self, step: str, fn: Callable[[], Any], *, force: bool = False) -> Any:
        """Run `fn` unless `step` is already recorded done.

        The step is recorded ONLY after `fn` returns, so an interrupted step is
        retried on the next launch. `fn` must therefore be idempotent.
        """
        if self.is_done(step) and not force:
            self.log(f"SKIP {step} (already done)")
            return self.result(step)
        self.log(f"START {step}")
        started = time.monotonic()
        value = fn()
        self.mark_done(step, value)
        self.log(f"DONE  {step} ({time.monotonic() - started:.1f}s)")
        return value

    # ---------------- reporting ----------------

    def summary(self) -> Dict[str, str]:
        return {k: v.get("status", "?") for k, v in self._data["steps"].items()}


def release_stale_lock(state_path, *, force: bool = False) -> Dict[str, Any]:
    """DELIBERATE recovery from a lock left by a hard-killed writer.

    Codex round 3 (MEDIUM): the lock is correct against concurrent writers, but a
    SIGKILL or power loss cannot release it, so recovery required an operator to
    know which file to delete -- turning the resumable unattended bake into a stuck
    run after exactly the failure the ledger exists to survive. That is a real
    operational cost, and the fix is a named command rather than a weaker lock.

    It is deliberately NOT automatic and NOT age-based. Both would re-admit the
    double-writer this prevents: a slow step can outlive any age threshold, and an
    auto-reap on startup is indistinguishable from no lock at all.

    Instead it checks LIVENESS of the recorded holder pid. A live holder is refused
    outright, so the command cannot be used to barge in on a running bake; a dead
    holder is released and reported. `force` exists for the case where the pid has
    been reused by an unrelated process, which liveness alone cannot distinguish --
    the operator asserts it, and the assertion is visible in the shell history.
    """
    lock_path = Path(str(state_path) + ".lock")
    if not lock_path.exists():
        return {"released": False, "reason": "no lock file present"}
    holder = ""
    try:
        holder = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    pid = None
    for token in holder.split():
        if token.startswith("pid="):
            try:
                pid = int(token.split("=", 1)[1])
            except ValueError:
                pid = None
    alive = False
    if pid is not None:
        try:
            # Signal 0 probes existence without delivering anything. On Windows
            # `os.kill` raises for a dead pid and returns for a live one, which is
            # the same signal we need; a PermissionError means the process exists
            # but is not ours, i.e. ALIVE -- so it must count as alive, not as an
            # error to swallow.
            os.kill(pid, 0)
            alive = True
        except PermissionError:
            alive = True
        except (OSError, ProcessLookupError):
            alive = False
    if alive and not force:
        raise LockHeldError(
            f"the lock holder ({holder or 'unknown'}) is STILL RUNNING. Refusing to "
            f"release it -- that would admit a second writer to a live bake and lose "
            f"one of their completed steps. Stop the process first, or pass --force if "
            f"you are certain the pid has been reused."
        )
    try:
        lock_path.unlink()
    except PermissionError:
        # Windows refuses to unlink a file another handle still holds open. That
        # only happens when the holder is LIVE and in this same process tree --
        # i.e. exactly the case `--force` is claiming is safe but is not. Report it
        # honestly rather than pretending the release succeeded.
        raise LockHeldError(
            f"the lock file is still open by a live handle ({holder or 'unknown'}), so "
            f"the OS refuses to remove it. The holder is genuinely running; stop it "
            f"before recovering."
        ) from None
    return {"released": True, "holder": holder, "holder_was_alive": alive}


def main(argv=None) -> int:
    """Small CLI for the operational cases the ledger owns."""
    import argparse

    ap = argparse.ArgumentParser(description="v3 bake state ledger utilities")
    ap.add_argument("--release-stale-lock", metavar="STATE_PATH", required=True,
                    help="Release the writer lock for this state file, ONLY if its "
                         "recorded holder process is no longer running.")
    ap.add_argument("--force", action="store_true",
                    help="Release even if the recorded pid appears live (use only "
                         "when you are certain the pid was reused).")
    args = ap.parse_args(argv)
    try:
        result = release_stale_lock(args.release_stale_lock, force=args.force)
    except LockHeldError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 1
    for key, value in result.items():
        print(f"  {key:18s} {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
