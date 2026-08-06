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
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional


class BakeState:
    """Resumable step ledger for one bake run."""

    def __init__(self, path: str | os.PathLike, *, run_id: str) -> None:
        self.path = Path(path)
        self.run_id = run_id
        self._data: Dict[str, Any] = {"run_id": run_id, "steps": {}, "log": []}
        if self.path.exists():
            self._load()
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._save()

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
        except BaseException:
            # Never leave a stray temp file behind on failure -- and never let
            # cleanup mask the original error.
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

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
