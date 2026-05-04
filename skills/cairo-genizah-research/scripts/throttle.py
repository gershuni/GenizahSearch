"""Token-bucket throttle with cross-process state persistence.

Per SKILL-06: separate buckets per endpoint, default ≤24 req/min, burst 5.
Each Python process invocation reads, decrements, and writes the shared
state file under an exclusive file lock so sequential `python scripts/X.py`
calls in the same skill run accumulate properly.

Design notes:
- time.time() used for wall-clock timestamps stored in throttle.json so that
  state persists correctly across process boundaries (monotonic clocks reset
  on each process start — cross-process persistence requires wall clock).
- Block-by-sleep when tokens insufficient; return wait_seconds for caller
  observability.
- Corrupt state recovers as empty (T-81B-05 / R3 mitigation).
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path
from typing import Any, Union

# Support both package import and direct script invocation.
try:
    from . import _config
    from . import _lock
except ImportError:
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import _config  # type: ignore
    import _lock  # type: ignore

STATE_FILENAME = "throttle.json"


def _read_state(path_or_file: Union[Path, Any]) -> dict[str, Any]:
    """Read throttle state from a file handle or from throttle.json in a directory.

    Accepts either:
    - An open file handle (used internally during acquire's locked region).
    - A pathlib.Path pointing to the state *directory* (used by tests to
      inspect persisted state after acquire calls).

    Returns empty dict on missing or corrupt JSON (R3 mitigation).
    """
    if isinstance(path_or_file, Path):
        # Called with a directory path — read throttle.json from it
        state_file = path_or_file / STATE_FILENAME
        if not state_file.exists():
            return {}
        try:
            return json.loads(state_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    else:
        # Called with a file handle (internal use during acquire)
        f = path_or_file
        f.seek(0)
        raw = f.read()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}


def _write_state(f, state: dict[str, Any]) -> None:
    """Write throttle state to an open file handle (internal use during acquire)."""
    f.seek(0)
    f.truncate()
    f.write(json.dumps(state))
    f.flush()


def acquire(bucket: str, rpm: int | None = None, burst: int | None = None) -> float:
    """Block until one token is available for `bucket`. Returns total wait_seconds.

    Uses an exclusive file lock so concurrent skill script invocations cannot
    race on the shared state file (T-81B-05 mitigation).
    """
    if rpm is None:
        rpm = _config.get_rpm()
    if burst is None:
        burst = _config.get_burst()

    path = _config.state_path(STATE_FILENAME)
    # Open in r+; create empty if absent.
    if not path.exists():
        path.write_text("{}", encoding="utf-8")

    with open(path, "r+", encoding="utf-8") as f:
        _lock.lock_file(f)
        try:
            state = _read_state(f)
            now = time.time()
            b = state.get(bucket, {"tokens": float(burst), "last_refill": now})
            elapsed = max(0.0, now - b["last_refill"])
            # Refill: tokens += elapsed * (rpm / 60), capped at burst.
            b["tokens"] = min(float(burst), b["tokens"] + elapsed * (rpm / 60.0))
            b["last_refill"] = now

            wait = 0.0
            if b["tokens"] < 1.0:
                wait = (1.0 - b["tokens"]) * 60.0 / rpm
                time.sleep(wait)
                # After sleep, consume the token and update timestamp.
                b["tokens"] = 0.0
                b["last_refill"] = time.time()
            else:
                b["tokens"] -= 1.0

            state[bucket] = b
            _write_state(f, state)
            return wait
        finally:
            _lock.unlock_file(f)
