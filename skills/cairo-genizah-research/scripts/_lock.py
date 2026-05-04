"""Cross-platform file lock for throttle state.

Unix: fcntl.flock (advisory, exclusive).
Windows: msvcrt.locking (mandatory, byte-range).
Both block until the lock is acquired.
"""
from __future__ import annotations
import sys

if sys.platform == "win32":
    import msvcrt

    def lock_file(f) -> None:
        """Acquire an exclusive lock on the file (Windows msvcrt)."""
        # Lock 1 byte at offset 0; LK_LOCK blocks (retries 10x at 1s intervals).
        f.seek(0)
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
        except OSError:
            # File too short to lock byte 0 — write a sentinel byte then retry.
            current_pos = f.tell()
            f.write(b"\0" if "b" in f.mode else "\0")
            f.flush()
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

    def unlock_file(f) -> None:
        """Release the exclusive lock on the file (Windows msvcrt)."""
        f.seek(0)
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass

else:
    import fcntl

    def lock_file(f) -> None:
        """Acquire an exclusive lock on the file (Unix fcntl)."""
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

    def unlock_file(f) -> None:
        """Release the exclusive lock on the file (Unix fcntl)."""
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
