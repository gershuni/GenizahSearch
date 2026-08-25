# -*- coding: utf-8 -*-
"""The desktop passage-index lifecycle: one state machine, testable without Qt.

Everything the desktop app needs to know about the letter-level passage index
lives here -- the single installed state, the build worker's phases, the
two-phase swap that replaces a live artifact without ever serving a half
-written one, startup recovery, staleness checking, and the pure readiness
gate the Passage tab's UI consults. It is deliberately factored as pure
functions plus a handful of small dataclasses: every filesystem operation
takes an explicit `root` (or path) argument rather than reading Config, so
the whole module is exercised with tmp_path and no QApplication. A future
Qt worker shell wires these functions to signals/slots; no decision logic
belongs there -- see the module docstring of that (not-yet-written) file
when it exists.

Directory layout, all siblings under `root` (production: Config.PASSAGE_INDEX_DIR):

    current/                        the live, currently-installed artifact
    passage_index.building/         staging: a build in progress
    passage_index._prev-<gen>/      one rollback generation per rebuild
    .failed-<gen>/                  a quarantined artifact that failed to open
    passage_index.lock              cross-process lock, BESIDE current/

`<gen>` is `_gen_token()`: nanosecond timestamp plus a random suffix, never a
bare timestamp -- two builds finishing in the same tick is a real collision
risk at Windows' clock resolution, not a theoretical one.
"""
from __future__ import annotations

import gc
import os
import shutil
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Optional

from shared.config import Config
from shared.passage_builder import (
    DEFAULT_BATCH_GRAMS, DEFAULT_PARTITIONS, BuildStats, build_index,
    check_free_space, estimate_artifact_bytes,
)
from shared.passage_corpus import source_manifest
from shared.passage_index import BuildCancelled, PassageIndex, open_index
from shared.passage_policy import (
    DEFAULT_DEPTH, DEFAULT_LENGTH, DEPTH_PROFILES, LENGTH_PROFILES, PRESETS,
    compose,
)

# ---------------------------------------------------------------------------
# Layout constants.
# ---------------------------------------------------------------------------

LIVE_DIRNAME = 'current'
STAGING_DIRNAME = 'passage_index.building'
PREV_PREFIX = 'passage_index._prev-'
FAILED_PREFIX = '.failed-'
LOCK_NAME = 'passage_index.lock'

_DEFAULT_WIDTH = 'widest-40'


def passage_root() -> str:
    """Production root. Every function below takes `root` explicitly instead
    of calling this, so tests never touch Config or the real filesystem."""
    return Config.PASSAGE_INDEX_DIR


def _gen_token() -> str:
    """time_ns() alone collides on back-to-back builds at Windows' clock
    resolution; the random suffix is what makes the name actually unique."""
    return f'{time.time_ns()}-{uuid.uuid4().hex[:8]}'


def _cleanup_staging(staging_dir: str) -> None:
    if os.path.isdir(staging_dir):
        shutil.rmtree(staging_dir)


def _release_index_handles(idx: Optional[PassageIndex]) -> None:
    """Close the underlying mmap.mmap of every memmapped section.

    `np.memmap()` closes the file descriptor it used to build the mapping by
    the time it returns -- the OS handle Windows actually cares about is the
    mapping itself, held by `._mmap`. A zero-length section is a plain
    `np.empty` array with no `._mmap` attribute (see passage_index.py's
    `_map`), so `getattr(..., '_mmap', None)` skips those safely.
    """
    if idx is None:
        return
    for name in ('gram_offsets', 'postings', 'streams', 'records',
                 'record_ids'):
        arr = getattr(idx, name, None)
        mm = getattr(arr, '_mmap', None)
        if mm is not None:
            mm.close()


def _open_with_retry(path: str, attempts: int = 2) -> Optional[PassageIndex]:
    """`open_index` converts EVERY exception, including a transient access
    error, into None -- so a single flaky read must not downgrade a genuinely
    valid artifact. Used by both the swap's reload check and startup
    recovery."""
    for _ in range(attempts):
        idx = open_index(path)
        if idx is not None:
            return idx
    return None


# Retry knobs for os.rename against a transient Windows PermissionError (a
# lagging antivirus scan, Explorer's thumbnail cache, or a background delete
# of a PREVIOUS generation this process itself started). Module attributes,
# not defaults baked into the function, so tests can shrink them.
RENAME_RETRY_ATTEMPTS = 5
RENAME_RETRY_DELAY_SECONDS = 0.1


def _retry_rename(src: str, dst: str) -> None:
    last: Optional[PermissionError] = None
    for i in range(RENAME_RETRY_ATTEMPTS):
        try:
            os.rename(src, dst)
            return
        except PermissionError as exc:
            last = exc
            if i < RENAME_RETRY_ATTEMPTS - 1:
                time.sleep(RENAME_RETRY_DELAY_SECONDS)
    raise last


def _delete_generation_background(path: str) -> threading.Thread:
    """A successful rebuild's old generation is deleted off-thread -- nothing
    downstream depends on it, and rmtree on a multi-GB tree should never hold
    up reporting the build as done. Each generation's name is unique
    (`_gen_token`), so a lagging delete can never collide with the NEXT
    build's generation name."""
    t = threading.Thread(
        target=lambda: shutil.rmtree(path, ignore_errors=True), daemon=True)
    t.start()
    return t


def _delete_stale_quarantines_background(root: str) -> list:
    """A `.failed-*` quarantine is KEPT while no proven copy exists (see the
    retention rule in `_swap_first_build`/`_swap_rebuild`) -- it may be the
    only surviving evidence of what went wrong. A proven-successful build is
    what finally lets it go: this is called ONLY from `run_build_and_swap`'s
    'installed' branch, never from recovery's 'live_ok' path, because
    recovery proves an EXISTING artifact still works, not that a fresh build
    succeeded."""
    threads = []
    if not os.path.isdir(root):
        return threads
    for n in os.listdir(root):
        if n.startswith(FAILED_PREFIX):
            threads.append(_delete_generation_background(os.path.join(root, n)))
    return threads


# ---------------------------------------------------------------------------
# Cross-process lock. msvcrt on Windows, fcntl on POSIX -- the Ubuntu half of
# CI runs these tests too, so the import must stay behind the platform check.
# Non-blocking: a second process is refused immediately rather than left to
# wait, and the OS releases the lock the moment the holding process dies (a
# create-O_EXCL marker file would strand every later recovery attempt behind
# a force-quit that never got to clean up after itself).
# ---------------------------------------------------------------------------

if sys.platform == 'win32':
    import msvcrt

    def _try_lock(fh) -> bool:
        # msvcrt.locking locks a BYTE RANGE, not bytes that have to already
        # exist -- it succeeds on a freshly created, zero-length lock file
        # (measured), so unlike the skills-tree's blocking _lock.py this
        # needs no "file too short, write a sentinel" fallback. Attempting
        # that fallback here instead deadlocks it: a second handle's write
        # into a range the first handle already holds is itself blocked by
        # Windows' MANDATORY locking, so the fallback's own write raises the
        # very PermissionError it exists to work around.
        fh.seek(0)
        try:
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False

    def _unlock(fh) -> None:
        fh.seek(0)
        try:
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass

else:
    import fcntl

    def _try_lock(fh) -> bool:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def _unlock(fh) -> None:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass


def acquire_lock(root: str):
    """Non-blocking attempt at the cross-process passage-index lock.

    Returns an open, retained file handle on success (closing it releases
    the lock) or None when another process holds it. The lock file sits
    BESIDE the swapped directory, in `root` itself -- never inside `current`,
    which is exactly the directory a rebuild renames away.
    """
    os.makedirs(root, exist_ok=True)
    fh = open(os.path.join(root, LOCK_NAME), 'a+b')
    if _try_lock(fh):
        return fh
    fh.close()
    return None


def release_lock(handle) -> None:
    """The ONE release helper. Every terminal path of a build+swap run
    reaches this exactly once: a build error, a build cancel, a hash cancel,
    a failure before the worker even starts, and every swap terminal --
    `run_build_and_swap`'s `finally` is the single call site."""
    if handle is None:
        return
    try:
        _unlock(handle)
    finally:
        try:
            handle.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# State: single owner. `_state` is the one and only copy this module (or the
# main window, which must hold no second copy) ever reads a live PassageIndex
# from.
# ---------------------------------------------------------------------------

@dataclass
class PassageState:
    index: PassageIndex
    live_dir: str


_state: Optional[PassageState] = None


def load_passage_state(root: Optional[str] = None) -> Optional[PassageState]:
    """Computes and RETURNS a state -- never assigns. `open_index` scans the
    full CSR (~109.5 MB on the shipped corpus), so this must run off the UI
    thread; the caller hands the result to `install_passage_state` there.

    Goes through the same bounded retry as every other reload check in this
    module -- `open_index` swallows a transient access error into None just
    like a real missing index, so a single flake here must not report a
    valid `current/` as absent and send the user toward a 3.5 GB rebuild."""
    root = root if root is not None else passage_root()
    live_dir = os.path.join(root, LIVE_DIRNAME)
    idx = _open_with_retry(live_dir)
    if idx is None:
        return None
    return PassageState(index=idx, live_dir=live_dir)


def install_passage_state(state: Optional[PassageState]) -> bool:
    """UI-thread-only, like `close_passage_state` (see its docstring for the
    ownership rule). An install that REPLACES a live state must release the
    outgoing one first: `_state` is the only reference this process holds to
    those five memmaps, so simply overwriting it would leak them, and on
    Windows a leaked handle can later block a rename of that very same
    directory. Same-object and None outgoing states are left alone --
    re-releasing handles this process no longer holds is a no-op at best and
    a double-close at worst. A staleness check computed against the artifact
    this replaces is now for a different generation, so any in-flight banner
    state is discarded here too.

    Returns False -- refusing to touch `_state` at all -- when the outgoing
    index has an outstanding `get_passage_searcher()` lease that never
    drained (see `_release_live_index`); True otherwise, including the
    same-object and None-outgoing no-ops above."""
    global _state
    outgoing = _state
    if outgoing is not None and outgoing is not state:
        if not _release_live_index(outgoing.index):
            return False
        gc.collect()
    _state = state
    _reset_freshness()
    return True


def close_passage_state() -> bool:
    """Release the live memmaps, then drop the reference. An open memmap
    blocks os.rename/rmtree on Windows for as long as it -- or a traceback
    holding it -- survives, so this is the mandatory step between "the UI
    stops using the live index" and "a rename may touch `current`".

    Returns False -- leaving `_state` exactly as it was, nothing closed --
    when a `get_passage_searcher()` lease is still outstanding after
    `_release_live_index`'s wait window (see that function): a search thread
    mid-query holds this SAME PassageIndex object, and force-closing it
    would invalidate its numpy memmaps out from under that read, which is an
    access violation, not an exception. Callers that treat every call as
    unconditionally successful are the bug this return value exists to
    catch -- see `run_build_and_swap`'s handling of its `release_live_state`
    seam.

    OWNERSHIP RULE (stated once, here): `_state` is mutated ONLY on the UI
    thread -- this function, `install_passage_state`, and nowhere else. A
    background worker never calls this directly; it requests the release
    through a caller-supplied seam instead (see `run_build_and_swap`'s
    `release_live_state` parameter) so the actual mutation still happens on
    the UI thread even though the request to do it originates off it."""
    global _state
    if _state is not None:
        if not _release_live_index(_state.index):
            return False
    _state = None
    gc.collect()
    _reset_freshness()
    return True


def passage_available() -> bool:
    return _state is not None


# ---------------------------------------------------------------------------
# Reader leases. `get_passage_searcher()` hands the caller a raw
# `PassageIndex` reference (wrapped in a `PassageSearcher`) that a worker
# thread may still be reading -- a numpy memmap access with no natural end
# this module can observe -- long after the call that produced it returns.
# `close_passage_state()`/`install_passage_state()` must know such a read is
# in flight before they force-close that SAME index's memmaps: a concurrent
# read off a mapping closed out from under it is an access violation, not a
# catchable exception. This lease is the single owner-side mechanism that
# makes that provable rather than merely hoped for.
# ---------------------------------------------------------------------------

LEASE_DRAIN_TIMEOUT_SECONDS = 5.0
LEASE_DRAIN_POLL_SECONDS = 0.05

_lease_lock = threading.Lock()
_outstanding_leases = 0


def _acquire_lease() -> None:
    global _outstanding_leases
    with _lease_lock:
        _outstanding_leases += 1


def _release_lease() -> None:
    global _outstanding_leases
    with _lease_lock:
        # Floored at 0 rather than trusting callers to release exactly
        # once -- `PassageSearcherLease.release()` already enforces that
        # itself (see its docstring), but the floor is what keeps a defect
        # THERE from ever wedging this count below zero and refusing every
        # close forever.
        if _outstanding_leases > 0:
            _outstanding_leases -= 1


def _leases_outstanding() -> bool:
    with _lease_lock:
        return _outstanding_leases > 0


def _wait_for_leases_to_drain(
        timeout: float = LEASE_DRAIN_TIMEOUT_SECONDS) -> bool:
    """Polls rather than blocking on a condition variable: a lease is
    released from whichever worker thread's query happens to finish, an
    arbitrary and unpredictable thread from this function's point of view,
    so there is no single event object it could wait on instead."""
    deadline = time.monotonic() + timeout
    while _leases_outstanding():
        if time.monotonic() >= deadline:
            return False
        time.sleep(LEASE_DRAIN_POLL_SECONDS)
    return True


def _release_live_index(idx: PassageIndex) -> bool:
    """The one seam `close_passage_state` and `install_passage_state` both
    route their outgoing-index release through. Waits out
    `_wait_for_leases_to_drain`'s bounded window, then either force-closes
    `idx` (leases gone) or refuses entirely (leases survived) -- there is no
    partial outcome: an index this function did not report closed is still
    exactly as open as it was before the call."""
    if not _wait_for_leases_to_drain():
        return False
    _release_index_handles(idx)
    return True


class PassageSearcherLease:
    """What `get_passage_searcher()` returns instead of a bare
    `PassageSearcher`. The searcher is reachable ONLY through the `with`
    block -- there is no public accessor for it otherwise -- so a caller
    cannot end up holding a reference to the index without the lease that
    reference requires being counted.

    `release()` is the non-context escape hatch, for a caller that truly
    cannot use `with`; it is what `__exit__` itself calls, and it is
    idempotent -- safe to call again, from a retry or a stray `finally` --
    because a second call is a no-op rather than a double decrement, which
    is what makes "release exactly once" true regardless of how many times
    release is actually requested.
    """

    def __init__(self, searcher) -> None:
        self._searcher = searcher
        self._released = False
        _acquire_lease()

    def __enter__(self):
        return self._searcher

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.release()
        return False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        _release_lease()


def get_passage_searcher(text_fetcher, width: str = _DEFAULT_WIDTH,
                          length: str = DEFAULT_LENGTH,
                          depth: str = DEFAULT_DEPTH):
    """A `PassageSearcherLease` wrapping a fresh PassageSearcher, or None
    when no index is installed. Use it as a `with` block:

        with get_passage_searcher(text_fetcher) as searcher:
            ... run the search ...

    The lease is what keeps `close_passage_state()`/`install_passage_state()`
    from force-closing the mmap this searcher reads while the `with` block
    -- typically the composition thread's terminal -- is still inside it;
    see `PassageSearcherLease`.

    Each axis is validated INDEPENDENTLY against the finite set `compose()`
    actually knows, falling back to the default on anything unrecognised --
    `compose`/`get_preset` raise on an unknown name, which is correct for a
    caller passing a literal, but wrong for a value that came out of a
    persisted settings file a user could have hand-edited or that predates a
    preset being renamed. render_cap is deliberately omitted: PassageSearcher
    defaults to PARALLELS_GROUP_CAP (200), which is already the right desktop
    cap.
    """
    if _state is None:
        return None
    if width not in PRESETS:
        width = _DEFAULT_WIDTH
    if length != DEFAULT_LENGTH and length not in LENGTH_PROFILES:
        length = DEFAULT_LENGTH
    if depth != DEFAULT_DEPTH and depth not in DEPTH_PROFILES:
        depth = DEFAULT_DEPTH
    from shared.passage_parallels import PassageSearcher  # local: keep this module import-light
    searcher = PassageSearcher(index=_state.index, text_fetcher=text_fetcher,
                               policy=compose(width, length, depth))
    return PassageSearcherLease(searcher)


# ---------------------------------------------------------------------------
# Staleness. Receives PLAIN COPIED DATA -- never a PassageIndex, which would
# re-pin all five memmaps for the duration of a corpus-wide hash. A
# generation token discards a result computed against an artifact that has
# since been replaced.
# ---------------------------------------------------------------------------

FRESH = 'fresh'
STALE = 'stale'
UNKNOWN = 'unknown'


@dataclass
class FreshnessState:
    generation: int = 0
    checking: bool = False
    result: Optional[str] = None


_freshness = FreshnessState()


def _reset_freshness() -> None:
    global _freshness
    _freshness = FreshnessState(generation=_freshness.generation + 1)


def freshness_snapshot() -> FreshnessState:
    return _freshness


def start_freshness_check() -> int:
    """Bumps the generation token and marks 'checking'. Returns the token
    this check's eventual result must be tagged with."""
    global _freshness
    _freshness = FreshnessState(generation=_freshness.generation + 1,
                                checking=True, result=None)
    return _freshness.generation


def compute_freshness(corpus_sources: list, source_paths: list,
                      cancel_check: Optional[Callable[[], bool]] = None
                      ) -> Optional[str]:
    """The cancellable worker body -- run OFF the UI thread with a COPY of
    `manifest['corpus']['sources']` (see `start_freshness_check`'s caller),
    never a PassageIndex. Returns None on cancel; `finish_freshness_check` is
    the only place allowed to act on that.

    `corpus_sources` empty/absent means the artifact predates fingerprinting
    being fixed for real (Judeo-Arabic-stratum era manifests) -- reported as
    unknown rather than guessed at either way.
    """
    if not corpus_sources:
        return UNKNOWN
    try:
        current = source_manifest(source_paths, cancel_check=cancel_check)
    except BuildCancelled:
        return None
    return FRESH if current == corpus_sources else STALE


def finish_freshness_check(token: int, result: Optional[str]) -> None:
    """The ONE terminal path. A stale token (the artifact was replaced while
    this check was running) is silently discarded. A None result (the check
    was itself cancelled) clears `checking` and leaves `result` at None --
    nothing stuck 'checking' forever, and no wrong verdict flashed either."""
    global _freshness
    if token != _freshness.generation:
        return
    _freshness = FreshnessState(generation=_freshness.generation,
                                checking=False, result=result)


# ---------------------------------------------------------------------------
# Build, phase 0: staging preparation, off the UI thread.
# ---------------------------------------------------------------------------

def estimate_build_bytes(corpus_path: str) -> int:
    """Free-space preflight sizing. `letters_ub` is a STRICT upper bound on
    the normalized letter count a build of `corpus_path` can produce: every
    Hebrew letter is 2 UTF-8 bytes, and normalization only ever REMOVES
    characters (punctuation, non-Hebrew runs, vowel points) -- so raw file
    bytes // 2 can only over-estimate. Passing raw bytes instead would
    roughly double the demand and refuse users who actually have room."""
    letters_ub = os.path.getsize(corpus_path) // 2
    return estimate_artifact_bytes(letters_ub) + 8 * letters_ub


def prepare_staging(root: str, corpus_path: str) -> None:
    """Build worker phase 0. Order matters: a crashed staging tree from a
    previous failed attempt can hold nearly the whole headroom a fresh build
    needs, so preflighting BEFORE clearing it would refuse every retry
    forever.

    `check_free_space` calls `os.makedirs` on its own argument, so it is
    pointed at `root` -- the parent of both `current/` and the staging tree
    -- and never at the live directory itself: creating an empty `current/`
    here would make a first build look, to every later `os.path.isdir(live)`
    check, exactly like a rebuild.
    """
    staging = os.path.join(root, STAGING_DIRNAME)
    if os.path.isdir(staging):
        shutil.rmtree(staging)
    check_free_space(root, estimate_build_bytes(corpus_path))


# ---------------------------------------------------------------------------
# Build, phase 1: run the builder into staging.
# ---------------------------------------------------------------------------

@dataclass
class BuildOutcome:
    status: str  # 'built' | 'cancelled' | 'error'
    stats: Optional[BuildStats] = None
    staging_dir: str = ''
    pre_build_manifest: Optional[list] = None
    error: str = ''


def execute_build(root: str, records, source_paths: list, *,
                  construction: str = 'spool',
                  partitions: int = DEFAULT_PARTITIONS,
                  batch_grams: int = DEFAULT_BATCH_GRAMS,
                  df_cap: Optional[int] = None, corpus_label: str = '',
                  progress: Optional[Callable] = None,
                  cancel_check: Optional[Callable[[], bool]] = None
                  ) -> BuildOutcome:
    """Runs `build_index` into `root/passage_index.building`.

    Progress passes straight through to `build_index`'s own callback: pass 1
    is INDETERMINATE (records seen and records indexed, neither a total),
    pass 2 is determinate per partition -- this function invents no progress
    model of its own.

    `owns_staging` is what makes "every unswapped failure cleans staging"
    true without racing a swap that has already taken over: it flips to
    False the instant `build_index` returns successfully, before anything
    else in this function runs, and `finally` only ever deletes staging
    while it is still True. From that point on, staging is the swap
    protocol's (`begin_swap`/`perform_swap`) to dispose of -- this function
    is finished with it whether or not the eventual swap succeeds.
    """
    staging = os.path.join(root, STAGING_DIRNAME)
    owns_staging = True
    stats: Optional[BuildStats] = None
    pre_manifest: Optional[list] = None
    try:
        pre_manifest = source_manifest(source_paths, cancel_check=cancel_check)
        stats = build_index(records, staging, construction=construction,
                            partitions=partitions, batch_grams=batch_grams,
                            df_cap=df_cap, corpus_label=corpus_label,
                            source_manifest=pre_manifest, progress=progress,
                            cancel_check=cancel_check)
        owns_staging = False
    except BuildCancelled:
        return BuildOutcome(status='cancelled', staging_dir=staging)
    except Exception as exc:
        return BuildOutcome(status='error', staging_dir=staging,
                            error=str(exc))
    finally:
        # No ignore_errors: this is the Windows regression the whole
        # try/finally in build_index() exists to prevent (its own docstring),
        # and swallowing a leak here would only defer the failure to the
        # NEXT build's stale-staging cleanup. A real leak must raise, loudly,
        # right here.
        if owns_staging and os.path.isdir(staging):
            shutil.rmtree(staging)

    return BuildOutcome(status='built', stats=stats, staging_dir=staging,
                        pre_build_manifest=pre_manifest)


# ---------------------------------------------------------------------------
# Swap, phase one: validate staging without touching live. Both steps here
# leave the working state installed no matter the outcome -- a failure here
# costs nothing.
# ---------------------------------------------------------------------------

@dataclass
class SwapValidation:
    ok: bool
    reason: str = ''


VALIDATION_INVALID_STAGING = 'invalid_staging'
VALIDATION_CORPUS_CHANGED = 'corpus_changed'


def begin_swap(staging_dir: str, source_paths: list,
               pre_build_manifest: list,
               cancel_check: Optional[Callable[[], bool]] = None
               ) -> SwapValidation:
    """Phase one. Opens `staging_dir` with the same bounded retry as every
    other reload check in this module and releases those handles immediately
    -- the only purpose is proving the artifact is real -- then re-fingerprints
    the corpus. A ten-minute build must not be discarded over one transient
    Windows access error; only a candidate that fails EVERY attempt is
    invalid, and nothing is deleted before it has been given that chance. If
    the corpus changed DURING the build, staging is discarded rather than
    swapped in: the artifact and the files on disk would silently disagree.

    A hash cancel raises `BuildCancelled` (propagated, not swallowed) after
    cleaning staging -- consistent with every other cancel point in this
    subsystem.
    """
    idx = _open_with_retry(staging_dir)
    if idx is None:
        _cleanup_staging(staging_dir)
        return SwapValidation(ok=False, reason=VALIDATION_INVALID_STAGING)
    _release_index_handles(idx)
    del idx
    gc.collect()

    try:
        post_manifest = source_manifest(source_paths, cancel_check=cancel_check)
    except BuildCancelled:
        _cleanup_staging(staging_dir)
        raise

    if post_manifest != pre_build_manifest:
        _cleanup_staging(staging_dir)
        return SwapValidation(ok=False, reason=VALIDATION_CORPUS_CHANGED)
    return SwapValidation(ok=True)


# ---------------------------------------------------------------------------
# Swap, phase two: the caller must already have released every handle on
# `live` (see `close_passage_state`) before calling this -- an open memmap
# blocks os.rename on Windows.
# ---------------------------------------------------------------------------

@dataclass
class SwapResult:
    status: str
    index: Optional[PassageIndex] = None
    live_dir: str = ''
    prev_dir: str = ''
    quarantine_dir: str = ''


def perform_swap(root: str, staging_dir: str,
                 cancel_check: Optional[Callable[[], bool]] = None
                 ) -> SwapResult:
    """Phase two. FIRST BUILD (no `live/`) renames staging straight to
    `current/`, inventing no rollback generation. REBUILD renames `current/`
    aside to a unique generation first, then staging to `current/`; if the
    second rename fails, the first is rolled back so `current/` never ends
    up missing. The cancelled latch is re-checked immediately before every
    destructive rename, not only once at entry.
    """
    live = os.path.join(root, LIVE_DIRNAME)

    def cancelled() -> bool:
        return cancel_check is not None and cancel_check()

    if cancelled():
        _cleanup_staging(staging_dir)
        return SwapResult(status='cancelled')

    if not os.path.isdir(live):
        return _swap_first_build(live, staging_dir, cancelled)
    return _swap_rebuild(root, live, staging_dir, cancelled)


def _swap_first_build(live: str, staging_dir: str,
                      cancelled: Callable[[], bool]) -> SwapResult:
    # Re-checked immediately before the destructive rename, not only at
    # `perform_swap`'s entry -- a cancel racing in between those two points
    # must not promote staging, exactly like `_swap_rebuild`'s own re-check
    # before ITS destructive rename.
    if cancelled():
        _cleanup_staging(staging_dir)
        return SwapResult(status='cancelled')

    try:
        _retry_rename(staging_dir, live)
    except PermissionError:
        _cleanup_staging(staging_dir)
        return SwapResult(status='rename_blocked')

    idx = _open_with_retry(live)
    if idx is not None:
        return SwapResult(status='installed', index=idx, live_dir=live)

    # Nothing to roll back to. Quarantine the failed artifact and KEEP it --
    # no proven copy exists at all, so it may be the only surviving evidence
    # of what went wrong; only a later proven build is allowed to delete it.
    root = os.path.dirname(live)
    failed = os.path.join(root, f'{FAILED_PREFIX}{_gen_token()}')
    try:
        _retry_rename(live, failed)
    except PermissionError:
        return SwapResult(status='rename_blocked')
    return SwapResult(status='reload_failed_first_install',
                      quarantine_dir=failed)


def _swap_rebuild(root: str, live: str, staging_dir: str,
                  cancelled: Callable[[], bool]) -> SwapResult:
    gen = _gen_token()
    prev = os.path.join(root, f'{PREV_PREFIX}{gen}')
    try:
        _retry_rename(live, prev)
    except PermissionError:
        _cleanup_staging(staging_dir)
        return SwapResult(status='rename_blocked')

    if cancelled():
        try:
            _retry_rename(prev, live)
        except PermissionError:
            # `prev` cannot go back to `live` either -- `current/` stays
            # absent here, exactly the state the terminal recovery walk
            # exists to fix, so this must return a status routed there
            # rather than let the exception skip that walk entirely.
            _cleanup_staging(staging_dir)
            return SwapResult(status='rollback_failed')
        _cleanup_staging(staging_dir)
        return SwapResult(status='cancelled')

    try:
        _retry_rename(staging_dir, live)
    except PermissionError:
        try:
            _retry_rename(prev, live)  # restore the working generation
        except PermissionError:
            # Same reasoning as the cancel path above: report a status the
            # terminal recovery walk handles instead of raising past it.
            _cleanup_staging(staging_dir)
            return SwapResult(status='rollback_failed')
        # The restore succeeded -- the caller ends up with a WORKING index,
        # so this is a routine failure, not the last-resort `rollback_failed`
        # above. Staging is still this function's to dispose of here: the
        # terminal recovery walk (`_recover_at_startup`) never looks inside
        # `passage_index.building`, so a validated multi-GB artifact would
        # otherwise sit on disk until some later build happened to clear it.
        _cleanup_staging(staging_dir)
        return SwapResult(status='rename_blocked')

    idx = _open_with_retry(live)
    if idx is not None:
        return SwapResult(status='installed', index=idx, live_dir=live,
                          prev_dir=prev)

    # The new artifact renamed in cleanly but will not open. Quarantine it,
    # restore the known-good previous generation, and re-validate THAT --
    # since a proven copy (`prev`) exists here, the quarantine is deleted
    # immediately once the rollback is proven good, unlike the first-install
    # case above.
    failed = os.path.join(root, f'{FAILED_PREFIX}{_gen_token()}')
    try:
        _retry_rename(live, failed)
    except PermissionError:
        return SwapResult(status='rename_blocked')
    try:
        _retry_rename(prev, live)
    except PermissionError:
        return SwapResult(status='rollback_failed', quarantine_dir=failed)

    idx2 = _open_with_retry(live)
    if idx2 is not None:
        shutil.rmtree(failed, ignore_errors=True)
        return SwapResult(status='reload_failed_rolled_back', index=idx2,
                          live_dir=live)
    return SwapResult(status='reload_failed_no_recovery', quarantine_dir=failed)


def _reopen_live(root: str) -> SwapResult:
    """Reopen whatever currently sits at `live`, with the same bounded retry
    as startup recovery. Used after any PRE-promotion swap failure so a
    failed rebuild never leaves a previously working feature reporting
    not-ready; if nothing openable is there (a first build that never got
    anywhere), the result carries no index and that is the honest answer."""
    live = os.path.join(root, LIVE_DIRNAME)
    idx = _open_with_retry(live)
    if idx is None:
        return SwapResult(status='no_prior_state')
    return SwapResult(status='reinstalled', index=idx, live_dir=live)


# ---------------------------------------------------------------------------
# The whole sequence, lock held throughout.
# ---------------------------------------------------------------------------

@dataclass
class BuildAndSwapResult:
    status: str
    index: Optional[PassageIndex] = None
    live_dir: str = ''
    stats: Optional[BuildStats] = None
    error: str = ''
    quarantine_dir: str = ''


def run_build_and_swap(root: str, records, source_paths: list,
                       corpus_path: str, *, construction: str = 'spool',
                       partitions: int = DEFAULT_PARTITIONS,
                       batch_grams: int = DEFAULT_BATCH_GRAMS,
                       df_cap: Optional[int] = None, corpus_label: str = '',
                       progress: Optional[Callable] = None,
                       cancel_check: Optional[Callable[[], bool]] = None,
                       release_live_state: Callable[[], Optional[bool]]
                       ) -> BuildAndSwapResult:
    """Build -> validate -> swap, with the cross-process lock held for the
    whole run and released exactly once, in `finally`, regardless of which
    of those stages fails. This function does NOT call
    `install_passage_state` on success -- see that function's docstring for
    why assignment stays a separate, UI-thread-only step -- it returns the
    opened index for the caller to install.

    This function runs off the UI thread, but `_state` is UI-thread-only
    (see `close_passage_state`'s docstring) -- so the one point where a live
    artifact's handles must be released before a destructive rename is NOT
    done by calling `close_passage_state` in place here. It is the two-phase
    swap handoff instead: staging is already validated and the corpus already
    re-fingerprinted (`begin_swap` above) by the time `release_live_state` is
    called, so the caller's job is only to hop to the UI thread, release
    there, and block until that release has actually happened before
    returning -- only THEN does this function proceed to `perform_swap`'s
    renames. The real Qt wiring supplies a callable that does that hop (e.g.
    a blocking queued signal/slot) and PROPAGATES `close_passage_state`'s own
    return value back across it; a test may pass `close_passage_state`
    directly since nothing in a test is actually multi-threaded. Required,
    not defaulted to `close_passage_state`, so this module can never again
    grow a path that touches the global itself.

    A return of `False` -- and ONLY `False`, checked with `is`, never plain
    falsiness -- means the release was explicitly refused: a
    `get_passage_searcher()` lease never drained (see `_release_live_index`).
    This function then abandons the swap without ever calling `perform_swap`
    (nothing was released, so nothing may be renamed) and cleans staging like
    every other unswapped terminal. A seam returning `None` (a legacy no-op,
    or a caller that has not been taught the new contract yet) is NOT treated
    as a refusal -- it proceeds exactly as before, so whatever the OS/rename
    layer does about a still-open handle is what happens, unchanged.

    A concurrent second run (another window, a leftover process) is refused
    up front rather than left to race the filesystem.
    """
    lock = acquire_lock(root)
    if lock is None:
        return BuildAndSwapResult(status='locked')
    try:
        prepare_staging(root, corpus_path)
        build = execute_build(root, records, source_paths,
                              construction=construction, partitions=partitions,
                              batch_grams=batch_grams, df_cap=df_cap,
                              corpus_label=corpus_label, progress=progress,
                              cancel_check=cancel_check)
        if build.status == 'cancelled':
            return BuildAndSwapResult(status='cancelled')
        if build.status == 'error':
            return BuildAndSwapResult(status='error', error=build.error)

        staging = build.staging_dir
        try:
            validation = begin_swap(staging, source_paths,
                                    build.pre_build_manifest,
                                    cancel_check=cancel_check)
        except BuildCancelled:
            recovered = _reopen_live(root)
            return BuildAndSwapResult(status='cancelled',
                                      index=recovered.index,
                                      live_dir=recovered.live_dir)
        if not validation.ok:
            recovered = _reopen_live(root)
            return BuildAndSwapResult(status=validation.reason,
                                      index=recovered.index,
                                      live_dir=recovered.live_dir)

        # Two-phase handoff: caller releases on the UI thread. `is False`,
        # never bare falsiness -- see the docstring above for why `None`
        # (a seam that does not yet return anything) must NOT be read as a
        # refusal.
        released = release_live_state()
        if released is False:
            _cleanup_staging(staging)
            return BuildAndSwapResult(status='readers_active')
        swap = perform_swap(root, staging, cancel_check=cancel_check)

        if swap.status == 'installed':
            if swap.prev_dir:
                _delete_generation_background(swap.prev_dir)
            _delete_stale_quarantines_background(root)
            return BuildAndSwapResult(status='installed', index=swap.index,
                                      live_dir=swap.live_dir, stats=build.stats)
        if swap.status == 'reload_failed_rolled_back':
            return BuildAndSwapResult(status=swap.status, index=swap.index,
                                      live_dir=swap.live_dir)
        if swap.status == 'reload_failed_first_install':
            return BuildAndSwapResult(status=swap.status,
                                      quarantine_dir=swap.quarantine_dir)

        # rename_blocked / cancelled / reload_failed_no_recovery /
        # rollback_failed -- `current/` may be ABSENT here (rollback_failed
        # leaves it missing while a perfectly good `_prev-*` still sits on
        # disk), so a bare open of `current/` is not enough. Route through
        # the same validate-newest-to-oldest walk startup recovery uses --
        # one recovery implementation, not two that can disagree -- so the
        # caller still ends up with the best index actually recoverable.
        # `_recover_at_startup` deletes nothing until a candidate proves
        # openable, and never touches the lock (already held here).
        recovered = _recover_at_startup(root)
        return BuildAndSwapResult(status=swap.status, index=recovered.index,
                                  live_dir=recovered.live_dir,
                                  quarantine_dir=swap.quarantine_dir)
    finally:
        release_lock(lock)


# ---------------------------------------------------------------------------
# Recovery at startup: validation-first. Nothing is deleted until some
# candidate has passed a REAL open_index.
# ---------------------------------------------------------------------------

@dataclass
class RecoveryResult:
    status: str
    index: Optional[PassageIndex] = None
    live_dir: str = ''


def _gen_sort_key(name: str, prefix: str) -> int:
    """Newest-first ordering by the numeric time_ns component of a generation
    token -- an explicit numeric sort rather than trusting string order,
    since tokens from different runs can differ in digit count."""
    tail = name[len(prefix):]
    try:
        return int(tail.split('-', 1)[0])
    except ValueError:
        return 0


def _candidates(root: str) -> list:
    """(kind, path), newest to oldest within each kind: live first, then
    every `_prev-*` generation, then every `.failed-*` quarantine last -- a
    quarantine is tried only when nothing better survived."""
    live = os.path.join(root, LIVE_DIRNAME)
    out = [('live', live)] if os.path.isdir(live) else []
    if not os.path.isdir(root):
        return out
    entries = os.listdir(root)
    prevs = sorted((n for n in entries if n.startswith(PREV_PREFIX)),
                   key=lambda n: _gen_sort_key(n, PREV_PREFIX), reverse=True)
    out += [('prev', os.path.join(root, n)) for n in prevs]
    faileds = sorted((n for n in entries if n.startswith(FAILED_PREFIX)),
                     key=lambda n: _gen_sort_key(n, FAILED_PREFIX),
                     reverse=True)
    out += [('failed', os.path.join(root, n)) for n in faileds]
    return out


def _delete_prev_generations(root: str) -> None:
    """Deletes every `_prev-*` generation. Called ONLY once `live` is
    proven -- quarantine retention is a separate rule, never touched here."""
    if not os.path.isdir(root):
        return
    for n in os.listdir(root):
        if n.startswith(PREV_PREFIX):
            shutil.rmtree(os.path.join(root, n), ignore_errors=True)


def _recover_at_startup(root: str) -> RecoveryResult:
    live = os.path.join(root, LIVE_DIRNAME)
    for kind, path in _candidates(root):
        idx = _open_with_retry(path)
        if idx is None:
            continue

        if kind == 'live':
            _delete_prev_generations(root)
            return RecoveryResult(status='live_ok', index=idx, live_dir=live)

        # A `_prev-*` or `.failed-*` candidate proved openable IN PLACE.
        # Release its handles before any rename -- the validation itself
        # pinned the directory, exactly like `begin_swap`'s staging check.
        _release_index_handles(idx)
        del idx
        gc.collect()

        if os.path.isdir(live):
            # `live` exists but nothing above proved it -- move it aside
            # first; a generation cannot be renamed onto an existing
            # directory.
            dead = os.path.join(root, f'.dead-{_gen_token()}')
            try:
                _retry_rename(live, dead)
            except PermissionError:
                continue  # try the next older candidate instead
            shutil.rmtree(dead, ignore_errors=True)

        try:
            _retry_rename(path, live)
        except PermissionError:
            continue

        idx2 = _open_with_retry(live)
        if idx2 is None:
            continue  # unlikely after in-place validation; try the next one

        if kind == 'prev':
            _delete_prev_generations(root)
        # a promoted quarantine is consumed, not retained -- nothing more
        # to do for the 'failed' case.
        status = 'recovered_from_prev' if kind == 'prev' \
            else 'recovered_from_quarantine'
        return RecoveryResult(status=status, index=idx2, live_dir=live)

    return RecoveryResult(status='nothing_recoverable')


def recover_at_startup(root: str) -> RecoveryResult:
    """Acquires the cross-process lock non-blocking; when unavailable, SKIPS
    recovery entirely and loads fail-closed -- another process is mid
    -operation, so this one touches nothing. The lock is released whether
    recovery finds anything or not.
    """
    lock = acquire_lock(root)
    if lock is None:
        return RecoveryResult(status='locked')
    try:
        return _recover_at_startup(root)
    finally:
        release_lock(lock)


# ---------------------------------------------------------------------------
# Task 4: the pure readiness gate. Every input explicit, no module or Qt
# state read -- tr() reads a global CURRENT_LANG, which would make this
# function impure, so it returns a reason KEY for the caller to translate.
# ---------------------------------------------------------------------------

REASON_SCOPE = 'passage_disabled_scope'
REASON_NOT_BUILT = 'passage_disabled_not_built'
REASON_LAB_ACTIVE = 'passage_disabled_lab_active'
REASON_BUILD_IN_FLIGHT = 'passage_disabled_build_in_flight'
REASON_MAIN_INDEX_MISSING = 'passage_disabled_main_index_missing'


def passage_disabled_reason(passage_ready: bool, corpus_scope: str,
                            lab_mode_on: bool, build_in_flight: bool,
                            main_index_ready: bool) -> Optional[str]:
    """None means passage search may be offered; otherwise a reason key.

    Checked in this order: scope first (a My Library / All-corpora search
    is simply the wrong surface for a Genizah-only artifact, independent of
    everything else); then not-built and lab-active and build-in-flight, the
    three ordinary day-to-day gates; main-index-missing LAST, because every
    rendered passage row still fetches its display text through the main
    Tantivy index -- readiness means the usable index/searcher attributes
    are present, not merely that a SearchEngine object exists -- and that
    condition is rare enough (a narrow startup window) that it belongs as
    the final fallback rather than pre-empting the routine reasons above it.
    """
    if corpus_scope != 'genizah':
        return REASON_SCOPE
    if not passage_ready:
        return REASON_NOT_BUILT
    if lab_mode_on:
        return REASON_LAB_ACTIVE
    if build_in_flight:
        return REASON_BUILD_IN_FLIGHT
    if not main_index_ready:
        return REASON_MAIN_INDEX_MISSING
    return None
