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
import logging
import os
import shutil
import sys
import threading
import time
import traceback
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

logger = logging.getLogger(__name__)

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
    """Delegates to `PassageIndex.close()` -- the object that owns the
    hazard owns the teardown, not this module reaching into `._mmap` on its
    behalf. `close()` both closes each section's underlying mapping AND
    replaces the section attribute with a poison stand-in that raises
    `PassageIndexClosed` on every access route, so a reference to `idx`
    that escapes this module entirely (a returned searcher, a captured
    bound method, a traceback frame, a logging LogRecord's `exc_info`
    tuple) degrades to a catchable exception on its next use instead of an
    access violation. See that method's docstring; `close()` is idempotent,
    so a second call from a different one of this module's three call
    sites racing the same `idx` is safe.
    """
    if idx is None:
        return
    idx.close()


def _open_with_retry(path: str, attempts: int = 2,
                     backoff: float = 0.0) -> Optional[PassageIndex]:
    """`open_index` converts EVERY exception, including a transient access
    error, into None -- so a single flaky read must not downgrade a genuinely
    valid artifact. Used by both the swap's reload check and startup
    recovery.

    `backoff` sleeps BETWEEN attempts (never before the first, never after
    the last) -- 0.0 by default, which is every existing call site's
    immediate-retry behaviour, unchanged. Startup recovery's re-confirmation
    of `current/` right before destroying it (`CURRENT_DESTROY_RECONFIRM_*`
    below) is the one caller that widens both knobs: two immediate retries
    can both land inside a virus scanner's transient hold on the files, but
    real time between attempts is what a hold that clears on its own needs
    to be told apart from a genuinely dead artifact.
    """
    for i in range(attempts):
        if i and backoff:
            time.sleep(backoff)
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

# Bumped on EVERY mutation of `_state`. A release request is authorised
# against the generation that was current when it was issued: `_call_release_
# seam` abandons a seam that does not answer within
# `UI_RELEASE_SEAM_TIMEOUT_SECONDS`, but abandoning a thread does not stop
# it -- it cannot be killed from here -- so a wedged UI thread that finally
# answers minutes later would otherwise run its close against whatever
# `_state` holds BY THEN. That is a different index: a second build can
# have succeeded and installed in the meantime, and the late close would
# silently take the feature offline until restart. Bounding what the late
# call is permitted to do is the same move `close()`'s poisoning makes --
# the escaped caller cannot be recalled, so it is denied its effect
# instead (Codex review round 7, finding 1).
_state_generation: int = 0


def current_state_generation() -> int:
    """The generation `_state` is on right now. A caller that is about to
    hand a release request across a thread boundary captures this and sends
    it along; `close_passage_state` refuses any request whose captured
    generation is no longer current."""
    return _state_generation


class PassageIndexReplaced(RuntimeError):
    """The installed index changed while a MULTI-WITNESS batch was running.

    Every `search_composition_logic` call resolves the index that is current
    AT THAT CALL -- deliberately, and it is what makes a loop memory-safe. But
    a batch is one ANSWER assembled from N calls, and an install between two
    of them means half the fused list describes one artifact and half another.
    The rank-fusion arithmetic then combines ranks that were never comparable.

    A swap is rare (it needs a rebuild to finish mid-search) and it is not a
    per-witness failure: reporting it as one would tell the reader a witness
    found nothing, when in fact the whole batch is void. Raised once, named
    for what it is, and the batch is abandoned rather than published.
    """


def generation_changed(pinned: Optional[int]) -> bool:
    """Has `_state` been swapped since `pinned` was captured?

    Pure and module level so a caller can be tested without installing an
    index. `pinned is None` means the caller never captured one, which is not
    a change -- a batch that started before anything was installed fails at
    its first lease with `PassageSearchUnavailableError`, which is the honest
    error for that case and not this one.
    """
    return pinned is not None and pinned != _state_generation


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
    global _state, _state_generation
    outgoing = _state
    if outgoing is not None and outgoing is not state:
        def _swap_in():
            global _state, _state_generation
            _state = state
            _state_generation += 1
        if not _release_live_index(outgoing.index, _swap_in):
            return False
        gc.collect()
        _reset_freshness()
        return True
    _state = state
    # NOT unconditional: this branch is reached both for a fresh install
    # (outgoing None) and for re-installing the SAME object, which the
    # docstring above already treats as a no-op. Moving the generation for a
    # no-op invalidates a release that is still perfectly valid -- the
    # identical mapping is still installed -- and `run_build_and_swap` reads
    # that refusal as `readers_active`, cleans staging and skips the rename
    # although nothing was blocking it (Codex review round 8, finding 1).
    if outgoing is not state:
        _state_generation += 1
    _reset_freshness()
    return True


def close_passage_state(expect_generation: Optional[int] = None) -> bool:
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
    the UI thread even though the request to do it originates off it.

    `expect_generation` is the generation this call is AUTHORISED to close
    (see `current_state_generation`). When it no longer matches, the state
    this request was issued against is already gone and something else is
    installed -- closing that would take a live feature offline on behalf
    of a request that is no longer about it -- so the call is refused and
    nothing is touched. Omitting it (the 47 direct UI-thread calls, and app
    shutdown) means "close whatever is current", which is race-free
    precisely because `_state` is UI-thread-only. The seam path never omits
    it: `run_build_and_swap` passes it positionally, so a seam that has not
    been taught to accept it raises TypeError on the first build rather
    than silently reverting to the unguarded behaviour."""
    global _state, _state_generation
    if expect_generation is not None and expect_generation != _state_generation:
        return False
    if _state is not None:
        current = _state

        def _clear():
            global _state, _state_generation
            _state = None
            _state_generation += 1
        if not _release_live_index(current.index, _clear):
            return False
        gc.collect()
    _reset_freshness()
    return True


def passage_available() -> bool:
    return _state is not None


# ---------------------------------------------------------------------------
# Reader leases: CALL-SCOPED, not object-scoped. `get_passage_searcher()`
# hands the caller a `PassageSearchAdapter` that holds neither a
# `PassageIndex` nor a `PassageSearcher` -- only the (text_fetcher, width,
# length, depth) needed to build one. A lease is acquired, resolved, used,
# and released entirely INSIDE one `search_composition_logic()` call; no
# reference to the index survives that call's return, so there is nothing
# for a caller to hold onto, capture in a closure, or leak past a `with`
# block that no longer exists. `close_passage_state()`/`install_passage_
# state()` must still know such a call is in flight before they force-close
# that SAME index's memmaps -- a concurrent read off a mapping closed out
# from under it is an access violation, not a catchable exception -- so the
# lease bookkeeping below is unchanged; only WHO holds the lease (a call
# frame, not a returned object) is different.
#
# ONE lock (`_lease_lock`) is the mutual-exclusion point for everything
# below: the outstanding-reader count, the "draining" flag, AND every read
# of `_state` a lease acquisition performs. That last part is what makes
# acquisition atomic with the state check -- `_try_acquire_lease` observes
# `_state` and increments the count under the SAME critical section, so a
# lease is either fully issued (both happened) or not issued at all. Once
# `_begin_drain()` has run, the same lock refuses every new acquisition
# until `_end_drain()` runs, so nothing can slip in between "count observed
# zero" and "mappings torn down" -- see `_release_live_index`.
# ---------------------------------------------------------------------------

LEASE_DRAIN_TIMEOUT_SECONDS = 5.0
LEASE_DRAIN_POLL_SECONDS = 0.05

_lease_lock = threading.Lock()
_outstanding_leases = 0
_draining = False


def _try_acquire_lease():
    """The one atomic acquisition step: `_state` is read and the count is
    incremented under the SAME lock, so no other thread ever observes a
    count that was bumped for a state nobody checked, or a state that was
    checked but never counted. Returns the live `PassageIndex` on success,
    or None -- exactly like "no index installed" -- when either no state is
    installed or a close/swap has already called `_begin_drain()`; a
    request arriving during a drain is refused outright rather than being
    allowed to slip through before the teardown it's racing against."""
    global _outstanding_leases
    with _lease_lock:
        if _state is None or _draining:
            return None
        _outstanding_leases += 1
        return _state.index


def _release_lease() -> None:
    global _outstanding_leases
    with _lease_lock:
        # Floored at 0 rather than trusting callers to release exactly
        # once -- `PassageSearchAdapter.search_composition_logic`'s
        # `finally` calls this exactly once per acquired call, but the
        # floor is what keeps a defect there from ever wedging this count
        # below zero and refusing every close forever.
        if _outstanding_leases > 0:
            _outstanding_leases -= 1


def _leases_outstanding() -> bool:
    with _lease_lock:
        return _outstanding_leases > 0


def _begin_drain() -> None:
    global _draining
    with _lease_lock:
        _draining = True


def _end_drain() -> None:
    """Always the matching call to `_begin_drain()`, success or failure --
    a refused close/swap must leave the no-new-leases flag exactly as it
    found it (cleared), or the NEXT attempt would refuse every acquisition
    for no reason."""
    global _draining
    with _lease_lock:
        _draining = False


def _wait_for_leases_to_drain(timeout: Optional[float] = None) -> bool:
    """Polls rather than blocking on a condition variable: a lease is
    released from whichever worker thread's query happens to finish, an
    arbitrary and unpredictable thread from this function's point of view,
    so there is no single event object it could wait on instead. Must only
    be called with draining already begun -- see `_release_live_index` --
    or a lease could be acquired between one poll and the next forever.

    The window is read at CALL time and never bound as a default: a default
    is evaluated at import, which would leave the module-level tunable
    unpatchable and let a caller that shrank it silently keep waiting the
    production window."""
    if timeout is None:
        timeout = LEASE_DRAIN_TIMEOUT_SECONDS
    deadline = time.monotonic() + timeout
    while _leases_outstanding():
        if time.monotonic() >= deadline:
            return False
        time.sleep(LEASE_DRAIN_POLL_SECONDS)
    return True


def _release_live_index(idx: PassageIndex, on_drained: Callable[[], None]) -> bool:
    """The one seam `close_passage_state` and `install_passage_state` both
    route their outgoing-index release through. Draining starts BEFORE the
    wait so no new lease can be issued against `idx` while this function is
    deciding its fate, and stays in effect through `on_drained` -- the
    caller's `_state` mutation -- so a reader can never observe `_state`
    still pointing at `idx` after its mappings are already closed: teardown
    and the pointer swap happen inside the SAME no-new-leases window, not
    as two separately-racy steps. Returns True (mappings closed, `_state`
    mutated) or False (leases survived the wait, nothing touched) -- there
    is no partial outcome either way."""
    _begin_drain()
    try:
        if not _wait_for_leases_to_drain():
            return False
        _release_index_handles(idx)
        on_drained()
        return True
    finally:
        _end_drain()


class PassageSearchError(RuntimeError):
    """Raised by `PassageSearchAdapter.search_composition_logic()` in place
    of WHATEVER exception actually failed inside the lease -- see that
    method's docstring for why the original is never allowed to cross the
    boundary as an object. The message carries the original type name and
    `str()`, so nothing diagnostic is lost to the user-facing string; the
    full traceback goes to `logger` instead, at the point of conversion.
    `RuntimeError` subclass, so `CompositionThread.run()`'s (gui_threads.py)
    existing bare `except Exception as e: self.error_signal.emit(str(e))`
    catches this exactly like it caught the original."""


class PassageSearchUnavailableError(PassageSearchError):
    """Raised by `PassageSearchAdapter.search_composition_logic()` when no
    passage index is installed at CALL time -- before any lease is taken, so
    it never goes through the boundary conversion above (there is no inner
    traceback to sever). Catchable -- unlike the access violation a stale
    index reference would raise, which is exactly what this whole
    call-scoped design exists to make structurally impossible."""


class PassageSearchAdapter:
    """What `get_passage_searcher()` returns -- ALWAYS, never None. Holds
    only the per-call configuration (`text_fetcher`, `width`, `length`,
    `depth`); no index, no `PassageSearcher`, nothing that pins a memmap.
    `__slots__` rules out a stray instance attribute -- set by this class or
    smuggled in from outside -- ever holding one instead.

    Every `search_composition_logic()` call is a complete, self-contained
    lease: acquire (`_try_acquire_lease`), resolve the CURRENT index, build
    a fresh `PassageSearcher` around it, run the query, release
    (`_release_lease`) in a `finally` -- all within that one call, never
    spanning two. Nothing produced by one call (the index, the searcher, a
    result) is retained afterward, so there is no lifetime here for a
    caller to respect and nothing to capture that would still work, or
    still be dangerous, later.

    That is what makes capturing the BOUND METHOD safe: `thread.searcher =
    get_passage_searcher(text_fetcher)` followed, an hour later, by
    `thread.searcher.search_composition_logic(...)` takes a fresh lease
    against whatever state is current AT THAT CALL -- the same index, a
    replaced one, or none at all (`PassageSearchUnavailableError`, not a
    stale pointer). Contrast the previous handle-based design, where
    `m = handle.search_composition_logic` captured a BOUND METHOD OF THE
    UNDERLYING SEARCHER, escaping the handle's own `__getattr__` guard
    entirely and staying callable after the lease that produced it ended --
    this design has no bound method to escape with, because the searcher
    that would own it is never exposed.

    EXCEPTION BOUNDARY: `search_composition_logic` is also a hard boundary
    on the way OUT. A lease is acquired and the index it hands out is
    reachable from local variables in THIS frame and in every frame of the
    call it makes into `PassageSearcher.search_composition_logic` -- and
    Python keeps every one of those frames alive for as long as anything
    holds the exception's traceback (`tb_next` chains the callee frames onto
    the raising frame). A caller that only lets the exception fall out of
    scope is fine; one that RETAINS it -- `logging.exception(...)`, a stored
    variable, `pytest.raises(...) as excinfo` then touching `excinfo.value`,
    a post-mortem debugger -- keeps the index reachable past
    `close_passage_state()`, and dereferencing it after that is an access
    violation, not a catchable error. So nothing that failed inside the
    lease is allowed to leave as the object it failed as: it is caught,
    formatted to a STRING (`traceback.format_exc()`, which holds no frames),
    logged, and re-raised as a fresh `PassageSearchError` only after the
    whole try/except/finally has completed -- never from inside the
    `except`, which would set `__context__` to the original and drag its
    traceback back in anyway. A `KeyboardInterrupt`/`SystemExit` is the one
    exception NOT converted: converting it to `PassageSearchError` would
    make it catchable by an ordinary `except Exception` and swallow shutdown
    semantics, so a fresh instance of the SAME class is raised instead --
    same type, no frames.

    Consequence worth stating plainly: a `progress_callback` that raises to
    signal cancellation would also be caught and converted here, same as any
    other failure inside the lease -- `PassageSearcher` never actually calls
    `progress_callback` today, so nothing relies on that idiom, but a future
    caller must not assume otherwise.
    """

    __slots__ = ('_text_fetcher', '_width', '_length', '_depth',
                 '_render_cap')

    def __init__(self, text_fetcher, width: str, length: str,
                 depth: str, render_cap: Optional[int] = None) -> None:
        self._text_fetcher = text_fetcher
        self._width = width
        self._length = length
        self._depth = depth
        # None => let PassageSearcher apply its own default. See
        # `get_passage_searcher` for why 0 is not the same as "no cap given".
        self._render_cap = render_cap

    def search_composition_logic(
        self,
        full_text: str,
        chunk_size: int = 5,
        max_freq: float = 100.0,
        mode: str = 'exact',
        *,
        filter_text: Optional[str] = None,
        progress_callback=None,
        boundary_mode: str = 'full',
        boundary_delimiter: str = '\n',
        boundary_boost: float = 1.5,
        min_boundary_matches: int = 0,
        min_delimiter_distance: int = 3,
        restrict_sys_ids=None,
        corpus_scope: str = 'genizah',
    ) -> dict:
        """Same parameter list as `gui_threads.CompositionThread.run()`'s
        call to `self.searcher.search_composition_logic(...)` -- the
        contract this adapter exists to preserve unchanged. Deliberately
        does NOT accept `witnesses`/`witness_text_cap`:
        `PassageSearcher.search_composition_logic` treats those as
        keyword-only and additive, but `CompositionThread` never passes
        them, and a signature that omitted them would silently swallow a
        typo'd caller into `**_ignored` on the far side instead of failing
        loudly here with a `TypeError`.
        """
        index = _try_acquire_lease()
        if index is None:
            raise PassageSearchUnavailableError(
                'no passage index is installed -- passage search is not '
                'available right now')
        searcher = None
        # Set only when something failed inside the lease -- a STRING
        # (formatted traceback + original type name + original str()), never
        # the exception object itself, so nothing below can hold a frame
        # from inside the call. Read and acted on only AFTER the
        # try/except/finally below has fully unwound -- see the class
        # docstring's "EXCEPTION BOUNDARY" section for why raising from
        # inside `except` would defeat the whole point via `__context__`.
        boundary_failure = None
        boundary_reraise = None
        try:
            try:
                from shared.passage_parallels import PassageSearcher  # local: keep this module import-light
                # Omitted when None so the searcher's own default applies:
                # passing `render_cap=None` explicitly would override the
                # dataclass default with None and crash the cap comparison.
                _cap_kw = ({} if self._render_cap is None
                           else {'render_cap': self._render_cap})
                searcher = PassageSearcher(
                    index=index, text_fetcher=self._text_fetcher,
                    policy=compose(self._width, self._length, self._depth),
                    **_cap_kw)
                return searcher.search_composition_logic(
                    full_text, chunk_size, max_freq, mode,
                    filter_text=filter_text, progress_callback=progress_callback,
                    boundary_mode=boundary_mode,
                    boundary_delimiter=boundary_delimiter,
                    boundary_boost=boundary_boost,
                    min_boundary_matches=min_boundary_matches,
                    min_delimiter_distance=min_delimiter_distance,
                    restrict_sys_ids=restrict_sys_ids,
                    corpus_scope=corpus_scope)
            except Exception as exc:
                boundary_failure = (
                    traceback.format_exc(), type(exc).__name__, str(exc))
            except BaseException as exc:
                # KeyboardInterrupt / SystemExit / GeneratorExit: must NOT
                # become a `PassageSearchError` -- that would make it
                # catchable by an ordinary `except Exception` and swallow
                # shutdown semantics. A FRESH instance of the SAME class
                # carries the type across the boundary without carrying any
                # frame: `type(exc)(*exc.args)` reconstructs it from plain
                # data, never from `exc` itself.
                logger.error(
                    'passage search interrupted by %s inside the lease '
                    'boundary:\n%s', type(exc).__name__, traceback.format_exc())
                boundary_reraise = type(exc)(*exc.args)
        finally:
            # Scrub BOTH names this frame holds a live PassageIndex through
            # -- `index` (this frame's own local) and `searcher` (whose
            # `.index` attribute is the same object) -- before the lease is
            # released. Necessary but not sufficient on its own: the
            # original exception's OWN frames (inside
            # `PassageSearcher.search_composition_logic` and deeper) are
            # never retained in the first place, because `boundary_failure`
            # above captured only strings, not the exception object those
            # frames hung off of.
            searcher = None
            index = None
            _release_lease()

        if boundary_reraise is not None:
            raise boundary_reraise
        if boundary_failure is not None:
            tb_text, exc_type_name, exc_str = boundary_failure
            logger.error(
                'passage search failed inside the lease boundary '
                '(%s: %s) -- original traceback:\n%s',
                exc_type_name, exc_str, tb_text)
            message = f'{exc_type_name}: {exc_str}' if exc_str else exc_type_name
            raise PassageSearchError(
                f'passage search failed -- {message}')
        # Unreachable: the inner try either returns, or one of the two
        # except clauses above sets exactly one of the two markers checked
        # above. Left unguarded (no `else`/final raise) rather than papered
        # over with an `assert False`, so a future edit that breaks this
        # invariant fails loudly as "function did not return" instead of
        # silently returning None as a passage result.


def get_passage_searcher(text_fetcher, width: str = _DEFAULT_WIDTH,
                          length: str = DEFAULT_LENGTH,
                          depth: str = DEFAULT_DEPTH,
                          render_cap: Optional[int] = None
                          ) -> PassageSearchAdapter:
    """The one obvious way to get something searchable. Returns a
    `PassageSearchAdapter` unconditionally -- whether or not an index is
    installed right now -- because the adapter holds no index itself; use
    it as:

        pl.get_passage_searcher(text_fetcher).search_composition_logic(...)

    A call made while no index is installed (or one made later, against an
    index that has since been closed and never replaced) raises
    `PassageSearchUnavailableError` from inside `search_composition_logic`
    itself -- a clear, catchable error at the point that actually needed the
    index, not a None this function would have returned for a caller to
    forget to check.

    Each axis is validated INDEPENDENTLY against the finite set `compose()`
    actually knows, falling back to the default on anything unrecognised --
    `compose`/`get_preset` raise on an unknown name, which is correct for a
    caller passing a literal, but wrong for a value that came out of a
    persisted settings file a user could have hand-edited or that predates a
    preset being renamed.

    `render_cap` is THREE-valued, and the distinction is load-bearing:

    * `None` (default) -- say nothing, and `PassageSearcher` applies its own
      `PARALLELS_GROUP_CAP` (200). This is the single-witness desktop search,
      unchanged from v9.0.0.
    * `0` -- UNCAPPED, for the MULTI-WITNESS path. Each witness must come back
      whole, because the cap has to be applied ONCE to the FUSED list. Capping
      each witness first fuses N already-truncated lists and silently drops
      every contributor that sat past rank 200 in its own witness -- which is
      exactly where a rare witness of a widely-copied work shows up. The web
      does the same thing for the same reason
      (`web/pages/parallels.py`: `render_cap=0` on the seed and on every
      witness, then one `_cap_main_results_by_group` after the fusion).
    * any other int -- that cap, for a caller that knows what it wants.

    Note `0` is a real value, not a falsy stand-in for "unset": the caps are
    applied as `if self.render_cap and self.render_cap > 0`, so 0 means no cap
    while None means no opinion. Testing this parameter for truthiness
    anywhere would collapse the two.
    """
    if width not in PRESETS:
        width = _DEFAULT_WIDTH
    if length != DEFAULT_LENGTH and length not in LENGTH_PROFILES:
        length = DEFAULT_LENGTH
    if depth != DEFAULT_DEPTH and depth not in DEPTH_PROFILES:
        depth = DEFAULT_DEPTH
    return PassageSearchAdapter(text_fetcher, width, length, depth, render_cap)


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

    ANY failure of that re-fingerprint -- a hash cancel raising
    `BuildCancelled`, or an ordinary `OSError` from a source file that
    disappeared mid-build -- cleans staging before propagating, consistent
    with every other unswapped terminal in this subsystem.
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
    except BaseException:
        # NOT just BuildCancelled: re-reading the corpus can fail for
        # ordinary reasons (a source file removed, a drive unmounted, a
        # permission change) and every one of those escapes to the worker
        # as an error. Staging is a multi-GB artifact this function owns
        # until it either promotes or discards it, and nothing downstream
        # looks inside `passage_index.building` -- the startup recovery
        # walk explicitly does not -- so an escape without this cleanup
        # strands it on disk until some later build happens to clear it
        # (Codex review round 7, finding 3).
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
    # Re-checked immediately before the destructive rename, exactly like
    # `_swap_first_build` -- `perform_swap`'s entry check can pass and the
    # latch be set a moment later, and the rollback that used to be the
    # only answer here is not free: it is a SECOND rename that can itself
    # fail with PermissionError and leave `current/` missing. Not renaming
    # at all cannot fail (Codex review round 7, finding 2).
    if cancelled():
        _cleanup_staging(staging_dir)
        return SwapResult(status='cancelled')

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


# `release_live_state` is an ARBITRARY caller-supplied callable (in
# production, a blocking queued Qt signal/slot hop to the UI thread) called
# below with the cross-process lock already held -- unlike
# `_wait_for_leases_to_drain` above, which polls a counter THIS module owns,
# there is nothing here to poll: the seam's insides are opaque, so the only
# way to bound it is to run it on its own thread and stop waiting on this
# one if it does not return in time. Larger than `LEASE_DRAIN_TIMEOUT_SECONDS`
# on purpose -- `close_passage_state()` (the real seam in production) has
# its own bounded wait for readers to drain, and that inner wait must have
# room to finish normally before this OUTER bound gives up on the seam
# entirely.
UI_RELEASE_SEAM_TIMEOUT_SECONDS = 15.0

# Distinct from every value `release_live_state` itself is documented to
# return (`True`, `False`, `None` -- see `run_build_and_swap`'s docstring)
# so a caller can never mistake a real seam outcome for a timeout.
_RELEASE_SEAM_TIMED_OUT = object()


def _call_release_seam(release_live_state: Callable[..., Optional[bool]],
                       expect_generation: int,
                       timeout: Optional[float] = None):
    """Runs `release_live_state()` on its own daemon thread and waits at
    most `timeout` seconds for it to return, so a seam whose event loop
    never answers -- the UI thread is wedged, or simply gone -- cannot hang
    THIS thread forever. This thread is a worker holding the cross-process
    lock inside `run_build_and_swap`'s try/finally, so a hang here would
    strand that lock across every future build and startup recovery, in
    every process, until this one is killed.

    On timeout the seam thread is simply abandoned (it cannot be killed
    from here); if it eventually does call back into `close_passage_state`
    for real, that happens unseen by this function and by whatever already
    gave up waiting on it. It cannot, however, do any DAMAGE unseen: the
    seam is handed `expect_generation` and must forward it, so a late close
    lands on a generation check that refuses it (see
    `current_state_generation`).

    Returns the seam's own return value on a normal return within
    `timeout`, or the sentinel `_RELEASE_SEAM_TIMED_OUT` on timeout.
    Whatever the seam itself raises is re-raised here once joined -- a
    seam is expected to release cleanly, not fail, so a failure is a real
    bug this must not swallow.

    `timeout` is read from the module tunable `UI_RELEASE_SEAM_TIMEOUT_
    SECONDS` at CALL time, exactly like `_wait_for_leases_to_drain` above
    and for the same reason: a default bound at import time would leave
    the tunable unpatchable by a test that shrinks it.
    """
    if timeout is None:
        timeout = UI_RELEASE_SEAM_TIMEOUT_SECONDS
    outcome: list = []

    def _run() -> None:
        try:
            # Positional, and NOT optional: a seam that has not been taught
            # the generation contract fails loudly here on the first build
            # instead of quietly closing the wrong generation later.
            outcome.append(('return', release_live_state(expect_generation)))
        except BaseException as exc:  # forwarded below, never swallowed
            outcome.append(('raise', exc))

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return _RELEASE_SEAM_TIMED_OUT
    kind, value = outcome[0]
    if kind == 'raise':
        raise value
    return value


def run_build_and_swap(root: str, records, source_paths: list,
                       corpus_path: str, *, construction: str = 'spool',
                       partitions: int = DEFAULT_PARTITIONS,
                       batch_grams: int = DEFAULT_BATCH_GRAMS,
                       df_cap: Optional[int] = None, corpus_label: str = '',
                       progress: Optional[Callable] = None,
                       cancel_check: Optional[Callable[[], bool]] = None,
                       release_live_state: Callable[..., Optional[bool]]
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
    directly since nothing in a test is actually multi-threaded. The seam
    receives ONE argument -- the state generation it is authorised to close
    -- and must forward it to `close_passage_state`, which is what stops an
    abandoned seam from closing a later index (see `current_state_
    generation`). Passing `close_passage_state` itself as the seam gets
    this right by construction. Required,
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

    The seam is called through `_call_release_seam`, which bounds it to
    `UI_RELEASE_SEAM_TIMEOUT_SECONDS`: a THIRD outcome, on top of the
    `True`/`False`/`None` triple above, is the seam simply never returning
    (a UI thread that is wedged, or gone). That is reported as status
    `'release_timed_out'` -- deliberately NOT `'readers_active'`, which
    means something different (a lease was seen and refused to drain in
    time; here, nothing about the release outcome is even known) -- and
    handled exactly like a refusal: staging is cleaned and `perform_swap`
    is never called, since nothing was proven released.

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
        # refusal. Bounded (`_call_release_seam`) because this thread holds
        # the cross-process lock through the whole `try` -- a seam that
        # never answers must not be allowed to hold it forever.
        released = _call_release_seam(release_live_state,
                                      current_state_generation())
        if released is _RELEASE_SEAM_TIMED_OUT:
            _cleanup_staging(staging)
            return BuildAndSwapResult(status='release_timed_out')
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


# MEDIUM-2: knobs for the ONE re-open `_recover_at_startup` gives `current/`
# immediately before it would otherwise be destroyed as unopenable. Wider
# than `_open_with_retry`'s plain default (2 immediate attempts) on purpose
# -- two consecutive access-denied errors from a virus scanner is an
# ordinary Windows event, not proof of corruption, and real time between
# attempts (not just more of them back-to-back) is what tells the two
# apart in practice.
CURRENT_DESTROY_RECONFIRM_ATTEMPTS = 5
CURRENT_DESTROY_RECONFIRM_BACKOFF_SECONDS = 0.3


def _confirm_promotion(root: str, live: str,
                        status: str) -> Optional[RecoveryResult]:
    """One more REAL open of a candidate this function already validated
    in place and renamed into `current/` -- used both when a later
    candidate is about to move `live` aside (working content earns another
    look before it is discarded for a merely-ALSO-valid alternative) and,
    if nothing older pans out either, as recovery's last resort before it
    would otherwise report nothing_recoverable. `open_index` swallows a
    transient access error into None exactly like a genuinely bad artifact
    (see `_open_with_retry`'s docstring); this call happens strictly later
    than the immediate post-rename check, giving a lagging antivirus scan
    or Explorer handle more real time to let go. None means still no."""
    idx = _open_with_retry(live)
    if idx is None:
        return None
    if status == 'recovered_from_prev':
        _delete_prev_generations(root)
    return RecoveryResult(status=status, index=idx, live_dir=live)


def _recover_at_startup(root: str) -> RecoveryResult:
    live = os.path.join(root, LIVE_DIRNAME)
    # Set once a `_prev-*`/`.failed-*` candidate has been validated in place
    # and renamed into `current/` but its immediate reopen failed -- the
    # promoted content is now the only thing sitting at `live`, and it must
    # be RE-CONSIDERED (via `_confirm_promotion`) rather than silently
    # abandoned the moment this function looks anywhere else.
    pending_status = None
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
            if pending_status is not None:
                confirmed = _confirm_promotion(root, live, pending_status)
                if confirmed is not None:
                    return confirmed
            else:
                # `live` here is the ORIGINAL `current/`: it never went
                # through a promotion (that case is `pending_status`
                # above), it simply failed `_open_with_retry`'s two
                # immediate attempts at the top of this loop. Validation
                # -first recovery's own rule -- nothing is deleted until a
                # candidate has passed a REAL open_index -- extends here to
                # the thing about to be DESTROYED: it must be re-proven
                # unopenable too, with more attempts and real backoff time
                # between them, not just the two immediate ones already
                # spent. Two consecutive access-denied errors from a virus
                # scanner is an ordinary Windows event, not proof `current/`
                # is actually corrupt.
                reconfirmed = _open_with_retry(
                    live, attempts=CURRENT_DESTROY_RECONFIRM_ATTEMPTS,
                    backoff=CURRENT_DESTROY_RECONFIRM_BACKOFF_SECONDS)
                if reconfirmed is not None:
                    _delete_prev_generations(root)
                    return RecoveryResult(status='live_ok',
                                          index=reconfirmed, live_dir=live)
            # `live` exists and nothing above proved it (a still-pending
            # promotion, or `current/` itself under the re-confirmation
            # just above, both failed their recheck too) -- move it aside
            # first; a generation cannot be renamed onto an existing
            # directory.
            dead = os.path.join(root, f'.dead-{_gen_token()}')
            try:
                _retry_rename(live, dead)
            except PermissionError:
                continue  # try the next older candidate instead
            shutil.rmtree(dead, ignore_errors=True)
            pending_status = None

        try:
            _retry_rename(path, live)
        except PermissionError:
            continue

        idx2 = _open_with_retry(live)
        if idx2 is None:
            # Transient or real -- either way `path`'s content is NOW
            # sitting at `live`, already validated in place moments ago.
            # Keep walking older candidates as a fallback, but remember
            # this promotion so it gets reconsidered instead of discarded.
            pending_status = 'recovered_from_prev' if kind == 'prev' \
                else 'recovered_from_quarantine'
            continue

        if kind == 'prev':
            _delete_prev_generations(root)
        # a promoted quarantine is consumed, not retained -- nothing more
        # to do for the 'failed' case.
        status = 'recovered_from_prev' if kind == 'prev' \
            else 'recovered_from_quarantine'
        return RecoveryResult(status=status, index=idx2, live_dir=live)

    if pending_status is not None:
        # Every candidate ran out, but the LAST promotion is still sitting
        # in `current/` -- reporting nothing_recoverable here would send
        # the UI toward rebuilding gigabytes that, in fact, already work.
        confirmed = _confirm_promotion(root, live, pending_status)
        if confirmed is not None:
            return confirmed

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
