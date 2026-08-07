# -*- coding: utf-8 -*-
"""Admin suppression of individual computed identifications (Phase 136).

WHY THIS EXISTS
---------------
Owner ruling, 2026-08-06: a clearly-wrong row must come off the live beta
immediately, "not going through long process". This is that mechanism, and it is
deliberately the smallest thing that does the job: a list of identification ids,
an admin-only ✕ that adds one, and a filter that drops them from the query.

WHY IT IS NOT IN THE ARTIFACT
-----------------------------
``web/discovery_assets.py`` verifies the sidecar's SHA-256 against the manifest's
``content_hash`` and refuses to serve on mismatch, so changing one byte of the
served ``.db`` makes the whole findings page clean-hide. The artifact already
carries ``assertion_visibility`` / ``identity_visibility`` columns that would be
the natural home for this -- and they are unreachable without a re-bake, a new
hash and a fresh 393 MB upload per takedown. So the hide list lives in Supabase,
outside the verified bytes, and is passed INTO the query at request time.

WHY THE COUNTS FOLLOW
---------------------
Owner ruling: suppressed rows come out of the totals too, not just the list. The
id set is handed to ``shared.discovery_service._build_findings_filter``, so it
lands in the SQL ``WHERE`` -- and ``total``, the pager and every facet count are
built from that one predicate, so all three drop by exactly what was hidden.
Filtering a fetched page instead would leave those three describing a population
the reader is not being shown, which is the defect CLAUDE.md names for a capped
total: "a capped total reported as exact is a correctness defect".

The LAUNCH HEADLINE figures deliberately do NOT move. They are corpus figures on
ruling U's fixed basis, read by a separate query, and a corpus figure that tracked
an admin's hide list would stop being a corpus figure. Owner confirmed.

FAIL OPEN
---------
If the Supabase read fails, this returns an EMPTY set -- i.e. everything is
shown. Owner-confirmed, and the opposite is defensible, so the reasoning is
recorded: the alternative is that a Supabase hiccup blanks the entire findings
page for every reader, which is a far worse failure than briefly showing one row
the owner wanted hidden. This is a cosmetic takedown list, not a security
boundary. What IS a security boundary is the RLS policy on the table: a non-admin
cannot insert, whatever the UI shows them.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, FrozenSet, Optional, Tuple

logger = logging.getLogger(__name__)

#: The Supabase table. One row per suppressed identification.
TABLE = "discovery_suppressed"

#: How long a fetched hide list is reused, in seconds.
#:
#: A page of 50 rows must not issue 50 reads, and the findings page renders the
#: filter on every refresh (every filter change, every page turn) -- so without a
#: cache this would put a Supabase round trip on the critical path of every
#: interaction, on a server with ONE uvicorn worker.
#:
#: 30s rather than a longer TTL because the owner's workflow is "click ✕, confirm
#: it is gone": a stale window measured in minutes would look like the button did
#: not work. `invalidate()` is called on every write, so the reader who clicked
#: sees the change immediately and 30s only bounds how long ANOTHER visitor's
#: process keeps the old list.
CACHE_TTL_SECONDS = 30.0

#: `(fetched_at, ids, ok)`. Module-level and lock-guarded: the cache is shared by
#: every request in the process, which is the point. `ok` records whether the read
#: SUCCEEDED, so a failure can expire faster than a good list without the two
#: being confused -- an empty set means "nothing hidden" on a success and "we
#: could not tell" on a failure, and those deserve different retry behaviour.
_CACHE: Optional[Tuple[float, FrozenSet[str], bool]] = None
_LOCK = threading.Lock()


#: How long a FAILED read is remembered before retrying, in seconds.
#:
#: Shorter than `CACHE_TTL_SECONDS` because the two states are different
#: promises: a successful list is good for its whole TTL, while a failure is a
#: statement about one instant that should not persist. But it must be cached for
#: SOME interval, and that is the load-bearing part -- without it every render
#: during a Supabase outage pays a fresh failed round trip (measured at ~1.9s
#: with no credentials configured), which turns a degraded dependency into a slow
#: page for everyone.
FAILURE_TTL_SECONDS = 5.0

def _supabase_is_configured() -> bool:
    """Whether there are credentials to call Supabase with, at all.

    Read from the shared provider rather than the client module, so this needs no
    client build to answer -- which is the point: it is the cheap check that keeps
    an unconfigured process from paying for an expensive failure.
    """
    try:
        from shared.supabase_provider import get_anon_key, get_url

        return bool(get_url() and get_anon_key())
    except Exception:  # noqa: BLE001 -- an unimportable provider is "not configured"
        return False


def invalidate() -> None:
    """Drop the cached hide list, so the next read re-fetches.

    Called after every write. Without it the admin who just clicked ✕ would keep
    seeing the row for up to `CACHE_TTL_SECONDS` and reasonably conclude the
    button is broken.
    """
    global _CACHE
    with _LOCK:
        _CACHE = None


def _fetch_ids() -> Tuple[FrozenSet[str], bool]:
    """Read the hide list from Supabase. Returns an EMPTY set on any failure.

    `get_client()` (the anon client) rather than `get_user_client()`: the SELECT
    policy is `TO public USING (true)`, because the filter has to apply to every
    visitor including anonymous ones -- a hidden row must be hidden for everyone,
    not only for whoever is logged in. Reading with the user's client would add a
    per-request client build for identical rows.
    """
    if not _supabase_is_configured():
        # NO NETWORK CALL AT ALL when there is nothing to call. Without this the
        # first render in any process with no Supabase credentials pays a full
        # client build plus a failed round trip -- measured at ~1.9s -- before
        # failing open. That is the desktop-adjacent case, the CI case, and every
        # test that renders this page: they were each burning two seconds to be
        # told what the environment already knew.
        return frozenset(), False
    try:
        from web.supabase_client import get_client

        client = get_client()
        if client is None:
            return frozenset(), False
        response = client.table(TABLE).select("identification_id").execute()
        return frozenset(
            str(row["identification_id"])
            for row in (getattr(response, "data", None) or [])
            if row.get("identification_id")
        ), True
    except Exception as exc:
        # FAIL OPEN, and say so at WARNING rather than swallowing it: the page
        # stays fully usable, but an operator needs to know the hide list is not
        # being applied. The exception TYPE and message are ours (a Supabase
        # client error), not artifact-derived, so logging them is not a D-25
        # egress.
        logger.warning(
            "discovery suppression list unavailable (%s: %s) -- rendering "
            "everything; hidden rows may reappear until this recovers",
            type(exc).__name__, exc)
        return frozenset(), False


def suppressed_ids() -> FrozenSet[str]:
    """The identification ids currently hidden, cached for `CACHE_TTL_SECONDS`.

    SYNCHRONOUS and NETWORK-BOUND, so callers on the findings page must reach it
    through an off-loop wrapper -- `web/discovery.py` does. Calling this directly
    from a page coroutine would put a Supabase round trip on the single uvicorn
    event loop, which stalls every concurrent request while burning no CPU (the
    exact failure `web/perf_watch.py` exists to catch).
    """
    global _CACHE
    now = time.monotonic()
    with _LOCK:
        cached = _CACHE
        if cached is not None:
            age = now - cached[0]
            ttl = CACHE_TTL_SECONDS if cached[2] else FAILURE_TTL_SECONDS
            if age < ttl:
                return cached[1]
    # FETCHED OUTSIDE THE LOCK: the read is network-bound, and holding the lock
    # across it would serialise every concurrent request behind one round trip.
    # A concurrent duplicate fetch is harmless (same answer, idempotent write of
    # the cache); a lock held across I/O is not.
    ids, ok = _fetch_ids()
    with _LOCK:
        _CACHE = (time.monotonic(), ids, ok)
    return ids


def suppress(identification_id: str, client: Any = None) -> bool:
    """Hide one identification. Returns whether it is now hidden.

    ``client`` IS EFFECTIVELY REQUIRED, and passing it is the whole fix for a real
    defect this shipped with (owner report, 2026-08-07: *"new row violates
    row-level security policy"*).

    THE TRAP, in full, because I documented it three lines away and then walked
    into it anyway. This function runs in a THREAD POOL worker (via
    ``bounded_io_bound``). ``get_user_client()`` starts by reading
    ``safe_user_get('auth_session')``, and ``app.storage.user`` is contextvar-
    scoped -- so off the event loop it raises "can only be used within a UI
    context", which ``safe_user_get`` catches and turns into ``{}``.
    ``get_user_client()`` then finds no tokens and returns the ANONYMOUS
    singleton, logging at INFO. The insert therefore arrives with no
    ``auth.uid()``, and the admin ``WITH CHECK`` policy refuses it -- correctly.

    Every layer behaved exactly as designed, which is what made it invisible:
    nothing raised, nothing warned, and the only symptom was Postgres rejecting a
    write the admin was entitled to make.

    So the client is built ON THE LOOP by the caller (``web/discovery.py``) and
    passed in. This is the same explicit-``client=`` shape v8.5.2 introduced in
    ``web/pages/corrections.py`` after the identical failure degraded user-scoped
    READS to anonymous; there it silently returned empty results, here it
    silently loses a write. Recorded in ``reference_io_bound_safe_storage_trap``.

    ``None`` is REFUSED rather than falling back to ``get_user_client()``: the
    fallback is precisely what fails here, and a "convenient" default would
    restore the bug for any future caller that forgets the argument.

    IDEMPOTENT via the primary key: an ``upsert`` on a row that is already hidden
    updates it instead of erroring, so a double-click reads as success -- the
    requested state and the actual state agree.
    """
    if not identification_id:
        return False
    if client is None:
        # LOUD, not a silent anonymous fallback. See the docstring: the fallback
        # is the defect.
        logger.error(
            "suppress() was called with no client. It runs off the event loop, "
            "where get_user_client() cannot read the session and degrades to "
            "anonymous -- which RLS refuses. Build the client on the loop and "
            "pass it in.")
        return False
    try:
        client.table(TABLE).upsert(
            {"identification_id": str(identification_id)},
            on_conflict="identification_id",
        ).execute()
        invalidate()
        return True
    except Exception as exc:
        logger.error("could not suppress a computed identification (%s: %s)",
                     type(exc).__name__, exc)
        return False


def unsuppress(identification_id: str, client: Any = None) -> bool:
    """Un-hide one identification. Returns whether the delete was accepted.

    The undo path, and the reason no confirmation dialog guards `suppress`: the
    action is one click to make and one click to reverse, so a dialog would cost
    more than the mistake. Admin-gated at the database by the DELETE policy.

    ``client`` is required for exactly the reason ``suppress``'s is -- see there.
    A DELETE under the anonymous client is refused by the same policy shape, so
    this would have failed identically the first time it was used.
    """
    if not identification_id:
        return False
    if client is None:
        logger.error(
            "unsuppress() was called with no client -- see suppress() for why "
            "the off-loop anonymous fallback cannot work.")
        return False
    try:
        client.table(TABLE).delete().eq(
            "identification_id", str(identification_id)).execute()
        invalidate()
        return True
    except Exception as exc:
        logger.error("could not un-suppress a computed identification (%s: %s)",
                     type(exc).__name__, exc)
        return False


__all__ = [
    "CACHE_TTL_SECONDS",
    "TABLE",
    "invalidate",
    "suppress",
    "suppressed_ids",
    "unsuppress",
]
