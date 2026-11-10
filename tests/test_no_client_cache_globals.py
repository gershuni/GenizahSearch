"""Runtime attr-absence test -- Phase 90 D-16 permanent CI guard.

Parametrized over the 6 deleted module-level names. Asserts each is
absent from `web.supabase_client` at runtime -- reintroducing any of
them (even at a different line or under a different docstring) trips
the test immediately, forcing a deliberate decision rather than a
silent regression.

Mirrors Phase 89 D-11 (tests/test_no_user_lists_mgr_field.py). The
parametrization shape is identical; only the DELETED_GLOBALS list
differs (6 names instead of 3).

Codex F1 + F3: the cache plumbing made cross-user authenticated-client
leakage possible via dict-cache lookup AND via the supabase event
listener mutating the singleton's Authorization header. Phase 90
removed both vectors -- the cache by deletion (Plan 90-02), the event
listener leak by D-10 throwaway-client refactor (Plan 90-01).

Test installation pattern (Phase 89 D-09 + R9, Codex review round 1 M5):
  - Task 2 of Plan 90-02 installs this test as a strict-xfail seed
    trap BEFORE the deletion happens. CI stays green: the assertion
    fails (because hasattr returns True for the still-existing names)
    and the strict-xfail marker converts that to an expected failure.
  - Task 4 of Plan 90-02 deletes the 4 globals + 2 helpers AND removes
    the strict-xfail marker in the SAME atomic commit. CI stays green:
    the test now passes for real (the assertion passes; no marker).
The literal pytest-mark substring is omitted from this docstring so
Task 4 acceptance grep (anchored to decorator-line start) does not
self-trip.
"""

import pytest

DELETED_GLOBALS = [
    '_client_cache',
    '_session_locks',
    '_locks_guard',
    '_CLIENT_CACHE_TTL',
    '_clear_stale_auth',
    '_prune_session_client_cache',
]


@pytest.mark.parametrize('name', DELETED_GLOBALS)
def test_attr_absent(name):
    """The named module-level attribute MUST NOT exist on web.supabase_client.

    AUTHC-01: the process-wide auth client cache and its plumbing
    (`_client_cache`, `_session_locks`, `_locks_guard`,
    `_CLIENT_CACHE_TTL`, `_prune_session_client_cache`) and the
    auth-resurrection guard (`_clear_stale_auth`) are all deleted by
    Plan 90-02. Reintroducing any of them must be a deliberate
    decision (delete this xfail/parametrize-entry with rationale),
    not a silent regression.
    """
    import web.supabase_client as mod
    assert not hasattr(mod, name), (
        f"{name} survived Phase 90 deletion. The cache + lock plumbing "
        "was removed; reintroducing it must be a deliberate decision, "
        "not a silent regression. If a future PR genuinely needs to "
        "re-cache authenticated clients, delete this test parameter "
        "with an explicit written rationale referencing the new "
        "cross-user-isolation guarantee."
    )
