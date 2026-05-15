"""Phase 89 D-11 — runtime attribute-absence regression tests.

Asserts the 3 fields deleted in Phase 89 cannot be accessed directly on
their respective instances:

  - AppState._user_lists_mgr             (LISTS-01 singleton)
  - UserListsManager._cache_entry         (LISTS-03 10s TTL tuple)
  - UserListsManager._cache_ttl           (LISTS-03 10s TTL constant)

The static AST guard (tests/test_no_deleted_lists_state_references.py)
catches `state._user_lists_mgr = ...` re-introductions at AST-scan time
even when the dynamic attribute would not yet have been read. This
runtime test catches the converse: someone re-introduces the field via
`__init__` body but the static scanner misses it (e.g., the field is
created via `setattr(self, name_built_from_concat, ...)` which the AST
walker doesn't statically resolve).

R9 (CI green throughout): `test_appstate_does_not_have_user_lists_mgr`
is marked `@pytest.mark.xfail(strict=True)` until Task 3 deletes the
field. Task 3 removes the marker in the SAME atomic commit as the
deletion (5-file commit boundary per R10).

See .planning/phases/89-lists-cache-per-request/89-CONTEXT.md D-11 and
89-REVIEWS.md R9.
"""
import pytest

from web.state import AppState
from web.user_lists import UserListsManager


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Phase 89 Task 3 deletion pending — _user_lists_mgr still exists on "
        "AppState until Task 3's atomic commit deletes the field. Task 3 "
        "removes this xfail marker in the SAME commit (R9 + R10)."
    ),
)
def test_appstate_does_not_have_user_lists_mgr():
    """Direct attribute access on AppState() for ``_user_lists_mgr`` is False.

    The AppState class is a __new__-based singleton, so AppState() returns
    the cached instance. We test hasattr (matching Phase 88 D-06 pattern).

    If a future PR re-introduces ``self._user_lists_mgr = None`` in
    AppState.init() OR sets the attribute dynamically anywhere before this
    test runs (e.g. fixture setup), this test will fail with a clear
    message naming the field.
    """
    instance = AppState()
    assert not hasattr(instance, '_user_lists_mgr'), (
        "AppState._user_lists_mgr still exists. Phase 89 LISTS-01 deleted "
        "this field; it must not be re-added directly or dynamically. "
        "See .planning/phases/89-lists-cache-per-request/89-CONTEXT.md D-01, D-11. "
        "If perf or convenience requires a server-side cache, the path "
        "forward is _session_uuid-keyed storage via safe_storage helpers "
        "(see CONTEXT.md Deferred Ideas)."
    )


def test_user_lists_manager_does_not_have_cache_entry():
    """Direct instantiation of UserListsManager produces no ``_cache_entry`` field.

    Defends against direct-instantiation regressions: someone re-adds
    ``self._cache_entry = ...`` in __init__ but the static AST scanner
    misses it (e.g., the field name is built via string concat or set
    through a metaclass).
    """
    mgr = UserListsManager(None, None)
    assert not hasattr(mgr, '_cache_entry'), (
        "UserListsManager._cache_entry still exists. Phase 89 LISTS-03 "
        "deleted the 10s TTL cache tuple; it must not be re-added. "
        "See CONTEXT.md D-03, D-11."
    )


def test_user_lists_manager_does_not_have_cache_ttl():
    """Direct instantiation of UserListsManager produces no ``_cache_ttl`` field.

    Defends against direct-instantiation regressions of the TTL constant.
    """
    mgr = UserListsManager(None, None)
    assert not hasattr(mgr, '_cache_ttl'), (
        "UserListsManager._cache_ttl still exists. Phase 89 LISTS-03 "
        "deleted the 10s TTL constant. See CONTEXT.md D-03, D-11."
    )


def test_appstate_still_has_local_lists_mgr():
    """Sanity check: ``_local_lists_mgr`` is out of Phase 89 scope and must
    still be present on AppState. Phase 89 only deletes ``_user_lists_mgr``;
    ``_local_lists_mgr`` is the per-device anonymous store wired by
    web/main.py:1505 ``state.lists_mgr = ListsManager(state.meta_mgr)``
    (which the setter writes to ``_local_lists_mgr``).

    If this test fails, Task 3's deletion was not surgical and accidentally
    removed an out-of-scope field.
    """
    instance = AppState()
    assert hasattr(instance, '_local_lists_mgr'), (
        "AppState._local_lists_mgr should still exist after Phase 89 deletion. "
        "Task 3 was supposed to delete _user_lists_mgr only, not _local_lists_mgr."
    )
