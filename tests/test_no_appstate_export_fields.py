"""Phase 88 D-06 -- runtime attribute-absence regression test.

Asserts the 10 per-user export-state fields deleted from web/state.py:AppState
in Phase 88 cannot be accessed directly on an AppState instance.

This is the RUNTIME guard. The companion STATIC guard
(tests/test_no_deleted_state_references.py) catches `state.last_results = ...`
re-introductions at AST-scan time even when the dynamic attr would not yet
have been read.

Both tests are intentionally in Phase 88 -- they do not belong in Phase 92
SWEEP-01 because SWEEP-01 audits raw `app.storage.user.get/pop/[]` access,
which is a different class of regression.
"""
import pytest

from web.state import AppState


DELETED_FIELDS = [
    'last_results',
    'current_search_query',
    'current_search_mode',
    'current_search_gap',
    'last_filters_applied',
    'last_search_warnings',
    'last_selected_uids',
    'parallels_results',
    'parallels_filtered',
    'parallels_search_meta',
]


@pytest.mark.parametrize('field', DELETED_FIELDS)
def test_appstate_does_not_have_deleted_field(field):
    """Direct attribute access on AppState() raises AttributeError for each
    Phase-88-deleted per-user mirror field.

    The AppState class is a __new__-based singleton, so AppState() returns
    the cached instance. We test hasattr (not isinstance / getattr-with-default
    / pytest.raises) because hasattr correctly reports "this attribute does
    not exist on the instance or its class" -- exactly the contract we want
    to enforce post-deletion.

    If a future PR re-introduces `self.last_results = []` in AppState.init()
    OR sets the attribute dynamically anywhere before this test runs (e.g.
    `state.last_results = [...]` in fixture setup), the corresponding
    parametrized test will fail with a clear message naming the field.
    """
    instance = AppState()
    assert not hasattr(instance, field), (
        f"AppState.{field} still exists. Phase 88 STATE-01 deleted this field; "
        f"it must not be re-added directly or dynamically. "
        f"See .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md D-06."
    )


def test_appstate_still_has_non_deleted_fields():
    """Sanity check: surviving fields (Phase 88 left untouched) are still present.

    Confirms Task 1's deletion was surgical -- did not accidentally remove
    non-export-state attributes.
    """
    instance = AppState()
    survivors = [
        'meta_mgr', 'var_mgr', 'searcher', 'lab_engine', 'indexer',
        '_local_lists_mgr',
    ]
    for attr in survivors:
        assert hasattr(instance, attr), (
            f"AppState.{attr} should still exist after Phase 88 deletion. "
            f"Task 1 was supposed to be surgical."
        )
