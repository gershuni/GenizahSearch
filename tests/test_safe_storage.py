"""Tests for the safe-storage helpers in ``web/safe_storage.py``.

Codex review HIGH finding: the v7.11.0 ``browse_state`` hotfix wrapped the
pruned-session ``app.storage.user`` AssertionError in two places, but the
same unprotected pattern remained at five other sites
(search_state, search bootstrap, parallels bootstrap, browse export).

These tests assert that the centralized helpers ``safe_user_get`` /
``safe_user_set`` / ``safe_user_pop`` swallow the NiceGUI assertion and
return the documented default instead of bubbling a 500.
"""
from unittest.mock import patch, MagicMock


PRUNED_SESSION_MSG = (
    "user storage for 6432b6d0-538a-4129-90a3-3ba9a6085e93 should be "
    "created before accessing it"
)


def test_safe_user_get_returns_default_on_assertion():
    storage = MagicMock()
    storage.get.side_effect = AssertionError(PRUNED_SESSION_MSG)

    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import safe_user_get

        assert safe_user_get('any_key', 'fallback') == 'fallback'
        assert safe_user_get('any_key') is None


def test_safe_user_get_returns_default_on_generic_exception():
    storage = MagicMock()
    storage.get.side_effect = RuntimeError('something else')

    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import safe_user_get

        assert safe_user_get('any_key', 42) == 42


def test_safe_user_set_returns_false_on_assertion():
    storage = MagicMock()
    # __setitem__ raises AssertionError
    def raise_assert(*_a, **_kw):
        raise AssertionError(PRUNED_SESSION_MSG)
    storage.__setitem__ = raise_assert

    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import safe_user_set

        assert safe_user_set('any_key', 'value') is False


def test_safe_user_set_returns_true_on_success():
    storage = {}

    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import safe_user_set

        assert safe_user_set('any_key', 'value') is True
        assert storage['any_key'] == 'value'


def test_safe_user_pop_returns_default_on_assertion():
    storage = MagicMock()
    storage.pop.side_effect = AssertionError(PRUNED_SESSION_MSG)

    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import safe_user_pop

        assert safe_user_pop('any_key', 'fallback') == 'fallback'


def test_safe_user_get_happy_path():
    """Sanity: when storage is healthy, the helper passes the value through."""
    storage = {'foo': 'bar', 'count': 7}

    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import safe_user_get

        assert safe_user_get('foo') == 'bar'
        assert safe_user_get('count') == 7
        assert safe_user_get('missing', 'default') == 'default'
