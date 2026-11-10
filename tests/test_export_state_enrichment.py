"""Phase 94 EXPORT-META-06: enrichment kwargs on set_search_export +
update_search_export_enrichment behavior.

Covers:
  - set_search_export accepts 3 new optional kwargs (transcription_sys_ids,
    printed_ids, result_domains) and casts sets to sorted lists for
    JSON-safety inside safe_storage.
  - update_search_export_enrichment patches only the specified fields,
    preserving Phase 88 D-11 (isinstance guard) + D-12 (copy-on-update)
    invariants.
  - Backward-compat: omitting the new kwargs yields stored payload with
    empty defaults ([], [], {}).
  - Multitenant invariant: separate sessions are isolated.

Test fixture pattern mirrors tests/test_export_state_selection.py:
SimpleNamespace stub + monkeypatch.setattr('web.safe_storage.app', stub).
"""

from types import SimpleNamespace
import pytest


def _make_stub(initial_storage: dict):
    """Instance-isolated stub mirroring app.storage.user surface."""
    return SimpleNamespace(storage=SimpleNamespace(user=initial_storage))


@pytest.fixture
def stub_storage(monkeypatch):
    fake_storage = {}
    stub = _make_stub(fake_storage)
    monkeypatch.setattr('web.safe_storage.app', stub)
    return fake_storage


def test_set_search_export_backward_compat_no_new_kwargs(stub_storage):
    from web.export_state import set_search_export, get_search_export
    set_search_export(results=[], query='q')
    payload = get_search_export()
    assert payload is not None
    assert payload['transcription_sys_ids'] == []
    assert payload['printed_ids'] == []
    assert payload['result_domains'] == {}


def test_set_search_export_3_new_kwargs_round_trip(stub_storage):
    from web.export_state import set_search_export, get_search_export
    set_search_export(
        results=[], query='q',
        transcription_sys_ids={'b', 'a'},  # set, unordered
        printed_ids={'d', 'c'},
        result_domains={'a': ['Bible'], 'b': ['Letter']},
    )
    payload = get_search_export()
    assert payload['transcription_sys_ids'] == ['a', 'b']  # sorted
    assert payload['printed_ids'] == ['c', 'd']
    assert payload['result_domains'] == {'a': ['Bible'], 'b': ['Letter']}


def test_set_search_export_accepts_list_input(stub_storage):
    from web.export_state import set_search_export, get_search_export
    set_search_export(
        results=[], query='q',
        transcription_sys_ids=['a', 'b', 'b'],  # list with dup
    )
    payload = get_search_export()
    assert payload['transcription_sys_ids'] == ['a', 'b']  # deduped + sorted


def test_set_search_export_none_defaults_to_empty(stub_storage):
    from web.export_state import set_search_export, get_search_export
    set_search_export(results=[], query='q', transcription_sys_ids=None)
    payload = get_search_export()
    assert payload['transcription_sys_ids'] == []


def test_update_enrichment_patches_only_specified_fields(stub_storage):
    from web.export_state import (
        set_search_export, update_search_export_enrichment, get_search_export,
    )
    set_search_export(
        results=[], query='q',
        transcription_sys_ids={'a'},
        printed_ids={'p'},
        result_domains={'a': ['Bible']},
    )
    update_search_export_enrichment(transcription_sys_ids={'a', 'b'})
    payload = get_search_export()
    assert payload['transcription_sys_ids'] == ['a', 'b']
    # printed_ids and result_domains preserved:
    assert payload['printed_ids'] == ['p']
    assert payload['result_domains'] == {'a': ['Bible']}


def test_update_enrichment_isinstance_guard(stub_storage):
    # NO prior set_search_export -- empty storage
    from web.export_state import (
        update_search_export_enrichment, get_search_export,
    )
    # Should not raise; should not create a payload from nothing
    update_search_export_enrichment(printed_ids={'a'})
    assert get_search_export() is None


def test_update_enrichment_all_none_is_noop(stub_storage):
    from web.export_state import (
        set_search_export, update_search_export_enrichment, get_search_export,
    )
    set_search_export(results=[], query='q', transcription_sys_ids={'a'})
    update_search_export_enrichment()  # all defaults
    payload = get_search_export()
    assert payload['transcription_sys_ids'] == ['a']  # unchanged


def test_update_enrichment_can_clear_via_empty(stub_storage):
    from web.export_state import (
        set_search_export, update_search_export_enrichment, get_search_export,
    )
    set_search_export(results=[], query='q', transcription_sys_ids={'a', 'b'})
    update_search_export_enrichment(transcription_sys_ids=set())  # empty set
    payload = get_search_export()
    assert payload['transcription_sys_ids'] == []


def test_session_isolation_across_two_stubs(monkeypatch):
    """Replicates Phase 88 instance-isolated SimpleNamespace cross-session test."""
    from web.export_state import set_search_export, get_search_export

    # Session A
    storage_a = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage_a))
    set_search_export(results=[], query='qa', transcription_sys_ids={'a1', 'a2'})
    payload_a = get_search_export()
    assert payload_a['transcription_sys_ids'] == ['a1', 'a2']

    # Session B (different stub = different "user")
    storage_b = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage_b))
    # No prior set in session B
    assert get_search_export() is None
    set_search_export(results=[], query='qb', transcription_sys_ids={'b1'})
    payload_b = get_search_export()
    assert payload_b['transcription_sys_ids'] == ['b1']
    assert payload_b['query'] == 'qb'

    # Session A storage is independent -- confirm its stub still has the original data
    # (by re-pointing back to storage_a, the data is still there)
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage_a))
    payload_a_again = get_search_export()
    assert payload_a_again['transcription_sys_ids'] == ['a1', 'a2']
    assert payload_a_again['query'] == 'qa'
