# -*- coding: utf-8 -*-
"""Web filter count: a lookup that cannot run shows 'Could not update filter count' (#17).

Drives web.components.filter_panel.recompute_filter_count (the live count of
/search and /parallels) against the real FjmsService singleton pointed at a
small 5.0.0 sidecar (no criteria tables), and at no sidecar. Also drives
load_filter_state, which drops a saved criterion only when the open catalog
is KNOWN to lack its data (rule 9 of docs/plans/CATALOGUE_CRITERIA_PLAN.md).
"""
import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import ROWS, build_sidecar  # noqa: E402

from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402
import web.components.filter_panel as fp  # noqa: E402


def _state(**over):
    s = SimpleNamespace(
        filter_include_mode=True, filter_domains=[], filter_authors=[], filter_works=[],
        filter_material_exclude=[], filter_criteria={},
        filter_text_all=[], filter_text_any=[], filter_text_not=[], filter_library=[],
        filter_manuscript_count='untouched', restrict_sys_ids='untouched',
    )
    for k, v in over.items():
        setattr(s, k, v)
    return s


def _recompute(state, monkeypatch):
    async def _to_thread(fn, *a, **kw):
        return await asyncio.to_thread(fn, *a, **kw)
    monkeypatch.setattr(fp.run, 'io_bound', _to_thread)
    statuses = []
    asyncio.run(fp.recompute_filter_count(state, lambda: None, on_state=statuses.append))
    return statuses


@pytest.fixture
def old_file(monkeypatch, tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db"))
    monkeypatch.setattr(fjms_service, "_default_service", svc)
    return svc


@pytest.fixture
def absent(monkeypatch, tmp_path):
    svc = FjmsService(db_path=str(tmp_path / "absent.db"))
    monkeypatch.setattr(fjms_service, "_default_service", svc)


def test_missing_data_shows_error_not_zero(old_file, monkeypatch):
    st = _state(filter_criteria={'columns': {'min': 2, 'max': 2}})
    statuses = _recompute(st, monkeypatch)
    assert st.filter_manuscript_count != 0, "a lookup that could not run was shown as 0 manuscripts"
    assert statuses[-1] == 'error', statuses


def test_missing_sidecar_shows_error_not_all(absent, monkeypatch):
    st = _state(filter_criteria={'period': {'min': 1000}})
    statuses = _recompute(st, monkeypatch)
    assert statuses[-1] == 'error', statuses


def test_working_lookup_still_counts(old_file, monkeypatch):
    st = _state(filter_criteria={'material': {'values': ['Paper'], 'source': 'measured'}})
    statuses = _recompute(st, monkeypatch)
    assert statuses[-1] == 'done'
    assert st.filter_manuscript_count == len(ROWS)


# -- saved values (load_filter_state) --

def _load(monkeypatch, saved, prefix='search'):
    """Run the real load_filter_state against a storage dict."""
    import web.safe_storage as ss
    written = {}
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: saved.get(key, default))
    monkeypatch.setattr(fp, 'persist_value', lambda key, value: written.__setitem__(key, value))
    st = SimpleNamespace()
    dropped = fp.load_filter_state(st, prefix)
    return st, dropped, written


SAVED = {'search_filter_criteria': {'columns': {'min': 2}, 'width': {'min': 10.0}},
         'parallels_filter_criteria': {'attributes': {'values': ['colophon']}}}


@pytest.mark.parametrize('prefix,key', [('search', 'columns'), ('parallels', 'attributes')])
def test_a_saved_criterion_is_dropped_when_the_catalog_lacks_it(old_file, monkeypatch, prefix, key):
    st, dropped, written = _load(monkeypatch, SAVED, prefix)
    assert dropped == [key], dropped
    assert key not in st.filter_criteria
    assert key not in written[f'{prefix}_filter_criteria'], (
        "the dropped value must also leave storage, or it comes back next visit")
    if prefix == 'search':
        assert st.filter_criteria == {'width': {'min': 10, 'source': 'either'}}, (
            "only the unsupported criterion is dropped")


def test_saved_values_are_kept_when_there_is_no_catalog(absent, monkeypatch):
    st, dropped, written = _load(monkeypatch, SAVED)
    assert dropped == [] and written == {}
    assert 'columns' in st.filter_criteria


def test_saved_values_are_kept_when_the_schema_could_not_be_read(old_file, monkeypatch):
    def unreadable():
        raise RuntimeError("database is locked")
    monkeypatch.setattr(old_file, 'criteria_support', unreadable)
    st, dropped, written = _load(monkeypatch, SAVED)
    assert dropped == [] and written == {}
    assert 'columns' in st.filter_criteria
