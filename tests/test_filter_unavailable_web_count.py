# -*- coding: utf-8 -*-
"""Web filter count: a lookup that cannot run shows 'Could not update filter count' (#17).

Drives web.components.filter_panel.recompute_filter_count (the live count of
/search and /parallels) against the real FjmsService singleton pointed at a
small sidecar without the line-height column, and at no sidecar. Also drives
load_filter_state, which drops a saved bound only when the open catalog is
KNOWN to lack it.
"""
import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import build_sidecar, unknown_schema_service  # noqa: E402

from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402
import web.components.filter_panel as fp  # noqa: E402


def _state(**over):
    s = SimpleNamespace(
        filter_include_mode=True, filter_domains=[], filter_authors=[], filter_works=[],
        filter_date_from=None, filter_date_to=None, filter_material_exclude=[],
        filter_text_all=[], filter_text_any=[], filter_text_not=[],
        filter_width_min=None, filter_width_max=None,
        filter_height_min=None, filter_height_max=None,
        filter_line_count_min=None, filter_line_count_max=None,
        filter_line_height_min=None, filter_line_height_max=None,
        filter_text_density_min=None, filter_text_density_max=None,
        filter_measurement_material=[], filter_library=[],
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
def no_lh(monkeypatch, tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db", with_line_height=False))
    monkeypatch.setattr(fjms_service, "_default_service", svc)


@pytest.fixture
def absent(monkeypatch, tmp_path):
    svc = FjmsService(db_path=str(tmp_path / "absent.db"))
    monkeypatch.setattr(fjms_service, "_default_service", svc)


def test_missing_column_shows_error_not_zero(no_lh, monkeypatch):
    st = _state(filter_line_height_min=3.0)
    statuses = _recompute(st, monkeypatch)
    assert st.filter_manuscript_count != 0, "a lookup that could not run was shown as 0 manuscripts"
    assert statuses[-1] == 'error', statuses


def test_missing_sidecar_shows_error_not_all(absent, monkeypatch):
    st = _state(filter_date_from=1000)
    statuses = _recompute(st, monkeypatch)
    assert statuses[-1] == 'error', statuses


def test_working_lookup_still_counts(no_lh, monkeypatch):
    st = _state(filter_width_min=10)
    statuses = _recompute(st, monkeypatch)
    assert statuses[-1] == 'done'
    assert st.filter_manuscript_count == 2


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


SAVED = {'search_filter_line_height_min': 3.0, 'search_filter_width_min': 10.0,
         'parallels_filter_line_height_max': 6.0}


@pytest.mark.parametrize('prefix,key', [('search', 'line_height_min'),
                                        ('parallels', 'line_height_max')])
def test_saved_line_height_is_dropped_when_the_catalog_lacks_it(no_lh, monkeypatch, prefix, key):
    st, dropped, written = _load(monkeypatch, SAVED, prefix)
    assert dropped == [key], dropped
    assert getattr(st, f'filter_{key}') is None
    assert written == {f'{prefix}_filter_{key}': None}, (
        "the dropped value must also leave storage, or it comes back next visit")
    if prefix == 'search':
        assert st.filter_width_min == 10.0, "only the unsupported filter is dropped"


def test_saved_values_are_kept_when_there_is_no_catalog(absent, monkeypatch):
    st, dropped, written = _load(monkeypatch, SAVED)
    assert dropped == [] and written == {}
    assert st.filter_line_height_min == 3.0


def test_saved_values_are_kept_when_the_schema_could_not_be_read(monkeypatch, tmp_path):
    svc = unknown_schema_service(build_sidecar(tmp_path / "u.db", with_line_height=False))
    monkeypatch.setattr(fjms_service, "_default_service", svc)
    st, dropped, written = _load(monkeypatch, SAVED)
    assert dropped == [] and written == {}
    assert st.filter_line_height_min == 3.0
