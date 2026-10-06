# -*- coding: utf-8 -*-
"""A filter lookup that cannot run is reported as unavailable, never as a result (#17).

get_filter_sys_ids returns a set of matching manuscripts, None for "no filter
is active", and must RAISE FilterUnavailable when a filter is active but the
lookup cannot be answered: the sidecar is absent, the query fails, or the
filter needs a table or column the sidecar is KNOWN not to have. An empty set
means "the filters matched nothing" and nothing else.

Capability is three-state (K-16): supported / known-unsupported / unknown. A
saved value may be dropped only when a schema inspection SUCCEEDED and said
the data is missing.
"""
import os
import pickle
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import (  # noqa: E402
    FailingFinalExecute, assert_unavailable, build_sidecar, call,
    unknown_schema_service,
)

from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
REAL_SIDECAR = REPO / "fist_data" / "fjms_enrichment.db"


@pytest.fixture
def no_lh_svc(tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "no_lh.db", with_line_height=False))
    assert svc.is_available()
    yield svc
    svc.close()


@pytest.fixture
def no_table_svc(tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "no_mm.db", with_measurements=False))
    assert svc.is_available()
    yield svc
    svc.close()


@pytest.fixture
def lh_svc(tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "lh.db", with_line_height=True))
    yield svc
    svc.close()


@pytest.fixture
def unknown_svc(tmp_path):
    """The real no-line-height file, but its schema cannot be read."""
    svc = unknown_schema_service(build_sidecar(tmp_path / "unk.db", with_line_height=False))
    yield svc
    svc.close()


# -- the fixtures are valid: other filters on the same files work (green before) --

def test_width_filter_on_missing_column_sidecar_still_answers(no_lh_svc):
    assert no_lh_svc.get_filter_sys_ids(width_min=10) == {"990001", "990002"}


def test_line_height_filter_answers_when_column_present(lh_svc):
    assert lh_svc.get_filter_sys_ids(line_height_min=4.0) == {"990002", "990003"}


def test_date_filter_on_sidecar_without_measurements_still_answers(no_table_svc):
    assert no_table_svc.get_filter_sys_ids(date_from=1000, date_to=1100) == {"990001"}


def test_real_no_match_is_still_an_empty_set(lh_svc):
    assert lh_svc.get_filter_sys_ids(date_from=1500, date_to=1600) == set()


def test_no_active_filter_without_sidecar_is_still_none(tmp_path):
    svc = FjmsService(db_path=str(tmp_path / "absent.db"))
    assert svc.get_filter_sys_ids() is None


# -- capability detection --

def test_capabilities_reflect_the_sidecar_schema(no_lh_svc, no_table_svc, lh_svc):
    assert (lh_svc.has_measurement_table, lh_svc.has_line_height) == (True, True)
    assert (no_lh_svc.has_measurement_table, no_lh_svc.has_line_height) == (True, False)
    assert (no_table_svc.has_measurement_table, no_table_svc.has_line_height) == (False, False)


def test_capability_is_three_state(tmp_path, no_lh_svc, no_table_svc, lh_svc, unknown_svc):
    """supported / unsupported only from a schema that was READ; no sidecar
    or a failed read is 'unknown', never 'unsupported'."""
    def both(svc):
        return (fjms_service.measurement_filter_support(svc),
                fjms_service.line_height_filter_support(svc))
    assert both(lh_svc) == ('supported', 'supported')
    assert both(no_lh_svc) == ('supported', 'unsupported')
    assert both(no_table_svc) == ('unsupported', 'unsupported')
    assert both(unknown_svc) == ('unknown', 'unknown')
    assert both(FjmsService(db_path=str(tmp_path / "absent.db"))) == ('unknown', 'unknown')


def test_capabilities_false_without_sidecar_and_no_stub_created(tmp_path):
    svc = FjmsService(db_path=str(tmp_path / "absent.db"))
    assert svc.has_measurement_table is False
    assert svc.has_line_height is False
    assert not (tmp_path / "absent.db").exists(), "probe must not create a stub file"


def test_known_missing_keys_only_when_the_schema_was_read(tmp_path, no_lh_svc, no_table_svc,
                                                          lh_svc, unknown_svc):
    """A saved bound is dropped only when the open sidecar is KNOWN to lack the
    data. An absent sidecar can come back (a download in progress, a swap),
    and a schema that could not be read says nothing, so both drop nothing;
    the lookup reports FilterUnavailable instead."""
    keys = fjms_service.unavailable_measurement_filter_keys
    assert keys(lh_svc) == frozenset()
    assert keys(no_lh_svc) == frozenset({'line_height_min', 'line_height_max'})
    assert {'width_min', 'line_height_min', 'measurement_material'} <= keys(no_table_svc)
    assert keys(FjmsService(db_path=str(tmp_path / "absent.db"))) == frozenset()
    assert keys(unknown_svc) == frozenset(), "a failed schema read authorized a drop"


def test_a_failed_schema_read_is_retried(unknown_svc):
    """'unknown' is not cached as an answer: once the schema can be read, the
    real capability is known."""
    assert fjms_service.line_height_filter_support(unknown_svc) == 'unknown'
    unknown_svc._conn = unknown_svc._conn._real
    assert fjms_service.line_height_filter_support(unknown_svc) == 'unsupported'


def test_drop_returns_a_new_dict_and_leaves_the_input_alone(no_lh_svc):
    saved = {'date_from': 1000, 'line_height_min': 3.0}
    kept, dropped = fjms_service.drop_unavailable_measurement_filters(saved, service=no_lh_svc)
    assert kept == {'date_from': 1000}
    assert dropped == ['line_height_min']
    assert saved == {'date_from': 1000, 'line_height_min': 3.0}, (
        "the caller's dict (a search-history entry) was modified")


@pytest.mark.parametrize("which", ["absent", "unknown"])
def test_drop_keeps_everything_when_the_answer_is_not_known(tmp_path, unknown_svc, which):
    svc = unknown_svc if which == "unknown" else FjmsService(db_path=str(tmp_path / "absent.db"))
    saved = {'date_from': 1000, 'line_height_min': 3.0}
    kept, dropped = fjms_service.drop_unavailable_measurement_filters(saved, service=svc)
    assert kept == saved and dropped == []


# -- false empty --

@pytest.mark.parametrize("kwargs", [
    {"line_height_min": 3.0},
    {"line_height_max": 9.0},
    {"width_min": 10, "line_height_max": 9.0},
])
def test_line_height_filter_without_column_is_unavailable(no_lh_svc, kwargs):
    outcome, value = call(no_lh_svc.get_filter_sys_ids, **kwargs)
    assert_unavailable(outcome, value, "column_missing")
    assert value.filters == ('line_height',)


@pytest.mark.parametrize("kwargs", [
    {"width_min": 10},
    {"measurement_material": ["Paper"]},
    {"date_from": 1000, "line_count_max": 40},
])
def test_measurement_filter_without_table_is_unavailable(no_table_svc, kwargs):
    outcome, value = call(no_table_svc.get_filter_sys_ids, **kwargs)
    assert_unavailable(outcome, value, "column_missing")
    assert value.filters == ('measurements',)


@pytest.mark.parametrize("kwargs", [
    {"date_from": 1000, "date_to": 1100},
    {"width_min": 10},
])
def test_query_error_at_final_execute_is_unavailable(lh_svc, kwargs):
    real = lh_svc._conn
    lh_svc._conn = FailingFinalExecute(real)
    try:
        outcome, value = call(lh_svc.get_filter_sys_ids, **kwargs)
    finally:
        lh_svc._conn = real
    assert_unavailable(outcome, value, "query_failed")
    assert isinstance(value.__cause__, Exception), "keep the database error as the cause"


def test_unknown_schema_runs_the_query_and_reports_its_failure(unknown_svc):
    """With the schema unknown, a Line Height bound is NOT declared missing;
    the query runs, and the real file's missing column makes it fail as
    query_failed -- still never an empty set."""
    outcome, value = call(unknown_svc.get_filter_sys_ids, line_height_min=3.0)
    assert_unavailable(outcome, value, "query_failed")


# -- false unrestricted --

def test_active_filter_without_sidecar_is_unavailable(tmp_path):
    svc = FjmsService(db_path=str(tmp_path / "absent.db"))
    outcome, value = call(svc.get_filter_sys_ids, date_from=1000, date_to=1100)
    assert_unavailable(outcome, value, "sidecar_unavailable")


# -- the module-level wrapper the API uses propagates it --

def test_module_wrapper_propagates(monkeypatch, tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "no_lh.db", with_line_height=False))
    monkeypatch.setattr(fjms_service, "_default_service", svc)
    outcome, value = call(fjms_service.get_filter_sys_ids, line_height_min=3.0)
    assert_unavailable(outcome, value, "column_missing")


def test_exception_survives_pickling():
    exc = fjms_service.FilterUnavailable('column_missing', 'no line heights', filters=('line_height',))
    back = pickle.loads(pickle.dumps(exc))
    assert (back.reason, back.filters, str(back)) == ('column_missing', ('line_height',), 'no line heights')


# -- the sidecar actually in use: a LOCAL check only. CI has no sidecar, so
#    this is always skipped there and is not a gate. --

@pytest.mark.skipif(not REAL_SIDECAR.is_file() or REAL_SIDECAR.stat().st_size == 0,
                    reason="real FJMS sidecar not present (local-only check)")
def test_real_sidecar_line_height_is_reported_consistently():
    svc = FjmsService(db_path=str(REAL_SIDECAR))
    try:
        outcome, value = call(svc.get_filter_sys_ids, line_height_min=3.0)
        if svc.has_line_height:
            assert outcome == "returned" and value, value
        else:
            assert_unavailable(outcome, value, "column_missing")
    finally:
        svc.close()
