"""The service figures the "Manuscript features" panel shows (PR 1b of
docs/plans/CATALOGUE_CRITERIA_PLAN.md, §6.3 and rule 8), and the shared
helpers its pages use: coverage per criterion, the live total with "of which
N not recorded" for "all except" lists (rule 2), the note under the total,
saved dates read in as the period (D18), and the criteria a 5.0.0 file
cannot answer (rule 9). Data: tests/criteria_sidecar.py."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from criteria_sidecar import UNIVERSE, build_criteria_sidecar  # noqa: E402

from shared.catalog_criteria import (  # noqa: E402
    BY_KEY, NEW_TABLE_KEYS, period_from_dates,
)
from shared.fjms_service import FilterUnavailable, FjmsService  # noqa: E402

U = frozenset(UNIVERSE)


@pytest.fixture(scope="module")
def svc(tmp_path_factory):
    return FjmsService(db_path=build_criteria_sidecar(tmp_path_factory.mktemp("p") / "f.db"))


@pytest.fixture(scope="module")
def old_svc(tmp_path_factory):
    return FjmsService(db_path=build_criteria_sidecar(
        tmp_path_factory.mktemp("o") / "f.db", upgrade=False))


@pytest.mark.parametrize("key, source, expected", [
    ("style", "either", 2),          # A, B
    ("region", "either", 3),         # A, B, C (C's "Unknown" is recorded)
    ("material", "catalogue", 2),    # A, B
    ("material", "measured", 1),     # D
    ("material", "either", 3),
    ("lines", "catalogue", 2),       # A "19-21", C "22,25" (B's "0" is none)
    ("lines", "either", 4),          # + B and D measured
    ("width", "catalogue", 1),       # A (C's size is flagged)
    ("columns", "either", 2),        # A, B
    ("period", "either", 2),         # A, B (C's -99 is no date)
])
def test_coverage(svc, key, source, expected):
    assert svc.get_criterion_coverage(key, U, source) == expected


def test_coverage_takes_the_corpus_as_a_callable(svc):
    assert svc.get_criterion_coverage("style", lambda: U) == 2


def test_coverage_needs_the_corpus(svc):
    with pytest.raises(FilterUnavailable) as err:
        svc.get_criterion_coverage("style", None)
    assert err.value.reason == "corpus_unavailable"


def test_coverage_fails_closed_on_a_5_0_0_file(old_svc):
    with pytest.raises(FilterUnavailable):
        old_svc.get_criterion_coverage("lines", U)


def test_live_total_counts_and_says_how_many_have_nothing_recorded(svc):
    total = svc.criteria_live_total({"region": {"values": ["Oriental"], "mode": "except"}}, U)
    # B, C, D, E, F; of those D, E, F have no region recorded (rule 2).
    assert total == {"total": 5, "not_recorded": {"region": 3}}


def test_live_total_follows_the_source_of_an_except_list(svc):
    total = svc.criteria_live_total(
        {"material": {"values": ["Vellum"], "mode": "except", "source": "catalogue"}}, U)
    assert total == {"total": 4, "not_recorded": {"material": 4}}   # C, D, E, F


def test_live_total_without_criteria(svc):
    assert svc.criteria_live_total({}, U) == {"total": None, "not_recorded": {}}


def test_live_total_fails_closed(old_svc):
    with pytest.raises(FilterUnavailable) as err:
        old_svc.criteria_live_total({"columns": {"min": 1}}, U)
    assert err.value.reason == "column_missing"


def test_the_corpus_note(svc):
    # Six manuscripts, D and E uncatalogued, neither with a catalogue size.
    assert svc.criteria_corpus_note(U) == {"corpus": 6, "uncatalogued": 2,
                                           "uncatalogued_sized": 0}
    with pytest.raises(FilterUnavailable):
        svc.criteria_corpus_note(None)


def test_criterion_values_take_the_corpus_as_a_callable(svc):
    values = svc.get_criterion_values("region", lambda: U)
    assert values[-1] == {"value": "_none", "label_en": "Not recorded", "label_he": "לא נרשם",
                          "count": 3}


@pytest.mark.parametrize("args, expected", [
    ((1000, 1200), {"period": {"min": 1000, "max": 1200}}),
    ((None, 1200, True), {"period": {"max": 1200, "not_recorded": True}}),
    ((1200, 1000), {"period": {"min": 1000, "max": 1200}}),     # reversed: swapped
    (("1000", None), {"period": {"min": 1000}}),
    ((None, None, True), {}),     # "include undated" alone never filtered
    (("x", 1100.5), {}),          # neither a whole year
])
def test_saved_dates_as_the_period(args, expected):
    assert period_from_dates(*args) == expected


def test_the_criteria_a_5_0_0_file_cannot_answer():
    assert set(NEW_TABLE_KEYS) == {k for k, c in BY_KEY.items() if c.number_key or c.value_key}
    assert set(NEW_TABLE_KEYS) == {"period", "vocal_extent", "columns", "lines", "attributes"}
