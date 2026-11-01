"""Chip text and the post-search matcher of the catalogue criteria (PR 1b of
docs/plans/CATALOGUE_CRITERIA_PLAN.md, used by the web and the desktop):
``criteria_chips`` / ``FjmsService.criteria_chips`` (§6.3) and
``FjmsService.match_result_criteria`` (D20: the post-search filters mean
what the panel means). Data: tests/criteria_sidecar.py."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from criteria_sidecar import A, B, C, D, E, F, UNIVERSE, build_criteria_sidecar  # noqa: E402

from shared.catalog_criteria import (  # noqa: E402
    NOT_RECORDED, criteria_chips, criteria_summary, criterion_chip, value_label,
)
from shared.fjms_service import FilterUnavailable, FjmsService  # noqa: E402


@pytest.fixture(scope="module")
def svc(tmp_path_factory):
    return FjmsService(db_path=build_criteria_sidecar(tmp_path_factory.mktemp("c") / "f.db"))


@pytest.fixture(scope="module")
def old_svc(tmp_path_factory):
    return FjmsService(db_path=build_criteria_sidecar(
        tmp_path_factory.mktemp("o") / "f.db", upgrade=False))


# ── chip text ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("key, spec, en, he", [
    ("style", {"values": ["Square", "Semi-Cursive"], "mode": "only"},
     "Style: Square, Semi-Cursive", "כתב: מרובע, בינוני"),
    ("region", {"values": ["Oriental"], "mode": "except"},
     "Region: all except Oriental", "אזור: הכול חוץ ממזרחי"),
    ("region", {"values": ["Oriental", NOT_RECORDED], "mode": "except"},
     "Region: all except Oriental, not recorded", "אזור: הכול חוץ ממזרחי, לא נרשם"),
    ("material", {"values": ["Vellum", NOT_RECORDED], "mode": "only", "source": "catalogue"},
     "Material (catalogue): Vellum, not recorded", "חומר (קטלוג): קלף, לא נרשם"),
    ("material", {"values": ["Wood"], "mode": "only", "source": "measured"},
     "Material (measured): Wood", "חומר (מדידה): עץ"),
    ("attributes", {"values": ["illustration", "colophon"], "mode": "only"},
     "Attributes: Illustration, Colophon", "תכונות: איור, קולופון"),
    ("lines", {"min": 20, "max": 25, "source": "either"},
     "Lines: 20–25", "שורות: 20–25"),
    ("lines", {"min": 20, "max": 25, "source": "measured"},
     "Lines (measured): 20–25", "שורות (מדידה): 20–25"),
    ("columns", {"min": 2, "max": 2, "not_recorded": True},
     "Columns: 2, or not recorded", "טורים: 2, או לא נרשם"),
    ("columns", {"not_recorded": True}, "Columns: not recorded", "טורים: לא נרשם"),
    ("lines", {"max": 25, "source": "either"}, "Lines: up to 25", "שורות: עד 25"),
    ("period", {"min": 1000}, "Period: 1000 or more", "תקופה: 1000 ומעלה"),
    ("width", {"min": 10, "max": 15.5, "source": "either"},
     "Width: 10–15.5 cm", 'רוחב: 10–15.5 ס"מ'),
    ("text_density", {"min": 3}, "Average text density: 3 or more per 10 cm",
     'צפיפות הכתב: 3 ומעלה ל־10 ס"מ'),
])
def test_chip_text(key, spec, en, he):
    labels = {"Square": "מרובע", "Semi-Cursive": "בינוני", "Oriental": "מזרחי",
              "Vellum": "קלף"}
    assert criterion_chip(key, spec, "en", labels) == en
    assert criterion_chip(key, spec, "he", labels) == he


def test_all_except_a_value_with_no_hebrew_label_keeps_the_preposition_apart():
    assert criterion_chip("region", {"values": ["Byzantine"], "mode": "except"}, "he") == \
        "אזור: הכול חוץ מ־Byzantine"


def test_chips_follow_the_registry_order_and_skip_empty_criteria():
    chips = criteria_chips({"lines": {"min": 20, "max": 25}, "style": {"values": ["Square"]},
                            "region": {"values": []}}, "en")
    assert chips == [("style", "Style: Square"), ("lines", "Lines: 20–25")]
    assert criteria_summary({"lines": {"min": 20, "max": 25},
                             "style": {"values": ["Square"]}}, "en") == \
        "Style: Square; Lines: 20–25"


def test_a_label_lookup_that_fails_never_fails_the_chip():
    def broken(key):
        raise FilterUnavailable("query_failed")
    assert criteria_chips({"style": {"values": ["Square"]}}, "he", broken) == \
        [("style", "כתב: Square")]


def test_malformed_criteria_give_no_chip():
    assert criteria_chips({"style": {"values": "Square"}, "nonsense": 1}, "en") == []


def test_value_label_defaults():
    assert value_label("region", NOT_RECORDED, "he") == "לא נרשם"
    assert value_label("attributes", "palimpsest", "en") == "Palimpsest"
    assert value_label("style", "Square", "he") == "Square"   # no labels given


def test_service_chips_use_the_catalogue_hebrew(svc):
    assert svc.criteria_chips({"style": {"values": ["Square"]},
                               "vocal_extent": {"values": ["Full"]}}, "he") == [
        ("style", "כתב: מרובע"), ("vocal_extent", "ניקוד: מלא")]
    assert svc.criteria_summary({"region": {"values": ["Oriental"], "mode": "except"}},
                                "he") == "אזור: הכול חוץ ממזרחי"


def test_service_chips_on_a_file_that_cannot_read_labels(old_svc):
    # 5.0.0: vocal_extent's values cannot be read; the chip still shows.
    assert old_svc.criteria_chips({"vocal_extent": {"values": ["Full"]}}, "he") == [
        ("vocal_extent", "ניקוד: Full")]


# ── post-search filters (D20) ≡ the panel ───────────────────────────────

RESULT = {A, C, D, E}

PANEL_CASES = [
    {"style": {"values": ["Square"]}},
    {"region": {"values": ["Oriental"], "mode": "except"}},
    {"region": {"values": [NOT_RECORDED]}},
    {"material": {"values": ["Paper"]}},
    {"material": {"values": ["Paper"], "source": "measured"}},
    {"material": {"values": [NOT_RECORDED]}},
    {"lines": {"min": 22, "max": 22}},
    {"lines": {"min": 20, "max": 20, "source": "measured"}},
    {"width": {"min": 10, "max": 12}},
    {"width": {"min": 10, "max": 12, "source": "measured"},
     "height": {"min": 13, "max": 16, "source": "measured"}},
    {"width": {"min": 10, "max": 12}, "margin_left": {"min": 1, "max": 2}},
    {"line_height": {"not_recorded": True}},
    {"lines": {"min": 22, "max": 22, "not_recorded": True},
     "width": {"min": 10, "max": 12, "source": "measured"}},
    {"text_density": {"min": 1}},
]


@pytest.mark.parametrize("criteria", PANEL_CASES)
def test_post_search_filters_mean_what_the_panel_means(svc, criteria):
    panel = svc.get_filter_sys_ids(criteria=criteria, universe=set(UNIVERSE))
    assert svc.match_result_criteria(criteria, RESULT) == panel & RESULT


def test_post_search_universe_is_the_result(svc):
    # Not recorded and all except are counted within the result, so they
    # need no corpus list.
    assert svc.match_result_criteria({"region": {"values": ["Oriental"], "mode": "except"}},
                                     [B, E, F]) == {B, E, F}
    assert svc.match_result_criteria({"width": {"not_recorded": True}}, [A, E]) == {E}


def test_post_search_without_criteria_is_no_restriction(svc):
    assert svc.match_result_criteria({}, RESULT) is None
    assert svc.match_result_criteria({"region": {"values": []}}, RESULT) is None


def test_post_search_results_are_copies(svc):
    first = svc.match_result_criteria({"style": {"values": ["Square"]}}, RESULT)
    first.add("x")
    assert svc.match_result_criteria({"style": {"values": ["Square"]}}, RESULT) == {A}


def test_post_search_fails_closed(old_svc):
    with pytest.raises(FilterUnavailable) as err:
        old_svc.match_result_criteria({"lines": {"min": 20, "max": 25}}, RESULT)
    assert err.value.reason == "column_missing"
    broken = FjmsService.__new__(FjmsService)
    broken.__dict__.update(old_svc.__dict__)
    broken._conn = None
    with pytest.raises(FilterUnavailable) as err:
        broken.match_result_criteria({"style": {"values": ["Square"]}}, RESULT)
    assert err.value.reason == "sidecar_unavailable"
