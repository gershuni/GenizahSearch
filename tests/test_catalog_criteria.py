"""shared/catalog_criteria.py: the criteria rules of
docs/plans/CATALOGUE_CRITERIA_PLAN.md §6.1, on the small sidecar of
tests/criteria_sidecar.py (each expectation is worked out by hand in that
file's docstring)."""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from criteria_sidecar import A, B, C, D, E, F, UNIVERSE, build_criteria_sidecar  # noqa: E402

from shared.catalog_criteria import (  # noqa: E402
    NOT_RECORDED, CriteriaUnavailable, clean_criteria, criteria_from_params,
    criteria_to_params, criterion_values, match_criteria,
)


@pytest.fixture(scope="module")
def conn(tmp_path_factory):
    path = build_criteria_sidecar(tmp_path_factory.mktemp("crit") / "fjms.db")
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    yield con
    con.close()


@pytest.fixture(scope="module")
def old_conn(tmp_path_factory):
    """The same data as a 5.0.0 file: no criteria_numbers / criteria_values."""
    path = build_criteria_sidecar(tmp_path_factory.mktemp("crit50") / "fjms.db", upgrade=False)
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    yield con
    con.close()


def m(conn, criteria, universe=UNIVERSE):
    return match_criteria(conn, criteria, set(universe) if universe is not None else None)


# ── lists: any record, all except, not recorded ─────────────────────────

@pytest.mark.parametrize("criteria, expected", [
    ({"style": {"values": ["Square"]}}, {A}),
    ({"region": {"values": ["Yemenite"]}}, {A}),                      # D4: any record
    ({"region": {"values": ["Oriental"], "mode": "except"}}, {B, C, D, E, F}),
    ({"region": {"values": ["Oriental", NOT_RECORDED], "mode": "except"}}, {B, C}),
    ({"region": {"values": [NOT_RECORDED]}}, {D, E, F}),              # D14: D, E included
    ({"region": {"values": ["Unknown"]}}, {C}),                       # D5: a recorded value
    ({"material": {"values": ["Vellum"], "source": "catalogue"}}, {A, B}),
    ({"material": {"values": ["Paper"], "source": "measured"}}, {D}),
    ({"material": {"values": ["Paper"]}}, {A, D}),                    # D6: either
    ({"material": {"values": [NOT_RECORDED], "source": "catalogue"}}, {C, D, E, F}),
    ({"material": {"values": [NOT_RECORDED]}}, {C, E, F}),            # follows the source
    ({"vocal_extent": {"values": ["Full"]}}, {A}),
    ({"vocal_extent": {"values": ["Unknown"]}}, {C}),
    ({"vocal_extent": {"values": [NOT_RECORDED]}}, {D, E, F}),
    ({"attributes": {"values": ["illustration", "colophon"]}}, {A, B}),  # R3: any of
    ({"attributes": {"values": ["illustration"], "mode": "except"}}, {B, C, D, E, F}),
    ({"attributes": {"values": ["massorah"]}}, {C}),
])
def test_lists(conn, criteria, expected):
    assert m(conn, criteria) == expected


def test_sections_combine_with_and(conn):
    assert m(conn, {"style": {"values": ["Square", "Semi-Cursive"]},
                    "region": {"values": ["Spanish"]}}) == {B}


# ── catalogue ranges: D7 numbers, D18 periods ───────────────────────────

@pytest.mark.parametrize("criteria, expected", [
    ({"lines": {"min": 20, "max": 20, "source": "catalogue"}}, {A}),   # "19-21" overlaps
    ({"lines": {"min": 25, "source": "catalogue"}}, {C}),              # "22,25"
    ({"lines": {"not_recorded": True, "source": "catalogue"}}, {B, D, E, F}),  # "0" is none
    ({"columns": {"min": 1, "max": 1}}, {B}),                          # "0" + "1" records
    ({"columns": {"not_recorded": True}}, {C, D, E, F}),               # B has a "1"
    ({"period": {"min": 1000, "max": 1030}}, {B}),                     # 1027-8 = 1027
    ({"period": {"min": 1090, "max": 1200}}, {A}),                     # span 1050-1100
    ({"period": {"min": 1090, "max": 1200, "not_recorded": True}}, {A, C, D, E, F}),
    ({"period": {"not_recorded": True}}, {C, D, E, F}),                # end-only = no date
])
def test_catalogue_ranges(conn, criteria, expected):
    assert m(conn, criteria) == expected


# ── measured pages (R1, D21) and either (D6) ────────────────────────────

@pytest.mark.parametrize("criteria, expected", [
    ({"lines": {"min": 20, "max": 20, "source": "measured"}}, {A}),
    ({"lines": {"min": 22, "max": 22}}, {C, D}),                       # catalogue C, page D
    ({"width": {"min": 10, "max": 12, "source": "measured"},
      "height": {"min": 13, "max": 16, "source": "measured"}}, {A, B, D}),
    ({"width": {"min": 10, "max": 12, "source": "measured"},
      "height": {"min": 13, "max": 16, "source": "measured"},
      "margin_left": {"min": 1, "max": 2}}, {A}),                      # one page for all
    ({"width": {"min": 12, "max": 14}, "height": {"min": 18, "max": 21}}, {A}),  # record 13x20
    ({"width": {"min": 10.5, "max": 11.5, "source": "catalogue"},
      "height": {"min": 15.5, "max": 16.5, "source": "catalogue"}}, set()),  # C's size flagged
    # Catalogue width 10 for A, but no page of A has that margin: measured-only
    # ranges always hold on a page (the 990001261770205171 case).
    ({"width": {"min": 10, "max": 12}, "margin_left": {"min": 2.5, "max": 3.5}}, {B}),
    # D21: measured lines and measured width on the same page.
    ({"lines": {"min": 24, "max": 24, "source": "measured"},
      "width": {"min": 11, "max": 12, "source": "measured"}}, {B}),
    # A's page P1 has 20 lines (10.5 wide), P2 is 12 wide (31 lines): no ONE
    # page of A is 12 wide with 20 lines, nor 10-11 wide and 29-31 high.
    ({"lines": {"min": 20, "max": 20, "source": "measured"},
      "width": {"min": 12, "max": 12, "source": "measured"}}, set()),
    ({"width": {"min": 10, "max": 11, "source": "measured"},
      "height": {"min": 29, "max": 31, "source": "measured"}}, set()),
    ({"lines": {"min": 22, "max": 22, "not_recorded": True}}, {C, D, E, F}),
    # 'or not recorded' on lines while width still applies to the others.
    ({"lines": {"min": 20, "max": 20, "source": "measured", "not_recorded": True},
      "width": {"min": 10, "max": 12, "source": "measured"}}, {A}),
    ({"dpi": {"not_recorded": True}}, UNIVERSE - set()),               # no page has a DPI
])
def test_measured_pages(conn, criteria, expected):
    assert m(conn, criteria) == expected


def test_width_and_height_are_one_catalogue_record(conn):
    # A has 10x15 and 13x20: width 13 with height 15 is no single record.
    assert m(conn, {"width": {"min": 13, "max": 13, "source": "catalogue"},
                    "height": {"min": 15, "max": 15, "source": "catalogue"}}) == set()


# ── universe and capabilities (rule 9) ──────────────────────────────────

def test_no_criteria_is_no_restriction(conn):
    assert match_criteria(conn, {}, set(UNIVERSE)) is None
    assert match_criteria(conn, None) is None


def test_without_a_universe_only_positive_criteria_run(conn):
    assert m(conn, {"style": {"values": ["Square"]}}, universe=None) == {A}
    for criteria in ({"region": {"values": ["Oriental"], "mode": "except"}},
                     {"region": {"values": [NOT_RECORDED]}},
                     {"period": {"min": 1000, "not_recorded": True}}):
        with pytest.raises(CriteriaUnavailable) as err:
            m(conn, criteria, universe=None)
        assert err.value.reason == "corpus_unavailable"


def test_the_universe_bounds_the_result(conn):
    assert m(conn, {"material": {"values": ["Paper"]}}, universe={D, E}) == {D}


@pytest.mark.parametrize("criteria", [
    {"lines": {"min": 20}},                          # either needs the catalogue half
    {"lines": {"min": 20, "source": "catalogue"}},
    {"columns": {"min": 1}},
    {"period": {"min": 1000}},
    {"vocal_extent": {"values": ["Full"]}},
    {"attributes": {"values": ["colophon"]}},
])
def test_a_5_0_0_file_cannot_answer_the_new_criteria(old_conn, criteria):
    with pytest.raises(CriteriaUnavailable) as err:
        m(old_conn, criteria)
    assert err.value.reason == "column_missing"
    assert err.value.filters == tuple(criteria)


def test_a_5_0_0_file_still_answers_the_rest(old_conn):
    assert m(old_conn, {"style": {"values": ["Square"]}}) == {A}
    assert m(old_conn, {"lines": {"min": 20, "max": 20, "source": "measured"}}) == {A}


# ── value lists ─────────────────────────────────────────────────────────

def test_region_values(conn):
    values = criterion_values(conn, "region", set(UNIVERSE))
    assert [(v["value"], v["count"]) for v in values] == [
        ("Oriental", 1), ("Spanish", 1), ("Yemenite", 1), ("Unknown", 1), (NOT_RECORDED, 3)]
    assert values[0]["label_he"] == "מזרחי"


def test_vocal_extent_values_carry_hebrew_labels(conn):
    values = {v["value"]: v for v in criterion_values(conn, "vocal_extent", set(UNIVERSE))}
    assert values["Full"]["label_he"] == "מלא"
    assert values[NOT_RECORDED]["count"] == 3


def test_attributes_have_no_not_recorded(conn):
    values = criterion_values(conn, "attributes", set(UNIVERSE))
    assert NOT_RECORDED not in {v["value"] for v in values}
    assert {v["value"] for v in values} == {"illustration", "colophon", "massorah"}


def test_material_values_merge_the_sources(conn):
    values = {v["value"]: v["count"] for v in criterion_values(conn, "material", set(UNIVERSE))}
    assert values == {"Paper": 2, "Vellum": 2, NOT_RECORDED: 3}


# ── shapes and the address (D8) ─────────────────────────────────────────

def test_clean_criteria_canonical_form():
    clean, problems = clean_criteria({
        "lines": {"min": "25", "max": "20"},             # reversed -> swapped
        "style": {"values": ["Square", "Square", " Cursive "]},
        "region": {"values": []},                        # empty -> dropped
        "period": {},
    })
    assert problems == []
    assert clean == {"style": {"values": ["Square", "Cursive"], "mode": "only"},
                     "lines": {"source": "either", "min": 20, "max": 25}}


@pytest.mark.parametrize("raw", [
    {"nonsense": {"values": ["x"]}},
    {"style": {"values": "Square"}},
    {"style": {"values": ["Square"], "mode": "never"}},
    {"style": {"values": ["Square"], "source": "measured"}},   # one source only
    {"attributes": {"values": [NOT_RECORDED]}},
    {"attributes": {"values": ["glitter"]}},
    {"lines": {"min": "twenty"}},
    {"lines": {"min": 20.5}},                                  # whole numbers
    {"lines": {"min": 20, "not_recorded": "yes"}},
    {"lines": {"min": 20, "extra": 1}},
])
def test_clean_criteria_rejects_malformed_input(raw):
    with pytest.raises(ValueError):
        clean_criteria(raw)
    clean, problems = clean_criteria(raw, strict=False)
    assert clean == {} and len(problems) == 1


def test_address_round_trip():
    criteria, _ = clean_criteria({
        "style": {"values": ["Square", "Semi-Cursive"]},
        "region": {"values": ["Oriental"], "mode": "except"},
        "material": {"values": ["Vellum", NOT_RECORDED], "source": "catalogue"},
        "lines": {"min": 20, "max": 25, "source": "measured"},
        "period": {"min": -1200, "max": -1000},
        "columns": {"min": 2, "max": 2, "not_recorded": True},
        "height": {"max": 25.5},
        "dpi": {"not_recorded": True},
        "status": {"values": ["Torn"], "mode": "except"},
    })
    params = criteria_to_params(criteria)
    assert params == {
        "period": "-1200..-1000", "style": "Square,Semi-Cursive", "region": "!Oriental",
        "columns": "2..2,_none", "lines": "meas:20..25", "status": "!Torn",
        "material": "cat:Vellum,_none", "height": "..25.5", "dpi": "_none",
    }
    assert criteria_from_params(params) == (criteria, [])


def test_material_measured_except_puts_the_source_first():
    criteria, _ = clean_criteria({"material": {"values": ["Paper"], "mode": "except",
                                               "source": "measured"}})
    assert criteria_to_params(criteria) == {"material": "meas:!Paper"}
    assert criteria_from_params({"material": "meas:!Paper"})[0] == criteria


def test_address_drops_and_reports_bad_values():
    criteria, problems = criteria_from_params(
        {"lines": "!20..25", "width": "abc", "style": "Square", "domain": "Bible"})
    assert criteria == {"style": {"values": ["Square"], "mode": "only"}}
    assert [key for key, _ in problems] == ["lines", "width"]
