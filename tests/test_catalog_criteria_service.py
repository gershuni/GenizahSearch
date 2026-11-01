"""FjmsService with catalogue criteria and the corpus universe (PR 1a of
docs/plans/CATALOGUE_CRITERIA_PLAN.md): "Search only in..." lookups (D17),
Catalog Browse over the corpus (D16, D22), fail-closed browse (rule 9), the
version-aware data-file choice. Data: tests/criteria_sidecar.py."""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from criteria_sidecar import A, B, C, D, E, F, UNIVERSE, build_criteria_sidecar  # noqa: E402

from shared.fjms_service import (  # noqa: E402
    FILTER_SUPPORTED, FILTER_UNSUPPORTED, FilterUnavailable, FjmsService, _choose_sidecar,
)

CATALOGUED_ORDER = [A, B, C, F]       # sys_id order
UNCATALOGUED_ORDER = [D, E]


@pytest.fixture(scope="module")
def svc(tmp_path_factory):
    return FjmsService(db_path=build_criteria_sidecar(tmp_path_factory.mktemp("s") / "f.db"))


@pytest.fixture(scope="module")
def old_svc(tmp_path_factory):
    return FjmsService(db_path=build_criteria_sidecar(
        tmp_path_factory.mktemp("o") / "f.db", upgrade=False))


U = set(UNIVERSE)


# ── "Search only in..." (get_filter_sys_ids) ────────────────────────────

def test_criteria_alone_can_select_uncatalogued_manuscripts(svc):
    assert svc.get_filter_sys_ids(criteria={"region": {"values": ["_none"]}},
                                  universe=U) == {D, E, F}


def test_criteria_and_older_filters_combine(svc):
    assert svc.get_filter_sys_ids(material_include=["Vellum"],
                                  criteria={"style": {"values": ["Square"]}}) == {A}


def test_exclusions_let_uncatalogued_manuscripts_through(svc):
    # D17: nothing says D or E belong to the excluded material.
    assert svc.get_filter_sys_ids(material_exclude=["Vellum"], universe=U) == {C, F, D, E}


def test_inclusions_cannot_match_uncatalogued_manuscripts(svc):
    assert svc.get_filter_sys_ids(material_include=["Paper"], universe=U) == {A}
    assert svc.get_filter_sys_ids(material_exclude=["Vellum"], date_from=1000,
                                  universe=U) == set()   # C and F are undated


def test_without_a_universe_the_older_filters_are_unchanged(svc):
    assert svc.get_filter_sys_ids(material_exclude=["Vellum"]) == {C, F}


def test_criteria_that_need_the_corpus_fail_closed_without_it(svc):
    with pytest.raises(FilterUnavailable) as err:
        svc.get_filter_sys_ids(criteria={"region": {"values": ["Oriental"], "mode": "except"}})
    assert err.value.reason == "corpus_unavailable"


def test_a_corpus_list_that_is_not_loaded_keeps_its_reason(svc):
    def not_loaded():
        raise FilterUnavailable('corpus_unavailable', 'loading')
    with pytest.raises(FilterUnavailable) as err:
        svc.get_filter_sys_ids(material_exclude=["Vellum"], universe=not_loaded)
    assert err.value.reason == "corpus_unavailable"
    # Not needed, not called: an inclusion filter never waits for the corpus.
    assert svc.get_filter_sys_ids(material_include=["Paper"], universe=not_loaded) == {A}


def test_a_5_0_0_file_reports_the_new_criteria_as_missing(old_svc):
    with pytest.raises(FilterUnavailable) as err:
        old_svc.get_filter_sys_ids(criteria={"columns": {"min": 1}}, universe=U)
    assert err.value.reason == "column_missing"
    assert err.value.filters == ("columns",)


def test_criteria_support(svc, old_svc):
    assert svc.criteria_support() == FILTER_SUPPORTED
    assert old_svc.criteria_support() == FILTER_UNSUPPORTED


def test_criterion_values_are_cached_copies(svc):
    first = svc.get_criterion_values("region", U)
    first[0]["count"] = -1
    assert svc.get_criterion_values("region", U)[0]["count"] == 1


# ── Catalog Browse over the corpus ──────────────────────────────────────

def ids(result):
    return [row["sys_id"] for row in result["results"]]


def test_browse_lists_the_whole_corpus_catalogued_first(svc):
    result = svc.get_browse_results(universe=U)
    assert result["total"] == 6
    assert ids(result) == CATALOGUED_ORDER + UNCATALOGUED_ORDER
    assert [row["catalogued"] for row in result["results"]] == [True] * 4 + [False] * 2


def test_browse_pages_across_the_boundary(svc):
    assert ids(svc.get_browse_results(universe=U, offset=3, limit=2)) == [F, D]
    assert ids(svc.get_browse_results(universe=U, offset=5, limit=2)) == [E]
    assert svc.get_browse_results(universe=U, offset=3, limit=2)["total"] == 6


def test_browse_without_a_universe_is_catalogued_only(svc):
    result = svc.get_browse_results()
    assert ids(result) == CATALOGUED_ORDER and result["total"] == 4


def test_browse_with_criteria(svc):
    result = svc.get_browse_results(criteria={"region": {"values": ["_none"]}}, universe=U)
    assert ids(result) == [F, D, E] and result["total"] == 3
    empty = svc.get_browse_results(criteria={"style": {"values": ["Kufic"]}}, universe=U)
    assert empty == {"results": [], "total": 0}


def test_browse_inclusion_filters_drop_uncatalogued_rows(svc):
    assert ids(svc.get_browse_results(date_from=1000, universe=U)) == [A, B]
    assert ids(svc.get_browse_results(domain="Bible", universe=U)) == [A]
    assert ids(svc.get_browse_results(text_any=["Title"], universe=U)) == CATALOGUED_ORDER


def test_browse_stays_inside_the_corpus(svc):
    # The catalogue has a sys_id libraries.csv lacks (one in the 5.0.0 file):
    # the corpus list bounds the catalogued rows too.
    inside = U - {F}
    result = svc.get_browse_results(universe=inside)
    assert F not in ids(result) and result["total"] == 5
    assert svc.get_browse_library_facets(sys_id_to_library=lambda s: "X",
                                         universe=inside) == {"X": 5}


def test_cached_results_follow_the_corpus_contents(svc):
    # Two corpora of one size must not share a cached criteria result.
    vellum = {"material": {"values": ["Vellum"]}}
    assert svc.get_filter_sys_ids(criteria=vellum, universe={A}) == {A}
    assert svc.get_filter_sys_ids(criteria=vellum, universe={B}) == {B}
    assert ids(svc.get_browse_results(criteria=vellum, universe={A})) == [A]
    assert ids(svc.get_browse_results(criteria=vellum, universe={B})) == [B]


class _OneHash(frozenset):
    """Unequal sets that hash alike (Codex built a real 66-id pair)."""

    def __hash__(self):
        return 1


def test_cached_results_follow_the_corpus_not_its_hash(svc):
    vellum = {"material": {"values": ["Vellum"]}}
    first, second = _OneHash({A, C}), _OneHash({B, C})
    assert ids(svc.get_browse_results(criteria=vellum, universe=first)) == [A]
    assert ids(svc.get_browse_results(criteria=vellum, universe=second)) == [B]
    assert ids(svc.get_browse_results(universe=first)) == [A, C]
    assert ids(svc.get_browse_results(universe=second)) == [B, C]
    def styles(universe):
        return [v["value"] for v in svc.get_criterion_values("style", universe=universe)]
    assert styles(first) == ["Square", "_none"]         # C: not recorded (D5)
    assert styles(second) == ["Semi-Cursive", "_none"]


def test_browse_exclusion_filters_keep_uncatalogued_rows(svc):
    assert ids(svc.get_browse_results(text_not=["zzz"], universe=U))[-2:] == UNCATALOGUED_ORDER


def test_browse_pgp_and_library_filters_apply_to_uncatalogued_rows(svc):
    assert ids(svc.get_browse_results(universe=U, pgp_filter="has_pgp",
                                      pgp_sys_ids={A, D})) == [A, D]
    assert ids(svc.get_browse_results(universe=U, library_codes=["X"], library_sys_ids={B, E},
                                      library_mode="hide")) == [A, C, F, D]


def test_browse_shows_copy_spans(svc):
    rows = {row["sys_id"]: row for row in svc.get_browse_results(universe=U)["results"]}
    assert rows[A]["copy_span"] == "1050–1100"
    assert rows[B]["copy_span"] == "1027"       # end before start: the start year
    assert rows[C]["copy_span"] == ""           # -99 with an end: no date
    assert rows[D]["copy_span"] == ""


def test_library_facets_count_uncatalogued_rows(svc):
    libraries = {A: "CUL", B: "CUL", C: "JTS", D: "JTS", E: "BL", F: "BL"}
    assert svc.get_browse_library_facets(sys_id_to_library=libraries.get,
                                         universe=U) == {"CUL": 2, "JTS": 2, "BL": 2}
    assert svc.get_browse_library_facets(
        sys_id_to_library=libraries.get, universe=U,
        criteria={"material": {"values": ["Paper"]}}) == {"CUL": 1, "JTS": 1}


def test_browse_fails_closed(svc, old_svc):
    with pytest.raises(FilterUnavailable) as err:
        old_svc.get_browse_results(criteria={"period": {"min": 1000}}, universe=U)
    assert err.value.reason == "column_missing"
    with pytest.raises(FilterUnavailable) as err:
        svc.get_browse_results(criteria={"region": {"values": ["_none"]}})
    assert err.value.reason == "corpus_unavailable"


def test_a_failed_browse_query_is_an_error_not_zero(tmp_path):
    # It used to be logged and shown as "0 manuscripts".
    broken = FjmsService(db_path=build_criteria_sidecar(tmp_path / "f.db", fts=False))
    with pytest.raises(FilterUnavailable) as err:
        broken.get_browse_results(text_all=["x"])     # this file has no catalog_fts
    assert err.value.reason == "query_failed"
    with pytest.raises(FilterUnavailable):
        broken.get_browse_library_facets(text_all=["x"], sys_id_to_library=str)


# ── which data file the desktop opens ───────────────────────────────────

def _versioned(path, version):
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE meta (key TEXT, value TEXT)")
    con.execute("INSERT INTO meta VALUES ('version', ?)", (version,))
    con.commit()
    con.close()
    return str(path)


def test_an_older_download_does_not_mask_a_newer_bundled_file(tmp_path):
    user = _versioned(tmp_path / "user.db", "5.0.0")
    bundled = _versioned(tmp_path / "bundled.db", "5.1.0")
    assert _choose_sidecar(user, bundled) == bundled
    assert _choose_sidecar(_versioned(tmp_path / "u2.db", "5.1.0"), bundled).endswith("u2.db")
    assert _choose_sidecar(user, str(tmp_path / "absent.db")) == user
    assert _choose_sidecar(str(tmp_path / "absent.db"), bundled) == bundled
    assert not (tmp_path / "absent.db").exists()      # probing never creates a file
