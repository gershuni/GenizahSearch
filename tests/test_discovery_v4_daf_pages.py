"""Per-daf/amud Wikisource acquisition -- the Zohar-class source shape (C8).

A Hebrew Wikisource work paginated per daf/amud (``"זהר חלק א א ב"`` = prefix
``"זהר חלק א"`` + daf numeral ``"א"`` + amud letter ``"ב"``) takes its locus
identity from PARSING each fetched page's own returned title, never from
enumeration order (``daf_bavli``'s Sefaria ordinal geometry does not
transfer). Missing/empty pages are a hard error -- the chapter-link
Wikisource path's ``coverage_status="partial"`` escape is NOT available in
this mode.

All fixtures here are synthetic (a made-up prefix, made-up Hebrew filler
text): nothing from ``same_work_spike/`` or any restricted corpus, and no
source-map identities are added by this test file (those await owner
approval per the V4.2 plan).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from shared.discovery_locus import daf_label_he, heb_numeral

from scripts.discovery_v4_common import load_source_config, reference_namespace
from scripts.discovery_v4_fetch_sources import (
    Fetcher,
    _acquire_wikisource_daf_pages,
    parse_daf_page_title,
    run as fetch_run,
)

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
V4_MAP = SCRIPTS / "discovery_v4_sources.json"
V4_1_MAP = SCRIPTS / "discovery_v4_1_sources.json"
V4_2_MAP = SCRIPTS / "discovery_v4_2_sources.json"

# A synthetic prefix -- not a real Wikisource work title -- used across every
# test in this file.
PREFIX = "בדיקה זהר חלק דמו"


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


class FakeFetcher:
    """Stand-in for ``Fetcher``: canned per-title ``wikisource_parse`` replies.

    Only the two attributes/methods ``_acquire_wikisource_daf_pages`` actually
    uses are implemented: ``raw_dir`` and ``wikisource_parse``. Every request
    is recorded in ``requested`` so a test can assert a page was (or, for the
    anomalous-sibling case, was NOT) ever asked for.
    """

    def __init__(self, raw_root: Path, pages: dict[str, dict]) -> None:
        self.raw_dir = raw_root
        self._pages = pages
        self.requested: list[str] = []

    def wikisource_parse(self, page: str, raw_path: Path) -> dict:
        self.requested.append(page)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        doc = self._pages.get(page, {"error": {"code": "missingtitle"}})
        raw_path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        return doc


def _page_title(prefix: str, daf: int, amud_letter: str) -> str:
    return f"{prefix} {heb_numeral(daf)} {amud_letter}"


def _page_doc(title: str, revid: int, hebrew_sentence: str) -> dict:
    return {
        "parse": {
            "title": title,
            "revid": revid,
            "text": f'<div class="mw-parser-output"><p>{hebrew_sentence}</p></div>',
        }
    }


def _build_pages(
    prefix: str, daf_first: int, daf_last: int, *, retitle: dict | None = None
) -> dict[str, dict]:
    """Build a full, valid page dict for ``daf_first..daf_last`` (inclusive).

    ``retitle`` maps ``(daf, amud_letter) -> replacement returned title``, for
    simulating a redirect/mislabeled page whose OWN title parses differently
    from what was requested.
    """
    retitle = retitle or {}
    pages: dict[str, dict] = {}
    revid = 1000
    for daf in range(daf_first, daf_last + 1):
        for amud_letter in ("א", "ב"):
            title = _page_title(prefix, daf, amud_letter)
            returned_title = retitle.get((daf, amud_letter), title)
            pages[title] = _page_doc(
                returned_title, revid, f"טקסט בדיקה סינתטי {daf} {amud_letter}"
            )
            revid += 1
    return pages


def _daf_pages_source(
    *, key: str = "daf_test", prefix: str = PREFIX, daf_range: list[int]
) -> dict:
    return {
        "key": key,
        "provider": "hewikisource",
        "mode": "daf_pages",
        "link_prefix": prefix,
        "daf_range": daf_range,
        "mappings": [{"target_work_id": "w000001"}],
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_three_daf_range_yields_six_units_with_right_labels_and_ordinals(
    tmp_path: Path,
):
    pages = _build_pages(PREFIX, 1, 3)
    fetcher = FakeFetcher(tmp_path / "raw", pages)
    source = _daf_pages_source(daf_range=[1, 3])

    acquired, raw_paths = _acquire_wikisource_daf_pages(fetcher, source)

    assert acquired["coverage_status"] == "complete"
    assert acquired["missing_pages"] == []
    assert acquired["locus_grain"] == "daf"
    assert acquired["daf_range"] == [1, 3]
    assert acquired["page_count"] == 6
    assert len(acquired["units"]) == 6
    assert len(raw_paths) == 6

    expected_ordinal = 1
    for daf in (1, 2, 3):
        for amud_index, amud_letter in enumerate(("א", "ב")):
            unit = acquired["units"][expected_ordinal - 1]
            assert unit["ordinal"] == expected_ordinal
            assert unit["label"] == daf_label_he(daf, amud_index + 1)
            assert unit["provider_ref"] == _page_title(PREFIX, daf, amud_letter)
            assert unit["hebrew_letters"] > 0
            assert unit["text"]
            expected_ordinal += 1


def test_happy_path_never_requests_a_page_outside_the_declared_range(tmp_path: Path):
    pages = _build_pages(PREFIX, 1, 2)
    fetcher = FakeFetcher(tmp_path / "raw", pages)
    source = _daf_pages_source(daf_range=[1, 2])
    _acquire_wikisource_daf_pages(fetcher, source)
    assert len(fetcher.requested) == 4
    assert len(set(fetcher.requested)) == 4


# ---------------------------------------------------------------------------
# Completeness gate: missing page, empty page, no partial escape
# ---------------------------------------------------------------------------


def test_missing_page_is_a_hard_error_naming_it(tmp_path: Path):
    pages = _build_pages(PREFIX, 1, 2)
    del pages[_page_title(PREFIX, 2, "ב")]  # simulate a page that doesn't exist
    fetcher = FakeFetcher(tmp_path / "raw", pages)
    source = _daf_pages_source(daf_range=[1, 2])

    with pytest.raises(ValueError) as excinfo:
        _acquire_wikisource_daf_pages(fetcher, source)
    message = str(excinfo.value)
    assert _page_title(PREFIX, 2, "ב") in message
    assert "daf-page completeness gate failed" in message


def test_empty_page_is_a_hard_error_naming_it(tmp_path: Path):
    pages = _build_pages(PREFIX, 1, 2)
    empty_title = _page_title(PREFIX, 1, "ב")
    pages[empty_title] = _page_doc(empty_title, 42, "12, 34 !!")  # no Hebrew letters
    fetcher = FakeFetcher(tmp_path / "raw", pages)
    source = _daf_pages_source(daf_range=[1, 2])

    with pytest.raises(ValueError) as excinfo:
        _acquire_wikisource_daf_pages(fetcher, source)
    message = str(excinfo.value)
    assert empty_title in message
    assert "daf-page completeness gate failed" in message


def test_no_partial_coverage_status_escape_in_daf_pages_mode(tmp_path: Path):
    """C8: unlike the chapter-link Wikisource path, a missing page here can
    NEVER surface as ``coverage_status="partial"`` -- it is always a raise."""
    pages = _build_pages(PREFIX, 1, 1)
    del pages[_page_title(PREFIX, 1, "ב")]
    fetcher = FakeFetcher(tmp_path / "raw", pages)
    source = _daf_pages_source(daf_range=[1, 1])
    with pytest.raises(ValueError):
        _acquire_wikisource_daf_pages(fetcher, source)


def test_anomalous_sibling_page_under_the_prefix_is_never_fetched(tmp_path: Path):
    """זהר חדש-style '<letter><digit>' suffixes are not valid daf/amud pages;
    enumeration never constructs such a title, so it is simply never asked
    for -- the fetch never even sees it, whether or not it exists."""
    pages = _build_pages(PREFIX, 1, 1)
    anomalous_title = f"{PREFIX} א1"
    pages[anomalous_title] = _page_doc(anomalous_title, 7, "טקסט חריג")
    fetcher = FakeFetcher(tmp_path / "raw", pages)
    source = _daf_pages_source(daf_range=[1, 1])

    acquired, _ = _acquire_wikisource_daf_pages(fetcher, source)

    assert acquired["coverage_status"] == "complete"
    assert anomalous_title not in fetcher.requested


# ---------------------------------------------------------------------------
# Traditional pagination euphemisms (live-verified on זהר חלק ג, 2026-08-17)
# ---------------------------------------------------------------------------


def test_euphemism_transposed_daf_numeral_parses_and_label_stays_canonical(
    tmp_path: Path,
):
    """Hebrew pagination avoids spelling offensive words: the REAL page for
    daf 298 carries the TRANSPOSED numeral (רחצ, not the canonical רצח),
    the canonical title being only a redirect to it. Requesting the
    canonical title with redirects therefore returns the transposed title,
    which the parser must accept as 298 -- while the unit label stays the
    CANONICAL ``daf_label_he`` form (the builder's daf-grain geometry check
    recomputes it)."""
    pages = _build_pages(
        PREFIX,
        297,
        299,
        retitle={
            (298, "א"): f"{PREFIX} רחצ א",
            (298, "ב"): f"{PREFIX} רחצ ב",
        },
    )
    fetcher = FakeFetcher(tmp_path / "raw", pages)
    source = _daf_pages_source(daf_range=[297, 299])

    acquired, _ = _acquire_wikisource_daf_pages(fetcher, source)

    assert acquired["coverage_status"] == "complete"
    assert acquired["page_count"] == 6
    unit = acquired["units"][2]  # daf 298, amud א
    assert unit["label"] == daf_label_he(298, 1)
    assert unit["provider_ref"] == f"{PREFIX} רחצ א"


def test_euphemism_variants_parse_to_their_values():
    assert parse_daf_page_title(f"{PREFIX} ער א", PREFIX) == (270, 1)
    assert parse_daf_page_title(f"{PREFIX} ערה ב", PREFIX) == (275, 2)
    assert parse_daf_page_title(f"{PREFIX} רחצ א", PREFIX) == (298, 1)


def test_non_traditional_transposition_is_still_rejected():
    # חצר also sums to 298 by gematria, but it is neither the canonical
    # rendering nor the traditional euphemism -- the strict table refuses it.
    with pytest.raises(ValueError, match="unrecognized daf numeral"):
        parse_daf_page_title(f"{PREFIX} חצר א", PREFIX)


# ---------------------------------------------------------------------------
# Parse/enumeration cross-check (the anti-drift gate)
# ---------------------------------------------------------------------------


def test_parse_enumeration_mismatch_is_a_hard_error(tmp_path: Path):
    """A fetched page's OWN title (e.g. after a wiki redirect) disagreeing
    with what was requested must fail loudly, never silently accept the
    redirect target's identity."""
    pages = _build_pages(
        PREFIX,
        1,
        1,
        retitle={(1, "א"): _page_title(PREFIX, 2, "א")},
    )
    fetcher = FakeFetcher(tmp_path / "raw", pages)
    source = _daf_pages_source(daf_range=[1, 1])

    with pytest.raises(ValueError) as excinfo:
        _acquire_wikisource_daf_pages(fetcher, source)
    message = str(excinfo.value)
    assert "parse/enumeration mismatch" in message


def test_parse_response_without_a_title_is_a_hard_error(tmp_path: Path):
    """A successful parse response that carries NO title must fail, not fall
    back to the requested title -- a fallback would pass the anti-drift
    cross-check vacuously (comparing the expectation against itself)."""
    pages = _build_pages(PREFIX, 1, 1)
    del pages[_page_title(PREFIX, 1, "א")]["parse"]["title"]
    fetcher = FakeFetcher(tmp_path / "raw", pages)
    source = _daf_pages_source(daf_range=[1, 1])

    with pytest.raises(ValueError) as excinfo:
        _acquire_wikisource_daf_pages(fetcher, source)
    assert "no title" in str(excinfo.value)


# ---------------------------------------------------------------------------
# parse_daf_page_title: strict grammar battery
# ---------------------------------------------------------------------------


def test_parser_round_trips_every_daf_1_to_600_both_amudim():
    for value in range(1, 601):
        numeral = heb_numeral(value)
        # heb_numeral never emits gershayim (its own docstring: "no thousands
        # and no geresh"); the parser's reverse table is built by calling it,
        # so this holds by construction -- pinned here as a live check too.
        assert "׳" not in numeral and "״" not in numeral
        for amud_letter, amud in (("א", 1), ("ב", 2)):
            title = f"{PREFIX} {numeral} {amud_letter}"
            assert parse_daf_page_title(title, PREFIX) == (value, amud)


def test_parser_round_trips_the_special_15_and_16_forms():
    assert heb_numeral(15) == "טו"
    assert heb_numeral(16) == "טז"
    assert parse_daf_page_title(f"{PREFIX} טו א", PREFIX) == (15, 1)
    assert parse_daf_page_title(f"{PREFIX} טז ב", PREFIX) == (16, 2)


def test_parser_rejects_zohar_chadash_style_suffix_glued_to_one_token():
    with pytest.raises(ValueError, match="does not split into exactly two tokens"):
        parse_daf_page_title(f"{PREFIX} א1", PREFIX)


def test_parser_rejects_zohar_chadash_style_suffix_as_a_separate_token():
    with pytest.raises(ValueError, match="unrecognized amud"):
        parse_daf_page_title(f"{PREFIX} א 1", PREFIX)


def test_parser_rejects_a_title_with_no_daf_or_amud_at_all():
    with pytest.raises(ValueError, match="does not start with"):
        parse_daf_page_title(PREFIX, PREFIX)


def test_parser_rejects_an_extra_trailing_token():
    with pytest.raises(ValueError, match="does not split into exactly two tokens"):
        parse_daf_page_title(f"{PREFIX} א ב נספח", PREFIX)


def test_parser_rejects_a_disagreeing_prefix():
    with pytest.raises(ValueError, match="does not start with"):
        parse_daf_page_title("אחר לגמרי א ב", PREFIX)


def test_parser_rejects_an_unrecognized_daf_numeral():
    with pytest.raises(ValueError, match="unrecognized daf numeral"):
        parse_daf_page_title(f"{PREFIX} קשקוש ב", PREFIX)


def test_parser_rejects_an_amud_beyond_alef_bet():
    with pytest.raises(ValueError, match="unrecognized amud"):
        parse_daf_page_title(f"{PREFIX} א ג", PREFIX)


def test_parser_rejects_double_space_before_the_daf_token():
    with pytest.raises(ValueError, match="does not split into exactly two tokens"):
        parse_daf_page_title(f"{PREFIX}  א ב", PREFIX)


# ---------------------------------------------------------------------------
# Manifest entry shape (end to end through run())
# ---------------------------------------------------------------------------


def test_run_end_to_end_records_daf_pages_manifest_fields(tmp_path, monkeypatch):
    pages = _build_pages(PREFIX, 1, 2)

    def fake_wikisource_parse(self, page: str, raw_path: Path) -> dict:
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        doc = pages.get(page, {"error": {"code": "missingtitle"}})
        raw_path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        return doc

    monkeypatch.setattr(Fetcher, "wikisource_parse", fake_wikisource_parse)

    source_map = tmp_path / "sources.json"
    source_map.write_text(
        json.dumps(
            {
                "schema_version": "discovery-v4-sources-v1",
                "reference_namespace": "REF6",
                "license_allowlist": ["cc-by-sa"],
                "minimum_hebrew_letters": 1,
                "sources": [_daf_pages_source(daf_range=[1, 2])],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        source_map=str(source_map),
        output_dir=str(tmp_path / "out"),
        timeout=5,
        reuse_existing=False,
    )
    manifest = fetch_run(args)

    assert manifest["summary"]["acquired_sources"] == 1
    entry = manifest["entries"][0]
    assert entry["status"] == "acquired"
    assert entry["mode"] == "daf_pages"
    assert entry["locus_grain"] == "daf"
    assert entry["daf_range"] == [1, 2]
    assert entry["page_count"] == 4
    assert entry["unit_count"] == 4
    assert entry["hebrew_letters"] > 0


# ---------------------------------------------------------------------------
# Schema (load_source_config): additive daf_pages validation
# ---------------------------------------------------------------------------


def _write_map(tmp_path: Path, source: dict) -> Path:
    path = tmp_path / "sources.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "discovery-v4-sources-v1",
                "reference_namespace": "REF6",
                "sources": [source],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def test_schema_accepts_a_well_formed_daf_pages_source(tmp_path: Path):
    path = _write_map(tmp_path, _daf_pages_source(daf_range=[1, 340]))
    config = load_source_config(path)
    assert reference_namespace(config) == "REF6"
    assert config["sources"][0]["mode"] == "daf_pages"


def test_schema_rejects_an_unsupported_mode(tmp_path: Path):
    source = _daf_pages_source(daf_range=[1, 2])
    source["mode"] = "something_else"
    path = _write_map(tmp_path, source)
    with pytest.raises(ValueError, match="unsupported mode"):
        load_source_config(path)


def test_schema_rejects_daf_pages_mode_on_a_non_wikisource_provider(tmp_path: Path):
    source = _daf_pages_source(daf_range=[1, 2])
    source["provider"] = "sefaria"
    path = _write_map(tmp_path, source)
    with pytest.raises(ValueError, match="requires provider 'hewikisource'"):
        load_source_config(path)


def test_schema_rejects_mode_combined_with_container(tmp_path: Path):
    source = _daf_pages_source(daf_range=[1, 2])
    # Container's own check requires provider "sefaria" first; give it that so
    # the assertion below actually exercises the mode-vs-container guard
    # rather than the (also correct, but different) provider mismatch.
    source["provider"] = "sefaria"
    source["container"] = True
    source["children"] = [{"child_key": "a", "source_ref": "Ref A"}]
    path = _write_map(tmp_path, source)
    with pytest.raises(ValueError, match="cannot also declare a mode"):
        load_source_config(path)


@pytest.mark.parametrize("bad_prefix", ["", "   ", None])
def test_schema_rejects_an_empty_link_prefix(tmp_path: Path, bad_prefix):
    source = _daf_pages_source(daf_range=[1, 2])
    source["link_prefix"] = bad_prefix
    path = _write_map(tmp_path, source)
    with pytest.raises(ValueError, match="link_prefix"):
        load_source_config(path)


@pytest.mark.parametrize(
    "bad_range",
    [
        None,
        [1],
        [1, 2, 3],
        [1, "2"],
        [1.0, 2],
        [0, 5],
        [5, 1],
        [1, 1000],
        [True, 2],
    ],
)
def test_schema_rejects_a_malformed_or_insane_daf_range(tmp_path: Path, bad_range):
    source = _daf_pages_source(daf_range=[1, 2])
    source["daf_range"] = bad_range
    path = _write_map(tmp_path, source)
    with pytest.raises(ValueError, match="daf_range"):
        load_source_config(path)


# ---------------------------------------------------------------------------
# Regression: existing V4/V4.1/V4.2 maps still load identically
# ---------------------------------------------------------------------------


def test_existing_v4_v41_v42_maps_still_load_identically():
    # None of these declare "daf_pages" (no source-map identities are added by
    # this task -- they await owner approval); V4/V4.1 do carry the
    # PRE-EXISTING "schema_leaves" Sefaria mode, which the new validation
    # block must let through completely unchanged.
    v4 = load_source_config(V4_MAP)
    assert reference_namespace(v4) == "REF4"
    assert len(v4["sources"]) == 43
    assert {source.get("mode") for source in v4["sources"]} <= {None, "schema_leaves"}
    assert all(source.get("mode") != "daf_pages" for source in v4["sources"])

    v4_1 = load_source_config(V4_1_MAP)
    assert reference_namespace(v4_1) == "REF5"
    assert len(v4_1["sources"]) == 10
    assert {source.get("mode") for source in v4_1["sources"]} <= {None, "schema_leaves"}
    assert all(source.get("mode") != "daf_pages" for source in v4_1["sources"])

    v4_2 = load_source_config(V4_2_MAP)
    assert reference_namespace(v4_2) == "REF6"
    # 15 Mishneh Torah containers + the four post-sitting (2026-08-16)
    # private_sibling additions + 32 owner-approved public_first additions
    # (this session); the exact composition of each group is pinned in
    # tests/test_discovery_v4_2_containers.py.
    assert len(v4_2["sources"]) == 50
    non_public_first = [
        source
        for source in v4_2["sources"]
        if source.get("identity_mode") != "public_first"
    ]
    assert len(non_public_first) == 19
    assert all(source.get("mode") is None for source in non_public_first)
    # The public_first additions DO carry "daf_pages" (the 3 Zohar cheleks)
    # and "schema_leaves" (8 Sefaria works) modes -- proving C5 composes
    # with both pre-existing modes exactly as designed (the Zohar shape).
    public_first_modes = {
        source.get("mode")
        for source in v4_2["sources"]
        if source.get("identity_mode") == "public_first"
    }
    assert public_first_modes == {None, "daf_pages", "schema_leaves"}


# ---------------------------------------------------------------------------
# Build-time locus for the "daf" grain (discovery_v4_build_reference.py)
# ---------------------------------------------------------------------------
#
# The acquisition above births the units; these tests pin what the reference
# builder does with them. Before the "daf" branch existed, a daf_pages work
# fell to the generic label branch and re-derived "{title} {heb_numeral(N)}"
# from the raw ordinal -- mislabeling every amud.

from scripts.discovery_v4_build_reference import (  # noqa: E402
    _locus_grain,
    _locus_label,
)


def _daf_unit(ordinal: int, daf: int, amud: int) -> dict:
    return {"ordinal": ordinal, "label": daf_label_he(daf, amud), "text": "א"}


def test_locus_grain_derives_daf_from_daf_pages_mode():
    assert _locus_grain({"mode": "daf_pages"}) == "daf"
    # Explicit locus_grain always wins over the mode-implied default.
    assert _locus_grain({"mode": "daf_pages", "locus_grain": "daf_bavli"}) == "daf_bavli"
    assert _locus_grain({"mode": "schema_leaves"}) == "section"
    assert _locus_grain({}) == "chapter"


def test_locus_label_daf_branch_uses_acquired_label_and_bavli_citation_pos():
    source = _daf_pages_source(daf_range=[30, 31])
    work = {"title": "בדיקה"}
    expected = [
        (1, 30, 1),
        (2, 30, 2),
        (3, 31, 1),
        (4, 31, 2),
    ]
    for ordinal, daf, amud in expected:
        unit = _daf_unit(ordinal, daf, amud)
        label, citation_pos = _locus_label(source, {}, work, unit, "daf")
        assert label == daf_label_he(daf, amud)
        # Same convention as the daf_bavli branch, so locus-range filtering
        # treats both daf shapes identically.
        assert citation_pos == daf * 2 + amud - 1


def test_locus_label_daf_geometry_disagreement_is_a_hard_error():
    """An acquired label disagreeing with the ordinal geometry (ordinal =
    2*(daf - daf_first) + amud) must fail loudly, never relabel by guess."""
    source = _daf_pages_source(daf_range=[30, 31])
    unit = _daf_unit(1, 31, 1)  # ordinal 1 under daf_first=30 implies daf 30
    with pytest.raises(ValueError, match="geometry disagreement"):
        _locus_label(source, {}, {"title": "בדיקה"}, unit, "daf")
