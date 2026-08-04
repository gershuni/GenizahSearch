# -*- coding: utf-8 -*-
"""The discovery panel's display model (Phase 136, plan 136-15, PANEL-01/02).

Every fixture here is named after the REAL case it protects, because every one
of these display rules was found by looking at real data rather than by
reasoning:

* ``two_titles_duplicate``      -- the mockup's real page showed the SAME work
  twice under two titles (``w000190`` / ``w001382``, 921 row-pairs corpus-wide).
* ``verse_chain_generic_group`` -- a prayer book whose page-6 verse-chain pulled
  in one halakhic work TWICE (two titles, one canonical work) plus an unrelated
  midrashic collection, all on byte-identical offsets 0-555.
* ``two_granularity_rashi``     -- T-S Misc. 12.31.14: ``w000171`` and
  ``w001281`` on the identical span 0-962, the SAME commentary at two
  catalogued granularities.
* ``sixty_six_letter_liturgical`` -- a siddur whose liturgical matches are 66
  matched letters each, below the ratified 150-letter floor.
* ``two_human_confirmed_rows``  -- Moss. V,374: two rows a human confirmed,
  treated differently because routing dropped one of them.
* ``curated_w000176_liturgy``   -- ruling R's curated display title.

The envelope fixtures are built from the EXACT live four-key shape; a fixture
that invented a row shape would prove nothing about the code that will run.
"""

from __future__ import annotations

import ast
import io
import pathlib
import re

import pytest

import scripts.discovery_ids as ids
import shared.discovery_display_strings as ds
import shared.discovery_grouping as grouping
import shared.discovery_panel_model as pm
from shared.discovery_band_labels import band_label
from shared.discovery_main_pool import (
    REASON_INSUFFICIENT_LENGTH,
    REASON_MAIN_FULL_COVERAGE,
    REASON_MAIN_HUMAN_CONFIRMED,
    REASON_MAIN_MULTIFOLIO,
    SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS,
)
from shared.discovery_surface_projection import (
    SURFACE_CLAIM_FIELDS,
    SURFACE_RELATED_PAGE_FIELDS,
    SURFACE_WORK_SUMMARY_FIELDS,
    make_envelope,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
MODEL_PATH = REPO_ROOT / "shared" / "discovery_panel_model.py"
GATE1_DECISIONS = (
    REPO_ROOT / ".planning" / "phases"
    / "136-read-surfaces-connections-panel-work-witnesses"
    / "136-GATE1-DECISIONS.md"
)

LANGS = ("en", "he")

# The raw recorded title of the ONE curated work (ruling R). The curation
# exists because this bare title tells a reader "Maimonides' halakhic book"
# over pages the owner ruled are mostly liturgy.
W000176_RAW_TITLE = "משנה תורה, ספר אהבה"


def _model_source() -> str:
    return io.open(MODEL_PATH, encoding="utf-8").read()


# ---------------------------------------------------------------------------
# Fixture builders -- the EXACT live row/envelope shapes.
# ---------------------------------------------------------------------------


def claim_row(**overrides):
    """One `surface_safe_claim` row: EVERY allowlisted key, missing as None."""
    row = {field: None for field in SURFACE_CLAIM_FIELDS}
    row.update({
        "page_id": "page-1",
        "sys_id": "990051079570205171",
        "claim_id": "claim-1",
        "evidence_id": "ev-1",
        "work_id": "w000001",
        "canonical_work_id": "w000001",
        "display_work_id": "w000001",
        "neutral_title": "Some Recorded Work",
        "author": None,
        "genre": None,
        "title_missing": False,
        "relation_kind": ids.CLAIM_TYPE_DIRECT_WITNESS,
        "evidence_source": ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        "confidence_band": ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC,
        "band_label": band_label(
            ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
            ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC,
        ),
        "band_rank": 0,
        "coverage_ppm": 680000,
        "coverage_status": "measured",
        "main_pool": True,
        "main_pool_reason": REASON_MAIN_FULL_COVERAGE,
        "identification_id": "ident-1",
        "identification_page_count": 1,
        "novelty_status": "not_checked",
        "novelty_source_label": None,
        "matched_letters": 500,
        "span_start": 0,
        "span_end": 500,
        "n_spans": 1,
        "eligibility_basis": "shipped",
        "restored_by_human_confirmation": False,
        "low_coverage_marker": False,
        "adjudication_status": ids.ADJUDICATION_STATUS_UNREVIEWED,
        "routing_status": ids.ROUTING_STATUS_SHIPPED,
        "routing_reason": None,
        "measurement_status": "measured_pass",
        "default_eligible": True,
    })
    row.update(overrides)
    assert set(row) == set(SURFACE_CLAIM_FIELDS), "fixture drifted from the live allowlist"
    return row


def work_summary_row(**overrides):
    """One `surface_safe_work_summary` row: EVERY allowlisted key."""
    row = {field: None for field in SURFACE_WORK_SUMMARY_FIELDS}
    row.update({
        "canonical_work_id": "w000001",
        "display_work_id": "w000001",
        "neutral_title": "Some Recorded Work",
        "author": None,
        "genre": None,
        "title_missing": False,
        "page_count": 5,
        "best_band_rank": 0,
        "gated": False,
        "main_pool": True,
        "relation_kind": ids.CLAIM_TYPE_DIRECT_WITNESS,
    })
    row.update(overrides)
    assert set(row) == set(SURFACE_WORK_SUMMARY_FIELDS)
    return row


def related_page_row(**overrides):
    """One `surface_safe_related_page` row: EVERY allowlisted key."""
    row = {field: None for field in SURFACE_RELATED_PAGE_FIELDS}
    row.update({
        "related_page_id": "page-99",
        "evidence_id": "ev-99",
        "evidence_source": ids.EVIDENCE_SOURCE_PROPAGATED,
        "confidence_band": ids.CONFIDENCE_BAND_NOT_EVALUATED,
        "band_rank": 6,
        "evidence_row_count": 3,
    })
    row.update(overrides)
    assert set(row) == set(SURFACE_RELATED_PAGE_FIELDS)
    return row


def envelope(status="ok", items=(), total=None, meta=None):
    """A literal four-key envelope -- deliberately NOT built through
    `make_envelope`, so the fixture pins the live SHAPE independently of the
    producer. A separate test asserts the two agree."""
    item_list = list(items)
    if status != "ok":
        return {"status": status, "items": [], "total": 0, "meta": dict(meta or {"reason": "query_timeout"})}
    return {
        "status": "ok",
        "items": item_list,
        "total": len(item_list) if total is None else int(total),
        "meta": dict(meta or {}),
    }


def claims_envelope(items=(), total=None, status="ok", meta=None):
    return envelope(status, items, total,
                    meta if meta is not None else ({"page_id": "page-1", "include_review": False}
                                                   if status == "ok" else None))


def page_ids_envelope(items=("page-1", "page-2"), total=None, status="ok",
                      resolved=True, truncated=False, meta=None):
    if meta is None and status == "ok":
        meta = {"sys_id": "990051079570205171", "resolved": resolved,
                "truncated": truncated, "volume_ie": None}
    return envelope(status, items, total, meta)


def works_envelope(items=(), total=None, status="ok", page_scope_resolved=True, meta=None):
    if meta is None and status == "ok":
        meta = {"page_scope_resolved": page_scope_resolved, "lang": "en"}
    return envelope(status, items, total, meta)


def related_count_envelope(total=0, status="ok", meta=None):
    if meta is None and status == "ok":
        meta = {"unit": "distinct_opposite_pages"}
    return envelope(status, (), total, meta)


def related_rows_envelope(items=(), total=None, status="ok", meta=None):
    if meta is None and status == "ok":
        meta = {"unit": "distinct_opposite_pages"}
    return envelope(status, items, total, meta)


def bundle(claim_items=(), lang="en", show_more=False, **overrides):
    """The default four-eager-envelope bundle, related ROWS left NOT REQUESTED."""
    kwargs = {
        "claims": claims_envelope(claim_items),
        "page_ids": page_ids_envelope(),
        "manuscript_works": works_envelope(),
        "related_count": related_count_envelope(),
        "lang": lang,
        "show_more": show_more,
    }
    kwargs.update(overrides)
    return pm.PanelServiceBundle(**kwargs)


# ---------------------------------------------------------------------------
# The named real-case fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def two_titles_duplicate():
    """The SAME work recorded twice under two titles on one page (D-13a).

    `w000190` carries a title from one identity source, `w001382` the other;
    `canonical_work_id` already records the merge, but claims key on
    `(page_id, work_id)` so dedup never saw them as one."""
    return [
        claim_row(claim_id="claim-a", evidence_id="ev-a", work_id="w000190",
                  canonical_work_id="w001382", display_work_id="w001382",
                  neutral_title="The Canonical Title", span_start=0, span_end=500),
        claim_row(claim_id="claim-b", evidence_id="ev-b", work_id="w001382",
                  canonical_work_id="w001382", display_work_id="w001382",
                  neutral_title="The Canonical Title", span_start=0, span_end=500),
    ]


@pytest.fixture
def verse_chain_generic_group():
    """The prayer book's page-6 verse-chain: one halakhic work claimed TWICE
    under two titles plus an unrelated midrashic collection, all on the
    byte-identical span 0-555 (D-13d, the owner's own case)."""
    return [
        claim_row(claim_id="claim-tur-a", evidence_id="ev-tur-a", work_id="w000300",
                  canonical_work_id="w000300", display_work_id="w000300",
                  neutral_title="Tur, first recorded title", author="יעקב בן אשר",
                  span_start=0, span_end=555, matched_letters=420),
        claim_row(claim_id="claim-tur-b", evidence_id="ev-tur-b", work_id="w000301",
                  canonical_work_id="w000300", display_work_id="w000300",
                  neutral_title="Tur, second recorded title", author="יעקב בן אשר",
                  span_start=0, span_end=555, matched_letters=420),
        claim_row(claim_id="claim-yalkut", evidence_id="ev-yalkut", work_id="w000400",
                  canonical_work_id="w000400", display_work_id="w000400",
                  neutral_title="Yalkut on the Prophets", author="שמעוני",
                  span_start=0, span_end=555, matched_letters=420),
    ]


@pytest.fixture
def two_granularity_rashi():
    """T-S Misc. 12.31.14: the SAME commentary at two catalogued granularities
    on the identical span 0-962, PLUS a genuinely separate identification on
    another span.

    The pair collapses like a duplicate (D-13d's ratified rule) and STAYS an
    identification; together with the separate row the page therefore renders
    TWO identifications and no generic group -- which is the defect the flaw
    note describes, where that manuscript rendered one where it should render
    two."""
    author = "שלמה בן יצחק (רש\"י)"
    return [
        claim_row(claim_id="claim-rashi-torah", evidence_id="ev-rashi-a",
                  work_id="w000171", canonical_work_id="w000171",
                  display_work_id="w000171", neutral_title="רש\"י על התורה",
                  author=author, span_start=0, span_end=962, matched_letters=800),
        claim_row(claim_id="claim-rashi-gen", evidence_id="ev-rashi-b",
                  work_id="w001281", canonical_work_id="w001281",
                  display_work_id="w001281", neutral_title="רש\"י על בראשית",
                  author=author, span_start=0, span_end=962, matched_letters=800),
        claim_row(claim_id="claim-other", evidence_id="ev-other", work_id="w000500",
                  canonical_work_id="w000500", display_work_id="w000500",
                  neutral_title="A genuinely separate work", author="מחבר אחר",
                  span_start=1200, span_end=2000, matched_letters=700),
    ]


@pytest.fixture
def sixty_six_letter_liturgical():
    """A siddur's liturgical matches, 66 matched letters each -- below the
    ratified short-evidence floor, on DISTINCT spans (so the identical-span
    rule is not what decides them). They are GATED, never deleted."""
    return [
        claim_row(claim_id="claim-lit-%d" % i, evidence_id="ev-lit-%d" % i,
                  work_id="w0006%02d" % i, canonical_work_id="w0006%02d" % i,
                  display_work_id="w0006%02d" % i,
                  neutral_title="Liturgical piece %d" % i,
                  span_start=100 * i, span_end=100 * i + 66,
                  matched_letters=66, coverage_ppm=20000,
                  main_pool=False, main_pool_reason=REASON_INSUFFICIENT_LENGTH)
        for i in range(1, 5)
    ]


@pytest.fixture
def two_human_confirmed_rows():
    """Moss. V,374: two rows a human confirmed on one manuscript. Routing
    demoted one of them (`review_only` / `low_coverage`), and the query used to
    drop it before the predicate meant to protect it ever ran (D-13g).

    Behavioral, NOT count-derived: the population figures for this class are
    artifact-specific and are recorded in the summary, never asserted here."""
    return [
        claim_row(claim_id="claim-esther", evidence_id="ev-esther", work_id="w000700",
                  canonical_work_id="w000700", display_work_id="w000700",
                  neutral_title="רש\"י על אסתר", span_start=0, span_end=400,
                  adjudication_status=ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED,
                  routing_status=ids.ROUTING_STATUS_SHIPPED,
                  main_pool=True, main_pool_reason=REASON_MAIN_HUMAN_CONFIRMED,
                  restored_by_human_confirmation=False, low_coverage_marker=False),
        claim_row(claim_id="claim-eicha", evidence_id="ev-eicha", work_id="w000701",
                  canonical_work_id="w000701", display_work_id="w000701",
                  neutral_title="רש\"י על איכה", span_start=600, span_end=700,
                  matched_letters=90, coverage_ppm=40000,
                  adjudication_status=ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED,
                  routing_status=ids.ROUTING_STATUS_REVIEW_ONLY,
                  routing_reason=ids.ROUTING_REASON_LOW_COVERAGE,
                  main_pool=True, main_pool_reason=REASON_MAIN_HUMAN_CONFIRMED,
                  restored_by_human_confirmation=True, low_coverage_marker=True),
    ]


@pytest.fixture
def curated_w000176_liturgy():
    """Ruling R: the 10th most-claimed work in the corpus, whose bare recorded
    title names a halakhic book over pages that are mostly liturgy."""
    return [
        claim_row(claim_id="claim-176", evidence_id="ev-176", work_id="w000176",
                  canonical_work_id="w000176", display_work_id="w000176",
                  neutral_title=W000176_RAW_TITLE, span_start=0, span_end=300),
    ]


# ===========================================================================
# Task 1 -- the input bundle contract
# ===========================================================================


@pytest.mark.parametrize("field_name", pm.ENVELOPE_FIELDS)
@pytest.mark.parametrize("bad_value", [
    pytest.param([], id="bare-empty-list"),
    pytest.param([{"page_id": "page-1"}], id="bare-row-list"),
    pytest.param({"status": "ok", "items": [], "total": 0}, id="three-key-dict"),
    pytest.param({"status": "ok", "items": [], "total": 0, "meta": {}, "extra": 1}, id="five-key-dict"),
    pytest.param({"status": "degraded", "items": [], "total": 0, "meta": {}}, id="status-outside-vocabulary"),
    pytest.param("ok", id="bare-string"),
])
def test_bundle_rejects_anything_that_is_not_a_four_key_envelope(field_name, bad_value):
    """A list cannot say whether it is empty because the manuscript is empty or
    because the service failed -- which is the whole reason this type exists."""
    with pytest.raises(ValueError) as exc:
        bundle(**{field_name: bad_value})
    assert field_name in str(exc.value)


@pytest.mark.parametrize("field_name", pm.ENVELOPE_FIELDS)
def test_a_four_key_envelope_is_accepted_on_every_one_of_the_five_fields(field_name):
    made = bundle(**{field_name: envelope("ok", (), 0, {})})
    assert getattr(made, field_name)["status"] == "ok"


@pytest.mark.parametrize("field_name", pm.EAGER_ENVELOPE_FIELDS)
def test_none_is_rejected_on_every_eager_envelope_field(field_name):
    """A bundle that accepted `None` everywhere would let a real outage be
    dropped on the floor as "nobody asked for this"."""
    with pytest.raises(ValueError) as exc:
        bundle(**{field_name: None})
    assert field_name in str(exc.value)


def test_none_is_accepted_only_on_the_lazy_related_rows_field():
    made = bundle(**{pm.LAZY_ENVELOPE_FIELD: None})
    assert getattr(made, pm.LAZY_ENVELOPE_FIELD) is None
    assert pm.LAZY_ENVELOPE_FIELD not in pm.EAGER_ENVELOPE_FIELDS
    assert set(pm.ENVELOPE_FIELDS) == set(pm.EAGER_ENVELOPE_FIELDS) | {pm.LAZY_ENVELOPE_FIELD}


def test_related_rows_default_is_not_requested():
    made = bundle()
    assert getattr(made, pm.LAZY_ENVELOPE_FIELD) is None


def test_bundle_fixture_key_sets_equal_the_live_envelope_shape():
    made = bundle(related_rows=related_rows_envelope([related_page_row()]))
    for field_name in pm.ENVELOPE_FIELDS:
        env = getattr(made, field_name)
        assert set(env) == set(pm.ENVELOPE_KEYS) == {"status", "items", "total", "meta"}
        assert set(env["meta"]) == set(pm.LIVE_OK_META_KEYS[field_name]), field_name


def test_make_envelope_agrees_with_the_literal_fixture_shape():
    """The fixtures are literals on purpose; this is what pins them to the
    producer, so a drifted service shape fails HERE and not in a renderer."""
    produced = make_envelope("ok", [], 0, {"page_id": "page-1", "include_review": False})
    assert set(produced) == set(pm.ENVELOPE_KEYS)


# --- the live meta-key sets, read out of the producing code itself ---------

_LIVE_META_SOURCES = {
    "claims": ("shared/discovery_service.py", "get_claims_for_page_enveloped"),
    "page_ids": ("web/discovery.py", "get_manuscript_page_ids"),
    "manuscript_works": ("shared/discovery_service.py", "get_manuscript_works_enveloped"),
    "related_count": ("shared/discovery_service.py", "get_related_page_count_enveloped"),
    "related_rows": ("shared/discovery_service.py", "get_related_pages_enveloped"),
}


def _ok_meta_keys_from_source(rel_path, func_name):
    """Every `meta=` key on an `ok`-status `make_envelope(...)` call inside
    `func_name`. Parsed, never imported -- reading `web/discovery.py` must not
    drag NiceGUI into this suite."""
    tree = ast.parse(io.open(REPO_ROOT / rel_path, encoding="utf-8").read())
    target = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name:
            target = node
            break
    assert target is not None, "%s::%s not found" % (rel_path, func_name)
    keys = set()
    for node in ast.walk(target):
        if not isinstance(node, ast.Call):
            continue
        if getattr(node.func, "id", None) != "make_envelope":
            continue
        first = node.args[0] if node.args else None
        if not (isinstance(first, ast.Name) and first.id == "STATUS_OK"):
            continue
        for kw in node.keywords:
            if kw.arg == "meta" and isinstance(kw.value, ast.Dict):
                keys.update(k.value for k in kw.value.keys if isinstance(k, ast.Constant))
    return frozenset(keys)


@pytest.mark.parametrize("field_name", pm.ENVELOPE_FIELDS)
def test_declared_meta_keys_equal_the_ones_the_live_code_emits(field_name):
    rel_path, func_name = _LIVE_META_SOURCES[field_name]
    assert _ok_meta_keys_from_source(rel_path, func_name) == set(pm.LIVE_OK_META_KEYS[field_name])


# ===========================================================================
# Task 1 -- the not-requested state is its own state
# ===========================================================================


def test_not_requested_is_distinct_from_ok_zero_and_from_an_outage():
    """An implementation that fabricated an `ok` zero for the unfetched section
    would make two of these three identical."""
    rows = [claim_row()]
    not_requested = pm.build_panel_rows(bundle(rows)).related_pages
    genuinely_empty = pm.build_panel_rows(
        bundle(rows, related_rows=related_rows_envelope([], 0))).related_pages
    outage = pm.build_panel_rows(
        bundle(rows, related_rows=related_rows_envelope(status="timeout"))).related_pages

    assert not_requested["rows_state"] == pm.ROWS_NOT_REQUESTED
    assert genuinely_empty["rows_state"] == pm.ROWS_EMPTY
    assert outage["rows_state"] == pm.ROWS_OUTAGE

    states = [not_requested["rows_state"], genuinely_empty["rows_state"], outage["rows_state"]]
    assert len(set(states)) == 3
    for a, b in ((not_requested, genuinely_empty), (not_requested, outage), (genuinely_empty, outage)):
        assert a != b


# ===========================================================================
# Task 1 -- the ten behaviors
# ===========================================================================


def test_two_titles_duplicate_collapses_to_one_row_with_the_canonical_title(two_titles_duplicate):
    model = pm.build_panel_rows(bundle(two_titles_duplicate))
    rows = list(pm.iter_rows(model))
    assert len(rows) == 1
    assert rows[0]["work_id"] == "w001382"
    assert rows[0]["work_title"] == ds.display_work_title("w001382", "The Canonical Title", "en")
    assert len(model.generic_groups) == 0


@pytest.mark.parametrize("lang", LANGS)
def test_curated_w000176_title_is_the_curated_string_and_never_the_raw_one(
        curated_w000176_liturgy, lang):
    model = pm.build_panel_rows(bundle(curated_w000176_liturgy, lang=lang))
    rows = list(pm.iter_rows(model))
    assert len(rows) == 1
    expected = ds.display_work_title("w000176", W000176_RAW_TITLE, lang)
    assert rows[0]["work_title"] == expected
    assert expected != W000176_RAW_TITLE

    for path, value in _walk_strings(model.as_dict()):
        assert W000176_RAW_TITLE != value, path
        # The Hebrew curated label legitimately CONTAINS the recorded title as
        # its first half. Strip every occurrence of the CURATED string and the
        # raw title must be gone -- so the only route it ever took to a display
        # field was through `display_work_title`.
        residue = value.replace(expected, "")
        assert W000176_RAW_TITLE not in residue, path


def test_an_uncurated_work_title_passes_through_unchanged():
    model = pm.build_panel_rows(bundle([claim_row(neutral_title="An Uncurated Work")]))
    rows = list(pm.iter_rows(model))
    assert rows[0]["work_title"] == "An Uncurated Work"


def test_verse_chain_group_leaves_the_identifications_bucket(verse_chain_generic_group):
    model = pm.build_panel_rows(bundle(verse_chain_generic_group))
    assert list(pm.iter_rows(model)) == []
    assert len(model.generic_groups) == 1
    group = model.generic_groups[0]
    assert group["span_start"] == 0 and group["span_end"] == 555
    assert group["matched_letters"] == 420
    # Two DISTINCT works survive the canonical collapse, not three claims.
    assert group["work_count"] == 2
    assert len(group["works"]) == 2
    assert group["note"] == ds.not_an_identification_note("en")


def test_two_granularity_rashi_pair_stays_an_identification(two_granularity_rashi):
    model = pm.build_panel_rows(bundle(two_granularity_rashi))
    rows = list(pm.iter_rows(model))
    assert len(model.generic_groups) == 0
    assert len(rows) == 2
    lead = [r for r in rows if r["span_start"] == 0][0]
    assert lead["granularity_subline"]
    assert len(lead["nested"]) == 1
    other_title = lead["nested"][0]["work_title"]
    assert lead["granularity_subline"] == ds.granularity_subline(other_title, "en")


def test_lead_attribution_nests_competing_attributions_deterministically():
    """One passage produces one row; the others nest, in the shared total
    order -- `lead_attribution`, never a fresh tie-break."""
    author = "שלמה בן יצחק (רש\"י)"
    members = [
        claim_row(claim_id="c-%s" % suffix, evidence_id="ev-%s" % suffix,
                  work_id=wid, canonical_work_id=wid, display_work_id=wid,
                  neutral_title=title, author=author, span_start=0, span_end=962)
        for suffix, wid, title in (
            ("c", "w001283", "רש\"י על ויקרא"),
            ("a", "w000171", "רש\"י על התורה"),
            ("b", "w001281", "רש\"י על בראשית"),
        )
    ]
    model = pm.build_panel_rows(bundle(members))
    rows = list(pm.iter_rows(model))
    assert len(rows) == 1
    expected_lead, expected_rest = grouping.lead_attribution(members)
    assert rows[0]["claim_id"] == expected_lead["claim_id"]
    assert [n["work_id"] for n in rows[0]["nested"]] == [
        r["display_work_id"] for r in expected_rest]


def test_sixty_six_letter_rows_are_gated_not_deleted(sixty_six_letter_liturgical):
    assert 66 < SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS
    model = pm.build_panel_rows(bundle(sixty_six_letter_liturgical))
    default_rows = [r for r in pm.iter_rows(model)
                    if r["disclosure_level"] == pm.LEVEL_IDENTIFICATIONS]
    gated_rows = [r for r in pm.iter_rows(model)
                  if r["disclosure_level"] == pm.LEVEL_MORE_MATCHES]
    assert default_rows == []
    assert len(gated_rows) == 4
    assert all(r["gated"] for r in gated_rows)

    opened = pm.build_panel_rows(bundle(sixty_six_letter_liturgical, show_more=True))
    more = [lvl for lvl in opened.disclosure_levels if lvl["key"] == pm.LEVEL_MORE_MATCHES][0]
    assert more["visible"] is True
    assert len(more["rows"]) == 4


def test_short_evidence_row_kept_by_multi_folio_agreement_stays_in_the_default_level():
    """D-13c's carve-out: the floor never applies to an identification already
    main via multi-folio agreement."""
    model = pm.build_panel_rows(bundle([
        claim_row(matched_letters=66, main_pool=True,
                  main_pool_reason=REASON_MAIN_MULTIFOLIO,
                  identification_page_count=3),
    ]))
    rows = list(pm.iter_rows(model))
    assert rows[0]["disclosure_level"] == pm.LEVEL_IDENTIFICATIONS


def test_two_human_confirmed_rows_both_show_by_default_with_a_coverage_note(
        two_human_confirmed_rows):
    model = pm.build_panel_rows(bundle(two_human_confirmed_rows))
    rows = list(pm.iter_rows(model))
    assert len(rows) == 2
    assert all(r["disclosure_level"] == pm.LEVEL_IDENTIFICATIONS for r in rows)

    demoted = [r for r in rows if r["claim_id"] == "claim-eicha"][0]
    assert demoted["low_coverage_note"] == ds.low_coverage_note("en")
    assert demoted["bucket"] is not None
    assert demoted["main_pool_reason"] is not None

    for row in rows:
        assert not any("badge" in key for key in row)


def test_direct_family_row_carries_coverage_and_a_propagated_row_carries_none():
    direct = pm.build_panel_rows(bundle([claim_row()]))
    propagated = pm.build_panel_rows(bundle([
        claim_row(evidence_source=ids.EVIDENCE_SOURCE_PROPAGATED,
                  confidence_band=ids.CONFIDENCE_BAND_CORROBORATED,
                  band_label=band_label(ids.EVIDENCE_SOURCE_PROPAGATED,
                                        ids.CONFIDENCE_BAND_CORROBORATED),
                  band_rank=2, coverage_ppm=None, matched_letters=None),
    ]))
    direct_row = list(pm.iter_rows(direct))[0]
    propagated_row = list(pm.iter_rows(propagated))[0]

    assert direct_row["coverage_ppm"] == 680000
    assert direct_row["coverage_label"] == ds.coverage_label("en")
    assert "68%" in direct_row["headline"]

    assert "coverage_ppm" not in propagated_row
    assert "coverage_label" not in propagated_row
    assert "%" not in propagated_row["headline"]


def test_every_row_carries_the_relation_chip_and_the_band_label_as_a_tooltip_only():
    model = pm.build_panel_rows(bundle([claim_row()]))
    row = list(pm.iter_rows(model))[0]
    assert row["relation_chip"] == ds.relation_chip(ids.CLAIM_TYPE_DIRECT_WITNESS, "en")
    assert row["band_tooltip"] == ds.relation_tooltip(
        ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC, "en")
    # The band string is a TOOLTIP value and nothing else: no other emitted
    # field may repeat it.
    repeats = [path for path, value in _walk_strings(model.as_dict())
               if value == row["band_tooltip"] and not path.endswith("band_tooltip")]
    assert repeats == []


# ===========================================================================
# Task 1 -- order of operations
# ===========================================================================


def test_collapse_runs_before_the_generic_pull_out(two_titles_duplicate, two_granularity_rashi):
    """Pulling out generic groups BEFORE collapsing duplicates is exactly the
    mistake that cost one manuscript a correct identification: the duplicate
    pair sits on one span, so a generic-first pipeline sees an identical-span
    group its own predicate cannot decide and files the identification away as
    generic shared text."""
    duplicate_model = pm.build_panel_rows(bundle(two_titles_duplicate))
    assert len(list(pm.iter_rows(duplicate_model))) == 1
    assert duplicate_model.generic_groups == ()

    rashi_model = pm.build_panel_rows(bundle(two_granularity_rashi))
    assert rashi_model.generic_groups == ()

    # `rindex`, not `index`: the first occurrence of each marker is its own
    # constant definition (all four sit together), so only the LAST occurrence
    # -- the use site inside the pipeline -- says anything about ordering.
    source = _model_source()
    order = [source.rindex(marker) for marker in (
        pm.STEP_COLLAPSE_DUPLICATES,
        pm.STEP_SEPARATE_GENERIC_GROUPS,
        pm.STEP_LEAD_ATTRIBUTION,
        pm.STEP_GATE_SHORT_EVIDENCE,
    )]
    assert order == sorted(order), "the named steps are out of order in the source"


# ===========================================================================
# Task 1 -- module-level greps
# ===========================================================================


def test_module_imports_nothing_from_nicegui_and_executes_no_query():
    source = _model_source()
    assert "nicegui" not in source.lower()
    for banned in ("sqlite3", "execute(", "cursor", "import web", "from web"):
        assert banned not in source, banned


def test_module_defines_no_human_review_badge_field():
    source = _model_source()
    assert "review_overlay" not in source
    assert "Expert-reviewed" not in source
    assert "נבדק בידי מומחה" not in source


def test_raw_recorded_title_is_read_at_exactly_one_call_site():
    """A model that formats the recorded title directly silently opts out of
    ruling R's curation."""
    source = _model_source()
    reads = re.findall(r'\[["\']neutral_title["\']\]|\.get\(["\']neutral_title["\']', source)
    assert len(reads) == 1, "expected exactly one raw title read, found %d" % len(reads)
    line = [ln for ln in source.splitlines()
            if re.search(r'\[["\']neutral_title["\']\]|\.get\(["\']neutral_title["\']', ln)][0]
    start = source.splitlines().index(line)
    window = "\n".join(source.splitlines()[max(0, start - 6):start + 6])
    assert "display_work_title" in window


# ---------------------------------------------------------------------------
# Shared walker used by several tests above.
# ---------------------------------------------------------------------------


def _walk_strings(node, path="model"):
    """Yield `(path, value)` for every STRING leaf reachable in `node`."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _walk_strings(value, "%s.%s" % (path, key))
    elif isinstance(node, (list, tuple)):
        for index, value in enumerate(node):
            yield from _walk_strings(value, "%s[%d]" % (path, index))
    elif isinstance(node, str):
        yield path, node
