# -*- coding: utf-8 -*-
"""Drift-guard + data-driven + word-gate + SC#1-inseparability + CI-fail-closed
D-18 + display-deduplicated-population test suite for
`shared/discovery_band_labels.py` + the new `band_precision` /
`get_band_claim_counts` readers on `shared/discovery_service.py`
(Phase 135, plan 135-01, Task 3).

Masking discipline (matches `tests/test_discovery_ids.py`): every
page_id/sys_id/work_id/evidence_id value below is a synthetic fixture
placeholder from `tests/fixtures/discovery/discovery-v1-fixture.db`, never a
real research-data identifier.
"""

import re
import shutil
import sqlite3
from pathlib import Path

import pytest

import scripts.discovery_ids as ids
from shared.discovery_band_labels import (
    BAND_LABELS,
    DENOMINATOR_LABEL,
    DRAW_SIZE_LABEL,
    NUMERATOR_LABEL,
    RECALL_DISCLAIMER,
    SHOW_MORE_TOGGLE,
    STRICT_FLOOR,
    _canon_band_key,
    band_label,
    band_measurement_status,
    format_precision_copy,
    is_default_eligible,
    review_overlay,
    serialize_banded_claim,
)
from shared.discovery_service import DiscoveryService

FIXTURE_DB = (
    Path(__file__).resolve().parent / "fixtures" / "discovery" / "discovery-v1-fixture.db"
)

_FORBIDDEN_WORDS = ("verified", "confirmed", "reviewed", "certified")

# Whole-WORD matcher (never a bare substring match): "unreviewed" legitimately
# contains "reviewed" as a substring but is the HONEST negation of it (the
# explicit "absence of review" marker, docs/specs/discovery-band-labels-v1.md
# §2) -- a naive `in` check would false-positive on it. `\b` requires a
# transition between a word char and a non-word char, so `\breviewed\b` does
# NOT match inside "unreviewed" (no boundary between "un" and "reviewed").
_FORBIDDEN_WORD_RES = {word: re.compile(rf"\b{re.escape(word)}\b") for word in _FORBIDDEN_WORDS}


def _leaked_forbidden_words(text: str):
    lowered = text.lower()
    return [word for word, pattern in _FORBIDDEN_WORD_RES.items() if pattern.search(lowered)]


_ALL_FROZEN_BAND_KEYS = [
    (source, band)
    for source, bands in ids.CONFIDENCE_BANDS_BY_SOURCE.items()
    for band in bands
]

_BARE_PERCENT_RE = re.compile(r"\d+(\.\d+)?%")


def _copy_fixture(tmp_path, name="corrupt.db"):
    dest = tmp_path / name
    shutil.copyfile(FIXTURE_DB, dest)
    return dest


def _connect_rw(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


def _service_for(db_path) -> DiscoveryService:
    path = str(db_path)
    return DiscoveryService(path_provider=lambda: path, availability_callable=lambda: True)


# ---------------------------------------------------------------------------
# BAND-01: totality / no-orphan drift guard over the frozen enum.
# ---------------------------------------------------------------------------

def test_every_frozen_band_has_a_label():
    for source, bands in ids.CONFIDENCE_BANDS_BY_SOURCE.items():
        for band in bands:
            key = (source, _canon_band_key(band))
            assert key in BAND_LABELS, f"missing label for {key}"


def test_no_orphan_labels():
    frozen_keys = {
        (source, _canon_band_key(band))
        for source, bands in ids.CONFIDENCE_BANDS_BY_SOURCE.items()
        for band in bands
    }
    assert set(BAND_LABELS.keys()) <= frozen_keys


def test_v1_and_v2_band_key_resolve_to_the_same_label():
    assert band_label("track1_direct", "expert_verified", "en") == band_label(
        "track1_direct", "high_confidence_algorithmic", "en"
    )
    assert band_label("track1_direct", "expert_verified", "he") == band_label(
        "track1_direct", "high_confidence_algorithmic", "he"
    )


# ---------------------------------------------------------------------------
# Rule 1 word gate -- static dict + rendered function output.
# ---------------------------------------------------------------------------

def test_word_gate():
    for (source, band), langs in BAND_LABELS.items():
        for lang, text in langs.items():
            leaked = _leaked_forbidden_words(text)
            assert not leaked, (
                f"forbidden word(s) {leaked} found in BAND_LABELS[{(source, band)}][{lang}] = {text!r}"
            )


def test_word_gate_rendered_output():
    null_row = {"precision": None, "ci_low": None, "ci_high": None}
    measured_row = {"precision": 0.9, "ci_low": 0.85, "ci_high": 0.95}

    for source, band in _ALL_FROZEN_BAND_KEYS:
        for lang in ("en", "he"):
            label = band_label(source, band, lang)
            leaked = _leaked_forbidden_words(label)
            assert not leaked, f"{leaked} leaked into band_label({source}, {band}, {lang})"
            for row in (null_row, measured_row):
                copy = format_precision_copy(row, lang)
                leaked = _leaked_forbidden_words(copy)
                assert not leaked, f"{leaked} leaked into format_precision_copy({row}, {lang})"

    # Every NON-human_confirmed adjudication status must never render any
    # forbidden word as a WHOLE word ("unreviewed" legitimately contains
    # "reviewed" as a substring -- it is the honest negation of it, not a
    # violation of Rule 1).
    for status in (
        ids.ADJUDICATION_STATUS_PROVISIONAL,
        ids.ADJUDICATION_STATUS_UNREVIEWED,
        None,
        "some_unknown_status",
    ):
        for lang in ("en", "he"):
            overlay = review_overlay(status, lang)
            leaked = _leaked_forbidden_words(overlay)
            assert not leaked, f"{leaked} leaked into review_overlay({status!r}, {lang})"

    # The SOLE exception: human_confirmed renders "reviewed" (EN) but NEVER
    # "certified" anywhere.
    for lang in ("en", "he"):
        overlay = review_overlay(ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED, lang)
        assert "certified" not in _leaked_forbidden_words(overlay)
    assert "reviewed" in _leaked_forbidden_words(review_overlay(ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED, "en"))


# ---------------------------------------------------------------------------
# BAND-02/CERT-02: precision copy -- CI omission / fail-closed-on-partial /
# no-bare-number / data-driven.
# ---------------------------------------------------------------------------

def test_precision_copy_ci_omitted_when_absent():
    row = {"precision": 0.9, "ci_low": None, "ci_high": None}
    copy = format_precision_copy(row, "en")
    assert "[" not in copy
    assert "estimated band precision" in copy
    copy_he = format_precision_copy(row, "he")
    assert "[" not in copy_he


def test_precision_copy_fails_closed_on_partial_interval():
    with pytest.raises(ValueError):
        format_precision_copy({"precision": 0.9, "ci_low": 0.85, "ci_high": None}, "en")
    with pytest.raises(ValueError):
        format_precision_copy({"precision": 0.9, "ci_low": None, "ci_high": 0.95}, "en")


def test_precision_copy_is_data_driven(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE band_precision SET precision = NULL, ci_low = NULL, ci_high = NULL "
        "WHERE scope = 'band' AND evidence_source = 'track1_direct' AND confidence_band = 'tier_a'"
    )
    conn.commit()
    conn.close()

    before = format_precision_copy(_service_for(db_path).get_band_precision("track1_direct", "tier_a"), "en")
    assert "not yet measured" in before

    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE band_precision SET precision = 0.912, ci_low = 0.88, ci_high = 0.94 "
        "WHERE scope = 'band' AND evidence_source = 'track1_direct' AND confidence_band = 'tier_a'"
    )
    conn.commit()
    conn.close()

    after = format_precision_copy(_service_for(db_path).get_band_precision("track1_direct", "tier_a"), "en")
    assert after != before
    assert "91.2%" in after


def test_tier_a_shows_no_number_before_cert01():
    copy = format_precision_copy({"precision": None, "ci_low": None, "ci_high": None}, "en")
    assert "not yet measured" in copy
    assert not _BARE_PERCENT_RE.search(copy)


def test_numerator_denominator_labels_distinct_from_population():
    for label_dict in (NUMERATOR_LABEL, DENOMINATOR_LABEL, DRAW_SIZE_LABEL):
        for text in label_dict.values():
            assert "population" not in text.lower()
    assert NUMERATOR_LABEL["en"] != DENOMINATOR_LABEL["en"] != DRAW_SIZE_LABEL["en"]


# ---------------------------------------------------------------------------
# band_measurement_status: not_measured / stored-status-preferred /
# CI-fail-closed downgrade / display-only measured_audit_pending /
# data-driven.
# ---------------------------------------------------------------------------

def test_precision_reader_tolerates_missing_135_05_columns(tmp_path):
    """A LEGACY asset -- one built before the 135-05 registry columns existed --
    must still read cleanly.

    ⟨AMENDED 2026-08-03, plan 136-12⟩ This used to assert the condition against
    the committed golden fixture, which happened to lack `measurement_status`
    only because it had never been regenerated. 136-12 refreshes the golden, so
    the legacy shape is now CONSTRUCTED here rather than borrowed from a stale
    file: a fixture's staleness is not a contract, and a test that depends on
    it silently stops testing anything the moment the fixture is rebuilt."""
    legacy_db = _copy_fixture(tmp_path, name="legacy-no-135-05-columns.db")
    conn = _connect_rw(legacy_db)
    keep = [
        r[1] for r in conn.execute("PRAGMA table_info(band_precision)")
        if r[1] not in ("measurement_status", "measurement_date", "grader",
                        "audit_status", "report_id")
    ]
    cols = ", ".join(f'"{c}"' for c in keep)
    conn.execute(f'CREATE TABLE band_precision__legacy AS SELECT {cols} FROM band_precision')
    conn.execute("DROP TABLE band_precision")
    conn.execute("ALTER TABLE band_precision__legacy RENAME TO band_precision")
    conn.commit()
    conn.close()

    row = _service_for(legacy_db).get_band_precision("track1_direct", "tier_a")
    assert row is not None
    assert "measurement_status" not in row  # the legacy shape, constructed above
    assert band_measurement_status(row) == "measured_audit_pending"


def test_precision_reader_reads_the_current_fixtures_registry_columns():
    """The other half of the pair: the CURRENT golden fixture does carry the
    135-05 registry columns, so the reader surfaces them."""
    row = _service_for(FIXTURE_DB).get_band_precision("track1_direct", "tier_a")
    assert row is not None
    assert "measurement_status" in row


def test_band_measurement_status_not_measured_when_nothing_stored():
    assert band_measurement_status({"precision": None}) == "not_measured"
    assert band_measurement_status({}) == "not_measured"


def test_is_default_eligible_fails_closed_on_ci():
    assert is_default_eligible(
        "track1_direct", "tier_a", "unreviewed", "shipped", "measured_pass", ci_low=None
    ) is False
    assert is_default_eligible(
        "track1_direct", "tier_a", "unreviewed", "shipped", "measured_pass", ci_low=0.80
    ) is False

    # AND band_measurement_status itself downgrades that same contradictory
    # row -- it must NEVER report 'measured_pass' when the CI contradicts it.
    assert band_measurement_status(
        {"precision": 0.9, "measurement_status": "measured_pass", "ci_low": None}
    ) == "measured_fail"
    assert band_measurement_status(
        {"precision": 0.9, "measurement_status": "measured_pass", "ci_low": 0.80}
    ) == "measured_fail"
    # A passing CI at or above the floor is NOT downgraded.
    assert band_measurement_status(
        {"precision": 0.9, "measurement_status": "measured_pass", "ci_low": STRICT_FLOOR}
    ) == "measured_pass"


def test_band_measurement_status_data_driven(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    # 136-12: `measurement_status` has existed in `create_schema` since 135-05;
    # the ALTER TABLE that used to be here only worked because the committed
    # golden fixture predated that column. It is now present by construction.
    conn.execute(
        "UPDATE band_precision SET precision = NULL, ci_low = NULL, ci_high = NULL, "
        "measurement_status = NULL "
        "WHERE scope = 'band' AND evidence_source = 'track1_direct' AND confidence_band = 'tier_a'"
    )
    conn.commit()
    conn.close()
    row = _service_for(db_path).get_band_precision("track1_direct", "tier_a")
    assert band_measurement_status(row) == "not_measured"

    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE band_precision SET precision = 0.9, ci_low = 0.87, ci_high = 0.93, "
        "measurement_status = 'measured_pass' "
        "WHERE scope = 'band' AND evidence_source = 'track1_direct' AND confidence_band = 'tier_a'"
    )
    conn.commit()
    conn.close()
    row = _service_for(db_path).get_band_precision("track1_direct", "tier_a")
    assert band_measurement_status(row) == "measured_pass"

    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE band_precision SET ci_low = 0.5 "
        "WHERE scope = 'band' AND evidence_source = 'track1_direct' AND confidence_band = 'tier_a'"
    )
    conn.commit()
    conn.close()
    row = _service_for(db_path).get_band_precision("track1_direct", "tier_a")
    assert band_measurement_status(row) == "measured_fail"


# ---------------------------------------------------------------------------
# SC#1: serialize_banded_claim band-inseparability.
# ---------------------------------------------------------------------------

def test_serialize_banded_claim_requires_band():
    with pytest.raises(ValueError):
        serialize_banded_claim({"evidence_source": "track1_direct", "adjudication_status": "unreviewed"})
    with pytest.raises(ValueError):
        serialize_banded_claim({"confidence_band": "tier_a", "adjudication_status": "unreviewed"})
    with pytest.raises(ValueError):
        serialize_banded_claim({"confidence_band": "tier_a", "evidence_source": "track1_direct"})

    for source, band in _ALL_FROZEN_BAND_KEYS:
        row = {
            "confidence_band": band,
            "evidence_source": source,
            "adjudication_status": "unreviewed",
            "routing_status": "shipped",
            "precision": 0.9,
            "ci_low": 0.87,
            "ci_high": 0.95,
        }
        result = serialize_banded_claim(row, "en")
        for key in ("band_label", "review_overlay", "measurement_status", "default_eligible"):
            assert key in result, f"serialized claim for {(source, band)} is missing {key!r} -- SC#1 violated"


def test_serialize_banded_claim_defaults_routing_status_conservatively():
    row = {
        "confidence_band": "tier_a",
        "evidence_source": "track1_direct",
        "adjudication_status": "unreviewed",
        "precision": 0.9,
        "ci_low": 0.9,
    }  # no routing_status key at all
    result = serialize_banded_claim(row, "en")
    assert result["routing_status"] == ids.ROUTING_STATUS_REVIEW_ONLY
    assert result["default_eligible"] is False


def test_serialize_banded_claim_human_confirmed_always_eligible():
    row = {
        "confidence_band": "screening_canon",
        "evidence_source": "track1_direct",
        "adjudication_status": "human_confirmed",
        "routing_status": "review_only",
    }
    result = serialize_banded_claim(row, "en")
    assert result["default_eligible"] is True
    assert "Expert-reviewed" in result["review_overlay"]


# ---------------------------------------------------------------------------
# D-18 default-eligibility predicate table.
# ---------------------------------------------------------------------------

def test_is_default_eligible_d18():
    assert is_default_eligible("track1_direct", "tier_a", "unreviewed", "shipped", "not_measured") is False
    assert is_default_eligible(
        "track1_direct", "tier_a", "unreviewed", "shipped", "measured_pass", ci_low=0.87
    ) is True
    assert is_default_eligible("track1_direct", "screening_rb", "unreviewed", "shipped", "not_measured") is False
    assert is_default_eligible("propagated", "not_evaluated", "unreviewed", "shipped", "not_measured") is False
    assert is_default_eligible(
        "track1_direct", "tier_a", "unreviewed", "review_only", "measured_pass", ci_low=0.9
    ) is False
    assert is_default_eligible(
        "track1_direct", "screening_canon", "human_confirmed", "review_only", "not_measured"
    ) is True
    assert is_default_eligible("propagated", "corroborated", "unreviewed", "shipped", "not_measured") is True
    assert is_default_eligible(
        "track1_direct", "high_confidence_algorithmic", "unreviewed", "shipped", "measured_audit_pending"
    ) is True


# ---------------------------------------------------------------------------
# get_band_precision / get_band_precision_collection reads (Task 1).
# ---------------------------------------------------------------------------

def test_get_band_precision_reads_known_row():
    row = _service_for(FIXTURE_DB).get_band_precision("track1_direct", "tier_a")
    assert row is not None
    assert row["precision"] == 0.9


def test_get_band_precision_missing_pair_returns_none():
    assert _service_for(FIXTURE_DB).get_band_precision("track1_direct", "does_not_exist") is None


def test_get_band_precision_unavailable_service_returns_none():
    svc = DiscoveryService(path_provider=lambda: None, availability_callable=lambda: False)
    assert svc.get_band_precision("track1_direct", "tier_a") is None
    assert svc.get_band_precision_collection() is None
    assert svc.get_band_claim_counts() == {}


def test_get_band_precision_collection_reads_the_measured_collection_number():
    row = _service_for(FIXTURE_DB).get_band_precision_collection()
    assert row is not None
    assert row["precision"] == 0.926
    assert row["scope"] == "collection"


# ---------------------------------------------------------------------------
# get_band_claim_counts: display-deduplicated SHIPPED CLAIM population
# (Codex #B1/#9).
# ---------------------------------------------------------------------------

def test_get_band_claim_counts_on_fixture_is_display_deduplicated():
    counts = _service_for(FIXTURE_DB).get_band_claim_counts()
    assert counts
    # 19 stored claims total, 1 excluded (its display evidence is review_only).
    assert sum(counts.values()) == 18


def test_band_claim_counts_display_deduplicated(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)

    naive_before = conn.execute(
        "SELECT COUNT(*) FROM discovery_evidence WHERE routing_status = 'shipped'"
    ).fetchone()[0]

    counts_before = _service_for(db_path).get_band_claim_counts()
    deduped_before_total = sum(counts_before.values())

    # The naive (undeduplicated) evidence-row count is ALREADY larger than
    # the display-deduplicated claim count on the unmodified fixture --
    # proves the dedup is load-bearing (Codex #B1/#9).
    assert naive_before > deduped_before_total

    claim_id, display_evidence_id = conn.execute(
        "SELECT claim_id, display_evidence_id FROM discovery_claim "
        "WHERE work_id = 'w000001' AND page_id = 'p001'"
    ).fetchone()
    evidence_source, confidence_band = conn.execute(
        "SELECT evidence_source, confidence_band FROM discovery_evidence WHERE evidence_id = ?",
        (display_evidence_id,),
    ).fetchone()

    # A SECOND evidence row on the SAME claim (a witness+shared_text-style
    # collision) must NOT inflate its band's count -- only the display
    # pointer's band is ever counted.
    conn.execute(
        """
        INSERT INTO discovery_evidence (
            evidence_id, claim_id, evidence_kind, evidence_source, confidence_band,
            adjudication_status, audit_status, routing_status, routing_reason,
            is_new, a_page_id, sys_id, span_start, span_end
        ) VALUES (?, ?, 'shared_text', 'propagated', 'not_evaluated',
                   'unreviewed', 'n/a', 'shipped', 'none', 0, 'p001', '990000000000000099', 0, 5)
        """,
        (f"test_extra_evidence_row_for_{claim_id}", claim_id),
    )
    conn.commit()

    counts_after_insert = _service_for(db_path).get_band_claim_counts()
    assert counts_after_insert[(evidence_source, confidence_band)] == counts_before[
        (evidence_source, confidence_band)
    ]
    assert sum(counts_after_insert.values()) == deduped_before_total

    # Flipping the claim's DISPLAY evidence row to review_only must drop it
    # from the shipped, display-deduplicated population entirely -- even
    # though its non-display sibling (inserted above) is still shipped.
    conn.execute(
        "UPDATE discovery_evidence SET routing_status = 'review_only' WHERE evidence_id = ?",
        (display_evidence_id,),
    )
    conn.commit()
    conn.close()

    counts_after_flip = _service_for(db_path).get_band_claim_counts()
    assert counts_after_flip.get((evidence_source, confidence_band), 0) == (
        counts_before[(evidence_source, confidence_band)] - 1
    )
    assert sum(counts_after_flip.values()) == deduped_before_total - 1


# ---------------------------------------------------------------------------
# D-11 / D-12 exact-string constants.
# ---------------------------------------------------------------------------

def test_toggle_wording_matches_d11():
    assert SHOW_MORE_TOGGLE["en"] == "Show more possible matches"
    assert SHOW_MORE_TOGGLE["he"] == "הצג התאמות אפשריות נוספות"


def test_disclaimer_matches_d12():
    assert RECALL_DISCLAIMER["en"] == "Not exhaustive — more identifications may exist."
    assert RECALL_DISCLAIMER["he"] == "אינו ממצה — ייתכנו זיהויים נוספים."
