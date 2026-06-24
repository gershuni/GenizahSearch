"""SEED-023 Part A — homepage corpus stats (web/stats_service.py).

The five headline numbers are hardcoded constants (decision 2026-06-24); computing
them live caused a partial-load race. These tests pin the constants and verify the
live-regeneration path still matches for the DB-deterministic metrics.
"""

from __future__ import annotations

import os

import pytest

import web.stats_service as ss

_EXPECTED = {
    "manuscripts": 255_723,
    "catalog_entries": 731_354,
    "images": 1_019_886,
    "scholarly_transcriptions": 27_424,
    "automatic_transcriptions": 232_450,
}


def test_corpus_stats_are_the_expected_constants():
    s = ss.get_corpus_stats()
    assert s == _EXPECTED


def test_all_keys_present_and_positive():
    s = ss.get_corpus_stats()
    assert set(s.keys()) == set(_EXPECTED)
    assert all(v > 0 for v in s.values())


def test_renamed_key_scholarly_transcriptions():
    s = ss.get_corpus_stats()
    assert "scholarly_transcriptions" in s
    assert "scholarly_editions" not in s  # renamed per UAT


def test_returns_a_copy_not_the_shared_dict():
    s = ss.get_corpus_stats()
    s["manuscripts"] = 0
    assert ss.get_corpus_stats()["manuscripts"] == _EXPECTED["manuscripts"]


# --- live regeneration sanity: DB-deterministic metrics must match the constants ---

@pytest.mark.skipif(
    not (
        os.path.exists("fist_data/fjms_enrichment.db")
        and os.path.exists("nli_data/nli_crossref.db")
        and os.path.exists("pgp_data/pgp.db")
    ),
    reason="sidecar DBs absent",
)
def test_live_db_metrics_match_constants():
    # These three are fully determined by the sidecars (no runtime state needed), so
    # a drift here means the hardcoded constants are stale and should be regenerated.
    assert ss._count_catalog_entries() == _EXPECTED["catalog_entries"]
    assert ss._count_image_records() == _EXPECTED["images"]
    assert ss._count_scholarly_transcriptions() == _EXPECTED["scholarly_transcriptions"]
