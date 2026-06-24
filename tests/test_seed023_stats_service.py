"""SEED-023 Part A — cached homepage corpus stats (web/stats_service.py)."""

from __future__ import annotations

import os

import pytest

import web.stats_service as ss

_EXPECTED = {
    "manuscripts": 255723,
    "catalog_entries": 731354,
    "images": 1019886,
    "scholarly_editions": 27424,
    "automatic_transcriptions": 232450,
}


def _patch_all(monkeypatch, **vals):
    monkeypatch.setattr(ss, "_count_manuscripts", lambda: vals["manuscripts"])
    monkeypatch.setattr(ss, "_count_catalog_entries", lambda: vals["catalog_entries"])
    monkeypatch.setattr(ss, "_count_image_records", lambda: vals["images"])
    monkeypatch.setattr(ss, "_count_scholarly_editions", lambda: vals["scholarly_editions"])
    monkeypatch.setattr(ss, "_count_automatic_transcriptions", lambda: vals["automatic_transcriptions"])


def test_aggregates_and_memoizes_when_corpus_ready(monkeypatch):
    ss.reset_cache()
    _patch_all(monkeypatch, **_EXPECTED)
    s = ss.get_corpus_stats()
    for k, v in _EXPECTED.items():
        assert s[k] == v
    assert "computed_at" in s
    assert ss._CACHE is not None  # memoized (manuscripts > 0)
    ss.reset_cache()


def test_second_call_hits_cache_no_recompute(monkeypatch):
    ss.reset_cache()
    calls = {"n": 0}

    def _counter():
        calls["n"] += 1
        return 5

    _patch_all(monkeypatch, **_EXPECTED)
    monkeypatch.setattr(ss, "_count_manuscripts", _counter)
    ss.get_corpus_stats()
    ss.get_corpus_stats()
    ss.get_corpus_stats()
    assert calls["n"] == 1  # computed exactly once
    ss.reset_cache()


def test_not_memoized_until_corpus_ready(monkeypatch):
    """manuscripts==0 (corpus not loaded yet) must NOT cache, so a later call recomputes."""
    ss.reset_cache()
    vals = dict(_EXPECTED, manuscripts=0, automatic_transcriptions=0)
    _patch_all(monkeypatch, **vals)
    s = ss.get_corpus_stats()
    assert s["catalog_entries"] == 731354  # state-independent counts still returned
    assert ss._CACHE is None               # but not memoized
    ss.reset_cache()


def test_force_refresh_recomputes(monkeypatch):
    ss.reset_cache()
    _patch_all(monkeypatch, **_EXPECTED)
    first = ss.get_corpus_stats()
    monkeypatch.setattr(ss, "_count_catalog_entries", lambda: 999)
    again = ss.get_corpus_stats(force_refresh=True)
    assert first["catalog_entries"] == 731354
    assert again["catalog_entries"] == 999
    ss.reset_cache()


def test_compute_failure_returns_uncached_zero_dict(monkeypatch):
    ss.reset_cache()
    def boom():
        raise RuntimeError("boom")
    monkeypatch.setattr(ss, "_compute", boom)
    s = ss.get_corpus_stats()
    for k in ss._KEYS:
        assert s[k] == 0
    assert "computed_at" in s
    assert ss._CACHE is None  # total failure must NOT poison the cache (Codex #306)
    ss.reset_cache()


def test_partial_degraded_metric_not_cached(monkeypatch):
    """manuscripts>0 but a sidecar metric transiently 0 -> incomplete -> not cached."""
    ss.reset_cache()
    vals = dict(_EXPECTED, catalog_entries=0)  # e.g. fjms transiently unavailable
    _patch_all(monkeypatch, **vals)
    s = ss.get_corpus_stats()
    assert s["manuscripts"] == _EXPECTED["manuscripts"]
    assert s["catalog_entries"] == 0
    assert ss._CACHE is None  # incomplete -> recompute next call
    ss.reset_cache()


# --- real-DB sanity for the state-independent metrics (skip if sidecars absent) ---

@pytest.mark.skipif(
    not (os.path.exists("fist_data/fjms_enrichment.db") and os.path.exists("nli_data/nli_crossref.db")),
    reason="sidecar DBs absent",
)
def test_real_db_counts_are_positive():
    assert ss._count_catalog_entries() > 100_000
    assert ss._count_image_records() > 500_000
    assert ss._count_scholarly_editions() > 1_000
