# -*- coding: utf-8 -*-
"""Fetcher politeness and version pinning: the inter-request interval,
429/Retry-After handling, and the explicit Sefaria ``version_title`` pin --
all added for the REF6-scale (~4,000-request) acquisition run.
All fixtures are synthetic; no network I/O."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from scripts.discovery_v4_common import load_source_config
from scripts.discovery_v4_fetch_sources import Fetcher, _select_hebrew_version


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"ok": True}
        self.headers = headers or {}
        self.content = json.dumps(self._payload).encode("utf-8")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses: list):
        self._responses = list(responses)
        self.calls = 0
        self.headers = {}

    def get(self, url, **kwargs):
        self.calls += 1
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _fetcher(tmp_path: Path, responses: list, **kwargs) -> tuple[Fetcher, FakeSession]:
    fetcher = Fetcher(tmp_path / "out", request_interval=0.0, **kwargs)
    session = FakeSession(responses)
    fetcher.session = session
    return fetcher, session


def test_429_then_success_recovers_and_sleeps_per_retry_after(
    tmp_path: Path, monkeypatch
):
    sleeps: list[float] = []
    monkeypatch.setattr(
        "scripts.discovery_v4_fetch_sources.time.sleep", sleeps.append
    )
    fetcher, session = _fetcher(
        tmp_path,
        [
            FakeResponse(429, headers={"Retry-After": "7"}),
            FakeResponse(200, {"parse": {"title": "x"}}),
        ],
    )
    doc = fetcher.get_json("https://example.invalid/api", params=None,
                           raw_path=tmp_path / "out/raw/x.json")
    assert doc == {"parse": {"title": "x"}}
    assert session.calls == 2
    assert 7.0 in sleeps  # the numeric Retry-After header was honored


def test_sustained_429_fails_after_retries_naming_the_status(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(
        "scripts.discovery_v4_fetch_sources.time.sleep", lambda _s: None
    )
    fetcher, session = _fetcher(tmp_path, [FakeResponse(429) for _ in range(6)])
    with pytest.raises(RuntimeError, match="429"):
        fetcher.get_json("https://example.invalid/api", params=None,
                         raw_path=tmp_path / "out/raw/x.json")
    assert session.calls == 6


def test_retry_after_backoff_is_exponential_and_capped_without_header():
    resp = FakeResponse(429)
    assert Fetcher._retry_after_seconds(resp, 0) == 10.0
    assert Fetcher._retry_after_seconds(resp, 1) == 20.0
    assert Fetcher._retry_after_seconds(resp, 2) == 40.0
    assert Fetcher._retry_after_seconds(resp, 5) == 120.0  # capped
    with_header = FakeResponse(429, headers={"Retry-After": "300"})
    assert Fetcher._retry_after_seconds(with_header, 0) == 120.0  # header capped too


def test_request_interval_spaces_successive_requests(tmp_path: Path, monkeypatch):
    sleeps: list[float] = []
    clock = {"now": 100.0}
    monkeypatch.setattr(
        "scripts.discovery_v4_fetch_sources.time.monotonic", lambda: clock["now"]
    )
    monkeypatch.setattr(
        "scripts.discovery_v4_fetch_sources.time.sleep", sleeps.append
    )
    fetcher = Fetcher(tmp_path / "out", request_interval=2.0)
    fetcher.session = FakeSession([FakeResponse(200), FakeResponse(200)])
    fetcher.get_json("https://example.invalid/a", params=None,
                     raw_path=tmp_path / "out/raw/a.json")
    # No time passes on the fake clock, so the second call must wait the
    # full interval.
    fetcher.get_json("https://example.invalid/b", params=None,
                     raw_path=tmp_path / "out/raw/b.json")
    assert 2.0 in sleeps


def test_generic_error_retry_behavior_is_preserved(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "scripts.discovery_v4_fetch_sources.time.sleep", lambda _s: None
    )
    fetcher, session = _fetcher(
        tmp_path,
        [requests.ConnectionError("boom"), FakeResponse(200, {"fine": 1})],
    )
    doc = fetcher.get_json("https://example.invalid/api", params=None,
                           raw_path=tmp_path / "out/raw/x.json")
    assert doc == {"fine": 1}
    assert session.calls == 2


# ---------------------------------------------------------------------------
# The explicit Sefaria version pin (version_title) -- added after the Kuzari
# quarantine: a Judeo-Arabic original listed under language "he" with a
# Public Domain license outranked the ibn Tibbon Hebrew translation.
# ---------------------------------------------------------------------------

ALLOWLIST = {"public domain", "cc0", "cc-by", "cc-by-sa"}

VERSIONS = [
    # The trap: PD, isPrimary, language "he" -- but not the wanted text.
    {"language": "he", "license": "Public Domain", "isPrimary": True,
     "versionTitle": "Kitab demo [jrb]"},
    {"language": "he", "license": "CC-BY-SA", "isPrimary": True,
     "versionTitle": "Demo ibn Tibbon translation"},
    {"language": "he", "license": "unknown", "isPrimary": False,
     "versionTitle": "Demo modern translation"},
]


def test_version_pin_selects_the_named_version_over_the_license_ranking():
    source = {"key": "k", "version_title": "Demo ibn Tibbon translation"}
    chosen = _select_hebrew_version(source, VERSIONS, ALLOWLIST)
    assert chosen["versionTitle"] == "Demo ibn Tibbon translation"


def test_without_a_pin_the_license_ranking_still_picks():
    chosen = _select_hebrew_version({"key": "k"}, VERSIONS, ALLOWLIST)
    assert chosen["versionTitle"] == "Kitab demo [jrb]"  # PD outranks CC-BY-SA


def test_pinned_version_absent_from_the_list_is_a_hard_error():
    source = {"key": "k", "version_title": "No Such Version"}
    with pytest.raises(ValueError, match="not among the provider's Hebrew versions"):
        _select_hebrew_version(source, VERSIONS, ALLOWLIST)


def test_pinned_version_with_a_non_allowlisted_license_is_a_hard_error():
    source = {"key": "k", "version_title": "Demo modern translation"}
    with pytest.raises(ValueError, match="non-allowlisted"):
        _select_hebrew_version(source, VERSIONS, ALLOWLIST)


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


def test_version_title_validation_rejects_empty_and_wrong_provider(tmp_path: Path):
    base = {
        "key": "k1",
        "provider": "sefaria",
        "source_ref": "Demo",
        "version_title": "   ",
        "mappings": [{"target_work_id": "w000001"}],
    }
    with pytest.raises(ValueError, match="invalid version_title"):
        load_source_config(_write_map(tmp_path, base))
    wrong_provider = {**base, "provider": "hewikisource", "version_title": "X"}
    with pytest.raises(ValueError, match="non-container sefaria source"):
        load_source_config(_write_map(tmp_path, wrong_provider))


def test_repo_map_pins_the_kuzari_ben_yehuda_version():
    config = load_source_config(
        Path(__file__).resolve().parents[1] / "scripts/discovery_v4_2_sources.json"
    )
    kuzari = next(s for s in config["sources"] if s["key"] == "kuzari_ibn_tibbon")
    assert kuzari["version_title"] == "Sefer haKuzari - Project Ben-Yehuda"
