"""Tests for the novelty-reuse measurement's pinning (Codex round 5, HIGH).

Round 4 asked for the inputs to be recorded; round 5 found that recording is not
pinning: `--population` was "freely selected by the caller and is not derived from,
or validated against, the input asset", `_hash` turned an unreadable input into a
report STRING while still exiting zero, and hashing happened only AFTER the
minutes-long candidate build.

The stake is specific: this report is the intended evidence for a spend decision.
A number that can be mislabelled is the exact failure that let the retracted ~$4
figure stand.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from scripts.v3_measure_novelty_reuse import (  # noqa: E402
    MeasurementError,
    _hash_or_die,
    _verify_population,
)


def _asset(path: Path, *, coverage_routing=None, version="discovery-v1-real"):
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    rows = [("sidecar_version", version)]
    if coverage_routing is not None:
        rows.append(("coverage_routing", coverage_routing))
    conn.executemany("INSERT INTO meta VALUES (?,?)", rows)
    conn.commit()
    conn.close()
    return path


def test_a_legacy_asset_cannot_be_labelled_pinned(tmp_path):
    """THE round-5 property: the label is verified, not asserted.

    `meta.coverage_routing = 'gen2_router'` is the row `finalize_build` writes when
    it ingests gen-2's router, which is what makes a population v3. Anything else is
    legacy, whatever the caller types.
    """
    legacy = _asset(tmp_path / "legacy.db")
    assert _verify_population(str(legacy), "legacy")["meta_coverage_routing"] is None
    with pytest.raises(MeasurementError, match="not 'gen2_router'"):
        _verify_population(str(legacy), "pinned")


def test_a_lever1_asset_is_also_legacy(tmp_path):
    """A build that CHOSE the legacy cliff is a legacy population too -- the field
    records what ran, so `lever1_cliff` must not pass as pinned."""
    cliff = _asset(tmp_path / "cliff.db", coverage_routing="lever1_cliff")
    with pytest.raises(MeasurementError, match="not 'gen2_router'"):
        _verify_population(str(cliff), "pinned")


def test_a_router_asset_cannot_be_labelled_legacy(tmp_path):
    """The other direction, which matters for a different reason: an under-claimed
    v3 measurement gets ignored as stale, so the real price never reaches the owner."""
    routed = _asset(tmp_path / "v3.db", coverage_routing="gen2_router")
    assert _verify_population(str(routed), "pinned")["meta_coverage_routing"] == "gen2_router"
    with pytest.raises(MeasurementError, match="Label it `pinned`"):
        _verify_population(str(routed), "legacy")


def test_an_unreadable_asset_halts_rather_than_defaulting(tmp_path):
    with pytest.raises(MeasurementError, match="cannot read `meta`"):
        _verify_population(str(tmp_path / "does-not-exist.db"), "legacy")


def test_an_unhashable_input_halts_instead_of_becoming_a_report_string(tmp_path):
    """Codex R5: `_hash` "converts an unreadable input into a report string and
    still exits zero", so a report could describe candidates whose inputs were not
    all hash-bound while still looking like option-0 evidence."""
    good = tmp_path / "f.bin"
    good.write_bytes(b"abc")
    assert len(_hash_or_die(str(good), "good")) == 64

    with pytest.raises(MeasurementError, match="cannot hash input"):
        _hash_or_die(str(tmp_path / "missing.bin"), "missing")


def test_the_hashes_are_taken_before_the_build_and_reverified_after():
    """A hash taken only afterwards does not cover the population that was built.

    Asserted on the source ORDER because the failure needs a minutes-long build to
    reproduce, and the property is structural: the pre-build hash must precede
    `build_all_candidates`, and a post-build re-verification must exist.
    """
    import scripts.v3_measure_novelty_reuse as mod

    src = Path(mod.__file__).read_text(encoding="utf-8")
    pre = src.index("hashing inputs BEFORE building candidates")
    build = src.index("build_all_candidates(")
    post = src.index("re-verifying input hashes AFTER the build")
    assert pre < build < post, (
        "the input hashes are no longer taken before the build and re-verified "
        "after it, so the report can describe a population its hashes do not cover"
    )
    assert "CHANGED during the measurement" in src, (
        "a changed input no longer halts the measurement"
    )


def test_a_failed_measurement_exits_nonzero(tmp_path, capsys):
    """An operator running this for a spend decision must not read a traceback as
    a result, nor a zero exit as a pass."""
    from scripts.v3_measure_novelty_reuse import _cli

    rc = _cli(["--asset", str(tmp_path / "nope.db"), "--population", "pinned"])
    assert rc == 1
    assert "fail-closed" in capsys.readouterr().err
