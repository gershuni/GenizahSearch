# -*- coding: utf-8 -*-
"""Population lock (schema Amendment 2026-08-12 (S)) — fam-v1, the emitter's
measure functions, the builder's copied constants, and the retention gate
PROVEN able to fail (a retention gate that has never breached in a test is a
gate nobody has watched work)."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(REPO_ROOT), str(REPO_ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import build_discovery_sidecar as sidecar_build
import emit_population_lock as emitter
import verify_discovery_sidecar as verify_mod

import shared.discovery_family as fam

LOCK_PATH = REPO_ROOT / "docs" / "specs" / "discovery-population-lock-v1.json"


# ---------------------------------------------------------------------------
# fam-v1 — the frozen assignment
# ---------------------------------------------------------------------------

def test_base_family_is_the_a0c_frozen_rule():
    assert fam.base_family("anything at all", "ja") == "ja"
    # language first, content second: a JA-corpus work is ja even with a
    # Bible-marker genre (Arabic tafsir of Bible sits in ja).
    assert fam.base_family("Bible: Texts / Tafsir", "ja") == "ja"
    assert fam.base_family("Bible: Texts / Something", "msource") == "bible"
    assert fam.base_family("Targumim / Onkelos", "sefaria") == "bible"
    assert fam.base_family("Talmud Bavli / Tractates", "msource") == "canonical"
    assert fam.base_family("Mishnah / Seder Moed", "msource") == "canonical"
    assert fam.base_family("Rabbinic Literature / Halakhah", "msource") == "other_staged"
    assert fam.base_family(None, "sefaria") == "other_staged"


def test_daf_override_applies_only_to_other_staged():
    overrides = {"w_rif"}
    assert fam.assign_family("Halakhah", "msource", "w_rif", overrides) == "daf"
    assert fam.assign_family("Halakhah", "msource", "w_else", overrides) == "other_staged"
    # An override can never corrupt another family.
    assert fam.assign_family("Talmud Bavli", "msource", "w_rif", overrides) == "canonical"
    assert fam.assign_family("x", "ja", "w_rif", overrides) == "ja"


def test_family_vocabulary_matches_the_amendment():
    assert fam.FAMILIES == ("bible", "canonical", "daf", "ja", "other_staged")
    assert fam.FAMILY_VERSION == "fam-v1"


# ---------------------------------------------------------------------------
# The tracked lock artifact — self-consistency
# ---------------------------------------------------------------------------

def test_tracked_lock_artifact_is_self_consistent():
    lock = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
    assert lock["lock_version"] == emitter.LOCK_VERSION
    assert lock["family_version"] == fam.FAMILY_VERSION
    assert set(lock["by_family"]) == set(fam.FAMILIES)
    assert lock["total"] == sum(lock["by_family"].values())
    assert 0 < lock["retention_floor_per_family"] <= lock["retention_floor_overall"] < 1
    assert lock["daf_override_canonical_ids"] == sorted(set(lock["daf_override_canonical_ids"]))
    # The measured reproduction of the preflight §2 denominators -- the
    # acceptance test fam-v1 was built against. A lock re-emitted against a
    # DIFFERENT asset legitimately changes these; that is a new lock file and
    # an A0b conversation, so this pin failing is a decision point, not noise.
    assert lock["total"] == 28464
    assert lock["by_family"] == {
        "bible": 19885, "canonical": 1823, "daf": 1026, "ja": 3842,
        "other_staged": 1888,
    }


# ---------------------------------------------------------------------------
# Emitter measure functions over a hand-built micro-asset
# ---------------------------------------------------------------------------

def _micro_public_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE works (work_id TEXT, canonical_work_id TEXT, "
                 "genre TEXT, source_corpus TEXT)")
    conn.execute("CREATE TABLE discovery_identification (display_work_id TEXT, "
                 "canonical_work_id TEXT, main_pool INTEGER)")
    conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
    conn.execute("INSERT INTO meta VALUES ('audience', 'public')")
    works = [
        ("w1", "w1", "Bible: Texts / Torah", "msource"),
        ("w2", "w2", "Talmud Bavli / X", "msource"),
        ("w3", "w3", "Halakhah / Y", "sefaria"),   # the Rif-like daf work
        ("w4", "w4", "Liturgy / Z", "sefaria"),
        ("w5", "w5", "Anything", "ja"),
    ]
    conn.executemany("INSERT INTO works VALUES (?, ?, ?, ?)", works)
    idents = [
        ("w1", "w1", 1), ("w1", "w1", 0),   # main_pool=0 must NOT count
        ("w2", "w2", 1),
        ("w3", "w3", 1),
        ("w4", "w4", 1),
        ("w5", "w5", 1),
    ]
    conn.executemany(
        "INSERT INTO discovery_identification VALUES (?, ?, ?)", idents)
    return conn


def test_measure_population_counts_main_pool_only():
    conn = _micro_public_conn()
    try:
        counts = emitter.measure_population(conn, daf_overrides={"w3"})
        assert counts == {
            "bible": 1, "canonical": 1, "daf": 1, "ja": 1, "other_staged": 1,
        }
    finally:
        conn.close()


def test_measure_daf_overrides_requires_daf_rif_grain_and_other_staged_base():
    pub = _micro_public_conn()
    loc = sqlite3.connect(":memory:")
    try:
        loc.execute("CREATE TABLE locus_work (locus_ref_id TEXT, family TEXT, "
                    "grain TEXT)")
        loc.executemany("INSERT INTO locus_work VALUES (?, ?, ?)", [
            ("REF:rif", "sefaria", "daf_rif"),      # -> override (base other_staged)
            ("REF:bavli", "sefaria", "daf_bavli"),  # wrong grain -> no
            ("REF:tal", "msource_daf", "daf_rif"),  # base canonical -> no
            ("REF:orphan", "sefaria", "daf_rif"),   # no crosswalk entry -> no
        ])
        crosswalk = {"REF:rif": "w3", "REF:bavli": "w4", "REF:tal": "w2"}
        assert emitter.measure_daf_overrides(pub, loc, crosswalk) == ["w3"]
    finally:
        pub.close()
        loc.close()


# ---------------------------------------------------------------------------
# Builder meta rows
# ---------------------------------------------------------------------------

def _lock_dict(**overrides):
    lock = {
        "lock_version": "poplock-v1",
        "family_version": fam.FAMILY_VERSION,
        "total": 5,
        "by_family": {"bible": 1, "canonical": 1, "daf": 1, "ja": 1, "other_staged": 1},
        "retention_floor_overall": 0.9,
        "retention_floor_per_family": 0.8,
        "daf_override_canonical_ids": ["w3"],
    }
    lock.update(overrides)
    return lock


def test_population_lock_meta_rows_shape():
    rows = dict(sidecar_build.population_lock_meta_rows(_lock_dict(), "f" * 64))
    assert rows["population_lock_version"] == "poplock-v1"
    assert rows["population_lock_family_version"] == "fam-v1"
    assert rows["population_lock_total"] == "5"
    for family in fam.FAMILIES:
        assert rows[f"population_lock_family_{family}"] == "1"
    assert json.loads(rows["population_lock_daf_overrides"]) == ["w3"]


def test_population_lock_meta_rows_rejects_wrong_family_contract():
    with pytest.raises(ValueError):
        sidecar_build.population_lock_meta_rows(
            _lock_dict(family_version="fam-v2"), "f" * 64)
    with pytest.raises(ValueError):
        sidecar_build.population_lock_meta_rows(
            _lock_dict(by_family={"bible": 1, "mystery": 4}), "f" * 64)


# ---------------------------------------------------------------------------
# The retention gate — clean, conditional, and PROVEN able to fail
# ---------------------------------------------------------------------------

def _lock_meta(total, by_family, **overrides):
    meta = {
        "population_lock_version": "poplock-v1",
        "population_lock_family_version": fam.FAMILY_VERSION,
        "population_lock_total": str(total),
        "population_lock_retention_floor_overall": "0.9",
        "population_lock_retention_floor_per_family": "0.8",
        "population_lock_daf_overrides": json.dumps(["w3"]),
    }
    for family in fam.FAMILIES:
        meta[f"population_lock_family_{family}"] = str(by_family.get(family, 0))
    meta.update(overrides)
    return meta


def test_retention_gate_skips_pre_lock_assets():
    conn = _micro_public_conn()
    try:
        assert verify_mod.check_population_lock_retention(conn, {}) == []
    finally:
        conn.close()


def test_retention_gate_clean_when_population_holds():
    conn = _micro_public_conn()
    try:
        meta = _lock_meta(5, {f: 1 for f in fam.FAMILIES})
        assert verify_mod.check_population_lock_retention(conn, meta) == []
    finally:
        conn.close()


def test_retention_gate_proven_able_to_fail():
    conn = _micro_public_conn()
    try:
        # Lock claims twice the population that exists: overall AND every
        # family floor must breach.
        meta = _lock_meta(10, {f: 2 for f in fam.FAMILIES})
        violations = verify_mod.check_population_lock_retention(conn, meta)
        assert any("overall retention breached" in v for v in violations)
        assert sum("retention breached" in v for v in violations) == 6
    finally:
        conn.close()


def test_retention_gate_refuses_a_different_family_rule():
    conn = _micro_public_conn()
    try:
        meta = _lock_meta(5, {f: 1 for f in fam.FAMILIES},
                          population_lock_family_version="fam-v2")
        violations = verify_mod.check_population_lock_retention(conn, meta)
        assert len(violations) == 1 and "fam-v1" in violations[0]
    finally:
        conn.close()


def test_retention_gate_fails_closed_on_incomplete_lock_keys():
    conn = _micro_public_conn()
    try:
        meta = _lock_meta(5, {f: 1 for f in fam.FAMILIES})
        del meta["population_lock_family_daf"]
        violations = verify_mod.check_population_lock_retention(conn, meta)
        assert len(violations) == 1 and "incomplete" in violations[0]
    finally:
        conn.close()
