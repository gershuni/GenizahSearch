# -*- coding: utf-8 -*-
"""Bake invariant tests for scripts/build_atlas_asset.py (Phase 133 ATLAS-01).

These tests drive the bake in --smoke/--golden (synthetic, fabricated) mode
so they run without the ~2.9 GB gitignored research DB. They require the
pinned bake-time deps in requirements-atlas-bake.txt (networkx/python-
louvain/Brotli) -- NOT installed in the main `tests` CI job
(requirements-lock.txt only), so this whole module is a clean pytest SKIP
(not a collection error) there via `pytest.importorskip`, and runs for real
in the dedicated `atlas-bake-tests` CI job (see .github/workflows/ci.yml).

The absolute node-count assertion against the REAL research DB (currently
~62,645 eligible, floor >= 62,414 per 133-CONTEXT.md D-09) is a PHASE-EXIT
check (plan 133-06), run manually against the real DB -- not here. These
tests lock the underlying LOGIC that check must rely on: exact
eligible==placed set equality, no-discovery-fields, byte budget, sys_id
precision + invalid-fails, determinism, content-hash invalidation, and a
golden per-field Python decode.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path

import pytest

networkx = pytest.importorskip("networkx")
community = pytest.importorskip("community")
brotli = pytest.importorskip("brotli")

from scripts import build_atlas_asset as bake  # noqa: E402

pytestmark = pytest.mark.atlas_bake

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "atlas"
GOLDEN_BIN = FIXTURES_DIR / "golden-v1.bin"
GOLDEN_EXPECTED = FIXTURES_DIR / "golden-v1-expected.json"

_FORBIDDEN_SUBSTRINGS = ("discovery", "gold_candidate", "gold_star", "is_discovery", "\"gold\"")


# ---------------------------------------------------------------------------
# 1. test_no_discovery_fields (D-04)
# ---------------------------------------------------------------------------

def test_no_discovery_fields():
    ms_pairs, sys_meta, domains, _ids = bake.synthetic_dataset(80, seed=42)
    result = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    encoded = bake.encode_asset(result)
    manifest = bake.build_manifest(
        result, encoded, "deadbeefcafe", "atlas-v1-deadbeefcafe", "test-source")

    manifest_text = json.dumps(manifest).lower()
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert forbidden not in manifest_text, f"manifest leaked a discovery-overlay field: {forbidden!r}"

    # Every section name in the frozen schema itself must be claim-free.
    for name in bake._SECTION_NAMES.values():
        assert "gold" not in name.lower()
        assert "discovery" not in name.lower()

    decoded = bake.decode_asset(encoded.plain_bytes)
    decoded_text = json.dumps(decoded).lower()
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert forbidden not in decoded_text, f"decoded payload leaked a discovery-overlay field: {forbidden!r}"


# ---------------------------------------------------------------------------
# 2. test_exact_node_set_equality (D-09 / HIGH-5 -- no ">=" fudge)
# ---------------------------------------------------------------------------

def test_exact_node_set_equality():
    # n_island=20 guarantees BOTH a multi-node island-only chain AND a true
    # singleton island-only component (see synthetic_dataset docstring).
    ms_pairs, sys_meta, domains, _ids = bake.synthetic_dataset(150, seed=42, n_island=20)
    result = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)

    assert result.missing == []
    assert result.extra == []
    assert result.placed_count == result.eligible_count

    eligible_ids = {int(bake.validate_sys_id(s)) for k in ms_pairs for s in k}
    placed_ids = {node[6] for node in result.nodes}
    assert placed_ids == eligible_ids

    # The manifest records both counts (this test's own -- the absolute
    # 62,645/floor-62,414 check against the real DB is plan 133-06's job).
    encoded = bake.encode_asset(result)
    manifest = bake.build_manifest(result, encoded, "x", "atlas-v1-x", "test-source")
    assert manifest["eligible_count"] == manifest["placed_count"] == len(eligible_ids)
    assert manifest["missing"] == []
    assert manifest["extra"] == []


# ---------------------------------------------------------------------------
# 3. test_byte_budget (D-10 / PERF-01)
# ---------------------------------------------------------------------------

def test_byte_budget():
    ms_pairs, sys_meta, domains, _ids = bake.synthetic_dataset(200, seed=42)
    result = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    encoded = bake.encode_asset(result)
    br_bytes = brotli.compress(encoded.plain_bytes, quality=11)

    assert len(br_bytes) <= bake.BYTE_BUDGET_CAP
    bake.assert_byte_budget(len(br_bytes))  # must not raise

    with pytest.raises(ValueError):
        bake.assert_byte_budget(bake.BYTE_BUDGET_CAP + 1)


# ---------------------------------------------------------------------------
# 4. test_sys_id_roundtrip
# ---------------------------------------------------------------------------

def test_sys_id_roundtrip():
    ms_pairs, sys_meta, domains, _ids = bake.synthetic_dataset(20, seed=42)
    result = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    encoded = bake.encode_asset(result)
    decoded = bake.decode_asset(encoded.plain_bytes)

    original_ids = {node[6] for node in result.nodes}
    decoded_ids = {int(nd["sys_id"]) for nd in decoded["nodes"]}
    assert original_ids == decoded_ids
    # Every synthetic sys_id is deliberately > 2**53 (Number.MAX_SAFE_INTEGER)
    # so the BigUint64 pathway is actually exercised, not < 2**64.
    assert all(2 ** 53 < sid < 2 ** 64 for sid in original_ids)


# ---------------------------------------------------------------------------
# 5. test_sys_id_invalid_fails_bake (Codex NEW LOW -- no fallback)
# ---------------------------------------------------------------------------

def test_sys_id_invalid_fails_bake():
    bad_sys_id = 2 ** 64 + 42  # out of BigUint64 range
    ms_pairs = {(1, bad_sys_id): [2, 10, 2, 0]}
    sys_meta = {1: ("SM1", "CUL", "Title1"), bad_sys_id: ("SM2", "CUL", "Title2")}
    domains = {1: (Counter({0: 1}), Counter()), bad_sys_id: (Counter({0: 1}), Counter())}

    with pytest.raises(ValueError):
        bake.run_bake(ms_pairs, sys_meta, domains, seed=42)

    with pytest.raises(ValueError):
        bake.validate_sys_id("12a34")  # non-pure-digit

    with pytest.raises(ValueError):
        bake.validate_sys_id(2 ** 64)  # exactly at the boundary -- must fail (< 2**64, not <=)


# ---------------------------------------------------------------------------
# 6. test_determinism
# ---------------------------------------------------------------------------

def test_determinism():
    ms_pairs, sys_meta, domains, _ids = bake.synthetic_dataset(200, seed=42)
    r1 = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    r2 = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    e1 = bake.encode_asset(r1)
    e2 = bake.encode_asset(r2)
    assert e1.plain_bytes == e2.plain_bytes


# ---------------------------------------------------------------------------
# 7. test_content_hash_changes (MEDIUM-4 -- no stale immutable-URL reuse)
# ---------------------------------------------------------------------------

def test_content_hash_changes():
    ms_pairs, sys_meta, domains, _ids = bake.synthetic_dataset(60, seed=42)

    result1 = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    encoded1 = bake.encode_asset(result1)
    hash1 = hashlib.sha256(encoded1.plain_bytes).hexdigest()[:12]

    sys_meta2 = dict(sys_meta)
    some_key = next(iter(sys_meta2))
    shelfmark, lib, title = sys_meta2[some_key]
    sys_meta2[some_key] = (shelfmark, lib, title + "x")  # one input byte changed

    result2 = bake.run_bake(ms_pairs, sys_meta2, domains, seed=42)
    encoded2 = bake.encode_asset(result2)
    hash2 = hashlib.sha256(encoded2.plain_bytes).hexdigest()[:12]

    assert hash1 != hash2


# ---------------------------------------------------------------------------
# 8. test_golden_python_decode
# ---------------------------------------------------------------------------

def test_golden_python_decode():
    data = GOLDEN_BIN.read_bytes()
    decoded = bake.decode_asset(data)
    expected = json.loads(GOLDEN_EXPECTED.read_text(encoding="utf-8"))

    assert decoded["schema_version"] == expected["schema_version"]
    assert len(decoded["nodes"]) == len(expected["nodes"])

    for got, want in zip(decoded["nodes"], expected["nodes"]):
        # sys_id (and any value > 2**53) is a decimal STRING in the expected
        # JSON, compared via int(str) -- so JSON.parse-style precision loss
        # can never mask a mismatch (schema §7 / checker cross-language-decode
        # observation).
        assert isinstance(want["sys_id"], str)
        assert int(got["sys_id"]) == int(want["sys_id"])
        assert got["title"] == want["title"]
        assert got["shelfmark"] == want["shelfmark"]
        assert got["cluster"] == want["cluster"]
        assert got["domain"] == want["domain"]
        assert got["library"] == want["library"]
        assert got["prominence"] == want["prominence"]
        assert got["x"] == pytest.approx(want["x"])
        assert got["y"] == pytest.approx(want["y"])

    assert decoded["edges"] == expected["edges"]
    assert decoded["flows"] == expected["flows"]
    assert decoded["cluster_labels"] == expected["cluster_labels"]

    # The golden fixture carries a deliberately fabricated (never-real)
    # XSS-shaped catalogue string for the downstream 133-04 DOM-XSS decode
    # test -- confirm it round-tripped byte-for-byte.
    malicious = [n for n in decoded["nodes"] if "onerror" in n["title"]]
    assert len(malicious) == 1
    assert "</script" in malicious[0]["title"]


# ---------------------------------------------------------------------------
# Bonus: CLI argument-contract coverage (Task 1 acceptance criterion)
# ---------------------------------------------------------------------------

def test_cli_requires_db_path_unless_smoke_or_golden():
    with pytest.raises(SystemExit) as exc_info:
        bake.main([])
    assert exc_info.value.code == 2


def test_cli_smoke_report_runs_without_db_path(capsys):
    rc = bake.main(["--smoke", "50", "--report"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "missing=0" in out
    assert "extra=0" in out
    assert "seed=42" in out
