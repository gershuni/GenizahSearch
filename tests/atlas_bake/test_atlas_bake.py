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
GOLDEN_BR = FIXTURES_DIR / "golden-v1.bin.br"
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
# 2b. test_edge_class_semantics (HIGH-1 -- FROZEN schema §4 id 11 polarity:
#     0 = continuation (same-work), 1 = island (citation/quotation))
# ---------------------------------------------------------------------------

def test_edge_class_semantics():
    # A synthetic graph with a KNOWN continuation pair and a KNOWN island pair.
    n, n_island = 40, 8
    n_cont = n - n_island
    ms_pairs, sys_meta, domains, sys_ids = bake.synthetic_dataset(
        n, seed=42, n_island=n_island)
    result = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)

    # sys_id -> node index, then an undirected {frozenset(node_idx): cls} map.
    idx = {node[6]: i for i, node in enumerate(result.nodes)}
    edge_cls = {frozenset((src, tgt)): cls for src, tgt, cls in result.edges}

    # (sys_ids[0], sys_ids[1]) is a continuation-dominant pair (cont=3, isl=0).
    cont_pair = frozenset((idx[sys_ids[0]], idx[sys_ids[1]]))
    assert edge_cls[cont_pair] == 0, "continuation edge MUST encode as 0 (schema §4 id 11)"

    # (sys_ids[n_cont], sys_ids[n_cont+1]) is an island-chain pair (cont=0, isl=2).
    island_pair = frozenset((idx[sys_ids[n_cont]], idx[sys_ids[n_cont + 1]]))
    assert edge_cls[island_pair] == 1, "island edge MUST encode as 1 (schema §4 id 11)"

    # And globally: every continuation-dominant ms_pair -> 0, every other -> 1.
    for (a, b), r in ms_pairs.items():
        ca, cb = bake.validate_sys_id(a), bake.validate_sys_id(b)
        key = frozenset((idx[ca], idx[cb]))
        expected = 0 if r[2] >= max(1, r[3]) else 1
        assert edge_cls[key] == expected


# ---------------------------------------------------------------------------
# 2c. test_mixed_sys_id_key_types_canonicalized (HIGH-2 -- one canonical int
#     representation for pair endpoints AND metadata keys)
# ---------------------------------------------------------------------------

def test_mixed_sys_id_key_types_canonicalized():
    # The SAME three manuscripts referenced with a deliberate MIX of str and
    # int sys_id forms across pairs, titles, and domains. Before canonicalizing
    # every id, str endpoints spawn phantom/duplicate nodes and int endpoints
    # miss their str-keyed metadata (title/domain/library lost).
    ms_pairs = {
        ("100", 200): [3, 40, 3, 0],    # str + int endpoints, continuation
        (200, "300"): [2, 30, 2, 0],    # int + str endpoints, continuation
        ("300", "100"): [1, 20, 0, 1],  # both str, island (closes the triangle)
    }
    sys_meta = {
        100: ("SM-100", "CUL", "Title A"),    # int key
        "200": ("SM-200", "JTS", "Title B"),  # str key
        300: ("SM-300", "RNL", "Title C"),    # int key
    }
    domains = {
        "100": (Counter({0: 2}), Counter({"Bible-he": 2})),    # str key -> group 0
        200: (Counter({6: 1}), Counter({"Talmud-he": 1})),     # int key -> group 6
        "300": (Counter({2: 3}), Counter({"Piyyut-he": 3})),   # str key -> group 2
    }

    result = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)

    # Exactly three nodes, no duplicate/extra, no missing.
    assert len(result.nodes) == 3
    assert result.missing == []
    assert result.extra == []

    by_sysid = {node[6]: node for node in result.nodes}
    assert set(by_sysid) == {100, 200, 300}  # all canonical ints, no phantom nodes

    # Node fields: [x, y, ci, domain_idx, lib_idx, prom, sys_id, title, shelfmark]
    assert by_sysid[100][7] == "Title A" and by_sysid[100][8] == "SM-100"
    assert by_sysid[200][7] == "Title B" and by_sysid[200][8] == "SM-200"
    assert by_sysid[300][7] == "Title C" and by_sysid[300][8] == "SM-300"

    # Domain index resolved from the correctly-typed key (not the OTHER fallback).
    assert by_sysid[100][3] == 0
    assert by_sysid[200][3] == 6
    assert by_sysid[300][3] == 2

    # Library index maps back to the right catalogue code for every node.
    libs = result.libraries
    assert libs[by_sysid[100][4]] == "CUL"
    assert libs[by_sysid[200][4]] == "JTS"
    assert libs[by_sysid[300][4]] == "RNL"


# ---------------------------------------------------------------------------
# 2d. test_bake_rejects_node_set_mismatch (HIGH-3 -- exact set equality is
#     ENFORCED, not merely reported)
# ---------------------------------------------------------------------------

def test_bake_rejects_node_set_mismatch(tmp_path):
    ms_pairs, sys_meta, domains, _ids = bake.synthetic_dataset(40, seed=42)
    result = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)

    # Healthy result passes the gate.
    bake.assert_bake_complete(result)

    # A missing eligible id must FAIL the bake (refuse to write).
    result.missing = [result.nodes[0][6]]
    with pytest.raises(ValueError):
        bake.assert_bake_complete(result)
    with pytest.raises(ValueError):
        bake._write_production(result, str(tmp_path), "some-real-db-hash")

    # An extra placed id must also FAIL the bake.
    result2 = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    result2.extra = [10 ** 18 + 7]
    with pytest.raises(ValueError):
        bake.assert_bake_complete(result2)
    with pytest.raises(ValueError):
        bake._write_golden(result2, str(tmp_path / "reject.bin"))

    # A placed/eligible count skew with empty lists is still rejected.
    result3 = bake.run_bake(ms_pairs, sys_meta, domains, seed=42)
    result3.placed_count = result3.eligible_count - 1
    with pytest.raises(ValueError):
        bake.assert_bake_complete(result3)


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

    # MEDIUM-2: the golden dataset is sized so at least one cluster clears
    # LABEL_MIN_N -- otherwise the CLUSTER_LABEL_* sections pass vacuously.
    # Assert the label sections are non-empty AND field-correct.
    labels = decoded["cluster_labels"]
    assert len(labels) >= 1, "golden fixture must exercise the cluster-label sections"
    node_clusters = {n["cluster"] for n in decoded["nodes"]}
    for lab in labels:
        assert lab["ci"] in node_clusters                # references a real cluster
        assert lab["n"] >= bake.LABEL_MIN_N              # only >= LABEL_MIN_N clusters get labelled
        assert 0 <= lab["dgrp"] < len(bake.DOMAIN_GROUPS)  # valid domain-group index
        assert isinstance(lab["title"], str) and lab["title"]  # non-empty representative title
        assert isinstance(lab["dom"], str) and lab["dom"]      # non-empty domain text label
        assert isinstance(lab["x"], float) and isinstance(lab["y"], float)
        assert isinstance(lab["r"], float) and lab["r"] > 0

    # The golden fixture carries a deliberately fabricated (never-real)
    # XSS-shaped catalogue string for the downstream 133-04 DOM-XSS decode
    # test -- confirm it round-tripped byte-for-byte.
    malicious = [n for n in decoded["nodes"] if "onerror" in n["title"]]
    assert len(malicious) == 1
    assert "</script" in malicious[0]["title"]


# ---------------------------------------------------------------------------
# 8b. test_golden_encoder_output_locked (MEDIUM-1 -- lock the ENCODER, not
#     just the committed bytes' decodability, so encoder drift fails the test)
# ---------------------------------------------------------------------------

def test_golden_encoder_output_locked():
    # Rebuild the canonical golden input in-test and assert the encoder
    # reproduces the committed golden bytes EXACTLY. Without this, encode_asset
    # could silently drift (e.g. the EDGE_CLASS polarity flip in HIGH-1) while
    # test_golden_python_decode stays green, because that test only decodes the
    # already-committed binary.
    ms_pairs, sys_meta, domains, _ids = bake.golden_dataset()
    result = bake.run_bake(ms_pairs, sys_meta, domains, seed=bake.SEED)
    encoded = bake.encode_asset(result)

    assert encoded.plain_bytes == GOLDEN_BIN.read_bytes(), (
        "encode_asset() drifted from the committed golden-v1.bin -- regenerate "
        "via `python scripts/build_atlas_asset.py --golden tests/fixtures/atlas/golden-v1.bin` "
        "if the change is intentional (and re-review the schema contract)"
    )
    assert brotli.compress(encoded.plain_bytes, quality=11) == GOLDEN_BR.read_bytes(), (
        "Brotli-compressed golden output drifted from the committed golden-v1.bin.br"
    )

    # Encoder drift on a KNOWN edge would silently pass a decode-only test:
    # assert the semantic polarity holds in the freshly-encoded asset too.
    assert any(cls == 0 for _, _, cls in result.edges)  # continuation edges present
    assert any(cls == 1 for _, _, cls in result.edges)  # island edges present


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
