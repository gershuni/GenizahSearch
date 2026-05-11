# -*- coding: utf-8 -*-
"""Phase 86 Plan 01: deterministic unit tests for shared/fist_cudl_bridge.

Builds small in-memory FIST.db schemas and exercises bridge logic directly;
NEVER touches real fist_data/FIST.db. Follows Phase 84 Round 3 Codex MEDIUM
discipline: no monkeypatch of sqlite3.connect outside test boundary.

Coverage:
  - 4 D-02a normalizers + Mosseri concat form (HIGH #1)
  - (N) strip family-gating negative fixture (Codex MEDIUM)
  - explain_fist_by_cudl status API (HIGH #6)
  - InventoryRecord title metadata propagation via 3-table production join
    (Gemini HIGH #8 + Pass 2 HIGH-2)
  - T-S NS 329.96 closure (originating user case)
"""
from __future__ import annotations

import sqlite3

import pytest

from shared.fist_cudl_bridge import (
    InventoryRecord,
    build_fist_alias_index,
    explain_fist_by_cudl,
    fist_to_cudl_keys,
    lookup_fist_by_cudl,
)


# ---------------------------------------------------------------------------
# Per-test module-state reset (avoids cross-test alias-index leakage).
# Module-scoped autouse fixture keeps conftest.py byte-clean.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_fist_alias_index():
    """Reset shared.fist_cudl_bridge._FIST_ALIAS_INDEX before each test.

    The bridge holds module-level state in _FIST_ALIAS_INDEX; tests that build
    their own seeds must start from a clean slate to avoid bleed-through from
    a previous test's seed.
    """
    import shared.fist_cudl_bridge as bridge

    bridge._FIST_ALIAS_INDEX = None
    yield
    bridge._FIST_ALIAS_INDEX = None


# ---------------------------------------------------------------------------
# TestFistToCudlKeys -- the 4 D-02a normalizers + Mosseri concat + (N) gating
# ---------------------------------------------------------------------------


class TestFistToCudlKeys:
    def test_empty_input(self):
        assert fist_to_cudl_keys("") == set()
        assert fist_to_cudl_keys(None) == set()

    def test_mosseri_roman_expansion(self):
        keys = fist_to_cudl_keys("Moss. III,27.1")
        assert "mosseriiii27.1" in keys, f"keys={keys}"

    def test_mosseri_roman_concat_form(self):
        """RESEARCH.md Pitfall 1 / Gemini+Codex HIGH #1: CUDL stores BOTH dotted and concat forms."""
        keys = fist_to_cudl_keys("Moss. III,27.1")
        assert "mosseriiii271" in keys, (
            f"Mosseri concat form missing (HIGH #1): {keys}"
        )

    def test_mosseri_roman_case_insensitive(self):
        keys = fist_to_cudl_keys("Moss. iii,27.1")
        assert "mosseriiii27.1" in keys
        assert "mosseriiii271" in keys

    def test_prefix_strip_after_last_colon(self):
        # NOTE (Rule 1 deviation): plan must_have spec wrote 'mosseriv27.1' /
        # 'mosseriv271' but the actual Mosseri concat pattern is mosseri + iv
        # (Roman IV) -> 'mosseriiv' (the trailing 'i' of "mosseri" plus 'i'
        # of "IV"). The III case the plan got right uses 'mosseriiii' (mosseri
        # + iii). Verified against construct_mosseri_cudl_label():
        # 'Moss. IV,27.1' -> 'MS-MOSSERI-IV-00027-00001' ->
        # _index_key_for_label = 'mosseriiv271'. Plan typo dropped one 'i'.
        keys = fist_to_cudl_keys("Mosseri: Moss. IV,27.1")
        assert "mosseriiv27.1" in keys, f"keys={keys}"
        assert "mosseriiv271" in keys, f"keys={keys}"

    @pytest.mark.parametrize("fixture", [
        "Library Shelmarks: Or. 1081/73b",
        "AIU: CUL: Or.1081 1.68",
        "T-S Ar.: T-S Ar 18.34",
    ])
    def test_prefix_strip_real_fixtures(self, fixture):
        keys = fist_to_cudl_keys(fixture)
        assert keys, f"prefix-strip failed for {fixture!r}: empty key set"

    def test_series_n_strip_tsf(self):
        keys1 = fist_to_cudl_keys("T-S F1(1).11")
        keys2 = fist_to_cudl_keys("T-S F1(2).11")
        assert "tsf1.11" in keys1, f"keys1={keys1}"
        assert "tsf1.11" in keys2, f"keys2={keys2}"

    def test_series_n_strip_tsar(self):
        keys = fist_to_cudl_keys("T-S Ar 18(2).34")
        # Any one of the dropped-(N) normalized variants is acceptable.
        stripped_present = any(("(" not in k and "tsar18" in k) for k in keys)
        assert stripped_present, (
            f"T-S Ar (N) strip didn't emit a dropped-(N) form: {keys}"
        )

    def test_series_n_strip_family_gating_add_not_stripped(self):
        """Codex MEDIUM: (N) strip is family-gated to T-S F / T-S Ar.

        Unrelated shelfmarks like 'Add. 12 (1)' must NOT spuriously gain
        a dropped-(N) alias.
        """
        keys = fist_to_cudl_keys("Add. 12 (1)")
        # Whatever normalizes from 'Add. 12 (1)' SHOULD include only the
        # cudl_normalize result of the literal input, not a dropped-(N)
        # version. The key derived from `Add. 12` alone must NOT appear.
        from shared.shelfmark_bridge import cudl_normalize
        stripped = cudl_normalize("Add. 12")
        assert stripped not in keys or stripped == cudl_normalize("Add. 12 (1)"), (
            f"Add. family wrongly stripped (N): {keys}"
        )

    def test_or_dot_fix(self):
        keys = fist_to_cudl_keys("Or.1080 1.5")
        assert ("or1080.1.5" in keys) or ("or1080.15" in keys), f"keys={keys}"


# ---------------------------------------------------------------------------
# In-memory FIST.db seed helper.
# Pass 2 HIGH-2: schema MUST include dbo_Signature so the test exercises
# the same 3-table join as production scripts/export_fist_enrichment.py.
# ---------------------------------------------------------------------------


def _seed_fist(rows, signatures=None, sig_links=None, ucr_rows=None):
    """Seed an in-memory FIST.db.

    rows:        list of (inv_id, shelfmark, alma_id_or_None)
    signatures:  optional list of (inv_id, set_signature_id) -> dbo_InventorySignature
    sig_links:   optional list of (set_signature_id, signature_id) -> dbo_Signature
                 (Pass 2 HIGH-2: production join routes through this table)
    ucr_rows:    optional list of (ucr_id, signature_id, title_heb, genizah_title)
                 where signature_id matches dbo_Signature.SignatureId (NOT
                 dbo_InventorySignature.SetSignatureId -- that was the bug)
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE dbo_Inventory (InventoryId INTEGER PRIMARY KEY, Shelfmark TEXT);
        CREATE TABLE dbo_InventoryAlma (ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                        InventoryId INTEGER, AlmaId INTEGER);
        CREATE TABLE dbo_InventorySignature (InventoryId INTEGER, SetSignatureId INTEGER);
        CREATE TABLE dbo_Signature (SetSignatureId INTEGER, SignatureId INTEGER);
        CREATE TABLE dbo_UnitCatalogRec (
            UnitCatalogRecId INTEGER PRIMARY KEY AUTOINCREMENT,
            SignatureId INTEGER, Title TEXT, GenizahTitleText TEXT
        );
    """)
    for inv_id, shelfmark, alma_id in rows:
        conn.execute("INSERT INTO dbo_Inventory VALUES (?, ?)", (inv_id, shelfmark))
        if alma_id is not None:
            conn.execute(
                "INSERT INTO dbo_InventoryAlma (InventoryId, AlmaId) VALUES (?, ?)",
                (inv_id, alma_id),
            )
    for inv_id, set_sig_id in (signatures or []):
        conn.execute(
            "INSERT INTO dbo_InventorySignature VALUES (?, ?)", (inv_id, set_sig_id)
        )
    for set_sig_id, sig_id in (sig_links or []):
        conn.execute(
            "INSERT INTO dbo_Signature (SetSignatureId, SignatureId) VALUES (?, ?)",
            (set_sig_id, sig_id),
        )
    for ucr_id, sig_id, title, gtitle in (ucr_rows or []):
        conn.execute(
            "INSERT INTO dbo_UnitCatalogRec (UnitCatalogRecId, SignatureId, Title, GenizahTitleText) "
            "VALUES (?, ?, ?, ?)",
            (ucr_id, sig_id, title, gtitle),
        )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# TestExplainFistByCudl -- status API (Codex HIGH #6)
# ---------------------------------------------------------------------------


class TestExplainFistByCudl:
    def test_status_not_found(self):
        # Index is None (autouse reset), so empty alias index produces 'not_found'.
        status, entries = explain_fist_by_cudl("ts12.345")
        assert status == "not_found"
        assert entries == []

    def test_status_single(self):
        conn = _seed_fist([(65549106, "T-S NS 329.96", None)])
        build_fist_alias_index(conn)
        status, entries = explain_fist_by_cudl("tsns329.96")
        assert status == "single", f"got status={status}"
        assert len(entries) == 1
        assert entries[0].inventory_id == 65549106

    def test_status_multi_inventory_ambiguous(self):
        """Codex HIGH #6: multi_inventory MUST be distinguishable from not_found."""
        conn = _seed_fist([
            (10, "T-S 12.345", None),
            (11, "T-S 12.345", None),
        ])
        build_fist_alias_index(conn)
        status, entries = explain_fist_by_cudl("ts12.345")
        assert status == "multi_inventory_ambiguous"
        assert len(entries) >= 2
        # Convenience wrapper still returns None.
        assert lookup_fist_by_cudl("ts12.345") is None


# ---------------------------------------------------------------------------
# TestLookupFistByCudl -- convenience wrapper + title metadata + T-S NS 329.96
# ---------------------------------------------------------------------------


class TestLookupFistByCudl:
    def test_empty_index_returns_none(self):
        # Autouse reset already set _FIST_ALIAS_INDEX = None.
        assert lookup_fist_by_cudl("ts12.345") is None

    def test_one_inventory_resolves_unambiguously(self):
        """Renamed per Gemini MEDIUM (old conceptually-misleading name retired).

        The new CUDL-walked architecture queries dbo_Inventory directly;
        multi-signature within one InventoryId is implicit. The behaviour
        being tested is: 'one inventory resolves cleanly via D-04 relax'.
        T-S NS 329.96 (InventoryId 65549106) is the originating user case.
        """
        conn = _seed_fist([(65549106, "T-S NS 329.96", None)])
        build_fist_alias_index(conn)
        rec = lookup_fist_by_cudl("tsns329.96")
        assert rec is not None, "T-S NS 329.96 must close under D-04 relax"
        assert rec.inventory_id == 65549106
        assert rec.has_alma is False

    def test_inventory_record_carries_title_metadata(self):
        """Gemini HIGH #8 + Pass 2 HIGH-2: title_heb/genizah_title must propagate
        from dbo_UnitCatalogRec, exercising the production 3-table join shape
        (dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec).
        """
        conn = _seed_fist(
            rows=[(700, "T-S 10J1.1", None)],
            signatures=[(700, 7001)],      # InventorySignature: inv -> SetSignatureId
            sig_links=[(7001, 70010)],     # Signature: SetSignatureId -> SignatureId
            ucr_rows=[(1, 70010, "שיר ידיד", "Liturgy poem")],  # UnitCatalogRec keyed by SignatureId
        )
        build_fist_alias_index(conn)
        rec = lookup_fist_by_cudl("ts10j1.1")
        assert rec is not None, "expected inventory 700 to resolve"
        assert rec.title_heb == "שיר ידיד", f"title_heb={rec.title_heb!r}"
        assert rec.genizah_title == "Liturgy poem", f"genizah_title={rec.genizah_title!r}"

    def test_has_alma_propagates(self):
        conn = _seed_fist([(200, "T-S Ar 1.1", 9001)])
        build_fist_alias_index(conn)
        rec = lookup_fist_by_cudl("tsar1.1")
        assert rec is not None
        assert rec.has_alma is True
        assert rec.inventory_id == 200

    def test_cudl_normalize_cascade(self):
        conn = _seed_fist([(300, "T-S F 8.2", None)])
        build_fist_alias_index(conn)
        rec = lookup_fist_by_cudl("tsf8.2")
        assert rec is not None
        assert rec.inventory_id == 300
