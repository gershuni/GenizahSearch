"""Phase 86 Plan 02: integration tests for CUDL-walked _build_qualifying_inventories.

NEVER touches real libraries.csv, real reports/, or real fist_data/ — uses
in-memory sqlite seeds with the production-correct 3-table FIST join schema
(dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec) per Pass 2 HIGH-2.

Coverage:
  - T-S NS 329.96 closure (originating user case)
  - D-01a image-bearing-only invariant
  - D-04a + HIGH #6: multi_inventory distinct from no_fist_match
  - HIGH #7: alias-only Alma audit-only semantics (no synthetic emitted)
  - HIGH #8: title_heb/genizah_title propagation through qualifying dict
  - Codex MEDIUM: _classify_library_code Mosseri-prefix detection
  - Pass 2 HIGH-1: idempotency when synthetic block already in csv_bank
  - Pass 2 HIGH-1: _build_real_only_csv_bank strips synthetics + leaves input untouched
  - Pass 3 shared MEDIUM (Codex + Gemini): idempotency test exercises the REAL
    shared.shelfmark_bridge.build_alias_index against BOTH raw and synthetic-
    stripped variants and asserts contrasting lookup_cudl behaviour BEFORE
    invoking _build_qualifying_inventories.
"""
from __future__ import annotations

import csv  # noqa: F401  (helpers may use csv for residue inspection)
import sqlite3
from pathlib import Path  # noqa: F401  (kept for path-aware future tests)

import pytest

from shared.synthetic_sys_id import encode_inventory_sys_id, is_synthetic_sys_id


# ---------------------------------------------------------------------------
# Local seed helpers (Pass 2 HIGH-2 schema: include dbo_Signature so tests
# exercise the production 3-table join the bridge SQL uses).
# ---------------------------------------------------------------------------


def _make_fist_seed(sql_script: str = "") -> sqlite3.Connection:
    """Build an in-memory FIST.db with the 5-table schema the bridge needs.

    Pass 2 HIGH-2: dbo_Signature is included so build_fist_alias_index's
    3-table join (InventorySignature -> Signature -> UnitCatalogRec) actually
    fires. The previous 2-table shortcut schema would silently produce empty
    title metadata against the real FIST.db.
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE dbo_Inventory (
            InventoryId INTEGER PRIMARY KEY,
            Shelfmark TEXT
        );
        CREATE TABLE dbo_InventoryAlma (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            InventoryId INTEGER,
            AlmaId INTEGER
        );
        CREATE TABLE dbo_InventorySignature (
            InventoryId INTEGER,
            SetSignatureId INTEGER
        );
        CREATE TABLE dbo_Signature (
            SetSignatureId INTEGER,
            SignatureId INTEGER
        );
        CREATE TABLE dbo_UnitCatalogRec (
            UnitCatalogRecId INTEGER PRIMARY KEY AUTOINCREMENT,
            SignatureId INTEGER,
            Title TEXT,
            GenizahTitleText TEXT
        );
        """
    )
    if sql_script:
        conn.executescript(sql_script)
    conn.commit()
    return conn


def _make_nli_seed(classmarks: list) -> sqlite3.Connection:
    """Seed an in-memory nli_crossref.db with the production cambridge_manifests
    schema (label, manifest_url, normalized_shelfmark).

    Codex MEDIUM: schema MUST be 3 columns — _build_qualifying_inventories
    SELECTs all three.
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE cambridge_manifests (
            label TEXT,
            manifest_url TEXT,
            normalized_shelfmark TEXT
        );
        """
    )
    for c in classmarks:
        conn.execute(
            "INSERT INTO cambridge_manifests VALUES (?, ?, ?)",
            (c, f"https://example/{c}.json", c),
        )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Module-state reset autouse fixture (FIST alias index + Phase 84 alias index).
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Clear FIST alias index between tests; reset Phase 84 CUDL index too so
    tests that mutate it (e.g. idempotency test) don't bleed into others."""
    import shared.fist_cudl_bridge as fbridge
    import shared.shelfmark_bridge as sbridge

    fbridge._FIST_ALIAS_INDEX = None
    sbridge._CUDL_ALIAS_INDEX = None
    yield
    fbridge._FIST_ALIAS_INDEX = None
    sbridge._CUDL_ALIAS_INDEX = None


# ---------------------------------------------------------------------------
# Codex MEDIUM: monkeypatch the IMPORTED name in scripts.generate_synthetic_rows,
# NOT shared.shelfmark_bridge.lookup_cudl. Production code does
# `from shared.shelfmark_bridge import lookup_cudl` which binds the name into
# the consuming module's namespace; monkeypatching the source module would NOT
# affect the consumer.
# ---------------------------------------------------------------------------


@pytest.fixture
def empty_phase84_index(monkeypatch):
    """Make lookup_cudl return None for all inputs (no Phase 84 hits)."""
    monkeypatch.setattr(
        "scripts.generate_synthetic_rows.lookup_cudl",
        lambda classmark: None,
    )


# ---------------------------------------------------------------------------
# Main test class: CUDL-walked generation behaviour.
# ---------------------------------------------------------------------------


class TestCudlWalkedGeneration:
    def test_tsns_329_96_synthetic_emitted(self, empty_phase84_index):
        """D-04 relax + CUDL-walk: T-S NS 329.96 closes (originating user case)."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (65549106, 'T-S NS 329.96');
            -- No InventoryAlma row -> has_alma = False -> emits synthetic.
            """
        )
        nli = _make_nli_seed(["tsns329.96"])
        qualifying, residue = _build_qualifying_inventories(fist, nli)

        assert 65549106 in qualifying, (
            "T-S NS 329.96 (D-04 multi_signature relax) failed to emit"
        )
        rec = qualifying[65549106]
        assert rec["canonical_shelfmark"] == "T-S NS 329.96"
        assert rec["has_cudl_manifest"] is True  # D-01a invariant
        assert rec["library_code"] == "CUL"

    def test_synthetic_row_has_title_metadata(self, empty_phase84_index):
        """Pass 1 HIGH #8 + Pass 2 HIGH-2: title metadata propagates through
        the 3-table production-correct join (InventorySignature -> Signature
        -> UnitCatalogRec).
        """
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (700, 'T-S 10J1.1');
            INSERT INTO dbo_InventorySignature VALUES (700, 7001);
            INSERT INTO dbo_Signature (SetSignatureId, SignatureId) VALUES (7001, 70010);
            INSERT INTO dbo_UnitCatalogRec (UnitCatalogRecId, SignatureId, Title, GenizahTitleText)
              VALUES (1, 70010, 'שיר ידיד', 'Liturgy poem');
            -- No InventoryAlma row -> emits synthetic with title metadata.
            """
        )
        nli = _make_nli_seed(["ts10j1.1"])
        qualifying, _ = _build_qualifying_inventories(fist, nli)
        assert 700 in qualifying, "expected inventory 700 to emit synthetic"
        rec = qualifying[700]
        assert rec["title_heb"] == "שיר ידיד", f"title_heb={rec['title_heb']!r}"
        assert rec["genizah_title"] == "Liturgy poem", (
            f"genizah_title={rec['genizah_title']!r}"
        )
        assert rec["has_fjms_metadata"] is True

    def test_all_emitted_have_cudl_manifest(self, empty_phase84_index):
        """D-01a: every synthetic row HAS a CUDL manifest by construction."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (200, 'T-S NS 999.1');
            -- Inventory in FIST but NO cambridge_manifests entry.
            """
        )
        nli = _make_nli_seed([])  # no CUDL manifest at all
        qualifying, _ = _build_qualifying_inventories(fist, nli)
        assert 200 not in qualifying, (
            "Phase 86 D-01a: bib-only inclusion is FORBIDDEN"
        )

    def test_has_alma_skipped(self, empty_phase84_index):
        """HIGH #7: rec.has_alma=True means libraries.csv row exists; alias-only audit-only."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (300, 'T-S 12.1');
            INSERT INTO dbo_InventoryAlma (InventoryId, AlmaId) VALUES (300, 9001);
            """
        )
        nli = _make_nli_seed(["ts12.1"])
        qualifying, residue = _build_qualifying_inventories(fist, nli)
        assert 300 not in qualifying
        # Audit-only coverage: NOT logged to residue either.
        assert not any(r.get("classmark") == "ts12.1" for r in residue), (
            "alias-only Alma case should be silent (covered), not logged as residue"
        )

    def test_multi_inventory_ambiguity_kind_distinct(self, empty_phase84_index):
        """Codex HIGH #6: multi_inventory MUST be distinct from no_fist_match."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (500, 'T-S X 1.1');
            INSERT INTO dbo_Inventory VALUES (501, 'T-S X 1.1');
            """
        )
        nli = _make_nli_seed(["tsx1.1"])
        qualifying, residue = _build_qualifying_inventories(fist, nli)
        assert 500 not in qualifying
        assert 501 not in qualifying
        multi = [r for r in residue if r["ambiguity_kind"] == "multi_inventory"]
        assert len(multi) == 1, (
            f"Expected one multi_inventory residue row, got {len(multi)}"
        )
        assert "500" in multi[0]["fist_inventory_ids"]
        assert "501" in multi[0]["fist_inventory_ids"]
        assert not any(
            r["classmark"] == "tsx1.1" and r["ambiguity_kind"] == "no_fist_match"
            for r in residue
        )

    def test_no_fist_match_ambiguity_kind(self, empty_phase84_index):
        """HIGH #6 mirror: classmark with no FIST candidate -> ambiguity_kind='no_fist_match'."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed("")  # no FIST rows
        nli = _make_nli_seed(["tsf99.99"])
        qualifying, residue = _build_qualifying_inventories(fist, nli)
        assert not qualifying
        assert any(
            r["classmark"] == "tsf99.99" and r["ambiguity_kind"] == "no_fist_match"
            for r in residue
        )

    def test_parent_shadow_filter_applied(self, empty_phase84_index, monkeypatch):
        """D-06: shelfmark in parent-shadow set is excluded from synthetic emission."""
        from scripts import generate_synthetic_rows as gen

        monkeypatch.setattr(
            gen,
            "_load_parent_shelfmark_set",
            lambda path=None: {"T-S NS 161"},
        )
        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (400, 'T-S NS 161');
            """
        )
        nli = _make_nli_seed(["tsns161"])
        qualifying, residue = gen._build_qualifying_inventories(fist, nli)
        assert 400 not in qualifying
        assert any(r["ambiguity_kind"] == "parent_shadow" for r in residue)
        assert any(r.get("pattern_guess") for r in residue)

    def test_pattern_guess_categories(self, empty_phase84_index):
        """Residue pattern_guess column tags each unresolved classmark."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed("")  # no FIST rows -> every CUDL goes to residue
        nli = _make_nli_seed(
            [
                "tsf1.1100",
                "tsar3.50",
                "tsns192minutefragments",
                "or1080.110",
                "mosseriii117.1a",
                "tsmisc1.131.1",
            ]
        )
        _, residue = _build_qualifying_inventories(fist, nli)
        by_classmark = {r["classmark"]: r["pattern_guess"] for r in residue}
        assert by_classmark["tsf1.1100"] == "tsf_flattened_series"
        assert by_classmark["tsar3.50"] == "tsar_flattened_series"
        assert by_classmark["tsns192minutefragments"] == "tsns_minute_or_letter"
        assert by_classmark["or1080.110"] == "or_single_segment"
        assert by_classmark["mosseriii117.1a"] == "mosseri_exotic_letter"
        assert by_classmark["tsmisc1.131.1"] == "tsmisc_multi_segment"

    def test_nli_conn_required_raises(self):
        """Phase 86 D-01 CUDL-walk: nli_conn is mandatory."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed("")
        with pytest.raises(ValueError, match=r"(Phase 86|CUDL-walk|nli_conn)"):
            _build_qualifying_inventories(fist, None)

    def test_image_bearing_invariant_for_all_qualifying(self, empty_phase84_index):
        """Every emitted row has has_cudl_manifest=True (sanity sweep)."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (601, 'T-S Ar 1.1');
            INSERT INTO dbo_Inventory VALUES (602, 'T-S Ar 2.2');
            INSERT INTO dbo_Inventory VALUES (603, 'T-S Ar 3.3');
            """
        )
        nli = _make_nli_seed(["tsar1.1", "tsar2.2"])
        qualifying, _ = _build_qualifying_inventories(fist, nli)
        assert all(rec["has_cudl_manifest"] is True for rec in qualifying.values())
        assert 603 not in qualifying  # no CUDL manifest, no emission

    def test_idempotent_when_synthetic_block_present_in_csv_bank(self):
        """Pass 2 HIGH-1 + Pass 3 shared MEDIUM (Codex + Gemini): re-applying
        with a prior synthetic block in csv_bank must NOT silently drop the
        same qualifying classmark on the next run.

        CRITICAL: this test does NOT use the `empty_phase84_index` fixture
        (which monkeypatched lookup_cudl -> None and thereby bypassed the
        very integration this test claims to cover). Instead it builds the
        REAL `shared.shelfmark_bridge.build_alias_index` against TWO csv_bank
        variants — raw (with synthetic) and `_build_real_only_csv_bank(csv_bank)`
        (synthetic-stripped) — and asserts the contrasting `lookup_cudl`
        behaviour BEFORE running `_build_qualifying_inventories`.

        Without _build_real_only_csv_bank, lookup_cudl('tsns329.96') would
        return the prior synthetic sys_id (because csv_bank includes the
        synthetic block), making step-1 skip the classmark -> qualifying set
        is empty -> next --apply wipes the block.

        With the fix, the synthetic-stripped csv_bank produces an alias
        index that does NOT cover synthetic sys_ids, so lookup_cudl returns
        None for `tsns329.96` and the classmark re-qualifies cleanly.
        """
        from scripts.generate_synthetic_rows import (
            _build_qualifying_inventories,
            _build_real_only_csv_bank,
        )
        from shared.shelfmark_bridge import build_alias_index, lookup_cudl

        synthetic_sys_id = encode_inventory_sys_id(65549106)
        assert is_synthetic_sys_id(synthetic_sys_id)

        # csv_bank in the exact shape build_alias_index expects (matches
        # what genizah_core.csv_bank produces — sys_id keys + per-row dict
        # whose call_numbers_raw field is the pipe-split shelfmark variants
        # plus a single canonical shelfmark and library_code).
        csv_bank = {
            "12345": {
                "shelfmark": "T-S Real 1.1",
                "call_numbers_raw": ["T-S Real 1.1"],
                "library_code": "CUL",
            },
            synthetic_sys_id: {
                "shelfmark": "T-S NS 329.96",
                "call_numbers_raw": ["T-S NS 329.96"],
                "library_code": "CUL",
            },
        }

        # --- Part 1: assert _build_real_only_csv_bank actually strips. ---
        stripped = _build_real_only_csv_bank(csv_bank)
        assert synthetic_sys_id not in stripped, (
            "Pass 2 HIGH-1: synthetic sys_id must be stripped"
        )
        assert "12345" in stripped, (
            "Pass 2 HIGH-1: real sys_id must survive stripping"
        )
        assert synthetic_sys_id in csv_bank, (
            "Pass 2 HIGH-1: original csv_bank must remain untouched"
        )

        # --- Part 2: build the REAL alias index over BOTH variants and
        # assert the contrasting lookup_cudl behaviour. This is the
        # integration step the Pass 3 shared MEDIUM (Codex + Gemini) demanded:
        # it actually exercises shared.shelfmark_bridge.build_alias_index
        # with both inputs, instead of mocking lookup_cudl.

        # Variant A — raw csv_bank (includes the synthetic row).
        build_alias_index(csv_bank)
        raw_hit = lookup_cudl("tsns329.96")
        assert raw_hit is not None, (
            "Sanity: raw csv_bank including the synthetic row MUST resolve "
            "'tsns329.96' via lookup_cudl — without the fix, this would mask "
            "the classmark on re-apply."
        )

        # Variant B — _build_real_only_csv_bank stripped view.
        build_alias_index(_build_real_only_csv_bank(csv_bank))
        stripped_hit = lookup_cudl("tsns329.96")
        assert stripped_hit is None, (
            "Pass 3 shared MEDIUM: lookup_cudl built against "
            "_build_real_only_csv_bank(csv_bank) MUST return None for "
            "'tsns329.96' — otherwise the idempotency fix is not "
            "wired into the alias index and re-apply will silently drop "
            "the qualifying classmark."
        )

        # --- Part 3: end-to-end qualifying assertion (only meaningful
        # because Parts 1+2 proved the alias index is now synthetic-free).
        # The outer caller in scripts/generate_synthetic_rows.py runs
        # build_alias_index(_build_real_only_csv_bank(csv_bank)) BEFORE
        # invoking _build_qualifying_inventories — we replicate that
        # ordering here so the test exercises the actual production
        # call sequence.
        build_alias_index(_build_real_only_csv_bank(csv_bank))
        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (65549106, 'T-S NS 329.96');
            """
        )
        nli = _make_nli_seed(["tsns329.96"])
        qualifying, _ = _build_qualifying_inventories(fist, nli)
        assert 65549106 in qualifying, (
            "Pass 2 HIGH-1: re-apply MUST re-emit the same synthetic row "
            "for inventory 65549106 even when the prior synthetic block "
            "is present in csv_bank."
        )


class TestClassifyLibraryCode:
    """Codex MEDIUM — _classify_library_code Mosseri-prefix detection coverage."""

    def test_canonical_moss(self):
        from scripts.generate_synthetic_rows import _classify_library_code

        assert _classify_library_code("Moss. III,27.1") == "Mosseri"

    def test_mosseri_prefix_form(self):
        from scripts.generate_synthetic_rows import _classify_library_code

        assert _classify_library_code("Mosseri: Moss. IV,27.1") == "Mosseri", (
            "Codex MEDIUM: 'Mosseri:' prefix must classify as Mosseri, not CUL"
        )

    def test_post_colon_moss(self):
        from scripts.generate_synthetic_rows import _classify_library_code

        assert _classify_library_code("AIU: Moss. III,27.1") == "Mosseri"

    def test_ts_defaults_cul(self):
        from scripts.generate_synthetic_rows import _classify_library_code

        assert _classify_library_code("T-S NS 329.96") == "CUL"


class TestBuildRealOnlyCsvBank:
    """Pass 2 HIGH-1 — synthetic-stripped csv_bank view: idempotency primitive."""

    def test_strips_synthetic_sys_ids(self):
        from scripts.generate_synthetic_rows import _build_real_only_csv_bank

        synthetic = encode_inventory_sys_id(65549106)
        bank = {
            "12345": {"shelfmark": "T-S Real 1.1"},
            synthetic: {"shelfmark": "T-S NS 329.96"},
        }
        out = _build_real_only_csv_bank(bank)
        assert "12345" in out
        assert synthetic not in out

    def test_does_not_mutate_input(self):
        from scripts.generate_synthetic_rows import _build_real_only_csv_bank

        synthetic = encode_inventory_sys_id(65549106)
        bank = {synthetic: {"shelfmark": "T-S NS 329.96"}}
        _build_real_only_csv_bank(bank)
        assert synthetic in bank, "input csv_bank must remain untouched"
