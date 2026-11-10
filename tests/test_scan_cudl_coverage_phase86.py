"""Phase 86 Plan 04: unit tests for scripts/scan_cudl_coverage_phase86.py.

Coverage:
  - Pass 2 HIGH-1: synthetic-resolving lookup_cudl hits are classified as
    phase86_synthetic, NOT phase84_hit
  - Pass 2 HIGH-3: alias-only-Alma tier is exactly 'phase86_existing_alma_candidate'
    (renamed; explicit framing)
  - Pass 2 HIGH-3: residue tier renamed from 'truly_orphan' to 'phase86_residue'
  - Pass 3 HIGH-1 (Codex): no-Alma bridge hits MUST consult synthetic_manifest.json:
      * manifest membership → phase86_synthetic
      * parent-shadow set    → phase86_excluded_parent_shadow
      * neither              → phase86_residue (safe fallback)
  - Tier completeness: distinct tier strings, no ad-hoc casings
"""
from __future__ import annotations
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class TestScanCudlCoverageClassification:
    @pytest.fixture(autouse=True)
    def _reset_scanner_caches(self, monkeypatch):
        """Pass 3 HIGH-1: per-test reset of the manifest-membership and
        parent-shadow caches the scanner now keeps as module state."""
        from scripts import scan_cudl_coverage_phase86 as scanner
        monkeypatch.setattr(scanner, "_SYNTHETIC_MANIFEST_INV_IDS", set())
        monkeypatch.setattr(scanner, "_PARENT_SHADOW_INV_IDS", set())
        # Also stub the loaders so accidental disk reads do not pollute the test.
        monkeypatch.setattr(
            scanner, "_load_synthetic_manifest_inventory_ids", lambda: set()
        )
        monkeypatch.setattr(
            scanner, "_load_parent_shadow_inv_ids", lambda: set()
        )

    def test_synthetic_resolving_lookup_cudl_classified_as_phase86_synthetic(
        self, monkeypatch
    ):
        """Pass 2 HIGH-1: synthetic sys_id returned by lookup_cudl MUST classify
        as phase86_synthetic, NOT phase84_hit."""
        from scripts import scan_cudl_coverage_phase86 as scanner
        monkeypatch.setattr(
            scanner, "lookup_cudl", lambda cm: {"sys_id": "990065549106000000"}
        )
        monkeypatch.setattr(
            scanner, "explain_fist_by_cudl", lambda cm: ("not_found", [])
        )
        tier, inv_id, notes = scanner.classify_classmark("tsns329.96")
        assert tier == scanner.TIER_SYNTHETIC, (
            f"Pass 2 HIGH-1: expected phase86_synthetic, got {tier!r}"
        )
        assert tier != scanner.TIER_PHASE84
        # Pass 3 LOW-86-04 (Gemini): inv_id is populated via decode_inventory_id.
        assert inv_id == "65549106"

    def test_pass3_high1_no_alma_in_manifest_classified_synthetic(self, monkeypatch):
        """Pass 3 HIGH-1 (Codex): a no-Alma single bridge hit whose
        inventory_id IS in synthetic_manifest.json must classify as
        phase86_synthetic."""
        from scripts import scan_cudl_coverage_phase86 as scanner

        class _Rec:
            inventory_id = 65549106
            has_alma = False
        monkeypatch.setattr(scanner, "lookup_cudl", lambda cm: None)
        monkeypatch.setattr(
            scanner, "explain_fist_by_cudl", lambda cm: ("single", [_Rec()])
        )
        monkeypatch.setattr(scanner, "_SYNTHETIC_MANIFEST_INV_IDS", {65549106})
        monkeypatch.setattr(scanner, "_PARENT_SHADOW_INV_IDS", set())

        tier, inv_id, notes = scanner.classify_classmark("tsns329.96")
        assert tier == scanner.TIER_SYNTHETIC
        assert inv_id == "65549106"
        assert "manifest membership confirmed" in notes

    def test_pass3_high1_no_alma_parent_shadow_routes_to_excluded(self, monkeypatch):
        """Pass 3 HIGH-1 (Codex): a no-Alma single bridge hit whose
        inventory_id was EXCLUDED by D-06 parent-shadow MUST classify as
        phase86_excluded_parent_shadow, NOT phase86_synthetic."""
        from scripts import scan_cudl_coverage_phase86 as scanner

        class _Rec:
            inventory_id = 70000001
            has_alma = False
        monkeypatch.setattr(scanner, "lookup_cudl", lambda cm: None)
        monkeypatch.setattr(
            scanner, "explain_fist_by_cudl", lambda cm: ("single", [_Rec()])
        )
        monkeypatch.setattr(scanner, "_SYNTHETIC_MANIFEST_INV_IDS", set())
        monkeypatch.setattr(scanner, "_PARENT_SHADOW_INV_IDS", {70000001})

        tier, inv_id, notes = scanner.classify_classmark("tsns161")
        assert tier == "phase86_excluded_parent_shadow", (
            f"Pass 3 HIGH-1: parent-shadow excluded inventory must NOT "
            f"classify as phase86_synthetic; got {tier!r}"
        )
        assert tier == scanner.TIER_EXCLUDED_PARENT_SHADOW
        assert tier != scanner.TIER_SYNTHETIC
        assert "parent-shadow" in notes.lower() or "D-06" in notes

    def test_pass3_high1_no_alma_not_in_manifest_falls_through_to_residue(
        self, monkeypatch
    ):
        """Pass 3 HIGH-1 (Codex): no-Alma bridge hit that is NEITHER in the
        manifest NOR in the parent-shadow set (e.g. CSV-injection rejection)
        falls through to phase86_residue rather than being misreported as
        phase86_synthetic."""
        from scripts import scan_cudl_coverage_phase86 as scanner

        class _Rec:
            inventory_id = 88888888
            has_alma = False
        monkeypatch.setattr(scanner, "lookup_cudl", lambda cm: None)
        monkeypatch.setattr(
            scanner, "explain_fist_by_cudl", lambda cm: ("single", [_Rec()])
        )
        monkeypatch.setattr(scanner, "_SYNTHETIC_MANIFEST_INV_IDS", set())
        monkeypatch.setattr(scanner, "_PARENT_SHADOW_INV_IDS", set())

        tier, inv_id, notes = scanner.classify_classmark("tsmisc99.99")
        assert tier == "phase86_residue", (
            f"Pass 3 HIGH-1: no-manifest no-parent-shadow no-Alma hit must "
            f"route to phase86_residue, not phase86_synthetic; got {tier!r}"
        )
        assert tier != scanner.TIER_SYNTHETIC
        assert "NOT present in synthetic_manifest" in notes or "no-emit" in notes

    def test_real_resolving_lookup_cudl_classified_as_phase84_hit(self, monkeypatch):
        """Real (non-synthetic) sys_id MUST stay phase84_hit."""
        from scripts import scan_cudl_coverage_phase86 as scanner
        monkeypatch.setattr(
            scanner, "lookup_cudl", lambda cm: {"sys_id": "990012345678"}
        )
        monkeypatch.setattr(
            scanner, "explain_fist_by_cudl", lambda cm: ("not_found", [])
        )
        tier, inv_id, notes = scanner.classify_classmark("ts10j1.1")
        assert tier == scanner.TIER_PHASE84

    def test_existing_alma_candidate_renamed_tier(self, monkeypatch):
        """Pass 2 HIGH-3: alias-only-Alma tier MUST be exactly
        'phase86_existing_alma_candidate' (renamed from anything implying
        'coverage achieved')."""
        from scripts import scan_cudl_coverage_phase86 as scanner

        class _Rec:
            inventory_id = 700
            has_alma = True
        monkeypatch.setattr(scanner, "lookup_cudl", lambda cm: None)
        monkeypatch.setattr(
            scanner, "explain_fist_by_cudl", lambda cm: ("single", [_Rec()])
        )
        tier, inv_id, notes = scanner.classify_classmark("ts10j1.1")
        assert tier == "phase86_existing_alma_candidate", (
            f"Pass 2 HIGH-3: expected renamed tier, got {tier!r}"
        )
        assert tier == scanner.TIER_EXISTING_ALMA_CANDIDATE
        assert (
            "Documented candidate" in notes or "NOT counted" in notes
        ), f"tier note must convey non-resolution framing; got: {notes!r}"

    def test_residue_tier_renamed_not_truly_orphan(self, monkeypatch):
        """Pass 2 HIGH-3: residue tier renamed from 'truly_orphan' to 'phase86_residue'."""
        from scripts import scan_cudl_coverage_phase86 as scanner
        monkeypatch.setattr(scanner, "lookup_cudl", lambda cm: None)
        monkeypatch.setattr(
            scanner, "explain_fist_by_cudl", lambda cm: ("not_found", [])
        )
        tier, _, _ = scanner.classify_classmark("tsf99.99")
        assert tier == "phase86_residue"
        assert tier != "truly_orphan"

    def test_multi_inventory_tier_unchanged(self, monkeypatch):
        from scripts import scan_cudl_coverage_phase86 as scanner

        class _R1:
            inventory_id = 10
            has_alma = False

        class _R2:
            inventory_id = 11
            has_alma = False
        monkeypatch.setattr(scanner, "lookup_cudl", lambda cm: None)
        monkeypatch.setattr(
            scanner,
            "explain_fist_by_cudl",
            lambda cm: ("multi_inventory_ambiguous", [_R1(), _R2()]),
        )
        tier, inv_id, _ = scanner.classify_classmark("ts12.345")
        assert tier == "multi_inventory_ambiguous"
        assert "10" in inv_id and "11" in inv_id
