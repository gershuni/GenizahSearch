# -*- coding: utf-8 -*-
"""Headless triage tests: sys_id triage keying (R-05).

No QApplication. No PyQt6 import. Tests the deliberate split between:
  - DEDUP key: (sys_id, page) — one entry per image (Candidate.key)
  - TRIAGE key: sys_id — one mark per physical fragment

This split means: if the same manuscript appears at page 3 (via the anchor
query) AND at page 4 (via the cross-side OR path), triage is shared — the
scholar triages the FRAGMENT, not a specific page image.
"""
from shared.joins_lab import normalize_candidate


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_result(sys_id: str, page: int, **extra) -> dict:
    """Build a minimal result dict matching the engine result-dict shape."""
    return {
        "display": {
            "id": sys_id,
            "shelfmark": extra.get("shelfmark", f"T-S 12.{sys_id[-3:]}"),
            "title": "",
            "library_code": "CUL",
            "img": page,
            "source": "FGP",
        },
        "uid": extra.get("uid", f"{sys_id}_FGP_P{page:03d}"),
        "full_text": extra.get("full_text", ""),
        "snippet": "",
        "highlight_pattern": None,
        "score": 1.0,
        "scope": "page",
    }


def triage_key(res: dict) -> str:
    """The triage key is sys_id (not (sys_id, page))."""
    return normalize_candidate(res).sys_id


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestTriageKeying:
    """Triage is keyed by sys_id, not (sys_id, page)."""

    def test_same_sys_id_different_pages_shares_triage(self):
        """R-05 core: marking a fragment Y via one page makes the OTHER page's lookup Y too.

        This proves triage is per-FRAGMENT (physical piece), not per-image.
        """
        SYS = "990001234560205171"
        res_page3 = _make_result(SYS, page=3)
        res_page4 = _make_result(SYS, page=4, uid=f"{SYS}_FGP_P004")

        triage: dict[str, str] = {}

        # Mark the fragment "Y" via page 3
        triage[triage_key(res_page3)] = "Y"

        # The same sys_id keyed from page 4 must also read "Y"
        assert triage.get(triage_key(res_page4)) == "Y", (
            "Triage keyed by (sys_id, page) would fail here — "
            "triage must be keyed by sys_id alone."
        )

    def test_dedup_key_differs_between_pages(self):
        """The dedup key (Candidate.key = (sys_id, page)) DIFFERS between pages
        while the triage key is identical — documenting the deliberate split.
        """
        SYS = "990001234560205171"
        res_page3 = _make_result(SYS, page=3)
        res_page4 = _make_result(SYS, page=4, uid=f"{SYS}_FGP_P004")

        cand3 = normalize_candidate(res_page3)
        cand4 = normalize_candidate(res_page4)

        # Dedup keys differ
        assert cand3.key != cand4.key, (
            "Dedup key (sys_id, page) must differ for the same manuscript at different pages"
        )
        # Triage keys are identical
        assert cand3.sys_id == cand4.sys_id, (
            "Triage key sys_id must be identical for the same physical fragment"
        )

    def test_clearing_triage_drops_all_lookups(self):
        """Re-anchor behavior: triage.clear() drops all triage state for the sys_id."""
        SYS = "990001234560205171"
        res_page3 = _make_result(SYS, page=3)
        res_page4 = _make_result(SYS, page=4, uid=f"{SYS}_FGP_P004")

        triage: dict[str, str] = {}
        triage[triage_key(res_page3)] = "Y"

        # Both pages see "Y" before clear
        assert triage.get(triage_key(res_page3)) == "Y"
        assert triage.get(triage_key(res_page4)) == "Y"

        # Re-anchor: clear the dict
        triage.clear()

        # Both lookups now return None
        assert triage.get(triage_key(res_page3)) is None
        assert triage.get(triage_key(res_page4)) is None

    def test_different_sys_ids_have_independent_triage(self):
        """Different sys_ids have independent triage entries."""
        SYS_A = "990001111110205171"
        SYS_B = "990002222220205171"

        res_a = _make_result(SYS_A, page=1)
        res_b = _make_result(SYS_B, page=1, uid=f"{SYS_B}_FGP_P001")

        triage: dict[str, str] = {}
        triage[triage_key(res_a)] = "Y"
        triage[triage_key(res_b)] = "N"

        assert triage.get(triage_key(res_a)) == "Y"
        assert triage.get(triage_key(res_b)) == "N"

    def test_candidate_key_property_is_sys_id_page_pair(self):
        """Verify Candidate.key is (sys_id, page) — the dedup key, NOT the triage key."""
        SYS = "990001234560205171"
        PAGE = 5
        res = _make_result(SYS, page=PAGE)
        cand = normalize_candidate(res)
        assert cand.key == (SYS, PAGE), (
            f"Candidate.key expected ({SYS!r}, {PAGE}), got {cand.key!r}"
        )

    def test_via_other_side_candidate_same_sys_id_shares_triage(self):
        """A cross-side OR candidate (via_other_side=True) at the neighbor page shares
        triage with the anchor-query hit at the candidate's own page."""
        SYS = "990001234560205171"
        # Regular hit at page 2
        res_own = _make_result(SYS, page=2)
        # Synthesized neighbor at page 3 (cross-side OR path)
        res_neighbor = _make_result(
            SYS,
            page=3,
            uid=f"{SYS}|3",
        )
        res_neighbor["_via_other_side"] = True

        triage: dict[str, str] = {}
        triage[triage_key(res_own)] = "?"

        # The neighbor lookup (page 3, different uid) must also see "?"
        assert triage.get(triage_key(res_neighbor)) == "?", (
            "Cross-side OR candidate at a neighbor page must share triage "
            "with the regular hit for the same sys_id"
        )
