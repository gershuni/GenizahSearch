"""Regressions for the two Codex findings in round 11 on PR #333 (2026-09-02).

P2 `desktop/widgets/__init__.py` + `viewers.py` — round 10 correctly stopped
restoring a stale index after navigation, which exposed the mapper on the RETURN
path: NLI's per-part list carries FL-only labels, so leaving it has no folio or
side signal and index 0 mapped proportionally to Oxford image 0 instead of folio
27a. The destination's last folio is now an anchor; the current image only picks
recto/verso.

P2 `web/pages/search_results.py` — Advanced View's Next/Previous re-renders the
SAME result object, so `snippet` keeps describing the original hit page. Its
phrase was reused on every folio visited afterwards.
"""
from __future__ import annotations

from desktop.widgets import map_matching_image_index

OX = [{"label": f"{f}{s}", "folio_num": f} for f in range(1, 83) for s in ("a", "b")]  # 164
NLI = [{"label": "FL168181477"}, {"label": "FL168181478"}]      # FL-only labels


def _read(path):
    return open(path, encoding="utf-8").read()


class TestFolioAnchorOnTheReturnPath:
    def test_nli_recto_returns_to_the_anchor_folio_recto(self):
        # folio 27 -> indices 52 (27a) and 53 (27b)
        assert map_matching_image_index(NLI, 0, OX, anchor_folio=27) == 52

    def test_nli_verso_returns_to_the_anchor_folio_verso(self):
        assert map_matching_image_index(NLI, 1, OX, anchor_folio=27) == 53

    def test_without_the_anchor_it_is_the_old_proportional_guess(self):
        # documents exactly what the anchor is for
        assert map_matching_image_index(NLI, 0, OX) == 0
        assert map_matching_image_index(NLI, 1, OX) == len(OX) - 1

    def test_an_anchor_absent_from_the_list_falls_through(self):
        idx = map_matching_image_index(NLI, 0, OX, anchor_folio=999)
        assert 0 <= idx < len(OX)

    def test_the_outbound_direction_is_unchanged(self):
        assert map_matching_image_index(OX, 53, NLI) == 1     # 27b -> verso
        assert map_matching_image_index(OX, 52, NLI) == 0     # 27a -> recto

    def test_single_sided_anchor_folio(self):
        one_side = [{"label": "5a", "folio_num": 5}]
        assert map_matching_image_index(NLI, 1, one_side, anchor_folio=5) == 0

    def test_viewer_records_and_passes_the_anchor(self):
        src = _read("desktop/viewers.py")
        assert "def _remember_folio(self, source, image_list, idx):" in src
        assert "anchor_folio=anchor" in src
        assert src.count("self._remember_folio(") >= 2      # both switch paths
        assert "self._last_folio_by_source = {}" in src


class TestAdvancedHitScope:
    def test_helper_exists_and_both_call_sites_use_it(self):
        src = _read("web/pages/search_results.py")
        assert "def _hit_scope_phrase(snippet, adv_state, page):" in src
        # the definition line matches the same text, so count CALL sites only
        assert src.count("must_contain=_hit_scope_phrase(snippet, adv_state, page)") == 2
        assert "must_contain=_snippet_match_phrase(snippet)" not in src

    def test_scope_is_reset_for_each_result(self):
        src = _read("web/pages/search_results.py")
        assert src.count("adv_state.hit_scope = None") >= 3   # open, index, next/prev

    def test_state_declares_the_field(self):
        assert "self.hit_scope" in _read("web/pages/search_state.py")

    def test_phrase_applies_only_on_the_hit_page(self):
        src = _read("web/pages/search_results.py")
        i = src.index("def _hit_scope_phrase(")
        j = src.index("\ndef ", i + 1)
        ns = {"_snippet_match_phrase": lambda s: "תקום רבה דיניך" if s else ""}
        exec(src[i:j], ns)
        f = ns["_hit_scope_phrase"]

        class _Adv:
            volume_ie = "IE1"
            hit_scope = None

        class _Page:
            def __init__(self, sys_id, p_num):
                self.sys_id = sys_id
                self.p_num = p_num

        adv = _Adv()
        assert f("*hit*", adv, _Page("A", 2)) == "תקום רבה דיניך"     # the hit page
        assert adv.hit_scope == ("A", "IE1", 2)
        assert f("*hit*", adv, _Page("A", 3)) is None                  # Next
        assert f("*hit*", adv, _Page("A", 1)) is None                  # Previous
        assert f("*hit*", adv, _Page("A", 2)) == "תקום רבה דיניך"     # back on it

    def test_no_snippet_or_no_page_is_safe(self):
        src = _read("web/pages/search_results.py")
        i = src.index("def _hit_scope_phrase(")
        j = src.index("\ndef ", i + 1)
        ns = {"_snippet_match_phrase": lambda s: "" if not s else "phrase"}
        exec(src[i:j], ns)
        f = ns["_hit_scope_phrase"]

        class _Adv:
            volume_ie = None
            hit_scope = None

        class _Page:
            sys_id = "A"
            p_num = 1

        assert f("", _Adv(), _Page()) is None
        assert f("*hit*", _Adv(), None) is None
