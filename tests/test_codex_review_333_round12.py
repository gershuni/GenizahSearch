"""Regressions for the two Codex findings in round 12 on PR #333 (2026-09-02).

P2 `desktop/widgets/__init__.py` — the folio anchor added in round 11 applied to
every source-list size. When BOTH sides expose whole-codex lists, switching at
folio 27, paging within the destination to folio 28 and switching back still found
the saved folio-27 entries and ignored the reader's current position. The anchor
exists only for the position-less two-image case.

P2 `shared/fgp_service.py` — the search-scoped override returned a phrase-matching
FGP row before any coverage check. For an HTR-backed hit the phrase is necessarily
in V0.8 as well, and even a tiny excerpt can contain the same common word, so
arriving from search promoted exactly the partial transcription that ordinary
browsing demotes.
"""
from __future__ import annotations

from desktop.widgets import map_matching_image_index
from shared.fgp_service import choose_default_source

# NLI per-part list: two entries, FL-only labels, no folio numbers.
NLI = [{"label": "FL168181477"}, {"label": "FL168181478"}]
# Oxford whole-codex list: 164 entries carrying folio numbers.
OX = [{"label": f"{f}{s}", "folio_num": f} for f in range(1, 83) for s in ("a", "b")]


class TestAnchorOnlyForThePositionlessPair:
    def test_the_two_entry_fl_only_case_still_uses_the_anchor(self):
        assert map_matching_image_index(NLI, 0, OX, anchor_folio=27) == 52   # 27a
        assert map_matching_image_index(NLI, 1, OX, anchor_folio=27) == 53   # 27b

    def test_a_list_with_folio_numbers_ignores_the_anchor(self):
        # Both sides whole-codex: the reader moved to folio 28 in the source list,
        # so the destination must follow them, not the folio-27 anchor.
        other = [{"label": f"{f}{s}", "folio_num": f} for f in range(1, 83) for s in ("a", "b")]
        idx = map_matching_image_index(other, 54, OX, anchor_folio=27)       # 28a
        assert idx not in (52, 53), f"the anchor must not drag the reader back (got {idx})"
        assert OX[idx]["folio_num"] == 28

    def test_a_longer_positionless_list_ignores_the_anchor(self):
        # Three FL-only entries is not the documented recto/verso pair; the
        # proportional mapping is the honest answer there.
        long_fl = [{"label": f"FL{i}"} for i in range(3)]
        idx = map_matching_image_index(long_fl, 2, OX, anchor_folio=27)
        assert idx not in (52, 53)

    def test_no_anchor_behaviour_is_unchanged(self):
        assert map_matching_image_index(OX, 53, NLI) == 1
        assert map_matching_image_index(OX, 52, NLI) == 0
        assert map_matching_image_index(NLI, 0, OX) == 0


# --------------------------------------------------------------------------
# The search-scoped override respects coverage
# --------------------------------------------------------------------------

FOLIO = (
    "תקום רבה דיניך וכהדרי אלצדקת עיניך ולבך מביט ועיניך וג במקום אחד "
    "אהובים היום נדמו כמלאכים בקומה זקופה כמעמד מל גנונים אגודים במ דוברים "
    "קדוש וברוך כמ הם לובשי לובן כמ ובמו אין אכילה ושתיה כמ זימון שינה מפרידים"
)
PHRASE = "תקום רבה דיניך"


def _fgp(content, source_id):
    return {
        "id": source_id, "content": content, "doc_relation": "Digital Edition",
        "source": "fgp", "is_fgp": True, "text_source": "FGP",
        "c_number": None, "image_side": None, "language": "Hebrew",
    }


class TestSearchMatchRespectsCoverage:
    def test_a_tiny_matching_excerpt_does_not_win_over_v08(self):
        sliver = _fgp(PHRASE, 1)          # contains the phrase, ~14 letters
        d = choose_default_source([sliver], FOLIO, full_htr_getter=lambda: FOLIO,
                                  must_contain=PHRASE)
        assert d["provider"] == "v08", (
            "a partial excerpt must not be promoted just because it contains the "
            f"searched phrase (reason={d['reason']}, id={(d.get('source') or {}).get('id')})"
        )
        assert d["reason"] == "must_contain_v08"
        assert d["must_contain_matched"] is True

    def test_a_comprehensive_matching_edition_still_wins(self):
        full = _fgp(FOLIO, 2)
        d = choose_default_source([full], FOLIO, full_htr_getter=lambda: FOLIO,
                                  must_contain=PHRASE)
        assert d["provider"] == "fgp"
        assert d["source"]["id"] == 2
        assert d["reason"] == "must_contain_match"
        assert d["must_contain_matched"] is True

    def test_the_better_of_two_matching_editions_is_chosen(self):
        d = choose_default_source([_fgp(PHRASE, 1), _fgp(FOLIO, 2)], FOLIO,
                                  full_htr_getter=lambda: FOLIO, must_contain=PHRASE)
        assert d["provider"] == "fgp"
        assert d["source"]["id"] == 2, "the sliver must be skipped, not returned"

    def test_pgp_still_wins_when_it_contains_the_phrase(self):
        pgp = {"id": 9, "content": FOLIO, "doc_relation": "Digital Edition",
               "language": "Hebrew"}
        d = choose_default_source([pgp, _fgp(FOLIO, 2)], FOLIO,
                                  full_htr_getter=lambda: FOLIO, must_contain=PHRASE)
        assert d["provider"] == "pgp"

    def test_a_phrase_in_nothing_falls_through_to_the_normal_order(self):
        d = choose_default_source([_fgp(FOLIO, 2)], FOLIO, full_htr_getter=lambda: FOLIO,
                                  must_contain="phrase that appears nowhere at all")
        assert d["must_contain_matched"] is False

    def test_no_phrase_behaves_exactly_as_before(self):
        d = choose_default_source([_fgp(FOLIO, 2)], FOLIO, full_htr_getter=lambda: FOLIO)
        assert d["must_contain_matched"] is False
        assert d["provider"] == "fgp"
