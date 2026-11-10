"""Oxford Part badge label: never a bare "[part]" / "[part ]".

2026-09-02 (debug/oxford-fgp-image-mismatch, owner UAT): MS heb. g.2/27's
``oxford_part_id`` is the whole codex "MS. Heb. g. 2" (no "/N"), so the web
header rendered "[part ]" and the desktop title "[part]". The label now falls
back to the record's own folio ("fol. 27") and is empty when nothing is
parseable; the display-name fallback no longer appends " part".
"""
from __future__ import annotations

from shared.codicological import CodicologicalManager


def _mgr():
    m = CodicologicalManager.__new__(CodicologicalManager)
    m.part_metadata = {
        "MS. Heb. d. 29/2": {},
        "MS. Heb. d. 25/C": {},
        "MS. Heb. g. 2": {},
    }
    return m


class TestGetPartLabel:
    def test_numbered_part_keeps_part_n(self):
        assert _mgr().get_part_label("MS. Heb. d. 29/2") == "part 2"
        assert _mgr().get_part_label("MS. Heb. d. 29/2", "MS heb. d.29/2") == "part 2"

    def test_letter_part(self):
        assert _mgr().get_part_label("MS. Heb. d. 25/C") == "part C"

    def test_whole_codex_uses_record_folio(self):
        # The real MS heb. g.2/27 case: images and FGP rows are positioned by folio 27.
        assert _mgr().get_part_label("MS. Heb. g. 2", "MS heb. g.2/27") == "fol. 27"

    def test_whole_codex_without_shelfmark_is_empty_not_bare_part(self):
        assert _mgr().get_part_label("MS. Heb. g. 2") == ""
        assert _mgr().get_part_label("MS. Heb. g. 2", None) == ""
        assert _mgr().get_part_label("MS. Heb. g. 2", "MS heb. g. 2") == ""

    def test_empty_part_id(self):
        assert _mgr().get_part_label("") == ""
        assert _mgr().get_part_label(None, "MS heb. g.2/27") == ""


class TestGetPartDisplayName:
    def test_numbered_part_display(self):
        assert _mgr().get_part_display_name("MS. Heb. d. 29/2") == "heb. d. 29 part 2"

    def test_whole_codex_display_has_no_dangling_part_word(self):
        name = _mgr().get_part_display_name("MS. Heb. g. 2")
        assert name == "MS. Heb. g. 2"
        assert not name.endswith("part")

    def test_unknown_part_id_passthrough(self):
        assert _mgr().get_part_display_name("MS. Heb. z. 99") == "MS. Heb. z. 99"


def test_web_browse_header_uses_shared_label_not_split_on_part():
    # The web header must not re-derive the badge by splitting the display
    # name on the word "part" (that produced "[part ]").
    src = open("web/pages/browse.py", encoding="utf-8").read()
    assert 'part_suffix.split("part")' not in src
    assert "oxford_part_label" in src
