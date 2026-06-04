# -*- coding: utf-8 -*-
"""Tier-1 unit tests for desktop/join_workbench.py pure helpers.

These tests are import-safe without a QApplication and run headlessly in CI.
Pattern source: tests/test_joins_lab.py (same domain, pure-function + class-per-domain structure).
"""

from desktop.join_workbench import (
    iiif_full,
    meta_brief,
    badge_for_source,
    dedup_join_rows,
    r_sid,
    r_shelf,
    r_title,
    r_text,
    r_lib,
)


# ── Module-level helpers ──────────────────────────────────────────────────────


def _make_result(sys_id: str, page: int = 1, **extra) -> dict:
    """Build a realistic result dict matching the verified engine result-dict shape."""
    d = {
        "display": {
            "id": extra.pop("id", sys_id),
            "shelfmark": extra.pop("shelfmark", f"T-S 12.{sys_id[-3:]}"),
            "title": extra.pop("title", ""),
            "library_code": extra.pop("library_code", "CUL"),
            "img": page,
        },
        "uid": extra.pop("uid", f"{sys_id}_FGP_P{page:03d}"),
        "full_text": extra.pop("full_text", ""),
        "sys_id": sys_id,
    }
    d.update(extra)
    return d


# ── TestIiifFull ─────────────────────────────────────────────────────────────


class TestIiifFull:
    """Unit tests for iiif_full() — IIIF full-resolution URL builder (D-05)."""

    def test_iiif_full(self):
        """NLI base URL → appends /full/2000,/0/default.jpg."""
        url = "https://www.nli.org.il/en/image/NNL_ALEPH001234567/FL12345678"
        assert iiif_full(url) == url + "/full/2000,/0/default.jpg"

    def test_nli_base_url_appends_path(self):
        url = "https://nli.org.il/FL9999"
        assert iiif_full(url) == url + "/full/2000,/0/default.jpg"

    def test_direct_jpg_returned_unchanged(self):
        """Direct .jpg URLs are returned as-is (Oxford, JTS, Cambridge pattern)."""
        url = "https://cudl.lib.cam.ac.uk/img/1.jpg"
        assert iiif_full(url) == url

    def test_another_direct_jpg(self):
        assert iiif_full("https://example.com/fragment/view.jpg") == "https://example.com/fragment/view.jpg"

    def test_empty_returns_empty(self):
        """Empty string returns empty string."""
        assert iiif_full("") == ""

    def test_none_returns_empty(self):
        """None returns empty string."""
        assert iiif_full(None) == ""

    def test_custom_width(self):
        """Custom width parameter is used in the path."""
        url = "https://nli.org.il/FL9999"
        assert iiif_full(url, width=400) == url + "/full/400,/0/default.jpg"

    def test_custom_width_2(self):
        url = "https://nli.org.il/FL1234"
        assert iiif_full(url, width=800) == url + "/full/800,/0/default.jpg"


# ── TestBadgeForSource ───────────────────────────────────────────────────────


class TestBadgeForSource:
    """Unit tests for badge_for_source() — source-provenance badge (D-09).

    Labels for 'user' and 'community' are resolved via tr() at CALL TIME
    (must-fix #9) — the test asserts the call-time result, not a frozen value.
    """

    def test_source_badge_mapping(self):
        """Covers all four known sources and the fallback."""
        # PGP light mode
        label, color = badge_for_source("PGP", is_dark=False)
        assert label == "PGP"
        assert color == "#0ea5e9"

        # FJMS dark mode
        label, color = badge_for_source("FJMS", is_dark=True)
        assert label == "FJMS"
        assert color == "#a78bfa"

        # user light mode
        label, color = badge_for_source("user", is_dark=False)
        assert color == "#10b981"

        # community light mode
        label, color = badge_for_source("community", is_dark=False)
        assert color == "#10b981"

        # unknown source → generic fallback
        label, color = badge_for_source("mystery", is_dark=False)
        assert "join" in label.lower() or "צירוף" in label
        assert color == "#6b7280"

    def test_pgp_light(self):
        label, color = badge_for_source("PGP", is_dark=False)
        assert label == "PGP"
        assert color == "#0ea5e9"

    def test_pgp_dark(self):
        label, color = badge_for_source("PGP", is_dark=True)
        assert label == "PGP"
        assert color == "#38bdf8"

    def test_fjms_light(self):
        label, color = badge_for_source("FJMS", is_dark=False)
        assert label == "FJMS"
        assert color == "#8b5cf6"

    def test_fjms_dark(self):
        label, color = badge_for_source("FJMS", is_dark=True)
        assert label == "FJMS"
        assert color == "#a78bfa"

    def test_user_light(self):
        _label, color = badge_for_source("user", is_dark=False)
        assert color == "#10b981"

    def test_user_dark(self):
        _label, color = badge_for_source("user", is_dark=True)
        assert color == "#34d399"

    def test_community_light(self):
        _label, color = badge_for_source("community", is_dark=False)
        assert color == "#10b981"

    def test_community_dark(self):
        _label, color = badge_for_source("community", is_dark=True)
        assert color == "#34d399"

    def test_unknown_source_gets_fallback_color(self):
        _label, color = badge_for_source("mystery", is_dark=False)
        assert color == "#6b7280"

    def test_unknown_source_dark_fallback(self):
        _label, color = badge_for_source("mystery", is_dark=True)
        assert color == "#9ca3af"

    def test_unknown_source_label_generic(self):
        label, _color = badge_for_source("mystery", is_dark=False)
        assert "join" in label.lower() or "צירוף" in label

    def test_none_source_gets_fallback(self):
        """None source key should gracefully fall back to the generic badge."""
        label, color = badge_for_source(None, is_dark=False)
        assert color == "#6b7280"
        assert "join" in label.lower() or "צירוף" in label

    def test_empty_source_gets_fallback(self):
        """Empty-string source key should gracefully fall back to the generic badge."""
        label, color = badge_for_source("", is_dark=False)
        assert color == "#6b7280"


# ── TestDedupJoinRows ────────────────────────────────────────────────────────


class TestDedupJoinRows:
    """Unit tests for dedup_join_rows() — order-insensitive (a,b) pair dedup."""

    def test_dedup_pairs(self):
        """Identical pair supplied in reverse order → deduped to 1 entry."""
        j1 = {"fragment_a": "T-S 12.1", "fragment_b": "T-S 12.2", "source": "PGP"}
        j2 = {"fragment_a": "T-S 12.2", "fragment_b": "T-S 12.1", "source": "FJMS"}
        result = dedup_join_rows([[j1], [j2]])
        assert len(result) == 1

    def test_identical_pair_reversed_first_source_wins(self):
        """When a pair appears in both lists, the first-list entry is kept (PGP priority)."""
        j1 = {"fragment_a": "T-S 12.1", "fragment_b": "T-S 12.2", "source": "PGP"}
        j2 = {"fragment_a": "T-S 12.2", "fragment_b": "T-S 12.1", "source": "FJMS"}
        result = dedup_join_rows([[j1], [j2]])
        assert result[0]["source"] == "PGP"

    def test_distinct_pairs_kept(self):
        """Two distinct pairs → both kept."""
        j1 = {"fragment_a": "T-S 12.1", "fragment_b": "T-S 12.2", "source": "PGP"}
        j2 = {"fragment_a": "T-S 12.1", "fragment_b": "T-S 12.3", "source": "FJMS"}
        result = dedup_join_rows([[j1], [j2]])
        assert len(result) == 2

    def test_empty_lists(self):
        """Empty input → empty result."""
        assert dedup_join_rows([]) == []
        assert dedup_join_rows([[]]) == []
        assert dedup_join_rows([[], []]) == []

    def test_single_list_single_item(self):
        j = {"fragment_a": "T-S 12.1", "fragment_b": "T-S 12.2", "source": "PGP"}
        result = dedup_join_rows([[j]])
        assert len(result) == 1
        assert result[0] is j

    def test_case_insensitive_dedup(self):
        """Pair keys are uppercased before comparison."""
        j1 = {"fragment_a": "t-s 12.1", "fragment_b": "T-S 12.2", "source": "PGP"}
        j2 = {"fragment_a": "T-S 12.1", "fragment_b": "t-s 12.2", "source": "FJMS"}
        result = dedup_join_rows([[j1], [j2]])
        assert len(result) == 1

    def test_three_sources_dedup(self):
        """Same pair from three sources → 1 result (first source wins)."""
        j1 = {"fragment_a": "A", "fragment_b": "B", "source": "PGP"}
        j2 = {"fragment_a": "B", "fragment_b": "A", "source": "FJMS"}
        j3 = {"fragment_a": "A", "fragment_b": "B", "source": "user"}
        result = dedup_join_rows([[j1], [j2], [j3]])
        assert len(result) == 1
        assert result[0]["source"] == "PGP"


# ── TestResultAccessors ──────────────────────────────────────────────────────


class TestResultAccessors:
    """Unit tests for r_sid/r_shelf/r_title/r_text/r_lib accessors."""

    def test_r_sid_display_id(self):
        res = _make_result("990001")
        assert r_sid(res) == "990001"

    def test_r_sid_sys_id_fallback(self):
        res = {"sys_id": "990002"}
        assert r_sid(res) == "990002"

    def test_r_sid_empty(self):
        assert r_sid({}) == ""

    def test_r_shelf_display_shelfmark(self):
        res = _make_result("990001", shelfmark="T-S 12.1")
        assert r_shelf(res) == "T-S 12.1"

    def test_r_shelf_top_level_fallback(self):
        res = {"shelfmark": "T-S 12.2"}
        assert r_shelf(res) == "T-S 12.2"

    def test_r_shelf_uid_fallback(self):
        res = {"uid": "990001_FGP_P001"}
        assert r_shelf(res) == "990001_FGP_P001"

    def test_r_shelf_question_mark_last_resort(self):
        assert r_shelf({}) == "?"

    def test_r_title_display(self):
        res = _make_result("990001", title="Some Title")
        assert r_title(res) == "Some Title"

    def test_r_title_empty(self):
        assert r_title({}) == ""

    def test_r_text_full_text(self):
        res = {"full_text": "some text"}
        assert r_text(res) == "some text"

    def test_r_text_text_fallback(self):
        res = {"text": "fallback"}
        assert r_text(res) == "fallback"

    def test_r_text_empty(self):
        assert r_text({}) == ""

    def test_r_lib_display_library_code(self):
        res = _make_result("990001", library_code="CUL")
        assert r_lib(res) == "CUL"

    def test_r_lib_display_library_fallback(self):
        res = {"display": {"library": "JTS"}}
        assert r_lib(res) == "JTS"

    def test_r_lib_empty(self):
        assert r_lib({}) == ""


# ── TestMetaBrief ────────────────────────────────────────────────────────────


class TestMetaBrief:
    """Unit tests for meta_brief() — one-line anchor summary."""

    def test_meta_brief(self):
        """meta dict with library_code + 2 images + title → output contains all three."""
        meta = {
            "library_code": "CUL",
            "images_nli": [{}, {}],
            "title": "Some title",
        }
        result = meta_brief(meta)
        assert "CUL" in result
        assert "2" in result
        assert "Some title" in result

    def test_meta_brief_no_images(self):
        """No images → image count part is omitted."""
        meta = {"library_code": "JTS", "images_nli": [], "title": "T"}
        result = meta_brief(meta)
        assert "JTS" in result
        assert "0" not in result  # "0 img" should be suppressed

    def test_meta_brief_images_ext_fallback(self):
        """images_ext used when images_nli is absent."""
        meta = {"library_code": "Oxford", "images_ext": [{}], "title": "X"}
        result = meta_brief(meta)
        assert "Oxford" in result
        assert "1" in result

    def test_meta_brief_title_truncated(self):
        """Title longer than 60 chars is truncated."""
        long_title = "A" * 100
        meta = {"library_code": "BL", "images_nli": [], "title": long_title}
        result = meta_brief(meta)
        assert "A" * 60 in result
        assert "A" * 61 not in result

    def test_meta_brief_empty(self):
        """Empty meta → empty string (no separators)."""
        result = meta_brief({})
        assert result == ""
