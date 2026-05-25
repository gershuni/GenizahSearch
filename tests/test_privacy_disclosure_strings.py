# -*- coding: utf-8 -*-
"""Phase 97 D-NEW-6: Privacy disclosure strings for zstd cleartext cache.

Tests verify that bilingual EN+HE privacy disclosure for the local_index.sqlite3
zstd cache appears in:
- web/pages/help.py (EN and HE)
- genizah_app.py About dialog HTML (EN)
"""
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# Help page disclosure tests
# ---------------------------------------------------------------------------

def test_help_page_contains_local_cache_disclosure_en():
    """D-NEW-6: English Help page must mention local_index.sqlite3, zstd, and not encryption."""
    help_src = (REPO_ROOT / "web" / "pages" / "help.py").read_text(encoding="utf-8")
    assert "local_index.sqlite3" in help_src, (
        "EN Help page must mention 'local_index.sqlite3' (Phase 97 D-NEW-6 zstd cache disclosure)"
    )
    assert "zstd" in help_src, (
        "EN Help page must mention 'zstd' compression (Phase 97 D-NEW-6)"
    )
    # "not encryption" OR "not encrypted" OR "not encrypt"
    assert (
        "not encryption" in help_src
        or "not encrypted" in help_src
        or "not encrypt" in help_src
    ), (
        "EN Help page must clarify zstd is NOT encryption (Phase 97 D-NEW-6)"
    )


def test_help_page_contains_local_cache_disclosure_he():
    """D-NEW-6: Hebrew Help page must mention local_index.sqlite3, zstd, and אינה הצפנה / לא מוצפן."""
    help_src = (REPO_ROOT / "web" / "pages" / "help.py").read_text(encoding="utf-8")
    assert "local_index.sqlite3" in help_src, (
        "HE Help page must mention 'local_index.sqlite3' (appears in both EN and HE sections)"
    )
    assert "zstd" in help_src, (
        "HE Help page must mention 'zstd' (appears in both EN and HE sections)"
    )
    # Hebrew "not encryption" patterns
    assert (
        "לא מוצפן" in help_src
        or "אינה הצפנה" in help_src
        or "אינו מוצפן" in help_src
    ), (
        "HE Help page must include Hebrew 'not encryption' phrase (Phase 97 D-NEW-6): "
        "expected 'לא מוצפן' OR 'אינה הצפנה' OR 'אינו מוצפן'"
    )


# ---------------------------------------------------------------------------
# About dialog disclosure tests
# ---------------------------------------------------------------------------

def test_about_dialog_contains_local_cache_disclosure_en():
    """D-NEW-6: EN About dialog in genizah_app.py must mention zstd and 'never uploaded'."""
    app_src = (REPO_ROOT / "genizah_app.py").read_text(encoding="utf-8")
    assert "zstd" in app_src, (
        "About dialog (genizah_app.py) must mention 'zstd' (Phase 97 D-NEW-6)"
    )
    assert (
        "never uploaded" in app_src.lower()
        or "not uploaded" in app_src.lower()
        or "never upload" in app_src.lower()
    ), (
        "About dialog must clarify cache is 'never uploaded' (Phase 97 D-NEW-6)"
    )


def test_about_dialog_contains_local_cache_disclosure_he():
    """D-NEW-6: About dialog must contain Hebrew 'not encrypted' disclosure string."""
    app_src = (REPO_ROOT / "genizah_app.py").read_text(encoding="utf-8")
    assert (
        "לא מוצפן" in app_src
        or "אינו מועלה" in app_src
        or "אינו מוצפן" in app_src
    ), (
        "About dialog (genizah_app.py) must include Hebrew disclosure phrase "
        "(Phase 97 D-NEW-6): expected 'לא מוצפן' OR 'אינו מועלה'"
    )
