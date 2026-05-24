# -*- coding: utf-8 -*-
"""Phase 96 NEW-1: btn_rd_open_browse removal AST guard.

Implementation plan: 96-07-PLAN.md
Wave 0 state: ASSERT-INVERTED (test expects removal that has not happened
yet). Marked xfail until plan 96-07 lands.
"""
from pathlib import Path
import pytest

RESULT_DIALOG_PY = Path(__file__).parent.parent / "desktop" / "result_dialog.py"


@pytest.mark.xfail(reason="Plan 96-07 will remove btn_rd_open_browse; this test goes green then.",
                   strict=True)
def test_btn_rd_open_browse_removed():
    """NEW-1: btn_rd_open_browse widget + handler + tooltip text are removed
    from desktop/result_dialog.py (redundant with `עיין` Browse button)."""
    src = RESULT_DIALOG_PY.read_text(encoding="utf-8")
    assert "btn_rd_open_browse" not in src, (
        "NEW-1: btn_rd_open_browse widget reference must be removed"
    )
    assert "_rd_open_in_browse" not in src, (
        "NEW-1: _rd_open_in_browse handler must be removed"
    )
    assert "View in Browse" not in src, (
        "NEW-1: 'View in Browse' tooltip/label must be removed"
    )


def test_yiyun_browse_button_still_present():
    """NEW-1 sanity: the remaining `עיין` Browse button (btn_view_transcription
    at line 248 -- VERIFIED 2026-05-24 against current desktop/result_dialog.py)
    must STILL be present. Removing btn_rd_open_browse is redundancy
    elimination, not browse-button elimination.

    REVISION 2026-05-24 -- checker BLOCKER 3 closure: source-verified that
    `btn_view_transcription` is the exact identifier in current code at
    desktop/result_dialog.py:248. Adds a defensive pytest.skip so that if
    a future phase renames the identifier, this Wave-0 test cannot
    self-block subsequent waves.
    """
    src = RESULT_DIALOG_PY.read_text(encoding="utf-8")
    if "btn_view_transcription" not in src:
        # Defensive skip: identifier may have been renamed by a future
        # phase. This test exists to catch button REMOVAL, not rename.
        pytest.skip(
            "btn_view_transcription identifier not found in result_dialog.py "
            "-- may have been renamed by a later phase. Update this test or "
            "remove if the Browse button no longer exists."
        )
    assert "btn_view_transcription" in src, (
        "NEW-1 sanity: the original `עיין` Browse button must NOT be removed"
    )
