"""Regression guard for anonymous /browse recent-item persistence."""

from pathlib import Path


def test_anonymous_browse_recent_tracking_is_auth_gated():
    source = (Path(__file__).parents[1] / "web" / "pages" / "browse.py").read_text(
        encoding="utf-8"
    )

    assert (
        "if state.sys_id and service.is_ready and GlobalAuthState.is_logged_in():"
        in source
    )
