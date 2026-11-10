"""Regression: the /joins-lab page must CONSTRUCT without raising.

Phase 118 hotfix. `create_joins_lab_page()` called a non-existent `.add()` method
on NiceGUI `Column` elements at two sites:

    builder_area.add(anchor_builder['container'])          # joins_lab.py:378 (old)
    other_side_controls.add(other_builder['container'])    # joins_lab.py:474 (old)

`create_joins_builder()` already mounts its container in the active `with` context,
so these calls were both redundant AND invalid (`Column` has no `.add()`), 500-ing
the entire page on initial render with::

    AttributeError: 'Column' object has no attribute 'add'

The other Phase-118 tests only exercised pure helpers and mocked the `ui.*`
factories — a ``MagicMock`` silently accepts ``.add()``, so the crash slipped past
unit tests, the code review, AND the verifier (none rendered the real page).

This test renders the REAL page into NiceGUI's live auto-index slot — no mocks — so
any element-construction / API-misuse on the cold-start path (which builds BOTH the
anchor builder and the other-side builder) fails loudly here instead of in prod.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _storage_secret():
    """app.storage.* needs a secret to back session storage during construction."""
    from nicegui import app
    try:
        app.storage.secret = 'test-secret'  # noqa: S105 (test-only)
    except Exception:
        pass
    yield


def test_cold_start_page_constructs_without_error():
    """Bare /joins-lab (no anchor) must build its full static UI without raising.

    This is the exact path that 500-ed: it constructs `builder_area` + the anchor
    `create_joins_builder()` AND the Advanced-options `other_side_controls` +
    other-side `create_joins_builder()`. A regression that reintroduces a bad
    container mount (`.add()` or similar) re-raises here.
    """
    from nicegui import context, ui
    from web.pages.joins_lab import create_joins_lab_page

    # Render inside a throwaway container so repeated/parallel test runs do not
    # pile elements onto the shared auto-index client.
    assert context.slot_stack, 'expected an active NiceGUI slot (auto-index client)'
    with ui.column():
        create_joins_lab_page()  # must not raise AttributeError
