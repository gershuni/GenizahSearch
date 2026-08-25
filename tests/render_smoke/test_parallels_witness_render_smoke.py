# -*- coding: utf-8 -*-
"""Render-smoke: /parallels builds, with the witness panel in the tree.

Headless AST/source-text tests (tests/test_parallels_multi_witness_page.py)
prove the DECISIONS are still encoded in the handlers. They cannot prove the
page renders -- and this page has a recorded history of exactly that failure:
`_apply_restored_search_config()` was once called above the widgets it sets,
which took the whole page down with a build-time NameError that no headless
test could see (owner-reported, 2026-08-23).

The witness feature adds three more chances to repeat it -- constants read
during the snapshot restore before the helpers exist, a panel whose lambdas
name functions defined 1,300 lines later, and a `set_options` call on the
sort control from inside a refresh -- so the cheap, decisive assertion is
"the page still builds, and the panel is in the tree".

Deliberately thin: no engine, no index, no search. `create_parallels_page` is
driven through NiceGUI's in-process User exactly like
tests/render_smoke/test_joins_lab_render_smoke.py, with the startup handlers
cleared so the real SearchEngine is never constructed.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx
from nicegui import core
from nicegui.context import context as _nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret

import web.main  # noqa: F401  -- registers /parallels on core.app at import
from web.state import state as _web_state
from web.translations import tr


@asynccontextmanager
async def _parallels_user(passage_ready: bool):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    saved_is_ready = _web_state.is_ready
    _web_state.is_ready = lambda: True
    try:
        prepare_simulation()
        set_storage_secret('render-smoke-test-secret', {})
        with patch('web.pages.parallels.passage_available',
                   return_value=passage_ready):
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(core.app),
                        base_url='http://test',
                    ) as client:
                        yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        _web_state.is_ready = saved_is_ready


def _run(driver, passage_ready: bool = True) -> None:
    saved_slot_stack = list(_nicegui_context.slot_stack)

    async def _go():
        async with _parallels_user(passage_ready) as user:
            await driver(user)

    try:
        asyncio.run(_go())
    finally:
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)


def _labels(user: User) -> set:
    from nicegui import ui
    out = set()
    for el in user.client.elements.values():
        text = getattr(el, 'text', None)
        if isinstance(text, str) and text:
            out.add(text)
        if isinstance(el, ui.select) and isinstance(el.options, dict):
            out.update(str(v) for v in el.options.values())
    return out


def test_parallels_page_builds_with_the_witness_panel():
    """The whole point: a build-time NameError here is invisible to every
    headless test and fatal to the page.

    EXACT label matches against `tr(...)`, never substrings. The page renders
    in the configured language, and a substring match on a Hebrew word finds
    it inside unrelated strings -- 'עדים' also appears in the search-depth
    tooltip and the auto-expand note, so a substring assertion stayed green
    with the panel heading deleted (verified by mutation).
    """
    async def driver(user: User) -> None:
        await user.open('/parallels')
        labels = _labels(user)
        missing = {tr('Witnesses'), tr('Add witness text')} - labels
        assert not missing, f'witness panel elements absent: {missing}'

    _run(driver)


def test_the_page_still_builds_when_passage_is_unavailable():
    """Flag off / index missing must hide the letter-level method cleanly,
    not take the page with it -- the panel's own visibility is driven from
    the same handler."""
    async def driver(user: User) -> None:
        await user.open('/parallels')
        # No assertion on the panel: with passage unavailable the method
        # selector hides and the panel goes with it. Reaching this line at
        # all is the assertion.
        assert user.client.elements

    _run(driver, passage_ready=False)


def test_the_sort_control_offers_no_fusion_orders_before_a_search():
    """"Sort by number of witnesses" on a single-text search would sort by a
    column that is 1 everywhere. The options appear only once there is fusion
    to order by.

    This is a real assertion only because the page sets the control's initial
    options through `_sync_sort_options()` at build time, from the same code
    path that later adds them. Left to the widget's own constructor, the two
    would be separate statements and this test could not tell them apart.
    """
    async def driver(user: User) -> None:
        from nicegui import ui
        await user.open('/parallels')
        selects = [el for el in user.client.elements.values()
                   if isinstance(el, ui.select)
                   and isinstance(el.options, dict)
                   and tr('Sort by score') in
                   [str(v) for v in el.options.values()]]
        assert selects, 'sort control not found'
        keys = set()
        for sel in selects:
            keys.update(sel.options.keys())
        assert keys == {'score', 'shelfmark', 'matches'}, (
            f'unexpected sort options before any search: {sorted(keys)}')

    _run(driver)


def _marked(user: User, marker: str):
    """The single element carrying `marker`, or an assertion failure."""
    hits = [el for el in user.client.elements.values()
            if marker in (getattr(el, '_markers', None) or [])]
    assert len(hits) == 1, f'expected one element marked {marker!r}, got {len(hits)}'
    return hits[0]


def test_letter_level_hides_the_paragraph_settings_and_lab_mode():
    """Both are chunk-only, and both were on screen under letter-level search
    (owner-reported 2026-08-25). The paragraph controls were already
    force-set and DISABLED -- that is what stops the UI ever sending a value
    `web/search_api.py` would reject with 400 `passage_option_unsupported` --
    but a greyed-out control still reads as an option you might have.

    Visibility is a runtime call inside a handler; only a real build executes
    it, so no AST test can see this.
    """
    async def driver(user: User) -> None:
        await user.open('/parallels')
        assert not _marked(user, 'boundary-settings').visible, (
            'paragraph-separator settings are showing under letter-level '
            'search, which has no paragraph boundaries'
        )
        assert not _marked(user, 'lab-mode-row').visible, (
            'Lab Mode is showing under letter-level search, which it is '
            'mutually exclusive with'
        )

    _run(driver)


def test_chunk_search_shows_the_paragraph_settings_and_lab_mode():
    """The other half. With no passage index the method pins to chunk, which
    is exactly the state those controls belong to -- hiding them there would
    remove real functionality rather than tidy an irrelevant one."""
    async def driver(user: User) -> None:
        await user.open('/parallels')
        assert _marked(user, 'boundary-settings').visible
        assert _marked(user, 'lab-mode-row').visible

    _run(driver, passage_ready=False)


def test_the_paragraph_controls_are_still_disabled_not_merely_hidden():
    """Hiding is presentation ON TOP of the guarantee, never instead of it: a
    restored snapshot or a stray programmatic write must still be unable to
    put an unsupported value on the wire."""
    from nicegui import ui

    async def driver(user: User) -> None:
        await user.open('/parallels')
        radios = [el for el in user.client.elements.values()
                  if isinstance(el, ui.radio)
                  and isinstance(el.options, dict)
                  and 'full' in el.options]
        assert radios, 'boundary_mode radio not found'
        assert radios[0].enabled is False, (
            'boundary_mode was hidden but left ENABLED -- the 400 guard is '
            'the contract, hiding is only presentation'
        )

    _run(driver)
