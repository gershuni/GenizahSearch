# -*- coding: utf-8 -*-
"""Work a page hands to a background task still runs inside that page.

NiceGUI keeps the current page per asyncio task, and a task started with
``asyncio.create_task`` / ``ensure_future`` starts with none: ``ui.notify``,
``ui.run_javascript`` and ``ui.context.client`` raise there, and every site
below swallowed the error, so the work silently did not happen. Found by the
2026-10-20 sweep of every task-spawn site under ``web/`` (after the
/catalog-browse first load, ``tests/render_smoke/
test_catalog_browse_initial_load_render_smoke.py``).
"""

from __future__ import annotations

import asyncio
import itertools

from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation

_paths = itertools.count()


def _run_in_page(build, settle: float = 0.4) -> None:
    """Build UI in a fresh page, then give its background tasks ``settle`` s."""
    prepare_simulation()

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_deferred_task_probe_{next(_paths)}')) as client:
            build(client)
        await asyncio.sleep(settle)

    asyncio.run(_run())


def test_the_version_selector_default_reaches_its_page():
    """The default source (here a PGP edition) is applied from a task; the
    /browse callback updates the citation chip, which needs the page. It got
    none, so the chip kept the citation of the text that was replaced."""
    from web.components.version_selector import create_version_selector

    seen = []

    def on_version_change(text, info):
        seen.append((text, info['source'], ui.context.client.id))

    pages = []

    def build(client):
        pages.append(client.id)
        create_version_selector(
            '990000000000017', 1, 'htr text',
            on_version_change=on_version_change,
            pgp_transcription={'content': 'pgp edition text', 'attribution': 'A. Scholar'},
        )

    _run_in_page(build)
    assert seen == [('pgp edition text', 'pgp', pages[0])]


def test_the_editor_keeps_auto_saving_after_the_first_save(monkeypatch):
    """The "Auto-saved" notice raised in the loop's task and ended the loop:
    the first draft was saved, later edits were not."""
    import web.components.text_editor as text_editor

    saves = []
    notices = []
    monkeypatch.setattr(text_editor, 'AUTO_SAVE_INTERVAL', 0.05)
    monkeypatch.setattr(text_editor, 'save_local_edit', lambda *args, **kwargs: saves.append(args[2]))
    real_notify = ui.notify
    monkeypatch.setattr(ui, 'notify', lambda message, **kwargs: (notices.append(message),
                                                                 real_notify(message, **kwargs)))

    def build(client):
        text_editor.create_edit_text_dialog('990000000000017', 1, 'original text')
        # The editable textarea is the one that counts changes on input (the
        # other one showing the same text is the read-only original).
        edited, on_input = next(
            (e, listener.handler)
            for e in client.elements.values() if isinstance(e, ui.textarea)
            for listener in e._event_listeners.values() if listener.type == 'input'
        )
        edited.value = 'edited text'
        on_input(None) if on_input.__code__.co_argcount else on_input()

    _run_in_page(build, settle=0.5)
    assert len(saves) >= 2 and set(saves) == {'edited text'}, saves
    assert len(notices) >= 2, notices
