# -*- coding: utf-8 -*-
"""The web "?" beside a Genuzot transcription (GS-274 option A).

The version selector reports whatever it puts on screen through
``on_version_change``; the "?" follows that, so it shows exactly when the text
on screen is a Genuzot transcription.
"""

from __future__ import annotations

import asyncio
import itertools

from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation

_paths = itertools.count()

TEXT = "ברוך אתה ה' אלהינו מלך העולם אשר קדשנו במצותיו וצונו <רא>\n?ב?רוך שאמר והיה העולם"


def _fgp(uid, credit_en, credit_he, content=TEXT):
    return {"source": "fgp", "is_fgp": True, "id": uid, "uid": f"fgp:{uid}",
            "doc_relation": "Digital Edition", "language": "Hebrew", "content": content,
            "source_credit_en": credit_en, "source_credit_he": credit_he}


GENUZOT = _fgp(1, "FGP Transcriptions Team - Genuzos", "צוות FGP להעתקות -גנוזות")
OTHER = _fgp(2, "Haim Milikowsky", "חיים מיליקובסקי, ראש צוות FGP למדרשי אגדה")


def _legend_buttons(sources, original_text, must_contain=None):
    """Build the selector in a page; return (sources shown, legend buttons' visibility)."""
    from web.components.version_selector import create_version_selector

    prepare_simulation()
    shown, buttons = [], []

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_marks_legend_probe_{next(_paths)}')) as client:
            create_version_selector(
                '990000000000017', 1, original_text,
                on_version_change=lambda text, info: shown.append(info.get('uid') or info['source']),
                all_sources=sources,
                must_contain=must_contain,
            )
        await asyncio.sleep(0.4)
        buttons.extend(e.visible for e in client.elements.values()
                       if isinstance(e, ui.button) and e.props.get('icon') == 'help_outline')

    asyncio.run(_run())
    return shown, buttons


def test_a_genuzot_transcription_on_screen_shows_the_legend_button():
    shown, buttons = _legend_buttons([GENUZOT], TEXT)
    assert shown == ['fgp:1']
    assert buttons == [True]


def test_another_teams_transcription_has_no_legend_button():
    shown, buttons = _legend_buttons([OTHER], TEXT)
    assert shown == ['fgp:2']
    assert buttons == []


def test_the_button_hides_when_the_automatic_transcription_is_reported():
    """A search phrase only V0.8 contains puts V0.8 on screen (SEED-033 Option A),
    reported through `on_version_change` like any other choice."""
    shown, buttons = _legend_buttons([GENUZOT], TEXT + "\nמשפט ייחודי בלבד",
                                     must_contain="משפט ייחודי בלבד")
    assert shown == ['original']
    assert buttons == [False]
