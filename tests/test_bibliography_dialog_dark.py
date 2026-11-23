# -*- coding: utf-8 -*-
"""Browse's bibliographies (FJMS, Ktiv) in the dark theme (owner, 2026-11-23: their tables stayed white).

Each dialog's table carries ``bibliography-table``, which common.css themes as it does the joins candidate
table; the detail panel under it takes the theme's colours (its ``--surface-variant`` is defined nowhere, so
it fell back to a light grey).
"""
from __future__ import annotations

import asyncio
import pathlib
import re

from nicegui import Client, core, ui
from nicegui.testing.general import prepare_simulation

from web.components import bibliography_dialog as bd

ROOT = pathlib.Path(__file__).resolve().parents[1]
FJMS = [{'author': 'Author', 'title': 'Title', 'year': '1979', 'pages': 'p. 114', 'mention_type': 'mention'}]
MARC = ['Author, A. Title. Journal 29 (2017), p. 261 [mention, transcription]']


def _drawn(build):
    prepare_simulation()
    seen = {}

    async def run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_bibliography_dark_{build.__name__}')) as client:
            build()
        seen['tables'] = [e.classes for e in client.elements.values() if isinstance(e, ui.table)]
        seen['panels'] = [e._style for e in client.elements.values() if isinstance(e, ui.html)]

    asyncio.run(run())
    return seen


def _fjms():
    bd.create_fjms_bibliography_dialog(FJMS, '990000000000205171', 'T-S Misc.8.15')


def _ktiv():
    bd.create_nli_bibliography_dialog(MARC, '990000000000205171', 'T-S Misc.8.15')


def test_both_tables_carry_the_themed_class():
    for build in (_fjms, _ktiv):
        seen = _drawn(build)
        assert seen['tables'] and all('bibliography-table' in c for c in seen['tables']), (build, seen['tables'])
        panels = [s for s in seen['panels'] if 'max-height' in s]
        assert panels and all(s.get('background') == 'var(--bg-tertiary)' for s in panels), panels


def test_the_dark_theme_styles_the_class():
    css = (ROOT / 'web/static/common.css').read_text(encoding='utf-8')
    for part in (r'\.q-table__card', r'\.q-table thead tr th', r'\.q-table tbody td', r'\.q-table__bottom'):
        assert re.search(r'\[data-theme="dark"\] \.bibliography-table ' + part + r'[^{]*\{', css), part
    assert 'surface-variant' not in (ROOT / 'web/components/bibliography_dialog.py').read_text(encoding='utf-8')
