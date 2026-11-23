# -*- coding: utf-8 -*-
"""Render-smoke: the head of Browse's text, and the reading text's size (owner, 2026-11-23).

* The version chooser's row (line numbers, A- A+, the dictionary switch, then the chooser) heads the
  text pane. It sat under the text: below the fold at 1280x800, after the whole text on a phone.
* The "V0.8" badge in the page's toolbar is gone: it named the automatic transcription's model even
  while an edition was on screen.
* A- / A+ (web/components/text_size.py): one size per visitor, a CSS variable the transcriptions'
  font sizes are multiplied by (Browse's page and Full Manuscript View, the Reading Desk), steps
  of the desktop app's (9pt to 40pt by 2pt of its 16: 9/16 to 2.5 by 0.125). The Reading Desk's texts
  have the buttons too.

Harnesses: ``test_browse_button_icons_render_smoke`` (Browse) and
``test_reading_desk_document_render_smoke`` (the desk).
"""
from __future__ import annotations

from nicegui import ui

from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE,
    _all_elements,
    _click,
    _open_browse,
    _run_browse,
    _wait_until,
)
from web.components import text_size
from web.pages import browse
from web.pages import browse_desk_document


def test_the_sizes_are_the_desktop_apps_steps():
    assert text_size.clamp(1) == 1.0 and text_size.clamp('1.125') == 1.125
    assert text_size.clamp(9) == 2.5 and text_size.clamp(0.1) == 0.5625     # 40pt and 9pt of 16 (Codex #108)
    assert text_size.clamp(1.03) == 1.0 and text_size.clamp(1.05) == 1.0625  # to the nearest whole point
    # down from the default as the desktop goes: 14, 12, 10, then its floor, 9
    sizes, size = [], 1.0
    for _ in range(5):
        size = text_size.clamp(size - text_size.STEP)
        sizes.append(size)
    assert sizes == [0.875, 0.75, 0.625, 0.5625, 0.5625]
    assert text_size.clamp(0.5625 + text_size.STEP) == 0.6875                # 11pt, as the desktop from 9
    # anything else is the default: a step below the page's sizes (owner, 2026-11-23: a bit smaller)
    assert text_size.DEFAULT == 0.875
    assert text_size.clamp(None) == 0.875 and text_size.clamp('x') == 0.875 and text_size.clamp(float('nan')) == 0.875
    assert text_size.scaled('1.2rem') == 'calc(1.2rem * var(--gs-text-scale, 1))'
    assert text_size.head_style(1.25) == '<style>:root { --gs-text-scale: 1.25; }</style>'


def test_the_texts_are_sized_by_the_readers_scale():
    styles = browse.VIEWER_STYLES
    for px in (17, 18, 19, 20):
        assert f'font-size: calc({px}px * var(--gs-text-scale, 1)) !important' in styles, px
    assert 'calc(1.2rem * var(--gs-text-scale, 1))' in browse_desk_document._TEXT_STYLE


def _row(user):
    rows = [e for e in _all_elements(user) if 'browse-text-head' in e.classes]
    return rows[0] if rows else None


def test_the_version_row_heads_the_text_and_the_badge_is_gone():
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        await _wait_until(lambda: _row(user) is not None)
        row = _row(user)
        siblings = list(row.parent_slot.children)
        texts = [i for i, el in enumerate(siblings)
                 if any(isinstance(d, ui.html) and 'line-numbered-text' in (d.content or '') for d in el.descendants())]
        seen['order'] = (siblings.index(row), texts)
        seen['in_row'] = sorted(b.props.get('aria-label') or b.props.get('icon') for b in row.descendants()
                                if isinstance(b, ui.button) and ('gs-text-size-btn' in b.classes
                                                                 or b.props.get('icon') == 'format_list_numbered'))
        seen['badge'] = [e for e in _all_elements(user) if 'source-badge' in e.classes]

    _run_browse(OPEN_PAGE, driver)
    row_at, text_at = seen['order']
    assert text_at and row_at < text_at[0], seen['order']             # the row first, then the text
    assert seen['in_row'] == ['Larger text', 'Smaller text', 'format_list_numbered']
    assert seen['badge'] == []


def test_the_line_numbers_change_the_text_in_place():
    """Owner, 2026-11-23: the line numbers' button made the window jump up. The text was drawn again from
    nothing: the pane was empty for a moment, the page lost its height and the browser pulled a short
    page's scroll up. Now the text's element stays and its content changes."""
    seen = {}

    def _texts(user):
        return [d for d in _all_elements(user) if isinstance(d, ui.html)
                and any(cls in (d.content or '') for cls in ('transcription-text', 'line-numbered-text'))]

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        await _wait_until(lambda: _row(user) is not None and _texts(user))
        [before] = _texts(user)
        seen['numbered'] = 'line-numbered-text' in before.content
        [button] = [b for b in _row(user).descendants() if isinstance(b, ui.button)
                    and b.props.get('icon') == 'format_list_numbered']
        _click(user, button)
        [after] = _texts(user)
        seen['same'] = after is before
        seen['numbered_after'] = 'line-numbered-text' in after.content
        _click(user, button)                                           # back as it was, for the visitor
        seen['numbered_again'] = 'line-numbered-text' in _texts(user)[0].content

    _run_browse(OPEN_PAGE, driver)
    assert seen['same']
    assert seen['numbered_after'] != seen['numbered']
    assert seen['numbered_again'] == seen['numbered']


def test_a_plus_and_a_minus_keep_the_visitors_size():
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        await _wait_until(lambda: _row(user) is not None)
        buttons = {b.props.get('aria-label'): b for b in _row(user).descendants() if 'gs-text-size-btn' in b.classes}
        with user._client:
            seen['start'] = text_size.current()
        _click(user, buttons['Larger text'])
        with user._client:
            seen['one_up'] = text_size.current()
        for _ in range(20):
            _click(user, buttons['Larger text'])
        with user._client:
            seen['top'] = text_size.current()
        _click(user, buttons['Smaller text'])
        with user._client:
            seen['one_down'] = text_size.current()

    _run_browse(OPEN_PAGE, driver)
    assert seen['start'] == 0.875                                     # a step below the page's sizes
    assert seen['one_up'] == 1.0
    assert seen['top'] == 2.5                                          # no further than the desktop's 40pt
    assert seen['one_down'] == 2.375


def test_the_reading_desk_texts_have_a_minus_and_a_plus(tmp_path):
    from tests.render_smoke.test_reading_desk_document_render_smoke import FRAGMENTS, _run as run_desk
    from tests.test_reading_desk_document import DOC
    seen = {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)                                         # a hand-built desk: texts by fragment
        await _wait_until(lambda: [e for e in _all_elements(user) if 'rd-text-head' in e.classes])
        [head] = [e for e in _all_elements(user) if 'rd-text-head' in e.classes]
        seen['plain'] = sorted(b.props.get('aria-label') for b in head.descendants() if 'gs-text-size-btn' in b.classes)
        enter(FRAGMENTS, DOC)                                          # a document's desk: its own text panel
        await _wait_until(lambda: any('rd-document-mode' in e.classes for e in _all_elements(user)))
        heads = [e for e in _all_elements(user) if 'rd-text-head' in e.classes]
        seen['document'] = [sorted(b.props.get('aria-label') for b in h.descendants() if 'gs-text-size-btn' in b.classes)
                            for h in heads]

    run_desk(tmp_path, driver)
    assert seen['plain'] == ['Larger text', 'Smaller text']
    assert seen['document'] == [['Larger text', 'Smaller text']]


def test_a_search_deep_link_rewrites_the_choosers_one_tooltip():
    """The chooser is a button with its tooltip, "Version". A search's deep link that showed the version
    holding the phrase says so in that tooltip: one tooltip, its text changed, never a second (Codex #108)."""
    import asyncio

    from nicegui import core
    from nicegui.client import Client
    from nicegui.testing.general import prepare_simulation

    from tests.test_version_selector_marks_legend import GENUZOT, TEXT
    from web.components.version_selector import create_version_selector
    from web.translations import tr

    prepare_simulation()
    tips = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        for name, phrase in (('browsing', None), ('search', 'משפט ייחודי בלבד')):
            with Client(ui.page(f'/_chooser_tooltip_probe_{name}')) as client:
                create_version_selector('990000000000017', 1, TEXT + '\nמשפט ייחודי בלבד',
                                        all_sources=[GENUZOT], must_contain=phrase)
            await asyncio.sleep(0.4)
            [chooser] = [e for e in client.elements.values() if e.props.get('data-testid') == 'version-chooser']
            # its own, and any aimed at it from outside (NiceGUI's ``.tooltip()`` adds one by the element's id)
            mine = set(map(id, chooser.descendants()))
            tips[name] = [t.text for t in client.elements.values() if isinstance(t, ui.tooltip)
                          and (id(t) in mine or t.props.get('target') == f'#{chooser.html_id}')]
            with client:
                words[name] = tr('Version') if phrase is None else tr('showing the version containing your search')

    words = {}
    asyncio.run(_run())
    assert tips['browsing'] == [words['browsing']]
    assert tips['search'] == [words['search']]
