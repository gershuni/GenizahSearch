# -*- coding: utf-8 -*-
"""The Print / Save as PDF control must be reachable without opening a panel.

This exists because of a real miss on 2026-09-04. The button was added beside
"Export Word", which sits in the Export group -- inside the metadata panel,
which `create_browse_page` starts COLLAPSED (`show_metadata = {'value': False}`).
So a reader had to click "Show Metadata" before the control existed at all, and
the owner reported the feature as missing. It was, in every way that matters.

Worth recording HOW that shipped, because the shape of the mistake is more
instructive than the mistake: printing had been verified end-to-end by driving
`window.print()` and rendering a real PDF. That proves the STYLESHEET and
bypasses the button entirely. A working feature behind an unreachable control is
not a working feature, and no amount of PDF rendering would ever have said so.

These tests check STRUCTURE in the source rather than a live DOM, deliberately:
`/browse` needs a manuscript, an image service and the enrichment pass before it
paints anything, so there is no render-smoke harness for it, and a test that
needs local sidecar data would skip in CI -- which is exactly where this guard
has to bite.
"""

from __future__ import annotations

import re
from pathlib import Path

BROWSE = Path(__file__).resolve().parents[1] / 'web' / 'pages' / 'browse.py'

PRINT_CALL = "ui.run_javascript('window.print()')"


def _lines():
    return BROWSE.read_text(encoding='utf-8').splitlines()


def _metadata_panel_span(lines):
    """The line range of the collapsed metadata-panel block, by indentation.

    Returns (start, end) as 0-based indices, end exclusive. The block begins at
    ``if show_metadata['value']:`` and ends at the first following non-blank
    line indented no deeper than that ``if``.
    """
    start = None
    for i, line in enumerate(lines):
        if re.match(r"\s*if show_metadata\['value'\]:", line):
            start = i
            break
    assert start is not None, (
        "could not find `if show_metadata['value']:` -- has the metadata panel "
        "been restructured? This guard needs re-pointing, not deleting.")
    indent = len(lines[start]) - len(lines[start].lstrip())
    for j in range(start + 1, len(lines)):
        stripped = lines[j].strip()
        if not stripped:
            continue
        if (len(lines[j]) - len(lines[j].lstrip())) <= indent:
            return start, j
    return start, len(lines)


def test_a_print_control_exists_at_all():
    lines = _lines()
    hits = [i for i, ln in enumerate(lines) if PRINT_CALL in ln]
    assert hits, 'no Print / Save as PDF control on the browse page'


def test_the_print_control_is_reachable_without_opening_the_metadata_panel():
    """The defect, pinned. `show_metadata` starts False, so anything only inside
    that block is invisible on arrival."""
    lines = _lines()
    start, end = _metadata_panel_span(lines)
    hits = [i for i, ln in enumerate(lines) if PRINT_CALL in ln]
    assert hits, 'no Print / Save as PDF control on the browse page'
    outside = [i for i in hits if not (start <= i < end)]
    assert outside, (
        'every Print control (lines %s) is inside the metadata panel '
        '(lines %d-%d), which starts collapsed -- the reader cannot see it. '
        'Put one on the always-visible toolbar.'
        % ([i + 1 for i in hits], start + 1, end)
    )


def test_the_metadata_panel_really_does_start_collapsed():
    """The control test above is only meaningful while this holds. If the panel
    is ever opened by default, that assertion stops proving anything -- so this
    is the control for it, not a restatement."""
    text = BROWSE.read_text(encoding='utf-8')
    assert re.search(r"show_metadata\s*=\s*\{'value':\s*False\}", text), (
        'the metadata panel no longer starts collapsed; re-read '
        'test_the_print_control_is_reachable_without_opening_the_metadata_panel '
        'before trusting it')


def _toolbar_print_button(lines):
    """The toolbar's print button declaration, as (index, its own source chain).

    Keyed to `icon='print'` OUTSIDE the metadata panel, then the following few
    lines of its `.props(...).classes(...).tooltip(...)` chain.

    NOT a fixed window around every `window.print()` call: the button now owns a
    MENU, so the class list sits ABOVE the menu items rather than below them. An
    earlier version of this test looked 12 lines forward from each call site and
    went red the moment the menu was added -- while the property it claimed to
    check (the control is print-hidden) was still perfectly true. A test that
    fails on a refactor it should not care about teaches you to ignore it.
    """
    start, end = _metadata_panel_span(lines)
    for i, ln in enumerate(lines):
        if "icon='print'" in ln and not (start <= i < end):
            return i, '\n'.join(lines[i:i + 8])
    return None, ''


def test_the_print_control_does_not_print_itself():
    """`print-hide` on the control, so the sheet does not carry a picture of the
    button that produced it. (The toolbar's card is print-hidden too, and
    `.q-menu` is hidden globally in the print block; this is what survives a
    refactor of either.)"""
    lines = _lines()
    i, chain = _toolbar_print_button(lines)
    assert i is not None, 'no toolbar print button found outside the metadata panel'
    assert 'print-hide' in chain, (
        'the visible Print control at line %d is not print-hide, so it can '
        'appear on its own output' % (i + 1))


def test_the_menu_itself_cannot_reach_the_page():
    """The print options live in a `ui.menu`, so the print stylesheet has to
    hide menus outright -- otherwise a menu left open at print time lands on
    the sheet."""
    css = (Path(__file__).resolve().parents[1] / 'web' / 'static' / 'common.css'
           ).read_text(encoding='utf-8')
    block = css[css.index('@media print'):]
    assert '.q-menu' in block, (
        'the print block does not hide .q-menu; an open menu would print')


def test_the_control_is_labelled_for_a_screen_reader_and_a_tooltip():
    """It is icon-only on the toolbar, so the accessible name is the only name
    it has."""
    lines = _lines()
    i, chain = _toolbar_print_button(lines)
    assert i is not None, 'no toolbar print button found outside the metadata panel'
    assert 'aria-label' in chain and 'tooltip' in chain, (
        'the visible Print control has no aria-label + tooltip pair; an '
        'icon-only button with neither is unnameable')


def test_printing_all_pages_is_offered_and_reaches_the_all_pages_view():
    """Owner, 2026-09-04: a manuscript may be long, so the option is "all
    manuscript pages", not "both sides".

    Full Manuscript View is a SEPARATE render branch, so printing it needs the
    view switch first. The intent has to survive `toggle_view_all` rebuilding
    the tree -- which is what `print_pending` is for, and what this pins.
    """
    text = BROWSE.read_text(encoding='utf-8')
    assert "tr('Print all manuscript pages')" in text
    assert "tr('Print this page')" in text
    assert "print_pending = {'value': False}" in text, (
        'the deferred-print flag is gone; a print fired from the click handler '
        'cannot survive toggle_view_all rebuilding the content tree')
    assert "print_pending['value'] = True" in text
    assert "print_pending['value'] = False" in text, (
        'the flag is never cleared, so switching to Full Manuscript View by '
        'hand would raise an unasked-for print dialog')


def test_the_all_pages_sheet_carries_its_own_masthead_and_credit():
    """The single-page masthead lives in the transcription panel, which the
    all-pages branch does not render. Without its own, the sheet most likely to
    leave the building is the one with no shelfmark and no credit on it."""
    lines = _lines()
    # The RENDER branch, not `toggle_view_all`'s own `if state.view_all:` a
    # couple of thousand lines earlier -- which is what an unqualified search
    # finds first, and which contains no masthead by design.
    fmv = None
    for i, ln in enumerate(lines[:-1]):
        if 'if state.view_all:' in ln and 'Show all pages' in lines[i + 1]:
            fmv = i
            break
    assert fmv is not None, (
        'could not find the Full Manuscript View render branch '
        "(`if state.view_all:` followed by the `# Show all pages` comment)")
    # Its span: to the next `elif`/`else` at the same indentation.
    indent = len(lines[fmv]) - len(lines[fmv].lstrip())
    end = len(lines)
    for j in range(fmv + 1, len(lines)):
        stripped = lines[j].strip()
        if not stripped:
            continue
        if (len(lines[j]) - len(lines[j].lstrip())) <= indent and stripped.startswith(
                ('elif ', 'else:')):
            end = j
            break
    block = '\n'.join(lines[fmv:end])
    assert "classes('print-only')" in block, (
        'the all-pages branch has no print-only masthead')
    assert 'resolve_transcription_credit' in block, (
        'the all-pages sheet carries no transcription credit')


def test_the_all_pages_sheet_credits_the_automatic_transcription():
    """FMV renders `doc_page.text` for every folio and offers no per-page
    version chooser, so the automatic transcription is what prints -- and
    `resolve_transcription_credit(None, ...)` is the honest credit for it.

    If FMV ever gains a version chooser this test is the tripwire: the credit
    would then have to follow the selection, exactly as the single-page one
    does.
    """
    text = BROWSE.read_text(encoding='utf-8')
    assert 'resolve_transcription_credit(None, lang=get_language())' in text, (
        'the all-pages credit is no longer the unconditional HTR one; if FMV '
        'gained a version selector, make the credit follow it')


def test_the_label_goes_through_translation():
    """Hebrew is the default UI language here, so an untranslated label reaches
    most of the site's readers."""
    text = BROWSE.read_text(encoding='utf-8')
    assert "tr('Print / Save as PDF')" in text, (
        'the Print label is not wrapped in tr()')
    translations = (Path(__file__).resolve().parents[1] / 'genizah_translations.py'
                    ).read_text(encoding='utf-8')
    assert '"Print / Save as PDF"' in translations, (
        'no Hebrew entry for the Print label -- tr() answers a miss by handing '
        'back the English key, so nothing would raise')
