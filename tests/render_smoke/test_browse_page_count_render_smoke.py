# -*- coding: utf-8 -*-
"""Render-smoke: Browse's page count for a manuscript without any automatic transcription.

The index holds no pages of such a manuscript, so its page comes with ``total_pages`` 0
(``SearchEngine._get_metadata_only_browse_page``): Browse showed "Page 1 / 0" and Next was disabled, so its other
images could not be reached (T-S NS 243.145 has 4). Phase B now counts its images (``web/page_count.py``,
unit-tested in tests/test_manuscript_image_count.py), Next/Previous keep that count while Phase B reruns, a count
that needs the NLI manifest arrives after the page renders (Codex on #454), and a count that cannot be known is not
shown at all.

Drives the real ``/browse`` over a stubbed browse service (harness: test_browse_button_icons_render_smoke.py).
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

from nicegui import ui

from tests.render_smoke.test_browse_button_icons_render_smoke import (
    _all_elements, _click, _run_home_smoke, _wait_until,
)
from web.pages import browse
from web.services import BrowsePage

SID = '990051614250205171'  # T-S NS 243.145


def _page(p_num, volume_suffix=1):
    return BrowsePage(uid='', p_num=p_num, text='', full_header='', total_pages=0, current_idx=p_num,
                      sys_id=SID, shelfmark='T-S NS 243.145', library_code='CUL', volume_suffix=volume_suffix)


def _service(volume_suffix=1):
    """The engine's metadata-only page: the requested page, moved by direction, with no count."""
    svc = MagicMock()
    svc.is_ready.return_value = True
    svc.get_browse_page.side_effect = (
        lambda sys_id, p_num=None, direction=0, volume_ie=None, **_kw:
        _page(max(1, (p_num or 1) + direction), volume_suffix))
    svc.get_browse_page_by_fl.return_value = None
    return svc


def _run(counts, driver, seconds=0.0, volume_suffix=1, asked=None, external=None):
    """``counts``: what manuscript_image_count answers, call by call (the last answer repeats), each after
    ``seconds``; ``asked`` collects the volume (suffix) each call was for; ``external``: (provider, image count)
    of an external image list (``resolve_external_images``)."""
    answers = list(counts)

    def count(sys_id, folio_images=None, suffix=1):
        if asked is not None:
            asked.append(suffix)
        time.sleep(seconds)
        return answers.pop(0) if len(answers) > 1 else answers[0]

    crossref = MagicMock()
    crossref.is_available.return_value = False
    provider, n_external = external or ('', 0)
    external_images = {'cambridge_images': [{'url': f'https://example.org/iiif/{i}/full/max/0/default.jpg'}
                                            for i in range(n_external)],
                       'external_provider': provider, 'cambridge_alignment': None}
    with patch.object(browse, 'get_service', return_value=_service(volume_suffix)), \
            patch('web.components.image_resolution.resolve_external_images', return_value=external_images), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch('web.page_count.manuscript_image_count', side_effect=count), \
            patch('web.page_count.PAGE_COUNT_WAIT_S', 0.05), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=False):
        _run_home_smoke(driver, lang='en')


def _labels(user):
    return {e.text for e in _all_elements(user) if isinstance(e, ui.label)}


def _next(user):
    [button] = [b for b in _all_elements(user) if isinstance(b, ui.button) and b.props.get('data-action') == 'next']
    return button


def _page_input(user):
    return next(e for e in _all_elements(user) if isinstance(e, ui.number))


def test_a_manuscript_without_automatic_text_counts_its_images_and_pages_on():
    async def driver(user):
        await user.open(f'/browse?sys_id={SID}')
        await _wait_until(lambda: '/ 4' in _labels(user))
        assert '/ 4' in _labels(user) and '/ 0' not in _labels(user), sorted(_labels(user))
        assert not _next(user).props.get('disabled')

        _click(user, _next(user))
        await _wait_until(lambda: _page_input(user).value == 2)
        await asyncio.sleep(0.5)  # past Phase B, whose count is now unknown (0): the page keeps the 4 it had
        assert _page_input(user).value == 2
        assert '/ 4' in _labels(user), sorted(_labels(user))
        assert not _next(user).props.get('disabled')

    _run([4, 0], driver)


def test_a_count_that_cannot_be_known_is_not_shown_as_zero():
    async def driver(user):
        await user.open(f'/browse?sys_id={SID}')
        await _wait_until(lambda: any(isinstance(e, ui.number) for e in _all_elements(user)))
        await asyncio.sleep(0.5)  # past Phase B
        assert not [label for label in _labels(user) if label.startswith('/ ')], sorted(_labels(user))
        assert _next(user).props.get('disabled')

    _run([0], driver)


def _version_chooser(user):
    return [b for b in _all_elements(user) if isinstance(b, ui.button) and b.props.get('icon') == 'history']


def test_a_slow_count_does_not_hold_up_phase_b():
    # Codex on #454: a count that needs the NLI manifest must not hold up Phase B (the edition and its chooser).
    async def driver(user):
        await user.open(f'/browse?sys_id={SID}')
        await _wait_until(lambda: _version_chooser(user))
        assert _version_chooser(user), 'Phase B did not finish'
        assert '/ 4' not in _labels(user)  # Phase B finished while the count was still on its way
        await _wait_until(lambda: '/ 4' in _labels(user))
        assert '/ 4' in _labels(user), sorted(_labels(user))
        assert not _next(user).props.get('disabled')

    _run([4], driver, seconds=3.0)


def test_the_count_is_of_the_volume_the_images_come_from():
    # Codex on #454: Browse shows a page's images from its volume's manifest (page.volume_suffix).
    asked = []

    async def driver(user):
        await user.open(f'/browse?sys_id={SID}')
        await _wait_until(lambda: '/ 4' in _labels(user))
        assert '/ 4' in _labels(user)

    _run([4], driver, volume_suffix=2, asked=asked)
    assert asked and set(asked) == {2}, asked


def test_an_external_image_list_counts_the_pages_it_shows():
    # Codex on #454: resolve_image_url shows JTS (and Manchester) images first; the pages are theirs, as before.
    asked = []

    async def driver(user):
        await user.open(f'/browse?sys_id={SID}')
        await _wait_until(lambda: '/ 3' in _labels(user))
        assert '/ 3' in _labels(user) and '/ 4' not in _labels(user), sorted(_labels(user))

    _run([4], driver, asked=asked, external=('jts', 3))
    assert asked == []  # no NLI count asked for
