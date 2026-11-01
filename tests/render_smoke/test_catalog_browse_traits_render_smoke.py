"""/catalog-browse lists manuscripts by script and size, from its address on.

Opens the real page with traits in the address and checks the three places they
must reach: the results query and the library facets (the list and its counts),
the chips (what the visitor sees), and "Search in these results" (the handoff).
"""

from __future__ import annotations

import asyncio
import json
import re
from unittest.mock import patch

import web.main as web_main  # noqa: F401  (registers /catalog-browse)
import web.pages.catalog_browse as catalog_browse
from nicegui import ui
from nicegui.testing.user import User
from tests.render_smoke.test_catalog_browse_initial_load_render_smoke import (
    _FakeFjms, _page_text, _run,
)

_WANTED = {'script_places': ['Spanish'], 'width_min': 10.0, 'width_max': 20.0}


class _TraitFjms(_FakeFjms):
    def __init__(self):
        super().__init__()
        self.result_traits = []
        self.facet_traits = []

    def measurement_filter_support(self):
        return 'supported'

    def line_height_filter_support(self):
        return 'supported'

    def get_browse_results(self, *args, **kwargs):
        self.result_traits.append(kwargs.get('traits'))
        return super().get_browse_results(*args, **kwargs)

    def get_browse_library_facets(self, *args, **kwargs):
        self.facet_traits.append(kwargs.get('traits'))
        return {}


def test_traits_in_the_address_reach_the_list_the_chips_and_the_handoff():
    fake = _TraitFjms()
    handed = {}

    def _record_set(key, value):
        handed[key] = value

    async def driver(user: User) -> None:
        await user.open('/catalog-browse?script_place=Spanish&width=10-20')
        deadline = asyncio.get_running_loop().time() + 5.0
        while not fake.facet_traits and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.2)  # the chips are drawn after the facets
        assert fake.result_traits and fake.result_traits[-1] == _WANTED, fake.result_traits
        assert fake.facet_traits[-1] == _WANTED, fake.facet_traits
        text = _page_text(user)
        assert 'Script Place: Spanish' in text, text
        assert 'Width: 10–20 cm' in text, text
        user.find(kind=ui.button, content='Search in these results').click()
        await asyncio.sleep(0.1)
        assert handed.get('incoming_filters', {}).get('script_places') == ['Spanish'], handed
        assert handed['incoming_filters'].get('width_min') == 10.0, handed

    with patch.object(catalog_browse, 'get_fjms_service', return_value=fake), \
            patch.object(catalog_browse, 'safe_user_set', side_effect=_record_set), \
            patch.object(catalog_browse.ui.navigate, 'to', lambda *a, **k: None):
        _run(driver)


def _chip_close_button(user: User, chip_text: str):
    """The close button of the chip whose label starts with *chip_text*."""
    for element in list(user._client.elements.values()):
        if not isinstance(element, ui.button) or element.props.get('icon') != 'close':
            continue
        row = element.parent_slot.parent
        if any(isinstance(c, ui.label) and c.text.startswith(chip_text) for c in row.default_slot.children):
            return element
    raise AssertionError(f'no chip {chip_text!r}')


def test_removing_a_chip_updates_the_saved_filters():
    """A chip's remove button redraws the chip bar, deleting itself; the saved
    filters (and the address) were then written from that deleted context, which
    raised -- and a reload brought the removed filter back."""
    from nicegui.client import Client
    from nicegui.testing.user_interaction import UserInteraction
    fake = _TraitFjms()
    saved = []
    _set_item = re.compile(r"sessionStorage\.setItem\('catalog_browse_filters', (.*)\)$", re.S)
    real_run_javascript = Client.run_javascript

    def _recording_run_javascript(self, code, *args, **kwargs):
        match = _set_item.match(code)
        if match:
            saved.append(json.loads(json.loads(match.group(1))))
        return real_run_javascript(self, code, *args, **kwargs)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse?script_place=Spanish&width=10-20')
        deadline = asyncio.get_running_loop().time() + 5.0
        while not saved and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        assert saved and saved[-1]['traits'] == _WANTED, saved
        with user._client:
            button = _chip_close_button(user, 'Width:')
        UserInteraction(user, {button}, None).click()
        deadline = asyncio.get_running_loop().time() + 5.0
        while saved[-1]['traits'] == _WANTED and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        assert saved[-1]['traits'] == {'script_places': ['Spanish']}, saved[-1]
        assert fake.result_traits[-1] == {'script_places': ['Spanish']}

    with patch.object(catalog_browse, 'get_fjms_service', return_value=fake), \
            patch.object(Client, 'run_javascript', _recording_run_javascript):
        _run(driver)
