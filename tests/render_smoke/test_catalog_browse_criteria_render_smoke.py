"""/catalog-browse with the catalogue criteria (PR 1b of
docs/plans/CATALOGUE_CRITERIA_PLAN.md): the "Manuscript features" button and
panel replace the traits card and the date controls.

The page runs on a real FjmsService over the small sidecar of
tests/criteria_sidecar.py, so what reaches the list is what the shared
matcher answers. Checked: the address (D8) in and out, unknown and malformed
parameters named in a notice, a 5.0.0 file dropping what it cannot answer
(rule 9) from the address and from the saved session, saved dates read in as
the period of copy (D18), chip removal reaching the saved filters, the panel
itself, the handoff, and the tree's "No catalogue record" line.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import web.main as web_main  # noqa: F401  (registers /catalog-browse)
import web.pages.catalog_browse as catalog_browse
from nicegui import ui
from nicegui.client import Client
from nicegui.testing.user import User
from nicegui.testing.user_interaction import UserInteraction
from shared.fjms_service import FjmsService
from tests.render_smoke.test_catalog_browse_initial_load_render_smoke import _page_text, _run
from web.state import state

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from criteria_sidecar import A, B, UNIVERSE, build_criteria_sidecar  # noqa: E402


class _Recording(FjmsService):
    """The real service, recording what the page asks for."""

    def __init__(self, path):
        super().__init__(db_path=path)
        self.result_criteria = []
        self.facet_criteria = []

    def get_browse_results(self, *args, **kwargs):
        self.result_criteria.append(kwargs.get('criteria'))
        return super().get_browse_results(*args, **kwargs)

    def get_browse_library_facets(self, *args, **kwargs):
        self.facet_criteria.append(kwargs.get('criteria'))
        return super().get_browse_library_facets(*args, **kwargs)


@pytest.fixture(scope='module')
def sidecar(tmp_path_factory):
    return build_criteria_sidecar(tmp_path_factory.mktemp('crit') / 'fjms.db')


@pytest.fixture(scope='module')
def old_sidecar(tmp_path_factory):
    return build_criteria_sidecar(tmp_path_factory.mktemp('crit50') / 'fjms.db', upgrade=False)


def _meta():
    return SimpleNamespace(
        corpus_sys_ids=lambda timeout=30.0: frozenset(UNIVERSE),
        get_meta_for_id=lambda sid: (f'T-S {sid[-6:]}', ''),
        get_library_for_id=lambda sid: 'CUL',
    )


async def _until(predicate, seconds=6.0):
    deadline = asyncio.get_running_loop().time() + seconds
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.05)
    return predicate()


def _notices(user) -> list:
    return [str(m) for m in user.notify.messages]


def _patched(fake, **extra):
    # Not ui.navigate.to: the test User puts its own navigator into ui.navigate
    # whenever it is used, so a patch on the object found here never applies
    # (tests that click a handoff patch user.navigate instead).
    patches = [patch.object(catalog_browse, 'get_fjms_service', return_value=fake),
               patch.object(state, 'meta_mgr', _meta())]
    patches += [patch.object(catalog_browse, name, value) for name, value in extra.items()]
    return patches


def _run_with(patches, driver):
    for p in patches:
        p.start()
    try:
        _run(driver)
    finally:
        for p in reversed(patches):
            p.stop()


_WANTED = {'style': {'values': ['Square'], 'mode': 'only'},
           'region': {'values': ['Oriental'], 'mode': 'except'},
           'period': {'min': 1000, 'max': 1200, 'not_recorded': True}}
_ADDRESS = '/catalog-browse?style=Square&region=!Oriental&period=1000..1200,_none'


def test_the_address_reaches_the_list_the_chips_and_the_handoff(sidecar):
    fake = _Recording(sidecar)
    handed = {}

    async def driver(user: User) -> None:
        await user.open(_ADDRESS)
        went = []
        # The User's own navigator (ui.navigate is it while the User runs): a
        # handoff stays on this page, and says where it would go. (A patch on
        # ui.navigate taken before the User existed never applied: the first
        # click opened /search in the background, and the second button was
        # gone when CI was slow.)
        user.navigate.to = lambda target, *a, **k: went.append(target)
        assert await _until(lambda: fake.facet_criteria), fake.result_criteria
        assert fake.result_criteria[-1] == _WANTED, fake.result_criteria
        assert fake.facet_criteria[-1] == _WANTED
        assert await _until(lambda: 'Region: all except Oriental' in _page_text(user))
        text = _page_text(user)
        assert 'Style: Square' in text, text
        assert 'Period: 1000–1200, or not recorded' in text, text
        assert await _until(lambda: 'Parallel search in these results' in _page_text(user))
        user.find(kind=ui.button, content='Search in these results').click()
        await asyncio.sleep(0.1)
        assert handed['incoming_filters']['criteria'] == _WANTED, handed
        user.find(kind=ui.button, content='Parallel search in these results').click()
        await asyncio.sleep(0.1)
        assert handed['incoming_filters']['criteria'] == _WANTED, handed
        assert went == ['/search?from_browse=1', '/parallels'], went

    _run_with(_patched(fake, safe_user_set=lambda k, v: handed.__setitem__(k, v)), driver)


def test_the_address_written_back_is_the_d8_form(sidecar):
    fake = _Recording(sidecar)
    urls = []
    real = Client.run_javascript

    def recording(self, code, *args, **kwargs):
        if code.startswith('history.replaceState'):
            urls.append(code)
        return real(self, code, *args, **kwargs)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse?lines=meas:20..25&material=cat:!Vellum,_none')
        assert await _until(lambda: urls), urls
        assert "lines=meas:20..25" in urls[-1], urls[-1]
        assert "material=cat:!Vellum,_none" in urls[-1], urls[-1]

    with patch.object(Client, 'run_javascript', recording):
        _run_with(_patched(fake), driver)


def test_unknown_and_malformed_parameters_are_named(sidecar):
    fake = _Recording(sidecar)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse?style=Square&lines=!20..25&script_style=Square&utm_source=x')
        assert await _until(lambda: fake.result_criteria)
        assert fake.result_criteria[-1] == {'style': {'values': ['Square'], 'mode': 'only'}}
        notes = ' '.join(_notices(user))
        assert 'were not understood' in notes, notes
        assert 'lines' in notes and 'script_style' in notes, notes
        assert 'utm_source' not in notes, notes

    _run_with(_patched(fake), driver)


def test_a_5_0_0_file_drops_what_it_cannot_answer_from_the_address(old_sidecar):
    fake = _Recording(old_sidecar)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse?style=Square&lines=20..25&period=1000..1200')
        assert await _until(lambda: fake.result_criteria)
        # Lines and period need the 5.1.0 tables: dropped whole, never run on
        # their measured half (rule 9); the rest still lists.
        assert fake.result_criteria[-1] == {'style': {'values': ['Square'], 'mode': 'only'}}
        assert any('Some criteria were removed' in n for n in _notices(user)), _notices(user)

    _run_with(_patched(fake), driver)


def _session_rules(user, saved):
    user.javascript_rules[re.compile(r".*sessionStorage\.getItem\('catalog_browse_filters'\)")] = (
        lambda _: json.dumps(saved))


def test_saved_dates_are_read_in_as_the_period_of_copy(sidecar):
    fake = _Recording(sidecar)

    async def driver(user: User) -> None:
        _session_rules(user, {'date_from': 1000, 'date_to': 1200, 'undated': True,
                              'traits': {'script_places': ['Spanish']}})
        await user.open('/catalog-browse')
        assert await _until(lambda: fake.result_criteria and fake.result_criteria[-1])
        assert fake.result_criteria[-1] == {
            'period': {'min': 1000, 'max': 1200, 'not_recorded': True}}, fake.result_criteria

    _run_with(_patched(fake), driver)


def test_a_5_0_0_file_drops_what_it_cannot_answer_from_the_session(old_sidecar):
    fake = _Recording(old_sidecar)

    async def driver(user: User) -> None:
        _session_rules(user, {'criteria': {'columns': {'min': 2, 'max': 2},
                                           'region': {'values': ['Spanish']}}})
        await user.open('/catalog-browse')
        assert await _until(lambda: fake.result_criteria and fake.result_criteria[-1])
        assert fake.result_criteria[-1] == {'region': {'values': ['Spanish'], 'mode': 'only'}}
        assert any('Some criteria were removed' in n for n in _notices(user)), _notices(user)

    _run_with(_patched(fake), driver)


def test_a_malformed_address_does_not_bring_back_the_saved_session(sidecar):
    """GitHub Codex on PR 1b: ?columns=oops alone restored the visitor's saved
    criteria under the notice that the address was not understood."""
    fake = _Recording(sidecar)

    async def driver(user: User) -> None:
        _session_rules(user, {'criteria': {'region': {'values': ['Spanish']}}})
        await user.open('/catalog-browse?columns=oops')
        assert await _until(lambda: fake.result_criteria)
        await asyncio.sleep(0.3)
        assert not any(fake.result_criteria), fake.result_criteria
        assert any('columns' in n for n in _notices(user)), _notices(user)

    _run_with(_patched(fake), driver)


def _chip_close_button(user: User, chip_text: str):
    for element in list(user._client.elements.values()):
        if not isinstance(element, ui.button) or element.props.get('icon') != 'close':
            continue
        row = element.parent_slot.parent
        if any(isinstance(c, ui.label) and c.text.startswith(chip_text)
               for c in row.default_slot.children):
            return element
    raise AssertionError(f'no chip {chip_text!r}')


def test_removing_a_chip_updates_the_list_and_the_saved_filters(sidecar):
    """A chip's remove button deletes itself when the chips are redrawn; the
    saved filters must still be written (through the page's client)."""
    fake = _Recording(sidecar)
    saved = []
    _set_item = re.compile(r"sessionStorage\.setItem\('catalog_browse_filters', (.*)\)$", re.S)
    real = Client.run_javascript

    def recording(self, code, *args, **kwargs):
        match = _set_item.match(code)
        if match:
            saved.append(json.loads(json.loads(match.group(1))))
        return real(self, code, *args, **kwargs)

    async def driver(user: User) -> None:
        await user.open(_ADDRESS)
        assert await _until(lambda: saved and saved[-1]['criteria'] == _WANTED), saved
        assert await _until(lambda: 'Region: all except Oriental' in _page_text(user))
        with user._client:
            button = _chip_close_button(user, 'Region:')
        UserInteraction(user, {button}, None).click()
        expected = {k: v for k, v in _WANTED.items() if k != 'region'}
        assert await _until(lambda: saved[-1]['criteria'] == expected), saved[-1]
        assert fake.result_criteria[-1] == expected
        assert await _until(lambda: 'Region:' not in _page_text(user))

    with patch.object(Client, 'run_javascript', recording):
        _run_with(_patched(fake), driver)


def test_the_panel_ticks_a_value_counts_and_applies(sidecar):
    fake = _Recording(sidecar)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse')
        assert await _until(lambda: fake.result_criteria)
        user.find(marker='catalog-criteria-button').click()
        assert await _until(lambda: 'Physical status' in _page_text(user))
        user.find(marker='catalog-criteria-section-script').click()
        assert await _until(lambda: 'Square (1)' in _page_text(user)), _page_text(user)
        text = _page_text(user)
        assert 'Semi-Cursive (1)' in text and 'Not recorded (4)' in text, text
        assert 'recorded for 2 of 6' in text, text
        assert 'These criteria cover all 6 manuscripts. 2 have no FJMS catalogue record' in text
        user.find(kind=ui.checkbox, content='Square (1)').click()
        assert await _until(lambda: '1 manuscripts have these features' in _page_text(user))
        with user._client:
            user.find(marker='catalog-criteria-mode-region').elements.pop().set_value('except')
        assert await _until(lambda: 'Oriental (1)' in _page_text(user))
        user.find(kind=ui.checkbox, content='Oriental (1)').click()
        # Square and not Oriental: none; A is both.
        assert await _until(lambda: '0 manuscripts have these features' in _page_text(user))
        user.find(marker='catalog-criteria-apply').click()
        assert await _until(lambda: fake.result_criteria[-1] == {
            'style': {'values': ['Square'], 'mode': 'only'},
            'region': {'values': ['Oriental'], 'mode': 'except'}}), fake.result_criteria
        assert await _until(lambda: 'Region: all except Oriental' in _page_text(user))
        badge = user.find(marker='catalog-criteria-badge').elements.pop()
        assert badge.text == '2' and badge.visible

    _run_with(_patched(fake), driver)


def test_the_panel_disables_what_a_5_0_0_file_cannot_answer(old_sidecar):
    fake = _Recording(old_sidecar)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse')
        assert await _until(lambda: fake.result_criteria)
        user.find(marker='catalog-criteria-button').click()
        assert await _until(lambda: 'Period of copy' in _page_text(user))
        user.find(marker='catalog-criteria-section-lines').click()
        assert await _until(lambda: 'not available on this site yet' in _page_text(user))
        low = user.find(marker='catalog-criteria-min-lines').elements.pop()
        assert not low.enabled

    _run_with(_patched(fake), driver)


def test_the_century_buttons_set_their_centuries(sidecar):
    """GitHub Codex on PR 1b: "16-19" set 1600-1999, one century late."""
    fake = _Recording(sidecar)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse')
        assert await _until(lambda: fake.result_criteria)
        user.find(marker='catalog-criteria-button').click()
        assert await _until(lambda: 'Period of copy' in _page_text(user))
        user.find(marker='catalog-criteria-section-period').click()
        assert await _until(lambda: '16-19' in _page_text(user))
        for label, years in (('10th', (900, 999)), ('15th', (1400, 1499)),
                             ('16-19', (1500, 1899))):
            user.find(kind=ui.button, content=label).click()
            low = user.find(marker='catalog-criteria-min-period').elements.pop()
            high = user.find(marker='catalog-criteria-max-period').elements.pop()
            assert (low.value, high.value) == years, (label, low.value, high.value)

    _run_with(_patched(fake), driver)


def test_the_tree_counts_the_uncatalogued_manuscripts(sidecar):
    fake = _Recording(sidecar)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse')
        assert await _until(lambda: 'No catalogue record (2)' in _page_text(user)), _page_text(user)

    _run_with(_patched(fake), driver)


def test_the_list_follows_the_criteria(sidecar):
    """The real matcher behind the page: Square lists A alone."""
    fake = _Recording(sidecar)
    rows = []
    real = FjmsService.get_browse_results

    def recording(self, *args, **kwargs):
        result = real(self, *args, **kwargs)
        rows.append([r['sys_id'] for r in result['results']])
        return result

    async def driver(user: User) -> None:
        await user.open('/catalog-browse?style=Square')
        assert await _until(lambda: rows)
        assert rows[-1] == [A], rows
        assert B not in rows[-1]

    with patch.object(FjmsService, 'get_browse_results', recording):
        _run_with(_patched(fake), driver)


def _controls():
    import gc
    from web.components.criteria_panel import CriteriaControl
    return [o for o in gc.get_objects() if isinstance(o, CriteriaControl)]


def test_apply_survives_the_dialog_being_deleted_while_the_list_refreshes(sidecar):
    """Codex round 1: a browser hides the dialog (and the control deletes it)
    as soon as Apply closes it, while the page's refresh still runs; a ui call
    it then made in the Apply button's slot raised and the list never updated.
    The simulation does not send 'hide': this close does what it leads to."""
    fake = _Recording(sidecar)
    real_close = ui.dialog.close

    def close_and_hide(self):
        real_close(self)
        for control in _controls():  # this page's control: the one whose dialog this is
            if control._dialog is self:
                control._closed(self)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse')
        assert await _until(lambda: fake.result_criteria)
        user.find(marker='catalog-criteria-button').click()
        assert await _until(lambda: 'Physical status' in _page_text(user))
        user.find(marker='catalog-criteria-section-script').click()
        assert await _until(lambda: 'Square (1)' in _page_text(user))
        user.find(kind=ui.checkbox, content='Square (1)').click()
        user.find(marker='catalog-criteria-apply').click()
        assert await _until(lambda: fake.result_criteria[-1] == {
            'style': {'values': ['Square'], 'mode': 'only'}}), fake.result_criteria

    with patch.object(ui.dialog, 'close', close_and_hide):
        _run_with(_patched(fake), driver)


def test_a_ticked_value_the_new_source_lacks_stays_in_view(sidecar):
    """Codex round 1: Vellum ticked, then "Measured only": Vellum is no
    measured value, but it is still ticked -- it stays listed, with 0."""
    fake = _Recording(sidecar)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse')
        assert await _until(lambda: fake.result_criteria)
        user.find(marker='catalog-criteria-button').click()
        assert await _until(lambda: 'Physical status' in _page_text(user))
        user.find(marker='catalog-criteria-section-material').click()
        assert await _until(lambda: 'Vellum (2)' in _page_text(user)), _page_text(user)
        user.find(kind=ui.checkbox, content='Vellum (2)').click()
        with user._client:
            user.find(marker='catalog-criteria-source-material').elements.pop().set_value('measured')
        assert await _until(lambda: 'Vellum (0)' in _page_text(user)), _page_text(user)
        with user._client:
            box = next(e for e in user.find(kind=ui.checkbox, content='Vellum (0)').elements)
        assert box.value is True

    _run_with(_patched(fake), driver)


def test_two_quick_clicks_open_one_panel(sidecar):
    fake = _Recording(sidecar)

    async def driver(user: User) -> None:
        await user.open('/catalog-browse')
        assert await _until(lambda: fake.result_criteria)
        control = next(c for c in _controls() if c._client is user._client)
        await asyncio.gather(control.open(), control.open())
        dialogs = [e for e in user._client.elements.values() if isinstance(e, ui.dialog)
                   and 'catalog-criteria-dialog' in (e._markers or [])]
        assert len(dialogs) == 1, dialogs
        control._closed(dialogs[0])
        assert control._dialog is None
        await control.open()                       # and it opens again afterwards
        assert control._dialog is not None

    _run_with(_patched(fake), driver)
