# -*- coding: utf-8 -*-
"""web/pages/browse_correction_edit.py: a correction is filed under the page its editing began on.

The page-level behaviour (a page turn while editing) is in tests/test_browse_correction_off_loop.py;
these pin the module's own rules, the race with a write already in flight among them.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

from web.pages.browse_correction_edit import (EditedPage, HeldEdit, done_keeping, end_edit_for, held_edit,
                                              keep_draft, reopen_kept, restore_unkept, submission_landed,
                                              write_correction)


def _page(p, sys_id='990000000000017'):
    return SimpleNamespace(sys_id=sys_id, p_num=p, shelfmark='T-S 1.1', text='שלום')


def _state(**kw):
    base = dict(edit_mode=True, edit_page=None, edit_text='שלום עולם', edit_notes='', original_edit_text='שלום',
                draft_id=None, draft_saved=False, fullscreen_edit=False, volume_ie='IE1', unkept={}, keeping={})
    return SimpleNamespace(**{**base, **kw})


def _recorder():
    calls = []

    def create(**kw):
        calls.append(('create', kw))
        return {'success': True, 'correction': {'id': 99}}

    def update(draft_id, fields, client=None):
        calls.append(('update', draft_id, fields))
        return {'success': True, 'correction': {'id': draft_id}}

    return calls, create, update


async def _io_bound(fn, *a, **k):
    return fn(*a, **k)


def _keep(kept, create, update):
    return asyncio.run(keep_draft(kept, lock=asyncio.Lock(), io_bound=_io_bound, author_id='u', client=None,
                                  create=create, update=update))


def test_a_new_correction_is_filed_under_the_edited_page_not_the_page_on_screen():
    calls, create, update = _recorder()
    edited = EditedPage.bind(_state(), _page(1))
    write_correction(edited, draft_id=None, author_id='u', text='t', notes=None, status='draft', client=None,
                     create=create, update=update)
    kw = calls[0][1]
    assert (kw['sys_id'], kw['page_number'], kw['ie_id'], kw['original_text'], kw['notes']) == \
        ('990000000000017', 1, 'IE1', 'שלום', '')
    write_correction(edited, draft_id=5, author_id='u', text='t', notes='n', status='pending', client=None,
                     create=create, update=update)
    assert calls[1] == ('update', 5, {'corrected_text': 't', 'notes': 'n', 'status': 'pending'})


def test_an_editor_with_no_page_is_bound_to_the_page_it_is_shown_on():
    state = _state()
    assert end_edit_for(state, _page(1)) is None
    assert state.edit_page.is_page(_page(1)) and state.edit_mode is True
    assert end_edit_for(state, _page(1)) is None and state.edit_mode is True   # the same page: stays open


def test_leaving_the_page_closes_the_editor_and_returns_the_unsaved_edit():
    state = _state()
    end_edit_for(state, _page(1))
    edited = state.edit_page
    kept = end_edit_for(state, _page(2))
    assert kept == HeldEdit(edited, None, 'שלום עולם', '')
    assert (state.edit_mode, state.edit_page, state.edit_text, state.draft_id) == (False, None, '', None)
    assert end_edit_for(_state(edit_mode=False), _page(2)) is None


def test_leaving_a_page_whose_edit_is_saved_or_untouched_keeps_nothing():
    untouched = _state(edit_text='שלום')
    end_edit_for(untouched, _page(1))
    assert end_edit_for(untouched, _page(2)) is None and untouched.edit_mode is False

    saved = _state(draft_id=7, draft_saved=True)
    end_edit_for(saved, _page(1))
    assert saved.edit_page.saved == ('שלום עולם', '') and saved.edit_page.saved_draft_id == 7
    assert end_edit_for(saved, _page(2)) is None


def test_keep_draft_skips_what_a_write_in_flight_already_filed():
    """The write in flight finished after the page turn: its page knows the draft, so no second one."""
    calls, create, update = _recorder()
    edited = EditedPage.bind(_state(), _page(1))
    kept = HeldEdit(edited, None, 'שלום עולם', '')
    edited.note_saved(('שלום עולם', ''), 41)
    assert _keep(kept, create, update) == ('nothing', None) and calls == []


def test_keep_draft_updates_the_draft_a_write_in_flight_created():
    calls, create, update = _recorder()
    edited = EditedPage.bind(_state(), _page(1))
    edited.note_saved(('שלום', ''), 41)                       # an earlier text went in
    outcome, _ = _keep(HeldEdit(edited, None, 'שלום עולם', ''), create, update)
    assert outcome == 'kept' and calls == [('update', 41, {'corrected_text': 'שלום עולם', 'notes': '', 'status': 'draft'})]
    assert edited.saved == ('שלום עולם', '')


def test_keep_draft_reports_a_failed_write():
    edited = EditedPage.bind(_state(), _page(1))
    kept = HeldEdit(edited, None, 'x', '')
    assert _keep(kept, lambda **_k: None, None) == ('failed', None)
    assert _keep(kept, lambda **_k: {'error': 'denied'}, None) == ('failed', 'denied')
    assert edited.saved is None


def test_page_1_of_another_volume_is_another_page():
    edited = EditedPage.bind(_state(), _page(1))            # volume IE1
    other_volume = SimpleNamespace(**vars(_page(1)), volume_ie='IE2')
    assert not edited.is_page(other_volume)
    assert edited.is_page(SimpleNamespace(**vars(_page(1)), volume_ie='IE1')) and edited.is_page(_page(1))
    state = _state()
    end_edit_for(state, _page(1))
    assert end_edit_for(state, other_volume) is not None and state.edit_mode is False


def test_keep_draft_never_writes_a_draft_over_a_submission():
    calls, create, update = _recorder()
    edited = EditedPage.bind(_state(draft_id=41, draft_saved=True), _page(1))
    edited.note_submitted(('שלום עולם', ''))
    assert _keep(HeldEdit(edited, 41, 'שלום עולם', ''), create, update) == ('nothing', None)
    assert calls == []
    outcome, _ = _keep(HeldEdit(edited, 41, 'שלום עולם ומלואו', ''), create, update)   # typed after it went out
    assert outcome == 'kept' and [c[0] for c in calls] == ['create'], 'the submitted row was updated'
    assert edited.submitted is None and edited.saved_draft_id == 99


def test_an_edit_that_could_not_be_kept_reopens_on_its_page():
    edited = EditedPage.bind(_state(), _page(1))
    kept = HeldEdit(edited, None, 'שלום עולם', 'הערה')
    state = _state(edit_mode=False, edit_text='', unkept={edited.key(): kept})
    assert end_edit_for(state, _page(2)) is None and state.edit_mode is False and state.unkept
    assert end_edit_for(state, _page(1)) is None
    assert (state.edit_mode, state.edit_page, state.edit_text, state.edit_notes, state.unkept) ==         (True, edited, 'שלום עולם', 'הערה', {})
    assert restore_unkept(state, _page(1)) is False                # reopened once


def test_edit_on_a_page_still_being_kept_opens_that_edit_on_its_own_page():
    edited = EditedPage.bind(_state(), _page(1))
    kept = HeldEdit(edited, None, 'שלום עולם', '')
    state = _state(edit_mode=False, edit_text='', keeping={edited.key(): kept})
    assert reopen_kept(state, _page(2)) is False and state.edit_mode is False
    assert reopen_kept(state, _page(1)) is True
    assert (state.edit_mode, state.edit_page, state.edit_text, state.draft_id) == (True, edited, 'שלום עולם', None)
    edited.note_saved(('שלום עולם', ''), 41)                   # the keep lands
    assert edited.draft_to_update(state.draft_id) == 41, 'a later save would create a second draft'


def test_draft_to_update_prefers_the_pages_own_draft_and_none_after_a_submission():
    edited = EditedPage.bind(_state(), _page(1))
    assert edited.draft_to_update(7) == 7
    edited.note_saved(('x', ''), 41)
    assert edited.draft_to_update(7) == 41
    edited.note_submitted(('x', ''))
    assert edited.draft_to_update(7) is None


def test_held_edit_fixes_the_text_to_its_page_and_is_none_without_an_editor():
    state = _state(current_page=_page(1), is_loading=False, draft_id=7)
    clicked = held_edit(state)
    assert clicked == HeldEdit(state.edit_page, 7, 'שלום עולם', '') and state.edit_page.is_page(_page(1))
    state.edit_text = 'אחר'                                         # typed after the click: not in it
    assert clicked.text == 'שלום עולם' and held_edit(state).page is clicked.page
    assert held_edit(_state(edit_mode=False, current_page=_page(1), is_loading=False)) is None
    assert held_edit(_state(current_page=_page(1), is_loading=True)) is None


def test_a_draft_written_after_a_submission_is_the_pages_draft_from_then_on():
    edited = EditedPage.bind(_state(), _page(1))
    edited.note_submitted(('x', ''))
    edited.note_saved(('y', ''), 99)
    assert edited.submitted is None and edited.draft_to_update() == 99


def test_done_keeping_leaves_a_newer_keep_of_the_same_page():
    edited = EditedPage.bind(_state(), _page(1))
    older, newer = HeldEdit(edited, None, 'א', ''), HeldEdit(edited, None, 'אב', '')
    state = _state(keeping={edited.key(): newer})
    assert done_keeping(state, older) is False and state.keeping == {edited.key(): newer}
    assert done_keeping(state, newer) is True and state.keeping == {}


def test_a_landed_submission_closes_only_its_own_editor_showing_what_went_out():
    state = _state(draft_id=41)
    end_edit_for(state, _page(1))
    edited, sent = state.edit_page, ('שלום עולם', '')
    assert submission_landed(state, edited, sent) is True and edited.submitted == sent
    state.edit_text = 'שלום עולם ומלואו'                              # typed while it ran
    assert submission_landed(state, edited, sent) is False and (state.draft_id, state.draft_saved) == (None, False)
    other = _state(draft_id=7)
    end_edit_for(other, _page(2))
    assert submission_landed(other, edited, sent) is False and other.draft_id == 7   # another page's editor
