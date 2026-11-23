# -*- coding: utf-8 -*-
"""A correction belongs to the page its editing began on (GS-5).

Before, the reader's editor survived a page turn: the textarea kept page 1's text over page 2's
image, and Save Draft or Submit filed that text under whichever page was on screen. A new draft
then landed on page 2 with page 1's words.

Now:

- `EditedPage` is fixed when editing begins (`toggle_edit_mode`). Every correction write files
  under it, never under the page on screen.
- When a load lands on another page while editing, `end_edit_for` closes the editor. An edit not
  yet saved comes back as a `HeldEdit`, and `keep_draft` writes it as a draft of the page it was
  typed on.
- `keep_draft` runs as a background task, so the page turn never waits for it. It takes the
  correction lock, so it runs after any write already in flight. It skips the write when that
  earlier write already holds the same text.
- A submission is never demoted: a kept draft whose text was submitted writes nothing. One typed
  after the submission went out becomes a new draft, and the submitted row is left alone.
- A kept draft that could not be written stays in `state.unkept`. When the reader comes back to
  its page, `end_edit_for` reopens the editor with that text, so a failed write loses nothing.
- Edit on a page whose kept draft is still being written (`state.keeping`) opens that edit, on the
  same `EditedPage`, instead of asking the server, which may not have the draft yet. The keep
  then tells the reopened editor which draft it created (`draft_to_update`), so there is no
  second draft. A Submit that succeeds after the reader moved on leaves their new editor alone, and
  so does one whose editor, reopened, holds newer text than went out (`submission_landed`).
- Reopened, changed and left again before that keep is written, the page has a newer keep waiting
  behind it. The newer one is the reader's text: the older keep, done or failed, leaves it in
  `state.keeping` (`done_keeping`), and its failure is not kept in `state.unkept`.
- Save Draft and Submit fix what they write, and for which page, when clicked (`held_edit`), then
  wait their turn for the lock. A page turn while one waits neither drops it nor moves it to the
  editor of another page.
- A page is the same page only in the same volume: page 1 of one volume is not page 1 of another.

Nothing here touches the UI: `web/pages/browse.py` wires it in and says what happened.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Tuple

Text = Tuple[str, str]  # (corrected text, notes)


@dataclass
class EditedPage:
    """The page an edit belongs to, and what is already saved of it."""
    sys_id: str
    p_num: Optional[int]
    shelfmark: str
    ie_id: Optional[str]
    original_text: str
    start: Text                         # the editor's text and notes when editing began
    saved: Optional[Text] = None        # the text and notes the draft holds, once written
    saved_draft_id: Optional[Any] = None
    submitted: Optional[Text] = None    # the text and notes submitted for review, once sent

    @classmethod
    def bind(cls, state, page, *, start: Optional[Text] = None) -> 'EditedPage':
        """The page `page` as the editor in `state` begins on it."""
        start = start if start is not None else (state.original_edit_text or '', '')
        saved = (state.edit_text, state.edit_notes) if (state.draft_id and state.draft_saved) else None
        return cls(sys_id=page.sys_id, p_num=page.p_num, shelfmark=page.shelfmark or '',
                   ie_id=state.volume_ie, original_text=state.original_edit_text or '',
                   start=start, saved=saved, saved_draft_id=state.draft_id)

    def key(self):
        return self.sys_id, self.p_num, self.ie_id

    def is_page(self, page) -> bool:
        """`page` is this page: the same manuscript, page number and, where both know it, volume."""
        if page is None or (page.sys_id, page.p_num) != (self.sys_id, self.p_num):
            return False
        volume = getattr(page, 'volume_ie', None)
        return volume is None or self.ie_id is None or volume == self.ie_id

    def note_saved(self, text: Text, draft_id) -> None:
        """A write filed `text` as draft `draft_id` (whether or not the editor is still here). One
        written after a submission is a new row (`draft_to_update`): the page's draft from here on."""
        self.saved, self.submitted = text, None
        if draft_id:
            self.saved_draft_id = draft_id

    def note_submitted(self, text: Text) -> None:
        """`text` went out for review: no kept draft may write over it."""
        self.submitted = text

    def draft_to_update(self, draft_id=None):
        """The draft a write of this page updates: the one its writes created, else `draft_id`.
        None after a submission, so that a later text becomes a new draft."""
        if self.submitted is not None:
            return None
        return self.saved_draft_id or draft_id


@dataclass(frozen=True)
class HeldEdit:
    """An edit's text, fixed to the page it belongs to: one left unsaved when the reader turned away
    from its page (`end_edit_for`), or what a Save Draft or Submit click writes (`held_edit`)."""
    page: EditedPage
    draft_id: Optional[Any]
    text: str
    notes: str


def write_correction(page: EditedPage, *, draft_id, author_id, text: str, notes: str, status: str,
                     client, create: Callable, update: Callable):
    """Update the draft if there is one, else create a correction filed under `page`.

    `create` and `update` are `web.supabase_client`'s `create_correction` and
    `update_correction`, passed in by the caller."""
    if draft_id:
        return update(draft_id, {'corrected_text': text, 'notes': notes, 'status': status}, client=client)
    return create(author_id=author_id, sys_id=page.sys_id, shelfmark=page.shelfmark,
                  page_number=page.p_num, original_text=page.original_text, corrected_text=text,
                  notes=notes or '', status=status, ie_id=page.ie_id, client=client)


def held_edit(state) -> Optional[HeldEdit]:
    """What a Save Draft or Submit click writes: the editor's text and draft, fixed to its page now.

    The write then waits its turn for the correction lock, and a page turn meanwhile closes this
    editor or opens another page's: it must neither drop the click nor write that editor's text.
    None when there is no editor to write from: a stale button (the form after a submit, kept under
    a reload) or a click while a page loads."""
    if not state.edit_mode or state.is_loading:
        return None
    if state.edit_page is None:
        state.edit_page = EditedPage.bind(state, state.current_page)
    return HeldEdit(state.edit_page, state.draft_id, state.edit_text, state.edit_notes)


def has_unsaved_edit(state, page: EditedPage) -> bool:
    """The editor holds text that is neither what editing began with nor what the draft holds."""
    return (state.edit_text, state.edit_notes) != (page.saved if page.saved is not None else page.start)


def _open_on(state, kept: HeldEdit) -> None:
    """Open the editor with `kept`'s text, on its own `EditedPage`: a write still in flight for it
    then tells this editor which draft it created."""
    state.edit_mode, state.edit_page = True, kept.page
    state.edit_text, state.edit_notes = kept.text, kept.notes
    state.original_edit_text = kept.page.original_text
    state.draft_id = kept.page.draft_to_update(kept.draft_id)
    state.draft_saved = False


def restore_unkept(state, page) -> bool:
    """Reopen the editor on `page` with an edit of it that could not be kept, if there is one."""
    for key, kept in list(state.unkept.items()):
        if kept.page.is_page(page):
            del state.unkept[key]
            _open_on(state, kept)
            return True
    return False


def reopen_kept(state, page) -> bool:
    """The reader asks to edit `page`. An edit of it still being kept, or one that could not be,
    opens instead of what the server holds, which may not have the kept draft yet."""
    for kept in state.keeping.values():
        if kept.page.is_page(page):
            _open_on(state, kept)
            return True
    return restore_unkept(state, page)


def submission_landed(state, page: EditedPage, text: Text) -> bool:
    """`text` of `page` went out for review. True when the editor is to close: it is this page's and
    shows just what went out. An editor on another page is the reader's, and so is newer text typed
    into this page's (reopened after a page turn, say) while the submission ran: it stays open, and
    becomes a new draft, never an update of the submitted row."""
    page.note_submitted(text)
    if state.edit_page is not page:
        return False
    if (state.edit_text, state.edit_notes) != text:
        state.draft_id, state.draft_saved = None, False
        return False
    return True


def done_keeping(state, kept: HeldEdit) -> bool:
    """`kept` has been written, or has failed: it is no longer being kept. False when a newer edit of
    its page, left while this one was being written, is being kept now: that one stays, the reader's
    text from here on."""
    if state.keeping.get(kept.page.key()) is not kept:
        return False
    del state.keeping[kept.page.key()]
    return True


def end_edit_for(state, page) -> Optional[HeldEdit]:
    """A load landed on `page`. Close an editor that belongs to another page, and return what of
    it must still be kept. An editor with no page bound yet is bound to this one. With no editor
    open, an edit of this page that could not be kept is reopened."""
    if not state.edit_mode:
        restore_unkept(state, page)
        return None
    edited = state.edit_page
    if edited is None:
        state.edit_page = EditedPage.bind(state, page)
        return None
    if edited.is_page(page):
        return None
    kept = None
    if has_unsaved_edit(state, edited):
        kept = HeldEdit(edited, state.draft_id or edited.saved_draft_id, state.edit_text, state.edit_notes)
    state.edit_mode = False
    state.fullscreen_edit = False
    state.edit_text = ''
    state.edit_notes = ''
    state.draft_id = None
    state.draft_saved = False
    state.edit_page = None
    return kept


async def keep_draft(kept: HeldEdit, *, lock, io_bound, author_id, client, create, update):
    """Write `kept` as a draft of its own page, after any write already in flight.

    Returns ('nothing', None) when an earlier write already holds this text, ('kept', result) or
    ('failed', reason). A failed one is the caller's to keep in `state.unkept`."""
    async with lock:
        page = kept.page
        text = (kept.text, kept.notes)
        if text in (page.saved, page.submitted):
            return 'nothing', None
        # After a submission the row is under review: a later text is a new draft, never an update
        # that would turn the submitted row back into a draft.
        result = await io_bound(write_correction, page, draft_id=page.draft_to_update(kept.draft_id),
                                author_id=author_id, text=kept.text, notes=kept.notes, status='draft',
                                client=client, create=create, update=update)
        if result is None:
            return 'failed', None
        if 'error' in result:
            return 'failed', result['error']
        page.note_saved(text, (result.get('correction') or {}).get('id'))
        return 'kept', result
